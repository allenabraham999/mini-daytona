from __future__ import annotations

import asyncio
import logging
import time

from .models import Sandbox, SandboxState
from .sandbox import SandboxBackend

log = logging.getLogger(__name__)


class PoolError(Exception):
    """Base for recoverable pool errors that should map to 4xx."""


class NoCapacityError(PoolError):
    """No READY sandbox and pool is at max_size."""


class NotFoundError(PoolError):
    pass


class PoolManager:
    """Tracks every sandbox in memory keyed by sandbox_id. All mutating
    operations go through `_lock` so the background reaper and health checker
    don't race the request path."""

    def __init__(
        self,
        backend: SandboxBackend,
        *,
        min_size: int,
        max_size: int,
        idle_timeout_seconds: int,
    ) -> None:
        self._backend = backend
        self._min_size = min_size
        self._max_size = max_size
        self._idle_timeout = idle_timeout_seconds
        self._sandboxes: dict[str, Sandbox] = {}
        self._lock = asyncio.Lock()
        # `_last_assigned_at` powers scale-to-zero: it advances every time a
        # caller actually receives a sandbox. Initialised to "now" so a freshly
        # started pool isn't immediately torn down.
        self._last_assigned_at: float = time.time()
        self._last_scale_event: dict | None = None

    # ----- public API ------------------------------------------------------

    async def warm_up(self) -> None:
        """Schedule pre-provisioning of min_size sandboxes in the background and
        return immediately so the API can start serving (and pass healthchecks)
        right away. If a request arrives before the pool is warm, `assign` will
        cold-create on demand."""
        asyncio.create_task(self._warm_up_background(), name="pool-warm-up")

    async def _warm_up_background(self) -> None:
        consecutive_failures = 0
        for _ in range(self._min_size):
            try:
                await self._provision()
                consecutive_failures = 0
            except Exception:
                log.exception("warm-up provisioning failed")
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    log.error(
                        "warm-up aborted after %d consecutive failures",
                        consecutive_failures,
                    )
                    return
                await asyncio.sleep(2 ** consecutive_failures)

    async def get_available(self) -> Sandbox | None:
        """Return a READY sandbox without assigning it. Mostly for diagnostics —
        `assign` is the atomic operation callers actually want."""
        async with self._lock:
            for sb in self._sandboxes.values():
                if sb.state == SandboxState.READY and sb.healthy:
                    return sb
            return None

    async def assign(self, user_id: str) -> Sandbox:
        """Atomically pick a READY sandbox (or provision one) and mark it IN_USE
        for `user_id`. Raises NoCapacityError if the pool is full."""
        async with self._lock:
            sb = self._pick_ready_locked()
            if sb is None:
                if self._active_count_locked() >= self._max_size:
                    raise NoCapacityError("pool is at max capacity")
                # Drop the lock around the (slow) backend.create call.
                pass
            else:
                sb.transition(SandboxState.IN_USE)
                sb.user_id = user_id
                sb.last_active_at = time.time()
                self._last_assigned_at = sb.last_active_at
                sandbox_id = sb.sandbox_id

        if sb is not None:
            # Warm path: pool member was cloned but not yet started. Boot it
            # now and refresh the connection details from the backend.
            try:
                handle = await self._backend.start(sandbox_id)
            except Exception:
                async with self._lock:
                    cur = self._sandboxes.get(sandbox_id)
                    if cur is not None and cur.state == SandboxState.IN_USE:
                        cur.transition(SandboxState.TERMINATING)
                try:
                    await self._backend.destroy(sandbox_id)
                except Exception:
                    log.exception("backend destroy failed for %s after start failure", sandbox_id)
                await self.finalize_destroy(sandbox_id)
                raise
            async with self._lock:
                cur = self._sandboxes.get(sandbox_id)
                if cur is not None:
                    cur.host = handle.host
                    cur.port = handle.port
                    cur.ssh_user = handle.ssh_user
                    cur.ssh_key_fingerprint = handle.ssh_key_fingerprint
                    return cur
                return sb

        # No READY sandbox; cold-create one synchronously for this request.
        sb = await self._provision_cold()
        async with self._lock:
            sb.transition(SandboxState.IN_USE)
            sb.user_id = user_id
            sb.last_active_at = time.time()
            self._last_assigned_at = sb.last_active_at
            return sb

    async def release(self, sandbox_id: str) -> None:
        """Caller is done with the sandbox. We destroy it rather than recycle —
        a real implementation may want to scrub-and-reuse, but for the skeleton
        a fresh VM per session is the simpler, safer default."""
        async with self._lock:
            sb = self._sandboxes.get(sandbox_id)
            if sb is None:
                raise NotFoundError(sandbox_id)
            if sb.state in (SandboxState.TERMINATING, SandboxState.DESTROYED):
                return
            sb.transition(SandboxState.TERMINATING)

        try:
            await self._backend.destroy(sandbox_id)
        except Exception:
            log.exception("backend destroy failed for %s", sandbox_id)

        async with self._lock:
            sb = self._sandboxes.get(sandbox_id)
            if sb is not None:
                sb.transition(SandboxState.DESTROYED)
                self._sandboxes.pop(sandbox_id, None)

        # Refill toward min_size if we dropped below it.
        asyncio.create_task(self._maybe_refill())

    async def get(self, sandbox_id: str) -> Sandbox:
        async with self._lock:
            sb = self._sandboxes.get(sandbox_id)
            if sb is None:
                raise NotFoundError(sandbox_id)
            return sb

    async def touch(self, sandbox_id: str) -> None:
        """Refresh idle timer; called on /exec."""
        async with self._lock:
            sb = self._sandboxes.get(sandbox_id)
            if sb is None:
                raise NotFoundError(sandbox_id)
            sb.last_active_at = time.time()

    async def get_pool_stats(self) -> dict:
        async with self._lock:
            return self._stats_locked()

    def _stats_locked(self) -> dict:
        total, available, in_use = self._counts_locked()
        ratio = available / max(total, 1)
        return {
            "total_count": total,
            "available_count": available,
            "in_use_count": in_use,
            "available_ratio": ratio,
            "last_scale_event": self._last_scale_event,
            "pool_min": self._min_size,
            "pool_max": self._max_size,
        }

    def _counts_locked(self) -> tuple[int, int, int]:
        total = 0
        available = 0
        in_use = 0
        for s in self._sandboxes.values():
            if s.state == SandboxState.DESTROYED:
                continue
            total += 1
            if s.state == SandboxState.READY and s.healthy:
                available += 1
            elif s.state == SandboxState.IN_USE:
                in_use += 1
        return total, available, in_use

    # ----- scaling primitives --------------------------------------------

    async def scaling_snapshot(self) -> dict:
        """Read-only stats plus `last_assigned_at` — used by pool_scaler to
        decide whether to act."""
        async with self._lock:
            stats = self._stats_locked()
            stats["last_assigned_at"] = self._last_assigned_at
            return stats

    async def scale_up(self, n: int) -> int:
        """Provision up to `n` new sandboxes, clamped by remaining headroom to
        max_size. Returns the count actually created."""
        if n <= 0:
            return 0
        async with self._lock:
            headroom = self._max_size - self._active_count_locked()
            target = max(0, min(n, headroom))
        created = 0
        consecutive_failures = 0
        for _ in range(target):
            try:
                await self._provision()
                created += 1
                consecutive_failures = 0
            except NoCapacityError:
                break
            except Exception:
                log.exception("scale_up provisioning failed")
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    log.error(
                        "scale_up aborted after %d consecutive failures",
                        consecutive_failures,
                    )
                    break
                await asyncio.sleep(2 ** consecutive_failures)
        if created:
            await self._record_scale_event("scale_up", created)
        return created

    async def scale_down_one(self) -> str | None:
        """Pick one idle (READY) sandbox, tear it down. Refuses to drop
        total_count below min_size. Returns the destroyed id (or None)."""
        async with self._lock:
            if self._active_count_locked() <= self._min_size:
                return None
            target: Sandbox | None = None
            for sb in self._sandboxes.values():
                if sb.state == SandboxState.READY:
                    target = sb
                    break
            if target is None:
                return None
            target.transition(SandboxState.TERMINATING)
            sandbox_id = target.sandbox_id

        try:
            await self._backend.destroy(sandbox_id)
        except Exception:
            log.exception("scale_down destroy failed for %s", sandbox_id)
        await self.finalize_destroy(sandbox_id)
        await self._record_scale_event("scale_down", 1)
        return sandbox_id

    async def scale_to_zero(self) -> list[str]:
        """When min_size is 0, destroy every idle (READY) sandbox. IN_USE
        sandboxes are left alone — the idle reaper handles those."""
        if self._min_size != 0:
            return []
        async with self._lock:
            targets = [s for s in self._sandboxes.values() if s.state == SandboxState.READY]
            for sb in targets:
                sb.transition(SandboxState.TERMINATING)
            ids = [s.sandbox_id for s in targets]

        for sandbox_id in ids:
            try:
                await self._backend.destroy(sandbox_id)
            except Exception:
                log.exception("scale_to_zero destroy failed for %s", sandbox_id)
            await self.finalize_destroy(sandbox_id)
        if ids:
            await self._record_scale_event("scale_to_zero", len(ids))
        return ids

    async def _record_scale_event(self, action: str, count: int) -> None:
        async with self._lock:
            stats = self._stats_locked()
            self._last_scale_event = {
                "action": action,
                "count": count,
                "at": time.time(),
                "stats": {
                    "total_count": stats["total_count"],
                    "available_count": stats["available_count"],
                    "in_use_count": stats["in_use_count"],
                    "available_ratio": stats["available_ratio"],
                },
            }

    # ----- snapshot helpers used by background loops ----------------------

    async def snapshot_active(self) -> list[Sandbox]:
        """Return a shallow copy of non-destroyed sandboxes for iteration
        outside the lock."""
        async with self._lock:
            return [s for s in self._sandboxes.values() if s.state != SandboxState.DESTROYED]

    async def mark_health(self, sandbox_id: str, healthy: bool) -> None:
        async with self._lock:
            sb = self._sandboxes.get(sandbox_id)
            if sb is None:
                return
            sb.healthy = healthy
            sb.last_health_at = time.time()

    async def reap_idle(self) -> list[str]:
        """Return ids of IN_USE sandboxes idle past the timeout, transitioning
        them to TERMINATING. Caller is responsible for the destroy call so we
        don't hold the lock across IO."""
        now = time.time()
        to_destroy: list[str] = []
        async with self._lock:
            for sb in self._sandboxes.values():
                if sb.state == SandboxState.IN_USE and (now - sb.last_active_at) > self._idle_timeout:
                    sb.transition(SandboxState.TERMINATING)
                    to_destroy.append(sb.sandbox_id)
        return to_destroy

    async def finalize_destroy(self, sandbox_id: str) -> None:
        async with self._lock:
            sb = self._sandboxes.get(sandbox_id)
            if sb is None:
                return
            if sb.state == SandboxState.TERMINATING:
                sb.transition(SandboxState.DESTROYED)
            self._sandboxes.pop(sandbox_id, None)

    # ----- internals ------------------------------------------------------

    def _pick_ready_locked(self) -> Sandbox | None:
        for sb in self._sandboxes.values():
            if sb.state == SandboxState.READY and sb.healthy:
                return sb
        return None

    def _active_count_locked(self) -> int:
        return sum(1 for s in self._sandboxes.values() if s.state != SandboxState.DESTROYED)

    async def _provision(self) -> Sandbox:
        """Pre-warm a stopped sandbox via the backend and register it as READY.
        The container is cloned but not started — `assign()` boots it on demand.
        Caller MUST NOT hold _lock when calling this — backend.create_pooled() can be slow."""
        return await self._provision_with(self._backend.create_pooled)

    async def _provision_cold(self) -> Sandbox:
        """Cold-create a fully started sandbox for an immediate assignment.
        Used when `assign()` finds no READY sandbox and the pool is below max."""
        return await self._provision_with(self._backend.create)

    async def _provision_with(self, factory) -> Sandbox:
        # Reserve a placeholder slot so concurrent provision calls respect max_size.
        placeholder_id = f"pending-{id(object())}"
        async with self._lock:
            if self._active_count_locked() >= self._max_size:
                raise NoCapacityError("pool is at max capacity")
            placeholder = Sandbox(sandbox_id=placeholder_id, state=SandboxState.PENDING)
            self._sandboxes[placeholder_id] = placeholder

        try:
            placeholder.transition(SandboxState.STARTING)
            handle = await factory()
        except Exception:
            async with self._lock:
                self._sandboxes.pop(placeholder_id, None)
            raise

        async with self._lock:
            self._sandboxes.pop(placeholder_id, None)
            sb = Sandbox(
                sandbox_id=handle.sandbox_id,
                state=SandboxState.STARTING,
                host=handle.host,
                port=handle.port,
                ssh_user=handle.ssh_user,
                ssh_key_fingerprint=handle.ssh_key_fingerprint,
            )
            sb.transition(SandboxState.READY)
            self._sandboxes[handle.sandbox_id] = sb
            return sb

    async def _maybe_refill(self) -> None:
        async with self._lock:
            ready_or_pending = sum(
                1
                for s in self._sandboxes.values()
                if s.state in (SandboxState.READY, SandboxState.STARTING, SandboxState.PENDING)
            )
            deficit = max(0, self._min_size - ready_or_pending)
            headroom = self._max_size - self._active_count_locked()
            to_create = min(deficit, headroom)

        for _ in range(to_create):
            try:
                await self._provision()
            except Exception:
                log.exception("background refill provisioning failed")
                return
