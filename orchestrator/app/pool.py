from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter

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

    # ----- public API ------------------------------------------------------

    async def warm_up(self) -> None:
        """Pre-provision min_size sandboxes so the first request is fast."""
        for _ in range(self._min_size):
            try:
                await self._provision()
            except Exception:
                log.exception("warm-up provisioning failed")

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
                return sb

        # No READY sandbox; provision one synchronously for this request.
        sb = await self._provision()
        async with self._lock:
            sb.transition(SandboxState.IN_USE)
            sb.user_id = user_id
            sb.last_active_at = time.time()
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
            counts: Counter[str] = Counter(s.state.value for s in self._sandboxes.values())
            return {
                "total": len(self._sandboxes),
                "min_size": self._min_size,
                "max_size": self._max_size,
                "by_state": dict(counts),
                "unhealthy": sum(1 for s in self._sandboxes.values() if not s.healthy),
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
        """Create a new sandbox via the backend and register it as READY.
        Caller MUST NOT hold _lock when calling this — backend.create() can be slow."""
        # Reserve a placeholder slot so concurrent provision calls respect max_size.
        placeholder_id = f"pending-{id(object())}"
        async with self._lock:
            if self._active_count_locked() >= self._max_size:
                raise NoCapacityError("pool is at max capacity")
            placeholder = Sandbox(sandbox_id=placeholder_id, state=SandboxState.PENDING)
            self._sandboxes[placeholder_id] = placeholder

        try:
            placeholder.transition(SandboxState.STARTING)
            handle = await self._backend.create()
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
