from __future__ import annotations

import asyncio
import logging
import secrets
import time
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field

from .base import ExecResult, SandboxBackend, SandboxHandle

logger = logging.getLogger(__name__)

BASE_CONTAINER = "base-container"
BASE_SNAPSHOT = "snap0"
POOL_SIZE = 5


@dataclass
class _BootMetrics:
    warm_boots: list[float] = field(default_factory=list)
    cold_boots: list[float] = field(default_factory=list)

    def record_warm(self, seconds: float) -> None:
        self.warm_boots.append(seconds)
        logger.info("warm boot %.3fs (n=%d)", seconds, len(self.warm_boots))

    def record_cold(self, seconds: float) -> None:
        self.cold_boots.append(seconds)
        logger.info("cold boot %.3fs (n=%d)", seconds, len(self.cold_boots))

    def summary(self) -> dict:
        def stats(samples: list[float]) -> dict:
            if not samples:
                return {"count": 0}
            return {
                "count": len(samples),
                "min": min(samples),
                "max": max(samples),
                "avg": sum(samples) / len(samples),
            }

        return {"warm": stats(self.warm_boots), "cold": stats(self.cold_boots)}


def _new_id() -> str:
    return f"sbx-{uuid.uuid4().hex[:12]}"


async def _run(
    *args: str, timeout: float = 30.0, check: bool = True
) -> tuple[int, str, str]:
    """Run an incus CLI command asynchronously."""
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise TimeoutError(f"command timed out after {timeout}s: {' '.join(args)}")

    rc = proc.returncode or 0
    stdout = stdout_b.decode(errors="replace")
    stderr = stderr_b.decode(errors="replace")

    if check and rc != 0:
        raise RuntimeError(
            f"incus command failed (rc={rc}): {' '.join(args)}\nstderr: {stderr}"
        )
    return rc, stdout, stderr


async def _run_streaming(
    *args: str, timeout: float = 30.0
) -> AsyncGenerator[dict, None]:
    """Run an incus CLI command and yield line-by-line output dicts."""
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    queue: asyncio.Queue[dict | None] = asyncio.Queue()

    async def drain(stream: asyncio.StreamReader, stream_type: str) -> None:
        async for raw in stream:
            await queue.put({"type": stream_type, "data": raw.decode(errors="replace").rstrip("\n")})
        await queue.put(None)

    tasks = [
        asyncio.create_task(drain(proc.stdout, "stdout")),
        asyncio.create_task(drain(proc.stderr, "stderr")),
    ]

    done_streams = 0
    deadline = asyncio.get_event_loop().time() + timeout
    timed_out = False

    while done_streams < 2:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            timed_out = True
            break
        try:
            item = await asyncio.wait_for(queue.get(), timeout=remaining)
        except asyncio.TimeoutError:
            timed_out = True
            break
        if item is None:
            done_streams += 1
        else:
            yield item

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    if timed_out:
        proc.kill()
        await proc.wait()
        yield {"type": "exit", "data": "124"}
    else:
        await proc.wait()
        yield {"type": "exit", "data": str(proc.returncode or 0)}


async def _stream_file_pull(sandbox_id: str, path: str) -> AsyncGenerator[bytes, None]:
    """Stream the contents of `path` inside `sandbox_id` via `incus file pull`."""
    proc = await asyncio.create_subprocess_exec(
        "incus", "file", "pull", f"{sandbox_id}{path}", "-",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        while True:
            chunk = await proc.stdout.read(64 * 1024)
            if not chunk:
                break
            yield chunk
    finally:
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        await proc.wait()


async def _clone_and_stop(sandbox_id: str) -> None:
    """Clone base-container/snap0 → sandbox_id and leave it stopped (pool ready)."""
    await _run("incus", "copy", f"{BASE_CONTAINER}/{BASE_SNAPSHOT}", sandbox_id)
    # copy from a snapshot creates the container in Stopped state — nothing else needed


async def _start_container(sandbox_id: str) -> None:
    await _run("incus", "start", sandbox_id)


async def _get_container_ip(sandbox_id: str, timeout: float = 30.0) -> str | None:
    """Poll until eth0 has an IPv4 address, return it or None on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _, stdout, _ = await _run(
                "incus", "query", f"/1.0/instances/{sandbox_id}/state",
                timeout=5.0,
            )
            import json

            state = json.loads(stdout)
            addresses = (
                state.get("network", {})
                .get("eth0", {})
                .get("addresses", [])
            )
            for addr in addresses:
                if addr.get("family") == "inet" and addr.get("scope") == "global":
                    return addr["address"]
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return None


class IncusSandboxBackend(SandboxBackend):
    """Sandbox backend backed by Incus containers.

    Pre-warms a pool of POOL_SIZE stopped containers cloned from
    base-container/snap0. When one is claimed it is started immediately and
    a replacement is queued in the background, keeping latency low.
    """

    def __init__(self, pool_size: int = POOL_SIZE) -> None:
        self._pool_size = pool_size
        self._pool: asyncio.Queue[str] = asyncio.Queue()
        self._alive: dict[str, SandboxHandle] = {}
        self._lock = asyncio.Lock()
        self._metrics = _BootMetrics()
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Pre-warm the container pool. Call once at startup."""
        if self._initialized:
            return
        self._initialized = True
        logger.info("pre-warming %d containers", self._pool_size)
        await asyncio.gather(*[self._add_to_pool() for _ in range(self._pool_size)])
        logger.info("pool ready")

    async def _add_to_pool(self) -> None:
        """Clone one stopped container and push it onto the pool queue."""
        sandbox_id = _new_id()
        try:
            await _clone_and_stop(sandbox_id)
            await self._pool.put(sandbox_id)
            logger.debug("pool +%s (size ~%d)", sandbox_id, self._pool.qsize())
        except Exception:
            logger.exception("failed to pre-warm container %s", sandbox_id)

    def _replenish_pool(self) -> None:
        """Schedule a background pool replenishment without blocking the caller."""
        asyncio.get_event_loop().create_task(self._add_to_pool())

    # ------------------------------------------------------------------
    # SandboxBackend interface
    # ------------------------------------------------------------------

    async def create(self) -> SandboxHandle:
        t0 = time.monotonic()

        try:
            # Non-blocking get: use a warm container if one is ready
            sandbox_id = self._pool.get_nowait()
            warm = True
        except asyncio.QueueEmpty:
            # Fall back to cold creation
            logger.warning("pool empty — cold-creating container")
            sandbox_id = _new_id()
            await _clone_and_stop(sandbox_id)
            warm = False

        # Start the container
        await _start_container(sandbox_id)

        elapsed = time.monotonic() - t0
        if warm:
            self._metrics.record_warm(elapsed)
        else:
            self._metrics.record_cold(elapsed)

        # Immediately queue a replacement so the pool refills in the background
        self._replenish_pool()

        ip = await _get_container_ip(sandbox_id) or sandbox_id
        handle = SandboxHandle(
            sandbox_id=sandbox_id,
            host=ip,
            port=2222,
            ssh_user="sandbox",
            ssh_key_fingerprint=f"SHA256:{secrets.token_urlsafe(32)}",
        )
        async with self._lock:
            self._alive[sandbox_id] = handle
        return handle

    async def destroy(self, sandbox_id: str) -> None:
        async with self._lock:
            self._alive.pop(sandbox_id, None)
        try:
            await _run("incus", "delete", sandbox_id, "--force")
        except Exception:
            logger.exception("error deleting container %s", sandbox_id)

    async def health_check(self, sandbox_id: str) -> bool:
        try:
            rc, stdout, _ = await _run(
                "incus", "info", sandbox_id, timeout=5.0, check=False
            )
            return rc == 0 and "Status: Running" in stdout
        except Exception:
            return False

    async def exec(
        self, sandbox_id: str, command: str, timeout_seconds: int
    ) -> ExecResult:
        async with self._lock:
            alive = sandbox_id in self._alive
        if not alive:
            return ExecResult(
                exit_code=127, stdout="", stderr=f"sandbox {sandbox_id} not found"
            )
        try:
            rc, stdout, stderr = await _run(
                "incus", "exec", sandbox_id, "--",
                "sh", "-c", command,
                timeout=float(timeout_seconds),
                check=False,
            )
            return ExecResult(exit_code=rc, stdout=stdout, stderr=stderr)
        except TimeoutError as exc:
            return ExecResult(exit_code=124, stdout="", stderr=str(exc))
        except Exception as exc:
            return ExecResult(exit_code=1, stdout="", stderr=str(exc))

    async def exec_stream(
        self, sandbox_id: str, command: str, timeout_seconds: int
    ) -> AsyncGenerator[dict, None]:
        async with self._lock:
            alive = sandbox_id in self._alive
        if not alive:
            yield {"type": "stderr", "data": f"sandbox {sandbox_id} not found"}
            yield {"type": "exit", "data": "127"}
            return
        async for chunk in _run_streaming(
            "incus", "exec", sandbox_id, "--",
            "sh", "-c", command,
            timeout=float(timeout_seconds),
        ):
            yield chunk

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    async def upload_file(
        self, sandbox_id: str, local_path: str, dest_path: str
    ) -> None:
        dest_dir = dest_path.rsplit("/", 1)[0] or "/"
        await _run(
            "incus", "exec", sandbox_id, "--",
            "mkdir", "-p", dest_dir,
            timeout=10.0,
        )
        await _run(
            "incus", "file", "push", local_path,
            f"{sandbox_id}{dest_path}",
            timeout=120.0,
        )

    async def download_file(
        self, sandbox_id: str, path: str
    ) -> AsyncGenerator[bytes, None]:
        # Verify the file exists before we start streaming so callers can map
        # the failure to a 404 instead of a broken response stream.
        rc, _, _ = await _run(
            "incus", "exec", sandbox_id, "--",
            "test", "-f", path,
            timeout=5.0, check=False,
        )
        if rc != 0:
            raise FileNotFoundError(f"file not found: {path}")

        return _stream_file_pull(sandbox_id, path)

    async def list_files(
        self, sandbox_id: str, directory: str
    ) -> list[dict]:
        rc, stdout, stderr = await _run(
            "incus", "exec", sandbox_id, "--",
            "ls", "-la", "--time-style=full-iso", directory,
            timeout=10.0, check=False,
        )
        if rc != 0:
            raise FileNotFoundError(f"cannot list {directory}: {stderr.strip()}")

        entries: list[dict] = []
        for line in stdout.splitlines():
            if line.startswith("total "):
                continue
            parts = line.split(maxsplit=8)
            if len(parts) < 9:
                continue
            perms, nlinks, owner, group, size, date_, time_, tz, name = parts
            try:
                size_int = int(size)
            except ValueError:
                size_int = 0
            entries.append({
                "name": name,
                "size": size_int,
                "permissions": perms,
                "owner": owner,
                "group": group,
                "modified": f"{date_} {time_} {tz}",
                "is_dir": perms.startswith("d"),
            })
        return entries

    # ------------------------------------------------------------------
    # Extras
    # ------------------------------------------------------------------

    async def get_status(self, sandbox_id: str) -> dict:
        """Return raw Incus state dict for sandbox_id."""
        _, stdout, _ = await _run(
            "incus", "query", f"/1.0/instances/{sandbox_id}/state",
            timeout=5.0,
        )
        import json

        return json.loads(stdout)

    def boot_metrics(self) -> dict:
        return self._metrics.summary()
