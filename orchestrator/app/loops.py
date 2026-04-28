from __future__ import annotations

import asyncio
import logging

from .models import SandboxState
from .pool import PoolManager
from .sandbox import SandboxBackend

log = logging.getLogger(__name__)


async def idle_reaper(pool: PoolManager, backend: SandboxBackend, interval_seconds: int = 15) -> None:
    """Sweeps the pool periodically and destroys IN_USE sandboxes idle past the
    configured threshold."""
    while True:
        try:
            stale = await pool.reap_idle()
            for sandbox_id in stale:
                log.info("reaping idle sandbox %s", sandbox_id)
                try:
                    await backend.destroy(sandbox_id)
                finally:
                    await pool.finalize_destroy(sandbox_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("idle_reaper iteration failed")
        await asyncio.sleep(interval_seconds)


async def health_check_loop(pool: PoolManager, backend: SandboxBackend, interval_seconds: int) -> None:
    """Pings every active sandbox at the configured cadence. Marks failures as
    unhealthy and tears them down so the pool eventually backfills."""
    while True:
        try:
            sandboxes = await pool.snapshot_active()
            results = await asyncio.gather(
                *(backend.health_check(s.sandbox_id) for s in sandboxes),
                return_exceptions=True,
            )
            for sb, ok in zip(sandboxes, results):
                healthy = bool(ok) and not isinstance(ok, BaseException)
                await pool.mark_health(sb.sandbox_id, healthy)
                if not healthy and sb.state in (SandboxState.READY, SandboxState.IN_USE):
                    log.warning("sandbox %s failed health check; destroying", sb.sandbox_id)
                    try:
                        await backend.destroy(sb.sandbox_id)
                    finally:
                        await pool.finalize_destroy(sb.sandbox_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("health_check_loop iteration failed")
        await asyncio.sleep(interval_seconds)
