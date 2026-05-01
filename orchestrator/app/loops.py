from __future__ import annotations

import asyncio
import logging
import time

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


async def pool_scaler(
    pool: PoolManager,
    *,
    scale_up_threshold: float,
    scale_down_threshold: float,
    idle_timeout_seconds: int,
    interval_seconds: int = 30,
) -> None:
    """Threshold-based dynamic scaler. Additive on top of the warm-pool
    replenish-on-use pattern — `assign`/`release` still drive the steady-state
    minimum; this loop just nudges the pool up or down when demand shifts.

    Rules (evaluated each tick):
      * available_ratio < scale_up_threshold AND total < pool_max  → +2 in bg
      * available_ratio > scale_down_threshold AND total > pool_min → -1 idle
      * pool_min == 0 AND no recent assignments AND no in_use      → tear down
        all idle containers (scale-to-zero)
    """
    while True:
        try:
            stats = await pool.scaling_snapshot()
            total = stats["total_count"]
            available = stats["available_count"]
            in_use = stats["in_use_count"]
            ratio = stats["available_ratio"]
            pool_min = stats["pool_min"]
            pool_max = stats["pool_max"]
            last_assigned_at = stats["last_assigned_at"]
            idle_since = time.time() - last_assigned_at

            if (
                pool_min == 0
                and in_use == 0
                and available > 0
                and idle_since > idle_timeout_seconds
            ):
                # Scale-to-zero takes precedence — once we're truly idle we want
                # to free everything regardless of where the ratio sits.
                ids = await pool.scale_to_zero()
                if ids:
                    log.info(
                        "pool_scaler scale_to_zero destroyed=%d idle_for=%.0fs total=%d available=%d in_use=%d",
                        len(ids),
                        idle_since,
                        total,
                        available,
                        in_use,
                    )
            elif (
                ratio < scale_up_threshold
                and total < pool_max
                # In scale-to-zero mode (pool_min=0) we only pre-warm when
                # there's actual demand — otherwise we'd immediately undo a
                # scale-to-zero on the next tick. The on-demand path in
                # `assign()` still cold-creates when a request lands.
                and (pool_min > 0 or in_use > 0)
            ):
                # Background clone — don't block the scaler tick on provision.
                async def _scale_up_bg() -> None:
                    created = await pool.scale_up(2)
                    if created:
                        post = await pool.get_pool_stats()
                        log.info(
                            "pool_scaler scale_up created=%d total=%d available=%d in_use=%d ratio=%.2f",
                            created,
                            post["total_count"],
                            post["available_count"],
                            post["in_use_count"],
                            post["available_ratio"],
                        )

                asyncio.create_task(_scale_up_bg(), name="pool-scale-up")
                log.info(
                    "pool_scaler scale_up triggered ratio=%.2f<%.2f total=%d/%d available=%d in_use=%d",
                    ratio,
                    scale_up_threshold,
                    total,
                    pool_max,
                    available,
                    in_use,
                )
            elif ratio > scale_down_threshold and total > pool_min:
                destroyed = await pool.scale_down_one()
                if destroyed:
                    post = await pool.get_pool_stats()
                    log.info(
                        "pool_scaler scale_down destroyed=%s total=%d available=%d in_use=%d ratio=%.2f",
                        destroyed,
                        post["total_count"],
                        post["available_count"],
                        post["in_use_count"],
                        post["available_ratio"],
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("pool_scaler iteration failed")
        await asyncio.sleep(interval_seconds)


async def health_check_loop(pool: PoolManager, backend: SandboxBackend, interval_seconds: int) -> None:
    """Pings every active sandbox at the configured cadence. Marks failures as
    unhealthy and tears them down so the pool eventually backfills. Also reaps
    ghost sandboxes stuck in PENDING/STARTING from provisions that never
    completed cleanup."""
    while True:
        try:
            sandboxes = await pool.snapshot_active()
            # Pool members in READY are intentionally STOPPED (clone-but-don't-start);
            # probing them with a "running?" check would mark them unhealthy and hide
            # them from available_count, causing the scaler to spin until max_size.
            in_use = [s for s in sandboxes if s.state == SandboxState.IN_USE]
            results = await asyncio.gather(
                *(backend.health_check(s.sandbox_id) for s in in_use),
                return_exceptions=True,
            )
            for sb, ok in zip(in_use, results):
                healthy = bool(ok) and not isinstance(ok, BaseException)
                await pool.mark_health(sb.sandbox_id, healthy)
                if not healthy and sb.state == SandboxState.IN_USE:
                    log.warning("sandbox %s failed health check; destroying", sb.sandbox_id)
                    try:
                        await backend.destroy(sb.sandbox_id)
                    finally:
                        await pool.finalize_destroy(sb.sandbox_id)

            stale = await pool.reap_stale_provisions(60)
            for sandbox_id in stale:
                log.warning("destroying ghost sandbox %s stuck in PENDING/STARTING >60s", sandbox_id)
                try:
                    await backend.destroy(sandbox_id)
                except Exception:
                    log.exception("backend destroy failed for ghost sandbox %s", sandbox_id)
                await pool.finalize_destroy(sandbox_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("health_check_loop iteration failed")
        await asyncio.sleep(interval_seconds)
