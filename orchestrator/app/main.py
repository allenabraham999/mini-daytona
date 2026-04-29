from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

from .config import settings
from .loops import health_check_loop, idle_reaper
from .pool import NoCapacityError, NotFoundError, PoolManager
from .sandbox import build_backend
from .schemas import (
    AssignRequest,
    ExecRequest,
    ExecResponse,
    PoolStats,
    SandboxView,
)

logging.basicConfig(level=settings.log_level)
log = logging.getLogger("orchestrator")


@asynccontextmanager
async def lifespan(app: FastAPI):
    backend = build_backend(settings.sandbox_backend)
    pool = PoolManager(
        backend,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        idle_timeout_seconds=settings.idle_timeout_seconds,
    )
    app.state.backend = backend
    app.state.pool = pool

    await pool.warm_up()

    tasks = [
        asyncio.create_task(idle_reaper(pool, backend), name="idle-reaper"),
        asyncio.create_task(
            health_check_loop(pool, backend, settings.health_check_interval_seconds),
            name="health-check",
        ),
    ]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(title="sandbox-orchestrator", lifespan=lifespan)


def get_pool() -> PoolManager:
    return app.state.pool


def get_backend():
    return app.state.backend


@app.exception_handler(NotFoundError)
async def _not_found(_, exc: NotFoundError):
    return JSONResponse(status_code=404, content={"error": "sandbox_not_found", "detail": str(exc)})


@app.exception_handler(NoCapacityError)
async def _no_capacity(_, exc: NoCapacityError):
    return JSONResponse(status_code=503, content={"error": "no_capacity", "detail": str(exc)})


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


@app.get("/pool/stats", response_model=PoolStats)
async def pool_stats(pool: PoolManager = Depends(get_pool)) -> PoolStats:
    return PoolStats(**await pool.get_pool_stats())


@app.post("/sandbox/assign", response_model=SandboxView, status_code=status.HTTP_201_CREATED)
async def assign(req: AssignRequest, pool: PoolManager = Depends(get_pool)) -> SandboxView:
    sb = await pool.assign(req.user_id)
    return SandboxView(**sb.to_dict())


@app.get("/sandbox/{sandbox_id}", response_model=SandboxView)
async def get_sandbox(sandbox_id: str, pool: PoolManager = Depends(get_pool)) -> SandboxView:
    sb = await pool.get(sandbox_id)
    return SandboxView(**sb.to_dict())


@app.delete("/sandbox/{sandbox_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
async def release(sandbox_id: str, pool: PoolManager = Depends(get_pool)) -> None:
    await pool.release(sandbox_id)


@app.get("/internal/sandbox/metrics")
async def sandbox_metrics(backend=Depends(get_backend)) -> dict:
    if hasattr(backend, "boot_metrics"):
        return await backend.boot_metrics()
    return {"error": "metrics not available for this backend"}


@app.post("/sandbox/{sandbox_id}/exec", response_model=ExecResponse)
async def exec_in_sandbox(
    sandbox_id: str,
    req: ExecRequest,
    pool: PoolManager = Depends(get_pool),
    backend=Depends(get_backend),
) -> ExecResponse:
    sb = await pool.get(sandbox_id)
    if sb.state.value != "IN_USE":
        raise HTTPException(status_code=409, detail=f"sandbox is {sb.state.value}, not IN_USE")
    await pool.touch(sandbox_id)
    result = await backend.exec(sandbox_id, req.command, req.timeout_seconds)
    return ExecResponse(exit_code=result.exit_code, stdout=result.stdout, stderr=result.stderr)
