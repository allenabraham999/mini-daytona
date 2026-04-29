from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile, status
from fastapi.responses import JSONResponse, StreamingResponse

from .config import settings
from .loops import health_check_loop, idle_reaper
from .pool import NoCapacityError, NotFoundError, PoolManager
from .sandbox import build_backend
from .schemas import (
    AgentRunRequest,
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


AGENT_RUN_TIMEOUT_SECONDS = 300


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
    from .sandbox.incus import IncusSandboxBackend

    if not isinstance(backend, IncusSandboxBackend):
        return {"error": "metrics only available for incus backend"}
    return backend.boot_metrics()


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


UPLOAD_DEST_DIR = "/tmp/uploads"


def _ensure_in_use(sb) -> None:
    if sb.state.value != "IN_USE":
        raise HTTPException(status_code=409, detail=f"sandbox is {sb.state.value}, not IN_USE")


@app.post("/sandbox/{sandbox_id}/files", status_code=status.HTTP_201_CREATED)
async def upload_files(
    sandbox_id: str,
    files: list[UploadFile] = File(...),
    pool: PoolManager = Depends(get_pool),
    backend=Depends(get_backend),
) -> dict:
    sb = await pool.get(sandbox_id)
    _ensure_in_use(sb)
    await pool.touch(sandbox_id)

    uploaded: list[str] = []
    for upload in files:
        if not upload.filename:
            continue
        # Strip any path segments — the caller doesn't get to choose where in
        # the sandbox the file lands, only the basename.
        safe_name = os.path.basename(upload.filename)
        dest = f"{UPLOAD_DEST_DIR}/{safe_name}"

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                tmp.write(chunk)
        try:
            await backend.upload_file(sandbox_id, tmp_path, dest)
            uploaded.append(dest)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    return {"uploaded": uploaded}


@app.get("/sandbox/{sandbox_id}/files")
async def download_file(
    sandbox_id: str,
    path: str = Query(..., min_length=1),
    pool: PoolManager = Depends(get_pool),
    backend=Depends(get_backend),
) -> StreamingResponse:
    sb = await pool.get(sandbox_id)
    _ensure_in_use(sb)
    await pool.touch(sandbox_id)

    try:
        stream = await backend.download_file(sandbox_id, path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    filename = os.path.basename(path) or "file"
    return StreamingResponse(
        stream,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/sandbox/{sandbox_id}/files/list")
async def list_files(
    sandbox_id: str,
    dir: str = Query(..., min_length=1),
    pool: PoolManager = Depends(get_pool),
    backend=Depends(get_backend),
) -> dict:
    sb = await pool.get(sandbox_id)
    _ensure_in_use(sb)
    await pool.touch(sandbox_id)

    try:
        entries = await backend.list_files(sandbox_id, dir)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"directory": dir, "files": entries}


@app.post("/sandbox/{sandbox_id}/exec/stream")
async def exec_stream_in_sandbox(
    sandbox_id: str,
    req: ExecRequest,
    pool: PoolManager = Depends(get_pool),
    backend=Depends(get_backend),
) -> StreamingResponse:
    sb = await pool.get(sandbox_id)
    if sb.state.value != "IN_USE":
        raise HTTPException(status_code=409, detail=f"sandbox is {sb.state.value}, not IN_USE")
    await pool.touch(sandbox_id)

    async def event_stream():
        async for chunk in backend.exec_stream(sandbox_id, req.command, req.timeout_seconds):
            yield f"data: {json.dumps(chunk)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/sandbox/{sandbox_id}/agent/run")
async def agent_run(
    sandbox_id: str,
    req: AgentRunRequest,
    pool: PoolManager = Depends(get_pool),
    backend=Depends(get_backend),
) -> StreamingResponse:
    sb = await pool.get(sandbox_id)
    _ensure_in_use(sb)
    await pool.touch(sandbox_id)

    payload = json.dumps({"task": req.task}).encode()

    async def event_stream():
        try:
            async for event in backend.agent_run_stream(
                sandbox_id, payload, AGENT_RUN_TIMEOUT_SECONDS
            ):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as exc:
            log.exception("agent_run failed for %s", sandbox_id)
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
