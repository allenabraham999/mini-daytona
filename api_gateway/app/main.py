from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse

from .auth import Principal, authenticate
from .config import settings
from .orchestrator_client import OrchestratorClient
from .rate_limit import RateLimiterMiddleware
from .schemas import (
    ConnectionDetails,
    CreateSandboxResponse,
    ExecRequest,
    ExecResponse,
    SandboxStatus,
)

logging.basicConfig(level=settings.log_level)
log = logging.getLogger("api-gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.orchestrator = OrchestratorClient()
    try:
        yield
    finally:
        await app.state.orchestrator.close()


app = FastAPI(title="sandbox-api-gateway", lifespan=lifespan)
app.add_middleware(RateLimiterMiddleware, exempt_paths=("/healthz",))


def get_orchestrator() -> OrchestratorClient:
    return app.state.orchestrator


@app.exception_handler(RequestValidationError)
async def _validation_handler(_: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"error": "validation_error", "detail": exc.errors()},
    )


@app.exception_handler(HTTPException)
async def _http_exception_handler(_: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": _slug_for(exc.status_code), "detail": exc.detail},
        headers=exc.headers or {},
    )


def _slug_for(code: int) -> str:
    return {
        400: "bad_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        409: "conflict",
        422: "validation_error",
        429: "rate_limited",
        502: "bad_gateway",
        503: "service_unavailable",
    }.get(code, "error")


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


@app.post(
    "/sandbox/create",
    response_model=CreateSandboxResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_sandbox(
    principal: Principal = Depends(authenticate),
    orch: OrchestratorClient = Depends(get_orchestrator),
) -> CreateSandboxResponse:
    data = await orch.assign(principal.user_id)
    if not data.get("connection"):
        raise HTTPException(status_code=502, detail="orchestrator returned no connection details")
    return CreateSandboxResponse(
        sandbox_id=data["sandbox_id"],
        state=data["state"],
        connection=ConnectionDetails(**data["connection"]),
    )


@app.delete("/sandbox/{sandbox_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
async def destroy_sandbox(
    sandbox_id: str,
    principal: Principal = Depends(authenticate),
    orch: OrchestratorClient = Depends(get_orchestrator),
) -> None:
    sb = await orch.get(sandbox_id)
    _ensure_owner(sb, principal)
    await orch.release(sandbox_id)


@app.get("/sandbox/{sandbox_id}/status", response_model=SandboxStatus)
async def get_status(
    sandbox_id: str,
    principal: Principal = Depends(authenticate),
    orch: OrchestratorClient = Depends(get_orchestrator),
) -> SandboxStatus:
    sb = await orch.get(sandbox_id)
    _ensure_owner(sb, principal)
    return SandboxStatus(
        sandbox_id=sb["sandbox_id"],
        state=sb["state"],
        healthy=sb["healthy"],
        user_id=sb.get("user_id"),
        last_active_at=sb["last_active_at"],
    )


@app.post("/sandbox/{sandbox_id}/exec", response_model=ExecResponse)
async def exec_in_sandbox(
    sandbox_id: str,
    body: ExecRequest,
    principal: Principal = Depends(authenticate),
    orch: OrchestratorClient = Depends(get_orchestrator),
) -> ExecResponse:
    sb = await orch.get(sandbox_id)
    _ensure_owner(sb, principal)
    data = await orch.exec(sandbox_id, body.command, body.timeout_seconds)
    return ExecResponse(**data)


@app.post("/sandbox/{sandbox_id}/exec/stream")
async def exec_stream_in_sandbox(
    sandbox_id: str,
    body: ExecRequest,
    principal: Principal = Depends(authenticate),
    orch: OrchestratorClient = Depends(get_orchestrator),
) -> StreamingResponse:
    sb = await orch.get(sandbox_id)
    _ensure_owner(sb, principal)

    async def proxy_stream():
        async for chunk in orch.exec_stream(sandbox_id, body.command, body.timeout_seconds):
            yield chunk

    return StreamingResponse(proxy_stream(), media_type="text/event-stream")


@app.post("/sandbox/{sandbox_id}/files", status_code=status.HTTP_201_CREATED)
async def upload_files(
    sandbox_id: str,
    files: list[UploadFile] = File(...),
    principal: Principal = Depends(authenticate),
    orch: OrchestratorClient = Depends(get_orchestrator),
) -> dict:
    sb = await orch.get(sandbox_id)
    _ensure_owner(sb, principal)

    payload: list[tuple[str, bytes, str]] = []
    for upload in files:
        if not upload.filename:
            continue
        content = await upload.read()
        payload.append(
            (upload.filename, content, upload.content_type or "application/octet-stream")
        )
    if not payload:
        raise HTTPException(status_code=400, detail="no files in upload")
    return await orch.upload_files(sandbox_id, payload)


@app.get("/sandbox/{sandbox_id}/files")
async def download_file(
    sandbox_id: str,
    path: str = Query(..., min_length=1),
    principal: Principal = Depends(authenticate),
    orch: OrchestratorClient = Depends(get_orchestrator),
) -> StreamingResponse:
    sb = await orch.get(sandbox_id)
    _ensure_owner(sb, principal)

    filename = os.path.basename(path) or "file"

    # Awaits orchestrator headers + status so a 404/409 surfaces here as an
    # HTTPException rather than as a 200 with an error body mid-stream.
    stream = await orch.download_file(sandbox_id, path)
    return StreamingResponse(
        stream,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/sandbox/{sandbox_id}/files/list")
async def list_files(
    sandbox_id: str,
    dir: str = Query(..., min_length=1),
    principal: Principal = Depends(authenticate),
    orch: OrchestratorClient = Depends(get_orchestrator),
) -> dict:
    sb = await orch.get(sandbox_id)
    _ensure_owner(sb, principal)
    return await orch.list_files(sandbox_id, dir)


def _ensure_owner(sandbox: dict, principal: Principal) -> None:
    owner = sandbox.get("user_id")
    # An unassigned sandbox should never be reachable by an end user, but
    # guard against it anyway so an orchestrator bug can't leak access.
    if owner is None or owner != principal.user_id:
        raise HTTPException(status_code=403, detail="sandbox does not belong to caller")
