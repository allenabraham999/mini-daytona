from __future__ import annotations

from pydantic import BaseModel, Field


class AssignRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)


class ExecRequest(BaseModel):
    command: str = Field(min_length=1, max_length=10_000)
    timeout_seconds: int = Field(default=30, ge=1, le=600)


class AgentRunRequest(BaseModel):
    task: str = Field(min_length=1, max_length=10_000)


class ConnectionDetails(BaseModel):
    host: str
    port: int
    ssh_user: str
    ssh_key_fingerprint: str


class SandboxView(BaseModel):
    sandbox_id: str
    state: str
    user_id: str | None
    connection: ConnectionDetails | None
    created_at: float
    last_active_at: float
    last_health_at: float
    healthy: bool


class ExecResponse(BaseModel):
    exit_code: int
    stdout: str
    stderr: str


class PoolStats(BaseModel):
    total: int
    min_size: int
    max_size: int
    by_state: dict[str, int]
    unhealthy: int
