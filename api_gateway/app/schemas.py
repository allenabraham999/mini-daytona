from __future__ import annotations

from pydantic import BaseModel, Field


class CreateSandboxRequest(BaseModel):
    pass


class ConnectionDetails(BaseModel):
    host: str
    port: int
    ssh_user: str
    ssh_key_fingerprint: str


class CreateSandboxResponse(BaseModel):
    sandbox_id: str
    state: str
    connection: ConnectionDetails


class SandboxStatus(BaseModel):
    sandbox_id: str
    state: str
    healthy: bool
    user_id: str | None
    last_active_at: float


class ExecRequest(BaseModel):
    command: str = Field(min_length=1, max_length=10_000)
    timeout_seconds: int = Field(default=30, ge=1, le=600)


class ExecResponse(BaseModel):
    exit_code: int
    stdout: str
    stderr: str


class AgentRunRequest(BaseModel):
    task: str = Field(min_length=1, max_length=10_000)
    anthropic_api_key: str = Field(min_length=1)
    model: str | None = None
    max_turns: int = Field(default=10, ge=1, le=50)
    timeout_seconds: int = Field(default=300, ge=10, le=1800)
