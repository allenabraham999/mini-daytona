from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class SandboxHandle:
    """Opaque handle returned by the backend after a sandbox boots."""

    sandbox_id: str
    host: str
    port: int
    ssh_user: str
    ssh_key_fingerprint: str

    def connection_details(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "ssh_user": self.ssh_user,
            "ssh_key_fingerprint": self.ssh_key_fingerprint,
        }


@dataclass
class ExecResult:
    exit_code: int
    stdout: str
    stderr: str


class SandboxBackend(ABC):
    """Pluggable backend interface. The mock implementation lives next door;
    a Firecracker implementation will subclass this and replace it via
    SANDBOX_BACKEND=firecracker."""

    @abstractmethod
    async def create(self) -> SandboxHandle: ...

    @abstractmethod
    async def destroy(self, sandbox_id: str) -> None: ...

    @abstractmethod
    async def health_check(self, sandbox_id: str) -> bool: ...

    @abstractmethod
    async def exec(self, sandbox_id: str, command: str, timeout_seconds: int) -> ExecResult: ...
