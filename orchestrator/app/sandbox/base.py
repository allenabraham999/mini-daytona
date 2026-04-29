from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any


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

    async def exec_stream(
        self, sandbox_id: str, command: str, timeout_seconds: int
    ) -> AsyncIterator[dict]:
        result = await self.exec(sandbox_id, command, timeout_seconds)
        for line in result.stdout.splitlines():
            yield {"type": "stdout", "data": line}
        for line in result.stderr.splitlines():
            yield {"type": "stderr", "data": line}
        yield {"type": "exit", "data": str(result.exit_code)}

    @abstractmethod
    async def upload_file(
        self, sandbox_id: str, local_path: str, dest_path: str
    ) -> None:
        """Push a file from `local_path` on the host to `dest_path` inside the sandbox."""

    @abstractmethod
    async def download_file(
        self, sandbox_id: str, path: str
    ) -> AsyncIterator[bytes]:
        """Return an async iterator that yields chunks of the file at `path`.

        Implementations should validate existence before returning the iterator
        so callers can map FileNotFoundError to a 404 cleanly."""

    @abstractmethod
    async def list_files(self, sandbox_id: str, directory: str) -> list[dict[str, Any]]:
        """Return metadata for entries in `directory` inside the sandbox."""
