from __future__ import annotations

import asyncio
import secrets
import uuid

from .base import ExecResult, SandboxBackend, SandboxHandle


class MockSandboxBackend(SandboxBackend):
    """In-memory stand-in for a real VM/Firecracker backend. Returns plausible
    fake connection details so the rest of the system can be exercised end-to-end."""

    def __init__(self) -> None:
        self._alive: set[str] = set()
        self._lock = asyncio.Lock()

    async def create(self) -> SandboxHandle:
        await asyncio.sleep(0.05)
        sandbox_id = f"sbx-{uuid.uuid4().hex[:12]}"
        async with self._lock:
            self._alive.add(sandbox_id)
        return SandboxHandle(
            sandbox_id=sandbox_id,
            host=f"10.200.0.{(hash(sandbox_id) % 250) + 2}",
            port=2222,
            ssh_user="sandbox",
            ssh_key_fingerprint=f"SHA256:{secrets.token_urlsafe(32)}",
        )

    async def destroy(self, sandbox_id: str) -> None:
        await asyncio.sleep(0.02)
        async with self._lock:
            self._alive.discard(sandbox_id)

    async def health_check(self, sandbox_id: str) -> bool:
        async with self._lock:
            return sandbox_id in self._alive

    async def exec(self, sandbox_id: str, command: str, timeout_seconds: int) -> ExecResult:
        async with self._lock:
            alive = sandbox_id in self._alive
        if not alive:
            return ExecResult(exit_code=127, stdout="", stderr=f"sandbox {sandbox_id} not running")
        await asyncio.sleep(0.05)
        return ExecResult(
            exit_code=0,
            stdout=f"[mock-exec {sandbox_id}] $ {command}\nok\n",
            stderr="",
        )
