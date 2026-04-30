from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
from fastapi import HTTPException

from .config import settings


class OrchestratorClient:
    """Thin async wrapper around the orchestrator's internal REST surface.
    Translates orchestrator errors into HTTPException so route handlers can
    just `await` and not think about transport."""

    def __init__(self, base_url: str | None = None) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url or settings.orchestrator_url,
            timeout=settings.orchestrator_timeout_seconds,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def assign(self, user_id: str) -> dict:
        return await self._post("/sandbox/assign", {"user_id": user_id})

    async def get(self, sandbox_id: str) -> dict:
        return await self._get(f"/sandbox/{sandbox_id}")

    async def release(self, sandbox_id: str) -> None:
        r = await self._client.delete(f"/sandbox/{sandbox_id}")
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail="sandbox not found")
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"orchestrator error: {r.text}")

    async def exec(self, sandbox_id: str, command: str, timeout_seconds: int) -> dict:
        return await self._post(
            f"/sandbox/{sandbox_id}/exec",
            {"command": command, "timeout_seconds": timeout_seconds},
        )

    async def upload_files(
        self,
        sandbox_id: str,
        files: list[tuple[str, bytes, str]],
    ) -> dict:
        """`files` is a list of (filename, content, content_type) tuples."""
        multipart = [
            ("files", (name, content, ctype or "application/octet-stream"))
            for name, content, ctype in files
        ]
        try:
            r = await self._client.post(
                f"/sandbox/{sandbox_id}/files",
                files=multipart,
                timeout=httpx.Timeout(connect=5.0, read=120.0, write=120.0, pool=5.0),
            )
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"orchestrator unreachable: {e}")
        return self._handle(r)

    async def download_file(
        self, sandbox_id: str, path: str
    ) -> AsyncIterator[bytes]:
        """Open the download stream and validate status before returning the
        body iterator. The caller awaits this coroutine and receives an async
        iterator — by that point we've already mapped 404/409/etc. to
        HTTPException, so headers won't be sent prematurely."""
        url = f"/sandbox/{sandbox_id}/files"
        timeout = httpx.Timeout(connect=5.0, read=120.0, write=10.0, pool=5.0)
        request = self._client.build_request(
            "GET", url, params={"path": path}, timeout=timeout
        )
        try:
            response = await self._client.send(request, stream=True)
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"orchestrator unreachable: {e}")

        try:
            if response.status_code == 404:
                await response.aread()
                raise HTTPException(status_code=404, detail="file not found")
            if response.status_code == 409:
                await response.aread()
                try:
                    detail = response.json().get("detail", "conflict")
                except Exception:
                    detail = "conflict"
                raise HTTPException(status_code=409, detail=detail)
            if response.status_code >= 400:
                await response.aread()
                raise HTTPException(
                    status_code=502,
                    detail=f"orchestrator error: {response.status_code}",
                )
        except BaseException:
            await response.aclose()
            raise

        async def _iter() -> AsyncIterator[bytes]:
            try:
                async for chunk in response.aiter_bytes():
                    yield chunk
            finally:
                await response.aclose()

        return _iter()

    async def list_files(self, sandbox_id: str, directory: str) -> dict:
        try:
            r = await self._client.get(
                f"/sandbox/{sandbox_id}/files/list",
                params={"dir": directory},
            )
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"orchestrator unreachable: {e}")
        return self._handle(r)

    async def agent_run_stream(
        self, sandbox_id: str, body: dict
    ) -> AsyncIterator[bytes]:
        url = f"/sandbox/{sandbox_id}/agent/run"
        read_timeout = float(body.get("timeout_seconds", 300)) + 30.0
        timeout = httpx.Timeout(connect=5.0, read=read_timeout, write=10.0, pool=5.0)
        async with self._client.stream("POST", url, json=body, timeout=timeout) as response:
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="sandbox not found")
            if response.status_code == 409:
                await response.aread()
                try:
                    detail = response.json().get("detail", "conflict")
                except Exception:
                    detail = "conflict"
                raise HTTPException(status_code=409, detail=detail)
            if response.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"orchestrator error: {response.status_code}")
            async for chunk in response.aiter_bytes():
                yield chunk

    async def exec_stream(
        self, sandbox_id: str, command: str, timeout_seconds: int
    ) -> AsyncIterator[bytes]:
        url = f"/sandbox/{sandbox_id}/exec/stream"
        body = {"command": command, "timeout_seconds": timeout_seconds}
        timeout = httpx.Timeout(connect=5.0, read=float(timeout_seconds) + 10.0, write=10.0, pool=5.0)
        async with self._client.stream("POST", url, json=body, timeout=timeout) as response:
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="sandbox not found")
            if response.status_code == 409:
                await response.aread()
                try:
                    detail = response.json().get("detail", "conflict")
                except Exception:
                    detail = "conflict"
                raise HTTPException(status_code=409, detail=detail)
            if response.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"orchestrator error: {response.status_code}")
            async for chunk in response.aiter_bytes():
                yield chunk

    # ---- internals ----

    async def _post(self, path: str, body: dict) -> dict:
        try:
            r = await self._client.post(path, json=body)
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"orchestrator unreachable: {e}")
        return self._handle(r)

    async def _get(self, path: str) -> dict:
        try:
            r = await self._client.get(path)
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"orchestrator unreachable: {e}")
        return self._handle(r)

    @staticmethod
    def _handle(r: httpx.Response) -> dict:
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail="sandbox not found")
        if r.status_code == 503:
            raise HTTPException(status_code=503, detail="no capacity in pool")
        if r.status_code == 409:
            try:
                detail = r.json().get("detail", "conflict")
            except Exception:
                detail = "conflict"
            raise HTTPException(status_code=409, detail=detail)
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"orchestrator error: {r.text}")
        return r.json()
