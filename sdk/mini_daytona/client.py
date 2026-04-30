from __future__ import annotations

import json
import os
from typing import Generator

import requests


class MiniDaytonaError(Exception):
    def __init__(self, status_code: int, body: str):
        super().__init__(f"HTTP {status_code}: {body}")
        self.status_code = status_code
        self.body = body


class MiniDaytona:
    def __init__(self, url: str, token: str):
        self.url = url.rstrip("/")
        self.token = token
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {token}"})

    def _check(self, response: requests.Response) -> None:
        if not (200 <= response.status_code < 300):
            raise MiniDaytonaError(response.status_code, response.text)

    def create_sandbox(self) -> dict:
        r = self._session.post(f"{self.url}/sandbox/create", timeout=120)
        self._check(r)
        return r.json()

    def destroy_sandbox(self, sandbox_id: str) -> None:
        r = self._session.delete(f"{self.url}/sandbox/{sandbox_id}", timeout=30)
        self._check(r)

    def exec(self, sandbox_id: str, command: str, timeout: int = 30) -> dict:
        r = self._session.post(
            f"{self.url}/sandbox/{sandbox_id}/exec",
            json={"command": command, "timeout_seconds": timeout},
            timeout=timeout + 30,
        )
        self._check(r)
        return r.json()

    def exec_stream(
        self, sandbox_id: str, command: str, timeout: int = 30
    ) -> Generator[dict, None, None]:
        r = self._session.post(
            f"{self.url}/sandbox/{sandbox_id}/exec/stream",
            json={"command": command, "timeout_seconds": timeout},
            stream=True,
            timeout=timeout + 30,
        )
        self._check(r)
        yield from _iter_sse_events(r)

    def upload_files(self, sandbox_id: str, file_paths: list[str]) -> dict:
        files = []
        try:
            for path in file_paths:
                fh = open(path, "rb")
                files.append(("files", (os.path.basename(path), fh, "application/octet-stream")))
            r = self._session.post(
                f"{self.url}/sandbox/{sandbox_id}/files",
                files=files,
                timeout=300,
            )
            self._check(r)
            return r.json()
        finally:
            for _, (_, fh, _) in files:
                fh.close()

    def download_file(self, sandbox_id: str, path: str) -> bytes:
        r = self._session.get(
            f"{self.url}/sandbox/{sandbox_id}/files",
            params={"path": path},
            timeout=300,
        )
        self._check(r)
        return r.content

    def list_files(self, sandbox_id: str, dir: str = "/tmp/uploads") -> dict:
        r = self._session.get(
            f"{self.url}/sandbox/{sandbox_id}/files/list",
            params={"dir": dir},
            timeout=30,
        )
        self._check(r)
        return r.json()

    def run_agent(self, sandbox_id: str, task: str) -> Generator[dict, None, None]:
        r = self._session.post(
            f"{self.url}/sandbox/{sandbox_id}/agent/run",
            json={"task": task},
            stream=True,
            timeout=600,
        )
        self._check(r)
        yield from _iter_sse_events(r)

    def pool_stats(self) -> dict:
        r = self._session.get(f"{self.url}/pool/stats", timeout=10)
        self._check(r)
        return r.json()


def _iter_sse_events(response: requests.Response) -> Generator[dict, None, None]:
    data_lines: list[str] = []
    for raw in response.iter_lines(decode_unicode=True):
        if raw is None:
            continue
        if raw == "":
            if data_lines:
                payload = "\n".join(data_lines)
                data_lines = []
                try:
                    yield json.loads(payload)
                except json.JSONDecodeError:
                    yield {"raw": payload}
            continue
        if raw.startswith(":"):
            continue
        if raw.startswith("data:"):
            data_lines.append(raw[5:].lstrip())
    if data_lines:
        payload = "\n".join(data_lines)
        try:
            yield json.loads(payload)
        except json.JSONDecodeError:
            yield {"raw": payload}
