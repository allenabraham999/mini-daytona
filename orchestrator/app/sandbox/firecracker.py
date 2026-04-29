from __future__ import annotations

import asyncio
import http.client
import itertools
import json
import os
import socket
import time
import uuid
from dataclasses import dataclass
from typing import Any

from .base import ExecResult, SandboxBackend, SandboxHandle

_BINARY    = os.environ.get("FIRECRACKER_BINARY",    "/usr/local/bin/firecracker")
_SNAP_FILE = os.environ.get("FIRECRACKER_SNAP_FILE", "/home/ubuntu/firecracker-demo/snap.file")
_SNAP_MEM  = os.environ.get("FIRECRACKER_SNAP_MEM",  "/home/ubuntu/firecracker-demo/snap.mem")
_SSH_USER  = os.environ.get("FIRECRACKER_SSH_USER",  "root")
_SSH_PORT  = int(os.environ.get("FIRECRACKER_SSH_PORT", "22"))


class _UnixSocketConnection(http.client.HTTPConnection):
    """HTTPConnection routed over a Unix domain socket."""

    def __init__(self, socket_path: str) -> None:
        super().__init__("localhost")
        self._socket_path = socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self._socket_path)
        self.sock = sock


@dataclass
class _VMRecord:
    sandbox_id: str
    process: asyncio.subprocess.Process
    socket_path: str
    vm_ip: str
    tap_name: str
    ip_slot: int


class FirecrackerSandboxBackend(SandboxBackend):
    """On-demand Firecracker backend.

    Every create() restores a fresh VM from the on-disk snapshot in ~8ms.
    No VMs sit idle between requests — pure on-demand with zero idle overhead.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._vms: dict[str, _VMRecord] = {}
        self._ip_counter = itertools.count(1)
        self._free_ip_slots: list[int] = []
        self._boot_times: list[float] = []  # milliseconds, appended under lock

    # ------------------------------------------------------------------ #
    # internal helpers
    # ------------------------------------------------------------------ #

    def _alloc_ip_slot(self) -> int:
        # Called synchronously (no await), so no concurrent mutation risk.
        if self._free_ip_slots:
            return self._free_ip_slots.pop()
        return next(self._ip_counter)

    def _release_ip_slot(self, slot: int) -> None:
        self._free_ip_slots.append(slot)

    @staticmethod
    def _tap_name(sandbox_id: str) -> str:
        return f"tap-{sandbox_id}"[:15]

    @staticmethod
    def _mac_from_slot(slot: int) -> str:
        """Locally-administered MAC derived from the IP slot."""
        return f"02:fc:00:00:{(slot >> 8) & 0xFF:02x}:{slot & 0xFF:02x}"

    # Synchronous; called via run_in_executor so the event loop isn't blocked.
    @staticmethod
    def _fc_request(
        socket_path: str, method: str, path: str, body: dict | None = None
    ) -> tuple[int, Any]:
        conn = _UnixSocketConnection(socket_path)
        try:
            headers: dict[str, str] = {"Accept": "application/json"}
            payload: bytes | None = None
            if body is not None:
                payload = json.dumps(body).encode()
                headers["Content-Type"] = "application/json"
            conn.request(method, path, body=payload, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
            data = json.loads(raw) if raw else {}
            return resp.status, data
        finally:
            conn.close()

    async def _api(
        self, socket_path: str, method: str, path: str, body: dict | None = None
    ) -> tuple[int, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._fc_request, socket_path, method, path, body
        )

    async def _run(self, *args: str) -> int:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        return await proc.wait()

    # ------------------------------------------------------------------ #
    # create
    # ------------------------------------------------------------------ #

    async def create(self) -> SandboxHandle:
        sandbox_id = f"sbx-{uuid.uuid4().hex[:12]}"
        socket_path = f"/tmp/firecracker-{sandbox_id}.socket"
        ip_slot   = self._alloc_ip_slot()
        tap_name  = self._tap_name(sandbox_id)
        host_ip   = f"172.16.{ip_slot}.1"
        vm_ip     = f"172.16.{ip_slot}.2"

        t_start = time.monotonic()
        process: asyncio.subprocess.Process | None = None
        tap_created = False

        try:
            # 1. Start Firecracker process
            process = await asyncio.create_subprocess_exec(
                _BINARY,
                "--api-sock", socket_path,
                "--log-level", "Error",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )

            # Wait for the API socket to appear (up to 500 ms)
            for _ in range(50):
                await asyncio.sleep(0.01)
                if os.path.exists(socket_path):
                    break
            else:
                raise RuntimeError(
                    f"Firecracker socket {socket_path} did not appear within 500 ms"
                )

            # 2. Create TAP interface on the host
            await self._run("ip", "tuntap", "add", tap_name, "mode", "tap")
            tap_created = True
            await self._run("ip", "addr", "add", f"{host_ip}/30", "dev", tap_name)
            await self._run("ip", "link", "set", tap_name, "up")

            # 3. Tell Firecracker about the TAP (must happen before snapshot load)
            status, resp = await self._api(socket_path, "PUT", "/network-interfaces/eth0", {
                "iface_id": "eth0",
                "guest_mac": self._mac_from_slot(ip_slot),
                "host_dev_name": tap_name,
            })
            if status not in (200, 204):
                raise RuntimeError(
                    f"PUT /network-interfaces failed: HTTP {status} — {resp}"
                )

            # 4. Restore snapshot and resume the VM
            status, resp = await self._api(socket_path, "PUT", "/snapshot/load", {
                "snapshot_path": _SNAP_FILE,
                "mem_backend": {
                    "backend_path": _SNAP_MEM,
                    "backend_type": "File",
                },
                "enable_diff_snapshots": False,
                "resume_vm": True,
            })
            if status not in (200, 204):
                raise RuntimeError(
                    f"PUT /snapshot/load failed: HTTP {status} — {resp}"
                )

            boot_ms = (time.monotonic() - t_start) * 1000.0

            async with self._lock:
                self._boot_times.append(boot_ms)
                self._vms[sandbox_id] = _VMRecord(
                    sandbox_id=sandbox_id,
                    process=process,
                    socket_path=socket_path,
                    vm_ip=vm_ip,
                    tap_name=tap_name,
                    ip_slot=ip_slot,
                )

            return SandboxHandle(
                sandbox_id=sandbox_id,
                host=vm_ip,
                port=_SSH_PORT,
                ssh_user=_SSH_USER,
                ssh_key_fingerprint="",
            )

        except Exception:
            # Best-effort cleanup on create failure
            if process is not None:
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=3.0)
                except Exception:
                    pass
            if tap_created:
                await self._run("ip", "link", "delete", tap_name)
            try:
                os.unlink(socket_path)
            except FileNotFoundError:
                pass
            self._release_ip_slot(ip_slot)
            raise

    # ------------------------------------------------------------------ #
    # exec
    # ------------------------------------------------------------------ #

    async def exec(self, sandbox_id: str, command: str, timeout_seconds: int) -> ExecResult:
        async with self._lock:
            record = self._vms.get(sandbox_id)
        if record is None:
            return ExecResult(
                exit_code=127, stdout="", stderr=f"sandbox {sandbox_id} not found"
            )

        try:
            proc = await asyncio.create_subprocess_exec(
                "ssh",
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "BatchMode=yes",
                "-o", f"ConnectTimeout={min(timeout_seconds, 10)}",
                "-p", str(_SSH_PORT),
                f"{_SSH_USER}@{record.vm_ip}",
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout_b, stderr_b = await asyncio.wait_for(
                    proc.communicate(), timeout=float(timeout_seconds)
                )
            except asyncio.TimeoutError:
                proc.kill()
                return ExecResult(exit_code=124, stdout="", stderr="command timed out")

            return ExecResult(
                exit_code=proc.returncode if proc.returncode is not None else 0,
                stdout=stdout_b.decode(errors="replace"),
                stderr=stderr_b.decode(errors="replace"),
            )
        except Exception as exc:
            return ExecResult(exit_code=1, stdout="", stderr=str(exc))

    # ------------------------------------------------------------------ #
    # destroy
    # ------------------------------------------------------------------ #

    async def destroy(self, sandbox_id: str) -> None:
        async with self._lock:
            record = self._vms.pop(sandbox_id, None)
        if record is None:
            return
        await self._teardown(record)

    async def _teardown(self, record: _VMRecord) -> None:
        # SIGTERM Firecracker; escalate to SIGKILL if it lingers
        try:
            record.process.terminate()
            try:
                await asyncio.wait_for(record.process.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                record.process.kill()
        except ProcessLookupError:
            pass

        await self._run("ip", "link", "delete", record.tap_name)

        try:
            os.unlink(record.socket_path)
        except FileNotFoundError:
            pass

        self._release_ip_slot(record.ip_slot)

    # ------------------------------------------------------------------ #
    # health_check
    # ------------------------------------------------------------------ #

    async def health_check(self, sandbox_id: str) -> bool:
        async with self._lock:
            record = self._vms.get(sandbox_id)
        if record is None:
            return False
        if record.process.returncode is not None:
            # Process has already exited
            return False
        try:
            status, _ = await self._api(record.socket_path, "GET", "/")
            return status == 200
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    # metrics (Firecracker-specific, not on the base interface)
    # ------------------------------------------------------------------ #

    def boot_metrics(self) -> dict:
        """Return aggregate boot-time statistics and current active VM count.

        Safe to call from sync context; reads are eventually-consistent.
        """
        times = self._boot_times
        count = len(times)
        return {
            "boot_times": {
                "count": count,
                "min_ms":  round(min(times), 2) if times else 0.0,
                "max_ms":  round(max(times), 2) if times else 0.0,
                "avg_ms":  round(sum(times) / count, 2) if times else 0.0,
            },
            "active_vms": len(self._vms),
        }
