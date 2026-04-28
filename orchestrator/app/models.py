from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class SandboxState(str, Enum):
    PENDING = "PENDING"
    STARTING = "STARTING"
    READY = "READY"
    IN_USE = "IN_USE"
    TERMINATING = "TERMINATING"
    DESTROYED = "DESTROYED"


# Forward transitions only — anything not in this map is rejected.
_VALID_TRANSITIONS: dict[SandboxState, set[SandboxState]] = {
    SandboxState.PENDING: {SandboxState.STARTING, SandboxState.TERMINATING},
    SandboxState.STARTING: {SandboxState.READY, SandboxState.TERMINATING},
    SandboxState.READY: {SandboxState.IN_USE, SandboxState.TERMINATING},
    SandboxState.IN_USE: {SandboxState.READY, SandboxState.TERMINATING},
    SandboxState.TERMINATING: {SandboxState.DESTROYED},
    SandboxState.DESTROYED: set(),
}


def can_transition(src: SandboxState, dst: SandboxState) -> bool:
    return dst in _VALID_TRANSITIONS[src]


@dataclass
class Sandbox:
    sandbox_id: str
    state: SandboxState = SandboxState.PENDING
    user_id: str | None = None
    host: str | None = None
    port: int | None = None
    ssh_user: str | None = None
    ssh_key_fingerprint: str | None = None
    created_at: float = field(default_factory=time.time)
    last_active_at: float = field(default_factory=time.time)
    last_health_at: float = field(default_factory=time.time)
    healthy: bool = True

    def transition(self, dst: SandboxState) -> None:
        if not can_transition(self.state, dst):
            raise ValueError(f"illegal transition {self.state} -> {dst}")
        self.state = dst

    def connection_details(self) -> dict | None:
        if self.host is None:
            return None
        return {
            "host": self.host,
            "port": self.port,
            "ssh_user": self.ssh_user,
            "ssh_key_fingerprint": self.ssh_key_fingerprint,
        }

    def to_dict(self) -> dict:
        return {
            "sandbox_id": self.sandbox_id,
            "state": self.state.value,
            "user_id": self.user_id,
            "connection": self.connection_details(),
            "created_at": self.created_at,
            "last_active_at": self.last_active_at,
            "last_health_at": self.last_health_at,
            "healthy": self.healthy,
        }
