from .base import ExecResult, SandboxBackend, SandboxHandle
from .mock import MockSandboxBackend


def build_backend(name: str) -> SandboxBackend:
    if name == "mock":
        return MockSandboxBackend()
    if name == "firecracker":
        raise NotImplementedError(
            "Firecracker backend is not implemented yet. "
            "Add a FirecrackerSandboxBackend subclass of SandboxBackend and wire it here."
        )
    raise ValueError(f"unknown sandbox backend: {name!r}")


__all__ = [
    "ExecResult",
    "SandboxBackend",
    "SandboxHandle",
    "MockSandboxBackend",
    "build_backend",
]
