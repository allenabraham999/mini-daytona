# mini-daytona-sdk

Python SDK for [mini-daytona](https://github.com/) sandboxes.

## Install

```bash
pip install mini-daytona-sdk
```

Or install from source:

```bash
pip install ./sdk
```

## Usage

```python
from mini_daytona import MiniDaytona

client = MiniDaytona("http://localhost:8000", token="your-jwt-token")
sb = client.create_sandbox()
result = client.exec(sb["sandbox_id"], "echo hello")
print(result["stdout"])
client.destroy_sandbox(sb["sandbox_id"])
```

## API

```python
client = MiniDaytona(url, token)

client.create_sandbox() -> dict
client.destroy_sandbox(sandbox_id)
client.exec(sandbox_id, command, timeout=30) -> dict
client.exec_stream(sandbox_id, command, timeout=30)        # generator of dicts
client.upload_files(sandbox_id, ["./a.txt", "./b.txt"]) -> dict
client.download_file(sandbox_id, "/tmp/uploads/a.txt") -> bytes
client.list_files(sandbox_id, dir="/tmp/uploads") -> dict
client.run_agent(sandbox_id, "summarize foo.txt")          # generator of dicts
client.pool_stats() -> dict
```

### Streaming

`exec_stream` and `run_agent` parse SSE events and yield each event as a dict.

```python
for event in client.exec_stream(sb["sandbox_id"], "for i in 1 2 3; do echo $i; sleep 1; done"):
    print(event)

for event in client.run_agent(sb["sandbox_id"], "list files in /tmp/uploads"):
    print(event)
```

### Errors

Non-2xx responses raise `mini_daytona.client.MiniDaytonaError`, which carries
`status_code` and `body` attributes.
