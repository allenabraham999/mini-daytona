# Mini Daytona

> Self-hosted sandbox orchestration with sub-220ms boot times.

## Benchmarks

| Configuration                                     | Boot time     |
| ------------------------------------------------- | ------------- |
| Incus cold boot (no optimization)                 | 6000 ms       |
| Typical self-hosted Incus                         | 1000 ms       |
| **Incus pre-warm pool (our solution)**            | **213 ms** ✅ |
| **Firecracker snapshot restore**                  | **114 ms** 🔥 |

## Features

- Sub-220ms sandbox boot times
- Streaming exec (SSE)
- File I/O (upload / download / list)
- AI agent runtime (OpenAI-powered)
- Threshold-based dynamic pool scaling (scale to zero)
- Pluggable backends (mock / incus / firecracker)
- JWT auth + per-user rate limiting
- Python SDK

## Architecture

```
User ──▶ API Gateway (8000) ──▶ Orchestrator (9000) ──▶ Incus / Firecracker
```

- **API Gateway** — public FastAPI surface. JWT auth, rate limiting, request validation.
- **Orchestrator** — internal pool manager. State machine, idle reaper, health checks, backend driver.
- **Backend** — pluggable: `mock`, `incus`, or `firecracker`.

## Quick start

```bash
git clone <this-repo> mini-daytona
cd mini-daytona
cp .env.example .env          # fill in JWT_SECRET, OPENAI_API_KEY, etc.
docker compose up --build
```

Mint a dev token and drive the API:

```bash
TOKEN=$(python scripts/mint_token.py alice)

SB=$(curl -s -X POST http://localhost:8000/sandbox/create \
  -H "Authorization: Bearer $TOKEN" | jq -r .sandbox_id)

curl -s -X POST http://localhost:8000/sandbox/$SB/exec \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"command":"echo hello"}'

curl -s -X DELETE http://localhost:8000/sandbox/$SB \
  -H "Authorization: Bearer $TOKEN"
```

## API

```
POST   /sandbox/create                    create sandbox (assigns from pool)
GET    /sandbox/{id}/status               current state
POST   /sandbox/{id}/exec                 run command, return {stdout, stderr, exit_code}
POST   /sandbox/{id}/exec/stream          run command, stream stdout/stderr via SSE
POST   /sandbox/{id}/files                upload file (multipart)
GET    /sandbox/{id}/files?path=          download file
GET    /sandbox/{id}/files/list?dir=      list directory
POST   /sandbox/{id}/agent/run            run AI agent task (SSE)
DELETE /sandbox/{id}                      destroy sandbox
GET    /pool/stats                        pool size, available count, in-use count
```

All endpoints require `Authorization: Bearer <jwt>`.

## SDK

```python
from mini_daytona import MiniDaytona

client = MiniDaytona("http://localhost:8000", token="your-token")

sb = client.create_sandbox()

result = client.exec(sb["sandbox_id"], "echo hello")
print(result["stdout"])

for event in client.exec_stream(sb["sandbox_id"], "ls -la"):
    print(event)

client.destroy_sandbox(sb["sandbox_id"])
```

## Pool scaling

The pool maintains a buffer of pre-warmed sandboxes so `create` returns in ~213ms instead of paying full cold-boot cost. A control loop watches `available_ratio = available / total` and resizes:

- `available_ratio < 0.3` → scale **up by 2** (load is rising; replenish faster than it drains)
- `available_ratio > 0.7` → scale **down by 1** (excess capacity; trim slowly to absorb spikes)
- Bounded by `POOL_MIN_SIZE` and `POOL_MAX_SIZE`
- **Scale to zero**: set `POOL_MIN_SIZE=0` to drop the pool to nothing when idle. The first request after idle pays cold-boot cost; subsequent requests grow the pool back up to `POOL_MAX_SIZE` under load.

## Branches

| Branch                 | Backend                              | Boot   |
| ---------------------- | ------------------------------------ | ------ |
| `main`                 | mock backend (dev / CI)              | n/a    |
| `feature/incus`        | Incus + pre-warm pool                | 213 ms |
| `feature/firecracker`  | Firecracker snapshot restore         | 114 ms |

## What's next

- Predictive scaling based on usage patterns (replace reactive thresholds with a forecast)
- Snapshot API to pause/resume agents mid-task
- Persistent shell sessions (long-lived PTY across exec calls)
- Multi-region support
