# Mini Daytona

Skeleton sandbox orchestration platform with two services:

- **api_gateway** (port `8000`) — public FastAPI surface. JWT-authenticated, rate-limited, validates requests, talks to the orchestrator over HTTP.
- **orchestrator** (port `9000`) — internal pool manager. Tracks sandbox state in memory, runs the idle reaper and health-check loop, exposes a small REST surface for the gateway.

VM/Firecracker logic is stubbed via `MockSandboxBackend`. Real integration drops in by implementing `SandboxBackend` (see `orchestrator/app/sandbox/base.py`) and wiring it in `orchestrator/app/sandbox/__init__.py::build_backend`.

## Layout

```
api_gateway/         # public FastAPI service
  app/
    main.py            # routes + exception handlers
    auth.py            # JWT bearer dependency
    rate_limit.py      # per-user token bucket middleware
    orchestrator_client.py
    schemas.py
orchestrator/        # internal service
  app/
    main.py            # internal REST surface, lifespan wiring
    pool.py            # PoolManager: getAvailable/assign/release/getPoolStats
    models.py          # Sandbox dataclass + state machine
    loops.py           # idle reaper + health check loop
    sandbox/
      base.py          # SandboxBackend abstract interface
      mock.py          # MockSandboxBackend (replace with Firecracker impl)
docker-compose.yml
.env.example
scripts/mint_token.py  # dev helper to issue JWTs
```

## Run locally

```bash
cp .env.example .env
docker compose up --build
```

Both services come up; the gateway waits for the orchestrator's `/healthz` to pass.

## Issuing a token

```bash
pip install pyjwt
export $(grep -v '^#' .env | xargs)   # load JWT_SECRET into shell
TOKEN=$(python scripts/mint_token.py alice)
```

## End-to-end smoke test

```bash
# 1. create a sandbox
curl -s -X POST http://localhost:8000/sandbox/create \
  -H "Authorization: Bearer $TOKEN" | tee /tmp/sb.json
SB=$(jq -r .sandbox_id /tmp/sb.json)

# 2. status
curl -s http://localhost:8000/sandbox/$SB/status \
  -H "Authorization: Bearer $TOKEN"

# 3. exec
curl -s -X POST http://localhost:8000/sandbox/$SB/exec \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"command":"echo hi"}'

# 4. destroy
curl -s -X DELETE http://localhost:8000/sandbox/$SB \
  -H "Authorization: Bearer $TOKEN" -i
```

## Lifecycle

```
PENDING → STARTING → READY → IN_USE → TERMINATING → DESTROYED
                       ↑________|
```

Transitions are enforced in `orchestrator/app/models.py`. Anything illegal raises immediately so bad state can't propagate.

## Background loops

- **Idle reaper** runs every 15s, terminates IN_USE sandboxes idle past `IDLE_TIMEOUT_SECONDS` (default 600).
- **Health check loop** runs every `HEALTH_CHECK_INTERVAL_SECONDS` (default 30), pings each active sandbox via the backend, marks failures and tears them down.

## Agent runtime (Incus)

The Incus backend ships an OpenAI-powered agent that runs inside the sandbox.
The agent takes a task on stdin, calls the OpenAI API with a `bash` tool, and
streams events back through SSE. The `OPENAI_API_KEY` is read from the
orchestrator's environment and forwarded into the sandbox at exec time, so it
never travels over the public HTTP surface.

Provision the base container once on the host (installs python3 + openai SDK,
pushes `agent.py` to `/usr/local/bin/agent`, refreshes `snap0`):

```bash
scripts/setup_base_container.sh             # uses 'base-container' by default
# scripts/setup_base_container.sh my-base   # or pass a different name
```

Re-run the script after editing `orchestrator/app/sandbox/agent.py` to refresh
the pool's snapshot.

Then exercise it end-to-end:

```bash
SB=$(curl -s -X POST http://localhost:8000/sandbox/create \
  -H "Authorization: Bearer $TOKEN" | jq -r .sandbox_id)

curl -N -X POST http://localhost:8000/sandbox/$SB/agent/run \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"task":"write a bash script that creates 5 random files in /tmp and list them"}'
```

The response is an SSE stream of JSON events: `start`, `text`, `tool_use`,
`tool_result`, then a final `complete` event with `output` and `files_created`.

## Plugging in Firecracker

1. Add `orchestrator/app/sandbox/firecracker.py` with a `FirecrackerSandboxBackend(SandboxBackend)` implementation.
2. Wire it in `orchestrator/app/sandbox/__init__.py::build_backend` under `name == "firecracker"`.
3. Set `SANDBOX_BACKEND=firecracker` in `.env`.

No other code in the orchestrator or gateway should need to change.
