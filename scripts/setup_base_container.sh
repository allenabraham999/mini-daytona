#!/usr/bin/env bash
# Provisions the Incus base-container with everything the agent needs:
#   - python3 + pip
#   - openai SDK
#   - /usr/local/bin/agent (the agent.py shipped in this repo)
#
# Run this once on the host that runs the orchestrator. Re-run it after
# editing orchestrator/app/sandbox/agent.py to refresh the snapshot.
#
# Usage: scripts/setup_base_container.sh [base-container-name]
set -euo pipefail

BASE="${1:-base-container}"
SNAP="snap0"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AGENT_SRC="$REPO_ROOT/orchestrator/app/sandbox/agent.py"

if [[ ! -f "$AGENT_SRC" ]]; then
  echo "agent.py not found at $AGENT_SRC" >&2
  exit 1
fi

if ! incus info "$BASE" >/dev/null 2>&1; then
  echo "container '$BASE' does not exist — create it first (e.g. 'incus launch images:ubuntu/22.04 $BASE')" >&2
  exit 1
fi

echo "==> ensuring $BASE is running"
state=$(incus list "$BASE" -c s --format csv)
if [[ "$state" != "RUNNING" ]]; then
  incus start "$BASE"
  # Give networking a moment so apt can resolve.
  sleep 5
fi

echo "==> installing python3 + pip"
incus exec "$BASE" -- bash -c '
  set -e
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq python3 python3-pip python3-venv ca-certificates >/dev/null
'

echo "==> installing openai SDK"
incus exec "$BASE" -- bash -c '
  set -e
  pip3 install --quiet openai
'

echo "==> verifying openai import"
incus exec "$BASE" -- python3 -c "import openai; print('ok')"

echo "==> pushing agent.py to /usr/local/bin/agent"
incus file push "$AGENT_SRC" "$BASE/usr/local/bin/agent"
incus exec "$BASE" -- chmod +x /usr/local/bin/agent

echo "==> stopping $BASE and recreating snapshot $SNAP"
incus stop "$BASE"
sudo incus snapshot delete "$BASE" "$SNAP" 2>/dev/null || true
sudo incus snapshot "$BASE" "$SNAP"

echo "done. base-container/$SNAP is ready."
