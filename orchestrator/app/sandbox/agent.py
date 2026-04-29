#!/usr/bin/env python3
"""Mini-Daytona sandbox agent.

Runs INSIDE the sandbox container. Reads a single JSON object from stdin:

    {"task": "<task description>",
     "anthropic_api_key": "<sk-ant-...>",
     "max_turns": 10,            # optional
     "model": "claude-..."}      # optional

Drives a Claude tool-use loop with a single `bash` tool. Each step (assistant
text, tool call, tool result) is emitted as a JSON line on stdout so the
orchestrator can stream it back via SSE. The final line is always:

    {"type": "complete", "status": "...", "output": "<final assistant text>",
     "files_created": [...], "turns": N}

Stays small on purpose: one file, only stdlib + anthropic.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_MAX_TURNS = 10
BASH_TIMEOUT_SECONDS = 60
WATCH_DIRS = ["/tmp", "/root", "/home", "/workspace"]

SYSTEM_PROMPT = """You are a coding agent running inside a Linux sandbox container.

You have one tool: `bash`. Use it to run shell commands to accomplish the user's task.
The container is ephemeral — feel free to create files in /tmp or /root.
When you have completed the task, reply with a short summary and stop calling tools."""

BASH_TOOL = {
    "name": "bash",
    "description": (
        "Run a bash command inside the sandbox. Returns stdout, stderr, and exit code. "
        "Use this for ALL filesystem and execution work. Each call is a fresh shell."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "description": "The shell command to run (passed to `bash -c`).",
            },
        },
        "required": ["command"],
    },
}


def emit(event: dict) -> None:
    """Write one JSON event to stdout and flush so the orchestrator sees it live."""
    sys.stdout.write(json.dumps(event) + "\n")
    sys.stdout.flush()


def run_bash(command: str) -> dict:
    """Execute a shell command and return stdout/stderr/exit_code."""
    try:
        proc = subprocess.run(
            ["bash", "-c", command],
            capture_output=True,
            text=True,
            timeout=BASH_TIMEOUT_SECONDS,
        )
        return {
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "exit_code": proc.returncode,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "stdout": exc.stdout or "",
            "stderr": (exc.stderr or "") + f"\n[timeout after {BASH_TIMEOUT_SECONDS}s]",
            "exit_code": 124,
        }
    except Exception as exc:
        return {"stdout": "", "stderr": f"agent error: {exc}", "exit_code": 1}


def snapshot_filesystem(start_ts: float) -> list[str]:
    """Return paths under WATCH_DIRS modified after `start_ts`. Best-effort —
    swallows errors so a missing dir or permission issue can't kill the run."""
    found: list[str] = []
    for d in WATCH_DIRS:
        if not os.path.isdir(d):
            continue
        try:
            for root, _dirs, files in os.walk(d):
                for f in files:
                    p = os.path.join(root, f)
                    try:
                        if os.path.getmtime(p) >= start_ts:
                            found.append(p)
                    except OSError:
                        pass
        except OSError:
            pass
    return sorted(set(found))


def main() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        emit({"type": "error", "message": "no task provided on stdin"})
        return 2
    try:
        req = json.loads(raw)
    except json.JSONDecodeError as exc:
        emit({"type": "error", "message": f"bad json on stdin: {exc}"})
        return 2

    task = req.get("task")
    api_key = req.get("anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY")
    model = req.get("model") or DEFAULT_MODEL
    max_turns = int(req.get("max_turns") or DEFAULT_MAX_TURNS)

    if not task:
        emit({"type": "error", "message": "missing 'task' in request"})
        return 2
    if not api_key:
        emit({"type": "error", "message": "missing 'anthropic_api_key' in request"})
        return 2

    try:
        from anthropic import Anthropic
    except ImportError:
        emit({"type": "error", "message": "anthropic SDK not installed in sandbox"})
        return 3

    client = Anthropic(api_key=api_key)
    messages: list[dict] = [{"role": "user", "content": task}]
    start_ts = time.time()
    emit({"type": "start", "task": task, "model": model, "max_turns": max_turns})

    final_text = ""
    stop_reason = "max_turns"
    turns = 0

    for turn in range(max_turns):
        turns = turn + 1
        try:
            resp = client.messages.create(
                model=model,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=[BASH_TOOL],
                messages=messages,
            )
        except Exception as exc:
            emit({"type": "error", "message": f"claude api error: {exc}"})
            return 4

        assistant_blocks: list[dict] = []
        tool_uses: list[dict] = []
        text_parts: list[str] = []

        for block in resp.content:
            if block.type == "text":
                text_parts.append(block.text)
                assistant_blocks.append({"type": "text", "text": block.text})
                emit({"type": "text", "turn": turns, "text": block.text})
            elif block.type == "tool_use":
                assistant_blocks.append(
                    {"type": "tool_use", "id": block.id, "name": block.name, "input": block.input}
                )
                tool_uses.append({"id": block.id, "name": block.name, "input": block.input})

        messages.append({"role": "assistant", "content": assistant_blocks})

        if not tool_uses:
            stop_reason = resp.stop_reason or "end_turn"
            final_text = "\n".join(text_parts).strip()
            break

        tool_results: list[dict] = []
        for tu in tool_uses:
            if tu["name"] != "bash":
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu["id"],
                    "content": f"unknown tool: {tu['name']}",
                    "is_error": True,
                })
                continue
            command = (tu["input"] or {}).get("command", "")
            emit({"type": "tool_use", "turn": turns, "command": command})
            result = run_bash(command)
            emit({
                "type": "tool_result",
                "turn": turns,
                "exit_code": result["exit_code"],
                "stdout": result["stdout"],
                "stderr": result["stderr"],
            })
            # Combine stdout/stderr for the model — keep it simple.
            content = f"exit_code: {result['exit_code']}\n"
            if result["stdout"]:
                content += f"stdout:\n{result['stdout']}\n"
            if result["stderr"]:
                content += f"stderr:\n{result['stderr']}\n"
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu["id"],
                "content": content,
                "is_error": result["exit_code"] != 0,
            })

        messages.append({"role": "user", "content": tool_results})

    files = snapshot_filesystem(start_ts)
    emit({
        "type": "complete",
        "status": "complete",
        "stop_reason": stop_reason,
        "output": final_text,
        "files_created": files,
        "turns": turns,
    })
    return 0


if __name__ == "__main__":
    sys.exit(main())
