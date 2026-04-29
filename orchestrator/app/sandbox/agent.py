#!/usr/bin/env python3
"""Mini-Daytona sandbox agent.

Runs INSIDE the sandbox container. Reads a single JSON object from stdin:

    {"task": "<task description>",
     "max_turns": 10,            # optional
     "model": "gpt-..."}         # optional

The API key is read from the OPENAI_API_KEY environment variable (forwarded
in by the orchestrator), never from the request payload — keeping the secret
off the public HTTP surface.

Drives an OpenAI tool-use loop with a single `bash` tool. Each step (assistant
text, tool call, tool result) is emitted as a JSON line on stdout so the
orchestrator can stream it back via SSE. The final line is always:

    {"type": "complete", "status": "...", "output": "<final assistant text>",
     "files_created": [...], "turns": N}

Stays small on purpose: one file, only stdlib + openai.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

DEFAULT_MODEL = "gpt-4o"
DEFAULT_MAX_TURNS = 10
BASH_TIMEOUT_SECONDS = 60
WATCH_DIRS = ["/tmp", "/root", "/home", "/workspace"]

SYSTEM_PROMPT = """You are a coding agent running inside a Linux sandbox container.

You have one tool: `bash`. Use it to run shell commands to accomplish the user's task.
The container is ephemeral — feel free to create files in /tmp or /root.
When you have completed the task, reply with a short summary and stop calling tools."""

BASH_TOOL = {
    "type": "function",
    "function": {
        "name": "bash",
        "description": (
            "Run a bash command inside the sandbox. Returns stdout, stderr, and exit code. "
            "Use this for ALL filesystem and execution work. Each call is a fresh shell."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command to run (passed to `bash -c`).",
                },
            },
            "required": ["command"],
        },
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
    api_key = os.environ.get("OPENAI_API_KEY")
    model = req.get("model") or DEFAULT_MODEL
    max_turns = int(req.get("max_turns") or DEFAULT_MAX_TURNS)

    if not task:
        emit({"type": "error", "message": "missing 'task' in request"})
        return 2
    if not api_key:
        emit({"type": "error", "message": "OPENAI_API_KEY not set in sandbox environment"})
        return 2

    try:
        from openai import OpenAI
    except ImportError:
        emit({"type": "error", "message": "openai SDK not installed in sandbox"})
        return 3

    client = OpenAI(api_key=api_key)
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": task},
    ]
    start_ts = time.time()
    emit({"type": "start", "task": task, "model": model, "max_turns": max_turns})

    final_text = ""
    stop_reason = "max_turns"
    turns = 0

    for turn in range(max_turns):
        turns = turn + 1
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                tools=[BASH_TOOL],
            )
        except Exception as exc:
            emit({"type": "error", "message": f"openai api error: {exc}"})
            return 4

        choice = resp.choices[0]
        msg = choice.message
        tool_calls = msg.tool_calls or []
        text = msg.content or ""

        if text:
            emit({"type": "text", "turn": turns, "text": text})

        # Echo the assistant message back into history. Only include tool_calls
        # if there were any — the API rejects empty tool_calls arrays.
        assistant_msg: dict = {"role": "assistant", "content": text}
        if tool_calls:
            assistant_msg["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in tool_calls
            ]
        messages.append(assistant_msg)

        if not tool_calls:
            stop_reason = choice.finish_reason or "stop"
            final_text = text.strip()
            break

        for tc in tool_calls:
            name = tc.function.name
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}

            if name != "bash":
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": f"unknown tool: {name}",
                })
                continue

            command = args.get("command", "")
            emit({"type": "tool_use", "turn": turns, "command": command})
            result = run_bash(command)
            emit({
                "type": "tool_result",
                "turn": turns,
                "exit_code": result["exit_code"],
                "stdout": result["stdout"],
                "stderr": result["stderr"],
            })
            content = f"exit_code: {result['exit_code']}\n"
            if result["stdout"]:
                content += f"stdout:\n{result['stdout']}\n"
            if result["stderr"]:
                content += f"stderr:\n{result['stderr']}\n"
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": content,
            })

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
