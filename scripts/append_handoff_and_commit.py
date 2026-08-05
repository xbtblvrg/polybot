#!/usr/bin/env python3
"""Append queued brainless HANDOFF text and commit it as one guarded transaction."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
from pathlib import Path


ROOT = Path(os.getenv("POLYMARKET_AGENT_ROOT", Path(__file__).resolve().parents[1])).resolve()


def _git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def append_and_commit(*, handoff: Path, pending: Path, lock: Path, message: str) -> dict[str, object]:
    payload = pending.read_bytes() if pending.exists() else b""
    if not payload.strip():
        return {"status": "NO_PENDING_HANDOFF", "committed": False}

    handoff = handoff if handoff.is_absolute() else ROOT / handoff
    lock = lock if lock.is_absolute() else ROOT / lock
    relative_handoff = os.path.relpath(handoff, ROOT)
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        if _git("rev-parse", "--is-inside-work-tree").returncode != 0:
            handoff.parent.mkdir(parents=True, exist_ok=True)
            with handoff.open("ab") as handle:
                handle.write(payload)
            return {"status": "APPENDED_NO_GIT_TEST_FIXTURE", "committed": False}
        if _git("ls-files", "--error-unmatch", "--", relative_handoff).returncode != 0:
            raise RuntimeError(f"HANDOFF must be tracked before atomic append: {relative_handoff}")
        if _git("diff", "--quiet", "--", relative_handoff).returncode != 0:
            raise RuntimeError(f"HANDOFF has an uncommitted worktree delta: {relative_handoff}")
        if _git("diff", "--cached", "--quiet", "--", relative_handoff).returncode != 0:
            raise RuntimeError(f"HANDOFF has a staged delta: {relative_handoff}")

        original = handoff.read_bytes()
        if original and not original.endswith(b"\n") and not payload.startswith(b"\n"):
            payload = b"\n" + payload
        expected = original + payload
        try:
            with handoff.open("ab") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            commit = _git("commit", "--only", "--no-verify", "-m", message, "--", relative_handoff)
            if commit.returncode != 0:
                raise RuntimeError((commit.stderr or commit.stdout or "git commit failed").strip())
        except Exception:
            if handoff.read_bytes() == expected:
                handoff.write_bytes(original)
            raise
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    return {
        "status": "APPENDED_AND_COMMITTED",
        "committed": True,
        "commit": _git("rev-parse", "HEAD").stdout.strip(),
        "handoff": relative_handoff,
        "bytes_appended": len(payload),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handoff", default="docs/agents/HANDOFF.md")
    parser.add_argument("--pending", required=True)
    parser.add_argument("--lock", default="data/research/brainless_handoff_commit.lock")
    parser.add_argument("--message", default="ops: record brainless heartbeat status")
    args = parser.parse_args(argv)
    try:
        result = append_and_commit(
            handoff=Path(args.handoff),
            pending=Path(args.pending),
            lock=Path(args.lock),
            message=str(args.message),
        )
    except Exception as exc:
        print(json.dumps({"status": "ATOMIC_APPEND_FAILED", "error": f"{type(exc).__name__}: {exc}"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
