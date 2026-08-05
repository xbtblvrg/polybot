#!/usr/bin/env python3
"""Shared Codex serving-run mutex.

Flow stage: SELF-DEV. This protects milestone work from two Codex serving
runs executing the same Fable next-list concurrently. It never touches live
trading state.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


DEFAULT_LOCK_DIR = "/tmp/polymarket_codex_serving_run.lock"
DEFAULT_TTL_S = 4 * 60 * 60
HELD_EXIT_CODE = 75


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _format_ts(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _metadata_path(lock_dir: Path) -> Path:
    return lock_dir / "metadata.json"


def _read_metadata(lock_dir: Path) -> dict[str, object]:
    try:
        return json.loads(_metadata_path(lock_dir).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_token(path: str | None, token: str) -> None:
    if not path:
        return
    token_path = Path(path)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(token + "\n", encoding="utf-8")


def _read_token(args: argparse.Namespace) -> str:
    if args.token:
        return str(args.token)
    if args.token_file:
        try:
            return Path(args.token_file).read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return ""
    return ""


def _is_stale(metadata: dict[str, object], now: datetime, ttl_s: int) -> bool:
    pid = metadata.get("pid")
    if isinstance(pid, int) and pid > 0:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            pass
    acquired_at = _parse_ts(metadata.get("acquired_at"))
    if acquired_at is None:
        return True
    return now - acquired_at > timedelta(seconds=max(1, ttl_s))


def acquire(args: argparse.Namespace) -> int:
    lock_dir = Path(args.lock_dir)
    now = _utc_now()
    token = uuid.uuid4().hex
    holder_pid = args.holder_pid or os.getpid()
    metadata = {
        "schema_version": 1,
        "kind": "codex_serving_mutex",
        "owner": args.owner,
        "token": token,
        "pid": holder_pid,
        "acquire_pid": os.getpid(),
        "cwd": os.getcwd(),
        "acquired_at": _format_ts(now),
        "expires_after_s": args.ttl_s,
        "expires_at": _format_ts(now + timedelta(seconds=args.ttl_s)),
    }

    try:
        lock_dir.mkdir(mode=0o700)
    except FileExistsError:
        existing = _read_metadata(lock_dir)
        if _is_stale(existing, now, args.ttl_s):
            stale_reason = "dead_pid_or_ttl_expired"
            shutil.rmtree(lock_dir)
            lock_dir.mkdir(mode=0o700)
        else:
            print(
                json.dumps(
                    {
                        "status": "HELD",
                        "lock_dir": str(lock_dir),
                        "owner": existing.get("owner"),
                        "acquired_at": existing.get("acquired_at"),
                        "expires_at": existing.get("expires_at"),
                    },
                    sort_keys=True,
                )
            )
            return HELD_EXIT_CODE

    _metadata_path(lock_dir).write_text(json.dumps(metadata, sort_keys=True) + "\n", encoding="utf-8")
    _write_token(args.token_file, token)
    payload = {"status": "ACQUIRED", "lock_dir": str(lock_dir), "pid": holder_pid, "token_file": args.token_file}
    if "stale_reason" in locals():
        payload["reclaimed_stale_lock"] = stale_reason
    print(json.dumps(payload, sort_keys=True))
    return 0


def release(args: argparse.Namespace) -> int:
    lock_dir = Path(args.lock_dir)
    token = _read_token(args)
    metadata = _read_metadata(lock_dir)
    if not metadata:
        print(json.dumps({"status": "NOT_HELD", "lock_dir": str(lock_dir)}, sort_keys=True))
        return 0
    if token and token == metadata.get("token"):
        shutil.rmtree(lock_dir)
        if args.token_file:
            Path(args.token_file).unlink(missing_ok=True)
        print(json.dumps({"status": "RELEASED", "lock_dir": str(lock_dir)}, sort_keys=True))
        return 0
    print(json.dumps({"status": "TOKEN_MISMATCH", "lock_dir": str(lock_dir)}, sort_keys=True), file=sys.stderr)
    return 2


def status(args: argparse.Namespace) -> int:
    lock_dir = Path(args.lock_dir)
    metadata = _read_metadata(lock_dir)
    state = "HELD" if metadata else "NOT_HELD"
    print(json.dumps({"status": state, "lock_dir": str(lock_dir), "metadata": metadata}, sort_keys=True))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock-dir", default=DEFAULT_LOCK_DIR)
    sub = parser.add_subparsers(dest="command", required=True)

    acquire_parser = sub.add_parser("acquire")
    acquire_parser.add_argument("--owner", default="codex_serving")
    acquire_parser.add_argument("--token-file")
    acquire_parser.add_argument("--holder-pid", type=int)
    acquire_parser.add_argument("--ttl-s", type=int, default=DEFAULT_TTL_S)
    acquire_parser.set_defaults(func=acquire)

    release_parser = sub.add_parser("release")
    release_parser.add_argument("--token")
    release_parser.add_argument("--token-file")
    release_parser.set_defaults(func=release)

    status_parser = sub.add_parser("status")
    status_parser.set_defaults(func=status)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
