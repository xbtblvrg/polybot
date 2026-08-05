#!/usr/bin/env python3
"""Retain the canonical daily floor-gate residency row as idempotent JSONL."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.brainless_live_guard_restart import generation_verdict  # noqa: E402
from scripts.update_state_digest import _daily_floor_gate_residency  # noqa: E402
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_text, load_json  # noqa: E402

DEFAULT_SCORECARD = ROOT / "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_RESTART_STATE = ROOT / "data/research/brainless_live_guard_restart_state.json"
DEFAULT_HISTORY = ROOT / "data/research/floor_gate_residency_daily_events.jsonl"


def build_row(
    *,
    scorecard_path: Path,
    restart_state_path: Path,
    now: dt.datetime,
) -> dict[str, Any] | None:
    scorecard = load_json(scorecard_path)
    restart = load_json(restart_state_path)
    by_day = ((scorecard.get("canonical_pnl_truth") or {}).get("by_day") or {})
    day_utc = max((str(day) for day in by_day), default="")
    if not day_utc:
        return None
    loaded = restart.get("loaded_generation") or {}
    disk = restart.get("disk_generation") or {}
    row = _daily_floor_gate_residency(
        day_utc,
        by_day.get(day_utc) or {},
        loaded_generation_sha256=loaded.get("sha256"),
        disk_generation_sha256=disk.get("sha256"),
        generation_verdict=generation_verdict(restart, now=now),
    )
    row["captured_at"] = utc_now_iso()
    return row


def retain_daily_row(history_path: Path, row: dict[str, Any]) -> list[dict[str, Any]]:
    rows_by_day: dict[str, dict[str, Any]] = {}
    if history_path.exists():
        for line in history_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            try:
                existing = json.loads(line)
            except json.JSONDecodeError:
                continue
            day = str(existing.get("day_utc") or "")
            if day:
                rows_by_day[day] = existing
    rows_by_day[str(row["day_utc"])] = row
    rows = [rows_by_day[day] for day in sorted(rows_by_day)]
    atomic_write_text(
        history_path,
        "".join(json.dumps(item, sort_keys=True) + "\n" for item in rows),
    )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--restart-state", type=Path, default=DEFAULT_RESTART_STATE)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    args = parser.parse_args()
    now = dt.datetime.now(dt.timezone.utc)
    row = build_row(
        scorecard_path=args.scorecard,
        restart_state_path=args.restart_state,
        now=now,
    )
    if row is None:
        print(json.dumps({"status": "NO_CANONICAL_DAY"}, sort_keys=True))
        return 0
    rows = retain_daily_row(args.history, row)
    print(json.dumps({"latest": row, "retained_days": len(rows)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
