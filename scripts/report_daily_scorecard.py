#!/usr/bin/env python3
"""Write the canonical wallet-copy daily scorecard artifact.

Flow stage: LIVE/SELF-DEV. This is a small wrapper around
scripts/daily_scorecard.py with heartbeat-friendly defaults: when no day is
provided, it closes the previous UTC day and writes one JSON artifact under
data/research.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data/research"


def _print_subprocess_stream(value: str | bytes | None, *, file=None) -> None:
    if not value:
        return
    text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
    print(text, end="", file=file)


def _default_closed_day() -> str:
    return (datetime.now(tz=UTC).date() - timedelta(days=1)).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default="", help="UTC day YYYY-MM-DD; default is the previous UTC day.")
    parser.add_argument("--output", default="", help="Output JSON path; default is data/research/wallet_copy_daily_scorecard_<day>.json")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--bankroll-usd", type=float, default=335.0)
    parser.add_argument("--reconciliation-start", default="", help="UTC timestamp for the last wallet top-up baseline.")
    parser.add_argument(
        "--balance-sample-count",
        type=int,
        default=1,
        help="Balance samples passed to daily_scorecard.py; heartbeat default is bounded.",
    )
    parser.add_argument(
        "--balance-sample-interval-s",
        type=float,
        default=0.0,
        help="Seconds between balance samples; heartbeat default avoids a 120s scorecard stall.",
    )
    parser.add_argument(
        "--offline-no-chain",
        action="store_true",
        help="Pass through daily_scorecard.py local-ledger-only mode; live balance/position reconciliation is skipped.",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=180.0,
        help="Maximum seconds to wait for daily_scorecard.py; 0 disables the timeout.",
    )
    parser.add_argument("--skip-handoff-roll", action="store_true", help="Test-only escape hatch; heartbeat runs the roll.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    day = args.day or _default_closed_day()
    output = Path(args.output) if args.output else DEFAULT_OUTPUT_DIR / f"wallet_copy_daily_scorecard_{day}.json"
    timeout_s = None if float(args.timeout_s) <= 0 else max(1.0, float(args.timeout_s))
    if not bool(getattr(args, "skip_handoff_roll", False)):
        roll_cmd = [sys.executable, str(ROOT / "scripts/roll_handoff_archive.py")]
        try:
            roll_proc = subprocess.run(
                roll_cmd,
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                check=False,
                timeout=None if timeout_s is None else min(timeout_s, 60.0),
            )
        except subprocess.TimeoutExpired as exc:
            _print_subprocess_stream(exc.stdout)
            _print_subprocess_stream(exc.stderr, file=sys.stderr)
            print("handoff roll timed out", file=sys.stderr)
            return 124
        if roll_proc.returncode != 0:
            _print_subprocess_stream(roll_proc.stdout)
            _print_subprocess_stream(roll_proc.stderr, file=sys.stderr)
            return roll_proc.returncode
    cmd = [
        sys.executable,
        str(ROOT / "scripts/daily_scorecard.py"),
        "--day",
        day,
        "--output",
        str(output),
        "--format",
        args.format,
        "--bankroll-usd",
        str(float(args.bankroll_usd)),
        "--balance-sample-count",
        str(max(1, int(args.balance_sample_count))),
        "--balance-sample-interval-s",
        str(max(0.0, float(args.balance_sample_interval_s))),
    ]
    if args.reconciliation_start:
        cmd.extend(["--reconciliation-start", args.reconciliation_start])
    if bool(getattr(args, "offline_no_chain", False)):
        cmd.append("--offline-no-chain")
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True, check=False, timeout=timeout_s)
    except subprocess.TimeoutExpired as exc:
        _print_subprocess_stream(exc.stdout)
        _print_subprocess_stream(exc.stderr, file=sys.stderr)
        print(f"daily_scorecard timed out after {timeout_s:.1f}s", file=sys.stderr)
        return 124
    if proc.returncode != 0:
        if proc.stdout:
            print(proc.stdout, end="")
        if proc.stderr:
            print(proc.stderr, end="", file=sys.stderr)
        return proc.returncode
    if args.format == "json":
        map_output = DEFAULT_OUTPUT_DIR / f"btc5m_288_participation_map_{day}.json"
        map_cmd = [
            sys.executable,
            str(ROOT / "scripts/report_288_participation_map.py"),
            "--day",
            day,
            "--scorecard",
            str(output),
            "--output",
            str(map_output),
        ]
        try:
            map_proc = subprocess.run(
                map_cmd,
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                check=False,
                timeout=timeout_s,
            )
        except subprocess.TimeoutExpired as exc:
            _print_subprocess_stream(exc.stdout)
            _print_subprocess_stream(exc.stderr, file=sys.stderr)
            print(f"288 participation map timed out after {timeout_s:.1f}s", file=sys.stderr)
            return 124
        if map_proc.returncode != 0:
            _print_subprocess_stream(map_proc.stdout)
            _print_subprocess_stream(map_proc.stderr, file=sys.stderr)
            return map_proc.returncode
    if args.format == "text" and proc.stdout:
        print(proc.stdout, end="")
    print(f"wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
