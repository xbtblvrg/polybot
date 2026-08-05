#!/usr/bin/env python3
"""Run a same-cut scorecard text-vs-JSON basis check.

Flow stage: MEASURE/SELF-DEV. This is report-only and exists to distinguish
reader defects from refresh skew while live orders are filling.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/wallet_copy_scorecard_same_cut_basis_check_latest.json"
TEXT_RE = re.compile(
    r"total orders=(?P<orders>\d+) fills=(?P<fills>\d+) "
    r"resolved=(?P<resolved>\d+) rejects=(?P<rejects>\d+) "
    r"pnl=(?P<pnl>[+-]?\d+(?:\.\d+)?)"
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rooted(path: str | Path) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def parse_text_totals(text: str) -> dict[str, Any]:
    match = TEXT_RE.search(text)
    if not match:
        return {}
    return {
        "orders": int(match.group("orders")),
        "fills": int(match.group("fills")),
        "resolved_fills": int(match.group("resolved")),
        "rejects": int(match.group("rejects")),
        "pnl_usd": float(match.group("pnl")),
    }


def _json_totals(scorecard: dict[str, Any]) -> dict[str, Any]:
    day = str(scorecard.get("day_utc") or "")
    canonical = scorecard.get("canonical_pnl_truth") if isinstance(scorecard.get("canonical_pnl_truth"), dict) else {}
    by_day = canonical.get("by_day") if isinstance(canonical.get("by_day"), dict) else {}
    row = by_day.get(day) if isinstance(by_day.get(day), dict) else {}
    if not row:
        total = canonical.get("total") if isinstance(canonical.get("total"), dict) else {}
        row = total
    return {
        "orders": int(row.get("orders") or 0),
        "fills": int(row.get("fills") or 0),
        "resolved_fills": int(row.get("resolved_fills") or 0),
        "rejects": int(row.get("rejects") or 0),
        "pnl_usd": float(row.get("pnl_usd") or 0.0),
    }


def build_report(*, day: str, output_path: Path, timeout_s: float) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(prefix="wallet_copy_same_cut_scorecard_", suffix=".json", delete=False) as handle:
        temp_output = Path(handle.name)
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "daily_scorecard.py"),
        "--day",
        day,
        "--output",
        str(temp_output),
        "--format",
        "text",
        "--balance-sample-count",
        "1",
        "--balance-sample-interval-s",
        "0",
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        check=False,
        timeout=max(1.0, float(timeout_s)),
    )
    text_totals = parse_text_totals(proc.stdout or "")
    try:
        scorecard = json.loads(temp_output.read_text())
    except Exception:
        scorecard = {}
    finally:
        try:
            temp_output.unlink()
        except OSError:
            pass
    json_totals = _json_totals(scorecard if isinstance(scorecard, dict) else {})
    pnl_match = (
        bool(text_totals)
        and abs(float(text_totals.get("pnl_usd", 0.0)) - float(json_totals.get("pnl_usd", 0.0))) <= 0.000001
    )
    count_keys = ("orders", "fills", "resolved_fills", "rejects")
    count_match = bool(text_totals) and all(text_totals.get(key) == json_totals.get(key) for key in count_keys)
    status = "MATCH" if proc.returncode == 0 and count_match and pnl_match else "MISMATCH"
    if proc.returncode != 0:
        status = "ERROR"
    return {
        "kind": "wallet_copy_scorecard_same_cut_basis_check",
        "flow_stage": "MEASURE/SELF-DEV",
        "generated_at": _utc_now_iso(),
        "status": status,
        "rule": "same daily_scorecard process writes JSON and prints text; equal totals close refresh-skew basis divergence",
        "day_utc": day,
        "command": cmd[:2] + ["--day", day, "--output", "<temp>", "--format", "text"],
        "returncode": proc.returncode,
        "scorecard_generated_at": scorecard.get("generated_at") if isinstance(scorecard, dict) else None,
        "scorecard_basis": scorecard.get("scorecard_basis") if isinstance(scorecard, dict) else None,
        "json_totals": json_totals,
        "text_totals": text_totals,
        "count_match": count_match,
        "pnl_match": pnl_match,
        "stdout_tail": (proc.stdout or "")[-1000:],
        "stderr_tail": (proc.stderr or "")[-1000:],
        "output": str(output_path.relative_to(ROOT) if output_path.is_relative_to(ROOT) else output_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default=datetime.now(UTC).date().isoformat())
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout-s", type=float, default=180.0)
    args = parser.parse_args()
    output = _rooted(args.output)
    report = build_report(day=str(args.day), output_path=output, timeout_s=float(args.timeout_s))
    atomic_write_json(output, report)
    print(
        "scorecard_same_cut_basis "
        f"status={report['status']} "
        f"json={report['json_totals']} "
        f"text={report['text_totals']}"
    )
    return 0 if report["status"] == "MATCH" else 1


if __name__ == "__main__":
    raise SystemExit(main())
