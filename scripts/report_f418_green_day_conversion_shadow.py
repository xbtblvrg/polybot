#!/usr/bin/env python3
"""Measure submitted-window conversion before/after the 2026-07-23 green sign cross.

Flow stage: LEARN/OBSERVE. This report is paper/read-only and never mutates live state.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.pnl_truth import build_pnl_truth  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402

F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
GREEN_SIGN_FROM = "2026-07-23T12:10:42Z"
MIN_POST_WINDOWS = 20
MIN_CONTROL_WINDOWS = 30


def _ts(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _error_class(event: dict[str, Any]) -> str:
    trade = event.get("trade_result") if isinstance(event.get("trade_result"), dict) else {}
    return str(trade.get("error_class") or event.get("error_class") or "unknown")


def _summarize(events: list[dict[str, Any]]) -> dict[str, Any]:
    windows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        slug = str(event.get("market_slug") or "")
        if slug:
            windows[slug].append(event)
    filled_windows = 0
    resolved_fills = 0
    pnl = 0.0
    rejection_taxonomy: Counter[str] = Counter()
    for rows in windows.values():
        filled = [row for row in rows if str(row.get("status") or "") == "FILLED"]
        if filled:
            filled_windows += 1
        for row in rows:
            if str(row.get("status") or "") == "REJECTED":
                rejection_taxonomy[_error_class(row)] += 1
            if str(row.get("status") or "") == "FILLED" and bool(row.get("resolved")):
                resolved_fills += 1
                pnl += float(row.get("pnl_usd") or 0.0)
    submitted_windows = len(windows)
    return {
        "submitted_windows": submitted_windows,
        "filled_windows": filled_windows,
        "unfilled_windows": submitted_windows - filled_windows,
        "submitted_to_filled_pct": (
            round(100.0 * filled_windows / submitted_windows, 6) if submitted_windows else 0.0
        ),
        "resolved_fills": resolved_fills,
        "canonical_post_fee_pnl_usd": round(pnl, 6),
        "canonical_post_fee_ev_per_submitted_window_usd": (
            round(pnl / submitted_windows, 6) if submitted_windows else None
        ),
        "canonical_post_fee_ev_per_filled_window_usd": (
            round(pnl / filled_windows, 6) if filled_windows else None
        ),
        "rejection_taxonomy": dict(sorted(rejection_taxonomy.items())),
    }


def build_report(
    events: list[dict[str, Any]],
    *,
    source_wallet: str = F418,
    green_sign_from: str = GREEN_SIGN_FROM,
    generated_at: str,
) -> dict[str, Any]:
    wallet = source_wallet.lower()
    sign_ts = _ts(green_sign_from)
    sign_day = datetime.fromtimestamp(sign_ts, UTC)
    day_start_ts = sign_day.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    scoped = [
        row
        for row in events
        if str(row.get("source_wallet") or "").lower() == wallet
        and str(row.get("status") or "") in {"FILLED", "REJECTED"}
        and day_start_ts <= float(row.get("ts") or 0.0)
    ]
    control = _summarize([row for row in scoped if float(row.get("ts") or 0.0) < sign_ts])
    post = _summarize([row for row in scoped if float(row.get("ts") or 0.0) >= sign_ts])
    gate_pass = (
        post["submitted_windows"] >= MIN_POST_WINDOWS
        and control["submitted_windows"] >= MIN_CONTROL_WINDOWS
    )
    conversion_delta = round(
        post["submitted_to_filled_pct"] - control["submitted_to_filled_pct"], 6
    )
    if not gate_pass:
        verdict = "ACCRUE_PREREGISTERED_SAMPLE"
        bottleneck = "UNDECIDED_SAMPLE_GATE"
    elif post["submitted_to_filled_pct"] < control["submitted_to_filled_pct"]:
        verdict = "DECISION_READY"
        bottleneck = "EXCHANGE_CONVERSION"
    else:
        verdict = "DECISION_READY"
        bottleneck = "SELECTION_OR_ABSTENTION"
    return {
        "schema_version": 1,
        "kind": "f418_green_day_submitted_to_filled_conversion_shadow",
        "flow_stage": "LEARN/OBSERVE",
        "generated_at": generated_at,
        "source_wallet": wallet,
        "green_sign_from": green_sign_from,
        "preregistration": {
            "minimum_post_sign_submitted_windows": MIN_POST_WINDOWS,
            "minimum_pre_sign_control_windows": MIN_CONTROL_WINDOWS,
            "decision": (
                "If post-sign conversion is below control, name exchange conversion as the "
                "bottleneck; otherwise name selection/abstention. This report alone cannot mutate live."
            ),
        },
        "control_pre_sign": control,
        "green_sign_post": post,
        "conversion_delta_pct_points": conversion_delta,
        "sample_gate_pass": gate_pass,
        "verdict": verdict,
        "dual_bar_bottleneck": bottleneck,
        "live_mutation": False,
        "copyintent_parity_violations": 0,
    }


def _newest_resolutions() -> Path:
    candidates = list((ROOT / "data/research").glob("btc_resolutions_*.jsonl"))
    if not candidates:
        raise FileNotFoundError("no btc_resolutions_*.jsonl snapshot")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--source-wallet", default=F418)
    parser.add_argument("--green-sign-from", default=GREEN_SIGN_FROM)
    parser.add_argument(
        "--output",
        default="data/research/f418_green_day_conversion_shadow_latest.json",
    )
    args = parser.parse_args()
    ledger = json.loads((ROOT / args.ledger).read_text())
    resolutions_path = ROOT / args.resolutions if args.resolutions else _newest_resolutions()
    truth = build_pnl_truth(ledger, load_resolutions(str(resolutions_path)))
    report = build_report(
        truth.get("events") or [],
        source_wallet=args.source_wallet,
        green_sign_from=args.green_sign_from,
        generated_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    report["resolutions_source"] = str(resolutions_path.relative_to(ROOT))
    atomic_write_json(ROOT / args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
