#!/usr/bin/env python3
"""Build the E7 paper-quote packet and mechanical gate verdict.

Flow stage: PROMOTE/LEARN. This is read-only evidence tooling for Fable's
2026-07-06T13:27Z E7 ruling. It does not mutate the E7 lane states and does
not touch live execution.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_E7_0_STATE = "data/research/e7_spot_open_paper_lane_state.json"
DEFAULT_E7_1_STATE = "data/research/e7_1_spot_open_paper_lane_state.json"
DEFAULT_OUTPUT = "data/research/e7_paper_packet_report.json"
DEFAULT_E7_0_STOP_AT = "2026-07-06T18:00:00Z"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _parse_ts(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _quoted_windows(quotes: list[dict[str, Any]]) -> set[int]:
    windows: set[int] = set()
    for quote in quotes:
        if str(quote.get("quote_status") or "").upper() != "QUOTED":
            continue
        window_start = int(_num(quote.get("window_start_s")))
        if window_start > 0:
            windows.add(window_start)
    return windows


def _first_quote_at(quotes: list[dict[str, Any]]) -> datetime | None:
    times = [
        parsed
        for quote in quotes
        if str(quote.get("quote_status") or "").upper() == "QUOTED"
        if (parsed := _parse_ts(quote.get("generated_at"))) is not None
    ]
    return min(times) if times else None


def _zero_live_pass(state: dict[str, Any]) -> bool:
    zero = state.get("zero_live_assertion") if isinstance(state.get("zero_live_assertion"), dict) else {}
    return (
        bool(state.get("paper_only", True))
        and not bool(state.get("live_orders_allowed", False))
        and str(zero.get("status") or "").upper() == "PASS"
        and int(_num(zero.get("orders_submitted"))) == 0
    )


def _paper_intents_only(state: dict[str, Any]) -> bool:
    orders = [row for row in state.get("orders", []) if isinstance(row, dict)]
    for order in orders:
        source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
        mode = str(source_intent.get("mode") or "paper").lower()
        if mode != "paper":
            return False
        if bool(source_intent.get("live_orders_allowed", False)):
            return False
    return True


def _packet_stop_at(
    *,
    label: str,
    quotes: list[dict[str, Any]],
    explicit_stop_at: str,
) -> datetime | None:
    if label == "E7.0":
        return _parse_ts(explicit_stop_at)
    first_quote = _first_quote_at(quotes)
    return first_quote + timedelta(hours=5) if first_quote else None


def _topology_counts(summary: dict[str, Any]) -> dict[str, int]:
    topology = summary.get("paper_quote_fill_topology")
    if not isinstance(topology, dict):
        return {}
    counts = topology.get("topology_counts")
    if not isinstance(counts, dict):
        return {}
    return {str(key): int(_num(value)) for key, value in counts.items()}


def summarize_variant(
    *,
    label: str,
    state_path: Path,
    now: datetime,
    stop_at: str,
) -> dict[str, Any]:
    state = load_json(state_path, default={})
    if not isinstance(state, dict):
        state = {}
    summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    quotes = [row for row in state.get("paper_quote_events", []) if isinstance(row, dict)]
    quoted_windows = _quoted_windows(quotes)
    quoted_window_count = len(quoted_windows)
    resolved_fills = int(_num(summary.get("resolved_paper_fills")))
    resolved_wins = int(_num(summary.get("resolved_paper_wins")))
    resolved_pnl = round(_num(summary.get("resolved_paper_pnl_usd")), 6)
    topology_counts = _topology_counts(summary)
    both_sides_filled = int(topology_counts.get("both_sides_filled", 0))
    zero_live = _zero_live_pass(state)
    parity = _paper_intents_only(state)
    stop_time = _packet_stop_at(label=label, quotes=quotes, explicit_stop_at=stop_at)

    packet_reasons: list[str] = []
    if quoted_window_count >= 24:
        packet_reasons.append("quoted_windows_gte_24")
    if stop_time is not None and now >= stop_time:
        packet_reasons.append("stop_time_reached")
    if resolved_fills >= 10 and resolved_wins == 0:
        packet_reasons.append("zero_wins_10_fill_tripwire")

    criteria = {
        "resolved_paper_pnl_positive": resolved_pnl > 0.0,
        "resolved_paper_fills_gte_12": resolved_fills >= 12,
        "zero_live_pass": zero_live,
        "copyintent_parity_intact": parity,
    }
    if label == "E7.1":
        criteria["both_sides_filled_gte_3"] = both_sides_filled >= 3
    packet_closed = bool(packet_reasons)
    eligible = packet_closed and all(criteria.values())
    if not packet_closed:
        gate_status = "COLLECTING"
    elif eligible:
        gate_status = "PASS_LIVE_READY_BY_PRECOMMITTED_RULE"
    else:
        gate_status = "FAILS_PRECOMMITTED_RULE"

    return {
        "label": label,
        "state_path": str(state_path),
        "exists": state_path.exists(),
        "lane": state.get("lane") or "",
        "state_updated_at": state.get("updated_at") or "",
        "paper_only": bool(state.get("paper_only", True)),
        "live_orders_allowed": bool(state.get("live_orders_allowed", False)),
        "zero_live_assertion": state.get("zero_live_assertion") if isinstance(state.get("zero_live_assertion"), dict) else {},
        "parameters": {
            "paper_quote_offset_s": _num((state.get("parameters") or {}).get("paper_quote_offset_s")),
            "paper_size_mode": str((state.get("parameters") or {}).get("paper_size_mode") or ""),
        },
        "packet_clock": {
            "required_unique_quoted_windows": 24,
            "unique_quoted_windows": quoted_window_count,
            "remaining_unique_quoted_windows": max(24 - quoted_window_count, 0),
            "first_quote_at": _iso(_first_quote_at(quotes)),
            "stop_at": _iso(stop_time),
            "packet_closed": packet_closed,
            "packet_close_reasons": packet_reasons,
        },
        "summary": {
            "paper_quote_events": len(quotes),
            "paper_orders": int(_num(summary.get("paper_orders"))),
            "paper_filled_orders": int(_num(summary.get("paper_filled_orders"))),
            "book_verified_fills": int(_num(summary.get("book_verified_fills"))),
            "resolved_paper_fills": resolved_fills,
            "resolved_paper_wins": resolved_wins,
            "resolved_paper_losses": int(_num(summary.get("resolved_paper_losses"))),
            "unresolved_paper_fills": int(_num(summary.get("unresolved_paper_fills"))),
            "resolved_paper_pnl_usd": resolved_pnl,
            "resolved_paper_wr_pct": round(_num(summary.get("resolved_paper_wr_pct")), 6),
            "both_sides_filled_windows": both_sides_filled,
            "one_sided_filled_windows": int(topology_counts.get("one_sided_filled", 0)),
            "none_filled_windows": int(topology_counts.get("none_filled", 0)),
            "paper_quote_fill_topology": summary.get("paper_quote_fill_topology")
            if isinstance(summary.get("paper_quote_fill_topology"), dict)
            else {},
        },
        "precommitted_live_criteria": criteria,
        "gate_status": gate_status,
    }


def _mechanical_next_action(variants: list[dict[str, Any]]) -> str:
    by_label = {str(row.get("label")): row for row in variants}
    e7_0 = by_label.get("E7.0", {})
    e7_1 = by_label.get("E7.1", {})
    if e7_0.get("gate_status") == "PASS_LIVE_READY_BY_PRECOMMITTED_RULE":
        return "start_E7.0_live_under_single_guard_per_precommitted_rule"
    if e7_0.get("gate_status") == "COLLECTING":
        return "continue_E7.0_packet_and_E7.1_parallel_paper"
    if e7_1.get("gate_status") == "PASS_LIVE_READY_BY_PRECOMMITTED_RULE":
        return "start_E7.1_live_under_single_guard_per_precommitted_rule"
    if e7_1.get("gate_status") == "FAILS_PRECOMMITTED_RULE":
        return "park_E7_after_two_negative_packets"
    return "keep_E7.0_paper_dead_continue_E7.1_measurement"


def build_report(
    *,
    e7_0_state: Path,
    e7_1_state: Path,
    e7_0_stop_at: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    variants = [
        summarize_variant(label="E7.0", state_path=e7_0_state, now=now, stop_at=e7_0_stop_at),
        summarize_variant(label="E7.1", state_path=e7_1_state, now=now, stop_at=e7_0_stop_at),
    ]
    return {
        "schema_version": 1,
        "kind": "e7_paper_packet_report",
        "flow_stage": "PROMOTE/LEARN",
        "generated_at": utc_now_iso(),
        "as_of": _iso(now),
        "source_ruling": "Fable DIRECTION 2026-07-06T13:27:03Z RULING AZ",
        "variants": variants,
        "mechanical_next_action": _mechanical_next_action(variants),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--e7-0-state", default=DEFAULT_E7_0_STATE)
    parser.add_argument("--e7-1-state", default=DEFAULT_E7_1_STATE)
    parser.add_argument("--e7-0-stop-at", default=DEFAULT_E7_0_STOP_AT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--now", default="", help="UTC ISO timestamp test/forensics hook")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = _parse_ts(args.now) if args.now else None
    report = build_report(
        e7_0_state=Path(args.e7_0_state),
        e7_1_state=Path(args.e7_1_state),
        e7_0_stop_at=str(args.e7_0_stop_at),
        now=now,
    )
    atomic_write_json(args.output, report)
    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        variants = {
            row["label"]: row
            for row in report.get("variants", [])
            if isinstance(row, dict) and row.get("label")
        }
        print(
            json.dumps(
                {
                    "generated_at": report["generated_at"],
                    "mechanical_next_action": report["mechanical_next_action"],
                    "E7.0": {
                        "gate_status": variants.get("E7.0", {}).get("gate_status"),
                        "packet_clock": variants.get("E7.0", {}).get("packet_clock"),
                        "precommitted_live_criteria": variants.get("E7.0", {}).get("precommitted_live_criteria"),
                        "both_sides_filled_windows": variants.get("E7.0", {})
                        .get("summary", {})
                        .get("both_sides_filled_windows"),
                        "resolved_paper_pnl_usd": variants.get("E7.0", {}).get("summary", {}).get("resolved_paper_pnl_usd"),
                    },
                    "E7.1": {
                        "gate_status": variants.get("E7.1", {}).get("gate_status"),
                        "packet_clock": variants.get("E7.1", {}).get("packet_clock"),
                        "precommitted_live_criteria": variants.get("E7.1", {}).get("precommitted_live_criteria"),
                        "both_sides_filled_windows": variants.get("E7.1", {})
                        .get("summary", {})
                        .get("both_sides_filled_windows"),
                        "resolved_paper_pnl_usd": variants.get("E7.1", {}).get("summary", {}).get("resolved_paper_pnl_usd"),
                    },
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
