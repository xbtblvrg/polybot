#!/usr/bin/env python3
"""Report the live-profit queue gates from current scorecard evidence.

Flow stage: LIVE/LEARN/SELF-DEV. This script is intentionally bounded: it
reads the current-day scorecard, live deadman state, and guard state, then
emits the P1d scaling verdict and P2b reject taxonomy without touching the
live guard.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/wallet_copy_live_profit_queue_report_latest.json"
DEFAULT_REJECT_OUTPUT = "data/research/wallet_copy_live_reject_reason_classification_latest.json"
FIVE_MIN_WINDOW_S = 300.0
EARLY_ENTRY_MAX_S = 60.0
SCALE_SAMPLE_FLOOR = 10
SCALE_FROM_TRANCHE_USD = 1.25
SCALE_TO_TRANCHE_USD = 2.5

FRESHNESS_REASONS = {
    "inventory_late_window_guard",
    "inventory_window_state_stale",
    "signal_age_gte_60s",
    "window_time_gte_60s",
    "window_time_gte_180s",
}
BAND_REASONS = {
    "inventory_best_ask_above_vwap_plus_buffer",
    "price_above_band",
    "price_band_skip",
}
ENVELOPE_REASONS = {
    "drip_residual_gap_below_min_tranche",
    "hard_entry_cap_skip",
    "inventory_best_ask_missing",
    "inventory_residual_gap_below_min_order",
    "live_order_rejected",
    "toxicity_protection",
}
PARITY_REASONS = {
    "filled",
    "inventory_confirmed_unchanged_no_edge",
    "inventory_target_already_met",
    "submitted",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _rooted(path: str) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _default_current_scorecard(day: str | None = None) -> Path:
    data_dir = ROOT / "data" / "research"
    day = day or datetime.now(timezone.utc).date().isoformat()
    preferred = data_dir / f"wallet_copy_daily_scorecard_{day}_current.json"
    if preferred.exists():
        return preferred
    matches = sorted(data_dir.glob("wallet_copy_daily_scorecard_*_current.json"), key=lambda path: path.stat().st_mtime)
    if matches:
        return matches[-1]
    return data_dir / "wallet_copy_daily_scorecard_current.json"


def _event_key(row: dict[str, Any]) -> tuple[str, str]:
    return str(row.get("market_slug") or ""), str(row.get("submitted_at") or "")


def _filled_event_index(scorecard: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    canonical = scorecard.get("canonical_pnl_truth") if isinstance(scorecard.get("canonical_pnl_truth"), dict) else {}
    events = canonical.get("events") if isinstance(canonical.get("events"), list) else []
    return {
        _event_key(row): row
        for row in events
        if isinstance(row, dict) and str(row.get("status") or "").upper() == "FILLED"
    }


def build_scaling_gate(scorecard: dict[str, Any]) -> dict[str, Any]:
    rows = scorecard.get("late_window_cohort", {}).get("rows") if isinstance(scorecard.get("late_window_cohort"), dict) else []
    rows = rows if isinstance(rows, list) else []
    event_index = _filled_event_index(scorecard)
    bucket_rows: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        seconds_to_close = row.get("seconds_to_close_s")
        try:
            entry_offset_s = FIVE_MIN_WINDOW_S - float(seconds_to_close)
        except (TypeError, ValueError):
            entry_offset_s = None
        event = event_index.get(_event_key(row), {})
        out = {
            "market_slug": row.get("market_slug"),
            "submitted_at": row.get("submitted_at"),
            "entry_offset_s": round(entry_offset_s, 6) if entry_offset_s is not None else None,
            "seconds_to_close_s": row.get("seconds_to_close_s"),
            "resolved": bool(row.get("resolved")),
            "pnl_usd": float(row.get("pnl_usd") or 0.0),
            "cost_usd": float(event.get("cost_usd") or 0.0),
            "source_wallet": event.get("source_wallet"),
            "side": event.get("side"),
            "winner": event.get("winner"),
        }
        is_early = entry_offset_s is not None and 0.0 <= entry_offset_s < EARLY_ENTRY_MAX_S and bool(row.get("resolved"))
        out["canonical_early_entry"] = is_early
        all_rows.append(out)
        if is_early:
            bucket_rows.append(out)
    pnl = sum(float(row.get("pnl_usd") or 0.0) for row in bucket_rows)
    cost = sum(float(row.get("cost_usd") or 0.0) for row in bucket_rows)
    roi_pct = (100.0 * pnl / cost) if cost else 0.0
    sample_n = len(bucket_rows)
    if sample_n >= SCALE_SAMPLE_FLOOR and roi_pct > 0.0:
        verdict = "SCALE_RAISE_GATE_MET"
        action = "EXECUTE_RAISE"
    elif sample_n < SCALE_SAMPLE_FLOOR:
        verdict = "NO_RAISE_SAMPLE_FLOOR_NOT_MET"
        action = "DO_NOT_RAISE"
    else:
        verdict = "NO_RAISE_ROI_NOT_POSITIVE"
        action = "DO_NOT_RAISE"
    return {
        "flow_stage": "LIVE/LEARN",
        "rule": "02:05Z pre-registration: early-window (<60s entry) resolved fills n>=10 and ROI>0 raises drip tranche 1.25->2.5",
        "basis": "scorecard.late_window_cohort.rows joined to canonical_pnl_truth.events by market_slug/submitted_at",
        "early_entry_max_s": EARLY_ENTRY_MAX_S,
        "sample_floor": SCALE_SAMPLE_FLOOR,
        "current_tranche_usd": SCALE_FROM_TRANCHE_USD,
        "target_tranche_usd": SCALE_TO_TRANCHE_USD,
        "sample_n": sample_n,
        "pnl_usd": round(pnl, 6),
        "cost_usd": round(cost, 6),
        "roi_pct": round(roi_pct, 6),
        "verdict": verdict,
        "action": action,
        "live_guard_reload_required": action == "EXECUTE_RAISE",
        "rows": bucket_rows,
        "all_fill_rows": all_rows,
    }


def _reason_gate(reason: str) -> str:
    if reason in FRESHNESS_REASONS:
        return "freshness"
    if reason in BAND_REASONS:
        return "band"
    if reason in ENVELOPE_REASONS:
        return "envelope"
    if reason in PARITY_REASONS:
        return "parity"
    return "unknown"


def _since_window_start(deadman: dict[str, Any]) -> float | None:
    latest = _parse_ts(deadman.get("latest_order_ts"))
    if latest is None:
        return None
    return float(int(latest.timestamp() // FIVE_MIN_WINDOW_S) * int(FIVE_MIN_WINDOW_S))


def _eligible_weight_by_gate(row: dict[str, Any], reasons: dict[str, int]) -> dict[str, float]:
    wallet_orders = float(row.get("wallet_eligible_orders") or 0.0)
    total_reasons = sum(max(0, int(value or 0)) for value in reasons.values())
    if wallet_orders <= 0.0 or total_reasons <= 0:
        return {}
    weights: dict[str, float] = defaultdict(float)
    for reason, count in reasons.items():
        weights[_reason_gate(str(reason))] += wallet_orders * (max(0, int(count or 0)) / total_reasons)
    return dict(weights)


def build_reject_taxonomy(scorecard: dict[str, Any], deadman: dict[str, Any], guard: dict[str, Any]) -> dict[str, Any]:
    volume = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    rows = volume.get("rows") if isinstance(volume.get("rows"), list) else []
    since_start = _since_window_start(deadman)
    selected_rows: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    gate_counts: Counter[str] = Counter()
    eligible_weighted_gate_counts: Counter[str] = Counter()
    wallet_order_total = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            window_start_s = float(row.get("window_start_s"))
        except (TypeError, ValueError):
            continue
        if since_start is not None and window_start_s < since_start:
            continue
        if str(row.get("missed_window_attribution") or "") != "guard_reject":
            continue
        if int(row.get("our_submits") or 0) != 0 or int(row.get("our_fills") or 0) != 0:
            continue
        reasons = row.get("skip_reasons") if isinstance(row.get("skip_reasons"), dict) else {}
        row_reason_counts = {str(key): int(value or 0) for key, value in reasons.items()}
        row_gate_counts: Counter[str] = Counter()
        for reason, count in row_reason_counts.items():
            reason_counts[reason] += count
            gate = _reason_gate(reason)
            gate_counts[gate] += count
            row_gate_counts[gate] += count
        for gate, weight in _eligible_weight_by_gate(row, row_reason_counts).items():
            eligible_weighted_gate_counts[gate] += weight
        wallet_orders = float(row.get("wallet_eligible_orders") or 0.0)
        wallet_order_total += wallet_orders
        selected_rows.append(
            {
                "market_slug": row.get("market_slug"),
                "window_start_s": window_start_s,
                "wallet_eligible_orders": int(wallet_orders),
                "skip_reasons": row_reason_counts,
                "gate_counts": dict(sorted(row_gate_counts.items())),
                "our_submits": int(row.get("our_submits") or 0),
                "our_fills": int(row.get("our_fills") or 0),
                "empty_window_reason": row.get("empty_window_reason"),
            }
        )
    weighted_total = sum(float(value) for value in eligible_weighted_gate_counts.values())
    freshness_weighted = float(eligible_weighted_gate_counts.get("freshness") or 0.0)
    freshness_weighted_pct = (100.0 * freshness_weighted / weighted_total) if weighted_total else 0.0
    reason_total = sum(reason_counts.values())
    freshness_reason_count = sum(reason_counts.get(reason, 0) for reason in FRESHNESS_REASONS)
    freshness_reason_pct = (100.0 * freshness_reason_count / reason_total) if reason_total else 0.0
    stale_rows = int(deadman.get("fresh_stale_signal_rows") or 0)
    approved_suppression_events = int(deadman.get("approved_suppression_events") or 0)
    verdict = "MIXED_REJECT_TAXONOMY"
    if freshness_weighted_pct >= 90.0 or (
        stale_rows > 0
        and approved_suppression_events >= stale_rows
        and set(str(tag) for tag in deadman.get("approved_suppression_tags") or []) <= FRESHNESS_REASONS
    ):
        verdict = "EXPECTED_THIN_FLOW"
    return {
        "flow_stage": "LIVE/LEARN",
        "source": "current-day scorecard volume_kpi.rows filtered to guard_reject windows after latest accepted order",
        "since_latest_order_ts": deadman.get("latest_order_ts"),
        "since_window_start_s": since_start,
        "window_count": len(selected_rows),
        "wallet_eligible_orders": int(wallet_order_total),
        "reason_counts": dict(sorted(reason_counts.items())),
        "gate_counts": dict(sorted(gate_counts.items())),
        "eligible_order_weighted_gate_counts": {
            key: round(float(value), 6) for key, value in sorted(eligible_weighted_gate_counts.items())
        },
        "freshness_reason_pct": round(freshness_reason_pct, 6),
        "freshness_weighted_pct": round(freshness_weighted_pct, 6),
        "deadman_fresh_stale_signal_rows": stale_rows,
        "deadman_approved_suppression_events": approved_suppression_events,
        "deadman_approved_suppression_tags": deadman.get("approved_suppression_tags") or [],
        "guard_window_participation_counts": (
            guard.get("window_participation", {}).get("dominant_skip_reason_counts")
            if isinstance(guard.get("window_participation"), dict)
            else {}
        ),
        "verdict": verdict,
        "gate_action": "NO_GATE_CHANGE",
        "fix": "more fresh flow; do not loosen the 60s freshness gate on a green day",
        "rows": selected_rows,
    }


def build_report(scorecard: dict[str, Any], deadman: dict[str, Any], guard: dict[str, Any], *, scorecard_path: Path) -> dict[str, Any]:
    scaling = build_scaling_gate(scorecard)
    taxonomy = build_reject_taxonomy(scorecard, deadman, guard)
    return {
        "schema_version": 1,
        "kind": "wallet_copy_live_profit_queue_report",
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "generated_at": _utc_now_iso(),
        "scorecard_path": str(scorecard_path.relative_to(ROOT) if scorecard_path.is_relative_to(ROOT) else scorecard_path),
        "scorecard_generated_at": scorecard.get("generated_at"),
        "day_utc": scorecard.get("day_utc"),
        "p1d_scaling": scaling,
        "p2b_reject_taxonomy": taxonomy,
        "live_path_change": scaling["action"] == "EXECUTE_RAISE",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default="")
    parser.add_argument("--scorecard", default="")
    parser.add_argument("--deadman", default="data/research/order_flow_deadman_state.json")
    parser.add_argument("--guard", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--reject-output", default=DEFAULT_REJECT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scorecard_path = _rooted(args.scorecard) if args.scorecard else _default_current_scorecard(args.day or None)
    deadman_path = _rooted(args.deadman)
    guard_path = _rooted(args.guard)
    from src.wallet_copy.scorecard import load_fresh_scorecard

    scorecard = load_fresh_scorecard(scorecard_path)
    deadman = _load_json(deadman_path, {})
    guard = _load_json(guard_path, {})
    if not isinstance(scorecard, dict) or not scorecard:
        print(json.dumps({"status": "ERROR", "reason": "missing_scorecard", "path": str(scorecard_path)}))
        return 2
    report = build_report(
        scorecard,
        deadman if isinstance(deadman, dict) else {},
        guard if isinstance(guard, dict) else {},
        scorecard_path=scorecard_path,
    )
    atomic_write_json(_rooted(args.output), report)
    reject_payload = {
        "schema_version": 2,
        "kind": "wallet_copy_live_reject_reason_classification",
        "generated_at": report["generated_at"],
        "flow_stage": report["flow_stage"],
        **report["p2b_reject_taxonomy"],
    }
    atomic_write_json(_rooted(args.reject_output), reject_payload)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
