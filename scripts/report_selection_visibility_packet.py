#!/usr/bin/env python3
"""Build the ORDER2 selection visibility packet.

Flow stage: LEARN/SELF-DEV. Reporting only: this script reads paper/shadow
routing artifacts, writes an evidence packet, and never mutates live routing.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_EXPERIMENT_ID = "campaign-lat-selection-visibility-20260710"
DEFAULT_REGISTRY = "data/research/experiment_preregistry.jsonl"
DEFAULT_ROUTING_SHADOW = "data/research/routing_shadow_validation_latest.json"
DEFAULT_OUTPUT = "data/research/selection_visibility_packet_latest.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rooted(path: str | Path) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _round(value: float | None) -> float | None:
    return None if value is None else round(float(value), 6)


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(100.0 * numerator / denominator, 6)


def _load_latest_preregistry_record(registry_path: str | Path, experiment_id: str) -> dict[str, Any]:
    path = _rooted(registry_path)
    latest: dict[str, Any] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return latest
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and row.get("experiment_id") == experiment_id:
            latest = row
    return latest


def _window_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("market_slug") or ""), str(row.get("window_start_s") or ""))


def _has_direction(row: dict[str, Any]) -> bool:
    return bool(str(row.get("side") or "").strip() and str(row.get("outcome") or "").strip())


def _measured_post_fee(row: dict[str, Any]) -> tuple[float | None, float | None, float | None, float | None]:
    outcome = row.get("realized_paper_outcome") if isinstance(row.get("realized_paper_outcome"), dict) else {}
    pre_fee = _float(outcome.get("paper_pnl_usd"))
    fee = _float(row.get("expected_fee_usd"))
    price = _float(row.get("limit_price"))
    shares = _float(row.get("shares"))
    cost = price * shares if price is not None and shares is not None else None
    if pre_fee is None:
        return None, None, fee, cost
    post_fee = pre_fee - fee if fee is not None else pre_fee
    return pre_fee, post_fee, fee, cost


def _row_preference(row: dict[str, Any]) -> tuple[int, int, float, str, str]:
    pre_fee, post_fee, _fee, _cost = _measured_post_fee(row)
    observed = _float(row.get("winning_observed_ts"))
    return (
        0 if post_fee is not None and pre_fee is not None else 1,
        0 if _has_direction(row) else 1,
        observed if observed is not None else float("inf"),
        str(row.get("winning_source_wallet") or ""),
        str(row.get("winning_intent_id") or ""),
    )


def _dedupe_extra_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_window: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = _window_key(row)
        if not key[0] and not key[1]:
            continue
        existing = by_window.get(key)
        if existing is None or _row_preference(row) < _row_preference(existing):
            by_window[key] = row
    return sorted(by_window.values(), key=lambda row: (str(row.get("cycle_generated_at") or ""), _window_key(row)))


def _selector_reason(row: dict[str, Any]) -> tuple[str, str]:
    runtime_wallet = str(row.get("runtime_selected_wallet") or row.get("selected_wallet_at_cycle") or "")
    if not bool(row.get("selected_wallet_signal_present")) and runtime_wallet:
        return (
            "runtime_selected_wallet_no_routeable_signal",
            "Shadow winner emitted a routeable signal, but the runtime-selected last-successful member had no routeable signal for this window.",
        )
    if bool(row.get("would_change_selected_wallet")):
        return (
            "shadow_winner_differs_from_runtime_selected_wallet",
            "Shadow router would select a different source wallet than the runtime-selected member for this window.",
        )
    if _int(row.get("routing_suppressed_signals")) > 0:
        return (
            "one_position_per_window_suppressed_competing_signal",
            "A routeable signal was present but lost the one-position-per-window shadow routing decision.",
        )
    return (
        "signal_emitted_not_selected_unclassified",
        "A signal was emitted but the current retained row lacks enough selector context for a sharper reason.",
    )


def _normalized_row(row: dict[str, Any]) -> dict[str, Any]:
    reason_code, reason_detail = _selector_reason(row)
    outcome = row.get("realized_paper_outcome") if isinstance(row.get("realized_paper_outcome"), dict) else {}
    pre_fee, post_fee, fee, cost = _measured_post_fee(row)
    missing_fields = [
        field
        for field, value in {
            "side": row.get("side"),
            "outcome": row.get("outcome"),
            "limit_price": row.get("limit_price"),
            "shares": row.get("shares"),
            "expected_fee_usd": row.get("expected_fee_usd"),
        }.items()
        if value is None or str(value).strip() == ""
    ]
    measurement_gap = outcome.get("measurement_gap_reason")
    if post_fee is None and not measurement_gap:
        measurement_gap = outcome.get("status") or "UNMEASURED"
    return {
        "window_id": row.get("market_slug"),
        "market_slug": row.get("market_slug"),
        "window_start_s": row.get("window_start_s"),
        "cycle_generated_at": row.get("cycle_generated_at"),
        "signal_ts": row.get("winning_observed_ts"),
        "signal_wallet": row.get("winning_source_wallet"),
        "signal_wallet_short": row.get("winning_source_wallet_short"),
        "runtime_selected_wallet": row.get("runtime_selected_wallet") or row.get("selected_wallet_at_cycle"),
        "runtime_selected_wallet_short": row.get("runtime_selected_wallet_short"),
        "selected_wallet_signal_present": bool(row.get("selected_wallet_signal_present")),
        "selector_reason_code": reason_code,
        "selector_reason_detail": reason_detail,
        "selector_reason_taxonomy_version": 1,
        "would_submit_side": row.get("side"),
        "would_submit_outcome": row.get("outcome"),
        "would_submit_price": row.get("limit_price"),
        "would_submit_size": row.get("shares"),
        "would_submit_cost_usd": _round(cost),
        "would_submit_fee_usd": _round(fee),
        "would_submit_pre_fee_pnl_usd": _round(pre_fee),
        "would_submit_post_fee_pnl_usd": _round(post_fee),
        "would_submit_pnl_status": "MEASURED" if post_fee is not None else "UNMEASURED",
        "would_submit_measurement_source": row.get("would_submit_measurement_source")
        or ("routing_shadow_decision_row" if not missing_fields else None),
        "measurement_gap_reason": measurement_gap,
        "would_submit_pnl_fee_field_status": "PRESENT" if not missing_fields else "MISSING_FIELDS",
        "missing_would_submit_fields": missing_fields,
        "realized_paper_outcome": outcome,
        "copyintent_parity_conflict": bool(row.get("copyintent_parity_conflict", False)),
        "paper_only": True,
        "live_orders_allowed": False,
        "source_extra_would_submit_window": bool(row.get("extra_would_submit_window")),
        "source_tiebreak_rule": row.get("tiebreak_rule"),
        "winning_intent_id": row.get("winning_intent_id"),
        "winning_candidate_id": row.get("winning_candidate_id"),
        "winning_policy_id": row.get("winning_policy_id"),
    }


def _rows_after_registered(
    rows: list[dict[str, Any]],
    *,
    refresh_generated_at: str,
    registered_at: str,
) -> tuple[list[dict[str, Any]], int]:
    registered_dt = _parse_dt(registered_at)
    if registered_dt is None:
        return rows, 0
    _ = refresh_generated_at
    kept: list[dict[str, Any]] = []
    excluded = 0
    for row in rows:
        cycle_dt = _parse_dt(row.get("cycle_generated_at"))
        if cycle_dt is not None and cycle_dt >= registered_dt:
            kept.append(row)
        else:
            excluded += 1
    return kept, excluded


def _fee_measurement_indexes(
    routing_shadow: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str, str], list[dict[str, Any]]], dict[tuple[str, str], list[dict[str, Any]]]]:
    by_intent: dict[str, dict[str, Any]] = {}
    by_window_wallet: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    by_market_wallet: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in routing_shadow.get("fee_gated_measurement_rows") or []:
        if not isinstance(row, dict):
            continue
        intent_id = str(row.get("intent_id") or "")
        if intent_id:
            by_intent[intent_id] = row
        market = str(row.get("market_slug") or "")
        wallet = str(row.get("source_wallet") or "")
        window = str(row.get("window_start_s") or "")
        if market and wallet and window:
            by_window_wallet[(market, window, wallet)].append(row)
        if market and wallet:
            by_market_wallet[(market, wallet)].append(row)
    return by_intent, by_window_wallet, by_market_wallet


def _closest_measurement(row: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    signal_ts = _float(row.get("winning_observed_ts"))

    def rank(candidate: dict[str, Any]) -> tuple[float, str]:
        observed_ts = _float(candidate.get("observed_ts"))
        distance = abs((observed_ts or 0.0) - (signal_ts or 0.0)) if observed_ts is not None and signal_ts is not None else float("inf")
        return (distance, str(candidate.get("intent_id") or ""))

    return sorted(candidates, key=rank)[0]


def _find_fee_measurement(
    row: dict[str, Any],
    by_intent: dict[str, dict[str, Any]],
    by_window_wallet: dict[tuple[str, str, str], list[dict[str, Any]]],
    by_market_wallet: dict[tuple[str, str], list[dict[str, Any]]],
) -> tuple[dict[str, Any] | None, str | None]:
    intent_id = str(row.get("winning_intent_id") or "")
    if intent_id and intent_id in by_intent:
        return by_intent[intent_id], "intent_id"
    market = str(row.get("market_slug") or "")
    wallet = str(row.get("winning_source_wallet") or "")
    window = str(row.get("window_start_s") or "")
    measurement = _closest_measurement(row, by_window_wallet.get((market, window, wallet), []))
    if measurement is not None:
        return measurement, "market_window_wallet_closest_observed_ts"
    measurement = _closest_measurement(row, by_market_wallet.get((market, wallet), []))
    if measurement is not None:
        return measurement, "market_wallet_closest_observed_ts"
    return None, None


def _merge_fee_measurement(
    row: dict[str, Any],
    measurement: dict[str, Any] | None,
    source: str | None,
) -> dict[str, Any]:
    if not measurement:
        merged = dict(row)
        merged["would_submit_measurement_source"] = "routing_shadow_rows_only"
        return merged
    merged = dict(row)
    for source_field, target in (
        ("side", "side"),
        ("outcome", "outcome"),
        ("limit_price", "limit_price"),
        ("shares", "shares"),
        ("expected_fee_usd", "expected_fee_usd"),
        ("expected_fee_rate", "expected_fee_rate"),
        ("copyintent_parity_conflict", "copyintent_parity_conflict"),
        ("copyintent_source_wallet", "copyintent_source_wallet"),
    ):
        if source_field in measurement:
            merged[target] = measurement.get(source_field)
    if isinstance(measurement.get("realized_paper_outcome"), dict):
        merged["realized_paper_outcome"] = measurement["realized_paper_outcome"]
    if measurement.get("observed_ts") is not None:
        merged["fee_measurement_observed_ts"] = measurement.get("observed_ts")
    merged["would_submit_measurement_source"] = f"routing_shadow_fee_gated_measurement_rows:{source or 'unknown'}"
    return merged


def build_packet(
    *,
    routing_shadow: dict[str, Any],
    preregistration: dict[str, Any],
    generated_at: str,
    experiment_id: str = DEFAULT_EXPERIMENT_ID,
    min_clock_start_samples: int = 10,
) -> dict[str, Any]:
    source_rows = [row for row in routing_shadow.get("rows") or [] if isinstance(row, dict)]
    fee_by_intent, fee_by_window_wallet, fee_by_market_wallet = _fee_measurement_indexes(routing_shadow)
    extra_rows: list[dict[str, Any]] = []
    join_counts: dict[str, int] = {}
    for row in source_rows:
        if not bool(row.get("extra_would_submit_window")):
            continue
        measurement, source = _find_fee_measurement(row, fee_by_intent, fee_by_window_wallet, fee_by_market_wallet)
        join_key = source or "routing_shadow_rows_only"
        join_counts[join_key] = join_counts.get(join_key, 0) + 1
        extra_rows.append(_merge_fee_measurement(row, measurement, source))
    post_registration_rows, excluded_pre_registration = _rows_after_registered(
        extra_rows,
        refresh_generated_at=str(routing_shadow.get("generated_at") or ""),
        registered_at=str(preregistration.get("registered_at") or ""),
    )
    sampled_rows = [_normalized_row(row) for row in _dedupe_extra_rows(post_registration_rows)]
    sampled = len(sampled_rows)
    sampled_join_counts: dict[str, int] = {}
    for row in sampled_rows:
        join_source = str(row.get("would_submit_measurement_source") or "unknown")
        sampled_join_counts[join_source] = sampled_join_counts.get(join_source, 0) + 1
    reason_rows = [
        row
        for row in sampled_rows
        if row["selector_reason_code"] and row["selector_reason_code"] != "signal_emitted_not_selected_unclassified"
    ]
    field_present_rows = [
        row for row in sampled_rows if row.get("would_submit_pnl_fee_field_status") == "PRESENT"
    ]
    measured_rows = [row for row in sampled_rows if row.get("would_submit_post_fee_pnl_usd") is not None]
    post_fee_sum = round(sum(float(row.get("would_submit_post_fee_pnl_usd") or 0.0) for row in measured_rows), 6)
    pre_fee_sum = round(sum(float(row.get("would_submit_pre_fee_pnl_usd") or 0.0) for row in measured_rows), 6)
    fee_sum = round(sum(float(row.get("would_submit_fee_usd") or 0.0) for row in measured_rows), 6)
    cost_sum = round(sum(float(row.get("would_submit_cost_usd") or 0.0) for row in measured_rows), 6)
    roi_pct = None if cost_sum <= 0 else round(100.0 * post_fee_sum / cost_sum, 6)
    shadow_summary = routing_shadow.get("summary") if isinstance(routing_shadow.get("summary"), dict) else {}
    parity_status = str(shadow_summary.get("copyintent_parity_status") or "")
    parity_conflicts = _int(shadow_summary.get("copyintent_parity_conflicts"))
    live_orders_allowed = bool(routing_shadow.get("live_orders_allowed"))
    producing_live_mutation = False
    reason_coverage = _pct(len(reason_rows), sampled)
    field_coverage = _pct(len(field_present_rows), sampled)
    live_flags_clear = not live_orders_allowed and not producing_live_mutation
    clock_start_condition_met = (
        sampled >= int(min_clock_start_samples)
        and parity_status == "PASS"
        and parity_conflicts == 0
        and live_flags_clear
    )
    instrumentation_fail_reasons: list[str] = []
    if sampled and reason_coverage < 90.0:
        instrumentation_fail_reasons.append("selector_reason_coverage_below_90_pct")
    if sampled and (100.0 - field_coverage) > 10.0:
        instrumentation_fail_reasons.append("missing_pnl_fee_fields_above_10_pct")
    if parity_status != "PASS" or parity_conflicts:
        instrumentation_fail_reasons.append("copyintent_parity_conflict")
    if not live_flags_clear:
        instrumentation_fail_reasons.append("live_mutation_flag_set")
    if not sampled:
        gate_status = "AWAITING_POST_REGISTRATION_SAMPLE"
    elif instrumentation_fail_reasons:
        gate_status = "INSTRUMENTATION_FAIL"
    elif not clock_start_condition_met:
        gate_status = "AWAITING_CLOCK_START_SAMPLE"
    elif sampled < 50 and measured_rows:
        gate_status = "CLOCK_STARTED_ACCRUING"
    elif sampled < 50:
        gate_status = "CLOCK_STARTED_AWAITING_RESOLUTIONS"
    elif post_fee_sum > 0.0 and (roi_pct is not None and roi_pct > 0.0):
        gate_status = "SUCCESS_CANARY_PACKET_ELIGIBLE"
    else:
        gate_status = "INFORMATIVE_FAIL_SELECTOR_ABSTAINS_SUPPORTED"

    return {
        "schema_version": 1,
        "kind": "selection_visibility_packet",
        "flow_stage": "LEARN/SELF-DEV",
        "paper_only": True,
        "live_orders_allowed": False,
        "producing_live_mutation": False,
        "generated_at": generated_at,
        "experiment_id": experiment_id,
        "preregistration": {
            "registered_at": preregistration.get("registered_at"),
            "deadline_utc": preregistration.get("deadline_utc"),
            "notes": preregistration.get("notes"),
        },
        "summary": {
            "status": gate_status,
            "clock_start_condition_met": clock_start_condition_met,
            "clock_started_at": generated_at if clock_start_condition_met else None,
            "clock_start_rule": (
                ">=10 post-registration signal-emitted-but-not-selected BTC5M sampled windows, "
                "CopyIntent parity PASS, live_orders_allowed=false, producing_live_mutation=false"
            ),
            "sample_basis": "post_registration_routing_shadow_extra_would_submit_windows",
            "source_extra_would_submit_rows": len(extra_rows),
            "excluded_pre_registration_rows": excluded_pre_registration,
            "post_registration_extra_rows": len(post_registration_rows),
            "sampled_signal_emitted_but_not_selected_windows": sampled,
            "selector_reason_coverage_pct": reason_coverage,
            "selector_reason_rows": len(reason_rows),
            "would_submit_pnl_fee_field_coverage_pct": field_coverage,
            "would_submit_pnl_fee_field_rows": len(field_present_rows),
            "would_submit_measurement_join_counts": dict(sorted(join_counts.items())),
            "sampled_would_submit_measurement_join_counts": dict(sorted(sampled_join_counts.items())),
            "measured_unique_windows": len(measured_rows),
            "unmeasured_unique_windows": sampled - len(measured_rows),
            "aggregate_measured_would_submit_pre_fee_pnl_usd": pre_fee_sum,
            "aggregate_measured_would_submit_fee_usd": fee_sum,
            "aggregate_measured_would_submit_post_fee_pnl_usd": post_fee_sum,
            "aggregate_measured_would_submit_cost_usd": cost_sum,
            "aggregate_measured_would_submit_roi_pct": roi_pct,
            "copyintent_parity_status": parity_status,
            "copyintent_parity_conflicts": parity_conflicts,
            "instrumentation_fail_reasons": instrumentation_fail_reasons,
            "informative_fail_semantics": (
                "If instrumentation gates pass but post-fee PnL/ROI are <=0, record evidence for selector abstains; "
                "do not change instrumentation."
            ),
        },
        "sources": {
            "routing_shadow": DEFAULT_ROUTING_SHADOW,
            "registry": DEFAULT_REGISTRY,
        },
        "rows": sampled_rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--routing-shadow", default=DEFAULT_ROUTING_SHADOW)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--experiment-id", default=DEFAULT_EXPERIMENT_ID)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--min-clock-start-samples", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    routing_shadow = load_json(_rooted(args.routing_shadow), default={})
    preregistration = _load_latest_preregistry_record(args.registry, str(args.experiment_id))
    packet = build_packet(
        routing_shadow=routing_shadow if isinstance(routing_shadow, dict) else {},
        preregistration=preregistration,
        generated_at=_utc_now_iso(),
        experiment_id=str(args.experiment_id),
        min_clock_start_samples=int(args.min_clock_start_samples),
    )
    packet["sources"] = {
        "routing_shadow": args.routing_shadow,
        "registry": args.registry,
    }
    atomic_write_json(_rooted(args.output), packet)
    print(json.dumps(packet["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
