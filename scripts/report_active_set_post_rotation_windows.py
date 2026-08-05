#!/usr/bin/env python3
"""Summarize the first measured active windows after an active-set rotation."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_GUARD = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_ROTATION = ROOT / "data/research/active_set_selected_rotation_execution_latest.json"
DEFAULT_LIVE_EXECUTION = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/active_set_post_rotation_windows_latest.json"
DEFAULT_TARGET_WALLET = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
DEFAULT_DECISION_AGE_THRESHOLD_S = 3.0


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") else ""


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _parse_ts(value: Any) -> datetime | None:
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


def _ts(value: Any) -> float | None:
    number = _as_float(value)
    if number is not None:
        return number
    parsed = _parse_ts(value)
    return parsed.timestamp() if parsed is not None else None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _window_start_for_ts(ts: float | None) -> float | None:
    if ts is None:
        return None
    return math.floor(ts / 300.0) * 300.0


def _rotation_window_start(rotation: dict[str, Any]) -> float | None:
    pin = _dict(rotation.get("selection_pin"))
    created = _parse_ts(pin.get("created_at") or rotation.get("generated_at"))
    return _window_start_for_ts(created.timestamp()) if created is not None else None


def _is_measured_window(row: dict[str, Any]) -> bool:
    if row.get("miss_pending_market_lifecycle") is True:
        return False
    if row.get("source_coverage_window") is True or row.get("participation_adjusted_denominator") is True:
        return True
    if row.get("measured_skip_window") is True:
        return True
    try:
        return int(row.get("wallet_eligible_orders") or 0) > 0
    except (TypeError, ValueError):
        return False


def _submit_eligible(row: dict[str, Any]) -> bool:
    for key in ("our_attempts", "our_submits", "our_fills"):
        try:
            if int(row.get(key) or 0) > 0:
                return True
        except (TypeError, ValueError):
            pass
    reason = str(row.get("dominant_skip_reason") or "").lower()
    category = str(row.get("participation_skip_category") or "").upper()
    return reason in {"eligible", "filled"} or category == "SUBMITTED"


def _nested_dict(value: Any, *keys: str) -> dict[str, Any]:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return current if isinstance(current, dict) else {}


def _live_build_max_age_s(row: dict[str, Any], order: dict[str, Any] | None) -> float | None:
    order = order if isinstance(order, dict) else {}
    source_intent = _nested_dict(order, "source_intent")
    metadata = _nested_dict(source_intent, "metadata")
    inventory = _nested_dict(metadata, "inventory_v2")
    for value in (
        inventory.get("live_build_max_observed_age_s"),
        metadata.get("live_build_max_observed_age_s"),
        row.get("live_build_max_observed_age_s"),
        _nested_dict(row, "stale_drop_audit").get("live_build_max_observed_age_s"),
    ):
        number = _as_float(value)
        if number is not None:
            return number
    return None


def _order_submit_ts(order: dict[str, Any]) -> float | None:
    latency = _dict(order.get("latency_budget"))
    for value in (
        latency.get("intent_built_ts"),
        latency.get("submit_sent_ts"),
        order.get("submitted_at"),
    ):
        ts = _ts(value)
        if ts is not None:
            return ts
    for event in _list(order.get("lifecycle")):
        if not isinstance(event, dict) or str(event.get("status") or "") != "LIVE_SUBMITTED":
            continue
        ts = _ts(event.get("ts"))
        if ts is not None:
            return ts
    return None


def _order_observed_ts(order: dict[str, Any]) -> float | None:
    source_intent = _nested_dict(order, "source_intent")
    metadata = _nested_dict(source_intent, "metadata")
    inventory = _nested_dict(metadata, "inventory_v2")
    latency = _dict(order.get("latency_budget"))
    for value in (
        source_intent.get("observed_ts"),
        inventory.get("latest_observed_ts"),
        inventory.get("source_detection_observed_ts"),
        latency.get("ws_recv_ts"),
    ):
        ts = _ts(value)
        if ts is not None:
            return ts
    return None


def _order_decision_age_s(order: dict[str, Any]) -> float | None:
    source_intent = _nested_dict(order, "source_intent")
    metadata = _nested_dict(source_intent, "metadata")
    inventory = _nested_dict(metadata, "inventory_v2")
    direct = _as_float(inventory.get("latest_observed_age_s"))
    if direct is not None:
        return direct
    decision_ts = _order_submit_ts(order)
    observed_ts = _order_observed_ts(order)
    if decision_ts is None or observed_ts is None:
        return None
    return max(0.0, decision_ts - observed_ts)


def _order_key(order: dict[str, Any]) -> tuple[str, str]:
    return (_wallet(order.get("source_wallet")), str(order.get("market_slug") or ""))


def _orders_by_wallet_market(
    live_execution: dict[str, Any] | None,
    *,
    rotation_at_ts: float | None,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    live_execution = live_execution if isinstance(live_execution, dict) else {}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for raw_order in _list(live_execution.get("orders")):
        if not isinstance(raw_order, dict):
            continue
        submitted_ts = _ts(raw_order.get("submitted_at"))
        if rotation_at_ts is not None and (submitted_ts is None or submitted_ts < rotation_at_ts):
            continue
        key = _order_key(raw_order)
        if not key[0] or not key[1]:
            continue
        grouped.setdefault(key, []).append(raw_order)
    for orders in grouped.values():
        orders.sort(key=lambda order: _order_submit_ts(order) or _ts(order.get("submitted_at")) or 0.0)
    return grouped


def _matching_order(
    row: dict[str, Any],
    orders_by_wallet_market: dict[tuple[str, str], list[dict[str, Any]]],
) -> dict[str, Any] | None:
    key = (_wallet(row.get("source_wallet")), str(row.get("market_slug") or ""))
    orders = orders_by_wallet_market.get(key) or []
    if not orders:
        return None
    outcome = str(row.get("outcome") or "").strip().lower()
    condition_id = str(row.get("condition_id") or "").strip().lower()
    for order in orders:
        if condition_id and str(order.get("condition_id") or "").strip().lower() != condition_id:
            continue
        if outcome and str(order.get("outcome") or "").strip().lower() != outcome:
            continue
        return order
    return orders[0] if not condition_id and not outcome else None


def _window_group_key(row: dict[str, Any]) -> tuple[float, str]:
    return (_as_float(row.get("window_start_s")) or 0.0, str(row.get("market_slug") or ""))


def _select_window_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    submitted = [row for row in rows if _submit_eligible(row)]
    if submitted:
        return sorted(
            submitted,
            key=lambda row: str(row.get("last_seen_at") or row.get("first_seen_at") or ""),
            reverse=True,
        )[0]
    return sorted(
        rows,
        key=lambda row: (
            int(row.get("wallet_eligible_orders") or 0),
            str(row.get("last_seen_at") or row.get("first_seen_at") or ""),
        ),
        reverse=True,
    )[0]


def _row_decision_age(row: dict[str, Any]) -> tuple[float | None, str, float | None, str, float | None]:
    decision_ts = _ts(row.get("last_seen_at")) or _ts(row.get("first_seen_at"))
    if decision_ts is None:
        snapshot = _as_float(row.get("latest_observed_age_s"))
        return snapshot, "row.latest_observed_age_s_fallback", None, "", None
    candidates = [
        ("freshness_watermark_ts", _ts(row.get("freshness_watermark_ts"))),
        ("effective_latest_observed_ts", _ts(row.get("effective_latest_observed_ts"))),
        ("latest_observed_ts", _ts(row.get("latest_observed_ts"))),
        ("source_detection_observed_ts", _ts(row.get("source_detection_observed_ts"))),
        ("source_latest_observed_ts", _ts(row.get("source_latest_observed_ts"))),
    ]
    valid = [(name, ts) for name, ts in candidates if ts is not None and ts <= decision_ts]
    if not valid:
        snapshot = _as_float(row.get("latest_observed_age_s"))
        return snapshot, "row.latest_observed_age_s_fallback", decision_ts, "", None
    source_name, observed_ts = max(valid, key=lambda item: item[1])
    return max(0.0, decision_ts - observed_ts), f"row.{source_name}", decision_ts, source_name, observed_ts


def _decision_age(
    row: dict[str, Any],
    *,
    order: dict[str, Any] | None,
) -> dict[str, Any]:
    if order is not None:
        age = _order_decision_age_s(order)
        if age is not None:
            return {
                "decision_time_observed_age_s": round(age, 6),
                "decision_time_observed_age_source": (
                    "live_order.source_intent.metadata.inventory_v2.latest_observed_age_s"
                    if _as_float(
                        _nested_dict(
                            _nested_dict(order, "source_intent"),
                            "metadata",
                            "inventory_v2",
                        ).get("latest_observed_age_s")
                    )
                    is not None
                    else "live_order.submit_ts_minus_source_intent_observed_ts"
                ),
                "decision_time_ts": _order_submit_ts(order),
                "decision_time_source": "live_order.latency_budget.intent_built_ts_or_submitted_at",
                "decision_observed_ts": _order_observed_ts(order),
                "decision_observed_ts_source": "live_order.source_intent.observed_ts",
            }
    age, source, decision_ts, observed_source, observed_ts = _row_decision_age(row)
    return {
        "decision_time_observed_age_s": None if age is None else round(age, 6),
        "decision_time_observed_age_source": source,
        "decision_time_ts": decision_ts,
        "decision_time_source": "row.last_seen_at_or_first_seen_at" if decision_ts is not None else "",
        "decision_observed_ts": observed_ts,
        "decision_observed_ts_source": observed_source,
    }


def _age_bucket(age: float | None, *, threshold_s: float) -> str:
    if age is None:
        return "missing"
    return f"<={threshold_s:g}s" if age <= threshold_s else f">{threshold_s:g}s"


def _histogram(summaries: list[dict[str, Any]], key: str, *, threshold_s: float) -> dict[str, Any]:
    low_key = f"<={threshold_s:g}s"
    high_key = f">{threshold_s:g}s"
    counts = Counter(_age_bucket(_as_float(row.get(key)), threshold_s=threshold_s) for row in summaries)
    ages = sorted(_as_float(row.get(key)) for row in summaries if _as_float(row.get(key)) is not None)
    median_age = None
    if ages:
        mid = len(ages) // 2
        median_age = ages[mid] if len(ages) % 2 else (ages[mid - 1] + ages[mid]) / 2.0
    return {
        low_key: int(counts.get(low_key, 0)),
        high_key: int(counts.get(high_key, 0)),
        "missing": int(counts.get("missing", 0)),
        "median_s": None if median_age is None else round(median_age, 6),
    }


def _window_summary(
    row: dict[str, Any],
    *,
    order: dict[str, Any] | None = None,
    decision_age_threshold_s: float = DEFAULT_DECISION_AGE_THRESHOLD_S,
) -> dict[str, Any]:
    observed_age = _as_float(row.get("latest_observed_age_s"))
    source_age = _as_float(row.get("source_latest_observed_age_s"))
    decision_age = _decision_age(row, order=order)
    decision_time_age = _as_float(decision_age.get("decision_time_observed_age_s"))
    live_build_max_age_s = _live_build_max_age_s(row, order)
    submit_eligible = bool(order is not None or _submit_eligible(row))
    return {
        "window_start_s": _as_float(row.get("window_start_s")),
        "market_slug": row.get("market_slug"),
        "outcome": row.get("outcome"),
        "dominant_skip_reason": row.get("dominant_skip_reason") or "unknown",
        "participation_skip_category": row.get("participation_skip_category"),
        "latest_observed_age_s": observed_age,
        "source_latest_observed_age_s": source_age,
        "snapshot_observed_age_bucket": _age_bucket(observed_age, threshold_s=decision_age_threshold_s),
        "observed_age_bucket": _age_bucket(decision_time_age, threshold_s=decision_age_threshold_s),
        "decision_time_observed_age_s": decision_time_age,
        "decision_time_observed_age_source": decision_age["decision_time_observed_age_source"],
        "decision_time_ts": decision_age["decision_time_ts"],
        "decision_time_source": decision_age["decision_time_source"],
        "decision_observed_ts": decision_age["decision_observed_ts"],
        "decision_observed_ts_source": decision_age["decision_observed_ts_source"],
        "live_build_max_observed_age_s": live_build_max_age_s,
        "decision_time_observed_age_cap_violation": (
            bool(decision_time_age is not None and live_build_max_age_s is not None and decision_time_age > live_build_max_age_s)
        ),
        "wallet_eligible_orders": row.get("wallet_eligible_orders"),
        "our_attempts": row.get("our_attempts"),
        "our_submits": row.get("our_submits"),
        "our_fills": row.get("our_fills"),
        "matched_live_order_id": order.get("order_id") if isinstance(order, dict) else None,
        "submit_eligible": submit_eligible,
    }


def build_report(
    *,
    guard: dict[str, Any],
    rotation: dict[str, Any],
    live_execution: dict[str, Any] | None = None,
    target_wallet: str = DEFAULT_TARGET_WALLET,
    required_windows: int = 3,
    decision_age_threshold_s: float = DEFAULT_DECISION_AGE_THRESHOLD_S,
) -> dict[str, Any]:
    target_wallet = _wallet(target_wallet)
    rotation_window = _rotation_window_start(rotation)
    rotation_at_ts = _ts(_dict(rotation.get("selection_pin")).get("created_at") or rotation.get("generated_at"))
    orders_by_wallet_market = _orders_by_wallet_market(live_execution, rotation_at_ts=rotation_at_ts)
    rows: list[dict[str, Any]] = []
    participation = _dict(guard.get("window_participation"))
    for row in _list(participation.get("rows")):
        if not isinstance(row, dict) or _wallet(row.get("source_wallet")) != target_wallet:
            continue
        window_start = _as_float(row.get("window_start_s"))
        if rotation_window is not None and (window_start is None or window_start < rotation_window):
            continue
        if not _is_measured_window(row):
            continue
        rows.append(row)
    rows.sort(key=lambda row: (_as_float(row.get("window_start_s")) or 0.0, str(row.get("market_slug") or "")))
    grouped_rows: list[list[dict[str, Any]]] = []
    for row in rows:
        key = _window_group_key(row)
        if grouped_rows and _window_group_key(grouped_rows[-1][0]) == key:
            grouped_rows[-1].append(row)
        else:
            grouped_rows.append([row])
    first_rows = [_select_window_row(group) for group in grouped_rows[: max(0, int(required_windows))]]
    summaries = [
        _window_summary(
            row,
            order=_matching_order(row, orders_by_wallet_market),
            decision_age_threshold_s=float(decision_age_threshold_s),
        )
        for row in first_rows
    ]
    reason_counts = Counter(str(row.get("dominant_skip_reason") or "unknown") for row in summaries)
    submit_eligible_rows = sum(1 for row in summaries if row.get("submit_eligible") is True)
    age_histogram = _histogram(summaries, "decision_time_observed_age_s", threshold_s=float(decision_age_threshold_s))
    snapshot_age_histogram = _histogram(summaries, "latest_observed_age_s", threshold_s=float(decision_age_threshold_s))
    median_age = age_histogram["median_s"]
    cap_violation_rows = sum(1 for row in summaries if row.get("decision_time_observed_age_cap_violation") is True)
    submitted_cap_violation_rows = sum(
        1
        for row in summaries
        if row.get("submit_eligible") is True and row.get("decision_time_observed_age_cap_violation") is True
    )
    complete = len(summaries) >= int(required_windows)
    si1_reopens = bool(complete and median_age is not None and median_age <= 3.0 and submit_eligible_rows == 0)
    return {
        "kind": "active_set_post_rotation_windows",
        "schema_version": 2,
        "flow_stage": "LIVE/ROTATE/MEASURE",
        "generated_at": _iso_now(),
        "status": "FIRST3_COMPLETE" if complete else "COLLECTING_FIRST3",
        "target_wallet": target_wallet,
        "target_candidate_id": rotation.get("target_candidate_id"),
        "rotation_at": _dict(rotation.get("selection_pin")).get("created_at") or rotation.get("generated_at"),
        "rotation_window_start_s": rotation_window,
        "required_windows": int(required_windows),
        "measured_windows_found": len(summaries),
        "dominant_skip_reason_distribution": dict(reason_counts),
        "decision_time_observed_age_threshold_s": float(decision_age_threshold_s),
        "observed_age_histogram": age_histogram,
        "observed_age_histogram_metric": "decision_time_observed_age_s",
        "snapshot_observed_age_histogram": snapshot_age_histogram,
        "snapshot_observed_age_histogram_metric": "latest_observed_age_s",
        "decision_time_observed_age_cap_violation_rows": cap_violation_rows,
        "submitted_decision_time_observed_age_cap_violation_rows": submitted_cap_violation_rows,
        "submit_eligible_rows": submit_eligible_rows,
        "si1_reopens": si1_reopens,
        "edge_snapshot": rotation.get("edge_snapshot"),
        "windows": summaries,
        "next_action": (
            "evaluate SI-1 and rotate-back comparison"
            if complete
            else "continue guard measurement until f418 has 3 measured active windows"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD))
    parser.add_argument("--rotation-report", default=str(DEFAULT_ROTATION))
    parser.add_argument("--live-execution-state", default=str(DEFAULT_LIVE_EXECUTION))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--target-wallet", default=DEFAULT_TARGET_WALLET)
    parser.add_argument("--required-windows", type=int, default=3)
    parser.add_argument("--decision-age-threshold-s", type=float, default=DEFAULT_DECISION_AGE_THRESHOLD_S)
    args = parser.parse_args()

    report = build_report(
        guard=load_json(args.guard_state, default={}) or {},
        rotation=load_json(args.rotation_report, default={}) or {},
        live_execution=load_json(args.live_execution_state, default={}) or {},
        target_wallet=args.target_wallet,
        required_windows=args.required_windows,
        decision_age_threshold_s=args.decision_age_threshold_s,
    )
    atomic_write_json(args.output, report)
    print(
        json.dumps(
            {
                "status": report["status"],
                "target_wallet": report["target_wallet"],
                "measured_windows_found": report["measured_windows_found"],
                "si1_reopens": report["si1_reopens"],
                "output": str(Path(args.output)),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
