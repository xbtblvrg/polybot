#!/usr/bin/env python3
"""Attribute BTC-5m window-time rejects to source lateness vs detection lag.

Flow stage: LEARN/LIVE. This is a read-only report for Fable rotation
decisions; it must not submit orders or alter live guard state.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json


DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_LIVE_LEDGER_STATE = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_EVENT_LOG = "data/research/wallet_copy_live_execution_events.jsonl"
DEFAULT_OUTPUT = "data/research/wallet_copy_window_time_reject_attribution_latest.json"
DEFAULT_TRIPWIRE_START_ISO = "2026-07-08T03:33:00Z"
WINDOW_SLUG_RE = re.compile(r"btc-updown-5m-(\d+)")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _parse_iso_ts(value: Any) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def _num(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _median(values: list[float]) -> float | None:
    return round(float(statistics.median(values)), 6) if values else None


def _window_start_from_slug(slug: Any) -> float | None:
    match = WINDOW_SLUG_RE.search(str(slug or ""))
    return float(match.group(1)) if match else None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        handle = path.open()
    except FileNotFoundError:
        return rows
    with handle:
        for raw in handle:
            if not raw.strip():
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _latest_order_ts(live: dict[str, Any]) -> float | None:
    summary = live.get("summary") if isinstance(live.get("summary"), dict) else {}
    candidates = [_parse_iso_ts(summary.get("latest_order_ts"))]
    orders = live.get("orders") if isinstance(live.get("orders"), list) else []
    for order in orders:
        if not isinstance(order, dict):
            continue
        for key in ("updated_at", "filled_at", "submitted_at", "created_at"):
            candidates.append(_parse_iso_ts(order.get(key)))
    parsed = [item for item in candidates if item is not None]
    return max(parsed) if parsed else None


def _event_lookup(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in events:
        if row.get("event") != "wallet_copy_live_profit_latency_suppression_reject":
            continue
        intent_id = str(row.get("intent_id") or "")
        if intent_id:
            lookup[intent_id] = row
    return lookup


def _argv_float(argv: list[Any], flag: str) -> float | None:
    for idx, value in enumerate(argv):
        if str(value) != flag:
            continue
        if idx + 1 >= len(argv):
            return None
        return _num(argv[idx + 1])
    return None


def _guard_profit_latency_thresholds(guard: dict[str, Any]) -> dict[str, float | None]:
    runtime = guard.get("live_execution_runtime") if isinstance(guard.get("live_execution_runtime"), dict) else {}
    argv = runtime.get("argv") if isinstance(runtime.get("argv"), list) else []
    live_execution = guard.get("live_execution") if isinstance(guard.get("live_execution"), dict) else {}
    gate = live_execution.get("profit_latency_suppression") if isinstance(live_execution.get("profit_latency_suppression"), dict) else {}
    return {
        "window_time_suppress_gte_s": _num(gate.get("window_time_suppress_gte_s"))
        or _argv_float(argv, "--profit-latency-window-time-suppress-gte-s"),
        "signal_age_suppress_gte_s": _num(gate.get("signal_age_suppress_gte_s"))
        or _argv_float(argv, "--profit-latency-signal-age-suppress-gte-s"),
    }


def _row_split(row: dict[str, Any], event: dict[str, Any] | None) -> dict[str, Any]:
    window_start = _num(row.get("window_start_s")) or _window_start_from_slug(row.get("market_slug"))
    detect_ts = _num(row.get("latest_observed_ts") or row.get("effective_latest_observed_ts"))
    source_trade_ts = None
    event_ts = _num((event or {}).get("event_ts"))
    event_detect_ts = _num(
        (event or {}).get("detection_observed_ts")
        or (event or {}).get("observed_ts")
        or (event or {}).get("dataapi_first_seen_ts")
    )
    if event_ts is not None:
        source_trade_ts = event_ts
    if event_detect_ts is not None:
        detect_ts = event_detect_ts
    if source_trade_ts is None:
        first_seen_ts = _parse_iso_ts(row.get("first_seen_at") or row.get("last_seen_at"))
        source_age = _num(row.get("source_latest_observed_age_s"))
        if first_seen_ts is not None and source_age is not None:
            source_trade_ts = first_seen_ts - source_age
    if window_start is None or detect_ts is None or source_trade_ts is None:
        source_lateness = None
        detection_latency = None
    else:
        source_lateness = source_trade_ts - window_start
        detection_latency = detect_ts - source_trade_ts
    return {
        "intent_id": row.get("intent_id"),
        "source_wallet": row.get("source_wallet"),
        "market_slug": row.get("market_slug"),
        "outcome": row.get("outcome"),
        "limit_price": (event or {}).get("limit_price"),
        "source_inventory_vwap": row.get("source_inventory_vwap"),
        "window_start_s": window_start,
        "source_trade_ts": source_trade_ts,
        "detect_ts": detect_ts,
        "source_lateness_s": round(source_lateness, 6) if source_lateness is not None else None,
        "detection_latency_s": round(detection_latency, 6) if detection_latency is not None else None,
        "source_traded_inside_first_120s": source_lateness is not None and source_lateness < 120.0,
        "source_traded_after_180s": source_lateness is not None and source_lateness >= 180.0,
        "detection_latency_gt_60s": detection_latency is not None and detection_latency > 60.0,
        "detection_source": (event or {}).get("detection_source") or row.get("detection_source"),
        "observation_sources": (event or {}).get("observation_sources") or row.get("observation_sources") or [],
        "detection_observed_ts": event_detect_ts,
        "alternate_observed_ts": (event or {}).get("alternate_observed_ts") or row.get("alternate_observed_ts"),
        "alternate_detection_source": (event or {}).get("alternate_detection_source")
        or row.get("alternate_detection_source"),
        "dominant_skip_reason": row.get("dominant_skip_reason"),
        "event_log_matched": bool(event),
        "reject_reason": (event or {}).get("reject_reason"),
        "taxonomy_tags": (event or {}).get("taxonomy_tags") or [],
        "window_time_suppress_gte_s": (event or {}).get("window_time_suppress_gte_s"),
        "signal_age_suppress_gte_s": (event or {}).get("signal_age_suppress_gte_s"),
    }


def _attach_threshold_fallback(row: dict[str, Any], thresholds: dict[str, float | None]) -> dict[str, Any]:
    out = dict(row)
    threshold_source = "event_log"
    if out.get("window_time_suppress_gte_s") is None and thresholds.get("window_time_suppress_gte_s") is not None:
        out["window_time_suppress_gte_s"] = thresholds.get("window_time_suppress_gte_s")
        threshold_source = "guard_runtime"
    if out.get("signal_age_suppress_gte_s") is None and thresholds.get("signal_age_suppress_gte_s") is not None:
        out["signal_age_suppress_gte_s"] = thresholds.get("signal_age_suppress_gte_s")
        threshold_source = "guard_runtime"
    out["threshold_source"] = threshold_source
    return out


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    source_lateness = [float(row["source_lateness_s"]) for row in rows if row.get("source_lateness_s") is not None]
    detection_latency = [
        float(row["detection_latency_s"]) for row in rows if row.get("detection_latency_s") is not None
    ]
    first_120 = [row for row in rows if row.get("source_traded_inside_first_120s")]
    first_120_detection = [
        float(row["detection_latency_s"]) for row in first_120 if row.get("detection_latency_s") is not None
    ]
    late_source = [row for row in rows if row.get("source_traded_after_180s")]
    fixable_pipeline = bool(first_120_detection and statistics.median(first_120_detection) > 60.0)
    source_lateness_dominates = len(late_source) > len(first_120)
    if fixable_pipeline:
        ruling_input = "FIXABLE_PIPELINE_LAG"
    elif source_lateness_dominates:
        ruling_input = "SOURCE_LATENESS_DOMINATES"
    else:
        ruling_input = "MIXED_OR_INSUFFICIENT"
    rtds_window_time_tripwire_rows = [
        row
        for row in rows
        if row.get("source_traded_after_180s") is False
        and (
            row.get("detection_source") == "rtds_activity"
            or "rtds_activity" in {str(source) for source in row.get("observation_sources") or []}
        )
    ]
    return {
        "rows": len(rows),
        "rows_with_split": min(len(source_lateness), len(detection_latency)),
        "median_source_lateness_s": _median(source_lateness),
        "median_detection_latency_s": _median(detection_latency),
        "source_first_120_rows": len(first_120),
        "source_after_180_rows": len(late_source),
        "detection_gt_60_source_first_120_rows": sum(
            1 for row in first_120 if row.get("detection_latency_gt_60s")
        ),
        "median_detection_latency_source_first_120_s": _median(first_120_detection),
        "fixable_pipeline_lag_threshold_met": fixable_pipeline,
        "source_lateness_dominates": source_lateness_dominates,
        "rtds_window_time_tripwire_rows": len(rtds_window_time_tripwire_rows),
        "rtds_window_time_tripwire_status": "TRIPWIRE_OPEN" if rtds_window_time_tripwire_rows else "CLEAR",
        "ruling_input": ruling_input,
    }


def _flow_money_reconciliation(live: dict[str, Any]) -> dict[str, Any]:
    today = datetime.now(timezone.utc).date().isoformat()
    orders = live.get("orders") if isinstance(live.get("orders"), list) else []
    filled: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict) or str(order.get("status") or "").upper() != "FILLED":
            continue
        ts = _parse_iso_ts(order.get("updated_at") or order.get("filled_at") or order.get("submitted_at"))
        if ts is None:
            continue
        day = datetime.fromtimestamp(ts, timezone.utc).date().isoformat()
        if day == today:
            filled.append(order)
    distinct_windows = sorted({str(order.get("market_slug") or "") for order in filled if order.get("market_slug")})
    return {
        "day_utc": today,
        "money_fill_count": len(filled),
        "distinct_filled_windows": len(distinct_windows),
        "sample_filled_windows": distinct_windows[:20],
        "classification": (
            "BENIGN_MULTIPLE_FILLS_PER_WINDOW"
            if len(filled) > len(distinct_windows) and len(distinct_windows) > 0
            else "CHECK_FLOW_WINDOW_COUNT"
        ),
    }


def build_report(
    *,
    guard_state_path: Path,
    live_ledger_state_path: Path,
    event_log_path: Path,
    tripwire_start_iso: str | None = DEFAULT_TRIPWIRE_START_ISO,
) -> dict[str, Any]:
    guard = _load_json(guard_state_path, {})
    live = _load_json(live_ledger_state_path, {})
    events = _read_jsonl(event_log_path)
    lookup = _event_lookup(events)
    runtime_thresholds = _guard_profit_latency_thresholds(guard)
    participation = guard.get("window_participation") if isinstance(guard.get("window_participation"), dict) else {}
    retained_rows = participation.get("rows") if isinstance(participation.get("rows"), list) else []
    current_rows = [
        _attach_threshold_fallback(_row_split(row, lookup.get(str(row.get("intent_id") or ""))), runtime_thresholds)
        for row in retained_rows
        if isinstance(row, dict) and row.get("dominant_skip_reason") == "window_time_gte_180s"
    ]
    latest_order_ts = _latest_order_ts(live)
    tripwire_start_ts = _parse_iso_ts(tripwire_start_iso) if tripwire_start_iso else None
    event_rows: list[dict[str, Any]] = []
    for event in events:
        tags = event.get("taxonomy_tags") if isinstance(event.get("taxonomy_tags"), list) else []
        taxonomy = str(event.get("taxonomy") or "")
        if "window_time_gte_180s" not in set(str(tag) for tag in tags + [taxonomy]):
            continue
        window_start = _window_start_from_slug(event.get("market_slug"))
        source_ts = _num(event.get("event_ts"))
        detect_ts = _num(
            event.get("detection_observed_ts") or event.get("observed_ts") or event.get("dataapi_first_seen_ts")
        )
        if window_start is None or source_ts is None or detect_ts is None:
            continue
        gate_eval_ts = _parse_iso_ts(event.get("ts"))
        event_reference_ts = gate_eval_ts if gate_eval_ts is not None else detect_ts
        event_rows.append(
            _attach_threshold_fallback(
                {
                    "intent_id": event.get("intent_id"),
                    "source_wallet": event.get("source_wallet"),
                    "market_slug": event.get("market_slug"),
                    "outcome": event.get("outcome"),
                    "limit_price": event.get("limit_price"),
                    "window_start_s": window_start,
                    "source_trade_ts": source_ts,
                    "detect_ts": detect_ts,
                    "source_lateness_s": round(source_ts - window_start, 6),
                    "detection_latency_s": round(detect_ts - source_ts, 6),
                    "source_traded_inside_first_120s": (source_ts - window_start) < 120.0,
                    "source_traded_after_180s": (source_ts - window_start) >= 180.0,
                    "detection_latency_gt_60s": (detect_ts - source_ts) > 60.0,
                    "detection_source": event.get("detection_source"),
                    "observation_sources": event.get("observation_sources") or [],
                    "detection_observed_ts": detect_ts,
                    "gate_eval_ts": gate_eval_ts,
                    "window_time_s": _num(event.get("window_time_s")),
                    "gate_eval_delay_after_detection_s": (
                        round(gate_eval_ts - detect_ts, 6)
                        if gate_eval_ts is not None
                        else None
                    ),
                    "alternate_observed_ts": event.get("alternate_observed_ts"),
                    "alternate_detection_source": event.get("alternate_detection_source"),
                    "reject_reason": event.get("reject_reason"),
                    "taxonomy_tags": tags,
                    "window_time_suppress_gte_s": event.get("window_time_suppress_gte_s"),
                    "signal_age_suppress_gte_s": event.get("signal_age_suppress_gte_s"),
                    "ts": event.get("ts"),
                    "post_latest_order": latest_order_ts is not None and source_ts > latest_order_ts,
                    "post_tripwire_start": (
                        tripwire_start_ts is not None and event_reference_ts >= tripwire_start_ts
                    ),
                },
                runtime_thresholds,
            )
        )
    post_latest = [row for row in event_rows if row.get("post_latest_order")]
    post_tripwire_start = [row for row in event_rows if row.get("post_tripwire_start")]
    return {
        "kind": "wallet_copy_window_time_reject_attribution",
        "flow_stage": "LEARN/LIVE",
        "generated_at": _utc_now_iso(),
        "direction_id": "2026-07-08T03:09Z-fable-window-time-reject-attribution",
        "tripwire_start_iso": tripwire_start_iso,
        "tripwire_start_ts": tripwire_start_ts,
        "inputs": {
            "guard_state": str(guard_state_path),
            "live_ledger_state": str(live_ledger_state_path),
            "event_log": str(event_log_path),
        },
        "runtime_profit_latency_thresholds": runtime_thresholds,
        "definitions": {
            "source_lateness_s": "source_trade_ts - BTC 5m window_open",
            "detection_latency_s": "our_detect_ts/dataapi_first_seen_ts - source_trade_ts",
            "threshold_fixable_pipeline": "median detection_latency > 60s among rows where source_lateness < 120s",
        },
        "latest_order_ts": latest_order_ts,
        "current_guard_rows": current_rows,
        "current_guard_summary": _summarize(current_rows),
        "post_latest_order_event_summary": _summarize(post_latest),
        "post_tripwire_start_event_summary": _summarize(post_tripwire_start),
        "all_event_summary": _summarize(event_rows),
        "post_latest_order_event_rows": post_latest,
        "post_tripwire_start_event_rows": post_tripwire_start,
        "flow_money_reconciliation": _flow_money_reconciliation(live),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--live-ledger-state", default=DEFAULT_LIVE_LEDGER_STATE)
    parser.add_argument("--event-log", default=DEFAULT_EVENT_LOG)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--tripwire-start",
        default=DEFAULT_TRIPWIRE_START_ISO,
        help="UTC ISO timestamp anchoring the RTDS routing tripwire window.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(
        guard_state_path=Path(args.guard_state),
        live_ledger_state_path=Path(args.live_ledger_state),
        event_log_path=Path(args.event_log),
        tripwire_start_iso=args.tripwire_start,
    )
    atomic_write_json(Path(args.output), report)
    print(json.dumps(report["current_guard_summary"], sort_keys=True))
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
