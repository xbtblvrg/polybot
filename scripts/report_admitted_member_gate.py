#!/usr/bin/env python3
"""Report the admitted-member submit gate evidence.

This is read-only instrumentation for the 2026-07-07 Fable gate: admitted
members must either submit live orders by the deadline or be classified as
source-inactive versus a pipeline defect.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_source_active_windows import DEFAULT_RTDS_JSONL, source_active_report  # noqa: E402
from src.wallet_copy.models import parse_ts, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_GUARD_EVENTS = "data/research/wallet_copy_live_guard_events.jsonl"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_LIVE_LEDGER_STATE = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = "data/research/admitted_member_gate_report.json"
DEFAULT_ADMITTED_STATUS = "FABLE_0608_ADMITTED_STANDARD_POLICY"
DEFAULT_SINCE = "2026-07-07T06:23:00Z"
DEFAULT_GATE_DEADLINE = "2026-07-07T08:30:00Z"
DEFAULT_GUARD_TAIL_BYTES = 8_589_934_592
DEFAULT_SOURCE_TAIL_BYTES = 1_610_612_736


def _load_json(path: str | Path, default: Any) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    try:
        with target.open() as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default


def _parse_ts_required(value: str, *, label: str) -> float:
    parsed = parse_ts(value)
    if parsed is None:
        raise SystemExit(f"invalid {label}: {value!r}")
    return float(parsed)


def _iso_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _event_ts(row: dict[str, Any]) -> float:
    for key in ("generated_at", "ts", "submitted_at", "updated_at"):
        raw = row.get(key)
        if raw:
            parsed = parse_ts(str(raw))
            if parsed is not None:
                return float(parsed)
    return 0.0


def _jsonl_rows(path: str | Path, *, tail_bytes: int | None) -> Iterable[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return
    with target.open("rb") as handle:
        if tail_bytes and tail_bytes > 0:
            handle.seek(max(0, os.fstat(handle.fileno()).st_size - int(tail_bytes)))
            if handle.tell() > 0:
                handle.readline()
        for raw in handle:
            if b"wallet_copy_live_guard_cycle" not in raw:
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _member_id(member: dict[str, Any]) -> str:
    return str(member.get("candidate_id") or member.get("source_wallet") or "").lower()


def admitted_members_from_guard_state(
    guard_state: dict[str, Any],
    *,
    admitted_status: str = DEFAULT_ADMITTED_STATUS,
) -> list[dict[str, Any]]:
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    raw_members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    members: list[dict[str, Any]] = []
    for raw in raw_members:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("status") or "") != admitted_status:
            continue
        wallet = str(raw.get("source_wallet") or "").lower()
        candidate_id = str(raw.get("candidate_id") or "").lower()
        if not wallet and not candidate_id:
            continue
        members.append(
            {
                "candidate_id": candidate_id,
                "source_wallet": wallet,
                "policy_id": str(raw.get("policy_id") or ""),
                "status": str(raw.get("status") or ""),
                "max_price": raw.get("max_price"),
                "wallet_fraction": raw.get("wallet_fraction"),
                "max_order_usd": raw.get("max_order_usd"),
            }
        )
    return members


def _selected_member_key(row: dict[str, Any], members: list[dict[str, Any]]) -> str:
    candidate = row.get("candidate") if isinstance(row.get("candidate"), dict) else {}
    candidate_id = str(row.get("candidate_id") or candidate.get("candidate_id") or "").lower()
    wallet = str(row.get("source_wallet") or candidate.get("source_wallet") or "").lower()
    for member in members:
        if candidate_id and candidate_id == str(member.get("candidate_id") or "").lower():
            return _member_id(member)
        if wallet and wallet == str(member.get("source_wallet") or "").lower():
            return _member_id(member)
    return ""


def _empty_member_summary(member: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": str(member.get("candidate_id") or ""),
        "source_wallet": str(member.get("source_wallet") or "").lower(),
        "policy_id": str(member.get("policy_id") or ""),
        "status": str(member.get("status") or ""),
        "max_price": member.get("max_price"),
        "cycles_landed": 0,
        "fresh_intents": 0,
        "new_live_candidate_intents": 0,
        "orders_submitted_in_cycles": 0,
        "cycle_outcome_counts": {},
        "live_execution_status_counts": {},
        "participation_alert_counts": {},
        "latest_cycle_at": "",
        "latest_cycle_status": "",
        "latest_cycle_alerts": [],
        "_cycle_ts": [],
        "ledger": {
            "orders": 0,
            "fills": 0,
            "rejects": 0,
            "submitted_open": 0,
            "latest_order_ts": "",
            "status_counts": {},
        },
        "source_activity": {
            "btc5m_buy_rows_since": 0,
            "btc5m_buy_windows_since": 0,
            "source_active_rows": 0,
            "source_active_windows": 0,
            "policy_eligible_rows": 0,
            "policy_eligible_windows": 0,
            "outside_band_rows": 0,
            "outside_policy_rows": 0,
            "policy_windows_with_event_fresh_overlap": 0,
            "policy_windows_without_event_fresh_overlap": 0,
            "policy_windows_with_received_fresh_overlap": 0,
            "policy_windows_without_received_fresh_overlap": 0,
            "policy_windows_with_cycle_fresh_overlap": 0,
            "policy_windows_without_cycle_fresh_overlap": 0,
            "policy_window_details": [],
        },
        "classification": "NO_EVIDENCE",
    }


def _summarize_guard_cycles(
    rows: Iterable[dict[str, Any]],
    *,
    members: list[dict[str, Any]],
    since_ts: float,
) -> dict[str, dict[str, Any]]:
    summaries = {_member_id(member): _empty_member_summary(member) for member in members}
    for row in rows:
        if str(row.get("event") or "") != "wallet_copy_live_guard_cycle":
            continue
        ts = _event_ts(row)
        if ts and ts < since_ts:
            continue
        key = _selected_member_key(row, members)
        if not key or key not in summaries:
            continue
        summary = summaries[key]
        live_execution = row.get("live_execution") if isinstance(row.get("live_execution"), dict) else {}
        alerts = row.get("participation_alerts") if isinstance(row.get("participation_alerts"), list) else []
        summary["cycles_landed"] += 1
        if ts:
            summary["_cycle_ts"].append(float(ts))
        summary["fresh_intents"] += _int(live_execution.get("fresh_candidate_intents"))
        summary["new_live_candidate_intents"] += _int(live_execution.get("new_live_candidate_intents"))
        summary["orders_submitted_in_cycles"] += _int(live_execution.get("orders_submitted"))
        summary["latest_cycle_at"] = row.get("generated_at") or ""
        summary["latest_cycle_status"] = str(live_execution.get("status") or row.get("cycle_outcome") or "")
        summary["latest_cycle_alerts"] = sorted(str(item) for item in alerts)

        outcome_counts = Counter(summary["cycle_outcome_counts"])
        outcome_counts[str(row.get("cycle_outcome") or "unknown")] += 1
        summary["cycle_outcome_counts"] = dict(sorted(outcome_counts.items()))

        live_status_counts = Counter(summary["live_execution_status_counts"])
        live_status_counts[str(live_execution.get("status") or "unknown")] += 1
        summary["live_execution_status_counts"] = dict(sorted(live_status_counts.items()))

        alert_counts = Counter(summary["participation_alert_counts"])
        for alert in alerts or ["none"]:
            alert_counts[str(alert)] += 1
        summary["participation_alert_counts"] = dict(sorted(alert_counts.items()))
    return summaries


def _order_ts(order: dict[str, Any]) -> float:
    for key in ("submitted_at", "updated_at", "created_at", "ts"):
        raw = order.get(key)
        if raw:
            parsed = parse_ts(str(raw))
            if parsed is not None:
                return float(parsed)
    lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
    for item in reversed(lifecycle):
        if isinstance(item, dict) and item.get("ts"):
            parsed = parse_ts(str(item.get("ts")))
            if parsed is not None:
                return float(parsed)
    return 0.0


def _merge_ledger_counts(
    summaries: dict[str, dict[str, Any]],
    *,
    ledger_state: dict[str, Any],
    members: list[dict[str, Any]],
    since_ts: float,
) -> None:
    wallet_to_key = {str(member.get("source_wallet") or "").lower(): _member_id(member) for member in members}
    orders = ledger_state.get("orders") if isinstance(ledger_state.get("orders"), list) else []
    for order in orders:
        if not isinstance(order, dict):
            continue
        wallet = str(order.get("source_wallet") or "").lower()
        key = wallet_to_key.get(wallet)
        if not key or key not in summaries:
            continue
        ts = _order_ts(order)
        if ts and ts < since_ts:
            continue
        status = str(order.get("final_status") or order.get("status") or "UNKNOWN").upper()
        ledger = summaries[key]["ledger"]
        ledger["orders"] += 1
        if "FILL" in status or status in {"MATCHED", "FILLED"}:
            ledger["fills"] += 1
        elif "REJECT" in status:
            ledger["rejects"] += 1
        elif "SUBMITTED" in status or "LIVE_SUBMITTED" in status:
            ledger["submitted_open"] += 1
        ledger["latest_order_ts"] = _iso_from_ts(ts) if ts else ledger.get("latest_order_ts", "")
        counts = Counter(ledger["status_counts"])
        counts[status] += 1
        ledger["status_counts"] = dict(sorted(counts.items()))


def _merge_source_activity(
    summaries: dict[str, dict[str, Any]],
    *,
    source_reports: dict[str, dict[str, Any]],
    fresh_horizon_s: float,
) -> None:
    for key, report in source_reports.items():
        if key not in summaries:
            continue
        summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
        source_active_rows = _int(summary.get("source_active_rows"))
        policy_eligible_rows = _int(summary.get("policy_eligible_rows"))
        btc5m_buy_rows = _int(summary.get("btc5m_buy_rows_since"))
        cycle_times = [float(item) for item in summaries[key].get("_cycle_ts", [])]
        policy_details: list[dict[str, Any]] = []
        windows = report.get("windows") if isinstance(report.get("windows"), list) else []
        for window in windows:
            if not isinstance(window, dict) or _int(window.get("le_max_price_rows")) <= 0:
                continue
            first_event_ts = (
                parse_ts(str(window.get("first_policy_event_iso") or ""))
                or parse_ts(str(window.get("first_event_iso") or ""))
                or 0.0
            )
            last_event_ts = (
                parse_ts(str(window.get("last_policy_event_iso") or ""))
                or parse_ts(str(window.get("last_event_iso") or ""))
                or first_event_ts
            )
            first_received_ts = (
                parse_ts(str(window.get("first_policy_received_iso") or ""))
                or parse_ts(str(window.get("first_received_iso") or ""))
                or first_event_ts
            )
            last_received_ts = (
                parse_ts(str(window.get("last_policy_received_iso") or ""))
                or parse_ts(str(window.get("last_received_iso") or ""))
                or last_event_ts
            )
            event_overlap = any(
                first_event_ts <= cycle_ts <= float(last_event_ts) + float(fresh_horizon_s)
                for cycle_ts in cycle_times
            )
            received_overlap = any(
                first_received_ts <= cycle_ts <= float(last_received_ts) + float(fresh_horizon_s)
                for cycle_ts in cycle_times
            )
            policy_details.append(
                {
                    "cycle_fresh_overlap": received_overlap,
                    "event_cycle_fresh_overlap": event_overlap,
                    "market_slug": str(window.get("market_slug") or ""),
                    "first_event_iso": str(window.get("first_event_iso") or ""),
                    "first_policy_event_iso": str(window.get("first_policy_event_iso") or ""),
                    "first_policy_received_iso": str(window.get("first_policy_received_iso") or ""),
                    "first_received_iso": str(window.get("first_received_iso") or ""),
                    "last_event_iso": str(window.get("last_event_iso") or ""),
                    "last_policy_event_iso": str(window.get("last_policy_event_iso") or ""),
                    "last_policy_received_iso": str(window.get("last_policy_received_iso") or ""),
                    "last_received_iso": str(window.get("last_received_iso") or ""),
                    "rows": _int(window.get("rows")),
                    "le_max_price_rows": _int(window.get("le_max_price_rows")),
                }
            )
        event_overlap_windows = sum(1 for item in policy_details if item.get("event_cycle_fresh_overlap"))
        received_overlap_windows = sum(1 for item in policy_details if item.get("cycle_fresh_overlap"))
        summaries[key]["source_activity"] = {
            "btc5m_buy_rows_since": btc5m_buy_rows,
            "btc5m_buy_windows_since": _int(summary.get("btc5m_buy_windows_since")),
            "source_active_rows": source_active_rows,
            "source_active_windows": _int(summary.get("source_active_windows")),
            "policy_eligible_rows": policy_eligible_rows,
            "policy_eligible_windows": _int(summary.get("policy_eligible_windows")),
            "outside_band_rows": max(0, btc5m_buy_rows - source_active_rows),
            "outside_policy_rows": max(0, source_active_rows - policy_eligible_rows),
            "policy_windows_with_event_fresh_overlap": event_overlap_windows,
            "policy_windows_without_event_fresh_overlap": max(0, len(policy_details) - event_overlap_windows),
            "policy_windows_with_received_fresh_overlap": received_overlap_windows,
            "policy_windows_without_received_fresh_overlap": max(0, len(policy_details) - received_overlap_windows),
            "policy_windows_with_cycle_fresh_overlap": received_overlap_windows,
            "policy_windows_without_cycle_fresh_overlap": max(0, len(policy_details) - received_overlap_windows),
            "policy_window_details": policy_details,
        }


def _classify_member(summary: dict[str, Any]) -> str:
    ledger = summary.get("ledger") if isinstance(summary.get("ledger"), dict) else {}
    source = summary.get("source_activity") if isinstance(summary.get("source_activity"), dict) else {}
    if _int(ledger.get("orders")) > 0 or _int(summary.get("orders_submitted_in_cycles")) > 0:
        return "SUBMITTED"
    if _int(summary.get("cycles_landed")) <= 0:
        return "NO_CYCLES_LANDED"
    if (
        _int(source.get("policy_eligible_windows")) > 0
        and _int(source.get("policy_windows_with_cycle_fresh_overlap")) <= 0
        and _int(summary.get("fresh_intents")) <= 0
    ):
        return "SOURCE_ACTIVE_BUT_NO_FRESH_CYCLE_OVERLAP"
    if _int(source.get("policy_eligible_windows")) > 0 and _int(summary.get("fresh_intents")) <= 0:
        return "PIPELINE_NO_INTENTS_FROM_SOURCE_ACTIVITY"
    if _int(summary.get("fresh_intents")) > 0:
        return "PIPELINE_FRESH_INTENTS_NO_SUBMIT"
    if _int(source.get("source_active_windows")) > 0 and _int(source.get("policy_eligible_windows")) <= 0:
        return "SOURCE_ACTIVE_OUTSIDE_POLICY"
    if _int(source.get("policy_eligible_windows")) <= 0:
        return "ADMITTED_SOURCE_INACTIVE"
    return "PENDING_NO_SUBMIT"


def build_report(
    *,
    guard_events: Iterable[dict[str, Any]],
    guard_state: dict[str, Any],
    live_ledger_state: dict[str, Any],
    source_reports: dict[str, dict[str, Any]],
    members: list[dict[str, Any]] | None,
    since_ts: float,
    gate_deadline_ts: float,
    fresh_horizon_s: float = 30.0,
    generated_at: str | None = None,
) -> dict[str, Any]:
    admitted_members = members or admitted_members_from_guard_state(guard_state)
    summaries = _summarize_guard_cycles(guard_events, members=admitted_members, since_ts=since_ts)
    _merge_ledger_counts(summaries, ledger_state=live_ledger_state, members=admitted_members, since_ts=since_ts)
    _merge_source_activity(summaries, source_reports=source_reports, fresh_horizon_s=float(fresh_horizon_s))
    for summary in summaries.values():
        summary["classification"] = _classify_member(summary)
        summary["cycle_times_retained"] = len(summary.get("_cycle_ts", []))
        summary.pop("_cycle_ts", None)

    now_iso = generated_at or utc_now_iso()
    now_ts = _parse_ts_required(now_iso, label="generated_at")
    member_rows = [summaries[_member_id(member)] for member in admitted_members if _member_id(member) in summaries]
    any_submitted = any(row["classification"] == "SUBMITTED" for row in member_rows)
    classifications = Counter(str(row.get("classification") or "") for row in member_rows)
    if any_submitted:
        gate_status = "PASS_SUBMITTED"
    elif now_ts < gate_deadline_ts:
        gate_status = "PENDING_BEFORE_DEADLINE"
    elif all(row.get("classification") == "ADMITTED_SOURCE_INACTIVE" for row in member_rows):
        gate_status = "FAIL_ADMITTED_SOURCE_INACTIVE"
    else:
        gate_status = "FAIL_PIPELINE_EVIDENCE"

    return {
        "schema_version": 1,
        "kind": "admitted_member_gate_report",
        "flow_stage": "LIVE/PROMOTE/ROTATE/SELF-DEV",
        "generated_at": now_iso,
        "since_ts": float(since_ts),
        "since_iso": _iso_from_ts(since_ts),
        "gate_deadline_ts": float(gate_deadline_ts),
        "gate_deadline_iso": _iso_from_ts(gate_deadline_ts),
        "fresh_horizon_s": float(fresh_horizon_s),
        "summary": {
            "gate_status": gate_status,
            "members": len(member_rows),
            "members_with_submit": sum(1 for row in member_rows if row["classification"] == "SUBMITTED"),
            "members_with_cycles": sum(1 for row in member_rows if _int(row.get("cycles_landed")) > 0),
            "classification_counts": dict(sorted(classifications.items())),
            "total_cycles_landed": sum(_int(row.get("cycles_landed")) for row in member_rows),
            "total_fresh_intents": sum(_int(row.get("fresh_intents")) for row in member_rows),
            "total_orders_submitted_in_cycles": sum(_int(row.get("orders_submitted_in_cycles")) for row in member_rows),
            "total_ledger_orders": sum(_int((row.get("ledger") or {}).get("orders")) for row in member_rows),
        },
        "members": member_rows,
    }


def _member_arg(value: str) -> dict[str, Any]:
    parts = value.split(",")
    data: dict[str, Any] = {}
    for part in parts:
        if "=" not in part:
            raise argparse.ArgumentTypeError("member must use key=value pairs")
        key, raw = part.split("=", 1)
        data[key.strip()] = raw.strip()
    data["candidate_id"] = str(data.get("candidate_id") or "").lower()
    data["source_wallet"] = str(data.get("source_wallet") or "").lower()
    if not data["candidate_id"] and not data["source_wallet"]:
        raise argparse.ArgumentTypeError("member requires candidate_id or source_wallet")
    return data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-events", default=DEFAULT_GUARD_EVENTS)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--live-ledger-state", default=DEFAULT_LIVE_LEDGER_STATE)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS_JSONL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--since", default=DEFAULT_SINCE)
    parser.add_argument("--gate-deadline", default=DEFAULT_GATE_DEADLINE)
    parser.add_argument("--member", action="append", type=_member_arg, default=[])
    parser.add_argument("--guard-tail-bytes", type=int, default=DEFAULT_GUARD_TAIL_BYTES)
    parser.add_argument("--guard-full-scan", action="store_true")
    parser.add_argument("--source-tail-bytes", type=int, default=DEFAULT_SOURCE_TAIL_BYTES)
    parser.add_argument("--source-full-scan", action="store_true")
    parser.add_argument("--band-start-s", type=float, default=0.0)
    parser.add_argument("--band-end-s", type=float, default=60.0)
    parser.add_argument("--fresh-horizon-s", type=float, default=30.0)
    parser.add_argument("--required-windows", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    since_ts = _parse_ts_required(args.since, label="--since")
    gate_deadline_ts = _parse_ts_required(args.gate_deadline, label="--gate-deadline")
    guard_state = _load_json(args.guard_state, {})
    live_ledger_state = _load_json(args.live_ledger_state, {})
    members = args.member or admitted_members_from_guard_state(guard_state)
    if not members:
        raise SystemExit("no admitted members found; pass --member or check guard state")

    source_reports: dict[str, dict[str, Any]] = {}
    source_tail_bytes = None if args.source_full_scan else int(args.source_tail_bytes)
    for member in members:
        key = _member_id(member)
        max_price = member.get("max_price")
        source_reports[key] = source_active_report(
            rtds_jsonl=args.rtds_jsonl,
            source_wallet=str(member.get("source_wallet") or ""),
            since_ts=since_ts,
            min_offset_s=float(args.band_start_s),
            max_offset_s=float(args.band_end_s),
            max_price=float(max_price) if max_price not in (None, "") else 0.5,
            required_windows=int(args.required_windows),
            tail_bytes=source_tail_bytes,
        )

    guard_tail_bytes = None if args.guard_full_scan else int(args.guard_tail_bytes)
    report = build_report(
        guard_events=_jsonl_rows(args.guard_events, tail_bytes=guard_tail_bytes),
        guard_state=guard_state,
        live_ledger_state=live_ledger_state,
        source_reports=source_reports,
        members=members,
        since_ts=since_ts,
        gate_deadline_ts=gate_deadline_ts,
        fresh_horizon_s=float(args.fresh_horizon_s),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
