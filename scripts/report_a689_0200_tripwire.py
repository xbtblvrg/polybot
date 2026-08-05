#!/usr/bin/env python3
"""Classify the 02:00Z a689 canary no-accepted-order tripwire."""

from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"
DEFAULT_POLICY_ID = "a6896d11_price_reject_canary_0.10_cap_2_le_70"
DEFAULT_POSTFIX_START = "2026-07-15T22:23:00Z"
DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_LIVE_STATE = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/a689_0200_tripwire_latest.json"
SOURCE_LATE_THRESHOLD_S = 180.0
POLICY_LEAK_MAX_USD = 1.0
DEFAULT_BASIS = (
    "Fable 2026-07-16T01:10Z 02:00Z live-row-only a689 tripwire; "
    "policy_max=1.0 leak check precedes any drip_min change"
)
DEFAULT_RULE = (
    "If policy_max_1_rows>0, do not tune around the leak; otherwise if "
    "FLOOR_BUDGET_BIND is dominant with >=3 budget-bind rows, lower a689 drip_min "
    "to 1 while cap/max_order/drip_max stay 2."
)
DEFAULT_BUDGET_BIND_ACTION = "LOWER_A689_DRIP_MIN_TO_1_CAP_STAYS_2"

REJECTED_ORDER_STATUSES = {"", "REJECTED", "UNFILLED", "LIVE_REJECTED"}
LATE_REASONS = {"inventory_late_window_guard", "window_time_gte_180s"}


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _parse_iso_ts(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _ts_to_iso(value: Any) -> str | None:
    ts = num(value, 0.0)
    if ts <= 0:
        return None
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _row_ts(row: dict[str, Any]) -> float:
    return (
        num(row.get("latest_observed_ts"))
        or num(row.get("source_detection_observed_ts"))
        or num(row.get("effective_latest_observed_ts"))
        or num(row.get("window_start_s"))
    )


def _row_deltas(row: dict[str, Any]) -> tuple[float | None, float | None]:
    window_start = num(row.get("window_start_s"), 0.0)
    if window_start <= 0:
        return None, None
    source_ts = num(row.get("source_detection_observed_ts")) or num(row.get("latest_observed_ts"))
    effective_ts = num(row.get("effective_latest_observed_ts")) or num(row.get("latest_observed_ts"))
    source_delta = source_ts - window_start if source_ts > 0 else None
    effective_delta = effective_ts - window_start if effective_ts > 0 else None
    return source_delta, effective_delta


def _first_touch_delta(row: dict[str, Any]) -> float | None:
    window_start = num(row.get("window_start_s"), 0.0)
    if window_start <= 0:
        return None
    first_touch_ts = (
        _parse_iso_ts(row.get("first_seen_at"))
        or num(row.get("first_seen_ts"))
        or num(row.get("first_observed_ts"))
        or num(row.get("source_detection_observed_ts"))
        or num(row.get("latest_observed_ts"))
    )
    return first_touch_ts - window_start if first_touch_ts > 0 else None


def _effective_min_submit_usd(row: dict[str, Any]) -> float:
    return max(
        num(row.get("process_min_live_order_usd")),
        num(row.get("effective_min_tranche_usd")),
        num(row.get("drip_min_tranche_usd")),
        num(row.get("would_floor_min_order_usd")),
    )


def _floor_budget_row(row: dict[str, Any]) -> bool:
    reason = str(row.get("dominant_skip_reason") or "").strip()
    category = str(row.get("participation_skip_category") or "").strip()
    return reason == "drip_min_tranche_exceeds_window_budget" or category == "FLOOR_BLOCKED_MISS"


def _policy_row(row: dict[str, Any]) -> bool:
    reason = str(row.get("dominant_skip_reason") or "").lower()
    category = str(row.get("participation_skip_category") or "").lower()
    return reason.startswith("policy:") or "price_outside_policy" in reason or "policy" in category


def _classify_row(row: dict[str, Any]) -> str:
    if _as_int(row.get("our_submits")) > 0 or _as_int(row.get("our_fills")) > 0:
        return "SUBMITTED"
    if _floor_budget_row(row):
        return "FLOOR_BUDGET_BIND"
    if _policy_row(row):
        return "POLICY_DEFECT"
    reason = str(row.get("dominant_skip_reason") or "").strip()
    if reason in LATE_REASONS:
        source_delta, effective_delta = _row_deltas(row)
        first_touch_delta = _first_touch_delta(row)
        if source_delta is not None and source_delta >= SOURCE_LATE_THRESHOLD_S:
            return "SOURCE_LATE"
        if (
            source_delta is not None
            and source_delta < SOURCE_LATE_THRESHOLD_S
            and first_touch_delta is not None
            and first_touch_delta >= SOURCE_LATE_THRESHOLD_S
        ):
            return "PIPELINE_LATE"
        if (
            source_delta is not None
            and source_delta < SOURCE_LATE_THRESHOLD_S
            and first_touch_delta is not None
            and first_touch_delta < SOURCE_LATE_THRESHOLD_S
            and effective_delta is not None
            and effective_delta >= SOURCE_LATE_THRESHOLD_S
        ):
            return "WINDOW_AGED_OUT_AFTER_TIMELY_TOUCH"
        return "LATE_BENIGN_UNDER_THRESHOLD"
    category = str(row.get("participation_skip_category") or "").strip()
    if category in {"CORRECT_SKIP", "PROTECTED_SKIP"}:
        return category
    return category or reason or "UNKNOWN"


def _compact_row(row: dict[str, Any]) -> dict[str, Any]:
    source_delta, effective_delta = _row_deltas(row)
    first_touch_delta = _first_touch_delta(row)
    return {
        "market_slug": row.get("market_slug"),
        "outcome": row.get("outcome"),
        "window_start_s": row.get("window_start_s"),
        "latest_observed_ts": row.get("latest_observed_ts"),
        "effective_latest_observed_ts": row.get("effective_latest_observed_ts"),
        "source_detection_observed_ts": row.get("source_detection_observed_ts"),
        "source_delta_s": None if source_delta is None else round(source_delta, 6),
        "effective_delta_s": None if effective_delta is None else round(effective_delta, 6),
        "first_touch_delta_s": None if first_touch_delta is None else round(first_touch_delta, 6),
        "dominant_skip_reason": row.get("dominant_skip_reason"),
        "participation_skip_category": row.get("participation_skip_category"),
        "tripwire_class": _classify_row(row),
        "window_budget_usd": row.get("window_budget_usd"),
        "drip_min_tranche_usd": row.get("drip_min_tranche_usd"),
        "min_order_usd": row.get("min_order_usd"),
        "policy_max_order_usd": row.get("policy_max_order_usd"),
        "source_inventory_usd": row.get("source_inventory_usd"),
        "target_usd_at_vwap": row.get("target_usd_at_vwap"),
        "gap_usd_at_vwap": row.get("gap_usd_at_vwap"),
        "wallet_eligible_orders": row.get("wallet_eligible_orders"),
        "our_submits": row.get("our_submits"),
        "our_fills": row.get("our_fills"),
    }


def _participation_rows(guard_state: dict[str, Any]) -> list[dict[str, Any]]:
    participation = guard_state.get("window_participation")
    participation = participation if isinstance(participation, dict) else {}
    rows = participation.get("rows")
    if not isinstance(rows, list):
        rows = participation.get("window_rollups")
    return [row for row in rows or [] if isinstance(row, dict)]


def _live_order_source_wallet(order: dict[str, Any]) -> str:
    candidates: list[Any] = []
    wallet_copy_inventory = order.get("wallet_copy_inventory")
    if isinstance(wallet_copy_inventory, dict):
        candidates.append(wallet_copy_inventory.get("source_wallet"))
    trade_decision = order.get("trade_decision")
    if isinstance(trade_decision, dict):
        wallet_copy = trade_decision.get("wallet_copy")
        if isinstance(wallet_copy, dict):
            candidates.append(wallet_copy.get("source_wallet"))
            metadata = wallet_copy.get("metadata")
            if isinstance(metadata, dict):
                candidates.append(metadata.get("source_wallet"))
                inventory = metadata.get("inventory_v2")
                if isinstance(inventory, dict):
                    candidates.append(inventory.get("source_wallet"))
    for candidate in candidates:
        wallet = _norm_wallet(candidate)
        if wallet:
            return wallet
    return ""


def _live_order_ts(order: dict[str, Any]) -> float | None:
    for key in ("updated_at", "submitted_at", "created_at"):
        parsed = _parse_iso_ts(order.get(key))
        if parsed is not None:
            return parsed
    lifecycle = order.get("lifecycle")
    if isinstance(lifecycle, list):
        for item in reversed(lifecycle):
            if isinstance(item, dict):
                parsed = _parse_iso_ts(item.get("ts"))
                if parsed is not None:
                    return parsed
    return None


def _a689_live_orders(live_state: dict[str, Any], *, wallet: str, postfix_start_ts: float) -> list[dict[str, Any]]:
    orders = live_state.get("orders") if isinstance(live_state.get("orders"), list) else []
    out: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict) or _live_order_source_wallet(order) != wallet:
            continue
        order_ts = _live_order_ts(order)
        if order_ts is None or order_ts < postfix_start_ts:
            continue
        out.append(order)
    return out


def _order_accepted(order: dict[str, Any]) -> bool:
    status = str(order.get("final_status") or order.get("status") or "").upper()
    return status not in REJECTED_ORDER_STATUSES


def build_report(
    *,
    guard_state_path: Path,
    live_state_path: Path,
    wallet: str,
    policy_id: str,
    postfix_start_iso: str,
    policy_leak_max_usd: float = POLICY_LEAK_MAX_USD,
    budget_bind_min_rows: int = 3,
    budget_bind_action: str = DEFAULT_BUDGET_BIND_ACTION,
    budget_bind_under_min_action: str = "",
    basis: str = DEFAULT_BASIS,
    rule: str = DEFAULT_RULE,
) -> dict[str, Any]:
    wallet = _norm_wallet(wallet)
    postfix_start_ts = _parse_iso_ts(postfix_start_iso)
    if not wallet or postfix_start_ts is None:
        raise ValueError("wallet and postfix_start_iso must be valid")
    guard_state = load_json(guard_state_path, default={})
    live_state = load_json(live_state_path, default={})
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    live_state = live_state if isinstance(live_state, dict) else {}

    rows = [
        row
        for row in _participation_rows(guard_state)
        if _norm_wallet(row.get("source_wallet")) == wallet and _row_ts(row) >= postfix_start_ts
    ]
    rows.sort(key=_row_ts, reverse=True)

    classes = Counter(_classify_row(row) for row in rows)
    categories = Counter(str(row.get("participation_skip_category") or "unknown") for row in rows)
    reasons = Counter(str(row.get("dominant_skip_reason") or "unknown") for row in rows)

    floor_rows = [row for row in rows if _classify_row(row) == "FLOOR_BUDGET_BIND"]
    budget_bind_rows = [
        row
        for row in floor_rows
        if num(row.get("window_budget_usd")) < _effective_min_submit_usd(row)
        and num(row.get("window_budget_usd")) >= num(row.get("min_order_usd"), 1.0)
    ]
    policy_leak_rows: list[dict[str, Any]] = []
    if policy_leak_max_usd > 0:
        policy_leak_rows = [
            row
            for row in floor_rows
            if 0 < num(row.get("policy_max_order_usd")) <= policy_leak_max_usd
        ]
    policy_rows = [row for row in rows if _classify_row(row) == "POLICY_DEFECT"]
    pipeline_late_rows = [row for row in rows if _classify_row(row) == "PIPELINE_LATE"]
    source_late_rows = [row for row in rows if _classify_row(row) == "SOURCE_LATE"]
    all_live_orders = _a689_live_orders(live_state, wallet=wallet, postfix_start_ts=postfix_start_ts)
    accepted_orders = [order for order in all_live_orders if _order_accepted(order)]

    top_class = classes.most_common(1)[0][0] if classes else "NO_ROWS"
    if accepted_orders:
        verdict = "A689_ACCEPTED_ORDER_PREEMPT"
        pre_ruled_action = "NOTIFY_ACCEPTED_ORDER"
    elif policy_leak_rows:
        verdict = "CONFIG_LEAK_DEFECT_POLICY_MAX_1"
        pre_ruled_action = "HOLD_DRIP_MIN_CHANGE_AND_ASK_FABLE"
    elif pipeline_late_rows or policy_rows:
        verdict = "REAL_DEFECT_NOTIFY_NO_SELF_REMEDIATION"
        pre_ruled_action = "NOTIFY_AND_ASK_FABLE"
    elif len(budget_bind_rows) >= budget_bind_min_rows and top_class == "FLOOR_BUDGET_BIND":
        verdict = "BUDGET_BINDS_DRIP_MIN_1_AUTHORIZED"
        pre_ruled_action = budget_bind_action
    elif budget_bind_under_min_action and budget_bind_rows and top_class == "FLOOR_BUDGET_BIND":
        verdict = "BUDGET_BIND_UNDER_MIN_SAMPLE"
        pre_ruled_action = budget_bind_under_min_action
    elif not rows or not any(item in classes for item in ("FLOOR_BUDGET_BIND", "POLICY_DEFECT", "PIPELINE_LATE")):
        verdict = "SOURCE_BEHAVIOR_NOT_DEFECT"
        pre_ruled_action = "KEEP_CANARY_ARMED_STANDING_ONE_LINER"
    else:
        verdict = "MIXED_TRIPWIRE_NEEDS_FABLE"
        pre_ruled_action = "NOTIFY_AND_ASK_FABLE"

    return {
        "schema_version": 1,
        "flow_stage": "LIVE/DEFEND/MEASURE",
        "generated_at": utc_now_iso(),
        "source_wallet": wallet,
        "policy_id": policy_id,
        "postfix_start_iso": postfix_start_iso,
        "guard_state_path": _display(guard_state_path),
        "live_state_path": _display(live_state_path),
        "basis": basis,
        "verdict": verdict,
        "pre_ruled_action": pre_ruled_action,
        "rows": len(rows),
        "windows": len({str(row.get("market_slug") or "") for row in rows if row.get("market_slug")}),
        "wallet_eligible_orders": sum(_as_int(row.get("wallet_eligible_orders")) for row in rows),
        "our_submits": sum(_as_int(row.get("our_submits")) for row in rows),
        "our_fills": sum(_as_int(row.get("our_fills")) for row in rows),
        "accepted_order_rows": len(accepted_orders),
        "live_order_rows": len(all_live_orders),
        "tripwire_class_counts": dict(sorted(classes.items())),
        "participation_category_counts": dict(sorted(categories.items())),
        "dominant_skip_reason_counts": dict(sorted(reasons.items())),
        "floor_budget_bind": {
            "rows": len(floor_rows),
            "budget_bind_rows": len(budget_bind_rows),
            "min_required_rows": budget_bind_min_rows,
            "policy_leak_max_usd": policy_leak_max_usd,
            "policy_max_1_rows": len(policy_leak_rows),
            "policy_max_values": sorted({num(row.get("policy_max_order_usd")) for row in floor_rows}),
            "budget_values": sorted({num(row.get("window_budget_usd")) for row in floor_rows}),
            "effective_min_values": sorted({_effective_min_submit_usd(row) for row in floor_rows}),
            "leak_check": (
                "DISABLED_EXPECTED_POLICY_MAX"
                if policy_leak_max_usd <= 0
                else "FAIL_POLICY_MAX_1"
                if policy_leak_rows
                else "PASS_NO_POLICY_MAX_1_BINDER"
            ),
            "samples": [_compact_row(row) for row in floor_rows[:10]],
        },
        "policy_defect": {
            "rows": len(policy_rows),
            "samples": [_compact_row(row) for row in policy_rows[:10]],
        },
        "pipeline_late": {
            "rows": len(pipeline_late_rows),
            "samples": [_compact_row(row) for row in pipeline_late_rows[:10]],
        },
        "source_late": {
            "rows": len(source_late_rows),
            "samples": [_compact_row(row) for row in source_late_rows[:10]],
        },
        "recent_rows": [_compact_row(row) for row in rows[:12]],
        "rule": rule,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD_STATE))
    parser.add_argument("--live-state", default=str(DEFAULT_LIVE_STATE))
    parser.add_argument("--wallet", default=DEFAULT_WALLET)
    parser.add_argument("--policy-id", default=DEFAULT_POLICY_ID)
    parser.add_argument("--postfix-start", default=DEFAULT_POSTFIX_START)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--policy-leak-max-usd", type=float, default=POLICY_LEAK_MAX_USD)
    parser.add_argument("--budget-bind-min-rows", type=int, default=3)
    parser.add_argument("--budget-bind-action", default=DEFAULT_BUDGET_BIND_ACTION)
    parser.add_argument("--budget-bind-under-min-action", default="")
    parser.add_argument("--basis", default=DEFAULT_BASIS)
    parser.add_argument("--rule", default=DEFAULT_RULE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_report(
        guard_state_path=Path(args.guard_state),
        live_state_path=Path(args.live_state),
        wallet=args.wallet,
        policy_id=args.policy_id,
        postfix_start_iso=args.postfix_start,
        policy_leak_max_usd=args.policy_leak_max_usd,
        budget_bind_min_rows=args.budget_bind_min_rows,
        budget_bind_action=args.budget_bind_action,
        budget_bind_under_min_action=args.budget_bind_under_min_action,
        basis=args.basis,
        rule=args.rule,
    )
    atomic_write_json(args.output, report)
    print(json.dumps({"verdict": report["verdict"], "pre_ruled_action": report["pre_ruled_action"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
