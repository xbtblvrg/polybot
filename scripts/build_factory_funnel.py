#!/usr/bin/env python3
"""Build the end-to-end money-machine factory funnel.

Flow stage: DISCOVER/LEARN/PROMOTE/LIVE. This is a read-only diagnostic that
walks the causal ladder from raw market ore to profitable sustained live output
and names the first materially broken conversion.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/factory_funnel_latest.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _summary(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("summary") if isinstance(payload.get("summary"), dict) else {}


def _today_from_now() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _order_ts(order: dict[str, Any]) -> str:
    for event in order.get("lifecycle") or []:
        if isinstance(event, dict) and event.get("status") == "INTENT_RECEIVED":
            return str(event.get("ts") or "")
    for key in ("submitted_at", "created_at", "ts", "timestamp"):
        value = order.get(key)
        if value:
            return str(value)
    return ""


def _today_live_order_counts(live_execution: dict[str, Any], day: str) -> dict[str, Any]:
    orders = live_execution.get("orders") if isinstance(live_execution.get("orders"), list) else []
    today_orders = [order for order in orders if isinstance(order, dict) and _order_ts(order).startswith(day)]
    fills = [order for order in today_orders if str(order.get("final_status") or "").upper() == "FILLED"]
    rejects = [order for order in today_orders if str(order.get("final_status") or "").upper() == "REJECTED"]
    submitted = [order for order in today_orders if str(order.get("final_status") or "").upper() == "SUBMITTED"]
    windows_submitted = {str(order.get("market_slug") or "") for order in today_orders if order.get("market_slug")}
    windows_filled = {str(order.get("market_slug") or "") for order in fills if order.get("market_slug")}
    return {
        "orders": len(today_orders),
        "fills": len(fills),
        "rejects": len(rejects),
        "submitted_open": len(submitted),
        "windows_submitted": len(windows_submitted),
        "windows_filled": len(windows_filled),
        "latest_order_ts": max((_order_ts(order) for order in today_orders), default=""),
    }


def _day_pnl_from_text(text: str) -> tuple[float | None, str]:
    match = re.search(r"total orders=\d+ fills=\d+ resolved=\d+ rejects=\d+ pnl=([+-]?\d+(?:\.\d+)?)", text)
    if match:
        return _as_float(match.group(1)), "scorecard_text_total_line"
    return None, "missing_scorecard_text_total_line"


def _day_pnl(scorecard: dict[str, Any], scorecard_text: str) -> dict[str, Any]:
    value, source = _day_pnl_from_text(scorecard_text)
    if value is not None:
        return {"day_pnl_usd": round(value, 6), "source": source}
    truth = scorecard.get("canonical_pnl_truth") if isinstance(scorecard.get("canonical_pnl_truth"), dict) else {}
    by_day = truth.get("by_day") if isinstance(truth.get("by_day"), dict) else {}
    today = by_day.get(_today_from_now()) if isinstance(by_day.get(_today_from_now()), dict) else {}
    if today:
        return {
            "day_pnl_usd": round(_as_float(today.get("pnl_usd")), 6),
            "source": "wallet_copy_daily_scorecard_current.canonical_pnl_truth",
        }
    return {"day_pnl_usd": None, "source": "missing"}


def _defense_posture(root: Path) -> dict[str, Any]:
    digest = _load_json(root / "data" / "research" / "state_digest.json", {})
    tripwires = digest.get("defense_tripwires") if isinstance(digest.get("defense_tripwires"), dict) else {}
    status = str(tripwires.get("status") or "UNKNOWN")
    triggered = status.upper() == "TRIGGERED"
    return {
        "defense_tripwires_status": status,
        "size_defense_action": tripwires.get("size_defense_action"),
        "intraday_probe_triggered": tripwires.get("intraday_probe_triggered"),
        "day_pnl_usd": tripwires.get("t1_day_pnl_usd"),
        "admission_widening_allowed": not triggered,
        "reason": (
            "defense_tripwires_triggered_hold_admission_widening"
            if triggered
            else "defense_tripwires_not_triggered"
        ),
    }


def _rate(to_count: int | float, from_count: int | float) -> float | None:
    if not from_count:
        return None
    return round((float(to_count) / float(from_count)) * 100.0, 6)


def _link(
    *,
    link_id: str,
    from_label: str,
    to_label: str,
    from_count: int | float,
    to_count: int | float,
    min_rate_pct: float | None,
    min_to_count: int | None = None,
    next_action: str,
) -> dict[str, Any]:
    rate = _rate(to_count, from_count)
    broken_reasons: list[str] = []
    if min_rate_pct is not None and rate is not None and rate < float(min_rate_pct):
        broken_reasons.append(f"rate_below_{min_rate_pct:g}_pct")
    if min_to_count is not None and int(to_count or 0) < int(min_to_count):
        broken_reasons.append(f"count_below_{min_to_count}")
    return {
        "id": link_id,
        "from": from_label,
        "to": to_label,
        "from_count": from_count,
        "to_count": to_count,
        "conversion_rate_pct": rate,
        "min_rate_pct": min_rate_pct,
        "min_to_count": min_to_count,
        "status": "BROKEN" if broken_reasons else "PASS",
        "broken_reasons": broken_reasons,
        "next_action": next_action,
    }


def build_funnel(root: Path, *, day: str | None = None) -> dict[str, Any]:
    day = day or _today_from_now()
    data = root / "data" / "research"
    intake = _load_json(data / "wallet_market_scan_ranked.json", {})
    mining = _load_json(data / "wallet_market_mining_cadence_state.json", {})
    cohort = _load_json(data / "wallet_market_cohort_replay_latest.json", {})
    packets = _load_json(data / "cohort_alive_admission_packets_latest.json", {})
    overlay = _load_json(data / "wallet_copy_active_set_auto_degrade_state.json", {})
    guard = _load_json(data / "wallet_copy_live_guard_state.json", {})
    live_execution = _load_json(data / "wallet_copy_live_execution_state.json", {})
    from src.wallet_copy.scorecard import load_fresh_scorecard

    scorecard = load_fresh_scorecard(data / "wallet_copy_daily_scorecard_current.json")
    scorecard_text = ""
    try:
        scorecard_text = (data / "brainless_ops_scorecard.out").read_text()
    except Exception:
        scorecard_text = ""

    intake_summary = _summary(intake)
    mining_observed = mining.get("observed") if isinstance(mining.get("observed"), dict) else {}
    cohort_summary = _summary(cohort)
    packet_summary = _summary(packets)
    wave = overlay.get("latest_admission_wave") if isinstance(overlay.get("latest_admission_wave"), dict) else {}
    active_runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    live_counts = _today_live_order_counts(live_execution, day)
    pnl = _day_pnl(scorecard, scorecard_text)
    defense_posture = _defense_posture(root)
    posture_hold = defense_posture.get("admission_widening_allowed") is False

    market_population = _as_int(intake_summary.get("wallets_ranked") or mining_observed.get("intake_wallets_ranked"))
    mined_actives = _as_int(intake_summary.get("active_wallets") or mining_observed.get("intake_active_wallets"))
    scored = _as_int(cohort_summary.get("cohort_size") or mining_observed.get("cohort_size"))
    shadow_positive = _as_int(cohort_summary.get("cohort_shadow_positive") or mining_observed.get("shadow_positive"))
    live_ready = _as_int(cohort_summary.get("live_ready_picks") or mining_observed.get("live_ready_picks"))
    packet_count = _as_int(packet_summary.get("packet_count") or mining_observed.get("packet_count"))
    admitted = _as_int(wave.get("admitted_count"))
    armed = _as_int(wave.get("runtime_loaded_count"))
    runtime_members = len(active_runtime.get("members") or []) if isinstance(active_runtime.get("members"), list) else 0
    submitted = _as_int(live_counts.get("orders"))
    filled = _as_int(live_counts.get("fills"))
    profitable = 1 if pnl.get("day_pnl_usd") is not None and float(pnl.get("day_pnl_usd") or 0.0) > 0 else 0
    sustained = 1 if profitable and filled >= 144 else 0
    live_ready_to_admitted_next = (
        "HOLD admission widening while defense tripwires are TRIGGERED; preserve current wave and repair filled->profitable quality under probe caps"
        if posture_hold
        else "run wave admissions until ready supply is no longer the choke; preserve probe caps"
    )

    links = [
        _link(
            link_id="market_population_to_mined_actives",
            from_label="market_population",
            to_label="mined_actives",
            from_count=market_population,
            to_count=mined_actives,
            min_rate_pct=20.0,
            next_action="expand market intake pages/sources if active yield drops",
        ),
        _link(
            link_id="mined_actives_to_scored",
            from_label="mined_actives",
            to_label="scored",
            from_count=mined_actives,
            to_count=scored,
            min_rate_pct=50.0,
            next_action="increase replay worker throughput or widen replay budget",
        ),
        _link(
            link_id="scored_to_shadow_positive",
            from_label="scored",
            to_label="shadow_positive",
            from_count=scored,
            to_count=shadow_positive,
            min_rate_pct=5.0,
            next_action="audit ore quality and scoring filters if positive yield collapses",
        ),
        _link(
            link_id="shadow_positive_to_live_ready",
            from_label="shadow_positive",
            to_label="live_ready",
            from_count=shadow_positive,
            to_count=live_ready,
            min_rate_pct=50.0,
            next_action="repair packet gates or score/serve parity if ready conversion falls",
        ),
        _link(
            link_id="live_ready_to_admitted",
            from_label="live_ready",
            to_label="admitted",
            from_count=live_ready,
            to_count=admitted,
            min_rate_pct=20.0 if live_ready >= 20 else None,
            min_to_count=10 if live_ready >= 10 else None,
            next_action=live_ready_to_admitted_next,
        ),
        _link(
            link_id="admitted_to_armed",
            from_label="admitted",
            to_label="armed_runtime_loaded",
            from_count=admitted,
            to_count=armed,
            min_rate_pct=90.0 if admitted else None,
            next_action="repair active-set overlay/runtime guard loading if admitted wallets are not armed",
        ),
        _link(
            link_id="armed_to_submitted",
            from_label="armed_runtime_loaded",
            to_label="submitted_live_orders_today",
            from_count=max(armed, runtime_members),
            to_count=submitted,
            min_rate_pct=None,
            min_to_count=1 if max(armed, runtime_members) else None,
            next_action="walk source row -> gate -> intent if armed members emit no submissions",
        ),
        _link(
            link_id="submitted_to_filled",
            from_label="submitted_live_orders_today",
            to_label="filled_live_orders_today",
            from_count=submitted,
            to_count=filled,
            min_rate_pct=50.0 if submitted else None,
            next_action="inspect execution quality, maker fallback, and FAK no-match causes",
        ),
        _link(
            link_id="filled_to_profitable_day",
            from_label="filled_live_orders_today",
            to_label="profitable_day",
            from_count=filled,
            to_count=profitable,
            min_rate_pct=None,
            min_to_count=1 if filled else None,
            next_action="decompose member/cell PnL and rotate/cap negative lanes mechanically",
        ),
        _link(
            link_id="profitable_day_to_sustained",
            from_label="profitable_day",
            to_label="sustained_target_run_rate",
            from_count=max(profitable, 1),
            to_count=sustained,
            min_rate_pct=None,
            min_to_count=1,
            next_action="continue ramp only after profitable day reaches target coverage/run-rate",
        ),
    ]
    first_broken = next((link for link in links if link.get("status") == "BROKEN"), None)
    filled_to_profitable = next((link for link in links if link.get("id") == "filled_to_profitable_day"), None)
    enemy = (
        filled_to_profitable
        if posture_hold and isinstance(filled_to_profitable, dict) and filled_to_profitable.get("status") == "BROKEN"
        else first_broken
    )
    payload = {
        "schema_version": 1,
        "kind": "factory_funnel",
        "flow_stage": "DISCOVER/LEARN/PROMOTE/LIVE/DEFEND",
        "generated_at": _utc_now_iso(),
        "day_utc": day,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "counts": {
            "market_population": market_population,
            "mined_actives": mined_actives,
            "scored": scored,
            "shadow_positive": shadow_positive,
            "live_ready": live_ready,
            "admission_packets": packet_count,
            "admitted": admitted,
            "armed_runtime_loaded": armed,
            "runtime_members": runtime_members,
            "submitted_live_orders_today": submitted,
            "filled_live_orders_today": filled,
            "rejected_live_orders_today": live_counts.get("rejects"),
            "profitable_day": profitable,
            "sustained_target_run_rate": sustained,
        },
        "money": {
            **pnl,
            "windows_submitted": live_counts.get("windows_submitted"),
            "windows_filled": live_counts.get("windows_filled"),
            "latest_order_ts": live_counts.get("latest_order_ts"),
        },
        "posture_gate": defense_posture,
        "links": links,
        "first_materially_broken_link": first_broken,
        "enemy_line": {
            "status": "RED" if enemy else "GREEN",
            "link_id": enemy.get("id") if enemy else None,
            "topological_first_link_id": first_broken.get("id") if first_broken else None,
            "next_action": enemy.get("next_action") if enemy else "continue cycle; no broken conversion named",
        },
        "inputs": {
            "intake": "data/research/wallet_market_scan_ranked.json",
            "mining_cadence": "data/research/wallet_market_mining_cadence_state.json",
            "cohort_replay": "data/research/wallet_market_cohort_replay_latest.json",
            "admission_packets": "data/research/cohort_alive_admission_packets_latest.json",
            "active_set_overlay": "data/research/wallet_copy_active_set_auto_degrade_state.json",
            "live_guard": "data/research/wallet_copy_live_guard_state.json",
            "live_execution": "data/research/wallet_copy_live_execution_state.json",
            "scorecard": "data/research/wallet_copy_daily_scorecard_current.json",
        },
    }
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--day", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    payload = build_funnel(root, day=args.day or None)
    atomic_write_json(root / args.output, payload)
    print(
        json.dumps(
            {
                "status": payload["enemy_line"]["status"],
                "first_materially_broken_link": (
                    payload["first_materially_broken_link"].get("id")
                    if isinstance(payload["first_materially_broken_link"], dict)
                    else None
                ),
                "enemy_line": payload["enemy_line"]["link_id"],
                "counts": payload["counts"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
