#!/usr/bin/env python3
"""Rank unchanged-bar candidate rows by fastest lawful live-seat evidence path."""

from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json

HARD_F1 = {
    "active_temporal_regime_cell_measured",
    "both_resolved_halves_positive",
    "f1_concentration_admissible",
    "f1_measured_positive_regime_cell",
    "f1_venue_reachable_admissible",
}


def _temporal_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("wallet") or "").lower(): row
        for row in payload.get("wallets") or []
        if isinstance(row, dict)
    }


def _hours_until(value: Any, now_s: float) -> float:
    try:
        return max(0.0, (datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() - now_s) / 3600.0)
    except (TypeError, ValueError):
        return 0.0


def _fresh_attempt_rate_per_h(row: dict[str, Any]) -> float:
    return max(0.0, 2.0 * float((row.get("direct_source") or {}).get("attempts") or 0.0))


def _latest_generation_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Collapse an identity to its latest/most-evidenced observation generation."""

    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        identity = (
            str(row.get("wallet") or "").lower(),
            str(row.get("wide_policy_fingerprint") or ""),
        )
        direct = row.get("direct_source") if isinstance(row.get("direct_source"), dict) else {}
        key = (
            _hours_until(direct.get("latest_receipt_at"), 0.0),
            float(direct.get("attempts") or 0.0),
            str(row.get("source_generation") or ""),
        )
        previous = latest.get(identity)
        if previous is None:
            latest[identity] = row
            continue
        previous_direct = previous.get("direct_source") if isinstance(previous.get("direct_source"), dict) else {}
        previous_key = (
            _hours_until(previous_direct.get("latest_receipt_at"), 0.0),
            float(previous_direct.get("attempts") or 0.0),
            str(previous.get("source_generation") or ""),
        )
        if key > previous_key:
            latest[identity] = row
    return list(latest.values()), len(rows) - len(latest)


def measure_row(row: dict[str, Any], temporal: dict[str, Any], now_s: float) -> dict[str, Any]:
    checks = row.get("checks") or {}
    deficits = [str(value) for value in row.get("evidence_deficits") or [] if ":" not in str(value)]
    active = row.get("active_temporal") or {}
    park = row.get("standby_exclusion") or {}
    distances: list[dict[str, Any]] = []
    structural: list[str] = []
    soft_eta_h: list[float] = []
    modeled_deficits: set[str] = set()
    if park.get("permanent_park") is True:
        structural.append("evidenced_permanent_park")
    if checks.get("own_evidenced_policy_available") is not True:
        structural.append("missing_own_evidenced_policy_without_rebuild_proof")
    hard_failed = sorted(name for name in HARD_F1 if checks.get(name) is not True)
    if hard_failed:
        structural.append("hard_f1_failures:" + ",".join(hard_failed))
    if checks.get("active_temporal_not_proven_negative") is not True:
        gap = abs(float(active.get("pnl_usd") or 0.0))
        distance = {"deficit": "active_temporal_not_proven_negative", "pnl_gap_usd": round(gap, 6)}
        distances.append(distance)
        modeled_deficits.add("active_temporal_not_proven_negative")
        if str(active.get("label") or "").upper() == "PROVEN-NEGATIVE" and gap > 25.0:
            structural.append("temporal_proven_negative_gt_25usd")
        else:
            rate = _fresh_attempt_rate_per_h(row)
            avg_stake = gap / abs(float(active.get("roi_pct") or 0.0) / 100.0) / max(1, int(active.get("resolved_trades") or 0)) if active.get("roi_pct") else 0.0
            trades = math.floor(gap / (avg_stake * 0.05)) + 1 if avg_stake else 1
            eta = trades / rate + 5.0 / 60.0 if rate else 9999.0
            distance.update({"assumed_roi_pct": 5.0, "required_additive_trades": trades, "eta_h": round(eta, 6)})
            soft_eta_h.append(eta)
    if checks.get("f2_fresh_rows_and_own_policy_copyable") is not True:
        direct = row.get("direct_source") or {}
        gap = max(0, 1 - int(direct.get("copyable") or 0))
        rate = _fresh_attempt_rate_per_h(row)
        # Zero observed successes receives a conservative Beta(1, n+1)
        # posterior mean rather than an invented zero/infinite rate.
        success_rate = (float(direct.get("copyable") or 0) + 1.0) / (float(direct.get("attempts") or 0) + 2.0)
        eta = gap / (rate * success_rate) if gap and rate else 0.0 if not gap else 9999.0
        distances.append({"deficit": "f2_fresh_rows_and_own_policy_copyable", "copyable_gap": gap, "posterior_success_rate": round(success_rate, 6), "eta_h": round(eta, 6)})
        modeled_deficits.add("f2_fresh_rows_and_own_policy_copyable")
        soft_eta_h.append(eta)
    if checks.get("f3_not_enabled_or_cooloff_or_fading") is not True and not structural:
        trow = temporal.get(str(row.get("wallet") or "").lower(), {})
        recent = trow.get("recent") or {}
        gap = abs(min(0.0, float(recent.get("pnl_usd") or 0.0)))
        rate = _fresh_attempt_rate_per_h(row)
        avg_stake = float(recent.get("stake_usd") or 0.0) / max(1, int(recent.get("resolved_trades") or 0))
        trades = math.floor(gap / (avg_stake * 0.05)) + 1 if gap and avg_stake else 0
        fading_eta = trades / rate + 5.0 / 60.0 if trades and rate else 0.0 if not gap else 9999.0
        cooloff_eta = _hours_until(row.get("cooloff_until"), now_s)
        eta = max(fading_eta, cooloff_eta)
        distances.append({"deficit": "fading_or_cooloff", "recent_pnl_gap_usd": round(gap, 6), "required_additive_trades_at_5pct_roi": trades, "cooloff_eta_h": round(cooloff_eta, 6), "eta_h": round(eta, 6)})
        modeled_deficits.add("f3_not_enabled_or_cooloff_or_fading")
        soft_eta_h.append(eta)
    if checks.get("f1_walk_forward_admissible") is not True and not (
        checks.get("active_temporal_not_proven_negative") is not True
        or checks.get("f3_not_enabled_or_cooloff_or_fading") is not True
        or hard_failed
    ):
        structural.append("walk_forward_fail_without_measured_soft_driver")
    partition = "STRUCTURAL_WALL" if structural else "SOFT_CLOCK"
    eta_h_modeled = None if structural else round(max(soft_eta_h, default=0.0), 6)
    unmodeled_deficits = sorted(set(deficits) - modeled_deficits)
    return {
        "wallet": str(row.get("wallet") or "").lower(),
        "wide_policy_fingerprint": str(row.get("wide_policy_fingerprint") or ""),
        "fail_count": len(deficits),
        "evidence_deficits": deficits,
        "partition": partition,
        "eta_h_modeled": eta_h_modeled,
        "unmodeled_deficits": unmodeled_deficits,
        "eta_completeness": "PARTIAL" if unmodeled_deficits else "COMPLETE",
        "distances": distances,
        "structural_reasons": structural,
        "venue_post_fee_pnl_usd": (row.get("regime_evidence") or {}).get("pnl_usd"),
        "policy_id": row.get("paper_policy_id") or (row.get("policy") or {}).get("policy_id"),
        "source_generation": row.get("source_generation"),
        "source_identity": row.get("source_identity"),
        "direct_source": row.get("direct_source"),
    }


def build_packet(
    candidate_evidence: dict[str, Any],
    temporal_payload: dict[str, Any],
    order6: dict[str, Any],
    now_s: float,
    *,
    deadman_checked_at: Any = None,
    score_run_id: str | None = None,
    prior_packet: dict[str, Any] | None = None,
) -> dict[str, Any]:
    temporal = _temporal_map(temporal_payload)
    source_rows = [row for row in candidate_evidence.get("rows") or [] if isinstance(row, dict)]
    latest_rows, collapsed_generations = _latest_generation_rows(source_rows)
    measured = [measure_row(row, temporal, now_s) for row in latest_rows]
    measured.sort(key=lambda row: (row["fail_count"], row["eta_h_modeled"] if row["eta_h_modeled"] is not None else 1e12, -float(row.get("venue_post_fee_pnl_usd") or 0.0), row["wallet"]))
    distinct: list[dict[str, Any]] = []
    seen_wallets: set[str] = set()
    for row in measured:
        if row["wallet"] in seen_wallets:
            continue
        seen_wallets.add(row["wallet"])
        distinct.append(row)
    soft = [row for row in distinct if row["partition"] == "SOFT_CLOCK"]
    top_soft = soft[0] if soft else None
    expected_row_count = candidate_evidence.get("candidate_count")
    cut_consistent = expected_row_count is None or int(expected_row_count) == len(source_rows)
    residual = int((order6.get("gen2") or {}).get("residual_to_200") or 175)
    order6_eta_h = round(residual / 5.0, 6)
    bind = bool(cut_consistent and top_soft and float(top_soft.get("eta_h_modeled") or 1e12) < 72.0 and float(top_soft.get("eta_h_modeled") or 1e12) < order6_eta_h)
    prior_packet = prior_packet if isinstance(prior_packet, dict) else {}
    prior_top = prior_packet.get("top_soft_clock") if isinstance(prior_packet.get("top_soft_clock"), dict) else {}
    same_top = bool(
        top_soft
        and prior_top.get("wallet") == top_soft.get("wallet")
        and prior_top.get("wide_policy_fingerprint") == top_soft.get("wide_policy_fingerprint")
    )
    distinct_cut = bool(
        deadman_checked_at
        and prior_packet.get("deadman_checked_at")
        and deadman_checked_at != prior_packet.get("deadman_checked_at")
    )
    prior_agreed = int((prior_packet.get("rank_stability") or {}).get("cuts_agreed") or 1)
    cuts_agreed = prior_agreed + 1 if same_top and distinct_cut else prior_agreed if same_top else 1
    return {
        "schema_version": 1,
        "kind": "order128_fastest_lawful_path",
        "flow_stage": "PROMOTE/LIVE",
        "generated_at": utc_now_iso(),
        "score_run_id": score_run_id,
        "paper_only": True,
        "live_orders_allowed": False,
        "bars_unchanged": True,
        "candidate_count": len(measured),
        "deadman_checked_at": deadman_checked_at,
        "deadman_row_count": len(source_rows),
        "deadman_declared_candidate_count": expected_row_count,
        "cut_consistent": cut_consistent,
        "collapsed_generations": collapsed_generations,
        "eligible_count": candidate_evidence.get("eligible_count"),
        "ranking_rule": "fail_count asc, eta_h_modeled asc, venue post-fee pnl desc; best row per wallet in top_10",
        "eta_assumptions": {"fresh_attempt_window_h": 0.5, "f2_zero_success_prior": "Beta(1,n+1) posterior mean", "fading_future_roi_pct": 5.0, "resolution_lag_h": round(5 / 60, 6), "order6_resolved_per_generation": 5, "order6_generation_h": 1.0},
        "order6_comparator": {"residual_to_200": residual, "eta_h": order6_eta_h},
        "top_10": distinct[:10],
        "ranked_rows": measured,
        "soft_clock_bench_depth": len({(row["wallet"], row["wide_policy_fingerprint"]) for row in measured if row["partition"] == "SOFT_CLOCK"}),
        "rank_stability": {
            "cuts_agreed": cuts_agreed,
            "required_cuts": 2,
            "stable_for_accrual": bool(bind and cuts_agreed >= 2),
            "prior_top_wallet": prior_top.get("wallet"),
            "prior_top_fingerprint": prior_top.get("wide_policy_fingerprint"),
            "prior_deadman_checked_at": prior_packet.get("deadman_checked_at"),
        },
        "stall_rule": {
            "after_h": 6,
            "condition": "f2 copyable gap remains 1 or walk-forward remains unmeasured",
            "status": "ORDER128_BIND_STALLED",
            "action": "DISCOVERY_WAVE_REFILL_SOFT_CLOCK_PARTITION",
            "successor_rerank_allowed": False,
        },
        "top_soft_clock": top_soft,
        "verdict": "REBIND_SOLE_PAPER_FOCUS" if bind else "CUT_MISMATCH_BIND_REFUSED" if not cut_consistent else "SEAT_PATH_STRUCTURALLY_CLOSED_NEAR_TERM",
        "binding_action": ({"wallet": top_soft["wallet"], "wide_policy_fingerprint": top_soft["wide_policy_fingerprint"], "policy_id": top_soft.get("policy_id"), "eta_h_modeled": top_soft.get("eta_h_modeled"), "eta_is_lower_bound": top_soft.get("eta_completeness") != "COMPLETE", "effective_scope": "next naturally generated WIDE paper manifest; no supervisor restart", "admission_authority": False} if bind and top_soft else None),
        "discovery_wave_required": not bind and cut_consistent,
    }


def candidate_evidence(deadman: dict[str, Any]) -> dict[str, Any]:
    choke = deadman.get("policy_choke") or {}
    return (
        ((choke.get("actuator") or {}).get("candidate_evidence"))
        or ((choke.get("source_roster_drought") or {}).get("candidate_evidence"))
        or {}
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadman", default="data/research/order_flow_deadman_state.json")
    parser.add_argument("--temporal", default="data/research/wallet_temporal_profitability_latest.json")
    parser.add_argument("--order6", default="data/research/wide_order6_gen2_retarget_latest.json")
    parser.add_argument("--output", default="data/research/order128_fastest_lawful_path_latest.json")
    parser.add_argument("--score-run-id")
    args = parser.parse_args()
    deadman = load_json(args.deadman, default={})
    evidence = candidate_evidence(deadman)
    prior_packet = load_json(args.output, default={})
    packet = build_packet(
        evidence,
        load_json(args.temporal, default={}),
        load_json(args.order6, default={}),
        datetime.now(timezone.utc).timestamp(),
        deadman_checked_at=deadman.get("checked_at"),
        score_run_id=args.score_run_id,
        prior_packet=prior_packet,
    )
    atomic_write_json(args.output, packet)
    print(packet["verdict"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
