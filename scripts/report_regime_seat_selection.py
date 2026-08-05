#!/usr/bin/env python3
"""Build a report-only regime-slice seat-selection evidence packet.

Flow stage: ROTATE/PROMOTE_PREP.  The packet deliberately separates the
best historical regime slice from the member that may hold the live seat
under the existing admission/runtime rules.  It never writes a live overlay
or selects a live candidate.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_TEMPORAL = ROOT / "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_GUARD = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_READY_SHADOW = ROOT / "data/research/wallet_copy_ready_shadow_lanes_state.json"
DEFAULT_READMISSION = ROOT / "data/research/watch_tier_readmission_rulings.json"
DEFAULT_OUTPUT = ROOT / "data/research/regime_seat_selection_latest.json"
DEFAULT_HOT_HISTORY = ROOT / "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_ROUTING_SHADOW = ROOT / "data/research/routing_shadow_validation_latest.json"
DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"


def _wallet(value: Any) -> str:
    value = str(value or "").strip().lower()
    return value if value.startswith("0x") and len(value) == 42 else ""


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _as_epoch(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def acceptance_share_rows(
    *,
    guard: dict[str, Any],
    temporal: dict[str, Any],
    hot_history: dict[str, Any],
    routing_shadow: dict[str, Any],
    ledger: dict[str, Any] | None = None,
    now_s: float,
    lookback_s: float,
    regime: str = "weekday",
) -> list[dict[str, Any]]:
    """Measure each runtime member's own-source policy acceptance share."""
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    wallets = {
        wallet
        for member in runtime.get("members") or []
        if isinstance(member, dict)
        for wallet in [_wallet(member.get("source_wallet") or member.get("wallet"))]
        if wallet and member.get("enabled") is not False
    }
    profiles = {
        wallet: row
        for row in temporal.get("wallets") or []
        if isinstance(row, dict)
        for wallet in [_wallet(row.get("wallet") or row.get("source_wallet"))]
        if wallet
    }
    raw_ids: dict[str, set[str]] = defaultdict(set)
    accepted_ids: dict[str, set[str]] = defaultdict(set)
    eligible_source_ids: dict[str, set[str]] = defaultdict(set)
    eligible_row_counts: dict[str, int] = defaultdict(int)
    live_accepted_ids: dict[str, set[str]] = defaultdict(set)
    since_s = now_s - lookback_s
    for row in hot_history.get("events") or []:
        if not isinstance(row, dict) or str(row.get("action") or "").upper() != "BUY":
            continue
        observed_s = _as_epoch(row.get("observed_ts") or row.get("event_ts"))
        wallet = _wallet(row.get("source_wallet"))
        if wallet in wallets and observed_s is not None and since_s <= observed_s <= now_s:
            event_id = str(row.get("event_id") or row.get("source_fingerprint") or "")
            if event_id:
                raw_ids[wallet].add(event_id)
    for row in routing_shadow.get("fee_gated_measurement_rows") or []:
        if not isinstance(row, dict):
            continue
        observed_s = _as_epoch(row.get("observed_ts") or row.get("source_detection_observed_ts"))
        wallet = _wallet(row.get("source_wallet") or row.get("copyintent_source_wallet"))
        accepted = str(row.get("dominant_skip_reason") or "").lower() == "eligible"
        if wallet in wallets and accepted and observed_s is not None and since_s <= observed_s <= now_s:
            intent_id = str(row.get("intent_id") or "")
            if intent_id:
                accepted_ids[wallet].add(intent_id)
            eligible_row_counts[wallet] += 1
            source_row_id = str(row.get("source_row_event_id") or "")
            if source_row_id:
                eligible_source_ids[wallet].add(source_row_id)
    accepted_statuses = {"FILLED", "LIVE_FILLED", "LIVE_MAKER_FILLED", "LIVE_SUBMITTED", "MATCHED", "SUBMITTED"}
    for row in (ledger or {}).get("orders") or []:
        if not isinstance(row, dict):
            continue
        statuses = {str(row.get("status") or "").upper(), str(row.get("final_status") or "").upper()}
        wallet = _wallet(row.get("source_wallet") or row.get("wallet"))
        observed = row.get("updated_at") or row.get("submitted_at") or row.get("accepted_at") or row.get("ts")
        try:
            observed_s = datetime.fromisoformat(str(observed).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError):
            observed_s = None
        if wallet in wallets and statuses & accepted_statuses and observed_s is not None and since_s <= observed_s <= now_s:
            live_id = str(row.get("order_id") or row.get("intent_id") or "")
            if live_id:
                live_accepted_ids[wallet].add(live_id)
    rows = []
    for wallet in sorted(wallets):
        raw_count = len(raw_ids[wallet])
        accepted_count = len(accepted_ids[wallet])
        comparable_source_ids = eligible_source_ids[wallet]
        identity_rows = len(comparable_source_ids)
        eligible_rows = eligible_row_counts[wallet]
        live_accepted_count = len(live_accepted_ids[wallet])
        slice_evidence = _slice_row(profiles.get(wallet, {}), regime)
        rows.append(
            {
                "wallet": wallet,
                "raw_own_source_buy_rows": raw_count,
                "policy_eligible_intents": accepted_count,
                "source_row_identity_coverage": (
                    round(identity_rows / eligible_rows, 6) if eligible_rows else 0.0
                ),
                "source_row_identity_rows": identity_rows,
                "source_row_identity_denominator": eligible_rows,
                "unmatched_source_row_count": len(raw_ids[wallet] - comparable_source_ids),
                "unmatched_source_row_ids": sorted(raw_ids[wallet] - comparable_source_ids)[:50],
                "unmatched_source_row_ids_truncated": len(raw_ids[wallet] - comparable_source_ids) > 50,
                "accepted_live_orders": live_accepted_count,
                "acceptance_share_pct": round(100.0 * live_accepted_count / raw_count, 6) if raw_count else 0.0,
                "materially_nonzero": live_accepted_count > 0,
                "regime_slice_label": slice_evidence["label"],
                "regime_slice_pnl_usd": slice_evidence["pnl_usd"],
                "regime_slice_roi_pct": slice_evidence["roi_pct"],
            }
        )
    return rows


def policy_choke_rung_a(
    rows: list[dict[str, Any]],
    selected_wallet: str,
    *,
    regime: str,
) -> dict[str, Any]:
    incumbent = next((row for row in rows if row.get("wallet") == selected_wallet), {})
    alternatives = [
        row for row in rows
        if row.get("wallet") != selected_wallet
        and row.get("regime_slice_label") == "PROVEN-POSITIVE"
        and bool(row.get("materially_nonzero"))
    ]
    alternatives.sort(
        key=lambda row: (
            -int(row.get("accepted_live_orders") or 0),
            -float(row.get("acceptance_share_pct") or 0.0),
            -float(row.get("regime_slice_pnl_usd") or 0.0),
            str(row.get("wallet") or ""),
        )
    )
    target = alternatives[0] if not bool(incumbent.get("materially_nonzero")) and alternatives else None
    return {
        "incumbent_wallet": selected_wallet or None,
        "incumbent": incumbent or None,
        "target_wallet": target.get("wallet") if target else None,
        "target": target,
        "action": "RUNG_A_RESELECT" if target else "NO_RUNG_A_TARGET",
        "rule": (
            f"a {regime}-positive runtime member with nonzero own-source "
            "acceptance outranks a zero-acceptance incumbent"
        ),
        "rows": rows,
    }


def _slice_row(profile: dict[str, Any], regime: str) -> dict[str, Any]:
    labels = profile.get("slice_labels") if isinstance(profile.get("slice_labels"), dict) else {}
    row = labels.get(regime) if isinstance(labels.get(regime), dict) else {}
    return {
        "label": str(row.get("label") or "UNPROVEN").upper(),
        "resolved_trades": int(row.get("resolved_trades") or 0),
        "pnl_usd": row.get("pnl_usd"),
        "roi_pct": row.get("roi_pct"),
        "reason": row.get("reason"),
    }


def _disabled_runtime_wallets(runtime: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    sweep = runtime.get("external_liveness_sweep") if isinstance(runtime.get("external_liveness_sweep"), dict) else {}
    for report in sweep.get("reports") or []:
        if not isinstance(report, dict):
            continue
        for row in report.get("disabled_members") or []:
            if not isinstance(row, dict):
                continue
            wallet = _wallet(row.get("source_wallet") or row.get("wallet"))
            if wallet:
                out[wallet] = row
    return out


def build_packet(
    *,
    temporal: dict[str, Any],
    guard: dict[str, Any],
    ready_shadow: dict[str, Any],
    readmission: dict[str, Any],
    regime: str,
    generated_at: str,
    hot_history: dict[str, Any] | None = None,
    routing_shadow: dict[str, Any] | None = None,
    ledger: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profiles = {
        wallet: row
        for row in temporal.get("wallets") or []
        if isinstance(row, dict)
        for wallet in [_wallet(row.get("wallet") or row.get("source_wallet"))]
        if wallet
    }
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    selected = runtime.get("selected_member") if isinstance(runtime.get("selected_member"), dict) else {}
    selected_wallet = _wallet(selected.get("source_wallet") or selected.get("wallet"))
    runtime_members = {
        wallet: row
        for row in runtime.get("members") or []
        if isinstance(row, dict)
        for wallet in [_wallet(row.get("source_wallet") or row.get("wallet"))]
        if wallet
    }
    disabled = _disabled_runtime_wallets(runtime)
    shadows = {
        wallet: row
        for row in ready_shadow.get("lanes") or []
        if isinstance(row, dict)
        for wallet in [_wallet(row.get("wallet") or row.get("source_wallet"))]
        if wallet
    }
    rulings = {
        wallet: row
        for row in readmission.get("rulings") or []
        if isinstance(row, dict)
        for wallet in [_wallet(row.get("source_wallet") or row.get("wallet"))]
        if wallet
    }
    candidates: list[dict[str, Any]] = []
    for wallet in sorted(set(runtime_members) | set(shadows) | set(rulings)):
        member = runtime_members.get(wallet, {})
        shadow = shadows.get(wallet, {})
        ruling = rulings.get(wallet, {})
        slice_evidence = _slice_row(profiles.get(wallet, {}), regime)
        runtime_enabled = bool(member) and member.get("enabled") is not False and wallet not in disabled
        live_orders_allowed = bool(member) and runtime_enabled and not bool(shadow.get("paper_only"))
        exclusion_reasons: list[str] = []
        if slice_evidence["label"] != "PROVEN-POSITIVE":
            exclusion_reasons.append(f"{regime}_slice_{slice_evidence['label'].lower()}")
        if not member:
            exclusion_reasons.append("not_in_runtime_active_set")
        elif wallet in disabled:
            exclusion_reasons.append(str(disabled[wallet].get("reason") or "runtime_member_disabled"))
        if shadow and not bool(shadow.get("live_orders_allowed")):
            exclusion_reasons.append("ready_shadow_paper_only")
        if ruling and not member:
            exclusion_reasons.append(f"readmission_{str(ruling.get('ruling') or 'pending').lower()}")
        eligible = bool(live_orders_allowed and slice_evidence["label"] == "PROVEN-POSITIVE")
        candidates.append(
            {
                "wallet": wallet,
                "candidate_id": member.get("candidate_id") or shadow.get("candidate_id"),
                "policy_id": member.get("policy_id") or shadow.get("paper_policy_id"),
                "current_selected": wallet == selected_wallet,
                "runtime_member": bool(member),
                "runtime_enabled": runtime_enabled,
                "live_seat_eligible_existing_rules": eligible,
                "exclusion_reasons": exclusion_reasons,
                "regime_slice": slice_evidence,
                "temporal_classification": profiles.get(wallet, {}).get("classification"),
                "ready_shadow": {
                    "present": bool(shadow),
                    "paper_only": shadow.get("paper_only"),
                    "live_orders_allowed": shadow.get("live_orders_allowed"),
                    "readiness_verdict": shadow.get("readiness_verdict"),
                    "readmission_started_at": shadow.get("readmission_started_at"),
                    "ready_shadow_min_hours": shadow.get("ready_shadow_min_hours"),
                    "resolved_paper_fills": shadow.get("resolved_paper_fills"),
                    "external_latest_trade_age_h": shadow.get("external_latest_trade_age_h"),
                    "fading_clear": shadow.get("fading_clear"),
                    "live_canary_packet_preconditions": shadow.get("live_canary_packet_preconditions"),
                },
                "readmission_ruling": ruling.get("ruling"),
            }
        )

    def rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
        evidence = row["regime_slice"]
        roi = evidence.get("roi_pct")
        pnl = evidence.get("pnl_usd")
        return (
            0 if row["live_seat_eligible_existing_rules"] else 1,
            0 if evidence.get("label") == "PROVEN-POSITIVE" else 1,
            -float(roi) if roi is not None else float("inf"),
            -float(pnl) if pnl is not None else float("inf"),
            row["wallet"],
        )

    candidates.sort(key=rank_key)
    eligible = [row for row in candidates if row["live_seat_eligible_existing_rules"]]
    positive = [row for row in candidates if row["regime_slice"]["label"] == "PROVEN-POSITIVE"]
    selected_eligible = next(
        (row for row in eligible if row["wallet"] == selected_wallet),
        None,
    )
    winner = selected_eligible or (eligible[0] if eligible else None)
    evidence_leader = sorted(positive, key=lambda row: rank_key({**row, "live_seat_eligible_existing_rules": True}))[0] if positive else None
    generated_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    now_s = generated_dt.timestamp()
    acceptance_30m = acceptance_share_rows(
        guard=guard, temporal=temporal, hot_history=hot_history or {},
        routing_shadow=routing_shadow or {}, ledger=ledger or {}, now_s=now_s, lookback_s=1800, regime=regime,
    )
    acceptance_90m = acceptance_share_rows(
        guard=guard, temporal=temporal, hot_history=hot_history or {},
        routing_shadow=routing_shadow or {}, ledger=ledger or {}, now_s=now_s, lookback_s=5400, regime=regime,
    )
    return {
        "schema_version": 1,
        "kind": "regime_seat_selection_evidence",
        "flow_stage": "ROTATE/PROMOTE_PREP",
        "generated_at": generated_at,
        "regime": regime,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "selection_authority": "existing runtime admission, temporal exclusion, and Fable readmission rules",
        "current_selected_wallet": selected_wallet,
        "winner_wallet": winner.get("wallet") if winner else None,
        "winner_reason": (
            "existing runtime-selected member remains eligible after explicit regime-slice evaluation"
            if selected_eligible
            else "highest regime-slice evidence among members already live-seat-eligible under existing rules"
            if winner
            else "no live-seat-eligible member under existing rules"
        ),
        "regime_evidence_leader_wallet": evidence_leader.get("wallet") if evidence_leader else None,
        "evidence_leader_is_live_eligible": bool(evidence_leader and evidence_leader["live_seat_eligible_existing_rules"]),
        "candidate_count": len(candidates),
        "eligible_count": len(eligible),
        "candidates": candidates,
        "acceptance_share_30m": policy_choke_rung_a(
            acceptance_30m, selected_wallet, regime=regime
        ),
        "acceptance_share_90m": policy_choke_rung_a(
            acceptance_90m, selected_wallet, regime=regime
        ),
        "next": "log winner and why; do not mutate live selection from this report-only packet",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--temporal", default=str(DEFAULT_TEMPORAL))
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD))
    parser.add_argument("--ready-shadow", default=str(DEFAULT_READY_SHADOW))
    parser.add_argument("--readmission", default=str(DEFAULT_READMISSION))
    parser.add_argument("--hot-history", default=str(DEFAULT_HOT_HISTORY))
    parser.add_argument("--routing-shadow", default=str(DEFAULT_ROUTING_SHADOW))
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--regime", choices=("weekday", "weekend"), required=True)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    packet = build_packet(
        temporal=load_json(Path(args.temporal), default={}),
        guard=load_json(Path(args.guard_state), default={}),
        ready_shadow=load_json(Path(args.ready_shadow), default={}),
        readmission=load_json(Path(args.readmission), default={}),
        regime=args.regime,
        generated_at=_iso_now(),
        hot_history=load_json(Path(args.hot_history), default={}),
        routing_shadow=load_json(Path(args.routing_shadow), default={}),
        ledger=load_json(Path(args.ledger), default={}),
    )
    atomic_write_json(Path(args.output), packet)
    print(Path(args.output))


if __name__ == "__main__":
    main()
