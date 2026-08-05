#!/usr/bin/env python3
"""Attribute activated members' own-source -> eligible -> accepted funnel.

Flow stage: LIVE/DEFEND/PROMOTE/LEARN.  This reporter reads the guard's
paper-only per-member probes and the order-flow deadman.  It never changes
selection, policy, caps, or live execution state.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


TARGETS = {
    "ee888f": {
        "wallet": "0xee888fa7b96007f7fa270988e92bddb0ae19ed10",
        "candidate_id": "market_cohort_alive_ddb0ae19ed10",
        "evidence": ROOT / "data/research/ee888f_weekday_admission_evidence_latest.json",
        "probe": ROOT / "data/research/wallet_copy_live_execution_probe_market_cohort_alive_ddb0ae19ed10.json",
    },
    "a3e0": {
        "wallet": "0xa3e0985f2d0b3209a52f171660287863690d095d",
        "candidate_id": "market_cohort_alive_a3e0985f2d",
        "evidence": ROOT / "data/research/a3e0_weekday_live_probe_evidence_latest.json",
        "probe": ROOT / "data/research/wallet_copy_live_execution_probe_market_cohort_alive_a3e0985f2d.json",
    },
}

DEFAULT_DEADMAN = ROOT / "data/research/order_flow_deadman_state.json"
DEFAULT_GUARD = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/coacceptance_eligibility_shadow_latest.json"


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _funnel(probe: dict[str, Any]) -> dict[str, Any]:
    summary = probe.get("candidate_intent_summary") if isinstance(probe.get("candidate_intent_summary"), dict) else {}
    prefilter = summary.get("live_event_prefilter") if isinstance(summary.get("live_event_prefilter"), dict) else {}
    reasons = Counter()
    for field in ("policy_reject_counts", "skip_counts"):
        values = prefilter.get(field) if isinstance(prefilter.get(field), dict) else {}
        reasons.update({str(key): int(value or 0) for key, value in values.items()})
    stages = {
        "source_events": int(summary.get("source_events") or 0),
        "candidate_build_events": int(summary.get("candidate_build_events") or 0),
        "candidate_intents": int(summary.get("candidate_intents") or 0),
        "fresh_candidate_intents": int(summary.get("fresh_candidate_intents") or 0),
        "after_entry_price_band_gate": int(summary.get("fresh_candidate_intents_after_entry_price_band_gate") or 0),
        "after_profit_latency_suppression": int(summary.get("fresh_candidate_intents_after_profit_latency_suppression") or 0),
        "after_window_fill_cap": int(summary.get("fresh_candidate_intents_after_window_fill_cap") or 0),
        "after_expected_fee_gate": int(summary.get("fresh_candidate_intents_after_expected_fee_gate") or 0),
    }
    return {
        "probe_generated_at": probe.get("generated_at"),
        "probe_status": probe.get("status"),
        "paper_only": True,
        "live_orders_allowed": False,
        "stages": stages,
        "prefilter_candidate_groups": int(prefilter.get("candidate_groups") or 0),
        "prefilter_retained_intents": int(prefilter.get("retained_intents") or 0),
        "observed_gate_counts": dict(sorted(reasons.items())),
        "dominant_observed_gate": reasons.most_common(1)[0][0] if reasons else "NO_RECORDED_GATE_REASON",
        "latest_source_event_runtime": prefilter.get("latest_source_event_runtime") or {},
    }


def build_packet(
    *,
    deadman: dict[str, Any],
    guard: dict[str, Any],
    evidence_by_name: dict[str, dict[str, Any]],
    probe_by_name: dict[str, dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    choke = deadman.get("policy_choke") if isinstance(deadman.get("policy_choke"), dict) else {}
    rung = choke.get("rung_a_seat_read") if isinstance(choke.get("rung_a_seat_read"), dict) else {}
    live_rows = {
        str(row.get("wallet") or "").lower(): row
        for row in rung.get("rows") or []
        if isinstance(row, dict)
    }
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    selected = runtime.get("selected_member") if isinstance(runtime.get("selected_member"), dict) else {}
    members = {
        str(row.get("source_wallet") or "").lower(): row
        for row in runtime.get("members") or []
        if isinstance(row, dict)
    }
    rows = []
    for name, target in TARGETS.items():
        wallet = target["wallet"]
        evidence = evidence_by_name.get(name) or {}
        live = live_rows.get(wallet, {})
        member = members.get(wallet, {})
        funnel = _funnel(probe_by_name.get(name) or {})
        raw = int(live.get("raw_own_source_buy_rows") or 0)
        eligible = int(live.get("policy_eligible_intents") or 0)
        accepted = int(live.get("accepted_live_orders") or 0)
        rows.append({
            "name": name,
            "wallet": wallet,
            "candidate_id": target["candidate_id"],
            "activation_status": evidence.get("status"),
            "activated_at": evidence.get("activated_at"),
            "runtime_enabled": member.get("enabled") is not False and bool(member),
            "policy_id": member.get("policy_id"),
            "own_source_rows_30m": raw,
            "policy_eligible_intents_30m": eligible,
            "accepted_live_orders_30m": accepted,
            "own_to_eligible_pct": round(100.0 * eligible / raw, 6) if raw else 0.0,
            "eligible_to_accepted_pct": round(100.0 * accepted / eligible, 6) if eligible else 0.0,
            "materially_nonzero_converting": accepted > 0,
            "probe_funnel": funnel,
        })
    converting = [row for row in rows if row["materially_nonzero_converting"]]
    return {
        "schema_version": 1,
        "kind": "coacceptance_eligibility_shadow",
        "flow_stage": "LIVE/DEFEND/PROMOTE/LEARN",
        "generated_at": generated_at,
        "status": "CONVERTING_TARGET_PRESENT" if converting else "ACCRUING_ZERO_TARGET_CONVERSION",
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
        "can_trade": deadman.get("can_trade"),
        "order_flow_status": deadman.get("status"),
        "policy_choke_status": choke.get("status"),
        "selected_wallet": selected.get("source_wallet"),
        "seat_action": "OP_NO_WAIT_REEVALUATE" if converting else "KEEP_A689_TARGETS_ZERO_CONVERTING",
        "rows": rows,
        "next": "refresh each heartbeat; selection changes only when a target has materially nonzero eligible-to-accepted conversion and canary preconditions pass",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadman", default=str(DEFAULT_DEADMAN))
    parser.add_argument("--guard", default=str(DEFAULT_GUARD))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    packet = build_packet(
        deadman=load_json(Path(args.deadman), default={}),
        guard=load_json(Path(args.guard), default={}),
        evidence_by_name={name: load_json(target["evidence"], default={}) for name, target in TARGETS.items()},
        probe_by_name={name: load_json(target["probe"], default={}) for name, target in TARGETS.items()},
        generated_at=_now(),
    )
    atomic_write_json(Path(args.output), packet)
    print(Path(args.output))


if __name__ == "__main__":
    main()
