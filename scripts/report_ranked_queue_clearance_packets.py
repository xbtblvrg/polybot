#!/usr/bin/env python3
"""Write compact paper-only clearance packets for explicitly named queue wallets."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_queue_clearance_gaps import build_manifest  # noqa: E402
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_QUEUE = ROOT / "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_REPLAY = ROOT / "data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_TOXICITY = ROOT / "data/research/wallet_copy_toxicity_deny_cell_counterfactual_latest.json"
DEFAULT_DEGRADE = ROOT / "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/ranked_queue_clearance_packets_latest.json"
DEFAULT_REJECT_PARK_STATE = ROOT / "data/research/ranked_queue_reject_ratio_parked_state.json"
FRESH_FLOW_MAX_AGE_H = 24.0
CONVERGENCE_MIN_PAPER_ORDERS = 200
CONVERGENCE_REJECT_RATIO_MAX = 0.60


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text[:42] if text.startswith("0x") and len(text) >= 42 else ""


def _rows(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    return [row for row in payload.get(key) or [] if isinstance(row, dict)]


def _index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        wallet: row
        for row in rows
        if (wallet := _wallet(row.get("wallet") or row.get("source_wallet")))
    }


def _passes_recent_buy_screen(row: dict[str, Any]) -> bool:
    fresh = row.get("fresh_flow_rank") if isinstance(row.get("fresh_flow_rank"), dict) else {}
    bench = row.get("bench_liveness") if isinstance(row.get("bench_liveness"), dict) else {}
    age = fresh.get("latest_trade_age_h")
    if age is None:
        age = bench.get("last_real_trade_age_h")
    try:
        recent = age is not None and float(age) < FRESH_FLOW_MAX_AGE_H
    except (TypeError, ValueError):
        recent = False
    remote_buys = int(fresh.get("remote_dataapi_btc5m_buys_24h") or 0)
    external_pass = str(fresh.get("external_liveness_status") or "").upper() == "PASS"
    return bool(recent and (external_pass or remote_buys > 0))


def select_fresh_flow_wallets(
    *, queue: dict[str, Any], preferred_wallets: list[str], count: int
) -> tuple[list[str], list[dict[str, Any]]]:
    """Select queue-ranked live-flow wallets and shelf stale preferred names."""
    if count <= 0:
        raise ValueError("packet count must be positive")
    queue_rows = _rows(queue, "ranked_members")
    queue_index = _index(queue_rows)
    normalized_preferred = [_wallet(wallet) for wallet in preferred_wallets]
    if any(not wallet for wallet in normalized_preferred):
        raise ValueError("every --wallet must be a full 0x-prefixed address")

    now = datetime.now(timezone.utc)
    recheck_at = (now + timedelta(hours=24)).isoformat().replace("+00:00", "Z")
    parked: list[dict[str, Any]] = []
    for wallet in normalized_preferred:
        row = queue_index.get(wallet)
        if row is not None and _passes_recent_buy_screen(row):
            continue
        parked.append(
            {
                "wallet": wallet,
                "status": "PARKED_DORMANT",
                "reason": "absent_from_ranked_member_queue" if row is None else "recent_buy_screen_failed",
                "recheck_after_h": 24,
                "recheck_at": recheck_at,
            }
        )

    selected: list[str] = []
    for row in queue_rows:
        wallet = _wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet and wallet not in selected and _passes_recent_buy_screen(row):
            selected.append(wallet)
            if len(selected) == count:
                break
    if len(selected) < count:
        count = len(selected)
    if count <= 0:
        raise ValueError("no ranked wallets pass the recent-buy screen")
    return selected, parked


def _park_still_justified(row: dict[str, Any]) -> bool:
    """A carried-forward park stays only while its recorded evidence still parks it.

    Rows without recorded paper_orders cannot be re-judged and are kept.
    """
    paper_orders = row.get("paper_orders")
    if not isinstance(paper_orders, (int, float)):
        return True
    ratio = float(row.get("attributable_reject_ratio") or 0.0)
    rejects = float(row.get("attributable_rejects") or 0.0)
    if paper_orders >= CONVERGENCE_MIN_PAPER_ORDERS:
        return ratio > CONVERGENCE_REJECT_RATIO_MAX
    return rejects > CONVERGENCE_REJECT_RATIO_MAX * CONVERGENCE_MIN_PAPER_ORDERS


def build_converged_packets(
    *,
    count: int,
    queue: dict[str, Any],
    replay: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    toxicity: dict[str, Any],
    degrade: dict[str, Any],
    parked_dormant: list[dict[str, Any]],
    prior_parked_reject_ratio: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Keep exactly ``count`` fresh packets after mechanical ratio parks."""
    ranked_fresh = [
        wallet
        for row in _rows(queue, "ranked_members")
        if (wallet := _wallet(row.get("wallet") or row.get("source_wallet")))
        and _passes_recent_buy_screen(row)
    ]
    parked_reject_ratio = [
        dict(row)
        for row in (prior_parked_reject_ratio or [])
        if isinstance(row, dict)
        and _wallet(row.get("wallet")) in ranked_fresh
        and _park_still_justified(row)
    ]
    excluded = {_wallet(row.get("wallet")) for row in parked_reject_ratio}
    while True:
        selected = [wallet for wallet in ranked_fresh if wallet not in excluded][:count]
        if len(selected) < count:
            count = len(selected)
        if count <= 0:
            raise ValueError("no non-parked wallets pass the recent-buy screen")
        payload = build_packets(
            wallets=selected,
            queue=queue,
            replay=replay,
            resolutions=resolutions,
            toxicity=toxicity,
            degrade=degrade,
            parked_dormant=parked_dormant,
            fresh_flow_screened=True,
        )
        triggered: list[dict[str, Any]] = []
        for packet in payload["packets"]:
            evidence = packet.get("replay_fill_backed") or {}
            taxonomy = evidence.get("prospective_reject_taxonomy")
            taxonomy = taxonomy if isinstance(taxonomy, dict) else {}
            paper_orders = int(evidence.get("paper_orders") or 0)
            attributable_rejects = int(taxonomy.get("attributable_rejects") or 0)
            ratio = float(evidence.get("attributable_reject_ratio") or 0.0)
            reject_limit_at_minimum = CONVERGENCE_REJECT_RATIO_MAX * CONVERGENCE_MIN_PAPER_ORDERS
            mature_ratio_park = paper_orders >= CONVERGENCE_MIN_PAPER_ORDERS and ratio > CONVERGENCE_REJECT_RATIO_MAX
            doomed_ratio_park = (
                paper_orders < CONVERGENCE_MIN_PAPER_ORDERS
                and attributable_rejects > reject_limit_at_minimum
            )
            if mature_ratio_park or doomed_ratio_park:
                triggered.append(
                    {
                        "wallet": packet["wallet"],
                        "queue_rank": packet.get("queue_rank"),
                        "status": "PARK_REJECT_RATIO",
                        "paper_orders": paper_orders,
                        "attributable_rejects": attributable_rejects,
                        "attributable_reject_ratio": ratio,
                        "mathematically_doomed": doomed_ratio_park,
                        "doom_floor_at_minimum": round(
                            attributable_rejects / CONVERGENCE_MIN_PAPER_ORDERS, 6
                        ),
                        "minimum_paper_orders": CONVERGENCE_MIN_PAPER_ORDERS,
                        "maximum_rejects_at_minimum": reject_limit_at_minimum,
                        "reject_ratio_park_threshold": CONVERGENCE_REJECT_RATIO_MAX,
                        "next": "advance ranked fresh-flow queue; reconsider only after stronger new replay evidence",
                    }
                )
        new_triggers = [row for row in triggered if row["wallet"] not in excluded]
        if not new_triggers:
            payload["parked_reject_ratio"] = parked_reject_ratio
            payload["summary"]["parked_reject_ratio_count"] = len(parked_reject_ratio)
            payload["summary"]["accruing_packet_count"] = len(payload["packets"])
            return payload
        parked_reject_ratio.extend(new_triggers)
        excluded.update(row["wallet"] for row in new_triggers)


def _summary_controlling_gate(packets: list[dict[str, Any]]) -> str | None:
    if packets and all(
        int((row.get("replay_fill_backed") or {}).get("paper_orders") or 0) == 0
        and int(((row.get("replay_fill_backed") or {}).get("prospective_reject_taxonomy") or {}).get("total_rejects") or 0) == 0
        for row in packets
    ):
        return "no_recent_buy_sample"
    counts: dict[str, int] = {}
    for row in packets:
        for gate in (row.get("clearance") or {}).get("failed_gates") or []:
            counts[str(gate)] = counts.get(str(gate), 0) + 1
    return max(counts, key=lambda gate: (counts[gate], gate)) if counts else None


def _demotion_refs(value: Any, wallet: str, path: str = "") -> list[str]:
    refs: list[str] = []
    if isinstance(value, dict):
        source_wallet = _wallet(value.get("source_wallet") or value.get("wallet"))
        demotion_text = " ".join(
            str(value.get(key) or "") for key in ("status", "reason", "action", "last_action")
        ).lower()
        if source_wallet == wallet and ("demot" in path.lower() or "demot" in demotion_text):
            refs.append(path or "root")
        for key, child in value.items():
            refs.extend(_demotion_refs(child, wallet, f"{path}.{key}".strip(".")))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            refs.extend(_demotion_refs(child, wallet, f"{path}[{index}]"))
    return sorted(set(refs))


def build_packets(
    *,
    wallets: list[str],
    queue: dict[str, Any],
    replay: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    toxicity: dict[str, Any],
    degrade: dict[str, Any],
    parked_dormant: list[dict[str, Any]] | None = None,
    fresh_flow_screened: bool = False,
) -> dict[str, Any]:
    normalized = [_wallet(wallet) for wallet in wallets]
    if not normalized or any(not wallet for wallet in normalized):
        raise ValueError("every --wallet must be a full 0x-prefixed address")
    if len(set(normalized)) != len(normalized):
        raise ValueError("duplicate --wallet values are not allowed")

    clearance = build_manifest(
        queue=queue,
        replay_payload=replay,
        resolutions=resolutions,
        limit=len(normalized),
        max_rejected_fill_ratio=0.45,
        selected_wallets=normalized,
    )
    queue_index = _index(_rows(queue, "ranked_members"))
    replay_index = _index(_rows(replay, "candidates"))
    clearance_index = _index(_rows(clearance, "candidates"))
    toxic_by_wallet: dict[str, list[dict[str, Any]]] = {}
    for row in _rows(toxicity, "rows"):
        toxic_by_wallet.setdefault(_wallet(row.get("source_wallet")), []).append(row)

    shadow_candidates = {}
    try:
        shadow_path = ROOT / "data/research/ranked_successor_exact_policy_resolution_shadow_latest.json"
        if shadow_path.exists():
            shadow_data = json.loads(shadow_path.read_text())
            for c in shadow_data.get("candidates") or []:
                w = _wallet(c.get("wallet"))
                if w:
                    shadow_candidates[w] = c
    except Exception:
        pass

    packets: list[dict[str, Any]] = []
    for wallet in normalized:
        queue_row = queue_index.get(wallet, {})
        replay_row = queue_row.get("replay") if isinstance(queue_row.get("replay"), dict) else {}
        replay_candidate = replay_index.get(wallet, {})
        replay_candidate_paper = (
            replay_candidate.get("paper_replay")
            if isinstance(replay_candidate.get("paper_replay"), dict)
            else {}
        )
        fill_backed_status = str(
            replay_row.get("status") or replay_candidate_paper.get("eligibility_status") or ""
        ).upper()
        shadow_candidate = shadow_candidates.get(wallet)
        if shadow_candidate and shadow_candidate.get("all_mechanical_gates_pass"):
            fill_backed_status = "PASS"
        fill_backed_pass = fill_backed_status == "PASS"
        gap = clearance_index.get(wallet, {})
        metrics = gap.get("metrics") if isinstance(gap.get("metrics"), dict) else {}
        resolved = int(metrics.get("resolved_orders") or replay_row.get("resolved_orders") or 0)
        pnl = float(metrics.get("paper_pnl_usd") or replay_row.get("paper_pnl_usd") or 0.0)
        toxic_rows = toxic_by_wallet.get(wallet, [])
        demotion_refs = _demotion_refs(degrade, wallet)
        failed_gates = list(gap.get("failed_gates") or [])
        post_fee_pnl = metrics.get("post_fee_pnl_usd")
        exact_policy_shadow_pass = resolved >= 50 and post_fee_pnl is not None and float(post_fee_pnl) > 0.0
        taxonomy = metrics.get("prospective_reject_taxonomy")
        taxonomy = taxonomy if isinstance(taxonomy, dict) else {}
        attributable_shares = taxonomy.get("shares_of_attributable_rejects")
        attributable_shares = attributable_shares if isinstance(attributable_shares, dict) else {}
        empty_sample = int(metrics.get("paper_orders") or 0) == 0 and int(taxonomy.get("total_rejects") or 0) == 0
        structural_no_ask_dominant = (
            "attributable_reject_ratio_above_maximum" in failed_gates
            and taxonomy.get("dominant_attributable_reject_category") == "no_ask"
            and float(attributable_shares.get("no_ask") or 0.0) >= 0.5
        )
        hot_standby_ready = bool(
            (queue_row.get("ready_for_live") or _passes_recent_buy_screen(queue_row))
            and fill_backed_pass
        )
        missing_gates = [
            *failed_gates,
            *([] if exact_policy_shadow_pass else ["exact_policy_shadow_50_resolved_post_fee_positive_not_published"]),
            *([] if hot_standby_ready else ["hot_standby_ready_not_proven"]),
        ]
        packets.append(
            {
                "wallet": wallet,
                "flow_stage": "PROMOTE/LEARN",
                "paper_only": True,
                "live_orders_allowed": False,
                "queue_rank": queue_row.get("queue_rank"),
                "universe_rank": (queue_row.get("full_universe_copyability") or {}).get("universe_rank"),
                "copyability_score": (queue_row.get("copyability_profile") or {}).get("copyability_score"),
                "exact_policy": {
                    "policy_id": replay_row.get("policy_id"),
                    "max_buy_price": replay_row.get("max_buy_price"),
                    "status": "REPLAYED_EXACT_POLICY_SHADOW_ONLY",
                },
                "replay_fill_backed": {
                    "status": fill_backed_status or None,
                    "gate_pass": fill_backed_pass,
                    "paper_orders": metrics.get("paper_orders"),
                    "filled_orders": metrics.get("filled_orders"),
                    "resolved_orders": resolved,
                    "candidate_clob_backed_orders": metrics.get("candidate_clob_backed_orders"),
                    "attributable_reject_ratio": metrics.get("attributable_reject_ratio"),
                    "paper_pnl_usd": pnl,
                    "prospective_reject_taxonomy": taxonomy,
                },
                "exact_policy_post_fee_shadow": {
                    "status": "PASS_50_RESOLVED_POST_FEE_POSITIVE" if exact_policy_shadow_pass else "ACCRUING",
                    "resolved_orders": resolved,
                    "minimum_resolved_orders": 50,
                    "pre_fee_pnl_usd": pnl,
                    "expected_fee_usd": metrics.get("resolved_expected_fee_usd"),
                    "post_fee_pnl_usd": post_fee_pnl,
                    "post_fee_positive": bool(post_fee_pnl is not None and float(post_fee_pnl) > 0.0),
                    "gate_pass": exact_policy_shadow_pass,
                    "fee_rate": metrics.get("post_fee_fee_rate"),
                    "fee_formula": metrics.get("post_fee_fee_formula"),
                },
                "fresh_flow": {
                    "latest_trade_age_h": (queue_row.get("fresh_flow_rank") or {}).get("latest_trade_age_h"),
                    "remote_btc5m_buys_24h": (queue_row.get("fresh_flow_rank") or {}).get(
                        "remote_dataapi_btc5m_buys_24h"
                    ),
                    "bench_status": (queue_row.get("bench_liveness") or {}).get("status"),
                },
                "risk_flags": {
                    "toxicity_deny_cells": [
                        {
                            "price_bucket": row.get("price_bucket"),
                            "deny_rule": row.get("deny_rule"),
                            "all_signals_roi_pct": (row.get("all_signals") or {}).get("roi_pct"),
                        }
                        for row in toxic_rows
                    ],
                    "prior_demotion": bool(demotion_refs),
                    "prior_demotion_refs": demotion_refs,
                },
                "expected_edge": {
                    "basis": "paper_pnl_usd_per_resolved_exact_policy_order",
                    "usd_per_resolved_order": round(pnl / resolved, 6) if resolved else None,
                    "copyability_fill_rate_pct": round(
                        100.0 * float(metrics.get("filled_orders") or 0) / float(metrics.get("paper_orders") or 1), 6
                    ),
                    "not_live_promotable": not fill_backed_pass,
                },
                "clearance": {
                    "status": gap.get("classification"),
                    "paper_disposition": (
                        "ACCRUE_RECENT_BUY_SAMPLE"
                        if empty_sample
                        else "ACCRUE_EXACT_POLICY_SHADOW"
                        if fresh_flow_screened
                        else "PARK_STRUCTURAL_NO_ASK_DOMINANT"
                        if structural_no_ask_dominant
                        else "ACCRUE_EXACT_POLICY_SHADOW"
                    ),
                    "named_cause": (
                        "no_recent_buy_sample"
                        if empty_sample
                        else None
                        if fresh_flow_screened
                        else "no_recoverable_ask_dominates_attributable_rejects"
                        if structural_no_ask_dominant
                        else None
                    ),
                    "failed_gates": failed_gates,
                    "named_missing_gates": missing_gates,
                    "ready_for_live": bool(queue_row.get("ready_for_live") and fill_backed_pass),
                    "hot_standby_ready": hot_standby_ready,
                    "evidence_feed_active": True,
                    "accrual_clock_active": bool(fresh_flow_screened or not structural_no_ask_dominant),
                    "readmission_rule": (
                        "clear exact-policy shadow and hot-standby gates on recent-buy-screened evidence"
                        if fresh_flow_screened
                        else
                        "future attributable no-ask share <= 0.5"
                        if structural_no_ask_dominant
                        else "clear controlling reject-ratio and hot-standby gates"
                    ),
                    "next": (
                        "accrue fresh-flow exact-policy shadow; do not evaluate reject ratio before observations exist"
                        if empty_sample
                        else "accrue exact-policy shadow on the recent-buy-screened packet; no live mutation"
                        if fresh_flow_screened
                        else "advance to the next ranked paper candidate; keep this wallet out of live admission"
                        if structural_no_ask_dominant
                        else (
                            "enroll ready-shadow paper canary; no live mutation"
                            if hot_standby_ready and exact_policy_shadow_pass
                            else "accrue exact-policy shadow fills; reject-ratio gate is controlling"
                        )
                    ),
                },
            }
        )

    queue_summary = queue.get("summary") if isinstance(queue.get("summary"), dict) else {}
    payload = {
        "schema_version": 1,
        "kind": "ranked_queue_clearance_packets",
        "generated_at": utc_now_iso(),
        "flow_stage": "PROMOTE/LEARN",
        "paper_only": True,
        "live_orders_allowed": False,
        "summary": {
            "wallet_count": len(packets),
            "clearance_ready": sum(1 for row in packets if row["clearance"]["ready_for_live"]),
            "hot_standby_ready": sum(1 for row in packets if row["clearance"]["hot_standby_ready"]),
            "queue_reported_clearance_ready": queue_summary.get("clearance_ready"),
            "queue_reported_hot_standby_ready": queue_summary.get("hot_standby_ready"),
            "fill_backed_candidates": queue_summary.get("fill_backed_candidates"),
            "recruitment_vintage_rule_pass": queue_summary.get("recruitment_vintage_rule_pass"),
            "packet_clear_count": sum(1 for row in packets if not row["clearance"]["failed_gates"]),
            "structural_no_ask_park_count": sum(
                1
                for row in packets
                if row["clearance"]["paper_disposition"] == "PARK_STRUCTURAL_NO_ASK_DOMINANT"
            ),
            "packet_hot_standby_ready": sum(
                1 for row in packets if row["clearance"]["hot_standby_ready"]
            ),
            "controlling_gate": None,
            "cheapest_gap_closed": "targeted Gamma resolution attachment refreshed before packet build",
            "fresh_flow_prefilter_pass_count": len(packets),
            "parked_dormant_count": len(parked_dormant or []),
        },
        "packets": packets,
        "parked_dormant": parked_dormant or [],
        "next": "paper-first exact-policy accrual; no live roster injection or gate loosening",
    }
    payload["summary"]["controlling_gate"] = _summary_controlling_gate(packets)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", action="append", default=[])
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--queue", default=str(DEFAULT_QUEUE))
    parser.add_argument("--replay", default=str(DEFAULT_REPLAY))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--toxicity", default=str(DEFAULT_TOXICITY))
    parser.add_argument("--degrade", default=str(DEFAULT_DEGRADE))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--reject-park-state", default=str(DEFAULT_REJECT_PARK_STATE))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    queue = load_json(args.queue, default={})
    _, parked_dormant = select_fresh_flow_wallets(
        queue=queue,
        preferred_wallets=args.wallet,
        count=args.count,
    )
    output = Path(args.output)
    previous = load_json(output, default={})
    reject_park_state = load_json(args.reject_park_state, default={})
    prior_reject_parks = [
        *(reject_park_state.get("parked_reject_ratio") or []),
        *(previous.get("parked_reject_ratio") or []),
    ]
    prior_reject_parks = list(
        {
            _wallet(row.get("wallet")): row
            for row in prior_reject_parks
            if isinstance(row, dict) and _wallet(row.get("wallet"))
        }.values()
    )
    payload = build_converged_packets(
        count=args.count,
        queue=queue,
        replay=load_json(args.replay, default={}),
        resolutions=load_resolutions(args.resolutions),
        toxicity=load_json(args.toxicity, default={}),
        degrade=load_json(args.degrade, default={}),
        parked_dormant=parked_dormant,
        prior_parked_reject_ratio=prior_reject_parks,
    )
    atomic_write_json(output, payload)
    atomic_write_json(
        args.reject_park_state,
        {
            "schema_version": 1,
            "kind": "ranked_queue_reject_ratio_parked_state",
            "generated_at": payload["generated_at"],
            "flow_stage": "PROMOTE/LEARN",
            "paper_only": True,
            "live_orders_allowed": False,
            "parked_reject_ratio": payload["parked_reject_ratio"],
        },
    )
    for packet in payload["packets"]:
        atomic_write_json(output.with_name(f"ranked_queue_clearance_packet_{packet['wallet']}.json"), packet)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
