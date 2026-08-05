#!/usr/bin/env python3
"""Accrue a preregistered, no-backfill WIDE positive-slice paper family."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import CopyIntent, num, stable_id, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json

WALLET = "0x4c9497941333332d29f1c235dd23200f3623ffad"
DEFAULT_STATE = "data/research/wide_positive_slice_family_state.json"
DEFAULT_PREREG = "data/research/wide_positive_slice_family_preregistration.json"
FAMILY_LANE = "wide_positive_slice_family"
FAMILY_SOURCE = "WIDE_POSITIVE_SLICE_FAMILY"


def _checksum(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _parse_ts(value: Any) -> float:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _append_jsonl(path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def _positive_slices(standings: dict[str, Any]) -> list[str]:
    return sorted(
        {
            str(row.get("move_slice_key") or "")
            for row in standings.get("slice_failure_matrix") or []
            if isinstance(row, dict)
            and str(row.get("wallet") or "").lower() == WALLET
            and row.get("positive_seed") is True
            and row.get("move_slice_key")
        }
    )


def _capture_wallets(measurement: dict[str, Any]) -> set[str]:
    wallets = measurement.get("wallets")
    if isinstance(wallets, dict):
        return {str(wallet).lower() for wallet in wallets}
    return {
        str(row.get("wallet") if isinstance(row, dict) else row).lower()
        for row in wallets or []
        if row
    }


def _source_lineage(row: dict[str, Any], measurement: dict[str, Any]) -> dict[str, Any]:
    manifest = measurement.get("manifest") if isinstance(measurement.get("manifest"), dict) else {}
    lineage = {
        "manifest_id": manifest.get("manifest_id"),
        "run_id": row.get("run_id"),
        "cohort_id": row.get("cohort_id"),
        "wallet": str(row.get("wallet") or "").lower(),
        "transaction_hash": row.get("transaction_hash"),
        "log_index": row.get("log_index"),
        "order_id": row.get("order_id"),
    }
    return {**lineage, "checksum": _checksum(lineage)}


def _paper_intent_from_order(
    row: dict[str, Any],
    *,
    family_checksum: str,
    evidence_checksum: str,
    lineage: dict[str, Any],
) -> CopyIntent:
    price = num(row.get("fill_price"))
    cost = num(row.get("filled_cost_usd"))
    observed_ts = _parse_ts(row.get("recorded_at"))
    event_ts = num(row.get("source_event_ts"), observed_ts)
    source_event_id = stable_id(
        "widefam_event",
        {
            "transaction_hash": row.get("transaction_hash"),
            "log_index": row.get("log_index"),
            "wallet": row.get("wallet"),
            "order_id": row.get("order_id"),
        },
    )
    return CopyIntent(
        intent_id=stable_id(
            "ci",
            {
                "family_checksum": family_checksum,
                "order_id": row.get("order_id"),
                "lineage_checksum": lineage.get("checksum"),
            },
        ),
        source_wallet=FAMILY_SOURCE,
        wallet_name=FAMILY_LANE,
        source_event_id=source_event_id,
        condition_id=str(row.get("condition_id") or ""),
        market_slug=str(row.get("market_slug") or ""),
        outcome=str(row.get("outcome") or ""),
        side="YES" if str(row.get("outcome") or "").lower() in {"up", "yes"} else "NO",
        limit_price=price,
        wallet_usdc_size=num(row.get("source_shares")) * num(row.get("source_price")),
        copy_size_usd=1.0,
        shares=round(1.0 / price, 6) if price > 0.0 else 0.0,
        observed_ts=observed_ts,
        strategy_family=FAMILY_LANE,
        policy_id=str(row.get("policy_id") or ""),
        sizing_policy_id="fixed_usd_1",
        mode="paper",
        action="BUY",
        order_type="PAPER_SOURCE_FILL",
        token_id=str(row.get("token_id") or ""),
        event_ts=event_ts,
        api_latency_s=num(row.get("receipt_to_book_fetch_lag_s")),
        live_orders_allowed=False,
        reason="gate-qualified WIDE positive-slice paper intent",
        metadata={
            "wide_positive_slice_family": {
                "family_checksum": family_checksum,
                "evidence_checksum": evidence_checksum,
                "source_order_id": row.get("order_id"),
                "source_lineage": lineage,
            },
            "source_book": {
                "book_hash": row.get("book_hash"),
                "book_timestamp": row.get("book_timestamp"),
                "executable_depth_pass": (row.get("f1_f4_terminal") or {}).get(
                    "F4_executable_book"
                )
                == "PASS",
                "expected_fee_usd": row.get("expected_fee_usd"),
                "post_fee_pnl_usd": row.get("post_fee_pnl_usd"),
            },
        },
    )


def _new_prereg(
    measurement: dict[str, Any],
    standings: dict[str, Any],
) -> dict[str, Any]:
    reconciliation = standings.get("manifest_reconciliation") or {}
    cohort = measurement.get("cohort") or {}
    slices = _positive_slices(standings)
    if not slices:
        raise RuntimeError("no positive immutable-manifest slice seed")
    if reconciliation.get("manifest_identity_exact") is not True:
        raise RuntimeError("WIDE standings are not reconciled to the immutable manifest")
    baseline_ids = sorted(
        str(row.get("order_id") or "")
        for row in measurement.get("orders") or []
        if isinstance(row, dict) and row.get("order_id")
    )
    registered_at = utc_now_iso()
    body = {
        "schema_version": 1,
        "kind": "wide_positive_slice_family_preregistration",
        "family_id": "wide_4c94_positive_slices_v1",
        "registered_at": registered_at,
        "registered_before_prospective_outcomes": True,
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_authority": False,
        "wallet": WALLET,
        "move_slice_keys": slices,
        "entry_price_band": "0.25-0.50",
        "source_manifest_id": reconciliation.get("manifest_id"),
        "source_run_id": cohort.get("run_id"),
        "source_cohort_id": cohort.get("cohort_id"),
        "source_policy_id": measurement.get("policy_id"),
        "baseline_order_count": len(baseline_ids),
        "baseline_order_ids_sha256": _checksum(baseline_ids),
        "baseline_order_ids": baseline_ids,
        "prospective_rule": "order_id_not_in_preregistered_baseline_and_recorded_at_after_registered_at",
        "minimum_resolved_orders": 50,
        "permanent_gates": [
            "immutable_source_identity",
            "current_alpha",
            "prospective_resolved_gte_50",
            "post_fee_positive",
            "chronological_halves_positive",
            "fees_measured",
            "receipt_lag_lte_5s",
            "executable_depth",
            "zero_parity_lookahead_disagreement",
        ],
    }
    return {**body, "checksum": _checksum(body)}


def _load_or_create_prereg(
    path: str,
    measurement: dict[str, Any],
    standings: dict[str, Any],
) -> dict[str, Any]:
    prior = load_json(path, default={})
    if prior:
        canonical = {key: value for key, value in prior.items() if key != "checksum"}
        if prior.get("checksum") != _checksum(canonical):
            raise RuntimeError("immutable positive-slice preregistration checksum mismatch")
        return prior
    prereg = _new_prereg(measurement, standings)
    atomic_write_json(path, prereg)
    return prereg


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    measurement = load_json(args.measurement, default={})
    standings = load_json(args.standings, default={})
    prereg = _load_or_create_prereg(args.preregistration, measurement, standings)
    baseline = set(prereg.get("baseline_order_ids") or [])
    allowed_slices = set(prereg.get("move_slice_keys") or [])
    registered_ts = _parse_ts(prereg.get("registered_at"))
    candidates: list[dict[str, Any]] = []
    duplicate_counts: dict[str, int] = {}
    for row in measurement.get("orders") or []:
        if not isinstance(row, dict):
            continue
        order_id = str(row.get("order_id") or "")
        move_slice = row.get("alpha_move_slice") or {}
        if (
            str(row.get("wallet") or "").lower() != WALLET
            or str(move_slice.get("move_slice_key") or "") not in allowed_slices
            or not order_id
            or order_id in baseline
            or _parse_ts(row.get("recorded_at")) <= registered_ts
        ):
            continue
        duplicate_counts[order_id] = duplicate_counts.get(order_id, 0) + 1
        if duplicate_counts[order_id] == 1:
            candidates.append(row)
    prior = load_json(args.state, default={})
    accrued_by_id = {
        str(row.get("order_id") or ""): row
        for row in prior.get("prospective_order_records") or []
        if isinstance(row, dict) and row.get("order_id")
    }
    capture_wallets = _capture_wallets(measurement)
    for row in candidates:
        enriched = dict(row)
        enriched["source_lineage"] = _source_lineage(row, measurement)
        accrued_by_id[str(row.get("order_id"))] = enriched
    candidates = sorted(
        accrued_by_id.values(),
        key=lambda row: (str(row.get("recorded_at") or ""), str(row.get("order_id") or "")),
    )
    known = set(prior.get("observed_order_ids") or [])
    new_rows = [row for row in candidates if row.get("order_id") not in known]
    now = utc_now_iso()
    _append_jsonl(
        args.events,
        [
            {
                "recorded_at": now,
                "family_checksum": prereg["checksum"],
                "order_id": row.get("order_id"),
                "source_event": {
                    "transaction_hash": row.get("transaction_hash"),
                    "log_index": row.get("log_index"),
                    "wallet": row.get("wallet"),
                    "move_slice_key": (row.get("alpha_move_slice") or {}).get("move_slice_key"),
                },
            }
            for row in new_rows
        ],
    )
    _append_jsonl(
        args.intents,
        [
            {
                "recorded_at": now,
                "family_checksum": prereg["checksum"],
                "order_id": row.get("order_id"),
                "intent": "COPY_EXACT_POLICY_PAPER",
                "policy_id": row.get("policy_id"),
                "filled_cost_usd": row.get("filled_cost_usd"),
            }
            for row in new_rows
        ],
    )
    prior_terminal_ids = set(prior.get("terminal_order_ids") or [])
    new_terminals = [
        row
        for row in candidates
        if row.get("resolved") is True and row.get("order_id") not in prior_terminal_ids
    ]
    _append_jsonl(
        args.terminals,
        [
            {
                "recorded_at": now,
                "family_checksum": prereg["checksum"],
                "order_id": row.get("order_id"),
                "terminal": "RESOLVED_EXACT_POLICY_PAPER",
                "post_fee_pnl_usd": row.get("post_fee_pnl_usd"),
            }
            for row in new_terminals
        ],
    )
    resolved = [row for row in candidates if row.get("resolved") is True]
    midpoint = len(resolved) // 2
    first, second = resolved[:midpoint], resolved[midpoint:]
    pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in resolved), 6)
    first_pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in first), 6)
    second_pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in second), 6)
    lags = [num(row.get("receipt_to_book_fetch_lag_s"), 999.0) for row in candidates]
    reconciliation = standings.get("manifest_reconciliation") or {}
    standing = next(
        (
            row
            for row in standings.get("standings") or []
            if isinstance(row, dict) and str(row.get("wallet") or "").lower() == WALLET
        ),
        {},
    )
    alpha_slices = {
        str(row.get("move_slice_key") or ""): row
        for row in standing.get("alpha_move_slices") or []
        if isinstance(row, dict)
    }
    lineage_defects = sum(
        not isinstance(row.get("source_lineage"), dict)
        or row["source_lineage"].get("checksum")
        != _checksum(
            {
                key: row["source_lineage"].get(key)
                for key in (
                    "manifest_id",
                    "run_id",
                    "cohort_id",
                    "wallet",
                    "transaction_hash",
                    "log_index",
                    "order_id",
                )
            }
        )
        or str(row.get("wallet") or "").lower() != WALLET
        for row in candidates
    )
    current_manifest = (
        measurement.get("manifest") if isinstance(measurement.get("manifest"), dict) else {}
    )
    source_identity = bool(
        reconciliation.get("manifest_identity_exact") is True
        and reconciliation.get("manifest_id") == current_manifest.get("manifest_id")
        and measurement.get("policy_id") == prereg.get("source_policy_id")
        and str(standing.get("wallet") or "").lower() == WALLET
        and WALLET in capture_wallets
        and lineage_defects == 0
    )
    parity_defects = sum(
        (row.get("f1_f4_terminal") or {}).get("terminal")
        != "COPYABLE_EXACT_POLICY_PAPER_FILL"
        for row in candidates
    )
    lookahead_defects = sum(_parse_ts(row.get("recorded_at")) <= registered_ts for row in candidates)
    disagreement_defects = sum(count > 1 for count in duplicate_counts.values())
    gates = {
        "immutable_source_identity": source_identity,
        "current_alpha": all(
            alpha_slices.get(key, {}).get("eligible") is True
            for key in allowed_slices
        ),
        "prospective_resolved_gte_50": len(resolved) >= 50,
        "post_fee_positive": bool(resolved) and pnl > 0.0,
        "chronological_halves_positive": bool(first)
        and bool(second)
        and first_pnl > 0.0
        and second_pnl > 0.0,
        "fees_measured": bool(resolved)
        and all(row.get("expected_fee_usd") is not None for row in resolved),
        "receipt_lag_lte_5s": bool(candidates) and max(lags, default=999.0) <= 5.0,
        "executable_depth": bool(candidates)
        and all(
            (row.get("f1_f4_terminal") or {}).get("F4_executable_book") == "PASS"
            and num(row.get("filled_cost_usd")) > 0.0
            for row in candidates
        ),
        "zero_parity_lookahead_disagreement": (
            parity_defects == 0 and lookahead_defects == 0 and disagreement_defects == 0
        ),
    }
    evidence_snapshot = {
        "family_checksum": prereg["checksum"],
        "gates": gates,
        "resolved_order_ids": [row.get("order_id") for row in resolved],
        "post_fee_pnl_usd": pnl,
        "first_half_post_fee_pnl_usd": first_pnl,
        "second_half_post_fee_pnl_usd": second_pnl,
        "lineage_checksums": [
            (row.get("source_lineage") or {}).get("checksum") for row in candidates
        ],
    }
    evidence_checksum = _checksum(evidence_snapshot)
    newest = candidates[-1] if candidates else {}
    activation: dict[str, Any] = {
        "status": "EVIDENCE_GATE_CLOSED",
        "live_mutation_allowed": False,
        "family_checksum": prereg["checksum"],
        "evidence_snapshot": evidence_snapshot,
        "evidence_checksum": evidence_checksum,
    }
    if all(gates.values()) and newest:
        lineage = newest.get("source_lineage") or {}
        paper_intent = _paper_intent_from_order(
            newest,
            family_checksum=prereg["checksum"],
            evidence_checksum=evidence_checksum,
            lineage=lineage,
        ).asdict()
        packet = {
            "status": "ACTIVATION_READY",
            "live_mutation_allowed": True,
            "family_checksum": prereg["checksum"],
            "evidence_snapshot": evidence_snapshot,
            "evidence_checksum": evidence_checksum,
            "source_order_checksum": _checksum(newest),
            "source_order": newest,
            "source_lineage": lineage,
            "paper_intent": paper_intent,
            "paper_intent_checksum": _checksum(paper_intent),
        }
        activation = {**packet, "activation_checksum": _checksum(packet)}
    all_orders = [
        row for row in measurement.get("orders") or [] if isinstance(row, dict)
    ]
    wallet_rows = [
        row for row in all_orders if str(row.get("wallet") or "").lower() == WALLET
    ]
    slice_rows = [
        row
        for row in wallet_rows
        if str((row.get("alpha_move_slice") or {}).get("move_slice_key") or "")
        in allowed_slices
    ]
    post_registration_rows = [
        row for row in slice_rows if _parse_ts(row.get("recorded_at")) > registered_ts
    ]
    attrition = {
        "watched_buy": len(all_orders),
        "wallet_match": len(wallet_rows),
        "frozen_slice_match": len(slice_rows),
        "post_registration": len(post_registration_rows),
        "policy_copyable": sum(
            (row.get("f1_f4_terminal") or {}).get("terminal")
            == "COPYABLE_EXACT_POLICY_PAPER_FILL"
            for row in post_registration_rows
        ),
        "resolved": sum(row.get("resolved") is True for row in post_registration_rows),
    }
    state = {
        "schema_version": 1,
        "kind": "wide_positive_slice_family_state",
        "flow_stage": "LEARN/OBSERVE/PROMOTE",
        "generated_at": now,
        "status": "PROMOTION_GATE_COMPLETE" if all(gates.values()) else "PROSPECTIVE_ACCRUAL",
        "paper_only": True,
        "live_orders_allowed": False,
        "single_live_submitter": "scripts/run_wallet_copy_live_guard.py",
        "preregistration_path": args.preregistration,
        "family_checksum": prereg["checksum"],
        "wallet": WALLET,
        "move_slice_keys": sorted(allowed_slices),
        "summary": {
            "prospective_orders": len(candidates),
            "prospective_resolved_orders": len(resolved),
            "post_fee_pnl_usd": pnl,
            "first_half_post_fee_pnl_usd": first_pnl,
            "second_half_post_fee_pnl_usd": second_pnl,
            "max_receipt_lag_s": round(max(lags), 6) if lags else None,
            "new_orders_this_cycle": len(new_rows),
            "new_terminals_this_cycle": len(new_terminals),
        },
        "defect_counts": {
            "parity": parity_defects,
            "lookahead": lookahead_defects,
            "disagreement": disagreement_defects,
            "lineage": lineage_defects,
        },
        "gates": gates,
        "admission_ready": all(gates.values()),
        "observed_order_ids": [row.get("order_id") for row in candidates],
        "terminal_order_ids": [row.get("order_id") for row in resolved],
        "prospective_order_records": candidates,
        "attrition_funnel": attrition,
        "activation": activation,
    }
    atomic_write_json(args.state, state)
    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurement", default="data/research/wide_exact_policy_paper_state.json")
    parser.add_argument("--standings", default="data/research/wide_candidate_standings_latest.json")
    parser.add_argument("--preregistration", default=DEFAULT_PREREG)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--events", default="data/research/wide_positive_slice_family_events.jsonl")
    parser.add_argument("--intents", default="data/research/wide_positive_slice_family_intents.jsonl")
    parser.add_argument("--terminals", default="data/research/wide_positive_slice_family_terminals.jsonl")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-s", type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    while True:
        state = run_once(args)
        print(json.dumps({"state": args.state, "status": state["status"], **state["summary"]}, sort_keys=True), flush=True)
        if not args.watch:
            return 0
        time.sleep(max(0.25, args.interval_s))


if __name__ == "__main__":
    raise SystemExit(main())
