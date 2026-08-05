#!/usr/bin/env python3
"""Run an isolated prospective 30-second WIDE sequential-quorum paper cell."""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_wide_multiwallet_consensus_slice import (
    DEFAULT_MEASUREMENT,
    DEFAULT_RESOLUTIONS,
    _append_jsonl,
    _build_cell,
    _checksum,
    _jsonl,
    _manifest_rows,
    _parse_ts,
    _receipt_ts,
    _resolution_index,
    _source_identity,
)
from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json
from scripts.wide_direct_handoff_journal import DEFAULT_JOURNAL, load_jsonl, row_identity

CELL_ID = "sequential_quorum_30s"
MAX_GAP_S = 30.0
DEFAULT_PREREG = "data/research/wide_sequential_quorum_preregistration.json"
DEFAULT_STATE = "data/research/wide_sequential_quorum_state.json"
DEFAULT_CELL_PREFIX = "data/research/wide_sequential_quorum_30s"


def _new_prereg(
    measurement: dict[str, Any], manifest_path: str, standings_path: str = ""
) -> dict[str, Any]:
    manifest = measurement.get("manifest") if isinstance(measurement.get("manifest"), dict) else {}
    cohort = measurement.get("cohort") if isinstance(measurement.get("cohort"), dict) else {}
    frozen_wallets = []
    for row in _manifest_rows(measurement, manifest_path, standings_path):
        wallet = str(row.get("wallet") or "").lower()
        if not wallet:
            continue
        rank = max(1, int(row.get("queue_rank") or len(frozen_wallets) + 1))
        frozen_wallets.append(
            {
                "wallet": wallet,
                "queue_rank": rank,
                "score_weight": round(1.0 / math.sqrt(rank), 12),
            }
        )
    frozen_wallets.sort(key=lambda row: (row["queue_rank"], row["wallet"]))
    if manifest_path and len(frozen_wallets) != 31:
        raise RuntimeError(
            f"immutable WIDE manifest must contain 31 wallets, got {len(frozen_wallets)}"
        )
    baseline = sorted(
        str(row.get("order_id") or "")
        for row in measurement.get("orders") or []
        if isinstance(row, dict) and row.get("order_id")
    )
    common = {
        "schema_version": 1,
        "kind": "wide_sequential_quorum_preregistration",
        "registered_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_authority": False,
        "source_manifest_id": manifest.get("manifest_id"),
        "source_manifest_path": manifest_path,
        "source_run_id": cohort.get("run_id"),
        "source_cohort_id": cohort.get("cohort_id"),
        "source_policy_id": measurement.get("policy_id"),
        "capture_wallet_count": len(frozen_wallets),
        "capture_wallets": frozen_wallets,
        "capture_wallets_checksum": _checksum(frozen_wallets),
        "maximum_sequential_gap_s": MAX_GAP_S,
        "sequential_rule": (
            "same condition/outcome; two distinct frozen wallets; second receipt "
            "strictly after first and <=30s; one vote per wallet; one intent per window"
        ),
        "entry_price_min": 0.25,
        "entry_price_max": 0.50,
        "copy_size_usd": 1.0,
        "max_receipt_lag_s": 5.0,
        "book_rule": "second_qualifying_receipt_contemporaneous_executable_book",
        "fee_rule": "POLYMARKET_EMBEDDED_FEE_FORMULA",
        "baseline_order_ids": baseline,
        "baseline_order_ids_checksum": _checksum(baseline),
        "prospective_rule": "not_in_baseline_and_recorded_at_after_registration",
        "permanent_live_gate": {
            "minimum_resolved": 50,
            "positive_aggregate": True,
            "positive_chronological_halves": True,
            "positive_incremental_component_pnl": True,
            "current_alpha": True,
            "fees_and_depth_measured": True,
            "max_receipt_lag_s": 5.0,
            "zero_parity_lookahead_disagreement": True,
            "live_authority": "sole live guard after separate Fable ruling",
        },
    }
    common_checksum = _checksum(common)
    cell_rule = {
        "minimum_distinct_wallets": 2,
        "maximum_sequential_gap_s": MAX_GAP_S,
        "weight_formula": "1/sqrt(frozen_queue_rank)",
    }
    cell = {
        **cell_rule,
        "cell_checksum": _checksum(
            {
                "cell_id": CELL_ID,
                "common_envelope_checksum": common_checksum,
                "rule": cell_rule,
            }
        ),
    }
    body = {**common, "cells": {CELL_ID: cell}}
    return {**body, "checksum": _checksum(body)}


def _load_or_create_prereg(
    path: str,
    measurement: dict[str, Any],
    manifest_path: str,
    standings_path: str,
) -> dict[str, Any]:
    prereg = load_json(path, default={})
    if prereg:
        expected = prereg.get("checksum")
        body = {key: value for key, value in prereg.items() if key != "checksum"}
        if not expected or expected != _checksum(body):
            raise RuntimeError("immutable sequential-quorum preregistration checksum mismatch")
        if prereg.get("baseline_order_ids_checksum") != _checksum(
            prereg.get("baseline_order_ids") or []
        ):
            raise RuntimeError("immutable sequential-quorum baseline checksum mismatch")
        if prereg.get("capture_wallets_checksum") != _checksum(
            prereg.get("capture_wallets") or []
        ):
            raise RuntimeError("immutable sequential-quorum wallet checksum mismatch")
        return prereg
    prereg = _new_prereg(measurement, manifest_path, standings_path)
    atomic_write_json(path, prereg)
    return prereg


def _sequential_groups(
    sources: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], Counter[str]]:
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    refusals: Counter[str] = Counter()
    for row in sources:
        receipt = _receipt_ts(row)
        if receipt <= 0:
            refusals["missing_receipt_time"] += 1
            continue
        buckets[
            (
                str(row.get("condition_id") or "").lower(),
                str(row.get("outcome") or "").upper(),
            )
        ].append(row)

    groups: list[dict[str, Any]] = []
    for (condition_id, outcome), rows in sorted(buckets.items()):
        ordered = sorted(rows, key=lambda row: (_receipt_ts(row), _source_identity(row)))
        pair: list[dict[str, Any]] | None = None
        for second_index, second in enumerate(ordered):
            second_wallet = str(second.get("wallet") or "").lower()
            second_ts = _receipt_ts(second)
            eligible_first = [
                first
                for first in ordered[:second_index]
                if str(first.get("wallet") or "").lower() != second_wallet
                and 0.0 < second_ts - _receipt_ts(first) <= MAX_GAP_S
            ]
            if eligible_first:
                first = max(
                    eligible_first,
                    key=lambda row: (_receipt_ts(row), _source_identity(row)),
                )
                pair = [first, second]
                break
        if pair is None:
            refusals["no_second_wallet_within_30s"] += 1
            continue
        groups.append(
            {
                "condition_id": condition_id,
                "outcome": outcome,
                "interval_start_s": _receipt_ts(pair[0]),
                "interval_end_s": _receipt_ts(pair[1]),
                "components": pair,
            }
        )
    return groups, refusals


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    measurement = load_json(args.measurement, default={})
    prereg = _load_or_create_prereg(
        args.preregistration,
        measurement,
        str(args.manifest),
        str(args.standings),
    )
    registered_ts = _parse_ts(prereg.get("registered_at"))
    baseline = set(prereg.get("baseline_order_ids") or [])
    allowed_wallets = {row["wallet"] for row in prereg["capture_wallets"]}
    prior = load_json(args.state, default={})
    sources_by_id = {
        _source_identity(row): row
        for row in prior.get("source_records") or []
        if isinstance(row, dict)
    }
    refusals: Counter[str] = Counter()
    terminal_funnel: Counter[str] = Counter()
    journal_path = Path(getattr(args, "direct_journal", DEFAULT_JOURNAL))
    source_run_id = str(prereg.get("source_run_id") or "")
    seen_terminal_rows: set[str] = set()
    for envelope in load_jsonl(journal_path):
        identity = envelope.get("identity") if isinstance(envelope.get("identity"), dict) else {}
        if str(identity.get("run_id") or "") != source_run_id:
            continue
        if envelope.get("input_equals_terminal") is not True:
            continue
        generation = str(envelope.get("source_generation") or source_run_id)
        for row in envelope.get("rows") or []:
            if not isinstance(row, dict):
                continue
            row_id = row_identity(row, generation)
            wallet = str(row.get("wallet") or row.get("source_wallet") or "").lower()
            recorded_at = _parse_ts(row.get("recorded_at"))
            if row_id in seen_terminal_rows or wallet not in allowed_wallets or recorded_at <= registered_ts:
                continue
            seen_terminal_rows.add(row_id)
            terminal_funnel["attempt"] += 1
            terminal = row.get("f1_f4_terminal") if isinstance(row.get("f1_f4_terminal"), dict) else {}
            terminal_name = str(terminal.get("terminal") or "MISSING_TERMINAL")
            if terminal_name == "REFUSED_ALPHA_PROFILE_FILTER":
                refusals["alpha_profile_filter"] += 1
                continue
            terminal_funnel["alpha_profile"] += 1
            if terminal_name == "REFUSED_METADATA_MISSING":
                refusals["metadata_missing"] += 1
                continue
            terminal_funnel["metadata"] += 1
            if terminal_name != "COPYABLE_EXACT_POLICY_PAPER_FILL":
                refusals[terminal_name.lower()] += 1
                continue
            terminal_funnel["copyable_depth_input"] += 1
            if not 0.25 <= num(row.get("fill_price")) <= 0.50:
                refusals["price_outside_frozen_band"] += 1
                continue
            if num(row.get("receipt_to_book_fetch_lag_s"), 999.0) > 5.0:
                refusals["receipt_lag_above_5s"] += 1
                continue
            if terminal.get("F4_executable_book") != "PASS":
                refusals["contemporaneous_book_or_depth_missing"] += 1
                continue
            sources_by_id[_source_identity(row)] = dict(row)
    for row in measurement.get("orders") or []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").lower()
        order_id = str(row.get("order_id") or "")
        if wallet not in allowed_wallets:
            refusals["wallet_outside_frozen_manifest"] += 1
            continue
        if not order_id or order_id in baseline or _parse_ts(row.get("recorded_at")) <= registered_ts:
            refusals["baseline_or_pre_registration"] += 1
            continue
        if not 0.25 <= num(row.get("fill_price")) <= 0.50:
            refusals["price_outside_frozen_band"] += 1
            continue
        if num(row.get("receipt_to_book_fetch_lag_s"), 999.0) > 5.0:
            refusals["receipt_lag_above_5s"] += 1
            continue
        if (row.get("f1_f4_terminal") or {}).get("F4_executable_book") != "PASS":
            refusals["contemporaneous_book_or_depth_missing"] += 1
            continue
        sources_by_id[_source_identity(row)] = dict(row)
    sources = sorted(
        sources_by_id.values(),
        key=lambda row: (_receipt_ts(row), _source_identity(row)),
    )
    groups, sequential_refusals = _sequential_groups(sources)
    refusals.update(sequential_refusals)
    paths = {
        "state": args.cell_state,
        "events": args.events,
        "intents": args.intents,
        "terminals": args.terminals,
    }
    cell = _build_cell(
        cell_id=CELL_ID,
        prereg=prereg,
        groups=groups,
        prior=load_json(args.cell_state, default={}),
        resolutions=_resolution_index(_jsonl(args.resolutions)),
        raw_count=len(sources),
        base_refusals=refusals,
        paths=paths,
    )
    state = {
        "schema_version": 1,
        "kind": "wide_sequential_quorum_supervisor_state",
        "flow_stage": "DISCOVER/OBSERVE/LEARN/PROMOTE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "status": "RUNNING_PAPER_ONLY",
        "paper_only": True,
        "live_orders_allowed": False,
        "productive_lane_count": 1,
        "preregistration_checksum": prereg["checksum"],
        "cell_checksum": prereg["cells"][CELL_ID]["cell_checksum"],
        "source_records": sources,
        "attrition_funnel": {
            **cell["attrition_funnel"],
            "attempt": int(terminal_funnel["attempt"]),
            "alpha_profile": int(terminal_funnel["alpha_profile"]),
            "metadata": int(terminal_funnel["metadata"]),
            "copyable_depth_input": int(terminal_funnel["copyable_depth_input"]),
            "distinct_wallet_30s_group": len(groups),
            "intent": int(cell["summary"].get("prospective_intents") or 0),
            "resolved": int(cell["summary"].get("resolved") or 0),
            "no_second_wallet_within_30s": int(
                cell["refusal_taxonomy"].get("no_second_wallet_within_30s") or 0
            ),
        },
        "cell": cell,
    }
    atomic_write_json(args.state, state)
    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurement", default=DEFAULT_MEASUREMENT)
    parser.add_argument("--direct-journal", default=DEFAULT_JOURNAL)
    parser.add_argument("--standings", default="data/research/wide_candidate_standings_latest.json")
    parser.add_argument("--manifest", default="")
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--preregistration", default=DEFAULT_PREREG)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--cell-state", default=f"{DEFAULT_CELL_PREFIX}_cell_state.json")
    parser.add_argument("--events", default=f"{DEFAULT_CELL_PREFIX}_events.jsonl")
    parser.add_argument("--intents", default=f"{DEFAULT_CELL_PREFIX}_intents.jsonl")
    parser.add_argument("--terminals", default=f"{DEFAULT_CELL_PREFIX}_terminals.jsonl")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-s", type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    while True:
        state = run_once(args)
        print(
            json.dumps(
                {
                    "state": args.state,
                    "status": state["status"],
                    "cell_checksum": state["cell_checksum"],
                    "summary": state["cell"]["summary"],
                    "refusal_taxonomy": state["cell"]["refusal_taxonomy"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if not args.watch:
            return 0
        time.sleep(max(0.25, args.interval_s))


if __name__ == "__main__":
    raise SystemExit(main())
