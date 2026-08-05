#!/usr/bin/env python3
"""Score wider same-family WIDE fingerprints as paper-only candidates."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_wide_policy_fingerprint_evidence import (  # noqa: E402
    DEFAULT_MANIFEST_GLOB,
    _jsonl,
    _source_identity,
    _summarize,
    _venue_executable_summary,
    load_manifest_index,
)
from scripts.reconcile_wide_exact_policy_paper import (  # noqa: E402
    DEFAULT_LEDGER,
    DEFAULT_RESOLUTIONS,
    apply_resolutions,
    replay_ledger,
    wide_policy_identity,
)
from src.wallet_copy.models import num  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_RECONCILIATION = (
    "data/research/f1_rescore_registry_reconciliation_latest.json"
)
DEFAULT_OUTPUT = "data/research/widened_fingerprint_paper_candidates_latest.json"
F1_RESOLVED_FLOOR = 200
CONCENTRATION_THRESHOLD_PCT = 50.0


def _f1_conjuncts(summary: dict[str, Any]) -> dict[str, bool]:
    return {
        "resolved_gte_200": int(summary.get("resolved") or 0)
        >= F1_RESOLVED_FLOOR,
        "post_fee_pnl_positive": num(summary.get("post_fee_pnl_usd")) > 0,
        "roi_positive": num(summary.get("roi_pct")) > 0,
        "both_resolved_halves_positive": (
            num(summary.get("first_half_post_fee_pnl_usd")) > 0
            and num(summary.get("second_half_post_fee_pnl_usd")) > 0
        ),
        "venue_reachable_admissible": (
            summary.get("f1_venue_reachable_admissible") is True
        ),
        "concentration_admissible": (
            summary.get("concentration_admissible") is True
        ),
    }


def _venue_disclosure(summary: dict[str, Any]) -> dict[str, Any]:
    conjuncts = _f1_conjuncts(summary)
    top_1_share = summary.get("top_1_market_share_pct")
    return {
        "resolved_signals": summary.get("resolved"),
        "post_fee_pnl_usd": summary.get("post_fee_pnl_usd"),
        "roi_pct": summary.get("roi_pct"),
        "first_half_post_fee_pnl_usd": summary.get(
            "first_half_post_fee_pnl_usd"
        ),
        "second_half_post_fee_pnl_usd": summary.get(
            "second_half_post_fee_pnl_usd"
        ),
        "concentration_admissible": summary.get("concentration_admissible"),
        "top_1_market_share_pct": top_1_share,
        "pnl_excluding_top_1_market": summary.get(
            "pnl_excluding_top_1_market"
        ),
        "concentration_threshold_pct": CONCENTRATION_THRESHOLD_PCT,
        "concentration_basis": (
            "measured" if top_1_share is not None else "unmeasured_null_share"
        ),
        "venue_reachable_share_pct": summary.get(
            "venue_reachable_share_pct"
        ),
        "f1_venue_reachable_admissible": summary.get(
            "f1_venue_reachable_admissible"
        ),
        "f1_conjuncts": conjuncts,
        "blocking_conjuncts": [
            name for name, passed in conjuncts.items() if not passed
        ],
    }


def _family_exhaustion_verdict(rows: list[dict[str, Any]]) -> str:
    expanded = [
        row for row in rows if row["additional_move_slice_key_count"] > 0
    ]
    if not expanded:
        return "NO_EXPANSION_EVIDENCE"
    for row in expanded:
        delta = row["baseline_to_widened"]
        if delta["baseline_all_f1_conjuncts_pass"] and (
            not delta["widened_all_f1_conjuncts_pass"]
            or num(delta["roi_pct_delta"]) < 0
            or num(delta["venue_reachable_share_pct_delta"]) < 0
        ):
            return "NEGATIVE"
    return "NON_NEGATIVE"


def build_packet(
    *,
    ledger_rows: list[dict[str, Any]],
    resolution_rows: list[dict[str, Any]],
    manifests: list[Path],
    reconciliation: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    orders, _event_ids = replay_ledger(ledger_rows)
    orders, _events = apply_resolutions(orders, resolution_rows)
    by_run, _manifest_wallets = load_manifest_index(manifests)
    target_rows = [
        row for row in reconciliation.get("rows") or [] if isinstance(row, dict)
    ]
    target_wallets = {str(row.get("wallet") or "").lower() for row in target_rows}
    current_keys: dict[str, set[str]] = defaultdict(set)
    for row in target_rows:
        wallet = str(row.get("wallet") or "").lower()
        current_keys[wallet].update(str(key) for key in row.get("move_slice_keys") or [])

    full_stream: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    family_keys: dict[str, set[str]] = defaultdict(set)
    for run in by_run.values():
        for wallet, identity in (run.get("wallet_policy_identities") or {}).items():
            if wallet in target_wallets:
                family_keys[wallet].update(
                    str(key) for key in identity.get("move_slice_keys") or []
                )
    for order in orders:
        wallet = str(order.get("wallet") or "").lower()
        if wallet not in target_wallets:
            continue
        run = by_run.get(str(order.get("run_id") or ""), {})
        identity = (run.get("wallet_policy_identities") or {}).get(wallet)
        if not identity:
            continue
        family_keys[wallet].update(
            str(key) for key in identity.get("move_slice_keys") or []
        )
        source_id = _source_identity(order)
        incumbent = full_stream[wallet].get(source_id)
        if incumbent is None or (
            order.get("resolved") is True and incumbent.get("resolved") is not True
        ):
            full_stream[wallet][source_id] = order

    rows: list[dict[str, Any]] = []
    for wallet in sorted(target_wallets):
        widened_keys = sorted(family_keys.get(wallet) or current_keys.get(wallet) or [])
        identity = wide_policy_identity(
            wallet=wallet,
            move_slice_keys=widened_keys,
        )
        selected = [
            order
            for order in full_stream.get(wallet, {}).values()
            if str(
                (order.get("alpha_move_slice") or {}).get("move_slice_key") or ""
            )
            in set(widened_keys)
        ]
        full = _summarize(selected)
        venue = _venue_executable_summary(selected, min_order_usd=1.0)
        baseline_selected = [
            order
            for order in full_stream.get(wallet, {}).values()
            if str(
                (order.get("alpha_move_slice") or {}).get("move_slice_key") or ""
            )
            in current_keys.get(wallet, set())
        ]
        baseline_venue = _venue_executable_summary(
            baseline_selected,
            min_order_usd=1.0,
        )
        baseline_conjuncts = _f1_conjuncts(baseline_venue)
        widened_conjuncts = _f1_conjuncts(venue)
        rows.append(
            {
                "wallet": wallet,
                "selection_rule_id": identity["selection_rule_id"],
                "paper_policy_id": identity["policy_id"],
                "wide_policy_fingerprint": identity["wide_policy_fingerprint"],
                "current_move_slice_keys": sorted(current_keys.get(wallet) or []),
                "current_move_slice_key_count": len(current_keys.get(wallet) or []),
                "widened_move_slice_keys": widened_keys,
                "widened_move_slice_key_count": len(widened_keys),
                "additional_move_slice_key_count": len(
                    set(widened_keys) - current_keys.get(wallet, set())
                ),
                "full_stream": {
                    "resolved_signals": full.get("resolved"),
                    "post_fee_pnl_usd": full.get("post_fee_pnl_usd"),
                    "roi_pct": full.get("roi_pct"),
                    "first_half_post_fee_pnl_usd": full.get(
                        "first_half_post_fee_pnl_usd"
                    ),
                    "second_half_post_fee_pnl_usd": full.get(
                        "second_half_post_fee_pnl_usd"
                    ),
                    "concentration_admissible": full.get(
                        "concentration_admissible"
                    ),
                },
                "venue_executable": _venue_disclosure(venue),
                "baseline_to_widened": {
                    "baseline_all_f1_conjuncts_pass": all(
                        baseline_conjuncts.values()
                    ),
                    "widened_all_f1_conjuncts_pass": all(
                        widened_conjuncts.values()
                    ),
                    "resolved_signals_delta": int(venue.get("resolved") or 0)
                    - int(baseline_venue.get("resolved") or 0),
                    "roi_pct_delta": round(
                        num(venue.get("roi_pct"))
                        - num(baseline_venue.get("roi_pct")),
                        6,
                    ),
                    "venue_reachable_share_pct_delta": round(
                        num(venue.get("venue_reachable_share_pct"))
                        - num(
                            baseline_venue.get("venue_reachable_share_pct")
                        ),
                        6,
                    ),
                    "concentration_admissible_changed": (
                        baseline_venue.get("concentration_admissible")
                        is not venue.get("concentration_admissible")
                    ),
                },
            }
        )
    family_exhaustion_verdict = _family_exhaustion_verdict(rows)
    return {
        "schema_version": 1,
        "kind": "widened_fingerprint_paper_candidates",
        "flow_stage": "PROMOTE/LEARN",
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "selection_rule_id": "frozen_positive_70pct_move_slices_v1",
        "f1_floor_unchanged": F1_RESOLVED_FLOOR,
        "family_exhaustion_verdict": family_exhaustion_verdict,
        "candidate_count": len(rows),
        "rows": rows,
        "summary": {
            "candidates_with_more_move_slices": sum(
                row["additional_move_slice_key_count"] > 0 for row in rows
            ),
            "venue_f1_pass_count": sum(
                all(row["venue_executable"]["f1_conjuncts"].values())
                for row in rows
            ),
        },
        "decision": "PAPER_ONLY_NO_PROMOTION_THIS_PULSE",
        "rule": (
            "score the union of same-selection-family move slices already "
            "captured at our prices; never write an F1 conjunct, overlay, or roster"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--manifest-glob", default=DEFAULT_MANIFEST_GLOB)
    parser.add_argument("--reconciliation", default=DEFAULT_RECONCILIATION)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    packet = build_packet(
        ledger_rows=_jsonl(Path(args.ledger)),
        resolution_rows=_jsonl(Path(args.resolutions)),
        manifests=sorted(ROOT.glob(args.manifest_glob)),
        reconciliation=json.loads(Path(args.reconciliation).read_text()),
        generated_at=datetime.now(UTC).isoformat(),
    )
    atomic_write_json(args.output, packet)
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
