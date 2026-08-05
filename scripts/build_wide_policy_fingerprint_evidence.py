#!/usr/bin/env python3
"""Build fingerprint-strict WIDE paper evidence from the append-only ledger."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.reconcile_wide_exact_policy_paper import (
    DEFAULT_LEDGER,
    DEFAULT_RESOLUTIONS,
    POLICY_ID,
    apply_resolutions,
    manifest_wallet_policy_identities,
    replay_ledger,
)
from src.trade_executor import (
    WALLET_COPY_MIN_SHARE_HARD_CAP_USD,
    WALLET_COPY_MIN_SHARES,
)
from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json
from src.wallet_copy.venue_executability import (
    VENUE_EVIDENCE_AUTHORITY,
    VENUE_REACHABLE_SHARE_MIN_PCT,
    venue_discard_price_band,
    venue_discard_reason,
    venue_execution_price,
    venue_gate_summary,
    venue_minimum_max_price,
)
from scripts.report_two_arm_concentration_decomposition import (
    _summarize as _summarize_concentration,
)
from scripts.run_freeze_resolution_accelerator import (
    DIRECTION_DIRECT_CLIMB_PRIORITY,
)

DEFAULT_OUTPUT = "data/research/wide_policy_fingerprint_evidence_latest.json"
DEFAULT_ATOMIC_OUTPUT = "data/research/order134_c_atomic_move_slice_rescore_latest.json"
DEFAULT_SWEEP_OUTPUT = "data/research/order134_d_venue_min_order_sweep_latest.json"
DEFAULT_MANIFEST_GLOB = "data/research/wide_exact_policy_manifest_wide_*.json"
DEFAULT_SOURCE_HISTORY_ACQUISITION = (
    "data/research/exact_wallet_source_history_acquisition_3048_2a1a.json"
)


def _jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def load_manifest_index(paths: list[Path]) -> tuple[dict[str, Any], dict[str, Any]]:
    by_run: dict[str, Any] = {}
    by_manifest_wallet: dict[str, Any] = {}
    for path in paths:
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(manifest, dict):
            continue
        run_id = str(manifest.get("score_run_id") or path.stem.removeprefix("wide_exact_policy_manifest_"))
        manifest_id = str(manifest.get("manifest_id") or "")
        identities = manifest_wallet_policy_identities(manifest)
        by_run[run_id] = {
            "manifest_id": manifest_id,
            "path": str(path),
            "wallet_policy_identities": identities,
        }
        for wallet, identity in identities.items():
            by_manifest_wallet[f"{manifest_id}|{wallet}"] = identity
    return by_run, by_manifest_wallet


def _source_identity(order: dict[str, Any]) -> str:
    return "|".join(
        (
            str(order.get("wallet") or "").lower(),
            str(order.get("transaction_hash") or ""),
            str(order.get("log_index") or ""),
            str(order.get("token_id") or ""),
        )
    )


def _source_trade_identity(order: dict[str, Any]) -> str:
    return "|".join(
        (
            str(order.get("wallet") or "").lower(),
            str(order.get("transaction_hash") or "").lower(),
            str(order.get("token_id") or ""),
        )
    )


def _valid_acquired_order(
    order: dict[str, Any],
    *,
    authority: dict[str, Any],
) -> bool:
    identity = (
        order.get("wide_policy_identity")
        if isinstance(order.get("wide_policy_identity"), dict)
        else {}
    )
    evidence = (
        order.get("our_price_evidence")
        if isinstance(order.get("our_price_evidence"), dict)
        else {}
    )
    wallet = str(authority.get("wallet") or "").lower()
    fingerprint = str(authority.get("wide_policy_fingerprint") or "")
    return bool(
        order.get("paper_only") is True
        and order.get("live_orders_allowed") is False
        and str(order.get("wallet") or "").lower() == wallet
        and str(order.get("wide_policy_fingerprint") or "") == fingerprint
        and str(identity.get("wallet") or "").lower() == wallet
        and str(identity.get("wide_policy_fingerprint") or "") == fingerprint
        and str(evidence.get("source") or "") == "clob_rest_book_snapshot"
        and evidence.get("asks")
        and 0 < num(order.get("fill_price")) < 1
        and num(order.get("filled_shares")) > 0
        and num(order.get("filled_cost_usd")) > 0
        and 0 <= num(order.get("receipt_to_book_fetch_lag_s")) <= 5.0
        and str((order.get("alpha_move_slice") or {}).get("move_slice_key") or "")
        in set(authority.get("move_slice_keys") or [])
    )


def _summarize_core(resolved: list[dict[str, Any]]) -> dict[str, Any]:
    cost = round(sum(num(row.get("filled_cost_usd")) for row in resolved), 6)
    pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in resolved), 6)
    roi = round(100.0 * pnl / cost, 6) if cost > 0 else None
    concentration = _summarize_concentration(
        resolved,
        market_domain="all_rows",
        derive_btc5m_market_from_timestamp=True,
    )
    concentration_domain_match = int(concentration.get("resolved") or 0) == len(
        resolved
    )
    pnl_excluding_top_1_market = round(
        pnl - num(concentration.get("top_1_market_pnl_usd")),
        6,
    )
    top_1_share = concentration["top_1_market_share_of_total_pnl_pct"]
    concentration_deficits = [
        name
        for name, passed in (
            ("concentration_row_domain_match", concentration_domain_match),
            (
                "concentration_market_identity_complete",
                int(concentration.get("missing_market_identity_rows") or 0) == 0,
            ),
            ("positive_total_pnl_share_defined", top_1_share is not None),
            (
                "top_1_market_share_lt_50pct",
                top_1_share is not None and num(top_1_share) < 50.0,
            ),
            ("pnl_excluding_top_1_market_positive", pnl_excluding_top_1_market > 0.0),
        )
        if not passed
    ]
    return {
        "resolved": len(resolved),
        "resolved_cost_usd": cost,
        "post_fee_pnl_usd": pnl,
        "roi_pct": roi,
        "distinct_markets": concentration["distinct_markets"],
        "top_1_market": concentration["top_1_market"],
        "top_1_market_pnl_usd": concentration["top_1_market_pnl_usd"],
        "top_1_market_share_pct": top_1_share,
        "pnl_excluding_top_1_market": pnl_excluding_top_1_market,
        "win_rate_pct": concentration["win_rate_pct"],
        "concentration_market_domain": concentration["market_domain"],
        "concentration_market_identity_strategy": concentration[
            "market_identity_strategy"
        ],
        "concentration_excluded_market_domain_rows": concentration[
            "excluded_market_domain_rows"
        ],
        "concentration_missing_market_identity_rows": concentration[
            "missing_market_identity_rows"
        ],
        "concentration_row_domain_match": concentration_domain_match,
        "concentration_deficits": concentration_deficits,
        "concentration_admissible": bool(
            concentration_domain_match
            and int(concentration.get("missing_market_identity_rows") or 0) == 0
            and top_1_share is not None
            and num(top_1_share) < 50.0
            and pnl_excluding_top_1_market > 0.0
        ),
        "genuine_concentration_edge": bool(
            concentration_domain_match
            and int(concentration.get("missing_market_identity_rows") or 0) == 0
            and top_1_share is not None
            and num(top_1_share) < 25.0
            and pnl_excluding_top_1_market > 0.0
            and num(concentration["win_rate_pct"]) >= 50.0
        ),
        "f1_pass": bool(len(resolved) >= 200 and pnl > 0 and roi is not None and roi > 0),
        "f1_deficits": [
            name
            for name, passed in (
                ("resolved_gte_200", len(resolved) >= 200),
                ("post_fee_pnl_positive", pnl > 0),
                ("roi_positive", roi is not None and roi > 0),
            )
            if not passed
        ],
    }


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = sorted(
        (row for row in rows if row.get("resolved") is True),
        key=lambda row: (num(row.get("source_event_ts")), str(row.get("order_id") or "")),
    )
    midpoint = (len(resolved) + 1) // 2
    first = resolved[:midpoint]
    second = resolved[midpoint:]
    first_summary = _summarize_core(first)
    second_summary = _summarize_core(second)
    return {
        "observed_fills": len(rows),
        **_summarize_core(resolved),
        "first_half_post_fee_pnl_usd": (
            first_summary["post_fee_pnl_usd"] if first else None
        ),
        "second_half_post_fee_pnl_usd": (
            second_summary["post_fee_pnl_usd"] if second else None
        ),
        "first_half": first_summary,
        "second_half": second_summary,
    }


def _venue_executable_summary(
    rows: list[dict[str, Any]],
    *,
    min_order_usd: float,
    order_type: str = "maker",
) -> dict[str, Any]:
    reachable: list[dict[str, Any]] = []
    reason_counts_all: dict[str, int] = defaultdict(int)
    reason_counts_resolved: dict[str, int] = defaultdict(int)
    discard_price_bands: dict[str, int] = defaultdict(int)
    band_agreement = {"agree": 0, "disagree": 0, "band_unknown": 0}
    resolved_total = 0
    resolved_reachable = 0
    for row in rows:
        reason = venue_discard_reason(
            row,
            min_order_usd=min_order_usd,
            order_type=order_type,
        )
        label = reason or "executable"
        reason_counts_all[label] += 1
        resolved = row.get("resolved") is True
        if resolved:
            resolved_total += 1
            reason_counts_resolved[label] += 1
        if reason is None:
            reachable.append(row)
            if resolved:
                resolved_reachable += 1
        elif resolved:
            discard_price_bands[
                venue_discard_price_band(venue_execution_price(row))
            ] += 1
        if resolved:
            declared_band = str(
                (row.get("alpha_move_slice") or {}).get("entry_price_band")
                or str((row.get("alpha_move_slice") or {}).get("move_slice_key") or "").partition("|")[2]
            )
            price = venue_execution_price(row)
            actual_band = (
                "unknown_price"
                if price is None
                else "<=0.25"
                if price <= 0.25
                else "0.25-0.50"
                if price <= 0.50
                else "0.50-0.75"
                if price <= 0.75
                else ">0.75"
            )
            if not declared_band or declared_band.startswith("unknown") or actual_band == "unknown_price":
                band_agreement["band_unknown"] += 1
            elif declared_band == actual_band:
                band_agreement["agree"] += 1
            else:
                band_agreement["disagree"] += 1
    summary = _summarize(reachable)
    reachable_share_pct = (
        round(100.0 * resolved_reachable / resolved_total, 6)
        if resolved_total
        else None
    )
    reachable_admissible = bool(
        reachable_share_pct is not None
        and reachable_share_pct >= VENUE_REACHABLE_SHARE_MIN_PCT
    )
    deficits = list(summary.get("f1_deficits") or [])
    if not reachable_admissible:
        deficits.append("venue_reachable_share_gte_40pct")
    summary["f1_deficits"] = deficits
    summary["f1_venue_reachable_admissible"] = reachable_admissible
    summary["f1_pass"] = bool(summary.get("f1_pass") and reachable_admissible)
    return {
        **summary,
        "evidence_authority": VENUE_EVIDENCE_AUTHORITY,
        "venue_executable_pnl_usd": summary["post_fee_pnl_usd"],
        "venue_executable_roi_pct": summary["roi_pct"],
        "venue_executable_resolved": resolved_reachable,
        "venue_unreachable_resolved": resolved_total - resolved_reachable,
        "venue_reachable_share_pct": reachable_share_pct,
        "venue_reachable_share_min_pct": VENUE_REACHABLE_SHARE_MIN_PCT,
        "venue_minimum_shares": (
            WALLET_COPY_MIN_SHARES if order_type == "maker" else None
        ),
        "venue_nominal_min_order_usd": (
            min_order_usd if order_type == "taker" else None
        ),
        "venue_minimum_hard_cap_usd": WALLET_COPY_MIN_SHARE_HARD_CAP_USD,
        "venue_order_type": order_type,
        "venue_minimum_max_price": round(
            venue_minimum_max_price(min_order_usd, order_type=order_type), 6
        ),
        "min_order_usd": min_order_usd,
        "venue_discard_reason_counts_resolved": dict(sorted(reason_counts_resolved.items())),
        "venue_discard_reason_counts_all": dict(sorted(reason_counts_all.items())),
        "venue_discard_price_band_counts_resolved": dict(sorted(discard_price_bands.items())),
        "entry_price_band_agreement": band_agreement,
        "gate_authority": True,
    }


def _both_halves_positive(summary: dict[str, Any]) -> bool:
    return bool(
        num(summary.get("first_half_post_fee_pnl_usd")) > 0.0
        and num(summary.get("second_half_post_fee_pnl_usd")) > 0.0
    )


def _best_evidence_rank(summary: dict[str, Any]) -> tuple[Any, ...]:
    return (
        summary.get("concentration_admissible") is True,
        summary.get("genuine_concentration_edge") is True,
        _both_halves_positive(summary),
        summary.get("f1_pass") is True,
        int(summary.get("resolved") or 0),
        num(summary.get("post_fee_pnl_usd")),
    )


def _first_half_rank(summary: dict[str, Any]) -> tuple[Any, ...]:
    return (
        summary.get("concentration_admissible") is True,
        summary.get("genuine_concentration_edge") is True,
        summary.get("f1_pass") is True,
        int(summary.get("resolved") or 0),
        num(summary.get("post_fee_pnl_usd")),
    )


def walk_forward_admissible(
    first_half: dict[str, Any],
    second_half: dict[str, Any],
) -> bool:
    return bool(
        first_half.get("f1_pass") is True
        and num(first_half.get("pnl_excluding_top_1_market")) > 0.0
        and num(second_half.get("post_fee_pnl_usd")) > 0.0
        and second_half.get("f1_pass") is True
        and num(second_half.get("pnl_excluding_top_1_market")) > 0.0
    )


def _resolution_evidence_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Expose unresolved exact-policy windows for the settlement accelerator.

    The current measurement state is intentionally bounded to one capture run,
    while F1 is scored from the append-only full stream.  Persisting the
    fingerprint-strict unresolved window set here lets the canonical Gamma
    refresher prioritize the same evidence population that the scorer uses.
    """

    now_s = dt.datetime.now(dt.UTC).timestamp()
    unresolved: dict[str, int] = {}
    for row in rows:
        if row.get("resolved") is True:
            continue
        slug = str(row.get("market_slug") or "")
        if not slug.startswith("btc-updown-5m-"):
            continue
        try:
            start_s = int(slug.rsplit("-", 1)[-1])
        except ValueError:
            continue
        if start_s + 300 > now_s:
            continue
        unresolved.setdefault(slug, start_s)
    windows = [
        slug
        for slug, _start_s in sorted(
            unresolved.items(),
            key=lambda item: item[1],
            reverse=True,
        )
    ]
    return {
        "matured_unresolved_window_count": len(windows),
        "matured_unresolved_windows": windows,
        "matured_unresolved_window_sample": windows[:25],
    }


def _unique_source_rows_by_wallet(
    *,
    full_stream_by_wallet: dict[str, dict[str, dict[str, Any]]],
    acquired_by_fingerprint: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, dict[str, Any]]]:
    unique_by_wallet: dict[str, dict[str, dict[str, Any]]] = {
        wallet: dict(rows) for wallet, rows in full_stream_by_wallet.items()
    }
    for rows in acquired_by_fingerprint.values():
        for row in rows:
            wallet = str(row.get("wallet") or "").lower()
            unique_by_wallet.setdefault(wallet, {})[_source_identity(row)] = row
    return unique_by_wallet


def _atomic_move_slice_rescore(
    *,
    unique_by_wallet: dict[str, dict[str, dict[str, Any]]],
    fingerprints: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Score disjoint atomic move buckets without changing gate authority."""

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    assigned_source_ids: set[tuple[str, str]] = set()
    duplicate_assignment_count = 0
    for wallet, rows in unique_by_wallet.items():
        for source_id, row in rows.items():
            assignment = (wallet, source_id)
            if assignment in assigned_source_ids:
                duplicate_assignment_count += 1
                continue
            assigned_source_ids.add(assignment)
            move_slice_key = str(
                (row.get("alpha_move_slice") or {}).get("move_slice_key")
                or "UNCLASSIFIED"
            )
            grouped[(wallet, move_slice_key)].append(row)

    rows: list[dict[str, Any]] = []
    for (wallet, move_slice_key), bucket_rows in sorted(grouped.items()):
        fixed = _summarize(bucket_rows)
        identity_min_order_usd_values = sorted(
            {
                num(identity.get("min_order_usd"), 1.0)
                for identity in fingerprints.values()
                if str(identity.get("wallet") or "").lower() == wallet
                and move_slice_key in set(identity.get("move_slice_keys") or [])
            }
        )
        # Atomic rows can be shared by several fingerprint cells, but their
        # fixed-policy ticket minimum must agree.  Missing or conflicting
        # identity evidence fails the venue rescore closed.
        identity_min_order_usd = (
            identity_min_order_usd_values[0]
            if len(identity_min_order_usd_values) == 1
            else 0.0
        )
        venue = _venue_executable_summary(
            bucket_rows,
            min_order_usd=identity_min_order_usd,
            order_type="taker",
        )
        fixed_walk_forward = walk_forward_admissible(
            fixed["first_half"], fixed["second_half"]
        )
        venue_walk_forward = walk_forward_admissible(
            venue["first_half"], venue["second_half"]
        )
        rows.append(
            {
                "wallet": wallet,
                "move_slice_key": move_slice_key,
                "source_row_count": len(bucket_rows),
                "fixed_policy_rescore": fixed,
                "venue_executable_rescore": venue,
                "identity_min_order_usd_values": identity_min_order_usd_values,
                "f1_walk_forward_admissible_fixed_policy": fixed_walk_forward,
                "f1_walk_forward_admissible_venue": venue_walk_forward,
                "paper_only": True,
                "promotion_authority": False,
                "live_authority": False,
            }
        )

    unique_resolved = sum(
        row.get("resolved") is True
        for wallet_rows in unique_by_wallet.values()
        for row in wallet_rows.values()
    )
    assigned_resolved = sum(
        int((row.get("fixed_policy_rescore") or {}).get("resolved") or 0)
        for row in rows
    )
    by_wallet: dict[str, dict[str, Any]] = {}
    for wallet in sorted(unique_by_wallet):
        wallet_rows = [row for row in rows if row["wallet"] == wallet]
        by_wallet[wallet] = {
            "atomic_bucket_count": len(wallet_rows),
            "unique_source_row_count": len(unique_by_wallet[wallet]),
            "unique_resolved_count": sum(
                row.get("resolved") is True
                for row in unique_by_wallet[wallet].values()
            ),
            "fixed_policy_walk_forward_admissible_count": sum(
                row["f1_walk_forward_admissible_fixed_policy"] is True
                for row in wallet_rows
            ),
            "venue_walk_forward_admissible_count": sum(
                row["f1_walk_forward_admissible_venue"] is True
                for row in wallet_rows
            ),
            "max_fixed_policy_min_half_resolved": max(
                (
                    min(
                        int(row["fixed_policy_rescore"]["first_half"]["resolved"]),
                        int(row["fixed_policy_rescore"]["second_half"]["resolved"]),
                    )
                    for row in wallet_rows
                ),
                default=0,
            ),
            "max_venue_min_half_resolved": max(
                (
                    min(
                        int(row["venue_executable_rescore"]["first_half"]["resolved"]),
                        int(row["venue_executable_rescore"]["second_half"]["resolved"]),
                    )
                    for row in wallet_rows
                ),
                default=0,
            ),
        }
    return {
        "schema_version": 1,
        "kind": "order134_c_atomic_move_slice_rescore",
        "flow_stage": "DISCOVER/LEARN/DEFEND",
        "generated_at": utc_now_iso(),
        "scope": {"status": "IN_PROCESS_UNSPECIFIED"},
        "wallet_scope": sorted(unique_by_wallet),
        "identity_rule": "wallet|exact_atomic_alpha_move_slice.move_slice_key",
        "partition_property": "each unique wallet source identity assigned to exactly one atomic bucket",
        "fixed_policy_knobs": {
            "max_order_usd": 1.0,
            "min_order_usd": 1.0,
            "wallet_fraction": 0.1,
            "max_fill_lag_s": 5.0,
        },
        "paper_only": True,
        "promotion_authority": False,
        "live_authority": False,
        "wallet_count": len(unique_by_wallet),
        "atomic_bucket_count": len(rows),
        "atomic_move_slice_keys": sorted({row["move_slice_key"] for row in rows}),
        "unique_source_row_count": sum(
            len(wallet_rows) for wallet_rows in unique_by_wallet.values()
        ),
        "unique_resolved_count": unique_resolved,
        "assigned_resolved_count": assigned_resolved,
        "resolved_partition_reconciles": assigned_resolved == unique_resolved,
        "duplicate_assignment_count": duplicate_assignment_count,
        "fixed_policy_walk_forward_admissible_count": sum(
            row["f1_walk_forward_admissible_fixed_policy"] is True for row in rows
        ),
        "venue_walk_forward_admissible_count": sum(
            row["f1_walk_forward_admissible_venue"] is True for row in rows
        ),
        "by_wallet": by_wallet,
        "rows": rows,
    }


def _order134_b_venue_discard_decomposition(
    unique_by_wallet: dict[str, dict[str, dict[str, Any]]],
    *,
    min_order_usd: float = 1.0,
) -> dict[str, Any]:
    reason_counts: dict[str, int] = defaultdict(int)
    price_bands: dict[str, int] = defaultdict(int)
    band_agreement = {"agree": 0, "disagree": 0, "band_unknown": 0}
    by_wallet: dict[str, dict[str, Any]] = {}
    resolved_total = 0
    for wallet, source_rows in sorted(unique_by_wallet.items()):
        wallet_counts: dict[str, int] = defaultdict(int)
        wallet_total = 0
        for row in source_rows.values():
            if row.get("resolved") is not True:
                continue
            wallet_total += 1
            resolved_total += 1
            reason = venue_discard_reason(row, min_order_usd=min_order_usd) or "executable"
            reason_counts[reason] += 1
            wallet_counts[reason] += 1
            declared_band = str(
                (row.get("alpha_move_slice") or {}).get("entry_price_band")
                or str((row.get("alpha_move_slice") or {}).get("move_slice_key") or "").partition("|")[2]
            )
            price = venue_execution_price(row)
            actual_band = (
                "unknown_price"
                if price is None
                else "<=0.25"
                if price <= 0.25
                else "0.25-0.50"
                if price <= 0.50
                else "0.50-0.75"
                if price <= 0.75
                else ">0.75"
            )
            if not declared_band or declared_band.startswith("unknown") or actual_band == "unknown_price":
                band_agreement["band_unknown"] += 1
            elif declared_band == actual_band:
                band_agreement["agree"] += 1
            else:
                band_agreement["disagree"] += 1
            if reason != "executable":
                price_bands[venue_discard_price_band(venue_execution_price(row))] += 1
        discarded = wallet_total - wallet_counts.get("executable", 0)
        coverage = wallet_counts.get("price_field_absent", 0) + wallet_counts.get("price_nonpositive", 0)
        structural = wallet_counts.get("price_above_venue_minimum_max_price", 0)
        by_wallet[wallet] = {
            "resolved_total": wallet_total,
            "retention_pct": round(100.0 * wallet_counts.get("executable", 0) / wallet_total, 6) if wallet_total else None,
            "coverage_defect_share_pct": round(100.0 * coverage / discarded, 6) if discarded else 0.0,
            "structural_reachability_share_pct": round(100.0 * structural / discarded, 6) if discarded else 0.0,
        }
    executable = reason_counts.get("executable", 0)
    discarded = resolved_total - executable
    coverage = reason_counts.get("price_field_absent", 0) + reason_counts.get("price_nonpositive", 0)
    structural = reason_counts.get("price_above_venue_minimum_max_price", 0)
    coverage_share = round(100.0 * coverage / discarded, 6) if discarded else 0.0
    structural_share = round(100.0 * structural / discarded, 6) if discarded else 0.0
    verdict = (
        "COVERAGE_DEFECT"
        if coverage_share >= 50.0
        else "STRUCTURAL_REACHABILITY"
        if structural_share >= 50.0
        else "MIXED_NO_ACTION"
    )
    for reason in (
        "executable",
        "price_field_absent",
        "price_nonpositive",
        "min_order_usd_nonpositive",
        "price_above_venue_minimum_max_price",
    ):
        reason_counts.setdefault(reason, 0)
    return {
        "schema_version": 1,
        "kind": "order134_b_venue_discard_decomposition",
        "flow_stage": "DISCOVER/DEFEND",
        "generated_at": utc_now_iso(),
        "scope": {"status": "IN_PROCESS_UNSPECIFIED"},
        "wallet_scope": sorted(unique_by_wallet),
        "min_order_usd": min_order_usd,
        "venue_minimum_shares": WALLET_COPY_MIN_SHARES,
        "venue_minimum_hard_cap_usd": WALLET_COPY_MIN_SHARE_HARD_CAP_USD,
        "venue_minimum_max_price": round(venue_minimum_max_price(min_order_usd), 6),
        "predicate_reduces_to": "0 < price <= venue_minimum_max_price",
        "resolved_total": resolved_total,
        "resolved_executable": executable,
        "resolved_discarded": discarded,
        "retention_pct": round(100.0 * executable / resolved_total, 6) if resolved_total else None,
        "reason_counts": dict(sorted(reason_counts.items())),
        "reason_share_pct": {
            reason: round(100.0 * count / discarded, 6) if discarded and reason != "executable" else None
            for reason, count in sorted(reason_counts.items())
        },
        "price_band_counts": dict(sorted(price_bands.items())),
        "entry_price_band_agreement": band_agreement,
        "coverage_defect_resolved": coverage,
        "coverage_defect_share_pct": coverage_share,
        "structural_reachability_resolved": structural,
        "structural_reachability_share_pct": structural_share,
        "verdict": verdict,
        "verdict_rule": ">=50% of discarded resolved rows",
        "verdict_basis": {
            "discarded_resolved": discarded,
            "coverage_defect_resolved": coverage,
            "structural_reachability_resolved": structural,
        },
        "by_wallet": by_wallet,
        "paper_only": True,
        "promotion_authority": False,
        "live_authority": False,
    }


def _rescale_rows_for_nominal(
    rows: list[dict[str, Any]],
    *,
    nominal_usd: float,
) -> tuple[list[dict[str, Any]], int]:
    rescaled: list[dict[str, Any]] = []
    unrescalable = 0
    for row in rows:
        price = venue_execution_price(row)
        ledger_shares = num(row.get("filled_shares"))
        if price is None or price <= 0 or ledger_shares <= 0:
            unrescalable += 1
            continue
        shares = max(WALLET_COPY_MIN_SHARES, nominal_usd / price)
        scale = shares / ledger_shares
        rescaled.append(
            {
                **row,
                "filled_cost_usd": round(shares * price, 9),
                "post_fee_pnl_usd": round(num(row.get("post_fee_pnl_usd")) * scale, 9),
                "shares_ledger": ledger_shares,
                "shares_sweep": shares,
                "shares_scale": scale,
                "raw_post_fee_pnl_usd": num(row.get("post_fee_pnl_usd")),
            }
        )
    return rescaled, unrescalable


def _sweep_population_summary(
    rows: list[dict[str, Any]],
    *,
    nominal_usd: float,
) -> dict[str, Any]:
    resolved_rows = [row for row in rows if row.get("resolved") is True]
    rescaled, unrescalable = _rescale_rows_for_nominal(
        resolved_rows, nominal_usd=nominal_usd
    )
    summary = _summarize(rescaled)
    return {
        **summary,
        "resolved": len(resolved_rows),
        "rescalable_resolved": len(rescaled),
        "unrescalable_rows": unrescalable,
        "raw_post_fee_pnl_usd": round(
            sum(num(row.get("raw_post_fee_pnl_usd")) for row in rescaled), 6
        ),
        "post_fee_pnl_usd": summary["post_fee_pnl_usd"],
    }


def _order134_d_venue_min_order_sweep(
    *,
    unique_by_wallet: dict[str, dict[str, dict[str, Any]]],
    fingerprints: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    grid = (1.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0)
    all_rows = [row for rows in unique_by_wallet.values() for row in rows.values()]
    cell_rows = {
        fingerprint: [
            row
            for row in unique_by_wallet.get(str(identity.get("wallet") or "").lower(), {}).values()
            if str((row.get("alpha_move_slice") or {}).get("move_slice_key") or "")
            in set(identity.get("move_slice_keys") or [])
        ]
        for fingerprint, identity in fingerprints.items()
    }
    crosstab: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    disagreements_straddling_ceiling = 0
    for row in all_rows:
        if row.get("resolved") is not True:
            continue
        declared = str(
            (row.get("alpha_move_slice") or {}).get("entry_price_band")
            or str((row.get("alpha_move_slice") or {}).get("move_slice_key") or "").partition("|")[2]
            or "unknown"
        )
        price = venue_execution_price(row)
        actual = (
            "unknown"
            if price is None
            else "<=0.25"
            if price <= 0.25
            else "0.25-0.50"
            if price <= 0.50
            else "0.50-0.75"
            if price <= 0.75
            else ">0.75"
        )
        crosstab[declared][actual] += 1
        declared_low = declared in {"<=0.25", "0.25-0.50"}
        actual_low = actual in {"<=0.25", "0.25-0.50"}
        if declared != "unknown" and actual != "unknown" and declared_low != actual_low:
            disagreements_straddling_ceiling += 1

    points: list[dict[str, Any]] = []
    previous_ceiling = 0.0
    previous_cell_half_floor: dict[str, bool] = {}
    for nominal in grid:
        ceiling = venue_minimum_max_price(nominal)
        cumulative_rows = [
            row
            for row in all_rows
            if venue_discard_reason(row, min_order_usd=nominal) is None
        ]
        marginal_rows = [
            row
            for row in cumulative_rows
            if (venue_execution_price(row) or 0.0) > previous_ceiling + 1e-9
        ]
        cumulative = _sweep_population_summary(cumulative_rows, nominal_usd=nominal)
        marginal = _sweep_population_summary(marginal_rows, nominal_usd=nominal)
        f1_pass_cells = 0
        walk_forward_pass_cells = 0
        marginal_f1_pass_cells = 0
        marginal_walk_forward_pass_cells = 0
        current_cell_half_floor: dict[str, bool] = {}
        crossing = 0
        for fingerprint, raw_rows in cell_rows.items():
            admitted = [
                row
                for row in raw_rows
                if venue_discard_reason(row, min_order_usd=nominal) is None
            ]
            rescaled, _unrescalable = _rescale_rows_for_nominal(
                admitted, nominal_usd=nominal
            )
            summary = _summarize(rescaled)
            retention = (
                100.0 * int(summary.get("resolved") or 0)
                / max(1, sum(row.get("resolved") is True for row in raw_rows))
            )
            if summary.get("f1_pass") is True and retention >= VENUE_REACHABLE_SHARE_MIN_PCT:
                f1_pass_cells += 1
            if walk_forward_admissible(summary["first_half"], summary["second_half"]):
                walk_forward_pass_cells += 1
            marginal_admitted = [
                row
                for row in admitted
                if (venue_execution_price(row) or 0.0) > previous_ceiling + 1e-9
            ]
            marginal_rescaled, _marginal_unrescalable = _rescale_rows_for_nominal(
                marginal_admitted, nominal_usd=nominal
            )
            marginal_cell_summary = _summarize(marginal_rescaled)
            if marginal_cell_summary.get("f1_pass") is True:
                marginal_f1_pass_cells += 1
            if walk_forward_admissible(
                marginal_cell_summary["first_half"],
                marginal_cell_summary["second_half"],
            ):
                marginal_walk_forward_pass_cells += 1
            half_floor = bool(
                int(summary["first_half"].get("resolved") or 0) >= 200
                and int(summary["second_half"].get("resolved") or 0) >= 200
            )
            current_cell_half_floor[fingerprint] = half_floor
            if half_floor and not previous_cell_half_floor.get(fingerprint, False):
                crossing += 1
        cumulative["f1_pass_cells"] = f1_pass_cells
        cumulative["walk_forward_pass_cells"] = walk_forward_pass_cells
        marginal["f1_pass_cells"] = marginal_f1_pass_cells
        marginal["walk_forward_pass_cells"] = marginal_walk_forward_pass_cells
        marginal_total_pass = num(marginal.get("post_fee_pnl_usd")) > 0
        marginal_first_half_pass = (
            num(marginal.get("first_half_post_fee_pnl_usd")) > 0
        )
        marginal_second_half_pass = (
            num(marginal.get("second_half_post_fee_pnl_usd")) > 0
        )
        marginal_concentration_pass = (
            marginal.get("concentration_admissible") is True
        )
        marginal_edge_pass = bool(
            marginal_total_pass
            and marginal_first_half_pass
            and marginal_second_half_pass
            and marginal_concentration_pass
        )
        marginal_sample_pass = bool(
            int((marginal.get("first_half") or {}).get("resolved") or 0) >= 200
            and int((marginal.get("second_half") or {}).get("resolved") or 0) >= 200
        )
        max_loss_per_ticket = max(nominal, WALLET_COPY_MIN_SHARE_HARD_CAP_USD)
        max_tickets_per_day = math.floor(8.0 / max_loss_per_ticket)
        ev_per_ticket = (
            num(marginal.get("post_fee_pnl_usd")) / int(marginal.get("rescalable_resolved") or 1)
            if int(marginal.get("rescalable_resolved") or 0) > 0
            else None
        )
        tickets_needed = (
            math.ceil(100.0 / ev_per_ticket)
            if ev_per_ticket is not None and ev_per_ticket > 0
            else None
        )
        rate_pass = bool(
            tickets_needed is not None and tickets_needed <= max_tickets_per_day
        )
        hard_bound_pass = bool(
            max_tickets_per_day >= 1
            and max_loss_per_ticket * max_tickets_per_day <= 8.0 + 1e-9
        )
        refusal_deficits: list[str] = []
        if not marginal_sample_pass:
            if int((marginal.get("first_half") or {}).get("resolved") or 0) < 200:
                refusal_deficits.append("marginal_first_half_resolved_lt_200")
            if int((marginal.get("second_half") or {}).get("resolved") or 0) < 200:
                refusal_deficits.append("marginal_second_half_resolved_lt_200")
        if not marginal_total_pass:
            refusal_deficits.append("marginal_total_post_fee_pnl_nonpositive")
        if not marginal_first_half_pass:
            refusal_deficits.append("marginal_first_half_post_fee_pnl_nonpositive")
        if not marginal_second_half_pass:
            refusal_deficits.append("marginal_second_half_post_fee_pnl_nonpositive")
        if not marginal_concentration_pass:
            refusal_deficits.extend(
                f"marginal_concentration:{deficit}"
                for deficit in marginal.get("concentration_deficits") or []
            )
        if not rate_pass:
            refusal_deficits.append("target_rate_unaffordable")
        verdict = (
            "CEILING_DUPLICATE"
            if ceiling <= previous_ceiling + 1e-9
            else "REFUSE_SAMPLE"
            if not marginal_sample_pass
            else "REFUSE_MARGINAL_TOTAL_NEGATIVE"
            if not marginal_total_pass
            else "REFUSE_MARGINAL_HALF_NEGATIVE"
            if not (marginal_first_half_pass and marginal_second_half_pass)
            else "REFUSE_MARGINAL_CONCENTRATION"
            if not marginal_concentration_pass
            else "REFUSE_RATE_UNAFFORDABLE"
            if not rate_pass
            else f"SIZE_UP_PROPOSED({nominal:g})"
        )
        by_wallet = {}
        for wallet, wallet_source_rows in sorted(unique_by_wallet.items()):
            admitted = [
                row
                for row in wallet_source_rows.values()
                if venue_discard_reason(row, min_order_usd=nominal) is None
            ]
            wallet_summary = _sweep_population_summary(admitted, nominal_usd=nominal)
            by_wallet[wallet] = {
                "resolved": wallet_summary["resolved"],
                "post_fee_pnl_usd": wallet_summary["post_fee_pnl_usd"],
                "first_half_post_fee_pnl_usd": wallet_summary["first_half_post_fee_pnl_usd"],
                "second_half_post_fee_pnl_usd": wallet_summary["second_half_post_fee_pnl_usd"],
                "unrescalable_rows": wallet_summary["unrescalable_rows"],
            }
        points.append(
            {
                "min_order_usd": nominal,
                "venue_minimum_max_price": round(ceiling, 6),
                "ceiling_duplicate_of": 1.0 if nominal == 2.5 else None,
                "venue_reachable_share_pct": round(
                    100.0 * cumulative["resolved"]
                    / max(1, sum(row.get("resolved") is True for row in all_rows)),
                    6,
                ),
                "venue_reachable_share_bar_fixed_pct": VENUE_REACHABLE_SHARE_MIN_PCT,
                "venue_gate_is_vacuous_at_ceiling": ceiling >= 1.0,
                "cumulative": cumulative,
                "marginal": marginal,
                "cells_crossing_200_floor_due_to_ceiling": crossing,
                "by_wallet": by_wallet,
                "loss_bound": {
                    "max_loss_per_ticket_usd": max_loss_per_ticket,
                    "max_tickets_per_utc_day_at_8usd": max_tickets_per_day,
                    "marginal_ev_per_ticket_usd": ev_per_ticket,
                    "tickets_per_day_needed_for_100usd": tickets_needed,
                    "max_daily_loss_at_bound_usd": max_loss_per_ticket * max_tickets_per_day,
                    "hard_bound_pass": hard_bound_pass,
                    "target_rate_affordable": rate_pass,
                },
                "marginal_edge_pass": marginal_edge_pass,
                "marginal_sample_pass": marginal_sample_pass,
                "refusal_deficits": refusal_deficits,
                "verdict": verdict,
            }
        )
        previous_ceiling = max(previous_ceiling, ceiling)
        previous_cell_half_floor = current_cell_half_floor
    precommitted_counts = {
        "0.50": 11417,
        "0.60": 17211,
        "0.70": 21678,
        "0.80": 24950,
        "0.90": 28060,
        "1.00": 31956,
    }
    observed_counts = {
        f"{point['venue_minimum_max_price']:.2f}": point["cumulative"]["resolved"]
        for point in points
        if point["ceiling_duplicate_of"] is None
    }
    total_accrual = observed_counts.get("1.00", 0) - precommitted_counts["1.00"]
    validation_pass = bool(
        total_accrual >= 0
        and all(
            abs(observed_counts.get(ceiling, 0) - expected) <= total_accrual
            for ceiling, expected in precommitted_counts.items()
        )
    )
    proposal = next(
        (point for point in points if str(point["verdict"]).startswith("SIZE_UP_PROPOSED")),
        None,
    )
    return {
        "schema_version": 1,
        "kind": "order134_d_venue_min_order_sweep",
        "flow_stage": "DISCOVER/DEFEND/PROMOTE",
        "generated_at": utc_now_iso(),
        "scope": {"status": "IN_PROCESS_UNSPECIFIED"},
        "wallet_scope": sorted(unique_by_wallet),
        "grid_min_order_usd": list(grid),
        "price_axis_authority": {
            "executability": "first_truthy_predicate_price_field",
            "regime_keying": "alpha_move_slice.entry_price_band",
        },
        "declared_x_actual_price_band_crosstab": {
            declared: dict(sorted(actual.items()))
            for declared, actual in sorted(crosstab.items())
        },
        "declared_actual_disagreements_straddling_0_50": disagreements_straddling_ceiling,
        "precommitted_cumulative_validation": {
            "baseline_generated_at": "2026-08-01T06:00:53Z",
            "expected": precommitted_counts,
            "observed": observed_counts,
            "total_resolved_accrual_since_baseline": total_accrual,
            "status": "PASS" if validation_pass else "MISMATCH_STOP_BEFORE_PNL_BRANCH",
        },
        "fixed_reachable_share_bar_pct": VENUE_REACHABLE_SHARE_MIN_PCT,
        "sized_proposal": proposal,
        "overall_verdict": proposal["verdict"] if proposal else "NO_SIZE_UP_PROPOSAL",
        "paper_only": True,
        "promotion_authority": False,
        "live_authority": False,
        "points": points,
    }


def build_evidence(
    *,
    ledger_rows: list[dict[str, Any]],
    manifests: list[Path],
    resolution_rows: list[dict[str, Any]] | None = None,
    source_history_acquisition: dict[str, Any] | None = None,
) -> dict[str, Any]:
    orders, _event_ids = replay_ledger(ledger_rows)
    if resolution_rows:
        # Full-stream F1 must consume the same canonical settlement index as
        # the bounded current-run reconciler.  The returned events are not
        # appended here; this scorer remains a read-only materialized view.
        orders, _resolution_events = apply_resolutions(orders, resolution_rows)
    by_run, by_manifest_wallet = load_manifest_index(manifests)
    fingerprints: dict[str, dict[str, Any]] = {}
    observed_cells: dict[str, list[dict[str, Any]]] = defaultdict(list)
    full_stream_by_wallet: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    acquired_by_fingerprint: dict[str, list[dict[str, Any]]] = defaultdict(list)
    ledger_trade_identities: set[str] = set()

    for order in orders:
        wallet = str(order.get("wallet") or "").lower()
        run = by_run.get(str(order.get("run_id") or ""), {})
        identity = (run.get("wallet_policy_identities") or {}).get(wallet)
        if not identity:
            continue
        fingerprint = str(identity["wide_policy_fingerprint"])
        fingerprints[fingerprint] = {
            **identity,
            "manifest_ids": sorted(
                {
                    *fingerprints.get(fingerprint, {}).get("manifest_ids", []),
                    str(run.get("manifest_id") or ""),
                }
            ),
        }
        observed_cells[fingerprint].append(order)
        ledger_trade_identities.add(_source_trade_identity(order))
        source_id = _source_identity(order)
        current = full_stream_by_wallet[wallet].get(source_id)
        if current is None or (order.get("resolved") is True and current.get("resolved") is not True):
            full_stream_by_wallet[wallet][source_id] = order

    acquisition = (
        source_history_acquisition
        if isinstance(source_history_acquisition, dict)
        else {}
    )
    authority = (
        acquisition.get("acquisition_authority")
        if isinstance(acquisition.get("acquisition_authority"), dict)
        else {}
    )
    acquired_rows = [
        row for row in acquisition.get("orders") or [] if isinstance(row, dict)
    ]
    if resolution_rows:
        acquired_rows, _events = apply_resolutions(acquired_rows, resolution_rows)
    acquired_admitted = 0
    for order in acquired_rows:
        if not _valid_acquired_order(order, authority=authority):
            continue
        trade_identity = _source_trade_identity(order)
        if (
            not trade_identity.strip("|")
            or trade_identity in ledger_trade_identities
        ):
            continue
        identity = order["wide_policy_identity"]
        wallet = str(identity["wallet"]).lower()
        fingerprint = str(identity["wide_policy_fingerprint"])
        fingerprints.setdefault(
            fingerprint,
            {**identity, "manifest_ids": ["source_history_acquisition"]},
        )
        observed_cells[fingerprint].append(order)
        acquired_by_fingerprint[fingerprint].append(order)
        ledger_trade_identities.add(trade_identity)
        acquired_admitted += 1

    cells: list[dict[str, Any]] = []
    for fingerprint, identity in fingerprints.items():
        wallet = str(identity["wallet"])
        allowed = set(identity["move_slice_keys"])
        rescore_rows = [
            order
            for order in full_stream_by_wallet[wallet].values()
            if str((order.get("alpha_move_slice") or {}).get("move_slice_key") or "") in allowed
        ]
        rescore_rows.extend(acquired_by_fingerprint.get(fingerprint, []))
        move_slice_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for order in rescore_rows:
            move_slice_key = str(
                (order.get("alpha_move_slice") or {}).get("move_slice_key") or ""
            )
            if move_slice_key:
                move_slice_rows[move_slice_key].append(order)
        full_band_summary = _summarize(rescore_rows)
        full_band_summary["advisory_only"] = True
        maker_venue_summary = _venue_executable_summary(
            rescore_rows,
            min_order_usd=num(identity.get("min_order_usd"), 1.0),
            order_type="maker",
        )
        venue_summary = _venue_executable_summary(
            rescore_rows,
            min_order_usd=num(identity.get("min_order_usd"), 1.0),
            order_type="taker",
        )
        cells.append(
            {
                "wide_policy_fingerprint": fingerprint,
                "identity": identity,
                "observed_same_fingerprint": _summarize(observed_cells[fingerprint]),
                "fixed_policy_full_stream_rescore": full_band_summary,
                "venue_executable_full_stream_rescore": venue_summary,
                "maker_venue_executable_full_stream_rescore": maker_venue_summary,
                "move_slice_venue_executable_full_stream_rescore": {
                    move_slice_key: _venue_executable_summary(
                        rows,
                        min_order_usd=num(identity.get("min_order_usd"), 1.0),
                        order_type="taker",
                    )
                    for move_slice_key, rows in sorted(move_slice_rows.items())
                },
                "resolution_evidence_summary": _resolution_evidence_summary(
                    rescore_rows
                ),
                "evidence_authority": VENUE_EVIDENCE_AUTHORITY,
            }
        )
    cells.sort(
        key=lambda row: (
            not venue_gate_summary(row)["f1_pass"],
            -int(venue_gate_summary(row)["resolved"]),
            -float(venue_gate_summary(row)["post_fee_pnl_usd"]),
            row["identity"]["wallet"],
            row["wide_policy_fingerprint"],
        )
    )
    old_size_rank_best_by_wallet: dict[str, dict[str, Any]] = {}
    for cell in cells:
        old_size_rank_best_by_wallet.setdefault(
            str(cell["identity"]["wallet"]), cell
        )
    cells_by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for cell in cells:
        cells_by_wallet[str(cell["identity"]["wallet"])].append(cell)
    best_by_wallet = {
        wallet: max(
            wallet_cells,
            key=lambda row: _best_evidence_rank(venue_gate_summary(row)),
        )
        for wallet, wallet_cells in sorted(cells_by_wallet.items())
    }
    best_by_wallet_rejected_for_concentration: dict[str, dict[str, Any]] = {}
    for wallet, old_cell in old_size_rank_best_by_wallet.items():
        selected_cell = best_by_wallet[wallet]
        old_summary = venue_gate_summary(old_cell)
        if (
            old_cell["wide_policy_fingerprint"]
            != selected_cell["wide_policy_fingerprint"]
            and old_summary.get("concentration_admissible") is not True
        ):
            best_by_wallet_rejected_for_concentration[wallet] = {
                "rejected_wide_policy_fingerprint": old_cell[
                    "wide_policy_fingerprint"
                ],
                "rejected_resolved": old_summary.get("resolved"),
                "rejected_top_1_market_share_pct": old_summary.get(
                    "top_1_market_share_pct"
                ),
                "rejected_pnl_excluding_top_1_market": old_summary.get(
                    "pnl_excluding_top_1_market"
                ),
                "selected_wide_policy_fingerprint": selected_cell[
                    "wide_policy_fingerprint"
                ],
                "reason": "old_size_rank_selected_concentration_inadmissible_cell",
            }
    walk_forward_best_by_wallet: dict[str, dict[str, Any]] = {}
    for wallet, wallet_cells in sorted(cells_by_wallet.items()):
        cells_tested = len(wallet_cells)
        selected = max(
            wallet_cells,
            key=lambda row: _first_half_rank(
                venue_gate_summary(row)["first_half"]
            ),
        )
        walk_forward_best_by_wallet[wallet] = selected
        for cell in wallet_cells:
            summary = venue_gate_summary(cell)
            first_half = summary["first_half"]
            second_half = summary["second_half"]
            selected_by_first_half = (
                cell["wide_policy_fingerprint"]
                == selected["wide_policy_fingerprint"]
            )
            summary["selected_by_first_half_rule"] = selected_by_first_half
            summary["half_pnl_excluding_top_1_market"] = {
                "first_half": num(first_half.get("pnl_excluding_top_1_market")),
                "second_half": num(second_half.get("pnl_excluding_top_1_market")),
            }
            summary["f1_walk_forward_admissible"] = walk_forward_admissible(
                first_half,
                second_half,
            )
            summary["selection_count_adjustment"] = {
                "cells_tested": cells_tested,
                "raw_second_half_post_fee_pnl_usd": num(
                    second_half.get("post_fee_pnl_usd")
                ),
                "haircut_second_half_post_fee_pnl_usd": round(
                    num(second_half.get("post_fee_pnl_usd"))
                    / max(1, cells_tested),
                    6,
                ),
                "method": "raw_h2_pnl_divided_by_wallet_cells_tested",
                "h1_used_for_selection_only": True,
            }
            advisory = cell["fixed_policy_full_stream_rescore"]
            for key in (
                "selected_by_first_half_rule",
                "half_pnl_excluding_top_1_market",
                "f1_walk_forward_admissible",
                "selection_count_adjustment",
            ):
                advisory[key] = summary[key]
    freeze_overrides = {
        wallet: {
            "wide_policy_fingerprint": cell["wide_policy_fingerprint"],
            "move_slice_keys": cell["identity"]["move_slice_keys"],
            "f1": venue_gate_summary(cell),
            "reason": "source_roster_drought_fingerprint_accrual",
        }
        for wallet, cell in best_by_wallet.items()
        if float(
            venue_gate_summary(cell).get("post_fee_pnl_usd") or 0.0
        )
        > 0
    }
    cells_by_identity = {
        (
            str(cell["identity"]["wallet"]).lower(),
            str(cell["wide_policy_fingerprint"]),
        ): cell
        for cell in cells
    }
    for wallet, fingerprint in DIRECTION_DIRECT_CLIMB_PRIORITY:
        if wallet in freeze_overrides and str(
            freeze_overrides[wallet].get("reason") or ""
        ) == "climb_priority_exact_fp_f1_closed_paper_feedstock":
            continue
        cell = cells_by_identity.get((wallet, fingerprint))
        if not cell:
            continue
        f1 = venue_gate_summary(cell)
        if (
            num(f1.get("post_fee_pnl_usd")) <= 0
            or num(f1.get("first_half_post_fee_pnl_usd")) <= 0
            or num(f1.get("second_half_post_fee_pnl_usd")) <= 0
        ):
            continue
        f1_closed = f1.get("f1_pass") is True
        freeze_overrides[wallet] = {
            "wide_policy_fingerprint": fingerprint,
            "move_slice_keys": cell["identity"]["move_slice_keys"],
            "f1": f1,
            "reason": (
                "climb_priority_exact_fp_f1_closed_paper_feedstock"
                if f1_closed
                else "climb_priority_exact_fp_both_halves_positive_f1_open_paper_feedstock"
            ),
        }
        break
    unique_by_wallet = _unique_source_rows_by_wallet(
        full_stream_by_wallet=full_stream_by_wallet,
        acquired_by_fingerprint=acquired_by_fingerprint,
    )
    atomic_move_slice_rescore = _atomic_move_slice_rescore(
        unique_by_wallet=unique_by_wallet,
        fingerprints=fingerprints,
    )
    venue_discard_decomposition = _order134_b_venue_discard_decomposition(
        unique_by_wallet
    )
    venue_min_order_sweep = _order134_d_venue_min_order_sweep(
        unique_by_wallet=unique_by_wallet,
        fingerprints=fingerprints,
    )
    return {
        "schema_version": 1,
        "kind": "wide_policy_fingerprint_evidence",
        "flow_stage": "LIVE/PROMOTE/LEARN/OBSERVE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "policy_id": POLICY_ID,
        "identity_rule": (
            "sha256(canonical_json(policy_id,wallet,sorted(move_slice_keys),"
            "max_order_usd,min_order_usd,wallet_fraction,max_fill_lag_s,"
            "fee_model_id,selection_rule_id))"
        ),
        "incompatible_policy_id_pooling_allowed": False,
        "logical_order_count": len(orders),
        "manifest_count": len(by_run),
        "fingerprint_cell_count": len(cells),
        "f1_pass_cell_count": sum(
            venue_gate_summary(cell)["f1_pass"] for cell in cells
        ),
        "walk_forward_admissible_count": sum(
            venue_gate_summary(cell).get("f1_walk_forward_admissible") is True
            for cell in cells
        ),
        "venue_reachability_floor_refusal_count": sum(
            venue_gate_summary(cell).get("f1_venue_reachable_admissible")
            is not True
            for cell in cells
        ),
        "source_history_acquisition": {
            "path_authority": authority,
            "rows_seen": len(acquired_rows),
            "rows_admitted_exact_fp": acquired_admitted,
            "source_price_only_rows_admitted": 0,
        },
        "cells": cells,
        "atomic_move_slice_rescore": atomic_move_slice_rescore,
        "order134_b_venue_discard_decomposition": venue_discard_decomposition,
        "order134_d_venue_min_order_sweep": venue_min_order_sweep,
        "best_by_wallet": best_by_wallet,
        "best_by_wallet_rejected_for_concentration": (
            best_by_wallet_rejected_for_concentration
        ),
        "walk_forward_best_by_wallet": walk_forward_best_by_wallet,
        "manifest_wallet_fingerprints": by_manifest_wallet,
        "freeze_overrides": freeze_overrides,
        "quality_bars": {
            "f1_min_resolved": 200,
            "f1_post_fee_pnl_gt": 0,
            "f1_roi_pct_gt": 0,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--manifest-glob", default=DEFAULT_MANIFEST_GLOB)
    parser.add_argument(
        "--source-history-acquisition",
        default=DEFAULT_SOURCE_HISTORY_ACQUISITION,
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    atomic_group = parser.add_mutually_exclusive_group()
    atomic_group.add_argument("--atomic-output", default=DEFAULT_ATOMIC_OUTPUT)
    atomic_group.add_argument("--no-atomic-output", action="store_true")
    sweep_group = parser.add_mutually_exclusive_group()
    sweep_group.add_argument("--sweep-output", default=DEFAULT_SWEEP_OUTPUT)
    sweep_group.add_argument("--no-sweep-output", action="store_true")
    args = parser.parse_args()
    evidence = build_evidence(
        ledger_rows=_jsonl(Path(args.ledger)),
        manifests=sorted(ROOT.glob(args.manifest_glob)),
        resolution_rows=_jsonl(Path(args.resolutions)),
        source_history_acquisition=(
            json.loads(Path(args.source_history_acquisition).read_text())
            if Path(args.source_history_acquisition).exists()
            else None
        ),
    )
    atomic = evidence["atomic_move_slice_rescore"]
    atomic["generated_at"] = evidence["generated_at"]
    atomic["scope"] = {
        "ledger": args.ledger,
        "manifest_glob": args.manifest_glob,
        "source_history_acquisition": args.source_history_acquisition,
        "output": None if args.no_atomic_output else args.atomic_output,
    }
    atomic["wallet_scope"] = (
        "ROSTER_WIDE"
        if args.ledger == DEFAULT_LEDGER and args.manifest_glob == DEFAULT_MANIFEST_GLOB
        else sorted(atomic["by_wallet"])
    )
    decomposition = evidence["order134_b_venue_discard_decomposition"]
    decomposition["generated_at"] = evidence["generated_at"]
    decomposition["scope"] = {
        **atomic["scope"],
        "output": args.output,
    }
    decomposition["wallet_scope"] = atomic["wallet_scope"]
    sweep = evidence["order134_d_venue_min_order_sweep"]
    sweep["generated_at"] = evidence["generated_at"]
    sweep["scope"] = {
        **atomic["scope"],
        "output": None if args.no_sweep_output else args.sweep_output,
    }
    sweep["wallet_scope"] = atomic["wallet_scope"]
    atomic_write_json(args.output, evidence)
    if not args.no_atomic_output:
        atomic_write_json(args.atomic_output, atomic)
    if not args.no_sweep_output:
        atomic_write_json(args.sweep_output, sweep)
    top = evidence["cells"][0] if evidence["cells"] else {}
    print(
        json.dumps(
            {
                "output": args.output,
                "atomic_output": None if args.no_atomic_output else args.atomic_output,
                "sweep_output": None if args.no_sweep_output else args.sweep_output,
                "fingerprint_cells": evidence["fingerprint_cell_count"],
                "f1_pass_cells": evidence["f1_pass_cell_count"],
                "top_wallet": (top.get("identity") or {}).get("wallet"),
                "top_fingerprint": top.get("wide_policy_fingerprint"),
                "top_f1": venue_gate_summary(top),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
