#!/usr/bin/env python3
"""Join current WIDE alpha and exact-policy paper evidence per wallet."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from glob import glob
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _checksum(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _fingerprint(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if len(text) == 64 and all(ch in "0123456789abcdef" for ch in text) else ""


def _run_generated_key(report: dict[str, Any]) -> tuple[str, str]:
    cohort = report.get("prospective_cohort") if isinstance(report.get("prospective_cohort"), dict) else {}
    run_id = str(cohort.get("run_id") or "")
    generated_at = str(report.get("generated_at") or "")
    return run_id, generated_at


def _row_counters(row: dict[str, Any]) -> dict[str, float | int]:
    return {
        "attempted_exact_policy_buys": int(
            row.get("attempted_exact_policy_buys")
            if row.get("attempted_exact_policy_buys") is not None
            else row.get("attempted_buy_events")
            or 0
        ),
        "copyable_exact_policy_buys": int(
            row.get("copyable_exact_policy_buys")
            if row.get("copyable_exact_policy_buys") is not None
            else row.get("copyable_buy_events")
            or 0
        ),
        "paper_resolved_orders": int(
            row.get("paper_resolved_orders")
            if row.get("paper_resolved_orders") is not None
            else row.get("resolved_orders")
            or 0
        ),
        "fees_usd": round(num(row.get("fees_usd")), 6),
        "post_fee_pnl_usd": round(num(row.get("post_fee_pnl_usd")), 6),
    }


def _latest_history_rows_by_run(
    history_reports: list[dict[str, Any]],
    *,
    current_run_id: str,
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], dict[str, Any]]:
    """Return latest prior per-run rows keyed by wallet/fingerprint/run.

    Same-run snapshots are excluded because prospective paper counters already
    accumulate within a run. Across runs, each wallet/fingerprint basin carries
    forward one latest snapshot per run id.
    """
    selected: dict[tuple[str, str, str], tuple[str, dict[str, Any]]] = {}
    skipped = Counter()
    for report in history_reports:
        if not isinstance(report, dict) or report.get("kind") != "wide_candidate_exact_policy_standings":
            skipped["not_standings_report"] += 1
            continue
        run_id, generated_at = _run_generated_key(report)
        if not run_id or run_id == current_run_id:
            skipped["same_or_missing_run_id"] += len(report.get("standings") or [])
            continue
        for row in report.get("standings") or []:
            if not isinstance(row, dict):
                skipped["non_object_row"] += 1
                continue
            wallet = _wallet(row.get("wallet"))
            fingerprint = _fingerprint(row.get("wide_policy_fingerprint"))
            if not wallet or not fingerprint:
                skipped["missing_wallet_or_fingerprint"] += 1
                continue
            key = (wallet, fingerprint, run_id)
            prior_generated, _ = selected.get(key, ("", {}))
            if str(generated_at) >= str(prior_generated):
                selected[key] = (str(generated_at), row)
    return {key: row for key, (_, row) in selected.items()}, dict(sorted(skipped.items()))


def _cumulative_for(
    *,
    wallet: str,
    fingerprint: str,
    current_run_id: str,
    current_generated_at: str,
    current: dict[str, float | int],
    prior_rows: dict[tuple[str, str, str], dict[str, Any]],
) -> dict[str, Any]:
    totals = dict(current)
    run_ids = [current_run_id] if current_run_id else []
    generated_ats = [current_generated_at] if current_generated_at else []
    for (prior_wallet, prior_fingerprint, prior_run_id), row in sorted(prior_rows.items()):
        if prior_wallet != wallet or prior_fingerprint != fingerprint:
            continue
        run_ids.append(prior_run_id)
        if row.get("generated_at") is not None:
            generated_ats.append(str(row.get("generated_at")))
        prior = _row_counters(row)
        for key, value in prior.items():
            totals[key] = round(num(totals.get(key)) + num(value), 6)
    first_run = sorted(set(run_ids))[0] if run_ids else None
    return {
        "cumulative_attempted_exact_policy_buys": int(totals["attempted_exact_policy_buys"]),
        "cumulative_copyable_exact_policy_buys": int(totals["copyable_exact_policy_buys"]),
        "cumulative_paper_resolved_orders": int(totals["paper_resolved_orders"]),
        "cumulative_fees_usd": round(num(totals["fees_usd"]), 6),
        "cumulative_post_fee_pnl_usd": round(num(totals["post_fee_pnl_usd"]), 6),
        "cumulative_first_run_id": first_run,
        "cumulative_run_count": len(set(run_ids)),
        "cumulative_since": min(generated_ats) if generated_ats else None,
        "cumulative_basis": "wallet|wide_policy_fingerprint; one latest snapshot per prior run id plus current per-run counters",
        "cumulative_admission_authority": False,
    }


def load_cumulative_history(patterns: list[str], *, output: str = "") -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    seen: set[Path] = set()
    output_path = Path(output).resolve() if output else None
    for pattern in patterns:
        for raw in sorted(glob(pattern)):
            path = Path(raw)
            try:
                resolved = path.resolve()
            except OSError:
                continue
            if resolved in seen or (output_path is not None and resolved == output_path):
                continue
            seen.add(resolved)
            report = load_json(path, default={})
            if isinstance(report, dict):
                reports.append(report)
    return reports


def _slice_matrix(
    measurement: dict[str, Any],
    profiles: dict[str, Any],
    capture_slices: dict[str, set[str]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for wallet, slice_keys in capture_slices.items():
        for slice_key in slice_keys:
            grouped[(wallet, slice_key)] = []
    seen: set[str] = set()
    for order in measurement.get("orders") or []:
        if not isinstance(order, dict) or order.get("resolved") is not True:
            continue
        wallet = _wallet(order.get("wallet"))
        move_slice = (
            order.get("alpha_move_slice")
            if isinstance(order.get("alpha_move_slice"), dict)
            else {}
        )
        slice_key = str(move_slice.get("move_slice_key") or "")
        order_id = str(order.get("order_id") or "")
        if not wallet or wallet not in capture_slices or not slice_key or not order_id:
            continue
        if order_id in seen:
            continue
        seen.add(order_id)
        grouped.setdefault((wallet, slice_key), []).append(order)
    rows: list[dict[str, Any]] = []
    for (wallet, slice_key), orders in sorted(grouped.items()):
        orders.sort(key=lambda row: (str(row.get("recorded_at") or ""), str(row.get("order_id") or "")))
        midpoint = len(orders) // 2
        first, second = orders[:midpoint], orders[midpoint:]
        pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in orders), 6)
        first_pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in first), 6)
        second_pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in second), 6)
        lags = sorted(num(row.get("receipt_to_book_fetch_lag_s")) for row in orders)
        p95_lag = lags[min(len(lags) - 1, int(0.95 * (len(lags) - 1)))] if lags else None
        profile = profiles.get(wallet) if isinstance(profiles.get(wallet), dict) else {}
        profile_slices = profile.get("move_slices") if isinstance(profile.get("move_slices"), list) else []
        alpha_slice = next(
            (
                row
                for row in profile_slices
                if isinstance(row, dict) and row.get("move_slice_key") == slice_key
            ),
            {},
        )
        seconds_bucket, _, entry_price_band = slice_key.partition("|")
        gates = {
            "resolved_nonzero": bool(orders),
            "post_fee_positive": pnl > 0.0,
            "first_half_positive": bool(first) and first_pnl > 0.0,
            "second_half_positive": bool(second) and second_pnl > 0.0,
            "fees_measured": all(row.get("expected_fee_usd") is not None for row in orders),
            "receipt_lag_lte_5s": bool(lags) and max(lags) <= 5.0,
            "current_alpha_slice_eligible": alpha_slice.get("eligible") is True,
            "entry_band_025_050": str(
                ((orders[0].get("alpha_move_slice") or {}).get("entry_price_band"))
                if orders
                else entry_price_band
            )
            == "0.25-0.50",
        }
        rows.append(
            {
                "wallet": wallet,
                "move_slice_key": slice_key,
                "seconds_bucket": (
                    (orders[0].get("alpha_move_slice") or {}).get("seconds_bucket")
                    if orders
                    else seconds_bucket
                ),
                "entry_price_band": (
                    (orders[0].get("alpha_move_slice") or {}).get("entry_price_band")
                    if orders
                    else entry_price_band
                ),
                "resolved_orders": len(orders),
                "post_fee_pnl_usd": pnl,
                "first_half": {
                    "resolved_orders": len(first),
                    "post_fee_pnl_usd": first_pnl,
                },
                "second_half": {
                    "resolved_orders": len(second),
                    "post_fee_pnl_usd": second_pnl,
                },
                "receipt_lag_p95_s": round(p95_lag, 6) if p95_lag is not None else None,
                "checks": gates,
                "positive_seed": all(gates.values()),
                "order_ids_sha256": _checksum([row.get("order_id") for row in orders]),
            }
        )
    return rows


def build_standings(
    copyability: dict[str, Any],
    alpha: dict[str, Any],
    measurement: dict[str, Any],
    manifest: dict[str, Any] | None = None,
    cumulative_history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    mutable_queue = [
        row
        for row in copyability.get("ranked_queue") or []
        if isinstance(row, dict) and row.get("admission_status") == "READY_QUEUE"
    ]
    manifest = manifest if isinstance(manifest, dict) else {}
    manifest_capture = [
        row
        for row in manifest.get("capture_watch_wallets") or []
        if isinstance(row, dict) and _wallet(row.get("wallet"))
    ]
    queue_by_wallet = {
        _wallet(row.get("wallet")): row for row in mutable_queue if _wallet(row.get("wallet"))
    }
    queue = (
        [
            {
                **queue_by_wallet.get(_wallet(row.get("wallet")), {}),
                **row,
                "wallet": _wallet(row.get("wallet")),
            }
            for row in manifest_capture
        ]
        if manifest_capture
        else mutable_queue
    )
    profiles_state = alpha.get("execution_profiles") if isinstance(alpha.get("execution_profiles"), dict) else {}
    profiles = profiles_state.get("profiles_by_wallet") if isinstance(profiles_state.get("profiles_by_wallet"), dict) else {}
    measured = measurement.get("wallets") if isinstance(measurement.get("wallets"), dict) else {}
    freshness = alpha.get("source_freshness") if isinstance(alpha.get("source_freshness"), dict) else {}
    alpha_current = (
        alpha.get("status") == "PASS_CURRENT_SOURCE"
        and freshness.get("pass") is True
        and freshness.get("history_is_frozen_d97") is False
    )
    policy_id = str(measurement.get("policy_id") or "")
    prospective = measurement.get("kind") == "wide_exact_policy_prospective_paper_state"
    cohort = measurement.get("cohort") if isinstance(measurement.get("cohort"), dict) else {}
    current_run_id = str(cohort.get("run_id") or manifest.get("score_run_id") or "")
    current_generated_at = utc_now_iso()
    manifest_authoritative = bool(
        manifest_capture
        and str(manifest.get("manifest_id") or "") == str(cohort.get("manifest_id") or "")
        and str(manifest.get("score_run_id") or "") == str(cohort.get("run_id") or "")
    )
    prior_rows, cumulative_skipped = _latest_history_rows_by_run(
        cumulative_history or [],
        current_run_id=current_run_id,
    )
    standings: list[dict[str, Any]] = []
    refusal_counts: Counter[str] = Counter()
    for queue_row in queue:
        wallet = _wallet(queue_row.get("wallet"))
        wide_policy_fingerprint = _fingerprint(queue_row.get("wide_policy_fingerprint"))
        paper = measured.get(wallet) if isinstance(measured.get(wallet), dict) else {}
        profile = profiles.get(wallet) if isinstance(profiles.get(wallet), dict) else {}
        attempted = int(
            (paper.get("attempted_exact_policy_buys") or 0)
            if prospective
            else paper.get("buy_events")
            or 0
        )
        copyable = int(
            (paper.get("copyable_exact_policy_buys") or 0)
            if prospective
            else paper.get("copyable_buy_events")
            or 0
        )
        copyable_rate = num(paper.get("copyable_rate_pct")) if attempted else None
        resolved = int(paper.get("paper_resolved_orders") or paper.get("resolved_orders") or 0)
        fees = paper.get("fees_usd")
        post_fee_pnl = paper.get("post_fee_pnl_usd")
        receipt_lag_s = paper.get("max_receipt_to_book_fetch_lag_s") if prospective else None
        if not prospective and paper.get("last_observed_ts") is not None and paper.get("last_event_ts") is not None:
            receipt_lag_s = round(
                max(0.0, num(paper.get("last_observed_ts")) - num(paper.get("last_event_ts"))),
                6,
            )
        gates = {
            "manifest_identity_exact": manifest_authoritative if manifest_capture else True,
            "current_alpha_pass": bool(alpha_current and profile.get("eligible") is True),
            "prospective_exact_policy_cohort": prospective,
            "copyable_rate_gte_70": bool(prospective and copyable_rate is not None and copyable_rate >= 70.0),
            "resolved_gte_50": bool(prospective and resolved >= 50),
            "receipt_lag_lte_5s": bool(
                prospective
                and paper.get("all_fill_lags_lte_5s") is True
                and receipt_lag_s is not None
                and receipt_lag_s <= 5.0
            ),
            "fees_measured": bool(
                prospective
                and resolved > 0
                and int(paper.get("fee_covered_resolved_orders") or 0) == resolved
            ),
            "post_fee_pnl_positive": bool(post_fee_pnl is not None and num(post_fee_pnl) > 0.0),
            "first_half_post_fee_positive": bool(
                paper.get("first_half_post_fee_pnl_usd") is not None
                and num(paper.get("first_half_post_fee_pnl_usd")) > 0.0
            ),
            "second_half_post_fee_positive": bool(
                paper.get("second_half_post_fee_pnl_usd") is not None
                and num(paper.get("second_half_post_fee_pnl_usd")) > 0.0
            ),
        }
        for gate, passed in gates.items():
            if not passed:
                refusal_counts[gate] += 1
        current_counters = {
            "attempted_exact_policy_buys": attempted,
            "copyable_exact_policy_buys": copyable,
            "paper_resolved_orders": resolved,
            "fees_usd": round(num(fees), 6),
            "post_fee_pnl_usd": round(num(post_fee_pnl), 6),
        }
        cumulative = (
            _cumulative_for(
                wallet=wallet,
                fingerprint=wide_policy_fingerprint,
                current_run_id=current_run_id,
                current_generated_at=current_generated_at,
                current=current_counters,
                prior_rows=prior_rows,
            )
            if wide_policy_fingerprint
            else {
                "cumulative_attempted_exact_policy_buys": attempted,
                "cumulative_copyable_exact_policy_buys": copyable,
                "cumulative_paper_resolved_orders": resolved,
                "cumulative_fees_usd": round(num(fees), 6),
                "cumulative_post_fee_pnl_usd": round(num(post_fee_pnl), 6),
                "cumulative_first_run_id": current_run_id or None,
                "cumulative_run_count": 1 if current_run_id else 0,
                "cumulative_since": current_generated_at,
                "cumulative_basis": "current_per_run_only_missing_wide_policy_fingerprint",
                "cumulative_admission_authority": False,
            }
        )
        standings.append(
            {
                "wallet": wallet,
                "wide_policy_fingerprint": wide_policy_fingerprint or None,
                "queue_rank": int(queue_row.get("queue_rank") or 0),
                "policy_id": policy_id,
                "alpha_profile_status": str(profile.get("status") or "MISSING"),
                "alpha_profile_eligible": profile.get("eligible") is True,
                "alpha_fill_sample": int(profile.get("fill_sample") or 0),
                "alpha_copyable_rate_pct": profile.get("copyable_rate_pct"),
                "alpha_mean_edge": profile.get("mean_edge"),
                "alpha_move_slices": profile.get("move_slices") if isinstance(profile.get("move_slices"), list) else [],
                "attempted_buy_events": attempted,
                "copyable_buy_events": copyable,
                "copyable_rate_pct": copyable_rate,
                "resolved_orders": resolved,
                "attempted_exact_policy_buys": attempted,
                "copyable_exact_policy_buys": copyable,
                "paper_resolved_orders": resolved,
                "per_run_leg_present": True,
                "fees_usd": fees,
                "post_fee_pnl_usd": post_fee_pnl,
                "mark_to_market_paper_pnl_usd": paper.get("paper_pnl_usd"),
                "receipt_lag_s": receipt_lag_s,
                "receipt_lag_p95_s": paper.get("p95_receipt_to_book_fetch_lag_s"),
                "fee_coverage_pct": paper.get("fee_coverage_pct"),
                "first_half_post_fee_pnl_usd": paper.get("first_half_post_fee_pnl_usd"),
                "second_half_post_fee_pnl_usd": paper.get("second_half_post_fee_pnl_usd"),
                "refusal_counts": paper.get("refusal_counts") if prospective else {},
                "prospective_cohort_id": cohort.get("cohort_id") if prospective else None,
                "prospective_run_id": cohort.get("run_id") if prospective else None,
                "gates": gates,
                "dual_gate_winner_basis": "per_run_gates_only_cumulative_reporting_forbidden",
                "dual_gate_winner": all(gates.values()),
                **cumulative,
            }
        )
    standings.sort(
        key=lambda row: (
            0 if row["dual_gate_winner"] else 1,
            -int(row["copyable_buy_events"]),
            -num(row["mark_to_market_paper_pnl_usd"], -1_000_000_000.0),
            int(row["queue_rank"]),
        )
    )
    winners = [row["wallet"] for row in standings if row["dual_gate_winner"]]
    capture_wallets = {row["wallet"] for row in standings}
    capture_slices = {
        _wallet(row.get("wallet")): {
            str(key)
            for key in row.get("move_slice_keys") or []
            if str(key)
        }
        for row in queue
        if _wallet(row.get("wallet"))
    }
    slice_matrix = _slice_matrix(measurement, profiles, capture_slices)
    reconciliation = {
        "manifest_id": manifest.get("manifest_id") if manifest_capture else None,
        "run_id": cohort.get("run_id"),
        "cohort_id": cohort.get("cohort_id"),
        "capture_watch_wallets": len(manifest_capture) if manifest_capture else len(queue),
        "promotion_admitted_wallets": len(manifest.get("promotion_admitted_wallets") or [])
        if manifest_capture
        else None,
        "manifest_identity_exact": manifest_authoritative if manifest_capture else None,
        "wallets_sha256": _checksum(sorted(capture_wallets)),
    }
    reconciliation["checksum"] = _checksum(reconciliation)
    return {
        "schema_version": 1,
        "kind": "wide_candidate_exact_policy_standings",
        "flow_stage": "LEARN/OBSERVE/PROMOTE",
        "generated_at": current_generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "alpha_status": alpha.get("status"),
        "source_freshness": freshness,
        "policy_id": policy_id,
        "prospective_exact_policy_cohort": prospective,
        "prospective_cohort": cohort if prospective else {},
        "diagnostic_broad_history_is_admission_evidence": False,
        "summary": {
            "ready_queue_wallets": len(queue),
            "mutable_ready_queue_wallets": len(mutable_queue),
            "capture_watch_wallets": len(manifest_capture) if manifest_capture else len(queue),
            "promotion_admitted_wallets": len(manifest.get("promotion_admitted_wallets") or [])
            if manifest_capture
            else None,
            "measured_wallets": len(standings),
            "wallets_with_attempts": sum(row["attempted_buy_events"] > 0 for row in standings),
            "attempted_buy_events": sum(row["attempted_buy_events"] for row in standings),
            "copyable_buy_events": sum(row["copyable_buy_events"] for row in standings),
            "resolved_orders": sum(row["resolved_orders"] for row in standings),
            "cumulative_attempted_exact_policy_buys": sum(
                row["cumulative_attempted_exact_policy_buys"] for row in standings
            ),
            "cumulative_copyable_exact_policy_buys": sum(
                row["cumulative_copyable_exact_policy_buys"] for row in standings
            ),
            "cumulative_paper_resolved_orders": sum(
                row["cumulative_paper_resolved_orders"] for row in standings
            ),
            "cumulative_post_fee_pnl_usd": round(
                sum(num(row["cumulative_post_fee_pnl_usd"]) for row in standings),
                6,
            ),
            "dual_gate_winners": len(winners),
            "dual_gate_winner_basis": "per_run_only_cumulative_fields_report_only",
            "positive_slice_seeds": sum(row["positive_seed"] for row in slice_matrix),
            "refusal_counts": dict(sorted(refusal_counts.items())),
            "counter_delta_interpretation_rule": "Only compare counters within the same (run_id, generated_at) lineage; differing run_id means basin reset and undefined delta.",
            "cumulative_counter_basis": "wallet|wide_policy_fingerprint across prior run ids; cumulative fields are report-only and do not feed dual_gate_winner",
            "cumulative_history_skipped_rows": cumulative_skipped,
        },
        "winner_wallets": winners,
        "manifest_reconciliation": reconciliation,
        "slice_failure_matrix": slice_matrix,
        "standings": standings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--copyability",
        default="data/research/wallet_copy_full_universe_copyability_latest.json",
    )
    parser.add_argument("--manifest", default="")
    parser.add_argument("--alpha", required=True)
    parser.add_argument(
        "--measurement",
        default="data/research/wallet_copy_full_pool_broad_paper_measurement_state.json",
    )
    parser.add_argument(
        "--output",
        default="data/research/wide_candidate_standings_latest.json",
    )
    parser.add_argument(
        "--cumulative-history-glob",
        action="append",
        default=["data/research/wide_candidate_standings_*.json"],
        help="Prior run-stamped standings files used for report-only cumulative counters.",
    )
    args = parser.parse_args()
    report = build_standings(
        load_json(args.copyability, default={}),
        load_json(args.alpha, default={}),
        load_json(args.measurement, default={}),
        load_json(args.manifest, default={}) if args.manifest else {},
        load_cumulative_history(args.cumulative_history_glob, output=args.output),
    )
    atomic_write_json(args.output, report)
    print(json.dumps({"output": args.output, **report["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
