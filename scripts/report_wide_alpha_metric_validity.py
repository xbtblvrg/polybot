#!/usr/bin/env python3
"""Measure whether the WIDE copyable-rate admission metric predicts money."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.alpha_freshness import require_fresh_alpha_report
from scripts.report_pipeline_slo import read_wide_supervisor_heartbeat


def resolve_active_alpha_report(
    *, root: Path = ROOT, pointer_path: str | Path | None = None
) -> str:
    """Resolve the paper-only alpha source consumed by the active WIDE manifest."""
    pointer_file = Path(pointer_path) if pointer_path else (
        root / "data/research/wide_exact_policy_manifest_active.json"
    )
    pointer = load_json(pointer_file, default={}) or {}
    manifest_name = str(pointer.get("manifest_path") or "")
    if not manifest_name:
        raise RuntimeError("ACTIVE_WIDE_MANIFEST_POINTER_MISSING")
    manifest_file = Path(manifest_name)
    if not manifest_file.is_absolute():
        manifest_file = root / manifest_file
    manifest = load_json(manifest_file, default={}) or {}
    if manifest.get("paper_only") is not True or manifest.get("live_orders_allowed") is not False:
        raise RuntimeError("ACTIVE_WIDE_MANIFEST_NOT_PAPER_ONLY")
    alpha_name = str(manifest.get("source_alpha_report") or "")
    if not alpha_name:
        raise RuntimeError("ACTIVE_WIDE_ALPHA_SOURCE_MISSING")
    alpha_file = Path(alpha_name)
    if not alpha_file.is_absolute():
        alpha_file = root / alpha_file
    if not alpha_file.exists():
        raise RuntimeError(f"ACTIVE_WIDE_ALPHA_SOURCE_NOT_FOUND:{alpha_name}")
    alpha = load_json(alpha_file, default={}) or {}
    if alpha.get("paper_only") is not True or alpha.get("live_orders_allowed") is not False:
        raise RuntimeError("ACTIVE_WIDE_ALPHA_SOURCE_NOT_PAPER_ONLY")
    return alpha_name


def _rank(values: list[float]) -> list[float]:
    ordered = sorted(range(len(values)), key=values.__getitem__)
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and values[ordered[end]] == values[ordered[index]]:
            end += 1
        average = (index + 1 + end) / 2.0
        for position in ordered[index:end]:
            ranks[position] = average
        index = end
    return ranks


def pearson(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    numerator = sum(
        (x - left_mean) * (y - right_mean) for x, y in zip(left, right)
    )
    denominator = math.sqrt(
        sum((x - left_mean) ** 2 for x in left)
        * sum((y - right_mean) ** 2 for y in right)
    )
    return round(numerator / denominator, 6) if denominator else None


def spearman(left: list[float], right: list[float]) -> float | None:
    return pearson(_rank(left), _rank(right))


def wilson_lower_bound(successes: int, sample: int, z: float = 1.959963984540054) -> float | None:
    if sample <= 0:
        return None
    proportion = successes / sample
    z2 = z * z
    center = proportion + z2 / (2 * sample)
    margin = z * math.sqrt(
        proportion * (1 - proportion) / sample + z2 / (4 * sample * sample)
    )
    return round(100.0 * (center - margin) / (1 + z2 / sample), 6)


def build_report(
    *,
    alpha: dict[str, Any],
    alpha_path: str,
    alpha_sha256: str,
    temporal: dict[str, Any],
    temporal_path: str,
    prior: dict[str, Any] | None = None,
    producer_heartbeat: dict[str, Any] | None = None,
) -> dict[str, Any]:
    producer_status = str((producer_heartbeat or {}).get("status") or "")
    producer_non_pass = bool(producer_status and producer_status != "PASS")
    profiles = ((alpha.get("execution_profiles") or {}).get("profiles_by_wallet") or {})
    temporal_by_wallet = {
        str(row.get("wallet") or "").lower(): row
        for row in (temporal.get("wallets") or [])
        if isinstance(row, dict) and row.get("wallet")
    }
    rows: list[dict[str, Any]] = []
    for wallet, profile in sorted(profiles.items()):
        if not isinstance(profile, dict):
            continue
        fill_sample = int(profile.get("fill_sample") or 0)
        raw_coverage = int(profile.get("raw_fill_coverage") or 0)
        stale = int(profile.get("stale_or_missing_book_observations") or 0)
        censored_rate = float(profile.get("copyable_rate_pct") or 0.0)
        copyable_samples = int(round(censored_rate * fill_sample / 100.0))
        weekday_row = temporal_by_wallet.get(str(wallet).lower(), {})
        weekday = (weekday_row.get("regime_profiles") or {}).get("weekday") or {}
        weekday_label = (weekday_row.get("slice_labels") or {}).get("weekday") or {}
        rows.append(
            {
                "wallet": str(wallet).lower(),
                "copyable_samples": copyable_samples,
                "copyable_rate_pct_censored": round(censored_rate, 6),
                "copyable_rate_pct_raw": (
                    round(100.0 * copyable_samples / raw_coverage, 6)
                    if raw_coverage
                    else None
                ),
                "fill_sample": fill_sample,
                "raw_fill_coverage": raw_coverage,
                "stale_or_missing_book_observations": stale,
                "book_censorship_pct": (
                    round(100.0 * stale / raw_coverage, 6)
                    if raw_coverage
                    else None
                ),
                "copyable_rate_wilson_95_lcb_pct": wilson_lower_bound(
                    copyable_samples, fill_sample
                ),
                "copyable_rate_bar_pct": 70.0,
                "wilson_lcb_clears_bar": bool(
                    fill_sample
                    and (wilson_lower_bound(copyable_samples, fill_sample) or 0.0)
                    >= 70.0
                ),
                "mean_edge": profile.get("mean_edge"),
                "median_edge": profile.get("median_edge"),
                "eligible_move_slice_count": int(
                    profile.get("eligible_move_slice_count") or 0
                ),
                "eligible": profile.get("eligible") is True,
                "weekday_roi_pct": weekday.get("roi_pct"),
                "weekday_pnl_usd": weekday.get("pnl_usd"),
                "weekday_resolved_trades": weekday.get("resolved_trades"),
                "weekday_label": weekday_label.get("label"),
            }
        )

    joined = [row for row in rows if row["weekday_roi_pct"] is not None]
    copyable = [float(row["copyable_rate_pct_censored"]) for row in joined]
    roi = [float(row["weekday_roi_pct"]) for row in joined]
    censorship = [float(row["book_censorship_pct"]) for row in joined]
    correlations = {
        "copyable_rate_pct_censored_vs_weekday_roi_pct": {
            "n": len(joined),
            "pearson_r": pearson(copyable, roi),
            "spearman_rho": spearman(copyable, roi),
        },
        "book_censorship_pct_vs_copyable_rate_pct_censored": {
            "n": len(joined),
            "pearson_r": pearson(censorship, copyable),
            "spearman_rho": spearman(censorship, copyable),
        },
        "copyable_rate_pct_raw_vs_weekday_roi_pct": {
            "n": len(joined),
            "pearson_r": pearson(
                [float(row["copyable_rate_pct_raw"]) for row in joined], roi
            ),
            "spearman_rho": spearman(
                [float(row["copyable_rate_pct_raw"]) for row in joined], roi
            ),
        },
        "book_censorship_pct_vs_weekday_roi_pct": {
            "n": len(joined),
            "pearson_r": pearson(censorship, roi),
            "spearman_rho": spearman(censorship, roi),
        },
    }
    cut = {
        "cut_id": hashlib.sha256(
            f"{alpha_sha256}|{alpha.get('updated_at')}".encode()
        ).hexdigest()[:24],
        "alpha_report": alpha_path,
        "alpha_sha256": alpha_sha256,
        "alpha_updated_at": alpha.get("updated_at"),
        "profile_count": len(rows),
        "weekday_joined_count": len(joined),
        "total_fill_sample": sum(row["fill_sample"] for row in rows),
        "producer_heartbeat_status": producer_status or None,
        "correlations": correlations,
    }
    prior_cuts = prior.get("independent_cuts") if isinstance(prior, dict) else []
    cuts = {
        str(row.get("cut_id")): row
        for row in (prior_cuts or [])
        if isinstance(row, dict) and row.get("cut_id")
    }
    cuts[cut["cut_id"]] = cut
    independent_cuts = sorted(
        cuts.values(),
        key=lambda row: (
            str(row.get("alpha_updated_at") or ""),
            str(row.get("cut_id") or ""),
        ),
    )
    qualified: list[dict[str, Any]] = []
    newest_qualifying_total = -1
    for row in independent_cuts:
        row_correlations = row.get("correlations") or {}
        total_fill_sample = int(row.get("total_fill_sample") or 0)
        minimum_sample_pass = (
            int(row.get("weekday_joined_count") or 0) >= 12
            and (row_correlations.get(
                "copyable_rate_pct_censored_vs_weekday_roi_pct"
            ) or {}).get("pearson_r") is not None
            and (row_correlations.get(
                "copyable_rate_pct_raw_vs_weekday_roi_pct"
            ) or {}).get("pearson_r") is not None
        )
        growth_pass = total_fill_sample > newest_qualifying_total
        row_producer_status = str(row.get("producer_heartbeat_status") or "")
        producer_heartbeat_pass = not row_producer_status or row_producer_status == "PASS"
        row["qualification"] = {
            "minimum_sample_pass": minimum_sample_pass,
            "strict_total_fill_sample_growth_pass": growth_pass,
            "producer_heartbeat_pass": producer_heartbeat_pass,
            "producer_status": row_producer_status or None,
            "prior_qualifying_total_fill_sample": (
                newest_qualifying_total if newest_qualifying_total >= 0 else None
            ),
            "disqualifier": (
                "PRODUCER_NON_PASS_AT_CUT" if not producer_heartbeat_pass else None
            ),
            "qualifies": minimum_sample_pass and growth_pass and producer_heartbeat_pass,
        }
        if minimum_sample_pass and growth_pass and producer_heartbeat_pass:
            qualified.append(row)
            newest_qualifying_total = total_fill_sample
    censored_by_cut = [
        float(row["correlations"]["copyable_rate_pct_censored_vs_weekday_roi_pct"]["pearson_r"])
        for row in qualified
    ]
    raw_by_cut = [
        float(row["correlations"]["copyable_rate_pct_raw_vs_weekday_roi_pct"]["pearson_r"])
        for row in qualified
    ]
    if len(qualified) < 2:
        falsifier_status = "AWAITING_SECOND_INDEPENDENT_CUT"
        gate_disposition = "HOLD_UNCHANGED_PENDING_EVIDENCE"
    elif all(value <= 0.0 for value in censored_by_cut[-2:]) and all(
        value <= 0.0 for value in raw_by_cut[-2:]
    ):
        falsifier_status = "FALSIFIED_DEMOTE_TO_REPORTED_DIAGNOSTIC"
        gate_disposition = "EVIDENCE_SUPPORTS_DEMOTION_NOT_APPLIED_BY_THIS_REPORT"
    elif all(value <= 0.0 for value in censored_by_cut[-2:]) and all(
        value > 0.0 for value in raw_by_cut[-2:]
    ):
        falsifier_status = "DENOMINATOR_DEFECT_REPAIR_RAW_BASIS_AND_REMEASURE"
        gate_disposition = "HOLD_70_UNCHANGED_REPAIR_BASIS_NOT_APPLIED_BY_THIS_REPORT"
    elif all(value > 0.3 for value in censored_by_cut[-2:]) and all(
        value > 0.3 for value in raw_by_cut[-2:]
    ):
        falsifier_status = "VINDICATED_HOLD_UNCHANGED"
        gate_disposition = "HOLD_UNCHANGED_STOP_RELITIGATION"
    else:
        falsifier_status = "INCONCLUSIVE_HOLD_UNCHANGED"
        gate_disposition = "HOLD_UNCHANGED_PENDING_STRONGER_EVIDENCE"
    if producer_non_pass:
        falsifier_status = "PRODUCER_NON_PASS_AT_CUT"
        gate_disposition = "HOLD_UNCHANGED_RESTORE_CUT_PRODUCER"

    return {
        "schema_version": 1,
        "kind": "wide_alpha_metric_validity",
        "flow_stage": "LIVE/PROMOTE/LEARN",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_authority": False,
        "gate_mutated": False,
        "threshold_mutated": False,
        "source": {
            "alpha_report": alpha_path,
            "alpha_sha256": alpha_sha256,
            "temporal_profitability": temporal_path,
        },
        "preregistered_falsifier": {
            "minimum_independent_cuts": 2,
            "minimum_profiled_wallets_per_cut": 12,
            "independence_rule": (
                "weekday_joined_count >= 12 and total_fill_sample strictly greater "
                "than the newest prior qualifying cut"
            ),
            "demote_when": (
                "censored-vs-weekday-ROI Pearson <= 0 AND raw-vs-weekday-ROI "
                "Pearson <= 0 on both qualifying cuts"
            ),
            "repair_denominator_when": (
                "censored-vs-weekday-ROI Pearson <= 0 AND raw-vs-weekday-ROI "
                "Pearson > 0 on both qualifying cuts"
            ),
            "vindicate_when": (
                "censored-vs-weekday-ROI Pearson > +0.3 AND raw-vs-weekday-ROI "
                "Pearson > +0.3 on both qualifying cuts"
            ),
            "status": falsifier_status,
            "gate_disposition": gate_disposition,
            "gate_change_applied": False,
            "producer_heartbeat": producer_heartbeat or {},
        },
        "independent_cuts": independent_cuts,
        "correlations": correlations,
        "rows": rows,
        "summary": {
            "profile_count": len(rows),
            "weekday_joined_count": len(joined),
            "eligible_profile_count": sum(row["eligible"] for row in rows),
            "wilson_lcb_clears_70_count": sum(
                row["wilson_lcb_clears_bar"] for row in rows
            ),
            "independent_cut_count": len(independent_cuts),
            "qualifying_independent_cut_count": len(qualified),
            "total_fill_sample": sum(row["fill_sample"] for row in rows),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alpha-report")
    parser.add_argument(
        "--temporal-profitability",
        default="data/research/wallet_temporal_profitability_latest.json",
    )
    parser.add_argument(
        "--prior", default="data/research/wide_alpha_metric_validity_latest.json"
    )
    parser.add_argument(
        "--output", default="data/research/wide_alpha_metric_validity_latest.json"
    )
    args = parser.parse_args()
    active_alpha_report = resolve_active_alpha_report()
    if args.alpha_report and Path(args.alpha_report).resolve() != Path(active_alpha_report).resolve():
        raise RuntimeError(
            f"ALPHA_REPORT_NOT_ACTIVE_MANIFEST_SOURCE:{args.alpha_report}!={active_alpha_report}"
        )
    alpha_report = args.alpha_report or active_alpha_report
    alpha_raw = Path(alpha_report).read_bytes()
    alpha = json.loads(alpha_raw)
    require_fresh_alpha_report(alpha, path=alpha_report, max_age_h=24.0)
    temporal = json.loads(Path(args.temporal_profitability).read_text())
    prior_path = Path(args.prior)
    prior = json.loads(prior_path.read_text()) if prior_path.exists() else {}
    producer_heartbeat = read_wide_supervisor_heartbeat()
    pipeline_slo = load_json(
        "data/research/pipeline_slo_and_standby_readiness_latest.json", default={}
    ) or {}
    pipeline_heartbeat = pipeline_slo.get("wide_supervisor_heartbeat") or {}
    if pipeline_heartbeat.get("status") != producer_heartbeat.get("status"):
        raise RuntimeError(
            "WIDE_PRODUCER_HEARTBEAT_PARITY_MISMATCH: "
            f"pipeline={pipeline_heartbeat.get('status')} "
            f"persisted={producer_heartbeat.get('status')}"
        )
    report = build_report(
        alpha=alpha,
        alpha_path=alpha_report,
        alpha_sha256=hashlib.sha256(alpha_raw).hexdigest(),
        temporal=temporal,
        temporal_path=args.temporal_profitability,
        prior=prior,
        producer_heartbeat=producer_heartbeat,
    )
    report["producer_heartbeat_parity"] = {
        "status": "PASS",
        "pipeline_status": pipeline_heartbeat.get("status"),
        "persisted_status": producer_heartbeat.get("status"),
        "rule": "validity and pipeline SLO consume the same persisted single-writer verdict",
    }
    atomic_write_json(args.output, report)
    print(json.dumps({"output": args.output, **report["summary"], "falsifier": report["preregistered_falsifier"]["status"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
