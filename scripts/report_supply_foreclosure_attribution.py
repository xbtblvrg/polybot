#!/usr/bin/env python3
"""Publish the two dominant BTC-5m supply foreclosures with code provenance."""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.models import num
from src.wallet_copy.performance import load_resolutions, score_order


DEFAULT_INPUT = "data/research/coverage_gap_diagnosis_latest.json"
DEFAULT_OUTPUT = "data/research/supply_foreclosure_attribution_latest.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
MONEY_PREDICATES = (
    "inventory_residual_gap_below_min_order",
    "drip_min_tranche_exceeds_window_budget",
    "hard_entry_floor_skip",
)

PROVENANCE = {
    "no_eligible_signal": {
        "classification_source": "scripts/report_coverage_gap_diagnosis.py::classify_zero_submission",
        "evidence_source": "scripts/report_coverage_gap_diagnosis.py::abstention_evidence",
        "meaning": "no retained guard rollup or selected active-roster history signal established eligibility",
    },
    "inventory_confirmed_unchanged_no_edge": {
        "classification_source": "scripts/run_wallet_copy_live_guard.py::_merge_window_participation",
        "incident_taxonomy_source": "src/wallet_copy/participation.py::CORRECT_SKIP_REASONS",
        "meaning": "fresh premerge watermark confirmed unchanged inventory and no executable incremental edge",
    },
}


def _row_price(row: dict[str, Any]) -> float:
    price = num(row.get("source_inventory_vwap"))
    if price > 0:
        return price
    target_cost = num(row.get("target_usd_at_vwap"))
    target_shares = num(row.get("target_shares"))
    return target_cost / target_shares if target_cost > 0 and target_shares > 0 else 0.0


def _row_cost(row: dict[str, Any]) -> float:
    for key in ("guard_sized_copy_usd", "target_usd_at_vwap", "gap_usd_at_vwap"):
        value = num(row.get(key))
        if value > 0:
            return value
    return 0.0


def _score_generation_row(
    row: dict[str, Any], resolutions: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    price = _row_price(row)
    cost = _row_cost(row)
    window_start_s = int(num(row.get("window_start_s")))
    day_utc = (
        datetime.fromtimestamp(window_start_s, UTC).date().isoformat()
        if window_start_s > 0
        else None
    )
    score = {}
    if price > 0 and cost > 0:
        shares = cost / price
        score = score_order(
            {
                "order_id": f"supply-foreclosure-{window_start_s}-{row.get('source_wallet')}",
                "condition_id": row.get("condition_id"),
                "market_slug": row.get("market_slug"),
                "outcome": row.get("outcome"),
                "status": "FILLED",
                "final_status": "FILLED",
                "filled_size_usd": cost,
                "filled_shares": shares,
                "limit_price": price,
            },
            resolutions,
        )
    return {
        "predicate": row.get("dominant_skip_reason"),
        "day_utc": day_utc,
        "market_slug": row.get("market_slug"),
        "source_wallet": row.get("source_wallet"),
        "set_generation_id": row.get("set_generation_id"),
        "window_start_s": window_start_s or None,
        "outcome": row.get("outcome"),
        "counterfactual_price": round(price, 8) if price > 0 else None,
        "counterfactual_cost_usd": round(cost, 6) if cost > 0 else None,
        "resolved": bool(score.get("resolved")),
        "would_have_won": score.get("win"),
        "would_be_realized_pnl_usd": (
            round(num(score.get("pnl_usd")), 6) if score.get("resolved") else None
        ),
        "resolution_source": (
            score.get("resolution", {}).get("source")
            if isinstance(score.get("resolution"), dict)
            else None
        ),
    }


def _money_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved")]
    pnl = round(sum(num(row.get("would_be_realized_pnl_usd")) for row in resolved), 6)
    cost = round(sum(num(row.get("counterfactual_cost_usd")) for row in resolved), 6)
    return {
        "generation_rows": len(rows),
        "resolved_rows": len(resolved),
        "unresolved_rows": len(rows) - len(resolved),
        "wins": sum(row.get("would_have_won") is True for row in resolved),
        "losses": sum(row.get("would_have_won") is False for row in resolved),
        "would_be_realized_cost_usd": cost,
        "would_be_realized_pnl_usd": pnl,
        "would_be_realized_roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
    }


def build_money_surface(
    guard_state: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    *,
    generated_at: str,
) -> dict[str, Any]:
    participation = (
        guard_state.get("window_participation")
        if isinstance(guard_state.get("window_participation"), dict)
        else {}
    )
    source_rows = participation.get("rows") if isinstance(participation.get("rows"), list) else []
    scored = [
        _score_generation_row(row, resolutions)
        for row in source_rows
        if isinstance(row, dict) and row.get("dominant_skip_reason") in MONEY_PREDICATES
    ]
    dates = sorted({str(row["day_utc"]) for row in scored if row.get("day_utc")})
    dev_count = max(1, int(len(dates) * 0.7)) if dates else 0
    dev_dates = set(dates[:dev_count])
    holdout_dates = set(dates[dev_count:])
    generated_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    latest_complete_day = (generated_dt.astimezone(UTC).date() - timedelta(days=1)).isoformat()
    by_predicate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_day_predicate: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in scored:
        predicate = str(row.get("predicate") or "")
        day = str(row.get("day_utc") or "")
        by_predicate[predicate].append(row)
        by_day_predicate[(day, predicate)].append(row)
    predicate_rows = []
    for predicate in MONEY_PREDICATES:
        rows = by_predicate[predicate]
        predicate_rows.append(
            {
                "predicate": predicate,
                "generation": _money_summary(rows),
                "dev": _money_summary([row for row in rows if row.get("day_utc") in dev_dates]),
                "holdout": _money_summary([row for row in rows if row.get("day_utc") in holdout_dates]),
                "latest_complete_day": _money_summary(
                    [row for row in rows if row.get("day_utc") == latest_complete_day]
                ),
            }
        )
    day_rows = [
        {
            "day_utc": day,
            "predicate": predicate,
            "split": "dev" if day in dev_dates else "holdout",
            **_money_summary(rows),
        }
        for (day, predicate), rows in sorted(by_day_predicate.items())
        if day
    ]
    return {
        "measurement_only": True,
        "live_mutation": False,
        "population_source": "wallet_copy_live_guard_state.window_participation.rows",
        "resolution_source": DEFAULT_RESOLUTIONS,
        "split_rule": "chronological UTC-day 70/30; a UTC day is never split across dev and holdout",
        "dev_days": sorted(dev_dates),
        "holdout_days": sorted(holdout_dates),
        "latest_complete_day": latest_complete_day,
        "generation_taxonomy_rows": len(scored),
        "predicates": predicate_rows,
        "day_bounded_slices": day_rows,
        "rows": scored,
    }


def build_report(
    source: dict[str, Any],
    *,
    generated_at: str,
    guard_state: dict[str, Any] | None = None,
    resolutions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    summary = source.get("summary") if isinstance(source.get("summary"), dict) else {}
    ranked = summary.get("abstaining_predicates_ranked")
    ranked = ranked if isinstance(ranked, list) else []
    counts = {
        str(row.get("predicate") or ""): int(row.get("windows_foreclosed") or 0)
        for row in ranked
        if isinstance(row, dict)
    }
    rows = [
        {"predicate": predicate, "windows_foreclosed": counts.get(predicate, 0), **provenance}
        for predicate, provenance in PROVENANCE.items()
    ]
    combined = sum(int(row["windows_foreclosed"]) for row in rows)
    report = {
        "schema_version": 1,
        "kind": "supply_foreclosure_attribution",
        "flow_stage": "MEASURE/LIVE",
        "generated_at": generated_at,
        "source_generated_at": source.get("generated_at"),
        "source": DEFAULT_INPUT,
        "measurement_only": True,
        "live_mutation": False,
        "windows_total": summary.get("windows_total"),
        "submitted_windows": summary.get("submitted_windows"),
        "zero_submission_windows": summary.get("zero_submission_windows"),
        "foreclosures": rows,
        "combined_windows_foreclosed": combined,
        "combined_share_of_288_pct": round(100.0 * combined / 288.0, 6),
        "decision": "SUPPLY_NOT_SIZE_IS_THE_NEXT_LEVER",
    }
    if guard_state is not None:
        report["predicate_money_surface"] = build_money_surface(
            guard_state,
            resolutions or {},
            generated_at=generated_at,
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    args = parser.parse_args()
    report = build_report(
        load_json(args.input, default={}),
        generated_at=datetime.now(UTC).isoformat(),
        guard_state=load_json(args.guard_state, default={}),
        resolutions=load_resolutions(args.resolutions),
    )
    atomic_write_json(args.output, report)
    print(report)


if __name__ == "__main__":
    main()
