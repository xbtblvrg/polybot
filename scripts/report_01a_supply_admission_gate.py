#!/usr/bin/env python3
"""Publish evidence-gated 01a supply for enabled seats and ready-queue candidates."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json

MIN_CALENDAR_SPAN_DAYS = 7.0
MIN_OBSERVED_MARKET_WINDOWS = 100
MAX_INPUT_DIVERGENCE_H = 6.0
MIN_DAILY_PARTITION_COVERAGE_WINDOWS = 200


def _wallet(row: dict[str, Any]) -> str:
    return str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()


def build_report(
    *,
    census: dict[str, Any],
    active_set: dict[str, Any],
    ready_queue: dict[str, Any],
    standings: dict[str, Any],
    generated_at: str,
    partitions: list[dict[str, Any]] | None = None,
    accumulator_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    enabled = {
        _wallet(row): row
        for row in active_set.get("members", [])
        if isinstance(row, dict) and row.get("enabled") is True and _wallet(row)
    }
    queued = {
        _wallet(row): row
        for row in ready_queue.get("ranked_members", [])
        if isinstance(row, dict) and _wallet(row)
    }
    census_rows = {
        _wallet(row): row
        for row in census.get("ranking", [])
        if isinstance(row, dict) and _wallet(row)
    }
    holdout_rows = {
        _wallet(row): row
        for row in standings.get("standings", [])
        if isinstance(row, dict) and _wallet(row)
    }

    accumulator_state = accumulator_state or {}
    initialized_at = _timestamp(accumulator_state.get("initialized_at"))
    first_full_partition_day = (
        initialized_at.date() + timedelta(days=1)
        if initialized_at is not None
        else None
    )
    completed_partitions = _contiguous_completed_partitions(
        partitions or [], generated_at, first_full_day=first_full_partition_day
    )
    partition_measurements = _measure_partitions(completed_partitions)
    partition_count = len(completed_partitions)
    evidence_wallets = set(partition_measurements) if partition_measurements else set(census_rows)
    resolved_evidence = {
        wallet: row
        for wallet, row in holdout_rows.items()
        if int(row.get("resolved_orders") or 0) > 0 and wallet in evidence_wallets
    }
    candidates = set(queued) | set(resolved_evidence)
    census_generated = _timestamp(census.get("generated_at"))
    standings_generated = _timestamp(standings.get("generated_at"))
    latest_partition_generated = max(
        (_timestamp(row.get("generated_at")) for row in completed_partitions),
        default=None,
        key=lambda value: value or datetime.min.replace(tzinfo=UTC),
    )
    supply_generated = latest_partition_generated or census_generated
    divergence_h = (
        abs((supply_generated - standings_generated).total_seconds()) / 3600.0
        if supply_generated is not None and standings_generated is not None
        else None
    )
    inputs_fresh = divergence_h is not None and divergence_h <= MAX_INPUT_DIVERGENCE_H
    generated_date = _timestamp(generated_at).date() if _timestamp(generated_at) else date.today()
    projected_maturity_date = (
        generated_date
        + timedelta(days=(7 - partition_count if partition_count else 8))
    ).isoformat()
    accumulator_wired = bool(accumulator_state.get("next_byte_offset") is not None)

    rows: list[dict[str, Any]] = []
    for wallet in sorted(set(enabled) | candidates):
        measured = census_rows.get(wallet, {})
        partition_row = partition_measurements.get(wallet, {})
        span_days = partition_count if partition_count else measured.get("span_days")
        observed_windows = int(
            partition_row.get("observed_market_windows")
            if partition_count
            else measured.get("observed_btc5m_window_count") or 0
        )
        qualifying_windows = int(
            partition_row.get("qualifying_01a_market_windows")
            if partition_count
            else measured.get("qualifying_window_count") or 0
        )
        span_pass = isinstance(span_days, (int, float)) and float(span_days) >= MIN_CALENDAR_SPAN_DAYS
        window_pass = observed_windows >= MIN_OBSERVED_MARKET_WINDOWS
        measurement_pass = span_pass and window_pass and inputs_fresh
        windows_per_day = (
            round(qualifying_windows / float(span_days), 6)
            if measurement_pass and float(span_days) > 0
            else "NO_MEASUREMENT"
        )

        holdout = holdout_rows.get(wallet, {})
        holdout_pnl = holdout.get("second_half_post_fee_pnl_usd")
        holdout_observed = isinstance(holdout_pnl, (int, float))
        holdout_nonnegative = holdout_observed and float(holdout_pnl) >= 0.0
        candidate_eligible = bool(
            wallet in candidates
            and measurement_pass
            and isinstance(windows_per_day, float)
            and windows_per_day > 0.0
            and holdout_nonnegative
        )
        deficits = []
        if not span_pass:
            deficits.append("calendar_span_days_lt_7")
        if not window_pass:
            deficits.append("observed_market_windows_lt_100")
        if wallet in candidates and not holdout_observed:
            deficits.append("holdout_ev_unmeasured")
        elif wallet in candidates and not holdout_nonnegative:
            deficits.append("holdout_ev_negative")
        if wallet in candidates and measurement_pass and windows_per_day == 0.0:
            deficits.append("measured_01a_windows_per_day_not_positive")
        if not inputs_fresh:
            deficits.append("input_generated_at_divergence_gt_6h")
        measurement_status = (
            "STALE_INPUT_REFUSED"
            if not inputs_fresh
            else "MEASURED"
            if measurement_pass
            else f"ACCRUING_PARTITIONS_{partition_count}_OF_7"
            if accumulator_wired
            else "UNREACHABLE_UNDER_CURRENT_CORPUS"
        )
        rows.append(
            {
                "wallet": wallet,
                "roles": [
                    role
                    for role, present in (
                        ("ENABLED_MEMBER", wallet in enabled),
                        ("READY_QUEUE_CANDIDATE", wallet in queued),
                        ("RESOLVED_DUAL_LEG_CANDIDATE", wallet in resolved_evidence),
                    )
                    if present
                ],
                "calendar_span_days": span_days,
                "observed_market_windows": observed_windows,
                "qualifying_01a_market_windows": qualifying_windows,
                "qualifying_01a_windows_per_day": windows_per_day,
                "measurement_status": measurement_status,
                "measurement_deficits": deficits,
                "holdout": {
                    "source": "wide_candidate_standings.second_half_post_fee_pnl_usd",
                    "post_fee_pnl_usd": holdout_pnl if holdout_observed else None,
                    "nonnegative": holdout_nonnegative if holdout_observed else None,
                    "paper_resolved_orders": int(holdout.get("resolved_orders") or 0),
                    "first_half_post_fee_pnl_usd": holdout.get("first_half_post_fee_pnl_usd"),
                },
                "candidate_admission_gate_pass": candidate_eligible,
            }
        )

    eligible = [row["wallet"] for row in rows if row["candidate_admission_gate_pass"]]
    return {
        "kind": "01a_supply_admission_gate",
        "generated_at": generated_at,
        "flow_stage": "OBSERVE/PROMOTE/LIVE/ROTATE",
        "paper_only": True,
        "live_mutation": False,
        "copy_intent_parity": True,
        "measurement_contract": {
            "price_band": "[0.25,0.32)",
            "minimum_calendar_span_days": MIN_CALENDAR_SPAN_DAYS,
            "minimum_observed_market_windows": MIN_OBSERVED_MARKET_WINDOWS,
            "underpowered_rule": "publish NO_MEASUREMENT; extrapolation is forbidden",
            "identity_source": "orderfilled_early_01a_supply_census identity-clean wallet rows",
            "input_max_generated_at_divergence_h": MAX_INPUT_DIVERGENCE_H,
        },
        "input_freshness": {
            "census_generated_at": census.get("generated_at"),
            "latest_completed_partition_generated_at": (
                latest_partition_generated.isoformat().replace("+00:00", "Z")
                if latest_partition_generated is not None
                else None
            ),
            "supply_input_selected": "daily_partitions" if completed_partitions else "legacy_census",
            "standings_generated_at": standings.get("generated_at"),
            "generated_at_divergence_h": round(divergence_h, 6) if divergence_h is not None else None,
            "status": "PASS" if inputs_fresh else "STALE_INPUT_REFUSED",
        },
        "dual_leg_cohort_overlap": {
            "census_intersect_standings": len(set(census_rows) & set(holdout_rows)),
            "census_intersect_resolved": len(resolved_evidence),
            "census_intersect_holdout": sum(
                isinstance(row.get("second_half_post_fee_pnl_usd"), (int, float))
                for row in resolved_evidence.values()
            ),
        },
        "partition_maturity": {
            "accumulator_wired": accumulator_wired,
            "contiguous_completed_partition_count": partition_count,
            "required_partition_count": 7,
            "projected_first_measured_cut_date": projected_maturity_date,
            "partition_coverage": [
                {
                    "day_utc": str(row.get("day_utc")),
                    "distinct_window_epoch_count": _partition_coverage_count(row),
                    "coverage_of_288": round(_partition_coverage_count(row) / 288.0, 6),
                    "counted_as_complete": _partition_coverage_count(row) >= MIN_DAILY_PARTITION_COVERAGE_WINDOWS,
                }
                for row in sorted(partitions or [], key=lambda item: str(item.get("day_utc") or ""))
                if row.get("day_utc") and str(row.get("day_utc")) < generated_date.isoformat()
            ],
            "status": f"ACCRUING_PARTITIONS_{partition_count}_OF_7" if accumulator_wired and partition_count < 7 else "READY" if partition_count >= 7 else "UNREACHABLE_UNDER_CURRENT_CORPUS",
        },
        "admission_rule": {
            "seat_yields_when": "enabled for >=48h AND zero fills AND 01a supply is NO_MEASUREMENT",
            "candidate_replaces_when": "measured 01a windows/day > 0 AND nonnegative holdout EV",
            "execution_authority": False,
            "separate_live_actuator_and_fable_ruling_required": True,
        },
        "enabled_member_count": len(enabled),
        "ready_queue_candidate_count": len(queued),
        "resolved_dual_leg_candidate_count": len(resolved_evidence),
        "rows": rows,
        "eligible_replacement_wallets": eligible,
        "rotation_authorized": False,
        "status": "CANDIDATE_EVIDENCE_READY" if eligible else "NO_ADMISSIBLE_REPLACEMENT",
    }


def _timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _contiguous_completed_partitions(
    partitions: list[dict[str, Any]],
    generated_at: str,
    *,
    first_full_day: date | None = None,
) -> list[dict[str, Any]]:
    generated = _timestamp(generated_at) or datetime.now(tz=UTC)
    by_day = {
        str(row.get("day_utc")): row
        for row in partitions
        if row.get("day_utc")
        and str(row.get("day_utc")) < generated.date().isoformat()
        and (first_full_day is None or str(row.get("day_utc")) >= first_full_day.isoformat())
        and _partition_coverage_count(row) >= MIN_DAILY_PARTITION_COVERAGE_WINDOWS
    }
    result: list[dict[str, Any]] = []
    cursor = generated.date() - timedelta(days=1)
    while cursor.isoformat() in by_day:
        result.append(by_day[cursor.isoformat()])
        cursor -= timedelta(days=1)
    return list(reversed(result))


def _partition_coverage_count(partition: dict[str, Any]) -> int:
    published = partition.get("distinct_window_epoch_count")
    if isinstance(published, int):
        return published
    epochs: set[int] = set()
    for row in partition.get("rows", []):
        if isinstance(row, dict):
            epochs.update(int(value) for value in row.get("observed_window_epochs", []))
    return len(epochs)


def _measure_partitions(partitions: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    observed: dict[str, set[int]] = {}
    qualifying: dict[str, set[int]] = {}
    for partition in partitions:
        for row in partition.get("rows", []):
            if not isinstance(row, dict) or not _wallet(row):
                continue
            wallet = _wallet(row)
            observed.setdefault(wallet, set()).update(int(value) for value in row.get("observed_window_epochs", []))
            qualifying.setdefault(wallet, set()).update(int(value) for value in row.get("qualifying_01a_window_epochs", []))
    return {wallet: {"observed_market_windows": len(values), "qualifying_01a_market_windows": len(qualifying.get(wallet, set()))} for wallet, values in observed.items()}


def _load(path: str) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _merge_standings_payloads(payloads: list[dict[str, Any]], *, generated_at: str) -> dict[str, Any]:
    reference = _timestamp(generated_at) or datetime.now(tz=UTC)
    fresh = [
        payload
        for payload in payloads
        if _timestamp(payload.get("generated_at")) is not None
        and 0 <= (reference - _timestamp(payload.get("generated_at"))).total_seconds() <= MAX_INPUT_DIVERGENCE_H * 3600
    ]
    resolved_payloads = [
        payload
        for payload in fresh
        if any(
            isinstance(row, dict) and int(row.get("resolved_orders") or 0) > 0
            for row in payload.get("standings", [])
        )
    ]
    pool = resolved_payloads or fresh
    selected = max(
        pool,
        default={},
        key=lambda payload: _timestamp(payload.get("generated_at")) or datetime.min.replace(tzinfo=UTC),
    )
    return {
        **selected,
        "fresh_artifact_count_considered": len(fresh),
        "selection_rule": "freshest_coherent_artifact_with_any_resolved_order; never union mutually exclusive run cohorts",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--census", default="data/research/orderfilled_early_01a_supply_census_latest.json")
    parser.add_argument("--active-set", default="data/research/wallet_copy_active_set_auto_degrade_state.json")
    parser.add_argument("--ready-queue", default="data/research/wallet_copy_full_pool_member_queue.json")
    parser.add_argument("--standings", default="data/research/wide_candidate_standings_latest.json")
    parser.add_argument("--standings-glob", default="data/research/wide_candidate_standings_wide_*.json")
    parser.add_argument("--output", default="data/research/01a_supply_admission_gate_latest.json")
    parser.add_argument("--partitions-dir", default="data/research/orderfilled_01a_supply_daily")
    parser.add_argument("--accumulator-state", default="data/research/orderfilled_01a_supply_daily_accumulator_state.json")
    args = parser.parse_args()
    generated_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    standings = _merge_standings_payloads(
        [_load(args.standings)] + [_load(str(path)) for path in sorted(Path().glob(args.standings_glob))],
        generated_at=generated_at,
    )
    report = build_report(
        census=_load(args.census),
        active_set=_load(args.active_set),
        ready_queue=_load(args.ready_queue),
        standings=standings,
        generated_at=generated_at,
        partitions=[_load(str(path)) for path in sorted(Path(args.partitions_dir).glob("*.json"))],
        accumulator_state=_load(args.accumulator_state),
    )
    atomic_write_json(Path(args.output), report)
    print(json.dumps({key: report[key] for key in ("status", "enabled_member_count", "ready_queue_candidate_count", "eligible_replacement_wallets")}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
