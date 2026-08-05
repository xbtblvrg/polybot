#!/usr/bin/env python3
"""Build the OP-RND-288 AMENDED daily BTC-5m participation map.

The 288 rows measure participation, not per-window profitability. Profit is
graded on daily/hour-band aggregates; per-window win rate is reporting only.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DATA_DIR = ROOT / "data/research"
UNMEASURED_PREDICATES = {
    "producer_never_ran_for_window",
    "row_evicted_by_retention_policy",
    "producer_ran_and_wrote_no_row",
    "window_precedes_producer_first_run",
}


def _number(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if result == result else 0.0


def _day_start(day: str) -> datetime:
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC)


def _generated_at(scorecard: dict[str, Any]) -> datetime:
    value = str(scorecard.get("generated_at") or utc_now_iso())
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _row_owner(skip_reasons: dict[str, Any]) -> str:
    keys = set(skip_reasons)
    if any("entry_price" in key or "band_gate" in key for key in keys):
        return "LIVE_DEFEND_ENTRY_PRICE_GATE"
    if any("window_time" in key or "late_window" in key or "stale" in key for key in keys):
        return "RND_TRACK_B_EXECUTION_LATENCY"
    if any("best_ask" in key or "precision" in key or "min_order" in key for key in keys):
        return "RND_TRACK_B_EXECUTION_QUALITY"
    if any("target_already_met" in key or "unchanged_no_edge" in key for key in keys):
        return "LIVE_EXPECTANCY_FILTER"
    return "RND_TRACK_A_COVERAGE_SUPPLY"


def _merge_participation_rows(rows: list[Any], *, start_ts: float, end_ts: float) -> dict[int, dict[str, Any]]:
    merged: dict[int, dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        ts = int(_number(raw.get("window_start_s")))
        if ts < start_ts or ts >= end_ts:
            continue
        row = merged.setdefault(
            ts,
            {"wallet_eligible_orders": 0, "our_submits": 0, "our_fills": 0, "skip_reasons": Counter()},
        )
        row["wallet_eligible_orders"] = max(
            int(row["wallet_eligible_orders"]), int(_number(raw.get("wallet_eligible_orders")))
        )
        row["our_submits"] = max(int(row["our_submits"]), int(_number(raw.get("our_submits"))))
        row["our_fills"] = max(int(row["our_fills"]), int(_number(raw.get("our_fills"))))
        if isinstance(raw.get("skip_reasons"), dict):
            row["skip_reasons"].update(
                {str(key): int(_number(value)) for key, value in raw["skip_reasons"].items() if _number(value) > 0}
            )
    return merged


def _pnl_by_slug(
    scorecard: dict[str, Any],
) -> tuple[dict[str, float], dict[str, set[str]], dict[str, set[str]], set[str], set[str]]:
    pnl: dict[str, float] = defaultdict(float)
    lanes: dict[str, set[str]] = defaultdict(set)
    wallets: dict[str, set[str]] = defaultdict(set)
    resolved_slugs: set[str] = set()
    filled_slugs: set[str] = set()
    truth = scorecard.get("canonical_pnl_truth")
    events = truth.get("events") if isinstance(truth, dict) else []
    for event in events or []:
        if not isinstance(event, dict) or str(event.get("status")) != "FILLED":
            continue
        slug = str(event.get("market_slug") or "")
        if not slug:
            continue
        filled_slugs.add(slug)
        if not event.get("resolved"):
            continue
        pnl[slug] += _number(event.get("pnl_usd"))
        resolved_slugs.add(slug)
        if event.get("lane"):
            lanes[slug].add(str(event["lane"]))
        if event.get("source_wallet"):
            wallets[slug].add(str(event["source_wallet"]))
    return pnl, lanes, wallets, resolved_slugs, filled_slugs


def build_map(scorecard: dict[str, Any], *, day: str) -> dict[str, Any]:
    start = _day_start(day)
    end = start + timedelta(days=1)
    generated = _generated_at(scorecard)
    volume = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    observed = _merge_participation_rows(
        volume.get("rows") or [], start_ts=start.timestamp(), end_ts=end.timestamp()
    )
    observed_timestamps = sorted(observed)
    observed_hours = {
        datetime.fromtimestamp(ts, tz=UTC).hour for ts in observed_timestamps
    }
    producer_first_ts = observed_timestamps[0] if observed_timestamps else None
    producer_last_ts = observed_timestamps[-1] if observed_timestamps else None
    row_source_producer = str(
        volume.get("source") or "wallet_copy_live_guard_state.volume_kpi.rows"
    )
    retention_window_start = (
        datetime.fromtimestamp(producer_first_ts, tz=UTC).isoformat().replace("+00:00", "Z")
        if producer_first_ts is not None
        else None
    )
    retention_window_end = (
        (datetime.fromtimestamp(producer_last_ts, tz=UTC) + timedelta(minutes=5))
        .isoformat()
        .replace("+00:00", "Z")
        if producer_last_ts is not None
        else None
    )
    pnl_by_slug, lanes_by_slug, wallets_by_slug, resolved_slugs, filled_slugs = _pnl_by_slug(scorecard)
    rows: list[dict[str, Any]] = []
    for slot in range(288):
        window = start + timedelta(minutes=5 * slot)
        ts = int(window.timestamp())
        slug = f"btc-updown-5m-{ts}"
        source = observed.get(ts)
        if window + timedelta(minutes=5) <= generated:
            lifecycle = "elapsed"
        elif source:
            lifecycle = "active"
        else:
            lifecycle = "future_pending"
        if (source and int(source["our_fills"]) > 0) or slug in filled_slugs:
            status = "traded"
            owner = "LIVE_GUARD"
            reason = "accepted_fill"
            measurement_status = "MEASURED"
            predicate = "our_fills_gt_0_or_canonical_filled_slug"
        elif source and source["skip_reasons"]:
            status = "skipped_with_measured_reason"
            owner = _row_owner(dict(source["skip_reasons"]))
            reason = source["skip_reasons"].most_common(1)[0][0]
            measurement_status = "MEASURED"
            predicate = f"dominant_skip_reason:{reason}"
        elif lifecycle == "future_pending":
            status = "uncovered_with_owner"
            owner = "LIVE_RUNTIME_FUTURE_WINDOW"
            reason = "window_not_closed_yet"
            measurement_status = "NOT_APPLICABLE_FUTURE"
            predicate = "window_end_gt_scorecard_generated_at"
        else:
            status = "uncovered_with_owner"
            owner = "RND_TRACK_A_MEASUREMENT_COVERAGE"
            reason = "UNMEASURED"
            measurement_status = "UNMEASURED"
            if producer_first_ts is None:
                predicate = "producer_never_ran_for_window"
                provenance_basis = "no retained producer row exists for the UTC day"
            elif ts < producer_first_ts:
                predicate = "window_precedes_producer_first_run"
                provenance_basis = "window starts before the first retained producer row"
            elif window.hour in observed_hours:
                predicate = "producer_ran_and_wrote_no_row"
                provenance_basis = "same UTC hour contains retained producer rows but this window does not"
            else:
                predicate = "producer_never_ran_for_window"
                provenance_basis = "this UTC hour contains no retained producer row"
        rows.append(
            {
                "slot": slot,
                "window_start": window.isoformat().replace("+00:00", "Z"),
                "market_slug": slug,
                "utc_hour_band": f"{window.hour:02d}:00-{window.hour:02d}:59Z",
                "lifecycle": lifecycle,
                "participation_status": status,
                "owner": owner,
                "measured_reason": reason,
                "measurement_status": measurement_status,
                "participation_predicate": predicate,
                "row_source_producer": (
                    row_source_producer if measurement_status == "UNMEASURED" else None
                ),
                "retention_window_start": (
                    retention_window_start if measurement_status == "UNMEASURED" else None
                ),
                "retention_window_end": (
                    retention_window_end if measurement_status == "UNMEASURED" else None
                ),
                "retention_provenance_basis": (
                    provenance_basis if measurement_status == "UNMEASURED" else None
                ),
                "wallet_eligible_orders": int(source["wallet_eligible_orders"]) if source else 0,
                "our_submits": int(source["our_submits"]) if source else 0,
                "our_fills": max(int(source["our_fills"]) if source else 0, 1 if slug in filled_slugs else 0),
                "skip_reasons": dict(source["skip_reasons"]) if source else {},
                "resolved_pnl_usd": round(pnl_by_slug.get(slug, 0.0), 6),
                "resolved_pnl_available": slug in resolved_slugs,
                "lanes": sorted(lanes_by_slug.get(slug, set())),
                "source_wallets": sorted(wallets_by_slug.get(slug, set())),
            }
        )

    bands: list[dict[str, Any]] = []
    for hour in range(24):
        band_rows = rows[hour * 12 : (hour + 1) * 12]
        observed_rows = [row for row in band_rows if row["lifecycle"] != "future_pending"]
        pnl = round(sum(_number(row["resolved_pnl_usd"]) for row in observed_rows), 6)
        traded = sum(row["participation_status"] == "traded" for row in observed_rows)
        if not observed_rows:
            color, cause, corrective = "PENDING", "band_not_elapsed", "LIVE_RUNTIME_FUTURE_WINDOW"
        elif pnl > 0 and traded > 0:
            color, cause, corrective = "GREEN", "positive_aggregate", "AMPLIFY_MEASURED_GREEN_MAKERS"
        else:
            color = "RED"
            cause = "aggregate_pnl_nonpositive" if traded else "zero_traded_windows"
            corrective = "RND_TRACK_B_GREEN_DAY_CAUSAL" if traded else "RND_TRACK_A_COVERAGE_SUPPLY"
        bands.append(
            {
                "utc_hour_band": f"{hour:02d}:00-{hour:02d}:59Z",
                "observed_windows": len(observed_rows),
                "elapsed_windows": sum(row["lifecycle"] == "elapsed" for row in observed_rows),
                "active_windows": sum(row["lifecycle"] == "active" for row in observed_rows),
                "traded_windows": traded,
                "skipped_with_measured_reason": sum(
                    row["participation_status"] == "skipped_with_measured_reason" for row in observed_rows
                ),
                "uncovered_with_owner": sum(
                    row["participation_status"] == "uncovered_with_owner" for row in observed_rows
                ),
                "unmeasured_windows": sum(
                    row["measurement_status"] == "UNMEASURED"
                    for row in observed_rows
                ),
                "resolved_pnl_usd": pnl,
                "production_color": color,
                "cause": cause,
                "corrective_owner": corrective,
            }
        )

    elapsed_rows = [row for row in rows if row["lifecycle"] == "elapsed"]
    observed_rows = [row for row in rows if row["lifecycle"] != "future_pending"]
    resolved = [row for row in observed_rows if row["resolved_pnl_available"]]
    positive = sum(_number(row["resolved_pnl_usd"]) > 0 for row in resolved)
    counts = Counter(row["participation_status"] for row in rows)
    unmeasured_predicate_counts = Counter(
        row["participation_predicate"]
        for row in elapsed_rows
        if row["measurement_status"] == "UNMEASURED"
    )
    unmeasured_total = sum(unmeasured_predicate_counts.values())
    max_unmeasured_predicate_pct = (
        round(100.0 * max(unmeasured_predicate_counts.values()) / unmeasured_total, 6)
        if unmeasured_total
        else 0.0
    )
    return {
        "kind": "btc5m_288_participation_map",
        "schema_version": 1,
        "flow_stage": "LIVE/LEARN/PROMOTE/SELF-DEV",
        "operator_decision": "OP-RND-288-AMENDED",
        "day_utc": day,
        "generated_at": utc_now_iso(),
        "definition": "participation per slot; profitability graded on daily/hour-band aggregates",
        "row_count": len(rows),
        "summary": {
            "elapsed_windows": len(elapsed_rows),
            "active_windows": sum(row["lifecycle"] == "active" for row in rows),
            "future_pending_windows": sum(row["lifecycle"] == "future_pending" for row in rows),
            "status_counts": dict(sorted(counts.items())),
            "elapsed_unmeasured_windows": sum(
                row["measurement_status"] == "UNMEASURED" for row in elapsed_rows
            ),
            "unmeasured_predicate_counts": dict(sorted(unmeasured_predicate_counts.items())),
            "unmeasured_predicate_closed_set": sorted(UNMEASURED_PREDICATES),
            "max_unmeasured_predicate_concentration_pct": max_unmeasured_predicate_pct,
            "unmeasured_predicate_concentration_limit_pct": 80.0,
            "unmeasured_predicate_concentration_bounded": (
                max_unmeasured_predicate_pct <= 80.0
            ),
            "elapsed_uncovered_without_classification": sum(
                row["participation_status"] == "uncovered_with_owner"
                and not row.get("participation_predicate")
                and row.get("measurement_status") != "UNMEASURED"
                for row in elapsed_rows
            ),
            "traded_windows": counts.get("traded", 0),
            "canonical_unique_filled_windows": len(filled_slugs),
            "filled_window_reconciliation_status": (
                "PASS" if counts.get("traded", 0) == len(filled_slugs) else "FAIL"
            ),
            "uncovered_plus_losing_owner_kpi": sum(
                row["participation_status"] == "uncovered_with_owner" for row in observed_rows
            ) + sum(band["production_color"] == "RED" for band in bands),
            "daily_resolved_pnl_usd": round(sum(_number(row["resolved_pnl_usd"]) for row in rows), 6),
            "win_rate_reporting_only_pct": round(100.0 * positive / len(resolved), 6) if resolved else None,
            "win_rate_is_gate": False,
        },
        "hour_band_aggregates": bands,
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True)
    parser.add_argument("--scorecard", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    scorecard_path = Path(args.scorecard) if args.scorecard else DATA_DIR / f"wallet_copy_daily_scorecard_{args.day}.json"
    output = Path(args.output) if args.output else DATA_DIR / f"btc5m_288_participation_map_{args.day}.json"
    scorecard = load_json(scorecard_path, default={})
    if not isinstance(scorecard, dict) or not scorecard:
        print(f"missing scorecard: {scorecard_path}", file=sys.stderr)
        return 2
    payload = build_map(scorecard, day=args.day)
    atomic_write_json(output, payload)
    print(json.dumps({"output": str(output), "summary": payload["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
