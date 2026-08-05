#!/usr/bin/env python3
"""Report prospective F3 attribution from durable fetch-cycle instrumentation."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, parse_ts, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json

F3_STALE = "REFUSED_STALE_RECEIPT_TO_FETCH"
COPYABLE = "COPYABLE_EXACT_POLICY_PAPER_FILL"
PROVENANCE = {
    "capture_prefetched",
    "reconcile_batch_fetched",
    "no_prefetch_entry",
}
REQUIRED_FIELDS = {
    "fetch_instrumentation_schema_version",
    "fetch_provenance",
    "fetch_cycle_id",
    "fetch_started_monotonic_s",
    "fetch_started_monotonic_observed",
    "recv_monotonic_s",
    "token_cycle_first_recv_monotonic_s",
    "token_event_ordinal_in_cycle",
}
MIN_WINDOW_S = 86400.0
MIN_F2_PASS_ATTEMPTS = 1500
BATCH_ATTRIBUTION_GATE = 0.90
CONTROL_ALPHA = 0.05
MIN_CONTROL_ATTEMPTS_PER_ARM = 30
F3_LIMIT_S = 5.0


def _terminal(row: dict[str, Any]) -> str:
    detail = row.get("f1_f4_terminal")
    return str((detail if isinstance(detail, dict) else {}).get("terminal") or "")


def _f2_pass(row: dict[str, Any]) -> bool:
    detail = row.get("f1_f4_terminal")
    return isinstance(detail, dict) and detail.get("F2_alpha_profile") == "PASS"


def _instrumented(row: dict[str, Any]) -> bool:
    return (
        REQUIRED_FIELDS.issubset(row)
        and row.get("fetch_instrumentation_schema_version") == 2
        and str(row.get("fetch_provenance") or "") in PROVENANCE
        and bool(row.get("fetch_cycle_id"))
        and row.get("fetch_started_monotonic_observed") is True
        and num(row.get("fetch_started_monotonic_s")) > 0
        and num(row.get("recv_monotonic_s")) > 0
        and num(row.get("token_cycle_first_recv_monotonic_s")) > 0
        and row.get("token_event_ordinal_in_cycle") is not None
    )


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(pct * len(ordered)) - 1))
    return round(ordered[index], 6)


def _interval_stats(values: list[float]) -> dict[str, Any]:
    values_ms = [max(0.0, value) * 1000.0 for value in values]
    return {
        "attempts": len(values_ms),
        "min_ms": round(min(values_ms), 6) if values_ms else None,
        "p50_ms": _percentile(values_ms, 0.50),
        "p90_ms": _percentile(values_ms, 0.90),
        "max_ms": round(max(values_ms), 6) if values_ms else None,
    }


def _raw_intervals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    event_to_fetch = [
        num(row["fetch_started_monotonic_s"]) - num(row["recv_monotonic_s"])
        for row in rows
    ]
    cycle_first_to_fetch = [
        num(row["fetch_started_monotonic_s"])
        - num(row["token_cycle_first_recv_monotonic_s"])
        for row in rows
    ]
    return {
        "event_recv_to_fetch_start": _interval_stats(event_to_fetch),
        "cycle_first_token_event_to_fetch_start": _interval_stats(
            cycle_first_to_fetch
        ),
    }


def _rate(numerator: int, denominator: int) -> float | None:
    return round(100.0 * numerator / denominator, 6) if denominator else None


def _two_proportion_control(
    *,
    single_refused: int,
    single_total: int,
    multi_refused: int,
    multi_total: int,
) -> dict[str, Any]:
    result = {
        "method": "two_sided_pooled_two_proportion_z_test",
        "alpha": CONTROL_ALPHA,
        "minimum_attempts_per_arm": MIN_CONTROL_ATTEMPTS_PER_ARM,
        "single_event_attempts": single_total,
        "single_event_f3_refused": single_refused,
        "single_event_f3_refusal_rate_pct": _rate(single_refused, single_total),
        "multi_event_attempts": multi_total,
        "multi_event_f3_refused": multi_refused,
        "multi_event_f3_refusal_rate_pct": _rate(multi_refused, multi_total),
        "absolute_rate_difference_pct_points": (
            round(
                abs(single_refused / single_total - multi_refused / multi_total)
                * 100.0,
                6,
            )
            if single_total and multi_total
            else None
        ),
        "z_score": None,
        "p_value": None,
        "test_eligible": False,
        "statistically_indistinguishable": None,
    }
    if (
        single_total < MIN_CONTROL_ATTEMPTS_PER_ARM
        or multi_total < MIN_CONTROL_ATTEMPTS_PER_ARM
    ):
        return result
    pooled = (single_refused + multi_refused) / (single_total + multi_total)
    expected = (
        pooled * single_total,
        (1.0 - pooled) * single_total,
        pooled * multi_total,
        (1.0 - pooled) * multi_total,
    )
    if min(expected) < 5.0:
        return result
    standard_error = math.sqrt(
        pooled * (1.0 - pooled) * (1.0 / single_total + 1.0 / multi_total)
    )
    difference = single_refused / single_total - multi_refused / multi_total
    if standard_error == 0:
        p_value = 1.0 if difference == 0 else 0.0
        z_score = 0.0 if difference == 0 else math.copysign(math.inf, difference)
    else:
        z_score = difference / standard_error
        p_value = math.erfc(abs(z_score) / math.sqrt(2.0))
    result.update(
        {
            "z_score": (
                round(z_score, 6) if math.isfinite(z_score) else str(z_score)
            ),
            "p_value": round(p_value, 9),
            "test_eligible": True,
            "statistically_indistinguishable": p_value >= CONTROL_ALPHA,
        }
    )
    return result


def _event_identity(row: dict[str, Any]) -> str:
    return str(
        row.get("source_event_id")
        or row.get("attempt_id")
        or (
            f"{row.get('transaction_hash')}:{row.get('token_id')}:"
            f"{row.get('recv_monotonic_s')}"
        )
    )


def load_instrumentation_events(path: str | Path) -> list[dict[str, Any]]:
    """Read the append-only stream and retain the newest immutable attempt row."""

    by_attempt_id: dict[str, dict[str, Any]] = {}
    target = Path(path)
    if not target.exists():
        return []
    with target.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"invalid instrumentation JSONL at line {line_number}"
                ) from exc
            if not isinstance(row, dict) or not row.get("attempt_id"):
                raise ValueError(
                    f"instrumentation row {line_number} lacks immutable attempt_id"
                )
            by_attempt_id[str(row["attempt_id"])] = row
    return sorted(
        by_attempt_id.values(),
        key=lambda row: (
            str(row.get("recorded_at") or ""),
            str(row.get("attempt_id") or ""),
        ),
    )


def build_report(
    instrumentation_events: list[dict[str, Any]],
    reachability: dict[str, Any],
    *,
    now_s: float | None = None,
) -> dict[str, Any]:
    now_s = (
        float(now_s)
        if now_s is not None
        else datetime.now(timezone.utc).timestamp()
    )
    f2_rows = [
        row
        for row in instrumentation_events
        if isinstance(row, dict) and _f2_pass(row)
    ]
    instrumented = [row for row in f2_rows if _instrumented(row)]
    first_s = min(
        (parse_ts(row.get("recorded_at")) for row in instrumented),
        default=0.0,
    )
    elapsed_s = max(0.0, now_s - first_s) if first_s else 0.0

    group_events: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in instrumented:
        group_events[
            (str(row["fetch_cycle_id"]), str(row.get("token_id") or ""))
        ].add(_event_identity(row))
    single_rows: list[dict[str, Any]] = []
    multi_rows: list[dict[str, Any]] = []
    for row in instrumented:
        group_key = (
            str(row["fetch_cycle_id"]),
            str(row.get("token_id") or ""),
        )
        (single_rows if len(group_events[group_key]) == 1 else multi_rows).append(row)

    by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in instrumented:
        by_wallet[str(row.get("wallet") or "")].append(row)
    wallet_rows: list[dict[str, Any]] = []
    for wallet, rows in sorted(by_wallet.items()):
        classes = {
            provenance: [
                row for row in rows if row.get("fetch_provenance") == provenance
            ]
            for provenance in sorted(PROVENANCE)
        }
        capture = classes["capture_prefetched"]
        wallet_rows.append(
            {
                "wallet": wallet,
                "f2_pass_attempts": len(rows),
                "fetch_provenance_counts": dict(
                    sorted(Counter(str(row["fetch_provenance"]) for row in rows).items())
                ),
                "raw_intervals_by_provenance": {
                    key: _raw_intervals(value) for key, value in classes.items()
                },
                "copyable_rate_pct_capture_prefetched_only": _rate(
                    sum(_terminal(row) == COPYABLE for row in capture),
                    len(capture),
                ),
                "f3_refused_attempts": sum(_terminal(row) == F3_STALE for row in rows),
            }
        )

    stale = [row for row in instrumented if _terminal(row) == F3_STALE]
    multi_ids = {id(row) for row in multi_rows}
    batch_exposed = [
        row
        for row in stale
        if id(row) in multi_ids
        and (
            num(row["fetch_started_monotonic_s"])
            - num(row["token_cycle_first_recv_monotonic_s"])
        )
        > F3_LIMIT_S
    ]
    batch_share = len(batch_exposed) / len(stale) if stale else None
    control = _two_proportion_control(
        single_refused=sum(_terminal(row) == F3_STALE for row in single_rows),
        single_total=len(single_rows),
        multi_refused=sum(_terminal(row) == F3_STALE for row in multi_rows),
        multi_total=len(multi_rows),
    )
    gate_pass = bool(
        batch_share is not None and batch_share >= BATCH_ATTRIBUTION_GATE
    )
    falsifier_hit = bool(
        gate_pass and control["statistically_indistinguishable"] is True
    )
    window_complete = (
        elapsed_s >= MIN_WINDOW_S and len(instrumented) >= MIN_F2_PASS_ATTEMPTS
    )
    control_eligible = control["test_eligible"] is True
    decision_is_binding = bool(window_complete and control_eligible)
    if not window_complete:
        decision = "ACCRUING_STEP1B_CONTROL_WINDOW"
    elif not control_eligible:
        decision = "ACCRUING_STEP1B_CONTROL_POWER"
    elif falsifier_hit:
        decision = "FALSIFY_DEFECT_Q_SINGLE_EVENT_CONTROL"
    elif gate_pass:
        decision = "OPEN_DEFECT_Q_ARCHITECTURE_INDUCED_STALENESS"
    else:
        decision = "DISCARD_DEFECT_Q_BATCH_INTERVAL_NOT_DOMINANT"

    reach_summary = (
        reachability.get("summary")
        if isinstance(reachability.get("summary"), dict)
        else {}
    )
    completeness = {
        "f2_pass_rows_total": len(f2_rows),
        "instrumented_f2_pass_rows": len(instrumented),
        "instrumented_share_pct": _rate(len(instrumented), len(f2_rows)),
        "run_ids_retained": sorted(
            {str(row.get("run_id") or "") for row in instrumented if row.get("run_id")}
        ),
        "cohort_ids_retained": sorted(
            {
                str(row.get("cohort_id") or "")
                for row in instrumented
                if row.get("cohort_id")
            }
        ),
        "manifest_ids_retained": sorted(
            {
                str(row.get("manifest_id") or "")
                for row in instrumented
                if row.get("manifest_id")
            }
        ),
        "first_instrumented_at": (
            datetime.fromtimestamp(first_s, timezone.utc).isoformat()
            if first_s
            else None
        ),
        "elapsed_s": round(elapsed_s, 6),
        "minimum_elapsed_s": MIN_WINDOW_S,
        "minimum_f2_pass_attempts": MIN_F2_PASS_ATTEMPTS,
        "elapsed_gate_pass": elapsed_s >= MIN_WINDOW_S,
        "attempt_gate_pass": len(instrumented) >= MIN_F2_PASS_ATTEMPTS,
        "window_complete": window_complete,
    }
    return {
        "schema_version": 2,
        "kind": "wide_f3_batch_interval_attribution",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "admission_authority": False,
        "measurement_only": True,
        "copyable_rate_threshold_pct_unchanged": 70.0,
        "f3_lag_limit_s_unchanged": F3_LIMIT_S,
        "instrumentation_source": (
            "data/research/wide_f3_instrumentation_events.jsonl"
        ),
        "instrumentation_completeness": completeness,
        "raw_intervals": _raw_intervals(instrumented),
        "single_event_control": control,
        "wallets": wallet_rows,
        "summary": {
            "instrumented_f2_pass_attempts": len(instrumented),
            "f3_refused_attempts": len(stale),
            "batch_interval_exposed_f3_attempts": len(batch_exposed),
            "batch_interval_exposed_f3_share_pct": (
                round(100.0 * batch_share, 6)
                if batch_share is not None
                else None
            ),
            "batch_attribution_gate_pass": gate_pass,
            "falsifier_hit": falsifier_hit,
            "decision_branch": decision,
            "decision_is_binding": decision_is_binding,
            "dual_gate_winners_current": int(
                reach_summary.get("dual_gate_winners_after") or 0
            ),
            "winner_wallets_current": reach_summary.get("winner_wallets_after") or [],
            "raw_terminal_reconciliation_pass": bool(
                reach_summary.get("input_equals_terminal")
                and reach_summary.get("input_rows")
                == reach_summary.get("terminal_rows")
            ),
        },
        "decision_rule": {
            "evidence_window": (
                "later of 24h wall-clock and 1500 instrumented F2-PASS attempts"
            ),
            "batch_gate": (
                ">=90% of F3 refusals are on multi-event token-cycles whose "
                "cycle-first-event-to-fetch-start raw interval exceeds 5s"
            ),
            "independent_falsifier": (
                "while batch gate passes, single-event and multi-event F3 refusal "
                "rates are statistically indistinguishable at two-sided alpha=0.05; "
                "each arm has >=30 attempts and every pooled expected cell is >=5"
            ),
            "scorer_independence": (
                "the cycle-first interval >5s conjunct is entailed on F3 refusals "
                "and is descriptive, not discriminating; multi-event membership "
                "drives the gate and the powered single-event control carries the "
                "inference; both raw intervals remain published"
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--measurement",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--instrumentation-events",
        default="data/research/wide_f3_instrumentation_events.jsonl",
    )
    parser.add_argument(
        "--reachability",
        default="data/research/wide_copyable_rate_reachability_latest.json",
    )
    parser.add_argument(
        "--output",
        default="data/research/wide_f3_batch_interval_attribution_latest.json",
    )
    args = parser.parse_args()
    report = build_report(
        load_instrumentation_events(args.instrumentation_events),
        load_json(args.reachability, default={}),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
