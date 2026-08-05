#!/usr/bin/env python3
"""Diagnose BTC-5m zero-submission coverage gaps for the last N hours."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_GUARD_EVENTS = "data/research/wallet_copy_live_guard_events.jsonl"
DEFAULT_HISTORY_STATE = "data/research/wallet_copy_history_state.json"
DEFAULT_OUTPUT = "data/research/coverage_gap_diagnosis_latest.json"
BTC5M_WINDOW_S = 300
REASON_CLASSES = (
    "no-eligible-signal",
    "selector-abstain",
    "price/eligibility-filter",
    "guard-reject",
    "stale-flow-protected",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--guard-events", default=DEFAULT_GUARD_EVENTS)
    parser.add_argument("--history-state", default=DEFAULT_HISTORY_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--lookback-hours", type=float, default=24.0)
    parser.add_argument("--end-ts", type=float, default=0.0, help="Unix seconds; default is now.")
    return parser.parse_args()


def _iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(float(ts), dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _window_start_from_slug(slug: Any) -> int | None:
    try:
        text = str(slug or "")
        if not text.startswith("btc-updown-5m-"):
            return None
        return int(text.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


def _window_start(row: dict[str, Any]) -> int | None:
    value = row.get("window_start_s")
    try:
        if value is not None:
            return int(float(value))
    except (TypeError, ValueError):
        pass
    return _window_start_from_slug(row.get("market_slug"))


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _slug(window_start_s: int) -> str:
    return f"btc-updown-5m-{int(window_start_s)}"


def _iter_participation_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    participation = payload.get("window_participation")
    if not isinstance(participation, dict):
        return []
    rows: list[dict[str, Any]] = []
    for key in ("window_rollups", "recent_window_rollups", "rows"):
        values = participation.get(key)
        if isinstance(values, list):
            rows.extend(row for row in values if isinstance(row, dict))
    return rows


def _row_identity(row: dict[str, Any], window_start_s: int) -> tuple[Any, ...]:
    outcomes = row.get("outcomes") if isinstance(row.get("outcomes"), list) else []
    condition_ids = row.get("condition_ids") if isinstance(row.get("condition_ids"), list) else []
    return (
        window_start_s,
        str(row.get("source_wallet") or ""),
        tuple(str(item) for item in outcomes),
        tuple(str(item) for item in condition_ids),
        str(row.get("set_generation_id") or ""),
        str(row.get("dominant_skip_reason") or ""),
    )


def _reason_counts(row: dict[str, Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    raw = row.get("dominant_skip_reason_counts")
    if isinstance(raw, dict):
        for key, value in raw.items():
            counts[str(key)] = max(counts[str(key)], _int(value))
    reason = str(row.get("dominant_skip_reason") or "").strip()
    if reason and not counts:
        counts[reason] += 1
    return counts


def collect_participation(
    *,
    guard_state: dict[str, Any],
    guard_events_path: Path,
    start_s: int,
    end_s: int,
) -> dict[str, dict[str, Any]]:
    latest_rows: dict[tuple[Any, ...], dict[str, Any]] = {}

    def ingest(payload: dict[str, Any], source: str) -> None:
        for row in _iter_participation_rows(payload):
            window_start_s = _window_start(row)
            if window_start_s is None or window_start_s < start_s or window_start_s >= end_s:
                continue
            key = _row_identity(row, window_start_s)
            current = latest_rows.get(key)
            candidate = dict(row)
            candidate["_coverage_source"] = source
            if current is None:
                latest_rows[key] = candidate
                continue
            if _int(candidate.get("wallet_eligible_orders")) >= _int(current.get("wallet_eligible_orders")):
                latest_rows[key] = candidate

    if guard_events_path.exists():
        with guard_events_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict):
                    ingest(event, "guard_events")
    ingest(guard_state, "guard_state")

    by_slug: dict[str, dict[str, Any]] = {}
    for row in latest_rows.values():
        window_start_s = _window_start(row)
        if window_start_s is None:
            continue
        slug = _slug(window_start_s)
        bucket = by_slug.setdefault(
            slug,
            {
                "market_slug": slug,
                "window_start_s": window_start_s,
                "wallet_eligible_orders": 0,
                "our_submits": 0,
                "our_fills": 0,
                "missed_active_window": False,
                "miss_pending_market_lifecycle": False,
                "skip_reasons": Counter(),
                "sources": set(),
                "source_rows": 0,
            },
        )
        bucket["wallet_eligible_orders"] += _int(row.get("wallet_eligible_orders"))
        bucket["our_submits"] += _int(row.get("our_submits"))
        bucket["our_fills"] += _int(row.get("our_fills"))
        bucket["missed_active_window"] = bool(bucket["missed_active_window"] or row.get("missed_active_window"))
        bucket["miss_pending_market_lifecycle"] = bool(bucket["miss_pending_market_lifecycle"] or row.get("miss_pending_market_lifecycle"))
        bucket["skip_reasons"].update(_reason_counts(row))
        bucket["sources"].add(str(row.get("_coverage_source") or ""))
        bucket["source_rows"] += 1
    return by_slug


def _active_roster_wallets(guard_state: dict[str, Any]) -> set[str]:
    runtime = guard_state.get("active_set_runtime") if isinstance(guard_state.get("active_set_runtime"), dict) else {}
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    members = runtime.get("members") if isinstance(runtime.get("members"), list) else []
    if not members:
        members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    wallets: set[str] = set()
    for member in members:
        if not isinstance(member, dict) or member.get("enabled") is False:
            continue
        wallet = _norm_wallet(member.get("source_wallet"))
        if wallet:
            wallets.add(wallet)
    return wallets


def _history_row_wallet(row: dict[str, Any]) -> str:
    wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet") or row.get("proxyWallet"))
    if wallet:
        return wallet
    intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
    return _norm_wallet(intent.get("source_wallet"))


def _history_row_slug(row: dict[str, Any]) -> str:
    slug = str(row.get("market_slug") or row.get("slug") or row.get("event_slug") or "")
    if slug:
        return slug
    intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
    return str(intent.get("market_slug") or "")


def _history_signal_sample(rows: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    sample: list[dict[str, Any]] = []
    for row in rows[: int(limit)]:
        sample.append(
            {
                "source_wallet": _history_row_wallet(row),
                "source": str(row.get("source") or row.get("source_name") or ""),
                "action": str(row.get("action") or row.get("side") or ""),
                "outcome": str(row.get("outcome") or ""),
                "price": row.get("price") or row.get("limit_price"),
                "event_ts": row.get("event_ts") or row.get("timestamp"),
                "observed_ts": row.get("observed_ts"),
                "tx": str(row.get("transaction_hash") or row.get("transactionHash") or row.get("tx") or ""),
            }
        )
    return sample


def collect_history_signals(
    *,
    guard_state: dict[str, Any],
    history_state: dict[str, Any],
    start_s: int,
    end_s: int,
) -> dict[str, list[dict[str, Any]]]:
    active_wallets = _active_roster_wallets(guard_state)
    rows_by_slug: dict[str, list[dict[str, Any]]] = {}
    for collection_name in ("events", "copy_intents"):
        collection = history_state.get(collection_name)
        if not isinstance(collection, list):
            continue
        for raw in collection:
            if not isinstance(raw, dict):
                continue
            wallet = _history_row_wallet(raw)
            if active_wallets and wallet not in active_wallets:
                continue
            slug = _history_row_slug(raw)
            window_start_s = _window_start_from_slug(slug)
            if window_start_s is None or window_start_s < start_s or window_start_s >= end_s:
                continue
            row = dict(raw)
            row["_history_collection"] = collection_name
            row["_history_wallet"] = wallet
            rows_by_slug.setdefault(_slug(window_start_s), []).append(row)
    return rows_by_slug


def submitted_windows(ledger: dict[str, Any], *, start_s: int, end_s: int) -> set[str]:
    out: set[str] = set()
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        window_start_s = _window_start_from_slug(order.get("market_slug"))
        if window_start_s is None or window_start_s < start_s or window_start_s >= end_s:
            continue
        out.add(_slug(window_start_s))
    return out


def classify_zero_submission(window: dict[str, Any] | None, history_signals: list[dict[str, Any]] | None = None) -> tuple[str, str]:
    if not window and history_signals:
        return "selector-abstain", "retention-selection visibility: retained active-roster wallet history saw source flow but compact guard rollup aged out"
    if not window:
        return "no-eligible-signal", "no guard rollup or submitted order observed"
    reasons = Counter(window.get("skip_reasons") or {})
    reason_text = " ".join(str(key).lower() for key in reasons)
    eligible = _int(window.get("wallet_eligible_orders"))
    missed = bool(window.get("missed_active_window"))
    if "selector" in reason_text or "candidate_not_pass" in reason_text or "selected_candidate" in reason_text:
        return "selector-abstain", "selector/pass-gate reason observed"
    if any(token in reason_text for token in ("window_time_gte", "signal_age", "stale", "late_window", "market_closed")):
        return "stale-flow-protected", "stale or late-window protection observed"
    if any(
        token in reason_text
        for token in (
            "best_ask",
            "above_vwap",
            "hard_entry",
            "toxicity",
            "profit_latency",
            "filtered",
            "not_btc_5m",
            "not_buy",
            "min_order",
            "price",
        )
    ):
        return "price/eligibility-filter", "price, eligibility, or toxicity filter observed"
    if eligible > 0 or missed:
        return "guard-reject", "wallet-eligible window had no submitted order"
    return "no-eligible-signal", "observed rollup had no eligible signal"


def abstention_evidence(
    window: dict[str, Any] | None,
    history_signals: list[dict[str, Any]],
    reason_class: str,
) -> tuple[str, dict[str, Any]]:
    """Name the predicate that foreclosed a zero-submit window and its inputs."""
    reasons = Counter((window or {}).get("skip_reasons") or {})
    if reasons:
        predicate, count = max(
            reasons.items(),
            key=lambda item: (_int(item[1]), str(item[0])),
        )
        return str(predicate), {
            "predicate_count": _int(count),
            "wallet_eligible_orders": _int((window or {}).get("wallet_eligible_orders")),
            "missed_active_window": bool((window or {}).get("missed_active_window")),
        }
    if history_signals and not window:
        return "retained_history_not_selected", {
            "history_signal_count": len(history_signals),
            "history_source_wallets": sorted(
                {
                    _history_row_wallet(row)
                    for row in history_signals
                    if _history_row_wallet(row)
                }
            ),
        }
    if reason_class == "guard-reject":
        return "guard_reject_no_named_predicate", {
            "wallet_eligible_orders": _int((window or {}).get("wallet_eligible_orders")),
            "missed_active_window": bool((window or {}).get("missed_active_window")),
        }
    return "no_eligible_signal", {
        "observed_guard_rollup": bool(window),
        "history_signal_count": len(history_signals),
        "wallet_eligible_orders": _int((window or {}).get("wallet_eligible_orders")),
    }


def build_report(
    *,
    ledger: dict[str, Any],
    guard_state: dict[str, Any],
    history_state: dict[str, Any],
    guard_events_path: Path,
    lookback_hours: float,
    end_ts: float,
) -> dict[str, Any]:
    end_s = int((end_ts or time.time()) // BTC5M_WINDOW_S) * BTC5M_WINDOW_S
    window_count = max(1, int(round(float(lookback_hours) * 3600.0 / BTC5M_WINDOW_S)))
    start_s = end_s - window_count * BTC5M_WINDOW_S
    submitted = submitted_windows(ledger, start_s=start_s, end_s=end_s)
    participation = collect_participation(
        guard_state=guard_state,
        guard_events_path=guard_events_path,
        start_s=start_s,
        end_s=end_s,
    )
    history_signals = collect_history_signals(
        guard_state=guard_state,
        history_state=history_state if isinstance(history_state, dict) else {},
        start_s=start_s,
        end_s=end_s,
    )
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    observed_zero = 0
    history_derived_zero = 0
    observed_selector_abstain = 0
    history_derived_selector_abstain = 0
    abstaining_predicates: Counter[str] = Counter()
    for window_start_s in range(start_s, end_s, BTC5M_WINDOW_S):
        slug = _slug(window_start_s)
        observed = participation.get(slug)
        if slug in submitted or (observed and _int(observed.get("our_submits")) > 0):
            continue
        history_rows = history_signals.get(slug, [])
        reason_class, detail = classify_zero_submission(observed, history_rows)
        counts[reason_class] += 1
        if observed:
            observed_zero += 1
        elif history_rows:
            history_derived_zero += 1
        if reason_class == "selector-abstain":
            if observed:
                observed_selector_abstain += 1
            elif history_rows:
                history_derived_selector_abstain += 1
        abstaining_predicate, measured_input = abstention_evidence(
            observed,
            history_rows,
            reason_class,
        )
        abstaining_predicates[abstaining_predicate] += 1
        rows.append(
            {
                "market_slug": slug,
                "window_start_s": window_start_s,
                "window_start_iso": _iso(window_start_s),
                "reason_class": reason_class,
                "reason_detail": detail,
                "abstaining_predicate": abstaining_predicate,
                "measured_input": measured_input,
                "observed_guard_rollup": bool(observed),
                "history_derived_signal": bool(history_rows),
                "history_signal_count": len(history_rows),
                "history_source_wallets": sorted({_history_row_wallet(row) for row in history_rows if _history_row_wallet(row)}),
                "history_sources": sorted({str(row.get("source") or row.get("_history_collection") or "") for row in history_rows}),
                "sample_history_signals": _history_signal_sample(history_rows),
                "wallet_eligible_orders": _int((observed or {}).get("wallet_eligible_orders")),
                "our_submits": _int((observed or {}).get("our_submits")),
                "our_fills": _int((observed or {}).get("our_fills")),
                "missed_active_window": bool((observed or {}).get("missed_active_window")),
                "skip_reasons": dict(sorted(((observed or {}).get("skip_reasons") or {}).items())),
                "sources": sorted((observed or {}).get("sources") or []),
            }
        )
    for reason in REASON_CLASSES:
        counts.setdefault(reason, 0)
    dominant = max(REASON_CLASSES, key=lambda reason: (counts[reason], reason))
    submitted_count = len(submitted)
    return {
        "schema_version": 1,
        "kind": "coverage_gap_diagnosis",
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": _iso(time.time()),
        "window": {
            "lookback_hours": float(lookback_hours),
            "start_ts": start_s,
            "start_iso": _iso(start_s),
            "end_ts": end_s,
            "end_iso": _iso(end_s),
            "window_seconds": BTC5M_WINDOW_S,
            "windows_total": window_count,
        },
        "sources": {
            "ledger_orders": DEFAULT_LEDGER,
            "guard_state": DEFAULT_GUARD_STATE,
            "guard_events": str(guard_events_path),
            "history_state": DEFAULT_HISTORY_STATE,
        },
        "summary": {
            "windows_total": window_count,
            "submitted_windows": submitted_count,
            "zero_submission_windows": len(rows),
            "observed_zero_submission_windows": observed_zero,
            "history_derived_zero_submission_windows": history_derived_zero,
            "unobserved_zero_submission_windows": len(rows) - observed_zero,
            "unobserved_without_guard_or_history_windows": len(rows) - observed_zero - history_derived_zero,
            "observed_selector_abstain_windows": observed_selector_abstain,
            "history_derived_selector_abstain_windows": history_derived_selector_abstain,
            "reason_class_counts": {reason: int(counts[reason]) for reason in REASON_CLASSES},
            "abstaining_predicates_ranked": [
                {
                    "predicate": predicate,
                    "windows_foreclosed": int(count),
                }
                for predicate, count in sorted(
                    abstaining_predicates.items(),
                    key=lambda item: (-item[1], item[0]),
                )
            ],
            "dominant_reason_class": dominant,
            "op_volume_target_windows": 144,
            "submitted_gap_to_op_volume": max(0, 144 - submitted_count),
            "submitted_coverage_pct": round(100.0 * submitted_count / window_count, 6),
        },
        "rows": rows,
    }


def main() -> int:
    args = parse_args()
    report = build_report(
        ledger=load_json(args.ledger, default={}),
        guard_state=load_json(args.guard_state, default={}),
        history_state=load_json(args.history_state, default={}),
        guard_events_path=Path(args.guard_events),
        lookback_hours=float(args.lookback_hours),
        end_ts=float(args.end_ts or 0.0),
    )
    report["sources"].update(
        {
            "ledger_orders": args.ledger,
            "guard_state": args.guard_state,
            "guard_events": args.guard_events,
            "history_state": args.history_state,
        }
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
