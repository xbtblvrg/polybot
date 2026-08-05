#!/usr/bin/env python3
"""Seed and refresh the copy-event-triggered scheduler paper lane.

Flow stage: LEARN/PROMOTE. This is paper-only measurement. It detects source
events that would have been recovered by scheduling an immediate evaluation
cycle on event arrival, without touching the live guard or submitting orders.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402


DEFAULT_REPLAY = "data/research/copy_event_triggered_cycle_scheduler_replay_20260715.json"
DEFAULT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_GUARD_EVENTS = "data/research/wallet_copy_live_guard_events.jsonl"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/copy_event_triggered_cycle_scheduler_paper_lane_latest.json"
DEFAULT_CLOCK_START = "2026-07-15T01:45:00Z"
MARKET_SLUG_TS_RE = re.compile(r"-(\d{10})(?:$|[^0-9])")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value else None


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _event_time(row: dict[str, Any]) -> datetime | None:
    for key in ("observed_ts", "received_ts", "event_ts", "ts"):
        value = row.get(key)
        if value is None:
            continue
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            parsed = _parse_iso(value)
            if parsed is not None:
                return parsed
    return None


def _market_window_start(market_slug: Any) -> int | None:
    match = MARKET_SLUG_TS_RE.search(str(market_slug or ""))
    if not match:
        return None
    return int(match.group(1))


def _market_close(market_slug: Any, *, window_seconds: int = 300) -> datetime | None:
    window_start = _market_window_start(market_slug)
    if window_start is None:
        return None
    return datetime.fromtimestamp(window_start + int(window_seconds), tz=timezone.utc)


def _candidate_by_wallet(guard_state: dict[str, Any]) -> dict[str, str]:
    members = ((guard_state.get("active_set_runtime") or {}).get("members")) or []
    out: dict[str, str] = {}
    for row in members:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        candidate_id = str(row.get("candidate_id") or "")
        if wallet and candidate_id:
            out[wallet] = candidate_id
    return out


def _load_guard_cycles(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            profile = row.get("guard_loop_profile") if isinstance(row.get("guard_loop_profile"), dict) else {}
            started = _parse_iso(profile.get("cycle_started_at") or row.get("generated_at"))
            if started is None:
                continue
            rows.append(
                {
                    "cycle_started_at": started,
                    "source_wallet": str(row.get("source_wallet") or "").lower(),
                    "candidate_id": str(row.get("candidate_id") or ""),
                    "cycle": row.get("cycle"),
                    "pid": row.get("pid"),
                }
            )
    return rows


def _next_matching_cycle(
    cycles: list[dict[str, Any]],
    *,
    source_wallet: str,
    candidate_id: str,
    event_at: datetime,
    close_at: datetime | None,
) -> dict[str, Any] | None:
    wallet = source_wallet.lower()
    matches = [
        row
        for row in cycles
        if row["cycle_started_at"] >= event_at
        and (close_at is None or row["cycle_started_at"] <= close_at)
        and (row.get("source_wallet") == wallet or row.get("candidate_id") == candidate_id)
    ]
    return min(matches, key=lambda row: row["cycle_started_at"]) if matches else None


def _is_btc5m_buy(row: dict[str, Any]) -> bool:
    if str(row.get("asset") or "").upper() != "BTC":
        return False
    if str(row.get("duration") or "").lower() != "5m":
        return False
    if str(row.get("action") or row.get("side") or "").upper() != "BUY":
        return False
    return str(row.get("row_type") or "").lower() == "trade"


def _event_price(row: dict[str, Any]) -> float | None:
    try:
        return float(row.get("price"))
    except (TypeError, ValueError):
        return None


def _stable_event_key(row: dict[str, Any]) -> str:
    for key in ("event_id", "transaction_hash", "source_fingerprint"):
        value = str(row.get(key) or "")
        if value:
            return value
    return "|".join(
        str(row.get(key) or "")
        for key in ("source_wallet", "market_slug", "outcome", "observed_ts", "event_ts")
    )


def _synthetic_paper_order(
    row: dict[str, Any],
    *,
    event_at: datetime,
    price: float,
    paper_order_size_usd: float,
) -> dict[str, Any]:
    cost = max(0.0, float(paper_order_size_usd))
    shares = round(cost / price, 9) if price > 0 else 0.0
    return {
        "order_id": f"r8_event_triggered_scheduler:{_stable_event_key(row)}",
        "intent_id": f"r8_event_triggered_scheduler:{_stable_event_key(row)}",
        "source_wallet": str(row.get("source_wallet") or "").lower(),
        "wallet_name": "copy_event_triggered_cycle_scheduler_paper_lane",
        "condition_id": row.get("condition_id"),
        "market_slug": row.get("market_slug"),
        "outcome": row.get("outcome"),
        "token_id": row.get("token_id"),
        "status": "FILLED",
        "final_status": "FILLED",
        "limit_price": round(price, 8),
        "filled_size_usd": round(cost, 6),
        "filled_shares": shares,
        "submitted_at": _iso(event_at),
        "source_intent": {
            "condition_id": row.get("condition_id"),
            "market_slug": row.get("market_slug"),
            "token_id": row.get("token_id"),
            "event_ts": row.get("event_ts"),
        },
    }


def _replay_seed_rows(replay: dict[str, Any], *, sample_limit: int) -> list[dict[str, Any]]:
    rows = []
    for row in replay.get("rows") or []:
        if not isinstance(row, dict):
            continue
        if not row.get("counterfactual_fresh_overlap_recovered"):
            continue
        rows.append(
            {
                "row_type": "replay_seed_recovered_window",
                "candidate_id": row.get("candidate_id"),
                "source_wallet": row.get("source_wallet"),
                "market_slug": row.get("market_slug"),
                "first_policy_received_iso": row.get("first_policy_received_iso"),
                "counterfactual_trigger_at_iso": row.get("counterfactual_trigger_at_iso"),
                "source_rows": row.get("source_rows"),
                "post_fee_would_pnl_status": "SEED_REPLAY_ONLY_NOT_48H_CLOCK_RESULT",
            }
        )
    return rows[: max(0, int(sample_limit))]


def build_state(
    *,
    replay: dict[str, Any],
    history: dict[str, Any],
    guard_state: dict[str, Any],
    guard_cycles: list[dict[str, Any]],
    clock_start: datetime,
    clock_hours: float,
    fresh_horizon_s: float,
    max_price: float,
    sample_limit: int,
    resolutions: dict[str, dict[str, Any]] | None = None,
    previous_state: dict[str, Any] | None = None,
    paper_order_size_usd: float = 1.0,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or _utc_now()
    previous_state = previous_state if isinstance(previous_state, dict) else {}
    stored_clock = _parse_iso(previous_state.get("clock_start_utc"))
    if stored_clock is not None:
        clock_start = stored_clock
    clock_end = clock_start + timedelta(hours=float(clock_hours))
    candidate_by_wallet = _candidate_by_wallet(guard_state)
    resolutions = resolutions if isinstance(resolutions, dict) else {}

    # Persistent paper-clock accumulator: the history input is a rolling
    # buffer, so per-scan counters shrink as clock rows age out. The gate
    # metric must accumulate landed rows across regens, keyed by stable
    # event key, with pending rows re-scored until resolution.
    accumulator: dict[str, dict[str, Any]] = {}
    prev_accumulator = previous_state.get("paper_clock_accumulator")
    if isinstance(prev_accumulator, dict):
        for acc_key, acc_entry in prev_accumulator.items():
            if isinstance(acc_entry, dict):
                accumulator[str(acc_key)] = dict(acc_entry)
    accumulation_started_at = (
        _parse_iso(previous_state.get("paper_clock_accumulation_started_at")) or generated_at
    )
    scanned_keys: set[str] = set()

    rows: list[dict[str, Any]] = []
    resolved_post_fee_pnl = 0.0
    resolved_measured_rows = 0
    resolved_windows: set[str] = set()
    resolved_window_pnl: dict[str, float] = {}
    resolved_positive_rows = 0
    recovered_candidates = 0
    for row in history.get("events") or []:
        if not isinstance(row, dict) or not _is_btc5m_buy(row):
            continue
        event_at = _event_time(row)
        if event_at is None or event_at < clock_start:
            continue
        price = _event_price(row)
        if price is None or price > float(max_price):
            continue
        source_wallet = str(row.get("source_wallet") or "").lower()
        candidate_id = candidate_by_wallet.get(source_wallet, "")
        if not candidate_id:
            continue
        close_at = _market_close(row.get("market_slug"))
        if close_at is not None and event_at > close_at:
            continue
        next_cycle = _next_matching_cycle(
            guard_cycles,
            source_wallet=source_wallet,
            candidate_id=candidate_id,
            event_at=event_at,
            close_at=close_at,
        )
        next_cycle_lag_s = (
            (next_cycle["cycle_started_at"] - event_at).total_seconds()
            if next_cycle is not None
            else None
        )
        recovered = next_cycle_lag_s is None or next_cycle_lag_s > float(fresh_horizon_s)
        if not recovered:
            continue
        recovered_candidates += 1
        scored_order = _synthetic_paper_order(
            row,
            event_at=event_at,
            price=price,
            paper_order_size_usd=paper_order_size_usd,
        )
        score = score_order(scored_order, resolutions)
        measured = bool(score.get("resolved"))
        market_slug = str(row.get("market_slug") or "")
        pnl = float(score.get("pnl_usd") or 0.0) if measured else None
        if measured:
            resolved_measured_rows += 1
            resolved_windows.add(market_slug)
            resolved_post_fee_pnl += float(pnl or 0.0)
            resolved_window_pnl[market_slug] = resolved_window_pnl.get(market_slug, 0.0) + float(pnl or 0.0)
            if float(pnl or 0.0) > 0.0:
                resolved_positive_rows += 1
        row_payload = {
            "row_type": "paper_clock_recovered_candidate",
            "candidate_id": candidate_id,
            "source_wallet": source_wallet,
            "market_slug": row.get("market_slug"),
            "event_id": row.get("event_id"),
            "transaction_hash": row.get("transaction_hash"),
            "event_at": _iso(event_at),
            "counterfactual_trigger_at_iso": _iso(event_at),
            "market_close_iso": _iso(close_at),
            "price": price,
            "outcome": row.get("outcome"),
            "next_actual_cycle_lag_s": (
                round(next_cycle_lag_s, 6) if next_cycle_lag_s is not None else None
            ),
            "paper_order_size_usd": round(float(paper_order_size_usd), 6),
            "post_fee_would_pnl_status": (
                "RESOLVED_POST_FEE_MEASURED" if measured else "PENDING_RESOLUTION_OR_JOIN"
            ),
            "post_fee_would_pnl_usd": round(float(pnl), 6) if pnl is not None else None,
        }
        if measured:
            row_payload["score"] = score
        rows.append(row_payload)

        row_key = _stable_event_key(row)
        scanned_keys.add(row_key)
        existing = accumulator.get(row_key)
        already_resolved = (
            isinstance(existing, dict)
            and existing.get("post_fee_would_pnl_status") == "RESOLVED_POST_FEE_MEASURED"
        )
        if not already_resolved:
            acc_entry = {k: v for k, v in row_payload.items() if k != "score"}
            if measured:
                acc_entry.pop("paper_order", None)
            else:
                acc_entry["paper_order"] = scored_order
            accumulator[row_key] = acc_entry

    # Re-score pending accumulator rows whose events aged out of the rolling
    # history; the stored synthetic paper order is enough to score them.
    for acc_key, acc_entry in accumulator.items():
        if acc_key in scanned_keys:
            continue
        if acc_entry.get("post_fee_would_pnl_status") == "RESOLVED_POST_FEE_MEASURED":
            continue
        paper_order = acc_entry.get("paper_order")
        if not isinstance(paper_order, dict):
            continue
        late_score = score_order(paper_order, resolutions)
        if late_score.get("resolved"):
            acc_entry["post_fee_would_pnl_status"] = "RESOLVED_POST_FEE_MEASURED"
            acc_entry["post_fee_would_pnl_usd"] = round(float(late_score.get("pnl_usd") or 0.0), 6)
            acc_entry.pop("paper_order", None)

    acc_rows_landed = len(accumulator)
    acc_rows_resolved = 0
    acc_post_fee_pnl = 0.0
    acc_positive_rows = 0
    acc_window_pnl: dict[str, float] = {}
    for acc_entry in accumulator.values():
        if acc_entry.get("post_fee_would_pnl_status") != "RESOLVED_POST_FEE_MEASURED":
            continue
        acc_rows_resolved += 1
        acc_pnl = float(acc_entry.get("post_fee_would_pnl_usd") or 0.0)
        acc_post_fee_pnl += acc_pnl
        acc_slug = str(acc_entry.get("market_slug") or "")
        acc_window_pnl[acc_slug] = acc_window_pnl.get(acc_slug, 0.0) + acc_pnl
        if acc_pnl > 0.0:
            acc_positive_rows += 1
    acc_positive_windows = sum(1 for value in acc_window_pnl.values() if value > 0.0)

    rows.sort(key=lambda item: str(item.get("event_at") or item.get("first_policy_received_iso") or ""), reverse=True)
    resolved_sample_rows = [
        row for row in rows if row.get("post_fee_would_pnl_status") == "RESOLVED_POST_FEE_MEASURED"
    ][: min(10, max(0, int(sample_limit)))]
    seed_rows = _replay_seed_rows(replay, sample_limit=sample_limit)
    clock_complete = generated_at >= clock_end
    resolved_positive_windows = sum(1 for value in resolved_window_pnl.values() if value > 0.0)
    if acc_positive_windows > 0 and acc_post_fee_pnl > 0:
        status = "PAPER_CLOCK_POSITIVE_ACCRUING"
    elif clock_complete:
        status = "PAPER_CLOCK_MATURE_NO_POSITIVE_RESOLVED_WINDOWS"
    else:
        status = "PAPER_CLOCK_RUNNING"

    return {
        "schema_version": 1,
        "kind": "copy_event_triggered_cycle_scheduler_paper_lane",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": _iso(generated_at),
        "paper_only": True,
        "live_orders_allowed": False,
        "copyintent_parity_change": False,
        "single_submitter_change": False,
        "guard_code_touched": False,
        "source": "Fable 2026-07-15T01:38Z R8 paper seed approval",
        "status": status,
        "clock_start_utc": _iso(clock_start),
        "clock_end_utc": _iso(clock_end),
        "criteria": {
            "fresh_horizon_s": float(fresh_horizon_s),
            "clock_hours": float(clock_hours),
            "max_price": float(max_price),
            "paper_order_size_usd": round(float(paper_order_size_usd), 6),
            "success": "48h paper clock recovered-class windows have aggregate post-fee would-PnL > 0",
            "failure": "post-fee would-PnL <= 0 or zero recovered-class windows by clock maturity",
        },
        "summary": {
            "replay_seed_status": replay.get("status"),
            "replay_recovered_windows": (replay.get("summary") or {}).get(
                "recovered_source_active_but_no_fresh_cycle_overlap_windows"
            ),
            "paper_clock_recovered_candidates": recovered_candidates,
            "paper_clock_rows_landed": acc_rows_landed,
            "paper_clock_rows_resolved": acc_rows_resolved,
            "paper_clock_post_fee_would_pnl_usd": round(acc_post_fee_pnl, 6),
            "paper_clock_resolved_positive_rows": acc_positive_rows,
            "paper_clock_resolved_windows": len(acc_window_pnl),
            "paper_clock_resolved_positive_windows": acc_positive_windows,
            "paper_clock_basis": "persistent_accumulator_gate_metric",
            "paper_clock_accumulation_started_at": _iso(accumulation_started_at),
            "rolling_scan_basis": (
                "resolved_measured_* and aggregate_post_fee_would_pnl_usd are "
                "recomputed from the rolling hot history each regen; context only, "
                "never the gate metric"
            ),
            "resolved_measured_rows": resolved_measured_rows,
            "resolved_measured_windows": len(resolved_windows),
            "resolved_positive_rows": resolved_positive_rows,
            "resolved_positive_windows": resolved_positive_windows,
            "aggregate_post_fee_would_pnl_usd": round(resolved_post_fee_pnl, 6),
            "clock_complete": clock_complete,
            "next": (
                "keep launchd paper lane running until the 48h clock matures or Fable rules on a positive packet"
                if not clock_complete
                else "write Fable verdict packet for R8 paper scheduler"
            ),
        },
        "replay_seed_rows": seed_rows,
        "resolved_sample_rows": resolved_sample_rows,
        "rows": rows[: max(0, int(sample_limit))],
        "paper_clock_accumulation_started_at": _iso(accumulation_started_at),
        "paper_clock_accumulator": accumulator,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", default=DEFAULT_REPLAY)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--guard-events", default=DEFAULT_GUARD_EVENTS)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--clock-start-utc", default=DEFAULT_CLOCK_START)
    parser.add_argument("--clock-hours", type=float, default=48.0)
    parser.add_argument("--fresh-horizon-s", type=float, default=30.0)
    parser.add_argument("--max-price", type=float, default=0.5)
    parser.add_argument("--paper-order-size-usd", type=float, default=1.0)
    parser.add_argument("--sample-limit", type=int, default=50)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.output)
    payload = build_state(
        replay=load_json(args.replay, default={}) or {},
        history=load_json(args.history, default={}) or {},
        guard_state=load_json(args.guard_state, default={}) or {},
        guard_cycles=_load_guard_cycles(Path(args.guard_events)),
        resolutions=load_resolutions(args.resolutions),
        previous_state=load_json(output_path, default={}) or {},
        clock_start=_parse_iso(args.clock_start_utc) or _utc_now(),
        clock_hours=float(args.clock_hours),
        fresh_horizon_s=float(args.fresh_horizon_s),
        max_price=float(args.max_price),
        paper_order_size_usd=float(args.paper_order_size_usd),
        sample_limit=int(args.sample_limit),
    )
    atomic_write_json(output_path, payload)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
