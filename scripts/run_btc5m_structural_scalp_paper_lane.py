#!/usr/bin/env python3
"""Run the BTC5M structural intra-window scalp paper lane.

Flow stage: LEARN/PROMOTE. This lane is paper-only: it replays observed BTC
5-minute live-window history with deterministic $1 FIFO scalp sizing and writes
the paper state needed for Fable's live-gate ruling. It never submits orders.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from collections import defaultdict, deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_btc5m_two_sided_prime_study import (  # noqa: E402
    _event,
    _load_resolutions,
    _resolutions_path,
)
from src.wallet_copy.models import CopyIntent  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_HISTORY = "data/research/btc5m_structural_scalp_forward_source_events.jsonl"
DEFAULT_HOT_SOURCE = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_STUDY = "data/research/btc5m_two_sided_prime_study_latest.json"
DEFAULT_STATE = "data/research/btc5m_structural_scalp_paper_lane_state.json"
DEFAULT_EVENTS = "data/research/btc5m_structural_scalp_paper_lane_events.jsonl"
STRUCTURAL_STRATEGY_FAMILY = "structural_scalp_v1"
STRUCTURAL_SIZING_POLICY_ID = "fixed_usd_1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--hot-source", default=DEFAULT_HOT_SOURCE)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--study", default=DEFAULT_STUDY)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--events", default=DEFAULT_EVENTS)
    parser.add_argument("--order-usd", type=float, default=1.0)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--gate-min-fills", type=int, default=30)
    parser.add_argument("--gate-window-hours", type=float, default=24.0)
    parser.add_argument("--seeded-at", default="")
    parser.add_argument("--max-current-intents", type=int, default=200)
    return parser.parse_args()


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _rel(root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        raw = float(value)
        return raw / 1000.0 if raw > 10_000_000_000 else raw
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _event_id(prefix: str, payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"{prefix}_{hashlib.sha1(raw).hexdigest()[:16]}"


def _policy_id(*, order_usd: float, tick_size: float) -> str:
    return f"structural_scalp_tick_{tick_size:g}_usd_{order_usd:g}"


def structural_copy_intent_from_row(
    row: dict[str, Any],
    *,
    mechanism_id: str,
    order_usd: float,
    policy_id: str,
) -> CopyIntent:
    action = "BUY" if str(row.get("action") or "") == "OPEN" else "SELL"
    leg = "entry" if action == "BUY" else "exit"
    price_key = "entry_price" if action == "BUY" else "exit_price"
    limit_price = _finite(row.get(price_key))
    if limit_price <= 0.0:
        raise ValueError(f"structural intent price missing for {leg}")
    condition_id = str(row.get("condition_id") or "")
    window_start_s = int(row.get("window_start_s") or 0)
    source_event_id = f"{condition_id}:{window_start_s}:{leg}"
    shares = float(order_usd) / limit_price
    return CopyIntent(
        source_wallet=f"structural::{mechanism_id}",
        wallet_name=mechanism_id,
        source_event_id=source_event_id,
        condition_id=condition_id,
        market_slug=str(row.get("market_slug") or ""),
        outcome=str(row.get("outcome") or ""),
        side="YES",
        limit_price=round(limit_price, 6),
        wallet_usdc_size=0.0,
        copy_size_usd=round(float(order_usd), 6),
        shares=round(shares, 6),
        observed_ts=float(row.get("event_ts") or 0.0),
        strategy_family=STRUCTURAL_STRATEGY_FAMILY,
        policy_id=policy_id,
        sizing_policy_id=STRUCTURAL_SIZING_POLICY_ID,
        mode="paper",
        action=action,
        order_type=f"STRUCTURAL_{leg.upper()}",
        token_id=str(row.get("token_id") or ""),
        market_id=str(row.get("market_id") or condition_id),
        event_ts=float(row.get("event_ts") or 0.0),
        live_orders_allowed=False,
        reason="structural scalp signal emitted as paper CopyIntent for guard parity",
        metadata={
            "flow_stage": "LEARN/PROMOTE",
            "mechanism_id": mechanism_id,
            "structural_leg": leg,
            "direct_submitter": False,
            "single_guard_required": True,
            "source_row_id": row.get("paper_order_id") or row.get("paper_fill_id"),
            "window_start_s": window_start_s,
        },
    )


def _metric(fills: list[dict[str, Any]]) -> dict[str, Any]:
    pnl = round(sum(_finite(row.get("pnl_usd")) for row in fills), 6)
    cost = round(sum(_finite(row.get("cost_usd")) for row in fills), 6)
    wins = sum(1 for row in fills if _finite(row.get("pnl_usd")) > 0.0)
    starts = [int(row.get("window_start_s") or 0) for row in fills if int(row.get("window_start_s") or 0) > 0]
    span_days = 0.0
    if starts:
        span_days = max((max(starts) - min(starts) + 300.0) / 86400.0, 300.0 / 86400.0)
    ev_days = max(span_days, 300.0 / 86400.0)
    return {
        "fills": len(fills),
        "wins": wins,
        "win_rate_pct": round(100.0 * wins / len(fills), 6) if fills else 0.0,
        "cost_usd": cost,
        "pnl_usd": pnl,
        "roi_pct": round(100.0 * pnl / cost, 6) if cost > 0 else 0.0,
        "span_days": round(span_days, 6),
        "ev_per_day_usd": round(pnl / ev_days, 6),
    }


def _load_history_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        if not path.exists():
            return rows
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
        return rows
    history = load_json(path, default={})
    return history.get("events") if isinstance(history.get("events"), list) else []


def _forward_cache_key(row: dict[str, Any]) -> str:
    key = str(row.get("event_id") or row.get("id") or "")
    return key if key else _event_id("source", row)


def _forward_cache_shards(path: Path) -> list[Path]:
    return sorted(path.parent.glob(f"{path.stem}_shard_*.jsonl"))


def _merge_forward_source_cache(
    path: Path,
    current_rows: list[dict[str, Any]],
    *,
    seeded_at_ts: float,
) -> list[dict[str, Any]]:
    """Persist rolling live snapshots so a three-day forward clock can accrue."""
    shard_rows: list[dict[str, Any]] = []
    for shard_path in _forward_cache_shards(path):
        shard_rows.extend(_load_history_rows(shard_path))
    active_rows = _load_history_rows(path)
    prior = [*shard_rows, *active_rows]
    frozen_keys = {_forward_cache_key(row) for row in shard_rows}
    merged: dict[str, dict[str, Any]] = {}
    for row in [*prior, *current_rows]:
        event_ts = _parse_ts(row.get("event_ts") or row.get("observed_ts") or row.get("timestamp"))
        if seeded_at_ts > 0 and event_ts < seeded_at_ts:
            continue
        merged[_forward_cache_key(row)] = row
    rows = sorted(
        merged.values(),
        key=lambda row: _parse_ts(row.get("event_ts") or row.get("observed_ts") or row.get("timestamp")),
    )
    active_write_rows = [
        row for key, row in merged.items()
        if key not in frozen_keys
    ]
    active_write_rows.sort(
        key=lambda row: _parse_ts(row.get("event_ts") or row.get("observed_ts") or row.get("timestamp")),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            for row in active_write_rows:
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return rows


def _load_events(
    root: Path,
    args: argparse.Namespace,
    *,
    seeded_at_ts: float,
) -> tuple[list[dict[str, Any]], dict[str, int], str, dict[str, Any]]:
    history_path = root / args.history
    resolutions = _resolutions_path(root, args.resolutions)
    winners = _load_resolutions(resolutions)
    diagnostics: dict[str, int] = defaultdict(int)
    parsed: list[dict[str, Any]] = []
    uses_forward_accumulator = history_path.name == Path(DEFAULT_HISTORY).name
    hot_source_arg = str(getattr(args, "hot_source", DEFAULT_HOT_SOURCE) or DEFAULT_HOT_SOURCE)
    hot_source_path = Path(hot_source_arg)
    if not hot_source_path.is_absolute():
        hot_source_path = root / hot_source_path
    current_rows = _load_history_rows(hot_source_path if uses_forward_accumulator else history_path)
    raw_events = (
        _merge_forward_source_cache(history_path, current_rows, seeded_at_ts=seeded_at_ts)
        if uses_forward_accumulator
        else current_rows
    )
    for row in raw_events:
        if not isinstance(row, dict):
            continue
        event = _event(row, winners)
        if event is None:
            diagnostics["skipped_unusable"] += 1
            continue
        parsed.append(event)
        diagnostics["accepted_events"] += 1
    newest_source_event_ts = max(
        (_parse_ts(row.get("event_ts") or row.get("observed_ts") or row.get("timestamp")) for row in current_rows),
        default=0.0,
    )
    now_ts = datetime.now(tz=UTC).timestamp()
    freshness = {
        "newest_source_event_ts": newest_source_event_ts or None,
        "newest_source_event_age_s": round(max(0.0, now_ts - newest_source_event_ts), 6)
        if newest_source_event_ts
        else None,
        "freshness_limit_s": 86400.0,
        "freshness_pass": bool(newest_source_event_ts and now_ts - newest_source_event_ts <= 86400.0),
        "hot_source": _rel(root, hot_source_path) if uses_forward_accumulator else None,
        "source_cache": _rel(root, history_path) if uses_forward_accumulator else None,
        "cached_forward_source_events": len(raw_events),
    }
    return parsed, dict(sorted(diagnostics.items())), _rel(root, resolutions), freshness


def _run_scalp_replay(events: list[dict[str, Any]], *, order_usd: float, tick_size: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    lots: dict[tuple[str, str, str], deque[dict[str, Any]]] = defaultdict(deque)
    opens: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    ordered = sorted(events, key=lambda item: (float(item.get("event_ts") or 0.0), item.get("action") != "BUY"))
    for event in ordered:
        key = (str(event["market_slug"]), str(event["wallet"]), str(event["outcome"]))
        if event["action"] == "BUY":
            entry_price = min(0.99, _finite(event.get("price")) + tick_size)
            if entry_price <= 0.0:
                continue
            shares = min(_finite(event.get("size")), order_usd / entry_price)
            if shares <= 0.0:
                continue
            opened = {
                "action": "OPEN",
                "condition_id": event.get("condition_id"),
                "entry_price": round(entry_price, 6),
                "event_ts": event.get("event_ts"),
                "market_slug": event.get("market_slug"),
                "outcome": event.get("outcome"),
                "paper_order_usd": round(entry_price * shares, 6),
                "shares": round(shares, 6),
                "source_price": event.get("price"),
                "source_wallet": event.get("wallet"),
                "window_start_s": event.get("window_start_s"),
            }
            opened["paper_order_id"] = _event_id("pscalp_open", opened)
            lots[key].append({**opened, "remaining_shares": shares})
            opens.append(opened)
            continue

        exit_price = max(0.01, _finite(event.get("price")) - tick_size)
        remaining = _finite(event.get("size"))
        while remaining > 1e-9 and lots[key]:
            lot = lots[key][0]
            matched = min(_finite(lot.get("remaining_shares")), remaining)
            if matched <= 0.0:
                lots[key].popleft()
                continue
            entry_price = _finite(lot.get("entry_price"))
            pnl = (exit_price - entry_price) * matched
            closed = {
                "action": "CLOSE",
                "condition_id": event.get("condition_id"),
                "cost_usd": round(entry_price * matched, 6),
                "entry_price": round(entry_price, 6),
                "event_ts": event.get("event_ts"),
                "exit_price": round(exit_price, 6),
                "market_slug": event.get("market_slug"),
                "matched_shares": round(matched, 6),
                "open_order_id": lot.get("paper_order_id"),
                "outcome": event.get("outcome"),
                "pnl_usd": round(pnl, 6),
                "source_price": event.get("price"),
                "source_wallet": event.get("wallet"),
                "window_start_s": event.get("window_start_s"),
            }
            closed["paper_fill_id"] = _event_id("pscalp_fill", closed)
            fills.append(closed)
            lot["remaining_shares"] = _finite(lot.get("remaining_shares")) - matched
            remaining -= matched
            if _finite(lot.get("remaining_shares")) <= 1e-9:
                lots[key].popleft()
    return opens, fills


def _current_intents(
    opens: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    *,
    mechanism_id: str,
    order_usd: float,
    policy_id: str,
    forward_floor_window_start_s: int,
    max_current_intents: int,
) -> list[dict[str, Any]]:
    rows = sorted(
        [
            row
            for row in [*opens, *fills]
            if int(row.get("window_start_s") or 0) >= int(forward_floor_window_start_s)
        ],
        key=lambda item: (float(item.get("event_ts") or 0.0), str(item.get("action") or "")),
    )
    if max_current_intents > 0:
        rows = rows[-max_current_intents:]
    intents: list[dict[str, Any]] = []
    for row in rows:
        try:
            intents.append(
                structural_copy_intent_from_row(
                    row,
                    mechanism_id=mechanism_id,
                    order_usd=order_usd,
                    policy_id=policy_id,
                ).asdict()
            )
        except ValueError:
            continue
    return intents


def _evidence_floors(
    *, latest_window_start_s: int, seeded_at_ts: float, gate_window_hours: float
) -> tuple[int, int]:
    """Return the fixed forward-clock floor and the separate rolling diagnostic floor."""
    seed_floor = int(seeded_at_ts // 300 * 300 + 300) if seeded_at_ts > 0 else 0
    rolling_floor = (
        latest_window_start_s - int(gate_window_hours * 3600.0) + 300
        if latest_window_start_s
        else 0
    )
    return seed_floor, rolling_floor


def build_state(root: Path, args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    previous_state = load_json(root / args.state, default={})
    previous_state = previous_state if isinstance(previous_state, dict) else {}
    generated_at = _utc_now_iso()
    previous_inputs = previous_state.get("inputs") if isinstance(previous_state.get("inputs"), dict) else {}
    previous_history = str(previous_inputs.get("history") or "")
    current_history = str(args.history)
    live_source_family = {
        DEFAULT_HISTORY,
        DEFAULT_HOT_SOURCE,
        "data/research/wallet_copy_live_guard_wallet_events.jsonl",
    }
    equivalent_live_source_migration = previous_history in live_source_family and current_history in live_source_family
    source_changed = bool(previous_state) and previous_history != current_history and not equivalent_live_source_migration
    seeded_at = str(
        getattr(args, "seeded_at", "")
        or ("" if source_changed else previous_state.get("seeded_at"))
        or generated_at
    )
    seeded_at_ts = _parse_ts(seeded_at)
    events, diagnostics, resolutions, source_freshness = _load_events(
        root,
        args,
        seeded_at_ts=seeded_at_ts,
    )
    opens, fills = _run_scalp_replay(events, order_usd=float(args.order_usd), tick_size=float(args.tick_size))
    latest_window = max((int(row.get("window_start_s") or 0) for row in fills), default=0)
    forward_floor, gate_floor = _evidence_floors(
        latest_window_start_s=latest_window,
        seeded_at_ts=seeded_at_ts,
        gate_window_hours=float(args.gate_window_hours),
    )
    gate_fills = [row for row in fills if int(row.get("window_start_s") or 0) >= gate_floor]
    forward_fills = [row for row in fills if int(row.get("window_start_s") or 0) >= forward_floor]
    total_metric = _metric(fills)
    gate_metric = _metric(gate_fills)
    forward_metric = _metric(forward_fills)
    generated_at_ts = _parse_ts(generated_at)
    forward_clock_days = (
        max(0.0, (generated_at_ts - seeded_at_ts) / 86400.0)
        if generated_at_ts > 0 and seeded_at_ts > 0
        else 0.0
    )
    forward_gate_passed = (
        bool(source_freshness["freshness_pass"])
        and forward_clock_days >= 3.0
        and int(forward_metric["fills"]) >= int(args.gate_min_fills)
        and float(forward_metric["pnl_usd"]) > 0.0
    )
    study = load_json(root / args.study, default={})
    study_scalp = study.get("intra_window_scalp") if isinstance(study.get("intra_window_scalp"), dict) else {}
    mechanism_id = "structural-intra-window-scalp"
    policy_id = _policy_id(order_usd=float(args.order_usd), tick_size=float(args.tick_size))
    current_intents = _current_intents(
        opens,
        fills,
        mechanism_id=mechanism_id,
        order_usd=float(args.order_usd),
        policy_id=policy_id,
        forward_floor_window_start_s=forward_floor,
        max_current_intents=int(getattr(args, "max_current_intents", 200)),
    )
    state = {
        "schema_version": 1,
        "kind": "btc5m_structural_scalp_paper_lane_state",
        "flow_stage": "LEARN/PROMOTE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": generated_at,
        "seeded_at": seeded_at,
        "lane_id": "paper_struct_intra_window_scalp",
        "mechanism_id": mechanism_id,
        "inputs": {
            "history": args.history,
            "resolutions": resolutions,
            "study": args.study,
            "order_usd": float(args.order_usd),
            "tick_size": float(args.tick_size),
            "gate_min_fills": int(args.gate_min_fills),
            "gate_window_hours": float(args.gate_window_hours),
            "forward_floor_window_start_s": forward_floor,
            "rolling_gate_floor_window_start_s": gate_floor,
            "forward_clock_elapsed_days": round(forward_clock_days, 6),
            "max_current_intents": int(getattr(args, "max_current_intents", 200)),
            **source_freshness,
        },
        "summary": {
            "accepted_source_events": int(diagnostics.get("accepted_events", 0)),
            "diagnostics": diagnostics,
            "gate_passed": forward_gate_passed,
            "replay_gate_passed": int(gate_metric["fills"]) >= int(args.gate_min_fills)
            and float(gate_metric["pnl_usd"]) > 0.0,
            "forward_gate_passed": forward_gate_passed,
            "latest_window_start_s": latest_window,
            "open_orders": len(opens),
            "paper_fills": len(fills),
            "paper_orders": len(opens),
            "paper_pnl_usd": total_metric["pnl_usd"],
            "forward_fills": forward_metric["fills"],
            "forward_pnl_usd": forward_metric["pnl_usd"],
            "current_intents": len(current_intents),
            "study_ev_per_day_usd": study_scalp.get("ev_per_day_usd"),
            "study_oos_trades": study_scalp.get("oos_trades"),
        },
        "metrics": {
            "all_time": total_metric,
            "historical_gate_24h": gate_metric,
            "gate_24h": gate_metric,
            "forward": forward_metric,
        },
        "forward_gate": {
            "status": "FORWARD_GATE_PENDING",
            "seeded_at": seeded_at,
            "seeded_at_ts": seeded_at_ts,
            "forward_floor_window_start_s": forward_floor,
            "basis": "fixed_seeded_at_cumulative",
            "rolling_24h_diagnostic_floor_window_start_s": gate_floor,
            "evidence_passed": forward_gate_passed,
            "requires": "fresh source <=24h, >=3 measured forward days, >=30 fills, positive full-span PnL; Fable owns live flip",
        },
        "live_gate": {
            "status": "FORWARD_GATE_PENDING",
            "requires": "fresh source <=24h, >=3 measured forward days, >=30 fills, positive full-span PnL; Fable decision required",
            "ready_for_live": False,
            "evidence_passed": forward_gate_passed,
            "rescinded_status": "PAPER_GATE_PASS was circular historical replay evidence, not forward evidence",
            "next": "accrue from the live guard event stream until the three-day freshness gate is available",
        },
        "single_guard_contract": {
            "direct_submitter": False,
            "live_path": "StructuralIntent -> CopyIntent parity adapter -> scripts/run_wallet_copy_live_guard.py",
            "structural_invariant": "single live guard remains the sole order submitter",
        },
        "adapter_contract": {
            "current_intents_field": "current_intents",
            "strategy_family": STRUCTURAL_STRATEGY_FAMILY,
            "sizing_policy_id": STRUCTURAL_SIZING_POLICY_ID,
            "source_wallet": f"structural::{mechanism_id}",
            "mode": "paper",
            "live_orders_allowed": False,
            "direct_submitter": False,
        },
        "current_intents": current_intents,
        "recent_fills": fills[-20:],
    }
    event_rows = [{**row, "lane_id": state["lane_id"], "mechanism_id": state["mechanism_id"]} for row in fills]
    return state, event_rows


def write_events(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def main() -> int:
    args = parse_args()
    state, events = build_state(ROOT, args)
    state_path = Path(args.state)
    if not state_path.is_absolute():
        state_path = ROOT / state_path
    events_path = Path(args.events)
    if not events_path.is_absolute():
        events_path = ROOT / events_path
    atomic_write_json(state_path, state)
    write_events(events_path, events)
    print(json.dumps(state["summary"], indent=2, sort_keys=True))
    print(json.dumps(state["live_gate"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
