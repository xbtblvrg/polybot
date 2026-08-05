#!/usr/bin/env python3
"""Score floor-blocked wallet-copy opportunities without changing live policy."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_FREEZE_HARVEST = ROOT / "data/research/order4_selection_visibility_freeze_harvest_latest.json"
DEFAULT_GUARD_EVENTS = ROOT / "data/research/wallet_copy_live_guard_events.jsonl"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_STATE = ROOT / "data/research/floor_opportunity_cost_counterfactual_state.json"
DEFAULT_EVENT_LOG = ROOT / "data/research/floor_opportunity_cost_counterfactual_events.jsonl"
DEFAULT_MIN_RESOLVED = 30


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _parse_iso_ts(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _ts_to_iso(value: Any) -> str | None:
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _iter_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _is_btc5m(row: dict[str, Any]) -> bool:
    return str(row.get("market_slug") or "").startswith("btc-updown-5m-")


def _row_event_ts(row: dict[str, Any]) -> float:
    return float(
        num(row.get("counterfactual_event_ts"))
        or num(row.get("event_ts"))
        or num(row.get("effective_latest_observed_ts"))
        or num(row.get("latest_observed_ts"))
        or num(row.get("source_detection_observed_ts"))
        or num(row.get("alternate_observed_ts"))
        or num(row.get("window_start_s"))
    )


def _floor_row(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    if str(row.get("dominant_skip_reason") or "") != "drip_min_tranche_exceeds_window_budget":
        return False
    if row.get("floor_blocked_miss") is not True:
        return False
    if str(row.get("participation_skip_category") or "") != "FLOOR_BLOCKED_MISS":
        return False
    if int(row.get("wallet_eligible_orders") or 0) <= 0:
        return False
    if not str(row.get("source_wallet") or "").lower().startswith("0x"):
        return False
    if not _is_btc5m(row):
        return False
    if not str(row.get("condition_id") or ""):
        return False
    if str(row.get("outcome") or "") not in {"Up", "Down"}:
        return False
    return True


def _price_for_row(row: dict[str, Any]) -> float:
    price = num(row.get("source_inventory_vwap"))
    if price > 0:
        return price
    target_usd = num(row.get("target_usd_at_vwap"))
    target_shares = num(row.get("target_shares"))
    if target_usd > 0 and target_shares > 0:
        return target_usd / target_shares
    return 0.0


def _cost_for_row(row: dict[str, Any]) -> float:
    for key in ("guard_sized_copy_usd", "target_usd_at_vwap", "gap_usd_at_vwap", "window_budget_usd"):
        cost = num(row.get(key))
        if cost > 0:
            return cost
    return 0.0


def _event_key(row: dict[str, Any]) -> str:
    return stable_id(
        "floor_opportunity_cost",
        {
            "source_wallet": str(row.get("source_wallet") or "").lower(),
            "market_slug": row.get("market_slug"),
            "condition_id": row.get("condition_id"),
            "outcome": row.get("outcome"),
            "window_start_s": int(num(row.get("window_start_s"))),
            "first_seen_at": row.get("first_seen_at"),
        },
        length=24,
    )


def _candidate_rows(
    guard_state: dict[str, Any],
    *,
    quiet_start_ts: float | None,
    freeze_ts: float | None,
) -> list[dict[str, Any]]:
    participation = guard_state.get("window_participation")
    participation = participation if isinstance(participation, dict) else {}
    rows = participation.get("rows") if isinstance(participation.get("rows"), list) else []
    by_key: dict[str, dict[str, Any]] = {}
    for raw in rows:
        if not _floor_row(raw):
            continue
        row = dict(raw)
        window_start = num(row.get("window_start_s"))
        if quiet_start_ts is not None and window_start < quiet_start_ts:
            continue
        event_ts = _row_event_ts(row)
        if freeze_ts is not None and event_ts > freeze_ts:
            continue
        price = _price_for_row(row)
        cost = _cost_for_row(row)
        if price <= 0 or cost <= 0:
            continue
        row["source_wallet"] = str(row.get("source_wallet") or "").lower()
        row["counterfactual_price"] = round(price, 8)
        row["counterfactual_cost_usd"] = round(cost, 6)
        row["counterfactual_event_ts"] = round(event_ts, 6)
        key = _event_key(row)
        prior = by_key.get(key)
        if prior is None or _row_event_ts(row) >= _row_event_ts(prior):
            by_key[key] = row
    return sorted(by_key.values(), key=lambda row: (_row_event_ts(row), str(row.get("source_wallet") or "")))


def _synthetic_order(row: dict[str, Any]) -> dict[str, Any]:
    price = num(row.get("counterfactual_price")) or num(row.get("price"))
    cost = num(row.get("counterfactual_cost_usd")) or num(row.get("hypothetical_cost_usd"))
    shares = cost / price if price > 0 else 0.0
    event_key = _event_key(row)
    submitted_at = (
        _ts_to_iso(row.get("counterfactual_event_ts"))
        or _ts_to_iso(row.get("event_ts"))
        or _ts_to_iso(row.get("window_start_s"))
    )
    return {
        "order_id": event_key,
        "intent_id": event_key,
        "source_wallet": str(row.get("source_wallet") or "").lower(),
        "wallet_name": "floor_opportunity_cost_counterfactual",
        "condition_id": row.get("condition_id"),
        "market_slug": row.get("market_slug"),
        "outcome": row.get("outcome"),
        "token_id": row.get("token_id"),
        "final_status": "FILLED",
        "status": "FILLED",
        "filled_size_usd": round(cost, 6),
        "filled_shares": round(shares, 9),
        "limit_price": round(price, 8),
        "submitted_at": submitted_at,
        "source_intent": {
            "token_id": row.get("token_id"),
            "market_slug": row.get("market_slug"),
            "condition_id": row.get("condition_id"),
        },
    }


def _score_rows(
    rows: list[dict[str, Any]],
    *,
    resolutions: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        order = _synthetic_order(row)
        score = score_order(order, resolutions)
        out.append(
            {
                "event_key": _event_key(row),
                "flow_stage": "LIVE/MEASURE",
                "source_wallet": str(row.get("source_wallet") or "").lower(),
                "market_slug": row.get("market_slug"),
                "condition_id": row.get("condition_id"),
                "outcome": row.get("outcome"),
                "window_start_s": row.get("window_start_s"),
                "event_ts": row.get("counterfactual_event_ts") or row.get("event_ts"),
                "first_seen_at": row.get("first_seen_at"),
                "last_seen_at": row.get("last_seen_at"),
                "price": row.get("counterfactual_price") or row.get("price"),
                "hypothetical_cost_usd": row.get("counterfactual_cost_usd") or row.get("hypothetical_cost_usd"),
                "wallet_eligible_orders": int(row.get("wallet_eligible_orders") or 0),
                "dominant_skip_reason": row.get("dominant_skip_reason"),
                "participation_skip_category": row.get("participation_skip_category"),
                "floor_blocked_miss": bool(row.get("floor_blocked_miss")),
                "hypothetical_order": order,
                "score": score,
            }
        )
    return out


def _existing_event_keys(path: Path) -> set[str]:
    keys: set[str] = set()
    for row in _iter_jsonl(path):
        key = row.get("event_key")
        if key:
            keys.add(str(key))
    return keys


def _append_new_events(path: Path, rows: list[dict[str, Any]]) -> int:
    existing = _existing_event_keys(path)
    new_rows = [row for row in rows if str(row.get("event_key") or "") not in existing]
    if not new_rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in new_rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    return len(new_rows)


def _event_log_rows(path: Path) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for row in _iter_jsonl(path):
        key = row.get("event_key")
        if key:
            by_key[str(key)] = dict(row)
    return sorted(by_key.values(), key=lambda row: (num(row.get("event_ts")), str(row.get("event_key") or "")))


def _summary(scored_rows: list[dict[str, Any]], *, min_resolved: int) -> dict[str, Any]:
    resolved = [row for row in scored_rows if isinstance(row.get("score"), dict) and row["score"].get("resolved")]
    pnl = round(sum(num(row["score"].get("pnl_usd")) for row in resolved), 6)
    cost = round(sum(num(row["score"].get("cost_usd")) for row in resolved), 6)
    wallet_windows = {
        (str(row.get("source_wallet") or "").lower(), str(row.get("market_slug") or ""))
        for row in scored_rows
    }
    distinct_windows = {int(num(row.get("window_start_s"))) for row in scored_rows if num(row.get("window_start_s")) > 0}
    wallet_eligible_orders = sum(int(row.get("wallet_eligible_orders") or 0) for row in scored_rows)
    per_wallet: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "candidate_events": 0,
            "wallet_window_rows": 0,
            "wallet_eligible_orders": 0,
            "resolved_n": 0,
            "wins": 0,
            "losses": 0,
            "post_fee_would_cost_usd": 0.0,
            "post_fee_would_pnl_usd": 0.0,
        }
    )
    per_wallet_window_sets: dict[str, set[str]] = defaultdict(set)
    for row in scored_rows:
        wallet = str(row.get("source_wallet") or "").lower()
        item = per_wallet[wallet]
        item["candidate_events"] += 1
        item["wallet_eligible_orders"] += int(row.get("wallet_eligible_orders") or 0)
        if row.get("market_slug"):
            per_wallet_window_sets[wallet].add(str(row["market_slug"]))
        score = row.get("score") if isinstance(row.get("score"), dict) else {}
        if score.get("resolved"):
            item["resolved_n"] += 1
            item["wins"] += 1 if score.get("win") is True else 0
            item["losses"] += 1 if score.get("win") is False else 0
            item["post_fee_would_cost_usd"] += num(score.get("cost_usd"))
            item["post_fee_would_pnl_usd"] += num(score.get("pnl_usd"))
    per_wallet_out: dict[str, dict[str, Any]] = {}
    for wallet, item in sorted(per_wallet.items()):
        item["wallet_window_rows"] = len(per_wallet_window_sets.get(wallet, set()))
        item["post_fee_would_cost_usd"] = round(float(item["post_fee_would_cost_usd"]), 6)
        item["post_fee_would_pnl_usd"] = round(float(item["post_fee_would_pnl_usd"]), 6)
        item["post_fee_would_roi_pct"] = (
            round(float(item["post_fee_would_pnl_usd"]) / float(item["post_fee_would_cost_usd"]) * 100.0, 6)
            if item["post_fee_would_cost_usd"] > 0
            else 0.0
        )
        per_wallet_out[wallet] = item
    return {
        "candidate_events": len(scored_rows),
        "wallet_window_rows": len(wallet_windows),
        "distinct_windows": len(distinct_windows),
        "wallet_eligible_orders": wallet_eligible_orders,
        "resolved_n": len(resolved),
        "unresolved_n": len(scored_rows) - len(resolved),
        "wins": sum(1 for row in resolved if row["score"].get("win") is True),
        "losses": sum(1 for row in resolved if row["score"].get("win") is False),
        "post_fee_would_pnl_usd": pnl,
        "post_fee_would_cost_usd": cost,
        "post_fee_would_roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
        "hypothetical_pnl_usd": pnl,
        "hypothetical_cost_usd": cost,
        "hypothetical_roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
        "min_resolved_for_decision": int(min_resolved),
        "decision_ready": len(resolved) >= int(min_resolved),
        "positive_gate": len(resolved) >= int(min_resolved) and pnl > 0.0,
        "per_source_wallet": per_wallet_out,
    }


def _compact_rollup_snapshot(
    guard_events_path: Path,
    *,
    freeze_ts: float | None,
    quiet_start_ts: float | None,
) -> dict[str, Any]:
    if freeze_ts is None or not guard_events_path.exists():
        return {"available": False, "reason": "missing_freeze_ts_or_guard_events"}
    best: tuple[float, float, dict[str, Any]] | None = None
    for event in _iter_jsonl(guard_events_path):
        wp = event.get("window_participation") if isinstance(event.get("window_participation"), dict) else {}
        if not wp:
            live_execution = event.get("live_execution") if isinstance(event.get("live_execution"), dict) else {}
            wp = (
                live_execution.get("window_participation")
                if isinstance(live_execution.get("window_participation"), dict)
                else {}
            )
        if not wp:
            continue
        event_ts = _parse_iso_ts(event.get("generated_at")) or num(event.get("ts"))
        if event_ts <= 0:
            continue
        diff = abs(event_ts - freeze_ts)
        if best is None or diff < best[0]:
            best = (diff, event_ts, wp)
    if best is None:
        return {"available": False, "reason": "no_window_participation_events"}
    rows = best[2].get("recent_window_rollups") or best[2].get("window_rollups") or []
    rows = [row for row in rows if isinstance(row, dict)]
    filtered = []
    for row in rows:
        if row.get("floor_blocked_miss") is not True:
            continue
        if str(row.get("dominant_skip_reason") or "") != "drip_min_tranche_exceeds_window_budget":
            continue
        if quiet_start_ts is not None and num(row.get("window_start_s")) < quiet_start_ts:
            continue
        filtered.append(row)
    return {
        "available": True,
        "nearest_generated_at": _ts_to_iso(best[1]),
        "nearest_abs_delta_s": round(best[0], 6),
        "wallet_window_rows": len(filtered),
        "wallet_eligible_orders": sum(int(row.get("wallet_eligible_orders") or 0) for row in filtered),
        "distinct_windows": len({int(num(row.get("window_start_s"))) for row in filtered if num(row.get("window_start_s")) > 0}),
        "wallet_counts": _wallet_counts(filtered),
    }


def _wallet_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        wallet = str(row.get("source_wallet") or "").lower()
        if wallet:
            counts[wallet] = counts.get(wallet, 0) + 1
    return dict(sorted(counts.items()))


def _population_reconstruction(
    *,
    freeze_harvest: dict[str, Any],
    current_rows: list[dict[str, Any]],
    guard_events_path: Path,
    quiet_start_ts: float | None,
    freeze_ts: float | None,
) -> dict[str, Any]:
    summary = freeze_harvest.get("summary") if isinstance(freeze_harvest.get("summary"), dict) else {}
    expected = {
        "wallet_window_rows": summary.get("floor_wallet_window_rows"),
        "wallet_eligible_orders": summary.get("floor_wallet_eligible_orders_sum"),
        "distinct_windows": summary.get("floor_distinct_windows"),
        "wallet_counts": summary.get("floor_wallet_counts") if isinstance(summary.get("floor_wallet_counts"), dict) else {},
    }
    current_wallet_window_rows = len(
        {
            (str(row.get("source_wallet") or "").lower(), str(row.get("market_slug") or ""))
            for row in current_rows
        }
    )
    current = {
        "candidate_events": len(current_rows),
        "wallet_window_rows": current_wallet_window_rows,
        "wallet_eligible_orders": sum(int(row.get("wallet_eligible_orders") or 0) for row in current_rows),
        "distinct_windows": len({int(num(row.get("window_start_s"))) for row in current_rows if num(row.get("window_start_s")) > 0}),
        "wallet_counts": _wallet_counts(current_rows),
        "unit": "wallet_window_outcome_rows_scored; wallet_window_rows dedupes source_wallet+market_slug",
    }
    compact = _compact_rollup_snapshot(
        guard_events_path,
        freeze_ts=freeze_ts,
        quiet_start_ts=quiet_start_ts,
    )
    expected_rows = expected.get("wallet_window_rows")
    expected_orders = expected.get("wallet_eligible_orders")
    delta_rows = (
        current["wallet_window_rows"] - int(expected_rows)
        if expected_rows is not None
        else None
    )
    delta_orders = (
        current["wallet_eligible_orders"] - int(expected_orders)
        if expected_orders is not None
        else None
    )
    status = "MATCH"
    if delta_rows or delta_orders:
        status = "RECONSTRUCTED_FROM_ADVANCED_GUARD_ROWS"
    return {
        "status": status,
        "freeze_expected": expected,
        "current_detailed_rows": current,
        "nearest_compact_guard_event": compact,
        "delta_vs_freeze_expected": {
            "wallet_window_rows": delta_rows,
            "wallet_eligible_orders": delta_orders,
        },
        "note": (
            "Freeze sidecar retained aggregate/sample rows only; scoring uses detailed guard rows "
            "with outcome and price fields. Deltas are reported, not hidden."
        ),
    }


def build_report(
    *,
    guard_state_path: Path,
    freeze_harvest_path: Path,
    guard_events_path: Path,
    resolutions_path: Path,
    state_path: Path,
    event_log: Path,
    min_resolved: int,
) -> dict[str, Any]:
    guard_state = load_json(guard_state_path, default={})
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    freeze_harvest = load_json(freeze_harvest_path, default={})
    freeze_harvest = freeze_harvest if isinstance(freeze_harvest, dict) else {}
    freeze_summary = (
        freeze_harvest.get("summary") if isinstance(freeze_harvest.get("summary"), dict) else {}
    )
    quiet_start = freeze_summary.get("quiet_stretch_start")
    quiet_start_ts = _parse_iso_ts(quiet_start)
    freeze_ts = _parse_iso_ts(freeze_harvest.get("generated_at"))
    current_rows = _candidate_rows(
        guard_state,
        quiet_start_ts=quiet_start_ts,
        freeze_ts=freeze_ts,
    )
    resolutions = load_resolutions(resolutions_path)
    current_scored = _score_rows(current_rows, resolutions=resolutions)
    previous_state = load_json(state_path, default={})
    previous_state = previous_state if isinstance(previous_state, dict) else {}
    previous_summary = previous_state.get("summary") if isinstance(previous_state.get("summary"), dict) else {}
    appended = _append_new_events(event_log, current_scored)
    ledger_rows = _event_log_rows(event_log)
    ledger_scored = _score_rows(ledger_rows, resolutions=resolutions)
    summary = _summary(ledger_scored, min_resolved=min_resolved)
    generated_at = utc_now_iso()
    if not summary["decision_ready"]:
        status = "MEASURING"
        decision = "DRIP_FLOOR_DECISION_DEFERRED_UNTIL_N_GE_30"
    elif summary["positive_gate"]:
        status = "FLOOR_POSITIVE_GATE_REQUIRES_FABLE"
        decision = "REPORT_SAME_HEARTBEAT_FLOOR_CHANGE_REQUIRES_FABLE_DIRECTION"
    else:
        status = "DRIP_FLOOR_STANDS_VINDICATED"
        decision = "DRIP_FLOOR_STANDS_PROTECTIVE"
    report: dict[str, Any] = {
        "schema_version": 1,
        "kind": "floor_opportunity_cost_counterfactual",
        "flow_stage": "LIVE/MEASURE/DEFEND",
        "generated_at": generated_at,
        "status": status,
        "decision_rule_preregistered_at": "2026-07-15T19:26Z fable DIRECTION ORDER(4b)",
        "decision": decision,
        "guard_state": _display(guard_state_path),
        "freeze_harvest": _display(freeze_harvest_path),
        "guard_events": _display(guard_events_path),
        "resolutions": _display(resolutions_path),
        "state_path": _display(state_path),
        "event_log": _display(event_log),
        "universe_mode": "append_only_event_log_union",
        "source_snapshot_candidate_events": len(current_scored),
        "event_log_candidate_events": len(ledger_scored),
        "event_log_appended": appended,
        "previous_generated_at": previous_state.get("generated_at"),
        "prev_resolved_n": previous_summary.get("resolved_n"),
        "prev_pnl_usd": previous_summary.get("post_fee_would_pnl_usd"),
        "resolved_n_delta": (
            summary["resolved_n"] - int(previous_summary["resolved_n"])
            if previous_summary.get("resolved_n") is not None
            else None
        ),
        "pnl_usd_delta": (
            round(summary["post_fee_would_pnl_usd"] - num(previous_summary.get("post_fee_would_pnl_usd")), 6)
            if previous_summary.get("post_fee_would_pnl_usd") is not None
            else None
        ),
        "operator_rule": (
            "ORDER(4b): negative ROI or resolved_n<30 means the drip floor stands; "
            "positive_gate at n>=30 is reported same-heartbeat and any floor change requires Fable DIRECTION."
        ),
        "population_filter": {
            "source": "guard_state.window_participation.rows",
            "quiet_stretch_start": quiet_start,
            "freeze_generated_at_lte": freeze_harvest.get("generated_at"),
            "require_dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
            "require_floor_blocked_miss": True,
            "require_participation_skip_category": "FLOOR_BLOCKED_MISS",
            "exclude_selector_abstain_rows": True,
            "exclude_selection_visibility_packet": True,
            "unit": "wallet_window_outcome_row",
        },
        "population_reconstruction": _population_reconstruction(
            freeze_harvest=freeze_harvest,
            current_rows=current_scored,
            guard_events_path=guard_events_path,
            quiet_start_ts=quiet_start_ts,
            freeze_ts=freeze_ts,
        ),
        "summary": summary,
        "events": ledger_scored[-200:],
        "live_orders_allowed": False,
        "paper_only": True,
        "live_path_mutated": False,
    }
    atomic_write_json(state_path, report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD_STATE))
    parser.add_argument("--freeze-harvest", default=str(DEFAULT_FREEZE_HARVEST))
    parser.add_argument("--guard-events", default=str(DEFAULT_GUARD_EVENTS))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    parser.add_argument("--min-resolved", type=int, default=DEFAULT_MIN_RESOLVED)
    args = parser.parse_args(argv)
    report = build_report(
        guard_state_path=Path(args.guard_state),
        freeze_harvest_path=Path(args.freeze_harvest),
        guard_events_path=Path(args.guard_events),
        resolutions_path=Path(args.resolutions),
        state_path=Path(args.state),
        event_log=Path(args.event_log),
        min_resolved=int(args.min_resolved),
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
