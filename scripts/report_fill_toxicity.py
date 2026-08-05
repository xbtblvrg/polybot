#!/usr/bin/env python3
"""Compare live filled-trade outcomes with all resolved source signals."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.performance import load_resolutions
from src.wallet_copy.pnl_truth import build_pnl_truth
from src.wallet_copy.scorecard import load_fresh_scorecard


DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_HISTORY = "data/research/wallet_copy_history_state.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_STATE_DIGEST = "data/research/state_digest.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_fill_toxicity_latest.json"
DEFAULT_DENYLIST_OUTPUT = "configs/wallet_copy/toxicity_denylist.json"
DEFAULT_RECONCILIATION_START = "2026-07-05T12:55:00Z"
DEFAULT_AD82_WALLET = "0xad825954d08beba32f74b594821f4251460c3df1"


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return _float(text, 0.0)


def _iso_from_ts(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), tz=UTC).isoformat().replace("+00:00", "Z")


def _day_bounds(day: str | None) -> tuple[str, float, float]:
    if day:
        start = datetime.fromisoformat(day).replace(tzinfo=UTC)
    else:
        now = datetime.now(tz=UTC)
        start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    end = start.replace(hour=0, minute=0, second=0, microsecond=0)
    if end != start:
        start = end
    return start.date().isoformat(), start.timestamp(), start.timestamp() + 86400.0


def _winner_from_resolution(row: dict[str, Any]) -> str:
    direction = str(row.get("direction") or "").upper()
    if direction == "UP":
        return "UP"
    if direction == "DOWN":
        return "DOWN"
    winner = str(row.get("winner") or row.get("resolved_outcome") or "").upper()
    if winner in {"YES", "UP"}:
        return "UP"
    if winner in {"NO", "DOWN"}:
        return "DOWN"
    return ""


def _load_resolutions(path: Path) -> dict[str, str]:
    winners: dict[str, str] = {}
    if not path.exists():
        return winners
    with path.open() as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            winner = _winner_from_resolution(row)
            if not winner:
                continue
            for key in (row.get("market_slug"), row.get("condition_id")):
                text = str(key or "")
                if text:
                    winners[text] = winner
    return winners


def _default_resolutions_path(root: Path) -> Path:
    candidates = [path for path in (root / "data" / "research").glob("btc_resolutions_*.jsonl") if path.is_file()]
    if not candidates:
        return root / "data" / "research" / "btc_resolutions_from_btcusdt_ticks.jsonl"
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _norm_outcome(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"UP", "YES"}:
        return "UP"
    if text in {"DOWN", "NO"}:
        return "DOWN"
    return ""


def _price_bucket(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price < 0.50:
        return "01_25_50"
    if price < 0.70:
        return "02_50_70"
    return "03_70_100"


def _parse_band_edges(value: str | None) -> list[float]:
    if not value:
        return []
    edges = sorted({_float(part.strip()) for part in str(value).split(",") if part.strip()})
    return [edge for edge in edges if 0.0 < edge < 1.0]


def _band_label(edges: list[float], idx: int) -> str:
    def pct(value: float) -> str:
        return f"{int(round(value * 100)):02d}"

    if not edges:
        return ""
    if idx == 0:
        return f"00_le_{pct(edges[0])}"
    if idx >= len(edges):
        return f"{idx:02d}_gt_{pct(edges[-1])}"
    return f"{idx:02d}_{pct(edges[idx - 1])}_{pct(edges[idx])}"


def _band_display(edges: list[float], idx: int) -> str:
    if not edges:
        return ""
    if idx == 0:
        return f"<={edges[0]:.2f}"
    if idx >= len(edges):
        return f">{edges[-1]:.2f}"
    return f"{edges[idx - 1]:.2f}-{edges[idx]:.2f}"


def _custom_price_bucket(price: float, edges: list[float]) -> str:
    if not edges:
        return _price_bucket(price)
    for idx, edge in enumerate(edges):
        if price <= edge:
            return _band_label(edges, idx)
    return _band_label(edges, len(edges))


def _band_spec(edges: list[float]) -> list[dict[str, Any]]:
    if not edges:
        return []
    return [
        {"label": _band_label(edges, idx), "display": _band_display(edges, idx)}
        for idx in range(len(edges) + 1)
    ]


def _new_bucket() -> dict[str, Any]:
    return {
        "count": 0,
        "wins": 0,
        "stake_usd": 0.0,
        "pnl_usd": 0.0,
        "price_sum": 0.0,
        "prices": [],
    }


def _add_unit_signal(bucket: dict[str, Any], *, price: float, win: bool, stake: float) -> None:
    if price <= 0.0 or price >= 1.0 or stake <= 0.0:
        return
    unit_pnl = (1.0 / price - 1.0) if win else -1.0
    bucket["count"] += 1
    bucket["wins"] += int(win)
    bucket["stake_usd"] += stake
    bucket["pnl_usd"] += unit_pnl * stake
    bucket["price_sum"] += price
    bucket["prices"].append(price)


def _add_live_fill(bucket: dict[str, Any], *, cost: float, pnl: float, price: float) -> None:
    if cost <= 0.0:
        return
    bucket["count"] += 1
    bucket["wins"] += int(pnl > 0.0)
    bucket["stake_usd"] += cost
    bucket["pnl_usd"] += pnl
    bucket["price_sum"] += price
    if price > 0:
        bucket["prices"].append(price)


def _merge_bucket(target: dict[str, Any], source: dict[str, Any]) -> None:
    count = int(source.get("count") or 0)
    target["count"] += count
    target["wins"] += int(source.get("wins") or 0)
    target["stake_usd"] += float(source.get("stake_usd") or 0.0)
    target["pnl_usd"] += float(source.get("pnl_usd") or 0.0)
    target["price_sum"] += float(source.get("price_sum") or 0.0)
    prices = source.get("prices") or []
    if prices:
        target["prices"].extend(prices)
    elif source.get("avg_price") is not None and count > 0:
        target["prices"].extend([float(source["avg_price"])] * count)


def _finalize(bucket: dict[str, Any]) -> dict[str, Any]:
    count = int(bucket.get("count") or 0)
    stake = float(bucket.get("stake_usd") or 0.0)
    pnl = float(bucket.get("pnl_usd") or 0.0)
    prices = bucket.get("prices") or []
    return {
        "count": count,
        "wins": int(bucket.get("wins") or 0),
        "win_rate_pct": round((float(bucket.get("wins") or 0) / count * 100.0), 6) if count else None,
        "stake_usd": round(stake, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round((pnl / stake * 100.0), 6) if stake else None,
        "avg_price": round(sum(prices) / len(prices), 6) if prices else None,
    }


def _policy_from_order(order: dict[str, Any]) -> str:
    meta = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    meta = meta.get("metadata") if isinstance(meta.get("metadata"), dict) else {}
    return str(order.get("policy_id") or meta.get("profit_policy_id") or meta.get("policy_id") or meta.get("copy_model") or "unknown")


def _active_wallets(scorecard: dict[str, Any], guard_state: dict[str, Any]) -> set[str]:
    guard_active = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    guard_members = guard_active.get("members") if isinstance(guard_active.get("members"), list) else []
    wallets = {
        str(row.get("source_wallet") or "").lower()
        for row in guard_members
        if isinstance(row, dict) and row.get("source_wallet")
    }
    if wallets:
        return wallets
    return {
        str(row.get("source_wallet") or "").lower()
        for row in ((scorecard.get("active_set_roster") or {}).get("members") or [])
        if isinstance(row, dict) and row.get("source_wallet")
    }


def _default_receipt_costs_path(root: Path) -> Path | None:
    candidates = [
        path
        for path in (root / "data" / "research").glob("wallet_copy_cash_flow_replay_tx_match_*.json")
        if path.is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _load_receipt_costs(root: Path) -> dict[str, float]:
    source_path = _default_receipt_costs_path(root)
    if source_path is None:
        return {}
    payload = _load_json(source_path, {})
    rows = payload.get("rows") if isinstance(payload, dict) and isinstance(payload.get("rows"), list) else []
    costs: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        amount = _float(row.get("pUSD_out"))
        if amount <= 0.0 or int(_float(row.get("orders"))) > 1:
            continue
        for key in (row.get("tx"), row.get("sample_order")):
            text = str(key or "").strip().lower()
            if text:
                costs[text] = amount
    return costs


def _load_actual_trade_costs(root: Path) -> dict[str, dict[str, Any]]:
    source_path = root / "data" / "research" / "wallet_copy_today_fill_cash_diff_latest.json"
    if not source_path.is_file():
        return {}
    payload = _load_json(source_path, {})
    rows = payload.get("rows") if isinstance(payload, dict) and isinstance(payload.get("rows"), list) else []
    costs: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or not str(row.get("join_status") or "").startswith("JOINED"):
            continue
        actual_cost = _float(row.get("actual_cost_usd"))
        order_ids = [str(value or "").strip().lower() for value in row.get("order_ids") or [] if str(value or "").strip()]
        if actual_cost <= 0.0 or len(order_ids) != 1:
            continue
        tx = str(row.get("tx") or "").strip().lower()
        record = {"actual_cost_usd": round(actual_cost, 6), "source": "actual_trade_record", "tx": tx}
        if tx:
            costs[tx] = record
        costs[order_ids[0]] = record
    return costs


def _signal_buckets(
    history: dict[str, Any],
    winners: dict[str, str],
    active_wallets: set[str],
    *,
    band_edges: list[float] | None = None,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    signal_groups: dict[tuple[str, str], dict[str, Any]] = defaultdict(_new_bucket)
    signal_overall = _new_bucket()
    signal_events = history.get("events") if isinstance(history.get("events"), list) else []
    edges = band_edges or []
    for event in signal_events:
        if not isinstance(event, dict) or str(event.get("action") or "").upper() != "BUY":
            continue
        wallet = str(event.get("source_wallet") or "").lower()
        if active_wallets and wallet not in active_wallets:
            continue
        winner = winners.get(str(event.get("market_slug") or "")) or winners.get(str(event.get("condition_id") or ""))
        outcome = _norm_outcome(event.get("outcome"))
        if not winner or not outcome:
            continue
        price = _float(event.get("price"))
        stake = _float(event.get("usdc_size"), 0.0) or max(price * _float(event.get("size")), 0.0)
        bucket_key = (wallet, _custom_price_bucket(price, edges))
        _add_unit_signal(signal_groups[bucket_key], price=price, win=outcome == winner, stake=stake)
        _add_unit_signal(signal_overall, price=price, win=outcome == winner, stake=stake)
    return signal_groups, signal_overall


def _fill_buckets_from_events(
    events: list[dict[str, Any]],
    *,
    band_edges: list[float] | None = None,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    fill_groups: dict[tuple[str, str], dict[str, Any]] = defaultdict(_new_bucket)
    fill_overall = _new_bucket()
    edges = band_edges or []
    for event in events:
        if not isinstance(event, dict) or str(event.get("status") or "").upper() != "FILLED":
            continue
        if not event.get("resolved"):
            continue
        price = _float(event.get("limit_price"))
        wallet = str(event.get("source_wallet") or "").lower()
        bucket_key = (wallet, _custom_price_bucket(price, edges))
        _add_live_fill(fill_groups[bucket_key], cost=_float(event.get("cost_usd")), pnl=_float(event.get("pnl_usd")), price=price)
        _add_live_fill(fill_overall, cost=_float(event.get("cost_usd")), pnl=_float(event.get("pnl_usd")), price=price)
    return fill_groups, fill_overall


def _truth_events_for_window(
    ledger: dict[str, Any],
    resolution_index: dict[str, dict[str, Any]],
    *,
    start_ts: float | None,
    end_ts: float | None,
    receipt_costs: dict[str, float],
    actual_trade_costs: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    truth = build_pnl_truth(
        ledger,
        resolution_index,
        start_ts=start_ts,
        end_ts=end_ts,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    return [row for row in truth.get("events") or [] if isinstance(row, dict)], truth


def _rows_from_groups(
    signal_groups: dict[tuple[str, str], dict[str, Any]],
    fill_groups: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in sorted(set(signal_groups) | set(fill_groups)):
        signal = _finalize(signal_groups.get(key, _new_bucket()))
        fills = _finalize(fill_groups.get(key, _new_bucket()))
        signal_roi = signal.get("roi_pct")
        fill_roi = fills.get("roi_pct")
        toxicity = None
        if signal_roi is not None and fill_roi is not None:
            toxicity = round(float(fill_roi) - float(signal_roi), 6)
        rows.append(
            {
                "source_wallet": key[0],
                "price_bucket": key[1],
                "all_signals": signal,
                "live_fills": fills,
                "toxicity_roi_pct": toxicity,
                "toxicity_cents_per_1usd": round(toxicity, 6) if toxicity is not None else None,
            }
        )
    return rows


def _aggregate_rows(rows: list[dict[str, Any]], key_name: str) -> list[dict[str, Any]]:
    signal_groups: dict[str, dict[str, Any]] = defaultdict(_new_bucket)
    fill_groups: dict[str, dict[str, Any]] = defaultdict(_new_bucket)
    for row in rows:
        key = str(row.get(key_name) or "")
        if not key:
            continue
        _merge_bucket(signal_groups[key], row.get("all_signals") or {})
        _merge_bucket(fill_groups[key], row.get("live_fills") or {})
    out = []
    for key in sorted(set(signal_groups) | set(fill_groups)):
        signal = _finalize(signal_groups.get(key, _new_bucket()))
        fills = _finalize(fill_groups.get(key, _new_bucket()))
        signal_roi = signal.get("roi_pct")
        fill_roi = fills.get("roi_pct")
        toxicity = None
        if signal_roi is not None and fill_roi is not None:
            toxicity = round(float(fill_roi) - float(signal_roi), 6)
        out.append(
            {
                key_name: key,
                "all_signals": signal,
                "live_fills": fills,
                "toxicity_roi_pct": toxicity,
                "toxicity_cents_per_1usd": round(toxicity, 6) if toxicity is not None else None,
            }
        )
    return out


def _window_report(
    *,
    name: str,
    start_ts: float | None,
    end_ts: float | None,
    truth_scope: dict[str, Any],
    signal_groups: dict[tuple[str, str], dict[str, Any]],
    signal_overall: dict[str, Any],
    fill_events: list[dict[str, Any]],
    band_edges: list[float],
    min_decision_fills: int,
) -> dict[str, Any]:
    fill_groups, fill_overall = _fill_buckets_from_events(fill_events, band_edges=band_edges)
    rows = _rows_from_groups(signal_groups, fill_groups)
    signal_summary = _finalize(signal_overall)
    fill_summary = _finalize(fill_overall)
    overall_toxicity = None
    if signal_summary.get("roi_pct") is not None and fill_summary.get("roi_pct") is not None:
        overall_toxicity = round(float(fill_summary["roi_pct"]) - float(signal_summary["roi_pct"]), 6)
    low_band = _band_label(band_edges, 0) if band_edges else ""
    low_band_row = next((row for row in _aggregate_rows(rows, "price_bucket") if row.get("price_bucket") == low_band), {})
    low_band_fills = (low_band_row.get("live_fills") or {}) if isinstance(low_band_row, dict) else {}
    low_band_roi = low_band_fills.get("roi_pct")
    low_band_count = int(low_band_fills.get("count") or 0)
    return {
        "name": name,
        "start_ts": start_ts,
        "start_iso": _iso_from_ts(start_ts),
        "end_ts": end_ts,
        "end_iso": _iso_from_ts(end_ts),
        "truth_scope": truth_scope,
        "all_signals": signal_summary,
        "live_fills": fill_summary,
        "toxicity_roi_pct": overall_toxicity,
        "toxicity_cents_per_1usd": round(overall_toxicity, 6) if overall_toxicity is not None else None,
        "price_bands": _aggregate_rows(rows, "price_bucket"),
        "source_wallets": _aggregate_rows(rows, "source_wallet"),
        "groups": rows,
        "low_band_decision_read": {
            "price_bucket": low_band,
            "min_fills": int(min_decision_fills),
            "live_fills": low_band_count,
            "roi_pct": low_band_roi,
            "floor_trigger_candidate": bool(
                low_band_count >= int(min_decision_fills)
                and low_band_roi is not None
                and float(low_band_roi) < 0.0
            ),
            "sample_size_honesty": "decision_ready" if low_band_count >= int(min_decision_fills) else "extend_measurement_window",
        },
    }


def _comparison_participation(
    *,
    args: argparse.Namespace,
    scorecard: dict[str, Any],
    state_digest: dict[str, Any],
    post_fix_start_ts: float,
) -> dict[str, Any]:
    volume_kpi = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    canonical = volume_kpi.get("canonical_daily") if isinstance(volume_kpi.get("canonical_daily"), dict) else {}
    rows = volume_kpi.get("rows") if isinstance(volume_kpi.get("rows"), list) else []
    post_rows = [row for row in rows if isinstance(row, dict) and _float(row.get("window_start_s")) >= post_fix_start_ts]
    digest_volume = state_digest.get("volume") if isinstance(state_digest.get("volume"), dict) else {}
    deadman = state_digest.get("order_flow_deadman") if isinstance(state_digest.get("order_flow_deadman"), dict) else {}
    return {
        "pre_fix_reference": {
            "windows_filled": int(getattr(args, "pre_fix_filled_windows", 17)),
            "filled_over_288": f"{int(getattr(args, 'pre_fix_filled_windows', 17))}/288",
            "missed_active_windows": 6,
            "source": "fable_direction_2026-07-08T05:34Z",
            "eligible_drought_s": None,
            "eligible_drought_evidence_gap": True,
            "evidence_note": "05:33/05:34 DIRECTION required eligible_drought_s before/after but did not persist the numeric pre-fix eligible_drought_s; current after value is sourced from state_digest.order_flow_deadman.",
        },
        "current_digest": {
            "windows_filled": digest_volume.get("windows_filled"),
            "windows_submitted": digest_volume.get("windows_submitted"),
            "denominator_windows": digest_volume.get("denominator_windows"),
            "missed_active_windows": digest_volume.get("missed_active_windows"),
            "consecutive_missed_active_windows": digest_volume.get("consecutive_missed_active_windows"),
            "eligible_drought_s": deadman.get("eligible_drought_s"),
            "eligible_drought_status": deadman.get("eligible_drought_status"),
        },
        "scorecard_canonical_daily": {
            "windows_filled": canonical.get("windows_filled"),
            "windows_submitted": canonical.get("windows_submitted"),
            "denominator_windows": canonical.get("denominator_windows"),
        },
        "post_fix_scorecard_rows": {
            "rows_observed": len(post_rows),
            "active_rows": sum(1 for row in post_rows if int(row.get("wallet_eligible_orders") or 0) > 0),
            "missed_active_windows": sum(1 for row in post_rows if row.get("missed_active_window")),
        },
    }


def _wallet_rows(report: dict[str, Any], wallet: str) -> dict[str, Any]:
    wallet = wallet.lower()
    groups = report.get("groups") if isinstance(report.get("groups"), list) else []
    rows = [row for row in groups if isinstance(row, dict) and str(row.get("source_wallet") or "").lower() == wallet]
    live_bucket = _new_bucket()
    signal_bucket = _new_bucket()
    for row in rows:
        _merge_bucket(live_bucket, row.get("live_fills") or {})
        _merge_bucket(signal_bucket, row.get("all_signals") or {})
    return {
        "source_wallet": wallet,
        "all_signals": _finalize(signal_bucket),
        "live_fills": _finalize(live_bucket),
        "groups": rows,
    }


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    ledger = _load_json(root / args.ledger, {})
    history = _load_json(root / args.history, {})
    scorecard = load_fresh_scorecard(root / args.scorecard)
    guard_state = _load_json(root / getattr(args, "guard_state", DEFAULT_GUARD_STATE), {})
    state_digest = _load_json(root / getattr(args, "state_digest", DEFAULT_STATE_DIGEST), {})
    resolutions_path = root / args.resolutions if args.resolutions else _default_resolutions_path(root)
    winners = _load_resolutions(resolutions_path)
    resolution_index = load_resolutions(resolutions_path)
    day, start_ts, end_ts = _day_bounds(args.day)

    active_wallets = _active_wallets(scorecard, guard_state)
    receipt_costs = _load_receipt_costs(root)
    actual_trade_costs = _load_actual_trade_costs(root)

    signal_groups, signal_overall = _signal_buckets(history, winners, active_wallets)
    fill_events, fill_truth = _truth_events_for_window(
        ledger,
        resolution_index,
        start_ts=start_ts,
        end_ts=end_ts,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    fill_groups, fill_overall = _fill_buckets_from_events(fill_events)
    rows = _rows_from_groups(signal_groups, fill_groups)

    signal_summary = _finalize(signal_overall)
    fill_summary = _finalize(fill_overall)
    overall_toxicity = None
    if signal_summary.get("roi_pct") is not None and fill_summary.get("roi_pct") is not None:
        overall_toxicity = round(float(fill_summary["roi_pct"]) - float(signal_summary["roi_pct"]), 6)
    toxic_rows = [
        row
        for row in rows
        if row.get("toxicity_roi_pct") is not None and row["live_fills"]["count"] >= int(args.min_fills)
    ]
    toxic_rows.sort(key=lambda row: float(row.get("toxicity_roi_pct") or 0.0))
    report = {
        "kind": "wallet_copy_fill_toxicity_report",
        "flow_stage": "LEARN/LIVE",
        "generated_at": _utc_now_iso(),
        "day_utc": day,
        "inputs": {
            "ledger": args.ledger,
            "history": args.history,
            "scorecard": args.scorecard,
            "guard_state": getattr(args, "guard_state", DEFAULT_GUARD_STATE),
            "resolutions": str(resolutions_path.relative_to(root)) if resolutions_path.is_relative_to(root) else str(resolutions_path),
            "receipt_costs_loaded": bool(receipt_costs),
            "actual_trade_costs_loaded": bool(actual_trade_costs),
        },
        "summary": {
            "active_wallets": len(active_wallets),
            "signal_scope": "active_set_history_resolved_buy_events",
            "fill_scope": "current_utc_day_resolved_live_fills_from_pnl_truth",
            "fill_truth_scope": fill_truth.get("scope"),
            "all_signals": signal_summary,
            "live_fills": fill_summary,
            "toxicity_roi_pct": overall_toxicity,
            "toxicity_cents_per_1usd": round(overall_toxicity, 6) if overall_toxicity is not None else None,
            "verdict": "TOXIC_FILLS" if overall_toxicity is not None and overall_toxicity < 0 else "NO_FILL_TOXICITY_DETECTED",
        },
        "worst_groups": toxic_rows[:10],
        "groups": rows,
    }
    post_fix_start = str(getattr(args, "post_fix_start", "") or "").strip()
    if post_fix_start:
        post_fix_start_ts = _parse_ts(post_fix_start)
        since_topup_start_ts = _parse_ts(getattr(args, "reconciliation_start", DEFAULT_RECONCILIATION_START))
        band_edges = _parse_band_edges(getattr(args, "comparison_bands", "0.20,0.40,0.60"))
        comparison_signal_groups, comparison_signal_overall = _signal_buckets(
            history,
            winners,
            active_wallets,
            band_edges=band_edges,
        )
        pre_events, pre_truth = _truth_events_for_window(
            ledger,
            resolution_index,
            start_ts=since_topup_start_ts,
            end_ts=post_fix_start_ts,
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        )
        post_events, post_truth = _truth_events_for_window(
            ledger,
            resolution_index,
            start_ts=post_fix_start_ts,
            end_ts=None,
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        )
        report["comparison"] = {
            "flow_stage": "LEARN/LIVE",
            "direction": "2026-07-08T05:34Z fable toxicity packet",
            "band_spec": _band_spec(band_edges),
            "decision_rule": {
                "price_floor_candidate": 0.20,
                "post_fix_low_band_min_fills": int(getattr(args, "post_fix_decision_min_fills", 8)),
                "floor_triggers_only_if_low_band_roi_negative_and_n_gte_min": True,
                "codex_authority": "report_only_no_threshold_or_denylist_edit",
            },
            "pre_fix_since_topup": _window_report(
                name="pre_fix_since_topup",
                start_ts=since_topup_start_ts,
                end_ts=post_fix_start_ts,
                truth_scope=pre_truth.get("scope") if isinstance(pre_truth.get("scope"), dict) else {},
                signal_groups=comparison_signal_groups,
                signal_overall=comparison_signal_overall,
                fill_events=pre_events,
                band_edges=band_edges,
                min_decision_fills=int(getattr(args, "post_fix_decision_min_fills", 8)),
            ),
            "post_fix": _window_report(
                name="post_fix",
                start_ts=post_fix_start_ts,
                end_ts=None,
                truth_scope=post_truth.get("scope") if isinstance(post_truth.get("scope"), dict) else {},
                signal_groups=comparison_signal_groups,
                signal_overall=comparison_signal_overall,
                fill_events=post_events,
                band_edges=band_edges,
                min_decision_fills=int(getattr(args, "post_fix_decision_min_fills", 8)),
            ),
        }
        ad82_wallet = str(getattr(args, "ad82_wallet", DEFAULT_AD82_WALLET) or DEFAULT_AD82_WALLET).lower()
        report["comparison"]["ad82_read"] = {
            "source_wallet": ad82_wallet,
            "pre_fix_since_topup": _wallet_rows(report["comparison"]["pre_fix_since_topup"], ad82_wallet),
            "post_fix": _wallet_rows(report["comparison"]["post_fix"], ad82_wallet),
        }
        report["comparison"]["windowed_participation"] = _comparison_participation(
            args=args,
            scorecard=scorecard,
            state_digest=state_digest,
            post_fix_start_ts=post_fix_start_ts,
        )
        post_low = report["comparison"]["post_fix"]["low_band_decision_read"]
        report["summary"]["post_fix_low_band_decision"] = post_low
        report["summary"]["post_fix_sample_size_honesty"] = post_low.get("sample_size_honesty")
        report["summary"]["post_fix_fill_count"] = report["comparison"]["post_fix"]["live_fills"]["count"]
        report["summary"]["pre_fix_since_topup_fill_count"] = report["comparison"]["pre_fix_since_topup"]["live_fills"]["count"]
        report["summary"]["ad82_post_fix_fill_count"] = report["comparison"]["ad82_read"]["post_fix"]["live_fills"]["count"]
        report["summary"]["windowed_participation"] = report["comparison"]["windowed_participation"]["current_digest"]
    return report


def build_denylist(report: dict[str, Any], *, min_signals: int = 100) -> dict[str, Any]:
    groups = report.get("groups") if isinstance(report.get("groups"), list) else []
    cells: list[dict[str, Any]] = []
    for row in groups:
        if not isinstance(row, dict):
            continue
        signals = row.get("all_signals") if isinstance(row.get("all_signals"), dict) else {}
        fills = row.get("live_fills") if isinstance(row.get("live_fills"), dict) else {}
        signal_count = int(signals.get("count") or 0)
        try:
            signal_roi = float(signals.get("roi_pct"))
        except (TypeError, ValueError):
            continue
        fill_count = int(fills.get("count") or 0)
        try:
            fill_roi = float(fills.get("roi_pct"))
        except (TypeError, ValueError):
            fill_roi = None
        # Tightened criteria (fable DIRECTION 2026-07-07T21:20Z, bucket
        # concentration): our-fill evidence outranks signal evidence, and
        # deeply negative signal cells are denied at lower sample counts.
        deny_rule = None
        if signal_count >= int(min_signals) and signal_roi <= 0.0:
            deny_rule = "signals_100_roi_le_0"
        elif signal_count >= 20 and signal_roi <= -50.0:
            deny_rule = "signals_20_roi_le_-50"
        # Live-positive override (fable RULING M8, 2026-07-08): our-fill
        # evidence outranks signal evidence in BOTH directions — a cell with
        # >=20 live fills at positive ROI is not denied on signal evidence.
        if deny_rule is not None and fill_count >= 20 and fill_roi is not None and fill_roi > 0.0:
            deny_rule = None
        if fill_count >= 5 and fill_roi is not None and fill_roi <= 0.0:
            deny_rule = "our_fills_5_roi_le_0"
        if deny_rule is None:
            continue
        cells.append(
            {
                "source_wallet": str(row.get("source_wallet") or "").lower(),
                "price_bucket": str(row.get("price_bucket") or ""),
                "reason": "toxicity_protection",
                "deny_rule": deny_rule,
                "all_signals": signals,
                "live_fills": fills,
                "toxicity_roi_pct": row.get("toxicity_roi_pct"),
            }
        )
    cells.sort(
        key=lambda row: (
            float((row.get("all_signals") or {}).get("roi_pct") or 0.0),
            str(row.get("source_wallet") or ""),
            str(row.get("price_bucket") or ""),
        )
    )
    return {
        "kind": "wallet_copy_toxicity_denylist",
        "schema_version": 1,
        "generated_at": _utc_now_iso(),
        "source_report": DEFAULT_OUTPUT,
        "criteria": {
            "all_signals_min_count": int(min_signals),
            "all_signals_roi_pct_lte": 0.0,
            "reject_reason": "toxicity_protection",
            "scope": "source_wallet_x_price_bucket",
            "tightened_20260707_fable": {
                "signals_min_20_roi_lte": -50.0,
                "our_fills_min_5_roi_lte": 0.0,
                "authority": "fable DIRECTION 2026-07-07T21:20Z (bucket concentration)",
            },
            "live_positive_override_20260708_fable": {
                "our_fills_min_20_roi_gt": 0.0,
                "effect": "signals-based deny rules do not apply to the cell",
                "authority": "fable RULING M8 2026-07-08 (deny-cell counterfactual)",
            },
        },
        "cells": cells,
        "cell_count": len(cells),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--state-digest", default=DEFAULT_STATE_DIGEST)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--day", default="")
    parser.add_argument("--min-fills", type=int, default=5)
    parser.add_argument(
        "--post-fix-start",
        default="",
        help="UTC timestamp for the post-fix measurement window; enables pre-fix-since-topup comparison.",
    )
    parser.add_argument(
        "--reconciliation-start",
        default=DEFAULT_RECONCILIATION_START,
        help="UTC timestamp for the since-topup baseline used by the pre-fix comparator.",
    )
    parser.add_argument(
        "--comparison-bands",
        default="0.20,0.40,0.60",
        help="Comma-separated upper edges for post-fix toxicity bands.",
    )
    parser.add_argument("--post-fix-decision-min-fills", type=int, default=8)
    parser.add_argument("--ad82-wallet", default=DEFAULT_AD82_WALLET)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--denylist-output", default=DEFAULT_DENYLIST_OUTPUT)
    parser.add_argument("--denylist-min-signals", type=int, default=100)
    parser.add_argument("--skip-denylist", action="store_true", help="Write only the toxicity report, not live denylist config.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    report = build_report(root, args)
    output = root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    denylist_path = ""
    if not bool(getattr(args, "skip_denylist", False)):
        denylist = build_denylist(report, min_signals=int(args.denylist_min_signals))
        denylist["source_report"] = args.output
        denylist_output = root / args.denylist_output
        denylist_output.parent.mkdir(parents=True, exist_ok=True)
        denylist_output.write_text(json.dumps(denylist, indent=2, sort_keys=True) + "\n")
        denylist_path = args.denylist_output
    print(
        json.dumps(
            {
                "output": args.output,
                "denylist_output": denylist_path,
                "skip_denylist": bool(getattr(args, "skip_denylist", False)),
                "summary": report["summary"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
