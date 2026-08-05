#!/usr/bin/env python3
"""Measure how much a689 paper/counterfactual edge survives live late gates."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"
DEFAULT_WATCH_TIER_HISTORY = "data/research/wallet_copy_watch_tier_history_state.json"
DEFAULT_WATCH_TIER_REPORT = "data/research/watch_tier_shadow_ev_latest.json"
DEFAULT_HOT_STANDBY = "data/research/a6896d11_hot_standby_paper_lane_latest.json"
DEFAULT_PRICE_REJECT_STATE = "data/research/a6896d11_price_reject_counterfactual_state.json"
DEFAULT_PRICE_REJECT_EVENTS = "data/research/a6896d11_price_reject_counterfactual_events.jsonl"
DEFAULT_LIVE_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/a689_edge_transfer_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", default=DEFAULT_WALLET)
    parser.add_argument("--watch-tier-history", default=DEFAULT_WATCH_TIER_HISTORY)
    parser.add_argument("--watch-tier-report", default=DEFAULT_WATCH_TIER_REPORT)
    parser.add_argument("--hot-standby", default=DEFAULT_HOT_STANDBY)
    parser.add_argument("--price-reject-state", default=DEFAULT_PRICE_REJECT_STATE)
    parser.add_argument("--price-reject-events", default=DEFAULT_PRICE_REJECT_EVENTS)
    parser.add_argument("--live-guard-state", default=DEFAULT_LIVE_GUARD_STATE)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _display(path: str | Path) -> str:
    p = Path(path)
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def _iter_jsonl(path: str | Path):
    p = Path(path)
    if not p.exists():
        return
    with p.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _window_start_s(slug: Any) -> float | None:
    match = re.search(r"btc-updown-5m-(\d+)", str(slug or ""))
    return float(match.group(1)) if match else None


def _event_ts(row: dict[str, Any]) -> float:
    source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
    return num(row.get("event_ts"), num(source_intent.get("event_ts"), 0.0))


def _observed_ts(row: dict[str, Any]) -> float:
    source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
    event_ts = _event_ts(row)
    return num(row.get("observed_ts"), num(source_intent.get("observed_ts"), event_ts))


def _event_price(row: dict[str, Any]) -> float:
    price = num(row.get("price"), 0.0)
    if price > 0:
        return price
    source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
    return num(source_intent.get("limit_price"), 0.0)


def _event_slug(row: dict[str, Any]) -> str:
    source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
    return str(row.get("market_slug") or row.get("event_slug") or source_intent.get("market_slug") or "")


def _entry_offset_s(row: dict[str, Any]) -> float | None:
    start = _window_start_s(_event_slug(row))
    event_ts = _event_ts(row)
    if start is None or event_ts <= 0:
        return None
    offset = event_ts - start
    if 0.0 <= offset < 300.0:
        return offset
    return event_ts % 300.0


def _gate_thresholds(live_guard_state: dict[str, Any]) -> dict[str, float]:
    guard_filter = live_guard_state.get("guard_runtime_filter")
    guard_filter = guard_filter if isinstance(guard_filter, dict) else {}
    live_execution = live_guard_state.get("live_execution")
    live_execution = live_execution if isinstance(live_execution, dict) else {}
    profit_latency = live_execution.get("profit_latency_suppression")
    profit_latency = profit_latency if isinstance(profit_latency, dict) else {}
    candidate_summary = live_execution.get("candidate_intent_summary")
    candidate_summary = candidate_summary if isinstance(candidate_summary, dict) else {}
    prefilter = candidate_summary.get("live_event_prefilter")
    prefilter = prefilter if isinstance(prefilter, dict) else {}
    return {
        "window_time_suppress_gte_s": num(
            guard_filter.get("profit_latency_window_time_suppress_gte_s"),
            num(profit_latency.get("window_time_suppress_gte_s"), 180.0),
        ),
        "inventory_late_window_stop_s": num(
            guard_filter.get("inventory_late_window_stop_s"),
            num(prefilter.get("late_window_stop_s"), 60.0),
        ),
    }


def _guard_gate(row: dict[str, Any], *, window_time_suppress_gte_s: float, inventory_late_window_stop_s: float) -> dict[str, Any]:
    start = _window_start_s(_event_slug(row))
    event_ts = _event_ts(row)
    observed_ts = _observed_ts(row)
    if start is None:
        return {"passes": False, "reason": "not_btc5m", "class": "NOT_BTC5M"}
    source_window_time_s = event_ts - start
    observed_window_time_s = observed_ts - start
    source_to_observed_lag_s = max(0.0, observed_ts - event_ts)
    close_ts = start + 300.0
    time_to_close_s = close_ts - observed_ts
    if window_time_suppress_gte_s > 0 and observed_window_time_s >= window_time_suppress_gte_s:
        late_class = "SOURCE_LATE" if source_window_time_s >= window_time_suppress_gte_s else "PIPELINE_LATE"
        return {
            "passes": False,
            "reason": "window_time_gte_threshold",
            "class": late_class,
            "source_window_time_s": round(source_window_time_s, 6),
            "observed_window_time_s": round(observed_window_time_s, 6),
            "source_to_observed_lag_s": round(source_to_observed_lag_s, 6),
            "window_time_suppress_gte_s": round(window_time_suppress_gte_s, 6),
            "time_to_close_s": round(time_to_close_s, 6),
        }
    if inventory_late_window_stop_s > 0 and time_to_close_s <= inventory_late_window_stop_s:
        late_class = "SOURCE_LATE" if close_ts - event_ts <= inventory_late_window_stop_s else "PIPELINE_LATE"
        return {
            "passes": False,
            "reason": "inventory_late_window_guard",
            "class": late_class,
            "source_window_time_s": round(source_window_time_s, 6),
            "observed_window_time_s": round(observed_window_time_s, 6),
            "source_to_observed_lag_s": round(source_to_observed_lag_s, 6),
            "inventory_late_window_stop_s": round(inventory_late_window_stop_s, 6),
            "time_to_close_s": round(time_to_close_s, 6),
        }
    return {
        "passes": True,
        "reason": "PASS",
        "class": "PASS",
        "source_window_time_s": round(source_window_time_s, 6),
        "observed_window_time_s": round(observed_window_time_s, 6),
        "source_to_observed_lag_s": round(source_to_observed_lag_s, 6),
        "time_to_close_s": round(time_to_close_s, 6),
    }


def _synthetic_order(row: dict[str, Any], *, wallet: str, order_usd: float, key_prefix: str) -> dict[str, Any]:
    price = max(0.000001, _event_price(row))
    event_key = str(row.get("event_key") or row.get("source_fingerprint") or row.get("event_id") or row.get("transaction_hash") or "")
    if not event_key:
        event_key = f"{key_prefix}_{_event_slug(row)}_{row.get('outcome')}_{_event_ts(row)}_{_observed_ts(row)}"
    return {
        "order_id": event_key,
        "intent_id": event_key,
        "source_wallet": wallet,
        "wallet_name": key_prefix,
        "condition_id": row.get("condition_id"),
        "market_slug": _event_slug(row),
        "outcome": row.get("outcome"),
        "token_id": row.get("token_id"),
        "final_status": "FILLED",
        "status": "FILLED",
        "filled_size_usd": float(order_usd),
        "filled_shares": float(order_usd) / price,
        "limit_price": price,
        "submitted_at": _event_ts(row),
        "source_intent": {
            "condition_id": row.get("condition_id"),
            "market_slug": _event_slug(row),
            "token_id": row.get("token_id"),
        },
    }


def _summary(scored: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]]) -> dict[str, Any]:
    resolved = [(row, gate, score) for row, gate, score in scored if score.get("resolved")]
    cost = sum(num(score.get("cost_usd")) for _, _, score in resolved)
    pnl = sum(num(score.get("pnl_usd")) for _, _, score in resolved)
    return {
        "events": len(scored),
        "resolved_n": len(resolved),
        "unresolved_n": len(scored) - len(resolved),
        "wins": sum(1 for _, _, score in resolved if score.get("win") is True),
        "losses": sum(1 for _, _, score in resolved if score.get("win") is False),
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
    }


def _score_with_gate(
    rows: list[dict[str, Any]],
    *,
    wallet: str,
    resolutions: dict[str, dict[str, Any]],
    order_usd: float,
    key_prefix: str,
    thresholds: dict[str, float],
) -> dict[str, Any]:
    scored: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    reason_counts: Counter[str] = Counter()
    class_counts: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    for row in rows:
        gate = _guard_gate(
            row,
            window_time_suppress_gte_s=thresholds["window_time_suppress_gte_s"],
            inventory_late_window_stop_s=thresholds["inventory_late_window_stop_s"],
        )
        score = score_order(_synthetic_order(row, wallet=wallet, order_usd=order_usd, key_prefix=key_prefix), resolutions)
        scored.append((row, gate, score))
        reason_counts[str(gate["reason"])] += 1
        class_counts[str(gate["class"])] += 1
        if not gate.get("passes") and len(samples) < 8:
            samples.append(
                {
                    "reason": gate.get("reason"),
                    "class": gate.get("class"),
                    "market_slug": _event_slug(row),
                    "outcome": row.get("outcome"),
                    "price": round(_event_price(row), 8),
                    "event_ts": _event_ts(row),
                    "observed_ts": _observed_ts(row),
                    "source_window_time_s": gate.get("source_window_time_s"),
                    "observed_window_time_s": gate.get("observed_window_time_s"),
                    "source_to_observed_lag_s": gate.get("source_to_observed_lag_s"),
                    "resolved": bool(score.get("resolved")),
                    "pnl_usd": score.get("pnl_usd"),
                }
            )
    guard_scored = [(row, gate, score) for row, gate, score in scored if gate.get("passes")]
    return {
        "full": _summary(scored),
        "guard_eligible": _summary(guard_scored),
        "guard_reject_counts": dict(sorted(reason_counts.items())),
        "guard_late_class_counts": dict(sorted(class_counts.items())),
        "sample_rejected_rows": samples,
    }


def _watch_rows(history_state: dict[str, Any], *, wallet: str, criteria: dict[str, Any]) -> list[dict[str, Any]]:
    rows = history_state.get("events") if isinstance(history_state.get("events"), list) else []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _wallet(row.get("source_wallet")) != wallet:
            continue
        if str(row.get("action") or "").upper() != "BUY":
            continue
        if _window_start_s(_event_slug(row)) is None:
            continue
        offset = _entry_offset_s(row)
        if offset is None or offset >= num(criteria.get("max_entry_offset_s"), 60.0):
            continue
        price = _event_price(row)
        if price < num(criteria.get("min_price"), 0.25) or price > num(criteria.get("max_price"), 0.5):
            continue
        out.append(row)
    return out


def _price_reject_rows(path: str | Path) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for row in _iter_jsonl(path) or []:
        key = str(row.get("event_key") or "")
        if key:
            by_key[key] = row
    return sorted(by_key.values(), key=lambda row: (_event_ts(row), str(row.get("event_key") or "")))


def _find_wallet_row(rows: Any, wallet: str) -> dict[str, Any]:
    if not isinstance(rows, list):
        return {}
    for row in rows:
        if isinstance(row, dict) and _wallet(row.get("source_wallet") or row.get("wallet")) == wallet:
            return row
    return {}


def build_report(
    *,
    wallet: str = DEFAULT_WALLET,
    watch_tier_history_path: str | Path = DEFAULT_WATCH_TIER_HISTORY,
    watch_tier_report_path: str | Path = DEFAULT_WATCH_TIER_REPORT,
    hot_standby_path: str | Path = DEFAULT_HOT_STANDBY,
    price_reject_state_path: str | Path = DEFAULT_PRICE_REJECT_STATE,
    price_reject_events_path: str | Path = DEFAULT_PRICE_REJECT_EVENTS,
    live_guard_state_path: str | Path = DEFAULT_LIVE_GUARD_STATE,
    resolutions_path: str | Path = DEFAULT_RESOLUTIONS,
) -> dict[str, Any]:
    wallet = _wallet(wallet)
    live_guard_state = load_json(live_guard_state_path, default={}) or {}
    thresholds = _gate_thresholds(live_guard_state if isinstance(live_guard_state, dict) else {})
    resolutions = load_resolutions(resolutions_path)
    watch_report = load_json(watch_tier_report_path, default={}) or {}
    watch_criteria = watch_report.get("criteria") if isinstance(watch_report.get("criteria"), dict) else {}
    watch_rows = _watch_rows(load_json(watch_tier_history_path, default={}) or {}, wallet=wallet, criteria=watch_criteria)
    watch_order_usd = num(watch_criteria.get("order_usd"), 1.0)
    watch_scored = _score_with_gate(
        watch_rows,
        wallet=wallet,
        resolutions=resolutions,
        order_usd=watch_order_usd,
        key_prefix="a689_watch_tier_guard_subset",
        thresholds=thresholds,
    )

    price_state = load_json(price_reject_state_path, default={}) or {}
    price_filters = price_state.get("filters") if isinstance(price_state.get("filters"), dict) else {}
    price_rows = _price_reject_rows(price_reject_events_path)
    canary_size = num(price_filters.get("canary_size_usd"), 2.0)
    price_scored = _score_with_gate(
        price_rows,
        wallet=wallet,
        resolutions=resolutions,
        order_usd=canary_size,
        key_prefix="a689_price_reject_guard_subset",
        thresholds=thresholds,
    )

    watch_wallet_row = _find_wallet_row(watch_report.get("wallets"), wallet)
    hot_state = load_json(hot_standby_path, default={}) or {}
    hot_summary = hot_state.get("summary") if isinstance(hot_state.get("summary"), dict) else {}
    price_summary = price_state.get("summary") if isinstance(price_state.get("summary"), dict) else {}
    price_full_matches_state = bool(
        int(price_summary.get("resolved_n") or -1) == int(price_scored["full"]["resolved_n"])
        and abs(num(price_summary.get("hypothetical_pnl_usd")) - num(price_scored["full"]["pnl_usd"])) < 0.00001
    )
    return {
        "schema_version": 1,
        "kind": "a689_edge_transfer_report",
        "flow_stage": "MEASURE/LIVE/DEFEND",
        "generated_at": utc_now_iso(),
        "wallet": wallet,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "source": "Fable 2026-07-15T22:53Z ORDER(6) a689 edge-transfer check",
        "guard_thresholds": {
            **thresholds,
            "taxonomy_label": "window_time_gte_180s",
            "threshold_note": "uses current live guard numeric threshold; taxonomy label may be historical",
            "live_guard_state": _display(live_guard_state_path),
        },
        "same_gate_verdict": {
            "watch_tier_paper_lane": {
                "same_as_live_late_gates": False,
                "reason": "watch-tier report applies source entry_offset and price band, but not observed-time live latency or inventory late-window gates",
                "source_artifact": _display(watch_tier_report_path),
            },
            "price_reject_counterfactual": {
                "same_as_live_late_gates": False,
                "reason": "price-reject counterfactual applies price/since filters and synthetic fills, but not observed-time live latency or inventory late-window gates",
                "source_artifact": _display(price_reject_state_path),
            },
        },
        "watch_tier_paper_lane": {
            "published_full": {
                "generated_at": watch_report.get("generated_at"),
                "eligible_signals": watch_wallet_row.get("eligible_signals"),
                "resolved_signals": watch_wallet_row.get("resolved_signals"),
                "pnl_usd": watch_wallet_row.get("pnl_usd"),
                "roi_pct": watch_wallet_row.get("roi_pct"),
                "hot_standby_resolved": hot_summary.get("resolved_paper_fills"),
                "hot_standby_post_fee_pnl_usd": hot_summary.get("in_lane_post_fee_pnl_usd"),
                "hot_standby_status": hot_state.get("status"),
            },
            "current_history_guard_replay": watch_scored,
            "coverage_note": (
                "current watch-tier history is a rolling row artifact; published hot-standby aggregate "
                "is cited separately, while this replay measures the currently reconstructable row subset"
            ),
        },
        "price_reject_counterfactual": {
            "published_full": {
                "generated_at": price_state.get("generated_at"),
                "candidate_events": price_summary.get("candidate_events"),
                "resolved_n": price_summary.get("resolved_n"),
                "hypothetical_pnl_usd": price_summary.get("hypothetical_pnl_usd"),
                "hypothetical_roi_pct": price_summary.get("hypothetical_roi_pct"),
                "state_matches_recomputed_event_log": price_full_matches_state,
            },
            "guard_replay": price_scored,
        },
        "verdict": {
            "price_reject_guard_eligible_positive": price_scored["guard_eligible"]["pnl_usd"] > 0,
            "watch_tier_current_history_guard_eligible_positive": watch_scored["guard_eligible"]["pnl_usd"] > 0,
            "summary": (
                "price-reject canary edge remains positive on the live late-gate subset; "
                "watch-tier hot-standby aggregate is not same-gated and current reconstructable watch rows are fully late-filtered"
            ),
        },
        "inputs": {
            "watch_tier_history": _display(watch_tier_history_path),
            "watch_tier_report": _display(watch_tier_report_path),
            "hot_standby": _display(hot_standby_path),
            "price_reject_state": _display(price_reject_state_path),
            "price_reject_events": _display(price_reject_events_path),
            "resolutions": _display(resolutions_path),
        },
    }


def main() -> int:
    args = parse_args()
    report = build_report(
        wallet=args.wallet,
        watch_tier_history_path=args.watch_tier_history,
        watch_tier_report_path=args.watch_tier_report,
        hot_standby_path=args.hot_standby,
        price_reject_state_path=args.price_reject_state,
        price_reject_events_path=args.price_reject_events,
        live_guard_state_path=args.live_guard_state,
        resolutions_path=args.resolutions,
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["verdict"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
