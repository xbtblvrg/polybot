#!/usr/bin/env python3
"""Measure budget<effective-min floor binds under a bounded round-up clamp."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import datetime as dt
import json
import math
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = ROOT / "data/research/budget_bind_margin_packet_latest.json"
WINDOW_LOOKBACK_HOURS = 24.0
ROUND_UP_MIN_RATIO = 0.75


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
    ts = num(value, 0.0)
    if ts <= 0:
        return None
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = (len(sorted_values) - 1) * pct
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return sorted_values[int(index)]
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (index - lower)


def _numeric_summary(values: Iterable[float]) -> dict[str, Any]:
    cleaned = sorted(float(value) for value in values if value is not None)
    if not cleaned:
        return {"count": 0}
    return {
        "count": len(cleaned),
        "min": _round(cleaned[0]),
        "p50": _round(_percentile(cleaned, 0.50)),
        "p90": _round(_percentile(cleaned, 0.90)),
        "max": _round(cleaned[-1]),
        "avg": _round(sum(cleaned) / len(cleaned)),
    }


def _row_ts(row: dict[str, Any]) -> float:
    return (
        num(row.get("effective_latest_observed_ts"))
        or num(row.get("latest_observed_ts"))
        or num(row.get("source_detection_observed_ts"))
        or num(row.get("alternate_observed_ts"))
        or num(row.get("window_start_s"))
    )


def _price_for_row(row: dict[str, Any]) -> float:
    price = num(row.get("source_inventory_vwap"))
    if price > 0:
        return price
    target_usd = num(row.get("target_usd_at_vwap"))
    target_shares = num(row.get("target_shares"))
    if target_usd > 0 and target_shares > 0:
        return target_usd / target_shares
    return 0.0


def _effective_min_usd(row: dict[str, Any]) -> float:
    return max(
        num(row.get("process_min_live_order_usd")),
        num(row.get("min_order_usd")),
        num(row.get("effective_min_tranche_usd")),
        num(row.get("drip_min_tranche_usd")),
        num(row.get("would_floor_min_order_usd")),
    )


def _is_budget_bind_row(row: dict[str, Any]) -> bool:
    reason = str(row.get("dominant_skip_reason") or "").strip()
    category = str(row.get("participation_skip_category") or "").strip()
    return reason == "drip_min_tranche_exceeds_window_budget" or category == "FLOOR_BLOCKED_MISS"


def _participation_rows(guard_state: dict[str, Any]) -> list[dict[str, Any]]:
    participation = guard_state.get("window_participation")
    participation = participation if isinstance(participation, dict) else {}
    rows = participation.get("rows")
    if not isinstance(rows, list):
        rows = participation.get("window_rollups")
    return [row for row in rows or [] if isinstance(row, dict)]


def _event_key(row: dict[str, Any]) -> str:
    return stable_id(
        "budget_bind_margin",
        {
            "source_wallet": _norm_wallet(row.get("source_wallet")),
            "market_slug": row.get("market_slug"),
            "condition_id": row.get("condition_id"),
            "outcome": row.get("outcome"),
            "window_start_s": int(num(row.get("window_start_s"))),
            "first_seen_at": row.get("first_seen_at"),
            "latest_observed_ts": row.get("latest_observed_ts"),
        },
        length=24,
    )


def _candidate_rows(
    guard_state: dict[str, Any],
    *,
    lookback_start_ts: float,
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for raw in _participation_rows(guard_state):
        if not _is_budget_bind_row(raw):
            continue
        wallet = _norm_wallet(raw.get("source_wallet"))
        if not wallet:
            continue
        row_ts = _row_ts(raw)
        if row_ts < lookback_start_ts:
            continue
        market_slug = str(raw.get("market_slug") or "")
        outcome = str(raw.get("outcome") or "")
        if not market_slug.startswith("btc-updown-5m-") or outcome not in {"Up", "Down"}:
            continue
        price = _price_for_row(raw)
        window_budget = num(raw.get("window_budget_usd"))
        effective_min = _effective_min_usd(raw)
        if price <= 0 or window_budget <= 0 or effective_min <= 0:
            continue
        row = dict(raw)
        row["source_wallet"] = wallet
        row["budget_bind_event_ts"] = round(row_ts, 6)
        row["budget_bind_price"] = round(price, 8)
        row["budget_bind_window_budget_usd"] = round(window_budget, 6)
        row["budget_bind_effective_min_usd"] = round(effective_min, 6)
        row["budget_bind_budget_to_min_ratio"] = round(window_budget / effective_min, 6)
        row["round_up_eligible"] = window_budget >= ROUND_UP_MIN_RATIO * effective_min
        row["round_up_cost_usd"] = round(effective_min, 6) if row["round_up_eligible"] else 0.0
        key = _event_key(row)
        prior = by_key.get(key)
        if prior is None or _row_ts(row) >= _row_ts(prior):
            by_key[key] = row
    return sorted(by_key.values(), key=lambda row: (_row_ts(row), row.get("source_wallet") or ""))


def _synthetic_order(row: dict[str, Any]) -> dict[str, Any]:
    cost = num(row.get("round_up_cost_usd"))
    price = num(row.get("budget_bind_price"))
    shares = cost / price if cost > 0 and price > 0 else 0.0
    event_key = _event_key(row)
    return {
        "order_id": event_key,
        "intent_id": event_key,
        "source_wallet": row.get("source_wallet"),
        "wallet_name": "budget_bind_margin_packet",
        "condition_id": row.get("condition_id"),
        "market_slug": row.get("market_slug"),
        "outcome": row.get("outcome"),
        "token_id": row.get("token_id"),
        "final_status": "FILLED" if cost > 0 else "SKIPPED",
        "status": "FILLED" if cost > 0 else "SKIPPED",
        "filled_size_usd": round(cost, 6),
        "filled_shares": round(shares, 9),
        "limit_price": round(price, 8),
        "submitted_at": _ts_to_iso(row.get("budget_bind_event_ts")) or _ts_to_iso(row.get("window_start_s")),
        "source_intent": {
            "token_id": row.get("token_id"),
            "market_slug": row.get("market_slug"),
            "condition_id": row.get("condition_id"),
        },
    }


def _score_rows(rows: list[dict[str, Any]], resolutions: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for row in rows:
        order = _synthetic_order(row)
        score = score_order(order, resolutions)
        scored.append(
            {
                "event_key": _event_key(row),
                "flow_stage": "LIVE/MEASURE/DEFEND",
                "source_wallet": row.get("source_wallet"),
                "market_slug": row.get("market_slug"),
                "condition_id": row.get("condition_id"),
                "outcome": row.get("outcome"),
                "window_start_s": row.get("window_start_s"),
                "event_ts": row.get("budget_bind_event_ts"),
                "window_budget_usd": row.get("budget_bind_window_budget_usd"),
                "effective_min_usd": row.get("budget_bind_effective_min_usd"),
                "budget_to_min_ratio": row.get("budget_bind_budget_to_min_ratio"),
                "round_up_eligible": bool(row.get("round_up_eligible")),
                "round_up_cost_usd": row.get("round_up_cost_usd"),
                "price": row.get("budget_bind_price"),
                "wallet_eligible_orders": int(row.get("wallet_eligible_orders") or 0),
                "dominant_skip_reason": row.get("dominant_skip_reason"),
                "participation_skip_category": row.get("participation_skip_category"),
                "score": score,
            }
        )
    return scored


def _per_wallet(scored_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "bind_rows": 0,
            "round_up_eligible_rows": 0,
            "resolved_n": 0,
            "wins": 0,
            "losses": 0,
            "would_cost_usd": 0.0,
            "would_pnl_usd": 0.0,
            "wallet_eligible_orders": 0,
        }
    )
    for row in scored_rows:
        wallet = str(row.get("source_wallet") or "").lower()
        item = out[wallet]
        item["bind_rows"] += 1
        item["wallet_eligible_orders"] += int(row.get("wallet_eligible_orders") or 0)
        item["round_up_eligible_rows"] += 1 if row.get("round_up_eligible") else 0
        score = row.get("score") if isinstance(row.get("score"), dict) else {}
        if score.get("resolved"):
            item["resolved_n"] += 1
            item["wins"] += 1 if score.get("win") is True else 0
            item["losses"] += 1 if score.get("win") is False else 0
            item["would_cost_usd"] += num(score.get("cost_usd"))
            item["would_pnl_usd"] += num(score.get("pnl_usd"))
    normalized: dict[str, dict[str, Any]] = {}
    for wallet, item in sorted(out.items()):
        item["would_cost_usd"] = round(float(item["would_cost_usd"]), 6)
        item["would_pnl_usd"] = round(float(item["would_pnl_usd"]), 6)
        item["would_roi_pct"] = (
            round(float(item["would_pnl_usd"]) / float(item["would_cost_usd"]) * 100.0, 6)
            if item["would_cost_usd"] > 0
            else 0.0
        )
        normalized[wallet] = item
    return normalized


def _summary(scored_rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in scored_rows if isinstance(row.get("score"), dict) and row["score"].get("resolved")]
    eligible = [row for row in scored_rows if row.get("round_up_eligible")]
    resolved_eligible = [row for row in eligible if isinstance(row.get("score"), dict) and row["score"].get("resolved")]
    pnl = round(sum(num(row["score"].get("pnl_usd")) for row in resolved_eligible), 6)
    cost = round(sum(num(row["score"].get("cost_usd")) for row in resolved_eligible), 6)
    distinct_windows = {int(num(row.get("window_start_s"))) for row in scored_rows if num(row.get("window_start_s")) > 0}
    return {
        "bind_rows": len(scored_rows),
        "distinct_windows": len(distinct_windows),
        "wallet_eligible_orders": sum(int(row.get("wallet_eligible_orders") or 0) for row in scored_rows),
        "round_up_eligible_rows": len(eligible),
        "round_up_skipped_rows": len(scored_rows) - len(eligible),
        "resolved_rows": len(resolved),
        "round_up_resolved_rows": len(resolved_eligible),
        "round_up_unresolved_rows": len(eligible) - len(resolved_eligible),
        "round_up_wins": sum(1 for row in resolved_eligible if row["score"].get("win") is True),
        "round_up_losses": sum(1 for row in resolved_eligible if row["score"].get("win") is False),
        "round_up_would_cost_usd": cost,
        "round_up_would_pnl_usd": pnl,
        "round_up_would_roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
        "decision_bind_gate_met": len(scored_rows) >= 10,
        "decision_pnl_gate_met": pnl >= 0.0 and len(resolved_eligible) > 0,
        "per_source_wallet": _per_wallet(scored_rows),
    }


def build_report(
    *,
    guard_state_path: Path,
    resolutions_path: Path,
    lookback_hours: float,
) -> dict[str, Any]:
    guard_state = load_json(guard_state_path, default={})
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    generated_at = utc_now_iso()
    generated_ts = _parse_iso_ts(generated_at) or dt.datetime.now(dt.timezone.utc).timestamp()
    lookback_start_ts = generated_ts - (float(lookback_hours) * 3600.0)
    rows = _candidate_rows(guard_state, lookback_start_ts=lookback_start_ts)
    resolutions = load_resolutions(resolutions_path)
    scored_rows = _score_rows(rows, resolutions)
    summary = _summary(scored_rows)
    budget_values = [num(row.get("window_budget_usd")) for row in scored_rows]
    effective_min_values = [num(row.get("effective_min_usd")) for row in scored_rows]
    ratios = [num(row.get("budget_to_min_ratio")) for row in scored_rows]
    if summary["decision_bind_gate_met"] and summary["decision_pnl_gate_met"]:
        verdict = "ROUND_UP_CLAMP_GATE_PASS_REQUIRES_FABLE_TUNE"
        action = "ADOPT_BOUNDED_ROUND_UP_CLAMP_PER_2026_07_16T04_24Z_PRE_RULE"
    else:
        verdict = "KEEP_CURRENT_BEHAVIOR_DEMOTE_DRIP_MIN_ONE_LINER"
        action = "KEEP_CURRENT_BEHAVIOR"
    return {
        "schema_version": 1,
        "kind": "budget_bind_margin_packet",
        "flow_stage": "LIVE/MEASURE/DEFEND",
        "generated_at": generated_at,
        "verdict": verdict,
        "pre_ruled_action": action,
        "decision_rule": {
            "direction": "2026-07-16T04:24Z fable DIRECTION replacement order",
            "adopt_if": "bind_rows>=10 over 24h and round_up_would_pnl_usd>=0 on resolved eligible rows",
            "round_up_clamp": "submit size=effective_min_usd only when window_budget_usd>=0.75*effective_min_usd; skip otherwise",
            "live_path_mutated": False,
        },
        "guard_state": _display(guard_state_path),
        "resolutions": _display(resolutions_path),
        "lookback_hours": float(lookback_hours),
        "lookback_start_iso": _ts_to_iso(lookback_start_ts),
        "rows_scanned": len(_participation_rows(guard_state)),
        "summary": summary,
        "window_budget_usd": _numeric_summary(budget_values),
        "effective_min_usd": _numeric_summary(effective_min_values),
        "budget_to_min_ratio": _numeric_summary(ratios),
        "dominant_skip_reason_counts": dict(
            sorted(Counter(str(row.get("dominant_skip_reason") or "unknown") for row in scored_rows).items())
        ),
        "participation_skip_category_counts": dict(
            sorted(Counter(str(row.get("participation_skip_category") or "unknown") for row in scored_rows).items())
        ),
        "sample_rows": scored_rows[:10],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", type=Path, default=DEFAULT_GUARD_STATE)
    parser.add_argument("--resolutions", type=Path, default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--lookback-hours", type=float, default=WINDOW_LOOKBACK_HOURS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(
        guard_state_path=args.guard_state,
        resolutions_path=args.resolutions,
        lookback_hours=float(args.lookback_hours),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
