#!/usr/bin/env python3
"""Score E6 whale net-flow paper fills against BTC 5m resolutions."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.refresh_btc_5m_resolutions_from_gamma import (  # noqa: E402
    CANONICAL_GAMMA_SOURCE_ROUTE_ENV_VARS,
    fetch_gamma_resolution,
)
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_LANE_STATE = "data/research/e6_whale_net_flow_paper_lane_state.json"
DEFAULT_PAPER_STATE = "data/research/e6_whale_net_flow_paper_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane-state", default=DEFAULT_LANE_STATE)
    parser.add_argument("--paper-state", default=DEFAULT_PAPER_STATE)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output-state", default=None)
    parser.add_argument("--fetch-missing-gamma", action="store_true")
    parser.add_argument("--append-fetched-resolutions", action="store_true")
    parser.add_argument("--timeout-s", type=float, default=8.0)
    parser.add_argument("--user-agent", default="Mozilla/5.0")
    return parser.parse_args()


def _orders(paper_state: dict[str, Any]) -> list[dict[str, Any]]:
    return [row for row in paper_state.get("orders") or [] if isinstance(row, dict)]


def _signal_for_order(order: dict[str, Any]) -> dict[str, Any]:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
    signal = metadata.get("e6_whale_net_flow_v1")
    return signal if isinstance(signal, dict) else {}


def _flow_bucket(value: float) -> str:
    if value < 100.0:
        return "lt_100"
    if value < 1_000.0:
        return "100_999"
    if value < 5_000.0:
        return "1k_5k"
    if value < 10_000.0:
        return "5k_10k"
    if value < 25_000.0:
        return "10k_25k"
    return "25k_plus"


def _dominance_decile(value: float) -> str:
    bounded = min(max(value, 0.0), 0.999999)
    lower = int(bounded * 10) / 10.0
    upper = lower + 0.1
    return f"{lower:.1f}_{upper:.1f}"


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved")]
    wins = [row for row in resolved if row.get("win")]
    cost = sum(num(row.get("cost_usd")) for row in resolved)
    payout = sum(num(row.get("payout_usd")) for row in resolved)
    pnl = sum(num(row.get("pnl_usd")) for row in resolved)
    return {
        "paper_filled_orders": len(rows),
        "resolved_paper_fills": len(resolved),
        "unresolved_paper_fills": len(rows) - len(resolved),
        "resolved_paper_wins": len(wins),
        "resolved_paper_losses": len(resolved) - len(wins),
        "resolved_paper_cost_usd": round(cost, 6),
        "resolved_paper_payout_usd": round(payout, 6),
        "resolved_paper_pnl_usd": round(pnl, 6),
        "resolved_paper_roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
        "resolved_paper_wr_pct": round((len(wins) / len(resolved)) * 100.0, 6) if resolved else 0.0,
    }


def _summarize_groups(scored: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in scored:
        grouped[str(row.get(key) or "unknown")].append(row)
    return [
        {"bucket": name, **_summary(rows)}
        for name, rows in sorted(grouped.items(), key=lambda item: item[0])
    ]


def _append_resolution_rows(path: str | Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


@contextmanager
def _direct_gamma_source_route() -> Any:
    previous = {key: os.environ.get(key) for key in CANONICAL_GAMMA_SOURCE_ROUTE_ENV_VARS}
    for key in CANONICAL_GAMMA_SOURCE_ROUTE_ENV_VARS:
        os.environ.pop(key, None)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _missing_slugs(orders: list[dict[str, Any]], scores: list[dict[str, Any]]) -> list[str]:
    slugs: list[str] = []
    seen: set[str] = set()
    for order, score in zip(orders, scores):
        if score.get("resolved"):
            continue
        slug = str(order.get("market_slug") or "")
        if slug.startswith("btc-updown-5m-") and slug not in seen:
            slugs.append(slug)
            seen.add(slug)
    return slugs


def _score_orders(orders: list[dict[str, Any]], resolutions_path: str | Path) -> tuple[list[dict[str, Any]], int]:
    resolutions = load_resolutions(resolutions_path)
    scored: list[dict[str, Any]] = []
    for order in orders:
        signal = _signal_for_order(order)
        score = score_order(order, resolutions)
        dominant_flow_usd = num(signal.get("dominant_flow_usd"))
        dominance = num(signal.get("dominance"))
        score["e6_signal"] = {
            "signal_id": signal.get("signal_id"),
            "dominant_outcome": signal.get("dominant_outcome"),
            "dominant_flow_usd": round(dominant_flow_usd, 6),
            "dominance": round(dominance, 6),
            "observed_ts": signal.get("observed_ts"),
            "window_start_s": signal.get("window_start_s"),
        }
        score["dominant_flow_bucket"] = _flow_bucket(dominant_flow_usd)
        score["dominance_decile"] = _dominance_decile(dominance)
        scored.append(score)
    return scored, len(resolutions)


def build_scored_state(
    *,
    lane_state: dict[str, Any],
    paper_state: dict[str, Any],
    resolutions_path: str | Path,
    fetch_missing_gamma: bool = False,
    append_fetched_resolutions: bool = False,
    timeout_s: float = 8.0,
    user_agent: str = "Mozilla/5.0",
) -> dict[str, Any]:
    orders = _orders(paper_state)
    scored, resolution_rows_indexed = _score_orders(orders, resolutions_path)
    fetched_rows: list[dict[str, Any]] = []
    fetch_errors: list[dict[str, Any]] = []
    if fetch_missing_gamma:
        for slug in _missing_slugs(orders, scored):
            try:
                with _direct_gamma_source_route():
                    row = fetch_gamma_resolution(slug, timeout_s=timeout_s, user_agent=user_agent)
            except Exception as exc:  # noqa: BLE001 - persisted diagnostics are more useful than a dead scorer.
                fetch_errors.append({"market_slug": slug, "error": type(exc).__name__, "message": str(exc)[:300]})
                continue
            if row is not None:
                fetched_rows.append(row)
        if fetched_rows:
            if append_fetched_resolutions:
                _append_resolution_rows(resolutions_path, fetched_rows)
            else:
                temp_path = Path(resolutions_path).with_suffix(".e6_tmp.jsonl")
                existing = Path(resolutions_path)
                text = existing.read_text(encoding="utf-8") if existing.exists() else ""
                temp_path.write_text(
                    text + "".join(json.dumps(row, sort_keys=True) + "\n" for row in fetched_rows),
                    encoding="utf-8",
                )
                try:
                    scored, resolution_rows_indexed = _score_orders(orders, temp_path)
                finally:
                    temp_path.unlink(missing_ok=True)
            if append_fetched_resolutions:
                scored, resolution_rows_indexed = _score_orders(orders, resolutions_path)

    summary = _summary(scored)
    target_50 = 50
    target_150 = 150
    gate_status = "PASS" if summary["resolved_paper_fills"] >= target_50 and summary["resolved_paper_pnl_usd"] > 0 else "PENDING"
    scale_status = "PASS" if summary["resolved_paper_fills"] >= target_150 and summary["resolved_paper_pnl_usd"] > 0 else "PENDING"

    state = dict(lane_state)
    state["updated_at"] = utc_now_iso()
    state_summary = dict(state.get("summary") if isinstance(state.get("summary"), dict) else {})
    state_summary.update(
        {
            "paper_orders": len(orders),
            "filled_orders": summary["paper_filled_orders"],
            "resolved_paper_fills": summary["resolved_paper_fills"],
            "resolved_paper_pnl_usd": summary["resolved_paper_pnl_usd"],
            "live_orders_allowed": False,
            "paper_only": True,
        }
    )
    state["summary"] = state_summary
    previous_gate = state.get("promotion_gate") if isinstance(state.get("promotion_gate"), dict) else {}
    previous_gate = {
        key: value
        for key, value in previous_gate.items()
        if key
        not in {
            "resolution_scoring",
            "by_dominant_flow_bucket",
            "by_dominance_decile",
            "by_dominant_flow_and_dominance",
            "scored_orders",
        }
    }
    state["promotion_gate"] = {
        **previous_gate,
        **summary,
        "promotion_50_resolved_positive": gate_status,
        "scale_150_resolved_positive": scale_status,
        "resolved_paper_fills_required": target_50,
        "scale_after_resolved_paper_fills": target_150,
        "requires_positive_pnl": True,
        "note": "Resolution scoring is active; promotion remains pending until 50 resolved positive paper fills.",
    }
    state["resolution_scoring"] = {
        "kind": "e6_whale_net_flow_resolution_scoring_v1",
        "updated_at": utc_now_iso(),
        "resolution_path": str(resolutions_path),
        "resolution_rows_indexed": resolution_rows_indexed,
        "gamma_rows_fetched": len(fetched_rows),
        "gamma_fetch_errors": fetch_errors,
        "summary": summary,
        "by_dominant_flow_bucket": _summarize_groups(scored, "dominant_flow_bucket"),
        "by_dominance_decile": _summarize_groups(scored, "dominance_decile"),
        "scored_orders": scored[-500:],
    }
    return state


def main() -> int:
    args = parse_args()
    lane_state = load_json(args.lane_state, default={})
    paper_state = load_json(args.paper_state, default={})
    state = build_scored_state(
        lane_state=lane_state if isinstance(lane_state, dict) else {},
        paper_state=paper_state if isinstance(paper_state, dict) else {},
        resolutions_path=args.resolutions,
        fetch_missing_gamma=bool(args.fetch_missing_gamma),
        append_fetched_resolutions=bool(args.append_fetched_resolutions),
        timeout_s=float(args.timeout_s),
        user_agent=str(args.user_agent),
    )
    output_state = args.output_state or args.lane_state
    atomic_write_json(output_state, state)
    print(json.dumps(state.get("resolution_scoring", {}).get("summary", {}), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
