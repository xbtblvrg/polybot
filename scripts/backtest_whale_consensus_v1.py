#!/usr/bin/env python3
"""Backtest the paper-only whale consensus signal from existing fill data.

Flow stage: LEARN. This script is offline evidence only; it does not create
CopyIntents and does not touch paper or live execution state.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_POLYGON_JSONL = "data/research/same_window_capture/20260719T162300Z/polygon_orderfilled.jsonl"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/whale_consensus_v1_backtest_latest.json"


@dataclass(frozen=True)
class TokenMeta:
    market_slug: str
    condition_id: str
    outcome: str
    winner: str
    window_start_s: float


@dataclass(frozen=True)
class FillEvent:
    wallet: str
    market_slug: str
    outcome: str
    side: str
    price: float
    size: float
    event_ts: float
    winner: str

    @property
    def window_start_s(self) -> float:
        try:
            return float(self.market_slug.rsplit("-", 1)[-1])
        except ValueError:
            return 0.0


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _norm_addr(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) >= 42:
        return text[:42]
    return ""


def load_token_map(path: Path) -> dict[str, TokenMeta]:
    token_map: dict[str, TokenMeta] = {}
    with path.open(errors="ignore") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            if str(row.get("asset") or "").upper() != "BTC":
                continue
            if str(row.get("window_type") or "").lower() != "5m":
                continue
            winner = str(row.get("direction") or "").upper()
            if winner not in {"UP", "DOWN"}:
                continue
            market_slug = str(row.get("market_slug") or "")
            if not market_slug.startswith("btc-updown-5m-"):
                continue
            window_start_s = _num(row.get("window_start_unix_ts") or market_slug.rsplit("-", 1)[-1])
            condition_id = str(row.get("condition_id") or "")
            for key, outcome in (("yes_token", "Up"), ("no_token", "Down")):
                token = str(row.get(key) or "")
                if token:
                    token_map[token] = TokenMeta(
                        market_slug=market_slug,
                        condition_id=condition_id,
                        outcome=outcome,
                        winner="Up" if winner == "UP" else "Down",
                        window_start_s=window_start_s,
                    )
    return token_map


def iter_fill_events(path: Path, token_map: dict[str, TokenMeta], *, max_rows: int = 0) -> tuple[list[FillEvent], dict[str, int]]:
    events: list[FillEvent] = []
    diagnostics: dict[str, int] = defaultdict(int)
    with path.open(errors="ignore") as fh:
        for line in fh:
            if max_rows and diagnostics["rows_seen"] >= max_rows:
                break
            diagnostics["rows_seen"] += 1
            if "polygon_orderfilled_log" not in line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                diagnostics["bad_json"] += 1
                continue
            if not row.get("is_registry_wallet"):
                diagnostics["non_registry"] += 1
                continue
            decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
            asset = str(decoded.get("asset") or "")
            meta = token_map.get(asset)
            if not meta:
                diagnostics["asset_not_btc5m_resolved"] += 1
                continue
            wallet = _norm_addr(row.get("selected_wallet"))
            if not wallet:
                diagnostics["missing_wallet"] += 1
                continue
            side = str(decoded.get("side") or "").upper()
            price = _num(decoded.get("price"))
            size = _num(decoded.get("size"))
            event_ts = _num(row.get("block_ts") or row.get("received_at_s"))
            if side not in {"BUY", "SELL"} or price <= 0.0 or size <= 0.0 or event_ts <= 0.0:
                diagnostics["incomplete_fill"] += 1
                continue
            events.append(
                FillEvent(
                    wallet=wallet,
                    market_slug=meta.market_slug,
                    outcome=meta.outcome,
                    side=side,
                    price=price,
                    size=size,
                    event_ts=event_ts,
                    winner=meta.winner,
                )
            )
            diagnostics["accepted"] += 1
    events.sort(key=lambda item: (item.window_start_s, item.event_ts))
    return events, dict(diagnostics)


def _wallet_profiles_before(events: list[FillEvent], window_start_s: float) -> dict[str, dict[str, float]]:
    profiles: dict[str, dict[str, float]] = defaultdict(lambda: {"pnl": 0.0, "cost": 0.0, "fills": 0.0})
    for event in events:
        if event.window_start_s >= window_start_s:
            break
        if event.side != "BUY" or event.price <= 0.0:
            continue
        cost = event.price * event.size
        payout = event.size if event.outcome == event.winner else 0.0
        profile = profiles[event.wallet]
        profile["pnl"] += payout - cost
        profile["cost"] += cost
        profile["fills"] += 1.0
    return profiles


def _profile_weight(profile: dict[str, float]) -> float:
    fills = profile.get("fills", 0.0)
    cost = profile.get("cost", 0.0)
    pnl = profile.get("pnl", 0.0)
    if fills < 1.0 or cost <= 0.0 or pnl <= 0.0:
        return 0.0
    roi = pnl / cost
    return max(0.0, roi) * math.sqrt(fills)


def profile_weight_rows_before(events: list[FillEvent], window_start_s: float, *, limit: int = 0) -> list[dict[str, Any]]:
    """Return profile-positive wallet weights using only earlier windows."""

    rows: list[dict[str, Any]] = []
    profiles = _wallet_profiles_before(events, window_start_s)
    for wallet, profile in profiles.items():
        weight = _profile_weight(profile)
        if weight <= 0.0:
            continue
        cost = profile.get("cost", 0.0)
        pnl = profile.get("pnl", 0.0)
        rows.append(
            {
                "wallet": wallet,
                "weight": round(weight, 9),
                "pnl_usd": round(pnl, 6),
                "cost_usd": round(cost, 6),
                "fills": int(profile.get("fills", 0.0)),
                "roi_pct": round(pnl / cost * 100.0, 6) if cost > 0 else 0.0,
            }
        )
    rows.sort(key=lambda item: (item["weight"], item["pnl_usd"], item["fills"], item["wallet"]), reverse=True)
    return rows[: int(limit)] if limit and limit > 0 else rows


def _bucket(event_ts: float, window_start_s: float) -> str:
    offset = max(0.0, event_ts - window_start_s)
    bucket_start = int(offset // 60) * 60
    bucket_end = bucket_start + 60
    return f"{bucket_start:03d}-{bucket_end:03d}s"


def run_backtest(
    events: list[FillEvent],
    *,
    thresholds: list[float],
    pmax_values: list[float],
    top_k: int,
    order_usd: float,
    max_entry_offset_s: float,
) -> dict[str, Any]:
    by_window: dict[str, list[FillEvent]] = defaultdict(list)
    for event in events:
        by_window[event.market_slug].append(event)

    grid: dict[tuple[float, float], dict[str, Any]] = {}
    for threshold in thresholds:
        for pmax in pmax_values:
            grid[(threshold, pmax)] = {
                "threshold": threshold,
                "pmax": pmax,
                "trades": 0,
                "wins": 0,
                "cost_usd": 0.0,
                "pnl_usd": 0.0,
                "buckets": defaultdict(lambda: {"trades": 0, "wins": 0, "cost_usd": 0.0, "pnl_usd": 0.0}),
                "samples": [],
            }

    skipped_no_profile = 0
    for market_slug, rows in sorted(by_window.items(), key=lambda item: item[1][0].window_start_s):
        window_start = rows[0].window_start_s
        selected = {
            str(row["wallet"]): float(row["weight"])
            for row in profile_weight_rows_before(events, window_start, limit=top_k)
        }
        if not selected:
            skipped_no_profile += 1
            continue
        for threshold, pmax in grid:
            accum = {"Up": 0.0, "Down": 0.0}
            fired = False
            for row in rows:
                if row.wallet not in selected:
                    continue
                if row.event_ts - window_start > max_entry_offset_s:
                    continue
                weight = selected[row.wallet]
                direction = 1.0 if row.side == "BUY" else -1.0
                accum[row.outcome] += direction * row.size * weight
                signal_outcome = "Up" if accum["Up"] >= accum["Down"] else "Down"
                signal_strength = accum[signal_outcome]
                if signal_strength < threshold or row.outcome != signal_outcome or row.price > pmax:
                    continue
                shares = order_usd / row.price
                pnl = (shares if signal_outcome == row.winner else 0.0) - order_usd
                record = grid[(threshold, pmax)]
                record["trades"] += 1
                record["wins"] += 1 if pnl > 0 else 0
                record["cost_usd"] += order_usd
                record["pnl_usd"] += pnl
                bucket = _bucket(row.event_ts, window_start)
                bucket_row = record["buckets"][bucket]
                bucket_row["trades"] += 1
                bucket_row["wins"] += 1 if pnl > 0 else 0
                bucket_row["cost_usd"] += order_usd
                bucket_row["pnl_usd"] += pnl
                if len(record["samples"]) < 20:
                    record["samples"].append(
                        {
                            "market_slug": market_slug,
                            "outcome": signal_outcome,
                            "winner": row.winner,
                            "price": round(row.price, 6),
                            "pnl_usd": round(pnl, 6),
                            "bucket": bucket,
                            "signal_strength": round(signal_strength, 6),
                            "profile_wallets": len(selected),
                        }
                    )
                fired = True
                break
            if fired:
                continue

    rows_out = []
    for record in grid.values():
        trades = record["trades"]
        cost = record["cost_usd"]
        pnl = record["pnl_usd"]
        rows_out.append(
            {
                "threshold": record["threshold"],
                "pmax": record["pmax"],
                "trades": trades,
                "wins": record["wins"],
                "wr_pct": round(record["wins"] / trades * 100.0, 6) if trades else 0.0,
                "cost_usd": round(cost, 6),
                "pnl_usd": round(pnl, 6),
                "roi_pct": round(pnl / cost * 100.0, 6) if cost else 0.0,
                "buckets": {
                    name: {
                        "trades": bucket["trades"],
                        "wins": bucket["wins"],
                        "wr_pct": round(bucket["wins"] / bucket["trades"] * 100.0, 6)
                        if bucket["trades"]
                        else 0.0,
                        "cost_usd": round(bucket["cost_usd"], 6),
                        "pnl_usd": round(bucket["pnl_usd"], 6),
                        "roi_pct": round(bucket["pnl_usd"] / bucket["cost_usd"] * 100.0, 6)
                        if bucket["cost_usd"]
                        else 0.0,
                    }
                    for name, bucket in sorted(record["buckets"].items())
                },
                "samples": record["samples"],
            }
        )
    rows_out.sort(key=lambda item: (item["roi_pct"], item["pnl_usd"], item["trades"]), reverse=True)
    best = rows_out[0] if rows_out else {}
    latest_profile_window_start_s = max((event.window_start_s for event in events), default=0.0) + 300.0
    return {
        "schema_version": 1,
        "flow_stage": "LEARN",
        "lane": "whale_consensus_v1",
        "paper_only": True,
        "live_orders_allowed": False,
        "survivor_bias_guard": "wallet weights for each replayed window use only earlier resolved-window fills from the same capture",
        "decision_rule": "EV>0 in at least one sizable bucket promotes to paper lane; EV<=0 everywhere kills the lane",
        "generated_at": datetime.now(UTC).isoformat(),
        "parameters": {
            "top_k": top_k,
            "order_usd": order_usd,
            "max_entry_offset_s": max_entry_offset_s,
            "thresholds": thresholds,
            "pmax_values": pmax_values,
        },
        "coverage": {
            "events": len(events),
            "windows": len(by_window),
            "windows_without_prior_positive_profile": skipped_no_profile,
        },
        "latest_profile_window_start_s": latest_profile_window_start_s,
        "latest_profile_weights": profile_weight_rows_before(events, latest_profile_window_start_s, limit=top_k),
        "best": best,
        "grid": rows_out,
    }


def _csv_floats(text: str) -> list[float]:
    return [float(item.strip()) for item in text.split(",") if item.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--polygon-jsonl", default=DEFAULT_POLYGON_JSONL)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--thresholds", default="0.5,1,2,5,10,20")
    parser.add_argument("--pmax-values", default="0.45,0.55,0.65,0.75")
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--order-usd", type=float, default=1.0)
    parser.add_argument("--max-entry-offset-s", type=float, default=240.0)
    parser.add_argument("--max-rows", type=int, default=0)
    args = parser.parse_args()

    token_map = load_token_map(Path(args.resolutions))
    events, diagnostics = iter_fill_events(Path(args.polygon_jsonl), token_map, max_rows=args.max_rows)
    report = run_backtest(
        events,
        thresholds=_csv_floats(args.thresholds),
        pmax_values=_csv_floats(args.pmax_values),
        top_k=args.top_k,
        order_usd=args.order_usd,
        max_entry_offset_s=args.max_entry_offset_s,
    )
    report["inputs"] = {
        "polygon_jsonl": args.polygon_jsonl,
        "resolutions": args.resolutions,
        "token_map_entries": len(token_map),
        "diagnostics": diagnostics,
        "max_rows": args.max_rows,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    best = report.get("best") or {}
    print(
        "whale_consensus_v1",
        f"events={report['coverage']['events']}",
        f"windows={report['coverage']['windows']}",
        f"best_trades={best.get('trades', 0)}",
        f"best_roi_pct={best.get('roi_pct', 0.0)}",
        f"output={output}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
