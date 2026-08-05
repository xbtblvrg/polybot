#!/usr/bin/env python3
"""Measure quoted BTC-5m taker cost for a $1 BUY across open markets."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
OFFICIAL_FEE_SCHEDULE_URL = "https://docs.polymarket.com/trading/fees"
CRYPTO_TAKER_FEE_RATE = 0.07
TICKET_USD = 1.0
PRECOMMITTED_EDGE_USD_PER_TICKET = 0.1116
DEFAULT_OUTPUT = "data/research/order134_e6_taker_execution_cost_latest.json"


def quoted_taker_cost(
    *, bid: float, ask: float, fee_rate: float = CRYPTO_TAKER_FEE_RATE
) -> dict[str, float]:
    """Return fee plus midpoint-to-ask crossing cost for a $1 market BUY."""

    if not 0 < bid <= ask <= 1:
        raise ValueError("book prices must satisfy 0 < bid <= ask <= 1")
    shares = TICKET_USD / ask
    midpoint = (bid + ask) / 2.0
    fee_usd = shares * fee_rate * ask * (1.0 - ask)
    half_spread_usd = shares * (ask - midpoint)
    return {
        "shares_for_one_usd": round(shares, 9),
        "fee_usd": round(fee_usd, 9),
        "half_spread_usd": round(half_spread_usd, 9),
        "total_cost_usd": round(fee_usd + half_spread_usd, 9),
    }


def _band(price: float) -> str | None:
    if 0.50 <= price < 0.75:
        return "0.50-0.75"
    if 0.75 <= price < 0.90:
        return "0.75-0.90"
    if 0.90 <= price <= 1.00:
        return "0.90-1.00"
    return None


def _get_json(url: str, params: dict[str, Any]) -> Any:
    request_url = f"{url}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        request_url,
        headers={"User-Agent": "polymarket-agent-order134-e6c/1"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def _json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    return []


def _best_prices(book: dict[str, Any]) -> tuple[float, float] | None:
    bids = [float(row["price"]) for row in book.get("bids") or []]
    asks = [float(row["price"]) for row in book.get("asks") or []]
    if not bids or not asks:
        return None
    return max(bids), min(asks)


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    costs = [float(row["total_cost_usd"]) for row in rows]
    if not costs:
        return {"samples": 0, "mean_cost_usd": None, "max_cost_usd": None}
    ordered = sorted(costs)
    p95_index = min(len(ordered) - 1, max(0, int(0.95 * len(ordered) + 0.999999) - 1))
    return {
        "samples": len(rows),
        "distinct_markets": len(
            {row.get("market_slug") for row in rows if row.get("market_slug")}
        ),
        "distinct_assets": len(
            {row.get("asset_id") for row in rows if row.get("asset_id")}
        ),
        "mean_cost_usd": round(statistics.fmean(costs), 9),
        "median_cost_usd": round(statistics.median(costs), 9),
        "p95_cost_usd": round(ordered[p95_index], 9),
        "max_cost_usd": round(max(costs), 9),
        "mean_fee_usd": round(statistics.fmean(row["fee_usd"] for row in rows), 9),
        "mean_half_spread_usd": round(
            statistics.fmean(row["half_spread_usd"] for row in rows), 9
        ),
    }


def _snapshot_measurement(paths: list[Path]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                continue
            if raw.get("event_type") != "best_bid_ask":
                continue
            try:
                bid = float(raw.get("best_bid"))
                ask = float(raw.get("best_ask"))
                band = _band(ask)
                if band is None:
                    continue
                cost = quoted_taker_cost(bid=bid, ask=ask)
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "asset_id": str(raw.get("asset_id") or ""),
                    "captured_at_iso": raw.get("captured_at_iso"),
                    "band": band,
                    "bid": bid,
                    "ask": ask,
                    **cost,
                }
            )
    distinct_assets = len({row["asset_id"] for row in rows if row["asset_id"]})
    by_band = {
        band: _summarize([row for row in rows if row["band"] == band])
        for band in ("0.50-0.75", "0.75-0.90", "0.90-1.00")
    }
    # Each binary BTC-5m market has exactly two outcome token IDs, so this is
    # a conservative market-count lower bound even without a slug join.
    market_count_lower_bound = (distinct_assets + 1) // 2
    captured = [str(row.get("captured_at_iso") or "") for row in rows]
    return {
        "source_paths": [str(path) for path in paths],
        "rows": len(rows),
        "distinct_assets": distinct_assets,
        "distinct_binary_markets_lower_bound": market_count_lower_bound,
        "market_sample_pass": market_count_lower_bound >= 20,
        "captured_at_min": min(captured) if captured else None,
        "captured_at_max": max(captured) if captured else None,
        "overall": _summarize(rows),
        "by_band": by_band,
    }


def sample(
    *, market_count: int, forward_windows: int, snapshot_paths: list[Path] | None = None
) -> dict[str, Any]:
    start = int(time.time() // 300 * 300)
    rows: list[dict[str, Any]] = []
    open_markets: list[str] = []
    errors: list[dict[str, str]] = []
    for offset in range(forward_windows):
        slug = f"btc-updown-5m-{start + offset * 300}"
        try:
            markets = _get_json(GAMMA_MARKETS_URL, {"slug": slug})
            market = markets[0] if isinstance(markets, list) and markets else None
            if not isinstance(market, dict):
                continue
            if not (
                market.get("active") is True
                and market.get("closed") is False
                and market.get("acceptingOrders") is True
            ):
                continue
            tokens = _json_array(market.get("clobTokenIds"))
            outcomes = _json_array(market.get("outcomes"))
            if len(tokens) != 2 or len(outcomes) != 2:
                continue
            open_markets.append(slug)
            fee_schedule = market.get("feeSchedule") or {}
            fee_rate = float(fee_schedule.get("rate") or CRYPTO_TAKER_FEE_RATE)
            for token, outcome in zip(tokens, outcomes):
                book = _get_json(CLOB_BOOK_URL, {"token_id": token})
                prices = _best_prices(book)
                if prices is None:
                    continue
                bid, ask = prices
                band = _band(ask)
                if band is None:
                    continue
                cost = quoted_taker_cost(bid=bid, ask=ask, fee_rate=fee_rate)
                rows.append(
                    {
                        "market_slug": slug,
                        "condition_id": market.get("conditionId"),
                        "token_id": token,
                        "outcome": outcome,
                        "band": band,
                        "bid": bid,
                        "ask": ask,
                        "spread": round(ask - bid, 9),
                        "fee_rate": fee_rate,
                        **cost,
                    }
                )
            if len(open_markets) >= market_count:
                break
        except Exception as exc:  # public measurement: retain bounded errors
            errors.append({"market_slug": slug, "error": str(exc)[:300]})

    by_band = {
        band: _summarize([row for row in rows if row["band"] == band])
        for band in ("0.50-0.75", "0.75-0.90", "0.90-1.00")
    }
    overall = _summarize(rows)
    market_sample_pass = len(set(open_markets)) >= market_count
    historical_touch = _snapshot_measurement(snapshot_paths or [])
    historical_band_means = [
        float(summary["mean_cost_usd"])
        for summary in historical_touch["by_band"].values()
        if summary.get("mean_cost_usd") is not None
    ]
    historical_complete = bool(
        historical_touch["market_sample_pass"]
        and len(historical_band_means) == 3
    )
    gate_cost = (
        max(historical_band_means)
        if historical_complete
        else overall.get("max_cost_usd")
    )
    clears = bool(
        market_sample_pass
        and gate_cost is not None
        and gate_cost < PRECOMMITTED_EDGE_USD_PER_TICKET
    )
    band_costs = historical_touch["by_band"]
    high_band_cost = max(
        float(band_costs[band]["mean_cost_usd"])
        for band in ("0.75-0.90", "0.90-1.00")
        if band_costs[band].get("mean_cost_usd") is not None
    ) if historical_complete else None
    low_band_cost = band_costs["0.50-0.75"].get("mean_cost_usd")
    bucket_edges = [
        {
            "wallet": "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b",
            "move_slice_key": "120-180|0.50-0.75",
            "gross_ev_usd_per_ticket": 0.101878,
            "measured_cost_usd_per_ticket": low_band_cost,
        },
        {
            "wallet": "0x6663e52b3683832aa611b0c7e0e91bc654d368ca",
            "move_slice_key": "120-180|>0.75",
            "gross_ev_usd_per_ticket": 0.04398635,
            "measured_cost_usd_per_ticket": high_band_cost,
        },
        {
            "wallet": "0x82c857cb4d18e919c1b7d3c6865be4debe50da77",
            "move_slice_key": "240-300|0.50-0.75",
            "gross_ev_usd_per_ticket": 0.19059261,
            "measured_cost_usd_per_ticket": low_band_cost,
        },
    ]
    for bucket in bucket_edges:
        measured_cost = bucket["measured_cost_usd_per_ticket"]
        bucket["net_ev_usd_per_ticket"] = (
            round(bucket["gross_ev_usd_per_ticket"] - float(measured_cost), 9)
            if measured_cost is not None
            else None
        )
    return {
        "schema_version": 1,
        "kind": "order134_e6_taker_execution_cost",
        "flow_stage": "LIVE/DEFEND/PROMOTE/LEARN/SELF-DEV",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_authority": False,
        "read_only_public_endpoints": True,
        "official_fee_schedule": {
            "url": OFFICIAL_FEE_SCHEDULE_URL,
            "category": "Crypto",
            "fee_rate": CRYPTO_TAKER_FEE_RATE,
            "formula": "fee_usd = shares * fee_rate * price * (1-price)",
            "makers_charged": False,
            "match_time_fee": True,
        },
        "measurement": {
            "ticket_usd": TICKET_USD,
            "touch_cost_formula": (
                "fee_usd + (1/ask) * (ask - (bid+ask)/2)"
            ),
            "requested_open_markets": market_count,
            "sampled_open_markets": len(set(open_markets)),
            "market_sample_pass": market_sample_pass,
            "open_market_slugs": open_markets,
            "overall": overall,
            "by_band": by_band,
            "errors": errors,
        },
        "historical_open_book_touch_supplement": historical_touch,
        "three_bucket_net_ev": bucket_edges,
        "precommitted_branch": {
            "edge_usd_per_ticket": PRECOMMITTED_EDGE_USD_PER_TICKET,
            "cost_gate_value_usd_per_ticket": gate_cost,
            "comparison_basis": (
                "maximum of per-band mean quoted costs across timestamped open-book snapshots"
                if historical_complete
                else "maximum quoted cost across current open-market rows"
            ),
            "clears_cost_gate": clears,
            "verdict": (
                "E6_B_PROCEEDS"
                if clears
                else "E6_B_DOES_NOT_SHIP"
                if market_sample_pass
                else "INSUFFICIENT_OPEN_MARKET_SAMPLE"
            ),
        },
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-count", type=int, default=20)
    parser.add_argument("--forward-windows", type=int, default=48)
    parser.add_argument("--snapshot", action="append", default=[])
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    report = sample(
        market_count=max(20, args.market_count),
        forward_windows=max(args.forward_windows, args.market_count),
        snapshot_paths=[Path(value) for value in args.snapshot],
    )
    atomic_write_json(Path(args.output), report)
    print(json.dumps({
        "measurement": report["measurement"],
        "precommitted_branch": report["precommitted_branch"],
    }, sort_keys=True))
    return 0 if report["measurement"]["market_sample_pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
