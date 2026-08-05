#!/usr/bin/env python3
"""Build the OBSERVE-stage top-10 broad paper lane state.

This script is paper/research only. It ranks the widened leaderboard universe,
attaches any existing per-wallet paper/profit evidence, and records which
wallets still need realtime paper-copy measurement before rotation decisions.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.mission import WALLET_COPY_MISSION_CONTRACT  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.alpha_decay import ExecutionProfileConfig, build_execution_profiles  # noqa: E402
from src.wallet_copy.alpha_freshness import require_fresh_alpha_report  # noqa: E402
from src.wallet_copy.market_categories import (  # noqa: E402
    market_categories_from_metadata,
    summarize_wallet_market_categories,
)
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_ACTIVITY_JSONL = "data/research/polygon_orderfilled_ws_capture.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leaderboard-state", default="data/research/wallet_copy_leaderboard_crypto_state.json")
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--live-fill-report", default="data/research/wallet_copy_live_fill_quality_report.json")
    parser.add_argument("--measurement-state", default="data/research/wallet_copy_top10_broad_paper_measurement_state.json")
    parser.add_argument("--comparison-measurement-state", action="append", default=[])
    parser.add_argument("--alpha-decay-report", default="data/research/alpha_decay_report.json")
    parser.add_argument("--alpha-profile-horizon-s", type=float, default=2.0)
    parser.add_argument("--alpha-profile-min-fills", type=int, default=20)
    parser.add_argument("--alpha-profile-min-positive-edge-fraction", type=float, default=0.70)
    parser.add_argument("--alpha-profile-min-mean-edge", type=float, default=0.0)
    parser.add_argument("--alpha-profile-min-median-edge", type=float, default=0.0)
    parser.add_argument("--alpha-profile-max-observation-lag-s", type=float, default=5.0)
    parser.add_argument(
        "--candidate-allowlist-state",
        default="",
        help="Optional copyability/queue artifact whose wallets bound this paper lane.",
    )
    parser.add_argument(
        "--candidate-allowlist-status",
        default="READY_QUEUE",
        help="Admission status required from candidate-allowlist-state ranked_queue rows.",
    )
    parser.add_argument("--activity-jsonl", default=DEFAULT_ACTIVITY_JSONL)
    parser.add_argument("--activity-scan-limit", type=int, default=250_000)
    parser.add_argument(
        "--min-source-buy-usd",
        type=float,
        default=0.0,
        help=(
            "Recent BUY notional needed to be copy-sized under the active paper policy. "
            "Defaults to mission min_order_usd / wallet_fraction."
        ),
    )
    parser.add_argument(
        "--min-ask-depth-usd",
        type=float,
        default=0.0,
        help=(
            "Optional liquidity criterion for recent copy-sized BUY activity. "
            "When >0, recent BUY assets are CLOB-refetched and ranking prefers wallets "
            "whose copy-sized BUYs occurred on assets with at least this best-ask depth."
        ),
    )
    parser.add_argument("--liquidity-clob-base-url", default="http://127.0.0.1:8787/clob")
    parser.add_argument("--liquidity-clob-timeout-s", type=float, default=1.5)
    parser.add_argument("--liquidity-clob-retries", type=int, default=1)
    parser.add_argument("--liquidity-max-assets", type=int, default=250)
    parser.add_argument(
        "--liquidity-disable-source-base-overrides",
        action="store_true",
        help="Temporarily clear POLYMARKET_CLOB_API_BASE_URL while refetching liquidity books.",
    )
    parser.add_argument("--no-prefer-active", action="store_true")
    parser.add_argument("--output", default="data/research/wallet_copy_top10_broad_paper_lane_state.json")
    parser.add_argument("--limit", type=int, default=10)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _opposite_side(side: str) -> str:
    value = str(side or "").upper()
    if value == "BUY":
        return "SELL"
    if value == "SELL":
        return "BUY"
    return value


def _activity_wallet_sides(row: dict[str, Any]) -> list[tuple[str, str]]:
    if row.get("event") != "polygon_orderfilled_log" or row.get("source") != "polygon_ws":
        return []
    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    maker = _norm_wallet(row.get("maker") or decoded.get("maker"))
    taker = _norm_wallet(row.get("taker") or decoded.get("taker") or row.get("topic3"))
    selected = _norm_wallet(row.get("selected_wallet"))
    maker_side = str(decoded.get("maker_side") or decoded.get("side") or "UNKNOWN").upper()
    selected_side = str(decoded.get("side") or "UNKNOWN").upper()
    out: dict[str, str] = {}
    if maker:
        out[maker] = maker_side
    if taker:
        out[taker] = _opposite_side(maker_side)
    if selected and selected not in out:
        out[selected] = selected_side
    return [(wallet, side) for wallet, side in out.items() if wallet and side]


def _source_usd(row: dict[str, Any]) -> float:
    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    return round(max(0.0, num(decoded.get("price")) * num(decoded.get("size"))), 6)


def _asset_id(row: dict[str, Any]) -> str:
    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    return str(decoded.get("asset") or decoded.get("asset_id") or row.get("asset") or row.get("asset_id") or "")


def _mission_sizing_defaults() -> dict[str, float]:
    runtime = WALLET_COPY_MISSION_CONTRACT.get("current_runtime_phase_contract")
    runtime = runtime if isinstance(runtime, dict) else {}
    policy = runtime.get("profitability_filter_contract")
    policy = policy if isinstance(policy, dict) else {}
    wallet_fraction = num(policy.get("wallet_fraction"), 0.05)
    min_order_usd = num(policy.get("min_live_order_usd"), 1.0)
    return {
        "wallet_fraction": wallet_fraction,
        "min_order_usd": min_order_usd,
        "min_source_buy_usd": round(min_order_usd / wallet_fraction, 6) if wallet_fraction > 0 else 0.0,
    }


def _empty_activity_profile() -> dict[str, Any]:
    return {
        "events": 0,
        "buy_events": 0,
        "sell_events": 0,
        "active_hour_of_week_count": 0,
        "buy_hour_of_week_count": 0,
        "copy_sized_buy_hour_of_week_count": 0,
        "active_hour_of_week_coverage_pct": 0.0,
        "active_hour_of_week_counts": {},
        "buy_hour_of_week_counts": {},
        "copy_sized_buy_hour_of_week_counts": {},
        "copy_sized_buy_events": 0,
        "liquid_copy_sized_buy_events": 0,
        "total_source_buy_usd": 0.0,
        "max_source_buy_usd": 0.0,
        "max_recent_ask_depth_usd": 0.0,
        "liquidity_checked_assets": 0,
        "liquidity_eligible_assets": 0,
        "liquidity_unchecked_assets": 0,
        "liquidity_reject_reasons": {},
        "recent_buy_assets": {},
    }


def _row_timestamp_s(row: dict[str, Any]) -> float:
    for key in (
        "timestamp",
        "ts",
        "observed_ts",
        "received_at_s",
        "captured_at_s",
        "block_timestamp",
        "block_ts",
    ):
        value = row.get(key)
        if value is None:
            continue
        try:
            number = float(value)
            if number > 10_000_000_000:
                number /= 1000.0
            return number
        except (TypeError, ValueError):
            pass
        text = str(value or "").strip()
        if not text:
            continue
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    return _row_timestamp_s(decoded) if decoded else 0.0


def _hour_of_week(timestamp_s: float) -> str:
    if timestamp_s <= 0:
        return ""
    dt = datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
    return f"{dt.weekday() * 24 + dt.hour:03d}"


def _bump_hour_bucket(profile: dict[str, Any], key: str, hour: str) -> None:
    if not hour:
        return
    counts = profile.setdefault(key, {})
    counts[hour] = int(counts.get(hour) or 0) + 1


def _finalize_activity_profile(profile: dict[str, Any]) -> None:
    active_hours = profile.get("active_hour_of_week_counts") if isinstance(
        profile.get("active_hour_of_week_counts"), dict
    ) else {}
    buy_hours = profile.get("buy_hour_of_week_counts") if isinstance(profile.get("buy_hour_of_week_counts"), dict) else {}
    copy_sized_hours = (
        profile.get("copy_sized_buy_hour_of_week_counts")
        if isinstance(profile.get("copy_sized_buy_hour_of_week_counts"), dict)
        else {}
    )
    profile["active_hour_of_week_count"] = len(active_hours)
    profile["buy_hour_of_week_count"] = len(buy_hours)
    profile["copy_sized_buy_hour_of_week_count"] = len(copy_sized_hours)
    profile["active_hour_of_week_coverage_pct"] = round(len(active_hours) / 168.0 * 100.0, 6)


def load_recent_activity_counts(path: str, *, limit: int) -> dict[str, int]:
    profiles = load_recent_activity_profiles(path, limit=limit, min_source_buy_usd=0.0)
    return {wallet: int(profile.get("events") or 0) for wallet, profile in profiles.items()}


def load_recent_activity_profiles(path: str, *, limit: int, min_source_buy_usd: float = 0.0) -> dict[str, dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return {}
    profiles: dict[str, dict[str, Any]] = {}
    scanned = 0
    orderfilled_rows = 0
    with target.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        chunks: list[bytes] = []
        while position > 0 and scanned < 128_000_000:
            size = min(1_048_576, position, 128_000_000 - scanned)
            position -= size
            handle.seek(position)
            chunks.append(handle.read(size))
            scanned += size
    for raw in reversed(b"".join(reversed(chunks)).splitlines()):
        try:
            row = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(row, dict):
            continue
        wallet_sides = _activity_wallet_sides(row)
        if not wallet_sides:
            continue
        orderfilled_rows += 1
        source_usd = _source_usd(row)
        asset = _asset_id(row)
        hour = _hour_of_week(_row_timestamp_s(row))
        for wallet, side in wallet_sides:
            profile = profiles.setdefault(wallet, _empty_activity_profile())
            profile["events"] = int(profile.get("events") or 0) + 1
            _bump_hour_bucket(profile, "active_hour_of_week_counts", hour)
            if side == "BUY":
                profile["buy_events"] = int(profile.get("buy_events") or 0) + 1
                _bump_hour_bucket(profile, "buy_hour_of_week_counts", hour)
                profile["total_source_buy_usd"] = round(num(profile.get("total_source_buy_usd")) + source_usd, 6)
                profile["max_source_buy_usd"] = round(max(num(profile.get("max_source_buy_usd")), source_usd), 6)
                is_copy_sized = source_usd + 1e-9 >= float(min_source_buy_usd)
                if is_copy_sized:
                    profile["copy_sized_buy_events"] = int(profile.get("copy_sized_buy_events") or 0) + 1
                    _bump_hour_bucket(profile, "copy_sized_buy_hour_of_week_counts", hour)
                if asset:
                    assets = profile.setdefault("recent_buy_assets", {})
                    asset_row = assets.setdefault(
                        asset,
                        {
                            "asset": asset,
                            "buy_events": 0,
                            "copy_sized_buy_events": 0,
                            "total_source_buy_usd": 0.0,
                            "max_source_buy_usd": 0.0,
                        },
                    )
                    asset_row["buy_events"] = int(asset_row.get("buy_events") or 0) + 1
                    asset_row["total_source_buy_usd"] = round(num(asset_row.get("total_source_buy_usd")) + source_usd, 6)
                    asset_row["max_source_buy_usd"] = round(max(num(asset_row.get("max_source_buy_usd")), source_usd), 6)
                    if is_copy_sized:
                        asset_row["copy_sized_buy_events"] = int(asset_row.get("copy_sized_buy_events") or 0) + 1
            elif side == "SELL":
                profile["sell_events"] = int(profile.get("sell_events") or 0) + 1
        if orderfilled_rows >= max(1, int(limit)):
            break
    for profile in profiles.values():
        _finalize_activity_profile(profile)
    return profiles


def _book_top_of_book(book: dict[str, Any]) -> dict[str, Any]:
    asks = sorted(
        ((num(row.get("price")), num(row.get("size"))) for row in (book.get("asks") or []) if isinstance(row, dict)),
        key=lambda item: item[0],
    )
    bids = sorted(
        ((num(row.get("price")), num(row.get("size"))) for row in (book.get("bids") or []) if isinstance(row, dict)),
        key=lambda item: item[0],
        reverse=True,
    )
    best_ask = asks[0][0] if asks else 0.0
    best_bid = bids[0][0] if bids else 0.0
    best_ask_shares = sum(size for price, size in asks if best_ask > 0 and abs(price - best_ask) <= 1e-9)
    best_bid_shares = sum(size for price, size in bids if best_bid > 0 and abs(price - best_bid) <= 1e-9)
    return {
        "asset_id": book.get("asset_id"),
        "best_ask": round(best_ask, 6),
        "best_bid": round(best_bid, 6),
        "ask_levels": len(asks),
        "bid_levels": len(bids),
        "best_ask_depth_shares": round(best_ask_shares, 6),
        "best_bid_depth_shares": round(best_bid_shares, 6),
        "best_ask_depth_usd": round(best_ask * best_ask_shares, 6) if best_ask > 0 else 0.0,
        "best_bid_depth_usd": round(best_bid * best_bid_shares, 6) if best_bid > 0 else 0.0,
        "route_report": book.get("__walletCopyClobRouteReport") if isinstance(book.get("__walletCopyClobRouteReport"), dict) else {},
    }


def apply_liquidity_depth_filter(
    profiles: dict[str, dict[str, Any]],
    *,
    clob: CLOBMarketClient,
    min_ask_depth_usd: float,
    max_assets: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Annotate recent activity with current CLOB best-ask depth evidence."""

    threshold = max(0.0, float(min_ask_depth_usd or 0.0))
    asset_pressure: dict[str, dict[str, Any]] = {}
    for profile in profiles.values():
        assets = profile.get("recent_buy_assets") if isinstance(profile.get("recent_buy_assets"), dict) else {}
        for asset, raw in assets.items():
            if not isinstance(raw, dict):
                continue
            copy_sized = int(raw.get("copy_sized_buy_events") or 0)
            if copy_sized <= 0:
                continue
            row = asset_pressure.setdefault(
                str(asset),
                {
                    "asset": str(asset),
                    "copy_sized_buy_events": 0,
                    "buy_events": 0,
                    "total_source_buy_usd": 0.0,
                    "max_source_buy_usd": 0.0,
                },
            )
            row["copy_sized_buy_events"] += copy_sized
            row["buy_events"] += int(raw.get("buy_events") or 0)
            row["total_source_buy_usd"] = round(num(row.get("total_source_buy_usd")) + num(raw.get("total_source_buy_usd")), 6)
            row["max_source_buy_usd"] = round(max(num(row.get("max_source_buy_usd")), num(raw.get("max_source_buy_usd"))), 6)

    ranked_assets = sorted(
        asset_pressure.values(),
        key=lambda row: (
            -int(row.get("copy_sized_buy_events") or 0),
            -num(row.get("total_source_buy_usd")),
            str(row.get("asset") or ""),
        ),
    )
    selected_assets = {str(row.get("asset")) for row in ranked_assets[: max(0, int(max_assets))]}
    asset_liquidity: dict[str, dict[str, Any]] = {}
    classification_counts: Counter[str] = Counter()
    for asset in selected_assets:
        try:
            book = clob.get_book(asset)
            top = _book_top_of_book(book)
            depth = num(top.get("best_ask_depth_usd"))
            classification = "eligible" if depth + 1e-9 >= threshold else "ask_depth_below_threshold"
            asset_liquidity[asset] = {
                "asset": asset,
                "status": "PASS",
                "classification": classification,
                "eligible": classification == "eligible",
                "top_of_book": top,
            }
        except Exception as exc:  # noqa: BLE001 - selector evidence should preserve route failures.
            classification = "book_fetch_error"
            asset_liquidity[asset] = {
                "asset": asset,
                "status": "ERROR",
                "classification": classification,
                "eligible": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        classification_counts[classification] += int(asset_pressure.get(asset, {}).get("copy_sized_buy_events") or 1)

    enriched: dict[str, dict[str, Any]] = {}
    for wallet, raw_profile in profiles.items():
        profile = {**_empty_activity_profile(), **(raw_profile if isinstance(raw_profile, dict) else {})}
        reasons: Counter[str] = Counter()
        checked_assets = 0
        eligible_assets = 0
        unchecked_assets = 0
        liquid_copy_sized = 0
        max_depth = 0.0
        assets = profile.get("recent_buy_assets") if isinstance(profile.get("recent_buy_assets"), dict) else {}
        for asset, raw in assets.items():
            if not isinstance(raw, dict) or int(raw.get("copy_sized_buy_events") or 0) <= 0:
                continue
            liquidity = asset_liquidity.get(str(asset))
            if not isinstance(liquidity, dict):
                unchecked_assets += 1
                reasons["asset_not_checked"] += int(raw.get("copy_sized_buy_events") or 0)
                continue
            checked_assets += 1
            top = liquidity.get("top_of_book") if isinstance(liquidity.get("top_of_book"), dict) else {}
            max_depth = max(max_depth, num(top.get("best_ask_depth_usd")))
            if liquidity.get("eligible"):
                eligible_assets += 1
                liquid_copy_sized += int(raw.get("copy_sized_buy_events") or 0)
            else:
                reasons[str(liquidity.get("classification") or "not_eligible")] += int(raw.get("copy_sized_buy_events") or 0)
        profile["liquid_copy_sized_buy_events"] = liquid_copy_sized
        profile["max_recent_ask_depth_usd"] = round(max_depth, 6)
        profile["liquidity_checked_assets"] = checked_assets
        profile["liquidity_eligible_assets"] = eligible_assets
        profile["liquidity_unchecked_assets"] = unchecked_assets
        profile["liquidity_reject_reasons"] = dict(sorted(reasons.items()))
        enriched[wallet] = profile

    return enriched, {
        "enabled": True,
        "min_ask_depth_usd": round(threshold, 6),
        "max_assets": int(max_assets),
        "candidate_assets": len(ranked_assets),
        "checked_assets": len(asset_liquidity),
        "eligible_assets": sum(1 for row in asset_liquidity.values() if row.get("eligible")),
        "classification_counts": dict(sorted(classification_counts.items())),
        "assets": asset_liquidity,
    }


def _max_period_metric(row: dict[str, Any], key: str) -> float:
    values = row.get(key) if isinstance(row.get(key), dict) else {}
    out = 0.0
    for value in values.values():
        try:
            out = max(out, float(value or 0.0))
        except (TypeError, ValueError):
            continue
    return out


def _min_rank(row: dict[str, Any]) -> int:
    ranks = row.get("ranks") if isinstance(row.get("ranks"), dict) else {}
    values: list[int] = []
    for value in ranks.values():
        try:
            values.append(int(value))
        except (TypeError, ValueError):
            continue
    return min(values) if values else 999999


def _wallet_evidence_by_address(profit_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    individual = (
        profit_state.get("individual_wallet_copy_universe")
        if isinstance(profit_state.get("individual_wallet_copy_universe"), dict)
        else {}
    )
    rows = individual.get("top_wallets_by_candidate_score") if isinstance(individual, dict) else []
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").lower()
        if not wallet:
            continue
        orders = int(row.get("orders") or 0)
        resolved_orders = int(row.get("resolved_orders") or 0)
        blockers = [str(item) for item in (row.get("blockers") or [])]
        status = str(row.get("status") or "")
        copyable_rate = None
        if orders > 0 and "candidate_missing_clob_fill_evidence" not in blockers:
            copyable_rate = 100.0
        out[wallet] = {
            "candidate_id": row.get("candidate_id"),
            "policy_id": row.get("policy_id"),
            "paper_pnl_usd": row.get("pnl_usd"),
            "paper_roi_pct": row.get("roi_pct"),
            "paper_wr_pct": row.get("wr_pct"),
            "paper_orders": orders,
            "paper_resolved_orders": resolved_orders,
            "copyable_rate_pct": copyable_rate,
            "status": status,
            "blockers": blockers,
        }
    return out


def _measurement_evidence_by_address(measurement_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    wallets = measurement_state.get("wallets") if isinstance(measurement_state.get("wallets"), dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for wallet, row in wallets.items():
        if not isinstance(row, dict):
            continue
        address = str(row.get("wallet") or wallet or "").lower()
        if not address.startswith("0x"):
            continue
        buy_events = int(row.get("buy_events") or 0)
        events_seen = int(row.get("events_seen") or 0)
        blockers: list[str] = []
        if events_seen <= 0:
            blockers.append("realtime_events_missing")
        if buy_events <= 0:
            blockers.append("realtime_buy_sample_missing")
        copyable_rate = row.get("copyable_rate_pct")
        if copyable_rate is None and buy_events <= 0:
            copyable_rate = 0.0
        out[address] = {
            "market_categories": row.get("market_categories") if isinstance(row.get("market_categories"), list) else [],
            "primary_market_category": row.get("primary_market_category") or "",
            "paper_pnl_usd": row.get("paper_pnl_usd", 0.0),
            "paper_realized_pnl_usd": row.get("realized_pnl_usd", 0.0),
            "paper_unrealized_pnl_usd": row.get("unrealized_pnl_usd", 0.0),
            "paper_orders": row.get("paper_orders", buy_events),
            "paper_resolved_orders": row.get("resolved_orders", 0),
            "copyable_rate_pct": copyable_rate,
            "observed_realtime_events": events_seen,
            "buy_events": buy_events,
            "sell_events": int(row.get("sell_events") or 0),
            "copyable_buy_events": int(row.get("copyable_buy_events") or 0),
            "rejected_buy_events": int(row.get("rejected_buy_events") or 0),
            "open_positions": int(row.get("open_positions") or 0),
            "open_cost_usd": row.get("open_cost_usd", 0.0),
            "sample_status": row.get("sample_status") or "UNKNOWN",
            "reject_reasons": row.get("reject_reasons") if isinstance(row.get("reject_reasons"), dict) else {},
            "status": row.get("sample_status") or "REALTIME_MEASURED",
            "blockers": blockers,
        }
    return out


def _policy_id_from_measurement(measurement_state: dict[str, Any], *, fallback: str) -> str:
    sizing = measurement_state.get("sizing") if isinstance(measurement_state.get("sizing"), dict) else {}
    policy_id = str(measurement_state.get("policy_id") or sizing.get("policy_id") or "").strip()
    if policy_id:
        return policy_id
    wallet_fraction = num(sizing.get("wallet_fraction"))
    max_order_usd = num(sizing.get("max_order_usd"))
    min_order_usd = num(sizing.get("min_order_usd"))
    if wallet_fraction > 0 and max_order_usd > 0 and min_order_usd > 0:
        fraction = str(round(wallet_fraction, 6)).replace(".", "p")
        max_order = str(round(max_order_usd, 6)).replace(".", "p")
        min_order = str(round(min_order_usd, 6)).replace(".", "p")
        return f"wf{fraction}_max{max_order}_min{min_order}"
    return fallback


def _measurement_policy_states(
    primary_measurement_state: dict[str, Any] | None,
    comparison_measurement_states: list[dict[str, Any]] | None,
) -> list[tuple[str, dict[str, Any]]]:
    states = []
    if isinstance(primary_measurement_state, dict) and primary_measurement_state:
        states.append(primary_measurement_state)
    for state in comparison_measurement_states or []:
        if isinstance(state, dict) and state:
            states.append(state)
    out: list[tuple[str, dict[str, Any]]] = []
    seen: Counter[str] = Counter()
    for index, state in enumerate(states, start=1):
        base = _policy_id_from_measurement(state, fallback=f"policy_{index}")
        seen[base] += 1
        policy_id = base if seen[base] == 1 else f"{base}_{seen[base]}"
        out.append((policy_id, state))
    return out


def _policy_measurements_by_address(policy_states: list[tuple[str, dict[str, Any]]]) -> dict[str, dict[str, dict[str, Any]]]:
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for policy_id, state in policy_states:
        wallets = state.get("wallets") if isinstance(state.get("wallets"), dict) else {}
        sizing = state.get("sizing") if isinstance(state.get("sizing"), dict) else {}
        for wallet, row in wallets.items():
            if not isinstance(row, dict):
                continue
            address = _norm_wallet(row.get("wallet") or wallet)
            if not address:
                continue
            out.setdefault(address, {})[policy_id] = {
                "policy_id": policy_id,
                "wallet_fraction": sizing.get("wallet_fraction"),
                "max_order_usd": sizing.get("max_order_usd"),
                "min_order_usd": sizing.get("min_order_usd"),
                "paper_pnl_usd": row.get("paper_pnl_usd", 0.0),
                "paper_realized_pnl_usd": row.get("realized_pnl_usd", 0.0),
                "paper_unrealized_pnl_usd": row.get("unrealized_pnl_usd", 0.0),
                "copyable_rate_pct": row.get("copyable_rate_pct"),
                "paper_orders": row.get("paper_orders", row.get("buy_events", 0)),
                "buy_events": int(row.get("buy_events") or 0),
                "copyable_buy_events": int(row.get("copyable_buy_events") or 0),
                "rejected_buy_events": int(row.get("rejected_buy_events") or 0),
                "below_min_order_events": int(row.get("below_min_order_events") or 0),
                "reject_reasons": row.get("reject_reasons") if isinstance(row.get("reject_reasons"), dict) else {},
                "sample_status": row.get("sample_status") or "UNKNOWN",
            }
    return out


def _paper_policy_gate(
    policy_measurements: dict[str, dict[str, Any]],
    fallback_evidence: dict[str, Any],
) -> dict[str, Any]:
    rows: list[tuple[str, dict[str, Any]]] = []
    for policy_id, measurement in sorted(policy_measurements.items()):
        if isinstance(measurement, dict):
            rows.append((str(policy_id), measurement))

    if not rows and fallback_evidence:
        has_paper = fallback_evidence.get("paper_pnl_usd") is not None
        has_copyable = fallback_evidence.get("copyable_rate_pct") is not None
        if has_paper or has_copyable:
            rows.append(
                (
                    str(fallback_evidence.get("policy_id") or "attached_paper_evidence"),
                    {
                        "paper_pnl_usd": fallback_evidence.get("paper_pnl_usd"),
                        "copyable_rate_pct": fallback_evidence.get("copyable_rate_pct"),
                        "copyable_buy_events": fallback_evidence.get("copyable_buy_events", 0),
                        "buy_events": fallback_evidence.get("buy_events", fallback_evidence.get("paper_orders", 0)),
                    },
                )
            )

    if not rows:
        return {
            "assessed": False,
            "eligible": False,
            "eligible_policy_ids": [],
            "best_policy_id": "",
            "best_policy_paper_pnl_usd": None,
            "best_policy_copyable_rate_pct": None,
            "best_policy_copyable_buy_events": 0,
            "has_positive_paper_pnl": False,
            "has_copyable_buy": False,
            "has_buy_sample": False,
        }

    eligible_policy_ids: list[str] = []
    best_policy_id = ""
    best_pnl: float | None = None
    best_rate: float | None = None
    best_copyable = 0
    has_positive_paper_pnl = False
    has_copyable_buy = False
    has_buy_sample = False
    for policy_id, measurement in rows:
        pnl = num(measurement.get("paper_pnl_usd"))
        rate_raw = measurement.get("copyable_rate_pct")
        rate = num(rate_raw) if rate_raw is not None else None
        copyable_buy_events = int(measurement.get("copyable_buy_events") or 0)
        buy_events = int(measurement.get("buy_events") or measurement.get("paper_orders") or 0)
        has_positive_paper_pnl = has_positive_paper_pnl or pnl > 0.0
        has_copyable_buy = has_copyable_buy or copyable_buy_events > 0 or (rate is not None and rate > 0.0)
        has_buy_sample = has_buy_sample or buy_events > 0
        if best_pnl is None or pnl > best_pnl:
            best_pnl = pnl
            best_policy_id = policy_id
        if rate is not None and (best_rate is None or rate > best_rate):
            best_rate = rate
        best_copyable = max(best_copyable, copyable_buy_events)
        if pnl > 0.0 and (copyable_buy_events > 0 or (rate is not None and rate > 0.0)):
            eligible_policy_ids.append(policy_id)

    return {
        "assessed": True,
        "eligible": bool(eligible_policy_ids),
        "eligible_policy_ids": eligible_policy_ids,
        "best_policy_id": best_policy_id,
        "best_policy_paper_pnl_usd": round(best_pnl, 6) if best_pnl is not None else None,
        "best_policy_copyable_rate_pct": round(best_rate, 6) if best_rate is not None else None,
        "best_policy_copyable_buy_events": best_copyable,
        "has_positive_paper_pnl": has_positive_paper_pnl,
        "has_copyable_buy": has_copyable_buy,
        "has_buy_sample": has_buy_sample,
    }


def _policy_summaries(policy_states: list[tuple[str, dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for policy_id, state in policy_states:
        liquidity = (
            state.get("liquidity_diagnosis")
            if isinstance(state.get("liquidity_diagnosis"), dict)
            else {"enabled": False, "status": "MISSING"}
        )
        summaries[policy_id] = {
            "status": state.get("status"),
            "updated_at": state.get("updated_at"),
            "sizing": state.get("sizing") if isinstance(state.get("sizing"), dict) else {},
            "summary": state.get("summary") if isinstance(state.get("summary"), dict) else {},
            "market_categories": (
                (state.get("summary") if isinstance(state.get("summary"), dict) else {}).get("market_categories")
                if isinstance((state.get("summary") if isinstance(state.get("summary"), dict) else {}).get("market_categories"), dict)
                else {}
            ),
            "liquidity_diagnosis": {
                "enabled": liquidity.get("enabled"),
                "status": liquidity.get("status"),
                "target_events": liquidity.get("target_events", 0),
                "target_assets": liquidity.get("target_assets", 0),
                "diagnosed_assets": liquidity.get("diagnosed_assets", 0),
                "classification_counts": (
                    liquidity.get("classification_counts")
                    if isinstance(liquidity.get("classification_counts"), dict)
                    else {}
                ),
                "dominant_classification": liquidity.get("dominant_classification", ""),
            },
        }
    return summaries


def _candidate_market_category_counts(candidates: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in candidates:
        categories = market_categories_from_metadata(row)
        counts[categories[0] if categories else "unknown"] += 1
    return dict(sorted(counts.items()))


def _execution_profile_rank(
    address: str,
    profiles: dict[str, dict[str, Any]],
    *,
    enabled: bool,
) -> tuple[int, float, float, int]:
    if not enabled:
        return (0, 0.0, 0.0, 0)
    profile = profiles.get(address) if isinstance(profiles.get(address), dict) else {}
    if not profile:
        return (2, 0.0, 0.0, 0)
    return (
        0 if profile.get("eligible") is True else 1,
        -num(profile.get("mean_edge"), -1_000_000_000.0),
        -num(profile.get("copyable_rate_pct")),
        -int(profile.get("fill_sample") or 0),
    )


def build_state(
    *,
    leaderboard_state: dict[str, Any],
    profit_state: dict[str, Any],
    live_fill_report: dict[str, Any],
    measurement_state: dict[str, Any] | None = None,
    comparison_measurement_states: list[dict[str, Any]] | None = None,
    execution_profiles_state: dict[str, Any] | None = None,
    recent_activity_counts: dict[str, Any] | None = None,
    min_source_buy_usd: float = 0.0,
    min_ask_depth_usd: float = 0.0,
    liquidity_summary: dict[str, Any] | None = None,
    candidate_allowlist: set[str] | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    candidates = [
        row
        for row in leaderboard_state.get("candidate_wallets") or []
        if (
            isinstance(row, dict)
            and row.get("address")
            and (
                candidate_allowlist is None
                or _norm_wallet(row.get("address")) in candidate_allowlist
            )
        )
    ]
    evidence = _wallet_evidence_by_address(profit_state)
    measurement_evidence = _measurement_evidence_by_address(measurement_state or {})
    policy_states = _measurement_policy_states(measurement_state, comparison_measurement_states)
    policy_measurements = _policy_measurements_by_address(policy_states)
    gate_addresses = set(evidence) | set(measurement_evidence) | set(policy_measurements)
    paper_policy_gates = {
        address: _paper_policy_gate(
            policy_measurements.get(address, {}),
            {**evidence.get(address, {}), **measurement_evidence.get(address, {})},
        )
        for address in gate_addresses
    }
    paper_eligible_addresses = {
        address
        for address, gate in paper_policy_gates.items()
        if gate.get("eligible")
    }
    execution_profile_state = execution_profiles_state if isinstance(execution_profiles_state, dict) else {}
    execution_profiles = (
        execution_profile_state.get("profiles_by_wallet")
        if isinstance(execution_profile_state.get("profiles_by_wallet"), dict)
        else {}
    )
    execution_profile_gate_enabled = bool(execution_profile_state)
    execution_profile_summary = {
        "enabled": execution_profile_gate_enabled,
        "status": execution_profile_state.get("status") if execution_profile_gate_enabled else "DISABLED",
        "flow_stage": execution_profile_state.get("flow_stage") if execution_profile_gate_enabled else "",
        "latency_horizon_s": execution_profile_state.get("latency_horizon_s"),
        "profile_count": int(execution_profile_state.get("profile_count") or 0),
        "eligible_profile_count": int(execution_profile_state.get("eligible_profile_count") or 0),
        "next_action": execution_profile_state.get("next_action") if execution_profile_gate_enabled else "",
        "blockers": (
            execution_profile_state.get("blockers")
            if isinstance(execution_profile_state.get("blockers"), list)
            else []
        ),
    }
    activity_profiles: dict[str, dict[str, Any]] = {}
    activity_counts: dict[str, int] = {}
    for wallet, raw_profile in (recent_activity_counts or {}).items():
        address = _norm_wallet(wallet)
        if not address:
            continue
        if isinstance(raw_profile, dict):
            profile = {**_empty_activity_profile(), **raw_profile}
            _finalize_activity_profile(profile)
            events = int(profile.get("events") or 0)
            if events <= 0:
                continue
            activity_profiles[address] = profile
            activity_counts[address] = events
        else:
            events = int(raw_profile or 0)
            if events <= 0:
                continue
            profile = _empty_activity_profile()
            profile["events"] = events
            _finalize_activity_profile(profile)
            activity_profiles[address] = profile
            activity_counts[address] = events
    active_candidate_wallets = {
        str(row.get("address") or "").lower()
        for row in candidates
        if activity_counts.get(str(row.get("address") or "").lower(), 0) > 0
    }
    copy_sized_active_candidate_wallets = {
        str(row.get("address") or "").lower()
        for row in candidates
        if int(activity_profiles.get(str(row.get("address") or "").lower(), {}).get("copy_sized_buy_events") or 0) > 0
    }
    liquid_copy_sized_active_candidate_wallets = {
        str(row.get("address") or "").lower()
        for row in candidates
        if int(activity_profiles.get(str(row.get("address") or "").lower(), {}).get("liquid_copy_sized_buy_events") or 0) > 0
    }
    if active_candidate_wallets:
        has_copy_sized_activity = bool(copy_sized_active_candidate_wallets)
        liquidity_enabled = float(min_ask_depth_usd or 0.0) > 0.0
        has_liquid_copy_sized_activity = bool(liquid_copy_sized_active_candidate_wallets)
        ranked_candidates = sorted(
            candidates,
            key=lambda row: (
                0 if str(row.get("address") or "").lower() in active_candidate_wallets else 1,
                _execution_profile_rank(
                    _norm_wallet(row.get("address")),
                    execution_profiles,
                    enabled=execution_profile_gate_enabled,
                ),
                (
                    0 if str(row.get("address") or "").lower() in liquid_copy_sized_active_candidate_wallets else 1
                )
                if liquidity_enabled
                else 0,
                -int(
                    activity_profiles.get(str(row.get("address") or "").lower(), {}).get("liquid_copy_sized_buy_events")
                    or 0
                )
                if liquidity_enabled
                else 0,
                -num(activity_profiles.get(str(row.get("address") or "").lower(), {}).get("max_recent_ask_depth_usd"))
                if liquidity_enabled
                else 0,
                0 if str(row.get("address") or "").lower() in copy_sized_active_candidate_wallets else 1,
                -int(
                    activity_profiles.get(str(row.get("address") or "").lower(), {}).get("copy_sized_buy_events")
                    or 0
                ),
                -int(
                    activity_profiles.get(str(row.get("address") or "").lower(), {}).get(
                        "copy_sized_buy_hour_of_week_count"
                    )
                    or 0
                ),
                -int(activity_profiles.get(str(row.get("address") or "").lower(), {}).get("buy_events") or 0),
                -int(
                    activity_profiles.get(str(row.get("address") or "").lower(), {}).get(
                        "active_hour_of_week_count"
                    )
                    or 0
                ),
                -activity_counts.get(str(row.get("address") or "").lower(), 0),
                -_max_period_metric(row, "pnl_by_period"),
                _min_rank(row),
                str(row.get("address") or ""),
            ),
        )
        if liquidity_enabled and has_liquid_copy_sized_activity:
            selection_ranking = (
                "recent_liquid_copy_sized_buy_desc_then_ask_depth_desc_then_recent_copy_sized_buy_desc_then_active_hour_coverage_desc_then_recent_buy_desc_then_recent_polygon_activity_desc_then_leaderboard_pnl_max_desc_then_best_rank"
            )
        else:
            selection_ranking = (
                "recent_copy_sized_buy_desc_then_active_hour_coverage_desc_then_recent_buy_desc_then_recent_polygon_activity_desc_then_leaderboard_pnl_max_desc_then_best_rank"
                if has_copy_sized_activity
                else "recent_polygon_activity_desc_then_active_hour_coverage_desc_then_leaderboard_pnl_max_desc_then_best_rank"
            )
    else:
        ranked_candidates = sorted(
            candidates,
            key=lambda row: (
                _execution_profile_rank(
                    _norm_wallet(row.get("address")),
                    execution_profiles,
                    enabled=execution_profile_gate_enabled,
                ),
                -_max_period_metric(row, "pnl_by_period"),
                _min_rank(row),
                str(row.get("address") or ""),
            ),
        )
        selection_ranking = "leaderboard_pnl_max_desc_then_best_rank"
    if execution_profile_gate_enabled:
        selection_ranking = f"copyability_profile_at_latency_then_{selection_ranking}"
    candidates_by_address = {_norm_wallet(row.get("address")): row for row in candidates}
    selected_source_rows: list[dict[str, Any]] = []
    selected_addresses: set[str] = set()
    liquidity_enabled = float(min_ask_depth_usd or 0.0) > 0.0
    prefer_liquid_candidates = liquidity_enabled and bool(liquid_copy_sized_active_candidate_wallets)
    if measurement_evidence:
        measured_order: list[str] = []
        measurement_rows = (
            measurement_state.get("ranked_wallets")
            if isinstance((measurement_state or {}).get("ranked_wallets"), list)
            else []
        )
        for row in measurement_rows:
            if not isinstance(row, dict):
                continue
            address = _norm_wallet(row.get("wallet"))
            if address and address not in measured_order:
                measured_order.append(address)
        for address in measurement_evidence:
            if address not in measured_order:
                measured_order.append(address)
        for address in measured_order:
            if candidate_allowlist is not None and address not in candidate_allowlist:
                continue
            if address not in paper_eligible_addresses:
                continue
            if len(selected_source_rows) >= max(1, int(limit)):
                break
            candidate_row = candidates_by_address.get(address)
            if not isinstance(candidate_row, dict):
                candidate_row = {
                    "address": address,
                    "categories": [],
                    "user_name": "",
                    "pnl_by_period": {},
                    "vol_by_period": {},
                    "ranks": {},
                    "periods": [],
                }
            selected_source_rows.append(candidate_row)
            selected_addresses.add(address)
    require_liquid_passes = [True, False] if prefer_liquid_candidates else [False]
    for require_liquid in require_liquid_passes:
        for row in ranked_candidates:
            if len(selected_source_rows) >= max(1, int(limit)):
                break
            address = _norm_wallet(row.get("address"))
            if not address or address in selected_addresses:
                continue
            if require_liquid and address not in liquid_copy_sized_active_candidate_wallets:
                continue
            if measurement_evidence and address in measurement_evidence and address not in paper_eligible_addresses:
                continue
            selected_source_rows.append(row)
            selected_addresses.add(address)
    if measurement_evidence:
        for address in measured_order:
            if len(selected_source_rows) >= max(1, int(limit)):
                break
            if candidate_allowlist is not None and address not in candidate_allowlist:
                continue
            if address in selected_addresses:
                continue
            candidate_row = candidates_by_address.get(address)
            if not isinstance(candidate_row, dict):
                candidate_row = {
                    "address": address,
                    "categories": [],
                    "user_name": "",
                    "pnl_by_period": {},
                    "vol_by_period": {},
                    "ranks": {},
                    "periods": [],
                }
            selected_source_rows.append(candidate_row)
            selected_addresses.add(address)
    selected: list[dict[str, Any]] = []
    for rank, row in enumerate(selected_source_rows, start=1):
        address = str(row.get("address") or "").lower()
        wallet_evidence = evidence.get(address, {})
        realtime_evidence = measurement_evidence.get(address, {})
        merged_evidence = {**wallet_evidence, **realtime_evidence} if realtime_evidence else dict(wallet_evidence)
        wallet_policy_measurements = policy_measurements.get(address, {})
        paper_gate = paper_policy_gates.get(address) or _paper_policy_gate(wallet_policy_measurements, merged_evidence)
        activity_profile = activity_profiles.get(address, _empty_activity_profile())
        execution_profile = (
            execution_profiles.get(address)
            if isinstance(execution_profiles.get(address), dict)
            else {}
        )
        blockers = list(wallet_evidence.get("blockers") or [])
        blockers.extend(str(item) for item in (realtime_evidence.get("blockers") or []))
        if merged_evidence.get("paper_pnl_usd") is None:
            blockers.append("paper_pnl_at_our_copy_prices_missing")
        if merged_evidence.get("copyable_rate_pct") is None:
            blockers.append("copyable_rate_at_our_latency_missing")
        if paper_gate.get("assessed") and not paper_gate.get("eligible"):
            blockers.append("no_positive_copyable_live_executable_policy")
            if not paper_gate.get("has_positive_paper_pnl"):
                blockers.append("no_positive_paper_pnl_policy")
            if not paper_gate.get("has_copyable_buy"):
                blockers.append("no_copyable_buy_policy")
            if not paper_gate.get("has_buy_sample"):
                blockers.append("realtime_buy_sample_missing")
        liquidity_gate_ok = True
        if float(min_ask_depth_usd or 0.0) > 0.0 and int(activity_profile.get("copy_sized_buy_events") or 0) > 0:
            if int(activity_profile.get("liquid_copy_sized_buy_events") or 0) <= 0:
                blockers.append("top10_liquid_copy_sized_buy_missing")
                liquidity_gate_ok = False
        execution_profile_gate_ok = True
        if execution_profile_gate_enabled:
            execution_profile_gate_ok = bool(execution_profile.get("eligible"))
            if not execution_profile:
                blockers.append("copyability_execution_profile_missing")
            elif not execution_profile_gate_ok:
                blockers.append("copyability_execution_profile_not_positive_at_latency")
                blockers.extend(str(item) for item in (execution_profile.get("blockers") or []))
        live_executable_paper_eligible = bool(paper_gate.get("eligible")) and liquidity_gate_ok and execution_profile_gate_ok
        market_categories = market_categories_from_metadata(row, realtime_evidence, execution_profile)
        selected.append(
            {
                "rank": rank,
                "leaderboard_selection_rank": rank,
                "wallet": address,
                "category": ",".join(row.get("categories") or []),
                "market_categories": market_categories,
                "primary_market_category": market_categories[0] if market_categories else "unknown",
                "user_name": row.get("user_name") or "",
                "leaderboard_pnl_max": round(_max_period_metric(row, "pnl_by_period"), 6),
                "leaderboard_vol_max": round(_max_period_metric(row, "vol_by_period"), 6),
                "leaderboard_best_rank": _min_rank(row),
                "periods": list(row.get("periods") or []),
                "recent_polygon_ws_events": activity_counts.get(address, 0),
                "recent_polygon_ws_buy_events": int(activity_profile.get("buy_events") or 0),
                "recent_polygon_ws_sell_events": int(activity_profile.get("sell_events") or 0),
                "recent_copy_sized_buy_events": int(activity_profile.get("copy_sized_buy_events") or 0),
                "recent_liquid_copy_sized_buy_events": int(activity_profile.get("liquid_copy_sized_buy_events") or 0),
                "active_hour_of_week_count": int(activity_profile.get("active_hour_of_week_count") or 0),
                "buy_hour_of_week_count": int(activity_profile.get("buy_hour_of_week_count") or 0),
                "copy_sized_buy_hour_of_week_count": int(
                    activity_profile.get("copy_sized_buy_hour_of_week_count") or 0
                ),
                "active_hour_of_week_coverage_pct": round(
                    num(activity_profile.get("active_hour_of_week_coverage_pct")),
                    6,
                ),
                "active_hour_of_week_counts": (
                    activity_profile.get("active_hour_of_week_counts")
                    if isinstance(activity_profile.get("active_hour_of_week_counts"), dict)
                    else {}
                ),
                "max_recent_source_buy_usd": round(num(activity_profile.get("max_source_buy_usd")), 6),
                "total_recent_source_buy_usd": round(num(activity_profile.get("total_source_buy_usd")), 6),
                "max_recent_ask_depth_usd": round(num(activity_profile.get("max_recent_ask_depth_usd")), 6),
                "recent_liquidity_checked_assets": int(activity_profile.get("liquidity_checked_assets") or 0),
                "recent_liquidity_eligible_assets": int(activity_profile.get("liquidity_eligible_assets") or 0),
                "recent_liquidity_unchecked_assets": int(activity_profile.get("liquidity_unchecked_assets") or 0),
                "recent_liquidity_reject_reasons": (
                    activity_profile.get("liquidity_reject_reasons")
                    if isinstance(activity_profile.get("liquidity_reject_reasons"), dict)
                    else {}
                ),
                "paper_pnl_usd": merged_evidence.get("paper_pnl_usd"),
                "paper_realized_pnl_usd": merged_evidence.get("paper_realized_pnl_usd"),
                "paper_unrealized_pnl_usd": merged_evidence.get("paper_unrealized_pnl_usd"),
                "copyable_rate_pct": merged_evidence.get("copyable_rate_pct"),
                "paper_orders": merged_evidence.get("paper_orders", 0),
                "paper_resolved_orders": merged_evidence.get("paper_resolved_orders", 0),
                "observed_realtime_events": merged_evidence.get("observed_realtime_events", 0),
                "buy_events": merged_evidence.get("buy_events", 0),
                "sell_events": merged_evidence.get("sell_events", 0),
                "copyable_buy_events": merged_evidence.get("copyable_buy_events", 0),
                "rejected_buy_events": merged_evidence.get("rejected_buy_events", 0),
                "open_positions": merged_evidence.get("open_positions", 0),
                "open_cost_usd": merged_evidence.get("open_cost_usd"),
                "sample_status": merged_evidence.get("sample_status"),
                "reject_reasons": merged_evidence.get("reject_reasons", {}),
                "policy_measurements": wallet_policy_measurements,
                "paper_policy_gate": paper_gate,
                "copyability_profile_gate_enabled": execution_profile_gate_enabled,
                "copyability_profile_eligible": (
                    bool(execution_profile.get("eligible")) if execution_profile_gate_enabled else None
                ),
                "execution_profile": execution_profile,
                "live_executable_paper_eligible": live_executable_paper_eligible,
                "paper_eligible_policy_ids": list(paper_gate.get("eligible_policy_ids") or []),
                "best_policy_id": paper_gate.get("best_policy_id"),
                "best_policy_paper_pnl_usd": paper_gate.get("best_policy_paper_pnl_usd"),
                "best_policy_copyable_rate_pct": paper_gate.get("best_policy_copyable_rate_pct"),
                "best_policy_copyable_buy_events": paper_gate.get("best_policy_copyable_buy_events"),
                "paper_pnl_usd_by_policy": {
                    policy_id: measurement.get("paper_pnl_usd")
                    for policy_id, measurement in wallet_policy_measurements.items()
                },
                "copyable_rate_pct_by_policy": {
                    policy_id: measurement.get("copyable_rate_pct")
                    for policy_id, measurement in wallet_policy_measurements.items()
                },
                "copyable_buy_events_by_policy": {
                    policy_id: measurement.get("copyable_buy_events")
                    for policy_id, measurement in wallet_policy_measurements.items()
                },
                "evidence_status": merged_evidence.get("status") or "MISSING_PAPER_OBSERVE",
                "evidence_source": "polygon_ws_top10_paper_measurement" if realtime_evidence else "profit_engine_or_missing",
                "blockers": sorted(set(blockers)),
            }
        )
    if measurement_evidence:
        selected = sorted(
            selected,
            key=lambda row: (
                0 if row.get("live_executable_paper_eligible") else (
                    2 if (row.get("paper_policy_gate") or {}).get("assessed") else 1
                ),
                -num(row.get("best_policy_paper_pnl_usd"), num(row.get("paper_pnl_usd"), -1_000_000_000.0)),
                -num(row.get("best_policy_copyable_rate_pct"), num(row.get("copyable_rate_pct"), -1.0)),
                -int(row.get("recent_liquid_copy_sized_buy_events") or 0),
                -num(row.get("max_recent_ask_depth_usd")),
                -int(row.get("recent_copy_sized_buy_events") or 0),
                -int(row.get("buy_events") or 0),
                int(row.get("leaderboard_selection_rank") or 999999),
                str(row.get("wallet") or ""),
            ),
        )
        for observed_rank, row in enumerate(selected, start=1):
            row["rank"] = observed_rank
    missing_paper = sum(
        1
        for row in selected
        if row["paper_pnl_usd"] is None or row["copyable_rate_pct"] is None
    )
    sample_missing = sum(
        1
        for row in selected
        if "realtime_buy_sample_missing" in set(row.get("blockers") or [])
        or "realtime_events_missing" in set(row.get("blockers") or [])
    )
    live_orders = int(live_fill_report.get("orders") or 0)
    live_filled = int(live_fill_report.get("filled") or 0)
    live_rejected = int(live_fill_report.get("rejected") or 0)
    eligible_selected = sum(1 for row in selected if bool(row.get("live_executable_paper_eligible")))
    failed_policy_selected = sum(
        1
        for row in selected
        if (row.get("paper_policy_gate") or {}).get("assessed") and not row.get("live_executable_paper_eligible")
    )
    profile_blocked_selected = sum(
        1
        for row in selected
        if execution_profile_gate_enabled and row.get("copyability_profile_eligible") is not True
    )
    execution_profile_blockers = [
        str(item)
        for item in (execution_profile_summary.get("blockers") or [])
        if str(item or "")
    ]
    status = "WATCH" if selected else "CORRECTION"
    if missing_paper or sample_missing:
        status = "ANALYZE"
    elif measurement_evidence and eligible_selected <= 0:
        status = "ANALYZE"
    measurement_summary = {}
    if isinstance(measurement_state, dict):
        measurement_summary = measurement_state.get("summary") if isinstance(measurement_state.get("summary"), dict) else {}
    selected_market_category_summary = summarize_wallet_market_categories(selected)
    return {
        "schema_version": 1,
        "kind": "wallet_copy_top10_broad_paper_lane_state",
        "flow_stage": "OBSERVE",
        "status": status,
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "selection": {
            "source": (
                "bounded_candidate_allowlist"
                if candidate_allowlist is not None
                else "widened_leaderboard_candidate_wallets"
            ),
            "limit": int(limit),
            "candidate_wallets": len(candidates),
            "active_candidate_wallets": len(active_candidate_wallets),
            "copy_sized_active_candidate_wallets": len(copy_sized_active_candidate_wallets),
            "liquid_copy_sized_active_candidate_wallets": len(liquid_copy_sized_active_candidate_wallets),
            "candidate_market_category_counts": _candidate_market_category_counts(candidates),
            "selected_market_category_counts": {
                category: int(row.get("selected_wallets") or 0)
                for category, row in selected_market_category_summary.items()
            },
            "selected_market_category_summary": selected_market_category_summary,
            "paper_positive_copyable_selected_wallets": eligible_selected,
            "paper_failed_measured_selected_wallets": failed_policy_selected,
            "copyability_profile_blocked_selected_wallets": profile_blocked_selected,
            "paper_positive_copyable_measured_wallets": len(paper_eligible_addresses),
            "min_source_buy_usd": round(float(min_source_buy_usd), 6),
            "min_top_of_book_ask_depth_usd": round(float(min_ask_depth_usd or 0.0), 6),
            "selected_wallets": len(selected),
            "preselection_ranking": selection_ranking,
            "ranking": (
                f"paper_pnl_desc_then_copyable_rate_desc_then_buy_sample_then_leaderboard_after_{selection_ranking}"
                if measurement_evidence
                else selection_ranking
            ),
            "liquidity_filter": liquidity_summary if isinstance(liquidity_summary, dict) else {"enabled": False},
            "copyability_profile_gate": execution_profile_summary,
        },
        "paper_measurement": {
            "status": (measurement_state or {}).get("status") if isinstance(measurement_state, dict) else "MISSING",
            "summary": measurement_summary,
            "policy_summaries": _policy_summaries(policy_states),
        },
        "live_baseline": {
            "orders": live_orders,
            "filled": live_filled,
            "rejected": live_rejected,
            "fill_rate_pct": live_fill_report.get("fill_rate_pct"),
        },
        "ranked_wallets": selected,
        "blockers": sorted(
            set(
                (["top10_parallel_paper_copy_metrics_missing"] if missing_paper else [])
                + (["top10_realtime_buy_sample_missing"] if sample_missing else [])
                + (
                    ["top10_no_positive_copyable_live_executable_policy"]
                    if measurement_evidence and eligible_selected <= 0 and not missing_paper and not sample_missing
                    else []
                )
                + (
                    ["top10_copyability_execution_profile_missing_or_not_positive"]
                    if execution_profile_gate_enabled and profile_blocked_selected > 0
                    else []
                )
                + [f"top10_{blocker}" for blocker in execution_profile_blockers]
            )
        ),
        "next_action": (
            str(execution_profile_summary.get("next_action") or "")
            if execution_profile_blockers and execution_profile_summary.get("next_action")
            else "capture CLOB book coverage and rebuild alpha-decay execution profiles before promotion"
            if execution_profile_gate_enabled
            and profile_blocked_selected > 0
            else
            "run realtime paper-copy measurement for selected wallets until each has paper PnL at our copy prices and copyable_rate"
            if missing_paper or sample_missing
            else "reject this measured cohort for rotation and broaden/retune selector toward positive liquid copyability"
            if measurement_evidence and eligible_selected <= 0
            else "compare positive, liquid, live-executable paper policies against the live wallet for rotation"
        ),
    }


def main() -> int:
    args = parse_args()
    alpha_decay_report = load_json(args.alpha_decay_report, default={}) if str(args.alpha_decay_report or "").strip() else {}
    if str(args.alpha_decay_report or "").strip():
        require_fresh_alpha_report(alpha_decay_report, path=args.alpha_decay_report)
    sizing_defaults = _mission_sizing_defaults()
    min_source_buy_usd = (
        float(args.min_source_buy_usd)
        if float(args.min_source_buy_usd or 0.0) > 0
        else sizing_defaults["min_source_buy_usd"]
    )
    recent_activity_counts = (
        {}
        if args.no_prefer_active
        else load_recent_activity_profiles(
            args.activity_jsonl,
            limit=int(args.activity_scan_limit),
            min_source_buy_usd=min_source_buy_usd,
        )
    )
    liquidity_summary: dict[str, Any] = {"enabled": False}
    min_ask_depth_usd = float(args.min_ask_depth_usd or 0.0)
    if min_ask_depth_usd > 0.0 and recent_activity_counts:
        prior_clob_override = os.environ.get("POLYMARKET_CLOB_API_BASE_URL")
        if bool(args.liquidity_disable_source_base_overrides):
            os.environ["POLYMARKET_CLOB_API_BASE_URL"] = ""
        try:
            recent_activity_counts, liquidity_summary = apply_liquidity_depth_filter(
                recent_activity_counts,
                clob=CLOBMarketClient(
                    host=str(args.liquidity_clob_base_url),
                    timeout_s=float(args.liquidity_clob_timeout_s),
                    retries=int(args.liquidity_clob_retries),
                ),
                min_ask_depth_usd=min_ask_depth_usd,
                max_assets=int(args.liquidity_max_assets),
            )
        finally:
            if bool(args.liquidity_disable_source_base_overrides):
                if prior_clob_override is None:
                    os.environ.pop("POLYMARKET_CLOB_API_BASE_URL", None)
                else:
                    os.environ["POLYMARKET_CLOB_API_BASE_URL"] = prior_clob_override
        liquidity_summary["source_base_overrides_disabled"] = bool(args.liquidity_disable_source_base_overrides)
    comparison_measurement_states = [
        load_json(path, default={})
        for path in (args.comparison_measurement_state or [])
        if str(path or "").strip()
    ]
    candidate_allowlist: set[str] | None = None
    if str(args.candidate_allowlist_state or "").strip():
        allowlist_state = load_json(args.candidate_allowlist_state, default={})
        candidate_allowlist = {
            wallet
            for row in allowlist_state.get("ranked_queue") or []
            if (
                isinstance(row, dict)
                and str(row.get("admission_status") or "") == str(args.candidate_allowlist_status)
                and (wallet := _norm_wallet(row.get("wallet")))
            )
        }
    execution_profiles_state: dict[str, Any] = {}
    if isinstance(alpha_decay_report, dict) and alpha_decay_report:
        existing_profiles = (
            alpha_decay_report.get("execution_profiles")
            if isinstance(alpha_decay_report.get("execution_profiles"), dict)
            else {}
        )
        execution_profiles_state = existing_profiles or build_execution_profiles(
            alpha_decay_report,
            config=ExecutionProfileConfig(
                latency_horizon_s=float(args.alpha_profile_horizon_s),
                min_fills=int(args.alpha_profile_min_fills),
                min_positive_edge_fraction=float(args.alpha_profile_min_positive_edge_fraction),
                min_mean_edge=float(args.alpha_profile_min_mean_edge),
                min_median_edge=float(args.alpha_profile_min_median_edge),
                max_observation_lag_s=float(args.alpha_profile_max_observation_lag_s),
            ),
        )
    state = build_state(
        leaderboard_state=load_json(args.leaderboard_state, default={}),
        profit_state=load_json(args.profit_state, default={}),
        live_fill_report=load_json(args.live_fill_report, default={}),
        measurement_state=load_json(args.measurement_state, default={}),
        comparison_measurement_states=[
            state for state in comparison_measurement_states if isinstance(state, dict)
        ],
        execution_profiles_state=execution_profiles_state,
        recent_activity_counts=recent_activity_counts,
        min_source_buy_usd=min_source_buy_usd,
        min_ask_depth_usd=min_ask_depth_usd,
        liquidity_summary=liquidity_summary,
        candidate_allowlist=candidate_allowlist,
        limit=int(args.limit),
    )
    atomic_write_json(args.output, state)
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0 if state["status"] in {"WATCH", "ANALYZE"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
