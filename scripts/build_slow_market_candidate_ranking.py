#!/usr/bin/env python3
"""Build the slow-market wallet-copy candidate ranking.

The output is paper/research only.  It converts the broad leaderboard registry
into a slow-market lane state that the existing broad paper measurement runner
can consume without touching the live guard.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.market_categories import market_categories_from_metadata  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_LEADERBOARD = "data/research/wallet_copy_leaderboard_crypto_state.json"
DEFAULT_RTDS = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_OUTPUT = "data/research/slow_market_candidate_ranking_20260705.json"
DEFAULT_LANE = "data/research/slow_market_paper_lane_state.json"
DEFAULT_EXCLUDED_WALLETS = "data/research/slow_market_abandoned_wallets.json"

SLOW_CATEGORY_MAP = {
    "BUSINESS": "events",
    "ECONOMICS": "events",
    "ECONOMY": "events",
    "EVENTS": "events",
    "POP_CULTURE": "events",
    "SCIENCE": "events",
    "SPORTS": "sports",
    "POLITICS": "politics",
}
SLOW_PRIMARY_ORDER = {"sports": 0, "politics": 1, "events": 2, "unknown": 3}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leaderboard-state", default=DEFAULT_LEADERBOARD)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--lane-output", default=DEFAULT_LANE)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--tail-bytes", type=int, default=256_000_000)
    parser.add_argument("--max-rows", type=int, default=250_000)
    parser.add_argument("--min-week-pnl", type=float, default=0.0)
    parser.add_argument("--min-month-pnl", type=float, default=0.0)
    parser.add_argument("--excluded-wallets-state", default=DEFAULT_EXCLUDED_WALLETS)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _load_excluded_wallets(path: str | Path) -> tuple[set[str], dict[str, Any]]:
    payload = load_json(path, default={})
    payload = payload if isinstance(payload, dict) else {}
    wallets: set[str] = set()
    for row in payload.get("wallets") or []:
        wallet = _norm_wallet(row.get("wallet") if isinstance(row, dict) else row)
        if wallet:
            wallets.add(wallet)
    return wallets, payload


def _max_period_metric(row: dict[str, Any], key: str) -> float:
    values = row.get(key) if isinstance(row.get(key), dict) else {}
    out = 0.0
    for value in values.values():
        try:
            out = max(out, float(value or 0.0))
        except (TypeError, ValueError):
            continue
    return out


def _period_metric(row: dict[str, Any], key: str, period: str) -> float:
    values = row.get(key) if isinstance(row.get(key), dict) else {}
    try:
        return float(values.get(period) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _min_rank(row: dict[str, Any]) -> int:
    ranks = row.get("ranks") if isinstance(row.get("ranks"), dict) else {}
    found: list[int] = []
    for value in ranks.values():
        try:
            found.append(int(value))
        except (TypeError, ValueError):
            continue
    return min(found) if found else 999999


def _slow_categories(row: dict[str, Any]) -> list[str]:
    categories: list[str] = []
    for value in row.get("categories") or []:
        mapped = SLOW_CATEGORY_MAP.get(str(value or "").strip().upper())
        if mapped and mapped not in categories:
            categories.append(mapped)
    inferred = market_categories_from_metadata(row)
    for category in inferred:
        if category in {"sports", "politics", "events"} and category not in categories:
            categories.append(category)
    return categories


def _is_btc_or_crypto_fast(row: dict[str, Any], slow_categories: list[str]) -> bool:
    tags = {str(tag or "").strip().lower() for tag in (row.get("tags") or [])}
    categories = {str(value or "").strip().upper() for value in (row.get("categories") or [])}
    inferred = set(market_categories_from_metadata(row))
    if "btc_5m" in tags or "btc_5m" in inferred:
        return True
    return "CRYPTO" in categories and not slow_categories


def _hour_of_week(ts: Any) -> str:
    try:
        value = float(ts)
    except (TypeError, ValueError):
        return ""
    if value <= 0:
        return ""
    if value > 10_000_000_000:
        value /= 1000.0
    dt = datetime.fromtimestamp(value, tz=timezone.utc)
    return f"{dt.weekday() * 24 + dt.hour:03d}"


def _slug_category(row: dict[str, Any]) -> str:
    slug = str(row.get("market_slug") or "")
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    text = " ".join(
        str(item or "")
        for item in (
            slug,
            raw.get("slug"),
            raw.get("eventSlug"),
            raw.get("title"),
        )
    ).lower()
    if text.startswith("btc-updown-5m-") or ("btc" in text and ("5m" in text or "5 minutes" in text)):
        return "btc_5m"
    sports_prefixes = (
        "afl-",
        "cs2-",
        "dota-",
        "epl-",
        "fifa-",
        "lol-",
        "mlb-",
        "mma-",
        "nba-",
        "nfl-",
        "nhl-",
        "soccer-",
        "tennis-",
        "ufc-",
        "val-",
        "valorant-",
        "wnba-",
    )
    if slug.lower().startswith(sports_prefixes) or any(
        word in text
        for word in (
            " vs ",
            " winner",
            "map 1",
            "game 1",
            "match",
            "league",
            "tournament",
            "mlb",
            "nba",
            "wnba",
            "nfl",
            "nhl",
            "ufc",
            "valorant",
        )
    ):
        return "sports"
    if any(
        word in text
        for word in (
            "trump",
            "biden",
            "election",
            "senate",
            "congress",
            "president",
            "mayor",
            "governor",
            "politic",
            "white house",
        )
    ):
        return "politics"
    return "events" if text.strip() else "unknown"


def _empty_activity() -> dict[str, Any]:
    return {
        "events": 0,
        "buy_events": 0,
        "sell_events": 0,
        "source_buy_usd": 0.0,
        "max_source_buy_usd": 0.0,
        "active_hour_of_week_counts": {},
        "buy_hour_of_week_counts": {},
        "market_category_counts": {},
        "latest_event_ts": None,
        "latest_received_at_s": None,
        "latest_market_slug": "",
    }


def _read_recent_rtds(path: Path, *, tail_bytes: int, max_rows: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    size = path.stat().st_size
    with path.open("rb") as handle:
        handle.seek(max(0, size - max(1, int(tail_bytes))))
        raw = handle.read()
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines()[-max(1, int(max_rows)) :]:
        try:
            row = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(row, dict) and row.get("event") == "rtds_trade_event":
            rows.append(row)
    return rows


def _activity_by_wallet(rows: list[dict[str, Any]], candidate_wallets: set[str]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    activity: dict[str, dict[str, Any]] = {}
    category_counts: Counter[str] = Counter()
    slow_rows = 0
    registry_rows = 0
    for row in rows:
        category = _slug_category(row)
        category_counts[category] += 1
        if category in {"sports", "politics", "events"}:
            slow_rows += 1
        wallet = _norm_wallet(row.get("source_wallet") or (row.get("raw") or {}).get("proxyWallet"))
        if wallet not in candidate_wallets:
            continue
        registry_rows += 1
        item = activity.setdefault(wallet, _empty_activity())
        item["events"] = int(item.get("events") or 0) + 1
        hour = _hour_of_week(row.get("event_ts") or row.get("received_at_s"))
        if hour:
            counts = item.setdefault("active_hour_of_week_counts", {})
            counts[hour] = int(counts.get(hour) or 0) + 1
        market_counts = item.setdefault("market_category_counts", {})
        market_counts[category] = int(market_counts.get(category) or 0) + 1
        item["latest_event_ts"] = row.get("event_ts")
        item["latest_received_at_s"] = row.get("received_at_s")
        item["latest_market_slug"] = row.get("market_slug") or (row.get("raw") or {}).get("slug") or ""
        if str(row.get("side") or "").upper() == "BUY":
            item["buy_events"] = int(item.get("buy_events") or 0) + 1
            if hour:
                counts = item.setdefault("buy_hour_of_week_counts", {})
                counts[hour] = int(counts.get(hour) or 0) + 1
            buy_usd = max(0.0, num(row.get("price")) * num(row.get("size")))
            item["source_buy_usd"] = round(num(item.get("source_buy_usd")) + buy_usd, 6)
            item["max_source_buy_usd"] = round(max(num(item.get("max_source_buy_usd")), buy_usd), 6)
        elif str(row.get("side") or "").upper() == "SELL":
            item["sell_events"] = int(item.get("sell_events") or 0) + 1

    for item in activity.values():
        active_hours = item.get("active_hour_of_week_counts") if isinstance(item.get("active_hour_of_week_counts"), dict) else {}
        buy_hours = item.get("buy_hour_of_week_counts") if isinstance(item.get("buy_hour_of_week_counts"), dict) else {}
        item["active_hour_of_week_count"] = len(active_hours)
        item["buy_hour_of_week_count"] = len(buy_hours)
        item["active_hour_of_week_coverage_pct"] = round(len(active_hours) / 168.0 * 100.0, 6)
    return activity, {
        "rows_scanned": len(rows),
        "slow_market_rows": slow_rows,
        "registry_candidate_rows": registry_rows,
        "market_category_counts": dict(sorted(category_counts.items())),
    }


def build_state(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    leaderboard = load_json(args.leaderboard_state, default={})
    candidates = [row for row in leaderboard.get("candidate_wallets") or [] if isinstance(row, dict)]
    excluded_wallets, exclusion_state = _load_excluded_wallets(args.excluded_wallets_state)
    slow_rows: list[dict[str, Any]] = []
    excluded_counts: Counter[str] = Counter()
    for row in candidates:
        wallet = _norm_wallet(row.get("address"))
        if not wallet:
            excluded_counts["missing_wallet"] += 1
            continue
        if wallet in excluded_wallets:
            excluded_counts["abandoned_selection"] += 1
            continue
        slow_categories = _slow_categories(row)
        if _is_btc_or_crypto_fast(row, slow_categories):
            excluded_counts["btc5m_or_crypto_fast"] += 1
            continue
        if not slow_categories:
            excluded_counts["no_slow_category"] += 1
            continue
        week_pnl = _period_metric(row, "pnl_by_period", "WEEK")
        month_pnl = _period_metric(row, "pnl_by_period", "MONTH")
        if week_pnl < float(args.min_week_pnl) and month_pnl < float(args.min_month_pnl):
            excluded_counts["pnl_below_floor"] += 1
            continue
        slow_rows.append({**row, "_wallet": wallet, "_slow_categories": slow_categories})

    rtds_rows = _read_recent_rtds(
        Path(args.rtds_jsonl),
        tail_bytes=int(args.tail_bytes),
        max_rows=int(args.max_rows),
    )
    activity, activity_summary = _activity_by_wallet(
        rtds_rows,
        {_norm_wallet(row.get("address")) for row in slow_rows},
    )

    ranked: list[dict[str, Any]] = []
    for row in slow_rows:
        wallet = str(row["_wallet"])
        slow_categories = list(row["_slow_categories"])
        primary = slow_categories[0] if slow_categories else "unknown"
        wallet_activity = activity.get(wallet, _empty_activity())
        ranked.append(
            {
                "wallet": wallet,
                "name": row.get("name") or "",
                "user_name": row.get("user_name") or "",
                "category": ",".join(row.get("categories") or []),
                "market_categories": slow_categories,
                "primary_market_category": primary,
                "leaderboard_pnl_max": round(_max_period_metric(row, "pnl_by_period"), 6),
                "leaderboard_pnl_week": round(_period_metric(row, "pnl_by_period", "WEEK"), 6),
                "leaderboard_pnl_month": round(_period_metric(row, "pnl_by_period", "MONTH"), 6),
                "leaderboard_vol_max": round(_max_period_metric(row, "vol_by_period"), 6),
                "leaderboard_best_rank": _min_rank(row),
                "periods": list(row.get("periods") or []),
                "recent_rtds_events": int(wallet_activity.get("events") or 0),
                "recent_rtds_buy_events": int(wallet_activity.get("buy_events") or 0),
                "recent_rtds_sell_events": int(wallet_activity.get("sell_events") or 0),
                "active_hour_of_week_count": int(wallet_activity.get("active_hour_of_week_count") or 0),
                "buy_hour_of_week_count": int(wallet_activity.get("buy_hour_of_week_count") or 0),
                "active_hour_of_week_coverage_pct": round(num(wallet_activity.get("active_hour_of_week_coverage_pct")), 6),
                "source_buy_usd": round(num(wallet_activity.get("source_buy_usd")), 6),
                "max_source_buy_usd": round(num(wallet_activity.get("max_source_buy_usd")), 6),
                "active_hour_of_week_counts": wallet_activity.get("active_hour_of_week_counts")
                if isinstance(wallet_activity.get("active_hour_of_week_counts"), dict)
                else {},
                "buy_hour_of_week_counts": wallet_activity.get("buy_hour_of_week_counts")
                if isinstance(wallet_activity.get("buy_hour_of_week_counts"), dict)
                else {},
                "recent_market_category_counts": wallet_activity.get("market_category_counts")
                if isinstance(wallet_activity.get("market_category_counts"), dict)
                else {},
                "latest_event_ts": wallet_activity.get("latest_event_ts"),
                "latest_received_at_s": wallet_activity.get("latest_received_at_s"),
                "latest_market_slug": wallet_activity.get("latest_market_slug") or "",
                "paper_pnl_usd": None,
                "copyable_rate_pct": None,
                "paper_orders": 0,
                "paper_resolved_orders": 0,
                "live_executable_paper_eligible": False,
                "evidence_status": "SLOW_MARKET_RANKED_NOT_MEASURED",
                "blockers": ["slow_market_paper_measurement_missing"],
            }
        )

    ranked.sort(
        key=lambda row: (
            0 if int(row.get("recent_rtds_buy_events") or 0) > 0 else 1,
            -int(row.get("recent_rtds_buy_events") or 0),
            -int(row.get("buy_hour_of_week_count") or 0),
            -int(row.get("recent_rtds_events") or 0),
            SLOW_PRIMARY_ORDER.get(str(row.get("primary_market_category") or "unknown"), 9),
            -num(row.get("leaderboard_pnl_week")),
            -num(row.get("leaderboard_pnl_month")),
            -num(row.get("leaderboard_pnl_max")),
            int(row.get("leaderboard_best_rank") or 999999),
            str(row.get("wallet") or ""),
        )
    )

    selected = ranked[: max(1, int(args.limit))]
    for index, row in enumerate(selected, start=1):
        row["rank"] = index
        row["leaderboard_selection_rank"] = index

    selected_counts = Counter(str(row.get("primary_market_category") or "unknown") for row in selected)
    status = "WATCH" if selected else "ESCALATE"
    state = {
        "schema_version": 1,
        "kind": "wallet_copy_slow_market_candidate_ranking",
        "flow_stage": "DISCOVER/OBSERVE/PROMOTE",
        "status": status,
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "inputs": {
            "leaderboard_state": str(args.leaderboard_state),
            "rtds_jsonl": str(args.rtds_jsonl),
            "tail_bytes": int(args.tail_bytes),
            "max_rows": int(args.max_rows),
            "limit": int(args.limit),
            "min_week_pnl": float(args.min_week_pnl),
            "min_month_pnl": float(args.min_month_pnl),
            "excluded_wallets_state": str(args.excluded_wallets_state),
            "excluded_wallets": len(excluded_wallets),
        },
        "summary": {
            "leaderboard_candidates": len(candidates),
            "slow_candidate_pool": len(slow_rows),
            "abandoned_wallets_excluded": len(excluded_wallets),
            "abandoned_selection_updated_at": exclusion_state.get("updated_at"),
            "selected_count": len(selected),
            "selected_market_category_counts": dict(sorted(selected_counts.items())),
            "selected_with_recent_buy_events": sum(1 for row in selected if int(row.get("recent_rtds_buy_events") or 0) > 0),
            "selected_recent_buy_events": sum(int(row.get("recent_rtds_buy_events") or 0) for row in selected),
            "selected_recent_events": sum(int(row.get("recent_rtds_events") or 0) for row in selected),
            "excluded_counts": dict(sorted(excluded_counts.items())),
            "activity": activity_summary,
        },
        "ranking": {
            "rule": (
                "recent_slow_market_buy_events_desc_then_buy_hour_coverage_desc_then_recent_events_desc_"
                "then_category_priority_then_week_month_max_pnl_desc"
            ),
            "live_ready_next": (
                "run paper measurement for this lane with the existing runner; promote only after >=50 resolved "
                "paper fills positive at our prices per OP-TARGET lane-promotion gate"
            ),
        },
        "ranked_wallets": selected,
        "top_unselected": ranked[max(1, int(args.limit)) : max(1, int(args.limit)) + 20],
        "next_action": "start slow-market paper qualification on selected wallets; live path remains untouched until the evidence gate passes",
    }
    lane_state = {
        "schema_version": 1,
        "kind": "wallet_copy_slow_market_paper_lane_state",
        "flow_stage": "OBSERVE/PROMOTE",
        "status": "WATCH" if selected else "ESCALATE",
        "updated_at": state["updated_at"],
        "paper_only": True,
        "live_orders_allowed": False,
        "selection": {
            "source": "slow_market_candidate_ranking",
            "ranking_output": str(args.output),
            "candidate_wallets": len(slow_rows),
            "selected_wallets": len(selected),
            "selected_market_category_counts": dict(sorted(selected_counts.items())),
            "ranking": state["ranking"]["rule"],
        },
        "ranked_wallets": selected,
        "blockers": ["slow_market_paper_measurement_missing"] if selected else ["slow_market_candidates_missing"],
        "next_action": state["next_action"],
    }
    return state, lane_state


def main() -> int:
    args = parse_args()
    state, lane_state = build_state(args)
    atomic_write_json(args.output, state)
    atomic_write_json(args.lane_output, lane_state)
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0 if state["status"] == "WATCH" else 2


if __name__ == "__main__":
    raise SystemExit(main())
