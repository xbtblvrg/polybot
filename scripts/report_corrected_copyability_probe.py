#!/usr/bin/env python3
"""Corrected local-feed copyability probe for rotation decisions.

Flow stage: ROTATE/LEARN/LIVE. This report uses wallet-copy local feed rows
instead of raw Data API last-trade probes, because raw endpoint age checks have
already been falsified by live copied fills.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/wallet_copy_corrected_copyability_probe_latest.json"
DEFAULT_WALLET_EVENTS = "data/research/wallet_copy_live_guard_wallet_events.jsonl"
DEFAULT_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_REGISTRY = "configs/wallet_copy/wallets.json"
DEFAULT_LEADERBOARD = "data/research/wallet_copy_leaderboard_crypto_state.json"
DEFAULT_FLEET = "data/research/btc5m_live_paper_fleet_latest.json"
DEFAULT_MORNING = "data/research/btc5m_morning_ranked_table_latest.json"
DEFAULT_FOLLOWABILITY = "data/research/wallet_copy_followability_leaderboard_latest.json"
DEFAULT_ACTIVE_SET = "data/research/wallet_copy_live_guard_state.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet-events", default=DEFAULT_WALLET_EVENTS)
    parser.add_argument("--queue", default=DEFAULT_QUEUE)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--leaderboard", default=DEFAULT_LEADERBOARD)
    parser.add_argument("--fleet", default=DEFAULT_FLEET)
    parser.add_argument("--morning", default=DEFAULT_MORNING)
    parser.add_argument("--followability", default=DEFAULT_FOLLOWABILITY)
    parser.add_argument("--active-set", default=DEFAULT_ACTIVE_SET)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--lookback-hours", type=float, default=24.0)
    parser.add_argument(
        "--tail-lines",
        type=int,
        default=50_000,
        help="Read this many recent local-feed rows with tail. Use <=0 for full file scan.",
    )
    parser.add_argument("--max-promotions", type=int, default=2)
    parser.add_argument("--fresh-hours", type=float, default=24.0)
    parser.add_argument("--median-entry-threshold-s", type=float, default=60.0)
    parser.add_argument("--inband-min-share-pct", type=float, default=50.0)
    parser.add_argument("--inband-min-price", type=float, default=0.25)
    parser.add_argument("--inband-max-price", type=float, default=0.50)
    parser.add_argument(
        "--min-btc5m-buys",
        type=int,
        default=20,
        help="P1 local-feed sample floor (Fable 2026-07-09T02:15Z).",
    )
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _add_source(index: dict[str, dict[str, Any]], wallet: str, source: str, evidence: dict[str, Any]) -> None:
    wallet = _norm_wallet(wallet)
    if not wallet:
        return
    row = index.setdefault(wallet, {"wallet": wallet, "sources": [], "evidence": {}})
    if source not in row["sources"]:
        row["sources"].append(source)
    row["evidence"][source] = evidence


def _candidate_index(args: argparse.Namespace) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}

    queue = load_json(args.queue, default={})
    for row in _as_list(queue.get("ranked_members")):
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet:
            continue
        _add_source(
            index,
            wallet,
            "member_queue",
            {
                "queue_rank": row.get("queue_rank"),
                "ready_for_live": bool(row.get("ready_for_live")),
                "queue_source": row.get("queue_source"),
                "paper_pnl_usd": row.get("resolved_pnl"),
                "copyable_buy_events": (row.get("replay") or {}).get("copyable_buy_events")
                if isinstance(row.get("replay"), dict)
                else None,
                "candidate_id": row.get("name") or (row.get("replay") or {}).get("candidate_id"),
            },
        )

    fleet = load_json(args.fleet, default={})
    for row in _as_list(fleet.get("fleet")):
        if not isinstance(row, dict):
            continue
        _add_source(
            index,
            _norm_wallet(row.get("wallet")),
            "btc5m_fleet",
            {
                "fleet_rank": row.get("fleet_rank"),
                "admission_status": row.get("admission_status"),
                "paper_pnl_usd": row.get("paper_pnl_usd"),
                "copyable_buy_events": row.get("copyable_buy_events"),
                "candidate_clob_backed_orders": row.get("candidate_clob_backed_orders"),
                "holdout_window_evidence": row.get("holdout_window_evidence"),
            },
        )

    morning = load_json(args.morning, default={})
    for row in _as_list(morning.get("ranked_rows")):
        if not isinstance(row, dict) or str(row.get("family") or "") != "copy":
            continue
        _add_source(
            index,
            _norm_wallet(row.get("candidate_id")),
            "btc5m_morning_table",
            {
                "rank": row.get("rank"),
                "status": row.get("status"),
                "notes": row.get("notes"),
                "oos_pnl_usd": row.get("oos_pnl_usd"),
                "oos_trades": row.get("oos_trades"),
                "holdout_passed": row.get("holdout_passed"),
                "evidence_pointer": row.get("evidence_pointer"),
            },
        )

    followability = load_json(args.followability, default={})
    for row in _as_list(followability.get("leaderboard")):
        if not isinstance(row, dict):
            continue
        _add_source(
            index,
            _norm_wallet(row.get("wallet")),
            "followability",
            {
                "followability_score": row.get("followability_score"),
                "eligible_windows": row.get("eligible_windows"),
                "early_win_rate_pct": row.get("early_win_rate_pct"),
                "early_side_predictiveness_pct": row.get("early_side_predictiveness_pct"),
            },
        )

    leaderboard = load_json(args.leaderboard, default={})
    for row in _as_list(leaderboard.get("top_wallets")):
        if not isinstance(row, dict):
            continue
        _add_source(
            index,
            _norm_wallet(row.get("address")),
            "leaderboard_crypto_state",
            {
                "name": row.get("name"),
                "ranks": row.get("ranks"),
                "pnl_by_period": row.get("pnl_by_period"),
                "vol_by_period": row.get("vol_by_period"),
            },
        )

    registry = load_json(args.registry, default={})
    for row in _as_list(registry.get("wallets")):
        if not isinstance(row, dict):
            continue
        tags = [str(item) for item in _as_list(row.get("tags"))]
        is_leaderboard_btc = (
            "leaderboard_crypto" in tags
            or "leaderboard_top50" in tags
            or str(row.get("market_filter") or "") == "btc_5m"
        )
        if not is_leaderboard_btc:
            continue
        _add_source(
            index,
            _norm_wallet(row.get("address")),
            "registry_leaderboard_btc5m",
            {
                "name": row.get("name"),
                "enabled": bool(row.get("enabled", True)),
                "market_filter": row.get("market_filter"),
                "tags": tags,
            },
        )
    return index


def _active_wallets(path: str) -> set[str]:
    payload = load_json(path, default={})
    active = payload.get("active_set") if isinstance(payload.get("active_set"), dict) else {}
    out: set[str] = set()
    for member in _as_list(active.get("members")):
        if isinstance(member, dict):
            wallet = _norm_wallet(member.get("source_wallet"))
            if wallet:
                out.add(wallet)
    return out


def _iter_event_lines(path: str, tail_lines: int) -> Iterable[str]:
    if int(tail_lines) > 0:
        proc = subprocess.Popen(
            ["tail", "-n", str(int(tail_lines)), str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            yield line
        _, stderr = proc.communicate()
        if proc.returncode not in {0, None}:
            raise RuntimeError(f"tail failed for {path}: {stderr.strip()}")
        return
    with Path(path).open("r", encoding="utf-8") as handle:
        yield from handle


def _slug(row: dict[str, Any]) -> str:
    return str(row.get("market_slug") or row.get("event_slug") or row.get("raw", {}).get("slug") or "").lower()


def _window_start(row: dict[str, Any]) -> float | None:
    value = row.get("window_start_s")
    if isinstance(value, (int, float)):
        return float(value)
    parts = _slug(row).split("-")
    if parts:
        try:
            return float(parts[-1])
        except ValueError:
            return None
    return None


def _event_ts(row: dict[str, Any]) -> float | None:
    value = row.get("event_ts")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _entry_offset_s(row: dict[str, Any]) -> float | None:
    ts = _event_ts(row)
    if ts is None:
        return None
    window_start = _window_start(row)
    if window_start is not None:
        offset = ts - window_start
        if 0.0 <= offset < 300.0:
            return offset
    # Fable P1 ordered the corrected probe as ts mod 300; stale local
    # window_start_s fields must not create negative "early" offsets.
    return ts % 300.0


def _is_btc5m(row: dict[str, Any]) -> bool:
    return "btc-updown-5m-" in _slug(row)


def _metrics_from_events(
    args: argparse.Namespace,
    candidates: dict[str, dict[str, Any]],
    *,
    now_s: float,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    start_s = now_s - float(args.lookback_hours) * 3600.0
    event_rows_by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    observed_wallets: set[str] = set()
    parsed_rows = 0
    json_errors = 0
    oldest_event_ts: float | None = None
    newest_event_ts: float | None = None
    candidate_wallets = set(candidates)

    for line in _iter_event_lines(args.wallet_events, int(args.tail_lines)):
        if '"source_wallet"' not in line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            json_errors += 1
            continue
        parsed_rows += 1
        wallet = _norm_wallet(row.get("source_wallet"))
        if not wallet:
            continue
        observed_wallets.add(wallet)
        ts = _event_ts(row)
        if ts is not None:
            oldest_event_ts = ts if oldest_event_ts is None else min(oldest_event_ts, ts)
            newest_event_ts = ts if newest_event_ts is None else max(newest_event_ts, ts)
        if wallet not in candidate_wallets:
            continue
        if ts is not None and ts < start_s:
            continue
        event_rows_by_wallet[wallet].append(row)

    metrics: dict[str, dict[str, Any]] = {}
    for wallet, candidate in candidates.items():
        rows = event_rows_by_wallet.get(wallet, [])
        trade_rows = [row for row in rows if str(row.get("row_type") or "trade") == "trade"]
        btc5m_rows = [row for row in trade_rows if _is_btc5m(row)]
        btc5m_buys = [row for row in btc5m_rows if str(row.get("action") or "").upper() == "BUY"]
        offsets: list[float] = []
        buy_offsets: list[float] = []
        inband_buys = 0
        latest_ts = max((_event_ts(row) or 0.0 for row in trade_rows), default=0.0)
        latest_btc5m_ts = max((_event_ts(row) or 0.0 for row in btc5m_rows), default=0.0)
        for row in btc5m_rows:
            offset = _entry_offset_s(row)
            if offset is not None:
                offsets.append(offset)
        for row in btc5m_buys:
            offset = _entry_offset_s(row)
            if offset is not None:
                buy_offsets.append(offset)
            price = num(row.get("price"), None)
            if price is not None and float(args.inband_min_price) <= price <= float(args.inband_max_price):
                inband_buys += 1
        latest_age_h = (now_s - latest_ts) / 3600.0 if latest_ts > 0 else None
        median_entry = statistics.median(offsets) if offsets else None
        inband_share = (100.0 * inband_buys / len(btc5m_buys)) if btc5m_buys else None
        pass_reasons: list[str] = []
        if latest_age_h is None or latest_age_h > float(args.fresh_hours):
            pass_reasons.append("no_fresh_local_feed_flow")
        if len(btc5m_buys) < int(args.min_btc5m_buys):
            pass_reasons.append("btc5m_buy_sample_below_min")
        if median_entry is None or median_entry >= float(args.median_entry_threshold_s):
            pass_reasons.append("median_entry_offset_not_lt_60s")
        if inband_share is None or inband_share < float(args.inband_min_share_pct):
            pass_reasons.append("inband_buy_share_below_50pct")

        metrics[wallet] = {
            "wallet": wallet,
            "sources": candidate["sources"],
            "evidence": candidate["evidence"],
            "local_feed_rows": len(rows),
            "trade_rows": len(trade_rows),
            "btc5m_trades": len(btc5m_rows),
            "btc5m_trade_share_pct": round(100.0 * len(btc5m_rows) / len(trade_rows), 6) if trade_rows else None,
            "btc5m_buys": len(btc5m_buys),
            "median_entry_offset_s": round(float(median_entry), 6) if median_entry is not None else None,
            "median_buy_entry_offset_s": round(float(statistics.median(buy_offsets)), 6) if buy_offsets else None,
            "inband_025_050_buy_count": inband_buys,
            "inband_025_050_buy_share_pct": round(inband_share, 6) if inband_share is not None else None,
            "latest_trade_ts": latest_ts or None,
            "latest_btc5m_trade_ts": latest_btc5m_ts or None,
            "latest_trade_age_h": round(latest_age_h, 6) if latest_age_h is not None else None,
            "fresh_flow": bool(latest_age_h is not None and latest_age_h <= float(args.fresh_hours)),
            "p1_promotion_eligible": not pass_reasons,
            "p1_reject_reasons": pass_reasons,
        }

    scan = {
        "parsed_rows": parsed_rows,
        "json_errors": json_errors,
        "observed_wallets": len(observed_wallets),
        "candidate_wallets": len(candidates),
        "candidate_wallets_with_rows": sum(1 for row in metrics.values() if int(row["trade_rows"]) > 0),
        "tail_lines": int(args.tail_lines),
        "lookback_hours": float(args.lookback_hours),
        "oldest_event_ts": oldest_event_ts,
        "newest_event_ts": newest_event_ts,
        "coverage_warning": "tail_does_not_cover_full_lookback"
        if oldest_event_ts is not None and oldest_event_ts > start_s
        else "",
    }
    return metrics, scan


def _rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
    queue = evidence.get("member_queue") if isinstance(evidence.get("member_queue"), dict) else {}
    fleet = evidence.get("btc5m_fleet") if isinstance(evidence.get("btc5m_fleet"), dict) else {}
    morning = evidence.get("btc5m_morning_table") if isinstance(evidence.get("btc5m_morning_table"), dict) else {}
    source_rank = 5
    if queue.get("ready_for_live"):
        source_rank = 0
    elif str(fleet.get("admission_status") or "") == "READY_QUEUE":
        source_rank = 1
    elif str(morning.get("status") or "").endswith("READY_QUEUE"):
        source_rank = 2
    elif "registry_leaderboard_btc5m" in row.get("sources", []):
        source_rank = 3
    return (
        source_rank,
        num(row.get("median_entry_offset_s"), 999999.0),
        -num(row.get("inband_025_050_buy_share_pct"), -1.0),
        -int(row.get("btc5m_buys") or 0),
        -num((queue or {}).get("paper_pnl_usd"), num((fleet or {}).get("paper_pnl_usd"), 0.0)),
    )


def build_report(args: argparse.Namespace, *, now_s: float | None = None) -> dict[str, Any]:
    now_s = time.time() if now_s is None else now_s
    candidates = _candidate_index(args)
    metrics, scan = _metrics_from_events(args, candidates, now_s=now_s)
    active_wallets = _active_wallets(args.active_set)
    for row in metrics.values():
        row["already_active"] = row["wallet"] in active_wallets

    ranked = sorted(metrics.values(), key=lambda row: (not bool(row["p1_promotion_eligible"]), _rank_key(row)))
    eligible = [row for row in ranked if row["p1_promotion_eligible"] and not row["already_active"]]
    recommendations = eligible[: max(0, int(args.max_promotions))]
    queue_measured = [
        row for row in ranked if "member_queue" in row.get("sources", []) and int(row.get("trade_rows") or 0) > 0
    ]
    fresh_local_feed_outside_queue = [
        row
        for row in ranked
        if "member_queue" not in row.get("sources", [])
        and row.get("fresh_flow")
        and int(row.get("btc5m_trades") or 0) > 0
    ]
    return {
        "schema_version": 1,
        "kind": "wallet_copy_corrected_copyability_probe",
        "flow_stage": "ROTATE/LEARN/LIVE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "criteria": {
            "source": "corrected local feed / poller rows, not raw Data API trades endpoint",
            "fresh_hours": float(args.fresh_hours),
            "median_entry_offset_lt_s": float(args.median_entry_threshold_s),
            "inband_buy_price_range": [float(args.inband_min_price), float(args.inband_max_price)],
            "inband_buy_share_min_pct": float(args.inband_min_share_pct),
            "min_btc5m_buys": int(args.min_btc5m_buys),
            "max_promotions": int(args.max_promotions),
        },
        "scan": scan,
        "summary": {
            "candidate_wallets": len(candidates),
            "candidate_wallets_with_local_rows": scan["candidate_wallets_with_rows"],
            "p1_promotion_eligible_total": sum(1 for row in ranked if row["p1_promotion_eligible"]),
            "p1_promotion_eligible_non_active": len(eligible),
            "recommendation_count": len(recommendations),
            "queue_candidates_measured": len(queue_measured),
            "fresh_local_feed_outside_queue": len(fresh_local_feed_outside_queue),
            "swap_action": "PROMOTE_RECOMMENDED" if recommendations else "NO_SWAP_NO_P1_PASSING_NON_ACTIVE_SOURCE",
        },
        "recommendations": recommendations,
        "queue_candidates_measured": queue_measured[:50],
        "fresh_local_feed_outside_queue": fresh_local_feed_outside_queue[:50],
        "ranked_candidates": ranked[:200],
    }


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
