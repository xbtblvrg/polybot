#!/usr/bin/env python3
"""Measure early-01a win rate where archived books were taker-executable."""
from __future__ import annotations

import argparse
import bisect
import glob
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, atomic_write_text  # noqa: E402

ROSTER = (
    "0x6ec53970165924e9bf0d5600209664a607d60588",
    "0x1e63c3c5c2f5e775aa7c939008510cb1db39aac8",
    "0x4462e46cf0d31466693058893f4913e2411fbfdf",
    "0xb353b92e8ef0a17b1e5d5f236c452f1029b5202e",
    "0x00033f1089ff061813850e5135483bed39ce3b49",
    "0x1972ace8aeded47fdd062d9ac0115c0c125e31c2",
    "0xbf337426aa856996b8bb79b238345dd1a0276bf7",
    "0xed8ea26967a75992811eac3ecfb02b9374f3d593",
)
SUSPECTED_TWO_SIDED_QUOTING = frozenset(
    {
        "0x00033f1089ff061813850e5135483bed39ce3b49",
        "0xbf337426aa856996b8bb79b238345dd1a0276bf7",
    }
)
CUT_ISO = "2026-08-03T04:20:00Z"
TRADES_URL = "https://data-api.polymarket.com/trades"
WINDOW_RE = re.compile(r"^btc-updown-5m-(\d+)$")
EXECUTABLE_ASK_CAP = 0.32
WIN_RATE_FLOOR_PCT = 31.0
MAX_CAUSAL_BOOK_LAG_S = 2.0
MAX_IMPOSSIBLE_ASK_FRACTION = 0.01


def _resolution_index(rows: Iterable[dict[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        winner = str(row.get("direction") or row.get("winning_outcome") or "").upper()
        slug = str(row.get("market_slug") or "")
        if slug and winner in {"UP", "DOWN"}:
            result[slug] = winner
    return result


def canonical_pairs(
    rows_by_wallet: dict[str, list[dict[str, Any]]],
    *,
    resolutions: dict[str, str],
    cut_ts: float,
) -> list[dict[str, Any]]:
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    for wallet, raw_rows in rows_by_wallet.items():
        seen: set[tuple[Any, ...]] = set()
        for row in raw_rows:
            dedupe_key = tuple(row.get(key) for key in ("transactionHash", "asset", "side", "price", "size"))
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            if str(row.get("side") or "").upper() != "BUY":
                continue
            slug = str(row.get("slug") or "")
            match = WINDOW_RE.fullmatch(slug)
            if not match:
                continue
            try:
                timestamp = float(row.get("timestamp"))
                price = float(row.get("price"))
                size = float(row.get("size") or 0.0)
            except (TypeError, ValueError):
                continue
            offset_s = timestamp - int(match.group(1))
            if timestamp > cut_ts or not (0.0 <= offset_s < 60.0) or not (0.25 <= price < 0.32):
                continue
            key = (wallet, slug)
            candidate = {
                "wallet": wallet,
                "slug": slug,
                "asset": str(row.get("asset") or ""),
                "timestamp": timestamp,
                "price": price,
                "size": size,
                "outcome": str(row.get("outcome") or "").upper(),
                "winning_outcome": resolutions.get(slug),
                "resolved": slug in resolutions,
                "transaction_hash": str(row.get("transactionHash") or ""),
                "suspected_two_sided_quoting": wallet in SUSPECTED_TWO_SIDED_QUOTING,
            }
            if key not in pairs or timestamp < pairs[key]["timestamp"]:
                pairs[key] = candidate
    return sorted(pairs.values(), key=lambda row: (row["timestamp"], row["wallet"], row["slug"]))


def attach_nearest_books(
    pairs: list[dict[str, Any]], book_rows: Iterable[dict[str, Any]], *, max_lag_s: float = MAX_CAUSAL_BOOK_LAG_S
) -> list[dict[str, Any]]:
    target_assets = {row["asset"] for row in pairs if row.get("asset")}
    books: dict[str, list[tuple[float, float, float]]] = {}
    for row in book_rows:
        asset = str(row.get("asset_id") or "")
        if asset not in target_assets:
            continue
        try:
            ts = float(row.get("captured_at_s"))
            bid = float(row.get("best_bid"))
            ask = float(row.get("best_ask"))
        except (TypeError, ValueError):
            continue
        books.setdefault(asset, []).append((ts, bid, ask))
    for values in books.values():
        values.sort()
    attached: list[dict[str, Any]] = []
    for pair in pairs:
        item = dict(pair)
        values = books.get(pair["asset"], [])
        if not values:
            item.update({"book_matched": False, "book_lag_s": None, "book_join_direction": "causal_unmatched", "best_bid": None, "best_ask": None})
            attached.append(item)
            continue
        timestamps = [value[0] for value in values]
        idx = bisect.bisect_right(timestamps, pair["timestamp"])
        candidates = [values[pos] for pos in range(max(0, idx - 4), idx)]
        causal = [value for value in candidates if 0.0 <= pair["timestamp"] - value[0] <= float(max_lag_s)]
        if not causal:
            item.update({"book_matched": False, "book_lag_s": None, "book_join_direction": "causal_unmatched", "best_bid": None, "best_ask": None})
            attached.append(item)
            continue
        ts, bid, ask = max(causal, key=lambda value: value[0])
        item.update(
            {
                "book_matched": True,
                "book_lag_s": round(ts - pair["timestamp"], 6),
                "book_join_direction": "causal",
                "book_captured_at_s": ts,
                "best_bid": bid,
                "best_ask": ask,
                "executable": ask < EXECUTABLE_ASK_CAP,
            }
        )
        attached.append(item)
    return attached


def build_report(rows: list[dict[str, Any]], *, generated_at: str) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved")]
    matched = [row for row in resolved if row.get("book_matched")]
    impossible = [row for row in matched if row.get("best_ask") is not None and row.get("price") is not None and float(row["best_ask"]) < float(row["price"])]
    impossible_fraction = len(impossible) / len(matched) if matched else 0.0
    causal_join_ok = impossible_fraction <= MAX_IMPOSSIBLE_ASK_FRACTION
    executable = [row for row in matched if row.get("book_join_direction") == "causal" and row.get("executable")] if causal_join_ok else []
    wins = sum(row.get("outcome") == row.get("winning_outcome") for row in executable)
    win_rate = round(100.0 * wins / len(executable), 6) if executable else None
    maker_flat_pnl = sum(((1.0 / float(row["price"])) if row.get("outcome") == row.get("winning_outcome") else 0.0) - 1.0 for row in executable)
    taker_flat_pnl = sum(((1.0 / float(row["best_ask"])) if row.get("outcome") == row.get("winning_outcome") else 0.0) - 1.0 for row in executable)
    mean_maker_fill = sum(float(row["price"]) for row in executable) / len(executable) if executable else None
    mean_taker_ask = sum(float(row["best_ask"]) for row in executable) / len(executable) if executable else None
    n_distinct_windows = len({row["slug"] for row in executable})
    correlation_ratio = round(len(executable) / n_distinct_windows, 6) if n_distinct_windows else None
    correlated = correlation_ratio is not None and correlation_ratio >= 2.0
    if not causal_join_ok:
        verdict = "CAUSAL_JOIN_VIOLATION"
    elif correlated:
        verdict = "OUTCOME_CORRELATED_POOL_NOT_INDEPENDENT"
    elif win_rate is None:
        verdict = "INSUFFICIENT_EXECUTABLE_BOOK_MATCHES"
    elif taker_flat_pnl <= 0.0:
        verdict = "FAIL_TAKER_EXECUTABLE_ROI"
    elif win_rate > WIN_RATE_FLOOR_PCT:
        verdict = "PASS_EXECUTABLE_SUBSET_WIN_RATE"
    else:
        verdict = "FAIL_EXECUTABLE_SUBSET_WIN_RATE"
    lags = sorted(float(row["book_lag_s"]) for row in matched)

    def cohort_metrics(cohort: list[dict[str, Any]]) -> dict[str, Any]:
        if not causal_join_ok:
            return {
                "executable_pairs": 0,
                "executable_wins": 0,
                "executable_subset_win_rate_pct": None,
                "n_distinct_windows": 0,
                "outcome_correlation_ratio": None,
                "edge_over_floor_pp": None,
                "verdict": "CAUSAL_JOIN_VIOLATION",
            }
        cohort_wins = sum(row.get("outcome") == row.get("winning_outcome") for row in cohort)
        cohort_win_rate = round(100.0 * cohort_wins / len(cohort), 6) if cohort else None
        distinct_windows = len({row["slug"] for row in cohort})
        ratio = round(len(cohort) / distinct_windows, 6) if distinct_windows else None
        cohort_pnl = sum(
            ((1.0 / float(row["best_ask"])) if row.get("outcome") == row.get("winning_outcome") else 0.0)
            - 1.0
            for row in cohort
        )
        if ratio is not None and ratio >= 2.0:
            cohort_verdict = "OUTCOME_CORRELATED_POOL_NOT_INDEPENDENT"
        elif cohort_win_rate is None:
            cohort_verdict = "INSUFFICIENT_EXECUTABLE_BOOK_MATCHES"
        elif cohort_pnl <= 0.0:
            cohort_verdict = "FAIL_TAKER_EXECUTABLE_ROI"
        elif cohort_win_rate > WIN_RATE_FLOOR_PCT:
            cohort_verdict = "PASS_EXECUTABLE_SUBSET_WIN_RATE"
        else:
            cohort_verdict = "FAIL_EXECUTABLE_SUBSET_WIN_RATE"
        result = {
            "executable_pairs": len(cohort),
            "executable_wins": cohort_wins,
            "executable_subset_win_rate_pct": cohort_win_rate,
            "n_distinct_windows": distinct_windows,
            "outcome_correlation_ratio": ratio,
            "edge_over_floor_pp": (
                round(cohort_win_rate - WIN_RATE_FLOOR_PCT, 6) if cohort_win_rate is not None else None
            ),
            "verdict": cohort_verdict,
        }
        if causal_join_ok:
            result["taker_best_ask_flat_1usd_roi_pct"] = round(100.0 * cohort_pnl / len(cohort), 6) if cohort else None
        return result

    excluding_quoting = [row for row in executable if not row.get("suspected_two_sided_quoting")]
    all_matched_wins = sum(row.get("outcome") == row.get("winning_outcome") for row in matched)
    all_matched_mean_fill = sum(float(row["price"]) for row in matched) / len(matched) if matched else None
    all_matched_mean_ask = sum(float(row["best_ask"]) for row in matched) / len(matched) if matched else None
    report = {
        "kind": "early_01a_executable_subset",
        "generated_at": generated_at,
        "publication_status": "PUBLISHABLE_CAUSAL_JOIN_MEASUREMENT" if causal_join_ok else "QUARANTINED_NON_CAUSAL_BOOK_JOIN",
        "lane_decision_status": "UNDERPOWERED_RETROSPECTIVE_CAUSAL_SAMPLE",
        "flow_stage": "MINE/MEASURE/MONEY/DEFEND",
        "paper_only": True,
        "live_mutation": False,
        "live_orders_allowed": False,
        "measurement_contract": {
            "unit": "earliest BUY in [0.25,0.32) at offset <60s per wallet+slug",
            "book_join": "greatest archived snapshot for the same asset with captured_at_s <= trade_ts and trade_ts - captured_at_s <= 2.0s",
            "book_join_direction": "causal",
            "executable": "best_ask < 0.32",
            "win_rate_floor_pct": WIN_RATE_FLOOR_PCT,
            "correlation_refusal": "n_resolved/n_distinct_windows >=2.0 refuses PASS",
            "causal_join_refusal": "CAUSAL_JOIN_VIOLATION if more than 1% of matched rows have best_ask < fill price",
            "lane_decision": "retrospective causal survivors are measurement-only and cannot open or close the lane",
        },
        "pairs_total": len(rows),
        "n_resolved": len(resolved),
        "unresolved_dropped": len(rows) - len(resolved),
        "book_matched": len(matched),
        "book_unmatched": len(resolved) - len(matched),
        "causal_join_violation_count": len(impossible),
        "causal_join_violation_fraction": round(impossible_fraction, 6),
        "causal_join_violation_max_fraction": MAX_IMPOSSIBLE_ASK_FRACTION,
        "all_matched": {
            "n": len(matched),
            "wins": all_matched_wins,
            "win_rate_pct": round(100.0 * all_matched_wins / len(matched), 6) if matched else None,
            "mean_maker_fill_price": round(all_matched_mean_fill, 6) if all_matched_mean_fill is not None else None,
            "mean_taker_best_ask": round(all_matched_mean_ask, 6) if all_matched_mean_ask is not None else None,
            "mean_taker_slippage_vs_maker_fill_pp": (
                round(100.0 * (all_matched_mean_ask - all_matched_mean_fill), 6)
                if all_matched_mean_ask is not None and all_matched_mean_fill is not None
                else None
            ),
        },
        "executable_pairs": len(executable),
        "executable_wins": wins,
        "executable_subset_win_rate_pct": win_rate,
        "mean_maker_fill_price": round(mean_maker_fill, 6) if mean_maker_fill is not None else None,
        "mean_taker_best_ask": round(mean_taker_ask, 6) if mean_taker_ask is not None else None,
        "mean_taker_slippage_vs_maker_fill_pp": (
            round(100.0 * (mean_taker_ask - mean_maker_fill), 6)
            if mean_taker_ask is not None and mean_maker_fill is not None
            else None
        ),
        "win_rate_floor_pct": WIN_RATE_FLOOR_PCT,
        "edge_over_floor_pp": round(win_rate - WIN_RATE_FLOOR_PCT, 6) if win_rate is not None else None,
        "n_distinct_windows": n_distinct_windows,
        "outcome_correlation_ratio": correlation_ratio,
        "book_lag_s": {
            "min": lags[0] if lags else None,
            "median": lags[len(lags) // 2] if lags else None,
            "max": lags[-1] if lags else None,
        },
        "verdict": verdict,
        "cohorts": {
            "including_suspected_two_sided_quoting": cohort_metrics(executable),
            "excluding_suspected_two_sided_quoting": cohort_metrics(excluding_quoting),
        },
    }
    if causal_join_ok:
        report["maker_fill_flat_1usd_roi_pct"] = round(100.0 * maker_flat_pnl / len(executable), 6) if executable else None
        report["taker_best_ask_flat_1usd_roi_pct"] = round(100.0 * taker_flat_pnl / len(executable), 6) if executable else None
    return report


def _jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--book-glob", default="data/research/clob_book_snapshots_*.jsonl")
    parser.add_argument("--book-files", type=int, default=40)
    parser.add_argument("--output", default="data/research/early_01a_executable_subset_latest.json")
    args = parser.parse_args()
    cut_ts = datetime.fromisoformat(CUT_ISO.replace("Z", "+00:00")).timestamp()
    session = requests.Session()
    rows_by_wallet: dict[str, list[dict[str, Any]]] = {}
    query_pages: dict[str, int] = {}
    for wallet in ROSTER:
        gathered: list[dict[str, Any]] = []
        pages = 0
        for offset in range(0, 100_000, 500):
            response = session.get(
                TRADES_URL,
                params={"user": wallet, "takerOnly": "false", "limit": 500, "offset": offset},
                timeout=20,
            )
            response.raise_for_status()
            page = response.json()
            if not isinstance(page, list):
                break
            gathered.extend(row for row in page if isinstance(row, dict))
            pages += 1
            timestamps = [float(row["timestamp"]) for row in page if row.get("timestamp") is not None]
            if len(page) < 500 or (timestamps and min(timestamps) < cut_ts):
                break
        rows_by_wallet[wallet] = gathered
        query_pages[wallet] = pages
    resolutions = _resolution_index(_jsonl(Path(args.resolutions)))
    pairs = canonical_pairs(rows_by_wallet, resolutions=resolutions, cut_ts=cut_ts)
    book_paths = sorted(glob.glob(args.book_glob), key=lambda value: Path(value).stat().st_mtime)[-max(1, args.book_files) :]

    def books() -> Iterable[dict[str, Any]]:
        for value in book_paths:
            yield from _jsonl(Path(value))

    attached = attach_nearest_books(pairs, books())
    generated_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    stamp = generated_at.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")
    rows_path = Path(f"data/research/early_01a_executable_subset_rows_{stamp}.jsonl")
    atomic_write_text(rows_path, "".join(json.dumps(row, sort_keys=True) + "\n" for row in attached))
    report = build_report(attached, generated_at=generated_at)
    report["input"] = {
        "roster": list(ROSTER),
        "roster_rule": "top 8 by qualifying_window_count after EXCHANGE_ADDRESSES exclusion; wallet lexicographic tiebreak",
        "query": "trades?user=<addr>&takerOnly=false&limit=500&offset=k*500",
        "query_pages": query_pages,
        "cut": CUT_ISO,
        "dedupe_key": ["transactionHash", "asset", "side", "price", "size"],
        "resolutions": args.resolutions,
        "book_glob": args.book_glob,
        "book_files_selected": book_paths,
        "rows": str(rows_path),
    }
    atomic_write_json(Path(args.output), report)
    print(json.dumps({key: report[key] for key in ("n_resolved", "book_matched", "executable_pairs", "n_distinct_windows", "executable_subset_win_rate_pct", "verdict")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
