#!/usr/bin/env python3
"""Collect prospective fee-aware long-horizon wallet-copy paper evidence."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.capture_clob_book_snapshots import _book_best_bid_ask  # noqa: E402
from scripts.merge_rtds_wallet_events import _token_metadata_from_history  # noqa: E402
from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    expected_polymarket_buy_fee_usd,
)
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402


EXPERIMENT_ID = "fee-aware-long-horizon-copy-paper-20260719"
A689 = "0xa6896d11f76dfa2820662c1f441496f51553559b"
W8BC = "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473"
WALLET_HORIZONS = {A689: (5.0, 30.0), W8BC: (30.0,)}
DEADLINE_TS = 1784746800.0  # 2026-07-22T19:00:00Z


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--polygon-jsonl", default="data/research/polygon_orderfilled_ws_shadow_resident.jsonl")
    parser.add_argument("--history-state", default="data/research/wallet_copy_live_guard_hot_history_state.json")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--state", default="data/research/fee_aware_long_horizon_copy_paper_latest.json")
    parser.add_argument("--event-log", default="data/research/fee_aware_long_horizon_copy_paper_events.jsonl")
    parser.add_argument("--lock-file", default="data/research/fee_aware_long_horizon_copy_paper.lock")
    parser.add_argument("--clob-base-url", default=os.getenv("POLYMARKET_CLOB_API_BASE_URL", "http://127.0.0.1:8787/clob"))
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--duration-s", type=float, default=0.0, help="0 runs until stopped")
    parser.add_argument("--deadline-ts", type=float, default=DEADLINE_TS)
    return parser.parse_args()


def _wallets(row: dict[str, Any]) -> list[str]:
    values = [str(row.get("selected_wallet") or ""), *(str(item) for item in row.get("registry_wallets") or [])]
    return sorted({value.lower() for value in values if value.lower() in WALLET_HORIZONS})


def _signal_id(row: dict[str, Any], wallet: str, token_id: str) -> str:
    return stable_id(
        "lhs",
        {
            "wallet": wallet,
            "transaction_hash": str(row.get("transaction_hash") or row.get("transactionHash") or "").lower(),
            "log_index": row.get("log_index"),
            "token_id": token_id,
        },
    )


def polygon_rows_to_intents(
    rows: list[dict[str, Any]],
    *,
    token_meta: dict[str, dict[str, str]],
    collector_start_ts: float,
) -> tuple[list[CopyIntent], list[dict[str, Any]], list[dict[str, Any]]]:
    intents: list[CopyIntent] = []
    unmapped: list[dict[str, Any]] = []
    excluded_non_qualifying: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if row.get("event") != "polygon_orderfilled_log":
            continue
        captured_at_s = float(row.get("captured_at_s") or row.get("received_at_s") or 0.0)
        if captured_at_s < collector_start_ts:
            continue
        decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
        if str(decoded.get("side") or "").upper() != "BUY":
            continue
        token_id = str(decoded.get("asset") or "")
        price = float(decoded.get("price") or 0.0)
        event_ts = float(row.get("event_ts") or row.get("block_ts") or 0.0)
        if not token_id or not (0.0 < price < 1.0) or event_ts <= 0.0:
            continue
        context = token_meta.get(token_id) if isinstance(token_meta.get(token_id), dict) else {}
        for wallet in _wallets(row):
            signal_id = _signal_id(row, wallet, token_id)
            if signal_id in seen:
                continue
            seen.add(signal_id)
            market_slug = str(context.get("market_slug") or "")
            if not market_slug:
                unmapped.append(
                    {
                        "signal_id": signal_id,
                        "source_wallet": wallet,
                        "token_id": token_id,
                        "event_ts": event_ts,
                        "captured_at_s": captured_at_s,
                        "source_row": row,
                    }
                )
                continue
            if not market_slug.startswith("btc-updown-5m-"):
                excluded_non_qualifying.append(
                    {
                        "signal_id": signal_id,
                        "source_wallet": wallet,
                        "token_id": token_id,
                        "event_ts": event_ts,
                        "captured_at_s": captured_at_s,
                        "market_slug": market_slug,
                        "reason": "mapped_non_btc5m_market",
                    }
                )
                continue
            intent = CopyIntent(
                source_wallet=wallet,
                wallet_name=f"long_horizon_{wallet[2:10]}",
                source_event_id=signal_id,
                condition_id=str(context.get("condition_id") or ""),
                market_slug=str(context.get("market_slug") or ""),
                outcome=str(context.get("outcome") or ""),
                side="YES" if str(context.get("outcome") or "").lower() == "up" else "NO",
                limit_price=round(price, 6),
                wallet_usdc_size=round(float(decoded.get("size") or 0.0) * price, 6),
                copy_size_usd=1.0,
                shares=round(1.0 / price, 6),
                observed_ts=captured_at_s,
                strategy_family="fee_aware_long_horizon_copy_paper_v1",
                policy_id=EXPERIMENT_ID,
                sizing_policy_id="fixed_usd_1",
                mode="paper",
                action="BUY",
                order_type="PAPER_CLOB_HORIZON_OBSERVATION",
                token_id=token_id,
                event_ts=event_ts,
                api_latency_s=round(max(0.0, captured_at_s - event_ts), 6),
                live_orders_allowed=False,
                reason="prospective parity-tapped source BUY for preregistered paper horizon observation",
                metadata={
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "source": "polygon_orderfilled_shadow",
                    "transaction_hash": str(row.get("transaction_hash") or "").lower(),
                    "log_index": row.get("log_index"),
                    "all_qualifying_intents_included": True,
                },
            )
            intents.append(intent)
    return intents, unmapped, excluded_non_qualifying


def pending_from_intents(intents: list[CopyIntent]) -> list[dict[str, Any]]:
    return [
        {
            "observation_id": stable_id("lho", {"intent_id": intent.intent_id, "horizon_s": horizon}),
            "intent": intent.asdict(),
            "horizon_s": horizon,
            "target_ts": round(float(intent.event_ts or 0.0) + horizon, 6),
            "status": "PENDING",
        }
        for intent in intents
        for horizon in WALLET_HORIZONS.get(intent.source_wallet.lower(), ())
    ]


def durable_intent_ids(state: dict[str, Any]) -> set[str]:
    ids = {str(item) for item in state.get("seen_intent_ids") or [] if str(item)}
    for key in ("pending", "expired"):
        for item in state.get(key) or []:
            if not isinstance(item, dict):
                continue
            intent = item.get("intent") if isinstance(item.get("intent"), dict) else {}
            if intent_id := str(intent.get("intent_id") or ""):
                ids.add(intent_id)
    for item in state.get("observations") or []:
        if isinstance(item, dict) and (intent_id := str(item.get("intent_id") or "")):
            ids.add(intent_id)
    return ids


def pending_disposition(item: dict[str, Any], *, now: float, deadline_ts: float) -> str:
    """Preregistered window is hard: nothing is observed at/after the deadline."""
    if now >= deadline_ts:
        return "AFTER_DEADLINE"
    target_ts = float(item.get("target_ts") or 0.0)
    if now < target_ts:
        return "WAIT"
    if now - target_ts > 5.0:
        return "MISSED_OBSERVATION_LAG"
    return "OBSERVE"


def observation_from_book(pending: dict[str, Any], book: dict[str, Any], *, captured_at_s: float) -> dict[str, Any] | None:
    target_ts = float(pending.get("target_ts") or 0.0)
    lag_s = captured_at_s - target_ts
    if lag_s < 0.0 or lag_s > 5.0:
        return None
    _, ask = _book_best_bid_ask(book)
    if ask is None or not (0.0 < ask < 1.0):
        return None
    shares = 1.0 / ask
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=ask)
    intent = pending.get("intent") if isinstance(pending.get("intent"), dict) else {}
    return {
        "schema_version": 1,
        "kind": "fee_aware_long_horizon_copy_paper_observation",
        "experiment_id": EXPERIMENT_ID,
        "observation_id": pending.get("observation_id"),
        "intent_id": intent.get("intent_id"),
        "source_wallet": str(intent.get("source_wallet") or "").lower(),
        "condition_id": intent.get("condition_id"),
        "market_slug": intent.get("market_slug"),
        "outcome": intent.get("outcome"),
        "token_id": intent.get("token_id"),
        "source_event_ts": intent.get("event_ts"),
        "source_price": intent.get("limit_price"),
        "horizon_s": pending.get("horizon_s"),
        "target_ts": target_ts,
        "captured_at_s": captured_at_s,
        "observation_lag_s": round(lag_s, 6),
        "best_ask": round(ask, 6),
        "filled_size_usd": round(1.0 + fee, 6),
        "principal_usd": 1.0,
        "filled_shares": round(shares, 6),
        "expected_fee_usd": fee,
        "fee_formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
        "final_status": "FILLED",
        "paper_only": True,
        "live_orders_allowed": False,
        "live_order_attempted": False,
        "copy_intent": intent,
    }


def score_and_summarize(observations: list[dict[str, Any]], resolutions_path: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    resolutions = load_resolutions(resolutions_path)
    scored: list[dict[str, Any]] = []
    by_lane: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in observations:
        score = score_order(row, resolutions)
        merged = {**row, "score": score}
        scored.append(merged)
        lane = f"{str(row.get('source_wallet') or '').lower()}@{float(row.get('horizon_s') or 0.0):g}s"
        by_lane[lane].append(merged)
    lanes: dict[str, Any] = {}
    for lane, rows in sorted(by_lane.items()):
        resolved = [row for row in rows if (row.get("score") or {}).get("resolved")]
        pnl = sum(float((row.get("score") or {}).get("pnl_usd") or 0.0) for row in resolved)
        lanes[lane] = {
            "observations": len(rows),
            "resolved": len(resolved),
            "aggregate_post_fee_pnl_usd": round(pnl, 6),
            "mean_post_fee_pnl_usd": round(pnl / len(resolved), 6) if resolved else None,
            "sample_gate_met": len(resolved) >= 30,
        }
    return scored, lanes


def _read_new_rows(path: Path, state: dict[str, Any]) -> list[dict[str, Any]]:
    stat = path.stat()
    inode = int(stat.st_ino)
    offset = int(state.get("polygon_offset") or stat.st_size)
    adjusted = False
    if int(state.get("polygon_inode") or inode) != inode or offset > stat.st_size:
        offset = max(0, stat.st_size - 50_000_000)
        adjusted = True
    if stat.st_size - offset > 50_000_000:
        offset = max(0, stat.st_size - 50_000_000)
        adjusted = True
    rows: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        handle.seek(offset)
        if adjusted and offset > 0:
            handle.readline()
        data = handle.read()
        next_offset = handle.tell()
    for line in data.splitlines():
        try:
            row = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(row, dict):
            rows.append(row)
    state["polygon_inode"] = inode
    state["polygon_offset"] = next_offset
    return rows


def _report(state: dict[str, Any], *, resolutions_path: str, deadline_ts: float) -> dict[str, Any]:
    observations = [row for row in state.get("observations") or [] if isinstance(row, dict)]
    _, lanes = score_and_summarize(observations, resolutions_path)
    expected_lanes = [f"{wallet}@{horizon:g}s" for wallet, horizons in WALLET_HORIZONS.items() for horizon in horizons]
    for lane in expected_lanes:
        lanes.setdefault(
            lane,
            {"observations": 0, "resolved": 0, "aggregate_post_fee_pnl_usd": 0.0, "mean_post_fee_pnl_usd": None, "sample_gate_met": False},
        )
    all_sampled = all(bool(row.get("sample_gate_met")) for row in lanes.values())
    all_positive = all(
        float(row.get("aggregate_post_fee_pnl_usd") or 0.0) > 0.0
        and float(row.get("mean_post_fee_pnl_usd") or 0.0) > 0.0
        for row in lanes.values()
    )
    combined = sum(float(row.get("aggregate_post_fee_pnl_usd") or 0.0) for row in lanes.values())
    return {
        **state,
        "schema_version": 1,
        "kind": "fee_aware_long_horizon_copy_paper_lane",
        "experiment_id": EXPERIMENT_ID,
        "flow_stages": ["LEARN", "PROMOTE"],
        "status": "PASS" if all_sampled and all_positive and combined > 0.0 else "FAIL" if time.time() >= deadline_ts else "ACCRUING",
        "updated_at": utc_now_iso(),
        "deadline_ts": deadline_ts,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_order_attempts": 0,
        "expired_count": int(state.get("expired_count") or 0),
        "excluded_non_qualifying_count": int(state.get("excluded_non_qualifying_count") or 0),
        "copy_intent_parity": {
            "status": "PASS" if not state.get("unmapped") and int(state.get("source_signals") or 0) == int(state.get("copy_intents") or 0) else "PENDING_TOKEN_MAPPING",
            "source_signals": int(state.get("source_signals") or 0),
            "copy_intents": int(state.get("copy_intents") or 0),
            "pending_token_mapping": len(state.get("unmapped") or []),
            "violations": 0,
            "all_qualifying_intents_included": True,
        },
        "fee": {"rate": POLYMARKET_EMBEDDED_FEE_RATE, "formula": POLYMARKET_EMBEDDED_FEE_FORMULA},
        "lanes": lanes,
        "combined_post_fee_pnl_usd": round(combined, 6),
        "pending_count": len(state.get("pending") or []),
        "observation_count": len(observations),
    }


def main() -> int:
    args = parse_args()
    state_path = Path(args.state)
    event_log = Path(args.event_log)
    lock_path = Path(args.lock_file)
    for path in (state_path, event_log, lock_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"status": "HELD", "lock_file": str(lock_path)}, sort_keys=True))
            return 0
        state = load_json(state_path, default={})
        if not isinstance(state, dict) or state.get("experiment_id") != EXPERIMENT_ID:
            state = {
                "experiment_id": EXPERIMENT_ID,
                "collector_start_ts": time.time(),
                "collector_start_at": utc_now_iso(),
                "source_signals": 0,
                "copy_intents": 0,
                "pending": [],
                "observations": [],
                "expired": [],
                "unmapped": [],
                "seen_signal_ids": [],
                "seen_intent_ids": [],
                "expired_count": 0,
                "excluded_non_qualifying_count": 0,
                "excluded_non_qualifying": [],
            }
        if "expired_count" not in state:
            state["expired_count"] = len(state.get("expired") or [])
        state.setdefault("excluded_non_qualifying_count", 0)
        state.setdefault("excluded_non_qualifying", [])
        clob = CLOBMarketClient(host=args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
        started = time.time()
        history_mtime = -1
        token_meta: dict[str, dict[str, str]] = {}
        seen_intents = durable_intent_ids(state)
        seen_signal_ids = {str(item) for item in state.get("seen_signal_ids") or [] if str(item)}
        while True:
            now = time.time()
            within_window = now < float(args.deadline_ts)
            history_path = Path(args.history_state)
            if history_path.exists() and history_path.stat().st_mtime_ns != history_mtime:
                history_payload = load_json(history_path, default={})
                token_meta = _token_metadata_from_history(history_payload if isinstance(history_payload, dict) else {})
                history_mtime = history_path.stat().st_mtime_ns
            polygon_path = Path(args.polygon_jsonl)
            rows = _read_new_rows(polygon_path, state) if within_window and polygon_path.exists() else []
            retry_rows = [
                item.get("source_row")
                for item in state.get("unmapped") or []
                if within_window and isinstance(item, dict) and isinstance(item.get("source_row"), dict)
            ]
            prior_unmapped_ids = {
                str(item.get("signal_id") or "")
                for item in state.get("unmapped") or []
                if isinstance(item, dict) and str(item.get("signal_id") or "")
            }
            intents, unmapped, excluded_non_qualifying = polygon_rows_to_intents(
                [*retry_rows, *rows],
                token_meta=token_meta,
                collector_start_ts=float(state["collector_start_ts"]),
            )
            qualifying_signal_ids = {
                *(intent.source_event_id for intent in intents),
                *(str(item.get("signal_id") or "") for item in unmapped),
            }
            excluded_signal_ids = {
                str(item.get("signal_id") or "") for item in excluded_non_qualifying if str(item.get("signal_id") or "")
            }
            new_signal_ids = {item for item in qualifying_signal_ids if item and item not in seen_signal_ids}
            new_excluded_ids = {item for item in excluded_signal_ids if item not in seen_signal_ids}
            reclassified_ids = excluded_signal_ids.intersection(prior_unmapped_ids)
            seen_signal_ids.update(new_signal_ids | new_excluded_ids)
            new_intents = [intent for intent in intents if intent.intent_id not in seen_intents]
            seen_intents.update(intent.intent_id for intent in new_intents)
            state["excluded_non_qualifying_count"] = (
                int(state.get("excluded_non_qualifying_count") or 0)
                + len(new_excluded_ids)
                + len(reclassified_ids)
            )
            prior_excluded = {
                str(item.get("signal_id") or ""): item
                for item in state.get("excluded_non_qualifying") or []
                if isinstance(item, dict) and str(item.get("signal_id") or "")
            }
            for item in excluded_non_qualifying:
                prior_excluded[str(item.get("signal_id") or "")] = item
            state["excluded_non_qualifying"] = list(prior_excluded.values())[-200:]
            state["source_signals"] = max(
                0,
                len(seen_signal_ids) - int(state.get("excluded_non_qualifying_count") or 0),
            )
            state["copy_intents"] = len(seen_intents)
            state["seen_signal_ids"] = sorted(seen_signal_ids)
            state["seen_intent_ids"] = sorted(seen_intents)
            state["pending"] = [*(state.get("pending") or []), *pending_from_intents(new_intents)]
            if within_window:
                state["unmapped"] = list({str(item.get("signal_id") or ""): item for item in unmapped}.values())
            keep: list[dict[str, Any]] = []
            emitted: list[dict[str, Any]] = []
            expired: list[dict[str, Any]] = []
            for item in state.get("pending") or []:
                disposition = pending_disposition(item, now=now, deadline_ts=float(args.deadline_ts))
                if disposition == "WAIT":
                    keep.append(item)
                    continue
                if disposition in ("MISSED_OBSERVATION_LAG", "AFTER_DEADLINE"):
                    expired.append({**item, "status": disposition, "expired_at": utc_now_iso()})
                    continue
                intent = item.get("intent") if isinstance(item.get("intent"), dict) else {}
                try:
                    book = clob.get_book(str(intent.get("token_id") or ""))
                except Exception:
                    keep.append(item)
                    continue
                observed_at = time.time()
                if observed_at >= float(args.deadline_ts):
                    expired.append({**item, "status": "AFTER_DEADLINE", "expired_at": utc_now_iso()})
                    continue
                observation = observation_from_book(item, book, captured_at_s=observed_at)
                if observation is None:
                    keep.append(item)
                    continue
                emitted.append(observation)
            if emitted:
                append_jsonl_many(event_log, emitted)
            state["pending"] = keep
            state["observations"] = [*(state.get("observations") or []), *emitted]
            state["expired"] = [*(state.get("expired") or []), *expired][-500:]
            state["expired_count"] = int(state.get("expired_count") or 0) + len(expired)
            report = _report(state, resolutions_path=args.resolutions, deadline_ts=float(args.deadline_ts))
            atomic_write_json(state_path, report)
            state = report
            if float(args.duration_s) > 0 and time.time() - started >= float(args.duration_s):
                print(json.dumps(report, indent=2, sort_keys=True))
                return 0
            time.sleep(max(0.1, float(args.interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
