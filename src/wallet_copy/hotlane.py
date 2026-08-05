"""Active wallet hot-lane selection for paper-only copy tracking.

The adaptive bot can only learn from fresh wallet moves. This module builds a
small scoped registry of wallets that are both strategically relevant and likely
to emit near-term BTC 5m events, so the tracker spends its latency budget on
the wallets most likely to produce usable copy evidence.
"""

from __future__ import annotations

import json
import math
import os
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from itertools import combinations
from pathlib import Path
from typing import Any

from src.wallet_copy.models import WalletSpec, parse_ts, utc_now_iso
from src.wallet_copy.registry import load_wallet_registry
from src.wallet_copy.store import atomic_write_json, load_json


MISSION_ANCHOR_TAGS = frozenset({"mission_anchor", "operator_mission_anchor", "current_poll_anchor"})
LEGACY_MISSION_ANCHOR_NAMES = frozenset({"weird_peak"})


@dataclass(frozen=True)
class ActiveHotlaneConfig:
    registry_path: str = "configs/wallet_copy/wallets.json"
    live_tracking_event_log_path: str = "data/research/wallet_copy_live_tracking_events.jsonl"
    active_hotlane_live_tracking_state_path: str = "data/research/wallet_copy_active_hotlane_live_tracking_state.json"
    history_state_path: str = "data/research/wallet_copy_history_state.json"
    leaderboard_state_path: str = "data/research/wallet_copy_leaderboard_crypto_state.json"
    profit_state_path: str = "data/research/wallet_copy_profit_engine_state.json"
    strategy_direction_state_path: str = "data/research/wallet_copy_strategy_direction_state.json"
    adaptive_state_path: str = "data/research/wallet_copy_adaptive_bot_state.json"
    hotlane_tick_state_path: str = "data/research/wallet_copy_hotlane_tick_state.json"
    output_registry_path: str = "data/research/wallet_copy_active_hotlane_registry.json"
    output_state_path: str = "data/research/wallet_copy_active_hotlane_state.json"
    max_wallets: int = 32
    live_log_tail_rows: int = 5000
    recent_window_s: float = 1800.0
    max_history_age_s: float = 6 * 3600.0
    min_score: float = 1.0
    development_bridge_slice_wallets: int = 4

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _max_full_history_state_load_bytes() -> int:
    raw = os.getenv("WALLET_COPY_HOTLANE_MAX_FULL_HISTORY_STATE_BYTES", "").strip()
    if not raw:
        return 64 * 1024 * 1024
    try:
        return max(1, int(raw))
    except ValueError:
        return 64 * 1024 * 1024


def _extract_tail_value_for_key(
    path: str | Path,
    key: str,
    *,
    max_bytes: int = 8 * 1024 * 1024,
) -> Any:
    target = Path(path)
    if not target.exists():
        return None
    try:
        size = target.stat().st_size
        with target.open("rb") as handle:
            handle.seek(max(0, size - max(1, int(max_bytes))))
            text = handle.read().decode("utf-8", errors="ignore")
    except OSError:
        return None

    marker = f'"{key}"'
    start = text.rfind(marker)
    if start < 0:
        return None
    colon = text.find(":", start + len(marker))
    if colon < 0:
        return None
    index = colon + 1
    while index < len(text) and text[index].isspace():
        index += 1
    if index >= len(text) or text[index] not in "{[":
        return None

    opener = text[index]
    closer = "}" if opener == "{" else "]"
    depth = 0
    in_string = False
    escaped = False
    for pos in range(index, len(text)):
        char = text[pos]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[index : pos + 1])
                except json.JSONDecodeError:
                    return None
    return None


def _extract_tail_list_for_key(path: str | Path, key: str, *, max_bytes: int = 8 * 1024 * 1024) -> list[Any]:
    value = _extract_tail_value_for_key(path, key, max_bytes=max_bytes)
    return value if isinstance(value, list) else []


def _load_history_state_for_scores(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    try:
        size = target.stat().st_size
    except OSError:
        size = 0
    if size > _max_full_history_state_load_bytes():
        wallet_results = _extract_tail_list_for_key(path, "wallet_results")
        return {
            "_hotlane_large_history_state_stub": True,
            "_file_size_bytes": int(size),
            "_reason": "history state exceeds bounded hot-lane full-load limit",
            "wallet_results": wallet_results,
        }
    payload = load_json(path, default={})
    return payload if isinstance(payload, dict) else {}


def _read_jsonl_tail(path: str | Path, limit: int) -> list[dict[str, Any]]:
    target = Path(path)
    if limit <= 0 or not target.exists():
        return []
    rows: deque[dict[str, Any]] = deque(maxlen=int(limit))
    with target.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return list(rows)


def _wallet_event(row: dict[str, Any]) -> dict[str, Any]:
    return row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}


def _row_wallet(row: dict[str, Any]) -> str:
    wallet_event = _wallet_event(row)
    return str(row.get("source_wallet") or wallet_event.get("source_wallet") or "").lower()


def _row_action(row: dict[str, Any]) -> str:
    wallet_event = _wallet_event(row)
    return str(row.get("action") or wallet_event.get("action") or "").upper()


def _row_market_slug(row: dict[str, Any]) -> str:
    wallet_event = _wallet_event(row)
    return str(row.get("market_slug") or wallet_event.get("market_slug") or "")


def _row_event_ts(row: dict[str, Any]) -> float | None:
    wallet_event = _wallet_event(row)
    return parse_ts(row.get("event_ts") or wallet_event.get("event_ts"))


def _row_usdc(row: dict[str, Any]) -> float:
    wallet_event = _wallet_event(row)
    ce = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
    for value in (row.get("source_usdc_size"), wallet_event.get("usdc_size"), ce.get("source_usdc_size")):
        try:
            if value is not None:
                return max(0.0, float(value))
        except (TypeError, ValueError):
            continue
    return 0.0


def _clob_status(row: dict[str, Any]) -> str:
    ce = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
    tracking = row.get("tracking_evidence") if isinstance(row.get("tracking_evidence"), dict) else {}
    clob = tracking.get("clob_book") if isinstance(tracking.get("clob_book"), dict) else {}
    return str(ce.get("clob_book_status") or clob.get("status") or "")


def _copyability_accepted(row: dict[str, Any]) -> bool:
    copyability = row.get("copyability") if isinstance(row.get("copyability"), dict) else {}
    ce = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
    return bool(copyability.get("accepted") is True or ce.get("copyability_accepted") is True)


def _score_recency(age_s: float, *, window_s: float) -> float:
    if age_s < 0 or age_s > window_s:
        return 0.0
    return max(0.0, 50.0 * (1.0 - age_s / max(1.0, window_s)))


def _leaderboard_wallet_scores(path: str | Path) -> dict[str, dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    rows = payload.get("candidate_wallets") if isinstance(payload.get("candidate_wallets"), list) else []
    scores: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        address = str(row.get("address") or "").lower()
        if not address:
            continue
        pnl_by_period = row.get("pnl_by_period") if isinstance(row.get("pnl_by_period"), dict) else {}
        vol_by_period = row.get("vol_by_period") if isinstance(row.get("vol_by_period"), dict) else {}
        ranks = row.get("ranks") if isinstance(row.get("ranks"), dict) else {}
        pnl = sum(max(0.0, float(value or 0.0)) for value in pnl_by_period.values())
        volume = sum(max(0.0, float(value or 0.0)) for value in vol_by_period.values())
        rank_bonus = 0.0
        for value in ranks.values():
            try:
                rank_bonus += max(0.0, 60.0 - float(value))
            except (TypeError, ValueError):
                continue
        scores[address] = {
            "leaderboard_score": round(rank_bonus + math.log1p(pnl) * 4.0 + math.log1p(volume) * 1.5, 6),
            "leaderboard_pnl": round(pnl, 6),
            "leaderboard_volume": round(volume, 6),
            "leaderboard_ranks": ranks,
        }
    return scores


def _profit_wallet_scores(path: str | Path) -> dict[str, dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    scores: dict[str, dict[str, Any]] = {}
    best = payload.get("best_candidate") if isinstance(payload.get("best_candidate"), dict) else {}
    metadata = best.get("metadata") if isinstance(best.get("metadata"), dict) else {}
    candidate = str(metadata.get("source_wallet") or "").lower()
    if candidate:
        scores[candidate] = {
            "profit_score": 150.0,
            "profit_candidate": True,
            "profit_candidate_status": best.get("status"),
            "profit_candidate_id": best.get("candidate_id"),
        }
    source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}
    wallet_search = source_contract.get("wallet_search_summary") if isinstance(source_contract.get("wallet_search_summary"), dict) else {}
    selected = wallet_search.get("selected_wallets") if isinstance(wallet_search.get("selected_wallets"), list) else []
    for index, row in enumerate(selected):
        if not isinstance(row, dict):
            continue
        address = str(row.get("source_wallet") or "").lower()
        if not address:
            continue
        entry = scores.setdefault(address, {})
        entry["profit_selected_wallet"] = True
        entry["profit_selected_rank"] = index + 1
        entry["profit_score"] = max(float(entry.get("profit_score") or 0.0), max(10.0, 100.0 - index * 2.0))
    bridge = payload.get("development_program_bridge") if isinstance(payload.get("development_program_bridge"), dict) else {}
    target_inventory = bridge.get("target_inventory") if isinstance(bridge.get("target_inventory"), dict) else {}
    target_inventory_wallets = _inventory_wallets(target_inventory)
    for index, address in enumerate(target_inventory_wallets[:25]):
        entry = scores.setdefault(address, {})
        entry["development_program_inventory_child"] = True
        entry["development_program_bridge_focus"] = True
        entry["development_program_target_inventory"] = target_inventory
        entry["development_program_next_major_change_action"] = bridge.get("next_major_change_action")
        entry["forward_queue_score"] = max(
            float(entry.get("forward_queue_score") or 0.0),
            max(120.0, 260.0 - index * 6.0),
        )
        entry["profit_score"] = max(float(entry.get("profit_score") or 0.0), 120.0)
    forward_queue = payload.get("forward_tracking_queue") if isinstance(payload.get("forward_tracking_queue"), list) else []
    for index, row in enumerate(forward_queue):
        if not isinstance(row, dict):
            continue
        address = str(row.get("source_wallet") or "").lower()
        if not address:
            continue
        entry = scores.setdefault(address, {})
        score = max(80.0, 220.0 - index * 12.0)
        if row.get("development_program_bridge_focus") is True:
            score += 180.0
            entry["development_program_bridge_focus"] = True
            entry["development_program_next_major_change_action"] = row.get(
                "development_program_next_major_change_action"
            )
            entry["development_program_stop_doing"] = (
                row.get("development_program_stop_doing")
                if isinstance(row.get("development_program_stop_doing"), list)
                else []
            )
            entry["development_program_target_inventory"] = (
                row.get("development_program_target_inventory")
                if isinstance(row.get("development_program_target_inventory"), dict)
                else {}
            )
        entry["forward_queue_score"] = max(float(entry.get("forward_queue_score") or 0.0), score)
        entry["forward_queue_rank"] = index + 1
        entry["forward_queue_candidate_id"] = row.get("candidate_id")
        entry["forward_queue_policy_id"] = row.get("policy_id")
        entry["profit_score"] = max(float(entry.get("profit_score") or 0.0), score * 0.6)
    runtime_queue = (
        payload.get("forward_queue_runtime_candidates")
        if isinstance(payload.get("forward_queue_runtime_candidates"), list)
        else []
    )
    for index, row in enumerate(runtime_queue):
        if not isinstance(row, dict):
            continue
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        address = str(metadata.get("source_wallet") or "").lower()
        if not address:
            continue
        entry = scores.setdefault(address, {})
        proof = row.get("runtime_copy_evidence") if isinstance(row.get("runtime_copy_evidence"), dict) else {}
        proof_events = float(proof.get("runtime_candidate_distinct_source_events") or 0.0)
        if row.get("status") == "PASS":
            proof_score = min(100.0, proof_events * 6.0)
            score = max(100.0, 240.0 - index * 10.0 + proof_score)
            entry["forward_queue_score"] = max(float(entry.get("forward_queue_score") or 0.0), score)
            entry["runtime_copy_proof_score"] = max(float(entry.get("runtime_copy_proof_score") or 0.0), proof_score)
        else:
            entry["runtime_copy_partial_score"] = max(
                float(entry.get("runtime_copy_partial_score") or 0.0),
                min(60.0, proof_events * 3.0),
            )
        entry["runtime_copy_candidate_id"] = row.get("candidate_id")
        entry["runtime_copy_candidate_status"] = row.get("status")
    return scores


def _history_wallet_scores(path: str | Path, *, now_ts: float, max_age_s: float) -> dict[str, dict[str, Any]]:
    payload = _load_history_state_for_scores(path)
    if not isinstance(payload, dict):
        return {}
    if payload.get("_hotlane_large_history_state_stub"):
        scores: dict[str, dict[str, Any]] = {}
        wallet_results = payload.get("wallet_results") if isinstance(payload.get("wallet_results"), list) else []
        for row in wallet_results:
            if not isinstance(row, dict):
                continue
            wallet_meta = row.get("wallet") if isinstance(row.get("wallet"), dict) else {}
            wallet = str(wallet_meta.get("address") or row.get("source_wallet") or row.get("wallet_address") or "").lower()
            if not wallet.startswith("0x"):
                continue
            copy_intents = int(row.get("copy_intents") or 0)
            events = int(row.get("events") or 0)
            buy_count = max(copy_intents, events)
            latest_ts = parse_ts(row.get("latest_event_ts"))
            if buy_count <= 0 and latest_ts is None:
                continue
            age = None if latest_ts is None else max(0.0, now_ts - float(latest_ts))
            recency_score = _score_recency(float(age), window_s=max_age_s) if age is not None else 0.0
            count_score = min(50.0, math.log1p(float(buy_count)) * 12.0)
            scores[wallet] = {
                "history_btc5m_buys": buy_count,
                "history_events": events,
                "history_copy_intents": copy_intents,
                "history_buy_usdc": 0.0,
                "latest_history_event_lag_s": round(age, 6) if age is not None else None,
                "history_score": round(recency_score + count_score, 6),
                "history_score_source": "wallet_results_tail_aggregate",
                "history_state_bounded": True,
                "history_state_size_bytes": payload.get("_file_size_bytes"),
            }
        return scores

    events = payload.get("events") if isinstance(payload.get("events"), list) else []
    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"history_btc5m_buys": 0, "history_buy_usdc": 0.0})
    for row in events:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        if not wallet or str(row.get("action") or "").upper() != "BUY":
            continue
        if "btc-updown-5m" not in str(row.get("market_slug") or ""):
            continue
        event_ts = parse_ts(row.get("event_ts"))
        age = None if event_ts is None else max(0.0, now_ts - float(event_ts))
        stats[wallet]["history_btc5m_buys"] += 1
        stats[wallet]["history_buy_usdc"] += max(0.0, float(row.get("usdc_size") or 0.0))
        if age is not None and (stats[wallet].get("latest_history_event_lag_s") is None or age < float(stats[wallet]["latest_history_event_lag_s"])):
            stats[wallet]["latest_history_event_lag_s"] = round(age, 6)
    scores: dict[str, dict[str, Any]] = {}
    for wallet, stat in stats.items():
        latest_lag = stat.get("latest_history_event_lag_s")
        recency_score = _score_recency(float(latest_lag), window_s=max_age_s) if latest_lag is not None else 0.0
        count_score = min(50.0, math.log1p(float(stat["history_btc5m_buys"])) * 12.0)
        size_score = min(30.0, math.log1p(float(stat["history_buy_usdc"])) * 2.0)
        scores[wallet] = {
            **stat,
            "history_score": round(recency_score + count_score + size_score, 6),
        }
    return scores


def _live_wallet_scores(rows: list[dict[str, Any]], *, now_ts: float, recent_window_s: float) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "live_btc5m_buys": 0,
            "live_buy_usdc": 0.0,
            "live_clob_ok": 0,
            "live_copyability_accepted": 0,
            "stale_live_btc5m_buys": 0,
        }
    )
    for row in rows:
        wallet = _row_wallet(row)
        if not wallet or _row_action(row) != "BUY" or "btc-updown-5m" not in _row_market_slug(row):
            continue
        event_ts = _row_event_ts(row)
        age = None if event_ts is None else max(0.0, now_ts - float(event_ts))
        if age is not None and (stats[wallet].get("latest_live_event_lag_s") is None or age < float(stats[wallet]["latest_live_event_lag_s"])):
            stats[wallet]["latest_live_event_lag_s"] = round(age, 6)
        if age is None or age > recent_window_s:
            stats[wallet]["stale_live_btc5m_buys"] += 1
            continue
        stats[wallet]["live_btc5m_buys"] += 1
        stats[wallet]["live_buy_usdc"] += _row_usdc(row)
        if _clob_status(row) == "OK":
            stats[wallet]["live_clob_ok"] += 1
        if _copyability_accepted(row):
            stats[wallet]["live_copyability_accepted"] += 1
    scores: dict[str, dict[str, Any]] = {}
    for wallet, stat in stats.items():
        latest_lag = stat.get("latest_live_event_lag_s")
        recency_score = _score_recency(float(latest_lag), window_s=recent_window_s) if latest_lag is not None else 0.0
        count_score = min(80.0, math.log1p(float(stat["live_btc5m_buys"])) * 18.0)
        size_score = min(40.0, math.log1p(float(stat["live_buy_usdc"])) * 3.0)
        evidence_score = min(60.0, float(stat["live_clob_ok"]) * 4.0 + float(stat["live_copyability_accepted"]) * 6.0)
        scores[wallet] = {
            **stat,
            "live_score": round(recency_score + count_score + size_score + evidence_score, 6),
        }
    return scores


def _live_coactivity_scores(
    rows: list[dict[str, Any]],
    *,
    now_ts: float,
    recent_window_s: float,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        wallet = _row_wallet(row)
        market_slug = _row_market_slug(row)
        if not wallet or _row_action(row) != "BUY" or "btc-updown-5m" not in market_slug:
            continue
        if _clob_status(row) != "OK" or not _copyability_accepted(row):
            continue
        event_ts = _row_event_ts(row)
        age = None if event_ts is None else max(0.0, now_ts - float(event_ts))
        if age is None or age > recent_window_s:
            continue
        outcome = str((_wallet_event(row) or {}).get("outcome") or row.get("outcome") or "")
        if not outcome:
            continue
        bucket = grouped[(market_slug, outcome)].setdefault(
            wallet,
            {
                "wallet": wallet,
                "events": 0,
                "usdc": 0.0,
                "latest_lag_s": None,
            },
        )
        bucket["events"] = int(bucket.get("events") or 0) + 1
        bucket["usdc"] = round(float(bucket.get("usdc") or 0.0) + _row_usdc(row), 6)
        if bucket.get("latest_lag_s") is None or age < float(bucket["latest_lag_s"]):
            bucket["latest_lag_s"] = round(age, 6)

    wallet_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "coactivity_groups": 0,
            "coactivity_events": 0,
            "coactivity_usdc": 0.0,
            "coactivity_peers": set(),
            "coactivity_score": 0.0,
        }
    )
    edge_stats: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "wallets": [],
            "coactive_markets": 0,
            "coactivity_events": 0,
            "coactivity_usdc": 0.0,
            "coactivity_score": 0.0,
            "latest_lag_s": None,
        }
    )
    groups: list[dict[str, Any]] = []
    for (market_slug, outcome), by_wallet in grouped.items():
        if len(by_wallet) < 2:
            continue
        latest_lag = min(float(row.get("latest_lag_s") or recent_window_s) for row in by_wallet.values())
        event_count = sum(int(row.get("events") or 0) for row in by_wallet.values())
        total_usdc = sum(float(row.get("usdc") or 0.0) for row in by_wallet.values())
        score = (
            _score_recency(latest_lag, window_s=recent_window_s) * 1.4
            + min(40.0, math.log1p(float(event_count)) * 10.0)
            + min(30.0, math.log1p(float(total_usdc)) * 3.0)
            + len(by_wallet) * 12.0
        )
        wallets = sorted(
            by_wallet,
            key=lambda wallet: (
                -float(by_wallet[wallet].get("usdc") or 0.0),
                -int(by_wallet[wallet].get("events") or 0),
                wallet,
            ),
        )
        group = {
            "market_slug": market_slug,
            "outcome": outcome,
            "wallets": wallets,
            "wallet_count": len(wallets),
            "event_count": event_count,
            "total_usdc": round(total_usdc, 6),
            "latest_lag_s": round(latest_lag, 6),
            "coactivity_score": round(score, 6),
        }
        groups.append(group)
        for wallet in wallets:
            stat = wallet_stats[wallet]
            stat["coactivity_groups"] = int(stat.get("coactivity_groups") or 0) + 1
            stat["coactivity_events"] = int(stat.get("coactivity_events") or 0) + int(by_wallet[wallet].get("events") or 0)
            stat["coactivity_usdc"] = round(
                float(stat.get("coactivity_usdc") or 0.0) + float(by_wallet[wallet].get("usdc") or 0.0),
                6,
            )
            stat["coactivity_score"] = round(float(stat.get("coactivity_score") or 0.0) + score, 6)
            stat["latest_coactivity_event_lag_s"] = (
                by_wallet[wallet].get("latest_lag_s")
                if stat.get("latest_coactivity_event_lag_s") is None
                else min(float(stat["latest_coactivity_event_lag_s"]), float(by_wallet[wallet].get("latest_lag_s") or recent_window_s))
            )
            stat["coactivity_peers"].update(peer for peer in wallets if peer != wallet)
        for left, right in combinations(wallets, 2):
            key = tuple(sorted((left, right)))
            edge = edge_stats[key]
            edge["wallets"] = list(key)
            edge["coactive_markets"] = int(edge.get("coactive_markets") or 0) + 1
            edge["coactivity_events"] = int(edge.get("coactivity_events") or 0) + event_count
            edge["coactivity_usdc"] = round(float(edge.get("coactivity_usdc") or 0.0) + total_usdc, 6)
            edge["coactivity_score"] = round(float(edge.get("coactivity_score") or 0.0) + score, 6)
            edge["latest_lag_s"] = (
                latest_lag
                if edge.get("latest_lag_s") is None
                else min(float(edge["latest_lag_s"]), latest_lag)
            )

    wallet_scores: dict[str, dict[str, Any]] = {}
    for wallet, stat in wallet_stats.items():
        wallet_scores[wallet] = {
            **stat,
            "coactivity_peers": sorted(stat.get("coactivity_peers") or []),
            "coactivity_score": round(float(stat.get("coactivity_score") or 0.0), 6),
        }
    groups = sorted(groups, key=lambda row: (-float(row["coactivity_score"]), float(row["latest_lag_s"]), row["market_slug"], row["outcome"]))
    edges = sorted(
        edge_stats.values(),
        key=lambda row: (
            -float(row.get("coactivity_score") or 0.0),
            float(row.get("latest_lag_s") or recent_window_s),
            str(row.get("wallets") or []),
        ),
    )
    return wallet_scores, groups[:50], edges[:50]


def _wallet_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    wallets: list[str] = []
    for item in value:
        text = str(item or "").lower()
        if text.startswith("0x") and text not in wallets:
            wallets.append(text)
    return wallets


def _inventory_wallets(row: dict[str, Any]) -> list[str]:
    wallets = _wallet_list(row.get("unique_wallets"))
    if wallets:
        return wallets
    outcome_wallets = row.get("outcome_wallets") if isinstance(row.get("outcome_wallets"), dict) else {}
    merged: list[str] = []
    for value in outcome_wallets.values():
        for wallet in _wallet_list(value):
            if wallet not in merged:
                merged.append(wallet)
    return merged


def _wallet_slices(wallets: list[str], *, slice_size: int) -> list[list[str]]:
    size = max(2, int(slice_size or 0))
    slices: list[list[str]] = []
    for index in range(0, len(wallets), size):
        chunk = wallets[index : index + size]
        if len(chunk) < 2 and slices:
            slices[-1].extend(chunk)
        elif len(chunk) >= 2:
            slices.append(chunk)
    return slices


def _recent_live_activity_cohort_rows(
    ranked_rows: list[dict[str, Any]],
    *,
    recent_window_s: float,
    max_rows: int = 20,
    slice_size: int = 4,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    wallets: list[str] = []
    for row in ranked_rows:
        address = str(row.get("address") or "").lower()
        if not address.startswith("0x") or address in wallets:
            continue
        try:
            lag_s = float(row.get("latest_live_event_lag_s"))
        except (TypeError, ValueError):
            continue
        if lag_s < 0 or lag_s > float(recent_window_s):
            continue
        if float(row.get("live_score") or 0.0) <= 0.0:
            continue
        wallets.append(address)
        if len(wallets) >= max(2, int(max_rows)):
            break
    cohorts: list[dict[str, Any]] = []
    for index, slice_wallets in enumerate(_wallet_slices(wallets, slice_size=slice_size), start=1):
        if len(slice_wallets) < 2:
            continue
        cohorts.append(
            {
                "source": "recent_live_activity",
                "selected_source": "recent_live_activity_current_poll_probe",
                "role": "current_poll_recent_activity_cohort_only_not_live_admission_truth",
                "rank": index,
                "wallets": slice_wallets,
                "selected_wallets": slice_wallets,
                "unique_wallets": slice_wallets,
                "wallet_count": len(slice_wallets),
                "recent_window_s": float(recent_window_s),
            }
        )
    return cohorts, {
        "recent_live_activity_wallets": len(wallets),
        "recent_live_activity_cohorts": len(cohorts),
        "recent_window_s": float(recent_window_s),
        "role": "poll_ranking_only_not_live_admission_truth",
    }


def _top_outcome_wallets(row: dict[str, Any]) -> tuple[str, list[str]]:
    outcome_wallets = row.get("outcome_wallets") if isinstance(row.get("outcome_wallets"), dict) else {}
    top_outcome = str(row.get("top_outcome") or "")
    if top_outcome and isinstance(outcome_wallets.get(top_outcome), list):
        wallets = _wallet_list(outcome_wallets.get(top_outcome))
        if wallets:
            return top_outcome, wallets
    best_outcome = ""
    best_wallets: list[str] = []
    best_score = -1.0
    outcome_scores = row.get("outcome_scores_usd") if isinstance(row.get("outcome_scores_usd"), dict) else {}
    for outcome, value in outcome_scores.items():
        wallets = _wallet_list(outcome_wallets.get(outcome))
        if len(wallets) < 2:
            continue
        try:
            score = float(value or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        if score > best_score:
            best_outcome = str(outcome)
            best_wallets = wallets
            best_score = score
    if best_wallets:
        return best_outcome, best_wallets
    return top_outcome, _inventory_wallets(row)


def _development_program_bridge_cohort_rows(
    path: str | Path,
    *,
    max_rows: int = 20,
    slice_size: int = 4,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return [], {"development_program_bridge_state_present": False}

    bridge = payload.get("development_program_bridge") if isinstance(payload.get("development_program_bridge"), dict) else {}
    forward_queue = payload.get("forward_tracking_queue") if isinstance(payload.get("forward_tracking_queue"), list) else []
    bridge_rows = [
        row
        for row in forward_queue
        if isinstance(row, dict) and row.get("development_program_bridge_focus") is True
    ]
    wallet_roles = bridge.get("wallet_roles") if isinstance(bridge.get("wallet_roles"), dict) else {}
    target_inventory = bridge.get("target_inventory") if isinstance(bridge.get("target_inventory"), dict) else {}

    wallets: list[str] = []
    roles_by_wallet: dict[str, list[str]] = {}
    for row in bridge_rows[:max_rows]:
        wallet = str(row.get("source_wallet") or "").lower()
        if not wallet.startswith("0x"):
            continue
        if wallet not in wallets:
            wallets.append(wallet)
        roles = row.get("development_program_bridge_roles")
        if isinstance(roles, list):
            roles_by_wallet.setdefault(wallet, [])
            for role in roles:
                role_text = str(role or "")
                if role_text and role_text not in roles_by_wallet[wallet]:
                    roles_by_wallet[wallet].append(role_text)
        if not target_inventory:
            row_inventory = row.get("development_program_target_inventory")
            if isinstance(row_inventory, dict):
                target_inventory = row_inventory

    for wallet, roles in wallet_roles.items():
        address = str(wallet or "").lower()
        if not address.startswith("0x"):
            continue
        if address not in wallets:
            wallets.append(address)
        if isinstance(roles, list):
            roles_by_wallet.setdefault(address, [])
            for role in roles:
                role_text = str(role or "")
                if role_text and role_text not in roles_by_wallet[address]:
                    roles_by_wallet[address].append(role_text)
    for address in _inventory_wallets(target_inventory):
        if address not in wallets:
            wallets.append(address)
        roles = roles_by_wallet.setdefault(address, [])
        if "multi_wallet_inventory_child" not in roles:
            roles.append("multi_wallet_inventory_child")

    active = bool(bridge.get("active") is True or bridge_rows)
    if not active or len(wallets) < 2:
        return [], {
            "development_program_bridge_state_present": bool(payload),
            "development_program_bridge_active": active,
            "development_program_bridge_cohorts": 0,
            "development_program_bridge_wallets": len(wallets),
            "role": "poll_ranking_only_not_live_admission_truth",
        }

    next_action = bridge.get("next_major_change_action") or next(
        (row.get("development_program_next_major_change_action") for row in bridge_rows if isinstance(row, dict)),
        None,
    )
    slices = _wallet_slices(wallets, slice_size=slice_size)
    cohorts: list[dict[str, Any]] = []
    for index, slice_wallets in enumerate(slices, start=1):
        cohorts.append(
            {
                "source": "development_program_bridge_candidate",
                "selected_source": "development_program_bridge_current_poll_inventory_burnin",
                "role": "current_poll_cohort_only_not_live_admission_truth",
                "rank": index,
                "wallets": slice_wallets,
                "unique_wallets": slice_wallets,
                "wallet_count": len(slice_wallets),
                "candidate_id": target_inventory.get("candidate_id"),
                "policy_id": target_inventory.get("policy_id"),
                "development_program_bridge_focus": True,
                "development_program_next_major_change_action": next_action,
                "development_program_target_inventory": target_inventory,
                "development_program_stop_doing": bridge.get("stop_doing") if isinstance(bridge.get("stop_doing"), list) else [],
                "development_program_bridge_wallet_roles": {
                    wallet: roles_by_wallet.get(wallet, []) for wallet in slice_wallets
                },
                "development_program_bridge_slice_index": index,
                "development_program_bridge_slice_count": len(slices),
                "development_program_bridge_total_wallets": len(wallets),
                "top_score_usd": target_inventory.get("pnl_usd"),
                "total_score_usd": target_inventory.get("pnl_usd"),
            }
        )
    return cohorts, {
        "development_program_bridge_state_present": True,
        "development_program_bridge_active": active,
        "development_program_bridge_cohorts": len(cohorts),
        "development_program_bridge_slice_wallets": max(2, int(slice_size or 0)),
        "development_program_bridge_wallets": len(wallets),
        "development_program_bridge_candidate_id": target_inventory.get("candidate_id"),
        "development_program_next_major_change_action": next_action,
        "role": "poll_ranking_only_not_live_admission_truth",
    }


def _tracker_time_inventory_candidate_rows(path: str | Path, *, max_rows: int = 20) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return [], {"tracker_time_inventory_cohort_state_present": False}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    tracker_replay = payload.get("tracker_time_replay") if isinstance(payload.get("tracker_time_replay"), dict) else {}
    tracker_replay_summary = (
        tracker_replay.get("summary") if isinstance(tracker_replay.get("summary"), dict) else {}
    )
    rows = summary.get("top_tracker_time_inventory_candidates")
    if not isinstance(rows, list):
        rows = tracker_replay_summary.get("top_inventory_candidates")
    if not isinstance(rows, list):
        rows = []

    cohorts: list[dict[str, Any]] = []
    for rank, item in enumerate(rows[:max_rows]):
        if not isinstance(item, dict):
            continue
        outcome, wallets = _top_outcome_wallets(item)
        if len(wallets) < 2:
            continue
        score = _signal_score(item, base=75.0, rank=rank)
        cohort = {
            "source": "tracker_time_inventory_candidate",
            "role": "poll_cohort_only_not_live_admission_truth",
            "candidate_id": item.get("candidate_id"),
            "policy_id": item.get("policy_id"),
            "mode": item.get("mode"),
            "market_slug": item.get("market_slug"),
            "outcome": outcome,
            "wallets": wallets,
            "wallet_count": len(wallets),
            "unique_wallets": _inventory_wallets(item),
            "top_score_usd": item.get("top_score_usd"),
            "total_score_usd": item.get("total_score_usd"),
            "dominance": item.get("dominance"),
            "balance_ratio": item.get("balance_ratio"),
            "clustered_out_moves": item.get("clustered_out_moves"),
            "tracker_time_inventory_score": score,
            "rank": rank + 1,
        }
        cohorts.append(cohort)
    cohorts.sort(
        key=lambda row: (
            -float(row.get("tracker_time_inventory_score") or 0.0),
            -float(row.get("top_score_usd") or 0.0),
            -int(row.get("wallet_count") or 0),
            str(row.get("market_slug") or ""),
            str(row.get("outcome") or ""),
        )
    )
    return cohorts, {
        "tracker_time_inventory_cohort_state_present": True,
        "tracker_time_inventory_candidates_read": len(rows),
        "tracker_time_inventory_cohorts": len(cohorts),
        "role": "poll_cohort_only_not_live_admission_truth",
    }


def _signal_score(row: dict[str, Any], *, base: float, rank: int) -> float:
    try:
        score_usd = max(0.0, float(row.get("score_usd") or row.get("top_score_usd") or 0.0))
    except (TypeError, ValueError):
        score_usd = 0.0
    try:
        events = max(0.0, float(row.get("fresh_event_count") or row.get("event_count") or row.get("clustered_out_moves") or 0.0))
    except (TypeError, ValueError):
        events = 0.0
    try:
        clob_events = max(0.0, float(row.get("clob_backed_event_count") or 0.0))
    except (TypeError, ValueError):
        clob_events = 0.0
    raw = base + min(35.0, math.log1p(score_usd) * 9.0) + min(24.0, events * 1.5) + min(20.0, clob_events * 2.0)
    return round(raw / max(1.0, 1.0 + rank * 0.2), 6)


def _add_wallet_score(
    scores: dict[str, dict[str, Any]],
    wallet: str,
    key: str,
    value: float,
    *,
    reason: str,
    market_slug: Any = None,
    outcome: Any = None,
) -> None:
    address = str(wallet or "").lower()
    if not address.startswith("0x") or value <= 0:
        return
    row = scores.setdefault(address, {"address": address})
    row[key] = round(float(row.get(key) or 0.0) + float(value), 6)
    row[f"{key}_count"] = int(row.get(f"{key}_count") or 0) + 1
    details = row.setdefault("evidence_selection_details", [])
    if isinstance(details, list) and len(details) < 12:
        details.append(
            {
                "reason": reason,
                "score_key": key,
                "score": round(float(value), 6),
                "market_slug": market_slug,
                "outcome": outcome,
            }
        )


def _score_signal_rows(
    scores: dict[str, dict[str, Any]],
    rows: list[Any],
    *,
    key: str,
    reason: str,
    base: float,
    max_rows: int = 20,
    include_opposing: bool = False,
) -> int:
    count = 0
    for rank, item in enumerate(rows[:max_rows]):
        if not isinstance(item, dict):
            continue
        wallets = _wallet_list(item.get("agreeing_wallets"))
        if include_opposing:
            for wallet in _wallet_list(item.get("opposing_wallets")):
                if wallet not in wallets:
                    wallets.append(wallet)
        if not wallets:
            continue
        value = _signal_score(item, base=base, rank=rank)
        for wallet in wallets:
            _add_wallet_score(
                scores,
                wallet,
                key,
                value,
                reason=reason,
                market_slug=item.get("market_slug"),
                outcome=item.get("outcome"),
            )
        count += 1
    return count


def _score_inventory_rows(
    scores: dict[str, dict[str, Any]],
    rows: list[Any],
    *,
    key: str,
    reason: str,
    base: float,
    max_rows: int = 20,
) -> int:
    count = 0
    for rank, item in enumerate(rows[:max_rows]):
        if not isinstance(item, dict):
            continue
        wallets = _inventory_wallets(item)
        if not wallets:
            continue
        value = _signal_score(item, base=base, rank=rank)
        for wallet in wallets:
            _add_wallet_score(
                scores,
                wallet,
                key,
                value,
                reason=reason,
                market_slug=item.get("market_slug"),
                outcome=item.get("top_outcome"),
            )
        count += 1
    return count


def _current_hot_path_wallet_scores(path: str | Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}, [], {"current_hot_path_state_present": False}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    hot_path = summary.get("hot_path_adaptive") if isinstance(summary.get("hot_path_adaptive"), dict) else {}
    hot_summary = hot_path.get("summary") if isinstance(hot_path.get("summary"), dict) else {}
    current_poll_diagnostics = (
        hot_path.get("current_poll_diagnostics")
        if isinstance(hot_path.get("current_poll_diagnostics"), dict)
        else {}
    )
    freshness = hot_summary.get("freshness_diagnostics") if isinstance(hot_summary.get("freshness_diagnostics"), dict) else {}
    top_signals = hot_path.get("top_signals") if isinstance(hot_path.get("top_signals"), list) else []
    blocked_signals = (
        hot_summary.get("top_blocked_runtime_signals")
        if isinstance(hot_summary.get("top_blocked_runtime_signals"), list)
        else []
    )
    inventory_measurement = (
        hot_summary.get("hot_path_inventory_measurement")
        if isinstance(hot_summary.get("hot_path_inventory_measurement"), dict)
        else {}
    )
    inventory_candidates = (
        inventory_measurement.get("top_inventory_candidates")
        if isinstance(inventory_measurement.get("top_inventory_candidates"), list)
        else hot_path.get("top_runtime_inventory_candidates")
    )
    if not isinstance(inventory_candidates, list):
        inventory_candidates = []

    scores: dict[str, dict[str, Any]] = {}
    pass_signals = [row for row in top_signals if isinstance(row, dict) and row.get("status") == "PASS"]
    partial_signals = [
        row
        for row in [*top_signals, *blocked_signals]
        if isinstance(row, dict) and row.get("status") != "PASS"
    ]
    pass_count = _score_signal_rows(
        scores,
        pass_signals,
        key="current_pass_score",
        reason="current_poll_hot_path_pass_signal_for_poll_ranking_only",
        base=140.0,
    )
    partial_count = _score_signal_rows(
        scores,
        partial_signals,
        key="current_partial_score",
        reason="current_poll_hot_path_partial_signal_for_poll_ranking_only",
        base=55.0,
        include_opposing=True,
    )
    inventory_count = _score_inventory_rows(
        scores,
        inventory_candidates,
        key="inventory_current_score",
        reason="current_poll_inventory_candidate_for_poll_ranking_only",
        base=95.0,
    )

    cohorts: list[dict[str, Any]] = []
    for rank, row in enumerate(pass_signals[:10], start=1):
        wallets = _wallet_list(row.get("agreeing_wallets"))
        if len(wallets) < 2:
            continue
        cohorts.append(
            {
                "candidate_id": row.get("signal_id") or row.get("candidate_id"),
                "market_slug": row.get("market_slug"),
                "condition_id": row.get("condition_id"),
                "outcome": row.get("outcome"),
                "source": "current_poll_hot_path_signal",
                "selected_source": "current_poll_hot_path_pass_signal_for_poll_only",
                "role": "current_poll_cohort_only_not_live_admission_truth",
                "rank": rank,
                "status": row.get("status"),
                "wallets": wallets,
                "selected_wallets": wallets,
                "wallet_count": len(wallets),
                "top_score_usd": row.get("score_usd"),
                "total_score_usd": row.get("score_usd"),
                "runtime_fresh_buy_events_le_cap": freshness.get("runtime_fresh_buy_events_le_cap"),
                "latest_buy_event_lag_s": freshness.get("latest_buy_event_lag_s"),
            }
        )
    for rank, row in enumerate(inventory_candidates[:10], start=1):
        outcome, wallets = _top_outcome_wallets(row)
        if len(wallets) < 2:
            continue
        cohorts.append(
            {
                **row,
                "outcome": outcome or row.get("top_outcome"),
                "wallets": wallets,
                "selected_wallets": wallets,
                "wallet_count": len(wallets),
                "source": "current_poll_inventory_candidate",
                "selected_source": "current_poll_inventory_candidate_for_poll_only",
                "role": "current_poll_inventory_cohort_only_not_live_admission_truth",
                "rank": rank,
                "runtime_fresh_buy_events_le_cap": freshness.get("runtime_fresh_buy_events_le_cap"),
                "latest_buy_event_lag_s": freshness.get("latest_buy_event_lag_s"),
            }
        )
    return scores, cohorts, {
        "current_hot_path_state_present": True,
        "current_hot_path_status": hot_path.get("status"),
        "wallets": len(scores),
        "pass_signals_scored": pass_count,
        "partial_signals_scored": partial_count,
        "inventory_candidates_scored": inventory_count,
        "cohorts": len(cohorts),
        "runtime_fresh_buy_events_le_cap": freshness.get("runtime_fresh_buy_events_le_cap"),
        "runtime_eligible_wallets": freshness.get("runtime_eligible_wallets"),
        "latest_buy_event_lag_s": freshness.get("latest_buy_event_lag_s"),
        "current_poll_diagnostics_status": current_poll_diagnostics.get("status"),
        "zero_current_poll_root_cause": current_poll_diagnostics.get("zero_current_poll_root_cause"),
        "current_poll_blockers": current_poll_diagnostics.get("blockers") or [],
        "role": "poll_ranking_only_not_live_admission_truth",
    }


def _adaptive_evidence_wallet_scores(path: str | Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}, {"adaptive_state_present": False}
    scores: dict[str, dict[str, Any]] = {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    tracker_replay = payload.get("tracker_time_replay") if isinstance(payload.get("tracker_time_replay"), dict) else {}
    tracker_replay_summary = (
        tracker_replay.get("summary") if isinstance(tracker_replay.get("summary"), dict) else {}
    )
    replay_signals = tracker_replay.get("signals") if isinstance(tracker_replay.get("signals"), list) else []
    tracker_time_signals = payload.get("tracker_time_signals") if isinstance(payload.get("tracker_time_signals"), list) else []
    pass_replay_signals = [
        row for row in [*replay_signals, *tracker_time_signals] if isinstance(row, dict) and row.get("status") == "PASS"
    ]
    blocked_replay_signals = summary.get("top_blocked_tracker_time_signals")
    if not isinstance(blocked_replay_signals, list):
        blocked_replay_signals = [
            row for row in tracker_time_signals if isinstance(row, dict) and row.get("status") != "PASS"
        ]
    runtime_partial_signals = summary.get("top_blocked_runtime_signals")
    if not isinstance(runtime_partial_signals, list):
        runtime_partial_signals = [
            row for row in payload.get("signals", []) if isinstance(row, dict) and row.get("status") != "PASS"
        ]
    tracker_inventory = summary.get("top_tracker_time_inventory_candidates")
    if not isinstance(tracker_inventory, list):
        tracker_inventory = tracker_replay_summary.get("top_inventory_candidates")
        if not isinstance(tracker_inventory, list):
            tracker_inventory = []
    runtime_inventory = summary.get("top_runtime_inventory_candidates")
    if not isinstance(runtime_inventory, list):
        runtime_inventory = []

    pass_count = _score_signal_rows(
        scores,
        pass_replay_signals,
        key="replay_pass_score",
        reason="tracker_time_replay_pass_signal_for_poll_ranking_only",
        base=80.0,
    )
    replay_partial_count = _score_signal_rows(
        scores,
        blocked_replay_signals,
        key="replay_partial_score",
        reason="tracker_time_blocked_signal_for_poll_ranking_only",
        base=22.0,
        include_opposing=True,
    )
    runtime_partial_count = _score_signal_rows(
        scores,
        runtime_partial_signals,
        key="current_partial_score",
        reason="runtime_blocked_signal_for_poll_ranking_only",
        base=45.0,
        include_opposing=True,
    )
    replay_inventory_count = _score_inventory_rows(
        scores,
        tracker_inventory,
        key="inventory_replay_score",
        reason="tracker_time_inventory_candidate_for_poll_ranking_only",
        base=55.0,
    )
    runtime_inventory_count = _score_inventory_rows(
        scores,
        runtime_inventory,
        key="inventory_current_score",
        reason="runtime_inventory_candidate_for_poll_ranking_only",
        base=70.0,
    )
    return scores, {
        "adaptive_state_present": True,
        "adaptive_status": payload.get("status"),
        "wallets": len(scores),
        "replay_pass_signals_scored": pass_count,
        "replay_partial_signals_scored": replay_partial_count,
        "runtime_partial_signals_scored": runtime_partial_count,
        "replay_inventory_candidates_scored": replay_inventory_count,
        "runtime_inventory_candidates_scored": runtime_inventory_count,
        "role": "poll_ranking_only_not_live_admission_truth",
    }


def _hotlane_tick_evidence_wallet_scores(path: str | Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}, {"hotlane_tick_state_present": False}
    scores: dict[str, dict[str, Any]] = {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    latest_tracker = summary.get("latest_tracker") if isinstance(summary.get("latest_tracker"), dict) else {}
    ticks = payload.get("ticks") if isinstance(payload.get("ticks"), list) else []
    runtime_partial_count = 0
    tracker_partial_count = 0
    for tick in ticks[-20:]:
        tracker = tick.get("tracker") if isinstance(tick, dict) and isinstance(tick.get("tracker"), dict) else {}
        runtime_partial_count += _score_signal_rows(
            scores,
            tracker.get("hot_path_top_blocked_runtime_signals") if isinstance(tracker.get("hot_path_top_blocked_runtime_signals"), list) else [],
            key="current_partial_score",
            reason="hotlane_tick_runtime_partial_for_next_poll_ranking_only",
            base=55.0,
            include_opposing=True,
        )
        tracker_partial_count += _score_signal_rows(
            scores,
            tracker.get("hot_path_top_blocked_tracker_time_signals") if isinstance(tracker.get("hot_path_top_blocked_tracker_time_signals"), list) else [],
            key="replay_partial_score",
            reason="hotlane_tick_tracker_time_partial_for_next_poll_ranking_only",
            base=24.0,
            include_opposing=True,
        )
    return scores, {
        "hotlane_tick_state_present": True,
        "hotlane_tick_status": payload.get("status"),
        "wallets": len(scores),
        "ticks_read": len(ticks),
        "runtime_partial_signals_scored": runtime_partial_count,
        "tracker_time_partial_signals_scored": tracker_partial_count,
        "latest_tracker": latest_tracker,
        "zero_current_poll_root_cause": latest_tracker.get("zero_current_poll_root_cause")
        or latest_tracker.get("current_poll_zero_current_poll_root_cause"),
        "current_poll_blockers": latest_tracker.get("current_poll_blockers") or [],
        "current_poll_ladder": latest_tracker.get("current_poll_ladder") or {},
        "current_poll_moves": latest_tracker.get("hot_path_current_poll_moves")
        or latest_tracker.get("current_poll_moves"),
        "new_wallet_events": latest_tracker.get("new_wallet_events"),
        "required_buy_copy_events": latest_tracker.get("required_buy_copy_events"),
        "role": "poll_ranking_only_not_live_admission_truth",
    }


def _merge_score_rows(*sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for source in sources:
        for address, row in source.items():
            target = merged.setdefault(address, {"address": address})
            target.update(row)
    for row in merged.values():
        row["score"] = round(
            float(row.get("live_score") or 0.0)
            + float(row.get("history_score") or 0.0)
            + float(row.get("leaderboard_score") or 0.0)
            + float(row.get("profit_score") or 0.0)
            + float(row.get("strategy_direction_score") or 0.0)
            + float(row.get("mission_anchor_score") or 0.0)
            + float(row.get("forward_queue_score") or 0.0)
            + float(row.get("runtime_copy_proof_score") or 0.0)
            + float(row.get("coactivity_score") or 0.0)
            + float(row.get("current_pass_score") or 0.0)
            + float(row.get("current_partial_score") or 0.0)
            + float(row.get("replay_pass_score") or 0.0)
            + float(row.get("replay_partial_score") or 0.0)
            + float(row.get("inventory_current_score") or 0.0)
            + float(row.get("inventory_replay_score") or 0.0),
            6,
        )
        reasons: list[str] = []
        for key in (
            "live_score",
            "history_score",
            "leaderboard_score",
            "profit_score",
            "strategy_direction_score",
            "mission_anchor_score",
            "forward_queue_score",
            "runtime_copy_proof_score",
            "coactivity_score",
            "current_pass_score",
            "current_partial_score",
            "replay_pass_score",
            "replay_partial_score",
            "inventory_current_score",
            "inventory_replay_score",
        ):
            if float(row.get(key) or 0.0) > 0:
                reasons.append(key)
        row["selection_reasons"] = reasons
    return merged


def _has_current_poll_basis(row: dict[str, Any]) -> bool:
    return any(
        float(row.get(key) or 0.0) > 0.0
        for key in ("live_score", "runtime_copy_proof_score", "current_pass_score", "current_partial_score")
    )


def _source_route_partial_requires_freshness_guard(summary: dict[str, Any]) -> bool:
    blockers = {str(item) for item in summary.get("current_poll_blockers") or []}
    root_cause = summary.get("zero_current_poll_root_cause") or summary.get("current_poll_zero_current_poll_root_cause")
    if root_cause != "source_route_partial" and "current_poll_source_route_partial" not in blockers:
        return False
    ladder = summary.get("current_poll_ladder") if isinstance(summary.get("current_poll_ladder"), dict) else {}
    has_fresh_buys = int(ladder.get("fresh_buy_rows_le_10s") or 0) > 0
    has_moves = int(summary.get("current_poll_moves") or 0) > 0 or int(summary.get("new_wallet_events") or 0) > 0
    has_required = int(summary.get("required_buy_copy_events") or 0) > 0
    return not (has_fresh_buys or has_moves or has_required)


def _has_source_partial_freshness_basis(row: dict[str, Any], *, recent_window_s: float) -> bool:
    if _has_current_poll_basis(row):
        return True
    try:
        lag_s = float(row.get("latest_live_event_lag_s"))
    except (TypeError, ValueError):
        return False
    return lag_s <= max(0.0, float(recent_window_s))


def _mission_anchor_wallet_scores(specs: list[WalletSpec]) -> dict[str, dict[str, Any]]:
    """Keep operator-designated mission wallets in the active polling lane.

    This is measurement pressure only. It does not make a stale or losing wallet
    admissible; it prevents the first mission wallet from disappearing from
    current-poll evidence just because a transient score-ranked cohort filled
    the small active hot-lane.
    """

    scores: dict[str, dict[str, Any]] = {}
    for spec in specs:
        address = spec.normalized_address()
        tags = {str(tag).lower() for tag in spec.tags}
        name = str(spec.name or "").lower()
        legacy_anchor = bool(tags.intersection(LEGACY_MISSION_ANCHOR_NAMES) or name in LEGACY_MISSION_ANCHOR_NAMES)
        anchor_tags = sorted(tags.intersection(MISSION_ANCHOR_TAGS))
        if not legacy_anchor and not anchor_tags:
            continue
        reason = "weird_peak_must_remain_current_poll_measured"
        if anchor_tags:
            reason = f"{anchor_tags[0]}_must_remain_current_poll_measured"
        scores[address] = {
            "mission_anchor": True,
            "mission_anchor_score": 500.0,
            "mission_anchor_reason": reason,
            "mission_anchor_tags": anchor_tags or ["weird_peak"],
        }
    return scores


def _strategy_direction_wallet_scores(path: str | Path) -> dict[str, dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    directions = payload.get("directions") if isinstance(payload.get("directions"), list) else []
    program_review = (
        payload.get("development_program_review")
        if isinstance(payload.get("development_program_review"), dict)
        else {}
    )
    full_rethink_required = bool(program_review.get("full_rethink_required"))
    next_major_change_action = str(program_review.get("next_major_change_action") or "")
    scores: dict[str, dict[str, Any]] = {}
    score_by_direction = {
        "profitable_wallet_copy_efficiency": 800.0,
        "wr_repair_single_wallet": 650.0,
        "single_wallet_best_copyable": 450.0,
    }
    for index, row in enumerate(directions):
        if not isinstance(row, dict):
            continue
        direction_id = str(row.get("id") or "")
        base_score = score_by_direction.get(direction_id)
        if base_score is None:
            continue
        address = str(row.get("wallet") or "").lower()
        if not address:
            continue
        entry = scores.setdefault(address, {"address": address})
        try:
            rank_score = float(row.get("rank_score") or 0.0)
        except (TypeError, ValueError):
            rank_score = 0.0
        direction_score = base_score + min(max(rank_score, 0.0), 2500.0) * 0.08 - index * 5.0
        if full_rethink_required and direction_id in {
            "profitable_wallet_copy_efficiency",
            "wr_repair_single_wallet",
            "single_wallet_best_copyable",
        }:
            direction_score += 500.0
            entry["development_program_bridge_focus"] = True
            entry["development_program_next_major_change_action"] = next_major_change_action
            entry["development_program_stop_doing"] = (
                program_review.get("stop_doing") if isinstance(program_review.get("stop_doing"), list) else []
            )
        entry["strategy_direction_focus"] = True
        entry["strategy_direction_id"] = direction_id
        entry["strategy_direction_status"] = row.get("status")
        entry["strategy_direction_blockers"] = row.get("blockers") if isinstance(row.get("blockers"), list) else []
        entry["strategy_direction_score"] = max(
            float(entry.get("strategy_direction_score") or 0.0),
            round(direction_score, 6),
        )
        paper = row.get("paper") if isinstance(row.get("paper"), dict) else {}
        if paper:
            entry["strategy_direction_paper_resolved_orders"] = paper.get("resolved_orders")
            entry["strategy_direction_paper_roi_pct"] = paper.get("roi_pct")
            entry["strategy_direction_paper_wr_pct"] = paper.get("wr_pct")
            entry["strategy_direction_validation_wr_pct"] = paper.get("validation_wr_pct")
    return scores


def _ordered_hotlane_registry_payload(specs: list[WalletSpec], path: str | Path) -> dict[str, Any]:
    seen: set[str] = set()
    ordered: list[WalletSpec] = []
    for spec in specs:
        address = spec.normalized_address()
        if address in seen:
            continue
        seen.add(address)
        ordered.append(spec)
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_registry",
        "generated_at": utc_now_iso(),
        "order_basis": "active_hotlane_coactivity_priority",
        "wallets": [spec.asdict() for spec in ordered],
    }
    atomic_write_json(path, payload)
    return payload


def _coactivity_adjacent_order(
    selected_addresses: list[str],
    adjacency_edges: list[dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]]]:
    """Place strongest coactive pairs next to each other for small tick slices."""

    selected_set = {str(address).lower() for address in selected_addresses}
    used: set[str] = set()
    ordered: list[str] = []
    adjacency_pairs: list[dict[str, Any]] = []
    original_index = {address: index for index, address in enumerate(selected_addresses)}
    for edge in adjacency_edges:
        edge_wallets = [
            str(wallet).lower()
            for wallet in edge.get("wallets") or []
            if str(wallet).lower() in selected_set
        ]
        if len(edge_wallets) < 2:
            continue
        pair = [wallet for wallet in selected_addresses if wallet in set(edge_wallets[:2])]
        if len(pair) < 2 or any(wallet in used for wallet in pair):
            continue
        ordered.extend(pair)
        used.update(pair)
        adjacency_pairs.append(
            {
                "wallets": pair,
                "source_edge_wallets": edge_wallets[:2],
                "coactive_markets": edge.get("coactive_markets"),
                "coactivity_events": edge.get("coactivity_events"),
                "coactivity_usdc": edge.get("coactivity_usdc"),
                "coactivity_score": edge.get("coactivity_score"),
                "tracker_time_inventory_score": edge.get("tracker_time_inventory_score"),
                "latest_lag_s": edge.get("latest_lag_s"),
                "source": edge.get("source") or "live_coactivity",
                "candidate_id": edge.get("candidate_id"),
                "market_slug": edge.get("market_slug"),
                "outcome": edge.get("outcome"),
                "original_positions": [original_index.get(wallet) for wallet in pair],
            }
        )
    ordered.extend(address for address in selected_addresses if address not in used)
    return ordered, adjacency_pairs


def _hotlane_spec(spec: WalletSpec, row: dict[str, Any]) -> WalletSpec:
    tags = tuple(dict.fromkeys([*spec.tags, "active_hotlane", *row.get("selection_reasons", [])]))
    notes = spec.notes
    note = (
        f"Active hot-lane selected score={row.get('score')} "
        f"latest_live_lag={row.get('latest_live_event_lag_s')} "
        f"latest_history_lag={row.get('latest_history_event_lag_s')}"
    )
    notes = f"{notes}\n{note}" if notes else note
    return WalletSpec(
        name=spec.name,
        address=spec.address,
        enabled=spec.enabled,
        data_api=spec.data_api,
        market_filter=spec.market_filter,
        asset_allowlist=tuple(spec.asset_allowlist),
        tags=tags,
        notes=notes,
    )


def build_active_hotlane(config: ActiveHotlaneConfig | None = None, *, now_ts: float | None = None) -> dict[str, Any]:
    cfg = config or ActiveHotlaneConfig()
    observed_now = time.time() if now_ts is None else float(now_ts)
    registry_specs = [spec for spec in load_wallet_registry(cfg.registry_path) if spec.enabled]
    spec_by_address = {spec.normalized_address(): spec for spec in registry_specs}
    live_rows = _read_jsonl_tail(cfg.live_tracking_event_log_path, cfg.live_log_tail_rows)
    coactivity_scores, coactivity_groups, coactivity_edges = _live_coactivity_scores(
        live_rows,
        now_ts=observed_now,
        recent_window_s=cfg.recent_window_s,
    )
    current_scores, current_hot_path_cohorts, current_hot_path_summary = _current_hot_path_wallet_scores(
        cfg.active_hotlane_live_tracking_state_path
    )
    adaptive_scores, adaptive_evidence_summary = _adaptive_evidence_wallet_scores(cfg.adaptive_state_path)
    tracker_time_inventory_cohorts, tracker_time_inventory_summary = _tracker_time_inventory_candidate_rows(
        cfg.adaptive_state_path
    )
    development_program_bridge_cohorts, development_program_bridge_summary = _development_program_bridge_cohort_rows(
        cfg.profit_state_path,
        slice_size=cfg.development_bridge_slice_wallets,
    )
    tick_scores, tick_evidence_summary = _hotlane_tick_evidence_wallet_scores(cfg.hotlane_tick_state_path)
    score_rows = _merge_score_rows(
        _live_wallet_scores(live_rows, now_ts=observed_now, recent_window_s=cfg.recent_window_s),
        _history_wallet_scores(cfg.history_state_path, now_ts=observed_now, max_age_s=cfg.max_history_age_s),
        _leaderboard_wallet_scores(cfg.leaderboard_state_path),
        _profit_wallet_scores(cfg.profit_state_path),
        _strategy_direction_wallet_scores(cfg.strategy_direction_state_path),
        _mission_anchor_wallet_scores(registry_specs),
        coactivity_scores,
        current_scores,
        adaptive_scores,
        tick_scores,
    )
    ranked = sorted(
        (
            row
            for address, row in score_rows.items()
            if address in spec_by_address and float(row.get("score") or 0.0) >= float(cfg.min_score)
        ),
        key=lambda row: (
            -float(row.get("forward_queue_score") or 0.0),
            -float(row.get("runtime_copy_proof_score") or 0.0),
            -float(row.get("current_pass_score") or 0.0),
            -float(row.get("development_program_bridge_focus") is True),
            -float(float(row.get("live_score") or 0.0) > 0.0),
            -float(row.get("mission_anchor_score") or 0.0),
            -float(row.get("current_partial_score") or 0.0),
            -float(row.get("replay_pass_score") or 0.0),
            -float(row.get("inventory_current_score") or 0.0),
            -float(row.get("inventory_replay_score") or 0.0),
            -float(row.get("live_score") or 0.0),
            -float(row.get("coactivity_score") or 0.0),
            -float(row.get("strategy_direction_score") or 0.0),
            -float(row.get("profit_candidate") is True),
            -float(row.get("score") or 0.0),
            str(row.get("address") or ""),
        ),
    )
    ranked_by_address = {str(row.get("address") or "").lower(): row for row in ranked}
    current_hot_path_has_poll_basis = any(
        int(current_hot_path_summary.get(key) or 0) > 0
        for key in (
            "wallets",
            "cohorts",
            "pass_signals_scored",
            "partial_signals_scored",
            "inventory_candidates_scored",
        )
    ) or bool(current_scores) or bool(coactivity_scores)
    current_poll_dedupe_exhausted = (
        current_hot_path_summary.get("zero_current_poll_root_cause") == "dedupe_exhausted"
        or "all_source_rows_deduped_or_deferred" in set(current_hot_path_summary.get("current_poll_blockers") or [])
    )
    ranked_fallback_requires_current_poll_basis = (
        current_poll_dedupe_exhausted and current_hot_path_has_poll_basis
    )
    source_route_partial_freshness_guard = _source_route_partial_requires_freshness_guard(
        current_hot_path_summary
    ) or _source_route_partial_requires_freshness_guard(tick_evidence_summary)
    selected_addresses: list[str] = []
    selected_cohorts: list[dict[str, Any]] = []
    selected_current_hot_path_cohorts: list[dict[str, Any]] = []
    selected_development_program_bridge_cohorts: list[dict[str, Any]] = []
    selected_recent_live_activity_cohorts: list[dict[str, Any]] = []
    skipped_tracker_time_inventory_cohorts: list[dict[str, Any]] = []
    for row in ranked:
        if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
            break
        address = str(row.get("address") or "").lower()
        if address and row.get("mission_anchor") is True and address not in selected_addresses:
            selected_addresses.append(address)
    recent_live_activity_cohorts, recent_live_activity_summary = _recent_live_activity_cohort_rows(
        ranked,
        recent_window_s=cfg.recent_window_s,
        slice_size=cfg.development_bridge_slice_wallets,
    )
    for cohort in recent_live_activity_cohorts:
        cohort_wallets = [
            wallet
            for wallet in cohort.get("wallets") or []
            if wallet in ranked_by_address
        ]
        if len(cohort_wallets) < 2:
            continue
        ordered_group_wallets = sorted(
            cohort_wallets,
            key=lambda wallet: (
                float(ranked_by_address[wallet].get("latest_live_event_lag_s") or cfg.recent_window_s),
                -float(ranked_by_address[wallet].get("live_score") or 0.0),
                -float(ranked_by_address[wallet].get("score") or 0.0),
                wallet,
            ),
        )
        added = []
        for wallet in ordered_group_wallets:
            if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
                break
            if wallet not in selected_addresses:
                selected_addresses.append(wallet)
                added.append(wallet)
        selected_in_cohort = [wallet for wallet in ordered_group_wallets if wallet in selected_addresses]
        if len(selected_in_cohort) >= 2:
            selected = {
                **cohort,
                "selected_wallets": selected_in_cohort,
            }
            selected_cohorts.append(selected)
            selected_recent_live_activity_cohorts.append(selected)
        if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
            break
    for row in ranked:
        if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
            break
        address = str(row.get("address") or "").lower()
        if address and row.get("strategy_direction_focus") is True and address not in selected_addresses:
            selected_addresses.append(address)
    for cohort in current_hot_path_cohorts:
        cohort_wallets = [
            wallet
            for wallet in cohort.get("wallets") or []
            if wallet in ranked_by_address
        ]
        if len(cohort_wallets) < 2:
            continue
        ordered_group_wallets = sorted(
            cohort_wallets,
            key=lambda wallet: (
                -float(ranked_by_address[wallet].get("current_pass_score") or 0.0),
                -float(ranked_by_address[wallet].get("inventory_current_score") or 0.0),
                -float(ranked_by_address[wallet].get("live_score") or 0.0),
                -float(ranked_by_address[wallet].get("score") or 0.0),
                wallet,
            ),
        )
        added = []
        for wallet in ordered_group_wallets:
            if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
                break
            if wallet not in selected_addresses:
                selected_addresses.append(wallet)
                added.append(wallet)
        selected_in_cohort = [wallet for wallet in ordered_group_wallets if wallet in selected_addresses]
        if len(selected_in_cohort) >= 2:
            selected = {
                **cohort,
                "selected_wallets": selected_in_cohort,
            }
            selected_cohorts.append(selected)
            selected_current_hot_path_cohorts.append(selected)
        if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
            break
    for cohort in development_program_bridge_cohorts:
        cohort_wallets = [
            wallet
            for wallet in cohort.get("wallets") or []
            if wallet in ranked_by_address
        ]
        if len(cohort_wallets) < 2:
            continue
        ordered_group_wallets = sorted(
            cohort_wallets,
            key=lambda wallet: (
                -float(ranked_by_address[wallet].get("development_program_bridge_focus") is True),
                -float(ranked_by_address[wallet].get("forward_queue_score") or 0.0),
                -float(ranked_by_address[wallet].get("strategy_direction_score") or 0.0),
                -float(ranked_by_address[wallet].get("score") or 0.0),
                wallet,
            ),
        )
        added = []
        for wallet in ordered_group_wallets:
            if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
                break
            if wallet not in selected_addresses:
                selected_addresses.append(wallet)
                added.append(wallet)
        selected_in_cohort = [wallet for wallet in ordered_group_wallets if wallet in selected_addresses]
        if len(selected_in_cohort) >= 2:
            selected = {
                **cohort,
                "selected_wallets": selected_in_cohort,
            }
            selected_cohorts.append(selected)
            selected_development_program_bridge_cohorts.append(selected)
        if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
            break
    for group in coactivity_groups:
        group_wallets = [
            wallet
            for wallet in group.get("wallets") or []
            if wallet in ranked_by_address
        ]
        if len(group_wallets) < 2:
            continue
        ordered_group_wallets = sorted(
            group_wallets,
            key=lambda wallet: (
                -float(ranked_by_address[wallet].get("coactivity_score") or 0.0),
                -float(ranked_by_address[wallet].get("live_score") or 0.0),
                -float(ranked_by_address[wallet].get("score") or 0.0),
                wallet,
            ),
        )
        added = []
        for wallet in ordered_group_wallets:
            if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
                break
            if wallet not in selected_addresses:
                selected_addresses.append(wallet)
                added.append(wallet)
        if len(added) >= 2:
            selected_cohorts.append({**group, "selected_wallets": added})
        if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
            break
    selected_tracker_time_inventory_cohorts: list[dict[str, Any]] = []
    for cohort in tracker_time_inventory_cohorts:
        cohort_wallets = [
            wallet
            for wallet in cohort.get("wallets") or []
            if wallet in ranked_by_address
        ]
        if ranked_fallback_requires_current_poll_basis:
            current_poll_wallets = [wallet for wallet in cohort_wallets if _has_current_poll_basis(ranked_by_address[wallet])]
            if len(current_poll_wallets) < 2:
                skipped_tracker_time_inventory_cohorts.append(
                    {
                        **cohort,
                        "skip_reason": "dedupe_exhausted_without_current_poll_basis",
                        "original_wallets": cohort_wallets,
                        "current_poll_basis_wallets": current_poll_wallets,
                    }
                )
                continue
            cohort_wallets = current_poll_wallets
        if len(cohort_wallets) < 2:
            continue
        ordered_group_wallets = sorted(
            cohort_wallets,
            key=lambda wallet: (
                -float(ranked_by_address[wallet].get("inventory_replay_score") or 0.0),
                -float(ranked_by_address[wallet].get("inventory_current_score") or 0.0),
                -float(ranked_by_address[wallet].get("replay_pass_score") or 0.0),
                -float(ranked_by_address[wallet].get("score") or 0.0),
                wallet,
            ),
        )
        added = []
        for wallet in ordered_group_wallets:
            if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
                break
            if wallet not in selected_addresses:
                selected_addresses.append(wallet)
                added.append(wallet)
        selected_in_cohort = [wallet for wallet in ordered_group_wallets if wallet in selected_addresses]
        if len(selected_in_cohort) >= 2:
            selected = {
                **cohort,
                "selected_wallets": selected_in_cohort,
                "selected_source": "tracker_time_inventory_candidate_for_poll_only",
            }
            selected_cohorts.append(selected)
            selected_tracker_time_inventory_cohorts.append(selected)
        if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
            break
    for row in ranked:
        if len(selected_addresses) >= max(0, int(cfg.max_wallets)):
            break
        address = str(row.get("address") or "").lower()
        if (
            ranked_fallback_requires_current_poll_basis
            and not _has_current_poll_basis(row)
            and row.get("mission_anchor") is not True
            and row.get("strategy_direction_focus") is not True
        ):
            continue
        if (
            source_route_partial_freshness_guard
            and not _has_source_partial_freshness_basis(row, recent_window_s=cfg.recent_window_s)
            and row.get("mission_anchor") is not True
            and row.get("strategy_direction_focus") is not True
        ):
            continue
        if address and address not in selected_addresses:
            selected_addresses.append(address)
    tracker_time_adjacency_edges = [
        {
            **cohort,
            "wallets": cohort.get("selected_wallets") or cohort.get("wallets") or [],
            "coactivity_score": cohort.get("tracker_time_inventory_score"),
        }
        for cohort in selected_tracker_time_inventory_cohorts
    ]
    current_hot_path_adjacency_edges = [
        {
            **cohort,
            "wallets": cohort.get("selected_wallets") or cohort.get("wallets") or [],
            "coactivity_score": cohort.get("top_score_usd") or cohort.get("total_score_usd") or 0.0,
        }
        for cohort in selected_current_hot_path_cohorts
    ]
    selected_addresses, coactivity_adjacency_pairs = _coactivity_adjacent_order(
        selected_addresses,
        [*current_hot_path_adjacency_edges, *coactivity_edges, *tracker_time_adjacency_edges],
    )
    selected_rows = [ranked_by_address[address] for address in selected_addresses if address in ranked_by_address]
    selected_specs = [_hotlane_spec(spec_by_address[str(row["address"])], row) for row in selected_rows]
    if selected_specs:
        _ordered_hotlane_registry_payload(selected_specs, cfg.output_registry_path)
    else:
        _ordered_hotlane_registry_payload([], cfg.output_registry_path)
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_active_hotlane_state",
        "generated_at": utc_now_iso(),
        "status": "PASS" if selected_specs else "WATCH",
        "blockers": [] if selected_specs else ["no_active_hotlane_wallets_selected"],
        "paper_only": True,
        "live_orders_allowed": False,
        "config": cfg.asdict(),
        "summary": {
            "registry_wallets": len(registry_specs),
            "live_log_rows_read": len(live_rows),
            "scored_wallets": len(score_rows),
            "selected_wallets": len(selected_rows),
            "recent_window_s": cfg.recent_window_s,
            "max_wallets": cfg.max_wallets,
            "coactivity_groups": len(coactivity_groups),
            "coactivity_edges": len(coactivity_edges),
            "selected_cohorts": len(selected_cohorts),
            "current_hot_path_cohorts": len(current_hot_path_cohorts),
            "selected_current_hot_path_cohorts": len(selected_current_hot_path_cohorts),
            "recent_live_activity_cohorts": len(recent_live_activity_cohorts),
            "selected_recent_live_activity_cohorts": len(selected_recent_live_activity_cohorts),
            "development_program_bridge_cohorts": len(development_program_bridge_cohorts),
            "selected_development_program_bridge_cohorts": len(selected_development_program_bridge_cohorts),
            "coactivity_adjacency_pairs": len(coactivity_adjacency_pairs),
            "current_hot_path_evidence_wallets": current_hot_path_summary.get("wallets", 0),
            "adaptive_evidence_wallets": adaptive_evidence_summary.get("wallets", 0),
            "tracker_time_inventory_cohorts": len(tracker_time_inventory_cohorts),
            "selected_tracker_time_inventory_cohorts": len(selected_tracker_time_inventory_cohorts),
            "skipped_tracker_time_inventory_cohorts": len(skipped_tracker_time_inventory_cohorts),
            "dedupe_exhausted_current_poll_guard": current_poll_dedupe_exhausted,
            "source_route_partial_freshness_guard": source_route_partial_freshness_guard,
            "hotlane_tick_evidence_wallets": tick_evidence_summary.get("wallets", 0),
        },
        "adaptive_evidence_summary": adaptive_evidence_summary,
        "current_hot_path_summary": current_hot_path_summary,
        "recent_live_activity_summary": recent_live_activity_summary,
        "development_program_bridge_summary": development_program_bridge_summary,
        "tracker_time_inventory_summary": tracker_time_inventory_summary,
        "current_hot_path_cohorts": current_hot_path_cohorts[:20],
        "recent_live_activity_cohorts": recent_live_activity_cohorts[:20],
        "selected_recent_live_activity_cohorts": selected_recent_live_activity_cohorts[:10],
        "hotlane_tick_evidence_summary": tick_evidence_summary,
        "development_program_bridge_cohorts": development_program_bridge_cohorts[:20],
        "selected_development_program_bridge_cohorts": selected_development_program_bridge_cohorts[:10],
        "coactivity_groups": coactivity_groups[:20],
        "tracker_time_inventory_cohorts": tracker_time_inventory_cohorts[:20],
        "skipped_tracker_time_inventory_cohorts": skipped_tracker_time_inventory_cohorts[:20],
        "coactivity_edges": coactivity_edges[:20],
        "coactivity_adjacency_pairs": coactivity_adjacency_pairs[:10],
        "selected_cohorts": selected_cohorts[:10],
        "selection_order_basis": (
            "coactive BTC 5m same-outcome wallets first with strongest coactivity edges kept adjacent for "
            "small tick slices, but fresh current-poll hot-path pass cohorts are pinned first when present; "
            "then full-rethink development bridge cohorts, tracker-time inventory cohorts as explicit "
            "poll-only cohorts, current partial, tracker-time replay, inventory research, and ranked fill; "
            "bridge/replay/inventory cohorts are poll ranking only"
        ),
        "selected_wallets": selected_rows,
        "ranked_wallets": ranked[:100],
        "output_registry": cfg.output_registry_path,
    }
    atomic_write_json(cfg.output_state_path, payload)
    return payload
