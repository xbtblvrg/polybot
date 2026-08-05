"""Adaptive paper-only bot derived from live wallet-copy evidence.

This module is deliberately downstream of wallet-copy tracking. It does not
invent an independent signal path: it consumes wallet-attributed live tracker
rows, requires fresh CLOB-backed copyability evidence, and emits ordinary
CopyIntent objects into the same paper/live lifecycle contract.
"""

from __future__ import annotations

import json
import time
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

from src.wallet_copy.models import CopyIntent, parse_ts, stable_id, utc_now_iso
from src.wallet_copy.paper import PaperExecutionConfig, PaperWalletCopyEngine
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.strategy import outcome_to_side


ADAPTIVE_SOURCE_WALLET = "ADAPTIVE_WALLET_DERIVED"
ADAPTIVE_INVENTORY_SOURCE_WALLET = "ADAPTIVE_WALLET_INVENTORY"
MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS = 10


@dataclass(frozen=True)
class AdaptiveBotConfig:
    live_tracking_state_path: str = "data/research/wallet_copy_live_tracking_state.json"
    live_tracking_event_log_path: str = "data/research/wallet_copy_live_tracking_events.jsonl"
    output_state_path: str = "data/research/wallet_copy_adaptive_bot_state.json"
    paper_state_path: str = "data/research/wallet_copy_adaptive_bot_paper_state.json"
    paper_event_log_path: str = "data/research/wallet_copy_adaptive_bot_paper_events.jsonl"
    single_wallet_exact_copy_paper_state_path: str = (
        "data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_state.json"
    )
    single_wallet_exact_copy_paper_event_log_path: str = (
        "data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_events.jsonl"
    )
    tracker_time_replay_paper_state_path: str = (
        "data/research/wallet_copy_adaptive_tracker_time_replay_paper_state.json"
    )
    tracker_time_replay_paper_event_log_path: str = (
        "data/research/wallet_copy_adaptive_tracker_time_replay_paper_events.jsonl"
    )
    max_event_log_rows: int = 2000
    min_move_generated_at_ts: float | None = None
    max_observed_event_age_s: float = 10.0
    max_observation_age_s: float = 30.0
    min_agreeing_wallets: int = 2
    min_signal_score_usd: float = 0.5
    min_directional_dominance: float = 0.62
    max_price_spread: float = 0.12
    max_signal_cluster_age_s: float = 8.0
    min_clob_fill_ratio: float = 0.999
    max_order_usd: float = 2.0
    min_order_usd: float = 0.1
    min_source_wallet_usd: float = 0.0
    apply_paper: bool = True
    apply_tracker_time_replay_paper: bool = True
    max_tracker_time_replay_intents: int = 200
    apply_single_wallet_exact_copy_paper: bool = True
    max_single_wallet_exact_copy_intents: int = 50

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AdaptiveSignal:
    signal_id: str
    condition_id: str
    market_slug: str
    outcome: str
    side: str
    status: str
    blockers: tuple[str, ...]
    agreeing_wallets: tuple[str, ...]
    opposing_wallets: tuple[str, ...]
    event_count: int
    fresh_event_count: int
    clob_backed_event_count: int
    score_usd: float
    opposing_score_usd: float
    directional_dominance: float
    average_price: float
    min_price: float
    max_price: float
    chosen_token_id: str
    chosen_evidence: dict[str, Any] = field(default_factory=dict)
    source_event_ids: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _read_jsonl_tail(path: str | Path, limit: int) -> list[dict[str, Any]]:
    target = Path(path)
    if limit <= 0 or not target.exists():
        return []
    rows: deque[dict[str, Any]] = deque(maxlen=int(limit))
    with target.open("r", encoding="utf-8") as handle:
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


def _move_timestamp(row: dict[str, Any]) -> float | None:
    generated = parse_ts(row.get("generated_at"))
    if generated is not None:
        return generated
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    return parse_ts(wallet_event.get("observed_ts") or row.get("observed_ts"))


def _move_fingerprint(row: dict[str, Any]) -> str:
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    return str(
        row.get("source_fingerprint")
        or wallet_event.get("source_fingerprint")
        or row.get("source_event_id")
        or wallet_event.get("event_id")
        or stable_id("adrow", row)
    )


def _source_event_timestamp(row: dict[str, Any]) -> float | None:
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    return parse_ts(row.get("event_ts") or wallet_event.get("event_ts")) or _move_timestamp(row)


def load_tracker_moves(config: AdaptiveBotConfig) -> list[dict[str, Any]]:
    state = load_json(config.live_tracking_state_path, default={})
    state_moves = []
    if isinstance(state, dict):
        state_moves = [row for row in state.get("last_moves") or [] if isinstance(row, dict)]
    log_moves = _read_jsonl_tail(config.live_tracking_event_log_path, config.max_event_log_rows)
    unique: dict[str, dict[str, Any]] = {}
    for row in [*log_moves, *state_moves]:
        if not isinstance(row, dict):
            continue
        if config.min_move_generated_at_ts is not None:
            generated_at = _move_timestamp(row)
            if generated_at is None or generated_at < float(config.min_move_generated_at_ts):
                continue
        unique[_move_fingerprint(row)] = row
    return sorted(unique.values(), key=lambda row: (_move_timestamp(row) or 0.0, _move_fingerprint(row)))


def _latest_time_cluster(rows: list[dict[str, Any]], config: AdaptiveBotConfig) -> tuple[list[dict[str, Any]], int]:
    if not rows or config.max_signal_cluster_age_s <= 0:
        return rows, 0
    timestamps = [_source_event_timestamp(row) for row in rows]
    available = [float(ts) for ts in timestamps if ts is not None]
    if not available:
        return rows, 0
    latest_ts = max(available)
    cluster = [
        row
        for row in rows
        if (_source_event_timestamp(row) is None)
        or latest_ts - float(_source_event_timestamp(row) or latest_ts) <= float(config.max_signal_cluster_age_s)
    ]
    return cluster, max(0, len(rows) - len(cluster))


def _avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return round(ordered[index], 6)


def _dynamic_event_age(row: dict[str, Any], now_ts: float) -> float | None:
    event_ts = _source_event_timestamp(row)
    if event_ts is None:
        return None
    return max(0.0, float(now_ts) - float(event_ts))


def _event_age(row: dict[str, Any], now_ts: float) -> float | None:
    dynamic_age = _dynamic_event_age(row, now_ts)
    if dynamic_age is not None:
        return dynamic_age
    ce = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
    if ce.get("event_age_s") is not None:
        try:
            return max(0.0, float(ce.get("event_age_s")))
        except (TypeError, ValueError):
            return None
    return None


def _source_usd(row: dict[str, Any]) -> float:
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    for value in (
        row.get("source_usdc_size"),
        wallet_event.get("usdc_size"),
        (row.get("copy_efficiency") or {}).get("source_usdc_size") if isinstance(row.get("copy_efficiency"), dict) else None,
    ):
        try:
            if value is not None:
                return max(0.0, float(value))
        except (TypeError, ValueError):
            continue
    return 0.0


def _accepted_copyability(row: dict[str, Any]) -> bool:
    copyability = row.get("copyability") if isinstance(row.get("copyability"), dict) else {}
    ce = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
    if copyability.get("accepted") is True or ce.get("copyability_accepted") is True:
        return True
    return str(ce.get("copy_status") or "") == "COPIED_FILLED"


def _clob_ok(row: dict[str, Any], *, min_fill_ratio: float) -> bool:
    ce = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
    tracking = row.get("tracking_evidence") if isinstance(row.get("tracking_evidence"), dict) else {}
    clob = tracking.get("clob_book") if isinstance(tracking.get("clob_book"), dict) else {}
    status = str(ce.get("clob_book_status") or clob.get("status") or "")
    try:
        ratio = float(ce.get("clob_fill_ratio") if ce.get("clob_fill_ratio") is not None else clob.get("fill_ratio"))
    except (TypeError, ValueError):
        ratio = 0.0
    return status == "OK" and ratio >= float(min_fill_ratio)


def _eligible_move(row: dict[str, Any], config: AdaptiveBotConfig, *, now_ts: float) -> tuple[bool, str]:
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    action = str(row.get("action") or wallet_event.get("action") or "").upper()
    if action != "BUY":
        return False, "not_buy"
    market_slug = str(row.get("market_slug") or wallet_event.get("market_slug") or "")
    if "btc-updown-5m" not in market_slug:
        return False, "not_btc_5m"
    source_wallet = str(row.get("source_wallet") or wallet_event.get("source_wallet") or "").lower()
    if not source_wallet or source_wallet == ADAPTIVE_SOURCE_WALLET.lower():
        return False, "invalid_source_wallet"
    if _source_usd(row) < float(config.min_source_wallet_usd):
        return False, "source_size_below_minimum"
    event_age = _event_age(row, now_ts)
    if event_age is None:
        return False, "missing_event_age"
    if event_age > float(config.max_observed_event_age_s):
        return False, "event_age_above_cap"
    observed_at = _move_timestamp(row)
    if observed_at is not None and max(0.0, float(now_ts) - observed_at) > float(config.max_observation_age_s):
        return False, "observation_age_above_cap"
    if not _accepted_copyability(row):
        return False, "copyability_not_accepted"
    if not _clob_ok(row, min_fill_ratio=config.min_clob_fill_ratio):
        return False, "clob_not_fillable"
    return True, "accepted"


def _eligible_move_at_tracker_time(row: dict[str, Any], config: AdaptiveBotConfig) -> tuple[bool, str]:
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    action = str(row.get("action") or wallet_event.get("action") or "").upper()
    if action != "BUY":
        return False, "not_buy"
    market_slug = str(row.get("market_slug") or wallet_event.get("market_slug") or "")
    if "btc-updown-5m" not in market_slug:
        return False, "not_btc_5m"
    source_wallet = str(row.get("source_wallet") or wallet_event.get("source_wallet") or "").lower()
    if not source_wallet or source_wallet == ADAPTIVE_SOURCE_WALLET.lower():
        return False, "invalid_source_wallet"
    if _source_usd(row) < float(config.min_source_wallet_usd):
        return False, "source_size_below_minimum"
    tracker_age = _tracker_event_age(row)
    if tracker_age is None:
        return False, "missing_tracker_event_age"
    if tracker_age > float(config.max_observed_event_age_s):
        return False, "tracker_event_age_above_cap"
    if not _accepted_copyability(row):
        return False, "copyability_not_accepted"
    if not _clob_ok(row, min_fill_ratio=config.min_clob_fill_ratio):
        return False, "clob_not_fillable"
    return True, "accepted"


def _row_price(row: dict[str, Any]) -> float:
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    for value in (
        row.get("source_price"),
        wallet_event.get("price"),
        (row.get("copy_efficiency") or {}).get("source_price") if isinstance(row.get("copy_efficiency"), dict) else None,
    ):
        try:
            if value is not None:
                return max(0.0, float(value))
        except (TypeError, ValueError):
            continue
    return 0.0


def _row_identity(row: dict[str, Any]) -> dict[str, Any]:
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    return {
        "source_wallet": str(row.get("source_wallet") or wallet_event.get("source_wallet") or "").lower(),
        "wallet_name": str(row.get("wallet_name") or wallet_event.get("wallet_name") or ""),
        "source_event_id": str(row.get("source_event_id") or wallet_event.get("event_id") or ""),
        "condition_id": str(row.get("condition_id") or wallet_event.get("condition_id") or ""),
        "market_slug": str(row.get("market_slug") or wallet_event.get("market_slug") or ""),
        "outcome": str(row.get("outcome") or wallet_event.get("outcome") or ""),
        "token_id": str(row.get("token_id") or wallet_event.get("token_id") or ""),
        "event_ts": parse_ts(row.get("event_ts") or wallet_event.get("event_ts")),
        "observed_ts": parse_ts(row.get("observed_ts") or wallet_event.get("observed_ts") or row.get("generated_at")),
    }


def _tracker_event_age(row: dict[str, Any]) -> float | None:
    ce = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
    try:
        if ce.get("event_age_s") is not None:
            return max(0.0, float(ce.get("event_age_s")))
    except (TypeError, ValueError):
        return None
    return None


def _adaptive_freshness_diagnostics(
    moves: list[dict[str, Any]],
    *,
    config: AdaptiveBotConfig,
    now_ts: float,
) -> dict[str, Any]:
    buy_rows: list[dict[str, Any]] = []
    tracker_fresh: list[dict[str, Any]] = []
    runtime_fresh: list[dict[str, Any]] = []
    tracker_fresh_copyable: list[dict[str, Any]] = []
    runtime_fresh_copyable: list[dict[str, Any]] = []
    tracker_fresh_clob_ok: list[dict[str, Any]] = []
    runtime_fresh_clob_ok: list[dict[str, Any]] = []
    runtime_eligible: list[dict[str, Any]] = []
    buy_lags: list[float] = []
    copyable_lags: list[float] = []
    clob_ok_lags: list[float] = []
    reason_by_wallet: dict[str, Counter[str]] = defaultdict(Counter)
    market_outcomes: Counter[str] = Counter()
    runtime_eligible_wallets: set[str] = set()
    transition_rows: list[dict[str, Any]] = []
    transition_counts: Counter[str] = Counter()

    for row in moves:
        identity = _row_identity(row)
        wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
        action = str(row.get("action") or wallet_event.get("action") or "").upper()
        market_slug = identity["market_slug"]
        if action != "BUY" or "btc-updown-5m" not in market_slug:
            continue
        buy_rows.append(row)
        dynamic_age = _dynamic_event_age(row, now_ts)
        tracker_age = _tracker_event_age(row)
        if dynamic_age is not None:
            buy_lags.append(dynamic_age)
        is_tracker_fresh = tracker_age is not None and tracker_age <= float(config.max_observed_event_age_s)
        is_runtime_fresh = dynamic_age is not None and dynamic_age <= float(config.max_observed_event_age_s)
        is_copyable = _accepted_copyability(row)
        is_clob_ok = _clob_ok(row, min_fill_ratio=config.min_clob_fill_ratio)
        ok, reason = _eligible_move(row, config, now_ts=now_ts)
        transition_reason = "runtime_eligible" if ok else reason
        age_delta_s = None
        if tracker_age is not None and dynamic_age is not None:
            age_delta_s = max(0.0, float(dynamic_age) - float(tracker_age))
        tracker_latency_budget_remaining_s = None
        if tracker_age is not None:
            tracker_latency_budget_remaining_s = float(config.max_observed_event_age_s) - float(tracker_age)
        if identity["source_wallet"]:
            reason_by_wallet[identity["source_wallet"]][reason] += 1
        if identity["market_slug"] and identity["outcome"]:
            market_outcomes[f"{identity['market_slug']}|{identity['outcome']}"] += 1
        if is_tracker_fresh or is_runtime_fresh:
            if is_tracker_fresh and not is_runtime_fresh:
                transition_reason = "tracker_fresh_became_runtime_stale"
            elif is_runtime_fresh and not is_copyable:
                transition_reason = "runtime_fresh_copyability_rejected"
            elif is_runtime_fresh and not is_clob_ok:
                transition_reason = "runtime_fresh_clob_not_ok"
            transition_counts[transition_reason] += 1
            if len(transition_rows) < 50:
                transition_rows.append(
                    {
                        "source_wallet": identity["source_wallet"],
                        "wallet_name": identity["wallet_name"],
                        "source_event_id": identity["source_event_id"],
                        "market_slug": identity["market_slug"],
                        "condition_id": identity["condition_id"],
                        "outcome": identity["outcome"],
                        "token_id": identity["token_id"],
                        "event_ts": round(float(identity["event_ts"]), 6) if identity["event_ts"] is not None else None,
                        "observed_ts": (
                            round(float(identity["observed_ts"]), 6)
                            if identity["observed_ts"] is not None
                            else None
                        ),
                        "tracker_event_age_s": round(tracker_age, 6) if tracker_age is not None else None,
                        "runtime_event_age_s": round(dynamic_age, 6) if dynamic_age is not None else None,
                        "runtime_minus_tracker_age_s": round(age_delta_s, 6) if age_delta_s is not None else None,
                        "tracker_latency_budget_remaining_s": (
                            round(tracker_latency_budget_remaining_s, 6)
                            if tracker_latency_budget_remaining_s is not None
                            else None
                        ),
                        "tracker_fresh": is_tracker_fresh,
                        "runtime_fresh": is_runtime_fresh,
                        "copyability_accepted": is_copyable,
                        "clob_ok": is_clob_ok,
                        "runtime_eligible": ok,
                        "runtime_filter_reason": reason,
                        "transition_reason": transition_reason,
                    }
                )
        if is_tracker_fresh:
            tracker_fresh.append(row)
            if is_copyable:
                tracker_fresh_copyable.append(row)
            if is_clob_ok:
                tracker_fresh_clob_ok.append(row)
        if is_runtime_fresh:
            runtime_fresh.append(row)
            if is_copyable:
                runtime_fresh_copyable.append(row)
            if is_clob_ok:
                runtime_fresh_clob_ok.append(row)
        if is_copyable and dynamic_age is not None:
            copyable_lags.append(dynamic_age)
        if is_clob_ok and dynamic_age is not None:
            clob_ok_lags.append(dynamic_age)
        if ok:
            runtime_eligible.append(row)
            if identity["source_wallet"]:
                runtime_eligible_wallets.add(identity["source_wallet"])

    reason_rows = []
    for wallet, counts in reason_by_wallet.items():
        total = sum(counts.values())
        top_reason, top_count = counts.most_common(1)[0]
        reason_rows.append(
            {
                "wallet": wallet,
                "events": total,
                "top_reason": top_reason,
                "top_reason_count": top_count,
                "reason_counts": dict(sorted(counts.items())),
            }
        )
    reason_rows = sorted(reason_rows, key=lambda row: (-int(row["events"]), str(row["wallet"])))[:20]

    return {
        "buy_events": len(buy_rows),
        "tracker_fresh_buy_events_le_cap": len(tracker_fresh),
        "runtime_fresh_buy_events_le_cap": len(runtime_fresh),
        "tracker_fresh_copyability_accepted_buy_events": len(tracker_fresh_copyable),
        "runtime_fresh_copyability_accepted_buy_events": len(runtime_fresh_copyable),
        "tracker_fresh_clob_ok_buy_events": len(tracker_fresh_clob_ok),
        "runtime_fresh_clob_ok_buy_events": len(runtime_fresh_clob_ok),
        "runtime_eligible_buy_events": len(runtime_eligible),
        "runtime_eligible_wallets": len(runtime_eligible_wallets),
        "latest_buy_event_lag_s": round(min(buy_lags), 6) if buy_lags else None,
        "latest_copyability_accepted_buy_lag_s": round(min(copyable_lags), 6) if copyable_lags else None,
        "latest_clob_ok_buy_lag_s": round(min(clob_ok_lags), 6) if clob_ok_lags else None,
        "tracker_fresh_but_runtime_stale": bool(tracker_fresh and not runtime_fresh),
        "source_feed_delayed": bool(buy_lags and min(buy_lags) > float(config.max_observed_event_age_s)),
        "max_observed_event_age_s": float(config.max_observed_event_age_s),
        "freshness_transition_counts": dict(sorted(transition_counts.items())),
        "freshness_transition_rows": transition_rows,
        "top_filter_reasons_by_wallet": reason_rows,
        "top_market_outcome_rows": [
            {"market_outcome": key, "events": value}
            for key, value in market_outcomes.most_common(20)
        ],
    }


def _signals_from_eligible_rows(
    eligible: list[dict[str, Any]],
    *,
    config: AdaptiveBotConfig,
) -> tuple[list[AdaptiveSignal], int]:
    by_market: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in eligible:
        identity = _row_identity(row)
        market_key = identity["condition_id"] or identity["market_slug"]
        if not market_key or not identity["outcome"]:
            continue
        by_market[market_key][identity["outcome"]].append(row)

    signals: list[AdaptiveSignal] = []
    clustered_out_moves = 0
    for market_key, outcomes in sorted(by_market.items()):
        clustered_outcomes: dict[str, list[dict[str, Any]]] = {}
        for outcome, rows in outcomes.items():
            cluster, dropped = _latest_time_cluster(rows, config)
            clustered_outcomes[outcome] = cluster
            clustered_out_moves += dropped
        outcomes = clustered_outcomes
        outcome_scores: dict[str, float] = {}
        for outcome, rows in outcomes.items():
            wallets_seen: set[str] = set()
            score = 0.0
            for row in rows:
                identity = _row_identity(row)
                wallet = identity["source_wallet"]
                if not wallet:
                    continue
                wallet_usd = min(float(config.max_order_usd), _source_usd(row))
                if wallet not in wallets_seen:
                    score += max(0.0, wallet_usd)
                    wallets_seen.add(wallet)
            outcome_scores[outcome] = round(score, 6)
        total_market_score = sum(outcome_scores.values())
        for outcome, rows in sorted(outcomes.items()):
            if not rows:
                continue
            identities = [_row_identity(row) for row in rows]
            wallets = sorted({item["source_wallet"] for item in identities if item["source_wallet"]})
            prices = [_row_price(row) for row in rows if _row_price(row) > 0]
            token_counts = Counter(item["token_id"] for item in identities if item["token_id"])
            chosen_token_id = token_counts.most_common(1)[0][0] if token_counts else ""
            chosen = max(
                rows,
                key=lambda row: (
                    (_move_timestamp(row) or 0.0),
                    _source_usd(row),
                ),
            )
            score = outcome_scores.get(outcome, 0.0)
            opposing_score = round(total_market_score - score, 6)
            dominance = round(score / total_market_score, 6) if total_market_score > 0 else 0.0
            blockers: list[str] = []
            if len(wallets) < int(config.min_agreeing_wallets):
                blockers.append("insufficient_agreeing_wallets")
            if score < float(config.min_signal_score_usd):
                blockers.append("signal_score_below_minimum")
            if dominance < float(config.min_directional_dominance):
                blockers.append("directional_dominance_below_minimum")
            if prices and max(prices) - min(prices) > float(config.max_price_spread):
                blockers.append("price_spread_too_wide")
            if not chosen_token_id:
                blockers.append("missing_token_id")
            status = "PASS" if not blockers else "BLOCKED"
            signal_id = stable_id(
                "ads",
                {
                    "market": market_key,
                    "outcome": outcome,
                    "wallets": wallets,
                    "source_events": sorted(item["source_event_id"] for item in identities),
                    "policy": "adaptive_wallet_derived_consensus_v1",
                },
            )
            signals.append(
                AdaptiveSignal(
                    signal_id=signal_id,
                    condition_id=identities[0]["condition_id"] or market_key,
                    market_slug=identities[0]["market_slug"],
                    outcome=outcome,
                    side=outcome_to_side(outcome),
                    status=status,
                    blockers=tuple(blockers),
                    agreeing_wallets=tuple(wallets),
                    opposing_wallets=tuple(
                        sorted(
                            {
                                _row_identity(row)["source_wallet"]
                                for other_outcome, other_rows in outcomes.items()
                                if other_outcome != outcome
                                for row in other_rows
                                if _row_identity(row)["source_wallet"]
                            }
                        )
                    ),
                    event_count=len(rows),
                    fresh_event_count=len(rows),
                    clob_backed_event_count=len(rows),
                    score_usd=round(score, 6),
                    opposing_score_usd=round(opposing_score, 6),
                    directional_dominance=dominance,
                    average_price=round(sum(prices) / len(prices), 6) if prices else 0.0,
                    min_price=round(min(prices), 6) if prices else 0.0,
                    max_price=round(max(prices), 6) if prices else 0.0,
                    chosen_token_id=chosen_token_id,
                    chosen_evidence=chosen.get("tracking_evidence") if isinstance(chosen.get("tracking_evidence"), dict) else {},
                    source_event_ids=tuple(sorted(item["source_event_id"] for item in identities)),
                    metadata={
                        "policy_id": "adaptive_wallet_derived_consensus_v1",
                        "market_total_score_usd": round(total_market_score, 6),
                        "outcome_scores_usd": outcome_scores,
                        "max_signal_cluster_age_s": config.max_signal_cluster_age_s,
                        "source_fingerprints": sorted(_move_fingerprint(row) for row in rows),
                    },
                )
            )

    return (
        sorted(signals, key=lambda signal: (signal.status != "PASS", -signal.score_usd, signal.market_slug, signal.outcome)),
        clustered_out_moves,
    )


def _inventory_candidates_from_eligible_rows(
    eligible: list[dict[str, Any]],
    *,
    config: AdaptiveBotConfig,
) -> list[dict[str, Any]]:
    by_market: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in eligible:
        identity = _row_identity(row)
        market_key = identity["condition_id"] or identity["market_slug"]
        if not market_key or not identity["outcome"]:
            continue
        by_market[market_key][identity["outcome"]].append(row)

    candidates: list[dict[str, Any]] = []
    for market_key, outcomes in sorted(by_market.items()):
        clustered: dict[str, list[dict[str, Any]]] = {}
        clustered_out_moves = 0
        for outcome, rows in outcomes.items():
            cluster, dropped = _latest_time_cluster(rows, config)
            clustered[outcome] = cluster
            clustered_out_moves += dropped
        outcome_scores: dict[str, float] = {}
        outcome_wallets: dict[str, list[str]] = {}
        outcome_event_counts: dict[str, int] = {}
        outcome_prices: dict[str, list[float]] = {}
        for outcome, rows in clustered.items():
            wallets_seen: set[str] = set()
            score = 0.0
            prices: list[float] = []
            for row in rows:
                identity = _row_identity(row)
                wallet = identity["source_wallet"]
                if not wallet:
                    continue
                if wallet not in wallets_seen:
                    score += max(0.0, min(float(config.max_order_usd), _source_usd(row)))
                    wallets_seen.add(wallet)
                price = _row_price(row)
                if price > 0:
                    prices.append(price)
            outcome_scores[outcome] = round(score, 6)
            outcome_wallets[outcome] = sorted(wallets_seen)
            outcome_event_counts[outcome] = len(rows)
            outcome_prices[outcome] = prices
        total_score = round(sum(outcome_scores.values()), 6)
        unique_wallets = sorted({wallet for wallets in outcome_wallets.values() for wallet in wallets})
        active_outcomes = [outcome for outcome, score in outcome_scores.items() if score > 0]
        if not active_outcomes:
            continue
        ranked = sorted(active_outcomes, key=lambda outcome: outcome_scores.get(outcome, 0.0), reverse=True)
        top_outcome = ranked[0]
        top_score = outcome_scores.get(top_outcome, 0.0)
        second_score = outcome_scores.get(ranked[1], 0.0) if len(ranked) > 1 else 0.0
        dominance = round(top_score / total_score, 6) if total_score > 0 else 0.0
        balance_ratio = round(second_score / top_score, 6) if top_score > 0 and second_score > 0 else 0.0
        mode = "one_sided_directional"
        if len(active_outcomes) >= 2:
            mode = "two_sided_balanced_inventory" if balance_ratio >= 0.75 else "two_sided_biased_inventory"
        blockers: list[str] = []
        if len(unique_wallets) < int(config.min_agreeing_wallets):
            blockers.append("insufficient_distinct_wallets")
        if total_score < float(config.min_signal_score_usd):
            blockers.append("market_score_below_minimum")
        prices = [price for values in outcome_prices.values() for price in values]
        if prices and max(prices) - min(prices) > max(0.40, float(config.max_price_spread) * 4.0):
            blockers.append("inventory_cross_outcome_price_range_too_wide")
        status = "RESEARCH_CANDIDATE" if not blockers else "BLOCKED"
        candidates.append(
            {
                "candidate_id": stable_id(
                    "aic",
                    {
                        "market": market_key,
                        "outcome_scores": outcome_scores,
                        "wallets": unique_wallets,
                        "policy": "adaptive_wallet_inventory_research_v1",
                    },
                ),
                "status": status,
                "blockers": blockers,
                "mode": mode,
                "condition_id": market_key,
                "market_slug": next(
                    (
                        _row_identity(row)["market_slug"]
                        for rows in clustered.values()
                        for row in rows
                        if _row_identity(row)["market_slug"]
                    ),
                    "",
                ),
                "top_outcome": top_outcome,
                "dominance": dominance,
                "balance_ratio": balance_ratio,
                "total_score_usd": total_score,
                "top_score_usd": round(top_score, 6),
                "second_score_usd": round(second_score, 6),
                "outcome_scores_usd": outcome_scores,
                "outcome_wallets": outcome_wallets,
                "outcome_event_counts": outcome_event_counts,
                "unique_wallets": unique_wallets,
                "clustered_out_moves": clustered_out_moves,
                "policy_id": "adaptive_wallet_inventory_research_v1",
                "live_admission_role": "research_only_until_runtime_fresh_paper_lifecycle_and_profit_validation_pass",
            }
        )
    return sorted(
        candidates,
        key=lambda row: (
            row.get("status") != "RESEARCH_CANDIDATE",
            -float(row.get("total_score_usd") or 0.0),
            str(row.get("market_slug") or ""),
        ),
    )


def _signal_blocker_counts(signals: list[AdaptiveSignal]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for signal in signals:
        if signal.status == "PASS":
            continue
        if not signal.blockers:
            counts["blocked_without_reason"] += 1
        for blocker in signal.blockers:
            counts[str(blocker)] += 1
    return dict(sorted(counts.items()))


def _market_outcome_counts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    wallets: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        identity = _row_identity(row)
        key = f"{identity['market_slug'] or identity['condition_id']}|{identity['outcome']}"
        counts[key] += 1
        if identity["source_wallet"]:
            wallets[key].add(identity["source_wallet"])
    return [
        {
            "market_outcome": key,
            "events": count,
            "wallets": len(wallets.get(key, set())),
        }
        for key, count in counts.most_common(20)
    ]


def _top_blocked_signals(signals: list[AdaptiveSignal]) -> list[dict[str, Any]]:
    blocked = [signal for signal in signals if signal.status != "PASS"]
    ordered = sorted(
        blocked,
        key=lambda signal: (
            -float(signal.score_usd),
            -len(signal.agreeing_wallets),
            signal.market_slug,
            signal.outcome,
        ),
    )
    return [signal.asdict() for signal in ordered[:20]]


def build_adaptive_signals(
    moves: list[dict[str, Any]],
    *,
    config: AdaptiveBotConfig | None = None,
    now_ts: float | None = None,
) -> tuple[list[AdaptiveSignal], dict[str, Any]]:
    cfg = config or AdaptiveBotConfig()
    observed_now = time.time() if now_ts is None else float(now_ts)
    reason_counts: Counter[str] = Counter()
    eligible: list[dict[str, Any]] = []
    dynamic_ages = [
        age
        for row in moves
        for age in [_dynamic_event_age(row, observed_now)]
        if age is not None
    ]
    freshness_diagnostics = _adaptive_freshness_diagnostics(moves, config=cfg, now_ts=observed_now)
    for row in moves:
        ok, reason = _eligible_move(row, cfg, now_ts=observed_now)
        reason_counts[reason] += 1
        if ok:
            eligible.append(row)

    tracker_time_reason_counts: Counter[str] = Counter()
    tracker_time_eligible: list[dict[str, Any]] = []
    for row in moves:
        ok, reason = _eligible_move_at_tracker_time(row, cfg)
        tracker_time_reason_counts[reason] += 1
        if ok:
            tracker_time_eligible.append(row)

    signals, clustered_out_moves = _signals_from_eligible_rows(eligible, config=cfg)
    tracker_time_signals, tracker_time_clustered_out = _signals_from_eligible_rows(tracker_time_eligible, config=cfg)
    runtime_inventory_candidates = _inventory_candidates_from_eligible_rows(eligible, config=cfg)
    tracker_time_inventory_candidates = _inventory_candidates_from_eligible_rows(tracker_time_eligible, config=cfg)
    runtime_inventory_research_candidates = [
        row for row in runtime_inventory_candidates if row.get("status") == "RESEARCH_CANDIDATE"
    ]
    tracker_time_inventory_research_candidates = [
        row for row in tracker_time_inventory_candidates if row.get("status") == "RESEARCH_CANDIDATE"
    ]
    summary = {
        "moves_seen": len(moves),
        "eligible_moves": len(eligible),
        "signals": len(signals),
        "pass_signals": sum(1 for signal in signals if signal.status == "PASS"),
        "runtime_signal_blocker_counts": _signal_blocker_counts(signals),
        "top_blocked_runtime_signals": _top_blocked_signals(signals),
        "runtime_market_outcome_counts": _market_outcome_counts(eligible),
        "clustered_out_moves": clustered_out_moves,
        "tracker_time_eligible_moves": len(tracker_time_eligible),
        "tracker_time_signals": len(tracker_time_signals),
        "tracker_time_pass_signals": sum(1 for signal in tracker_time_signals if signal.status == "PASS"),
        "tracker_time_signal_blocker_counts": _signal_blocker_counts(tracker_time_signals),
        "top_blocked_tracker_time_signals": _top_blocked_signals(tracker_time_signals),
        "tracker_time_clustered_out_moves": tracker_time_clustered_out,
        "tracker_time_filter_reason_counts": dict(sorted(tracker_time_reason_counts.items())),
        "runtime_inventory_candidates": len(runtime_inventory_candidates),
        "runtime_inventory_research_candidates": len(runtime_inventory_research_candidates),
        "tracker_time_inventory_candidates": len(tracker_time_inventory_candidates),
        "tracker_time_inventory_research_candidates": len(tracker_time_inventory_research_candidates),
        "tracker_time_inventory_modes": dict(
            sorted(Counter(str(row.get("mode") or "unknown") for row in tracker_time_inventory_research_candidates).items())
        ),
        "top_tracker_time_inventory_candidates": tracker_time_inventory_candidates[:10],
        "top_runtime_inventory_candidates": runtime_inventory_candidates[:10],
        "dynamic_event_age_avg_s": _avg(dynamic_ages),
        "dynamic_event_age_p95_s": _p95(dynamic_ages),
        "dynamic_event_age_max_s": round(max(dynamic_ages), 6) if dynamic_ages else None,
        "filter_reason_counts": dict(sorted(reason_counts.items())),
        "freshness_diagnostics": freshness_diagnostics,
        "paper_only": True,
        "live_orders_allowed": False,
    }
    summary["_tracker_time_signals"] = [signal.asdict() for signal in tracker_time_signals[:200]]
    return signals, summary


def build_tracker_time_adaptive_signals(
    moves: list[dict[str, Any]],
    *,
    config: AdaptiveBotConfig | None = None,
) -> tuple[list[AdaptiveSignal], dict[str, Any]]:
    """Replay consensus using each row's observed-at tracker freshness.

    This is a paper/research diagnostic for Data API lag. It answers: "When the
    tracker actually saw these wallet rows, were they CLOB-backed and copyable?"
    It must not be treated as current live-admission truth because the source
    event may already be stale by wall-clock time.
    """

    cfg = config or AdaptiveBotConfig()
    reason_counts: Counter[str] = Counter()
    eligible: list[dict[str, Any]] = []
    for row in moves:
        ok, reason = _eligible_move_at_tracker_time(row, cfg)
        reason_counts[reason] += 1
        if ok:
            eligible.append(row)
    signals, clustered_out_moves = _signals_from_eligible_rows(eligible, config=cfg)
    inventory_candidates = _inventory_candidates_from_eligible_rows(eligible, config=cfg)
    research_candidates = [row for row in inventory_candidates if row.get("status") == "RESEARCH_CANDIDATE"]
    return signals, {
        "moves_seen": len(moves),
        "eligible_moves": len(eligible),
        "signals": len(signals),
        "pass_signals": sum(1 for signal in signals if signal.status == "PASS"),
        "clustered_out_moves": clustered_out_moves,
        "filter_reason_counts": dict(sorted(reason_counts.items())),
        "inventory_candidates": len(inventory_candidates),
        "inventory_research_candidates": len(research_candidates),
        "inventory_modes": dict(
            sorted(Counter(str(row.get("mode") or "unknown") for row in research_candidates).items())
        ),
        "top_inventory_candidates": inventory_candidates[:10],
        "paper_only": True,
        "live_orders_allowed": False,
        "role": "tracker_time_replay_not_live_admission",
    }


def _inventory_candidate_outcome_intent(
    candidate: dict[str, Any],
    outcome: str,
    rows: list[dict[str, Any]],
    *,
    config: AdaptiveBotConfig,
    now_ts: float,
    role: str,
) -> CopyIntent | None:
    outcome_score = float((candidate.get("outcome_scores_usd") or {}).get(outcome) or 0.0)
    if outcome_score <= 0 or not rows:
        return None
    prices = [price for row in rows for price in [_row_price(row)] if price > 0]
    if not prices:
        return None
    token_counts = Counter(_row_identity(row)["token_id"] for row in rows if _row_identity(row)["token_id"])
    token_id = token_counts.most_common(1)[0][0] if token_counts else ""
    if not token_id:
        return None
    chosen = next((row for row in rows if _row_identity(row)["token_id"] == token_id), rows[0])
    chosen_identity = _row_identity(chosen)
    average_price = round(sum(prices) / len(prices), 6)
    copy_size = min(float(config.max_order_usd), max(float(config.min_order_usd), outcome_score))
    shares = round(copy_size / average_price, 6)
    source_event_ids = sorted({_row_identity(row)["source_event_id"] for row in rows if _row_identity(row)["source_event_id"]})
    source_wallets = sorted({_row_identity(row)["source_wallet"] for row in rows if _row_identity(row)["source_wallet"]})
    wallet_names = sorted({_row_identity(row)["wallet_name"] for row in rows if _row_identity(row)["wallet_name"]})
    intent_id = stable_id(
        "ci",
        {
            "candidate_id": candidate.get("candidate_id"),
            "outcome": outcome,
            "source_event_ids": source_event_ids,
            "policy_id": "adaptive_wallet_inventory_current_poll_v1",
        },
    )
    return CopyIntent(
        intent_id=intent_id,
        source_wallet=ADAPTIVE_INVENTORY_SOURCE_WALLET,
        wallet_name="+".join(wallet_names) or "adaptive_wallet_inventory_bot",
        source_event_id=str(candidate.get("candidate_id") or intent_id),
        condition_id=str(candidate.get("condition_id") or chosen_identity["condition_id"]),
        market_slug=str(candidate.get("market_slug") or chosen_identity["market_slug"]),
        outcome=outcome,
        side=outcome_to_side(outcome),
        limit_price=average_price,
        wallet_usdc_size=round(outcome_score, 6),
        copy_size_usd=round(copy_size, 6),
        shares=shares,
        observed_ts=now_ts,
        strategy_family="adaptive_wallet_inventory_current_poll_v1",
        policy_id="adaptive_wallet_inventory_current_poll_v1",
        sizing_policy_id=f"adaptive_inventory_cap_{config.max_order_usd:g}",
        mode="paper",
        order_type="PAPER_CLOB_INVENTORY_EVIDENCE",
        token_id=token_id,
        event_ts=now_ts,
        api_latency_s=0.0,
        live_orders_allowed=False,
        reason="fresh multi-wallet inventory candidate converted to paper CopyIntent; not live-admission truth",
        metadata={
            "adaptive_inventory_candidate": candidate,
            "inventory_outcome": outcome,
            "inventory_source_wallets": source_wallets,
            "source_event_ids": source_event_ids,
            "live_tracking_evidence": (
                chosen.get("tracking_evidence") if isinstance(chosen.get("tracking_evidence"), dict) else {}
            ),
            "paper_only": True,
            "live_orders_allowed": False,
            "live_admission_role": role,
        },
    )


def build_runtime_inventory_intents(
    moves: list[dict[str, Any]],
    *,
    config: AdaptiveBotConfig | None = None,
    now_ts: float | None = None,
    max_intents: int = 20,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    """Convert fresh runtime inventory candidates into paper-only CopyIntents.

    These intents measure whether a live-poll inventory posture can actually
    enter the paper lifecycle with CLOB evidence. They are not live-admission
    truth by themselves; profit validation and copy-efficiency gates still win.
    """

    cfg = config or AdaptiveBotConfig()
    observed_now = time.time() if now_ts is None else float(now_ts)
    eligible: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    for row in moves:
        ok, reason = _eligible_move(row, cfg, now_ts=observed_now)
        reason_counts[reason] += 1
        if ok:
            eligible.append(row)
    candidates = _inventory_candidates_from_eligible_rows(eligible, config=cfg)
    research_candidates = [row for row in candidates if row.get("status") == "RESEARCH_CANDIDATE"]
    intents: list[CopyIntent] = []
    skipped: Counter[str] = Counter()
    role = "current_poll_inventory_measurement_only_not_live_admission"
    for candidate in research_candidates:
        condition_id = str(candidate.get("condition_id") or "")
        market_slug = str(candidate.get("market_slug") or "")
        outcome_scores = candidate.get("outcome_scores_usd") if isinstance(candidate.get("outcome_scores_usd"), dict) else {}
        for outcome, score in sorted(outcome_scores.items(), key=lambda item: (-float(item[1] or 0.0), str(item[0]))):
            if len(intents) >= max(0, int(max_intents)):
                skipped["max_intents_reached"] += 1
                break
            if float(score or 0.0) <= 0:
                continue
            rows = [
                row
                for row in eligible
                for identity in [_row_identity(row)]
                if identity["outcome"] == outcome
                and (identity["condition_id"] == condition_id or identity["market_slug"] == market_slug)
            ]
            intent = _inventory_candidate_outcome_intent(
                candidate,
                str(outcome),
                rows,
                config=cfg,
                now_ts=observed_now,
                role=role,
            )
            if intent is None:
                skipped["intent_build_failed"] += 1
                continue
            intents.append(intent)
    summary = {
        "moves_seen": len(moves),
        "eligible_moves": len(eligible),
        "filter_reason_counts": dict(sorted(reason_counts.items())),
        "inventory_candidates": len(candidates),
        "inventory_research_candidates": len(research_candidates),
        "inventory_intents": len(intents),
        "inventory_modes": dict(
            sorted(Counter(str(row.get("mode") or "unknown") for row in research_candidates).items())
        ),
        "skipped_reasons": dict(sorted(skipped.items())),
        "top_inventory_candidates": candidates[:10],
        "paper_only": True,
        "live_orders_allowed": False,
        "role": role,
    }
    return intents, summary


def _single_wallet_exact_copy_intent(
    row: dict[str, Any],
    *,
    config: AdaptiveBotConfig,
    now_ts: float,
) -> CopyIntent | None:
    identity = _row_identity(row)
    source_wallet = identity["source_wallet"]
    source_event_id = identity["source_event_id"] or _move_fingerprint(row)
    price = _row_price(row)
    token_id = identity["token_id"]
    if not source_wallet or not source_event_id or price <= 0 or not token_id:
        return None
    wallet_usd = _source_usd(row)
    copy_size = min(float(config.max_order_usd), max(float(config.min_order_usd), wallet_usd))
    if copy_size <= 0:
        return None
    event_ts = identity["event_ts"] or _source_event_timestamp(row) or now_ts
    return CopyIntent(
        source_wallet=source_wallet,
        wallet_name=identity["wallet_name"] or "single_wallet_exact_copy",
        source_event_id=source_event_id,
        condition_id=identity["condition_id"],
        market_slug=identity["market_slug"],
        outcome=identity["outcome"],
        side=outcome_to_side(identity["outcome"]),
        limit_price=round(price, 6),
        wallet_usdc_size=round(wallet_usd, 6),
        copy_size_usd=round(copy_size, 6),
        shares=round(copy_size / price, 6),
        observed_ts=now_ts,
        strategy_family="adaptive_single_wallet_exact_copy_current_poll_v1",
        policy_id="adaptive_single_wallet_exact_copy_current_poll_v1",
        sizing_policy_id=f"adaptive_single_wallet_cap_{config.max_order_usd:g}",
        mode="paper",
        order_type="PAPER_CLOB_SINGLE_WALLET_COPY_EVIDENCE",
        token_id=token_id,
        event_ts=event_ts,
        api_latency_s=round(max(0.0, now_ts - float(event_ts)), 6),
        live_orders_allowed=False,
        reason="fresh single-wallet CLOB-backed move copied to separate paper ledger; not live-admission truth",
        metadata={
            "single_wallet_exact_copy": True,
            "source_fingerprint": _move_fingerprint(row),
            "live_tracking_evidence": row.get("tracking_evidence")
            if isinstance(row.get("tracking_evidence"), dict)
            else {},
            "copy_efficiency": row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {},
            "paper_only": True,
            "live_orders_allowed": False,
            "live_admission_role": "current_poll_single_wallet_measurement_only_until_profit_validation_passes",
        },
    )


def build_single_wallet_exact_copy_intents(
    moves: list[dict[str, Any]],
    *,
    config: AdaptiveBotConfig | None = None,
    now_ts: float | None = None,
    max_intents: int | None = None,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    """Copy fresh runtime wallet moves 1:1 into a separate paper ledger.

    This covers the primary wallet-copy path when only one active wallet moves
    in the current poll. It is intentionally not consensus and not live
    admission; it measures whether exact single-wallet copying is executable
    with CLOB evidence before profit validation decides whether the wallet is
    worth promoting.
    """

    cfg = config or AdaptiveBotConfig()
    observed_now = time.time() if now_ts is None else float(now_ts)
    limit = int(cfg.max_single_wallet_exact_copy_intents if max_intents is None else max_intents)
    reason_counts: Counter[str] = Counter()
    eligible: list[dict[str, Any]] = []
    for row in moves:
        ok, reason = _eligible_move(row, cfg, now_ts=observed_now)
        reason_counts[reason] += 1
        if ok:
            eligible.append(row)
    ordered = sorted(
        eligible,
        key=lambda row: (
            _dynamic_event_age(row, observed_now) if _dynamic_event_age(row, observed_now) is not None else 999999.0,
            _move_fingerprint(row),
        ),
    )
    intents = [
        intent
        for row in ordered[: max(0, limit)]
        for intent in [_single_wallet_exact_copy_intent(row, config=cfg, now_ts=observed_now)]
        if intent is not None
    ]
    wallets = sorted({_row_identity(row)["source_wallet"] for row in eligible if _row_identity(row)["source_wallet"]})
    market_outcomes = _market_outcome_counts(eligible)
    return intents, {
        "moves_seen": len(moves),
        "eligible_moves": len(eligible),
        "intents": len(intents),
        "wallets": wallets,
        "wallet_count": len(wallets),
        "market_outcomes": market_outcomes,
        "filter_reason_counts": dict(sorted(reason_counts.items())),
        "paper_only": True,
        "live_orders_allowed": False,
        "role": "single_wallet_exact_copy_current_poll_measurement_only_not_live_admission",
    }


def _paper_batch_summary(state: dict[str, Any] | None, intents: list[CopyIntent]) -> dict[str, Any]:
    intent_ids = {intent.intent_id for intent in intents if intent.intent_id}
    orders = [
        row
        for row in ((state or {}).get("orders") or [])
        if isinstance(row, dict) and str(row.get("intent_id") or "") in intent_ids
    ]
    filled = [row for row in orders if row.get("final_status") == "FILLED"]
    rejected = [row for row in orders if row.get("final_status") == "REJECTED"]
    ledger_summary = (state or {}).get("summary") if isinstance((state or {}).get("summary"), dict) else {}
    return {
        "paper_orders": len(orders),
        "filled_orders": len(filled),
        "rejected_orders": len(rejected),
        "ledger_paper_orders": ledger_summary.get("paper_orders"),
        "ledger_filled_orders": ledger_summary.get("filled_orders"),
        "ledger_rejected_orders": ledger_summary.get("rejected_orders"),
    }


def _positive_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _clob_only_fill_sources(counts: Any, *, required: int) -> bool:
    if not isinstance(counts, dict):
        return required <= 0
    clob = _positive_int(counts.get("clob_book_evidence"))
    total = sum(_positive_int(value) for value in counts.values())
    return required > 0 and clob >= required and total == clob


def _embedded_tracker_hot_path(config: AdaptiveBotConfig) -> dict[str, Any]:
    """Return the live tracker's current-poll hot-path proof, if available.

    The standalone adaptive bot reads the tracker event log, which can already
    be stale by the time it runs. The live tracker also persists the hot-path
    measurement from the same poll. This bridge keeps that current-poll truth
    visible without treating tracker-time replay as admission evidence.
    """

    state = load_json(config.live_tracking_state_path, default={})
    if not isinstance(state, dict):
        return {"status": "MISSING", "blockers": ["live_tracking_state_missing"]}
    summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    hot_path = summary.get("hot_path_adaptive") if isinstance(summary.get("hot_path_adaptive"), dict) else {}
    if not hot_path:
        return {"status": "MISSING", "blockers": ["embedded_hot_path_missing"]}
    hot_summary = hot_path.get("summary") if isinstance(hot_path.get("summary"), dict) else {}
    paper_lifecycle = hot_path.get("paper_lifecycle") if isinstance(hot_path.get("paper_lifecycle"), dict) else {}
    inventory_lifecycle = (
        hot_path.get("inventory_paper_lifecycle")
        if isinstance(hot_path.get("inventory_paper_lifecycle"), dict)
        else {}
    )
    single_wallet_lifecycle = (
        hot_path.get("single_wallet_exact_copy_paper_lifecycle")
        if isinstance(hot_path.get("single_wallet_exact_copy_paper_lifecycle"), dict)
        else {}
    )
    hot_intents = _positive_int(hot_summary.get("hot_path_intents_created"))
    hot_filled = _positive_int(hot_summary.get("hot_path_filled_orders"))
    hot_rejected = _positive_int(hot_summary.get("hot_path_rejected_orders"))
    inventory_intents = _positive_int(hot_summary.get("hot_path_inventory_intents_created"))
    inventory_filled = _positive_int(hot_summary.get("hot_path_inventory_filled_orders"))
    inventory_rejected = _positive_int(hot_summary.get("hot_path_inventory_rejected_orders"))
    single_intents = _positive_int(hot_summary.get("hot_path_single_wallet_exact_copy_intents_created"))
    single_filled = _positive_int(hot_summary.get("hot_path_single_wallet_exact_copy_filled_orders"))
    single_rejected = _positive_int(hot_summary.get("hot_path_single_wallet_exact_copy_rejected_orders"))
    direct_clob_only = _clob_only_fill_sources(
        hot_summary.get("hot_path_fill_source_counts") or paper_lifecycle.get("fill_source_counts"),
        required=hot_intents,
    )
    inventory_clob_only = _clob_only_fill_sources(
        hot_summary.get("hot_path_inventory_fill_source_counts") or inventory_lifecycle.get("fill_source_counts"),
        required=inventory_intents,
    )
    single_clob_only = _clob_only_fill_sources(
        hot_summary.get("hot_path_single_wallet_exact_copy_fill_source_counts")
        or single_wallet_lifecycle.get("fill_source_counts"),
        required=single_intents,
    )
    direct_pass = bool(
        hot_intents > 0
        and hot_filled >= hot_intents
        and hot_rejected == 0
        and direct_clob_only
    )
    inventory_pass = bool(
        inventory_intents > 0
        and inventory_filled >= inventory_intents
        and inventory_rejected == 0
        and inventory_clob_only
    )
    single_wallet_pass = bool(
        single_intents > 0
        and single_filled >= single_intents
        and single_rejected == 0
        and single_clob_only
    )
    single_wallet_promotion_blockers: list[str] = []
    if single_intents <= 0:
        single_wallet_promotion_blockers.append("no_single_wallet_exact_copy_intents")
    if single_intents < MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS:
        single_wallet_promotion_blockers.append("single_wallet_live_promotion_required_buy_copy_events_below_10")
    if not single_wallet_pass:
        single_wallet_promotion_blockers.append("single_wallet_live_promotion_clob_copy_lifecycle_not_pass")
    single_wallet_live_promotion = {
        "status": "PASS" if not single_wallet_promotion_blockers else "ANALYZE",
        "required_buy_copy_events": single_intents,
        "clob_filled_buy_copy_events": single_filled if single_clob_only else 0,
        "fallback_filled_buy_copy_events": 0,
        "rejected_buy_copy_events": single_rejected,
        "missed_buy_copy_events": max(0, single_intents - single_filled),
        "min_required_buy_copy_events": MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS,
        "fill_source_counts": (
            hot_summary.get("hot_path_single_wallet_exact_copy_fill_source_counts")
            or single_wallet_lifecycle.get("fill_source_counts")
            or {}
        ),
        "blockers": sorted(set(single_wallet_promotion_blockers)),
        "paper_only": True,
        "live_orders_allowed": False,
        "role": "primary_single_wallet_live_promotion_current_poll_truth",
    }
    freshness = hot_summary.get("freshness_diagnostics") if isinstance(hot_summary.get("freshness_diagnostics"), dict) else {}
    return {
        "status": hot_path.get("status") or "MISSING",
        "blockers": hot_path.get("blockers") or [],
        "role": hot_path.get("role"),
        "source_state_path": config.live_tracking_state_path,
        "current_poll_moves": hot_summary.get("current_poll_moves"),
        "pass_signals": hot_summary.get("pass_signals"),
        "runtime_fresh_buy_events_le_cap": freshness.get("runtime_fresh_buy_events_le_cap"),
        "runtime_eligible_wallets": freshness.get("runtime_eligible_wallets"),
        "runtime_inventory_research_candidates": hot_summary.get("runtime_inventory_research_candidates"),
        "hot_path_intents_created": hot_intents,
        "hot_path_filled_orders": hot_filled,
        "hot_path_rejected_orders": hot_rejected,
        "hot_path_fill_source_counts": hot_summary.get("hot_path_fill_source_counts")
        or paper_lifecycle.get("fill_source_counts")
        or {},
        "hot_path_inventory_intents_created": inventory_intents,
        "hot_path_inventory_filled_orders": inventory_filled,
        "hot_path_inventory_rejected_orders": inventory_rejected,
        "hot_path_inventory_fill_source_counts": hot_summary.get("hot_path_inventory_fill_source_counts")
        or inventory_lifecycle.get("fill_source_counts")
        or {},
        "hot_path_single_wallet_exact_copy_intents_created": single_intents,
        "hot_path_single_wallet_exact_copy_filled_orders": single_filled,
        "hot_path_single_wallet_exact_copy_rejected_orders": single_rejected,
        "hot_path_single_wallet_exact_copy_fill_source_counts": (
            hot_summary.get("hot_path_single_wallet_exact_copy_fill_source_counts")
            or single_wallet_lifecycle.get("fill_source_counts")
            or {}
        ),
        "current_poll_multi_wallet_pass": bool(hot_path.get("status") == "PASS" and (direct_pass or inventory_pass)),
        "current_poll_direct_consensus_pass": bool(hot_path.get("status") == "PASS" and direct_pass),
        "current_poll_inventory_pass": bool(hot_path.get("status") == "PASS" and inventory_pass),
        "current_poll_single_wallet_exact_copy_pass": single_wallet_pass,
        "primary_single_wallet_live_promotion": single_wallet_live_promotion,
        "primary_single_wallet_live_promotion_pass": single_wallet_live_promotion["status"] == "PASS",
        "paper_only": True,
        "live_orders_allowed": False,
        "live_admission_role": "embedded_current_poll_measurement_only_not_live_admission_truth",
    }


def signal_to_intent(signal: AdaptiveSignal, *, config: AdaptiveBotConfig | None = None) -> CopyIntent | None:
    cfg = config or AdaptiveBotConfig()
    if signal.status != "PASS":
        return None
    if signal.average_price <= 0 or signal.score_usd <= 0:
        return None
    copy_size = min(float(cfg.max_order_usd), max(float(cfg.min_order_usd), float(signal.score_usd)))
    shares = round(copy_size / float(signal.average_price), 6)
    return CopyIntent(
        source_wallet=ADAPTIVE_SOURCE_WALLET,
        wallet_name="adaptive_wallet_copy_bot",
        source_event_id=signal.signal_id,
        condition_id=signal.condition_id,
        market_slug=signal.market_slug,
        outcome=signal.outcome,
        side=signal.side,
        limit_price=signal.average_price,
        wallet_usdc_size=signal.score_usd,
        copy_size_usd=round(copy_size, 6),
        shares=shares,
        observed_ts=time.time(),
        strategy_family="adaptive_wallet_derived_bot_v1",
        policy_id="adaptive_fresh_clob_consensus_v1",
        sizing_policy_id=f"adaptive_cap_{cfg.max_order_usd:g}",
        mode="paper",
        order_type="PAPER_CLOB_EVIDENCE",
        token_id=signal.chosen_token_id,
        event_ts=time.time(),
        api_latency_s=0.0,
        live_orders_allowed=False,
        reason="fresh profitable-wallet consensus converted to paper CopyIntent",
        metadata={
            "adaptive_signal": signal.asdict(),
            "live_tracking_evidence": signal.chosen_evidence,
            "paper_only": True,
            "live_orders_allowed": False,
        },
    )


def _signal_to_tracker_time_replay_intent(
    signal: AdaptiveSignal,
    *,
    config: AdaptiveBotConfig,
) -> CopyIntent | None:
    intent = signal_to_intent(signal, config=config)
    if intent is None:
        return None
    metadata = dict(intent.metadata)
    metadata.update(
        {
            "tracker_time_replay": True,
            "paper_only": True,
            "live_orders_allowed": False,
            "live_admission_role": "research_replay_only_not_current_poll_truth",
        }
    )
    return replace(
        intent,
        intent_id="",
        strategy_family="adaptive_wallet_tracker_time_replay_v1",
        policy_id="adaptive_tracker_time_replay_clob_consensus_v1",
        order_type="PAPER_TRACKER_TIME_REPLAY_CLOB_EVIDENCE",
        reason="tracker-time wallet consensus replayed into separate paper ledger; not live-admission truth",
        metadata=metadata,
        live_orders_allowed=False,
    )


def run_adaptive_bot(config: AdaptiveBotConfig | None = None) -> dict[str, Any]:
    cfg = config or AdaptiveBotConfig()
    moves = load_tracker_moves(cfg)
    embedded_hot_path = _embedded_tracker_hot_path(cfg)
    signals, summary = build_adaptive_signals(moves, config=cfg)
    intents = [intent for signal in signals for intent in [signal_to_intent(signal, config=cfg)] if intent is not None]
    single_wallet_intents, single_wallet_summary = build_single_wallet_exact_copy_intents(
        moves,
        config=cfg,
    )
    tracker_time_replay_signals, tracker_time_replay_summary = build_tracker_time_adaptive_signals(
        moves,
        config=cfg,
    )
    tracker_time_replay_intents = [
        intent
        for signal in tracker_time_replay_signals
        for intent in [_signal_to_tracker_time_replay_intent(signal, config=cfg)]
        if intent is not None
    ][: max(0, int(cfg.max_tracker_time_replay_intents))]
    paper_state: dict[str, Any] | None = None
    if cfg.apply_paper:
        paper_state = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=cfg.paper_state_path,
                event_log_path=cfg.paper_event_log_path,
                min_fill_ratio=cfg.min_clob_fill_ratio,
            )
        ).apply_intents(intents)
    single_wallet_paper_state: dict[str, Any] | None = None
    if cfg.apply_paper and cfg.apply_single_wallet_exact_copy_paper:
        single_wallet_paper_state = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=cfg.single_wallet_exact_copy_paper_state_path,
                event_log_path=cfg.single_wallet_exact_copy_paper_event_log_path,
                min_fill_ratio=cfg.min_clob_fill_ratio,
            )
        ).apply_intents(single_wallet_intents)
    tracker_time_replay_paper_state: dict[str, Any] | None = None
    if cfg.apply_paper and cfg.apply_tracker_time_replay_paper:
        tracker_time_replay_paper_state = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=cfg.tracker_time_replay_paper_state_path,
                event_log_path=cfg.tracker_time_replay_paper_event_log_path,
                min_fill_ratio=cfg.min_clob_fill_ratio,
            )
        ).apply_intents(tracker_time_replay_intents)
    adaptive_batch = _paper_batch_summary(paper_state, intents)
    single_wallet_batch = _paper_batch_summary(single_wallet_paper_state, single_wallet_intents)
    tracker_time_replay_batch = _paper_batch_summary(tracker_time_replay_paper_state, tracker_time_replay_intents)
    embedded_multi_wallet_pass = embedded_hot_path.get("current_poll_multi_wallet_pass") is True
    embedded_single_wallet_pass = embedded_hot_path.get("current_poll_single_wallet_exact_copy_pass") is True
    embedded_single_wallet_promotion_pass = embedded_hot_path.get("primary_single_wallet_live_promotion_pass") is True
    status = "PASS" if intents or embedded_multi_wallet_pass else "WATCH"
    blockers: list[str] = []
    if not intents and not embedded_multi_wallet_pass:
        blockers.append("no_adaptive_pass_signal")
        if tracker_time_replay_intents:
            blockers.append("tracker_time_replay_available_but_runtime_truth_missing")
        if single_wallet_intents:
            blockers.append("single_wallet_live_promotion_waiting_for_current_poll_burnin")
        if embedded_single_wallet_pass and not embedded_single_wallet_promotion_pass:
            blockers.append("embedded_hot_path_single_wallet_live_promotion_sample_below_threshold")
        elif embedded_hot_path.get("status") == "PASS":
            blockers.append("embedded_hot_path_pass_without_multi_wallet_copyintent_lifecycle")
    source_wallets = sorted(
        {
            _row_identity(move)["source_wallet"]
            for move in moves
            if _row_identity(move)["source_wallet"]
        }
    )
    source_lane = (
        "active_hotlane"
        if "active_hotlane" in str(cfg.live_tracking_state_path)
        or "active_hotlane" in str(cfg.live_tracking_event_log_path)
        else "canonical"
    )
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_adaptive_bot_state",
        "generated_at": utc_now_iso(),
        "status": status,
        "blockers": blockers,
        "paper_only": True,
        "live_orders_allowed": False,
        "config": cfg.asdict(),
        "source_provenance": {
            "lane": source_lane,
            "live_tracking_state_path": cfg.live_tracking_state_path,
            "live_tracking_event_log_path": cfg.live_tracking_event_log_path,
            "embedded_hot_path_state_path": cfg.live_tracking_state_path,
            "moves_seen": len(moves),
            "source_wallet_count": len(source_wallets),
            "source_wallets": source_wallets[:200],
        },
        "summary": {
            **{key: value for key, value in summary.items() if key != "_tracker_time_signals"},
            "intents": len(intents),
            "paper_orders": adaptive_batch["paper_orders"],
            "filled_orders": adaptive_batch["filled_orders"],
            "rejected_orders": adaptive_batch["rejected_orders"],
            "ledger_paper_orders": adaptive_batch["ledger_paper_orders"],
            "ledger_filled_orders": adaptive_batch["ledger_filled_orders"],
            "ledger_rejected_orders": adaptive_batch["ledger_rejected_orders"],
            "single_wallet_exact_copy_intents": len(single_wallet_intents),
            "single_wallet_exact_copy_paper_orders": single_wallet_batch["paper_orders"],
            "single_wallet_exact_copy_filled_orders": single_wallet_batch["filled_orders"],
            "single_wallet_exact_copy_rejected_orders": single_wallet_batch["rejected_orders"],
            "single_wallet_exact_copy_ledger_paper_orders": single_wallet_batch["ledger_paper_orders"],
            "single_wallet_exact_copy_ledger_filled_orders": single_wallet_batch["ledger_filled_orders"],
            "single_wallet_exact_copy_ledger_rejected_orders": single_wallet_batch["ledger_rejected_orders"],
            "single_wallet_exact_copy": single_wallet_summary,
            "tracker_time_replay_intents": len(tracker_time_replay_intents),
            "tracker_time_replay_paper_orders": tracker_time_replay_batch["paper_orders"],
            "tracker_time_replay_filled_orders": tracker_time_replay_batch["filled_orders"],
            "tracker_time_replay_rejected_orders": tracker_time_replay_batch["rejected_orders"],
            "tracker_time_replay_ledger_paper_orders": tracker_time_replay_batch["ledger_paper_orders"],
            "tracker_time_replay_ledger_filled_orders": tracker_time_replay_batch["ledger_filled_orders"],
            "tracker_time_replay_ledger_rejected_orders": tracker_time_replay_batch["ledger_rejected_orders"],
            "embedded_hot_path_status": embedded_hot_path.get("status"),
            "embedded_hot_path_role": embedded_hot_path.get("role"),
            "embedded_hot_path_current_poll_multi_wallet_pass": embedded_multi_wallet_pass,
            "embedded_hot_path_single_wallet_exact_copy_pass": embedded_single_wallet_pass,
            "embedded_hot_path_primary_single_wallet_live_promotion_pass": embedded_single_wallet_promotion_pass,
            "embedded_hot_path_current_poll_moves": embedded_hot_path.get("current_poll_moves"),
            "embedded_hot_path_runtime_fresh_buy_events_le_cap": embedded_hot_path.get(
                "runtime_fresh_buy_events_le_cap"
            ),
            "embedded_hot_path_runtime_eligible_wallets": embedded_hot_path.get("runtime_eligible_wallets"),
            "embedded_hot_path_intents_created": embedded_hot_path.get("hot_path_intents_created"),
            "embedded_hot_path_filled_orders": embedded_hot_path.get("hot_path_filled_orders"),
            "embedded_hot_path_rejected_orders": embedded_hot_path.get("hot_path_rejected_orders"),
            "embedded_hot_path_inventory_intents_created": embedded_hot_path.get(
                "hot_path_inventory_intents_created"
            ),
            "embedded_hot_path_inventory_filled_orders": embedded_hot_path.get("hot_path_inventory_filled_orders"),
            "embedded_hot_path_inventory_rejected_orders": embedded_hot_path.get(
                "hot_path_inventory_rejected_orders"
            ),
            "embedded_hot_path_single_wallet_exact_copy_intents_created": embedded_hot_path.get(
                "hot_path_single_wallet_exact_copy_intents_created"
            ),
            "embedded_hot_path_single_wallet_exact_copy_filled_orders": embedded_hot_path.get(
                "hot_path_single_wallet_exact_copy_filled_orders"
            ),
            "embedded_hot_path_single_wallet_exact_copy_rejected_orders": embedded_hot_path.get(
                "hot_path_single_wallet_exact_copy_rejected_orders"
            ),
        },
        "embedded_tracker_hot_path": embedded_hot_path,
        "signals": [signal.asdict() for signal in signals[:200]],
        "single_wallet_exact_copy": {
            "status": "PASS" if single_wallet_intents else "WATCH",
            "blockers": [] if single_wallet_intents else ["no_fresh_single_wallet_exact_copy_intents"],
            "role": "separate_current_poll_single_wallet_copy_measurement_not_live_admission_truth",
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {
                **single_wallet_summary,
                **single_wallet_batch,
                "paper_state_path": cfg.single_wallet_exact_copy_paper_state_path,
                "paper_event_log_path": cfg.single_wallet_exact_copy_paper_event_log_path,
            },
            "copy_intents": [intent.asdict() for intent in single_wallet_intents[:200]],
        },
        "tracker_time_signals": summary.get("_tracker_time_signals", []),
        "tracker_time_replay": {
            "status": "PASS" if tracker_time_replay_intents else "WATCH",
            "blockers": [] if tracker_time_replay_intents else ["no_tracker_time_replay_intents"],
            "role": "separate_paper_replay_not_live_admission_truth",
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {
                **tracker_time_replay_summary,
                "intents": len(tracker_time_replay_intents),
                **tracker_time_replay_batch,
            },
            "signals": [signal.asdict() for signal in tracker_time_replay_signals[:200]],
            "copy_intents": [intent.asdict() for intent in tracker_time_replay_intents[:200]],
            "paper_state_path": cfg.tracker_time_replay_paper_state_path
            if cfg.apply_paper and cfg.apply_tracker_time_replay_paper
            else None,
        },
        "copy_intents": [intent.asdict() for intent in intents],
        "paper_state_path": cfg.paper_state_path if cfg.apply_paper else None,
    }
    atomic_write_json(cfg.output_state_path, payload)
    return payload
