"""Profitability search and admission for wallet-copy strategies.

The engine is deliberately paper/replay only. It searches single-wallet,
multi-wallet consensus, and inventory-copy candidates against resolved BTC 5m
history, then writes an explicit PASS/ANALYZE/CORRECTION decision. Live
execution can consume only candidates that pass this report and the separate
execution gate.
"""

from __future__ import annotations

import json
import re
from collections import Counter, OrderedDict, defaultdict
from dataclasses import asdict, dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from src.wallet_copy.consensus import ConsensusConfig, build_consensus_signals, consensus_signal_to_intent
from src.wallet_copy.alpha_decay import btc_5m_move_slice_for_values
from src.wallet_copy.fill_model import FillModelConfig, estimate_executable_fill
from src.wallet_copy.features import slug_window_start
from src.wallet_copy.inventory import InventoryConfig, build_inventory_plans, inventory_plan_child_intents
from src.wallet_copy.models import CopyIntent, SizingPolicy, WalletEvent, stable_id, utc_now_iso
from src.wallet_copy.performance import load_resolutions, score_order, summarize_scores
from src.wallet_copy.research import unique_wallet_events
from src.wallet_copy.source_route import source_route_allows_live_execution
from src.wallet_copy.status import ANALYZE, PASS, active_plan_mode, active_status_from_blockers
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.strategy import CopyPolicy, outcome_to_side

DEFAULT_HISTORY_WINDOW_INDEX_PATH = Path("data/research/wallet_copy_history_window_index.json")


@dataclass(frozen=True)
class CandidatePolicy:
    policy_id: str
    min_price: float = 0.01
    max_price: float = 1.0
    min_wallet_usdc: float = 0.0
    max_wallet_usdc: float = 0.0
    min_seconds_from_open: float | None = None
    max_seconds_from_open: float | None = None
    move_slice_keys: tuple[str, ...] = ()
    wallet_fraction: float = 0.05
    max_order_usd: float = 2.0
    min_order_usd: float = 0.0
    maker_min_share_funding_cap_usd: float = 0.0
    maker_min_share_original_policy_cap_usd: float = 0.0
    maker_min_share_base_request_cap_usd: float = 0.0
    maker_fallback_defense_cap_usd: float = 0.0

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProfitEngineConfig:
    min_train_resolved: int = 20
    min_validation_resolved: int = 10
    min_all_resolved: int = 30
    min_roi_pct: float = 2.0
    min_wr_pct: float = 70.0
    max_drawdown_usd: float = 20.0
    max_unresolved_ratio: float = 0.5
    train_fraction: float = 0.7
    slippage_bps: float = 250.0
    max_candidates: int = 200
    raw_baseline_guard: bool = True
    min_raw_baseline_resolved: int = 30
    min_filtered_resolved_when_raw_negative: int = 100
    min_candidate_vs_raw_delta_roi_pct: float = 5.0
    require_live_tracker_truth_for_live_admission: bool = True
    live_tracker_state_path: str = "data/research/wallet_copy_live_tracking_state.json"
    active_hotlane_state_path: str = "data/research/wallet_copy_active_hotlane_state.json"
    active_hotlane_live_tracker_state_path: str = "data/research/wallet_copy_active_hotlane_live_tracking_state.json"
    candidate_forward_live_tracker_state_path: str = (
        "data/research/wallet_copy_candidate_forward_live_tracking_state.json"
    )
    candidate_forward_probe_live_tracker_state_paths: tuple[str, ...] = ()
    require_candidate_clob_fill_evidence: bool = True
    min_agreeing_wallets: int = 2
    max_price_spread: float = 0.08
    inventory_max_window_usd: float = 10.0
    inventory_max_per_wallet_usd: float = 2.0
    inventory_min_plan_usd: float = 1.0
    require_copy_efficiency_truth: bool = True
    min_runtime_candidate_required_buy_copy_events: int = 3
    min_runtime_candidate_required_market_windows: int = 2
    enable_consensus_search: bool = True
    enable_inventory_search: bool = True
    max_multi_wallet_base_intents: int = 0
    max_wallets_for_search: int = 0
    max_single_wallet_candidate_intents: int = 0
    skip_candidates_below_min_intent_count: bool = True
    min_all_unique_windows: int = 0
    min_train_unique_windows: int = 0
    min_validation_unique_windows: int = 0
    max_candidate_orders_per_window: int = 25
    max_candidate_orders_per_window_ratio: float = 0.08
    forward_tracking_queue_size: int = 12
    forward_fresh_lag_cap_s: float = 1800.0
    forward_probe_fresh_lag_cap_s: float = 60.0
    allow_forward_runtime_candidate_selection: bool = True
    candidate_runtime_proof_index_path: str = ""
    strategy_direction_state_path: str = "data/research/wallet_copy_strategy_direction_state.json"
    source_route_state_path: str = ""
    live_today_sprint_operator_approval_id: str = ""
    live_target_candidate_types: tuple[str, ...] = ("SINGLE_WALLET", "MULTI_WALLET_INVENTORY")
    live_target_min_resolved_orders: int = 100
    live_target_min_unique_windows: int = 10
    live_target_min_avg_orders_per_window: float = 2.0
    live_target_min_wr_pct: float = 70.0
    live_target_min_validation_wr_pct: float = 70.0
    live_target_min_roi_pct: float = 2.0
    live_target_min_pnl_usd: float = 0.0

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


RUNTIME_COPY_RESOLVABLE_BLOCKERS = {
    "candidate_missing_clob_fill_evidence",
    "candidate_window_order_concentration_above_maximum",
    "candidate_window_order_concentration_ratio_above_maximum",
}

MISSION_ADVISORY_BLOCKER_PREFIXES_WHEN_LIVE_TARGET_PASS: tuple[str, ...] = (
    "development_program_bridge_pending_current_poll_inventory",
)


def _runtime_resolvable_blockers_for_live_target(
    blockers: Iterable[str],
    live_target_profile: dict[str, Any] | None,
) -> set[str]:
    """Blockers runtime CLOB proof can clear without relaxing paper gates.

    Runtime proof answers whether we can copy a candidate through current CLOB
    truth. It must not clear profitability, validation ROI, drawdown, raw
    baseline, or sample-size blockers from the paper result itself.
    """

    live_target_pass = isinstance(live_target_profile, dict) and live_target_profile.get("status") == PASS
    resolved: set[str] = set()
    for blocker in blockers:
        if blocker in RUNTIME_COPY_RESOLVABLE_BLOCKERS:
            resolved.add(blocker)
            continue
        if live_target_pass and any(
            blocker.startswith(prefix) for prefix in MISSION_ADVISORY_BLOCKER_PREFIXES_WHEN_LIVE_TARGET_PASS
        ):
            resolved.add(blocker)
    return resolved


def _row_source_wallet(row: dict[str, Any]) -> str:
    wallet = str(row.get("source_wallet") or row.get("wallet") or row.get("proxy_wallet") or "")
    if wallet:
        return wallet.lower()
    wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
    return str(wallet_event.get("source_wallet") or wallet_event.get("wallet") or "").lower()


def _row_window_start(row: dict[str, Any]) -> int | None:
    for key in ("window_start_s", "window_start_ts", "window_start", "market_window_start"):
        value = row.get(key)
        if value is None:
            continue
        try:
            numeric = int(float(value))
        except (TypeError, ValueError):
            continue
        if numeric > 0:
            return numeric
    slug = str(row.get("market_slug") or "")
    return slug_window_start(slug)


def _history_row_matches_scope(
    row: dict[str, Any],
    *,
    source_wallet: str,
    window_starts: set[int],
) -> bool:
    if source_wallet and _row_source_wallet(row) != source_wallet.lower():
        return False
    if window_starts and _row_window_start(row) not in window_starts:
        return False
    return True


def _history_window_index_source_meta(path: str | Path) -> dict[str, Any]:
    source_path = Path(path)
    try:
        stat = source_path.stat()
    except FileNotFoundError:
        return {"path": str(source_path), "size": 0, "mtime_ns": 0}
    return {"path": str(source_path), "size": int(stat.st_size), "mtime_ns": int(stat.st_mtime_ns)}


def _default_history_window_index_path(path: str | Path) -> Path:
    source_path = Path(path)
    if source_path.name == "wallet_copy_history_state.json":
        return DEFAULT_HISTORY_WINDOW_INDEX_PATH
    return source_path.with_name(f"{source_path.stem}_window_index.json")


def _history_window_index_is_current(index: dict[str, Any], path: str | Path) -> bool:
    return index.get("schema_version") == 1 and index.get("source") == _history_window_index_source_meta(path)


def _iter_history_event_row_spans(path: str | Path) -> Iterable[tuple[dict[str, Any], int, int]]:
    text = Path(path).read_text(encoding="utf-8")
    marker = '"events"'
    marker_index = text.find(marker)
    if marker_index < 0:
        return
    array_start = text.find("[", marker_index)
    if array_start < 0:
        return
    decoder = json.JSONDecoder()
    index = array_start + 1
    length = len(text)
    while index < length:
        while index < length and text[index] in " \t\r\n,":
            index += 1
        if index >= length or text[index] == "]":
            break
        row_start = index
        row, index = decoder.raw_decode(text, index)
        if isinstance(row, dict):
            yield row, row_start, index


def build_history_window_index(
    path: str | Path,
    *,
    index_path: str | Path | None = None,
) -> dict[str, Any]:
    """Persist byte spans for wallet/window scoped history reads.

    The live inventory path needs only a few BTC-5m windows. This sidecar lets
    it avoid deserializing the full history file on every guard cycle.
    """

    source_path = Path(path)
    target_index_path = Path(index_path) if index_path is not None else _default_history_window_index_path(source_path)
    windows: dict[str, dict[str, list[list[int]]]] = defaultdict(lambda: defaultdict(list))
    indexed_rows = 0
    skipped_rows = 0
    for row, start, end in _iter_history_event_row_spans(source_path):
        wallet = _row_source_wallet(row)
        window_start = _row_window_start(row)
        if not wallet or window_start is None:
            skipped_rows += 1
            continue
        windows[str(window_start)][wallet].append([int(start), int(end - start)])
        indexed_rows += 1

    index = {
        "kind": "wallet_copy_history_window_index",
        "schema_version": 1,
        "source": _history_window_index_source_meta(source_path),
        "indexed_rows": indexed_rows,
        "skipped_rows": skipped_rows,
        "windows": {window: dict(wallets) for window, wallets in windows.items()},
    }
    atomic_write_json(target_index_path, index)
    return index


def load_history_window_index(
    path: str | Path,
    *,
    index_path: str | Path | None = None,
    rebuild_if_stale: bool = True,
) -> dict[str, Any]:
    target_index_path = Path(index_path) if index_path is not None else _default_history_window_index_path(path)
    index = load_json(target_index_path, default={})
    if isinstance(index, dict) and _history_window_index_is_current(index, path):
        return index
    if not rebuild_if_stale:
        return {}
    return build_history_window_index(path, index_path=target_index_path)


def history_index_stats_for_scope(
    path: str | Path,
    *,
    source_wallet: str = "",
    window_starts: Iterable[int] | None = None,
    index_path: str | Path | None = None,
) -> dict[str, Any]:
    scoped_windows = {int(float(value)) for value in (window_starts or []) if value is not None}
    if not source_wallet or not scoped_windows:
        return {"enabled": False, "reason": "source_wallet_and_window_starts_required"}
    index = load_history_window_index(path, index_path=index_path)
    source = _history_window_index_source_meta(path)
    spans: list[list[int]] = []
    windows = index.get("windows") if isinstance(index, dict) else {}
    if isinstance(windows, dict):
        wallet_key = source_wallet.lower()
        for window_start in sorted(scoped_windows):
            wallet_spans = windows.get(str(window_start), {})
            if isinstance(wallet_spans, dict):
                spans.extend(span for span in wallet_spans.get(wallet_key, []) if isinstance(span, list) and len(span) == 2)
    indexed_bytes = sum(int(span[1]) for span in spans)
    file_size = int(source.get("size") or 0)
    return {
        "enabled": True,
        "index_path": str(Path(index_path) if index_path is not None else _default_history_window_index_path(path)),
        "file_size": file_size,
        "indexed_spans": len(spans),
        "indexed_bytes": indexed_bytes,
        "indexed_file_pct": round((indexed_bytes / file_size) * 100.0, 6) if file_size > 0 else 0.0,
    }


def _load_events_from_history_index(
    path: str | Path,
    *,
    source_wallet: str,
    window_starts: set[int],
    index_path: str | Path | None = None,
) -> list[WalletEvent]:
    stats = history_index_stats_for_scope(
        path,
        source_wallet=source_wallet,
        window_starts=window_starts,
        index_path=index_path,
    )
    spans: list[list[int]] = []
    index = load_history_window_index(path, index_path=index_path)
    windows = index.get("windows") if isinstance(index, dict) else {}
    if isinstance(windows, dict):
        wallet_key = source_wallet.lower()
        for window_start in sorted(window_starts):
            wallet_spans = windows.get(str(window_start), {})
            if isinstance(wallet_spans, dict):
                spans.extend(span for span in wallet_spans.get(wallet_key, []) if isinstance(span, list) and len(span) == 2)

    events: list[WalletEvent] = []
    with Path(path).open("rb") as handle:
        for offset, length in spans:
            handle.seek(int(offset))
            raw = handle.read(int(length))
            try:
                row = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(row, dict):
                continue
            if not _history_row_matches_scope(row, source_wallet=source_wallet, window_starts=window_starts):
                continue
            try:
                events.append(WalletEvent.from_dict(row))
            except TypeError:
                continue
    load_events_from_history.last_index_stats = stats
    return unique_wallet_events(events)


def _iter_history_event_rows(path: str | Path) -> Iterable[dict[str, Any]]:
    for row, _, _ in _iter_history_event_row_spans(path):
        yield row


def load_events_from_history(
    path: str | Path,
    *,
    source_wallet: str = "",
    window_starts: Iterable[int] | None = None,
    history_window_index_path: str | Path | None = None,
) -> list[WalletEvent]:
    load_events_from_history.last_index_stats = {"enabled": False}
    scoped_windows = {int(float(value)) for value in (window_starts or []) if value is not None}
    if source_wallet or scoped_windows:
        if source_wallet and scoped_windows:
            return _load_events_from_history_index(
                path,
                source_wallet=source_wallet,
                window_starts=scoped_windows,
                index_path=history_window_index_path,
            )
        events: list[WalletEvent] = []
        for row in _iter_history_event_rows(path):
            if not _history_row_matches_scope(row, source_wallet=source_wallet, window_starts=scoped_windows):
                continue
            try:
                events.append(WalletEvent.from_dict(row))
            except TypeError:
                continue
        return unique_wallet_events(events)

    payload = load_json(path, default={})
    rows = payload.get("events") if isinstance(payload, dict) else []
    events: list[WalletEvent] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            events.append(WalletEvent.from_dict(row))
        except TypeError:
            continue
    return unique_wallet_events(events)


def load_events_from_histories(paths: Iterable[str | Path]) -> list[WalletEvent]:
    events: list[WalletEvent] = []
    for path in paths:
        events.extend(load_events_from_history(path))
    return sorted(unique_wallet_events(events), key=lambda event: (event.event_ts or 0.0, event.event_id))


def default_candidate_policies(
    *,
    wallet_fractions: Iterable[float] = (0.02, 0.05, 0.1),
    max_order_usd_values: Iterable[float] = (1.0, 2.0, 5.0),
) -> list[CandidatePolicy]:
    price_bands = (
        (0.01, 1.0),
        (0.05, 0.95),
        (0.10, 0.90),
        (0.20, 0.80),
        (0.20, 0.60),
        (0.40, 0.80),
        (0.55, 1.0),
        (0.01, 0.50),
    )
    timing_bands: tuple[tuple[float | None, float | None, str], ...] = (
        (None, None, "all_window"),
        (0.0, 120.0, "first_120s"),
        (120.0, 300.0, "last_180s"),
        (0.0, 60.0, "first_60s"),
        (180.0, 300.0, "last_120s"),
    )
    min_sizes = (0.0, 2.0, 5.0)
    policies: list[CandidatePolicy] = []
    for fraction in wallet_fractions:
        for cap in max_order_usd_values:
            for min_price, max_price in price_bands:
                for min_size in min_sizes:
                    for min_from_open, max_from_open, timing_id in timing_bands:
                        policies.append(
                            CandidatePolicy(
                                policy_id=(
                                    f"wf_{fraction:g}_cap_{cap:g}_p_{min_price:g}_{max_price:g}_"
                                    f"minusd_{min_size:g}_{timing_id}"
                                ),
                                min_price=min_price,
                                max_price=max_price,
                                min_wallet_usdc=min_size,
                                min_seconds_from_open=min_from_open,
                                max_seconds_from_open=max_from_open,
                                wallet_fraction=fraction,
                                max_order_usd=cap,
                            )
                        )
    return policies


def fast_candidate_policies() -> list[CandidatePolicy]:
    """Small heartbeat-safe policy grid for large leaderboard universes."""

    price_bands = (
        (0.01, 1.0, "all_prices"),
        (0.20, 0.80, "mid_prices"),
        (0.20, 0.60, "cheap_up_to_60"),
        (0.55, 1.0, "late_high_conviction"),
    )
    timing_bands: tuple[tuple[float | None, float | None, str], ...] = (
        (None, None, "all_window"),
        (0.0, 90.0, "first_90s"),
        (90.0, 180.0, "middle_90s"),
        (180.0, 300.0, "last_120s"),
    )
    min_sizes = (0.0, 5.0)
    policies: list[CandidatePolicy] = []
    for fraction in (0.05,):
        for cap in (2.0,):
            for min_price, max_price, price_id in price_bands:
                for min_size in min_sizes:
                    for min_from_open, max_from_open, timing_id in timing_bands:
                        policies.append(
                            CandidatePolicy(
                                policy_id=(
                                    f"fast_wf_{fraction:g}_cap_{cap:g}_{price_id}_"
                                    f"minusd_{min_size:g}_{timing_id}"
                                ),
                                min_price=min_price,
                                max_price=max_price,
                                min_wallet_usdc=min_size,
                                min_seconds_from_open=min_from_open,
                                max_seconds_from_open=max_from_open,
                                wallet_fraction=fraction,
                                max_order_usd=cap,
                            )
                        )
    return policies


@lru_cache(maxsize=200_000)
def _cached_slug_window_start(market_slug: str) -> int | None:
    return slug_window_start(market_slug)


def _event_btc_5m_window_start_s(event: WalletEvent) -> int | None:
    if event.window_start_s is not None:
        try:
            return int(float(event.window_start_s))
        except (TypeError, ValueError):
            return None

    start = _cached_slug_window_start(event.market_slug)
    if start is not None:
        return start

    market_text = " ".join(
        str(value or "").lower()
        for value in (
            getattr(event, "market_slug", ""),
            getattr(event, "event_slug", ""),
            getattr(event, "title", ""),
        )
    )
    is_btc_5m = (
        ("btc-updown-5m" in market_text or "bitcoin-up-or-down" in market_text or "bitcoin up or down" in market_text)
        and ("5m" in market_text or "5 minutes" in market_text or re.search(r"\b5[- ]?minute", market_text))
    )
    if not is_btc_5m:
        return None

    if event.event_ts is None:
        return None
    try:
        event_ts = float(event.event_ts)
    except (TypeError, ValueError):
        return None
    if event_ts <= 0:
        return None
    return int(event_ts // 300) * 300


def _seconds_from_open(event: WalletEvent) -> float | None:
    start = _event_btc_5m_window_start_s(event)
    if start is None:
        start = event.window_start_s
    if start is None or event.event_ts is None:
        return None
    return float(event.event_ts) - float(start)


def policy_accepts_event(policy: CandidatePolicy, event: WalletEvent) -> tuple[bool, str]:
    if event.action.upper() != "BUY":
        return False, "not_buy"
    if not (float(policy.min_price) <= float(event.price) <= float(policy.max_price)):
        return False, "price_outside_policy"
    if float(event.usdc_size) < float(policy.min_wallet_usdc):
        return False, "wallet_size_below_minimum"
    if policy.max_wallet_usdc > 0 and float(event.usdc_size) > float(policy.max_wallet_usdc):
        return False, "wallet_size_above_maximum"
    seconds_from_open = _seconds_from_open(event)
    if policy.min_seconds_from_open is not None:
        min_open = float(policy.min_seconds_from_open)
        if min_open > 0 and (seconds_from_open is None or seconds_from_open < min_open):
            return False, "before_timing_band"
    if policy.max_seconds_from_open is not None and seconds_from_open is not None:
        if seconds_from_open > float(policy.max_seconds_from_open):
            return False, "after_timing_band"
    if policy.move_slice_keys:
        move_slice = btc_5m_move_slice_for_values(
            market_slug=event.market_slug,
            event_ts=float(event.event_ts or 0.0),
            price=float(event.price),
        )
        if str(move_slice.get("move_slice_key") or "") not in set(policy.move_slice_keys):
            return False, "move_slice_outside_policy"
    return True, "accepted"


def _copy_policy(policy: CandidatePolicy) -> CopyPolicy:
    return CopyPolicy(
        policy_id=policy.policy_id,
        strategy_family="wallet_copy_profit_search_v1",
        allowed_assets=("BTC",),
        market_filter="btc_5m",
        min_price=policy.min_price,
        max_price=policy.max_price,
        sizing=SizingPolicy(
            policy_id=f"wallet_fraction_{policy.wallet_fraction:g}_cap_{policy.max_order_usd:g}",
            basis="wallet_usdc_fraction",
            wallet_fraction=policy.wallet_fraction,
            max_order_usd=policy.max_order_usd,
            min_order_usd=policy.min_order_usd,
        ),
    )


def _accepted_events_for_policy(events: Iterable[WalletEvent], policy: CandidatePolicy) -> list[WalletEvent]:
    return [event for event in events if policy_accepts_event(policy, event)[0]]


def _recent_events_for_intent_build(
    events: Iterable[WalletEvent],
    *,
    max_events: int,
) -> tuple[list[WalletEvent], int, bool]:
    ordered = sorted(events, key=lambda event: (event.event_ts or 0.0, event.event_id))
    original_count = len(ordered)
    if max_events > 0 and original_count > int(max_events):
        return ordered[-int(max_events) :], original_count, True
    return ordered, original_count, False


def _intents_from_accepted_events(events: Iterable[WalletEvent], policy: CandidatePolicy) -> list[CopyIntent]:
    copy_policy = _copy_policy(policy)
    intents: dict[str, CopyIntent] = {}
    for event in events:
        intent = _profit_search_intent_from_event(event, copy_policy=copy_policy)
        if intent is not None:
            intents[intent.intent_id] = intent
    return sorted(intents.values(), key=lambda intent: (intent.event_ts or 0.0, intent.intent_id))


def intents_for_policy(events: list[WalletEvent], policy: CandidatePolicy) -> list[CopyIntent]:
    return _intents_from_accepted_events(_accepted_events_for_policy(events, policy), policy)


def _profit_search_intent_from_event(event: WalletEvent, *, copy_policy: CopyPolicy) -> CopyIntent | None:
    """Build the same CopyIntent identity path without expensive research metadata.

    Historical profit search may create millions of temporary intents. The live
    paper path still uses `event_to_intent`; here we preserve the fields that
    determine CopyIntent identity, sizing, token mapping, scoring, and parity
    while avoiding repeated source-fingerprint hashing that is not consumed by
    the profit engine.
    """

    accepted, reason = copy_policy.accepts(event)
    if not accepted:
        return None
    copy_size = copy_policy.sizing.size_usd(event)
    if copy_size <= 0:
        return None
    try:
        side = outcome_to_side(event.outcome)
    except ValueError:
        return None
    raw = event.raw if isinstance(event.raw, dict) else {}
    return CopyIntent(
        source_wallet=event.source_wallet,
        wallet_name=event.wallet_name,
        source_event_id=event.event_id,
        source_row_event_id=event.event_id,
        condition_id=event.condition_id,
        market_slug=event.market_slug,
        market_id=event.market_id,
        outcome=event.outcome,
        side=side,
        limit_price=round(float(event.price), 6),
        wallet_usdc_size=round(float(event.usdc_size), 6),
        copy_size_usd=copy_size,
        shares=round(copy_size / float(event.price), 6) if event.price > 0 else 0.0,
        observed_ts=event.observed_ts,
        strategy_family=copy_policy.strategy_family,
        policy_id=copy_policy.policy_id,
        sizing_policy_id=copy_policy.sizing.policy_id,
        mode="paper",
        order_type=copy_policy.order_type,
        token_id=event.token_id,
        event_ts=event.event_ts,
        api_latency_s=event.api_latency_s,
        live_orders_allowed=False,
        reason=reason,
        metadata={
            "row_type": event.row_type,
            "asset": event.asset,
            "duration": event.duration,
            "source_fingerprint": raw.get("source_fingerprint") or event.event_id,
            "transaction_hash": event.transaction_hash,
            "wallet_size": event.size,
            "wallet_copy_policy": {
                "min_price": copy_policy.min_price,
                "max_price": copy_policy.max_price,
                "max_event_age_s": copy_policy.max_event_age_s,
            },
            "profit_search_fast_intent": True,
        },
    )


def _intent_source_payload(intent: CopyIntent) -> dict[str, Any]:
    """Return only scoring-relevant intent fields without dataclass deep-copy."""

    return {
        "condition_id": intent.condition_id,
        "event_ts": intent.event_ts,
        "intent_id": intent.intent_id,
        "market_id": intent.market_id,
        "market_slug": intent.market_slug,
        "metadata": dict(intent.metadata or {}),
        "observed_ts": intent.observed_ts,
        "outcome": intent.outcome,
        "policy_id": intent.policy_id,
        "source_event_id": intent.source_event_id,
        "source_wallet": intent.source_wallet.lower(),
        "token_id": intent.token_id,
        "wallet_name": intent.wallet_name,
    }


def _order_from_intent(
    intent: CopyIntent,
    *,
    slippage_bps: float = 0.0,
    fill_config: FillModelConfig | None = None,
) -> dict[str, Any]:
    cfg = fill_config or FillModelConfig(fallback_slippage_bps=slippage_bps, allow_fallback_without_book=True)
    fill = estimate_executable_fill(intent, cfg)
    effective_price = float(fill.get("effective_price") or 0.0)
    shares = float(fill.get("filled_shares") or 0.0)
    filled_size = float(fill.get("filled_size_usd") or 0.0)
    status = "FILLED" if fill.get("status") == "FILLED" and filled_size > 0 else "REJECTED"
    return {
        "order_id": stable_id("bt", {"intent": intent.intent_id, "slippage_bps": slippage_bps}),
        "intent_id": intent.intent_id,
        "source_wallet": intent.source_wallet.lower(),
        "wallet_name": intent.wallet_name,
        "condition_id": intent.condition_id,
        "market_slug": intent.market_slug,
        "outcome": intent.outcome,
        "side": intent.side,
        "token_id": intent.token_id,
        "limit_price": round(effective_price, 6),
        "requested_size_usd": float(intent.copy_size_usd),
        "requested_shares": round(float(intent.copy_size_usd) / float(intent.limit_price), 6) if intent.limit_price > 0 else 0.0,
        "filled_size_usd": filled_size,
        "filled_shares": shares,
        "status": status,
        "final_status": status,
        "paper_only": True,
        "live_orders_allowed": False,
        "fill_model": str(fill.get("fill_model") or "executable_copy_fill_v1"),
        "fill_estimate": fill,
        "source_intent": _intent_source_payload(intent),
    }


def _fill_evidence_summary(orders: list[dict[str, Any]], scored: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for order, score in zip(orders, scored):
        fill = order.get("fill_estimate") if isinstance(order.get("fill_estimate"), dict) else {}
        rows.append(
            {
                "order_id": order.get("order_id"),
                "intent_id": order.get("intent_id"),
                "resolved": bool(score.get("resolved")),
                "final_status": order.get("final_status") or order.get("status"),
                "fill_source": fill.get("source"),
                "fill_status": fill.get("status"),
                "book_age_s": (fill.get("book") or {}).get("book_age_s") if isinstance(fill.get("book"), dict) else None,
            }
        )
    filled = [row for row in rows if str(row.get("final_status") or "").upper() == "FILLED"]
    resolved_filled = [row for row in filled if row.get("resolved")]
    clob_filled = [row for row in filled if row.get("fill_source") == "clob_book_evidence"]
    clob_resolved = [row for row in resolved_filled if row.get("fill_source") == "clob_book_evidence"]
    fallback_filled = [row for row in filled if row.get("fill_source") == "source_price_plus_slippage_fallback"]
    rejected = [row for row in rows if str(row.get("final_status") or "").upper() == "REJECTED"]
    return {
        "orders": len(rows),
        "filled_orders": len(filled),
        "rejected_orders": len(rejected),
        "resolved_filled_orders": len(resolved_filled),
        "candidate_clob_backed_orders": len(clob_filled),
        "candidate_clob_backed_resolved_orders": len(clob_resolved),
        "candidate_fallback_filled_orders": len(fallback_filled),
        "candidate_fallback_filled_resolved_orders": len(
            [row for row in resolved_filled if row.get("fill_source") == "source_price_plus_slippage_fallback"]
        ),
        "candidate_executable_fill_rate_pct": round(len(filled) / len(rows) * 100.0, 6) if rows else 0.0,
        "candidate_clob_backed_fill_rate_pct": round(len(clob_filled) / len(filled) * 100.0, 6) if filled else 0.0,
        "candidate_rejected_fill_count": len(rejected),
        "fill_source_counts": dict(
            sorted(
                {
                    str(source or "missing"): sum(1 for row in filled if row.get("fill_source") == source)
                    for source in {row.get("fill_source") for row in filled}
                }.items()
            )
        ),
    }


def score_intents(
    intents: list[CopyIntent],
    resolutions: dict[str, dict[str, Any]],
    *,
    slippage_bps: float = 0.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    fill_config = FillModelConfig(fallback_slippage_bps=slippage_bps, allow_fallback_without_book=True)
    orders = [_order_from_intent(intent, slippage_bps=slippage_bps, fill_config=fill_config) for intent in intents]
    scored = [score_order(order, resolutions) for order in orders]
    return scored, summarize_scores(scored)


def _score_intents_with_fill_summary(
    intents: list[CopyIntent],
    resolutions: dict[str, dict[str, Any]],
    *,
    slippage_bps: float = 0.0,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    fill_config = FillModelConfig(fallback_slippage_bps=slippage_bps, allow_fallback_without_book=True)
    orders = [_order_from_intent(intent, slippage_bps=slippage_bps, fill_config=fill_config) for intent in intents]
    scored = [score_order(order, resolutions) for order in orders]
    return scored, summarize_scores(scored), _fill_evidence_summary(orders, scored)


def _max_drawdown(rows: list[dict[str, Any]]) -> float:
    resolved = [row for row in rows if row.get("resolved")]
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for row in resolved:
        equity += float(row.get("pnl_usd") or 0.0)
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    return round(abs(max_drawdown), 6)


def _window_key(row: dict[str, Any]) -> str:
    return str(row.get("market_slug") or row.get("condition_id") or row.get("order_id") or "")


def _window_metrics(
    rows: list[dict[str, Any]],
    train_rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved")]
    all_windows = [_window_key(row) for row in resolved if _window_key(row)]
    train_windows = {_window_key(row) for row in train_rows if _window_key(row)}
    validation_windows = {_window_key(row) for row in validation_rows if _window_key(row)}
    window_counts = Counter(all_windows)
    max_orders_per_window = max(window_counts.values()) if window_counts else 0
    unique_windows = len(set(all_windows))
    return {
        "unique_windows": unique_windows,
        "train_unique_windows": len(train_windows),
        "validation_unique_windows": len(validation_windows),
        "train_validation_shared_windows": len(train_windows & validation_windows),
        "max_orders_per_window": max_orders_per_window,
        "avg_orders_per_window": round(len(resolved) / unique_windows, 6) if unique_windows else 0.0,
        "max_orders_per_window_ratio": round(max_orders_per_window / len(resolved), 6) if resolved else 0.0,
        "max_orders_per_window_ratio_pct": round(max_orders_per_window / len(resolved) * 100.0, 6)
        if resolved
        else 0.0,
        "orders_per_window_top": [
            {"window": window, "orders": count}
            for window, count in window_counts.most_common(10)
        ],
        "split_method": "walk_forward_grouped_by_market_window",
    }


def _resolution_evidence_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved")]
    unresolved = [row for row in rows if not row.get("resolved")]
    resolution_rows = [
        row.get("resolution")
        for row in resolved
        if isinstance(row.get("resolution"), dict)
    ]
    source_counts = Counter(str((row or {}).get("source") or "missing") for row in resolution_rows)
    research_only_rows = [
        row
        for row in resolution_rows
        if isinstance(row, dict) and bool(row.get("research_only"))
    ]
    return {
        "resolved_orders": len(resolved),
        "unresolved_orders": len(unresolved),
        "research_only_resolved_orders": len(research_only_rows),
        "canonical_resolved_orders": max(0, len(resolved) - len(research_only_rows)),
        "source_counts": dict(sorted(source_counts.items())),
        "has_research_only_labels": bool(research_only_rows),
    }


def _canonical_resolution_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical: list[dict[str, Any]] = []
    for row in rows:
        if not row.get("resolved"):
            continue
        resolution = row.get("resolution")
        if not isinstance(resolution, dict) or bool(resolution.get("research_only")):
            continue
        canonical.append(row)
    return canonical


def _canonical_resolution_profile(
    rows: list[dict[str, Any]],
    *,
    candidate_type: str,
    metadata: dict[str, Any],
    cfg: ProfitEngineConfig,
) -> dict[str, Any]:
    canonical_rows = _canonical_resolution_rows(rows)
    if not canonical_rows:
        return {
            "status": "BLOCKED",
            "blockers": ["canonical_resolution_rows_missing"],
            "summary": summarize_scores([]),
            "validation_summary": summarize_scores([]),
            "live_target_profile": {
                "status": "BLOCKED",
                "blockers": ["target_resolved_orders_below_minimum"],
            },
        }
    train_rows, validation_rows = _split_resolved_scores(canonical_rows, cfg)
    all_summary = summarize_scores(canonical_rows)
    train_summary = summarize_scores(train_rows)
    validation_summary = summarize_scores(validation_rows)
    window_metrics = _window_metrics(canonical_rows, train_rows, validation_rows)
    all_summary = {**all_summary, "window_metrics": window_metrics}
    train_summary = {**train_summary, "unique_windows": window_metrics["train_unique_windows"]}
    validation_summary = {**validation_summary, "unique_windows": window_metrics["validation_unique_windows"]}
    canonical_cfg = replace(cfg, require_candidate_clob_fill_evidence=False)
    drawdown = _max_drawdown(canonical_rows)
    status, blockers = _candidate_status(
        all_summary,
        train_summary,
        validation_summary,
        drawdown,
        canonical_cfg,
    )
    live_target_profile = _candidate_live_target_profile(
        candidate_type=candidate_type,
        all_summary=all_summary,
        validation_summary=validation_summary,
        metadata=metadata,
        cfg=cfg,
    )
    if live_target_profile.get("status") != PASS:
        blockers.extend(
            f"canonical_only_{blocker}"
            for blocker in live_target_profile.get("blockers") or []
        )
        status = "BLOCKED"
    return {
        "status": status,
        "blockers": blockers,
        "summary": all_summary,
        "train_summary": train_summary,
        "validation_summary": validation_summary,
        "max_drawdown_usd": drawdown,
        "live_target_profile": live_target_profile,
    }


def _window_start_from_row(row: dict[str, Any]) -> int | None:
    slug_start = slug_window_start(str(row.get("market_slug") or ""))
    if slug_start is not None:
        return int(slug_start)
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    for key in ("window_start_ts", "window_start", "market_window_start"):
        try:
            value = int(float(metadata.get(key)))
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def _latest_resolution_expiry(resolutions: dict[str, dict[str, Any]]) -> int | None:
    expiries: list[int] = []
    for row in resolutions.values():
        if not isinstance(row, dict):
            continue
        try:
            expiry = int(float(row.get("expiry_unix_ts")))
        except (TypeError, ValueError):
            continue
        if expiry > 0:
            expiries.append(expiry)
    return max(expiries) if expiries else None


def _resolution_coverage_summary(
    rows: list[dict[str, Any]],
    *,
    resolutions: dict[str, dict[str, Any]],
    latest_resolution_expiry: int | None,
) -> dict[str, Any]:
    unresolved = [row for row in rows if not row.get("resolved")]
    all_window_starts = [_window_start_from_row(row) for row in rows]
    unresolved_window_starts = [_window_start_from_row(row) for row in unresolved]
    all_windows = {window for window in all_window_starts if window is not None}
    unresolved_windows = {window for window in unresolved_window_starts if window is not None}
    unresolved_window_rows: list[tuple[int, str]] = []
    newer_than_index_window_rows: list[tuple[int, str]] = []
    matured_unresolved_window_rows: list[tuple[int, str]] = []
    for row, window_start in zip(unresolved, unresolved_window_starts, strict=False):
        if window_start is None:
            continue
        slug = _window_key(row)
        if not slug.startswith("btc-updown-5m-"):
            slug = f"btc-updown-5m-{window_start}"
        unresolved_window_rows.append((window_start, slug))
        if latest_resolution_expiry is not None and window_start + 300 <= latest_resolution_expiry:
            matured_unresolved_window_rows.append((window_start, slug))
        else:
            newer_than_index_window_rows.append((window_start, slug))

    def window_sample(rows_with_start: list[tuple[int, str]], limit: int = 25) -> list[str]:
        unique: dict[str, int] = {}
        for window_start, slug in rows_with_start:
            unique.setdefault(slug, window_start)
        return [
            slug
            for slug, _window_start in sorted(unique.items(), key=lambda item: item[1], reverse=True)[:limit]
        ]

    if latest_resolution_expiry is None:
        newer_than_index = len(unresolved)
        matured_unresolved = 0
        status = "RESOLUTION_INDEX_EMPTY"
    else:
        newer_than_index = sum(
            1
            for row, window_start in zip(unresolved, unresolved_window_starts, strict=False)
            if window_start is not None and window_start + 300 > latest_resolution_expiry
        )
        matured_unresolved = sum(
            1
            for row, window_start in zip(unresolved, unresolved_window_starts, strict=False)
            if window_start is not None and window_start + 300 <= latest_resolution_expiry
        )
        if not unresolved:
            status = "COMPLETE"
        elif newer_than_index == len(unresolved):
            status = "PENDING_NEWER_THAN_RESOLUTION_INDEX"
        elif matured_unresolved > 0:
            status = "MATURED_UNRESOLVED_GAP"
        else:
            status = "PARTIAL"
    return {
        "candidate_policy_history_events": len(rows),
        "canonical_resolution_indexed_keys": len(resolutions),
        "canonical_resolution_covered_windows": len(all_windows - unresolved_windows),
        "candidate_market_windows": len(all_windows),
        "candidate_unresolved_windows": len(unresolved_windows),
        "latest_resolution_expiry": latest_resolution_expiry,
        "newer_than_resolution_index_events": newer_than_index,
        "newer_than_resolution_index_window_sample": window_sample(newer_than_index_window_rows),
        "matured_unresolved_count": matured_unresolved,
        "matured_unresolved_window_sample": window_sample(matured_unresolved_window_rows),
        "unresolved_window_sample": window_sample(unresolved_window_rows),
        "resolution_coverage_status": status,
    }


def _split_resolved_scores(rows: list[dict[str, Any]], cfg: ProfitEngineConfig) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    resolved = [row for row in rows if row.get("resolved")]
    if not resolved:
        return [], []
    groups: "OrderedDict[str, list[dict[str, Any]]]" = OrderedDict()
    for row in resolved:
        groups.setdefault(_window_key(row), []).append(row)
    if len(groups) <= 1:
        return resolved, []
    target = int(len(resolved) * max(0.05, min(0.95, float(cfg.train_fraction))))
    train: list[dict[str, Any]] = []
    validation: list[dict[str, Any]] = []
    for _key, group in groups.items():
        if not train or (len(train) + len(group) <= target and len(groups) > 1):
            train.extend(group)
        else:
            validation.extend(group)
    if not validation and train:
        last_key = next(reversed(groups))
        last_group = groups[last_key]
        train = [row for row in train if _window_key(row) != last_key]
        validation = list(last_group)
    return train, validation


def _candidate_status(
    all_summary: dict[str, Any],
    train_summary: dict[str, Any],
    validation_summary: dict[str, Any],
    drawdown: float,
    cfg: ProfitEngineConfig,
    raw_baseline_summary: dict[str, Any] | None = None,
    fill_evidence_summary: dict[str, Any] | None = None,
) -> tuple[str, list[str]]:
    blockers: list[str] = []
    if int(all_summary.get("resolved_orders") or 0) < int(cfg.min_all_resolved):
        blockers.append("insufficient_all_resolved")
    if int(train_summary.get("resolved_orders") or 0) < int(cfg.min_train_resolved):
        blockers.append("insufficient_train_resolved")
    if int(validation_summary.get("resolved_orders") or 0) < int(cfg.min_validation_resolved):
        blockers.append("insufficient_validation_resolved")
    for prefix, summary in (("all", all_summary), ("train", train_summary), ("validation", validation_summary)):
        if float(summary.get("roi_pct") or 0.0) < float(cfg.min_roi_pct):
            blockers.append(f"{prefix}_roi_below_minimum")
        if float(summary.get("wr_pct") or 0.0) < float(cfg.min_wr_pct):
            blockers.append(f"{prefix}_wr_below_minimum")
    if float(all_summary.get("unresolved_ratio") or 0.0) > float(cfg.max_unresolved_ratio):
        blockers.append("unresolved_ratio_above_maximum")
    if drawdown > float(cfg.max_drawdown_usd):
        blockers.append("drawdown_above_maximum")
    window_metrics = all_summary.get("window_metrics") if isinstance(all_summary.get("window_metrics"), dict) else {}
    if int(window_metrics.get("train_validation_shared_windows") or 0) > 0:
        blockers.append("train_validation_window_overlap")
    if int(cfg.min_all_unique_windows) > 0 and int(window_metrics.get("unique_windows") or 0) < int(cfg.min_all_unique_windows):
        blockers.append("insufficient_unique_windows")
    if int(cfg.min_train_unique_windows) > 0 and int(window_metrics.get("train_unique_windows") or 0) < int(cfg.min_train_unique_windows):
        blockers.append("insufficient_train_unique_windows")
    if int(cfg.min_validation_unique_windows) > 0 and int(window_metrics.get("validation_unique_windows") or 0) < int(cfg.min_validation_unique_windows):
        blockers.append("insufficient_validation_unique_windows")
    max_orders_per_window = int(window_metrics.get("max_orders_per_window") or 0)
    resolved_orders = int(all_summary.get("resolved_orders") or 0)
    max_order_ratio = (float(max_orders_per_window) / float(resolved_orders)) if resolved_orders > 0 else 0.0
    max_orders_cap = int(cfg.max_candidate_orders_per_window)
    window_count_above_cap = max_orders_cap > 0 and max_orders_per_window > max_orders_cap
    if window_count_above_cap:
        blockers.append("candidate_window_order_concentration_above_maximum")
    if (
        float(cfg.max_candidate_orders_per_window_ratio) > 0.0
        and resolved_orders > 0
        and max_order_ratio > float(cfg.max_candidate_orders_per_window_ratio)
        and (max_orders_cap <= 0 or window_count_above_cap)
    ):
        blockers.append("candidate_window_order_concentration_ratio_above_maximum")
    if cfg.require_candidate_clob_fill_evidence:
        fill_summary = fill_evidence_summary or {}
        resolved_orders = int(all_summary.get("resolved_orders") or 0)
        clob_resolved = int(fill_summary.get("candidate_clob_backed_resolved_orders") or 0)
        if resolved_orders > 0 and clob_resolved < resolved_orders:
            blockers.append("candidate_missing_clob_fill_evidence")
        if int(fill_summary.get("candidate_rejected_fill_count") or 0) > 0:
            blockers.append("candidate_rejected_fill_events_present")
    if cfg.raw_baseline_guard and raw_baseline_summary:
        raw_resolved = int(raw_baseline_summary.get("resolved_orders") or 0)
        raw_roi = float(raw_baseline_summary.get("roi_pct") or 0.0)
        candidate_resolved = int(all_summary.get("resolved_orders") or 0)
        candidate_roi = float(all_summary.get("roi_pct") or 0.0)
        if raw_resolved >= int(cfg.min_raw_baseline_resolved) and raw_roi < 0.0:
            if candidate_resolved < int(cfg.min_filtered_resolved_when_raw_negative):
                blockers.append("raw_baseline_negative_candidate_sample_too_thin")
            if candidate_roi - raw_roi < float(cfg.min_candidate_vs_raw_delta_roi_pct):
                blockers.append("candidate_vs_raw_roi_delta_below_minimum")
    return ("PASS" if not blockers else "BLOCKED"), blockers


def _profit_score(all_summary: dict[str, Any], validation_summary: dict[str, Any], drawdown: float) -> float:
    window_metrics = all_summary.get("window_metrics") if isinstance(all_summary.get("window_metrics"), dict) else {}
    return round(
        float(validation_summary.get("pnl_usd") or 0.0) * 3.0
        + float(all_summary.get("pnl_usd") or 0.0)
        + float(validation_summary.get("roi_pct") or 0.0) * 0.05
        + float(validation_summary.get("wr_pct") or all_summary.get("wr_pct") or 0.0) * 0.1
        + float(window_metrics.get("avg_orders_per_window") or 0.0) * 2.0
        - drawdown * 0.25,
        6,
    )


def _inventory_plan_profile(plans: list[Any]) -> dict[str, Any]:
    pass_plans = [plan for plan in plans if getattr(plan, "status", "") == "PASS"]
    agreeing_wallet_counts = [len(getattr(plan, "source_wallets", ()) or ()) for plan in pass_plans]
    child_order_counts = [len(getattr(plan, "child_orders", ()) or ()) for plan in pass_plans]
    total_usd = sum(float(getattr(plan, "total_usd", 0.0) or 0.0) for plan in pass_plans)
    source_wallet_counts: Counter[str] = Counter()
    source_wallet_usd: Counter[str] = Counter()
    for plan in pass_plans:
        plan_usd = float(getattr(plan, "total_usd", 0.0) or 0.0)
        for wallet in getattr(plan, "source_wallets", ()) or ():
            address = str(wallet or "").lower()
            if not address.startswith("0x"):
                continue
            source_wallet_counts[address] += 1
            source_wallet_usd[address] += plan_usd
    top_source_wallets = [
        {
            "wallet": wallet,
            "pass_plans": int(source_wallet_counts[wallet]),
            "plan_usd": round(float(source_wallet_usd[wallet]), 6),
        }
        for wallet in sorted(
            source_wallet_counts,
            key=lambda item: (
                -int(source_wallet_counts[item]),
                -float(source_wallet_usd[item]),
                item,
            ),
        )[:25]
    ]
    return {
        "plans": len(plans),
        "pass_plans": len(pass_plans),
        "blocked_plans": max(0, len(plans) - len(pass_plans)),
        "unique_source_wallets": len(source_wallet_counts),
        "top_source_wallets": top_source_wallets,
        "min_agreeing_wallets": min(agreeing_wallet_counts) if agreeing_wallet_counts else 0,
        "max_agreeing_wallets": max(agreeing_wallet_counts) if agreeing_wallet_counts else 0,
        "avg_agreeing_wallets": round(sum(agreeing_wallet_counts) / len(agreeing_wallet_counts), 6)
        if agreeing_wallet_counts
        else 0.0,
        "avg_child_orders_per_plan": round(sum(child_order_counts) / len(child_order_counts), 6)
        if child_order_counts
        else 0.0,
        "total_plan_usd": round(total_usd, 6),
    }


def _candidate_live_target_profile(
    *,
    candidate_type: str,
    all_summary: dict[str, Any],
    validation_summary: dict[str, Any],
    metadata: dict[str, Any],
    cfg: ProfitEngineConfig,
) -> dict[str, Any]:
    window_metrics = all_summary.get("window_metrics") if isinstance(all_summary.get("window_metrics"), dict) else {}
    inventory_profile = metadata.get("inventory_profile") if isinstance(metadata.get("inventory_profile"), dict) else {}
    blockers: list[str] = []
    allowed_types = {str(row) for row in cfg.live_target_candidate_types if str(row)}
    if allowed_types and candidate_type not in allowed_types:
        blockers.append("target_candidate_type_not_allowed")
    if int(all_summary.get("resolved_orders") or 0) < int(cfg.live_target_min_resolved_orders):
        blockers.append("target_resolved_orders_below_minimum")
    if int(window_metrics.get("unique_windows") or 0) < int(cfg.live_target_min_unique_windows):
        blockers.append("target_unique_windows_below_minimum")
    if float(window_metrics.get("avg_orders_per_window") or 0.0) < float(cfg.live_target_min_avg_orders_per_window):
        blockers.append("target_avg_orders_per_window_below_minimum")
    if float(all_summary.get("wr_pct") or 0.0) < float(cfg.live_target_min_wr_pct):
        blockers.append("target_all_wr_below_70pct")
    if float(validation_summary.get("wr_pct") or 0.0) < float(cfg.live_target_min_validation_wr_pct):
        blockers.append("target_validation_wr_below_70pct")
    if float(all_summary.get("roi_pct") or 0.0) < float(cfg.live_target_min_roi_pct):
        blockers.append("target_roi_below_minimum")
    if float(all_summary.get("pnl_usd") or 0.0) <= float(cfg.live_target_min_pnl_usd):
        blockers.append("target_pnl_not_positive")
    if candidate_type == "MULTI_WALLET_INVENTORY":
        if int(inventory_profile.get("pass_plans") or 0) <= 0:
            blockers.append("target_inventory_pass_plans_missing")
        if int(inventory_profile.get("max_agreeing_wallets") or 0) < int(cfg.min_agreeing_wallets):
            blockers.append("target_inventory_agreeing_wallets_below_minimum")
    resolved_orders = int(all_summary.get("resolved_orders") or 0)
    unique_windows = int(window_metrics.get("unique_windows") or 0)
    avg_orders_per_window = float(window_metrics.get("avg_orders_per_window") or 0.0)
    wr_pct = float(all_summary.get("wr_pct") or 0.0)
    validation_wr_pct = float(validation_summary.get("wr_pct") or 0.0)
    roi_pct = float(all_summary.get("roi_pct") or 0.0)
    pnl_usd = float(all_summary.get("pnl_usd") or 0.0)
    return {
        "status": "PASS" if not blockers else "BLOCKED",
        "blockers": blockers,
        "target": {
            "candidate_types": sorted(allowed_types),
            "min_resolved_orders": int(cfg.live_target_min_resolved_orders),
            "min_unique_windows": int(cfg.live_target_min_unique_windows),
            "min_avg_orders_per_window": float(cfg.live_target_min_avg_orders_per_window),
            "min_wr_pct": float(cfg.live_target_min_wr_pct),
            "min_validation_wr_pct": float(cfg.live_target_min_validation_wr_pct),
            "min_roi_pct": float(cfg.live_target_min_roi_pct),
            "min_pnl_usd": float(cfg.live_target_min_pnl_usd),
            "min_agreeing_wallets": int(cfg.min_agreeing_wallets),
        },
        "observed": {
            "candidate_type": candidate_type,
            "resolved_orders": resolved_orders,
            "resolved_gap": max(0, int(cfg.live_target_min_resolved_orders) - resolved_orders),
            "unique_windows": unique_windows,
            "unique_window_gap": max(0, int(cfg.live_target_min_unique_windows) - unique_windows),
            "avg_orders_per_window": avg_orders_per_window,
            "avg_orders_per_window_gap": round(
                max(0.0, float(cfg.live_target_min_avg_orders_per_window) - avg_orders_per_window),
                6,
            ),
            "wr_pct": wr_pct,
            "wr_gap_pct": round(max(0.0, float(cfg.live_target_min_wr_pct) - wr_pct), 6),
            "validation_wr_pct": validation_wr_pct,
            "validation_wr_gap_pct": round(
                max(0.0, float(cfg.live_target_min_validation_wr_pct) - validation_wr_pct),
                6,
            ),
            "roi_pct": roi_pct,
            "roi_gap_pct": round(max(0.0, float(cfg.live_target_min_roi_pct) - roi_pct), 6),
            "pnl_usd": pnl_usd,
            "pnl_gap_usd": round(max(0.0, float(cfg.live_target_min_pnl_usd) - pnl_usd), 6),
            "inventory_profile": inventory_profile,
        },
    }


def evaluate_candidate(
    *,
    candidate_type: str,
    policy: CandidatePolicy,
    intents: list[CopyIntent],
    resolutions: dict[str, dict[str, Any]],
    cfg: ProfitEngineConfig,
    metadata: dict[str, Any] | None = None,
    raw_baseline_summary: dict[str, Any] | None = None,
    latest_resolution_expiry: int | None = None,
) -> dict[str, Any]:
    all_scored, all_summary, fill_summary = _score_intents_with_fill_summary(
        intents,
        resolutions,
        slippage_bps=cfg.slippage_bps,
    )
    train_scored, validation_scored = _split_resolved_scores(all_scored, cfg)
    train_summary = summarize_scores(train_scored)
    validation_summary = summarize_scores(validation_scored)
    window_metrics = _window_metrics(all_scored, train_scored, validation_scored)
    all_summary = {**all_summary, "window_metrics": window_metrics}
    train_summary = {**train_summary, "unique_windows": window_metrics["train_unique_windows"]}
    validation_summary = {**validation_summary, "unique_windows": window_metrics["validation_unique_windows"]}
    resolution_evidence = {
        **_resolution_evidence_summary(all_scored),
        **_resolution_coverage_summary(
            all_scored,
            resolutions=resolutions,
            latest_resolution_expiry=latest_resolution_expiry,
        ),
    }
    metadata_payload = metadata or {}
    canonical_resolution_profile = _canonical_resolution_profile(
        all_scored,
        candidate_type=candidate_type,
        metadata=metadata_payload,
        cfg=cfg,
    )
    drawdown = _max_drawdown(all_scored)
    status, blockers = _candidate_status(
        all_summary,
        train_summary,
        validation_summary,
        drawdown,
        cfg,
        raw_baseline_summary=raw_baseline_summary,
        fill_evidence_summary=fill_summary,
    )
    canonical_resolution_pass = canonical_resolution_profile.get("status") == PASS
    if int(resolution_evidence.get("research_only_resolved_orders") or 0) > 0 and not canonical_resolution_pass:
        blockers.append("candidate_research_only_resolution_evidence")
        status = "BLOCKED"
    live_target_profile = _candidate_live_target_profile(
        candidate_type=candidate_type,
        all_summary=all_summary,
        validation_summary=validation_summary,
        metadata=metadata_payload,
        cfg=cfg,
    )
    if metadata_payload.get("intents_limited") or metadata_payload.get("base_intents_limited"):
        blockers.append("candidate_scored_on_limited_history")
        status = "BLOCKED"
    candidate_id = stable_id(
        "wcp",
        {
            "candidate_type": candidate_type,
            "policy_id": policy.policy_id,
            "intent_ids": [intent.intent_id for intent in intents[:200]],
            "intent_count": len(intents),
            "slippage_bps": cfg.slippage_bps,
        },
    )
    candidate_key = _candidate_admission_key(
        candidate_type=candidate_type,
        policy=policy,
        metadata=metadata_payload,
        slippage_bps=cfg.slippage_bps,
    )
    return {
        "candidate_id": candidate_id,
        "candidate_key": candidate_key,
        "candidate_id_aliases": [candidate_id],
        "admission_identity": {
            "candidate_key": candidate_key,
            "candidate_type": candidate_type,
            "source_wallet": str(metadata_payload.get("source_wallet") or "").lower(),
            "policy_id": policy.policy_id,
            "sample_dependent_candidate_id": candidate_id,
        },
        "candidate_type": candidate_type,
        "status": status,
        "blockers": blockers,
        "profit_score": _profit_score(all_summary, validation_summary, drawdown),
        "policy": policy.asdict(),
        "intent_count": len(intents),
        "summary": all_summary,
        "fill_evidence_summary": fill_summary,
        "resolution_evidence_summary": resolution_evidence,
        "canonical_resolution_profile": canonical_resolution_profile,
        "optimistic_replay_summary": all_summary,
        "executable_copy_summary": fill_summary,
        "train_summary": train_summary,
        "validation_summary": validation_summary,
        "max_drawdown_usd": drawdown,
        "raw_baseline_summary": raw_baseline_summary or {},
        "live_target_profile": live_target_profile,
        "sample_intent_ids": [intent.intent_id for intent in intents[:20]],
        "metadata": metadata_payload,
    }


def _by_wallet(events: list[WalletEvent]) -> dict[str, list[WalletEvent]]:
    grouped: dict[str, list[WalletEvent]] = {}
    for event in events:
        grouped.setdefault(event.source_wallet.lower(), []).append(event)
    return grouped


def _event_window_key(event: WalletEvent) -> str:
    return str(event.market_slug or event.condition_id or event.event_id or "")


def _policy_id(candidate: dict[str, Any]) -> str:
    policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    return str(policy.get("policy_id") or "")


def _summary(candidate: dict[str, Any]) -> dict[str, Any]:
    return candidate.get("summary") if isinstance(candidate.get("summary"), dict) else {}


def _window_summary(candidate: dict[str, Any]) -> dict[str, Any]:
    summary = _summary(candidate)
    return summary.get("window_metrics") if isinstance(summary.get("window_metrics"), dict) else {}


def _candidate_wallet(candidate: dict[str, Any]) -> str:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    return str(metadata.get("source_wallet") or "").lower()


def _candidate_wallet_name(candidate: dict[str, Any], *, fallback_wallet: str = "") -> str:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    return str(metadata.get("wallet_name") or fallback_wallet)


def _single_wallet_candidate_better(left: dict[str, Any], right: dict[str, Any] | None) -> bool:
    if not isinstance(right, dict):
        return True
    left_summary = _summary(left)
    right_summary = _summary(right)
    return (
        0 if left.get("status") == "PASS" else 1,
        -float(left.get("profit_score") or 0.0),
        -float(left_summary.get("roi_pct") or 0.0),
        -int(left_summary.get("resolved_orders") or 0),
        -float(left_summary.get("wr_pct") or 0.0),
    ) < (
        0 if right.get("status") == "PASS" else 1,
        -float(right.get("profit_score") or 0.0),
        -float(right_summary.get("roi_pct") or 0.0),
        -int(right_summary.get("resolved_orders") or 0),
        -float(right_summary.get("wr_pct") or 0.0),
    )


def _individual_wallet_copy_universe(
    *,
    events: list[WalletEvent],
    searched_events: list[WalletEvent],
    candidates: list[dict[str, Any]],
    wallet_search_summary: dict[str, Any],
    cfg: ProfitEngineConfig,
) -> dict[str, Any]:
    """Summarize the broad per-wallet paper-copy universe without changing gates."""

    buy_events = [event for event in events if event.is_buy]
    searched_buy_events = [event for event in searched_events if event.is_buy]
    grouped = _by_wallet(events)
    buy_grouped = _by_wallet(buy_events)
    source_wallets_with_buys = len(buy_grouped)
    source_buy_windows = {_event_window_key(event) for event in buy_events if _event_window_key(event)}
    candidate_rows = [row for row in candidates if row.get("candidate_type") == "SINGLE_WALLET"]
    candidate_wallets: dict[str, dict[str, Any]] = {}
    candidate_counts_by_wallet: Counter[str] = Counter()
    pass_counts_by_wallet: Counter[str] = Counter()
    candidate_order_slots = 0
    candidate_resolved_slots = 0
    for candidate in candidate_rows:
        wallet = _candidate_wallet(candidate)
        if not wallet:
            continue
        candidate_counts_by_wallet[wallet] += 1
        if candidate.get("status") == "PASS":
            pass_counts_by_wallet[wallet] += 1
        candidate_order_slots += int(candidate.get("intent_count") or 0)
        candidate_resolved_slots += int(_summary(candidate).get("resolved_orders") or 0)
        current_best = candidate_wallets.get(wallet)
        if _single_wallet_candidate_better(candidate, current_best):
            candidate_wallets[wallet] = candidate

    top_activity = []
    for wallet, wallet_events in buy_grouped.items():
        windows = {_event_window_key(event) for event in wallet_events if _event_window_key(event)}
        top_activity.append(
            {
                "wallet": wallet,
                "wallet_name": wallet_events[0].wallet_name if wallet_events else wallet,
                "buy_events": len(wallet_events),
                "unique_windows": len(windows),
                "buy_usdc": round(sum(float(event.usdc_size or 0.0) for event in wallet_events), 6),
                "first_event_ts": min((event.event_ts for event in wallet_events if event.event_ts is not None), default=None),
                "latest_event_ts": max((event.event_ts for event in wallet_events if event.event_ts is not None), default=None),
            }
        )
    top_activity = sorted(
        top_activity,
        key=lambda row: (-int(row.get("buy_events") or 0), -float(row.get("buy_usdc") or 0.0), str(row.get("wallet"))),
    )

    top_candidates = []
    for wallet, candidate in candidate_wallets.items():
        summary = _summary(candidate)
        windows = _window_summary(candidate)
        top_candidates.append(
            {
                "wallet": wallet,
                "wallet_name": _candidate_wallet_name(candidate, fallback_wallet=wallet),
                "candidate_id": candidate.get("candidate_id"),
                "policy_id": _policy_id(candidate),
                "status": candidate.get("status"),
                "blockers": candidate.get("blockers") or [],
                "candidate_count": int(candidate_counts_by_wallet.get(wallet, 0)),
                "pass_candidate_count": int(pass_counts_by_wallet.get(wallet, 0)),
                "profit_score": candidate.get("profit_score"),
                "orders": int(candidate.get("intent_count") or 0),
                "resolved_orders": int(summary.get("resolved_orders") or 0),
                "unique_windows": int(windows.get("unique_windows") or 0),
                "avg_orders_per_window": windows.get("avg_orders_per_window"),
                "wr_pct": summary.get("wr_pct"),
                "roi_pct": summary.get("roi_pct"),
                "pnl_usd": summary.get("pnl_usd"),
            }
        )
    top_candidates = sorted(
        top_candidates,
        key=lambda row: (
            0 if row.get("status") == "PASS" else 1,
            -float(row.get("profit_score") or 0.0),
            -int(row.get("resolved_orders") or 0),
            -float(row.get("roi_pct") or 0.0),
            str(row.get("wallet")),
        ),
    )

    target_min_wallets = 100
    target_min_buy_events = 1000
    target_min_resolved_slots = max(1000, int(cfg.live_target_min_resolved_orders) * 10)
    source_buy_depth_scaled = len(buy_events) >= target_min_buy_events
    resolved_slots_scaled = candidate_resolved_slots >= target_min_resolved_slots
    per_wallet_surface_scaled = source_buy_depth_scaled and resolved_slots_scaled

    blockers: list[str] = []
    if source_wallets_with_buys < target_min_wallets and not per_wallet_surface_scaled:
        blockers.append("per_wallet_source_wallet_count_below_scale_target")
    if len(buy_events) < target_min_buy_events:
        blockers.append("per_wallet_source_buy_events_below_scale_target")
    if not candidate_rows:
        blockers.append("per_wallet_candidate_scoring_missing")
    if candidate_resolved_slots < target_min_resolved_slots:
        blockers.append("per_wallet_candidate_resolved_order_slots_below_scale_target")
    if int(wallet_search_summary.get("skipped_wallets") or 0) > 0:
        blockers.append("per_wallet_search_bounded_not_full_universe")

    return {
        "schema_version": 1,
        "mode": "copy_each_wallet_individually_in_paper",
        "paper_only": True,
        "live_orders_allowed": False,
        "copy_all_observed_wallets_individually": True,
        "status": PASS if not blockers else ANALYZE,
        "blockers": blockers,
        "target": {
            "min_source_wallets_with_buy_events": target_min_wallets,
            "min_source_buy_events": target_min_buy_events,
            "min_candidate_resolved_order_slots": target_min_resolved_slots,
            "source_buy_depth_can_satisfy_scale_when_wallet_count_is_below_target": True,
            "live_gate_relaxation_allowed": False,
        },
        "coverage": {
            "source_wallets_seen": len(grouped),
            "source_wallets_with_buy_events": source_wallets_with_buys,
            "source_buy_events": len(buy_events),
            "source_buy_unique_windows": len(source_buy_windows),
            "searched_buy_events": len(searched_buy_events),
            "single_wallet_candidate_count": len(candidate_rows),
            "wallets_with_single_wallet_candidates": len(candidate_wallets),
            "wallets_with_pass_single_wallet_candidates": sum(1 for value in pass_counts_by_wallet.values() if value > 0),
            "candidate_policy_order_slots": candidate_order_slots,
            "candidate_policy_resolved_order_slots": candidate_resolved_slots,
            "source_buy_depth_scaled": source_buy_depth_scaled,
            "candidate_resolved_slots_scaled": resolved_slots_scaled,
            "per_wallet_surface_scaled": per_wallet_surface_scaled,
            "wallet_search_summary": wallet_search_summary,
        },
        "top_wallets_by_activity": top_activity[:50],
        "top_wallets_by_candidate_score": top_candidates[:50],
        "next_action": (
            "continue_copying_every_leaderboard_wallet_individually_in_paper_and_feed_best_wallets_to_inventory"
            if not blockers
            else "expand_or_resume_per_wallet_copy_universe_before_claiming_scale"
        ),
    }


def _inventory_candidate_projection(candidate: dict[str, Any]) -> dict[str, Any]:
    summary = _summary(candidate)
    windows = _window_summary(candidate)
    fill = candidate.get("fill_evidence_summary") if isinstance(candidate.get("fill_evidence_summary"), dict) else {}
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    target_profile = (
        candidate.get("live_target_profile")
        if isinstance(candidate.get("live_target_profile"), dict)
        else {}
    )
    inventory_profile = metadata.get("inventory_profile") if isinstance(metadata.get("inventory_profile"), dict) else {}
    return {
        "candidate_id": candidate.get("candidate_id"),
        "candidate_type": candidate.get("candidate_type"),
        "policy_id": _policy_id(candidate),
        "status": candidate.get("status"),
        "blockers": candidate.get("blockers") or [],
        "profit_score": candidate.get("profit_score"),
        "orders": int(candidate.get("intent_count") or 0),
        "resolved_orders": int(summary.get("resolved_orders") or 0),
        "unique_windows": int(windows.get("unique_windows") or 0),
        "avg_orders_per_window": windows.get("avg_orders_per_window"),
        "wr_pct": summary.get("wr_pct"),
        "validation_wr_pct": (candidate.get("validation_summary") or {}).get("wr_pct")
        if isinstance(candidate.get("validation_summary"), dict)
        else None,
        "roi_pct": summary.get("roi_pct"),
        "pnl_usd": summary.get("pnl_usd"),
        "clob_backed_fill_rate_pct": fill.get("candidate_clob_backed_fill_rate_pct"),
        "fallback_filled_orders": fill.get("candidate_fallback_filled_orders"),
        "rejected_orders": fill.get("candidate_rejected_fill_count"),
        "base_intents": metadata.get("base_intents"),
        "original_base_intents": metadata.get("original_base_intents"),
        "base_intents_limited": metadata.get("base_intents_limited"),
        "plans": metadata.get("plans"),
        "inventory_profile": inventory_profile,
        "live_target_status": target_profile.get("status"),
        "live_target_blockers": target_profile.get("blockers") or [],
    }


def _inventory_scaled_rebuild_projection(
    *,
    individual_wallet_universe: dict[str, Any] | None,
    total_order_slots: int,
    total_plan_slots: int,
    max_base_intents: int,
    target_min_order_slots: int,
) -> dict[str, Any]:
    individual_coverage = (
        individual_wallet_universe.get("coverage")
        if isinstance(individual_wallet_universe, dict)
        and isinstance(individual_wallet_universe.get("coverage"), dict)
        else {}
    )
    per_wallet_order_slots = int(individual_coverage.get("candidate_policy_order_slots") or 0)
    per_wallet_resolved_slots = int(individual_coverage.get("candidate_policy_resolved_order_slots") or 0)
    per_wallet_source_buy_events = int(individual_coverage.get("source_buy_events") or 0)
    per_wallet_surface_scaled = bool(individual_coverage.get("per_wallet_surface_scaled"))
    projected_order_slots = max(
        int(total_order_slots),
        int(total_plan_slots),
        int(max_base_intents),
        int(per_wallet_order_slots),
    )
    projected_resolved_slots = max(0, int(per_wallet_resolved_slots))
    rebuild_gap = max(0, int(target_min_order_slots) - int(total_order_slots))
    projected_gap = max(0, int(target_min_order_slots) - int(projected_order_slots))
    return {
        "status": PASS
        if per_wallet_surface_scaled and projected_order_slots >= int(target_min_order_slots)
        else ANALYZE,
        "role": "paper_only_scaled_rebuild_projection_not_live_admission",
        "paper_only": True,
        "live_orders_allowed": False,
        "current_inventory_candidate_order_slots": int(total_order_slots),
        "current_gap_to_scale_target": rebuild_gap,
        "projected_inventory_order_slots_from_full_per_wallet_universe": projected_order_slots,
        "projected_resolved_order_slots_from_full_per_wallet_universe": projected_resolved_slots,
        "projected_gap_to_scale_target": projected_gap,
        "per_wallet_candidate_policy_order_slots": per_wallet_order_slots,
        "per_wallet_candidate_policy_resolved_order_slots": per_wallet_resolved_slots,
        "per_wallet_source_buy_events": per_wallet_source_buy_events,
        "per_wallet_surface_scaled": per_wallet_surface_scaled,
        "inventory_plan_slots": int(total_plan_slots),
        "max_original_base_intents": int(max_base_intents),
        "target_min_order_slots": int(target_min_order_slots),
        "next_action": "rebuild_scaled_multi_wallet_inventory_from_full_per_wallet_copy_universe",
    }


def _multi_wallet_inventory_universe(
    *,
    candidates: list[dict[str, Any]],
    cfg: ProfitEngineConfig,
    individual_wallet_universe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    inventory_candidates = [
        row for row in candidates if row.get("candidate_type") == "MULTI_WALLET_INVENTORY"
    ]
    projected = [_inventory_candidate_projection(row) for row in inventory_candidates]
    projected = sorted(
        projected,
        key=lambda row: (
            0 if row.get("status") == "PASS" else 1,
            0 if row.get("live_target_status") == "PASS" else 1,
            -float(row.get("profit_score") or 0.0),
            -int(row.get("resolved_orders") or 0),
            -float(row.get("avg_orders_per_window") or 0.0),
        ),
    )
    total_order_slots = sum(int(row.get("orders") or 0) for row in projected)
    total_resolved_slots = sum(int(row.get("resolved_orders") or 0) for row in projected)
    total_plan_slots = sum(int(row.get("plans") or 0) for row in projected)
    max_base_intents = max((int(row.get("original_base_intents") or row.get("base_intents") or 0) for row in projected), default=0)
    pass_candidates = [row for row in projected if row.get("status") == "PASS"]
    live_target_pass_candidates = [row for row in projected if row.get("live_target_status") == "PASS"]
    target_min_order_slots = max(1000, int(cfg.live_target_min_resolved_orders) * 10)
    scaled_rebuild_projection = _inventory_scaled_rebuild_projection(
        individual_wallet_universe=individual_wallet_universe,
        total_order_slots=total_order_slots,
        total_plan_slots=total_plan_slots,
        max_base_intents=max_base_intents,
        target_min_order_slots=target_min_order_slots,
    )
    blockers: list[str] = []
    if not projected:
        blockers.append("multi_wallet_inventory_candidates_missing")
    if total_order_slots < target_min_order_slots:
        if scaled_rebuild_projection.get("status") == PASS:
            blockers.append("multi_wallet_inventory_requires_scaled_rebuild_from_full_per_wallet_universe")
        else:
            blockers.append("multi_wallet_inventory_order_slots_below_scale_target")
    if not pass_candidates:
        blockers.append("multi_wallet_inventory_no_profit_pass_candidate")
    if not live_target_pass_candidates:
        blockers.append("multi_wallet_inventory_no_live_target_profile_pass")
    if any(row.get("base_intents_limited") for row in projected):
        blockers.append("multi_wallet_inventory_scored_on_limited_base_intents")

    return {
        "schema_version": 1,
        "mode": "weighted_multi_wallet_inventory_from_per_wallet_copy_universe",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": PASS if not blockers else ANALYZE,
        "blockers": blockers,
        "target": {
            "candidate_type": "MULTI_WALLET_INVENTORY",
            "min_order_slots": target_min_order_slots,
            "min_resolved_orders_per_live_candidate": int(cfg.live_target_min_resolved_orders),
            "min_unique_windows": int(cfg.live_target_min_unique_windows),
            "min_avg_orders_per_window": float(cfg.live_target_min_avg_orders_per_window),
            "min_agreeing_wallets": int(cfg.min_agreeing_wallets),
            "live_gate_relaxation_allowed": False,
        },
        "coverage": {
            "inventory_candidate_count": len(projected),
            "pass_inventory_candidate_count": len(pass_candidates),
            "live_target_profile_pass_candidate_count": len(live_target_pass_candidates),
            "inventory_candidate_order_slots": total_order_slots,
            "inventory_candidate_resolved_order_slots": total_resolved_slots,
            "inventory_plan_slots": total_plan_slots,
            "max_original_base_intents": max_base_intents,
            "scaled_rebuild_projected_order_slots": scaled_rebuild_projection.get(
                "projected_inventory_order_slots_from_full_per_wallet_universe"
            ),
            "scaled_rebuild_projected_resolved_order_slots": scaled_rebuild_projection.get(
                "projected_resolved_order_slots_from_full_per_wallet_universe"
            ),
        },
        "scaled_rebuild_projection": scaled_rebuild_projection,
        "best_inventory_candidate": projected[0] if projected else {},
        "top_inventory_candidates": projected[:50],
        "next_action": (
            "run_current_poll_multi_wallet_inventory_burnin_and_attach_candidate_specific_clob_truth"
            if projected and "multi_wallet_inventory_requires_scaled_rebuild_from_full_per_wallet_universe" not in blockers
            else "rebuild_weighted_multi_wallet_inventory_search_from_full_per_wallet_copy_universe"
        ),
    }


def _select_wallet_groups(
    events: list[WalletEvent],
    *,
    max_wallets: int = 0,
) -> tuple[list[tuple[str, list[WalletEvent]]], dict[str, Any]]:
    all_groups = list(_by_wallet(events).items())
    ranked = sorted(
        all_groups,
        key=lambda item: (
            -sum(1 for event in item[1] if event.action.upper() == "BUY"),
            -sum(float(event.usdc_size or 0.0) for event in item[1] if event.action.upper() == "BUY"),
            item[0],
        ),
    )
    if max_wallets > 0:
        selected = ranked[: int(max_wallets)]
    else:
        selected = ranked
    selected_addresses = {wallet for wallet, _events in selected}
    return selected, {
        "all_wallets": len(all_groups),
        "searched_wallets": len(selected),
        "skipped_wallets": max(0, len(all_groups) - len(selected)),
        "max_wallets_for_search": int(max_wallets),
        "selected_wallets": [
            {
                "source_wallet": wallet,
                "wallet_name": wallet_events[0].wallet_name if wallet_events else wallet,
                "events": len(wallet_events),
                "buy_events": sum(1 for event in wallet_events if event.action.upper() == "BUY"),
                "buy_usdc": round(sum(float(event.usdc_size or 0.0) for event in wallet_events if event.action.upper() == "BUY"), 6),
            }
            for wallet, wallet_events in selected[:50]
        ],
        "skipped_wallet_addresses": [wallet for wallet, _events in ranked if wallet not in selected_addresses][:100],
    }


def _raw_baseline_summary(
    wallet_events: list[WalletEvent],
    *,
    policy: CandidatePolicy,
    resolutions: dict[str, dict[str, Any]],
    cfg: ProfitEngineConfig,
) -> dict[str, Any]:
    raw_policy = CandidatePolicy(
        policy_id=f"raw_all_buys_baseline_for_{policy.policy_id}",
        min_price=0.01,
        max_price=1.0,
        wallet_fraction=policy.wallet_fraction,
        max_order_usd=policy.max_order_usd,
        min_order_usd=policy.min_order_usd,
    )
    raw_intents = intents_for_policy(wallet_events, raw_policy)
    _scored, summary, fill_summary = _score_intents_with_fill_summary(
        raw_intents,
        resolutions,
        slippage_bps=cfg.slippage_bps,
    )
    return {
        **summary,
        "policy_id": raw_policy.policy_id,
        "intent_count": len(raw_intents),
        "guard_role": "full_raw_same_wallet_all_buys_baseline",
        "fill_evidence_summary": fill_summary,
        "resolution_evidence_summary": _resolution_evidence_summary(_scored),
    }


def _resolution_contract(path: str | Path, *, indexed_count: int) -> dict[str, Any]:
    p = Path(path)
    rows: list[dict[str, Any]] = []
    if p.exists():
        for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    source_counts = Counter(str(row.get("source") or "missing") for row in rows)
    research_only_count = sum(1 for row in rows if bool(row.get("research_only")))
    return {
        "path": str(path),
        "row_count": len(rows),
        "indexed_key_count": int(indexed_count),
        "research_only_count": research_only_count,
        "canonical_count": max(0, len(rows) - research_only_count),
        "source_counts": dict(sorted(source_counts.items())),
    }


def _p95(values: Iterable[Any]) -> float | None:
    clean: list[float] = []
    for value in values:
        try:
            clean.append(float(value))
        except (TypeError, ValueError):
            continue
    if not clean:
        return None
    clean.sort()
    index = int(round((len(clean) - 1) * 0.95))
    return round(clean[index], 6)


def _event_log_path_for_tracker_state(path: str | Path) -> Path:
    state_path = Path(path)
    name = state_path.name
    if name.endswith(".json"):
        name = name[: -len(".json")] + ".jsonl"
    if "_state" in name:
        name = name.replace("_state", "_events", 1)
    return state_path.with_name(name)


def _tail_jsonl_dicts(path: str | Path, *, max_bytes: int = 8_000_000, max_rows: int = 5000) -> list[dict[str, Any]]:
    log_path = Path(path)
    if not log_path.exists() or not log_path.is_file():
        return []
    try:
        size = log_path.stat().st_size
        with log_path.open("rb") as handle:
            if size > max_bytes:
                handle.seek(-max_bytes, 2)
                handle.readline()
            rows: list[dict[str, Any]] = []
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                try:
                    payload = json.loads(raw_line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
                    if len(rows) > max_rows:
                        rows = rows[-max_rows:]
            return rows
    except OSError:
        return []


def _copy_efficiency_event_scores_from_log(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in _tail_jsonl_dicts(_event_log_path_for_tracker_state(path)):
        score = payload.get("copy_efficiency") if isinstance(payload.get("copy_efficiency"), dict) else {}
        if not score:
            continue
        row = dict(score)
        for key in (
            "action",
            "candidate_id",
            "data_api_trade_query_keys",
            "data_api_trade_query_scope",
            "event_ts",
            "generated_at",
            "live_orders_allowed",
            "market_slug",
            "outcome",
            "paper_only",
            "profit_policy_candidate_id",
            "profit_policy_context_id",
            "source_event_id",
            "source_fingerprint",
            "source_price",
            "source_usdc_size",
            "tx_hash",
        ):
            if row.get(key) is None and payload.get(key) is not None:
                row[key] = payload.get(key)
        if row.get("wallet_action") is None and payload.get("action") is not None:
            row["wallet_action"] = payload.get("action")
        rows.append(row)
    return rows


def _dedupe_event_scores(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: OrderedDict[tuple[str, ...], dict[str, Any]] = OrderedDict()
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = (
            str(row.get("source_event_id") or row.get("source_fingerprint") or ""),
            str(row.get("candidate_id") or row.get("profit_policy_candidate_id") or ""),
            str(row.get("policy_id") or row.get("profit_policy_id") or row.get("profit_policy_context_id") or ""),
            str(row.get("copy_status") or ""),
            str(row.get("fill_source") or ""),
            str(row.get("market_slug") or ""),
        )
        if not any(key):
            key = (str(len(deduped)),)
        deduped[key] = row
    return list(deduped.values())


def _candidate_copy_truth_summary(
    *,
    buy_scores: list[dict[str, Any]],
    required_buy_scores: list[dict[str, Any]],
    filtered_buy_scores: list[dict[str, Any]],
) -> dict[str, Any]:
    clob_filled = [
        row
        for row in required_buy_scores
        if row.get("copy_status") == "COPIED_FILLED"
        and str(row.get("fill_source") or "") == "clob_book_evidence"
    ]
    fallback_filled = [
        row
        for row in required_buy_scores
        if row.get("copy_status") == "COPIED_FILLED"
        and str(row.get("fill_source") or "") != "clob_book_evidence"
    ]
    rejected = [
        row
        for row in required_buy_scores
        if str(row.get("copy_status") or "") in {"COPY_REJECTED", "REJECTED"}
        or str(row.get("paper_final_status") or "") == "REJECTED"
    ]
    missed = [row for row in required_buy_scores if str(row.get("copy_status") or "") == "MISSED"]
    clob_source_events = {str(row.get("source_event_id") or "") for row in clob_filled if row.get("source_event_id")}
    clob_market_windows = {str(row.get("market_slug") or "") for row in clob_filled if row.get("market_slug")}
    required_event_ages = [row.get("event_age_s") for row in required_buy_scores]
    required_api_latencies = [row.get("api_latency_s") for row in required_buy_scores]
    return {
        "source_buy_events": len(buy_scores),
        "required_buy_copy_events": len(required_buy_scores),
        "clob_filled_buy_copy_events": len(clob_filled),
        "fallback_filled_buy_copy_events": len(fallback_filled),
        "rejected_buy_copy_events": len(rejected),
        "missed_buy_copy_events": len(missed),
        "filtered_buy_copy_events": len(filtered_buy_scores),
        "distinct_clob_filled_source_events": len(clob_source_events),
        "distinct_clob_filled_market_windows": len(clob_market_windows),
        "required_event_age_p95_s": _p95(required_event_ages),
        "required_api_latency_p95_s": _p95(required_api_latencies),
    }


def _live_tracker_truth(
    path: str | Path,
    *,
    require_copy_efficiency_truth: bool = True,
    candidate_policy_id: str | None = None,
    candidate_type: str | None = None,
    candidate_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict) or payload.get("kind") != "wallet_copy_live_tracking_state":
        return {"status": "MISSING", "path": str(path)}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    copy_efficiency = summary.get("copy_efficiency") if isinstance(summary.get("copy_efficiency"), dict) else {}
    efficiency_summary = (
        copy_efficiency.get("summary")
        if isinstance(copy_efficiency.get("summary"), dict)
        else {}
    )
    event_scores = [
        row
        for row in copy_efficiency.get("event_scores") or []
        if isinstance(row, dict)
    ]
    event_scores = _dedupe_event_scores(
        [*event_scores, *_copy_efficiency_event_scores_from_log(path)]
    )
    candidate_id = str((candidate_metadata or {}).get("candidate_id") or "")
    candidate_source_wallet = str((candidate_metadata or {}).get("source_wallet") or "").lower()
    candidate_policy = (
        (candidate_metadata or {}).get("candidate_policy")
        if isinstance((candidate_metadata or {}).get("candidate_policy"), dict)
        else {}
    )
    neutral_runtime_policy_ids = {"", "NO_PROFIT_POLICY", "live_tracking_exact_btc_5m_all_buys"}
    candidate_event_scores: list[dict[str, Any]] = []
    for row in event_scores:
        row_policy_id = str(row.get("policy_id") or row.get("profit_policy_id") or "")
        if candidate_policy_id and row_policy_id == str(candidate_policy_id):
            candidate_event_scores.append(row)
            continue
        if not candidate_policy_id or row_policy_id not in neutral_runtime_policy_ids or not candidate_policy:
            continue
        if candidate_source_wallet and str(row.get("source_wallet") or "").lower() != candidate_source_wallet:
            continue
        accepted, reason = _event_score_policy_acceptance(candidate_policy, row)
        if not accepted:
            continue
        # Active hot-lane exact-copy evidence is intentionally policy-neutral.
        # Re-stamp a scoped copy for candidate admission only after the same
        # source wallet event satisfies the candidate's policy filter.
        candidate_event_scores.append(
            {
                **row,
                "candidate_id": candidate_id or row.get("candidate_id"),
                "profit_policy_candidate_id": candidate_id or row.get("profit_policy_candidate_id"),
                "policy_id": str(candidate_policy_id),
                "profit_policy_id": str(candidate_policy_id),
                "profit_policy_context_id": str(candidate_policy_id),
                "profit_policy_reason": reason,
            }
        )
    candidate_policy_event_scores_before_candidate_filter = list(candidate_event_scores)
    if candidate_id:
        candidate_event_scores = [
            row
            for row in candidate_event_scores
            if str(row.get("candidate_id") or row.get("profit_policy_candidate_id") or "") == candidate_id
        ]
    candidate_policy_source_wallet_counts = Counter(
        str(row.get("source_wallet") or "").lower()
        for row in candidate_event_scores
        if row.get("source_wallet")
    )
    candidate_policy_required_buy_source_wallet_counts = Counter(
        str(row.get("source_wallet") or "").lower()
        for row in candidate_event_scores
        if row.get("wallet_action") == "BUY"
        and row.get("copy_status") != "FILTERED"
        and row.get("source_wallet")
    )
    if candidate_source_wallet:
        candidate_event_scores = [
            row
            for row in candidate_event_scores
            if str(row.get("source_wallet") or "").lower() == candidate_source_wallet
        ]
    candidate_buy_scores = [
        row
        for row in candidate_event_scores
        if str(row.get("wallet_action") or "").upper() == "BUY"
    ]
    candidate_required_buy_scores = [
        row
        for row in candidate_buy_scores
        if row.get("copy_status") != "FILTERED"
    ]
    candidate_filtered_buy_scores = [
        row
        for row in candidate_buy_scores
        if row.get("copy_status") == "FILTERED"
    ]
    candidate_copy_truth_summary = _candidate_copy_truth_summary(
        buy_scores=candidate_buy_scores,
        required_buy_scores=candidate_required_buy_scores,
        filtered_buy_scores=candidate_filtered_buy_scores,
    )
    candidate_filter_policy_counts = Counter(
        str(row.get("filter_policy") or "unknown")
        for row in candidate_filtered_buy_scores
    )
    candidate_copyability_reason_counts = Counter(
        str(row.get("copyability_reason") or row.get("missed_copy_reason") or "unknown")
        for row in candidate_filtered_buy_scores
        if str(row.get("filter_policy") or "") == "copyability"
    )
    candidate_profit_policy_reason_counts = Counter(
        str(row.get("profit_policy_reason") or row.get("missed_copy_reason") or "unknown")
        for row in candidate_filtered_buy_scores
        if str(row.get("filter_policy") or "") == "profit_policy"
    )
    tracker_scope = summary.get("tracker_scope") if isinstance(summary.get("tracker_scope"), dict) else {}
    tracked_wallet_rows = (
        tracker_scope.get("tracked_wallets") if isinstance(tracker_scope.get("tracked_wallets"), list) else []
    )
    tracked_wallet_addresses = {
        str(row.get("address") or "").lower()
        for row in tracked_wallet_rows
        if isinstance(row, dict) and row.get("address")
    }
    wallet_reports = summary.get("wallet_reports") if isinstance(summary.get("wallet_reports"), list) else []
    tracker_wallet_count = max(int(summary.get("wallets") or 0), len(wallet_reports))
    candidate_wallet_reports = [
        row
        for row in wallet_reports
        if isinstance(row, dict)
        and candidate_source_wallet
        and str(row.get("wallet") or row.get("address") or "").lower() == candidate_source_wallet
    ]
    candidate_wallet_report_summary = {
        "tracked": bool(candidate_source_wallet and candidate_source_wallet in tracked_wallet_addresses),
        "reports": len(candidate_wallet_reports),
        "raw_rows": sum(int(row.get("raw_rows") or 0) for row in candidate_wallet_reports),
        "events_seen": sum(int(row.get("events_seen") or 0) for row in candidate_wallet_reports),
        "new_events": sum(int(row.get("new_events") or 0) for row in candidate_wallet_reports),
    }
    blockers: list[str] = []
    if tracker_wallet_count <= 0:
        blockers.append("empty_poll")
    if payload.get("paper_only") is not True:
        blockers.append("tracker_not_paper_only")
    if payload.get("live_orders_allowed") is not False:
        blockers.append("tracker_live_orders_allowed")
    tracker_config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    if tracker_config.get("admission_mode") is not True:
        blockers.append("live_tracker_state_not_admission_mode")
    if summary.get("mirror_coverage_status") != "PASS":
        blockers.append("mirror_coverage_not_pass")
    if int(summary.get("mirror_required_events") or 0) <= 0:
        blockers.append("no_mirror_required_events")
    identity_contamination = summary.get("identity_contamination") if isinstance(summary.get("identity_contamination"), dict) else {}
    if identity_contamination.get("status") == "FAIL":
        if int(identity_contamination.get("wallet_identity_mismatch_rows") or 0) > 0:
            blockers.append("wallet_identity_mismatch_rows_present")
        if int(identity_contamination.get("wallet_identity_missing_rows") or 0) > 0:
            blockers.append("wallet_identity_missing_rows_present")
    if require_copy_efficiency_truth:
        if not copy_efficiency:
            blockers.append("copy_efficiency_missing")
        elif not candidate_policy_id and copy_efficiency.get("status") != "PASS":
            blockers.append("copy_efficiency_not_pass")
        scoped_efficiency_summary = (
            candidate_copy_truth_summary if candidate_policy_id else efficiency_summary
        )
        source_buy_events = int(scoped_efficiency_summary.get("source_buy_events") or 0)
        fresh_buy_rows = int(
            scoped_efficiency_summary.get("admission_relevant_buy_rows")
            or scoped_efficiency_summary.get("source_fresh_buy_events_le_10s")
            or scoped_efficiency_summary.get("required_buy_copy_events")
            or 0
        )
        required_buy_events = int(scoped_efficiency_summary.get("required_buy_copy_events") or 0)
        clob_filled_events = int(scoped_efficiency_summary.get("clob_filled_buy_copy_events") or 0)
        fallback_filled_events = int(scoped_efficiency_summary.get("fallback_filled_buy_copy_events") or 0)
        rejected_events = int(scoped_efficiency_summary.get("rejected_buy_copy_events") or 0)
        missed_events = int(scoped_efficiency_summary.get("missed_buy_copy_events") or 0)
        fresh_clob_counts = (
            efficiency_summary.get("clob_book_status_counts_for_fresh_buys")
            if isinstance(efficiency_summary.get("clob_book_status_counts_for_fresh_buys"), dict)
            else {}
        )
        if source_buy_events <= 0:
            blockers.append("no_wallet_buy_rows")
        elif fresh_buy_rows <= 0:
            blockers.append("stale_wallet_buy_rows_only")
        elif required_buy_events <= 0:
            blockers.append("no_required_fresh_buy_copy_evidence")
        if fresh_buy_rows > 0 and clob_filled_events <= 0:
            book_missing = int(fresh_clob_counts.get("BOOK_NOT_FOUND_OR_CLOSED") or 0)
            book_errors = int(fresh_clob_counts.get("ERROR") or 0)
            if book_missing or book_errors:
                blockers.append("fresh_buy_rows_no_clob_book")
        if fresh_buy_rows > 0 and rejected_events > 0:
            blockers.append("fresh_buy_rows_clob_rejected")
        if fresh_buy_rows > 0 and missed_events > 0:
            blockers.append("fresh_buy_rows_copy_missed")
        if required_buy_events > 0:
            if fallback_filled_events > 0:
                blockers.append("fallback_fills_not_live_admissible")
            if rejected_events > 0:
                blockers.append("rejected_copy_events_present")
            if missed_events > 0:
                blockers.append("missed_copy_events_present")
        if candidate_policy_id:
            if not candidate_event_scores:
                if candidate_source_wallet:
                    if not candidate_wallet_report_summary["tracked"]:
                        blockers.append("candidate_source_wallet_not_tracked")
                    elif int(candidate_wallet_report_summary["new_events"] or 0) <= 0:
                        blockers.append("candidate_source_wallet_inactive_or_deduped_in_forward_window")
                    else:
                        blockers.append("candidate_source_wallet_no_policy_compatible_fresh_events")
                blockers.append(
                    "candidate_source_wallet_copy_efficiency_missing"
                    if candidate_source_wallet
                    else "candidate_policy_copy_efficiency_missing"
                )
            elif not candidate_required_buy_scores:
                blockers.append("candidate_policy_required_buy_copy_missing")
                if candidate_filter_policy_counts.get("copyability"):
                    blockers.append("candidate_policy_copyability_filtered_buy_only")
                if candidate_filter_policy_counts.get("profit_policy"):
                    blockers.append("candidate_policy_profit_filtered_buy_only")
            elif any(row.get("copy_status") != "COPIED_FILLED" for row in candidate_required_buy_scores):
                blockers.append("candidate_policy_copy_efficiency_not_filled")
            elif any(str(row.get("fill_source") or "") != "clob_book_evidence" for row in candidate_required_buy_scores):
                blockers.append("candidate_policy_copy_efficiency_not_clob_backed")
            if candidate_type and candidate_type != "SINGLE_WALLET":
                blockers.append("multi_wallet_candidate_copy_efficiency_scope_not_verified")
    return {
        "status": "PASS" if not blockers else "BLOCKED",
        "path": str(path),
        "blockers": blockers,
        "event_scores": candidate_required_buy_scores,
        "summary": {
            "new_wallet_events": summary.get("new_wallet_events"),
            "mirror_required_events": summary.get("mirror_required_events"),
            "mirrored_events": summary.get("mirrored_events"),
            "mirror_coverage_pct": summary.get("mirror_coverage_pct"),
            "mirror_coverage_status": summary.get("mirror_coverage_status"),
            "copy_efficiency_status": copy_efficiency.get("status"),
            "copy_efficiency_blockers": copy_efficiency.get("blockers") or [],
            "copy_efficiency_summary": efficiency_summary,
            "candidate_copy_truth_summary": candidate_copy_truth_summary,
            "candidate_policy_id": candidate_policy_id,
            "candidate_id": candidate_id or None,
            "candidate_type": candidate_type,
            "candidate_source_wallet": candidate_source_wallet or None,
            "candidate_policy_copy_events": len(candidate_event_scores),
            "candidate_policy_copy_events_before_candidate_filter": len(
                candidate_policy_event_scores_before_candidate_filter
            ),
            "candidate_policy_required_buy_copy_events": len(candidate_required_buy_scores),
            "candidate_policy_buy_events": len(candidate_buy_scores),
            "candidate_policy_filtered_buy_events": len(candidate_filtered_buy_scores),
            "candidate_policy_filter_policy_counts": dict(sorted(candidate_filter_policy_counts.items())),
            "candidate_policy_copyability_reason_counts": dict(sorted(candidate_copyability_reason_counts.items())),
            "candidate_policy_profit_policy_reason_counts": dict(sorted(candidate_profit_policy_reason_counts.items())),
            "candidate_policy_source_wallet_counts": dict(
                sorted(candidate_policy_source_wallet_counts.items())
            ),
            "candidate_policy_required_buy_source_wallet_counts": dict(
                sorted(candidate_policy_required_buy_source_wallet_counts.items())
            ),
            "candidate_source_wallet_policy_event_count": int(
                candidate_policy_source_wallet_counts.get(candidate_source_wallet, 0)
            )
            if candidate_source_wallet
            else None,
            "candidate_source_wallet_required_buy_count": int(
                candidate_policy_required_buy_source_wallet_counts.get(candidate_source_wallet, 0)
            )
            if candidate_source_wallet
            else None,
            "candidate_source_wallet_report_summary": candidate_wallet_report_summary
            if candidate_source_wallet
            else None,
            "identity_contamination": identity_contamination,
            "paper_summary": summary.get("paper_summary"),
        },
    }


def _candidate_source_wallet(candidate: dict[str, Any] | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    return str(
        metadata.get("source_wallet")
        or candidate.get("source_wallet")
        or candidate.get("candidate_source_wallet")
        or candidate.get("wallet")
        or candidate.get("address")
        or ""
    ).lower()


def _candidate_policy_id(candidate: dict[str, Any] | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    return str(policy.get("policy_id") or "")


def _candidate_admission_key(
    *,
    candidate_type: str,
    policy: CandidatePolicy | dict[str, Any],
    metadata: dict[str, Any] | None = None,
    slippage_bps: float,
) -> str:
    policy_payload = policy.asdict() if isinstance(policy, CandidatePolicy) else dict(policy)
    metadata = metadata if isinstance(metadata, dict) else {}
    return stable_id(
        "wcpk",
        {
            "candidate_type": str(candidate_type or ""),
            "source_wallet": str(metadata.get("source_wallet") or "").lower(),
            "policy": policy_payload,
            "slippage_bps": float(slippage_bps),
        },
    )


def _bridge_inventory_wallets(target_inventory: dict[str, Any]) -> list[str]:
    wallets: list[str] = []

    def add(value: Any) -> None:
        wallet = str(value or "").lower()
        if wallet.startswith("0x") and wallet not in wallets:
            wallets.append(wallet)

    unique_wallets = (
        target_inventory.get("unique_wallets")
        if isinstance(target_inventory.get("unique_wallets"), list)
        else []
    )
    for wallet in unique_wallets:
        add(wallet)

    inventory_profile = (
        target_inventory.get("inventory_profile")
        if isinstance(target_inventory.get("inventory_profile"), dict)
        else {}
    )
    top_source_wallets = (
        inventory_profile.get("top_source_wallets")
        if isinstance(inventory_profile.get("top_source_wallets"), list)
        else []
    )
    for row in top_source_wallets:
        if isinstance(row, dict):
            add(row.get("wallet") or row.get("source_wallet"))
        else:
            add(row)
    return wallets


def _development_program_bridge_context(
    strategy_direction_state_path: str | Path,
    ranked_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = load_json(strategy_direction_state_path, default={})
    if not isinstance(payload, dict):
        return {
            "active": False,
            "state_path": str(strategy_direction_state_path),
            "reason": "strategy_direction_state_missing",
        }
    review = (
        payload.get("development_program_review")
        if isinstance(payload.get("development_program_review"), dict)
        else {}
    )
    next_action = str(review.get("next_major_change_action") or "")
    bridge_actions = {
        "rebuild_scaled_multi_wallet_inventory_from_full_per_wallet_copy_universe",
        "build_current_poll_inventory_bridge_from_profitable_single_wallet_and_weighted_inventory_candidate",
        "run_current_poll_inventory_bridge_burnin_and_attach_clob_truth",
    }
    if not bool(review.get("full_rethink_required")) or next_action not in bridge_actions:
        return {
            "active": False,
            "state_path": str(strategy_direction_state_path),
            "full_rethink_required": bool(review.get("full_rethink_required")),
            "next_major_change_action": next_action or None,
            "reason": "development_program_bridge_not_selected",
        }

    wallet_roles: dict[str, set[str]] = {}
    wallet_names: dict[str, str] = {}
    directions = payload.get("directions") if isinstance(payload.get("directions"), list) else []
    bridge_direction_ids = {
        "profitable_wallet_copy_efficiency",
        "single_wallet_best_copyable",
        "wr_repair_single_wallet",
    }
    for row in directions:
        if not isinstance(row, dict):
            continue
        direction_id = str(row.get("id") or "")
        if direction_id not in bridge_direction_ids:
            continue
        wallet = str(row.get("wallet") or "").lower()
        if not wallet:
            continue
        wallet_roles.setdefault(wallet, set()).add(direction_id)
        wallet_name = str(row.get("wallet_name") or "")
        if wallet_name:
            wallet_names.setdefault(wallet, wallet_name)

    inventory_candidates = [
        candidate
        for candidate in ranked_candidates
        if isinstance(candidate, dict) and candidate.get("candidate_type") == "MULTI_WALLET_INVENTORY"
    ]
    target_inventory = inventory_candidates[0] if inventory_candidates else {}
    target_summary = (
        target_inventory.get("summary")
        if isinstance(target_inventory.get("summary"), dict)
        else {}
    )
    target_fill = (
        target_inventory.get("fill_evidence_summary")
        if isinstance(target_inventory.get("fill_evidence_summary"), dict)
        else {}
    )
    target_metadata = (
        target_inventory.get("metadata")
        if isinstance(target_inventory.get("metadata"), dict)
        else {}
    )
    return {
        "active": bool(wallet_roles),
        "state_path": str(strategy_direction_state_path),
        "full_rethink_required": True,
        "next_major_change_action": next_action,
        "stop_doing": review.get("stop_doing") if isinstance(review.get("stop_doing"), list) else [],
        "strategic_traps": review.get("strategic_traps") if isinstance(review.get("strategic_traps"), list) else [],
        "wallet_roles": {wallet: sorted(roles) for wallet, roles in sorted(wallet_roles.items())},
        "wallet_names": wallet_names,
        "target_inventory": {
            "candidate_id": target_inventory.get("candidate_id") if isinstance(target_inventory, dict) else None,
            "candidate_type": target_inventory.get("candidate_type") if isinstance(target_inventory, dict) else None,
            "status": target_inventory.get("status") if isinstance(target_inventory, dict) else None,
            "blockers": target_inventory.get("blockers") if isinstance(target_inventory, dict) else [],
            "policy_id": (
                (target_inventory.get("policy") or {}).get("policy_id")
                if isinstance(target_inventory.get("policy"), dict)
                else None
            )
            if isinstance(target_inventory, dict)
            else None,
            "resolved_orders": target_summary.get("resolved_orders"),
            "wr_pct": target_summary.get("wr_pct"),
            "roi_pct": target_summary.get("roi_pct"),
            "pnl_usd": target_summary.get("pnl_usd"),
            "avg_orders_per_window": (
                (target_summary.get("window_metrics") or {}).get("avg_orders_per_window")
                if isinstance(target_summary.get("window_metrics"), dict)
                else None
            ),
            "clob_book_filled_buy_count": target_fill.get("clob_book_filled_buy_count"),
            "fallback_filled_buy_count": target_fill.get("fallback_filled_buy_count"),
            "base_intents": target_metadata.get("base_intents"),
            "plans": target_metadata.get("plans"),
            "unique_wallets": [
                row.get("wallet")
                for row in (
                    target_metadata.get("inventory_profile", {}).get("top_source_wallets")
                    if isinstance(target_metadata.get("inventory_profile"), dict)
                    else []
                )
                if isinstance(row, dict) and str(row.get("wallet") or "").startswith("0x")
            ],
            "inventory_profile": (
                target_metadata.get("inventory_profile")
                if isinstance(target_metadata.get("inventory_profile"), dict)
                else {}
            ),
        },
        "reason": "development_program_bridge_selected",
    }


def _apply_development_program_bridge_to_forward_queue(
    queue: list[dict[str, Any]],
    bridge_context: dict[str, Any],
) -> list[dict[str, Any]]:
    if not isinstance(bridge_context, dict) or not bridge_context.get("active"):
        return queue
    wallet_roles = (
        bridge_context.get("wallet_roles")
        if isinstance(bridge_context.get("wallet_roles"), dict)
        else {}
    )
    target_inventory = (
        bridge_context.get("target_inventory")
        if isinstance(bridge_context.get("target_inventory"), dict)
        else {}
    )
    target_candidate_id = str(target_inventory.get("candidate_id") or "")
    role_wallets = [
        str(wallet or "").lower()
        for wallet in wallet_roles
        if str(wallet or "").lower().startswith("0x")
    ]
    inventory_wallets = _bridge_inventory_wallets(target_inventory)
    target_source_wallets = list(dict.fromkeys([*inventory_wallets, *role_wallets]))
    patched_queue: list[dict[str, Any]] = []
    has_target_inventory_entry = False
    if target_candidate_id:
        for row in queue:
            if isinstance(row, dict) and str(row.get("candidate_id") or "") == target_candidate_id:
                has_target_inventory_entry = True
                break
        if not has_target_inventory_entry:
            patched_queue.append(
                {
                    "candidate_id": target_candidate_id,
                    "candidate_type": target_inventory.get("candidate_type") or "MULTI_WALLET_INVENTORY",
                    "source_wallet": target_source_wallets[0] if target_source_wallets else "",
                    "source_wallets": target_source_wallets,
                    "wallet_name": "development_program_inventory_target",
                    "policy_id": target_inventory.get("policy_id"),
                    "development_program_bridge_focus": True,
                    "development_program_inventory_target_entry": True,
                    "development_program_bridge_roles": ["multi_wallet_inventory_target"],
                    "development_program_next_major_change_action": bridge_context.get(
                        "next_major_change_action"
                    ),
                    "development_program_stop_doing": bridge_context.get("stop_doing") or [],
                    "development_program_target_inventory": target_inventory,
                    "blockers": [
                        "development_program_bridge_pending_current_poll_inventory",
                        "development_program_inventory_target_needs_current_poll_clob_truth",
                    ],
                    "resolved_orders": target_inventory.get("resolved_orders"),
                    "wr_pct": target_inventory.get("wr_pct"),
                    "roi_pct": target_inventory.get("roi_pct"),
                    "profit_score": target_inventory.get("pnl_usd") or target_inventory.get("roi_pct") or 0.0,
                }
            )
    for row in queue:
        if not isinstance(row, dict):
            continue
        patched = dict(row)
        wallet = str(patched.get("source_wallet") or "").lower()
        roles = [str(role) for role in wallet_roles.get(wallet, [])]
        if roles:
            patched["development_program_bridge_focus"] = True
            patched["development_program_bridge_roles"] = roles
            patched["development_program_next_major_change_action"] = bridge_context.get(
                "next_major_change_action"
            )
            patched["development_program_stop_doing"] = bridge_context.get("stop_doing") or []
            patched["development_program_target_inventory"] = target_inventory
            blockers = list(patched.get("blockers") or [])
            bridge_blocker = "development_program_bridge_pending_current_poll_inventory"
            if bridge_blocker not in blockers:
                blockers.append(bridge_blocker)
            patched["blockers"] = blockers
        patched_queue.append(patched)
    return patched_queue


def _float_or_inf(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


def _active_hotlane_wallet_evidence(path: str | Path) -> dict[str, dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    rows = payload.get("selected_wallets") if isinstance(payload.get("selected_wallets"), list) else []
    by_wallet: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("address") or row.get("wallet") or "").lower()
        if not wallet or wallet in by_wallet:
            continue
        by_wallet[wallet] = {
            "active_rank": index,
            "latest_live_event_lag_s": row.get("latest_live_event_lag_s"),
            "live_copyability_accepted": row.get("live_copyability_accepted"),
            "live_btc5m_buys": row.get("live_btc5m_buys"),
            "live_buy_usdc": row.get("live_buy_usdc"),
            "live_score": row.get("live_score"),
            "selection_reasons": row.get("selection_reasons") or [],
        }
    return by_wallet


def _active_hotlane_live_tracker_wallet_evidence(path: str | Path) -> dict[str, dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    copy_efficiency = (
        summary.get("copy_efficiency")
        if isinstance(summary.get("copy_efficiency"), dict)
        else {}
    )
    by_wallet: dict[str, dict[str, Any]] = {}
    for row in copy_efficiency.get("event_scores") or []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        if not wallet:
            continue
        entry = by_wallet.setdefault(
            wallet,
            {
                "active_rank": 9999 + len(by_wallet),
                "latest_live_event_lag_s": None,
                "live_copyability_accepted": 0,
                "live_btc5m_buys": 0,
                "live_buy_usdc": 0.0,
                "live_clob_ok": 0,
                "selection_reasons": ["active_hotlane_live_tracker_copy_evidence"],
            },
        )
        if str(row.get("wallet_action") or "").upper() != "BUY":
            continue
        entry["live_btc5m_buys"] = int(entry.get("live_btc5m_buys") or 0) + 1
        try:
            entry["live_buy_usdc"] = round(
                float(entry.get("live_buy_usdc") or 0.0) + float(row.get("source_usdc_size") or 0.0),
                6,
            )
        except (TypeError, ValueError):
            pass
        age_s = _float_or_inf(row.get("event_age_s"))
        if age_s != float("inf"):
            previous = _float_or_inf(entry.get("latest_live_event_lag_s"))
            entry["latest_live_event_lag_s"] = age_s if age_s < previous else previous
        if row.get("copyability_accepted") is True:
            entry["live_copyability_accepted"] = int(entry.get("live_copyability_accepted") or 0) + 1
        if row.get("copy_status") == "COPIED_FILLED" and row.get("fill_source") == "clob_book_evidence":
            entry["live_clob_ok"] = int(entry.get("live_clob_ok") or 0) + 1
    for entry in by_wallet.values():
        if entry.get("latest_live_event_lag_s") == float("inf"):
            entry["latest_live_event_lag_s"] = None
    return by_wallet


def _merge_active_hotlane_evidence(
    scope_evidence: dict[str, dict[str, Any]],
    live_evidence: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged = {wallet: dict(row) for wallet, row in scope_evidence.items()}
    for wallet, live in live_evidence.items():
        row = dict(merged.get(wallet, {}))
        row.setdefault("active_rank", live.get("active_rank"))
        for key in ("live_copyability_accepted", "live_btc5m_buys", "live_clob_ok"):
            row[key] = max(int(row.get(key) or 0), int(live.get(key) or 0))
        for key in ("live_buy_usdc", "live_score"):
            if live.get(key) is not None:
                row[key] = max(float(row.get(key) or 0.0), float(live.get(key) or 0.0))
        live_lag = _float_or_inf(live.get("latest_live_event_lag_s"))
        row_lag = _float_or_inf(row.get("latest_live_event_lag_s"))
        if live_lag < row_lag:
            row["latest_live_event_lag_s"] = live_lag
        reasons = list(row.get("selection_reasons") or [])
        for reason in live.get("selection_reasons") or []:
            if reason not in reasons:
                reasons.append(reason)
        row["selection_reasons"] = reasons
        merged[wallet] = row
    return merged


def _candidate_forward_viability(
    candidate: dict[str, Any],
    *,
    active_evidence: dict[str, dict[str, Any]],
    fresh_lag_cap_s: float,
) -> dict[str, Any]:
    wallet = _candidate_source_wallet(candidate)
    blockers = {str(blocker) for blocker in candidate.get("blockers") or []}
    active = active_evidence.get(wallet, {})
    latest_lag_s = _float_or_inf(active.get("latest_live_event_lag_s"))
    labels: list[str] = []
    if not wallet:
        labels.append("missing_source_wallet")
    if active:
        labels.append("active_hotlane_overlap")
    else:
        labels.append("no_active_hotlane_overlap")
    if not active or latest_lag_s > float(fresh_lag_cap_s):
        labels.append("stale_forward_source")
    if "candidate_research_only_resolution_evidence" in blockers:
        labels.append("research_only_resolution")
    if any(str(blocker).startswith("candidate_window_order_concentration") for blocker in blockers):
        labels.append("window_concentrated")
    if "candidate_missing_clob_fill_evidence" in blockers:
        labels.append("missing_clob_fill_evidence")
    forward_trackable = bool(wallet and active and latest_lag_s <= float(fresh_lag_cap_s))
    return {
        "status": "TRACKABLE" if forward_trackable else "STALE_OR_UNTRACKABLE",
        "forward_trackable": forward_trackable,
        "labels": labels,
        "latest_live_event_lag_s": None if latest_lag_s == float("inf") else latest_lag_s,
        "active_hotlane": active,
    }


def _freshness_bucket(latest_lag_s: float | None) -> tuple[int, float]:
    lag = _float_or_inf(latest_lag_s)
    if lag <= 30.0:
        bucket = 0
    elif lag <= 60.0:
        bucket = 1
    elif lag <= 180.0:
        bucket = 2
    elif lag <= 1800.0:
        bucket = 3
    else:
        bucket = 4
    return bucket, lag


def _candidate_forward_event_scores(path: str | Path) -> list[dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return []
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    copy_efficiency = (
        summary.get("copy_efficiency")
        if isinstance(summary.get("copy_efficiency"), dict)
        else {}
    )
    return [row for row in copy_efficiency.get("event_scores") or [] if isinstance(row, dict)]


def _candidate_forward_tracker_context(path: str | Path) -> dict[str, Any]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    profit_policy = (
        summary.get("profit_policy")
        if isinstance(summary.get("profit_policy"), dict)
        else {}
    )
    copy_efficiency = (
        summary.get("copy_efficiency")
        if isinstance(summary.get("copy_efficiency"), dict)
        else {}
    )
    copy_efficiency_summary = (
        copy_efficiency.get("summary")
        if isinstance(copy_efficiency.get("summary"), dict)
        else {}
    )
    return {
        "candidate_id": profit_policy.get("candidate_id"),
        "candidate_source_wallet": str(profit_policy.get("candidate_source_wallet") or "").lower(),
        "policy_id": (
            profit_policy.get("policy").get("policy_id")
            if isinstance(profit_policy.get("policy"), dict)
            else None
        ),
        "source_buy_events": int(copy_efficiency_summary.get("source_buy_events") or 0),
        "fresh_buy_events_le_30s": int(copy_efficiency_summary.get("source_fresh_buy_events_le_30s") or 0),
        "latest_buy_event_lag_s": copy_efficiency_summary.get("latest_buy_event_lag_s"),
    }


def _candidate_forward_tracker_paths(cfg: ProfitEngineConfig) -> list[str]:
    paths = [
        str(cfg.candidate_forward_live_tracker_state_path),
        *[str(path) for path in cfg.candidate_forward_probe_live_tracker_state_paths],
    ]
    return list(dict.fromkeys(path for path in paths if path))


def _candidate_forward_event_scores_from_paths(paths: Iterable[str | Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        rows.extend(_candidate_forward_event_scores(path))
    return rows


def _candidate_forward_tracker_contexts(paths: Iterable[str | Path]) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for path in paths:
        context = _candidate_forward_tracker_context(path)
        if context:
            context["state_path"] = str(path)
            contexts.append(context)
    return contexts


def _matching_forward_tracker_context(
    candidate: dict[str, Any],
    contexts: list[dict[str, Any]],
) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id") or "")
    wallet = _candidate_source_wallet(candidate)
    policy_id = _candidate_policy_id(candidate)
    for context in contexts:
        if candidate_id and str(context.get("candidate_id") or "") == candidate_id:
            return context
    if candidate_id:
        return {}
    for context in contexts:
        if (
            wallet
            and str(context.get("candidate_source_wallet") or "").lower() == wallet
            and policy_id
            and str(context.get("policy_id") or "") == policy_id
        ):
            return context
    for context in contexts:
        if wallet and str(context.get("candidate_source_wallet") or "").lower() == wallet:
            return context
    return contexts[0] if contexts else {}


def _tracker_truth_score(truth: dict[str, Any]) -> tuple[Any, ...]:
    summary = truth.get("summary") if isinstance(truth.get("summary"), dict) else {}
    report = (
        summary.get("candidate_source_wallet_report_summary")
        if isinstance(summary.get("candidate_source_wallet_report_summary"), dict)
        else {}
    )
    copy_efficiency = (
        summary.get("copy_efficiency_summary")
        if isinstance(summary.get("copy_efficiency_summary"), dict)
        else {}
    )
    candidate_copy = (
        summary.get("candidate_copy_truth_summary")
        if isinstance(summary.get("candidate_copy_truth_summary"), dict)
        else {}
    )
    status_rank = {"PASS": 0, "WATCH": 1, "BLOCKED": 2, "MISSING": 3}.get(str(truth.get("status") or ""), 4)
    blockers = {str(blocker) for blocker in truth.get("blockers") or []}
    return (
        status_rank,
        0 if report.get("tracked") else 1,
        -int(summary.get("candidate_policy_required_buy_copy_events") or 0),
        -int(summary.get("candidate_policy_copy_events") or 0),
        _float_or_inf(candidate_copy.get("required_event_age_p95_s")),
        _float_or_inf(candidate_copy.get("required_api_latency_p95_s")),
        -int(report.get("new_events") or 0),
        -int(copy_efficiency.get("profit_policy_accepted_buy_events") or 0),
        int("candidate_source_wallet_not_tracked" in blockers),
    )


def _best_candidate_tracker_truth(
    paths: Iterable[str | Path],
    *,
    require_copy_efficiency_truth: bool,
    candidate_policy_id: str | None,
    candidate_type: str | None,
    candidate_metadata: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    truths: list[dict[str, Any]] = []
    for path in paths:
        truth = _live_tracker_truth(
            path,
            require_copy_efficiency_truth=require_copy_efficiency_truth,
            candidate_policy_id=candidate_policy_id,
            candidate_type=candidate_type,
            candidate_metadata=candidate_metadata,
        )
        truth = dict(truth)
        truth["state_path"] = str(path)
        truths.append(truth)
    if not truths:
        return {"status": "MISSING", "blockers": ["candidate_forward_tracker_state_missing"]}, []
    return sorted(truths, key=_tracker_truth_score)[0], truths


def _candidate_runtime_proof_row_key(row: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(row.get("candidate_key") or ""),
        str(row.get("candidate_id") or ""),
        str(row.get("policy_id") or row.get("profit_policy_id") or ""),
        str(row.get("source_wallet") or "").lower(),
        str(row.get("source_event_id") or ""),
        str(row.get("token_id") or ""),
        str(row.get("market_slug") or ""),
        str(row.get("book_hash") or row.get("clob_book_hash") or ""),
    )


def _clean_runtime_proof_score(row: dict[str, Any]) -> bool:
    return (
        isinstance(row, dict)
        and str(row.get("wallet_action") or "").upper() == "BUY"
        and row.get("copy_status") == "COPIED_FILLED"
        and row.get("fill_source") == "clob_book_evidence"
    )


def _is_runtime_proof_wallet_address(value: Any) -> bool:
    wallet = str(value or "").lower()
    return wallet.startswith("0x") and len(wallet) == 42 and all(ch in "0123456789abcdef" for ch in wallet[2:])


def _loadable_runtime_proof_index_row(row: dict[str, Any]) -> bool:
    return _clean_runtime_proof_score(row) and _is_runtime_proof_wallet_address(row.get("source_wallet"))


def _runtime_proof_rows_from_truth(
    candidate: dict[str, Any] | None,
    truth: dict[str, Any],
    *,
    source: str,
) -> list[dict[str, Any]]:
    if not isinstance(candidate, dict) or not isinstance(truth, dict):
        return []
    summary = truth.get("summary") if isinstance(truth.get("summary"), dict) else {}
    candidate_copy_summary = (
        summary.get("candidate_copy_truth_summary")
        if isinstance(summary.get("candidate_copy_truth_summary"), dict)
        else {}
    )
    dirty_candidate_counts = (
        int(candidate_copy_summary.get("fallback_filled_buy_copy_events") or 0),
        int(candidate_copy_summary.get("rejected_buy_copy_events") or 0),
        int(candidate_copy_summary.get("missed_buy_copy_events") or 0),
    )
    if any(count > 0 for count in dirty_candidate_counts):
        return []
    candidate_id = str(candidate.get("candidate_id") or summary.get("candidate_id") or "")
    candidate_key = str(candidate.get("candidate_key") or summary.get("candidate_key") or "")
    policy_id = str(_candidate_policy_id(candidate) or summary.get("candidate_policy_id") or "")
    source_wallet = str(_candidate_source_wallet(candidate) or summary.get("candidate_source_wallet") or "").lower()
    if not candidate_id or not policy_id or not source_wallet:
        return []
    state_path = str(truth.get("state_path") or truth.get("path") or "")
    rows: list[dict[str, Any]] = []
    for row in truth.get("event_scores") or []:
        if not _clean_runtime_proof_score(row):
            continue
        row_candidate_id = str(row.get("candidate_id") or row.get("profit_policy_candidate_id") or "")
        if row_candidate_id and row_candidate_id != candidate_id:
            continue
        row_policy_id = str(row.get("policy_id") or row.get("profit_policy_id") or "")
        if row_policy_id and row_policy_id != policy_id:
            continue
        row_source_wallet = str(row.get("source_wallet") or "").lower()
        if row_source_wallet and row_source_wallet != source_wallet:
            continue
        rows.append(
            {
                "candidate_id": candidate_id,
                "candidate_key": candidate_key,
                "policy_id": policy_id,
                "source_wallet": source_wallet,
                "source": source,
                "state_path": state_path,
                "source_event_id": row.get("source_event_id"),
                "source_fingerprint": row.get("source_fingerprint"),
                "market_slug": row.get("market_slug"),
                "token_id": row.get("token_id"),
                "condition_id": row.get("condition_id"),
                "outcome": row.get("outcome"),
                "source_price": row.get("source_price"),
                "source_usdc_size": row.get("source_usdc_size"),
                "event_age_s": row.get("event_age_s"),
                "api_latency_s": row.get("api_latency_s"),
                "book_hash": row.get("book_hash") or row.get("clob_book_hash"),
                "book_timestamp": row.get("book_timestamp"),
                "copy_status": row.get("copy_status"),
                "fill_source": row.get("fill_source"),
                "wallet_action": row.get("wallet_action"),
                "route_status": row.get("route_status") or row.get("wallet_route_status"),
                "route_class": row.get("route_class") or row.get("wallet_route_class"),
                "route_report_id": row.get("route_report_id") or row.get("wallet_route_report_id"),
                "routed_host": row.get("routed_host") or row.get("wallet_routed_host"),
                "source_base_override_configured": (
                    row.get("source_base_override_configured")
                    if row.get("source_base_override_configured") is not None
                    else row.get("wallet_source_base_override_configured")
                ),
                "source_base_override_env_var": row.get("source_base_override_env_var"),
                "request_fingerprint": row.get("request_fingerprint") or row.get("wallet_request_fingerprint"),
                "request_role": row.get("request_role"),
                "wallet_route_status": row.get("wallet_route_status") or row.get("route_status"),
                "wallet_route_class": row.get("wallet_route_class") or row.get("route_class"),
                "wallet_route_report_id": row.get("wallet_route_report_id") or row.get("route_report_id"),
                "clob_route_status": row.get("clob_route_status"),
                "clob_route_class": row.get("clob_route_class"),
                "clob_route_report_id": row.get("clob_route_report_id"),
                "clob_routed_host": row.get("clob_routed_host"),
                "clob_source_base_override_configured": row.get("clob_source_base_override_configured"),
                "clob_request_fingerprint": row.get("clob_request_fingerprint"),
                "generated_at": row.get("generated_at"),
            }
        )
    return rows


def _runtime_proof_rows_from_tracker_state(path: str | Path, *, source: str) -> list[dict[str, Any]]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return []
    if payload.get("paper_only") is not True or payload.get("live_orders_allowed") is not False:
        return []
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    copy_efficiency = summary.get("copy_efficiency") if isinstance(summary.get("copy_efficiency"), dict) else {}
    event_scores = [
        row for row in copy_efficiency.get("event_scores") or [] if isinstance(row, dict)
    ]
    event_scores = _dedupe_event_scores(
        [*event_scores, *_copy_efficiency_event_scores_from_log(path)]
    )
    clean_by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    dirty_keys: set[tuple[str, str, str]] = set()
    for row in event_scores:
        candidate_id = str(row.get("candidate_id") or row.get("profit_policy_candidate_id") or "")
        candidate_key = str(row.get("candidate_key") or row.get("profit_policy_candidate_key") or "")
        policy_id = str(row.get("policy_id") or row.get("profit_policy_id") or row.get("profit_policy_context_id") or "")
        source_wallet = str(row.get("source_wallet") or "").lower()
        if not candidate_id or not policy_id or not _is_runtime_proof_wallet_address(source_wallet):
            continue
        if str(row.get("wallet_action") or "").upper() != "BUY":
            continue
        key = (candidate_key or candidate_id, policy_id, source_wallet)
        copy_status = str(row.get("copy_status") or "")
        fill_source = str(row.get("fill_source") or "")
        paper_final_status = str(row.get("paper_final_status") or "")
        if _clean_runtime_proof_score(row):
            clean_by_key[key].append(row)
            continue
        if copy_status != "FILTERED" and (
            copy_status in {"COPY_REJECTED", "REJECTED", "MISSED"}
            or paper_final_status == "REJECTED"
            or (copy_status == "COPIED_FILLED" and fill_source != "clob_book_evidence")
        ):
            dirty_keys.add(key)

    rows: list[dict[str, Any]] = []
    state_path = str(path)
    for (proof_identity, policy_id, source_wallet), clean_rows in clean_by_key.items():
        if (proof_identity, policy_id, source_wallet) in dirty_keys:
            continue
        for row in clean_rows:
            candidate_id = str(row.get("candidate_id") or row.get("profit_policy_candidate_id") or "")
            candidate_key = str(row.get("candidate_key") or row.get("profit_policy_candidate_key") or "")
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "candidate_key": candidate_key,
                    "policy_id": policy_id,
                    "source_wallet": source_wallet,
                    "source": source,
                    "state_path": state_path,
                    "source_event_id": row.get("source_event_id"),
                    "source_fingerprint": row.get("source_fingerprint"),
                    "market_slug": row.get("market_slug"),
                    "token_id": row.get("token_id"),
                    "condition_id": row.get("condition_id"),
                    "outcome": row.get("outcome"),
                    "source_price": row.get("source_price"),
                    "source_usdc_size": row.get("source_usdc_size"),
                    "event_age_s": row.get("event_age_s"),
                    "api_latency_s": row.get("api_latency_s"),
                    "book_hash": row.get("book_hash") or row.get("clob_book_hash"),
                    "book_timestamp": row.get("book_timestamp"),
                    "copy_status": row.get("copy_status"),
                    "fill_source": row.get("fill_source"),
                    "wallet_action": row.get("wallet_action"),
                    "route_status": row.get("route_status") or row.get("wallet_route_status"),
                    "route_class": row.get("route_class") or row.get("wallet_route_class"),
                    "route_report_id": row.get("route_report_id") or row.get("wallet_route_report_id"),
                    "routed_host": row.get("routed_host") or row.get("wallet_routed_host"),
                    "source_base_override_configured": (
                        row.get("source_base_override_configured")
                        if row.get("source_base_override_configured") is not None
                        else row.get("wallet_source_base_override_configured")
                    ),
                    "source_base_override_env_var": row.get("source_base_override_env_var"),
                    "request_fingerprint": row.get("request_fingerprint")
                    or row.get("wallet_request_fingerprint"),
                    "request_role": row.get("request_role"),
                    "wallet_route_status": row.get("wallet_route_status") or row.get("route_status"),
                    "wallet_route_class": row.get("wallet_route_class") or row.get("route_class"),
                    "wallet_route_report_id": row.get("wallet_route_report_id") or row.get("route_report_id"),
                    "clob_route_status": row.get("clob_route_status"),
                    "clob_route_class": row.get("clob_route_class"),
                    "clob_route_report_id": row.get("clob_route_report_id"),
                    "clob_routed_host": row.get("clob_routed_host"),
                    "clob_source_base_override_configured": row.get("clob_source_base_override_configured"),
                    "clob_request_fingerprint": row.get("clob_request_fingerprint"),
                    "generated_at": row.get("generated_at"),
                }
            )
    return rows


def _load_runtime_proof_index_rows(path: str | Path) -> list[dict[str, Any]]:
    if not str(path or ""):
        return []
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return []
    rows = payload.get("proof_rows")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict) and _loadable_runtime_proof_index_row(row)]


def _merge_runtime_proof_rows(
    *row_sets: Iterable[dict[str, Any]],
    max_rows: int = 50_000,
) -> list[dict[str, Any]]:
    deduped: OrderedDict[tuple[str, ...], dict[str, Any]] = OrderedDict()
    for rows in row_sets:
        for row in rows:
            if not _clean_runtime_proof_score(row):
                continue
            key = _candidate_runtime_proof_row_key(row)
            if not any(key):
                continue
            deduped[key] = dict(row)
    merged = list(deduped.values())
    return merged[-max(1, int(max_rows)) :]


def _runtime_proof_index_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_candidate: dict[str, dict[str, Any]] = {}
    for row in rows:
        candidate_id = str(row.get("candidate_id") or "")
        candidate_key = str(row.get("candidate_key") or "")
        identity = candidate_key or candidate_id
        if not identity:
            continue
        entry = by_candidate.setdefault(
            identity,
            {
                "candidate_id": candidate_id,
                "candidate_key": candidate_key,
                "policy_id": row.get("policy_id"),
                "source_wallet": row.get("source_wallet"),
                "proof_rows": 0,
                "market_windows": set(),
                "source_events": set(),
            },
        )
        entry["proof_rows"] = int(entry.get("proof_rows") or 0) + 1
        if row.get("market_slug"):
            entry["market_windows"].add(str(row.get("market_slug")))
        if row.get("source_event_id"):
            entry["source_events"].add(str(row.get("source_event_id")))
    candidate_summaries = []
    for entry in by_candidate.values():
        candidate_summaries.append(
            {
                "candidate_id": entry.get("candidate_id"),
                "candidate_key": entry.get("candidate_key"),
                "policy_id": entry.get("policy_id"),
                "source_wallet": entry.get("source_wallet"),
                "proof_rows": entry.get("proof_rows"),
                "market_windows": len(entry.get("market_windows") or []),
                "source_events": len(entry.get("source_events") or []),
            }
        )
    candidate_summaries.sort(
        key=lambda row: (
            -int(row.get("market_windows") or 0),
            -int(row.get("proof_rows") or 0),
            str(row.get("candidate_id") or ""),
        )
    )
    return {
        "proof_rows": len(rows),
        "candidates": len(candidate_summaries),
        "top_candidates": candidate_summaries[:20],
    }


def _runtime_proof_candidate_summaries(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        if not _loadable_runtime_proof_index_row(row):
            continue
        candidate_id = str(row.get("candidate_id") or "")
        candidate_key = str(row.get("candidate_key") or "")
        policy_id = str(row.get("policy_id") or row.get("profit_policy_id") or "")
        source_wallet = str(row.get("source_wallet") or "").lower()
        if not candidate_id or not policy_id or not source_wallet:
            continue
        key = (candidate_key or candidate_id, policy_id, source_wallet)
        entry = grouped.setdefault(
            key,
            {
                "candidate_id": candidate_id,
                "candidate_key": candidate_key,
                "policy_id": policy_id,
                "source_wallet": source_wallet,
                "proof_rows": 0,
                "market_windows": set(),
                "source_events": set(),
                "event_ages": [],
                "sources": set(),
            },
        )
        entry["proof_rows"] = int(entry.get("proof_rows") or 0) + 1
        if row.get("market_slug"):
            entry["market_windows"].add(str(row.get("market_slug")))
        if row.get("source_event_id"):
            entry["source_events"].add(str(row.get("source_event_id")))
        try:
            entry["event_ages"].append(float(row.get("event_age_s")))
        except (TypeError, ValueError):
            pass
        if row.get("source"):
            entry["sources"].add(str(row.get("source")))

    summaries: list[dict[str, Any]] = []
    for entry in grouped.values():
        event_ages = sorted(float(value) for value in entry.get("event_ages") or [])
        p95 = event_ages[int(round((len(event_ages) - 1) * 0.95))] if event_ages else None
        summaries.append(
            {
                "candidate_id": entry.get("candidate_id"),
                "candidate_key": entry.get("candidate_key"),
                "policy_id": entry.get("policy_id"),
                "source_wallet": entry.get("source_wallet"),
                "proof_rows": int(entry.get("proof_rows") or 0),
                "market_windows": len(entry.get("market_windows") or []),
                "source_events": len(entry.get("source_events") or []),
                "event_age_p95_s": round(float(p95), 6) if p95 is not None else None,
                "sources": sorted(entry.get("sources") or []),
            }
        )
    summaries.sort(
        key=lambda row: (
            -int(row.get("source_events") or 0),
            -int(row.get("market_windows") or 0),
            _float_or_inf(row.get("event_age_p95_s")),
            str(row.get("candidate_id") or ""),
        )
    )
    return summaries


def _write_runtime_proof_index(path: str | Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not str(path or ""):
        return {
            "schema_version": 1,
            "kind": "wallet_copy_candidate_runtime_proof_index",
            "generated_at": utc_now_iso(),
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": _runtime_proof_index_summary(rows),
            "proof_rows": rows,
            "persisted": False,
        }
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_candidate_runtime_proof_index",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "proof_contract": {
            "accepted_rows": "required BUY CopyIntent scores with copy_status=COPIED_FILLED and fill_source=clob_book_evidence",
            "live_admission_note": "index aggregation does not relax candidate gates; fallback/reject/miss remain blockers",
        },
        "summary": _runtime_proof_index_summary(rows),
        "proof_rows": rows,
    }
    atomic_write_json(path, payload)
    return payload


def _runtime_proof_rows_for_candidate(
    rows: Iterable[dict[str, Any]],
    *,
    candidate_id: str,
    candidate_key: str = "",
    policy_id: str,
    source_wallet: str,
) -> list[dict[str, Any]]:
    wallet = source_wallet.lower()
    key_matched: list[dict[str, Any]] = []
    exact_legacy_matched: list[dict[str, Any]] = []
    legacy_wallet_policy_matched: list[dict[str, Any]] = []
    for row in rows:
        if not _clean_runtime_proof_score(row):
            continue
        if policy_id and str(row.get("policy_id") or row.get("profit_policy_id") or "") != policy_id:
            continue
        if wallet and str(row.get("source_wallet") or "").lower() != wallet:
            continue
        row_candidate_key = str(row.get("candidate_key") or "")
        row_candidate_id = str(row.get("candidate_id") or row.get("profit_policy_candidate_id") or "")
        if candidate_key and row_candidate_key == candidate_key:
            matched = dict(row)
            if candidate_id and row_candidate_id != candidate_id:
                matched["runtime_candidate_key_matched"] = True
                matched["runtime_original_candidate_id"] = row_candidate_id
                matched["candidate_id"] = candidate_id
                matched["profit_policy_candidate_id"] = candidate_id
            key_matched.append(matched)
            continue
        if not row_candidate_key and candidate_id and row_candidate_id == candidate_id:
            exact_legacy_matched.append(row)
            continue
        if not candidate_id and not row_candidate_key:
            exact_legacy_matched.append(row)
            continue
        if not row_candidate_key and policy_id and wallet:
            matched = dict(row)
            matched["runtime_candidate_legacy_policy_matched"] = True
            matched["runtime_original_candidate_id"] = row_candidate_id
            if candidate_id:
                matched["candidate_id"] = candidate_id
                matched["profit_policy_candidate_id"] = candidate_id
            legacy_wallet_policy_matched.append(matched)
    if key_matched:
        return key_matched
    if exact_legacy_matched:
        return exact_legacy_matched
    return legacy_wallet_policy_matched


def _candidate_with_runtime_copy_evidence(
    candidate: dict[str, Any] | None,
    truth: dict[str, Any],
    *,
    source: str,
    min_required_buy_copy_events: int = 3,
    min_required_market_windows: int = 2,
    proof_index_rows: Iterable[dict[str, Any]] = (),
) -> dict[str, Any] | None:
    if not isinstance(candidate, dict):
        return None
    patched = dict(candidate)
    blockers = [str(blocker) for blocker in patched.get("blockers") or []]
    summary = truth.get("summary") if isinstance(truth.get("summary"), dict) else {}
    copy_summary = (
        summary.get("copy_efficiency_summary")
        if isinstance(summary.get("copy_efficiency_summary"), dict)
        else {}
    )
    candidate_copy_summary = (
        summary.get("candidate_copy_truth_summary")
        if isinstance(summary.get("candidate_copy_truth_summary"), dict)
        else {}
    )
    evidence = {
        "source": source,
        "truth_status": truth.get("status"),
        "candidate_id": summary.get("candidate_id"),
        "candidate_policy_id": summary.get("candidate_policy_id"),
        "candidate_source_wallet": summary.get("candidate_source_wallet"),
        "candidate_policy_required_buy_copy_events": summary.get("candidate_policy_required_buy_copy_events"),
        "required_buy_copy_events": candidate_copy_summary.get("required_buy_copy_events"),
        "clob_filled_buy_copy_events": candidate_copy_summary.get("clob_filled_buy_copy_events"),
        "fallback_filled_buy_copy_events": candidate_copy_summary.get("fallback_filled_buy_copy_events"),
        "rejected_buy_copy_events": candidate_copy_summary.get("rejected_buy_copy_events"),
        "missed_buy_copy_events": candidate_copy_summary.get("missed_buy_copy_events"),
        "required_event_age_p95_s": candidate_copy_summary.get("required_event_age_p95_s"),
        "global_required_buy_copy_events": copy_summary.get("required_buy_copy_events"),
        "global_clob_filled_buy_copy_events": copy_summary.get("clob_filled_buy_copy_events"),
        "min_required_buy_copy_events": int(min_required_buy_copy_events),
        "min_required_market_windows": int(min_required_market_windows),
    }
    event_scores = (
        truth.get("event_scores")
        if isinstance(truth.get("event_scores"), list)
        else []
    )
    candidate_id = str(candidate.get("candidate_id") or summary.get("candidate_id") or "")
    candidate_key = str(candidate.get("candidate_key") or summary.get("candidate_key") or "")
    candidate_policy_id = str(
        _candidate_policy_id(candidate) or summary.get("candidate_policy_id") or ""
    )
    candidate_wallet = str(
        _candidate_source_wallet(candidate) or summary.get("candidate_source_wallet") or ""
    ).lower()
    evidence["candidate_id"] = candidate_id
    evidence["candidate_key"] = candidate_key
    evidence["candidate_policy_id"] = candidate_policy_id
    evidence["candidate_source_wallet"] = candidate_wallet
    evidence["tracker_candidate_id"] = summary.get("candidate_id")
    evidence["tracker_candidate_policy_id"] = summary.get("candidate_policy_id")
    evidence["tracker_candidate_source_wallet"] = summary.get("candidate_source_wallet")
    indexed_scores = _runtime_proof_rows_for_candidate(
        proof_index_rows,
        candidate_id=candidate_id,
        candidate_key=candidate_key,
        policy_id=candidate_policy_id,
        source_wallet=candidate_wallet,
    )
    indexed_score_sources = sorted(
        {
            str(row.get("source") or "")
            for row in indexed_scores
            if isinstance(row, dict) and row.get("source")
        }
    )
    evidence_scores = _merge_runtime_proof_rows(event_scores, indexed_scores)
    required_market_windows: set[str] = set()
    required_source_events: set[str] = set()
    current_truth_market_windows: set[str] = set()
    current_truth_source_events: set[str] = set()
    current_truth_rows = 0
    for row in evidence_scores:
        if not isinstance(row, dict):
            continue
        if candidate_id and str(row.get("candidate_id") or row.get("profit_policy_candidate_id") or "") not in {
            "",
            candidate_id,
        }:
            continue
        if candidate_policy_id and str(row.get("policy_id") or row.get("profit_policy_id") or "") not in {
            "",
            candidate_policy_id,
        }:
            continue
        if candidate_wallet and str(row.get("source_wallet") or "").lower() not in {"", candidate_wallet}:
            continue
        if row.get("copy_status") != "COPIED_FILLED":
            continue
        if row.get("fill_source") != "clob_book_evidence":
            continue
        source_event_id = str(row.get("source_event_id") or "")
        if source_event_id:
            required_source_events.add(source_event_id)
        market_slug = str(row.get("market_slug") or "")
        if market_slug:
            required_market_windows.add(market_slug)
    for row in event_scores:
        if not isinstance(row, dict):
            continue
        if candidate_id and str(row.get("candidate_id") or row.get("profit_policy_candidate_id") or "") not in {
            "",
            candidate_id,
        }:
            continue
        if candidate_policy_id and str(row.get("policy_id") or row.get("profit_policy_id") or "") not in {
            "",
            candidate_policy_id,
        }:
            continue
        if candidate_wallet and str(row.get("source_wallet") or "").lower() not in {"", candidate_wallet}:
            continue
        if row.get("copy_status") != "COPIED_FILLED":
            continue
        if row.get("fill_source") != "clob_book_evidence":
            continue
        current_truth_rows += 1
        source_event_id = str(row.get("source_event_id") or "")
        if source_event_id:
            current_truth_source_events.add(source_event_id)
        market_slug = str(row.get("market_slug") or "")
        if market_slug:
            current_truth_market_windows.add(market_slug)
    evidence["runtime_candidate_distinct_source_events"] = len(required_source_events)
    evidence["runtime_candidate_distinct_market_windows"] = len(required_market_windows)
    evidence["runtime_current_truth_rows"] = current_truth_rows
    evidence["runtime_current_truth_distinct_source_events"] = len(current_truth_source_events)
    evidence["runtime_current_truth_distinct_market_windows"] = len(current_truth_market_windows)
    evidence["runtime_proof_index_rows"] = len(indexed_scores)
    evidence["runtime_proof_index_alias_rows"] = sum(
        1 for row in indexed_scores if isinstance(row, dict) and row.get("runtime_candidate_id_alias_matched")
    )
    evidence["runtime_proof_index_key_matched_rows"] = sum(
        1 for row in indexed_scores if isinstance(row, dict) and row.get("runtime_candidate_key_matched")
    )
    evidence["runtime_proof_index_legacy_bridge_rows"] = sum(
        1 for row in indexed_scores if isinstance(row, dict) and row.get("runtime_candidate_legacy_policy_matched")
    )
    evidence["runtime_proof_index_sources"] = indexed_score_sources
    evidence["runtime_proof_total_rows"] = len(evidence_scores)
    patched["runtime_copy_evidence"] = evidence
    dirty_counts = [
        int(candidate_copy_summary.get("fallback_filled_buy_copy_events") or 0),
        int(candidate_copy_summary.get("rejected_buy_copy_events") or 0),
        int(candidate_copy_summary.get("missed_buy_copy_events") or 0),
    ]
    if any(count > 0 for count in dirty_counts):
        patched["runtime_copy_evidence_gate_blockers"] = [
            "runtime_candidate_dirty_copy_evidence"
        ]
        return patched
    required = max(int(candidate_copy_summary.get("required_buy_copy_events") or 0), len(evidence_scores))
    clob_filled = max(int(candidate_copy_summary.get("clob_filled_buy_copy_events") or 0), len(evidence_scores))
    if evidence_scores:
        # Candidate-scoped runtime proof is the strict CLOB truth used for the
        # live gate. Normalize the summary to the full proof row count so the
        # certificate cannot under-report a multi-row proof as a one-row poll.
        evidence["required_buy_copy_events"] = required
        evidence["clob_filled_buy_copy_events"] = clob_filled
        evidence["fallback_filled_buy_copy_events"] = 0
        evidence["rejected_buy_copy_events"] = 0
        evidence["missed_buy_copy_events"] = 0
        if indexed_scores and not event_scores and truth.get("status") != "PASS":
            # Persisted runtime proof is candidate-scoped CLOB truth, but
            # current tracker truth remains a separate live-readiness gate.
            if "runtime_tracker_state" in indexed_score_sources:
                evidence["runtime_candidate_tracker_state_proof_used_without_current_tracker_pass"] = True
            else:
                evidence["runtime_candidate_persisted_proof_used_without_current_tracker_pass"] = True
        patched["runtime_copy_evidence"] = evidence
    if required <= 0 or clob_filled < required:
        return patched
    if clob_filled < int(min_required_buy_copy_events):
        patched["runtime_copy_evidence_gate_blockers"] = [
            "runtime_candidate_required_buy_copy_events_below_minimum"
        ]
        return patched
    if len(required_market_windows) < int(min_required_market_windows):
        patched["runtime_copy_evidence_gate_blockers"] = [
            "runtime_candidate_required_market_windows_below_minimum"
        ]
        return patched
    live_target_profile = (
        patched.get("live_target_profile")
        if isinstance(patched.get("live_target_profile"), dict)
        else {}
    )
    runtime_resolvable_blockers = _runtime_resolvable_blockers_for_live_target(
        blockers,
        live_target_profile,
    )
    structural_blockers = [
        blocker for blocker in blockers if blocker not in runtime_resolvable_blockers
    ]
    empty_blocker_status_is_runtime_clob_gap = (
        patched.get("status") != PASS
        and not blockers
        and live_target_profile.get("status") == PASS
    )
    resolved_runtime_blockers = [
        blocker for blocker in blockers if blocker in runtime_resolvable_blockers
    ]
    if resolved_runtime_blockers or empty_blocker_status_is_runtime_clob_gap:
        patched["historical_fill_evidence_summary"] = patched.get("fill_evidence_summary") or {}
        patched["fill_evidence_summary"] = {
            **(patched.get("fill_evidence_summary") if isinstance(patched.get("fill_evidence_summary"), dict) else {}),
            "runtime_candidate_clob_backed_required_buy_copy_events": required,
            "runtime_candidate_clob_backed_filled_buy_copy_events": clob_filled,
            "runtime_candidate_clob_backed_market_windows": len(required_market_windows),
            "runtime_candidate_clob_evidence_source": source,
        }
        if resolved_runtime_blockers:
            patched["runtime_copy_evidence_resolved_blockers"] = resolved_runtime_blockers
        patched["blockers"] = structural_blockers
        patched["runtime_copy_evidence_applied"] = True
        if not patched["blockers"]:
            patched["status"] = "PASS"
    return patched


def _event_score_seconds_from_open(row: dict[str, Any]) -> float | None:
    explicit = row.get("seconds_from_open")
    if explicit is not None:
        try:
            return float(explicit)
        except (TypeError, ValueError):
            return None
    start = slug_window_start(str(row.get("market_slug") or ""))
    try:
        event_ts = float(row.get("source_event_ts"))
    except (TypeError, ValueError):
        return None
    if start is None:
        return None
    return event_ts - float(start)


def _event_score_policy_acceptance(policy: dict[str, Any], row: dict[str, Any]) -> tuple[bool, str]:
    if str(row.get("wallet_action") or "").upper() != "BUY":
        return False, "not_buy"
    try:
        price = float(row.get("source_price"))
    except (TypeError, ValueError):
        return False, "missing_price"
    min_price = float(policy.get("min_price", 0.01) or 0.01)
    max_price = float(policy.get("max_price", 1.0) or 1.0)
    if not (min_price <= price <= max_price):
        return False, "price_outside_policy"
    try:
        source_usdc_size = float(row.get("source_usdc_size") or 0.0)
    except (TypeError, ValueError):
        source_usdc_size = 0.0
    min_wallet_usdc = float(policy.get("min_wallet_usdc") or 0.0)
    max_wallet_usdc = float(policy.get("max_wallet_usdc") or 0.0)
    if source_usdc_size < min_wallet_usdc:
        return False, "wallet_size_below_minimum"
    if max_wallet_usdc > 0 and source_usdc_size > max_wallet_usdc:
        return False, "wallet_size_above_maximum"
    seconds_from_open = _event_score_seconds_from_open(row)
    min_seconds = policy.get("min_seconds_from_open")
    max_seconds = policy.get("max_seconds_from_open")
    if min_seconds is not None and (seconds_from_open is None or seconds_from_open < float(min_seconds)):
        return False, "before_timing_band"
    if max_seconds is not None and (seconds_from_open is None or seconds_from_open > float(max_seconds)):
        return False, "after_timing_band"
    return True, "accepted"


def _recent_price_compatibility(candidate: dict[str, Any], event_scores: list[dict[str, Any]]) -> dict[str, Any]:
    wallet = _candidate_source_wallet(candidate)
    candidate_policy_id = _candidate_policy_id(candidate)
    policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    min_price = float(policy.get("min_price", 0.01) or 0.01)
    max_price = float(policy.get("max_price", 1.0) or 1.0)
    price_compatible = 0
    price_incompatible = 0
    policy_compatible = 0
    policy_incompatible = 0
    policy_compatible_fresh_le_10 = 0
    policy_compatible_fresh_le_30 = 0
    policy_and_copyability_accepted = 0
    policy_copyability_rejected = 0
    freshest_compatible_lag_s: float | None = None
    freshest_policy_compatible_lag_s: float | None = None
    freshest_policy_and_copyability_accepted_lag_s: float | None = None
    rejection_reasons: Counter[str] = Counter()
    copyability_reasons: Counter[str] = Counter()
    policy_copyability_rejection_reasons: Counter[str] = Counter()
    for row in event_scores:
        if str(row.get("wallet_action") or "").upper() != "BUY":
            continue
        if wallet and str(row.get("source_wallet") or "").lower() != wallet:
            continue
        row_policy_id = str(row.get("policy_id") or row.get("profit_policy_id") or "")
        neutral_runtime_policy_ids = {"", "NO_PROFIT_POLICY", "live_tracking_exact_btc_5m_all_buys"}
        if (
            row_policy_id
            and candidate_policy_id
            and row_policy_id != candidate_policy_id
            and row_policy_id not in neutral_runtime_policy_ids
        ):
            continue
        age_s = _float_or_inf(row.get("event_age_s"))
        if age_s > 60.0:
            continue
        try:
            price = float(row.get("source_price"))
        except (TypeError, ValueError):
            continue
        if min_price <= price <= max_price:
            price_compatible += 1
            freshest_compatible_lag_s = age_s if freshest_compatible_lag_s is None else min(freshest_compatible_lag_s, age_s)
        else:
            price_incompatible += 1
        accepted, reason = _event_score_policy_acceptance(policy, row)
        if accepted:
            policy_compatible += 1
            freshest_policy_compatible_lag_s = (
                age_s
                if freshest_policy_compatible_lag_s is None
                else min(freshest_policy_compatible_lag_s, age_s)
            )
            if age_s <= 10.0:
                policy_compatible_fresh_le_10 += 1
            if age_s <= 30.0:
                policy_compatible_fresh_le_30 += 1
            if row.get("copyability_accepted") is True:
                policy_and_copyability_accepted += 1
                freshest_policy_and_copyability_accepted_lag_s = (
                    age_s
                    if freshest_policy_and_copyability_accepted_lag_s is None
                    else min(freshest_policy_and_copyability_accepted_lag_s, age_s)
                )
            else:
                policy_copyability_rejected += 1
                policy_copyability_rejection_reasons[str(row.get("copyability_reason") or "missing_copyability")] += 1
        else:
            policy_incompatible += 1
            rejection_reasons[reason] += 1
        copyability_reason = str(row.get("copyability_reason") or "")
        if copyability_reason:
            copyability_reasons[copyability_reason] += 1
    return {
        "recent_price_compatible_buy_events": price_compatible,
        "recent_price_incompatible_buy_events": price_incompatible,
        "recent_policy_compatible_buy_events": policy_compatible,
        "recent_policy_incompatible_buy_events": policy_incompatible,
        "recent_policy_compatible_fresh_buy_events_le_10s": policy_compatible_fresh_le_10,
        "recent_policy_compatible_fresh_buy_events_le_30s": policy_compatible_fresh_le_30,
        "recent_policy_and_copyability_accepted_buy_events": policy_and_copyability_accepted,
        "recent_policy_copyability_rejected_buy_events": policy_copyability_rejected,
        "recent_policy_rejection_reasons": dict(sorted(rejection_reasons.items())),
        "recent_copyability_reasons": dict(sorted(copyability_reasons.items())),
        "recent_policy_copyability_rejection_reasons": dict(
            sorted(policy_copyability_rejection_reasons.items())
        ),
        "freshest_price_compatible_lag_s": freshest_compatible_lag_s,
        "freshest_policy_compatible_lag_s": freshest_policy_compatible_lag_s,
        "freshest_policy_and_copyability_accepted_lag_s": freshest_policy_and_copyability_accepted_lag_s,
        "price_feedback_window_s": 60.0,
    }


def _forward_tracking_queue(
    ranked: list[dict[str, Any]],
    *,
    cfg: ProfitEngineConfig,
    proof_index_rows: Iterable[dict[str, Any]] = (),
    policies: Iterable[CandidatePolicy] = (),
) -> list[dict[str, Any]]:
    active_evidence = _merge_active_hotlane_evidence(
        _active_hotlane_wallet_evidence(cfg.active_hotlane_state_path),
        _active_hotlane_live_tracker_wallet_evidence(cfg.active_hotlane_live_tracker_state_path),
    )
    forward_tracker_paths = _candidate_forward_tracker_paths(cfg)
    forward_event_scores = _candidate_forward_event_scores_from_paths(
        [cfg.active_hotlane_live_tracker_state_path, *forward_tracker_paths]
    )
    forward_tracker_contexts = _candidate_forward_tracker_contexts(forward_tracker_paths)
    entries: list[dict[str, Any]] = []
    for rank, candidate in enumerate(ranked):
        if not isinstance(candidate, dict) or candidate.get("candidate_type") != "SINGLE_WALLET":
            continue
        wallet = _candidate_source_wallet(candidate)
        policy_id = _candidate_policy_id(candidate)
        if not wallet or not policy_id:
            continue
        viability = _candidate_forward_viability(
            candidate,
            active_evidence=active_evidence,
            fresh_lag_cap_s=cfg.forward_probe_fresh_lag_cap_s,
        )
        summary = candidate.get("summary") if isinstance(candidate.get("summary"), dict) else {}
        validation = (
            candidate.get("validation_summary")
            if isinstance(candidate.get("validation_summary"), dict)
            else {}
        )
        window_metrics = (
            summary.get("window_metrics")
            if isinstance(summary.get("window_metrics"), dict)
            else {}
        )
        price_compatibility = _recent_price_compatibility(candidate, forward_event_scores)
        forward_tracker_context = _matching_forward_tracker_context(candidate, forward_tracker_contexts)
        tracker_pinned = bool(
            forward_tracker_context.get("candidate_id")
            and str(candidate.get("candidate_id") or "") == str(forward_tracker_context.get("candidate_id"))
            and int(forward_tracker_context.get("source_buy_events") or 0) > 0
        )
        active = viability.get("active_hotlane") if isinstance(viability.get("active_hotlane"), dict) else {}
        entries.append(
            {
                "ranked_candidate_index": rank,
                "candidate_id": candidate.get("candidate_id"),
                "candidate_type": candidate.get("candidate_type"),
                "source_wallet": wallet,
                "wallet_name": (candidate.get("metadata") or {}).get("wallet_name")
                if isinstance(candidate.get("metadata"), dict)
                else None,
                "policy_id": policy_id,
                "policy": candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {},
                "status": candidate.get("status"),
                "blockers": candidate.get("blockers") or [],
                "profit_score": candidate.get("profit_score"),
                "roi_pct": summary.get("roi_pct"),
                "resolved_orders": summary.get("resolved_orders"),
                "wr_pct": summary.get("wr_pct"),
                "validation_roi_pct": validation.get("roi_pct"),
                "validation_unique_windows": validation.get("unique_windows"),
                "unique_windows": window_metrics.get("unique_windows"),
                "max_orders_per_window_ratio": window_metrics.get("max_orders_per_window_ratio"),
                "forward_viability": viability,
                "forward_price_feedback": price_compatibility,
                "forward_tracker_context": {
                    **forward_tracker_context,
                    "pinned_current_tracker_candidate": tracker_pinned,
                },
                "active_hotlane": active,
                "_candidate": candidate,
            }
        )
    existing_candidate_ids = {str(entry.get("candidate_id") or "") for entry in entries if entry.get("candidate_id")}
    policy_by_id = {policy.policy_id: policy for policy in policies}
    for proof in _runtime_proof_candidate_summaries(proof_index_rows):
        candidate_id = str(proof.get("candidate_id") or "")
        wallet = str(proof.get("source_wallet") or "").lower()
        policy_id = str(proof.get("policy_id") or "")
        if not candidate_id or not wallet or not policy_id or candidate_id in existing_candidate_ids:
            continue
        policy = policy_by_id.get(policy_id)
        if policy is None:
            continue
        synthetic_candidate = {
            "candidate_id": candidate_id,
            "candidate_type": "SINGLE_WALLET",
            "status": ANALYZE,
            "blockers": ["proof_led_candidate_not_in_current_profit_rankings"],
            "policy": policy.asdict(),
            "metadata": {
                "source_wallet": wallet,
                "wallet_name": wallet,
                "proof_led_runtime_candidate": True,
            },
            "runtime_copy_evidence": {
                "source": "candidate_runtime_proof_index",
                "candidate_id": candidate_id,
                "candidate_policy_id": policy_id,
                "candidate_source_wallet": wallet,
                "runtime_candidate_distinct_source_events": proof.get("source_events"),
                "runtime_candidate_distinct_market_windows": proof.get("market_windows"),
                "required_event_age_p95_s": proof.get("event_age_p95_s"),
                "runtime_proof_index_rows": proof.get("proof_rows"),
            },
            "summary": {},
            "validation_summary": {},
            "profit_score": 0.0,
        }
        viability = _candidate_forward_viability(
            synthetic_candidate,
            active_evidence=active_evidence,
            fresh_lag_cap_s=cfg.forward_probe_fresh_lag_cap_s,
        )
        price_compatibility = _recent_price_compatibility(synthetic_candidate, forward_event_scores)
        active = viability.get("active_hotlane") if isinstance(viability.get("active_hotlane"), dict) else {}
        entries.append(
            {
                "ranked_candidate_index": None,
                "candidate_id": candidate_id,
                "candidate_type": "SINGLE_WALLET",
                "source_wallet": wallet,
                "wallet_name": wallet,
                "policy_id": policy_id,
                "policy": policy.asdict(),
                "status": ANALYZE,
                "blockers": ["proof_led_candidate_not_in_current_profit_rankings"],
                "profit_score": 0.0,
                "roi_pct": None,
                "resolved_orders": None,
                "wr_pct": None,
                "validation_roi_pct": None,
                "validation_unique_windows": None,
                "unique_windows": None,
                "max_orders_per_window_ratio": None,
                "runtime_copy_proof": proof,
                "forward_viability": viability,
                "forward_price_feedback": price_compatibility,
                "forward_tracker_context": {
                    "candidate_id": candidate_id,
                    "candidate_source_wallet": wallet,
                    "policy_id": policy_id,
                    "pinned_current_tracker_candidate": False,
                    "source": "candidate_runtime_proof_index",
                },
                "active_hotlane": active,
                "_candidate": synthetic_candidate,
            }
        )
        existing_candidate_ids.add(candidate_id)

    def proof_probe_bucket(entry: dict[str, Any]) -> tuple[int, float, int, int, float, float]:
        blockers = {str(blocker) for blocker in entry.get("blockers") or []}
        unique_windows = int(entry.get("unique_windows") or 0)
        validation_windows = int(entry.get("validation_unique_windows") or 0)
        validation_roi = float(entry.get("validation_roi_pct") or 0.0)
        roi = float(entry.get("roi_pct") or 0.0)
        wr = float(entry.get("wr_pct") or 0.0)
        concentration_ratio = float(entry.get("max_orders_per_window_ratio") or 1.0)
        missing_clob = "candidate_missing_clob_fill_evidence" in blockers
        hard_profit_blocker = any(
            blocker.startswith(prefix)
            for blocker in blockers
            for prefix in (
                "all_roi_below",
                "all_wr_below",
                "train_roi_below",
                "train_wr_below",
                "validation_roi_below",
                "validation_wr_below",
                "drawdown_above",
                "raw_baseline_negative",
                "candidate_vs_raw",
                "candidate_research_only",
                "insufficient_",
                "unresolved_",
            )
        )
        broad_positive = (
            missing_clob
            and unique_windows >= 20
            and validation_windows >= 8
            and validation_roi > 0.0
            and roi > 0.0
            and wr >= float(cfg.live_target_min_wr_pct)
        )
        if broad_positive and not hard_profit_blocker and concentration_ratio <= 0.15:
            bucket = 0
        elif broad_positive and not hard_profit_blocker:
            bucket = 1
        elif broad_positive:
            bucket = 2
        elif missing_clob:
            bucket = 3
        else:
            bucket = 4
        if bucket <= 2:
            return (
                bucket,
                concentration_ratio,
                -validation_windows,
                -unique_windows,
                -validation_roi,
                -wr,
            )
        return (bucket, 0.0, 0, 0, 0.0, 0.0)

    def rank_key(entry: dict[str, Any]) -> tuple[Any, ...]:
        viability = entry.get("forward_viability") if isinstance(entry.get("forward_viability"), dict) else {}
        labels = set(viability.get("labels") or [])
        active = entry.get("active_hotlane") if isinstance(entry.get("active_hotlane"), dict) else {}
        price_feedback = (
            entry.get("forward_price_feedback")
            if isinstance(entry.get("forward_price_feedback"), dict)
            else {}
        )
        runtime_proof = (
            entry.get("runtime_copy_proof")
            if isinstance(entry.get("runtime_copy_proof"), dict)
            else {}
        )
        blockers = {str(blocker) for blocker in entry.get("blockers") or []}
        measurement_blockers = {
            "candidate_missing_clob_fill_evidence",
            "candidate_research_only_resolution_evidence",
            "proof_led_candidate_not_in_current_profit_rankings",
        }
        non_measurement_blockers = [
            blocker
            for blocker in blockers
            if blocker not in measurement_blockers
        ]
        hard_blocked = bool(non_measurement_blockers)
        proof_events = int(runtime_proof.get("source_events") or 0)
        proof_windows = int(runtime_proof.get("market_windows") or 0)
        proof_p95 = _float_or_inf(runtime_proof.get("event_age_p95_s"))
        proof_required_events = max(int(cfg.min_runtime_candidate_required_buy_copy_events), 10)
        proof_required_windows = max(int(cfg.min_runtime_candidate_required_market_windows), 3)
        base_proof_led_bucket = 0 if (
            proof_events >= proof_required_events
            and proof_windows >= proof_required_windows
            and proof_p95 <= 10.0
        ) else (1 if runtime_proof else 2)
        # Runtime proof answers "can we copy this wallet/policy quickly?"
        # It must not outrank a better profit target once history replay proves
        # the proof-led candidate is materially unprofitable.
        proof_led_bucket = max(base_proof_led_bucket, 3) if hard_blocked else base_proof_led_bucket
        proof_rank_events = 0 if hard_blocked else proof_events
        proof_rank_windows = 0 if hard_blocked else proof_windows
        proof_rank_p95 = float("inf") if hard_blocked else proof_p95
        tracker_context = (
            entry.get("forward_tracker_context")
            if isinstance(entry.get("forward_tracker_context"), dict)
            else {}
        )
        price_compatible = int(price_feedback.get("recent_price_compatible_buy_events") or 0)
        policy_compatible = int(price_feedback.get("recent_policy_compatible_buy_events") or 0)
        policy_compatible_fresh_10 = int(
            price_feedback.get("recent_policy_compatible_fresh_buy_events_le_10s") or 0
        )
        policy_copyability_accepted = int(
            price_feedback.get("recent_policy_and_copyability_accepted_buy_events") or 0
        )
        tracker_pinned = bool(tracker_context.get("pinned_current_tracker_candidate"))
        effective_tracker_pinned = bool(
            tracker_pinned and (policy_copyability_accepted > 0 or policy_compatible_fresh_10 > 0)
        )
        freshness = _freshness_bucket(viability.get("latest_live_event_lag_s"))
        if hard_blocked:
            trackable_probe_bucket = 4
        elif policy_copyability_accepted > 0:
            trackable_probe_bucket = 0
        elif policy_compatible_fresh_10 > 0:
            trackable_probe_bucket = 1
        elif viability.get("forward_trackable") and price_compatible > 0:
            trackable_probe_bucket = 2
        elif viability.get("forward_trackable"):
            trackable_probe_bucket = 3
        else:
            trackable_probe_bucket = 4
        # Measured policy-compatible CLOB/copyability evidence is stronger than
        # raw active-hotlane freshness. Otherwise a just-active but unmeasured
        # wallet can displace the candidate whose forward tracker already proved
        # required BUY copyability.
        return (
            proof_led_bucket,
            -proof_rank_events,
            -proof_rank_windows,
            proof_rank_p95,
            trackable_probe_bucket,
            proof_probe_bucket(entry),
            0 if "window_concentrated" not in labels else 1,
            0 if policy_copyability_accepted > 0 else 1,
            -policy_copyability_accepted,
            0 if policy_compatible_fresh_10 > 0 else 1,
            -policy_compatible_fresh_10,
            0 if viability.get("forward_trackable") else 1,
            freshness[0],
            freshness[1],
            0 if policy_compatible > 0 else 1,
            -policy_compatible,
            0 if price_compatible > 0 else 1,
            -price_compatible,
            -int(active.get("live_copyability_accepted") or 0),
            0 if effective_tracker_pinned else 1,
            0 if "candidate_missing_clob_fill_evidence" in blockers else 1,
            len(non_measurement_blockers),
            0 if "research_only_resolution" not in labels else 1,
            -int(entry.get("unique_windows") or 0),
            -int(entry.get("validation_unique_windows") or 0),
            -float(entry.get("wr_pct") or 0.0),
            -float(entry.get("validation_roi_pct") or 0.0),
            float(entry.get("max_orders_per_window_ratio") or 1.0),
            int(active.get("active_rank") or 9999),
            -float(entry.get("profit_score") or 0.0),
            -float(entry.get("roi_pct") or 0.0),
            int(entry.get("ranked_candidate_index") or 0),
        )

    sorted_entries = sorted(entries, key=rank_key)
    queue: list[dict[str, Any]] = []
    seen_candidate_ids: set[str] = set()
    seen_wallets: set[str] = set()
    target_size = max(1, int(cfg.forward_tracking_queue_size))

    def add_entry(entry: dict[str, Any]) -> None:
        candidate_id = str(entry.get("candidate_id") or "")
        if candidate_id and candidate_id in seen_candidate_ids:
            return
        public_entry = {key: value for key, value in entry.items() if key != "_candidate"}
        public_entry["forward_queue_rank"] = len(queue)
        queue.append(public_entry)
        if candidate_id:
            seen_candidate_ids.add(candidate_id)
        wallet = str(entry.get("source_wallet") or "").lower()
        if wallet:
            seen_wallets.add(wallet)

    def has_current_policy_signal(entry: dict[str, Any]) -> bool:
        feedback = (
            entry.get("forward_price_feedback")
            if isinstance(entry.get("forward_price_feedback"), dict)
            else {}
        )
        return (
            int(feedback.get("recent_policy_and_copyability_accepted_buy_events") or 0) > 0
            or int(feedback.get("recent_policy_compatible_fresh_buy_events_le_10s") or 0) > 0
            or int(feedback.get("recent_policy_compatible_buy_events") or 0) > 0
        )

    def is_live_target_missing_only_runtime_measurement(entry: dict[str, Any]) -> bool:
        blockers = {str(blocker) for blocker in entry.get("blockers") or []}
        measurement_blockers = {
            "candidate_missing_clob_fill_evidence",
            "candidate_research_only_resolution_evidence",
            "proof_led_candidate_not_in_current_profit_rankings",
        }
        if not blockers or any(blocker not in measurement_blockers for blocker in blockers):
            return False
        candidate = entry.get("_candidate") if isinstance(entry.get("_candidate"), dict) else {}
        live_target = (
            candidate.get("live_target_profile")
            if isinstance(candidate.get("live_target_profile"), dict)
            else {}
        )
        if live_target.get("status") == PASS:
            return True
        summary = candidate.get("summary") if isinstance(candidate.get("summary"), dict) else {}
        validation = (
            candidate.get("validation_summary")
            if isinstance(candidate.get("validation_summary"), dict)
            else {}
        )
        window_metrics = (
            summary.get("window_metrics")
            if isinstance(summary.get("window_metrics"), dict)
            else {}
        )
        return (
            int(summary.get("resolved_orders") or 0) >= int(cfg.live_target_min_resolved_orders)
            and int(window_metrics.get("unique_windows") or 0) >= int(cfg.live_target_min_unique_windows)
            and float(window_metrics.get("avg_orders_per_window") or 0.0)
            >= float(cfg.live_target_min_avg_orders_per_window)
            and float(summary.get("wr_pct") or 0.0) >= float(cfg.live_target_min_wr_pct)
            and float(validation.get("wr_pct") or 0.0) >= float(cfg.live_target_min_validation_wr_pct)
            and float(summary.get("roi_pct") or 0.0) >= float(cfg.live_target_min_roi_pct)
        )

    for entry in sorted_entries:
        wallet = str(entry.get("source_wallet") or "").lower()
        if (
            wallet in seen_wallets
            and not has_current_policy_signal(entry)
            and not is_live_target_missing_only_runtime_measurement(entry)
        ):
            continue
        add_entry(entry)
        if len(queue) >= target_size:
            return queue
    for entry in sorted_entries:
        if len(queue) >= target_size:
            break
        add_entry(entry)
    return queue


def _policy_from_queue_entry(
    entry: dict[str, Any],
    policy_by_id: dict[str, CandidatePolicy],
) -> CandidatePolicy | None:
    def optional_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        return float(value)

    policy_id = str(entry.get("policy_id") or "")
    if policy_id and policy_id in policy_by_id:
        return policy_by_id[policy_id]
    policy_payload = entry.get("policy") if isinstance(entry.get("policy"), dict) else {}
    if not policy_payload:
        return None
    try:
        return CandidatePolicy(
            policy_id=str(policy_payload["policy_id"]),
            min_price=float(policy_payload.get("min_price", 0.01) or 0.01),
            max_price=float(policy_payload.get("max_price", 1.0) or 1.0),
            min_wallet_usdc=float(policy_payload.get("min_wallet_usdc", 0.0) or 0.0),
            max_wallet_usdc=float(policy_payload.get("max_wallet_usdc", 0.0) or 0.0),
            min_seconds_from_open=optional_float(policy_payload.get("min_seconds_from_open")),
            max_seconds_from_open=optional_float(policy_payload.get("max_seconds_from_open")),
            wallet_fraction=float(policy_payload.get("wallet_fraction", 0.05) or 0.05),
            max_order_usd=float(policy_payload.get("max_order_usd", 2.0) or 2.0),
            min_order_usd=float(policy_payload.get("min_order_usd", 0.0) or 0.0),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _reconcile_proof_led_forward_queue(
    queue: list[dict[str, Any]],
    *,
    events_by_wallet: dict[str, list[WalletEvent]],
    resolutions: dict[str, dict[str, Any]],
    cfg: ProfitEngineConfig,
    policies: Iterable[CandidatePolicy],
    latest_resolution_expiry: int | None = None,
) -> list[dict[str, Any]]:
    """Attach history replay profit metrics to proof-led runtime candidates."""

    if latest_resolution_expiry is None:
        latest_resolution_expiry = _latest_resolution_expiry(resolutions)
    policy_by_id = {policy.policy_id: policy for policy in policies}
    reconciled_queue: list[dict[str, Any]] = []
    for entry in queue:
        if not isinstance(entry, dict):
            continue
        patched = dict(entry)
        runtime_proof = (
            patched.get("runtime_copy_proof")
            if isinstance(patched.get("runtime_copy_proof"), dict)
            else {}
        )
        if not runtime_proof:
            reconciled_queue.append(patched)
            continue
        wallet = str(patched.get("source_wallet") or runtime_proof.get("source_wallet") or "").lower()
        policy = _policy_from_queue_entry(patched, policy_by_id)
        wallet_events = list(events_by_wallet.get(wallet) or [])
        if not wallet or policy is None or not wallet_events:
            reconciliation = {
                "status": "BLOCKED",
                "blockers": [
                    "proof_led_reconciliation_missing_wallet_events"
                    if wallet and policy is not None
                    else "proof_led_reconciliation_missing_wallet_or_policy"
                ],
                "source_wallet": wallet,
                "policy_id": str(patched.get("policy_id") or runtime_proof.get("policy_id") or ""),
                "history_profit_metrics_present": False,
                "resolved_orders": 0,
                "wr_pct": 0.0,
                "roi_pct": 0.0,
            }
            patched["proof_led_profit_reconciliation"] = reconciliation
            patched.setdefault("blockers", [])
            for blocker in reconciliation["blockers"]:
                if blocker not in patched["blockers"]:
                    patched["blockers"].append(blocker)
            reconciled_queue.append(patched)
            continue

        intents = intents_for_policy(wallet_events, policy)
        original_intent_count = len(intents)
        if (
            cfg.max_single_wallet_candidate_intents > 0
            and original_intent_count > int(cfg.max_single_wallet_candidate_intents)
        ):
            intents = intents[-int(cfg.max_single_wallet_candidate_intents) :]
        raw_summary = _raw_baseline_summary(wallet_events, policy=policy, resolutions=resolutions, cfg=cfg)
        evaluated = evaluate_candidate(
            candidate_type=str(patched.get("candidate_type") or "SINGLE_WALLET"),
            policy=policy,
            intents=intents,
            resolutions=resolutions,
            cfg=cfg,
            raw_baseline_summary=raw_summary,
            latest_resolution_expiry=latest_resolution_expiry,
            metadata={
                "source_wallet": wallet,
                "wallet_name": patched.get("wallet_name") or wallet,
                "intent_count": len(intents),
                "original_intent_count": original_intent_count,
                "intents_limited": len(intents) < original_intent_count,
                "max_single_wallet_candidate_intents": cfg.max_single_wallet_candidate_intents,
                "proof_led_runtime_candidate": True,
                "proof_led_runtime_candidate_id": patched.get("candidate_id"),
                "proof_led_runtime_policy_id": policy.policy_id,
            },
        )
        summary = evaluated.get("summary") if isinstance(evaluated.get("summary"), dict) else {}
        validation = (
            evaluated.get("validation_summary")
            if isinstance(evaluated.get("validation_summary"), dict)
            else {}
        )
        window_metrics = (
            summary.get("window_metrics")
            if isinstance(summary.get("window_metrics"), dict)
            else {}
        )
        proof_events = int(runtime_proof.get("source_events") or 0)
        proof_windows = int(runtime_proof.get("market_windows") or 0)
        proof_p95 = _float_or_inf(runtime_proof.get("event_age_p95_s"))
        required_proof_events = max(int(cfg.min_runtime_candidate_required_buy_copy_events), 10)
        required_proof_windows = max(int(cfg.min_runtime_candidate_required_market_windows), 3)
        proof_minimum_blockers: list[str] = []
        if proof_events < required_proof_events:
            proof_minimum_blockers.append("runtime_candidate_required_buy_copy_events_below_minimum")
        if proof_windows < required_proof_windows:
            proof_minimum_blockers.append("runtime_candidate_required_market_windows_below_minimum")
        if proof_p95 > 10.0:
            proof_minimum_blockers.append("runtime_candidate_event_age_p95_above_10s")
        proof_minimum_status = PASS if not proof_minimum_blockers else ANALYZE
        blockers = list(evaluated.get("blockers") or [])
        live_target_profile = evaluated.get("live_target_profile") or {}
        runtime_resolvable_blockers = _runtime_resolvable_blockers_for_live_target(
            blockers,
            live_target_profile if isinstance(live_target_profile, dict) else {},
        )
        resolved_runtime_blockers: list[str] = []
        if proof_minimum_status == PASS:
            resolved_runtime_blockers = [
                blocker for blocker in blockers if blocker in runtime_resolvable_blockers
            ]
            blockers = [
                blocker for blocker in blockers if blocker not in runtime_resolvable_blockers
            ]
        elif (
            "candidate_missing_clob_fill_evidence" in blockers
            and "runtime_copy_evidence_pending" not in blockers
        ):
            blockers.append("runtime_copy_evidence_pending")
        patched_status = evaluated.get("status") or patched.get("status")
        live_target_status = (
            live_target_profile.get("status")
            if isinstance(live_target_profile, dict)
            else None
        )
        if proof_minimum_status == PASS and not blockers and live_target_status == PASS:
            patched_status = PASS
        fill_evidence_summary = evaluated.get("fill_evidence_summary") or {}
        if resolved_runtime_blockers:
            fill_evidence_summary = {
                **(
                    fill_evidence_summary
                    if isinstance(fill_evidence_summary, dict)
                    else {}
                ),
                "runtime_candidate_clob_backed_required_buy_copy_events": proof_events,
                "runtime_candidate_clob_backed_filled_buy_copy_events": proof_events,
                "runtime_candidate_clob_backed_market_windows": proof_windows,
                "runtime_candidate_clob_evidence_source": "candidate_runtime_proof_index",
            }
        patched.update(
            {
                "status": patched_status,
                "blockers": blockers,
                "profit_score": evaluated.get("profit_score"),
                "roi_pct": summary.get("roi_pct"),
                "resolved_orders": summary.get("resolved_orders"),
                "wr_pct": summary.get("wr_pct"),
                "validation_roi_pct": validation.get("roi_pct"),
                "validation_wr_pct": validation.get("wr_pct"),
                "validation_unique_windows": validation.get("unique_windows"),
                "unique_windows": window_metrics.get("unique_windows"),
                "max_orders_per_window_ratio": window_metrics.get("max_orders_per_window_ratio"),
                "summary": summary,
                "train_summary": evaluated.get("train_summary") or {},
                "validation_summary": validation,
                "fill_evidence_summary": fill_evidence_summary,
                "raw_baseline_summary": evaluated.get("raw_baseline_summary") or {},
                "resolution_evidence_summary": evaluated.get("resolution_evidence_summary") or {},
                "live_target_profile": live_target_profile,
                "runtime_copy_evidence_resolved_blockers": resolved_runtime_blockers,
                "proof_led_profit_reconciliation": {
                    "status": patched_status,
                    "evaluated_candidate_id": evaluated.get("candidate_id"),
                    "proof_led_candidate_id": patched.get("candidate_id"),
                    "proof_led_not_in_rankings": True,
                    "history_profit_metrics_present": True,
                    "proof_minimum_status": proof_minimum_status,
                    "proof_minimum_blockers": proof_minimum_blockers,
                    "proof_events": proof_events,
                    "proof_events_required": required_proof_events,
                    "proof_events_gap": max(0, required_proof_events - proof_events),
                    "proof_windows": proof_windows,
                    "proof_windows_required": required_proof_windows,
                    "proof_windows_gap": max(0, required_proof_windows - proof_windows),
                    "proof_event_age_p95_s": None if proof_p95 == float("inf") else proof_p95,
                    "policy_id": policy.policy_id,
                    "source_wallet": wallet,
                    "intent_count": len(intents),
                    "original_intent_count": original_intent_count,
                    "resolved_orders": summary.get("resolved_orders"),
                    "wr_pct": summary.get("wr_pct"),
                    "roi_pct": summary.get("roi_pct"),
                    "validation_wr_pct": validation.get("wr_pct"),
                    "validation_roi_pct": validation.get("roi_pct"),
                    "blockers": blockers,
                    "resolved_runtime_blockers": resolved_runtime_blockers,
                },
            }
        )
        reconciled_queue.append(patched)
    return reconciled_queue


def _rerank_reconciled_forward_queue(queue: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reorder forward probes once proof-led candidates have paper profit metrics."""

    def metric_float(row: dict[str, Any], key: str, default: float = 0.0) -> float:
        value = row.get(key)
        reconciliation = (
            row.get("proof_led_profit_reconciliation")
            if isinstance(row.get("proof_led_profit_reconciliation"), dict)
            else {}
        )
        if value in (None, ""):
            value = reconciliation.get(key)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def metric_int(row: dict[str, Any], key: str, default: int = 0) -> int:
        try:
            return int(metric_float(row, key, float(default)))
        except (TypeError, ValueError):
            return default

    def original_rank(row: dict[str, Any]) -> int:
        try:
            return int(row.get("forward_queue_rank") or 0)
        except (TypeError, ValueError):
            return 0

    hard_prefixes = (
        "all_roi_below",
        "all_wr_below",
        "train_roi_below",
        "train_wr_below",
        "validation_roi_below",
        "validation_wr_below",
        "drawdown_above",
        "candidate_vs_raw",
        "raw_baseline_negative",
    )

    def rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
        reconciliation = (
            row.get("proof_led_profit_reconciliation")
            if isinstance(row.get("proof_led_profit_reconciliation"), dict)
            else {}
        )
        blockers = {
            str(blocker)
            for blocker in (reconciliation.get("blockers") or row.get("blockers") or [])
        }
        hard_profit_blocked = any(
            blocker.startswith(prefix)
            for blocker in blockers
            for prefix in hard_prefixes
        )
        roi = metric_float(row, "roi_pct")
        wr = metric_float(row, "wr_pct")
        validation_roi = metric_float(row, "validation_roi_pct")
        validation_wr = metric_float(row, "validation_wr_pct")
        profit_score = metric_float(row, "profit_score")
        resolved_orders = metric_int(row, "resolved_orders")
        price_feedback = (
            row.get("forward_price_feedback")
            if isinstance(row.get("forward_price_feedback"), dict)
            else {}
        )
        viability = row.get("forward_viability") if isinstance(row.get("forward_viability"), dict) else {}
        tracker_context = (
            row.get("forward_tracker_context")
            if isinstance(row.get("forward_tracker_context"), dict)
            else {}
        )
        policy_copyability_accepted = int(
            price_feedback.get("recent_policy_and_copyability_accepted_buy_events") or 0
        )
        policy_compatible_fresh_10 = int(
            price_feedback.get("recent_policy_compatible_fresh_buy_events_le_10s") or 0
        )
        policy_compatible = int(price_feedback.get("recent_policy_compatible_buy_events") or 0)
        tracker_pinned = bool(tracker_context.get("pinned_current_tracker_candidate"))
        if tracker_pinned and policy_copyability_accepted > 0:
            current_bucket = 0
        elif policy_copyability_accepted > 0:
            current_bucket = 1
        elif policy_compatible_fresh_10 > 0:
            current_bucket = 2
        elif policy_compatible > 0:
            current_bucket = 3
        elif viability.get("forward_trackable"):
            current_bucket = 4
        else:
            current_bucket = 5
        proof_minimum_status = str(reconciliation.get("proof_minimum_status") or "")
        proof_events = metric_int(reconciliation, "proof_events")
        proof_pass = proof_minimum_status == PASS
        # This is a measurement queue, not live admission. A stale proof-led
        # candidate whose reconciled paper metrics are materially losing should
        # not monopolize current-poll proof just because it once copied fast.
        positive_profit_probe = roi > 0.0 and profit_score > 0.0 and resolved_orders >= 50
        near_wr_probe = wr >= 65.0
        validation_support = validation_roi > 0.0 and validation_wr >= 60.0
        if not hard_profit_blocked and proof_pass and current_bucket <= 1:
            bucket = 0
        elif positive_profit_probe and current_bucket <= 1:
            bucket = 1
        elif positive_profit_probe and current_bucket <= 3:
            bucket = 2
        elif positive_profit_probe and near_wr_probe and validation_support:
            bucket = 3
        elif positive_profit_probe and near_wr_probe:
            bucket = 4
        elif positive_profit_probe:
            bucket = 5
        elif proof_pass:
            bucket = 6
        else:
            bucket = 7
        return (
            0 if row.get("development_program_inventory_target_entry") is True else 1,
            0 if row.get("development_program_bridge_focus") is True else 1,
            bucket,
            current_bucket,
            0 if tracker_pinned else 1,
            -policy_copyability_accepted,
            -policy_compatible_fresh_10,
            -policy_compatible,
            0 if not hard_profit_blocked else 1,
            -profit_score,
            -roi,
            -validation_roi,
            -wr,
            -validation_wr,
            -resolved_orders,
            -proof_events,
            original_rank(row),
        )

    reranked = [dict(row) for row in sorted(queue, key=rank_key)]
    for index, row in enumerate(reranked):
        row["forward_queue_rank"] = index
    return reranked


def _candidate_from_queue_entry(
    ranked: list[dict[str, Any]],
    entry: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        return None
    entry_source_wallets = [
        str(wallet or "").lower()
        for wallet in (
            entry.get("source_wallets")
            if isinstance(entry.get("source_wallets"), list)
            else []
        )
        if str(wallet or "").lower().startswith("0x")
    ]

    def with_bridge_metadata(candidate: dict[str, Any]) -> dict[str, Any]:
        if entry.get("development_program_bridge_focus") is not True:
            return candidate
        patched = dict(candidate)
        metadata = dict(patched.get("metadata")) if isinstance(patched.get("metadata"), dict) else {}
        metadata["development_program_bridge_focus"] = True
        metadata["development_program_bridge_roles"] = entry.get("development_program_bridge_roles") or []
        metadata["development_program_target_inventory"] = entry.get(
            "development_program_target_inventory"
        ) or {}
        if entry.get("development_program_inventory_target_entry") is True:
            metadata["development_program_inventory_target_entry"] = True
            patched["development_program_inventory_target_entry"] = True
        if entry_source_wallets:
            metadata["source_wallets"] = entry_source_wallets
            patched["source_wallets"] = entry_source_wallets
        patched["metadata"] = metadata
        patched["development_program_bridge_focus"] = True
        patched["development_program_next_major_change_action"] = entry.get(
            "development_program_next_major_change_action"
        )
        return patched

    candidate_id = str(entry.get("candidate_id") or "")
    if candidate_id:
        for candidate in ranked:
            if isinstance(candidate, dict) and str(candidate.get("candidate_id") or "") == candidate_id:
                return with_bridge_metadata(candidate)
    wallet = str(entry.get("source_wallet") or "").lower()
    policy_id = str(entry.get("policy_id") or "")
    reconciliation = (
        entry.get("proof_led_profit_reconciliation")
        if isinstance(entry.get("proof_led_profit_reconciliation"), dict)
        else {}
    )

    def direct_proof_led_candidate() -> dict[str, Any] | None:
        policy = entry.get("policy") if isinstance(entry.get("policy"), dict) else {}
        if not (wallet and policy_id and policy):
            return None
        reconciliation_blockers = (
            list(reconciliation.get("blockers") or [])
            if isinstance(reconciliation, dict)
            else []
        )
        entry_blockers = list(entry.get("blockers") or reconciliation_blockers)
        metadata = {
            "source_wallet": wallet,
            "wallet_name": entry.get("wallet_name") or wallet,
            "proof_led_runtime_candidate": bool(entry.get("runtime_copy_proof")),
            "candidate_id_source": "forward_tracking_queue_runtime_proof",
        }
        if entry_source_wallets:
            metadata["source_wallets"] = entry_source_wallets
        if isinstance(reconciliation, dict):
            metadata["evaluated_candidate_id"] = reconciliation.get("evaluated_candidate_id")
            metadata["proof_led_not_in_current_profit_rankings"] = bool(
                reconciliation.get("proof_led_not_in_rankings")
            )
        return with_bridge_metadata({
            "candidate_id": candidate_id,
            "candidate_type": str(entry.get("candidate_type") or "SINGLE_WALLET"),
            "status": str(entry.get("status") or ANALYZE),
            "blockers": entry_blockers,
            "metadata": metadata,
            "source_wallet": wallet,
            "policy": policy,
            "summary": entry.get("summary") if isinstance(entry.get("summary"), dict) else {},
            "train_summary": entry.get("train_summary")
            if isinstance(entry.get("train_summary"), dict)
            else {},
            "validation_summary": entry.get("validation_summary")
            if isinstance(entry.get("validation_summary"), dict)
            else {},
            "fill_evidence_summary": entry.get("fill_evidence_summary")
            if isinstance(entry.get("fill_evidence_summary"), dict)
            else {},
            "raw_baseline_summary": entry.get("raw_baseline_summary")
            if isinstance(entry.get("raw_baseline_summary"), dict)
            else {},
            "resolution_evidence_summary": entry.get("resolution_evidence_summary")
            if isinstance(entry.get("resolution_evidence_summary"), dict)
            else {},
            "live_target_profile": entry.get("live_target_profile")
            if isinstance(entry.get("live_target_profile"), dict)
            else {},
            "runtime_copy_evidence": entry.get("runtime_copy_proof")
            if isinstance(entry.get("runtime_copy_proof"), dict)
            else {},
            "proof_led_profit_reconciliation": reconciliation,
            "profit_score": entry.get("profit_score"),
        })

    if reconciliation.get("status") == PASS:
        direct_candidate = direct_proof_led_candidate()
        if isinstance(direct_candidate, dict):
            return direct_candidate

    evaluated_candidate_id = str(reconciliation.get("evaluated_candidate_id") or "")
    if evaluated_candidate_id:
        for candidate in ranked:
            if isinstance(candidate, dict) and str(candidate.get("candidate_id") or "") == evaluated_candidate_id:
                resolved = dict(candidate)
                metadata = (
                    dict(resolved.get("metadata"))
                    if isinstance(resolved.get("metadata"), dict)
                    else {}
                )
                metadata.setdefault("source_wallet", wallet or _candidate_source_wallet(candidate))
                if entry_source_wallets:
                    metadata["source_wallets"] = entry_source_wallets
                metadata.setdefault("wallet_name", entry.get("wallet_name") or wallet)
                metadata["proof_led_runtime_candidate"] = bool(entry.get("runtime_copy_proof"))
                metadata["proof_led_runtime_candidate_id"] = candidate_id
                metadata["candidate_id_source"] = "profit_ranked_evaluated_candidate_id"
                resolved["metadata"] = metadata
                if isinstance(entry.get("runtime_copy_proof"), dict):
                    resolved["runtime_copy_evidence"] = entry.get("runtime_copy_proof")
                if isinstance(reconciliation, dict):
                    resolved["proof_led_profit_reconciliation"] = reconciliation
                return with_bridge_metadata(resolved)
    if isinstance(entry.get("proof_led_profit_reconciliation"), dict):
        direct_candidate = direct_proof_led_candidate()
        if isinstance(direct_candidate, dict):
            return direct_candidate
    for candidate in ranked:
        if _candidate_source_wallet(candidate) == wallet and _candidate_policy_id(candidate) == policy_id:
            if not candidate_id or str(candidate.get("candidate_id") or "") == candidate_id:
                return candidate
            resolved = dict(candidate)
            ranked_candidate_id = resolved.get("candidate_id")
            resolved["candidate_id"] = candidate_id
            metadata = (
                dict(resolved.get("metadata"))
                if isinstance(resolved.get("metadata"), dict)
                else {}
            )
            metadata.setdefault("source_wallet", wallet)
            if entry_source_wallets:
                metadata["source_wallets"] = entry_source_wallets
            metadata.setdefault("wallet_name", entry.get("wallet_name") or wallet)
            metadata["proof_led_runtime_candidate"] = bool(entry.get("runtime_copy_proof"))
            metadata["ranked_candidate_id"] = ranked_candidate_id
            metadata["candidate_id_source"] = "forward_tracking_queue_runtime_proof"
            resolved["metadata"] = metadata
            if isinstance(entry.get("runtime_copy_proof"), dict):
                resolved["runtime_copy_evidence"] = entry.get("runtime_copy_proof")
            blockers = list(resolved.get("blockers") or [])
            if "proof_led_candidate_not_in_current_profit_rankings" not in blockers:
                blockers.append("proof_led_candidate_not_in_current_profit_rankings")
            resolved["blockers"] = blockers
            return with_bridge_metadata(resolved)
    policy = entry.get("policy") if isinstance(entry.get("policy"), dict) else {}
    if wallet and policy_id and policy:
        return with_bridge_metadata({
            "candidate_id": candidate_id,
            "candidate_type": str(entry.get("candidate_type") or "SINGLE_WALLET"),
            "status": str(entry.get("status") or ANALYZE),
            "blockers": list(entry.get("blockers") or ["proof_led_candidate_not_in_current_profit_rankings"]),
            "metadata": {
                "source_wallet": wallet,
                **({"source_wallets": entry_source_wallets} if entry_source_wallets else {}),
                "wallet_name": entry.get("wallet_name") or wallet,
                "proof_led_runtime_candidate": bool(entry.get("runtime_copy_proof")),
            },
            "source_wallet": wallet,
            "policy": policy,
            "summary": entry.get("summary") if isinstance(entry.get("summary"), dict) else {},
            "validation_summary": entry.get("validation_summary")
            if isinstance(entry.get("validation_summary"), dict)
            else {},
            "runtime_copy_evidence": entry.get("runtime_copy_proof")
            if isinstance(entry.get("runtime_copy_proof"), dict)
            else {},
            "profit_score": entry.get("profit_score"),
        })
    return None


def _source_route_diagnostic_fields(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    keys = [
        "reset_by_host",
        "reset_by_transport",
        "best_nonpassing_variants",
        "direct_dns_family_counts_by_endpoint",
        "source_base_overrides",
        "source_proxy_env_var",
        "external_route_required",
        "code_route_recovery_exhausted",
        "required_operator_inputs",
    ]
    diagnostics = {key: payload.get(key) for key in keys if key in payload}
    if "source_proxy_configured" in payload:
        diagnostics["source_proxy_configured"] = payload.get("source_proxy_configured")
    return diagnostics


def _source_route_truth(path: str | Path | None) -> dict[str, Any]:
    if not str(path or ""):
        return {}
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {"status": "MISSING", "path": str(path), "blockers": ["source_route_truth_missing"]}
    raw_endpoints = payload.get("endpoints") or []
    if isinstance(raw_endpoints, dict):
        endpoints = [row for row in raw_endpoints.values() if isinstance(row, dict)]
        endpoint_statuses = {
            str(name): row.get("status")
            for name, row in raw_endpoints.items()
            if isinstance(row, dict)
        }
    else:
        endpoints = [row for row in raw_endpoints if isinstance(row, dict)]
        endpoint_statuses = {
            str(row.get("name") or f"endpoint_{index}"): row.get("status")
            for index, row in enumerate(endpoints)
        }
    route_class_counts = payload.get("route_class_counts")
    if not isinstance(route_class_counts, dict):
        route_class_counts = dict(Counter(str(row.get("route_class") or "UNKNOWN") for row in endpoints))
    if "source_proxy_configured" in payload:
        source_proxy_configured = bool(payload.get("source_proxy_configured"))
    else:
        source_proxy_configured = any(
            bool(
                row.get("source_proxy_configured")
                or row.get("proxy_configured")
                or row.get("source_base_override_configured")
            )
            for row in endpoints
        )
    status = str(payload.get("status") or "UNKNOWN")
    blockers: list[str] = []
    live_admissible_route = source_route_allows_live_execution(payload)
    if not live_admissible_route and (
        status == "POLYMARKET_ROUTE_RESET" or any(value == "DIRECT_RESET" for value in route_class_counts)
    ):
        blockers.append("source_route_polymarket_route_reset")
    elif status and status not in {"PASS", "OK"} and not live_admissible_route:
        blockers.append("source_route_truth_not_pass")
    return {
        "status": status,
        "path": str(path),
        "generated_at": payload.get("generated_at"),
        "endpoint_statuses": endpoint_statuses,
        "route_class_counts": route_class_counts,
        "source_proxy_configured": source_proxy_configured,
        "live_admissible": live_admissible_route,
        **_source_route_diagnostic_fields(payload),
        "blockers": blockers,
    }


def _live_readiness_certificate(
    *,
    decision: dict[str, Any],
    best_candidate: dict[str, Any] | None,
    forward_candidate: dict[str, Any] | None,
    pass_candidates: list[dict[str, Any]],
    effective_live_truth: dict[str, Any],
    resolution_contract: dict[str, Any],
    wallet_search_summary: dict[str, Any],
    skipped_candidate_counts: dict[str, int],
    source_route_truth: dict[str, Any] | None = None,
) -> dict[str, Any]:
    best = best_candidate if isinstance(best_candidate, dict) else {}
    forward = forward_candidate if isinstance(forward_candidate, dict) else {}
    effective_summary = (
        effective_live_truth.get("summary")
        if isinstance(effective_live_truth.get("summary"), dict)
        else {}
    )
    global_copy_summary = (
        effective_summary.get("copy_efficiency_summary")
        if isinstance(effective_summary.get("copy_efficiency_summary"), dict)
        else {}
    )
    candidate_copy_summary_raw = (
        effective_summary.get("candidate_copy_truth_summary")
        if isinstance(effective_summary.get("candidate_copy_truth_summary"), dict)
        else {}
    )
    candidate_copy_summary = candidate_copy_summary_raw
    candidate = pass_candidates[0] if pass_candidates else forward or best
    candidate_id = str(candidate.get("candidate_id") or decision.get("runtime_admission_candidate_id") or "")
    candidate_policy_id = _candidate_policy_id(candidate)
    candidate_source_wallet = _candidate_source_wallet(candidate)
    requires_candidate_copy_truth = bool(candidate_id or candidate_policy_id or candidate_source_wallet)
    runtime_copy_evidence = (
        candidate.get("runtime_copy_evidence")
        if isinstance(candidate.get("runtime_copy_evidence"), dict)
        else {}
    )
    runtime_copy_summary = {
        "required_buy_copy_events": runtime_copy_evidence.get("required_buy_copy_events"),
        "clob_filled_buy_copy_events": runtime_copy_evidence.get("clob_filled_buy_copy_events"),
        "fallback_filled_buy_copy_events": runtime_copy_evidence.get("fallback_filled_buy_copy_events"),
        "rejected_buy_copy_events": runtime_copy_evidence.get("rejected_buy_copy_events"),
        "missed_buy_copy_events": runtime_copy_evidence.get("missed_buy_copy_events"),
        "required_event_age_p95_s": runtime_copy_evidence.get("required_event_age_p95_s"),
        "required_api_latency_p95_s": runtime_copy_evidence.get("required_api_latency_p95_s"),
    }
    runtime_required_buy_events = int(runtime_copy_summary.get("required_buy_copy_events") or 0)
    runtime_clob_filled_buy_events = int(runtime_copy_summary.get("clob_filled_buy_copy_events") or 0)
    runtime_dirty_counts = (
        int(runtime_copy_summary.get("fallback_filled_buy_copy_events") or 0),
        int(runtime_copy_summary.get("rejected_buy_copy_events") or 0),
        int(runtime_copy_summary.get("missed_buy_copy_events") or 0),
    )
    runtime_copy_summary_usable = (
        runtime_required_buy_events > 0
        and runtime_clob_filled_buy_events >= runtime_required_buy_events
        and not any(count > 0 for count in runtime_dirty_counts)
    )
    runtime_proof_without_current_tracker_pass = bool(
        runtime_copy_evidence.get("runtime_candidate_persisted_proof_used_without_current_tracker_pass")
        and str(decision.get("live_tracker_truth_source") or "") == "canonical"
    )
    copy_summary = candidate_copy_summary if requires_candidate_copy_truth else global_copy_summary
    if int(copy_summary.get("required_buy_copy_events") or 0) <= 0 and runtime_copy_summary_usable:
        copy_summary = runtime_copy_summary
    candidate_resolution = (
        candidate.get("resolution_evidence_summary")
        if isinstance(candidate.get("resolution_evidence_summary"), dict)
        else {}
    )
    live_target_profile = (
        candidate.get("live_target_profile")
        if isinstance(candidate.get("live_target_profile"), dict)
        else {}
    )
    live_blockers = list(decision.get("live_admission_blockers") or [])
    proof_blockers: list[str] = []
    runtime_gate_blockers = [
        str(blocker)
        for blocker in candidate.get("runtime_copy_evidence_gate_blockers") or []
    ]
    certificate_runtime_resolvable_blockers = _runtime_resolvable_blockers_for_live_target(
        [str(blocker) for blocker in candidate.get("blockers") or []],
        live_target_profile,
    )
    structural_candidate_blockers = [
        str(blocker)
        for blocker in candidate.get("blockers") or []
        if str(blocker) not in certificate_runtime_resolvable_blockers
    ]
    proof_blockers.extend(runtime_gate_blockers)
    proof_blockers.extend(structural_candidate_blockers)
    if decision.get("status") != "PASS":
        proof_blockers.append("profit_engine_decision_not_pass")
    if live_target_profile.get("status") != "PASS":
        proof_blockers.append("live_target_profile_not_pass")
        proof_blockers.extend(str(blocker) for blocker in live_target_profile.get("blockers") or [])
    runtime_selection_used = bool(decision.get("runtime_admission_candidate_id"))
    if not pass_candidates and not runtime_selection_used:
        proof_blockers.append("no_profit_candidate_pass")
    runtime_admission_truth_usable = (
        runtime_selection_used
        and decision.get("live_admission_status") == PASS
        and str(decision.get("runtime_admission_candidate_id") or "") == candidate_id
        and runtime_copy_summary_usable
        and not runtime_proof_without_current_tracker_pass
    )
    if effective_live_truth.get("status") != "PASS" and not runtime_admission_truth_usable:
        proof_blockers.append("effective_live_tracker_truth_not_pass")
    if runtime_proof_without_current_tracker_pass:
        proof_blockers.append("runtime_copy_evidence_missing_current_tracker_pass")
    candidate_canonical_profile = (
        candidate.get("canonical_resolution_profile")
        if isinstance(candidate.get("canonical_resolution_profile"), dict)
        else {}
    )
    candidate_canonical_resolution_pass = candidate_canonical_profile.get("status") == PASS
    candidate_research_only_resolutions = int(candidate_resolution.get("research_only_resolved_orders") or 0)
    if (
        int(resolution_contract.get("research_only_count") or 0) > 0
        and candidate_research_only_resolutions > 0
        and not candidate_canonical_resolution_pass
    ):
        proof_blockers.append("resolution_source_contains_research_only_rows")
    if candidate_research_only_resolutions > 0 and not candidate_canonical_resolution_pass:
        proof_blockers.append("candidate_uses_research_only_resolutions")
    bounded_or_limited_candidate_counts = _live_candidate_relevant_limited_counts(
        candidate,
        skipped_candidate_counts,
    )
    if any(value > 0 for value in bounded_or_limited_candidate_counts.values()):
        proof_blockers.append("candidate_search_was_bounded_or_limited")
    if requires_candidate_copy_truth and not candidate_copy_summary and not runtime_copy_summary_usable:
        proof_blockers.append("candidate_specific_copy_truth_missing")
    if int(copy_summary.get("required_buy_copy_events") or 0) <= 0:
        proof_blockers.append("no_required_clob_backed_buy_copy_evidence")
    if int(copy_summary.get("fallback_filled_buy_copy_events") or 0) > 0:
        proof_blockers.append("fallback_fills_not_live_ready")
    if int(copy_summary.get("rejected_buy_copy_events") or 0) > 0:
        proof_blockers.append("rejected_copy_events_not_live_ready")
    if int(copy_summary.get("missed_buy_copy_events") or 0) > 0:
        proof_blockers.append("missed_copy_events_not_live_ready")
    route_truth = source_route_truth if isinstance(source_route_truth, dict) else {}
    proof_blockers.extend(str(blocker) for blocker in route_truth.get("blockers") or [])

    blockers = sorted(set(live_blockers + proof_blockers))
    policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    summary = candidate.get("summary") if isinstance(candidate.get("summary"), dict) else {}
    validation = candidate.get("validation_summary") if isinstance(candidate.get("validation_summary"), dict) else {}
    return {
        "schema_version": 1,
        "status": "PROOF_READY" if not blockers else active_status_from_blockers(blockers, default=ANALYZE),
        "profitability_proven": not blockers,
        "live_ready": not blockers,
        "reason": (
            "profitable_candidate_has_canonical_resolution_and_clob_backed_copy_truth"
            if not blockers
            else "paper_research_or_copy_truth_gates_still_block_live_readiness"
        ),
        "blockers": blockers,
        "paper_results": {
            "candidate_id": candidate.get("candidate_id"),
            "candidate_type": candidate.get("candidate_type"),
            "candidate_status": candidate.get("status"),
            "source_wallet": candidate.get("source_wallet") or metadata.get("source_wallet"),
            "policy_id": policy.get("policy_id"),
            "resolved_orders": summary.get("resolved_orders"),
            "roi_pct": summary.get("roi_pct"),
            "wr_pct": summary.get("wr_pct"),
            "pnl_usd": summary.get("pnl_usd"),
            "validation_resolved_orders": validation.get("resolved_orders"),
            "validation_roi_pct": validation.get("roi_pct"),
            "validation_wr_pct": validation.get("wr_pct"),
            "unique_windows": summary.get("window_metrics", {}).get("unique_windows")
            if isinstance(summary.get("window_metrics"), dict)
            else None,
            "avg_orders_per_window": summary.get("window_metrics", {}).get("avg_orders_per_window")
            if isinstance(summary.get("window_metrics"), dict)
            else None,
            "candidate_blockers": candidate.get("blockers") or [],
            "live_target_profile": live_target_profile,
            "runtime_copy_evidence_gate_blockers": runtime_gate_blockers,
            "runtime_copy_evidence": candidate.get("runtime_copy_evidence")
            if isinstance(candidate.get("runtime_copy_evidence"), dict)
            else {},
        },
        "copy_truth": {
            "source": decision.get("live_tracker_truth_source"),
            "effective_live_tracker_truth_status": effective_live_truth.get("status"),
            "candidate_specific_copy_truth_required": requires_candidate_copy_truth,
            "candidate_specific_copy_truth_present": bool(candidate_copy_summary or runtime_copy_summary_usable),
            "candidate_specific_copy_truth_source": (
                "effective_live_tracker_truth"
                if candidate_copy_summary
                else ("runtime_copy_evidence" if runtime_copy_summary_usable else None)
            ),
            "candidate_id": candidate_id,
            "candidate_policy_id": candidate_policy_id,
            "candidate_source_wallet": candidate_source_wallet,
            "required_buy_copy_events": copy_summary.get("required_buy_copy_events"),
            "clob_filled_buy_copy_events": copy_summary.get("clob_filled_buy_copy_events"),
            "fallback_filled_buy_copy_events": copy_summary.get("fallback_filled_buy_copy_events"),
            "rejected_buy_copy_events": copy_summary.get("rejected_buy_copy_events"),
            "missed_buy_copy_events": copy_summary.get("missed_buy_copy_events"),
            "required_event_age_p95_s": copy_summary.get("required_event_age_p95_s"),
            "required_api_latency_p95_s": copy_summary.get("required_api_latency_p95_s"),
            "global_required_buy_copy_events": global_copy_summary.get("required_buy_copy_events"),
            "global_clob_filled_buy_copy_events": global_copy_summary.get("clob_filled_buy_copy_events"),
            "runtime_proof_index_rows": runtime_copy_evidence.get("runtime_proof_index_rows"),
            "runtime_proof_total_rows": runtime_copy_evidence.get("runtime_proof_total_rows"),
        },
        "resolution_truth": {
            "contract": resolution_contract,
            "candidate_resolution_evidence": candidate_resolution,
        },
        "source_route_truth": route_truth,
        "search_truth": {
            "wallet_search_summary": wallet_search_summary,
            "skipped_candidate_counts": skipped_candidate_counts,
        },
        "live_connection_plan": {
            "mode": active_plan_mode(not blockers, blockers, target="CERTIFICATE_PROOF_READY"),
            "paper_live_parity": "same CopyIntent lifecycle; live only flips execution permission",
            "live_orders_allowed": False,
            "explicit_operator_gate_required": True,
            "do_not_use_if_blockers_present": True,
        },
    }


def _live_admission_runtime_gate_blockers(
    *,
    runtime_admission_candidate: dict[str, Any] | None,
    forward_runtime_candidate: dict[str, Any] | None,
    best_runtime_candidate: dict[str, Any] | None,
) -> list[str]:
    """Return runtime proof blockers only for the candidate live admission can use.

    Ranked forward probes often include thin diagnostic candidates. Those should
    remain visible in their own candidate reports, but they must not globally
    block a stronger proof-led candidate or make the live-readiness summary claim
    the chosen lane lacks proof when it does not.
    """

    selected: dict[str, Any] | None = None
    for candidate in (runtime_admission_candidate, forward_runtime_candidate, best_runtime_candidate):
        if isinstance(candidate, dict):
            selected = candidate
            break
    if not isinstance(selected, dict):
        return []
    return sorted({str(blocker) for blocker in (selected.get("runtime_copy_evidence_gate_blockers") or [])})


def _runtime_candidate_has_live_admission_truth(candidate: dict[str, Any] | None) -> bool:
    """Return whether a runtime-selected candidate can replace a pre-runtime pass.

    Runtime selection exists to attach current-poll CLOB truth to a ranked paper
    candidate whose only remaining blocker was missing CLOB evidence. It should
    not be treated as a shortcut: the candidate still needs live-target paper
    performance, enough distinct runtime source events/windows, and zero
    fallback/reject/miss BUY evidence.
    """

    if not isinstance(candidate, dict):
        return False
    if candidate.get("status") != PASS:
        return False
    if candidate.get("runtime_copy_evidence_gate_blockers"):
        return False
    live_target_profile = (
        candidate.get("live_target_profile")
        if isinstance(candidate.get("live_target_profile"), dict)
        else {}
    )
    runtime_resolvable_blockers = _runtime_resolvable_blockers_for_live_target(
        [str(blocker) for blocker in candidate.get("blockers") or []],
        live_target_profile,
    )
    structural_blockers = [
        str(blocker)
        for blocker in candidate.get("blockers") or []
        if str(blocker) not in runtime_resolvable_blockers
    ]
    if structural_blockers:
        return False
    if live_target_profile.get("status") != PASS:
        return False
    evidence = (
        candidate.get("runtime_copy_evidence")
        if isinstance(candidate.get("runtime_copy_evidence"), dict)
        else {}
    )
    required = int(evidence.get("required_buy_copy_events") or 0)
    clob_filled = int(evidence.get("clob_filled_buy_copy_events") or 0)
    if required <= 0 or clob_filled < required:
        return False
    dirty = (
        int(evidence.get("fallback_filled_buy_copy_events") or 0),
        int(evidence.get("rejected_buy_copy_events") or 0),
        int(evidence.get("missed_buy_copy_events") or 0),
    )
    if any(count > 0 for count in dirty):
        return False
    evidence_source = str(evidence.get("source") or "")
    if evidence.get("runtime_candidate_persisted_proof_used_without_current_tracker_pass"):
        return False
    min_events = int(evidence.get("min_required_buy_copy_events") or 0)
    min_windows = int(evidence.get("min_required_market_windows") or 0)
    if evidence_source == "candidate_runtime_proof_index":
        min_events = max(min_events, 10)
        min_windows = max(min_windows, 3)
    distinct_events = int(evidence.get("runtime_candidate_distinct_source_events") or 0)
    distinct_windows = int(evidence.get("runtime_candidate_distinct_market_windows") or 0)
    if min_events > 0 and distinct_events < min_events:
        return False
    if min_windows > 0 and distinct_windows < min_windows:
        return False
    return True


def _runtime_selection_depends_on_forward_probe(candidate: dict[str, Any] | None) -> bool:
    """Return whether a forward-queue runtime candidate still needs live truth.

    A forward-queue probe is diagnostic until its candidate-scoped CLOB proof is
    also present in the runtime tracker proof index. That bridge keeps a thin
    one-off probe blocked, while allowing the first-live single-wallet lane to
    use strict persisted current-poll CopyIntent proof when it has enough BUYs
    and market windows.
    """

    if not isinstance(candidate, dict):
        return True
    evidence = candidate.get("runtime_copy_evidence")
    if not isinstance(evidence, dict):
        return True
    source = str(evidence.get("source") or "")
    forward_probe_source = source.startswith("forward_queue_rank_") or source in {"candidate_forward", "forward_candidate"}
    if not forward_probe_source:
        return False
    if evidence.get("runtime_candidate_persisted_proof_used_without_current_tracker_pass"):
        return True
    if not _runtime_candidate_has_live_admission_truth(candidate):
        return True
    required_events = int(evidence.get("min_required_buy_copy_events") or 0)
    required_windows = int(evidence.get("min_required_market_windows") or 0)
    if int(evidence.get("runtime_candidate_distinct_source_events") or 0) < required_events:
        return True
    if int(evidence.get("runtime_candidate_distinct_market_windows") or 0) < required_windows:
        return True
    if source.startswith("forward_queue_rank_"):
        current_truth_events = int(
            evidence.get("runtime_current_truth_distinct_source_events") or 0
        )
        current_truth_windows = int(
            evidence.get("runtime_current_truth_distinct_market_windows") or 0
        )
        if (
            evidence.get("truth_status") == PASS
            and current_truth_events >= required_events
            and current_truth_windows >= required_windows
        ):
            return False
        proof_sources = {str(item) for item in (evidence.get("runtime_proof_index_sources") or [])}
        if "runtime_tracker_state" not in proof_sources:
            return True
    return False


def _runtime_admission_candidates(candidates: Iterable[dict[str, Any] | None]) -> list[dict[str, Any]]:
    """Return single-wallet runtime candidates that satisfy the first-live contract."""

    selected: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if candidate.get("candidate_type") != "SINGLE_WALLET":
            continue
        if _runtime_candidate_has_live_admission_truth(candidate):
            selected.append(candidate)
    return sorted(selected, key=_runtime_candidate_admission_sort_key, reverse=True)


def _runtime_candidate_admission_sort_key(candidate: dict[str, Any] | None) -> tuple[float, int, int, int, float]:
    if not isinstance(candidate, dict):
        return (0, 0, 0, 0, 0.0)
    evidence = candidate.get("runtime_copy_evidence")
    evidence = evidence if isinstance(evidence, dict) else {}
    event_age = evidence.get("required_event_age_p95_s")
    try:
        age_score = -float(event_age)
    except (TypeError, ValueError):
        age_score = -999999.0
    return (
        float(_runtime_candidate_source_priority(candidate)),
        int(evidence.get("runtime_candidate_distinct_source_events") or 0),
        int(evidence.get("runtime_candidate_distinct_market_windows") or 0),
        int(evidence.get("runtime_proof_index_rows") or 0),
        age_score,
    )


def _sprint_runtime_candidate_from_proof(
    proof: dict[str, Any],
    *,
    policy_by_id: dict[str, CandidatePolicy],
    approval_id: str,
    min_required_events: int,
    min_required_windows: int,
) -> dict[str, Any] | None:
    """Build an operator-approved live-today candidate from strict runtime proof.

    This is intentionally available only behind an explicit sprint approval id.
    It does not create orders or bypass CopyIntent parity; it converts already
    persisted CLOB-backed runtime proof into the same runtime admission shape the
    guarded live executor consumes.
    """

    candidate_id = str(proof.get("candidate_id") or "")
    candidate_key = str(proof.get("candidate_key") or "")
    policy_id = str(proof.get("policy_id") or "")
    source_wallet = str(proof.get("source_wallet") or "").lower()
    source_events = int(proof.get("source_events") or 0)
    market_windows = int(proof.get("market_windows") or 0)
    if not (candidate_id and policy_id and source_wallet):
        return None
    if source_events < int(min_required_events) or market_windows < int(min_required_windows):
        return None
    policy = policy_by_id.get(policy_id)
    if policy is None:
        return None
    evidence = {
        "source": "candidate_runtime_proof_index",
        "truth_status": PASS,
        "candidate_id": candidate_id,
        "candidate_key": candidate_key,
        "candidate_policy_id": policy_id,
        "candidate_source_wallet": source_wallet,
        "required_buy_copy_events": source_events,
        "clob_filled_buy_copy_events": source_events,
        "fallback_filled_buy_copy_events": 0,
        "rejected_buy_copy_events": 0,
        "missed_buy_copy_events": 0,
        "required_event_age_p95_s": proof.get("event_age_p95_s"),
        "min_required_buy_copy_events": int(min_required_events),
        "min_required_market_windows": int(min_required_windows),
        "runtime_candidate_distinct_source_events": source_events,
        "runtime_candidate_distinct_market_windows": market_windows,
        "runtime_current_truth_rows": source_events,
        "runtime_current_truth_distinct_source_events": source_events,
        "runtime_current_truth_distinct_market_windows": market_windows,
        "runtime_proof_index_rows": int(proof.get("proof_rows") or source_events),
        "runtime_proof_index_sources": proof.get("sources") or ["candidate_runtime_proof_index"],
        "runtime_proof_total_rows": int(proof.get("proof_rows") or source_events),
        "operator_approval_id": approval_id,
    }
    return {
        "candidate_id": candidate_id,
        "candidate_key": candidate_key,
        "candidate_type": "SINGLE_WALLET",
        "status": PASS,
        "blockers": [],
        "policy": policy.asdict(),
        "metadata": {
            "source_wallet": source_wallet,
            "wallet_name": source_wallet,
            "proof_led_runtime_candidate": True,
            "candidate_key": candidate_key,
            "live_today_sprint_operator_approval_id": approval_id,
        },
        "runtime_copy_evidence": evidence,
        "summary": {
            "runtime_proof_source_events": source_events,
            "runtime_proof_market_windows": market_windows,
        },
        "validation_summary": {},
        "live_target_profile": {
            "status": PASS,
            "blockers": [],
            "operator_approval_id": approval_id,
            "note": "LIVE_TODAY_SPRINT: historical replay gaps do not block strict runtime CLOB proof admission.",
        },
        "profit_score": float(source_events),
    }


def _runtime_candidate_source_priority(candidate: dict[str, Any] | None) -> int:
    if not isinstance(candidate, dict):
        return 0
    evidence = candidate.get("runtime_copy_evidence")
    source = str(evidence.get("source") or "") if isinstance(evidence, dict) else ""
    if source == "candidate_runtime_proof_index":
        return 100
    if source == "best_candidate":
        return 90
    if source.startswith("forward_queue_rank_"):
        return 10
    if source in {"candidate_forward", "forward_candidate"}:
        return 20
    return 50


def _live_candidate_relevant_limited_counts(
    candidate: dict[str, Any] | None,
    skipped_candidate_counts: dict[str, int],
) -> dict[str, int]:
    counts = {
        key: int(value or 0)
        for key, value in skipped_candidate_counts.items()
        if str(key).endswith("_limited")
    }
    if not isinstance(candidate, dict):
        return counts
    candidate_type = str(candidate.get("candidate_type") or "")
    if candidate_type == "SINGLE_WALLET":
        return {
            key: value
            for key, value in counts.items()
            if not str(key).startswith("multi_wallet_")
        }
    return counts


def evaluate_profit_candidates(
    events: list[WalletEvent],
    *,
    resolutions_path: str | Path = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl",
    config: ProfitEngineConfig | None = None,
    policies: list[CandidatePolicy] | None = None,
) -> dict[str, Any]:
    cfg = config or ProfitEngineConfig()
    candidate_policies = policies or default_candidate_policies()
    resolutions = load_resolutions(resolutions_path)
    latest_resolution_expiry = _latest_resolution_expiry(resolutions)
    resolution_contract = _resolution_contract(resolutions_path, indexed_count=len(resolutions))
    events = sorted(unique_wallet_events(events), key=lambda event: (event.event_ts or 0.0, event.event_id))
    candidates: list[dict[str, Any]] = []
    raw_baseline_cache: dict[tuple[str, float, float, float, float], dict[str, Any]] = {}
    wallet_groups, wallet_search_summary = _select_wallet_groups(events, max_wallets=cfg.max_wallets_for_search)
    searched_events = [event for _wallet, wallet_events in wallet_groups for event in wallet_events]
    skipped_candidate_counts = {
        "single_wallet_below_min_intent_count": 0,
        "single_wallet_intents_limited": 0,
        "multi_wallet_base_intents_limited": 0,
    }

    for wallet, wallet_events in sorted(wallet_groups):
        wallet_name = wallet_events[0].wallet_name if wallet_events else wallet
        for policy in candidate_policies:
            accepted_events = _accepted_events_for_policy(wallet_events, policy)
            if not accepted_events:
                continue
            if cfg.skip_candidates_below_min_intent_count and len(accepted_events) < int(cfg.min_all_resolved):
                skipped_candidate_counts["single_wallet_below_min_intent_count"] += 1
                continue
            intents = _intents_from_accepted_events(accepted_events, policy)
            if not intents:
                continue
            original_intent_count = len(intents)
            if cfg.skip_candidates_below_min_intent_count and original_intent_count < int(cfg.min_all_resolved):
                skipped_candidate_counts["single_wallet_below_min_intent_count"] += 1
                continue
            if (
                cfg.max_single_wallet_candidate_intents > 0
                and original_intent_count > int(cfg.max_single_wallet_candidate_intents)
            ):
                intents = intents[-int(cfg.max_single_wallet_candidate_intents) :]
                skipped_candidate_counts["single_wallet_intents_limited"] += 1
            raw_cache_key = (
                wallet,
                float(policy.wallet_fraction),
                float(policy.max_order_usd),
                float(policy.min_order_usd),
                float(cfg.slippage_bps),
            )
            raw_summary = raw_baseline_cache.get(raw_cache_key)
            if raw_summary is None:
                raw_summary = _raw_baseline_summary(wallet_events, policy=policy, resolutions=resolutions, cfg=cfg)
                raw_baseline_cache[raw_cache_key] = raw_summary
            candidates.append(
                evaluate_candidate(
                    candidate_type="SINGLE_WALLET",
                    policy=policy,
                    intents=intents,
                    resolutions=resolutions,
                    cfg=cfg,
                    raw_baseline_summary=raw_summary,
                    latest_resolution_expiry=latest_resolution_expiry,
                    metadata={
                        "source_wallet": wallet,
                        "wallet_name": wallet_name,
                        "intent_count": len(intents),
                        "original_intent_count": original_intent_count,
                        "intents_limited": len(intents) < original_intent_count,
                        "max_single_wallet_candidate_intents": cfg.max_single_wallet_candidate_intents,
                    },
                )
            )

    if len(wallet_groups) >= int(cfg.min_agreeing_wallets):
        for policy in candidate_policies:
            accepted_events = _accepted_events_for_policy(searched_events, policy)
            if not accepted_events:
                continue
            intent_source_events, original_base_event_count, base_events_limited = _recent_events_for_intent_build(
                accepted_events,
                max_events=int(cfg.max_multi_wallet_base_intents),
            )
            base_intents = _intents_from_accepted_events(intent_source_events, policy)
            if not base_intents:
                continue
            original_base_intent_count = len(base_intents)
            if base_events_limited:
                original_base_intent_count = max(original_base_intent_count, original_base_event_count)
                skipped_candidate_counts["multi_wallet_base_intents_limited"] += 1
            elif cfg.max_multi_wallet_base_intents > 0 and original_base_intent_count > int(cfg.max_multi_wallet_base_intents):
                base_intents = base_intents[-int(cfg.max_multi_wallet_base_intents) :]
                skipped_candidate_counts["multi_wallet_base_intents_limited"] += 1
            multi_metadata = {
                "base_intents": len(base_intents),
                "original_base_intents": original_base_intent_count,
                "base_events": len(intent_source_events),
                "original_base_events": original_base_event_count,
                "base_intents_limited": len(base_intents) < original_base_intent_count,
                "base_events_limited_before_intent_build": base_events_limited,
                "intent_build_strategy": (
                    "recent_event_slice_before_copyintent_generation"
                    if base_events_limited
                    else "full_accepted_event_surface"
                ),
                "max_multi_wallet_base_intents": cfg.max_multi_wallet_base_intents,
            }
            if cfg.enable_consensus_search:
                consensus_signals = build_consensus_signals(
                    base_intents,
                    config=ConsensusConfig(
                        min_agreeing_wallets=cfg.min_agreeing_wallets,
                        max_price_spread=cfg.max_price_spread,
                        max_consensus_usd=policy.max_order_usd,
                    ),
                )
                consensus_intents = [
                    intent
                    for intent in (consensus_signal_to_intent(signal) for signal in consensus_signals)
                    if intent is not None
                ]
                if consensus_intents:
                    candidates.append(
                        evaluate_candidate(
                            candidate_type="MULTI_WALLET_CONSENSUS",
                            policy=policy,
                            intents=consensus_intents,
                            resolutions=resolutions,
                            cfg=cfg,
                            latest_resolution_expiry=latest_resolution_expiry,
                            metadata={**multi_metadata, "signals": len(consensus_signals)},
                        )
                    )
            if cfg.enable_inventory_search:
                inventory_plans = build_inventory_plans(
                    base_intents,
                    config=InventoryConfig(
                        min_agreeing_wallets=cfg.min_agreeing_wallets,
                        max_price_spread=cfg.max_price_spread,
                        max_window_usd=cfg.inventory_max_window_usd,
                        max_per_wallet_usd=cfg.inventory_max_per_wallet_usd,
                        min_plan_usd=cfg.inventory_min_plan_usd,
                        policy_id=policy.policy_id,
                    ),
                )
                inventory_intents = [
                    intent
                    for plan in inventory_plans
                    for intent in inventory_plan_child_intents(plan, policy_id=policy.policy_id)
                ]
                if inventory_intents:
                    candidates.append(
                        evaluate_candidate(
                            candidate_type="MULTI_WALLET_INVENTORY",
                            policy=policy,
                            intents=inventory_intents,
                            resolutions=resolutions,
                            cfg=cfg,
                            latest_resolution_expiry=latest_resolution_expiry,
                            metadata={
                                **multi_metadata,
                                "plans": len(inventory_plans),
                                "inventory_profile": _inventory_plan_profile(inventory_plans),
                                "inventory_scoring_mode": "child_copy_intents",
                                "inventory_child_copy_intents": len(inventory_intents),
                            },
                        )
                    )

    ranked_all = sorted(
        candidates,
        key=lambda row: (
            row.get("status") != "PASS",
            (row.get("live_target_profile") or {}).get("status") != "PASS",
            0
            if row.get("candidate_type") == "MULTI_WALLET_INVENTORY"
            else (1 if row.get("candidate_type") == "MULTI_WALLET_CONSENSUS" else 2),
            -float(row.get("profit_score") or 0.0),
            -float(((row.get("summary") or {}).get("window_metrics") or {}).get("avg_orders_per_window") or 0.0),
            -float((row.get("validation_summary") or {}).get("wr_pct") or 0.0),
            -float((row.get("validation_summary") or {}).get("roi_pct") or 0.0),
            -int((row.get("summary") or {}).get("resolved_orders") or 0),
        ),
    )
    individual_wallet_universe = _individual_wallet_copy_universe(
        events=events,
        searched_events=searched_events,
        candidates=candidates,
        wallet_search_summary=wallet_search_summary,
        cfg=cfg,
    )
    multi_wallet_inventory_universe = _multi_wallet_inventory_universe(
        candidates=candidates,
        cfg=cfg,
        individual_wallet_universe=individual_wallet_universe,
    )
    ranked = ranked_all[: int(cfg.max_candidates)]
    pass_candidates = [row for row in ranked if row.get("status") == "PASS"]
    best = pass_candidates[0] if pass_candidates else (ranked[0] if ranked else None)
    development_bridge_context = _development_program_bridge_context(
        cfg.strategy_direction_state_path,
        ranked_all,
    )
    runtime_proof_index_existing_rows = _load_runtime_proof_index_rows(cfg.candidate_runtime_proof_index_path)
    requested_forward_queue_size = max(1, int(cfg.forward_tracking_queue_size))
    forward_queue_buffer_cfg = replace(
        cfg,
        forward_tracking_queue_size=max(requested_forward_queue_size, min(25, requested_forward_queue_size * 4)),
    )
    forward_tracking_queue = _forward_tracking_queue(
        ranked_all,
        cfg=forward_queue_buffer_cfg,
        proof_index_rows=runtime_proof_index_existing_rows,
        policies=candidate_policies,
    )
    forward_tracking_queue = _reconcile_proof_led_forward_queue(
        forward_tracking_queue,
        events_by_wallet=_by_wallet(events),
        resolutions=resolutions,
        cfg=cfg,
        policies=candidate_policies,
        latest_resolution_expiry=latest_resolution_expiry,
    )
    forward_tracking_queue = _apply_development_program_bridge_to_forward_queue(
        forward_tracking_queue,
        development_bridge_context,
    )
    runtime_forward_tracking_queue = _rerank_reconciled_forward_queue(forward_tracking_queue)
    forward_tracking_queue = runtime_forward_tracking_queue[:requested_forward_queue_size]
    for index, row in enumerate(forward_tracking_queue):
        row["forward_queue_rank"] = index
    forward_candidate_entry = forward_tracking_queue[0] if forward_tracking_queue else {}
    forward_candidate = _candidate_from_queue_entry(ranked_all, forward_candidate_entry)
    best_policy = best.get("policy") if isinstance(best, dict) and isinstance(best.get("policy"), dict) else {}
    best_metadata = best.get("metadata") if isinstance(best, dict) and isinstance(best.get("metadata"), dict) else {}
    best_candidate_type = str(best.get("candidate_type") or "") if isinstance(best, dict) else ""
    candidate_policy_id = str(best_policy.get("policy_id") or "") if best_policy else None
    forward_policy = (
        forward_candidate.get("policy")
        if isinstance(forward_candidate, dict) and isinstance(forward_candidate.get("policy"), dict)
        else {}
    )
    forward_metadata = (
        forward_candidate.get("metadata")
        if isinstance(forward_candidate, dict) and isinstance(forward_candidate.get("metadata"), dict)
        else {}
    )
    forward_candidate_type = str(forward_candidate.get("candidate_type") or "") if isinstance(forward_candidate, dict) else ""
    forward_candidate_policy_id = str(forward_policy.get("policy_id") or "") if forward_policy else None
    stale_best_candidate_demoted_for_forward_tracking = bool(
        isinstance(best, dict)
        and isinstance(forward_candidate, dict)
        and best.get("candidate_id") != forward_candidate.get("candidate_id")
    )
    live_truth = _live_tracker_truth(
        cfg.live_tracker_state_path,
        require_copy_efficiency_truth=cfg.require_copy_efficiency_truth,
        candidate_policy_id=candidate_policy_id or None,
        candidate_type=best_candidate_type or None,
        candidate_metadata={
            **best_metadata,
            "candidate_id": best.get("candidate_id") if isinstance(best, dict) else None,
            "candidate_policy": best_policy,
        },
    )
    active_hotlane_live_truth = _live_tracker_truth(
        cfg.active_hotlane_live_tracker_state_path,
        require_copy_efficiency_truth=cfg.require_copy_efficiency_truth,
        candidate_policy_id=None,
        candidate_type=None,
        candidate_metadata=None,
    )
    candidate_forward_tracker_paths = _candidate_forward_tracker_paths(cfg)
    runtime_tracker_paths = list(
        dict.fromkeys([str(cfg.active_hotlane_live_tracker_state_path), *candidate_forward_tracker_paths])
    )
    candidate_forward_live_truth, candidate_forward_live_truths = _best_candidate_tracker_truth(
        candidate_forward_tracker_paths,
        require_copy_efficiency_truth=cfg.require_copy_efficiency_truth,
        candidate_policy_id=candidate_policy_id or None,
        candidate_type=best_candidate_type or None,
        candidate_metadata={
            **best_metadata,
            "candidate_id": best.get("candidate_id") if isinstance(best, dict) else None,
            "candidate_policy": best_policy,
        },
    )
    forward_candidate_live_truth, forward_candidate_live_truths = _best_candidate_tracker_truth(
        runtime_tracker_paths,
        require_copy_efficiency_truth=cfg.require_copy_efficiency_truth,
        candidate_policy_id=forward_candidate_policy_id or None,
        candidate_type=forward_candidate_type or None,
        candidate_metadata={
            **forward_metadata,
            "candidate_id": forward_candidate.get("candidate_id") if isinstance(forward_candidate, dict) else None,
            "candidate_policy": forward_policy,
        },
    )
    runtime_proof_index_new_rows: list[dict[str, Any]] = []
    runtime_proof_index_new_rows.extend(
        _runtime_proof_rows_from_truth(best, candidate_forward_live_truth, source="candidate_forward")
    )
    runtime_proof_index_new_rows.extend(
        _runtime_proof_rows_from_truth(forward_candidate, forward_candidate_live_truth, source="forward_candidate")
    )
    for tracker_path in runtime_tracker_paths:
        runtime_proof_index_new_rows.extend(
            _runtime_proof_rows_from_tracker_state(
                tracker_path,
                source="runtime_tracker_state",
            )
        )
    best_runtime_candidate = _candidate_with_runtime_copy_evidence(
        best,
        candidate_forward_live_truth,
        source="candidate_forward",
        min_required_buy_copy_events=cfg.min_runtime_candidate_required_buy_copy_events,
        min_required_market_windows=cfg.min_runtime_candidate_required_market_windows,
        proof_index_rows=runtime_proof_index_existing_rows,
    )
    forward_runtime_candidate = _candidate_with_runtime_copy_evidence(
        forward_candidate,
        forward_candidate_live_truth,
        source="forward_candidate",
        min_required_buy_copy_events=cfg.min_runtime_candidate_required_buy_copy_events,
        min_required_market_windows=cfg.min_runtime_candidate_required_market_windows,
        proof_index_rows=runtime_proof_index_existing_rows,
    )
    forward_queue_runtime_candidates: list[dict[str, Any]] = []
    for queue_rank, queue_entry in enumerate(runtime_forward_tracking_queue):
        queue_candidate = _candidate_from_queue_entry(ranked_all, queue_entry)
        if not isinstance(queue_candidate, dict):
            continue
        queue_policy = (
            queue_candidate.get("policy")
            if isinstance(queue_candidate.get("policy"), dict)
            else {}
        )
        queue_metadata = (
            queue_candidate.get("metadata")
            if isinstance(queue_candidate.get("metadata"), dict)
            else {}
        )
        queue_truth, _queue_truths = _best_candidate_tracker_truth(
            runtime_tracker_paths,
            require_copy_efficiency_truth=cfg.require_copy_efficiency_truth,
            candidate_policy_id=str(queue_policy.get("policy_id") or "") or None,
            candidate_type=str(queue_candidate.get("candidate_type") or "") or None,
            candidate_metadata={
                **queue_metadata,
                "candidate_id": queue_candidate.get("candidate_id"),
                "candidate_policy": queue_policy,
            },
        )
        runtime_proof_index_new_rows.extend(
            _runtime_proof_rows_from_truth(
                queue_candidate,
                queue_truth,
                source=f"forward_queue_rank_{queue_rank}",
            )
        )
        queue_runtime_candidate = _candidate_with_runtime_copy_evidence(
            queue_candidate,
            queue_truth,
            source=f"forward_queue_rank_{queue_rank}",
            min_required_buy_copy_events=cfg.min_runtime_candidate_required_buy_copy_events,
            min_required_market_windows=cfg.min_runtime_candidate_required_market_windows,
            proof_index_rows=runtime_proof_index_existing_rows,
        )
        if isinstance(queue_runtime_candidate, dict):
            queue_runtime_candidate["forward_queue_rank"] = queue_rank
            queue_runtime_candidate["source_wallet"] = (
                queue_entry.get("source_wallet") or _candidate_source_wallet(queue_candidate)
            )
            if queue_entry.get("wallet_name"):
                queue_runtime_candidate["wallet_name"] = queue_entry.get("wallet_name")
            queue_runtime_candidate["forward_queue_tracker_truth"] = queue_truth
            if queue_entry.get("development_program_bridge_focus") is True:
                queue_runtime_candidate["development_program_bridge_focus"] = True
                queue_runtime_candidate["development_program_next_major_change_action"] = queue_entry.get(
                    "development_program_next_major_change_action"
                )
                metadata = (
                    dict(queue_runtime_candidate.get("metadata"))
                    if isinstance(queue_runtime_candidate.get("metadata"), dict)
                    else {}
                )
                metadata["development_program_bridge_focus"] = True
                metadata["development_program_bridge_roles"] = queue_entry.get(
                    "development_program_bridge_roles"
                ) or []
                metadata["development_program_target_inventory"] = queue_entry.get(
                    "development_program_target_inventory"
                ) or {}
                queue_runtime_candidate["metadata"] = metadata
            forward_queue_runtime_candidates.append(queue_runtime_candidate)
    runtime_proof_index_candidate_rows = _merge_runtime_proof_rows(
        runtime_proof_index_existing_rows,
        runtime_proof_index_new_rows,
    )
    ranked_runtime_candidates: list[dict[str, Any]] = []
    for ranked_candidate in ranked_all:
        ranked_runtime_candidate = _candidate_with_runtime_copy_evidence(
            ranked_candidate,
            {},
            source="candidate_runtime_proof_index",
            min_required_buy_copy_events=cfg.min_runtime_candidate_required_buy_copy_events,
            min_required_market_windows=cfg.min_runtime_candidate_required_market_windows,
            proof_index_rows=runtime_proof_index_candidate_rows,
        )
        if not isinstance(ranked_runtime_candidate, dict):
            continue
        runtime_evidence = (
            ranked_runtime_candidate.get("runtime_copy_evidence")
            if isinstance(ranked_runtime_candidate.get("runtime_copy_evidence"), dict)
            else {}
        )
        if int(runtime_evidence.get("runtime_proof_index_rows") or 0) <= 0:
            continue
        ranked_runtime_candidate["runtime_proof_ranked_candidate"] = True
        ranked_runtime_candidates.append(ranked_runtime_candidate)
    runtime_proof_index_rows = _merge_runtime_proof_rows(
        runtime_proof_index_existing_rows,
        runtime_proof_index_new_rows,
    )
    runtime_proof_index_state = _write_runtime_proof_index(
        cfg.candidate_runtime_proof_index_path,
        runtime_proof_index_rows,
    )
    sprint_runtime_candidates: list[dict[str, Any]] = []
    if cfg.live_today_sprint_operator_approval_id:
        policy_by_id = {policy.policy_id: policy for policy in candidate_policies}
        for proof in _runtime_proof_candidate_summaries(runtime_proof_index_rows):
            candidate = _sprint_runtime_candidate_from_proof(
                proof,
                policy_by_id=policy_by_id,
                approval_id=cfg.live_today_sprint_operator_approval_id,
                min_required_events=max(int(cfg.min_runtime_candidate_required_buy_copy_events), 10),
                min_required_windows=max(int(cfg.min_runtime_candidate_required_market_windows), 3),
            )
            if isinstance(candidate, dict):
                sprint_runtime_candidates.append(candidate)
    runtime_candidates_by_id: dict[str, dict[str, Any]] = {}
    for candidate in (
        best_runtime_candidate,
        forward_runtime_candidate,
        *forward_queue_runtime_candidates,
        *ranked_runtime_candidates,
        *sprint_runtime_candidates,
    ):
        if not isinstance(candidate, dict):
            continue
        candidate_id = str(candidate.get("candidate_id") or "")
        existing_candidate = runtime_candidates_by_id.get(candidate_id) if candidate_id else None
        candidate_admissible = _runtime_candidate_has_live_admission_truth(candidate)
        existing_admissible = _runtime_candidate_has_live_admission_truth(existing_candidate)
        if candidate_id and (
            not isinstance(existing_candidate, dict)
            or (candidate_admissible and not existing_admissible)
            or (
                candidate_admissible == existing_admissible
                and _runtime_candidate_source_priority(candidate)
                > _runtime_candidate_source_priority(existing_candidate)
            )
        ):
            runtime_candidates_by_id[candidate_id] = candidate
    runtime_pass_candidates = _runtime_admission_candidates(runtime_candidates_by_id.values())
    runtime_admission_candidate = runtime_pass_candidates[0] if runtime_pass_candidates else None
    effective_live_truth = live_truth
    live_tracker_truth_source = "canonical"
    if live_truth.get("status") != "PASS" and candidate_forward_live_truth.get("status") == "PASS":
        effective_live_truth = candidate_forward_live_truth
        live_tracker_truth_source = "candidate_forward"
    elif live_truth.get("status") != "PASS" and forward_candidate_live_truth.get("status") == "PASS":
        effective_live_truth = forward_candidate_live_truth
        live_tracker_truth_source = "forward_candidate"
    if isinstance(runtime_admission_candidate, dict):
        runtime_truth = runtime_admission_candidate.get("forward_queue_tracker_truth")
        if isinstance(runtime_truth, dict) and runtime_truth.get("status") == "PASS":
            effective_live_truth = runtime_truth
            live_tracker_truth_source = str(
                (runtime_admission_candidate.get("runtime_copy_evidence") or {}).get("source")
                or "forward_queue_runtime_candidate"
            )
    live_admission_blockers: list[str] = []
    research_mode = (
        not cfg.require_live_tracker_truth_for_live_admission
        or not cfg.require_copy_efficiency_truth
        or not cfg.require_candidate_clob_fill_evidence
    )
    if research_mode:
        live_admission_blockers.append("research_mode_not_live_admissible")
    best_resolution_evidence = (
        best.get("resolution_evidence_summary")
        if isinstance(best, dict) and isinstance(best.get("resolution_evidence_summary"), dict)
        else {}
    )
    best_canonical_resolution_profile = (
        best.get("canonical_resolution_profile")
        if isinstance(best, dict) and isinstance(best.get("canonical_resolution_profile"), dict)
        else {}
    )
    if (
        int(best_resolution_evidence.get("research_only_resolved_orders") or 0) > 0
        and best_canonical_resolution_profile.get("status") != PASS
    ):
        live_admission_blockers.append("research_only_resolution_not_live_admissible")
    runtime_selection_used = bool(
        cfg.allow_forward_runtime_candidate_selection
        and not research_mode
        and not pass_candidates
        and isinstance(runtime_admission_candidate, dict)
    )
    runtime_selection_has_live_truth = bool(
        runtime_selection_used and _runtime_candidate_has_live_admission_truth(runtime_admission_candidate)
    )
    runtime_selection_source = ""
    if runtime_selection_used and isinstance(runtime_admission_candidate, dict):
        runtime_selection_evidence = runtime_admission_candidate.get("runtime_copy_evidence")
        if isinstance(runtime_selection_evidence, dict):
            runtime_selection_source = str(runtime_selection_evidence.get("source") or "")
    if (
        cfg.require_live_tracker_truth_for_live_admission
        and effective_live_truth.get("status") != "PASS"
        and not runtime_selection_has_live_truth
    ):
        live_admission_blockers.append("live_tracker_truth_not_pass")
    if not pass_candidates and not runtime_selection_used:
        live_admission_blockers.append("no_profit_candidate_pass")
    if runtime_selection_used and (
        not runtime_selection_has_live_truth
        or _runtime_selection_depends_on_forward_probe(runtime_admission_candidate)
    ):
        live_admission_blockers.append("profit_candidate_pass_depends_on_forward_runtime_clob_probe")
    runtime_gate_blockers = _live_admission_runtime_gate_blockers(
        runtime_admission_candidate=runtime_admission_candidate,
        forward_runtime_candidate=forward_runtime_candidate,
        best_runtime_candidate=best_runtime_candidate,
    )
    if runtime_gate_blockers:
        live_admission_blockers.extend(runtime_gate_blockers)
    live_candidate_for_bounded_search = (
        runtime_admission_candidate
        if isinstance(runtime_admission_candidate, dict)
        else (pass_candidates[0] if pass_candidates else best)
    )
    bounded_or_limited_candidate_counts = _live_candidate_relevant_limited_counts(
        live_candidate_for_bounded_search,
        skipped_candidate_counts,
    )
    bounded_search_applied = bool(
        wallet_search_summary.get("skipped_wallets")
        or any(value > 0 for value in bounded_or_limited_candidate_counts.values())
        or (pass_candidates and (best_metadata.get("intents_limited") or best_metadata.get("base_intents_limited")))
    )
    if bounded_search_applied:
        if not (
            cfg.live_today_sprint_operator_approval_id
            and runtime_selection_has_live_truth
            and runtime_selection_source == "candidate_runtime_proof_index"
        ):
            live_admission_blockers.append("fast_or_bounded_search_not_live_admissible")
    candidate_selection_status = PASS if pass_candidates or runtime_selection_used else ANALYZE
    live_admission_status = (
        PASS if not live_admission_blockers else active_status_from_blockers(live_admission_blockers, default=ANALYZE)
    )
    development_bridge_queue_entries = [
        row
        for row in forward_tracking_queue
        if isinstance(row, dict) and row.get("development_program_bridge_focus") is True
    ]
    decision_blockers = list(live_admission_blockers)
    if candidate_selection_status != PASS:
        decision_blockers.append("no_candidate_passed_walk_forward_admission")
    decision = {
        "status": PASS
        if candidate_selection_status == PASS and live_admission_status == PASS
        else active_status_from_blockers(decision_blockers, default=ANALYZE),
        "candidate_selection_status": candidate_selection_status,
        "reason": (
            "profitable_candidate_and_copy_efficiency_passed_live_admission"
            if candidate_selection_status == PASS and live_admission_status == PASS
            else (
                "profitable_candidate_found_but_live_admission_blocked"
                if candidate_selection_status == PASS
                else "no_candidate_passed_walk_forward_admission"
            )
        ),
        "best_candidate_id": best.get("candidate_id") if isinstance(best, dict) else None,
        "best_candidate_type": best.get("candidate_type") if isinstance(best, dict) else None,
        "active_hotlane_forward_truth_status": active_hotlane_live_truth.get("status"),
        "candidate_forward_truth_status": candidate_forward_live_truth.get("status"),
        "forward_candidate_truth_status": forward_candidate_live_truth.get("status"),
        "forward_candidate_id": forward_candidate_entry.get("candidate_id") if isinstance(forward_candidate_entry, dict) else None,
        "forward_candidate_source_wallet": forward_candidate_entry.get("source_wallet")
        if isinstance(forward_candidate_entry, dict)
        else None,
        "runtime_admission_candidate_id": runtime_admission_candidate.get("candidate_id")
        if isinstance(runtime_admission_candidate, dict)
        else None,
        "runtime_admission_source": (
            (runtime_admission_candidate.get("runtime_copy_evidence") or {}).get("source")
            if isinstance(runtime_admission_candidate, dict)
            and isinstance(runtime_admission_candidate.get("runtime_copy_evidence"), dict)
            else None
        ),
        "forward_tracking_queue_size": len(forward_tracking_queue),
        "development_program_bridge_focus": bool(development_bridge_queue_entries),
        "individual_wallet_copy_universe_status": individual_wallet_universe.get("status"),
        "individual_wallet_source_buy_events": (
            individual_wallet_universe.get("coverage", {}).get("source_buy_events")
            if isinstance(individual_wallet_universe.get("coverage"), dict)
            else None
        ),
        "multi_wallet_inventory_universe_status": multi_wallet_inventory_universe.get("status"),
        "multi_wallet_inventory_candidate_order_slots": (
            multi_wallet_inventory_universe.get("coverage", {}).get("inventory_candidate_order_slots")
            if isinstance(multi_wallet_inventory_universe.get("coverage"), dict)
            else None
        ),
        "stale_best_candidate_demoted_for_forward_tracking": stale_best_candidate_demoted_for_forward_tracking,
        "live_tracker_truth_source": live_tracker_truth_source,
        "live_admission_status": live_admission_status,
        "live_admission_blockers": live_admission_blockers,
        "live_orders_allowed": False,
        "paper_only": True,
    }
    source_route_truth = _source_route_truth(cfg.source_route_state_path)
    live_readiness_certificate = _live_readiness_certificate(
        decision=decision,
        best_candidate=best,
        forward_candidate=runtime_admission_candidate or forward_runtime_candidate or forward_candidate,
        pass_candidates=pass_candidates,
        effective_live_truth=effective_live_truth,
        resolution_contract=resolution_contract,
        wallet_search_summary=wallet_search_summary,
        skipped_candidate_counts=skipped_candidate_counts,
        source_route_truth=source_route_truth,
    )
    return {
        "schema_version": 1,
        "kind": "wallet_copy_profit_engine_state",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "source_contract": {
            "scope": "BTC 5m wallet-copy replay only",
            "history_events": len(events),
            "wallet_count": len(_by_wallet(events)),
            "wallet_search_summary": wallet_search_summary,
            "searched_history_events": len(searched_events),
            "skipped_candidate_counts": skipped_candidate_counts,
            "resolutions_indexed": len(resolutions),
            "resolution_contract": resolution_contract,
            "source_route_truth": source_route_truth,
            "slippage_bps": cfg.slippage_bps,
        },
        "live_tracker_truth": live_truth,
        "active_hotlane_live_tracker_truth": active_hotlane_live_truth,
        "candidate_forward_live_tracker_truth": candidate_forward_live_truth,
        "candidate_forward_live_tracker_truths": candidate_forward_live_truths,
        "forward_candidate_live_tracker_truth": forward_candidate_live_truth,
        "forward_candidate_live_tracker_truths": forward_candidate_live_truths,
        "effective_live_tracker_truth": effective_live_truth,
        "candidate_runtime_proof_index": {
            "path": cfg.candidate_runtime_proof_index_path,
            "candidate_runtime_proof_index_path": cfg.candidate_runtime_proof_index_path,
            "existing_rows": len(runtime_proof_index_existing_rows),
            "loaded_runtime_proof_rows": len(runtime_proof_index_existing_rows),
            "new_rows": len(runtime_proof_index_new_rows),
            "total_rows": len(runtime_proof_index_rows),
            "persisted_runtime_proof_rows": len(runtime_proof_index_rows),
            "top_runtime_proof_candidates": _runtime_proof_candidate_summaries(runtime_proof_index_rows)[:20],
            "summary": runtime_proof_index_state.get("summary"),
        },
        "development_program_bridge": {
            **development_bridge_context,
            "forward_queue_bridge_entries": len(development_bridge_queue_entries),
            "forward_queue_bridge_wallets": [
                str(row.get("source_wallet") or "").lower()
                for row in development_bridge_queue_entries
                if str(row.get("source_wallet") or "").strip()
            ],
        },
        "individual_wallet_copy_universe": individual_wallet_universe,
        "multi_wallet_inventory_universe": multi_wallet_inventory_universe,
        "best_runtime_candidate": best_runtime_candidate,
        "forward_runtime_candidate": forward_runtime_candidate,
        "forward_queue_runtime_candidates": forward_queue_runtime_candidates,
        "runtime_admission_candidate": runtime_admission_candidate,
        "config": cfg.asdict(),
        "decision": decision,
        "live_readiness_certificate": live_readiness_certificate,
        "best_candidate": best,
        "forward_candidate": forward_candidate_entry,
        "forward_tracking_queue": forward_tracking_queue,
        "pass_candidates": pass_candidates[:20],
        "ranked_candidates": ranked,
    }


def run_profit_engine(
    *,
    history_states: list[str | Path],
    resolutions_path: str | Path,
    output_path: str | Path,
    config: ProfitEngineConfig | None = None,
    policies: list[CandidatePolicy] | None = None,
    policy_preset: str | None = None,
) -> dict[str, Any]:
    events = load_events_from_histories(history_states)
    report = evaluate_profit_candidates(events, resolutions_path=resolutions_path, config=config, policies=policies)
    if policy_preset:
        source_contract = report.get("source_contract") if isinstance(report.get("source_contract"), dict) else {}
        source_contract["policy_preset"] = policy_preset
        report["source_contract"] = source_contract
        report.setdefault("config", {})["policy_preset"] = policy_preset
    atomic_write_json(output_path, report, compact=True)
    return report
