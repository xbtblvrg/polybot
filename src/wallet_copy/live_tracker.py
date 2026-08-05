"""Low-latency multi-source tracking for BTC 5m wallet-copy.

The tracker separates evidence levels:

- wallet Data API rows are wallet-attributed truth,
- CLOB books/market streams are fast execution context but not wallet-attributed,
- onchain receipts are settlement evidence and usually arrive later.

It never submits live orders. It turns confirmed wallet-attributed moves into
paper CopyIntents and records every lifecycle action.
"""

from __future__ import annotations

import json
import hashlib
import os
import sys
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

from src.wallet_copy.copyability import CopyabilityDecision, CopyabilityPolicy, score_copyability
from src.wallet_copy.copy_efficiency import build_copy_efficiency_report, build_copy_efficiency_report_from_scores
from src.wallet_copy.copy_tactics import build_copy_execution_corrections, build_copy_execution_tactic_plan
from src.wallet_copy.fill_model import FillModelConfig, estimate_executable_fill
from src.wallet_copy.http_client import PolymarketHttpClient, PolymarketRouteError
from src.wallet_copy.adaptive_bot import (
    AdaptiveBotConfig,
    build_adaptive_signals,
    build_runtime_inventory_intents,
    build_single_wallet_exact_copy_intents,
    build_tracker_time_adaptive_signals,
    signal_to_intent,
)
from src.wallet_copy.ingest import (
    WalletHistoryClient,
    normalize_polymarket_wallet_row,
    parse_ts,
    wallet_identity_status,
)
from src.wallet_copy.mission import WALLET_COPY_MISSION_CONTRACT
from src.wallet_copy.models import CopyIntent, SizingPolicy, WalletEvent, WalletSpec, num, stable_id, utc_now_iso
from src.wallet_copy.paper import PaperExecutionConfig, PaperWalletCopyEngine
from src.wallet_copy.performance import load_resolutions
from src.wallet_copy.profit_engine import CandidatePolicy, policy_accepts_event
from src.wallet_copy.registry import load_wallet_registry
from src.wallet_copy.status import ANALYZE, PASS, active_status_from_blockers
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json
from src.wallet_copy.strategy import CopyPolicy, event_to_intent
from src.wallet_copy.tactic_performance import score_tactic_replay_pnl


MIRROR_CLASSIFIER_VERSION = 2
MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS = 10


class EmptyTrackerPollRejected(RuntimeError):
    """Raised when an empty poll is quarantined instead of clobbering evidence."""

    def __init__(self, quarantine_path: str):
        super().__init__("empty tracker poll rejected; substantive canonical state preserved")
        self.quarantine_path = quarantine_path


def _tracker_writer_provenance() -> dict[str, Any]:
    return {
        "argv": list(sys.argv),
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "cwd": os.getcwd(),
    }


def _tracker_wallet_count(state: dict[str, Any]) -> int:
    summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    reports = summary.get("wallet_reports") if isinstance(summary.get("wallet_reports"), list) else []
    return max(int(summary.get("wallets") or 0), len(reports))


def _tracker_rolling_move_count(state: dict[str, Any]) -> int:
    rows = state.get("admission_evidence_window")
    if not isinstance(rows, list):
        rows = state.get("last_moves") if isinstance(state.get("last_moves"), list) else []
    return len(rows)


def _is_canonical_tracker_state_path(state_path: str) -> bool:
    try:
        return Path(state_path).resolve() == Path("data/research/wallet_copy_live_tracking_state.json").resolve()
    except OSError:
        return False


def _write_tracker_state_with_non_regression(state_path: str, state: dict[str, Any]) -> None:
    """Persist tracker truth without allowing an empty poll to erase evidence."""
    state["writer_provenance"] = _tracker_writer_provenance()
    summary = state.setdefault("summary", {})
    wallet_count = _tracker_wallet_count(state)
    rolling_moves = _tracker_rolling_move_count(state)
    canonical_tracker_state = _is_canonical_tracker_state_path(state_path)
    non_substantive_tiny_poll = canonical_tracker_state and wallet_count <= 1 and rolling_moves == 0
    if wallet_count == 0 or non_substantive_tiny_poll:
        summary["mirror_coverage_status"] = "FAIL"
        summary["mirror_coverage_reason"] = "empty_poll" if wallet_count == 0 else "non_substantive_tiny_poll"
        copy_efficiency = summary.setdefault("copy_efficiency", {})
        copy_efficiency["status"] = "FAIL"
        blockers = [str(item) for item in copy_efficiency.get("blockers") or []]
        blocker = "empty_poll" if wallet_count == 0 else "non_substantive_tiny_poll"
        if blocker not in blockers:
            blockers.append(blocker)
        copy_efficiency["blockers"] = blockers

    existing = load_json(state_path, default={})
    existing_wallet_count = _tracker_wallet_count(existing) if isinstance(existing, dict) else 0
    existing_rolling_moves = _tracker_rolling_move_count(existing) if isinstance(existing, dict) else 0
    non_regression_violation = (
        isinstance(existing, dict)
        and existing_wallet_count > 0
        and (
            (wallet_count == 0 and rolling_moves == 0)
            or (existing_rolling_moves > 0 and rolling_moves == 0 and wallet_count < existing_wallet_count)
        )
    )
    if non_regression_violation:
        canonical = Path(state_path)
        quarantine = canonical.with_name(f"{canonical.stem}.rejected_empty.json")
        reason = (
            "empty_poll_would_replace_substantive_canonical_state"
            if wallet_count == 0
            else "non_substantive_tiny_poll_would_replace_substantive_canonical_state"
        )
        atomic_write_json(
            quarantine,
            {
                "kind": "wallet_copy_live_tracker_rejected_empty_poll",
                "generated_at": utc_now_iso(),
                "paper_only": True,
                "live_orders_allowed": False,
                "reason": reason,
                "canonical_state_path": str(canonical),
                "existing_wallet_count": existing_wallet_count,
                "existing_rolling_moves": existing_rolling_moves,
                "rejected_wallet_count": wallet_count,
                "rejected_rolling_moves": rolling_moves,
                "writer_provenance": state["writer_provenance"],
                "rejected_state": state,
            },
        )
        raise EmptyTrackerPollRejected(str(quarantine))
    atomic_write_json(state_path, state)


def _get_json_with_retries(
    url: str,
    *,
    params: dict[str, Any],
    timeout_s: float,
    headers: dict[str, str],
    retries: int = 3,
    request_role: str = "live_tracker_source_request",
) -> Any:
    payload, _route_report = _get_json_with_route_report(
        url,
        params=params,
        timeout_s=timeout_s,
        headers=headers,
        retries=retries,
        request_role=request_role,
    )
    return payload


def _get_json_with_route_report(
    url: str,
    *,
    params: dict[str, Any],
    timeout_s: float,
    headers: dict[str, str],
    retries: int = 3,
    request_role: str = "live_tracker_source_request",
) -> tuple[Any, dict[str, Any]]:
    client = PolymarketHttpClient(
        timeout_s=timeout_s,
        retries=retries,
        user_agent=str(headers.get("User-Agent") or "wallet-copy-live-tracker/1.0"),
    )
    payload, route_report = client.get_json(
        url,
        params=params,
        headers=headers,
        request_role=request_role,
    )
    return payload, route_report


@dataclass(frozen=True)
class LiveTrackerConfig:
    registry_path: str = "configs/wallet_copy/wallets.json"
    state_path: str = "data/research/wallet_copy_live_tracking_state.json"
    event_log_path: str = "data/research/wallet_copy_live_tracking_events.jsonl"
    paper_state_path: str = "data/research/wallet_copy_paper_state.json"
    paper_event_log_path: str = "data/research/wallet_copy_paper_events.jsonl"
    tracker_time_replay_paper_state_path: str = (
        "data/research/wallet_copy_live_tracker_time_replay_paper_state.json"
    )
    tracker_time_replay_paper_event_log_path: str = (
        "data/research/wallet_copy_live_tracker_time_replay_paper_events.jsonl"
    )
    single_wallet_exact_copy_paper_state_path: str = (
        "data/research/wallet_copy_live_tracker_single_wallet_exact_copy_paper_state.json"
    )
    single_wallet_exact_copy_paper_event_log_path: str = (
        "data/research/wallet_copy_live_tracker_single_wallet_exact_copy_paper_events.jsonl"
    )
    all_order_exact_copy_paper_state_path: str = ""
    all_order_exact_copy_paper_event_log_path: str = ""
    all_order_tactic_replay_paper_state_path: str = ""
    all_order_tactic_replay_paper_event_log_path: str = ""
    resolutions_path: str = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
    data_api_limit: int = 100
    data_api_pages: int = 1
    data_api_timeout_s: float = 8.0
    data_api_retries: int = 3
    max_poll_runtime_s: float = 0.0
    include_activity: bool = True
    trade_query_keys: tuple[str, ...] = ("user", "proxyWallet")
    parallel_data_api_sources: bool = False
    clob_host: str = "https://clob.polymarket.com"
    gamma_host: str = "https://gamma-api.polymarket.com"
    polygon_rpc_url: str = "https://polygon-bor-rpc.publicnode.com"
    enable_clob_books: bool = True
    enable_onchain_receipts: bool = False
    clob_timeout_s: float = 5.0
    gamma_timeout_s: float = 5.0
    onchain_timeout_s: float = 8.0
    market_ws_jsonl_path: str = ""
    market_ws_lookback_s: float = 120.0
    preconfirm_match_window_s: float = 8.0
    preconfirm_price_tolerance: float = 0.01
    max_book_slippage_bps: float = 150.0
    max_copy_efficiency_latency_s: float = 10.0
    max_copy_efficiency_slippage_bps: float = 500.0
    require_clob_book_evidence_for_efficiency: bool = True
    enable_copyability_gate: bool = True
    admission_mode: bool = False
    max_copyability_event_age_s: float = 10.0
    max_wallet_fetch_duration_s: float = 2.0
    min_copyability_clob_fill_ratio: float = 0.999
    wallet_fraction: float = 0.05
    max_order_usd: float = 2.0
    min_order_usd: float = 0.0
    profit_policy_state_path: str = ""
    strategy_direction_state_path: str = ""
    strict_mirror_coverage: bool = True
    paper_retain_orders: int = 1_000
    paper_retain_lifecycle_events: int = 3_000
    paper_retain_dedupe_ids: int = 250_000
    retain_seen: int = 250_000
    failed_retry_after_s: float = 300.0
    failed_retry_max_attempts: int = 3
    seed_history_state_path: str = ""
    seed_before_poll: bool = False
    seed_max_events: int = 0
    seed_lookback_s: float = 0.0
    wallet_address_allowlist: tuple[str, ...] = ()
    wallet_name_allowlist: tuple[str, ...] = ()
    max_wallets: int = 0
    parallel_wallet_fetches: int = 1
    use_profit_search_scope: bool = True
    track_blocked_profit_policy: bool = False
    profit_policy_candidate_only: bool = False
    allow_source_base_overrides_in_admission: bool = False
    intent_time_copyability_proof_state_path: str = ""
    rtds_activity_jsonl_path: str = ""

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _mux_event_identity(event: WalletEvent) -> tuple[str, str, str, str]:
    """Cross-route identity: stable market/token/side/transaction tuple."""

    return (
        str(event.condition_id or event.market_slug),
        str(event.token_id or event.outcome),
        str(event.action).upper(),
        str(event.transaction_hash or event.event_id),
    )


def _prefer_earliest_observed_events(events: Iterable[WalletEvent]) -> list[WalletEvent]:
    """Keep the first-observed representation when API and RTDS overlap."""

    selected: dict[tuple[str, str, str, str], WalletEvent] = {}
    for event in events:
        identity = _mux_event_identity(event)
        previous = selected.get(identity)
        if previous is None or float(event.observed_ts) < float(previous.observed_ts):
            selected[identity] = event
    return list(selected.values())


def _read_rtds_wallet_events(
    path: str,
    *,
    specs: Iterable[WalletSpec],
    cursor: dict[str, Any] | None,
    now_ts: float | None = None,
) -> tuple[dict[str, list[WalletEvent]], dict[str, Any]]:
    """Read new canonical RTDS rows without replaying history on first attach."""

    target_specs = {spec.normalized_address(): spec for spec in specs}
    prior = cursor if isinstance(cursor, dict) else {}
    source_path = Path(path)
    report: dict[str, Any] = {
        "path": str(source_path),
        "route": "rtds_activity",
        "byte_offset": int(prior.get("byte_offset") or 0),
        "rows_read": 0,
        "trade_rows": 0,
        "matching_rows": 0,
        "fresh_le_policy_age": 0,
        "malformed_rows": 0,
        "initialized_at_eof": False,
        "paper_only": True,
        "live_orders_allowed": False,
    }
    events_by_wallet: dict[str, list[WalletEvent]] = defaultdict(list)
    if not path or not source_path.exists():
        report["status"] = "SOURCE_MISSING"
        return events_by_wallet, report

    file_size = source_path.stat().st_size
    if "byte_offset" not in prior:
        report["byte_offset"] = file_size
        report["next_byte_offset"] = file_size
        report["initialized_at_eof"] = True
        report["status"] = "CURSOR_INITIALIZED_NO_REPLAY"
        return events_by_wallet, report

    offset = max(0, min(int(prior.get("byte_offset") or 0), file_size))
    observed_now = float(now_ts if now_ts is not None else time.time())
    with source_path.open("rb") as handle:
        handle.seek(offset)
        for raw_line in handle:
            report["rows_read"] += 1
            try:
                row = json.loads(raw_line)
            except (TypeError, ValueError):
                report["malformed_rows"] += 1
                continue
            if not isinstance(row, dict) or row.get("event") != "rtds_trade_event":
                continue
            report["trade_rows"] += 1
            wallet = str(row.get("source_wallet") or "").lower()
            spec = target_specs.get(wallet)
            if spec is None:
                continue
            source_row = row.get("raw") if isinstance(row.get("raw"), dict) else row
            observed_ts = num(row.get("received_at_s"), observed_now)
            event = normalize_polymarket_wallet_row(
                source_row,
                spec=spec,
                row_type="rtds_activity",
                observed_ts=observed_ts,
            )
            if event is None:
                continue
            event = replace(event, source="rtds_activity")
            events_by_wallet[wallet].append(event)
            report["matching_rows"] += 1
            if event.age_s is not None and event.age_s <= 10.0:
                report["fresh_le_policy_age"] += 1
        report["next_byte_offset"] = handle.tell()
    report["status"] = "PASS"
    report["latest_observed_ts"] = max(
        (event.observed_ts for events in events_by_wallet.values() for event in events),
        default=None,
    )
    report["latest_event_ts"] = max(
        (
            event.event_ts
            for events in events_by_wallet.values()
            for event in events
            if event.event_ts is not None
        ),
        default=None,
    )
    return events_by_wallet, report


class CLOBMarketClient:
    DIRECT_CLOB_HOST = "https://clob.polymarket.com"

    def __init__(
        self,
        host: str = "https://clob.polymarket.com",
        timeout_s: float = 5.0,
        fallback_hosts: Iterable[str] | None = None,
        retries: int = 3,
    ):
        self.host = host.rstrip("/")
        self.timeout_s = timeout_s
        self.fallback_hosts = self._fallback_hosts(self.host, fallback_hosts)
        self.retries = max(1, int(retries))
        self.last_route_report: dict[str, Any] = {}

    @classmethod
    def _fallback_hosts(cls, host: str, fallback_hosts: Iterable[str] | None) -> tuple[str, ...]:
        values = [str(item or "").rstrip("/") for item in (fallback_hosts or []) if str(item or "").strip()]
        if not values and str(host or "").rstrip("/") != cls.DIRECT_CLOB_HOST:
            values.append(cls.DIRECT_CLOB_HOST)
        seen: set[str] = set()
        out: list[str] = []
        for value in values:
            if value and value not in seen and value != str(host or "").rstrip("/"):
                seen.add(value)
                out.append(value)
        return tuple(out)

    @staticmethod
    def _is_empty_book_http_error(exc: requests.HTTPError) -> bool:
        response = getattr(exc, "response", None)
        return int(getattr(response, "status_code", 0) or 0) == 404

    @staticmethod
    def _is_retryable_book_http_error(exc: requests.HTTPError) -> bool:
        response = getattr(exc, "response", None)
        return int(getattr(response, "status_code", 0) or 0) in {429, 500, 502, 503, 504}

    @staticmethod
    def _route_report_from_http_error(exc: requests.HTTPError) -> dict[str, Any]:
        response = getattr(exc, "response", None)
        report = getattr(response, "wallet_copy_route_report", {}) if response is not None else {}
        return report if isinstance(report, dict) else {}

    @staticmethod
    def _book_with_route_report(book: dict[str, Any], route_report: dict[str, Any]) -> dict[str, Any]:
        if route_report:
            book = dict(book)
            book["__walletCopyClobRouteReport"] = dict(route_report)
        return book

    def _empty_book_from_http_404(
        self,
        token_id: str,
        *,
        host: str,
        route_report: dict[str, Any],
        fallback_attempts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        report = {
            **route_report,
            "status": "HTTP_404_EMPTY_BOOK",
            "route_class": "EMPTY_BOOK",
            "empty_book_truth": True,
            "clob_host_used": host,
            "fallback_attempts": fallback_attempts,
        }
        self.last_route_report = report
        return self._book_with_route_report(
            {
                "asset_id": str(token_id),
                "asks": [],
                "bids": [],
                "empty_book_truth": True,
                "empty_book_reason": "clob_book_http_404",
            },
            report,
        )

    def get_book(self, token_id: str) -> dict[str, Any]:
        fallback_attempts: list[dict[str, Any]] = []
        last_exc: Exception | None = None
        hosts = (self.host, *self.fallback_hosts)
        for index, host in enumerate(hosts):
            try:
                payload, route_report = _get_json_with_route_report(
                    f"{host}/book",
                    params={"token_id": str(token_id)},
                    timeout_s=self.timeout_s,
                    headers={"Accept": "application/json", "User-Agent": "wallet-copy-live-tracker/1.0"},
                    retries=self.retries,
                    request_role="clob_book_fetch",
                )
                report = route_report if isinstance(route_report, dict) else {}
                if fallback_attempts:
                    report = {**report, "fallback_attempts": list(fallback_attempts), "clob_host_used": host}
                self.last_route_report = report
                book = payload if isinstance(payload, dict) else {}
                return self._book_with_route_report(book, self.last_route_report)
            except PolymarketRouteError as exc:
                last_exc = exc
                report = exc.route_report if isinstance(exc.route_report, dict) else {}
                fallback_attempts.append(
                    {
                        "host": host,
                        "status": "ROUTE_ERROR",
                        "route_class": report.get("route_class"),
                        "error": str(exc)[:500],
                    }
                )
                self.last_route_report = report
                if index + 1 < len(hosts):
                    continue
                raise
            except requests.HTTPError as exc:
                last_exc = exc
                report = self._route_report_from_http_error(exc)
                response = getattr(exc, "response", None)
                status_code = int(getattr(response, "status_code", 0) or 0)
                if self._is_empty_book_http_error(exc):
                    return self._empty_book_from_http_404(
                        str(token_id),
                        host=host,
                        route_report=report,
                        fallback_attempts=fallback_attempts,
                    )
                fallback_attempts.append(
                    {
                        "host": host,
                        "status": "HTTP_ERROR",
                        "http_status": status_code,
                        "route_class": report.get("route_class"),
                        "error": str(exc)[:500],
                    }
                )
                self.last_route_report = report
                if self._is_retryable_book_http_error(exc) and index + 1 < len(hosts):
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        return {}

    @staticmethod
    def summarize_book(
        book: dict[str, Any],
        *,
        copy_size_usd: float,
        source_price: float,
        max_slippage_bps: float,
    ) -> dict[str, Any]:
        asks = [row for row in book.get("asks") or [] if isinstance(row, dict)]
        bids = [row for row in book.get("bids") or [] if isinstance(row, dict)]
        ask_rows = sorted(
            ((num(row.get("price")), num(row.get("size"))) for row in asks),
            key=lambda item: item[0],
        )
        bid_rows = sorted(
            ((num(row.get("price")), num(row.get("size"))) for row in bids),
            key=lambda item: item[0],
            reverse=True,
        )
        best_ask = ask_rows[0][0] if ask_rows else 0.0
        best_bid = bid_rows[0][0] if bid_rows else 0.0
        max_copy_price = min(0.99, float(source_price) * (1.0 + max(0.0, float(max_slippage_bps)) / 10_000.0))
        remaining_usd = max(0.0, float(copy_size_usd))
        shares = 0.0
        spent = 0.0
        levels_used = 0
        for price, available_shares in ask_rows:
            if price <= 0 or available_shares <= 0:
                continue
            if price > max_copy_price:
                break
            level_usd = price * available_shares
            take_usd = min(remaining_usd, level_usd)
            if take_usd <= 0:
                continue
            shares += take_usd / price
            spent += take_usd
            remaining_usd -= take_usd
            levels_used += 1
            if remaining_usd <= 1e-9:
                break
        avg_fill_price = spent / shares if shares > 0 else 0.0
        fill_ratio = spent / float(copy_size_usd) if copy_size_usd > 0 else 0.0
        remaining_usd_rounded = round(max(0.0, remaining_usd), 6)
        if copy_size_usd <= 0:
            blocking_reason = "zero_copy_size"
        elif not ask_rows:
            blocking_reason = "no_ask_liquidity"
        elif spent <= 0:
            blocking_reason = "price_above_slippage_cap"
        elif fill_ratio < 0.999:
            blocking_reason = "insufficient_depth_within_slippage_cap"
        else:
            blocking_reason = "none"
        return {
            "book_market": book.get("market"),
            "asset_id": book.get("asset_id"),
            "book_timestamp": book.get("timestamp"),
            "book_hash": book.get("hash"),
            "best_bid": round(best_bid, 6),
            "best_ask": round(best_ask, 6),
            "spread": round(best_ask - best_bid, 6) if best_ask and best_bid else 0.0,
            "source_price": round(float(source_price), 6),
            "max_copy_price": round(max_copy_price, 6),
            "copy_size_usd": round(float(copy_size_usd), 6),
            "fillable_usd": round(spent, 6),
            "fillable_shares": round(shares, 6),
            "remaining_usd": remaining_usd_rounded,
            "avg_fill_price": round(avg_fill_price, 6),
            "fill_ratio": round(fill_ratio, 6),
            "levels_used": levels_used,
            "blocking_reason": blocking_reason,
            "instant_fill_status": "PASS" if copy_size_usd > 0 and fill_ratio >= 0.999 else "BLOCKED",
        }

    @staticmethod
    def tactical_fillability_profiles(
        book: dict[str, Any],
        *,
        copy_size_usd: float,
        source_price: float,
        base_slippage_bps: float,
    ) -> dict[str, Any]:
        """Paper-only execution tactic probes for rejected wallet-copy BUYs.

        These profiles never relax admission. They record whether a rejected or
        stale event might become reproducible with a separately tested tactic.
        """

        profiles = {
            "strict_current": {
                "slippage_bps": max(0.0, float(base_slippage_bps)),
                "copy_size_usd": max(0.0, float(copy_size_usd)),
                "role": "current_tracker_copyability_profile",
            },
            "aggressive_best_ask_250bps": {
                "slippage_bps": 250.0,
                "copy_size_usd": max(0.0, float(copy_size_usd)),
                "role": "paper_only_aggressive_limit_probe",
            },
            "aggressive_best_ask_500bps": {
                "slippage_bps": 500.0,
                "copy_size_usd": max(0.0, float(copy_size_usd)),
                "role": "paper_only_aggressive_limit_probe",
            },
            "aggressive_best_ask_750bps": {
                "slippage_bps": 750.0,
                "copy_size_usd": max(0.0, float(copy_size_usd)),
                "role": "paper_only_aggressive_limit_probe",
            },
            "aggressive_best_ask_1000bps": {
                "slippage_bps": 1000.0,
                "copy_size_usd": max(0.0, float(copy_size_usd)),
                "role": "paper_only_aggressive_limit_probe",
            },
            "aggressive_best_ask_1500bps": {
                "slippage_bps": 1500.0,
                "copy_size_usd": max(0.0, float(copy_size_usd)),
                "role": "paper_only_aggressive_limit_probe",
            },
            "aggressive_best_ask_2000bps": {
                "slippage_bps": 2000.0,
                "copy_size_usd": max(0.0, float(copy_size_usd)),
                "role": "paper_only_aggressive_limit_probe",
            },
            "aggressive_best_ask_3000bps": {
                "slippage_bps": 3000.0,
                "copy_size_usd": max(0.0, float(copy_size_usd)),
                "role": "paper_only_aggressive_limit_probe",
            },
            "aggressive_750bps": {
                "slippage_bps": 750.0,
                "copy_size_usd": max(0.0, float(copy_size_usd)),
                "role": "paper_only_aggressive_limit_probe_legacy_alias",
            },
            "micro_batch_min_1usd_aggressive_750bps": {
                "slippage_bps": 750.0,
                "copy_size_usd": max(1.0, float(copy_size_usd)),
                "role": "paper_only_micro_batch_probe",
            },
        }
        out: dict[str, Any] = {}
        for profile_id, profile in profiles.items():
            summary = CLOBMarketClient.summarize_book(
                book,
                copy_size_usd=float(profile["copy_size_usd"]),
                source_price=source_price,
                max_slippage_bps=float(profile["slippage_bps"]),
            )
            out[profile_id] = {
                **profile,
                **summary,
                "profile_id": profile_id,
                "paper_only": True,
                "live_orders_allowed": False,
                "status": "PASS" if summary.get("instant_fill_status") == "PASS" else "WATCH",
                "would_fill_event": summary.get("instant_fill_status") == "PASS",
                "would_fill_additional_event": (
                    profile_id != "strict_current" and summary.get("instant_fill_status") == "PASS"
                ),
            }
        return out


class GammaMarketClient:
    def __init__(self, host: str = "https://gamma-api.polymarket.com", timeout_s: float = 5.0):
        self.host = host.rstrip("/")
        self.timeout_s = timeout_s
        self.last_error: dict[str, Any] = {}

    @staticmethod
    def _maybe_json(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    def market_by_slug(self, slug: str) -> dict[str, Any]:
        self.last_error = {}
        if not slug:
            return {}
        try:
            payload = _get_json_with_retries(
                f"{self.host}/markets",
                params={"slug": slug, "limit": 1},
                timeout_s=self.timeout_s,
                headers={"Accept": "application/json", "User-Agent": "wallet-copy-live-tracker/1.0"},
            )
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            self.last_error = {
                "status": "ERROR",
                "error_class": type(exc).__name__,
                "error": str(exc),
                "http_status_code": status_code,
                "slug": slug,
            }
            return {}
        if isinstance(payload, list) and payload:
            return payload[0] if isinstance(payload[0], dict) else {}
        if isinstance(payload, dict):
            rows = payload.get("data") or payload.get("markets")
            if isinstance(rows, list) and rows:
                return rows[0] if isinstance(rows[0], dict) else {}
            return payload
        return {}

    def clob_token_ids_for_event(self, event: WalletEvent) -> list[str]:
        market = self.market_by_slug(event.market_slug)
        token_ids = self._maybe_json(market.get("clobTokenIds") or market.get("clob_token_ids") or [])
        if isinstance(token_ids, list):
            return [str(token_id) for token_id in token_ids if token_id]
        return []


class OnchainReceiptClient:
    def __init__(self, rpc_url: str = "https://polygon-bor-rpc.publicnode.com", timeout_s: float = 8.0):
        self.rpc_url = rpc_url
        self.timeout_s = timeout_s

    def get_receipt(self, tx_hash: str) -> dict[str, Any]:
        response = requests.post(
            self.rpc_url,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt", "params": [tx_hash]},
            timeout=self.timeout_s,
            headers={"Accept": "application/json", "User-Agent": "wallet-copy-live-tracker/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        return result if isinstance(result, dict) else {}

    @staticmethod
    def summarize_receipt(receipt: dict[str, Any], *, tx_hash: str, wallet: str) -> dict[str, Any]:
        if not receipt:
            return {"tx_hash": tx_hash, "status": "PENDING_OR_NOT_FOUND"}
        status_hex = str(receipt.get("status") or "")
        logs = [row for row in receipt.get("logs") or [] if isinstance(row, dict)]
        wallet_tail = wallet.lower().removeprefix("0x")
        log_blob = json.dumps(logs, sort_keys=True).lower()
        return {
            "tx_hash": tx_hash,
            "status": "CONFIRMED" if status_hex in {"0x1", "1"} else "FAILED",
            "block_number": int(str(receipt.get("blockNumber") or "0x0"), 16),
            "transaction_index": int(str(receipt.get("transactionIndex") or "0x0"), 16),
            "log_count": len(logs),
            "contract_addresses": sorted({str(row.get("address") or "").lower() for row in logs if row.get("address")}),
            "tracked_wallet_seen_in_logs": bool(wallet_tail and wallet_tail in log_blob),
        }


def _empty_state() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "wallet_copy_live_tracking_state",
        "paper_only": True,
        "live_orders_allowed": False,
        "seen_source_fingerprints": [],
        "failed_source_fingerprints": [],
        "failed_source_details": {},
        "seed": {"status": "DISABLED"},
        "wallets": {},
        "last_moves": [],
        "summary": {},
    }


def _move_key(move: dict[str, Any]) -> str:
    wallet_event = move.get("wallet_event") if isinstance(move.get("wallet_event"), dict) else {}
    event_id = wallet_event.get("event_id") or move.get("source_event_id")
    if event_id:
        return f"event:{event_id}"
    fingerprint = wallet_event.get("source_fingerprint") or move.get("source_fingerprint")
    if fingerprint:
        return f"fingerprint:{fingerprint}"
    return "|".join(
        str(value or "")
        for value in (
            wallet_event.get("source_wallet"),
            wallet_event.get("transaction_hash"),
            wallet_event.get("condition_id") or wallet_event.get("market_slug"),
            wallet_event.get("action") or move.get("action"),
            wallet_event.get("outcome"),
            wallet_event.get("event_ts"),
        )
    )


def _sidecar_json_path(path: str, stem_suffix: str) -> str:
    source = Path(path)
    suffix = source.suffix or ".json"
    return str(source.with_name(f"{source.stem}_{stem_suffix}{suffix}"))


def _sidecar_jsonl_path(path: str, stem_suffix: str) -> str:
    source = Path(path)
    suffix = source.suffix or ".jsonl"
    return str(source.with_name(f"{source.stem}_{stem_suffix}{suffix}"))


def _recent_event_log_moves(path: str, *, limit: int) -> list[dict[str, Any]]:
    if not path or limit <= 0:
        return []
    log_path = Path(path)
    if not log_path.exists():
        return []
    rows: deque[dict[str, Any]] = deque(maxlen=int(limit))
    max_scan_bytes = max(1_048_576, min(64_000_000, int(limit) * 256_000))
    chunk_size = 1_048_576
    with log_path.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        chunks: list[bytes] = []
        newline_count = 0
        scanned = 0
        while position > 0 and scanned < max_scan_bytes and newline_count <= int(limit) * 3:
            read_size = min(chunk_size, position, max_scan_bytes - scanned)
            position -= read_size
            handle.seek(position)
            chunk = handle.read(read_size)
            chunks.append(chunk)
            scanned += len(chunk)
            newline_count += chunk.count(b"\n")
        raw_lines = b"".join(reversed(chunks)).splitlines()
        for raw_line in raw_lines[-max(int(limit) * 5, int(limit)):]:
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict) and isinstance(row.get("wallet_event"), dict):
                rows.append(row)
    return list(rows)


DEFAULT_TRADE_QUERY_KEYS = ("user", "proxyWallet")


def _normalized_trade_query_keys(keys: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    values = tuple(str(item).strip() for item in (keys or DEFAULT_TRADE_QUERY_KEYS) if str(item).strip())
    return values or DEFAULT_TRADE_QUERY_KEYS


def _trade_query_scope_compatible(
    move: dict[str, Any],
    *,
    trade_query_keys: tuple[str, ...] | list[str] | None,
) -> bool:
    current_scope = _normalized_trade_query_keys(trade_query_keys)
    move_scope = move.get("data_api_trade_query_keys")
    if isinstance(move_scope, list):
        return _normalized_trade_query_keys([str(item) for item in move_scope]) == current_scope
    if isinstance(move_scope, tuple):
        return _normalized_trade_query_keys(list(move_scope)) == current_scope
    # Older persisted rows did not carry an exact source-scope contract. They are
    # still usable for the default audit lane, but not for a narrowed hot-copy
    # lane where stale full-source timing would pollute live-readiness proof.
    return current_scope == DEFAULT_TRADE_QUERY_KEYS


def _trade_query_scope_compatible_count(
    moves: list[dict[str, Any]],
    *,
    trade_query_keys: tuple[str, ...] | list[str] | None,
) -> int:
    return sum(
        1
        for move in moves
        if isinstance(move, dict)
        and isinstance(move.get("wallet_event"), dict)
        and _trade_query_scope_compatible(move, trade_query_keys=trade_query_keys)
    )


def _copyability_context(config: LiveTrackerConfig) -> dict[str, Any]:
    return {
        "admission_mode": bool(config.admission_mode),
        "enable_clob_books": bool(config.enable_clob_books),
        "enable_copyability_gate": bool(config.enable_copyability_gate),
        "max_book_slippage_bps": round(float(config.max_book_slippage_bps), 6),
        "max_copyability_event_age_s": round(float(config.max_copyability_event_age_s), 6),
        "max_order_usd": round(float(config.max_order_usd), 6),
        "max_wallet_fetch_duration_s": round(float(config.max_wallet_fetch_duration_s), 6),
        "min_copyability_clob_fill_ratio": round(float(config.min_copyability_clob_fill_ratio), 6),
        "min_order_usd": round(float(config.min_order_usd), 6),
        "require_clob_book_evidence_for_efficiency": bool(config.require_clob_book_evidence_for_efficiency),
        "strict_mirror_coverage": bool(config.strict_mirror_coverage),
        "wallet_fraction": round(float(config.wallet_fraction), 8),
        "allow_source_base_overrides_in_admission": bool(config.allow_source_base_overrides_in_admission),
    }


def _copyability_context_id_from_context(context: dict[str, Any]) -> str:
    blob = json.dumps(context, sort_keys=True, separators=(",", ":"))
    return "copyctx_" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _copyability_context_id(config: LiveTrackerConfig) -> str:
    return _copyability_context_id_from_context(_copyability_context(config))


def _default_copyability_context_id() -> str:
    return _copyability_context_id(LiveTrackerConfig())


def _copyability_context_compatible(move: dict[str, Any], *, copyability_context_id: str) -> bool:
    move_context_id = str(move.get("copyability_context_id") or "")
    if move_context_id:
        return move_context_id == str(copyability_context_id)
    mirror = move.get("mirror_result") if isinstance(move.get("mirror_result"), dict) else {}
    mirror_context_id = str(mirror.get("copyability_context_id") or "")
    if mirror_context_id:
        return mirror_context_id == str(copyability_context_id)
    return str(copyability_context_id) == _default_copyability_context_id()


def _copyability_context_compatible_count(
    moves: list[dict[str, Any]],
    *,
    copyability_context_id: str,
) -> int:
    return sum(
        1
        for move in moves
        if isinstance(move, dict)
        and isinstance(move.get("wallet_event"), dict)
        and _copyability_context_compatible(move, copyability_context_id=copyability_context_id)
    )


def _admission_evidence_window(
    *move_groups: list[dict[str, Any]] | Any,
    limit: int,
    mirror_classifier_version: int | None = None,
    profit_policy_context_id: str | None = None,
    profit_policy_candidate_id: str | None = None,
    trade_query_keys: tuple[str, ...] | list[str] | None = None,
    copyability_context_id: str | None = None,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for group in move_groups:
        if not isinstance(group, list):
            continue
        for move in group:
            if not isinstance(move, dict) or not isinstance(move.get("wallet_event"), dict):
                continue
            if mirror_classifier_version is not None:
                mirror_result = move.get("mirror_result") if isinstance(move.get("mirror_result"), dict) else {}
                if int(mirror_result.get("mirror_classifier_version") or 0) != int(mirror_classifier_version):
                    continue
            if profit_policy_context_id is not None:
                mirror_result = move.get("mirror_result") if isinstance(move.get("mirror_result"), dict) else {}
                context_id = move.get("profit_policy_context_id") or mirror_result.get("profit_policy_context_id")
                if str(context_id or "") != str(profit_policy_context_id):
                    continue
            if profit_policy_candidate_id is not None:
                mirror_result = move.get("mirror_result") if isinstance(move.get("mirror_result"), dict) else {}
                candidate_id = move.get("profit_policy_candidate_id") or mirror_result.get("profit_policy_candidate_id")
                if str(candidate_id or "") != str(profit_policy_candidate_id):
                    continue
            if not _trade_query_scope_compatible(move, trade_query_keys=trade_query_keys):
                continue
            if copyability_context_id is not None and not _copyability_context_compatible(
                move,
                copyability_context_id=copyability_context_id,
            ):
                continue
            key = _move_key(move)
            if key in merged:
                merged.pop(key)
            merged[key] = move
    return list(merged.values())[-int(limit):]


def _mirror_version_compatible_count(
    moves: list[dict[str, Any]],
    *,
    mirror_classifier_version: int,
) -> int:
    return sum(
        1
        for move in moves
        if isinstance(move, dict)
        and isinstance(move.get("wallet_event"), dict)
        and isinstance(move.get("mirror_result"), dict)
        and int(move["mirror_result"].get("mirror_classifier_version") or 0) == int(mirror_classifier_version)
    )


def _profit_policy_context_compatible_count(
    moves: list[dict[str, Any]],
    *,
    profit_policy_context_id: str,
) -> int:
    return sum(
        1
        for move in moves
        if isinstance(move, dict)
        and isinstance(move.get("wallet_event"), dict)
        and str(
            move.get("profit_policy_context_id")
            or (move.get("mirror_result") if isinstance(move.get("mirror_result"), dict) else {}).get(
                "profit_policy_context_id"
            )
            or ""
        )
        == str(profit_policy_context_id)
    )


def _profit_policy_candidate_compatible_count(
    moves: list[dict[str, Any]],
    *,
    profit_policy_candidate_id: str,
) -> int:
    return sum(
        1
        for move in moves
        if isinstance(move, dict)
        and isinstance(move.get("wallet_event"), dict)
        and str(
            move.get("profit_policy_candidate_id")
            or (move.get("mirror_result") if isinstance(move.get("mirror_result"), dict) else {}).get(
                "profit_policy_candidate_id"
            )
            or ""
        )
        == str(profit_policy_candidate_id)
    )


def _copy_efficiency_from_window_moves(
    moves: list[dict[str, Any]],
    *,
    max_api_latency_s: float,
    max_avg_worse_slippage_bps: float,
    require_clob_book_evidence: bool,
    extra_scores: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    def backfilled_score(move: dict[str, Any]) -> dict[str, Any] | None:
        score = move.get("copy_efficiency")
        if not isinstance(score, dict):
            return None
        row = dict(score)
        copyability = move.get("copyability") if isinstance(move.get("copyability"), dict) else {}
        details = copyability.get("details") if isinstance(copyability.get("details"), dict) else {}
        if copyability:
            row.setdefault("copyability_accepted", copyability.get("accepted"))
            row.setdefault("copyability_reason", copyability.get("reason"))
            row.setdefault("copyability_policy_id", copyability.get("policy_id"))
            row.setdefault("copyability_details", details)
        else:
            details = row.get("copyability_details") if isinstance(row.get("copyability_details"), dict) else {}
        mirror = move.get("mirror_result") if isinstance(move.get("mirror_result"), dict) else {}
        profit_policy_candidate_id = move.get("profit_policy_candidate_id") or mirror.get("profit_policy_candidate_id")
        if profit_policy_candidate_id is not None:
            row.setdefault("profit_policy_candidate_id", profit_policy_candidate_id)
            row.setdefault("candidate_id", profit_policy_candidate_id)
        profit_policy_id = (
            move.get("profit_policy_context_id")
            or mirror.get("profit_policy_context_id")
            or mirror.get("profit_policy_id")
        )
        if profit_policy_id is not None:
            row.setdefault("profit_policy_id", profit_policy_id)
            row.setdefault("profit_policy_context_id", profit_policy_id)
            if row.get("policy_id") is None and (
                str(mirror.get("filter_policy") or "") in {"profit_policy", "copyability"}
                or row.get("profit_policy_accepted") is not None
            ):
                row["policy_id"] = profit_policy_id
        if row.get("data_api_trade_query_keys") is None and move.get("data_api_trade_query_keys") is not None:
            row["data_api_trade_query_keys"] = move.get("data_api_trade_query_keys")
        if row.get("data_api_trade_query_scope") is None and move.get("data_api_trade_query_scope") is not None:
            row["data_api_trade_query_scope"] = move.get("data_api_trade_query_scope")
        if row.get("data_api_trade_query_key") is None:
            evidence = move.get("tracking_evidence") if isinstance(move.get("tracking_evidence"), dict) else {}
            wallet_api = evidence.get("wallet_api") if isinstance(evidence.get("wallet_api"), dict) else {}
            if wallet_api.get("data_api_query_param") is not None:
                row["data_api_trade_query_key"] = wallet_api.get("data_api_query_param")
        if row.get("profit_policy_accepted") is None and str(mirror.get("filter_policy") or "") == "profit_policy":
            row["profit_policy_accepted"] = False
            row["profit_policy_reason"] = mirror.get("reason") or row.get("missed_copy_reason")
        elif row.get("profit_policy_accepted") is None and str(mirror.get("filter_policy") or "") != "profit_policy":
            row["profit_policy_accepted"] = True
            row["profit_policy_reason"] = row.get("profit_policy_reason") or "accepted"
        fallback_fields = {
            "clob_instant_fill_status": "clob_instant_fill_status",
            "clob_best_bid": "clob_best_bid",
            "clob_best_ask": "clob_best_ask",
            "clob_spread": "clob_spread",
            "clob_fillable_usd": "clob_fillable_usd",
            "clob_remaining_usd": "clob_remaining_usd",
            "clob_fill_ratio": "clob_fill_ratio",
            "clob_max_copy_price": "clob_max_copy_price",
            "clob_blocking_reason": "clob_blocking_reason",
        }
        for target, source in fallback_fields.items():
            if row.get(target) is None and source in details:
                row[target] = details.get(source)
        return row

    scores = [
        score
        for move in moves
        for score in [backfilled_score(move) if isinstance(move, dict) else None]
        if isinstance(score, dict)
    ]
    if extra_scores:
        scores.extend(dict(score) for score in extra_scores if isinstance(score, dict))
    return build_copy_efficiency_report_from_scores(
        scores,
        max_api_latency_s=max_api_latency_s,
        max_avg_worse_slippage_bps=max_avg_worse_slippage_bps,
        require_clob_book_evidence=require_clob_book_evidence,
    )


def _intent_time_copyability_proof_scores(path: str, *, max_age_s: float = 1800.0) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not str(path or "").strip():
        return [], {
            "enabled": False,
            "status": "DISABLED_NO_STATE_PATH",
            "records_loaded": 0,
            "records_used": 0,
            "accepted_records_used": 0,
        }
    state_path = Path(path)
    if not state_path.is_absolute():
        state_path = Path.cwd() / state_path
    payload = load_json(state_path, default={})
    if not isinstance(payload, dict):
        return [], {"enabled": False, "reason": "state_not_object", "state": str(state_path)}
    rows = payload.get("records") if isinstance(payload.get("records"), list) else []
    now_ts = time.time()
    scores: list[dict[str, Any]] = []
    stale = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            generated = datetime.fromisoformat(str(row.get("generated_at")).replace("Z", "+00:00"))
            if generated.tzinfo is None:
                generated = generated.replace(tzinfo=timezone.utc)
            age_s = max(0.0, now_ts - generated.timestamp())
        except (TypeError, ValueError):
            age_s = 0.0
        if max_age_s > 0 and age_s > float(max_age_s):
            stale += 1
            continue
        score = dict(row)
        score.setdefault("copyability_context_id", "intent_time_guard_copyability_v1")
        score.setdefault("profit_policy_context_id", "INTENT_TIME_GUARD_PROOF")
        scores.append(score)
    accepted = sum(1 for row in scores if row.get("copyability_accepted") is True)
    rejected = len(scores) - accepted
    return scores, {
        "enabled": True,
        "state": str(state_path),
        "source": "live_guard_intent_time_copyability_proof",
        "max_age_s": float(max_age_s),
        "records_loaded": len(rows),
        "records_used": len(scores),
        "records_stale": stale,
        "accepted_records_used": accepted,
        "rejected_records_used": rejected,
        "status": "PASS" if accepted else "WATCH",
    }


def _merge_intent_time_copyability_proof(
    report: dict[str, Any],
    *,
    scores: list[dict[str, Any]],
    proof_summary: dict[str, Any],
    max_api_latency_s: float,
    max_avg_worse_slippage_bps: float,
    require_clob_book_evidence: bool,
) -> dict[str, Any]:
    if not scores:
        merged = dict(report)
        merged["intent_time_copyability_proof"] = proof_summary
        return merged
    base_scores = [
        dict(row)
        for row in report.get("event_scores") or []
        if isinstance(row, dict)
    ]
    merged = build_copy_efficiency_report_from_scores(
        [*base_scores, *scores],
        max_api_latency_s=max_api_latency_s,
        max_avg_worse_slippage_bps=max_avg_worse_slippage_bps,
        require_clob_book_evidence=require_clob_book_evidence,
    )
    merged["intent_time_copyability_proof"] = proof_summary
    return merged


def _failed_detail(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _should_retry_failed_event(
    event: WalletEvent,
    failure: dict[str, Any],
    *,
    now_ts: float,
    retry_after_s: float,
    max_attempts: int,
) -> bool:
    """Bound noisy repeats while still allowing lifecycle rows to become copyable."""

    if not failure:
        return True
    attempts = int(failure.get("attempts") or 0)
    if attempts >= int(max_attempts):
        return False
    action = event.action.upper()
    if action not in {"SELL", "MERGE", "REDEEM"}:
        return False
    last_failed_ts = num(failure.get("last_failed_ts"), 0.0)
    return last_failed_ts <= 0 or now_ts - last_failed_ts >= float(retry_after_s)


def _record_failed_event_detail(
    failure_details: dict[str, Any],
    event: WalletEvent,
    *,
    mirror_result: dict[str, Any],
    now_ts: float,
    retry_after_s: float,
    max_attempts: int,
) -> None:
    previous = _failed_detail(failure_details.get(event.source_fingerprint))
    attempts = int(previous.get("attempts") or 0) + 1
    action = event.action.upper()
    retryable = action in {"SELL", "MERGE", "REDEEM"} and attempts < int(max_attempts)
    failure_details[event.source_fingerprint] = {
        "source_event_id": event.event_id,
        "source_wallet": event.source_wallet.lower(),
        "wallet_name": event.wallet_name,
        "wallet_action": action,
        "condition_id": event.condition_id,
        "outcome": event.outcome,
        "first_failed_ts": previous.get("first_failed_ts") or now_ts,
        "last_failed_ts": now_ts,
        "attempts": attempts,
        "retry_after_s": float(retry_after_s),
        "max_attempts": int(max_attempts),
        "retryable": retryable,
        "last_reason": mirror_result.get("reason") or mirror_result.get("lifecycle_status") or "coverage_violation",
    }


def _load_market_ws_rows(path: str, *, lookback_s: float) -> list[dict[str, Any]]:
    if not path:
        return []
    target = Path(path)
    if not target.exists():
        return []
    now = time.time()
    rows: list[dict[str, Any]] = []
    try:
        max_lines = 200_000
        max_scan_bytes = 64_000_000
        chunk_size = 1_048_576
        with target.open("rb") as handle:
            handle.seek(0, 2)
            position = handle.tell()
            chunks: list[bytes] = []
            newline_count = 0
            scanned = 0
            while position > 0 and scanned < max_scan_bytes and newline_count <= max_lines:
                read_size = min(chunk_size, position, max_scan_bytes - scanned)
                position -= read_size
                handle.seek(position)
                chunk = handle.read(read_size)
                chunks.append(chunk)
                scanned += len(chunk)
                newline_count += chunk.count(b"\n")
        raw_lines = b"".join(reversed(chunks)).splitlines()[-max_lines:]
    except OSError:
        return []
    for raw_line in raw_lines:
        try:
            line = raw_line.decode("utf-8")
        except UnicodeDecodeError:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        ts = _market_ws_capture_ts(row)
        if ts is not None and now - ts > lookback_s:
            continue
        rows.append(row)
    return rows


def _market_ws_capture_ts(row: dict[str, Any]) -> float | None:
    return parse_ts(
        row.get("captured_at_s")
        or row.get("received_at_s")
        or row.get("timestamp")
        or row.get("ts")
        or row.get("time")
        or row.get("last_update")
    )


def _market_ws_event_ts(row: dict[str, Any]) -> float | None:
    return parse_ts(
        row.get("timestamp")
        or row.get("ts")
        or row.get("time")
        or row.get("last_update")
        or row.get("captured_at_s")
        or row.get("received_at_s")
    )


def _match_preconfirm(event: WalletEvent, rows: list[dict[str, Any]], *, window_s: float, price_tolerance: float) -> dict[str, Any]:
    matches = []
    for row in rows:
        token_id = str(row.get("asset_id") or row.get("asset") or row.get("token_id") or row.get("tokenId") or "")
        if token_id and event.token_id and token_id != event.token_id:
            continue
        price = num(row.get("price", row.get("last_trade_price", row.get("p"))), -1.0)
        if price >= 0 and abs(price - event.price) > price_tolerance:
            continue
        side = str(row.get("side") or row.get("taker_side") or row.get("trade_side") or "").upper()
        if side and event.action.upper() in {"BUY", "SELL"} and side not in {event.action.upper(), "TRADE"}:
            continue
        size = num(row.get("size", row.get("amount", row.get("shares"))), -1.0)
        if size >= 0 and event.size > 0 and abs(size - event.size) > max(0.01, abs(event.size) * 0.05):
            continue
        ts = _market_ws_event_ts(row)
        if ts is not None and event.event_ts is not None and abs(ts - event.event_ts) > window_s:
            continue
        captured_at_s = _market_ws_capture_ts(row)
        matches.append(
            {
                "event_type": row.get("event_type") or row.get("type"),
                "timestamp": ts,
                "captured_at_s": captured_at_s,
                "time_to_wallet_event_s": round(event.event_ts - captured_at_s, 6)
                if captured_at_s is not None and event.event_ts is not None
                else None,
                "token_id": token_id,
                "price": price,
                "side": side or None,
                "size": size if size >= 0 else None,
                "raw_keys": sorted(row.keys()),
            }
        )
    return {
        "status": "MATCHED_UNATTRIBUTED_MARKET_PRECONFIRM" if matches else "NO_MATCH",
        "matches": matches[:5],
        "match_count": len(matches),
    }


def _attach_tracking_evidence(intent: CopyIntent, evidence: dict[str, Any]) -> CopyIntent:
    payload = intent.asdict()
    metadata = dict(payload.get("metadata") or {})
    metadata["live_tracking_evidence"] = evidence
    payload["metadata"] = metadata
    return CopyIntent.from_dict(payload)


def _dedupe_intents(intents: list[CopyIntent]) -> list[CopyIntent]:
    unique: dict[str, CopyIntent] = {}
    for intent in intents:
        unique[intent.intent_id] = intent
    return sorted(unique.values(), key=lambda intent: (intent.event_ts or 0.0, intent.intent_id))


def _event_identity(event: WalletEvent) -> dict[str, Any]:
    return {
        "source_wallet": event.source_wallet.lower(),
        "wallet_name": event.wallet_name,
        "source_event_id": event.event_id,
        "source_fingerprint": event.source_fingerprint,
        "row_type": event.row_type,
        "action": event.action.upper(),
        "condition_id": event.condition_id,
        "market_slug": event.market_slug,
        "outcome": event.outcome,
        "price": round(float(event.price), 6),
        "size": round(float(event.size), 6),
        "usdc_size": round(float(event.usdc_size), 6),
        "token_id": event.token_id,
        "transaction_hash": event.transaction_hash,
        "event_ts": event.event_ts,
    }


def _filtered_entry_keys(event: WalletEvent) -> tuple[str, str]:
    wallet = event.source_wallet.lower()
    condition_id = str(event.condition_id or "")
    outcome = str(event.outcome or "").lower()
    return (
        "|".join([wallet, condition_id, outcome]),
        "|".join([wallet, condition_id, "*"]),
    )


def _filtered_entry_context(
    events: list[WalletEvent],
    *,
    policy: CopyPolicy,
    profit_policy: CandidatePolicy | None,
    profit_policy_candidate_id: str | None,
    profit_filter_decisions: dict[str, tuple[bool, str]],
    copyability_decisions: dict[str, CopyabilityDecision],
    now_ts: float,
) -> dict[str, dict[str, Any]]:
    rows_by_key: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        if not event.is_buy:
            continue
        filter_policy = ""
        reason = ""
        extra: dict[str, Any] = {}
        allowed, profit_reason = profit_filter_decisions.get(event.event_id, (True, "accepted"))
        if profit_policy is not None and not allowed:
            filter_policy = "profit_policy"
            reason = profit_reason
            extra["profit_policy_id"] = profit_policy.policy_id
            extra["profit_policy_context_id"] = profit_policy.policy_id
            if profit_policy_candidate_id:
                extra["profit_policy_candidate_id"] = profit_policy_candidate_id
        else:
            copyability = copyability_decisions.get(event.event_id)
            if copyability is not None and not copyability.accepted:
                filter_policy = "copyability"
                reason = copyability.reason
                if profit_policy is not None:
                    extra["profit_policy_id"] = profit_policy.policy_id
                    extra["profit_policy_context_id"] = profit_policy.policy_id
                    if profit_policy_candidate_id:
                        extra["profit_policy_candidate_id"] = profit_policy_candidate_id
                extra["copyability_policy_id"] = copyability.policy_id
                extra["copyability_decision"] = copyability.asdict()
            else:
                accepted, policy_reason = policy.accepts(event, now_ts=now_ts)
                if not accepted:
                    filter_policy = "copy_policy"
                    reason = policy_reason
                    extra["copy_policy_id"] = policy.policy_id
        if not filter_policy:
            continue
        row = {
            "source_event_id": event.event_id,
            "source_fingerprint": event.source_fingerprint,
            "filter_policy": filter_policy,
            "reason": reason,
            "condition_id": event.condition_id,
            "outcome": event.outcome,
            "event_ts": event.event_ts,
            "source_usdc_size": round(float(event.usdc_size), 6),
            **extra,
        }
        for key in _filtered_entry_keys(event):
            rows_by_key.setdefault(key, []).append(row)

    summary_by_key: dict[str, dict[str, Any]] = {}
    for key, rows in rows_by_key.items():
        policy_counts = Counter(str(row.get("filter_policy") or "unknown") for row in rows)
        reason_counts = Counter(str(row.get("reason") or "unknown") for row in rows)
        dominant_policy = policy_counts.most_common(1)[0][0] if policy_counts else "unknown"
        summary_by_key[key] = {
            "filtered_entry_events": len(rows),
            "dominant_filter_policy": dominant_policy,
            "filter_policy_counts": dict(policy_counts),
            "reason_counts": dict(reason_counts),
            "source_event_ids": [str(row.get("source_event_id")) for row in rows[:10]],
            "outcomes": sorted({str(row.get("outcome") or "") for row in rows if row.get("outcome")}),
            "sample": rows[:5],
        }
    return summary_by_key


def _lookup_filtered_entry_context(
    event: WalletEvent,
    filtered_entry_context_by_key: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    exact_key, condition_key = _filtered_entry_keys(event)
    context = filtered_entry_context_by_key.get(exact_key)
    if isinstance(context, dict):
        return {**context, "match_scope": "same_condition_outcome"}
    context = filtered_entry_context_by_key.get(condition_key)
    if isinstance(context, dict):
        return {**context, "match_scope": "same_condition"}
    return {}


def _seed_exclusion_keys(event: WalletEvent) -> set[str]:
    rounded_price = round(float(event.price), 8)
    rounded_size = round(float(event.size), 8)
    event_ts = event.event_ts
    wallet = event.source_wallet.lower()
    keys = {
        f"fingerprint:{event.source_fingerprint}",
        f"event_id:{event.event_id}",
        "|".join(
            [
                "semantic",
                wallet,
                str(event.transaction_hash or ""),
                str(event.condition_id or ""),
                str(event.token_id or ""),
                str(event.outcome or ""),
                str(event.action or "").upper(),
                str(rounded_price),
                str(rounded_size),
                str(event_ts),
            ]
        ),
    }
    if event.transaction_hash:
        keys.add(
            "|".join(
                [
                    "tx",
                    wallet,
                    str(event.transaction_hash),
                    str(event.condition_id or ""),
                    str(event.token_id or ""),
                    str(event.outcome or ""),
                    str(event.action or "").upper(),
                    str(event_ts),
                ]
            )
        )
        keys.add(
            "|".join(
                [
                    "tx_market",
                    wallet,
                    str(event.transaction_hash),
                    str(event.condition_id or ""),
                    str(event.outcome or ""),
                    str(event.action or "").upper(),
                    str(event_ts),
                ]
            )
        )
    keys.discard("event_id:")
    return keys


def _iso_from_ts(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(float(ts), timezone.utc).isoformat()


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * max(0.0, min(100.0, pct)) / 100.0))
    return round(ordered[index], 6)


def _wallet_event_action_counts(events: list[WalletEvent]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for event in events:
        action = str(event.action or "").upper() or "UNKNOWN"
        counts[action] += 1
    return dict(sorted(counts.items()))


def _dual_action_event_id_count(events: list[WalletEvent]) -> int:
    actions_by_event_id: dict[str, set[str]] = {}
    for event in events:
        if not event.event_id:
            continue
        actions_by_event_id.setdefault(str(event.event_id), set()).add(str(event.action or "").upper() or "UNKNOWN")
    return sum(1 for actions in actions_by_event_id.values() if len(actions) > 1)


def _same_tx_opposing_action_count(events: list[WalletEvent]) -> int:
    actions_by_tx: dict[tuple[str, str, str, float | None], set[str]] = {}
    for event in events:
        if not event.transaction_hash:
            continue
        key = (
            event.source_wallet.lower(),
            str(event.transaction_hash),
            str(event.condition_id or event.market_slug or ""),
            event.event_ts,
        )
        actions_by_tx.setdefault(key, set()).add(str(event.action or "").upper() or "UNKNOWN")
    return sum(1 for actions in actions_by_tx.values() if {"BUY", "SELL"} <= actions)


def _latency_summary(events: list[WalletEvent], *, now_ts: float, wallet_reports: list[dict[str, Any]]) -> dict[str, Any]:
    legacy_latencies = [float(event.api_latency_s) for event in events if event.api_latency_s is not None]
    event_ages = [float(event.age_s) for event in events if event.age_s is not None]
    event_ts = [float(event.event_ts) for event in events if event.event_ts is not None]
    fetch_durations = [
        float(row.get("fetch_duration_s"))
        for row in wallet_reports
        if isinstance(row, dict) and row.get("fetch_duration_s") is not None
    ]
    latest = max(event_ts) if event_ts else None
    oldest = min(event_ts) if event_ts else None
    return {
        "latency_basis": "api_latency_s is legacy wallet Data API event age; fetch_duration_s is request/poll duration",
        "api_latency_min_s": round(min(legacy_latencies), 6) if legacy_latencies else None,
        "api_latency_avg_s": _mean(legacy_latencies),
        "api_latency_max_s": round(max(legacy_latencies), 6) if legacy_latencies else None,
        "event_age_min_s": round(min(event_ages), 6) if event_ages else None,
        "event_age_avg_s": _mean(event_ages),
        "event_age_max_s": round(max(event_ages), 6) if event_ages else None,
        "wallet_api_fetch_duration_avg_s": _mean(fetch_durations),
        "wallet_api_fetch_duration_max_s": round(max(fetch_durations), 6) if fetch_durations else None,
        "latest_wallet_event_ts": latest,
        "latest_wallet_event_iso": _iso_from_ts(latest),
        "oldest_wallet_event_ts": oldest,
        "oldest_wallet_event_iso": _iso_from_ts(oldest),
        "freshest_event_lag_s": round(max(0.0, now_ts - latest), 6) if latest is not None else None,
        "oldest_event_lag_s": round(max(0.0, now_ts - oldest), 6) if oldest is not None else None,
    }


def _clob_book_cache_summary(report: dict[str, Any]) -> dict[str, Any]:
    durations = [
        float(value)
        for value in report.get("fetch_durations_s", [])
        if value is not None
    ]
    micro_durations = [
        float(value)
        for value in report.get("micro_probe_fetch_durations_s", [])
        if value is not None
    ]
    token_ids = report.get("token_ids")
    unique_token_ids = sorted(str(item) for item in token_ids) if isinstance(token_ids, set) else []
    lookup_events = int(report.get("lookup_events") or 0)
    cache_hits = int(report.get("cache_hits") or 0)
    cache_misses = int(report.get("cache_misses") or 0)
    micro_lookup_events = int(report.get("micro_probe_lookup_events") or 0)
    micro_cache_hits = int(report.get("micro_probe_cache_hits") or 0)
    micro_cache_misses = int(report.get("micro_probe_cache_misses") or 0)
    stale_not_admission_relevant_skipped = int(report.get("stale_not_admission_relevant_skipped") or 0)
    total_lookup_events = lookup_events + micro_lookup_events
    total_cache_hits = cache_hits + micro_cache_hits
    total_cache_misses = cache_misses + micro_cache_misses
    avoided_fetches = max(0, total_cache_hits)
    return {
        "enabled": True,
        "lookup_events": lookup_events,
        "unique_token_ids": len(unique_token_ids),
        "token_ids": unique_token_ids[:20],
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "micro_probe_lookup_events": micro_lookup_events,
        "micro_probe_cache_hits": micro_cache_hits,
        "micro_probe_cache_misses": micro_cache_misses,
        "micro_probe_errors": int(report.get("micro_probe_errors") or 0),
        "stale_not_admission_relevant_skipped": stale_not_admission_relevant_skipped,
        "total_lookup_events": total_lookup_events,
        "total_cache_hits": total_cache_hits,
        "total_cache_misses": total_cache_misses,
        "actual_book_fetches": total_cache_misses,
        "avoided_duplicate_book_fetches": avoided_fetches,
        "hit_rate_pct": round(100.0 * cache_hits / lookup_events, 6) if lookup_events else None,
        "total_hit_rate_pct": round(100.0 * total_cache_hits / total_lookup_events, 6)
        if total_lookup_events
        else None,
        "errors": int(report.get("errors") or 0),
        "fetch_duration_min_s": round(min(durations), 6) if durations else None,
        "fetch_duration_avg_s": _mean(durations),
        "fetch_duration_max_s": round(max(durations), 6) if durations else None,
        "micro_probe_fetch_duration_avg_s": _mean(micro_durations),
        "micro_probe_fetch_duration_max_s": round(max(micro_durations), 6) if micro_durations else None,
    }


def _paper_copy_contract(report: dict[str, Any], *, scope: str) -> dict[str, Any]:
    """Strict paper-first contract: every required copyable BUY must become a CLOB-backed paper fill."""

    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    required = int(summary.get("required_buy_copy_events") or 0)
    clob_filled = int(summary.get("clob_filled_buy_copy_events") or 0)
    fallback_filled = int(summary.get("fallback_filled_buy_copy_events") or 0)
    rejected = int(summary.get("rejected_buy_copy_events") or 0)
    missed = int(summary.get("missed_buy_copy_events") or 0)
    blockers: list[str] = []
    if required <= 0:
        blockers.append("no_required_copyable_buy_events")
    if clob_filled < required:
        blockers.append("copyable_buy_not_clob_backed_paper_filled")
    if fallback_filled > 0:
        blockers.append("fallback_fill_not_valid_for_live_copy_contract")
    if rejected > 0:
        blockers.append("paper_order_rejected")
    if missed > 0:
        blockers.append("required_buy_copy_missed")
    status = PASS if not blockers else active_status_from_blockers(blockers, default=ANALYZE)
    return {
        "status": status,
        "scope": scope,
        "role": "mechanical_copy_contract_not_profit_proof",
        "required_buy_copy_events": required,
        "clob_filled_buy_copy_events": clob_filled,
        "fallback_filled_buy_copy_events": fallback_filled,
        "rejected_buy_copy_events": rejected,
        "missed_buy_copy_events": missed,
        "paper_copy_fill_rate_pct": round(100.0 * clob_filled / required, 6) if required else None,
        "blockers": blockers,
        "live_orders_allowed": False,
        "paper_only": True,
    }


def _poll_event_processing_priority(event: WalletEvent, *, max_event_age_s: float) -> tuple[int, float, str]:
    """Prioritize live-admission evidence when a poll runtime limit cuts processing short."""

    age_s = event.age_s
    is_fresh_buy = bool(event.is_buy and age_s is not None and float(age_s) <= float(max_event_age_s))
    event_ts = float(event.event_ts or 0.0)
    if is_fresh_buy:
        return (0, -event_ts, event.event_id)
    if event.is_buy:
        return (1, -event_ts, event.event_id)
    return (2, -event_ts, event.event_id)


def _poll_processing_reserve_s(max_poll_runtime_s: float) -> float:
    """Reserve enough poll budget to process fresh BUY evidence after parallel fetches."""

    runtime_s = max(0.0, float(max_poll_runtime_s or 0.0))
    if runtime_s <= 0.0:
        return 0.0
    return min(5.0, max(0.25, runtime_s * 0.20))


def _fresh_buy_event_count(events: Iterable[WalletEvent], *, max_event_age_s: float) -> int:
    return sum(
        1
        for event in events
        if event.is_buy and event.age_s is not None and float(event.age_s) <= float(max_event_age_s)
    )


def _fresh_buy_events(events: Iterable[WalletEvent], *, max_event_age_s: float) -> list[WalletEvent]:
    return [
        event
        for event in events
        if event.is_buy and event.age_s is not None and float(event.age_s) <= float(max_event_age_s)
    ]


def _report_fresh_buy_rows_le_10s(report: dict[str, Any]) -> int:
    freshness = report.get("freshness_diagnosis") if isinstance(report, dict) else None
    if not isinstance(freshness, dict):
        return 0
    try:
        return int(freshness.get("fresh_buy_rows_le_10s") or 0)
    except (TypeError, ValueError):
        return 0


def _evidence_status_counts(enriched_moves: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    def nested_status(move: dict[str, Any], *keys: str) -> str:
        value: Any = move
        for key in keys:
            if not isinstance(value, dict):
                return "MISSING"
            value = value.get(key)
        return str(value or "MISSING")

    clob_admission_relevant: Counter[str] = Counter()
    clob_stale_or_non_admission: Counter[str] = Counter()
    for move in enriched_moves:
        evidence = move.get("tracking_evidence") if isinstance(move.get("tracking_evidence"), dict) else {}
        clob = evidence.get("clob_book") if isinstance(evidence.get("clob_book"), dict) else {}
        status = str(clob.get("status") or "MISSING")
        if clob.get("admission_relevant") is True:
            clob_admission_relevant[status] += 1
        elif clob.get("admission_relevant") is False:
            clob_stale_or_non_admission[status] += 1

    return {
        "mirror_status": dict(Counter(nested_status(move, "mirror_result", "mirror_status") for move in enriched_moves)),
        "wallet_api": dict(Counter(nested_status(move, "tracking_evidence", "wallet_api", "status") for move in enriched_moves)),
        "market_ws_preconfirm": dict(
            Counter(nested_status(move, "tracking_evidence", "market_ws_preconfirm", "status") for move in enriched_moves)
        ),
        "clob_book": dict(Counter(nested_status(move, "tracking_evidence", "clob_book", "status") for move in enriched_moves)),
        "clob_book_admission_relevant": dict(clob_admission_relevant),
        "clob_book_stale_or_non_admission": dict(clob_stale_or_non_admission),
        "onchain": dict(Counter(nested_status(move, "tracking_evidence", "onchain", "status") for move in enriched_moves)),
    }


def _wallet_freshness_diagnosis(
    events: list[WalletEvent],
    *,
    now_ts: float,
    max_event_age_s: float,
    max_fetch_duration_s: float,
    fetch_report: dict[str, Any],
    dedupe_exhausted: bool,
) -> dict[str, Any]:
    buy_events = [event for event in events if event.is_buy]
    buy_ages = [float(event.age_s) for event in buy_events if event.age_s is not None]
    buy_event_ts_values = [float(event.event_ts) for event in buy_events if event.event_ts is not None]
    latest_buy_ts = max(buy_event_ts_values) if buy_event_ts_values else None
    latest_buy_lag_s = round(max(0.0, now_ts - latest_buy_ts), 6) if latest_buy_ts is not None else None
    fetch_duration = fetch_report.get("fetch_duration_s")
    try:
        fetch_duration_float = float(fetch_duration) if fetch_duration is not None else None
    except (TypeError, ValueError):
        fetch_duration_float = None
    fresh_buy_rows_le_10s = sum(1 for age in buy_ages if age <= 10.0)
    fresh_buy_rows_le_30s = sum(1 for age in buy_ages if age <= 30.0)
    source_feed_delayed = bool(
        buy_events
        and fresh_buy_rows_le_10s == 0
        and latest_buy_lag_s is not None
        and latest_buy_lag_s > float(max_event_age_s)
        and (fetch_duration_float is None or fetch_duration_float <= float(max_fetch_duration_s))
    )
    return {
        "latest_buy_event_ts": latest_buy_ts,
        "latest_buy_event_iso": _iso_from_ts(latest_buy_ts) if latest_buy_ts is not None else None,
        "latest_buy_event_lag_s": latest_buy_lag_s,
        "fresh_buy_rows_le_10s": fresh_buy_rows_le_10s,
        "fresh_buy_rows_le_30s": fresh_buy_rows_le_30s,
        "stale_buy_rows_gt_300s": sum(1 for age in buy_ages if age > 300.0),
        "buy_event_age_p95_s": _percentile(buy_ages, 95.0),
        "buy_event_age_max_s": round(max(buy_ages), 6) if buy_ages else None,
        "buy_rows": len(buy_events),
        "dedupe_exhausted": bool(dedupe_exhausted),
        "source_feed_delayed": source_feed_delayed,
        "data_api_fetch_duration_s": fetch_duration_float,
        "max_event_age_s": float(max_event_age_s),
        "max_fetch_duration_s": float(max_fetch_duration_s),
    }


def _current_poll_diagnostics(
    *,
    wallet_reports: list[dict[str, Any]],
    new_events: list[WalletEvent],
    raw_intents: list[CopyIntent],
    enriched_intents: list[CopyIntent],
    current_poll_copy_efficiency: dict[str, Any],
    profit_filter_decisions: dict[str, tuple[bool, str]],
    copyability_decisions: dict[str, CopyabilityDecision],
) -> dict[str, Any]:
    """Explain why the current poll did or did not produce live-admission truth.

    These counters are diagnostic only.  They deliberately do not relax the
    paper/live proof gates; they make empty current polls distinguishable from
    source-route failure, dedupe exhaustion, policy filtering, and CLOB fill
    failure.
    """

    def sum_report_int(key: str) -> int:
        total = 0
        for report in wallet_reports:
            if not isinstance(report, dict):
                continue
            try:
                total += int(report.get(key) or 0)
            except (TypeError, ValueError):
                continue
        return total

    def sum_nested_int(key: str, nested_key: str) -> int:
        total = 0
        for report in wallet_reports:
            nested = report.get(key) if isinstance(report, dict) else None
            if not isinstance(nested, dict):
                continue
            try:
                total += int(nested.get(nested_key) or 0)
            except (TypeError, ValueError):
                continue
        return total

    def source_freshness_summary() -> dict[str, dict[str, Any]]:
        by_source: dict[str, dict[str, Any]] = {}
        for report in wallet_reports:
            if not isinstance(report, dict):
                continue
            freshness_by_source = report.get("source_freshness_by_source")
            if not isinstance(freshness_by_source, dict):
                continue
            duration_by_source = report.get("source_fetch_duration_s_by_source")
            if not isinstance(duration_by_source, dict):
                duration_by_source = {}
            route_status_by_source = report.get("source_route_status_by_source")
            if not isinstance(route_status_by_source, dict):
                route_status_by_source = {}
            for source, freshness in freshness_by_source.items():
                if not isinstance(freshness, dict):
                    continue
                source_key = str(source)
                row = by_source.setdefault(
                    source_key,
                    {
                        "wallet_reports": 0,
                        "buy_rows": 0,
                        "fresh_buy_rows_le_10s": 0,
                        "fresh_buy_rows_le_30s": 0,
                        "stale_buy_rows_gt_300s": 0,
                        "source_feed_delayed_10s_wallets": 0,
                        "source_feed_delayed_30s_wallets": 0,
                        "latest_buy_event_lag_s_min": None,
                        "latest_buy_event_lag_s_max": None,
                        "fetch_duration_s_max": None,
                        "route_status_counts": {},
                    },
                )
                row["wallet_reports"] += 1
                for key in (
                    "buy_rows",
                    "fresh_buy_rows_le_10s",
                    "fresh_buy_rows_le_30s",
                    "stale_buy_rows_gt_300s",
                ):
                    try:
                        row[key] += int(freshness.get(key) or 0)
                    except (TypeError, ValueError):
                        pass
                if freshness.get("source_feed_delayed_10s"):
                    row["source_feed_delayed_10s_wallets"] += 1
                if freshness.get("source_feed_delayed_30s"):
                    row["source_feed_delayed_30s_wallets"] += 1
                lag_s = float_or_none(freshness.get("latest_buy_event_lag_s"))
                if lag_s is not None:
                    current_min = row["latest_buy_event_lag_s_min"]
                    current_max = row["latest_buy_event_lag_s_max"]
                    row["latest_buy_event_lag_s_min"] = lag_s if current_min is None else min(float(current_min), lag_s)
                    row["latest_buy_event_lag_s_max"] = lag_s if current_max is None else max(float(current_max), lag_s)
                duration_s = float_or_none(duration_by_source.get(source_key))
                if duration_s is not None:
                    current_duration = row["fetch_duration_s_max"]
                    row["fetch_duration_s_max"] = (
                        duration_s if current_duration is None else max(float(current_duration), duration_s)
                    )
                route_status = route_status_by_source.get(source_key)
                if route_status not in (None, ""):
                    route_counts = row["route_status_counts"]
                    route_key = str(route_status)
                    route_counts[route_key] = int(route_counts.get(route_key) or 0) + 1
        for row in by_source.values():
            for key in ("latest_buy_event_lag_s_min", "latest_buy_event_lag_s_max", "fetch_duration_s_max"):
                if row.get(key) is not None:
                    row[key] = round(float(row[key]), 6)
            row["route_status_counts"] = dict(sorted(row["route_status_counts"].items()))
        return dict(sorted(by_source.items()))

    def float_or_none(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def event_source_age_s(event: WalletEvent) -> float | None:
        return float_or_none(event.age_s)

    def is_source_fresh(event: WalletEvent, max_age_s: float) -> bool:
        age_s = event_source_age_s(event)
        return bool(age_s is not None and age_s <= float(max_age_s))

    def accepted_by_profit(event: WalletEvent) -> bool:
        return bool(profit_filter_decisions.get(event.event_id, (True, "accepted"))[0])

    def accepted_by_copyability(event: WalletEvent) -> bool:
        return bool(
            copyability_decisions.get(
                event.event_id,
                CopyabilityDecision(False, "missing", "", {}),
            ).accepted
        )

    def report_raw_rows(report: dict[str, Any]) -> int:
        try:
            return int(report.get("raw_rows") or 0)
        except (TypeError, ValueError):
            return 0

    def route_statuses(report: dict[str, Any]) -> list[str]:
        statuses: list[str] = []
        status_by_source = report.get("source_route_status_by_source")
        if isinstance(status_by_source, dict):
            statuses.extend(str(value) for value in status_by_source.values() if value not in (None, ""))
        reports_by_source = report.get("source_route_reports_by_source")
        if isinstance(reports_by_source, dict):
            for row in reports_by_source.values():
                if isinstance(row, dict) and row.get("status") not in (None, ""):
                    statuses.append(str(row.get("status")))
        for error in report.get("api_errors") or []:
            if not isinstance(error, dict):
                continue
            route_report = error.get("route_report")
            if isinstance(route_report, dict) and route_report.get("status") not in (None, ""):
                statuses.append(str(route_report.get("status")))
        api_error = report.get("api_error")
        if isinstance(api_error, dict):
            route_report = api_error.get("route_report")
            if isinstance(route_report, dict) and route_report.get("status") not in (None, ""):
                statuses.append(str(route_report.get("status")))
        return statuses

    def route_classes(report: dict[str, Any]) -> list[str]:
        classes: list[str] = []
        reports_by_source = report.get("source_route_reports_by_source")
        if isinstance(reports_by_source, dict):
            for row in reports_by_source.values():
                if isinstance(row, dict) and row.get("route_class") not in (None, ""):
                    classes.append(str(row.get("route_class")))
        for error in report.get("api_errors") or []:
            if not isinstance(error, dict):
                continue
            route_report = error.get("route_report")
            if isinstance(route_report, dict) and route_report.get("route_class") not in (None, ""):
                classes.append(str(route_report.get("route_class")))
        api_error = report.get("api_error")
        if isinstance(api_error, dict):
            route_report = api_error.get("route_report")
            if isinstance(route_report, dict) and route_report.get("route_class") not in (None, ""):
                classes.append(str(route_report.get("route_class")))
        return classes

    def report_has_route_reset(report: dict[str, Any]) -> bool:
        if any(route_class == "DIRECT_RESET" for route_class in route_classes(report)):
            return True
        if any(status in {"CONNECTION_RESET", "POLYMARKET_ROUTE_RESET", "TRANSPORT_ERROR"} for status in route_statuses(report)):
            return True
        for error in report.get("api_errors") or []:
            if not isinstance(error, dict):
                continue
            route_report = error.get("route_report")
            if not isinstance(route_report, dict):
                continue
            for attempt in route_report.get("attempts") or []:
                if not isinstance(attempt, dict):
                    continue
                text = json.dumps(attempt, sort_keys=True).lower()
                if "connection reset" in text or "connectionreseterror" in text:
                    return True
        return False

    def report_has_route_error(report: dict[str, Any]) -> bool:
        classes = route_classes(report)
        if any(route_class not in {"DIRECT_PASS", "ROUTE_RECOVERED_DEGRADED", "PROXY_RECOVERED_DEGRADED"} for route_class in classes):
            return True
        statuses = route_statuses(report)
        if any(status not in {"PASS", "HTTP_NON_2XX"} for status in statuses):
            return True
        return bool(report_has_route_reset(report))

    def report_has_degraded_recovery(report: dict[str, Any]) -> bool:
        return any(
            route_class in {"ROUTE_RECOVERED_DEGRADED", "PROXY_RECOVERED_DEGRADED"}
            for route_class in route_classes(report)
        )

    def report_has_override_recovery(report: dict[str, Any]) -> bool:
        override_counts = report.get("source_base_override_counts")
        override_configured = isinstance(override_counts, dict) and any(int(value or 0) > 0 for value in override_counts.values())
        if not override_configured:
            return False
        statuses = route_statuses(report)
        return bool(statuses) and any(status == "PASS" for status in statuses) and not report_has_route_error(report)

    summary = current_poll_copy_efficiency.get("summary") if isinstance(current_poll_copy_efficiency, dict) else {}
    if not isinstance(summary, dict):
        summary = {}
    source_error_wallets = sum(
        1
        for report in wallet_reports
        if isinstance(report, dict)
        and (
            report.get("api_error")
            or report.get("api_errors")
            or "partial_data_api_source_error" in (report.get("blockers") or [])
        )
    )
    route_error_wallets = sum(1 for report in wallet_reports if isinstance(report, dict) and report_has_route_error(report))
    route_reset_wallets = sum(1 for report in wallet_reports if isinstance(report, dict) and report_has_route_reset(report))
    route_override_recovered_wallets = sum(
        1 for report in wallet_reports if isinstance(report, dict) and report_has_override_recovery(report)
    )
    route_degraded_recovered_wallets = sum(
        1 for report in wallet_reports if isinstance(report, dict) and report_has_degraded_recovery(report)
    )
    route_status_counts = Counter(
        status
        for report in wallet_reports
        if isinstance(report, dict)
        for status in route_statuses(report)
    )
    route_class_counts = Counter(
        route_class
        for report in wallet_reports
        if isinstance(report, dict)
        for route_class in route_classes(report)
    )
    route_pass_rows = sum(
        report_raw_rows(report)
        for report in wallet_reports
        if isinstance(report, dict) and route_statuses(report) and all(status == "PASS" for status in route_statuses(report))
    )
    route_error_rows = sum(
        report_raw_rows(report)
        for report in wallet_reports
        if isinstance(report, dict) and report_has_route_error(report)
    )
    route_override_recovered_rows = sum(
        report_raw_rows(report)
        for report in wallet_reports
        if isinstance(report, dict) and report_has_override_recovery(report)
    )
    route_degraded_recovered_rows = sum(
        report_raw_rows(report)
        for report in wallet_reports
        if isinstance(report, dict) and report_has_degraded_recovery(report)
    )
    route_usable_rows = max(route_pass_rows, route_override_recovered_rows, route_degraded_recovered_rows)
    new_buy_events = [event for event in new_events if event.is_buy]
    source_fresh_buy_events_le_10s = [event for event in new_buy_events if is_source_fresh(event, 10.0)]
    source_fresh_buy_events_le_30s = [event for event in new_buy_events if is_source_fresh(event, 30.0)]
    profit_accepted_buy_events = sum(1 for event in new_buy_events if accepted_by_profit(event))
    profit_accepted_fresh_buy_events_le_10s = sum(
        1 for event in source_fresh_buy_events_le_10s if accepted_by_profit(event)
    )
    profit_accepted_fresh_buy_events_le_30s = sum(
        1 for event in source_fresh_buy_events_le_30s if accepted_by_profit(event)
    )
    copyability_accepted_buy_events = sum(1 for event in new_buy_events if accepted_by_copyability(event))
    copyability_accepted_fresh_buy_events_le_10s = sum(
        1 for event in source_fresh_buy_events_le_10s if accepted_by_copyability(event)
    )
    copyability_accepted_fresh_buy_events_le_30s = sum(
        1 for event in source_fresh_buy_events_le_30s if accepted_by_copyability(event)
    )
    copyability_reason_counts: Counter[str] = Counter()
    copyability_rejected_reason_counts: Counter[str] = Counter()
    copyability_blocker_counts: Counter[str] = Counter()
    copyability_rejected_blocker_counts: Counter[str] = Counter()
    copyability_rejected_samples: list[dict[str, Any]] = []
    fresh_at_fetch_start_stale_at_decision_rows = 0
    fresh_at_fetch_start_stale_at_decision_samples: list[dict[str, Any]] = []
    stale_before_fetch_rows = 0
    stale_before_fetch_samples: list[dict[str, Any]] = []
    copyability_staleness_origin_counts: Counter[str] = Counter()
    book_timing_issue_counts: Counter[str] = Counter()
    book_timing_samples: list[dict[str, Any]] = []
    for event in new_buy_events:
        decision = copyability_decisions.get(event.event_id)
        details: dict[str, Any] = {}
        event_age_s: float | None = None
        max_event_age_s: float | None = None
        fetch_duration_s: float | None = None
        explicit_age_at_fetch_start_s: float | None = None
        if not isinstance(decision, CopyabilityDecision):
            reason = "missing_copyability_decision"
            accepted = False
            decision_blockers = [reason]
        else:
            reason = str(decision.reason or ("accepted" if decision.accepted else "copyability_rejected"))
            accepted = bool(decision.accepted)
            details = decision.details if isinstance(decision.details, dict) else {}
            raw_blockers = details.get("blockers") if isinstance(details.get("blockers"), list) else []
            decision_blockers = [str(blocker) for blocker in raw_blockers if blocker]
            if not decision_blockers:
                decision_blockers = [reason]
            event_age_s = float_or_none(details.get("event_age_s"))
            max_event_age_s = float_or_none(details.get("max_event_age_s"))
            fetch_duration_s = float_or_none(
                details.get("source_fetch_duration_s")
                if details.get("source_fetch_duration_s") is not None
                else details.get("wallet_fetch_duration_s")
            )
            if fetch_duration_s is None:
                fetch_duration_s = float_or_none(details.get("wallet_batch_fetch_duration_s"))
            explicit_age_at_fetch_start_s = float_or_none(details.get("event_age_at_source_fetch_start_s"))
            if (
                not accepted
                and "event_age_above_cap" in decision_blockers
                and event_age_s is not None
                and max_event_age_s is not None
                and (fetch_duration_s is not None or explicit_age_at_fetch_start_s is not None)
            ):
                age_at_fetch_start_s = (
                    explicit_age_at_fetch_start_s
                    if explicit_age_at_fetch_start_s is not None
                    else max(0.0, event_age_s - float(fetch_duration_s))
                )
                if age_at_fetch_start_s > max_event_age_s:
                    copyability_staleness_origin_counts["stale_before_wallet_fetch"] += 1
                    stale_before_fetch_rows += 1
                    if len(stale_before_fetch_samples) < 5:
                        stale_before_fetch_samples.append(
                            {
                                "event_id": event.event_id,
                                "wallet": event.source_wallet,
                                "event_age_s": round(event_age_s, 6),
                                "fetch_duration_s": round(fetch_duration_s, 6) if fetch_duration_s is not None else None,
                                "age_at_fetch_start_s": round(age_at_fetch_start_s, 6),
                                "age_at_fetch_start_basis": (
                                    "event_age_at_source_fetch_start_s"
                                    if explicit_age_at_fetch_start_s is not None
                                    else "event_age_minus_fetch_duration"
                                ),
                                "age_at_fetch_start_over_cap_s": round(
                                    age_at_fetch_start_s - max_event_age_s,
                                    6,
                                ),
                                "max_event_age_s": round(max_event_age_s, 6),
                                "reason": reason,
                            }
                        )
                elif "fetch_duration_above_cap" in decision_blockers and age_at_fetch_start_s <= max_event_age_s < event_age_s:
                    copyability_staleness_origin_counts["fresh_at_fetch_start_stale_at_copyability"] += 1
                    fetch_stale_blocker = "fresh_at_fetch_start_stale_at_copyability"
                    decision_blockers.append(fetch_stale_blocker)
                    fresh_at_fetch_start_stale_at_decision_rows += 1
                    if len(fresh_at_fetch_start_stale_at_decision_samples) < 5:
                        fresh_at_fetch_start_stale_at_decision_samples.append(
                            {
                                "event_id": event.event_id,
                                "wallet": event.source_wallet,
                                "event_age_s": round(event_age_s, 6),
                                "fetch_duration_s": round(fetch_duration_s, 6) if fetch_duration_s is not None else None,
                                "age_at_fetch_start_s": round(age_at_fetch_start_s, 6),
                                "age_at_fetch_start_basis": (
                                    "event_age_at_source_fetch_start_s"
                                    if explicit_age_at_fetch_start_s is not None
                                    else "event_age_minus_fetch_duration"
                                ),
                                "max_event_age_s": round(max_event_age_s, 6),
                                "reason": reason,
                            }
                        )
                else:
                    copyability_staleness_origin_counts["event_age_above_cap_origin_unclassified"] += 1
        book_timing_blockers = {
            "no_ask_liquidity",
            "depth_below_min_fill_ratio",
            "best_ask_above_slippage_cap",
            "clob_price_above_slippage_cap",
            "book_closed_or_no_liquidity_analysis",
        }
        has_book_timing_reject = (
            not accepted
            and bool(book_timing_blockers.intersection(decision_blockers))
            or (not accepted and str(details.get("clob_blocking_reason") or "") in book_timing_blockers)
        )
        if has_book_timing_reject:
            if explicit_age_at_fetch_start_s is not None:
                age_at_fetch_start_s = explicit_age_at_fetch_start_s
                age_at_fetch_start_basis = "event_age_at_source_fetch_start_s"
            elif event_age_s is not None and fetch_duration_s is not None:
                age_at_fetch_start_s = max(0.0, event_age_s - fetch_duration_s)
                age_at_fetch_start_basis = "event_age_minus_fetch_duration"
            else:
                age_at_fetch_start_s = None
                age_at_fetch_start_basis = None
            if age_at_fetch_start_s is None or max_event_age_s is None:
                timing_class = "book_timing_unknown"
            elif age_at_fetch_start_s > max_event_age_s:
                timing_class = "source_trade_stale_before_book_fetch"
            elif event_age_s is not None and event_age_s > max_event_age_s:
                timing_class = "source_trade_fresh_at_fetch_start_stale_at_book_decision"
            else:
                timing_class = "source_trade_fresh_during_book_check"
            book_timing_issue_counts[timing_class] += 1
            if details.get("clob_blocking_reason"):
                book_timing_issue_counts[f"clob_blocking_reason:{details.get('clob_blocking_reason')}"] += 1
            for blocker in decision_blockers:
                if blocker in book_timing_blockers:
                    book_timing_issue_counts[f"blocker:{blocker}"] += 1
            if len(book_timing_samples) < 5:
                book_timing_samples.append(
                    {
                        "event_id": event.event_id,
                        "wallet": event.source_wallet,
                        "reason": reason,
                        "blockers": decision_blockers[:8],
                        "timing_class": timing_class,
                        "event_age_s": round(event_age_s, 6) if event_age_s is not None else None,
                        "max_event_age_s": round(max_event_age_s, 6) if max_event_age_s is not None else None,
                        "fetch_duration_s": round(fetch_duration_s, 6) if fetch_duration_s is not None else None,
                        "age_at_wallet_fetch_start_s": (
                            round(age_at_fetch_start_s, 6) if age_at_fetch_start_s is not None else None
                        ),
                        "age_at_wallet_fetch_start_basis": age_at_fetch_start_basis,
                        "source_price": details.get("source_price"),
                        "clob_best_ask": details.get("clob_best_ask"),
                        "clob_max_copy_price": details.get("clob_max_copy_price"),
                        "clob_blocking_reason": details.get("clob_blocking_reason"),
                        "clob_fill_ratio": details.get("clob_fill_ratio"),
                        "clob_fillable_usd": details.get("clob_fillable_usd"),
                        "copy_size_usd": details.get("copy_size_usd"),
                        "min_slippage_to_fill_bps": details.get("min_slippage_to_fill_bps"),
                    }
                )
        if not accepted and len(copyability_rejected_samples) < 5:
            sample_event_age_s = float_or_none(details.get("event_age_s"))
            sample_max_event_age_s = float_or_none(details.get("max_event_age_s"))
            sample_fetch_duration_s = float_or_none(
                details.get("source_fetch_duration_s")
                if details.get("source_fetch_duration_s") is not None
                else details.get("wallet_fetch_duration_s")
            )
            if sample_fetch_duration_s is None:
                sample_fetch_duration_s = float_or_none(details.get("wallet_batch_fetch_duration_s"))
            sample_explicit_age_at_fetch_start_s = float_or_none(details.get("event_age_at_source_fetch_start_s"))
            sample_age_at_fetch_start_s = (
                round(sample_explicit_age_at_fetch_start_s, 6)
                if sample_explicit_age_at_fetch_start_s is not None
                else (
                    round(max(0.0, sample_event_age_s - sample_fetch_duration_s), 6)
                    if sample_event_age_s is not None and sample_fetch_duration_s is not None
                    else None
                )
            )
            sample_age_at_fetch_start_basis = (
                "event_age_at_source_fetch_start_s"
                if sample_explicit_age_at_fetch_start_s is not None
                else (
                    "event_age_minus_fetch_duration"
                    if sample_event_age_s is not None and sample_fetch_duration_s is not None
                    else None
                )
            )
            sample_event_age_over_cap_s = (
                round(max(0.0, sample_event_age_s - sample_max_event_age_s), 6)
                if sample_event_age_s is not None and sample_max_event_age_s is not None
                else None
            )
            copyability_rejected_samples.append(
                {
                    "event_id": event.event_id,
                    "wallet": event.source_wallet,
                    "reason": reason,
                    "blockers": decision_blockers[:8],
                    "event_age_s": details.get("event_age_s"),
                    "max_event_age_s": details.get("max_event_age_s"),
                    "event_age_over_cap_s": sample_event_age_over_cap_s,
                    "age_at_wallet_fetch_start_s": sample_age_at_fetch_start_s,
                    "age_at_wallet_fetch_start_basis": sample_age_at_fetch_start_basis,
                    "wallet_fetch_duration_s": details.get("wallet_fetch_duration_s"),
                    "source_fetch_duration_s": details.get("source_fetch_duration_s"),
                    "wallet_batch_fetch_duration_s": details.get("wallet_batch_fetch_duration_s"),
                    "clob_blocking_reason": details.get("clob_blocking_reason"),
                    "clob_best_ask": details.get("clob_best_ask"),
                    "clob_max_copy_price": details.get("clob_max_copy_price"),
                    "clob_fill_ratio": details.get("clob_fill_ratio"),
                    "clob_fillable_usd": details.get("clob_fillable_usd"),
                    "copy_size_usd": details.get("copy_size_usd"),
                    "source_price": details.get("source_price"),
                    "min_slippage_to_fill_bps": details.get("min_slippage_to_fill_bps"),
                }
            )
        copyability_reason_counts[reason] += 1
        if not accepted:
            copyability_rejected_reason_counts[reason] += 1
        for blocker in decision_blockers:
            copyability_blocker_counts[blocker] += 1
            if not accepted:
                copyability_rejected_blocker_counts[blocker] += 1
    top_copyability_reject_reason = None
    if copyability_rejected_reason_counts:
        top_copyability_reject_reason = sorted(
            copyability_rejected_reason_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]
    top_copyability_reject_blocker = None
    if copyability_rejected_blocker_counts:
        top_copyability_reject_blocker = sorted(
            copyability_rejected_blocker_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]
    reported_raw_fresh_buy_rows_le_10s = sum_nested_int("freshness_diagnosis", "fresh_buy_rows_le_10s")
    reported_raw_fresh_buy_rows_le_30s = sum_nested_int("freshness_diagnosis", "fresh_buy_rows_le_30s")
    raw_fresh_buy_rows_le_10s = reported_raw_fresh_buy_rows_le_10s or len(source_fresh_buy_events_le_10s)
    raw_fresh_buy_rows_le_30s = reported_raw_fresh_buy_rows_le_30s or len(source_fresh_buy_events_le_30s)
    source_freshness_by_source = source_freshness_summary()
    runtime_limited_events_skipped = sum_report_int("runtime_limited_events_skipped")
    runtime_limited_buy_events_skipped = sum_report_int("runtime_limited_buy_events_skipped")
    runtime_limited_fresh_buy_events_skipped = sum_report_int("runtime_limited_fresh_buy_events_skipped")
    runtime_limited_wallet_reports = sum(
        1
        for report in wallet_reports
        if isinstance(report, dict) and report.get("poll_deadline_exceeded_before_event_processing")
    )
    runtime_limited_reason_counts = Counter(
        str(report.get("poll_runtime_skip_reason") or "poll_deadline_exceeded_before_event_processing")
        for report in wallet_reports
        if isinstance(report, dict) and report.get("poll_deadline_exceeded_before_event_processing")
    )
    copyability_rejected_fresh_buy_events_le_10s = (
        sum(1 for event in source_fresh_buy_events_le_10s if not accepted_by_copyability(event))
    )
    copyability_rejected_fresh_buy_events_le_30s = (
        sum(1 for event in source_fresh_buy_events_le_30s if not accepted_by_copyability(event))
    )
    raw_intent_event_ids = {str(intent.source_event_id) for intent in raw_intents}
    enriched_intent_event_ids = {str(intent.source_event_id) for intent in enriched_intents}
    raw_intent_fresh_buy_events_le_10s = sum(
        1 for event in source_fresh_buy_events_le_10s if str(event.event_id) in raw_intent_event_ids
    )
    enriched_intent_fresh_buy_events_le_10s = sum(
        1 for event in source_fresh_buy_events_le_10s if str(event.event_id) in enriched_intent_event_ids
    )
    raw_intent_fresh_buy_events_le_30s = sum(
        1 for event in source_fresh_buy_events_le_30s if str(event.event_id) in raw_intent_event_ids
    )
    enriched_intent_fresh_buy_events_le_30s = sum(
        1 for event in source_fresh_buy_events_le_30s if str(event.event_id) in enriched_intent_event_ids
    )
    required_buy_copy_events = int(summary.get("required_buy_copy_events") or 0)
    clob_filled_buy_copy_events = int(summary.get("clob_filled_buy_copy_events") or 0)
    fresh_buy_loss_stage = None
    if raw_fresh_buy_rows_le_10s > 0:
        if not source_fresh_buy_events_le_10s:
            fresh_buy_loss_stage = "dedupe_or_retry_deferred_before_policy"
        elif profit_accepted_fresh_buy_events_le_10s <= 0:
            fresh_buy_loss_stage = "profit_policy_filtered_source_fresh_buys"
        elif copyability_accepted_fresh_buy_events_le_10s <= 0:
            fresh_buy_loss_stage = "copyability_filtered_source_fresh_buys"
        elif raw_intent_fresh_buy_events_le_10s > 0 and enriched_intent_fresh_buy_events_le_10s <= 0:
            fresh_buy_loss_stage = "intent_dedupe_filtered_source_fresh_buys"
        elif enriched_intent_fresh_buy_events_le_10s > 0 and clob_filled_buy_copy_events <= 0:
            fresh_buy_loss_stage = "clob_fill_failed_after_source_fresh_intent"
        else:
            fresh_buy_loss_stage = "source_fresh_buy_reached_copy_path"
    blockers: list[str] = []
    if sum_report_int("raw_rows") <= 0:
        blockers.append("raw_source_rows_zero")
    if source_error_wallets:
        blockers.append("source_fetch_errors_present")
    if route_reset_wallets:
        blockers.append("current_poll_source_route_reset")
    if route_error_wallets:
        blockers.append("current_poll_source_route_partial")
    if route_error_wallets and route_usable_rows > 0 and raw_fresh_buy_rows_le_10s <= 0:
        blockers.append("current_poll_usable_route_rows_no_fresh_buy")
    if route_degraded_recovered_wallets:
        blockers.append("current_poll_source_route_recovered_degraded")
    if runtime_limited_events_skipped or runtime_limited_wallet_reports:
        blockers.append("current_poll_runtime_limited_before_event_processing")
    if sum_report_int("events_seen") > 0 and sum_report_int("new_events") <= 0:
        blockers.append("all_source_rows_deduped_or_deferred")
    if new_buy_events and profit_accepted_buy_events <= 0:
        blockers.append("all_new_buys_filtered_by_profit_policy")
    if new_buy_events and copyability_accepted_buy_events <= 0:
        blockers.append("all_new_buys_filtered_by_copyability")
    if stale_before_fetch_rows:
        blockers.append("current_poll_source_feed_pre_fetch_stale")
    if book_timing_issue_counts:
        blockers.append("current_poll_source_trade_book_timing_unverified")
    if raw_intents and not enriched_intents:
        blockers.append("raw_intents_deduped_to_zero")
    if required_buy_copy_events > 0 and clob_filled_buy_copy_events < required_buy_copy_events:
        blockers.append("required_buys_not_all_clob_filled")
    if not blockers and required_buy_copy_events <= 0:
        blockers.append("no_current_required_buy_copy_events")
    zero_current_poll_root_cause = None
    if required_buy_copy_events <= 0:
        if route_reset_wallets:
            zero_current_poll_root_cause = "route_reset"
        elif runtime_limited_fresh_buy_events_skipped > 0:
            zero_current_poll_root_cause = "runtime_limited_fresh_buy_before_policy"
        elif route_error_wallets and route_usable_rows > 0 and raw_fresh_buy_rows_le_10s <= 0:
            zero_current_poll_root_cause = "source_latency_no_fresh_buy_on_usable_route_rows"
        elif route_error_wallets:
            zero_current_poll_root_cause = "source_route_partial"
        elif runtime_limited_events_skipped or runtime_limited_wallet_reports:
            zero_current_poll_root_cause = "runtime_limited_before_event_processing"
        elif sum_report_int("raw_rows") <= 0:
            zero_current_poll_root_cause = "raw_source_rows_zero"
        elif sum_report_int("events_seen") > 0 and sum_report_int("new_events") <= 0:
            zero_current_poll_root_cause = "dedupe_exhausted"
        elif new_buy_events and profit_accepted_buy_events <= 0:
            zero_current_poll_root_cause = "profit_policy_filtered"
        elif stale_before_fetch_rows > 0:
            zero_current_poll_root_cause = "source_feed_pre_fetch_stale"
        elif fresh_at_fetch_start_stale_at_decision_rows > 0:
            zero_current_poll_root_cause = "fresh_buy_staled_during_source_fetch"
        elif new_buy_events and copyability_accepted_buy_events <= 0:
            zero_current_poll_root_cause = "copyability_filtered"
        elif raw_intents and not enriched_intents:
            zero_current_poll_root_cause = "raw_intents_deduped_to_zero"
        else:
            zero_current_poll_root_cause = "no_required_buy_copy_events"
    current_poll_ladder = {
        "wallets_polled": len([report for report in wallet_reports if isinstance(report, dict)]),
        "source_route_pass_rows": route_pass_rows,
        "source_route_error_rows": route_error_rows,
        "source_route_usable_rows": route_usable_rows,
        "source_route_override_recovered_rows": route_override_recovered_rows,
        "source_route_degraded_recovered_rows": route_degraded_recovered_rows,
        "raw_rows": sum_report_int("raw_rows"),
        "normalized_events": sum_report_int("events_seen"),
        "runtime_limited_events_skipped": runtime_limited_events_skipped,
        "runtime_limited_buy_events_skipped": runtime_limited_buy_events_skipped,
        "runtime_limited_fresh_buy_events_skipped": runtime_limited_fresh_buy_events_skipped,
        "runtime_limited_wallet_reports": runtime_limited_wallet_reports,
        "runtime_limited_reason_counts": dict(sorted(runtime_limited_reason_counts.items())),
        "fresh_buy_rows_le_10s": raw_fresh_buy_rows_le_10s,
        "fresh_after_dedupe_buy_rows_le_10s": len(source_fresh_buy_events_le_10s),
        "fresh_profit_policy_buy_rows_le_10s": profit_accepted_fresh_buy_events_le_10s,
        "fresh_copyability_buy_rows_le_10s": copyability_accepted_fresh_buy_events_le_10s,
        "after_dedupe_events": sum_report_int("new_events"),
        "after_dedupe_buy_rows": len(new_buy_events),
        "profit_policy_buy_rows": profit_accepted_buy_events,
        "copyability_buy_rows": copyability_accepted_buy_events,
        "copyability_rejected_buy_rows": sum(copyability_rejected_reason_counts.values()),
        "copy_intents": len(enriched_intents),
        "paper_orders": int(summary.get("paper_orders") or summary.get("filled_buy_copy_events") or 0),
        "clob_filled_orders": clob_filled_buy_copy_events,
    }
    return {
        "role": "diagnostic_only_not_live_admission_truth",
        "current_poll_ladder": current_poll_ladder,
        "zero_current_poll_root_cause": zero_current_poll_root_cause,
        "raw_source_rows_seen": sum_report_int("raw_rows"),
        "normalized_source_rows_seen": sum_report_int("events_seen"),
        "fresh_buy_rows_le_10s": raw_fresh_buy_rows_le_10s,
        "fresh_buy_rows_le_30s": raw_fresh_buy_rows_le_30s,
        "current_poll_source_freshness_by_source": source_freshness_by_source,
        "fresh_buy_loss_stage": fresh_buy_loss_stage,
        "deduped_rows": sum_report_int("seen_duplicate_events") + sum_report_int("pending_duplicate_events"),
        "deduped_fresh_rows": sum_report_int("seen_duplicate_fresh_events")
        + sum_report_int("pending_duplicate_fresh_events"),
        "failed_retry_deferred_rows": sum_report_int("failed_retry_deferred_events"),
        "failed_retry_deferred_fresh_rows": sum_report_int("failed_retry_deferred_fresh_events"),
        "new_rows_after_dedupe": sum_report_int("new_events"),
        "new_buy_rows_after_dedupe": len(new_buy_events),
        "new_source_fresh_buy_rows_after_dedupe_le_10s": len(source_fresh_buy_events_le_10s),
        "new_source_fresh_buy_rows_after_dedupe_le_30s": len(source_fresh_buy_events_le_30s),
        "new_profit_policy_compatible_buy_rows": profit_accepted_buy_events,
        "new_profit_policy_source_fresh_buy_rows_le_10s": profit_accepted_fresh_buy_events_le_10s,
        "new_profit_policy_source_fresh_buy_rows_le_30s": profit_accepted_fresh_buy_events_le_30s,
        "new_copyability_compatible_buy_rows": copyability_accepted_buy_events,
        "new_copyability_source_fresh_buy_rows_le_10s": copyability_accepted_fresh_buy_events_le_10s,
        "new_copyability_source_fresh_buy_rows_le_30s": copyability_accepted_fresh_buy_events_le_30s,
        "new_copyability_rejected_buy_rows": sum(copyability_rejected_reason_counts.values()),
        "new_copyability_rejected_source_fresh_buy_rows_le_10s": copyability_rejected_fresh_buy_events_le_10s,
        "new_copyability_rejected_source_fresh_buy_rows_le_30s": copyability_rejected_fresh_buy_events_le_30s,
        "current_poll_copyability_reason_counts": dict(sorted(copyability_reason_counts.items())),
        "current_poll_copyability_rejected_reason_counts": dict(sorted(copyability_rejected_reason_counts.items())),
        "current_poll_top_copyability_reject_reason": top_copyability_reject_reason,
        "current_poll_copyability_blocker_counts": dict(sorted(copyability_blocker_counts.items())),
        "current_poll_copyability_rejected_blocker_counts": dict(
            sorted(copyability_rejected_blocker_counts.items())
        ),
        "current_poll_top_copyability_reject_blocker": top_copyability_reject_blocker,
        "current_poll_copyability_rejected_samples": copyability_rejected_samples,
        "current_poll_source_trade_book_timing_rows": sum(
            count
            for key, count in book_timing_issue_counts.items()
            if not key.startswith("blocker:") and not key.startswith("clob_blocking_reason:")
        ),
        "current_poll_source_trade_book_timing_issue_counts": dict(sorted(book_timing_issue_counts.items())),
        "current_poll_source_trade_book_timing_samples": book_timing_samples,
        "current_poll_fresh_at_fetch_start_stale_at_decision_rows": (
            fresh_at_fetch_start_stale_at_decision_rows
        ),
        "current_poll_fresh_at_fetch_start_stale_at_decision_samples": (
            fresh_at_fetch_start_stale_at_decision_samples
        ),
        "current_poll_stale_before_wallet_fetch_rows": stale_before_fetch_rows,
        "current_poll_stale_before_wallet_fetch_samples": stale_before_fetch_samples,
        "current_poll_copyability_staleness_origin_counts": dict(
            sorted(copyability_staleness_origin_counts.items())
        ),
        "new_policy_compatible_rows": len(raw_intents),
        "new_unique_policy_compatible_rows": len(enriched_intents),
        "new_policy_source_fresh_buy_rows_le_10s": raw_intent_fresh_buy_events_le_10s,
        "new_unique_policy_source_fresh_buy_rows_le_10s": enriched_intent_fresh_buy_events_le_10s,
        "new_policy_source_fresh_buy_rows_le_30s": raw_intent_fresh_buy_events_le_30s,
        "new_unique_policy_source_fresh_buy_rows_le_30s": enriched_intent_fresh_buy_events_le_30s,
        "new_clob_filled_required_buys": clob_filled_buy_copy_events,
        "current_poll_required_buy_copy_events": required_buy_copy_events,
        "current_poll_fallback_buy_copy_events": int(summary.get("fallback_filled_buy_copy_events") or 0),
        "current_poll_rejected_buy_copy_events": int(summary.get("rejected_buy_copy_events") or 0),
        "current_poll_missed_buy_copy_events": int(summary.get("missed_buy_copy_events") or 0),
        "source_error_wallets": source_error_wallets,
        "source_route_error_wallets": route_error_wallets,
        "source_route_reset_wallets": route_reset_wallets,
        "source_route_override_recovered_wallets": route_override_recovered_wallets,
        "source_route_degraded_recovered_wallets": route_degraded_recovered_wallets,
        "source_route_override_recovered_rows": route_override_recovered_rows,
        "source_route_degraded_recovered_rows": route_degraded_recovered_rows,
        "source_route_pass_rows": route_pass_rows,
        "source_route_error_rows": route_error_rows,
        "source_route_status_counts": dict(sorted(route_status_counts.items())),
        "source_route_class_counts": dict(sorted(route_class_counts.items())),
        "blockers": blockers,
        "status": PASS if not blockers else active_status_from_blockers(blockers, default=ANALYZE),
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _information_source_fusion_plan(
    *,
    config: LiveTrackerConfig,
    wallet_reports: list[dict[str, Any]],
    current_poll_moves: list[dict[str, Any]],
    evidence_window_moves: list[dict[str, Any]],
    market_ws_rows: list[dict[str, Any]],
    current_poll_diagnostics: dict[str, Any],
    copy_efficiency: dict[str, Any],
    current_poll_copy_efficiency: dict[str, Any],
    hot_path_adaptive_summary: dict[str, Any],
    clob_book_cache_report: dict[str, Any],
    poll_runtime_limited: bool,
    runtime_skipped_wallets: list[str],
    runtime_skipped_events: int,
) -> dict[str, Any]:
    """Fuse source, copy, and execution signals into one active operational plan.

    This is diagnostic/coordination state only. It never changes admission truth,
    paper/live parity, or the operator gate.
    """

    def dict_value(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {}

    def list_value(value: Any) -> list[Any]:
        return value if isinstance(value, list) else []

    def int_value(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def status_count(counts: dict[str, Any], *statuses: str) -> int:
        wanted = {status.upper() for status in statuses}
        total = 0
        for key, value in counts.items():
            if str(key).upper() in wanted:
                total += int_value(value)
        return total

    def report_sum(key: str) -> int:
        total = 0
        for report in wallet_reports:
            if isinstance(report, dict):
                total += int_value(report.get(key))
        return total

    diag = dict_value(current_poll_diagnostics)
    ladder = dict_value(diag.get("current_poll_ladder"))
    current_summary = dict_value(current_poll_copy_efficiency.get("summary"))
    rolling_summary = dict_value(copy_efficiency.get("summary"))
    hot_summary = dict_value(hot_path_adaptive_summary)
    current_evidence = _evidence_status_counts(current_poll_moves)
    rolling_evidence = _evidence_status_counts(evidence_window_moves)
    clob_cache = _clob_book_cache_summary(clob_book_cache_report) if config.enable_clob_books else {"enabled": False}

    raw_rows = int_value(diag.get("raw_source_rows_seen") or ladder.get("raw_rows") or report_sum("raw_rows"))
    normalized_rows = int_value(diag.get("normalized_source_rows_seen") or ladder.get("normalized_events"))
    fresh_buy_rows = int_value(diag.get("fresh_buy_rows_le_10s") or ladder.get("fresh_buy_rows_le_10s"))
    route_status_counts = dict_value(diag.get("source_route_status_counts"))
    route_class_counts = dict_value(diag.get("source_route_class_counts"))
    source_freshness_by_source = dict_value(diag.get("current_poll_source_freshness_by_source"))

    data_blockers: list[str] = []
    if raw_rows <= 0:
        data_blockers.append("data_api_raw_source_rows_zero")
    if int_value(diag.get("source_error_wallets")) > 0:
        data_blockers.append("data_api_source_fetch_errors_present")
    if int_value(diag.get("source_route_reset_wallets")) > 0:
        data_blockers.append("data_api_route_reset_present")
    if int_value(diag.get("source_route_error_wallets")) > 0:
        data_blockers.append("data_api_route_partial_present")
    if int_value(diag.get("source_route_degraded_recovered_wallets")) > 0:
        data_blockers.append("data_api_route_degraded_recovery_present")

    runtime_blockers: list[str] = []
    runtime_skipped = len(runtime_skipped_wallets) + int_value(runtime_skipped_events)
    if poll_runtime_limited:
        runtime_blockers.append("poll_runtime_limited")
    if runtime_skipped:
        runtime_blockers.append("poll_scope_skipped_wallets_or_events")
    if int_value(ladder.get("runtime_limited_events_skipped")) > 0:
        runtime_blockers.append("runtime_limited_before_event_processing")

    current_required = int_value(current_summary.get("required_buy_copy_events"))
    current_clob_filled = int_value(current_summary.get("clob_filled_buy_copy_events"))
    current_fallback = int_value(current_summary.get("fallback_filled_buy_copy_events"))
    current_rejected = int_value(current_summary.get("rejected_buy_copy_events"))
    current_missed = int_value(current_summary.get("missed_buy_copy_events"))
    rolling_required = int_value(rolling_summary.get("required_buy_copy_events"))
    rolling_clob_filled = int_value(rolling_summary.get("clob_filled_buy_copy_events"))
    rolling_fallback = int_value(rolling_summary.get("fallback_filled_buy_copy_events"))
    rolling_rejected = int_value(rolling_summary.get("rejected_buy_copy_events"))
    rolling_missed = int_value(rolling_summary.get("missed_buy_copy_events"))

    current_clob_counts = dict_value(current_evidence.get("clob_book_admission_relevant"))
    rolling_clob_counts = dict_value(rolling_evidence.get("clob_book_admission_relevant"))
    current_clob_ok = status_count(current_clob_counts, "OK")
    rolling_clob_ok = status_count(rolling_clob_counts, "OK")
    intent_time_current = dict_value(current_poll_copy_efficiency.get("intent_time_copyability_proof"))
    intent_time_rolling = dict_value(copy_efficiency.get("intent_time_copyability_proof"))
    intent_time_current_ok = int_value(intent_time_current.get("accepted_records_used"))
    intent_time_rolling_ok = int_value(intent_time_rolling.get("accepted_records_used"))
    current_clob_ok += intent_time_current_ok
    rolling_clob_ok += intent_time_rolling_ok
    clob_blockers: list[str] = []
    if not config.enable_clob_books and (current_required > 0 or rolling_required > 0):
        clob_blockers.append("clob_book_source_disabled_for_copyable_buys")
    if config.enable_clob_books and current_required > 0 and current_clob_ok <= 0:
        clob_blockers.append("current_poll_clob_book_ok_evidence_missing")
    if config.enable_clob_books and current_required > 0 and current_clob_filled < current_required:
        clob_blockers.append("current_poll_copyable_buys_not_all_clob_filled")
    if config.enable_clob_books and rolling_required > 0 and rolling_clob_ok <= 0:
        clob_blockers.append("admission_window_clob_book_ok_evidence_missing")
    if int_value(clob_cache.get("errors")) > 0:
        clob_blockers.append("clob_book_fetch_errors_present")

    market_ws_counts = dict_value(current_evidence.get("market_ws_preconfirm"))
    rolling_market_ws_counts = dict_value(rolling_evidence.get("market_ws_preconfirm"))
    market_ws_matched = status_count(market_ws_counts, "MATCHED", "OK") + status_count(
        rolling_market_ws_counts,
        "MATCHED",
        "OK",
    )
    ws_blockers: list[str] = []
    if config.market_ws_jsonl_path and not market_ws_rows:
        ws_blockers.append("market_ws_cache_configured_but_empty")
    if config.market_ws_jsonl_path and market_ws_rows and market_ws_matched <= 0 and (current_required or rolling_required):
        ws_blockers.append("market_ws_preconfirm_not_matching_copyable_buys")

    onchain_counts = dict_value(current_evidence.get("onchain"))
    rolling_onchain_counts = dict_value(rolling_evidence.get("onchain"))
    onchain_confirmed = status_count(onchain_counts, "CONFIRMED", "OK") + status_count(
        rolling_onchain_counts,
        "CONFIRMED",
        "OK",
    )

    copy_blockers: list[str] = []
    if current_required <= 0:
        copy_blockers.append("no_current_poll_required_buy_copy_events")
    if current_fallback > 0 or rolling_fallback > 0:
        copy_blockers.append("fallback_filled_buy_copy_events_present")
    if current_rejected > 0 or rolling_rejected > 0:
        copy_blockers.append("rejected_buy_copy_events_present")
    if current_missed > 0 or rolling_missed > 0:
        copy_blockers.append("missed_buy_copy_events_present")
    if rolling_required > 0 and rolling_clob_filled < rolling_required:
        copy_blockers.append("admission_window_copyable_buys_not_all_clob_filled")

    hot_blockers = [str(blocker) for blocker in list_value(hot_summary.get("blockers")) if blocker]
    hot_status = str(hot_summary.get("status") or ANALYZE)
    if hot_status != PASS:
        copy_blockers.extend(f"hot_path:{blocker}" for blocker in hot_blockers[:5])

    all_blockers = [
        *data_blockers,
        *runtime_blockers,
        *clob_blockers,
        *ws_blockers,
        *copy_blockers,
    ]
    if not all_blockers and copy_efficiency.get("status") != PASS:
        all_blockers.append("rolling_copy_efficiency_not_pass")
    if not all_blockers and current_poll_copy_efficiency.get("status") != PASS:
        all_blockers.append("current_poll_copy_efficiency_not_pass")

    if any(blocker.startswith("data_api_route") or blocker.startswith("data_api_source") for blocker in all_blockers):
        next_action = "repair_data_api_routes_and_keep_user_proxyWallet_parallel_polling"
    elif runtime_blockers:
        next_action = "split_wallet_universe_into_checkpointed_parallel_poll_slices"
    elif raw_rows <= 0 or fresh_buy_rows <= 0:
        next_action = "expand_weekly_monthly_wallet_scope_and_refresh_fresh_source_routes"
    elif clob_blockers:
        next_action = "fetch_clob_books_for_fresh_copyable_buys_before_paper_fill"
    elif any("fallback" in blocker or "rejected" in blocker or "missed" in blocker for blocker in copy_blockers):
        next_action = "tighten_copy_execution_tactics_until_zero_fallback_reject_miss_buy"
    elif any("insufficient_agreeing_wallets" in blocker for blocker in copy_blockers):
        next_action = "rotate_multi_wallet_cohorts_from_scaled_per_wallet_copy_universe"
    elif current_required <= 0:
        next_action = "continue_high_coverage_polling_until_current_copyable_buy_evidence_arrives"
    else:
        next_action = "continue_scaled_per_wallet_and_multi_wallet_inventory_burnin"

    return {
        "role": "source_fusion_polling_copy_execution_plan_not_live_admission_truth",
        "status": PASS if not all_blockers else active_status_from_blockers(all_blockers, default=ANALYZE),
        "blockers": sorted(set(all_blockers)),
        "next_action": next_action,
        "sources": {
            "wallet_data_api": {
                "status": PASS if not data_blockers else active_status_from_blockers(data_blockers, default=ANALYZE),
                "blockers": data_blockers,
                "trade_query_keys": list(config.trade_query_keys),
                "parallel_data_api_sources": bool(config.parallel_data_api_sources),
                "wallet_reports": len(wallet_reports),
                "raw_rows": raw_rows,
                "normalized_rows": normalized_rows,
                "fresh_buy_rows_le_10s": fresh_buy_rows,
                "route_status_counts": dict(sorted(route_status_counts.items())),
                "route_class_counts": dict(sorted(route_class_counts.items())),
                "freshness_by_source": source_freshness_by_source,
            },
            "clob_books": {
                "status": PASS if not clob_blockers else active_status_from_blockers(clob_blockers, default=ANALYZE),
                "blockers": clob_blockers,
                "enabled": bool(config.enable_clob_books),
                "current_poll_admission_relevant_status_counts": current_clob_counts,
                "admission_window_admission_relevant_status_counts": rolling_clob_counts,
                "current_poll_ok_rows": current_clob_ok,
                "admission_window_ok_rows": rolling_clob_ok,
                "cache": clob_cache,
            },
            "market_ws_preconfirm": {
                "status": PASS if not ws_blockers else active_status_from_blockers(ws_blockers, default=ANALYZE),
                "blockers": ws_blockers,
                "enabled": bool(config.market_ws_jsonl_path),
                "path": config.market_ws_jsonl_path,
                "candidates_loaded": len(market_ws_rows),
                "matched_rows": market_ws_matched,
                "current_poll_status_counts": market_ws_counts,
                "admission_window_status_counts": rolling_market_ws_counts,
            },
            "onchain_receipts": {
                "status": PASS if (not config.enable_onchain_receipts or onchain_confirmed > 0) else ANALYZE,
                "enabled": bool(config.enable_onchain_receipts),
                "confirmed_rows": onchain_confirmed,
                "current_poll_status_counts": onchain_counts,
                "admission_window_status_counts": rolling_onchain_counts,
            },
        },
        "polling_plan": {
            "max_poll_runtime_s": float(config.max_poll_runtime_s or 0.0),
            "runtime_limited": bool(poll_runtime_limited),
            "parallel_wallet_fetches": int(config.parallel_wallet_fetches),
            "parallel_data_api_sources": bool(config.parallel_data_api_sources),
            "wallets_skipped_by_runtime": len(runtime_skipped_wallets),
            "events_skipped_by_runtime": int_value(runtime_skipped_events),
            "recommended_next_slice": next_action
            if runtime_blockers
            else "keep_full_weekly_monthly_leaderboard_universe_in_rotation",
        },
        "copy_plan": {
            "strategy": "copy_each_wallet_individually_then_promote_multi_wallet_inventory",
            "current_poll_required_buy_copy_events": current_required,
            "current_poll_clob_filled_buy_copy_events": current_clob_filled,
            "admission_window_required_buy_copy_events": rolling_required,
            "admission_window_clob_filled_buy_copy_events": rolling_clob_filled,
            "intent_time_current_clob_ok_records": intent_time_current_ok,
            "intent_time_admission_window_clob_ok_records": intent_time_rolling_ok,
            "hot_path_status": hot_status,
            "hot_path_runtime_eligible_wallets": hot_summary.get("runtime_eligible_wallets"),
            "hot_path_pass_signals": hot_summary.get("pass_signals"),
        },
        "execution_plan": {
            "mode": "paper_copy_intent_only_until_explicit_operator_gate",
            "paper_only": True,
            "live_orders_allowed": False,
            "operator_gate_required": True,
            "required_zero_live_blockers": [
                "fallback_filled_buy_copy_events_present",
                "rejected_buy_copy_events_present",
                "missed_buy_copy_events_present",
                "current_poll_clob_book_ok_evidence_missing",
            ],
            "current_poll_fallback_buy_copy_events": current_fallback,
            "current_poll_rejected_buy_copy_events": current_rejected,
            "current_poll_missed_buy_copy_events": current_missed,
            "admission_window_fallback_buy_copy_events": rolling_fallback,
            "admission_window_rejected_buy_copy_events": rolling_rejected,
            "admission_window_missed_buy_copy_events": rolling_missed,
        },
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _copyability_reject_diagnostic_sample(
    event: WalletEvent,
    decision: CopyabilityDecision | None,
    *,
    tracking_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compact diagnostic for rejected BUYs; never used as admission truth."""

    details = decision.details if isinstance(decision, CopyabilityDecision) and isinstance(decision.details, dict) else {}
    blockers = details.get("blockers") if isinstance(details.get("blockers"), list) else []
    wallet_api = {}
    clob_book = {}
    if isinstance(tracking_evidence, dict):
        wallet_api = tracking_evidence.get("wallet_api") if isinstance(tracking_evidence.get("wallet_api"), dict) else {}
        clob_book = tracking_evidence.get("clob_book") if isinstance(tracking_evidence.get("clob_book"), dict) else {}
    reason = (
        str(decision.reason)
        if isinstance(decision, CopyabilityDecision) and decision.reason
        else "missing_copyability_decision"
    )
    return {
        "event_id": event.event_id,
        "wallet": event.source_wallet,
        "market_slug": event.market_slug,
        "condition_id": event.condition_id,
        "outcome": event.outcome,
        "reason": reason,
        "blockers": [str(blocker) for blocker in blockers if blocker][:8] or [reason],
        "event_age_s": details.get("event_age_s"),
        "max_event_age_s": details.get("max_event_age_s"),
        "event_age_at_source_fetch_start_s": details.get("event_age_at_source_fetch_start_s"),
        "source_fetch_duration_s": details.get("source_fetch_duration_s"),
        "wallet_fetch_duration_s": details.get("wallet_fetch_duration_s"),
        "wallet_route_status": details.get("wallet_route_status") or wallet_api.get("route_status"),
        "wallet_route_class": details.get("wallet_route_class") or wallet_api.get("route_class"),
        "wallet_route_report_id": details.get("wallet_route_report_id") or wallet_api.get("route_report_id"),
        "wallet_data_api_source": details.get("wallet_data_api_source") or wallet_api.get("data_api_source"),
        "wallet_data_api_query_param": details.get("wallet_data_api_query_param")
        or wallet_api.get("data_api_query_param"),
        "clob_route_status": details.get("clob_route_status") or clob_book.get("route_status"),
        "clob_route_class": details.get("clob_route_class") or clob_book.get("route_class"),
        "clob_route_report_id": details.get("clob_route_report_id") or clob_book.get("route_report_id"),
        "clob_book_status": details.get("clob_book_status") or clob_book.get("status"),
        "clob_book_timestamp": details.get("clob_book_timestamp") or clob_book.get("book_timestamp"),
        "clob_book_hash": details.get("clob_book_hash") or clob_book.get("book_hash"),
        "clob_blocking_reason": details.get("clob_blocking_reason") or clob_book.get("blocking_reason"),
        "clob_best_ask": details.get("clob_best_ask") or clob_book.get("best_ask"),
        "clob_max_copy_price": details.get("clob_max_copy_price") or clob_book.get("max_copy_price"),
        "clob_fill_ratio": details.get("clob_fill_ratio") or clob_book.get("fill_ratio"),
        "clob_fillable_usd": details.get("clob_fillable_usd") or clob_book.get("fillable_usd"),
        "copy_size_usd": details.get("copy_size_usd") or clob_book.get("copy_size_usd"),
        "source_price": details.get("source_price") if details.get("source_price") is not None else event.price,
        "min_slippage_to_fill_bps": details.get("min_slippage_to_fill_bps"),
    }


def _hot_path_adaptive_evidence(
    enriched_moves: list[dict[str, Any]],
    *,
    config: LiveTrackerConfig,
    now_ts: float,
) -> dict[str, Any]:
    """Score adaptive wallet consensus on the current poll before log-lag can hide it."""

    adaptive_config = AdaptiveBotConfig(
        max_event_log_rows=max(1, len(enriched_moves)),
        max_observed_event_age_s=float(config.max_copyability_event_age_s),
        max_observation_age_s=max(float(config.max_copy_efficiency_latency_s), float(config.max_copyability_event_age_s)),
        min_agreeing_wallets=2,
        min_signal_score_usd=max(0.1, float(config.min_order_usd)),
        max_order_usd=float(config.max_order_usd),
        min_order_usd=max(0.1, float(config.min_order_usd)),
        min_clob_fill_ratio=float(config.min_copyability_clob_fill_ratio),
        apply_paper=False,
    )
    signals, adaptive_summary = build_adaptive_signals(
        enriched_moves,
        config=adaptive_config,
        now_ts=now_ts,
    )
    inventory_intents, inventory_summary = build_runtime_inventory_intents(
        enriched_moves,
        config=adaptive_config,
        now_ts=now_ts,
    )
    single_wallet_intents, single_wallet_summary = build_single_wallet_exact_copy_intents(
        enriched_moves,
        config=adaptive_config,
        now_ts=now_ts,
    )
    pass_signal_objects = [signal for signal in signals if signal.status == "PASS"]
    signal_rows = [signal.asdict() for signal in signals[:20]]
    pass_signal_count = len(pass_signal_objects)
    freshness = (
        adaptive_summary.get("freshness_diagnostics")
        if isinstance(adaptive_summary.get("freshness_diagnostics"), dict)
        else {}
    )
    runtime_fresh = int(freshness.get("runtime_fresh_buy_events_le_cap") or 0)
    runtime_wallets = int(freshness.get("runtime_eligible_wallets") or 0)
    runtime_inventory = int(adaptive_summary.get("runtime_inventory_research_candidates") or 0)
    tracker_time_inventory = int(adaptive_summary.get("tracker_time_inventory_research_candidates") or 0)
    blockers: list[str] = []
    if pass_signal_count <= 0:
        if not enriched_moves:
            blockers.append("no_current_poll_moves")
        elif freshness.get("source_feed_delayed") is True:
            blockers.append("source_feed_delayed")
        elif runtime_fresh <= 0:
            blockers.append("no_runtime_fresh_buy_events")
        elif runtime_wallets < adaptive_config.min_agreeing_wallets:
            blockers.append("runtime_fresh_but_single_wallet_only")
        elif runtime_inventory > 0:
            blockers.append("runtime_inventory_candidate_research_only")
        elif tracker_time_inventory > 0:
            blockers.append("tracker_time_inventory_candidate_research_only")
        else:
            blockers.append("no_hot_path_adaptive_pass_signal")
    return {
        "status": "PASS" if pass_signal_count > 0 else "WATCH",
        "blockers": blockers,
        "paper_only": True,
        "live_orders_allowed": False,
        "role": "current_poll_measurement_only_not_live_admission",
        "config": {
            "max_observed_event_age_s": adaptive_config.max_observed_event_age_s,
            "max_observation_age_s": adaptive_config.max_observation_age_s,
            "min_agreeing_wallets": adaptive_config.min_agreeing_wallets,
            "min_signal_score_usd": adaptive_config.min_signal_score_usd,
            "max_signal_cluster_age_s": adaptive_config.max_signal_cluster_age_s,
            "min_clob_fill_ratio": adaptive_config.min_clob_fill_ratio,
        },
        "summary": {
            "current_poll_moves": len(enriched_moves),
            "eligible_moves": adaptive_summary.get("eligible_moves"),
            "signals": adaptive_summary.get("signals"),
            "pass_signals": pass_signal_count,
            "runtime_signal_blocker_counts": adaptive_summary.get("runtime_signal_blocker_counts") or {},
            "top_blocked_runtime_signals": adaptive_summary.get("top_blocked_runtime_signals") or [],
            "runtime_market_outcome_counts": adaptive_summary.get("runtime_market_outcome_counts") or [],
            "tracker_time_pass_signals": adaptive_summary.get("tracker_time_pass_signals"),
            "tracker_time_signal_blocker_counts": adaptive_summary.get("tracker_time_signal_blocker_counts") or {},
            "top_blocked_tracker_time_signals": adaptive_summary.get("top_blocked_tracker_time_signals") or [],
            "runtime_inventory_research_candidates": runtime_inventory,
            "runtime_inventory_intents_created": inventory_summary.get("inventory_intents"),
            "runtime_inventory_modes": inventory_summary.get("inventory_modes") or {},
            "single_wallet_exact_copy": single_wallet_summary,
            "tracker_time_inventory_research_candidates": tracker_time_inventory,
            "tracker_time_inventory_modes": adaptive_summary.get("tracker_time_inventory_modes") or {},
            "freshness_diagnostics": freshness,
            "filter_reason_counts": adaptive_summary.get("filter_reason_counts") or {},
            "tracker_time_filter_reason_counts": adaptive_summary.get("tracker_time_filter_reason_counts") or {},
        },
        "top_signals": signal_rows,
        "top_runtime_inventory_candidates": adaptive_summary.get("top_runtime_inventory_candidates") or [],
        "top_tracker_time_inventory_candidates": adaptive_summary.get("top_tracker_time_inventory_candidates") or [],
        "_pass_signal_objects": pass_signal_objects,
        "_inventory_intent_objects": inventory_intents,
        "_inventory_summary": inventory_summary,
        "_single_wallet_intent_objects": single_wallet_intents,
        "_single_wallet_summary": single_wallet_summary,
        "_adaptive_config": adaptive_config,
    }


def _move_observed_ts(move: dict[str, Any]) -> float | None:
    wallet_event = move.get("wallet_event") if isinstance(move.get("wallet_event"), dict) else {}
    return (
        parse_ts(move.get("generated_at"))
        or parse_ts(move.get("observed_ts"))
        or parse_ts(wallet_event.get("observed_ts"))
    )


def _hot_path_tracker_time_replay_evidence(
    window_moves: list[dict[str, Any]],
    *,
    config: LiveTrackerConfig,
    now_ts: float,
) -> dict[str, Any]:
    """Paper-only replay of recently observed tracker-fresh wallet consensus.

    Current-poll hot-path evidence is the only live-admission shaped proof. This
    rolling replay is intentionally weaker: it preserves CLOB/copyability facts
    from the moment the tracker observed each wallet row, so Data API lag can be
    measured and fixed without pretending stale rows were executable live truth.
    """

    adaptive_config = AdaptiveBotConfig(
        max_event_log_rows=max(1, len(window_moves)),
        max_observed_event_age_s=float(config.max_copyability_event_age_s),
        max_observation_age_s=max(float(config.max_copy_efficiency_latency_s), float(config.max_copyability_event_age_s)),
        min_agreeing_wallets=2,
        min_signal_score_usd=max(0.1, float(config.min_order_usd)),
        max_order_usd=float(config.max_order_usd),
        min_order_usd=max(0.1, float(config.min_order_usd)),
        min_clob_fill_ratio=float(config.min_copyability_clob_fill_ratio),
        apply_paper=False,
    )
    observation_window_s = max(
        30.0,
        float(adaptive_config.max_observation_age_s) * 3.0,
        float(adaptive_config.max_observed_event_age_s) * 3.0,
    )
    recent_moves: list[dict[str, Any]] = []
    missing_observed_ts = 0
    stale_observation_rows = 0
    observation_ages: list[float] = []
    for move in window_moves:
        observed_ts = _move_observed_ts(move)
        if observed_ts is None:
            missing_observed_ts += 1
            continue
        age_s = max(0.0, float(now_ts) - float(observed_ts))
        observation_ages.append(age_s)
        if age_s <= observation_window_s:
            recent_moves.append(move)
        else:
            stale_observation_rows += 1

    signals, replay_summary = build_tracker_time_adaptive_signals(
        recent_moves,
        config=adaptive_config,
    )
    pass_signal_objects = [signal for signal in signals if signal.status == "PASS"]
    pass_signal_count = len(pass_signal_objects)
    blockers: list[str] = []
    if pass_signal_count <= 0:
        if not window_moves:
            blockers.append("no_rolling_window_moves")
        elif not recent_moves:
            blockers.append("no_recently_observed_tracker_time_moves")
        elif int(replay_summary.get("eligible_moves") or 0) <= 0:
            blockers.append("no_tracker_time_copyable_clob_backed_moves")
        elif int(replay_summary.get("signals") or 0) <= 0:
            blockers.append("no_tracker_time_consensus_signal")
        else:
            blockers.append("no_tracker_time_pass_signal")

    summary = {
        **replay_summary,
        "rolling_window_moves": len(window_moves),
        "recent_observed_moves": len(recent_moves),
        "missing_observed_ts_rows": missing_observed_ts,
        "stale_observation_rows": stale_observation_rows,
        "observation_window_s": round(observation_window_s, 6),
        "observation_age_min_s": round(min(observation_ages), 6) if observation_ages else None,
        "observation_age_max_s": round(max(observation_ages), 6) if observation_ages else None,
    }
    return {
        "status": "PASS" if pass_signal_count > 0 else "WATCH",
        "blockers": blockers,
        "paper_only": True,
        "live_orders_allowed": False,
        "role": "rolling_tracker_time_replay_not_live_admission",
        "config": {
            "max_observed_event_age_s": adaptive_config.max_observed_event_age_s,
            "max_observation_age_s": adaptive_config.max_observation_age_s,
            "rolling_observation_window_s": observation_window_s,
            "min_agreeing_wallets": adaptive_config.min_agreeing_wallets,
            "min_signal_score_usd": adaptive_config.min_signal_score_usd,
            "max_signal_cluster_age_s": adaptive_config.max_signal_cluster_age_s,
            "min_clob_fill_ratio": adaptive_config.min_clob_fill_ratio,
        },
        "summary": summary,
        "top_signals": [signal.asdict() for signal in signals[:20]],
        "top_tracker_time_inventory_candidates": replay_summary.get("top_inventory_candidates") or [],
        "_pass_signal_objects": pass_signal_objects,
        "_adaptive_config": adaptive_config,
    }


def _tracker_time_replay_intent(intent: CopyIntent) -> CopyIntent:
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
        strategy_family="live_tracker_hot_path_tracker_time_replay_v1",
        policy_id="live_tracker_tracker_time_replay_clob_consensus_v1",
        order_type="PAPER_TRACKER_TIME_REPLAY_CLOB_EVIDENCE",
        reason="tracker-time hot-path replay written to separate paper ledger; not live-admission truth",
        metadata=metadata,
        live_orders_allowed=False,
    )


class LiveWalletTracker:
    def __init__(self, config: LiveTrackerConfig | None = None):
        cfg = config or LiveTrackerConfig()
        if not cfg.all_order_exact_copy_paper_state_path:
            cfg = replace(
                cfg,
                all_order_exact_copy_paper_state_path=_sidecar_json_path(
                    cfg.paper_state_path,
                    "all_order_exact_copy",
                ),
            )
        if not cfg.all_order_exact_copy_paper_event_log_path:
            cfg = replace(
                cfg,
                all_order_exact_copy_paper_event_log_path=_sidecar_jsonl_path(
                    cfg.paper_event_log_path,
                    "all_order_exact_copy",
                ),
            )
        if not cfg.all_order_tactic_replay_paper_state_path:
            cfg = replace(
                cfg,
                all_order_tactic_replay_paper_state_path=_sidecar_json_path(
                    cfg.all_order_exact_copy_paper_state_path,
                    "aggressive_tactic_replay",
                ),
            )
        if not cfg.all_order_tactic_replay_paper_event_log_path:
            cfg = replace(
                cfg,
                all_order_tactic_replay_paper_event_log_path=_sidecar_jsonl_path(
                    cfg.all_order_exact_copy_paper_event_log_path,
                    "aggressive_tactic_replay",
                ),
            )
        self.config = cfg
        self.clob = CLOBMarketClient(self.config.clob_host, timeout_s=self.config.clob_timeout_s)
        self.gamma = GammaMarketClient(self.config.gamma_host, timeout_s=self.config.gamma_timeout_s)
        self.onchain = OnchainReceiptClient(self.config.polygon_rpc_url, timeout_s=self.config.onchain_timeout_s)
        self.paper = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=self.config.paper_state_path,
                event_log_path=self.config.paper_event_log_path,
                retain_orders=self.config.paper_retain_orders,
                retain_lifecycle_events=self.config.paper_retain_lifecycle_events,
                retain_dedupe_ids=self.config.paper_retain_dedupe_ids,
            )
        )
        self.tracker_time_replay_paper = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=self.config.tracker_time_replay_paper_state_path,
                event_log_path=self.config.tracker_time_replay_paper_event_log_path,
                retain_orders=self.config.paper_retain_orders,
                retain_lifecycle_events=self.config.paper_retain_lifecycle_events,
                retain_dedupe_ids=self.config.paper_retain_dedupe_ids,
            )
        )
        self.single_wallet_exact_copy_paper = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=self.config.single_wallet_exact_copy_paper_state_path,
                event_log_path=self.config.single_wallet_exact_copy_paper_event_log_path,
                retain_orders=self.config.paper_retain_orders,
                retain_lifecycle_events=self.config.paper_retain_lifecycle_events,
                retain_dedupe_ids=self.config.paper_retain_dedupe_ids,
            )
        )
        self.all_order_exact_copy_paper = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=self.config.all_order_exact_copy_paper_state_path,
                event_log_path=self.config.all_order_exact_copy_paper_event_log_path,
                retain_orders=self.config.paper_retain_orders,
                retain_lifecycle_events=self.config.paper_retain_lifecycle_events,
                retain_dedupe_ids=self.config.paper_retain_dedupe_ids,
            )
        )
        self.all_order_tactic_replay_paper = PaperWalletCopyEngine(
            PaperExecutionConfig(
                state_path=self.config.all_order_tactic_replay_paper_state_path,
                event_log_path=self.config.all_order_tactic_replay_paper_event_log_path,
                fill_model="all_order_aggressive_tactic_replay_fill_v1",
                allow_fallback_without_book=False,
                retain_orders=self.config.paper_retain_orders,
                retain_lifecycle_events=self.config.paper_retain_lifecycle_events,
                retain_dedupe_ids=self.config.paper_retain_dedupe_ids,
            )
        )

    def load_state(self) -> dict[str, Any]:
        state = load_json(self.config.state_path, default=None)
        if not isinstance(state, dict) or state.get("kind") != "wallet_copy_live_tracking_state":
            state = _empty_state()
        state["paper_only"] = True
        state["live_orders_allowed"] = False
        return state

    def _profit_candidate_source_addresses(self) -> list[str]:
        if not (
            self.config.use_profit_search_scope
            or self.config.track_blocked_profit_policy
        ) or not self.config.profit_policy_state_path:
            return []
        payload = load_json(self.config.profit_policy_state_path, default={})
        if not isinstance(payload, dict):
            return []
        candidate = self._profit_policy_candidate_row(payload)
        return self._candidate_source_addresses(candidate)

    def _candidate_source_addresses(self, candidate: dict[str, Any]) -> list[str]:
        addresses: list[str] = []

        def add_address(value: Any) -> None:
            address = str(value or "").lower().strip()
            if address.startswith("0x") and address not in addresses:
                addresses.append(address)

        if not isinstance(candidate, dict):
            return addresses
        metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
        add_address(candidate.get("source_wallet"))
        add_address(metadata.get("source_wallet"))
        for key in ("source_wallets", "unique_wallets"):
            values = candidate.get(key)
            if not isinstance(values, list):
                values = metadata.get(key)
            if isinstance(values, list):
                for value in values:
                    add_address(value)
        live_target_profile = candidate.get("live_target_profile") if isinstance(candidate.get("live_target_profile"), dict) else {}
        observed_profile = (
            live_target_profile.get("observed")
            if isinstance(live_target_profile.get("observed"), dict)
            else {}
        )
        inventory_profiles = [
            candidate.get("inventory_profile") if isinstance(candidate.get("inventory_profile"), dict) else {},
            metadata.get("inventory_profile") if isinstance(metadata.get("inventory_profile"), dict) else {},
            observed_profile.get("inventory_profile") if isinstance(observed_profile.get("inventory_profile"), dict) else {},
        ]
        for profile in inventory_profiles:
            rows = profile.get("top_source_wallets") if isinstance(profile, dict) else []
            if not isinstance(rows, list):
                continue
            for row in rows:
                if isinstance(row, dict):
                    add_address(row.get("wallet") or row.get("source_wallet"))
                else:
                    add_address(row)
        return addresses

    @staticmethod
    def _candidate_policy_id(candidate: dict[str, Any]) -> str:
        if not isinstance(candidate, dict):
            return ""
        policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
        admission_identity = (
            candidate.get("admission_identity")
            if isinstance(candidate.get("admission_identity"), dict)
            else {}
        )
        return str(
            candidate.get("policy_id")
            or policy.get("policy_id")
            or admission_identity.get("policy_id")
            or ""
        )

    def _explicit_profit_policy_candidate_row(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not bool(self.config.profit_policy_candidate_only):
            return {}
        explicit_addresses = {
            str(address).lower().strip()
            for address in self.config.wallet_address_allowlist
            if str(address or "").lower().strip().startswith("0x")
        }
        if not explicit_addresses:
            return {}
        phase = (
            WALLET_COPY_MISSION_CONTRACT.get("current_runtime_phase_contract")
            if isinstance(WALLET_COPY_MISSION_CONTRACT.get("current_runtime_phase_contract"), dict)
            else {}
        )
        mission_candidate = (
            phase.get("primary_live_candidate")
            if isinstance(phase.get("primary_live_candidate"), dict)
            else {}
        )
        mission_policy_id = str(mission_candidate.get("policy_id") or "")
        candidates: list[dict[str, Any]] = []
        for key in (
            "best_candidate",
            "forward_candidate",
            "runtime_admission_candidate",
        ):
            row = payload.get(key)
            if isinstance(row, dict):
                candidates.append(row)
        for key in (
            "forward_queue_runtime_candidates",
            "pass_candidates",
            "ranked_candidates",
            "forward_tracking_queue",
        ):
            rows = payload.get(key)
            if isinstance(rows, list):
                candidates.extend(row for row in rows if isinstance(row, dict))

        best_match: tuple[int, int, dict[str, Any]] | None = None
        for index, candidate in enumerate(candidates):
            addresses = set(self._candidate_source_addresses(candidate))
            if not addresses.intersection(explicit_addresses):
                continue
            score = 100
            candidate_type = str(candidate.get("candidate_type") or "").upper()
            if candidate_type == "SINGLE_WALLET":
                score += 20
            policy_id = self._candidate_policy_id(candidate)
            status = str(candidate.get("status") or "").upper()
            if status == "PASS":
                score += 50
            elif status == "BLOCKED":
                score -= 20
            live_target_profile = (
                candidate.get("live_target_profile")
                if isinstance(candidate.get("live_target_profile"), dict)
                else {}
            )
            live_target_status = str(live_target_profile.get("status") or "").upper()
            if live_target_status == "PASS":
                score += 40
            elif live_target_status == "BLOCKED":
                score -= 10
            if mission_policy_id and policy_id == mission_policy_id:
                score += 5
            if best_match is None or score > best_match[0]:
                best_match = (score, -index, candidate)
        return best_match[2] if best_match else {}

    def _strategy_direction_scope_addresses(self) -> list[str]:
        if not self.config.use_profit_search_scope or not self.config.strategy_direction_state_path:
            return []
        payload = load_json(self.config.strategy_direction_state_path, default={})
        if not isinstance(payload, dict):
            return []
        directions = payload.get("directions") if isinstance(payload.get("directions"), list) else []
        addresses: list[str] = []
        for row in directions:
            if not isinstance(row, dict):
                continue
            direction_id = str(row.get("id") or "")
            if direction_id not in {
                "profitable_wallet_copy_efficiency",
                "wr_repair_single_wallet",
                "single_wallet_best_copyable",
            }:
                continue
            address = str(row.get("wallet") or "").lower()
            if address and address not in addresses:
                addresses.append(address)
        return addresses

    def _profit_scope_addresses(self) -> list[str]:
        if not self.config.use_profit_search_scope:
            return []
        strategy_addresses = self._strategy_direction_scope_addresses()
        if not self.config.profit_policy_state_path:
            return strategy_addresses
        payload = load_json(self.config.profit_policy_state_path, default={})
        if not isinstance(payload, dict):
            return strategy_addresses
        source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}
        wallet_search = (
            source_contract.get("wallet_search_summary")
            if isinstance(source_contract.get("wallet_search_summary"), dict)
            else {}
        )
        selected = wallet_search.get("selected_wallets") if isinstance(wallet_search.get("selected_wallets"), list) else []
        addresses = [
            str(row.get("source_wallet") or "").lower()
            for row in selected
            if isinstance(row, dict) and row.get("source_wallet")
        ]
        return list(
            dict.fromkeys(
                address
                for address in [
                    *self._profit_candidate_source_addresses(),
                    *addresses,
                    *strategy_addresses,
                ]
                if address
            )
        )

    def _profit_policy_candidate_row(self, payload: dict[str, Any]) -> dict[str, Any]:
        explicit_candidate = self._explicit_profit_policy_candidate_row(payload)
        if explicit_candidate:
            return explicit_candidate
        best = payload.get("best_candidate") if isinstance(payload.get("best_candidate"), dict) else {}
        forward = payload.get("forward_candidate") if isinstance(payload.get("forward_candidate"), dict) else {}
        if (
            self.config.track_blocked_profit_policy
            and forward
            and isinstance(forward.get("policy"), dict)
        ):
            return forward
        return best

    def _scoped_specs(self, state: dict[str, Any] | None = None) -> tuple[list[WalletSpec], dict[str, Any]]:
        all_specs = [spec for spec in load_wallet_registry(self.config.registry_path) if spec.enabled]
        state_payload = state if isinstance(state, dict) else {}
        explicit_address_list = list(
            dict.fromkeys(
                str(address).lower()
                for address in self.config.wallet_address_allowlist
                if str(address or "").strip()
            )
        )
        explicit_addresses = set(explicit_address_list)
        explicit_names = {
            str(name)
            for name in self.config.wallet_name_allowlist
            if str(name or "").strip()
        }
        candidate_only_requested = bool(self.config.profit_policy_candidate_only)
        strategy_direction_addresses = (
            [] if candidate_only_requested else self._strategy_direction_scope_addresses()
        )
        profit_candidate_addresses = self._profit_candidate_source_addresses()
        pinned_profit_addresses = list(
            dict.fromkeys(
                [
                    *profit_candidate_addresses,
                    *strategy_direction_addresses,
                ]
            )
        )
        pinned_profit_address_set = set(pinned_profit_addresses)
        profit_scope_addresses = self._profit_scope_addresses()
        if candidate_only_requested and profit_candidate_addresses:
            profit_scope_addresses = profit_candidate_addresses
        scope_addresses = explicit_address_list or list(dict.fromkeys(profit_scope_addresses))
        specs = all_specs
        scope_reason = "all_enabled_registry_wallets"
        if scope_addresses:
            specs_by_address = {spec.normalized_address(): spec for spec in all_specs}
            specs = [specs_by_address[address] for address in scope_addresses if address in specs_by_address]
            for address in scope_addresses:
                if address not in specs_by_address and str(address).startswith("0x"):
                    specs.append(WalletSpec(name=f"explicit_{address[-8:]}", address=address))
            scope_reason = "explicit_wallet_addresses" if explicit_addresses else "profit_search_selected_wallets"
        if explicit_names and not explicit_addresses:
            specs = [spec for spec in specs if spec.name in explicit_names]
            scope_reason = "explicit_wallet_names"
        original_scoped_count = len(specs)
        pinned_specs: list[WalletSpec] = []
        if pinned_profit_addresses and not explicit_addresses and not explicit_names:
            pinned_by_address = {
                spec.normalized_address(): spec
                for spec in specs
                if spec.normalized_address() in pinned_profit_address_set
            }
            pinned_specs = [
                pinned_by_address[address]
                for address in pinned_profit_addresses
                if address in pinned_by_address
            ]
            specs = [spec for spec in specs if spec.normalized_address() not in pinned_profit_address_set]
        candidate_only_scope = bool(
            candidate_only_requested
            and pinned_specs
            and not explicit_addresses
            and not explicit_names
        )
        if candidate_only_scope:
            specs = []
            scope_reason = "profit_policy_candidate_only"
        rotation_offset = 0
        rotatable_scoped_count = len(specs)
        available_rotating_slots = max(0, int(self.config.max_wallets) - len(pinned_specs)) if self.config.max_wallets > 0 else 0
        rotation_enabled = bool(
            self.config.max_wallets > 0
            and available_rotating_slots > 0
            and rotatable_scoped_count > available_rotating_slots
            and not explicit_addresses
            and not explicit_names
            and not candidate_only_scope
        )
        if rotation_enabled:
            previous_summary = state_payload.get("summary") if isinstance(state_payload.get("summary"), dict) else {}
            previous_scope = (
                previous_summary.get("tracker_scope") if isinstance(previous_summary.get("tracker_scope"), dict) else {}
            )
            try:
                rotation_offset = int(previous_scope.get("next_rotation_offset") or 0) % rotatable_scoped_count
            except (TypeError, ValueError, ZeroDivisionError):
                rotation_offset = 0
            specs = specs[rotation_offset:] + specs[:rotation_offset]
        if self.config.max_wallets > 0:
            specs = [*pinned_specs, *specs[:available_rotating_slots]]
        else:
            specs = [*pinned_specs, *specs]
        next_rotation_offset = None
        if rotation_enabled:
            next_rotation_offset = (rotation_offset + available_rotating_slots) % rotatable_scoped_count
        return specs, {
            "scope_reason": scope_reason,
            "registry_enabled_wallets": len(all_specs),
            "profit_scope_wallets": len(profit_scope_addresses),
            "strategy_direction_scope_wallets": len(strategy_direction_addresses),
            "pinned_profit_candidate_wallets": len(pinned_specs),
            "pinned_profit_candidate_wallet_addresses": [spec.normalized_address() for spec in pinned_specs[:50]],
            "profit_policy_candidate_only": candidate_only_scope,
            "explicit_address_count": len(explicit_addresses),
            "explicit_name_count": len(explicit_names),
            "scoped_wallets_before_max": original_scoped_count,
            "rotatable_scoped_wallets_before_max": rotatable_scoped_count,
            "max_wallets": int(self.config.max_wallets),
            "rotation_enabled": rotation_enabled,
            "rotation_offset": rotation_offset,
            "next_rotation_offset": next_rotation_offset,
            "wallets_tracked": len(specs),
            "tracked_wallets": [
                {"name": spec.name, "address": spec.normalized_address(), "tags": list(spec.tags)}
                for spec in specs[:50]
            ],
        }

    def _load_profit_policy(self) -> tuple[CandidatePolicy | None, dict[str, Any]]:
        if not self.config.profit_policy_state_path:
            return None, {"status": "DISABLED"}
        payload = load_json(self.config.profit_policy_state_path, default={})
        if not isinstance(payload, dict):
            return None, {"status": "INVALID_STATE"}
        decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else {}
        best = payload.get("best_candidate") if isinstance(payload.get("best_candidate"), dict) else {}
        candidate = self._profit_policy_candidate_row(payload)
        policy_row = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
        candidate_selection_status = decision.get("candidate_selection_status") or decision.get("status")
        candidate_role = "forward_candidate" if candidate is not best else "best_candidate"
        policy_is_passed = (
            candidate_role == "best_candidate"
            and candidate_selection_status == "PASS"
            and best.get("status") == "PASS"
        )
        policy_is_blocked_but_trackable = (
            bool(self.config.track_blocked_profit_policy)
            and bool(policy_row)
            and (
                candidate_role == "forward_candidate"
                or str(candidate.get("status") or "") == "BLOCKED"
                or bool(payload.get("queue_rank_probe") if isinstance(payload.get("queue_rank_probe"), dict) else {})
            )
        )
        if not policy_row or not (policy_is_passed or policy_is_blocked_but_trackable):
            return None, {
                "status": "NO_PASS_POLICY",
                "decision_status": decision.get("status"),
                "candidate_selection_status": candidate_selection_status,
                "best_candidate_status": best.get("status"),
                "candidate_role": candidate_role,
                "candidate_status": candidate.get("status"),
                "track_blocked_profit_policy": bool(self.config.track_blocked_profit_policy),
            }
        policy = CandidatePolicy(
            policy_id=str(policy_row.get("policy_id") or "profit_policy"),
            min_price=float(policy_row.get("min_price", 0.01)),
            max_price=float(policy_row.get("max_price", 1.0)),
            min_wallet_usdc=float(policy_row.get("min_wallet_usdc", 0.0)),
            max_wallet_usdc=float(policy_row.get("max_wallet_usdc", 0.0)),
            min_seconds_from_open=(
                None if policy_row.get("min_seconds_from_open") is None else float(policy_row.get("min_seconds_from_open"))
            ),
            max_seconds_from_open=(
                None if policy_row.get("max_seconds_from_open") is None else float(policy_row.get("max_seconds_from_open"))
            ),
            wallet_fraction=float(policy_row.get("wallet_fraction", self.config.wallet_fraction)),
            max_order_usd=float(policy_row.get("max_order_usd", self.config.max_order_usd)),
            min_order_usd=float(policy_row.get("min_order_usd", self.config.min_order_usd)),
        )
        if policy_is_blocked_but_trackable:
            status = "BLOCKED_POLICY_LOADED_FOR_FORWARD_MEASUREMENT_ONLY"
        else:
            status = (
                "PASS_POLICY_LOADED"
                if decision.get("live_admission_status") == "PASS"
                else "PAPER_POLICY_LOADED_FOR_TRACKING_ONLY"
            )
        candidate_source_wallets = self._candidate_source_addresses(candidate)
        return policy, {
            "status": status,
            "state_path": self.config.profit_policy_state_path,
            "candidate_id": candidate.get("candidate_id"),
            "candidate_type": candidate.get("candidate_type"),
            "candidate_role": candidate_role,
            "candidate_source_wallet": candidate.get("source_wallet")
            or (
                (candidate.get("metadata") or {}).get("source_wallet")
                if isinstance(candidate.get("metadata"), dict)
                else None
            ),
            "candidate_source_wallets": candidate_source_wallets,
            "candidate_source_wallet_count": len(candidate_source_wallets),
            "profit_score": candidate.get("profit_score"),
            "decision_status": decision.get("status"),
            "candidate_selection_status": candidate_selection_status,
            "best_candidate_status": best.get("status"),
            "candidate_status": candidate.get("status"),
            "live_admission_status": decision.get("live_admission_status"),
            "live_admission_blockers": decision.get("live_admission_blockers") or [],
            "track_blocked_profit_policy": bool(self.config.track_blocked_profit_policy),
            "live_admission_note": "paper_only_forward_measurement_not_live_admission"
            if policy_is_blocked_but_trackable
            else None,
            "policy": policy.asdict(),
        }

    def _build_policy(self, profit_policy: CandidatePolicy | None = None) -> CopyPolicy:
        if profit_policy is not None:
            return CopyPolicy(
                policy_id=profit_policy.policy_id,
                strategy_family="wallet_copy_live_profit_policy_v1",
                allowed_assets=("BTC",),
                market_filter="btc_5m",
                min_price=profit_policy.min_price,
                max_price=profit_policy.max_price,
                sizing=SizingPolicy(
                    policy_id=f"wallet_fraction_{profit_policy.wallet_fraction:g}_cap_{profit_policy.max_order_usd:g}",
                    basis="wallet_usdc_fraction",
                    wallet_fraction=profit_policy.wallet_fraction,
                    max_order_usd=profit_policy.max_order_usd,
                    min_order_usd=profit_policy.min_order_usd,
                ),
            )
        return CopyPolicy(
            policy_id="live_tracking_exact_btc_5m_all_buys",
            strategy_family="wallet_copy_live_tracking_v1",
            allowed_assets=("BTC",),
            market_filter="btc_5m",
            sizing=SizingPolicy(
                policy_id=f"wallet_fraction_{self.config.wallet_fraction:g}_cap_{self.config.max_order_usd:g}",
                basis="wallet_usdc_fraction",
                wallet_fraction=self.config.wallet_fraction,
                max_order_usd=self.config.max_order_usd,
                min_order_usd=self.config.min_order_usd,
            ),
        )

    def _build_all_order_exact_copy_policy(self, sizing: SizingPolicy | None = None) -> CopyPolicy:
        return CopyPolicy(
            policy_id="all_order_exact_btc_5m_all_buys",
            strategy_family="wallet_copy_all_order_exact_v1",
            allowed_assets=("BTC",),
            market_filter="btc_5m",
            sizing=sizing or SizingPolicy(
                policy_id=f"all_order_wallet_fraction_{self.config.wallet_fraction:g}_cap_{self.config.max_order_usd:g}",
                basis="wallet_usdc_fraction",
                wallet_fraction=self.config.wallet_fraction,
                max_order_usd=self.config.max_order_usd,
                min_order_usd=self.config.min_order_usd,
            ),
        )

    def _all_order_micro_batch_probe(
        self,
        intents: list[CopyIntent],
        event_by_id: dict[str, WalletEvent],
        *,
        window_s: float = 1.0,
        min_batch_usd: float = 1.0,
        max_slippage_bps: float = 750.0,
        max_event_age_s: float = 120.0,
        max_groups: int = 50,
        clob_book_cache: dict[str, dict[str, Any]] | None = None,
        clob_book_cache_report: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Paper-only CLOB probe for batching tiny all-order BUY copies.

        The strict all-order ledger remains the live-truth source. This probe is
        an execution research lane: it asks whether same-wallet/same-token BUYs
        in a tiny time bucket become CLOB-fillable when replayed as one CopyIntent.
        """

        buy_intents = [intent for intent in intents if str(intent.action or "BUY").upper() == "BUY"]
        if not buy_intents:
            return {
                "status": "NO_EVENTS",
                "role": "paper_only_micro_batch_all_order_probe_not_live_admission",
                "source_buy_intents": 0,
                "paper_only": True,
                "live_orders_allowed": False,
            }

        grouped: dict[tuple[str, str, str, str, str, int], list[CopyIntent]] = defaultdict(list)
        skipped_stale = 0
        for intent in buy_intents:
            event = event_by_id.get(intent.source_event_id)
            if event is not None and event.age_s is not None and float(event.age_s) > float(max_event_age_s):
                skipped_stale += 1
                continue
            ts = float(intent.event_ts or intent.observed_ts or 0.0)
            bucket = int(ts // max(0.001, float(window_s)))
            key = (
                str(intent.source_wallet or "").lower(),
                str(intent.condition_id or ""),
                str(intent.token_id or ""),
                str(intent.side or "BUY").upper(),
                str(intent.outcome or ""),
                bucket,
            )
            grouped[key].append(intent)

        book_cache: dict[str, dict[str, Any]] = clob_book_cache if clob_book_cache is not None else {}
        groups: list[dict[str, Any]] = []
        status_counts: Counter[str] = Counter()
        fill_source_counts: Counter[str] = Counter()
        reject_reason_counts: Counter[str] = Counter()
        covered_child_events = 0
        filled_child_events = 0
        rejected_child_events = 0
        exact_total_usd = 0.0
        submit_total_usd = 0.0
        overcopy_usd = 0.0
        groups_limited = 0

        for index, rows in enumerate(
            sorted(grouped.values(), key=lambda batch: min(float(x.event_ts or x.observed_ts or 0.0) for x in batch))
        ):
            if index >= int(max_groups):
                groups_limited += 1
                continue
            rows = sorted(rows, key=lambda item: float(item.event_ts or item.observed_ts or 0.0))
            representative = rows[-1]
            exact_size = round(sum(max(0.0, float(row.copy_size_usd)) for row in rows), 6)
            if exact_size <= 0:
                continue
            submit_size = round(max(float(min_batch_usd), exact_size), 6)
            batch_overcopy = round(max(0.0, submit_size - exact_size), 6)
            source_wallet_total_usd = round(
                sum(max(0.0, float(row.wallet_usdc_size or 0.0)) for row in rows),
                6,
            )
            exact_source_wallet_fraction = (
                round(exact_size / source_wallet_total_usd, 9) if source_wallet_total_usd > 0 else None
            )
            submit_source_wallet_fraction = (
                round(submit_size / source_wallet_total_usd, 9) if source_wallet_total_usd > 0 else None
            )
            source_wallet_fraction_floor_delta = (
                round(submit_source_wallet_fraction - exact_source_wallet_fraction, 9)
                if submit_source_wallet_fraction is not None and exact_source_wallet_fraction is not None
                else None
            )
            weighted_price = (
                sum(float(row.limit_price) * max(0.0, float(row.copy_size_usd)) for row in rows) / exact_size
            )
            token_id = str(representative.token_id or "")
            book_status = "MISSING_TOKEN"
            clob_summary: dict[str, Any] = {"enabled": self.config.enable_clob_books, "status": book_status}
            if self.config.enable_clob_books and token_id:
                if clob_book_cache_report is not None:
                    clob_book_cache_report["micro_probe_lookup_events"] = int(
                        clob_book_cache_report.get("micro_probe_lookup_events") or 0
                    ) + 1
                    tokens = clob_book_cache_report.setdefault("token_ids", set())
                    if isinstance(tokens, set):
                        tokens.add(token_id)
                if token_id not in book_cache:
                    if clob_book_cache_report is not None:
                        clob_book_cache_report["micro_probe_cache_misses"] = int(
                            clob_book_cache_report.get("micro_probe_cache_misses") or 0
                        ) + 1
                    fetch_started = time.perf_counter()
                    try:
                        book_cache[token_id] = self.clob.get_book(token_id)
                        if clob_book_cache_report is not None:
                            durations = clob_book_cache_report.setdefault("micro_probe_fetch_durations_s", [])
                            if isinstance(durations, list):
                                durations.append(round(time.perf_counter() - fetch_started, 6))
                    except Exception as exc:  # noqa: BLE001 - probe diagnostics must not stop tracking
                        book_cache[token_id] = {"_probe_error": str(exc)}
                        if clob_book_cache_report is not None:
                            clob_book_cache_report["micro_probe_errors"] = int(
                                clob_book_cache_report.get("micro_probe_errors") or 0
                            ) + 1
                            clob_book_cache_report["errors"] = int(clob_book_cache_report.get("errors") or 0) + 1
                else:
                    if clob_book_cache_report is not None:
                        clob_book_cache_report["micro_probe_cache_hits"] = int(
                            clob_book_cache_report.get("micro_probe_cache_hits") or 0
                        ) + 1
                book = book_cache[token_id]
                if "_probe_error" in book:
                    clob_summary = {
                        "enabled": True,
                        "status": "ERROR",
                        "error": str(book.get("_probe_error") or ""),
                    }
                else:
                    clob_summary = {
                        "enabled": True,
                        "status": "OK",
                        **CLOBMarketClient.summarize_book(
                            book,
                            copy_size_usd=submit_size,
                            source_price=weighted_price,
                            max_slippage_bps=max_slippage_bps,
                        ),
                    }

            batch_event_id = stable_id(
                "mb",
                {
                    "source_wallet": representative.source_wallet.lower(),
                    "condition_id": representative.condition_id,
                    "token_id": representative.token_id,
                    "outcome": representative.outcome,
                    "event_ids": [row.source_event_id for row in rows],
                    "submit_size": submit_size,
                },
            )
            batch_intent = CopyIntent(
                source_wallet=representative.source_wallet,
                wallet_name=representative.wallet_name,
                source_event_id=batch_event_id,
                condition_id=representative.condition_id,
                market_slug=representative.market_slug,
                outcome=representative.outcome,
                side=representative.side,
                limit_price=round(weighted_price, 6),
                wallet_usdc_size=round(sum(float(row.wallet_usdc_size) for row in rows), 6),
                copy_size_usd=submit_size,
                shares=round(submit_size / max(0.000001, weighted_price), 6),
                observed_ts=max(float(row.observed_ts or 0.0) for row in rows),
                strategy_family="wallet_copy_all_order_micro_batch_probe_v1",
                policy_id="paper_only_micro_batch_all_order_probe_v1",
                sizing_policy_id=f"micro_batch_min_{min_batch_usd:g}_slippage_{max_slippage_bps:g}",
                mode="paper",
                action="BUY",
                order_type="PAPER_MICRO_BATCH_PROBE",
                token_id=representative.token_id,
                market_id=representative.market_id,
                event_ts=max(float(row.event_ts or row.observed_ts or 0.0) for row in rows),
                api_latency_s=max(
                    [float(row.api_latency_s) for row in rows if row.api_latency_s is not None] or [0.0]
                ),
                live_orders_allowed=False,
                reason="paper-only all-order micro batch fillability probe; not live admission truth",
                metadata={
                    "live_tracking_evidence": {"clob_book": clob_summary},
                    "micro_batch_probe": {
                        "child_source_event_ids": [row.source_event_id for row in rows],
                        "child_count": len(rows),
                        "exact_copy_size_usd": exact_size,
                        "submit_copy_size_usd": submit_size,
                        "overcopy_usd": batch_overcopy,
                        "source_wallet_total_usd": source_wallet_total_usd,
                        "exact_source_wallet_fraction": exact_source_wallet_fraction,
                        "submit_source_wallet_fraction": submit_source_wallet_fraction,
                        "source_wallet_fraction_floor_delta": source_wallet_fraction_floor_delta,
                        "window_s": window_s,
                        "max_slippage_bps": max_slippage_bps,
                    },
                },
            )
            fill = estimate_executable_fill(
                batch_intent,
                FillModelConfig(
                    model_id="all_order_micro_batch_probe_fill_v1",
                    min_fill_ratio=0.999,
                    allow_fallback_without_book=False,
                ),
            )
            status = "PASS" if fill.get("status") == "FILLED" else "WATCH"
            status_counts[status] += 1
            fill_source_counts[str(fill.get("source") or "unknown")] += 1
            if fill.get("reject_reason"):
                reject_reason_counts[str(fill.get("reject_reason"))] += 1
            child_count = len(rows)
            covered_child_events += child_count
            if status == "PASS":
                filled_child_events += child_count
            else:
                rejected_child_events += child_count
            exact_total_usd += exact_size
            submit_total_usd += submit_size
            overcopy_usd += batch_overcopy
            groups.append(
                {
                    "status": status,
                    "batch_intent_id": batch_intent.intent_id,
                    "source_wallet": representative.source_wallet,
                    "wallet_name": representative.wallet_name,
                    "condition_id": representative.condition_id,
                    "market_slug": representative.market_slug,
                    "outcome": representative.outcome,
                    "token_id": token_id,
                    "child_count": child_count,
                    "child_source_event_ids": [row.source_event_id for row in rows],
                    "exact_copy_size_usd": exact_size,
                    "submit_copy_size_usd": submit_size,
                    "overcopy_usd": batch_overcopy,
                    "source_wallet_total_usd": source_wallet_total_usd,
                    "exact_source_wallet_fraction": exact_source_wallet_fraction,
                    "submit_source_wallet_fraction": submit_source_wallet_fraction,
                    "source_wallet_fraction_floor_delta": source_wallet_fraction_floor_delta,
                    "weighted_source_price": round(weighted_price, 6),
                    "fill": fill,
                }
            )

        rejected_groups = int(status_counts.get("WATCH") or 0)
        pass_groups = int(status_counts.get("PASS") or 0)
        exact_no_overcopy_pass_groups = sum(
            1 for row in groups if row.get("status") == "PASS" and num(row.get("overcopy_usd")) <= 0
        )
        exact_no_overcopy_filled_child_events = sum(
            int(row.get("child_count") or 0)
            for row in groups
            if row.get("status") == "PASS" and num(row.get("overcopy_usd")) <= 0
        )
        overcopy_research_pass_groups = sum(
            1 for row in groups if row.get("status") == "PASS" and num(row.get("overcopy_usd")) > 0
        )
        overcopy_research_filled_child_events = sum(
            int(row.get("child_count") or 0)
            for row in groups
            if row.get("status") == "PASS" and num(row.get("overcopy_usd")) > 0
        )
        blockers: list[str] = []
        if not groups:
            blockers.append("no_probe_groups_after_stale_filter")
        if skipped_stale:
            blockers.append("stale_buy_events_skipped_from_probe")
        if rejected_groups:
            blockers.append("micro_batch_probe_rejected_groups_present")
        if overcopy_usd > 0:
            blockers.append("micro_batch_probe_requires_overcopy_to_meet_min_order")
        if groups_limited:
            blockers.append("micro_batch_probe_group_limit_reached")
        status = (
            "NO_EVENTS"
            if not groups
            else (PASS if not rejected_groups and overcopy_usd <= 0 else active_status_from_blockers(blockers, default=ANALYZE))
        )
        overcopy_pct_of_exact_total = (
            round(100.0 * overcopy_usd / exact_total_usd, 6)
            if exact_total_usd > 0
            else None
        )
        overcopy_per_filled_child_event_usd = (
            round(overcopy_usd / filled_child_events, 6)
            if filled_child_events > 0
            else None
        )
        exact_no_overcopy_groups = [row for row in groups if num(row.get("overcopy_usd")) <= 0]
        overcopy_research_groups = [row for row in groups if num(row.get("overcopy_usd")) > 0]

        def _fraction_floor_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
            submit_values: list[float] = []
            exact_values: list[float] = []
            delta_values: list[float] = []
            for row in rows:
                submit_value = row.get("submit_source_wallet_fraction")
                exact_value = row.get("exact_source_wallet_fraction")
                delta_value = row.get("source_wallet_fraction_floor_delta")
                if submit_value is not None:
                    submit_values.append(float(submit_value))
                if exact_value is not None:
                    exact_values.append(float(exact_value))
                if delta_value is not None:
                    delta_values.append(float(delta_value))
            submit_values = sorted(submit_values)
            exact_values = sorted(exact_values)
            delta_values = sorted(delta_values)
            if not submit_values:
                return {
                    "status": ANALYZE,
                    "role": "paper_only_source_wallet_fraction_floor_diagnostics",
                    "groups": 0,
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            return {
                "status": PASS,
                "role": "paper_only_source_wallet_fraction_floor_diagnostics",
                "groups": len(submit_values),
                "min_submit_source_wallet_fraction": round(submit_values[0], 9),
                "p50_submit_source_wallet_fraction": round(submit_values[len(submit_values) // 2], 9),
                "max_submit_source_wallet_fraction": round(submit_values[-1], 9),
                "min_exact_source_wallet_fraction": round(exact_values[0], 9) if exact_values else None,
                "max_exact_source_wallet_fraction": round(exact_values[-1], 9) if exact_values else None,
                "max_fraction_floor_delta": round(delta_values[-1], 9) if delta_values else 0.0,
                "requires_fraction_increase": any(value > 0 for value in delta_values),
                "paper_only": True,
                "live_orders_allowed": False,
            }

        source_wallet_fraction_floor_summary = _fraction_floor_summary(groups)
        exact_source_wallet_fraction_floor_summary = _fraction_floor_summary(exact_no_overcopy_groups)
        min_order_source_wallet_fraction_floor_summary = _fraction_floor_summary(overcopy_research_groups)
        return {
            "status": status,
            "role": "paper_only_micro_batch_all_order_probe_not_live_admission",
            "paper_only": True,
            "live_orders_allowed": False,
            "config": {
                "window_s": window_s,
                "min_batch_usd": min_batch_usd,
                "max_slippage_bps": max_slippage_bps,
                "max_event_age_s": max_event_age_s,
                "max_groups": max_groups,
            },
            "blockers": blockers,
            "source_buy_intents": len(buy_intents),
            "skipped_stale_buy_intents": skipped_stale,
            "probe_groups": len(groups),
            "groups_limited": groups_limited,
            "pass_groups": pass_groups,
            "rejected_groups": rejected_groups,
            "covered_child_events": covered_child_events,
            "filled_child_events": filled_child_events,
            "rejected_child_events": rejected_child_events,
            "child_fill_rate_pct": round(100.0 * filled_child_events / covered_child_events, 6)
            if covered_child_events
            else None,
            "exact_total_usd": round(exact_total_usd, 6),
            "submit_total_usd": round(submit_total_usd, 6),
            "overcopy_usd": round(overcopy_usd, 6),
            "overcopy_pct_of_exact_total": overcopy_pct_of_exact_total,
            "overcopy_per_filled_child_event_usd": overcopy_per_filled_child_event_usd,
            "source_wallet_fraction_floor_summary": source_wallet_fraction_floor_summary,
            "micro_batch_exact_no_overcopy": {
                "status": PASS if exact_no_overcopy_pass_groups > 0 and rejected_groups == 0 else ANALYZE,
                "role": "paper_only_exact_micro_batch_probe_no_overcopy",
                "research_only": False,
                "probe_groups": len(exact_no_overcopy_groups),
                "pass_groups": exact_no_overcopy_pass_groups,
                "covered_child_events": sum(int(row.get("child_count") or 0) for row in exact_no_overcopy_groups),
                "filled_child_events": exact_no_overcopy_filled_child_events,
                "child_fill_rate_pct": round(
                    100.0 * exact_no_overcopy_filled_child_events / covered_child_events,
                    6,
                )
                if covered_child_events
                else None,
                "requires_overcopy": False,
                "missing_exact_no_overcopy_reason": (
                    "all_pass_groups_require_min_order_overcopy"
                    if not exact_no_overcopy_pass_groups and overcopy_research_pass_groups
                    else None
                ),
                "source_wallet_fraction_floor_summary": exact_source_wallet_fraction_floor_summary,
                "live_orders_allowed": False,
            },
            "micro_batch_min_order_research": {
                "status": PASS if overcopy_research_pass_groups > 0 else ANALYZE,
                "role": "paper_only_micro_batch_min_order_research_overcopy_not_exact",
                "research_only": True,
                "pass_groups": overcopy_research_pass_groups,
                "filled_child_events": overcopy_research_filled_child_events,
                "child_fill_rate_pct": round(
                    100.0 * overcopy_research_filled_child_events / covered_child_events,
                    6,
                )
                if covered_child_events
                else None,
                "overcopy_usd": round(overcopy_usd, 6),
                "overcopy_pct_of_exact_total": overcopy_pct_of_exact_total,
                "overcopy_per_filled_child_event_usd": overcopy_per_filled_child_event_usd,
                "requires_overcopy": overcopy_usd > 0,
                "source_wallet_fraction_floor_summary": min_order_source_wallet_fraction_floor_summary,
                "live_orders_allowed": False,
            },
            "status_counts": dict(sorted(status_counts.items())),
            "fill_source_counts": dict(sorted(fill_source_counts.items())),
            "reject_reason_counts": dict(sorted(reject_reason_counts.items())),
            "sample_groups": groups[:20],
        }

    @staticmethod
    def _selected_aggressive_tactic_profile(execution_tactic_plan: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
        if not isinstance(execution_tactic_plan, dict):
            return None, {}
        recommended = str(execution_tactic_plan.get("recommended_tactic") or "")
        profile_id = ""
        if recommended.startswith("paper_profile_replay:"):
            profile_id = recommended.split(":", 1)[1].strip()
        else:
            best_profile = (
                execution_tactic_plan.get("best_profile")
                if isinstance(execution_tactic_plan.get("best_profile"), dict)
                else {}
            )
            if (
                best_profile
                and not best_profile.get("research_only")
                and int(best_profile.get("incremental_pass_events_vs_strict") or 0) > 0
            ):
                profile_id = str(execution_tactic_plan.get("best_profile_id") or "")
        if not profile_id or profile_id == "strict_current" or "micro_batch" in profile_id:
            return None, {}
        profile_summaries = (
            execution_tactic_plan.get("profile_summaries")
            if isinstance(execution_tactic_plan.get("profile_summaries"), dict)
            else {}
        )
        profile_summary = profile_summaries.get(profile_id) if isinstance(profile_summaries.get(profile_id), dict) else {}
        if profile_summary.get("research_only"):
            return None, {}
        return profile_id, profile_summary

    @staticmethod
    def _tactic_profile_replay_intent(intent: CopyIntent, *, profile_id: str) -> CopyIntent | None:
        metadata = dict(intent.metadata or {})
        evidence = metadata.get("live_tracking_evidence") if isinstance(metadata.get("live_tracking_evidence"), dict) else {}
        tactic_profiles = (
            evidence.get("paper_tactic_fillability")
            if isinstance(evidence.get("paper_tactic_fillability"), dict)
            else {}
        )
        profile = tactic_profiles.get(profile_id) if isinstance(tactic_profiles.get(profile_id), dict) else {}
        profile_status = str(profile.get("status") or profile.get("instant_fill_status") or "")
        if not profile or (profile_status != PASS and profile.get("instant_fill_status") != PASS):
            return None
        clob_book = {
            **profile,
            "status": "OK",
            "tactic_profile_id": profile_id,
            "tactic_profile_status": profile_status,
            "paper_tactic_replay": True,
            "strict_profile_status": (
                tactic_profiles.get("strict_current", {}).get("status")
                if isinstance(tactic_profiles.get("strict_current"), dict)
                else None
            ),
        }
        replay_evidence = {
            **evidence,
            "strict_clob_book": evidence.get("clob_book") if isinstance(evidence.get("clob_book"), dict) else {},
            "clob_book": clob_book,
            "paper_tactic_replay": {
                "profile_id": profile_id,
                "slippage_bps": profile.get("slippage_bps"),
                "source_all_order_intent_id": intent.intent_id,
                "role": "paper_only_aggressive_tactic_replay_not_live_admission",
                "live_orders_allowed": False,
            },
        }
        payload = intent.asdict()
        payload.pop("intent_id", None)
        payload["strategy_family"] = "wallet_copy_all_order_aggressive_tactic_replay_v1"
        payload["policy_id"] = f"paper_only_aggressive_tactic_replay_{profile_id}"
        payload["sizing_policy_id"] = f"{intent.sizing_policy_id}|tactic_profile_{profile_id}"
        payload["order_type"] = "PAPER_AGGRESSIVE_TACTIC_REPLAY_CLOB_EVIDENCE"
        payload["reason"] = (
            "paper-only aggressive CLOB tactic replay selected from measured fillability profile; "
            "not live-admission truth"
        )
        payload["live_orders_allowed"] = False
        metadata["live_tracking_evidence"] = replay_evidence
        metadata["source_all_order_intent_id"] = intent.intent_id
        metadata["paper_tactic_replay"] = {
            "profile_id": profile_id,
            "slippage_bps": profile.get("slippage_bps"),
            "profile_status": profile_status,
            "paper_only": True,
            "live_orders_allowed": False,
        }
        payload["metadata"] = metadata
        return CopyIntent.from_dict(payload)

    @staticmethod
    def _paper_order_fill_estimate(row: dict[str, Any]) -> dict[str, Any]:
        source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
        fill_estimate = source_intent.get("fill_estimate") if isinstance(source_intent.get("fill_estimate"), dict) else {}
        return fill_estimate

    @classmethod
    def _all_order_tactic_profile_matrix(
        cls,
        execution_tactic_plan: dict[str, Any],
        strict_current_orders: list[dict[str, Any]],
        *,
        replay_summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Compare strict, micro, and relaxed paper-only profiles for one poll."""

        def order_source_event_id(row: dict[str, Any]) -> str:
            source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
            return str(source_intent.get("source_event_id") or "")

        def fill_cost(rows: list[dict[str, Any]]) -> float:
            return round(sum(float(row.get("filled_size_usd") or 0.0) for row in rows), 6)

        strict_buy_orders = [
            row for row in strict_current_orders if isinstance(row, dict)
        ]
        strict_filled = [
            row for row in strict_buy_orders if str(row.get("final_status") or row.get("status") or "") == "FILLED"
        ]
        strict_rejected = [
            row for row in strict_buy_orders if str(row.get("final_status") or row.get("status") or "") == "REJECTED"
        ]
        strict_event_ids = sorted(
            {
                event_id
                for row in strict_buy_orders
                for event_id in [order_source_event_id(row)]
                if event_id
            }
        )
        strict_filled_event_ids = sorted(
            {
                event_id
                for row in strict_filled
                for event_id in [order_source_event_id(row)]
                if event_id
            }
        )
        replay = replay_summary if isinstance(replay_summary, dict) else {}
        profile_summaries = (
            execution_tactic_plan.get("profile_summaries")
            if isinstance(execution_tactic_plan.get("profile_summaries"), dict)
            else {}
        )
        micro = (
            execution_tactic_plan.get("micro_batch_summary")
            if isinstance(execution_tactic_plan.get("micro_batch_summary"), dict)
            else {}
        )
        micro_exact = (
            execution_tactic_plan.get("micro_batch_exact_no_overcopy")
            if isinstance(execution_tactic_plan.get("micro_batch_exact_no_overcopy"), dict)
            else {}
        )
        micro_research = (
            execution_tactic_plan.get("micro_batch_min_order_research")
            if isinstance(execution_tactic_plan.get("micro_batch_min_order_research"), dict)
            else {}
        )
        rows: list[dict[str, Any]] = [
            {
                "profile_id": "strict_current",
                "kind": "strict",
                "status": PASS if strict_buy_orders and not strict_rejected else active_status_from_blockers(
                    ["strict_rejected_orders_present"] if strict_rejected else ["no_strict_current_orders"],
                    default=ANALYZE,
                ),
                "buy_events": len(strict_buy_orders),
                "filled_events": len(strict_filled),
                "rejected_events": len(strict_rejected),
                "fill_rate_pct": round(100.0 * len(strict_filled) / len(strict_buy_orders), 6)
                if strict_buy_orders
                else None,
                "incremental_filled_events_vs_strict": 0,
                "cost_usd": fill_cost(strict_filled),
                "source_event_ids_sample": strict_event_ids[:20],
                "filled_source_event_ids_sample": strict_filled_event_ids[:20],
                "research_only": False,
            }
        ]
        if micro:
            rows.append(
                {
                    "profile_id": "micro_batch_exact_no_overcopy",
                    "kind": "micro_batch",
                    "status": micro_exact.get("status") or micro.get("status"),
                    "buy_events": micro.get("covered_child_events"),
                    "filled_events": micro.get("filled_child_events"),
                    "rejected_events": micro.get("rejected_child_events"),
                    "fill_rate_pct": micro.get("child_fill_rate_pct"),
                    "incremental_filled_events_vs_strict": micro.get("incremental_filled_child_events_vs_strict"),
                    "improves_strict_fill_count": micro.get("improves_strict_fill_count"),
                    "actionable_for_paper_replay": micro.get("actionable_for_paper_replay"),
                    "research_only_not_live_admission": micro.get("research_only_not_live_admission"),
                    "actionability_blockers": list(micro.get("actionability_blockers") or []),
                    "overcopy_usd": micro.get("overcopy_usd"),
                    "research_only": bool(micro_exact.get("research_only", False)),
                }
            )
            rows.append(
                {
                    "profile_id": "micro_batch_min_order_research",
                    "kind": "micro_batch",
                    "status": micro_research.get("status") or micro.get("status"),
                    "buy_events": micro.get("covered_child_events"),
                    "filled_events": micro.get("filled_child_events"),
                    "rejected_events": micro.get("rejected_child_events"),
                    "fill_rate_pct": micro.get("child_fill_rate_pct"),
                    "incremental_filled_events_vs_strict": micro.get("incremental_filled_child_events_vs_strict"),
                    "improves_strict_fill_count": micro_research.get(
                        "improves_strict_fill_count",
                        micro.get("improves_strict_fill_count"),
                    ),
                    "actionable_for_paper_replay": micro_research.get(
                        "actionable_for_paper_replay",
                        micro.get("actionable_for_paper_replay"),
                    ),
                    "research_only_not_live_admission": micro_research.get(
                        "research_only_not_live_admission",
                        micro.get("research_only_not_live_admission"),
                    ),
                    "actionability_blockers": list(
                        micro_research.get("actionability_blockers") or micro.get("actionability_blockers") or []
                    ),
                    "overcopy_usd": micro.get("overcopy_usd"),
                    "research_only": True,
                }
            )
        for profile_id, summary in sorted(
            profile_summaries.items(),
            key=lambda item: (
                float(item[1].get("slippage_bps") or 999999.0) if isinstance(item[1], dict) else 999999.0,
                str(item[0]),
            ),
        ):
            if not isinstance(summary, dict):
                continue
            if profile_id == "strict_current":
                continue
            rows.append(
                {
                    "profile_id": str(profile_id),
                    "kind": summary.get("kind"),
                    "status": PASS if int(summary.get("pass_events") or 0) > 0 else ANALYZE,
                    "max_slippage_bps": summary.get("slippage_bps"),
                    "buy_events": len(strict_buy_orders),
                    "filled_events": int(summary.get("pass_events") or 0),
                    "rejected_events": max(0, len(strict_buy_orders) - int(summary.get("pass_events") or 0)),
                    "fill_rate_pct": summary.get("fill_rate_pct_of_current_buys"),
                    "incremental_filled_events_vs_strict": int(
                        summary.get("incremental_pass_events_vs_strict") or 0
                    ),
                    "status_counts": summary.get("status_counts") if isinstance(summary.get("status_counts"), dict) else {},
                    "research_only": bool(summary.get("research_only")),
                }
            )
        selected_profile_id = str(replay.get("profile_id") or execution_tactic_plan.get("best_profile_id") or "")
        for row in rows:
            row["selected_for_replay"] = bool(selected_profile_id and row.get("profile_id") == selected_profile_id)
        return {
            "status": PASS if rows else ANALYZE,
            "role": "paper_only_candidate_scoped_tactic_profile_matrix_not_live_admission",
            "recommended_tactic": execution_tactic_plan.get("recommended_tactic"),
            "recommended_reason": execution_tactic_plan.get("recommended_reason"),
            "strict_buy_orders": len(strict_buy_orders),
            "strict_filled_buy_orders": len(strict_filled),
            "strict_rejected_buy_orders": len(strict_rejected),
            "strict_cost_usd": fill_cost(strict_filled),
            "selected_profile_id": selected_profile_id or None,
            "replay_status": replay.get("status"),
            "replay_filled_orders": replay.get("filled_orders"),
            "replay_rejected_orders": replay.get("rejected_orders"),
            "replay_cost_delta_usd": replay.get("cost_delta_usd"),
            "policy_profiles": rows,
            "blockers": sorted({str(row) for row in (execution_tactic_plan.get("blockers") or []) if row}),
            "paper_only": True,
            "live_orders_allowed": False,
        }

    def _all_order_aggressive_tactic_replay(
        self,
        events: list[WalletEvent],
        intents: list[CopyIntent],
        execution_tactic_plan: dict[str, Any],
        *,
        strict_current_orders: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Persist measured aggressive tactic profiles as a separate paper lifecycle lane."""

        def policy_validation(
            *,
            current_lifecycle_status: str,
            current_lifecycle_blockers: list[str] | None = None,
        ) -> dict[str, Any]:
            strict_state = self.all_order_exact_copy_paper.load_state()
            strict_orders = [
                row for row in strict_state.get("orders") or [] if isinstance(row, dict)
            ]
            tactic_state = self.all_order_tactic_replay_paper.load_state()
            tactic_orders = [
                row for row in tactic_state.get("orders") or [] if isinstance(row, dict)
            ]
            resolutions = (
                load_resolutions(self.config.resolutions_path)
                if strict_orders or tactic_orders
                else {}
            )
            pnl_attribution = score_tactic_replay_pnl(strict_orders, tactic_orders, resolutions)
            pnl_blockers = [str(row) for row in (pnl_attribution.get("blockers") or []) if row]
            return {
                "status": str(pnl_attribution.get("status") or ANALYZE),
                "role": "paper_only_aggressive_tactic_policy_validation_not_live_admission",
                "current_lifecycle_status": current_lifecycle_status,
                "current_lifecycle_blockers": sorted(
                    {str(row) for row in (current_lifecycle_blockers or []) if row}
                ),
                "pnl_attribution_status": pnl_attribution.get("status"),
                "pnl_attribution_blockers": pnl_blockers,
                "resolution_path": self.config.resolutions_path,
                "resolution_rows_indexed": len(resolutions),
                "strict_orders_scored": len(strict_orders),
                "tactic_orders_scored": len(tactic_orders),
                "pnl_attribution": pnl_attribution,
                "paper_only": True,
                "live_orders_allowed": False,
            }

        profile_id, profile_summary = self._selected_aggressive_tactic_profile(execution_tactic_plan)
        if not profile_id:
            validation = policy_validation(
                current_lifecycle_status="NO_ACTION",
                current_lifecycle_blockers=["no_actionable_aggressive_profile"],
            )
            diagnostics = (
                execution_tactic_plan.get("no_actionable_tactic_diagnostics")
                if isinstance(execution_tactic_plan.get("no_actionable_tactic_diagnostics"), dict)
                else {}
            )
            return {
                "status": "NO_ACTION",
                "role": "paper_only_aggressive_tactic_replay_not_live_admission",
                "reason": "no actionable aggressive CLOB profile improved strict all-order copy in this poll",
                "recommended_tactic": execution_tactic_plan.get("recommended_tactic"),
                "no_actionable_tactic_diagnostics": diagnostics,
                "profile_summaries": execution_tactic_plan.get("profile_summaries")
                if isinstance(execution_tactic_plan.get("profile_summaries"), dict)
                else {},
                "micro_batch_summary": execution_tactic_plan.get("micro_batch_summary")
                if isinstance(execution_tactic_plan.get("micro_batch_summary"), dict)
                else {},
                "paper_policy_profile_matrix": self._all_order_tactic_profile_matrix(
                    execution_tactic_plan,
                    strict_current_orders,
                ),
                "correction_counts": execution_tactic_plan.get("correction_counts")
                if isinstance(execution_tactic_plan.get("correction_counts"), dict)
                else {},
                "all_correction_counts": execution_tactic_plan.get("all_correction_counts")
                if isinstance(execution_tactic_plan.get("all_correction_counts"), dict)
                else {},
                "policy_validation": validation,
                "pnl_attribution_status": validation.get("pnl_attribution_status"),
                "pnl_attribution_blockers": validation.get("pnl_attribution_blockers"),
                "paper_only": True,
                "live_orders_allowed": False,
                "paper_state_path": self.config.all_order_tactic_replay_paper_state_path,
                "paper_event_log_path": self.config.all_order_tactic_replay_paper_event_log_path,
            }

        replay_intents = [
            replay
            for intent in intents
            for replay in [self._tactic_profile_replay_intent(intent, profile_id=profile_id)]
            if replay is not None
        ]
        if not replay_intents:
            validation = policy_validation(
                current_lifecycle_status=ANALYZE,
                current_lifecycle_blockers=["selected_profile_missing_per_intent_pass_evidence"],
            )
            return {
                "status": "ANALYZE",
                "role": "paper_only_aggressive_tactic_replay_not_live_admission",
                "profile_id": profile_id,
                "profile": profile_summary,
                "source_buy_intents": len([row for row in intents if str(row.action or "BUY").upper() == "BUY"]),
                "replay_intents": 0,
                "blockers": ["selected_profile_missing_per_intent_pass_evidence"],
                "paper_policy_profile_matrix": self._all_order_tactic_profile_matrix(
                    execution_tactic_plan,
                    strict_current_orders,
                    replay_summary={
                        "status": ANALYZE,
                        "profile_id": profile_id,
                        "filled_orders": 0,
                        "rejected_orders": 0,
                        "cost_delta_usd": None,
                    },
                ),
                "policy_validation": validation,
                "pnl_attribution_status": validation.get("pnl_attribution_status"),
                "pnl_attribution_blockers": validation.get("pnl_attribution_blockers"),
                "paper_only": True,
                "live_orders_allowed": False,
                "paper_state_path": self.config.all_order_tactic_replay_paper_state_path,
                "paper_event_log_path": self.config.all_order_tactic_replay_paper_event_log_path,
            }

        before_state = self.all_order_tactic_replay_paper.load_state()
        before_order_ids = {
            str(row.get("order_id"))
            for row in before_state.get("orders") or []
            if isinstance(row, dict) and row.get("order_id")
        }
        replay_intent_by_event_id = {intent.source_event_id: intent for intent in replay_intents}
        replay_state = self.all_order_tactic_replay_paper.apply_wallet_events_in_order(
            events,
            intent_by_event_id=replay_intent_by_event_id,
        )
        replay_intent_ids = {intent.intent_id for intent in replay_intents}
        replay_orders = [
            row
            for row in replay_state.get("orders") or []
            if isinstance(row, dict) and str(row.get("intent_id") or "") in replay_intent_ids
        ]
        new_replay_orders = [
            row for row in replay_orders if str(row.get("order_id") or "") not in before_order_ids
        ]
        filled_orders = [row for row in replay_orders if str(row.get("final_status") or "") == "FILLED"]
        rejected_orders = [row for row in replay_orders if str(row.get("final_status") or "") == "REJECTED"]
        fill_source_counts = Counter(
            str(self._paper_order_fill_estimate(row).get("source") or "unknown")
            for row in replay_orders
        )
        reject_reason_counts = Counter(
            str(self._paper_order_fill_estimate(row).get("reject_reason") or "")
            for row in rejected_orders
            if self._paper_order_fill_estimate(row).get("reject_reason")
        )
        clob_filled = sum(
            1
            for row in filled_orders
            if str(self._paper_order_fill_estimate(row).get("source") or "") == "clob_book_evidence"
        )
        fallback_filled = sum(
            1
            for row in filled_orders
            if str(self._paper_order_fill_estimate(row).get("source") or "") == "source_price_plus_slippage_fallback"
        )
        strict_cost_usd = sum(float(row.get("filled_size_usd") or 0.0) for row in strict_current_orders)
        tactic_cost_usd = sum(float(row.get("filled_size_usd") or 0.0) for row in replay_orders)
        strict_filled = sum(1 for row in strict_current_orders if str(row.get("final_status") or "") == "FILLED")
        def source_event_id_for_order(row: dict[str, Any]) -> str:
            source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
            return str(source_intent.get("source_event_id") or "")

        strict_status_by_source_event_id = {
            source_event_id: str(row.get("final_status") or row.get("status") or "")
            for row in strict_current_orders
            for source_event_id in [source_event_id_for_order(row)]
            if source_event_id
        }
        replay_status_by_source_event_id = {
            source_event_id: str(row.get("final_status") or row.get("status") or "")
            for row in replay_orders
            for source_event_id in [source_event_id_for_order(row)]
            if source_event_id
        }
        tactic_event_proofs = []
        for row in replay_orders:
            source_event_id = source_event_id_for_order(row)
            if not source_event_id:
                continue
            fill = self._paper_order_fill_estimate(row)
            source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
            metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
            replay_meta = metadata.get("paper_tactic_replay") if isinstance(metadata.get("paper_tactic_replay"), dict) else {}
            book = fill.get("book") if isinstance(fill.get("book"), dict) else {}
            tactic_event_proofs.append(
                {
                    "source_event_id": source_event_id,
                    "strict_status": strict_status_by_source_event_id.get(source_event_id),
                    "tactic_status": str(row.get("final_status") or ""),
                    "profile_id": replay_meta.get("profile_id") or profile_id,
                    "slippage_bps": replay_meta.get("slippage_bps") or profile_summary.get("slippage_bps"),
                    "fill_source": fill.get("source"),
                    "book_hash": book.get("book_hash"),
                    "strict_order_id": None,
                    "tactic_order_id": row.get("order_id"),
                    "requested_size_usd": row.get("requested_size_usd"),
                    "filled_size_usd": row.get("filled_size_usd"),
                    "effective_price": fill.get("effective_price"),
                    "fill_ratio": fill.get("fill_ratio"),
                    "cost_delta_vs_strict_usd": round(
                        float(row.get("filled_size_usd") or 0.0)
                        - sum(
                            float(strict_row.get("filled_size_usd") or 0.0)
                            for strict_row in strict_current_orders
                            if source_event_id_for_order(strict_row) == source_event_id
                        ),
                        6,
                    ),
                }
            )
        strict_order_id_by_source_event_id = {
            source_event_id_for_order(row): row.get("order_id")
            for row in strict_current_orders
            if source_event_id_for_order(row)
        }
        for proof in tactic_event_proofs:
            proof["strict_order_id"] = strict_order_id_by_source_event_id.get(str(proof.get("source_event_id") or ""))
        strict_rejected_source_event_ids = sorted(
            event_id
            for event_id, status in strict_status_by_source_event_id.items()
            if status == "REJECTED"
        )
        tactic_filled_source_event_ids = sorted(
            event_id
            for event_id, status in replay_status_by_source_event_id.items()
            if status == "FILLED"
        )
        incremental_source_event_ids = sorted(set(tactic_filled_source_event_ids) & set(strict_rejected_source_event_ids))
        blockers: list[str] = []
        if len(filled_orders) < len(replay_intents):
            blockers.append("not_all_tactic_replay_intents_filled")
        if rejected_orders:
            blockers.append("tactic_replay_rejected_orders_present")
        if fallback_filled:
            blockers.append("tactic_replay_fallback_fills_present")
        status = PASS if len(filled_orders) >= len(replay_intents) and not rejected_orders and not fallback_filled else "CORRECTION"
        validation = policy_validation(
            current_lifecycle_status=status,
            current_lifecycle_blockers=blockers,
        )
        replay_summary = {
            "status": status,
            "profile_id": profile_id,
            "filled_orders": len(filled_orders),
            "rejected_orders": len(rejected_orders),
            "cost_delta_usd": round(tactic_cost_usd - strict_cost_usd, 6),
        }
        return {
            "status": status,
            "role": "paper_only_aggressive_tactic_replay_not_live_admission",
            "profile_id": profile_id,
            "profile": profile_summary,
            "recommended_tactic": execution_tactic_plan.get("recommended_tactic"),
            "source_buy_intents": len([row for row in intents if str(row.action or "BUY").upper() == "BUY"]),
            "replay_intents": len(replay_intents),
            "paper_orders": len(replay_orders),
            "new_paper_orders": len(new_replay_orders),
            "filled_orders": len(filled_orders),
            "rejected_orders": len(rejected_orders),
            "clob_filled_orders": clob_filled,
            "fallback_filled_orders": fallback_filled,
            "strict_current_filled_orders": strict_filled,
            "incremental_filled_orders_vs_strict": max(0, len(filled_orders) - strict_filled),
            "strict_rejected_source_event_ids": strict_rejected_source_event_ids[:50],
            "tactic_filled_source_event_ids": tactic_filled_source_event_ids[:50],
            "incremental_source_event_ids_vs_strict": incremental_source_event_ids[:50],
            "event_proofs": tactic_event_proofs[:50],
            "strict_cost_usd": round(strict_cost_usd, 6),
            "tactic_cost_usd": round(tactic_cost_usd, 6),
            "cost_delta_usd": round(tactic_cost_usd - strict_cost_usd, 6),
            "paper_policy_profile_matrix": self._all_order_tactic_profile_matrix(
                execution_tactic_plan,
                strict_current_orders,
                replay_summary=replay_summary,
            ),
            "fill_source_counts": dict(sorted(fill_source_counts.items())),
            "reject_reason_counts": dict(sorted(reject_reason_counts.items())),
            "blockers": sorted(set(blockers)),
            "policy_validation": validation,
            "pnl_attribution_status": validation.get("pnl_attribution_status"),
            "pnl_attribution_blockers": validation.get("pnl_attribution_blockers"),
            "paper_state_path": self.config.all_order_tactic_replay_paper_state_path,
            "paper_event_log_path": self.config.all_order_tactic_replay_paper_event_log_path,
            "paper_summary": replay_state.get("summary") if isinstance(replay_state.get("summary"), dict) else {},
            "live_admission_note": (
                "paper_only_replay; promotion requires resolved PnL attribution and later ordinary live-gated "
                "CopyIntent truth, not this tactic lane alone"
            ),
            "paper_only": True,
            "live_orders_allowed": False,
        }

    def _seed_history_into_paper(
        self,
        new_events: list[WalletEvent],
        *,
        specs: list[WalletSpec],
        policy: CopyPolicy,
        profit_policy: CandidatePolicy | None,
        paper_engine: PaperWalletCopyEngine,
        now_ts: float,
    ) -> dict[str, Any]:
        if not self.config.seed_before_poll:
            return {"status": "DISABLED"}
        if not self.config.seed_history_state_path:
            return {"status": "BLOCKED", "reason": "missing_seed_history_state_path"}

        payload = load_json(self.config.seed_history_state_path, default={})
        if not isinstance(payload, dict):
            return {"status": "BLOCKED", "reason": "invalid_seed_history_state"}

        raw_rows = payload.get("events") if isinstance(payload.get("events"), list) else []
        scoped_wallets = {spec.normalized_address() for spec in specs}
        current_batch_keys = {key for event in new_events for key in _seed_exclusion_keys(event)}
        cutoff_ts = now_ts
        min_seed_ts = cutoff_ts - float(self.config.seed_lookback_s) if self.config.seed_lookback_s > 0 else None
        seed_events: list[WalletEvent] = []
        skipped_after_cutoff = 0
        skipped_current_batch = 0
        skipped_current_batch_action_counts: Counter[str] = Counter()
        skipped_scope = 0
        skipped_lookback = 0

        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            try:
                event = WalletEvent.from_dict(row)
            except TypeError:
                continue
            if scoped_wallets and event.source_wallet.lower() not in scoped_wallets:
                skipped_scope += 1
                continue
            if _seed_exclusion_keys(event) & current_batch_keys:
                skipped_current_batch += 1
                skipped_current_batch_action_counts[str(event.action or "").upper() or "UNKNOWN"] += 1
                continue
            if event.event_ts is not None and float(event.event_ts) >= cutoff_ts:
                skipped_after_cutoff += 1
                continue
            if min_seed_ts is not None and (event.event_ts is None or float(event.event_ts) < min_seed_ts):
                skipped_lookback += 1
                continue
            seed_events.append(event)

        seed_events = sorted(seed_events, key=lambda event: (event.event_ts or 0.0, event.event_id))
        if self.config.seed_max_events > 0:
            seed_events = seed_events[-int(self.config.seed_max_events) :]
        if not seed_events:
            return {
                "status": "NO_SEED_EVENTS",
                "history_state_path": self.config.seed_history_state_path,
                "cutoff_ts": cutoff_ts,
                "current_batch_event_action_counts": _wallet_event_action_counts(new_events),
                "current_batch_dual_action_event_id_count": _dual_action_event_id_count(new_events),
                "current_batch_same_tx_opposing_action_count": _same_tx_opposing_action_count(new_events),
                "skipped_current_batch": skipped_current_batch,
                "skipped_current_batch_action_counts": dict(sorted(skipped_current_batch_action_counts.items())),
                "skipped_after_cutoff": skipped_after_cutoff,
                "skipped_scope": skipped_scope,
                "skipped_lookback": skipped_lookback,
                "paper_only": True,
                "live_orders_allowed": False,
            }

        before = paper_engine.load_state()
        before_order_count = len([row for row in before.get("orders") or [] if isinstance(row, dict)])
        before_lifecycle_count = len([row for row in before.get("lifecycle_events") or [] if isinstance(row, dict)])
        seed_intent_by_event_id: dict[str, CopyIntent] = {}
        seed_buy_events = 0
        seed_lifecycle_events = 0
        seed_profit_filtered_buy_events = 0
        for event in seed_events:
            if not event.is_buy:
                seed_lifecycle_events += 1
                continue
            seed_buy_events += 1
            if profit_policy is not None:
                accepted, _reason = policy_accepts_event(profit_policy, event)
                if not accepted:
                    seed_profit_filtered_buy_events += 1
                    continue
            intent = event_to_intent(event, policy=policy, mode="paper", now_ts=now_ts)
            if intent is not None:
                seed_intent_by_event_id[event.event_id] = intent

        after = paper_engine.apply_wallet_events_in_order(seed_events, intent_by_event_id=seed_intent_by_event_id)
        after_order_count = len([row for row in after.get("orders") or [] if isinstance(row, dict)])
        after_lifecycle_count = len([row for row in after.get("lifecycle_events") or [] if isinstance(row, dict)])
        return {
            "status": "SEEDED",
            "history_state_path": self.config.seed_history_state_path,
            "cutoff_ts": cutoff_ts,
            "current_batch_event_action_counts": _wallet_event_action_counts(new_events),
            "current_batch_dual_action_event_id_count": _dual_action_event_id_count(new_events),
            "current_batch_same_tx_opposing_action_count": _same_tx_opposing_action_count(new_events),
            "seed_events": len(seed_events),
            "seed_event_action_counts": _wallet_event_action_counts(seed_events),
            "seed_buy_events": seed_buy_events,
            "seed_lifecycle_events": seed_lifecycle_events,
            "seed_intents": len(seed_intent_by_event_id),
            "seed_profit_filtered_buy_events": seed_profit_filtered_buy_events,
            "new_seed_orders": after_order_count - before_order_count,
            "new_seed_lifecycle_events": after_lifecycle_count - before_lifecycle_count,
            "paper_orders_after_seed": after_order_count,
            "paper_lifecycle_events_after_seed": after_lifecycle_count,
            "skipped_current_batch": skipped_current_batch,
            "skipped_current_batch_action_counts": dict(sorted(skipped_current_batch_action_counts.items())),
            "skipped_after_cutoff": skipped_after_cutoff,
            "skipped_scope": skipped_scope,
            "skipped_lookback": skipped_lookback,
            "paper_only": True,
            "live_orders_allowed": False,
        }

    def _seed_history_before_poll(
        self,
        new_events: list[WalletEvent],
        *,
        specs: list[WalletSpec],
        policy: CopyPolicy,
        profit_policy: CandidatePolicy | None,
        now_ts: float,
    ) -> dict[str, Any]:
        return self._seed_history_into_paper(
            new_events,
            specs=specs,
            policy=policy,
            profit_policy=profit_policy,
            paper_engine=self.paper,
            now_ts=now_ts,
        )

    def _copyability_policy(self) -> CopyabilityPolicy:
        return CopyabilityPolicy(
            max_event_age_s=float(self.config.max_copyability_event_age_s),
            max_wallet_fetch_duration_s=float(self.config.max_wallet_fetch_duration_s),
            min_clob_fill_ratio=float(self.config.min_copyability_clob_fill_ratio),
            require_clob_book=bool(self.config.require_clob_book_evidence_for_efficiency),
        )

    def _enrich_event(
        self,
        event: WalletEvent,
        copy_size_usd: float,
        market_ws_rows: list[dict[str, Any]],
        *,
        wallet_fetch_report: dict[str, Any] | None = None,
        clob_book_cache: dict[str, dict[str, Any]] | None = None,
        clob_book_cache_report: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        fetch_report = wallet_fetch_report if isinstance(wallet_fetch_report, dict) else {}
        raw = event.raw if isinstance(event.raw, dict) else {}
        source_fetch_duration_s = raw.get("_walletCopySourceFetchDurationS")
        source_fetch_started_ts = raw.get("_walletCopySourceFetchStartedTs")
        source_fetch_completed_ts = raw.get("_walletCopySourceFetchCompletedTs")
        batch_fetch_duration_s = fetch_report.get("fetch_duration_s")
        if source_fetch_duration_s is not None:
            selected_fetch_duration_s = source_fetch_duration_s
            selected_fetch_started_ts = source_fetch_started_ts
            selected_fetch_completed_ts = source_fetch_completed_ts
            fetch_duration_basis = "source_fetch_duration_s"
        else:
            selected_fetch_duration_s = batch_fetch_duration_s
            selected_fetch_started_ts = fetch_report.get("fetch_started_ts")
            selected_fetch_completed_ts = fetch_report.get("fetch_completed_ts")
            fetch_duration_basis = "wallet_batch_fetch_duration_s"
        identity_status, raw_wallet_identities = wallet_identity_status(
            raw,
            WalletSpec(name=event.wallet_name, address=event.source_wallet),
        )
        wallet_api_status = (
            "TRUTH_CONFIRMED"
            if identity_status == "MATCHED"
            else ("IDENTITY_UNVERIFIED" if identity_status == "MISSING" else "IDENTITY_MISMATCH")
        )
        event_age = float(event.age_s) if event.age_s is not None else None
        event_age_at_source_fetch_start_s = None
        try:
            if source_fetch_started_ts is not None and event.event_ts is not None:
                event_age_at_source_fetch_start_s = max(0.0, float(source_fetch_started_ts) - float(event.event_ts))
        except (TypeError, ValueError):
            event_age_at_source_fetch_start_s = None
        max_admission_event_age_s = float(self.config.max_copyability_event_age_s)
        clob_admission_relevant = bool(
            event.is_buy
            and event_age is not None
            and event_age <= max_admission_event_age_s
        )
        source_feed_stale_for_admission = bool(
            event.is_buy
            and event_age_at_source_fetch_start_s is not None
            and event_age_at_source_fetch_start_s > max_admission_event_age_s
        )
        clob_common = {
            "admission_relevant": clob_admission_relevant if event.is_buy else False,
            "event_age_s": event_age,
            "event_age_at_source_fetch_start_s": (
                round(event_age_at_source_fetch_start_s, 6)
                if event_age_at_source_fetch_start_s is not None
                else None
            ),
            "source_feed_stale_for_admission": source_feed_stale_for_admission,
            "max_admission_event_age_s": max_admission_event_age_s,
        }
        evidence: dict[str, Any] = {
            "schema_version": 1,
            "wallet_api": {
                "status": wallet_api_status,
                "requested_wallet": event.source_wallet.lower(),
                "raw_wallet_identity_status": identity_status,
                "raw_wallet_identities": raw_wallet_identities,
                "raw_proxy_wallet": raw.get("proxyWallet") or raw.get("proxy_wallet"),
                "raw_user": raw.get("user"),
                "row_type": event.row_type,
                "api_latency_s": event.api_latency_s,
                "api_latency_basis": "legacy_wallet_data_api_event_age_s",
                "event_age_s": event.age_s,
                "event_age_at_source_fetch_start_s": (
                    round(event_age_at_source_fetch_start_s, 6)
                    if event_age_at_source_fetch_start_s is not None
                    else None
                ),
                "observed_ts": event.observed_ts,
                "event_ts": event.event_ts,
                "data_api_endpoint": "/trades" if event.row_type == "trade" else "/activity",
                "data_api_query_param": raw.get("_walletCopyQueryKey"),
                "data_api_source": raw.get("_walletCopySource"),
                "data_api_trade_query_keys": list(self.config.trade_query_keys),
                "data_api_trade_query_scope": ",".join(self.config.trade_query_keys),
                "fetch_duration_s": selected_fetch_duration_s,
                "fetch_duration_basis": fetch_duration_basis,
                "fetch_started_ts": selected_fetch_started_ts,
                "fetch_completed_ts": selected_fetch_completed_ts,
                "source_fetch_duration_s": source_fetch_duration_s,
                "source_fetch_started_ts": source_fetch_started_ts,
                "source_fetch_completed_ts": source_fetch_completed_ts,
                "wallet_batch_fetch_duration_s": batch_fetch_duration_s,
                "wallet_batch_fetch_started_ts": fetch_report.get("fetch_started_ts"),
                "wallet_batch_fetch_completed_ts": fetch_report.get("fetch_completed_ts"),
                "source_fetch_source": raw.get("_walletCopySourceFetchSource"),
                "source_fetch_offset": raw.get("_walletCopySourceFetchOffset"),
                "source_fetch_limit": raw.get("_walletCopySourceFetchLimit"),
                "source_fingerprint": event.source_fingerprint,
                "route_status": raw.get("_walletCopySourceRouteStatus"),
                "route_class": (
                    raw.get("__walletCopySourceRouteReport", {}).get("route_class")
                    if isinstance(raw.get("__walletCopySourceRouteReport"), dict)
                    else None
                ),
                "route_report_id": (
                    raw.get("__walletCopySourceRouteReport", {}).get("route_report_id")
                    if isinstance(raw.get("__walletCopySourceRouteReport"), dict)
                    else None
                ),
                "route_host": raw.get("_walletCopySourceRouteHost"),
                "route_original_host": raw.get("_walletCopySourceRouteOriginalHost"),
                "routed_host": raw.get("_walletCopySourceRouteRoutedHost"),
                "source_base_override_configured": bool(raw.get("_walletCopySourceBaseOverrideConfigured")),
                "source_base_override_env_var": raw.get("_walletCopySourceBaseOverrideEnvVar"),
                "request_fingerprint": (
                    raw.get("__walletCopySourceRouteReport", {}).get("request_fingerprint")
                    if isinstance(raw.get("__walletCopySourceRouteReport"), dict)
                    else None
                ),
                "request_role": (
                    raw.get("__walletCopySourceRouteReport", {}).get("request_role")
                    if isinstance(raw.get("__walletCopySourceRouteReport"), dict)
                    else None
                ),
                "attempt_count": (
                    raw.get("__walletCopySourceRouteReport", {}).get("attempt_count")
                    if isinstance(raw.get("__walletCopySourceRouteReport"), dict)
                    else None
                ),
                "reset_attempt_count": (
                    raw.get("__walletCopySourceRouteReport", {}).get("reset_attempt_count")
                    if isinstance(raw.get("__walletCopySourceRouteReport"), dict)
                    else None
                ),
                "elapsed_ms_total": (
                    raw.get("__walletCopySourceRouteReport", {}).get("elapsed_ms_total")
                    if isinstance(raw.get("__walletCopySourceRouteReport"), dict)
                    else None
                ),
                "query_param_keys": (
                    raw.get("__walletCopySourceRouteReport", {}).get("query_param_keys")
                    if isinstance(raw.get("__walletCopySourceRouteReport"), dict)
                    else None
                ),
            },
            "market_ws_preconfirm": _match_preconfirm(
                event,
                market_ws_rows,
                window_s=self.config.preconfirm_match_window_s,
                price_tolerance=self.config.preconfirm_price_tolerance,
            ),
            "clob_book": {"enabled": self.config.enable_clob_books, "status": "SKIPPED", **clob_common},
            "onchain": {"enabled": self.config.enable_onchain_receipts, "status": "SKIPPED"},
        }
        token_id = event.token_id
        should_fetch_clob_book = bool(
            self.config.enable_clob_books
            and token_id
            and event.is_buy
            and not (self.config.admission_mode and source_feed_stale_for_admission)
        )
        if (
            self.config.enable_clob_books
            and token_id
            and event.is_buy
            and not should_fetch_clob_book
            and source_feed_stale_for_admission
        ):
            if clob_book_cache_report is not None:
                clob_book_cache_report["stale_not_admission_relevant_skipped"] = int(
                    clob_book_cache_report.get("stale_not_admission_relevant_skipped") or 0
                ) + 1
            evidence["clob_book"] = {
                "enabled": True,
                "status": "SKIPPED_STALE_SOURCE_FEED_NOT_ADMISSION_RELEVANT",
                "reason": "event_age_at_source_fetch_start_above_cap",
                "token_id": token_id,
                "cache_key": str(token_id),
                **clob_common,
            }
        if should_fetch_clob_book:
            cache_key = str(token_id)
            cache_hit = False
            fetch_duration_s: float | None = None
            if clob_book_cache_report is not None:
                clob_book_cache_report["lookup_events"] = int(clob_book_cache_report.get("lookup_events") or 0) + 1
                tokens = clob_book_cache_report.setdefault("token_ids", set())
                if isinstance(tokens, set):
                    tokens.add(cache_key)
            try:
                if clob_book_cache is not None and cache_key in clob_book_cache:
                    book = clob_book_cache[cache_key]
                    cache_hit = True
                    if clob_book_cache_report is not None:
                        clob_book_cache_report["cache_hits"] = int(clob_book_cache_report.get("cache_hits") or 0) + 1
                else:
                    if clob_book_cache_report is not None:
                        clob_book_cache_report["cache_misses"] = int(
                            clob_book_cache_report.get("cache_misses") or 0
                        ) + 1
                    fetch_started = time.perf_counter()
                    book = self.clob.get_book(token_id)
                    fetch_duration_s = round(max(0.0, time.perf_counter() - fetch_started), 6)
                    if clob_book_cache is not None:
                        clob_book_cache[cache_key] = book
                    if clob_book_cache_report is not None:
                        durations = clob_book_cache_report.setdefault("fetch_durations_s", [])
                        if isinstance(durations, list):
                            durations.append(fetch_duration_s)
                clob_summary = CLOBMarketClient.summarize_book(
                    book,
                    copy_size_usd=copy_size_usd,
                    source_price=event.price,
                    max_slippage_bps=self.config.max_book_slippage_bps,
                )
                clob_route_report = (
                    book.get("__walletCopyClobRouteReport")
                    if isinstance(book.get("__walletCopyClobRouteReport"), dict)
                    else {}
                )
                evidence["clob_book"] = {
                    "enabled": True,
                    "status": "OK",
                    "cache_key": cache_key,
                    "cache_hit": cache_hit,
                    "fetch_duration_s": fetch_duration_s,
                    "route_status": clob_route_report.get("status"),
                    "route_class": clob_route_report.get("route_class"),
                    "route_report_id": clob_route_report.get("route_report_id"),
                    "route_host": clob_route_report.get("host"),
                    "route_original_host": clob_route_report.get("original_host"),
                    "routed_host": clob_route_report.get("routed_host"),
                    "source_base_override_configured": bool(
                        clob_route_report.get("source_base_override_configured")
                    ),
                    "source_base_override_env_var": clob_route_report.get("source_base_override_env_var"),
                    "request_fingerprint": clob_route_report.get("request_fingerprint"),
                    "request_role": clob_route_report.get("request_role"),
                    "attempt_count": clob_route_report.get("attempt_count"),
                    "reset_attempt_count": clob_route_report.get("reset_attempt_count"),
                    "elapsed_ms_total": clob_route_report.get("elapsed_ms_total"),
                    **clob_common,
                    **clob_summary,
                }
                evidence["paper_tactic_fillability"] = CLOBMarketClient.tactical_fillability_profiles(
                    book,
                    copy_size_usd=copy_size_usd,
                    source_price=event.price,
                    base_slippage_bps=self.config.max_book_slippage_bps,
                )
            except requests.HTTPError as exc:
                if clob_book_cache_report is not None:
                    clob_book_cache_report["errors"] = int(clob_book_cache_report.get("errors") or 0) + 1
                status_code = getattr(exc.response, "status_code", None)
                if status_code == 404:
                    clob_route_report = self.clob.last_route_report if isinstance(self.clob.last_route_report, dict) else {}
                    evidence["clob_book"] = {
                        "enabled": True,
                        "status": "BOOK_NOT_FOUND_OR_CLOSED",
                        "token_id": token_id,
                        "cache_key": cache_key,
                        "cache_hit": cache_hit,
                        "fetch_duration_s": fetch_duration_s,
                        "route_status": clob_route_report.get("status"),
                        "route_class": clob_route_report.get("route_class"),
                        "route_report_id": clob_route_report.get("route_report_id"),
                        "routed_host": clob_route_report.get("routed_host"),
                        "request_fingerprint": clob_route_report.get("request_fingerprint"),
                        "error": str(exc),
                        **clob_common,
                    }
                else:
                    clob_route_report = self.clob.last_route_report if isinstance(self.clob.last_route_report, dict) else {}
                    evidence["clob_book"] = {
                        "enabled": True,
                        "status": "ERROR",
                        "cache_key": cache_key,
                        "cache_hit": cache_hit,
                        "fetch_duration_s": fetch_duration_s,
                        "route_status": clob_route_report.get("status"),
                        "route_class": clob_route_report.get("route_class"),
                        "route_report_id": clob_route_report.get("route_report_id"),
                        "routed_host": clob_route_report.get("routed_host"),
                        "request_fingerprint": clob_route_report.get("request_fingerprint"),
                        "error": str(exc),
                        **clob_common,
                    }
            except Exception as exc:  # noqa: BLE001 - evidence should record transport failures
                if clob_book_cache_report is not None:
                    clob_book_cache_report["errors"] = int(clob_book_cache_report.get("errors") or 0) + 1
                clob_route_report = self.clob.last_route_report if isinstance(self.clob.last_route_report, dict) else {}
                evidence["clob_book"] = {
                    "enabled": True,
                    "status": "ERROR",
                    "cache_key": cache_key,
                    "cache_hit": cache_hit,
                    "fetch_duration_s": fetch_duration_s,
                    "route_status": clob_route_report.get("status"),
                    "route_class": clob_route_report.get("route_class"),
                    "route_report_id": clob_route_report.get("route_report_id"),
                    "routed_host": clob_route_report.get("routed_host"),
                    "request_fingerprint": clob_route_report.get("request_fingerprint"),
                    "error": str(exc),
                    **clob_common,
                }
        elif self.config.enable_clob_books and not token_id:
            token_ids = self.gamma.clob_token_ids_for_event(event)
            gamma_error = self.gamma.last_error if isinstance(self.gamma.last_error, dict) else {}
            evidence["gamma_market"] = (
                {**gamma_error, "clob_token_ids": token_ids}
                if gamma_error
                else {"status": "LOOKUP", "clob_token_ids": token_ids}
            )

        if self.config.enable_onchain_receipts and event.transaction_hash:
            try:
                receipt = self.onchain.get_receipt(event.transaction_hash)
                evidence["onchain"] = {
                    "enabled": True,
                    **OnchainReceiptClient.summarize_receipt(
                        receipt,
                        tx_hash=event.transaction_hash,
                        wallet=event.source_wallet,
                    ),
                }
            except Exception as exc:  # noqa: BLE001
                evidence["onchain"] = {"enabled": True, "status": "ERROR", "error": str(exc)}
        return evidence

    def _mirror_result(
        self,
        event: WalletEvent,
        *,
        policy: CopyPolicy,
        profit_policy: CandidatePolicy | None,
        profit_filter_decisions: dict[str, tuple[bool, str]],
        copyability_decisions: dict[str, CopyabilityDecision],
        intent_by_event_id: dict[str, CopyIntent],
        paper_order_by_intent_id: dict[str, dict[str, Any]],
        lifecycle_by_event_id: dict[str, dict[str, Any]],
        filtered_entry_context_by_key: dict[str, dict[str, Any]],
        seed_report: dict[str, Any] | None,
        now_ts: float,
    ) -> dict[str, Any]:
        action = event.action.upper()
        if action == "BUY":
            allowed, filter_reason = profit_filter_decisions.get(event.event_id, (True, "accepted"))
            if profit_policy is not None and not allowed:
                return {
                    "coverage_status": "FILTERED",
                    "mirror_status": "FILTERED_BY_PROFIT_POLICY",
                    "copy_action": "FILTERED_NO_COPY",
                    "filter_policy": "profit_policy",
                    "profit_policy_id": profit_policy.policy_id,
                    "profit_policy_context_id": profit_policy.policy_id,
                    "reason": filter_reason,
                }
            copyability = copyability_decisions.get(event.event_id)
            if copyability is not None and not copyability.accepted:
                result = {
                    "coverage_status": "FILTERED",
                    "mirror_status": "FILTERED_BY_COPYABILITY_POLICY",
                    "copy_action": "FILTERED_NO_COPY",
                    "filter_policy": "copyability",
                    "copyability_policy_id": copyability.policy_id,
                    "copyability_decision": copyability.asdict(),
                    "reason": copyability.reason,
                }
                if profit_policy is not None:
                    result["profit_policy_id"] = profit_policy.policy_id
                    result["profit_policy_context_id"] = profit_policy.policy_id
                return result
            intent = intent_by_event_id.get(event.event_id)
            if intent is not None:
                paper_order = paper_order_by_intent_id.get(intent.intent_id) or {}
                final_status = str(paper_order.get("final_status") or "")
                if final_status == "REJECTED":
                    return {
                        "coverage_status": "MIRRORED",
                        "mirror_status": "MIRRORED_BUY_TO_PAPER_REJECTED_BY_FILL_MODEL",
                        "copy_action": "COPY_BUY_REJECTED_BY_FILL_MODEL",
                        "intent_id": intent.intent_id,
                        "order_id": paper_order.get("order_id"),
                        "fill_model": paper_order.get("fill_model"),
                        "fill_estimate": (paper_order.get("source_intent") or {}).get("fill_estimate")
                        if isinstance(paper_order.get("source_intent"), dict)
                        else None,
                        "reason": "wallet BUY produced a paper CopyIntent but executable fill model rejected the paper order",
                    }
                return {
                    "coverage_status": "MIRRORED",
                    "mirror_status": "MIRRORED_BUY_TO_PAPER",
                    "copy_action": "COPY_BUY_TO_PAPER",
                    "intent_id": intent.intent_id,
                    "order_type": intent.order_type,
                    "copy_size_usd": intent.copy_size_usd,
                    "shares": intent.shares,
                    "reason": "wallet BUY produced a paper CopyIntent",
                }
            accepted, reason = policy.accepts(event, now_ts=now_ts)
            return {
                "coverage_status": "VIOLATION",
                "mirror_status": "COVERAGE_VIOLATION",
                "copy_action": "NO_COPY_INTENT",
                "policy_accepts": accepted,
                "reason": reason,
            }
        if action in {"SELL", "MERGE", "REDEEM"}:
            lifecycle_event = lifecycle_by_event_id.get(event.event_id) or {}
            reduction = lifecycle_event.get("position_reduction") if isinstance(lifecycle_event.get("position_reduction"), dict) else {}
            reduction_status = str(reduction.get("status") or "MISSING_LIFECYCLE_REDUCTION")
            if reduction_status in {"NO_MATCHING_POSITION", "ZERO_REDUCTION", "NO_PAIRED_POSITION", "MISSING_LIFECYCLE_REDUCTION"}:
                seed_status = str((seed_report or {}).get("status") or "UNKNOWN")
                filtered_entry_context = _lookup_filtered_entry_context(event, filtered_entry_context_by_key)
                dominant_filter_policy = str(filtered_entry_context.get("dominant_filter_policy") or "")
                if dominant_filter_policy == "copyability":
                    return {
                        "coverage_status": "FILTERED",
                        "mirror_status": "FILTERED_BY_COPYABILITY_ENTRY",
                        "copy_action": "FILTERED_LIFECYCLE_NO_ENTRY_POSITION",
                        "filter_policy": "copyability",
                        "lifecycle_filter_class": "lifecycle_filtered_due_to_entry_copyability",
                        "lifecycle_status": reduction_status,
                        "seed_status": seed_status,
                        "filtered_entry_context": filtered_entry_context or None,
                        "position_reduction": reduction,
                        "reason": "lifecycle_filtered_due_to_entry_copyability",
                    }
                elif dominant_filter_policy == "profit_policy":
                    return {
                        "coverage_status": "FILTERED",
                        "mirror_status": "FILTERED_BY_PROFIT_ENTRY",
                        "copy_action": "FILTERED_LIFECYCLE_NO_ENTRY_POSITION",
                        "filter_policy": "profit_policy",
                        "lifecycle_filter_class": "lifecycle_filtered_due_to_entry_profit_policy",
                        "lifecycle_status": reduction_status,
                        "seed_status": seed_status,
                        "filtered_entry_context": filtered_entry_context or None,
                        "position_reduction": reduction,
                        "reason": "lifecycle_filtered_due_to_entry_profit_policy",
                    }
                elif dominant_filter_policy == "copy_policy":
                    return {
                        "coverage_status": "FILTERED",
                        "mirror_status": "FILTERED_BY_COPY_POLICY_ENTRY",
                        "copy_action": "FILTERED_LIFECYCLE_NO_ENTRY_POSITION",
                        "filter_policy": "copy_policy",
                        "lifecycle_filter_class": "lifecycle_filtered_due_to_entry_copy_policy",
                        "lifecycle_status": reduction_status,
                        "seed_status": seed_status,
                        "filtered_entry_context": filtered_entry_context or None,
                        "position_reduction": reduction,
                        "reason": "lifecycle_filtered_due_to_entry_copy_policy",
                    }
                if seed_status == "SEEDED" and reduction_status in {
                    "NO_MATCHING_POSITION",
                    "NO_PAIRED_POSITION",
                    "ZERO_REDUCTION",
                }:
                    return {
                        "coverage_status": "FILTERED",
                        "mirror_status": "FILTERED_BY_PREEXISTING_POSITION_GAP",
                        "copy_action": "FILTERED_LIFECYCLE_NO_ENTRY_POSITION",
                        "filter_policy": "preexisting_position_gap",
                        "lifecycle_filter_class": "lifecycle_filtered_due_to_preexisting_position_gap",
                        "lifecycle_status": reduction_status,
                        "seed_status": seed_status,
                        "filtered_entry_context": filtered_entry_context or None,
                        "position_reduction": reduction,
                        "reason": "lifecycle_filtered_due_to_preexisting_position_gap",
                    }
                if seed_status != "SEEDED" and action in {"MERGE", "REDEEM"} and reduction_status in {
                    "NO_MATCHING_POSITION",
                    "NO_PAIRED_POSITION",
                    "ZERO_REDUCTION",
                }:
                    return {
                        "coverage_status": "FILTERED",
                        "mirror_status": "FILTERED_BY_PREEXISTING_POSITION_GAP",
                        "copy_action": "FILTERED_LIFECYCLE_NO_ENTRY_POSITION",
                        "filter_policy": "preexisting_position_gap",
                        "lifecycle_filter_class": "lifecycle_filtered_due_to_preexisting_position_gap",
                        "lifecycle_status": reduction_status,
                        "seed_status": seed_status,
                        "filtered_entry_context": filtered_entry_context or None,
                        "position_reduction": reduction,
                        "reason": "cold_start_merge_redeem_without_copied_entry_position",
                    }
                if seed_status != "SEEDED" and reduction_status in {"NO_MATCHING_POSITION", "NO_PAIRED_POSITION"}:
                    lifecycle_miss_class = "lifecycle_miss_no_seed_position"
                elif seed_status == "SEEDED" and reduction_status in {"NO_MATCHING_POSITION", "NO_PAIRED_POSITION"}:
                    lifecycle_miss_class = "lifecycle_miss_seeded_no_matching_position"
                else:
                    lifecycle_miss_class = "lifecycle_miss_reduction_not_applied"
                return {
                    "coverage_status": "VIOLATION",
                    "mirror_status": "COVERAGE_VIOLATION",
                    "copy_action": "LIFECYCLE_NOT_APPLIED_TO_POSITION",
                    "lifecycle_status": reduction_status,
                    "lifecycle_miss_class": lifecycle_miss_class,
                    "seed_status": seed_status,
                    "filtered_entry_context": filtered_entry_context or None,
                    "position_reduction": reduction,
                    "reason": f"wallet {action} did not reduce a matching paper position",
                }
            return {
                "coverage_status": "MIRRORED",
                "mirror_status": f"MIRRORED_{action}_TO_PAPER_LIFECYCLE",
                "copy_action": "COPY_LIFECYCLE_TO_PAPER",
                "lifecycle_status": reduction_status,
                "position_reduction": reduction,
                "reason": f"wallet {action} recorded through paper lifecycle engine",
            }
        return {
            "coverage_status": "VIOLATION",
            "mirror_status": "COVERAGE_VIOLATION",
            "copy_action": "UNSUPPORTED_WALLET_ACTION",
            "reason": f"unsupported_wallet_action:{action or 'EMPTY'}",
        }

    def poll_once(self) -> dict[str, Any]:
        poll_started_ts = time.time()
        poll_clock = getattr(self, "_poll_clock", time.time)
        using_real_poll_clock = poll_clock is time.time
        poll_deadline_started = poll_clock()
        max_poll_runtime_s = max(0.0, float(self.config.max_poll_runtime_s))
        poll_deadline_ts = poll_deadline_started + max_poll_runtime_s if max_poll_runtime_s > 0 else None
        poll_runtime_limited = False
        runtime_skipped_wallets: list[str] = []
        runtime_skipped_wallet_reports: list[dict[str, Any]] = []
        runtime_skipped_events = 0

        def poll_deadline_exceeded() -> bool:
            return poll_deadline_ts is not None and poll_clock() >= poll_deadline_ts

        def runtime_skipped_wallet_report(spec: WalletSpec, *, reason: str) -> dict[str, Any]:
            return {
                "wallet": spec.normalized_address(),
                "wallet_name": spec.name,
                "events_seen": 0,
                "new_events": 0,
                "raw_rows": 0,
                "source_row_counts": {},
                "runtime_limited_events_skipped": 0,
                "poll_deadline_exceeded_before_event_processing": True,
                "poll_runtime_skip_reason": reason,
                "dedupe_exhausted": False,
                "freshness_diagnosis": {
                    "status": "ANALYZE",
                    "blockers": [reason],
                    "buy_rows": 0,
                    "fresh_buy_rows_le_10s": 0,
                    "fresh_buy_rows_le_30s": 0,
                    "dedupe_exhausted": False,
                    "source_feed_delayed": False,
                },
            }

        def record_runtime_skipped_wallets(pending_specs: Iterable[WalletSpec], *, reason: str) -> None:
            nonlocal runtime_skipped_wallet_reports, runtime_skipped_wallets
            ordered_specs = list(pending_specs)
            runtime_skipped_wallets = [spec.normalized_address() for spec in ordered_specs]
            runtime_skipped_wallet_reports = [
                runtime_skipped_wallet_report(spec, reason=reason)
                for spec in ordered_specs
            ]

        state = self.load_state()
        seen = {str(item) for item in state.get("seen_source_fingerprints") or []}
        failed_seen = {str(item) for item in state.get("failed_source_fingerprints") or []}
        failed_details = state.get("failed_source_details") if isinstance(state.get("failed_source_details"), dict) else {}
        pending_seen: set[str] = set()
        specs, tracker_scope = self._scoped_specs(state)
        rtds_events_by_wallet, rtds_source_report = _read_rtds_wallet_events(
            self.config.rtds_activity_jsonl_path,
            specs=specs,
            cursor=state.get("rtds_source_cursor"),
        )
        profit_policy, profit_policy_report = self._load_profit_policy()
        policy = self._build_policy(profit_policy)
        all_order_policy = self._build_all_order_exact_copy_policy(policy.sizing)
        profit_policy_context_id = str(
            (profit_policy_report.get("policy") if isinstance(profit_policy_report.get("policy"), dict) else {}).get(
                "policy_id"
            )
            or "NO_PROFIT_POLICY"
        )
        profit_policy_candidate_id = str(profit_policy_report.get("candidate_id") or "")
        prior_summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
        prior_profit_policy = (
            prior_summary.get("profit_policy")
            if isinstance(prior_summary.get("profit_policy"), dict)
            else {}
        )
        prior_profit_policy_row = (
            prior_profit_policy.get("policy")
            if isinstance(prior_profit_policy.get("policy"), dict)
            else {}
        )
        prior_profit_policy_context_id = str(
            state.get("profit_policy_context_id") or prior_profit_policy_row.get("policy_id") or ""
        )
        prior_profit_policy_candidate_id = str(
            state.get("profit_policy_candidate_id") or prior_profit_policy.get("candidate_id") or ""
        )
        current_copyability_context = _copyability_context(self.config)
        current_copyability_context_id = _copyability_context_id_from_context(current_copyability_context)
        prior_copyability_context_id = str(state.get("copyability_context_id") or "")
        reprocess_seen_for_new_copyability_context = bool(
            (
                prior_copyability_context_id
                and prior_copyability_context_id != current_copyability_context_id
            )
            or (
                not prior_copyability_context_id
                and current_copyability_context_id != _default_copyability_context_id()
            )
        )
        reprocess_seen_for_new_profit_context = bool(
            self.config.track_blocked_profit_policy
            and (
                (
                    prior_profit_policy_context_id
                    and prior_profit_policy_context_id != profit_policy_context_id
                )
                or (
                    profit_policy_candidate_id
                    and prior_profit_policy_candidate_id != profit_policy_candidate_id
                )
            )
        )
        reprocess_seen_for_new_evidence_context = bool(
            reprocess_seen_for_new_profit_context or reprocess_seen_for_new_copyability_context
        )
        market_ws_rows = _load_market_ws_rows(self.config.market_ws_jsonl_path, lookback_s=self.config.market_ws_lookback_s)
        now_ts = time.time()
        new_events: list[WalletEvent] = []
        event_by_id: dict[str, WalletEvent] = {}
        profit_filter_decisions: dict[str, tuple[bool, str]] = {}
        copyability_decisions: dict[str, CopyabilityDecision] = {}
        enriched_moves: list[dict[str, Any]] = []
        wallet_reports: list[dict[str, Any]] = []
        copyability_policy = self._copyability_policy()
        clob_book_cache: dict[str, dict[str, Any]] = {}
        clob_book_cache_report: dict[str, Any] = {
            "lookup_events": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "errors": 0,
            "micro_probe_lookup_events": 0,
            "micro_probe_cache_hits": 0,
            "micro_probe_cache_misses": 0,
            "micro_probe_errors": 0,
            "stale_not_admission_relevant_skipped": 0,
            "token_ids": set(),
            "fetch_durations_s": [],
            "micro_probe_fetch_durations_s": [],
        }

        def fetch_wallet(spec: WalletSpec) -> tuple[WalletSpec, list[WalletEvent], dict[str, Any]]:
            try:
                client = WalletHistoryClient(
                    spec,
                    timeout_s=float(self.config.data_api_timeout_s),
                    retries=max(1, int(self.config.data_api_retries)),
                )
            except TypeError:
                client = WalletHistoryClient(spec)
            try:
                events = client.fetch_events(
                    limit=self.config.data_api_limit,
                    pages=self.config.data_api_pages,
                    include_activity=self.config.include_activity,
                    trade_query_keys=self.config.trade_query_keys,
                    parallel_sources=self.config.parallel_data_api_sources,
                )
            except TypeError:
                events = client.fetch_events(
                    limit=self.config.data_api_limit,
                    pages=self.config.data_api_pages,
                    include_activity=self.config.include_activity,
                )
            report = dict(client.last_fetch_report)
            rtds_events = rtds_events_by_wallet.get(spec.normalized_address(), [])
            merged_events = _prefer_earliest_observed_events([*events, *rtds_events])
            source_row_counts = (
                dict(report.get("source_row_counts"))
                if isinstance(report.get("source_row_counts"), dict)
                else {}
            )
            source_row_counts["rtds_activity"] = len(rtds_events)
            report["source_row_counts"] = source_row_counts
            raw_rows_by_source = (
                report.get("raw_rows_by_source")
                if isinstance(report.get("raw_rows_by_source"), dict)
                else {}
            )
            fresh_by_source = (
                report.get("fresh_buy_rows_le_10s_by_source")
                if isinstance(report.get("fresh_buy_rows_le_10s_by_source"), dict)
                else {}
            )
            lag_by_source = (
                report.get("freshest_buy_lag_s_by_source")
                if isinstance(report.get("freshest_buy_lag_s_by_source"), dict)
                else {}
            )
            route_metrics = {
                route: {
                    "rows": int(raw_rows_by_source.get(route) or 0),
                    "fresh_buy_rows_le_policy_age": int(fresh_by_source.get(route) or 0),
                    "freshest_buy_lag_s": lag_by_source.get(route),
                }
                for route in ("activity:user", "trade:user", "trade:proxyWallet")
            }
            route_metrics["rtds_activity"] = {
                "rows": len(rtds_events),
                "fresh_buy_rows_le_policy_age": sum(
                    1
                    for event in rtds_events
                    if event.is_buy
                    and event.age_s is not None
                    and event.age_s <= float(self.config.max_copyability_event_age_s)
                ),
                "freshest_buy_lag_s": min(
                    (
                        event.age_s
                        for event in rtds_events
                        if event.is_buy and event.age_s is not None
                    ),
                    default=None,
                ),
            }
            report["source_mux"] = {
                "routes": ["activity:user", "trade:user", "trade:proxyWallet", "rtds_activity"],
                "route_metrics": route_metrics,
                "data_api_events": len(events),
                "rtds_events": len(rtds_events),
                "deduped_events": len(merged_events),
                "cross_route_duplicates": max(0, len(events) + len(rtds_events) - len(merged_events)),
                "winner_counts": dict(
                    Counter(event.source for event in merged_events)
                ),
            }
            return spec, merged_events, report

        fetched_wallets: list[tuple[WalletSpec, list[WalletEvent], dict[str, Any]]] = []
        parallel_fetches = max(1, int(self.config.parallel_wallet_fetches))
        if parallel_fetches > 1 and len(specs) > 1:
            executor = ThreadPoolExecutor(max_workers=min(parallel_fetches, len(specs)))
            future_specs = {
                executor.submit(fetch_wallet, spec): (spec_index, spec)
                for spec_index, spec in enumerate(specs)
            }
            try:
                while future_specs:
                    if poll_deadline_exceeded():
                        # State/policy setup and executor scheduling can consume
                        # the entire small synthetic budget before the first
                        # worker gets CPU. Preserve one bounded first-completion
                        # grace so an immediate wallet result is not reported as
                        # a fabricated zero-row timeout merely due to scheduler
                        # load. A genuinely slow fetch still times out within one
                        # additional configured runtime budget.
                        if (
                            using_real_poll_clock
                            and not fetched_wallets
                            and max_poll_runtime_s > 0.0
                        ):
                            try:
                                future = next(
                                    as_completed(
                                        tuple(future_specs.keys()),
                                        timeout=max_poll_runtime_s,
                                    )
                                )
                            except FuturesTimeoutError:
                                future = None
                            if future is not None:
                                _spec_index, _spec = future_specs.pop(future)
                                fetched_wallets.append(future.result())
                                if future_specs:
                                    poll_runtime_limited = True
                                    record_runtime_skipped_wallets(
                                        [
                                            pending_spec
                                            for _, pending_spec in sorted(
                                                future_specs.values(),
                                                key=lambda item: item[0],
                                            )
                                        ],
                                        reason="poll_processing_reserve_before_wallet_fetch_completed",
                                    )
                                    for pending_future in future_specs:
                                        pending_future.cancel()
                                break
                        poll_runtime_limited = True
                        record_runtime_skipped_wallets(
                            [
                                spec
                                for _, spec in sorted(future_specs.values(), key=lambda item: item[0])
                            ],
                            reason="poll_deadline_exceeded_before_wallet_fetch_completed",
                        )
                        for future in future_specs:
                            future.cancel()
                        break
                    timeout_s = None
                    if poll_deadline_ts is not None:
                        timeout_s = max(0.0, poll_deadline_ts - poll_clock())
                    try:
                        future = next(as_completed(tuple(future_specs.keys()), timeout=timeout_s))
                    except FuturesTimeoutError:
                        poll_runtime_limited = True
                        record_runtime_skipped_wallets(
                            [
                                spec
                                for _, spec in sorted(future_specs.values(), key=lambda item: item[0])
                            ],
                            reason="poll_deadline_timeout_before_wallet_fetch_completed",
                        )
                        for pending_future in future_specs:
                            pending_future.cancel()
                        break
                    spec_index, spec = future_specs.pop(future)
                    fetched_result = future.result()
                    fetched_wallets.append(fetched_result)
                    if (
                        poll_deadline_ts is not None
                        and future_specs
                        and _report_fresh_buy_rows_le_10s(fetched_result[2]) > 0
                    ):
                        poll_runtime_limited = True
                        record_runtime_skipped_wallets(
                            [
                                pending_spec
                                for _, pending_spec in sorted(
                                    future_specs.values(),
                                    key=lambda item: item[0],
                                )
                            ],
                            reason="fresh_buy_processing_priority_before_wallet_fetch_completed",
                        )
                        for pending_future in future_specs:
                            pending_future.cancel()
                        break
                    if poll_deadline_ts is not None and future_specs:
                        processing_reserve_s = _poll_processing_reserve_s(max_poll_runtime_s)
                        if (poll_deadline_ts - poll_clock()) <= processing_reserve_s:
                            poll_runtime_limited = True
                            record_runtime_skipped_wallets(
                                [
                                    pending_spec
                                    for _, pending_spec in sorted(
                                        future_specs.values(),
                                        key=lambda item: item[0],
                                    )
                                ],
                                reason="poll_processing_reserve_before_wallet_fetch_completed",
                            )
                            for pending_future in future_specs:
                                pending_future.cancel()
                            break
            finally:
                executor.shutdown(wait=not poll_runtime_limited, cancel_futures=poll_runtime_limited)
        else:
            for spec_index, spec in enumerate(specs):
                # Always start the first wallet fetch. State/policy setup can
                # consume a tiny synthetic runtime budget before any source is
                # touched; returning a fabricated zero-row report in that case
                # discards the source client's completed fetch diagnostics and
                # makes deadline behavior scheduler-dependent. Later wallets
                # still respect the hard poll deadline.
                if spec_index > 0 and poll_deadline_exceeded():
                    poll_runtime_limited = True
                    record_runtime_skipped_wallets(
                        specs[spec_index:],
                        reason="poll_deadline_exceeded_before_wallet_fetch_started",
                    )
                    break
                fetched_result = fetch_wallet(spec)
                fetched_wallets.append(fetched_result)
                if (
                    poll_deadline_ts is not None
                    and spec_index + 1 < len(specs)
                    and _report_fresh_buy_rows_le_10s(fetched_result[2]) > 0
                ):
                    poll_runtime_limited = True
                    record_runtime_skipped_wallets(
                        specs[spec_index + 1 :],
                        reason="fresh_buy_processing_priority_before_wallet_fetch_started",
                    )
                    break

        for spec_index, (spec, events, report) in enumerate(fetched_wallets):
            deadline_exceeded_before_processing = poll_deadline_exceeded()
            if deadline_exceeded_before_processing:
                poll_runtime_limited = True
            wallet_new: list[WalletEvent] = []
            seen_duplicate_events = 0
            seen_duplicate_fresh_events = 0
            pending_duplicate_events = 0
            pending_duplicate_fresh_events = 0
            failed_retry_deferred_events = 0
            failed_retry_deferred_fresh_events = 0
            event_ts_values = [float(event.event_ts) for event in events if event.event_ts is not None]
            event_ages = [float(event.age_s) for event in events if event.age_s is not None]
            for event in events:
                is_fresh_event = bool(
                    event.age_s is not None
                    and float(event.age_s) <= float(self.config.max_copyability_event_age_s)
                )
                if event.source_fingerprint in seen and not reprocess_seen_for_new_evidence_context:
                    seen_duplicate_events += 1
                    if is_fresh_event:
                        seen_duplicate_fresh_events += 1
                    continue
                if event.source_fingerprint in pending_seen:
                    pending_duplicate_events += 1
                    if is_fresh_event:
                        pending_duplicate_fresh_events += 1
                    continue
                if event.source_fingerprint in failed_seen and not _should_retry_failed_event(
                    event,
                    _failed_detail(failed_details.get(event.source_fingerprint)),
                    now_ts=now_ts,
                    retry_after_s=self.config.failed_retry_after_s,
                    max_attempts=self.config.failed_retry_max_attempts,
                ):
                    failed_retry_deferred_events += 1
                    if is_fresh_event:
                        failed_retry_deferred_fresh_events += 1
                    continue
                wallet_new.append(event)
                pending_seen.add(event.source_fingerprint)
            max_copyability_event_age_s = float(self.config.max_copyability_event_age_s)
            wallet_processing_events = sorted(
                wallet_new,
                key=lambda event: _poll_event_processing_priority(
                    event,
                    max_event_age_s=max_copyability_event_age_s,
                ),
            )
            processed_wallet_new: list[WalletEvent] = []
            runtime_limited_events_skipped = 0
            runtime_limited_event_slice: list[WalletEvent] = []
            if deadline_exceeded_before_processing:
                report["deadline_after_fetch_processing_all_fetched_events"] = True
                emergency_fresh_buys = [
                    event
                    for event in wallet_processing_events
                    if event.is_buy
                    and event.age_s is not None
                    and float(event.age_s) <= max_copyability_event_age_s
                ]
                if emergency_fresh_buys:
                    report["deadline_emergency_fresh_buy_processing"] = True
                    wallet_processing_events = emergency_fresh_buys
            for event_index, event in enumerate(wallet_processing_events):
                event_is_fresh_buy = bool(
                    event.is_buy
                    and event.age_s is not None
                    and float(event.age_s) <= max_copyability_event_age_s
                )
                if poll_deadline_exceeded() and processed_wallet_new:
                    poll_runtime_limited = True
                    runtime_limited_event_slice = wallet_processing_events[event_index:]
                    report["deadline_emergency_fresh_buy_processing"] = True
                    report["deadline_exceeded_during_event_processing_stopped_after_fresh_buy"] = True
                    break
                if poll_deadline_exceeded() and not report.get("deadline_emergency_fresh_buy_processing"):
                    poll_runtime_limited = True
                    report["deadline_exceeded_during_event_processing_continued"] = True
                copy_size = policy.sizing.size_usd(event)
                evidence = self._enrich_event(
                    event,
                    copy_size,
                    market_ws_rows,
                    wallet_fetch_report=report,
                    clob_book_cache=clob_book_cache,
                    clob_book_cache_report=clob_book_cache_report,
                )
                copyability = (
                    score_copyability(event, evidence, copyability_policy)
                    if self.config.enable_copyability_gate
                    else CopyabilityDecision(True, "disabled", copyability_policy.policy_id, {})
                )
                copyability_decisions[event.event_id] = copyability
                event_by_id[event.event_id] = event
                processed_wallet_new.append(event)
                enriched_moves.append(
                    {
                        "event": "wallet_copy_live_tracked_move",
                        "generated_at": utc_now_iso(),
                        "paper_only": True,
                        "live_orders_allowed": False,
                        "action": event.action.upper(),
                        "source_event_id": event.event_id,
                        "source_fingerprint": event.source_fingerprint,
                        "tx_hash": event.transaction_hash,
                        "market_slug": event.market_slug,
                        "outcome": event.outcome,
                        "event_ts": event.event_ts,
                        "source_price": event.price,
                        "source_usdc_size": event.usdc_size,
                        "data_api_trade_query_keys": list(self.config.trade_query_keys),
                        "data_api_trade_query_scope": ",".join(self.config.trade_query_keys),
                        "copyability_context_id": current_copyability_context_id,
                        "copyability_context": current_copyability_context,
                        "profit_policy_context_id": profit_policy_context_id,
                        "profit_policy_candidate_id": profit_policy_candidate_id or None,
                        "profit_policy_context_status": profit_policy_report.get("status"),
                        "wallet_event": event.asdict(),
                        "tracking_evidence": evidence,
                        "copyability": copyability.asdict(),
                    }
                )
            if report.get("deadline_emergency_fresh_buy_processing") and not runtime_limited_event_slice:
                processed_event_ids = {event.event_id for event in processed_wallet_new}
                runtime_limited_event_slice = [
                    event for event in wallet_new if event.event_id not in processed_event_ids
                ]
                runtime_limited_events_skipped = len(runtime_limited_event_slice)
                runtime_skipped_events += runtime_limited_events_skipped
            if (
                report.get("deadline_emergency_fresh_buy_processing")
                and runtime_limited_events_skipped == 0
                and len(events) > len(processed_wallet_new)
            ):
                runtime_limited_events_skipped = len(events) - len(processed_wallet_new)
                runtime_skipped_events += runtime_limited_events_skipped
            wallet_new = processed_wallet_new
            new_events.extend(wallet_new)
            runtime_limited_buy_events_skipped = sum(1 for event in runtime_limited_event_slice if event.is_buy)
            runtime_limited_fresh_buy_events_skipped = _fresh_buy_event_count(
                runtime_limited_event_slice,
                max_event_age_s=max_copyability_event_age_s,
            )
            runtime_skip_reason = report.get("poll_runtime_skip_reason")
            if deadline_exceeded_before_processing and not runtime_skip_reason:
                runtime_skip_reason = "poll_deadline_exceeded_before_event_processing_after_wallet_fetch_completed"
            elif runtime_limited_events_skipped and not runtime_skip_reason:
                runtime_skip_reason = "poll_deadline_exceeded_during_event_processing"
            report.update(
                {
                    "wallet": spec.normalized_address(),
                    "wallet_name": spec.name,
                    "events_seen": len(events),
                    "new_events": len(wallet_new),
                    "event_processing_priority": "fresh_buy_first_desc_event_ts",
                    "runtime_limited_events_skipped": runtime_limited_events_skipped,
                    "runtime_limited_buy_events_skipped": runtime_limited_buy_events_skipped,
                    "runtime_limited_fresh_buy_events_skipped": runtime_limited_fresh_buy_events_skipped,
                    "poll_deadline_exceeded_before_event_processing": deadline_exceeded_before_processing,
                    "poll_runtime_skip_reason": runtime_skip_reason,
                    "seen_duplicate_events": seen_duplicate_events,
                    "seen_duplicate_fresh_events": seen_duplicate_fresh_events,
                    "pending_duplicate_events": pending_duplicate_events,
                    "pending_duplicate_fresh_events": pending_duplicate_fresh_events,
                    "failed_retry_deferred_events": failed_retry_deferred_events,
                    "failed_retry_deferred_fresh_events": failed_retry_deferred_fresh_events,
                    "latest_seen_event_ts": max(event_ts_values) if event_ts_values else None,
                    "latest_seen_event_iso": _iso_from_ts(max(event_ts_values)) if event_ts_values else None,
                    "oldest_seen_event_ts": min(event_ts_values) if event_ts_values else None,
                    "oldest_seen_event_iso": _iso_from_ts(min(event_ts_values)) if event_ts_values else None,
                    "freshest_seen_event_lag_s": round(max(0.0, now_ts - max(event_ts_values)), 6)
                    if event_ts_values
                    else None,
                    "oldest_seen_event_lag_s": round(max(0.0, now_ts - min(event_ts_values)), 6)
                    if event_ts_values
                    else None,
                    "seen_event_age_avg_s": _mean(event_ages),
                    "seen_event_action_counts": _wallet_event_action_counts(events),
                    "new_event_action_counts": _wallet_event_action_counts(wallet_new),
                    "dedupe_exhausted": bool(events and not wallet_new),
                    "freshness_diagnosis": _wallet_freshness_diagnosis(
                        events,
                        now_ts=now_ts,
                        max_event_age_s=float(self.config.max_copyability_event_age_s),
                        max_fetch_duration_s=float(self.config.max_wallet_fetch_duration_s),
                        fetch_report=report,
                        dedupe_exhausted=bool(events and not wallet_new),
                    ),
                }
            )
            wallet_reports.append(report)

        if runtime_skipped_wallet_reports:
            reported_wallets = {
                str(report.get("wallet") or "").lower()
                for report in wallet_reports
                if isinstance(report, dict) and report.get("wallet")
            }
            for skipped_report in runtime_skipped_wallet_reports:
                wallet = str(skipped_report.get("wallet") or "").lower()
                if wallet not in reported_wallets:
                    wallet_reports.append(skipped_report)
                    reported_wallets.add(wallet)

        for event in new_events:
            if profit_policy is None:
                profit_filter_decisions[event.event_id] = (True, "accepted")
            else:
                profit_filter_decisions[event.event_id] = policy_accepts_event(profit_policy, event)
        raw_intents = [
            intent
            for intent in (
                event_to_intent(event, policy=policy, mode="paper", now_ts=now_ts)
                for event in new_events
                if profit_filter_decisions.get(event.event_id, (True, "accepted"))[0]
                and copyability_decisions.get(
                    event.event_id,
                    CopyabilityDecision(True, "missing_decision", copyability_policy.policy_id, {}),
                ).accepted
            )
            if intent is not None
        ]
        filtered_entry_context_by_key = _filtered_entry_context(
            new_events,
            policy=policy,
            profit_policy=profit_policy,
            profit_policy_candidate_id=profit_policy_candidate_id or None,
            profit_filter_decisions=profit_filter_decisions,
            copyability_decisions=copyability_decisions,
            now_ts=now_ts,
        )
        evidence_by_event_id = {
            str(move["wallet_event"]["event_id"]): move["tracking_evidence"]
            for move in enriched_moves
            if isinstance(move.get("wallet_event"), dict)
        }
        intents = _dedupe_intents(raw_intents)
        enriched_intents = [
            _attach_tracking_evidence(intent, evidence_by_event_id.get(intent.source_event_id, {}))
            for intent in intents
        ]
        enriched_intent_by_event_id = {intent.source_event_id: intent for intent in enriched_intents}
        all_order_raw_intents = [
            intent
            for intent in (
                event_to_intent(event, policy=all_order_policy, mode="paper", now_ts=now_ts)
                for event in new_events
            )
            if intent is not None
        ]
        all_order_intents = _dedupe_intents(all_order_raw_intents)
        all_order_enriched_intents = [
            _attach_tracking_evidence(intent, evidence_by_event_id.get(intent.source_event_id, {}))
            for intent in all_order_intents
        ]
        all_order_intent_by_event_id = {
            intent.source_event_id: intent
            for intent in all_order_enriched_intents
        }
        all_order_profit_filter_decisions = {
            event.event_id: (True, "all_order_exact_copy_profit_policy_bypassed")
            for event in new_events
        }
        all_order_filtered_entry_context_by_key = _filtered_entry_context(
            new_events,
            policy=all_order_policy,
            profit_policy=None,
            profit_policy_candidate_id=None,
            profit_filter_decisions=all_order_profit_filter_decisions,
            copyability_decisions={},
            now_ts=now_ts,
        )
        seed_report = self._seed_history_before_poll(
            new_events,
            specs=specs,
            policy=policy,
            profit_policy=profit_policy,
            now_ts=now_ts,
        )
        all_order_seed_report = self._seed_history_into_paper(
            new_events,
            specs=specs,
            policy=all_order_policy,
            profit_policy=None,
            paper_engine=self.all_order_exact_copy_paper,
            now_ts=now_ts,
        )
        paper_state = self.paper.apply_wallet_events_in_order(
            new_events,
            intent_by_event_id=enriched_intent_by_event_id,
        )
        all_order_paper_state = self.all_order_exact_copy_paper.apply_wallet_events_in_order(
            new_events,
            intent_by_event_id=all_order_intent_by_event_id,
        )
        all_order_micro_batch_probe = self._all_order_micro_batch_probe(
            all_order_enriched_intents,
            event_by_id,
            clob_book_cache=clob_book_cache,
            clob_book_cache_report=clob_book_cache_report,
        )
        paper_order_by_intent_id = {
            str(order.get("intent_id")): order
            for order in paper_state.get("orders") or []
            if isinstance(order, dict) and order.get("intent_id")
        }
        all_order_paper_order_by_intent_id = {
            str(order.get("intent_id")): order
            for order in all_order_paper_state.get("orders") or []
            if isinstance(order, dict) and order.get("intent_id")
        }
        lifecycle_by_event_id = {
            str(row.get("source_event_id")): row
            for row in paper_state.get("lifecycle_events") or []
            if isinstance(row, dict) and row.get("source_event_id")
        }
        all_order_lifecycle_by_event_id = {
            str(row.get("source_event_id")): row
            for row in all_order_paper_state.get("lifecycle_events") or []
            if isinstance(row, dict) and row.get("source_event_id")
        }
        mirror_result_by_event_id: dict[str, dict[str, Any]] = {}
        all_order_mirror_result_by_event_id: dict[str, dict[str, Any]] = {}

        for move in enriched_moves:
            wallet_event = move.get("wallet_event")
            if not isinstance(wallet_event, dict):
                continue
            event = event_by_id.get(str(wallet_event.get("event_id")))
            if event is None:
                continue
            mirror_result = self._mirror_result(
                event,
                policy=policy,
                profit_policy=profit_policy,
                profit_filter_decisions=profit_filter_decisions,
                copyability_decisions=copyability_decisions,
                intent_by_event_id=enriched_intent_by_event_id,
                paper_order_by_intent_id=paper_order_by_intent_id,
                lifecycle_by_event_id=lifecycle_by_event_id,
                filtered_entry_context_by_key=filtered_entry_context_by_key,
                seed_report=seed_report,
                now_ts=now_ts,
            )
            mirror_result = {
                **mirror_result,
                "mirror_classifier_version": MIRROR_CLASSIFIER_VERSION,
                "copyability_context_id": current_copyability_context_id,
                "profit_policy_context_id": profit_policy_context_id,
                "profit_policy_candidate_id": profit_policy_candidate_id or None,
                "profit_policy_context_status": profit_policy_report.get("status"),
            }
            move["copy_action"] = mirror_result["copy_action"]
            move["mirror_result"] = mirror_result
            mirror_result_by_event_id[event.event_id] = mirror_result
            all_order_mirror_result = self._mirror_result(
                event,
                policy=all_order_policy,
                profit_policy=None,
                profit_filter_decisions=all_order_profit_filter_decisions,
                copyability_decisions={},
                intent_by_event_id=all_order_intent_by_event_id,
                paper_order_by_intent_id=all_order_paper_order_by_intent_id,
                lifecycle_by_event_id=all_order_lifecycle_by_event_id,
                filtered_entry_context_by_key=all_order_filtered_entry_context_by_key,
                seed_report=all_order_seed_report,
                now_ts=now_ts,
            )
            all_order_mirror_result = {
                **all_order_mirror_result,
                "mirror_classifier_version": MIRROR_CLASSIFIER_VERSION,
                "profit_policy_context_id": "ALL_ORDER_EXACT_COPY",
                "profit_policy_candidate_id": None,
                "profit_policy_context_status": "PROFIT_POLICY_BYPASSED_FOR_ALL_ORDER_PROOF",
                "all_order_exact_copy": True,
            }
            move["all_order_exact_copy"] = all_order_mirror_result
            all_order_mirror_result_by_event_id[event.event_id] = all_order_mirror_result
            if mirror_result.get("coverage_status") == "VIOLATION":
                failed_seen.add(event.source_fingerprint)
                _record_failed_event_detail(
                    failed_details,
                    event,
                    mirror_result=mirror_result,
                    now_ts=now_ts,
                    retry_after_s=self.config.failed_retry_after_s,
                    max_attempts=self.config.failed_retry_max_attempts,
                )
            else:
                seen.add(event.source_fingerprint)
                failed_seen.discard(event.source_fingerprint)
                failed_details.pop(event.source_fingerprint, None)

        copy_efficiency = build_copy_efficiency_report(
            new_events,
            intent_by_event_id=enriched_intent_by_event_id,
            paper_order_by_intent_id=paper_order_by_intent_id,
            mirror_result_by_event_id=mirror_result_by_event_id,
            profit_filter_decisions=profit_filter_decisions,
            copyability_decisions={event_id: decision.asdict() for event_id, decision in copyability_decisions.items()},
            max_api_latency_s=self.config.max_copy_efficiency_latency_s,
            max_avg_worse_slippage_bps=self.config.max_copy_efficiency_slippage_bps,
            require_clob_book_evidence=self.config.require_clob_book_evidence_for_efficiency,
        )
        intent_time_scores, intent_time_proof_summary = _intent_time_copyability_proof_scores(
            self.config.intent_time_copyability_proof_state_path
        )
        copy_efficiency = _merge_intent_time_copyability_proof(
            copy_efficiency,
            scores=intent_time_scores,
            proof_summary=intent_time_proof_summary,
            max_api_latency_s=self.config.max_copy_efficiency_latency_s,
            max_avg_worse_slippage_bps=self.config.max_copy_efficiency_slippage_bps,
            require_clob_book_evidence=self.config.require_clob_book_evidence_for_efficiency,
        )
        copy_efficiency_by_event_id = {
            str(score.get("source_event_id")): score
            for score in copy_efficiency.get("event_scores") or []
            if isinstance(score, dict) and score.get("source_event_id")
        }
        for move in enriched_moves:
            wallet_event = move.get("wallet_event")
            if isinstance(wallet_event, dict):
                score = copy_efficiency_by_event_id.get(str(wallet_event.get("event_id")))
                if score is not None:
                    move["copy_efficiency"] = score

        current_poll_copy_efficiency = copy_efficiency
        prior_window = state.get("admission_evidence_window")
        if not isinstance(prior_window, list):
            prior_window = state.get("last_moves") if isinstance(state.get("last_moves"), list) else []
        log_window = _recent_event_log_moves(self.config.event_log_path, limit=200)
        admission_window_moves = _admission_evidence_window(
            prior_window,
            log_window,
            enriched_moves,
            limit=200,
            mirror_classifier_version=MIRROR_CLASSIFIER_VERSION,
            profit_policy_context_id=profit_policy_context_id,
            profit_policy_candidate_id=profit_policy_candidate_id or None,
            trade_query_keys=self.config.trade_query_keys,
            copyability_context_id=current_copyability_context_id,
        )
        summary_moves = admission_window_moves if admission_window_moves else enriched_moves
        copy_efficiency = _copy_efficiency_from_window_moves(
            summary_moves,
            max_api_latency_s=self.config.max_copy_efficiency_latency_s,
            max_avg_worse_slippage_bps=self.config.max_copy_efficiency_slippage_bps,
            require_clob_book_evidence=self.config.require_clob_book_evidence_for_efficiency,
            extra_scores=intent_time_scores,
        )
        copy_efficiency["intent_time_copyability_proof"] = intent_time_proof_summary
        copy_efficiency["evidence_window"] = {
            "source": "rolling_admission_evidence_window",
            "mirror_classifier_version": MIRROR_CLASSIFIER_VERSION,
            "profit_policy_context_id": profit_policy_context_id,
            "profit_policy_candidate_id": profit_policy_candidate_id or None,
            "window_moves": len(summary_moves),
            "current_poll_moves": len(enriched_moves),
            "prior_window_moves": len(prior_window),
            "event_log_window_moves": len(log_window),
            "prior_window_version_compatible_moves": _mirror_version_compatible_count(
                prior_window,
                mirror_classifier_version=MIRROR_CLASSIFIER_VERSION,
            ),
            "event_log_window_version_compatible_moves": _mirror_version_compatible_count(
                log_window,
                mirror_classifier_version=MIRROR_CLASSIFIER_VERSION,
            ),
            "prior_window_profit_policy_context_compatible_moves": _profit_policy_context_compatible_count(
                prior_window,
                profit_policy_context_id=profit_policy_context_id,
            ),
            "event_log_window_profit_policy_context_compatible_moves": _profit_policy_context_compatible_count(
                log_window,
                profit_policy_context_id=profit_policy_context_id,
            ),
            "prior_window_profit_policy_candidate_compatible_moves": (
                _profit_policy_candidate_compatible_count(
                    prior_window,
                    profit_policy_candidate_id=profit_policy_candidate_id,
                )
                if profit_policy_candidate_id
                else None
            ),
            "event_log_window_profit_policy_candidate_compatible_moves": (
                _profit_policy_candidate_compatible_count(
                    log_window,
                    profit_policy_candidate_id=profit_policy_candidate_id,
                )
                if profit_policy_candidate_id
                else None
            ),
            "trade_query_keys": list(self.config.trade_query_keys),
            "copyability_context_id": current_copyability_context_id,
            "prior_window_copyability_context_compatible_moves": _copyability_context_compatible_count(
                prior_window,
                copyability_context_id=current_copyability_context_id,
            ),
            "event_log_window_copyability_context_compatible_moves": _copyability_context_compatible_count(
                log_window,
                copyability_context_id=current_copyability_context_id,
            ),
            "prior_window_trade_query_scope_compatible_moves": _trade_query_scope_compatible_count(
                prior_window,
                trade_query_keys=self.config.trade_query_keys,
            ),
            "event_log_window_trade_query_scope_compatible_moves": _trade_query_scope_compatible_count(
                log_window,
                trade_query_keys=self.config.trade_query_keys,
            ),
            "dedupe_key": "wallet_event.event_id/source_fingerprint",
        }
        copy_efficiency["current_poll"] = current_poll_copy_efficiency

        coverage_violations = [
            {
                **(
                    _event_identity(event_by_id[str(move["wallet_event"]["event_id"])])
                    if str(move["wallet_event"].get("event_id")) in event_by_id
                    else {
                        "source_wallet": str(move["wallet_event"].get("source_wallet") or "").lower(),
                        "wallet_name": move["wallet_event"].get("wallet_name"),
                        "source_event_id": move["wallet_event"].get("event_id"),
                        "source_fingerprint": move["wallet_event"].get("source_fingerprint"),
                        "row_type": move["wallet_event"].get("row_type"),
                        "action": str(move["wallet_event"].get("action") or "").upper(),
                        "condition_id": move["wallet_event"].get("condition_id"),
                        "market_slug": move["wallet_event"].get("market_slug"),
                        "outcome": move["wallet_event"].get("outcome"),
                        "price": move["wallet_event"].get("price"),
                        "size": move["wallet_event"].get("size"),
                        "usdc_size": move["wallet_event"].get("usdc_size"),
                        "token_id": move["wallet_event"].get("token_id"),
                        "transaction_hash": move["wallet_event"].get("transaction_hash"),
                        "event_ts": move["wallet_event"].get("event_ts"),
                    }
                ),
                "mirror_result": move.get("mirror_result"),
            }
            for move in summary_moves
            if isinstance(move.get("wallet_event"), dict)
            and isinstance(move.get("mirror_result"), dict)
            and move["mirror_result"].get("coverage_status") == "VIOLATION"
        ]
        mirrored_events = sum(
            1
            for move in summary_moves
            if isinstance(move.get("mirror_result"), dict)
            and move["mirror_result"].get("coverage_status") == "MIRRORED"
        )
        filtered_events = sum(
            1
            for move in summary_moves
            if isinstance(move.get("mirror_result"), dict)
            and move["mirror_result"].get("coverage_status") == "FILTERED"
        )
        filtered_by_policy_counts = Counter(
            str((move.get("mirror_result") if isinstance(move.get("mirror_result"), dict) else {}).get("filter_policy") or "unknown")
            for move in summary_moves
            if isinstance(move.get("mirror_result"), dict)
            and move["mirror_result"].get("coverage_status") == "FILTERED"
        )
        mirror_required_events = len(summary_moves) - filtered_events
        coverage_pct = None if mirror_required_events == 0 else round(100.0 * mirrored_events / mirror_required_events, 6)
        mirror_coverage_status = (
            "FAIL"
            if coverage_violations
            else ("NO_REQUIRED_EVIDENCE" if mirror_required_events == 0 else "PASS")
        )
        mirror_coverage_by_action: dict[str, dict[str, Any]] = {}
        coverage_violation_by_action_reason: dict[str, dict[str, int]] = {}
        lifecycle_miss_class_counts: Counter[str] = Counter()
        buy_source_events = 0
        buy_required_events = 0
        buy_filtered_events = 0
        filled_buy_copy_events = 0
        rejected_buy_copy_events = 0
        uncovered_sell_notional_usd = 0.0
        for move in enriched_moves:
            wallet_event = move.get("wallet_event")
            if not isinstance(wallet_event, dict):
                continue
            action = str(wallet_event.get("action") or "").upper() or "UNKNOWN"
            mirror_result = move.get("mirror_result") if isinstance(move.get("mirror_result"), dict) else {}
            coverage_status = str(mirror_result.get("coverage_status") or "MISSING")
            mirror_status = str(mirror_result.get("mirror_status") or "MISSING")
            action_row = mirror_coverage_by_action.setdefault(
                action,
                {
                    "source_events": 0,
                    "filtered_events": 0,
                    "mirror_required_events": 0,
                    "mirrored_events": 0,
                    "coverage_violations": 0,
                },
            )
            action_row["source_events"] += 1
            if coverage_status == "FILTERED":
                action_row["filtered_events"] += 1
            else:
                action_row["mirror_required_events"] += 1
            if coverage_status == "MIRRORED":
                action_row["mirrored_events"] += 1
            if coverage_status == "VIOLATION":
                action_row["coverage_violations"] += 1
                reason = str(mirror_result.get("reason") or mirror_result.get("copy_action") or "unknown")
                coverage_violation_by_action_reason.setdefault(action, {})
                coverage_violation_by_action_reason[action][reason] = (
                    coverage_violation_by_action_reason[action].get(reason, 0) + 1
                )
                lifecycle_miss_class = str(mirror_result.get("lifecycle_miss_class") or "")
                if lifecycle_miss_class:
                    lifecycle_miss_class_counts[lifecycle_miss_class] += 1
                if action == "SELL":
                    uncovered_sell_notional_usd += float(wallet_event.get("usdc_size") or 0.0)
            if action == "BUY":
                buy_source_events += 1
                if coverage_status == "FILTERED":
                    buy_filtered_events += 1
                else:
                    buy_required_events += 1
                if mirror_status == "MIRRORED_BUY_TO_PAPER":
                    filled_buy_copy_events += 1
                elif mirror_status == "MIRRORED_BUY_TO_PAPER_REJECTED_BY_FILL_MODEL":
                    rejected_buy_copy_events += 1
        for action_row in mirror_coverage_by_action.values():
            required = int(action_row.get("mirror_required_events") or 0)
            action_row["mirror_coverage_pct"] = (
                None if required == 0 else round(100.0 * int(action_row.get("mirrored_events") or 0) / required, 6)
            )
        buy_fill_coverage = {
            "source_buy_events": buy_source_events,
            "filtered_buy_events": buy_filtered_events,
            "required_buy_events": buy_required_events,
            "filled_buy_copy_events": filled_buy_copy_events,
            "rejected_buy_copy_events": rejected_buy_copy_events,
            "filled_pct_of_source_buy_events": None
            if buy_source_events == 0
            else round(100.0 * filled_buy_copy_events / buy_source_events, 6),
            "filled_pct_of_required_buy_events": None
            if buy_required_events == 0
            else round(100.0 * filled_buy_copy_events / buy_required_events, 6),
        }
        all_order_source_events = 0
        all_order_buy_source_events = 0
        all_order_lifecycle_source_events = 0
        all_order_buy_intents = 0
        all_order_filled_buy_copy_events = 0
        all_order_rejected_buy_copy_events = 0
        all_order_mirrored_lifecycle_events = 0
        all_order_missed_lifecycle_events = 0
        all_order_copy_policy_rejected_buy_events = 0
        all_order_coverage_violations = 0
        all_order_mirror_status_counts: Counter[str] = Counter()
        all_order_lifecycle_miss_class_counts: Counter[str] = Counter()
        all_order_violation_reasons: Counter[str] = Counter()
        all_order_fill_source_counts: Counter[str] = Counter()
        all_order_filled_fill_source_counts: Counter[str] = Counter()
        all_order_rejected_fill_source_counts: Counter[str] = Counter()
        all_order_tactic_profile_status_counts: dict[str, Counter[str]] = defaultdict(Counter)
        all_order_tactic_profile_pass_events: Counter[str] = Counter()
        all_order_required_event_ids: set[str] = set()
        all_order_clob_book_hashes: set[str] = set()
        for move in enriched_moves:
            wallet_event = move.get("wallet_event") if isinstance(move.get("wallet_event"), dict) else {}
            if not wallet_event:
                continue
            source_event_id = str(wallet_event.get("event_id") or "")
            event = event_by_id.get(source_event_id)
            all_order_result = (
                move.get("all_order_exact_copy")
                if isinstance(move.get("all_order_exact_copy"), dict)
                else all_order_mirror_result_by_event_id.get(source_event_id, {})
            )
            if not isinstance(all_order_result, dict):
                all_order_result = {}
            all_order_source_events += 1
            action = str(wallet_event.get("action") or "").upper()
            mirror_status = str(all_order_result.get("mirror_status") or "MISSING")
            coverage_status = str(all_order_result.get("coverage_status") or "MISSING")
            all_order_mirror_status_counts[mirror_status] += 1
            if coverage_status != "FILTERED" and source_event_id:
                all_order_required_event_ids.add(source_event_id)
            if coverage_status == "VIOLATION":
                all_order_coverage_violations += 1
                reason = str(all_order_result.get("reason") or all_order_result.get("copy_action") or "unknown")
                all_order_violation_reasons[reason] += 1
                miss_class = str(all_order_result.get("lifecycle_miss_class") or "")
                if miss_class:
                    all_order_lifecycle_miss_class_counts[miss_class] += 1
            if action == "BUY":
                all_order_buy_source_events += 1
                tracking_evidence = (
                    move.get("tracking_evidence") if isinstance(move.get("tracking_evidence"), dict) else {}
                )
                tactic_profiles = (
                    tracking_evidence.get("paper_tactic_fillability")
                    if isinstance(tracking_evidence.get("paper_tactic_fillability"), dict)
                    else {}
                )
                for profile_id, profile in tactic_profiles.items():
                    if not isinstance(profile, dict):
                        continue
                    profile_status = str(profile.get("status") or profile.get("instant_fill_status") or "UNKNOWN")
                    all_order_tactic_profile_status_counts[str(profile_id)][profile_status] += 1
                    if profile.get("instant_fill_status") == "PASS" or profile_status == "PASS":
                        all_order_tactic_profile_pass_events[str(profile_id)] += 1
                intent = all_order_intent_by_event_id.get(source_event_id)
                if intent is None:
                    if event is not None and not all_order_policy.accepts(event, now_ts=now_ts)[0]:
                        all_order_copy_policy_rejected_buy_events += 1
                    continue
                all_order_buy_intents += 1
                paper_order = all_order_paper_order_by_intent_id.get(intent.intent_id) or {}
                final_status = str(paper_order.get("final_status") or "")
                fill_estimate = (
                    (paper_order.get("source_intent") or {}).get("fill_estimate")
                    if isinstance(paper_order.get("source_intent"), dict)
                    else {}
                )
                fill_source = str((fill_estimate or {}).get("source") or "unknown")
                if final_status == "FILLED":
                    all_order_filled_buy_copy_events += 1
                    all_order_fill_source_counts[fill_source] += 1
                    all_order_filled_fill_source_counts[fill_source] += 1
                    book_hash = str(((fill_estimate or {}).get("book") or {}).get("book_hash") or "")
                    if book_hash:
                        all_order_clob_book_hashes.add(book_hash)
                elif final_status == "REJECTED":
                    all_order_rejected_buy_copy_events += 1
                    all_order_fill_source_counts[fill_source] += 1
                    all_order_rejected_fill_source_counts[fill_source] += 1
            elif action in {"SELL", "MERGE", "REDEEM"}:
                all_order_lifecycle_source_events += 1
                if coverage_status == "MIRRORED":
                    all_order_mirrored_lifecycle_events += 1
                elif coverage_status == "VIOLATION":
                    all_order_missed_lifecycle_events += 1

        all_order_fallback_filled_buy_copy_events = int(
            all_order_filled_fill_source_counts.get("source_price_plus_slippage_fallback") or 0
        )
        all_order_clob_filled_buy_copy_events = int(
            all_order_filled_fill_source_counts.get("clob_book_evidence") or 0
        )
        all_order_paper_summary = (
            all_order_paper_state.get("summary")
            if isinstance(all_order_paper_state.get("summary"), dict)
            else {}
        )
        current_all_order_event_ids = {event.event_id for event in new_events}
        all_order_paper_orders = [
            row
            for row in all_order_paper_state.get("orders") or []
            if isinstance(row, dict)
        ]
        all_order_paper_lifecycle_events = [
            row
            for row in all_order_paper_state.get("lifecycle_events") or []
            if isinstance(row, dict)
        ]

        def _paper_order_source_event_id(row: dict[str, Any]) -> str:
            source_intent = row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}
            return str(source_intent.get("source_event_id") or "")

        all_order_current_paper_orders = [
            row
            for row in all_order_paper_orders
            if _paper_order_source_event_id(row) in current_all_order_event_ids
        ]
        all_order_current_lifecycle_events = [
            row
            for row in all_order_paper_lifecycle_events
            if str(row.get("source_event_id") or "") in current_all_order_event_ids
        ]
        all_order_execution_corrections = build_copy_execution_corrections(
            all_order_current_paper_orders,
            all_order_current_lifecycle_events,
        )
        all_order_cumulative_execution_corrections = build_copy_execution_corrections(
            all_order_paper_orders,
            all_order_paper_lifecycle_events,
        )
        all_order_current_execution_tactic_plan = {
            **build_copy_execution_tactic_plan(
                all_order_current_paper_orders,
                tactic_profile_status_counts={
                    profile_id: dict(counts)
                    for profile_id, counts in all_order_tactic_profile_status_counts.items()
                },
                tactic_profile_pass_events=dict(all_order_tactic_profile_pass_events),
                micro_batch_probe=all_order_micro_batch_probe,
                correction_report=all_order_execution_corrections,
            ),
            "order_scope": "current_poll",
            "current_poll_order_count": len(all_order_current_paper_orders),
            "cumulative_order_count": len(all_order_paper_orders),
        }
        all_order_cumulative_execution_tactic_plan = {
            **build_copy_execution_tactic_plan(
                all_order_paper_orders,
                correction_report=all_order_cumulative_execution_corrections,
            ),
            "order_scope": "cumulative_without_current_tactic_profiles",
            "current_poll_order_count": len(all_order_current_paper_orders),
            "cumulative_order_count": len(all_order_paper_orders),
        }
        all_order_execution_tactic_plan = (
            all_order_current_execution_tactic_plan
            if all_order_current_paper_orders
            else {
                **all_order_cumulative_execution_tactic_plan,
                "blockers": sorted(
                    set(
                        list(all_order_cumulative_execution_tactic_plan.get("blockers") or [])
                        + ["no_current_poll_orders_for_current_tactic_replay"]
                    )
                ),
            }
        )
        all_order_ledger_rejected_orders = sum(
            1
            for row in all_order_current_paper_orders
            if str(row.get("final_status") or row.get("status") or "") == "REJECTED"
        )
        all_order_cumulative_ledger_rejected_orders = int(all_order_paper_summary.get("rejected_orders") or 0)
        all_order_aggressive_tactic_replay = self._all_order_aggressive_tactic_replay(
            new_events,
            all_order_enriched_intents,
            all_order_execution_tactic_plan,
            strict_current_orders=all_order_current_paper_orders,
        )
        all_order_status = (
            "NO_EVENTS"
            if all_order_source_events == 0 and all_order_ledger_rejected_orders == 0
            else (
                "PASS"
                if all_order_coverage_violations == 0
                and all_order_rejected_buy_copy_events == 0
                and all_order_ledger_rejected_orders == 0
                and all_order_buy_intents == all_order_buy_source_events
                else "FAIL"
            )
        )
        all_order_status_blockers: list[str] = []
        if all_order_source_events == 0:
            all_order_status_blockers.append("no_current_all_order_source_events")
        if all_order_coverage_violations > 0:
            all_order_status_blockers.append("all_order_coverage_violations_present")
        if all_order_rejected_buy_copy_events > 0:
            all_order_status_blockers.append("all_order_rejected_buy_copy_events_present")
        if all_order_ledger_rejected_orders > 0:
            all_order_status_blockers.append("all_order_current_ledger_has_rejected_orders")
        if all_order_buy_intents != all_order_buy_source_events:
            all_order_status_blockers.append("all_order_buy_intent_count_mismatch")
        all_order_paper_proof_status = (
            PASS if all_order_status == PASS else active_status_from_blockers(all_order_status_blockers, default=ANALYZE)
        )
        all_order_copyability_accepted_buy_events = 0
        all_order_copyability_rejected_buy_events = 0
        all_order_copyability_reason_counts: Counter[str] = Counter()
        all_order_copyability_rejected_blocker_counts: Counter[str] = Counter()
        all_order_copyability_rejected_wallet_route_class_counts: Counter[str] = Counter()
        all_order_copyability_rejected_clob_route_class_counts: Counter[str] = Counter()
        all_order_copyability_rejected_book_hashes: set[str] = set()
        all_order_copyability_rejected_samples: list[dict[str, Any]] = []
        tracking_evidence_by_event_id = {
            str(move.get("wallet_event", {}).get("event_id") or ""): (
                move.get("tracking_evidence") if isinstance(move.get("tracking_evidence"), dict) else {}
            )
            for move in enriched_moves
            if isinstance(move.get("wallet_event"), dict)
        }
        for event in new_events:
            if not event.is_buy:
                continue
            decision = copyability_decisions.get(event.event_id)
            if decision is None:
                all_order_copyability_rejected_buy_events += 1
                all_order_copyability_reason_counts["missing_copyability_decision"] += 1
                all_order_copyability_rejected_blocker_counts["missing_copyability_decision"] += 1
            elif decision.accepted:
                all_order_copyability_accepted_buy_events += 1
            else:
                all_order_copyability_rejected_buy_events += 1
                all_order_copyability_reason_counts[str(decision.reason or "copyability_rejected")] += 1
                details = decision.details if isinstance(decision.details, dict) else {}
                blockers = details.get("blockers") if isinstance(details.get("blockers"), list) else []
                for blocker in blockers or [decision.reason or "copyability_rejected"]:
                    all_order_copyability_rejected_blocker_counts[str(blocker)] += 1
            if decision is None or not decision.accepted:
                tracking_evidence = tracking_evidence_by_event_id.get(event.event_id, {})
                sample = _copyability_reject_diagnostic_sample(
                    event,
                    decision,
                    tracking_evidence=tracking_evidence,
                )
                wallet_route_class = sample.get("wallet_route_class")
                if wallet_route_class not in (None, ""):
                    all_order_copyability_rejected_wallet_route_class_counts[str(wallet_route_class)] += 1
                clob_route_class = sample.get("clob_route_class")
                if clob_route_class not in (None, ""):
                    all_order_copyability_rejected_clob_route_class_counts[str(clob_route_class)] += 1
                book_hash = sample.get("clob_book_hash")
                if book_hash not in (None, ""):
                    all_order_copyability_rejected_book_hashes.add(str(book_hash))
                if len(all_order_copyability_rejected_samples) < 10:
                    all_order_copyability_rejected_samples.append(sample)
        all_order_live_truth_blockers: list[str] = []
        if all_order_source_events == 0:
            all_order_live_truth_blockers.append("no_current_all_order_source_events")
        if all_order_buy_source_events == 0:
            all_order_live_truth_blockers.append("no_current_all_order_buy_events")
        if all_order_coverage_violations > 0:
            all_order_live_truth_blockers.append("all_order_coverage_violations_present")
        if all_order_rejected_buy_copy_events > 0:
            all_order_live_truth_blockers.append("all_order_rejected_buy_copy_events_present")
        if all_order_ledger_rejected_orders > 0:
            all_order_live_truth_blockers.append("all_order_current_ledger_has_rejected_orders")
        if all_order_fallback_filled_buy_copy_events > 0:
            all_order_live_truth_blockers.append("all_order_fallback_fills_not_live_admissible")
        if all_order_clob_filled_buy_copy_events < all_order_buy_source_events:
            all_order_live_truth_blockers.append("not_all_all_order_buys_have_clob_filled_copy")
        if all_order_execution_corrections.get("status") != PASS:
            all_order_live_truth_blockers.append("all_order_execution_corrections_required")
        if all_order_copyability_rejected_buy_events > 0:
            all_order_live_truth_blockers.append("all_order_copyability_rejected_buy_events_present")
        if all_order_copyability_accepted_buy_events < all_order_buy_source_events:
            all_order_live_truth_blockers.append("not_all_all_order_buys_have_copyability_truth")
        all_order_live_truth_status = (
            PASS
            if all_order_status == PASS
            and all_order_buy_source_events > 0
            and all_order_clob_filled_buy_copy_events >= all_order_buy_source_events
            and all_order_fallback_filled_buy_copy_events == 0
            and all_order_rejected_buy_copy_events == 0
            and all_order_execution_corrections.get("status") == PASS
            and all_order_copyability_accepted_buy_events >= all_order_buy_source_events
            and all_order_copyability_rejected_buy_events == 0
            else active_status_from_blockers(all_order_live_truth_blockers, default=ANALYZE)
        )
        all_order_active_status = (
            PASS
            if all_order_live_truth_status == PASS
            else active_status_from_blockers(all_order_live_truth_blockers, default=ANALYZE)
        )
        all_order_exact_copy_summary = {
            "status": all_order_status,
            "active_status": all_order_active_status,
            "paper_proof_status": all_order_paper_proof_status,
            "role": "profit_policy_independent_all_observed_btc5m_order_copy_paper_proof",
            "scope": "current poll new wallet-attributed BTC 5m rows from tracked wallets",
            "rolling_window_moves": len(summary_moves),
            "current_poll_moves": len(enriched_moves),
            "policy": asdict(all_order_policy),
            "source_events": all_order_source_events,
            "buy_source_events": all_order_buy_source_events,
            "lifecycle_source_events": all_order_lifecycle_source_events,
            "buy_intents": all_order_buy_intents,
            "filled_buy_copy_events": all_order_filled_buy_copy_events,
            "clob_filled_buy_copy_events": all_order_clob_filled_buy_copy_events,
            "fallback_filled_buy_copy_events": all_order_fallback_filled_buy_copy_events,
            "rejected_buy_copy_events": all_order_rejected_buy_copy_events,
            "ledger_rejected_orders": all_order_ledger_rejected_orders,
            "cumulative_ledger_rejected_orders": all_order_cumulative_ledger_rejected_orders,
            "copy_policy_rejected_buy_events": all_order_copy_policy_rejected_buy_events,
            "copyability_accepted_buy_events": all_order_copyability_accepted_buy_events,
            "copyability_rejected_buy_events": all_order_copyability_rejected_buy_events,
            "copyability_reason_counts": dict(sorted(all_order_copyability_reason_counts.items())),
            "copyability_rejected_blocker_counts": dict(
                sorted(all_order_copyability_rejected_blocker_counts.items())
            ),
            "copyability_rejected_wallet_route_class_counts": dict(
                sorted(all_order_copyability_rejected_wallet_route_class_counts.items())
            ),
            "copyability_rejected_clob_route_class_counts": dict(
                sorted(all_order_copyability_rejected_clob_route_class_counts.items())
            ),
            "copyability_rejected_book_hashes": sorted(all_order_copyability_rejected_book_hashes)[:20],
            "copyability_rejected_samples": all_order_copyability_rejected_samples,
            "mirrored_lifecycle_events": all_order_mirrored_lifecycle_events,
            "missed_lifecycle_events": all_order_missed_lifecycle_events,
            "coverage_violations": all_order_coverage_violations,
            "mirror_status_counts": dict(sorted(all_order_mirror_status_counts.items())),
            "violation_reason_counts": dict(sorted(all_order_violation_reasons.items())),
            "lifecycle_miss_class_counts": dict(sorted(all_order_lifecycle_miss_class_counts.items())),
            "fill_source_counts": dict(sorted(all_order_fill_source_counts.items())),
            "filled_fill_source_counts": dict(sorted(all_order_filled_fill_source_counts.items())),
            "rejected_fill_source_counts": dict(sorted(all_order_rejected_fill_source_counts.items())),
            "paper_tactic_profile_status_counts": {
                profile_id: dict(sorted(counts.items()))
                for profile_id, counts in sorted(all_order_tactic_profile_status_counts.items())
            },
            "paper_tactic_profile_pass_events": dict(sorted(all_order_tactic_profile_pass_events.items())),
            "clob_book_hashes": sorted(all_order_clob_book_hashes)[:20],
            "required_event_ids_sample": sorted(all_order_required_event_ids)[:50],
            "seed": all_order_seed_report,
            "paper_summary": all_order_paper_summary,
            "execution_corrections": all_order_execution_corrections,
            "current_poll_execution_corrections": all_order_execution_corrections,
            "cumulative_execution_corrections": all_order_cumulative_execution_corrections,
            "execution_tactic_plan": all_order_execution_tactic_plan,
            "current_poll_execution_tactic_plan": all_order_current_execution_tactic_plan,
            "cumulative_execution_tactic_plan": all_order_cumulative_execution_tactic_plan,
            "aggressive_tactic_replay": all_order_aggressive_tactic_replay,
            "micro_batch_all_order_probe": all_order_micro_batch_probe,
            "paper_state_path": self.config.all_order_exact_copy_paper_state_path,
            "paper_event_log_path": self.config.all_order_exact_copy_paper_event_log_path,
            "live_truth_status": all_order_live_truth_status,
            "live_truth_blockers": sorted(set(all_order_live_truth_blockers)),
            "paper_only": True,
            "live_orders_allowed": False,
        }
        state["generated_at"] = utc_now_iso()
        state["config"] = self.config.asdict()
        state["rtds_source_cursor"] = {
            "path": rtds_source_report.get("path"),
            "byte_offset": int(
                rtds_source_report.get("next_byte_offset")
                or rtds_source_report.get("byte_offset")
                or 0
            ),
            "updated_at": utc_now_iso(),
        }
        state["source_mux"] = rtds_source_report
        state["wallets"] = {spec.normalized_address(): spec.asdict() for spec in specs}
        state["seed"] = seed_report
        state["profit_policy_context_id"] = profit_policy_context_id
        state["profit_policy_candidate_id"] = profit_policy_candidate_id or None
        state["prior_profit_policy_context_id"] = prior_profit_policy_context_id or None
        state["prior_profit_policy_candidate_id"] = prior_profit_policy_candidate_id or None
        state["reprocessed_seen_for_new_profit_context"] = reprocess_seen_for_new_profit_context
        state["copyability_context_id"] = current_copyability_context_id
        state["copyability_context"] = current_copyability_context
        state["prior_copyability_context_id"] = prior_copyability_context_id or None
        state["reprocessed_seen_for_new_copyability_context"] = reprocess_seen_for_new_copyability_context
        state["seen_source_fingerprints"] = sorted(seen)[-int(self.config.retain_seen):]
        state["failed_source_fingerprints"] = sorted(failed_seen)[-int(self.config.retain_seen):]
        state["failed_source_details"] = {
            fingerprint: failed_details[fingerprint]
            for fingerprint in sorted(failed_seen)[-int(self.config.retain_seen):]
            if isinstance(failed_details.get(fingerprint), dict)
        }
        state["last_moves"] = summary_moves[-200:]
        state["admission_evidence_window"] = summary_moves[-200:]

        def _identity_count(report: dict, key: str, fallback_key: str | None = None) -> int:
            counts = report.get(key)
            if counts is None and fallback_key:
                counts = report.get(fallback_key)
            if not isinstance(counts, dict):
                return 0
            return sum(int(count or 0) for count in counts.values())

        wallet_identity_mismatch_rows = sum(
            _identity_count(
                report,
                "wallet_identity_mismatch_rows_admission_relevant_by_source",
                "wallet_identity_mismatch_rows_by_source",
            )
            for report in wallet_reports
            if isinstance(report, dict)
        )
        wallet_identity_mismatch_quarantined_rows = sum(
            _identity_count(report, "wallet_identity_mismatch_rows_quarantined_by_source")
            for report in wallet_reports
            if isinstance(report, dict)
        )
        wallet_identity_missing_rows = sum(
            sum(int(count or 0) for count in (report.get("wallet_identity_missing_rows_by_source") or {}).values())
            for report in wallet_reports
            if isinstance(report, dict)
        )
        identity_contamination = {
            "status": "PASS" if wallet_identity_mismatch_rows == 0 and wallet_identity_missing_rows == 0 else "FAIL",
            "wallet_identity_mismatch_rows": wallet_identity_mismatch_rows,
            "wallet_identity_mismatch_quarantined_rows": wallet_identity_mismatch_quarantined_rows,
            "wallet_identity_missing_rows": wallet_identity_missing_rows,
            "admission_blocker": bool(wallet_identity_mismatch_rows or wallet_identity_missing_rows),
        }
        admission_config_eligible = bool(
            self.config.admission_mode
            and self.config.enable_copyability_gate
            and self.config.enable_clob_books
            and self.config.require_clob_book_evidence_for_efficiency
            and self.config.strict_mirror_coverage
        )
        admission_eligible = bool(
            admission_config_eligible
            and identity_contamination["status"] == "PASS"
            and copy_efficiency.get("status") == "PASS"
            and mirror_coverage_status == "PASS"
        )
        hot_path_adaptive = _hot_path_adaptive_evidence(
            enriched_moves,
            config=self.config,
            now_ts=now_ts,
        )
        hot_path_pass_signals = hot_path_adaptive.pop("_pass_signal_objects", [])
        hot_path_inventory_intents = hot_path_adaptive.pop("_inventory_intent_objects", [])
        hot_path_inventory_summary = hot_path_adaptive.pop("_inventory_summary", {})
        hot_path_single_wallet_intents = hot_path_adaptive.pop("_single_wallet_intent_objects", [])
        hot_path_single_wallet_summary = hot_path_adaptive.pop("_single_wallet_summary", {})
        hot_path_adaptive_config = hot_path_adaptive.pop("_adaptive_config", None)
        hot_path_intents = [
            intent
            for signal in hot_path_pass_signals
            for intent in [signal_to_intent(signal, config=hot_path_adaptive_config)]
            if intent is not None
        ]

        def _paper_lifecycle_for_adaptive_intents(
            intents: list[CopyIntent],
            *,
            no_signal_status: str,
            paper_engine: PaperWalletCopyEngine,
        ) -> dict[str, Any]:
            if not intents:
                return {
                    "status": no_signal_status,
                    "intents_created": 0,
                    "new_paper_orders": 0,
                    "paper_orders": 0,
                    "filled_orders": 0,
                    "rejected_orders": 0,
                    "fill_source_counts": {},
                    "signal_to_paper_latency_s": None,
                    "source_event_ids": [],
                    "clob_book_hashes": [],
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            before_paper_state = paper_engine.load_state()
            before_order_ids = {
                str(row.get("order_id"))
                for row in before_paper_state.get("orders") or []
                if isinstance(row, dict) and row.get("order_id")
            }
            paper_state = paper_engine.apply_intents(intents)
            intent_ids = {intent.intent_id for intent in intents}
            intent_orders = [
                row
                for row in paper_state.get("orders") or []
                if isinstance(row, dict) and str(row.get("intent_id")) in intent_ids
            ]
            new_intent_orders = [
                row for row in intent_orders if str(row.get("order_id") or "") not in before_order_ids
            ]
            fill_source_counts = Counter(
                str(
                    (
                        (
                            (row.get("source_intent") or {}).get("fill_estimate")
                            if isinstance(row.get("source_intent"), dict)
                            else {}
                        )
                        or {}
                    ).get("source")
                    or "unknown"
                )
                for row in intent_orders
                if isinstance(row, dict)
            )
            clob_book_hashes = sorted(
                {
                    str(
                        (
                            (
                                (
                                    (row.get("source_intent") or {}).get("fill_estimate")
                                    if isinstance(row.get("source_intent"), dict)
                                    else {}
                                )
                                or {}
                            ).get("book")
                            or {}
                        ).get("book_hash")
                        or ""
                    )
                    for row in intent_orders
                    if isinstance(row, dict)
                }
                - {""}
            )
            source_event_ids = sorted(
                {
                    str(source_event_id)
                    for intent in intents
                    for source_event_id in (
                        (
                            (intent.metadata.get("adaptive_signal") or {}).get("source_event_ids")
                            if isinstance(intent.metadata, dict)
                            and isinstance(intent.metadata.get("adaptive_signal"), dict)
                            else []
                        )
                        or []
                    )
                }
            )
            filled_orders = sum(1 for row in intent_orders if row.get("final_status") == "FILLED")
            rejected_orders = sum(1 for row in intent_orders if row.get("final_status") == "REJECTED")
            return {
                "status": "PASS"
                if filled_orders >= len(intents) and rejected_orders == 0
                else "WATCH",
                "intents_created": len(intents),
                "new_paper_orders": len(new_intent_orders),
                "paper_orders": len(intent_orders),
                "filled_orders": filled_orders,
                "rejected_orders": rejected_orders,
                "fill_source_counts": dict(sorted(fill_source_counts.items())),
                "signal_to_paper_latency_s": round(max(0.0, time.time() - now_ts), 6),
                "source_event_ids": source_event_ids[:50],
                "clob_book_hashes": clob_book_hashes[:20],
                "paper_only": True,
                "live_orders_allowed": False,
            }

        hot_path_paper_lifecycle = _paper_lifecycle_for_adaptive_intents(
            hot_path_intents,
            no_signal_status="NO_PASS_SIGNALS",
            paper_engine=self.paper,
        )
        hot_path_inventory_paper_lifecycle = _paper_lifecycle_for_adaptive_intents(
            hot_path_inventory_intents,
            no_signal_status="NO_RUNTIME_INVENTORY_INTENTS",
            paper_engine=self.paper,
        )
        hot_path_single_wallet_paper_lifecycle = _paper_lifecycle_for_adaptive_intents(
            hot_path_single_wallet_intents,
            no_signal_status="NO_SINGLE_WALLET_EXACT_COPY_INTENTS",
            paper_engine=self.single_wallet_exact_copy_paper,
        )
        single_wallet_clob_only = (
            hot_path_single_wallet_paper_lifecycle["intents_created"] > 0
            and int(hot_path_single_wallet_paper_lifecycle["fill_source_counts"].get("clob_book_evidence") or 0)
            >= hot_path_single_wallet_paper_lifecycle["intents_created"]
        )
        single_wallet_live_promotion_blockers: list[str] = []
        if hot_path_single_wallet_paper_lifecycle["intents_created"] <= 0:
            single_wallet_live_promotion_blockers.append("no_single_wallet_exact_copy_intents")
        if hot_path_single_wallet_paper_lifecycle["intents_created"] < MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS:
            single_wallet_live_promotion_blockers.append(
                "single_wallet_live_promotion_required_buy_copy_events_below_10"
            )
        if hot_path_single_wallet_paper_lifecycle["status"] != "PASS" or not single_wallet_clob_only:
            single_wallet_live_promotion_blockers.append(
                "single_wallet_live_promotion_clob_copy_lifecycle_not_pass"
            )
        single_wallet_live_promotion = {
            "status": "PASS" if not single_wallet_live_promotion_blockers else "ANALYZE",
            "required_buy_copy_events": hot_path_single_wallet_paper_lifecycle["intents_created"],
            "clob_filled_buy_copy_events": (
                hot_path_single_wallet_paper_lifecycle["filled_orders"] if single_wallet_clob_only else 0
            ),
            "fallback_filled_buy_copy_events": 0,
            "rejected_buy_copy_events": hot_path_single_wallet_paper_lifecycle["rejected_orders"],
            "missed_buy_copy_events": max(
                0,
                hot_path_single_wallet_paper_lifecycle["intents_created"]
                - hot_path_single_wallet_paper_lifecycle["filled_orders"],
            ),
            "min_required_buy_copy_events": MIN_SINGLE_WALLET_LIVE_PROMOTION_CLOB_BUYS,
            "fill_source_counts": hot_path_single_wallet_paper_lifecycle["fill_source_counts"],
            "paper_only": True,
            "live_orders_allowed": False,
            "role": "primary_single_wallet_live_promotion_current_poll_truth",
            "blockers": sorted(set(single_wallet_live_promotion_blockers)),
        }
        tracker_time_replay = _hot_path_tracker_time_replay_evidence(
            summary_moves,
            config=self.config,
            now_ts=now_ts,
        )
        replay_pass_signals = tracker_time_replay.pop("_pass_signal_objects", [])
        replay_adaptive_config = tracker_time_replay.pop("_adaptive_config", None)
        replay_intents = [
            _tracker_time_replay_intent(intent)
            for signal in replay_pass_signals
            for intent in [signal_to_intent(signal, config=replay_adaptive_config)]
            if intent is not None
        ]
        replay_paper_lifecycle = _paper_lifecycle_for_adaptive_intents(
            replay_intents,
            no_signal_status="NO_TRACKER_TIME_PASS_SIGNALS",
            paper_engine=self.tracker_time_replay_paper,
        )
        replay_summary = (
            tracker_time_replay.get("summary")
            if isinstance(tracker_time_replay.get("summary"), dict)
            else {}
        )
        replay_summary.update(
            {
                "tracker_time_replay_intents_created": replay_paper_lifecycle["intents_created"],
                "tracker_time_replay_new_paper_orders": replay_paper_lifecycle["new_paper_orders"],
                "tracker_time_replay_paper_orders": replay_paper_lifecycle["paper_orders"],
                "tracker_time_replay_filled_orders": replay_paper_lifecycle["filled_orders"],
                "tracker_time_replay_rejected_orders": replay_paper_lifecycle["rejected_orders"],
                "tracker_time_replay_fill_source_counts": replay_paper_lifecycle["fill_source_counts"],
                "tracker_time_replay_signal_to_paper_latency_s": replay_paper_lifecycle["signal_to_paper_latency_s"],
                "tracker_time_replay_paper_state_path": self.config.tracker_time_replay_paper_state_path,
                "tracker_time_replay_paper_event_log_path": self.config.tracker_time_replay_paper_event_log_path,
            }
        )
        tracker_time_replay["summary"] = replay_summary
        tracker_time_replay["paper_lifecycle"] = replay_paper_lifecycle
        if replay_paper_lifecycle["intents_created"] > 0:
            tracker_time_replay["status"] = (
                "PASS" if replay_paper_lifecycle["status"] == "PASS" else "WATCH"
            )
            if replay_paper_lifecycle["status"] != "PASS":
                tracker_time_replay["blockers"] = [
                    *list(tracker_time_replay.get("blockers") or []),
                    "tracker_time_replay_paper_lifecycle_not_fully_filled",
                ]
        hot_path_summary = (
            hot_path_adaptive.get("summary")
            if isinstance(hot_path_adaptive.get("summary"), dict)
            else {}
        )
        hot_path_summary.update(
            {
                "hot_path_intents_created": hot_path_paper_lifecycle["intents_created"],
                "hot_path_new_paper_orders": hot_path_paper_lifecycle["new_paper_orders"],
                "hot_path_paper_orders": hot_path_paper_lifecycle["paper_orders"],
                "hot_path_filled_orders": hot_path_paper_lifecycle["filled_orders"],
                "hot_path_rejected_orders": hot_path_paper_lifecycle["rejected_orders"],
                "hot_path_fill_source_counts": hot_path_paper_lifecycle["fill_source_counts"],
                "hot_path_signal_to_paper_latency_s": hot_path_paper_lifecycle["signal_to_paper_latency_s"],
                "hot_path_inventory_intents_created": hot_path_inventory_paper_lifecycle["intents_created"],
                "hot_path_inventory_new_paper_orders": hot_path_inventory_paper_lifecycle["new_paper_orders"],
                "hot_path_inventory_paper_orders": hot_path_inventory_paper_lifecycle["paper_orders"],
                "hot_path_inventory_filled_orders": hot_path_inventory_paper_lifecycle["filled_orders"],
                "hot_path_inventory_rejected_orders": hot_path_inventory_paper_lifecycle["rejected_orders"],
                "hot_path_inventory_fill_source_counts": hot_path_inventory_paper_lifecycle["fill_source_counts"],
                "hot_path_inventory_signal_to_paper_latency_s": (
                    hot_path_inventory_paper_lifecycle["signal_to_paper_latency_s"]
                ),
                "hot_path_inventory_measurement": hot_path_inventory_summary,
                "hot_path_single_wallet_exact_copy_intents_created": hot_path_single_wallet_paper_lifecycle[
                    "intents_created"
                ],
                "hot_path_single_wallet_exact_copy_new_paper_orders": hot_path_single_wallet_paper_lifecycle[
                    "new_paper_orders"
                ],
                "hot_path_single_wallet_exact_copy_paper_orders": hot_path_single_wallet_paper_lifecycle[
                    "paper_orders"
                ],
                "hot_path_single_wallet_exact_copy_filled_orders": hot_path_single_wallet_paper_lifecycle[
                    "filled_orders"
                ],
                "hot_path_single_wallet_exact_copy_rejected_orders": hot_path_single_wallet_paper_lifecycle[
                    "rejected_orders"
                ],
                "hot_path_single_wallet_exact_copy_fill_source_counts": hot_path_single_wallet_paper_lifecycle[
                    "fill_source_counts"
                ],
                "hot_path_single_wallet_exact_copy_signal_to_paper_latency_s": (
                    hot_path_single_wallet_paper_lifecycle["signal_to_paper_latency_s"]
                ),
                "hot_path_single_wallet_exact_copy_measurement": hot_path_single_wallet_summary,
                "hot_path_single_wallet_exact_copy_paper_state_path": (
                    self.config.single_wallet_exact_copy_paper_state_path
                ),
                "hot_path_single_wallet_exact_copy_paper_event_log_path": (
                    self.config.single_wallet_exact_copy_paper_event_log_path
                ),
                "primary_single_wallet_live_promotion": single_wallet_live_promotion,
                "primary_single_wallet_live_promotion_pass": single_wallet_live_promotion["status"] == "PASS",
                "tracker_time_replay_status": tracker_time_replay.get("status"),
                "tracker_time_replay_pass_signals": replay_summary.get("pass_signals"),
                "tracker_time_replay_intents_created": replay_paper_lifecycle["intents_created"],
                "tracker_time_replay_filled_orders": replay_paper_lifecycle["filled_orders"],
                "tracker_time_replay_rejected_orders": replay_paper_lifecycle["rejected_orders"],
                "tracker_time_replay_recent_observed_moves": replay_summary.get("recent_observed_moves"),
                "tracker_time_replay_observation_window_s": replay_summary.get("observation_window_s"),
            }
        )
        hot_path_adaptive["summary"] = hot_path_summary
        hot_path_adaptive["paper_lifecycle"] = hot_path_paper_lifecycle
        hot_path_adaptive["inventory_paper_lifecycle"] = hot_path_inventory_paper_lifecycle
        hot_path_adaptive["single_wallet_exact_copy_paper_lifecycle"] = hot_path_single_wallet_paper_lifecycle
        hot_path_adaptive["tracker_time_replay"] = tracker_time_replay
        if hot_path_paper_lifecycle["intents_created"] > 0:
            hot_path_adaptive["status"] = (
                "PASS" if hot_path_paper_lifecycle["status"] == "PASS" else "WATCH"
            )
            if hot_path_paper_lifecycle["status"] != "PASS":
                hot_path_adaptive["blockers"] = [
                    *list(hot_path_adaptive.get("blockers") or []),
                    "hot_path_paper_lifecycle_not_fully_filled",
                ]
        elif hot_path_inventory_paper_lifecycle["intents_created"] > 0:
            hot_path_adaptive["status"] = (
                "PASS" if hot_path_inventory_paper_lifecycle["status"] == "PASS" else "WATCH"
            )
            if hot_path_inventory_paper_lifecycle["status"] == "PASS":
                hot_path_adaptive["blockers"] = []
                hot_path_adaptive["role"] = "current_poll_inventory_measurement_only_not_live_admission"
            else:
                hot_path_adaptive["blockers"] = [
                    *list(hot_path_adaptive.get("blockers") or []),
                    "hot_path_inventory_paper_lifecycle_not_fully_filled",
                ]
        elif hot_path_single_wallet_paper_lifecycle["intents_created"] > 0:
            hot_path_adaptive["status"] = "WATCH"
            hot_path_adaptive["role"] = "current_poll_single_wallet_live_promotion_burnin"
            single_wallet_blockers = [
                blocker
                for blocker in list(hot_path_adaptive.get("blockers") or [])
                if blocker not in {"runtime_fresh_but_single_wallet_only"}
            ]
            if hot_path_single_wallet_paper_lifecycle["status"] == "PASS":
                single_wallet_blockers.extend(single_wallet_live_promotion["blockers"])
            else:
                single_wallet_blockers.append("hot_path_single_wallet_exact_copy_paper_lifecycle_not_fully_filled")
            hot_path_adaptive["blockers"] = sorted(set(single_wallet_blockers))
        elif tracker_time_replay.get("status") == "PASS":
            hot_path_adaptive["blockers"] = [
                *list(hot_path_adaptive.get("blockers") or []),
                "tracker_time_replay_passed_but_current_poll_truth_missing",
            ]
        current_poll_diagnostics = _current_poll_diagnostics(
            wallet_reports=wallet_reports,
            new_events=new_events,
            raw_intents=raw_intents,
            enriched_intents=enriched_intents,
            current_poll_copy_efficiency=current_poll_copy_efficiency,
            profit_filter_decisions=profit_filter_decisions,
            copyability_decisions=copyability_decisions,
        )
        hot_path_current_poll_diagnostics = {
            "status": current_poll_diagnostics.get("status"),
            "blockers": list(current_poll_diagnostics.get("blockers") or []),
            "current_poll_ladder": current_poll_diagnostics.get("current_poll_ladder") or {},
            "zero_current_poll_root_cause": current_poll_diagnostics.get("zero_current_poll_root_cause"),
            "fresh_buy_loss_stage": current_poll_diagnostics.get("fresh_buy_loss_stage"),
            "current_poll_top_copyability_reject_reason": current_poll_diagnostics.get(
                "current_poll_top_copyability_reject_reason"
            ),
            "current_poll_copyability_rejected_reason_counts": (
                current_poll_diagnostics.get("current_poll_copyability_rejected_reason_counts") or {}
            ),
            "current_poll_top_copyability_reject_blocker": current_poll_diagnostics.get(
                "current_poll_top_copyability_reject_blocker"
            ),
            "current_poll_copyability_rejected_blocker_counts": (
                current_poll_diagnostics.get("current_poll_copyability_rejected_blocker_counts") or {}
            ),
            "current_poll_fresh_at_fetch_start_stale_at_decision_rows": current_poll_diagnostics.get(
                "current_poll_fresh_at_fetch_start_stale_at_decision_rows"
            ),
            "current_poll_stale_before_wallet_fetch_rows": current_poll_diagnostics.get(
                "current_poll_stale_before_wallet_fetch_rows"
            ),
            "current_poll_copyability_staleness_origin_counts": (
                current_poll_diagnostics.get("current_poll_copyability_staleness_origin_counts") or {}
            ),
            "source_route_status_counts": current_poll_diagnostics.get("source_route_status_counts") or {},
            "source_route_class_counts": current_poll_diagnostics.get("source_route_class_counts") or {},
            "source_route_degraded_recovered_rows": current_poll_diagnostics.get(
                "source_route_degraded_recovered_rows"
            ),
            "all_order_copyability_rejected_buy_events": all_order_exact_copy_summary.get(
                "copyability_rejected_buy_events"
            ),
            "all_order_copyability_rejected_blocker_counts": all_order_exact_copy_summary.get(
                "copyability_rejected_blocker_counts"
            )
            or {},
            "all_order_copyability_rejected_wallet_route_class_counts": all_order_exact_copy_summary.get(
                "copyability_rejected_wallet_route_class_counts"
            )
            or {},
            "all_order_copyability_rejected_clob_route_class_counts": all_order_exact_copy_summary.get(
                "copyability_rejected_clob_route_class_counts"
            )
            or {},
            "all_order_copyability_rejected_samples": all_order_exact_copy_summary.get(
                "copyability_rejected_samples"
            )
            or [],
            "paper_only": True,
            "live_orders_allowed": False,
        }
        hot_path_summary["current_poll_diagnostics"] = hot_path_current_poll_diagnostics
        hot_path_adaptive["current_poll_diagnostics"] = hot_path_current_poll_diagnostics
        hot_path_freshness = (
            hot_path_summary.get("freshness_diagnostics")
            if isinstance(hot_path_summary.get("freshness_diagnostics"), dict)
            else {}
        )
        hot_path_adaptive_summary = {
            "status": hot_path_adaptive.get("status"),
            "blockers": list(hot_path_adaptive.get("blockers") or []),
            "current_poll_moves": hot_path_summary.get("current_poll_moves"),
            "pass_signals": hot_path_summary.get("pass_signals"),
            "hot_path_intents_created": hot_path_summary.get("hot_path_intents_created"),
            "hot_path_filled_orders": hot_path_summary.get("hot_path_filled_orders"),
            "hot_path_rejected_orders": hot_path_summary.get("hot_path_rejected_orders"),
            "runtime_fresh_buy_events_le_cap": hot_path_freshness.get("runtime_fresh_buy_events_le_cap"),
            "runtime_eligible_wallets": hot_path_freshness.get("runtime_eligible_wallets"),
            "source_feed_delayed": hot_path_freshness.get("source_feed_delayed"),
            "runtime_inventory_research_candidates": hot_path_summary.get("runtime_inventory_research_candidates"),
            "current_poll_diagnostics": hot_path_current_poll_diagnostics,
            "current_poll_ladder": hot_path_current_poll_diagnostics["current_poll_ladder"],
            "current_poll_zero_current_poll_root_cause": hot_path_current_poll_diagnostics[
                "zero_current_poll_root_cause"
            ],
            "current_poll_fresh_buy_loss_stage": hot_path_current_poll_diagnostics["fresh_buy_loss_stage"],
            "hot_path_inventory_intents_created": hot_path_summary.get("hot_path_inventory_intents_created"),
            "hot_path_inventory_filled_orders": hot_path_summary.get("hot_path_inventory_filled_orders"),
            "hot_path_inventory_rejected_orders": hot_path_summary.get("hot_path_inventory_rejected_orders"),
            "single_wallet_exact_copy_intents_created": hot_path_summary.get(
                "hot_path_single_wallet_exact_copy_intents_created"
            ),
            "single_wallet_exact_copy_filled_orders": hot_path_summary.get(
                "hot_path_single_wallet_exact_copy_filled_orders"
            ),
            "paper_lifecycle_status": hot_path_paper_lifecycle.get("status"),
            "inventory_paper_lifecycle_status": hot_path_inventory_paper_lifecycle.get("status"),
            "single_wallet_exact_copy_paper_lifecycle_status": hot_path_single_wallet_paper_lifecycle.get("status"),
            "tracker_time_replay_status": tracker_time_replay.get("status"),
            "role": "current_poll_adaptive_summary_alias_not_live_admission",
            "paper_only": True,
            "live_orders_allowed": False,
        }
        information_source_fusion = _information_source_fusion_plan(
            config=self.config,
            wallet_reports=wallet_reports,
            current_poll_moves=enriched_moves,
            evidence_window_moves=summary_moves,
            market_ws_rows=market_ws_rows,
            current_poll_diagnostics=current_poll_diagnostics,
            copy_efficiency=copy_efficiency,
            current_poll_copy_efficiency=current_poll_copy_efficiency,
            hot_path_adaptive_summary=hot_path_adaptive_summary,
            clob_book_cache_report=clob_book_cache_report,
            poll_runtime_limited=poll_runtime_limited,
            runtime_skipped_wallets=runtime_skipped_wallets,
            runtime_skipped_events=runtime_skipped_events,
        )
        state["summary"] = {
            "wallets": len(specs),
            "tracker_scope": tracker_scope,
            "admission_config_eligible": admission_config_eligible,
            "admission_eligible": admission_eligible,
            "identity_contamination": identity_contamination,
            "research_mode": not admission_config_eligible,
            "new_wallet_events": len(new_events),
            "new_wallet_event_action_counts": _wallet_event_action_counts(new_events),
            "admission_evidence_window": {
                "window_moves": len(summary_moves),
                "current_poll_moves": len(enriched_moves),
                "prior_window_moves": len(prior_window),
                "event_log_window_moves": len(log_window),
            },
            "filtered_by_profit_policy_events": int(filtered_by_policy_counts.get("profit_policy") or 0),
            "filtered_by_copyability_policy_events": int(filtered_by_policy_counts.get("copyability") or 0),
            "filtered_by_copy_policy_events": int(filtered_by_policy_counts.get("copy_policy") or 0),
            "filtered_by_policy_counts": dict(sorted(filtered_by_policy_counts.items())),
            "new_buy_copy_events": len(raw_intents),
            "new_buy_copy_intents": len(enriched_intents),
            "new_unique_buy_copy_intents": len(enriched_intents),
            "new_lifecycle_events": sum(1 for event in new_events if not event.is_buy),
            "mirror_scope": "wallet-attributed normalized BTC 5m Data API rows",
            "mirror_required_events": mirror_required_events,
            "mirrored_events": mirrored_events,
            "mirror_coverage_pct": coverage_pct,
            "mirror_coverage_status": mirror_coverage_status,
            "mirror_coverage_by_action": mirror_coverage_by_action,
            "coverage_violation_by_action_reason": coverage_violation_by_action_reason,
            "lifecycle_miss_class_counts": dict(lifecycle_miss_class_counts),
            "buy_fill_coverage": buy_fill_coverage,
            "dual_action_event_id_count": _dual_action_event_id_count(new_events),
            "same_tx_opposing_action_count": _same_tx_opposing_action_count(new_events),
            "uncovered_sell_notional_usd": round(uncovered_sell_notional_usd, 6),
            "coverage_violations": coverage_violations[:50],
            "failed_source_fingerprints": len(failed_seen),
            "profit_policy": profit_policy_report,
            "seed": seed_report,
            "copyability_policy": {
                "enabled": bool(self.config.enable_copyability_gate),
                **copyability_policy.asdict(),
            },
            "latency": _latency_summary(new_events, now_ts=now_ts, wallet_reports=wallet_reports),
            "clob_book_cache": _clob_book_cache_summary(clob_book_cache_report)
            if self.config.enable_clob_books
            else {"enabled": False},
            "copy_efficiency": copy_efficiency,
            "paper_copy_contract": {
                "current_poll": _paper_copy_contract(
                    current_poll_copy_efficiency,
                    scope="current_poll",
                ),
                "evidence_window": _paper_copy_contract(
                    copy_efficiency,
                    scope="rolling_admission_evidence_window",
                ),
            },
            "current_poll_diagnostics": current_poll_diagnostics,
            "all_order_exact_copy": all_order_exact_copy_summary,
            "hot_path_adaptive": hot_path_adaptive,
            "hot_path_adaptive_summary": hot_path_adaptive_summary,
            "information_source_fusion": information_source_fusion,
            "information_source_fusion_status": information_source_fusion["status"],
            "information_source_next_action": information_source_fusion["next_action"],
            "hot_path_adaptive_status": hot_path_adaptive_summary["status"],
            "hot_path_current_poll_moves": hot_path_adaptive_summary["current_poll_moves"],
            "hot_path_pass_signals": hot_path_adaptive_summary["pass_signals"],
            "hot_path_runtime_fresh_buy_events_le_cap": hot_path_adaptive_summary[
                "runtime_fresh_buy_events_le_cap"
            ],
            "hot_path_runtime_eligible_wallets": hot_path_adaptive_summary["runtime_eligible_wallets"],
            "hot_path_source_feed_delayed": hot_path_adaptive_summary["source_feed_delayed"],
            "hot_path_runtime_inventory_research_candidates": hot_path_adaptive_summary[
                "runtime_inventory_research_candidates"
            ],
            "hot_path_paper_lifecycle_status": hot_path_adaptive_summary["paper_lifecycle_status"],
            "evidence_status_counts": _evidence_status_counts(summary_moves),
            "market_ws_candidates_loaded": len(market_ws_rows),
            "paper_summary": paper_state.get("summary"),
            "all_order_exact_copy_paper_summary": all_order_paper_state.get("summary"),
            "all_order_tactic_replay_paper_summary": all_order_aggressive_tactic_replay.get("paper_summary")
            if isinstance(all_order_aggressive_tactic_replay, dict)
            else {},
            "paper_only": True,
            "live_orders_allowed": False,
            "wallet_reports": wallet_reports,
            "poll_runtime": {
                "status": "LIMIT_REACHED" if poll_runtime_limited else "OK",
                "max_poll_runtime_s": max_poll_runtime_s,
                "elapsed_s": round(time.time() - poll_started_ts, 6),
                "skipped_wallets": len(runtime_skipped_wallets),
                "skipped_wallet_addresses": runtime_skipped_wallets[:20],
                "skipped_events": runtime_skipped_events,
            },
        }
        _write_tracker_state_with_non_regression(self.config.state_path, state)
        append_jsonl_many(self.config.event_log_path, enriched_moves)
        return state
