"""Polymarket wallet-history ingestion and normalization."""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

import requests

from src.wallet_copy.http_client import PolymarketHttpClient, PolymarketRouteError
from src.wallet_copy.models import WalletEvent, WalletSpec, num, parse_ts, stable_id


BTC_5M_PATTERNS = (
    re.compile(r"\bbtc\b.*\b5m\b", re.IGNORECASE),
    re.compile(r"bitcoin.*(?<!\d)5\s*minute", re.IGNORECASE),
    re.compile(r"btc.*up.*down.*(?:\b5m\b|(?<!\d)5\s*minute)", re.IGNORECASE),
)


def extract_asset(*values: Any) -> str:
    text = " ".join(str(value or "") for value in values).lower()
    if re.search(r"(?<![a-z0-9])btc(?![a-z0-9])", text) or "bitcoin" in text:
        return "BTC"
    return ""


def extract_duration(*values: Any) -> str:
    text = " ".join(str(value or "") for value in values).lower()
    if re.search(r"\b15m\b|15\s*minute", text):
        return "15m"
    if re.search(r"\b5m\b|(?<!\d)5\s*minute", text):
        return "5m"
    if re.search(r"\b1h\b|1\s*hour", text):
        return "1h"
    return ""


def btc_5m_like(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("marketSlug", "market_slug", "eventSlug", "slug", "title", "question")
    )
    return any(pattern.search(text) for pattern in BTC_5M_PATTERNS) or (
        extract_asset(text) == "BTC" and extract_duration(text) == "5m"
    )


def _first(row: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return default


def _outcome_index(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _source_key(row_type: str, query_key: str = "user") -> str:
    return f"{row_type}:{query_key}"


def _tag_source(
    row: dict[str, Any],
    *,
    row_type: str,
    query_key: str,
    source_timing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tagged = dict(row)
    tagged.setdefault("_walletCopySource", _source_key(row_type, query_key))
    tagged.setdefault("_walletCopyQueryKey", query_key)
    if isinstance(source_timing, dict):
        for key, value in source_timing.items():
            if str(key).startswith("__"):
                continue
            tagged.setdefault(key, value)
    return tagged


WALLET_IDENTITY_KEYS = (
    "proxyWallet",
    "proxy_wallet",
    "wallet",
    "walletAddress",
    "wallet_address",
    "user",
)


def _wallet_identity_values(row: dict[str, Any]) -> dict[str, str]:
    identities: dict[str, str] = {}
    for key in WALLET_IDENTITY_KEYS:
        value = row.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip().lower()
        if text.startswith("0x"):
            identities[key] = text
    return identities


def wallet_identity_status(row: dict[str, Any], spec: WalletSpec) -> tuple[str, dict[str, str]]:
    identities = _wallet_identity_values(row)
    if not identities:
        return "MISSING", identities
    expected = spec.normalized_address()
    if all(value == expected for value in identities.values()):
        return "MATCHED", identities
    return "MISMATCH", identities


def wallet_identity_matches(row: dict[str, Any], spec: WalletSpec) -> bool:
    status, _identities = wallet_identity_status(row, spec)
    return status == "MATCHED"


def _event_source(event: WalletEvent) -> str:
    return str(event.raw.get("_walletCopySource") or event.row_type)


def _event_quality(event: WalletEvent) -> tuple[int, int, int, int, int, int]:
    return (
        1 if event.transaction_hash else 0,
        1 if event.condition_id else 0,
        1 if event.token_id else 0,
        1 if event.usdc_size > 0 else 0,
        1 if event.size > 0 else 0,
        1 if event.row_type == "trade" else 0,
    )


def _identity_quality_at_least(candidate: WalletEvent, existing: WalletEvent) -> bool:
    candidate_quality = _event_quality(candidate)[:5]
    existing_quality = _event_quality(existing)[:5]
    return all(candidate_value >= existing_value for candidate_value, existing_value in zip(candidate_quality, existing_quality))


def _event_age_or_inf(event: WalletEvent) -> float:
    return float(event.age_s) if event.age_s is not None else float("inf")


def _semantic_event_key(event: WalletEvent) -> str:
    tx = str(event.transaction_hash or "").strip().lower()
    if tx:
        return stable_id(
            "wesk",
            {
                "wallet": event.source_wallet.lower(),
                "tx": tx,
                "token_id": event.token_id,
                "action": event.action.upper(),
                "price": round(float(event.price), 8),
                "size": round(float(event.size), 8),
            },
        )
    return stable_id(
        "wesk",
        {
            "wallet": event.source_wallet.lower(),
            "condition_id": event.condition_id,
            "token_id": event.token_id,
            "outcome": event.outcome,
            "action": event.action.upper(),
            "price": round(float(event.price), 8),
            "size": round(float(event.size), 8),
            "event_ts": event.event_ts,
        },
    )


def _prefer_event(existing: WalletEvent, candidate: WalletEvent) -> WalletEvent:
    existing_age = _event_age_or_inf(existing)
    candidate_age = _event_age_or_inf(candidate)
    if candidate_age + 1e-6 < existing_age and _identity_quality_at_least(candidate, existing):
        return candidate
    if existing_age + 1e-6 < candidate_age and _identity_quality_at_least(existing, candidate):
        return existing
    if existing.row_type == "activity" and candidate.row_type == "trade":
        return candidate
    if _event_quality(candidate) > _event_quality(existing):
        return candidate
    return existing


def _age_summary(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"min_s": None, "avg_s": None, "max_s": None}
    return {
        "min_s": round(min(values), 6),
        "avg_s": round(sum(values) / len(values), 6),
        "max_s": round(max(values), 6),
    }


def _source_freshness_report(events: list[WalletEvent], *, reference_ts: float) -> dict[str, dict[str, Any]]:
    by_source: dict[str, list[WalletEvent]] = {}
    for event in events:
        by_source.setdefault(_event_source(event), []).append(event)

    report: dict[str, dict[str, Any]] = {}
    for source, source_events in sorted(by_source.items()):
        buy_events = [event for event in source_events if event.is_buy]
        buy_ages = [float(event.age_s) for event in buy_events if event.age_s is not None]
        event_ages = [float(event.age_s) for event in source_events if event.age_s is not None]
        buy_ts_values = [float(event.event_ts) for event in buy_events if event.event_ts is not None]
        latest_buy_ts = max(buy_ts_values) if buy_ts_values else None
        latest_buy_lag_s = (
            round(max(0.0, float(reference_ts) - latest_buy_ts), 6)
            if latest_buy_ts is not None
            else None
        )
        fresh_buy_rows_le_10s = sum(1 for age in buy_ages if age <= 10.0)
        fresh_buy_rows_le_30s = sum(1 for age in buy_ages if age <= 30.0)
        stale_buy_rows_gt_300s = sum(1 for age in buy_ages if age > 300.0)
        report[source] = {
            "events": len(source_events),
            "buy_rows": len(buy_events),
            "fresh_buy_rows_le_10s": fresh_buy_rows_le_10s,
            "fresh_buy_rows_le_30s": fresh_buy_rows_le_30s,
            "stale_buy_rows_gt_300s": stale_buy_rows_gt_300s,
            "latest_buy_event_ts": latest_buy_ts,
            "latest_buy_event_lag_s": latest_buy_lag_s,
            "buy_event_age": _age_summary(buy_ages),
            "event_age": _age_summary(event_ages),
            "source_feed_delayed_10s": bool(
                buy_events
                and fresh_buy_rows_le_10s == 0
                and latest_buy_lag_s is not None
                and latest_buy_lag_s > 10.0
            ),
            "source_feed_delayed_30s": bool(
                buy_events
                and fresh_buy_rows_le_30s == 0
                and latest_buy_lag_s is not None
                and latest_buy_lag_s > 30.0
            ),
        }
    return report


def _request_exception_type(exc: requests.RequestException) -> str:
    if isinstance(exc, PolymarketRouteError):
        attempts = exc.route_report.get("attempts") if isinstance(exc.route_report, dict) else []
        exception_types = [
            str(row.get("exception"))
            for row in attempts
            if isinstance(row, dict) and row.get("exception")
        ]
        unique_types = sorted(set(exception_types))
        if len(unique_types) == 1:
            return unique_types[0]
        if unique_types:
            return "PolymarketRouteError_" + "_".join(unique_types[:3])
    return type(exc).__name__


def _route_report_from_exception(exc: BaseException, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(exc, PolymarketRouteError) and isinstance(exc.route_report, dict):
        return deepcopy(exc.route_report)
    response = getattr(exc, "response", None)
    response_report = getattr(response, "wallet_copy_route_report", None)
    if isinstance(response_report, dict):
        return deepcopy(response_report)
    if isinstance(fallback, dict):
        return deepcopy(fallback)
    return {}


def normalize_polymarket_wallet_row(
    row: dict[str, Any],
    *,
    spec: WalletSpec,
    row_type: str,
    observed_ts: float | None = None,
) -> WalletEvent | None:
    observed = float(observed_ts if observed_ts is not None else time.time())
    event_ts = parse_ts(_first(row, "timestamp", "createdAt", "created_at", "time", "date"))
    raw_type = str(row.get("type") or row.get("activityType") or "").upper()
    action = str(_first(row, "side", "action", "transactionType", default="")).upper()
    if not action and raw_type in {"BUY", "SELL", "MERGE", "REDEEM"}:
        action = raw_type
    if not action and raw_type == "TRADE":
        action = str(_first(row, "takerSide", "makerSide", default="")).upper()
    if action in {"", "TRADE"}:
        action = "BUY" if num(_first(row, "size", "amount", default=0.0)) >= 0 else "SELL"

    market_slug = str(_first(row, "marketSlug", "market_slug", "slug", default=""))
    event_slug = str(_first(row, "eventSlug", "event_slug", default=""))
    title = str(_first(row, "title", "question", default=""))
    asset = extract_asset(market_slug, event_slug, title)
    duration = extract_duration(market_slug, event_slug, title)

    if spec.market_filter == "btc_5m" and not btc_5m_like(row):
        return None
    if spec.asset_allowlist and asset and asset.upper() not in {item.upper() for item in spec.asset_allowlist}:
        return None
    if not wallet_identity_matches(row, spec):
        return None

    condition_id = str(_first(row, "conditionId", "condition_id", "market", "marketId", "market_id", default=""))
    outcome = str(_first(row, "outcome", "outcomeName", "tokenOutcome", default=""))
    price = num(_first(row, "price", "avgPrice", "limitPrice", default=0.0))
    size = num(_first(row, "size", "shares", "amount", default=0.0))
    usdc_size = num(_first(row, "usdcSize", "usdc_size", "notional", "value", default=0.0))
    if usdc_size <= 0 and price > 0 and size > 0:
        usdc_size = price * size
    token_id = str(_first(row, "asset", "tokenId", "token_id", "clobTokenId", default=""))
    tx = str(_first(row, "transactionHash", "transaction_hash", "txHash", default=""))
    event_id = stable_id(
        "we",
        {
            "wallet": spec.normalized_address(),
            "row_type": row_type,
            "tx": tx,
            "condition_id": condition_id,
            "token_id": token_id,
            "outcome": outcome,
            "price": round(price, 8),
            "size": round(size, 8),
            "event_ts": event_ts,
        },
    )
    return WalletEvent(
        event_id=event_id,
        source_wallet=spec.normalized_address(),
        wallet_name=spec.name,
        row_type=row_type,
        action=action,
        condition_id=condition_id,
        market_id=str(_first(row, "marketId", "market_id", default=condition_id)),
        market_slug=market_slug,
        event_slug=event_slug,
        title=title,
        asset=asset,
        duration=duration,
        window_start_s=None,
        token_id=token_id,
        outcome=outcome,
        outcome_index=_outcome_index(_first(row, "outcomeIndex", "outcome_index", default=None)),
        price=price,
        size=size,
        usdc_size=round(usdc_size, 6),
        transaction_hash=tx,
        event_ts=event_ts,
        observed_ts=observed,
        api_latency_s=(round(observed - event_ts, 6) if event_ts is not None else None),
        source=str(row.get("_walletCopySource") or "polymarket_data_api"),
        raw=row,
    )


@dataclass(frozen=True)
class WalletHistoryClient:
    spec: WalletSpec
    timeout_s: float = 8.0
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 30.0
    user_agent: str = "polymarket-wallet-copy/1.0"
    retries: int = 3
    last_fetch_report: dict[str, Any] = field(default_factory=dict, init=False, compare=False)
    last_route_report: dict[str, Any] = field(default_factory=dict, init=False, compare=False)

    def _get(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        client = PolymarketHttpClient(
            timeout_s=self.timeout_s,
            connect_timeout_s=self.connect_timeout_s,
            read_timeout_s=self.read_timeout_s,
            retries=self.retries,
            user_agent=self.user_agent,
        )
        try:
            response = client.request(
                "GET",
                f"{self.spec.data_api.rstrip('/')}/{path.lstrip('/')}",
                params=params,
                headers={"Accept": "application/json"},
            )
        except PolymarketRouteError as exc:
            object.__setattr__(self, "last_route_report", exc.route_report)
            raise
        object.__setattr__(
            self,
            "last_route_report",
            getattr(response, "wallet_copy_route_report", {}),
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            for key in ("data", "results", "trades", "activity"):
                rows = payload.get(key)
                if isinstance(rows, list):
                    return [row for row in rows if isinstance(row, dict)]
        return []

    def fetch_trades(self, *, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        return self._get(
            "/trades",
            {
                "user": self.spec.normalized_address(),
                "takerOnly": "false",
                "limit": int(limit),
                "offset": int(offset),
            },
        )

    def fetch_proxy_wallet_trades(self, *, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        return self._get(
            "/trades",
            {
                "proxyWallet": self.spec.normalized_address(),
                "takerOnly": "false",
                "limit": int(limit),
                "offset": int(offset),
            },
        )

    def fetch_activity(self, *, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        return self._get(
            "/activity",
            {
                "user": self.spec.normalized_address(),
                "sortDirection": "DESC",
                "limit": int(limit),
                "offset": int(offset),
            },
        )

    def fetch_events(
        self,
        *,
        limit: int = 500,
        include_activity: bool = True,
        pages: int = 1,
        offset: int = 0,
        parallel_sources: bool = False,
        trade_query_keys: tuple[str, ...] | list[str] | None = None,
    ) -> list[WalletEvent]:
        fetch_started = time.time()
        observed = fetch_started
        full_pagination = int(pages) <= 0
        page_count = 1000 if full_pagination else max(1, int(pages))
        page_size = max(1, int(limit))
        rows: list[tuple[str, dict[str, Any]]] = []
        stop_reason = "page_limit"
        api_error: dict[str, Any] | None = None
        source_errors: list[dict[str, Any]] = []
        last_offset = int(offset)
        pages_read = 0
        source_row_counts: dict[str, int] = {}
        source_btc_5m_like_counts: dict[str, int] = {}
        source_stop_reasons: dict[str, str] = {}
        source_fetch_duration_s_by_source: dict[str, float] = {}
        source_route_reports_by_source: dict[str, dict[str, Any]] = {}
        exhausted_sources: set[str] = set()
        all_trade_sources: tuple[tuple[str, Any], ...] = (
            ("user", self.fetch_trades),
            ("proxyWallet", self.fetch_proxy_wallet_trades),
        )
        requested_trade_query_keys = tuple(str(item) for item in (trade_query_keys or ("user", "proxyWallet")))
        requested_trade_query_key_set = set(requested_trade_query_keys)
        trade_sources: tuple[tuple[str, Any], ...] = tuple(
            (query_key, fetcher)
            for query_key, fetcher in all_trade_sources
            if query_key in requested_trade_query_key_set
        )

        def fetch_source(
            row_type: str,
            query_key: str,
            fetcher: Any,
            *,
            limit: int,
            offset: int,
        ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
            source_started = time.time()
            source_rows = fetcher(limit=limit, offset=offset)
            source_completed = time.time()
            source = _source_key(row_type, query_key)
            route_report = deepcopy(self.last_route_report) if isinstance(self.last_route_report, dict) else {}
            return source_rows, {
                "_walletCopySourceFetchStartedTs": round(source_started, 6),
                "_walletCopySourceFetchCompletedTs": round(source_completed, 6),
                "_walletCopySourceFetchDurationS": round(max(0.0, source_completed - source_started), 6),
                "_walletCopySourceFetchOffset": int(offset),
                "_walletCopySourceFetchLimit": int(limit),
                "_walletCopySourceFetchSource": source,
                "_walletCopySourceRouteStatus": route_report.get("status"),
                "_walletCopySourceRouteHost": route_report.get("host"),
                "_walletCopySourceRouteOriginalHost": route_report.get("original_host"),
                "_walletCopySourceRouteRoutedHost": route_report.get("routed_host"),
                "_walletCopySourceBaseOverrideConfigured": bool(
                    route_report.get("source_base_override_configured")
                ),
                "_walletCopySourceBaseOverrideEnvVar": route_report.get("source_base_override_env_var"),
                "__walletCopySourceRouteReport": route_report,
            }

        def record_source_error(row_type: str, query_key: str, exc: requests.RequestException) -> None:
            source = _source_key(row_type, query_key)
            error_type = _request_exception_type(exc)
            endpoint = f"trades:{query_key}" if row_type == "trade" else "activity"
            is_timeout = isinstance(exc, (requests.Timeout, requests.ReadTimeout, requests.ConnectTimeout)) or (
                "timeout" in error_type.lower()
            )
            source_error = {
                "endpoint": endpoint,
                "source": source,
                "type": error_type,
                "message": str(exc),
                "offset": page_offset,
                "skip_reason": (
                    f"data_api_{error_type}_skip_after_bounded_retries"
                    if is_timeout
                    else f"data_api_{error_type}_skip_after_error"
                ),
                "bounded_retry_count": max(0, int(self.retries) - 1),
                "timeout_connect_s": min(10.0, max(0.001, float(self.connect_timeout_s))),
                "timeout_read_s": min(30.0, max(0.001, float(self.read_timeout_s))),
            }
            route_report = _route_report_from_exception(exc, self.last_route_report)
            if route_report:
                source_error["route_report"] = route_report
                source_route_reports_by_source[source] = route_report
            source_errors.append(source_error)
            source_stop_reasons[source] = f"api_error_{error_type}"
            if row_type == "trade":
                exhausted_sources.add(source)

        for page in range(page_count):
            page_offset = int(offset) + page * page_size
            last_offset = page_offset
            page_trade_rows = 0
            page_sources: list[tuple[str, str, Any]] = [
                ("trade", query_key, fetcher)
                for query_key, fetcher in trade_sources
                if _source_key("trade", query_key) not in exhausted_sources
            ]
            if include_activity:
                page_sources.append(("activity", "user", self.fetch_activity))

            fetched_sources: list[tuple[str, str, list[dict[str, Any]], dict[str, Any]]] = []
            if parallel_sources and len(page_sources) > 1:
                executor = ThreadPoolExecutor(max_workers=len(page_sources))
                futures = {
                    executor.submit(
                        fetch_source,
                        row_type,
                        query_key,
                        fetcher,
                        limit=page_size,
                        offset=page_offset,
                    ): (row_type, query_key)
                    for row_type, query_key, fetcher in page_sources
                }
                pending = set(futures)
                # The HTTP client uses an explicit (connect, read) pair. Give
                # each source enough wall time to exhaust its bounded attempts
                # without restoring an unbounded future wait.
                attempts = max(1, min(3, int(self.retries)))
                per_attempt_s = min(10.0, max(0.001, float(self.connect_timeout_s))) + min(
                    30.0, max(0.001, float(self.read_timeout_s))
                )
                backoff_s = sum(min(0.25 * (2**retry_index), 1.0) for retry_index in range(attempts - 1))
                source_timeout_s = max(0.25, attempts * per_attempt_s + backoff_s)
                parallel_deadline = time.monotonic() + source_timeout_s
                try:
                    while pending:
                        remaining_s = parallel_deadline - time.monotonic()
                        if remaining_s <= 0:
                            raise FuturesTimeoutError()
                        completed = next((future for future in tuple(pending) if future.done()), None)
                        if completed is None:
                            time.sleep(min(0.05, max(0.0, remaining_s)))
                            continue
                        future = completed
                        pending.remove(future)
                        row_type, query_key = futures[future]
                        source = _source_key(row_type, query_key)
                        try:
                            source_rows, source_timing = future.result(timeout=0)
                            fetched_sources.append((row_type, query_key, source_rows, source_timing))
                        except requests.HTTPError as exc:
                            status_code = getattr(exc.response, "status_code", None)
                            if full_pagination and page > 0 and status_code in {400, 404, 416}:
                                reason = f"api_pagination_limit_http_{status_code}"
                                source_stop_reasons[source] = reason
                                if row_type == "trade":
                                    exhausted_sources.add(source)
                                    continue
                                stop_reason = reason
                                pages_read += 1
                                break
                            record_source_error(row_type, query_key, exc)
                            continue
                        except requests.RequestException as exc:
                            record_source_error(row_type, query_key, exc)
                            continue
                except FuturesTimeoutError:
                    for pending_future in pending:
                        row_type, query_key = futures[pending_future]
                        pending_future.cancel()
                        record_source_error(
                            row_type,
                            query_key,
                            requests.ReadTimeout(
                                f"parallel data-api source fetch exceeded {source_timeout_s:.3f}s"
                            ),
                        )
                finally:
                    executor.shutdown(wait=False, cancel_futures=True)
            else:
                for row_type, query_key, fetcher in page_sources:
                    source = _source_key(row_type, query_key)
                    try:
                        source_rows, source_timing = fetch_source(
                            row_type,
                            query_key,
                            fetcher,
                            limit=page_size,
                            offset=page_offset,
                        )
                        fetched_sources.append((row_type, query_key, source_rows, source_timing))
                    except requests.HTTPError as exc:
                        status_code = getattr(exc.response, "status_code", None)
                        if full_pagination and page > 0 and status_code in {400, 404, 416}:
                            reason = f"api_pagination_limit_http_{status_code}"
                            source_stop_reasons[source] = reason
                            if row_type == "trade":
                                exhausted_sources.add(source)
                                continue
                            stop_reason = reason
                            pages_read += 1
                            break
                        record_source_error(row_type, query_key, exc)
                        continue
                    except requests.RequestException as exc:
                        record_source_error(row_type, query_key, exc)
                        continue

            for row_type, query_key, source_rows, source_timing in fetched_sources:
                source = _source_key(row_type, query_key)
                source_duration = source_timing.get("_walletCopySourceFetchDurationS")
                if source_duration is not None:
                    source_fetch_duration_s_by_source[source] = float(source_duration)
                route_report = source_timing.get("__walletCopySourceRouteReport")
                if isinstance(route_report, dict) and route_report:
                    source_route_reports_by_source[source] = deepcopy(route_report)
                source_row_counts[source] = source_row_counts.get(source, 0) + len(source_rows)
                source_btc_5m_like_counts[source] = source_btc_5m_like_counts.get(source, 0) + sum(
                    1 for row in source_rows if btc_5m_like(row)
                )
                if row_type == "trade":
                    page_trade_rows += len(source_rows)
                rows.extend(
                    (
                        row_type,
                        _tag_source(
                            row,
                            row_type=row_type,
                            query_key=query_key,
                            source_timing=source_timing,
                        ),
                    )
                    for row in source_rows
                )
            pages_read += 1
            page_activity_rows = sum(
                len(source_rows)
                for row_type, _, source_rows, _ in fetched_sources
                if row_type == "activity"
            )
            if not page_trade_rows and (not include_activity or not page_activity_rows):
                stop_reason = next(iter(source_stop_reasons.values()), "empty_page")
                break
            if full_pagination and all(_source_key("trade", key) in exhausted_sources for key, _ in trade_sources):
                stop_reason = "all_trade_sources_exhausted"
                break
        fetch_completed = time.time()
        events_by_fingerprint: dict[str, WalletEvent] = {}
        events_by_semantic_key: dict[str, WalletEvent] = {}
        normalized_event_counts: dict[str, int] = {}
        wallet_identity_matched_counts: dict[str, int] = {}
        wallet_identity_missing_counts: dict[str, int] = {}
        wallet_identity_mismatch_counts: dict[str, int] = {}
        wallet_identity_missing_examples: list[dict[str, Any]] = []
        wallet_identity_mismatch_examples: list[dict[str, Any]] = []
        duplicate_trade_rows_dropped = 0
        for row_type, row in rows:
            source = str(row.get("_walletCopySource") or row_type)
            identity_status, identities = wallet_identity_status(row, self.spec)
            if identity_status == "MATCHED":
                wallet_identity_matched_counts[source] = wallet_identity_matched_counts.get(source, 0) + 1
            elif identity_status == "MISSING":
                wallet_identity_missing_counts[source] = wallet_identity_missing_counts.get(source, 0) + 1
                if len(wallet_identity_missing_examples) < 10:
                    wallet_identity_missing_examples.append(
                        {
                            "source": source,
                            "query_key": row.get("_walletCopyQueryKey"),
                            "expected_wallet": self.spec.normalized_address(),
                            "slug": _first(row, "marketSlug", "market_slug", "eventSlug", "slug", default=""),
                            "timestamp": _first(row, "timestamp", "createdAt", "created_at", "time", "date", default=None),
                            "transaction_hash": _first(row, "transactionHash", "transaction_hash", "txHash", default=""),
                        }
                    )
                continue
            else:
                wallet_identity_mismatch_counts[source] = wallet_identity_mismatch_counts.get(source, 0) + 1
                if len(wallet_identity_mismatch_examples) < 10:
                    wallet_identity_mismatch_examples.append(
                        {
                            "source": source,
                            "query_key": row.get("_walletCopyQueryKey"),
                            "expected_wallet": self.spec.normalized_address(),
                            "observed_wallet_fields": identities,
                            "slug": _first(row, "marketSlug", "market_slug", "eventSlug", "slug", default=""),
                            "timestamp": _first(row, "timestamp", "createdAt", "created_at", "time", "date", default=None),
                            "transaction_hash": _first(row, "transactionHash", "transaction_hash", "txHash", default=""),
                        }
                    )
                continue
            row_observed = num(row.get("_walletCopySourceFetchCompletedTs"), observed)
            event = normalize_polymarket_wallet_row(row, spec=self.spec, row_type=row_type, observed_ts=row_observed)
            if event is not None:
                source = _event_source(event)
                normalized_event_counts[source] = normalized_event_counts.get(source, 0) + 1
                existing = events_by_fingerprint.get(event.source_fingerprint)
                if existing is not None:
                    duplicate_trade_rows_dropped += 1
                    preferred = _prefer_event(existing, event)
                    events_by_fingerprint[event.source_fingerprint] = preferred
                    events_by_semantic_key[_semantic_event_key(preferred)] = preferred
                    continue
                semantic_key = _semantic_event_key(event)
                semantic_existing = events_by_semantic_key.get(semantic_key)
                if semantic_existing is not None and (
                    not event.transaction_hash
                    or not semantic_existing.transaction_hash
                    or _event_source(event) != _event_source(semantic_existing)
                ):
                    duplicate_trade_rows_dropped += 1
                    preferred = _prefer_event(semantic_existing, event)
                    events_by_semantic_key[semantic_key] = preferred
                    events_by_fingerprint[semantic_existing.source_fingerprint] = preferred
                    events_by_fingerprint[event.source_fingerprint] = preferred
                    continue
                events_by_fingerprint[event.source_fingerprint] = event
                events_by_semantic_key[semantic_key] = event
        events = {event.source_fingerprint: event for event in events_by_semantic_key.values()}
        if api_error is None and source_errors:
            api_error = source_errors[0]
        normalized_events = list(events.values())
        normalized_trade_events = [event for event in normalized_events if event.row_type == "trade"]
        normalized_activity_events = [event for event in normalized_events if event.row_type == "activity"]
        source_freshness_by_source = _source_freshness_report(
            normalized_events,
            reference_ts=fetch_completed,
        )
        raw_trade_rows_by_query_param = {
            source.split(":", 1)[1]: count for source, count in source_row_counts.items() if source.startswith("trade:")
        }
        normalized_trade_events_by_query_param: dict[str, int] = {}
        for event in normalized_trade_events:
            query_key = str(event.raw.get("_walletCopyQueryKey") or "unknown")
            normalized_trade_events_by_query_param[query_key] = normalized_trade_events_by_query_param.get(query_key, 0) + 1
        wallet_identity_mismatch_admission_counts = dict(wallet_identity_mismatch_counts)
        wallet_identity_mismatch_quarantined_counts: dict[str, int] = {}
        supplemental_proxy_source = _source_key("trade", "proxyWallet")
        supplemental_proxy_mismatches = int(wallet_identity_mismatch_counts.get(supplemental_proxy_source) or 0)
        authoritative_user_source_coverage = (
            int(normalized_trade_events_by_query_param.get("user") or 0) > 0
            or bool(normalized_activity_events)
            or int(wallet_identity_matched_counts.get(_source_key("trade", "user")) or 0) > 0
            or int(wallet_identity_matched_counts.get(_source_key("activity", "user")) or 0) > 0
        )
        if (
            supplemental_proxy_mismatches > 0
            and authoritative_user_source_coverage
        ):
            wallet_identity_mismatch_quarantined_counts[supplemental_proxy_source] = supplemental_proxy_mismatches
            wallet_identity_mismatch_admission_counts.pop(supplemental_proxy_source, None)
        filtered_trade_rows_by_query_param = {
            query_key: max(0, int(raw_count) - int(normalized_trade_events_by_query_param.get(query_key, 0)))
            for query_key, raw_count in raw_trade_rows_by_query_param.items()
        }
        blockers: list[str] = []
        if not normalized_events:
            blockers.append("no_normalized_btc_5m_events_after_user_proxy_queries")
        if source_errors:
            blockers.append("partial_data_api_source_error")
        if wallet_identity_missing_counts:
            blockers.append("wallet_identity_missing_filtered")
        if wallet_identity_mismatch_admission_counts:
            blockers.append("wallet_identity_mismatch_filtered")
        if wallet_identity_mismatch_quarantined_counts:
            blockers.append("supplemental_proxy_wallet_source_quarantined")
        if normalized_events and not full_pagination and stop_reason == "page_limit":
            blockers.append("history_page_limit_reached")
        ingest_status = "PASS"
        if blockers:
            ingest_status = "WATCH" if not normalized_events else "PARTIAL"
        source_route_status_by_source = {
            source: str(report.get("status") or "")
            for source, report in source_route_reports_by_source.items()
            if isinstance(report, dict)
        }
        source_route_hosts_by_source = {
            source: {
                "host": report.get("host"),
                "original_host": report.get("original_host"),
                "routed_host": report.get("routed_host"),
            }
            for source, report in source_route_reports_by_source.items()
            if isinstance(report, dict)
        }
        source_base_override_counts: dict[str, int] = {}
        for report in source_route_reports_by_source.values():
            if not isinstance(report, dict) or not report.get("source_base_override_configured"):
                continue
            env_var = str(report.get("source_base_override_env_var") or "unknown")
            source_base_override_counts[env_var] = source_base_override_counts.get(env_var, 0) + 1
        object.__setattr__(
            self,
            "last_fetch_report",
            {
                "limit": page_size,
                "requested_pages": int(pages),
                "parallel_sources": bool(parallel_sources),
                "timeout_connect_s": min(10.0, max(0.001, float(self.connect_timeout_s))),
                "timeout_read_s": min(30.0, max(0.001, float(self.read_timeout_s))),
                "bounded_retry_count": max(0, int(self.retries) - 1),
                "full_pagination": full_pagination,
                "pages_read": pages_read,
                "last_offset": last_offset,
                "stop_reason": stop_reason,
                "raw_rows": len(rows),
                "raw_rows_by_source": source_row_counts,
                "btc_5m_like_rows_by_source": source_btc_5m_like_counts,
                "normalized_events_by_source": normalized_event_counts,
                "source_freshness_by_source": source_freshness_by_source,
                "fresh_buy_rows_le_10s_by_source": {
                    source: int(source_report.get("fresh_buy_rows_le_10s") or 0)
                    for source, source_report in source_freshness_by_source.items()
                },
                "freshest_buy_lag_s_by_source": {
                    source: source_report.get("latest_buy_event_lag_s")
                    for source, source_report in source_freshness_by_source.items()
                },
                "wallet_identity_matched_rows_by_source": wallet_identity_matched_counts,
                "wallet_identity_missing_rows_by_source": wallet_identity_missing_counts,
                "wallet_identity_mismatch_rows_by_source": wallet_identity_mismatch_counts,
                "wallet_identity_mismatch_rows_admission_relevant_by_source": wallet_identity_mismatch_admission_counts,
                "wallet_identity_mismatch_rows_quarantined_by_source": wallet_identity_mismatch_quarantined_counts,
                "wallet_identity_missing_examples": wallet_identity_missing_examples,
                "wallet_identity_mismatch_examples": wallet_identity_mismatch_examples,
                "trade_query_params_requested": [query_key for query_key, _ in trade_sources],
                "trade_query_keys": [query_key for query_key, _ in trade_sources],
                "available_trade_query_keys": [query_key for query_key, _ in all_trade_sources],
                "raw_trade_rows_by_query_param": raw_trade_rows_by_query_param,
                "normalized_trade_events_by_query_param": normalized_trade_events_by_query_param,
                "filtered_trade_rows_by_query_param": filtered_trade_rows_by_query_param,
                "wallet_identity_matched_rows_by_query_param": {
                    source.split(":", 1)[1]: count
                    for source, count in wallet_identity_matched_counts.items()
                    if source.startswith("trade:")
                },
                "wallet_identity_mismatch_rows_by_query_param": {
                    source.split(":", 1)[1]: count
                    for source, count in wallet_identity_mismatch_counts.items()
                    if source.startswith("trade:")
                },
                "wallet_identity_mismatch_rows_admission_relevant_by_query_param": {
                    source.split(":", 1)[1]: count
                    for source, count in wallet_identity_mismatch_admission_counts.items()
                    if source.startswith("trade:")
                },
                "wallet_identity_mismatch_rows_quarantined_by_query_param": {
                    source.split(":", 1)[1]: count
                    for source, count in wallet_identity_mismatch_quarantined_counts.items()
                    if source.startswith("trade:")
                },
                "wallet_identity_missing_rows_by_query_param": {
                    source.split(":", 1)[1]: count
                    for source, count in wallet_identity_missing_counts.items()
                    if source.startswith("trade:")
                },
                "duplicate_trade_rows_dropped": duplicate_trade_rows_dropped,
                "activity_query_params_requested": ["user"] if include_activity else [],
                "raw_activity_rows": source_row_counts.get(_source_key("activity", "user"), 0),
                "normalized_events": len(normalized_events),
                "normalized_trade_events": len(normalized_trade_events),
                "normalized_activity_events": len(normalized_activity_events),
                "ingest_status": ingest_status,
                "blockers": blockers,
                "source_stop_reasons": source_stop_reasons,
                "source_route_reports_by_source": source_route_reports_by_source,
                "source_route_status_by_source": source_route_status_by_source,
                "source_route_hosts_by_source": source_route_hosts_by_source,
                "source_base_override_counts": source_base_override_counts,
                "observed_ts": round(observed, 6),
                "fetch_started_ts": round(fetch_started, 6),
                "fetch_completed_ts": round(fetch_completed, 6),
                "fetch_duration_s": round(max(0.0, fetch_completed - fetch_started), 6),
                "source_fetch_duration_s_by_source": source_fetch_duration_s_by_source,
                "api_error": api_error,
                "api_errors": source_errors,
                "data_api_skip_rows": source_errors,
            },
        )
        return sorted(normalized_events, key=lambda event: (event.event_ts or 0.0, event.event_id))
