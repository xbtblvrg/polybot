"""Polymarket crypto leaderboard intake for wallet-copy discovery."""

from __future__ import annotations

import os
import re
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import requests

from src.wallet_copy.http_client import PolymarketHttpClient, PolymarketRouteError, SOURCE_BASE_ENV_BY_HOST
from src.wallet_copy.models import WalletSpec, num, stable_id, utc_now_iso
from src.wallet_copy.registry import DEFAULT_REGISTRY_PATH, load_wallet_registry, save_wallet_registry


LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"
LEADERBOARD_MAX_PAGE_LIMIT = 50
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


class LeaderboardCategoryEmptyPayloadError(ValueError):
    """The API returned its terminal unsupported-category payload."""

    def __init__(self, payload: dict[str, Any]):
        self.payload = dict(payload)
        detail = str(payload.get("error") or payload.get("message") or "category has no wallet rows")
        super().__init__(f"leaderboard category has no wallet rows: {detail}")


@dataclass(frozen=True)
class LeaderboardRow:
    period: str
    rank: int
    proxy_wallet: str
    user_name: str = ""
    x_username: str = ""
    vol: float = 0.0
    pnl: float = 0.0
    category: str = "CRYPTO"
    raw: dict[str, Any] | None = None
    route_report: dict[str, Any] | None = None

    def normalized_address(self) -> str:
        return self.proxy_wallet.lower()

    def asdict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["proxy_wallet"] = self.normalized_address()
        payload["raw"] = self.raw or {}
        return payload


@dataclass(frozen=True)
class LeaderboardWalletCandidate:
    address: str
    name: str
    categories: tuple[str, ...]
    periods: tuple[str, ...]
    ranks: dict[str, int]
    pnl_by_period: dict[str, float]
    vol_by_period: dict[str, float]
    user_name: str = ""
    x_username: str = ""
    tags: tuple[str, ...] = ()
    notes: str = ""

    def normalized_address(self) -> str:
        return self.address.lower()

    def asdict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["address"] = self.normalized_address()
        return payload


def normalize_leaderboard_row(
    row: dict[str, Any],
    *,
    period: str,
    fallback_rank: int,
    category: str = "CRYPTO",
    route_report: dict[str, Any] | None = None,
) -> LeaderboardRow | None:
    address = str(row.get("proxyWallet") or row.get("proxy_wallet") or "").strip().lower()
    if not ADDRESS_RE.match(address):
        return None
    rank = int(num(row.get("rank"), fallback_rank) or fallback_rank)
    return LeaderboardRow(
        category=str(category or "").upper(),
        period=str(period or "").upper(),
        rank=rank,
        proxy_wallet=address,
        user_name=str(row.get("userName") or row.get("username") or ""),
        x_username=str(row.get("xUsername") or row.get("x_username") or ""),
        vol=num(row.get("vol")),
        pnl=num(row.get("pnl")),
        raw=dict(row),
        route_report=dict(route_report or {}),
    )


@contextmanager
def _cleared_source_base_override(host: str):
    env_var = SOURCE_BASE_ENV_BY_HOST.get(str(host or "").lower(), "")
    if not env_var:
        yield ""
        return
    previous = os.environ.get(env_var)
    os.environ[env_var] = ""
    try:
        yield env_var
    finally:
        if previous is None:
            os.environ.pop(env_var, None)
        else:
            os.environ[env_var] = previous


def _compact_route_report(report: dict[str, Any] | None) -> dict[str, Any]:
    raw = report if isinstance(report, dict) else {}
    return {
        "status": raw.get("status"),
        "route_class": raw.get("route_class"),
        "host": raw.get("host"),
        "original_host": raw.get("original_host"),
        "routed_host": raw.get("routed_host"),
        "source_base_override_configured": bool(raw.get("source_base_override_configured")),
        "source_base_override_env_var": raw.get("source_base_override_env_var"),
        "http_statuses": [
            attempt.get("http_status")
            for attempt in (raw.get("attempts") or [])
            if isinstance(attempt, dict) and attempt.get("http_status") is not None
        ],
        "route_report_id": raw.get("route_report_id"),
    }


def _combine_direct_fallback_report(
    *,
    primary_report: dict[str, Any] | None,
    direct_report: dict[str, Any] | None,
    suppressed_env_var: str,
) -> dict[str, Any]:
    return {
        "status": "PASS",
        "fallback_source": "direct_data_api_after_primary_failure",
        "suppressed_env_var": suppressed_env_var,
        "primary_route_report": _compact_route_report(primary_report),
        "direct_route_report": _compact_route_report(direct_report),
    }


def _request_leaderboard_json(
    *,
    client: PolymarketHttpClient,
    base_url: str,
    params: dict[str, Any],
) -> tuple[Any, dict[str, Any]]:
    response = client.request("GET", base_url, params=params, request_role="leaderboard_fetch")
    route_report = getattr(response, "wallet_copy_route_report", {})
    if (
        bool((route_report or {}).get("source_base_override_configured"))
        and int(getattr(response, "status_code", 0) or 0) >= 500
    ):
        with _cleared_source_base_override("data-api.polymarket.com") as suppressed_env_var:
            direct_response = client.request("GET", base_url, params=params, request_role="leaderboard_fetch_direct_fallback")
        direct_report = getattr(direct_response, "wallet_copy_route_report", {})
        direct_response.raise_for_status()
        return direct_response.json(), _combine_direct_fallback_report(
            primary_report=route_report,
            direct_report=direct_report,
            suppressed_env_var=suppressed_env_var,
        )
    response.raise_for_status()
    return response.json(), _compact_route_report(route_report)


def fetch_leaderboard(
    *,
    category: str = "CRYPTO",
    period: str = "WEEK",
    order_by: str = "PNL",
    limit: int = 50,
    offset: int = 0,
    base_url: str = LEADERBOARD_URL,
    timeout_s: float = 20.0,
    retries: int = 3,
) -> list[LeaderboardRow]:
    """Fetch and normalize one Polymarket leaderboard page."""

    limit = min(LEADERBOARD_MAX_PAGE_LIMIT, max(1, int(limit)))
    params = {
        "category": str(category).upper(),
        "timePeriod": str(period).upper(),
        "orderBy": str(order_by).upper(),
        "limit": limit,
        "offset": int(offset),
    }
    client = PolymarketHttpClient(
        timeout_s=timeout_s,
        retries=retries,
        user_agent="wallet-copy-leaderboard/1.0",
    )
    try:
        payload, route_report = _request_leaderboard_json(client=client, base_url=base_url, params=params)
    except PolymarketRouteError as exc:
        route_report = getattr(exc, "route_report", {})
        if not bool((route_report or {}).get("source_base_override_configured")):
            raise
        with _cleared_source_base_override("data-api.polymarket.com") as suppressed_env_var:
            direct_payload, direct_report = client.get_json(
                base_url,
                params=params,
                request_role="leaderboard_fetch_direct_fallback",
            )
        payload = direct_payload
        route_report = _combine_direct_fallback_report(
            primary_report=route_report,
            direct_report=direct_report,
            suppressed_env_var=suppressed_env_var,
        )
    if isinstance(payload, dict) and set(payload).issubset({"error", "message"}) and str(
        payload.get("error") or payload.get("message") or ""
    ).strip().lower() == "invalid category parameter":
        raise LeaderboardCategoryEmptyPayloadError(payload)
    if not isinstance(payload, list):
        raise ValueError(f"unexpected leaderboard payload type: {type(payload).__name__}")
    rows: list[LeaderboardRow] = []
    for idx, raw_row in enumerate(payload, start=offset + 1):
        if isinstance(raw_row, dict):
            normalized = normalize_leaderboard_row(
                raw_row,
                period=period,
                fallback_rank=idx,
                category=category,
                route_report=route_report,
            )
            if normalized is not None:
                rows.append(normalized)
    return rows


def fetch_crypto_top_wallets(
    *,
    periods: Iterable[str] = ("WEEK", "MONTH"),
    category: str = "CRYPTO",
    order_by: str = "PNL",
    limit: int = 50,
    pages: int = 1,
    max_pages: int = 20,
    base_url: str = LEADERBOARD_URL,
    timeout_s: float = 20.0,
    retries: int = 3,
    max_wall_runtime_s: float = 0.0,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> list[LeaderboardRow]:
    rows: list[LeaderboardRow] = []
    limit = min(LEADERBOARD_MAX_PAGE_LIMIT, max(1, int(limit)))
    overall_started = time.time()
    max_wall_runtime_s = max(0.0, float(max_wall_runtime_s or 0.0))
    for period in periods:
        period_rows = 0
        requested_pages = int(pages)
        page_cap = max(1, int(max_pages))
        if requested_pages <= 0:
            requested_pages = page_cap
        else:
            requested_pages = min(max(1, requested_pages), page_cap)
        for page in range(requested_pages):
            offset = page * int(limit)
            event_base = {
                "flow_stage": "DISCOVER",
                "category": str(category).upper(),
                "period": str(period).upper(),
                "page_index": page,
                "requested_pages": requested_pages,
                "offset": offset,
                "limit": int(limit),
                "timeout_s": float(timeout_s),
                "retries": max(1, int(retries)),
                "max_wall_runtime_s": max_wall_runtime_s,
            }
            elapsed_s = time.time() - overall_started
            if max_wall_runtime_s > 0.0 and elapsed_s >= max_wall_runtime_s:
                if progress_callback is not None:
                    progress_callback(
                        {
                            "status": "PAGE_BUDGET_EXHAUSTED",
                            "ts": utc_now_iso(),
                            "duration_s": round(elapsed_s, 6),
                            "rows_total_before_page": len(rows),
                            "period_rows_before_page": period_rows,
                            **event_base,
                        }
                    )
                return rows
            if progress_callback is not None:
                progress_callback({"status": "PAGE_STARTED", "ts": utc_now_iso(), **event_base})
            started = time.time()
            try:
                page_rows = fetch_leaderboard(
                    category=category,
                    period=str(period).upper(),
                    order_by=order_by,
                    limit=limit,
                    offset=offset,
                    base_url=base_url,
                    timeout_s=timeout_s,
                    retries=retries,
                )
            except Exception as exc:
                if progress_callback is not None:
                    progress_callback(
                        {
                            "status": "PAGE_ERROR",
                            "ts": utc_now_iso(),
                            "duration_s": round(time.time() - started, 6),
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:500],
                            "rows_total_before_page": len(rows),
                            "period_rows_before_page": period_rows,
                            **event_base,
                        }
                    )
                raise
            duration_s = round(time.time() - started, 6)
            page_row_count = len(page_rows)
            if progress_callback is not None:
                progress_callback(
                    {
                        "status": "PAGE_FETCHED",
                        "ts": utc_now_iso(),
                        "duration_s": duration_s,
                        "page_rows": page_row_count,
                        "rows_total_after_page": len(rows) + page_row_count,
                        "period_rows_after_page": period_rows + page_row_count,
                        "empty_page": page_row_count == 0,
                        "partial_page": 0 < page_row_count < int(limit),
                        **event_base,
                    }
                )
            if not page_rows:
                break
            rows.extend(page_rows)
            period_rows += page_row_count
            if len(page_rows) < int(limit):
                break
            elapsed_s = time.time() - overall_started
            if max_wall_runtime_s > 0.0 and elapsed_s >= max_wall_runtime_s:
                if progress_callback is not None:
                    progress_callback(
                        {
                            "status": "PAGE_BUDGET_EXHAUSTED",
                            "ts": utc_now_iso(),
                            "duration_s": round(elapsed_s, 6),
                            "rows_total_before_page": len(rows),
                            "period_rows_before_page": period_rows,
                            **event_base,
                        }
                    )
                return rows
    return rows


def _candidate_name(address: str, categories: tuple[str, ...]) -> str:
    cleaned = address.lower().removeprefix("0x")
    if categories == ("CRYPTO",):
        return f"leaderboard_crypto_{cleaned[:10]}"
    if len(categories) == 1:
        return f"leaderboard_{categories[0].lower()}_{cleaned[:10]}"
    return f"leaderboard_multi_{cleaned[:10]}"


def _candidate_notes(
    categories: tuple[str, ...],
    periods: tuple[str, ...],
    ranks: dict[str, int],
    pnl: dict[str, float],
    vol: dict[str, float],
) -> str:
    chunks = []
    for period in periods:
        chunks.append(
            f"{period} rank #{ranks.get(period)} pnl={pnl.get(period, 0.0):.6f} vol={vol.get(period, 0.0):.6f}"
        )
    return f"Polymarket {','.join(categories)} leaderboard PNL wallet; " + "; ".join(chunks)


def _category_tag(category: str) -> str:
    safe = re.sub(r"[^a-z0-9]+", "_", str(category or "").strip().lower()).strip("_")
    return f"leaderboard_{safe or 'unknown'}"


def _candidate_market_filter(candidate: LeaderboardWalletCandidate) -> str:
    return "btc_5m" if candidate.categories == ("CRYPTO",) else "all"


def _candidate_asset_allowlist(candidate: LeaderboardWalletCandidate) -> tuple[str, ...]:
    return ("BTC",) if _candidate_market_filter(candidate) == "btc_5m" else ()


def build_leaderboard_candidates(rows: Iterable[LeaderboardRow]) -> list[LeaderboardWalletCandidate]:
    grouped: dict[str, list[LeaderboardRow]] = {}
    for row in rows:
        grouped.setdefault(row.normalized_address(), []).append(row)

    candidates: list[LeaderboardWalletCandidate] = []
    for address, wallet_rows in grouped.items():
        ordered_rows = sorted(wallet_rows, key=lambda item: (item.period, item.rank))
        categories = tuple(dict.fromkeys(row.category for row in ordered_rows))
        periods = tuple(dict.fromkeys(row.period for row in ordered_rows))
        ranks = {row.period: row.rank for row in ordered_rows}
        pnl = {row.period: row.pnl for row in ordered_rows}
        vol = {row.period: row.vol for row in ordered_rows}
        market_tags = ("btc_5m",) if categories == ("CRYPTO",) else ("broad_market", "leaderboard_multi_category")
        tags = tuple(
            dict.fromkeys(
                (
                    *(_category_tag(category) for category in categories),
                    "leaderboard_top50",
                    "leaderboard_pnl",
                    *market_tags,
                    "candidate",
                    *(f"leaderboard_{period.lower()}" for period in periods),
                )
            )
        )
        first = ordered_rows[0]
        candidates.append(
            LeaderboardWalletCandidate(
                address=address,
                name=_candidate_name(address, categories),
                categories=categories,
                periods=periods,
                ranks=ranks,
                pnl_by_period=pnl,
                vol_by_period=vol,
                user_name=first.user_name,
                x_username=first.x_username,
                tags=tags,
                notes=_candidate_notes(categories, periods, ranks, pnl, vol),
            )
        )
    return sorted(
        candidates,
        key=lambda item: (
            min(item.ranks.values()) if item.ranks else 999999,
            item.normalized_address(),
        ),
    )


def merge_leaderboard_wallets(
    candidates: list[LeaderboardWalletCandidate],
    *,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
) -> dict[str, Any]:
    """Upsert leaderboard wallets while preserving named operator wallets."""

    existing = load_wallet_registry(registry_path)
    by_address = {spec.normalized_address(): spec for spec in existing}
    added = 0
    updated = 0
    for candidate in candidates:
        address = candidate.normalized_address()
        previous = by_address.get(address)
        if previous is None:
            by_address[address] = WalletSpec(
                name=candidate.name,
                address=address,
                enabled=True,
                market_filter=_candidate_market_filter(candidate),
                asset_allowlist=_candidate_asset_allowlist(candidate),
                tags=candidate.tags,
                notes=candidate.notes,
            )
            added += 1
            continue

        merged_tags = tuple(dict.fromkeys((*previous.tags, *candidate.tags)))
        candidate_note = candidate.notes
        notes = previous.notes or ""
        if candidate_note and candidate_note not in notes:
            notes = f"{notes}\n{candidate_note}".strip()
        market_filter = previous.market_filter or _candidate_market_filter(candidate)
        asset_allowlist = previous.asset_allowlist or _candidate_asset_allowlist(candidate)
        if "leaderboard_multi_category" in set(candidate.tags) and not previous.asset_allowlist:
            market_filter = "all"
            asset_allowlist = ()
        by_address[address] = WalletSpec(
            name=previous.name,
            address=previous.normalized_address(),
            enabled=previous.enabled,
            data_api=previous.data_api,
            market_filter=market_filter,
            asset_allowlist=asset_allowlist,
            tags=merged_tags,
            notes=notes,
        )
        updated += 1
    payload = save_wallet_registry(list(by_address.values()), path=registry_path)
    return {
        "schema_version": 1,
        "kind": "wallet_copy_leaderboard_registry_merge",
        "generated_at": utc_now_iso(),
        "registry_path": str(registry_path),
        "candidate_count": len(candidates),
        "added_wallets": added,
        "updated_wallets": updated,
        "registry_wallet_count": len(payload.get("wallets") or []),
    }


def build_leaderboard_state(
    rows: list[LeaderboardRow],
    candidates: list[LeaderboardWalletCandidate],
    registry_merge: dict[str, Any],
    *,
    category: str = "CRYPTO",
    order_by: str = "PNL",
) -> dict[str, Any]:
    weekly = [candidate for candidate in candidates if "WEEK" in candidate.periods]
    monthly = [candidate for candidate in candidates if "MONTH" in candidate.periods]
    overlap = [candidate for candidate in candidates if {"WEEK", "MONTH"}.issubset(set(candidate.periods))]
    return {
        "schema_version": 1,
        "kind": "wallet_copy_leaderboard_crypto_state",
        "generated_at": utc_now_iso(),
        "state_id": stable_id(
            "wclb",
            {
                "rows": [row.asdict() for row in rows],
                "category": category,
                "order_by": order_by,
            },
        ),
        "paper_only": True,
        "live_orders_allowed": False,
        "source": {
            "data_api": LEADERBOARD_URL,
            "category": category,
            "categories": sorted({row.category for row in rows}),
            "order_by": order_by,
            "periods": sorted({row.period for row in rows}),
            "raw_rows": len(rows),
            "category_counts": {
                category: sum(1 for row in rows if row.category == category)
                for category in sorted({row.category for row in rows})
            },
            "period_counts": {
                period: sum(1 for row in rows if row.period == period)
                for period in sorted({row.period for row in rows})
            },
        },
        "summary": {
            "unique_wallets": len(candidates),
            "weekly_wallets": len(weekly),
            "monthly_wallets": len(monthly),
            "weekly_monthly_overlap_wallets": len(overlap),
        },
        "top_wallets": [candidate.asdict() for candidate in candidates[:20]],
        "candidate_wallets": [candidate.asdict() for candidate in candidates],
        "leaderboard_rows": [row.asdict() for row in rows],
        "registry_merge": registry_merge,
        "required_pipeline": [
            "registry_upsert",
            "history_ingest",
            "exact_copy_paper_replay",
            "sweeper_profile",
            "cross_wallet_research",
            "ml_dataset_export",
            "profit_engine",
            "paper_live_tracker_copy_efficiency",
        ],
        "admission_contract": {
            "research_only_until": [
                "resolved history coverage is deep enough",
                "raw/full baseline is visible",
                "filtered candidate beats raw baseline under unresolved cap",
                "paper lifecycle has CLOB-backed copy-efficiency evidence",
                "live_orders_allowed remains false until separate explicit operator gate",
            ],
        },
    }
