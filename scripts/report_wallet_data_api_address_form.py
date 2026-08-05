#!/usr/bin/env python3
"""Report which Polymarket Data API trade address form is authoritative.

Flow stage: OBSERVE/ROTATE/LEARN. This probes `/trades` with both `user=` and
`proxyWallet=` for active and rotation-candidate wallets, records identity
matches, and writes evidence only. It never submits orders or mutates the live
guard.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.ingest import (  # noqa: E402
    btc_5m_like,
    normalize_polymarket_wallet_row,
    wallet_identity_status,
)
from src.wallet_copy.models import WalletSpec, parse_ts, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_LIVE_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_BREADTH_DISPOSITIONS = "data/research/wallet_copy_breadth_dispositions.json"
DEFAULT_OUTPUT = "data/research/wallet_data_api_address_form_latest.json"
DATA_API_SOURCE_BASE_ENV_VARS = ("POLYMARKET_DATA_API_BASE_URL",)
TRADE_QUERY_KEYS = ("user", "proxyWallet")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-guard-state", default=DEFAULT_LIVE_GUARD_STATE)
    parser.add_argument("--queue", default=DEFAULT_QUEUE)
    parser.add_argument("--breadth-dispositions", default=DEFAULT_BREADTH_DISPOSITIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--include-wallet", action="append", default=[])
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--timeout-s", type=float, default=8.0)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--direct-data-api", action="store_true", default=True)
    parser.add_argument("--no-direct-data-api", dest="direct_data_api", action="store_false")
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _split_csv(values: list[str] | tuple[str, ...]) -> list[str]:
    out: list[str] = []
    for value in values:
        out.extend(item.strip() for item in str(value or "").split(",") if item.strip())
    return out


def _iso_from_ts(value: float | int | None) -> str | None:
    if value is None:
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).isoformat().replace("+00:00", "Z")


def _age_h(reference_ts: float, event_ts: float | int | None) -> float | None:
    if event_ts is None:
        return None
    try:
        return round((float(reference_ts) - float(event_ts)) / 3600.0, 6)
    except (TypeError, ValueError):
        return None


def _row_ts(row: dict[str, Any]) -> float | None:
    for key in ("timestamp", "createdAt", "created_at", "time", "date"):
        ts = parse_ts(row.get(key))
        if ts is not None:
            return ts
    return None


def _add_wallet(
    rows: dict[str, dict[str, Any]],
    wallet: Any,
    *,
    source: str,
    status: str = "",
    candidate_id: str = "",
) -> None:
    normalized = _norm_wallet(wallet)
    if not normalized:
        return
    row = rows.setdefault(normalized, {"wallet": normalized, "sources": [], "statuses": [], "candidate_ids": []})
    if source and source not in row["sources"]:
        row["sources"].append(source)
    if status and status not in row["statuses"]:
        row["statuses"].append(status)
    if candidate_id and candidate_id not in row["candidate_ids"]:
        row["candidate_ids"].append(candidate_id)


def select_wallets(
    *,
    live_guard_state: dict[str, Any],
    breadth_dispositions: dict[str, Any],
    queue: dict[str, Any],
    include_wallets: list[str],
) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    active_set = live_guard_state.get("active_set") if isinstance(live_guard_state.get("active_set"), dict) else {}
    for member in active_set.get("members") or []:
        if not isinstance(member, dict):
            continue
        _add_wallet(
            rows,
            member.get("source_wallet") or member.get("wallet"),
            source="live_guard_active_set",
            status=str(member.get("status") or ""),
            candidate_id=str(member.get("candidate_id") or ""),
        )
    for row in breadth_dispositions.get("wallets") or []:
        if not isinstance(row, dict):
            continue
        _add_wallet(
            rows,
            row.get("wallet"),
            source="breadth_disposition",
            status=str(row.get("status") or ""),
            candidate_id=str(row.get("candidate_id") or ""),
        )
    for row in queue.get("ranked_members") or []:
        if not isinstance(row, dict):
            continue
        disposition = row.get("breadth_disposition") if isinstance(row.get("breadth_disposition"), dict) else {}
        if not disposition:
            continue
        _add_wallet(
            rows,
            row.get("wallet"),
            source="full_pool_queue_breadth_row",
            status=str(disposition.get("status") or ""),
            candidate_id=str(row.get("candidate_id") or disposition.get("candidate_id") or ""),
        )
    for wallet in _split_csv(include_wallets):
        _add_wallet(rows, wallet, source="explicit_include")
    return list(rows.values())


def _fetch_query_rows(
    wallet: str,
    query_key: str,
    *,
    pages: int,
    limit: int,
    timeout_s: float,
    retries: int,
) -> dict[str, Any]:
    from src.wallet_copy.ingest import WalletHistoryClient

    client = WalletHistoryClient(
        WalletSpec(name=f"address_form_{wallet[-10:]}", address=wallet),
        timeout_s=float(timeout_s),
        retries=int(retries),
    )
    fetcher = client.fetch_trades if query_key == "user" else client.fetch_proxy_wallet_trades
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    started = time.time()
    for page in range(max(1, int(pages))):
        offset = page * int(limit)
        try:
            page_rows = fetcher(limit=int(limit), offset=offset)
        except requests.RequestException as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
            break
        rows.extend(page_rows)
        if len(page_rows) < int(limit):
            break
    return {
        "query_key": query_key,
        "rows": rows,
        "duration_s": round(max(0.0, time.time() - started), 6),
        "errors": errors,
        "route_report": client.last_route_report if isinstance(client.last_route_report, dict) else {},
    }


def summarize_query_rows(
    *,
    wallet: str,
    query_key: str,
    rows: list[dict[str, Any]],
    observed_ts: float,
    route_report: dict[str, Any] | None = None,
    duration_s: float = 0.0,
    errors: list[str] | None = None,
    requested_pages: int = 1,
    limit: int = 500,
) -> dict[str, Any]:
    spec = WalletSpec(name=f"address_form_{wallet[-10:]}", address=wallet)
    identity_counts = {"MATCHED": 0, "MISSING": 0, "MISMATCH": 0}
    identity_examples: list[dict[str, Any]] = []
    matched_trade_ts: list[float] = []
    matched_btc5m_events = []
    observed_identity_values: dict[str, list[str]] = {}
    btc5m_like_rows = 0
    for row in rows:
        if btc_5m_like(row):
            btc5m_like_rows += 1
        status, identities = wallet_identity_status(row, spec)
        identity_counts[status] = identity_counts.get(status, 0) + 1
        for key, value in identities.items():
            values = observed_identity_values.setdefault(key, [])
            if value not in values and len(values) < 5:
                values.append(value)
        if status != "MATCHED":
            if len(identity_examples) < 10:
                identity_examples.append(
                    {
                        "status": status,
                        "observed_wallet_fields": identities,
                        "timestamp": row.get("timestamp")
                        or row.get("createdAt")
                        or row.get("created_at")
                        or row.get("time"),
                        "market_slug": row.get("marketSlug") or row.get("market_slug") or row.get("slug"),
                        "transaction_hash": row.get("transactionHash") or row.get("transaction_hash"),
                    }
                )
            continue
        ts = _row_ts(row)
        if ts is not None:
            matched_trade_ts.append(float(ts))
        event = normalize_polymarket_wallet_row(row, spec=spec, row_type="trade", observed_ts=observed_ts)
        if event is not None:
            matched_btc5m_events.append(event)

    latest_matched_trade_ts = max(matched_trade_ts, default=None)
    latest_btc5m_trade_ts = max((event.event_ts or 0.0 for event in matched_btc5m_events), default=0.0) or None
    latest_btc5m_buy_ts = (
        max((event.event_ts or 0.0 for event in matched_btc5m_events if event.is_buy), default=0.0)
        or None
    )
    raw_rows = len(rows)
    matched_rows = int(identity_counts.get("MATCHED") or 0)
    mismatched_rows = int(identity_counts.get("MISMATCH") or 0)
    missing_rows = int(identity_counts.get("MISSING") or 0)
    route = route_report if isinstance(route_report, dict) else {}
    return {
        "query_key": query_key,
        "endpoint": "/trades",
        "status": "PASS" if not errors else "ERROR",
        "duration_s": round(max(0.0, float(duration_s)), 6),
        "raw_rows": raw_rows,
        "remote_rows_saturated": raw_rows >= max(1, int(requested_pages)) * int(limit),
        "btc5m_like_raw_rows": btc5m_like_rows,
        "wallet_identity_matched_rows": matched_rows,
        "wallet_identity_missing_rows": missing_rows,
        "wallet_identity_mismatch_rows": mismatched_rows,
        "identity_match_rate_pct": round(100.0 * matched_rows / raw_rows, 6) if raw_rows else None,
        "observed_identity_values": observed_identity_values,
        "identity_examples": identity_examples,
        "normalized_btc5m_trade_events": len(matched_btc5m_events),
        "normalized_btc5m_buy_events": sum(1 for event in matched_btc5m_events if event.is_buy),
        "latest_matched_trade_ts": latest_matched_trade_ts,
        "latest_matched_trade_iso": _iso_from_ts(latest_matched_trade_ts),
        "latest_matched_trade_age_h": _age_h(observed_ts, latest_matched_trade_ts),
        "latest_btc5m_trade_ts": latest_btc5m_trade_ts,
        "latest_btc5m_trade_iso": _iso_from_ts(latest_btc5m_trade_ts),
        "latest_btc5m_trade_age_h": _age_h(observed_ts, latest_btc5m_trade_ts),
        "latest_btc5m_buy_ts": latest_btc5m_buy_ts,
        "latest_btc5m_buy_iso": _iso_from_ts(latest_btc5m_buy_ts),
        "latest_btc5m_buy_age_h": _age_h(observed_ts, latest_btc5m_buy_ts),
        "appears_unfiltered_or_global": bool(raw_rows > 0 and matched_rows == 0 and mismatched_rows > 0),
        "errors": errors or [],
        "route_status": route.get("status"),
        "route_class": route.get("route_class"),
        "route_host": route.get("host"),
        "route_report": route,
    }


def choose_authoritative_key(query_reports: list[dict[str, Any]]) -> dict[str, Any]:
    by_key = {str(row.get("query_key") or ""): row for row in query_reports}

    def tie_score(row: dict[str, Any], ts_key: str) -> tuple[float, int, int]:
        ts = float(row.get(ts_key) or 0.0)
        user_preference = 1 if row.get("query_key") == "user" else 0
        matched = int(row.get("wallet_identity_matched_rows") or 0)
        return ts, user_preference, matched

    btc5m = [
        row
        for row in query_reports
        if int(row.get("normalized_btc5m_trade_events") or 0) > 0
        and int(row.get("wallet_identity_matched_rows") or 0) > 0
    ]
    if btc5m:
        chosen = max(btc5m, key=lambda row: tie_score(row, "latest_btc5m_trade_ts"))
        basis = "freshest_matching_btc5m_trade"
        last_ts = chosen.get("latest_btc5m_trade_ts")
        last_age_h = chosen.get("latest_btc5m_trade_age_h")
    else:
        matched = [
            row
            for row in query_reports
            if int(row.get("wallet_identity_matched_rows") or 0) > 0
            and row.get("latest_matched_trade_ts") is not None
        ]
        if matched:
            chosen = max(matched, key=lambda row: tie_score(row, "latest_matched_trade_ts"))
            basis = "freshest_matching_trade"
            last_ts = chosen.get("latest_matched_trade_ts")
            last_age_h = chosen.get("latest_matched_trade_age_h")
        else:
            chosen = by_key.get("user") or (query_reports[0] if query_reports else {})
            basis = "user_default_no_matching_rows"
            last_ts = chosen.get("latest_matched_trade_ts")
            last_age_h = chosen.get("latest_matched_trade_age_h")

    proxy = by_key.get("proxyWallet") or {}
    user = by_key.get("user") or {}
    proxy_status = "UNFILTERED_OR_GLOBAL" if proxy.get("appears_unfiltered_or_global") else "MATCHING_OR_EMPTY"
    return {
        "recommended_query_key": chosen.get("query_key"),
        "selection_basis": basis,
        "last_trade_ts": last_ts,
        "last_trade_iso": _iso_from_ts(last_ts),
        "last_trade_age_h": last_age_h,
        "user_matched_rows": int(user.get("wallet_identity_matched_rows") or 0),
        "proxyWallet_matched_rows": int(proxy.get("wallet_identity_matched_rows") or 0),
        "proxyWallet_mismatch_rows": int(proxy.get("wallet_identity_mismatch_rows") or 0),
        "proxyWallet_route_status": proxy_status,
        "user_last_trade_ts": user.get("latest_btc5m_trade_ts")
        or user.get("latest_matched_trade_ts"),
        "user_last_trade_iso": _iso_from_ts(
            user.get("latest_btc5m_trade_ts") or user.get("latest_matched_trade_ts")
        ),
        "user_last_trade_age_h": user.get("latest_btc5m_trade_age_h")
        if user.get("latest_btc5m_trade_age_h") is not None
        else user.get("latest_matched_trade_age_h"),
        "freshest_btc5m_query_key": (
            max(btc5m, key=lambda row: tie_score(row, "latest_btc5m_trade_ts")).get("query_key")
            if btc5m
            else None
        ),
        "freshest_matching_trade_query_key": chosen.get("query_key") if basis != "user_default_no_matching_rows" else None,
        "user_only_hot_path_supported": bool(
            int(user.get("wallet_identity_matched_rows") or 0) > 0
        ),
    }


def _queue_rows_by_wallet(queue: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        wallet: row
        for row in queue.get("ranked_members") or []
        if isinstance(row, dict) and (wallet := _norm_wallet(row.get("wallet")))
    }


def freshness_context(
    *,
    wallet: str,
    queue_row: dict[str, Any] | None,
    address_selection: dict[str, Any],
) -> dict[str, Any]:
    fresh_flow_rank = (
        queue_row.get("fresh_flow_rank")
        if isinstance(queue_row, dict) and isinstance(queue_row.get("fresh_flow_rank"), dict)
        else {}
    )
    source_reported_age = fresh_flow_rank.get("source_reported_latest_trade_age_h")
    computed_age = fresh_flow_rank.get("latest_trade_age_h")
    selected_age = address_selection.get("last_trade_age_h")
    contradiction = False
    if source_reported_age is not None and computed_age is not None:
        try:
            contradiction = abs(float(computed_age) - float(source_reported_age)) > 1.0
        except (TypeError, ValueError):
            contradiction = False
    return {
        "wallet": wallet,
        "queue_latest_btc5m_trade_ts": fresh_flow_rank.get("latest_btc5m_trade_ts"),
        "queue_computed_latest_trade_age_h": computed_age,
        "queue_source_reported_latest_trade_age_h": source_reported_age,
        "address_form_latest_trade_age_h": selected_age,
        "freshness_age_contradiction_detected": contradiction,
        "resolution_rule": (
            "trust direct address-form recomputation at report time; do not use stale embedded source_reported age"
            if contradiction
            else "no embedded source/computed freshness contradiction detected"
        ),
    }


def build_report(
    *,
    live_guard_state: dict[str, Any],
    breadth_dispositions: dict[str, Any],
    queue: dict[str, Any],
    include_wallets: list[str],
    pages: int,
    limit: int,
    timeout_s: float,
    retries: int,
    fetch_query_rows: Any = _fetch_query_rows,
) -> dict[str, Any]:
    observed_ts = time.time()
    generated_at = utc_now_iso()
    wallets = select_wallets(
        live_guard_state=live_guard_state,
        breadth_dispositions=breadth_dispositions,
        queue=queue,
        include_wallets=include_wallets,
    )
    queue_by_wallet = _queue_rows_by_wallet(queue)
    rows: list[dict[str, Any]] = []
    for wallet_meta in wallets:
        wallet = str(wallet_meta["wallet"])
        query_reports: list[dict[str, Any]] = []
        for query_key in TRADE_QUERY_KEYS:
            fetched = fetch_query_rows(
                wallet,
                query_key,
                pages=pages,
                limit=limit,
                timeout_s=timeout_s,
                retries=retries,
            )
            query_reports.append(
                summarize_query_rows(
                    wallet=wallet,
                    query_key=query_key,
                    rows=fetched.get("rows") or [],
                    observed_ts=observed_ts,
                    route_report=fetched.get("route_report") if isinstance(fetched.get("route_report"), dict) else {},
                    duration_s=float(fetched.get("duration_s") or 0.0),
                    errors=[str(item) for item in fetched.get("errors") or []],
                    requested_pages=pages,
                    limit=limit,
                )
            )
        selection = choose_authoritative_key(query_reports)
        rows.append(
            {
                **wallet_meta,
                "query_reports": query_reports,
                "address_selection": selection,
                "freshness_context": freshness_context(
                    wallet=wallet,
                    queue_row=queue_by_wallet.get(wallet),
                    address_selection=selection,
                ),
            }
        )
    proxy_global = sum(
        1
        for row in rows
        for report in row.get("query_reports") or []
        if report.get("query_key") == "proxyWallet" and report.get("appears_unfiltered_or_global")
    )
    user_wins = sum(1 for row in rows if row.get("address_selection", {}).get("recommended_query_key") == "user")
    proxy_wins = sum(1 for row in rows if row.get("address_selection", {}).get("recommended_query_key") == "proxyWallet")
    contradictions = [
        row.get("wallet")
        for row in rows
        if row.get("freshness_context", {}).get("freshness_age_contradiction_detected")
    ]
    return {
        "schema_version": 1,
        "kind": "wallet_data_api_address_form_map",
        "flow_stage": "OBSERVE/ROTATE/LEARN",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": generated_at,
        "observed_ts": observed_ts,
        "criteria": {
            "endpoint": "/trades",
            "query_keys": list(TRADE_QUERY_KEYS),
            "pages": int(pages),
            "limit": int(limit),
            "timeout_s": float(timeout_s),
            "retries": int(retries),
            "selection_rule": "freshest identity-matched BTC-5m trade, else freshest identity-matched trade, user tie-break",
        },
        "summary": {
            "wallets_probed": len(rows),
            "user_recommended": user_wins,
            "proxyWallet_recommended": proxy_wins,
            "proxyWallet_unfiltered_or_global": proxy_global,
            "freshness_age_contradictions": len(contradictions),
            "freshness_age_contradiction_wallets": contradictions,
        },
        "rows": rows,
    }


def main() -> int:
    args = parse_args()
    original_source_base_overrides = {key: os.environ.get(key) for key in DATA_API_SOURCE_BASE_ENV_VARS}
    if bool(args.direct_data_api):
        for key in DATA_API_SOURCE_BASE_ENV_VARS:
            os.environ.pop(key, None)
    try:
        report = build_report(
            live_guard_state=load_json(args.live_guard_state, default={}) or {},
            breadth_dispositions=load_json(args.breadth_dispositions, default={}) or {},
            queue=load_json(args.queue, default={}) or {},
            include_wallets=[str(item) for item in args.include_wallet or []],
            pages=int(args.pages),
            limit=int(args.limit),
            timeout_s=float(args.timeout_s),
            retries=int(args.retries),
        )
    finally:
        for key, value in original_source_base_overrides.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    report["criteria"]["direct_data_api"] = bool(args.direct_data_api)
    report["criteria"]["data_api_base_override_cleared"] = bool(
        args.direct_data_api and any(original_source_base_overrides.values())
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
