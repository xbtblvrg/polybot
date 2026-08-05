#!/usr/bin/env python3
"""Bounded remote Data API fresh-flow probe for queue admission ranking."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.ingest import WalletHistoryClient, normalize_polymarket_wallet_row  # noqa: E402
from src.wallet_copy.models import WalletSpec, num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_COHORT = "data/research/wallet_market_cohort_replay_latest.json"
DEFAULT_PROBE = "data/research/wallet_copy_corrected_copyability_probe_latest.json"
DEFAULT_OUTPUT = "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json"
DEFAULT_CHECKPOINT = "data/research/queue_remote_dataapi_fresh_flow_probe_checkpoint.json"
DEFAULT_LIVE_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_ACTIVE_SET_OVERLAY = "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_INCLUDE_WALLET = "0x11c058db73b3c3c5322da3caf5e94c41486e34b0"
CENSORED_PAGINATION_CAP_STATUS = "CENSORED_PAGINATION_CAP"
BTC5M_BUY_PRICE_SUBBANDS = {
    "00_below_25": (0.0, 0.25),
    "01a_25_32": (0.25, 0.32),
    "01b_32_40": (0.32, 0.40),
    "01c_40_50": (0.40, 0.5000001),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default=DEFAULT_QUEUE)
    parser.add_argument(
        "--cohort-replay",
        default="",
        help="Optional market-cohort replay whose live_ready_picks are probed directly.",
    )
    parser.add_argument("--cohort-offset", type=int, default=0)
    parser.add_argument("--cohort-limit", type=int, default=0)
    parser.add_argument(
        "--cohort-unseen-first",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Prioritize cohort wallets absent from the durable direct-user-trades checkpoint.",
    )
    parser.add_argument("--probe", default=DEFAULT_PROBE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT)
    parser.add_argument("--live-guard-state", default=DEFAULT_LIVE_GUARD_STATE)
    parser.add_argument("--active-set-overlay", default=DEFAULT_ACTIVE_SET_OVERLAY)
    parser.add_argument("--include-wallet", action="append", default=[])
    parser.add_argument(
        "--include-active-set-wallets",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Refresh live-guard active-set wallets in every run so liveness rows are current.",
    )
    parser.add_argument(
        "--no-default-include-wallet",
        action="store_true",
        help="Probe only the requested queue slice/include wallets; omit the standing second-seat wallet.",
    )
    parser.add_argument("--clearance-limit", type=int, default=8)
    parser.add_argument("--ranked-offset", type=int, default=0)
    parser.add_argument("--ranked-limit", type=int, default=0)
    parser.add_argument("--lookback-hours", type=float, default=24.0)
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--timeout-s", type=float, default=8.0)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--direct-data-api", action="store_true", default=True)
    parser.add_argument("--no-direct-data-api", dest="direct_data_api", action="store_false")
    parser.add_argument("--min-price", type=float, default=0.25)
    parser.add_argument("--max-price", type=float, default=0.50)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _wallets_from_active_set(live_guard_state: dict[str, Any]) -> list[str]:
    active_set = (
        live_guard_state.get("active_set")
        if isinstance(live_guard_state.get("active_set"), dict)
        else {}
    )
    wallets: list[str] = []
    seen: set[str] = set()
    for member in active_set.get("members") or []:
        if not isinstance(member, dict):
            continue
        wallet = _norm_wallet(member.get("source_wallet") or member.get("wallet"))
        if wallet and wallet not in seen:
            wallets.append(wallet)
            seen.add(wallet)
    return wallets


def _wallets_from_active_set_overlay(
    overlay: dict[str, Any], *, now_s: float | None = None
) -> list[str]:
    """Return every live-eligible overlay member plus the active selection pin.

    The persisted overlay is the admission authority even when the resident
    guard currently has an empty runtime active set.  Disabled members stay
    excluded, while an active pin is included independently because it is the
    single-seat selection authority.
    """
    wallets: list[str] = []
    seen: set[str] = set()
    for member in overlay.get("members") or []:
        if not isinstance(member, dict) or member.get("enabled") is not True:
            continue
        wallet = _norm_wallet(member.get("source_wallet") or member.get("wallet"))
        if wallet and wallet not in seen:
            wallets.append(wallet)
            seen.add(wallet)
    pin = overlay.get("selection_pin") if isinstance(overlay.get("selection_pin"), dict) else {}
    pin_active = pin.get("enabled") is True and not pin.get("disabled_at")
    expires_at = str(pin.get("expires_at") or "").strip()
    if pin_active and expires_at:
        try:
            expires_s = dt.datetime.fromisoformat(
                expires_at.replace("Z", "+00:00")
            ).timestamp()
            pin_active = expires_s > float(now_s if now_s is not None else time.time())
        except (TypeError, ValueError):
            pin_active = False
    if pin_active:
        wallet = _norm_wallet(pin.get("source_wallet") or pin.get("wallet"))
        if wallet and wallet not in seen:
            wallets.append(wallet)
    return wallets


def _wallets_from_queue(
    queue: dict[str, Any],
    *,
    clearance_limit: int,
    ranked_offset: int = 0,
    ranked_limit: int = 0,
    include_wallets: list[str],
) -> list[str]:
    wallets: list[str] = []
    seen: set[str] = set()
    ranked_rows = [row for row in queue.get("ranked_members") or [] if isinstance(row, dict)]
    if int(ranked_limit) > 0:
        start = max(0, int(ranked_offset))
        for row in ranked_rows[start : start + int(ranked_limit)]:
            wallet = _norm_wallet(row.get("wallet"))
            if wallet and wallet not in seen:
                wallets.append(wallet)
                seen.add(wallet)
    if int(clearance_limit) > 0:
        clearance_added = 0
        for row in queue.get("ranked_members") or []:
            if not isinstance(row, dict) or not row.get("clearance_ready"):
                continue
            wallet = _norm_wallet(row.get("wallet"))
            if wallet and wallet not in seen:
                wallets.append(wallet)
                seen.add(wallet)
                clearance_added += 1
            if clearance_added >= int(clearance_limit):
                break
    for raw in include_wallets:
        wallet = _norm_wallet(raw)
        if wallet and wallet not in seen:
            wallets.append(wallet)
            seen.add(wallet)
    return wallets


def _wallets_from_cohort(
    cohort: dict[str, Any],
    *,
    offset: int,
    limit: int,
    seen_wallets: set[str] | None = None,
    unseen_first: bool = True,
) -> list[str]:
    rows = cohort.get("live_ready_picks") if isinstance(cohort.get("live_ready_picks"), list) else []
    wallets: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet and wallet not in seen:
            wallets.append(wallet)
            seen.add(wallet)
    checkpoint_wallets = seen_wallets or set()
    if unseen_first:
        wallets = [
            *[wallet for wallet in wallets if wallet not in checkpoint_wallets],
            *[wallet for wallet in wallets if wallet in checkpoint_wallets],
        ]
    start = max(0, int(offset))
    if int(limit) <= 0:
        return wallets[start:]
    return wallets[start : start + int(limit)]


def _stamp_probe_row(
    row: dict[str, Any],
    *,
    fetched_at: str,
    fetched_at_s: float,
    selected_this_run: bool,
) -> dict[str, Any]:
    stamped = dict(row)
    stamped["fetched_at"] = fetched_at
    stamped["fetched_at_s"] = round(float(fetched_at_s), 6)
    stamped["selected_this_run"] = bool(selected_this_run)
    stamped["checkpoint_carryover"] = not bool(selected_this_run)
    return stamped


def _fetch_wallet(wallet: str, args: argparse.Namespace, *, now_s: float) -> dict[str, Any]:
    client = WalletHistoryClient(
        WalletSpec(name=f"remote_dataapi_{wallet[-8:]}", address=wallet),
        timeout_s=float(args.timeout_s),
        retries=int(args.retries),
    )
    start_s = now_s - float(args.lookback_hours) * 3600.0
    raw_rows: list[dict[str, Any]] = []
    normalized_all = []
    normalized = []
    errors: list[str] = []
    started = time.time()
    history_exhausted = False
    coverage_complete_24h = False
    for page in range(max(1, int(args.pages))):
        offset = page * int(args.limit)
        try:
            # Data API /trades filters by `user=`; it silently ignores `proxyWallet=`
            # and returns global rows, which the identity filter then drops to zero.
            rows = client.fetch_trades(limit=int(args.limit), offset=offset)
        except Exception as exc:  # pragma: no cover - live network path.
            errors.append(f"{type(exc).__name__}: {exc}")
            break
        raw_rows.extend(rows)
        for raw in rows:
            event = normalize_polymarket_wallet_row(raw, spec=client.spec, row_type="trade", observed_ts=now_s)
            if event is None or event.event_ts is None:
                continue
            normalized_all.append(event)
            if event.event_ts < start_s:
                coverage_complete_24h = True
                continue
            normalized.append(event)
        if len(rows) < int(args.limit):
            history_exhausted = True
            break
        if coverage_complete_24h:
            break
    btc5m = [event for event in normalized if event.asset.upper() == "BTC" and event.duration == "5m"]
    buys = [event for event in btc5m if event.action.upper() == "BUY"]
    buys_30m = [event for event in buys if event.event_ts is not None and event.event_ts >= now_s - 1800.0]
    inband_buys = [
        event
        for event in buys
        if float(args.min_price) <= num(event.price, 0.0) <= float(args.max_price)
    ]
    buys_by_price_subband = {
        name: sum(low <= num(event.price, 0.0) < high for event in buys)
        for name, (low, high) in BTC5M_BUY_PRICE_SUBBANDS.items()
    }
    latest_trade_ts = max((event.event_ts or 0.0 for event in btc5m), default=0.0)
    latest_age_h = round((now_s - latest_trade_ts) / 3600.0, 6) if latest_trade_ts > 0 else None
    remote_rows_saturated = len(raw_rows) >= max(1, int(args.pages)) * int(args.limit)
    coverage_complete_24h = bool(coverage_complete_24h or history_exhausted)
    page_cap_reached = bool(remote_rows_saturated or (errors and raw_rows and not coverage_complete_24h))
    censored = bool(page_cap_reached and not coverage_complete_24h and not btc5m)
    status = "ERROR" if errors and not censored else (CENSORED_PAGINATION_CAP_STATUS if censored else "PASS")
    return {
        "wallet": wallet,
        "status": status,
        "censored": "PAGINATION_CAP" if censored else "",
        "coverage_complete_24h": coverage_complete_24h,
        "duration_s": round(max(0.0, time.time() - started), 6),
        "raw_rows": len(raw_rows),
        "remote_rows_saturated": remote_rows_saturated,
        "normalized_rows_total": len(normalized_all),
        "normalized_rows_24h": len(normalized),
        "btc5m_trades_24h": len(btc5m),
        "btc5m_buys_24h": len(buys),
        "btc5m_buys_30m": len(buys_30m),
        "policy_compatible_inband_buy_rows_24h": len(inband_buys),
        "btc5m_buy_rows_24h_by_price_subband": buys_by_price_subband,
        "latest_btc5m_trade_ts": latest_trade_ts or None,
        "latest_trade_age_h": latest_age_h,
        "pass_admission_threshold": bool(len(buys) >= 3 and latest_age_h is not None and latest_age_h <= 6.0),
        "errors": errors,
        "route_report": client.last_route_report,
    }


def _merge_probe(probe: dict[str, Any], rows_by_wallet: dict[str, dict[str, Any]]) -> dict[str, Any]:
    merged = dict(probe)
    for key in ("ranked_candidates", "fresh_local_feed_outside_queue", "queue_candidates_measured", "recommendations"):
        values = merged.get(key)
        if not isinstance(values, list):
            continue
        for row in values:
            if not isinstance(row, dict):
                continue
            wallet = _norm_wallet(row.get("wallet"))
            result = rows_by_wallet.get(wallet)
            if not result:
                continue
            evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
            evidence = dict(evidence)
            evidence["remote_dataapi_24h"] = {
                "status": result.get("status"),
                "btc5m_trades_24h": result.get("btc5m_trades_24h"),
                "btc5m_buys_24h": result.get("btc5m_buys_24h"),
                "policy_compatible_inband_buy_rows_24h": result.get("policy_compatible_inband_buy_rows_24h"),
                "btc5m_buy_rows_24h_by_price_subband": result.get(
                    "btc5m_buy_rows_24h_by_price_subband"
                ),
                "latest_trade_age_h": result.get("latest_trade_age_h"),
                "latest_btc5m_trade_ts": result.get("latest_btc5m_trade_ts"),
                "pass_admission_threshold": result.get("pass_admission_threshold"),
                "remote_rows_saturated": result.get("remote_rows_saturated"),
                "coverage_complete_24h": result.get("coverage_complete_24h"),
                "censored": result.get("censored") or "",
            }
            row["evidence"] = evidence
    remote_rows = [rows_by_wallet[wallet] for wallet in rows_by_wallet]
    pass_wallets = [
        wallet for wallet, row in rows_by_wallet.items() if row.get("pass_admission_threshold")
    ]
    error_wallets = [
        wallet for wallet, row in rows_by_wallet.items() if row.get("status") == "ERROR"
    ]
    censored_wallets = [
        wallet
        for wallet, row in rows_by_wallet.items()
        if row.get("status") == CENSORED_PAGINATION_CAP_STATUS or row.get("censored")
    ]
    merged["remote_dataapi_24h"] = {
        "generated_at": utc_now_iso(),
        "wallets": list(rows_by_wallet),
        "rows": remote_rows,
        "pass_admission_threshold_wallets": pass_wallets,
        "error_wallets": error_wallets,
        "censored_wallets": censored_wallets,
    }
    summary = merged.get("summary") if isinstance(merged.get("summary"), dict) else {}
    summary = dict(summary)
    summary.update(
        {
            "remote_dataapi_24h_wallets": len(remote_rows),
            "remote_dataapi_24h_errors": len(error_wallets),
            "remote_dataapi_24h_censored": len(censored_wallets),
            "remote_dataapi_24h_p1_eligible": len(pass_wallets),
            "remote_dataapi_24h_btc5m_buys": sum(
                int(row.get("btc5m_buys_24h") or 0) for row in remote_rows
            ),
            "btc5m_buy_rows_24h_by_price_subband": {
                name: sum(
                    int((row.get("btc5m_buy_rows_24h_by_price_subband") or {}).get(name) or 0)
                    for row in remote_rows
                )
                for name in BTC5M_BUY_PRICE_SUBBANDS
            },
        }
    )
    merged["summary"] = summary
    return merged


def _paper_shadow_enrollments(
    prior: dict[str, Any],
    rows_by_wallet: dict[str, dict[str, Any]],
    *,
    generated_at: str,
) -> list[dict[str, Any]]:
    prior_rows = prior.get("paper_shadow_enrollments") if isinstance(prior.get("paper_shadow_enrollments"), list) else []
    by_wallet = {
        wallet: dict(row)
        for row in prior_rows
        if isinstance(row, dict)
        for wallet in [_norm_wallet(row.get("wallet"))]
        if wallet
    }
    for wallet, evidence in rows_by_wallet.items():
        if not evidence.get("pass_admission_threshold"):
            continue
        previous = by_wallet.get(wallet, {})
        by_wallet[wallet] = {
            "wallet": wallet,
            "status": "DIRECT_USER_TRADES_ACTIVE_PAPER_SHADOW",
            "paper_only": True,
            "live_orders_allowed": False,
            "paper_policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
            "paper_canary_enrolled_at": previous.get("paper_canary_enrolled_at") or generated_at,
            "last_refreshed_at": evidence.get("fetched_at") or generated_at,
            "latest_btc5m_trade_ts": evidence.get("latest_btc5m_trade_ts"),
            "latest_trade_age_h": evidence.get("latest_trade_age_h"),
            "btc5m_buys_24h": evidence.get("btc5m_buys_24h"),
            "btc5m_buys_30m": evidence.get("btc5m_buys_30m"),
            "policy_compatible_inband_buy_rows_24h": evidence.get(
                "policy_compatible_inband_buy_rows_24h"
            ),
            "btc5m_buy_rows_24h_by_price_subband": evidence.get(
                "btc5m_buy_rows_24h_by_price_subband"
            ),
            "next": (
                "accrue direct user-trade and local source-active paper evidence; "
                "promotion still requires the unchanged temporal, source-active, history, and Fable gates"
            ),
        }
    return sorted(by_wallet.values(), key=lambda row: str(row.get("wallet") or ""))


def main() -> int:
    args = parse_args()
    original_data_api_override = os.environ.get("POLYMARKET_DATA_API_BASE_URL")
    if bool(args.direct_data_api):
        os.environ.pop("POLYMARKET_DATA_API_BASE_URL", None)
    now_s = time.time()
    queue = load_json(args.queue, default={})
    cohort = load_json(args.cohort_replay, default={}) if str(args.cohort_replay or "").strip() else {}
    live_guard_state = load_json(args.live_guard_state, default={})
    active_set_overlay = load_json(args.active_set_overlay, default={})
    checkpoint = load_json(args.checkpoint, default={})
    checkpoint_rows = checkpoint.get("rows_by_wallet") if isinstance(checkpoint.get("rows_by_wallet"), dict) else {}
    include_wallets = [str(item) for item in args.include_wallet or []]
    if not bool(args.no_default_include_wallet):
        include_wallets = [DEFAULT_INCLUDE_WALLET, *include_wallets]
    active_set_wallets = (
        _wallets_from_active_set(live_guard_state)
        if bool(args.include_active_set_wallets) and isinstance(live_guard_state, dict)
        else []
    )
    if bool(args.include_active_set_wallets) and isinstance(active_set_overlay, dict):
        active_set_wallets.extend(
            wallet
            for wallet in _wallets_from_active_set_overlay(active_set_overlay, now_s=now_s)
            if wallet not in active_set_wallets
        )
    include_wallets = [*active_set_wallets, *include_wallets]
    cohort_wallets = _wallets_from_cohort(
        cohort if isinstance(cohort, dict) else {},
        offset=int(args.cohort_offset),
        limit=int(args.cohort_limit),
        seen_wallets={_norm_wallet(wallet) for wallet in checkpoint_rows},
        unseen_first=bool(args.cohort_unseen_first),
    )
    wallets = _wallets_from_queue(
        queue,
        clearance_limit=int(args.clearance_limit),
        ranked_offset=int(args.ranked_offset),
        ranked_limit=int(args.ranked_limit),
        include_wallets=[*cohort_wallets, *include_wallets],
    )
    rows_by_wallet: dict[str, dict[str, Any]] = {
        wallet: _stamp_probe_row(
            row,
            fetched_at=str(row.get("fetched_at") or ""),
            fetched_at_s=_as_float(row.get("fetched_at_s"), 0.0),
            selected_this_run=False,
        )
        for wallet, row in checkpoint_rows.items()
        if isinstance(row, dict)
    }
    for wallet in wallets:
        result = _fetch_wallet(wallet, args, now_s=now_s)
        rows_by_wallet[wallet] = _stamp_probe_row(
            result,
            fetched_at=utc_now_iso(),
            fetched_at_s=now_s,
            selected_this_run=True,
        )
        atomic_write_json(
            args.checkpoint,
            {
                "schema_version": 1,
                "kind": "queue_remote_dataapi_fresh_flow_probe_checkpoint",
                "updated_at": utc_now_iso(),
                "rows_by_wallet": rows_by_wallet,
                "paper_shadow_enrollments": (
                    checkpoint.get("paper_shadow_enrollments")
                    if isinstance(checkpoint.get("paper_shadow_enrollments"), list)
                    else []
                ),
            },
        )
    report_rows = [rows_by_wallet[wallet] for wallet in wallets if wallet in rows_by_wallet]
    selected_wallet_set = set(wallets)
    generated_at = utc_now_iso()
    paper_shadow_enrollments = _paper_shadow_enrollments(
        checkpoint if isinstance(checkpoint, dict) else {},
        rows_by_wallet,
        generated_at=generated_at,
    )
    cumulative_rows = [
        (
            row
            if wallet in selected_wallet_set and row.get("selected_this_run") is True
            else _stamp_probe_row(
                row,
                fetched_at=str(row.get("fetched_at") or ""),
                fetched_at_s=_as_float(row.get("fetched_at_s"), 0.0),
                selected_this_run=False,
            )
        )
        for wallet, row in rows_by_wallet.items()
        if isinstance(row, dict)
    ]
    report = {
        "schema_version": 1,
        "kind": "queue_remote_dataapi_fresh_flow_probe",
        "flow_stage": "ROTATE/LEARN",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": generated_at,
        "criteria": {
            "lookback_hours": float(args.lookback_hours),
            "clearance_limit": int(args.clearance_limit),
            "ranked_offset": int(args.ranked_offset),
            "ranked_limit": int(args.ranked_limit),
            "cohort_replay": str(args.cohort_replay or ""),
            "cohort_live_ready_wallets": len(
                (cohort or {}).get("live_ready_picks")
                if isinstance((cohort or {}).get("live_ready_picks"), list)
                else []
            ),
            "cohort_offset": int(args.cohort_offset),
            "cohort_limit": int(args.cohort_limit),
            "cohort_unseen_first": bool(args.cohort_unseen_first),
            "cohort_wallets_selected": len(cohort_wallets),
            "pages": int(args.pages),
            "limit": int(args.limit),
            "timeout_s": float(args.timeout_s),
            "direct_data_api": bool(args.direct_data_api),
            "data_api_base_override_cleared": bool(args.direct_data_api and original_data_api_override),
            "include_active_set_wallets": bool(args.include_active_set_wallets),
            "active_set_wallets": active_set_wallets,
            "admission_threshold": "btc5m_buys_24h>=3 and latest_trade_age_h<=6",
        },
        "summary": {
            "wallets": len(wallets),
            "cohort_wallets": len(cohort_wallets),
            "active_set_wallets": len(active_set_wallets),
            "pass_admission_threshold": sum(1 for row in report_rows if row.get("pass_admission_threshold")),
            "error_wallets": sum(1 for row in report_rows if row.get("status") == "ERROR"),
            "censored_wallets": sum(
                1
                for row in report_rows
                if row.get("status") == CENSORED_PAGINATION_CAP_STATUS or row.get("censored")
            ),
            "cumulative_wallets": len(cumulative_rows),
            "cumulative_pass_admission_threshold": sum(
                1 for row in cumulative_rows if row.get("pass_admission_threshold")
            ),
            "cumulative_checkpoint_carryover_wallets": sum(
                1
                for row in cumulative_rows
                if row.get("checkpoint_carryover") is True
                and str(row.get("wallet") or "").strip().lower() not in selected_wallet_set
            ),
            "cumulative_error_wallets": sum(1 for row in cumulative_rows if row.get("status") == "ERROR"),
            "cumulative_censored_wallets": sum(
                1
                for row in cumulative_rows
                if row.get("status") == CENSORED_PAGINATION_CAP_STATUS or row.get("censored")
            ),
            "paper_shadow_enrollments": len(paper_shadow_enrollments),
            "btc5m_buy_rows_24h_by_price_subband": {
                name: sum(
                    int((row.get("btc5m_buy_rows_24h_by_price_subband") or {}).get(name) or 0)
                    for row in cumulative_rows
                )
                for name in BTC5M_BUY_PRICE_SUBBANDS
            },
        },
        "rows": cumulative_rows,
        "selected_wallets": wallets,
        "selected_rows": report_rows,
        "paper_shadow_enrollments": paper_shadow_enrollments,
    }
    atomic_write_json(
        args.checkpoint,
        {
            "schema_version": 1,
            "kind": "queue_remote_dataapi_fresh_flow_probe_checkpoint",
            "updated_at": generated_at,
            "rows_by_wallet": rows_by_wallet,
            "paper_shadow_enrollments": paper_shadow_enrollments,
        },
    )
    atomic_write_json(args.output, report)
    probe = load_json(args.probe, default={})
    atomic_write_json(args.probe, _merge_probe(probe, rows_by_wallet))
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
