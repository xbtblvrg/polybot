#!/usr/bin/env python3
"""Score and reconcile the prospective WIDE exact-policy paper cohort.

The ledger is append-only and paper-only.  A fill and its later resolution are
separate immutable events; the atomic state is the restart-safe materialized
view.  No CopyIntent is created and no live file is read or written.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import re
import sys
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_realtime_runtime_proof import _gamma_token_metadata, _token_metadata
from scripts.run_top10_broad_paper_lane import (
    _book_top_of_book,
    _book_unavailable_or_market_closed,
    _copy_size_usd,
    _iter_recent_jsonl,
    _normalized_realtime_event,
    _wallet_sides,
)
from src.wallet_copy.alpha_decay import btc_5m_move_slice_for_values
from src.wallet_copy.alpha_freshness import require_fresh_alpha_report
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd
from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.models import num, parse_ts, stable_id, utc_now_iso
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json


DEFAULT_STATE = "data/research/wide_exact_policy_paper_state.json"
DEFAULT_LEDGER = "data/research/wide_exact_policy_paper_orders.jsonl"
DEFAULT_MANIFEST_POINTER = "data/research/wide_exact_policy_manifest_active.json"
DEFAULT_F3_INSTRUMENTATION = (
    "data/research/wide_f3_instrumentation_events.jsonl"
)
DEFAULT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_CLOB_BASE = "https://clob.polymarket.com"
POLICY_ID = "wide_positive_alpha_exact_wf0p1_max1_min1"
WIDE_POLICY_MAX_ORDER_USD = 1.0
WIDE_POLICY_MIN_ORDER_USD = 1.0
WIDE_POLICY_WALLET_FRACTION = 0.1
WIDE_POLICY_MAX_FILL_LAG_S = 5.0
WIDE_POLICY_FEE_MODEL_ID = "polymarket_embedded_buy_fee_v1"
WIDE_POLICY_SELECTION_RULE_ID = "frozen_positive_70pct_move_slices_v1"


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def terminal_fetch_context(
    *,
    prefetch: dict[str, Any],
    raw_row: dict[str, Any],
    default_cycle_id: str,
    token_event_ordinal_in_cycle: int,
    token_cycle_first_recv_monotonic_s: float | None,
) -> dict[str, Any]:
    """Return provenance fields independently of any computed lag outcome."""

    provenance = str(prefetch.get("fetch_provenance") or "no_prefetch_entry")
    allowed = {
        "capture_prefetched",
        "reconcile_batch_fetched",
        "no_prefetch_entry",
    }
    if provenance not in allowed:
        raise ValueError(f"unknown fetch provenance: {provenance}")
    fetch_started = prefetch.get("fetch_started_monotonic_s")
    fetch_started_observed = fetch_started is not None and num(fetch_started) > 0
    recv_monotonic_s = num(raw_row.get("recv_monotonic_s"))
    fanout_received_monotonic_s = num(
        raw_row.get("fanout_received_monotonic_s")
    )
    fetch_started_monotonic_s = (
        num(fetch_started) if fetch_started_observed else None
    )
    fanout_to_fetch_ms = (
        round(max(0.0, fetch_started_monotonic_s - fanout_received_monotonic_s) * 1000.0, 3)
        if fetch_started_monotonic_s is not None
        and fanout_received_monotonic_s > 0
        else None
    )
    queue_wait_ms = prefetch.get("prefetch_queue_wait_ms")
    pairing_check = None
    if provenance == "capture_prefetched" and fanout_to_fetch_ms is not None:
        pairing_check = (
            "PASS"
            if 0.0 <= fanout_to_fetch_ms <= num(queue_wait_ms) + 1.0
            else "FAIL"
        )
    decision_monotonic_s = time.monotonic()
    return {
        "fetch_provenance": provenance,
        "fetch_cycle_id": str(prefetch.get("fetch_cycle_id") or default_cycle_id),
        "fetch_started_monotonic_s": fetch_started_monotonic_s,
        "fetch_started_monotonic_observed": fetch_started_observed,
        "recv_monotonic_s": recv_monotonic_s,
        "fanout_received_monotonic_s": (
            fanout_received_monotonic_s if fanout_received_monotonic_s > 0 else None
        ),
        "upstream_to_fanout_ms": (
            round(max(0.0, fanout_received_monotonic_s - recv_monotonic_s) * 1000.0, 3)
            if fanout_received_monotonic_s > 0 and recv_monotonic_s > 0
            else None
        ),
        "fanout_to_fetch_ms": fanout_to_fetch_ms,
        "prefetch_queue_wait_ms": queue_wait_ms,
        "prefetch_worker_queue_ms": prefetch.get("prefetch_worker_queue_ms"),
        "prefetch_network_ms": prefetch.get("prefetch_network_ms"),
        "prefetch_parse_ms": prefetch.get("prefetch_parse_ms"),
        "fetch_pairing_check": pairing_check,
        "capture_to_decision_ms": (
            round(max(0.0, decision_monotonic_s - fetch_started_monotonic_s) * 1000.0, 3)
            if fetch_started_monotonic_s is not None
            else None
        ),
        "token_cycle_first_recv_monotonic_s": (
            num(token_cycle_first_recv_monotonic_s)
            if token_cycle_first_recv_monotonic_s is not None
            else None
        ),
        "token_event_ordinal_in_cycle": int(token_event_ordinal_in_cycle),
    }


def wide_policy_identity(
    *,
    wallet: str,
    move_slice_keys: list[str] | tuple[str, ...] | set[str],
    policy_id: str = POLICY_ID,
    max_order_usd: float = WIDE_POLICY_MAX_ORDER_USD,
    min_order_usd: float = WIDE_POLICY_MIN_ORDER_USD,
    wallet_fraction: float = WIDE_POLICY_WALLET_FRACTION,
    max_fill_lag_s: float = WIDE_POLICY_MAX_FILL_LAG_S,
    fee_model_id: str = WIDE_POLICY_FEE_MODEL_ID,
    selection_rule_id: str = WIDE_POLICY_SELECTION_RULE_ID,
) -> dict[str, Any]:
    """Return the immutable per-wallet WIDE policy identity and SHA-256."""

    identity = {
        "policy_id": str(policy_id),
        "wallet": _norm_wallet(wallet),
        "move_slice_keys": sorted({str(value) for value in move_slice_keys if str(value)}),
        "max_order_usd": float(max_order_usd),
        "min_order_usd": float(min_order_usd),
        "wallet_fraction": float(wallet_fraction),
        "max_fill_lag_s": float(max_fill_lag_s),
        "fee_model_id": str(fee_model_id),
        "selection_rule_id": str(selection_rule_id),
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return {**identity, "wide_policy_fingerprint": hashlib.sha256(encoded).hexdigest()}


def manifest_wallet_policy_identities(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build exact policy identities for every wallet in a frozen manifest."""

    rows = (
        manifest.get("capture_watch_wallets")
        if isinstance(manifest.get("capture_watch_wallets"), list)
        else manifest.get("admitted_wallets")
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        move_slice_keys = [
            str(value)
            for value in row.get("move_slice_keys") or []
            if str(value)
        ]
        if wallet and move_slice_keys and row.get("policy_absent") is not True:
            result[wallet] = wide_policy_identity(
                wallet=wallet,
                move_slice_keys=move_slice_keys,
            )
    return result


def _jsonl(path: str, limit: int = 500_000) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
                if len(rows) > limit:
                    rows.pop(0)
    return rows


def resolution_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index canonical BTC resolutions by token, condition, and slug."""

    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction") or "").upper()
        if direction not in {"UP", "DOWN"}:
            continue
        for key in (
            str(row.get("market_slug") or ""),
            str(row.get("condition_id") or "").lower(),
            str(row.get("yes_token") or ""),
            str(row.get("no_token") or ""),
        ):
            if key:
                index[key] = row
    return index


def _winning_token(resolution: dict[str, Any]) -> str:
    return str(
        resolution.get("yes_token")
        if str(resolution.get("direction") or "").upper() == "UP"
        else resolution.get("no_token")
    )


def apply_resolutions(
    orders: list[dict[str, Any]],
    resolutions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return updated logical orders and immutable resolution ledger events."""

    index = resolution_index(resolutions)
    updated: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for raw in orders:
        order = dict(raw)
        if order.get("resolved") is True:
            updated.append(order)
            continue
        resolution = None
        for key in (
            str(order.get("token_id") or ""),
            str(order.get("condition_id") or "").lower(),
            str(order.get("market_slug") or ""),
        ):
            if key and key in index:
                resolution = index[key]
                break
        if not resolution:
            updated.append(order)
            continue
        shares = num(order.get("filled_shares"))
        price = num(order.get("fill_price"))
        cost = num(order.get("filled_cost_usd"), shares * price)
        won = str(order.get("token_id") or "") == _winning_token(resolution)
        payout = shares if won else 0.0
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        pre_fee = round(payout - cost, 6)
        post_fee = round(pre_fee - fee, 6)
        patch = {
            "resolved": True,
            "resolution_direction": str(resolution.get("direction") or "").upper(),
            "resolution_source": resolution.get("source"),
            "resolution_computed_at": resolution.get("computed_at_iso"),
            "won": won,
            "payout_usd": round(payout, 6),
            "expected_fee_usd": fee,
            "pre_fee_pnl_usd": pre_fee,
            "post_fee_pnl_usd": post_fee,
        }
        order.update(patch)
        updated.append(order)
        event = {
                "schema_version": 1,
                "event": "wide_exact_policy_paper_order_resolved",
                "recorded_at": utc_now_iso(),
                "order_id": order["order_id"],
                **patch,
        }
        event["event_id"] = stable_id(
            "wideevent", {"event": event["event"], "order_id": order["order_id"]}, length=32
        )
        events.append(event)
    return updated, events


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, math.ceil(pct * len(ordered)) - 1))
    return round(ordered[rank], 6)


def _half_pnl(rows: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    resolved = sorted(
        (row for row in rows if row.get("resolved") is True),
        key=lambda row: (num(row.get("source_event_ts")), str(row.get("order_id") or "")),
    )
    if not resolved:
        return None, None
    midpoint = (len(resolved) + 1) // 2
    first = round(sum(num(row.get("post_fee_pnl_usd")) for row in resolved[:midpoint]), 6)
    second_rows = resolved[midpoint:]
    second = round(sum(num(row.get("post_fee_pnl_usd")) for row in second_rows), 6) if second_rows else None
    return first, second


def build_summary(
    *,
    cohort: dict[str, Any],
    orders: list[dict[str, Any]],
    attempts: dict[str, int],
    refusals: dict[str, Counter[str]],
    wallet_order: list[str],
) -> dict[str, Any]:
    wallets: dict[str, Any] = {}
    for wallet in wallet_order:
        rows = [row for row in orders if row.get("wallet") == wallet]
        resolved = [row for row in rows if row.get("resolved") is True]
        lags = [num(row.get("receipt_to_book_fetch_lag_s")) for row in rows]
        first_half, second_half = _half_pnl(rows)
        attempted = int(attempts.get(wallet, 0))
        copyable = len(rows)
        fees = round(sum(num(row.get("expected_fee_usd")) for row in resolved), 6)
        post_fee = round(sum(num(row.get("post_fee_pnl_usd")) for row in resolved), 6)
        fee_covered = sum(row.get("expected_fee_usd") is not None for row in resolved)
        computed_refusals = Counter(refusals.get(wallet, Counter()))
        computed_refusals["resolution_pending"] = copyable - len(resolved)
        computed_refusals["fee_reconciliation_missing"] = len(resolved) - fee_covered
        wallets[wallet] = {
            "decision_rank": wallet_order.index(wallet) + 1,
            "attempted_exact_policy_buys": attempted,
            "copyable_exact_policy_buys": copyable,
            "copyable_rate_pct": round(100.0 * copyable / attempted, 6) if attempted else None,
            "resolved_orders": len(resolved),
            "unique_resolved_windows": len({row.get("market_slug") for row in resolved}),
            "fee_covered_resolved_orders": fee_covered,
            "fee_coverage_pct": round(100.0 * fee_covered / len(resolved), 6) if resolved else None,
            "fees_usd": fees,
            "pre_fee_pnl_usd": round(sum(num(row.get("pre_fee_pnl_usd")) for row in resolved), 6),
            "post_fee_pnl_usd": post_fee,
            "first_half_post_fee_pnl_usd": first_half,
            "second_half_post_fee_pnl_usd": second_half,
            "max_receipt_to_book_fetch_lag_s": max(lags) if lags else None,
            "p95_receipt_to_book_fetch_lag_s": _percentile(lags, 0.95),
            "all_fill_lags_lte_5s": bool(lags and max(lags) <= 5.0),
            "first_prospective_ts": min((row["source_event_ts"] for row in rows), default=None),
            "last_prospective_ts": max((row["source_event_ts"] for row in rows), default=None),
            "checkpoint": 50 if len(resolved) >= 50 else 25 if len(resolved) >= 25 else 10 if len(resolved) >= 10 else 0,
            "refusal_counts": {
                key: value for key, value in sorted(computed_refusals.items()) if value
            },
            "promotion_gates": {
                "copyable_rate_gte_70": bool(attempted and copyable / attempted >= 0.70),
                "resolved_gte_50": len(resolved) >= 50,
                "fees_complete": bool(resolved and fee_covered == len(resolved)),
                "post_fee_total_positive": bool(resolved and post_fee > 0),
                "first_half_post_fee_positive": bool(first_half is not None and first_half > 0),
                "second_half_post_fee_positive": bool(second_half is not None and second_half > 0),
                "every_fill_lag_lte_5s": bool(lags and max(lags) <= 5.0),
            },
        }
        wallets[wallet]["paper_accounting_gate_pass"] = all(wallets[wallet]["promotion_gates"].values())
    return {
        "schema_version": 1,
        "kind": "wide_exact_policy_prospective_paper_state",
        "flow_stage": "LEARN/OBSERVE/PROMOTE",
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "policy_id": POLICY_ID,
        "cohort": cohort,
        "diagnostic_broad_history_is_admission_evidence": False,
        "orders": orders,
        "wallets": wallets,
        "summary": {
            "attempted_exact_policy_buys": sum(attempts.values()),
            "copyable_exact_policy_buys": len(orders),
            "resolved_orders": sum(row.get("resolved") is True for row in orders),
            "winner_wallets": [wallet for wallet, row in wallets.items() if row["paper_accounting_gate_pass"]],
        },
    }


def reconcile_state(
    prior: dict[str, Any],
    *,
    new_orders: list[dict[str, Any]],
    resolutions: list[dict[str, Any]],
    attempts: dict[str, int],
    refusals: dict[str, Counter[str]],
    wallet_order: list[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Dedupe fills, reconcile late outcomes, and materialize state."""

    cohort = prior.get("cohort") if isinstance(prior.get("cohort"), dict) else {}
    by_id = {
        str(row.get("order_id")): dict(row)
        for row in prior.get("orders") or []
        if isinstance(row, dict) and row.get("order_id")
    }
    append_events: list[dict[str, Any]] = []
    for order in new_orders:
        order_id = str(order.get("order_id") or "")
        if not order_id or order_id in by_id:
            continue
        by_id[order_id] = dict(order)
        event = {"event": "wide_exact_policy_paper_order_filled", **order}
        event["event_id"] = stable_id(
            "wideevent", {"event": event["event"], "order_id": order_id}, length=32
        )
        append_events.append(event)
    orders, resolution_events = apply_resolutions(list(by_id.values()), resolutions)
    append_events.extend(resolution_events)
    summary = build_summary(
        cohort=cohort,
        orders=orders,
        attempts=attempts,
        refusals=refusals,
        wallet_order=wallet_order,
    )
    return summary, append_events


def replay_ledger(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], set[str]]:
    """Materialize logical orders from the authoritative append-only ledger."""

    orders: dict[str, dict[str, Any]] = {}
    event_ids: set[str] = set()
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        event = str(raw.get("event") or "")
        order_id = str(raw.get("order_id") or "")
        event_id = str(raw.get("event_id") or stable_id(
            "wideevent", {"event": event, "order_id": order_id}, length=32
        ))
        if event_id in event_ids:
            continue
        event_ids.add(event_id)
        if not order_id or event not in {
            "wide_exact_policy_paper_order_filled",
            "wide_exact_policy_paper_order_resolved",
        }:
            continue
        if event == "wide_exact_policy_paper_order_filled":
            order = {
                key: value
                for key, value in raw.items()
                if key not in {"event", "event_id"}
            }
            orders.setdefault(order_id, order)
        elif order_id in orders:
            orders[order_id].update(
                {
                    key: value
                    for key, value in raw.items()
                    if key not in {"schema_version", "event", "event_id", "recorded_at", "order_id"}
                }
            )
    return list(orders.values()), event_ids


def _positive_profile_slices(alpha: dict[str, Any], wallets: list[str]) -> dict[str, set[str]]:
    profiles = ((alpha.get("execution_profiles") or {}).get("profiles_by_wallet") or {})
    selected: dict[str, set[str]] = {}
    for wallet in wallets:
        profile = profiles.get(wallet) if isinstance(profiles.get(wallet), dict) else {}
        selected[wallet] = {
            str(row.get("move_slice_key"))
            for row in profile.get("move_slices") or []
            if isinstance(row, dict)
            and num(row.get("mean_edge")) > 0
            and num(row.get("median_edge")) > 0
            and num(row.get("copyable_rate_pct")) >= 70.0
        }
    return selected


def _load_fresh_alpha_report(path: str) -> dict[str, Any]:
    report = load_json(path, default={})
    if not isinstance(report, dict) or not report:
        raise ValueError(f"ALPHA_REPORT_MISSING_REFUSED path={path}")
    require_fresh_alpha_report(report, path=path, max_age_h=24.0)
    return report


def _require_fresh_manifest_alpha(manifest: dict[str, Any], *, manifest_path: str) -> None:
    source_path = str(manifest.get("source_alpha_report") or "")
    if not source_path:
        raise ValueError(
            f"MANIFEST_ALPHA_REPORT_MISSING_REFUSED path={manifest_path}"
        )
    _load_fresh_alpha_report(source_path)


def resolve_manifest_pointer(pointer_path: str) -> str:
    pointer = load_json(pointer_path, default={})
    manifest_path = (
        str(pointer.get("manifest_path") or "")
        if isinstance(pointer, dict)
        else ""
    )
    if not manifest_path:
        raise ValueError(f"MANIFEST_POINTER_MISSING_REFUSED path={pointer_path}")
    manifest = load_json(manifest_path, default={})
    if not isinstance(manifest, dict) or not manifest:
        raise ValueError(f"MANIFEST_MISSING_REFUSED path={manifest_path}")
    if str(pointer.get("manifest_id") or "") != str(manifest.get("manifest_id") or ""):
        raise ValueError(f"MANIFEST_POINTER_ID_MISMATCH_REFUSED path={pointer_path}")
    _require_fresh_manifest_alpha(manifest, manifest_path=manifest_path)
    return manifest_path


def _manifest_policy(manifest: dict[str, Any]) -> tuple[list[str], dict[str, set[str]]]:
    rows = (
        manifest.get("capture_watch_wallets")
        if isinstance(manifest.get("capture_watch_wallets"), list)
        else manifest.get("admitted_wallets")
    )
    rows = rows if isinstance(rows, list) else []
    wallets: list[str] = []
    slices: dict[str, set[str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        move_slice_keys = {
            str(value)
            for value in row.get("move_slice_keys") or []
            if str(value)
        }
        if (
            not wallet
            or not move_slice_keys
            or row.get("policy_absent") is True
        ):
            continue
        wallets.append(wallet)
        slices[wallet] = move_slice_keys
    return wallets, slices


def direct_event_input(args: argparse.Namespace) -> str:
    direct_event_json = str(getattr(args, "direct_event_json", "") or "")
    direct_event_file = str(getattr(args, "direct_event_file", "") or "")
    if direct_event_json and direct_event_file:
        raise ValueError("DIRECT_EVENT_INPUT_AMBIGUOUS_REFUSED")
    return (
        Path(direct_event_file).read_text(encoding="utf-8")
        if direct_event_file
        else direct_event_json
    )


def _metadata_for_events(
    history_path: str,
    gamma_base_url: str,
    rows: list[dict[str, Any]],
    cache_path: str = "",
    merge_summary: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    cached = load_json(cache_path, default={}) if cache_path else {}
    meta = dict(cached) if isinstance(cached, dict) else {}
    history_meta = _token_metadata(history_path)
    history_merged = False
    for token_id, value in history_meta.items():
        if token_id in meta or not isinstance(value, dict):
            continue
        meta[token_id] = {
            **value,
            "token_mapping_source": "rtds_source_event_derived",
        }
        history_merged = True
    missing_starts: set[int] = set()
    for row in rows:
        normalized = _normalized_realtime_event(row)
        token_id = str(
            (normalized or {}).get("asset")
            or (row.get("decoded") or {}).get("asset")
            or ""
        )
        event_ts = num((normalized or {}).get("event_ts") or row.get("block_ts"))
        if event_ts > 0 and (not token_id or token_id not in meta):
            missing_starts.add(int(event_ts // 300 * 300))
    starts = sorted(missing_starts)
    if starts:
        for token_id, value in _gamma_token_metadata(gamma_base_url, starts=starts).items():
            meta.setdefault(token_id, value)
    if cache_path and (history_merged or starts or not cached):
        atomic_write_json(cache_path, meta)
    if merge_summary is not None:
        merge_summary["new_tokens_merged"] = max(0, len(meta) - len(cached))
        merge_summary["new_tokens_from_hot_history"] = sum(
            token_id not in cached for token_id in history_meta
        )
    return meta


def _order_identity(cohort_id: str, run_id: str, wallet: str, normalized: dict[str, Any]) -> str:
    return stable_id(
        "widepaper",
        {
            "cohort_id": cohort_id,
            "run_id": run_id,
            "policy_id": POLICY_ID,
            "wallet": wallet,
            "transaction_hash": normalized["tx"],
            "log_index": normalized["source_event_id"],
            "token_id": normalized["asset"],
        },
        length=32,
    )


def _metadata_retry_rows(
    path: str,
    *,
    token_ids: set[str],
    desired_attempt_ids: set[str],
    cohort_id: str,
    run_id: str,
    wallets: set[str],
) -> list[dict[str, Any]]:
    """Recover raw events for now-mapped terminal metadata misses.

    The normal reader is intentionally tail-bounded. This explicit repair pass
    filters the append-only source by token bytes before JSON decoding and
    stops as soon as every recoverable immutable attempt identity is found.
    """

    target = Path(path)
    if not target.exists() or not token_ids or not desired_attempt_ids:
        return []
    token_pattern = re.compile(
        b"|".join(re.escape(token.encode("ascii")) for token in sorted(token_ids))
    )
    recovered: dict[str, dict[str, Any]] = {}
    with target.open("rb") as handle:
        for raw_line in handle:
            if not token_pattern.search(raw_line):
                continue
            try:
                row = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(row, dict):
                continue
            normalized = _normalized_realtime_event(row)
            if (
                not normalized
                or normalized.get("source") != "polygon_ws"
                or str(normalized.get("asset") or "") not in token_ids
            ):
                continue
            for wallet, side in _wallet_sides(row, wallets):
                if side != "BUY":
                    continue
                attempt_id = _order_identity(
                    cohort_id, run_id, wallet, normalized
                )
                if attempt_id in desired_attempt_ids:
                    recovered.setdefault(attempt_id, row)
            if len(recovered) == len(desired_attempt_ids):
                break
    return list(recovered.values())


def score_cycle(args: argparse.Namespace, prior: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = load_json(args.manifest, default={}) if args.manifest else {}
    manifest_policy_identities = (
        manifest_wallet_policy_identities(manifest) if manifest else {}
    )
    if manifest:
        wallets, positive_slices = _manifest_policy(manifest)
    else:
        wallets = [_norm_wallet(value) for value in args.wallet]
        wallets = [wallet for wallet in wallets if wallet]
        if not wallets:
            raise ValueError("a frozen --manifest or explicit --wallet is required")
        alpha = load_json(args.alpha_report, default={})
        positive_slices = _positive_profile_slices(alpha, wallets)
    now_s = time.time()
    cohort = prior.get("cohort") if isinstance(prior.get("cohort"), dict) else {}
    prior_manifest_id = ((prior.get("manifest") or {}).get("manifest_id"))
    manifest_id = manifest.get("manifest_id") if manifest else None
    if manifest_id and prior_manifest_id != manifest_id:
        effective_s = parse_ts(manifest.get("generated_at")) or now_s
        cohort = {
            "cohort_id": stable_id(
                "wide_cohort",
                {"manifest_id": manifest_id, "run_id": args.run_id},
                length=24,
            ),
            "run_id": args.run_id,
            "run_ids": [args.run_id],
            "started_at_s": round(effective_s, 6),
            "started_at": manifest.get("generated_at") or utc_now_iso(),
            "source": "direct_polygon_ws_only",
            "max_fill_lag_s": 5.0,
            "manifest_id": manifest_id,
        }
        prior = {
            **prior,
            "cohort": cohort,
            "orders": [
                row for row in prior.get("orders") or []
                if isinstance(row, dict) and row.get("cohort_id") == cohort["cohort_id"]
            ],
            "wallets": {},
            "attempt_ids": [],
            "attempt_terminals": [],
            "terminal_source_ids": [],
            "observed_source_ids": [],
        }
    if not cohort:
        cohort = {
            "cohort_id": args.cohort_id or stable_id("wide_cohort", {"started_at_s": now_s}, length=16),
            "run_id": args.run_id,
            "run_ids": [args.run_id],
            "started_at_s": round(now_s, 6),
            "started_at": utc_now_iso(),
            "source": "direct_polygon_ws_only",
            "max_fill_lag_s": 5.0,
        }
        prior = {**prior, "cohort": cohort}
    elif args.run_id not in set(cohort.get("run_ids") or [cohort.get("run_id")]):
        cohort = {**cohort, "run_ids": [*(cohort.get("run_ids") or [cohort.get("run_id")]), args.run_id]}
        prior = {**prior, "cohort": cohort}
    if manifest and cohort.get("cohort_id"):
        prior = {
            **prior,
            "orders": [
                row for row in prior.get("orders") or []
                if isinstance(row, dict) and row.get("cohort_id") == cohort["cohort_id"]
            ],
        }
    direct_event_json = direct_event_input(args)
    score_fetch_cycle_id = uuid.uuid4().hex
    if direct_event_json:
        decoded = json.loads(direct_event_json)
        raw_rows = [decoded] if isinstance(decoded, dict) else [
            row for row in decoded if isinstance(row, dict)
        ]
    else:
        raw_rows = _iter_recent_jsonl(args.polygon_jsonl, args.scan_limit)
    metadata_merge_summary: dict[str, Any] = {}
    meta = _metadata_for_events(
        args.history_state,
        args.gamma_base_url,
        raw_rows,
        str(getattr(args, "token_metadata_cache", "") or ""),
        metadata_merge_summary,
    )
    attempts: dict[str, int] = {
        wallet: int(((prior.get("wallets") or {}).get(wallet) or {}).get("attempted_exact_policy_buys") or 0)
        for wallet in wallets
    }
    refusals: dict[str, Counter[str]] = {
        wallet: Counter(((prior.get("wallets") or {}).get(wallet) or {}).get("refusal_counts") or {})
        for wallet in wallets
    }
    attempted_ids = set(prior.get("attempt_ids") or [])
    terminal_ids = set(prior.get("terminal_source_ids") or [])
    observed_ids = set(prior.get("observed_source_ids") or [])
    observed_ids_at_cycle_start = set(observed_ids)
    cycle_wallet_buy_rows = 0
    cycle_unique_attempt_ids: set[str] = set()
    cycle_duplicate_attempt_rows = 0
    cycle_terminal_attempt_ids: set[str] = set()
    new_orders: list[dict[str, Any]] = []
    clob = CLOBMarketClient(host=args.clob_base_url, timeout_s=args.clob_timeout_s)
    row_prefetch: dict[int, dict[str, Any]] = {}
    reconcile_batch_prefetch: dict[str, dict[str, Any]] = {}
    if direct_event_json:
        row_prefetch = {
            id(raw): dict(raw["_direct_book_prefetch"])
            for raw in raw_rows
            if (normalized := _normalized_realtime_event(raw))
            and normalized.get("source") == "polygon_ws"
            and isinstance(raw.get("_direct_book_prefetch"), dict)
            and raw["_direct_book_prefetch"].get("fetch_started_monotonic_s") is not None
        }
        for prefetch in row_prefetch.values():
            prefetch.setdefault("fetch_provenance", "capture_prefetched")
            prefetch.setdefault("fetch_cycle_id", score_fetch_cycle_id)
        token_ids = sorted(
            {
                str(normalized["asset"])
                for raw in raw_rows
                if (normalized := _normalized_realtime_event(raw))
                and normalized.get("source") == "polygon_ws"
                and id(raw) not in row_prefetch
            }
        )

        reconcile_fetch_cycle_id = uuid.uuid4().hex

        def fetch_one(token_id: str) -> tuple[str, dict[str, Any]]:
            started_mono = time.monotonic()
            try:
                book = clob.get_book(token_id)
                return token_id, {
                    "book": book,
                    "fetch_started_monotonic_s": started_mono,
                    "fetch_cycle_id": reconcile_fetch_cycle_id,
                    "fetch_provenance": "reconcile_batch_fetched",
                    "book_fetch_ms": round((time.monotonic() - started_mono) * 1000.0, 3),
                    "error": None,
                }
            except Exception as exc:
                return token_id, {
                    "book": None,
                    "fetch_started_monotonic_s": started_mono,
                    "fetch_cycle_id": reconcile_fetch_cycle_id,
                    "fetch_provenance": "reconcile_batch_fetched",
                    "book_fetch_ms": round((time.monotonic() - started_mono) * 1000.0, 3),
                    "error": f"{type(exc).__name__}: {exc}",
                }

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, min(8, len(token_ids)))
        ) as pool:
            reconcile_batch_prefetch.update(dict(pool.map(fetch_one, token_ids)))

    def resolve_prefetch(raw: dict[str, Any], token_id: str) -> dict[str, Any]:
        return dict(
            row_prefetch.get(id(raw))
            or reconcile_batch_prefetch.get(token_id, {})
        )
    token_event_ordinals: Counter[tuple[str, str]] = Counter()
    raw_event_ordinals: dict[int, int] = {}
    token_cycle_first_receipts: dict[tuple[str, str], float] = {}
    for raw in raw_rows:
        normalized = _normalized_realtime_event(raw)
        if not normalized or normalized.get("source") != "polygon_ws":
            continue
        token_id = str(normalized["asset"])
        prefetch = resolve_prefetch(raw, token_id)
        fetch_cycle_id = str(
            prefetch.get("fetch_cycle_id") or score_fetch_cycle_id
        )
        ordinal_key = (fetch_cycle_id, token_id)
        raw_event_ordinals[id(raw)] = int(token_event_ordinals[ordinal_key])
        token_event_ordinals[ordinal_key] += 1
        receipt_monotonic_s = num(raw.get("recv_monotonic_s"))
        prior_first = token_cycle_first_receipts.get(ordinal_key)
        token_cycle_first_receipts[ordinal_key] = (
            receipt_monotonic_s
            if prior_first is None
            else min(prior_first, receipt_monotonic_s)
        )
    all_prior_terminals = [
        row
        for row in prior.get("attempt_terminals") or []
        if isinstance(row, dict)
        and str(row.get("cohort_id") or "") == str(cohort.get("cohort_id") or "")
    ]
    quarantined_v3 = [
        row for row in all_prior_terminals
        if int(row.get("fetch_instrumentation_schema_version") or 0) == 3
    ]
    prior_terminals = [
        row for row in all_prior_terminals
        if int(row.get("fetch_instrumentation_schema_version") or 0) != 3
    ]
    quarantined_ids = {str(row.get("attempt_id") or "") for row in quarantined_v3}
    observed_ids.difference_update(quarantined_ids)
    observed_ids_at_cycle_start.difference_update(quarantined_ids)
    terminal_ids.difference_update(quarantined_ids)
    attempted_ids.difference_update(quarantined_ids)
    terminal_by_id = {
        str(row.get("attempt_id") or ""): row
        for row in prior_terminals
        if row.get("attempt_id")
    }
    prior_metadata_missing_by_wallet: Counter[str] = Counter(
        str(row.get("wallet") or "")
        for row in prior_terminals
        if str((row.get("f1_f4_terminal") or {}).get("terminal") or "")
        == "REFUSED_METADATA_MISSING"
    )
    metadata_recovery_ids: set[str] = set()
    metadata_retry_requested = 0
    metadata_retry_source_rows = 0
    if bool(getattr(args, "retry_metadata_misses", False)) and not direct_event_json:
        recoverable_terminals = {
            attempt_id: terminal
            for attempt_id, terminal in terminal_by_id.items()
            if str((terminal.get("f1_f4_terminal") or {}).get("terminal") or "")
            == "REFUSED_METADATA_MISSING"
            and all(
                (meta.get(str(terminal.get("token_id") or "")) or {}).get(key)
                for key in ("market_slug", "condition_id", "outcome")
            )
        }
        metadata_retry_requested = len(recoverable_terminals)
        retry_rows = _metadata_retry_rows(
            args.polygon_jsonl,
            token_ids={
                str(row.get("token_id") or "")
                for row in recoverable_terminals.values()
                if row.get("token_id")
            },
            desired_attempt_ids=set(recoverable_terminals),
            cohort_id=str(cohort["cohort_id"]),
            run_id=args.run_id,
            wallets=set(wallets),
        )
        metadata_retry_source_rows = len(retry_rows)
        raw_rows.extend(retry_rows)

    def record_terminal(
        *,
        attempt_id: str,
        wallet: str,
        normalized: dict[str, Any],
        terminal: str,
        f1: str,
        f2: str,
        f3: str,
        f4: str,
        receipt_to_fetch_ms: float | None = None,
        book_fetch_ms: float | None = None,
        route_fingerprint: str | None = None,
        fetch_provenance: str = "no_prefetch_entry",
        fetch_cycle_id: str | None = None,
        fetch_started_monotonic_s: float | None = None,
        fetch_started_monotonic_observed: bool = False,
        recv_monotonic_s: float | None = None,
        fanout_received_monotonic_s: float | None = None,
        upstream_to_fanout_ms: float | None = None,
        fanout_to_fetch_ms: float | None = None,
        prefetch_queue_wait_ms: float | None = None,
        prefetch_worker_queue_ms: float | None = None,
        prefetch_network_ms: float | None = None,
        prefetch_parse_ms: float | None = None,
        fetch_pairing_check: str | None = None,
        capture_to_decision_ms: float | None = None,
        token_cycle_first_recv_monotonic_s: float | None = None,
        token_event_ordinal_in_cycle: int | None = None,
    ) -> None:
        allowed_provenance = {
            "capture_prefetched",
            "reconcile_batch_fetched",
            "no_prefetch_entry",
        }
        if fetch_provenance not in allowed_provenance:
            raise ValueError(f"unknown fetch provenance: {fetch_provenance}")
        terminal_row = {
            "schema_version": 1,
            "attempt_id": attempt_id,
            "run_id": args.run_id,
            "cohort_id": cohort["cohort_id"],
            "manifest_id": manifest_id,
            "wallet": wallet,
            "transaction_hash": normalized.get("tx"),
            "source_event_id": normalized.get("source_event_id"),
            "token_id": normalized.get("asset"),
            "receipt_to_fetch_ms": receipt_to_fetch_ms,
            "book_fetch_ms": book_fetch_ms,
            "route_fingerprint": route_fingerprint,
            "fetch_instrumentation_schema_version": 4,
            "fetch_provenance": fetch_provenance,
            "fetch_cycle_id": fetch_cycle_id or score_fetch_cycle_id,
            "fetch_started_monotonic_s": fetch_started_monotonic_s,
            "fetch_started_monotonic_observed": bool(
                fetch_started_monotonic_observed
            ),
            "recv_monotonic_s": recv_monotonic_s,
            "fanout_received_monotonic_s": fanout_received_monotonic_s,
            "upstream_to_fanout_ms": upstream_to_fanout_ms,
            "fanout_to_fetch_ms": fanout_to_fetch_ms,
            "prefetch_queue_wait_ms": prefetch_queue_wait_ms,
            "prefetch_worker_queue_ms": prefetch_worker_queue_ms,
            "prefetch_network_ms": prefetch_network_ms,
            "prefetch_parse_ms": prefetch_parse_ms,
            "fetch_pairing_check": fetch_pairing_check,
            "capture_to_decision_ms": capture_to_decision_ms,
            "token_cycle_first_recv_monotonic_s": (
                token_cycle_first_recv_monotonic_s
            ),
            "token_event_ordinal_in_cycle": token_event_ordinal_in_cycle,
            "f1_f4_terminal": {
                "F1_metadata": f1,
                "F2_alpha_profile": f2,
                "F3_receipt_freshness": f3,
                "F4_executable_book": f4,
                "terminal": terminal,
            },
            "recorded_at": utc_now_iso(),
        }
        terminal_by_id[attempt_id] = terminal_row
        cycle_terminal_attempt_ids.add(attempt_id)
        instrumentation_path = str(
            getattr(args, "f3_instrumentation_jsonl", "") or ""
        )
        instrumentation_run_prefix = str(
            getattr(args, "f3_instrumentation_run_prefix", "wide_")
            or ""
        )
        if (
            f2 == "PASS"
            and instrumentation_path
            and str(args.run_id).startswith(instrumentation_run_prefix)
        ):
            append_jsonl_many(instrumentation_path, [terminal_row])

    for row in raw_rows:
        normalized = _normalized_realtime_event(row)
        if not normalized or normalized.get("source") != "polygon_ws":
            continue
        if num(normalized.get("received_at_s")) + 1e-9 < num(cohort.get("started_at_s")):
            continue
        for wallet, side in _wallet_sides(row, set(wallets)):
            if side != "BUY":
                continue
            cycle_wallet_buy_rows += 1
            token_id = str(normalized["asset"])
            token_meta = meta.get(token_id) if isinstance(meta.get(token_id), dict) else {}
            attempt_id = _order_identity(str(cohort["cohort_id"]), args.run_id, wallet, normalized)
            if attempt_id in observed_ids_at_cycle_start or attempt_id in cycle_unique_attempt_ids:
                cycle_duplicate_attempt_rows += 1
            else:
                cycle_unique_attempt_ids.add(attempt_id)
            prior_terminal = terminal_by_id.get(attempt_id, {})
            prior_terminal_name = str(
                (prior_terminal.get("f1_f4_terminal") or {}).get("terminal") or ""
            )
            metadata_now_complete = bool(
                token_meta.get("market_slug")
                and token_meta.get("condition_id")
                and token_meta.get("outcome")
            )
            recovering_metadata = bool(
                attempt_id in terminal_ids
                and prior_terminal_name == "REFUSED_METADATA_MISSING"
                and metadata_now_complete
            )
            if attempt_id in terminal_ids and not recovering_metadata:
                continue
            if recovering_metadata:
                terminal_ids.discard(attempt_id)
                metadata_recovery_ids.add(attempt_id)
                refusals[wallet]["metadata_missing"] = max(
                    0, int(refusals[wallet]["metadata_missing"]) - 1
                )
            first_observation = attempt_id not in observed_ids
            observed_ids.add(attempt_id)
            if not token_meta.get("market_slug") or not token_meta.get("condition_id") or not token_meta.get("outcome"):
                if first_observation:
                    refusals[wallet]["metadata_missing"] += 1
                if direct_event_json:
                    record_terminal(
                        attempt_id=attempt_id, wallet=wallet, normalized=normalized,
                        terminal="REFUSED_METADATA_MISSING",
                        f1="REFUSED_METADATA_MISSING", f2="NOT_EVALUATED",
                        f3="PASS", f4="NOT_EVALUATED",
                    )
                    terminal_ids.add(attempt_id)
                continue
            slice_row = btc_5m_move_slice_for_values(
                market_slug=str(token_meta["market_slug"]),
                event_ts=num(normalized.get("event_ts")),
                price=num(normalized.get("price")),
            )
            if slice_row["move_slice_key"] not in positive_slices.get(wallet, set()):
                refusals[wallet]["alpha_profile_filter"] += 1
                record_terminal(
                    attempt_id=attempt_id, wallet=wallet, normalized=normalized,
                    terminal="REFUSED_ALPHA_PROFILE_FILTER",
                    f1="PASS", f2="REFUSED_ALPHA_PROFILE_FILTER",
                    f3="PASS", f4="NOT_EVALUATED",
                )
                terminal_ids.add(attempt_id)
                continue
            if attempt_id not in attempted_ids:
                attempts[wallet] += 1
                attempted_ids.add(attempt_id)
            prefetch = resolve_prefetch(row, token_id)
            fetch_cycle_id = str(
                prefetch.get("fetch_cycle_id") or score_fetch_cycle_id
            )
            ordinal_key = (fetch_cycle_id, token_id)
            if id(row) not in raw_event_ordinals:
                raw_event_ordinals[id(row)] = int(
                    token_event_ordinals[ordinal_key]
                )
                token_event_ordinals[ordinal_key] += 1
                receipt_value = num(row.get("recv_monotonic_s"))
                prior_first = token_cycle_first_receipts.get(ordinal_key)
                token_cycle_first_receipts[ordinal_key] = (
                    receipt_value
                    if prior_first is None
                    else min(prior_first, receipt_value)
                )
            token_event_ordinal = raw_event_ordinals[id(row)]
            receipt_monotonic_s = num(row.get("recv_monotonic_s"))
            fetch_terminal_fields = terminal_fetch_context(
                prefetch=prefetch,
                raw_row=row,
                default_cycle_id=score_fetch_cycle_id,
                token_event_ordinal_in_cycle=token_event_ordinal,
                token_cycle_first_recv_monotonic_s=(
                    token_cycle_first_receipts[ordinal_key]
                ),
            )
            fetch_started_s = time.time()
            fetch_started_monotonic_s = num(
                fetch_terminal_fields["fetch_started_monotonic_s"],
                time.monotonic(),
            )
            lag_s = (
                max(0.0, fetch_started_monotonic_s - receipt_monotonic_s)
                if direct_event_json and receipt_monotonic_s > 0
                else max(0.0, fetch_started_s - num(normalized.get("received_at_s")))
            )
            if lag_s > 5.0:
                upstream_delay = num(fetch_terminal_fields.get("upstream_to_fanout_ms"))
                paper_delay = num(fetch_terminal_fields.get("fanout_to_fetch_ms"))
                if (
                    fetch_terminal_fields.get("fetch_provenance")
                    == "capture_prefetched"
                    and upstream_delay > 5000.0
                    and paper_delay <= 5000.0
                ):
                    refusal_name = "upstream_fanout_delay_gt_5s"
                    terminal_name = "REFUSED_UPSTREAM_FANOUT_DELAY_GT_5S"
                elif paper_delay > 5000.0:
                    refusal_name = "paper_prefetch_delay_gt_5s"
                    terminal_name = "REFUSED_PAPER_PREFETCH_DELAY_GT_5S"
                else:
                    refusal_name = "stale_receipt_to_fetch"
                    terminal_name = "REFUSED_STALE_RECEIPT_TO_FETCH"
                refusals[wallet][refusal_name] += 1
                record_terminal(
                    attempt_id=attempt_id, wallet=wallet, normalized=normalized,
                    terminal=terminal_name,
                    f1="PASS", f2="PASS", f3="REFUSED_GT_5S",
                    f4="NOT_EVALUATED", receipt_to_fetch_ms=round(lag_s * 1000.0, 3),
                    book_fetch_ms=prefetch.get("book_fetch_ms"),
                    **fetch_terminal_fields,
                )
                terminal_ids.add(attempt_id)
                continue
            copy_size = max(
                1.0,
                _copy_size_usd(
                    num(normalized["price"]),
                    num(normalized["size"]),
                    wallet_fraction=0.1,
                    max_order_usd=1.0,
                ),
            )
            try:
                if direct_event_json:
                    if prefetch.get("error"):
                        raise RuntimeError(str(prefetch["error"]))
                    book = prefetch.get("book")
                    book_fetch_ms = num(prefetch.get("book_fetch_ms"))
                else:
                    book_fetch_started_monotonic_s = time.monotonic()
                    book = clob.get_book(token_id)
                    book_fetch_ms = round(
                        (time.monotonic() - book_fetch_started_monotonic_s) * 1000.0,
                        3,
                    )
            except Exception:
                if first_observation:
                    refusals[wallet]["book_fetch_error"] += 1
                if direct_event_json:
                    record_terminal(
                        attempt_id=attempt_id, wallet=wallet, normalized=normalized,
                        terminal="REFUSED_BOOK_FETCH_ERROR",
                        f1="PASS", f2="PASS", f3="PASS",
                        f4="REFUSED_BOOK_FETCH_ERROR",
                        receipt_to_fetch_ms=round(lag_s * 1000.0, 3),
                        book_fetch_ms=prefetch.get("book_fetch_ms"),
                        **fetch_terminal_fields,
                    )
                    terminal_ids.add(attempt_id)
                continue
            top = _book_top_of_book(book)
            route_fingerprint = (
                ((book or {}).get("__walletCopyClobRouteReport") or {}).get(
                    "request_fingerprint"
                )
                if isinstance(book, dict)
                else None
            )
            if _book_unavailable_or_market_closed(top):
                refusals[wallet]["book_unavailable_or_closed"] += 1
                record_terminal(
                    attempt_id=attempt_id, wallet=wallet, normalized=normalized,
                    terminal="REFUSED_BOOK_UNAVAILABLE_OR_CLOSED",
                    f1="PASS", f2="PASS", f3="PASS",
                    f4="REFUSED_BOOK_UNAVAILABLE_OR_CLOSED",
                    receipt_to_fetch_ms=round(lag_s * 1000.0, 3),
                    book_fetch_ms=book_fetch_ms,
                    route_fingerprint=route_fingerprint,
                    **fetch_terminal_fields,
                )
                terminal_ids.add(attempt_id)
                continue
            scored = CLOBMarketClient.summarize_book(
                book,
                copy_size_usd=copy_size,
                source_price=num(normalized["price"]),
                max_slippage_bps=250.0,
            )
            if scored.get("instant_fill_status") != "PASS" or num(scored.get("fill_ratio")) < 0.999:
                refusals[wallet][str(scored.get("blocking_reason") or "not_fillable")] += 1
                record_terminal(
                    attempt_id=attempt_id, wallet=wallet, normalized=normalized,
                    terminal=f"REFUSED_{str(scored.get('blocking_reason') or 'NOT_FILLABLE').upper()}",
                    f1="PASS", f2="PASS", f3="PASS",
                    f4=f"REFUSED_{str(scored.get('blocking_reason') or 'NOT_FILLABLE').upper()}",
                    receipt_to_fetch_ms=round(lag_s * 1000.0, 3),
                    book_fetch_ms=book_fetch_ms,
                    route_fingerprint=route_fingerprint,
                    **fetch_terminal_fields,
                )
                terminal_ids.add(attempt_id)
                continue
            filled_shares = num(scored.get("fillable_shares"))
            fill_price = num(scored.get("avg_fill_price"))
            filled_cost = num(scored.get("fillable_usd"), filled_shares * fill_price)
            new_orders.append(
                {
                    "schema_version": 1,
                    "order_id": attempt_id,
                    "recorded_at": utc_now_iso(),
                    "cohort_id": cohort["cohort_id"],
                    "run_id": args.run_id,
                    "policy_id": POLICY_ID,
                    "wide_policy_fingerprint": (
                        manifest_policy_identities.get(wallet) or {}
                    ).get("wide_policy_fingerprint"),
                    "wide_policy_identity": manifest_policy_identities.get(wallet),
                    "wallet": wallet,
                    "source": "polygon_ws",
                    "transaction_hash": normalized["tx"],
                    "log_index": normalized["source_event_id"],
                    "source_event_ts": num(normalized.get("event_ts")),
                    "source_received_at_s": num(normalized.get("received_at_s")),
                    "book_fetch_started_at_s": round(fetch_started_s, 6),
                    "receipt_to_book_fetch_lag_s": round(lag_s, 6),
                    "receipt_to_fetch_ms": round(lag_s * 1000.0, 3),
                    "book_fetch_ms": book_fetch_ms,
                    "route_fingerprint": (
                        ((book or {}).get("__walletCopyClobRouteReport") or {}).get(
                            "request_fingerprint"
                        )
                        if isinstance(book, dict)
                        else None
                    ),
                    "f1_f4_terminal": {
                        "F1_metadata": "PASS",
                        "F2_alpha_profile": "PASS",
                        "F3_receipt_freshness": "PASS",
                        "F4_executable_book": "PASS",
                        "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
                    },
                    "book_timestamp": top.get("book_timestamp"),
                    "book_hash": top.get("book_hash"),
                    "condition_id": token_meta["condition_id"],
                    "market_slug": token_meta["market_slug"],
                    "token_id": token_id,
                    "outcome": token_meta["outcome"],
                    "source_price": round(num(normalized["price"]), 9),
                    "source_shares": round(num(normalized["size"]), 6),
                    "fill_price": round(fill_price, 9),
                    "filled_shares": round(filled_shares, 6),
                    "filled_cost_usd": round(filled_cost, 6),
                    "alpha_move_slice": slice_row,
                    "resolved": False,
                    "expected_fee_usd": None,
                    "pre_fee_pnl_usd": None,
                    "post_fee_pnl_usd": None,
                }
            )
            record_terminal(
                attempt_id=attempt_id, wallet=wallet, normalized=normalized,
                terminal="COPYABLE_EXACT_POLICY_PAPER_FILL",
                f1="PASS", f2="PASS", f3="PASS", f4="PASS",
                receipt_to_fetch_ms=round(lag_s * 1000.0, 3),
                book_fetch_ms=book_fetch_ms,
                route_fingerprint=route_fingerprint,
                **fetch_terminal_fields,
            )
            terminal_ids.add(attempt_id)
    state, events = reconcile_state(
        prior,
        new_orders=new_orders,
        resolutions=_jsonl(args.resolutions),
        attempts=attempts,
        refusals=refusals,
        wallet_order=wallets,
    )
    state["summary"].update(metadata_merge_summary)
    state["summary"]["quarantined_fetch_instrumentation_v3_rows"] = (
        int((prior.get("summary") or {}).get(
            "quarantined_fetch_instrumentation_v3_rows"
        ) or 0)
        + len(quarantined_v3)
    )
    state["summary"]["fetch_estimate_schema"] = 4
    state["attempt_ids"] = sorted(attempted_ids)
    state["terminal_source_ids"] = sorted(terminal_ids)
    state["observed_source_ids"] = sorted(observed_ids)
    state["attempt_terminals"] = sorted(
        terminal_by_id.values(),
        key=lambda row: (str(row.get("recorded_at") or ""), str(row.get("attempt_id") or "")),
    )[-100_000:]
    state["terminal_reconciliation"] = {
        "run_id": args.run_id,
        "cohort_id": cohort["cohort_id"],
        "input_rows": len(observed_ids),
        "terminal_rows": len(terminal_by_id),
        "input_equals_terminal": len(observed_ids) == len(terminal_by_id),
        "direct_event_handoff": bool(direct_event_json),
        "scope": "generation_local_tx_hash_log_index_wallet_buy",
    }
    normalized_polygon_rows = sum(
        1
        for raw in raw_rows
        if (normalized := _normalized_realtime_event(raw))
        and normalized.get("source") == "polygon_ws"
    )
    terminal_joined_rows = len(cycle_unique_attempt_ids & cycle_terminal_attempt_ids)
    if len(raw_rows) == 0:
        empty_stage = "FETCH_ZERO"
    elif normalized_polygon_rows == 0 or cycle_wallet_buy_rows == 0:
        empty_stage = "FILTER_ZERO"
    elif len(cycle_unique_attempt_ids) == 0:
        empty_stage = "DEDUPE_ZERO"
    elif terminal_joined_rows == 0:
        empty_stage = "TERMINAL_JOIN_ZERO"
    else:
        empty_stage = None
    state["generation_flow"] = {
        "schema_version": 1,
        "direct_event_handoff": bool(direct_event_json),
        "empty_stage": empty_stage,
        "empty_generation": terminal_joined_rows == 0,
        "stages": {
            "fetch": {
                "input_direct_rows": len(raw_rows),
                "output_rows": len(raw_rows),
            },
            "filter": {
                "input_rows": len(raw_rows),
                "normalized_polygon_rows": normalized_polygon_rows,
                "output_wallet_buy_rows": cycle_wallet_buy_rows,
            },
            "dedupe": {
                "input_wallet_buy_rows": cycle_wallet_buy_rows,
                "output_unique_attempt_rows": len(cycle_unique_attempt_ids),
                "duplicate_attempt_rows": cycle_duplicate_attempt_rows,
            },
            "terminal_join": {
                "input_unique_attempt_rows": len(cycle_unique_attempt_ids),
                "output_terminal_rows": terminal_joined_rows,
                "unjoined_attempt_rows": max(
                    0, len(cycle_unique_attempt_ids) - terminal_joined_rows
                ),
            },
        },
        "rule": (
            "first zero output across fetch, wallet-BUY filter, immutable-attempt "
            "dedupe, and terminal join names the empty-generation source"
        ),
    }
    metadata_recovery_outcomes = Counter(
        str(
            (
                terminal_by_id.get(attempt_id, {}).get("f1_f4_terminal")
                or {}
            ).get("terminal")
            or "MISSING_TERMINAL"
        )
        for attempt_id in metadata_recovery_ids
    )
    recovered_by_wallet: Counter[str] = Counter(
        str(terminal_by_id.get(attempt_id, {}).get("wallet") or "")
        for attempt_id in metadata_recovery_ids
    )
    state["metadata_recovery"] = {
        "prior_metadata_missing_rows": sum(prior_metadata_missing_by_wallet.values()),
        "retry_requested_rows": metadata_retry_requested,
        "retry_source_rows": metadata_retry_source_rows,
        "reprocessed_rows": len(metadata_recovery_ids),
        "copyable_delta": int(
            metadata_recovery_outcomes.get("COPYABLE_EXACT_POLICY_PAPER_FILL") or 0
        ),
        "outcome_taxonomy": dict(sorted(metadata_recovery_outcomes.items())),
        "per_wallet": {
            wallet: {
                "prior_metadata_missing_rows": int(
                    prior_metadata_missing_by_wallet.get(wallet) or 0
                ),
                "reprocessed_rows": int(recovered_by_wallet.get(wallet) or 0),
            }
            for wallet in sorted(
                set(prior_metadata_missing_by_wallet) | set(recovered_by_wallet)
            )
            if wallet
        },
        "rule": (
            "a previously terminal metadata miss is re-evaluated exactly once "
            "when the cache later contains market_slug, condition_id, and outcome"
        ),
    }
    if direct_event_json:
        for terminal in terminal_by_id.values():
            event = {
                "event": "wide_exact_policy_attempt_terminal",
                "order_id": terminal["attempt_id"],
                **terminal,
            }
            event["event_id"] = stable_id(
                "wideevent",
                {"event": event["event"], "order_id": event["order_id"]},
                length=32,
            )
            events.append(event)
    state["manifest"] = {
        "manifest_id": manifest.get("manifest_id"),
        "manifest_path": args.manifest,
        "source_alpha_report": manifest.get("source_alpha_report"),
        "source_alpha_status": manifest.get("source_alpha_status"),
        "source_alpha_age_h": manifest.get("source_alpha_age_h"),
        "effective_at": manifest.get("effective_at"),
        "admitted_wallet_count": len(wallets),
        "wallet_policy_identities": manifest_policy_identities,
    } if manifest else {}
    return state, events


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cohort-id", default="")
    parser.add_argument("--wallet", action="append", default=[])
    parser.add_argument("--manifest", default="")
    parser.add_argument("--manifest-pointer", default=DEFAULT_MANIFEST_POINTER)
    parser.add_argument("--polygon-jsonl", required=True)
    parser.add_argument("--alpha-report", required=True)
    parser.add_argument("--history-state", default=DEFAULT_HISTORY)
    parser.add_argument(
        "--token-metadata-cache",
        default="data/research/wide_token_metadata_cache.json",
    )
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument(
        "--f3-instrumentation-jsonl",
        default=DEFAULT_F3_INSTRUMENTATION,
    )
    parser.add_argument(
        "--f3-instrumentation-run-prefix",
        default="wide_",
    )
    parser.add_argument("--gamma-base-url", default="http://127.0.0.1:8787/gamma-api")
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.5)
    parser.add_argument("--scan-limit", type=int, default=250_000)
    parser.add_argument("--direct-event-json", default="")
    parser.add_argument("--direct-event-file", default="")
    parser.add_argument(
        "--retry-metadata-misses",
        action="store_true",
        help=(
            "one-shot full-source repair of terminal metadata misses whose "
            "token metadata is now present in the cache"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _load_fresh_alpha_report(args.alpha_report)
    if not args.manifest and not args.wallet:
        args.manifest = resolve_manifest_pointer(args.manifest_pointer)
    if args.manifest:
        manifest = load_json(args.manifest, default={})
        if not isinstance(manifest, dict) or not manifest:
            raise ValueError(f"MANIFEST_MISSING_REFUSED path={args.manifest}")
        _require_fresh_manifest_alpha(manifest, manifest_path=args.manifest)
    prior = load_json(args.state, default={})
    ledger_rows = _jsonl(args.ledger)
    ledger_orders, ledger_event_ids = replay_ledger(ledger_rows)
    prior = {**(prior if isinstance(prior, dict) else {}), "orders": ledger_orders}
    state, events = score_cycle(args, prior if isinstance(prior, dict) else {})
    new_events = [row for row in events if row.get("event_id") not in ledger_event_ids]
    if new_events:
        append_jsonl_many(args.ledger, new_events)
    atomic_write_json(args.state, state)
    print(json.dumps({"state": args.state, "ledger": args.ledger, "appended_events": len(new_events), **state["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
