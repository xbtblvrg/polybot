#!/usr/bin/env python3
"""Current freshness gate for the deadman micro-probe order.

Flow stage: LIVE/ROTATE. Read-only probe for Fable direction
2026-07-11T18:57Z. It does not edit live roster or submit orders.
"""

from __future__ import annotations

import argparse
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
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import WalletEvent, WalletSpec, num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from scripts.run_wallet_copy_live_guard import _temporal_slice_exclusion  # noqa: E402


DIRECTION_ID = "2026-07-11T18:57Z-fable-deadman-freshness-gated-microprobe"
DEFAULT_OUTPUT = "data/research/deadman_microprobe_freshness_gate_latest.json"
DEFAULT_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_CLEARANCE = "data/research/wallet_copy_queue_clearance_gaps.json"
DEFAULT_ROTATION = "data/research/wallet_copy_promotion_rotation_state.json"
DEFAULT_PRIOR_ABORT_POSTMORTEM = "data/research/d918_5960_abort_postmortem_20260710T0606Z.json"
DEFAULT_TEMPORAL_PROFITABILITY = "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_REQUIRED_WALLETS = (
    "0x19729634ac5ffcd658f0847b9e8cf7c026a95821",
    "0x23c72ed89eac711b06689cd9a85f8e4fb3d68266",
)


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _num(value: Any, default: float = 0.0) -> float:
    return num(value, default)


def _iso_from_ts(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _index_by_wallet(rows: list[Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet and wallet not in out:
            out[wallet] = row
    return out


def _prior_live_aborts_by_wallet(path: Path) -> dict[str, dict[str, Any]]:
    payload = load_json(path, default={})
    packets = payload.get("accepted_abort_packets") if isinstance(payload, dict) else {}
    packets = packets if isinstance(packets, dict) else {}
    output: dict[str, dict[str, Any]] = {}
    for packet in packets.values():
        if not isinstance(packet, dict):
            continue
        wallet = _norm_wallet(packet.get("source_wallet"))
        if not wallet:
            continue
        output[wallet] = {
            "candidate_id": packet.get("candidate_id"),
            "ts": packet.get("pinned_start") or packet.get("pinned_start_at"),
            "pnl": packet.get("worse_of_pnl_usd")
            if packet.get("worse_of_pnl_usd") is not None
            else packet.get("canonical_pnl_usd")
            if packet.get("canonical_pnl_usd") is not None
            else packet.get("pnl_usd"),
            "pnl_usd": packet.get("canonical_pnl_usd") if packet.get("canonical_pnl_usd") is not None else packet.get("pnl_usd"),
            "worse_of_pnl_usd": packet.get("worse_of_pnl_usd"),
            "resolved_fills": packet.get("resolved_fills"),
            "fills": packet.get("fills"),
            "rule": packet.get("abort_rule"),
            "source": str(path),
        }
    return output


def _candidate_wallets(
    *,
    required_wallets: list[str],
    rotation: dict[str, Any],
    queue: dict[str, Any],
    max_candidates: int,
) -> list[str]:
    wallets: list[str] = []
    seen: set[str] = set()

    def add(raw: Any) -> None:
        wallet = _norm_wallet(raw)
        if wallet and wallet not in seen and len(wallets) < max_candidates:
            wallets.append(wallet)
            seen.add(wallet)

    for wallet in required_wallets:
        add(wallet)

    paper = rotation.get("paper_promotion") if isinstance(rotation.get("paper_promotion"), dict) else {}
    for row in paper.get("candidates") or []:
        if isinstance(row, dict):
            add(row.get("wallet"))

    for row in queue.get("ranked_members") or []:
        if not isinstance(row, dict):
            continue
        if row.get("clearance_ready") or row.get("ready_for_live"):
            add(row.get("wallet"))

    return wallets


def _fetch_events(wallet: str, args: argparse.Namespace, *, now_s: float) -> tuple[list[WalletEvent], dict[str, Any]]:
    client = WalletHistoryClient(
        WalletSpec(name=f"deadman_probe_{wallet[-8:]}", address=wallet),
        timeout_s=float(args.timeout_s),
        retries=int(args.retries),
    )
    events: list[WalletEvent] = []
    raw_rows = 0
    errors: list[str] = []
    for page in range(max(1, int(args.pages))):
        offset = page * int(args.limit)
        try:
            rows = client.fetch_trades(limit=int(args.limit), offset=offset)
        except Exception as exc:  # pragma: no cover - live network path.
            errors.append(f"{type(exc).__name__}: {exc}")
            break
        raw_rows += len(rows)
        for raw in rows:
            event = normalize_polymarket_wallet_row(raw, spec=client.spec, row_type="trade", observed_ts=now_s)
            if event is not None and event.event_ts is not None:
                events.append(event)
        if len(rows) < int(args.limit):
            break
    report = {
        "raw_rows": raw_rows,
        "errors": errors,
        "status": "PASS" if not errors else "ERROR",
        "route_report": client.last_route_report,
    }
    return events, report


def _temporal_registry_basis(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    raw_path = Path(getattr(args, "temporal_profitability", DEFAULT_TEMPORAL_PROFITABILITY))
    path = raw_path if raw_path.is_absolute() else root / raw_path
    payload = load_json(path, default={})
    payload = payload if isinstance(payload, dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    skipped = summary.get("history_skipped") if isinstance(summary.get("history_skipped"), dict) else {}
    return {
        "path": str(path),
        "generated_at": payload.get("generated_at"),
        "kind": payload.get("kind"),
        "events_scanned": skipped.get("events_scanned"),
        "duplicates": skipped.get("duplicates"),
        "unresolved": skipped.get("unresolved"),
        "wallets_total": summary.get("wallets_total"),
        "wallet_rows_emitted": summary.get("wallet_rows_emitted"),
        "slice_label_counts": summary.get("slice_label_counts"),
    }


def _temporal_integrity_check(temporal: dict[str, Any], basis: dict[str, Any] | None, args: argparse.Namespace) -> dict[str, Any]:
    active_slices = [str(item) for item in temporal.get("active_slices") or []]
    evaluated = [item for item in temporal.get("evaluated_slices") or [] if isinstance(item, dict)]
    active_evaluated = [item for item in evaluated if str(item.get("slice") or "") in set(active_slices)]
    active_unproven = [
        {
            "slice": item.get("slice"),
            "label": item.get("label"),
            "resolved_trades": item.get("resolved_trades"),
            "pnl_usd": item.get("pnl_usd"),
            "roi_pct": item.get("roi_pct"),
            "label_reason": item.get("label_reason"),
        }
        for item in active_evaluated
        if str(item.get("label") or "").upper() == "UNPROVEN"
    ]
    reasons: list[str] = []
    events_scanned = _num((basis or {}).get("events_scanned"), 0.0)
    min_events = float(getattr(args, "min_temporal_history_events_scanned", 0.0) or 0.0)
    if min_events > 0 and events_scanned > 0 and events_scanned < min_events:
        reasons.append("temporal_registry_history_events_scanned_below_minimum")
    if bool(getattr(args, "fail_closed_on_active_unproven_temporal_slice", True)) and active_unproven:
        reasons.append("temporal_slice_active_unproven_basis")
    return {
        "pass": not reasons,
        "reasons": reasons,
        "active_slices": active_slices,
        "active_unproven_slices": active_unproven,
        "basis": basis or {},
        "minimum_history_events_scanned": min_events,
        "fail_closed_on_active_unproven_temporal_slice": bool(
            getattr(args, "fail_closed_on_active_unproven_temporal_slice", True)
        ),
    }


def _copy_size_usd(event: WalletEvent, *, wallet_fraction: float, max_order_usd: float) -> float:
    source_usd = max(0.0, float(event.price) * float(event.size))
    if source_usd <= 0.0:
        source_usd = max(0.0, float(event.usdc_size))
    return round(min(source_usd * max(0.0, wallet_fraction), max(0.0, max_order_usd)), 6)


def _score_current_books(
    events: list[WalletEvent],
    *,
    clob: CLOBMarketClient,
    now_s: float,
    args: argparse.Namespace,
) -> dict[str, Any]:
    book_cache: dict[str, dict[str, Any]] = {}
    scored: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    copyable_6h = 0
    copyable_24h = 0
    for event in sorted(events, key=lambda item: item.event_ts or 0.0, reverse=True):
        age_s = now_s - float(event.event_ts or 0.0)
        if age_s < 0 or age_s > 24.0 * 3600.0:
            continue
        if not event.is_buy:
            continue
        if not (float(args.min_price) <= float(event.price) <= float(args.max_price)):
            continue
        if len(scored) >= int(args.max_book_fetches):
            break
        if not event.token_id:
            status = "MISSING_TOKEN"
            status_counts[status] = status_counts.get(status, 0) + 1
            scored.append(
                {
                    "event_id": event.event_id,
                    "event_ts": event.event_ts,
                    "age_s": round(age_s, 6),
                    "market_slug": event.market_slug,
                    "price": event.price,
                    "status": status,
                    "copyable": False,
                }
            )
            continue
        try:
            book = book_cache.get(event.token_id)
            if book is None:
                book = clob.get_book(event.token_id)
                book_cache[event.token_id] = book
            copy_size = _copy_size_usd(
                event,
                wallet_fraction=float(args.wallet_fraction),
                max_order_usd=float(args.probe_max_order_usd),
            )
            summary = CLOBMarketClient.summarize_book(
                book,
                copy_size_usd=copy_size,
                source_price=float(event.price),
                max_slippage_bps=float(args.max_slippage_bps),
            )
            copyable = bool(summary.get("instant_fill_status") == "PASS" and _num(summary.get("fill_ratio")) >= 0.999)
            status = "COPYABLE" if copyable else str(summary.get("blocking_reason") or "NOT_COPYABLE")
            if copyable:
                copyable_24h += 1
                if age_s <= 6.0 * 3600.0:
                    copyable_6h += 1
            status_counts[status] = status_counts.get(status, 0) + 1
            scored.append(
                {
                    "event_id": event.event_id,
                    "event_ts": event.event_ts,
                    "age_s": round(age_s, 6),
                    "market_slug": event.market_slug,
                    "token_id": event.token_id,
                    "price": event.price,
                    "copy_size_usd": copy_size,
                    "status": status,
                    "copyable": copyable,
                    "fill_ratio": summary.get("fill_ratio"),
                    "best_ask": summary.get("best_ask"),
                    "blocking_reason": summary.get("blocking_reason"),
                }
            )
        except Exception as exc:  # pragma: no cover - live network path.
            status = f"BOOK_ERROR_{type(exc).__name__}"
            status_counts[status] = status_counts.get(status, 0) + 1
            scored.append(
                {
                    "event_id": event.event_id,
                    "event_ts": event.event_ts,
                    "age_s": round(age_s, 6),
                    "market_slug": event.market_slug,
                    "token_id": event.token_id,
                    "price": event.price,
                    "status": status,
                    "copyable": False,
                    "error": str(exc)[:500],
                }
            )
    return {
        "basis": "current_clob_book_fetch_at_probe_time_for_recent_dataapi_buys",
        "book_fetches": len(book_cache),
        "max_book_fetches": int(args.max_book_fetches),
        "copyable_buy_count_6h": copyable_6h,
        "copyable_buy_count_24h": copyable_24h,
        "status_counts": dict(sorted(status_counts.items())),
        "scored_events": scored[: int(args.keep_scored_events)],
    }


def _row_for_wallet(
    wallet: str,
    *,
    events: list[WalletEvent],
    fetch_report: dict[str, Any],
    clearance_by_wallet: dict[str, dict[str, Any]],
    queue_by_wallet: dict[str, dict[str, Any]],
    rotation_by_wallet: dict[str, dict[str, Any]],
    clob: CLOBMarketClient,
    now_s: float,
    args: argparse.Namespace,
    prior_abort_by_wallet: dict[str, dict[str, Any]] | None = None,
    temporal_registry_basis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    buys_24h = [
        event
        for event in events
        if event.is_buy and event.event_ts is not None and 0 <= now_s - float(event.event_ts) <= 24.0 * 3600.0
    ]
    buys_6h = [event for event in buys_24h if now_s - float(event.event_ts or 0.0) <= 6.0 * 3600.0]
    inband_24h = [event for event in buys_24h if float(args.min_price) <= float(event.price) <= float(args.max_price)]
    inband_6h = [event for event in buys_6h if float(args.min_price) <= float(event.price) <= float(args.max_price)]
    latest_buy_ts = max((float(event.event_ts or 0.0) for event in buys_24h), default=0.0)
    freshest_buy_lag_s = round(now_s - latest_buy_ts, 6) if latest_buy_ts > 0 else None
    book = (
        {
            "basis": "skipped_for_corrected_joint_gate",
            "book_fetches": 0,
            "copyable_buy_count_6h": None,
            "copyable_buy_count_24h": None,
            "status_counts": {},
            "scored_events": [],
        }
        if bool(getattr(args, "corrected_joint_gate", False))
        else _score_current_books(inband_24h, clob=clob, now_s=now_s, args=args)
    )
    clearance = clearance_by_wallet.get(wallet, {})
    queue = queue_by_wallet.get(wallet, {})
    rotation = rotation_by_wallet.get(wallet, {})
    clearance_metrics = clearance.get("metrics") if isinstance(clearance.get("metrics"), dict) else {}
    queue_replay = queue.get("replay") if isinstance(queue.get("replay"), dict) else {}
    prior_abort = (prior_abort_by_wallet or {}).get(wallet.lower(), {})
    gate_reasons: list[str] = []
    freshness_pass = freshest_buy_lag_s is not None and freshest_buy_lag_s <= float(args.max_freshest_buy_lag_s)
    corrected_prong2_pass = len(inband_24h) >= int(args.min_clob_copyable_buys_24h)
    clob_pass = int(book.get("copyable_buy_count_24h") or 0) >= int(args.min_clob_copyable_buys_24h)
    temporal = _temporal_slice_exclusion(
        {
            "candidate_id": "deadman_microprobe_joint_gate",
            "source_wallet": wallet,
            "policy_id": "deadman_microprobe_0.10_cap_0.5_le_25",
        }
    )
    temporal_integrity = _temporal_integrity_check(temporal, temporal_registry_basis, args)
    temporal_pass = temporal.get("excluded") is not True and bool(temporal_integrity.get("pass"))
    if not freshness_pass:
        gate_reasons.append("freshest_buy_lag_gt_7200_or_no_current_buy")
    if bool(getattr(args, "corrected_joint_gate", False)):
        if not corrected_prong2_pass:
            gate_reasons.append("policy_compatible_inband_buy_count_24h_below_5")
        if not temporal_pass:
            gate_reasons.extend(
                temporal_integrity.get("reasons")
                or [str(temporal.get("reason") or "active_temporal_slice_proven_negative")]
            )
    elif not clob_pass:
        gate_reasons.append("current_clob_backed_copyable_buys_24h_below_5")
    gate_pass = (
        bool(freshness_pass and corrected_prong2_pass and temporal_pass)
        if bool(getattr(args, "corrected_joint_gate", False))
        else bool(freshness_pass and clob_pass)
    )
    gate_payload = {
        "freshness_pass": freshness_pass,
        "clob_copyable_pass": clob_pass,
        "pass": gate_pass,
        "reasons": gate_reasons,
    }
    if bool(getattr(args, "corrected_joint_gate", False)):
        gate_payload.update(
            {
                "corrected_prong2_pass": corrected_prong2_pass,
                "temporal_slice_gate_pass": temporal_pass,
                "temporal_integrity_pass": bool(temporal_integrity.get("pass")),
            }
        )
    return {
        "wallet": wallet,
        "status": fetch_report.get("status"),
        "source": "polymarket_data_api_trades_plus_temporal_joint_gate"
        if bool(getattr(args, "corrected_joint_gate", False))
        else "polymarket_data_api_trades_plus_current_clob_book",
        "fetch": fetch_report,
        "freshness": {
            "latest_buy_ts": latest_buy_ts or None,
            "freshest_buy_lag_s": freshest_buy_lag_s,
            "btc5m_buy_count_6h": len(buys_6h),
            "btc5m_buy_count_24h": len(buys_24h),
            "policy_compatible_inband_buy_count_6h": len(inband_6h),
            "policy_compatible_inband_buy_count_24h": len(inband_24h),
        },
        "current_clob_backed": book,
        "corrected_joint_gate": {
            "enabled": bool(getattr(args, "corrected_joint_gate", False)),
            "freshness_pass": freshness_pass,
            "corrected_prong2_pass": corrected_prong2_pass,
            "temporal_slice_gate_pass": temporal_pass,
            "temporal_slice_gate_reason": temporal.get("reason"),
            "temporal_slice_exclusion": temporal,
            "temporal_integrity": temporal_integrity,
            "policy_compatible_inband_buy_count_24h": len(inband_24h),
            "pass": gate_pass,
            "reasons": gate_reasons,
        },
        "historical_replay_reference_not_gate": {
            "clearance_status": clearance.get("clearance_status") or clearance.get("classification"),
            "clearance_queue_rank": clearance.get("queue_rank"),
            "clearance_copyable_buy_events": clearance_metrics.get("copyable_buy_events"),
            "clearance_candidate_clob_backed_orders": clearance_metrics.get("candidate_clob_backed_orders"),
            "clearance_paper_pnl_usd": clearance_metrics.get("paper_pnl_usd"),
            "clearance_raw_reject_ratio": clearance_metrics.get("raw_reject_ratio"),
            "failure_reasons": clearance.get("failure_reasons"),
            "queue_ready_for_live": queue.get("ready_for_live"),
            "queue_rank": queue.get("queue_rank"),
            "queue_replay_status": queue_replay.get("status"),
            "queue_replay_unresolved_ratio": queue_replay.get("unresolved_ratio"),
            "rotation_rank": rotation.get("rank"),
            "rotation_policy": rotation.get("best_policy_id"),
            "rotation_paper_pnl_usd": rotation.get("best_policy_paper_pnl_usd"),
            "rotation_copyable_buy_events": rotation.get("best_policy_copyable_buy_events"),
        },
        "prior_live_abort": prior_abort or None,
        "gate": gate_payload,
    }


def _decision_for_rows(rows: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    passing = [row for row in rows if (row.get("gate") or {}).get("pass")]
    best = sorted(
        passing,
        key=lambda row: (
            int(((row.get("current_clob_backed") or {}).get("copyable_buy_count_24h") or 0)),
            int(((row.get("freshness") or {}).get("btc5m_buy_count_24h") or 0)),
        ),
        reverse=True,
    )[:1]
    action = "ELIGIBLE_PENDING_FABLE_RULING" if best else "NO_ELIGIBLE_WALLET"
    return {
        "action": action,
        "best_wallet": best[0].get("wallet") if best else "",
        "direction_id": str(args.direction_id),
        "gate": "freshest_buy_lag_s<=7200 AND policy_compatible_inband_buy_count_24h>=5 AND no_active_temporal_slice_PROVEN_NEGATIVE"
        if bool(args.corrected_joint_gate)
        else "freshest_buy_lag_s<=7200 AND current_clob_backed_copyable_buys_24h>=5",
        "probe_terms_if_pass": {
            "mode": "ADD_SECOND_MEMBER",
            "max_order_usd": float(args.probe_max_order_usd),
            "max_price": float(args.max_price_for_probe),
            "total_concurrent_probe_exposure_usd": float(args.total_probe_exposure_usd),
            "auto_demote_realized_pnl_usd_lte": -abs(float(args.auto_demote_loss_usd)),
            "live_orders_allowed": False,
            "script_live_mutation": False,
        },
    }


def build_probe(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    if bool(args.direct_data_api):
        os.environ.pop("POLYMARKET_DATA_API_BASE_URL", None)
    now_s = time.time()
    queue = load_json(root / args.queue, default={})
    clearance = load_json(root / args.clearance, default={})
    rotation = load_json(root / args.rotation, default={})
    queue_rows = queue.get("ranked_members") if isinstance(queue.get("ranked_members"), list) else []
    clearance_rows = clearance.get("candidates") if isinstance(clearance.get("candidates"), list) else []
    paper = rotation.get("paper_promotion") if isinstance(rotation.get("paper_promotion"), dict) else {}
    rotation_rows = paper.get("candidates") if isinstance(paper.get("candidates"), list) else []
    wallets = _candidate_wallets(
        required_wallets=[str(item) for item in args.include_wallet],
        rotation=rotation if isinstance(rotation, dict) else {},
        queue=queue if isinstance(queue, dict) else {},
        max_candidates=int(args.max_candidates),
    )
    clearance_by_wallet = _index_by_wallet(clearance_rows)
    queue_by_wallet = _index_by_wallet(queue_rows)
    rotation_by_wallet = _index_by_wallet(rotation_rows)
    prior_abort_by_wallet = _prior_live_aborts_by_wallet(root / args.prior_abort_postmortem)
    temporal_basis = _temporal_registry_basis(root, args)
    clob = CLOBMarketClient(timeout_s=float(args.book_timeout_s), retries=int(args.book_retries))
    rows: list[dict[str, Any]] = []
    for wallet in wallets:
        events, fetch_report = _fetch_events(wallet, args, now_s=now_s)
        rows.append(
            _row_for_wallet(
                wallet,
                events=events,
                fetch_report=fetch_report,
                clearance_by_wallet=clearance_by_wallet,
                queue_by_wallet=queue_by_wallet,
                rotation_by_wallet=rotation_by_wallet,
                clob=clob,
                now_s=now_s,
                args=args,
                prior_abort_by_wallet=prior_abort_by_wallet,
                temporal_registry_basis=temporal_basis,
            )
        )
    passing = [row for row in rows if (row.get("gate") or {}).get("pass")]
    decision = _decision_for_rows(rows, args)
    history_window = {
        "source": "polymarket_data_api_user_activity",
        "event_basis": "BTC-5m BUY wallet activity rows normalized by WalletHistoryClient and filtered to the last 24h",
        "lookback_s": 24.0 * 3600.0,
        "start_ts": now_s - 24.0 * 3600.0,
        "end_ts": now_s,
        "start_iso": _iso_from_ts(now_s - 24.0 * 3600.0),
        "end_iso": _iso_from_ts(now_s),
        "temporal_registry_basis_path": temporal_basis.get("path"),
        "temporal_registry_generated_at": temporal_basis.get("generated_at"),
        "temporal_registry_events_scanned": temporal_basis.get("events_scanned"),
    }
    return {
        "schema_version": 1,
        "kind": "deadman_microprobe_freshness_gate",
        "flow_stage": "LIVE/ROTATE",
        "direction_id": str(args.direction_id),
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "criteria": {
            "max_freshest_buy_lag_s": float(args.max_freshest_buy_lag_s),
            "min_current_clob_backed_copyable_buys_24h": int(args.min_clob_copyable_buys_24h),
            "corrected_joint_gate": bool(args.corrected_joint_gate),
            "min_policy_compatible_inband_buy_count_24h": int(args.min_clob_copyable_buys_24h),
            "temporal_pre_screen": "no active temporal slice may be PROVEN-NEGATIVE",
            "temporal_integrity": "active temporal slices must be proven on the loaded registry basis; UNPROVEN active slices fail closed for admission",
            "min_temporal_history_events_scanned": float(args.min_temporal_history_events_scanned),
            "lookback_hours": 24.0,
            "book_basis": "skipped in corrected_joint_gate because current-book checks on expired BTC-5m windows are structurally biased to zero"
            if bool(args.corrected_joint_gate)
            else "current book fetch at probe time; stale replay counts are recorded but cannot satisfy gate",
            "candidate_wallets": wallets,
        },
        "decision": decision,
        "history_window": history_window,
        "event_basis": history_window["event_basis"],
        "temporal_registry_basis": temporal_basis,
        "summary": {
            "wallets": len(rows),
            "passing_wallets": len(passing),
            "best_wallet": decision["best_wallet"],
            "action": decision["action"],
        },
        "rows": rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--direction-id", default=DIRECTION_ID)
    parser.add_argument("--corrected-joint-gate", action="store_true")
    parser.add_argument("--prior-abort-postmortem", default=DEFAULT_PRIOR_ABORT_POSTMORTEM)
    parser.add_argument("--temporal-profitability", default=DEFAULT_TEMPORAL_PROFITABILITY)
    parser.add_argument("--queue", default=DEFAULT_QUEUE)
    parser.add_argument("--clearance", default=DEFAULT_CLEARANCE)
    parser.add_argument("--rotation", default=DEFAULT_ROTATION)
    parser.add_argument("--include-wallet", action="append", default=list(DEFAULT_REQUIRED_WALLETS))
    parser.add_argument("--max-candidates", type=int, default=10)
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--timeout-s", type=float, default=8.0)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--direct-data-api", action="store_true", default=True)
    parser.add_argument("--min-price", type=float, default=0.25)
    parser.add_argument("--max-price", type=float, default=0.50)
    parser.add_argument("--max-freshest-buy-lag-s", type=float, default=7200.0)
    parser.add_argument("--min-clob-copyable-buys-24h", type=int, default=5)
    parser.add_argument("--min-temporal-history-events-scanned", type=float, default=153000.0)
    parser.add_argument("--fail-closed-on-active-unproven-temporal-slice", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-book-fetches", type=int, default=40)
    parser.add_argument("--keep-scored-events", type=int, default=20)
    parser.add_argument("--book-timeout-s", type=float, default=4.0)
    parser.add_argument("--book-retries", type=int, default=1)
    parser.add_argument("--wallet-fraction", type=float, default=0.10)
    parser.add_argument("--probe-max-order-usd", type=float, default=0.50)
    parser.add_argument("--max-price-for-probe", type=float, default=0.25)
    parser.add_argument("--total-probe-exposure-usd", type=float, default=2.0)
    parser.add_argument("--auto-demote-loss-usd", type=float, default=2.0)
    parser.add_argument("--max-slippage-bps", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_probe(ROOT, args)
    output = Path(args.output)
    output = output if output.is_absolute() else ROOT / output
    atomic_write_json(output, payload)
    print(json.dumps({"output": str(output), **payload["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
