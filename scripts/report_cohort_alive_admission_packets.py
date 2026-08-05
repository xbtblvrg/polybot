#!/usr/bin/env python3
"""Build Fable-facing admission packets from alive market-cohort replay picks.

Flow stage: DISCOVER/LEARN/PROMOTE. Paper/research only: this script joins
cohort replay winners to the same external liveness probe used by the live
guard, adds temporal/hour evidence when available, and writes ranked packets.
It does not mutate live configuration or place orders.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.update_state_digest import (  # noqa: E402
    _external_liveness_passes_for_digest,
    _external_liveness_rows_by_wallet_from_probe,
    _norm_wallet,
    _parse_utc_ts,
)
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_COHORT_REPLAY = ROOT / "data/research/wallet_market_cohort_replay_latest.json"
DEFAULT_LIVENESS = ROOT / "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json"
DEFAULT_TEMPORAL = ROOT / "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_DENYLIST = ROOT / "configs/wallet_copy/toxicity_denylist.json"
DEFAULT_LIVE_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_SOURCE_ACTIVE_COHORT = ROOT / "data/research/source_active_liveness_cohort_latest.json"
DEFAULT_SUPPLEMENTAL_HISTORY_MANIFEST = ROOT / "data/research/temporal_supplemental_history_manifest.json"
DEFAULT_OUTPUT = ROOT / "data/research/cohort_alive_admission_packets_latest.json"
DEFAULT_REGISTRY = ROOT / "configs/wallet_copy/wallets.json"
DEFAULT_FULL_POOL_QUEUE = ROOT / "data/research/wallet_copy_full_pool_member_queue.json"
COHORT_EVIDENCED_POLICY_ID = "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
COMPLETE_REPLAY_STOP_REASONS = {"empty_page", "lookback_cutoff_reached", "short_page"}
TRUNCATED_REPLAY_STOP_REASONS = {
    "api_error",
    "hard_stop_utc",
    "max_events_per_wallet",
    "max_pages_or_event_cap",
    "pagination_cap_reached",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _active_slices(now: datetime) -> list[str]:
    slices = ["weekday" if now.weekday() < 5 else "weekend"]
    if 18 <= now.hour < 22:
        slices.append("dead_band_18_22_utc")
    return slices


def _temporal_profiles_by_wallet(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("wallets") if isinstance(payload.get("wallets"), list) else []
    return {
        wallet: row
        for row in rows
        if isinstance(row, dict)
        for wallet in [_norm_wallet(row.get("wallet") or row.get("source_wallet"))]
        if wallet
    }


def _denylist_cells_by_wallet(payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    cells = payload.get("cells") if isinstance(payload.get("cells"), list) else []
    out: dict[str, list[dict[str, Any]]] = {}
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        wallet = _norm_wallet(cell.get("source_wallet") or cell.get("wallet"))
        if not wallet:
            continue
        out.setdefault(wallet, []).append(
            {
                "price_bucket": cell.get("price_bucket"),
                "deny_rule": cell.get("deny_rule"),
                "reason": cell.get("reason"),
                "all_signals_roi_pct": (cell.get("all_signals") or {}).get("roi_pct")
                if isinstance(cell.get("all_signals"), dict)
                else None,
                "live_fills_roi_pct": (cell.get("live_fills") or {}).get("roi_pct")
                if isinstance(cell.get("live_fills"), dict)
                else None,
            }
        )
    return out


def _source_active_reports_by_wallet(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    reports = payload.get("reports") if isinstance(payload.get("reports"), list) else []
    reports = [
        *reports,
        *(payload.get("top_source_active") if isinstance(payload.get("top_source_active"), list) else []),
    ]
    out: dict[str, dict[str, Any]] = {}
    for row in reports:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if not wallet:
            continue
        existing = out.get(wallet)
        if existing is None or _as_float(row.get("policy_eligible_windows")) > _as_float(
            existing.get("policy_eligible_windows")
        ):
            out[wallet] = row
    return out


def _source_active_decision(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {
            "status": "MISSING",
            "source_active_tally_status": "MISSING",
            "policy_eligible_tally_status": "MISSING",
            "source_active_windows": 0,
            "policy_eligible_windows": 0,
            "required_source_active_windows": None,
            "output": None,
        }
    return {
        "status": "POLICY_ELIGIBLE_PASS"
        if str(row.get("policy_eligible_tally_status") or "") == "PASS"
        else "SOURCE_ACTIVE_PASS_POLICY_PENDING"
        if str(row.get("source_active_tally_status") or "") == "PASS"
        else "PENDING",
        "source_active_tally_status": row.get("source_active_tally_status"),
        "policy_eligible_tally_status": row.get("policy_eligible_tally_status"),
        "source_active_windows": int(row.get("source_active_windows") or 0),
        "policy_eligible_windows": int(row.get("policy_eligible_windows") or 0),
        "required_source_active_windows": row.get("required_source_active_windows"),
        "source_active_rows": int(row.get("source_active_rows") or 0),
        "policy_eligible_rows": int(row.get("policy_eligible_rows") or 0),
        "output": row.get("output"),
    }


def _history_completeness_status(replay: dict[str, Any]) -> str:
    stop_reason = str(replay.get("stop_reason") or "")
    if replay.get("api_errors"):
        return "truncated"
    if bool(replay.get("pagination_cap_reached")):
        return "truncated"
    if stop_reason in COMPLETE_REPLAY_STOP_REASONS:
        return "complete"
    if stop_reason in TRUNCATED_REPLAY_STOP_REASONS:
        return "truncated"
    return "truncated"


def _history_completeness_by_wallet_from_manifest(
    manifest: dict[str, Any],
    *,
    root: Path = ROOT,
) -> dict[str, dict[str, Any]]:
    files = manifest.get("supplemental_history_files") if isinstance(manifest.get("supplemental_history_files"), list) else []
    by_wallet: dict[str, dict[str, Any]] = {}
    for raw_path in files:
        text = str(raw_path or "").strip()
        if not text:
            continue
        path = Path(text)
        if not path.is_absolute():
            path = root / path
        payload = load_json(path, default={})
        if not isinstance(payload, dict):
            continue
        replay = payload.get("replay") if isinstance(payload.get("replay"), dict) else {}
        wallets: set[str] = set()
        for row in payload.get("wallets") or []:
            if isinstance(row, dict):
                wallet = _norm_wallet(row.get("address") or row.get("wallet"))
                if wallet:
                    wallets.add(wallet)
        for row in payload.get("wallet_results") or []:
            if isinstance(row, dict):
                wallet_obj = row.get("wallet") if isinstance(row.get("wallet"), dict) else {}
                wallet = _norm_wallet(wallet_obj.get("address") or row.get("source_wallet"))
                if wallet:
                    wallets.add(wallet)
        for row in payload.get("events") or []:
            if isinstance(row, dict):
                wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet"))
                if wallet:
                    wallets.add(wallet)
        if not wallets:
            continue
        status = _history_completeness_status(replay) if replay else "truncated"
        stop_reason = replay.get("stop_reason") if replay else "missing_replay_metadata"
        detail = {
            "history_completeness": status,
            "artifact_count": 1,
            "stop_reason_counts": {str(stop_reason): 1},
            "latest_artifact": str(raw_path),
            "latest_replay_batch_id": replay.get("batch_id") if replay else None,
            "rule": (
                "complete only when the replay reached an empty/short page or configured lookback cutoff; "
                "pagination caps, API errors, event caps, and missing replay metadata are tagged truncated"
            ),
        }
        for wallet in wallets:
            existing = by_wallet.get(wallet)
            if existing is None:
                by_wallet[wallet] = dict(detail)
                continue
            existing["artifact_count"] = int(existing.get("artifact_count") or 0) + 1
            counts = existing.setdefault("stop_reason_counts", {})
            counts[str(stop_reason)] = int(counts.get(str(stop_reason)) or 0) + 1
            if status == "truncated":
                existing["history_completeness"] = "truncated"
                existing["latest_artifact"] = str(raw_path)
                existing["latest_replay_batch_id"] = replay.get("batch_id") if replay else None
    return by_wallet


def load_source_active_cohort(path: Path) -> dict[str, Any]:
    payload = load_json(path, default={})
    payload = payload if isinstance(payload, dict) else {}
    reports: list[dict[str, Any]] = []
    for raw in payload.get("batch_files") or []:
        batch_path = Path(str(raw))
        if not batch_path.is_absolute():
            batch_path = ROOT / batch_path
        batch = load_json(batch_path, default={})
        if not isinstance(batch, dict):
            continue
        reports.extend(row for row in batch.get("reports") or [] if isinstance(row, dict))
    if reports:
        payload = dict(payload)
        payload["reports"] = reports
    return payload


def _runtime_wallets(live_guard_state: dict[str, Any]) -> list[str]:
    runtime = live_guard_state.get("active_set_runtime")
    runtime = runtime if isinstance(runtime, dict) else {}
    rows = list(runtime.get("members") if isinstance(runtime.get("members"), list) else [])
    temporal = runtime.get("temporal_slice_exclusion") if isinstance(runtime.get("temporal_slice_exclusion"), dict) else {}
    if isinstance(temporal.get("excluded_members"), list):
        rows.extend(temporal["excluded_members"])
    wallets: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet"))
        if wallet:
            wallets.append(wallet)
    return wallets


def _temporal_decision(profile: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    slices = _active_slices(now)
    if not profile:
        return {
            "status": "UNPROVEN_NO_TEMPORAL_PROFILE",
            "active_slices": slices,
            "classification": None,
            "matched_slice": None,
        }
    labels = profile.get("slice_labels") if isinstance(profile.get("slice_labels"), dict) else {}
    evaluated = []
    for name in slices:
        label = labels.get(name) if isinstance(labels.get(name), dict) else {}
        evaluated.append({"slice": name, **label})
        if label.get("label") == "PROVEN-NEGATIVE":
            return {
                "status": "FAIL_PROVEN_NEGATIVE_ACTIVE_SLICE",
                "active_slices": slices,
                "classification": profile.get("classification"),
                "matched_slice": {"slice": name, **label},
                "evaluated_slices": evaluated,
            }
        if label.get("label") == "PROVEN-POSITIVE":
            return {
                "status": "PASS_PROVEN_POSITIVE_ACTIVE_SLICE",
                "active_slices": slices,
                "classification": profile.get("classification"),
                "matched_slice": {"slice": name, **label},
                "evaluated_slices": evaluated,
            }
    return {
        "status": "UNPROVEN_ACTIVE_SLICE",
        "active_slices": slices,
        "classification": profile.get("classification"),
        "matched_slice": None,
        "evaluated_slices": evaluated,
    }


def _rank_key(packet: dict[str, Any]) -> tuple[int, float, float, str]:
    hour_status = str(packet.get("hour_match_status") or "")
    if hour_status == "PASS_PROVEN_POSITIVE_ACTIVE_SLICE":
        tier = 0
    elif hour_status.startswith("UNPROVEN"):
        tier = 1
    else:
        tier = 2
    return (
        tier,
        -_as_float(packet.get("paper_pnl_usd")),
        -_as_float(packet.get("resolved_copyable_events")),
        str(packet.get("wallet") or ""),
    )


def build_registry_admission_packets(
    *,
    registry: dict[str, Any],
    liveness_probe: dict[str, Any],
    temporal: dict[str, Any],
    live_guard_state: dict[str, Any],
    existing_wallets: set[str] | None = None,
    generated_at: datetime | None = None,
    packet_limit: int = 50,
) -> dict[str, Any]:
    """Widen rung-C supply from registry evidence without changing F1 digits."""
    now = generated_at or _utc_now()
    registry_rows = registry.get("wallets") if isinstance(registry.get("wallets"), list) else []
    registry_wallets = {
        wallet
        for row in registry_rows
        if isinstance(row, dict)
        for wallet in [_norm_wallet(row.get("address") or row.get("wallet"))]
        if wallet
    }
    liveness_rows = _external_liveness_rows_by_wallet_from_probe(liveness_probe)
    probe_generated_at = _parse_utc_ts(liveness_probe.get("generated_at"))
    temporal_rows = _temporal_profiles_by_wallet(temporal)
    active_wallets = set(_runtime_wallets(live_guard_state))
    excluded = set(existing_wallets or set()) | active_wallets
    eligible: list[dict[str, Any]] = []
    rejection_counts: dict[str, int] = {}

    for wallet in sorted(registry_wallets):
        liveness = liveness_rows.get(wallet)
        if liveness is None:
            rejection_counts["liveness_row_missing"] = rejection_counts.get("liveness_row_missing", 0) + 1
            continue
        passed, reason, age_h = _external_liveness_passes_for_digest(
            liveness, now=now, probe_generated_at=probe_generated_at
        )
        if not passed or age_h is None or age_h > 24.0:
            key = reason if not passed else "latest_trade_age_gt_24h"
            rejection_counts[key] = rejection_counts.get(key, 0) + 1
            continue
        profile = temporal_rows.get(wallet) or {}
        slices = profile.get("slice_labels") if isinstance(profile.get("slice_labels"), dict) else {}
        weekday = slices.get("weekday") if isinstance(slices.get("weekday"), dict) else {}
        resolved = int(weekday.get("resolved_trades") or weekday.get("resolved_signals") or 0)
        pnl = _as_float(weekday.get("pnl_usd"))
        roi = _as_float(weekday.get("roi_pct"))
        if resolved < 200 or pnl <= 0 or roi <= 0:
            rejection_counts["f1_weekday_positive_cell"] = rejection_counts.get("f1_weekday_positive_cell", 0) + 1
            continue
        if wallet in excluded:
            rejection_counts["already_packeted_or_active"] = rejection_counts.get("already_packeted_or_active", 0) + 1
            continue
        eligible.append(
            {
                "wallet": wallet,
                "candidate_id": f"registry_fresh_weekday_{wallet[-12:]}",
                "paper_policy_id": COHORT_EVIDENCED_POLICY_ID,
                "recommendation": "REGISTRY_FRESH_FLOW_ADMISSION_PACKET",
                "paper_pnl_usd": pnl,
                "roi_pct": roi,
                "resolved_copyable_events": resolved,
                "fresh_own_source_buy_rows_30m": int(liveness.get("btc5m_buys_30m") or (1 if age_h <= 0.5 else 0)),
                "latest_trade_age_h": round(age_h, 6),
                "external_liveness": {
                    "status": "PASS",
                    "reason": reason,
                    "btc5m_trades_24h": liveness.get("btc5m_trades_24h")
                    or liveness.get("remote_dataapi_btc5m_trades_24h"),
                    "btc5m_buys_24h": liveness.get("btc5m_buys_24h")
                    or liveness.get("remote_dataapi_btc5m_buys_24h"),
                    "state_generated_at": liveness_probe.get("generated_at"),
                },
                "hour_match_status": "PASS_PROVEN_POSITIVE_ACTIVE_SLICE",
                "temporal_evidence": {
                    "status": "PASS_PROVEN_POSITIVE_ACTIVE_SLICE",
                    "classification": profile.get("classification"),
                    "matched_slice": {"slice": "weekday", **weekday},
                },
                "registry_screen": {
                    "f1_min_resolved_signals": 200,
                    "f1_pnl_usd_gt": 0,
                    "f1_roi_pct_gt": 0,
                    "f4_external_liveness_max_age_h": 24.0,
                },
                "paper_only": True,
                "live_orders_allowed": False,
            }
        )

    eligible.sort(key=lambda row: (-float(row["paper_pnl_usd"]), -float(row["roi_pct"]), row["wallet"]))
    selected = eligible[: max(0, int(packet_limit))]
    return {
        "registry_rows": len(registry_rows),
        "registry_unique_addresses": len(registry_wallets),
        "liveness_rows": len(liveness_rows),
        "weekday_f1_and_liveness_eligible": len(eligible),
        "packet_limit": int(packet_limit),
        "packet_count": len(selected),
        "rejection_counts": dict(sorted(rejection_counts.items())),
        "packets": selected,
    }


def build_registry_f1_probe_queue(*, registry: dict[str, Any], temporal: dict[str, Any]) -> dict[str, Any]:
    """Build the bounded remote-liveness input after the unchanged weekday F1 filter."""
    registry_rows = registry.get("wallets") if isinstance(registry.get("wallets"), list) else []
    registry_wallets = {
        wallet
        for row in registry_rows
        if isinstance(row, dict)
        for wallet in [_norm_wallet(row.get("address") or row.get("wallet"))]
        if wallet
    }
    rows: list[dict[str, Any]] = []
    for wallet, profile in _temporal_profiles_by_wallet(temporal).items():
        if wallet not in registry_wallets:
            continue
        slices = profile.get("slice_labels") if isinstance(profile.get("slice_labels"), dict) else {}
        weekday = slices.get("weekday") if isinstance(slices.get("weekday"), dict) else {}
        resolved = int(weekday.get("resolved_trades") or weekday.get("resolved_signals") or 0)
        pnl = _as_float(weekday.get("pnl_usd"))
        roi = _as_float(weekday.get("roi_pct"))
        if resolved < 200 or pnl <= 0 or roi <= 0:
            continue
        rows.append(
            {
                "wallet": wallet,
                "resolved_pnl": pnl,
                "retrospective_gross_pnl_usd": pnl,
                "retrospective_gross_roi_pct": roi,
                "retrospective_resolved_signals": resolved,
                "paper_only": True,
                "live_orders_allowed": False,
            }
        )
    rows.sort(
        key=lambda row: (
            -float(row["retrospective_gross_pnl_usd"]),
            -float(row["retrospective_gross_roi_pct"]),
            row["wallet"],
        )
    )
    return {
        "schema_version": 1,
        "kind": "registry_weekday_f1_remote_liveness_probe_queue",
        "flow_stage": "DISCOVER/ROTATE",
        "paper_only": True,
        "live_orders_allowed": False,
        "summary": {
            "registry_rows": len(registry_rows),
            "registry_unique_addresses": len(registry_wallets),
            "ranked_members": len(rows),
            "f1_min_resolved_signals": 200,
            "f1_pnl_usd_gt": 0,
            "f1_roi_pct_gt": 0,
        },
        "ranked_members": rows,
    }


def build_report(
    *,
    cohort_replay: dict[str, Any],
    liveness_probe: dict[str, Any],
    temporal: dict[str, Any],
    denylist: dict[str, Any],
    live_guard_state: dict[str, Any],
    source_active_cohort: dict[str, Any] | None = None,
    history_completeness_by_wallet: dict[str, dict[str, Any]] | None = None,
    require_source_active_policy: bool = False,
    generated_at: datetime | None = None,
    packet_limit: int = 10,
) -> dict[str, Any]:
    now = generated_at or _utc_now()
    picks = cohort_replay.get("live_ready_picks") if isinstance(cohort_replay.get("live_ready_picks"), list) else []
    rows_by_wallet = _external_liveness_rows_by_wallet_from_probe(liveness_probe)
    probe_generated_at = _parse_utc_ts(liveness_probe.get("generated_at"))
    profiles = _temporal_profiles_by_wallet(temporal)
    deny_cells = _denylist_cells_by_wallet(denylist)
    source_active_reports = _source_active_reports_by_wallet(source_active_cohort or {})
    history_completeness_by_wallet = history_completeness_by_wallet or {}
    active_wallets = set(_runtime_wallets(live_guard_state))
    packets: list[dict[str, Any]] = []
    liveness_fail_counts: dict[str, int] = {}
    for pick in picks:
        if not isinstance(pick, dict):
            continue
        wallet = _norm_wallet(pick.get("wallet") or pick.get("source_wallet"))
        row = rows_by_wallet.get(wallet)
        if not wallet or row is None:
            liveness_fail_counts["external_liveness_row_missing"] = (
                liveness_fail_counts.get("external_liveness_row_missing", 0) + 1
            )
            continue
        passed, reason, age_h = _external_liveness_passes_for_digest(
            row,
            now=now,
            probe_generated_at=probe_generated_at,
        )
        if not passed:
            liveness_fail_counts[reason] = liveness_fail_counts.get(reason, 0) + 1
            continue
        temporal_decision = _temporal_decision(profiles.get(wallet, {}), now=now)
        cells = deny_cells.get(wallet, [])
        source_active = _source_active_decision(source_active_reports.get(wallet))
        history_completeness = history_completeness_by_wallet.get(wallet) or {
            "history_completeness": "truncated",
            "artifact_count": 0,
            "stop_reason_counts": {"missing_history_artifact": 1},
            "latest_artifact": None,
            "latest_replay_batch_id": None,
            "rule": "missing replay metadata is conservatively tagged truncated",
        }
        source_policy_pass = source_active["policy_eligible_tally_status"] == "PASS"
        already_active = wallet in active_wallets
        history_complete = history_completeness.get("history_completeness") == "complete"
        if require_source_active_policy and not source_policy_pass:
            recommendation = "HOLD_SOURCE_ACTIVE_POLICY_PENDING"
        elif cells:
            recommendation = "HOLD_DENYLIST_CELL_PRESENT"
        elif already_active:
            recommendation = "ALREADY_ACTIVE_MEASURE_LIVE"
        elif temporal_decision["status"] == "PASS_PROVEN_POSITIVE_ACTIVE_SLICE" and not history_complete:
            recommendation = "DEEP_HISTORY_PENDING"
        elif temporal_decision["status"] == "PASS_PROVEN_POSITIVE_ACTIVE_SLICE":
            recommendation = "ADMISSION_PACKET_READY"
        elif temporal_decision["status"].startswith("UNPROVEN"):
            recommendation = "SHADOW_ADMISSION_PACKET_READY_HOUR_PROFILE_PENDING"
        else:
            recommendation = "HOLD_TEMPORAL_ACTIVE_SLICE_NEGATIVE"
        packets.append(
            {
                "wallet": wallet,
                "candidate_id": f"market_cohort_alive_{wallet[-12:]}",
                "paper_policy_id": COHORT_EVIDENCED_POLICY_ID,
                "recommendation": recommendation,
                "paper_pnl_usd": pick.get("paper_pnl_usd"),
                "roi_pct": pick.get("roi_pct"),
                "resolved_copyable_events": pick.get("resolved_copyable_events"),
                "copyable_buy_events": pick.get("copyable_buy_events"),
                "unique_markets": pick.get("unique_markets"),
                "stake_usd": pick.get("stake_usd"),
                "win_rate_pct": pick.get("win_rate_pct"),
                "latest_trade_age_h": None if age_h is None else round(age_h, 6),
                "external_liveness": {
                    "status": "PASS",
                    "reason": reason,
                    "btc5m_trades_24h": row.get("btc5m_trades_24h")
                    or row.get("remote_dataapi_btc5m_trades_24h"),
                    "btc5m_buys_24h": row.get("btc5m_buys_24h") or row.get("remote_dataapi_btc5m_buys_24h"),
                    "state_generated_at": liveness_probe.get("generated_at"),
                },
                "source_active": source_active,
                "history_completeness": history_completeness["history_completeness"],
                "history_completeness_detail": history_completeness,
                "source_active_policy_required": bool(require_source_active_policy),
                "hour_match_status": temporal_decision["status"],
                "temporal_evidence": temporal_decision,
                "denylist_cells": cells,
                "already_active_runtime_member": already_active,
                "paper_only": True,
                "live_orders_allowed": False,
            }
        )
    packets.sort(key=_rank_key)
    selected_pool = [
        row
        for row in packets
        if not require_source_active_policy
        or (row.get("source_active") or {}).get("policy_eligible_tally_status") == "PASS"
    ]
    selected = selected_pool[: max(0, int(packet_limit))]
    source_policy_packets = [
        row for row in packets if (row.get("source_active") or {}).get("policy_eligible_tally_status") == "PASS"
    ]
    source_policy_hour_pass = [
        row for row in source_policy_packets if row.get("hour_match_status") == "PASS_PROVEN_POSITIVE_ACTIVE_SLICE"
    ]
    four_way_ready = [
        row
        for row in source_policy_hour_pass
        if not row.get("denylist_cells")
        and not row.get("already_active_runtime_member")
        and row.get("history_completeness") == "complete"
    ]
    deep_history_pending = [
        row
        for row in source_policy_hour_pass
        if not row.get("denylist_cells")
        and not row.get("already_active_runtime_member")
        and row.get("history_completeness") != "complete"
    ]
    history_completeness_counts: dict[str, int] = {}
    for row in selected:
        status = str(row.get("history_completeness") or "truncated")
        history_completeness_counts[status] = history_completeness_counts.get(status, 0) + 1
    return {
        "schema_version": 1,
        "kind": "cohort_alive_admission_packets",
        "flow_stage": "DISCOVER/LEARN/PROMOTE",
        "generated_at": _iso(now),
        "paper_only": True,
        "live_orders_allowed": False,
        "single_submitter_invariant": "research packet only; scripts/run_wallet_copy_live_guard.py remains the only live submitter",
        "inputs": {
            "cohort_replay_generated_at": cohort_replay.get("generated_at"),
            "liveness_probe_generated_at": liveness_probe.get("generated_at"),
            "temporal_generated_at": temporal.get("generated_at"),
            "source_active_cohort_generated_at": (source_active_cohort or {}).get("generated_at"),
            "source_active_required": bool(require_source_active_policy),
        },
        "summary": {
            "raw_live_ready_picks": len([row for row in picks if isinstance(row, dict)]),
            "alive_liveness_pass": len(packets),
            "source_active_rows_seen": len(source_active_reports),
            "source_active_pass": sum(
                1 for row in packets if (row.get("source_active") or {}).get("source_active_tally_status") == "PASS"
            ),
            "source_active_policy_eligible": len(source_policy_packets),
            "source_active_policy_pending_or_missing": sum(
                1 for row in packets if (row.get("source_active") or {}).get("policy_eligible_tally_status") != "PASS"
            ),
            "source_active_policy_packet_pool": len(selected_pool),
            "packet_count": len(selected),
            "packet_limit": packet_limit,
            "hour_match_pass": sum(
                1 for row in packets if row.get("hour_match_status") == "PASS_PROVEN_POSITIVE_ACTIVE_SLICE"
            ),
            "source_active_policy_hour_match_pass": len(source_policy_hour_pass),
            "four_way_admission_ready": len(four_way_ready),
            "deep_history_pending": len(deep_history_pending),
            "history_completeness_counts": dict(sorted(history_completeness_counts.items())),
            "history_truncated_packets": history_completeness_counts.get("truncated", 0),
            "hour_match_unproven": sum(1 for row in packets if str(row.get("hour_match_status") or "").startswith("UNPROVEN")),
            "denylist_cell_present": sum(1 for row in packets if row.get("denylist_cells")),
            "already_configured_live_members": sum(1 for row in packets if row.get("already_active_runtime_member")),
            "liveness_fail_reason_counts": liveness_fail_counts,
            "top_wallet": selected[0]["wallet"] if selected else "",
            "top_four_way_wallet": four_way_ready[0]["wallet"] if four_way_ready else "",
            "next_action": (
                "send complete-history P1 candidate to Fable promotion audit; never self-admit"
                if four_way_ready
                else "complete deep history before any admission recommendation"
                if deep_history_pending
                else "keep hour-match strict and profile the policy-eligible pending set"
            ),
        },
        "top_four_way_candidate": four_way_ready[0] if four_way_ready else {},
        "packets": selected,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort-replay", default=str(DEFAULT_COHORT_REPLAY))
    parser.add_argument("--liveness", default=str(DEFAULT_LIVENESS))
    parser.add_argument("--temporal", default=str(DEFAULT_TEMPORAL))
    parser.add_argument("--denylist", default=str(DEFAULT_DENYLIST))
    parser.add_argument("--live-guard-state", default=str(DEFAULT_LIVE_GUARD_STATE))
    parser.add_argument("--source-active-cohort", default=str(DEFAULT_SOURCE_ACTIVE_COHORT))
    parser.add_argument("--supplemental-history-manifest", default=str(DEFAULT_SUPPLEMENTAL_HISTORY_MANIFEST))
    parser.add_argument("--require-source-active-policy", action="store_true")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--packet-limit", type=int, default=10)
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY))
    parser.add_argument("--full-pool-queue", default=str(DEFAULT_FULL_POOL_QUEUE))
    parser.add_argument("--registry-probe-queue-output", default="")
    parser.add_argument("--registry-probe-queue-only", action="store_true")
    parser.add_argument(
        "--base-report",
        default="",
        help="Optional existing cohort packet report to extend without reranking away its packets.",
    )
    parser.add_argument(
        "--registry-screen-limit",
        type=int,
        default=0,
        help="Append up to this many paper-only registry fresh-flow packets at unchanged F1 digits.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_json(Path(args.registry), default={})
    temporal = load_json(Path(args.temporal), default={})
    if args.registry_probe_queue_output:
        probe_queue = build_registry_f1_probe_queue(registry=registry, temporal=temporal)
        atomic_write_json(Path(args.registry_probe_queue_output), probe_queue)
        if args.registry_probe_queue_only:
            print(json.dumps({"output": args.registry_probe_queue_output, "summary": probe_queue["summary"]}, sort_keys=True))
            return 0
    liveness_probe = load_json(Path(args.liveness), default={})
    live_guard_state = load_json(Path(args.live_guard_state), default={})
    report = load_json(Path(args.base_report), default={}) if args.base_report else {}
    if not isinstance(report, dict) or not isinstance(report.get("packets"), list):
        report = build_report(
            cohort_replay=load_json(Path(args.cohort_replay), default={}),
            liveness_probe=liveness_probe,
            temporal=temporal,
            denylist=load_json(Path(args.denylist), default={}),
            live_guard_state=live_guard_state,
            source_active_cohort=load_source_active_cohort(Path(args.source_active_cohort)),
            history_completeness_by_wallet=_history_completeness_by_wallet_from_manifest(
                load_json(Path(args.supplemental_history_manifest), default={})
            ),
            require_source_active_policy=bool(args.require_source_active_policy),
            packet_limit=args.packet_limit,
        )
    current_liveness_rows = _external_liveness_rows_by_wallet_from_probe(liveness_probe)
    for packet in report.get("packets") or []:
        if not isinstance(packet, dict):
            continue
        liveness_row = current_liveness_rows.get(_norm_wallet(packet.get("wallet")))
        if liveness_row is not None:
            packet["fresh_own_source_buy_rows_30m"] = int(liveness_row.get("btc5m_buys_30m") or 0)
    if args.registry_screen_limit > 0:
        registry_screen = build_registry_admission_packets(
            registry=registry,
            liveness_probe=liveness_probe,
            temporal=temporal,
            live_guard_state=live_guard_state,
            existing_wallets={
                *(_norm_wallet(row.get("wallet")) for row in report["packets"]),
                *(
                    _norm_wallet(row.get("wallet"))
                    for row in load_json(Path(args.full_pool_queue), default={}).get("ranked_members", [])
                    if isinstance(row, dict)
                ),
            },
            packet_limit=args.registry_screen_limit,
        )
        report["packets"].extend(registry_screen.pop("packets"))
        report["registry_fresh_flow_screen"] = registry_screen
        report["summary"]["registry_screen_packets"] = registry_screen["packet_count"]
        report["summary"]["packet_count"] = len(report["packets"])
    output = Path(args.output)
    atomic_write_json(output, report)
    print(json.dumps({"output": str(output), "summary": report["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
