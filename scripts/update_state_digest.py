#!/usr/bin/env python3
"""Build the shadow-validated agent state digest.

Flow stage: SELF-DEV. The digest is deliberately a reading aid, not an
authority source. During the validation phase agents must read it alongside
the full source documents and grade whether it contained everything used.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shlex
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, atomic_write_text
from src.wallet_copy.wide_standby import binding_terminally_executed
from src.wallet_copy.venue_executability import venue_gate_summary
from src.wallet_copy.participation import (
    PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS,
    annotate_participation_item,
    summarize_adjusted_participation,
)
from scripts.order_flow_incident_archive import tail_incident_rows
from scripts.order_flow_deadman import _brain_policy_choke
from scripts.brainless_live_guard_restart import generation_verdict


DEFAULT_OUTPUT = "data/research/state_digest.md"
DEFAULT_JSON_OUTPUT = "data/research/state_digest.json"
DEFAULT_LATEST_OUTPUT = "data/research/state_digest_latest.md"
DEFAULT_LATEST_JSON_OUTPUT = "data/research/state_digest_latest.json"
WALK_FORWARD_POOL_HIGH_WATER_SEED = 20
WALK_FORWARD_POOL_HIGH_WATER_FIRST_OBSERVED_AT = "2026-07-30T09:26:00Z"
STRUCTURAL_SCALP_HISTORY = "data/research/btc5m_structural_scalp_forward_source_events.jsonl"
STRUCTURAL_SCALP_HOT_SOURCE = "data/research/wallet_copy_live_guard_hot_history_state.json"
STRUCTURAL_SCALP_STUDY = "data/research/btc5m_two_sided_prime_study_latest.json"
STRUCTURAL_SCALP_STATE = "data/research/btc5m_structural_scalp_paper_lane_state.json"
STRUCTURAL_SCALP_EVENTS = "data/research/btc5m_structural_scalp_paper_lane_events.jsonl"
MAX_DIGEST_LINES = 100
RECENT_DIRECTION_LIMIT = 2
MATERIAL_DIRECTION_LIMIT = 8
MATERIAL_DIRECTION_FALLBACK_LIMIT = 40
RESIDUAL_TREND_LIMIT = 50
MATERIAL_DIRECTION_PREFIXES = (
    "- ANSWER",
    "- answer",
    "- CORRECTION",
    "- DECISION",
    "- MATERIAL",
    "- NEXT",
    "- NEW DATUM",
    "- named structural fact",
    "- next",
    "- ORDER",
    "- OPERATOR ORDER",
    "- ORDERED",
    "- PRIORITY",
    "- PRIORITIES",
    "- BUDGET NOTE",
    "- KPI",
    "- LIVE unchanged",
    "- Milestones stand",
    "- RULING",
    "- ruling",
    "- THREE AMENDMENTS",
)
DEFENSE_DAY_PNL_FLOOR_USD = -35.0
DEFENSE_SINCE_TOPUP_ACTUAL_FLOOR_USD = 20.0
DEFENSE_STANDARD_INTRADAY_PROBE_TRIGGER_USD = -15.0
DEFENSE_FLOOR_BREACH_INTRADAY_PROBE_TRIGGER_USD = -8.0
DEFENSE_SINGLE_FILL_LOSS_USD = -5.0
DEFENSE_PROBE_WEIGHT = 0.10
DEFENSE_PROBE_CAP_USD = 1.0
RUNTIME_SPEED_ABS_DELTA_FLOOR_S = 1.0
WEEKEND_SEAT_LOSS_RIDER_DIRECTION_ID = "2026-07-18T07:10Z-fable-f418-seat-loss-rotation-rider"
WEEKEND_SEAT_LOSS_RIDER_FROM_WALLET = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
WEEKEND_SEAT_LOSS_RIDER_TARGET_WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"
TERMINAL_COMMITMENT_STATUSES = {"CLOSED", "MERGED", "SATISFIED", "SUPERSEDED", "CANCELLED"}
SCORECARD_DIRECT_RUNTIME_THRESHOLD_S = 100.0
SCORECARD_CURRENT_BUILD_TIMEOUT_S = 120.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _annotate_covered_rates(value: Any, *, enforced: bool) -> Any:
    """Label per-order gate-enforcement rates without inventing coverage."""
    if isinstance(value, list):
        return [_annotate_covered_rates(item, enforced=enforced) for item in value]
    if not isinstance(value, dict):
        return value
    out = {
        key: _annotate_covered_rates(item, enforced=enforced)
        for key, item in value.items()
    }
    covered_rate_keys = [
        key
        for key in value
        if "floor_gate_enforced_" in key
        and (key.endswith("_rate") or key.endswith("_rate_pct"))
    ]
    if covered_rate_keys:
        out["enforced_by_running_binary"] = bool(enforced)
        out["coverage_measurement_status"] = (
            "PER_ORDER_ENFORCEMENT_MEASUREMENT"
            if enforced
            else "UNENFORCED_MEASUREMENT"
        )
        for key in covered_rate_keys:
            out[f"{key}_enforced_by_running_binary"] = bool(enforced)
            out[f"{key}_status"] = out["coverage_measurement_status"]
    return out


def _daily_floor_gate_residency(
    day_utc: str,
    canonical_day: dict[str, Any],
    *,
    loaded_generation_sha256: str | None,
    disk_generation_sha256: str | None,
    generation_verdict: dict[str, Any],
) -> dict[str, Any]:
    """Publish the daily cost and generation identity of floor-gate residency."""
    fills = int(canonical_day.get("fills") or 0)
    enforced_fills = int(canonical_day.get("floor_gate_enforced_fill_count") or 0)
    loaded = str(loaded_generation_sha256 or "").strip() or None
    disk = str(disk_generation_sha256 or "").strip() or None
    citable = bool(generation_verdict.get("generation_mismatch_citable"))
    if not citable or not loaded or not disk:
        evidence_status = "UNKNOWN_STALE_OR_MISSING"
    elif loaded == disk:
        evidence_status = "MATCH"
    else:
        evidence_status = "MISMATCH"
    return {
        "day_utc": day_utc,
        "fills": fills,
        "floor_gate_enforced_fill_count": enforced_fills,
        "floor_gate_enforced_fill_rate": (
            round(enforced_fills / fills, 9) if fills else None
        ),
        "floor_gate_enforced_fill_rate_pct": (
            round(100.0 * enforced_fills / fills, 6) if fills else None
        ),
        "ruled_floor_breach_count": int(
            canonical_day.get("ruled_floor_breach_count") or 0
        ),
        "ruled_floor_breach_cost_usd": round(
            float(canonical_day.get("ruled_floor_breach_cost_usd") or 0.0), 6
        ),
        "running_submitter_floor_gate_generation_sha256": loaded,
        "disk_floor_gate_generation_sha256": disk,
        "generation_match": bool(loaded and disk and loaded == disk),
        "generation_evidence_status": evidence_status,
        "generation_verdict_age_s": generation_verdict.get("age_s"),
        "generation_mismatch_citable": citable,
        "measurement_only": True,
        "live_mutation": False,
    }


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _order128_manifest_binding(
    pointer: dict[str, Any],
    manifest: dict[str, Any],
    packet: dict[str, Any],
) -> dict[str, Any]:
    """Report manifest authorization only with its exact producer cut."""

    pointer_path = str(pointer.get("manifest_path") or "")
    absence = "NO_POINTER" if not pointer_path else "NO_MANIFEST"
    present = bool(manifest)
    summary = manifest.get("summary") if isinstance(manifest.get("summary"), dict) else {}
    packet_generated_at = str(packet.get("generated_at") or "")
    packet_cut = str(packet.get("score_run_id") or "UNASSERTABLE")
    manifest_score_run_id = str(manifest.get("score_run_id") or absence)
    manifest_cut = str(manifest.get("score_run_id") or "UNASSERTABLE")

    def value(raw: Any) -> Any:
        return raw if present else absence

    return {
        "status": "PRESENT" if present else absence,
        "manifest_generated_at": value(manifest.get("generated_at")),
        "manifest_id": value(manifest.get("manifest_id")),
        "manifest_score_run_id": manifest_score_run_id,
        "order128_packet_generated_at": packet_generated_at or "NO_PACKET_CUT",
        "order128_packet_score_run_id": packet_cut,
        "cuts_agree": (
            manifest_cut == packet_cut
            if present
            and manifest_cut != "UNASSERTABLE"
            and packet_cut != "UNASSERTABLE"
            else "UNASSERTABLE"
        ),
        "paper_only": value(manifest.get("paper_only")),
        "live_orders_allowed": value(manifest.get("live_orders_allowed")),
        "admitted_wallets": value(summary.get("admitted_wallets")),
        "promotion_admitted_wallets": value(summary.get("promotion_admitted_wallets")),
        "order128_sticky_focus_authorized": value(
            summary.get("order128_sticky_focus_authorized")
        ),
    }


def _park_provenance_rows(candidate_evidence: dict[str, Any]) -> list[dict[str, Any]]:
    """Carry bounded ORDER129 refusal provenance into the digest."""

    carried: list[dict[str, Any]] = []
    for row in candidate_evidence.get("rows") or []:
        if not isinstance(row, dict) or not isinstance(row.get("park_provenance"), dict):
            continue
        carried.append(
            {
                "wallet": row.get("wallet"),
                "wide_policy_fingerprint": row.get("wide_policy_fingerprint"),
                **row["park_provenance"],
            }
        )
        if len(carried) >= 10:
            break
    return carried


def _merge_authoritative_wide_standby(
    pipeline: dict[str, Any],
    binding_artifact: dict[str, Any],
    lane_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Keep an executed WIDE standby binding authoritative over stale scorecards."""

    binding = (
        binding_artifact.get("binding")
        if isinstance(binding_artifact.get("binding"), dict)
        else {}
    )
    if (
        not binding_terminally_executed(binding_artifact)
        or binding.get("source_binding_status") != "WIRED"
    ):
        return pipeline
    merged = dict(pipeline)
    standby = (
        dict(merged.get("standby_ready"))
        if isinstance(merged.get("standby_ready"), dict)
        else {}
    )
    seat = standby.get("seat") if isinstance(standby.get("seat"), dict) else {}
    lane = lane_summary or {}
    binding_resolved = binding.get("resolved_paper_fills")
    lane_resolved = lane.get("resolved_paper_fills")
    resolved_delta = (
        int(binding_resolved) - int(lane_resolved)
        if binding_resolved is not None and lane_resolved is not None
        else None
    )
    binding_elapsed = binding.get("standby_evidence_elapsed_h")
    digest_elapsed = seat.get("wall_clock_elapsed_h", seat.get("elapsed_h"))
    elapsed_delta = (
        round(abs(float(digest_elapsed) - float(binding_elapsed)), 6)
        if digest_elapsed is not None and binding_elapsed is not None
        else None
    )
    lane_present = lane.get("lane_present")
    divergent = bool(
        (resolved_delta is not None and resolved_delta != 0)
        or (elapsed_delta is not None and elapsed_delta > 1.0)
        or lane_present is False
    )
    binding_started = _parse_utc_ts(binding.get("standby_evidence_started_at"))
    generated = _parse_utc_ts(binding_artifact.get("generated_at"))
    wall_elapsed_h = (
        digest_elapsed
        if digest_elapsed is not None
        else max(0.0, (generated - binding_started).total_seconds() / 3600.0)
        if generated and binding_started
        else None
    )
    binding_resolved_count = int(binding_resolved or 0)
    measured_elapsed_h = float(binding_elapsed or 0.0)
    unfed_clock = bool(
        wall_elapsed_h is not None
        and float(wall_elapsed_h) > 0.0
        and measured_elapsed_h == 0.0
    )
    projected_resolved = (
        round(binding_resolved_count * 48.0 / measured_elapsed_h, 6)
        if measured_elapsed_h > 0.0
        else 0.0
    )
    seat_clock_divergence = {
        "binding_resolved": binding_resolved,
        "lane_resolved": lane_resolved,
        "resolved_delta": resolved_delta,
        "binding_elapsed_h": binding_elapsed,
        "digest_elapsed_h": digest_elapsed,
        "elapsed_delta_h": elapsed_delta,
        "lane_present": lane_present,
        "status": "ACCRUING_RED_DIVERGENT" if divergent else "CONSISTENT",
    }
    standby["seat"] = {
        "wallet": binding.get("wallet"),
        "status": (
            "PARK_SEAT_UNFED_CLOCK_COMMITTED"
            if binding_artifact.get("execution_status") == "PARK_COMMITTED"
            else "UNFED_CLOCK_CANNOT_MATURE"
            if unfed_clock
            else "ACCRUING_RED_DIVERGENT"
            if divergent
            else "ACCRUING_RED"
        ),
        "source_binding_status": "WIRED",
        "source_binding_id": binding.get("source_binding_id"),
        "clock_or_source_binding_missing": False,
        "clock_start": binding.get("standby_evidence_started_at"),
        "elapsed_h": binding.get("standby_evidence_elapsed_h"),
        "required_h": binding.get("standby_evidence_minimum_h"),
        "resolved": binding.get("resolved_paper_fills"),
        "resolutions_attempted": seat.get("resolutions_attempted"),
        "resolution_attempt_taxonomy": seat.get("resolution_attempt_taxonomy") or {},
        "attempt_log_retention": seat.get("attempt_log_retention") or {},
        "attempt_window_start": seat.get("attempt_window_start"),
        "attempt_window_end": seat.get("attempt_window_end"),
        "required_resolved": binding.get("promotion_resolved_fill_gate"),
        "projected_resolved_at_48h": projected_resolved,
        "admission_forecast": bool(not unfed_clock and projected_resolved >= 30),
        "post_fee_pnl_usd": binding.get("in_lane_post_fee_pnl_usd"),
        "next_action": (
            (
                "none; terminal park committed at "
                f"{binding.get('terminal_executed_at')}"
            )
            if binding_artifact.get("execution_status") == "PARK_COMMITTED"
            else "execute pre-committed PARK_SEAT_UNFED_CLOCK at the immutable deadline"
            if unfed_clock
            else binding.get("next")
        ),
        "terminal_executed_at": binding.get("terminal_executed_at"),
        "terminal_outcome_on_deadline": binding.get("terminal_outcome_on_deadline"),
        "source": "82c8_wide_standby_binding_latest.json",
        "seat_clock_divergence": seat_clock_divergence,
    }
    merged["standby_ready"] = standby
    merged["seat_clock_divergence"] = seat_clock_divergence
    return merged


_WIDE_SUPERVISOR_REQUIRED_SCORE_STEPS = (
    "scripts/reconcile_wide_exact_policy_paper.py",
    "scripts/build_wide_candidate_standings.py",
    "scripts/build_wide_policy_fingerprint_evidence.py",
    "scripts/build_frozen_fingerprint_f2_prewarm_shadow.py",
    "scripts/build_copy_freeze_near_bar_allpass_dryrun_sidecar.py",
)


def _wide_supervisor_pipeline_deployment(supervisor: dict[str, Any]) -> dict[str, Any]:
    """Prove the resident scorer has executed the currently required pipeline."""

    cycles = supervisor.get("latest_cycles")
    latest = cycles[-1] if isinstance(cycles, list) and cycles else {}
    results = latest.get("results") if isinstance(latest, dict) else []
    observed: list[str] = []
    failed: list[str] = []
    for row in results if isinstance(results, list) else []:
        if not isinstance(row, dict):
            continue
        cmd = row.get("cmd")
        script = str(cmd[1]) if isinstance(cmd, list) and len(cmd) > 1 else ""
        if script:
            observed.append(script)
            if row.get("ok") is not True:
                failed.append(script)
    missing = [
        script for script in _WIDE_SUPERVISOR_REQUIRED_SCORE_STEPS if script not in observed
    ]
    if not latest:
        status = "NO_COMPLETED_CYCLE"
    elif missing or failed:
        status = "STALE_OR_INCOMPLETE_PIPELINE"
    else:
        status = "PASS"
    return {
        "status": status,
        "required_steps": list(_WIDE_SUPERVISOR_REQUIRED_SCORE_STEPS),
        "observed_steps": observed,
        "missing_steps": missing,
        "failed_steps": failed,
        "cycle_started_at_s": latest.get("started_at_s")
        if isinstance(latest, dict)
        else None,
        "rule": (
            "resident supervisor health requires a completed latest cycle containing "
            "prewarm and sidecar, not PID/process health alone"
        ),
    }


def _cross_exchange_campaign_truth(live: dict[str, Any]) -> dict[str, Any]:
    """Derive monotone cross-exchange campaign truth from the canonical live ledger."""
    method_orders: list[dict[str, Any]] = []
    resolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in live.get("orders") or []:
        if not isinstance(row, dict):
            continue
        decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), dict) else {}
        wallet_copy = (
            decision.get("wallet_copy")
            if isinstance(decision.get("wallet_copy"), dict)
            else {}
        )
        source_wallet = str(row.get("source_wallet") or wallet_copy.get("source_wallet") or "")
        strategy_family = str(
            decision.get("strategy_family") or wallet_copy.get("strategy_family") or ""
        )
        if (
            source_wallet.lower() != "btc5m_cross_exchange_probability_edge_v1"
            and strategy_family != "paper_struct_btc5m_cross_exchange_probability_edge_v1"
        ):
            continue
        identity = str(row.get("order_id") or row.get("intent_id") or "")
        if identity and identity in seen:
            continue
        if identity:
            seen.add(identity)
        method_orders.append(row)
        pnl_raw = row.get("pnl_usd")
        if pnl_raw is None:
            pnl_raw = row.get("resolved_post_fee_pnl_usd")
        if pnl_raw is None and isinstance(row.get("resolution"), dict):
            pnl_raw = row["resolution"].get("pnl_usd")
        attribution = (
            row.get("alternate_transport_attribution")
            if isinstance(row.get("alternate_transport_attribution"), dict)
            else {}
        )
        if pnl_raw is None and str(attribution.get("resolution_status") or "").upper() == "RESOLVED":
            pnl_raw = attribution.get("resolved_post_fee_pnl_usd")
        if pnl_raw is None:
            continue
        try:
            pnl = float(pnl_raw)
        except (TypeError, ValueError):
            continue
        resolved.append(
            {
                "order_id": str(row.get("order_id") or "") or None,
                "accepted_at": row.get("accepted_at") or row.get("submitted_at"),
                "pnl_usd": round(pnl, 6),
            }
        )
    accepted_statuses = {"SUBMITTED", "FILLED", "LIVE", "MATCHED"}
    accepted = [
        row
        for row in method_orders
        if str(row.get("final_status") or row.get("status") or "").upper() in accepted_statuses
    ]
    filled = [
        row
        for row in accepted
        if str(row.get("final_status") or row.get("status") or "").upper()
        in {"FILLED", "MATCHED"}
        or float(row.get("filled_size_usd") or row.get("response_filled_size_usd") or 0.0) > 0.0
    ]
    rolling = resolved[-20:]
    last = accepted[-1] if accepted else {}
    return {
        "campaign_count_source": "method_tagged_live_ledger",
        "orders_submitted": len(method_orders),
        "orders_accepted": len(accepted),
        "orders_filled": len(filled),
        "last_order_id": str(last.get("order_id") or "") or None,
        "last_order_status": str(last.get("final_status") or last.get("status") or "") or None,
        "last_accepted_at": last.get("accepted_at") or last.get("submitted_at"),
        "method_pnl": {
            "resolved_fills": len(resolved),
            "unresolved_fills": max(0, len(filled) - len(resolved)),
            "rolling_window_fills": 20,
            "rolling_resolved_fills": len(rolling),
            "rolling_realized_pnl_usd": round(
                sum(float(row["pnl_usd"]) for row in rolling), 6
            ),
            "rows": rolling,
        },
    }


def _count_file_lines(path: Path | None) -> int:
    """Count newline-delimited capture rows without materializing the file."""
    if path is None or not path.is_file():
        return 0
    count = 0
    last_byte = b""
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                count += chunk.count(b"\n")
                last_byte = chunk[-1:]
    except OSError:
        return 0
    return count + int(bool(last_byte) and last_byte != b"\n")


def _launchctl_job_pid(label: str) -> int | None:
    """Return a launchd job PID when available; fail closed off macOS."""
    try:
        result = subprocess.run(
            ["launchctl", "list"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    for line in result.stdout.splitlines():
        columns = line.split()
        if len(columns) >= 3 and columns[-1] == label and columns[0].isdigit():
            return int(columns[0])
    return None


def _model_runtime_evidence(codex_dir: Path | None = None) -> dict[str, Any]:
    """Return bounded configured-vs-runtime Codex model evidence."""
    codex_dir = codex_dir or (Path.home() / ".codex")
    config_path = codex_dir / "config.toml"
    logs_path = codex_dir / "logs_2.sqlite"
    configured_model = None
    runtime_model = None
    runtime_observed_at = None

    try:
        match = re.search(
            r'^\s*model\s*=\s*["\']([^"\']+)["\']',
            config_path.read_text(errors="replace"),
            flags=re.MULTILINE,
        )
        configured_model = match.group(1) if match else None
    except OSError:
        pass

    try:
        with sqlite3.connect(f"file:{logs_path}?mode=ro", uri=True, timeout=1.0) as connection:
            row = connection.execute(
                "SELECT ts, feedback_log_body FROM logs "
                "WHERE feedback_log_body LIKE '%model=%' "
                "ORDER BY ts DESC, ts_nanos DESC, id DESC LIMIT 1"
            ).fetchone()
        if row:
            model_match = re.search(r"\bmodel=([^\s}:]+)", str(row[1] or ""))
            runtime_model = model_match.group(1) if model_match else None
            runtime_observed_at = datetime.fromtimestamp(
                int(row[0]), tz=timezone.utc
            ).isoformat().replace("+00:00", "Z")
    except (OSError, sqlite3.Error, TypeError, ValueError):
        pass

    if configured_model and runtime_model:
        status = "MATCH" if configured_model == runtime_model else "MISMATCH"
    else:
        status = "MISSING_EVIDENCE"
    return {
        "status": status,
        "configured_model": configured_model,
        "runtime_model": runtime_model,
        "runtime_observed_at": runtime_observed_at,
        "config_source": str(config_path),
        "runtime_source": str(logs_path),
    }


def _load_latest_json_artifact(directory: Path, pattern: str) -> tuple[dict[str, Any], Path | None]:
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return {}, None
    latest = max(candidates, key=lambda path: (path.stat().st_mtime, path.name))
    data = _load_json(latest, {})
    return data if isinstance(data, dict) else {}, latest


def _latest_same_window_capture_summary(data_dir: Path) -> dict[str, Any]:
    root = data_dir.parents[1]
    candidates = [
        path
        for path in (data_dir / "same_window_capture").glob("*/same_window_capture_state.json")
        if path.is_file()
    ]
    completed: list[tuple[Path, dict[str, Any]]] = []
    for path in candidates:
        state = _load_json(path, {})
        if isinstance(state, dict) and state.get("status") == "COMPLETED":
            completed.append((path, state))
    if not completed:
        return {"status": "MISSING"}
    state_path, state = max(
        completed,
        key=lambda item: (
            _parse_utc_ts(item[1].get("completed_at")) or datetime.min.replace(tzinfo=timezone.utc),
            item[0].stat().st_mtime,
        ),
    )
    paths = state.get("paths") if isinstance(state.get("paths"), dict) else {}
    alpha_report = _load_json(root / str(paths.get("alpha_report") or ""), {})
    top10 = _load_json(root / str(paths.get("top10") or ""), {})
    alpha = alpha_report.get("alpha_decay") if isinstance(alpha_report.get("alpha_decay"), dict) else {}
    per_wallet = alpha.get("per_wallet") if isinstance(alpha.get("per_wallet"), dict) else {}
    clearance = _load_json(data_dir / "ranked_queue_clearance_packets_latest.json", {})
    packet_wallets = [
        str(row.get("wallet") or "").lower()
        for row in (clearance.get("packets") or [])
        if isinstance(row, dict) and _norm_wallet_for_digest(row.get("wallet"))
    ]
    exact_policy: list[dict[str, Any]] = []
    selected = {str(wallet).lower() for wallet in (state.get("selected_wallets") or [])}
    for wallet in packet_wallets:
        metrics = per_wallet.get(wallet) if isinstance(per_wallet.get(wallet), dict) else {}
        one_s = (metrics.get("horizons") or {}).get("1s") if isinstance(metrics, dict) else {}
        one_s = one_s if isinstance(one_s, dict) else {}
        edge = one_s.get("edge") if isinstance(one_s.get("edge"), dict) else {}
        exact_policy.append(
            {
                "wallet": wallet,
                "selected": wallet in selected,
                "status": "MEASURED" if metrics else "NO_OVERLAPPING_FILL_BOOK_SAMPLE",
                "fills_with_any_coverage": metrics.get("fills_with_any_coverage") if metrics else 0,
                "edge_mean_1s": edge.get("mean"),
                "positive_edge_fraction_1s": one_s.get("positive_edge_fraction"),
                "timely_coverage_1s": one_s.get("timely_coverage"),
            }
        )
    top10_summary = top10.get("summary") if isinstance(top10.get("summary"), dict) else {}
    return {
        "status": state.get("status"),
        "gate_status": state.get("gate_status"),
        "run_id": state.get("run_id"),
        "completed_at": state.get("completed_at"),
        "path": str(state_path),
        "selected_wallet_count": len(selected),
        "gates": state.get("gates") if isinstance(state.get("gates"), dict) else {},
        "top10": {
            "status": top10.get("status"),
            "blockers": top10.get("blockers") or [],
            "rows_scanned": ((top10.get("source") or {}).get("rows_scanned")),
            "buy_events": top10_summary.get("buy_events"),
            "copyable_buy_events": top10_summary.get("copyable_buy_events"),
            "paper_pnl_usd": top10_summary.get("paper_pnl_usd"),
        },
        "alpha": {
            "status": alpha.get("status"),
            "fills_total": alpha.get("fills_total"),
            "fills_with_any_book_coverage": alpha.get("fills_with_any_book_coverage"),
            "overlap_s": ((alpha.get("capture_windows") or {}).get("overlap_s")),
        },
        "exact_policy": exact_policy,
        "paper_only": state.get("paper_only"),
        "live_orders_allowed": state.get("live_orders_allowed"),
    }


def _norm_wallet_for_digest(value: Any) -> str:
    wallet = str(value or "").strip().lower()
    return wallet if wallet.startswith("0x") and len(wallet) == 42 else ""


def _rel_path(path: Path | None, root: Path = ROOT) -> str:
    if path is None:
        return ""
    try:
        return str(path.relative_to(root)).replace(os.sep, "/")
    except ValueError:
        return str(path)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_utc_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _flow_episode_summary(
    data_dir: Path,
    now: datetime,
    order_flow_deadman: dict[str, Any] | None = None,
) -> dict[str, Any]:
    episode_path = data_dir / "order_flow_deadman_episodes.jsonl"
    manifest_path = data_dir / "order_flow_deadman_episodes_manifest.json"
    rows = tail_incident_rows(
        episode_path,
        manifest_path=manifest_path,
        max_rows=10_000,
    )
    manifest = _load_json(manifest_path, {})
    ledger_scope = (
        manifest.get("ledger_scope")
        if isinstance(manifest, dict) and isinstance(manifest.get("ledger_scope"), dict)
        else {
            "first_armed_commit": "09176b85",
            "first_armed_at": None,
            "prior_episodes_unrecorded": True,
            "prior_incident_rows_at_arming": 12,
            "prior_recorded_clears": 0,
            "rule": (
                "episodes closing before first_armed_at are permanently unrecorded; "
                "an empty or short ledger is NOT evidence of zero episodes"
            ),
        }
    )
    today_rows: list[dict[str, Any]] = []
    for row in rows:
        cleared = _parse_utc_ts(row.get("cleared_at"))
        if cleared is not None and cleared.date() == now.date():
            today_rows.append(row)
    total_dead_s = 0.0
    for row in today_rows:
        duration = _as_float(row.get("episode_duration_s"))
        if duration is not None:
            total_dead_s += max(0.0, duration)
    live_state = order_flow_deadman if isinstance(order_flow_deadman, dict) else {}
    open_fire_at = None
    open_episode_idle_s = None
    open_dead_s = 0.0
    if str(live_state.get("status") or "").startswith("INCIDENT_"):
        open_fire_at = live_state.get("episode_fire_at") or live_state.get("checked_at")
        open_fire_ts = _parse_utc_ts(open_fire_at)
        if open_fire_ts is not None:
            open_dead_s = round(max(0.0, (now - open_fire_ts).total_seconds()), 6)
        open_episode_idle_s = live_state.get("episode_fire_idle_s")
        if open_episode_idle_s is None:
            open_episode_idle_s = live_state.get("idle_s")
    open_episode_today = bool(
        open_fire_at
        and (open_fire_ts := _parse_utc_ts(open_fire_at)) is not None
        and open_fire_ts.date() == now.date()
    )
    return {
        "episodes_today": len(today_rows) + int(open_episode_today),
        "total_dead_s": round(total_dead_s, 6),
        "open_episode_fire_at": open_fire_at,
        "open_episode_idle_s": open_episode_idle_s,
        "open_dead_s": open_dead_s,
        "natural_clears": sum(
            1 for row in today_rows if row.get("restart_performed") is False
        ),
        "restarts_performed": sum(
            1 for row in today_rows if row.get("restart_performed") is True
        ),
        "unknown_restart_clears": sum(
            1 for row in today_rows if row.get("restart_performed") is None
        ),
        "escalations_proposed": sum(
            1
            for row in today_rows
            if str(row.get("fire_mechanical_escalation") or "NONE") != "NONE"
        ),
        "ledger_scope": ledger_scope,
        "row_count": len(rows),
        "source": "data/research/order_flow_deadman_episodes.jsonl",
    }


def _wide_heartbeat_watch_summary(
    observation_watermarks: dict[str, Any],
    fingerprint_evidence: dict[str, Any],
    *,
    now: datetime,
    freeze_resolution_accelerator: dict[str, Any] | None = None,
    order_flow_deadman: dict[str, Any] | None = None,
    wake_wallets: tuple[str, ...] = (
        "0x3048d65321be3497164cdfc2996f94f98a2e7537",
        "0x9d57c42e847173d06841703825d3fe2299e456ea",
    ),
) -> dict[str, Any]:
    freeze_resolution_accelerator = freeze_resolution_accelerator or {}
    order_flow_deadman = order_flow_deadman or {}
    wallets = (
        observation_watermarks.get("wallets")
        if isinstance(observation_watermarks.get("wallets"), dict)
        else {}
    )
    liveness_wake_watches: list[dict[str, Any]] = []
    for wake_wallet in wake_wallets:
        wake_row = wallets.get(wake_wallet, {})
        wake_row = wake_row if isinstance(wake_row, dict) else {}
        latest_event_ts = _as_float(wake_row.get("latest_matching_event_ts"))
        latest_event_at = (
            datetime.fromtimestamp(latest_event_ts, tz=timezone.utc)
            if latest_event_ts is not None
            else None
        )
        wake_age_h = (
            round(max(0.0, (now - latest_event_at).total_seconds()) / 3600.0, 6)
            if latest_event_at is not None
            else None
        )
        liveness_wake_watches.append(
            {
                "wallet": wake_wallet,
                "latest_own_buy_ts": latest_event_ts,
                "latest_own_buy_at": (
                    latest_event_at.isoformat().replace("+00:00", "Z")
                    if latest_event_at is not None
                    else None
                ),
                "age_h": wake_age_h,
                "trigger_age_lte_h": 24.0,
                "triggered": wake_age_h is not None and wake_age_h <= 24.0,
                "source": "wallet_copy_rtds_observation_watermarks.wallets",
                "on_trigger": (
                    "mechanically_requeue_and_rescore_candidate_only"
                    if wake_wallet
                    == "0x9d57c42e847173d06841703825d3fe2299e456ea"
                    else "build_fable_readmission_packet"
                ),
                "live_enablement_from_wake": False,
            }
        )

    qualifying_cells: list[dict[str, Any]] = []
    for cell in fingerprint_evidence.get("cells") or []:
        if not isinstance(cell, dict):
            continue
        rescore = venue_gate_summary(cell)
        first_half = _as_float(rescore.get("first_half_post_fee_pnl_usd"))
        second_half = _as_float(rescore.get("second_half_post_fee_pnl_usd"))
        if not (
            rescore.get("f1_pass") is True
            and first_half is not None
            and first_half > 0
            and second_half is not None
            and second_half > 0
        ):
            continue
        identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        qualifying_cells.append(
            {
                "wallet": str(identity.get("wallet") or cell.get("wallet") or "").lower(),
                "wide_policy_fingerprint": str(
                    identity.get("wide_policy_fingerprint")
                    or cell.get("wide_policy_fingerprint")
                    or ""
                ),
            }
        )
    qualifying_wallets = sorted(
        {row["wallet"] for row in qualifying_cells if row.get("wallet")}
    )
    climb_rows = (
        freeze_resolution_accelerator.get("direct_climb_priority")
        if isinstance(
            freeze_resolution_accelerator.get("direct_climb_priority"), list
        )
        else []
    )
    climb_identity = (
        climb_rows[0]
        if climb_rows and isinstance(climb_rows[0], dict)
        else {}
    )
    fresh_forward_clock = (
        freeze_resolution_accelerator.get("fresh_forward_clock")
        if isinstance(
            freeze_resolution_accelerator.get("fresh_forward_clock"), dict
        )
        else {}
    )
    guard_memory = (
        order_flow_deadman.get("guard_memory")
        if isinstance(order_flow_deadman.get("guard_memory"), dict)
        else {}
    )
    best_by_wallet = (
        fingerprint_evidence.get("best_by_wallet")
        if isinstance(fingerprint_evidence.get("best_by_wallet"), dict)
        else {}
    )
    rejected_by_wallet = (
        fingerprint_evidence.get("best_by_wallet_rejected_for_concentration")
        if isinstance(
            fingerprint_evidence.get("best_by_wallet_rejected_for_concentration"), dict
        )
        else {}
    )
    walk_forward_by_wallet = (
        fingerprint_evidence.get("walk_forward_best_by_wallet")
        if isinstance(fingerprint_evidence.get("walk_forward_best_by_wallet"), dict)
        else {}
    )

    def _compact_fingerprint_row(row: Any) -> dict[str, Any]:
        if not isinstance(row, dict):
            return {}
        identity = row.get("identity") if isinstance(row.get("identity"), dict) else {}
        rescore = venue_gate_summary(row)
        return {
            "wallet": identity.get("wallet") or row.get("wallet"),
            "wide_policy_fingerprint": (
                identity.get("wide_policy_fingerprint")
                or row.get("wide_policy_fingerprint")
            ),
            "resolved": rescore.get("resolved"),
            "post_fee_pnl_usd": rescore.get("post_fee_pnl_usd"),
            "roi_pct": rescore.get("roi_pct"),
            "concentration_admissible": rescore.get("concentration_admissible"),
            "genuine_concentration_edge": rescore.get("genuine_concentration_edge"),
            "both_halves_positive": (
                (_as_float(rescore.get("first_half_post_fee_pnl_usd")) or 0.0) > 0
                and (_as_float(rescore.get("second_half_post_fee_pnl_usd")) or 0.0) > 0
            ),
            "f1_pass": rescore.get("f1_pass"),
            "f1_walk_forward_admissible": rescore.get("f1_walk_forward_admissible"),
            "venue_reachable_share_pct": rescore.get("venue_reachable_share_pct"),
            "f1_venue_reachable_admissible": rescore.get(
                "f1_venue_reachable_admissible"
            ),
            "first_half": rescore.get("first_half")
            if isinstance(rescore.get("first_half"), dict)
            else {},
            "second_half": rescore.get("second_half")
            if isinstance(rescore.get("second_half"), dict)
            else {},
        }

    walk_forward_admissible_selected_wallets = [
        _compact_fingerprint_row(row)
        for row in walk_forward_by_wallet.values()
        if isinstance(row, dict)
        and venue_gate_summary(row).get("f1_walk_forward_admissible") is True
    ]
    walk_forward_admissible_all_cells = [
        _compact_fingerprint_row(row)
        for row in fingerprint_evidence.get("cells") or []
        if isinstance(row, dict)
        and venue_gate_summary(row).get("f1_walk_forward_admissible") is True
    ]
    walk_forward_all_cells_triggered = (
        len(walk_forward_admissible_all_cells) > 5
    )
    return {
        # Preserve the original 3048 field for compatibility while exposing
        # the complete standing wake roster below.
        "wake_watch": {
            key: value
            for key, value in liveness_wake_watches[0].items()
            if key not in {"on_trigger", "live_enablement_from_wake"}
        },
        "liveness_wake_watches": liveness_wake_watches,
        "triggered_wake_wallets": [
            row["wallet"] for row in liveness_wake_watches if row["triggered"]
        ],
        "dual_half_positive_inventory": {
            "f1_pass_cell_count": len(qualifying_cells),
            "distinct_wallet_count": len(qualifying_wallets),
            "wallets": qualifying_wallets,
            "source": "wide_policy_fingerprint_evidence.cells.venue_executable_full_stream_rescore",
        },
        "fingerprint_evidence_selection": {
            "best_by_wallet": {
                wallet: _compact_fingerprint_row(row)
                for wallet, row in best_by_wallet.items()
            },
            "best_by_wallet_rejected_for_concentration": rejected_by_wallet,
            "walk_forward_selected_wallet_count": len(walk_forward_by_wallet),
            "walk_forward_admissible_count": len(
                walk_forward_admissible_selected_wallets
            ),
            "walk_forward_admissible_count_selected_wallets": len(
                walk_forward_admissible_selected_wallets
            ),
            "walk_forward_admissible_count_all_cells": len(
                walk_forward_admissible_all_cells
            ),
            "walk_forward_admissible_selected_wallets": (
                walk_forward_admissible_selected_wallets
            ),
            "walk_forward_admissible": walk_forward_admissible_selected_wallets,
            "walk_forward_admissible_all_cells": walk_forward_admissible_all_cells,
            "walk_forward_all_cells_gt_5_triggered": (
                walk_forward_all_cells_triggered
            ),
            "walk_forward_all_cells_gt_5_trigger_rule": (
                "walk_forward_admissible_count_all_cells > 5"
            ),
            "source": "wide_policy_fingerprint_evidence_latest.json",
        },
        "fresh_forward_climb": {
            "direction_id": freeze_resolution_accelerator.get("direction_id"),
            "wallet": climb_identity.get("wallet"),
            "wide_policy_fingerprint": climb_identity.get(
                "wide_policy_fingerprint"
            ),
            "started_at": fresh_forward_clock.get("started_at"),
            "status": fresh_forward_clock.get("status"),
            "target_resolved": fresh_forward_clock.get("target_resolved"),
            "baseline_full_window": fresh_forward_clock.get(
                "baseline_full_window"
            )
            if isinstance(
                fresh_forward_clock.get("baseline_full_window"), dict
            )
            else {},
            "kill_rules": fresh_forward_clock.get("kill_rules")
            if isinstance(fresh_forward_clock.get("kill_rules"), dict)
            else {},
            "source_drought_policy": fresh_forward_clock.get(
                "source_drought_policy"
            )
            if isinstance(
                fresh_forward_clock.get("source_drought_policy"), dict
            )
            else {},
            "unresolved_window_count": freeze_resolution_accelerator.get(
                "direct_climb_priority_unresolved_window_count"
            ),
            "paper_only": freeze_resolution_accelerator.get("paper_only"),
            "live_orders_allowed": freeze_resolution_accelerator.get(
                "live_orders_allowed"
            ),
            "source": "freeze_resolution_accelerator_state",
        },
        "guard_rss_watch": {
            "pid": guard_memory.get("pid"),
            "rss_gib": guard_memory.get("rss_gib"),
            "threshold_rss_gib": guard_memory.get("threshold_rss_gib"),
            "threshold_rss_source": guard_memory.get("threshold_rss_source"),
            "in_process_stage_boundary_rss": (
                guard_memory.get("in_process_stage_boundary_rss")
                if isinstance(guard_memory.get("in_process_stage_boundary_rss"), dict)
                else {}
            ),
            "rss_observation_threshold_grade": guard_memory.get(
                "rss_observation_threshold_grade"
            ),
            "warn_gib": guard_memory.get("warn_gib"),
            "restart_gib": guard_memory.get("restart_gib"),
            "rss_observation": (
                guard_memory.get("rss_observation")
                if isinstance(guard_memory.get("rss_observation"), dict)
                else {}
            ),
            "source": "order_flow_deadman_state.guard_memory",
        },
    }


def _latest_guard_restart_scope_start(order_flow_deadman: dict[str, Any]) -> str | None:
    guard_memory = (
        order_flow_deadman.get("guard_memory")
        if isinstance(order_flow_deadman.get("guard_memory"), dict)
        else {}
    )
    current_pid = guard_memory.get("pid")
    restart_gib = _as_float(guard_memory.get("restart_gib"))
    samples = guard_memory.get("samples") if isinstance(guard_memory.get("samples"), list) else []
    restart_at: datetime | None = None
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        sample_pid = sample.get("pid")
        sample_rss = _as_float(sample.get("rss_gib"))
        sample_ts = _parse_utc_ts(sample.get("checked_at"))
        if sample_ts is None:
            continue
        crossed_threshold = (
            restart_gib is not None
            and sample_rss is not None
            and sample_rss >= restart_gib
        )
        if current_pid is not None and sample_pid != current_pid and crossed_threshold:
            if restart_at is None or sample_ts > restart_at:
                restart_at = sample_ts
    if restart_at is None:
        return None
    return restart_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _guard_memory_pid(order_flow_deadman: dict[str, Any]) -> int | None:
    guard_memory = (
        order_flow_deadman.get("guard_memory")
        if isinstance(order_flow_deadman.get("guard_memory"), dict)
        else {}
    )
    pid = guard_memory.get("pid")
    try:
        return int(pid)
    except (TypeError, ValueError):
        return None


def _latest_post_restart_scope_start_from_handoff(entries: list[dict[str, str]]) -> str | None:
    patterns = (
        r"after\s+the\s+(\d{2}:\d{2}:\d{2})(?:\.\d+)?Z\s+restart",
        r"restart(?:ed|s| executed| execution)?\s+(?:at|=|==)\s*(\d{2}:\d{2}:\d{2})(?:\.\d+)?Z",
        r"(\d{2}:\d{2}:\d{2})(?:\.\d+)?Z\s*(?:[-:;,.]|—)?\s*(?:old guard|restart|started_pid|acceptance clock)",
        r"acceptance(?: clock)?(?: starts now)?[^.\n]*\s(?:at|begins at)\s+(\d{2}:\d{2}:\d{2})(?:\.\d+)?Z",
        r"post-restart[^.\n]*\bbegins\s+at\s+(\d{2}:\d{2}:\d{2})(?:\.\d+)?Z",
    )
    for entry in reversed(entries):
        text = f"{entry.get('heading', '')}\n{entry.get('body', '')}"
        lowered = text.lower()
        explicit_after_restart = re.search(
            r"after\s+the\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s+restart",
            text,
            flags=re.IGNORECASE,
        )
        if "restart" not in lowered or not (
            "post-restart" in lowered
            or "rss-16" in lowered
            or "acceptance clock" in lowered
            or "canonical restart" in lowered
            or explicit_after_restart
        ):
            continue
        entry_ts = _entry_timestamp(entry)
        if entry_ts is None:
            continue
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            parsed = _parse_utc_ts(f"{entry_ts.date().isoformat()}T{match.group(1)}Z")
            if parsed is not None:
                return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return None


def _wallet_outflow_external_wait_explanation(
    wallet_outflow_deadman: dict[str, Any],
    row: dict[str, Any],
) -> dict[str, Any] | None:
    metric = str(row.get("metric") or "")
    if metric != "brainless_step:wallet_outflow_deadman":
        return None
    current = _as_float(row.get("current"))
    baseline = _as_float(row.get("baseline"))
    if current is None or baseline is None:
        return None
    delta_s = current - baseline
    if delta_s <= 0:
        return None
    fetch = wallet_outflow_deadman.get("fetch") if isinstance(wallet_outflow_deadman.get("fetch"), dict) else {}
    attempts = fetch.get("transfer_attempts") if isinstance(fetch.get("transfer_attempts"), list) else None
    if attempts is None:
        attempts = wallet_outflow_deadman.get("transfer_attempts")
    if not isinstance(attempts, list):
        return None
    external_attempts: list[dict[str, Any]] = []
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        source = str(attempt.get("source") or attempt.get("fetch_source") or attempt.get("url_source") or "")
        status = str(attempt.get("status") or attempt.get("fetch_status") or "")
        duration_s = _as_float(attempt.get("duration_s"))
        if duration_s is None:
            continue
        source_l = source.lower()
        status_l = status.lower()
        providerish = any(token in source_l for token in ("polygon", "blockscout", "rpc"))
        degraded = any(token in status_l for token in ("error", "403", "timeout", "cooldown", "5xx", "degraded"))
        if providerish and (degraded or duration_s >= 5.0):
            external_attempts.append(
                {
                    "source": source,
                    "status": status,
                    "duration_s": round(duration_s, 6),
                }
            )
    if not external_attempts:
        return None
    external_wait_s = sum(_as_float(attempt.get("duration_s")) or 0.0 for attempt in external_attempts)
    dominant = max(external_attempts, key=lambda attempt: _as_float(attempt.get("duration_s")) or 0.0)
    if external_wait_s < max(RUNTIME_SPEED_ABS_DELTA_FLOOR_S, delta_s * 0.75):
        return None
    return {
        "classification": "EXPLAINED_EXTERNAL",
        "rule": "wallet_outflow runtime delta dominated by recorded external transfer_attempts wait",
        "delta_s": round(delta_s, 6),
        "external_wait_s": round(external_wait_s, 6),
        "dominant_attempt": dominant,
    }


def _runtime_speed_effective_persistence_status(
    row: dict[str, Any],
    wallet_outflow_deadman: dict[str, Any],
) -> tuple[str, dict[str, Any] | None]:
    status = str(row.get("status") or "")
    if status != "REGRESSION":
        return status, None
    external = _wallet_outflow_external_wait_explanation(wallet_outflow_deadman, row)
    if external is not None:
        return "EXPLAINED_EXTERNAL", external
    current = _as_float(row.get("current"))
    baseline = _as_float(row.get("baseline"))
    if current is not None and baseline is not None:
        delta_s = current - baseline
        if delta_s < RUNTIME_SPEED_ABS_DELTA_FLOOR_S:
            return (
                "PASS_ABS_DELTA_LT_1S",
                {
                    "classification": "PASS_ABS_DELTA_LT_1S",
                    "rule": "ratio regression ignored when absolute delta is below 1s",
                    "delta_s": round(delta_s, 6),
                    "absolute_delta_floor_s": RUNTIME_SPEED_ABS_DELTA_FLOOR_S,
                },
            )
    return status, None


def _runtime_speed_persistence_counts(
    runtime_speed: dict[str, Any],
    previous_digest: dict[str, Any],
    order_flow_deadman: dict[str, Any],
    wallet_outflow_deadman: dict[str, Any] | None = None,
    *,
    fallback_scope_start_at: str | None = None,
) -> dict[str, Any]:
    comparison = runtime_speed.get("comparison") if isinstance(runtime_speed.get("comparison"), dict) else {}
    rows = comparison.get("rows") if isinstance(comparison.get("rows"), list) else []
    previous_runtime = (
        previous_digest.get("runtime_speed_baseline")
        if isinstance(previous_digest.get("runtime_speed_baseline"), dict)
        else {}
    )
    previous_counts = (
        previous_runtime.get("persistence_counts")
        if isinstance(previous_runtime.get("persistence_counts"), dict)
        else {}
    )
    previous_rows = previous_counts.get("rows") if isinstance(previous_counts.get("rows"), dict) else {}
    sample_scope_start_at = _latest_guard_restart_scope_start(order_flow_deadman)
    fallback_scope_start_at = (
        fallback_scope_start_at
        if _parse_utc_ts(fallback_scope_start_at) is not None
        else None
    )
    scope_start_at = sample_scope_start_at or fallback_scope_start_at
    current_pid = _guard_memory_pid(order_flow_deadman)
    previous_scope_start_at = previous_counts.get("scope_start_at")
    previous_scope_pid = previous_counts.get("scope_pid")
    same_scope_pid = True
    if previous_scope_pid is not None and current_pid is not None:
        try:
            same_scope_pid = int(previous_scope_pid) == int(current_pid)
        except (TypeError, ValueError):
            same_scope_pid = False
    same_scope = bool(scope_start_at and previous_scope_start_at == scope_start_at and same_scope_pid)
    same_runtime_sample = previous_counts.get("runtime_speed_generated_at") == runtime_speed.get("generated_at")
    count_rows: dict[str, dict[str, Any]] = {}
    actionable: list[str] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        metric = str(row.get("metric") or "")
        if not metric:
            continue
        raw_status = str(row.get("status") or "")
        status, exclusion = _runtime_speed_effective_persistence_status(
            row,
            wallet_outflow_deadman if isinstance(wallet_outflow_deadman, dict) else {},
        )
        over_threshold = status == "REGRESSION"
        previous_row = previous_rows.get(metric) if isinstance(previous_rows, dict) else None
        previous_count = (
            _as_int((previous_row or {}).get("consecutive_over_threshold"))
            if same_scope and isinstance(previous_row, dict)
            else 0
        )
        if over_threshold and same_runtime_sample and same_scope and previous_count > 0:
            count = previous_count
        elif over_threshold:
            count = previous_count + 1
        else:
            count = 0
        count_rows[metric] = {
            "consecutive_over_threshold": count,
            "status": status,
            "raw_status": raw_status,
            "ratio": row.get("ratio"),
            "threshold_ratio": row.get("threshold_ratio"),
            "current": row.get("current"),
            "baseline": row.get("baseline"),
            "comparison_rule": row.get("comparison_rule"),
        }
        if exclusion is not None:
            count_rows[metric]["regression_exclusion"] = exclusion
        if count >= 3:
            actionable.append(metric)

    money_path_metrics = {
        name: count_rows.get(name)
        for name in (
            "brainless_run_duration_s",
            "brainless_step:wallet_outflow_deadman",
            "guard_cycle_total_s",
            "signal_age_p90_s",
        )
        if count_rows.get(name) is not None
    }

    return {
        "rule": (
            "maintenance rows require 3 consecutive post-restart heartbeat REGRESSION "
            "counts before same-day fix; external-wait rows and ratio rows with "
            "absolute delta <1s do not count; money-path rows must PASS separately"
        ),
        "scope": "post_restart" if scope_start_at else "unscoped",
        "scope_start_at": scope_start_at,
        "scope_source": (
            "guard_memory_samples"
            if sample_scope_start_at
            else "handoff_direction"
            if fallback_scope_start_at
            else None
        ),
        "scope_pid": current_pid,
        "runtime_speed_generated_at": runtime_speed.get("generated_at"),
        "threshold_consecutive": 3,
        "rows": count_rows,
        "actionable_candidates": actionable,
        "money_path_rows": money_path_metrics,
    }


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _external_liveness_rows_by_wallet_from_probe(probe: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: list[Any] = []
    for key in ("rows", "selected_rows"):
        values = probe.get(key)
        if isinstance(values, list):
            rows.extend(values)
    remote = probe.get("remote_dataapi_24h") if isinstance(probe.get("remote_dataapi_24h"), dict) else {}
    if isinstance(remote.get("rows"), list):
        rows.extend(remote["rows"])
    if isinstance(probe.get("paper_shadow_enrollments"), list):
        rows.extend(probe["paper_shadow_enrollments"])
    out: dict[str, dict[str, Any]] = {}

    def row_rank(row: dict[str, Any]) -> tuple[int, int, float]:
        has_trade_ts = 1 if (_as_float(row.get("latest_btc5m_trade_ts") or row.get("latest_trade_ts")) or 0) > 0 else 0
        selected = 1 if row.get("selected_this_run") is True else 0
        try:
            fetched_at_s = float(row.get("fetched_at_s") or 0.0)
        except (TypeError, ValueError):
            fetched_at_s = 0.0
        if row.get("checkpoint_carryover") is True:
            selected -= 1
        return has_trade_ts, selected, fetched_at_s

    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet:
            existing = out.get(wallet)
            if existing is None or row_rank(row) >= row_rank(existing):
                out[wallet] = row
    return out


def _external_liveness_age_h_for_digest(
    row: dict[str, Any],
    *,
    now: datetime,
    probe_generated_at: datetime | None,
) -> float | None:
    for key in ("latest_btc5m_trade_ts", "latest_trade_ts"):
        try:
            trade_ts = float(row.get(key) or 0.0)
        except (TypeError, ValueError):
            trade_ts = 0.0
        if trade_ts > 0:
            return max(0.0, (now.timestamp() - trade_ts) / 3600.0)
    for key in ("latest_trade_age_h", "remote_dataapi_latest_trade_age_h"):
        age_h = _as_float(row.get(key))
        if age_h is None or age_h < 0:
            continue
        if probe_generated_at is not None:
            age_h += max(0.0, (now - probe_generated_at).total_seconds() / 3600.0)
        return age_h
    return None


def _external_liveness_passes_for_digest(
    row: dict[str, Any],
    *,
    now: datetime,
    probe_generated_at: datetime | None,
    max_age_h: float = 24.0,
) -> tuple[bool, str, float | None]:
    status = str(row.get("status") or "").upper()
    censored = bool(row.get("censored")) or status == "CENSORED_PAGINATION_CAP"
    age_h = _external_liveness_age_h_for_digest(row, now=now, probe_generated_at=probe_generated_at)
    btc5m_trades = _as_int(
        row.get("btc5m_trades_24h")
        or row.get("remote_dataapi_btc5m_trades_24h")
        or row.get("btc5m_buys_24h")
        or row.get("remote_dataapi_btc5m_buys_24h"),
        default=0,
    )
    if status == "ERROR":
        return False, "external_liveness_error", age_h
    if censored:
        return False, "external_liveness_censored", age_h
    if age_h is None:
        return False, "external_liveness_no_btc5m_trade_ts", age_h
    if age_h >= max_age_h:
        return False, "external_liveness_age_gte_24h", age_h
    if btc5m_trades <= 0:
        return False, "external_liveness_zero_btc5m_trades_24h", age_h
    return True, "external_liveness_pass", age_h


def _cohort_replay_alive_profitable_summary(
    cohort_replay: dict[str, Any],
    fresh_flow_probe: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    picks = cohort_replay.get("live_ready_picks") if isinstance(cohort_replay.get("live_ready_picks"), list) else []
    rows_by_wallet = _external_liveness_rows_by_wallet_from_probe(fresh_flow_probe)
    probe_generated_at = _parse_utc_ts(fresh_flow_probe.get("generated_at"))
    alive: list[dict[str, Any]] = []
    missing = 0
    fail_counts: dict[str, int] = {}
    for pick in picks:
        if not isinstance(pick, dict):
            continue
        wallet = _norm_wallet(pick.get("wallet") or pick.get("source_wallet"))
        row = rows_by_wallet.get(wallet)
        if not wallet or row is None:
            missing += 1
            fail_counts["external_liveness_row_missing"] = fail_counts.get("external_liveness_row_missing", 0) + 1
            continue
        passed, reason, age_h = _external_liveness_passes_for_digest(
            row,
            now=now,
            probe_generated_at=probe_generated_at,
        )
        if not passed:
            fail_counts[reason] = fail_counts.get(reason, 0) + 1
            continue
        alive.append(
            {
                "wallet": wallet,
                "paper_pnl_usd": pick.get("paper_pnl_usd"),
                "roi_pct": pick.get("roi_pct"),
                "resolved_copyable_events": pick.get("resolved_copyable_events"),
                "latest_trade_age_h": None if age_h is None else round(age_h, 6),
            }
        )
    alive.sort(
        key=lambda row: (
            -float(row.get("paper_pnl_usd") or 0.0),
            str(row.get("wallet") or ""),
        )
    )
    return {
        "definition": "count of cohort_replay live_ready_picks whose external Data-API BTC-5m liveness passes the same <24h gate used by the live guard",
        "raw_live_ready_picks": len([row for row in picks if isinstance(row, dict)]),
        "scanned_alive_profitable": len(alive),
        "liveness_probe_generated_at": fresh_flow_probe.get("generated_at"),
        "liveness_probe_rows": len(rows_by_wallet),
        "missing_liveness_rows": missing,
        "failed_liveness_reason_counts": fail_counts,
        "top_alive_wallets": alive[:5],
    }


def _iter_commitment_evidence_paths(root: Path, pattern: str) -> list[str]:
    rel_paths: list[str] = []
    prefixes = {
        "data/research/": root / "data" / "research",
        "docs/agents/": root / "docs" / "agents",
        "scripts/": root / "scripts",
        "tests/": root / "tests",
    }
    for prefix, base in prefixes.items():
        if pattern.startswith(prefix) and base.exists():
            try:
                rel_paths.extend(
                    str(path.relative_to(root)).replace(os.sep, "/")
                    for path in base.iterdir()
                    if path.is_file()
                )
            except OSError:
                return []
            return rel_paths
    return rel_paths


def _commitment_evidence_found(root: Path, evidence_pattern: Any) -> bool:
    if not isinstance(evidence_pattern, str) or not evidence_pattern:
        return False
    for raw_part in evidence_pattern.split("|"):
        part = raw_part.strip()
        if not part:
            continue
        if ":" in part:
            rel_path, text_pattern = part.split(":", 1)
            path = root / rel_path
            try:
                text = path.read_text()
            except OSError:
                continue
            try:
                if re.search(text_pattern, text, flags=re.IGNORECASE | re.MULTILINE):
                    return True
            except re.error:
                continue
            continue
        exact_path = root / part
        if exact_path.exists():
            return True
        rel_paths = _iter_commitment_evidence_paths(root, part)
        for rel_path in rel_paths:
            try:
                if re.search(part, rel_path, flags=re.IGNORECASE):
                    return True
            except re.error:
                if part == rel_path:
                    return True
    return False


def _commitments_overdue_summary(root: Path, *, now: datetime) -> dict[str, Any]:
    path = root / "data" / "research" / "commitments.jsonl"
    rows: list[dict[str, Any]] = []
    parse_errors = 0
    try:
        raw_lines = path.read_text().splitlines()
    except OSError:
        return {
            "path": str(path),
            "exists": False,
            "rows": 0,
            "active": 0,
            "late": 0,
            "due_today": 0,
            "overdue": 0,
            "oldest_id": None,
            "oldest_due_ts": None,
            "evidence_unmarked": 0,
            "overdue_with_evidence": 0,
            "parse_errors": 0,
            "sample_ids": [],
        }
    for line in raw_lines:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            parse_errors += 1
            continue
        if isinstance(obj, dict):
            rows.append(obj)

    active: list[dict[str, Any]] = []
    late = 0
    due_today = 0
    evidence_unmarked = 0
    overdue_with_evidence = 0
    overdue: list[tuple[datetime, dict[str, Any]]] = []
    today = now.date()
    for row in rows:
        status = str(row.get("status") or "").upper()
        if status in TERMINAL_COMMITMENT_STATUSES:
            continue
        active.append(row)
        if status == "LATE":
            late += 1
        due_ts = _parse_utc_ts(row.get("due_ts"))
        if due_ts is not None and due_ts.date() == today:
            due_today += 1
        evidence_found = _commitment_evidence_found(root, row.get("evidence_pattern"))
        if evidence_found:
            if status not in {"SATISFIED", "CLOSED"}:
                evidence_unmarked += 1
        if due_ts is not None and due_ts <= now:
            overdue.append((due_ts, row))
            if evidence_found:
                overdue_with_evidence += 1
    overdue.sort(key=lambda item: (item[0], str(item[1].get("id") or "")))
    oldest = overdue[0][1] if overdue else {}
    oldest_due = overdue[0][0].isoformat().replace("+00:00", "Z") if overdue else None
    return {
        "path": str(path),
        "exists": True,
        "rows": len(rows),
        "active": len(active),
        "late": late,
        "due_today": due_today,
        "overdue": len(overdue),
        "oldest_id": oldest.get("id"),
        "oldest_due_ts": oldest_due,
        "evidence_unmarked": evidence_unmarked,
        "overdue_with_evidence": overdue_with_evidence,
        "parse_errors": parse_errors,
        "sample_ids": [str(row.get("id") or "") for _, row in overdue[:8]],
    }


def _defense_tripwires(
    score_total: dict[str, Any],
    since_topup: dict[str, Any],
    per_window_histogram: dict[str, Any],
    *,
    effective_stake_usd: float = 1.0,
) -> dict[str, Any]:
    """Fable 2026-07-11T06:00Z loss floors for pulling ORDER4 forward."""
    day_pnl = _as_float(score_total.get("pnl_usd"))
    since_topup_actual = _as_float(since_topup.get("actual_delta_vs_baseline_usd"))
    bucket_counts = (
        per_window_histogram.get("bucket_counts")
        if isinstance(per_window_histogram.get("bucket_counts"), dict)
        else {}
    )
    reported_stake = max(0.01, _as_float(effective_stake_usd) or 1.0)
    # Keep sub-$1 triggers conservative while reporting R against actual stake.
    trigger_stake = max(1.0, reported_stake)
    stake_admissible_max = round(
        abs(DEFENSE_DAY_PNL_FLOOR_USD)
        / (
            abs(DEFENSE_STANDARD_INTRADAY_PROBE_TRIGGER_USD)
            + abs(DEFENSE_SINGLE_FILL_LOSS_USD)
        ),
        6,
    )
    trigger_floor_guard = abs(DEFENSE_DAY_PNL_FLOOR_USD + 3.0)
    first_clamp_stake = round(
        trigger_floor_guard / abs(DEFENSE_STANDARD_INTRADAY_PROBE_TRIGGER_USD),
        6,
    )
    posture_collapse_stake = round(
        trigger_floor_guard / abs(DEFENSE_FLOOR_BREACH_INTRADAY_PROBE_TRIGGER_USD),
        6,
    )
    stake_exceeds_bankroll_admissible = reported_stake > stake_admissible_max
    ladder_degenerate = reported_stake >= posture_collapse_stake
    single_fill_probe_trigger = max(
        DEFENSE_SINGLE_FILL_LOSS_USD * trigger_stake,
        DEFENSE_DAY_PNL_FLOOR_USD + 3.0,
    )
    lte_minus_5_count = _as_int(bucket_counts.get("lte_-5"), default=0)
    day_floor_triggered = day_pnl is not None and day_pnl <= DEFENSE_DAY_PNL_FLOOR_USD
    since_topup_floor_triggered = (
        since_topup_actual is not None
        and since_topup_actual < DEFENSE_SINCE_TOPUP_ACTUAL_FLOOR_USD
    )
    worst_windows = (
        per_window_histogram.get("worst_windows")
        if isinstance(per_window_histogram.get("worst_windows"), list)
        else []
    )
    min_window_pnl = _as_float(per_window_histogram.get("min_window_pnl_usd"))
    if min_window_pnl is not None:
        window_tail_triggered = min_window_pnl <= single_fill_probe_trigger
    elif trigger_stake == 1.0:
        window_tail_triggered = lte_minus_5_count > 0
    else:
        window_tail_triggered = any(
            (pnl := _as_float(row.get("pnl_usd"))) is not None
            and pnl <= single_fill_probe_trigger
            for row in worst_windows
            if isinstance(row, dict)
        )
    base_intraday_probe_trigger = (
        DEFENSE_FLOOR_BREACH_INTRADAY_PROBE_TRIGGER_USD
        if since_topup_floor_triggered
        else DEFENSE_STANDARD_INTRADAY_PROBE_TRIGGER_USD
    )
    floor_breach_probe_trigger = max(
        base_intraday_probe_trigger * trigger_stake,
        DEFENSE_DAY_PNL_FLOOR_USD + 3.0,
    )
    intraday_probe_triggered = day_pnl is not None and day_pnl <= floor_breach_probe_trigger
    single_fill_probe_triggered = since_topup_floor_triggered and window_tail_triggered
    # A cumulative floor cannot clamp the size needed to recover by itself.
    # It tightens the intraday trigger and combines with same-day loss evidence.
    floor_breach_size_defense_triggered = bool(
        single_fill_probe_triggered or intraday_probe_triggered
    )
    triggered = bool(day_floor_triggered or since_topup_floor_triggered or window_tail_triggered)
    return {
        "status": "TRIGGERED" if triggered else "OK",
        "direction_id": "2026-07-13T14:11Z-fable-floor-breach-size-defense",
        "flow_stage": "LIVE/DEFEND",
        "t1_day_pnl_floor_usd": DEFENSE_DAY_PNL_FLOOR_USD,
        "t1_day_pnl_usd": day_pnl,
        "t1_day_pnl_distance_to_floor_usd": None
        if day_pnl is None
        else round(day_pnl - DEFENSE_DAY_PNL_FLOOR_USD, 6),
        "t1_day_pnl_triggered": day_floor_triggered,
        "t1_day_floor_action": (
            "PAPER_ONLY_REST_OF_UTC_DAY" if day_floor_triggered else "NONE"
        ),
        "t1_day_floor_resume_rule": (
            "resume live at UTC rollover under standard restore evaluation"
            if day_floor_triggered
            else ""
        ),
        "t1_since_topup_actual_floor_usd": DEFENSE_SINCE_TOPUP_ACTUAL_FLOOR_USD,
        "t1_since_topup_actual_usd": since_topup_actual,
        "t1_since_topup_distance_to_floor_usd": None
        if since_topup_actual is None
        else round(since_topup_actual - DEFENSE_SINCE_TOPUP_ACTUAL_FLOOR_USD, 6),
        "t1_since_topup_triggered": since_topup_floor_triggered,
        "t2_lte_minus_5_window_count": lte_minus_5_count,
        "t2_window_tail_triggered": window_tail_triggered,
        "floor_breach_defense_posture": "ARMED" if since_topup_floor_triggered else "CLEAR",
        "intraday_probe_degrade_trigger_usd": floor_breach_probe_trigger,
        "effective_stake_usd": round(reported_stake, 6),
        "stake_admissible_max_usd": stake_admissible_max,
        "stake_exceeds_bankroll_admissible": stake_exceeds_bankroll_admissible,
        "first_trigger_clamp_stake_usd": first_clamp_stake,
        "armed_clear_posture_collapse_stake_usd": posture_collapse_stake,
        "ladder_degenerate": ladder_degenerate,
        "min_window_pnl_usd": min_window_pnl,
        "trigger_r_multiple": {
            "intraday": round(abs(floor_breach_probe_trigger) / reported_stake, 6),
            "single_fill": round(abs(single_fill_probe_trigger) / reported_stake, 6),
        },
        "standard_intraday_probe_degrade_trigger_usd": max(
            DEFENSE_STANDARD_INTRADAY_PROBE_TRIGGER_USD * trigger_stake,
            DEFENSE_DAY_PNL_FLOOR_USD + 3.0,
        ),
        "floor_breach_intraday_probe_degrade_trigger_usd": max(
            DEFENSE_FLOOR_BREACH_INTRADAY_PROBE_TRIGGER_USD * trigger_stake,
            DEFENSE_DAY_PNL_FLOOR_USD + 3.0,
        ),
        "single_fill_probe_degrade_trigger_usd": single_fill_probe_trigger,
        "intraday_probe_triggered": intraday_probe_triggered,
        "single_fill_probe_triggered": single_fill_probe_triggered,
        "probe_caps_weight": DEFENSE_PROBE_WEIGHT,
        "probe_caps_cap_usd": DEFENSE_PROBE_CAP_USD,
        "size_defense_action": (
            "PAPER_ONLY_REST_OF_UTC_DAY"
            if day_floor_triggered
            else (
                "PROBE_CAPS_REST_OF_UTC_DAY"
                if floor_breach_size_defense_triggered
                else (
                    "WATCH_PROBE_TRIGGER_NO_SIZE_CHANGE"
                    if since_topup_floor_triggered and intraday_probe_triggered
                    else "WATCH_TIGHTENED_PROBE_TRIGGER"
                    if since_topup_floor_triggered
                    else "STANDARD_SIZE"
                )
            )
        ),
        "next_action": (
            "flip guard to paper_only for rest of UTC day; resume live at UTC rollover under standard restore evaluation"
            if day_floor_triggered
            else (
                "drop lane to probe caps for rest of UTC day"
                if floor_breach_size_defense_triggered
                else (
                    "record probe-watch packet; no size change at live floor"
                    if since_topup_floor_triggered and intraday_probe_triggered
                    else "hold producer; tighten intraday probe trigger while since_topup_actual < +20"
                    if since_topup_floor_triggered
                    else (
                        "ask_fable DEFEND immediately"
                        if triggered
                        else "continue 05:15 queue; report tripwire distances each heartbeat"
                    )
                )
            )
        ),
    }


def _weekend_day_probe(
    weekend_packet: dict[str, Any],
    score_total: dict[str, Any],
    *,
    generated_at: str,
) -> dict[str, Any]:
    """Packet-level weekend probe from Fable 2026-07-17T16:01Z."""
    plan = (
        weekend_packet.get("current_roster_weekend_posture_plan")
        if isinstance(weekend_packet.get("current_roster_weekend_posture_plan"), dict)
        else {}
    )
    ladder = (
        plan.get("weekend_loss_ladder")
        if isinstance(plan.get("weekend_loss_ladder"), dict)
        else {}
    )
    probe_trigger = _as_float(ladder.get("day_probe_trigger_usd"))
    day_pnl = _as_float(score_total.get("pnl_usd"))
    generated_dt = _parse_utc_ts(generated_at)
    starts_at = _parse_utc_ts(plan.get("weekend_starts_at"))
    # Explicit packet end if present; else first Monday 00:00 UTC strictly after
    # weekend_starts_at. Without an end bound, `generated_dt >= starts_at` stays
    # true forever and falsely reports weekend on weekdays (caught 2026-07-20 Mon).
    ends_at = _parse_utc_ts(plan.get("weekend_ends_at"))
    if ends_at is None and starts_at is not None:
        ends_at = starts_at
        while ends_at.weekday() != 0:  # Monday
            ends_at = ends_at + timedelta(days=1)
        if ends_at <= starts_at:
            ends_at = ends_at + timedelta(days=7)
    calendar_starts_at = None
    calendar_ends_at = None
    packet_bounds_current = False
    if generated_dt is not None:
        weekday = generated_dt.weekday()
        if weekday in (5, 6, 0):
            days_since_saturday = {5: 0, 6: 1, 0: 2}[weekday]
            calendar_starts_at = (generated_dt - timedelta(days=days_since_saturday)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        else:
            calendar_starts_at = (generated_dt + timedelta(days=5 - weekday)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        calendar_ends_at = calendar_starts_at + timedelta(days=2)
        packet_bounds_current = bool(
            starts_at == calendar_starts_at and ends_at == calendar_ends_at
        )
    if packet_bounds_current and generated_dt is not None:
        current_is_weekend = bool(starts_at <= generated_dt < ends_at)
    else:
        current_is_weekend = bool(
            generated_dt is not None and generated_dt.weekday() >= 5
        )
        starts_at = calendar_starts_at
        ends_at = calendar_ends_at
    triggered = bool(
        current_is_weekend
        and probe_trigger is not None
        and day_pnl is not None
        and day_pnl <= probe_trigger
    )
    distance = None
    if probe_trigger is not None and day_pnl is not None:
        distance = round(day_pnl - probe_trigger, 6)
    if not plan or probe_trigger is None:
        status = "MISSING_PACKET_TRIGGER"
    elif not current_is_weekend:
        # After a defined weekend window has closed, report CLOSED (not "pending open").
        if (
            starts_at is not None
            and ends_at is not None
            and generated_dt is not None
            and generated_dt >= ends_at
        ):
            status = "WEEKEND_CLOSED"
        else:
            status = "PENDING_WEEKEND_OPEN"
    else:
        status = "TRIGGERED" if triggered else "OK"
    return {
        "status": status,
        "direction_id": plan.get("direction_id") or "2026-07-17T16:01Z-fable-weekend-probe-correction",
        "flow_stage": "LIVE/DEFEND/WEEKEND",
        "packet_generated_at": weekend_packet.get("generated_at"),
        "weekend_starts_at": (
            starts_at.isoformat().replace("+00:00", "Z") if starts_at is not None else plan.get("weekend_starts_at")
        ),
        "weekend_ends_at": (
            ends_at.isoformat().replace("+00:00", "Z")
            if ends_at is not None
            else plan.get("weekend_ends_at")
        ),
        "current_is_weekend": current_is_weekend,
        "packet_bounds_current_week": packet_bounds_current,
        "weekend_day_probe_trigger_usd": probe_trigger,
        "day_pnl_usd": day_pnl,
        "distance_to_weekend_probe_usd": distance,
        "machine_tripwire_controlling": False if current_is_weekend and probe_trigger is not None else None,
        "triggered": triggered,
        "size_defense_action": "PROBE_CAPS_AND_ASK_FABLE" if triggered else "NONE",
        "seat_loss_rotation_rider": {
            "enabled": True,
            "direction_id": WEEKEND_SEAT_LOSS_RIDER_DIRECTION_ID,
            "flow_stage": "LIVE/ROTATE/DEFEND/WEEKEND",
            "trigger_wallet": WEEKEND_SEAT_LOSS_RIDER_FROM_WALLET,
            "target_wallet": WEEKEND_SEAT_LOSS_RIDER_TARGET_WALLET,
            "triggered": triggered,
            "action": "ROTATE_F418_TO_A689" if triggered else "NONE",
            "rule": (
                "if the -8.0 weekend day probe fires while f418 holds the selected seat, "
                "the same guard heartbeat pins the selected member to a689"
            ),
        },
        "next_action": (
            "drop to probe size immediately, rotate f418 selected seat to a689 if f418 holds it, and call ask_fable in the same heartbeat"
            if triggered
            else "evaluate packet -8.0 weekend day probe each heartbeat; do not wait for machine -15.0 trigger"
            if current_is_weekend and probe_trigger is not None
            else "arm packet -8.0 weekend day probe at weekend open"
            if probe_trigger is not None
            else "regenerate weekend parity packet with day_probe_trigger_usd"
        ),
    }


def _recent_guard_latency_trigger(
    data_dir: Path,
    *,
    current_pid: int | None = None,
    threshold_s: float = 30.0,
    required_consecutive: int = 3,
    tail_rows: int = 40,
) -> dict[str, Any]:
    path = data_dir / "wallet_copy_live_guard_events.jsonl"
    try:
        lines = path.read_text().splitlines()[-max(1, int(tail_rows)) :]
    except OSError:
        return {
            "status": "NO_EVENTS",
            "threshold_s": threshold_s,
            "required_consecutive": required_consecutive,
            "max_consecutive_over_threshold": 0,
            "latest_consecutive_over_threshold": 0,
            "recent": [],
        }
    rows: list[dict[str, Any]] = []
    historical_rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        profile = event.get("guard_loop_profile") if isinstance(event.get("guard_loop_profile"), dict) else {}
        total = profile.get("total_s_before_state_write")
        try:
            total_s = None if total is None else float(total)
        except (TypeError, ValueError):
            total_s = None
        rtds_s = None
        stage_timers = profile.get("stage_timers") if isinstance(profile.get("stage_timers"), list) else []
        for timer in stage_timers:
            if not isinstance(timer, dict) or timer.get("name") != "active_set_rtds_premerge":
                continue
            try:
                rtds_s = float(timer.get("duration_s"))
            except (TypeError, ValueError):
                rtds_s = None
            break
        digest_row = {
            "generated_at": event.get("generated_at"),
            "pid": event.get("pid"),
            "total_s": None if total_s is None else round(total_s, 6),
            "active_set_rtds_premerge_s": None if rtds_s is None else round(rtds_s, 6),
            "over_threshold": bool(total_s is not None and total_s > float(threshold_s)),
        }
        historical_rows.append(digest_row)
        if current_pid is None or int(event.get("pid") or -1) == int(current_pid):
            rows.append(digest_row)

    def run_summary(candidate_rows: list[dict[str, Any]]) -> tuple[int, int, list[dict[str, Any]]]:
        max_run = 0
        current_run = 0
        trigger_rows: list[dict[str, Any]] = []
        current_rows: list[dict[str, Any]] = []
        for row in candidate_rows:
            if row["over_threshold"]:
                current_run += 1
                current_rows.append(row)
                if current_run > max_run:
                    max_run = current_run
                    trigger_rows = list(current_rows)
            else:
                current_run = 0
                current_rows = []
        return max_run, current_run, trigger_rows

    max_run, latest_run, trigger_rows = run_summary(rows)
    historical_max_run, historical_latest_run, historical_trigger_rows = run_summary(historical_rows)
    historical_triggered = historical_max_run >= int(required_consecutive)
    triggered = max_run >= int(required_consecutive)
    return {
        "status": "TRIGGERED" if triggered else "OK",
        "threshold_s": threshold_s,
        "required_consecutive": required_consecutive,
        "current_pid": current_pid,
        "scope": "current_guard_pid" if current_pid is not None else "all_recent_guard_events",
        "max_consecutive_over_threshold": max_run,
        "latest_consecutive_over_threshold": latest_run,
        "trigger_rows": trigger_rows[-int(required_consecutive) :] if triggered else [],
        "recent": rows[-8:],
        "historical_status": "TRIGGERED" if historical_triggered else "OK",
        "historical_max_consecutive_over_threshold": historical_max_run,
        "historical_latest_consecutive_over_threshold": historical_latest_run,
        "historical_trigger_rows": (
            historical_trigger_rows[-int(required_consecutive) :] if historical_triggered else []
        ),
        "historical_recent": historical_rows[-8:],
    }


def _tail_jsonl_dicts(path: Path, *, max_bytes: int = 2_000_000, max_rows: int = 200) -> list[dict[str, Any]]:
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - int(max_bytes)))
            raw = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines()[-max(1, int(max_rows)) :]:
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            rows.append(event)
    return rows


def _deadman_admission_publish_health(path: Path) -> dict[str, Any]:
    rows = [
        row
        for row in _tail_jsonl_dicts(path, max_rows=500)
        if row.get("kind") == "order_flow_deadman_admission_publish"
        and row.get("stage")
    ]
    lags = sorted(
        float(row["publish_lag_s"])
        for row in rows
        if isinstance(row.get("publish_lag_s"), (int, float))
    )
    window_counts: dict[str, int] = {}
    for row in rows:
        window = row.get("publish_window_start_s")
        if window is not None:
            key = str(window)
            window_counts[key] = window_counts.get(key, 0) + 1
    p95_index = (
        max(0, min(len(lags) - 1, math.ceil(len(lags) * 0.95) - 1))
        if lags
        else None
    )
    return {
        "sample_count": len(rows),
        "publish_lag_p95_s": lags[p95_index] if p95_index is not None else None,
        "publish_counts_by_window": window_counts,
        "authorized_crossings": sum(
            row.get("crossed") is True and row.get("authorized") is True
            for row in rows
        ),
        "stale_window_refusals": sum(
            row.get("crossed") is True
            and row.get("authorized") is False
            and row.get("status") == "ADMISSION_STALE_WINDOW"
            for row in rows
        ),
        "latest": rows[-1] if rows else {},
    }


def _maker_fallback_conversion_summary(data_dir: Path, scorecard: dict[str, Any]) -> dict[str, Any]:
    day_utc = str(scorecard.get("day_utc") or "").strip()
    canonical = scorecard.get("canonical_pnl_truth") if isinstance(scorecard.get("canonical_pnl_truth"), dict) else {}
    by_day = canonical.get("by_day") if isinstance(canonical.get("by_day"), dict) else {}
    if not day_utc and by_day:
        day_utc = sorted(str(key) for key in by_day)[-1]
    if not day_utc:
        day_utc = datetime.now(timezone.utc).date().isoformat()
    day_prefix = f"{day_utc}T"

    events = canonical.get("events") if isinstance(canonical.get("events"), list) else []
    scorecard_filled_order_ids = {
        str(event.get("order_id") or "")
        for event in events
        if isinstance(event, dict) and event.get("status") == "FILLED" and event.get("order_id")
    }
    scorecard_rejected_order_ids = {
        str(event.get("order_id") or "")
        for event in events
        if isinstance(event, dict) and event.get("status") == "REJECTED" and event.get("order_id")
    }

    states_by_order: dict[str, set[str]] = {}
    path = data_dir / "wallet_copy_live_execution_events.jsonl"
    for event in _tail_jsonl_dicts(path, max_bytes=64_000_000, max_rows=5000):
        if event.get("event") != "wallet_copy_live_lifecycle":
            continue
        if not str(event.get("ts") or "").startswith(day_prefix):
            continue
        order_id = str(event.get("order_id") or "")
        status = str(event.get("status") or "")
        if not order_id or not status:
            continue
        states_by_order.setdefault(order_id, set()).add(status)

    maker_filled = {order_id for order_id, states in states_by_order.items() if "LIVE_MAKER_FILLED" in states}
    maker_canceled = {
        order_id
        for order_id, states in states_by_order.items()
        if "LIVE_MAKER_CANCELED" in states and "LIVE_MAKER_FILLED" not in states
    }
    raw_rejected = {
        order_id
        for order_id, states in states_by_order.items()
        if "LIVE_REJECTED" in states and "LIVE_MAKER_CANCELED" not in states and "LIVE_MAKER_FILLED" not in states
    }

    return {
        "day_utc": day_utc,
        "source": _rel_path(path, data_dir.parents[1]),
        "scorecard_filled_submissions": len(scorecard_filled_order_ids),
        "scorecard_rejected_submissions": len(scorecard_rejected_order_ids),
        "lifecycle_orders_seen": len(states_by_order),
        "maker_fallback_filled_before_cancel": len(maker_filled),
        "maker_fallback_filled_before_cancel_scorecard_filled": len(
            maker_filled & scorecard_filled_order_ids
        ),
        "maker_fallback_canceled_window_end_no_fill": len(maker_canceled),
        "maker_fallback_canceled_window_end_no_fill_scorecard_rejected": len(
            maker_canceled & scorecard_rejected_order_ids
        ),
        "raw_rejected_or_unfilled": len(raw_rejected),
        "raw_rejected_or_unfilled_scorecard_rejected": len(raw_rejected & scorecard_rejected_order_ids),
    }


def _guard_profile_total_s(profile: dict[str, Any]) -> float | None:
    for key in ("total_s_before_state_write", "cycle_duration_s"):
        value = _as_float(profile.get(key))
        if value is not None:
            return round(value, 6)
    return None


def _latest_guard_loop_profile_from_events(
    data_dir: Path,
    *,
    current_pid: int | None,
    max_age_s: float = 300.0,
) -> dict[str, Any]:
    path = data_dir / "wallet_copy_live_guard_events.jsonl"
    now = datetime.now(timezone.utc)
    latest: dict[str, Any] = {}
    for event in reversed(_tail_jsonl_dicts(path)):
        if current_pid is not None:
            try:
                if int(event.get("pid") or -1) != int(current_pid):
                    continue
            except (TypeError, ValueError):
                continue
        profile = event.get("guard_loop_profile") if isinstance(event.get("guard_loop_profile"), dict) else {}
        if not profile:
            continue
        generated_at = _parse_utc_ts(event.get("generated_at"))
        age_s = None if generated_at is None else max(0.0, (now - generated_at).total_seconds())
        if age_s is not None and age_s > float(max_age_s):
            continue
        latest = dict(profile)
        latest.setdefault("generated_at", event.get("generated_at"))
        latest.setdefault("pid", event.get("pid"))
        latest["source"] = _rel_path(path, data_dir.parents[1])
        latest["age_s"] = None if age_s is None else round(age_s, 3)
        break
    return latest


def _rooted_path(root: Path, path: str) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else root / parsed


def _overlay_runtime_member_count(
    overlay_members: list[Any],
    guard_runtime_members: list[Any],
) -> int:
    """Prefer the live guard's filtered runtime roster over raw overlay rows."""
    runtime_count = len([row for row in guard_runtime_members if isinstance(row, dict)])
    if runtime_count:
        return runtime_count
    active_rows = []
    for row in overlay_members:
        if not isinstance(row, dict) or not row.get("enabled", True):
            continue
        status = str(row.get("status") or "").upper()
        if status.startswith("DEMOTED") or status.startswith("AUTO_DISABLED"):
            continue
        if row.get("total_loss_disabled") or row.get("demoted"):
            continue
        active_rows.append(row)
    return len(active_rows)


def _admission_wave_summary(
    active_set_overlay: dict[str, Any],
    guard_runtime_members: list[Any],
    live_members_today: list[dict[str, Any]],
    guard_active_runtime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wave = (
        active_set_overlay.get("latest_admission_wave")
        if isinstance(active_set_overlay.get("latest_admission_wave"), dict)
        else {}
    )
    if not wave:
        return {}
    picked_rows = wave.get("picks") if isinstance(wave.get("picks"), list) else []
    picked_wallets = [
        str(wallet).lower()
        for wallet in wave.get("picked_wallets", [])
        if str(wallet or "").strip()
    ]
    if not picked_wallets:
        picked_wallets = [
            str(row.get("wallet") or row.get("source_wallet") or "").lower()
            for row in picked_rows
            if isinstance(row, dict) and str(row.get("wallet") or row.get("source_wallet") or "").strip()
        ]
    overlay_members = active_set_overlay.get("members") if isinstance(active_set_overlay.get("members"), list) else []
    enabled_overlay_wallets = {
        str(row.get("source_wallet") or row.get("wallet") or "").lower()
        for row in overlay_members
        if isinstance(row, dict) and row.get("enabled") is not False
    }
    runtime_wallets = {
        str(row.get("source_wallet") or row.get("wallet") or "").lower()
        for row in guard_runtime_members
        if isinstance(row, dict) and str(row.get("source_wallet") or row.get("wallet") or "").strip()
    }
    filled_wallets = {
        str(row.get("wallet") or "").lower()
        for row in live_members_today
        if isinstance(row, dict) and int(row.get("fills") or 0) > 0
    }
    admitted_wallets = [
        wallet for wallet in picked_wallets if wallet in enabled_overlay_wallets
    ]
    runtime_loaded_wallets = [
        wallet for wallet in picked_wallets if wallet in runtime_wallets
    ]
    filled_wave_wallets = [
        wallet for wallet in picked_wallets if wallet in filled_wallets
    ]
    runtime_missing_wallets = [
        wallet for wallet in admitted_wallets if wallet not in runtime_wallets
    ]
    temporal_exclusion = {}
    if isinstance(guard_active_runtime, dict):
        temporal_exclusion = (
            guard_active_runtime.get("temporal_slice_live_exclusion")
            if isinstance(guard_active_runtime.get("temporal_slice_live_exclusion"), dict)
            else (
                guard_active_runtime.get("temporal_slice_exclusion")
                if isinstance(guard_active_runtime.get("temporal_slice_exclusion"), dict)
                else {}
            )
        )
    temporal_excluded_wallets = {
        str(wallet).lower()
        for wallet in temporal_exclusion.get("excluded_wallets", [])
        if str(wallet or "").strip()
    }
    wave_temporal_excluded_wallets = [
        wallet for wallet in picked_wallets if wallet in temporal_excluded_wallets
    ]
    status = wave.get("status")
    if runtime_missing_wallets and wave_temporal_excluded_wallets:
        status = "TEMPORAL_SLICE_PARTIAL_RUNTIME"
    forward_seats = wave.get("forward_seats") if isinstance(wave.get("forward_seats"), dict) else {}
    forward_seat_clocks = {
        str(key): {
            "wallet": row.get("wallet"),
            "clock_start": row.get("clock_start"),
            "clock_binding_status": row.get("clock_binding_status"),
            "route": (
                (row.get("source_binding") or {}).get("route")
                if isinstance(row.get("source_binding"), dict)
                else None
            ),
            "source_event_id": (
                (row.get("source_binding") or {}).get("source_event_id")
                if isinstance(row.get("source_binding"), dict)
                else None
            ),
            "rtds_byte_offset": (
                (((row.get("source_mux") or {}).get("rtds_cursor") or {}).get("byte_offset"))
                if isinstance(row.get("source_mux"), dict)
                else None
            ),
            "paper_only": row.get("paper_only"),
            "live_orders_allowed": row.get("live_orders_allowed"),
        }
        for key, row in forward_seats.items()
        if isinstance(row, dict)
    }
    return {
        "direction_id": wave.get("direction_id") or active_set_overlay.get("direction_id"),
        "updated_at": wave.get("updated_at") or active_set_overlay.get("updated_at"),
        "status": status,
        "configured_status": wave.get("status"),
        "picked_count": int(wave.get("picked_count") or len(picked_wallets)),
        "admitted_count": int(wave.get("admitted_count") or len(admitted_wallets)),
        "runtime_loaded_count": len(runtime_loaded_wallets),
        "runtime_missing_count": len(runtime_missing_wallets),
        "filled_count": len(filled_wave_wallets),
        "picked_wallets": picked_wallets,
        "admitted_wallets": admitted_wallets,
        "runtime_loaded_wallets": runtime_loaded_wallets,
        "runtime_missing_wallets": runtime_missing_wallets,
        "filled_wallets": filled_wave_wallets,
        "temporal_excluded_wallets": wave_temporal_excluded_wallets,
        "runtime_cap": wave.get("runtime_cap"),
        "unruled_aging_gt24h_count": wave.get("unruled_aging_gt24h_count"),
        "raw_input_timestamps": wave.get("input_timestamps"),
        "raw_output_timestamps": wave.get("raw_output_timestamps"),
        "freshness_deadman": wave.get("freshness_deadman"),
        "input_freshness": wave.get("input_freshness"),
        "own_source_rows_30m": wave.get("own_source_rows_30m"),
        "intent_count": wave.get("intent_count"),
        "submit_count": wave.get("submit_count"),
        "reported_fill_count": wave.get("fill_count"),
        "forward_seat_clocks": forward_seat_clocks,
    }


def _gate_artifact_summary(report: dict[str, Any], path: Path | None) -> dict[str, Any]:
    if not report:
        return {}
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    rows: list[dict[str, Any]] = []
    for row in report.get("members") if isinstance(report.get("members"), list) else []:
        if not isinstance(row, dict):
            continue
        source = row.get("source_activity") if isinstance(row.get("source_activity"), dict) else {}
        ledger = row.get("ledger") if isinstance(row.get("ledger"), dict) else {}
        rows.append(
            {
                "wallet": row.get("source_wallet"),
                "candidate_id": row.get("candidate_id"),
                "classification": row.get("classification"),
                "cycles_landed": row.get("cycles_landed"),
                "fresh_intents": row.get("fresh_intents"),
                "ledger_orders": ledger.get("orders"),
                "policy_eligible_windows": source.get("policy_eligible_windows"),
                "policy_eligible_rows": source.get("policy_eligible_rows"),
                "source_active_windows": source.get("source_active_windows"),
            }
        )
    return {
        "path": _rel_path(path),
        "generated_at": report.get("generated_at"),
        "gate_status": summary.get("gate_status"),
        "members": summary.get("members"),
        "members_with_cycles": summary.get("members_with_cycles"),
        "members_with_submit": summary.get("members_with_submit"),
        "total_cycles_landed": summary.get("total_cycles_landed"),
        "total_fresh_intents": summary.get("total_fresh_intents"),
        "total_ledger_orders": summary.get("total_ledger_orders"),
        "total_orders_submitted_in_cycles": summary.get("total_orders_submitted_in_cycles"),
        "classification_counts": summary.get("classification_counts")
        if isinstance(summary.get("classification_counts"), dict)
        else {},
        "rows": rows,
    }


def _wave_repair_addendum_summary(report: dict[str, Any], path: Path | None) -> dict[str, Any]:
    if not report:
        return {}
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    corrected = summary.get("pipeline_no_intents_corrected_classification_counts")
    return {
        "path": _rel_path(path),
        "generated_at": report.get("generated_at"),
        "status": summary.get("status") or report.get("status"),
        "guard_evaluates_all_runtime_members_per_cycle_current": summary.get(
            "guard_evaluates_all_runtime_members_per_cycle_current"
        ),
        "main_live_execution_selected_members_per_cycle": summary.get(
            "main_live_execution_selected_members_per_cycle"
        ),
        "all_member_flag_added": summary.get("all_member_flag_added"),
        "all_member_flag_default": summary.get("all_member_flag_default"),
        "loaded_next_managed_restart_only": summary.get("loaded_next_managed_restart_only"),
        "zero_cycle_wallets_explained": summary.get("zero_cycle_wallets_explained"),
        "single_seat_only_reclass_confirmed_for_pipeline_no_intents": summary.get(
            "single_seat_only_reclass_confirmed_for_pipeline_no_intents"
        ),
        "pipeline_no_intents_corrected_classification_counts": corrected
        if isinstance(corrected, dict)
        else {},
    }


def _deadman_r1_attribution_summary(report: dict[str, Any], path: Path | None) -> dict[str, Any]:
    if not report:
        return {}
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    selected_failed_checks = summary.get("selected_failed_checks")
    policy_reject_counts = summary.get("policy_reject_counts_fresh_buy_le_30s")
    return {
        "path": _rel_path(path),
        "generated_at": report.get("generated_at"),
        "direction_id": report.get("direction_id"),
        "status": summary.get("status") or report.get("status"),
        "expires_at": report.get("expires_at"),
        "deadman_top_status": summary.get("deadman_top_status"),
        "deadman_warning": summary.get("deadman_warning"),
        "gated_quiet_classification": summary.get("gated_quiet_classification"),
        "selected_wallet": summary.get("selected_wallet"),
        "selected_status": summary.get("selected_status"),
        "selected_failed_checks": selected_failed_checks if isinstance(selected_failed_checks, list) else [],
        "selected_live_protection_passed": summary.get("selected_live_protection_passed"),
        "fallthrough_admissible_targets": summary.get("fallthrough_admissible_targets"),
        "fresh_buy_rows_le_10s": summary.get("fresh_buy_rows_le_10s"),
        "policy_compatible_fresh_buy_rows_le_30s": summary.get(
            "policy_compatible_fresh_buy_rows_le_30s"
        ),
        "policy_reject_counts_fresh_buy_le_30s": policy_reject_counts
        if isinstance(policy_reject_counts, dict)
        else {},
        "latest_live_order_ts": summary.get("latest_live_order_ts"),
        "repair_status": summary.get("repair_status"),
        "allowlist_entry": summary.get("allowlist_entry"),
        "midnight_restart_mandatory": report.get("midnight_restart_mandatory"),
        "no_restart_before_expiry": report.get("no_restart_before_expiry"),
    }


def _trade_executor_lane_attribution_summary(report: dict[str, Any], path: Path | None) -> dict[str, Any]:
    if not report:
        return {}
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    return {
        "path": _rel_path(path),
        "generated_at": report.get("generated_at"),
        "direction_id": report.get("direction_id"),
        "status": summary.get("status") or report.get("status"),
        "executing_trade_lines_203202z": summary.get("executing_trade_lines_203202z"),
        "size_usd_gt_1_lines": summary.get("size_usd_gt_1_lines"),
        "unit_test_signature_lines": summary.get("unit_test_signature_lines"),
        "live_ledger_orders_at_2032": summary.get("live_ledger_orders_at_2032"),
        "live_execution_event_rows_at_2032": summary.get("live_execution_event_rows_at_2032"),
        "ledger_bypassing_submissions": summary.get("ledger_bypassing_submissions"),
        "only_live_guard_process_seen": summary.get("only_live_guard_process_seen"),
        "single_live_guard_pid": summary.get("single_live_guard_pid"),
        "logging_fix": summary.get("logging_fix"),
    }


def _e6db_2000_probe_cap_cut_summary(active_set_overlay: dict[str, Any]) -> dict[str, Any]:
    packet = (
        active_set_overlay.get("latest_e6db_2000_probe_cap_cut")
        if isinstance(active_set_overlay.get("latest_e6db_2000_probe_cap_cut"), dict)
        else {}
    )
    if not packet:
        return {}
    return {
        "status": packet.get("status"),
        "flow_stage": packet.get("flow_stage"),
        "direction_id": packet.get("direction_id"),
        "applied_at": packet.get("applied_at"),
        "trigger_pnl_usd": packet.get("trigger_pnl_usd"),
        "threshold_pnl_usd": packet.get("threshold_pnl_usd"),
        "live_path_mutation": packet.get("live_path_mutation"),
        "members_touched": packet.get("members_touched")
        if isinstance(packet.get("members_touched"), list)
        else [],
    }


def _runtime_total_loss_auto_disable_summary(runtime: dict[str, Any]) -> dict[str, Any]:
    total_loss = (
        runtime.get("total_loss_auto_disable")
        if isinstance(runtime.get("total_loss_auto_disable"), dict)
        else {}
    )
    disabled_members = (
        total_loss.get("disabled_members")
        if isinstance(total_loss.get("disabled_members"), list)
        else []
    )
    disabled_rows: list[dict[str, Any]] = []
    for row in disabled_members:
        if not isinstance(row, dict):
            continue
        disabled_rows.append(
            {
                "candidate_id": row.get("candidate_id"),
                "wallet": row.get("source_wallet"),
                "resolved_fills": row.get("resolved_fills"),
                "total_loss_fills": row.get("total_loss_fills"),
                "pnl_usd": row.get("pnl_usd"),
                "cost_usd": row.get("cost_usd"),
            }
        )
    return {
        "enabled": bool(total_loss.get("enabled")),
        "min_resolved_fills": total_loss.get("min_resolved_fills"),
        "rule": total_loss.get("rule"),
        "disabled_count": len(disabled_rows),
        "disabled_members": disabled_rows,
    }


def _overlay_member_is_active(row: Any) -> bool:
    if not isinstance(row, dict) or not row.get("enabled", True):
        return False
    status = str(row.get("status") or "").upper()
    if status.startswith("DEMOTED") or status.startswith("AUTO_DISABLED"):
        return False
    if row.get("total_loss_disabled") or row.get("demoted"):
        return False
    return True


def _latest_active_auto_degrade_member(overlay: dict[str, Any]) -> dict[str, Any]:
    """Return the overlay member currently admitted by the latest action."""
    members = overlay.get("members") if isinstance(overlay.get("members"), list) else []
    active_members = [row for row in members if _overlay_member_is_active(row)]
    last_action = overlay.get("last_action") if isinstance(overlay.get("last_action"), dict) else {}
    admitted = str(last_action.get("admitted") or "").strip().lower()
    if admitted:
        for row in active_members:
            if admitted in {
                str(row.get("candidate_id") or "").strip().lower(),
                str(row.get("source_wallet") or "").strip().lower(),
            }:
                return row

    latest = overlay.get("latest_admission") if isinstance(overlay.get("latest_admission"), dict) else {}
    latest_candidate = str(latest.get("candidate_id") or "").strip().lower()
    latest_wallet = str(latest.get("source_wallet") or "").strip().lower()
    if latest_candidate or latest_wallet:
        for row in active_members:
            if latest_candidate and str(row.get("candidate_id") or "").strip().lower() == latest_candidate:
                return row
            if latest_wallet and str(row.get("source_wallet") or "").strip().lower() == latest_wallet:
                return row
        if not members and _overlay_member_is_active(latest):
            return latest

    def sort_key(row: dict[str, Any]) -> str:
        summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
        return str(summary.get("admitted_at") or row.get("admitted_at") or "")

    return max(active_members, key=sort_key, default={})


def _rotation_wallets(rotation: dict[str, Any], *, list_key: str, scalar_key: str) -> list[str]:
    values = rotation.get(list_key)
    if isinstance(values, list):
        return [str(value).strip().lower() for value in values if str(value or "").strip()]
    value = str(rotation.get(scalar_key) or "").strip().lower()
    if value:
        return [value]
    return []


def _refresh_btc5m_structural_scalp_lane(
    root: Path,
    *,
    build_state_func: Any | None = None,
    write_events_func: Any | None = None,
) -> dict[str, Any]:
    """Feed the forward paper gate on the same cadence as digest refreshes."""
    hot_source_path = _rooted_path(root, STRUCTURAL_SCALP_HOT_SOURCE)
    if not hot_source_path.exists():
        return {
            "status": "SKIPPED_SOURCE_MISSING",
            "history": STRUCTURAL_SCALP_HISTORY,
            "hot_source": STRUCTURAL_SCALP_HOT_SOURCE,
            "state": STRUCTURAL_SCALP_STATE,
            "events": STRUCTURAL_SCALP_EVENTS,
        }
    if build_state_func is None or write_events_func is None:
        from scripts.run_btc5m_structural_scalp_paper_lane import (  # noqa: PLC0415
            build_state as runner_build_state,
            write_events as runner_write_events,
        )

        build_state_func = build_state_func or runner_build_state
        write_events_func = write_events_func or runner_write_events
    args = argparse.Namespace(
        history=STRUCTURAL_SCALP_HISTORY,
        hot_source=STRUCTURAL_SCALP_HOT_SOURCE,
        resolutions="data/research/btc_resolutions_from_btcusdt_ticks.jsonl",
        study=STRUCTURAL_SCALP_STUDY,
        state=STRUCTURAL_SCALP_STATE,
        events=STRUCTURAL_SCALP_EVENTS,
        order_usd=1.0,
        tick_size=0.01,
        gate_min_fills=30,
        gate_window_hours=24.0,
        seeded_at="",
        max_current_intents=200,
    )
    try:
        state, event_rows = build_state_func(root, args)
        state = state if isinstance(state, dict) else {}
        event_rows = event_rows if isinstance(event_rows, list) else []
        state_path = _rooted_path(root, STRUCTURAL_SCALP_STATE)
        events_path = _rooted_path(root, STRUCTURAL_SCALP_EVENTS)
        atomic_write_json(state_path, state)
        write_events_func(events_path, event_rows)
    except Exception as exc:  # pragma: no cover - surfaced in digest/runtime output.
        return {
            "status": "ERROR",
            "history": STRUCTURAL_SCALP_HISTORY,
            "state": STRUCTURAL_SCALP_STATE,
            "events": STRUCTURAL_SCALP_EVENTS,
            "error": f"{type(exc).__name__}: {exc}",
        }
    summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    metrics = state.get("metrics") if isinstance(state.get("metrics"), dict) else {}
    forward = metrics.get("forward") if isinstance(metrics.get("forward"), dict) else {}
    live_gate = state.get("live_gate") if isinstance(state.get("live_gate"), dict) else {}
    return {
        "status": "REFRESHED",
        "history": STRUCTURAL_SCALP_HISTORY,
        "state": STRUCTURAL_SCALP_STATE,
        "events": STRUCTURAL_SCALP_EVENTS,
        "generated_at": state.get("generated_at"),
        "forward_fills": forward.get("fills", summary.get("forward_fills")),
        "forward_pnl_usd": forward.get("pnl_usd", summary.get("forward_pnl_usd")),
        "forward_span_days": forward.get("span_days"),
        "current_intents": summary.get("current_intents"),
        "live_gate_status": live_gate.get("status"),
        "ready_for_live": live_gate.get("ready_for_live"),
        "event_rows": len(event_rows),
    }


def _live_can_trade(payload: dict[str, Any]) -> bool:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return bool(summary.get("can_trade"))


def _c539_deferred_open_probe_summary(payload: dict[str, Any]) -> dict[str, Any]:
    """Expose the binding S1 paper clock without promoting its raw rows."""
    from scripts.run_c539_deferred_open_paper_probe import canonicalize_source_rows

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    admission = payload.get("admission") if isinstance(payload.get("admission"), dict) else {}
    frozen = payload.get("frozen_policy") if isinstance(payload.get("frozen_policy"), dict) else {}
    terminal_records = (
        payload.get("predecessor_terminal_records")
        if isinstance(payload.get("predecessor_terminal_records"), list)
        else []
    )
    raw_rows = payload.get("raw_source_rows")
    if not isinstance(raw_rows, list):
        raw_rows = payload.get("source_rows") if isinstance(payload.get("source_rows"), list) else []
    distinct_rows = canonicalize_source_rows(raw_rows)
    not_open_rows = sum(bool(row.get("not_open_yet")) for row in distinct_rows)
    return {
        "generated_at": payload.get("generated_at"),
        "registered_at": payload.get("registered_at"),
        "observation_deadline_at": payload.get("observation_deadline_at"),
        "status": payload.get("status"),
        "paper_only": payload.get("paper_only"),
        "live_orders_allowed": payload.get("live_orders_allowed"),
        "wallet": payload.get("source_wallet"),
        "policy_id": frozen.get("policy_id"),
        "policy_fingerprint": frozen.get("policy_fingerprint"),
        "policy_drift": payload.get("policy_drift"),
        "runtime_policy_diff_expected": payload.get("runtime_policy_diff_expected"),
        "runtime_policy_present": payload.get("runtime_policy_present"),
        "runtime_policy_absence_expected": payload.get(
            "runtime_policy_absence_expected"
        ),
        "raw_rows": len(raw_rows),
        "distinct_signals": len(distinct_rows),
        "cross_feed_duplicate_rows": max(0, len(raw_rows) - len(distinct_rows)),
        "c539_buy_rows": len(distinct_rows),
        "not_open_yet_rows": not_open_rows,
        "not_open_yet_share": (
            round(not_open_rows / len(distinct_rows), 6) if distinct_rows else 0.0
        ),
        "deferred_window_outcomes": summary.get("deferred_window_outcomes"),
        "survived_open_re_evaluation": summary.get("survived_open_re_evaluation"),
        "venue_executable_max_price": summary.get("venue_executable_max_price"),
        "venue_executable_open_fills": summary.get(
            "venue_executable_open_fills"
        ),
        "venue_unreachable_open_fills": summary.get(
            "venue_unreachable_open_fills"
        ),
        "frozen_band_venue_reachable_share_pct": summary.get(
            "frozen_band_venue_reachable_share_pct"
        ),
        "open_fill_venue_reachable_share_pct": summary.get(
            "open_fill_venue_reachable_share_pct"
        ),
        "venue_executable_resolved": summary.get("venue_executable_resolved"),
        "venue_unreachable_resolved": summary.get("venue_unreachable_resolved"),
        "resolved": summary.get("resolved"),
        "forward_evidence_projection": (
            summary.get("forward_evidence_projection")
            if isinstance(summary.get("forward_evidence_projection"), dict)
            else {}
        ),
        "post_fee_pnl_usd": summary.get("post_fee_pnl_usd"),
        "first_half_post_fee_pnl_usd": summary.get("first_half_post_fee_pnl_usd"),
        "second_half_post_fee_pnl_usd": summary.get("second_half_post_fee_pnl_usd"),
        "open_grace_covered_windows": summary.get("open_grace_covered_windows"),
        "open_grace_total_windows": summary.get("open_grace_total_windows"),
        "open_grace_coverage": summary.get("open_grace_coverage"),
        "open_grace_instrumented_covered_windows": summary.get(
            "open_grace_instrumented_covered_windows"
        ),
        "open_grace_instrumented_total_windows": summary.get(
            "open_grace_instrumented_total_windows"
        ),
        "open_grace_instrumented_coverage": summary.get(
            "open_grace_instrumented_coverage"
        ),
        "open_grace_coverage_below_60pct": summary.get(
            "open_grace_coverage_below_60pct"
        ),
        "open_grace_coverage_rows": summary.get("open_grace_coverage_rows") or [],
        "open_grace_coverage_by_hour": summary.get("open_grace_coverage_by_hour") or [],
        "open_grace_coverage_rows": summary.get("open_grace_coverage_rows") or [],
        "latest_predecessor_terminal": terminal_records[-1] if terminal_records else None,
        "preregistration": payload.get("preregistration"),
        "eligible": admission.get("eligible"),
        "live_authority": admission.get("live_authority"),
        "required_bars": (
            admission.get("required_bars")
            if isinstance(admission.get("required_bars"), dict)
            else {}
        ),
    }


def _seat_standdown_readmits_at(slice_name: str, as_of: datetime) -> str | None:
    """Deterministic clock at which a temporal-slice exclusion stops matching."""
    as_of = as_of.astimezone(timezone.utc)
    if slice_name == "dead_band_18_22_utc":
        boundary = as_of.replace(hour=22, minute=0, second=0, microsecond=0)
        if as_of >= boundary:
            boundary += timedelta(days=1)
        return boundary.isoformat().replace("+00:00", "Z")
    if slice_name == "weekend":
        boundary = as_of.replace(hour=0, minute=0, second=0, microsecond=0)
        while boundary <= as_of or boundary.weekday() >= 5:
            boundary += timedelta(days=1)
        return boundary.isoformat().replace("+00:00", "Z")
    return None


def _live_seat_standdown(guard: dict[str, Any]) -> dict[str, Any]:
    """Explain an empty runtime seat that a temporal-slice exclusion caused.

    An empty seat inside an active negative slice is a designed stand-down with
    a known re-admission clock, not an outage; the digest must say so or every
    heartbeat re-escalates it as a live-flow defect.
    """
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    exclusion = (
        runtime.get("temporal_slice_exclusion")
        if isinstance(runtime.get("temporal_slice_exclusion"), dict)
        else {}
    )
    blockers = [str(row) for row in guard.get("blockers") or [] if isinstance(row, (str, int))]
    rows: list[dict[str, Any]] = []
    as_of_raw = str(exclusion.get("as_of") or guard.get("generated_at") or "")
    try:
        as_of = datetime.fromisoformat(as_of_raw.replace("Z", "+00:00"))
    except ValueError:
        as_of = datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    for member in exclusion.get("excluded_members") or []:
        if not isinstance(member, dict):
            continue
        matched = member.get("matched_slice") if isinstance(member.get("matched_slice"), dict) else {}
        slice_name = str(matched.get("slice") or "")
        rows.append(
            {
                "wallet": member.get("source_wallet"),
                "policy_id": member.get("policy_id"),
                "reason": member.get("reason"),
                "slice": slice_name or None,
                "resolved_trades": matched.get("resolved_trades"),
                "roi_pct": matched.get("roi_pct"),
                "pnl_usd": matched.get("pnl_usd"),
                "readmits_at": _seat_standdown_readmits_at(slice_name, as_of),
            }
        )
    seat_empty = int(runtime.get("member_count") or 0) == 0
    # The exclusion only explains an empty seat when it left nothing behind.
    # `included_count` is len(post-filter members) at
    # run_wallet_copy_live_guard.py:13655, so included_count > 0 with an empty
    # seat means survivors failed for some non-slice reason and the stand-down
    # label would suppress a real roster failure until the *longest* remaining
    # readmission clock. Absent (older artefact) stays permissive.
    included_raw = exclusion.get("included_count")
    slice_explains_seat = not (isinstance(included_raw, int) and included_raw > 0)
    if not seat_empty:
        status = "SEAT_HELD"
    elif rows and slice_explains_seat and "no_enabled_active_set_members" in blockers:
        status = "SEAT_STOOD_DOWN_TEMPORAL_SLICE"
    elif seat_empty:
        status = "SEAT_EMPTY_NOT_SLICE_EXPLAINED"
    else:
        status = "NO_STANDDOWN"
    readmits = sorted(str(row["readmits_at"]) for row in rows if row.get("readmits_at"))
    pin = runtime.get("selection_pin") if isinstance(runtime.get("selection_pin"), dict) else {}
    coverage = guard.get("coverage_kpi") if isinstance(guard.get("coverage_kpi"), dict) else {}
    stood_down = status == "SEAT_STOOD_DOWN_TEMPORAL_SLICE"
    return {
        "status": status,
        "derived_during_standdown": {
            # These are constants of the empty-selection branch in
            # run_wallet_copy_live_guard.py, not independent measurements.
            # Quoting any of them as a breach during a stand-down repeats the
            # 2026-08-01T18:19Z false-positive escalation through a new field.
            "suppressed": stood_down,
            "active_set_runtime.enabled": runtime.get("enabled"),
            "active_set_runtime.qualified_member_count": runtime.get("qualified_member_count"),
            "guard.live_orders_allowed": guard.get("live_orders_allowed"),
            "guard.paper_only": guard.get("paper_only"),
            "coverage_kpi.below_active_set_min": coverage.get("below_active_set_min"),
            "coverage_kpi.qualified_member_count": coverage.get("qualified_member_count"),
            "coverage_kpi.armed_idle_hours_since_latest_live_order": coverage.get(
                "armed_idle_hours_since_latest_live_order"
            ),
            "coverage_kpi.target_member_count_min": coverage.get("target_member_count_min"),
            "coverage_actuator_inert": bool(
                not coverage.get("enabled")
                and coverage.get("nonempty_set_armed_since_ts") is None
            ),
            # _GENERIC_NO_CANDIDATE_BLOCKERS (run_wallet_copy_live_guard.py:14072)
            # are appended whenever _load_candidate returns nothing; during a
            # stand-down that is guaranteed, so they are the empty seat restated.
            "blockers.generic_no_candidate": sorted(
                blocker
                for blocker in blockers
                if blocker
                in {
                    "runtime_admission_candidate_missing",
                    "runtime_admission_source_wallet_missing",
                    "all_active_set_members_selection_priority_frozen",
                }
            ),
            "rule": "during a stand-down these fields are outputs of the empty seat, not causes of "
            "it; they clear with the re-admission clock and no backfill, qualification, restart "
            "or admission change is authorized to move them",
        },
        "as_of": as_of.isoformat().replace("+00:00", "Z"),
        "selection_mode": runtime.get("selection_mode"),
        "runtime_member_count": runtime.get("member_count"),
        "active_slices": exclusion.get("active_slices") or [],
        "excluded_count": exclusion.get("excluded_count"),
        "included_count": included_raw,
        "slice_explains_empty_seat": slice_explains_seat,
        "excluded_members": rows,
        "earliest_readmission_at": readmits[0] if readmits else None,
        "pinned_wallet": pin.get("source_wallet"),
        "pin_status": pin.get("pin_status") or pin.get("status"),
        "pin_expires_at": pin.get("expires_at"),
        "blockers": blockers[:5],
        "rule": "an empty runtime seat explained by an active negative temporal slice is a "
        "scheduled stand-down with a deterministic re-admission clock, not a live-flow outage; "
        "no restart or seat mutation restores it",
    }


def _live_read_is_clean(guard: dict[str, Any], live: dict[str, Any]) -> bool:
    return guard.get("status") != "LIVE_GUARD_BLOCKED" and _live_can_trade(live)


def _guard_live_pair_is_blocked(guard: dict[str, Any], live: dict[str, Any]) -> bool:
    return guard.get("status") == "LIVE_GUARD_BLOCKED" and not _live_can_trade(live)


def _recent_live_fills(live: dict[str, Any], *, limit: int = 5) -> list[dict[str, Any]]:
    orders = live.get("orders") if isinstance(live.get("orders"), list) else []
    rows: list[dict[str, Any]] = []
    for order in reversed(orders):
        if not isinstance(order, dict) or str(order.get("final_status") or "").upper() != "FILLED":
            continue
        lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
        fill_payload: dict[str, Any] = {}
        for event in reversed(lifecycle):
            if isinstance(event, dict) and str(event.get("status") or "").upper() == "LIVE_FILLED":
                fill_payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                break
        details = fill_payload.get("details") if isinstance(fill_payload.get("details"), dict) else {}
        fee = order.get("expected_vs_realized_fee")
        fee = fee if isinstance(fee, dict) else {}
        rows.append(
            {
                "intent_id": order.get("intent_id"),
                "market_slug": order.get("market_slug"),
                "outcome": order.get("outcome"),
                "updated_at": order.get("updated_at"),
                "making_amount": details.get("makingAmount") or fill_payload.get("making_amount"),
                "taking_amount": details.get("takingAmount") or fill_payload.get("taking_amount"),
                "response_fill_price": fill_payload.get("response_fill_price"),
                "response_expected_fee_usd": fee.get("response_expected_fee_usd"),
            }
        )
        if len(rows) >= max(0, int(limit)):
            break
    return rows


def _command_arg_float(command: str, flag: str) -> float | None:
    try:
        parts = shlex.split(command)
    except ValueError:
        parts = command.split()
    for index, part in enumerate(parts):
        if part == flag and index + 1 < len(parts):
            return _as_float(parts[index + 1])
        prefix = f"{flag}="
        if part.startswith(prefix):
            return _as_float(part[len(prefix) :])
    return None


def _guard_process_snapshot(root: Path) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,stat=,etime=,command="],
            cwd=str(root),
            text=True,
            capture_output=True,
            timeout=2.0,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"status": "TIMEOUT", "process_count": None, "rows": []}
    except Exception as exc:  # pragma: no cover - diagnostic-only path.
        return {"status": "ERROR", "process_count": None, "error": f"{type(exc).__name__}: {exc}", "rows": []}

    rows: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        if "scripts/run_wallet_copy_live_guard.py" not in line:
            continue
        parts = line.strip().split(None, 4)
        if len(parts) < 5:
            continue
        pid_raw, ppid_raw, stat, etime, command = parts
        try:
            pid = int(pid_raw)
            ppid = int(ppid_raw)
        except ValueError:
            continue
        rows.append(
            {
                "pid": pid,
                "ppid": ppid,
                "stat": stat,
                "etime": etime,
                "max_order_usd": _command_arg_float(command, "--max-order-usd"),
                "drip_max_tranche_usd": _command_arg_float(command, "--drip-max-tranche-usd"),
                "rtds_tail_bytes": _command_arg_float(command, "--rtds-tail-bytes"),
                "rtds_cold_tail_bytes": _command_arg_float(command, "--rtds-cold-tail-bytes"),
                "command": command[:240],
            }
        )
    return {
        "status": "PASS" if proc.returncode == 0 else "PS_ERROR",
        "process_count": len(rows),
        "rows": rows[:5],
    }


def _guard_process_caps(snapshot: dict[str, Any]) -> dict[str, Any]:
    rows = snapshot.get("rows") if isinstance(snapshot.get("rows"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        max_order_usd = _as_float(row.get("max_order_usd"))
        drip_max_tranche_usd = _as_float(row.get("drip_max_tranche_usd"))
        if max_order_usd is not None or drip_max_tranche_usd is not None:
            return {
                "source": "ps",
                "max_order_usd": max_order_usd,
                "drip_max_tranche_usd": drip_max_tranche_usd,
            }
    return {"source": "ps", "max_order_usd": None, "drip_max_tranche_usd": None}


def _subband_bounds(name: str) -> tuple[float | None, float | None] | None:
    if name.startswith("00_below_25") or name.startswith("00_00_25"):
        return (None, 0.25)
    match = re.match(r"^\d+[a-z]?_(\d{2})_(\d{2})$", name)
    if not match:
        return None
    return (int(match.group(1)) / 100.0, int(match.group(2)) / 100.0)


def _ranges_overlap(
    left_min: float | None,
    left_max: float | None,
    right_min: float | None,
    right_max: float | None,
) -> bool:
    lows = [value for value in (left_min, right_min) if value is not None]
    highs = [value for value in (left_max, right_max) if value is not None]
    lo = max(lows) if lows else None
    hi = min(highs) if highs else None
    return lo is None or hi is None or lo < hi


def _reachable_closed_leg_subband(
    *,
    subbands: dict[str, Any],
    measured_band_name: str,
    min_buy_price: float | None,
    max_buy_price: float | None,
) -> dict[str, Any] | None:
    if min_buy_price is None or max_buy_price is None or max_buy_price <= min_buy_price:
        return None
    candidates: list[dict[str, Any]] = []
    for name, row in subbands.items():
        if name == measured_band_name or not isinstance(row, dict):
            continue
        bounds = _subband_bounds(str(name))
        if bounds is None:
            continue
        if not _ranges_overlap(bounds[0], bounds[1], min_buy_price, max_buy_price):
            continue
        aggregate = (
            row.get("chronological_holdout")
            if isinstance(row.get("chronological_holdout"), dict)
            else row.get("aggregate")
        )
        if not isinstance(aggregate, dict):
            continue
        roi = _as_float(aggregate.get("post_fee_roi_pct", aggregate.get("roi_pct")))
        if roi is None:
            continue
        candidates.append(
            {
                "subband": name,
                "post_fee_roi_pct": roi,
                "source": (
                    "data/research/taker_price_subband_holdout_latest.json"
                    f"#subbands.{name}.chronological_holdout.post_fee_roi_pct"
                ),
            }
        )
    return min(candidates, key=lambda row: row["post_fee_roi_pct"]) if candidates else None


def _observed_closed_leg_subband(
    *,
    subbands: dict[str, Any],
    measured_band_name: str,
    realized_entry_events: list[Any],
) -> dict[str, Any] | None:
    observed_counts = Counter(
        str(row.get("realized_entry_band") or "")
        for row in realized_entry_events
        if isinstance(row, dict)
        and row.get("out_of_band_fill") is True
        and str(row.get("realized_entry_band") or "")
    )
    candidates: list[dict[str, Any]] = []
    for observed_name, count in observed_counts.items():
        aliases = [observed_name]
        if observed_name == "00_00_25":
            aliases.append("00_below_25")
        elif observed_name == "00_below_25":
            aliases.append("00_00_25")
        for name in aliases:
            row = subbands.get(name)
            if name == measured_band_name or not isinstance(row, dict):
                continue
            evidence = (
                row.get("chronological_holdout")
                if isinstance(row.get("chronological_holdout"), dict)
                else row.get("aggregate")
            )
            if not isinstance(evidence, dict):
                continue
            roi = _as_float(evidence.get("post_fee_roi_pct", evidence.get("roi_pct")))
            if roi is None:
                continue
            candidates.append(
                {
                    "subband": name,
                    "observed_realized_entry_band": observed_name,
                    "observed_fill_count": count,
                    "post_fee_roi_pct": roi,
                    "sample_gate_status": (
                        row.get("sample_gate", {}).get("status")
                        if isinstance(row.get("sample_gate"), dict)
                        else None
                    ),
                    "source": (
                        "data/research/taker_price_subband_holdout_latest.json"
                        f"#subbands.{name}.chronological_holdout.post_fee_roi_pct"
                    ),
                }
            )
            break
    return min(candidates, key=lambda row: row["post_fee_roi_pct"]) if candidates else None


def _goal_reachability(
    *,
    guard: dict[str, Any],
    guard_caps: dict[str, Any],
    measured_band: dict[str, Any],
    measured_band_name: str = "01a_25_32",
    measured_band_source: str = "taker_price_subband_holdout.subbands.01a_25_32.chronological_holdout",
    day_actual_pnl_usd: Any,
    source_side_supply: dict[str, Any] | None = None,
    roi_evidence: dict[str, Any] | None = None,
    active_set_registry: dict[str, Any] | None = None,
    realized_live_participation_windows: Any = None,
    resolved_live_fills_at_current_h: Any = None,
    since_topup_actual_usd: Any = None,
    restart_acceptance: dict[str, Any] | None = None,
    in_band_fill_rate: Any = None,
    in_band_fill_rate_source: str = "canonical_pnl_truth.by_day[current].in_band_fill_rate",
    closed_leg_roi_pct: Any = None,
    closed_leg_roi_source: str = "taker_price_subband_holdout.subbands.00_below_25.aggregate.post_fee_roi_pct",
    min_buy_price: Any = None,
    max_buy_price: Any = None,
    closed_leg_reachable_under_enforced_gate: bool | None = None,
    realized_entry_events: list[Any] | None = None,
    floor_gate_enforced_fill_count: Any = None,
    floor_gate_observation_started_at: Any = None,
    eligible_intent_count: Any = None,
    floor_gate_target_fills: int = 30,
    now: datetime | None = None,
    goal_floor_usd: float = 100.0,
) -> dict[str, Any]:
    """Measure the best remaining-day outcome without changing live sizing."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)

    enforced_fill_count = _as_int(floor_gate_enforced_fill_count, default=0)
    eligible_count = (
        _as_int(eligible_intent_count, default=0)
        if eligible_intent_count is not None
        else None
    )
    observation_started_at = _parse_utc_ts(floor_gate_observation_started_at)
    elapsed_s = (
        max(0.0, (now - observation_started_at).total_seconds())
        if observation_started_at is not None
        else None
    )
    fill_rate_per_s = (
        enforced_fill_count / elapsed_s
        if elapsed_s is not None and elapsed_s > 0 and enforced_fill_count > 0
        else None
    )
    projected_maturity_at = (
        now
        + timedelta(
            seconds=max(0, floor_gate_target_fills - enforced_fill_count)
            / fill_rate_per_s
        )
        if fill_rate_per_s is not None and enforced_fill_count < floor_gate_target_fills
        else now
        if enforced_fill_count >= floor_gate_target_fills
        else None
    )
    if enforced_fill_count >= floor_gate_target_fills:
        floor_gate_attribution_status = "READY_FOR_TWO_CLASS_ATTRIBUTION"
    elif eligible_count == 0:
        floor_gate_attribution_status = "UNDEFINED_ZERO_ELIGIBLE"
        projected_maturity_at = None
    elif eligible_count is None:
        floor_gate_attribution_status = "PENDING_ELIGIBILITY_UNKNOWN"
    elif fill_rate_per_s is None:
        floor_gate_attribution_status = "PENDING_RATE_NOT_YET_OBSERVED"
    else:
        floor_gate_attribution_status = "ACCRUING_WITH_ETA"
    floor_gate_attribution = {
        "status": floor_gate_attribution_status,
        "target_enforced_in_band_fills": floor_gate_target_fills,
        "observed_enforced_in_band_fills": enforced_fill_count,
        "remaining_fills": max(0, floor_gate_target_fills - enforced_fill_count),
        "eligible_intent_count": eligible_count,
        "observation_started_at": (
            observation_started_at.isoformat().replace("+00:00", "Z")
            if observation_started_at is not None
            else None
        ),
        "projected_maturity_at": (
            projected_maturity_at.isoformat().replace("+00:00", "Z")
            if projected_maturity_at is not None
            else None
        ),
        "projection_basis": {
            "method": "observed_enforced_fill_count_divided_by_elapsed_wall_time",
            "elapsed_s": round(elapsed_s, 6) if elapsed_s is not None else None,
            "observed_fill_rate_per_hour": (
                round(fill_rate_per_s * 3600.0, 9)
                if fill_rate_per_s is not None
                else None
            ),
            "zero_rate_rule": (
                "eligible_intent_count == 0 makes maturity undefined; never label PENDING"
            ),
        },
        "measurement_only": True,
        "live_mutation": False,
    }

    measured_roi_pct = _as_float(
        measured_band.get("post_fee_roi_pct", measured_band.get("roi_pct"))
    )
    day_actual = _as_float(day_actual_pnl_usd)
    guard_flag_cap = _as_float(guard_caps.get("max_order_usd"))
    active_set = guard.get("active_set") if isinstance(guard.get("active_set"), dict) else {}
    active_members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    member_caps: list[dict[str, Any]] = []
    for member in active_members:
        if not isinstance(member, dict):
            continue
        policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
        cap = _as_float(member.get("max_order_usd"))
        cap_source = "member.max_order_usd"
        if cap is None:
            cap = _as_float(policy.get("max_order_usd"))
            cap_source = "member.policy.max_order_usd"
        if cap is not None and cap > 0:
            member_caps.append(
                {
                    "source_wallet": member.get("source_wallet") or member.get("wallet"),
                    "policy_id": member.get("policy_id") or policy.get("policy_id"),
                    "max_order_usd": cap,
                    "source": cap_source,
                }
            )

    active_runtime = (
        guard.get("active_set_runtime")
        if isinstance(guard.get("active_set_runtime"), dict)
        else {}
    )
    runtime_submittability = (
        active_runtime.get("runtime_member_submittability")
        if isinstance(active_runtime.get("runtime_member_submittability"), dict)
        else guard.get("runtime_member_submittability")
        if isinstance(guard.get("runtime_member_submittability"), dict)
        else {}
    )
    selected_runtime_cap = _as_float(runtime_submittability.get("effective_max_order_usd"))
    positive_caps = [row["max_order_usd"] for row in member_caps]
    if selected_runtime_cap is not None and selected_runtime_cap > 0:
        positive_caps.append(selected_runtime_cap)
    if guard_flag_cap is not None and guard_flag_cap > 0:
        positive_caps.append(guard_flag_cap)
    effective_cap = min(positive_caps) if positive_caps else None
    runtime_filter = (
        guard.get("guard_runtime_filter")
        if isinstance(guard.get("guard_runtime_filter"), dict)
        else {}
    )
    min_buy_price = _as_float(runtime_filter.get("price_band_decision_min_price"))
    max_buy_price = _as_float(runtime_filter.get("price_band_decision_max_price"))
    entry_gate = (
        guard.get("live_execution", {})
        if isinstance(guard.get("live_execution"), dict)
        else {}
    )
    candidate_summary = (
        entry_gate.get("candidate_intent_summary")
        if isinstance(entry_gate.get("candidate_intent_summary"), dict)
        else {}
    )
    hard_floor = (
        candidate_summary.get("live_hard_entry_floor")
        if isinstance(candidate_summary.get("live_hard_entry_floor"), dict)
        else {}
    )
    hard_cap = (
        candidate_summary.get("live_hard_entry_cap")
        if isinstance(candidate_summary.get("live_hard_entry_cap"), dict)
        else {}
    )
    if min_buy_price is None:
        min_buy_price = _as_float(hard_floor.get("min_buy_price"))
    if max_buy_price is None:
        max_buy_price = _as_float(hard_cap.get("max_buy_price"))
    per_window_fill_cap = max(0, _as_int(runtime_filter.get("per_window_fill_cap"), default=1))
    seconds_since_midnight = now.hour * 3600 + now.minute * 60 + now.second
    completed_windows = min(288, seconds_since_midnight // 300)
    windows_remaining = max(0, 288 - completed_windows)

    inputs_complete = bool(
        measured_roi_pct is not None
        and measured_roi_pct > 0
        and day_actual is not None
        and effective_cap is not None
        and effective_cap > 0
    )
    remaining_profit_ceiling = (
        windows_remaining
        * per_window_fill_cap
        * effective_cap
        * measured_roi_pct
        / 100.0
        if inputs_complete
        else 0.0
    )
    max_achievable_day = (day_actual + remaining_profit_ceiling) if day_actual is not None else None
    status = (
        "PASS"
        if inputs_complete and max_achievable_day is not None and max_achievable_day >= goal_floor_usd
        else "UNREACHABLE_AT_CURRENT_CAPS"
    )
    required_daily_notional = (
        goal_floor_usd / (measured_roi_pct / 100.0)
        if measured_roi_pct is not None and measured_roi_pct > 0
        else None
    )
    first_at = _parse_utc_ts(measured_band.get("first_submitted_at"))
    last_at = _parse_utc_ts(measured_band.get("last_submitted_at"))
    measured_rows = _as_int(
        measured_band.get("rows", measured_band.get("resolved_fills")), default=0
    )
    calendar_span_days = (
        max(1, (last_at.date() - first_at.date()).days + 1)
        if first_at is not None and last_at is not None
        else None
    )
    qualifying_windows_per_day = (
        measured_rows / calendar_span_days
        if calendar_span_days and measured_rows > 0
        else None
    )
    required_qualifying_windows_per_day = (
        required_daily_notional / effective_cap
        if required_daily_notional is not None and effective_cap is not None and effective_cap > 0
        else None
    )
    gated_fill_supply_profit_usd = (
        qualifying_windows_per_day * effective_cap * measured_roi_pct / 100.0
        if qualifying_windows_per_day is not None and inputs_complete
        else None
    )
    source_prospective = (
        source_side_supply.get("prospective_current_market")
        if isinstance(source_side_supply, dict)
        and isinstance(source_side_supply.get("prospective_current_market"), dict)
        else {}
    )
    source_gate = (
        source_prospective.get("actuator_consumption_gate")
        if isinstance(source_prospective.get("actuator_consumption_gate"), dict)
        else {}
    )
    source_holdouts = (
        source_gate.get("exact_policy_chronological_holdout_by_wallet")
        if isinstance(source_gate.get("exact_policy_chronological_holdout_by_wallet"), dict)
        else {}
    )
    source_green_wallets = {
        str(wallet).strip().lower()
        for wallet, fingerprint_rows in source_holdouts.items()
        if isinstance(fingerprint_rows, dict)
        and any(
            isinstance(row, dict)
            and row.get("passed") is True
            and str(row.get("wide_policy_fingerprint") or "") == str(fingerprint)
            for fingerprint, row in fingerprint_rows.items()
        )
    }
    source_rows = [
        row
        for row in source_prospective.get("identity_clean_events") or []
        if isinstance(row, dict)
        and row.get("paper_only") is True
        and str(row.get("action") or "").upper() == "BUY"
        and str(row.get("source_wallet") or "").strip().lower()
        in source_green_wallets
        and str(row.get("market_slug") or row.get("market") or "")
    ]
    source_market_windows = {
        str(row.get("market_slug") or row.get("market") or "") for row in source_rows
    }
    source_01a_rows = [
        row
        for row in source_rows
        if (price := _as_float(row.get("price") or row.get("source_price"))) is not None
        and 0.25 <= price < 0.32
    ]
    source_01a_windows = {
        str(row.get("market_slug") or row.get("market") or "")
        for row in source_01a_rows
    }
    source_push_01a_windows = {
        str(row.get("market_slug") or row.get("market") or "")
        for row in source_01a_rows
        if str(row.get("source") or "").lower() == "polygon_ws"
    }
    source_qualifying_windows_per_day = (
        288.0 * len(source_01a_windows) / len(source_market_windows)
        if source_market_windows
        else None
    )
    registry_rows = (
        active_set_registry.get("members")
        if isinstance(active_set_registry, dict)
        and isinstance(active_set_registry.get("members"), list)
        else []
    )
    registry_by_wallet = {
        str(row.get("source_wallet") or row.get("wallet") or "").strip().lower(): row
        for row in registry_rows
        if isinstance(row, dict)
        and str(row.get("source_wallet") or row.get("wallet") or "").strip()
    }
    active_live_wallets = {
        str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
        for row in active_members
        if isinstance(row, dict)
        and str(row.get("source_wallet") or row.get("wallet") or "").strip()
    }
    traded_cohort_rows = [
        row
        for row in source_prospective.get("identity_clean_events") or []
        if isinstance(row, dict)
        and row.get("paper_only") is True
        and str(row.get("action") or "").upper() == "BUY"
        and str(row.get("source_wallet") or "").strip().lower() in active_live_wallets
        and str(row.get("market_slug") or row.get("market") or "")
    ]
    traded_cohort_market_windows = {
        str(row.get("market_slug") or row.get("market") or "")
        for row in traded_cohort_rows
    }
    traded_cohort_01a_windows = {
        str(row.get("market_slug") or row.get("market") or "")
        for row in traded_cohort_rows
        if (price := _as_float(row.get("price") or row.get("source_price"))) is not None
        and 0.25 <= price < 0.32
    }
    traded_cohort_windows_per_day = (
        288.0 * len(traded_cohort_01a_windows) / len(traded_cohort_market_windows)
        if traded_cohort_market_windows
        else None
    )
    source_cohort_members = [
        {
            "source_wallet": wallet,
            "enabled": bool(registry_by_wallet.get(wallet, {}).get("enabled")),
            "status": registry_by_wallet.get(wallet, {}).get("status"),
        }
        for wallet in sorted(source_green_wallets)
    ]
    live_overlap_wallets = sorted(
        wallet
        for wallet in source_green_wallets & active_live_wallets
        if registry_by_wallet.get(wallet, {}).get("enabled") is True
    )
    live_members_with_no_supply_measurement = sorted(active_live_wallets - source_green_wallets)
    supply_cohort_live_overlap = len(live_overlap_wallets) if registry_rows else None
    ladder_observed_windows_per_day = (
        traded_cohort_windows_per_day
        if active_live_wallets
        else source_qualifying_windows_per_day
    )
    supply_denominator_status = (
        "TRADED_COHORT_MEASURED"
        if traded_cohort_windows_per_day is not None
        else "NO_MEASUREMENT_FOR_TRADED_COHORT"
        if active_live_wallets
        else "SOURCE_COHORT_MEASURED"
        if source_qualifying_windows_per_day is not None
        else "COHORT_IDENTITY_UNAVAILABLE"
    )
    cap_provenance_members: list[dict[str, Any]] = []
    for row in registry_rows:
        wallet = str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
        evidence_link = (
            row.get("cap_reason")
            or row.get("cap_evidence")
            or row.get("max_order_usd_evidence")
        )
        admission_wave_id = row.get("admission_wave_id")
        cap_basis = (
            "evidence_linked"
            if evidence_link
            else "hand_set_admission_wave"
            if admission_wave_id
            else "unattributed"
        )
        cap_provenance_members.append(
            {
                "source_wallet": wallet,
                "enabled": bool(row.get("enabled")),
                "status": row.get("status"),
                "max_order_usd": _as_float(row.get("max_order_usd")),
                "fable_cap_max_order_usd": _as_float(row.get("fable_cap_max_order_usd")),
                "cap_reason": row.get("cap_reason"),
                "admission_wave_id": admission_wave_id,
                "cap_basis": cap_basis,
            }
        )
    cap_basis_counts = Counter(row["cap_basis"] for row in cap_provenance_members)
    roi_evidence = roi_evidence or {}
    observed_h = _as_float(in_band_fill_rate)
    observed_h = observed_h if observed_h is not None and 0.0 <= observed_h <= 1.0 else None
    closed_roi = _as_float(closed_leg_roi_pct)
    subbands = roi_evidence.get("subbands") if isinstance(roi_evidence.get("subbands"), dict) else {}
    realized_entry_events = realized_entry_events or []
    reachable_closed_leg = _observed_closed_leg_subband(
        subbands=subbands,
        measured_band_name=measured_band_name,
        realized_entry_events=realized_entry_events,
    )
    closed_leg_reachable_under_enforced_gate = None
    if reachable_closed_leg:
        closed_roi = reachable_closed_leg["post_fee_roi_pct"]
        closed_leg_roi_source = reachable_closed_leg["source"]
        closed_leg_reachable_under_enforced_gate = True
    elif observed_h is not None and observed_h < 1.0:
        closed_roi = None
        closed_leg_roi_source = "NO_OBSERVED_CLOSED_LEG_ROI_FOR_NONZERO_OOB"
        closed_leg_reachable_under_enforced_gate = None
    elif subbands and min_buy_price is not None and max_buy_price is not None:
        closed_roi = 0.0
        closed_leg_roi_source = "NO_REACHABLE_CLOSED_LEG_UNDER_ENFORCED_GATE"
        closed_leg_reachable_under_enforced_gate = False
    measured_h = observed_h
    robustness = (
        roi_evidence.get("focus_robustness")
        if isinstance(roi_evidence.get("focus_robustness"), dict)
        else {}
    )
    drop_curve = (
        robustness.get("holdout_drop_k_curve")
        if isinstance(robustness.get("holdout_drop_k_curve"), list)
        else []
    )
    lodo = (
        robustness.get("holdout_leave_one_day_out")
        if isinstance(robustness.get("holdout_leave_one_day_out"), dict)
        else {}
    )
    lodo_curve = lodo.get("curve") if isinstance(lodo.get("curve"), list) else []
    development = (
        roi_evidence.get("development")
        if isinstance(roi_evidence.get("development"), dict)
        else {}
    )
    roi_rows = [
        (
            "holdout_headline",
            measured_roi_pct,
            f"{measured_band_source}.post_fee_roi_pct",
        ),
        (
            "development",
            _as_float(development.get("post_fee_roi_pct")),
            "data/research/taker_price_subband_holdout_latest.json#subbands.01a_25_32.development.post_fee_roi_pct",
        ),
        (
            "holdout_drop_1",
            next(
                (
                    _as_float(row.get("post_fee_roi_pct"))
                    for row in drop_curve
                    if isinstance(row, dict) and row.get("drop_k") == 1
                ),
                None,
            ),
            "data/research/taker_price_subband_holdout_latest.json#focus_robustness.holdout_drop_k_curve[drop_k=1].post_fee_roi_pct",
        ),
        (
            "holdout_drop_2",
            next(
                (
                    _as_float(row.get("post_fee_roi_pct"))
                    for row in drop_curve
                    if isinstance(row, dict) and row.get("drop_k") == 2
                ),
                None,
            ),
            "data/research/taker_price_subband_holdout_latest.json#focus_robustness.holdout_drop_k_curve[drop_k=2].post_fee_roi_pct",
        ),
        (
            "lodo_worst_day",
            min(
                (
                    value
                    for row in lodo_curve
                    if isinstance(row, dict)
                    and (value := _as_float(row.get("post_fee_roi_pct"))) is not None
                ),
                default=None,
            ),
            "data/research/taker_price_subband_holdout_latest.json#focus_robustness.holdout_leave_one_day_out.curve[min(post_fee_roi_pct)]",
        ),
    ]
    supply_bases = [
        (
            "observed_source_side",
            ladder_observed_windows_per_day,
            len(traded_cohort_market_windows) if active_live_wallets else len(source_market_windows),
        ),
        ("perfect_288", 288.0, None),
    ]
    cap_rungs: list[dict[str, Any]] = []
    for roi_basis, roi_pct, roi_source in roi_rows:
        break_even_h = (
            -closed_roi / (roi_pct - closed_roi)
            if roi_pct is not None
            and roi_pct > 0
            and closed_roi is not None
            and closed_roi < 0
            and roi_pct != closed_roi
            else None
        )
        blended_roi_pct = (
            measured_h * roi_pct + (1.0 - measured_h) * closed_roi
            if measured_h is not None and roi_pct is not None and closed_roi is not None
            else None
        )
        for supply_basis, windows_per_day, observed_sample in supply_bases:
            usd_per_cap = (
                windows_per_day * per_window_fill_cap * blended_roi_pct / 100.0
                if windows_per_day is not None
                and blended_roi_pct is not None
                and blended_roi_pct > 0
                else None
            )
            required_cap = (
                goal_floor_usd / usd_per_cap
                if usd_per_cap is not None and usd_per_cap > 0
                else None
            )
            cap_rungs.append(
                {
                    "roi_basis": roi_basis,
                    "roi_pct": roi_pct,
                    "roi_source": roi_source,
                    "governing_selection_basis": "raw_roi_pct_pre_leak",
                    "in_band_fill_rate_h": measured_h,
                    "observed_in_band_fill_rate_h": observed_h,
                    "hypothetical_h": 1.0,
                    "in_band_fill_rate_source": in_band_fill_rate_source,
                    "in_band_roi_contribution_pct": (
                        round(measured_h * roi_pct, 6)
                        if measured_h is not None and roi_pct is not None
                        else None
                    ),
                    "out_of_band_fill_rate": (
                        round(1.0 - measured_h, 9) if measured_h is not None else None
                    ),
                    "closed_leg_roi_pct": closed_roi,
                    "closed_leg_roi_source": closed_leg_roi_source,
                    "closed_leg_reachable_under_enforced_gate": (
                        closed_leg_reachable_under_enforced_gate
                    ),
                    "observed_closed_leg_subband": (
                        reachable_closed_leg.get("observed_realized_entry_band")
                        if reachable_closed_leg
                        else None
                    ),
                    "observed_closed_leg_fill_count": (
                        reachable_closed_leg.get("observed_fill_count")
                        if reachable_closed_leg
                        else 0
                    ),
                    "closed_leg_sample_gate_status": (
                        reachable_closed_leg.get("sample_gate_status")
                        if reachable_closed_leg
                        else None
                    ),
                    "min_buy_price": min_buy_price,
                    "max_buy_price": max_buy_price,
                    "closed_leg_roi_contribution_pct": (
                        round((1.0 - measured_h) * closed_roi, 6)
                        if measured_h is not None and closed_roi is not None
                        else None
                    ),
                    "blended_roi_pct": (
                        round(blended_roi_pct, 6)
                        if blended_roi_pct is not None
                        else None
                    ),
                    "hypothetical_h_one_blended_roi_pct": (
                        round(roi_pct, 6) if roi_pct is not None else None
                    ),
                    "break_even_h": round(break_even_h, 9) if break_even_h is not None else None,
                    "supply_basis": supply_basis,
                    "windows_per_day": (
                        round(windows_per_day, 6)
                        if windows_per_day is not None
                        else "NO_MEASUREMENT_FOR_TRADED_COHORT"
                        if supply_basis == "observed_source_side" and active_live_wallets
                        else None
                    ),
                    "extrapolated_from_observed_market_windows": (
                        observed_sample
                        if supply_basis == "observed_source_side"
                        else None
                    ),
                    "usd_per_day_per_dollar_cap": (
                        round(usd_per_cap, 6) if usd_per_cap is not None else None
                    ),
                    "required_cap_usd": (
                        round(required_cap, 6) if required_cap is not None else None
                    ),
                    "admissible_under_guard_flag": bool(
                        required_cap is not None
                        and guard_flag_cap is not None
                        and required_cap <= guard_flag_cap
                    ),
                    "rung_verdict": (
                        "UNREACHABLE_AT_ANY_CAP"
                        if measured_h is None
                        or blended_roi_pct is None
                        or blended_roi_pct <= 0
                        else "CAP_REQUIRED"
                    ),
                }
            )
    positive_roi_rows = [row for row in roi_rows if row[1] is not None and row[1] > 0]
    governing_roi = min(positive_roi_rows, key=lambda row: row[1]) if positive_roi_rows else None
    governing_perfect = next(
        (
            row
            for row in cap_rungs
            if governing_roi is not None
            and row["roi_basis"] == governing_roi[0]
            and row["supply_basis"] == "perfect_288"
        ),
        None,
    )
    governing_required = (
        governing_perfect.get("required_cap_usd")
        if isinstance(governing_perfect, dict)
        else None
    )
    governing_blended_roi = (
        governing_perfect.get("blended_roi_pct")
        if isinstance(governing_perfect, dict)
        else None
    )
    cap_to_goal = {
        "goal_floor_usd": round(goal_floor_usd, 6),
        "per_window_fill_cap": per_window_fill_cap,
        "guard_flag_max_order_usd": guard_flag_cap,
        "rungs": cap_rungs,
        "governing_selection_basis": "raw_roi_pct_pre_leak",
        "governing_rung": (
            {
                "roi_basis": governing_roi[0],
                "roi_pct": governing_roi[1],
                "roi_source": governing_roi[2],
                "supply_basis": "perfect_288",
                "required_cap_usd": governing_required,
                "in_band_fill_rate_h": measured_h,
                "blended_roi_pct": governing_blended_roi,
                "governing_selection_basis": "raw_roi_pct_pre_leak",
            }
            if governing_roi is not None
            else None
        ),
        "governing_verdict": (
            "UNREACHABLE_AT_ANY_CAP"
            if measured_h is None
            or governing_blended_roi is None
            or governing_blended_roi <= 0
            else "UNREACHABLE_AT_ADMISSIBLE_CAP"
            if governing_required is not None
            and guard_flag_cap is not None
            and governing_required > guard_flag_cap
            else "REACHABLE_AT_ADMISSIBLE_CAP"
            if governing_required is not None and guard_flag_cap is not None
            else "INCOMPLETE_INPUTS"
        ),
        "fill_cap_sensitivity": [
            {
                "per_window_fill_cap": fill_cap,
                "required_cap_usd_at_governing_rung": (
                    round(
                        goal_floor_usd
                        / (288.0 * fill_cap * governing_blended_roi / 100.0),
                        6,
                    )
                    if governing_roi is not None
                    and governing_blended_roi is not None
                    and governing_blended_roi > 0
                    else None
                ),
            }
            for fill_cap in (1, 2)
        ],
        "fill_cap_sensitivity_exercised": False,
    }
    restart_acceptance = restart_acceptance or {}
    restart_all_pass = all(
        restart_acceptance.get(key) is True
        for key in (
            "blocker_taxonomy_published",
            "post_restart_in_band_fill_rate_is_one",
            "first_hour_submitted_nonincrease",
        )
    )
    resolved_at_h = _as_int(resolved_live_fills_at_current_h, default=0)
    since_topup_actual = _as_float(since_topup_actual_usd)
    cap_step_criteria = {
        "d14_restart_acceptance_all_pass": restart_all_pass,
        "resolved_live_fills_at_h_one_gte_30": bool(
            measured_h == 1.0 and resolved_at_h >= 30
        ),
        "lodo_worst_is_governing_positive_rung": bool(
            governing_roi is not None and governing_roi[0] == "lodo_worst_day"
        ),
        "since_topup_actual_not_below_preregistered_baseline": bool(
            since_topup_actual is not None and since_topup_actual >= -30.392442
        ),
    }
    cap_step_preregistration = {
        "status": "NO_STEP_CRITERIA_INCOMPLETE",
        "live_mutation": False,
        "selected_member_cap_from_usd": 1.0,
        "selected_member_cap_to_usd": 2.0,
        "precedent_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
        "since_topup_actual_baseline_usd": -30.392442,
        "criteria": cap_step_criteria,
        "criteria_passed": sum(cap_step_criteria.values()),
        "criteria_required": 4,
        "all_four_required": True,
        "cap_raise_authorized": False,
        "rule": "four-of-four is necessary for a future Fable ruling; this preregistration never mutates live caps",
    }
    source_supply_profit_usd = (
        ladder_observed_windows_per_day
        * effective_cap
        * measured_roi_pct
        / 100.0
        if ladder_observed_windows_per_day is not None and inputs_complete
        else None
    )
    authoritative_supply_profit_usd = (
        source_supply_profit_usd
        if ladder_observed_windows_per_day is not None
        else gated_fill_supply_profit_usd
    )
    perfect_supply_profit_usd = (
        288 * per_window_fill_cap * effective_cap * measured_roi_pct / 100.0
        if inputs_complete
        else None
    )
    goal_floor_reachable_today = bool(
        perfect_supply_profit_usd is not None
        and perfect_supply_profit_usd >= goal_floor_usd
    )
    if not inputs_complete:
        status = "INCOMPLETE_INPUTS"
    elif perfect_supply_profit_usd is not None and perfect_supply_profit_usd < goal_floor_usd:
        status = "CAP_BOUND"
    elif (
        authoritative_supply_profit_usd is not None
        and authoritative_supply_profit_usd < goal_floor_usd
    ):
        status = "SUPPLY_BOUND"
    else:
        status = "PASS"
    return {
        "status": status,
        "measurement_only": True,
        "live_mutation": False,
        "goal_floor_usd": round(goal_floor_usd, 6),
        "goal_floor_reachable_today": goal_floor_reachable_today,
        "goal_floor_reachability_basis": {
            "metric": "perfect_288_profit_ceiling_usd_per_day",
            "profit_ceiling_usd_per_day": (
                round(perfect_supply_profit_usd, 6)
                if perfect_supply_profit_usd is not None
                else None
            ),
            "rule": "reachable only when the perfect-288 post-fee profit ceiling meets the daily goal floor",
        },
        "measured_band": measured_band_name,
        "measured_band_roi_pct": measured_roi_pct,
        "measured_band_resolved_fills": measured_rows,
        "measured_band_source": measured_band_source,
        "guard_flag_max_order_usd": guard_flag_cap,
        "active_member_policy_caps": member_caps,
        "effective_max_order_usd": effective_cap,
        "selected_runtime_cap_usd": selected_runtime_cap,
        "effective_cap_rule": (
            "MIN(guard process --max-order-usd, selected runtime effective max_order_usd, active member policy max_order_usd)"
        ),
        "cap_to_goal": cap_to_goal,
        "live_price_gate": {
            "min_buy_price": min_buy_price,
            "max_buy_price": max_buy_price,
            "binds": "decision_price_and_gate_probe_best_ask",
            "gate_probe_best_ask_floor_predicate": "below_ruled_entry_floor",
            "realized_entry_bound": False,
            "realized_entry_binding_status": (
                floor_gate_attribution_status
                if eligible_intent_count is not None
                else "PENDING_POST_ACTIVATION_FILL_EVIDENCE"
            ),
            "first_30_enforced_fill_attribution": floor_gate_attribution,
            "closed_leg_reachable_under_enforced_gate": (
                closed_leg_reachable_under_enforced_gate
            ),
            "closed_leg_roi_pct": closed_roi,
            "closed_leg_roi_source": closed_leg_roi_source,
            "rule": (
                "closed leg ROI comes from observed realized out-of-band fills; "
                "the live gate binds decision price plus gate-probe best ask and "
                "rejects probe evidence older than 1.0 seconds at submit handoff "
                "(INVENTORY_BEST_ASK_MAX_AGE_AT_GATE_S), while "
                "realized-fill compliance is graded separately after activation"
            ),
        },
        "cap_provenance": {
            "member_count": len(cap_provenance_members),
            "cap_basis_counts": dict(sorted(cap_basis_counts.items())),
            "members": cap_provenance_members,
        },
        "cap_step_preregistration": cap_step_preregistration,
        "per_window_fill_cap": per_window_fill_cap,
        "windows_remaining": windows_remaining,
        "day_actual_pnl_usd": day_actual,
        "remaining_profit_ceiling_usd": round(remaining_profit_ceiling, 6),
        "max_achievable_day_usd": round(max_achievable_day, 6) if max_achievable_day is not None else None,
        "required_daily_notional_usd": (
            round(required_daily_notional, 6) if required_daily_notional is not None else None
        ),
        "supply": {
            "supply_cohort_live_overlap": supply_cohort_live_overlap,
            "supply_cohort_live_overlap_wallets": live_overlap_wallets,
            "live_members_with_no_supply_measurement": live_members_with_no_supply_measurement,
            "denominator_status": supply_denominator_status,
            "authority": (
                "traded_cohort_source_side_distinct_market_windows"
                if traded_cohort_windows_per_day is not None
                else "NO_MEASUREMENT_FOR_TRADED_COHORT"
                if active_live_wallets
                else "qualified_pool_source_side_distinct_market_windows"
                if source_qualifying_windows_per_day is not None
                else "COHORT_IDENTITY_UNAVAILABLE"
            ),
            "counting_basis": (
                "active live wallet identity-clean paper-only 01a BUY windows / all active live wallet observed source windows"
                if traded_cohort_windows_per_day is not None
                else "no ladder denominator; active live wallet cohort has no source-side measurement"
                if active_live_wallets
                else "holdout-green qualified-pool source windows carrying >=1 identity-clean paper-only 01a BUY / all observed source windows"
            ),
            "observed_rows": (
                len({str(row.get("event_id") or "") for row in source_01a_rows})
                if source_qualifying_windows_per_day is not None
                else measured_rows
            ),
            "calendar_span_days": (
                None
                if source_qualifying_windows_per_day is not None
                else calendar_span_days
            ),
            "qualifying_windows_per_day": (
                "NO_MEASUREMENT_FOR_TRADED_COHORT"
                if active_live_wallets and traded_cohort_windows_per_day is None
                else round(traded_cohort_windows_per_day, 6)
                if traded_cohort_windows_per_day is not None
                else
                round(source_qualifying_windows_per_day, 6)
                if source_qualifying_windows_per_day is not None
                else round(qualifying_windows_per_day, 6)
                if qualifying_windows_per_day is not None
                else None
            ),
            "measured_non_traded_cohort_qualifying_windows_per_day": (
                round(source_qualifying_windows_per_day, 6)
                if supply_cohort_live_overlap == 0
                and source_qualifying_windows_per_day is not None
                else None
            ),
            "qualifying_windows_per_day_for_ladder": (
                "NO_MEASUREMENT_FOR_TRADED_COHORT"
                if active_live_wallets and ladder_observed_windows_per_day is None
                else
                round(ladder_observed_windows_per_day, 6)
                if ladder_observed_windows_per_day is not None
                else None
            ),
            "required_qualifying_windows_per_day": (
                round(required_qualifying_windows_per_day, 6)
                if required_qualifying_windows_per_day is not None
                else None
            ),
            "observed_supply_profit_ceiling_usd_per_day": (
                round(authoritative_supply_profit_usd, 6)
                if authoritative_supply_profit_usd is not None
                else None
            ),
            "source_side": {
                "artifact_generated_at": (
                    source_side_supply.get("generated_at")
                    if isinstance(source_side_supply, dict)
                    else None
                ),
                "holdout_green_wallets": source_cohort_members,
                "observed_market_windows": len(source_market_windows),
                "qualifying_01a_market_windows": len(source_01a_windows),
                "push_observed_01a_market_windows": len(source_push_01a_windows),
                "identity_clean_01a_events": len(
                    {str(row.get("event_id") or "") for row in source_01a_rows}
                ),
                "qualifying_windows_per_day": (
                    round(source_qualifying_windows_per_day, 6)
                    if source_qualifying_windows_per_day is not None
                    else None
                ),
                "qualifying_windows_per_day_for_ladder": (
                    "NO_MEASUREMENT_FOR_TRADED_COHORT"
                    if active_live_wallets and ladder_observed_windows_per_day is None
                    else
                    round(ladder_observed_windows_per_day, 6)
                    if ladder_observed_windows_per_day is not None
                    else None
                ),
                "traded_cohort_wallets": sorted(active_live_wallets),
                "traded_cohort_observed_market_windows": len(traded_cohort_market_windows),
                "traded_cohort_qualifying_01a_market_windows": len(traded_cohort_01a_windows),
                "profit_ceiling_usd_per_day": (
                    round(source_supply_profit_usd, 6)
                    if source_supply_profit_usd is not None
                    else None
                ),
            },
            "gated_fill_counterfactual": {
                "authority": False,
                "warning": "closed-feed output; never publish as market supply alone",
                "observed_rows": measured_rows,
                "calendar_span_days": calendar_span_days,
                "qualifying_windows_per_day": (
                    round(qualifying_windows_per_day, 6)
                    if qualifying_windows_per_day is not None
                    else None
                ),
                "profit_ceiling_usd_per_day": (
                    round(gated_fill_supply_profit_usd, 6)
                    if gated_fill_supply_profit_usd is not None
                    else None
                ),
            },
            "perfect_288_profit_ceiling_usd_per_day": (
                round(perfect_supply_profit_usd, 6)
                if perfect_supply_profit_usd is not None
                else None
            ),
        },
        "inputs_complete": inputs_complete,
        "cap_origin_note": "effective cap is the tightest positive guard flag, selected runtime, or active member cap",
    }


def _material_working_tree_snapshot(root: Path) -> dict[str, Any]:
    material_prefixes = (
        "src/",
        "scripts/",
        "tests/",
        "docs/",
        "configs/",
        "AGENTS.md",
        "pyproject.toml",
        "requirements",
    )
    try:
        proc = subprocess.run(
            ["git", "status", "--short", "--untracked-files=no"],
            cwd=str(root),
            text=True,
            capture_output=True,
            timeout=2.0,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"status": "TIMEOUT", "count": None, "paths": []}
    except Exception as exc:  # pragma: no cover - diagnostic-only path.
        return {"status": "ERROR", "count": None, "paths": [], "error": f"{type(exc).__name__}: {exc}"}

    paths: list[str] = []
    for line in proc.stdout.splitlines():
        path = line[3:].strip()
        if " -> " in path:
            path = path.rsplit(" -> ", 1)[1].strip()
        if path.startswith(material_prefixes):
            paths.append(path)
    return {
        "status": "PASS" if proc.returncode == 0 else "GIT_STATUS_ERROR",
        "count": len(paths),
        "paths": paths[:20],
        "truncated": len(paths) > 20,
    }


def _window_start_from_slug(slug: Any) -> int | None:
    try:
        return int(str(slug or "").rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


def _rolling_window_participation_summary(participation: dict[str, Any]) -> dict[str, Any]:
    """Recover rolling FLOW truth from retained per-window rows when cycle aggregates reset."""
    rows = participation.get("window_rollups")
    if not isinstance(rows, list):
        rows = participation.get("rows")
    if not isinstance(rows, list):
        rows = []
    by_window: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        slug = str(row.get("market_slug") or "")
        if not slug:
            continue
        bucket = by_window.setdefault(
            slug,
            {
                "window_start": _window_start_from_slug(slug),
                "wallet_eligible_orders": 0,
                "our_submits": 0,
                "our_fills": 0,
                "missed": False,
            },
        )
        try:
            bucket["wallet_eligible_orders"] += int(row.get("wallet_eligible_orders") or 0)
            bucket["our_submits"] += int(row.get("our_submits") or 0)
            bucket["our_fills"] += int(row.get("our_fills") or 0)
        except (TypeError, ValueError):
            pass
        bucket["missed"] = bool(bucket["missed"] or row.get("missed_active_window"))
    windows = sorted(
        by_window.values(),
        key=lambda item: (item.get("window_start") is None, item.get("window_start") or 0),
    )
    active = [row for row in windows if int(row.get("wallet_eligible_orders") or 0) > 0]
    missed = [
        row
        for row in active
        if bool(row.get("missed")) or (int(row.get("our_submits") or 0) == 0 and int(row.get("our_fills") or 0) == 0)
    ]
    consecutive = 0
    for row in reversed(active):
        if row in missed:
            consecutive += 1
        else:
            break
    incident_threshold = participation.get("incident_threshold_windows", 6)
    try:
        threshold = int(incident_threshold or 6)
    except (TypeError, ValueError):
        threshold = 6
    return {
        "active_windows": len(active),
        "missed_active_windows": len(missed),
        "consecutive_missed_active_windows": consecutive,
        "incident_triggered": consecutive >= max(1, threshold),
        "basis": "rolling_window_rollups",
    }


def _live_order_counts_by_market(live: dict[str, Any]) -> dict[str, dict[str, int]]:
    orders = live.get("orders") if isinstance(live.get("orders"), list) else []
    counts: dict[str, dict[str, int]] = {}
    for row in orders:
        if not isinstance(row, dict):
            continue
        slug = str(row.get("market_slug") or "")
        if not slug:
            continue
        status = str(row.get("status") or row.get("final_status") or "").upper()
        bucket = counts.setdefault(slug, {"our_submits": 0, "our_fills": 0, "our_rejects": 0})
        if status in {"FILLED", "REJECTED"}:
            bucket["our_submits"] += 1
        if status == "FILLED":
            bucket["our_fills"] += 1
        elif status == "REJECTED":
            bucket["our_rejects"] += 1
    return counts


def _current_adjusted_participation_summary(participation: dict[str, Any]) -> dict[str, Any]:
    existing = participation.get("adjusted_participation")
    if isinstance(existing, dict) and existing:
        return dict(existing)
    rows = participation.get("window_rollups")
    if not isinstance(rows, list):
        rows = participation.get("rows")
    if not isinstance(rows, list):
        rows = []
    set_generation_id = str(participation.get("set_generation_id") or "")
    current_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if set_generation_id and row.get("set_generation_id") and str(row.get("set_generation_id")) != set_generation_id:
            continue
        item = dict(row)
        annotate_participation_item(item)
        if int(item.get("wallet_eligible_orders") or 0) > 0 and not item.get("miss_pending_market_lifecycle"):
            current_rows.append(item)
    threshold = participation.get("incident_threshold_windows", PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS)
    try:
        threshold_int = int(threshold or PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS)
    except (TypeError, ValueError):
        threshold_int = PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS
    return summarize_adjusted_participation(current_rows, incident_threshold_windows=threshold_int)


def _flow_truth_participation(participation: dict[str, Any]) -> dict[str, Any]:
    adjusted = _current_adjusted_participation_summary(participation)
    return {
        "active_windows": participation.get("active_windows"),
        "missed_active_windows": participation.get("missed_active_windows"),
        "consecutive_missed_active_windows": participation.get("consecutive_missed_active_windows"),
        "incident_triggered": adjusted.get("adjusted_incident_triggered", participation.get("incident_triggered")),
        "basis": "window_participation_current_generation",
    }


A689_CANARY_WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"
A689_POSTFIX_START_ISO = "2026-07-15T22:23:00Z"
A689_POSTFIX_START_TS = 1784154180.0


def _participation_row_ts(row: dict[str, Any]) -> float:
    for key in ("latest_observed_ts", "source_detection_observed_ts", "effective_latest_observed_ts", "window_start_s"):
        value = _as_float(row.get(key))
        if value is not None:
            return value
    return 0.0


def _live_order_source_wallet(order: dict[str, Any]) -> str:
    candidates: list[Any] = [order.get("source_wallet")]
    wallet_copy_inventory = order.get("wallet_copy_inventory")
    if isinstance(wallet_copy_inventory, dict):
        candidates.append(wallet_copy_inventory.get("source_wallet"))
    trade_decision = order.get("trade_decision")
    if isinstance(trade_decision, dict):
        wallet_copy = trade_decision.get("wallet_copy")
        if isinstance(wallet_copy, dict):
            candidates.append(wallet_copy.get("source_wallet"))
            metadata = wallet_copy.get("metadata")
            if isinstance(metadata, dict):
                candidates.append(metadata.get("source_wallet"))
                inventory = metadata.get("inventory_v2")
                if isinstance(inventory, dict):
                    candidates.append(inventory.get("source_wallet"))
    for candidate in candidates:
        wallet = _norm_wallet(candidate)
        if wallet:
            return wallet
    return ""


def _live_order_ts(order: dict[str, Any]) -> datetime | None:
    for key in ("updated_at", "submitted_at", "created_at"):
        parsed = _parse_utc_ts(order.get(key))
        if parsed:
            return parsed
    lifecycle = order.get("lifecycle")
    if isinstance(lifecycle, list):
        for item in reversed(lifecycle):
            if isinstance(item, dict):
                parsed = _parse_utc_ts(item.get("ts"))
                if parsed:
                    return parsed
    return None


def _live_fill_payload(order: dict[str, Any]) -> dict[str, Any]:
    lifecycle = order.get("lifecycle")
    if not isinstance(lifecycle, list):
        return {}
    for item in reversed(lifecycle):
        if not isinstance(item, dict):
            continue
        payload = item.get("payload")
        if (
            str(item.get("status") or "").upper() == "LIVE_FILLED"
            and isinstance(payload, dict)
        ):
            return payload
    return {}


def _d16_entry_band_acceptance(
    orders: list[Any],
    *,
    activation_at: Any,
    now: datetime,
    min_price: float = 0.25,
    max_price_exclusive: float = 0.32,
    sample_target: int = 20,
) -> dict[str, Any]:
    """Measure D16 without granting any authority to widen the live gate."""
    activation = _parse_utc_ts(activation_at)
    if activation is None:
        return {
            "flow_stage": "LIVE/LEARN/DEFEND",
            "status": "NO_ACTIVATION_EVIDENCE",
            "activation_at": None,
            "rollback_to_0_50_allowed": False,
            "next_action": "persist the loaded 0.32 generation start and rerun D16-5",
        }

    fills: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        if str(order.get("final_status") or "").upper() != "FILLED":
            continue
        order_ts = _live_order_ts(order)
        if order_ts is None or order_ts < activation:
            continue
        fill = _live_fill_payload(order)
        price = _as_float(
            fill.get("realized_entry_price")
            if fill.get("realized_entry_price") is not None
            else fill.get("response_fill_price")
        )
        cost = _as_float(
            fill.get("response_filled_size_usd")
            if fill.get("response_filled_size_usd") is not None
            else fill.get("making_amount")
        )
        attribution = order.get("alternate_transport_attribution")
        resolved = bool(
            isinstance(attribution, dict)
            and str(attribution.get("resolution_status") or "").upper() == "RESOLVED"
        )
        fills.append(
            {
                "ts": order_ts,
                "price": price,
                "cost_usd": cost or 0.0,
                "resolved": resolved,
                "in_band": bool(
                    price is not None
                    and min_price <= price < max_price_exclusive
                ),
            }
        )

    resolved_sample = [row for row in fills if row["resolved"]][:sample_target]
    out_of_band = [row for row in fills if not row["in_band"]]
    in_band_resolved = sum(1 for row in resolved_sample if row["in_band"])
    in_band_rate = (
        round(in_band_resolved / len(resolved_sample), 6)
        if resolved_sample
        else None
    )

    out_of_band_cost_by_day: dict[str, float] = {}
    fills_01a_by_day: dict[str, int] = {}
    for row in fills:
        day = row["ts"].date().isoformat()
        if not row["in_band"]:
            out_of_band_cost_by_day[day] = round(
                out_of_band_cost_by_day.get(day, 0.0) + row["cost_usd"], 6
            )
        if row["in_band"]:
            fills_01a_by_day[day] = fills_01a_by_day.get(day, 0) + 1

    first_full_day = activation.date() + timedelta(days=1)
    full_days = [first_full_day + timedelta(days=offset) for offset in range(2)]
    completed_days = [day for day in full_days if now.date() > day]
    daily_acceptance = [
        {
            "day": day.isoformat(),
            "complete": day in completed_days,
            "fills_01a": fills_01a_by_day.get(day.isoformat(), 0),
            "minimum_fills": 3,
            "passed": (
                fills_01a_by_day.get(day.isoformat(), 0) >= 3
                if day in completed_days
                else None
            ),
        }
        for day in full_days
    ]
    daily_failure = any(row["passed"] is False for row in daily_acceptance)
    sample_complete = len(resolved_sample) >= sample_target
    if out_of_band:
        status = "FAIL_OUT_OF_BAND_FILL"
        next_action = "record the live defect immediately; keep the 0.32 ceiling enforced"
    elif daily_failure:
        status = "FAIL_DAILY_SUPPLY"
        next_action = "execute D16-3 traded-cohort supply measurement; keep the 0.32 ceiling enforced"
    elif sample_complete and len(completed_days) == 2:
        status = "PASS"
        next_action = "retain the 0.32 ceiling and continue governed LIVE measurement"
    else:
        status = "PENDING_EVIDENCE"
        next_action = "collect the first 20 resolved fills and two full UTC days under the 0.32 ceiling"

    return {
        "flow_stage": "LIVE/LEARN/DEFEND",
        "status": status,
        "activation_at": activation.isoformat(),
        "enforced_band": {
            "min_inclusive": min_price,
            "max_exclusive": max_price_exclusive,
        },
        "resolved_fill_sample": {
            "target": sample_target,
            "count": len(resolved_sample),
            "in_band_count": in_band_resolved,
            "in_band_rate": in_band_rate,
            "passed": in_band_rate == 1.0 if sample_complete else None,
        },
        "all_post_activation_fills": len(fills),
        "out_of_band_fill_count": len(out_of_band),
        "out_of_band_cost_usd_by_day": out_of_band_cost_by_day,
        "out_of_band_cost_usd_total": round(
            sum(row["cost_usd"] for row in out_of_band), 6
        ),
        "first_two_full_utc_days": daily_acceptance,
        "rollback_to_0_50_allowed": False,
        "next_action": next_action,
    }


def _a689_live_postfix_summary(participation: dict[str, Any], live_state: dict[str, Any]) -> dict[str, Any]:
    rows_source = participation.get("rows")
    if not isinstance(rows_source, list):
        rows_source = participation.get("window_rollups")
    if not isinstance(rows_source, list):
        rows_source = []

    rows = [
        row
        for row in rows_source
        if isinstance(row, dict)
        and _norm_wallet(row.get("source_wallet")) == A689_CANARY_WALLET
        and _participation_row_ts(row) >= A689_POSTFIX_START_TS
    ]
    windows = {str(row.get("market_slug") or "") for row in rows if row.get("market_slug")}

    category_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    wallet_orders = 0
    our_submits = 0
    our_fills = 0
    for row in rows:
        category = str(row.get("participation_skip_category") or "unknown")
        reason = str(row.get("dominant_skip_reason") or "unknown")
        category_counts[category] = category_counts.get(category, 0) + 1
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        wallet_orders += _as_int(row.get("wallet_eligible_orders"))
        our_submits += _as_int(row.get("our_submits"))
        our_fills += _as_int(row.get("our_fills"))

    orders = live_state.get("orders") if isinstance(live_state.get("orders"), list) else []
    postfix_start_dt = _parse_utc_ts(A689_POSTFIX_START_ISO)
    live_orders = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        if _live_order_source_wallet(order) != A689_CANARY_WALLET:
            continue
        order_ts = _live_order_ts(order)
        if postfix_start_dt and (order_ts is None or order_ts < postfix_start_dt):
            continue
        live_orders.append(order)
    accepted_orders = [
        order
        for order in live_orders
        if str(order.get("final_status") or order.get("status") or "").upper()
        not in {"", "REJECTED", "UNFILLED", "LIVE_REJECTED"}
    ]

    recent = sorted(rows, key=_participation_row_ts, reverse=True)[:8]
    return {
        "flow_stage": "LIVE/DEFEND/MEASURE",
        "basis": "Fable 2026-07-16T00:10Z post-22:23Z a689 one-line instrumentation; no late decomposition",
        "source_wallet": A689_CANARY_WALLET,
        "postfix_start_iso": A689_POSTFIX_START_ISO,
        "rows": len(rows),
        "windows": len(windows),
        "wallet_eligible_orders": wallet_orders,
        "our_submits": our_submits,
        "our_fills": our_fills,
        "live_order_rows": len(live_orders),
        "accepted_order_rows": len(accepted_orders),
        "category_counts": dict(sorted(category_counts.items())),
        "dominant_skip_reason_counts": dict(sorted(reason_counts.items())),
        "recent": [
            {
                "market_slug": row.get("market_slug"),
                "window_start_s": row.get("window_start_s"),
                "wallet_eligible_orders": row.get("wallet_eligible_orders"),
                "our_submits": row.get("our_submits"),
                "our_fills": row.get("our_fills"),
                "dominant_skip_reason": row.get("dominant_skip_reason"),
                "participation_skip_category": row.get("participation_skip_category"),
                "latest_observed_ts": row.get("latest_observed_ts"),
            }
            for row in recent
        ],
    }


def _load_confirmed_guard_live_pair(
    data_dir: Path,
    *,
    confirm_delay_s: float = 1.0,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Avoid publishing a halt-shaped digest from one racing state read."""
    guard_path = data_dir / "wallet_copy_live_guard_state.json"
    live_path = data_dir / "wallet_copy_live_execution_state.json"
    guard = _load_json(guard_path, {})
    live = _load_json(live_path, {})
    guard_dict = guard if isinstance(guard, dict) else {}
    live_dict = live if isinstance(live, dict) else {}
    if _live_read_is_clean(guard_dict, live_dict):
        return guard_dict, live_dict, {
            "double_read": False,
            "confirmed_blocked": False,
            "confirmed_non_trading": False,
        }
    time.sleep(max(1.0, float(confirm_delay_s)))
    second_guard = _load_json(guard_path, {})
    second_live = _load_json(live_path, {})
    second_guard_dict = second_guard if isinstance(second_guard, dict) else {}
    second_live_dict = second_live if isinstance(second_live, dict) else {}
    second_clean = _live_read_is_clean(second_guard_dict, second_live_dict)
    return second_guard_dict, second_live_dict, {
        "double_read": True,
        "confirmed_blocked": _guard_live_pair_is_blocked(second_guard_dict, second_live_dict),
        "confirmed_non_trading": not second_clean,
        "first_guard_status": guard_dict.get("status"),
        "first_can_trade": _live_can_trade(live_dict),
        "second_guard_status": second_guard_dict.get("status"),
        "second_can_trade": _live_can_trade(second_live_dict),
    }


def _atomic_write(path: Path, text: str) -> None:
    atomic_write_text(path, text)


def _latest_scorecard(
    data_dir: Path,
    *,
    day_utc: str | None = None,
) -> dict[str, Any]:
    target_day = day_utc or datetime.now(timezone.utc).date().isoformat()
    candidates = sorted(
        data_dir.glob("wallet_copy_daily_scorecard_*.json"),
        key=lambda item: (item.stat().st_mtime if item.exists() else 0.0, item.name),
        reverse=True,
    )
    fallback: dict[str, Any] = {}
    for path in candidates:
        loaded = _load_json(path, {})
        if isinstance(loaded, dict) and loaded.get("kind") == "wallet_copy_daily_scorecard":
            loaded["_path"] = str(path)
            if str(loaded.get("day_utc") or "") == target_day:
                return loaded
            if not fallback:
                fallback = loaded
    return fallback


def _scorecard_runtime_evidence(
    data_dir: Path,
    *,
    threshold_s: float = SCORECARD_DIRECT_RUNTIME_THRESHOLD_S,
) -> dict[str, Any]:
    path = data_dir / "daily_scorecard_timing_latest.err"
    evidence: dict[str, Any] = {
        "path": str(path),
        "threshold_s": threshold_s,
    }
    if not path.exists():
        evidence["status"] = "MISSING"
        return evidence
    try:
        text = path.read_text(errors="replace")
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
    except OSError as exc:
        evidence["status"] = "UNREADABLE"
        evidence["error"] = str(exc)[:160]
        return evidence

    parsed: dict[str, float] = {}
    for line in text.splitlines():
        parts = line.strip().split()
        if len(parts) != 2 or parts[0] not in {"real", "user", "sys"}:
            continue
        try:
            value = float(parts[1])
        except ValueError:
            continue
        if value == value:
            parsed[parts[0]] = value

    real_s = parsed.get("real")
    evidence.update(
        {
            "source_mtime": mtime,
            "real_s": real_s,
            "user_s": parsed.get("user"),
            "sys_s": parsed.get("sys"),
        }
    )
    if real_s is None:
        evidence["status"] = "INVALID"
    elif real_s <= threshold_s:
        evidence["status"] = "PASS"
    else:
        evidence["status"] = "REGRESSION"
    return evidence


def _append_cash_diff_residual_trend(
    root: Path,
    residual: dict[str, Any],
    *,
    generated_at: str,
) -> list[dict[str, Any]]:
    if not isinstance(residual, dict):
        return []
    state_path = str(residual.get("state_path") or "").strip()
    if not state_path or residual.get("residual_usd") is None:
        return []
    path = _rooted_path(root, state_path)
    artifact = _load_json(path, {})
    if not isinstance(artifact, dict):
        return []
    summary = artifact.get("summary") if isinstance(artifact.get("summary"), dict) else {}
    previous_trend = summary.get("scorecard_delta_residual_trend")
    if not isinstance(previous_trend, list):
        previous_trend = []
    trend = [row for row in previous_trend if isinstance(row, dict)]
    trend.append(
        {
            "generated_at": generated_at,
            "cash_diff_residual_usd": residual.get("residual_usd"),
            "basis": str(residual.get("basis") or "unknown"),
            "writer": str(residual.get("writer") or "scripts/update_state_digest.py"),
        }
    )
    trend = trend[-RESIDUAL_TREND_LIMIT:]
    summary["scorecard_delta_residual_trend"] = trend
    artifact["summary"] = summary
    atomic_write_json(path, artifact)
    return trend


def _latest_enabled_overflow_proposal(data_dir: Path) -> dict[str, Any]:
    candidates = sorted(
        data_dir.glob("enabled_overflow_semantics_proposal_*.json"),
        key=lambda item: (item.stat().st_mtime if item.exists() else 0.0, item.name),
        reverse=True,
    )
    for path in candidates:
        loaded = _load_json(path, {})
        if isinstance(loaded, dict) and loaded.get("kind") == "wallet_copy_enabled_overflow_semantics_proposal":
            loaded["_path"] = str(path)
            return loaded
    return {}


def _latest_closed_scorecard(data_dir: Path) -> dict[str, Any]:
    today = datetime.now(timezone.utc).date().isoformat()
    candidates = sorted(
        data_dir.glob("wallet_copy_daily_scorecard_*.json"),
        key=lambda item: (item.stat().st_mtime if item.exists() else 0.0, item.name),
        reverse=True,
    )
    for path in candidates:
        loaded = _load_json(path, {})
        if not isinstance(loaded, dict) or loaded.get("kind") != "wallet_copy_daily_scorecard":
            continue
        day = str(loaded.get("day_utc") or "")
        if day and day < today:
            loaded["_path"] = str(path)
            return loaded
    return {}


def _scorecard_automation_drift_summary(scorecard: dict[str, Any]) -> dict[str, Any]:
    rows = scorecard.get("automation_drift")
    if not isinstance(rows, list):
        rows = []
    items: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    content_defects: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("pointer_status") or "UNKNOWN")
        item = {
            "kind": row.get("kind"),
            "name": row.get("name"),
            "path": row.get("path"),
            "pointer_status": status,
        }
        items.append(item)
        status_counts[status] = status_counts.get(status, 0) + 1
        if status.startswith("CONTENT"):
            content_defects.append(item)
    return {
        "entries": len(items),
        "content_defects": len(content_defects),
        "status_counts": dict(sorted(status_counts.items())),
        "items": items,
    }


def _scorecard_daily_pnl_usd(scorecard: dict[str, Any]) -> float | None:
    total = _canonical_day_score(scorecard)
    value = total.get("pnl_usd") if isinstance(total, dict) else None
    if value is None:
        target = scorecard.get("target_ladder") if isinstance(scorecard.get("target_ladder"), dict) else {}
        actual = target.get("actual") if isinstance(target.get("actual"), dict) else {}
        value = actual.get("day_pnl_usd")
    return _as_float(value)


def _scorecard_windows_filled(scorecard: dict[str, Any]) -> int:
    volume = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    canonical = volume.get("canonical_daily") if isinstance(volume.get("canonical_daily"), dict) else volume
    return _as_int(canonical.get("windows_filled") or canonical.get("windows_traded"), default=0)


def _weekly_verdict_from_scorecards(
    data_dir: Path,
    *,
    today: datetime | None = None,
    bankroll_usd: float | None = None,
) -> dict[str, Any]:
    now = today or datetime.now(timezone.utc)
    today_date = now.astimezone(timezone.utc).date()
    due = today_date.weekday() == 6  # Sunday 00:00 UTC heartbeat verdict.
    start_date = today_date - timedelta(days=7)
    # 2026-07-05 was explicitly baseline-only in AUTONOMOUS_FLOW.
    first_full_verdict_day = datetime(2026, 7, 6, tzinfo=timezone.utc).date()
    if start_date < first_full_verdict_day:
        start_date = first_full_verdict_day
    end_date = today_date - timedelta(days=1)
    by_day: dict[str, dict[str, Any]] = {}
    baseline_candidates: list[float] = []
    for path in sorted(data_dir.glob("wallet_copy_daily_scorecard_*.json")):
        loaded = _load_json(path, {})
        if not isinstance(loaded, dict) or loaded.get("kind") != "wallet_copy_daily_scorecard":
            continue
        day_text = str(loaded.get("day_utc") or "")
        try:
            day = datetime.fromisoformat(day_text).date()
        except ValueError:
            continue
        if day < start_date or day > end_date:
            continue
        pnl = _scorecard_daily_pnl_usd(loaded)
        if pnl is None:
            continue
        since_topup = loaded.get("since_topup_truth") if isinstance(loaded.get("since_topup_truth"), dict) else {}
        baseline = _as_float(since_topup.get("baseline_usd"))
        if baseline is not None:
            baseline_candidates.append(baseline)
        row = {
            "day_utc": day_text,
            "pnl_usd": round(float(pnl), 6),
            "windows_filled": _scorecard_windows_filled(loaded),
            "path": str(path),
            "_priority": 2 if path.name == f"wallet_copy_daily_scorecard_{day_text}.json" else 1,
            "_mtime": path.stat().st_mtime if path.exists() else 0.0,
        }
        previous = by_day.get(day_text)
        if (
            previous is None
            or row["_priority"] > previous.get("_priority", 0)
            or (row["_priority"] == previous.get("_priority", 0) and row["_mtime"] > previous.get("_mtime", 0.0))
        ):
            by_day[day_text] = row
    expected_days = []
    cursor = start_date
    while cursor <= end_date:
        expected_days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    rows = [by_day[day] for day in sorted(by_day)]
    for row in rows:
        row.pop("_priority", None)
        row.pop("_mtime", None)
    weekly_pnl = round(sum(float(row["pnl_usd"]) for row in rows), 6)
    bankroll = float(bankroll_usd or (baseline_candidates[-1] if baseline_candidates else 335.0))
    target_min = round(bankroll * 0.10, 6)
    target_max = round(bankroll * 0.15, 6)
    pct = round((weekly_pnl / bankroll * 100.0), 6) if bankroll else None
    return {
        "due": due,
        "week_start_utc": start_date.isoformat(),
        "week_end_utc": end_date.isoformat(),
        "days_expected": expected_days,
        "days_present": [row["day_utc"] for row in rows],
        "missing_days": [day for day in expected_days if day not in by_day],
        "weekly_pnl_usd": weekly_pnl,
        "weekly_pct": pct,
        "bankroll_usd": bankroll,
        "target_min_usd": target_min,
        "target_max_usd": target_max,
        "gap_to_min_usd": round(weekly_pnl - target_min, 6),
        "verdict": "PASS" if weekly_pnl >= target_min and not [day for day in expected_days if day not in by_day] else "FAIL",
        "windows_filled_total": sum(_as_int(row.get("windows_filled"), default=0) for row in rows),
        "rows": rows,
    }


def _current_scorecard(root: Path) -> dict[str, Any]:
    """Build a fresh current-day scorecard without writing a repo artifact."""
    script = root / "scripts" / "daily_scorecard.py"
    if not script.exists():
        return {}
    env = dict(os.environ)
    env.setdefault("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_COUNT", "0")
    try:
        proc = subprocess.run(
            [
                sys.executable,
                str(script),
                "--format",
                "json",
                "--balance-sample-count",
                "1",
                "--balance-sample-interval-s",
                "0",
            ],
            cwd=str(root),
            text=True,
            capture_output=True,
            check=False,
            env=env,
            timeout=SCORECARD_CURRENT_BUILD_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        return {}
    if proc.returncode != 0:
        return {}
    loaded = _load_json_from_text(proc.stdout, {})
    if isinstance(loaded, dict) and loaded.get("kind") == "wallet_copy_daily_scorecard":
        loaded["_path"] = "fresh_current_day_scorecard_stdout"
        return loaded
    return {}


def _load_json_from_text(text: str, default: Any) -> Any:
    try:
        return json.loads(text)
    except Exception:
        pass
    decoder = json.JSONDecoder()
    best: Any = default
    index = 0
    while True:
        start = text.find("{", index)
        if start < 0:
            break
        try:
            loaded, end = decoder.raw_decode(text[start:])
        except Exception:
            index = start + 1
            continue
        if isinstance(loaded, dict):
            best = loaded
            if loaded.get("kind") == "wallet_copy_daily_scorecard":
                return loaded
        index = start + max(end, 1)
    return best


def _parse_utc_ts(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _scorecard_generated_ts(scorecard: dict[str, Any]) -> datetime | None:
    for key in ("generated_at", "created_at", "updated_at"):
        parsed = _parse_utc_ts(scorecard.get(key))
        if parsed:
            return parsed
    return None


def _latest_live_order_ts(data_dir: Path) -> datetime | None:
    live = _load_json(data_dir / "wallet_copy_live_execution_state.json", {})
    summary = live.get("summary") if isinstance(live.get("summary"), dict) else {}
    candidates = [_parse_utc_ts(summary.get("latest_order_ts"))]
    orders = live.get("orders") if isinstance(live.get("orders"), list) else []
    for order in orders:
        if not isinstance(order, dict):
            continue
        for key in ("submitted_at", "filled_at", "created_at", "updated_at"):
            candidates.append(_parse_utc_ts(order.get(key)))
    parsed = [item for item in candidates if item is not None]
    return max(parsed) if parsed else None


def _latest_live_submitted_at(live_state: dict[str, Any]) -> datetime | None:
    orders = live_state.get("orders") if isinstance(live_state.get("orders"), list) else []
    parsed = [
        submitted
        for order in orders
        if isinstance(order, dict)
        for submitted in [_parse_utc_ts(order.get("submitted_at"))]
        if submitted is not None
    ]
    return max(parsed) if parsed else None


def _latest_live_order_snapshot(
    live_state: dict[str, Any],
    *,
    accepted_only: bool = False,
) -> dict[str, Any]:
    orders = live_state.get("orders") if isinstance(live_state.get("orders"), list) else []
    candidates: list[tuple[datetime, dict[str, Any]]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        status = str(order.get("final_status") or order.get("status") or "").upper()
        trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        venue_submitted = bool(str(trade_result.get("order_id") or "").strip())
        if accepted_only and not venue_submitted:
            continue
        timestamps = [
            _parse_utc_ts(order.get(key))
            for key in ("submitted_at", "filled_at", "created_at", "updated_at")
        ]
        order_ts = max((value for value in timestamps if value is not None), default=None)
        if order_ts is not None:
            candidates.append((order_ts, order))
    if not candidates:
        return {}
    order_ts, order = max(candidates, key=lambda item: item[0])
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    funding = (
        trade_result.get("maker_min_share_funding")
        if isinstance(trade_result.get("maker_min_share_funding"), dict)
        else {}
    )
    return {
        "order_id": order.get("order_id"),
        "intent_id": order.get("intent_id"),
        "status": order.get("final_status") or order.get("status"),
        "submitted_at": order.get("submitted_at") or order_ts.isoformat(),
        "filled_at": order.get("filled_at"),
        "market_slug": order.get("market_slug"),
        "maker": order.get("maker"),
        "limit_price": order.get("limit_price"),
        "requested_notional_usd": funding.get("requested_notional_usd"),
        "funded_notional_usd": funding.get("funded_notional_usd"),
        "funded_shares": funding.get("funded_shares"),
        "filled_size_usd": order.get("filled_size_usd")
        if order.get("filled_size_usd") is not None
        else trade_result.get("filled_size_usd"),
        "response_fill_size_shares": (
            (trade_result.get("wallet_copy_maker_cancel") or {}).get("matched_shares")
            if isinstance(trade_result.get("wallet_copy_maker_cancel"), dict)
            else trade_result.get("response_fill_size_shares")
        ),
    }


def _scorecard_for_digest(root: Path, data_dir: Path) -> dict[str, Any]:
    latest = _latest_scorecard(data_dir)
    today = datetime.now(timezone.utc).date().isoformat()
    if latest and str(latest.get("day_utc") or "") == today:
        live_latest = _latest_live_order_ts(data_dir)
        generated = _scorecard_generated_ts(latest)
        if live_latest and (generated is None or generated < live_latest):
            current = _current_scorecard(root)
            if current:
                return current
        return latest
    current = _current_scorecard(root)
    if current:
        return current
    if root.resolve() != ROOT.resolve():
        # Fixture/portable roots may intentionally model a historical day.
        return latest
    if latest and not str(latest.get("day_utc") or ""):
        # Legacy/test packets without a declared day are not evidence of a
        # stale UTC cut; retain their existing semantics.
        return latest
    # Never publish yesterday's day PnL, fill count, or UTC-scoped defenses as
    # today's truth when the fresh scorecard subprocess times out. Preserve
    # only cross-day since-topup truth and make the missing current-day cut
    # explicit; a false zero/stale day is more dangerous than an unknown day.
    return {
        "kind": "wallet_copy_daily_scorecard",
        "day_utc": today,
        "generated_at": _utc_now_iso(),
        "today": {},
        "canonical_pnl_truth": {},
        "day_pnl_basis": {},
        "since_topup_truth": (
            latest.get("since_topup_truth")
            if isinstance(latest.get("since_topup_truth"), dict)
            else {}
        ),
        "current_day_scorecard_status": "UNAVAILABLE_STALE_DAY_DATA_REFUSED",
        "stale_scorecard_day_utc": latest.get("day_utc"),
        "_path": "fresh_current_day_scorecard_unavailable",
    }


def _scorecard_day_key(scorecard: dict[str, Any], by_day: dict[str, Any]) -> str:
    day = str(scorecard.get("day_utc") or "")
    if day and day in by_day:
        return day
    if by_day:
        return sorted(str(key) for key in by_day)[-1]
    return day


def _canonical_day_score(scorecard: dict[str, Any]) -> dict[str, Any]:
    basis = scorecard.get("day_pnl_basis") if isinstance(scorecard.get("day_pnl_basis"), dict) else {}
    if basis.get("day_pnl_response_basis") is not None:
        today = scorecard.get("today") if isinstance(scorecard.get("today"), dict) else {}
        total = dict(today.get("total") if isinstance(today.get("total"), dict) else {})
        total["pnl_usd"] = basis.get("day_pnl_response_basis")
        return total
    canonical = scorecard.get("canonical_pnl_truth")
    canonical = canonical if isinstance(canonical, dict) else {}
    by_day = canonical.get("by_day") if isinstance(canonical.get("by_day"), dict) else {}
    day_key = _scorecard_day_key(scorecard, by_day)
    row = by_day.get(day_key) if day_key else None
    return row if isinstance(row, dict) else {}


def _canonical_member_scores(scorecard: dict[str, Any]) -> dict[str, Any]:
    canonical = scorecard.get("canonical_pnl_truth")
    canonical = canonical if isinstance(canonical, dict) else {}
    by_member = canonical.get("by_member") if isinstance(canonical.get("by_member"), dict) else {}
    return by_member


def _canonical_member_trigger_watch(scorecard: dict[str, Any]) -> dict[str, dict[str, Any]]:
    canonical = scorecard.get("canonical_pnl_truth")
    canonical = canonical if isinstance(canonical, dict) else {}
    events = canonical.get("events") if isinstance(canonical.get("events"), list) else []
    by_wallet: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        if not isinstance(event, dict) or not event.get("resolved"):
            continue
        wallet = _norm_wallet(event.get("source_wallet"))
        if not wallet:
            continue
        by_wallet.setdefault(wallet, []).append(event)

    out: dict[str, dict[str, Any]] = {}
    trigger_usd = -6.0
    for wallet, rows in by_wallet.items():
        rows.sort(key=lambda row: (str(row.get("submitted_at") or ""), float(row.get("ts") or 0.0)))
        signs = []
        for row in rows[-6:]:
            pnl = _as_float(row.get("pnl_usd")) or 0.0
            signs.append("+" if pnl > 0 else "-" if pnl < 0 else "0")
        tail_negative = 0
        for row in reversed(rows):
            pnl = _as_float(row.get("pnl_usd")) or 0.0
            if pnl < 0:
                tail_negative += 1
            else:
                break
        resolved_pnl = round(sum((_as_float(row.get("pnl_usd")) or 0.0) for row in rows), 6)
        out[wallet] = {
            "resolved_fills": len(rows),
            "resolved_only_pnl_usd": resolved_pnl,
            "last6_signs": "".join(signs),
            "tail_negative": tail_negative,
            "first_slice_trigger_usd": trigger_usd,
            "distance_to_first_slice_trigger_usd": round(resolved_pnl - trigger_usd, 6),
            "trigger_fired_by_pnl": resolved_pnl <= trigger_usd,
            "trigger_fired_by_tail": tail_negative >= 5,
        }
    return out


def _scorecard_text_day_pnl(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        text = path.read_text(errors="replace")
    except Exception:
        return None
    day_match = re.search(r"^day_utc=(?P<day>\d{4}-\d{2}-\d{2})\b", text, flags=re.MULTILINE)
    match = re.search(
        r"total orders=(?P<orders>\d+) fills=(?P<fills>\d+) "
        r"resolved=(?P<resolved>\d+) rejects=(?P<rejects>\d+) "
        r"pnl=(?P<pnl>[+-]?\d+(?:\.\d+)?)",
        text,
    )
    if not match:
        return None
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        mtime = None
    volume_match = re.search(
        r"^volume_kpi:\s+windows_filled=(?P<filled>\d+)/(?P<denominator>\d+).*?"
        r"windows_submitted=(?P<submitted>\d+)/(?P<submitted_denominator>\d+)",
        text,
        flags=re.MULTILINE,
    )
    since_topup_match = re.search(
        r"^since_topup_truth:\s+verdict=(?P<verdict>\S+).*?"
        r"\bactual_delta=(?P<actual_delta>[+-]?\d+(?:\.\d+)?)",
        text,
        flags=re.MULTILINE,
    )
    return {
        "path": _rel_path(path),
        "day_utc": day_match.group("day") if day_match else None,
        "orders": _as_int(match.group("orders")),
        "fills": _as_int(match.group("fills")),
        "resolved_fills": _as_int(match.group("resolved")),
        "rejects": _as_int(match.group("rejects")),
        "pnl_usd": _as_float(match.group("pnl")),
        "mtime": mtime,
        "windows_filled": _as_int(volume_match.group("filled")) if volume_match else None,
        "windows_submitted": _as_int(volume_match.group("submitted")) if volume_match else None,
        "denominator_windows": _as_int(volume_match.group("denominator")) if volume_match else None,
        "since_topup_actual_delta_usd": (
            _as_float(since_topup_match.group("actual_delta")) if since_topup_match else None
        ),
        "since_topup_verdict": since_topup_match.group("verdict") if since_topup_match else None,
    }


def _prefer_newer_scorecard_text_truth(
    *,
    scorecard: dict[str, Any],
    score_total: dict[str, Any],
    volume: dict[str, Any],
    since_topup: dict[str, Any],
    text_truth: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Overlay a newer synchronous scorecard text packet onto stale JSON.

    ``brainless_ops_scorecard.out`` is refreshed immediately from the ledger.
    The JSON scorecard family can lag behind it during concurrent artifact
    rewrites.  A same-day text packet with more resolved fills is therefore the
    stronger current-money/volume observation.
    """
    text_truth = text_truth if isinstance(text_truth, dict) else {}
    selected_day = str(scorecard.get("day_utc") or "")
    text_day = str(text_truth.get("day_utc") or "")
    selected_resolved = _as_int(score_total.get("resolved_fills"), default=-1)
    text_resolved = _as_int(text_truth.get("resolved_fills"), default=-1)
    if not selected_day or text_day != selected_day or text_resolved <= selected_resolved:
        return dict(score_total), dict(volume), dict(since_topup)

    preferred_total = dict(score_total)
    for source_key, target_key in (
        ("orders", "orders"),
        ("fills", "fills"),
        ("resolved_fills", "resolved_fills"),
        ("rejects", "rejects"),
        ("pnl_usd", "pnl_usd"),
    ):
        if text_truth.get(source_key) is not None:
            preferred_total[target_key] = text_truth[source_key]

    preferred_volume = dict(volume)
    for key in ("windows_filled", "windows_submitted", "denominator_windows"):
        if text_truth.get(key) is not None:
            preferred_volume[key] = text_truth[key]

    preferred_since_topup = dict(since_topup)
    if text_truth.get("since_topup_actual_delta_usd") is not None:
        preferred_since_topup["actual_delta_vs_baseline_usd"] = text_truth[
            "since_topup_actual_delta_usd"
        ]
    if text_truth.get("since_topup_verdict"):
        preferred_since_topup["primary_verdict"] = text_truth["since_topup_verdict"]
    preferred_since_topup["scorecard_text_truth_preferred"] = True
    return preferred_total, preferred_volume, preferred_since_topup


def _deadman_fill_basis_day_pnl(order_flow_deadman: dict[str, Any]) -> float | None:
    quiet = (
        order_flow_deadman.get("gated_quiet_classification")
        if isinstance(order_flow_deadman.get("gated_quiet_classification"), dict)
        else {}
    )
    ruling = (
        quiet.get("morning_bench_ruling")
        if isinstance(quiet.get("morning_bench_ruling"), dict)
        else {}
    )
    selected = (
        ruling.get("selected_member")
        if isinstance(ruling.get("selected_member"), dict)
        else {}
    )
    size_defense = (
        selected.get("size_defense")
        if isinstance(selected.get("size_defense"), dict)
        else {}
    )
    return _as_float(size_defense.get("day_pnl_usd"))


def _deadman_fill_basis_observation(order_flow_deadman: dict[str, Any]) -> dict[str, Any]:
    day_pnl = _deadman_fill_basis_day_pnl(order_flow_deadman)
    if day_pnl is None:
        return {}
    observed_at = (
        order_flow_deadman.get("checked_at")
        or order_flow_deadman.get("generated_at")
        or order_flow_deadman.get("updated_at")
        or order_flow_deadman.get("ts")
    )
    quiet = (
        order_flow_deadman.get("gated_quiet_classification")
        if isinstance(order_flow_deadman.get("gated_quiet_classification"), dict)
        else {}
    )
    ruling = quiet.get("morning_bench_ruling") if isinstance(quiet.get("morning_bench_ruling"), dict) else {}
    selected = ruling.get("selected_member") if isinstance(ruling.get("selected_member"), dict) else {}
    size_defense = selected.get("size_defense") if isinstance(selected.get("size_defense"), dict) else {}
    return {
        "day_pnl_usd": day_pnl,
        "observed_at": observed_at,
        "day_utc": size_defense.get("day_utc"),
    }


def _live_selection_surfaces(
    order_flow_deadman: dict[str, Any],
    orderfilled_fast_lane: dict[str, Any],
) -> list[dict[str, Any]]:
    """Report every independently armed selector without declaring one canonical."""
    halt_signal = (
        order_flow_deadman.get("guard_side_halt_signal")
        if isinstance(order_flow_deadman.get("guard_side_halt_signal"), dict)
        else {}
    )
    runtime_member = (
        halt_signal.get("runtime_member_submittability")
        if isinstance(halt_signal.get("runtime_member_submittability"), dict)
        else {}
    )
    bridge_report = (
        orderfilled_fast_lane.get("latest_nonempty_bridge_report")
        if isinstance(
            orderfilled_fast_lane.get("latest_nonempty_bridge_report"), dict
        )
        else orderfilled_fast_lane.get("bridge_report")
        if isinstance(orderfilled_fast_lane.get("bridge_report"), dict)
        else {}
    )
    terminal_ring = [
        row
        for row in orderfilled_fast_lane.get("terminal_ring") or []
        if isinstance(row, dict)
    ]

    def submitted_for(wallet: Any) -> int:
        normalized = str(wallet or "").strip().lower()
        return sum(
            int(row.get("orders_submitted") or 0)
            for row in terminal_ring
            if str(row.get("source_wallet") or "").strip().lower() == normalized
        )

    rows: list[dict[str, Any]] = []
    runtime_wallet = runtime_member.get("source_wallet")
    if runtime_wallet:
        rows.append(
            {
                "surface": "deadman_runtime_member",
                "wallet": str(runtime_wallet).lower(),
                "policy_id": runtime_member.get("policy_id"),
                "submitted_orders_last_ring": submitted_for(runtime_wallet),
                "source_artifact": "data/research/order_flow_deadman_state.json",
            }
        )
    bridge_wallet = bridge_report.get("selected_wallet")
    if bridge_wallet:
        rows.append(
            {
                "surface": "alternate_transport_bridge",
                "wallet": str(bridge_wallet).lower(),
                "policy_id": bridge_report.get("selected_policy_id"),
                "submitted_orders_last_ring": submitted_for(bridge_wallet),
                "source_artifact": (
                    "data/research/wallet_copy_orderfilled_fast_lane_state.json"
                ),
            }
        )
    return rows


def _day_pnl_basis_reconciliation(
    *,
    selected_day_pnl: Any,
    selected_basis: Any,
    selected_resolved_fills: Any = None,
    selected_day_utc: Any = None,
    scorecard_text_day_pnl: dict[str, Any] | float | None = None,
    deadman_fill_basis_day_pnl: float | None = None,
    deadman_fill_basis_observed_at: Any = None,
    deadman_fill_basis_day_utc: Any = None,
) -> dict[str, Any]:
    selected = _as_float(selected_day_pnl)
    selected_resolved = _as_int(selected_resolved_fills, default=-1)
    selected_resolved = None if selected_resolved < 0 else selected_resolved
    text_meta = scorecard_text_day_pnl if isinstance(scorecard_text_day_pnl, dict) else {}
    selected_day = str(selected_day_utc or "") or None
    text_day = str(text_meta.get("day_utc") or "") or None
    deadman_day = str(deadman_fill_basis_day_utc or "") or None
    scorecard_text_pnl = (
        _as_float(text_meta.get("pnl_usd"))
        if isinstance(scorecard_text_day_pnl, dict)
        else _as_float(scorecard_text_day_pnl)
    )
    text_fills = _as_int(text_meta.get("fills"), default=-1) if text_meta else -1
    text_fills = None if text_fills < 0 else text_fills
    text_resolved = _as_int(text_meta.get("resolved_fills"), default=-1) if text_meta else -1
    text_resolved = None if text_resolved < 0 else text_resolved
    text_compare_count = text_resolved if text_resolved is not None else text_fills
    text_mtime = _parse_utc_ts(text_meta.get("mtime")) if text_meta else None
    deadman_observed_at = _parse_utc_ts(deadman_fill_basis_observed_at)
    text_older_than_deadman = (
        None
        if text_mtime is None or deadman_observed_at is None
        else text_mtime < deadman_observed_at
    )
    text_day_mismatch = bool(selected_day and text_day and text_day != selected_day)
    # A legacy deadman observation without an explicit basis day is not safe
    # across UTC rollover. Treat it as unknown until the guard republishes the
    # size-defense payload with its day_utc.
    deadman_day_mismatch = bool(selected_day and deadman_day != selected_day)
    if text_day_mismatch:
        scorecard_text_pnl = None
        text_compare_count = None
    if deadman_day_mismatch:
        deadman_fill_basis_day_pnl = None

    def delta(left: float | None, right: float | None) -> float | None:
        if left is None or right is None:
            return None
        return round(left - right, 6)

    values = [
        value
        for value in (selected, scorecard_text_pnl, deadman_fill_basis_day_pnl)
        if value is not None
    ]
    if len(values) < 2:
        status = "PARTIAL"
    elif max(values) - min(values) <= 0.01:
        status = "MATCH"
    elif (
        scorecard_text_pnl is not None
        and deadman_fill_basis_day_pnl is not None
        and abs(scorecard_text_pnl - deadman_fill_basis_day_pnl) <= 0.01
    ):
        # The synchronously refreshed text and deadman fill bases agree.  A
        # newer selected scorecard may already have hardened an additional
        # resolution; that named ordering race is informational, not a stale
        # text alarm (Fable 2026-07-22T19:21Z).
        status = "BENIGN_RACE"
    elif (
        selected_resolved is not None
        and text_compare_count is not None
        and text_compare_count < selected_resolved
    ):
        status = "STALE_TEXT_BASIS"
    elif (
        selected_resolved is not None
        and text_compare_count is not None
        and text_compare_count != selected_resolved
    ):
        status = "FILL_COUNT_DIVERGED"
    elif (
        selected is not None
        and scorecard_text_pnl is not None
        and deadman_fill_basis_day_pnl is not None
        and abs(selected - scorecard_text_pnl) <= 0.01
        and abs(selected - deadman_fill_basis_day_pnl) > 0.01
    ):
        status = "STALE_TEXT_BASIS" if text_older_than_deadman is True else "BENIGN_RACE"
    else:
        status = "MISMATCH"
    return {
        "status": status,
        "selected_day_pnl_usd": selected,
        "selected_day_utc": selected_day,
        "selected_basis": selected_basis,
        "selected_resolved_fills": selected_resolved,
        "scorecard_text_day_pnl_usd": scorecard_text_pnl,
        "scorecard_text_day_utc": text_day,
        "scorecard_text_day_mismatch": text_day_mismatch,
        "scorecard_text_orders": text_meta.get("orders") if text_meta else None,
        "scorecard_text_fills": text_fills,
        "scorecard_text_resolved_fills": text_resolved,
        "scorecard_text_rejects": text_meta.get("rejects") if text_meta else None,
        "scorecard_text_mtime": text_meta.get("mtime") if text_meta else None,
        "scorecard_text_path": text_meta.get("path") if text_meta else None,
        "deadman_fill_basis_day_pnl_usd": deadman_fill_basis_day_pnl,
        "deadman_fill_basis_day_utc": deadman_day,
        "deadman_fill_basis_day_mismatch": deadman_day_mismatch,
        "deadman_fill_basis_observed_at": deadman_fill_basis_observed_at,
        "scorecard_text_older_than_deadman_fill_basis": text_older_than_deadman,
        "scorecard_text_vs_deadman_delta_usd": delta(scorecard_text_pnl, deadman_fill_basis_day_pnl),
        "scorecard_text_vs_deadman_abs_delta_usd": (
            None
            if scorecard_text_pnl is None or deadman_fill_basis_day_pnl is None
            else round(abs(scorecard_text_pnl - deadman_fill_basis_day_pnl), 6)
        ),
        "selected_vs_deadman_delta_usd": delta(selected, deadman_fill_basis_day_pnl),
        "selected_vs_scorecard_text_delta_usd": delta(selected, scorecard_text_pnl),
        "dashboard_money_source": "state_digest.pnl.day_pnl_usd",
    }


def _handoff_entries(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    text = path.read_text(errors="replace")
    matches = list(re.finditer(r"^## .+$", text, flags=re.MULTILINE))
    entries: list[dict[str, str]] = []
    for idx, match in enumerate(matches):
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        heading = match.group(0).strip()
        body = text[match.end() : end].strip()
        entries.append({"heading": heading, "body": body})
    return entries


def _entry_timestamp(entry: dict[str, str]) -> datetime | None:
    match = re.match(r"^##\s+(\S+)", entry.get("heading", ""))
    if not match:
        return None
    return _parse_utc_ts(match.group(1))


def _entry_timestamp_iso(entry: dict[str, str]) -> str | None:
    parsed = _entry_timestamp(entry)
    if parsed is None:
        return None
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _latest_matching_entry(entries: list[dict[str, str]], matcher: Any) -> dict[str, str]:
    for entry in reversed(entries):
        if matcher(entry):
            return entry
    return {}


def _latest_entry(entries: list[dict[str, str]], needle: str) -> dict[str, str]:
    needle = needle.lower()
    for entry in reversed(entries):
        if needle in entry["heading"].lower():
            return entry
    return {}


def _is_status_entry(entry: dict[str, str]) -> bool:
    heading = entry.get("heading", "").lower()
    status_heading = re.match(
        r"^##\s+\S+(?:\s+codex)?\s+(?:daily[+\s/-]*)?status(?:\s|\[|$)",
        heading,
    )
    return bool(
        status_heading
    ) or bool(re.match(r"^##\s+\S+\s+daily(?:\s|\[|$)", heading))


def _latest_status_entry(entries: list[dict[str, str]]) -> dict[str, str]:
    latest: dict[str, str] = {}
    latest_ts: datetime | None = None
    for entry in entries:
        if not _is_status_entry(entry):
            continue
        entry_ts = _entry_timestamp(entry)
        if entry_ts is None:
            if not latest:
                latest = entry
            continue
        if latest_ts is None or entry_ts >= latest_ts:
            latest = entry
            latest_ts = entry_ts
    return latest


def _latest_handoff_entry(entries: list[dict[str, str]]) -> dict[str, str]:
    """Return the newest timestamped HANDOFF entry regardless of its kind."""
    latest: dict[str, str] = {}
    latest_ts: datetime | None = None
    for entry in entries:
        entry_ts = _entry_timestamp(entry)
        if entry_ts is None:
            if not latest:
                latest = entry
            continue
        if latest_ts is None or entry_ts >= latest_ts:
            latest = entry
            latest_ts = entry_ts
    return latest


def _status_usd_value(body: str, label: str) -> float | None:
    """Read a signed USD value from a compact STATUS evidence line."""
    # STATUS evidence commonly wraps individual values in Markdown backticks.
    # Strip only that presentation character before parsing so the persisted
    # baseline is independent of handoff formatting.
    body = body.replace("`", "")
    match = re.search(
        rf"{re.escape(label)}\s*=\s*([+-]?)\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
        body,
        flags=re.IGNORECASE,
    )
    if not match:
        # Heartbeat STATUS entries commonly report an explicit before/after
        # transition (``day +$1.00 -> +$2.00``).  The right-hand value is the
        # persisted baseline for the next heartbeat, not the left-hand value.
        match = re.search(
            rf"{re.escape(label)}\s+"
            r"[+-]?\s*\$?\s*[0-9][0-9,]*(?:\.[0-9]+)?\s*"
            r"(?:->|→)\s*([+-]?)\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
            body,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
    value = float(match.group(2).replace(",", ""))
    return -value if match.group(1) == "-" else value


def _status_pnl_day_usd(body: str) -> float | None:
    """Read day PnL only from canonical STATUS money bullets.

    A body-wide lookup for ``day`` is unsafe because STATUS packets also
    contain unrelated day-scoped counters.  Current packets use a dedicated
    ``- pnl [STAGE]: day=...`` or ``- money [STAGE]: day=...`` line, so
    constrain the fallback to those lines.
    """
    match = re.search(
        r"(?:^|\n)-\s*(?:pnl|money|daily[-_]pnl(?:\s+|_)gap)(?:\s*\[[^\]]+\])?\s*:[^\n]*?"
        r"\b(?:day|nap)\s*=\s*`?([+-]?)\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
        body,
        flags=re.IGNORECASE,
    )
    if not match:
        match = re.search(
            r"(?:^|\n)-\s*pnl(?:\s*\[[^\]]+\])?\s*:[^\n]*?"
            r"\bday\s+[+-]?\s*\$?\s*[0-9][0-9,]*(?:\.[0-9]+)?\s*"
            r"(?:->|→)\s*([+-]?)\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
            body,
            flags=re.IGNORECASE,
        )
        if not match:
            match = re.search(
                r"(?:^|\n)-\s*ledger_delta(?:\s*\[[^\]]+\])?\s*:[^\n]*?"
                r"\b(?:day|today)(?:\s+pnl)?\s+`?[+-]?\s*\$?\s*"
                r"[0-9][0-9,]*(?:\.[0-9]+)?`?\s*"
                r"(?:->|→)\s*`?([+-]?)\s*\$?\s*"
                r"([0-9][0-9,]*(?:\.[0-9]+)?)",
                body,
                flags=re.IGNORECASE,
            )
            if not match:
                match = re.search(
                    r"(?:^|\n)-\s*ledger_delta(?:\s*\[[^\]]+\])?\s*:[^\n]*?"
                    r"\b(?:day|today)(?:\s+pnl)?\s*(?:=\s*)?`?([+-]?)\s*\$?\s*"
                    r"([0-9][0-9,]*(?:\.[0-9]+)?)",
                    body,
                    flags=re.IGNORECASE,
                )
            if not match:
                match = re.search(
                    r"(?:^|\n)-\s*gaps(?:\s*\[[^\]]+\])?\s*:[^\n]*?"
                    r"\bdaily[-_]pnl(?:\s+|_)gap\s*(?:(?:persists\s+)?at|=)\s*`?"
                    r"([+-]?)\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
                    body,
                    flags=re.IGNORECASE,
                )
                if not match:
                    return None
    value = float(match.group(2).replace(",", ""))
    return -value if match.group(1) == "-" else value


def _heartbeat_ledger_delta(
    live: dict[str, Any],
    latest_status: dict[str, str],
    *,
    current_day_pnl_usd: Any,
    current_since_topup_actual_usd: Any,
) -> dict[str, Any]:
    """Summarize live-ledger changes since the latest persisted heartbeat STATUS."""
    since = _entry_timestamp(latest_status)
    orders = live.get("orders") if isinstance(live.get("orders"), list) else []
    rows: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        submitted_at = _parse_utc_ts(order.get("submitted_at"))
        if since is not None and submitted_at is not None and submitted_at > since:
            rows.append(order)

    status_counts: dict[str, int] = {}
    filled_size_usd = 0.0
    for order in rows:
        status = str(order.get("final_status") or order.get("status") or "UNKNOWN").upper()
        status_counts[status] = status_counts.get(status, 0) + 1
        if status != "FILLED":
            continue
        result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        filled_size_usd += _as_float(
            result.get("response_filled_size_usd")
            if result.get("response_filled_size_usd") is not None
            else result.get("filled_size_usd")
        ) or 0.0

    body = latest_status.get("body", "")
    # Newer STATUS packets label the canonical ledger value as ``canonical day``.
    # Prefer it over the legacy ``today`` token, which can also occur in sensor
    # fields such as ``peer_active_idle_windows today=97``.
    previous_day_pnl = _status_usd_value(body, "canonical day")
    if previous_day_pnl is None:
        previous_day_pnl = _status_pnl_day_usd(body)
    previous_since_topup = _status_usd_value(body, "since-topup actual")
    current_day_pnl = _as_float(current_day_pnl_usd)
    current_since_topup = _as_float(current_since_topup_actual_usd)
    return {
        "since_status_at": since.isoformat().replace("+00:00", "Z") if since else None,
        "ledger_records": len(rows),
        "status_counts": status_counts,
        "filled_size_usd": round(filled_size_usd, 6),
        "latest_submitted_at": max((str(row.get("submitted_at") or "") for row in rows), default=None),
        "previous_day_pnl_usd": previous_day_pnl,
        "current_day_pnl_usd": current_day_pnl,
        "realized_pnl_delta_usd": (
            round(current_day_pnl - previous_day_pnl, 6)
            if current_day_pnl is not None and previous_day_pnl is not None
            else None
        ),
        "previous_since_topup_actual_usd": previous_since_topup,
        "current_since_topup_actual_usd": current_since_topup,
        "since_topup_actual_delta_usd": (
            round(current_since_topup - previous_since_topup, 6)
            if current_since_topup is not None and previous_since_topup is not None
            else None
        ),
        "basis": "orders.submitted_at strictly after latest STATUS; PnL deltas compare canonical STATUS values",
    }


def _eth5m_scout_cadence(state: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    """Distinguish an intentionally stopped tombstone from a stale live lane."""
    status = str(state.get("status") or "")
    tombstone = state.get("tombstone") if isinstance(state.get("tombstone"), dict) else {}
    generated = _parse_utc_ts(state.get("generated_at"))
    state_age_h = round(max(0.0, (now - generated).total_seconds() / 3600.0), 6) if generated else None
    retired = status.startswith("TOMBSTONED") and tombstone.get("reopen_allowed") is False
    if retired:
        return {
            "status": "NO_ACTIVE_ITEM_TOMBSTONED",
            "time_in_stage_h": 0.0,
            "state_age_h": state_age_h,
            "clock_start": tombstone.get("bound_at"),
            "budget_h": None,
            "breached": False,
            "collector_expected_running": False,
            "evidence": (
                f"{tombstone.get('id')}; reopen_allowed=false; "
                f"final_post_fee_pnl_usd={tombstone.get('final_post_fee_pnl_usd')}"
            ),
        }
    return {
        "status": "ACTIVE_STATE_AGE_REPORTED",
        "time_in_stage_h": state_age_h,
        "state_age_h": state_age_h,
        "clock_start": state.get("generated_at"),
        "budget_h": None,
        "breached": False,
        "collector_expected_running": True,
        "evidence": "active scout state age; no new SLO threshold inferred",
    }


def _is_fable_direction(entry: dict[str, str]) -> bool:
    heading = entry.get("heading", "").lower()
    return "fable" in heading and (
        "direction" in heading
        or "order addendum" in heading
        or re.search(r"\bfable\b.*\borders?\b", heading) is not None
        or re.search(r"\bfable\b.*\baddendum\b", heading) is not None
    )


def _is_operator_order(entry: dict[str, str]) -> bool:
    heading = entry.get("heading", "").lower()
    return (
        "operator order" in heading
        or "operator addendum" in heading
        or "operator pin" in heading
        or "operator handover" in heading
        or "operator preference" in heading
        or "operator correction" in heading
    )


def _latest_fable_direction(entries: list[dict[str, str]]) -> dict[str, str]:
    matching = [entry for entry in entries if _is_fable_direction(entry)]
    timestamped = [
        (entry, _entry_timestamp(entry))
        for entry in matching
    ]
    parsed = [(entry, ts) for entry, ts in timestamped if ts is not None]

    # Long-lived HANDOFF files can contain a locally stamped heading with a
    # literal ``Z``. Once document order is overwhelmingly append-chronological,
    # the final direction is stronger recency evidence than one isolated future
    # timestamp. Keep short/mixed logs on the timestamp rules below.
    if len(parsed) >= 4:
        ascents = sum(
            1 for (_, left), (_, right) in zip(parsed, parsed[1:]) if right >= left
        )
        descents = (len(parsed) - 1) - ascents
        if ascents >= 2 * max(1, descents):
            return parsed[-1][0]

    # HANDOFF roll/incident writers may prepend the newest entries.  Once the
    # first two timestamped DIRECTIONs establish descending document order,
    # that order is the authoritative recency signal.  This also prevents a
    # malformed future timestamp in an older, lower entry from overriding a
    # direction that was subsequently prepended at the top of the live log.
    if len(parsed) >= 2 and parsed[0][1] > parsed[1][1]:
        # A rolled file can later resume append-at-end ordering.  One lower
        # malformed future timestamp is not enough to override the prepended
        # head, but two or more later directions newer than that head prove an
        # append-era continuation.  A newer STATUS followed by a newer
        # appended direction is equivalent proof: brain gateways append their
        # ruling after the STATUS they were asked to audit.
        append_era = [(entry, ts) for entry, ts in parsed[2:] if ts > parsed[0][1]]
        if len(append_era) >= 2:
            return max(append_era, key=lambda pair: pair[1])[0]
        if append_era:
            candidate, candidate_ts = max(append_era, key=lambda pair: pair[1])
            candidate_idx = _entry_index(entries, candidate)
            head_idx = _entry_index(entries, parsed[0][0])
            append_status_proof = any(
                _is_status_entry(entry)
                and head_idx < idx < candidate_idx
                and (status_ts := _entry_timestamp(entry)) is not None
                and parsed[0][1] < status_ts <= candidate_ts
                for idx, entry in enumerate(entries)
            )
            if append_status_proof:
                return candidate
        return parsed[0][0]

    latest: dict[str, str] = {}
    latest_ts: datetime | None = None
    for entry, entry_ts in timestamped:
        if entry_ts is None:
            if not latest:
                latest = entry
            continue
        if latest_ts is None or entry_ts >= latest_ts:
            latest = entry
            latest_ts = entry_ts
    return latest


def _entries_after(entries: list[dict[str, str]], reference: dict[str, str], matcher: Any) -> list[dict[str, str]]:
    reference_idx = _entry_index(entries, reference)
    rows: list[dict[str, str]] = []
    for idx, entry in enumerate(entries):
        if not matcher(entry):
            continue
        if reference_idx >= 0 and idx > reference_idx:
            rows.append(entry)
        elif reference_idx < 0:
            rows.append(entry)
    return rows


def _recent_entries(entries: list[dict[str, str]], matcher: Any, *, limit: int) -> list[dict[str, str]]:
    indexed = [
        (idx, entry)
        for idx, entry in enumerate(entries)
        if matcher(entry)
    ]
    return [entry for _, entry in indexed[-limit:]]


def _entry_index(entries: list[dict[str, str]], target: dict[str, str]) -> int:
    for idx, entry in enumerate(entries):
        if entry is target or (
            entry.get("heading") == target.get("heading") and entry.get("body") == target.get("body")
        ):
            return idx
    return -1


def _bullet_block_by_match(text: str, starts_block: Any) -> list[str]:
    lines = text.splitlines()
    for idx, raw in enumerate(lines):
        if not starts_block(raw):
            continue
        block = [raw.rstrip()]
        for continuation in lines[idx + 1 :]:
            if continuation.startswith("- ") and not starts_block(continuation):
                break
            if continuation.startswith("## "):
                break
            block.append(continuation.rstrip())
        return block
    return []


def _bullet_block(text: str, bullet_prefix: str) -> list[str]:
    return _bullet_block_by_match(text, lambda raw: raw.startswith(bullet_prefix))


def _plain_direction_bullet(raw: str) -> str:
    line = raw.strip()
    line = re.sub(r"^-\s*", "", line)
    line = line.replace("**", "").strip()
    return line


def _is_next_line(raw: str) -> bool:
    line = raw.strip()
    if re.match(r"^-\s*next(?:\s*\([^)]*\))?(?::|\s|$)", line, re.IGNORECASE):
        return True
    if re.match(r"^-\s*directions?\s*:", line, re.IGNORECASE):
        return True
    plain = _plain_direction_bullet(line)
    return bool(re.match(r"^answer\b.{0,120}\bnext\b", plain, re.IGNORECASE))


def _is_direction_next_summary(raw: str) -> bool:
    plain = _plain_direction_bullet(raw).lower()
    if not plain:
        return False
    if "next(" not in plain:
        return False
    return plain.startswith(("direction status", "queue", "ordered", "answer"))


def _is_direction_order_summary(raw: str) -> bool:
    plain = _plain_direction_bullet(raw).lower()
    if not plain:
        return False
    order_phrases = (
        "next action order",
        "next order",
        "action order restated",
        "order restated",
        "sequence stands",
    )
    if not any(phrase in plain for phrase in order_phrases):
        return False
    return plain.startswith(("answer", "direction status", "next", "queue", "ordered"))


def _is_direction_action_line(raw: str) -> bool:
    plain = _plain_direction_bullet(raw).lower()
    if not plain:
        return False
    if plain.startswith(("order ", "order(", "ordered ", "operator order")):
        return True
    if plain.startswith(("budget note", "live unchanged", "milestones stand", "priority", "priorities")):
        return True
    return False


def _direction_action_blocks(text: str, *, limit: int = 32) -> list[str]:
    lines = text.splitlines()
    rows: list[str] = []
    idx = 0
    while idx < len(lines):
        raw = lines[idx]
        if not _is_direction_action_line(raw):
            idx += 1
            continue
        rows.append(raw.rstrip())
        idx += 1
        while idx < len(lines):
            continuation = lines[idx]
            if continuation.startswith("## "):
                break
            if continuation.startswith("- "):
                break
            if continuation.strip():
                rows.append(continuation.rstrip())
            idx += 1
        if len(rows) >= limit:
            return rows[:limit]
    return rows


def _is_direction_queue_heading(raw: str) -> bool:
    stripped = raw.strip()
    if re.match(r"^#{1,6}\s+(?:\d+\.\s+)?", stripped) and re.search(
        r"\bqueue\b", stripped, re.IGNORECASE
    ):
        return True
    return bool(
        re.match(
            r"^(?:#{1,6}\s+(?:\d+\.\s+)?)?-?\s*"
            r"(?:(?:DIRECTION|NEXT|ACTION)\s*(?:\u2014|-|:)\s*(?:THE\s+)?)?"
            r"(?:[A-Z0-9-]+\s+)*QUEUE(?:\s+\([^)]*\))?"
            r"(?:\s+for\s+\w+)?(?::|,|\s|\u2014|$)",
            stripped,
            re.IGNORECASE,
        )
    )


def _direction_next_block(text: str) -> list[str]:
    explicit_next = _bullet_block_by_match(text, _is_next_line)
    if explicit_next:
        return explicit_next
    summary_next = _bullet_block_by_match(text, _is_direction_next_summary)
    if summary_next:
        return summary_next
    ordered_next = _bullet_block_by_match(text, _is_direction_order_summary)
    if ordered_next:
        return ordered_next
    action_next = _direction_action_blocks(text)
    if action_next:
        return action_next
    lines = text.splitlines()
    for idx, raw in enumerate(lines):
        if not _is_direction_queue_heading(raw):
            continue
        block = [raw.rstrip()]
        seen_content = False
        for continuation in lines[idx + 1 :]:
            cont = continuation.rstrip()
            cont_stripped = cont.strip()
            if re.match(r"^#{1,6}\s", cont_stripped):
                break
            if not cont_stripped:
                if seen_content:
                    break
                continue
            if re.match(r"^-?\s*(RULING|FORBIDDEN|konzisztens|CONSISTENT)\b", cont_stripped, re.IGNORECASE):
                break
            block.append(cont)
            seen_content = True
        return block
    next_actions_heading = re.compile(
        r"^#{1,6}\s*(?:\d+\.\s*)?.*(következő akciók|next actions|pontos következő|sorrend)",
        re.IGNORECASE,
    )
    for idx, raw in enumerate(lines):
        if not next_actions_heading.match(raw.strip()):
            continue
        block = [raw.rstrip()]
        seen_content = False
        for continuation in lines[idx + 1 :]:
            cont = continuation.rstrip()
            cont_stripped = cont.strip()
            if re.match(r"^#{1,6}\s", cont_stripped):
                break
            if not cont_stripped:
                if seen_content:
                    break
                continue
            if re.match(
                r"^-?\s*(RULING|FORBIDDEN|konzisztens|CONSISTENT)\b",
                cont_stripped,
                re.IGNORECASE,
            ):
                break
            block.append(cont)
            seen_content = True
        return block
    return []


def _defect_lines(text: str, *, limit: int) -> list[str]:
    rows: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if re.match(r"^- defect(?:\s*\||:)", line):
            rows.append(line)
        if len(rows) >= limit:
            break
    return rows


def _compact_lines(text: str, *, prefix: str, limit: int) -> list[str]:
    rows: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith(prefix):
            rows.append(line)
        if len(rows) >= limit:
            break
    return rows


def _material_direction_lines(text: str, *, limit: int = MATERIAL_DIRECTION_LIMIT) -> list[str]:
    rows: list[str] = []
    prefixes = tuple(prefix.lower() for prefix in MATERIAL_DIRECTION_PREFIXES)
    plain_prefixes = tuple(prefix.removeprefix("- ").lower() for prefix in MATERIAL_DIRECTION_PREFIXES)
    amendment_block = False
    ordered_next_block = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            amendment_block = False
            ordered_next_block = False
            continue
        plain = _plain_direction_bullet(line).lower()
        if (
            line.lower().startswith(prefixes)
            or plain.startswith(plain_prefixes)
            or _is_next_line(line)
            or _is_direction_next_summary(line)
            or _is_direction_order_summary(line)
            or (amendment_block and re.match(r"^\d+\.\s+", line))
            or (ordered_next_block and re.match(r"^\d+\.\s+", line))
        ):
            rows.append(line)
            if "amendment" in plain:
                amendment_block = True
            if _is_direction_order_summary(line):
                ordered_next_block = True
        elif amendment_block and raw.startswith((" ", "\t")):
            continue
        elif ordered_next_block and raw.startswith((" ", "\t")):
            continue
        elif not re.match(r"^\d+\.\s+", line):
            amendment_block = False
            ordered_next_block = False
        if len(rows) >= limit:
            break
    return rows


def _top_level_direction_bullets(
    text: str,
    *,
    limit: int = MATERIAL_DIRECTION_FALLBACK_LIMIT,
) -> list[str]:
    rows: list[str] = []
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.startswith("## "):
            break
        if not line.startswith("- "):
            continue
        rows.append(line)
        if len(rows) >= limit:
            break
    return rows


def _top_level_entry_bullet_blocks(text: str, *, limit: int = 3) -> list[str]:
    """Return top-level bullets with their wrapped continuation lines intact."""
    rows: list[str] = []
    current: list[str] = []
    for raw in text.splitlines():
        if raw.startswith("- "):
            if current:
                rows.append(" ".join(current))
                if len(rows) >= limit:
                    return rows
            current = [raw.strip()]
        elif current and raw.startswith((" ", "\t")) and raw.strip():
            current.append(raw.strip())
        elif current and not raw.strip():
            rows.append(" ".join(current))
            current = []
            if len(rows) >= limit:
                return rows
    if current and len(rows) < limit:
        rows.append(" ".join(current))
    return rows


def _direction_material_with_basis(text: str) -> dict[str, Any]:
    material = _material_direction_lines(text)
    if material:
        return {"lines": material, "extraction_basis": "material_prefix"}
    fallback = _top_level_direction_bullets(text)
    if fallback:
        return {"lines": fallback, "extraction_basis": "verbatim_fallback"}
    return {"lines": [], "extraction_basis": "empty"}


def _short_wallet(value: str) -> str:
    value = str(value or "")
    if len(value) <= 14:
        return value
    return f"{value[:6]}...{value[-4:]}"


def _latest_mechanical_demotion_cooloff(digest: dict[str, Any]) -> Any:
    demotion = digest["active_set"]["latest_mechanical_temporal_loss_demotion"]
    cooloffs = digest["order_flow_deadman"].get("policy_choke_rung_b_cooloffs", {})
    if not isinstance(cooloffs, dict):
        return None
    wallet = str(demotion.get("target_wallet") or "").lower()
    fingerprint = str(demotion.get("wide_policy_fingerprint") or "").lower()
    cooloff = cooloffs.get(f"{wallet}|{fingerprint}") if wallet and fingerprint else None
    if cooloff is None:
        cooloff = cooloffs.get(wallet)
    if isinstance(cooloff, dict):
        return cooloff.get("expires_at")
    return cooloff


def _norm_wallet(value: Any) -> str:
    value = str(value or "").strip().lower()
    return value if value.startswith("0x") and len(value) == 42 else ""


def _market_cohort_shadow_accrual(
    queue: dict[str, Any],
    realtime_shadow: dict[str, Any],
    guard_shadow: dict[str, Any],
) -> dict[str, Any]:
    members = queue.get("ranked_members") if isinstance(queue.get("ranked_members"), list) else []
    rows = [
        row
        for row in members
        if isinstance(row, dict)
        and (
            row.get("queue_source") == "market_cohort_replay"
            or row.get("bench_tier") == "market_cohort_shadow_accrual"
        )
        and _norm_wallet(row.get("wallet"))
    ]
    wallets = [_norm_wallet(row.get("wallet")) for row in rows]
    wallet_set = set(wallets)
    realtime_summary = (
        realtime_shadow.get("summary") if isinstance(realtime_shadow.get("summary"), dict) else {}
    )
    realtime_windows = (
        realtime_summary.get("realtime_taker_distinct_market_windows_by_wallet")
        if isinstance(realtime_summary.get("realtime_taker_distinct_market_windows_by_wallet"), dict)
        else {}
    )
    realtime_moved = {
        wallet
        for wallet in wallet_set
        if _as_int(realtime_windows.get(wallet), default=0) > 0
    }
    guard_rows = guard_shadow.get("rows") if isinstance(guard_shadow.get("rows"), list) else []
    guard_moved: set[str] = set()
    for row in guard_rows:
        if not isinstance(row, dict):
            continue
        candidates = [
            row.get("source_wallet"),
            (row.get("source_intent") or {}).get("source_wallet")
            if isinstance(row.get("source_intent"), dict)
            else None,
        ]
        guard_moved.update(wallet for wallet in map(_norm_wallet, candidates) if wallet in wallet_set)
    moved = realtime_moved | guard_moved
    active_recent = {
        _norm_wallet(row.get("wallet"))
        for row in rows
        if _as_float(row.get("days_since_last_trade")) is not None
        and float(row.get("days_since_last_trade")) <= 7.0
    }
    active_recent_moved = moved & active_recent
    liveness_counts: dict[str, int] = {}
    tier_counts: dict[str, int] = {}
    for row in rows:
        liveness = row.get("bench_liveness") if isinstance(row.get("bench_liveness"), dict) else {}
        status = str(liveness.get("status") or "UNKNOWN")
        liveness_counts[status] = liveness_counts.get(status, 0) + 1
        tier = str(row.get("bench_tier") or "unknown")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    return {
        "seated_count": len(rows),
        "movement_wallet_count": len(moved),
        "movement_status": "MOVING" if moved else "NO_MOVEMENT_YET",
        "active_recent_count": len(active_recent),
        "active_recent_days": 7,
        "active_recent_movement_wallet_count": len(active_recent_moved),
        "active_recent_movement_status": "MOVING" if active_recent_moved else "NO_ACTIVE_RECENT_MOVEMENT_YET",
        "realtime_shadow_wallet_count": len(realtime_moved),
        "realtime_shadow_windows_sum": sum(_as_int(realtime_windows.get(wallet), default=0) for wallet in wallet_set),
        "guard_shadow_wallet_count": len(guard_moved),
        "bench_liveness_status_counts": liveness_counts,
        "bench_tier_counts": tier_counts,
        "sample_first9": [
            {
                "rank": row.get("queue_rank"),
                "wallet": row.get("wallet"),
                "bench_tier": row.get("bench_tier"),
                "bench_liveness": (
                    row.get("bench_liveness", {}).get("status")
                    if isinstance(row.get("bench_liveness"), dict)
                    else None
                ),
                "realtime_windows": _as_int(realtime_windows.get(_norm_wallet(row.get("wallet"))), default=0),
                "guard_shadow_seen": _norm_wallet(row.get("wallet")) in guard_moved,
                "days_since_last_trade": row.get("days_since_last_trade"),
            }
            for row in rows[:9]
        ],
    }


def _rolling20_row_label(row: dict[str, Any]) -> str:
    return (
        f"{_short_wallet(str(row.get('wallet') or ''))}:"
        f"n{row.get('rolling20_n')}:"
        f"pnl{row.get('rolling20_pnl_usd')}"
    )


def _live_execution_probe_summaries(data_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(data_dir.glob("wallet_copy_live_execution_probe_*.json")):
        payload = _load_json(path, {})
        if not isinstance(payload, dict):
            continue
        summary = (
            payload.get("candidate_intent_summary")
            if isinstance(payload.get("candidate_intent_summary"), dict)
            else {}
        )
        prefilter = (
            summary.get("live_event_prefilter")
            if isinstance(summary.get("live_event_prefilter"), dict)
            else {}
        )
        latest = (
            prefilter.get("latest_source_event_runtime")
            if isinstance(prefilter.get("latest_source_event_runtime"), dict)
            else {}
        )
        rows.append(
            {
                "path": str(path),
                "label": path.stem.removeprefix("wallet_copy_live_execution_probe_"),
                "status": payload.get("status"),
                "candidate_id": payload.get("candidate_id"),
                "source_wallet": payload.get("source_wallet"),
                "orders_submitted": payload.get("orders_submitted"),
                "fresh_candidate_intents": summary.get("fresh_candidate_intents"),
                "fresh_candidate_intents_after_toxicity_protection": summary.get(
                    "fresh_candidate_intents_after_toxicity_protection"
                ),
                "source_events": summary.get("source_events"),
                "skip_counts": prefilter.get("skip_counts") if isinstance(prefilter.get("skip_counts"), dict) else {},
                "min_live_floor_pin_enabled": prefilter.get("min_live_floor_pin_enabled"),
                "min_live_floor_pin_direction_id": prefilter.get("min_live_floor_pin_direction_id"),
                "latest_market_slug": latest.get("market_slug"),
                "latest_btc_5m_scope_ok": latest.get("btc_5m_scope_ok"),
                "latest_market_closed_now": latest.get("market_closed_now"),
                "latest_event_age_s": latest.get("event_age_s"),
                "latest_observed_age_s": latest.get("observed_age_s"),
            }
        )
    return rows


def _edge_stat(report: dict[str, Any], horizon_key: str, field: str, stat: str) -> Any:
    alpha_decay = report.get("alpha_decay") if isinstance(report.get("alpha_decay"), dict) else report
    alpha_decay = alpha_decay if isinstance(alpha_decay, dict) else {}
    coverage = alpha_decay.get("coverage_by_horizon") if isinstance(alpha_decay.get("coverage_by_horizon"), dict) else {}
    horizon = coverage.get(horizon_key) if isinstance(coverage.get(horizon_key), dict) else {}
    bucket = horizon.get(field) if isinstance(horizon.get(field), dict) else {}
    return bucket.get(stat)


def _alpha_decay_rule(report: dict[str, Any]) -> dict[str, Any]:
    alpha_decay = report.get("alpha_decay") if isinstance(report.get("alpha_decay"), dict) else report
    alpha_decay = alpha_decay if isinstance(alpha_decay, dict) else {}
    coverage = alpha_decay.get("coverage_by_horizon") if isinstance(alpha_decay.get("coverage_by_horizon"), dict) else {}
    five_s = coverage.get("5s") if isinstance(coverage.get("5s"), dict) else {}
    edge = five_s.get("edge") if isinstance(five_s.get("edge"), dict) else {}
    mean = edge.get("mean")
    n = int(five_s.get("coverage") or 0)
    threshold = 0.015
    if n < 200:
        verdict = "PENDING_SAMPLE"
    elif mean is not None and float(mean) >= threshold:
        verdict = "FAST_PATH_BUILD_ORDER_1"
    else:
        verdict = "TAKER_COPY_EDGE_BELOW_5S_RULE"
    return {
        "horizon_key": "5s",
        "n": n,
        "mean_edge": mean,
        "median_edge": edge.get("p50"),
        "threshold_mean_edge": threshold,
        "verdict": verdict,
    }


def _walk_forward_refusal_frontier(
    current_actuator: dict[str, Any],
    previous_deadman: dict[str, Any],
    *,
    generated_at: str,
) -> dict[str, Any]:
    refusals = (
        current_actuator.get("refusal_counts")
        if isinstance(current_actuator.get("refusal_counts"), dict)
        else {}
    )
    candidate_evidence = (
        current_actuator.get("candidate_evidence")
        if isinstance(current_actuator.get("candidate_evidence"), dict)
        else {}
    )
    candidate_pool = int(candidate_evidence.get("candidate_count") or 0)
    refusal_count = int(refusals.get("f1_walk_forward_admissible") or 0)
    previous_frontier = (
        previous_deadman.get("walk_forward_refusal_frontier")
        if isinstance(previous_deadman.get("walk_forward_refusal_frontier"), dict)
        else {}
    )
    previous_high_water = max(
        WALK_FORWARD_POOL_HIGH_WATER_SEED,
        int(previous_frontier.get("candidate_pool_high_water") or 0),
    )
    candidate_pool_high_water = max(previous_high_water, candidate_pool)
    first_observed_at = (
        generated_at
        if candidate_pool > previous_high_water
        else str(
            previous_frontier.get("candidate_pool_high_water_first_observed_at")
            or WALK_FORWARD_POOL_HIGH_WATER_FIRST_OBSERVED_AT
        )
    )
    delta_from_high_water = candidate_pool - candidate_pool_high_water
    return {
        "refusal_count": refusal_count,
        "candidate_pool": candidate_pool,
        "candidate_pool_high_water": candidate_pool_high_water,
        "candidate_pool_high_water_first_observed_at": first_observed_at,
        "candidate_pool_delta_from_high_water": delta_from_high_water,
        "display": (
            f"{refusal_count}/{candidate_pool} "
            f"(pool {candidate_pool_high_water}->{candidate_pool}, "
            f"delta {delta_from_high_water:+d})"
        ),
    }


def build_digest(root: Path, *, codex_dir: Path | None = None) -> tuple[dict[str, Any], str]:
    data_dir = root / "data" / "research"
    same_window_capture = _latest_same_window_capture_summary(data_dir)
    e1_audit_inputs, e1_audit_inputs_path = _load_latest_json_artifact(
        data_dir, "e1_framework_audit_inputs_*.json"
    )
    positive_wallet_slice_falsifier = _load_json(
        data_dir / "positive_wallet_slice_selector_falsifier_latest.json", {}
    )
    two_arm_concentration = _load_json(
        data_dir / "two_arm_concentration_decomposition_latest.json", {}
    )
    participation_today_path = (
        data_dir
        / f"btc5m_288_participation_map_{datetime.now(timezone.utc).date().isoformat()}.json"
    )
    if participation_today_path.exists():
        participation_288_map = _load_json(participation_today_path, {})
        participation_288_map_path: Path | None = participation_today_path
    else:
        participation_288_map, participation_288_map_path = _load_latest_json_artifact(
            data_dir, "btc5m_288_participation_map_*.json"
        )
    structural_scalp_refresh = _refresh_btc5m_structural_scalp_lane(root)
    handoff_entries = _handoff_entries(root / "docs" / "agents" / "HANDOFF.md")
    latest_handoff = _latest_handoff_entry(handoff_entries)
    latest_direction = _latest_fable_direction(handoff_entries)
    handoff_restart_scope_start_at = _latest_post_restart_scope_start_from_handoff(handoff_entries)
    latest_operator_order = _latest_matching_entry(handoff_entries, _is_operator_order)
    latest_operator_order_material = _top_level_entry_bullet_blocks(
        latest_operator_order.get("body", "")
    )
    latest_status = _latest_status_entry(handoff_entries)
    newer_directions = [
        entry
        for entry in _entries_after(handoff_entries, latest_status, _is_fable_direction)
        if entry != latest_direction
    ]
    recent_directions = _recent_entries(
        handoff_entries,
        _is_fable_direction,
        limit=RECENT_DIRECTION_LIMIT,
    )
    guard, live, live_read_consistency = _load_confirmed_guard_live_pair(data_dir)
    e1_reject_cluster = (
        e1_audit_inputs.get("reject_cluster")
        if isinstance(e1_audit_inputs.get("reject_cluster"), dict)
        else {}
    )
    e1_artifact_ledger_cut = _parse_utc_ts(
        e1_reject_cluster.get("ledger_newest_submitted_at")
    )
    e1_live_ledger_cut = _latest_live_submitted_at(live)
    e1_deadman = _load_json(data_dir / "order_flow_deadman_state.json", {})
    e1_live_ledger_cut_frozen_by = (
        "ORDER_FLOW_DEAD"
        if e1_live_ledger_cut
        and e1_deadman.get("can_trade") is False
        and str(e1_deadman.get("status") or "").startswith("INCIDENT_")
        else None
    )
    e1_artifact_stale = bool(
        e1_live_ledger_cut
        and (
            e1_artifact_ledger_cut is None
            or e1_live_ledger_cut > e1_artifact_ledger_cut
        )
    )
    e1_ledger_lag_s = (
        round((e1_live_ledger_cut - e1_artifact_ledger_cut).total_seconds(), 6)
        if e1_live_ledger_cut and e1_artifact_ledger_cut
        else None
    )
    intent_time_copyability_proof = _load_json(
        data_dir / "wallet_copy_intent_time_copyability_proof_state.json",
        {},
    )
    intent_time_copyability_proof_summary = (
        intent_time_copyability_proof.get("summary")
        if isinstance(intent_time_copyability_proof.get("summary"), dict)
        else {}
    )
    guard_processes = _guard_process_snapshot(root)
    guard_caps = _guard_process_caps(guard_processes)
    working_tree_material = _material_working_tree_snapshot(root)
    polygon_ws_shadow = _load_json(data_dir / "polygon_ws_shadow_service_state.json", {})
    realtime_shadow = _load_json(data_dir / "wallet_copy_realtime_shadow_watch_scored_state.json", {})
    guard_shadow = _load_json(data_dir / "wallet_copy_guard_shadow_lanes_state.json", {})
    queue = _load_json(data_dir / "wallet_copy_full_pool_member_queue.json", {})
    fresh_flow_probe = _load_json(data_dir / "queue_remote_dataapi_fresh_flow_probe_latest.json", {})
    order136d_early_entry = _load_json(
        data_dir / "order136d_early_entry_ranking_latest.json", {}
    )
    targeted_copyability_probe = _load_json(
        data_dir / "fable_0410_targeted_corrected_copyability_probe.json", {}
    )
    e5 = _load_json(data_dir / "maker_first_btc5m_paper_state.json", {})
    e5_signal_gated = _load_json(data_dir / "maker_first_btc5m_signal_gated_paper_state.json", {})
    e5_signal_gated_book = _load_json(data_dir / "maker_first_btc5m_signal_gated_book_aware_state.json", {})
    e5_book_aware = _load_json(data_dir / "maker_first_btc5m_book_aware_state.json", {})
    e5_review = _load_json(data_dir / "e5_review_split_latest.json", {})
    e5_5share_regrade = _load_json(data_dir / "e5_maker_first_5share_regrade_latest.json", {})
    e5_live_actuator = _load_json(data_dir / "e5_maker_first_live_actuator_latest.json", {})
    e5_divergence = _load_json(data_dir / "e5_paper_live_divergence_latest.json", {})
    e11 = _load_json(data_dir / "e11_cross_window_momentum_book_aware_state.json", {})
    e11_book = _load_json(data_dir / "e11_cross_window_momentum_book_aware_book_state.json", {})
    e7 = _load_json(data_dir / "btc5m_late_window_penny_watcher_state.json", {})
    brainless = _load_json(data_dir / "brainless_ops_latest.json", {})
    boot_recovery = _load_json(data_dir / "boot_recovery_audit_state.json", {})
    live_guard_restart = _load_json(
        data_dir / "brainless_live_guard_restart_state.json", {}
    )
    guard_generation_delta = _load_json(
        data_dir / "guard_generation_delta_latest.json", {}
    )
    live_guard_restart_decision = (
        live_guard_restart.get("latest_decision")
        if isinstance(live_guard_restart.get("latest_decision"), dict)
        else live_guard_restart
    )
    live_guard_restart_execution = (
        live_guard_restart_decision.get("execution")
        if isinstance(live_guard_restart_decision.get("execution"), dict)
        else {}
    )
    live_guard_restart_preflight = (
        live_guard_restart_decision.get("feed_health_preflight")
        if isinstance(live_guard_restart_decision.get("feed_health_preflight"), dict)
        else {}
    )
    live_guard_restart_sweep = (
        live_guard_restart_execution.get("quiescent_remnant_sweep")
        if isinstance(live_guard_restart_execution.get("quiescent_remnant_sweep"), dict)
        else {}
    )
    latest_executed_restart = next(
        (
            row
            for row in reversed(
                _tail_jsonl_dicts(
                    data_dir / "brainless_live_guard_restart_events.jsonl",
                    max_bytes=2_000_000,
                    max_rows=200,
                )
            )
            if row.get("status") == "RESTART_EXECUTED"
        ),
        live_guard_restart_decision
        if live_guard_restart_decision.get("status") == "RESTART_EXECUTED"
        else {},
    )
    latest_executed_restart_execution = (
        latest_executed_restart.get("execution")
        if isinstance(latest_executed_restart.get("execution"), dict)
        else {}
    )
    latest_executed_restart_preflight = (
        latest_executed_restart.get("feed_health_preflight")
        if isinstance(latest_executed_restart.get("feed_health_preflight"), dict)
        else {}
    )
    latest_executed_restart_sweep = (
        latest_executed_restart_execution.get("quiescent_remnant_sweep")
        if isinstance(
            latest_executed_restart_execution.get("quiescent_remnant_sweep"), dict
        )
        else {}
    )
    market_mining = _load_json(data_dir / "wallet_market_mining_cadence_state.json", {})
    leaderboard_scan = _load_json(data_dir / "wallet_copy_leaderboard_scan_state.json", {})
    leaderboard_scan_progress = _load_json(
        data_dir / "wallet_copy_leaderboard_scan_progress.json", {}
    )
    leaderboard_pipeline = _load_json(data_dir / "wallet_copy_leaderboard_crypto_state.json", {})
    leaderboard_history_command = next(
        (
            row
            for row in (leaderboard_pipeline.get("command_results") or [])
            if isinstance(row, dict) and row.get("name") == "history_and_paper"
        ),
        {},
    )
    leaderboard_history_stdout = (
        leaderboard_history_command.get("stdout_json")
        if isinstance(leaderboard_history_command.get("stdout_json"), dict)
        else {}
    )
    cohort_admission = _load_json(data_dir / "cohort_alive_admission_packets_latest.json", {})
    registry_liveness = _load_json(data_dir / "registry_weekday_f1_remote_liveness_probe_latest.json", {})
    registry_observation_admissions = _load_json(
        root / "configs/wallet_copy/registry_observation_admissions.json", {}
    )
    watch_tier_poller = _load_json(data_dir / "wallet_copy_watch_tier_poller_state.json", {})
    registry_observation_wallets = {
        str(wallet).lower()
        for wallet in (
            registry_observation_admissions.get("wallets")
            if isinstance(registry_observation_admissions.get("wallets"), list)
            else []
        )
        if wallet
    }
    watch_tier_source_wallets = (
        watch_tier_poller.get("source_wallets")
        if isinstance(watch_tier_poller.get("source_wallets"), list)
        else []
    )
    watch_tier_source_wallet_cohorts = [
        {
            "wallet": str(wallet).lower(),
            "cohort": "registry_observation"
            if str(wallet).lower() in registry_observation_wallets
            else "legacy_watch_tier",
        }
        for wallet in watch_tier_source_wallets
        if wallet
    ]
    admitted_watch_tier_polled_count = sum(
        1 for row in watch_tier_source_wallet_cohorts if row.get("cohort") == "registry_observation"
    )
    research_lane_cadence = _load_json(data_dir / "research_lane_cadence_latest.json", {})
    factory_funnel = _load_json(data_dir / "factory_funnel_latest.json", {})
    cli_versions = _load_json(data_dir / "cli_versions_state.json", {})
    agy_fallback_smoke = _load_json(data_dir / "agy_fallback_smoke_latest.json", {})
    agy_quota_state = _load_json(data_dir / "agy_quota_state.json", {})
    runtime_speed = _load_json(data_dir / "runtime_speed_baseline_latest.json", {})
    previous_digest = _load_json(data_dir / "state_digest.json", {})
    scorecard_runtime_evidence = _scorecard_runtime_evidence(data_dir)
    guard_json_cache_evidence = _load_json(data_dir / "guard_memory_json_cache_evidence_latest.json", {})
    window_participation_merge_profile = _load_json(
        data_dir / "window_participation_merge_profile_latest.json",
        {},
    )
    order_flow_deadman = _load_json(data_dir / "order_flow_deadman_state.json", {})
    deadman_admission_publish_health = _deadman_admission_publish_health(
        data_dir / "order_flow_deadman_admission_intervals.jsonl"
    )
    operator_notification_discipline = _load_json(
        data_dir / "operator_notification_discipline_state.json",
        {},
    )
    latest_forced_sweep: dict[str, Any] = {}
    for incident in reversed(
        tail_incident_rows(
            data_dir / "order_flow_deadman_incidents.jsonl",
            max_rows=20,
        )
    ):
        incident_policy_choke = (
            incident.get("policy_choke")
            if isinstance(incident.get("policy_choke"), dict)
            else {}
        )
        incident_actuator = (
            incident_policy_choke.get("actuator")
            if isinstance(incident_policy_choke.get("actuator"), dict)
            else {}
        )
        if incident_actuator.get("status") == "RUNG_C_NO_ADMISSIBLE_TARGET":
            latest_forced_sweep = {
                "checked_at": incident.get("checked_at"),
                "status": incident_actuator.get("status"),
                "reason": incident_actuator.get("reason"),
                "refusal_counts": incident_actuator.get("refusal_counts"),
                "quality_bars_unchanged": incident_actuator.get(
                    "quality_bars_unchanged"
                ),
            }
            break
    active_member_orderfilled_hot_source_shadow = _load_json(
        data_dir / "active_member_orderfilled_hot_source_shadow_state.json", {}
    )
    early_01a_decision_time_book = _load_json(
        data_dir / "early_01a_decision_time_book_latest.json", {}
    )
    qualified_pool_orderfilled_stakeout = _load_json(
        data_dir / "copy_qualified_pool_orderfilled_resident_stakeout_state.json", {}
    )
    copy_source_identity_reconciliation = _load_json(
        data_dir / "copy_source_identity_reconciliation_router_latest.json", {}
    )
    copy_source_wake_activation = _load_json(
        data_dir / "copy_source_wake_activation_latest.json", {}
    )
    orderfilled_fast_lane = _load_json(
        data_dir / "wallet_copy_orderfilled_fast_lane_state.json", {}
    )
    realized_fee_receipts = _load_json(data_dir / "wallet_copy_realized_fee_receipts_latest.json", {})
    regime_seat_selection = _load_json(data_dir / "regime_seat_selection_latest.json", {})
    coacceptance_shadow = _load_json(data_dir / "coacceptance_eligibility_shadow_latest.json", {})
    probe_fill_quality_shadows = _load_json(data_dir / "probe_fill_quality_shadows_latest.json", {})
    monday_return_proof_path = data_dir / "monday_return_synthetic_proof_latest.json"
    monday_return_proof = _load_json(monday_return_proof_path, {})
    if not isinstance(monday_return_proof, dict) or not monday_return_proof:
        monday_return_proof_path = data_dir / "monday_return_synthetic_proof_20260713.json"
        monday_return_proof = _load_json(monday_return_proof_path, {})
    own_positions = _load_json(data_dir / "own_positions_latest.json", {})
    own_position_deadman = _load_json(data_dir / "own_position_deadman_state.json", {})
    own_redeemer = _load_json(data_dir / "own_redeemer_state.json", {})
    wallet_outflow_deadman = _load_json(data_dir / "wallet_outflow_deadman_state.json", {})
    research_disk_deadman = _load_json(data_dir / "research_disk_deadman_state.json", {})
    repo_storage_hygiene = _load_json(data_dir / "repo_storage_hygiene_latest.json", {})
    guard_event_log_rotation = _load_json(data_dir / "wallet_copy_guard_event_log_rotation_state.json", {})
    alpha_decay_curve = _load_json(data_dir / "alpha_decay_curve_study_latest.json", {})
    overlap_capture_label = "com.polymarket.alpha-overlap-13e0-f418-a689"
    overlap_polygon_candidates = sorted(
        data_dir.glob("polygon_orderfilled_alpha_overlap_13e0_f418_a689_*T*.jsonl"),
        key=lambda path: (path.stat().st_mtime, path.name),
    )
    overlap_polygon_path = overlap_polygon_candidates[-1] if overlap_polygon_candidates else None
    overlap_run_suffix = (
        overlap_polygon_path.stem.removeprefix("polygon_orderfilled_alpha_overlap_13e0_f418_a689_")
        if overlap_polygon_path is not None
        else None
    )
    overlap_clob_path = (
        data_dir / f"clob_books_alpha_overlap_13e0_f418_a689_{overlap_run_suffix}.jsonl"
        if overlap_run_suffix
        else None
    )
    overlap_report_path = data_dir / "alpha_decay_13e0_f418_a689_overlap_latest.json"
    overlap_state_path = data_dir / "alpha_decay_13e0_f418_a689_overlap_state.json"
    overlap_registry_path = data_dir / "alpha_decay_13e0_f418_a689_registry.json"
    overlap_registry = _load_json(overlap_registry_path, {})
    overlap_capture_pid = _launchctl_job_pid(overlap_capture_label)
    e6db_loser_autopsy = _load_json(data_dir / "e6db_loser_autopsy_latest.json", {})
    successor_dossier = _load_json(data_dir / "successor_dossier_latest.json", {})
    active_set_rotation_packet = _load_json(data_dir / "active_set_rotation_packet_latest.json", {})
    active_set_post_rotation_windows = _load_json(data_dir / "active_set_post_rotation_windows_latest.json", {})
    active_set_pin_consumer_sweep = _load_json(data_dir / "active_set_pin_consumer_sweep_latest.json", {})
    alpha_decay_curve_path = data_dir / "alpha_decay_curve_study_latest.json"
    if not isinstance(alpha_decay_curve, dict) or not alpha_decay_curve:
        alpha_decay_curve = _load_json(data_dir / "alpha_decay_report.json", {})
        alpha_decay_curve_path = data_dir / "alpha_decay_report.json"
    order_flow_guard_memory = (
        order_flow_deadman.get("guard_memory")
        if isinstance(order_flow_deadman.get("guard_memory"), dict)
        else {}
    )
    order_flow_guard_memory_auto_restart = (
        order_flow_guard_memory.get("auto_restart")
        if isinstance(order_flow_guard_memory.get("auto_restart"), dict)
        else {}
    )
    order_flow_guard_memory_samples = (
        order_flow_guard_memory.get("samples")
        if isinstance(order_flow_guard_memory.get("samples"), list)
        else []
    )
    member_factory = _load_json(data_dir / "member_factory_kpi_state.json", {})
    enabled_overflow_proposal = _latest_enabled_overflow_proposal(data_dir)
    watcher_gap = _load_json(data_dir / "wallet_copy_no_copy_signal_watcher_gap_latest.json", {})
    active_set_poller = _load_json(data_dir / "wallet_copy_active_set_dataapi_poller_state.json", {})
    live_execution_probes = _live_execution_probe_summaries(data_dir)
    active_set_overlay = _load_json(data_dir / "wallet_copy_active_set_auto_degrade_state.json", {})
    current_admission_wave = _load_json(data_dir / "current_admission_wave_latest.json", {})
    frozen_fingerprint_f2_prewarm = _load_json(
        data_dir / "frozen_fingerprint_f2_prewarm_backup_rank_shadow_latest.json",
        {},
    )
    copy_freeze_sidecar = _load_json(
        data_dir / "copy_freeze_near_bar_allpass_dryrun_sidecar_latest.json",
        {},
    )
    freeze_resolution_accelerator = _load_json(
        data_dir / "freeze_resolution_accelerator_state.json",
        {},
    )
    wide_direct_admissible_frontier = _load_json(
        data_dir / "wide_direct_admissible_frontier_latest.json",
        {},
    )
    order147_seat_feedstock = _load_json(
        data_dir / "order147_seat_feedstock_divergence_latest.json",
        {},
    )
    order148_seated_fill_dispositions = _load_json(
        data_dir / "order148_seated_fill_dispositions_latest.json",
        {},
    )
    order149_rotation_qualification = _load_json(
        data_dir / "order149_rotation_qualification_latest.json",
        {},
    )
    order149_token_metadata_backfill = _load_json(
        data_dir / "order149_token_metadata_backfill_latest.json",
        {},
    )
    order149_gamma_metadata_recovery = _load_json(
        data_dir / "order149_gamma_metadata_recovery_latest.json",
        {},
    )
    order149_depth_at_size = _load_json(
        data_dir / "order149_depth_at_size_latest.json",
        {},
    )
    order150_window_supply_attribution = _load_json(
        data_dir / "order150_window_supply_attribution_latest.json",
        {},
    )
    order150_joint_supply_size_projection = _load_json(
        data_dir / "order150_joint_supply_size_projection_latest.json",
        {},
    )
    order151_bf337_fading_adjudication = _load_json(
        data_dir / "order151_bf337_fading_adjudication_latest.json", {},
    )
    order151_fee_bps = _load_json(data_dir / "order151_fee_bps_latest.json", {})
    wide_all_pass_seat_path = _load_json(
        data_dir / "wide_all_pass_seat_path_latest.json",
        {},
    )
    wide_ee3f_venue_diagnosis = _load_json(
        data_dir / "wide_ee3f_venue_reachable_diagnosis_latest.json",
        {},
    )
    wide_951b_concentration_diagnosis = _load_json(
        data_dir / "wide_951b_concentration_diagnosis_latest.json",
        {},
    )
    wide_order7a_alpha_causal_reanchor = _load_json(
        data_dir / "wide_order7a_alpha_causal_reanchor_latest.json",
        {},
    )
    wide_order7b_metadata_diagnosis = _load_json(
        data_dir / "wide_order7b_metadata_diagnosis_latest.json",
        {},
    )
    order127_measured_seat_distance = _load_json(
        data_dir / "order127_measured_seat_distance_latest.json",
        {},
    )
    order128_fastest_lawful_path = _load_json(
        data_dir / "order128_fastest_lawful_path_latest.json",
        {},
    )
    wide_manifest_pointer = _load_json(
        data_dir / "wide_exact_policy_manifest_active.json",
        {},
    )
    wide_manifest_path_value = str(
        wide_manifest_pointer.get("manifest_path") or ""
    )
    wide_manifest_path = Path(wide_manifest_path_value)
    if wide_manifest_path_value and not wide_manifest_path.is_absolute():
        wide_manifest_path = root / wide_manifest_path
    wide_manifest = (
        _load_json(wide_manifest_path, {})
        if wide_manifest_path_value
        else {}
    )
    order128_manifest_binding = _order128_manifest_binding(
        wide_manifest_pointer,
        wide_manifest,
        order128_fastest_lawful_path,
    )
    wide_order6_gen2_retarget = _load_json(
        data_dir / "wide_order6_gen2_retarget_latest.json",
        {},
    )
    wide_wallet_terminal_breakdown = _load_json(
        data_dir / "82c8_wide_terminal_breakdown_latest.json",
        {},
    )
    resolved_tape_gap_closure = _load_json(
        data_dir / "82c8_resolved_tape_gap_closure_latest.json",
        {},
    )
    wave_gate_attribution, wave_gate_attribution_path = _load_latest_json_artifact(
        data_dir,
        "wave_gate_attribution_*.json",
    )
    admitted_member_gate_3048, admitted_member_gate_3048_path = _load_latest_json_artifact(
        data_dir,
        "admitted_member_gate_3048_*.json",
    )
    wave_repair_addendum, wave_repair_addendum_path = _load_latest_json_artifact(
        data_dir,
        "wave_repair_attribution_addendum_*.json",
    )
    order_flow_deadman_r1_attribution, order_flow_deadman_r1_attribution_path = _load_latest_json_artifact(
        data_dir,
        "order_flow_deadman_r1_attribution_*.json",
    )
    trade_executor_lane_attribution, trade_executor_lane_attribution_path = _load_latest_json_artifact(
        data_dir,
        "trade_executor_lane_attribution_*.json",
    )
    self_feed = _load_json(data_dir / "wallet_copy_self_feed_vs_ledger_latest.json", {})
    cash_ledger = _load_json(data_dir / "wallet_copy_recon_window_cash_ledger_latest.json", {})
    self_feed_trace = _load_json(data_dir / "wallet_copy_self_feed_missing_trace_latest.json", {})
    self_feed_full_retrace = _load_json(data_dir / "wallet_copy_self_feed_full_ledger_retrace_latest.json", {})
    self_feed_duckdb_benchmark = _load_json(
        data_dir / "wallet_copy_self_feed_duckdb_benchmark_latest.json", {}
    )
    h2_external_redemptions = _load_json(data_dir / "h2_external_redemption_ingestion_latest.json", {})
    h2_account_value_residual = _load_json(
        data_dir / "h2_account_value_residual_reconstruction_latest.json", {}
    )
    residual_cash_diff_audit = _load_json(data_dir / "wallet_copy_residual_cash_diff_audit_latest.json", {})
    scorecard_same_cut_basis = _load_json(
        data_dir / "wallet_copy_scorecard_same_cut_basis_check_latest.json", {}
    )
    post_panic_integrity = _load_json(data_dir / "post_panic_integrity_audit_latest.json", {})
    scheduler_ratchet_path = data_dir / "copy_event_triggered_cycle_scheduler_paper_lane_latest.json"
    scheduler_ratchet = _load_json(scheduler_ratchet_path, {})
    scheduler_verdict = _load_json(data_dir / "copy_event_triggered_cycle_scheduler_verdict_latest.json", {})
    scheduler_stratification = _load_json(
        data_dir / "copy_event_triggered_cycle_scheduler_stratification_latest.json",
        {},
    )
    scheduler_retirement = _load_json(
        data_dir / "copy_event_triggered_cycle_scheduler_retirement_latest.json",
        {},
    )
    pinned_tranche_economics = _load_json(
        data_dir / "wallet_copy_pinned_tranche_economics_latest.json", {}
    )
    pinned_tranche_midday_due_check = _load_json(
        data_dir / "wallet_copy_pinned_tranche_midday_due_check_latest.json", {}
    )
    guard_fill_audit = _load_json(data_dir / "wallet_copy_guard_fill_recording_audit_latest.json", {})
    fill_toxicity = _load_json(data_dir / "wallet_copy_fill_toxicity_latest.json", {})
    fill_loss_attribution = _load_json(data_dir / "wallet_copy_fill_conditioned_loss_attribution_latest.json", {})
    window_time_reject_attribution = _load_json(
        data_dir / "wallet_copy_window_time_reject_attribution_latest.json", {}
    )
    active_set_starvation_packet = _load_json(data_dir / "active_set_starvation_packet_latest.json", {})
    inventory_skip_lifecycle = _load_json(data_dir / "inventory_skip_lifecycle_trace_latest.json", {})
    toxicity_denylist = _load_json(root / "configs" / "wallet_copy" / "toxicity_denylist.json", {})
    strategy_map = _load_json(data_dir / "strategy_map_latest.json", {})
    resource_utilization = _load_json(data_dir / "resource_utilization_latest.json", {})
    decompiler_intake = _load_json(data_dir / "wallet_copy_strategy_decompiler_intake_latest.json", {})
    followability = _load_json(data_dir / "wallet_copy_followability_leaderboard_latest.json", {})
    full_universe = _load_json(data_dir / "wallet_copy_full_universe_copyability_latest.json", {})
    wide_capture_roster = _load_json(data_dir / "wide_alpha_capture_roster_latest.json", {})
    wide_current_manifest: dict[str, Any] = {}
    wide_current_manifest_path = ""
    wide_depth_frontier = _load_json(
        data_dir / "wide_depth_priority_frontier_latest.json", {}
    )
    park_reconciliation_82c8 = _load_json(
        data_dir / "82c8_park_reconciliation_latest.json", {}
    )
    repaired_eligible_slice_cohort_gap = _load_json(
        data_dir / "repaired_eligible_slice_cohort_gap_latest.json", {}
    )
    wide_order104_alpha = _load_json(
        data_dir / "alpha_decay_report_wide_order104_latest.json", {}
    )
    wide_order106_delta = _load_json(
        data_dir / "wide_reconciler_slice_selection_delta_order106_latest.json", {}
    )
    wide_alpha_metric_validity = _load_json(
        data_dir / "wide_alpha_metric_validity_latest.json", {}
    )
    standby_park_registry = _load_json(
        data_dir / "wallet_copy_standby_park_exclusions.json", {}
    )
    f1_accrual_stop_951b = _load_json(
        data_dir / "951b_f1_accrual_stop_latest.json", {}
    )
    order109_residual_ledger = _load_json(
        data_dir / "wallet_copy_order109_usdc_transfer_ledger_latest.json", {}
    )
    wide_candidate_standings = _load_json(data_dir / "wide_candidate_standings_latest.json", {})
    wide_copyable_rate_reachability = _load_json(
        data_dir / "wide_copyable_rate_reachability_latest.json", {}
    )
    wide_f3_batch_interval_attribution = _load_json(
        data_dir / "wide_f3_batch_interval_attribution_latest.json", {}
    )
    wide_frontier_deficit_partition = _load_json(
        data_dir / "wide_frontier_deficit_partition_latest.json", {}
    )
    wide_resolved_signal_accrual = _load_json(
        data_dir / "wide_resolved_signal_accrual_latest.json", {}
    )
    wide_fingerprint_durability = _load_json(
        data_dir / "wide_fingerprint_durability_latest.json", {}
    )
    wide_selector_admissibility_divergence = _load_json(
        data_dir / "wide_selector_admissibility_divergence_latest.json", {}
    )
    wide_fingerprint_evidence = _load_json(
        data_dir / "wide_policy_fingerprint_evidence_latest.json", {}
    )
    rtds_observation_watermarks = _load_json(
        data_dir / "wallet_copy_rtds_observation_watermarks.json", {}
    )
    wide_positive_slice_family = _load_json(
        data_dir / "wide_positive_slice_family_state.json", {}
    )
    wide_positive_slice_family_actuator = _load_json(
        data_dir / "wide_positive_slice_family_live_actuator_latest.json", {}
    )
    wide_multiwallet_consensus = _load_json(
        data_dir / "wide_multiwallet_consensus_state.json", {}
    )
    wide_sequential_quorum = _load_json(
        data_dir / "wide_sequential_quorum_state.json", {}
    )
    wide_exact_policy = _load_json(data_dir / "wide_exact_policy_paper_state.json", {})
    wide_climb_backup_manifest = _load_json(
        data_dir / "wide_exact_policy_manifest_climb_backup_fd05.json", {}
    )
    wide_climb_backup_state = _load_json(
        data_dir / "wide_exact_policy_paper_state_climb_backup_fd05.json", {}
    )
    wide_supervisor = _load_json(data_dir / "wide_prospective_supervisor_state.json", {})
    lane_manifest_meta = (
        wide_exact_policy.get("manifest")
        if isinstance(wide_exact_policy.get("manifest"), dict)
        else {}
    )
    wide_manifest_ref = str(
        lane_manifest_meta.get("manifest_path")
        or wide_supervisor.get("manifest")
        or ""
    )
    wide_manifest_path = Path(wide_manifest_ref)
    if wide_manifest_ref and not wide_manifest_path.is_absolute():
        wide_manifest_path = root / wide_manifest_path
    wide_manifest = _load_json(wide_manifest_path, {}) if wide_manifest_ref else {}
    wide_current_manifest = wide_manifest or _load_json(
        data_dir / "wide_exact_policy_manifest_order108_fresh_latest.json", {}
    ) or _load_json(
        data_dir / "wide_exact_policy_manifest_order93_widened_latest.json", {}
    )
    wide_current_manifest_path = wide_manifest_ref or (
        "data/research/wide_exact_policy_manifest_order108_fresh_latest.json"
        if (data_dir / "wide_exact_policy_manifest_order108_fresh_latest.json").exists()
        else "data/research/wide_exact_policy_manifest_order93_widened_latest.json"
    )
    wide_fingerprint_cells = {
        str(row.get("wide_policy_fingerprint") or ""): row
        for row in (wide_fingerprint_evidence.get("cells") or [])
        if isinstance(row, dict) and row.get("wide_policy_fingerprint")
    }
    wide_frozen_capture_evidence = []
    for row in wide_manifest.get("capture_watch_wallets") or []:
        if not isinstance(row, dict) or not isinstance(row.get("slice_freeze"), dict):
            continue
        fingerprint = str(row.get("wide_policy_fingerprint") or "")
        cell = wide_fingerprint_cells.get(fingerprint, {})
        rescore = venue_gate_summary(cell)
        wide_frozen_capture_evidence.append(
            {
                "wallet": row.get("wallet"),
                "wide_policy_fingerprint": fingerprint,
                "capture_exclusion_overridden": row["slice_freeze"].get(
                    "capture_exclusion_overridden"
                ),
                "resolved": rescore.get("resolved"),
                "post_fee_pnl_usd": rescore.get("post_fee_pnl_usd"),
                "roi_pct": rescore.get("roi_pct"),
                "first_half_post_fee_pnl_usd": rescore.get(
                    "first_half_post_fee_pnl_usd"
                ),
                "second_half_post_fee_pnl_usd": rescore.get(
                    "second_half_post_fee_pnl_usd"
                ),
                "f1_pass": rescore.get("f1_pass"),
            }
        )
    wide_frozen_capture_evidence.sort(
        key=lambda row: (str(row.get("wallet") or ""), str(row.get("wide_policy_fingerprint") or ""))
    )
    wide_terminal_reconciliation = (
        wide_exact_policy.get("terminal_reconciliation")
        if isinstance(wide_exact_policy.get("terminal_reconciliation"), dict)
        else {}
    )
    wide_terminal_run_id = str(wide_terminal_reconciliation.get("run_id") or "")
    wide_receipt_lags_ms = sorted(
        float(row.get("receipt_to_fetch_ms"))
        for row in (wide_exact_policy.get("attempt_terminals") or [])
        if isinstance(row, dict)
        and str(row.get("run_id") or "") == wide_terminal_run_id
        and row.get("receipt_to_fetch_ms") is not None
    )
    wide_receipt_p95_ms = (
        wide_receipt_lags_ms[
            max(0, min(len(wide_receipt_lags_ms) - 1, math.ceil(0.95 * len(wide_receipt_lags_ms)) - 1))
        ]
        if wide_receipt_lags_ms
        else None
    )
    wide_prewarm_primary = (
        frozen_fingerprint_f2_prewarm.get("primary")
        if isinstance(frozen_fingerprint_f2_prewarm.get("primary"), dict)
        else {}
    )
    wide_sidecar_primary = (
        copy_freeze_sidecar.get("primary")
        if isinstance(copy_freeze_sidecar.get("primary"), dict)
        else {}
    )
    wide_sidecar_checks = (
        copy_freeze_sidecar.get("checks")
        if isinstance(copy_freeze_sidecar.get("checks"), dict)
        else {}
    )
    wide_accelerator_priority = (
        freeze_resolution_accelerator.get("direct_climb_priority")
        if isinstance(freeze_resolution_accelerator.get("direct_climb_priority"), list)
        else []
    )
    wide_accelerator_primary = (
        wide_accelerator_priority[0]
        if wide_accelerator_priority and isinstance(wide_accelerator_priority[0], dict)
        else {}
    )
    wide_climb_identity = wide_accelerator_primary or wide_prewarm_primary
    wide_climb_wallet = str(wide_climb_identity.get("wallet") or "").lower()
    wide_climb_fingerprint = str(
        wide_climb_identity.get("wide_policy_fingerprint") or ""
    )
    wide_climb_primary = {}
    if (
        str(wide_sidecar_primary.get("wallet") or "").lower() == wide_climb_wallet
        and str(wide_sidecar_primary.get("wide_policy_fingerprint") or "")
        == wide_climb_fingerprint
    ):
        wide_climb_primary = {**wide_sidecar_primary, **wide_sidecar_checks}
    elif (
        str(wide_prewarm_primary.get("wallet") or "").lower() == wide_climb_wallet
        and str(wide_prewarm_primary.get("wide_policy_fingerprint") or "")
        == wide_climb_fingerprint
    ):
        wide_climb_primary = wide_prewarm_primary
    wide_exact_wallets = (
        wide_exact_policy.get("wallets")
        if isinstance(wide_exact_policy.get("wallets"), dict)
        else {}
    )
    wide_climb_exact_wallet = (
        wide_exact_wallets.get(wide_climb_wallet)
        if isinstance(wide_exact_wallets.get(wide_climb_wallet), dict)
        else {}
    )
    wide_climb_frontier = next(
        (
            row
            for row in (wide_direct_admissible_frontier.get("nearest_frontier") or [])
            if isinstance(row, dict)
            and str(row.get("wallet") or "").lower() == wide_climb_wallet
            and str(row.get("wide_policy_fingerprint") or "") == wide_climb_fingerprint
        ),
        {},
    )
    wide_climb_direct_source = (
        wide_climb_frontier.get("direct_source")
        if isinstance(wide_climb_frontier.get("direct_source"), dict)
        else {}
    )
    wide_direct_climb_exact = {
        "wallet": wide_climb_wallet or None,
        "wide_policy_fingerprint": wide_climb_fingerprint or None,
        "attempted_exact_policy_buys": wide_climb_exact_wallet.get(
            "attempted_exact_policy_buys"
        ),
        "copyable_exact_policy_buys": wide_climb_exact_wallet.get(
            "copyable_exact_policy_buys"
        ),
        "resolved_orders": wide_climb_exact_wallet.get("resolved_orders"),
        "fresh_own_source_buy_rows_30m": wide_climb_primary.get(
            "fresh_own_source_buy_rows_30m"
        ),
        "f2_minimum": wide_climb_primary.get("f2_minimum"),
        "direct_source_attempts": wide_climb_direct_source.get("attempts"),
        "direct_source_copyable": wide_climb_direct_source.get("copyable"),
        "latest_direct_source_receipt_at": wide_climb_direct_source.get(
            "latest_receipt_at"
        ),
    }
    full_universe_legacy_pointers = [
        _load_json(data_dir / name, {})
        for name in (
            "wallet_copy_full_universe_copyability_leaderboard_latest.json",
            "wallet_copy_full_universe_copyability_leaderboard_summary_latest.json",
        )
    ]
    hot_history_accumulator = _load_json(data_dir / "wallet_copy_hot_history_accumulator_state.json", {})
    market_scan = _load_json(data_dir / "wallet_market_scan_ranked.json", {})
    market_cohort_replay = _load_json(data_dir / "wallet_market_cohort_replay_latest.json", {})
    temporal_profitability = _load_json(data_dir / "wallet_temporal_profitability_latest.json", {})
    source_active_replay = _load_json(data_dir / "source_active_policy_history_replay_latest.json", {})
    source_active_cohort = _load_json(data_dir / "source_active_liveness_cohort_latest.json", {})
    focused_candidate_p1 = _load_json(data_dir / "focused_candidate_p1_a3e0985f2d_latest.json", {})
    if not focused_candidate_p1:
        focused_candidate_p1 = _load_json(data_dir / "focused_candidate_p1_8a47951a3c_latest.json", {})
    a3e0_midnight_bundle = _load_json(data_dir / "a3e0_midnight_bundle_prepared_latest.json", {})
    temporal_supplemental_manifest = _load_json(data_dir / "temporal_supplemental_history_manifest.json", {})
    winner_variation = _load_json(data_dir / "wallet_copy_winner_variation_siblings_latest.json", {})
    temporal_probe_apply = _load_json(data_dir / "temporal_watch_tier_probe_apply_latest.json", {})
    watch_tier_shadow_ev = _load_json(data_dir / "watch_tier_shadow_ev_latest.json", {})
    weekend_stakeout = _load_json(data_dir / "weekend_specialist_stakeout_packet_latest.json", {})
    weekend_parity_packet = _load_json(data_dir / "weekend_parity_packet_latest.json", {})
    sub25_spot_check = _load_json(data_dir / "sub25_bucket_accounting_spot_check_latest.json", {})
    btc5m_fleet = _load_json(data_dir / "btc5m_live_paper_fleet_latest.json", {})
    two_sided_prime = _load_json(data_dir / "btc5m_two_sided_prime_study_latest.json", {})
    morning_table = _load_json(data_dir / "btc5m_morning_ranked_table_latest.json", {})
    structural_scalp_lane = _load_json(data_dir / "btc5m_structural_scalp_paper_lane_state.json", {})
    structural_scalp_promotion = _load_json(data_dir / "btc5m_structural_scalp_promotion_prep_latest.json", {})
    volume_standby_promotion = _load_json(data_dir / "13e0_exact_policy_promotion_packet_latest.json", {})
    frozen_history_audit = _load_json(data_dir / "frozen_history_consumer_audit_latest.json", {})
    eth5m_replication_scout = _load_json(data_dir / "eth5m_replication_scout_paper_state.json", {})
    dispatch_throughput = _load_json(data_dir / "wallet_copy_dispatch_throughput_audit_latest.json", {})
    data_layer = _load_json(data_dir / "data_layer_v1_manifest.json", {})
    dr_preflight = _load_json(data_dir / "wallet_copy_dr_preflight_latest.json", {})
    clearance_gaps = _load_json(data_dir / "wallet_copy_queue_clearance_gaps.json", {})
    ranked_clearance_packets = _load_json(data_dir / "ranked_queue_clearance_packets_latest.json", {})
    ready_shadow = _load_json(data_dir / "wallet_copy_ready_shadow_lanes_state.json", {})
    a689_82c8_cut = _load_json(data_dir / "a689_82c8_ready_shadow_cut_spec_latest.json", {})
    terminal_82c8_decision = _load_json(data_dir / "82c8_terminal_decision_latest.json", {})
    wide_82c8_standby_binding = _load_json(
        data_dir / "82c8_wide_standby_binding_latest.json", {}
    )
    hot_standby_source_liveness = _load_json(
        data_dir / "hot_standby_source_liveness_latest.json", {}
    )
    weekday_readmission = _load_json(data_dir / "weekday_readmission_status_latest.json", {})
    inventory_convergence_skip_lane = _load_json(
        data_dir / "inventory_convergence_skip_paper_lane_latest.json",
        {},
    )
    fee_aware_long_horizon_lane = _load_json(
        data_dir / "fee_aware_long_horizon_copy_paper_latest.json",
        {},
    )
    alpha_eligible_profiles_lane = _load_json(
        data_dir / "alpha_decay_eligible_profiles_paper_lane_state.json",
        {},
    )
    a689_hot_standby_lane = _load_json(data_dir / "a6896d11_hot_standby_paper_lane_latest.json", {})
    c539_deferred_open_probe = _load_json(
        data_dir / "c539_deferred_open_paper_probe_state.json",
        {},
    )
    bac25_forward_only_lane = _load_json(
        data_dir / "bac25_forward_only_lane_latest.json",
        {},
    )
    wallet_951b_forward_only_lane = _load_json(
        data_dir / "951b_forward_only_lane_latest.json",
        {},
    )
    bac25_forward_writer_scope = _load_json(
        data_dir / "bac25_forward_writer_scope_latest.json",
        {},
    )
    wide_forward_sibling_lanes_state = _load_json(
        data_dir / "wide_forward_sibling_lanes_state.json",
        {},
    )
    wallet_82c8_8bb70201_forward_only_lane = _load_json(
        data_dir / "82c8_8bb70201_forward_only_lane_latest.json",
        {},
    )
    wallet_82c8_fdd8af33_forward_only_lane = _load_json(
        data_dir / "82c8_fdd8af33_forward_only_lane_latest.json",
        {},
    )
    policy_family_terminal_registry = _load_json(
        data_dir / "wide_policy_family_terminal_registry_latest.json",
        {},
    )
    pipeline_slo_artifact_path = (
        data_dir / "pipeline_slo_and_standby_readiness_latest.json"
    )
    pipeline_slo_artifact = _load_json(pipeline_slo_artifact_path, {})
    refresh_cadence_state = _load_json(
        data_dir / "codex_refresh_cadence_state.json", {}
    )
    a689_edge_transfer = _load_json(data_dir / "a689_edge_transfer_latest.json", {})
    a689_0200_tripwire = _load_json(data_dir / "a689_0200_tripwire_latest.json", {})
    pipeline_late_decomposition = _load_json(data_dir / "pipeline_late_decomposition_latest.json", {})
    budget_bind_margin_packet = _load_json(data_dir / "budget_bind_margin_packet_latest.json", {})
    f418_readmission_packet = _load_json(data_dir / "f418_readmission_packet_latest.json", {})
    ruling10_abstain_probe = _load_json(data_dir / "ruling10_abstain_probe_latest.json", {})
    d60c_latency_attribution = _load_json(data_dir / "d60c_latency_attribution_latest.json", {})
    live_order_reject_attribution = _load_json(data_dir / "live_order_reject_attribution_latest.json", {})
    pipeline_late_wall_time = (
        pipeline_late_decomposition.get("guard_cycle_wall_time_estimate")
        if isinstance(pipeline_late_decomposition.get("guard_cycle_wall_time_estimate"), dict)
        else {}
    )
    pipeline_late_cycle_duration = (
        pipeline_late_wall_time.get("cycle_duration_s")
        if isinstance(pipeline_late_wall_time.get("cycle_duration_s"), dict)
        else {}
    )
    experiment_preregistration = _load_json(data_dir / "experiment_preregistration_latest.json", {})
    fee_edge_decomposition = _load_json(data_dir / "fee_edge_decomposition_latest.json", {})
    entry_price_band_gate = _load_json(root / "configs/wallet_copy/entry_price_band_gate.json", {})
    entry_price_band_gate_counterfactual = _load_json(
        data_dir / "entry_price_loss_band_gate_counterfactual_latest.json", {}
    )
    f418_post_band_causal = _load_json(
        data_dir / "f418_post_band_gate_residual_loss_causal_shadow_latest.json", {}
    )
    profit_latency_counterfactual = _load_json(
        data_dir / "profit_latency_suppression_counterfactual_latest.json", {}
    )
    f418_acceptance_funnel = _load_json(data_dir / "f418_acceptance_funnel_latest.json", {})
    f418_green_day_conversion = _load_json(
        data_dir / "f418_green_day_conversion_shadow_latest.json", {}
    )
    f418_size_clamp_fee_leak = _load_json(
        data_dir / "f418_size_clamp_fee_leak_shadow_latest.json", {}
    )
    f418_spread_elasticity = _load_json(
        data_dir / "f418_spread_elasticity_shadow_latest.json", {}
    )
    f418_window_time_book_crossed = _load_json(
        data_dir / "f418_window_time_book_crossed_latest.json", {}
    )
    fak_depth_persistence = _load_json(
        data_dir / "fak_depth_persistence_timing_shadow_latest.json", {}
    )
    member_native_policy_uplift = _load_json(
        data_dir / "member_native_policy_acceptance_uplift_shadow_latest.json", {}
    )
    top10_direct_clob_paper = _load_json(
        data_dir / "wallet_copy_top10_broad_paper_measurement_state.json", {}
    )
    selected_member_attribution = _load_json(
        data_dir / "selected_member_guard_submit_attribution_latest.json", {}
    )
    market_buy_precision_counterfactual = _load_json(
        data_dir / "market_buy_precision_counterfactual_latest.json", {}
    )
    weekend_window_sign_skew = _load_json(data_dir / "paper_copy_weekend_window_sign_skew_latest.json", {})
    weekend_hour_skew = _load_json(data_dir / "paper_copy_weekend_hour_of_day_skew_latest.json", {})
    fak_nomatch_requote = _load_json(data_dir / "paper_copy_fak_nomatch_requote_latest.json", {})
    coverage_gap_diagnosis = _load_json(data_dir / "coverage_gap_diagnosis_latest.json", {})
    coverage_gap_signal_supply = _load_json(data_dir / "coverage_gap_signal_supply_check_latest.json", {})
    routing_disambiguation = _load_json(data_dir / "routing_disambiguation_latest.json", {})
    campaign_lat_packet = _load_json(data_dir / "campaign_lat_p1_packet_latest.json", {})
    routing_shadow_validation = _load_json(data_dir / "routing_shadow_validation_latest.json", {})
    selection_visibility_packet = _load_json(data_dir / "selection_visibility_packet_latest.json", {})
    routing_shadow_attribution_pin = _load_json(
        data_dir / "routing_shadow_validation_attribution_pin_latest.json", {}
    )
    member_rolling20 = _load_json(data_dir / "wallet_copy_member_rolling20_latest.json", {})
    cross_exchange_probability = _load_json(
        data_dir / "btc5m_cross_exchange_probability_edge_paper_lane_state.json", {}
    )
    cross_exchange_live_actuator = _load_json(
        data_dir / "btc5m_cross_exchange_probability_edge_live_actuator_latest.json", {}
    )
    cross_exchange_campaign = _cross_exchange_campaign_truth(live)
    if isinstance(cross_exchange_live_actuator, dict):
        cross_exchange_live_actuator = {
            **cross_exchange_live_actuator,
            **cross_exchange_campaign,
        }
    live_method_supply = _load_json(data_dir / "live_method_supply_packet_latest.json", {})
    delayed_offset_park = _load_json(
        data_dir / "e5_delayed_offset_side_selective_promotion_packet_latest.json", {}
    )
    multivenue_residual_matrix = _load_json(
        data_dir / "btc5m_multivenue_residual_matrix_state.json", {}
    )
    complete_set_paired_maker = _load_json(
        data_dir / "btc5m_complete_set_paired_maker_state.json", {}
    )
    complete_set_split_sell = _load_json(
        data_dir / "btc5m_complete_set_split_sell_overround_state.json", {}
    )
    book_shock_reversion = _load_json(
        data_dir / "btc5m_book_shock_reversion_state.json", {}
    )
    queue_hazard_maker = _load_json(
        data_dir / "btc5m_queue_hazard_inventory_skew_maker_state.json", {}
    )
    native_aggressor_sweep = _load_json(
        data_dir / "btc5m_native_aggressor_sweep_continuation_state.json", {}
    )
    native_complement_lead_lag = _load_json(
        data_dir / "btc5m_native_complement_lead_lag_taker_state.json", {}
    )
    polymarket_cross_asset_leader_lag = _load_json(
        data_dir / "btc5m_polymarket_cross_asset_leader_lag_state.json", {}
    )
    polymarket_first_leader_cross_asset_lag = _load_json(
        data_dir / "btc5m_polymarket_first_leader_cross_asset_lag_state.json", {}
    )
    native_signed_tape_imbalance_stale_ask = _load_json(
        data_dir / "btc5m_native_signed_tape_imbalance_stale_ask_state.json", {}
    )
    native_l2_microprice_displacement_stale_ask = _load_json(
        data_dir / "btc5m_native_l2_microprice_displacement_stale_ask_state.json", {}
    )
    native_l2_tob_pressure_imbalance = _load_json(
        data_dir / "btc5m_native_l2_tob_pressure_imbalance_state.json", {}
    )
    native_l2_cross_outcome_parity_stale_ask = _load_json(
        data_dir / "btc5m_native_l2_cross_outcome_parity_stale_ask_state.json", {}
    )
    native_l2_depth_weighted_microprice_parity_stale_ask = _load_json(
        data_dir
        / "btc5m_native_l2_depth_weighted_microprice_parity_stale_ask_state.json",
        {},
    )
    native_l2_complement_bid_support_parity_stale_ask = _load_json(
        data_dir
        / "btc5m_native_l2_complement_bid_support_parity_stale_ask_state.json",
        {},
    )
    native_l2_complement_ask_cap_parity_stale_ask = _load_json(
        data_dir
        / "btc5m_native_l2_complement_ask_cap_parity_stale_ask_state.json",
        {},
    )
    rung_c_no_target = _load_json(
        data_dir / "rung_c_no_admissible_target_latest.json", {}
    )
    current_f1_f4_fallout = _load_json(
        data_dir / "current_f1_f4_fallout_audit.json", {}
    )
    promoted_cell_selector = _load_json(
        data_dir / "btc5m_cross_exchange_promoted_cell_latest.json", {}
    )
    scorecard = _scorecard_for_digest(root, data_dir)
    passive_at_source_holdout = _load_json(
        data_dir / "passive_at_source_holdout_latest.json", {}
    )
    taker_price_subband_holdout = _load_json(
        data_dir / "taker_price_subband_holdout_latest.json", {}
    )
    band_pnl_surface_reconciliation = _load_json(
        data_dir / "band_pnl_surface_reconciliation_latest.json", {}
    )
    fee_realization_bank_reconciliation = _load_json(
        data_dir / "fee_realization_bank_reconciliation_latest.json", {}
    )
    payout_receipt_reconciliation = _load_json(
        data_dir / "payout_receipt_reconciliation_latest.json", {}
    )
    temporal_slice_label_divergence = _load_json(
        data_dir / "temporal_slice_label_divergence_latest.json", {}
    )
    maker_min_share_cap_choke = _load_json(
        data_dir / "maker_min_share_cap_choke_latest.json", {}
    )
    closed_scorecard = _latest_closed_scorecard(data_dir)
    closed_automation_drift = _scorecard_automation_drift_summary(closed_scorecard)
    closed_weekly_verdict = _weekly_verdict_from_scorecards(data_dir)
    market_facts_path = root / "docs" / "agents" / "MARKET_FACTS.md"
    market_facts_text = market_facts_path.read_text(errors="replace") if market_facts_path.exists() else ""
    market_fact_bullets = [
        line.strip()
        for line in market_facts_text.splitlines()
        if line.strip().startswith("- ")
    ][:6]

    live_summary = live.get("summary") if isinstance(live.get("summary"), dict) else {}
    latest_live_order = _latest_live_order_snapshot(live)
    latest_accepted_live_order = _latest_live_order_snapshot(live, accepted_only=True)
    runtime_permission = (
        live.get("runtime_permission") if isinstance(live.get("runtime_permission"), dict) else {}
    )
    runtime_permission_blockers = (
        runtime_permission.get("blockers")
        if isinstance(runtime_permission.get("blockers"), list)
        else []
    )
    guard_loop_profile = (
        guard.get("guard_loop_profile") if isinstance(guard.get("guard_loop_profile"), dict) else {}
    )
    alternate_transport_bridge = (
        guard.get("alternate_transport_copyintent_bridge")
        if isinstance(guard.get("alternate_transport_copyintent_bridge"), dict)
        else {}
    )
    alternate_source_rotation = (
        guard.get("alternate_source_rotation")
        if isinstance(guard.get("alternate_source_rotation"), dict)
        else {}
    )
    try:
        current_guard_pid = int(guard.get("pid"))
    except (TypeError, ValueError):
        current_guard_pid = None
    event_guard_loop_profile = _latest_guard_loop_profile_from_events(data_dir, current_pid=current_guard_pid)
    if event_guard_loop_profile and (
        not guard_loop_profile
        or _guard_profile_total_s(guard_loop_profile) is None
        or not guard_loop_profile.get("status")
    ):
        guard_loop_profile = event_guard_loop_profile
    guard_loop_stage_timers = (
        guard_loop_profile.get("stage_timers")
        if isinstance(guard_loop_profile.get("stage_timers"), list)
        else []
    )
    guard_loop_top_stage_timers = sorted(
        [row for row in guard_loop_stage_timers if isinstance(row, dict)],
        key=lambda row: float(row.get("duration_s") or 0.0),
        reverse=True,
    )[:8]
    guard_latency_trigger = _recent_guard_latency_trigger(data_dir, current_pid=current_guard_pid)
    score_total = _canonical_day_score(scorecard)
    if not score_total:
        score_total = (
            (scorecard.get("today") or {}).get("total") or {}
            if isinstance(scorecard.get("today"), dict)
            else {}
        )
    score_today = scorecard.get("today") if isinstance(scorecard.get("today"), dict) else {}
    score_today_members = _canonical_member_scores(scorecard)
    if not score_today_members:
        score_today_members = score_today.get("per_member") if isinstance(score_today.get("per_member"), dict) else {}
    score_member_trigger_watch = _canonical_member_trigger_watch(scorecard)
    closed_total = _canonical_day_score(closed_scorecard)
    if not closed_total:
        closed_today = closed_scorecard.get("today") if isinstance(closed_scorecard.get("today"), dict) else {}
        closed_total = closed_today.get("total") if isinstance(closed_today.get("total"), dict) else {}
    closed_volume = (
        (closed_scorecard.get("volume_kpi") or {}).get("canonical_daily")
        if isinstance(closed_scorecard.get("volume_kpi"), dict)
        else {}
    )
    closed_since_topup = (
        closed_scorecard.get("since_topup_truth")
        if isinstance(closed_scorecard.get("since_topup_truth"), dict)
        else {}
    )
    closed_target = (
        (closed_scorecard.get("target_ladder") or {}).get("actual")
        if isinstance(closed_scorecard.get("target_ladder"), dict)
        else {}
    )
    since_topup = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    lifetime_pnl_truth = (
        scorecard.get("lifetime_pnl_truth")
        if isinstance(scorecard.get("lifetime_pnl_truth"), dict)
        else {}
    )
    lifetime_price_bands = (
        lifetime_pnl_truth.get("by_price_band")
        if isinstance(lifetime_pnl_truth.get("by_price_band"), dict)
        else {}
    )
    lifetime_price_subbands = (
        lifetime_pnl_truth.get("by_price_subband")
        if isinstance(lifetime_pnl_truth.get("by_price_subband"), dict)
        else {}
    )
    lifetime_money_subbands = {
        key: {
            "resolved_fills": row.get("resolved_fills"),
            "cost_usd": row.get("cost_usd"),
            "payout_usd": row.get("payout_usd"),
            "pnl_usd_realized": row.get("pnl_usd"),
            "roi_pct_realized": row.get("roi_pct"),
        }
        for key in ("01a_25_32", "01b_32_40", "01c_40_50")
        if isinstance((row := lifetime_price_subbands.get(key)), dict)
    }
    since_topup_overlay = (
        since_topup.get("self_feed_reconciliation_overlay")
        if isinstance(since_topup.get("self_feed_reconciliation_overlay"), dict)
        else {}
    )
    since_topup_cash_residual = (
        since_topup.get("cash_diff_reconciliation_residual")
        if isinstance(since_topup.get("cash_diff_reconciliation_residual"), dict)
        else {}
    )
    balance_feed_monitor = (
        scorecard.get("balance_feed_monitor")
        if isinstance(scorecard.get("balance_feed_monitor"), dict)
        else {}
    )
    scorecard_cash_residual = (
        scorecard.get("cash_diff_reconciliation_residual")
        if isinstance(scorecard.get("cash_diff_reconciliation_residual"), dict)
        else {}
    )
    volume = (
        (scorecard.get("volume_kpi") or {}).get("canonical_daily")
        if isinstance(scorecard.get("volume_kpi"), dict)
        else {}
    )
    scorecard_text_truth = _scorecard_text_day_pnl(
        data_dir / "brainless_ops_scorecard.out"
    )
    score_total, volume, since_topup = _prefer_newer_scorecard_text_truth(
        scorecard=scorecard,
        score_total=score_total,
        volume=volume,
        since_topup=since_topup,
        text_truth=scorecard_text_truth,
    )
    volume_rows = (
        (scorecard.get("volume_kpi") or {}).get("rows")
        if isinstance(scorecard.get("volume_kpi"), dict)
        else []
    )
    execution = scorecard.get("execution_model_kpi") if isinstance(scorecard.get("execution_model_kpi"), dict) else {}
    execution_drip = execution.get("drip") if isinstance(execution.get("drip"), dict) else {}
    execution_strong = execution.get("strong_tier") if isinstance(execution.get("strong_tier"), dict) else {}
    maker_fallback_conversion = _maker_fallback_conversion_summary(data_dir, scorecard)
    guard_active_set = guard.get("active_set") if isinstance(guard.get("active_set"), dict) else {}
    guard_members = guard_active_set.get("members") if isinstance(guard_active_set.get("members"), list) else []
    guard_active_runtime = (
        guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    )
    guard_runtime_members = (
        guard_active_runtime.get("members")
        if isinstance(guard_active_runtime.get("members"), list)
        else []
    )
    guard_runtime_wallets = [
        str(row.get("source_wallet") or "")
        for row in guard_runtime_members
        if isinstance(row, dict) and row.get("source_wallet")
    ]
    guard_runtime_selected = (
        guard_active_runtime.get("selected_member")
        if isinstance(guard_active_runtime.get("selected_member"), dict)
        else {}
    )
    guard_candidate = guard.get("candidate") if isinstance(guard.get("candidate"), dict) else {}
    guard_candidate_pass_gate = (
        guard_candidate.get("pass_gate")
        if isinstance(guard_candidate.get("pass_gate"), dict)
        else {}
    )
    score_active_roster = scorecard.get("active_set_roster") if isinstance(scorecard.get("active_set_roster"), dict) else {}
    score_members = score_active_roster.get("members") if isinstance(score_active_roster.get("members"), list) else []
    members = guard_members or score_members
    configured_member_count = len(members)
    legacy_current_member = {
        "candidate_id": guard.get("candidate_id"),
        "source_wallet": guard.get("source_wallet"),
        "policy_id": guard.get("policy_id"),
    }
    active_set_current_member = next(
        (row for row in members if isinstance(row, dict) and row.get("is_current_cycle_member")),
        {},
    )
    current_member = next(
        (
            row
            for row in (guard_runtime_selected, active_set_current_member, legacy_current_member)
            if isinstance(row, dict) and (row.get("candidate_id") or row.get("source_wallet"))
        ),
        {},
    )

    participation = guard.get("window_participation") if isinstance(guard.get("window_participation"), dict) else {}
    guard_volume_rows = (
        participation.get("window_rollups")
        if isinstance(participation.get("window_rollups"), list)
        else participation.get("rows")
        if isinstance(participation.get("rows"), list)
        else []
    )
    participation_source_rows = (
        guard_volume_rows
        if isinstance(guard_volume_rows, list) and guard_volume_rows
        else volume_rows
        if isinstance(volume_rows, list)
        else []
    )
    live_order_counts_by_market = _live_order_counts_by_market(live)
    recent_participation_rows = [
        {
            "market_slug": row.get("market_slug"),
            "wallet_eligible_orders": row.get("wallet_eligible_orders"),
            "our_submits": live_order_counts_by_market.get(str(row.get("market_slug") or ""), {}).get(
                "our_submits",
                row.get("our_submits"),
            ),
            "our_fills": live_order_counts_by_market.get(str(row.get("market_slug") or ""), {}).get(
                "our_fills",
                row.get("our_fills"),
            ),
            "our_rejects": live_order_counts_by_market.get(str(row.get("market_slug") or ""), {}).get(
                "our_rejects",
                0,
            ),
            "rollup_our_submits": row.get("our_submits"),
            "rollup_our_fills": row.get("our_fills"),
            "dominant_skip_reason": row.get("dominant_skip_reason"),
            "missed_active_window": row.get("missed_active_window"),
            "missed_window_attribution": row.get("missed_window_attribution"),
            "empty_window_reason": row.get("empty_window_reason"),
            "skip_reasons": row.get("dominant_skip_reason_counts")
            if isinstance(row.get("dominant_skip_reason_counts"), dict)
            else row.get("skip_reasons")
            if isinstance(row.get("skip_reasons"), dict)
            else {},
            "participation_skip_category": row.get("participation_skip_category"),
            "participation_equivalent": row.get("participation_equivalent"),
            "adjusted_missed_active_window": row.get("adjusted_missed_active_window"),
        }
        for row in participation_source_rows[:8]
        if isinstance(row, dict)
    ]
    rolling_participation = _rolling_window_participation_summary(participation)
    flow_participation = _flow_truth_participation(participation)
    adjusted_participation = _current_adjusted_participation_summary(participation)
    source_coverage = {
        "covered_windows": adjusted_participation.get("source_coverage_windows"),
        "denominator_windows": adjusted_participation.get("source_coverage_denominator_windows"),
        "rate_pct": adjusted_participation.get("source_coverage_rate_pct"),
        "no_signal_windows": adjusted_participation.get("no_signal_windows"),
        "basis": adjusted_participation.get("basis"),
    }
    active_set_rtds_premerge = (
        guard.get("active_set_rtds_premerge") if isinstance(guard.get("active_set_rtds_premerge"), dict) else {}
    )
    event_triggered_cycle_scheduler = (
        guard.get("event_triggered_cycle_scheduler")
        if isinstance(guard.get("event_triggered_cycle_scheduler"), dict)
        else {}
    )
    active_set_rtds_selected_priority = (
        active_set_rtds_premerge.get("selected_wallet_priority_premerge")
        if isinstance(active_set_rtds_premerge.get("selected_wallet_priority_premerge"), dict)
        else {}
    )
    active_set_rtds_rows = (
        active_set_rtds_premerge.get("rows") if isinstance(active_set_rtds_premerge.get("rows"), list) else []
    )
    active_set_polygon_premerge_events = 0
    for row in active_set_rtds_rows:
        if not isinstance(row, dict):
            continue
        profile = row.get("premerge_substage_profile") if isinstance(row.get("premerge_substage_profile"), dict) else {}
        polygon_profile = (
            profile.get("polygon_ws_premerge_parse")
            if isinstance(profile.get("polygon_ws_premerge_parse"), dict)
            else {}
        )
        try:
            active_set_polygon_premerge_events += int(polygon_profile.get("matching_events") or 0)
        except (TypeError, ValueError):
            continue
    active_set_latest_rotation = (
        active_set_overlay.get("latest_rotation")
        if isinstance(active_set_overlay.get("latest_rotation"), dict)
        else {}
    )
    active_set_overlay_members = (
        active_set_overlay.get("members") if isinstance(active_set_overlay.get("members"), list) else []
    )
    active_set_latest_auto_degrade = _latest_active_auto_degrade_member(active_set_overlay)
    active_set_overlay_runtime_count = _overlay_runtime_member_count(
        active_set_overlay_members,
        guard_runtime_members,
    )
    active_set_overlay_enabled_count = sum(
        1 for row in active_set_overlay_members if isinstance(row, dict) and row.get("enabled") is True
    )
    active_set_overlay_disabled_count = sum(
        1 for row in active_set_overlay_members if isinstance(row, dict) and row.get("enabled") is False
    )
    active_set_liveness_admissions = (
        active_set_overlay.get("latest_liveness_admissions")
        if isinstance(active_set_overlay.get("latest_liveness_admissions"), list)
        else []
    )
    active_set_admission_wave: dict[str, Any] = {}
    member_rolling20_rows = (
        member_rolling20.get("rows") if isinstance(member_rolling20.get("rows"), list) else []
    )
    member_rolling20_ready_rows = [
        row
        for row in member_rolling20_rows
        if isinstance(row, dict) and bool(row.get("rolling20_ready"))
    ]
    member_rolling20_trigger_rows = [
        row
        for row in member_rolling20_rows
        if isinstance(row, dict) and bool(row.get("rolling20_rotation_triggered"))
    ]
    member_rolling20_incomplete_negative = [
        row
        for row in member_rolling20_rows
        if isinstance(row, dict)
        and not bool(row.get("rolling20_ready"))
        and float(row.get("rolling20_pnl_usd") or 0.0) <= float(member_rolling20.get("threshold_usd") or -8.0)
    ]
    own_impact_monitor = (
        guard.get("own_impact_monitor") if isinstance(guard.get("own_impact_monitor"), dict) else {}
    )
    e7_summary = e7.get("summary") if isinstance(e7.get("summary"), dict) else {}
    e5_gate = e5.get("promotion_gate") if isinstance(e5.get("promotion_gate"), dict) else {}
    e5_signal_summary = (
        e5_signal_gated.get("summary") if isinstance(e5_signal_gated.get("summary"), dict) else {}
    )
    e5_signal_gate = (
        e5_signal_gated.get("promotion_gate") if isinstance(e5_signal_gated.get("promotion_gate"), dict) else {}
    )
    e5_signal_book_summary = (
        e5_signal_gated_book.get("prospective_no_fallback_summary")
        if isinstance(e5_signal_gated_book.get("prospective_no_fallback_summary"), dict)
        else {}
    )
    e5_book_aware_summary = (
        e5_book_aware.get("prospective_no_fallback_summary")
        if isinstance(e5_book_aware.get("prospective_no_fallback_summary"), dict)
        else {}
    )
    e5_book_aware_ledger = (
        e5_book_aware.get("prospective_no_fallback_resolved_fill_ledger")
        if isinstance(e5_book_aware.get("prospective_no_fallback_resolved_fill_ledger"), dict)
        else {}
    )
    e5_book_aware_gate = (
        e5_book_aware.get("promotion_gate")
        if isinstance(e5_book_aware.get("promotion_gate"), dict)
        else {}
    )
    e5_non_fallback = (
        e5_review.get("book_aware_non_fallback_summary")
        if isinstance(e5_review.get("book_aware_non_fallback_summary"), dict)
        else {}
    )
    e5_5share_summary = (
        e5_5share_regrade.get("summary")
        if isinstance(e5_5share_regrade.get("summary"), dict)
        else {}
    )
    e5_5share_gate = (
        e5_5share_regrade.get("gate")
        if isinstance(e5_5share_regrade.get("gate"), dict)
        else {}
    )
    e5_live_results = [
        row
        for row in (e5_live_actuator.get("results") or [])
        if isinstance(row, dict)
    ]
    latest_e5_live_order = next(
        (
            row
            for row in reversed(live.get("orders") or [])
            if isinstance(row, dict)
            and isinstance(row.get("trade_decision"), dict)
            and str(row["trade_decision"].get("execution_lane") or "") == "e5_maker_first_btc5m_v1"
        ),
        {},
    )
    latest_e5_decision = (
        latest_e5_live_order.get("trade_decision")
        if isinstance(latest_e5_live_order.get("trade_decision"), dict)
        else {}
    )
    latest_e5_result = (
        latest_e5_live_order.get("trade_result")
        if isinstance(latest_e5_live_order.get("trade_result"), dict)
        else {}
    )
    e11_summary = e11.get("summary") if isinstance(e11.get("summary"), dict) else {}
    e11_signal_status = e11.get("signal_status") if isinstance(e11.get("signal_status"), dict) else {}
    e11_book_summary = e11_book.get("summary") if isinstance(e11_book.get("summary"), dict) else {}
    queue_summary = queue.get("summary") if isinstance(queue.get("summary"), dict) else {}
    fresh_flow_probe_summary = (
        fresh_flow_probe.get("summary")
        if isinstance(fresh_flow_probe.get("summary"), dict)
        else {}
    )
    fresh_flow_probe_rows = fresh_flow_probe.get("rows") if isinstance(fresh_flow_probe.get("rows"), list) else []
    market_cohort_shadow_accrual = _market_cohort_shadow_accrual(queue, realtime_shadow, guard_shadow)
    watcher_gap_summary = watcher_gap.get("summary") if isinstance(watcher_gap.get("summary"), dict) else {}
    active_set_poller_summary = (
        active_set_poller.get("summary") if isinstance(active_set_poller.get("summary"), dict) else {}
    )
    active_set_poller_duplicates = (
        active_set_poller_summary.get("duplicate_counts")
        if isinstance(active_set_poller_summary.get("duplicate_counts"), dict)
        else {}
    )
    self_feed_summary = self_feed.get("summary") if isinstance(self_feed.get("summary"), dict) else {}
    cash_ledger_summary = cash_ledger.get("summary") if isinstance(cash_ledger.get("summary"), dict) else {}
    self_feed_trace_summary = (
        self_feed_trace.get("summary") if isinstance(self_feed_trace.get("summary"), dict) else {}
    )
    self_feed_trace_coverage = (
        self_feed_trace.get("coverage") if isinstance(self_feed_trace.get("coverage"), dict) else {}
    )
    self_feed_trace_gap = (
        self_feed_trace.get("gap_equation") if isinstance(self_feed_trace.get("gap_equation"), dict) else {}
    )
    self_feed_full_retrace_summary = (
        self_feed_full_retrace.get("summary")
        if isinstance(self_feed_full_retrace.get("summary"), dict)
        else {}
    )
    self_feed_full_retrace_equation = (
        self_feed_full_retrace.get("reconciliation_equation")
        if isinstance(self_feed_full_retrace.get("reconciliation_equation"), dict)
        else {}
    )
    self_feed_duckdb_jsonl = (
        self_feed_duckdb_benchmark.get("jsonl_summary")
        if isinstance(self_feed_duckdb_benchmark.get("jsonl_summary"), dict)
        else {}
    )
    self_feed_duckdb_summary = (
        self_feed_duckdb_benchmark.get("duckdb_summary")
        if isinstance(self_feed_duckdb_benchmark.get("duckdb_summary"), dict)
        else {}
    )
    self_feed_duckdb_benchmarks = (
        self_feed_duckdb_benchmark.get("benchmarks")
        if isinstance(self_feed_duckdb_benchmark.get("benchmarks"), list)
        else []
    )
    self_feed_duckdb_gap = (
        self_feed_duckdb_benchmark.get("gap_scan")
        if isinstance(self_feed_duckdb_benchmark.get("gap_scan"), dict)
        else {}
    )
    self_feed_duckdb_gap_summary = (
        self_feed_duckdb_gap.get("duckdb_summary")
        if isinstance(self_feed_duckdb_gap.get("duckdb_summary"), dict)
        else {}
    )
    self_feed_duckdb_packet = (
        self_feed_duckdb_benchmark.get("classification_packet")
        if isinstance(self_feed_duckdb_benchmark.get("classification_packet"), dict)
        else {}
    )
    self_feed_duckdb_packet_classification = (
        self_feed_duckdb_packet.get("classification_summary")
        if isinstance(self_feed_duckdb_packet.get("classification_summary"), dict)
        else {}
    )
    self_feed_duckdb_packet_retrace = (
        self_feed_duckdb_packet.get("full_ledger_retrace")
        if isinstance(self_feed_duckdb_packet.get("full_ledger_retrace"), dict)
        else {}
    )
    self_feed_duckdb_packet_overlay = (
        self_feed_duckdb_packet.get("resolved_pnl_overlay")
        if isinstance(self_feed_duckdb_packet.get("resolved_pnl_overlay"), dict)
        else {}
    )
    self_feed_duckdb_packet_recommendation = (
        self_feed_duckdb_packet.get("recommendation")
        if isinstance(self_feed_duckdb_packet.get("recommendation"), dict)
        else {}
    )
    self_feed_duckdb_elapsed = {
        str(row.get("label")): row.get("elapsed_ms")
        for row in self_feed_duckdb_benchmarks
        if isinstance(row, dict)
    }
    guard_fill_audit_summary = (
        guard_fill_audit.get("summary") if isinstance(guard_fill_audit.get("summary"), dict) else {}
    )
    guard_fill_audit_equation = (
        guard_fill_audit.get("reconciliation_equation")
        if isinstance(guard_fill_audit.get("reconciliation_equation"), dict)
        else {}
    )
    guard_audit_full_b1_zero = (
        guard_fill_audit_summary.get("audit_scope") == "full_population"
        and int(guard_fill_audit_summary.get("b1_count") or 0) == 0
    )
    p0_guard_fill_recording_audit_required = (
        False
        if guard_audit_full_b1_zero
        else cash_ledger_summary.get("p0_guard_fill_recording_audit_required")
    )
    fill_toxicity_summary = (
        fill_toxicity.get("summary") if isinstance(fill_toxicity.get("summary"), dict) else {}
    )
    fill_toxicity_worst = (
        fill_toxicity.get("worst_groups") if isinstance(fill_toxicity.get("worst_groups"), list) else []
    )
    fill_toxicity_worst_row = (
        fill_toxicity_worst[0] if fill_toxicity_worst and isinstance(fill_toxicity_worst[0], dict) else {}
    )
    fill_loss_summary = (
        fill_loss_attribution.get("summary") if isinstance(fill_loss_attribution.get("summary"), dict) else {}
    )
    fill_loss_top = (
        fill_loss_attribution.get("top_loss_concentrations")
        if isinstance(fill_loss_attribution.get("top_loss_concentrations"), list)
        else []
    )
    window_time_current = (
        window_time_reject_attribution.get("current_guard_summary")
        if isinstance(window_time_reject_attribution.get("current_guard_summary"), dict)
        else {}
    )
    window_time_post_latest = (
        window_time_reject_attribution.get("post_latest_order_event_summary")
        if isinstance(window_time_reject_attribution.get("post_latest_order_event_summary"), dict)
        else {}
    )
    window_time_post_tripwire = (
        window_time_reject_attribution.get("post_tripwire_start_event_summary")
        if isinstance(window_time_reject_attribution.get("post_tripwire_start_event_summary"), dict)
        else {}
    )
    window_time_flow_money = (
        window_time_reject_attribution.get("flow_money_reconciliation")
        if isinstance(window_time_reject_attribution.get("flow_money_reconciliation"), dict)
        else {}
    )
    inventory_skip_trace = (
        active_set_starvation_packet.get("inventory_skip_lifecycle_trace")
        if isinstance(active_set_starvation_packet.get("inventory_skip_lifecycle_trace"), dict)
        else {}
    )
    active_set_starvation_summary = (
        active_set_starvation_packet.get("summary")
        if isinstance(active_set_starvation_packet.get("summary"), dict)
        else {}
    )
    inventory_skip_summary = (
        inventory_skip_lifecycle.get("summary")
        if isinstance(inventory_skip_lifecycle.get("summary"), dict)
        else {}
    )
    inventory_skip_source = (
        "active_set_starvation_packet"
        if inventory_skip_trace
        else "inventory_skip_lifecycle_trace_latest"
        if inventory_skip_summary
        else None
    )
    inventory_skip_counts = (
        inventory_skip_trace.get("skip_reason_counts_24h")
        if isinstance(inventory_skip_trace.get("skip_reason_counts_24h"), dict)
        else {}
    )
    inventory_skip_wallet_counts = (
        inventory_skip_trace.get("skip_reason_counts_24h_by_wallet")
        if isinstance(inventory_skip_trace.get("skip_reason_counts_24h_by_wallet"), dict)
        else inventory_skip_trace.get("skip_reason_counts_by_wallet_24h")
        if isinstance(inventory_skip_trace.get("skip_reason_counts_by_wallet_24h"), dict)
        else {}
    )
    inventory_skip_samples = (
        inventory_skip_trace.get("sample_traces")
        if isinstance(inventory_skip_trace.get("sample_traces"), list)
        else []
    )
    inventory_skip_c4 = (
        {
            "eligible_intents_24h": active_set_starvation_summary.get("eligible_intents_24h"),
            "late_suppressed_intents_24h": active_set_starvation_summary.get("late_suppressed_intents_24h"),
            "pass_to_late_transitions_24h": active_set_starvation_summary.get("pass_to_late_transitions_24h"),
            "pass_to_terminal_resnapshot_24h": active_set_starvation_summary.get("pass_to_terminal_resnapshot_24h"),
            "unique_intents_24h": active_set_starvation_summary.get("unique_intents_24h"),
        }
        if inventory_skip_trace and active_set_starvation_summary
        else inventory_skip_summary.get("c4")
        if isinstance(inventory_skip_summary.get("c4"), dict)
        else {}
    )
    toxicity_denylist_cells = (
        toxicity_denylist.get("cells") if isinstance(toxicity_denylist.get("cells"), list) else []
    )
    strategy_map_summary = strategy_map.get("summary") if isinstance(strategy_map.get("summary"), dict) else {}
    strategy_map_stale = (
        strategy_map.get("stale_defects") if isinstance(strategy_map.get("stale_defects"), list) else []
    )
    resource_utilization_defect = (
        resource_utilization.get("defect") if isinstance(resource_utilization.get("defect"), dict) else {}
    )
    decompiler_summary = (
        decompiler_intake.get("summary") if isinstance(decompiler_intake.get("summary"), dict) else {}
    )
    decompiler_selected = (
        decompiler_intake.get("selected_wallets")
        if isinstance(decompiler_intake.get("selected_wallets"), list)
        else []
    )
    decompiler_top = (
        decompiler_selected[0] if decompiler_selected and isinstance(decompiler_selected[0], dict) else {}
    )
    followability_summary = (
        followability.get("summary") if isinstance(followability.get("summary"), dict) else {}
    )
    followability_selected = (
        followability.get("selected_wallets") if isinstance(followability.get("selected_wallets"), list) else []
    )
    followability_top = (
        followability_selected[0] if followability_selected and isinstance(followability_selected[0], dict) else {}
    )
    full_universe_summary = (
        full_universe.get("summary") if isinstance(full_universe.get("summary"), dict) else {}
    )
    full_universe_top = (
        (full_universe.get("top_wallets") or [])[0]
        if isinstance(full_universe.get("top_wallets"), list) and full_universe.get("top_wallets")
        else {}
    )
    full_universe_top_replay = (
        full_universe_top.get("copy_replay") if isinstance(full_universe_top.get("copy_replay"), dict) else {}
    )
    full_universe_top_followability = (
        full_universe_top.get("followability") if isinstance(full_universe_top.get("followability"), dict) else {}
    )
    market_scan_summary = market_scan.get("summary") if isinstance(market_scan.get("summary"), dict) else {}
    market_scan_rate_limit = (
        market_scan.get("rate_limit_budget") if isinstance(market_scan.get("rate_limit_budget"), dict) else {}
    )
    market_scan_route_counts = (
        market_scan.get("route_class_counts") if isinstance(market_scan.get("route_class_counts"), dict) else {}
    )
    market_cohort_summary = (
        market_cohort_replay.get("summary") if isinstance(market_cohort_replay.get("summary"), dict) else {}
    )
    temporal_summary = (
        temporal_profitability.get("summary")
        if isinstance(temporal_profitability.get("summary"), dict)
        else {}
    )
    temporal_dow = (
        temporal_profitability.get("dow_weight_verification")
        if isinstance(temporal_profitability.get("dow_weight_verification"), dict)
        else {}
    )
    temporal_feed = (
        temporal_profitability.get("watch_tier_probe_feed")
        if isinstance(temporal_profitability.get("watch_tier_probe_feed"), dict)
        else {}
    )
    temporal_candidates = (
        temporal_feed.get("candidates") if isinstance(temporal_feed.get("candidates"), list) else []
    )
    temporal_top_candidate = (
        temporal_candidates[0] if temporal_candidates and isinstance(temporal_candidates[0], dict) else {}
    )
    source_active_replay_summary = (
        source_active_replay.get("summary") if isinstance(source_active_replay.get("summary"), dict) else {}
    )
    source_active_replay_criteria = (
        source_active_replay.get("criteria") if isinstance(source_active_replay.get("criteria"), dict) else {}
    )
    source_active_replay_results = (
        source_active_replay.get("results") if isinstance(source_active_replay.get("results"), list) else []
    )
    source_active_replay_stop_reasons: dict[str, int] = {}
    for row in source_active_replay_results:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("stop_reason") or "unknown")
        source_active_replay_stop_reasons[reason] = source_active_replay_stop_reasons.get(reason, 0) + 1
    manifest_files = (
        temporal_supplemental_manifest.get("supplemental_history_files")
        if isinstance(temporal_supplemental_manifest, dict)
        else []
    )
    if not isinstance(manifest_files, list):
        manifest_files = []
    winner_variation_summary = (
        winner_variation.get("summary") if isinstance(winner_variation.get("summary"), dict) else {}
    )
    winner_variation_parent = (
        winner_variation.get("parent_config") if isinstance(winner_variation.get("parent_config"), dict) else {}
    )
    winner_variation_epoch = (
        winner_variation.get("parent_epoch") if isinstance(winner_variation.get("parent_epoch"), dict) else {}
    )
    winner_variation_lanes = (
        winner_variation.get("lanes") if isinstance(winner_variation.get("lanes"), list) else []
    )
    winner_variation_best = next(
        (
            row
            for row in winner_variation_lanes
            if isinstance(row, dict)
            and row.get("lane_id") == winner_variation_summary.get("best_sibling_lane_id")
        ),
        {},
    )
    winner_variation_best_gate = (
        winner_variation_best.get("promotion_gate")
        if isinstance(winner_variation_best.get("promotion_gate"), dict)
        else {}
    )
    winner_variation_best_evidence = (
        winner_variation_best.get("evidence") if isinstance(winner_variation_best.get("evidence"), dict) else {}
    )
    temporal_probe_apply_summary = (
        temporal_probe_apply.get("summary")
        if isinstance(temporal_probe_apply.get("summary"), dict)
        else {}
    )
    temporal_probe_skip_rows = (
        temporal_probe_apply_summary.get("skipped_wallets")
        if isinstance(temporal_probe_apply_summary.get("skipped_wallets"), list)
        else []
    )
    temporal_probe_skipped_wallets = [
        str(row.get("wallet") or "")
        for row in temporal_probe_skip_rows
        if isinstance(row, dict) and row.get("wallet")
    ]
    watch_tier_shadow_summary = (
        watch_tier_shadow_ev.get("summary")
        if isinstance(watch_tier_shadow_ev.get("summary"), dict)
        else {}
    )
    watch_tier_shadow_wallets = (
        watch_tier_shadow_ev.get("wallets")
        if isinstance(watch_tier_shadow_ev.get("wallets"), list)
        else []
    )
    watch_tier_shadow_top = (
        watch_tier_shadow_wallets[0]
        if watch_tier_shadow_wallets and isinstance(watch_tier_shadow_wallets[0], dict)
        else {}
    )
    weekend_stakeout_ac05 = (
        weekend_stakeout.get("ac05_weekend_slice_audit")
        if isinstance(weekend_stakeout.get("ac05_weekend_slice_audit"), dict)
        else {}
    )
    weekend_stakeout_ac05_weekend = (
        weekend_stakeout_ac05.get("weekend")
        if isinstance(weekend_stakeout_ac05.get("weekend"), dict)
        else {}
    )
    weekend_stakeout_c03c = (
        weekend_stakeout.get("c03c_shadow_evidence")
        if isinstance(weekend_stakeout.get("c03c_shadow_evidence"), dict)
        else {}
    )
    weekend_stakeout_c03c_watch = (
        weekend_stakeout_c03c.get("watch_tier_shadow_ev")
        if isinstance(weekend_stakeout_c03c.get("watch_tier_shadow_ev"), dict)
        else {}
    )
    sub25_spot_summary = (
        sub25_spot_check.get("summary")
        if isinstance(sub25_spot_check.get("summary"), dict)
        else {}
    )
    btc5m_fleet_summary = (
        btc5m_fleet.get("summary") if isinstance(btc5m_fleet.get("summary"), dict) else {}
    )
    btc5m_fleet_top50 = (
        btc5m_fleet_summary.get("top50_matrix_coverage")
        if isinstance(btc5m_fleet_summary.get("top50_matrix_coverage"), dict)
        else {}
    )
    two_sided_summary = (
        two_sided_prime.get("summary") if isinstance(two_sided_prime.get("summary"), dict) else {}
    )
    two_sided_rows = (
        two_sided_prime.get("mechanism_rows") if isinstance(two_sided_prime.get("mechanism_rows"), list) else []
    )
    two_sided_top = two_sided_rows[0] if two_sided_rows and isinstance(two_sided_rows[0], dict) else {}
    morning_summary = (
        morning_table.get("summary") if isinstance(morning_table.get("summary"), dict) else {}
    )
    morning_top = (
        morning_summary.get("top_rank") if isinstance(morning_summary.get("top_rank"), dict) else {}
    )
    structural_scalp_summary = (
        structural_scalp_lane.get("summary")
        if isinstance(structural_scalp_lane.get("summary"), dict)
        else {}
    )
    structural_scalp_metrics = (
        structural_scalp_lane.get("metrics")
        if isinstance(structural_scalp_lane.get("metrics"), dict)
        else {}
    )
    structural_scalp_all_time = (
        structural_scalp_metrics.get("all_time")
        if isinstance(structural_scalp_metrics.get("all_time"), dict)
        else {}
    )
    structural_scalp_forward = (
        structural_scalp_metrics.get("forward")
        if isinstance(structural_scalp_metrics.get("forward"), dict)
        else {}
    )
    structural_scalp_live_gate = (
        structural_scalp_lane.get("live_gate")
        if isinstance(structural_scalp_lane.get("live_gate"), dict)
        else {}
    )
    e6db_loser_autopsy_summary = (
        e6db_loser_autopsy.get("summary")
        if isinstance(e6db_loser_autopsy.get("summary"), dict)
        else {}
    )
    successor_dossier_summary = (
        successor_dossier.get("summary")
        if isinstance(successor_dossier.get("summary"), dict)
        else {}
    )
    active_set_rotation_rows = (
        active_set_rotation_packet.get("ranked_candidates")
        if isinstance(active_set_rotation_packet.get("ranked_candidates"), list)
        else []
    )
    active_set_rotation_top = (
        active_set_rotation_rows[0]
        if active_set_rotation_rows and isinstance(active_set_rotation_rows[0], dict)
        else {}
    )
    active_set_rotation_quiet = (
        active_set_rotation_packet.get("quiet_clock")
        if isinstance(active_set_rotation_packet.get("quiet_clock"), dict)
        else {}
    )
    active_set_rotation_summary = {
        "status": active_set_rotation_packet.get("status"),
        "presumptive_target": active_set_rotation_packet.get("presumptive_target"),
        "presumptive_candidate_id": active_set_rotation_packet.get("presumptive_candidate_id"),
        "quiet_anchor_iso": active_set_rotation_quiet.get("anchor_iso"),
        "earliest_fire_iso": active_set_rotation_quiet.get("earliest_fire_iso"),
        "fires_now": active_set_rotation_quiet.get("fires_now"),
        "selected_wallet": active_set_rotation_packet.get("selected_wallet"),
        "selected_premerge_new_matching_events": active_set_rotation_packet.get(
            "selected_premerge_new_matching_events"
        ),
        "selected_latest_retained_matching_event_iso": active_set_rotation_packet.get(
            "selected_latest_retained_matching_event_iso"
        ),
        "top_fresh_matching_events_4h": active_set_rotation_top.get("fresh_matching_events_4h"),
        "top_fresh_rate_per_hour": active_set_rotation_top.get("fresh_matching_event_rate_per_hour"),
        "top_latency_p50_s": active_set_rotation_top.get("local_entry_latency_p50_s"),
        "top_post_fee_pnl_usd": active_set_rotation_top.get("routing_shadow_post_fee_pnl_usd"),
        "live_path_mutated": active_set_rotation_packet.get("live_path_mutated"),
    } if active_set_rotation_packet else {}
    active_set_post_rotation_summary = {
        "status": active_set_post_rotation_windows.get("status"),
        "target_wallet": active_set_post_rotation_windows.get("target_wallet"),
        "measured_windows_found": active_set_post_rotation_windows.get("measured_windows_found"),
        "dominant_skip_reason_distribution": active_set_post_rotation_windows.get(
            "dominant_skip_reason_distribution"
        ),
        "observed_age_histogram_metric": active_set_post_rotation_windows.get("observed_age_histogram_metric"),
        "observed_age_histogram": active_set_post_rotation_windows.get("observed_age_histogram"),
        "snapshot_observed_age_histogram": active_set_post_rotation_windows.get("snapshot_observed_age_histogram"),
        "submit_eligible_rows": active_set_post_rotation_windows.get("submit_eligible_rows"),
        "submitted_decision_time_observed_age_cap_violation_rows": active_set_post_rotation_windows.get(
            "submitted_decision_time_observed_age_cap_violation_rows"
        ),
        "si1_reopens": active_set_post_rotation_windows.get("si1_reopens"),
    } if active_set_post_rotation_windows else {}
    dispatch_allocator = (
        dispatch_throughput.get("allocator") if isinstance(dispatch_throughput.get("allocator"), dict) else {}
    )
    dispatch_model = (
        dispatch_throughput.get("dispatch_model")
        if isinstance(dispatch_throughput.get("dispatch_model"), dict)
        else {}
    )
    dispatch_api_budget = (
        dispatch_throughput.get("api_budget")
        if isinstance(dispatch_throughput.get("api_budget"), dict)
        else {}
    )
    dispatch_fairness = (
        dispatch_throughput.get("fairness")
        if isinstance(dispatch_throughput.get("fairness"), dict)
        else {}
    )
    clearance_summary = clearance_gaps.get("summary") if isinstance(clearance_gaps.get("summary"), dict) else {}
    ranked_clearance_rows = [
        row
        for row in ranked_clearance_packets.get("packets") or []
        if isinstance(row, dict)
    ]
    ranked_clearance_summary = (
        ranked_clearance_packets.get("summary")
        if isinstance(ranked_clearance_packets.get("summary"), dict)
        else {}
    )
    ready_shadow_summary = ready_shadow.get("summary") if isinstance(ready_shadow.get("summary"), dict) else {}
    ready_shadow_lanes = ready_shadow.get("lanes") if isinstance(ready_shadow.get("lanes"), list) else []
    next_ready_shadow_lane = ready_shadow_lanes[0] if ready_shadow_lanes and isinstance(ready_shadow_lanes[0], dict) else {}
    ready_shadow_paper_canary = next(
        (
            row
            for row in ready_shadow_lanes
            if isinstance(row, dict) and row.get("canary_path") == "CLEAR_TO_HOT_STANDBY_PAPER_CANARY"
        ),
        {},
    )
    hot_standby_ranked = (
        ready_shadow.get("hot_standby_ranked_candidates")
        if isinstance(ready_shadow.get("hot_standby_ranked_candidates"), list)
        else []
    )
    top_hot_standby = hot_standby_ranked[0] if hot_standby_ranked and isinstance(hot_standby_ranked[0], dict) else {}
    inventory_convergence_summary = (
        inventory_convergence_skip_lane.get("summary")
        if isinstance(inventory_convergence_skip_lane.get("summary"), dict)
        else {}
    )
    a689_hot_standby_summary = (
        a689_hot_standby_lane.get("summary")
        if isinstance(a689_hot_standby_lane.get("summary"), dict)
        else {}
    )
    ready_shadow_lanes = [
        row for row in ready_shadow.get("lanes") or [] if isinstance(row, dict)
    ]
    a689_wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
    successor_wallet = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    successor_shadow_lane = next(
        (row for row in ready_shadow_lanes if str(row.get("wallet") or "").lower() == successor_wallet),
        {},
    )
    successor_liveness_row = next(
        (
            row
            for row in hot_standby_source_liveness.get("rows") or []
            if isinstance(row, dict)
            and str(row.get("wallet") or row.get("source_wallet") or "").lower()
            == successor_wallet
        ),
        {},
    )
    successor_address_selection = (
        successor_liveness_row.get("address_selection")
        if isinstance(successor_liveness_row.get("address_selection"), dict)
        else {}
    )
    ready_shadow_cut = (
        ready_shadow.get("a689_82c8_cut")
        if isinstance(ready_shadow.get("a689_82c8_cut"), dict)
        else {}
    )
    a689_terminal_absent = not any(
        str(row.get("wallet") or "").lower() == a689_wallet for row in ready_shadow_lanes
    )
    successor_bound = bool(
        successor_shadow_lane
        and successor_shadow_lane.get("source_binding_status") == "WIRED"
        and successor_shadow_lane.get("paper_only") is True
        and successor_shadow_lane.get("live_orders_allowed") is False
    )
    a689_edge_price_reject = (
        a689_edge_transfer.get("price_reject_counterfactual")
        if isinstance(a689_edge_transfer.get("price_reject_counterfactual"), dict)
        else {}
    )
    a689_edge_price_replay = (
        a689_edge_price_reject.get("guard_replay")
        if isinstance(a689_edge_price_reject.get("guard_replay"), dict)
        else {}
    )
    a689_edge_watch_tier = (
        a689_edge_transfer.get("watch_tier_paper_lane")
        if isinstance(a689_edge_transfer.get("watch_tier_paper_lane"), dict)
        else {}
    )
    a689_edge_watch_replay = (
        a689_edge_watch_tier.get("current_history_guard_replay")
        if isinstance(a689_edge_watch_tier.get("current_history_guard_replay"), dict)
        else {}
    )
    a689_edge_summary = {
        "generated_at": a689_edge_transfer.get("generated_at"),
        "paper_only": a689_edge_transfer.get("paper_only"),
        "live_orders_allowed": a689_edge_transfer.get("live_orders_allowed"),
        "guard_thresholds": a689_edge_transfer.get("guard_thresholds")
        if isinstance(a689_edge_transfer.get("guard_thresholds"), dict)
        else {},
        "same_gate_verdict": a689_edge_transfer.get("same_gate_verdict")
        if isinstance(a689_edge_transfer.get("same_gate_verdict"), dict)
        else {},
        "verdict": a689_edge_transfer.get("verdict")
        if isinstance(a689_edge_transfer.get("verdict"), dict)
        else {},
        "price_reject_published_full": a689_edge_price_reject.get("published_full")
        if isinstance(a689_edge_price_reject.get("published_full"), dict)
        else {},
        "price_reject_full": a689_edge_price_replay.get("full")
        if isinstance(a689_edge_price_replay.get("full"), dict)
        else {},
        "price_reject_guard_eligible": a689_edge_price_replay.get("guard_eligible")
        if isinstance(a689_edge_price_replay.get("guard_eligible"), dict)
        else {},
        "price_reject_late_class_counts": a689_edge_price_replay.get("guard_late_class_counts")
        if isinstance(a689_edge_price_replay.get("guard_late_class_counts"), dict)
        else {},
        "watch_tier_published_hot_standby": a689_edge_watch_tier.get("published_hot_standby")
        if isinstance(a689_edge_watch_tier.get("published_hot_standby"), dict)
        else {},
        "watch_tier_current_full": a689_edge_watch_replay.get("full")
        if isinstance(a689_edge_watch_replay.get("full"), dict)
        else {},
        "watch_tier_guard_eligible": a689_edge_watch_replay.get("guard_eligible")
        if isinstance(a689_edge_watch_replay.get("guard_eligible"), dict)
        else {},
        "watch_tier_late_class_counts": a689_edge_watch_replay.get("guard_late_class_counts")
        if isinstance(a689_edge_watch_replay.get("guard_late_class_counts"), dict)
        else {},
    }
    prereg_missing = (
        experiment_preregistration.get("missing_required_ids")
        if isinstance(experiment_preregistration.get("missing_required_ids"), list)
        else []
    )
    dr_summary = dr_preflight.get("summary") if isinstance(dr_preflight.get("summary"), dict) else {}
    dr_snapshot_push = (
        dr_preflight.get("snapshot_push") if isinstance(dr_preflight.get("snapshot_push"), dict) else {}
    )
    latest_order = {}
    orders = live.get("orders") if isinstance(live.get("orders"), list) else []
    if orders:
        latest_order = orders[-1] if isinstance(orders[-1], dict) else {}
    latest_order_meta = (
        (latest_order.get("source_intent") or {}).get("metadata")
        if isinstance(latest_order.get("source_intent"), dict)
        else {}
    )
    latest_order_meta = latest_order_meta if isinstance(latest_order_meta, dict) else {}
    own_positions_summary = (
        own_positions.get("summary") if isinstance(own_positions.get("summary"), dict) else {}
    )
    own_positions_data_api = (
        own_positions.get("data_api") if isinstance(own_positions.get("data_api"), dict) else {}
    )
    alpha_decay_alpha = (
        alpha_decay_curve.get("alpha_decay")
        if isinstance(alpha_decay_curve.get("alpha_decay"), dict)
        else alpha_decay_curve
    )
    alpha_decay_alpha = alpha_decay_alpha if isinstance(alpha_decay_alpha, dict) else {}
    alpha_decay_rule = _alpha_decay_rule(alpha_decay_curve if isinstance(alpha_decay_curve, dict) else {})
    cli_versions_freshness = (
        cli_versions.get("freshness") if isinstance(cli_versions.get("freshness"), dict) else {}
    )
    cli_versions_tools = {}
    for tool in ("claude", "codex", "grok", "agy"):
        row = cli_versions_freshness.get(tool) if isinstance(cli_versions_freshness.get(tool), dict) else {}
        latest = row.get("latest") if isinstance(row.get("latest"), dict) else {}
        cli_versions_tools[tool] = {
            "installed": row.get("installed_version"),
            "latest": latest.get("latest_version"),
            "source": latest.get("source"),
            "status": row.get("status"),
            "stale": row.get("stale"),
        }
    model_runtime_evidence = _model_runtime_evidence(codex_dir)

    defects = _defect_lines(latest_status.get("body", ""), limit=8)
    direction_next = _direction_next_block(latest_direction.get("body", ""))
    direction_warnings = _compact_lines(latest_direction.get("body", ""), prefix="- warnings:", limit=2)
    latest_direction_material_summary = _direction_material_with_basis(latest_direction.get("body", ""))
    latest_direction_material = latest_direction_material_summary["lines"]
    live_members_today = [
        {
            "wallet": wallet,
            "orders": stats.get("orders"),
            "fills": stats.get("fills"),
            "resolved_fills": stats.get("resolved_fills"),
            "rejects": stats.get("rejects"),
            "pnl_usd": stats.get("pnl_usd"),
            "trigger_watch": score_member_trigger_watch.get(_norm_wallet(wallet), {}),
        }
        for wallet, stats in score_today_members.items()
        if isinstance(stats, dict) and int(stats.get("orders") or 0) > 0
    ]
    live_members_today.sort(key=lambda row: int(row.get("orders") or 0), reverse=True)
    admission_wave_overlay = dict(active_set_overlay)
    if current_admission_wave:
        admission_wave_overlay["latest_admission_wave"] = current_admission_wave
    active_set_admission_wave = _admission_wave_summary(
        admission_wave_overlay,
        guard_runtime_members,
        live_members_today,
        guard_active_runtime,
    )
    e6db_2000_probe_cap_cut = _e6db_2000_probe_cap_cut_summary(active_set_overlay)
    wave_gate_attribution_summary = _gate_artifact_summary(wave_gate_attribution, wave_gate_attribution_path)
    admitted_member_gate_3048_summary = _gate_artifact_summary(
        admitted_member_gate_3048,
        admitted_member_gate_3048_path,
    )
    wave_repair_addendum_summary = _wave_repair_addendum_summary(
        wave_repair_addendum,
        wave_repair_addendum_path,
    )
    order_flow_deadman_r1_attribution_summary = _deadman_r1_attribution_summary(
        order_flow_deadman_r1_attribution,
        order_flow_deadman_r1_attribution_path,
    )
    trade_executor_lane_attribution_summary = _trade_executor_lane_attribution_summary(
        trade_executor_lane_attribution,
        trade_executor_lane_attribution_path,
    )
    newer_direction_blocks = [
        {
            "heading": entry.get("heading", ""),
            "next_verbatim": _direction_next_block(entry.get("body", "")),
        }
        for entry in newer_directions
    ]
    recent_direction_blocks = [
        {
            "heading": entry.get("heading", ""),
            "next_verbatim": _direction_next_block(entry.get("body", "")),
            "warnings": _compact_lines(entry.get("body", ""), prefix="- warnings:", limit=1),
        }
        for entry in recent_directions
    ]
    newest_fable_direction = _latest_fable_direction(handoff_entries)
    latest_direction_ts = _entry_timestamp(latest_direction)
    newest_fable_direction_ts = _entry_timestamp(newest_fable_direction)
    direction_freshness_status = "FRESH"
    if latest_direction_ts is None or newest_fable_direction_ts is None:
        direction_freshness_status = "UNKNOWN_TS"
    elif latest_direction_ts < newest_fable_direction_ts or latest_direction.get("heading") != newest_fable_direction.get(
        "heading"
    ):
        direction_freshness_status = "STALE"
    digest_generated_at = _utc_now_iso()
    digest_generated_dt = datetime.fromisoformat(digest_generated_at.replace("Z", "+00:00"))
    newest_forward_lane_dt = max(
        (
            parsed
            for parsed in (
                _parse_utc_ts(bac25_forward_only_lane.get("generated_at")),
                _parse_utc_ts(wallet_951b_forward_only_lane.get("generated_at")),
                _parse_utc_ts(
                    wallet_82c8_8bb70201_forward_only_lane.get("generated_at")
                ),
                _parse_utc_ts(
                    wallet_82c8_fdd8af33_forward_only_lane.get("generated_at")
                ),
            )
            if parsed is not None
        ),
        default=None,
    )
    forward_lane_digest_lag_s = (
        round(
            max(0.0, (digest_generated_dt - newest_forward_lane_dt).total_seconds()),
            6,
        )
        if newest_forward_lane_dt is not None
        else None
    )
    guard_profile_generated_at = (
        guard_loop_profile.get("generated_at")
        or guard.get("generated_at")
    )
    guard_profile_generated_dt = _parse_utc_ts(guard_profile_generated_at)
    guard_profile_age_s = (
        round(max(0.0, (digest_generated_dt - guard_profile_generated_dt).total_seconds()), 6)
        if guard_profile_generated_dt is not None
        else None
    )
    guard_profile_freshness_slo_s = 600.0
    guard_profile_freshness_status = (
        "CURRENT"
        if guard_profile_age_s is not None
        and guard_profile_age_s <= guard_profile_freshness_slo_s
        else "STALE"
        if guard_profile_age_s is not None
        else "UNKNOWN"
    )
    guard_cache_generated_dt = _parse_utc_ts(guard_json_cache_evidence.get("generated_at"))
    guard_cache_age_s = (
        round(max(0.0, (digest_generated_dt - guard_cache_generated_dt).total_seconds()), 6)
        if guard_cache_generated_dt is not None
        else None
    )
    guard_cache_freshness_slo_s = 600.0
    guard_cache_freshness_status = (
        "CURRENT"
        if guard_cache_age_s is not None
        and guard_cache_age_s <= guard_cache_freshness_slo_s
        else "STALE"
        if guard_cache_age_s is not None
        else "UNKNOWN"
    )
    member_native_generated_dt = _parse_utc_ts(member_native_policy_uplift.get("generated_at"))
    member_native_output_age_s = (
        round(
            max(0.0, (digest_generated_dt - member_native_generated_dt).total_seconds()),
            3,
        )
        if member_native_generated_dt is not None
        else None
    )
    member_native_runner_pid = _launchctl_job_pid(
        "com.belavarga.polymarket.member-native-policy-uplift-paper"
    )
    top10_direct_generated_dt = _parse_utc_ts(
        top10_direct_clob_paper.get("service_cycle_completed_at")
        or top10_direct_clob_paper.get("updated_at")
    )
    top10_direct_output_age_s = (
        round(max(0.0, (digest_generated_dt - top10_direct_generated_dt).total_seconds()), 3)
        if top10_direct_generated_dt is not None
        else None
    )
    top10_direct_runner_pid = _launchctl_job_pid(
        "com.belavarga.polymarket.top10-direct-clob-paper"
    )
    cohort_alive_profitable = _cohort_replay_alive_profitable_summary(
        market_cohort_replay,
        fresh_flow_probe,
        now=digest_generated_dt,
    )
    commitments_overdue = _commitments_overdue_summary(root, now=digest_generated_dt)
    cash_residual_for_trend = dict(since_topup_cash_residual)
    if not cash_residual_for_trend.get("state_path") and scorecard_cash_residual.get("state_path"):
        cash_residual_for_trend["state_path"] = scorecard_cash_residual.get("state_path")
    if cash_residual_for_trend.get("residual_usd") is None and scorecard_cash_residual.get("residual_usd") is not None:
        cash_residual_for_trend["residual_usd"] = scorecard_cash_residual.get("residual_usd")
    day_pnl_basis = scorecard.get("day_pnl_basis") if isinstance(scorecard.get("day_pnl_basis"), dict) else {}
    taker_holdout_subbands = (
        taker_price_subband_holdout.get("subbands")
        if isinstance(taker_price_subband_holdout.get("subbands"), dict)
        else {}
    )
    taker_01a = (
        taker_holdout_subbands.get("01a_25_32")
        if isinstance(taker_holdout_subbands.get("01a_25_32"), dict)
        else {}
    )
    taker_01a_holdout = (
        taker_01a.get("chronological_holdout")
        if isinstance(taker_01a.get("chronological_holdout"), dict)
        else {}
    )
    canonical_pnl_truth = (
        scorecard.get("canonical_pnl_truth")
        if isinstance(scorecard.get("canonical_pnl_truth"), dict)
        else {}
    )
    canonical_by_day = (
        canonical_pnl_truth.get("by_day")
        if isinstance(canonical_pnl_truth.get("by_day"), dict)
        else {}
    )
    canonical_today = (
        canonical_by_day.get(digest_generated_dt.date().isoformat())
        if isinstance(canonical_by_day.get(digest_generated_dt.date().isoformat()), dict)
        else {}
    )
    live_guard_generation_verdict = generation_verdict(
        live_guard_restart_decision,
        now=digest_generated_dt,
    )
    loaded_generation_sha = (
        (live_guard_restart_decision.get("loaded_generation") or {}).get("sha256")
        if isinstance(live_guard_restart_decision.get("loaded_generation"), dict)
        else None
    )
    disk_generation_sha = (
        (live_guard_restart_decision.get("disk_generation") or {}).get("sha256")
        if isinstance(live_guard_restart_decision.get("disk_generation"), dict)
        else None
    )
    coverage_enforced_by_running_binary = bool(
        not live_guard_generation_verdict["stale"]
        and live_guard_restart_decision.get("generation_mismatch") is False
        and loaded_generation_sha
        and loaded_generation_sha == disk_generation_sha
    )
    daily_floor_gate_residency = _daily_floor_gate_residency(
        digest_generated_dt.date().isoformat(),
        canonical_today,
        loaded_generation_sha256=loaded_generation_sha,
        disk_generation_sha256=disk_generation_sha,
        generation_verdict=live_guard_generation_verdict,
    )
    score_total = _annotate_covered_rates(
        score_total,
        enforced=coverage_enforced_by_running_binary,
    )
    canonical_today = _annotate_covered_rates(
        canonical_today,
        enforced=coverage_enforced_by_running_binary,
    )
    lifetime_price_bands = _annotate_covered_rates(
        lifetime_price_bands,
        enforced=coverage_enforced_by_running_binary,
    )
    lifetime_money_subbands = _annotate_covered_rates(
        lifetime_money_subbands,
        enforced=coverage_enforced_by_running_binary,
    )
    taker_below_25 = (
        taker_holdout_subbands.get("00_below_25")
        if isinstance(taker_holdout_subbands.get("00_below_25"), dict)
        else taker_holdout_subbands.get("00_00_25")
        if isinstance(taker_holdout_subbands.get("00_00_25"), dict)
        else {}
    )
    taker_below_25_aggregate = (
        taker_below_25.get("aggregate")
        if isinstance(taker_below_25.get("aggregate"), dict)
        else {}
    )
    scorecard_volume = (
        scorecard.get("volume_kpi")
        if isinstance(scorecard.get("volume_kpi"), dict)
        else {}
    )
    scorecard_canonical_daily = (
        scorecard_volume.get("canonical_daily")
        if isinstance(scorecard_volume.get("canonical_daily"), dict)
        else {}
    )
    guard_live_execution = (
        guard.get("live_execution")
        if isinstance(guard.get("live_execution"), dict)
        else {}
    )
    guard_candidate_summary = (
        guard_live_execution.get("candidate_intent_summary")
        if isinstance(guard_live_execution.get("candidate_intent_summary"), dict)
        else {}
    )
    guard_best_ask_gate = (
        guard_candidate_summary.get("inventory_best_ask_gate")
        if isinstance(guard_candidate_summary.get("inventory_best_ask_gate"), dict)
        else {}
    )
    guard_blocker_taxonomy = (
        guard_best_ask_gate.get("blocker_taxonomy")
        if isinstance(guard_best_ask_gate.get("blocker_taxonomy"), dict)
        else {}
    )
    deadman_policy_choke = (
        order_flow_deadman.get("policy_choke")
        if isinstance(order_flow_deadman.get("policy_choke"), dict)
        else {}
    )
    deadman_source_roster_drought = (
        deadman_policy_choke.get("source_roster_drought")
        if isinstance(deadman_policy_choke.get("source_roster_drought"), dict)
        else {}
    )
    deadman_candidate_evidence_for_clock = (
        deadman_source_roster_drought.get("candidate_evidence")
        if isinstance(deadman_source_roster_drought.get("candidate_evidence"), dict)
        else {}
    )
    guard_code_identity = (
        guard.get("guard_code_identity")
        if isinstance(guard.get("guard_code_identity"), dict)
        else {}
    )
    goal_reachability = _goal_reachability(
        guard=guard,
        guard_caps=guard_caps,
        measured_band=taker_01a_holdout,
        measured_band_name="01a_25_32",
        measured_band_source=(
            "data/research/taker_price_subband_holdout_latest.json"
            "#subbands.01a_25_32.chronological_holdout"
        ),
        day_actual_pnl_usd=(
            day_pnl_basis.get("day_pnl_actual_basis")
            if day_pnl_basis.get("day_pnl_actual_basis") is not None
            else scorecard.get("day_pnl_actual_basis")
            if scorecard.get("day_pnl_actual_basis") is not None
            else score_total.get("pnl_usd")
        ),
        source_side_supply=qualified_pool_orderfilled_stakeout,
        roi_evidence={
            "subbands": taker_holdout_subbands,
            "development": (
                taker_01a.get("development")
                if isinstance(taker_01a.get("development"), dict)
                else {}
            ),
            "focus_robustness": (
                taker_price_subband_holdout.get("focus_robustness")
                if isinstance(taker_price_subband_holdout.get("focus_robustness"), dict)
                else {}
            ),
        },
        active_set_registry=active_set_overlay,
        realized_live_participation_windows=(
            scorecard_canonical_daily.get("windows_filled")
            if scorecard_canonical_daily.get("windows_filled") is not None
            else canonical_today.get("fills")
        ),
        resolved_live_fills_at_current_h=canonical_today.get("resolved_fills"),
        since_topup_actual_usd=since_topup.get("actual_delta_vs_baseline_usd"),
        restart_acceptance={
            "blocker_taxonomy_published": (
                "inventory_best_ask_below_ruled_entry_floor"
                in guard_blocker_taxonomy
            ),
            "post_restart_in_band_fill_rate_is_one": (
                canonical_today.get("in_band_fill_rate") == 1.0
            ),
            "first_hour_submitted_nonincrease": None,
        },
        in_band_fill_rate=canonical_today.get("in_band_fill_rate"),
        in_band_fill_rate_source=(
            "wallet_copy_daily_scorecard_current.json#canonical_pnl_truth.by_day."
            f"{digest_generated_dt.date().isoformat()}.in_band_fill_rate"
        ),
        closed_leg_roi_pct=taker_below_25_aggregate.get("post_fee_roi_pct"),
        closed_leg_roi_source=(
            "data/research/taker_price_subband_holdout_latest.json"
            "#subbands.00_below_25.aggregate.post_fee_roi_pct"
        ),
        realized_entry_events=[
            row
            for row in canonical_pnl_truth.get("events") or []
            if isinstance(row, dict)
            and row.get("day_utc") == digest_generated_dt.date().isoformat()
            and row.get("status") == "FILLED"
        ],
        floor_gate_enforced_fill_count=canonical_today.get(
            "floor_gate_enforced_fill_count"
        ),
        floor_gate_observation_started_at=guard_code_identity.get(
            "started_at_utc"
        ),
        eligible_intent_count=(
            deadman_candidate_evidence_for_clock.get("eligible_count")
            if deadman_candidate_evidence_for_clock.get("eligible_count") is not None
            else deadman_policy_choke.get("whole_runtime_eligible_intents")
        ),
        now=digest_generated_dt,
    )
    d16_entry_band_acceptance = _d16_entry_band_acceptance(
        live.get("orders") if isinstance(live.get("orders"), list) else [],
        activation_at=(
            live_guard_restart.get("generation_verdict", {}).get(
                "loaded_generation_started_at_utc"
            )
            if isinstance(live_guard_restart.get("generation_verdict"), dict)
            else None
        ),
        now=digest_generated_dt,
    )
    actual_basis_coverage = (
        day_pnl_basis.get("actual_basis_coverage")
        if isinstance(day_pnl_basis.get("actual_basis_coverage"), dict)
        else scorecard.get("actual_basis_coverage")
        if isinstance(scorecard.get("actual_basis_coverage"), dict)
        else {}
    )
    basis_split_decomposition = (
        scorecard.get("basis_split_decomposition")
        if isinstance(scorecard.get("basis_split_decomposition"), dict)
        else {}
    )
    per_window_pnl_histogram = (
        scorecard.get("per_window_pnl_histogram")
        if isinstance(scorecard.get("per_window_pnl_histogram"), dict)
        else {}
    )
    defense_tripwires = _defense_tripwires(
        score_total,
        since_topup,
        per_window_pnl_histogram,
        effective_stake_usd=goal_reachability.get("effective_max_order_usd") or 1.0,
    )
    defense_regret = (
        scorecard.get("defense_regret")
        if isinstance(scorecard.get("defense_regret"), dict)
        else {}
    )
    peer_active_idle_windows = (
        scorecard.get("peer_active_idle_windows")
        if isinstance(scorecard.get("peer_active_idle_windows"), dict)
        else {}
    )
    pipeline_slo_and_standby_readiness = (
        scorecard.get("pipeline_slo_and_standby_readiness")
        if isinstance(scorecard.get("pipeline_slo_and_standby_readiness"), dict)
        else {}
    )
    if isinstance(pipeline_slo_artifact, dict) and pipeline_slo_artifact.get(
        "pipeline_slo"
    ):
        pipeline_slo_and_standby_readiness = dict(pipeline_slo_artifact)
    pipeline_slo_generator_path = root / "scripts" / "report_pipeline_slo.py"
    artifact_generated_at = _parse_utc_ts(
        pipeline_slo_and_standby_readiness.get("generated_at")
    )
    artifact_age_h = (
        round(
            max(
                0.0,
                (digest_generated_dt - artifact_generated_at).total_seconds()
                / 3600.0,
            ),
            6,
        )
        if artifact_generated_at is not None
        else None
    )
    if artifact_age_h is not None:
        pipeline_slo_and_standby_readiness["artifact_age_h"] = artifact_age_h
    pipeline_slo_and_standby_readiness["freshness_artifact"] = str(
        pipeline_slo_artifact_path.relative_to(root)
    )
    pipeline_slo_and_standby_readiness["freshness_producer"] = str(
        pipeline_slo_generator_path.relative_to(root)
    )
    pipeline_slo_and_standby_readiness["producer_path"] = refresh_cadence_state.get(
        "producer_path"
    )
    pipeline_slo_and_standby_readiness["last_cycle_start_at"] = refresh_cadence_state.get(
        "last_cycle_start_at"
    )
    pipeline_slo_and_standby_readiness["cycle_period_s_observed"] = refresh_cadence_state.get(
        "cycle_period_s_observed"
    )
    pipeline_slo_and_standby_readiness["cycle_period_status"] = refresh_cadence_state.get(
        "cycle_period_status"
    )
    if artifact_age_h is not None and artifact_age_h > 2.0:
        pipeline_slo_and_standby_readiness["freshness_status"] = (
            "SLO_ARTIFACT_STALE_AGE"
        )
    elif (
        pipeline_slo_artifact_path.exists()
        and pipeline_slo_generator_path.exists()
        and pipeline_slo_artifact_path.stat().st_mtime_ns
        < pipeline_slo_generator_path.stat().st_mtime_ns
    ):
        pipeline_slo_and_standby_readiness["freshness_status"] = (
            "SLO_ARTIFACT_PREDATES_GENERATOR"
        )
    else:
        pipeline_slo_and_standby_readiness.pop("freshness_status", None)
    pipeline_slo_and_standby_readiness = _merge_authoritative_wide_standby(
        pipeline_slo_and_standby_readiness,
        wide_82c8_standby_binding,
        a689_hot_standby_summary,
    )
    weekend_day_probe = _weekend_day_probe(
        weekend_parity_packet,
        score_total,
        generated_at=digest_generated_at,
    )
    selected_day_pnl_basis = day_pnl_basis.get("primary_basis") or scorecard.get("cost_basis_source")
    deadman_fill_basis = _deadman_fill_basis_observation(order_flow_deadman)
    live_selection_surfaces = _live_selection_surfaces(
        order_flow_deadman,
        orderfilled_fast_lane,
    )
    if deadman_fill_basis.get("day_pnl_usd") is None and selected_day_pnl_basis == "response_filled_size_usd":
        scorecard_generated_at = scorecard.get("generated_at")
        score_pnl = _as_float(score_total.get("pnl_usd"))
        if score_pnl is not None and scorecard_generated_at:
            deadman_fill_basis = {
                "day_pnl_usd": score_pnl,
                "observed_at": scorecard_generated_at,
                "day_utc": scorecard.get("day_utc"),
            }
    day_pnl_basis_reconciliation = _day_pnl_basis_reconciliation(
        selected_day_pnl=score_total.get("pnl_usd"),
        selected_basis=selected_day_pnl_basis,
        selected_resolved_fills=score_total.get("resolved_fills"),
        selected_day_utc=scorecard.get("day_utc"),
        scorecard_text_day_pnl=scorecard_text_truth,
        deadman_fill_basis_day_pnl=deadman_fill_basis.get("day_pnl_usd"),
        deadman_fill_basis_observed_at=deadman_fill_basis.get("observed_at"),
        deadman_fill_basis_day_utc=deadman_fill_basis.get("day_utc"),
    )
    cash_residual_for_trend.setdefault(
        "basis",
        selected_day_pnl_basis or "unknown",
    )
    cash_residual_for_trend.setdefault("writer", "scripts/update_state_digest.py")
    cash_diff_residual_trend = _append_cash_diff_residual_trend(
        root,
        cash_residual_for_trend,
        generated_at=digest_generated_at,
    )
    heartbeat_ledger_delta = _heartbeat_ledger_delta(
        live,
        latest_status,
        current_day_pnl_usd=score_total.get("pnl_usd"),
        current_since_topup_actual_usd=since_topup.get("actual_delta_vs_baseline_usd"),
    )
    wide_heartbeat_watch = _wide_heartbeat_watch_summary(
        rtds_observation_watermarks,
        wide_fingerprint_evidence,
        now=digest_generated_dt,
        freeze_resolution_accelerator=freeze_resolution_accelerator,
        order_flow_deadman=order_flow_deadman,
    )
    flow_episode_digest = _flow_episode_summary(
        data_dir,
        digest_generated_dt,
        order_flow_deadman,
    )
    deadman_checked_at = _parse_utc_ts(order_flow_deadman.get("checked_at"))
    deadman_policy_choke = (
        order_flow_deadman.get("policy_choke")
        if isinstance(order_flow_deadman.get("policy_choke"), dict)
        else {}
    )
    deadman_source_roster_drought = (
        deadman_policy_choke.get("source_roster_drought")
        if isinstance(deadman_policy_choke.get("source_roster_drought"), dict)
        else {}
    )
    deadman_direct_source = (
        deadman_source_roster_drought.get("direct_source")
        if isinstance(deadman_source_roster_drought.get("direct_source"), dict)
        else {}
    )
    deadman_candidate_evidence = (
        deadman_source_roster_drought.get("candidate_evidence")
        if isinstance(
            deadman_source_roster_drought.get("candidate_evidence"), dict
        )
        else {}
    )
    deadman_digest_lag_s = (
        round(max(0.0, (digest_generated_dt - deadman_checked_at).total_seconds()), 6)
        if deadman_checked_at is not None
        else None
    )
    previous_deadman = (
        previous_digest.get("order_flow_deadman")
        if isinstance(previous_digest.get("order_flow_deadman"), dict)
        else {}
    )
    current_actuator = (
        deadman_policy_choke.get("actuator")
        if isinstance(deadman_policy_choke.get("actuator"), dict)
        else {}
    )
    actuator_candidate_evidence = (
        current_actuator.get("candidate_evidence")
        if isinstance(current_actuator.get("candidate_evidence"), dict)
        else {}
    )
    if actuator_candidate_evidence:
        deadman_candidate_evidence = actuator_candidate_evidence
    inflight_lost_identity = (
        deadman_candidate_evidence.get("inflight_observation_lost_eligibility")
        if isinstance(
            deadman_candidate_evidence.get("inflight_observation_lost_eligibility"),
            dict,
        )
        else {}
    )
    inflight_lost_row = next(
        (
            {
                "wallet": row.get("wallet"),
                "wide_policy_fingerprint": row.get("wide_policy_fingerprint"),
                "eligible": row.get("eligible"),
                "evidence_deficits": row.get("evidence_deficits") or [],
                "fresh_own_source_buy_rows_30m": row.get(
                    "fresh_own_source_buy_rows_30m"
                ),
                "f2_evaluated_copyable": row.get("f2_evaluated_copyable"),
                "direct_source": row.get("direct_source") or {},
            }
            for row in deadman_candidate_evidence.get("rows") or []
            if isinstance(row, dict)
            and row.get("wallet") == inflight_lost_identity.get("wallet")
            and row.get("wide_policy_fingerprint")
            == inflight_lost_identity.get("wide_policy_fingerprint")
        ),
        None,
    )
    walk_forward_refusal_frontier = _walk_forward_refusal_frontier(
        current_actuator,
        previous_deadman,
        generated_at=digest_generated_at,
    )
    deadman_stale_across_transition = bool(
        (deadman_digest_lag_s is not None and deadman_digest_lag_s > 60.0)
        or (
            previous_deadman.get("status") is not None
            and previous_deadman.get("status") != order_flow_deadman.get("status")
        )
    )

    digest = {
        "schema_version": 1,
        "kind": "agent_state_digest",
        "flow_stage": "SELF-DEV",
        "generated_at": digest_generated_at,
        "live_selection_surfaces": live_selection_surfaces,
        "forward_lane_digest_lag_s": forward_lane_digest_lag_s,
        "forward_lane_digest_lag_budget_s": 300.0,
        "forward_lane_digest_lag_status": (
            "PASS"
            if forward_lane_digest_lag_s is not None
            and forward_lane_digest_lag_s <= 300.0
            else "OVER_BUDGET"
            if forward_lane_digest_lag_s is not None
            else "NO_FORWARD_LANE_TIMESTAMP"
        ),
        "forward_lane_digest_generated_at": digest_generated_at,
        "forward_lane_newest_generated_at": (
            newest_forward_lane_dt.isoformat().replace("+00:00", "Z")
            if newest_forward_lane_dt is not None
            else None
        ),
        "validation_mode": "SHADOW_READ_BOTH_FULL_CONTEXT_AND_DIGEST",
        "order136d_early_entry": order136d_early_entry
        if isinstance(order136d_early_entry, dict)
        else {},
        "frozen_fingerprint_f2_prewarm": frozen_fingerprint_f2_prewarm
        if isinstance(frozen_fingerprint_f2_prewarm, dict)
        else {},
        "freeze_resolution_accelerator": freeze_resolution_accelerator
        if isinstance(freeze_resolution_accelerator, dict)
        else {},
        "wide_direct_admissible_frontier": wide_direct_admissible_frontier
        if isinstance(wide_direct_admissible_frontier, dict)
        else {},
        "order147_seat_feedstock": order147_seat_feedstock
        if isinstance(order147_seat_feedstock, dict)
        else {},
        "order148_seated_fill_dispositions": order148_seated_fill_dispositions
        if isinstance(order148_seated_fill_dispositions, dict)
        else {},
        "order149_rotation_qualification": order149_rotation_qualification
        if isinstance(order149_rotation_qualification, dict)
        else {},
        "order149_token_metadata_backfill": order149_token_metadata_backfill
        if isinstance(order149_token_metadata_backfill, dict)
        else {},
        "order149_gamma_metadata_recovery": order149_gamma_metadata_recovery
        if isinstance(order149_gamma_metadata_recovery, dict)
        else {},
        "order149_depth_at_size": order149_depth_at_size
        if isinstance(order149_depth_at_size, dict)
        else {},
        "order150_window_supply_attribution": order150_window_supply_attribution
        if isinstance(order150_window_supply_attribution, dict)
        else {},
        "order150_joint_supply_size_projection": order150_joint_supply_size_projection
        if isinstance(order150_joint_supply_size_projection, dict)
        else {},
        "order151_bf337_fading_adjudication": order151_bf337_fading_adjudication
        if isinstance(order151_bf337_fading_adjudication, dict)
        else {},
        "order151_fee_bps": order151_fee_bps
        if isinstance(order151_fee_bps, dict)
        else {},
        "wide_all_pass_seat_path": wide_all_pass_seat_path
        if isinstance(wide_all_pass_seat_path, dict)
        else {},
        "wide_ee3f_venue_reachable_diagnosis": wide_ee3f_venue_diagnosis
        if isinstance(wide_ee3f_venue_diagnosis, dict)
        else {},
        "wide_951b_concentration_diagnosis": wide_951b_concentration_diagnosis
        if isinstance(wide_951b_concentration_diagnosis, dict)
        else {},
        "wide_order7a_alpha_causal_reanchor": wide_order7a_alpha_causal_reanchor
        if isinstance(wide_order7a_alpha_causal_reanchor, dict)
        else {},
        "wide_order7b_metadata_diagnosis": wide_order7b_metadata_diagnosis
        if isinstance(wide_order7b_metadata_diagnosis, dict)
        else {},
        "order127_measured_seat_distance": order127_measured_seat_distance
        if isinstance(order127_measured_seat_distance, dict)
        else {},
        "order128_fastest_lawful_path": order128_fastest_lawful_path
        if isinstance(order128_fastest_lawful_path, dict)
        else {},
        "order128_manifest_binding": order128_manifest_binding,
        "wide_order6_gen2_retarget": wide_order6_gen2_retarget
        if isinstance(wide_order6_gen2_retarget, dict)
        else {},
        "wide_wallet_terminal_breakdown": wide_wallet_terminal_breakdown
        if isinstance(wide_wallet_terminal_breakdown, dict)
        else {},
        "resolved_tape_gap_closure": resolved_tape_gap_closure
        if isinstance(resolved_tape_gap_closure, dict)
        else {},
        "wide_heartbeat_watch": wide_heartbeat_watch,
        "flow_episodes": flow_episode_digest,
        "cross_exchange_probability_edge": {
            "status": cross_exchange_probability.get("status"),
            "paper_only": cross_exchange_probability.get("paper_only"),
            "orders_submitted": cross_exchange_probability.get("orders_submitted"),
            "frozen_model": cross_exchange_probability.get("frozen_model")
            if isinstance(cross_exchange_probability.get("frozen_model"), dict)
            else {},
            "current_terminal": cross_exchange_probability.get("current_terminal")
            if isinstance(cross_exchange_probability.get("current_terminal"), dict)
            else {},
            "prospective": cross_exchange_probability.get("prospective_executable_book")
            if isinstance(cross_exchange_probability.get("prospective_executable_book"), dict)
            else {},
            "promotion_gate": cross_exchange_probability.get("promotion_gate")
            if isinstance(cross_exchange_probability.get("promotion_gate"), dict)
            else {},
            "live_actuator": cross_exchange_live_actuator
            if isinstance(cross_exchange_live_actuator, dict)
            else {},
            "live_method_supply": live_method_supply
            if isinstance(live_method_supply, dict)
            else {},
            "delayed_offset_park": delayed_offset_park
            if isinstance(delayed_offset_park, dict)
            else {},
            "multivenue_residual_matrix": {
                "generated_at": multivenue_residual_matrix.get("generated_at"),
                "status": multivenue_residual_matrix.get("status"),
                "generation_checksum": multivenue_residual_matrix.get(
                    "generation_checksum"
                ),
                "synchronized_venue_clocks": multivenue_residual_matrix.get(
                    "synchronized_venue_clocks"
                ),
                "consensus_cell_count": multivenue_residual_matrix.get(
                    "consensus_cell_count"
                ),
                "residual_lane_slots": multivenue_residual_matrix.get(
                    "residual_lane_slots"
                ),
                "residual_sibling_count": multivenue_residual_matrix.get(
                    "residual_sibling_count"
                ),
                "cell_count": multivenue_residual_matrix.get("cell_count"),
                "paper_only": multivenue_residual_matrix.get("paper_only"),
                "live_orders_allowed": multivenue_residual_matrix.get(
                    "live_orders_allowed"
                ),
                "selector_status": promoted_cell_selector.get("status"),
                "selected": promoted_cell_selector.get("selected"),
            },
            "complete_set_paired_maker": {
                "generated_at": complete_set_paired_maker.get("generated_at"),
                "status": complete_set_paired_maker.get("status"),
                "generation_checksum": complete_set_paired_maker.get(
                    "generation_checksum"
                ),
                "complete_liveness_window_starts_s": complete_set_paired_maker.get(
                    "complete_liveness_window_starts_s"
                ),
                "completed_liveness_windows": complete_set_paired_maker.get(
                    "completed_liveness_windows"
                ),
                "positive_edge_intents": complete_set_paired_maker.get(
                    "positive_edge_intents"
                ),
                "blocker_taxonomy": complete_set_paired_maker.get(
                    "blocker_taxonomy"
                ),
                "stop_writer": complete_set_paired_maker.get("stop_writer"),
                "promoted_cell_selector": complete_set_paired_maker.get(
                    "promoted_cell_selector"
                ),
                "paper_only": complete_set_paired_maker.get("paper_only"),
                "live_orders_allowed": complete_set_paired_maker.get(
                    "live_orders_allowed"
                ),
            },
            "complete_set_split_sell": {
                "generated_at": complete_set_split_sell.get("generated_at"),
                "status": complete_set_split_sell.get("status"),
                "generation_checksum": complete_set_split_sell.get(
                    "generation_checksum"
                ),
                "complete_liveness_window_starts_s": complete_set_split_sell.get(
                    "complete_liveness_window_starts_s"
                ),
                "completed_liveness_windows": (
                    len(complete_set_split_sell.get("complete_liveness_window_starts_s") or [])
                    if complete_set_split_sell.get("complete_liveness_window_starts_s")
                    else complete_set_split_sell.get("completed_liveness_windows")
                ),
                "positive_intent_cycles": complete_set_split_sell.get(
                    "positive_edge_intents"
                ),
                "resolved_cycles": (
                    (complete_set_split_sell.get("attribution_funnel") or {}).get(
                        "resolved_selector_cells"
                    )
                    if isinstance(
                        complete_set_split_sell.get("attribution_funnel"), dict
                    )
                    else None
                ),
                "realized_post_cost_pnl_usd": complete_set_split_sell.get(
                    "realized_post_cost_pnl_usd"
                ),
                "gate_checks": complete_set_split_sell.get("gate_checks"),
                "stop_writer": complete_set_split_sell.get("stop_writer"),
                "paper_only": complete_set_split_sell.get("paper_only"),
                "live_orders_allowed": complete_set_split_sell.get(
                    "live_orders_allowed"
                ),
            },
            "book_shock_reversion": {
                "generated_at": book_shock_reversion.get("generated_at"),
                "status": book_shock_reversion.get("status"),
                "generation_checksum": book_shock_reversion.get(
                    "generation_checksum"
                ),
                "completed_windows": book_shock_reversion.get(
                    "completed_windows"
                ),
                "positive_edge_intents": book_shock_reversion.get(
                    "positive_edge_intents"
                ),
                "resolved_orders": book_shock_reversion.get("resolved_orders"),
                "post_fee_pnl_usd": book_shock_reversion.get(
                    "post_fee_pnl_usd"
                ),
                "gate_checks": book_shock_reversion.get("gate_checks"),
                "stop_writer": book_shock_reversion.get("stop_writer"),
                "rung_c_status": rung_c_no_target.get("status"),
                "released_slot_occupant": rung_c_no_target.get(
                    "released_slot_occupant"
                ),
                "paper_only": book_shock_reversion.get("paper_only"),
                "live_orders_allowed": book_shock_reversion.get(
                    "live_orders_allowed"
                ),
            },
            "queue_hazard_maker": {
                "generated_at": queue_hazard_maker.get("generated_at"),
                "status": queue_hazard_maker.get("status"),
                "generation_checksum": queue_hazard_maker.get(
                    "generation_checksum"
                ),
                "completed_windows": queue_hazard_maker.get(
                    "completed_windows"
                ),
                "positive_edge_intents": queue_hazard_maker.get(
                    "positive_edge_intents"
                ),
                "genuine_queue_fills": queue_hazard_maker.get(
                    "genuine_queue_fills"
                ),
                "resolved_orders": queue_hazard_maker.get("resolved_orders"),
                "post_cost_pnl_usd": queue_hazard_maker.get(
                    "post_cost_pnl_usd"
                ),
                "terminal_reconciliation": queue_hazard_maker.get(
                    "terminal_reconciliation"
                ),
                "gate_checks": queue_hazard_maker.get("gate_checks"),
                "stop_writer": queue_hazard_maker.get("stop_writer"),
                "paper_only": queue_hazard_maker.get("paper_only"),
                "live_orders_allowed": queue_hazard_maker.get(
                    "live_orders_allowed"
                ),
            },
            "native_aggressor_sweep": {
                "generated_at": native_aggressor_sweep.get("generated_at"),
                "status": native_aggressor_sweep.get("status"),
                "generation_checksum": native_aggressor_sweep.get(
                    "generation_checksum"
                ),
                "model_checksum": (
                    native_aggressor_sweep.get("frozen_model") or {}
                ).get("checksum"),
                "completed_windows": native_aggressor_sweep.get(
                    "completed_windows"
                ),
                "positive_edge_intents": native_aggressor_sweep.get(
                    "positive_edge_intents"
                ),
                "paper_fills": native_aggressor_sweep.get("paper_fills"),
                "resolved_orders": native_aggressor_sweep.get("resolved_orders"),
                "post_cost_pnl_usd": native_aggressor_sweep.get(
                    "post_cost_pnl_usd"
                ),
                "terminal_reconciliation": native_aggressor_sweep.get(
                    "terminal_reconciliation"
                ),
                "gate_checks": native_aggressor_sweep.get("gate_checks"),
                "stop_writer": native_aggressor_sweep.get("stop_writer"),
                "paper_only": native_aggressor_sweep.get("paper_only"),
                "live_orders_allowed": native_aggressor_sweep.get(
                    "live_orders_allowed"
                ),
            },
            "native_complement_lead_lag": {
                "generated_at": native_complement_lead_lag.get("generated_at"),
                "status": native_complement_lead_lag.get("status"),
                "generation_checksum": native_complement_lead_lag.get(
                    "generation_checksum"
                ),
                "completed_windows": native_complement_lead_lag.get(
                    "completed_windows"
                ),
                "positive_edge_intents": native_complement_lead_lag.get(
                    "positive_edge_intents"
                ),
                "paper_fills": native_complement_lead_lag.get("paper_fills"),
                "resolved_orders": native_complement_lead_lag.get(
                    "resolved_orders"
                ),
                "post_cost_pnl_usd": native_complement_lead_lag.get(
                    "post_cost_pnl_usd"
                ),
                "terminal_reconciliation": native_complement_lead_lag.get(
                    "terminal_reconciliation"
                ),
                "legacy_arbiter_publication": native_complement_lead_lag.get(
                    "legacy_arbiter_publication"
                ),
                "gate_checks": native_complement_lead_lag.get("gate_checks"),
                "stop_writer": native_complement_lead_lag.get("stop_writer"),
                "paper_only": native_complement_lead_lag.get("paper_only"),
                "live_orders_allowed": native_complement_lead_lag.get(
                    "live_orders_allowed"
                ),
            },
            "polymarket_cross_asset_leader_lag": {
                "generated_at": polymarket_cross_asset_leader_lag.get(
                    "generated_at"
                ),
                "status": polymarket_cross_asset_leader_lag.get("status"),
                "generation_checksum": polymarket_cross_asset_leader_lag.get(
                    "generation_checksum"
                ),
                "completed_windows": polymarket_cross_asset_leader_lag.get(
                    "completed_windows"
                ),
                "positive_edge_intents": polymarket_cross_asset_leader_lag.get(
                    "positive_edge_intents"
                ),
                "paper_fills": polymarket_cross_asset_leader_lag.get(
                    "paper_fills"
                ),
                "resolved_orders": polymarket_cross_asset_leader_lag.get(
                    "resolved_orders"
                ),
                "post_cost_pnl_usd": polymarket_cross_asset_leader_lag.get(
                    "post_cost_pnl_usd"
                ),
                "measured_integrity": polymarket_cross_asset_leader_lag.get(
                    "measured_integrity"
                ),
                "terminal_reconciliation": polymarket_cross_asset_leader_lag.get(
                    "terminal_reconciliation"
                ),
                "legacy_arbiter_publication": polymarket_cross_asset_leader_lag.get(
                    "legacy_arbiter_publication"
                ),
                "gate_checks": polymarket_cross_asset_leader_lag.get(
                    "gate_checks"
                ),
                "stop_writer": polymarket_cross_asset_leader_lag.get(
                    "stop_writer"
                ),
                "paper_only": polymarket_cross_asset_leader_lag.get(
                    "paper_only"
                ),
                "live_orders_allowed": polymarket_cross_asset_leader_lag.get(
                    "live_orders_allowed"
                ),
            },
            "polymarket_first_leader_cross_asset_lag": {
                key: polymarket_first_leader_cross_asset_lag.get(key)
                for key in (
                    "generated_at",
                    "status",
                    "generation_checksum",
                    "completed_windows",
                    "positive_edge_intents",
                    "paper_fills",
                    "resolved_orders",
                    "post_cost_pnl_usd",
                    "measured_integrity",
                    "terminal_reconciliation",
                    "legacy_arbiter_publication",
                    "gate_checks",
                    "stop_writer",
                    "paper_only",
                    "live_orders_allowed",
                )
            },
            "native_signed_tape_imbalance_stale_ask": {
                key: native_signed_tape_imbalance_stale_ask.get(key)
                for key in (
                    "generated_at",
                    "status",
                    "generation_checksum",
                    "completed_windows",
                    "positive_edge_intents",
                    "paper_fills",
                    "resolved_orders",
                    "post_cost_pnl_usd",
                    "measured_integrity",
                    "terminal_reconciliation",
                    "legacy_arbiter_publication",
                    "gate_checks",
                    "stop_writer",
                    "paper_only",
                    "live_orders_allowed",
                )
            },
            "native_l2_microprice_displacement_stale_ask": {
                key: native_l2_microprice_displacement_stale_ask.get(key)
                for key in (
                    "generated_at",
                    "status",
                    "generation_checksum",
                    "completed_windows",
                    "positive_edge_intents",
                    "paper_fills",
                    "resolved_orders",
                    "post_cost_pnl_usd",
                    "measured_integrity",
                    "terminal_reconciliation",
                    "legacy_arbiter_publication",
                    "gate_checks",
                    "stop_writer",
                    "paper_only",
                    "live_orders_allowed",
                )
            },
            "native_l2_tob_pressure_imbalance": {
                key: native_l2_tob_pressure_imbalance.get(key)
                for key in (
                    "generated_at",
                    "status",
                    "generation_checksum",
                    "completed_windows",
                    "positive_edge_intents",
                    "paper_fills",
                    "resolved_orders",
                    "post_cost_pnl_usd",
                    "measured_integrity",
                    "terminal_reconciliation",
                    "legacy_arbiter_publication",
                    "gate_checks",
                    "stop_writer",
                    "paper_only",
                    "live_orders_allowed",
                )
            },
            "native_l2_cross_outcome_parity_stale_ask": {
                key: native_l2_cross_outcome_parity_stale_ask.get(key)
                for key in (
                    "generated_at",
                    "status",
                    "generation_checksum",
                    "completed_windows",
                    "positive_edge_intents",
                    "paper_fills",
                    "resolved_orders",
                    "post_cost_pnl_usd",
                    "measured_integrity",
                    "terminal_reconciliation",
                    "legacy_arbiter_publication",
                    "gate_checks",
                    "stop_writer",
                    "paper_only",
                    "live_orders_allowed",
                )
            },
            "native_l2_depth_weighted_microprice_parity_stale_ask": {
                key: native_l2_depth_weighted_microprice_parity_stale_ask.get(key)
                for key in (
                    "generated_at",
                    "status",
                    "generation_checksum",
                    "completed_windows",
                    "positive_edge_intents",
                    "paper_fills",
                    "resolved_orders",
                    "post_cost_pnl_usd",
                    "measured_integrity",
                    "terminal_reconciliation",
                    "legacy_arbiter_publication",
                    "gate_checks",
                    "stop_writer",
                    "paper_only",
                    "live_orders_allowed",
                )
            },
            "native_l2_complement_bid_support_parity_stale_ask": {
                key: native_l2_complement_bid_support_parity_stale_ask.get(key)
                for key in (
                    "generated_at",
                    "status",
                    "generation_checksum",
                    "completed_windows",
                    "positive_edge_intents",
                    "paper_fills",
                    "resolved_orders",
                    "post_cost_pnl_usd",
                    "measured_integrity",
                    "terminal_reconciliation",
                    "legacy_arbiter_publication",
                    "gate_checks",
                    "stop_writer",
                    "paper_only",
                    "live_orders_allowed",
                )
            },
            "native_l2_complement_ask_cap_parity_stale_ask": {
                key: native_l2_complement_ask_cap_parity_stale_ask.get(key)
                for key in (
                    "generated_at",
                    "status",
                    "generation_checksum",
                    "completed_windows",
                    "positive_edge_intents",
                    "paper_fills",
                    "resolved_orders",
                    "post_cost_pnl_usd",
                    "measured_integrity",
                    "terminal_reconciliation",
                    "legacy_arbiter_publication",
                    "gate_checks",
                    "stop_writer",
                    "paper_only",
                    "live_orders_allowed",
                )
            },
            "current_f1_f4_fallout": {
                "generated_at": current_f1_f4_fallout.get("generated_at"),
                "status": current_f1_f4_fallout.get("status"),
                "candidate_count": current_f1_f4_fallout.get("candidate_count"),
                "eligible_count": current_f1_f4_fallout.get("eligible_count"),
                "refusal_counts": current_f1_f4_fallout.get("refusal_counts"),
                "active_temporal_join_defect_found": current_f1_f4_fallout.get(
                    "active_temporal_join_defect_found"
                ),
            },
        },
        "live_flow_incident_foreground": {
            "status": order_flow_deadman.get("status"),
            "can_trade": order_flow_deadman.get("can_trade"),
            "accepted_order_idle_s": (
                (order_flow_deadman.get("raw_accepted_order_deadman") or {}).get(
                    "accepted_order_idle_s"
                )
                if isinstance(order_flow_deadman.get("raw_accepted_order_deadman"), dict)
                else order_flow_deadman.get("accepted_order_idle_s")
            ),
            "consecutive_incidents": order_flow_deadman.get("consecutive_incidents"),
            "selected_wallet": (
                (order_flow_deadman.get("policy_choke") or {}).get("selected_wallet")
                if isinstance(order_flow_deadman.get("policy_choke"), dict)
                else None
            ),
            "selected_fresh_source_rows": (
                (order_flow_deadman.get("policy_choke") or {}).get(
                    "selected_fresh_source_rows"
                )
                if isinstance(order_flow_deadman.get("policy_choke"), dict)
                else None
            ),
            "selected_eligible_intents": (
                (order_flow_deadman.get("policy_choke") or {}).get(
                    "selected_eligible_intents"
                )
                if isinstance(order_flow_deadman.get("policy_choke"), dict)
                else None
            ),
            "selected_guard_submit_attempts": (
                (order_flow_deadman.get("policy_choke") or {}).get(
                    "selected_guard_submit_attempts"
                )
                if isinstance(order_flow_deadman.get("policy_choke"), dict)
                else None
            ),
            "selected_accepted_orders": (
                (order_flow_deadman.get("policy_choke") or {}).get(
                    "selected_accepted_orders"
                )
                if isinstance(order_flow_deadman.get("policy_choke"), dict)
                else None
            ),
            "candidate_supply": {
                "regime": deadman_candidate_evidence.get("regime"),
                "candidate_count": deadman_candidate_evidence.get(
                    "candidate_count"
                ),
                "eligible_count": deadman_candidate_evidence.get(
                    "eligible_count"
                ),
                "refusal_counts": deadman_candidate_evidence.get(
                    "refusal_counts"
                )
                or {},
                "supply_dropouts": deadman_candidate_evidence.get(
                    "supply_dropouts"
                )
                or [],
                "park_provenance": _park_provenance_rows(
                    deadman_candidate_evidence
                ),
                "status": deadman_candidate_evidence.get("status"),
                "requires_fable_ping": deadman_candidate_evidence.get(
                    "requires_fable_ping"
                ),
                "selected": deadman_candidate_evidence.get("selected"),
                "deferred_allpass_challengers": deadman_candidate_evidence.get(
                    "deferred_allpass_challengers"
                )
                or [],
                "inflight_observation_lost_eligibility": (
                    inflight_lost_identity or None
                ),
                "inflight_lost_row": inflight_lost_row,
            },
            "loss_exclusions": (
                (
                    (active_set_overlay.get("alternate_source_rotation") or {}).get(
                        "mechanical_demotion"
                    )
                    or {}
                ).get("demoted_members")
                if isinstance(active_set_overlay.get("alternate_source_rotation"), dict)
                else []
            )
            or [],
            "cross_exchange_actuator_status": cross_exchange_live_actuator.get("status"),
            "cross_exchange_actuator_terminal_reason": cross_exchange_live_actuator.get(
                "terminal_reason"
            ),
            "cross_exchange_last_order_id": cross_exchange_live_actuator.get(
                "last_order_id"
            ),
            "cross_exchange_campaign_orders_submitted": cross_exchange_live_actuator.get(
                "orders_submitted"
            ),
            "cross_exchange_campaign_orders_accepted": cross_exchange_live_actuator.get(
                "orders_accepted"
            ),
            "cross_exchange_campaign_orders_filled": cross_exchange_live_actuator.get(
                "orders_filled"
            ),
            "cross_exchange_campaign_method_pnl": cross_exchange_live_actuator.get(
                "method_pnl"
            ),
            "running_guard_never_implies_flow_pass": True,
        },
        "max_lines": MAX_DIGEST_LINES,
        "latest_handoff": latest_handoff.get("heading", ""),
        "latest_handoff_ts": _entry_timestamp_iso(latest_handoff),
        "latest_direction": latest_direction.get("heading", ""),
        "latest_direction_ts": _entry_timestamp_iso(latest_direction),
        "latest_operator_order": latest_operator_order.get("heading", ""),
        "latest_operator_order_ts": _entry_timestamp_iso(latest_operator_order),
        "latest_operator_order_material": latest_operator_order_material,
        "latest_operator_order_material_extraction_basis": "top_level_bullet_blocks",
        "newest_fable_direction": newest_fable_direction.get("heading", ""),
        "newest_fable_direction_ts": _entry_timestamp_iso(newest_fable_direction),
        "direction_freshness_status": direction_freshness_status,
        "latest_status": latest_status.get("heading", ""),
        "commitments_overdue": commitments_overdue,
        "working_tree_material": working_tree_material,
        "cli_versions": {
            "status": cli_versions.get("status"),
            "updated_at": cli_versions.get("updated_at"),
            "stale_tools": cli_versions.get("stale_tools")
            if isinstance(cli_versions.get("stale_tools"), list)
            else [],
            "unknown_latest_tools": cli_versions.get("unknown_latest_tools")
            if isinstance(cli_versions.get("unknown_latest_tools"), list)
            else [],
            "pending_smoke_test": cli_versions.get("pending_smoke_test")
            if isinstance(cli_versions.get("pending_smoke_test"), dict)
            else None,
            "last_smoke_test": cli_versions.get("last_smoke_test")
            if isinstance(cli_versions.get("last_smoke_test"), dict)
            else None,
            "tools": cli_versions_tools,
            "model_runtime_evidence": model_runtime_evidence,
        },
        "agy_fallback_smoke": {
            "result": agy_fallback_smoke.get("result"),
            "tested_at": agy_fallback_smoke.get("tested_at"),
            "fallback_provider": agy_fallback_smoke.get("fallback_provider"),
            "initial_agy_answer_proof": agy_fallback_smoke.get("initial_agy_answer_proof")
            if isinstance(agy_fallback_smoke.get("initial_agy_answer_proof"), dict)
            else None,
            "post_pin_substitute_attempts": agy_fallback_smoke.get("post_pin_substitute_attempts")
            if isinstance(agy_fallback_smoke.get("post_pin_substitute_attempts"), list)
            else [],
            "next_action": agy_fallback_smoke.get("next_action"),
        },
        "agy_quota": {
            "status": agy_quota_state.get("status"),
            "degraded_until": agy_quota_state.get("degraded_until"),
            "observed_at": agy_quota_state.get("observed_at"),
        },
        "live": {
            "guard_pid": guard.get("pid"),
            "guard_status": guard.get("status"),
            "guard_processes": guard_processes,
            "guard_caps": guard_caps,
            "intent_time_copyability_proof": {
                "status": intent_time_copyability_proof.get("status"),
                "generated_at": intent_time_copyability_proof.get("generated_at"),
                "records": intent_time_copyability_proof_summary.get("records"),
                "distinct_intents": intent_time_copyability_proof_summary.get("distinct_intents"),
                "required_buy_copy_events": intent_time_copyability_proof_summary.get("required_buy_copy_events"),
                "clob_filled_buy_copy_events": intent_time_copyability_proof_summary.get(
                    "clob_filled_buy_copy_events"
                ),
                "copyability_rejected_buy_events": intent_time_copyability_proof_summary.get(
                    "copyability_rejected_buy_events"
                ),
                "blocker_counts": intent_time_copyability_proof_summary.get("blocker_counts") or {},
                "covered_members": intent_time_copyability_proof_summary.get("covered_members") or [],
                "covered_members_this_cycle": intent_time_copyability_proof_summary.get(
                    "covered_members_this_cycle"
                )
                or [],
                "covered_member_count_this_cycle": intent_time_copyability_proof_summary.get(
                    "covered_member_count_this_cycle"
                ),
                "runtime_member_count_this_cycle": intent_time_copyability_proof_summary.get(
                    "runtime_member_count_this_cycle"
                ),
                "dropped_runtime_members_this_cycle": intent_time_copyability_proof_summary.get(
                    "dropped_runtime_members_this_cycle"
                ),
                "sampled_intents_this_cycle": intent_time_copyability_proof_summary.get(
                    "sampled_intents_this_cycle"
                ),
            },
            "polygon_ws_shadow": {
                "status": polygon_ws_shadow.get("status"),
                "pid": polygon_ws_shadow.get("pid"),
                "started_at": polygon_ws_shadow.get("started_at"),
                "cycles": polygon_ws_shadow.get("cycles"),
                "paper_only": polygon_ws_shadow.get("paper_only"),
                "live_orders_allowed": polygon_ws_shadow.get("live_orders_allowed"),
                "process_invariant": polygon_ws_shadow.get("process_invariant")
                if isinstance(polygon_ws_shadow.get("process_invariant"), dict)
                else {},
                "comparison_jsonl": polygon_ws_shadow.get("comparison_jsonl"),
            },
            "live_orders_allowed": bool(guard.get("live_orders_allowed")),
            "can_trade": bool(live_summary.get("can_trade")),
            "can_trade_source": "data/research/wallet_copy_live_execution_state.json.summary.can_trade",
            "runtime_permission": {
                "status": runtime_permission.get("status"),
                "can_trade": runtime_permission.get("can_trade"),
                "live_orders_allowed": runtime_permission.get("live_orders_allowed"),
                "paper_only": runtime_permission.get("paper_only"),
                "owner": runtime_permission.get("owner"),
                "updated_at": runtime_permission.get("updated_at"),
                "first_blocker": runtime_permission_blockers[0] if runtime_permission_blockers else None,
                "blockers": runtime_permission_blockers[:5],
            },
            "read_consistency": live_read_consistency,
            "latest_order_ts": live_summary.get("latest_order_ts"),
            "accepted_order_liveness_ts": live_summary.get("latest_order_ts"),
            "latest_live_order": latest_live_order,
            "latest_accepted_live_order": latest_accepted_live_order,
            "alternate_transport_bridge": {
                "status": alternate_transport_bridge.get("status"),
                "input_delta_rows": alternate_transport_bridge.get("input_delta_rows"),
                "selected_wallet": alternate_transport_bridge.get("selected_wallet"),
                "selected_candidate_id": alternate_transport_bridge.get("selected_candidate_id"),
                "selected_event_id": alternate_transport_bridge.get("selected_event_id"),
                "post_protection_survivors": alternate_transport_bridge.get(
                    "post_protection_survivors"
                ),
                "submit_stage_invocations": alternate_transport_bridge.get(
                    "submit_stage_invocations"
                ),
                "planner_rows": alternate_transport_bridge.get("planner_rows")
                if isinstance(alternate_transport_bridge.get("planner_rows"), list)
                else [],
            },
            "alternate_source_rotation": alternate_source_rotation,
            "orders": live_summary.get("live_orders"),
            "fills": live_summary.get("filled_orders"),
            "rejects": live_summary.get("rejected_orders"),
            "submitted": live_summary.get("submitted_orders"),
            "recent_fills": _recent_live_fills(live),
            "active_set_weekend_seat_loss_rotation": guard.get("active_set_weekend_seat_loss_rotation")
            if isinstance(guard.get("active_set_weekend_seat_loss_rotation"), dict)
            else {},
            "guard_loop_profile": {
                "status": guard_loop_profile.get("status"),
                "source": guard_loop_profile.get("source") or "data/research/wallet_copy_live_guard_state.json",
                "generated_at": guard_profile_generated_at,
                "pid": guard_loop_profile.get("pid") or current_guard_pid,
                "age_s": guard_profile_age_s,
                "freshness_slo_s": guard_profile_freshness_slo_s,
                "freshness_status": guard_profile_freshness_status,
                "cycle_started_at": guard_loop_profile.get("cycle_started_at"),
                "cycle_duration_s": guard_loop_profile.get("cycle_duration_s"),
                "cycle_duration_health": (
                    guard_loop_profile.get("cycle_duration_health")
                    if isinstance(guard_loop_profile.get("cycle_duration_health"), dict)
                    else {}
                ),
                "cycle_duration_series": (
                    guard_loop_profile.get("cycle_duration_series")
                    if isinstance(guard_loop_profile.get("cycle_duration_series"), list)
                    else []
                ),
                "target_median_iteration_lt_s": guard_loop_profile.get("target_median_iteration_lt_s"),
                "live_build_max_observed_age_s": guard_loop_profile.get("live_build_max_observed_age_s"),
                "total_s_before_state_write": _guard_profile_total_s(guard_loop_profile),
                "slow_path_cadence": guard_loop_profile.get("slow_path_cadence")
                if isinstance(guard_loop_profile.get("slow_path_cadence"), dict)
                else {},
                "top_stage_timers": [
                    {
                        "name": row.get("name"),
                        "duration_s": row.get("duration_s"),
                        "elapsed_s": row.get("elapsed_s"),
                    }
                    for row in guard_loop_top_stage_timers
                ],
            },
            "guard_json_cache_evidence": {
                "status": guard_json_cache_evidence.get("status"),
                "generated_at": guard_json_cache_evidence.get("generated_at"),
                "age_s": guard_cache_age_s,
                "freshness_slo_s": guard_cache_freshness_slo_s,
                "freshness_status": guard_cache_freshness_status,
                "patch_status": guard_json_cache_evidence.get("patch_status"),
                "cache_mode": guard_json_cache_evidence.get("cache_mode"),
                "default_load_json_semantics": guard_json_cache_evidence.get("default_load_json_semantics"),
                "hot_file": guard_json_cache_evidence.get("hot_file"),
                "hot_file_size_bytes": guard_json_cache_evidence.get("hot_file_size_bytes"),
                "hot_file_order_rows": guard_json_cache_evidence.get("hot_file_order_rows"),
                "first_load_s": guard_json_cache_evidence.get("first_load_s"),
                "second_load_s": guard_json_cache_evidence.get("second_load_s"),
                "same_object": guard_json_cache_evidence.get("same_object"),
                "cache_entries_after_probe": guard_json_cache_evidence.get("cache_entries_after_probe"),
                "cache_min_bytes": guard_json_cache_evidence.get("cache_min_bytes"),
                "cache_max_entries": guard_json_cache_evidence.get("cache_max_entries"),
                "large_json_files": guard_json_cache_evidence.get("large_json_files")
                if isinstance(guard_json_cache_evidence.get("large_json_files"), list)
                else [],
                "hot_call_sites": guard_json_cache_evidence.get("hot_call_sites")
                if isinstance(guard_json_cache_evidence.get("hot_call_sites"), list)
                else [],
                "cycle_stage_evidence": guard_json_cache_evidence.get("cycle_stage_evidence")
                if guard_cache_freshness_status == "CURRENT"
                and isinstance(guard_json_cache_evidence.get("cycle_stage_evidence"), dict)
                else {},
                "historical_cycle_stage_evidence": guard_json_cache_evidence.get(
                    "cycle_stage_evidence"
                )
                if guard_cache_freshness_status != "CURRENT"
                and isinstance(guard_json_cache_evidence.get("cycle_stage_evidence"), dict)
                else {},
                "cycle_stage_evidence_current": guard_cache_freshness_status == "CURRENT",
                "artifact": "data/research/guard_memory_json_cache_evidence_latest.json"
                if guard_json_cache_evidence
                else None,
            },
            "event_triggered_cycle_scheduler": {
                "status": event_triggered_cycle_scheduler.get("status"),
                "enabled": event_triggered_cycle_scheduler.get("enabled"),
                "triggered": event_triggered_cycle_scheduler.get("triggered"),
                "reason": event_triggered_cycle_scheduler.get("reason"),
                "sleep_s": event_triggered_cycle_scheduler.get("sleep_s"),
                "configured_sleep_s": event_triggered_cycle_scheduler.get("configured_sleep_s"),
                "trigger_sleep_s": event_triggered_cycle_scheduler.get("trigger_sleep_s"),
                "premerge_new_matching_events": event_triggered_cycle_scheduler.get("premerge_new_matching_events"),
                "premerge_wallets": event_triggered_cycle_scheduler.get("premerge_wallets")
                if isinstance(event_triggered_cycle_scheduler.get("premerge_wallets"), list)
                else [],
                "source_event": event_triggered_cycle_scheduler.get("source_event")
                if isinstance(event_triggered_cycle_scheduler.get("source_event"), dict)
                else {},
                "last_trigger": event_triggered_cycle_scheduler.get("last_trigger")
                if isinstance(event_triggered_cycle_scheduler.get("last_trigger"), dict)
                else {},
                "single_submitter_change": event_triggered_cycle_scheduler.get("single_submitter_change"),
                "copyintent_parity_change": event_triggered_cycle_scheduler.get("copyintent_parity_change"),
                "cap_threshold_eligibility_change": event_triggered_cycle_scheduler.get(
                    "cap_threshold_eligibility_change"
                ),
                "scheduler_submits_orders": event_triggered_cycle_scheduler.get("scheduler_submits_orders"),
                "submitter_invariant": event_triggered_cycle_scheduler.get("submitter_invariant"),
            },
            "guard_latency_trigger": guard_latency_trigger,
            "monday_return_proof": {
                "status": monday_return_proof.get("status"),
                "generated_at": monday_return_proof.get("generated_at"),
                "commitment_id": monday_return_proof.get("commitment_id"),
                "checks": monday_return_proof.get("checks")
                if isinstance(monday_return_proof.get("checks"), dict)
                else {},
                "target_wallet": (
                    (monday_return_proof.get("weekend_return_plan") or {}).get("target_wallets", [None])[0]
                    if isinstance(monday_return_proof.get("weekend_return_plan"), dict)
                    and isinstance((monday_return_proof.get("weekend_return_plan") or {}).get("target_wallets"), list)
                    and (monday_return_proof.get("weekend_return_plan") or {}).get("target_wallets")
                    else (monday_return_proof.get("runtime_roster") or {}).get("target_wallet")
                    if isinstance(monday_return_proof.get("runtime_roster"), dict)
                    else None
                ),
                "target_wallets": (
                    (monday_return_proof.get("weekend_return_plan") or {}).get("target_wallets")
                    if isinstance(monday_return_proof.get("weekend_return_plan"), dict)
                    and isinstance((monday_return_proof.get("weekend_return_plan") or {}).get("target_wallets"), list)
                    else []
                ),
                "target_count": (
                    (monday_return_proof.get("weekend_return_plan") or {}).get("target_count")
                    if isinstance(monday_return_proof.get("weekend_return_plan"), dict)
                    else None
                ),
                "bench_ids": (
                    (monday_return_proof.get("weekend_return_plan") or {}).get("bench_ids")
                    if isinstance(monday_return_proof.get("weekend_return_plan"), dict)
                    and isinstance((monday_return_proof.get("weekend_return_plan") or {}).get("bench_ids"), list)
                    else []
                ),
                "auto_return_at": (
                    (monday_return_proof.get("weekend_return_plan") or {}).get("auto_return_at")
                    if isinstance(monday_return_proof.get("weekend_return_plan"), dict)
                    else None
                ),
                "target_unbenched": (
                    (monday_return_proof.get("runtime_roster") or {}).get("target_unbenched")
                    if isinstance(monday_return_proof.get("runtime_roster"), dict)
                    else None
                ),
                "runtime_selected_wallet": (
                    (monday_return_proof.get("runtime_roster") or {}).get("runtime_selected_wallet")
                    if isinstance(monday_return_proof.get("runtime_roster"), dict)
                    else None
                ),
                "auto_return_reason": (
                    (
                        (
                            ((monday_return_proof.get("synthetic_monday_auto_returns") or [{}])[0]).get(
                                "auto_return"
                            )
                            or {}
                        ).get("reason")
                    )
                    if isinstance(monday_return_proof.get("synthetic_monday_auto_returns"), list)
                    and monday_return_proof.get("synthetic_monday_auto_returns")
                    else ((monday_return_proof.get("synthetic_monday_auto_return") or {}).get("auto_return") or {}).get("reason")
                    if isinstance(monday_return_proof.get("synthetic_monday_auto_return"), dict)
                    else None
                ),
                "evidence": str(monday_return_proof_path.relative_to(root))
                if monday_return_proof
                else None,
            },
        },
        "order_flow_deadman": {
            "status": order_flow_deadman.get("status"),
            "deadman_class": order_flow_deadman.get("deadman_class"),
            "checked_at": order_flow_deadman.get("checked_at"),
            "mechanical_escalation": order_flow_deadman.get("mechanical_escalation"),
            "digest_lag_s": deadman_digest_lag_s,
            "digest_lag_budget_s": 60.0,
            "digest_lag_status": (
                "CURRENT"
                if deadman_digest_lag_s is not None and deadman_digest_lag_s <= 60.0
                else "STALE"
                if deadman_digest_lag_s is not None
                else "UNKNOWN"
            ),
            "stale_across_transition": deadman_stale_across_transition,
            "can_trade": order_flow_deadman.get("can_trade"),
            "can_trade_reason": order_flow_deadman.get("can_trade_reason"),
            "idle_s": order_flow_deadman.get("idle_s"),
            "effective_deadman_idle_s": order_flow_deadman.get("effective_deadman_idle_s"),
            "host_downtime_attribution": order_flow_deadman.get("host_downtime_attribution")
            if isinstance(order_flow_deadman.get("host_downtime_attribution"), dict)
            else {},
            "deadman_warning": order_flow_deadman.get("deadman_warning"),
            "benign_skip_overrode": order_flow_deadman.get("benign_skip_overrode")
            if isinstance(order_flow_deadman.get("benign_skip_overrode"), list)
            else [],
            "guard_side_halt_warning": (
                (order_flow_deadman.get("guard_side_halt_signal") or {}).get("warning")
                if isinstance(order_flow_deadman.get("guard_side_halt_signal"), dict)
                else None
            ),
            "guard_side_halt_benign_skip_overrode": (
                (order_flow_deadman.get("guard_side_halt_signal") or {}).get("benign_skip_overrode")
                if isinstance(order_flow_deadman.get("guard_side_halt_signal"), dict)
                and isinstance((order_flow_deadman.get("guard_side_halt_signal") or {}).get("benign_skip_overrode"), list)
                else []
            ),
            "liveness_source": order_flow_deadman.get("liveness_source"),
            "liveness_ts": order_flow_deadman.get("liveness_ts"),
            "latest_order_ts": order_flow_deadman.get("latest_order_ts"),
            "latest_approved_suppression_ts": order_flow_deadman.get("latest_approved_suppression_ts"),
            "eligible_drought_s": order_flow_deadman.get("eligible_drought_s"),
            "eligible_drought_status": order_flow_deadman.get("eligible_drought_status"),
            "fresh_stale_signal_rows": order_flow_deadman.get("fresh_stale_signal_rows"),
            "approved_suppression_events": order_flow_deadman.get("approved_suppression_events"),
            "approved_suppression_tags": order_flow_deadman.get("approved_suppression_tags"),
            "copyable_orders_in_window": deadman_direct_source.get(
                "current_copyable_rows"
            ),
            "envelope_copyable_orders_in_window": deadman_direct_source.get(
                "envelope_copyable_orders_in_window"
            ),
            "copyable_cross_source_parity": deadman_direct_source.get(
                "copyable_cross_source_parity"
            ),
            "member_signal_age": order_flow_deadman.get("member_signal_age")
            if isinstance(order_flow_deadman.get("member_signal_age"), dict)
            else {},
            "policy_choke": _brain_policy_choke(order_flow_deadman.get("policy_choke")),
            "deadman_cycle_duration_health": (
                order_flow_deadman.get("deadman_cycle_duration_health")
                if isinstance(
                    order_flow_deadman.get("deadman_cycle_duration_health"), dict
                )
                else {}
            ),
            "early_admission_authority": (
                order_flow_deadman.get("early_admission_authority")
                if isinstance(order_flow_deadman.get("early_admission_authority"), dict)
                else {}
            ),
            "admission_publish_health": deadman_admission_publish_health,
            "money_anchored_status": order_flow_deadman.get("money_anchored_status"),
            "utc_day_money": order_flow_deadman.get("utc_day_money"),
            "policy_choke_rung_b_lifecycle": order_flow_deadman.get("policy_choke_rung_b_lifecycle")
            if isinstance(order_flow_deadman.get("policy_choke_rung_b_lifecycle"), dict)
            else {},
            "policy_choke_rung_b_cooloffs": order_flow_deadman.get("policy_choke_rung_b_cooloffs")
            if isinstance(order_flow_deadman.get("policy_choke_rung_b_cooloffs"), dict)
            else {},
            "policy_choke_fire_drill": {
                key: (order_flow_deadman.get("policy_choke_fire_drill") or {}).get(key)
                for key in ("status", "verdict", "generated_at", "live_mutation")
            }
            if isinstance(order_flow_deadman.get("policy_choke_fire_drill"), dict)
            else {},
            "walk_forward_refusal_frontier": walk_forward_refusal_frontier,
            "guard_memory": {
                "status": order_flow_guard_memory.get("status"),
                "checked_at": order_flow_deadman.get("checked_at"),
                "age_s": deadman_digest_lag_s,
                "freshness_slo_s": 60.0,
                "freshness_status": (
                    "CURRENT"
                    if deadman_digest_lag_s is not None and deadman_digest_lag_s <= 60.0
                    else "STALE"
                    if deadman_digest_lag_s is not None
                    else "UNKNOWN"
                ),
                "pid": order_flow_guard_memory.get("pid"),
                "rss_gib": order_flow_guard_memory.get("rss_gib"),
                "threshold_rss_gib": order_flow_guard_memory.get(
                    "threshold_rss_gib"
                ),
                "threshold_rss_source": order_flow_guard_memory.get(
                    "threshold_rss_source"
                ),
                "in_process_stage_boundary_rss": (
                    order_flow_guard_memory.get("in_process_stage_boundary_rss")
                    if isinstance(
                        order_flow_guard_memory.get("in_process_stage_boundary_rss"),
                        dict,
                    )
                    else {}
                ),
                "rss_observation_threshold_grade": order_flow_guard_memory.get(
                    "rss_observation_threshold_grade"
                ),
                "rss_peak_gib": order_flow_guard_memory.get("rss_peak_gib"),
                "rss_peak_at": order_flow_guard_memory.get("rss_peak_at"),
                "rss_observation": (
                    order_flow_guard_memory.get("rss_observation")
                    if isinstance(
                        order_flow_guard_memory.get("rss_observation"), dict
                    )
                    else {}
                ),
                "rss_kib": order_flow_guard_memory.get("rss_kib"),
                "raw_ps_line": (
                    order_flow_guard_memory.get("memory_probe", {}).get("raw_ps_line")
                    if isinstance(order_flow_guard_memory.get("memory_probe"), dict)
                    else None
                ),
                "verified_pid": (
                    order_flow_guard_memory.get("memory_probe", {}).get("verified_pid")
                    if isinstance(order_flow_guard_memory.get("memory_probe"), dict)
                    else None
                ),
                "warn_gib": order_flow_guard_memory.get("warn_gib"),
                "restart_gib": order_flow_guard_memory.get("restart_gib"),
                "auto_restart_status": order_flow_guard_memory_auto_restart.get("status"),
                "sample_count": len(order_flow_guard_memory_samples),
                "cycle_duration_health": {
                    **(
                        order_flow_guard_memory.get("cycle_duration_health")
                        if isinstance(order_flow_guard_memory.get("cycle_duration_health"), dict)
                        else {}
                    ),
                    "source_checked_at": order_flow_deadman.get("checked_at"),
                    "age_s": deadman_digest_lag_s,
                    "freshness_status": (
                        "CURRENT"
                        if deadman_digest_lag_s is not None and deadman_digest_lag_s <= 60.0
                        else "STALE"
                        if deadman_digest_lag_s is not None
                        else "UNKNOWN"
                    ),
                },
                "source": (
                    "data/research/order_flow_deadman_state.json."
                    "guard_memory.threshold_rss_gib"
                ),
                "samples_are_history_not_authority": True,
            },
        },
        "window_participation_merge_profile": (
            window_participation_merge_profile
            if isinstance(window_participation_merge_profile, dict)
            else {}
        ),
        "operator_notification_discipline": {
            "generated_at": operator_notification_discipline.get("generated_at"),
            "seen_incident_classes": operator_notification_discipline.get("seen_incident_classes")
            if isinstance(operator_notification_discipline.get("seen_incident_classes"), list)
            else [],
            "active_actionable_classes": operator_notification_discipline.get("active_actionable_classes")
            if isinstance(operator_notification_discipline.get("active_actionable_classes"), list)
            else [],
            "daily_counts": operator_notification_discipline.get("daily_counts")
            if isinstance(operator_notification_discipline.get("daily_counts"), dict)
            else {},
            "rule": operator_notification_discipline.get("rule"),
        },
        "active_member_orderfilled_hot_source_shadow": {
            key: active_member_orderfilled_hot_source_shadow.get(key)
            for key in (
                "generated_at",
                "status",
                "paper_only",
                "live_orders_allowed",
                "pid",
                "iteration",
                "prospective_wallet_count",
                "prospective_wallets",
                "prospective_current_market",
                "unique_resolved_source_events",
                "required_unique_resolved_source_events",
                "current_or_next_window_events",
                "duplicate_rows",
                "token_mapping_missing",
                "unmapped_out_of_scope_rows",
                "identity_market_outcome_parity_violations",
                "live_source_wiring_gate_passed",
                "identity_rule",
                "incremental_reader",
                "resource_usage",
            )
        },
        "early_01a_decision_time_book": {
            key: early_01a_decision_time_book.get(key)
            for key in (
                "generated_at",
                "status",
                "lane_decision_status",
                "signal_count",
                "distinct_windows",
                "book_lag_s",
                "book_status_fail_count",
                "executable_count",
                "paper_only",
                "live_orders_allowed",
            )
        },
        "qualified_pool_orderfilled_stakeout": {
            key: qualified_pool_orderfilled_stakeout.get(key)
            for key in (
                "generated_at",
                "status",
                "paper_only",
                "live_orders_allowed",
                "pid",
                "iteration",
                "qualified_pool_roster",
                "prospective_current_market",
                "identity_market_outcome_parity_violations",
                "incremental_reader",
                "resource_usage",
            )
        },
        "copy_source_identity_reconciliation": {
            "generated_at": copy_source_identity_reconciliation.get("generated_at"),
            "identity_rule": copy_source_identity_reconciliation.get("identity_rule"),
            "paper_only": copy_source_identity_reconciliation.get("paper_only"),
            "polygon_replay_rows_deduped": copy_source_identity_reconciliation.get(
                "polygon_replay_rows_deduped"
            ),
            "frozen_cohort": copy_source_identity_reconciliation.get("frozen_cohort")
            if isinstance(copy_source_identity_reconciliation.get("frozen_cohort"), dict)
            else {},
            "current_cohort": copy_source_identity_reconciliation.get("current_cohort")
            if isinstance(copy_source_identity_reconciliation.get("current_cohort"), dict)
            else {},
            "gates": copy_source_identity_reconciliation.get("gates")
            if isinstance(copy_source_identity_reconciliation.get("gates"), dict)
            else {},
        },
        "copy_source_wake_activation": {
            "activated": copy_source_wake_activation.get("activated"),
            "generated_at": copy_source_wake_activation.get("generated_at"),
            "paper_only_proof": copy_source_wake_activation.get("paper_only_proof"),
            "live_orders_allowed": copy_source_wake_activation.get("live_orders_allowed"),
            "paper_survivor_identity": copy_source_wake_activation.get(
                "paper_survivor_identity"
            ),
            "paper_survivor_orders_submitted": (
                (copy_source_wake_activation.get("paper_survivor") or {}).get(
                    "orders_submitted"
                )
                if isinstance(copy_source_wake_activation.get("paper_survivor"), dict)
                else None
            ),
            "runtime_generation": copy_source_wake_activation.get("runtime_generation"),
            "resident_guard": {
                "pid": guard.get("pid"),
                "git_head_at_launch": (
                    (guard.get("guard_code_identity") or {}).get("git_head_at_launch")
                    if isinstance(guard.get("guard_code_identity"), dict)
                    else None
                ),
                "script_sha256": (
                    (guard.get("guard_code_identity") or {}).get("script_sha256")
                    if isinstance(guard.get("guard_code_identity"), dict)
                    else None
                ),
                "live_guard_generation_sha256": (
                    (guard.get("guard_code_identity") or {}).get(
                        "live_guard_generation_sha256"
                    )
                    if isinstance(guard.get("guard_code_identity"), dict)
                    else None
                ),
            },
            "forced_sweep": latest_forced_sweep,
        },
        "orderfilled_fast_lane": {
            key: orderfilled_fast_lane.get(key)
            for key in (
                "generated_at",
                "status",
                "pid",
                "thread_name",
                "sole_submitter_process",
                "wake_socket",
                "wake_source",
                "stat_poll_fallback_s",
                "receipt_to_guard_sample_count",
                "receipt_to_guard_p95_s",
                "latency_aggregation_generation",
                "latency_generation_resets",
                "cursor_owner",
                "cursor_owner_count",
                "duplicate_bridge_invocations",
                "duplicate_bridge_rows_suppressed",
                "stage_counters",
                "last_stage_timing",
                "runtime_generation",
                "source_report",
                "bridge_report",
            )
        },
        "realized_fee_receipts": {
            "generated_at": realized_fee_receipts.get("generated_at"),
            "ledger_rewrite": realized_fee_receipts.get("ledger_rewrite"),
            "summary": realized_fee_receipts.get("summary")
            if isinstance(realized_fee_receipts.get("summary"), dict)
            else {},
        },
        "regime_seat_selection": {
            "generated_at": regime_seat_selection.get("generated_at"),
            "regime": regime_seat_selection.get("regime"),
            "current_selected_wallet": regime_seat_selection.get("current_selected_wallet"),
            "winner_wallet": regime_seat_selection.get("winner_wallet"),
            "winner_reason": regime_seat_selection.get("winner_reason"),
            "regime_evidence_leader_wallet": regime_seat_selection.get("regime_evidence_leader_wallet"),
            "eligible_count": regime_seat_selection.get("eligible_count"),
            "acceptance_share_30m": regime_seat_selection.get("acceptance_share_30m")
            if isinstance(regime_seat_selection.get("acceptance_share_30m"), dict)
            else {},
            "acceptance_share_90m": regime_seat_selection.get("acceptance_share_90m")
            if isinstance(regime_seat_selection.get("acceptance_share_90m"), dict)
            else {},
        },
        "coacceptance_eligibility_shadow": {
            "generated_at": coacceptance_shadow.get("generated_at"),
            "status": coacceptance_shadow.get("status"),
            "can_trade": coacceptance_shadow.get("can_trade"),
            "order_flow_status": coacceptance_shadow.get("order_flow_status"),
            "policy_choke_status": coacceptance_shadow.get("policy_choke_status"),
            "selected_wallet": coacceptance_shadow.get("selected_wallet"),
            "seat_action": coacceptance_shadow.get("seat_action"),
            "rows": coacceptance_shadow.get("rows") if isinstance(coacceptance_shadow.get("rows"), list) else [],
            "live_mutation": coacceptance_shadow.get("live_mutation"),
        },
        "probe_fill_quality_shadows": {
            "generated_at": probe_fill_quality_shadows.get("generated_at"),
            "status": probe_fill_quality_shadows.get("status"),
            "weekday_micro_loss": probe_fill_quality_shadows.get("weekday_micro_loss") or {},
            "window_time_near_miss": probe_fill_quality_shadows.get("window_time_near_miss") or {},
            "early_utc_probe_fill_quality": probe_fill_quality_shadows.get("early_utc_probe_fill_quality") or {},
            "decision": probe_fill_quality_shadows.get("decision"),
            "live_mutation": probe_fill_quality_shadows.get("live_mutation"),
        },
        "order_flow_deadman_r1_attribution": order_flow_deadman_r1_attribution_summary,
        "trade_executor_lane_attribution": trade_executor_lane_attribution_summary,
        "own_positions": {
            "status": own_positions.get("status"),
            "generated_at": own_positions.get("generated_at"),
            "wallet": own_positions.get("wallet"),
            "positions_rows": own_positions_summary.get("positions_rows"),
            "data_api_redeemable_rows": own_positions_summary.get("data_api_redeemable_rows"),
            "data_api_redeemable_locked_usd": own_positions_summary.get("data_api_redeemable_locked_usd"),
            "ledger_estimated_redeemable_locked_usd": own_positions_summary.get(
                "ledger_estimated_redeemable_locked_usd"
            ),
            "redeemable_locked_usd": own_positions_summary.get("redeemable_locked_usd"),
            "locked_value_source": own_positions_summary.get("locked_value_source"),
            "data_api_status": own_positions_data_api.get("status"),
            "data_api_reason": own_positions_data_api.get("reason"),
            "deadman_status": own_position_deadman.get("status"),
            "deadman_age_s": own_position_deadman.get("age_s"),
            "deadman_incident": own_position_deadman.get("incident"),
            "deadman_acknowledged_reason": own_position_deadman.get("acknowledged_reason"),
            "deadman_acknowledged_until": own_position_deadman.get("acknowledged_until"),
            "deadman_next_action": own_position_deadman.get("next_action"),
        },
        "own_redeemer": {
            "status": own_redeemer.get("status"),
            "generated_at": own_redeemer.get("generated_at"),
            "candidate_count": own_redeemer.get("candidate_count"),
            "candidate_source": own_redeemer.get("candidate_source"),
            "executed": own_redeemer.get("executed"),
            "skipped": own_redeemer.get("skipped"),
            "next_retry_at": own_redeemer.get("next_retry_at"),
            "error": own_redeemer.get("error"),
            "transaction_hash": ((own_redeemer.get("result") or {}).get("transaction_hash"))
            if isinstance(own_redeemer.get("result"), dict)
            else None,
            "post_redeem_recon_status": ((own_redeemer.get("post_redeem_recon") or {}).get("status"))
            if isinstance(own_redeemer.get("post_redeem_recon"), dict)
            else None,
        },
        "wallet_outflow_deadman": {
            "status": wallet_outflow_deadman.get("status"),
            "checked_at": wallet_outflow_deadman.get("checked_at"),
            "incident": wallet_outflow_deadman.get("incident"),
            "wallet": wallet_outflow_deadman.get("wallet"),
            "last_ok_checked_at": wallet_outflow_deadman.get("last_ok_checked_at"),
            "consecutive_degraded_fetches": wallet_outflow_deadman.get("consecutive_degraded_fetches"),
            "fetch_gap_exceeded": (wallet_outflow_deadman.get("window") or {}).get("fetch_gap_exceeded")
            if isinstance(wallet_outflow_deadman.get("window"), dict)
            else None,
            "outflow_rows": (wallet_outflow_deadman.get("summary") or {}).get("outflow_rows")
            if isinstance(wallet_outflow_deadman.get("summary"), dict)
            else None,
            "matched_order_outflows": (wallet_outflow_deadman.get("summary") or {}).get(
                "matched_order_outflows"
            )
            if isinstance(wallet_outflow_deadman.get("summary"), dict)
            else None,
            "matched_redemption_outflows": (wallet_outflow_deadman.get("summary") or {}).get(
                "matched_redemption_outflows"
            )
            if isinstance(wallet_outflow_deadman.get("summary"), dict)
            else None,
            "unmatched_outflows": (wallet_outflow_deadman.get("summary") or {}).get("unmatched_outflows")
            if isinstance(wallet_outflow_deadman.get("summary"), dict)
            else None,
            "incident_outflows": (wallet_outflow_deadman.get("summary") or {}).get("incident_outflows")
            if isinstance(wallet_outflow_deadman.get("summary"), dict)
            else None,
            "unmatched_outflow_usd": (wallet_outflow_deadman.get("summary") or {}).get(
                "unmatched_outflow_usd"
            )
            if isinstance(wallet_outflow_deadman.get("summary"), dict)
            else None,
            "next_action": wallet_outflow_deadman.get("next_action"),
        },
        "research_disk_deadman": {
            "status": research_disk_deadman.get("status"),
            "generated_at": research_disk_deadman.get("generated_at"),
            "incident": research_disk_deadman.get("incident"),
            "incident_keys": research_disk_deadman.get("incident_keys")
            if isinstance(research_disk_deadman.get("incident_keys"), list)
            else [],
            "free_gib": (research_disk_deadman.get("disk") or {}).get("free_gib")
            if isinstance(research_disk_deadman.get("disk"), dict)
            else None,
            "used_pct": (research_disk_deadman.get("disk") or {}).get("used_pct")
            if isinstance(research_disk_deadman.get("disk"), dict)
            else None,
            "inventory_count": research_disk_deadman.get("inventory_count"),
            "large_file_count": len(research_disk_deadman.get("large_files") or [])
            if isinstance(research_disk_deadman.get("large_files"), list)
            else None,
            "uninventoried_count": len(research_disk_deadman.get("uninventoried_large_files") or [])
            if isinstance(research_disk_deadman.get("uninventoried_large_files"), list)
            else None,
            "largest_uninventoried": (research_disk_deadman.get("uninventoried_large_files") or [None])[0]
            if isinstance(research_disk_deadman.get("uninventoried_large_files"), list)
            and research_disk_deadman.get("uninventoried_large_files")
            else None,
            "memory_swap_status": (research_disk_deadman.get("memory_swap") or {}).get("status")
            if isinstance(research_disk_deadman.get("memory_swap"), dict)
            else None,
            "memory_swap_incident": (research_disk_deadman.get("memory_swap") or {}).get("incident")
            if isinstance(research_disk_deadman.get("memory_swap"), dict)
            else None,
            "swapfile_count": ((research_disk_deadman.get("memory_swap") or {}).get("swapfiles") or {}).get("count")
            if isinstance(research_disk_deadman.get("memory_swap"), dict)
            and isinstance((research_disk_deadman.get("memory_swap") or {}).get("swapfiles"), dict)
            else None,
            "memory_pressure_free_pct": (
                ((research_disk_deadman.get("memory_swap") or {}).get("memory_pressure") or {}).get("free_pct")
            )
            if isinstance(research_disk_deadman.get("memory_swap"), dict)
            and isinstance((research_disk_deadman.get("memory_swap") or {}).get("memory_pressure"), dict)
            else None,
            "next_action": research_disk_deadman.get("next_action"),
        },
        "guard_event_log_rotation": {
            "status": guard_event_log_rotation.get("status"),
            "generated_at": guard_event_log_rotation.get("generated_at"),
            "action": guard_event_log_rotation.get("action"),
            "path": guard_event_log_rotation.get("path"),
            "size_before_bytes": guard_event_log_rotation.get("size_before_bytes"),
            "size_after_bytes": guard_event_log_rotation.get("size_after_bytes"),
            "archive_path": guard_event_log_rotation.get("archive_path"),
            "archive_size_bytes": guard_event_log_rotation.get("archive_size_bytes"),
            "bytes_removed_from_hot_path": guard_event_log_rotation.get("bytes_removed_from_hot_path"),
            "retained_tail_bytes": guard_event_log_rotation.get("retained_tail_bytes"),
            "tail_line_aligned": guard_event_log_rotation.get("tail_line_aligned"),
        },
        "alpha_decay_curve": {
            "path": str(alpha_decay_curve_path),
            "updated_at": alpha_decay_curve.get("updated_at") if isinstance(alpha_decay_curve, dict) else None,
            "status": alpha_decay_alpha.get("status"),
            "horizons_s": alpha_decay_alpha.get("horizons_s") if isinstance(alpha_decay_alpha.get("horizons_s"), list) else [],
            "fills_total": alpha_decay_alpha.get("fills_total"),
            "fills_with_any_book_coverage": alpha_decay_alpha.get("fills_with_any_book_coverage"),
            "overlapping_fill_book_assets": alpha_decay_alpha.get("overlapping_fill_book_assets"),
            "five_s": alpha_decay_rule,
            "edge_mean_1s": _edge_stat(alpha_decay_alpha, "1s", "edge", "mean"),
            "edge_mean_2s": _edge_stat(alpha_decay_alpha, "2s", "edge", "mean"),
            "edge_mean_5s": _edge_stat(alpha_decay_alpha, "5s", "edge", "mean"),
            "edge_mean_10s": _edge_stat(alpha_decay_alpha, "10s", "edge", "mean"),
            "edge_mean_30s": _edge_stat(alpha_decay_alpha, "30s", "edge", "mean"),
            "edge_mean_60s": _edge_stat(alpha_decay_alpha, "60s", "edge", "mean"),
            "edge_mean_120s": _edge_stat(alpha_decay_alpha, "120s", "edge", "mean"),
            "next_action": alpha_decay_alpha.get("next_action"),
        },
        "alpha_overlap_capture": {
            "launchd_label": overlap_capture_label,
            "pid": overlap_capture_pid,
            "run_suffix": overlap_run_suffix,
            "polygon_path": _rel_path(overlap_polygon_path),
            "polygon_rows": _count_file_lines(overlap_polygon_path),
            "clob_path": _rel_path(overlap_clob_path),
            "clob_rows": _count_file_lines(overlap_clob_path),
            "report_path": _rel_path(overlap_report_path),
            "report_exists": overlap_report_path.is_file(),
            "state_path": _rel_path(overlap_state_path),
            "state_exists": overlap_state_path.is_file(),
            "registry_path": _rel_path(overlap_registry_path),
            "paper_only": overlap_registry.get("paper_only"),
            "live_orders_allowed": overlap_registry.get("live_orders_allowed"),
            "wallet_count": len(overlap_registry.get("wallets") or []),
            "status": (
                "RUNNING_CAPTURE"
                if overlap_capture_pid
                else "COMPLETE_REPORT_READY"
                if overlap_report_path.is_file() and overlap_state_path.is_file()
                else "NO_ACTIVE_CAPTURE"
            ),
        },
        "research_lane_cadence": {
            "generated_at": research_lane_cadence.get("generated_at"),
            "authority": research_lane_cadence.get("authority"),
            "alpha_decay": (research_lane_cadence.get("lanes") or {}).get("alpha_decay", {}),
            "top10_broad": (research_lane_cadence.get("lanes") or {}).get("top10_broad", {}),
            "whale_consensus": (research_lane_cadence.get("lanes") or {}).get("whale_consensus", {}),
        },
        "same_window_research_capture": same_window_capture,
        "repo_storage_hygiene": repo_storage_hygiene,
        "targeted_copyability_probe": {
            "generated_at": targeted_copyability_probe.get("generated_at"),
            "criteria": targeted_copyability_probe.get("criteria")
            if isinstance(targeted_copyability_probe.get("criteria"), dict)
            else {},
            "summary": targeted_copyability_probe.get("summary")
            if isinstance(targeted_copyability_probe.get("summary"), dict)
            else {},
            "rows": [
                {
                    "wallet": row.get("wallet"),
                    "local_feed_rows": row.get("local_feed_rows"),
                    "btc5m_buys": row.get("btc5m_buys"),
                    "inband_025_050_buy_share_pct": row.get("inband_025_050_buy_share_pct"),
                    "median_entry_offset_s": row.get("median_entry_offset_s"),
                    "latest_trade_age_h": row.get("latest_trade_age_h"),
                    "fresh_flow": row.get("fresh_flow"),
                    "p1_promotion_eligible": row.get("p1_promotion_eligible"),
                    "p1_reject_reasons": row.get("p1_reject_reasons") or [],
                }
                for row in (targeted_copyability_probe.get("ranked_candidates") or [])
                if isinstance(row, dict)
                and row.get("wallet")
                in {
                    "0x141d08cb2efe0b57ee1d7d7d4f524cce12f40f59",
                    "0x927f7694de44d19a72bce76254e628d1c141d215",
                    "0x251c1a283703beed41590b0875a8dcb8ddd1541f",
                }
            ],
        },
        "pnl": {
            "day_pnl_usd": score_total.get("pnl_usd"),
            "day_utc": scorecard.get("day_utc"),
            "day_pnl_primary_basis": selected_day_pnl_basis,
            "day_pnl_response_basis": day_pnl_basis.get("day_pnl_response_basis")
            if day_pnl_basis
            else score_total.get("pnl_usd"),
            "day_pnl_actual_basis": day_pnl_basis.get("day_pnl_actual_basis") or scorecard.get("day_pnl_actual_basis"),
            "basis_split_delta_usd": day_pnl_basis.get("basis_split_delta_usd") or scorecard.get("basis_split_delta_usd"),
            "day_pnl_basis_reconciliation": day_pnl_basis_reconciliation,
            "actual_basis_coverage": actual_basis_coverage,
            "basis_split_decomposition": {
                "assertion": basis_split_decomposition.get("assertion"),
                "joined_group_count": basis_split_decomposition.get("joined_group_count"),
                "sum_joined_improvement_usd": basis_split_decomposition.get("sum_joined_improvement_usd"),
                "remainder_usd": basis_split_decomposition.get("remainder_usd"),
                "remainder_term": basis_split_decomposition.get("remainder_term"),
                "join_key_schema": basis_split_decomposition.get("join_key_schema")
                if isinstance(basis_split_decomposition.get("join_key_schema"), dict)
                else {},
                "top_improvements": basis_split_decomposition.get("top_improvements")
                if isinstance(basis_split_decomposition.get("top_improvements"), list)
                else [],
            },
            "day_resolved_fills": score_total.get("resolved_fills"),
            "day_payout_fill_count": score_total.get("payout_fill_count"),
            "day_in_band_resolved_fill_count": score_total.get(
                "in_band_resolved_fill_count"
            ),
            "day_in_band_payout_fill_count": score_total.get(
                "in_band_payout_fill_count"
            ),
            "day_in_band_winner_binomial_lower_tail_p_value": score_total.get(
                "in_band_winner_binomial_lower_tail_p_value"
            ),
            "day_in_band_holdout_live_status": score_total.get(
                "in_band_holdout_live_status"
            ),
            "daily_floor_gate_residency": daily_floor_gate_residency,
            "lifetime_price_band_01_25_50": lifetime_price_bands.get("01_25_50", {}),
            "lifetime_price_subbands_01_25_50": lifetime_money_subbands,
            "since_topup_verdict": since_topup.get("primary_verdict"),
            "since_topup_canonical_pnl_usd": since_topup.get("canonical_pnl_usd"),
            "since_topup_actual_delta_usd": since_topup.get("actual_delta_vs_baseline_usd"),
            "reconciled_actual_delta_usd": since_topup.get("actual_basis_reconciled_delta_vs_baseline_usd"),
            "reconciled_actual_verdict": since_topup.get("actual_basis_reconciled_verdict"),
            "self_feed_overlay_delta_usd": since_topup_overlay.get("overlay_delta_usd"),
            "self_feed_overlay_mode": since_topup_overlay.get("mode"),
            "self_feed_overlay_ledger_rewrite": since_topup_overlay.get("ledger_rewrite"),
            "reconciliation_status": since_topup.get("reconciliation_status"),
            "raw_reconciliation_status": since_topup.get("raw_reconciliation_status"),
            "unadjusted_reconciliation_status": since_topup.get("unadjusted_reconciliation_status"),
            "cash_diff_residual_usd": since_topup_cash_residual.get("residual_usd"),
            "cash_diff_fill_explained_usd": since_topup_cash_residual.get("fill_cost_payout_explained_usd"),
            "cash_diff_residual_classification": since_topup_cash_residual.get("residual_classification"),
            "cash_diff_joined_tx_groups": since_topup_cash_residual.get("joined_tx_groups"),
            "cash_diff_ledger_tx_groups": since_topup_cash_residual.get("ledger_tx_groups"),
            "cash_diff_unjoined_tx_groups": since_topup_cash_residual.get("unjoined_tx_groups"),
            "cash_diff_residual_trend_count": len(cash_diff_residual_trend),
            "cash_diff_residual_trend_latest": cash_diff_residual_trend[-1] if cash_diff_residual_trend else None,
            "heartbeat_ledger_delta": heartbeat_ledger_delta,
            "goal_reachability": goal_reachability,
            "d16_entry_band_acceptance": d16_entry_band_acceptance,
            "passive_at_source_holdout": {
                "generated_at": passive_at_source_holdout.get("generated_at"),
                "verdict": passive_at_source_holdout.get("verdict"),
                "sample_gate": passive_at_source_holdout.get("sample_gate")
                if isinstance(passive_at_source_holdout.get("sample_gate"), dict)
                else {},
                "aggregate": passive_at_source_holdout.get("aggregate")
                if isinstance(passive_at_source_holdout.get("aggregate"), dict)
                else {},
                "development": passive_at_source_holdout.get("development")
                if isinstance(passive_at_source_holdout.get("development"), dict)
                else {},
                "chronological_holdout": passive_at_source_holdout.get("chronological_holdout")
                if isinstance(passive_at_source_holdout.get("chronological_holdout"), dict)
                else {},
                "next_action": passive_at_source_holdout.get("next_action"),
                "measurement_only": passive_at_source_holdout.get("measurement_only"),
                "live_orders_allowed": passive_at_source_holdout.get("live_orders_allowed"),
            },
            "taker_price_subband_holdout": {
                "generated_at": taker_price_subband_holdout.get("generated_at"),
                "focus_subband": taker_price_subband_holdout.get("focus_subband"),
                "focus_verdict": taker_price_subband_holdout.get("focus_verdict"),
                "size_ruling_request_ready": taker_price_subband_holdout.get(
                    "size_ruling_request_ready"
                ),
                "focus_holdout_pnl_concentration": taker_price_subband_holdout.get(
                    "focus_holdout_pnl_concentration"
                ),
                "focus_robustness": taker_price_subband_holdout.get("focus_robustness"),
                "subbands": taker_price_subband_holdout.get("subbands")
                if isinstance(taker_price_subband_holdout.get("subbands"), dict)
                else {},
                "next_action": taker_price_subband_holdout.get("next_action"),
                "measurement_only": taker_price_subband_holdout.get("measurement_only"),
                "live_orders_allowed": taker_price_subband_holdout.get("live_orders_allowed"),
            },
            "band_pnl_surface_reconciliation": {
                "generated_at": band_pnl_surface_reconciliation.get("generated_at"),
                "decision": band_pnl_surface_reconciliation.get("decision"),
                "payout_usd_semantics": band_pnl_surface_reconciliation.get(
                    "payout_usd_semantics"
                ),
                "band_definition_mismatch": band_pnl_surface_reconciliation.get(
                    "band_definition_mismatch"
                ),
                "bands": band_pnl_surface_reconciliation.get("bands"),
                "whole_book_day_bounded": band_pnl_surface_reconciliation.get(
                    "whole_book_day_bounded"
                ),
                "live_mutation": band_pnl_surface_reconciliation.get("live_mutation"),
            },
            "fee_realization_bank_reconciliation": {
                "generated_at": fee_realization_bank_reconciliation.get("generated_at"),
                "status": fee_realization_bank_reconciliation.get("status"),
                "bank_identity": fee_realization_bank_reconciliation.get("bank_identity"),
                "response_basis_counterfactual": fee_realization_bank_reconciliation.get(
                    "response_basis_counterfactual"
                ),
                "cost_basis_migration": fee_realization_bank_reconciliation.get(
                    "cost_basis_migration"
                ),
                "population_fork": fee_realization_bank_reconciliation.get("population_fork"),
                "fee_authority": fee_realization_bank_reconciliation.get("fee_authority"),
                "live_mutation": fee_realization_bank_reconciliation.get("live_mutation"),
            },
            "payout_receipt_reconciliation": {
                "generated_at": payout_receipt_reconciliation.get("generated_at"),
                "status": payout_receipt_reconciliation.get("status"),
                "summary": payout_receipt_reconciliation.get("summary"),
                "coverage_seam": payout_receipt_reconciliation.get("coverage_seam"),
                "unmatched_receipt_credit_rows": payout_receipt_reconciliation.get(
                    "unmatched_receipt_credit_rows"
                ),
            },
            "temporal_slice_label_divergence": {
                "generated_at": temporal_slice_label_divergence.get("generated_at"),
                "status": temporal_slice_label_divergence.get("status"),
                "authority": temporal_slice_label_divergence.get("authority"),
                "summary": temporal_slice_label_divergence.get("summary"),
            },
            "maker_min_share_cap_choke": {
                "generated_at": maker_min_share_cap_choke.get("generated_at"),
                "day_utc": maker_min_share_cap_choke.get("day_utc"),
                "summary": maker_min_share_cap_choke.get("summary"),
                "by_wallet": maker_min_share_cap_choke.get("by_wallet"),
                "live_path_mutated": maker_min_share_cap_choke.get("live_path_mutated"),
            },
            "subband_basis_reconciliation": {
                "lifetime_01c_resolved_fill_rows": (
                    lifetime_money_subbands.get("01c_40_50", {}).get("resolved_fills")
                    if isinstance(lifetime_money_subbands.get("01c_40_50"), dict)
                    else None
                ),
                "holdout_instrument_01c_unique_taker_windows": (
                    (taker_holdout_subbands.get("01c_40_50", {}).get("aggregate") or {}).get("rows")
                    if isinstance(taker_holdout_subbands.get("01c_40_50"), dict)
                    else None
                ),
                "basis_difference": (
                    "lifetime scorecard counts every resolved FILLED ledger row across execution roles; "
                    "holdout instrument keeps taker rows only and deduplicates to the earliest FILLED "
                    "order per market_slug, so repeat fills in one window are intentionally removed"
                ),
                "decision_basis": "day-bounded unique-window taker holdout",
            },
        },
        "since_topup_identity": {
            "scope": "current_steering_metric",
            "source_scorecard_generated_at": scorecard.get("generated_at"),
            "baseline_usd": since_topup.get("baseline_usd"),
            "baseline_iso": since_topup.get("baseline_iso"),
            "baseline_source": since_topup.get("baseline_source"),
            "canonical_pnl_usd": since_topup.get("canonical_pnl_usd"),
            "actual_delta_vs_baseline_usd": since_topup.get("actual_delta_vs_baseline_usd"),
            "actual_value_basis": since_topup.get("actual_value_basis"),
            "actual_basis_reconciled_delta_vs_baseline_usd": since_topup.get(
                "actual_basis_reconciled_delta_vs_baseline_usd"
            ),
            "actual_basis_reconciled_verdict": since_topup.get("actual_basis_reconciled_verdict"),
            "self_feed_overlay_freshness_status": since_topup_overlay.get("freshness_status"),
            "self_feed_overlay_generated_at": since_topup_overlay.get("generated_at"),
            "self_feed_overlay_fallback_status": since_topup_overlay.get("fallback_status"),
        },
        "balance_feed_monitor": {
            "status": balance_feed_monitor.get("status"),
            "balance_status": balance_feed_monitor.get("balance_status"),
            "balance_reason": balance_feed_monitor.get("balance_reason"),
            "consecutive_unavailable_generations": balance_feed_monitor.get("consecutive_unavailable_generations"),
            "defect": balance_feed_monitor.get("defect"),
            "state_path": balance_feed_monitor.get("state_path"),
        },
        "closed_daily": {
            "scope": "historical_closed_day_not_current_steering_metric",
            "current_steering_metric": False,
            "day_utc": closed_scorecard.get("day_utc"),
            "generated_at": closed_scorecard.get("generated_at"),
            "path": closed_scorecard.get("_path"),
            "pnl_usd": closed_total.get("pnl_usd"),
            "orders": closed_total.get("orders"),
            "fills": closed_total.get("fills"),
            "rejects": closed_total.get("rejects"),
            "roi_pct": closed_total.get("roi_pct"),
            "filled_volume_usd": closed_total.get("cost_usd"),
            "windows_filled": closed_volume.get("windows_filled"),
            "windows_submitted": closed_volume.get("windows_submitted"),
            "denominator_windows": closed_volume.get("denominator_windows"),
            "since_topup_canonical_pnl_usd": closed_since_topup.get("canonical_pnl_usd"),
            "since_topup_canonical_pnl_pct": closed_since_topup.get("canonical_pnl_pct"),
            "since_topup_actual_delta_usd": closed_since_topup.get("actual_delta_vs_baseline_usd"),
            "since_topup_reconciled_actual_delta_usd": closed_since_topup.get(
                "actual_basis_reconciled_delta_vs_baseline_usd"
            ),
            "since_topup_verdict": closed_since_topup.get("primary_verdict"),
            "target_day_pnl_usd": closed_target.get("day_pnl_usd"),
            "target_north_star_gap_usd": closed_target.get("north_star_daily_gap_usd"),
            "target_phase_gap_usd": closed_target.get("phase_daily_min_gap_usd"),
        },
        "closed_daily_automation_drift": closed_automation_drift,
        "closed_weekly_verdict": closed_weekly_verdict,
        "volume": {
            "windows_filled": volume.get("windows_filled"),
            "windows_submitted": volume.get("windows_submitted"),
            "denominator_windows": volume.get("denominator_windows"),
            "active_windows": flow_participation.get("active_windows"),
            "missed_active_windows": flow_participation.get("missed_active_windows"),
            "consecutive_missed_active_windows": flow_participation.get("consecutive_missed_active_windows"),
            "incident_triggered": flow_participation.get("incident_triggered"),
            "participation_basis": flow_participation.get("basis"),
            "raw_participation": {
                "active_windows": participation.get("active_windows"),
                "missed_active_windows": participation.get("missed_active_windows"),
                "consecutive_missed_active_windows": participation.get("consecutive_missed_active_windows"),
                "incident_triggered": participation.get("raw_incident_triggered", participation.get("incident_triggered")),
                "basis": "window_participation_current_generation_raw",
            },
            "adjusted_participation": adjusted_participation,
            "source_coverage": source_coverage,
            "current_generation": {
                "active_windows": participation.get("active_windows"),
                "missed_active_windows": participation.get("missed_active_windows"),
                "consecutive_missed_active_windows": participation.get("consecutive_missed_active_windows"),
                "incident_triggered": participation.get("incident_triggered"),
                "set_generation_id": participation.get("set_generation_id"),
            },
            "rolling_288": rolling_participation,
            "recent_participation_rows": recent_participation_rows,
        },
        "per_window_pnl_histogram": per_window_pnl_histogram,
        "e6db_loser_autopsy": {
            "generated_at": e6db_loser_autopsy.get("generated_at"),
            "path": "data/research/e6db_loser_autopsy_latest.json" if e6db_loser_autopsy else None,
            "summary": e6db_loser_autopsy_summary,
        },
        "successor_dossier": {
            "generated_at": successor_dossier.get("generated_at"),
            "path": "data/research/successor_dossier_latest.json" if successor_dossier else None,
            "summary": successor_dossier_summary,
            "routing_shadow": successor_dossier.get("routing_shadow")
            if isinstance(successor_dossier.get("routing_shadow"), dict)
            else {},
            "fee_calibration_coverage": successor_dossier.get("fee_calibration_coverage")
            if isinstance(successor_dossier.get("fee_calibration_coverage"), dict)
            else {},
            "corrected_probe": successor_dossier.get("corrected_probe")
            if isinstance(successor_dossier.get("corrected_probe"), dict)
            else {},
            "deadman_corrected_gate": successor_dossier.get("deadman_corrected_gate")
            if isinstance(successor_dossier.get("deadman_corrected_gate"), dict)
            else {},
            "temporal_profile": successor_dossier.get("temporal_profile")
            if isinstance(successor_dossier.get("temporal_profile"), dict)
            else {},
            "gap_sigma": successor_dossier.get("gap_sigma")
            if isinstance(successor_dossier.get("gap_sigma"), dict)
            else {},
        },
        "active_set_rotation_packet": {
            "generated_at": active_set_rotation_packet.get("generated_at"),
            "path": "data/research/active_set_rotation_packet_latest.json" if active_set_rotation_packet else None,
            "summary": active_set_rotation_summary,
            "ranked_candidates": active_set_rotation_rows[:3],
        },
        "active_set_post_rotation_windows": {
            "generated_at": active_set_post_rotation_windows.get("generated_at"),
            "path": "data/research/active_set_post_rotation_windows_latest.json"
            if active_set_post_rotation_windows
            else None,
            "summary": active_set_post_rotation_summary,
        },
        "active_set_pin_consumer_sweep": {
            "generated_at": active_set_pin_consumer_sweep.get("generated_at"),
            "path": "data/research/active_set_pin_consumer_sweep_latest.json"
            if active_set_pin_consumer_sweep
            else None,
            "status": active_set_pin_consumer_sweep.get("status"),
            "source_of_truth": active_set_pin_consumer_sweep.get("source_of_truth"),
            "dangerous_consumers": active_set_pin_consumer_sweep.get("dangerous_consumers")
            if isinstance(active_set_pin_consumer_sweep.get("dangerous_consumers"), list)
            else [],
            "finding": active_set_pin_consumer_sweep.get("finding"),
            "packet_snapshots": active_set_pin_consumer_sweep.get("packet_snapshots")
            if isinstance(active_set_pin_consumer_sweep.get("packet_snapshots"), list)
            else [],
        },
        "defense_tripwires": defense_tripwires,
        "defense_regret": defense_regret,
        "e1_framework_audit_inputs": {
            "generated_at": e1_audit_inputs.get("generated_at"),
            "day_utc": e1_audit_inputs.get("day_utc"),
            "path": _rel_path(e1_audit_inputs_path, root),
            "freshness_status": (
                "STALE_ARTEFACT_LEDGER_LAG" if e1_artifact_stale else "CURRENT"
            ),
            "ledger_lag_s": e1_ledger_lag_s,
            "live_ledger_cut_frozen_by": e1_live_ledger_cut_frozen_by,
            "artifact_ledger_newest_submitted_at": (
                e1_artifact_ledger_cut.isoformat() if e1_artifact_ledger_cut else None
            ),
            "live_ledger_newest_submitted_at": (
                e1_live_ledger_cut.isoformat() if e1_live_ledger_cut else None
            ),
            "defense_regret": e1_audit_inputs.get("defense_regret")
            if isinstance(e1_audit_inputs.get("defense_regret"), dict)
            else {},
            "reject_cluster": e1_audit_inputs.get("reject_cluster")
            if isinstance(e1_audit_inputs.get("reject_cluster"), dict)
            else {},
            "full_utc_day_reject_cluster": e1_audit_inputs.get(
                "full_utc_day_reject_cluster"
            )
            if isinstance(e1_audit_inputs.get("full_utc_day_reject_cluster"), dict)
            else {},
            "participation_288_map_summary": e1_audit_inputs.get("participation_288_map_summary")
            if isinstance(e1_audit_inputs.get("participation_288_map_summary"), dict)
            else {},
            "daily_gate_conversion": e1_audit_inputs.get("daily_gate_conversion")
            if isinstance(e1_audit_inputs.get("daily_gate_conversion"), dict)
            else {},
            "highest_rejection_gate_ev": e1_audit_inputs.get("highest_rejection_gate_ev")
            if isinstance(e1_audit_inputs.get("highest_rejection_gate_ev"), dict)
            else {},
            "multi_day_roi_distribution": e1_audit_inputs.get("multi_day_roi_distribution")
            if isinstance(e1_audit_inputs.get("multi_day_roi_distribution"), dict)
            else {},
        },
        "positive_wallet_slice_selector_falsifier": {
            "generated_at": positive_wallet_slice_falsifier.get("generated_at"),
            "path": (
                "data/research/positive_wallet_slice_selector_falsifier_latest.json"
                if positive_wallet_slice_falsifier
                else None
            ),
            "verdict": positive_wallet_slice_falsifier.get("verdict"),
            "sign_inversion_wallets": positive_wallet_slice_falsifier.get(
                "sign_inversion_wallets"
            )
            if isinstance(
                positive_wallet_slice_falsifier.get("sign_inversion_wallets"), list
            )
            else [],
            "rows": positive_wallet_slice_falsifier.get("rows")
            if isinstance(positive_wallet_slice_falsifier.get("rows"), list)
            else [],
        },
        "two_arm_concentration_decomposition": {
            "generated_at": two_arm_concentration.get("generated_at"),
            "path": (
                "data/research/two_arm_concentration_decomposition_latest.json"
                if two_arm_concentration
                else None
            ),
            "arms": two_arm_concentration.get("arms")
            if isinstance(two_arm_concentration.get("arms"), list)
            else [],
            "seat_82c8_maturity_precommit": two_arm_concentration.get(
                "seat_82c8_maturity_precommit"
            )
            if isinstance(
                two_arm_concentration.get("seat_82c8_maturity_precommit"), dict
            )
            else {},
            "execution_status": two_arm_concentration.get("execution_status"),
        },
        "participation_288_map": {
            "generated_at": participation_288_map.get("generated_at"),
            "day_utc": participation_288_map.get("day_utc"),
            "path": _rel_path(participation_288_map_path, root),
            "summary": participation_288_map.get("summary")
            if isinstance(participation_288_map.get("summary"), dict)
            else {},
            "hour_band_aggregates": participation_288_map.get("hour_band_aggregates")
            if isinstance(participation_288_map.get("hour_band_aggregates"), list)
            else [],
        },
        "peer_active_idle_windows": peer_active_idle_windows,
        "pipeline_slo_and_standby_readiness": pipeline_slo_and_standby_readiness,
        "weekend_day_probe": weekend_day_probe,
        "coverage_gap_diagnosis": {
            "generated_at": coverage_gap_diagnosis.get("generated_at"),
            "window": coverage_gap_diagnosis.get("window")
            if isinstance(coverage_gap_diagnosis.get("window"), dict)
            else {},
            "summary": coverage_gap_diagnosis.get("summary")
            if isinstance(coverage_gap_diagnosis.get("summary"), dict)
            else {},
        },
        "coverage_gap_signal_supply": {
            "generated_at": coverage_gap_signal_supply.get("generated_at"),
            "summary": coverage_gap_signal_supply.get("summary")
            if isinstance(coverage_gap_signal_supply.get("summary"), dict)
            else {},
        },
        "routing_disambiguation": {
            "generated_at": routing_disambiguation.get("generated_at"),
            "summary": routing_disambiguation.get("summary")
            if isinstance(routing_disambiguation.get("summary"), dict)
            else {},
        },
        "campaign_lat_p1": {
            "generated_at": campaign_lat_packet.get("generated_at"),
            "summary": campaign_lat_packet.get("summary")
            if isinstance(campaign_lat_packet.get("summary"), dict)
            else {},
            "recommended_next_actions": campaign_lat_packet.get("recommended_next_actions", [])[:5]
            if isinstance(campaign_lat_packet.get("recommended_next_actions"), list)
            else [],
        },
        "routing_shadow_validation": {
            "generated_at": routing_shadow_validation.get("generated_at"),
            "summary": routing_shadow_validation.get("summary")
            if isinstance(routing_shadow_validation.get("summary"), dict)
            else {},
        },
        "selection_visibility_packet": {
            "generated_at": selection_visibility_packet.get("generated_at"),
            "summary": selection_visibility_packet.get("summary")
            if isinstance(selection_visibility_packet.get("summary"), dict)
            else {},
        },
        "routing_shadow_attribution_pin": {
            "generated_at": routing_shadow_attribution_pin.get("generated_at"),
            "summary": routing_shadow_attribution_pin.get("summary")
            if isinstance(routing_shadow_attribution_pin.get("summary"), dict)
            else {},
        },
        "execution_model": {
            "orders_per_submitted_window": execution.get("orders_per_submitted_window"),
            "orders_per_filled_window": execution.get("orders_per_filled_window"),
            "fill_rate_pct": execution.get("fill_rate_pct"),
            "copy_model_counts": execution.get("copy_model_counts") if isinstance(execution.get("copy_model_counts"), dict) else {},
            "drip_orders": execution_drip.get("orders"),
            "drip_fills": execution_drip.get("fills"),
            "drip_stop_saves": execution_drip.get("drip_stop_saves"),
            "drip_avg_entry_minus_source_vwap": execution_drip.get("avg_entry_minus_source_vwap"),
            "strong_orders": execution_strong.get("orders"),
            "strong_resolved_fills": execution_strong.get("resolved_fills"),
            "strong_pnl_usd": execution_strong.get("resolved_pnl_usd"),
            "maker_fallback_conversion": maker_fallback_conversion,
        },
        "market_facts": {
            "path": str(market_facts_path),
            "exists": market_facts_path.exists(),
            "summary_bullets": market_fact_bullets,
        },
        "active_set": {
            "member_count": configured_member_count,
            "seat_standdown": _live_seat_standdown(guard),
            "current_candidate_id": current_member.get("candidate_id") or guard.get("candidate_id"),
            "current_wallet": current_member.get("source_wallet") or guard.get("source_wallet"),
            "current_policy_id": current_member.get("policy_id") or guard.get("policy_id"),
            "live_members_today": live_members_today,
            "latest_rotation": {
                "direction_id": active_set_latest_rotation.get("direction_id") or active_set_overlay.get("direction_id"),
                "updated_at": active_set_latest_rotation.get("updated_at") or active_set_overlay.get("updated_at"),
                "demoted_wallets": _rotation_wallets(
                    active_set_latest_rotation,
                    list_key="demoted_wallets",
                    scalar_key="demoted_wallet",
                ),
                "admitted_wallets": _rotation_wallets(
                    active_set_latest_rotation,
                    list_key="admitted_wallets",
                    scalar_key="admitted_wallet",
                ),
                "post_patch_fresh_fill_basis": active_set_latest_rotation.get("post_patch_fresh_fill_basis")
                if isinstance(active_set_latest_rotation.get("post_patch_fresh_fill_basis"), dict)
                else {},
                "reason": active_set_latest_rotation.get("reason"),
            },
            "latest_auto_degrade": {
                "direction_id": active_set_latest_auto_degrade.get("summary", {}).get("direction_id")
                if isinstance(active_set_latest_auto_degrade.get("summary"), dict)
                else active_set_overlay.get("direction_id"),
                "updated_at": active_set_overlay.get("updated_at"),
                "candidate_id": active_set_latest_auto_degrade.get("candidate_id"),
                "wallet": active_set_latest_auto_degrade.get("source_wallet"),
                "policy_id": active_set_latest_auto_degrade.get("policy_id"),
                "status": active_set_latest_auto_degrade.get("status"),
                "replaces_existing_wallet": active_set_latest_auto_degrade.get(
                    "auto_degrade_replaces_existing_wallet"
                ),
                "members_count": active_set_overlay_runtime_count,
                "liveness_admissions_count": len(active_set_liveness_admissions),
                "latest_rotation_preserved": bool(active_set_latest_rotation),
            },
            "latest_mechanical_temporal_loss_demotion": (
                active_set_overlay.get("latest_mechanical_temporal_loss_demotion")
                if isinstance(
                    active_set_overlay.get("latest_mechanical_temporal_loss_demotion"),
                    dict,
                )
                else {}
            ),
            "runtime": {
                "member_count": len(guard_runtime_members),
                "overlay_enabled_member_count": active_set_overlay_enabled_count,
                "overlay_disabled_member_count": active_set_overlay_disabled_count,
                "overlay_runtime_member_delta": (
                    active_set_overlay_enabled_count - len(guard_runtime_members)
                ),
                "qualified_member_count": guard_active_runtime.get("qualified_member_count"),
                "selected_wallet": guard_runtime_selected.get("source_wallet"),
                "selected_candidate_id": guard_runtime_selected.get("candidate_id"),
                "candidate_pass_gate": {
                    "passed": guard_candidate_pass_gate.get("passed"),
                    "failed_checks": list(
                        guard_candidate_pass_gate.get("failed_checks") or []
                    ),
                    "status": guard_candidate_pass_gate.get("status"),
                    "live_protection_passed": (
                        guard_candidate_pass_gate.get("live_protection_gate") or {}
                    ).get("passed")
                    if isinstance(
                        guard_candidate_pass_gate.get("live_protection_gate"), dict
                    )
                    else None,
                },
                "wallets": guard_runtime_wallets,
                "d97_present": "0xd97ae021645712fe5cf73139049383a100cac068"
                in {wallet.lower() for wallet in guard_runtime_wallets},
                "total_loss_auto_disable": _runtime_total_loss_auto_disable_summary(guard_active_runtime),
            },
            "admission_wave": active_set_admission_wave,
            "e6db_2000_probe_cap_cut": e6db_2000_probe_cap_cut,
            "wave_gate_attribution": wave_gate_attribution_summary,
            "admitted_member_gate_3048": admitted_member_gate_3048_summary,
            "wave_repair_addendum": wave_repair_addendum_summary,
        },
        "member_rolling20": {
            "generated_at": member_rolling20.get("generated_at"),
            "active_member_count": member_rolling20.get("active_member_count") or configured_member_count,
            "ready_count": len(member_rolling20_ready_rows),
            "threshold_usd": member_rolling20.get("threshold_usd"),
            "mechanical_rotation_required": bool(member_rolling20.get("mechanical_rotation_required"))
            or bool(member_rolling20_trigger_rows),
            "trigger_rows": [
                {
                    "wallet": row.get("wallet"),
                    "rolling20_n": row.get("rolling20_n"),
                    "rolling20_pnl_usd": row.get("rolling20_pnl_usd"),
                }
                for row in member_rolling20_trigger_rows
            ],
            "sample_incomplete_negative_rows": [
                {
                    "wallet": row.get("wallet"),
                    "rolling20_n": row.get("rolling20_n"),
                    "rolling20_pnl_usd": row.get("rolling20_pnl_usd"),
                }
                for row in member_rolling20_incomplete_negative
            ],
            "rows": member_rolling20_rows,
        },
        "gates": {
            "queue_ready_for_live": queue_summary.get("ready_for_live"),
            "queue_ready_alive": queue_summary.get("ready_alive"),
            "hot_standby_ready": queue_summary.get("hot_standby_ready"),
            "hot_standby_required": queue_summary.get("hot_standby_required"),
            "hot_standby_gap": queue_summary.get("hot_standby_gap"),
            "queue_bench_alive_not_ready": queue_summary.get("bench_alive_not_ready"),
            "queue_dormant_stale_gt_48h": queue_summary.get("dormant_stale_gt_48h"),
            "queue_unknown_remote_liveness": queue_summary.get("unknown_remote_liveness"),
            "queue_recruitment_vintage_rule_pass": queue_summary.get("recruitment_vintage_rule_pass"),
            "queue_recruitment_vintage_max_share": queue_summary.get("recruitment_vintage_max_share"),
            "queue_market_cohort_bridge_candidates": queue_summary.get("market_cohort_bridge_candidates"),
            "queue_market_cohort_bridge_ranked": queue_summary.get("market_cohort_bridge_ranked"),
            "queue_market_cohort_bridge_defects": queue_summary.get("market_cohort_bridge_defects"),
            "queue_market_cohort_bridge_source_live_ready_picks": queue_summary.get(
                "market_cohort_bridge_source_live_ready_picks"
            ),
            "queue_market_cohort_bridge_bridged": queue_summary.get("market_cohort_bridge_bridged"),
            "queue_market_cohort_bridge_excluded": queue_summary.get("market_cohort_bridge_excluded"),
            "queue_market_cohort_bridge_excluded_reason_counts": queue_summary.get(
                "market_cohort_bridge_excluded_reason_counts"
            ),
            "queue_depth": queue_summary.get("queue_depth"),
            "queue_rotation_action": queue_summary.get("rotation_action"),
            "fresh_flow_probe": {
                "generated_at": fresh_flow_probe.get("generated_at"),
                "selected_wallets": fresh_flow_probe_summary.get("wallets"),
                "selected_pass_admission_threshold": fresh_flow_probe_summary.get(
                    "pass_admission_threshold"
                ),
                "selected_error_wallets": fresh_flow_probe_summary.get("error_wallets"),
                "cumulative_wallets": fresh_flow_probe_summary.get("cumulative_wallets"),
                "cumulative_pass_admission_threshold": fresh_flow_probe_summary.get(
                    "cumulative_pass_admission_threshold"
                ),
                "cumulative_error_wallets": fresh_flow_probe_summary.get("cumulative_error_wallets"),
                "paper_shadow_enrollments": fresh_flow_probe_summary.get(
                    "paper_shadow_enrollments"
                ),
                "rows": len(fresh_flow_probe_rows),
            },
            "e5_promotion_status": e5_gate.get("promotion_50_resolved_positive"),
            "e5_unresolved_paper_fills": e5_gate.get("unresolved_paper_fills"),
            "e5_no_old_unresolved": e5_gate.get("no_unresolved_inventory_older_than_one_window"),
            "e5_non_fallback_resolved": e5_non_fallback.get("resolved_paper_fills"),
            "e5_non_fallback_pnl_usd": e5_non_fallback.get("resolved_paper_pnl_usd"),
            "e5_signal_gated_signals": e5_signal_summary.get("signals"),
            "e5_signal_gated_filled_orders": e5_signal_summary.get("filled_orders"),
            "e5_signal_gated_resolved": e5_signal_book_summary.get("resolved_paper_fills"),
            "e5_signal_gated_pnl_usd": e5_signal_book_summary.get("resolved_paper_pnl_usd"),
            "e5_signal_gated_gate": e5_signal_gate.get("active_gate_decision"),
            "e5_book_aware_no_fallback_gate": e5_book_aware_gate.get(
                "promotion_150_prospective_no_fallback_positive"
            ),
            "e5_book_aware_gate_metric": e5_book_aware_gate.get("gate_metric"),
            "e5_book_aware_gate_source_file": e5_book_aware_gate.get("gate_source_file"),
            "e5_book_aware_append_only_resolved": e5_book_aware_ledger.get("distinct_resolved_fill_ids"),
            "e5_book_aware_current_source_resolved": e5_book_aware_ledger.get(
                "current_source_distinct_resolved_fill_ids"
            ),
            "e5_book_aware_prior_resolved": e5_book_aware_ledger.get("prior_distinct_resolved_fill_ids"),
            "e5_book_aware_source_restated_lower_than_prior": e5_book_aware_ledger.get(
                "source_restated_lower_than_prior"
            ),
            "e5_book_aware_monotonicity": (
                e5_book_aware_gate.get("monotonicity_tripwire") or {}
            ).get("status")
            if isinstance(e5_book_aware_gate.get("monotonicity_tripwire"), dict)
            else None,
            "e5_book_aware_summary_resolved": e5_book_aware_summary.get("resolved_paper_fills"),
            "e5_book_aware_summary_pnl_usd": e5_book_aware_summary.get("resolved_paper_pnl_usd"),
            "e5_book_aware_terminal_fill_rate_pct": e5_book_aware_summary.get(
                "terminal_maker_fill_rate_pct"
            ),
            "e5_5share_gate": e5_5share_gate.get("decision"),
            "e5_5share_cohort_sha256": (
                (e5_5share_regrade.get("source") or {}).get("cohort_sha256")
                if isinstance(e5_5share_regrade.get("source"), dict)
                else None
            ),
            "e5_5share_resolved": e5_5share_summary.get("resolved_distinct_executions"),
            "e5_5share_post_fee_pnl_usd": e5_5share_summary.get("resolved_post_fee_pnl_usd"),
            "e5_5share_post_fee_roi_pct": e5_5share_summary.get("resolved_post_fee_roi_pct"),
            "e5_5share_terminal_fill_rate_pct": e5_5share_summary.get("terminal_maker_fill_rate_pct"),
            "e5_5share_parity_violations": e5_5share_summary.get("copyintent_parity_violations"),
            "e5_5share_fallback_violations": e5_5share_summary.get("fallback_violations"),
            "e5_5share_max_notional_usd": e5_5share_summary.get("max_notional_usd"),
            "e5_live_actuator_status": e5_live_actuator.get("status"),
            "e5_live_actuator_generated_at": e5_live_actuator.get("generated_at"),
            "e5_live_actuator_orders_accepted": e5_live_actuator.get("orders_accepted"),
            "e5_live_actuator_intent_ids": e5_live_actuator.get("intent_ids"),
            "e5_live_actuator_book_hashes": e5_live_actuator.get("book_hashes"),
            "e5_divergence_paper_summary": (e5_divergence.get("paper") or {}).get("summary"),
            "e5_divergence_live_summary": (e5_divergence.get("live") or {}).get("summary"),
            "e5_divergence_optimism": e5_divergence.get("optimism_components"),
            "e5_live_actuator_result_sizes": [
                {
                    "order_id": row.get("order_id"),
                    "entry_price": row.get("entry_price"),
                    "size_usd": row.get("size_usd"),
                    "order_size": row.get("order_size"),
                    "response_fill_size_shares": row.get("response_fill_size_shares"),
                    "response_filled_size_usd": row.get("response_filled_size_usd"),
                    "status": row.get("status"),
                }
                for row in e5_live_results
            ],
            "e5_latest_live_order_proof": {
                "intent_id": latest_e5_live_order.get("intent_id"),
                "order_id": latest_e5_live_order.get("order_id"),
                "status": latest_e5_live_order.get("final_status") or latest_e5_live_order.get("status"),
                "updated_at": latest_e5_live_order.get("updated_at"),
                "limit_price": latest_e5_decision.get("limit_price"),
                "size_shares": latest_e5_decision.get("size_shares"),
                "size_usd": latest_e5_decision.get("size_usd"),
                "exchange_order_size": latest_e5_result.get("order_size"),
                "exchange_size_usd": latest_e5_result.get("size_usd"),
                "response_fill_size_shares": latest_e5_result.get("response_fill_size_shares"),
                "response_filled_size_usd": latest_e5_result.get("response_filled_size_usd"),
                "book_hash": (
                    (latest_e5_decision.get("e5_maker_first_live") or {}).get("book_hash")
                    if isinstance(latest_e5_decision.get("e5_maker_first_live"), dict)
                    else None
                ),
                "sizing_policy_id": (
                    (latest_e5_decision.get("wallet_copy") or {}).get("sizing_policy_id")
                    if isinstance(latest_e5_decision.get("wallet_copy"), dict)
                    else None
                ),
            },
            "e11_signal_status": e11_signal_status.get("status"),
            "e11_paper_quotes": e11_summary.get("paper_quotes"),
            "e11_open_orders": e11_summary.get("open_orders"),
            "e11_filled_orders": e11_summary.get("filled_orders"),
            "e11_pnl_usd": e11_summary.get("pnl_usd"),
            "e11_book_orders": e11_book_summary.get("book_evidence_orders"),
            "e11_non_fallback_book_orders": e11_book_summary.get("non_fallback_book_evidence_orders"),
            "e11_direct_fallback_orders": e11_book_summary.get("direct_fallback_orders"),
            "e7_observed_windows": e7_summary.get("observed_windows"),
            "e7_required_windows": (e7_summary.get("calibration_gate") or {}).get("required_observed_windows")
            if isinstance(e7_summary.get("calibration_gate"), dict)
            else None,
            "e7_penny_opportunities": e7_summary.get("penny_opportunities"),
        },
        "fee_event_check": {
            "latest_order_submitted_at": latest_order.get("submitted_at"),
            "latest_order_status": latest_order.get("status"),
            "expected_fee_gate_present": "expected_fee_gate" in latest_order_meta,
            "expected_vs_realized_fee_present": bool(
                latest_order.get("expected_vs_realized_fee")
                or latest_order_meta.get("expected_vs_realized_fee")
            ),
        },
        "brainless_ops": {
            "status": brainless.get("status"),
            "rotation_action": brainless.get("rotation_action"),
            "queue_ready_for_live": brainless.get("queue_ready_for_live"),
            "queue_depth": brainless.get("queue_depth"),
        },
        "boot_recovery_audit": {
            "status": boot_recovery.get("status"),
            "generated_at": boot_recovery.get("generated_at"),
            "defects": boot_recovery.get("defects")
            if isinstance(boot_recovery.get("defects"), list)
            else [],
            "stale_locks": ((boot_recovery.get("stale_remnants") or {}).get("stale_locks"))
            if isinstance(boot_recovery.get("stale_remnants"), dict)
            else [],
            "held_locks": ((boot_recovery.get("stale_remnants") or {}).get("held_locks"))
            if isinstance(boot_recovery.get("stale_remnants"), dict)
            else [],
        },
        "live_guard_restart": {
            "status": live_guard_restart_decision.get("status"),
            "generated_at": live_guard_restart_decision.get("generated_at"),
            "reason": live_guard_restart_decision.get("reason"),
            "generation_mismatch": live_guard_restart_decision.get("generation_mismatch"),
            "generation_verdict": live_guard_generation_verdict,
            "stale": live_guard_generation_verdict.get("stale"),
            "enforced_by_running_binary": coverage_enforced_by_running_binary,
            "loaded_generation": live_guard_restart_decision.get("loaded_generation")
            if isinstance(live_guard_restart_decision.get("loaded_generation"), dict)
            else {},
            "disk_generation": live_guard_restart_decision.get("disk_generation")
            if isinstance(live_guard_restart_decision.get("disk_generation"), dict)
            else {},
            "preflight_status": live_guard_restart_preflight.get("status"),
            "preflight_passed": live_guard_restart_preflight.get("passed"),
            "preflight_checks": live_guard_restart_preflight.get("checks")
            if isinstance(live_guard_restart_preflight.get("checks"), dict)
            else {},
            "actual_pid": live_guard_restart_execution.get("actual_pid"),
            "sweep_status": live_guard_restart_sweep.get("status"),
            "sweep_deleted": live_guard_restart_sweep.get("deleted")
            if isinstance(live_guard_restart_sweep.get("deleted"), list)
            else [],
            "sweep_retained": live_guard_restart_sweep.get("retained")
            if isinstance(live_guard_restart_sweep.get("retained"), list)
            else [],
            "latest_executed_restart": {
                "status": latest_executed_restart.get("status"),
                "generated_at": latest_executed_restart.get("generated_at"),
                "reason": latest_executed_restart.get("reason"),
                "preflight_status": latest_executed_restart_preflight.get("status"),
                "preflight_passed": latest_executed_restart_preflight.get("passed"),
                "preflight_checks": latest_executed_restart_preflight.get("checks")
                if isinstance(latest_executed_restart_preflight.get("checks"), dict)
                else {},
                "actual_pid": latest_executed_restart_execution.get("actual_pid"),
                "sweep_status": latest_executed_restart_sweep.get("status"),
                "sweep_deleted": latest_executed_restart_sweep.get("deleted")
                if isinstance(latest_executed_restart_sweep.get("deleted"), list)
                else [],
                "sweep_retained": latest_executed_restart_sweep.get("retained")
                if isinstance(latest_executed_restart_sweep.get("retained"), list)
                else [],
            },
            "generation_delta": {
                "status": guard_generation_delta.get("status"),
                "generated_at": guard_generation_delta.get("generated_at"),
                "reconstruction_status": guard_generation_delta.get("reconstruction_status"),
                "citation_allowed": guard_generation_delta.get("citation_allowed"),
                "loaded_commit": guard_generation_delta.get("loaded_commit"),
                "historical_loaded_file_count": guard_generation_delta.get(
                    "historical_loaded_file_count"
                ),
                "comparison_file_count": guard_generation_delta.get("comparison_file_count"),
                "changed_count": guard_generation_delta.get("changed_count"),
                "changed_paths": [
                    str(row.get("path"))
                    for row in guard_generation_delta.get("rows", [])
                    if isinstance(row, dict) and row.get("changed") and row.get("path")
                ],
                "live_mutation": guard_generation_delta.get("live_mutation"),
            },
        },
        "market_mining_cadence": {
            "status": market_mining.get("status"),
            "generated_at": market_mining.get("generated_at"),
            "ran_steps": market_mining.get("ran_steps") if isinstance(market_mining.get("ran_steps"), list) else [],
            "due_steps": market_mining.get("due_steps") if isinstance(market_mining.get("due_steps"), list) else [],
            "observed": market_mining.get("observed") if isinstance(market_mining.get("observed"), dict) else {},
            "delta_vs_prior_observed": market_mining.get("delta_vs_prior_observed")
            if isinstance(market_mining.get("delta_vs_prior_observed"), dict)
            else {},
            "null_cycle": market_mining.get("null_cycle") if isinstance(market_mining.get("null_cycle"), dict) else {},
            "next_scope": market_mining.get("next_scope") if isinstance(market_mining.get("next_scope"), dict) else {},
            "intake_window": {
                "lookback_complete": (market_scan.get("window") or {}).get("lookback_complete"),
                "oldest_trade_iso_seen": (market_scan.get("window") or {}).get("oldest_trade_iso_seen"),
                "newest_trade_iso_seen": (market_scan.get("window") or {}).get("newest_trade_iso_seen"),
            }
            if isinstance(market_scan.get("window"), dict)
            else {},
            "intake_exhaustion": {
                "class": market_scan_rate_limit.get("exhaustion_class"),
                "short_page_truncation": market_scan_rate_limit.get("short_page_truncation"),
                "api_pagination_cap_reached": market_scan_rate_limit.get("api_pagination_cap_reached"),
                "page_cap_exhausted": market_scan_rate_limit.get("page_cap_exhausted"),
                "budget_exhausted": market_scan_rate_limit.get("budget_exhausted"),
            },
        },
        "leaderboard_pipeline_heartbeat": {
            "generated_at": leaderboard_scan.get("updated_at"),
            "scan_status": leaderboard_scan.get("status"),
            "scan_returncode": ((leaderboard_scan.get("command") or {}).get("returncode"))
            if isinstance(leaderboard_scan.get("command"), dict)
            else None,
            "scan_duration_s": ((leaderboard_scan.get("command") or {}).get("duration_s"))
            if isinstance(leaderboard_scan.get("command"), dict)
            else None,
            "pipeline_requested": leaderboard_pipeline.get("pipeline_requested"),
            "pipeline_roster_wallets": leaderboard_pipeline.get("pipeline_roster_wallets"),
            "register_all_wallets": ((leaderboard_pipeline.get("summary") or {}).get("copy_all_fetched_wallets_to_registry"))
            if isinstance(leaderboard_pipeline.get("summary"), dict)
            else None,
            "registered_wallets": ((leaderboard_pipeline.get("summary") or {}).get("unique_wallets"))
            if isinstance(leaderboard_pipeline.get("summary"), dict)
            else None,
            "history_returncode": leaderboard_history_command.get("returncode"),
            "history_duration_s": leaderboard_history_command.get("duration_s"),
            "data_api_ingest_status": leaderboard_history_stdout.get("data_api_ingest_status"),
            "data_api_skip_count": leaderboard_history_stdout.get("data_api_skip_count"),
            "data_api_timeout": leaderboard_history_stdout.get("data_api_timeout")
            if isinstance(leaderboard_history_stdout.get("data_api_timeout"), dict)
            else {},
            "api_page_rows_fetched": leaderboard_scan_progress.get("rows_fetched"),
            "api_period_limits_requested": leaderboard_scan_progress.get("period_limits")
            if isinstance(leaderboard_scan_progress.get("period_limits"), dict)
            else {},
            "api_effective_page_limit": (
                (leaderboard_scan_progress.get("last_successful_page") or {}).get("limit")
                if isinstance(leaderboard_scan_progress.get("last_successful_page"), dict)
                else None
            ),
            "api_category_terminal_skips": leaderboard_scan_progress.get(
                "category_terminal_skip_count"
            ),
            "api_category_fetch_errors": leaderboard_scan_progress.get(
                "category_fetch_error_count"
            ),
            "api_last_error": leaderboard_scan_progress.get("last_error")
            if isinstance(leaderboard_scan_progress.get("last_error"), dict)
            else {},
        },
        "cohort_admission": {
            "generated_at": cohort_admission.get("generated_at"),
            "summary": cohort_admission.get("summary")
            if isinstance(cohort_admission.get("summary"), dict)
            else {},
            "top_four_way_candidate": cohort_admission.get("top_four_way_candidate")
            if isinstance(cohort_admission.get("top_four_way_candidate"), dict)
            else {},
            "paper_only": cohort_admission.get("paper_only"),
            "live_orders_allowed": cohort_admission.get("live_orders_allowed"),
            "registry_screen": cohort_admission.get("registry_fresh_flow_screen")
            if isinstance(cohort_admission.get("registry_fresh_flow_screen"), dict)
            else {},
            "registry_liveness_generated_at": registry_liveness.get("generated_at"),
            "registry_liveness_summary": registry_liveness.get("summary")
            if isinstance(registry_liveness.get("summary"), dict)
            else {},
            "observation_admission_count": len(registry_observation_admissions.get("wallets") or []),
            "runtime_observation_top_n": registry_observation_admissions.get("runtime_observation_top_n"),
            "queue_observation_members": queue_summary.get("registry_admission_observation_members"),
            "watch_tier_generated_at": watch_tier_poller.get("generated_at"),
            "watch_tier_source_wallets": watch_tier_source_wallets,
            "watch_tier_source_wallet_cohorts": watch_tier_source_wallet_cohorts,
            "admitted_watch_tier_polled_count": admitted_watch_tier_polled_count,
            "watch_tier_fresh_by_wallet": ((watch_tier_poller.get("summary") or {}).get("fresh_poll_only_by_wallet"))
            if isinstance(watch_tier_poller.get("summary"), dict)
            else {},
        },
        "factory_funnel": {
            "generated_at": factory_funnel.get("generated_at"),
            "day_utc": factory_funnel.get("day_utc"),
            "counts": factory_funnel.get("counts") if isinstance(factory_funnel.get("counts"), dict) else {},
            "enemy_line": factory_funnel.get("enemy_line")
            if isinstance(factory_funnel.get("enemy_line"), dict)
            else {},
            "first_materially_broken_link": factory_funnel.get("first_materially_broken_link")
            if isinstance(factory_funnel.get("first_materially_broken_link"), dict)
            else {},
        },
        "runtime_speed_baseline": {
            "status": runtime_speed.get("status"),
            "generated_at": runtime_speed.get("generated_at"),
            "baseline_created_at": runtime_speed.get("baseline_created_at"),
            "regression_threshold_ratio": runtime_speed.get("regression_threshold_ratio"),
            "regression_count": (runtime_speed.get("comparison") or {}).get("regression_count")
            if isinstance(runtime_speed.get("comparison"), dict)
            else None,
            "regressions": (runtime_speed.get("comparison") or {}).get("regressions", [])[:5]
            if isinstance(runtime_speed.get("comparison"), dict)
            else [],
            "metrics": {
                name: (row or {}).get("value")
                for name, row in (runtime_speed.get("metrics") or {}).items()
                if isinstance(row, dict)
            }
            if isinstance(runtime_speed.get("metrics"), dict)
            else {},
            "metric_statuses": {
                name: ((row or {}).get("status") or (row or {}).get("comparison_status_override"))
                for name, row in (runtime_speed.get("metrics") or {}).items()
                if isinstance(row, dict)
            }
            if isinstance(runtime_speed.get("metrics"), dict)
            else {},
            "metric_annotations": {
                name: {
                    key: row.get(key)
                    for key in (
                        "sample_age_s",
                        "stale_sample_threshold_s",
                        "sample_status",
                        "sampled_run_started_at",
                        "sampled_run_finished_at",
                        "post_stale_lock_reclaim_run_ordinal",
                        "step_duration_count",
                        "timed_sample_count",
                        "slowest_step",
                        "attribution_rule",
                        "comparison_rule",
                        "slow_threshold_s",
                        "return_code",
                    )
                    if key in row
                }
                for name, row in (runtime_speed.get("metrics") or {}).items()
                if isinstance(row, dict)
            }
            if isinstance(runtime_speed.get("metrics"), dict)
            else {},
            "persistence_counts": _runtime_speed_persistence_counts(
                runtime_speed if isinstance(runtime_speed, dict) else {},
                previous_digest if isinstance(previous_digest, dict) else {},
                order_flow_deadman if isinstance(order_flow_deadman, dict) else {},
                wallet_outflow_deadman if isinstance(wallet_outflow_deadman, dict) else {},
                fallback_scope_start_at=handoff_restart_scope_start_at,
            ),
            "next_action": runtime_speed.get("next_action"),
        },
        "scorecard_runtime_evidence": scorecard_runtime_evidence,
        "member_factory": member_factory if isinstance(member_factory, dict) else {},
        "member_factory_kpi": {
            "queue_depth": member_factory.get("queue_depth") if isinstance(member_factory.get("queue_depth"), dict) else {},
            "set_trajectory": member_factory.get("set_trajectory")
            if isinstance(member_factory.get("set_trajectory"), dict)
            else {},
            "member_freshness": member_factory.get("member_freshness")
            if isinstance(member_factory.get("member_freshness"), dict)
            else {},
            "hour_coverage": member_factory.get("hour_coverage")
            if isinstance(member_factory.get("hour_coverage"), dict)
            else {},
            "factory_throughput": member_factory.get("factory_throughput")
            if isinstance(member_factory.get("factory_throughput"), dict)
            else {},
            "series_census": member_factory.get("series_census")
            if isinstance(member_factory.get("series_census"), dict)
            else {},
            "defects": member_factory.get("defects") if isinstance(member_factory.get("defects"), list) else [],
        },
        "enabled_overflow_proposal": {
            "path": enabled_overflow_proposal.get("_path"),
            "generated_at": enabled_overflow_proposal.get("generated_at"),
            "authority": enabled_overflow_proposal.get("authority"),
            "status": enabled_overflow_proposal.get("status"),
            "live_mutation": enabled_overflow_proposal.get("live_mutation"),
            "recommended_design": (
                enabled_overflow_proposal.get("recommended_design", {}).get("name")
                if isinstance(enabled_overflow_proposal.get("recommended_design"), dict)
                else None
            ),
            "enabled_overlay_rows": (
                enabled_overflow_proposal.get("problem", {}).get("enabled_overlay_rows")
                if isinstance(enabled_overflow_proposal.get("problem"), dict)
                else None
            ),
            "current_enabled_overlay_rows": active_set_overlay_enabled_count,
            "current_disabled_overlay_rows": active_set_overlay_disabled_count,
            "runtime_target_member_count": (
                enabled_overflow_proposal.get("problem", {}).get("runtime_target_member_count")
                if isinstance(enabled_overflow_proposal.get("problem"), dict)
                else None
            ),
            "current_runtime_target_member_count": active_set_overlay.get("target_member_count_max"),
            "decision_gate": enabled_overflow_proposal.get("decision_gate"),
        },
        "watcher_gap": watcher_gap_summary,
        "active_set_dataapi_poller": {
            "status": active_set_poller.get("status"),
            "generated_at": active_set_poller.get("generated_at"),
            "active_set_wallets": active_set_poller_summary.get("active_set_wallets"),
            "source_wallet_count": len(active_set_poller.get("source_wallets") or []),
            "events_fetched": active_set_poller_summary.get("events_fetched"),
            "poll_only_signals": active_set_poller_summary.get("poll_only_signals"),
            "fresh_poll_only_signals": active_set_poller_summary.get("fresh_poll_only_signals"),
            "duplicate_tx_hash": active_set_poller_duplicates.get("duplicate_tx_hash"),
            "errored_wallets": active_set_poller_summary.get("errored_wallets"),
            "history_write_skipped": active_set_poller_summary.get("history_write_skipped"),
            "live_orders_allowed": active_set_poller.get("live_orders_allowed"),
            "paper_only": active_set_poller.get("paper_only"),
        },
        "active_set_rtds_premerge": {
            "status": active_set_rtds_premerge.get("status"),
            "wallets_refreshed": active_set_rtds_premerge.get("wallets_refreshed"),
            "new_matching_events": active_set_rtds_premerge.get("new_matching_events"),
            "retained_matching_rows": active_set_rtds_premerge.get("retained_matching_rows"),
            "max_rtds_catchup_lag_s": active_set_rtds_premerge.get("max_rtds_catchup_lag_s"),
            "status_counts": active_set_rtds_premerge.get("status_counts")
            if isinstance(active_set_rtds_premerge.get("status_counts"), dict)
            else {},
            "submitter_invariant": active_set_rtds_premerge.get("submitter_invariant"),
            "paper_only": active_set_rtds_premerge.get("paper_only"),
            "live_orders_allowed": active_set_rtds_premerge.get("live_orders_allowed"),
            "selected_wallet": active_set_rtds_selected_priority.get("selected_wallet"),
            "selected_priority_new_matching_events": active_set_rtds_selected_priority.get("new_matching_events"),
            "selected_priority_retained_matching_rows": active_set_rtds_selected_priority.get("retained_matching_rows"),
            "selected_priority_lag_s": active_set_rtds_selected_priority.get("rtds_catchup_lag_s"),
            "polygon_ws_premerge_matching_events": active_set_polygon_premerge_events,
        },
        "live_execution_probes": live_execution_probes,
        "own_impact_monitor": {
            "status": own_impact_monitor.get("status"),
            "named_cause": own_impact_monitor.get("named_cause"),
            "stale_inventory_rows": own_impact_monitor.get("stale_inventory_rows"),
            "wallet_eligible_orders": own_impact_monitor.get("wallet_eligible_orders"),
            "our_submits": own_impact_monitor.get("our_submits"),
            "our_fills": own_impact_monitor.get("our_fills"),
            "active_set_rtds_wallets_refreshed": own_impact_monitor.get("active_set_rtds_wallets_refreshed"),
            "active_set_rtds_new_matching_events": own_impact_monitor.get("active_set_rtds_new_matching_events"),
            "next_action": own_impact_monitor.get("next_action"),
        },
        "self_feed_vs_ledger": {
            "status": since_topup.get("reconciliation_status") or self_feed.get("status") or self_feed_summary.get("status"),
            "raw_status": self_feed.get("status") or self_feed_summary.get("status"),
            "primary_verdict": since_topup.get("primary_verdict"),
            "actual_basis_reconciled_verdict": since_topup.get("actual_basis_reconciled_verdict"),
            "raw_reconciliation_status": since_topup.get("raw_reconciliation_status"),
            "overlay_delta_usd": since_topup_overlay.get("overlay_delta_usd"),
            "generated_at": self_feed.get("generated_at"),
            "ledger_filled_tx_groups": self_feed_summary.get("ledger_filled_tx_groups"),
            "matched_ledger_tx_groups": self_feed_summary.get("matched_ledger_tx_groups"),
            "self_feed_tx_groups": self_feed_summary.get("self_feed_tx_groups"),
            "data_api_trade_rows": self_feed_summary.get("data_api_trade_rows"),
            "polygon_orderfilled_rows": self_feed_summary.get("polygon_orderfilled_rows"),
            "ledger_missing_self_feed_critical": self_feed_summary.get("ledger_missing_self_feed_critical"),
            "ledger_missing_self_feed_within_grace": self_feed_summary.get("ledger_missing_self_feed_within_grace"),
            "self_feed_missing_ledger_critical": self_feed_summary.get("self_feed_missing_ledger_critical"),
            "amount_mismatch_tx_groups": self_feed_summary.get("amount_mismatch_tx_groups"),
            "price_rounding_mismatch_tx_groups": self_feed_summary.get("price_rounding_mismatch_tx_groups"),
            "probable_split_fill_groups": self_feed_summary.get("probable_split_fill_groups"),
            "probable_split_fill_missing_tx_groups": self_feed_summary.get("probable_split_fill_missing_tx_groups"),
            "self_feed_missing_ledger_cost_usd": self_feed_summary.get("self_feed_missing_ledger_cost_usd"),
            "self_feed_missing_ledger_pnl_usd": self_feed_summary.get("self_feed_missing_ledger_pnl_usd"),
        },
        "cash_ledger_classification": {
            "generated_at": cash_ledger.get("generated_at"),
            "true_unrecorded_fill_candidate": cash_ledger_summary.get("true_unrecorded_fill_candidate"),
            "join_key_defect_probable_split_fill": cash_ledger_summary.get("join_key_defect_probable_split_fill"),
            "self_feed_missing_ledger_rows": cash_ledger_summary.get("self_feed_missing_ledger_rows"),
            "amount_mismatch_split_overlap": cash_ledger_summary.get("amount_mismatch_split_overlap"),
            "p0_guard_fill_recording_audit_required": p0_guard_fill_recording_audit_required,
            "raw_p0_guard_fill_recording_audit_required": cash_ledger_summary.get("p0_guard_fill_recording_audit_required"),
            "cost_by_class_usd": cash_ledger_summary.get("cost_by_class_usd"),
            "pnl_by_class_usd": cash_ledger_summary.get("pnl_by_class_usd"),
        },
        "self_feed_missing_trace": {
            "generated_at": self_feed_trace.get("generated_at"),
            "sampled": self_feed_trace_coverage.get("sampled"),
            "candidate_total": self_feed_trace_coverage.get("true_unrecorded_candidates_total"),
            "trace_counts": self_feed_trace_summary.get("trace_counts"),
            "cost_by_trace_class_usd": self_feed_trace_summary.get("cost_by_trace_class_usd"),
            "pnl_by_trace_class_usd": self_feed_trace_summary.get("pnl_by_trace_class_usd"),
            "b2_non_guard_fill_confirmed": self_feed_trace_summary.get("b2_non_guard_fill_confirmed"),
            "immediate_notify_required": self_feed_trace_summary.get("immediate_notify_required"),
            "gap_equation": self_feed_trace_gap,
        },
        "self_feed_full_ledger_retrace": {
            "generated_at": self_feed_full_retrace.get("generated_at"),
            "candidate_total": self_feed_full_retrace_summary.get("candidate_total"),
            "class_counts": self_feed_full_retrace_summary.get("class_counts"),
            "b1_confirmed_count": self_feed_full_retrace_summary.get("b1_confirmed_count"),
            "b2_suspect_count": self_feed_full_retrace_summary.get("b2_suspect_count"),
            "immediate_notify_required": self_feed_full_retrace_summary.get("immediate_notify_required"),
            "reconciliation_equation": self_feed_full_retrace_equation,
        },
        "self_feed_duckdb_benchmark": {
            "generated_at": self_feed_duckdb_benchmark.get("generated_at"),
            "status": self_feed_duckdb_benchmark.get("status"),
            "jsonl_rows": self_feed_duckdb_jsonl.get("rows"),
            "duckdb_rows": self_feed_duckdb_summary.get("rows"),
            "tx_groups": self_feed_duckdb_summary.get("tx_groups"),
            "cost_usd": self_feed_duckdb_summary.get("cost_usd"),
            "parity": self_feed_duckdb_benchmark.get("parity")
            if isinstance(self_feed_duckdb_benchmark.get("parity"), dict)
            else {},
            "jsonl_elapsed_ms": self_feed_duckdb_elapsed.get("jsonl_self_feed_scan"),
            "duckdb_elapsed_ms": self_feed_duckdb_elapsed.get("duckdb_wallet_copy_events_scan"),
            "gap_elapsed_ms": self_feed_duckdb_elapsed.get("duckdb_self_feed_gap_scan"),
            "gap_status": self_feed_duckdb_gap.get("status"),
            "gap_critical": self_feed_duckdb_gap_summary.get("self_feed_missing_ledger_critical"),
            "gap_missing_cost_usd": self_feed_duckdb_gap_summary.get("self_feed_missing_ledger_cost_usd"),
            "gap_split_groups": self_feed_duckdb_gap_summary.get("probable_split_fill_groups"),
            "gap_price_rounding": self_feed_duckdb_gap_summary.get("price_rounding_mismatch_tx_groups"),
            "gap_parity": self_feed_duckdb_gap.get("parity")
            if isinstance(self_feed_duckdb_gap.get("parity"), dict)
            else {},
            "classification_rows": self_feed_duckdb_packet_classification.get("self_feed_missing_ledger_rows"),
            "join_key_split": self_feed_duckdb_packet_classification.get(
                "join_key_defect_probable_split_fill"
            ),
            "true_unrecorded": self_feed_duckdb_packet_classification.get("true_unrecorded_fill_candidate"),
            "retrace_counts": self_feed_duckdb_packet_retrace.get("class_counts"),
            "overlay_pnl_usd": self_feed_duckdb_packet_overlay.get("pnl_usd"),
            "reconciled_actual_estimate_usd": self_feed_duckdb_packet_overlay.get(
                "reconciled_actual_estimate_usd"
            ),
            "recommendation_mode": self_feed_duckdb_packet_recommendation.get("mode"),
            "ledger_rewrite": self_feed_duckdb_packet_recommendation.get("ledger_rewrite"),
            "next_action": self_feed_duckdb_benchmark.get("next_action"),
        },
        "h2_external_redemptions": {
            "generated_at": h2_external_redemptions.get("generated_at"),
            "status": h2_external_redemptions.get("status"),
            "overlay_source_name": h2_external_redemptions.get("overlay_source_name"),
            "ledger_rewrite": h2_external_redemptions.get("ledger_rewrite"),
            "external_redeem_rows": (h2_external_redemptions.get("summary") or {}).get("external_redeem_rows")
            if isinstance(h2_external_redemptions.get("summary"), dict)
            else None,
            "confirmed_external_redeem_rows": (h2_external_redemptions.get("summary") or {}).get(
                "confirmed_external_redeem_rows"
            )
            if isinstance(h2_external_redemptions.get("summary"), dict)
            else None,
            "total_redeem_usdc": (h2_external_redemptions.get("summary") or {}).get("total_redeem_usdc")
            if isinstance(h2_external_redemptions.get("summary"), dict)
            else None,
            "max_abs_delta_usd": (h2_external_redemptions.get("summary") or {}).get("max_abs_delta_usd")
            if isinstance(h2_external_redemptions.get("summary"), dict)
            else None,
            "cash_diff_residual_usd": (h2_external_redemptions.get("acceptance") or {}).get(
                "cash_diff_residual_usd"
            )
            if isinstance(h2_external_redemptions.get("acceptance"), dict)
            else None,
            "residual_explained_by_external_redeems_usd": (h2_external_redemptions.get("acceptance") or {}).get(
                "residual_explained_by_external_redeems_usd"
            )
            if isinstance(h2_external_redemptions.get("acceptance"), dict)
            else None,
            "residual_unexplained_after_external_redeems_usd": (h2_external_redemptions.get("acceptance") or {}).get(
                "residual_unexplained_after_external_redeems_usd"
            )
            if isinstance(h2_external_redemptions.get("acceptance"), dict)
            else None,
            "anchor_relabel": (h2_external_redemptions.get("acceptance") or {}).get("anchor_relabel")
            if isinstance(h2_external_redemptions.get("acceptance"), dict)
            else None,
        },
        "h2_account_value_residual": {
            "generated_at": h2_account_value_residual.get("generated_at"),
            "status": (h2_account_value_residual.get("summary") or {}).get("status")
            if isinstance(h2_account_value_residual.get("summary"), dict)
            else None,
            "snapshot_count": (h2_account_value_residual.get("summary") or {}).get("snapshot_count")
            if isinstance(h2_account_value_residual.get("summary"), dict)
            else None,
            "residual_class": (h2_account_value_residual.get("summary") or {}).get("residual_class")
            if isinstance(h2_account_value_residual.get("summary"), dict)
            else None,
            "latest_cash_diff_residual_usd": (h2_account_value_residual.get("summary") or {}).get(
                "latest_cash_diff_residual_usd"
            )
            if isinstance(h2_account_value_residual.get("summary"), dict)
            else None,
            "open_position_mark_timing_ruled_out": (h2_account_value_residual.get("summary") or {}).get(
                "open_position_mark_timing_ruled_out"
            )
            if isinstance(h2_account_value_residual.get("summary"), dict)
            else None,
            "fee_or_dust_ruled_out": (h2_account_value_residual.get("summary") or {}).get(
                "fee_or_dust_ruled_out"
            )
            if isinstance(h2_account_value_residual.get("summary"), dict)
            else None,
            "next_action": (h2_account_value_residual.get("summary") or {}).get("next_action")
            if isinstance(h2_account_value_residual.get("summary"), dict)
            else None,
        },
        "residual_cash_diff_audit": {
            "generated_at": residual_cash_diff_audit.get("generated_at"),
            "status": residual_cash_diff_audit.get("status"),
            "canonical_residual_usd": (residual_cash_diff_audit.get("residual_reconciliation") or {}).get(
                "canonical_residual_usd"
            )
            if isinstance(residual_cash_diff_audit.get("residual_reconciliation"), dict)
            else None,
            "canonical_residual_classification": (
                residual_cash_diff_audit.get("residual_reconciliation") or {}
            ).get("canonical_residual_classification")
            if isinstance(residual_cash_diff_audit.get("residual_reconciliation"), dict)
            else None,
            "single_movement_match_status": (
                residual_cash_diff_audit.get("residual_reconciliation") or {}
            ).get("current_residual_single_movement_match_status")
            if isinstance(residual_cash_diff_audit.get("residual_reconciliation"), dict)
            else None,
            "scorecard_day_direct_matches": len(
                (residual_cash_diff_audit.get("residual_reconciliation") or {}).get(
                    "scorecard_day_residual_direct_tx_matches"
                )
                or []
            )
            if isinstance(residual_cash_diff_audit.get("residual_reconciliation"), dict)
            else None,
            "baseline_window_direct_matches": len(
                (residual_cash_diff_audit.get("residual_reconciliation") or {}).get(
                    "current_residual_direct_tx_matches"
                )
                or []
            )
            if isinstance(residual_cash_diff_audit.get("residual_reconciliation"), dict)
            else None,
            "other_counterparty_rows": len(residual_cash_diff_audit.get("other_counterparty_rows") or []),
            "conclusion": (residual_cash_diff_audit.get("residual_reconciliation") or {}).get("conclusion")
            if isinstance(residual_cash_diff_audit.get("residual_reconciliation"), dict)
            else None,
        },
        "scorecard_same_cut_basis_check": {
            "generated_at": scorecard_same_cut_basis.get("generated_at"),
            "status": scorecard_same_cut_basis.get("status"),
            "scorecard_generated_at": scorecard_same_cut_basis.get("scorecard_generated_at"),
            "json_totals": scorecard_same_cut_basis.get("json_totals")
            if isinstance(scorecard_same_cut_basis.get("json_totals"), dict)
            else {},
            "text_totals": scorecard_same_cut_basis.get("text_totals")
            if isinstance(scorecard_same_cut_basis.get("text_totals"), dict)
            else {},
            "count_match": scorecard_same_cut_basis.get("count_match"),
            "pnl_match": scorecard_same_cut_basis.get("pnl_match"),
        },
        "post_panic_integrity": {
            "generated_at": post_panic_integrity.get("generated_at"),
            "status": post_panic_integrity.get("status"),
            "checked_count": post_panic_integrity.get("checked_count"),
            "failure_count": post_panic_integrity.get("failure_count"),
            "repair_count": post_panic_integrity.get("repair_count"),
            "output": post_panic_integrity.get("output"),
        },
        "scheduler_ratchet": {
            "path": str(scheduler_ratchet_path.relative_to(root)),
            "generated_at": scheduler_ratchet.get("generated_at"),
            "status": scheduler_ratchet.get("status"),
            "clock_start_utc": scheduler_ratchet.get("clock_start_utc"),
            "clock_end_utc": scheduler_ratchet.get("clock_end_utc"),
            "paper_clock_rows_landed": (scheduler_ratchet.get("summary") or {}).get(
                "paper_clock_rows_landed"
            )
            if isinstance(scheduler_ratchet.get("summary"), dict)
            else None,
            "paper_clock_rows_resolved": (scheduler_ratchet.get("summary") or {}).get(
                "paper_clock_rows_resolved"
            )
            if isinstance(scheduler_ratchet.get("summary"), dict)
            else None,
            "paper_clock_post_fee_would_pnl_usd": (scheduler_ratchet.get("summary") or {}).get(
                "paper_clock_post_fee_would_pnl_usd"
            )
            if isinstance(scheduler_ratchet.get("summary"), dict)
            else None,
        },
        "scheduler_verdict": {
            "generated_at": scheduler_verdict.get("generated_at"),
            "summary": scheduler_verdict.get("summary")
            if isinstance(scheduler_verdict.get("summary"), dict)
            else {},
            "gate": scheduler_verdict.get("gate")
            if isinstance(scheduler_verdict.get("gate"), dict)
            else {},
            "clock": scheduler_verdict.get("clock")
            if isinstance(scheduler_verdict.get("clock"), dict)
            else {},
            "invariants": scheduler_verdict.get("invariants")
            if isinstance(scheduler_verdict.get("invariants"), dict)
            else {},
            "promotion": scheduler_verdict.get("promotion")
            if isinstance(scheduler_verdict.get("promotion"), dict)
            else {},
        },
        "scheduler_stratification": {
            "generated_at": scheduler_stratification.get("generated_at"),
            "summary": scheduler_stratification.get("summary")
            if isinstance(scheduler_stratification.get("summary"), dict)
            else {},
            "survival_rule": scheduler_stratification.get("survival_rule")
            if isinstance(scheduler_stratification.get("survival_rule"), dict)
            else {},
            "retirement": scheduler_retirement
            if isinstance(scheduler_retirement, dict)
            else {},
        },
        "pinned_tranche_economics": {
            "generated_at": pinned_tranche_economics.get("generated_at"),
            "status": pinned_tranche_economics.get("status"),
            "scorecard_generated_at": (pinned_tranche_economics.get("inputs") or {}).get(
                "scorecard_generated_at"
            )
            if isinstance(pinned_tranche_economics.get("inputs"), dict)
            else None,
            "resolved_pinned_fills": (pinned_tranche_economics.get("summary") or {}).get(
                "resolved_pinned_fills"
            )
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "pinned_filled_orders": (pinned_tranche_economics.get("summary") or {}).get(
                "pinned_filled_orders"
            )
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "pinned_status_counts": (pinned_tranche_economics.get("summary") or {}).get(
                "pinned_status_counts"
            )
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "pnl_usd": (pinned_tranche_economics.get("summary") or {}).get("pnl_usd")
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "win_rate_pct": (pinned_tranche_economics.get("summary") or {}).get("win_rate_pct")
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "breakeven_win_rate_pct": (pinned_tranche_economics.get("summary") or {}).get(
                "breakeven_win_rate_pct"
            )
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "wilson_95_lower_bound_win_rate_pct": (pinned_tranche_economics.get("summary") or {}).get(
                "wilson_95_lower_bound_win_rate_pct"
            )
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "wilson_lower_bound_gt_breakeven": (pinned_tranche_economics.get("summary") or {}).get(
                "wilson_lower_bound_gt_breakeven"
            )
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "worst_bucket": (pinned_tranche_economics.get("summary") or {}).get("worst_bucket")
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "probe_trigger_usd": (pinned_tranche_economics.get("summary") or {}).get(
                "probe_trigger_usd"
            )
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "distance_to_probe_trigger_usd": (pinned_tranche_economics.get("summary") or {}).get(
                "distance_to_probe_trigger_usd"
            )
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "sizing_gate_20_30z": (pinned_tranche_economics.get("summary") or {}).get(
                "sizing_gate_20_30z"
            )
            if isinstance(pinned_tranche_economics.get("summary"), dict)
            else None,
            "threshold_change_allowed": pinned_tranche_economics.get("threshold_change_allowed"),
        },
        "pinned_tranche_midday_due_check": {
            "generated_at": pinned_tranche_midday_due_check.get("generated_at"),
            "status": pinned_tranche_midday_due_check.get("status"),
            "scorecard_generated_at": (pinned_tranche_midday_due_check.get("inputs") or {}).get(
                "scorecard_generated_at"
            )
            if isinstance(pinned_tranche_midday_due_check.get("inputs"), dict)
            else None,
            "trigger_n": (pinned_tranche_midday_due_check.get("trigger") or {}).get(
                "resolved_pinned_fill_floor"
            )
            if isinstance(pinned_tranche_midday_due_check.get("trigger"), dict)
            else None,
            "resolved_pinned_fills": (pinned_tranche_midday_due_check.get("summary") or {}).get(
                "resolved_pinned_fills"
            )
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "pinned_filled_orders": (pinned_tranche_midday_due_check.get("summary") or {}).get(
                "pinned_filled_orders"
            )
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "pnl_usd": (pinned_tranche_midday_due_check.get("summary") or {}).get("pnl_usd")
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "win_rate_pct": (pinned_tranche_midday_due_check.get("summary") or {}).get("win_rate_pct")
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "breakeven_win_rate_pct": (pinned_tranche_midday_due_check.get("summary") or {}).get(
                "breakeven_win_rate_pct"
            )
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "wilson_95_lower_bound_win_rate_pct": (pinned_tranche_midday_due_check.get("summary") or {}).get(
                "wilson_95_lower_bound_win_rate_pct"
            )
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "wilson_lower_bound_gt_breakeven": (
                pinned_tranche_midday_due_check.get("summary") or {}
            ).get("wilson_lower_bound_gt_breakeven")
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "probe_trigger_usd": (pinned_tranche_midday_due_check.get("summary") or {}).get(
                "probe_trigger_usd"
            )
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "distance_to_probe_trigger_usd": (
                pinned_tranche_midday_due_check.get("summary") or {}
            ).get("distance_to_probe_trigger_usd")
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "sizing_gate_20_30z": (pinned_tranche_midday_due_check.get("summary") or {}).get(
                "sizing_gate_20_30z"
            )
            if isinstance(pinned_tranche_midday_due_check.get("summary"), dict)
            else None,
            "threshold_change_allowed": pinned_tranche_midday_due_check.get("threshold_change_allowed"),
        },
        "guard_fill_recording_audit": {
            "generated_at": guard_fill_audit.get("generated_at"),
            "true_candidate_population": guard_fill_audit_summary.get("true_candidate_population"),
            "sample_size": guard_fill_audit_summary.get("sample_size"),
            "b1_count": guard_fill_audit_summary.get("b1_count"),
            "b2_count": guard_fill_audit_summary.get("b2_count"),
            "b3_count": guard_fill_audit_summary.get("b3_count"),
            "immediate_notify_required": guard_fill_audit_summary.get("immediate_notify_required"),
            "audit_scope": guard_fill_audit_summary.get("audit_scope"),
            "actual_delta_usd": guard_fill_audit_equation.get("actual_delta_usd"),
            "ruled_residual_usd": guard_fill_audit_equation.get("ruled_residual_usd"),
            "sample_b1_effect_usd": guard_fill_audit_equation.get("sample_b1_effect_usd"),
            "sample_b2_effect_usd": guard_fill_audit_equation.get("sample_b2_effect_usd"),
            "b3_accounting_effect_usd": guard_fill_audit_equation.get("b3_accounting_effect_usd"),
            "unexplained_usd": guard_fill_audit_equation.get("unexplained_usd"),
        },
        "fill_toxicity": {
            "generated_at": fill_toxicity.get("generated_at"),
            "verdict": fill_toxicity_summary.get("verdict"),
            "signal_count": (fill_toxicity_summary.get("all_signals") or {}).get("count")
            if isinstance(fill_toxicity_summary.get("all_signals"), dict)
            else None,
            "signal_roi_pct": (fill_toxicity_summary.get("all_signals") or {}).get("roi_pct")
            if isinstance(fill_toxicity_summary.get("all_signals"), dict)
            else None,
            "live_fill_count": (fill_toxicity_summary.get("live_fills") or {}).get("count")
            if isinstance(fill_toxicity_summary.get("live_fills"), dict)
            else None,
            "live_fill_roi_pct": (fill_toxicity_summary.get("live_fills") or {}).get("roi_pct")
            if isinstance(fill_toxicity_summary.get("live_fills"), dict)
            else None,
            "toxicity_roi_pct": fill_toxicity_summary.get("toxicity_roi_pct"),
            "worst_wallet": fill_toxicity_worst_row.get("source_wallet"),
            "worst_bucket": fill_toxicity_worst_row.get("price_bucket"),
            "worst_toxicity_roi_pct": fill_toxicity_worst_row.get("toxicity_roi_pct"),
            "worst_live_fills": (fill_toxicity_worst_row.get("live_fills") or {}).get("count")
            if isinstance(fill_toxicity_worst_row.get("live_fills"), dict)
            else None,
        },
        "fill_conditioned_loss_attribution": {
            "generated_at": fill_loss_attribution.get("generated_at"),
            "resolved_fills": fill_loss_summary.get("resolved_fills"),
            "pnl_usd": fill_loss_summary.get("pnl_usd"),
            "roi_pct": fill_loss_summary.get("roi_pct"),
            "top_loss_scope": fill_loss_summary.get("top_loss_scope"),
            "post_floor_25_50_fills": fill_loss_summary.get("post_floor_25_50_fills"),
            "post_floor_25_50_rejects": fill_loss_summary.get("post_floor_25_50_rejects"),
            "top_loss_concentrations": [
                {
                    "dimension": row.get("dimension"),
                    "value": row.get("value"),
                    "fills": row.get("fills"),
                    "pnl_usd": row.get("pnl_usd"),
                    "roi_pct": row.get("roi_pct"),
                    "candidate_policy_change": row.get("candidate_policy_change"),
                }
                for row in fill_loss_top[:3]
                if isinstance(row, dict)
            ],
        },
        "window_time_reject_attribution": {
            "generated_at": window_time_reject_attribution.get("generated_at"),
            "tripwire_start_iso": window_time_reject_attribution.get("tripwire_start_iso"),
            "current_rows": window_time_current.get("rows"),
            "current_ruling_input": window_time_current.get("ruling_input"),
            "current_median_source_lateness_s": window_time_current.get("median_source_lateness_s"),
            "current_median_detection_latency_s": window_time_current.get("median_detection_latency_s"),
            "current_rtds_window_time_tripwire_rows": window_time_current.get("rtds_window_time_tripwire_rows"),
            "current_rtds_window_time_tripwire_status": window_time_current.get("rtds_window_time_tripwire_status"),
            "post_latest_rows": window_time_post_latest.get("rows"),
            "post_latest_ruling_input": window_time_post_latest.get("ruling_input"),
            "post_latest_median_source_lateness_s": window_time_post_latest.get("median_source_lateness_s"),
            "post_latest_median_detection_latency_s": window_time_post_latest.get("median_detection_latency_s"),
            "post_latest_rtds_window_time_tripwire_rows": window_time_post_latest.get(
                "rtds_window_time_tripwire_rows"
            ),
            "post_latest_rtds_window_time_tripwire_status": window_time_post_latest.get(
                "rtds_window_time_tripwire_status"
            ),
            "post_tripwire_rows": window_time_post_tripwire.get("rows"),
            "post_tripwire_ruling_input": window_time_post_tripwire.get("ruling_input"),
            "post_tripwire_median_source_lateness_s": window_time_post_tripwire.get(
                "median_source_lateness_s"
            ),
            "post_tripwire_median_detection_latency_s": window_time_post_tripwire.get(
                "median_detection_latency_s"
            ),
            "post_tripwire_rtds_window_time_tripwire_rows": window_time_post_tripwire.get(
                "rtds_window_time_tripwire_rows"
            ),
            "post_tripwire_rtds_window_time_tripwire_status": window_time_post_tripwire.get(
                "rtds_window_time_tripwire_status"
            ),
            "flow_money_classification": window_time_flow_money.get("classification"),
            "money_fill_count": window_time_flow_money.get("money_fill_count"),
            "distinct_filled_windows": window_time_flow_money.get("distinct_filled_windows"),
        },
        "inventory_skip_lifecycle": {
            "source": inventory_skip_source,
            "generated_at": active_set_starvation_packet.get("generated_at")
            if inventory_skip_trace
            else inventory_skip_lifecycle.get("generated_at"),
            "measure_only": inventory_skip_trace.get("measure_only"),
            "live_path_mutated": False
            if inventory_skip_trace.get("measure_only") is True
            else inventory_skip_lifecycle.get("live_path_mutated"),
            "record_count": inventory_skip_trace.get("record_count"),
            "recoverable_intent_estimate": inventory_skip_trace.get("recoverable_intent_estimate"),
            "skip_reason_counts_24h": dict(sorted(inventory_skip_counts.items())),
            "skip_reason_counts_24h_by_wallet": {
                wallet: dict(sorted(counter.items())) if isinstance(counter, dict) else counter
                for wallet, counter in sorted(inventory_skip_wallet_counts.items())
            },
            "sample_trace_count": len(inventory_skip_samples),
            "dominant_mechanism": inventory_skip_summary.get("dominant_mechanism"),
            "inventory_skip_total": inventory_skip_summary.get("inventory_skip_total"),
            "recoverable_inventory_skip_total": inventory_skip_summary.get(
                "recoverable_inventory_skip_total"
            ),
            "late_inventory_skip_total": inventory_skip_summary.get("late_inventory_skip_total"),
            "window_time_skip_total": inventory_skip_summary.get("window_time_skip_total"),
            "orders_submitted_this_cycle": inventory_skip_summary.get("orders_submitted_this_cycle"),
            "fresh_candidate_intents": inventory_skip_summary.get("fresh_candidate_intents"),
            "fresh_after_inventory_best_ask_gate": inventory_skip_summary.get(
                "fresh_after_inventory_best_ask_gate"
            ),
            "c4": inventory_skip_c4,
            "next_action": inventory_skip_summary.get("next_action"),
        },
        "toxicity_denylist": {
            "generated_at": toxicity_denylist.get("generated_at"),
            "source_report": toxicity_denylist.get("source_report"),
            "cell_count": toxicity_denylist.get("cell_count", len(toxicity_denylist_cells)),
            "criteria": toxicity_denylist.get("criteria") if isinstance(toxicity_denylist.get("criteria"), dict) else {},
            "cells": [
                {
                    "source_wallet": row.get("source_wallet"),
                    "price_bucket": row.get("price_bucket"),
                    "direction": row.get("direction"),
                    "signal_count": (row.get("all_signals") or {}).get("count")
                    if isinstance(row.get("all_signals"), dict)
                    else None,
                    "signal_roi_pct": (row.get("all_signals") or {}).get("roi_pct")
                    if isinstance(row.get("all_signals"), dict)
                    else None,
                    "our_fills": (row.get("live_fills") or {}).get("count")
                    if isinstance(row.get("live_fills"), dict)
                    else None,
                    "our_fill_roi_pct": row.get("live_fill_roi_pct")
                    if row.get("live_fill_roi_pct") is not None
                    else ((row.get("live_fills") or {}).get("roi_pct") if isinstance(row.get("live_fills"), dict) else None),
                }
                for row in toxicity_denylist_cells[:10]
                if isinstance(row, dict)
            ],
        },
        "strategy_map": {
            "generated_at": strategy_map.get("generated_at"),
            "authority": strategy_map.get("authority"),
            "rows": strategy_map_summary.get("rows"),
            "active_or_gated_rows": strategy_map_summary.get("active_or_gated_rows"),
            "fresh_rows": strategy_map_summary.get("fresh_rows"),
            "stale_rows": strategy_map_summary.get("stale_rows"),
            "stale_is_defect": strategy_map_summary.get("stale_is_defect"),
            "stale_ids": [row.get("id") for row in strategy_map_stale[:10] if isinstance(row, dict)],
        },
        "resource_utilization": {
            "generated_at": resource_utilization.get("generated_at"),
            "verdict": resource_utilization.get("verdict"),
            "registry_active_or_gated_lane_count": resource_utilization.get(
                "registry_active_or_gated_lane_count",
                resource_utilization.get("active_or_gated_lane_count"),
            ),
            "registry_status_occupancy_pct": resource_utilization.get(
                "registry_status_occupancy_pct"
            ),
            "productive_lane_count": resource_utilization.get("productive_lane_count"),
            "measured_max_lane_count": resource_utilization.get(
                "measured_max_lane_count",
                resource_utilization.get("memory_capped_max_lane_count"),
            ),
            "idle_lane_capacity": resource_utilization.get("idle_lane_capacity"),
            "productive_utilization_pct": resource_utilization.get(
                "productive_utilization_pct",
                resource_utilization.get("utilization_pct"),
            ),
            "cpu_headroom_pct": (resource_utilization.get("cpu") or {}).get("headroom_pct")
            if isinstance(resource_utilization.get("cpu"), dict)
            else None,
            "memory_headroom_to_pause_pct": (resource_utilization.get("memory_pressure") or {}).get(
                "headroom_to_pause_pct"
            )
            if isinstance(resource_utilization.get("memory_pressure"), dict)
            else None,
            "defect_open": resource_utilization_defect.get("open"),
            "defect_next": resource_utilization_defect.get("next"),
        },
        "strategy_decompiler_intake": {
            "generated_at": decompiler_intake.get("generated_at"),
            "eligible_wallets": decompiler_summary.get("eligible_wallets"),
            "selected_wallets": decompiler_summary.get("selected_wallets"),
            "events_scanned": decompiler_summary.get("events_scanned"),
            "top_wallet": decompiler_top.get("wallet"),
            "top_pnl_usd": decompiler_top.get("pnl_usd"),
            "top_roi_pct": decompiler_top.get("roi_pct"),
            "top_resolved_buy_events": decompiler_top.get("resolved_buy_events"),
            "top_unique_conditions": decompiler_top.get("unique_conditions"),
        },
        "followability_leaderboard": {
            "generated_at": followability.get("generated_at"),
            "selected_wallets": followability_summary.get("selected_wallets"),
            "wallets_scored": followability_summary.get("wallets_scored"),
            "early_commitment_windows": followability_summary.get("early_commitment_windows"),
            "top_wallet": followability_top.get("wallet"),
            "top_score": followability_top.get("followability_score"),
            "top_predictiveness_pct": followability_top.get("early_side_predictiveness_pct"),
            "top_win_rate_pct": followability_top.get("early_win_rate_pct"),
            "top_avg_continuation_usd": followability_top.get("avg_continuation_same_side_usd"),
            "top_eligible_windows": followability_top.get("eligible_windows"),
        },
        "full_universe_copyability": {
            "generated_at": full_universe.get("generated_at"),
            "registry_wallets": full_universe_summary.get("registry_wallets"),
            "wallets_scored": full_universe_summary.get("wallets_scored"),
            "wallets_with_any_evidence": full_universe_summary.get("wallets_with_any_evidence"),
            "wallets_with_replay": full_universe_summary.get("wallets_with_replay"),
            "positive_copy_pnl_wallets": full_universe_summary.get("positive_copy_pnl_wallets"),
            "ranked_queue_depth": full_universe_summary.get("ranked_queue_depth"),
            "prior_live_demotion_excluded": full_universe_summary.get("prior_live_demotion_excluded"),
            "top_wallet": full_universe_top.get("wallet"),
            "top_score": full_universe_top.get("copyability_score"),
            "top_admission_status": full_universe_top.get("admission_status"),
            "top_paper_pnl_usd": full_universe_top_replay.get("paper_pnl_usd"),
            "top_copyable_buy_events": full_universe_top_replay.get("copyable_buy_events"),
            "top_followability_score": full_universe_top_followability.get("score"),
            "legacy_twin_pointers": [
                {
                    "status": row.get("status"),
                    "canonical_path": row.get("canonical_path"),
                    "generated_at": row.get("generated_at"),
                }
                for row in full_universe_legacy_pointers
            ],
        },
        "wide_candidate_measurement": {
            "roster_generated_at": wide_capture_roster.get("generated_at"),
            "roster_source_counts": wide_capture_roster.get("source_counts")
            if isinstance(wide_capture_roster.get("source_counts"), dict)
            else {},
            "roster_direct_climb_members": [
                {
                    "wallet": row.get("address"),
                    "enabled": row.get("enabled"),
                    "tags": row.get("tags") if isinstance(row.get("tags"), list) else [],
                }
                for row in (wide_capture_roster.get("wallets") or [])
                if isinstance(row, dict)
                and "direct_climb_priority" in (row.get("tags") or [])
            ],
            "roster_paper_only": wide_capture_roster.get("paper_only"),
            "roster_live_orders_allowed": wide_capture_roster.get("live_orders_allowed"),
            "current_alpha_manifest": {
                "manifest_path": wide_current_manifest_path,
                "manifest_id": wide_current_manifest.get("manifest_id"),
                "generated_at": wide_current_manifest.get("generated_at"),
                "source_alpha_report": wide_current_manifest.get("source_alpha_report"),
                "source_alpha_status": wide_current_manifest.get("source_alpha_status"),
                "source_alpha_age_h": wide_current_manifest.get("source_alpha_age_h"),
                "lane_manifest_id": lane_manifest_meta.get("manifest_id"),
                "lane_digest_manifest_match": bool(
                    wide_current_manifest.get("manifest_id")
                    and wide_current_manifest.get("manifest_id")
                    == lane_manifest_meta.get("manifest_id")
                ),
                "admitted_wallets_blocked_by": wide_current_manifest.get(
                    "admitted_wallets_blocked_by"
                ),
                "summary": wide_current_manifest.get("summary")
                if isinstance(wide_current_manifest.get("summary"), dict)
                else {},
            },
            "alpha_metric_validity": {
                "summary": wide_alpha_metric_validity.get("summary")
                if isinstance(wide_alpha_metric_validity.get("summary"), dict)
                else {},
                "correlations": wide_alpha_metric_validity.get("correlations")
                if isinstance(wide_alpha_metric_validity.get("correlations"), dict)
                else {},
                "preregistered_falsifier": wide_alpha_metric_validity.get(
                    "preregistered_falsifier"
                )
                if isinstance(
                    wide_alpha_metric_validity.get("preregistered_falsifier"), dict
                )
                else {},
            },
            "depth_priority_frontier": {
                "generated_at": wide_depth_frontier.get("generated_at"),
                "summary": wide_depth_frontier.get("summary")
                if isinstance(wide_depth_frontier.get("summary"), dict)
                else {},
                "cells": [
                    {
                        "wallet": row.get("wallet"),
                        "wide_policy_fingerprint": row.get("wide_policy_fingerprint"),
                        "resolved": row.get("resolved"),
                        "gap_to_400": row.get("gap_to_400"),
                        "observed_resolved_signals_per_day": row.get(
                            "observed_resolved_signals_per_day"
                        ),
                        "rate_status": row.get("rate_status"),
                        "f1_walk_forward_admissible": row.get(
                            "f1_walk_forward_admissible"
                        ),
                    }
                    for row in (wide_depth_frontier.get("cells") or [])
                    if isinstance(row, dict)
                ],
            },
            "park_reconciliation_82c8": park_reconciliation_82c8,
            "repaired_eligible_slice_cohort_gap": {
                "generated_at": repaired_eligible_slice_cohort_gap.get("generated_at"),
                "cohort_manifest_id": repaired_eligible_slice_cohort_gap.get(
                    "cohort_manifest_id"
                ),
                "frontier_checksum": repaired_eligible_slice_cohort_gap.get(
                    "frontier_checksum"
                ),
                "summary": repaired_eligible_slice_cohort_gap.get("summary")
                if isinstance(repaired_eligible_slice_cohort_gap.get("summary"), dict)
                else {},
                "rows": repaired_eligible_slice_cohort_gap.get("rows")
                if isinstance(repaired_eligible_slice_cohort_gap.get("rows"), list)
                else [],
                "paper_only": repaired_eligible_slice_cohort_gap.get("paper_only"),
                "live_orders_allowed": repaired_eligible_slice_cohort_gap.get(
                    "live_orders_allowed"
                ),
                "promotion_authority": repaired_eligible_slice_cohort_gap.get(
                    "promotion_authority"
                ),
            },
            "order104_alpha_eligibility_delta": wide_order104_alpha.get(
                "eligibility_delta"
            )
            if isinstance(wide_order104_alpha.get("eligibility_delta"), dict)
            else {},
            "order106_slice_selection_delta": {
                "generated_at": wide_order106_delta.get("generated_at"),
                "summary": wide_order106_delta.get("summary"),
                "wallet_0484": next(
                    (
                        row
                        for row in (wide_order106_delta.get("rows") or [])
                        if isinstance(row, dict)
                        and row.get("wallet")
                        == "0x0484e64092ba4108c2786b61e6fc052d3bf41b1a"
                    ),
                    {},
                ),
            },
            "standby_park_registry": {
                "kind": standby_park_registry.get("kind"),
                "exclusions": standby_park_registry.get("exclusions") or [],
                "paper_only": standby_park_registry.get("paper_only"),
                "live_orders_allowed": standby_park_registry.get(
                    "live_orders_allowed"
                ),
            },
            "f1_accrual_stop_951b": f1_accrual_stop_951b,
            "order109_residual_ledger": {
                "generated_at": order109_residual_ledger.get("generated_at"),
                "raw_transfer_count": order109_residual_ledger.get(
                    "raw_transfer_count"
                ),
                "canonical_cash_diff_residual_usd": order109_residual_ledger.get(
                    "canonical_cash_diff_residual_usd"
                ),
                "ledger_row_count": order109_residual_ledger.get(
                    "ledger_row_count"
                ),
                "ledger_signed_sum_usd": order109_residual_ledger.get(
                    "ledger_signed_sum_usd"
                ),
                "acceptance": order109_residual_ledger.get("acceptance") or {},
            },
            "standings_generated_at": wide_candidate_standings.get("generated_at"),
            "alpha_status": wide_candidate_standings.get("alpha_status"),
            "source_freshness": wide_candidate_standings.get("source_freshness")
            if isinstance(wide_candidate_standings.get("source_freshness"), dict)
            else {},
            "policy_id": wide_candidate_standings.get("policy_id"),
            "prospective_cohort": wide_exact_policy.get("cohort")
            if isinstance(wide_exact_policy.get("cohort"), dict)
            else {},
            "prospective_summary": wide_exact_policy.get("summary")
            if isinstance(wide_exact_policy.get("summary"), dict)
            else {},
            "metadata_recovery": wide_exact_policy.get("metadata_recovery")
            if isinstance(wide_exact_policy.get("metadata_recovery"), dict)
            else {},
            "direct_climb_exact": wide_direct_climb_exact,
            "climb_backup": {
                "status": (
                    "ARMED_PAPER_ONLY"
                    if wide_climb_backup_state.get("paper_only") is True
                    and wide_climb_backup_state.get("live_orders_allowed") is False
                    and (
                        (
                            (wide_climb_backup_state.get("manifest") or {}).get(
                                "wallet_policy_identities"
                            )
                            or {}
                        ).get("0x00033f1089ff061813850e5135483bed39ce3b49")
                        or {}
                    ).get("wide_policy_fingerprint")
                    == "fd05d1bbcb24ba2f6e58f6e0337db000f24b766d83da8193d7e02fa441916317"
                    else "NOT_ARMED"
                ),
                "armed_at": wide_climb_backup_state.get("updated_at"),
                "wallet": "0x00033f1089ff061813850e5135483bed39ce3b49",
                "wide_policy_fingerprint": (
                    (
                        (
                            (wide_climb_backup_state.get("manifest") or {}).get(
                                "wallet_policy_identities"
                            )
                            or {}
                        ).get("0x00033f1089ff061813850e5135483bed39ce3b49")
                        or {}
                    ).get("wide_policy_fingerprint")
                ),
                "move_slice_keys": (
                    (
                        (
                            (wide_climb_backup_state.get("manifest") or {}).get(
                                "wallet_policy_identities"
                            )
                            or {}
                        ).get("0x00033f1089ff061813850e5135483bed39ce3b49")
                        or {}
                    ).get("move_slice_keys")
                    or []
                ),
                "paper_measurement_only": (
                    (wide_climb_backup_manifest.get("capture_watch_wallets") or [{}])[0]
                    .get("paper_measurement_only")
                ),
                "promotion_authority": (
                    (wide_climb_backup_manifest.get("capture_watch_wallets") or [{}])[0]
                    .get("promotion_authority")
                ),
                "live_orders_allowed": wide_climb_backup_state.get(
                    "live_orders_allowed"
                ),
                "summary": wide_climb_backup_state.get("summary")
                if isinstance(wide_climb_backup_state.get("summary"), dict)
                else {},
            },
            "terminal_reconciliation": wide_terminal_reconciliation,
            "direct_latency": {
                "samples": len(wide_receipt_lags_ms),
                "p95_receipt_to_fetch_ms": wide_receipt_p95_ms,
                "max_receipt_to_fetch_ms": max(wide_receipt_lags_ms)
                if wide_receipt_lags_ms
                else None,
                "gate_lte_5s": bool(
                    wide_receipt_p95_ms is not None and wide_receipt_p95_ms <= 5000.0
                ),
            },
            "immutable_alpha_manifest": {
                "manifest_id": wide_manifest.get("manifest_id"),
                "source_identity": wide_manifest.get("source_identity")
                if isinstance(wide_manifest.get("source_identity"), dict)
                else {},
                "summary": wide_manifest.get("summary")
                if isinstance(wide_manifest.get("summary"), dict)
                else {},
            },
            "frozen_capture_fingerprint_evidence": wide_frozen_capture_evidence,
            "diagnostic_broad_history_is_admission_evidence": wide_exact_policy.get(
                "diagnostic_broad_history_is_admission_evidence"
            ),
            "supervisor": {
                key: wide_supervisor.get(key)
                for key in (
                    "status",
                    "adopted_run_id",
                    "seed_run_id",
                    "managed_run_id",
                    "capture_pid",
                    "manifest",
                    "cycle_count",
                    "direct_event_cycles",
                    "event_handoff",
                    "updated_at_s",
                )
                if wide_supervisor.get(key) is not None
            },
            "supervisor_pipeline_deployment": _wide_supervisor_pipeline_deployment(
                wide_supervisor
            ),
            "summary": wide_candidate_standings.get("summary")
            if isinstance(wide_candidate_standings.get("summary"), dict)
            else {},
            "winner_wallets": wide_candidate_standings.get("winner_wallets")
            if isinstance(wide_candidate_standings.get("winner_wallets"), list)
            else [],
            "copyable_rate_reachability": {
                "generated_at": wide_copyable_rate_reachability.get("generated_at"),
                "paper_only": wide_copyable_rate_reachability.get("paper_only"),
                "live_orders_allowed": wide_copyable_rate_reachability.get(
                    "live_orders_allowed"
                ),
                "threshold_pct_unchanged": wide_copyable_rate_reachability.get(
                    "threshold_pct_unchanged"
                ),
                "summary": wide_copyable_rate_reachability.get("summary")
                if isinstance(wide_copyable_rate_reachability.get("summary"), dict)
                else {},
                "wallets": [
                    {
                        "wallet": row.get("wallet"),
                        "as_built_rate_pct": (
                            (row.get("as_built") or {}).get("copyable_rate_pct")
                        ),
                        "policy_addressable_rate_pct": (
                            (row.get("policy_addressable") or {}).get(
                                "copyable_rate_pct"
                            )
                        ),
                        "denominators_equal": (
                            (row.get("denominator_comparison") or {}).get("equal")
                        ),
                        "residual_taxonomy": row.get("excluded_attempt_taxonomy")
                        if isinstance(row.get("excluded_attempt_taxonomy"), dict)
                        else {},
                    }
                    for row in (wide_copyable_rate_reachability.get("wallets") or [])
                    if isinstance(row, dict)
                ],
            },
            "f3_batch_interval_attribution": {
                "generated_at": wide_f3_batch_interval_attribution.get(
                    "generated_at"
                ),
                "paper_only": wide_f3_batch_interval_attribution.get("paper_only"),
                "live_orders_allowed": wide_f3_batch_interval_attribution.get(
                    "live_orders_allowed"
                ),
                "copyable_rate_threshold_pct_unchanged": (
                    wide_f3_batch_interval_attribution.get(
                        "copyable_rate_threshold_pct_unchanged"
                    )
                ),
                "f3_lag_limit_s_unchanged": wide_f3_batch_interval_attribution.get(
                    "f3_lag_limit_s_unchanged"
                ),
                "instrumentation_completeness": (
                    wide_f3_batch_interval_attribution.get(
                        "instrumentation_completeness"
                    )
                    if isinstance(
                        wide_f3_batch_interval_attribution.get(
                            "instrumentation_completeness"
                        ),
                        dict,
                    )
                    else {}
                ),
                "summary": wide_f3_batch_interval_attribution.get("summary")
                if isinstance(wide_f3_batch_interval_attribution.get("summary"), dict)
                else {},
                "decision_rule": wide_f3_batch_interval_attribution.get(
                    "decision_rule"
                )
                if isinstance(
                    wide_f3_batch_interval_attribution.get("decision_rule"), dict
                )
                else {},
            },
            "frontier_deficit_partition": {
                "generated_at": wide_frontier_deficit_partition.get(
                    "generated_at"
                ),
                "paper_only": wide_frontier_deficit_partition.get("paper_only"),
                "measurement_only": wide_frontier_deficit_partition.get(
                    "measurement_only"
                ),
                "live_orders_allowed": wide_frontier_deficit_partition.get(
                    "live_orders_allowed"
                ),
                "source": wide_frontier_deficit_partition.get("source")
                if isinstance(wide_frontier_deficit_partition.get("source"), dict)
                else {},
                "partition": wide_frontier_deficit_partition.get("partition")
                if isinstance(
                    wide_frontier_deficit_partition.get("partition"), dict
                )
                else {},
                "single_check_flip_ranking": (
                    wide_frontier_deficit_partition.get(
                        "single_check_flip_ranking"
                    )
                    if isinstance(
                        wide_frontier_deficit_partition.get(
                            "single_check_flip_ranking"
                        ),
                        list,
                    )
                    else []
                ),
                "not_passed_prevalence": (
                    wide_frontier_deficit_partition.get(
                        "not_passed_prevalence"
                    )
                    if isinstance(
                        wide_frontier_deficit_partition.get(
                            "not_passed_prevalence"
                        ),
                        dict,
                    )
                    else {}
                ),
                "check_outcomes": wide_frontier_deficit_partition.get(
                    "check_outcomes"
                )
                if isinstance(
                    wide_frontier_deficit_partition.get("check_outcomes"), dict
                )
                else {},
            },
            "resolved_signal_accrual": {
                "generated_at": wide_resolved_signal_accrual.get(
                    "generated_at"
                ),
                "paper_only": wide_resolved_signal_accrual.get("paper_only"),
                "measurement_only": wide_resolved_signal_accrual.get(
                    "measurement_only"
                ),
                "source": wide_resolved_signal_accrual.get("source")
                if isinstance(wide_resolved_signal_accrual.get("source"), dict)
                else {},
                "measurement": wide_resolved_signal_accrual.get("measurement")
                if isinstance(
                    wide_resolved_signal_accrual.get("measurement"), dict
                )
                else {},
                "rows": wide_resolved_signal_accrual.get("rows")
                if isinstance(wide_resolved_signal_accrual.get("rows"), list)
                else [],
                "summary": wide_resolved_signal_accrual.get("summary")
                if isinstance(wide_resolved_signal_accrual.get("summary"), dict)
                else {},
            },
            "fingerprint_durability": {
                "generated_at": wide_fingerprint_durability.get(
                    "generated_at"
                ),
                "paper_only": wide_fingerprint_durability.get("paper_only"),
                "measurement_only": wide_fingerprint_durability.get(
                    "measurement_only"
                ),
                "source": wide_fingerprint_durability.get("source")
                if isinstance(wide_fingerprint_durability.get("source"), dict)
                else {},
                "quality_bars_unchanged": wide_fingerprint_durability.get(
                    "quality_bars_unchanged"
                )
                if isinstance(
                    wide_fingerprint_durability.get("quality_bars_unchanged"),
                    dict,
                )
                else {},
                "top3_wallets": wide_fingerprint_durability.get(
                    "top3_wallets"
                )
                if isinstance(
                    wide_fingerprint_durability.get("top3_wallets"), list
                )
                else [],
                "observation_frame": wide_fingerprint_durability.get(
                    "observation_frame"
                )
                if isinstance(
                    wide_fingerprint_durability.get("observation_frame"),
                    dict,
                )
                else {},
                "rows": wide_fingerprint_durability.get("rows")
                if isinstance(wide_fingerprint_durability.get("rows"), list)
                else [],
                "summary": wide_fingerprint_durability.get("summary")
                if isinstance(
                    wide_fingerprint_durability.get("summary"), dict
                )
                else {},
            },
            "selector_admissibility_divergence": {
                "generated_at": wide_selector_admissibility_divergence.get(
                    "generated_at"
                ),
                "paper_only": wide_selector_admissibility_divergence.get(
                    "paper_only"
                ),
                "measurement_only": (
                    wide_selector_admissibility_divergence.get(
                        "measurement_only"
                    )
                ),
                "source": wide_selector_admissibility_divergence.get("source")
                if isinstance(
                    wide_selector_admissibility_divergence.get("source"), dict
                )
                else {},
                "quality_bars_unchanged": (
                    wide_selector_admissibility_divergence.get(
                        "quality_bars_unchanged"
                    )
                )
                if isinstance(
                    wide_selector_admissibility_divergence.get(
                        "quality_bars_unchanged"
                    ),
                    dict,
                )
                else {},
                "predicate": wide_selector_admissibility_divergence.get(
                    "predicate"
                ),
                "admissibility_time_basis": (
                    wide_selector_admissibility_divergence.get(
                        "admissibility_time_basis"
                    )
                ),
                "wallets": wide_selector_admissibility_divergence.get(
                    "wallets"
                )
                if isinstance(
                    wide_selector_admissibility_divergence.get("wallets"),
                    list,
                )
                else [],
                "summary": wide_selector_admissibility_divergence.get(
                    "summary"
                )
                if isinstance(
                    wide_selector_admissibility_divergence.get("summary"),
                    dict,
                )
                else {},
            },
            "positive_slice_family": {
                "generated_at": wide_positive_slice_family.get("generated_at"),
                "status": wide_positive_slice_family.get("status"),
                "family_checksum": wide_positive_slice_family.get("family_checksum"),
                "wallet": wide_positive_slice_family.get("wallet"),
                "move_slice_keys": wide_positive_slice_family.get("move_slice_keys")
                if isinstance(wide_positive_slice_family.get("move_slice_keys"), list)
                else [],
                "summary": wide_positive_slice_family.get("summary")
                if isinstance(wide_positive_slice_family.get("summary"), dict)
                else {},
                "gates": wide_positive_slice_family.get("gates")
                if isinstance(wide_positive_slice_family.get("gates"), dict)
                else {},
                "admission_ready": wide_positive_slice_family.get("admission_ready"),
                "attrition_funnel": wide_positive_slice_family.get("attrition_funnel")
                if isinstance(wide_positive_slice_family.get("attrition_funnel"), dict)
                else {},
                "activation": {
                    key: (wide_positive_slice_family.get("activation") or {}).get(key)
                    for key in (
                        "status",
                        "live_mutation_allowed",
                        "family_checksum",
                        "evidence_checksum",
                        "activation_checksum",
                    )
                },
                "live_actuator": {
                    key: wide_positive_slice_family_actuator.get(key)
                    for key in (
                        "generated_at",
                        "status",
                        "terminal_reason",
                        "orders_submitted",
                        "orders_accepted",
                        "orders_filled",
                        "single_submitter",
                    )
                },
                "paper_only": wide_positive_slice_family.get("paper_only"),
                "live_orders_allowed": wide_positive_slice_family.get(
                    "live_orders_allowed"
                ),
            },
            "multiwallet_consensus": {
                "generated_at": wide_multiwallet_consensus.get("generated_at"),
                "status": wide_multiwallet_consensus.get("status"),
                "envelope_checksum": wide_multiwallet_consensus.get(
                    "envelope_checksum"
                ),
                "productive_lane_count": wide_multiwallet_consensus.get(
                    "productive_lane_count"
                ),
                "attrition_funnel": wide_multiwallet_consensus.get(
                    "attrition_funnel"
                )
                if isinstance(
                    wide_multiwallet_consensus.get("attrition_funnel"), dict
                )
                else {},
                "cells": {
                    name: {
                        key: cell.get(key)
                        for key in (
                            "cell_checksum",
                            "prospective_intents",
                            "resolved_terminals",
                            "post_fee_pnl_usd",
                            "new_intents_this_cycle",
                        )
                    }
                    for name, cell in (
                        wide_multiwallet_consensus.get("cells") or {}
                    ).items()
                    if isinstance(cell, dict)
                },
                "paper_only": wide_multiwallet_consensus.get("paper_only"),
                "live_orders_allowed": wide_multiwallet_consensus.get(
                    "live_orders_allowed"
                ),
            },
            "sequential_quorum_30s": {
                "generated_at": wide_sequential_quorum.get("generated_at"),
                "status": wide_sequential_quorum.get("status"),
                "preregistration_checksum": wide_sequential_quorum.get(
                    "preregistration_checksum"
                ),
                "cell_checksum": wide_sequential_quorum.get("cell_checksum"),
                "productive_lane_count": wide_sequential_quorum.get(
                    "productive_lane_count"
                ),
                "attrition_funnel": wide_sequential_quorum.get(
                    "attrition_funnel"
                )
                if isinstance(wide_sequential_quorum.get("attrition_funnel"), dict)
                else {},
                "summary": (wide_sequential_quorum.get("cell") or {}).get("summary")
                if isinstance(wide_sequential_quorum.get("cell"), dict)
                else {},
                "refusal_taxonomy": (wide_sequential_quorum.get("cell") or {}).get(
                    "refusal_taxonomy"
                )
                if isinstance(wide_sequential_quorum.get("cell"), dict)
                else {},
                "paper_only": wide_sequential_quorum.get("paper_only"),
                "live_orders_allowed": wide_sequential_quorum.get(
                    "live_orders_allowed"
                ),
            },
        },
        "wallet_copy_hot_history_accumulator": {
            "generated_at": hot_history_accumulator.get("generated_at"),
            "status": hot_history_accumulator.get("status"),
            "event_count": hot_history_accumulator.get("event_count"),
            "events_inserted": hot_history_accumulator.get("events_inserted"),
            "source_wallet_count": hot_history_accumulator.get(
                "source_wallet_count"
            ),
            "span_days": hot_history_accumulator.get("span_days"),
            "newest_source_event_age_s": (
                hot_history_accumulator.get("source_freshness") or {}
            ).get("newest_source_event_age_s"),
            "source_freshness_pass": (
                hot_history_accumulator.get("source_freshness") or {}
            ).get("pass"),
            "repoint_allowed": hot_history_accumulator.get("repoint_allowed"),
            "repoint_gate": hot_history_accumulator.get("repoint_gate"),
            "supplemental_export": hot_history_accumulator.get(
                "supplemental_export"
            )
            if isinstance(
                hot_history_accumulator.get("supplemental_export"), dict
            )
            else {},
        },
        "wallet_market_scan": {
            "generated_at": market_scan.get("generated_at"),
            "status": market_scan.get("status"),
            "active_wallets": market_scan_summary.get("active_wallets"),
            "new_active_wallets": market_scan_summary.get("new_active_wallets"),
            "wallets_ranked": market_scan_summary.get("wallets_ranked"),
            "trades_scanned": market_scan_summary.get("trades_scanned"),
            "crypto5m_trades_matched": market_scan_summary.get("crypto5m_trades_matched"),
            "pages_completed": market_scan_summary.get("pages_completed"),
            "scanned_alive_profitable": cohort_alive_profitable.get("scanned_alive_profitable"),
            "scanned_alive_profitable_definition": cohort_alive_profitable.get("definition"),
            "raw_cohort_live_ready_picks": cohort_alive_profitable.get("raw_live_ready_picks"),
            "alive_profitable_liveness_probe_generated_at": cohort_alive_profitable.get(
                "liveness_probe_generated_at"
            ),
            "alive_profitable_liveness_probe_rows": cohort_alive_profitable.get("liveness_probe_rows"),
            "alive_profitable_missing_liveness_rows": cohort_alive_profitable.get("missing_liveness_rows"),
            "alive_profitable_failed_liveness_reason_counts": cohort_alive_profitable.get(
                "failed_liveness_reason_counts"
            ),
            "alive_profitable_top_wallets": cohort_alive_profitable.get("top_alive_wallets"),
            "replay_status": market_scan_summary.get("replay_status"),
            "lookback_complete": (market_scan.get("window") or {}).get("lookback_complete")
            if isinstance(market_scan.get("window"), dict)
            else None,
            "page_cap_exhausted": market_scan_rate_limit.get("page_cap_exhausted"),
            "budget_exhausted": market_scan_rate_limit.get("budget_exhausted"),
            "route_class_counts": market_scan_route_counts,
            "top_wallet": market_scan_summary.get("top_wallet"),
            "top_rank_score": market_scan_summary.get("top_rank_score"),
            "cohort_replay_generated_at": market_cohort_replay.get("generated_at"),
            "cohort_replay_status": market_cohort_replay.get("status"),
            "cohort_size": market_cohort_summary.get("cohort_size"),
            "cohort_shadow_positive": market_cohort_summary.get("cohort_shadow_positive"),
            "live_ready_picks": market_cohort_summary.get("live_ready_picks"),
            "cohort_top_live_ready_wallet": market_cohort_summary.get("top_live_ready_wallet"),
            "cohort_top_shadow_positive_wallet": market_cohort_summary.get("top_shadow_positive_wallet"),
            "cohort_shadow_accrual": market_cohort_shadow_accrual,
        },
        "temporal_profitability": {
            "generated_at": temporal_profitability.get("generated_at"),
            "wallets_total": temporal_summary.get("wallets_total"),
            "wallets_with_resolved_btc5m_history": temporal_summary.get("wallets_with_resolved_btc5m_history"),
            "classification_counts": temporal_summary.get("classification_counts")
            if isinstance(temporal_summary.get("classification_counts"), dict)
            else {},
            "slice_label_counts": temporal_summary.get("slice_label_counts")
            if isinstance(temporal_summary.get("slice_label_counts"), dict)
            else {},
            "dead_band_candidate_count": temporal_summary.get("dead_band_candidate_count"),
            "watch_tier_feed_count": temporal_summary.get("watch_tier_feed_count"),
            "dow_weight_status": temporal_dow.get("status"),
            "current_dead_band_empty_confirmed": temporal_dow.get("current_dead_band_empty_confirmed"),
            "top_candidate_wallet": temporal_top_candidate.get("wallet"),
            "top_candidate_score": temporal_top_candidate.get("rank_score"),
            "top_candidate_classification": temporal_top_candidate.get("classification"),
            "top_candidate_slice_labels": temporal_top_candidate.get("slice_labels")
            if isinstance(temporal_top_candidate.get("slice_labels"), dict)
            else {},
            "top_candidate_dead_band": temporal_top_candidate.get("dead_band_18_22_utc")
            if isinstance(temporal_top_candidate.get("dead_band_18_22_utc"), dict)
            else {},
        },
        "source_active_policy_replay": {
            "generated_at": source_active_replay.get("generated_at"),
            "batch_id": source_active_replay.get("batch_id"),
            "targets_selected": source_active_replay_summary.get("targets_selected"),
            "wallets_replayed": source_active_replay_summary.get("wallets_replayed"),
            "raw_rows_seen": source_active_replay_summary.get("raw_rows_seen"),
            "normalized_btc5m_buy_events": source_active_replay_summary.get("normalized_btc5m_buy_events"),
            "api_error_wallets": source_active_replay_summary.get("api_error_wallets"),
            "manifest_files_after": source_active_replay_summary.get("manifest_files_after"),
            "already_replayed_wallets_skipped": source_active_replay_criteria.get(
                "already_replayed_wallets_skipped"
            ),
            "stop_reason_counts": source_active_replay_stop_reasons,
            "manifest_files": len(manifest_files),
            "manifest_updated_at": temporal_supplemental_manifest.get("updated_at")
            if isinstance(temporal_supplemental_manifest, dict)
            else None,
            "manifest_last_batch_id": temporal_supplemental_manifest.get("last_batch_id")
            if isinstance(temporal_supplemental_manifest, dict)
            else None,
        },
        "source_active_cohort": {
            "generated_at": source_active_cohort.get("generated_at"),
            "wallet_count": source_active_cohort.get("wallet_count"),
            "source_active_cumulative": source_active_cohort.get("source_active_cumulative")
            if isinstance(source_active_cohort.get("source_active_cumulative"), dict)
            else {},
            "latest_pass": source_active_cohort.get("latest_source_active_pass_window")
            if isinstance(source_active_cohort.get("latest_source_active_pass_window"), dict)
            else {},
            "external_liveness_cumulative": source_active_cohort.get("external_liveness_cumulative")
            if isinstance(source_active_cohort.get("external_liveness_cumulative"), dict)
            else {},
        },
        "a3e0_midnight_bundle": {
            "generated_at": a3e0_midnight_bundle.get("generated_at"),
            "status": a3e0_midnight_bundle.get("status"),
            "activate_not_before_utc": a3e0_midnight_bundle.get("activate_not_before_utc"),
            "live_mutation_before_arm": a3e0_midnight_bundle.get("live_mutation_before_arm"),
            "current_gate": a3e0_midnight_bundle.get("current_gate")
            if isinstance(a3e0_midnight_bundle.get("current_gate"), dict)
            else {},
            "pending_midnight_evidence": a3e0_midnight_bundle.get("pending_midnight_evidence")
            if isinstance(a3e0_midnight_bundle.get("pending_midnight_evidence"), dict)
            else {},
        },
        "focused_candidate_p1": {
            "generated_at": focused_candidate_p1.get("generated_at"),
            "wallet": focused_candidate_p1.get("wallet"),
            "history_depth": focused_candidate_p1.get("history_depth")
            if isinstance(focused_candidate_p1.get("history_depth"), dict)
            else {},
            "old_vs_new": focused_candidate_p1.get("old_vs_new")
            if isinstance(focused_candidate_p1.get("old_vs_new"), dict)
            else {},
            "temporal_hour_match": focused_candidate_p1.get("temporal_hour_match")
            if isinstance(focused_candidate_p1.get("temporal_hour_match"), dict)
            else {},
            "concentration": focused_candidate_p1.get("concentration")
            if isinstance(focused_candidate_p1.get("concentration"), dict)
            else {},
            "decision": focused_candidate_p1.get("decision")
            if isinstance(focused_candidate_p1.get("decision"), dict)
            else {},
        },
        "winner_variation_siblings": {
            "generated_at": winner_variation.get("generated_at"),
            "status": winner_variation.get("status"),
            "paper_only": winner_variation.get("paper_only"),
            "live_orders_allowed": winner_variation.get("live_orders_allowed"),
            "epoch_id": winner_variation_epoch.get("epoch_id"),
            "source_wallet": winner_variation_parent.get("source_wallet"),
            "policy_id": winner_variation_parent.get("policy_id"),
            "copy_model": winner_variation_parent.get("copy_model"),
            "lane_count": winner_variation_summary.get("lane_count"),
            "sibling_count": winner_variation_summary.get("sibling_count"),
            "freshness_siblings_s": winner_variation_summary.get("freshness_siblings_s"),
            "paper_twin_lane_id": winner_variation_summary.get("paper_twin_lane_id"),
            "ledger_orders_in_epoch": winner_variation_summary.get("ledger_orders_in_epoch"),
            "best_sibling_lane_id": winner_variation_summary.get("best_sibling_lane_id"),
            "best_sibling_roi_diff_pp": winner_variation_summary.get("best_sibling_roi_diff_pp"),
            "best_sibling_resolved_fills": winner_variation_summary.get("best_sibling_resolved_fills"),
            "best_sibling_gate_status": winner_variation_best_gate.get("status"),
            "best_sibling_pnl_usd": winner_variation_best_evidence.get("estimated_paper_pnl_usd"),
            "best_sibling_roi_pct": winner_variation_best_evidence.get("estimated_paper_roi_pct"),
            "single_submitter_invariant": winner_variation.get("single_submitter_invariant"),
        },
        "temporal_watch_tier_probe_apply": {
            "generated_at": temporal_probe_apply.get("generated_at"),
            "status": temporal_probe_apply.get("status"),
            "feed_candidates": temporal_probe_apply_summary.get("feed_candidates"),
            "temporal_applied": temporal_probe_apply_summary.get("temporal_applied"),
            "configured_wallets_before": temporal_probe_apply_summary.get("configured_wallets_before"),
            "configured_wallets_after": temporal_probe_apply_summary.get("configured_wallets_after"),
            "added_wallets": temporal_probe_apply_summary.get("added_wallets")
            if isinstance(temporal_probe_apply_summary.get("added_wallets"), list)
            else [],
            "already_present_wallets": temporal_probe_apply_summary.get("already_present_wallets")
            if isinstance(temporal_probe_apply_summary.get("already_present_wallets"), list)
            else [],
            "skipped_wallets": temporal_probe_skipped_wallets,
            "single_submitter_invariant": temporal_probe_apply_summary.get("single_submitter_invariant"),
        },
        "watch_tier_shadow_ev": {
            "generated_at": watch_tier_shadow_ev.get("generated_at"),
            "status": watch_tier_shadow_summary.get("status", watch_tier_shadow_ev.get("status")),
            "configured_wallets": watch_tier_shadow_summary.get("configured_wallets"),
            "eligible_signals": watch_tier_shadow_summary.get("eligible_signals"),
            "resolved_signals": watch_tier_shadow_summary.get("resolved_signals"),
            "readmission_ruling_due": watch_tier_shadow_summary.get("readmission_ruling_due"),
            "wallets_due": watch_tier_shadow_summary.get("wallets_due")
            if isinstance(watch_tier_shadow_summary.get("wallets_due"), list)
            else [],
            "top_wallet": watch_tier_shadow_top.get("source_wallet"),
            "top_resolved_signals": watch_tier_shadow_top.get("resolved_signals"),
            "top_pnl_usd": watch_tier_shadow_top.get("pnl_usd"),
            "top_roi_pct": watch_tier_shadow_top.get("roi_pct"),
            "top_status": watch_tier_shadow_top.get("status"),
        },
        "weekend_specialist_stakeout": {
            "generated_at": weekend_stakeout.get("generated_at"),
            "status": weekend_stakeout.get("stakeout_status") or weekend_stakeout.get("status"),
            "fresh_alerts": len(weekend_stakeout.get("fresh_alerts") or []),
            "poll_alerts": len(weekend_stakeout.get("poll_alerts") or []),
            "candidates": len(weekend_stakeout.get("weekend_candidates") or []),
            "ac05_proven_positive": weekend_stakeout_ac05.get("proven_positive_n_gte_20"),
            "ac05_weekend_n": weekend_stakeout_ac05_weekend.get("resolved_trades"),
            "ac05_weekend_roi_pct": weekend_stakeout_ac05_weekend.get("roi_pct"),
            "c03c_status": weekend_stakeout_c03c_watch.get("status"),
            "c03c_resolved_signals": weekend_stakeout_c03c_watch.get("resolved_signals"),
            "c03c_roi_pct": weekend_stakeout_c03c_watch.get("roi_pct"),
        },
        "sub25_accounting_spot_check": {
            "generated_at": sub25_spot_check.get("generated_at"),
            "sampled_orders": sub25_spot_summary.get("sampled_orders"),
            "sampled_unique_markets": sub25_spot_summary.get("sampled_unique_markets"),
            "sampled_unique_wallets": sub25_spot_summary.get("sampled_unique_wallets"),
            "resolved_orders": sub25_spot_summary.get("resolved_orders"),
            "wins": sub25_spot_summary.get("wins"),
            "losses": sub25_spot_summary.get("losses"),
            "token_outcome_mismatches": sub25_spot_summary.get("token_outcome_mismatches"),
            "conclusion": sub25_spot_summary.get("conclusion"),
            "gate": sub25_spot_summary.get("gate"),
        },
        "btc5m_live_paper_fleet": {
            "generated_at": btc5m_fleet.get("generated_at"),
            "fleet_size": btc5m_fleet_summary.get("fleet_size"),
            "matrix_rows": btc5m_fleet_summary.get("matrix_rows"),
            "matrix_windows": btc5m_fleet_summary.get("matrix_windows"),
            "fleet_wallets_with_matrix_rows": btc5m_fleet_summary.get("fleet_wallets_with_matrix_rows"),
            "ready_queue_wallets": btc5m_fleet_summary.get("ready_queue_wallets"),
            "positive_copy_pnl_wallets": btc5m_fleet_summary.get("positive_copy_pnl_wallets"),
            "top_wallet": btc5m_fleet_summary.get("top_wallet"),
            "top_score": btc5m_fleet_summary.get("top_score"),
            "top50_matrix_coverage": btc5m_fleet_top50,
            "coverage_defect": btc5m_fleet_summary.get("coverage_defect"),
            "coverage_defect_next_action": btc5m_fleet_summary.get("coverage_defect_next_action"),
        },
        "btc5m_two_sided_prime": {
            "generated_at": two_sided_prime.get("generated_at"),
            "accepted_events": two_sided_summary.get("accepted_events"),
            "resolved_windows": two_sided_summary.get("resolved_windows"),
            "paired_markets": two_sided_summary.get("paired_markets"),
            "pair_sum_candidates": two_sided_summary.get("pair_sum_candidates"),
            "pair_sum_frequency_pct": two_sided_summary.get("pair_sum_frequency_pct"),
            "two_sided_wallets": two_sided_summary.get("two_sided_wallets"),
            "top_mechanism": two_sided_summary.get("top_mechanism"),
            "top_ev_per_day_usd": two_sided_summary.get("top_ev_per_day_usd"),
            "top_status": two_sided_top.get("status"),
            "top_oos_trades": two_sided_top.get("oos_trades"),
            "top_oos_pnl_usd": two_sided_top.get("oos_pnl_usd"),
        },
        "btc5m_morning_ranked_table": {
            "generated_at": morning_table.get("generated_at"),
            "rows": morning_summary.get("rows"),
            "holdout_passed_rows": morning_summary.get("holdout_passed_rows"),
            "matrix_coverage_none_rows": morning_summary.get("matrix_coverage_none_rows"),
            "top_rank": morning_top.get("rank"),
            "top_mechanism": morning_top.get("mechanism_id"),
            "top_candidate": morning_top.get("candidate_id"),
            "top_family": morning_top.get("family"),
            "top_status": morning_top.get("status"),
            "top_ev_per_day_usd": morning_top.get("ev_per_day_usd"),
            "top_oos_trades": morning_top.get("oos_trades"),
            "top_proposed_funding_size_usd": morning_top.get("proposed_funding_size_usd"),
        },
        "btc5m_structural_scalp_paper_lane": {
            "refresh_status": structural_scalp_refresh.get("status"),
            "refresh_generated_at": structural_scalp_refresh.get("generated_at"),
            "refresh_forward_fills": structural_scalp_refresh.get("forward_fills"),
            "refresh_forward_pnl_usd": structural_scalp_refresh.get("forward_pnl_usd"),
            "refresh_forward_span_days": structural_scalp_refresh.get("forward_span_days"),
            "refresh_current_intents": structural_scalp_refresh.get("current_intents"),
            "refresh_error": structural_scalp_refresh.get("error"),
            "generated_at": structural_scalp_lane.get("generated_at"),
            "lane_id": structural_scalp_lane.get("lane_id"),
            "mechanism_id": structural_scalp_lane.get("mechanism_id"),
            "paper_only": structural_scalp_lane.get("paper_only"),
            "live_orders_allowed": structural_scalp_lane.get("live_orders_allowed"),
            "paper_fills": structural_scalp_summary.get("paper_fills"),
            "paper_orders": structural_scalp_summary.get("paper_orders"),
            "paper_pnl_usd": structural_scalp_summary.get("paper_pnl_usd"),
            "study_ev_per_day_usd": structural_scalp_summary.get("study_ev_per_day_usd"),
            "all_time_pnl_usd": structural_scalp_all_time.get("pnl_usd"),
            "all_time_fills": structural_scalp_all_time.get("fills"),
            "forward_fills": structural_scalp_forward.get("fills"),
            "forward_pnl_usd": structural_scalp_forward.get("pnl_usd"),
            "forward_ev_per_day_usd": structural_scalp_forward.get("ev_per_day_usd"),
            "live_gate_status": structural_scalp_live_gate.get("status"),
            "ready_for_live": structural_scalp_live_gate.get("ready_for_live"),
            "next": structural_scalp_live_gate.get("next"),
            "history": (structural_scalp_lane.get("inputs") or {}).get("history"),
            "source_cache": (structural_scalp_lane.get("inputs") or {}).get("source_cache"),
            "cached_forward_source_events": (structural_scalp_lane.get("inputs") or {}).get("cached_forward_source_events"),
            "newest_source_event_age_s": (structural_scalp_lane.get("inputs") or {}).get("newest_source_event_age_s"),
            "freshness_pass": (structural_scalp_lane.get("inputs") or {}).get("freshness_pass"),
            "frozen_history_audit_status": frozen_history_audit.get("status"),
            "frozen_history_consumer_count": len(frozen_history_audit.get("frozen_source_consumers") or []),
            "stale_tainted_conclusion_count": len(frozen_history_audit.get("stale_tainted_standing_conclusions") or []),
        },
        "btc5m_structural_scalp_promotion_prep": {
            "generated_at": structural_scalp_promotion.get("generated_at"),
            "status": structural_scalp_promotion.get("status"),
            "forward_window": structural_scalp_promotion.get("forward_window"),
            "fee_aware_economics": structural_scalp_promotion.get("fee_aware_economics"),
            "capacity_at_live_size": structural_scalp_promotion.get("capacity_at_live_size"),
            "proposed_initial_live_sizing": structural_scalp_promotion.get("proposed_initial_live_sizing"),
            "copyintent_parity": structural_scalp_promotion.get("copyintent_parity"),
            "single_guard_path": structural_scalp_promotion.get("single_guard_path"),
            "evidence_gate_pass": structural_scalp_promotion.get("evidence_gate_pass"),
            "decision": structural_scalp_promotion.get("decision"),
        },
        "volume_standby_promotion_prep": {
            "generated_at": volume_standby_promotion.get("generated_at"),
            "wallet": volume_standby_promotion.get("wallet"),
            "decision_clock": volume_standby_promotion.get("decision_clock"),
            "fee_aware_economics": volume_standby_promotion.get("fee_aware_economics"),
            "divergence_review": volume_standby_promotion.get("divergence_review"),
            "precondition_input_freshness": volume_standby_promotion.get("precondition_input_freshness"),
            "prederived_decision_branches": volume_standby_promotion.get("prederived_decision_branches"),
            "evidence_gate_pass": volume_standby_promotion.get("evidence_gate_pass"),
            "decision": volume_standby_promotion.get("decision"),
            "paper_only": volume_standby_promotion.get("paper_only"),
            "live_mutation_allowed": volume_standby_promotion.get("live_mutation_allowed"),
        },
        "eth5m_replication_scout": {
            "generated_at": eth5m_replication_scout.get("generated_at"),
            "status": eth5m_replication_scout.get("status"),
            "observations": eth5m_replication_scout.get("observations"),
            "distinct_windows": eth5m_replication_scout.get("distinct_windows"),
            "resolved_intents": eth5m_replication_scout.get("resolved_intents"),
            "resolved_windows": eth5m_replication_scout.get("resolved_windows"),
            "post_fee_pnl_usd": eth5m_replication_scout.get("post_fee_pnl_usd"),
            "promotion_gate_pass": eth5m_replication_scout.get("promotion_gate_pass"),
            "copyintent_parity": eth5m_replication_scout.get("copyintent_parity"),
            "live_orders_allowed": eth5m_replication_scout.get("live_orders_allowed"),
            "accrual_age_s": eth5m_replication_scout.get("accrual_age_s"),
            "tombstone": eth5m_replication_scout.get("tombstone")
            if isinstance(eth5m_replication_scout.get("tombstone"), dict)
            else {},
            "cadence": _eth5m_scout_cadence(
                eth5m_replication_scout,
                now=_parse_utc_ts(digest_generated_at) or datetime.now(timezone.utc),
            ),
        },
        "portfolio_allocator": {
            "generated_at": dispatch_throughput.get("generated_at"),
            "status": dispatch_throughput.get("status"),
            "signal_count": (dispatch_throughput.get("audit_scope") or {}).get("signal_count")
            if isinstance(dispatch_throughput.get("audit_scope"), dict)
            else None,
            "allocated_member_count": dispatch_allocator.get("allocated_member_count"),
            "starved_member_count": dispatch_allocator.get("starved_member_count"),
            "allocated_usd": dispatch_allocator.get("allocated_usd"),
            "estimated_drain_s": dispatch_model.get("estimated_drain_s"),
            "estimated_api_requests_per_s": dispatch_api_budget.get("estimated_api_requests_per_s"),
            "all_members_receive_floor": dispatch_fairness.get("all_members_receive_floor_when_cash_sufficient"),
            "single_submitter_invariant": dispatch_throughput.get("single_submitter_invariant"),
            "next_action": dispatch_throughput.get("next_action"),
        },
        "data_layer_v1": {
            "generated_at": data_layer.get("generated_at"),
            "status": data_layer.get("status"),
            "missing_dependencies": data_layer.get("missing_dependencies")
            if isinstance(data_layer.get("missing_dependencies"), list)
            else [],
            "rows_converted": data_layer.get("rows_converted"),
            "files_converted": data_layer.get("files_converted"),
            "files_considered": data_layer.get("files_considered"),
            "bytes_read": data_layer.get("bytes_read"),
            "duckdb_rows": (data_layer.get("duckdb") or {}).get("rows")
            if isinstance(data_layer.get("duckdb"), dict)
            else None,
            "duckdb_path": (data_layer.get("duckdb") or {}).get("duckdb_path")
            if isinstance(data_layer.get("duckdb"), dict)
            else None,
        },
        "dr_preflight": {
            "status": dr_preflight.get("status"),
            "generated_at": dr_preflight.get("generated_at"),
            "has_push_remote": dr_summary.get("has_push_remote"),
            "remote_count": dr_summary.get("remote_count"),
            "dirty_paths": dr_summary.get("dirty_paths"),
            "snapshot_branch": dr_summary.get("snapshot_branch"),
            "snapshot_total_bytes": dr_summary.get("snapshot_total_bytes"),
            "snapshot_file_count": dr_summary.get("snapshot_file_count"),
            "tracked_total_bytes": dr_summary.get("tracked_total_bytes"),
            "tracked_oversized_count": len(dr_summary.get("tracked_oversized_files") or [])
            if isinstance(dr_summary.get("tracked_oversized_files"), list)
            else None,
            "snapshot_oversized_count": len(dr_summary.get("snapshot_oversized_files") or [])
            if isinstance(dr_summary.get("snapshot_oversized_files"), list)
            else None,
            "snapshot_rejected_count": len(dr_summary.get("snapshot_rejected_candidates") or [])
            if isinstance(dr_summary.get("snapshot_rejected_candidates"), list)
            else None,
            "snapshot_push_commit": dr_snapshot_push.get("commit_sha"),
            "snapshot_push_status": dr_snapshot_push.get("status"),
            "snapshot_push_remote_ref": dr_snapshot_push.get("remote_ref"),
            "tracked_secret_paths": dr_summary.get("tracked_secret_paths")
            if isinstance(dr_summary.get("tracked_secret_paths"), list)
            else [],
            "uncommitted_secret_paths": dr_summary.get("uncommitted_secret_paths")
            if isinstance(dr_summary.get("uncommitted_secret_paths"), list)
            else [],
            "next_action": dr_preflight.get("next_action"),
        },
        "queue_clearance_gaps": clearance_summary,
        "ranked_queue_clearance_packets": {
            "generated_at": ranked_clearance_packets.get("generated_at"),
            "paper_only": ranked_clearance_packets.get("paper_only"),
            "live_orders_allowed": ranked_clearance_packets.get("live_orders_allowed"),
            "summary": ranked_clearance_summary,
            "parked_dormant": [
                {
                    "wallet": row.get("wallet"),
                    "status": row.get("status"),
                    "reason": row.get("reason"),
                    "recheck_at": row.get("recheck_at"),
                }
                for row in ranked_clearance_packets.get("parked_dormant") or []
                if isinstance(row, dict)
            ],
            "parked_reject_ratio": [
                {
                    "wallet": row.get("wallet"),
                    "status": row.get("status"),
                    "paper_orders": row.get("paper_orders"),
                    "attributable_reject_ratio": row.get("attributable_reject_ratio"),
                    "reject_ratio_park_threshold": row.get("reject_ratio_park_threshold"),
                }
                for row in ranked_clearance_packets.get("parked_reject_ratio") or []
                if isinstance(row, dict)
            ],
            "packets": [
                {
                    "wallet": row.get("wallet"),
                    "attributable_reject_ratio": (row.get("replay_fill_backed") or {}).get(
                        "attributable_reject_ratio"
                    ),
                    "reject_taxonomy": (row.get("replay_fill_backed") or {}).get(
                        "prospective_reject_taxonomy"
                    ),
                    "resolved_orders": (row.get("exact_policy_post_fee_shadow") or {}).get(
                        "resolved_orders"
                    ),
                    "post_fee_pnl_usd": (row.get("exact_policy_post_fee_shadow") or {}).get(
                        "post_fee_pnl_usd"
                    ),
                    "post_fee_gate_pass": (row.get("exact_policy_post_fee_shadow") or {}).get(
                        "gate_pass"
                    ),
                    "paper_disposition": (row.get("clearance") or {}).get("paper_disposition"),
                    "named_cause": (row.get("clearance") or {}).get("named_cause"),
                    "ready_for_live": (row.get("clearance") or {}).get("ready_for_live"),
                    "hot_standby_ready": (row.get("clearance") or {}).get("hot_standby_ready"),
                }
                for row in ranked_clearance_rows
            ],
        },
        "ready_shadow": {
            "summary": ready_shadow_summary,
            "standby_adjudications": [
                row
                for row in ready_shadow.get("standby_adjudications") or []
                if isinstance(row, dict)
            ],
            "paper_canary": {
                "wallet": ready_shadow_paper_canary.get("wallet"),
                "canary_path": ready_shadow_paper_canary.get("canary_path"),
                "paper_policy_id": ready_shadow_paper_canary.get("paper_policy_id"),
                "paper_canary_enrolled_at": ready_shadow_paper_canary.get("paper_canary_enrolled_at"),
                "paper_canary_elapsed_h": ready_shadow_paper_canary.get("paper_canary_elapsed_h"),
                "paper_canary_minimum_h": ready_shadow_paper_canary.get("paper_canary_minimum_h"),
                "ready_shadow_full_utc_day": ready_shadow_paper_canary.get("ready_shadow_full_utc_day"),
                "copyintent_parity_capture": ready_shadow_paper_canary.get("copyintent_parity_capture")
                if isinstance(ready_shadow_paper_canary.get("copyintent_parity_capture"), dict)
                else {},
                "source_liveness": ready_shadow_paper_canary.get("source_liveness")
                if isinstance(ready_shadow_paper_canary.get("source_liveness"), dict)
                else {},
                "live_canary_packet_preconditions": ready_shadow_paper_canary.get(
                    "live_canary_packet_preconditions"
                )
                if isinstance(ready_shadow_paper_canary.get("live_canary_packet_preconditions"), dict)
                else {},
                "readiness_verdict": ready_shadow_paper_canary.get("readiness_verdict"),
                "paper_only": ready_shadow_paper_canary.get("paper_only"),
                "live_orders_allowed": ready_shadow_paper_canary.get("live_orders_allowed"),
            },
            "next_lane": {
                "wallet": next_ready_shadow_lane.get("wallet"),
                "copyable_buy_events": next_ready_shadow_lane.get("copyable_buy_events"),
                "copyable_buy_gap": next_ready_shadow_lane.get("copyable_buy_gap"),
                "resolved_paper_fills": next_ready_shadow_lane.get("resolved_paper_fills"),
                "resolved_fill_gap": next_ready_shadow_lane.get("resolved_fill_gap"),
                "paper_pnl_usd": next_ready_shadow_lane.get("paper_pnl_usd"),
                "in_lane_post_fee_pnl_usd": next_ready_shadow_lane.get("in_lane_post_fee_pnl_usd"),
                "hot_standby_ready": next_ready_shadow_lane.get("hot_standby_ready"),
                "hot_standby_pending_liveness": next_ready_shadow_lane.get("hot_standby_pending_liveness"),
                "page_fable_due": next_ready_shadow_lane.get("page_fable_due"),
                "readiness_verdict": next_ready_shadow_lane.get("readiness_verdict"),
                "ready_for_live": next_ready_shadow_lane.get("ready_for_live"),
                "shadow_status": next_ready_shadow_lane.get("shadow_status"),
                "candidate_id": next_ready_shadow_lane.get("candidate_id"),
                "temporal_classification": next_ready_shadow_lane.get("temporal_classification"),
                "copyintent_parity_capture": next_ready_shadow_lane.get("copyintent_parity_capture")
                if isinstance(next_ready_shadow_lane.get("copyintent_parity_capture"), dict)
                else {},
                "live_canary_packet_preconditions": next_ready_shadow_lane.get(
                    "live_canary_packet_preconditions"
                )
                if isinstance(next_ready_shadow_lane.get("live_canary_packet_preconditions"), dict)
                else {},
            },
            "top_hot_standby_candidate": top_hot_standby,
        },
        "weekday_readmission_status": {
            "generated_at": weekday_readmission.get("generated_at"),
            "summary": weekday_readmission.get("summary")
            if isinstance(weekday_readmission.get("summary"), dict)
            else {},
            "wallets": weekday_readmission.get("wallets")
            if isinstance(weekday_readmission.get("wallets"), list)
            else [],
            "paper_only": weekday_readmission.get("paper_only"),
            "live_orders_allowed": weekday_readmission.get("live_orders_allowed"),
        },
        "inventory_convergence_skip_paper_lane": {
            "generated_at": inventory_convergence_skip_lane.get("generated_at"),
            "paper_only": inventory_convergence_skip_lane.get("paper_only"),
            "live_orders_allowed": inventory_convergence_skip_lane.get("live_orders_allowed"),
            "summary": inventory_convergence_summary,
        },
        "fee_aware_long_horizon_copy_paper_lane": {
            "status": fee_aware_long_horizon_lane.get("status"),
            "updated_at": fee_aware_long_horizon_lane.get("updated_at"),
            "collector_start_at": fee_aware_long_horizon_lane.get("collector_start_at"),
            "source_signals": fee_aware_long_horizon_lane.get("source_signals"),
            "copy_intents": fee_aware_long_horizon_lane.get("copy_intents"),
            "pending_count": fee_aware_long_horizon_lane.get("pending_count"),
            "observation_count": fee_aware_long_horizon_lane.get("observation_count"),
            "copy_intent_parity": fee_aware_long_horizon_lane.get("copy_intent_parity"),
            "lanes": fee_aware_long_horizon_lane.get("lanes"),
            "combined_post_fee_pnl_usd": fee_aware_long_horizon_lane.get("combined_post_fee_pnl_usd"),
            "paper_only": fee_aware_long_horizon_lane.get("paper_only"),
            "live_orders_allowed": fee_aware_long_horizon_lane.get("live_orders_allowed"),
            "live_order_attempts": fee_aware_long_horizon_lane.get("live_order_attempts"),
            "expired_count": fee_aware_long_horizon_lane.get("expired_count"),
            "excluded_non_qualifying_count": fee_aware_long_horizon_lane.get("excluded_non_qualifying_count"),
        },
        "alpha_decay_eligible_profiles_paper_lane": {
            "status": alpha_eligible_profiles_lane.get("status"),
            "updated_at": alpha_eligible_profiles_lane.get("updated_at"),
            "experiment_id": alpha_eligible_profiles_lane.get("experiment_id"),
            "collector_start_at": alpha_eligible_profiles_lane.get("collector_start_at"),
            "source_signals": alpha_eligible_profiles_lane.get("source_signals"),
            "copy_intents": alpha_eligible_profiles_lane.get("copyintents_created"),
            "pending_count": alpha_eligible_profiles_lane.get("pending_count"),
            "copy_intent_parity": alpha_eligible_profiles_lane.get("copy_intent_parity"),
            "summary": alpha_eligible_profiles_lane.get("summary"),
            "source": alpha_eligible_profiles_lane.get("source"),
            "paper_only": alpha_eligible_profiles_lane.get("paper_only"),
            "live_orders_allowed": alpha_eligible_profiles_lane.get("live_orders_allowed"),
            "orders_submitted": alpha_eligible_profiles_lane.get("orders_submitted"),
            "expired_observation_count": alpha_eligible_profiles_lane.get("expired_observation_count"),
        },
        "a6896d11_hot_standby_paper_lane": {
            "generated_at": a689_hot_standby_lane.get("generated_at"),
            "paper_only": a689_hot_standby_lane.get("paper_only"),
            "live_orders_allowed": a689_hot_standby_lane.get("live_orders_allowed"),
            "wallet": a689_hot_standby_lane.get("wallet"),
            "status": a689_hot_standby_lane.get("status"),
            "summary": a689_hot_standby_summary,
        },
        "c539_deferred_open_paper_probe": _c539_deferred_open_probe_summary(
            c539_deferred_open_probe
        ),
        "bac25_forward_only_lane": {
            key: bac25_forward_only_lane.get(key)
            for key in (
                "generated_at",
                "kind",
                "registered_at",
                "observation_deadline_at",
                "paper_only",
                "live_orders_allowed",
                "wallet",
                "wide_policy_fingerprint",
                "score_run_id",
                "decision_variable",
                "headline_sign_reliability",
                "top_1_market_contribution_usd",
                "retrospective_n",
                "retrospective_is_admission_input",
                "forward_n",
                "forward_post_fee_pnl_usd",
                "marginal_pnl_per_row",
                "marginal_basis",
                "headline_margin_rows",
                "headline_margin_rows_basis",
                "marginal_pnl_per_row_venue_executable",
                "headline_margin_rows_venue_executable",
                "headline_margin_rows_venue_executable_basis",
                "boundary_proximity",
                "forward_roi_pct",
                "forward_half_pnl_excluding_top_1_market",
                "forward_evidence_projection",
                "retrospective_and_forward_may_be_summed",
                "checks",
                "admission_eligible",
                "live_authority",
            )
        },
        "wallet_951b_forward_only_lane": {
            key: wallet_951b_forward_only_lane.get(key)
            for key in (
                "generated_at",
                "kind",
                "registered_at",
                "observation_deadline_at",
                "paper_only",
                "live_orders_allowed",
                "wallet",
                "wide_policy_fingerprint",
                "score_run_id",
                "decision_variable",
                "headline_sign_reliability",
                "top_1_market_contribution_usd",
                "retrospective_n",
                "retrospective_is_admission_input",
                "forward_n",
                "forward_post_fee_pnl_usd",
                "marginal_pnl_per_row",
                "marginal_basis",
                "headline_margin_rows",
                "headline_margin_rows_basis",
                "marginal_pnl_per_row_venue_executable",
                "headline_margin_rows_venue_executable",
                "headline_margin_rows_venue_executable_basis",
                "boundary_proximity",
                "forward_roi_pct",
                "forward_half_pnl_excluding_top_1_market",
                "forward_evidence_projection",
                "retrospective_and_forward_may_be_summed",
                "checks",
                "admission_eligible",
                "live_authority",
            )
        },
        "bac25_forward_writer_scope": bac25_forward_writer_scope,
        "wide_forward_sibling_lanes": {
            "writer_state": wide_forward_sibling_lanes_state,
            "lanes": [
                {
                    key: lane.get(key)
                    for key in (
                        "generated_at",
                        "kind",
                        "registered_at",
                        "observation_deadline_at",
                        "observation_window_s",
                        "paper_only",
                        "live_orders_allowed",
                        "wallet",
                        "wide_policy_fingerprint",
                        "score_run_id",
                        "forward_n",
                        "forward_post_fee_pnl_usd",
                        "forward_roi_pct",
                        "forward_half_pnl_excluding_top_1_market",
                        "forward_venue_reachable_share_pct",
                        "retrospective_and_forward_may_be_summed",
                        "checks",
                        "admission_eligible",
                        "live_authority",
                    )
                }
                for lane in (
                    wallet_82c8_8bb70201_forward_only_lane,
                    wallet_82c8_fdd8af33_forward_only_lane,
                )
            ],
        },
        "policy_family_terminal_registry": {
            "generated_at": policy_family_terminal_registry.get("generated_at"),
            "paper_only": policy_family_terminal_registry.get("paper_only"),
            "live_orders_allowed": policy_family_terminal_registry.get(
                "live_orders_allowed"
            ),
            "entries": [
                {
                    key: row.get(key)
                    for key in (
                        "policy_family",
                        "wide_policy_fingerprint",
                        "score_run_id",
                        "status",
                        "terminal",
                        "stop_writer",
                        "refuse_alias_reregistration",
                        "effective_at",
                        "reason_source",
                        "headline_margin_rows",
                        "boundary_proximity",
                        "marginal_basis",
                        "headline_margin_rows_basis",
                        "marginal_pnl_per_row_venue_executable",
                        "headline_margin_rows_venue_executable",
                        "headline_margin_rows_venue_executable_basis",
                        "precommitted_negative_outcomes",
                        "precommitted_nonnegative_outcomes",
                    )
                }
                for row in policy_family_terminal_registry.get("entries") or []
                if isinstance(row, dict)
            ],
        },
        "a689_82c8_ready_shadow_cut": {
            "generated_at": a689_82c8_cut.get("generated_at"),
            "execution_status": a689_82c8_cut.get("execution_status"),
            "decision": a689_82c8_cut.get("decision"),
            "cut_at": a689_82c8_cut.get("cut_at"),
            "state_rebind": ready_shadow_cut,
            "a689_terminal_absent_from_lanes": a689_terminal_absent,
            "successor_wallet": successor_wallet,
            "successor_bound": successor_bound,
            "successor_source_binding": successor_shadow_lane.get("source_binding"),
            "successor_clock_started_at": successor_shadow_lane.get(
                "standby_evidence_started_at"
            ),
            "successor_liveness": {
                "recommended_query_key": successor_address_selection.get(
                    "recommended_query_key"
                ),
                "user_only_hot_path_supported": successor_address_selection.get(
                    "user_only_hot_path_supported"
                ),
                "last_trade_age_h": successor_address_selection.get("last_trade_age_h"),
                "last_trade_iso": successor_address_selection.get("last_trade_iso"),
            },
            "paper_only": successor_shadow_lane.get("paper_only"),
            "live_orders_allowed": successor_shadow_lane.get("live_orders_allowed"),
            "atomic_verification_pass": bool(
                a689_82c8_cut.get("execution_status") == "EXECUTED"
                and ready_shadow_cut.get("status") == "EXECUTED_ATOMIC_STATE_REBIND"
                and a689_terminal_absent
                and successor_bound
                and successor_address_selection.get("recommended_query_key") == "user"
            ),
        },
        "terminal_82c8_decision": {
            "generated_at": terminal_82c8_decision.get("generated_at"),
            "decision": terminal_82c8_decision.get("decision")
            or terminal_82c8_decision.get("status"),
            "status": terminal_82c8_decision.get("status"),
            "terminal_decision": terminal_82c8_decision.get("terminal_decision"),
            "terminal_decision_monotone": terminal_82c8_decision.get(
                "terminal_decision_monotone"
            ),
            "clock": terminal_82c8_decision.get("clock")
            if isinstance(terminal_82c8_decision.get("clock"), dict)
            else {
                "start": terminal_82c8_decision.get("clock_start"),
                "decision_at": terminal_82c8_decision.get("decision_at"),
                "required_h": terminal_82c8_decision.get("required_hours"),
                "due": terminal_82c8_decision.get("due"),
            },
            "evidence": terminal_82c8_decision.get("evidence")
            if isinstance(terminal_82c8_decision.get("evidence"), dict)
            else {},
            "failed_gates": terminal_82c8_decision.get("failed_gates")
            if isinstance(terminal_82c8_decision.get("failed_gates"), list)
            else [
                key
                for key, passed in (
                    terminal_82c8_decision.get("checks")
                    if isinstance(terminal_82c8_decision.get("checks"), dict)
                    else {}
                ).items()
                if passed is False
            ],
            "paper_only": terminal_82c8_decision.get("paper_only"),
            "live_orders_allowed": terminal_82c8_decision.get("live_orders_allowed"),
        },
        "a689_live_postfix": _a689_live_postfix_summary(participation, live),
        "a689_0200_tripwire": {
            "generated_at": a689_0200_tripwire.get("generated_at"),
            "verdict": a689_0200_tripwire.get("verdict"),
            "pre_ruled_action": a689_0200_tripwire.get("pre_ruled_action"),
            "rows": a689_0200_tripwire.get("rows"),
            "windows": a689_0200_tripwire.get("windows"),
            "accepted_order_rows": a689_0200_tripwire.get("accepted_order_rows"),
            "tripwire_class_counts": a689_0200_tripwire.get("tripwire_class_counts")
            if isinstance(a689_0200_tripwire.get("tripwire_class_counts"), dict)
            else {},
            "dominant_skip_reason_counts": a689_0200_tripwire.get("dominant_skip_reason_counts")
            if isinstance(a689_0200_tripwire.get("dominant_skip_reason_counts"), dict)
            else {},
            "floor_budget_bind": a689_0200_tripwire.get("floor_budget_bind")
            if isinstance(a689_0200_tripwire.get("floor_budget_bind"), dict)
            else {},
        },
        "pipeline_late_decomposition": {
            "generated_at": pipeline_late_decomposition.get("generated_at"),
            "verdict": pipeline_late_decomposition.get("verdict"),
            "pre_ruled_action": pipeline_late_decomposition.get("pre_ruled_action"),
            "pipeline_late_rows": pipeline_late_decomposition.get("pipeline_late_rows"),
            "pipeline_late_windows": pipeline_late_decomposition.get("pipeline_late_windows"),
            "first_evaluation_late_gte_180s": pipeline_late_decomposition.get("first_evaluation_late_gte_180s")
            if isinstance(pipeline_late_decomposition.get("first_evaluation_late_gte_180s"), dict)
            else {},
            "first_touch_timely_lt_180s": pipeline_late_decomposition.get("first_touch_timely_lt_180s")
            if isinstance(pipeline_late_decomposition.get("first_touch_timely_lt_180s"), dict)
            else {},
            "timely_first_eval_with_later_late_skip": pipeline_late_decomposition.get(
                "timely_first_eval_with_later_late_skip"
            )
            if isinstance(pipeline_late_decomposition.get("timely_first_eval_with_later_late_skip"), dict)
            else {},
            "cycle_duration_s": pipeline_late_cycle_duration,
        },
        "budget_bind_margin_packet": {
            "generated_at": budget_bind_margin_packet.get("generated_at"),
            "verdict": budget_bind_margin_packet.get("verdict"),
            "pre_ruled_action": budget_bind_margin_packet.get("pre_ruled_action"),
            "summary": budget_bind_margin_packet.get("summary")
            if isinstance(budget_bind_margin_packet.get("summary"), dict)
            else {},
            "window_budget_usd": budget_bind_margin_packet.get("window_budget_usd")
            if isinstance(budget_bind_margin_packet.get("window_budget_usd"), dict)
            else {},
            "effective_min_usd": budget_bind_margin_packet.get("effective_min_usd")
            if isinstance(budget_bind_margin_packet.get("effective_min_usd"), dict)
            else {},
            "budget_to_min_ratio": budget_bind_margin_packet.get("budget_to_min_ratio")
            if isinstance(budget_bind_margin_packet.get("budget_to_min_ratio"), dict)
            else {},
        },
        "f418_readmission_packet": {
            "generated_at": f418_readmission_packet.get("generated_at"),
            "status": f418_readmission_packet.get("status"),
            "decision": f418_readmission_packet.get("decision"),
            "source_wallet": f418_readmission_packet.get("source_wallet"),
            "candidate_id": f418_readmission_packet.get("candidate_id"),
            "counterfactual": f418_readmission_packet.get("counterfactual_gate_since_non_admissible_boundary")
            if isinstance(f418_readmission_packet.get("counterfactual_gate_since_non_admissible_boundary"), dict)
            else {},
            "sizing": f418_readmission_packet.get("sizing_if_fable_allows_activation")
            if isinstance(f418_readmission_packet.get("sizing_if_fable_allows_activation"), dict)
            else {},
            "next_action": f418_readmission_packet.get("next_action"),
        },
        "ruling10_abstain_probe": {
            "generated_at": ruling10_abstain_probe.get("generated_at"),
            "day": ruling10_abstain_probe.get("day"),
            "summary": ruling10_abstain_probe.get("summary")
            if isinstance(ruling10_abstain_probe.get("summary"), dict)
            else {},
            "members": [
                {
                    "label": row.get("label"),
                    "due_for_ruling10": row.get("due_for_ruling10"),
                    "day_live_orders": row.get("day_live_orders"),
                    "day_filled_orders": row.get("day_filled_orders"),
                    "top_abstain_reasons": row.get("top_abstain_reasons")
                    if isinstance(row.get("top_abstain_reasons"), list)
                    else [],
                    "intent_id_report": row.get("intent_id_report"),
                    "probe_generated_at": (row.get("guard_stamp") or {}).get("probe_generated_at")
                    if isinstance(row.get("guard_stamp"), dict)
                    else None,
                    "guard_generated_at": (row.get("guard_stamp") or {}).get("live_guard_generated_at")
                    if isinstance(row.get("guard_stamp"), dict)
                    else None,
                }
                for row in (
                    ruling10_abstain_probe.get("members")
                    if isinstance(ruling10_abstain_probe.get("members"), list)
                    else []
                )
                if isinstance(row, dict)
            ],
        },
        "d60c_latency_attribution": {
            "generated_at": d60c_latency_attribution.get("generated_at"),
            "summary": d60c_latency_attribution.get("summary")
            if isinstance(d60c_latency_attribution.get("summary"), dict)
            else {},
            "target": d60c_latency_attribution.get("target")
            if isinstance(d60c_latency_attribution.get("target"), dict)
            else {},
            "scan": d60c_latency_attribution.get("scan")
            if isinstance(d60c_latency_attribution.get("scan"), dict)
            else {},
            "events": [
                {
                    "event_id": row.get("event_id"),
                    "market_slug": row.get("market_slug"),
                    "received_ts_iso": row.get("received_ts_iso"),
                    "window_close_ts_iso": row.get("window_close_ts_iso"),
                    "decided_ts_iso": row.get("decided_ts_iso"),
                    "latency_class": row.get("latency_class"),
                    "decision_minus_close_s": row.get("decision_minus_close_s"),
                    "decision_minus_received_s": row.get("decision_minus_received_s"),
                }
                for row in (
                    d60c_latency_attribution.get("events")
                    if isinstance(d60c_latency_attribution.get("events"), list)
                    else []
                )[:8]
                if isinstance(row, dict)
            ],
        },
        "live_order_reject_attribution": {
            "generated_at": live_order_reject_attribution.get("generated_at"),
            "summary": live_order_reject_attribution.get("summary")
            if isinstance(live_order_reject_attribution.get("summary"), dict)
            else {},
            "fak_no_match_summary": (
                (live_order_reject_attribution.get("fak_no_match_analysis") or {}).get("summary")
                if isinstance(live_order_reject_attribution.get("fak_no_match_analysis"), dict)
                and isinstance((live_order_reject_attribution.get("fak_no_match_analysis") or {}).get("summary"), dict)
                else {}
            ),
            "negative_fill_pnl_summary": (
                (live_order_reject_attribution.get("negative_fill_pnl_analysis") or {}).get("summary")
                if isinstance(live_order_reject_attribution.get("negative_fill_pnl_analysis"), dict)
                and isinstance((live_order_reject_attribution.get("negative_fill_pnl_analysis") or {}).get("summary"), dict)
                else {}
            ),
            "examples": [
                {
                    "submitted_at": row.get("submitted_at"),
                    "market_slug": row.get("market_slug"),
                    "intent_id": row.get("intent_id"),
                    "execution_role": row.get("execution_role"),
                    "reason": row.get("reason"),
                    "class": row.get("class"),
                    "forgone_usd": row.get("forgone_usd"),
                    "scheduler_subset": row.get("scheduler_subset"),
                }
                for row in (
                    live_order_reject_attribution.get("examples")
                    if isinstance(live_order_reject_attribution.get("examples"), list)
                    else []
                )[:8]
                if isinstance(row, dict)
            ],
        },
        "a689_edge_transfer": a689_edge_summary,
        "experiment_preregistration": {
            "status": experiment_preregistration.get("status"),
            "generated_at": experiment_preregistration.get("generated_at"),
            "registry_records": experiment_preregistration.get("registry_records"),
            "valid_records": experiment_preregistration.get("valid_records"),
            "active_count": experiment_preregistration.get("active_count"),
            "latest_experiment_id": experiment_preregistration.get("latest_experiment_id"),
            "latest_success_criterion": experiment_preregistration.get("latest_success_criterion"),
            "latest_deadline_utc": experiment_preregistration.get("latest_deadline_utc"),
            "missing_required_ids": prereg_missing,
        },
        "fee_edge_decomposition": {
            "generated_at": fee_edge_decomposition.get("generated_at"),
            "experiment_id": fee_edge_decomposition.get("experiment_id"),
            "verdict": fee_edge_decomposition.get("verdict"),
            "winner_count": fee_edge_decomposition.get("winner_count"),
            "winners": fee_edge_decomposition.get("winners", [])[:8]
            if isinstance(fee_edge_decomposition.get("winners"), list)
            else [],
            "measurement_only": fee_edge_decomposition.get("measurement_only"),
            "live_mutation": fee_edge_decomposition.get("live_mutation"),
        },
        "entry_price_band_gate": {
            "enabled": entry_price_band_gate.get("enabled"),
            "direction_id": entry_price_band_gate.get("direction_id"),
            "experiment_id": entry_price_band_gate.get("experiment_id"),
            "blocked_bands": entry_price_band_gate.get("blocked_bands", []),
            "caps_changed": (entry_price_band_gate.get("invariants") or {}).get("caps_changed")
            if isinstance(entry_price_band_gate.get("invariants"), dict)
            else None,
            "counterfactual": {
                "status": entry_price_band_gate_counterfactual.get("status"),
                "resolved_gated_flow_windows": entry_price_band_gate_counterfactual.get(
                    "resolved_gated_flow_windows"
                ),
                "gated_minus_ungated_post_fee_pnl_usd": entry_price_band_gate_counterfactual.get(
                    "gated_minus_ungated_post_fee_pnl_usd"
                ),
                "copyintent_parity_violations": entry_price_band_gate_counterfactual.get(
                    "copyintent_parity_violations"
                ),
            },
        },
        "profit_latency_suppression_counterfactual": {
            "generated_at": profit_latency_counterfactual.get("generated_at"),
            "status": profit_latency_counterfactual.get("status"),
            "resolved_suppressed_windows": profit_latency_counterfactual.get(
                "resolved_suppressed_windows"
            ),
            "decision_band_60_180": profit_latency_counterfactual.get("decision_band_60_180"),
            "buckets": profit_latency_counterfactual.get("buckets"),
            "live_mutation": profit_latency_counterfactual.get("live_mutation"),
        },
        "f418_post_band_gate_residual_loss_causal": {
            "generated_at": f418_post_band_causal.get("generated_at"),
            "activation_utc": f418_post_band_causal.get("activation_utc"),
            "status": f418_post_band_causal.get("status"),
            "gate": f418_post_band_causal.get("gate"),
            "cohorts": f418_post_band_causal.get("cohorts"),
            "train": f418_post_band_causal.get("train"),
            "holdout": f418_post_band_causal.get("holdout"),
            "worst_cells": (f418_post_band_causal.get("cells") or [])[:5],
            "live_mutation": f418_post_band_causal.get("live_mutation"),
        },
        "f418_acceptance_funnel": {
            "generated_at": f418_acceptance_funnel.get("generated_at"),
            "status": f418_acceptance_funnel.get("status"),
            "seat_tenure": f418_acceptance_funnel.get("seat_tenure"),
            "rolling_30m": f418_acceptance_funnel.get("rolling_30m"),
            "canonical_live_ledger": f418_acceptance_funnel.get("canonical_live_ledger"),
            "unattributed_selected_intents": f418_acceptance_funnel.get(
                "unattributed_selected_intents"
            ),
            "live_mutation": f418_acceptance_funnel.get("live_mutation"),
        },
        "f418_green_day_conversion_shadow": {
            "generated_at": f418_green_day_conversion.get("generated_at"),
            "verdict": f418_green_day_conversion.get("verdict"),
            "sample_gate_pass": f418_green_day_conversion.get("sample_gate_pass"),
            "dual_bar_bottleneck": f418_green_day_conversion.get("dual_bar_bottleneck"),
            "control_pre_sign": f418_green_day_conversion.get("control_pre_sign"),
            "green_sign_post": f418_green_day_conversion.get("green_sign_post"),
            "conversion_delta_pct_points": f418_green_day_conversion.get(
                "conversion_delta_pct_points"
            ),
            "live_mutation": f418_green_day_conversion.get("live_mutation"),
        },
        "f418_size_clamp_fee_leak_shadow": {
            "generated_at": f418_size_clamp_fee_leak.get("generated_at"),
            "status": f418_size_clamp_fee_leak.get("status"),
            "micro_1usd": f418_size_clamp_fee_leak.get("micro_1usd"),
            "standing_2_to_2_5usd": f418_size_clamp_fee_leak.get(
                "standing_2_to_2_5usd"
            ),
            "comparison": f418_size_clamp_fee_leak.get("comparison"),
            "decision": f418_size_clamp_fee_leak.get("decision"),
            "live_mutation": f418_size_clamp_fee_leak.get("live_mutation"),
        },
        "f418_spread_elasticity_shadow": {
            "generated_at": f418_spread_elasticity.get("generated_at"),
            "status": f418_spread_elasticity.get("status"),
            "experiment_id": f418_spread_elasticity.get("experiment_id"),
            "coverage": f418_spread_elasticity.get("coverage"),
            "gate": f418_spread_elasticity.get("gate"),
            "baseline": f418_spread_elasticity.get("baseline"),
            "cells": f418_spread_elasticity.get("cells"),
            "live_mutation": f418_spread_elasticity.get("live_mutation"),
        },
        "f418_window_time_book_crossed": {
            "generated_at": f418_window_time_book_crossed.get("generated_at"),
            "coverage": f418_window_time_book_crossed.get("coverage"),
            "temporal_coverage_proof": f418_window_time_book_crossed.get(
                "temporal_coverage_proof"
            ),
            "sample_ready_cells": f418_window_time_book_crossed.get(
                "sample_ready_cells"
            ),
            "surviving_cells": f418_window_time_book_crossed.get(
                "surviving_cells"
            ),
            "verdict": f418_window_time_book_crossed.get("verdict"),
            "matrix": f418_window_time_book_crossed.get("matrix"),
            "paper_only": f418_window_time_book_crossed.get("paper_only"),
            "live_mutation": f418_window_time_book_crossed.get("live_mutation"),
        },
        "fak_depth_persistence_timing_shadow": {
            "generated_at": fak_depth_persistence.get("generated_at"),
            "status": fak_depth_persistence.get("status"),
            "experiment_id": fak_depth_persistence.get("experiment_id"),
            "coverage": fak_depth_persistence.get("coverage"),
            "metrics": fak_depth_persistence.get("metrics"),
            "gate": fak_depth_persistence.get("gate"),
            "parity": fak_depth_persistence.get("parity"),
            "live_mutation": fak_depth_persistence.get("live_mutation"),
        },
        "member_native_policy_acceptance_uplift_shadow": {
            "generated_at": member_native_policy_uplift.get("generated_at"),
            "verdict": member_native_policy_uplift.get("verdict"),
            "sample_started_at": member_native_policy_uplift.get("sample_started_at"),
            "member_count": member_native_policy_uplift.get("member_count"),
            "frozen_policy_binding_count": member_native_policy_uplift.get(
                "frozen_policy_binding_count"
            ),
            "incremental": member_native_policy_uplift.get("incremental"),
            "incumbent_twin": member_native_policy_uplift.get("incumbent_twin"),
            "gate": member_native_policy_uplift.get("gate"),
            "cohort_id": member_native_policy_uplift.get("cohort_id"),
            "sample_valid": member_native_policy_uplift.get("sample_valid"),
            "exclusions": member_native_policy_uplift.get("exclusions"),
            "persistent_runner": {
                "kind": "launchd",
                "label": "com.belavarga.polymarket.member-native-policy-uplift-paper",
                "pid": member_native_runner_pid,
                "running": member_native_runner_pid is not None,
                "raw_output_age_s": member_native_output_age_s,
                "freshness_slo_s": 180,
                "fresh": member_native_output_age_s is not None
                and member_native_output_age_s <= 180,
                "deadman_status": (
                    "OK"
                    if member_native_runner_pid is not None
                    and member_native_output_age_s is not None
                    and member_native_output_age_s <= 180
                    else "OPEN_DEFECT_RESTART_OR_REFRESH_RUNNER"
                ),
            },
            "single_submitter_preserved": member_native_policy_uplift.get(
                "single_submitter_preserved"
            ),
            "live_mutation": member_native_policy_uplift.get("live_mutation"),
        },
        "top10_direct_clob_paper_lane": {
            "updated_at": top10_direct_clob_paper.get("updated_at"),
            "status": top10_direct_clob_paper.get("status"),
            "paper_only": top10_direct_clob_paper.get("paper_only"),
            "live_orders_allowed": top10_direct_clob_paper.get("live_orders_allowed"),
            "service_cycle_count": top10_direct_clob_paper.get("service_cycle_count"),
            "summary": top10_direct_clob_paper.get("summary"),
            "source": top10_direct_clob_paper.get("source"),
            "persistent_runner": {
                "kind": "launchd",
                "label": "com.belavarga.polymarket.top10-direct-clob-paper",
                "pid": top10_direct_runner_pid,
                "running": top10_direct_runner_pid is not None,
                "raw_output_age_s": top10_direct_output_age_s,
                "freshness_slo_s": 60,
                "fresh": top10_direct_output_age_s is not None
                and top10_direct_output_age_s <= 60,
                "deadman_status": (
                    "OK"
                    if top10_direct_runner_pid is not None
                    and top10_direct_output_age_s is not None
                    and top10_direct_output_age_s <= 60
                    else "OPEN_DEFECT_RESTART_OR_REFRESH_RUNNER"
                ),
            },
        },
        "selected_member_guard_submit_attribution": {
            "generated_at": selected_member_attribution.get("generated_at"),
            "measurement_started_at": selected_member_attribution.get("measurement_started_at"),
            "status": selected_member_attribution.get("status"),
            "defect_classification": selected_member_attribution.get("defect_classification"),
            "selected_wallet_count": selected_member_attribution.get("selected_wallet_count"),
            "selected_policy_eligible_unique_intents": selected_member_attribution.get(
                "selected_policy_eligible_unique_intents"
            ),
            "submitted_intents": selected_member_attribution.get("submitted_intents"),
            "telemetry_defects": selected_member_attribution.get("telemetry_defects"),
            "wiring_defects": selected_member_attribution.get("wiring_defects"),
            "terminal_stage_counts": selected_member_attribution.get("terminal_stage_counts"),
            "live_mutation": selected_member_attribution.get("live_mutation"),
        },
        "market_buy_precision_counterfactual": {
            "generated_at": market_buy_precision_counterfactual.get("generated_at"),
            "status": market_buy_precision_counterfactual.get("status"),
            "resolved_suppressed_windows": market_buy_precision_counterfactual.get(
                "resolved_suppressed_windows"
            ),
            "min_resolved_suppressed_windows": market_buy_precision_counterfactual.get(
                "min_resolved_suppressed_windows"
            ),
            "post_fee_counterfactual_pnl_usd": market_buy_precision_counterfactual.get(
                "post_fee_counterfactual_pnl_usd"
            ),
            "precision_decomposition": market_buy_precision_counterfactual.get(
                "precision_decomposition"
            ),
            "live_mutation": market_buy_precision_counterfactual.get("live_mutation"),
        },
        "weekend_copy_shadows": {
            "window_sign_skew": weekend_window_sign_skew.get("summary")
            if isinstance(weekend_window_sign_skew.get("summary"), dict)
            else {},
            "hour_of_day_skew": weekend_hour_skew.get("summary")
            if isinstance(weekend_hour_skew.get("summary"), dict)
            else {},
            "fak_nomatch_requote": fak_nomatch_requote.get("summary")
            if isinstance(fak_nomatch_requote.get("summary"), dict)
            else {},
            "paper_only": all(
                payload.get("paper_only") is True
                for payload in (weekend_window_sign_skew, weekend_hour_skew, fak_nomatch_requote)
                if payload
            ),
            "live_path_mutated": any(
                payload.get("live_path_mutated") is True
                for payload in (weekend_window_sign_skew, weekend_hour_skew, fak_nomatch_requote)
            ),
        },
        "defects": defects,
        "latest_direction_next_verbatim": direction_next,
        "latest_direction_material": latest_direction_material,
        "latest_direction_material_extraction_basis": latest_direction_material_summary["extraction_basis"],
        "recent_directions": recent_direction_blocks,
        "directions_newer_than_latest_status": newer_direction_blocks,
        "direction_warnings": direction_warnings,
    }
    factory_queue = digest["member_factory_kpi"]["queue_depth"]
    factory_trajectory = digest["member_factory_kpi"]["set_trajectory"]
    factory_freshness = digest["member_factory_kpi"]["member_freshness"]
    factory_hours = digest["member_factory_kpi"]["hour_coverage"]
    factory_throughput = digest["member_factory_kpi"]["factory_throughput"]
    factory_series = digest["member_factory_kpi"]["series_census"]
    overflow_proposal = digest["enabled_overflow_proposal"]
    watcher = digest["watcher_gap"]
    active_set_poller_digest = digest["active_set_dataapi_poller"]
    active_set_rtds_premerge_digest = digest["active_set_rtds_premerge"]
    event_scheduler_digest = (
        digest["live"].get("event_triggered_cycle_scheduler")
        if isinstance(digest["live"].get("event_triggered_cycle_scheduler"), dict)
        else {}
    )
    guard_json_cache_digest = (
        digest["live"].get("guard_json_cache_evidence")
        if isinstance(digest["live"].get("guard_json_cache_evidence"), dict)
        else {}
    )
    live_probe_digest = digest.get("live_execution_probes") if isinstance(digest.get("live_execution_probes"), list) else []
    adjusted_volume = digest["volume"].get("adjusted_participation") if isinstance(digest["volume"].get("adjusted_participation"), dict) else {}
    source_coverage = digest["volume"].get("source_coverage") if isinstance(digest["volume"].get("source_coverage"), dict) else {}
    window_pnl_histogram = (
        digest.get("per_window_pnl_histogram")
        if isinstance(digest.get("per_window_pnl_histogram"), dict)
        else {}
    )
    e6db_autopsy = digest.get("e6db_loser_autopsy") if isinstance(digest.get("e6db_loser_autopsy"), dict) else {}
    e6db_autopsy_summary = (
        e6db_autopsy.get("summary") if isinstance(e6db_autopsy.get("summary"), dict) else {}
    )
    successor_dossier_digest = (
        digest.get("successor_dossier")
        if isinstance(digest.get("successor_dossier"), dict)
        else {}
    )
    successor_dossier_summary = (
        successor_dossier_digest.get("summary")
        if isinstance(successor_dossier_digest.get("summary"), dict)
        else {}
    )
    active_set_rotation_packet_digest = (
        digest.get("active_set_rotation_packet")
        if isinstance(digest.get("active_set_rotation_packet"), dict)
        else {}
    )
    active_set_rotation_summary = (
        active_set_rotation_packet_digest.get("summary")
        if isinstance(active_set_rotation_packet_digest.get("summary"), dict)
        else {}
    )
    active_set_post_rotation_digest = (
        digest.get("active_set_post_rotation_windows")
        if isinstance(digest.get("active_set_post_rotation_windows"), dict)
        else {}
    )
    active_set_post_rotation_summary = (
        active_set_post_rotation_digest.get("summary")
        if isinstance(active_set_post_rotation_digest.get("summary"), dict)
        else {}
    )
    active_set_pin_consumer_sweep_digest = (
        digest.get("active_set_pin_consumer_sweep")
        if isinstance(digest.get("active_set_pin_consumer_sweep"), dict)
        else {}
    )
    defense_tripwire_digest = (
        digest.get("defense_tripwires")
        if isinstance(digest.get("defense_tripwires"), dict)
        else {}
    )
    defense_regret_digest = (
        digest.get("defense_regret")
        if isinstance(digest.get("defense_regret"), dict)
        else {}
    )
    e1_audit_digest = (
        digest.get("e1_framework_audit_inputs")
        if isinstance(digest.get("e1_framework_audit_inputs"), dict)
        else {}
    )
    reject_cluster_digest = (
        e1_audit_digest.get("reject_cluster")
        if isinstance(e1_audit_digest.get("reject_cluster"), dict)
        else {}
    )
    full_day_reject_cluster_digest = (
        e1_audit_digest.get("full_utc_day_reject_cluster")
        if isinstance(e1_audit_digest.get("full_utc_day_reject_cluster"), dict)
        else {}
    )
    e1_defense_regret_digest = (
        e1_audit_digest.get("defense_regret")
        if isinstance(e1_audit_digest.get("defense_regret"), dict)
        else {}
    )
    e1_gate_conversion_digest = (
        e1_audit_digest.get("daily_gate_conversion")
        if isinstance(e1_audit_digest.get("daily_gate_conversion"), dict)
        else {}
    )
    e1_gate_ev_digest = (
        e1_audit_digest.get("highest_rejection_gate_ev")
        if isinstance(e1_audit_digest.get("highest_rejection_gate_ev"), dict)
        else {}
    )
    e1_roi_digest = (
        e1_audit_digest.get("multi_day_roi_distribution")
        if isinstance(e1_audit_digest.get("multi_day_roi_distribution"), dict)
        else {}
    )
    e1_roi_distribution_digest = (
        e1_roi_digest.get("distribution")
        if isinstance(e1_roi_digest.get("distribution"), dict)
        else {}
    )
    if e1_audit_digest.get("freshness_status") == "STALE_ARTEFACT_LEDGER_LAG":
        e1_inputs_text = (
            "e1_inputs=STALE_ARTEFACT_LEDGER_LAG,"
            f"artifact_ledger_cut:{e1_audit_digest.get('artifact_ledger_newest_submitted_at')},"
            f"live_ledger_cut:{e1_audit_digest.get('live_ledger_newest_submitted_at')},"
            f"ledger_lag_s:{e1_audit_digest.get('ledger_lag_s')},"
            "live_ledger_cut_frozen_by:"
            f"{e1_audit_digest.get('live_ledger_cut_frozen_by')} "
        )
    else:
        e1_inputs_text = (
            "e1_inputs="
            f"rejects:{reject_cluster_digest.get('reject_rows')},"
            f"taxonomy:{reject_cluster_digest.get('taxonomy_counts')},"
            "ruled_ceiling_refused:"
            f"{reject_cluster_digest.get('ruled_ceiling_refused_by_tighter_cap_rejects')},"
            f"full_day_rejects:{full_day_reject_cluster_digest.get('reject_rows')},"
            f"full_day_taxonomy:{full_day_reject_cluster_digest.get('taxonomy_counts')},"
            f"actual:{e1_defense_regret_digest.get('actual_probe_capped_pnl_usd')},"
            f"sign_flip:{e1_defense_regret_digest.get('defense_flipped_sign')},"
            f"gate_in:{e1_gate_conversion_digest.get('policy_eligible_signals_in')},"
            f"gate_gap:{e1_gate_conversion_digest.get('accounting_gap')},"
            f"highest_gate:{e1_gate_ev_digest.get('gate')},"
            f"gate_ev_roi:{e1_gate_ev_digest.get('paper_counterfactual_roi_pct')},"
            f"days:{e1_roi_digest.get('days_with_cost')},"
            f"multi_roi:{e1_roi_distribution_digest.get('aggregate_roi_pct')},"
            f"worst_roi:{e1_roi_distribution_digest.get('worst_daily_roi_pct')} "
        )
    map_digest = (
        digest.get("participation_288_map")
        if isinstance(digest.get("participation_288_map"), dict)
        else {}
    )
    map_summary_digest = (
        map_digest.get("summary") if isinstance(map_digest.get("summary"), dict) else {}
    )
    peer_idle_digest = (
        digest.get("peer_active_idle_windows")
        if isinstance(digest.get("peer_active_idle_windows"), dict)
        else {}
    )
    pipeline_digest = (
        digest.get("pipeline_slo_and_standby_readiness")
        if isinstance(digest.get("pipeline_slo_and_standby_readiness"), dict)
        else {}
    )
    pipeline_stages = (
        (pipeline_digest.get("pipeline_slo") or {}).get("stages")
        if isinstance(pipeline_digest.get("pipeline_slo"), dict)
        else []
    )
    standby_digest = (
        pipeline_digest.get("standby_ready")
        if isinstance(pipeline_digest.get("standby_ready"), dict)
        else {}
    )
    weekend_day_probe_digest = (
        digest.get("weekend_day_probe")
        if isinstance(digest.get("weekend_day_probe"), dict)
        else {}
    )
    weekend_seat_loss_rider_digest = (
        weekend_day_probe_digest.get("seat_loss_rotation_rider")
        if isinstance(weekend_day_probe_digest.get("seat_loss_rotation_rider"), dict)
        else {}
    )
    live_weekend_rider_digest = (
        digest["live"].get("active_set_weekend_seat_loss_rotation")
        if isinstance(digest["live"].get("active_set_weekend_seat_loss_rotation"), dict)
        else {}
    )
    live_weekend_cap_check = (
        live_weekend_rider_digest.get("probe_caps_guard_flag_check")
        if isinstance(live_weekend_rider_digest.get("probe_caps_guard_flag_check"), dict)
        else {}
    )
    live_guard_caps = digest["live"].get("guard_caps") if isinstance(digest["live"].get("guard_caps"), dict) else {}
    live_guard_max_order_usd = live_weekend_cap_check.get("guard_max_order_usd")
    if live_guard_max_order_usd is None:
        live_guard_max_order_usd = live_guard_caps.get("max_order_usd")
    live_guard_drip_max_tranche_usd = live_weekend_cap_check.get("guard_drip_max_tranche_usd")
    if live_guard_drip_max_tranche_usd is None:
        live_guard_drip_max_tranche_usd = live_guard_caps.get("drip_max_tranche_usd")
    coverage_gap_digest = digest["coverage_gap_diagnosis"]
    coverage_gap_summary = (
        coverage_gap_digest.get("summary") if isinstance(coverage_gap_digest.get("summary"), dict) else {}
    )
    signal_supply_digest = digest["coverage_gap_signal_supply"]
    signal_supply_summary = (
        signal_supply_digest.get("summary") if isinstance(signal_supply_digest.get("summary"), dict) else {}
    )
    routing_digest = digest["routing_disambiguation"]
    routing_summary = routing_digest.get("summary") if isinstance(routing_digest.get("summary"), dict) else {}
    campaign_lat_digest = digest.get("campaign_lat_p1")
    campaign_lat_summary = (
        campaign_lat_digest.get("summary")
        if isinstance(campaign_lat_digest, dict)
        and isinstance(campaign_lat_digest.get("summary"), dict)
        else {}
    )
    routing_shadow_digest = digest["routing_shadow_validation"]
    routing_shadow_summary = (
        routing_shadow_digest.get("summary") if isinstance(routing_shadow_digest.get("summary"), dict) else {}
    )
    selection_visibility_digest = digest.get("selection_visibility_packet")
    selection_visibility_summary = (
        selection_visibility_digest.get("summary")
        if isinstance(selection_visibility_digest, dict)
        and isinstance(selection_visibility_digest.get("summary"), dict)
        else {}
    )
    routing_shadow_pin_digest = digest.get("routing_shadow_attribution_pin")
    routing_shadow_pin_summary = (
        routing_shadow_pin_digest.get("summary")
        if isinstance(routing_shadow_pin_digest, dict)
        and isinstance(routing_shadow_pin_digest.get("summary"), dict)
        else {}
    )
    # A stale attribution-pin packet must not mask live accrual: prefer the
    # pin summary only while it is at least as fresh as the main validation
    # packet (ISO-8601 UTC timestamps compare lexicographically).
    _pin_generated_at = (
        routing_shadow_pin_digest.get("generated_at")
        if isinstance(routing_shadow_pin_digest, dict)
        and isinstance(routing_shadow_pin_digest.get("generated_at"), str)
        else ""
    )
    _main_generated_at = (
        routing_shadow_digest.get("generated_at")
        if isinstance(routing_shadow_digest.get("generated_at"), str)
        else ""
    )
    if routing_shadow_pin_summary and _pin_generated_at >= _main_generated_at:
        routing_shadow_render_summary = dict(routing_shadow_pin_summary)
        if (
            routing_shadow_render_summary.get("runtime_selected_wallet_source") is None
            and routing_shadow_summary.get("runtime_selected_wallet_source") is not None
        ):
            routing_shadow_render_summary["runtime_selected_wallet_source"] = routing_shadow_summary.get(
                "runtime_selected_wallet_source"
            )
    else:
        routing_shadow_render_summary = routing_shadow_summary or routing_shadow_pin_summary
    routing_shadow_pin_extra = (
        routing_shadow_pin_summary.get("extra_would_submit_post_fee_measurement")
        if isinstance(routing_shadow_pin_summary.get("extra_would_submit_post_fee_measurement"), dict)
        else {}
    )
    routing_shadow_pin_stability = (
        routing_shadow_pin_summary.get("attribution_stability")
        if isinstance(routing_shadow_pin_summary.get("attribution_stability"), dict)
        else {}
    )
    routing_shadow_attrition = (
        routing_shadow_render_summary.get("filter_attrition_totals_latest_cycle")
        if isinstance(routing_shadow_render_summary.get("filter_attrition_totals_latest_cycle"), dict)
        else {}
    )
    routing_shadow_fee_cal = (
        routing_shadow_render_summary.get("fee_gate_calibration_retained")
        if isinstance(routing_shadow_render_summary.get("fee_gate_calibration_retained"), dict)
        else {}
    )
    own_impact_digest = digest["own_impact_monitor"]
    self_feed_digest = digest["self_feed_vs_ledger"]
    cash_ledger_digest = digest["cash_ledger_classification"]
    self_feed_trace_digest = digest["self_feed_missing_trace"]
    self_feed_full_retrace_digest = digest["self_feed_full_ledger_retrace"]
    self_feed_duckdb_digest = digest["self_feed_duckdb_benchmark"]
    h2_external_digest = digest["h2_external_redemptions"]
    h2_account_value_digest = digest["h2_account_value_residual"]
    residual_cash_diff_digest = digest["residual_cash_diff_audit"]
    same_cut_basis_digest = digest["scorecard_same_cut_basis_check"]
    pinned_tranche_digest = digest["pinned_tranche_economics"]
    pinned_midday_digest = digest["pinned_tranche_midday_due_check"]
    guard_fill_audit_digest = digest["guard_fill_recording_audit"]
    fill_toxicity_digest = digest["fill_toxicity"]
    fill_loss_digest = digest["fill_conditioned_loss_attribution"]
    window_time_reject_digest = digest["window_time_reject_attribution"]
    inventory_skip_digest = digest["inventory_skip_lifecycle"]
    toxicity_denylist_digest = digest["toxicity_denylist"]
    strategy_map_digest = digest["strategy_map"]
    decompiler_digest = digest["strategy_decompiler_intake"]
    followability_digest = digest["followability_leaderboard"]
    full_universe_digest = digest["full_universe_copyability"]
    wide_candidate_digest = digest["wide_candidate_measurement"]
    hot_history_accumulator_digest = digest["wallet_copy_hot_history_accumulator"]
    market_scan_digest = digest["wallet_market_scan"]
    temporal_digest = digest["temporal_profitability"]
    source_active_replay_digest = digest["source_active_policy_replay"]
    source_active_cohort_digest = digest["source_active_cohort"]
    a3e0_midnight_bundle_digest = digest["a3e0_midnight_bundle"]
    focused_candidate_p1_digest = digest["focused_candidate_p1"]
    winner_variation_digest = digest["winner_variation_siblings"]
    temporal_probe_apply_digest = digest["temporal_watch_tier_probe_apply"]
    watch_tier_shadow_digest = digest["watch_tier_shadow_ev"]
    stakeout_digest = digest["weekend_specialist_stakeout"]
    sub25_spot_digest = digest["sub25_accounting_spot_check"]
    btc5m_fleet_digest = digest["btc5m_live_paper_fleet"]
    btc5m_fleet_top50_digest = (
        btc5m_fleet_digest.get("top50_matrix_coverage")
        if isinstance(btc5m_fleet_digest.get("top50_matrix_coverage"), dict)
        else {}
    )
    two_sided_digest = digest["btc5m_two_sided_prime"]
    morning_table_digest = digest["btc5m_morning_ranked_table"]
    structural_scalp_lane_digest = digest["btc5m_structural_scalp_paper_lane"]
    structural_scalp_promotion_digest = digest["btc5m_structural_scalp_promotion_prep"]
    volume_standby_promotion_digest = digest["volume_standby_promotion_prep"]
    eth5m_replication_digest = digest["eth5m_replication_scout"]
    portfolio_allocator_digest = digest["portfolio_allocator"]
    data_layer_digest = digest["data_layer_v1"]
    dr_digest = digest["dr_preflight"]
    clearance = digest["queue_clearance_gaps"]
    ranked_clearance = digest.get("ranked_queue_clearance_packets", {})
    ranked_clearance = ranked_clearance if isinstance(ranked_clearance, dict) else {}
    ready_shadow_summary_digest = digest["ready_shadow"]["summary"]
    ready_shadow_paper_canary_digest = digest["ready_shadow"].get("paper_canary", {})
    ready_shadow_adjudications_digest = digest["ready_shadow"].get("standby_adjudications", [])
    ready_shadow_digest = digest["ready_shadow"]["next_lane"]
    ready_shadow_top_hot = digest["ready_shadow"]["top_hot_standby_candidate"]
    weekday_readmission_digest = (
        digest.get("weekday_readmission_status")
        if isinstance(digest.get("weekday_readmission_status"), dict)
        else {}
    )
    weekday_readmission_summary = (
        weekday_readmission_digest.get("summary")
        if isinstance(weekday_readmission_digest.get("summary"), dict)
        else {}
    )
    weekday_readmission_wallets = (
        weekday_readmission_digest.get("wallets")
        if isinstance(weekday_readmission_digest.get("wallets"), list)
        else []
    )
    inventory_convergence_digest = digest["inventory_convergence_skip_paper_lane"]["summary"]
    fee_aware_long_horizon_digest = digest["fee_aware_long_horizon_copy_paper_lane"]
    alpha_eligible_profiles_digest = digest["alpha_decay_eligible_profiles_paper_lane"]
    a689_hot_standby_digest = digest["a6896d11_hot_standby_paper_lane"]["summary"]
    c539_deferred_open_digest = digest["c539_deferred_open_paper_probe"]
    bac25_forward_lane_digest = digest["bac25_forward_only_lane"]
    wallet_951b_forward_lane_digest = digest["wallet_951b_forward_only_lane"]
    a689_cut_digest = digest.get("a689_82c8_ready_shadow_cut", {})
    a689_postfix_digest = (
        digest.get("a689_live_postfix") if isinstance(digest.get("a689_live_postfix"), dict) else {}
    )
    a689_tripwire_digest = (
        digest.get("a689_0200_tripwire") if isinstance(digest.get("a689_0200_tripwire"), dict) else {}
    )
    pipeline_late_decomp_digest = (
        digest.get("pipeline_late_decomposition")
        if isinstance(digest.get("pipeline_late_decomposition"), dict)
        else {}
    )
    budget_bind_digest = (
        digest.get("budget_bind_margin_packet")
        if isinstance(digest.get("budget_bind_margin_packet"), dict)
        else {}
    )
    budget_bind_summary = (
        budget_bind_digest.get("summary")
        if isinstance(budget_bind_digest.get("summary"), dict)
        else {}
    )
    f418_readmission_digest = (
        digest.get("f418_readmission_packet")
        if isinstance(digest.get("f418_readmission_packet"), dict)
        else {}
    )
    f418_counterfactual_digest = (
        f418_readmission_digest.get("counterfactual")
        if isinstance(f418_readmission_digest.get("counterfactual"), dict)
        else {}
    )
    f418_routing_shadow_counterfactual = (
        f418_counterfactual_digest.get("routing_shadow_member")
        if isinstance(f418_counterfactual_digest.get("routing_shadow_member"), dict)
        else {}
    )
    ruling10_digest = (
        digest.get("ruling10_abstain_probe")
        if isinstance(digest.get("ruling10_abstain_probe"), dict)
        else {}
    )
    ruling10_summary = (
        ruling10_digest.get("summary")
        if isinstance(ruling10_digest.get("summary"), dict)
        else {}
    )
    ruling10_members = (
        ruling10_digest.get("members")
        if isinstance(ruling10_digest.get("members"), list)
        else []
    )
    d60c_latency_digest = (
        digest.get("d60c_latency_attribution")
        if isinstance(digest.get("d60c_latency_attribution"), dict)
        else {}
    )
    d60c_latency_summary = (
        d60c_latency_digest.get("summary")
        if isinstance(d60c_latency_digest.get("summary"), dict)
        else {}
    )
    live_reject_digest = (
        digest.get("live_order_reject_attribution")
        if isinstance(digest.get("live_order_reject_attribution"), dict)
        else {}
    )
    live_reject_summary = (
        live_reject_digest.get("summary")
        if isinstance(live_reject_digest.get("summary"), dict)
        else {}
    )
    live_reject_fak_summary = (
        live_reject_digest.get("fak_no_match_summary")
        if isinstance(live_reject_digest.get("fak_no_match_summary"), dict)
        else {}
    )
    live_negative_summary = (
        live_reject_digest.get("negative_fill_pnl_summary")
        if isinstance(live_reject_digest.get("negative_fill_pnl_summary"), dict)
        else {}
    )
    live_negative_maker_vs_direct = (
        live_negative_summary.get("maker_recovery_vs_direct_taker")
        if isinstance(live_negative_summary.get("maker_recovery_vs_direct_taker"), dict)
        else {}
    )
    live_negative_band_tranche = (
        live_negative_summary.get("aggregate_by_price_band_tranche_type")
        if isinstance(live_negative_summary.get("aggregate_by_price_band_tranche_type"), dict)
        else {}
    )
    live_negative_band_gate_passes = [
        key
        for key, value in live_negative_band_tranche.items()
        if isinstance(value, dict)
        and isinstance(value.get("fix_candidate_gate"), dict)
        and bool(value["fix_candidate_gate"].get("passes"))
    ]
    live_negative_band_roi = [
        (key, value.get("resolved_fills"), value.get("roi_pct"))
        for key, value in sorted(live_negative_band_tranche.items())
        if isinstance(value, dict)
    ][:4]
    pipeline_late_cycle_digest = (
        pipeline_late_decomp_digest.get("cycle_duration_s")
        if isinstance(pipeline_late_decomp_digest.get("cycle_duration_s"), dict)
        else {}
    )
    a689_edge_digest = digest.get("a689_edge_transfer") if isinstance(digest.get("a689_edge_transfer"), dict) else {}
    prereg_digest = digest["experiment_preregistration"]
    fee_edge_digest = digest.get("fee_edge_decomposition", {})
    fee_edge_digest = fee_edge_digest if isinstance(fee_edge_digest, dict) else {}
    entry_band_digest = digest.get("entry_price_band_gate", {})
    entry_band_digest = entry_band_digest if isinstance(entry_band_digest, dict) else {}
    profit_latency_digest = digest.get("profit_latency_suppression_counterfactual", {})
    profit_latency_digest = profit_latency_digest if isinstance(profit_latency_digest, dict) else {}
    f418_post_band_digest = digest.get("f418_post_band_gate_residual_loss_causal", {})
    f418_post_band_digest = (
        f418_post_band_digest if isinstance(f418_post_band_digest, dict) else {}
    )
    f418_acceptance_digest = digest.get("f418_acceptance_funnel", {})
    f418_acceptance_digest = f418_acceptance_digest if isinstance(f418_acceptance_digest, dict) else {}
    f418_conversion_digest = digest.get("f418_green_day_conversion_shadow", {})
    f418_conversion_digest = (
        f418_conversion_digest if isinstance(f418_conversion_digest, dict) else {}
    )
    f418_fee_leak_digest = digest.get("f418_size_clamp_fee_leak_shadow", {})
    f418_fee_leak_digest = (
        f418_fee_leak_digest if isinstance(f418_fee_leak_digest, dict) else {}
    )
    f418_spread_digest = digest.get("f418_spread_elasticity_shadow", {})
    fak_depth_digest = digest.get("fak_depth_persistence_timing_shadow", {})
    f418_spread_digest = (
        f418_spread_digest if isinstance(f418_spread_digest, dict) else {}
    )
    member_native_uplift_digest = digest.get(
        "member_native_policy_acceptance_uplift_shadow", {}
    )
    member_native_uplift_digest = (
        member_native_uplift_digest
        if isinstance(member_native_uplift_digest, dict)
        else {}
    )
    f418_fee_leak_micro = (
        f418_fee_leak_digest.get("micro_1usd")
        if isinstance(f418_fee_leak_digest.get("micro_1usd"), dict)
        else {}
    )
    f418_fee_leak_standing = (
        f418_fee_leak_digest.get("standing_2_to_2_5usd")
        if isinstance(f418_fee_leak_digest.get("standing_2_to_2_5usd"), dict)
        else {}
    )
    f418_fee_leak_comparison = (
        f418_fee_leak_digest.get("comparison")
        if isinstance(f418_fee_leak_digest.get("comparison"), dict)
        else {}
    )
    selected_attribution_digest = digest.get("selected_member_guard_submit_attribution", {})
    selected_attribution_digest = (
        selected_attribution_digest if isinstance(selected_attribution_digest, dict) else {}
    )
    precision_cf_digest = digest.get("market_buy_precision_counterfactual", {})
    precision_cf_digest = precision_cf_digest if isinstance(precision_cf_digest, dict) else {}
    weekend_shadows_digest = digest["weekend_copy_shadows"]
    weekend_window_digest = weekend_shadows_digest.get("window_sign_skew", {})
    weekend_hour_digest = weekend_shadows_digest.get("hour_of_day_skew", {})
    fak_requote_digest = weekend_shadows_digest.get("fak_nomatch_requote", {})
    cli_versions_digest = digest["cli_versions"]
    deadman_digest = digest["order_flow_deadman"]
    flow_incident_digest = digest.get("live_flow_incident_foreground", {})
    flow_incident_digest = (
        flow_incident_digest if isinstance(flow_incident_digest, dict) else {}
    )
    flow_episode_digest = digest.get("flow_episodes", {})
    flow_episode_digest = flow_episode_digest if isinstance(flow_episode_digest, dict) else {}
    flow_episode_scope = (
        flow_episode_digest.get("ledger_scope")
        if isinstance(flow_episode_digest.get("ledger_scope"), dict)
        else {}
    )
    cross_exchange_digest = digest.get("cross_exchange_probability_edge", {})
    cross_exchange_digest = (
        cross_exchange_digest if isinstance(cross_exchange_digest, dict) else {}
    )
    cross_exchange_actuator_digest = (
        cross_exchange_digest.get("live_actuator")
        if isinstance(cross_exchange_digest.get("live_actuator"), dict)
        else {}
    )
    cross_exchange_delayed_park_digest = (
        cross_exchange_digest.get("delayed_offset_park")
        if isinstance(cross_exchange_digest.get("delayed_offset_park"), dict)
        else {}
    )
    cross_exchange_multivenue_digest = (
        cross_exchange_digest.get("multivenue_residual_matrix")
        if isinstance(cross_exchange_digest.get("multivenue_residual_matrix"), dict)
        else {}
    )
    cross_exchange_complete_set_digest = (
        cross_exchange_digest.get("complete_set_paired_maker")
        if isinstance(cross_exchange_digest.get("complete_set_paired_maker"), dict)
        else {}
    )
    cross_exchange_split_sell_digest = (
        cross_exchange_digest.get("complete_set_split_sell")
        if isinstance(cross_exchange_digest.get("complete_set_split_sell"), dict)
        else {}
    )
    cross_exchange_book_shock_digest = (
        cross_exchange_digest.get("book_shock_reversion")
        if isinstance(cross_exchange_digest.get("book_shock_reversion"), dict)
        else {}
    )
    cross_exchange_queue_hazard_digest = (
        cross_exchange_digest.get("queue_hazard_maker")
        if isinstance(cross_exchange_digest.get("queue_hazard_maker"), dict)
        else {}
    )
    cross_exchange_native_sweep_digest = (
        cross_exchange_digest.get("native_aggressor_sweep")
        if isinstance(cross_exchange_digest.get("native_aggressor_sweep"), dict)
        else {}
    )
    cross_exchange_native_complement_digest = (
        cross_exchange_digest.get("native_complement_lead_lag")
        if isinstance(
            cross_exchange_digest.get("native_complement_lead_lag"), dict
        )
        else {}
    )
    cross_exchange_cross_asset_digest = (
        cross_exchange_digest.get("polymarket_cross_asset_leader_lag")
        if isinstance(
            cross_exchange_digest.get("polymarket_cross_asset_leader_lag"), dict
        )
        else {}
    )
    cross_exchange_first_leader_digest = (
        cross_exchange_digest.get("polymarket_first_leader_cross_asset_lag")
        if isinstance(
            cross_exchange_digest.get("polymarket_first_leader_cross_asset_lag"),
            dict,
        )
        else {}
    )
    cross_exchange_signed_tape_digest = (
        cross_exchange_digest.get("native_signed_tape_imbalance_stale_ask")
        if isinstance(
            cross_exchange_digest.get("native_signed_tape_imbalance_stale_ask"),
            dict,
        )
        else {}
    )
    cross_exchange_l2_displacement_digest = (
        cross_exchange_digest.get("native_l2_microprice_displacement_stale_ask")
        if isinstance(
            cross_exchange_digest.get("native_l2_microprice_displacement_stale_ask"),
            dict,
        )
        else {}
    )
    cross_exchange_tob_pressure_digest = (
        cross_exchange_digest.get("native_l2_tob_pressure_imbalance")
        if isinstance(
            cross_exchange_digest.get("native_l2_tob_pressure_imbalance"), dict
        )
        else {}
    )
    cross_exchange_cross_parity_digest = (
        cross_exchange_digest.get("native_l2_cross_outcome_parity_stale_ask")
        if isinstance(
            cross_exchange_digest.get("native_l2_cross_outcome_parity_stale_ask"),
            dict,
        )
        else {}
    )
    cross_exchange_depth_weighted_cross_parity_digest = (
        cross_exchange_digest.get(
            "native_l2_depth_weighted_microprice_parity_stale_ask"
        )
        if isinstance(
            cross_exchange_digest.get(
                "native_l2_depth_weighted_microprice_parity_stale_ask"
            ),
            dict,
        )
        else {}
    )
    cross_exchange_bid_support_cross_parity_digest = (
        cross_exchange_digest.get(
            "native_l2_complement_bid_support_parity_stale_ask"
        )
        if isinstance(
            cross_exchange_digest.get(
                "native_l2_complement_bid_support_parity_stale_ask"
            ),
            dict,
        )
        else {}
    )
    cross_exchange_ask_cap_cross_parity_digest = (
        cross_exchange_digest.get(
            "native_l2_complement_ask_cap_parity_stale_ask"
        )
        if isinstance(
            cross_exchange_digest.get(
                "native_l2_complement_ask_cap_parity_stale_ask"
            ),
            dict,
        )
        else {}
    )
    cross_exchange_f1_f4_digest = (
        cross_exchange_digest.get("current_f1_f4_fallout")
        if isinstance(cross_exchange_digest.get("current_f1_f4_fallout"), dict)
        else {}
    )
    active_member_orderfilled_digest = digest.get(
        "active_member_orderfilled_hot_source_shadow", {}
    )
    active_member_orderfilled_digest = (
        active_member_orderfilled_digest
        if isinstance(active_member_orderfilled_digest, dict)
        else {}
    )
    early_01a_book_digest = digest.get("early_01a_decision_time_book", {})
    early_01a_book_digest = (
        early_01a_book_digest if isinstance(early_01a_book_digest, dict) else {}
    )
    qualified_pool_stakeout_digest = digest.get(
        "qualified_pool_orderfilled_stakeout", {}
    )
    qualified_pool_stakeout_digest = (
        qualified_pool_stakeout_digest
        if isinstance(qualified_pool_stakeout_digest, dict)
        else {}
    )
    source_identity_digest = digest.get("copy_source_identity_reconciliation", {})
    source_identity_digest = (
        source_identity_digest if isinstance(source_identity_digest, dict) else {}
    )
    source_identity_frozen = source_identity_digest.get("frozen_cohort", {})
    source_identity_frozen = (
        source_identity_frozen if isinstance(source_identity_frozen, dict) else {}
    )
    source_identity_current = source_identity_digest.get("current_cohort", {})
    source_identity_current = (
        source_identity_current if isinstance(source_identity_current, dict) else {}
    )
    source_identity_gates = source_identity_digest.get("gates", {})
    source_identity_gates = (
        source_identity_gates if isinstance(source_identity_gates, dict) else {}
    )
    wake_activation_digest = digest.get("copy_source_wake_activation", {})
    wake_activation_digest = (
        wake_activation_digest if isinstance(wake_activation_digest, dict) else {}
    )
    wake_resident_guard = wake_activation_digest.get("resident_guard", {})
    wake_resident_guard = (
        wake_resident_guard if isinstance(wake_resident_guard, dict) else {}
    )
    wake_forced_sweep = wake_activation_digest.get("forced_sweep", {})
    wake_forced_sweep = (
        wake_forced_sweep if isinstance(wake_forced_sweep, dict) else {}
    )
    orderfilled_fast_lane_digest = digest.get("orderfilled_fast_lane", {})
    orderfilled_fast_lane_digest = (
        orderfilled_fast_lane_digest
        if isinstance(orderfilled_fast_lane_digest, dict)
        else {}
    )
    orderfilled_fast_lane_source_digest = orderfilled_fast_lane_digest.get(
        "source_report", {}
    )
    orderfilled_fast_lane_source_digest = (
        orderfilled_fast_lane_source_digest
        if isinstance(orderfilled_fast_lane_source_digest, dict)
        else {}
    )
    orderfilled_fast_lane_bridge_digest = orderfilled_fast_lane_digest.get(
        "bridge_report", {}
    )
    orderfilled_fast_lane_bridge_digest = (
        orderfilled_fast_lane_bridge_digest
        if isinstance(orderfilled_fast_lane_bridge_digest, dict)
        else {}
    )
    realized_fee_digest = digest.get("realized_fee_receipts", {})
    realized_fee_digest = realized_fee_digest if isinstance(realized_fee_digest, dict) else {}
    realized_fee_summary = realized_fee_digest.get("summary", {})
    realized_fee_summary = realized_fee_summary if isinstance(realized_fee_summary, dict) else {}
    policy_choke_digest = deadman_digest.get("policy_choke", {})
    policy_choke_digest = policy_choke_digest if isinstance(policy_choke_digest, dict) else {}
    rung_b_lifecycle_digest = deadman_digest.get("policy_choke_rung_b_lifecycle", {})
    rung_b_lifecycle_digest = rung_b_lifecycle_digest if isinstance(rung_b_lifecycle_digest, dict) else {}
    choke_drill_digest = deadman_digest.get("policy_choke_fire_drill", {})
    choke_drill_digest = choke_drill_digest if isinstance(choke_drill_digest, dict) else {}
    regime_seat_digest = digest.get("regime_seat_selection", {})
    regime_seat_digest = regime_seat_digest if isinstance(regime_seat_digest, dict) else {}
    coacceptance_digest = digest.get("coacceptance_eligibility_shadow", {})
    coacceptance_digest = coacceptance_digest if isinstance(coacceptance_digest, dict) else {}
    probe_fill_quality_digest = digest.get("probe_fill_quality_shadows", {})
    probe_fill_quality_digest = probe_fill_quality_digest if isinstance(probe_fill_quality_digest, dict) else {}
    deadman_guard_memory = (
        deadman_digest.get("guard_memory")
        if isinstance(deadman_digest.get("guard_memory"), dict)
        else {}
    )
    deadman_r1_digest = digest.get("order_flow_deadman_r1_attribution", {})
    deadman_r1_digest = deadman_r1_digest if isinstance(deadman_r1_digest, dict) else {}
    trade_lane_digest = digest.get("trade_executor_lane_attribution", {})
    trade_lane_digest = trade_lane_digest if isinstance(trade_lane_digest, dict) else {}
    own_positions_digest = digest["own_positions"]
    own_redeemer_digest = digest["own_redeemer"]
    wallet_outflow_digest = digest["wallet_outflow_deadman"]
    research_disk_digest = digest.get("research_disk_deadman", {})
    research_disk_digest = research_disk_digest if isinstance(research_disk_digest, dict) else {}
    repo_storage_hygiene_digest = digest.get("repo_storage_hygiene", {})
    repo_storage_hygiene_digest = (
        repo_storage_hygiene_digest if isinstance(repo_storage_hygiene_digest, dict) else {}
    )
    repo_storage_after = repo_storage_hygiene_digest.get("after", {})
    repo_storage_after = repo_storage_after if isinstance(repo_storage_after, dict) else {}
    guard_event_log_rotation_digest = digest.get("guard_event_log_rotation", {})
    guard_event_log_rotation_digest = (
        guard_event_log_rotation_digest if isinstance(guard_event_log_rotation_digest, dict) else {}
    )
    post_panic_digest = digest.get("post_panic_integrity", {})
    post_panic_digest = post_panic_digest if isinstance(post_panic_digest, dict) else {}
    scheduler_ratchet_digest = digest.get("scheduler_ratchet", {})
    scheduler_ratchet_digest = scheduler_ratchet_digest if isinstance(scheduler_ratchet_digest, dict) else {}
    scheduler_verdict_digest = digest.get("scheduler_verdict", {})
    scheduler_verdict_digest = scheduler_verdict_digest if isinstance(scheduler_verdict_digest, dict) else {}
    scheduler_verdict_summary = (
        scheduler_verdict_digest.get("summary")
        if isinstance(scheduler_verdict_digest.get("summary"), dict)
        else {}
    )
    scheduler_stratification_digest = digest.get("scheduler_stratification", {})
    scheduler_stratification_digest = (
        scheduler_stratification_digest if isinstance(scheduler_stratification_digest, dict) else {}
    )
    scheduler_stratification_summary = (
        scheduler_stratification_digest.get("summary")
        if isinstance(scheduler_stratification_digest.get("summary"), dict)
        else {}
    )
    scheduler_retirement_digest = (
        scheduler_stratification_digest.get("retirement")
        if isinstance(scheduler_stratification_digest.get("retirement"), dict)
        else {}
    )
    alpha_decay_curve_digest = digest["alpha_decay_curve"]
    alpha_overlap_capture_digest = digest["alpha_overlap_capture"]
    research_lane_cadence_digest = digest.get("research_lane_cadence", {})
    research_lane_cadence_digest = (
        research_lane_cadence_digest if isinstance(research_lane_cadence_digest, dict) else {}
    )
    same_window_capture_digest = (
        digest.get("same_window_research_capture")
        if isinstance(digest.get("same_window_research_capture"), dict)
        else {}
    )
    targeted_copyability_digest = digest.get("targeted_copyability_probe", {})
    targeted_copyability_digest = (
        targeted_copyability_digest if isinstance(targeted_copyability_digest, dict) else {}
    )
    alpha_decay_five_s = (
        alpha_decay_curve_digest.get("five_s")
        if isinstance(alpha_decay_curve_digest.get("five_s"), dict)
        else {}
    )
    member_age_rows = deadman_digest.get("member_signal_age") if isinstance(deadman_digest.get("member_signal_age"), dict) else {}
    member_age_summary = [
        (
            _short_wallet(wallet),
            row.get("signal_age_p50_s"),
            row.get("signal_age_p90_s"),
            row.get("eligible_intents"),
            row.get("suppressed_intents"),
        )
        for wallet, row in sorted(member_age_rows.items())
        if isinstance(row, dict)
    ][:5]
    guard_latency_trigger = (
        digest["live"].get("guard_latency_trigger")
        if isinstance(digest["live"].get("guard_latency_trigger"), dict)
        else {}
    )
    guard_latency_recent = [
        (
            row.get("generated_at"),
            row.get("total_s"),
            row.get("active_set_rtds_premerge_s"),
        )
        for row in guard_latency_trigger.get("recent", [])
        if isinstance(row, dict)
    ][-4:]
    runtime_speed_digest = digest.get("runtime_speed_baseline", {})
    runtime_speed_metrics = (
        runtime_speed_digest.get("metrics")
        if isinstance(runtime_speed_digest.get("metrics"), dict)
        else {}
    )
    runtime_speed_metric_statuses = (
        runtime_speed_digest.get("metric_statuses")
        if isinstance(runtime_speed_digest.get("metric_statuses"), dict)
        else {}
    )
    runtime_speed_metric_annotations = (
        runtime_speed_digest.get("metric_annotations")
        if isinstance(runtime_speed_digest.get("metric_annotations"), dict)
        else {}
    )
    scorecard_runtime_digest = (
        digest.get("scorecard_runtime_evidence")
        if isinstance(digest.get("scorecard_runtime_evidence"), dict)
        else {}
    )
    runtime_speed_regressions = [
        row.get("metric")
        for row in runtime_speed_digest.get("regressions", [])
        if isinstance(row, dict)
    ]
    runtime_speed_persistence = (
        runtime_speed_digest.get("persistence_counts")
        if isinstance(runtime_speed_digest.get("persistence_counts"), dict)
        else {}
    )
    runtime_speed_persistence_rows = (
        runtime_speed_persistence.get("rows")
        if isinstance(runtime_speed_persistence.get("rows"), dict)
        else {}
    )
    runtime_speed_persistence_nonzero = [
        f"{name}:{row.get('consecutive_over_threshold')}"
        for name, row in runtime_speed_persistence_rows.items()
        if isinstance(row, dict) and _as_int(row.get("consecutive_over_threshold")) > 0
    ][:6]

    def _runtime_speed_metric_label(name: str) -> str:
        value = runtime_speed_metrics.get(name)
        status = runtime_speed_metric_statuses.get(name)
        annotation = runtime_speed_metric_annotations.get(name)
        annotation = annotation if isinstance(annotation, dict) else {}
        if status:
            label = f"{value}/{status}"
        else:
            label = str(value)
        extras: list[str] = []
        if annotation.get("sample_age_s") is not None:
            extras.append(f"age={annotation.get('sample_age_s')}")
        if annotation.get("post_stale_lock_reclaim_run_ordinal") is not None:
            extras.append(f"post_reclaim_run={annotation.get('post_stale_lock_reclaim_run_ordinal')}")
        if annotation.get("timed_sample_count") is not None:
            extras.append(f"timed_samples={annotation.get('timed_sample_count')}")
        slowest_step = annotation.get("slowest_step")
        if isinstance(slowest_step, dict) and slowest_step.get("name"):
            extras.append(f"slowest={slowest_step.get('name')}:{slowest_step.get('duration_s')}")
        if annotation.get("return_code") is not None:
            extras.append(f"rc={annotation.get('return_code')}")
        if extras:
            label = f"{label}({','.join(extras)})"
        return label

    runtime_total_loss = digest["active_set"]["runtime"].get("total_loss_auto_disable", {})
    runtime_total_loss_disabled = (
        runtime_total_loss.get("disabled_members") if isinstance(runtime_total_loss, dict) else []
    )
    runtime_total_loss_disabled_labels = [
        (
            f"{row.get('candidate_id')}:{_short_wallet(str(row.get('wallet') or ''))}:"
            f"n{row.get('resolved_fills')}:loss{row.get('total_loss_fills')}:"
            f"pnl{row.get('pnl_usd')}"
        )
        for row in (runtime_total_loss_disabled or [])[:3]
        if isinstance(row, dict)
    ]
    cli_versions_tool_labels = [
        (
            f"{tool}:{row.get('status')}/{row.get('installed')}->"
            f"{row.get('latest')}@{row.get('source')}"
        )
        for tool, row in cli_versions_digest.get("tools", {}).items()
        if isinstance(row, dict)
    ]
    agy_smoke_digest = digest.get("agy_fallback_smoke") if isinstance(digest.get("agy_fallback_smoke"), dict) else {}
    agy_smoke_attempts = agy_smoke_digest.get("post_pin_substitute_attempts")
    agy_smoke_attempts = agy_smoke_attempts if isinstance(agy_smoke_attempts, list) else []
    agy_smoke_line = (
        "agy_smoke: "
        f"result={agy_smoke_digest.get('result')} "
        f"tested_at={agy_smoke_digest.get('tested_at')} "
        f"fallback={agy_smoke_digest.get('fallback_provider')} "
        f"initial_rc={((agy_smoke_digest.get('initial_agy_answer_proof') or {}) if isinstance(agy_smoke_digest.get('initial_agy_answer_proof'), dict) else {}).get('provider_log_basename')} "
        f"post_pin={[attempt.get('rc') for attempt in agy_smoke_attempts if isinstance(attempt, dict)]} "
        f"next={agy_smoke_digest.get('next_action')}"
    )
    agy_quota_digest = digest.get("agy_quota") if isinstance(digest.get("agy_quota"), dict) else {}
    agy_quota_line = (
        "agy_quota: "
        f"status={agy_quota_digest.get('status')} "
        f"degraded_until={agy_quota_digest.get('degraded_until')} "
        f"observed_at={agy_quota_digest.get('observed_at')}"
    )
    cohort_accrual_digest = (
        market_scan_digest.get("cohort_shadow_accrual")
        if isinstance(market_scan_digest.get("cohort_shadow_accrual"), dict)
        else {}
    )
    cohort_accrual_line = (
        "market_cohort_shadow_accrual: "
        f"seated={cohort_accrual_digest.get('seated_count')} "
        f"movement={cohort_accrual_digest.get('movement_wallet_count')} "
        f"status={cohort_accrual_digest.get('movement_status')} "
        f"active_recent={cohort_accrual_digest.get('active_recent_count')} "
        f"active_recent_movement={cohort_accrual_digest.get('active_recent_movement_wallet_count')} "
        f"active_recent_status={cohort_accrual_digest.get('active_recent_movement_status')} "
        f"rt_wallets={cohort_accrual_digest.get('realtime_shadow_wallet_count')} "
        f"rt_windows={cohort_accrual_digest.get('realtime_shadow_windows_sum')} "
        f"guard_wallets={cohort_accrual_digest.get('guard_shadow_wallet_count')} "
        f"liveness={cohort_accrual_digest.get('bench_liveness_status_counts')}"
    ) if _as_int(cohort_accrual_digest.get("seated_count"), default=0) > 0 else None
    fresh_flow_probe_digest = (
        digest["gates"].get("fresh_flow_probe")
        if isinstance(digest["gates"].get("fresh_flow_probe"), dict)
        else {}
    )
    mining_digest = (
        digest.get("market_mining_cadence")
        if isinstance(digest.get("market_mining_cadence"), dict)
        else {}
    )
    mining_observed = (
        mining_digest.get("observed") if isinstance(mining_digest.get("observed"), dict) else {}
    )
    mining_null = (
        mining_digest.get("null_cycle") if isinstance(mining_digest.get("null_cycle"), dict) else {}
    )
    mining_next_scope = (
        mining_digest.get("next_scope") if isinstance(mining_digest.get("next_scope"), dict) else {}
    )
    mining_intake_window = (
        mining_digest.get("intake_window")
        if isinstance(mining_digest.get("intake_window"), dict)
        else {}
    )
    mining_intake_exhaustion = (
        mining_digest.get("intake_exhaustion")
        if isinstance(mining_digest.get("intake_exhaustion"), dict)
        else {}
    )
    mining_cadence_suffix = (
        " market_mining_cadence: "
        f"status={mining_digest.get('status')} "
        f"generated_at={mining_digest.get('generated_at')} "
        f"ran={mining_digest.get('ran_steps')} "
        f"due={mining_digest.get('due_steps')} "
        f"active={mining_observed.get('intake_active_wallets')} "
        f"cohort={mining_observed.get('cohort_size')} "
        f"shadow_positive={mining_observed.get('shadow_positive')} "
        f"live_ready={mining_observed.get('live_ready_picks')} "
        f"packets={mining_observed.get('packet_count')} "
        f"external_pass={mining_observed.get('external_liveness_pass')} "
        f"source_active_policy_pass={mining_observed.get('source_active_policy_pass')} "
        f"null={mining_null.get('status')} "
        f"widened={mining_null.get('scope_widened')} "
        f"lookback_complete={mining_intake_window.get('lookback_complete')} "
        f"oldest_trade={mining_intake_window.get('oldest_trade_iso_seen')} "
        f"exhaustion={mining_intake_exhaustion.get('class')} "
        f"next_scope={mining_next_scope}"
    )
    leaderboard_pipeline_digest = (
        digest.get("leaderboard_pipeline_heartbeat")
        if isinstance(digest.get("leaderboard_pipeline_heartbeat"), dict)
        else {}
    )
    leaderboard_pipeline_suffix = (
        " leaderboard_pipeline_heartbeat: "
        f"generated_at={leaderboard_pipeline_digest.get('generated_at')} "
        f"scan={leaderboard_pipeline_digest.get('scan_status')}"
        f"/rc={leaderboard_pipeline_digest.get('scan_returncode')}"
        f"/wall={leaderboard_pipeline_digest.get('scan_duration_s')} "
        f"registered={leaderboard_pipeline_digest.get('registered_wallets')}"
        f"/all={leaderboard_pipeline_digest.get('register_all_wallets')} "
        f"roster={leaderboard_pipeline_digest.get('pipeline_roster_wallets')} "
        f"history_rc={leaderboard_pipeline_digest.get('history_returncode')}"
        f"/wall={leaderboard_pipeline_digest.get('history_duration_s')} "
        f"ingest={leaderboard_pipeline_digest.get('data_api_ingest_status')} "
        f"skips={leaderboard_pipeline_digest.get('data_api_skip_count')} "
        f"timeouts={leaderboard_pipeline_digest.get('data_api_timeout')} "
        f"api_rows={leaderboard_pipeline_digest.get('api_page_rows_fetched')} "
        f"api_requested={leaderboard_pipeline_digest.get('api_period_limits_requested')} "
        f"api_effective_limit={leaderboard_pipeline_digest.get('api_effective_page_limit')} "
        f"api_terminal_skips={leaderboard_pipeline_digest.get('api_category_terminal_skips')} "
        f"api_errors={leaderboard_pipeline_digest.get('api_category_fetch_errors')}"
    )
    admission_digest = (
        digest.get("cohort_admission")
        if isinstance(digest.get("cohort_admission"), dict)
        else {}
    )
    admission_summary = (
        admission_digest.get("summary")
        if isinstance(admission_digest.get("summary"), dict)
        else {}
    )
    admission_top = (
        admission_digest.get("top_four_way_candidate")
        if isinstance(admission_digest.get("top_four_way_candidate"), dict)
        else {}
    )
    cohort_admission_suffix = (
        " cohort_admission: "
        f"generated_at={admission_digest.get('generated_at')} "
        f"four_way_ready={admission_summary.get('four_way_admission_ready')} "
        f"top={_short_wallet(str(admission_summary.get('top_four_way_wallet') or ''))} "
        f"candidate={admission_top.get('candidate_id')} "
        f"history={admission_top.get('history_completeness')} "
        f"resolved={admission_top.get('resolved_copyable_events')} "
        f"pnl={admission_top.get('paper_pnl_usd')} "
        f"roi={admission_top.get('roi_pct')} "
        f"paper_only={admission_digest.get('paper_only')} "
        f"live_allowed={admission_digest.get('live_orders_allowed')}"
        f" registry_probe={admission_digest.get('registry_liveness_generated_at')}"
        f" registry_probe_summary={admission_digest.get('registry_liveness_summary')}"
        f" observation_admitted={admission_digest.get('observation_admission_count')}"
        f" queue_observation={admission_digest.get('queue_observation_members')}"
        f" runtime_top_n={admission_digest.get('runtime_observation_top_n')}"
        f" watch_polled={len(admission_digest.get('watch_tier_source_wallets') or [])}"
        f" admitted_watch_polled={admission_digest.get('admitted_watch_tier_polled_count')}"
        f" watch_cohorts={admission_digest.get('watch_tier_source_wallet_cohorts')}"
        f" watch_fresh={admission_digest.get('watch_tier_fresh_by_wallet')}"
    )
    funnel_digest = digest.get("factory_funnel") if isinstance(digest.get("factory_funnel"), dict) else {}
    funnel_counts = funnel_digest.get("counts") if isinstance(funnel_digest.get("counts"), dict) else {}
    funnel_enemy = funnel_digest.get("enemy_line") if isinstance(funnel_digest.get("enemy_line"), dict) else {}
    factory_funnel_suffix = (
        " factory_funnel: "
        f"generated_at={funnel_digest.get('generated_at')} "
        f"enemy={funnel_enemy.get('status')} "
        f"enemy_link={funnel_enemy.get('link_id')} "
        f"topological_first={funnel_enemy.get('topological_first_link_id') or funnel_enemy.get('link_id')} "
        f"market={funnel_counts.get('market_population')} "
        f"active={funnel_counts.get('mined_actives')} "
        f"scored={funnel_counts.get('scored')} "
        f"positive={funnel_counts.get('shadow_positive')} "
        f"live_ready={funnel_counts.get('live_ready')} "
        f"admitted={funnel_counts.get('admitted')} "
        f"armed={funnel_counts.get('armed_runtime_loaded')} "
        f"submitted={funnel_counts.get('submitted_live_orders_today')} "
        f"filled={funnel_counts.get('filled_live_orders_today')} "
        f"profitable={funnel_counts.get('profitable_day')}"
    )
    paper_lane_line = None
    a689_edge_price_full = (
        a689_edge_digest.get("price_reject_full")
        if isinstance(a689_edge_digest.get("price_reject_full"), dict)
        else {}
    )
    a689_edge_price_guard = (
        a689_edge_digest.get("price_reject_guard_eligible")
        if isinstance(a689_edge_digest.get("price_reject_guard_eligible"), dict)
        else {}
    )
    a689_edge_price_late = (
        a689_edge_digest.get("price_reject_late_class_counts")
        if isinstance(a689_edge_digest.get("price_reject_late_class_counts"), dict)
        else {}
    )
    a689_edge_watch_full = (
        a689_edge_digest.get("watch_tier_current_full")
        if isinstance(a689_edge_digest.get("watch_tier_current_full"), dict)
        else {}
    )
    a689_edge_watch_guard = (
        a689_edge_digest.get("watch_tier_guard_eligible")
        if isinstance(a689_edge_digest.get("watch_tier_guard_eligible"), dict)
        else {}
    )
    a689_edge_thresholds = (
        a689_edge_digest.get("guard_thresholds")
        if isinstance(a689_edge_digest.get("guard_thresholds"), dict)
        else {}
    )
    if (
        inventory_convergence_digest
        or a689_hot_standby_digest
        or a689_edge_digest
        or a689_postfix_digest
        or a689_tripwire_digest
        or budget_bind_digest
        or f418_readmission_digest
        or ruling10_digest
        or d60c_latency_digest
        or live_reject_digest
    ):
        tripwire_floor = (
            a689_tripwire_digest.get("floor_budget_bind")
            if isinstance(a689_tripwire_digest.get("floor_budget_bind"), dict)
            else {}
        )
        paper_lane_line = (
            "paper_lanes: "
            f"inventory_convergence_status={inventory_convergence_digest.get('paper_lane_status')} "
            f"inventory_net_post_fee={inventory_convergence_digest.get('total_probe_cap_post_fee_pnl_usd')} "
            f"inventory_positive_post_fee={inventory_convergence_digest.get('positive_probe_cap_post_fee_pnl_usd')} "
            f"inventory_negative_post_fee={inventory_convergence_digest.get('negative_probe_cap_post_fee_pnl_usd')} "
            f"recoverable={inventory_convergence_digest.get('recoverable_positive_windows')} "
            f"recoverable_positive_only_post_fee={inventory_convergence_digest.get('recoverable_probe_cap_post_fee_pnl_usd')} "
            f"a689_ready={a689_hot_standby_digest.get('hot_standby_ready')} "
            f"a689_post_fee={a689_hot_standby_digest.get('in_lane_post_fee_pnl_usd')} "
            f"a689_resolved={a689_hot_standby_digest.get('resolved_paper_fills')} "
            f"a689_liveness={a689_hot_standby_digest.get('source_liveness_status')} "
            f"a689_cut={a689_cut_digest.get('execution_status')}/"
            f"{a689_cut_digest.get('atomic_verification_pass')} "
            f"successor={_short_wallet(a689_cut_digest.get('successor_wallet'))}/"
            f"{a689_cut_digest.get('successor_bound')} "
            f"a689_edge_gate_s={a689_edge_thresholds.get('window_time_suppress_gte_s')} "
            f"a689_price_full={a689_edge_price_full.get('resolved_n')}/{a689_edge_price_full.get('pnl_usd')} "
            f"a689_price_guard={a689_edge_price_guard.get('resolved_n')}/{a689_edge_price_guard.get('pnl_usd')} "
            f"a689_price_late={a689_edge_price_late} "
            f"a689_watch_current={a689_edge_watch_full.get('resolved_n')}/{a689_edge_watch_full.get('pnl_usd')} "
            f"a689_watch_guard={a689_edge_watch_guard.get('resolved_n')}/{a689_edge_watch_guard.get('pnl_usd')} "
            f"a689_postfix_rows={a689_postfix_digest.get('rows')}/"
            f"windows={a689_postfix_digest.get('windows')}/"
            f"accepted={a689_postfix_digest.get('accepted_order_rows')} "
            f"a689_postfix_classes={a689_postfix_digest.get('category_counts')} "
            f"a689_0200_verdict={a689_tripwire_digest.get('verdict')} "
            f"a689_0200_action={a689_tripwire_digest.get('pre_ruled_action')} "
            f"a689_0200_classes={a689_tripwire_digest.get('tripwire_class_counts')} "
            f"a689_0200_policy_max_1={tripwire_floor.get('policy_max_1_rows')} "
            f"pipeline_late_decomp={pipeline_late_decomp_digest.get('verdict')}/"
            f"{pipeline_late_decomp_digest.get('pre_ruled_action')} "
            f"rows={pipeline_late_decomp_digest.get('pipeline_late_rows')} "
            f"windows={pipeline_late_decomp_digest.get('pipeline_late_windows')} "
            f"first_late={pipeline_late_decomp_digest.get('first_evaluation_late_gte_180s')} "
            f"first_timely={pipeline_late_decomp_digest.get('first_touch_timely_lt_180s')} "
            f"cycle_p50={pipeline_late_cycle_digest.get('p50')} "
            f"budget_bind={budget_bind_digest.get('verdict')}/"
            f"{budget_bind_digest.get('pre_ruled_action')} "
            f"bind_rows={budget_bind_summary.get('bind_rows')} "
            f"round_up_eligible={budget_bind_summary.get('round_up_eligible_rows')} "
            f"round_up_resolved={budget_bind_summary.get('round_up_resolved_rows')} "
            f"round_up_pnl={budget_bind_summary.get('round_up_would_pnl_usd')} "
            f"f418_readmit={f418_readmission_digest.get('status')} "
            f"f418_basis={f418_counterfactual_digest.get('basis')} "
            f"f418_n={f418_routing_shadow_counterfactual.get('measurable_resolved_intents')} "
            f"f418_post_fee={f418_routing_shadow_counterfactual.get('post_fee_pnl_usd')} "
            f"eth5m={eth5m_replication_digest.get('status')}:"
            f"{eth5m_replication_digest.get('observations')}/"
            f"{eth5m_replication_digest.get('distinct_windows')}:"
            f"resolved={eth5m_replication_digest.get('resolved_intents')}/"
            f"{eth5m_replication_digest.get('resolved_windows')}:"
            f"post_fee={eth5m_replication_digest.get('post_fee_pnl_usd')}:"
            f"gate={eth5m_replication_digest.get('promotion_gate_pass')}:"
            f"parity={eth5m_replication_digest.get('copyintent_parity')}:"
            f"live={eth5m_replication_digest.get('live_orders_allowed')}:"
            f"cadence={eth5m_replication_digest.get('cadence')} "
            f"ruling10_abstain_due={ruling10_summary.get('due_labels')} "
            f"ruling10_members={[(row.get('label'), row.get('top_abstain_reasons')) for row in ruling10_members[:3] if isinstance(row, dict)]} "
            f"d60c_latency={d60c_latency_summary.get('majority_verdict')} "
            f"target={d60c_latency_summary.get('target_market_closed_count')} "
            f"recovered={d60c_latency_summary.get('recovered_market_closed_count')} "
            f"classes={d60c_latency_summary.get('class_counts')} "
            f"live_rejects={live_reject_summary.get('rejects')}/"
            f"{live_reject_summary.get('orders')} "
            f"dominant={live_reject_summary.get('dominant_class')} "
            f"scheduler_subset={((live_reject_summary.get('scheduler_triggered_subset') or {}) if isinstance(live_reject_summary.get('scheduler_triggered_subset'), dict) else {}).get('rejects')} "
            f"forgone={live_reject_summary.get('forgone_usd_estimate')} "
            f"fak_unrecovered={live_reject_fak_summary.get('unrecovered_rows')}/"
            f"{live_reject_fak_summary.get('net_unrecovered_forgone_usd')} "
            f"race_loss={live_reject_fak_summary.get('race_loss_usd')} "
            f"negative_fills={live_negative_summary.get('negative_fills')} "
            f"negative_pnl={live_negative_summary.get('negative_pnl_usd')} "
            f"maker_recovery_avg={live_negative_maker_vs_direct.get('maker_recovery_avg_pnl_usd')} "
            f"direct_taker_avg={live_negative_maker_vs_direct.get('direct_taker_avg_pnl_usd')} "
            f"band_gate_passes={live_negative_band_gate_passes} "
            f"band_tranche_roi={live_negative_band_roi}"
        )
    admission_wave_digest = (
        digest["active_set"].get("admission_wave")
        if isinstance(digest["active_set"].get("admission_wave"), dict)
        else {}
    )
    admission_wave_suffix = ""
    if admission_wave_digest:
        admission_wave_suffix = (
            " admission_wave: "
            f"status={admission_wave_digest.get('status')} "
            f"direction={admission_wave_digest.get('direction_id')} "
            f"picked={admission_wave_digest.get('picked_count')} "
            f"admitted={admission_wave_digest.get('admitted_count')} "
            f"runtime_loaded={admission_wave_digest.get('runtime_loaded_count')} "
            f"runtime_missing={admission_wave_digest.get('runtime_missing_count')} "
            f"filled={admission_wave_digest.get('filled_count')} "
            f"runtime_cap={admission_wave_digest.get('runtime_cap')} "
            f"aging_gt24h_unruled={admission_wave_digest.get('unruled_aging_gt24h_count')} "
            f"timestamps={admission_wave_digest.get('raw_input_timestamps')} "
            f"outputs={admission_wave_digest.get('raw_output_timestamps')} "
            f"freshness={admission_wave_digest.get('freshness_deadman')} "
            f"input_freshness={admission_wave_digest.get('input_freshness')} "
            f"own_source={admission_wave_digest.get('own_source_rows_30m')} "
            f"intents={admission_wave_digest.get('intent_count')} "
            f"submits={admission_wave_digest.get('submit_count')} "
            f"reported_fills={admission_wave_digest.get('reported_fill_count')} "
            f"picked_wallets={[ _short_wallet(str(wallet)) for wallet in admission_wave_digest.get('picked_wallets', [])[:10] ]} "
            f"runtime_wallets={[ _short_wallet(str(wallet)) for wallet in admission_wave_digest.get('runtime_loaded_wallets', [])[:10] ]} "
            f"runtime_missing_wallets={[ _short_wallet(str(wallet)) for wallet in admission_wave_digest.get('runtime_missing_wallets', [])[:10] ]} "
            f"temporal_excluded_wallets={[ _short_wallet(str(wallet)) for wallet in admission_wave_digest.get('temporal_excluded_wallets', [])[:10] ]} "
            f"filled_wallets={[ _short_wallet(str(wallet)) for wallet in admission_wave_digest.get('filled_wallets', [])[:10] ]}"
            f" forward_seat_clocks={admission_wave_digest.get('forward_seat_clocks')}"
        )
    e6db_cap_cut_digest = (
        digest["active_set"].get("e6db_2000_probe_cap_cut")
        if isinstance(digest["active_set"].get("e6db_2000_probe_cap_cut"), dict)
        else {}
    )
    e6db_cap_cut_suffix = ""
    if e6db_cap_cut_digest:
        touched = e6db_cap_cut_digest.get("members_touched")
        first_touched = touched[0] if isinstance(touched, list) and touched and isinstance(touched[0], dict) else {}
        e6db_cap_cut_suffix = (
            " e6db_2000_cap_cut: "
            f"status={e6db_cap_cut_digest.get('status')} "
            f"applied={e6db_cap_cut_digest.get('applied_at')} "
            f"pnl={e6db_cap_cut_digest.get('trigger_pnl_usd')}/"
            f"{e6db_cap_cut_digest.get('threshold_pnl_usd')} "
            f"old={first_touched.get('old_member_max_order_usd')} "
            f"new={first_touched.get('new_max_order_usd')} "
            f"mutation={e6db_cap_cut_digest.get('live_path_mutation')}"
        )
    wave_gate_digest = (
        digest["active_set"].get("wave_gate_attribution")
        if isinstance(digest["active_set"].get("wave_gate_attribution"), dict)
        else {}
    )
    wave_gate_suffix = ""
    if wave_gate_digest:
        wave_gate_suffix = (
            " wave_gate_attribution: "
            f"status={wave_gate_digest.get('gate_status')} "
            f"members={wave_gate_digest.get('members')} "
            f"cycles={wave_gate_digest.get('total_cycles_landed')} "
            f"fresh={wave_gate_digest.get('total_fresh_intents')} "
            f"orders={wave_gate_digest.get('total_ledger_orders')} "
            f"classes={wave_gate_digest.get('classification_counts')} "
            f"path={wave_gate_digest.get('path')}"
        )
    gate_3048_digest = (
        digest["active_set"].get("admitted_member_gate_3048")
        if isinstance(digest["active_set"].get("admitted_member_gate_3048"), dict)
        else {}
    )
    gate_3048_suffix = ""
    if gate_3048_digest:
        first_row = (
            gate_3048_digest.get("rows", [])[0]
            if isinstance(gate_3048_digest.get("rows"), list)
            and gate_3048_digest.get("rows")
            and isinstance(gate_3048_digest.get("rows", [])[0], dict)
            else {}
        )
        gate_3048_suffix = (
            " gate_3048: "
            f"status={gate_3048_digest.get('gate_status')} "
            f"class={first_row.get('classification')} "
            f"policy_windows={first_row.get('policy_eligible_windows')} "
            f"fresh={first_row.get('fresh_intents')} "
            f"orders={first_row.get('ledger_orders')} "
            f"path={gate_3048_digest.get('path')}"
        )
    wave_repair_digest = (
        digest["active_set"].get("wave_repair_addendum")
        if isinstance(digest["active_set"].get("wave_repair_addendum"), dict)
        else {}
    )
    wave_repair_suffix = ""
    if wave_repair_digest:
        wave_repair_suffix = (
            " wave_repair_addendum: "
            f"status={wave_repair_digest.get('status')} "
            f"eval_all_current={wave_repair_digest.get('guard_evaluates_all_runtime_members_per_cycle_current')} "
            f"main_selected_per_cycle={wave_repair_digest.get('main_live_execution_selected_members_per_cycle')} "
            f"flag={wave_repair_digest.get('all_member_flag_added')} "
            f"default={wave_repair_digest.get('all_member_flag_default')} "
            f"zero_cycles={wave_repair_digest.get('zero_cycle_wallets_explained')} "
            f"single_seat_reclass={wave_repair_digest.get('single_seat_only_reclass_confirmed_for_pipeline_no_intents')} "
            f"corrected={wave_repair_digest.get('pipeline_no_intents_corrected_classification_counts')} "
            f"path={wave_repair_digest.get('path')}"
        )

    def compact_for_header(rows: list[Any], *, limit: int = 3, max_chars: int = 600) -> str:
        selected = [str(row).strip() for row in rows[:limit] if str(row).strip()]
        text = " ".join(selected)
        if len(rows) > limit:
            text = f"{text} ... (+{len(rows) - limit} lines)"
        if len(text) > max_chars:
            return f"{text[: max_chars - 3]}..."
        return text

    direction_next_header = compact_for_header(digest["latest_direction_next_verbatim"])
    direction_material_header = compact_for_header(digest["latest_direction_material"])
    notification_daily = digest.get("operator_notification_discipline", {}).get("daily_counts", {})
    notification_daily = notification_daily if isinstance(notification_daily, dict) else {}
    notification_day = sorted(notification_daily)[-1] if notification_daily else None
    notification_rows = (
        notification_daily.get(notification_day)
        if notification_day and isinstance(notification_daily.get(notification_day), dict)
        else {}
    )
    notification_counts = {
        key: {
            "count": value.get("count"),
            "push_count": value.get("push_count"),
            "self_healed_count": value.get("self_healed_count"),
        }
        for key, value in notification_rows.items()
        if isinstance(value, dict)
        and key.replace("_", "").isalnum()
        and key.upper() == key
        and " " not in key
    }

    payout_line = None
    payout_digest = (digest.get("pnl") or {}).get("payout_receipt_reconciliation") or {}
    if payout_digest.get("status"):
        payout_line = (
            "payout_receipt_reconciliation: "
            f"status={payout_digest.get('status')} "
            f"summary={payout_digest.get('summary')} "
            f"seam={payout_digest.get('coverage_seam')}"
        )
    temporal_divergence_line = None
    temporal_divergence_digest = (digest.get("pnl") or {}).get("temporal_slice_label_divergence") or {}
    if temporal_divergence_digest.get("status"):
        temporal_divergence_line = (
            "temporal_slice_label_divergence: "
            f"authority={temporal_divergence_digest.get('authority')} "
            f"summary={temporal_divergence_digest.get('summary')}"
        )
    receipt_bank_line = None
    receipt_bank = (
        ((digest.get("pnl") or {}).get("fee_realization_bank_reconciliation") or {}).get("bank_identity")
    )
    if receipt_bank:
        receipt_bank_line = f"receipt_bank_identity: {receipt_bank}"

    lines = [
        "# Agent State Digest",
        f"generated_at: {digest['generated_at']}",
        "validation_mode: SHADOW - read this AND full source context; never digest-only.",
        "grade_required: every STATUS says whether this digest contained all context used.",
        (
            f"latest_direction: {digest['latest_direction'] or 'missing'} | "
            f"next={direction_next_header or 'missing'} | "
            f"material={direction_material_header or 'missing'} | "
            f"latest_handoff={digest.get('latest_handoff') or 'missing'} "
            f"(ts={digest.get('latest_handoff_ts')})"
        ),
        *([payout_line] if payout_line else []),
        *([temporal_divergence_line] if temporal_divergence_line else []),
        *([receipt_bank_line] if receipt_bank_line else []),
        *(
            [
                (
                    "operator_notifications: "
                    f"day={notification_day} counts={notification_counts} "
                    f"active_actionable={digest.get('operator_notification_discipline', {}).get('active_actionable_classes')} "
                    "rule=new-class-or-actionable-only; repeats/self-healed aggregate without push"
                )
            ]
            if notification_day
            else []
        ),
        *(
            [
                (
                    "two_arm_concentration: "
                    f"generated_at={digest['two_arm_concentration_decomposition'].get('generated_at')} "
                    "arms="
                    f"{[(row.get('arm'), ((row.get('base') or {}).get('resolved')), ((row.get('base') or {}).get('distinct_markets')), ((row.get('base') or {}).get('top_1_market_share_of_total_pnl_pct')), ((row.get('base') or {}).get('win_rate_pct')), ((row.get('min_price_0_10') or {}).get('post_fee_pnl_usd')), ((row.get('verdict') or {}).get('classification'))) for row in digest['two_arm_concentration_decomposition'].get('arms', []) if isinstance(row, dict)]} "
                    f"seat_precommit={digest['two_arm_concentration_decomposition'].get('seat_82c8_maturity_precommit')} "
                    f"execution={digest['two_arm_concentration_decomposition'].get('execution_status')} "
                    f"path={digest['two_arm_concentration_decomposition'].get('path')}"
                )
            ]
            if digest["two_arm_concentration_decomposition"].get("path")
            else []
        ),
        (
            "direction_freshness: "
            f"{digest.get('direction_freshness_status')} "
            f"latest_ts={digest.get('latest_direction_ts')} "
            f"newest_fable_ts={digest.get('newest_fable_direction_ts')} "
            "commitments_overdue="
            f"{{count:{digest['commitments_overdue'].get('overdue')},"
            f"oldest_id:{digest['commitments_overdue'].get('oldest_id')}}} "
            f"late={digest['commitments_overdue'].get('late')} "
            f"due_today={digest['commitments_overdue'].get('due_today')} "
            f"active={digest['commitments_overdue'].get('active')} "
            f"evidence_unmarked={digest['commitments_overdue'].get('evidence_unmarked')} "
            f"overdue_with_evidence={digest['commitments_overdue'].get('overdue_with_evidence')} "
            f"sample={digest['commitments_overdue'].get('sample_ids')}"
        ),
        (
            f"latest_status: {digest['latest_status'] or 'missing'} | "
            "cli_versions: "
            f"status={cli_versions_digest.get('status')} "
            f"updated_at={cli_versions_digest.get('updated_at')} "
            f"stale={cli_versions_digest.get('stale_tools')} "
            f"unknown_latest={cli_versions_digest.get('unknown_latest_tools')} "
            f"pending_smoke={bool(cli_versions_digest.get('pending_smoke_test'))} "
            f"last_smoke={cli_versions_digest.get('last_smoke_test')} "
            "model_evidence="
            f"{(cli_versions_digest.get('model_runtime_evidence') or {}).get('status')}/"
            f"{(cli_versions_digest.get('model_runtime_evidence') or {}).get('configured_model')}/"
            f"{(cli_versions_digest.get('model_runtime_evidence') or {}).get('runtime_model')} "
            f"tools={{{', '.join(cli_versions_tool_labels)}}} | "
            "boot_recovery="
            f"status={digest['boot_recovery_audit'].get('status')} "
            f"generated_at={digest['boot_recovery_audit'].get('generated_at')} "
            f"defects={digest['boot_recovery_audit'].get('defects')} "
            f"stale_locks={digest['boot_recovery_audit'].get('stale_locks')} "
            f"held_locks={digest['boot_recovery_audit'].get('held_locks')} | "
            "live_guard_restart="
            f"status={digest['live_guard_restart'].get('status')} "
            f"generated_at={digest['live_guard_restart'].get('generated_at')} "
            f"reason={digest['live_guard_restart'].get('reason')} "
            f"preflight={digest['live_guard_restart'].get('preflight_status')}/"
            f"{digest['live_guard_restart'].get('preflight_passed')} "
            f"checks={digest['live_guard_restart'].get('preflight_checks')} "
            f"pid={digest['live_guard_restart'].get('actual_pid')} "
            f"sweep={digest['live_guard_restart'].get('sweep_status')} "
            f"deleted={digest['live_guard_restart'].get('sweep_deleted')} "
            f"retained={digest['live_guard_restart'].get('sweep_retained')} "
            "latest_executed="
            f"{digest['live_guard_restart']['latest_executed_restart'].get('status')}@"
            f"{digest['live_guard_restart']['latest_executed_restart'].get('generated_at')} "
            "executed_preflight="
            f"{digest['live_guard_restart']['latest_executed_restart'].get('preflight_status')}/"
            f"{digest['live_guard_restart']['latest_executed_restart'].get('preflight_passed')} "
            "executed_pid="
            f"{digest['live_guard_restart']['latest_executed_restart'].get('actual_pid')} "
            "executed_sweep="
            f"{digest['live_guard_restart']['latest_executed_restart'].get('sweep_status')} "
            "generation_delta="
            f"{digest['live_guard_restart']['generation_delta'].get('status')}/"
            f"{digest['live_guard_restart']['generation_delta'].get('reconstruction_status')} "
            f"citable={digest['live_guard_restart']['generation_delta'].get('citation_allowed')} "
            f"changed={digest['live_guard_restart']['generation_delta'].get('changed_count')}:"
            f"{digest['live_guard_restart']['generation_delta'].get('changed_paths')}"
        ),
        *([agy_smoke_line] if agy_smoke_digest.get("result") else []),
        *([agy_quota_line] if agy_quota_digest.get("status") else []),
        "## Live",
        (
            "guard: "
            f"live_selection_surfaces={digest.get('live_selection_surfaces', [])} "
            f"pid={digest['live']['guard_pid']} status={digest['live']['guard_status']} "
            f"can_trade={digest['live']['can_trade']} "
            f"(source={digest['live']['can_trade_source']}) "
            f"live_allowed={digest['live']['live_orders_allowed']} "
            f"live_gate={digest['live']['runtime_permission'].get('status')}/"
            f"{digest['live']['runtime_permission'].get('first_blocker')} "
            f"gate_updated={digest['live']['runtime_permission'].get('updated_at')} "
            f"gate_owner={digest['live']['runtime_permission'].get('owner')} "
            "f418_green_day_conversion="
            f"{f418_conversion_digest.get('verdict')}/"
            f"{f418_conversion_digest.get('sample_gate_pass')}/"
            f"{f418_conversion_digest.get('dual_bar_bottleneck')} "
            f"control={f418_conversion_digest.get('control_pre_sign')} "
            f"post={f418_conversion_digest.get('green_sign_post')} "
            f"delta_pp={f418_conversion_digest.get('conversion_delta_pct_points')} "
            f"live_mutation={f418_conversion_digest.get('live_mutation')} "
            "f418_size_fee_leak="
            f"{f418_fee_leak_digest.get('status')}:"
            f"micro={f418_fee_leak_micro.get('n')}/{f418_fee_leak_micro.get('post_fee_ev_per_fill_usd')}:"
            f"standing={f418_fee_leak_standing.get('n')}/{f418_fee_leak_standing.get('post_fee_ev_per_fill_usd')}:"
            f"ev_delta={f418_fee_leak_comparison.get('micro_minus_standing_ev_usd')}:"
            f"ci95=[{f418_fee_leak_comparison.get('ci95_low_usd')},{f418_fee_leak_comparison.get('ci95_high_usd')}]:"
            f"fee_delta_pp={f418_fee_leak_comparison.get('expected_fee_share_delta_pp')}:"
            f"live_mutation={f418_fee_leak_digest.get('live_mutation')} "
            "f418_spread_elasticity="
            f"{f418_spread_digest.get('status')}:"
            f"coverage={f418_spread_digest.get('coverage')}:"
            f"gate={f418_spread_digest.get('gate')}:"
            f"cells={f418_spread_digest.get('cells')}:"
            f"live_mutation={f418_spread_digest.get('live_mutation')} "
            "fak_depth_persistence="
            f"{fak_depth_digest.get('status')}:"
            f"coverage={fak_depth_digest.get('coverage')}:"
            f"metrics={fak_depth_digest.get('metrics')}:"
            f"gate={fak_depth_digest.get('gate')}:"
            f"parity={fak_depth_digest.get('parity')}:"
            f"live_mutation={fak_depth_digest.get('live_mutation')} "
            "member_native_uplift="
            f"{member_native_uplift_digest.get('verdict')}:"
            f"members={member_native_uplift_digest.get('member_count')}/"
            f"{member_native_uplift_digest.get('frozen_policy_binding_count')}:"
            f"incremental={member_native_uplift_digest.get('incremental')}:"
            f"incumbent={member_native_uplift_digest.get('incumbent_twin')}:"
            f"gate={member_native_uplift_digest.get('gate')}:"
            f"runner={member_native_uplift_digest.get('persistent_runner')}:"
            f"single_submitter={member_native_uplift_digest.get('single_submitter_preserved')}:"
            f"live_mutation={member_native_uplift_digest.get('live_mutation')}"
        ),
        (
            "guard_process: "
            f"status={digest['live']['guard_processes'].get('status')} "
            f"count={digest['live']['guard_processes'].get('process_count')} "
            f"rows={[(row.get('pid'), row.get('stat'), row.get('etime')) for row in digest['live']['guard_processes'].get('rows', [])]} "
            f"rtds_tail_bytes={[(row.get('rtds_tail_bytes'), row.get('rtds_cold_tail_bytes')) for row in digest['live']['guard_processes'].get('rows', [])]} "
            f"material_dirty={digest.get('working_tree_material', {}).get('count')} "
            f"material_paths={digest.get('working_tree_material', {}).get('paths')} "
            f"truncated={digest.get('working_tree_material', {}).get('truncated')} "
            "intent_proof="
            f"status={digest['live']['intent_time_copyability_proof'].get('status')} "
            f"generated_at={digest['live']['intent_time_copyability_proof'].get('generated_at')} "
            f"records={digest['live']['intent_time_copyability_proof'].get('records')} "
            f"distinct={digest['live']['intent_time_copyability_proof'].get('distinct_intents')} "
            f"accepted={digest['live']['intent_time_copyability_proof'].get('required_buy_copy_events')}/"
            f"{digest['live']['intent_time_copyability_proof'].get('clob_filled_buy_copy_events')} "
            f"rejected={digest['live']['intent_time_copyability_proof'].get('copyability_rejected_buy_events')} "
            f"blockers={digest['live']['intent_time_copyability_proof'].get('blocker_counts')} "
            f"coverage={digest['live']['intent_time_copyability_proof'].get('covered_member_count_this_cycle')}/"
            f"{digest['live']['intent_time_copyability_proof'].get('runtime_member_count_this_cycle')} "
            f"dropped={digest['live']['intent_time_copyability_proof'].get('dropped_runtime_members_this_cycle')} "
            f"sampled={digest['live']['intent_time_copyability_proof'].get('sampled_intents_this_cycle')}"
        ),
        (
            "guard_loop_profile: "
            f"status={digest['live'].get('guard_loop_profile', {}).get('status')} "
            f"total_s={digest['live'].get('guard_loop_profile', {}).get('total_s_before_state_write')} "
            f"target_lt={digest['live'].get('guard_loop_profile', {}).get('target_median_iteration_lt_s')} "
            f"latency_trigger={guard_latency_trigger.get('status')}/"
            f"{guard_latency_trigger.get('max_consecutive_over_threshold')} "
            f"scope={guard_latency_trigger.get('scope')} pid={guard_latency_trigger.get('current_pid')} "
            f"historical={guard_latency_trigger.get('historical_status')}/"
            f"{guard_latency_trigger.get('historical_max_consecutive_over_threshold')} "
            f"recent={guard_latency_recent} "
            f"cadence={digest['live'].get('guard_loop_profile', {}).get('slow_path_cadence')} "
            f"top_stages={[(row.get('name'), row.get('duration_s')) for row in digest['live'].get('guard_loop_profile', {}).get('top_stage_timers', [])]} "
            "speed_baseline="
            f"status={runtime_speed_digest.get('status')} "
            f"baseline={runtime_speed_digest.get('baseline_created_at')} "
            f"regressions={runtime_speed_digest.get('regression_count')}:{runtime_speed_regressions} "
            f"guard={runtime_speed_metrics.get('guard_cycle_total_s')} "
            f"signal_p90={runtime_speed_metrics.get('signal_age_p90_s')} "
            f"heartbeat={runtime_speed_metrics.get('heartbeat_cadence_latest_s')} "
            f"brainless={_runtime_speed_metric_label('brainless_run_duration_s')} "
            f"scorecard_direct={scorecard_runtime_digest.get('real_s')}/"
            f"{scorecard_runtime_digest.get('status')} "
            f"ask_fable={_runtime_speed_metric_label('ask_fable_latest_wall_s')} "
            f"digest={runtime_speed_metrics.get('state_digest_generation_s')} "
            f"signal_to_order_p90={runtime_speed_metrics.get('signal_to_order_p90_s')} "
            f"persistence_scope={runtime_speed_persistence.get('scope')}@"
            f"{runtime_speed_persistence.get('scope_start_at')} "
            f"persistence_counts={runtime_speed_persistence_nonzero} "
            f"actionable={runtime_speed_persistence.get('actionable_candidates')} "
            "alternate_transport="
            f"{digest['live'].get('alternate_transport_bridge', {}).get('status')}/"
            f"input:{digest['live'].get('alternate_transport_bridge', {}).get('input_delta_rows')}/"
            f"survivors:{digest['live'].get('alternate_transport_bridge', {}).get('post_protection_survivors')}/"
            f"submits:{digest['live'].get('alternate_transport_bridge', {}).get('submit_stage_invocations')}/"
            f"accept_ts:{digest['live'].get('accepted_order_liveness_ts')} "
            "latest_live_order="
            f"{digest['live'].get('latest_live_order', {}).get('status')}/"
            f"{digest['live'].get('latest_live_order', {}).get('order_id')}/"
            f"{digest['live'].get('latest_live_order', {}).get('funded_shares')}sh/"
            f"${digest['live'].get('latest_live_order', {}).get('funded_notional_usd')}/"
            f"fill:{digest['live'].get('latest_live_order', {}).get('response_fill_size_shares')} "
            "latest_accepted_order="
            f"{digest['live'].get('latest_accepted_live_order', {}).get('status')}/"
            f"{digest['live'].get('latest_accepted_live_order', {}).get('order_id')}/"
            f"{digest['live'].get('latest_accepted_live_order', {}).get('funded_shares')}sh/"
            f"${digest['live'].get('latest_accepted_live_order', {}).get('funded_notional_usd')}/"
            f"fill:{digest['live'].get('latest_accepted_live_order', {}).get('response_fill_size_shares')}"
        ),
        *(
            [
                (
                    "guard_json_cache: "
                    f"status={guard_json_cache_digest.get('status')} "
                    f"patch={guard_json_cache_digest.get('patch_status')} "
                    f"mode={guard_json_cache_digest.get('cache_mode')} "
                    f"default={guard_json_cache_digest.get('default_load_json_semantics')} "
                    f"hot={guard_json_cache_digest.get('hot_file')} "
                    f"size={guard_json_cache_digest.get('hot_file_size_bytes')} "
                    f"orders={guard_json_cache_digest.get('hot_file_order_rows')} "
                    f"first_s={guard_json_cache_digest.get('first_load_s')} "
                    f"second_s={guard_json_cache_digest.get('second_load_s')} "
                    f"same_object={guard_json_cache_digest.get('same_object')} "
                    f"cache_entries={guard_json_cache_digest.get('cache_entries_after_probe')} "
                    f"min_bytes={guard_json_cache_digest.get('cache_min_bytes')} "
                    f"max_entries={guard_json_cache_digest.get('cache_max_entries')} "
                    f"large_files={len(guard_json_cache_digest.get('large_json_files') or [])} "
                    f"hot_sites={guard_json_cache_digest.get('hot_call_sites')} "
                    f"cycle={guard_json_cache_digest.get('cycle_stage_evidence')} "
                    f"artifact={guard_json_cache_digest.get('artifact')}"
                )
            ]
            if guard_json_cache_digest.get("status")
            else []
        ),
        *(
            [
                "ready_shadow_dead_source_adjudication: "
                f"wallet={_short_wallet(str(row.get('wallet') or ''))} "
                f"status={row.get('status')} "
                f"adjudicated_at={row.get('adjudicated_at')} "
                f"source_last_trade={row.get('source_last_trade_iso')} "
                f"slot_action={row.get('slot_action')} "
                f"reenrollment={row.get('reenrollment_rule')}"
                for row in ready_shadow_adjudications_digest
                if isinstance(row, dict)
            ]
        ),
        (
            "polygon_ws_shadow: "
            f"status={digest['live']['polygon_ws_shadow'].get('status')} "
            f"pid={digest['live']['polygon_ws_shadow'].get('pid')} "
            f"started_at={digest['live']['polygon_ws_shadow'].get('started_at')} "
            f"cycles={digest['live']['polygon_ws_shadow'].get('cycles')} "
            f"process_count={digest['live']['polygon_ws_shadow'].get('process_invariant', {}).get('count')} "
            f"paper_only={digest['live']['polygon_ws_shadow'].get('paper_only')} "
            f"live_allowed={digest['live']['polygon_ws_shadow'].get('live_orders_allowed')}"
        ),
        (
            "ledger: "
            f"orders={digest['live']['orders']} fills={digest['live']['fills']} "
            f"rejects={digest['live']['rejects']} submitted={digest['live']['submitted']} "
            f"latest={digest['live']['latest_order_ts']} "
            f"recent_fills={digest['live'].get('recent_fills')} "
            f"monday_return_proof={digest['live'].get('monday_return_proof', {}).get('status')} "
            f"target={_short_wallet(str(digest['live'].get('monday_return_proof', {}).get('target_wallet') or ''))} "
            f"targets={digest['live'].get('monday_return_proof', {}).get('target_count')} "
            f"bench_ids={digest['live'].get('monday_return_proof', {}).get('bench_ids')} "
            f"return_at={digest['live'].get('monday_return_proof', {}).get('auto_return_at')} "
            f"unbenched={digest['live'].get('monday_return_proof', {}).get('target_unbenched')} "
            f"selected={_short_wallet(str(digest['live'].get('monday_return_proof', {}).get('runtime_selected_wallet') or ''))} "
            f"auto_return={digest['live'].get('monday_return_proof', {}).get('auto_return_reason')} "
            "selected_member_guard_submit_attribution: "
            f"status={selected_attribution_digest.get('status')} "
            f"defect={selected_attribution_digest.get('defect_classification')} "
            f"wallets={selected_attribution_digest.get('selected_wallet_count')} "
            f"eligible={selected_attribution_digest.get('selected_policy_eligible_unique_intents')} "
            f"submitted={selected_attribution_digest.get('submitted_intents')} "
            f"telemetry={selected_attribution_digest.get('telemetry_defects')} "
            f"wiring={selected_attribution_digest.get('wiring_defects')} "
            f"stages={selected_attribution_digest.get('terminal_stage_counts')} "
            f"live_mutation={selected_attribution_digest.get('live_mutation')}"
        ),
        (
            "flow_incident_foreground: "
            f"status={flow_incident_digest.get('status')} "
            f"can_trade={flow_incident_digest.get('can_trade')} "
            f"accepted_idle_s={flow_incident_digest.get('accepted_order_idle_s')} "
            f"consecutive={flow_incident_digest.get('consecutive_incidents')} "
            f"selected={_short_wallet(str(flow_incident_digest.get('selected_wallet') or ''))} "
            "order147="
            f"{digest.get('order147_seat_feedstock', {}).get('status')}/"
            f"{digest.get('order147_seat_feedstock', {}).get('pre_registered_branch')}/"
            f"{(digest.get('order147_seat_feedstock', {}).get('paper_accumulator') or {}).get('seated_wallet_buy_rows_in_generation')}/"
            f"{(digest.get('order147_seat_feedstock', {}).get('paper_accumulator') or {}).get('unique_chain_identities')}/"
            f"{(digest.get('order147_seat_feedstock', {}).get('paper_accumulator') or {}).get('guard_read_unique_identities')} "
            "order148="
            f"{(digest.get('order148_seated_fill_dispositions', {}).get('price_band_hypothesis') or {}).get('status')}/"
            f"{(digest.get('order148_seated_fill_dispositions', {}).get('supply_goal_arithmetic') or {}).get('in_band_fills')}/"
            f"{(digest.get('order148_seated_fill_dispositions', {}).get('supply_goal_arithmetic') or {}).get('perfect_all_wins_daily_profit_upper_bound_usd')} "
            "order149="
            f"{digest.get('order149_rotation_qualification', {}).get('status')}/"
            f"{digest.get('order149_rotation_qualification', {}).get('pre_registered_branch')}/"
            f"{digest.get('order149_rotation_qualification', {}).get('shortlist_wallet_count')}/"
            f"{digest.get('order149_rotation_qualification', {}).get('exact_blocking_fields')} "
            "order149_meta="
            f"{digest.get('order149_token_metadata_backfill', {}).get('pre_registered_branch')}/"
            f"{digest.get('order149_token_metadata_backfill', {}).get('metadata_resolved')}/"
            f"{digest.get('order149_token_metadata_backfill', {}).get('unique_rows')}/"
            f"residual_tokens={len(digest.get('order149_token_metadata_backfill', {}).get('unresolved_token_ids') or [])} "
            "order149_gamma="
            f"{digest.get('order149_gamma_metadata_recovery', {}).get('status')}/"
            f"{digest.get('order149_gamma_metadata_recovery', {}).get('recovered_count')}/"
            f"{digest.get('order149_gamma_metadata_recovery', {}).get('residual_count')} "
            "order149_depth="
            f"{digest.get('order149_depth_at_size', {}).get('verdict')}/"
            f"{digest.get('order149_depth_at_size', {}).get('snapshot_count')}/"
            f"{[(row.get('target_usd'), row.get('within_250bps_of_best_ask_fill_rate')) for row in digest.get('order149_depth_at_size', {}).get('targets') or []]} "
            "order150_supply="
            f"{digest.get('order150_window_supply_attribution', {}).get('elapsed_windows')}/"
            f"{digest.get('order150_window_supply_attribution', {}).get('category_counts')}/"
            f"recoverable={digest.get('order150_window_supply_attribution', {}).get('book_observed_unreasoned_windows_with_12p138_fillability')} "
            "order150_joint="
            f"{digest.get('order150_joint_supply_size_projection', {}).get('status')}/"
            f"{digest.get('order150_joint_supply_size_projection', {}).get('projected_effective_fillable_windows_per_288')}/"
            f"${digest.get('order150_joint_supply_size_projection', {}).get('projected_incremental_daily_profit_usd')} "
            "order151_fading="
            f"{digest.get('order151_bf337_fading_adjudication', {}).get('verdict')}/"
            f"recent_pnl={digest.get('order151_bf337_fading_adjudication', {}).get('classifier_own_basis', {}).get('recent_profile', {}).get('pnl_usd')}/"
            f"rotated={digest.get('order151_bf337_fading_adjudication', {}).get('rotation_authorized')} "
            "order151_fee_bps="
            f"{digest.get('order151_fee_bps', {}).get('resolved_fill_basis', {}).get('weighted_fee_bps')} "
            "funnel="
            f"{flow_incident_digest.get('selected_fresh_source_rows')}/"
            f"{flow_incident_digest.get('selected_eligible_intents')}/"
            f"{flow_incident_digest.get('selected_guard_submit_attempts')}/"
            f"{flow_incident_digest.get('selected_accepted_orders')} "
            f"candidate_supply={flow_incident_digest.get('candidate_supply')} "
            f"loss_exclusions={flow_incident_digest.get('loss_exclusions')} "
            "cross_exchange_actuator="
            f"{flow_incident_digest.get('cross_exchange_actuator_status')}/"
            f"{flow_incident_digest.get('cross_exchange_actuator_terminal_reason')}/"
            f"{flow_incident_digest.get('cross_exchange_last_order_id')} "
            "campaign="
            f"{flow_incident_digest.get('cross_exchange_campaign_orders_submitted')}/"
            f"{flow_incident_digest.get('cross_exchange_campaign_orders_accepted')}/"
            f"{flow_incident_digest.get('cross_exchange_campaign_orders_filled')} "
            f"method_pnl={flow_incident_digest.get('cross_exchange_campaign_method_pnl')} "
            f"activation={cross_exchange_actuator_digest.get('activation_started_at')}->"
            f"{cross_exchange_actuator_digest.get('activation_expires_at')} "
            f"delayed_offset={cross_exchange_delayed_park_digest.get('decision')} "
            "multivenue="
            f"{cross_exchange_multivenue_digest.get('status')}/"
            f"{cross_exchange_multivenue_digest.get('cell_count')}/"
            f"{cross_exchange_multivenue_digest.get('selector_status')}/"
            f"{str(cross_exchange_multivenue_digest.get('generation_checksum') or '')[:12]} "
            "paired_complete_set="
            f"{cross_exchange_complete_set_digest.get('status')}/"
            f"{cross_exchange_complete_set_digest.get('completed_liveness_windows')}/"
            f"{cross_exchange_complete_set_digest.get('positive_edge_intents')}/"
            f"{str(cross_exchange_complete_set_digest.get('generation_checksum') or '')[:12]} "
            "split_sell="
            f"{cross_exchange_split_sell_digest.get('status')}/"
            f"{cross_exchange_split_sell_digest.get('completed_liveness_windows')}/"
            f"{cross_exchange_split_sell_digest.get('positive_intent_cycles')}/"
            f"{cross_exchange_split_sell_digest.get('resolved_cycles')}/"
            f"{str(cross_exchange_split_sell_digest.get('generation_checksum') or '')[:12]} "
            "book_shock="
            f"{cross_exchange_book_shock_digest.get('status')}/"
            f"{cross_exchange_book_shock_digest.get('completed_windows')}/"
            f"{cross_exchange_book_shock_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_book_shock_digest.get('resolved_orders')}/"
            f"{str(cross_exchange_book_shock_digest.get('generation_checksum') or '')[:12]} "
            f"rung_c={cross_exchange_book_shock_digest.get('rung_c_status')} "
            "queue_hazard="
            f"{cross_exchange_queue_hazard_digest.get('status')}/"
            f"{cross_exchange_queue_hazard_digest.get('completed_windows')}/"
            f"{cross_exchange_queue_hazard_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_queue_hazard_digest.get('genuine_queue_fills')}/"
            f"{str(cross_exchange_queue_hazard_digest.get('generation_checksum') or '')[:12]} "
            "native_sweep="
            f"{cross_exchange_native_sweep_digest.get('status')}/"
            f"{cross_exchange_native_sweep_digest.get('completed_windows')}/"
            f"{cross_exchange_native_sweep_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_native_sweep_digest.get('paper_fills')}/"
            f"{str(cross_exchange_native_sweep_digest.get('generation_checksum') or '')[:12]} "
            "native_complement="
            f"{cross_exchange_native_complement_digest.get('status')}/"
            f"{cross_exchange_native_complement_digest.get('completed_windows')}/"
            f"{cross_exchange_native_complement_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_native_complement_digest.get('paper_fills')}/"
            f"{str(cross_exchange_native_complement_digest.get('generation_checksum') or '')[:12]} "
            "cross_asset_leader="
            f"{cross_exchange_cross_asset_digest.get('status')}/"
            f"{cross_exchange_cross_asset_digest.get('completed_windows')}/"
            f"{cross_exchange_cross_asset_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_cross_asset_digest.get('paper_fills')}/"
            f"{str(cross_exchange_cross_asset_digest.get('generation_checksum') or '')[:12]} "
            f"integrity={cross_exchange_cross_asset_digest.get('measured_integrity')} "
            "first_leader_cross_asset="
            f"{cross_exchange_first_leader_digest.get('status')}/"
            f"{cross_exchange_first_leader_digest.get('completed_windows')}/"
            f"{cross_exchange_first_leader_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_first_leader_digest.get('paper_fills')}/"
            f"{str(cross_exchange_first_leader_digest.get('generation_checksum') or '')[:12]} "
            f"integrity={cross_exchange_first_leader_digest.get('measured_integrity')} "
            "signed_tape_stale_ask="
            f"{cross_exchange_signed_tape_digest.get('status')}/"
            f"{cross_exchange_signed_tape_digest.get('completed_windows')}/"
            f"{cross_exchange_signed_tape_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_signed_tape_digest.get('paper_fills')}/"
            f"{str(cross_exchange_signed_tape_digest.get('generation_checksum') or '')[:12]} "
            f"integrity={cross_exchange_signed_tape_digest.get('measured_integrity')} "
            "l2_displacement_stale_ask="
            f"{cross_exchange_l2_displacement_digest.get('status')}/"
            f"{cross_exchange_l2_displacement_digest.get('completed_windows')}/"
            f"{cross_exchange_l2_displacement_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_l2_displacement_digest.get('paper_fills')}/"
            f"{str(cross_exchange_l2_displacement_digest.get('generation_checksum') or '')[:12]} "
            f"integrity={cross_exchange_l2_displacement_digest.get('measured_integrity')} "
            "l2_tob_pressure="
            f"{cross_exchange_tob_pressure_digest.get('status')}/"
            f"{cross_exchange_tob_pressure_digest.get('completed_windows')}/"
            f"{cross_exchange_tob_pressure_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_tob_pressure_digest.get('paper_fills')}/"
            f"{str(cross_exchange_tob_pressure_digest.get('generation_checksum') or '')[:12]} "
            f"integrity={cross_exchange_tob_pressure_digest.get('measured_integrity')} "
            "l2_cross_parity="
            f"{cross_exchange_cross_parity_digest.get('status')}/"
            f"{cross_exchange_cross_parity_digest.get('completed_windows')}/"
            f"{cross_exchange_cross_parity_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_cross_parity_digest.get('paper_fills')}/"
            f"{str(cross_exchange_cross_parity_digest.get('generation_checksum') or '')[:12]} "
            f"integrity={cross_exchange_cross_parity_digest.get('measured_integrity')} "
            "l2_depth_weighted_cross_parity="
            f"{cross_exchange_depth_weighted_cross_parity_digest.get('status')}/"
            f"{cross_exchange_depth_weighted_cross_parity_digest.get('completed_windows')}/"
            f"{cross_exchange_depth_weighted_cross_parity_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_depth_weighted_cross_parity_digest.get('paper_fills')}/"
            f"{str(cross_exchange_depth_weighted_cross_parity_digest.get('generation_checksum') or '')[:12]} "
            f"integrity={cross_exchange_depth_weighted_cross_parity_digest.get('measured_integrity')} "
            "l2_bid_support_cross_parity="
            f"{cross_exchange_bid_support_cross_parity_digest.get('status')}/"
            f"{cross_exchange_bid_support_cross_parity_digest.get('completed_windows')}/"
            f"{cross_exchange_bid_support_cross_parity_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_bid_support_cross_parity_digest.get('paper_fills')}/"
            f"{str(cross_exchange_bid_support_cross_parity_digest.get('generation_checksum') or '')[:12]} "
            f"integrity={cross_exchange_bid_support_cross_parity_digest.get('measured_integrity')} "
            "l2_ask_cap_cross_parity="
            f"{cross_exchange_ask_cap_cross_parity_digest.get('status')}/"
            f"{cross_exchange_ask_cap_cross_parity_digest.get('completed_windows')}/"
            f"{cross_exchange_ask_cap_cross_parity_digest.get('positive_edge_intents')}/"
            f"{cross_exchange_ask_cap_cross_parity_digest.get('paper_fills')}/"
            f"{str(cross_exchange_ask_cap_cross_parity_digest.get('generation_checksum') or '')[:12]} "
            f"integrity={cross_exchange_ask_cap_cross_parity_digest.get('measured_integrity')} "
            "f1_f4_fallout="
            f"{cross_exchange_f1_f4_digest.get('status')}/"
            f"{cross_exchange_f1_f4_digest.get('candidate_count')}/"
            f"{cross_exchange_f1_f4_digest.get('eligible_count')}/"
            f"join_defect={cross_exchange_f1_f4_digest.get('active_temporal_join_defect_found')} "
            "flow_episodes: "
            f"episodes_today={flow_episode_digest.get('episodes_today')}"
            f"(+{flow_episode_scope.get('prior_incident_rows_at_arming') or 0}_prior_unrecorded) "
            f"armed={flow_episode_scope.get('first_armed_at')} "
            f"total_dead_s={flow_episode_digest.get('total_dead_s')} "
            f"natural_clears={flow_episode_digest.get('natural_clears')} "
            f"restarts_performed={flow_episode_digest.get('restarts_performed')} "
            f"unknown_restart_clears={flow_episode_digest.get('unknown_restart_clears')} "
            f"escalations_proposed={flow_episode_digest.get('escalations_proposed')} "
            f"rows={flow_episode_digest.get('row_count')} "
            f"first_armed={flow_episode_scope.get('first_armed_at')} "
            f"prior_unrecorded={flow_episode_scope.get('prior_episodes_unrecorded')} "
            f"source={flow_episode_digest.get('source')} "
            f"{'STALE_ACROSS_TRANSITION ' if deadman_digest.get('stale_across_transition') else ''}"
            "rule=RUNNING_PID_NEVER_IMPLIES_FLOW_PASS | deadman: "
            f"status={deadman_digest.get('status')} can_trade={deadman_digest.get('can_trade')} "
            f"mechanical_escalation={deadman_digest.get('mechanical_escalation')} "
            f"deadman_checked_at={deadman_digest.get('checked_at')} "
            f"digest_lag_s={deadman_digest.get('digest_lag_s')} "
            f"digest_lag_budget_s={deadman_digest.get('digest_lag_budget_s')} "
            f"digest_lag_status={deadman_digest.get('digest_lag_status')} "
            f"can_trade_reason={deadman_digest.get('can_trade_reason')} "
            f"class={deadman_digest.get('deadman_class')} "
            f"idle_s={deadman_digest.get('idle_s')} "
            f"effective_idle_s={deadman_digest.get('effective_deadman_idle_s')} "
            f"host_downtime={deadman_digest.get('host_downtime_attribution')} "
            f"liveness_source={deadman_digest.get('liveness_source')} "
            f"liveness_ts={deadman_digest.get('liveness_ts')} "
            f"latest_order={deadman_digest.get('latest_order_ts')} "
            f"latest_approved_suppression={deadman_digest.get('latest_approved_suppression_ts')} "
            f"eligible_drought_s={deadman_digest.get('eligible_drought_s')} "
            f"eligible_status={deadman_digest.get('eligible_drought_status')} "
            f"fresh_stale_signal_rows={deadman_digest.get('fresh_stale_signal_rows')} "
            f"warning={deadman_digest.get('deadman_warning')} "
            f"benign_skip_overrode={deadman_digest.get('benign_skip_overrode')} "
            f"approved_suppression_events={deadman_digest.get('approved_suppression_events')} "
            f"tags={deadman_digest.get('approved_suppression_tags')} "
            f"deadman_cycle={deadman_digest.get('deadman_cycle_duration_health')} "
            f"early_admission={deadman_digest.get('early_admission_authority')} "
            f"admission_publish={deadman_digest.get('admission_publish_health')} "
            f"member_signal_age={member_age_summary} "
            "guard_memory="
            f"status:{deadman_guard_memory.get('status')},"
            f"rss_gib:{deadman_guard_memory.get('rss_gib')},"
            f"rss_kib:{deadman_guard_memory.get('rss_kib')},"
            f"raw_ps_line:{deadman_guard_memory.get('raw_ps_line')},"
            f"verified_pid:{deadman_guard_memory.get('verified_pid')},"
            f"pid:{deadman_guard_memory.get('pid')},"
            f"rss_peak_gib:{deadman_guard_memory.get('rss_peak_gib')},"
            f"rss_peak_at:{deadman_guard_memory.get('rss_peak_at')},"
            f"rss_observation:{deadman_guard_memory.get('rss_observation')},"
            f"warn_restart:{deadman_guard_memory.get('warn_gib')}/"
            f"{deadman_guard_memory.get('restart_gib')},"
            f"auto_restart:{deadman_guard_memory.get('auto_restart_status')},"
            f"source:{deadman_guard_memory.get('source')},"
            f"cycle_health:{deadman_guard_memory.get('cycle_duration_health')},"
            f"samples_history:{deadman_guard_memory.get('sample_count')} "
            "window_participation_merge_profile="
            f"{digest.get('window_participation_merge_profile')} "
            "active_member_orderfilled_hot_source_shadow: "
            f"status={active_member_orderfilled_digest.get('status')} "
            f"corrected_gate={active_member_orderfilled_digest.get('unique_resolved_source_events')}/"
            f"{active_member_orderfilled_digest.get('required_unique_resolved_source_events')} "
            f"current_next={active_member_orderfilled_digest.get('current_or_next_window_events')} "
            f"mapping_missing={active_member_orderfilled_digest.get('token_mapping_missing')} "
            f"parity={active_member_orderfilled_digest.get('identity_market_outcome_parity_violations')} "
            f"gate={active_member_orderfilled_digest.get('live_source_wiring_gate_passed')} "
            f"rss_gib={(active_member_orderfilled_digest.get('resource_usage') or {}).get('max_rss_gib')} "
            f"incremental_rows={(active_member_orderfilled_digest.get('incremental_reader') or {}).get('accumulator_rows')} "
            f"pid={active_member_orderfilled_digest.get('pid')} "
            "early_01a_decision_time_book: "
            f"status={early_01a_book_digest.get('status')} "
            f"lane={early_01a_book_digest.get('lane_decision_status')} "
            f"signals={early_01a_book_digest.get('signal_count')} "
            f"windows={early_01a_book_digest.get('distinct_windows')} "
            f"book_lag_s={early_01a_book_digest.get('book_lag_s')} "
            f"book_fail={early_01a_book_digest.get('book_status_fail_count')} "
            f"executable={early_01a_book_digest.get('executable_count')} "
            f"paper_only={early_01a_book_digest.get('paper_only')} "
            "qualified_pool_orderfilled_stakeout: "
            f"status={qualified_pool_stakeout_digest.get('status')} "
            f"generated_at={qualified_pool_stakeout_digest.get('generated_at')} "
            f"pid={qualified_pool_stakeout_digest.get('pid')} "
            f"roster={(qualified_pool_stakeout_digest.get('qualified_pool_roster') or {}).get('wallet_count')} "
            f"f2={(qualified_pool_stakeout_digest.get('prospective_current_market') or {}).get('genuine_buy_identities')} "
            f"gate={((qualified_pool_stakeout_digest.get('prospective_current_market') or {}).get('actuator_consumption_gate') or {}).get('passed')} "
            f"parity={qualified_pool_stakeout_digest.get('identity_market_outcome_parity_violations')} "
            f"paper={qualified_pool_stakeout_digest.get('paper_only')}/"
            f"{qualified_pool_stakeout_digest.get('live_orders_allowed')} "
            "source_identity_router: "
            f"frozen={source_identity_frozen.get('input_rows')}/"
            f"{source_identity_frozen.get('terminal_rows')} "
            f"frozen_counts={source_identity_frozen.get('terminal_counts')} "
            f"frozen_actionable={source_identity_frozen.get('dominant_actionable_stage')} "
            f"current={source_identity_current.get('input_rows')}/"
            f"{source_identity_current.get('terminal_rows')} "
            f"current_counts={source_identity_current.get('terminal_counts')} "
            f"parity={source_identity_frozen.get('identity_market_outcome_parity_violations')}/"
            f"{source_identity_current.get('identity_market_outcome_parity_violations')} "
            f"duplicate_routes={source_identity_frozen.get('duplicate_routes')}/"
            f"{source_identity_current.get('duplicate_routes')} "
            f"replay_deduped={source_identity_frozen.get('source_identity_replays_deduped')}/"
            f"{source_identity_current.get('source_identity_replays_deduped')} "
            f"gates={source_identity_gates} "
            "wake_activation: "
            f"active={wake_activation_digest.get('activated')} "
            f"proof={wake_activation_digest.get('paper_survivor_identity')} "
            f"proof_submits={wake_activation_digest.get('paper_survivor_orders_submitted')} "
            f"proof_live_allowed={wake_activation_digest.get('live_orders_allowed')} "
            f"resident={wake_resident_guard.get('pid')}/"
            f"{wake_resident_guard.get('git_head_at_launch')}/"
            f"{wake_resident_guard.get('script_sha256')} "
            f"generation={wake_resident_guard.get('live_guard_generation_sha256')} "
            f"forced_sweep={wake_forced_sweep.get('status')} "
            f"refusals={wake_forced_sweep.get('refusal_counts')} "
            "orderfilled_fast_lane: "
            f"status={orderfilled_fast_lane_digest.get('status')} "
            f"pid={orderfilled_fast_lane_digest.get('pid')} "
            f"wake={orderfilled_fast_lane_digest.get('wake_source')} "
            f"samples={orderfilled_fast_lane_digest.get('receipt_to_guard_sample_count')} "
            f"p95_s={orderfilled_fast_lane_digest.get('receipt_to_guard_p95_s')} "
            f"source={orderfilled_fast_lane_source_digest.get('status')}/"
            f"{orderfilled_fast_lane_source_digest.get('eligible_rows')} "
            f"bridge={orderfilled_fast_lane_bridge_digest.get('status')}/"
            f"{orderfilled_fast_lane_bridge_digest.get('submit_stage_invocations')} "
            f"sole={orderfilled_fast_lane_digest.get('sole_submitter_process')} | "
            "flow_episodes: "
            f"episodes_today={digest.get('flow_episodes', {}).get('episodes_today')}"
            f"(+{digest.get('flow_episodes', {}).get('ledger_scope', {}).get('prior_incident_rows_at_arming') or 0}"
            "_prior_unrecorded) "
            f"armed={digest.get('flow_episodes', {}).get('ledger_scope', {}).get('first_armed_at')} "
            f"total_dead_s={digest.get('flow_episodes', {}).get('total_dead_s')} "
            f"open_episode_fire_at={digest.get('flow_episodes', {}).get('open_episode_fire_at')} "
            f"open_episode_idle_s={digest.get('flow_episodes', {}).get('open_episode_idle_s')} "
            f"open_dead_s={digest.get('flow_episodes', {}).get('open_dead_s')} "
            f"natural_clears={digest.get('flow_episodes', {}).get('natural_clears')} "
            f"restarts_performed={digest.get('flow_episodes', {}).get('restarts_performed')} "
            f"escalations_proposed={digest.get('flow_episodes', {}).get('escalations_proposed')} "
            f"first_armed_at={digest.get('flow_episodes', {}).get('ledger_scope', {}).get('first_armed_at')} "
            f"prior_episodes_unrecorded="
            f"{digest.get('flow_episodes', {}).get('ledger_scope', {}).get('prior_episodes_unrecorded')}"
        ),
        (
            "policy_choke: "
            f"status={policy_choke_digest.get('status')} "
            f"deadman_surface_selected={_short_wallet(str(policy_choke_digest.get('selected_wallet') or ''))} "
            f"fresh={policy_choke_digest.get('selected_fresh_source_rows')}/"
            f"{policy_choke_digest.get('whole_runtime_fresh_source_rows')} "
            f"eligible={policy_choke_digest.get('selected_eligible_intents')}/"
            f"{policy_choke_digest.get('whole_runtime_eligible_intents')} "
            f"accepted={policy_choke_digest.get('selected_accepted_orders')}/"
            f"{policy_choke_digest.get('whole_runtime_accepted_orders')} "
            f"method_accepted={policy_choke_digest.get('method_accepted_orders')} "
            f"wallet_diagnostic={policy_choke_digest.get('wallet_policy_diagnostic')} "
            f"taxonomy={policy_choke_digest.get('selected_suppression_taxonomy')} "
            f"escalation={policy_choke_digest.get('mechanical_escalation')} "
            f"target={_short_wallet(str((policy_choke_digest.get('rung_a_seat_read') or {}).get('target_wallet') or ''))} "
            f"rung_a_recon={(policy_choke_digest.get('rung_a_candidate_reconciliation') or {}).get('status')}:"
            f"excluded={(policy_choke_digest.get('rung_a_candidate_reconciliation') or {}).get('positive_liveness_only_excluded_wallets')} "
            f"actuator={(policy_choke_digest.get('actuator') or {}).get('status')} "
            f"rung_c_refusals={(policy_choke_digest.get('actuator') or {}).get('refusal_counts')} "
            f"walk_forward_frontier={deadman_digest.get('walk_forward_refusal_frontier')} "
            f"rung_b={rung_b_lifecycle_digest.get('status')}/cooloffs:{len(deadman_digest.get('policy_choke_rung_b_cooloffs') or {})}/"
            f"drill:{choke_drill_digest.get('verdict')}/dry:{(choke_drill_digest.get('rung_b_dry_run') or {}).get('status')}/"
            f"ttl:{(choke_drill_digest.get('rung_b_dry_run') or {}).get('ttl_s')} "
            "fee_receipts="
            f"{realized_fee_summary.get('pass')}/{realized_fee_summary.get('reconciled')}:"
            f"realized{realized_fee_summary.get('realized_fee_usd')}:"
            f"expected{realized_fee_summary.get('expected_fee_usd')}:"
            f"abs_delta{realized_fee_summary.get('absolute_delta_usd')}:"
            f"ledger_rewrite{realized_fee_digest.get('ledger_rewrite')} "
            "frozen_fingerprint_f2_prewarm="
            f"generated_at={digest.get('frozen_fingerprint_f2_prewarm', {}).get('generated_at')} "
            f"paper_only={digest.get('frozen_fingerprint_f2_prewarm', {}).get('paper_only')} "
            f"primary={_short_wallet(str((digest.get('frozen_fingerprint_f2_prewarm', {}).get('primary') or {}).get('wallet') or ''))}/"
            f"{str((digest.get('frozen_fingerprint_f2_prewarm', {}).get('primary') or {}).get('wide_policy_fingerprint') or '')[:4]} "
            f"resolved={(digest.get('frozen_fingerprint_f2_prewarm', {}).get('primary') or {}).get('resolved')}/"
            f"{(digest.get('frozen_fingerprint_f2_prewarm', {}).get('primary') or {}).get('resolved_target')} "
            f"f2={(digest.get('frozen_fingerprint_f2_prewarm', {}).get('primary') or {}).get('fresh_own_source_buy_rows_30m')}/"
            f"{(digest.get('frozen_fingerprint_f2_prewarm', {}).get('primary') or {}).get('f2_minimum')} "
            f"prewarmed={(digest.get('frozen_fingerprint_f2_prewarm', {}).get('primary') or {}).get('f2_prewarmed')} "
            f"backup_count={digest.get('frozen_fingerprint_f2_prewarm', {}).get('backup_count')} "
            f"top_backup={[(row.get('wallet'), str(row.get('wide_policy_fingerprint') or '')[:4], row.get('resolved'), row.get('deficits')) for row in (digest.get('frozen_fingerprint_f2_prewarm', {}).get('backup_rank') or [])[:3]]} "
            "freeze_resolution_accelerator="
            f"{digest.get('freeze_resolution_accelerator', {}).get('status')}/"
            f"pid:{digest.get('freeze_resolution_accelerator', {}).get('pid')}/"
            f"interval:{digest.get('freeze_resolution_accelerator', {}).get('interval_s')}/"
            f"direction:{digest.get('freeze_resolution_accelerator', {}).get('direction_id')}/"
            "direct_priority:"
            f"{[(_short_wallet(str(row.get('wallet') or '')), str(row.get('wide_policy_fingerprint') or '')[:4]) for row in (digest.get('freeze_resolution_accelerator', {}).get('direct_climb_priority') or []) if isinstance(row, dict)]}/"
            "direct_unresolved:"
            f"{digest.get('freeze_resolution_accelerator', {}).get('direct_climb_priority_unresolved_window_count')}/"
            f"priority:{digest.get('freeze_resolution_accelerator', {}).get('priority_unresolved_window_count')}/"
            f"delta:{digest.get('freeze_resolution_accelerator', {}).get('canonical_resolution_row_delta')} "
            "wide_direct_frontier="
            f"generated_at:{digest.get('wide_direct_admissible_frontier', {}).get('generated_at')}/"
            f"eligible:{digest.get('wide_direct_admissible_frontier', {}).get('eligible_count')}/"
            f"candidates:{digest.get('wide_direct_admissible_frontier', {}).get('candidate_count')}/"
            "park_provenance:"
            f"{(digest.get('live_flow_incident_foreground', {}).get('candidate_supply') or {}).get('park_provenance')}/"
            "nearest:"
            f"{[(_short_wallet(str(row.get('wallet') or '')), str(row.get('wide_policy_fingerprint') or '')[:4], row.get('evidence_deficits'), (row.get('regime_evidence') or {}).get('pnl_usd'), (row.get('regime_evidence') or {}).get('pnl_excluding_top_1_market'), (row.get('regime_evidence') or {}).get('top_1_market_share_pct')) for row in (digest.get('wide_direct_admissible_frontier', {}).get('nearest_frontier') or [])[:10] if isinstance(row, dict)]}/"
            "freeze_primary:"
            f"{[(_short_wallet(str(row.get('wallet') or '')), str(row.get('wide_policy_fingerprint') or '')[:4], (row.get('direct_source') or {}).get('attempts'), (row.get('direct_source') or {}).get('copyable'), row.get('evidence_deficits')) for row in (digest.get('wide_direct_admissible_frontier', {}).get('nearest_frontier') or []) if isinstance(row, dict) and str(row.get('wallet') or '') == str((digest.get('frozen_fingerprint_f2_prewarm', {}).get('primary') or {}).get('wallet') or '') and str(row.get('wide_policy_fingerprint') or '') == str((digest.get('frozen_fingerprint_f2_prewarm', {}).get('primary') or {}).get('wide_policy_fingerprint') or '')][:1]} "
            "all_pass_path:"
            f"eligible={digest.get('wide_all_pass_seat_path', {}).get('eligible_count')}/"
            f"top={_short_wallet(str((digest.get('wide_all_pass_seat_path', {}).get('top_row') or {}).get('wallet') or ''))}/"
            f"top_deficit={(digest.get('wide_all_pass_seat_path', {}).get('top_row') or {}).get('binding_deficit')}/"
            f"diagnosis={(digest.get('wide_all_pass_seat_path', {}).get('walk_forward_diagnosis') or {}).get('classification')}/"
            f"focus={_short_wallet(str((digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('wallet') or ''))}/"
            f"focus_after={(digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('after')}/"
            f"focus_velocity={(digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('residual_velocity')}/"
            f"focus_temporal={(digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('active_temporal')}/"
            f"focus_paper={((digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('exact_policy_paper') or {}).get('status')}:"
            f"{((digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('exact_policy_paper') or {}).get('attempted_exact_policy_buys')}/"
            f"{((digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('exact_policy_paper') or {}).get('copyable_exact_policy_buys')}/"
            f"{((digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('exact_policy_paper') or {}).get('resolved_orders')}/"
            f"focus_walk={((digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('walk_forward_diagnosis') or {}).get('classification')} "
            f"focus_generations={((digest.get('wide_all_pass_seat_path', {}).get('sole_accrual_focus') or {}).get('exact_policy_paper') or {}).get('generation_history')} "
            "ee3f_venue="
            f"{digest.get('wide_ee3f_venue_reachable_diagnosis', {}).get('diagnosis')}/"
            f"share:{((digest.get('wide_ee3f_venue_reachable_diagnosis', {}).get('sample_stability') or {}).get('rescore_history') or [{}])[-1].get('venue_reachable_share_pct')}/"
            f"zero_residual:{(digest.get('wide_ee3f_venue_reachable_diagnosis', {}).get('residual_zero_projection') or {}).get('projected_share_pct')}/"
            f"trend:{(digest.get('wide_ee3f_venue_reachable_diagnosis', {}).get('sample_stability') or {}).get('trend')} "
            "focus_concentration="
            f"{digest.get('wide_951b_concentration_diagnosis', {}).get('diagnosis')}/"
            f"projection:{((digest.get('wide_951b_concentration_diagnosis', {}).get('focus') or {}).get('residual_zero_clearance_projection') or {}).get('plausibly_clears_at_residual_zero')} "
            "order7="
            f"{digest.get('wide_order7a_alpha_causal_reanchor', {}).get('verdict')}/"
            f"recenter:{(digest.get('wide_order7a_alpha_causal_reanchor', {}).get('control_chart') or {}).get('recenter_applied')}/"
            f"metadata:{digest.get('wide_order7b_metadata_diagnosis', {}).get('diagnosis')}/"
            f"unknown_shrink:{(digest.get('wide_order7b_metadata_diagnosis', {}).get('unknown_shrink') or {}).get('rows')}/"
            f"order127:{digest.get('order127_measured_seat_distance', {}).get('verdict')}:"
            f"eligible={digest.get('order127_measured_seat_distance', {}).get('eligible_count')}:"
            f"distance_wallets={[(_short_wallet(str(row.get('wallet') or '')), row.get('evidence_deficits')) for row in (digest.get('order127_measured_seat_distance', {}).get('rows') or [])[:3] if isinstance(row, dict)]}/"
            f"order128:{digest.get('order128_fastest_lawful_path', {}).get('verdict')}:"
            f"bind={_short_wallet(str((digest.get('order128_fastest_lawful_path', {}).get('binding_action') or {}).get('wallet') or ''))}/"
            f"manifest_auth={digest.get('order128_manifest_binding', {}).get('order128_sticky_focus_authorized')}@"
            f"{digest.get('order128_manifest_binding', {}).get('manifest_score_run_id')}:"
            f"cuts_agree={digest.get('order128_manifest_binding', {}).get('cuts_agree')}:"
            f"paper={digest.get('order128_manifest_binding', {}).get('paper_only')}:"
            f"admitted={digest.get('order128_manifest_binding', {}).get('admitted_wallets')}/"
            f"retarget:{digest.get('wide_order6_gen2_retarget', {}).get('decision')}:"
            f"{str(digest.get('wide_order6_gen2_retarget', {}).get('to_fingerprint') or '')[:8]} "
            "wallet_terminal_breakdown="
            f"{_short_wallet(str(digest.get('wide_wallet_terminal_breakdown', {}).get('wallet') or ''))}/"
            f"{digest.get('wide_wallet_terminal_breakdown', {}).get('measurement_cut_at')}/"
            f"{digest.get('wide_wallet_terminal_breakdown', {}).get('attempts')}/"
            f"{digest.get('wide_wallet_terminal_breakdown', {}).get('terminal_taxonomy')}/"
            f"metadata_predominant:{digest.get('wide_wallet_terminal_breakdown', {}).get('metadata_missing_predominant')} "
            "resolved_tape_gap="
            f"{digest.get('resolved_tape_gap_closure', {}).get('decision')}/"
            f"age_h:{digest.get('resolved_tape_gap_closure', {}).get('resolved_latest_event_age_h')}/"
            f"recent:{digest.get('resolved_tape_gap_closure', {}).get('resolved_recent_trades')}/"
            f"windows:{digest.get('resolved_tape_gap_closure', {}).get('resolved_recent_unique_windows')}/"
            f"pnl:{digest.get('resolved_tape_gap_closure', {}).get('resolved_recent_pnl_usd')} "
            "heartbeat_watch="
            f"wakes:{digest.get('wide_heartbeat_watch', {}).get('liveness_wake_watches')}/"
            f"triggered:{digest.get('wide_heartbeat_watch', {}).get('triggered_wake_wallets')}/"
            f"dh_inventory:{digest.get('wide_heartbeat_watch', {}).get('dual_half_positive_inventory')}/"
            f"fingerprint_selection:{digest.get('wide_heartbeat_watch', {}).get('fingerprint_evidence_selection')}/"
            f"climb:{digest.get('wide_heartbeat_watch', {}).get('fresh_forward_climb')}/"
            f"guard_rss:{digest.get('wide_heartbeat_watch', {}).get('guard_rss_watch')}"
        ),
        (
            "regime_seat: "
            f"generated_at={regime_seat_digest.get('generated_at')} "
            f"regime={regime_seat_digest.get('regime')} "
            f"selected={_short_wallet(str(regime_seat_digest.get('current_selected_wallet') or ''))} "
            f"winner={_short_wallet(str(regime_seat_digest.get('winner_wallet') or ''))} "
            f"leader={_short_wallet(str(regime_seat_digest.get('regime_evidence_leader_wallet') or ''))} "
            f"eligible={regime_seat_digest.get('eligible_count')} "
            f"accept30_action={(regime_seat_digest.get('acceptance_share_30m') or {}).get('action')} "
            f"accept30_target={_short_wallet(str((regime_seat_digest.get('acceptance_share_30m') or {}).get('target_wallet') or ''))} "
            f"accept90_action={(regime_seat_digest.get('acceptance_share_90m') or {}).get('action')} "
            "coacceptance="
            f"{coacceptance_digest.get('status')}:"
            f"flow={coacceptance_digest.get('order_flow_status')}/"
            f"{coacceptance_digest.get('policy_choke_status')}:"
            f"seat={coacceptance_digest.get('seat_action')}:"
            f"rows={[(row.get('name'), row.get('own_source_rows_30m'), row.get('policy_eligible_intents_30m'), row.get('accepted_live_orders_30m'), (row.get('probe_funnel') or {}).get('dominant_observed_gate')) for row in coacceptance_digest.get('rows') or [] if isinstance(row, dict)]}:"
            f"live_mutation={coacceptance_digest.get('live_mutation')} "
            "probe_fill_shadows="
            f"{probe_fill_quality_digest.get('status')}:"
            f"micro={((probe_fill_quality_digest.get('weekday_micro_loss') or {}).get('n'))}/"
            f"{((probe_fill_quality_digest.get('weekday_micro_loss') or {}).get('post_fee_ev_usd'))}:"
            f"near={((probe_fill_quality_digest.get('window_time_near_miss') or {}).get('n'))}/"
            f"{((probe_fill_quality_digest.get('window_time_near_miss') or {}).get('post_fee_ev_usd'))}:"
            f"early={(((probe_fill_quality_digest.get('early_utc_probe_fill_quality') or {}).get('early_utc') or {}).get('n'))}/"
            f"{(((probe_fill_quality_digest.get('early_utc_probe_fill_quality') or {}).get('early_utc') or {}).get('post_fee_ev_usd'))}:"
            f"decision={probe_fill_quality_digest.get('decision')}:"
            f"live_mutation={probe_fill_quality_digest.get('live_mutation')}"
        ),
        *(
            [
                (
                    "deadman_r1_attribution: "
                    f"status={deadman_r1_digest.get('status')} "
                    f"top={deadman_r1_digest.get('deadman_top_status')} "
                    f"class={deadman_r1_digest.get('gated_quiet_classification')} "
                    f"selected={_short_wallet(str(deadman_r1_digest.get('selected_wallet') or ''))} "
                    f"selected_status={deadman_r1_digest.get('selected_status')} "
                    f"failed={deadman_r1_digest.get('selected_failed_checks')} "
                    f"fallthrough={deadman_r1_digest.get('fallthrough_admissible_targets')} "
                    f"fresh={deadman_r1_digest.get('fresh_buy_rows_le_10s')}/"
                    f"{deadman_r1_digest.get('policy_compatible_fresh_buy_rows_le_30s')} "
                    f"policy_rejects={deadman_r1_digest.get('policy_reject_counts_fresh_buy_le_30s')} "
                    f"repair={deadman_r1_digest.get('repair_status')} "
                    f"allowlist={deadman_r1_digest.get('allowlist_entry')} "
                    f"midnight_restart={deadman_r1_digest.get('midnight_restart_mandatory')} "
                    f"expiry={deadman_r1_digest.get('expires_at')} "
                    f"latest_order={deadman_r1_digest.get('latest_live_order_ts')} "
                    f"path={deadman_r1_digest.get('path')}"
                )
            ]
            if deadman_r1_digest
            else []
        ),
        *(
            [
                (
                    "trade_executor_lane_attribution: "
                    f"status={trade_lane_digest.get('status')} "
                    f"lines={trade_lane_digest.get('executing_trade_lines_203202z')} "
                    f"size_gt_1={trade_lane_digest.get('size_usd_gt_1_lines')} "
                    f"unit_test={trade_lane_digest.get('unit_test_signature_lines')} "
                    f"ledger_2032={trade_lane_digest.get('live_ledger_orders_at_2032')} "
                    f"events_2032={trade_lane_digest.get('live_execution_event_rows_at_2032')} "
                    f"bypass={trade_lane_digest.get('ledger_bypassing_submissions')} "
                    f"single_guard={trade_lane_digest.get('only_live_guard_process_seen')}/"
                    f"{trade_lane_digest.get('single_live_guard_pid')} "
                    f"fix={trade_lane_digest.get('logging_fix')} "
                    f"path={trade_lane_digest.get('path')}"
                )
            ]
            if trade_lane_digest
            else []
        ),
        (
            "own_positions: "
            f"status={own_positions_digest.get('status')} "
            f"wallet={own_positions_digest.get('wallet')} "
            f"primary_locked={own_positions_digest.get('redeemable_locked_usd')} "
            f"source={own_positions_digest.get('locked_value_source')} "
            f"rows={own_positions_digest.get('positions_rows')} "
            f"redeemable_rows={own_positions_digest.get('data_api_redeemable_rows')} "
            f"ledger_estimate_only={own_positions_digest.get('ledger_estimated_redeemable_locked_usd')} "
            f"data_api={own_positions_digest.get('data_api_status')}/"
            f"{own_positions_digest.get('data_api_reason')} "
            f"deadman={own_positions_digest.get('deadman_status')} "
            f"ack={own_positions_digest.get('deadman_acknowledged_reason')} "
            f"ack_until={own_positions_digest.get('deadman_acknowledged_until')} "
            f"age_s={own_positions_digest.get('deadman_age_s')} "
            f"incident={own_positions_digest.get('deadman_incident')}"
        ),
        (
            "own_redeemer: "
            f"status={own_redeemer_digest.get('status')} "
            f"candidates={own_redeemer_digest.get('candidate_count')} "
            f"source={own_redeemer_digest.get('candidate_source')} "
            f"executed={own_redeemer_digest.get('executed')} "
            f"skipped={own_redeemer_digest.get('skipped')} "
            f"next_retry={own_redeemer_digest.get('next_retry_at')} "
            f"tx={own_redeemer_digest.get('transaction_hash')} "
            f"recon={own_redeemer_digest.get('post_redeem_recon_status')} "
            "wallet_outflow_deadman: "
            f"status={wallet_outflow_digest.get('status')} "
            f"incident={wallet_outflow_digest.get('incident')} "
            f"degraded={wallet_outflow_digest.get('consecutive_degraded_fetches')} "
            f"last_ok={wallet_outflow_digest.get('last_ok_checked_at')} "
            f"gap_exceeded={wallet_outflow_digest.get('fetch_gap_exceeded')} "
            f"outflows={wallet_outflow_digest.get('outflow_rows')} "
            f"matched_order={wallet_outflow_digest.get('matched_order_outflows')} "
            f"matched_redeem={wallet_outflow_digest.get('matched_redemption_outflows')} "
            f"unmatched={wallet_outflow_digest.get('unmatched_outflows')} "
            f"incident_rows={wallet_outflow_digest.get('incident_outflows')} "
            f"usd={wallet_outflow_digest.get('unmatched_outflow_usd')} "
            f"next={wallet_outflow_digest.get('next_action')}"
        ),
        (
            "alpha_decay_curve: "
            f"status={alpha_decay_curve_digest.get('status')} "
            f"updated={alpha_decay_curve_digest.get('updated_at')} "
            f"horizons={alpha_decay_curve_digest.get('horizons_s')} "
            f"fills={alpha_decay_curve_digest.get('fills_with_any_book_coverage')}/"
            f"{alpha_decay_curve_digest.get('fills_total')} "
            f"overlap_assets={alpha_decay_curve_digest.get('overlapping_fill_book_assets')} "
            f"mean_edges="
            f"1s:{alpha_decay_curve_digest.get('edge_mean_1s')},"
            f"2s:{alpha_decay_curve_digest.get('edge_mean_2s')},"
            f"5s:{alpha_decay_curve_digest.get('edge_mean_5s')},"
            f"10s:{alpha_decay_curve_digest.get('edge_mean_10s')},"
            f"30s:{alpha_decay_curve_digest.get('edge_mean_30s')},"
            f"60s:{alpha_decay_curve_digest.get('edge_mean_60s')},"
            f"120s:{alpha_decay_curve_digest.get('edge_mean_120s')} "
            f"rule5s={alpha_decay_five_s.get('verdict')} "
            f"n={alpha_decay_five_s.get('n')} "
            f"threshold={alpha_decay_five_s.get('threshold_mean_edge')}"
        ),
        *(
            [
                "alpha_overlap_capture: "
                f"status={alpha_overlap_capture_digest.get('status')} "
                f"pid={alpha_overlap_capture_digest.get('pid')} "
                f"run={alpha_overlap_capture_digest.get('run_suffix')} "
                f"rows={alpha_overlap_capture_digest.get('polygon_rows')}/"
                f"{alpha_overlap_capture_digest.get('clob_rows')} "
                f"report={alpha_overlap_capture_digest.get('report_exists')} "
                f"state={alpha_overlap_capture_digest.get('state_exists')} "
                f"wallets={alpha_overlap_capture_digest.get('wallet_count')} "
                f"paper_only={alpha_overlap_capture_digest.get('paper_only')} "
                f"live_allowed={alpha_overlap_capture_digest.get('live_orders_allowed')}"
            ]
            if alpha_overlap_capture_digest.get("run_suffix")
            else []
        ),
        (
            "research_disk_deadman: "
            f"status={research_disk_digest.get('status')} "
            f"incident={research_disk_digest.get('incident')} "
            f"free_gib={research_disk_digest.get('free_gib')} "
            f"used_pct={research_disk_digest.get('used_pct')} "
            f"inventory={research_disk_digest.get('inventory_count')} "
            f"large_files={research_disk_digest.get('large_file_count')} "
            f"uninventoried={research_disk_digest.get('uninventoried_count')} "
            f"memory_swap={research_disk_digest.get('memory_swap_status')} "
            f"swapfiles={research_disk_digest.get('swapfile_count')} "
            f"mem_free_pct={research_disk_digest.get('memory_pressure_free_pct')} "
            f"top_uninventoried={research_disk_digest.get('largest_uninventoried')} "
            "guard_log_rotation="
            f"status:{guard_event_log_rotation_digest.get('status')},"
            f"action:{guard_event_log_rotation_digest.get('action')},"
            f"before:{guard_event_log_rotation_digest.get('size_before_bytes')},"
            f"after:{guard_event_log_rotation_digest.get('size_after_bytes')},"
            f"archive:{guard_event_log_rotation_digest.get('archive_path')},"
            f"aligned:{guard_event_log_rotation_digest.get('tail_line_aligned')} "
            "repo_gc="
            f"status:{repo_storage_hygiene_digest.get('status')},"
            f"pack_mib:{repo_storage_after.get('pack_size_mib')},"
            f"packed:{repo_storage_after.get('packed_object_count')},"
            f"loose:{repo_storage_after.get('loose_object_count')},"
            f"garbage:{repo_storage_after.get('garbage_count')},"
            f"tmp:{repo_storage_after.get('orphan_tmp_obj_files')} "
            f"next={research_disk_digest.get('next_action')}"
        ),
        (
            "pnl: "
            f"day={digest['pnl']['day_pnl_usd']} fills={digest['pnl']['day_resolved_fills']} "
            f"payout_fills={digest['pnl'].get('day_payout_fill_count')} "
            "in_band_winners="
            f"{digest['pnl'].get('day_in_band_payout_fill_count')}/"
            f"{digest['pnl'].get('day_in_band_resolved_fill_count')} "
            "in_band_lower_tail_p="
            f"{digest['pnl'].get('day_in_band_winner_binomial_lower_tail_p_value')} "
            f"in_band_status={digest['pnl'].get('day_in_band_holdout_live_status')} "
            f"floor_gate_residency={digest['pnl'].get('daily_floor_gate_residency')} "
            f"basis={digest['pnl'].get('day_pnl_primary_basis')} "
            f"day_actual={digest['pnl'].get('day_pnl_actual_basis')} "
            f"basis_split={digest['pnl'].get('basis_split_delta_usd')} "
            "basis_recon="
            f"status:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('status')},"
            f"text:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('scorecard_text_day_pnl_usd')},"
            f"text_fills:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('scorecard_text_fills')},"
            f"text_resolved:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('scorecard_text_resolved_fills')},"
            f"text_mtime:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('scorecard_text_mtime')},"
            f"fill:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('deadman_fill_basis_day_pnl_usd')},"
            f"fill_ts:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('deadman_fill_basis_observed_at')},"
            f"text_older_fill:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('scorecard_text_older_than_deadman_fill_basis')},"
            f"text_minus_fill:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('scorecard_text_vs_deadman_delta_usd')},"
            f"abs_text_fill:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('scorecard_text_vs_deadman_abs_delta_usd')},"
            f"selected_minus_fill:{digest['pnl'].get('day_pnl_basis_reconciliation', {}).get('selected_vs_deadman_delta_usd')} "
            f"split_decomp={digest['pnl'].get('basis_split_decomposition', {}).get('assertion')} "
            f"split_remainder={digest['pnl'].get('basis_split_decomposition', {}).get('remainder_usd')} "
            f"since_topup={digest['pnl']['since_topup_canonical_pnl_usd']} "
            f"actual={digest['pnl']['since_topup_actual_delta_usd']} "
            f"verdict={digest['pnl']['since_topup_verdict']} "
            f"reconciled_actual={digest['pnl']['reconciled_actual_delta_usd']} "
            f"reconciled_verdict={digest['pnl']['reconciled_actual_verdict']} "
            f"overlay_delta={digest['pnl']['self_feed_overlay_delta_usd']} "
            f"overlay_mode={digest['pnl']['self_feed_overlay_mode']} "
            f"ledger_rewrite={digest['pnl']['self_feed_overlay_ledger_rewrite']} "
            f"recon={digest['pnl']['reconciliation_status']} "
            f"raw_recon={digest['pnl'].get('raw_reconciliation_status')} "
            f"unadjusted_recon={digest['pnl'].get('unadjusted_reconciliation_status')} "
            f"cash_diff_residual={digest['pnl'].get('cash_diff_residual_usd')} "
            f"fill_explained={digest['pnl'].get('cash_diff_fill_explained_usd')} "
            f"residual_class={digest['pnl'].get('cash_diff_residual_classification')} "
            f"heartbeat_delta={digest['pnl'].get('heartbeat_ledger_delta')} "
            f"lifetime_money_subbands={digest['pnl'].get('lifetime_price_subbands_01_25_50')} "
            f"goal_reachability={digest['pnl'].get('goal_reachability')} "
            f"passive_holdout={digest['pnl'].get('passive_at_source_holdout')} "
            f"taker_subband_holdout={digest['pnl'].get('taker_price_subband_holdout')} "
            "defense_tripwires="
            f"status:{defense_tripwire_digest.get('status')},"
            f"day:{defense_tripwire_digest.get('t1_day_pnl_usd')}/"
            f"{defense_tripwire_digest.get('t1_day_pnl_floor_usd')}"
            f"(dist:{defense_tripwire_digest.get('t1_day_pnl_distance_to_floor_usd')}),"
            f"since_topup_actual:{defense_tripwire_digest.get('t1_since_topup_actual_usd')}/"
            f"{defense_tripwire_digest.get('t1_since_topup_actual_floor_usd')}"
            f"(dist:{defense_tripwire_digest.get('t1_since_topup_distance_to_floor_usd')}),"
            f"lte_-5:{defense_tripwire_digest.get('t2_lte_minus_5_window_count')},"
            f"posture:{defense_tripwire_digest.get('floor_breach_defense_posture')},"
            f"probe_trigger:{defense_tripwire_digest.get('intraday_probe_degrade_trigger_usd')},"
            f"size_action:{defense_tripwire_digest.get('size_defense_action')} "
            "defense_regret="
            f"status:{defense_regret_digest.get('status')},"
            f"actual:{defense_regret_digest.get('actual_probe_capped_pnl_usd')},"
            f"raw_standard:{defense_regret_digest.get('raw_standard_cap_post_fee_pnl_usd')},"
            f"haircut_standard:{(defense_regret_digest.get('fill_realism_haircut') or {}).get('post_fee_pnl_usd')},"
            f"sign_flip:{defense_regret_digest.get('defense_flipped_sign')},"
            f"two_day:{defense_regret_digest.get('two_consecutive_sign_flips')},"
            f"audit_pull:{defense_regret_digest.get('framework_audit_auto_pull')} "
            f"{e1_inputs_text}"
            "map288="
            f"traded:{map_summary_digest.get('traded_windows')}/288,"
            f"elapsed:{map_summary_digest.get('elapsed_windows')},"
            f"active:{map_summary_digest.get('active_windows')},"
            f"future:{map_summary_digest.get('future_pending_windows')},"
            f"pnl:{map_summary_digest.get('daily_resolved_pnl_usd')},"
            f"win_rate_reporting_only:{map_summary_digest.get('win_rate_reporting_only_pct')} "
            "peer_active_idle="
            f"status:{peer_idle_digest.get('status')},"
            f"total:{peer_idle_digest.get('peer_active_idle_windows')},"
            f"consecutive:{peer_idle_digest.get('consecutive_peer_active_idle_windows')}/"
            f"{peer_idle_digest.get('incident_threshold_windows')},"
            f"incident:{peer_idle_digest.get('incident_triggered')} "
            "pipeline_slo="
            f"freshness:{pipeline_digest.get('freshness_status')},"
            f"age_h:{pipeline_digest.get('artifact_age_h')},"
            f"artifact:{pipeline_digest.get('freshness_artifact')},"
            f"producer:{pipeline_digest.get('freshness_producer')},"
            f"cadence_producer:{pipeline_digest.get('producer_path')},"
            f"last_cycle:{pipeline_digest.get('last_cycle_start_at')},"
            f"cycle_period_s:{pipeline_digest.get('cycle_period_s_observed')},"
            f"cycle_status:{pipeline_digest.get('cycle_period_status')},"
            f"breaches:{(pipeline_digest.get('pipeline_slo') or {}).get('breach_count')},"
            f"stages:{[(row.get('stage'), row.get('time_in_stage_h'), row.get('budget_h'), row.get('status')) for row in pipeline_stages if isinstance(row, dict)]} "
            "standby_clocks="
            f"seat:{standby_digest.get('seat')},"
            f"method:{standby_digest.get('method')},"
            f"volume:{standby_digest.get('volume')} "
            "weekend_day_probe="
            f"status={weekend_day_probe_digest.get('status')} "
            f"current_is_weekend={weekend_day_probe_digest.get('current_is_weekend')} "
            f"day={weekend_day_probe_digest.get('day_pnl_usd')}/"
            f"{weekend_day_probe_digest.get('weekend_day_probe_trigger_usd')} "
            f"dist={weekend_day_probe_digest.get('distance_to_weekend_probe_usd')} "
            f"size_action={weekend_day_probe_digest.get('size_defense_action')} "
            f"seat_loss_action={weekend_seat_loss_rider_digest.get('action')} "
            f"seat_loss_target={weekend_seat_loss_rider_digest.get('target_wallet')} "
            f"rider_status={live_weekend_rider_digest.get('status')} "
            f"rider_cap_check={live_weekend_cap_check.get('status')} "
            f"guard_caps={live_guard_max_order_usd}/"
            f"{live_guard_drip_max_tranche_usd} "
            f"machine_tripwire_controlling={weekend_day_probe_digest.get('machine_tripwire_controlling')} "
            f"next={weekend_day_probe_digest.get('next_action')}"
        ),
        *(
            [
                (
                    "positive_wallet_slice_falsifier: "
                    f"generated_at={digest['positive_wallet_slice_selector_falsifier'].get('generated_at')} "
                    f"verdict={digest['positive_wallet_slice_selector_falsifier'].get('verdict')} "
                    f"sign_inversions={digest['positive_wallet_slice_selector_falsifier'].get('sign_inversion_wallets')} "
                    "rows="
                    f"{[(row.get('wallet'), (row.get('unsliced') or {}).get('post_fee_pnl_usd'), (row.get('unsliced') or {}).get('both_halves_positive'), (row.get('sliced') or {}).get('post_fee_pnl_usd')) for row in digest['positive_wallet_slice_selector_falsifier'].get('rows', []) if isinstance(row, dict)]} "
                    f"path={digest['positive_wallet_slice_selector_falsifier'].get('path')}"
                )
            ]
            if digest["positive_wallet_slice_selector_falsifier"].get("path")
            else []
        ),
        (
            "closed_daily: "
            f"scope={digest['closed_daily'].get('scope')} "
            f"day={digest['closed_daily'].get('day_utc')} "
            f"pnl={digest['closed_daily'].get('pnl_usd')} "
            f"orders={digest['closed_daily'].get('orders')} fills={digest['closed_daily'].get('fills')} "
            f"rejects={digest['closed_daily'].get('rejects')} roi={digest['closed_daily'].get('roi_pct')} "
            f"filled_volume={digest['closed_daily'].get('filled_volume_usd')} "
            f"windows={digest['closed_daily'].get('windows_filled')}/{digest['closed_daily'].get('denominator_windows')} "
            f"submitted={digest['closed_daily'].get('windows_submitted')}/{digest['closed_daily'].get('denominator_windows')} "
            f"since_topup={digest['closed_daily'].get('since_topup_canonical_pnl_usd')} "
            f"actual={digest['closed_daily'].get('since_topup_actual_delta_usd')} "
            f"reconciled_actual={digest['closed_daily'].get('since_topup_reconciled_actual_delta_usd')} "
            f"verdict={digest['closed_daily'].get('since_topup_verdict')} "
            f"target_gap={digest['closed_daily'].get('target_north_star_gap_usd')} "
            "weekly="
            f"due:{digest['closed_weekly_verdict'].get('due')},"
            f"week:{digest['closed_weekly_verdict'].get('week_start_utc')}.."
            f"{digest['closed_weekly_verdict'].get('week_end_utc')} "
            f"pnl:{digest['closed_weekly_verdict'].get('weekly_pnl_usd')},"
            f"pct:{digest['closed_weekly_verdict'].get('weekly_pct')},"
            f"target10_15:{digest['closed_weekly_verdict'].get('target_min_usd')}.."
            f"{digest['closed_weekly_verdict'].get('target_max_usd')} "
            f"gap:{digest['closed_weekly_verdict'].get('gap_to_min_usd')},"
            f"verdict:{digest['closed_weekly_verdict'].get('verdict')},"
            f"days:{digest['closed_weekly_verdict'].get('days_present')},"
            f"missing:{digest['closed_weekly_verdict'].get('missing_days')},"
            f"windows:{digest['closed_weekly_verdict'].get('windows_filled_total')} "
            "automation="
            f"entries:{digest['closed_daily_automation_drift'].get('entries')},"
            f"content_defects:{digest['closed_daily_automation_drift'].get('content_defects')},"
            f"status_counts:{digest['closed_daily_automation_drift'].get('status_counts')},"
            f"items:{[(item.get('kind'), item.get('name'), item.get('pointer_status')) for item in digest['closed_daily_automation_drift'].get('items', [])]}"
        ),
        (
            "volume: "
            f"filled={digest['volume']['windows_filled']}/{digest['volume']['denominator_windows']} "
            f"submitted={digest['volume']['windows_submitted']}/{digest['volume']['denominator_windows']} "
            f"raw_current_active={digest['volume']['active_windows']} raw_missed={digest['volume']['missed_active_windows']} "
            f"raw_consecutive={digest['volume']['consecutive_missed_active_windows']} "
            f"adjusted_active={adjusted_volume.get('adjusted_active_windows')} "
            f"adjusted_missed={adjusted_volume.get('adjusted_missed_active_windows')} "
            f"adjusted_consecutive={adjusted_volume.get('adjusted_consecutive_missed_active_windows')} "
            f"incident={digest['volume']['incident_triggered']} "
            f"source_coverage={source_coverage.get('covered_windows')}/{source_coverage.get('denominator_windows')} "
            f"source_coverage_pct={source_coverage.get('rate_pct')} "
            f"coverage_gap=submitted={coverage_gap_summary.get('submitted_windows')}/"
            f"{coverage_gap_summary.get('windows_total')} "
            f"zero={coverage_gap_summary.get('zero_submission_windows')} "
            f"dominant={coverage_gap_summary.get('dominant_reason_class')} "
            f"gap_to_144={coverage_gap_summary.get('submitted_gap_to_op_volume')} "
            f"abstain_rank={coverage_gap_summary.get('abstaining_predicates_ranked')} "
            f"signal_supply=idle={signal_supply_summary.get('sources_idle_windows')} "
            f"traded_unobserved={signal_supply_summary.get('sources_traded_but_unobserved_windows')} "
            f"unknown={signal_supply_summary.get('unknown_fetch_incomplete_windows')} "
            f"dominant={signal_supply_summary.get('dominant_class')} "
            f"root_cause={signal_supply_summary.get('root_cause')} "
            f"routing=sampled={routing_summary.get('sampled_windows')} "
            f"dominant={routing_summary.get('dominant_class')} "
            f"selected={_short_wallet(routing_summary.get('selected_wallet_at_report_time'))} "
            f"selection_visibility=status={selection_visibility_summary.get('status')} "
            f"sampled={selection_visibility_summary.get('sampled_signal_emitted_but_not_selected_windows')} "
            f"clock={selection_visibility_summary.get('clock_start_condition_met')} "
            f"reason_cov={selection_visibility_summary.get('selector_reason_coverage_pct')} "
            f"field_cov={selection_visibility_summary.get('would_submit_pnl_fee_field_coverage_pct')} "
            f"measured={selection_visibility_summary.get('measured_unique_windows')} "
            f"post={selection_visibility_summary.get('aggregate_measured_would_submit_post_fee_pnl_usd')} "
            f"campaign_lat=status={campaign_lat_summary.get('next_decision')} "
            f"primary={campaign_lat_summary.get('primary_constraint')} "
            f"live_gap={campaign_lat_summary.get('live_day_submitted_gap_to_144')} "
            f"trail_gap={campaign_lat_summary.get('trailing_24h_submitted_gap_to_144')} "
            f"stage2={campaign_lat_summary.get('stage2_verdict')} "
            f"window_pnl=reporting_only:{window_pnl_histogram.get('reporting_only')}"
            f"/gate:{window_pnl_histogram.get('gate_use_allowed')}"
            f"/resolved:{window_pnl_histogram.get('resolved_windows')}"
            f"/positive:{window_pnl_histogram.get('positive_windows')}"
            f"/negative:{window_pnl_histogram.get('negative_windows')}"
            f"/zero:{window_pnl_histogram.get('zero_windows')}"
            f"/buckets:{window_pnl_histogram.get('bucket_counts')} "
            f"routing_shadow=status={routing_shadow_render_summary.get('status')} "
            f"elapsed_h={routing_shadow_render_summary.get('validation_elapsed_hours')} "
            f"would={routing_shadow_render_summary.get('would_submit_windows')} "
            f"extra={routing_shadow_render_summary.get('extra_would_submit_windows')} "
            f"parity={routing_shadow_render_summary.get('copyintent_parity_status')} "
            f"runtime={_short_wallet(routing_shadow_render_summary.get('runtime_selected_wallet'))} "
            f"runtime_source={routing_shadow_render_summary.get('runtime_selected_wallet_source')} "
            f"shadow={_short_wallet(routing_shadow_render_summary.get('shadow_selected_wallet'))} "
            f"selection_changes={routing_shadow_render_summary.get('selection_changes')} "
            f"coverage={routing_shadow_render_summary.get('evaluated_member_count')}/"
            f"{routing_shadow_render_summary.get('non_denied_runtime_members')} "
            f"accounted={routing_shadow_render_summary.get('coverage_accounted_member_count')}/"
            f"{routing_shadow_render_summary.get('non_denied_runtime_members')} "
            f"attrition=fresh:{routing_shadow_attrition.get('fresh_candidate_intents')}"
            f"->fee:{routing_shadow_attrition.get('fresh_candidate_intents_after_expected_fee_gate')}"
            f"->tox:{routing_shadow_attrition.get('fresh_candidate_intents_after_toxicity_protection')}"
            f"->route:{routing_shadow_attrition.get('routeable_signals')}"
            f"->would:{routing_shadow_attrition.get('would_submit')} "
            f"fee_cal=count:{routing_shadow_fee_cal.get('fee_gated_intents')}"
            f"/resolved:{routing_shadow_fee_cal.get('resolved_intents')}"
            f"/measured:{routing_shadow_fee_cal.get('measurable_resolved_intents')}"
            f"/unmeasured:{routing_shadow_fee_cal.get('unmeasured_resolved_intents')}"
            f"/pre:{routing_shadow_fee_cal.get('pre_fee_pnl_usd')}"
            f"/fee:{routing_shadow_fee_cal.get('expected_fee_usd_sum')}"
            f"/post:{routing_shadow_fee_cal.get('post_fee_pnl_usd')}"
            + (
                ""
                if not routing_shadow_pin_summary
                else (
                    f" pin_rule={routing_shadow_pin_summary.get('attribution_rule')}"
                    f" pin_measured={routing_shadow_pin_extra.get('measured_unique_windows')}"
                    f" pin_post={routing_shadow_pin_extra.get('post_fee_pnl_usd')}"
                    f" pin_stability={routing_shadow_pin_stability.get('status')}"
                    f" pin_changed={routing_shadow_pin_stability.get('changed_window_count')}"
                )
            )
        ),
        (
            "recent_participation: "
            + "; ".join(
                f"{row.get('market_slug')} w={row.get('wallet_eligible_orders')} "
                f"s={row.get('our_submits')} f={row.get('our_fills')} r={row.get('our_rejects')} "
                f"cause={row.get('dominant_skip_reason') or row.get('missed_window_attribution') or row.get('empty_window_reason')}"
                for row in digest["volume"].get("recent_participation_rows", [])
            )
            + (
                ""
                if not e6db_autopsy_summary
                else (
                    f" e6db_autopsy: day={e6db_autopsy_summary.get('day_utc')} "
                    f"classification={e6db_autopsy_summary.get('classification')} "
                    f"fills={e6db_autopsy_summary.get('fills')} "
                    f"pnl={e6db_autopsy_summary.get('realized_pnl_usd')} "
                    f"win_rate={e6db_autopsy_summary.get('actual_win_rate_pct')} "
                    f"required={e6db_autopsy_summary.get('required_win_rate_at_payoff_shape_pct')} "
                    f"gap_pp={e6db_autopsy_summary.get('actual_minus_required_win_rate_pp')} "
                    f"sigma_pp={e6db_autopsy_summary.get('gap_sigma_pp')} "
                    f"gap_sigma={e6db_autopsy_summary.get('gap_in_sigma')} "
                    f"avg_win={e6db_autopsy_summary.get('avg_win_per_winner_usd')} "
                    f"avg_loss={e6db_autopsy_summary.get('avg_loss_per_loser_abs_usd')} "
                    f"fee={e6db_autopsy_summary.get('expected_fee_usd')} "
                    f"fee_share_swing={e6db_autopsy_summary.get('expected_fee_share_of_gross_pnl_swing_pct')} "
                    f"pre_fee={e6db_autopsy_summary.get('diagnostic_pre_expected_fee_pnl_usd')} "
                    f"path={e6db_autopsy.get('path')}"
                )
            )
        ),
        (
            "execution_model: "
            f"orders_per_submitted_window={digest['execution_model']['orders_per_submitted_window']} "
            f"orders_per_filled_window={digest['execution_model']['orders_per_filled_window']} "
            f"fill_rate_pct={digest['execution_model']['fill_rate_pct']} "
            f"copy_models={digest['execution_model']['copy_model_counts']} "
            f"drip_orders={digest['execution_model']['drip_orders']} "
            f"drip_fills={digest['execution_model']['drip_fills']} "
            f"drip_stop_saves={digest['execution_model']['drip_stop_saves']} "
            f"strong_orders={digest['execution_model']['strong_orders']} "
            f"strong_pnl={digest['execution_model']['strong_pnl_usd']} "
            "maker_fallback_conversion: "
            f"day={digest['execution_model']['maker_fallback_conversion']['day_utc']} "
            f"canceled_window_end_no_fill="
            f"{digest['execution_model']['maker_fallback_conversion']['maker_fallback_canceled_window_end_no_fill']} "
            f"filled_submissions="
            f"{digest['execution_model']['maker_fallback_conversion']['scorecard_filled_submissions']} "
            f"maker_filled_before_cancel="
            f"{digest['execution_model']['maker_fallback_conversion']['maker_fallback_filled_before_cancel']} "
            f"raw_rejected_or_unfilled="
            f"{digest['execution_model']['maker_fallback_conversion']['raw_rejected_or_unfilled']} "
            f"source={digest['execution_model']['maker_fallback_conversion']['source']}"
        ),
        (
            "active_member: "
            f"{digest['active_set']['current_candidate_id']} "
            f"{_short_wallet(str(digest['active_set']['current_wallet'] or ''))} "
            f"policy={digest['active_set']['current_policy_id']} members={digest['active_set']['member_count']} "
            f"rolling20_ready={digest['member_rolling20']['ready_count']}/"
            f"{digest['member_rolling20']['active_member_count']} "
            f"trigger={digest['member_rolling20']['mechanical_rotation_required']} "
            f"incomplete_negative="
            f"{[_rolling20_row_label(row) for row in digest['member_rolling20']['sample_incomplete_negative_rows'][:3]]}"
        ),
        (
            "runtime_active_set: "
            f"members={digest['active_set']['runtime'].get('member_count')} "
            f"overlay_enabled={digest['active_set']['runtime'].get('overlay_enabled_member_count')} "
            f"overlay_runtime_delta={digest['active_set']['runtime'].get('overlay_runtime_member_delta')} "
            f"qualified={digest['active_set']['runtime'].get('qualified_member_count')} "
            f"selected={_short_wallet(str(digest['active_set']['runtime'].get('selected_wallet') or ''))} "
            f"candidate_pass={digest['active_set']['runtime'].get('candidate_pass_gate')} "
            f"d97_present={digest['active_set']['runtime'].get('d97_present')} "
            f"total_loss_disabled={runtime_total_loss.get('disabled_count') if isinstance(runtime_total_loss, dict) else None} "
            f"disabled_members={runtime_total_loss_disabled_labels} "
            f"wallets={[ _short_wallet(str(wallet)) for wallet in digest['active_set']['runtime'].get('wallets') or [] ]}"
            f"{admission_wave_suffix}"
            f"{e6db_cap_cut_suffix}"
            f"{wave_gate_suffix}"
            f"{gate_3048_suffix}"
            f"{wave_repair_suffix}"
        ),
        (
            "live_members_today: "
            + "; ".join(
                (
                    f"{_short_wallet(str(row.get('wallet') or ''))} "
                    f"orders={row.get('orders')} fills={row.get('fills')} "
                    f"resolved={row.get('resolved_fills')} rejects={row.get('rejects')} "
                    f"pnl={row.get('pnl_usd')} "
                    f"signs={(row.get('trigger_watch') or {}).get('last6_signs')} "
                    f"tail={(row.get('trigger_watch') or {}).get('tail_negative')} "
                    f"dist={(row.get('trigger_watch') or {}).get('distance_to_first_slice_trigger_usd')}"
                )
                for row in digest["active_set"]["live_members_today"][:5]
            )
            if digest["active_set"]["live_members_today"]
            else "live_members_today: none"
        ),
        (
            "active_set_rotation: "
            f"direction={digest['active_set']['latest_rotation'].get('direction_id')} "
            f"updated={digest['active_set']['latest_rotation'].get('updated_at')} "
            f"demoted="
            f"{','.join(_short_wallet(str(wallet)) for wallet in digest['active_set']['latest_rotation'].get('demoted_wallets') or [])} "
            f"admitted="
            f"{','.join(_short_wallet(str(wallet)) for wallet in digest['active_set']['latest_rotation'].get('admitted_wallets') or [])} "
            f"post_fill_wallet="
            f"{_short_wallet(str((digest['active_set']['latest_rotation'].get('post_patch_fresh_fill_basis') or {}).get('wallet') or ''))} "
            f"reason={digest['active_set']['latest_rotation'].get('reason')}"
        ),
        (
            "active_set_auto_degrade: "
            f"direction={digest['active_set']['latest_auto_degrade'].get('direction_id')} "
            f"updated={digest['active_set']['latest_auto_degrade'].get('updated_at')} "
            f"candidate={digest['active_set']['latest_auto_degrade'].get('candidate_id')} "
            f"wallet={_short_wallet(str(digest['active_set']['latest_auto_degrade'].get('wallet') or ''))} "
            f"policy={digest['active_set']['latest_auto_degrade'].get('policy_id')} "
            f"status={digest['active_set']['latest_auto_degrade'].get('status')} "
            f"replaces={digest['active_set']['latest_auto_degrade'].get('replaces_existing_wallet')} "
            f"members={digest['active_set']['latest_auto_degrade'].get('members_count')} "
            f"liveness={digest['active_set']['latest_auto_degrade'].get('liveness_admissions_count')} "
            f"rotation_preserved={digest['active_set']['latest_auto_degrade'].get('latest_rotation_preserved')} "
            "mechanical_demotion="
            f"{_short_wallet(str(digest['active_set']['latest_mechanical_temporal_loss_demotion'].get('target_wallet') or ''))}/"
            f"{digest['active_set']['latest_mechanical_temporal_loss_demotion'].get('status')}/"
            f"pnl:{digest['active_set']['latest_mechanical_temporal_loss_demotion'].get('canonical_pnl_usd')}/"
            f"winner:{digest['active_set']['latest_mechanical_temporal_loss_demotion'].get('resolution_winner')}/"
            f"at:{digest['active_set']['latest_mechanical_temporal_loss_demotion'].get('generated_at')} "
            "cooloff="
            f"{_latest_mechanical_demotion_cooloff(digest)}"
        ),
        (
            "market_facts: "
            f"exists={digest['market_facts']['exists']} path={digest['market_facts']['path']} "
            "fee=no_fixed_per_order_charge_crypto_taker_fee_possible tick_size=fetch_market_tick"
        ),
        "",
        "## Gates",
        (
            "queue: "
            f"ready={digest['gates']['queue_ready_for_live']} depth={digest['gates']['queue_depth']} "
            f"ready_alive={digest['gates']['queue_ready_alive']} "
            f"hot_standby_ready={digest['gates']['hot_standby_ready']}/{digest['gates']['hot_standby_required']} "
            f"hot_standby_gap={digest['gates']['hot_standby_gap']} "
            f"alive_not_ready={digest['gates']['queue_bench_alive_not_ready']} "
            f"dormant_stale_gt_48h={digest['gates']['queue_dormant_stale_gt_48h']} "
            f"unknown_liveness={digest['gates']['queue_unknown_remote_liveness']} "
            f"vintage_pass={digest['gates']['queue_recruitment_vintage_rule_pass']} "
            f"vintage_max_share={digest['gates']['queue_recruitment_vintage_max_share']} "
            f"market_cohort_bridge={digest['gates']['queue_market_cohort_bridge_ranked']}/"
            f"{digest['gates']['queue_market_cohort_bridge_candidates']} "
            f"market_cohort_bridge_accounting="
            f"source={digest['gates']['queue_market_cohort_bridge_source_live_ready_picks']} "
            f"bridged={digest['gates']['queue_market_cohort_bridge_bridged']} "
            f"excluded={digest['gates']['queue_market_cohort_bridge_excluded']} "
            f"excluded_reasons={digest['gates']['queue_market_cohort_bridge_excluded_reason_counts']} "
            f"market_cohort_defects={digest['gates']['queue_market_cohort_bridge_defects']} "
            f"queue_rotation={digest['gates']['queue_rotation_action']} "
            f"brainless_rotation={digest['brainless_ops']['rotation_action']} "
            "fresh_flow_probe: "
            f"generated_at={fresh_flow_probe_digest.get('generated_at')} "
            f"selected={fresh_flow_probe_digest.get('selected_pass_admission_threshold')}/"
            f"{fresh_flow_probe_digest.get('selected_wallets')} "
            f"errors={fresh_flow_probe_digest.get('selected_error_wallets')} "
            f"cumulative={fresh_flow_probe_digest.get('cumulative_pass_admission_threshold')}/"
            f"{fresh_flow_probe_digest.get('cumulative_wallets')} "
            f"cumulative_errors={fresh_flow_probe_digest.get('cumulative_error_wallets')} "
            f"rows={fresh_flow_probe_digest.get('rows')} "
            f"paper_shadow={fresh_flow_probe_digest.get('paper_shadow_enrollments')}"
            + (
                ""
                if not successor_dossier_summary
                else (
                    f" successor={_short_wallet(successor_dossier_summary.get('candidate_wallet'))}"
                    f"/rank={successor_dossier_summary.get('queue_rank')}"
                    f"/status={successor_dossier_summary.get('status')}"
                    f"/routing={successor_dossier_summary.get('routing_status')}"
                    f"/mw={successor_dossier_summary.get('routing_measured_windows')}"
                    f"/post={successor_dossier_summary.get('routing_post_fee_pnl_usd')}"
                    f"/would={successor_dossier_summary.get('routing_would_fill_count')}"
                    f"/fee={successor_dossier_summary.get('fee_coverage_status')}"
                    f"/sigma={successor_dossier_summary.get('gap_sigma_status')}"
                    f"/gap_sigma={successor_dossier_summary.get('gap_in_sigma')}"
                    f"/live_change={successor_dossier_summary.get('live_change')}"
                    f"/path={successor_dossier_digest.get('path')}"
                )
            )
            + (
                ""
                if not active_set_rotation_summary
                else (
                    f" rotation_packet={active_set_rotation_summary.get('status')}"
                    f"/target={_short_wallet(active_set_rotation_summary.get('presumptive_target'))}"
                    f"/fresh4h={active_set_rotation_summary.get('top_fresh_matching_events_4h')}"
                    f"/rate_h={active_set_rotation_summary.get('top_fresh_rate_per_hour')}"
                    f"/lat_p50={active_set_rotation_summary.get('top_latency_p50_s')}"
                    f"/post={active_set_rotation_summary.get('top_post_fee_pnl_usd')}"
                    f"/quiet_fire={active_set_rotation_summary.get('earliest_fire_iso')}"
                    f"/fires_now={active_set_rotation_summary.get('fires_now')}"
                    f"/live_change={active_set_rotation_summary.get('live_path_mutated')}"
                    f"/path={active_set_rotation_packet_digest.get('path')}"
                )
            )
            + (
                ""
                if not active_set_pin_consumer_sweep_digest
                else (
                    f" pin_consumer_sweep={active_set_pin_consumer_sweep_digest.get('status')}"
                    f"/dangerous={len(active_set_pin_consumer_sweep_digest.get('dangerous_consumers') or [])}"
                    f"/authority={active_set_pin_consumer_sweep_digest.get('source_of_truth')}"
                    f"/snapshots=display_only"
                )
            )
            + (
                ""
                if not active_set_post_rotation_summary
                else (
                    f" post_rotation={active_set_post_rotation_summary.get('status')}"
                    f"/target={_short_wallet(active_set_post_rotation_summary.get('target_wallet'))}"
                    f"/measured={active_set_post_rotation_summary.get('measured_windows_found')}"
                    f"/age_metric={active_set_post_rotation_summary.get('observed_age_histogram_metric')}"
                    f"/cap_viol="
                    f"{active_set_post_rotation_summary.get('submitted_decision_time_observed_age_cap_violation_rows')}"
                    f"/si1={active_set_post_rotation_summary.get('si1_reopens')}"
                    f"/path={active_set_post_rotation_digest.get('path')}"
                )
            )
            + mining_cadence_suffix
            + leaderboard_pipeline_suffix
            + cohort_admission_suffix
            + factory_funnel_suffix
        ),
        (
            "E5: "
            f"status={digest['gates']['e5_promotion_status']} unresolved={digest['gates']['e5_unresolved_paper_fills']} "
            f"no_old_unresolved={digest['gates']['e5_no_old_unresolved']} "
            f"non_fallback_resolved={digest['gates']['e5_non_fallback_resolved']} "
            f"non_fallback_pnl={digest['gates']['e5_non_fallback_pnl_usd']}"
        ),
        (
            "E5_book_aware_no_fallback: "
            f"gate={digest['gates']['e5_book_aware_no_fallback_gate']} "
            f"append_only={digest['gates']['e5_book_aware_append_only_resolved']} "
            f"current_source={digest['gates']['e5_book_aware_current_source_resolved']} "
            f"summary={digest['gates']['e5_book_aware_summary_resolved']} "
            f"pnl={digest['gates']['e5_book_aware_summary_pnl_usd']} "
            f"terminal_fill={digest['gates']['e5_book_aware_terminal_fill_rate_pct']} "
            f"monotonicity={digest['gates']['e5_book_aware_monotonicity']} "
            f"source_restated={digest['gates']['e5_book_aware_source_restated_lower_than_prior']} "
            f"metric={digest['gates']['e5_book_aware_gate_metric']} "
            f"fixed_shares_5=gate:{digest['gates']['e5_5share_gate']},"
            f"cohort={digest['gates']['e5_5share_cohort_sha256']} "
            f"resolved={digest['gates']['e5_5share_resolved']} "
            f"post_fee_pnl={digest['gates']['e5_5share_post_fee_pnl_usd']} "
            f"roi={digest['gates']['e5_5share_post_fee_roi_pct']} "
            f"terminal_fill={digest['gates']['e5_5share_terminal_fill_rate_pct']} "
            f"parity_viol={digest['gates']['e5_5share_parity_violations']} "
            f"fallback_viol={digest['gates']['e5_5share_fallback_violations']} "
            f"max_notional={digest['gates']['e5_5share_max_notional_usd']} "
            f"live={digest['gates']['e5_live_actuator_status']}@"
            f"{digest['gates']['e5_live_actuator_generated_at']} "
            f"accepted={digest['gates']['e5_live_actuator_orders_accepted']} "
            f"intents={digest['gates']['e5_live_actuator_intent_ids']} "
            f"book_hashes={digest['gates']['e5_live_actuator_book_hashes']} "
            f"divergence=paper:{digest['gates']['e5_divergence_paper_summary']},"
            f"live:{digest['gates']['e5_divergence_live_summary']},"
            f"optimism:{digest['gates']['e5_divergence_optimism']} "
            f"sizes={digest['gates']['e5_live_actuator_result_sizes']} "
            f"latest_order={digest['gates']['e5_latest_live_order_proof']}"
        ),
        (
            "E11: "
            f"signal={digest['gates']['e11_signal_status']} quotes={digest['gates']['e11_paper_quotes']} "
            f"open={digest['gates']['e11_open_orders']} fills={digest['gates']['e11_filled_orders']} "
            f"pnl={digest['gates']['e11_pnl_usd']} book={digest['gates']['e11_book_orders']} "
            f"non_fallback_book={digest['gates']['e11_non_fallback_book_orders']} "
            f"direct_fallback={digest['gates']['e11_direct_fallback_orders']}"
        ),
        (
            "E7: "
            f"observed={digest['gates']['e7_observed_windows']}/{digest['gates']['e7_required_windows']} "
            f"penny_opportunities={digest['gates']['e7_penny_opportunities']}"
        ),
        (
            "fee_event_check: "
            f"latest_order={digest['fee_event_check']['latest_order_submitted_at']} "
            f"status={digest['fee_event_check']['latest_order_status']} "
            f"expected_fee_gate={digest['fee_event_check']['expected_fee_gate_present']} "
            f"expected_vs_realized={digest['fee_event_check']['expected_vs_realized_fee_present']}"
        ),
        "",
        "## Member Factory KPI",
        (
            "queue_depth: "
            f"ready={factory_queue.get('ready_for_live')} target={factory_queue.get('target_ready_for_live')} "
            f"depth={factory_queue.get('queue_depth')} status={factory_queue.get('status')} "
            f"cause={factory_queue.get('named_cause')}"
        ),
        (
            "set_trajectory: "
            f"members={factory_trajectory.get('member_count')} basis={factory_trajectory.get('compare_basis')} "
            f"delta={factory_trajectory.get('member_count_delta')} "
            f"adds={len(factory_trajectory.get('added') or [])} "
            f"demotes={len(factory_trajectory.get('removed_or_demoted') or [])}"
        ),
        (
            "member_freshness: "
            f"stale={len(factory_freshness.get('stale_members') or [])} "
            f"threshold_s={factory_freshness.get('stale_threshold_s')}"
        ),
        (
            "hour_coverage: "
            f"covered={factory_hours.get('covered_hours')}/{factory_hours.get('denominator_hours')} "
            f"pct={factory_hours.get('coverage_pct')} active_hours={factory_hours.get('active_hours_utc')}"
        ),
        (
            "factory_throughput: "
            f"replay_candidates={factory_throughput.get('replay_candidates_total')} "
            f"fill_backed={factory_throughput.get('fill_backed_candidates')} "
            f"promotable={factory_throughput.get('replay_promotable')} "
            f"ready={factory_throughput.get('ready_for_live')} "
            f"entered_delta={factory_throughput.get('entered_measurement_delta')} "
            f"passing_delta={factory_throughput.get('passing_delta')}"
        ),
        (
            "series_census: "
            f"btc5m={((factory_series.get('series') or {}).get('btc_5m') or {}).get('unique_windows_24h')}/288 "
            f"btc_hourly={((factory_series.get('series') or {}).get('btc_hourly') or {}).get('unique_windows_24h')} "
            f"eth5m={((factory_series.get('series') or {}).get('eth_5m') or {}).get('unique_windows_24h')} "
            f"eth_hourly={((factory_series.get('series') or {}).get('eth_hourly') or {}).get('unique_windows_24h')} "
            f"total_windows={(factory_series.get('total_across_series') or {}).get('unique_windows_24h')}"
        ),
        *(
            [
                (
                    "enabled_overflow_proposal: "
                    f"status={overflow_proposal.get('status')} "
                    f"design={overflow_proposal.get('recommended_design')} "
                    f"current_enabled_rows={overflow_proposal.get('current_enabled_overlay_rows')}/"
                    f"{overflow_proposal.get('current_runtime_target_member_count')} "
                    f"draft_enabled_rows={overflow_proposal.get('enabled_overlay_rows')}/"
                    f"{overflow_proposal.get('runtime_target_member_count')} "
                    f"live_mutation={overflow_proposal.get('live_mutation')} "
                    f"path={overflow_proposal.get('path')} "
                    f"decision_gate={overflow_proposal.get('decision_gate')}"
                )
            ]
            if overflow_proposal.get("path")
            else []
        ),
        (
            "watcher_gap_sample: "
            f"sampled={watcher.get('sampled_windows')} watcher_gap={watcher.get('watcher_gap_windows')} "
            f"coverage_gap={watcher.get('coverage_gap_windows')} pct={watcher.get('watcher_gap_pct')} "
            f"active_btc_trade_windows={watcher.get('active_set_btc_trade_windows_total')}"
        ),
        (
            "active_set_dataapi_poller: "
            f"status={active_set_poller_digest.get('status')} "
            f"wallets={active_set_poller_digest.get('active_set_wallets')}/"
            f"{active_set_poller_digest.get('source_wallet_count')} "
            f"events_fetched={active_set_poller_digest.get('events_fetched')} "
            f"poll_only={active_set_poller_digest.get('poll_only_signals')} "
            f"fresh={active_set_poller_digest.get('fresh_poll_only_signals')} "
            f"duplicate_tx_hash={active_set_poller_digest.get('duplicate_tx_hash')} "
            f"live_allowed={active_set_poller_digest.get('live_orders_allowed')} "
            f"paper_only={active_set_poller_digest.get('paper_only')}"
        ),
        (
            "active_set_rtds_premerge: "
            f"status={active_set_rtds_premerge_digest.get('status')} "
            f"wallets={active_set_rtds_premerge_digest.get('wallets_refreshed')} "
            f"new={active_set_rtds_premerge_digest.get('new_matching_events')} "
            f"retained={active_set_rtds_premerge_digest.get('retained_matching_rows')} "
            f"max_lag={active_set_rtds_premerge_digest.get('max_rtds_catchup_lag_s')} "
            f"selected_new={active_set_rtds_premerge_digest.get('selected_priority_new_matching_events')} "
            f"polygon_match={active_set_rtds_premerge_digest.get('polygon_ws_premerge_matching_events')} "
            f"live_allowed={active_set_rtds_premerge_digest.get('live_orders_allowed')} "
            f"paper_only={active_set_rtds_premerge_digest.get('paper_only')} "
            f"event_scheduler={event_scheduler_digest.get('status')}/"
            f"{event_scheduler_digest.get('triggered')} "
            f"event_sleep={event_scheduler_digest.get('sleep_s')} "
            f"event_id={(event_scheduler_digest.get('source_event') or {}).get('event_id')} "
            f"last_event={(event_scheduler_digest.get('last_trigger') or {}).get('event_id')} "
            f"single_submitter_change={event_scheduler_digest.get('single_submitter_change')} "
            f"copyintent_parity_change={event_scheduler_digest.get('copyintent_parity_change')}"
        ),
        (
            "live_execution_probes: "
            + "; ".join(
                (
                    f"{row.get('label')} wallet={_short_wallet(row.get('source_wallet'))} "
                    f"fresh={row.get('fresh_candidate_intents')} "
                    f"after_tox={row.get('fresh_candidate_intents_after_toxicity_protection')} "
                    f"events={row.get('source_events')} skips={row.get('skip_counts')} "
                    f"min_pin={row.get('min_live_floor_pin_enabled')} "
                    f"latest={row.get('latest_market_slug')} "
                    f"btc5m={row.get('latest_btc_5m_scope_ok')} "
                    f"closed={row.get('latest_market_closed_now')}"
                )
                for row in live_probe_digest
            )
            if live_probe_digest
            else "live_execution_probes: none"
        ),
        (
            "own_impact_monitor: "
            f"status={own_impact_digest.get('status')} "
            f"cause={own_impact_digest.get('named_cause')} "
            f"stale_rows={own_impact_digest.get('stale_inventory_rows')} "
            f"wallet_orders={own_impact_digest.get('wallet_eligible_orders')} "
            f"our_submits={own_impact_digest.get('our_submits')} "
            f"our_fills={own_impact_digest.get('our_fills')} "
            f"premerge_wallets={own_impact_digest.get('active_set_rtds_wallets_refreshed')}"
        ),
        (
            "self_feed_vs_ledger: "
            f"primary_recon={self_feed_digest.get('status')} "
            f"verdict={self_feed_digest.get('primary_verdict')} "
            f"actual_reconciled={self_feed_digest.get('actual_basis_reconciled_verdict')} "
            f"raw_status={self_feed_digest.get('raw_status')} "
            f"raw_recon={self_feed_digest.get('raw_reconciliation_status')} "
            f"overlay_delta={self_feed_digest.get('overlay_delta_usd')} "
            f"matched={self_feed_digest.get('matched_ledger_tx_groups')}/"
            f"{self_feed_digest.get('ledger_filled_tx_groups')} "
            f"self_feed_tx={self_feed_digest.get('self_feed_tx_groups')} "
            f"data_api_rows={self_feed_digest.get('data_api_trade_rows')} "
            f"polygon_rows={self_feed_digest.get('polygon_orderfilled_rows')} "
            f"ledger_missing={self_feed_digest.get('ledger_missing_self_feed_critical')} "
            f"grace={self_feed_digest.get('ledger_missing_self_feed_within_grace')} "
            f"self_missing={self_feed_digest.get('self_feed_missing_ledger_critical')} "
            f"amount_mismatch={self_feed_digest.get('amount_mismatch_tx_groups')} "
            f"price_rounding={self_feed_digest.get('price_rounding_mismatch_tx_groups')} "
            f"split_groups={self_feed_digest.get('probable_split_fill_groups')}/"
            f"{self_feed_digest.get('probable_split_fill_missing_tx_groups')} "
            f"missing_cost={self_feed_digest.get('self_feed_missing_ledger_cost_usd')} "
            f"missing_pnl={self_feed_digest.get('self_feed_missing_ledger_pnl_usd')}"
        ),
        (
            "cash_ledger_classification: "
            f"true_unrecorded={cash_ledger_digest.get('true_unrecorded_fill_candidate')} "
            f"join_key={cash_ledger_digest.get('join_key_defect_probable_split_fill')} "
            f"rows={cash_ledger_digest.get('self_feed_missing_ledger_rows')} "
            f"amount_mismatch_split_overlap={cash_ledger_digest.get('amount_mismatch_split_overlap')} "
            f"p0={cash_ledger_digest.get('p0_guard_fill_recording_audit_required')} "
            f"cost_by_class={cash_ledger_digest.get('cost_by_class_usd')} "
            f"pnl_by_class={cash_ledger_digest.get('pnl_by_class_usd')}"
        ),
        (
            "self_feed_missing_trace: "
            f"sampled={self_feed_trace_digest.get('sampled')}/"
            f"{self_feed_trace_digest.get('candidate_total')} "
            f"counts={self_feed_trace_digest.get('trace_counts')} "
            f"b2={self_feed_trace_digest.get('b2_non_guard_fill_confirmed')} "
            f"notify={self_feed_trace_digest.get('immediate_notify_required')} "
            f"gap={self_feed_trace_digest.get('gap_equation')}"
        ),
        (
            "self_feed_full_ledger_retrace: "
            f"candidates={self_feed_full_retrace_digest.get('candidate_total')} "
            f"counts={self_feed_full_retrace_digest.get('class_counts')} "
            f"b1={self_feed_full_retrace_digest.get('b1_confirmed_count')} "
            f"b2={self_feed_full_retrace_digest.get('b2_suspect_count')} "
            f"notify={self_feed_full_retrace_digest.get('immediate_notify_required')} "
            f"equation={self_feed_full_retrace_digest.get('reconciliation_equation')}"
        ),
        (
            "self_feed_duckdb_benchmark: "
            f"status={self_feed_duckdb_digest.get('status')} "
            f"rows={self_feed_duckdb_digest.get('jsonl_rows')}/{self_feed_duckdb_digest.get('duckdb_rows')} "
            f"tx_groups={self_feed_duckdb_digest.get('tx_groups')} "
            f"cost={self_feed_duckdb_digest.get('cost_usd')} "
            f"parity={self_feed_duckdb_digest.get('parity')} "
            f"gap={self_feed_duckdb_digest.get('gap_status')} "
            f"critical={self_feed_duckdb_digest.get('gap_critical')} "
            f"gap_cost={self_feed_duckdb_digest.get('gap_missing_cost_usd')} "
            f"split={self_feed_duckdb_digest.get('gap_split_groups')} "
            f"price_rounding={self_feed_duckdb_digest.get('gap_price_rounding')} "
            f"class_rows={self_feed_duckdb_digest.get('classification_rows')} "
            f"join_key={self_feed_duckdb_digest.get('join_key_split')} "
            f"true_unrecorded={self_feed_duckdb_digest.get('true_unrecorded')} "
            f"retrace_counts={self_feed_duckdb_digest.get('retrace_counts')} "
            f"overlay_pnl={self_feed_duckdb_digest.get('overlay_pnl_usd')} "
            f"reconciled_actual={self_feed_duckdb_digest.get('reconciled_actual_estimate_usd')} "
            f"recommendation={self_feed_duckdb_digest.get('recommendation_mode')} "
            f"ledger_rewrite={self_feed_duckdb_digest.get('ledger_rewrite')} "
            f"ms=jsonl:{self_feed_duckdb_digest.get('jsonl_elapsed_ms')} "
            f"duckdb:{self_feed_duckdb_digest.get('duckdb_elapsed_ms')} "
            f"gap:{self_feed_duckdb_digest.get('gap_elapsed_ms')} "
            f"next={self_feed_duckdb_digest.get('next_action')}"
        ),
        (
            "h2_external_redemptions: "
            f"status={h2_external_digest.get('status')} rows={h2_external_digest.get('external_redeem_rows')} "
            f"confirmed={h2_external_digest.get('confirmed_external_redeem_rows')} "
            f"redeem_usdc={h2_external_digest.get('total_redeem_usdc')} "
            f"max_delta={h2_external_digest.get('max_abs_delta_usd')} "
            f"residual={h2_external_digest.get('cash_diff_residual_usd')} "
            f"explained={h2_external_digest.get('residual_explained_by_external_redeems_usd')} "
            f"unexplained={h2_external_digest.get('residual_unexplained_after_external_redeems_usd')} "
            f"anchor={h2_external_digest.get('anchor_relabel')} "
            f"overlay_source={h2_external_digest.get('overlay_source_name')} "
            f"ledger_rewrite={h2_external_digest.get('ledger_rewrite')}"
        ),
        (
            "h2_account_value_residual: "
            f"status={h2_account_value_digest.get('status')} "
            f"snapshots={h2_account_value_digest.get('snapshot_count')} "
            f"residual={h2_account_value_digest.get('latest_cash_diff_residual_usd')} "
            f"class={h2_account_value_digest.get('residual_class')} "
            f"open_mark_ruled_out={h2_account_value_digest.get('open_position_mark_timing_ruled_out')} "
            f"fee_dust_ruled_out={h2_account_value_digest.get('fee_or_dust_ruled_out')} "
            f"next={h2_account_value_digest.get('next_action')}"
        ),
        (
            "residual_cash_diff_audit: "
            f"status={residual_cash_diff_digest.get('status')} "
            f"residual={residual_cash_diff_digest.get('canonical_residual_usd')} "
            f"class={residual_cash_diff_digest.get('canonical_residual_classification')} "
            f"match={residual_cash_diff_digest.get('single_movement_match_status')} "
            f"scorecard_day_matches={residual_cash_diff_digest.get('scorecard_day_direct_matches')} "
            f"baseline_matches={residual_cash_diff_digest.get('baseline_window_direct_matches')} "
            f"other_counterparty={residual_cash_diff_digest.get('other_counterparty_rows')}"
        ),
        (
            "scorecard_same_cut_basis: "
            f"status={same_cut_basis_digest.get('status')} "
            f"scorecard_at={same_cut_basis_digest.get('scorecard_generated_at')} "
            f"json={same_cut_basis_digest.get('json_totals')} "
            f"text={same_cut_basis_digest.get('text_totals')} "
            f"count_match={same_cut_basis_digest.get('count_match')} "
            f"pnl_match={same_cut_basis_digest.get('pnl_match')} "
            f"post_panic={post_panic_digest.get('status')}/{post_panic_digest.get('checked_count')}"
            f"/fail{post_panic_digest.get('failure_count')}/repair{post_panic_digest.get('repair_count')} "
            f"scheduler_path={scheduler_ratchet_digest.get('path')} "
            f"scheduler_rows={scheduler_ratchet_digest.get('paper_clock_rows_landed')} "
            f"scheduler_post_fee={scheduler_ratchet_digest.get('paper_clock_post_fee_would_pnl_usd')} "
            f"scheduler_verdict={scheduler_verdict_summary.get('verdict')} "
            f"gate_pnl={scheduler_verdict_summary.get('gate_pnl_usd')} "
            f"gate_pass={scheduler_verdict_summary.get('gate_pass')} "
            f"strat_status={scheduler_stratification_summary.get('status')} "
            f"survivors={scheduler_stratification_summary.get('survivors')} "
            f"retirement={scheduler_retirement_digest.get('status')}"
        ),
        (
            "pinned_tranche_economics: "
            f"status={pinned_tranche_digest.get('status')} "
            f"resolved={pinned_tranche_digest.get('resolved_pinned_fills')} "
            f"filled={pinned_tranche_digest.get('pinned_filled_orders')} "
            f"status_counts={pinned_tranche_digest.get('pinned_status_counts')} "
            f"pnl={pinned_tranche_digest.get('pnl_usd')} "
            f"win_rate={pinned_tranche_digest.get('win_rate_pct')} "
            f"breakeven={pinned_tranche_digest.get('breakeven_win_rate_pct')} "
            f"wilson_lb={pinned_tranche_digest.get('wilson_95_lower_bound_win_rate_pct')} "
            f"wilson_gt_breakeven={pinned_tranche_digest.get('wilson_lower_bound_gt_breakeven')} "
            f"worst_bucket={pinned_tranche_digest.get('worst_bucket')} "
            f"probe={pinned_tranche_digest.get('probe_trigger_usd')} "
            f"distance={pinned_tranche_digest.get('distance_to_probe_trigger_usd')} "
            f"sizing_gate={pinned_tranche_digest.get('sizing_gate_20_30z')} "
            f"threshold_change_allowed={pinned_tranche_digest.get('threshold_change_allowed')}"
        ),
        (
            "pinned_tranche_midday_due_check: "
            f"status={pinned_midday_digest.get('status')} "
            f"trigger_n={pinned_midday_digest.get('trigger_n')} "
            f"resolved={pinned_midday_digest.get('resolved_pinned_fills')} "
            f"filled={pinned_midday_digest.get('pinned_filled_orders')} "
            f"pnl={pinned_midday_digest.get('pnl_usd')} "
            f"win_rate={pinned_midday_digest.get('win_rate_pct')} "
            f"breakeven={pinned_midday_digest.get('breakeven_win_rate_pct')} "
            f"wilson_lb={pinned_midday_digest.get('wilson_95_lower_bound_win_rate_pct')} "
            f"wilson_gt_breakeven={pinned_midday_digest.get('wilson_lower_bound_gt_breakeven')} "
            f"probe={pinned_midday_digest.get('probe_trigger_usd')} "
            f"distance={pinned_midday_digest.get('distance_to_probe_trigger_usd')} "
            f"sizing_gate={pinned_midday_digest.get('sizing_gate_20_30z')} "
            f"threshold_change_allowed={pinned_midday_digest.get('threshold_change_allowed')}"
        ),
        (
            "guard_fill_audit: "
            f"sample={guard_fill_audit_digest.get('sample_size')}/"
            f"{guard_fill_audit_digest.get('true_candidate_population')} "
            f"b1={guard_fill_audit_digest.get('b1_count')} "
            f"b2={guard_fill_audit_digest.get('b2_count')} "
            f"b3={guard_fill_audit_digest.get('b3_count')} "
            f"notify={guard_fill_audit_digest.get('immediate_notify_required')} "
            f"actual={guard_fill_audit_digest.get('actual_delta_usd')} "
            f"residual={guard_fill_audit_digest.get('ruled_residual_usd')} "
            f"b1_effect={guard_fill_audit_digest.get('sample_b1_effect_usd')} "
            f"b2_effect={guard_fill_audit_digest.get('sample_b2_effect_usd')} "
            f"b3_effect={guard_fill_audit_digest.get('b3_accounting_effect_usd')} "
            f"unexplained={guard_fill_audit_digest.get('unexplained_usd')}"
        ),
        (
            "fill_toxicity: "
            f"verdict={fill_toxicity_digest.get('verdict')} "
            f"signals={fill_toxicity_digest.get('signal_count')} "
            f"signal_roi={fill_toxicity_digest.get('signal_roi_pct')} "
            f"fills={fill_toxicity_digest.get('live_fill_count')} "
            f"fill_roi={fill_toxicity_digest.get('live_fill_roi_pct')} "
            f"toxicity={fill_toxicity_digest.get('toxicity_roi_pct')} "
            f"worst={_short_wallet(str(fill_toxicity_digest.get('worst_wallet') or ''))}/"
            f"{fill_toxicity_digest.get('worst_bucket')} "
            f"worst_toxicity={fill_toxicity_digest.get('worst_toxicity_roi_pct')} "
            f"worst_fills={fill_toxicity_digest.get('worst_live_fills')}"
        ),
        (
            "fill_conditioned_loss_attribution: "
            f"fills={fill_loss_digest.get('resolved_fills')} "
            f"pnl={fill_loss_digest.get('pnl_usd')} "
            f"roi={fill_loss_digest.get('roi_pct')} "
            f"scope={fill_loss_digest.get('top_loss_scope')} "
            f"post_floor_25_50_fills={fill_loss_digest.get('post_floor_25_50_fills')} "
            f"post_floor_rejects={fill_loss_digest.get('post_floor_25_50_rejects')} "
            f"top={[(row.get('dimension'), row.get('value'), row.get('fills'), row.get('pnl_usd')) for row in fill_loss_digest.get('top_loss_concentrations') or []]}"
        ),
        (
            "window_time_reject_attribution: "
            f"current={window_time_reject_digest.get('current_rows')} "
            f"{window_time_reject_digest.get('current_ruling_input')} "
            f"source_med={window_time_reject_digest.get('current_median_source_lateness_s')} "
            f"detect_med={window_time_reject_digest.get('current_median_detection_latency_s')} "
            f"post_latest={window_time_reject_digest.get('post_latest_rows')} "
            f"{window_time_reject_digest.get('post_latest_ruling_input')} "
            f"post_tripwire={window_time_reject_digest.get('post_tripwire_rows')} "
            f"{window_time_reject_digest.get('post_tripwire_rtds_window_time_tripwire_status')}/"
            f"{window_time_reject_digest.get('post_tripwire_rtds_window_time_tripwire_rows')} "
            f"flow_money={window_time_reject_digest.get('money_fill_count')}/"
            f"{window_time_reject_digest.get('distinct_filled_windows')} "
            f"{window_time_reject_digest.get('flow_money_classification')}"
        ),
        (
            "inventory_skip_lifecycle: "
            f"source={inventory_skip_digest.get('source')} "
            f"records={inventory_skip_digest.get('record_count')} "
            f"recoverable_intents={inventory_skip_digest.get('recoverable_intent_estimate')} "
            f"counts={inventory_skip_digest.get('skip_reason_counts_24h')} "
            f"mechanism={inventory_skip_digest.get('dominant_mechanism')} "
            f"inventory={inventory_skip_digest.get('inventory_skip_total')} "
            f"recoverable={inventory_skip_digest.get('recoverable_inventory_skip_total')} "
            f"late_inventory={inventory_skip_digest.get('late_inventory_skip_total')} "
            f"window_time={inventory_skip_digest.get('window_time_skip_total')} "
            f"submitted={inventory_skip_digest.get('orders_submitted_this_cycle')} "
            f"c4={inventory_skip_digest.get('c4')} "
            f"live_mutated={inventory_skip_digest.get('live_path_mutated')}"
        ),
        (
            "toxicity_denylist: "
            f"cells={toxicity_denylist_digest.get('cell_count')} "
            f"criteria={toxicity_denylist_digest.get('criteria')} "
            f"top_cells="
            f"{[( _short_wallet(str(row.get('source_wallet') or '')), row.get('price_bucket'), row.get('direction'), row.get('our_fills'), row.get('our_fill_roi_pct')) for row in toxicity_denylist_digest.get('cells') or []][:3]}"
        ),
        (
            "strategy_map: "
            f"rows={strategy_map_digest.get('rows')} "
            f"fresh={strategy_map_digest.get('fresh_rows')} "
            f"stale={strategy_map_digest.get('stale_rows')} "
            f"active_or_gated={strategy_map_digest.get('active_or_gated_rows')} "
            f"authority={strategy_map_digest.get('authority')} "
            f"stale_ids={strategy_map_digest.get('stale_ids')} "
            f"utilization=verdict={digest['resource_utilization'].get('verdict')} "
            f"registry_active={digest['resource_utilization'].get('registry_active_or_gated_lane_count')} "
            f"registry_occupancy_pct={digest['resource_utilization'].get('registry_status_occupancy_pct')} "
            f"productive={digest['resource_utilization'].get('productive_lane_count')} "
            f"measured_max={digest['resource_utilization'].get('measured_max_lane_count')} "
            f"idle={digest['resource_utilization'].get('idle_lane_capacity')} "
            f"productive_utilization_pct={digest['resource_utilization'].get('productive_utilization_pct')} "
            f"cpu_headroom_pct={digest['resource_utilization'].get('cpu_headroom_pct')} "
            f"mem_headroom_pct={digest['resource_utilization'].get('memory_headroom_to_pause_pct')} "
            f"defect_open={digest['resource_utilization'].get('defect_open')}"
        ),
        (
            "strategy_decompiler_intake: "
            f"selected={decompiler_digest.get('selected_wallets')} "
            f"eligible={decompiler_digest.get('eligible_wallets')} "
            f"events={decompiler_digest.get('events_scanned')} "
            f"top={_short_wallet(str(decompiler_digest.get('top_wallet') or ''))} "
            f"top_pnl={decompiler_digest.get('top_pnl_usd')} "
            f"top_roi={decompiler_digest.get('top_roi_pct')} "
            f"top_resolved={decompiler_digest.get('top_resolved_buy_events')} "
            f"top_conditions={decompiler_digest.get('top_unique_conditions')}"
        ),
        (
            "followability_leaderboard: "
            f"selected={followability_digest.get('selected_wallets')} "
            f"scored={followability_digest.get('wallets_scored')} "
            f"windows={followability_digest.get('early_commitment_windows')} "
            f"top={_short_wallet(str(followability_digest.get('top_wallet') or ''))} "
            f"score={followability_digest.get('top_score')} "
            f"predict={followability_digest.get('top_predictiveness_pct')} "
            f"win={followability_digest.get('top_win_rate_pct')} "
            f"cont={followability_digest.get('top_avg_continuation_usd')} "
            f"eligible={followability_digest.get('top_eligible_windows')}"
        ),
        (
            "full_universe_copyability: "
            f"registry={full_universe_digest.get('registry_wallets')} "
            f"scored={full_universe_digest.get('wallets_scored')} "
            f"evidence={full_universe_digest.get('wallets_with_any_evidence')} "
            f"replay={full_universe_digest.get('wallets_with_replay')} "
            f"positive_copy={full_universe_digest.get('positive_copy_pnl_wallets')} "
            f"ready_queue={full_universe_digest.get('ranked_queue_depth')} "
            f"prior_live_demotion_excluded={full_universe_digest.get('prior_live_demotion_excluded')} "
            f"top={_short_wallet(str(full_universe_digest.get('top_wallet') or ''))} "
            f"score={full_universe_digest.get('top_score')} "
            f"status={full_universe_digest.get('top_admission_status')} "
            f"paper_pnl={full_universe_digest.get('top_paper_pnl_usd')} "
            f"copyable={full_universe_digest.get('top_copyable_buy_events')} "
            f"follow={full_universe_digest.get('top_followability_score')} "
            f"legacy_twins={full_universe_digest.get('legacy_twin_pointers')} "
            "research_cadence="
            f"alpha:{((research_lane_cadence_digest.get('alpha_decay') or {}).get('status'))},"
            f"top10:{((research_lane_cadence_digest.get('top10_broad') or {}).get('status'))},"
            f"whale:{((research_lane_cadence_digest.get('whale_consensus') or {}).get('status'))} "
            f"targeted_copyability={targeted_copyability_digest.get('summary')} "
            f"targeted_rows={targeted_copyability_digest.get('rows')} "
            f"market_scan_status={market_scan_digest.get('status')} "
            f"market_scan_active={market_scan_digest.get('active_wallets')} "
            f"market_scan_ranked={market_scan_digest.get('wallets_ranked')} "
            f"scanned_alive_profitable={market_scan_digest.get('scanned_alive_profitable')} "
            f"raw_cohort_live_ready={market_scan_digest.get('raw_cohort_live_ready_picks')} "
            f"alive_liveness_missing={market_scan_digest.get('alive_profitable_missing_liveness_rows')} "
            f"alive_liveness_fail={market_scan_digest.get('alive_profitable_failed_liveness_reason_counts')} "
            f"cohort_size={market_scan_digest.get('cohort_size')} "
            f"cohort_shadow_positive={market_scan_digest.get('cohort_shadow_positive')} "
            f"live_ready_picks={market_scan_digest.get('live_ready_picks')} "
            f"market_scan_pages={market_scan_digest.get('pages_completed')} "
            f"market_scan_complete={market_scan_digest.get('lookback_complete')} "
            "hot_history_accumulator="
            f"status={hot_history_accumulator_digest.get('status')} "
            f"events={hot_history_accumulator_digest.get('event_count')} "
            f"inserted={hot_history_accumulator_digest.get('events_inserted')} "
            f"wallets={hot_history_accumulator_digest.get('source_wallet_count')} "
            f"span_days={hot_history_accumulator_digest.get('span_days')} "
            f"source_age={hot_history_accumulator_digest.get('newest_source_event_age_s')} "
            f"fresh={hot_history_accumulator_digest.get('source_freshness_pass')} "
            f"repoint_allowed={hot_history_accumulator_digest.get('repoint_allowed')} "
            f"gate={hot_history_accumulator_digest.get('repoint_gate')} "
            f"supplemental={hot_history_accumulator_digest.get('supplemental_export')} "
            "same_window="
            f"status={same_window_capture_digest.get('status')} "
            f"gate={same_window_capture_digest.get('gate_status')} "
            f"run={same_window_capture_digest.get('run_id')} "
            f"completed={same_window_capture_digest.get('completed_at')} "
            f"wallets={same_window_capture_digest.get('selected_wallet_count')} "
            f"gates={same_window_capture_digest.get('gates')} "
            f"top10={same_window_capture_digest.get('top10')} "
            f"alpha={same_window_capture_digest.get('alpha')} "
            f"exact={same_window_capture_digest.get('exact_policy')} "
            f"paper_only={same_window_capture_digest.get('paper_only')} "
            f"live_allowed={same_window_capture_digest.get('live_orders_allowed')} "
            "wide_candidate_measurement="
            f"roster={wide_candidate_digest.get('roster_source_counts')} "
            f"direct_climb_members={wide_candidate_digest.get('roster_direct_climb_members')} "
            f"paper_only={wide_candidate_digest.get('roster_paper_only')} "
            f"live_orders_allowed={wide_candidate_digest.get('roster_live_orders_allowed')} "
            f"current_alpha_manifest={wide_candidate_digest.get('current_alpha_manifest')} "
            f"depth_priority_frontier={wide_candidate_digest.get('depth_priority_frontier')} "
            f"park_reconciliation_82c8={wide_candidate_digest.get('park_reconciliation_82c8')} "
            f"repaired_cohort_gap={wide_candidate_digest.get('repaired_eligible_slice_cohort_gap')} "
            f"order104_delta={wide_candidate_digest.get('order104_alpha_eligibility_delta')} "
            f"order106_delta={wide_candidate_digest.get('order106_slice_selection_delta')} "
            f"standby_park_registry={wide_candidate_digest.get('standby_park_registry')} "
            f"f1_accrual_stop_951b={wide_candidate_digest.get('f1_accrual_stop_951b')} "
            f"order109_residual={wide_candidate_digest.get('order109_residual_ledger')} "
            f"alpha_status={wide_candidate_digest.get('alpha_status')} "
            f"freshness={wide_candidate_digest.get('source_freshness')} "
            f"policy={wide_candidate_digest.get('policy_id')} "
            f"cohort={wide_candidate_digest.get('prospective_cohort')} "
            f"prospective={wide_candidate_digest.get('prospective_summary')} "
            f"metadata_recovery={wide_candidate_digest.get('metadata_recovery')} "
            f"direct_climb_exact={wide_candidate_digest.get('direct_climb_exact')} "
            f"climb_backup={wide_candidate_digest.get('climb_backup')} "
            f"terminal_reconciliation={wide_candidate_digest.get('terminal_reconciliation')} "
            f"direct_latency={wide_candidate_digest.get('direct_latency')} "
            f"immutable_alpha={wide_candidate_digest.get('immutable_alpha_manifest')} "
            f"frozen_fingerprints={wide_candidate_digest.get('frozen_capture_fingerprint_evidence')} "
            f"broad_admission={wide_candidate_digest.get('diagnostic_broad_history_is_admission_evidence')} "
            f"supervisor={wide_candidate_digest.get('supervisor')} "
            f"supervisor_pipeline={wide_candidate_digest.get('supervisor_pipeline_deployment')} "
            f"summary={wide_candidate_digest.get('summary')} "
            f"winners={wide_candidate_digest.get('winner_wallets')} "
            f"copyable_rate_reachability={wide_candidate_digest.get('copyable_rate_reachability')} "
            f"f3_batch_interval_attribution={wide_candidate_digest.get('f3_batch_interval_attribution')} "
            f"frontier_deficit_partition={wide_candidate_digest.get('frontier_deficit_partition')} "
            f"resolved_signal_accrual={wide_candidate_digest.get('resolved_signal_accrual')} "
            f"fingerprint_durability={wide_candidate_digest.get('fingerprint_durability')} "
            f"selector_admissibility_divergence={wide_candidate_digest.get('selector_admissibility_divergence')} "
            f"positive_slice_family={wide_candidate_digest.get('positive_slice_family')} "
            "bac25_forward_only="
            f"registered={bac25_forward_lane_digest.get('registered_at')} "
            f"deadline={bac25_forward_lane_digest.get('observation_deadline_at')} "
            f"retrospective_n={bac25_forward_lane_digest.get('retrospective_n')} "
            f"retrospective_input={bac25_forward_lane_digest.get('retrospective_is_admission_input')} "
            f"forward_n={bac25_forward_lane_digest.get('forward_n')} "
            f"forward_pnl={bac25_forward_lane_digest.get('forward_post_fee_pnl_usd')} "
            f"forward_roi_pct={bac25_forward_lane_digest.get('forward_roi_pct')} "
            f"forward_half_ex_top1={bac25_forward_lane_digest.get('forward_half_pnl_excluding_top_1_market')} "
            f"substantive_checks={(bac25_forward_lane_digest.get('forward_evidence_projection') or {}).get('substantive_checks')} "
            f"projection={bac25_forward_lane_digest.get('forward_evidence_projection')} "
            f"checks={bac25_forward_lane_digest.get('checks')} "
            f"eligible={bac25_forward_lane_digest.get('admission_eligible')} "
            f"paper_only={bac25_forward_lane_digest.get('paper_only')} "
            f"live_authority={bac25_forward_lane_digest.get('live_authority')} "
            "forward_lane_digest_lag_s="
            f"{digest.get('forward_lane_digest_lag_s')} "
            f"forward_lane_digest_lag_budget_s={digest.get('forward_lane_digest_lag_budget_s')} "
            f"forward_lane_digest_lag_status={digest.get('forward_lane_digest_lag_status')} "
            f"digest_generated_at={digest.get('forward_lane_digest_generated_at')} "
            f"newest_lane_generated_at={digest.get('forward_lane_newest_generated_at')} "
            "wallet_951b_forward_only="
            f"registered={wallet_951b_forward_lane_digest.get('registered_at')} "
            f"deadline={wallet_951b_forward_lane_digest.get('observation_deadline_at')} "
            f"forward_n={wallet_951b_forward_lane_digest.get('forward_n')} "
            f"forward_pnl={wallet_951b_forward_lane_digest.get('forward_post_fee_pnl_usd')} "
            f"checks={wallet_951b_forward_lane_digest.get('checks')} "
            f"eligible={wallet_951b_forward_lane_digest.get('admission_eligible')} "
            f"paper_only={wallet_951b_forward_lane_digest.get('paper_only')} "
            f"live_authority={wallet_951b_forward_lane_digest.get('live_authority')} "
            "bac25_forward_writer_scope="
            f"{digest.get('bac25_forward_writer_scope')} "
            "wide_forward_sibling_lanes="
            f"{digest.get('wide_forward_sibling_lanes')} "
            "family_terminal_registry="
            f"{digest.get('policy_family_terminal_registry')} "
            "c539_deferred_open="
            f"status={c539_deferred_open_digest.get('status')} "
            f"paper_only={c539_deferred_open_digest.get('paper_only')} "
            f"live_allowed={c539_deferred_open_digest.get('live_orders_allowed')} "
            f"policy={c539_deferred_open_digest.get('policy_id')}@"
            f"{str(c539_deferred_open_digest.get('policy_fingerprint') or '')[:12]} "
            f"drift={c539_deferred_open_digest.get('policy_drift')} "
            f"clock={c539_deferred_open_digest.get('registered_at')}->"
            f"{c539_deferred_open_digest.get('observation_deadline_at')} "
            f"raw_rows={c539_deferred_open_digest.get('raw_rows')} "
            f"distinct_signals={c539_deferred_open_digest.get('distinct_signals')} "
            f"duplicates={c539_deferred_open_digest.get('cross_feed_duplicate_rows')} "
            f"not_open={c539_deferred_open_digest.get('not_open_yet_rows')}/"
            f"{c539_deferred_open_digest.get('distinct_signals')} "
            f"share={c539_deferred_open_digest.get('not_open_yet_share')} "
            f"survived={c539_deferred_open_digest.get('survived_open_re_evaluation')}/"
            f"{c539_deferred_open_digest.get('deferred_window_outcomes')} "
            f"open_grace={c539_deferred_open_digest.get('open_grace_covered_windows')}/"
            f"{c539_deferred_open_digest.get('open_grace_total_windows')} "
            f"coverage={c539_deferred_open_digest.get('open_grace_coverage')} "
            f"instrumented_open_grace="
            f"{c539_deferred_open_digest.get('open_grace_instrumented_covered_windows')}/"
            f"{c539_deferred_open_digest.get('open_grace_instrumented_total_windows')} "
            f"instrumented_coverage="
            f"{c539_deferred_open_digest.get('open_grace_instrumented_coverage')} "
            f"coverage_lt_60={c539_deferred_open_digest.get('open_grace_coverage_below_60pct')} "
            f"resolved={c539_deferred_open_digest.get('resolved')} "
            f"projection={c539_deferred_open_digest.get('forward_evidence_projection')} "
            f"post={c539_deferred_open_digest.get('post_fee_pnl_usd')} "
            f"halves={c539_deferred_open_digest.get('first_half_post_fee_pnl_usd')}/"
            f"{c539_deferred_open_digest.get('second_half_post_fee_pnl_usd')} "
            f"eligible={c539_deferred_open_digest.get('eligible')} "
            f"live_authority={c539_deferred_open_digest.get('live_authority')} "
            f"bars={c539_deferred_open_digest.get('required_bars')}"
        ),
        *([cohort_accrual_line] if cohort_accrual_line else []),
        (
            "temporal_profitability: "
            f"dow={temporal_digest.get('dow_weight_status')} "
            f"dead_band_empty={temporal_digest.get('current_dead_band_empty_confirmed')} "
            f"wallets={temporal_digest.get('wallets_total')} "
            f"history={temporal_digest.get('wallets_with_resolved_btc5m_history')} "
            f"candidates={temporal_digest.get('dead_band_candidate_count')} "
            f"feed={temporal_digest.get('watch_tier_feed_count')} "
            f"top={_short_wallet(str(temporal_digest.get('top_candidate_wallet') or ''))} "
            f"score={temporal_digest.get('top_candidate_score')} "
            f"class={temporal_digest.get('top_candidate_classification')} "
            f"slice_wd={((temporal_digest.get('top_candidate_slice_labels') or {}).get('weekday') or {}).get('label')} "
            f"slice_we={((temporal_digest.get('top_candidate_slice_labels') or {}).get('weekend') or {}).get('label')} "
            f"dead_roi={(temporal_digest.get('top_candidate_dead_band') or {}).get('roi_pct')} "
            f"source_replay=batch:{source_active_replay_digest.get('batch_id')},"
            f"replayed:{source_active_replay_digest.get('wallets_replayed')}/"
            f"{source_active_replay_digest.get('targets_selected')} "
            f"raw:{source_active_replay_digest.get('raw_rows_seen')},"
            f"btc5m_buys:{source_active_replay_digest.get('normalized_btc5m_buy_events')},"
            f"api_errors:{source_active_replay_digest.get('api_error_wallets')},"
            f"stops:{source_active_replay_digest.get('stop_reason_counts')},"
            f"manifest:{source_active_replay_digest.get('manifest_files')},"
            f"skipped_seen:{source_active_replay_digest.get('already_replayed_wallets_skipped')} "
            "source_active_cohort:"
            f"wallets={source_active_cohort_digest.get('wallet_count')} "
            f"cumulative={source_active_cohort_digest.get('source_active_cumulative')} "
            f"latest_pass={source_active_cohort_digest.get('latest_pass')} "
            f"external_track_at={((source_active_cohort_digest.get('external_liveness_cumulative') or {}).get('generated_at'))} "
            "a3e0_midnight_bundle:"
            f"status={a3e0_midnight_bundle_digest.get('status')} "
            f"generated_at={a3e0_midnight_bundle_digest.get('generated_at')} "
            f"arm_at={a3e0_midnight_bundle_digest.get('activate_not_before_utc')} "
            f"live_mutation={a3e0_midnight_bundle_digest.get('live_mutation_before_arm')} "
            f"gate={((a3e0_midnight_bundle_digest.get('current_gate') or {}).get('status'))} "
            "focused_candidate_p1:"
            f"wallet={_short_wallet(str(focused_candidate_p1_digest.get('wallet') or ''))} "
            f"history={((focused_candidate_p1_digest.get('history_depth') or {}).get('status'))} "
            f"resolved={((focused_candidate_p1_digest.get('old_vs_new') or {}).get('old_packet_resolved'))}->"
            f"{((focused_candidate_p1_digest.get('old_vs_new') or {}).get('new_deep_resolved'))} "
            f"pnl={((focused_candidate_p1_digest.get('old_vs_new') or {}).get('old_packet_pnl_usd'))}->"
            f"{((focused_candidate_p1_digest.get('old_vs_new') or {}).get('new_deep_pnl_usd'))} "
            f"delta={((focused_candidate_p1_digest.get('old_vs_new') or {}).get('pnl_delta_usd'))} "
            f"temporal={((focused_candidate_p1_digest.get('temporal_hour_match') or {}).get('status'))} "
            f"top1={((focused_candidate_p1_digest.get('concentration') or {}).get('top1_positive_pnl_share_pct'))} "
            f"top3={((focused_candidate_p1_digest.get('concentration') or {}).get('top3_positive_pnl_share_pct'))} "
            f"discounted={((focused_candidate_p1_digest.get('concentration') or {}).get('concentration_discounted'))} "
            f"verdict={((focused_candidate_p1_digest.get('decision') or {}).get('verdict'))}"
        ),
        (
            "winner_variation_siblings: "
            f"status={winner_variation_digest.get('status')} "
            f"paper_only={winner_variation_digest.get('paper_only')} "
            f"live_allowed={winner_variation_digest.get('live_orders_allowed')} "
            f"wallet={_short_wallet(str(winner_variation_digest.get('source_wallet') or ''))} "
            f"policy={winner_variation_digest.get('policy_id')} "
            f"epoch={winner_variation_digest.get('epoch_id')} "
            f"lanes={winner_variation_digest.get('lane_count')} "
            f"siblings={winner_variation_digest.get('freshness_siblings_s')} "
            f"orders={winner_variation_digest.get('ledger_orders_in_epoch')} "
            f"best={winner_variation_digest.get('best_sibling_lane_id')} "
            f"diff={winner_variation_digest.get('best_sibling_roi_diff_pp')} "
            f"n={winner_variation_digest.get('best_sibling_resolved_fills')} "
            f"gate={winner_variation_digest.get('best_sibling_gate_status')}"
        ),
        (
            "temporal_watch_tier_probe_apply: "
            f"status={temporal_probe_apply_digest.get('status')} "
            f"feed={temporal_probe_apply_digest.get('feed_candidates')} "
            f"applied={temporal_probe_apply_digest.get('temporal_applied')} "
            f"before={temporal_probe_apply_digest.get('configured_wallets_before')} "
            f"after={temporal_probe_apply_digest.get('configured_wallets_after')} "
            f"added={len(temporal_probe_apply_digest.get('added_wallets') or [])} "
            f"present={len(temporal_probe_apply_digest.get('already_present_wallets') or [])} "
            f"skipped={[_short_wallet(str(wallet)) for wallet in (temporal_probe_apply_digest.get('skipped_wallets') or [])]}"
        ),
        (
            "watch_tier_shadow_ev: "
            f"status={watch_tier_shadow_digest.get('status')} "
            f"configured={watch_tier_shadow_digest.get('configured_wallets')} "
            f"eligible={watch_tier_shadow_digest.get('eligible_signals')} "
            f"resolved={watch_tier_shadow_digest.get('resolved_signals')} "
            f"due={watch_tier_shadow_digest.get('readmission_ruling_due')} "
            f"top={_short_wallet(str(watch_tier_shadow_digest.get('top_wallet') or ''))} "
            f"top_n={watch_tier_shadow_digest.get('top_resolved_signals')} "
            f"top_pnl={watch_tier_shadow_digest.get('top_pnl_usd')} "
            f"top_roi={watch_tier_shadow_digest.get('top_roi_pct')} "
            f"wallets_due={[_short_wallet(str(wallet)) for wallet in (watch_tier_shadow_digest.get('wallets_due') or [])[:5]]}"
        ),
        (
            "weekend_stakeout: "
            f"status={stakeout_digest.get('status')} "
            f"candidates={stakeout_digest.get('candidates')} "
            f"fresh_alerts={stakeout_digest.get('fresh_alerts')} "
            f"poll_alerts={stakeout_digest.get('poll_alerts')} "
            f"ac05_n={stakeout_digest.get('ac05_weekend_n')} "
            f"ac05_roi={stakeout_digest.get('ac05_weekend_roi_pct')} "
            f"ac05_proven={stakeout_digest.get('ac05_proven_positive')} "
            f"c03c_status={stakeout_digest.get('c03c_status')} "
            f"c03c_n={stakeout_digest.get('c03c_resolved_signals')} "
            f"c03c_roi={stakeout_digest.get('c03c_roi_pct')} "
            "order136d_early_entry="
            f"generated_at={order136d_early_entry.get('generated_at')} "
            f"coverage={(order136d_early_entry.get('pool_coverage') or {}).get('observed_intersection_pool')}/"
            f"{(order136d_early_entry.get('pool_coverage') or {}).get('pool_wallets')} "
            f"observed={(order136d_early_entry.get('pool_coverage') or {}).get('observed_wallets')} "
            f"all_rows={len((order136d_early_entry.get('all_time') or {}).get('rows') or [])} "
            f"recent_rows={len((order136d_early_entry.get('last_7d') or {}).get('rows') or [])} "
            f"lead={_short_wallet(((order136d_early_entry.get('lead_gate_verification') or {}).get('wallet')))} "
            f"lead_checks={(order136d_early_entry.get('lead_gate_verification') or {}).get('checks')}"
        ),
        (
            "sub25_accounting_spot_check: "
            f"gate={sub25_spot_digest.get('gate')} "
            f"conclusion={sub25_spot_digest.get('conclusion')} "
            f"sample={sub25_spot_digest.get('sampled_orders')} "
            f"markets={sub25_spot_digest.get('sampled_unique_markets')} "
            f"wins={sub25_spot_digest.get('wins')} "
            f"losses={sub25_spot_digest.get('losses')} "
            f"token_mismatch={sub25_spot_digest.get('token_outcome_mismatches')}"
        ),
        (
            "btc5m_fleet: "
            f"size={btc5m_fleet_digest.get('fleet_size')} "
            f"matrix_rows={btc5m_fleet_digest.get('matrix_rows')} "
            f"matrix_windows={btc5m_fleet_digest.get('matrix_windows')} "
            f"wallets_with_rows={btc5m_fleet_digest.get('fleet_wallets_with_matrix_rows')} "
            f"top50_with_rows={btc5m_fleet_top50_digest.get('with_rows')}/"
            f"{btc5m_fleet_top50_digest.get('wallets')} "
            f"top50_none={btc5m_fleet_top50_digest.get('none')} "
            f"coverage_defect={btc5m_fleet_digest.get('coverage_defect')}"
        ),
        (
            "btc5m_two_sided_prime: "
            f"events={two_sided_digest.get('accepted_events')} "
            f"windows={two_sided_digest.get('resolved_windows')} "
            f"paired={two_sided_digest.get('paired_markets')} "
            f"pair_sum={two_sided_digest.get('pair_sum_candidates')} "
            f"freq={two_sided_digest.get('pair_sum_frequency_pct')} "
            f"wallets={two_sided_digest.get('two_sided_wallets')} "
            f"top={two_sided_digest.get('top_mechanism')} "
            f"status={two_sided_digest.get('top_status')} "
            f"ev_day={two_sided_digest.get('top_ev_per_day_usd')} "
            f"oos={two_sided_digest.get('top_oos_trades')}/{two_sided_digest.get('top_oos_pnl_usd')}"
        ),
        (
            "btc5m_morning_table: "
            f"rows={morning_table_digest.get('rows')} "
            f"holdout={morning_table_digest.get('holdout_passed_rows')} "
            f"matrix_none={morning_table_digest.get('matrix_coverage_none_rows')} "
            f"top_rank={morning_table_digest.get('top_rank')} "
            f"top={morning_table_digest.get('top_mechanism')} "
            f"candidate={_short_wallet(str(morning_table_digest.get('top_candidate') or ''))} "
            f"status={morning_table_digest.get('top_status')} "
            f"ev_day={morning_table_digest.get('top_ev_per_day_usd')} "
            f"funding={morning_table_digest.get('top_proposed_funding_size_usd')}"
        ),
        (
            "btc5m_structural_scalp_lane: "
            f"fills={structural_scalp_lane_digest.get('paper_fills')} "
            f"pnl={structural_scalp_lane_digest.get('paper_pnl_usd')} "
            f"forward={structural_scalp_lane_digest.get('forward_fills')}/"
            f"{structural_scalp_lane_digest.get('forward_pnl_usd')} "
            f"span_days={structural_scalp_lane_digest.get('refresh_forward_span_days')} "
            f"gate_status={structural_scalp_lane_digest.get('live_gate_status')} "
            f"ready={structural_scalp_lane_digest.get('ready_for_live')} "
            f"live_allowed={structural_scalp_lane_digest.get('live_orders_allowed')} "
            f"refresh={structural_scalp_lane_digest.get('refresh_status')} "
            f"source={structural_scalp_lane_digest.get('history')} "
            f"source_age={structural_scalp_lane_digest.get('newest_source_event_age_s')} "
            f"fresh={structural_scalp_lane_digest.get('freshness_pass')} "
            f"cache_events={structural_scalp_lane_digest.get('cached_forward_source_events')} "
            f"frozen_audit={structural_scalp_lane_digest.get('frozen_history_audit_status')}/"
            f"{structural_scalp_lane_digest.get('frozen_history_consumer_count')}/"
            f"{structural_scalp_lane_digest.get('stale_tainted_conclusion_count')} "
            "promotion="
            f"status={structural_scalp_promotion_digest.get('status')} "
            f"gate={structural_scalp_promotion_digest.get('evidence_gate_pass')} "
            f"economics={structural_scalp_promotion_digest.get('fee_aware_economics')} "
            f"decision={structural_scalp_promotion_digest.get('decision')} "
            "volume_prep="
            f"wallet={volume_standby_promotion_digest.get('wallet')} "
            f"clock={volume_standby_promotion_digest.get('decision_clock')} "
            f"economics={volume_standby_promotion_digest.get('fee_aware_economics')} "
            f"divergence={volume_standby_promotion_digest.get('divergence_review')} "
            f"freshness={volume_standby_promotion_digest.get('precondition_input_freshness')} "
            f"branches={volume_standby_promotion_digest.get('prederived_decision_branches')} "
            f"gate={volume_standby_promotion_digest.get('evidence_gate_pass')} "
            f"decision={volume_standby_promotion_digest.get('decision')} "
            f"paper_only={volume_standby_promotion_digest.get('paper_only')} "
            f"live_mutation_allowed={volume_standby_promotion_digest.get('live_mutation_allowed')}"
        ),
        (
            "portfolio_allocator: "
            f"status={portfolio_allocator_digest.get('status')} "
            f"signals={portfolio_allocator_digest.get('signal_count')} "
            f"allocated_members={portfolio_allocator_digest.get('allocated_member_count')} "
            f"starved={portfolio_allocator_digest.get('starved_member_count')} "
            f"allocated_usd={portfolio_allocator_digest.get('allocated_usd')} "
            f"drain_s={portfolio_allocator_digest.get('estimated_drain_s')} "
            f"api_rps={portfolio_allocator_digest.get('estimated_api_requests_per_s')} "
            f"fair_floor={portfolio_allocator_digest.get('all_members_receive_floor')} "
            f"single_submitter={portfolio_allocator_digest.get('single_submitter_invariant')}"
        ),
        (
            "data_layer_v1: "
            f"status={data_layer_digest.get('status')} "
            f"rows={data_layer_digest.get('rows_converted')} "
            f"files={data_layer_digest.get('files_converted')}/"
            f"{data_layer_digest.get('files_considered')} "
            f"bytes={data_layer_digest.get('bytes_read')} "
            f"duckdb_rows={data_layer_digest.get('duckdb_rows')} "
            f"duckdb={data_layer_digest.get('duckdb_path')} "
            f"missing={data_layer_digest.get('missing_dependencies')}"
        ),
        (
            "dr_preflight: "
            f"status={dr_digest.get('status')} remote={dr_digest.get('has_push_remote')} "
            f"remote_count={dr_digest.get('remote_count')} dirty={dr_digest.get('dirty_paths')} "
            f"tracked_secrets={len(dr_digest.get('tracked_secret_paths') or [])} "
            f"branch={dr_digest.get('snapshot_branch')} "
            f"snapshot_bytes={dr_digest.get('snapshot_total_bytes')} "
            f"tracked_oversized={dr_digest.get('tracked_oversized_count')} "
            f"snapshot_rejected={dr_digest.get('snapshot_rejected_count')} "
            f"push={dr_digest.get('snapshot_push_status')} "
            f"commit={dr_digest.get('snapshot_push_commit')} "
            f"next={dr_digest.get('next_action')}"
        ),
        (
            "queue_clearance_gaps: "
            f"candidates={clearance.get('candidate_count')} gates={clearance.get('gate_counts')} "
            f"classifications={clearance.get('classification_counts')}"
        ),
        *(
            [
                "ranked_queue_clearance: "
                f"paper_only={ranked_clearance.get('paper_only')} "
                f"live_allowed={ranked_clearance.get('live_orders_allowed')} "
                f"summary={ranked_clearance.get('summary')} "
                f"parked_dormant={[(row.get('wallet'), row.get('status'), row.get('reason'), row.get('recheck_at')) for row in ranked_clearance.get('parked_dormant') or [] if isinstance(row, dict)]} "
                f"parked_reject_ratio={[(row.get('wallet'), row.get('status'), row.get('paper_orders'), row.get('attributable_reject_ratio')) for row in ranked_clearance.get('parked_reject_ratio') or [] if isinstance(row, dict)]} "
                f"packets={[(row.get('wallet'), row.get('attributable_reject_ratio'), row.get('resolved_orders'), row.get('post_fee_pnl_usd'), row.get('paper_disposition'), row.get('named_cause')) for row in ranked_clearance.get('packets') or [] if isinstance(row, dict)]}"
            ]
            if ranked_clearance.get("packets")
            else []
        ),
        (
            "ready_shadow_next: "
            f"wallet={_short_wallet(str(ready_shadow_digest.get('wallet') or ''))} "
            f"copyable={ready_shadow_digest.get('copyable_buy_events')} "
            f"copyable_gap={ready_shadow_digest.get('copyable_buy_gap')} "
            f"resolved={ready_shadow_digest.get('resolved_paper_fills')} "
            f"resolved_gap={ready_shadow_digest.get('resolved_fill_gap')} "
            f"pnl={ready_shadow_digest.get('paper_pnl_usd')} "
            f"post_fee={ready_shadow_digest.get('in_lane_post_fee_pnl_usd')} "
            f"hot_standby={ready_shadow_digest.get('hot_standby_ready')} "
            f"pending_liveness={ready_shadow_digest.get('hot_standby_pending_liveness')} "
            f"page_due={ready_shadow_digest.get('page_fable_due')} "
            f"verdict={ready_shadow_digest.get('readiness_verdict')} "
            f"ready={ready_shadow_digest.get('ready_for_live')} "
            f"shadow_status={ready_shadow_digest.get('shadow_status')} "
            f"candidate={ready_shadow_digest.get('candidate_id')} "
            f"temporal={ready_shadow_digest.get('temporal_classification')} "
            f"parity={((ready_shadow_digest.get('copyintent_parity_capture') or {}).get('status'))} "
            f"preconditions={ready_shadow_digest.get('live_canary_packet_preconditions')} "
            f"hot_all={ready_shadow_summary_digest.get('all_measurement_hot_standby_ready')} "
            f"watch_ready={ready_shadow_summary_digest.get('watch_tier_hot_standby_ready')} "
            f"watch_pending={ready_shadow_summary_digest.get('watch_tier_hot_standby_pending_liveness')} "
            f"watch_page={ready_shadow_summary_digest.get('watch_tier_page_fable_due')} "
            f"top_ready={_short_wallet(str(ready_shadow_summary_digest.get('sos_top_standby_wallet') or ''))} "
            f"top_candidate={_short_wallet(str(ready_shadow_summary_digest.get('sos_top_candidate_wallet') or ''))} "
            f"top_post_fee={ready_shadow_top_hot.get('in_lane_post_fee_pnl_usd')}"
        ),
        *(
            [
                "ready_shadow_paper_canary: "
                f"wallet={_short_wallet(str(ready_shadow_paper_canary_digest.get('wallet') or ''))} "
                f"path={ready_shadow_paper_canary_digest.get('canary_path')} "
                f"policy={ready_shadow_paper_canary_digest.get('paper_policy_id')} "
                f"enrolled={ready_shadow_paper_canary_digest.get('paper_canary_enrolled_at')} "
                f"elapsed_h={ready_shadow_paper_canary_digest.get('paper_canary_elapsed_h')}/"
                f"{ready_shadow_paper_canary_digest.get('paper_canary_minimum_h')} "
                f"parity={ready_shadow_paper_canary_digest.get('copyintent_parity_capture')} "
                f"liveness={ready_shadow_paper_canary_digest.get('source_liveness')} "
                f"preconditions={ready_shadow_paper_canary_digest.get('live_canary_packet_preconditions')} "
                f"verdict={ready_shadow_paper_canary_digest.get('readiness_verdict')} "
                f"paper_only={ready_shadow_paper_canary_digest.get('paper_only')} "
                f"live_allowed={ready_shadow_paper_canary_digest.get('live_orders_allowed')}"
            ]
            if ready_shadow_paper_canary_digest.get("wallet")
            else []
        ),
        (
            "weekday_readmission_status: "
            f"generated_at={weekday_readmission_digest.get('generated_at')} "
            f"decision={weekday_readmission_summary.get('decision')} "
            f"clock_pass={weekday_readmission_summary.get('clock_pass')} "
            f"gate_pass={weekday_readmission_summary.get('readmission_gate_pass')} "
            f"queued={weekday_readmission_summary.get('queued_for_normal_admission')} "
            f"wallets={[{
                'wallet': _short_wallet(str(row.get('source_wallet') or '')),
                'clock_h': row.get('demotion_clock_elapsed_h'),
                'resolved': ((row.get('fresh_watch_tier') or {}).get('resolved_signals')),
                'resolved_gap': ((row.get('fresh_watch_tier') or {}).get('resolved_gap')),
                'roi': ((row.get('fresh_watch_tier') or {}).get('roi_pct')),
                'trades24h': ((row.get('fresh_external_liveness') or {}).get('btc5m_trades_24h')),
                'verdict': row.get('verdict'),
            } for row in weekday_readmission_wallets if isinstance(row, dict)]}"
        ),
        *([paper_lane_line] if paper_lane_line else []),
        *(
            [
                "fee_aware_long_horizon_copy: "
                f"status={fee_aware_long_horizon_digest.get('status')} "
                f"collector_start={fee_aware_long_horizon_digest.get('collector_start_at')} "
                f"signals={fee_aware_long_horizon_digest.get('source_signals')} "
                f"intents={fee_aware_long_horizon_digest.get('copy_intents')} "
                f"observations={fee_aware_long_horizon_digest.get('observation_count')} "
                f"pending={fee_aware_long_horizon_digest.get('pending_count')} "
                f"parity={((fee_aware_long_horizon_digest.get('copy_intent_parity') or {}) if isinstance(fee_aware_long_horizon_digest.get('copy_intent_parity'), dict) else {}).get('status')}/"
                f"{((fee_aware_long_horizon_digest.get('copy_intent_parity') or {}) if isinstance(fee_aware_long_horizon_digest.get('copy_intent_parity'), dict) else {}).get('violations')} "
                f"combined_post_fee={fee_aware_long_horizon_digest.get('combined_post_fee_pnl_usd')} "
                f"paper_only={fee_aware_long_horizon_digest.get('paper_only')} "
                f"live_allowed={fee_aware_long_horizon_digest.get('live_orders_allowed')} "
                f"live_attempts={fee_aware_long_horizon_digest.get('live_order_attempts')} "
                f"expired={fee_aware_long_horizon_digest.get('expired_count')} "
                f"excluded_non_qualifying={fee_aware_long_horizon_digest.get('excluded_non_qualifying_count')}"
            ]
            if fee_aware_long_horizon_digest.get("status")
            else []
        ),
        *(
            [
                "alpha_eligible_profiles_paper: "
                f"status={alpha_eligible_profiles_digest.get('status')} "
                f"experiment={alpha_eligible_profiles_digest.get('experiment_id')} "
                f"collector_start={alpha_eligible_profiles_digest.get('collector_start_at')} "
                f"signals={alpha_eligible_profiles_digest.get('source_signals')} "
                f"intents={alpha_eligible_profiles_digest.get('copy_intents')} "
                f"pending={alpha_eligible_profiles_digest.get('pending_count')} "
                f"parity={((alpha_eligible_profiles_digest.get('copy_intent_parity') or {}) if isinstance(alpha_eligible_profiles_digest.get('copy_intent_parity'), dict) else {}).get('status')}/"
                f"{((alpha_eligible_profiles_digest.get('copy_intent_parity') or {}) if isinstance(alpha_eligible_profiles_digest.get('copy_intent_parity'), dict) else {}).get('violations')} "
                f"buys={((alpha_eligible_profiles_digest.get('summary') or {}) if isinstance(alpha_eligible_profiles_digest.get('summary'), dict) else {}).get('buy_events')} "
                f"copyable={((alpha_eligible_profiles_digest.get('summary') or {}) if isinstance(alpha_eligible_profiles_digest.get('summary'), dict) else {}).get('copyable_buy_events')} "
                f"paper_pnl={((alpha_eligible_profiles_digest.get('summary') or {}) if isinstance(alpha_eligible_profiles_digest.get('summary'), dict) else {}).get('paper_pnl_usd')} "
                f"paper_only={alpha_eligible_profiles_digest.get('paper_only')} "
                f"live_allowed={alpha_eligible_profiles_digest.get('live_orders_allowed')} "
                f"submitted={alpha_eligible_profiles_digest.get('orders_submitted')} "
                f"expired={alpha_eligible_profiles_digest.get('expired_observation_count')}"
            ]
            if alpha_eligible_profiles_digest.get("status")
            else []
        ),
        (
            "experiment_preregistration: "
            f"status={prereg_digest.get('status')} records={prereg_digest.get('valid_records')}/"
            f"{prereg_digest.get('registry_records')} active={prereg_digest.get('active_count')} "
            f"latest={prereg_digest.get('latest_experiment_id')} "
            f"deadline={prereg_digest.get('latest_deadline_utc')} "
            f"missing={prereg_digest.get('missing_required_ids')} "
            "weekend_shadows="
            f"window={weekend_window_digest.get('windows')}/"
            f"{weekend_window_digest.get('pnl_usd')}/cells_n10={weekend_window_digest.get('negative_discovery_cells_n10')}/"
            f"{weekend_window_digest.get('gate')} "
            f"hours_n20={weekend_hour_digest.get('candidate_hours_n20')}/"
            f"{weekend_hour_digest.get('gate')} "
            f"fak={fak_requote_digest.get('prospective_observations')}/"
            f"{fak_requote_digest.get('target_observations')} eligible={fak_requote_digest.get('prospective_requote_eligible')} "
            f"paper_only={weekend_shadows_digest.get('paper_only')} "
            f"live_mutated={weekend_shadows_digest.get('live_path_mutated')} "
            "fee_edge_decomposition: "
            f"verdict={fee_edge_digest.get('verdict')} winners={fee_edge_digest.get('winner_count')} "
            f"top={fee_edge_digest.get('winners', [])[:4]} "
            f"measurement_only={fee_edge_digest.get('measurement_only')} "
            f"live_mutation={fee_edge_digest.get('live_mutation')} "
            "entry_price_band_gate="
            f"enabled={entry_band_digest.get('enabled')} "
            f"bands={entry_band_digest.get('blocked_bands')} "
            f"caps_changed={entry_band_digest.get('caps_changed')} "
            f"counterfactual={entry_band_digest.get('counterfactual')} "
            "profit_latency_counterfactual="
            f"status={profit_latency_digest.get('status')} "
            f"resolved={profit_latency_digest.get('resolved_suppressed_windows')} "
            f"decision_band={profit_latency_digest.get('decision_band_60_180')} "
            f"live_mutation={profit_latency_digest.get('live_mutation')} "
            "f418_post_band_causal="
            f"status={f418_post_band_digest.get('status')} "
            f"gate={f418_post_band_digest.get('gate')} "
            f"cohorts={f418_post_band_digest.get('cohorts')} "
            f"holdout={f418_post_band_digest.get('holdout')} "
            f"worst_cells={f418_post_band_digest.get('worst_cells')} "
            "f418_spread_elasticity="
            f"status={f418_spread_digest.get('status')} "
            f"coverage={f418_spread_digest.get('coverage')} "
            f"gate={f418_spread_digest.get('gate')} "
            f"cells={f418_spread_digest.get('cells')} "
            "f418_acceptance_funnel="
            f"status={f418_acceptance_digest.get('status')} "
            f"seat_tenure={f418_acceptance_digest.get('seat_tenure')} "
            f"unattributed={f418_acceptance_digest.get('unattributed_selected_intents')} "
            "f418_green_day_conversion="
            f"verdict={f418_conversion_digest.get('verdict')} "
            f"gate={f418_conversion_digest.get('sample_gate_pass')} "
            f"bottleneck={f418_conversion_digest.get('dual_bar_bottleneck')} "
            f"control={f418_conversion_digest.get('control_pre_sign')} "
            f"post={f418_conversion_digest.get('green_sign_post')} "
            "selected_member_guard_submit_attribution="
            f"status={selected_attribution_digest.get('status')} "
            f"defect={selected_attribution_digest.get('defect_classification')} "
            f"wallets={selected_attribution_digest.get('selected_wallet_count')} "
            f"eligible={selected_attribution_digest.get('selected_policy_eligible_unique_intents')} "
            f"submitted={selected_attribution_digest.get('submitted_intents')} "
            f"telemetry={selected_attribution_digest.get('telemetry_defects')} "
            f"wiring={selected_attribution_digest.get('wiring_defects')} "
            f"stages={selected_attribution_digest.get('terminal_stage_counts')} "
            "market_buy_precision_counterfactual="
            f"status={precision_cf_digest.get('status')} "
            f"resolved={precision_cf_digest.get('resolved_suppressed_windows')}/"
            f"{precision_cf_digest.get('min_resolved_suppressed_windows')} "
            f"pnl={precision_cf_digest.get('post_fee_counterfactual_pnl_usd')} "
            f"live_mutation={precision_cf_digest.get('live_mutation')}"
        ),
        "",
        "## Open Defects From Latest Status",
    ]
    if not any(
        stakeout_digest.get(key)
        for key in (
            "status",
            "candidates",
            "fresh_alerts",
            "poll_alerts",
            "ac05_weekend_n",
            "c03c_resolved_signals",
        )
    ):
        lines = [line for line in lines if not line.startswith("weekend_stakeout: ")]
    if not same_cut_basis_digest.get("status"):
        lines = [line for line in lines if not line.startswith("scorecard_same_cut_basis: ")]
    if not pinned_tranche_digest.get("status"):
        lines = [line for line in lines if not line.startswith("pinned_tranche_economics: ")]
    if not pinned_midday_digest.get("status"):
        lines = [line for line in lines if not line.startswith("pinned_tranche_midday_due_check: ")]
    if digest.get("latest_operator_order"):
        operator_material = compact_for_header(digest.get("latest_operator_order_material") or [])
        lines.insert(
            5,
            f"latest_operator_order: {digest.get('latest_operator_order')} | "
            f"material={operator_material or 'missing'}",
        )
    lines.extend(digest["defects"] or ["- none found in latest STATUS"])
    def extend_rendered_next_block(row: dict[str, Any]) -> None:
        rendered_next = row.get("next_verbatim") or ["- next: missing"]
        if len(rendered_next) > 5:
            compact = " ".join(str(line).strip() for line in rendered_next[:3])
            lines.append(f"next_compact: {compact} ... ({len(rendered_next) - 3} more next lines in JSON)")
        else:
            lines.extend(rendered_next)

    next_lines = digest["latest_direction_next_verbatim"] or ["- next: missing from latest DIRECTION"]
    if len(next_lines) > 2:
        compact_next = " ".join(line.strip() for line in next_lines)
        lines.extend(
            [
                "",
                f"latest_direction_next_compact: {compact_next}",
            ]
        )
    else:
        lines.extend(["", "## Latest Direction Next Verbatim"])
        lines.extend(next_lines)
    if digest["directions_newer_than_latest_status"]:
        lines.extend(["", "## Directions Newer Than Latest Status"])
        for row in digest["directions_newer_than_latest_status"]:
            lines.append(str(row.get("heading") or "missing heading"))
            extend_rendered_next_block(row)
    if digest["recent_directions"]:
        lines.extend(["", "## Recent Fable Directions"])
        for row in digest["recent_directions"]:
            lines.append(str(row.get("heading") or "missing heading"))
            extend_rendered_next_block(row)
    if digest["latest_direction_material"]:
        basis = digest.get("latest_direction_material_extraction_basis")
        lines.extend(["", f"## Latest Direction Material Lines ({basis})"])
        lines.extend(digest["latest_direction_material"])

    if digest["direction_warnings"]:
        lines.extend(["", "## Latest Direction Warnings"])
        lines.extend(digest["direction_warnings"])
    if len(lines) > MAX_DIGEST_LINES:
        lines = lines[: MAX_DIGEST_LINES - 1] + ["TRUNCATED: digest exceeded line budget"]
    text = "\n".join(lines) + "\n"
    digest["line_count"] = len(lines)
    digest["line_budget_ok"] = len(lines) <= MAX_DIGEST_LINES
    return digest, text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--json-output", default=DEFAULT_JSON_OUTPUT)
    parser.add_argument("--latest-output", default=DEFAULT_LATEST_OUTPUT)
    parser.add_argument("--latest-json-output", default=DEFAULT_LATEST_JSON_OUTPUT)
    parser.add_argument("--codex-dir", default=None, help="Override ~/.codex for hermetic tests or snapshots")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    started = time.perf_counter()
    digest, text = build_digest(root, codex_dir=Path(args.codex_dir) if args.codex_dir else None)
    digest["generation_duration_s"] = round(time.perf_counter() - started, 6)
    _atomic_write(root / args.output, text)
    atomic_write_json(root / args.json_output, digest)
    _atomic_write(root / args.latest_output, text)
    atomic_write_json(root / args.latest_json_output, digest)
    print(json.dumps({
        "output": args.output,
        "json_output": args.json_output,
        "latest_output": args.latest_output,
        "latest_json_output": args.latest_json_output,
        "line_count": digest["line_count"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
