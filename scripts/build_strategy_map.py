#!/usr/bin/env python3
"""Build the one-page strategy map for every registered BTC5M direction."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402

DEFAULT_MECHANISMS = "docs/agents/MECHANISMS.md"
DEFAULT_JSON = "data/research/strategy_map_latest.json"
DEFAULT_MD = "data/research/strategy_map_latest.md"
STALE_HOURS = 48.0

ARTIFACT_HINTS: dict[str, list[str]] = {
    "copy-1to1-taker": ["wallet_copy_ready_shadow_lanes_state.json", "wallet_copy_full_pool_member_queue.json"],
    "copy-drip-inventory": ["wallet_copy_daily_scorecard_2026-07-10_current.json", "state_digest.json"],
    "copy-selected-wallet-first-freshpoll": ["wallet_copy_active_set_dataapi_poller_state.json"],
    "copy-auto-degrade-admission-selection-priority": ["wallet_copy_live_guard_state.json"],
    "copy-selected-wallet-wss-priority-intake": ["polygon_ws_dataapi_active_set_comparison.jsonl"],
    "copy-policy-compatible-fresh-member-selection": ["wallet_copy_live_guard_state.json"],
    "copy-last-successful-nondenied-member-priority": ["wallet_copy_live_guard_state.json"],
    "copy-parallel-half-size-second-member": ["member_factory_kpi_state.json"],
    "copy-dual-member-fresh-coverage-router": ["campaign_lat_p1_packet_latest.json"],
    "copy-selection-pin-overrides-policy-compatible": ["wallet_copy_selection_priority_freeze_state.json"],
    "copy-winner-single-parameter-variation-engine": ["wallet_copy_winner_variation_siblings_latest.json"],
    "copy-two-sided-inventory": ["btc5m_two_sided_prime_study_latest.json"],
    "copy-mass-lln-portfolio": ["btc5m_live_paper_fleet_latest.json", "portfolio_allocator_latest.json"],
    "structural-pair-sum-arb": ["btc5m_pair_sum_forward_prereg_checkin_latest.json", "experiment_preregistration_latest.json"],
    "structural-intra-window-scalp": ["btc5m_structural_scalp_paper_lane_state.json"],
    "structural-btc5m-cross-exchange-offset-execution-matrix": [
        "btc5m_cross_exchange_comparator_matrix_state.json"
    ],
    "structural-e7-spot-lag": ["e7_spot_open_paper_lane_state.json"],
    "structural-maker-lp": ["maker_first_btc5m_paper_state.json", "maker_first_btc5m_book_aware_state.json"],
    "signal-aggregate-whale-side": ["e6_whale_net_flow_paper_lane_state.json"],
    "signal-consensus": ["whale_consensus_paper_state.json", "decision2_combined_burnin_latest.json"],
    "signal-inventory-e4": ["wallet_copy_inventory_e4_parking_latest.json", "wallet_copy_inventory_paper_state.json"],
    "signal-whale-exit-fade": ["e7_spot_open_e6_exit_backtest_20260706.json"],
    "signal-book-imbalance": ["maker_first_btc5m_book_aware_state.json"],
    "signal-early-fade-e9": ["btc5m_late_window_penny_watcher_state.json"],
    "signal-fair-value-e10": ["current_btc5m_asset_ids_latest.json"],
    "signal-momentum-e11": ["e11_cross_window_momentum_book_aware_state.json"],
    "signal-hour-bias-e14": ["member_dow_profiles_latest.json"],
    "signal-ensemble": ["campaign_lat_p1_packet_latest.json"],
    "signal-toxicity-filter": ["fill_toxicity_report_latest.json", "wallet_copy_live_guard_state.json"],
    "extracted-decompiler-rules": ["wallet_copy_strategy_decompiler_intake_latest.json"],
    "extracted-followability-targets": ["wallet_copy_followability_leaderboard_latest.json", "state_digest.json"],
    "structural-c4-early-pass": ["inventory_skip_lifecycle_trace_latest.json"],
    "copy-closed-day-dual-bar-gap-attribution-shadow": ["wallet_copy_daily_scorecard_2026-07-20_final.json"],
    "copy-f418-post-fak-drought-first-accept-quality-shadow": ["f418_acceptance_funnel_latest.json"],
    "copy-f418-green-day-submitted-to-filled-conversion-shadow": ["f418_green_day_conversion_shadow_latest.json"],
    "copy-f418-size-clamp-fee-leak-ev-shadow": ["f418_size_clamp_fee_leak_shadow_latest.json"],
    "copy-member-native-policy-acceptance-uplift-shadow": [
        "member_native_policy_acceptance_uplift_shadow_latest.json"
    ],
    "copy-top10-direct-clob-paper-lane": [
        "wallet_copy_top10_broad_paper_measurement_state.json"
    ],
    "copy-market-buy-precision-feasibility-shadow": ["market_buy_precision_counterfactual_latest.json"],
    "copy-profit-latency-suppression-opportunity-shadow": [
        "profit_latency_suppression_counterfactual_latest.json"
    ],
    "copy-entry-price-loss-band-counterfactual-shadow": [
        "entry_price_loss_band_gate_counterfactual_latest.json"
    ],
    "copy-f418-post-band-gate-residual-loss-causal-shadow": [
        "f418_post_band_gate_residual_loss_causal_shadow_latest.json"
    ],
    "copy-f418-window-time-60s-near-miss-ev-shadow": [
        "f418_window_time_60s_near_miss_ev_shadow_latest.json"
    ],
    "copy-ranked-successor-exact-policy-resolution-shadow": [
        "ranked_successor_exact_policy_resolution_shadow_latest.json"
    ],
    "copy-qualified-pool-orderfilled-resident-stakeout": [
        "copy_qualified_pool_orderfilled_resident_stakeout_state.json"
    ],
    "copy-inventory-convergence-counterfactual-shadow": [
        "inventory_convergence_skip_paper_lane_latest.json"
    ],
    "copy-routing-shadow-validation-lane": ["routing_shadow_validation_latest.json"],
    "copy-wide-positive-slice-prospective-family": [
        "wide_positive_slice_family_state.json"
    ],
    "copy-wide-multiwallet-consensus-slice-shadow": [
        "wide_multiwallet_consensus_state.json"
    ],
    "copy-wide-sequential-quorum-slice-shadow": [
        "wide_sequential_quorum_state.json"
    ],
    "structural-btc5m-multivenue-lead-consensus-shadow": [
        "btc5m_multivenue_residual_matrix_state.json"
    ],
    "structural-btc5m-cross-venue-residual-leadlag": [
        "btc5m_multivenue_residual_matrix_state.json"
    ],
    "structural-btc5m-native-complement-lead-lag-taker": [
        "btc5m_native_complement_lead_lag_taker_state.json"
    ],
    "structural-btc5m-polymarket-cross-asset-leader-lag": [
        "btc5m_polymarket_cross_asset_leader_lag_state.json"
    ],
    "structural-btc5m-polymarket-first-leader-cross-asset-lag": [
        "btc5m_polymarket_first_leader_cross_asset_lag_state.json"
    ],
    "structural-btc5m-native-signed-tape-imbalance-stale-ask": [
        "btc5m_native_signed_tape_imbalance_stale_ask_state.json"
    ],
    "structural-btc5m-native-l2-microprice-displacement-stale-ask": [
        "btc5m_native_l2_microprice_displacement_stale_ask_state.json"
    ],
    "structural-btc5m-native-l2-tob-pressure-imbalance": [
        "btc5m_native_l2_tob_pressure_imbalance_state.json"
    ],
    "structural-btc5m-native-l2-cross-outcome-parity-stale-ask": [
        "btc5m_native_l2_cross_outcome_parity_stale_ask_state.json"
    ],
    "structural-btc5m-native-l2-depth-weighted-microprice-parity-stale-ask": [
        "btc5m_native_l2_depth_weighted_microprice_parity_stale_ask_state.json"
    ],
    "structural-btc5m-native-l2-complement-bid-support-parity-stale-ask": [
        "btc5m_native_l2_complement_bid_support_parity_stale_ask_state.json"
    ],
    "structural-btc5m-native-l2-complement-ask-cap-parity-stale-ask": [
        "btc5m_native_l2_complement_ask_cap_parity_stale_ask_state.json"
    ],
    "copy-coacceptance-eligibility-shadow-lane": ["coacceptance_eligibility_shadow_latest.json"],
    "copy-probe-fill-quality-shadow-lane": ["probe_fill_quality_shadows_latest.json"],
    "copy-toxicity-deny-cell-counterfactual-lane": [
        "wallet_copy_toxicity_deny_cell_counterfactual_latest.json"
    ],
    "copy-floor-opportunity-cost-counterfactual-lane": [
        "floor_opportunity_cost_counterfactual_state.json"
    ],
    "copy-927f-forward-exact-policy-paper": [
        "927f_forward_live_tracking_state.json",
        "current_admission_wave_latest.json",
    ],
    "copy-a689-forward-exact-policy-paper": [
        "a689_forward_live_tracking_state.json",
        "current_admission_wave_latest.json",
    ],
    "e5-delayed-offset-side-selective-maker-shadow": [
        "e5_delayed_offset_paper_state.json",
        "e5_delayed_offset_book_aware_state.json",
    ],
    "CAMPAIGN-LAT": ["campaign_lat_p1_packet_latest.json", "campaign_lat_packet_latest.json"],
    "CAMPAIGN-BREADTH": ["member_factory_kpi_state.json"],
    "CAMPAIGN-SAMPLE": ["campaign_lat_p1_packet_latest.json"],
    "CAMPAIGN-CAPACITY": ["wallet_copy_live_guard_state.json", "state_digest.json"],
}

RUNTIME_BINDINGS: dict[str, dict[str, Any]] = {
    "structural-btc5m-native-l2-cross-outcome-parity-stale-ask": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-native-l2-cross-outcome-parity-stale-ask-paper",
        "freshness_slo_s": 10,
        "ownership": "paper-only dual-L2 cross-outcome parity producer; sole guard retains live authority",
    },
    "structural-btc5m-native-l2-tob-pressure-imbalance": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-native-l2-tob-pressure-imbalance-paper",
        "freshness_slo_s": 10,
        "ownership": "paper-only receipt-clock TOB-pressure producer; sole guard retains live authority",
    },
    "structural-btc5m-native-l2-microprice-displacement-stale-ask": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-native-l2-microprice-displacement-stale-ask-paper",
        "freshness_slo_s": 10,
        "ownership": "paper-only receipt-clock dual-L2 producer; sole guard retains live authority",
    },
    "structural-btc5m-native-signed-tape-imbalance-stale-ask": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-native-signed-tape-imbalance-stale-ask-paper",
        "freshness_slo_s": 10,
        "ownership": "paper-only BTC-native signed-tape producer; sole guard retains live authority",
    },
    "structural-btc5m-polymarket-first-leader-cross-asset-lag": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-polymarket-first-leader-cross-asset-lag-paper",
        "freshness_slo_s": 10,
        "ownership": "paper-only native first ETH-or-SOL leader producer; sole guard retains live authority",
    },
    "structural-btc5m-polymarket-cross-asset-leader-lag": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-polymarket-cross-asset-leader-lag-paper",
        "freshness_slo_s": 10,
        "ownership": "paper-only native ETH+SOL leader producer; sole guard retains live authority",
    },
    "structural-btc5m-native-complement-lead-lag-taker": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.native-complement-lead-lag-taker",
        "freshness_slo_s": 10,
        "ownership": "paper-only receipt-integrity producer; sole live guard retains submission authority",
    },
    "copy-routing-shadow-validation-lane": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-live-guard",
        "freshness_slo_s": 180,
        "ownership": "sole guard recurring shadow cadence; report-only and zero-submit",
    },
    "structural-btc5m-multivenue-lead-consensus-shadow": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.btc5m-multivenue-residual-matrix",
        "freshness_slo_s": 60,
    },
    "structural-btc5m-cross-venue-residual-leadlag": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.btc5m-multivenue-residual-matrix",
        "freshness_slo_s": 60,
    },
    "structural-btc5m-cross-exchange-offset-execution-matrix": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.btc5m-cross-exchange-comparator-matrix",
        "freshness_slo_s": 60,
        "productive_cell_artifact": "data/research/btc5m_cross_exchange_comparator_matrix_state.json",
        "productive_cell_field": "productive_cell_count",
    },
    "copy-wide-positive-slice-prospective-family": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wide-positive-slice-family",
        "freshness_slo_s": 30,
        "ownership": "paper-only immutable 4c94 slice family; sole guard retains live authority",
    },
    "copy-wide-multiwallet-consensus-slice-shadow": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wide-multiwallet-consensus",
        "freshness_slo_s": 30,
        "productive_cell_artifact": "data/research/wide_multiwallet_consensus_state.json",
        "productive_cell_field": "productive_lane_count",
        "ownership": "two checksum-isolated paper cells; sole guard retains live authority",
    },
    "copy-wide-sequential-quorum-slice-shadow": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wide-sequential-quorum",
        "freshness_slo_s": 30,
        "productive_cell_artifact": "data/research/wide_sequential_quorum_state.json",
        "productive_cell_field": "productive_lane_count",
        "ownership": "isolated 30s sequential-quorum paper cell; sole guard retains live authority",
    },
    "copy-qualified-pool-orderfilled-resident-stakeout": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.copy-qualified-pool-orderfilled-stakeout",
        "freshness_slo_s": 30,
    },
    "copy-ranked-successor-exact-policy-resolution-shadow": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.ranked-successor-exact-policy-resolution-shadow",
        "freshness_slo_s": 300,
    },
    "copy-f418-window-time-60s-near-miss-ev-shadow": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.f418-window-time-60s-near-miss-ev-shadow",
        "freshness_slo_s": 300,
    },
    "copy-f418-post-band-gate-residual-loss-causal-shadow": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.brainless-ops",
        "freshness_slo_s": 900,
    },
    "copy-member-native-policy-acceptance-uplift-shadow": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.member-native-policy-uplift-paper",
        "freshness_slo_s": 180,
    },
    "copy-top10-direct-clob-paper-lane": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.top10-direct-clob-paper",
        "freshness_slo_s": 60,
    },
    "copy-927f-forward-exact-policy-paper": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-927f-forward-paper",
        "freshness_slo_s": 180,
    },
    "copy-a689-forward-exact-policy-paper": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-a689-forward-paper",
        "freshness_slo_s": 180,
    },
    "e5-delayed-offset-side-selective-maker-shadow": {
        "kind": "launchd",
        "label": "com.belavarga.polymarket.wallet-copy-e5-delayed-offset-paper",
        "freshness_slo_s": 180,
    },
}


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000.0
        try:
            return datetime.fromtimestamp(raw, tz=UTC)
        except (OSError, ValueError, OverflowError):
            return None
    text = str(value or "").strip()
    if not text:
        return None
    if text.isdigit():
        return _parse_ts(float(text))
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC)


def _age_hours(ts: datetime | None, *, now: datetime) -> float | None:
    if ts is None:
        return None
    return round(max(0.0, (now - ts).total_seconds() / 3600.0), 3)


def _weekday_age_hours(ts: datetime | None, *, now: datetime) -> float | None:
    if ts is None:
        return None
    if ts > now:
        return 0.0
    hours = 0.0
    cursor = ts
    while cursor < now:
        next_hour = min(now, cursor.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        if cursor.weekday() < 5:
            hours += (next_hour - cursor).total_seconds() / 3600.0
        cursor = next_hour
    return round(hours, 3)


def _markdown_table_rows(text: str, heading: str) -> list[dict[str, str]]:
    lines = text.splitlines()
    start = -1
    for idx, line in enumerate(lines):
        if line.strip() == heading:
            start = idx + 1
            break
    if start < 0:
        return []
    table_lines: list[str] = []
    for line in lines[start:]:
        stripped = line.strip()
        if stripped.startswith("## ") and table_lines:
            break
        if stripped.startswith("|"):
            table_lines.append(stripped)
        elif table_lines and stripped:
            break
    if len(table_lines) < 3:
        return []
    headers = [cell.strip() for cell in table_lines[0].strip("|").split("|")]
    rows: list[dict[str, str]] = []
    for raw in table_lines[2:]:
        cells = [cell.strip() for cell in raw.strip("|").split("|")]
        if len(cells) != len(headers):
            continue
        rows.append(dict(zip(headers, cells, strict=True)))
    return rows


def _walk(value: Any, prefix: str = "") -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            rows.extend(_walk(item, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(value, list):
        for idx, item in enumerate(value[:20]):
            rows.extend(_walk(item, f"{prefix}[{idx}]"))
    else:
        rows.append((prefix, value))
    return rows


def _num(value: Any) -> float | None:
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _first_metric(payload: Any, keys: tuple[str, ...]) -> tuple[str, float] | None:
    for path, value in _walk(payload):
        leaf = re.sub(r".*[.\[]", "", path).rstrip("]")
        if leaf in keys:
            number = _num(value)
            if number is not None:
                return path, number
    return None


def _status_from_registry(row: dict[str, str], payload: Any) -> str:
    raw = (row.get("status") or "").upper()
    if row.get("enemy") or row.get("baseline metric"):
        return "PAPER"
    if row.get("id") == "copy-drip-inventory":
        return "LIVE"
    if "PARK" in raw:
        return "PARKED"
    if "DEAD" in raw or "FAIL" in raw:
        return "DEAD"
    if isinstance(payload, dict) and payload.get("live_orders_allowed") is True:
        return "LIVE"
    if "RUN" in raw:
        return "PAPER"
    if "PRIME" in raw or "SEED" in raw:
        return "GATED"
    return raw or "UNKNOWN"


def _payload_contains(payload: Any, needle: str) -> bool:
    text = json.dumps(payload, sort_keys=True, default=str).lower() if payload is not None else ""
    return needle.lower() in text


def _artifact_candidates(row_id: str, data_dir: Path) -> list[Path]:
    candidates = [data_dir / name for name in ARTIFACT_HINTS.get(row_id, [])]
    safe = row_id.lower().replace("-", "_")
    candidates.extend(sorted(data_dir.glob(f"*{safe}*latest*.json")))
    return [path for path in candidates if path.exists()]


def _artifact_payload(path: Path) -> Any:
    if path.suffix == ".json":
        return _load_json(path)
    return None


def _generated_at(payload: Any, path: Path) -> datetime | None:
    if isinstance(payload, dict):
        for key in ("generated_at", "updated_at", "created_at"):
            parsed = _parse_ts(payload.get(key))
            if parsed is not None:
                return parsed
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return None


def _next_gate(row: dict[str, str], payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("next_action", "next"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:220]
        actions = payload.get("recommended_next_actions")
        if isinstance(actions, list) and actions and isinstance(actions[0], dict):
            return str(actions[0].get("id") or actions[0].get("action") or "")[:220]
    return (row.get("live_path") or "evidence gate").strip()


def _clock(payload: Any) -> str:
    if isinstance(payload, dict):
        for path, value in _walk(payload):
            leaf = path.split(".")[-1].lower()
            if "deadline" in leaf or leaf.endswith("due") or "evaluation_due" in leaf:
                text = str(value or "").strip()
                if text:
                    return text[:80]
    return ""


def _row_from_registry(row: dict[str, str], *, data_dir: Path, now: datetime, row_type: str) -> dict[str, Any]:
    row_id = row.get("id") or row.get("campaign_id") or ""
    artifacts = _artifact_candidates(row_id, data_dir)
    artifact = artifacts[0] if artifacts else None
    payload = _artifact_payload(artifact) if artifact else None
    generated_at = _generated_at(payload, artifact) if artifact else None
    weekday_only = _payload_contains(payload, "weekday-only") or _payload_contains(payload, "WEEKDAY-ONLY")
    age = _weekday_age_hours(generated_at, now=now) if weekday_only else _age_hours(generated_at, now=now)
    status = _status_from_registry(row, payload)
    evidence_metric = _first_metric(
        payload,
        (
            "resolved_fills",
            "fills",
            "orders",
            "paper_orders",
            "resolved_orders",
            "resolved_buy_events",
            "measurable_resolved_intents",
            "would_submit_windows",
            "windows_traded",
            "selected_wallets",
            "candidate_total",
        ),
    )
    ev_metric = _first_metric(
        payload,
        ("pnl_usd", "paper_pnl_usd", "post_fee_pnl_usd", "ev_day", "roi_pct", "overlay_delta_usd"),
    )
    stale_applies = status in {"LIVE", "PAPER", "GATED"}
    stale = stale_applies and (age is None or age > STALE_HOURS)
    runner_binding = dict(RUNTIME_BINDINGS.get(row_id) or {})
    artifact_text = (
        str(artifact.relative_to(ROOT))
        if artifact and artifact.is_relative_to(ROOT)
        else str(artifact or "")
    )
    if runner_binding and artifact_text:
        runner_binding["evidence_artifact"] = artifact_text
    return {
        "id": row_id,
        "type": row_type,
        "family": row.get("family") or row.get("enemy") or "",
        "mechanism": row.get("mechanism") or row.get("hypothesis") or "",
        "paper_lane_id": row.get("paper_lane_id") or row.get("paper/shadow lane") or "",
        "status": status,
        "latest_evidence_number": f"{evidence_metric[0]}={evidence_metric[1]}" if evidence_metric else "",
        "current_ev_estimate": f"{ev_metric[0]}={ev_metric[1]}" if ev_metric else "",
        "next_gate": _next_gate(row, payload),
        "clock": _clock(payload),
        "artifact": artifact_text,
        "runner_binding": runner_binding or None,
        "evidence_generated_at": generated_at.isoformat().replace("+00:00", "Z") if generated_at else "",
        "freshness_age_h": age,
        "freshness_clock": "WEEKDAY_ONLY_CALENDAR_AWARE" if weekday_only else "WALL_CLOCK",
        "stale_applies": stale_applies,
        "freshness": "STALE" if stale else "FRESH",
        "defect": "strategy_evidence_stale_gt_48h" if stale else "",
    }


def build_map(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    mechanisms_path = Path(args.mechanisms)
    mechanisms_path = mechanisms_path if mechanisms_path.is_absolute() else root / mechanisms_path
    data_dir = Path(args.data_dir)
    data_dir = data_dir if data_dir.is_absolute() else root / data_dir
    now = _parse_ts(args.now_iso) if args.now_iso else datetime.now(tz=UTC)
    if now is None:
        now = datetime.now(tz=UTC)
    text = mechanisms_path.read_text(encoding="utf-8")
    registry = _markdown_table_rows(text, "## Registry")
    campaigns = _markdown_table_rows(text, "## Enemy Campaigns")
    rows = [
        *[_row_from_registry(row, data_dir=data_dir, now=now, row_type="mechanism") for row in registry],
        *[_row_from_registry({"id": row.get("id", ""), **row}, data_dir=data_dir, now=now, row_type="campaign") for row in campaigns],
    ]
    stale_rows = [row for row in rows if row.get("freshness") == "STALE"]
    active_rows = [row for row in rows if row.get("status") in {"LIVE", "PAPER", "GATED"}]
    return {
        "kind": "strategy_map",
        "flow_stage": "SELF-DEV/LEARN",
        "generated_at": _utc_now_iso(),
        "source": str(mechanisms_path.relative_to(root)) if mechanisms_path.is_relative_to(root) else str(mechanisms_path),
        "authority": "READING_AID_NOT_AUTHORITY_FULL_CONTEXT_REQUIRED",
        "runtime_budget": "wide-lane scorecard cadence; report-only; no live-path mutation",
        "stale_threshold_hours": STALE_HOURS,
        "summary": {
            "rows": len(rows),
            "active_or_gated_rows": len(active_rows),
            "stale_rows": len(stale_rows),
            "fresh_rows": len(rows) - len(stale_rows),
            "stale_is_defect": bool(stale_rows),
        },
        "rows": rows,
        "stale_defects": [
            {
                "id": row["id"],
                "artifact": row["artifact"],
                "freshness_age_h": row["freshness_age_h"],
                "next": "refresh evidence artifact or write tombstone/parking ref",
            }
            for row in stale_rows
        ],
        "next_action": "feed this map into the EVOI re-rank and refresh/tombstone every STALE row",
    }


def _md_escape(value: Any) -> str:
    text = str(value if value is not None else "")
    return text.replace("|", "\\|").replace("\n", " ")[:220]


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Strategy Map",
        f"generated_at: {payload.get('generated_at')}",
        f"authority: {payload.get('authority')}",
        f"runtime_budget: {payload.get('runtime_budget')}",
        f"rows: {payload['summary']['rows']} fresh: {payload['summary']['fresh_rows']} stale: {payload['summary']['stale_rows']}",
        "",
        "| id | type | status | evidence | EV | next gate | clock | freshness | artifact |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in payload.get("rows", []):
        lines.append(
            "| "
            + " | ".join(
                _md_escape(row.get(key))
                for key in (
                    "id",
                    "type",
                    "status",
                    "latest_evidence_number",
                    "current_ev_estimate",
                    "next_gate",
                    "clock",
                    "freshness",
                    "artifact",
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mechanisms", default=DEFAULT_MECHANISMS)
    parser.add_argument("--data-dir", default="data/research")
    parser.add_argument("--output-json", default=DEFAULT_JSON)
    parser.add_argument("--output-md", default=DEFAULT_MD)
    parser.add_argument("--now-iso", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_map(ROOT, args)
    output_json = Path(args.output_json)
    output_json = output_json if output_json.is_absolute() else ROOT / output_json
    output_md = Path(args.output_md)
    output_md = output_md if output_md.is_absolute() else ROOT / output_md
    atomic_write_json(output_json, payload)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps({"output": str(output_json), "markdown": str(output_md), **payload["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
