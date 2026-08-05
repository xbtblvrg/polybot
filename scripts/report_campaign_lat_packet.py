#!/usr/bin/env python3
"""Build the CAMPAIGN-LAT P1 observation-latency/coverage packet.

Flow stage: LIVE/LEARN/SELF-DEV. This is evidence aggregation only. It
does not alter routing, eligibility, wallets, prices, or live order flow.
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

from src.wallet_copy.store import atomic_write_json, atomic_write_text, load_json  # noqa: E402


DEFAULT_COVERAGE_GAP = "data/research/coverage_gap_diagnosis_latest.json"
DEFAULT_SIGNAL_SUPPLY = "data/research/coverage_gap_signal_supply_check_latest.json"
DEFAULT_ROUTING_DISAMBIGUATION = "data/research/routing_disambiguation_latest.json"
DEFAULT_STATE_DIGEST = "data/research/state_digest.json"
DEFAULT_ROUTING_SHADOW = "data/research/routing_shadow_validation_latest.json"
DEFAULT_STAGE2_VERDICT_GLOB = "data/research/stage2_freeze_verdict_*.json"
DEFAULT_STAGE2_FREEZE_TARGET_UTC = "2026-07-10T20:30:00Z"
DEFAULT_OUTPUT = "data/research/campaign_lat_p1_packet_latest.json"
DEFAULT_MARKDOWN_OUTPUT = "data/research/campaign_lat_p1_packet_latest.md"
OP_VOLUME_TARGET_WINDOWS = 144


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rooted(path: str | Path) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _short_wallet(wallet: Any) -> str:
    text = str(wallet or "")
    return f"{text[:6]}...{text[-4:]}" if text.startswith("0x") and len(text) >= 10 else text


def _latest_stage2_verdict(path_or_glob: str) -> dict[str, Any]:
    if not path_or_glob:
        return {}
    rooted = _rooted(path_or_glob)
    if rooted.exists():
        return _as_dict(load_json(rooted, default={}))
    matches = sorted(ROOT.glob(path_or_glob) if not rooted.is_absolute() else rooted.parent.glob(rooted.name))
    if not matches:
        return {}
    return _as_dict(load_json(matches[-1], default={}))


def _parse_utc_iso(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _state_digest_volume(digest: dict[str, Any]) -> dict[str, Any]:
    volume = _as_dict(digest.get("volume"))
    denominator = _int(volume.get("denominator_windows"), 288)
    windows_submitted = _int(volume.get("windows_submitted"))
    windows_filled = _int(volume.get("windows_filled"))
    return {
        "source_generated_at": digest.get("generated_at"),
        "windows_filled": windows_filled,
        "windows_submitted": windows_submitted,
        "denominator_windows": denominator,
        "target_windows": OP_VOLUME_TARGET_WINDOWS,
        "submitted_gap_to_target": max(0, OP_VOLUME_TARGET_WINDOWS - windows_submitted),
        "filled_gap_to_target": max(0, OP_VOLUME_TARGET_WINDOWS - windows_filled),
        "missed_active_windows": _int(volume.get("missed_active_windows")),
        "incident_triggered": bool(volume.get("incident_triggered")),
    }


def _coverage_summary(coverage_gap: dict[str, Any]) -> dict[str, Any]:
    summary = _as_dict(coverage_gap.get("summary"))
    submitted = _int(summary.get("submitted_windows"))
    target = _int(summary.get("op_volume_target_windows"), OP_VOLUME_TARGET_WINDOWS)
    return {
        "source_generated_at": coverage_gap.get("generated_at"),
        "window": _as_dict(coverage_gap.get("window")),
        "windows_total": _int(summary.get("windows_total")),
        "submitted_windows": submitted,
        "submitted_gap_to_target": max(0, target - submitted),
        "zero_submission_windows": _int(summary.get("zero_submission_windows")),
        "unobserved_zero_submission_windows": _int(summary.get("unobserved_zero_submission_windows")),
        "dominant_reason_class": str(summary.get("dominant_reason_class") or ""),
        "reason_class_counts": _as_dict(summary.get("reason_class_counts")),
        "status": "TRAILING_24H_TARGET_CLEAR" if submitted >= target else "TRAILING_24H_UNDER_TARGET",
    }


def _signal_supply_summary(signal_supply: dict[str, Any]) -> dict[str, Any]:
    summary = _as_dict(signal_supply.get("summary"))
    return {
        "source_generated_at": signal_supply.get("generated_at"),
        "unobserved_no_signal_windows": _int(summary.get("unobserved_no_signal_windows")),
        "sources_traded_but_unobserved_windows": _int(summary.get("sources_traded_but_unobserved_windows")),
        "sources_idle_windows": _int(summary.get("sources_idle_windows")),
        "unknown_fetch_incomplete_windows": _int(summary.get("unknown_fetch_incomplete_windows")),
        "fetch_complete": bool(summary.get("fetch_complete")),
        "dominant_class": str(summary.get("dominant_class") or ""),
        "root_cause": str(summary.get("root_cause") or ""),
        "wallet_hit_counts": _as_dict(summary.get("wallet_hit_counts")),
        "traded_unobserved_hour_utc_histogram": _as_dict(summary.get("traded_but_unobserved_hour_utc_histogram")),
    }


def _routing_summary(routing_disambiguation: dict[str, Any]) -> dict[str, Any]:
    summary = _as_dict(routing_disambiguation.get("summary"))
    selected_wallet = str(summary.get("selected_wallet_at_report_time") or "")
    return {
        "source_generated_at": routing_disambiguation.get("generated_at"),
        "candidate_windows": _int(summary.get("candidate_windows")),
        "sampled_windows": _int(summary.get("sampled_windows")),
        "dominant_class": str(summary.get("dominant_class") or ""),
        "class_counts": _as_dict(summary.get("class_counts")),
        "target_wallet_candidate_id": summary.get("target_wallet_candidate_id"),
        "target_wallet_active_runtime_member": bool(summary.get("target_wallet_active_runtime_member")),
        "target_wallet_active_rtds_watch": bool(summary.get("target_wallet_active_rtds_watch")),
        "selected_wallet_at_report_time": selected_wallet,
        "selected_wallet_short": _short_wallet(selected_wallet),
        "selection_mode": summary.get("selection_mode"),
    }


def _routing_shadow_summary(routing_shadow: dict[str, Any]) -> dict[str, Any]:
    summary = _as_dict(routing_shadow.get("summary"))
    measurement = _as_dict(summary.get("extra_would_submit_post_fee_measurement"))
    return {
        "source_generated_at": routing_shadow.get("generated_at"),
        "copyintent_parity_status": summary.get("copyintent_parity_status"),
        "copyintent_parity_conflicts": _int(summary.get("copyintent_parity_conflicts")),
        "would_submit_windows": _int(summary.get("would_submit_windows")),
        "extra_would_submit_windows": _int(summary.get("extra_would_submit_windows")),
        "measured_unique_windows": _int(measurement.get("measured_unique_windows")),
        "post_fee_pnl_usd": round(_num(measurement.get("post_fee_pnl_usd")), 6),
        "pre_fee_pnl_usd": round(_num(measurement.get("pre_fee_pnl_usd")), 6),
        "wins": _int(measurement.get("wins")),
        "losses": _int(measurement.get("losses")),
        "gate_result": measurement.get("gate_result"),
    }


def _latency_summary(digest: dict[str, Any]) -> dict[str, Any]:
    speed = _as_dict(digest.get("runtime_speed_baseline"))
    metrics = _as_dict(speed.get("metrics"))
    return {
        "source_generated_at": speed.get("generated_at") or digest.get("generated_at"),
        "status": speed.get("status"),
        "signal_age_p50_s": metrics.get("signal_age_p50_s"),
        "signal_age_p90_s": metrics.get("signal_age_p90_s"),
        "signal_to_order_p50_s": metrics.get("signal_to_order_p50_s"),
        "signal_to_order_p90_s": metrics.get("signal_to_order_p90_s"),
        "guard_cycle_recent_p50_s": metrics.get("guard_cycle_recent_p50_s"),
        "regression_count": _int(speed.get("regression_count")),
        "regressions": speed.get("regressions") if isinstance(speed.get("regressions"), list) else [],
    }


def build_packet(
    *,
    coverage_gap: dict[str, Any],
    signal_supply: dict[str, Any],
    routing_disambiguation: dict[str, Any],
    state_digest: dict[str, Any],
    routing_shadow: dict[str, Any],
    stage2_verdict: dict[str, Any],
    stage2_freeze_target_utc: str = DEFAULT_STAGE2_FREEZE_TARGET_UTC,
    generated_at: str,
) -> dict[str, Any]:
    live_volume = _state_digest_volume(state_digest)
    coverage = _coverage_summary(coverage_gap)
    signal = _signal_supply_summary(signal_supply)
    routing = _routing_summary(routing_disambiguation)
    latency = _latency_summary(state_digest)
    routing_shadow_packet = _routing_shadow_summary(routing_shadow)

    target_gap = int(live_volume["submitted_gap_to_target"])
    live_status = "LIVE_DAY_UNDER_TARGET" if target_gap > 0 else "LIVE_DAY_TARGET_CLEAR"
    generated_dt = _parse_utc_iso(generated_at)
    freeze_target_dt = _parse_utc_iso(stage2_freeze_target_utc)
    freeze_due = bool(generated_dt and freeze_target_dt and generated_dt >= freeze_target_dt)
    interim_verdict = stage2_verdict.get("verdict")
    freeze_status = (
        "DUE_OR_PAST_TARGET"
        if freeze_due
        else "NOT_DUE_INTERIM_ONLY"
        if interim_verdict
        else "NOT_DUE_NO_INTERIM"
    )
    stage2_summary_verdict = interim_verdict if freeze_due else (
        f"INTERIM_{interim_verdict}_NOT_FREEZE_OF_RECORD" if interim_verdict else "NOT_DUE"
    )
    selection_visibility_gap = (
        signal["root_cause"] == "participation_rollup_retention_gap_not_source_ingest"
        and routing["dominant_class"] == "signal-emitted-but-not-selected"
    )
    primary_constraint = (
        "retention_selection_visibility"
        if selection_visibility_gap
        else signal["root_cause"] or coverage["dominant_reason_class"] or routing["dominant_class"]
    )
    actions = [
        {
            "id": "D1_RETENTION_PACKET",
            "flow_stage": "LEARN/SELF-DEV",
            "action": "derive coverage packets from retained wallet history or retain >=288 BTC5M rollup rows so no-signal windows do not age out",
            "success_metric": "sources_traded_but_unobserved_windows decreases without reducing submitted_windows or CopyIntent parity",
            "live_path": "none",
        },
        {
            "id": "SELECTION_VISIBILITY_PACKET",
            "flow_stage": "LEARN",
            "action": "pre-register a paper/shadow router test that explains when 4d8b-style signals are not selected under last-successful-member priority",
            "success_metric": "sampled signal-emitted-but-not-selected windows receive explicit selector reasons and measured would-submit PnL",
            "live_path": "none until gated canary",
        },
        {
            "id": "CAMPAIGN_LAT_REFRESH",
            "flow_stage": "SELF-DEV",
            "action": "refresh coverage_gap, signal_supply, routing_disambiguation, routing_shadow, and speed_baseline before any eligibility proposal",
            "success_metric": "packet inputs are current and no runtime_speed regression remains unexplained",
            "live_path": "none",
        },
    ]
    if coverage["status"] == "TRAILING_24H_TARGET_CLEAR":
        actions.append(
            {
                "id": "NO_ELIGIBILITY_LOOSENING",
                "flow_stage": "LIVE",
                "action": "do not loosen eligibility from this packet; current trailing coverage is above 144 submitted windows and Stage-2 freeze-of-record is not passed in this packet",
                "success_metric": "producing live path remains unchanged while paper/shadow evidence improves",
                "live_path": "explicit Fable gate plus canary only",
            }
        )

    return {
        "schema_version": 1,
        "kind": "campaign_lat_p1_packet",
        "campaign_id": "CAMPAIGN-LAT",
        "enemy": "observation latency / coverage",
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "paper_only": True,
        "live_orders_allowed": False,
        "producing_live_mutation": False,
        "generated_at": generated_at,
        "status": "P1_PACKET_READY",
        "summary": {
            "primary_constraint": primary_constraint,
            "live_day_status": live_status,
            "live_day_submitted_gap_to_144": target_gap,
            "trailing_24h_coverage_status": coverage["status"],
            "trailing_24h_submitted_gap_to_144": coverage["submitted_gap_to_target"],
            "signal_supply_dominant": signal["dominant_class"],
            "routing_dominant": routing["dominant_class"],
            "copyintent_parity_status": routing_shadow_packet["copyintent_parity_status"],
            "stage2_verdict": stage2_summary_verdict,
            "stage2_interim_verdict": interim_verdict,
            "stage2_freeze_of_record_status": freeze_status,
            "stage2_freeze_target_utc": stage2_freeze_target_utc,
            "next_decision": "paper/shadow evidence only; no live eligibility loosening from this packet",
        },
        "live_day_volume": live_volume,
        "trailing_24h_coverage": coverage,
        "signal_supply": signal,
        "routing_disambiguation": routing,
        "routing_shadow": routing_shadow_packet,
        "latency_speed": latency,
        "stage2_freeze": {
            "source_generated_at": stage2_verdict.get("generated_at"),
            "interim_verdict": interim_verdict,
            "freeze_of_record_status": freeze_status,
            "freeze_target_utc": stage2_freeze_target_utc,
            "evidence": _as_dict(stage2_verdict.get("evidence")),
        },
        "recommended_next_actions": actions,
        "sources": {
            "coverage_gap": DEFAULT_COVERAGE_GAP,
            "signal_supply": DEFAULT_SIGNAL_SUPPLY,
            "routing_disambiguation": DEFAULT_ROUTING_DISAMBIGUATION,
            "state_digest": DEFAULT_STATE_DIGEST,
            "routing_shadow": DEFAULT_ROUTING_SHADOW,
            "stage2_verdict": DEFAULT_STAGE2_VERDICT_GLOB,
        },
    }


def render_markdown(packet: dict[str, Any]) -> str:
    summary = _as_dict(packet.get("summary"))
    live = _as_dict(packet.get("live_day_volume"))
    coverage = _as_dict(packet.get("trailing_24h_coverage"))
    signal = _as_dict(packet.get("signal_supply"))
    routing = _as_dict(packet.get("routing_disambiguation"))
    stage2 = _as_dict(packet.get("stage2_freeze"))
    latency = _as_dict(packet.get("latency_speed"))
    lines = [
        "# CAMPAIGN-LAT P1 Packet",
        "",
        f"generated_at: {packet.get('generated_at')}",
        "flow_stage: LIVE/LEARN/SELF-DEV",
        "live_orders_allowed: false",
        "producing_live_mutation: false",
        "",
        "## Summary",
        f"- primary_constraint: {summary.get('primary_constraint')}",
        f"- live_day: submitted={live.get('windows_submitted')}/288 filled={live.get('windows_filled')}/288 gap_to_144={live.get('submitted_gap_to_target')}",
        f"- trailing_24h: submitted={coverage.get('submitted_windows')}/{coverage.get('windows_total')} gap_to_144={coverage.get('submitted_gap_to_target')} status={coverage.get('status')}",
        f"- signal_supply: dominant={signal.get('dominant_class')} traded_unobserved={signal.get('sources_traded_but_unobserved_windows')} idle={signal.get('sources_idle_windows')} root={signal.get('root_cause')}",
        f"- routing: sampled={routing.get('sampled_windows')} dominant={routing.get('dominant_class')} selected={routing.get('selected_wallet_short')}",
        f"- stage2: interim={stage2.get('interim_verdict')} freeze_of_record={stage2.get('freeze_of_record_status')} target={stage2.get('freeze_target_utc')} measured={_as_dict(stage2.get('evidence')).get('measured_unique_windows')} post_fee={_as_dict(stage2.get('evidence')).get('post_fee_pnl_usd')}",
        f"- latency_speed: status={latency.get('status')} signal_p90={latency.get('signal_age_p90_s')} signal_to_order_p90={latency.get('signal_to_order_p90_s')} regressions={latency.get('regression_count')}",
        "",
        "## Next Actions",
    ]
    for action in packet.get("recommended_next_actions") or []:
        if not isinstance(action, dict):
            continue
        lines.append(f"- {action.get('id')}: {action.get('action')}")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-gap", default=DEFAULT_COVERAGE_GAP)
    parser.add_argument("--signal-supply", default=DEFAULT_SIGNAL_SUPPLY)
    parser.add_argument("--routing-disambiguation", default=DEFAULT_ROUTING_DISAMBIGUATION)
    parser.add_argument("--state-digest", default=DEFAULT_STATE_DIGEST)
    parser.add_argument("--routing-shadow", default=DEFAULT_ROUTING_SHADOW)
    parser.add_argument("--stage2-verdict", default=DEFAULT_STAGE2_VERDICT_GLOB)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--markdown-output", default=DEFAULT_MARKDOWN_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    packet = build_packet(
        coverage_gap=_as_dict(load_json(_rooted(args.coverage_gap), default={})),
        signal_supply=_as_dict(load_json(_rooted(args.signal_supply), default={})),
        routing_disambiguation=_as_dict(load_json(_rooted(args.routing_disambiguation), default={})),
        state_digest=_as_dict(load_json(_rooted(args.state_digest), default={})),
        routing_shadow=_as_dict(load_json(_rooted(args.routing_shadow), default={})),
        stage2_verdict=_latest_stage2_verdict(str(args.stage2_verdict)),
        generated_at=_utc_now_iso(),
    )
    atomic_write_json(_rooted(args.output), packet)
    atomic_write_text(_rooted(args.markdown_output), render_markdown(packet))
    print(json.dumps(packet["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
