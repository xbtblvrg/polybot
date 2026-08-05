#!/usr/bin/env python3
"""Pre-stage the standing-queue successor dossier without live mutation."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_QUEUE = ROOT / "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_ROUTING_SHADOW = ROOT / "data/research/routing_shadow_validation_latest.json"
DEFAULT_SHADOW_SEATS = ROOT / "data/research/routing_shadow_candidate_seats.json"
DEFAULT_CORRECTED_PROBE = ROOT / "data/research/corrected_copyability_probe_live_now_20260713T1317Z.json"
DEFAULT_TEMPORAL = ROOT / "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_DEADMAN_GATE = ROOT / "data/research/deadman_microprobe_corrected_gate_20260711T1912Z.json"
DEFAULT_LATEST = ROOT / "data/research/successor_dossier_latest.json"


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") else ""


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_number(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _as_float(row.get(key))
        if value is not None:
            return value
    return None


def _find_wallet(rows: Any, wallet: str) -> dict[str, Any]:
    target = wallet.lower()
    for row in _list(rows):
        if not isinstance(row, dict):
            continue
        if _wallet(row.get("wallet") or row.get("source_wallet")) == target:
            return row
    return {}


def _shadow_seat(shadow_seats: dict[str, Any], wallet: str) -> dict[str, Any]:
    return _find_wallet(shadow_seats.get("seats"), wallet)


def _safe_probe_label(value: Any, *, fallback: str = "member") -> str:
    label = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(value or ""))
    return label[:80] or fallback


def _probe_state_for_seat(shadow_seat: dict[str, Any]) -> dict[str, Any]:
    if not shadow_seat:
        return {}
    candidate_id = str(shadow_seat.get("candidate_id") or "")
    wallet = str(shadow_seat.get("source_wallet") or shadow_seat.get("wallet") or "").strip().lower()
    label = _safe_probe_label(candidate_id or wallet[-12:])
    path = ROOT / "data/research" / f"wallet_copy_live_execution_probe_{label}.json"
    return _dict(load_json(path, default={}))


def _queue_head(queue: dict[str, Any]) -> dict[str, Any]:
    for row in _list(queue.get("ranked_members")):
        if isinstance(row, dict) and row.get("ready_for_live"):
            return row
    ranked = _list(queue.get("ranked_members"))
    return ranked[0] if ranked and isinstance(ranked[0], dict) else {}


def _routing_member(routing: dict[str, Any], wallet: str) -> dict[str, Any]:
    summary = _dict(routing.get("summary"))
    retained = _dict(summary.get("fee_gate_calibration_retained"))
    by_member = _dict(retained.get("by_member"))
    return _dict(by_member.get(wallet.lower()))


def _routing_member_evidence(routing: dict[str, Any], wallet: str) -> dict[str, Any]:
    return _find_wallet(routing.get("member_evidence"), wallet)


def _candidate_routing(routing: dict[str, Any], wallet: str, shadow_seat: dict[str, Any], probe_state: dict[str, Any]) -> dict[str, Any]:
    summary = _dict(routing.get("summary"))
    member = _routing_member(routing, wallet)
    if not member:
        evidence = _routing_member_evidence(routing, wallet)
        if evidence:
            attrition = _dict(evidence.get("filter_attrition"))
            return {
                "status": "SEATED_NO_FEE_CALIBRATION_ROWS_YET",
                "validation_elapsed_hours": summary.get("validation_elapsed_hours"),
                "overall_would_submit_windows": summary.get("would_submit_windows"),
                "runtime_selected_wallet": summary.get("runtime_selected_wallet"),
                "shadow_candidate_seat_count": summary.get("shadow_candidate_seat_count"),
                "candidate_measured_windows": 0,
                "candidate_post_fee_pnl_usd": None,
                "candidate_would_fill_count": attrition.get("would_submit", 0),
                "candidate_routeable_signals": attrition.get("routeable_signals", 0),
                "candidate_probe_status": evidence.get("probe_status"),
                "candidate_fresh_candidate_intents": evidence.get("fresh_candidate_intents"),
                "candidate_fresh_candidate_intents_after_expected_fee_gate": evidence.get(
                    "fresh_candidate_intents_after_expected_fee_gate"
                ),
                "candidate_sample_intents": evidence.get("sample_intents"),
                "reason": "candidate wallet is seated in routing_shadow_validation.member_evidence but has no retained fee calibration rows yet",
            }
        if shadow_seat:
            probe_summary = _dict(probe_state.get("candidate_intent_summary"))
            return {
                "status": "SHADOW_SEAT_CONFIGURED_AWAITING_ROUTING_SHADOW_REFRESH",
                "validation_elapsed_hours": summary.get("validation_elapsed_hours"),
                "overall_would_submit_windows": summary.get("would_submit_windows"),
                "runtime_selected_wallet": summary.get("runtime_selected_wallet"),
                "shadow_candidate_seat_configured": True,
                "candidate_measured_windows": 0,
                "candidate_post_fee_pnl_usd": None,
                "candidate_would_fill_count": 0,
                "candidate_routeable_signals": 0,
                "candidate_probe_status": probe_state.get("status"),
                "candidate_fresh_candidate_intents": probe_summary.get("fresh_candidate_intents"),
                "candidate_fresh_candidate_intents_after_expected_fee_gate": probe_summary.get(
                    "fresh_candidate_intents_after_expected_fee_gate"
                ),
                "candidate_sample_intents": len(probe_summary.get("sample_intents") or []),
                "reason": "shadow seat artifact is configured and paper probe exists, but current routing_shadow_validation_latest was refreshed before the new seat appeared in member_evidence",
            }
        return {
            "status": "NOT_SEATED_IN_ROUTING_SHADOW_RETAINED",
            "validation_elapsed_hours": summary.get("validation_elapsed_hours"),
            "overall_would_submit_windows": summary.get("would_submit_windows"),
            "runtime_selected_wallet": summary.get("runtime_selected_wallet"),
            "candidate_measured_windows": None,
            "candidate_post_fee_pnl_usd": None,
            "candidate_would_fill_count": None,
            "reason": "candidate wallet absent from routing_shadow_validation.summary.fee_gate_calibration_retained.by_member",
        }
    return {
        "status": "PRESENT_IN_ROUTING_SHADOW_RETAINED",
        "validation_elapsed_hours": summary.get("validation_elapsed_hours"),
        "overall_would_submit_windows": summary.get("would_submit_windows"),
        "runtime_selected_wallet": summary.get("runtime_selected_wallet"),
        "candidate_measured_windows": member.get("measured_unique_windows"),
        "candidate_post_fee_pnl_usd": member.get("post_fee_pnl_usd"),
        "candidate_would_fill_count": member.get("fee_gated_intents"),
        "candidate_pre_fee_pnl_usd": member.get("pre_fee_pnl_usd"),
        "candidate_expected_fee_usd": member.get("expected_fee_usd_sum"),
    }


def _fee_coverage(routing: dict[str, Any], wallet: str, shadow_seat: dict[str, Any], probe_state: dict[str, Any]) -> dict[str, Any]:
    member = _routing_member(routing, wallet)
    if not member:
        evidence = _routing_member_evidence(routing, wallet)
        if evidence:
            return {
                "status": "SEATED_NO_FEE_CALIBRATION_ROWS_YET",
                "fee_gated_intents": 0,
                "measurable_resolved_intents": 0,
                "expected_fee_usd_sum": 0.0,
                "post_fee_pnl_usd": None,
                "probe_status": evidence.get("probe_status"),
                "sample_intents": evidence.get("sample_intents"),
            }
        if shadow_seat:
            probe_summary = _dict(probe_state.get("candidate_intent_summary"))
            return {
                "status": "SHADOW_SEAT_CONFIGURED_NO_FEE_ROWS_YET",
                "fee_gated_intents": 0,
                "measurable_resolved_intents": 0,
                "expected_fee_usd_sum": 0.0,
                "post_fee_pnl_usd": None,
                "probe_status": probe_state.get("status"),
                "sample_intents": len(probe_summary.get("sample_intents") or []),
            }
        return {
            "status": "NO_MEMBER_FEE_CALIBRATION_ROW",
            "fee_gated_intents": 0,
            "measurable_resolved_intents": 0,
            "expected_fee_usd_sum": 0.0,
            "post_fee_pnl_usd": None,
        }
    return {
        "status": "PRESENT",
        "fee_gated_intents": member.get("fee_gated_intents"),
        "measurable_resolved_intents": member.get("measurable_resolved_intents"),
        "resolved_intents": member.get("resolved_intents"),
        "unresolved_intents": member.get("unresolved_intents"),
        "expected_fee_usd_sum": member.get("expected_fee_usd_sum"),
        "post_fee_pnl_usd": member.get("post_fee_pnl_usd"),
    }


def _sigma_status(temporal_row: dict[str, Any]) -> dict[str, Any]:
    recent = _dict(temporal_row.get("recent"))
    n = _as_int(recent.get("resolved_trades") or recent.get("n"))
    observed_win_rate_pct = _as_float(recent.get("win_rate_pct") or recent.get("actual_win_rate_pct"))
    avg_win = _first_number(
        recent,
        (
            "avg_win_per_winner_usd",
            "avg_win_usd",
            "average_win_usd",
            "mean_win_usd",
        ),
    )
    avg_loss_abs = _first_number(
        recent,
        (
            "avg_loss_per_loser_abs_usd",
            "avg_loss_abs_usd",
            "average_loss_abs_usd",
            "mean_loss_abs_usd",
        ),
    )
    if n and n > 0 and observed_win_rate_pct is not None and avg_win and avg_loss_abs and avg_win > 0 and avg_loss_abs > 0:
        required = avg_loss_abs / (avg_loss_abs + avg_win)
        gap_pp = observed_win_rate_pct - (required * 100.0)
        sigma_pp = math.sqrt(required * (1.0 - required) / n) * 100.0
        gap_in_sigma = None if sigma_pp == 0 else gap_pp / sigma_pp
        return {
            "status": "COMPUTED",
            "n": n,
            "observed_win_rate_pct": round(observed_win_rate_pct, 6),
            "required_win_rate_pct": round(required * 100.0, 6),
            "actual_minus_required_win_rate_pp": round(gap_pp, 6),
            "gap_sigma_pp": round(sigma_pp, 6),
            "gap_in_sigma": round(gap_in_sigma, 6) if gap_in_sigma is not None else None,
            "avg_win_per_winner_usd": avg_win,
            "avg_loss_per_loser_abs_usd": avg_loss_abs,
        }
    return {
        "status": "NOT_COMPUTABLE_MISSING_BREAKEVEN_PAYOFF_SHAPE",
        "n": n,
        "observed_win_rate_pct": observed_win_rate_pct,
        "gap_sigma_pp": None,
        "gap_in_sigma": None,
        "reason": "temporal candidate row has n and wins but no avg_win/avg_loss breakeven payoff shape",
    }


def build_dossier(
    *,
    queue_path: Path,
    routing_shadow_path: Path,
    shadow_seats_path: Path,
    corrected_probe_path: Path,
    temporal_path: Path,
    deadman_gate_path: Path,
) -> dict[str, Any]:
    queue = _dict(load_json(queue_path, default={}))
    routing = _dict(load_json(routing_shadow_path, default={}))
    shadow_seats = _dict(load_json(shadow_seats_path, default={}))
    corrected_probe = _dict(load_json(corrected_probe_path, default={}))
    temporal = _dict(load_json(temporal_path, default={}))
    deadman_gate = _dict(load_json(deadman_gate_path, default={}))

    queue_row = _queue_head(queue)
    wallet = _wallet(queue_row.get("wallet"))
    corrected_row = _find_wallet(corrected_probe.get("ranked_candidates"), wallet)
    temporal_row = _find_wallet(temporal.get("wallets"), wallet)
    deadman_row = _find_wallet(deadman_gate.get("rows"), wallet)
    shadow_seat = _shadow_seat(shadow_seats, wallet)
    probe_state = _probe_state_for_seat(shadow_seat)
    routing_row = _candidate_routing(routing, wallet, shadow_seat, probe_state)
    fee_coverage = _fee_coverage(routing, wallet, shadow_seat, probe_state)
    sigma = _sigma_status(temporal_row)
    summary = {
        "flow_stage": "LIVE/DEFEND",
        "status": "PRESTAGED_NO_LIVE_CHANGE",
        "candidate_wallet": wallet,
        "candidate_id": queue_row.get("name"),
        "queue_rank": queue_row.get("queue_rank"),
        "queue_source": queue_row.get("queue_source"),
        "ready_for_live": bool(queue_row.get("ready_for_live")),
        "clearance_ready": bool(queue_row.get("clearance_ready")),
        "resolved_pnl": queue_row.get("resolved_pnl"),
        "recent_fill_windows": queue_row.get("recent_fill_windows"),
        "routing_status": routing_row.get("status"),
        "routing_measured_windows": routing_row.get("candidate_measured_windows"),
        "routing_post_fee_pnl_usd": routing_row.get("candidate_post_fee_pnl_usd"),
        "routing_would_fill_count": routing_row.get("candidate_would_fill_count"),
        "fee_coverage_status": fee_coverage.get("status"),
        "gap_sigma_status": sigma.get("status"),
        "gap_sigma_pp": sigma.get("gap_sigma_pp"),
        "gap_in_sigma": sigma.get("gap_in_sigma"),
        "live_change": False,
        "next_action": "provide dossier to Fable post-13:00Z ruling; do not promote from this packet",
    }
    return {
        "kind": "successor_dossier",
        "flow_stage": "LIVE/DEFEND",
        "generated_at": utc_now_iso(),
        "sources": {
            "queue": _rel(queue_path),
            "routing_shadow": _rel(routing_shadow_path),
            "shadow_seats": _rel(shadow_seats_path),
            "corrected_probe": _rel(corrected_probe_path),
            "temporal": _rel(temporal_path),
            "deadman_gate": _rel(deadman_gate_path),
        },
        "summary": summary,
        "queue_head": queue_row,
        "routing_shadow": routing_row,
        "fee_calibration_coverage": fee_coverage,
        "shadow_candidate_seat": shadow_seat,
        "shadow_candidate_probe": {
            "status": probe_state.get("status"),
            "generated_at": probe_state.get("generated_at"),
            "paper_only": probe_state.get("paper_only"),
            "live_orders_allowed": probe_state.get("live_orders_allowed"),
            "candidate_intent_summary": _dict(probe_state.get("candidate_intent_summary")),
        },
        "corrected_probe": {
            "btc5m_buys": corrected_row.get("btc5m_buys"),
            "btc5m_trades": corrected_row.get("btc5m_trades"),
            "inband_025_050_buy_share_pct": corrected_row.get("inband_025_050_buy_share_pct"),
            "median_buy_entry_offset_s": corrected_row.get("median_buy_entry_offset_s"),
            "latest_trade_age_h": corrected_row.get("latest_trade_age_h"),
            "p1_promotion_eligible": corrected_row.get("p1_promotion_eligible"),
            "p1_reject_reasons": corrected_row.get("p1_reject_reasons"),
        },
        "deadman_corrected_gate": {
            "admission_check_pass": deadman_row.get("admission_check_pass"),
            "btc5m_buy_count_24h": deadman_row.get("btc5m_buy_count_24h"),
            "policy_compatible_inband_buy_count_24h": deadman_row.get("policy_compatible_inband_buy_count_24h"),
            "freshest_buy_lag_s": deadman_row.get("freshest_buy_lag_s"),
            "temporal_slice_gate_pass": deadman_row.get("temporal_slice_gate_pass"),
            "fail_reasons": deadman_row.get("fail_reasons"),
        },
        "temporal_profile": {
            "classification": temporal_row.get("classification"),
            "all": temporal_row.get("all"),
            "recent": temporal_row.get("recent"),
            "weekday": _dict(temporal_row.get("regime_profiles")).get("weekday"),
            "weekend": _dict(temporal_row.get("regime_profiles")).get("weekend"),
            "slice_labels": temporal_row.get("slice_labels"),
        },
        "gap_sigma": sigma,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default=str(DEFAULT_QUEUE))
    parser.add_argument("--routing-shadow", default=str(DEFAULT_ROUTING_SHADOW))
    parser.add_argument("--shadow-seats", default=str(DEFAULT_SHADOW_SEATS))
    parser.add_argument("--corrected-probe", default=str(DEFAULT_CORRECTED_PROBE))
    parser.add_argument("--temporal", default=str(DEFAULT_TEMPORAL))
    parser.add_argument("--deadman-gate", default=str(DEFAULT_DEADMAN_GATE))
    parser.add_argument("--output", default=str(DEFAULT_LATEST))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_dossier(
        queue_path=Path(args.queue),
        routing_shadow_path=Path(args.routing_shadow),
        shadow_seats_path=Path(args.shadow_seats),
        corrected_probe_path=Path(args.corrected_probe),
        temporal_path=Path(args.temporal),
        deadman_gate_path=Path(args.deadman_gate),
    )
    output = Path(args.output)
    atomic_write_json(output, report)
    print(json.dumps({**report["summary"], "output": _rel(output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
