#!/usr/bin/env python3
"""Execute a selected-member active-set rotation from a validated packet."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_PACKET = ROOT / "data/research/active_set_rotation_packet_latest.json"
DEFAULT_OVERLAY = ROOT / "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/active_set_selected_rotation_execution_latest.json"
DEFAULT_TARGET_WALLET = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
DEFAULT_DIRECTION_ID = "2026-07-18T05:56Z-fable-qc1-f418-selected-rotation"
SELECTION_PIN_MAX_TTL_S = 3300


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") else ""


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _now(now_iso: str | None = None) -> datetime:
    parsed = _parse_ts(now_iso)
    return parsed if parsed is not None else datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _ranked_target(packet: dict[str, Any], target_wallet: str) -> dict[str, Any]:
    for row in _list(packet.get("ranked_candidates")):
        if isinstance(row, dict) and _wallet(row.get("source_wallet")) == target_wallet:
            return row
    return {}


def _overlay_member(overlay: dict[str, Any], target_wallet: str) -> dict[str, Any]:
    latest = overlay.get("latest_admission")
    if isinstance(latest, dict) and _wallet(latest.get("source_wallet") or latest.get("wallet")) == target_wallet:
        return latest
    for row in _list(overlay.get("members")):
        if isinstance(row, dict) and _wallet(row.get("source_wallet") or row.get("wallet")) == target_wallet:
            return row
    return {}


def execute_rotation(
    *,
    packet: dict[str, Any],
    overlay: dict[str, Any],
    target_wallet: str = DEFAULT_TARGET_WALLET,
    direction_id: str = DEFAULT_DIRECTION_ID,
    now_iso: str | None = None,
    pin_ttl_s: int = SELECTION_PIN_MAX_TTL_S,
) -> tuple[dict[str, Any], dict[str, Any]]:
    target_wallet = _wallet(target_wallet)
    quiet = _dict(packet.get("quiet_clock"))
    if quiet.get("fires_now") is not True:
        raise ValueError("rotation packet quiet_clock.fires_now is not true")
    if _wallet(packet.get("presumptive_target")) != target_wallet:
        raise ValueError("rotation packet presumptive_target does not match requested target")
    target_row = _ranked_target(packet, target_wallet)
    if not target_row:
        raise ValueError("target wallet is absent from ranked_candidates")
    member = _overlay_member(overlay, target_wallet)
    if not member:
        raise ValueError("target wallet is absent from active-set overlay")

    now = _now(now_iso)
    ttl = max(1, min(int(pin_ttl_s), SELECTION_PIN_MAX_TTL_S))
    expires = now + timedelta(seconds=ttl)
    pin = {
        "enabled": True,
        "pin_id": "qc1-f418-selected-rotation",
        "direction_id": direction_id,
        "created_at": _iso(now),
        "expires_at": _iso(expires),
        "candidate_id": member.get("candidate_id") or target_row.get("candidate_id"),
        "source_wallet": target_wallet,
        "reason": "QC-1 quiet-clock fired; mechanically rotate selected member to f418 per Fable 2026-07-18T05:56Z",
    }
    updated_overlay = dict(overlay)
    updated_overlay["selection_pin"] = pin
    updated_overlay["updated_at"] = _iso(now)

    edge_snapshot = {
        "source_wallet": target_wallet,
        "candidate_id": target_row.get("candidate_id"),
        "fresh_matching_events_4h": target_row.get("fresh_matching_events_4h"),
        "fresh_matching_event_rate_per_hour": target_row.get("fresh_matching_event_rate_per_hour"),
        "local_entry_latency_p50_s": target_row.get("local_entry_latency_p50_s"),
        "local_entry_latency_p90_s": target_row.get("local_entry_latency_p90_s"),
        "routing_shadow_post_fee_pnl_usd": target_row.get("routing_shadow_post_fee_pnl_usd"),
        "routing_shadow_measured_unique_windows": target_row.get("routing_shadow_measured_unique_windows"),
        "routing_shadow_fee_gated_intents": target_row.get("routing_shadow_fee_gated_intents"),
        "premerge_new_matching_events": target_row.get("premerge_new_matching_events"),
        "premerge_rtds_catchup_lag_s": target_row.get("premerge_rtds_catchup_lag_s"),
    }
    report = {
        "kind": "active_set_selected_rotation_execution",
        "schema_version": 1,
        "status": "SELECTED_ROTATION_PIN_WRITTEN",
        "flow_stage": "LIVE/ROTATE/DEFEND",
        "generated_at": _iso(now),
        "direction_id": direction_id,
        "target_wallet": target_wallet,
        "target_candidate_id": pin["candidate_id"],
        "selection_pin": pin,
        "quiet_clock": quiet,
        "edge_snapshot": edge_snapshot,
        "live_path_mutated": False,
        "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains the only live order submitter",
        "post_rotation_obligation": (
            "record dominant_skip_reason distribution and observed-age histogram (<=3s vs >3s) "
            "for f418's first 3 measured active windows"
        ),
    }
    return updated_overlay, report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", default=str(DEFAULT_PACKET))
    parser.add_argument("--overlay", default=str(DEFAULT_OVERLAY))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--target-wallet", default=DEFAULT_TARGET_WALLET)
    parser.add_argument("--direction-id", default=DEFAULT_DIRECTION_ID)
    parser.add_argument("--now")
    parser.add_argument("--pin-ttl-s", type=int, default=SELECTION_PIN_MAX_TTL_S)
    args = parser.parse_args()

    overlay_path = Path(args.overlay)
    output_path = Path(args.output)
    updated_overlay, report = execute_rotation(
        packet=load_json(args.packet, default={}) or {},
        overlay=load_json(overlay_path, default={}) or {},
        target_wallet=args.target_wallet,
        direction_id=args.direction_id,
        now_iso=args.now,
        pin_ttl_s=args.pin_ttl_s,
    )
    atomic_write_json(overlay_path, updated_overlay)
    atomic_write_json(output_path, report)
    print(json.dumps({"status": report["status"], "target_wallet": report["target_wallet"], "output": str(output_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
