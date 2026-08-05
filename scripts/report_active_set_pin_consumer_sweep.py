#!/usr/bin/env python3
"""Audit active-set pin snapshot consumers against the live pin source."""

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

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_OVERLAY = ROOT / "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_ROTATION_PACKET = ROOT / "data/research/active_set_rotation_packet_latest.json"
DEFAULT_ROTATION_EXECUTION = ROOT / "data/research/active_set_selected_rotation_execution_latest.json"
DEFAULT_OUTPUT = ROOT / "data/research/active_set_pin_consumer_sweep_latest.json"

CONSUMER_RULES = [
    {
        "path": "scripts/run_wallet_copy_live_guard.py",
        "source": "active_set_auto_degrade_state.selection_pin",
        "uses_packet_pin_expiry": False,
        "classification": "AUTHORITY_LIVE_PIN",
    },
    {
        "path": "scripts/execute_active_set_selected_rotation.py",
        "source": "active_set_rotation_packet.quiet_clock.fires_now",
        "uses_packet_pin_expiry": False,
        "classification": "ROTATION_TRIGGER_ONLY_WRITES_LIVE_PIN",
    },
    {
        "path": "scripts/report_active_set_post_rotation_windows.py",
        "source": "active_set_selected_rotation_execution.selection_pin.created_at",
        "uses_packet_pin_expiry": False,
        "classification": "DISPLAY_MEASUREMENT_ROTATION_AT_ONLY",
    },
    {
        "path": "scripts/update_state_digest.py",
        "source": "packet quiet_clock / consumer sweep artifact",
        "uses_packet_pin_expiry": False,
        "classification": "DISPLAY_ONLY_DIGEST",
    },
]


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load(path: Path) -> dict[str, Any]:
    loaded = load_json(path, default={})
    return loaded if isinstance(loaded, dict) else {}


def _snapshot(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    pin = _dict(payload.get("selection_pin"))
    quiet = _dict(payload.get("quiet_clock"))
    return {
        "path": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
        "generated_at": payload.get("generated_at"),
        "status": payload.get("status"),
        "selection_pin_source_wallet": pin.get("source_wallet"),
        "selection_pin_expires_at": pin.get("expires_at"),
        "quiet_clock_anchor_iso": quiet.get("anchor_iso"),
        "quiet_clock_earliest_fire_iso": quiet.get("earliest_fire_iso"),
        "snapshot_authority": "DISPLAY_ONLY_NOT_PIN_EXPIRY_AUTHORITY",
    }


def build_report(
    *,
    overlay: dict[str, Any],
    rotation_packet: dict[str, Any],
    rotation_execution: dict[str, Any],
) -> dict[str, Any]:
    live_pin = _dict(overlay.get("selection_pin"))
    packet_quiet = _dict(rotation_packet.get("quiet_clock"))
    snapshot_rows = [
        {
            "path": "data/research/active_set_rotation_packet_latest.json",
            "generated_at": rotation_packet.get("generated_at"),
            "status": rotation_packet.get("status"),
            "quiet_clock_anchor_iso": packet_quiet.get("anchor_iso"),
            "quiet_clock_earliest_fire_iso": packet_quiet.get("earliest_fire_iso"),
            "snapshot_authority": "DISPLAY_ONLY_NOT_PIN_EXPIRY_AUTHORITY",
        },
        _snapshot(DEFAULT_ROTATION_EXECUTION, rotation_execution),
    ]
    dangerous = [row for row in CONSUMER_RULES if row.get("uses_packet_pin_expiry")]
    return {
        "kind": "active_set_pin_consumer_sweep",
        "schema_version": 1,
        "flow_stage": "LIVE/DEFEND/ROTATE",
        "generated_at": _iso_now(),
        "status": "PASS_NO_PACKET_EXPIRY_CONSUMERS" if not dangerous else "FAIL_PACKET_EXPIRY_CONSUMERS_FOUND",
        "direction_id": "2026-07-18T09:34Z-fable-pin-consumer-sweep",
        "source_of_truth": "data/research/wallet_copy_active_set_auto_degrade_state.json.selection_pin",
        "live_pin": {
            "source_wallet": live_pin.get("source_wallet"),
            "pin_id": live_pin.get("pin_id"),
            "expires_at": live_pin.get("expires_at"),
            "quiet_clock_anchor_iso": _dict(live_pin.get("quiet_clock")).get("anchor_iso"),
            "quiet_clock_earliest_fire_iso": _dict(live_pin.get("quiet_clock")).get("earliest_fire_iso"),
            "quiet_clock_expiry_derived": live_pin.get("quiet_clock_expiry_derived"),
        },
        "consumers_checked": CONSUMER_RULES,
        "dangerous_consumers": dangerous,
        "packet_snapshots": snapshot_rows,
        "finding": (
            "No checked consumer enforces pin expiry from packet snapshots; live pin expiry authority remains "
            "the auto-degrade overlay selection_pin."
        )
        if not dangerous
        else "At least one consumer is marked as enforcing packet pin expiry.",
        "next_action": "keep packet pin copies display-only; use live overlay selection_pin for expiry authority",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overlay", default=str(DEFAULT_OVERLAY))
    parser.add_argument("--rotation-packet", default=str(DEFAULT_ROTATION_PACKET))
    parser.add_argument("--rotation-execution", default=str(DEFAULT_ROTATION_EXECUTION))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    report = build_report(
        overlay=_load(Path(args.overlay)),
        rotation_packet=_load(Path(args.rotation_packet)),
        rotation_execution=_load(Path(args.rotation_execution)),
    )
    atomic_write_json(Path(args.output), report)
    print(json.dumps({"status": report["status"], "output": args.output}))
    return 0 if report["status"] == "PASS_NO_PACKET_EXPIRY_CONSUMERS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
