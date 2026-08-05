#!/usr/bin/env python3
"""Refresh own-wallet position visibility and redemption deadman state."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.own_positions import build_positions_report, update_redeem_deadman


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data/research/own_positions_latest.json")
    parser.add_argument("--deadman-state", default="data/research/own_position_deadman_state.json")
    parser.add_argument("--redeemer-state", default="data/research/own_redeemer_state.json")
    parser.add_argument("--ledger", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--redeem-events", default="data/research/own_redeem_events.jsonl")
    parser.add_argument("--handoff", default="docs/agents/HANDOFF.md")
    parser.add_argument("--timeout-s", type=float, default=8.0)
    parser.add_argument("--threshold-usd", type=float, default=20.0)
    parser.add_argument("--threshold-age-s", type=float, default=3600.0)
    parser.add_argument("--write-handoff-on-incident", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_positions_report(
        output_path=Path(args.output),
        ledger_path=Path(args.ledger),
        resolutions_path=Path(args.resolutions),
        redeem_events_path=Path(args.redeem_events),
        timeout_s=float(args.timeout_s),
    )
    deadman = update_redeem_deadman(
        report=report,
        state_path=Path(args.deadman_state),
        redeemer_state_path=Path(args.redeemer_state),
        threshold_usd=float(args.threshold_usd),
        threshold_age_s=float(args.threshold_age_s),
        handoff_path=Path(args.handoff),
        append_handoff_on_incident=bool(args.write_handoff_on_incident),
    )
    payload = {
        "status": report.get("status"),
        "generated_at": report.get("generated_at"),
        "summary": report.get("summary"),
        "deadman": deadman,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if deadman.get("status") != "INCIDENT_REDEEMABLE_LOCKED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
