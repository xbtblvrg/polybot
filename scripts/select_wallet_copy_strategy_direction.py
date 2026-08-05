#!/usr/bin/env python3
"""Rank the viable wallet-copy strategy directions from persisted evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402
from src.wallet_copy.strategy_selection import build_strategy_direction_state_from_paths  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet-analysis-state", default="data/research/wallet_copy_wallet_analysis_state.json")
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--active-hotlane-state", default="data/research/wallet_copy_active_hotlane_state.json")
    parser.add_argument(
        "--active-tracking-state",
        default="data/research/wallet_copy_active_hotlane_live_tracking_state.json",
    )
    parser.add_argument(
        "--hotlane-tick-state",
        default="data/research/wallet_copy_hotlane_tick_state.json",
    )
    parser.add_argument(
        "--candidate-runtime-proof-index",
        default="data/research/wallet_copy_candidate_runtime_proof_index.json",
    )
    parser.add_argument("--source-route-state", default="data/research/wallet_copy_source_route_state.json")
    parser.add_argument("--leaderboard-state", default="data/research/wallet_copy_leaderboard_crypto_state.json")
    parser.add_argument(
        "--previous-strategy-state",
        default=None,
        help="Prior strategy state used to detect lane stagnation; defaults to --output before overwrite.",
    )
    parser.add_argument("--output", default="data/research/wallet_copy_strategy_direction_state.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_strategy_direction_state_from_paths(
        wallet_analysis_path=args.wallet_analysis_state,
        profit_state_path=args.profit_state,
        active_hotlane_state_path=args.active_hotlane_state,
        active_tracking_state_path=args.active_tracking_state,
        hotlane_tick_state_path=args.hotlane_tick_state,
        candidate_runtime_proof_index_path=args.candidate_runtime_proof_index,
        source_route_state_path=args.source_route_state,
        leaderboard_state_path=args.leaderboard_state,
        previous_strategy_state_path=args.previous_strategy_state or args.output,
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["decision"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
