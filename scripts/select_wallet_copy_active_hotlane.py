#!/usr/bin/env python3
"""Select active BTC 5m wallets for a paper-only hot-lane tracker."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.hotlane import ActiveHotlaneConfig, build_active_hotlane  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--live-tracking-event-log", default="data/research/wallet_copy_live_tracking_events.jsonl")
    parser.add_argument(
        "--active-hotlane-live-tracking-state",
        default="data/research/wallet_copy_active_hotlane_live_tracking_state.json",
    )
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--leaderboard-state", default="data/research/wallet_copy_leaderboard_crypto_state.json")
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--strategy-direction-state", default="data/research/wallet_copy_strategy_direction_state.json")
    parser.add_argument("--adaptive-state", default="data/research/wallet_copy_adaptive_bot_state.json")
    parser.add_argument("--hotlane-tick-state", default="data/research/wallet_copy_hotlane_tick_state.json")
    parser.add_argument("--output-registry", default="data/research/wallet_copy_active_hotlane_registry.json")
    parser.add_argument("--output", default="data/research/wallet_copy_active_hotlane_state.json")
    parser.add_argument("--max-wallets", type=int, default=32)
    parser.add_argument("--live-log-tail-rows", type=int, default=5000)
    parser.add_argument("--recent-window-s", type=float, default=1800.0)
    parser.add_argument("--max-history-age-s", type=float, default=21600.0)
    parser.add_argument("--min-score", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = build_active_hotlane(
        ActiveHotlaneConfig(
            registry_path=args.registry,
            live_tracking_event_log_path=args.live_tracking_event_log,
            active_hotlane_live_tracking_state_path=args.active_hotlane_live_tracking_state,
            history_state_path=args.history_state,
            leaderboard_state_path=args.leaderboard_state,
            profit_state_path=args.profit_state,
            strategy_direction_state_path=args.strategy_direction_state,
            adaptive_state_path=args.adaptive_state,
            hotlane_tick_state_path=args.hotlane_tick_state,
            output_registry_path=args.output_registry,
            output_state_path=args.output,
            max_wallets=args.max_wallets,
            live_log_tail_rows=args.live_log_tail_rows,
            recent_window_s=args.recent_window_s,
            max_history_age_s=args.max_history_age_s,
            min_score=args.min_score,
        )
    )
    printed = {
        "status": state.get("status"),
        "blockers": state.get("blockers"),
        "summary": state.get("summary"),
        "output_registry": state.get("output_registry"),
        "selected_wallets": [
            {
                "address": row.get("address"),
                "score": row.get("score"),
                "live_score": row.get("live_score"),
                "history_score": row.get("history_score"),
                "leaderboard_score": row.get("leaderboard_score"),
                "profit_score": row.get("profit_score"),
                "forward_queue_score": row.get("forward_queue_score"),
                "runtime_copy_proof_score": row.get("runtime_copy_proof_score"),
                "current_partial_score": row.get("current_partial_score"),
                "replay_pass_score": row.get("replay_pass_score"),
                "inventory_replay_score": row.get("inventory_replay_score"),
                "latest_live_event_lag_s": row.get("latest_live_event_lag_s"),
                "selection_reasons": row.get("selection_reasons"),
            }
            for row in state.get("selected_wallets", [])[:20]
            if isinstance(row, dict)
        ],
        "paper_only": state.get("paper_only"),
        "live_orders_allowed": state.get("live_orders_allowed"),
    }
    print(json.dumps(printed, indent=2, sort_keys=True, default=str))
    return 0 if state.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
