#!/usr/bin/env python3
"""Run the adaptive paper-only wallet-derived bot."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.adaptive_bot import AdaptiveBotConfig, run_adaptive_bot  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-tracking-state", default="data/research/wallet_copy_live_tracking_state.json")
    parser.add_argument("--live-tracking-event-log", default="data/research/wallet_copy_live_tracking_events.jsonl")
    parser.add_argument("--output", default="data/research/wallet_copy_adaptive_bot_state.json")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_adaptive_bot_paper_state.json")
    parser.add_argument("--paper-event-log", default="data/research/wallet_copy_adaptive_bot_paper_events.jsonl")
    parser.add_argument(
        "--single-wallet-exact-copy-paper-state",
        default="data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_state.json",
    )
    parser.add_argument(
        "--single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument(
        "--tracker-time-replay-paper-state",
        default="data/research/wallet_copy_adaptive_tracker_time_replay_paper_state.json",
    )
    parser.add_argument(
        "--tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_adaptive_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument("--max-event-log-rows", type=int, default=2000)
    parser.add_argument(
        "--min-move-generated-at-ts",
        type=float,
        default=None,
        help=(
            "Ignore tracker rows generated before this unix timestamp. Hot-lane ticks "
            "use this to keep stale event-log tail rows from masquerading as the "
            "current poll batch."
        ),
    )
    parser.add_argument("--max-observed-event-age-s", type=float, default=10.0)
    parser.add_argument("--max-observation-age-s", type=float, default=30.0)
    parser.add_argument("--min-agreeing-wallets", type=int, default=2)
    parser.add_argument("--min-signal-score-usd", type=float, default=0.5)
    parser.add_argument("--min-directional-dominance", type=float, default=0.62)
    parser.add_argument("--max-price-spread", type=float, default=0.12)
    parser.add_argument("--max-signal-cluster-age-s", type=float, default=8.0)
    parser.add_argument("--min-clob-fill-ratio", type=float, default=0.999)
    parser.add_argument("--max-order-usd", type=float, default=2.0)
    parser.add_argument("--min-order-usd", type=float, default=0.1)
    parser.add_argument("--min-source-wallet-usd", type=float, default=0.0)
    parser.add_argument("--max-tracker-time-replay-intents", type=int, default=200)
    parser.add_argument("--no-apply-paper", action="store_true")
    parser.add_argument("--no-apply-tracker-time-replay-paper", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = run_adaptive_bot(
        AdaptiveBotConfig(
            live_tracking_state_path=args.live_tracking_state,
            live_tracking_event_log_path=args.live_tracking_event_log,
            output_state_path=args.output,
            paper_state_path=args.paper_state,
            paper_event_log_path=args.paper_event_log,
            single_wallet_exact_copy_paper_state_path=args.single_wallet_exact_copy_paper_state,
            single_wallet_exact_copy_paper_event_log_path=args.single_wallet_exact_copy_paper_event_log,
            tracker_time_replay_paper_state_path=args.tracker_time_replay_paper_state,
            tracker_time_replay_paper_event_log_path=args.tracker_time_replay_paper_event_log,
            max_event_log_rows=args.max_event_log_rows,
            min_move_generated_at_ts=args.min_move_generated_at_ts,
            max_observed_event_age_s=args.max_observed_event_age_s,
            max_observation_age_s=args.max_observation_age_s,
            min_agreeing_wallets=args.min_agreeing_wallets,
            min_signal_score_usd=args.min_signal_score_usd,
            min_directional_dominance=args.min_directional_dominance,
            max_price_spread=args.max_price_spread,
            max_signal_cluster_age_s=args.max_signal_cluster_age_s,
            min_clob_fill_ratio=args.min_clob_fill_ratio,
            max_order_usd=args.max_order_usd,
            min_order_usd=args.min_order_usd,
            min_source_wallet_usd=args.min_source_wallet_usd,
            max_tracker_time_replay_intents=args.max_tracker_time_replay_intents,
            apply_paper=not args.no_apply_paper,
            apply_tracker_time_replay_paper=not args.no_apply_tracker_time_replay_paper,
        )
    )
    print(json.dumps({k: state.get(k) for k in ("status", "blockers", "summary", "paper_only", "live_orders_allowed")}, indent=2, sort_keys=True))
    return 0 if state.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
