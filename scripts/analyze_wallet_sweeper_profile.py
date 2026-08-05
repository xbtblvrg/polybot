#!/usr/bin/env python3
"""Analyze BTC 5-minute wallets for post-resolution sweeper behavior."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.sweeper import analyze_sweeper_profiles  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--output", default="data/research/wallet_copy_sweeper_profile_state.json")
    parser.add_argument("--high-price-threshold", type=float, default=0.95)
    parser.add_argument("--queue-band-s", type=float, default=30.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = load_json(args.history_state, default={})
    events = state.get("events") if isinstance(state, dict) else []
    if not isinstance(events, list):
        events = []
    payload = analyze_sweeper_profiles(
        [row for row in events if isinstance(row, dict)],
        high_price_threshold=args.high_price_threshold,
        queue_band_s=args.queue_band_s,
    )
    payload["inputs"] = {
        "history_state": args.history_state,
        "event_count": len(events),
    }
    atomic_write_json(args.output, payload)
    print(
        json.dumps(
            {
                "output": args.output,
                "strong_sweeper_wallets": [
                    {
                        "wallet_name": row["wallet_name"],
                        "address": row["address"],
                        "buy_events": row["buy_events"],
                        "high_price_near_or_post_close_buy_events": row[
                            "high_price_near_or_post_close_buy_events"
                        ],
                        "high_price_near_or_post_close_buy_pct": row[
                            "high_price_near_or_post_close_buy_pct"
                        ],
                    }
                    for row in payload["strong_sweeper_wallets"]
                ],
                "profile_count": len(payload["profiles"]),
                "paper_only": True,
                "live_orders_allowed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
