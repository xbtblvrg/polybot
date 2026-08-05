import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from scripts.retrace_self_feed_full_ledger import build_report


def _candidate(tx: str, condition: str, outcome: str, *, cost: float, pnl: float) -> dict:
    return {
        "classification": "true_unrecorded_fill_candidate",
        "tx": tx,
        "condition_ids": [condition],
        "market_slugs": [f"btc-updown-5m-{condition[-3:]}"],
        "outcomes": [outcome],
        "cost_usd": cost,
        "pnl_usd": pnl,
    }


def test_full_ledger_retrace_classifies_duplicates_b1_and_b2(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    rows = [
        _candidate("0xdup", "cond-100", "Up", cost=1.0, pnl=0.5),
        _candidate("0xb1", "cond-200", "Down", cost=2.0, pnl=-0.25),
        _candidate("0xb2", "cond-300", "Up", cost=3.0, pnl=1.25),
        _candidate("0xopp", "cond-400", "Up", cost=4.0, pnl=2.0),
    ]
    (data / "classification.json").write_text(json.dumps({"rows": rows}))
    (data / "self.json").write_text(
        json.dumps(
            {
                "self_missing_ledger_rows": [
                    {
                        "tx": "0xdup",
                        "self_feed": {
                            "condition_ids": ["cond-100"],
                            "market_slugs": ["btc-updown-5m-100"],
                            "outcomes": ["Up"],
                            "sources": ["data_api_trades_user"],
                            "avg_price": 0.5,
                            "size": 2.0,
                            "min_event_ts": 1000.0,
                        },
                    },
                    {
                        "tx": "0xb1",
                        "self_feed": {
                            "condition_ids": ["cond-200"],
                            "market_slugs": ["btc-updown-5m-200"],
                            "outcomes": ["Down"],
                            "sources": ["data_api_trades_user"],
                            "avg_price": 0.4,
                            "size": 5.0,
                            "min_event_ts": 2000.0,
                        },
                    },
                    {
                        "tx": "0xb2",
                        "self_feed": {
                            "condition_ids": ["cond-300"],
                            "market_slugs": ["btc-updown-5m-300"],
                            "outcomes": ["Up"],
                            "sources": ["polygon_orderfilled"],
                            "order_ids": ["0xexternal"],
                            "avg_price": 0.6,
                            "size": 5.0,
                            "min_event_ts": 3000.0,
                        },
                    },
                    {
                        "tx": "0xopp",
                        "self_feed": {
                            "condition_ids": ["cond-400"],
                            "market_slugs": ["btc-updown-5m-400"],
                            "outcomes": ["Up"],
                            "sources": ["data_api_trades_user"],
                            "avg_price": 0.8,
                            "size": 5.0,
                            "min_event_ts": 4000.0,
                        },
                    },
                ]
            }
        )
    )
    (data / "live.json").write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "final_status": "FILLED",
                        "condition_id": "cond-100",
                        "market_slug": "btc-updown-5m-100",
                        "outcome": "Up",
                        "submitted_at": "1970-01-01T00:16:40+00:00",
                        "limit_price": 0.5,
                        "lifecycle": [
                            {
                                "status": "LIVE_FILLED",
                                "payload": {
                                    "response_filled_size_usd": 1.0,
                                    "response_fill_size_shares": 2.0,
                                    "response_fill_price": 0.5,
                                },
                            }
                        ],
                    },
                    {
                        "final_status": "REJECTED",
                        "condition_id": "cond-200",
                        "market_slug": "btc-updown-5m-200",
                        "outcome": "Down",
                        "submitted_at": "1970-01-01T00:33:20+00:00",
                    },
                    {
                        "final_status": "FILLED",
                        "condition_id": "cond-400",
                        "market_slug": "btc-updown-5m-400",
                        "outcome": "Down",
                        "submitted_at": "1970-01-01T01:06:40+00:00",
                        "limit_price": 0.8,
                        "lifecycle": [
                            {
                                "status": "LIVE_FILLED",
                                "payload": {
                                    "response_filled_size_usd": 4.0,
                                    "response_fill_size_shares": 5.0,
                                    "response_fill_price": 0.8,
                                },
                            }
                        ],
                    },
                ]
            }
        )
    )
    (data / "scorecard.json").write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "since_topup_truth": {"actual_delta_vs_baseline_usd": -5.368576},
            }
        )
    )
    report = build_report(
        root,
        argparse.Namespace(
            classification="data/research/classification.json",
            self_feed_report="data/research/self.json",
            live_state="data/research/live.json",
            scorecard="data/research/scorecard.json",
            time_tolerance_s=300.0,
            share_tolerance=0.75,
            price_tolerance=0.03,
            ruled_residual_usd=-5.27,
        ),
    )
    counts = report["summary"]["class_counts"]
    assert counts["b3_duplicate_full_ledger_match"] == 1
    assert counts["b1_confirmed_guard_evidence_no_full_ledger_fill"] == 1
    assert counts["b2_suspect_non_guard_fill"] == 1
    assert counts["b3_opposite_side_or_merge_artifact"] == 1
    assert report["summary"]["immediate_notify_required"] is True
