import json
from argparse import Namespace
from datetime import UTC, datetime
from pathlib import Path

from scripts.audit_guard_fill_recording_path import build_report


def _row(tx: str, *, cost: float, pnl: float, condition: str, outcome: str) -> dict:
    return {
        "tx": tx,
        "classification": "true_unrecorded_fill_candidate",
        "cost_usd": cost,
        "pnl_usd": pnl,
        "condition_ids": [condition],
        "market_slugs": [f"btc-updown-5m-{condition[-3:]}"],
        "outcomes": [outcome],
    }


def test_guard_fill_recording_audit_splits_b1_b2_b3(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    condition_b1 = "cond-b1"
    condition_b2 = "cond-b2"
    condition_b3 = "cond-b3"
    cash_rows = [
        _row("0xb100000000000000000000000000000000000000000000000000000000000000", cost=9.0, pnl=1.0, condition=condition_b1, outcome="Up"),
        _row("0xb200000000000000000000000000000000000000000000000000000000000000", cost=8.0, pnl=-2.0, condition=condition_b2, outcome="Down"),
        _row("0xb300000000000000000000000000000000000000000000000000000000000000", cost=7.0, pnl=3.0, condition=condition_b3, outcome="Up"),
    ]
    (data / "cash.json").write_text(
        json.dumps(
            {
                "summary": {"pnl_by_class_usd": {"true_unrecorded_fill_candidate": 2.0}},
                "rows": cash_rows,
            }
        ),
        encoding="utf-8",
    )
    (data / "self.json").write_text(
        json.dumps(
            {
                "self_missing_ledger_rows": [
                    {
                        "tx": "0xb100000000000000000000000000000000000000000000000000000000000000",
                        "self_feed": {
                            "condition_ids": [condition_b1],
                            "outcomes": ["Up"],
                            "sources": ["data_api_trades_user"],
                            "order_ids": [],
                            "min_event_ts": 1000.0,
                        },
                    },
                    {
                        "tx": "0xb200000000000000000000000000000000000000000000000000000000000000",
                        "self_feed": {
                            "condition_ids": [condition_b2],
                            "outcomes": ["Down"],
                            "sources": ["polygon_orderfilled"],
                            "order_ids": ["0xmissingorder"],
                            "min_event_ts": 2000.0,
                        },
                    },
                    {
                        "tx": "0xb300000000000000000000000000000000000000000000000000000000000000",
                        "self_feed": {
                            "condition_ids": [condition_b3],
                            "outcomes": ["Up"],
                            "sources": ["data_api_trades_user"],
                            "order_ids": [],
                            "min_event_ts": 3000.0,
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    (data / "live.json").write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "status": "FILLED",
                        "order_id": "0xguardorder",
                        "condition_id": condition_b1,
                        "outcome": "Up",
                        "submitted_at": "1970-01-01T00:16:40+00:00",
                        "lifecycle": [
                            {
                                "status": "LIVE_FILLED",
                                "payload": {
                                    "details": {
                                        "transactionsHashes": [
                                            "0xb100000000000000000000000000000000000000000000000000000000000000"
                                        ]
                                    }
                                },
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (data / "scorecard.json").write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "since_topup_truth": {
                    "canonical_pnl_usd": 1.0,
                    "actual_delta_vs_baseline_usd": -5.5,
                },
                "chain_reconciliation": {"cash_delta_vs_expected_identity_usd": -6.5},
            }
        ),
        encoding="utf-8",
    )

    report = build_report(
        root,
        Namespace(
            cash_ledger="data/research/cash.json",
            self_feed_report="data/research/self.json",
            live_state="data/research/live.json",
            scorecard="data/research/scorecard.json",
            sample_size=3,
            all_candidates=True,
            seed=1,
            time_tolerance_s=180.0,
            share_tolerance=0.000001,
            price_tolerance=0.000001,
            ruled_residual_usd=-5.27,
        ),
    )

    assert report["summary"]["audit_scope"] == "full_population"
    assert report["summary"]["b1_count"] == 1
    assert report["summary"]["b2_count"] == 1
    assert report["summary"]["b3_count"] == 1
    assert report["summary"]["immediate_notify_required"] is True
    assert report["reconciliation_equation"]["actual_delta_usd"] == -5.5
    assert report["spread_identity"]["spread_widening_if_added_usd"] == 2.0
