import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from scripts.trace_self_feed_missing_groups import build_report


def test_trace_classifies_sample_against_same_market_ledger_rows(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    (data / "classification.json").write_text(
        json.dumps(
            {
                "scorecard_reconciliation": {"actual_delta_usd": -5.5},
                "rows": [
                    {
                        "classification": "true_unrecorded_fill_candidate",
                        "tx": "0x1",
                        "cost_usd": 1.0,
                        "pnl_usd": 2.0,
                        "condition_ids": ["c1"],
                        "market_slugs": ["m1"],
                        "outcomes": ["Up"],
                    },
                    {
                        "classification": "true_unrecorded_fill_candidate",
                        "tx": "0x2",
                        "cost_usd": 3.0,
                        "pnl_usd": -1.0,
                        "condition_ids": ["c2"],
                        "market_slugs": ["m2"],
                        "outcomes": ["Down"],
                    },
                ],
            }
        )
    )
    (data / "self.json").write_text(
        json.dumps(
            {
                "ledger_rows": [
                    {
                        "tx": "0xledger",
                        "status": "MATCH",
                        "ledger": {
                            "condition_ids": ["c1"],
                            "market_slugs": ["m1"],
                            "outcomes": ["Up"],
                            "cost_usd": 1.0,
                            "order_ids": ["order-1"],
                        },
                    }
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
            scorecard="data/research/scorecard.json",
            output="unused.json",
            sample_size=2,
            top_cost=2,
            seed=1,
            ruled_residual_usd=-5.27,
        ),
    )
    counts = report["summary"]["trace_counts"]
    assert counts["b3_join_scope_artifact_same_market_outcome_cost_match"] == 1
    assert counts["b1_recording_defect_candidate_no_same_market_ledger_match"] == 1
    assert report["gap_equation"]["actual_delta_usd"] == -5.5


def test_trace_falls_back_to_scorecard_actual_delta(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    (data / "classification.json").write_text(
        json.dumps(
            {
                "scorecard_reconciliation": {"actual_delta_usd": None},
                "rows": [
                    {
                        "classification": "true_unrecorded_fill_candidate",
                        "tx": "0x1",
                        "cost_usd": 1.0,
                        "pnl_usd": 0.25,
                        "condition_ids": ["c1"],
                        "market_slugs": ["m1"],
                        "outcomes": ["Up"],
                    }
                ],
            }
        )
    )
    (data / "self.json").write_text(json.dumps({"ledger_rows": []}))
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
            scorecard="data/research/scorecard.json",
            output="unused.json",
            sample_size=1,
            top_cost=1,
            seed=1,
            ruled_residual_usd=-5.27,
        ),
    )
    assert report["gap_equation"]["actual_delta_usd"] == -5.368576
