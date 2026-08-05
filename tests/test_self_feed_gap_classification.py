import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from scripts.classify_self_feed_ledger_gaps import build_report


def test_self_feed_gap_classification_splits_join_defects_from_true_candidates(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    (data / "self.json").write_text(
        json.dumps(
            {
                "self_missing_ledger_rows": [
                    {"tx": "0xsplit", "self_feed": {"cost_usd": 1.0, "sources": ["data_api_trades_user"]}},
                    {"tx": "0xtrue", "self_feed": {"cost_usd": 2.0, "sources": ["data_api_trades_user"]}},
                ],
                "probable_split_fill_groups": [
                    {
                        "ledger_tx": "0xledger",
                        "ledger_order_ids": ["order-1"],
                        "missing_companion_self_feed_txs": [{"tx": "0xsplit", "cost_usd": 1.0}],
                    }
                ],
                "ledger_rows": [{"tx": "0xledger", "status": "AMOUNT_MISMATCH"}],
                "summary": {},
            }
        )
    )
    (data / "scorecard.json").write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "chain_reconciliation": {"status": "MISMATCH"},
            }
        )
    )

    report = build_report(
        root,
        argparse.Namespace(
            self_feed_report="data/research/self.json",
            scorecard="data/research/scorecard.json",
            output="unused.json",
        ),
    )

    assert report["summary"]["join_key_defect_probable_split_fill"] == 1
    assert report["summary"]["true_unrecorded_fill_candidate"] == 1
    assert report["summary"]["p0_guard_fill_recording_audit_required"] is True
    assert report["summary"]["amount_mismatch_split_overlap"] == 1
