from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from scripts import report_fill_conditioned_loss_attribution as report


def test_loss_attribution_ranks_top_loss_and_post_floor_sample(tmp_path: Path) -> None:
    scorecard = tmp_path / "scorecard.json"
    ledger = tmp_path / "ledger.json"
    scorecard.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "canonical_pnl_truth": {
                    "events": [
                        {
                            "day_utc": "2026-07-07",
                            "status": "FILLED",
                            "resolved": True,
                            "order_id": "order-1",
                            "submitted_at": "2026-07-07T21:16:00Z",
                            "market_slug": "btc-updown-5m-1783458900",
                            "limit_price": 0.46,
                            "cost_usd": 2.0,
                            "pnl_usd": -2.0,
                            "side": "YES",
                            "source_wallet": "0xwallet",
                        },
                        {
                            "day_utc": "2026-07-07",
                            "status": "FILLED",
                            "resolved": True,
                            "order_id": "order-2",
                            "submitted_at": "2026-07-07T21:17:00Z",
                            "market_slug": "btc-updown-5m-1783458900",
                            "limit_price": 0.31,
                            "cost_usd": 1.0,
                            "pnl_usd": 2.0,
                            "side": "NO",
                            "source_wallet": "0xwallet",
                        },
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    ledger.write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "order_id": "order-1",
                        "final_status": "FILLED",
                        "submitted_at": "2026-07-07T21:16:00Z",
                        "limit_price": 0.46,
                        "latency_budget": {"hops": {"source_fill_block_to_exchange_ack_s": 181.0}},
                    },
                    {
                        "order_id": "reject-1",
                        "final_status": "REJECTED",
                        "submitted_at": "2026-07-07T21:18:00Z",
                        "limit_price": 0.46,
                        "reject_reason": "precision_reject",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = report.build_report(
        argparse.Namespace(
            ledger=str(ledger),
            scorecard=str(scorecard),
            output=str(tmp_path / "out.json"),
            day="2026-07-07",
            post_floor_since="2026-07-07T21:15:02Z",
            top=3,
        )
    )

    assert result["summary"]["resolved_fills"] == 2
    assert result["summary"]["post_floor_25_50_fills"] == 2
    assert result["summary"]["post_floor_25_50_rejects"] == {"precision_reject": 1}
    assert result["top_loss_concentrations"][0]["dimension"] == "price_bucket_5c"
    assert result["top_loss_concentrations"][0]["value"] == "45_50"
    assert "candidate" in result["top_loss_concentrations"][0]["candidate_policy_change"]
