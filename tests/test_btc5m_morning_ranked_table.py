import json
from argparse import Namespace
from pathlib import Path

from scripts.build_btc5m_morning_ranked_table import build_report


def test_morning_ranked_table_merges_families_and_flags_matrix_gaps(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    (research / "btc5m_live_paper_fleet_latest.json").write_text(
        json.dumps(
            {
                "fleet": [
                    {
                        "wallet": wallet_a,
                        "fleet_rank": 1,
                        "paper_lane_id": "paper-a",
                        "admission_status": "READY_QUEUE",
                        "paper_pnl_usd": 4.0,
                        "resolved_orders": 12,
                        "matrix_coverage": "WINDOW_ROWS",
                    },
                    {
                        "wallet": wallet_b,
                        "fleet_rank": 2,
                        "paper_lane_id": "paper-b",
                        "admission_status": "READY_QUEUE",
                        "paper_pnl_usd": 8.0,
                        "resolved_orders": 20,
                        "matrix_coverage": "NONE",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    (research / "btc5m_two_sided_prime_study_latest.json").write_text(
        json.dumps(
            {
                "mechanism_rows": [
                    {
                        "mechanism_id": "structural-pair-sum-arb",
                        "family": "structural",
                        "paper_lane_id": "paper-pair",
                        "status": "HOLDOUT_PASS",
                        "holdout_passed": True,
                        "ev_per_day_usd": 5.0,
                        "oos_pnl_usd": 5.0,
                        "oos_trades": 3,
                        "evidence_pointer": "two#pair",
                        "proposed_funding_size_usd": 1.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (research / "btc5m_corpus_signal_batch_20260707_full.json").write_text(
        json.dumps(
            {
                "resolution_summary": {"min_window_start_s": 1000, "max_window_start_s": 1000 + 86400},
                "studies": {
                    "E11_cross_window_momentum": {
                        "status": "POSITIVE_OOS_REGION",
                        "best": {"key": "entry=0.45", "oos_pnl_usd": 2.0, "oos_trades": 4},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (research / "wallet_copy_strategy_decompiler_intake_latest.json").write_text(
        json.dumps({"selected_wallets": [{"wallet": wallet_a, "pnl_usd": 10.0, "roi_pct": 2.0, "span_days": 2.0}]}),
        encoding="utf-8",
    )

    report = build_report(
        root,
        Namespace(
            fleet="data/research/btc5m_live_paper_fleet_latest.json",
            two_sided="data/research/btc5m_two_sided_prime_study_latest.json",
            e_batch="data/research/btc5m_corpus_signal_batch_20260707_full.json",
            decompiler="data/research/wallet_copy_strategy_decompiler_intake_latest.json",
            output="data/research/btc5m_morning_ranked_table_latest.json",
            top_fleet=2,
        ),
    )

    assert report["summary"]["rows"] == 5
    assert report["summary"]["matrix_coverage_none_rows"] == 1
    assert report["ranked_rows"][0]["mechanism_id"] == "structural-pair-sum-arb"
    gap = [row for row in report["ranked_rows"] if row["candidate_id"] == wallet_b][0]
    assert gap["status"] == "DATA_INCOMPLETE"
    assert gap["notes"] == "matrix_coverage_missing"
    assert gap["holdout_passed"] is False
