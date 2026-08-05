from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from scripts import report_today_fill_cash_diff as report


def test_actual_trade_summary_scores_cost_and_winning_payout_by_tx() -> None:
    trades = [
        {
            "transactionHash": "0xTx",
            "side": "BUY",
            "conditionId": "0xcond",
            "outcome": "Up",
            "size": 5,
            "price": 0.4,
            "slug": "btc-updown-5m-1",
        },
        {
            "transactionHash": "0xTx",
            "side": "BUY",
            "conditionId": "0xcond",
            "outcome": "Down",
            "size": 3,
            "price": 0.3,
            "slug": "btc-updown-5m-1",
        },
    ]

    summary = report._actual_trade_summary(trades, {"0xcond": {"direction": "UP"}})

    assert summary["0xtx"]["trades"] == 2
    assert summary["0xtx"]["actual_cost_usd"] == 2.9
    assert summary["0xtx"]["actual_payout_usd"] == 5.0
    assert summary["0xtx"]["markets"] == ["btc-updown-5m-1"]
    assert summary["0xtx"]["outcomes"] == ["Down", "Up"]


def test_build_report_joins_only_our_execution_tx_hash(tmp_path, monkeypatch) -> None:
    ledger_path = tmp_path / "ledger.json"
    resolutions_path = tmp_path / "resolutions.jsonl"
    scorecard_path = tmp_path / "scorecard.json"
    output_path = tmp_path / "out.json"
    ledger_path.write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "order_id": "order-1",
                        "status": "FILLED",
                        "submitted_at": "2026-07-07T01:00:00+00:00",
                        "condition_id": "0xcond",
                        "side": "YES",
                        "trade_result": {
                            "tx_hashes": ["0xmine"],
                            "response_filled_size_usd": 2.0,
                            "response_fill_size_shares": 5.0,
                        },
                    },
                    {
                        "order_id": "order-2",
                        "status": "FILLED",
                        "submitted_at": "2026-07-07T01:05:00+00:00",
                        "condition_id": "0xcond",
                        "side": "YES",
                        "source_intent": {"metadata": {"transaction_hash": "0xsource-wallet-trade"}},
                        "trade_result": {
                            "response_filled_size_usd": 1.0,
                            "response_fill_size_shares": 2.0,
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    resolutions_path.write_text(json.dumps({"condition_id": "0xcond", "direction": "UP"}) + "\n", encoding="utf-8")
    scorecard_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "chain_reconciliation": {"delta_vs_expected_usd": 0.4},
            }
        ),
        encoding="utf-8",
    )
    output_path.write_text(
        json.dumps(
            {
                "summary": {
                    "scorecard_delta_residual_trend": [
                        {"generated_at": f"2026-07-07T00:{idx:02d}:00Z", "cash_diff_residual_usd": idx}
                        for idx in range(51)
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    def fake_fetch(**kwargs):
        return (
            [
                {
                    "transactionHash": "0xmine",
                    "side": "BUY",
                    "conditionId": "0xcond",
                    "outcome": "Up",
                    "size": 5.0,
                    "price": 0.32,
                    "timestamp": 1783386000,
                }
            ],
            {"status": "OK", "pages": 1, "truncated": False},
        )

    monkeypatch.setattr(report, "_load_dotenv_value", lambda name: "0xproxy")
    monkeypatch.setattr(report, "_fetch_data_api_trades", fake_fetch)

    result = report.build_report(
        argparse.Namespace(
            day="2026-07-07",
            ledger=str(ledger_path),
            resolutions=str(resolutions_path),
            scorecard=str(scorecard_path),
            output=str(output_path),
            limit=100,
            max_pages=1,
            timeout_s=1.0,
        )
    )

    assert result["summary"]["ledger_fills_today"] == 2
    assert result["summary"]["ledger_fills_missing_tx"] == 1
    assert result["summary"]["ledger_tx_groups"] == 1
    assert result["summary"]["joined_tx_groups"] == 1
    assert result["summary"]["sum_ledger_cost_minus_actual_cost_usd"] == 0.4
    assert result["summary"]["sum_actual_payout_minus_canonical_payout_usd"] == 0.0
    assert result["summary"]["scorecard_delta_explained_by_fill_cost_payout_usd"] == 0.4
    assert result["summary"]["scorecard_delta_residual_after_fill_cost_payout_usd"] == 0.0
    assert result["summary"]["scorecard_delta_residual_classification"] == "fully_explained_by_joined_fill_cost_or_payout"
    trend = result["summary"]["scorecard_delta_residual_trend"]
    assert len(trend) == report.RESIDUAL_TREND_LIMIT
    assert trend[0]["cash_diff_residual_usd"] == 2
    assert trend[-1]["cash_diff_residual_usd"] == 0.0
    assert trend[-1]["basis"] == "unknown"
    assert trend[-1]["writer"] == "scripts/report_today_fill_cash_diff.py"
    assert result["top_offenders"][0]["scorecard_delta_contribution_usd"] == 0.4
    assert result["top_offenders"][0]["tx"] == "0xmine"


def test_item4_closeout_reopens_falsified_stale_fill_cash_diff_term(tmp_path) -> None:
    item4_path = tmp_path / "item4.json"
    item4_path.write_text(
        json.dumps(
            {
                "status": "PASS_TX_HASH_OR_TIMESTAMP_SKEW_NAMED",
                "generated_at": "2026-07-09T21:42:14Z",
                "classification_summary": {"other_counterparty": {"rows": 0, "net_usd": 0.0}},
                "residual_reconciliation": {
                    "canonical_residual_usd": -9.373849,
                    "h2_post_external_redeem_residual_usd": -7.590385,
                    "direct_tx_matches": [{"tx": "0xabc", "signed_amount_usd": 7.58}],
                },
                "one_page_summary": {"verdict": "PASS_TX_HASH_OR_TIMESTAMP_SKEW_NAMED"},
            }
        ),
        encoding="utf-8",
    )

    closeout = report._item4_residual_closeout(str(item4_path))

    assert closeout["status"] == "REOPENED_FALSIFIED_BY_FRESH_REBUILD"
    assert closeout["previous_status"] == "CLOSED_STALE_FILL_CASH_DIFF_ACCOUNTING_TERM"
    assert closeout["replacement_residual_classification"] == "unaccounted_one_time_cash_movement"
    assert "stale-fill cause disproven" in closeout["falsification_evidence"]["reason"]
    assert closeout["other_counterparty_rows"] == 0
    assert closeout["direct_tx_matches"][0]["tx"] == "0xabc"


def test_order_tx_prefers_live_execution_hash_over_source_wallet_hash() -> None:
    order = {
        "transaction_hash": "0xorder",
        "source_intent": {"metadata": {"transaction_hash": "0xsource"}},
        "trade_result": {"tx_hashes": ["0xlive"]},
    }

    assert report._order_tx(order) == "0xlive"


def test_residual_trend_treats_null_previous_trend_as_empty(tmp_path) -> None:
    output_path = tmp_path / "out.json"
    output_path.write_text(json.dumps({"summary": {"scorecard_delta_residual_trend": None}}), encoding="utf-8")

    trend = report._residual_trend(
        str(output_path),
        "2026-07-10T21:30:00Z",
        15.8,
        basis="response_filled_size_usd",
    )

    assert trend == [
        {
            "generated_at": "2026-07-10T21:30:00Z",
            "cash_diff_residual_usd": 15.8,
            "basis": "response_filled_size_usd",
            "writer": "scripts/report_today_fill_cash_diff.py",
        }
    ]
