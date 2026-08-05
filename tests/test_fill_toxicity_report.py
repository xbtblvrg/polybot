import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from scripts import report_fill_toxicity


WALLET = "0xabc0000000000000000000000000000000000000"


def test_fill_toxicity_report_flags_fills_underperforming_signal_pool(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    (data / "resolutions.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"condition_id": "cond-1", "market_slug": "btc-updown-5m-1", "direction": "UP"}),
                json.dumps({"condition_id": "cond-2", "market_slug": "btc-updown-5m-2", "direction": "DOWN"}),
            ]
        )
        + "\n"
    )
    (data / "history.json").write_text(
        json.dumps(
            {
                "events": [
                    {
                        "action": "BUY",
                        "market_slug": "btc-updown-5m-1",
                        "outcome": "Up",
                        "price": 0.5,
                        "size": 10,
                        "source_wallet": WALLET,
                        "usdc_size": 5.0,
                    },
                    {
                        "action": "BUY",
                        "market_slug": "btc-updown-5m-2",
                        "outcome": "Down",
                        "price": 0.5,
                        "size": 10,
                        "source_wallet": WALLET,
                        "usdc_size": 5.0,
                    },
                ]
            }
        )
    )
    (data / "ledger.json").write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "condition_id": "cond-1",
                        "final_status": "FILLED",
                        "order_id": "fill-1",
                        "limit_price": 0.5,
                        "market_slug": "btc-updown-5m-1",
                        "side": "NO",
                        "source_wallet": WALLET,
                        "submitted_at": "2026-07-07T01:00:00Z",
                        "trade_result": {
                            "response_fill_size_shares": 1.0,
                            "response_filled_size_usd": 1.0,
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
                    "active_set_roster": {"members": [{"source_wallet": WALLET}]},
                "canonical_pnl_truth": {
                    "events": [
                        {
                            "cost_usd": 1.0,
                            "limit_price": 0.5,
                            "order_id": "fill-1",
                            "pnl_usd": -1.0,
                            "resolved": True,
                            "source_wallet": WALLET,
                            "status": "FILLED",
                            "submitted_at": "2026-07-07T01:00:00Z",
                        }
                    ]
                },
            }
        )
    )

    report = report_fill_toxicity.build_report(
        root,
        argparse.Namespace(
            day="2026-07-07",
            comparison_bands="0.20,0.40,0.60",
            guard_state="data/research/missing_guard.json",
            history="data/research/history.json",
            ledger="data/research/ledger.json",
            min_fills=1,
            output="unused.json",
            post_fix_decision_min_fills=8,
            post_fix_start="",
            reconciliation_start="2026-07-05T12:55:00Z",
            resolutions="data/research/resolutions.jsonl",
            scorecard="data/research/scorecard.json",
        ),
    )

    assert report["summary"]["verdict"] == "TOXIC_FILLS"
    assert report["summary"]["all_signals"]["roi_pct"] == 100.0
    assert report["summary"]["live_fills"]["roi_pct"] == -100.0
    assert report["summary"]["toxicity_roi_pct"] == -200.0
    assert report["worst_groups"][0]["source_wallet"] == WALLET
    denylist = report_fill_toxicity.build_denylist(report, min_signals=1)
    assert denylist["cell_count"] == 0

    report["groups"][0]["all_signals"]["roi_pct"] = -1.0
    report["groups"][0]["all_signals"]["count"] = 2
    denylist = report_fill_toxicity.build_denylist(report, min_signals=1)
    assert denylist["cell_count"] == 1
    assert denylist["cells"][0]["source_wallet"] == WALLET
    assert denylist["cells"][0]["price_bucket"] == "02_50_70"

    # Live-positive override (fable RULING M8): >=20 live fills at positive
    # ROI beats a signals-based deny; a live-based deny is unaffected.
    report["groups"][0]["live_fills"]["count"] = 20
    report["groups"][0]["live_fills"]["roi_pct"] = 2.0
    denylist = report_fill_toxicity.build_denylist(report, min_signals=1)
    assert denylist["cell_count"] == 0

    report["groups"][0]["live_fills"]["roi_pct"] = -1.0
    denylist = report_fill_toxicity.build_denylist(report, min_signals=1)
    assert denylist["cell_count"] == 1
    assert denylist["cells"][0]["deny_rule"] == "our_fills_5_roi_le_0"


def test_fill_toxicity_report_builds_post_fix_comparison_packet(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    (data / "resolutions.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"condition_id": "cond-pre", "market_slug": "btc-updown-5m-pre", "direction": "UP"}),
                json.dumps({"condition_id": "cond-post", "market_slug": "btc-updown-5m-post", "direction": "UP"}),
            ]
        )
        + "\n"
    )
    (data / "history.json").write_text(
        json.dumps(
            {
                "events": [
                    {
                        "action": "BUY",
                        "market_slug": "btc-updown-5m-pre",
                        "outcome": "Up",
                        "price": 0.19,
                        "size": 10,
                        "source_wallet": WALLET,
                        "usdc_size": 1.9,
                    },
                    {
                        "action": "BUY",
                        "market_slug": "btc-updown-5m-post",
                        "outcome": "Up",
                        "price": 0.35,
                        "size": 10,
                        "source_wallet": WALLET,
                        "usdc_size": 3.5,
                    },
                ]
            }
        )
    )
    (data / "ledger.json").write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "condition_id": "cond-pre",
                        "final_status": "FILLED",
                        "limit_price": 0.19,
                        "market_slug": "btc-updown-5m-pre",
                        "order_id": "fill-pre",
                        "side": "NO",
                        "source_wallet": WALLET,
                        "submitted_at": "2026-07-07T23:00:00Z",
                        "trade_result": {
                            "response_fill_size_shares": 10.0,
                            "response_filled_size_usd": 1.9,
                        },
                    },
                    {
                        "condition_id": "cond-post",
                        "final_status": "FILLED",
                        "limit_price": 0.19,
                        "market_slug": "btc-updown-5m-post",
                        "order_id": "fill-post",
                        "side": "NO",
                        "source_wallet": WALLET,
                        "submitted_at": "2026-07-08T05:30:00Z",
                        "trade_result": {
                            "response_fill_size_shares": 10.0,
                            "response_filled_size_usd": 1.9,
                        },
                    },
                ]
            }
        )
    )
    (data / "scorecard.json").write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "active_set_roster": {"members": [{"source_wallet": WALLET}]},
            }
        )
    )

    report = report_fill_toxicity.build_report(
        root,
        argparse.Namespace(
            comparison_bands="0.20,0.40,0.60",
            day="2026-07-08",
            guard_state="data/research/missing_guard.json",
            history="data/research/history.json",
            ledger="data/research/ledger.json",
            min_fills=1,
            output="unused.json",
            post_fix_decision_min_fills=8,
            post_fix_start="2026-07-08T05:22:00Z",
            reconciliation_start="2026-07-05T12:55:00Z",
            resolutions="data/research/resolutions.jsonl",
            scorecard="data/research/scorecard.json",
        ),
    )

    comparison = report["comparison"]
    assert comparison["band_spec"][0]["label"] == "00_le_20"
    assert comparison["pre_fix_since_topup"]["live_fills"]["count"] == 1
    assert comparison["post_fix"]["live_fills"]["count"] == 1
    assert comparison["post_fix"]["low_band_decision_read"]["live_fills"] == 1
    assert comparison["post_fix"]["low_band_decision_read"]["floor_trigger_candidate"] is False
    assert comparison["post_fix"]["low_band_decision_read"]["sample_size_honesty"] == "extend_measurement_window"
    assert report["summary"]["post_fix_sample_size_honesty"] == "extend_measurement_window"
