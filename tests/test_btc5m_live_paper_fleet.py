import json
from argparse import Namespace
from pathlib import Path

from scripts.build_btc5m_live_paper_fleet import _select_fleet, build_report


def test_select_fleet_excludes_registry_disabled_wallets() -> None:
    disabled = "0x" + "1" * 40
    enabled = "0x" + "2" * 40
    rows = _select_fleet(
        {"leaderboard": [
            {"wallet": disabled, "score": 99, "registry_enabled": False},
            {"wallet": enabled, "score": 98, "registry_enabled": True},
        ]},
        1,
    )
    assert [row["wallet"] for row in rows] == [enabled]


def test_btc5m_live_paper_fleet_selects_top_wallets_and_builds_matrix(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    wallet_c = "0xcccccccccccccccccccccccccccccccccccccccc"
    (research / "wallet_copy_full_universe_copyability_latest.json").write_text(
        json.dumps(
            {
                "leaderboard": [
                    {
                        "wallet": wallet_a,
                        "copyability_score": 10.0,
                        "admission_status": "READY_QUEUE",
                        "queue_eligible": True,
                        "copy_replay": {"paper_pnl_usd": 2.5, "copyable_buy_events": 21},
                    },
                    {
                        "wallet": wallet_b,
                        "copyability_score": 9.0,
                        "admission_status": "POSITIVE_THIN_COPYABLE_SAMPLE",
                        "copy_replay": {"paper_pnl_usd": 1.5, "copyable_buy_events": 3},
                    },
                    {"wallet": wallet_c, "copyability_score": 8.0, "admission_status": "NO_COPY_REPLAY"},
                ]
            }
        ),
        encoding="utf-8",
    )
    (research / "wallet_copy_history_state.json").write_text(
        json.dumps(
            {
                "events": [
                    {
                        "action": "BUY",
                        "source_wallet": wallet_a,
                        "market_slug": "btc-updown-5m-1700000100",
                        "condition_id": "cond-a",
                        "outcome": "Up",
                        "price": 0.5,
                        "size": 4.0,
                        "event_ts": 1700000120,
                    },
                    {
                        "action": "BUY",
                        "source_wallet": wallet_a,
                        "market_slug": "btc-updown-5m-1700000100",
                        "condition_id": "cond-a",
                        "outcome": "Down",
                        "usdc_size": 1.0,
                        "event_ts": 1700000130,
                    },
                    {
                        "action": "BUY",
                        "source_wallet": wallet_c,
                        "market_slug": "btc-updown-5m-1700000100",
                        "condition_id": "cond-a",
                        "outcome": "Up",
                        "usdc_size": 9.0,
                        "event_ts": 1700000140,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    (research / "btc_resolutions_test.jsonl").write_text(
        json.dumps({"market_slug": "btc-updown-5m-1700000100", "condition_id": "cond-a", "direction": "UP"}) + "\n",
        encoding="utf-8",
    )

    report = build_report(
        root,
        Namespace(
            leaderboard="data/research/wallet_copy_full_universe_copyability_latest.json",
            history="data/research/wallet_copy_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            output="data/research/btc5m_live_paper_fleet_latest.json",
            top_n=2,
            matrix_max_rows=100,
        ),
    )

    assert report["summary"]["fleet_size"] == 2
    assert report["summary"]["ready_queue_wallets"] == 1
    assert report["summary"]["top50_matrix_coverage"]["wallets"] == 2
    assert report["summary"]["top50_matrix_coverage"]["with_rows"] == 1
    assert report["summary"]["coverage_defect"] is True
    assert [row["wallet"] for row in report["fleet"]] == [wallet_a, wallet_b]
    assert report["fleet"][0]["matrix_coverage"] == "WINDOW_ROWS"
    assert report["fleet"][1]["matrix_coverage"] == "NONE"
    assert report["summary"]["matrix_rows"] == 1
    row = report["window_matrix"][0]
    assert row["wallet"] == wallet_a
    assert row["buy_actions"] == 2
    assert row["up_buy_usd"] == 2.0
    assert row["down_buy_usd"] == 1.0
    assert row["winner"] == "UP"
