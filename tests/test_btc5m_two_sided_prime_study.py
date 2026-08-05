import json
from argparse import Namespace
from pathlib import Path

from scripts.build_btc5m_two_sided_prime_study import build_report


def test_two_sided_prime_study_outputs_ev_rows(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    wallet = "0x1111111111111111111111111111111111111111"
    scalp_wallet = "0x2222222222222222222222222222222222222222"
    history = {
        "events": [
            {
                "action": "BUY",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000000",
                "condition_id": "cond-a",
                "outcome": "Up",
                "price": 0.45,
                "size": 2.0,
                "event_ts": 1000000010,
            },
            {
                "action": "BUY",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000000",
                "condition_id": "cond-a",
                "outcome": "Down",
                "price": 0.50,
                "size": 2.0,
                "event_ts": 1000000020,
            },
            {
                "action": "BUY",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000300",
                "condition_id": "cond-b",
                "outcome": "Up",
                "price": 0.40,
                "size": 2.0,
                "event_ts": 1000000310,
            },
            {
                "action": "BUY",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000300",
                "condition_id": "cond-b",
                "outcome": "Down",
                "price": 0.45,
                "size": 2.0,
                "event_ts": 1000000320,
            },
            {
                "action": "BUY",
                "source_wallet": scalp_wallet,
                "market_slug": "btc-updown-5m-1000000300",
                "condition_id": "cond-b",
                "outcome": "Down",
                "price": 0.40,
                "size": 10.0,
                "event_ts": 1000000330,
            },
            {
                "action": "SELL",
                "source_wallet": scalp_wallet,
                "market_slug": "btc-updown-5m-1000000300",
                "condition_id": "cond-b",
                "outcome": "Down",
                "price": 0.55,
                "size": 10.0,
                "event_ts": 1000000340,
            },
        ]
    }
    (research / "wallet_copy_history_state.json").write_text(json.dumps(history), encoding="utf-8")
    (research / "btc_resolutions_test.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"market_slug": "btc-updown-5m-1000000000", "condition_id": "cond-a", "direction": "UP"}),
                json.dumps({"market_slug": "btc-updown-5m-1000000300", "condition_id": "cond-b", "direction": "DOWN"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = build_report(
        root,
        Namespace(
            history="data/research/wallet_copy_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            output="data/research/btc5m_two_sided_prime_study_latest.json",
            order_usd=1.0,
            tick_size=0.01,
            train_fraction=0.5,
            min_oos_trades=1,
            top_wallets=10,
        ),
    )

    assert report["summary"]["pair_sum_candidates"] == 2
    ids = {row["mechanism_id"] for row in report["mechanism_rows"]}
    assert ids == {"copy-two-sided-inventory", "structural-intra-window-scalp", "structural-pair-sum-arb"}
    assert report["best_two_sided_wallet"]["wallet"] == wallet
    assert report["wallet_two_sided_rows"][0]["copy_replay"]["test"]["trades"] == 2
    assert report["promotion_grade"] is False
    assert report["status"] == "STALE_SOURCE_FAIL_CLOSED"
