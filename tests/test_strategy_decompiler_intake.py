import json
import time
from argparse import Namespace
from pathlib import Path

from scripts.build_strategy_decompiler_intake import build_report


def test_strategy_decompiler_intake_selects_profitable_wallets(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    history_events = []
    for idx in range(6):
        history_events.append(
            {
                "action": "BUY",
                "source_wallet": "0xwin",
                "market_slug": f"btc-up-or-down-{idx}",
                "condition_id": f"cond-{idx}",
                "outcome": "UP",
                "price": 0.5,
                "usdc_size": 2.0,
                "event_ts": time.time() - 60 + idx,
            }
        )
        history_events.append(
            {
                "action": "BUY",
                "source_wallet": "0xlose",
                "market_slug": f"btc-up-or-down-{idx}",
                "condition_id": f"cond-{idx}",
                "outcome": "DOWN",
                "price": 0.5,
                "usdc_size": 2.0,
                "event_ts": time.time() - 60 + idx,
            }
        )
    (research / "wallet_copy_live_guard_hot_history_state.json").write_text(json.dumps({"events": history_events}))
    with (research / "btc_resolutions_test.jsonl").open("w") as handle:
        for idx in range(6):
            handle.write(json.dumps({"market_slug": f"btc-up-or-down-{idx}", "condition_id": f"cond-{idx}", "direction": "UP"}) + "\n")

    report = build_report(
        root,
        Namespace(
            history="data/research/wallet_copy_live_guard_hot_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            top_n=10,
            min_resolved_buys=3,
            min_unique_conditions=3,
        ),
    )

    assert report["summary"]["selected_wallets"] == 1
    assert report["selected_wallets"][0]["wallet"] == "0xwin"
    assert report["selected_wallets"][0]["pnl_usd"] == 12.0
    assert report["promotion_grade"] is True
    assert report["status"] == "PASS_CURRENT_SOURCE"
