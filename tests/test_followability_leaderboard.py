import json
import time
from argparse import Namespace
from pathlib import Path

from scripts.build_wallet_followability_leaderboard import build_report


def _event(wallet: str, market_idx: int, *, offset: int, outcome: str, stake: float) -> dict:
    window_start = int(time.time() // 300 * 300) - (5 - market_idx) * 300
    price = 0.5
    return {
        "action": "BUY",
        "source_wallet": wallet,
        "market_slug": f"btc-updown-5m-{window_start}",
        "condition_id": f"cond-{market_idx}",
        "outcome": outcome,
        "price": price,
        "size": stake / price,
        "usdc_size": stake,
        "event_ts": window_start + offset,
    }


def test_followability_leaderboard_scores_early_commitment_continuation(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    events = []
    for idx in range(5):
        events.append(_event("0xfollow", idx, offset=20, outcome="UP", stake=2.0))
        events.append(_event("0xfollow", idx, offset=120, outcome="UP", stake=3.0))
        events.append(_event("0noise", idx, offset=20, outcome="UP", stake=2.0))
        events.append(_event("0noise", idx, offset=120, outcome="DOWN", stake=3.0))
    (research / "wallet_copy_live_guard_hot_history_state.json").write_text(json.dumps({"events": events}), encoding="utf-8")
    with (research / "btc_resolutions_test.jsonl").open("w", encoding="utf-8") as handle:
        for idx in range(5):
            handle.write(
                json.dumps(
                    {
                        "market_slug": _event("0xfollow", idx, offset=20, outcome="UP", stake=2.0)["market_slug"],
                        "condition_id": f"cond-{idx}",
                        "direction": "UP",
                    }
                )
                + "\n"
            )

    report = build_report(
        root,
        Namespace(
            history="data/research/wallet_copy_live_guard_hot_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            top_n=10,
            early_window_s=60.0,
            min_windows=3,
            min_early_stake_usd=0.1,
        ),
    )

    assert report["summary"]["wallets_scored"] == 2
    assert report["selected_wallets"][0]["wallet"] == "0xfollow"
    assert report["selected_wallets"][0]["early_side_predictiveness_pct"] == 100.0
    assert report["selected_wallets"][0]["early_win_rate_pct"] == 100.0
    assert report["selected_wallets"][0]["avg_continuation_same_side_usd"] == 3.0
    assert report["selected_wallets"][0]["followability_score"] == 3.0
    assert report["promotion_grade"] is True
    assert report["source_freshness"]["newest_source_event_age_s"] < 86400.0

    (research / "wallet_copy_history_state.json").write_text(json.dumps({"events": events}), encoding="utf-8")
    stale = build_report(
        root,
        Namespace(
            history="data/research/wallet_copy_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            top_n=10,
            early_window_s=60.0,
            min_windows=3,
            min_early_stake_usd=0.1,
        ),
    )
    assert stale["promotion_grade"] is False
    assert stale["selected_wallets"] == []
