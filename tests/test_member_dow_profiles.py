import json
from pathlib import Path

from scripts.report_member_dow_profiles import build_report


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_member_dow_profiles_counts_active_and_queue_wallets(tmp_path: Path) -> None:
    active_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    queue_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    log = tmp_path / "events.jsonl"
    _write_jsonl(
        log,
        [
            {
                "source_wallet": active_wallet,
                "action": "BUY",
                "event_ts": 1_783_468_800.0,  # 2026-07-08 00:00 UTC, Wednesday.
                "market_slug": "btc-updown-5m-1783468800",
                "window_start_s": 1_783_468_800,
                "price": 0.4,
                "size": 10,
                "transaction_hash": "0x1",
            },
            {
                "source_wallet": active_wallet,
                "action": "SELL",
                "event_ts": 1_783_814_400.0,  # 2026-07-12 00:00 UTC, Sunday.
                "market_slug": "btc-updown-5m-1783814400",
                "window_start_s": 1_783_814_400,
                "price": 0.5,
                "size": 2,
                "transaction_hash": "0x2",
            },
            {
                "source_wallet": queue_wallet,
                "action": "BUY",
                "event_ts": 1_783_474_200.0,  # Wednesday 01:30 UTC.
                "market_slug": "eth-updown-5m-1783474200",
                "window_start_s": 1_783_474_200,
                "price": 0.25,
                "size": 4,
                "transaction_hash": "0x3",
            },
        ],
    )

    report = build_report(
        root=tmp_path,
        guard_state={
            "active_set": {
                "members": [
                    {"candidate_id": "active_a", "source_wallet": active_wallet, "policy_id": "policy_a"}
                ]
            }
        },
        queue_state={
            "ranked_members": [
                {"name": "queue_b", "wallet": queue_wallet, "queue_rank": 1, "ready_for_live": True}
            ]
        },
        top_queue=3,
        wallet_event_logs=[str(log)],
        combined_tail_log="",
        max_tail_bytes=100_000,
    )

    active = report["profiles_by_wallet"][active_wallet]
    queue = report["profiles_by_wallet"][queue_wallet]
    assert report["summary"]["target_count"] == 2
    assert active["trade_count"] == 2
    assert active["buy_trade_count"] == 1
    assert active["sell_trade_count"] == 1
    assert active["btc5m_unique_window_count"] == 2
    assert active["trades_by_dow"]["2:wed"]["trades"] == 1
    assert active["trades_by_dow"]["6:sun"]["trades"] == 1
    assert active["weekend_evidence_status"] == "HAS_WEEKEND_SAMPLE"
    assert active["weekend_activity_weight_vs_weekday"] == 1.0
    assert queue["roles"] == ["top_queue_candidate"]
    assert queue["btc5m_trade_count"] == 0
    assert queue["trades_by_utc_hour"]["1"]["trades"] == 1
