import json
from argparse import Namespace
from pathlib import Path

from scripts.build_watch_tier_expansion_ranking import build_report, write_watch_config


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_watch_tier_ranking_prefers_fresh_probe_then_copy_evidence(tmp_path: Path) -> None:
    leaderboard = tmp_path / "leaderboard.json"
    probe = tmp_path / "probe.json"
    fleet = tmp_path / "fleet.json"
    morning = tmp_path / "morning.json"
    queue = tmp_path / "queue.json"
    active = tmp_path / "active.json"
    out = tmp_path / "ranking.json"
    config = tmp_path / "watch.json"

    _write(
        leaderboard,
        {
            "candidate_wallets": [
                {"address": "0x1111111111111111111111111111111111111111", "ranks": {"WEEK": 5}},
                {"address": "0x2222222222222222222222222222222222222222", "ranks": {"WEEK": 1}},
                {"address": "0x3333333333333333333333333333333333333333", "ranks": {"WEEK": 2}},
            ]
        },
    )
    _write(
        probe,
        {
            "ranked_candidates": [
                {
                    "wallet": "0x1111111111111111111111111111111111111111",
                    "fresh_flow": True,
                    "median_entry_offset_s": 42,
                    "inband_025_050_buy_share_pct": 80,
                    "btc5m_buys": 20,
                }
            ],
            "fresh_local_feed_outside_queue": [],
        },
    )
    _write(fleet, {"fleet": [{"wallet": "0x2222222222222222222222222222222222222222", "admission_status": "READY_QUEUE", "paper_pnl_usd": 9.0}]})
    _write(morning, {"ranked_rows": []})
    _write(queue, {"ranked_members": []})
    _write(active, {"active_set": {"members": [{"source_wallet": "0x3333333333333333333333333333333333333333"}]}})
    args = Namespace(
        leaderboard=leaderboard,
        probe=probe,
        fleet=fleet,
        morning=morning,
        queue=queue,
        active_set=active,
        output=out,
        watch_config=config,
        cap=2,
    )

    report = build_report(args)
    config_payload = write_watch_config(args, report)

    assert [row["wallet"] for row in report["selected"]] == [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
    ]
    assert report["summary"]["candidate_wallets"] == 3
    assert report["summary"]["selected_fresh_probe"] == 1
    assert config_payload["measure_only"] is True
    assert config_payload["live_orders_allowed"] is False
