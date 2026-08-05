from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from scripts.report_corrected_copyability_probe import build_report


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _event(wallet: str, ts: float, price: float, *, slug: str | None = None) -> dict:
    window_start = ts - (ts % 300)
    slug = slug or f"btc-updown-5m-{int(window_start)}"
    return {
        "row_type": "trade",
        "action": "BUY",
        "source_wallet": wallet,
        "market_slug": slug,
        "event_ts": ts,
        "window_start_s": window_start,
        "price": price,
    }


def _args(tmp_path: Path) -> Namespace:
    return Namespace(
        wallet_events=str(tmp_path / "events.jsonl"),
        queue=str(tmp_path / "queue.json"),
        registry=str(tmp_path / "wallets.json"),
        leaderboard=str(tmp_path / "leaderboard.json"),
        fleet=str(tmp_path / "fleet.json"),
        morning=str(tmp_path / "morning.json"),
        followability=str(tmp_path / "followability.json"),
        active_set=str(tmp_path / "guard.json"),
        output=str(tmp_path / "out.json"),
        lookback_hours=24.0,
        tail_lines=0,
        max_promotions=2,
        fresh_hours=24.0,
        median_entry_threshold_s=60.0,
        inband_min_share_pct=50.0,
        inband_min_price=0.25,
        inband_max_price=0.50,
        min_btc5m_buys=1,
    )


def test_corrected_probe_promotes_early_inband_non_active_candidate(tmp_path: Path) -> None:
    good = "0x1111111111111111111111111111111111111111"
    late = "0x2222222222222222222222222222222222222222"
    now = 1_783_560_000.0
    _write_json(
        tmp_path / "queue.json",
        {
            "ranked_members": [
                {"wallet": late, "queue_rank": 1, "ready_for_live": True, "resolved_pnl": 4.0},
                {"wallet": good, "queue_rank": 2, "ready_for_live": True, "resolved_pnl": 3.0},
            ]
        },
    )
    _write_json(tmp_path / "wallets.json", {"wallets": []})
    _write_json(tmp_path / "leaderboard.json", {"top_wallets": []})
    _write_json(tmp_path / "fleet.json", {"fleet": []})
    _write_json(tmp_path / "morning.json", {"ranked_rows": []})
    _write_json(tmp_path / "followability.json", {"leaderboard": []})
    _write_json(tmp_path / "guard.json", {"active_set": {"members": []}})
    rows = [
        _event(late, now - 120.0, 0.40),
        _event(good, now - 290.0, 0.40),
        _event(good, now - 280.0, 0.35),
    ]
    (tmp_path / "events.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    report = build_report(_args(tmp_path), now_s=now)

    assert report["summary"]["p1_promotion_eligible_non_active"] == 1
    assert report["recommendations"][0]["wallet"] == good
    late_row = next(row for row in report["ranked_candidates"] if row["wallet"] == late)
    assert "median_entry_offset_not_lt_60s" in late_row["p1_reject_reasons"]


def test_corrected_probe_tracks_fresh_leaderboard_wallet_outside_queue(tmp_path: Path) -> None:
    wallet = "0x3333333333333333333333333333333333333333"
    now = 1_783_560_000.0
    _write_json(tmp_path / "queue.json", {"ranked_members": []})
    _write_json(
        tmp_path / "wallets.json",
        {
            "wallets": [
                {
                    "address": wallet,
                    "enabled": True,
                    "market_filter": "btc_5m",
                    "tags": ["leaderboard_crypto", "candidate"],
                }
            ]
        },
    )
    _write_json(tmp_path / "leaderboard.json", {"top_wallets": []})
    _write_json(tmp_path / "fleet.json", {"fleet": []})
    _write_json(tmp_path / "morning.json", {"ranked_rows": []})
    _write_json(tmp_path / "followability.json", {"leaderboard": []})
    _write_json(tmp_path / "guard.json", {"active_set": {"members": []}})
    (tmp_path / "events.jsonl").write_text(json.dumps(_event(wallet, now - 280.0, 0.30)) + "\n", encoding="utf-8")

    report = build_report(_args(tmp_path), now_s=now)

    assert report["summary"]["fresh_local_feed_outside_queue"] == 1
    assert report["fresh_local_feed_outside_queue"][0]["wallet"] == wallet
    assert report["fresh_local_feed_outside_queue"][0]["p1_promotion_eligible"] is True


def test_corrected_probe_uses_modulo_offset_when_window_start_is_stale(tmp_path: Path) -> None:
    wallet = "0x4444444444444444444444444444444444444444"
    now = 1_783_560_000.0
    _write_json(tmp_path / "queue.json", {"ranked_members": [{"wallet": wallet, "ready_for_live": True}]})
    _write_json(tmp_path / "wallets.json", {"wallets": []})
    _write_json(tmp_path / "leaderboard.json", {"top_wallets": []})
    _write_json(tmp_path / "fleet.json", {"fleet": []})
    _write_json(tmp_path / "morning.json", {"ranked_rows": []})
    _write_json(tmp_path / "followability.json", {"leaderboard": []})
    _write_json(tmp_path / "guard.json", {"active_set": {"members": []}})
    row = _event(wallet, now - 20.0, 0.30)
    row["window_start_s"] = row["event_ts"] + 600.0
    (tmp_path / "events.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")

    report = build_report(_args(tmp_path), now_s=now)
    measured = next(row for row in report["ranked_candidates"] if row["wallet"] == wallet)

    assert measured["median_entry_offset_s"] == 280.0
    assert "median_entry_offset_not_lt_60s" in measured["p1_reject_reasons"]
