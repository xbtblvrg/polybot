from __future__ import annotations

import json
from pathlib import Path

import scripts.run_btc5m_late_window_penny_watcher as watcher


def test_spot_context_from_klines_maps_instant_outcome_probability() -> None:
    window_start = 1_779_999_900
    context = watcher.spot_context_from_klines(
        {
            window_start: [window_start, "100.0", "100.0", "100.0", "100.0"],
            window_start + 240: [window_start + 240, "100.0", "101.0", "100.0", "100.5"],
        },
        window_start_s=window_start,
        now_ts=window_start + 242.0,
    )

    assert context["status"] == "OK"
    assert context["instant_outcome_if_expired_now"] == "Up"
    assert context["outcome_probabilities"] == {"Up": 1.0, "Down": 0.0}
    assert context["delta_bps"] == 50.0


def test_snapshot_from_book_records_penny_depth_and_book_latency() -> None:
    snapshot = watcher._snapshot_from_book(
        token_id="down-token",
        book={
            "asset_id": "down-token",
            "timestamp": 1_780_000_240_000,
            "asks": [
                {"price": "0.01", "size": "5"},
                {"price": "0.02", "size": "10"},
                {"price": "0.03", "size": "100"},
            ],
            "bids": [{"price": "0.009", "size": "1"}],
        },
        captured_at_s=1_780_000_241.25,
        fetch_started_at_s=1_780_000_241.0,
        route_report={"status": "PASS"},
        order_usd=1.0,
        max_penny_ask=0.02,
    )

    assert snapshot["status"] == "OK"
    assert snapshot["best_ask"] == 0.01
    assert snapshot["penny_levels"] == 2
    assert snapshot["penny_depth_shares"] == 15.0
    assert snapshot["penny_depth_cost_usd"] == 0.25
    assert snapshot["weighted_penny_ask"] == 0.016667
    assert snapshot["detection_to_book_timestamp_latency_s"] == 1.25


def test_build_state_records_paper_only_penny_opportunity(tmp_path: Path, monkeypatch) -> None:
    state_path = tmp_path / "penny_state.json"
    event_log_path = tmp_path / "penny_events.jsonl"
    window_start = 1_779_999_900

    def fake_klines(symbol: str, start_s: int, end_s: int, timeout_s: float) -> dict[int, list[str]]:
        assert symbol == "BTCUSDT"
        return {
            window_start: [window_start, "100.0", "100.0", "100.0", "100.0"],
            window_start + 240: [window_start + 240, "99.0", "100.0", "98.0", "99.5"],
        }

    def fake_market(slug: str, *, timeout_s: float) -> dict:
        return {
            "conditionId": "0xcondition",
            "outcomes": '["Up","Down"]',
            "clobTokenIds": '["up-token","down-token"]',
            "_gamma_route_used": "test",
            "_gamma_route_attempts": [{"route": "test", "status": "PASS"}],
        }

    def fake_book_snapshot(*, clob, token_id: str, order_usd: float, max_penny_ask: float) -> dict:
        if token_id == "down-token":
            return {
                "status": "OK",
                "token_id": token_id,
                "captured_at_s": window_start + 242.0,
                "captured_at_iso": "2026-06-01T00:04:02Z",
                "book_timestamp_s": window_start + 241.5,
                "book_timestamp_iso": "2026-06-01T00:04:01.500000Z",
                "detection_to_book_timestamp_latency_s": 0.5,
                "latency_basis": "clob_book_timestamp_vs_detection_response_time",
                "best_bid": 0.009,
                "best_ask": 0.01,
                "penny_depth_shares": 10.0,
                "penny_depth_cost_usd": 0.1,
                "weighted_penny_ask": 0.01,
                "best_penny_ask": 0.01,
                "best_penny_shares": 10.0,
                "book_route_used": "configured_clob_route",
                "route_report": {"status": "PASS"},
            }
        return {
            "status": "OK",
            "token_id": token_id,
            "best_bid": 0.98,
            "best_ask": 0.99,
            "penny_depth_shares": 0.0,
            "penny_depth_cost_usd": 0.0,
            "weighted_penny_ask": 0.0,
            "book_route_used": "configured_clob_route",
            "route_report": {"status": "PASS"},
        }

    class Args:
        state = str(state_path)
        event_log = str(event_log_path)
        symbol = "BTCUSDT"
        max_penny_ask = 0.02
        final_window_s = 90.0
        order_usd = 1.0
        timeout_s = 1.0
        clob_base_url = "http://127.0.0.1:8787/clob"
        clob_timeout_s = 1.0
        reset_state = True
        now_ts = float(window_start + 242)

    monkeypatch.setattr(watcher, "fetch_1m_klines", fake_klines)
    monkeypatch.setattr(watcher, "_market_for_slug", fake_market)
    monkeypatch.setattr(watcher, "_book_snapshot_with_direct_fallback", fake_book_snapshot)

    state = watcher.build_state(Args())

    assert state["kind"] == "btc5m_late_window_penny_watcher_state"
    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False
    assert state["summary"]["observed_windows"] == 1
    assert state["summary"]["penny_opportunities"] == 1
    assert state["calibration_gate"]["remaining_observed_windows"] == 99
    opportunity = state["penny_opportunities"][0]
    assert opportunity["outcome"] == "Down"
    assert opportunity["spot_implied_win_probability"] == 1.0
    assert opportunity["expected_embedded_fee_usd"] == 0.0
    assert opportunity["zero_live_assertion"]["orders_submitted"] == 0
    rows = [json.loads(line) for line in event_log_path.read_text().splitlines()]
    assert [row["event"] for row in rows] == [
        "btc5m_late_window_penny_capture",
        "btc5m_late_window_penny_opportunity",
    ]
