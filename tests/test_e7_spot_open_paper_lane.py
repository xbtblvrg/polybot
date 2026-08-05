from __future__ import annotations

import json
import os

import scripts.run_e7_spot_open_paper_lane as lane
from scripts.run_e7_spot_open_paper_lane import entry_from_books, spot_signal_from_klines


def test_spot_signal_from_klines_predicts_threshold_side() -> None:
    signal = spot_signal_from_klines(
        {
            1780000200: [1780000200, "100.0", "100.0", "100.0", "100.0"],
            1780000440: [1780000440, "100.2", "100.4", "100.1", "100.3"],
        },
        window_start_s=1780000200,
        now_ts=1780000442.0,
        threshold_bps=10.0,
    )

    assert signal["status"] == "SIGNAL"
    assert signal["predicted_outcome"] == "Up"
    assert signal["delta_bps"] == 30.0


def test_spot_signal_from_klines_records_below_threshold() -> None:
    signal = spot_signal_from_klines(
        {
            1780000200: [1780000200, "100.0", "100.0", "100.0", "100.0"],
            1780000440: [1780000440, "100.02", "100.03", "100.0", "100.05"],
        },
        window_start_s=1780000200,
        now_ts=1780000442.0,
        threshold_bps=10.0,
    )

    assert signal["status"] == "NO_SIGNAL_THRESHOLD"
    assert signal["predicted_outcome"] == ""


def test_entry_from_books_requires_actual_ask_cap_and_depth() -> None:
    entry = entry_from_books(
        predicted_outcome="Down",
        books={
            "Down": {
                "status": "OK",
                "best_ask": 0.72,
                "avg_fill_price": 0.73,
                "fillable_usd": 8.0,
                "instant_fill_status": "PASS",
            }
        },
        order_usd=8.0,
        max_entry_price=0.90,
    )

    assert entry["entry_status"] == "FILLED"
    assert entry["paper_fill"] is True
    assert entry["entry_price"] == 0.73


def test_entry_from_books_rejects_thin_or_expensive_books() -> None:
    expensive = entry_from_books(
        predicted_outcome="Up",
        books={"Up": {"status": "OK", "best_ask": 0.91, "avg_fill_price": 0.91, "fillable_usd": 8.0}},
        order_usd=8.0,
        max_entry_price=0.90,
    )
    thin = entry_from_books(
        predicted_outcome="Up",
        books={"Up": {"status": "OK", "best_ask": 0.80, "avg_fill_price": 0.80, "fillable_usd": 4.0}},
        order_usd=8.0,
        max_entry_price=0.90,
    )

    assert expensive["reject_reason"] == "ask_above_cap"
    assert thin["reject_reason"] == "insufficient_depth"


def test_market_for_slug_preserves_signal_when_gamma_route_fails(monkeypatch) -> None:
    def fail_fetch(*args, **kwargs):
        raise RuntimeError("relay busy")

    monkeypatch.setattr(lane, "_fetch_gamma_event", fail_fetch)

    market = lane._market_for_slug("btc-updown-5m-1783330800", timeout_s=1.0)

    assert market["_fetch_error"] == "RuntimeError"
    assert lane._token_map(market) == {}


def test_market_for_slug_falls_back_to_direct_gamma_when_relay_busy(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_GAMMA_API_BASE_URL", "http://127.0.0.1:8787/gamma-api")
    monkeypatch.delenv("POLYMARKET_SOURCE_PROXY_URL", raising=False)
    monkeypatch.delenv("POLYMARKET_HTTPS_PROXY", raising=False)

    def fake_fetch(slug, *, timeout_s, user_agent, client=None):
        if os.environ.get("POLYMARKET_GAMMA_API_BASE_URL"):
            raise RuntimeError("relay busy")
        return [
            {
                "markets": [
                    {
                        "slug": slug,
                        "conditionId": "0xabc",
                        "outcomes": '["Up","Down"]',
                        "clobTokenIds": '["111","222"]',
                    }
                ]
            }
        ]

    monkeypatch.setattr(lane, "_fetch_gamma_event", fake_fetch)

    market = lane._market_for_slug("btc-updown-5m-1783332000", timeout_s=1.0)

    assert market["conditionId"] == "0xabc"
    assert lane._token_map(market) == {"Up": "111", "Down": "222"}
    assert market["_gamma_route_used"] == "direct_gamma_fallback"
    assert market["_gamma_route_attempts"][0]["status"] == "ERROR"
    assert market["_gamma_route_attempts"][1]["status"] == "PASS"
    assert os.environ["POLYMARKET_GAMMA_API_BASE_URL"] == "http://127.0.0.1:8787/gamma-api"


def test_book_snapshot_falls_back_to_direct_clob_when_relay_busy(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_CLOB_API_BASE_URL", "http://127.0.0.1:8787/clob")
    monkeypatch.delenv("POLYMARKET_SOURCE_PROXY_URL", raising=False)
    monkeypatch.delenv("POLYMARKET_HTTPS_PROXY", raising=False)

    class FakeClob:
        timeout_s = 1.0
        retries = 1

    def fake_snapshot(*, clob, token_id, order_usd, max_entry_price):
        if os.environ.get("POLYMARKET_CLOB_API_BASE_URL"):
            return {
                "status": "ERROR",
                "error_type": "HTTPError",
                "error": "503 relay busy",
                "blocking_reason": "book_fetch_error",
                "instant_fill_status": "BLOCKED",
                "route_report": {"routed_host": "127.0.0.1:8787"},
            }
        return {
            "status": "OK",
            "best_ask": 0.88,
            "best_bid": 0.87,
            "avg_fill_price": 0.88,
            "fillable_usd": 8.0,
            "instant_fill_status": "PASS",
            "blocking_reason": "none",
            "route_report": {"clob_host_used": "https://clob.polymarket.com"},
        }

    monkeypatch.setattr(lane, "_book_snapshot", fake_snapshot)

    snapshot = lane._book_snapshot_with_direct_fallback(
        clob=FakeClob(),
        token_id="123",
        order_usd=8.0,
        max_entry_price=0.90,
    )

    assert snapshot["status"] == "OK"
    assert snapshot["book_route_used"] == "direct_clob_fallback"
    assert snapshot["configured_clob_route_error"]["error"] == "503 relay busy"
    assert os.environ["POLYMARKET_CLOB_API_BASE_URL"] == "http://127.0.0.1:8787/clob"


def test_build_state_counts_existing_gamma_degraded_signal_events(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "state.json"
    event_log_path = tmp_path / "events.jsonl"
    resolution_path = tmp_path / "resolutions.jsonl"
    state_path.write_text(
        json.dumps(
            {
                "signal_events": [
                    {
                        "signal_id": "e7_degraded",
                        "predicted_outcome": "Up",
                        "market": {"gamma_fetch_error": "HTTPError"},
                        "books": {"Up": {"best_ask": 0.0}},
                        "entry": {"reject_reason": "missing_token"},
                    }
                ],
                "orders": [],
            }
        )
    )
    resolution_path.write_text("")

    class Args:
        state = str(state_path)
        event_log = str(event_log_path)
        resolutions = str(resolution_path)
        reset_state = False
        now_ts = 1783332001.0
        symbol = "BTCUSDT"
        timeout_s = 1.0
        threshold_bps = 10.0
        order_usd = 8.0
        max_entry_price = 0.90
        signal_start_s = 210.0
        signal_end_s = 240.0
        clob_base_url = "http://127.0.0.1:8787/clob"
        clob_timeout_s = 1.0
        ask_sample_offsets = "30,150"
        ask_sample_tolerance_s = 7.5

    monkeypatch.setattr(lane, "_build_signal_event", lambda args, prior_signal_ids, now_ts: (None, {"status": "WAITING_SIGNAL_WINDOW"}))
    monkeypatch.setattr(lane, "_build_ask_sample_event", lambda args, prior_sample_ids, now_ts: (None, {"status": "WAITING_ASK_SAMPLE_OFFSET"}))

    state = lane.build_state(Args())

    assert state["summary"]["signal_events"] == 1
    assert state["summary"]["gamma_degraded_signal_events"] == 1
    assert state["summary"]["no_ask_signal_events"] == 0


def test_delta_window_records_keep_best_abs_delta_and_cap() -> None:
    first = {
        "window_start_s": 100,
        "market_slug": "btc-updown-5m-100",
        "observed_at": "2026-07-06T09:48:38Z",
        "signal_offset_s": 216.0,
        "delta_bps": -2.5,
        "abs_delta_bps": 2.5,
        "spot_status": "NO_SIGNAL_THRESHOLD",
        "threshold_bps": 10.0,
    }
    second = {**first, "observed_at": "2026-07-06T09:48:49Z", "signal_offset_s": 228.0, "delta_bps": 7.25, "abs_delta_bps": 7.25}
    third = {**first, "observed_at": "2026-07-06T09:49:00Z", "signal_offset_s": 239.0, "delta_bps": -3.0, "abs_delta_bps": 3.0}

    records = lane.merge_delta_window_records([], first)
    records = lane.merge_delta_window_records(records, second)
    records = lane.merge_delta_window_records(records, third)

    assert records == [
        {
            "window_start_s": 100,
            "market_slug": "btc-updown-5m-100",
            "threshold_bps": 10.0,
            "in_band_samples": 3,
            "last_delta_bps": -3.0,
            "last_abs_delta_bps": 3.0,
            "last_signal_offset_s": 239.0,
            "last_observed_at": "2026-07-06T09:49:00Z",
            "last_spot_status": "NO_SIGNAL_THRESHOLD",
            "best_abs_delta_bps": 7.25,
            "best_delta_bps": 7.25,
            "best_signal_offset_s": 228.0,
            "best_observed_at": "2026-07-06T09:48:49Z",
            "best_spot_status": "NO_SIGNAL_THRESHOLD",
        }
    ]

    assert lane.delta_window_distribution(records) == {
        "count": 1,
        "p50_abs_delta_bps": 7.25,
        "p90_abs_delta_bps": 7.25,
        "max_abs_delta_bps": 7.25,
    }

    old_records = [{"window_start_s": index, "best_abs_delta_bps": 1.0} for index in range(300)]
    capped = lane.merge_delta_window_records(old_records, None, cap=288)

    assert len(capped) == 288
    assert capped[0]["window_start_s"] == 12


def test_build_state_counts_no_ask_separately_from_gamma_degraded(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "state.json"
    event_log_path = tmp_path / "events.jsonl"
    resolution_path = tmp_path / "resolutions.jsonl"
    state_path.write_text(
        json.dumps(
            {
                "signal_events": [
                    {
                        "signal_id": "e7_no_ask",
                        "predicted_outcome": "Up",
                        "signal_offset_s": 232.9,
                        "market": {"tokens": {"Up": "111", "Down": "222"}},
                        "books": {
                            "Up": {"status": "OK", "best_ask": 0.0, "best_bid": 0.99},
                            "Down": {"status": "OK", "best_ask": 0.01, "best_bid": 0.0},
                        },
                        "entry": {"reject_reason": "missing_best_ask"},
                    }
                ],
                "orders": [],
            }
        )
    )
    resolution_path.write_text("")

    class Args:
        state = str(state_path)
        event_log = str(event_log_path)
        resolutions = str(resolution_path)
        reset_state = False
        now_ts = 1783332001.0
        symbol = "BTCUSDT"
        timeout_s = 1.0
        threshold_bps = 10.0
        order_usd = 8.0
        max_entry_price = 0.90
        signal_start_s = 210.0
        signal_end_s = 240.0
        clob_base_url = "http://127.0.0.1:8787/clob"
        clob_timeout_s = 1.0
        ask_sample_offsets = "30,150"
        ask_sample_tolerance_s = 7.5

    monkeypatch.setattr(lane, "_build_signal_event", lambda args, prior_signal_ids, now_ts: (None, {"status": "WAITING_SIGNAL_WINDOW"}))
    monkeypatch.setattr(lane, "_build_ask_sample_event", lambda args, prior_sample_ids, now_ts: (None, {"status": "WAITING_ASK_SAMPLE_OFFSET"}))

    state = lane.build_state(Args())

    signal = state["signal_events"][0]
    assert state["summary"]["signal_events"] == 1
    assert state["summary"]["gamma_degraded_signal_events"] == 0
    assert state["summary"]["no_ask_signal_events"] == 1
    assert state["summary"]["signal_offset_distribution"]["p50"] == 232.9
    assert signal["no_ask_signal"] is True
    assert signal["implied_predicted_price"] == 0.99


def test_fixed_offset_ask_sampler_records_books_without_orders(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "state.json"
    event_log_path = tmp_path / "events.jsonl"
    resolution_path = tmp_path / "resolutions.jsonl"
    state_path.write_text(json.dumps({"signal_events": [], "orders": [], "ask_samples": []}))
    resolution_path.write_text("")

    class Args:
        state = str(state_path)
        event_log = str(event_log_path)
        resolutions = str(resolution_path)
        reset_state = False
        now_ts = 1783332030.0
        symbol = "BTCUSDT"
        timeout_s = 1.0
        threshold_bps = 10.0
        order_usd = 8.0
        max_entry_price = 0.90
        signal_start_s = 210.0
        signal_end_s = 240.0
        clob_base_url = "http://127.0.0.1:8787/clob"
        clob_timeout_s = 1.0
        ask_sample_offsets = "30,150"
        ask_sample_tolerance_s = 7.5

    monkeypatch.setattr(lane, "_build_signal_event", lambda args, prior_signal_ids, now_ts: (None, {"status": "WAITING_SIGNAL_WINDOW"}))
    monkeypatch.setattr(
        lane,
        "_market_for_slug",
        lambda slug, *, timeout_s: {
            "conditionId": "0xabc",
            "outcomes": '["Up","Down"]',
            "clobTokenIds": '["111","222"]',
            "_gamma_route_used": "configured_gamma_route",
            "_gamma_route_attempts": [{"route": "configured_gamma_route", "status": "PASS"}],
        },
    )
    snapshots = {
        "111": {"status": "OK", "best_ask": 0.44, "best_bid": 0.43, "blocking_reason": "none"},
        "222": {"status": "OK", "best_ask": 0.57, "best_bid": 0.56, "blocking_reason": "none"},
    }
    monkeypatch.setattr(
        lane,
        "_book_snapshot",
        lambda *, clob, token_id, order_usd, max_entry_price: dict(snapshots[token_id]),
    )

    state = lane.build_state(Args())

    assert state["summary"]["ask_sample_events"] == 1
    assert state["summary"]["book_verified_fills"] == 0
    assert state["orders"] == []
    sample = state["ask_samples"][0]
    assert sample["target_offset_s"] == 30.0
    assert sample["ask_sample"]["both_sides_have_ask"] is True
    offset_summary = state["summary"]["ask_sample_summary"]["offsets"]["30"]
    assert offset_summary["sample_events"] == 1
    assert offset_summary["real_book_sample_events"] == 1
    assert offset_summary["real_book_unique_windows"] == 1
    assert offset_summary["both_sides_have_ask_pct"] == 100.0
    assert offset_summary["both_sides_have_ask_pct_of_real_books"] == 100.0
    assert offset_summary["outcomes"]["Up"]["ask_present_pct"] == 100.0
    assert offset_summary["outcomes"]["Up"]["ask_present_pct_of_real_books"] == 100.0
    assert offset_summary["outcomes"]["Up"]["best_ask_distribution"]["p50"] == 0.44
    assert offset_summary["outcomes"]["Down"]["best_ask_distribution"]["p50"] == 0.57
    assert event_log_path.read_text().count("e7_fixed_offset_ask_sample") == 1


def test_fixed_offset_ask_sampler_uses_failure_only_direct_fallback(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_CLOB_API_BASE_URL", "http://127.0.0.1:8787/clob")
    monkeypatch.delenv("POLYMARKET_SOURCE_PROXY_URL", raising=False)
    monkeypatch.delenv("POLYMARKET_HTTPS_PROXY", raising=False)
    state_path = tmp_path / "state.json"
    event_log_path = tmp_path / "events.jsonl"
    resolution_path = tmp_path / "resolutions.jsonl"
    state_path.write_text(json.dumps({"signal_events": [], "orders": [], "ask_samples": []}))
    resolution_path.write_text("")

    class Args:
        state = str(state_path)
        event_log = str(event_log_path)
        resolutions = str(resolution_path)
        reset_state = False
        now_ts = 1783332030.0
        symbol = "BTCUSDT"
        timeout_s = 1.0
        threshold_bps = 10.0
        order_usd = 8.0
        max_entry_price = 0.90
        signal_start_s = 210.0
        signal_end_s = 240.0
        clob_base_url = "http://127.0.0.1:8787/clob"
        clob_timeout_s = 1.0
        ask_sample_offsets = "30,150"
        ask_sample_tolerance_s = 7.5

    monkeypatch.setattr(lane, "_build_signal_event", lambda args, prior_signal_ids, now_ts: (None, {"status": "WAITING_SIGNAL_WINDOW"}))
    monkeypatch.setattr(
        lane,
        "_market_for_slug",
        lambda slug, *, timeout_s: {
            "conditionId": "0xabc",
            "outcomes": '["Up","Down"]',
            "clobTokenIds": '["111","222"]',
            "_gamma_route_used": "configured_gamma_route",
            "_gamma_route_attempts": [{"route": "configured_gamma_route", "status": "PASS"}],
        },
    )

    def fake_snapshot(*, clob, token_id, order_usd, max_entry_price):
        if os.environ.get("POLYMARKET_CLOB_API_BASE_URL"):
            return {
                "status": "ERROR",
                "error_type": "HTTPError",
                "error": "503 relay busy",
                "blocking_reason": "book_fetch_error",
                "instant_fill_status": "BLOCKED",
                "route_report": {"routed_host": "127.0.0.1:8787"},
            }
        return {
            "status": "OK",
            "best_ask": 0.45 if token_id == "111" else 0.56,
            "best_bid": 0.44 if token_id == "111" else 0.55,
            "avg_fill_price": 0.45 if token_id == "111" else 0.56,
            "fillable_usd": 8.0,
            "instant_fill_status": "PASS",
            "blocking_reason": "none",
            "route_report": {"clob_host_used": "https://clob.polymarket.com"},
        }

    monkeypatch.setattr(lane, "_book_snapshot", fake_snapshot)

    state = lane.build_state(Args())

    assert state["orders"] == []
    sample = state["ask_samples"][0]
    assert sample["books"]["Up"]["book_route_used"] == "direct_clob_fallback"
    assert sample["books"]["Down"]["book_route_used"] == "direct_clob_fallback"
    assert sample["books"]["Up"]["configured_clob_route_error"]["error"] == "503 relay busy"
    assert sample["ask_sample"]["both_sides_have_ask"] is True
    assert state["summary"]["ask_sample_summary"]["real_book_unique_windows"] == 1
    assert state["summary"]["ask_sample_summary"]["offsets"]["30"]["both_sides_have_ask_pct"] == 100.0
    assert state["summary"]["ask_sample_summary"]["offsets"]["30"]["both_sides_have_ask_pct_of_real_books"] == 100.0


def test_ask_sample_summary_splits_offsets_and_outcomes() -> None:
    samples = [
        {
            "window_start_s": 100,
            "target_offset_s": 30.0,
            "ask_sample": {
                "both_sides_have_ask": True,
                "outcomes": {
                    "Up": {"status": "OK", "has_ask": True, "best_ask": 0.42},
                    "Down": {"status": "OK", "has_ask": True, "best_ask": 0.59},
                },
            },
        },
        {
            "window_start_s": 400,
            "target_offset_s": 30.0,
            "ask_sample": {
                "both_sides_have_ask": False,
                "outcomes": {
                    "Up": {"status": "OK", "has_ask": False, "best_ask": 0.0},
                    "Down": {"status": "OK", "has_ask": True, "best_ask": 0.51},
                },
            },
        },
        {
            "window_start_s": 400,
            "target_offset_s": 150.0,
            "ask_sample": {
                "both_sides_have_ask": True,
                "outcomes": {
                    "Up": {"status": "OK", "has_ask": True, "best_ask": 0.48},
                    "Down": {"status": "OK", "has_ask": True, "best_ask": 0.53},
                },
            },
        },
    ]

    summary = lane.ask_sample_summary(samples)

    assert summary["sample_events"] == 3
    assert summary["unique_windows"] == 2
    assert summary["real_book_sample_events"] == 3
    assert summary["real_book_unique_windows"] == 2
    assert summary["offsets"]["30"]["sample_events"] == 2
    assert summary["offsets"]["30"]["unique_windows"] == 2
    assert summary["offsets"]["30"]["real_book_unique_windows"] == 2
    assert summary["offsets"]["30"]["both_sides_have_ask_pct"] == 50.0
    assert summary["offsets"]["30"]["both_sides_have_ask_pct_of_real_books"] == 50.0
    assert summary["offsets"]["30"]["outcomes"]["Up"]["ask_present_pct"] == 50.0
    assert summary["offsets"]["30"]["outcomes"]["Up"]["ask_present_pct_of_real_books"] == 50.0
    assert summary["offsets"]["30"]["outcomes"]["Down"]["best_ask_distribution"]["p50"] == 0.55
    assert summary["offsets"]["150"]["both_sides_have_ask_pct"] == 100.0


def test_selected_offset_paper_quotes_fill_against_later_book(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "state.json"
    event_log_path = tmp_path / "events.jsonl"
    resolution_path = tmp_path / "resolutions.jsonl"
    state_path.write_text(json.dumps({"signal_events": [], "orders": [], "ask_samples": [], "paper_quote_events": []}))
    resolution_path.write_text("")

    class Args:
        state = str(state_path)
        event_log = str(event_log_path)
        resolutions = str(resolution_path)
        reset_state = False
        now_ts = 1783332030.0
        symbol = "BTCUSDT"
        timeout_s = 1.0
        threshold_bps = 10.0
        order_usd = 8.0
        max_entry_price = 0.90
        signal_start_s = 210.0
        signal_end_s = 240.0
        clob_base_url = "http://127.0.0.1:8787/clob"
        clob_timeout_s = 1.0
        ask_sample_offsets = "30,150"
        ask_sample_tolerance_s = 7.5
        paper_quote_offset_s = 30.0
        maker_quote_tick_size = 0.01
        maker_quote_cancel_before_close_s = 30.0

    monkeypatch.setattr(lane, "_build_signal_event", lambda args, prior_signal_ids, now_ts: (None, {"status": "WAITING_SIGNAL_WINDOW"}))
    monkeypatch.setattr(
        lane,
        "_market_for_slug",
        lambda slug, *, timeout_s: {
            "conditionId": "0xabc",
            "outcomes": '["Up","Down"]',
            "clobTokenIds": '["111","222"]',
            "_gamma_route_used": "configured_gamma_route",
            "_gamma_route_attempts": [{"route": "configured_gamma_route", "status": "PASS"}],
        },
    )
    snapshots = {
        "111": {"status": "OK", "best_ask": 0.44, "best_bid": 0.43, "blocking_reason": "none"},
        "222": {"status": "OK", "best_ask": 0.57, "best_bid": 0.56, "blocking_reason": "none"},
    }
    monkeypatch.setattr(lane, "_book_snapshot", lambda *, clob, token_id, order_usd, max_entry_price: dict(snapshots[token_id]))

    state = lane.build_state(Args())

    assert len(state["paper_quote_events"]) == 2
    assert state["summary"]["paper_quotes"] == 2
    assert state["summary"]["paper_filled_orders"] == 0
    assert {order["final_status"] for order in state["orders"]} == {"OPEN"}
    up_quote = next(row for row in state["paper_quote_events"] if row["outcome"] == "Up")
    assert up_quote["quote_price"] == 0.43

    Args.now_ts = 1783332150.0
    snapshots = {
        "111": {"status": "OK", "best_ask": 0.42, "best_bid": 0.41, "blocking_reason": "none"},
        "222": {"status": "OK", "best_ask": 0.60, "best_bid": 0.59, "blocking_reason": "none"},
    }
    state = lane.build_state(Args())

    assert len(state["paper_quote_events"]) == 2
    assert state["summary"]["paper_quotes"] == 2
    assert state["summary"]["paper_filled_orders"] == 1
    assert state["summary"]["book_verified_fills"] == 1
    orders = {order["outcome"]: order for order in state["orders"]}
    assert orders["Up"]["final_status"] == "FILLED"
    assert orders["Up"]["maker_fill_evidence"]["crossing_rule"] == "later_best_ask_lte_maker_quote"
    assert orders["Down"]["final_status"] == "OPEN"
    assert state["summary"]["paper_quote_fill_topology"]["topology_counts"] == {"one_sided_filled": 1}


def test_equal_shares_paper_size_mode_quotes_same_shares_per_side(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "state.json"
    event_log_path = tmp_path / "events.jsonl"
    resolution_path = tmp_path / "resolutions.jsonl"
    state_path.write_text(json.dumps({"signal_events": [], "orders": [], "ask_samples": [], "paper_quote_events": []}))
    resolution_path.write_text("")

    class Args:
        lane_id = "e7_1_spot_open_btc5m_equal_shares_v1"
        state = str(state_path)
        event_log = str(event_log_path)
        resolutions = str(resolution_path)
        reset_state = False
        now_ts = 1783332030.0
        symbol = "BTCUSDT"
        timeout_s = 1.0
        threshold_bps = 10.0
        order_usd = 8.0
        max_entry_price = 0.90
        signal_start_s = 210.0
        signal_end_s = 240.0
        clob_base_url = "http://127.0.0.1:8787/clob"
        clob_timeout_s = 1.0
        ask_sample_offsets = "30,150"
        ask_sample_tolerance_s = 7.5
        paper_quote_offset_s = 30.0
        paper_size_mode = "equal_shares"
        maker_quote_tick_size = 0.01
        maker_quote_cancel_before_close_s = 30.0

    monkeypatch.setattr(lane, "_build_signal_event", lambda args, prior_signal_ids, now_ts: (None, {"status": "WAITING_SIGNAL_WINDOW"}))
    monkeypatch.setattr(
        lane,
        "_market_for_slug",
        lambda slug, *, timeout_s: {
            "conditionId": "0xabc",
            "outcomes": '["Up","Down"]',
            "clobTokenIds": '["111","222"]',
            "_gamma_route_used": "configured_gamma_route",
            "_gamma_route_attempts": [{"route": "configured_gamma_route", "status": "PASS"}],
        },
    )
    snapshots = {
        "111": {"status": "OK", "best_ask": 0.44, "best_bid": 0.43, "blocking_reason": "none"},
        "222": {"status": "OK", "best_ask": 0.57, "best_bid": 0.56, "blocking_reason": "none"},
    }
    monkeypatch.setattr(lane, "_book_snapshot", lambda *, clob, token_id, order_usd, max_entry_price: dict(snapshots[token_id]))

    state = lane.build_state(Args())

    assert state["lane"] == "e7_1_spot_open_btc5m_equal_shares_v1"
    assert state["parameters"]["paper_size_mode"] == "equal_shares"
    quotes = {row["outcome"]: row for row in state["paper_quote_events"]}
    assert quotes["Up"]["lane"] == "e7_1_spot_open_btc5m_equal_shares_v1"
    assert quotes["Up"]["paper_size_mode"] == "equal_shares"
    assert quotes["Up"]["quote_price"] == 0.43
    assert quotes["Up"]["requested_shares"] == 8.0
    assert quotes["Up"]["order_usd"] == 3.44
    assert quotes["Down"]["quote_price"] == 0.56
    assert quotes["Down"]["requested_shares"] == 8.0
    assert quotes["Down"]["order_usd"] == 4.48
    assert {order["requested_size_usd"] for order in state["orders"]} == {3.44, 4.48}
    assert {order["requested_shares"] for order in state["orders"]} == {8.0}
