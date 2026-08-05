from __future__ import annotations

import json

from scripts.order139_fast_pipe_sampler import capture_lags, guard_sample


def test_capture_lags_deduplicates_and_reports_receive_lag(tmp_path):
    capture = tmp_path / "capture.jsonl"
    wallet = "0xabc"
    rows = [
        {"event_id": "e1", "event_ts": 100, "received_at_s": 101.5, "source_wallet": wallet, "side": "BUY"},
        {"event_id": "e1", "event_ts": 100, "received_at_s": 102, "source_wallet": wallet, "side": "BUY"},
        {"event_id": "e2", "event_ts": 200, "received_at_s": 202.25, "source_wallet": wallet, "side": "BUY"},
    ]
    capture.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    result = capture_lags(capture, wallet=wallet, target_n=10, scan_bytes=10000)
    assert [row["event_id"] for row in result] == ["e1", "e2"]
    assert [row["receive_lag_s"] for row in result] == [1.5, 2.25]


def test_guard_sample_latches_advance_with_zero_fresh_rows_and_cycle_period():
    guard = {
        "generated_at": "2026-08-02T03:00:10Z",
        "guard_code_identity": {"pid": 94607},
        "freshness_discriminator": {
            "selected_wallet": "0x2d7c",
            "fast_pipe_latest_observed_ts": 200.0,
            "policy_compatible_fresh_buy_rows_le_30s": 0,
        },
        "drought_funnel": {"base_intents": 0, "fresh_candidate_intents": 0, "orders_submitted": 0},
    }
    sample, state = guard_sample(
        guard,
        {"fast_pipe_latest_observed_ts": 100.0, "guard_generated_at": "2026-08-02T03:00:00Z", "cumulative_transport_firings": 2},
    )
    assert sample["fast_pipe_advanced_level"] is True
    assert sample["transport_firing"] is True
    assert sample["cumulative_transport_firings"] == 3
    assert sample["guard_cycle_period_s"] == 10.0
    assert state["cumulative_transport_firings"] == 3
