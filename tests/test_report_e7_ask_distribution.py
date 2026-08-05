from __future__ import annotations

import json

from scripts.report_e7_ask_distribution import build_report


def test_e7_ask_distribution_splits_populated_empty_partial_and_route_error(tmp_path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "updated_at": "2026-07-06T11:40:00+00:00",
                "paper_only": True,
                "live_orders_allowed": False,
                "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
                "summary": {"ask_sample_summary": {"real_book_unique_windows": 2}},
                "delta_windows": [
                    {
                        "window_start_s": 100,
                        "market_slug": "btc-updown-5m-100",
                        "best_spot_status": "SIGNAL",
                        "best_abs_delta_bps": 12.3,
                    },
                    {
                        "window_start_s": 400,
                        "market_slug": "btc-updown-5m-400",
                        "best_spot_status": "NO_SIGNAL_THRESHOLD",
                        "best_abs_delta_bps": 2.3,
                    },
                    {
                        "window_start_s": 700,
                        "market_slug": "btc-updown-5m-700",
                        "best_spot_status": "SIGNAL",
                        "best_abs_delta_bps": 15.0,
                    },
                ],
                "ask_samples": [
                    {
                        "window_start_s": 100,
                        "target_offset_s": 30.0,
                        "ask_sample": {
                            "outcomes": {
                                "Up": {"status": "OK", "has_ask": True, "best_ask": 0.42},
                                "Down": {"status": "OK", "has_ask": True, "best_ask": 0.58},
                            }
                        },
                    },
                    {
                        "window_start_s": 100,
                        "target_offset_s": 150.0,
                        "ask_sample": {
                            "outcomes": {
                                "Up": {"status": "OK", "has_ask": False, "best_ask": 0.0},
                                "Down": {"status": "OK", "has_ask": True, "best_ask": 0.63},
                            }
                        },
                    },
                    {
                        "window_start_s": 400,
                        "target_offset_s": 30.0,
                        "ask_sample": {
                            "outcomes": {
                                "Up": {"status": "OK", "has_ask": False, "best_ask": 0.0},
                                "Down": {"status": "OK", "has_ask": False, "best_ask": 0.0},
                            }
                        },
                    },
                    {
                        "window_start_s": 700,
                        "target_offset_s": 30.0,
                        "ask_sample": {
                            "outcomes": {
                                "Up": {"status": "ERROR", "has_ask": False, "best_ask": 0.0},
                                "Down": {"status": "ERROR", "has_ask": False, "best_ask": 0.0},
                            }
                        },
                    },
                ],
            }
        )
    )

    report = build_report(state_path)

    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False
    assert report["zero_live_assertion"]["orders_submitted"] == 0
    assert report["gate"]["current_unique_real_book_windows"] == 2
    assert report["gate"]["ready_for_fable_ruling"] is False
    assert report["sample_totals"]["classes"] == {
        "empty": 1,
        "partial": 1,
        "populated": 1,
        "route_error": 1,
    }
    assert report["offsets"]["30"]["populated_samples"] == 1
    assert report["offsets"]["30"]["empty_samples"] == 1
    assert report["offsets"]["30"]["route_error_samples"] == 1
    assert report["offsets"]["150"]["partial_samples"] == 1
    assert report["offsets"]["30"]["outcomes"]["Up"]["best_ask_distribution"]["p50"] == 0.42
    assert report["accrual_diagnosis"]["window_reason_counts"] == {
        "real_book_sampled": 2,
        "route_error": 1,
    }


def test_e7_ask_distribution_names_unsampled_signal_and_no_signal_windows(tmp_path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "delta_windows": [
                    {"window_start_s": 100, "market_slug": "btc-updown-5m-100", "best_spot_status": "SIGNAL"},
                    {
                        "window_start_s": 400,
                        "market_slug": "btc-updown-5m-400",
                        "best_spot_status": "NO_SIGNAL_THRESHOLD",
                    },
                ],
                "ask_samples": [],
            }
        )
    )

    report = build_report(state_path)

    assert report["accrual_diagnosis"]["window_reason_counts"] == {"no_signal": 1, "sampler_idle": 1}
    assert [row["reason"] for row in report["accrual_diagnosis"]["unsampled_windows"]] == [
        "sampler_idle",
        "no_signal",
    ]
