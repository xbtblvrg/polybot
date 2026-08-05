from __future__ import annotations

from scripts.report_active_set_post_rotation_windows import build_report


F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
OTHER = "0x32de91fa203321fa7735e7854f2b1c844e71ce9d"


def _row(wallet: str, window: float, reason: str, age: float, *, submits: int = 0, pending: bool = False) -> dict:
    return {
        "source_wallet": wallet,
        "window_start_s": window,
        "market_slug": f"btc-updown-5m-{int(window)}",
        "dominant_skip_reason": reason,
        "latest_observed_age_s": age,
        "source_latest_observed_age_s": age,
        "source_coverage_window": True,
        "participation_adjusted_denominator": True,
        "miss_pending_market_lifecycle": pending,
        "wallet_eligible_orders": 1,
        "our_attempts": submits,
        "our_submits": submits,
        "our_fills": 0,
    }


def test_post_rotation_report_counts_first_three_measured_f418_windows() -> None:
    guard = {
        "window_participation": {
            "rows": [
                _row(OTHER, 1784354700.0, "inventory_window_state_stale", 200.0),
                _row(F418, 1784354700.0, "inventory_window_state_stale", 4.0),
                _row(F418, 1784355000.0, "eligible", 1.2, submits=1),
                _row(F418, 1784355300.0, "inventory_confirmed_unchanged_no_edge", 2.9),
                _row(F418, 1784355600.0, "ignored_fourth", 2.0),
            ]
        }
    }
    rotation = {
        "target_candidate_id": "runtime_auto_degrade_f418d3a1a9",
        "selection_pin": {"created_at": "2026-07-18T06:05:23Z"},
        "edge_snapshot": {"fresh_matching_events_4h": 944},
    }

    report = build_report(guard=guard, rotation=rotation)

    assert report["status"] == "FIRST3_COMPLETE"
    assert report["measured_windows_found"] == 3
    assert report["dominant_skip_reason_distribution"] == {
        "eligible": 1,
        "inventory_confirmed_unchanged_no_edge": 1,
        "inventory_window_state_stale": 1,
    }
    assert report["observed_age_histogram"]["<=3s"] == 2
    assert report["observed_age_histogram"][">3s"] == 1
    assert report["submit_eligible_rows"] == 1
    assert report["si1_reopens"] is False
    assert report["edge_snapshot"]["fresh_matching_events_4h"] == 944


def test_post_rotation_report_collects_until_three_non_pending_windows() -> None:
    guard = {
        "window_participation": {
            "rows": [
                _row(F418, 1784354700.0, "inventory_window_state_stale", 4.0, pending=True),
                _row(F418, 1784355000.0, "inventory_window_state_stale", 4.5),
            ]
        }
    }
    rotation = {"selection_pin": {"created_at": "2026-07-18T06:05:23Z"}}

    report = build_report(guard=guard, rotation=rotation)

    assert report["status"] == "COLLECTING_FIRST3"
    assert report["measured_windows_found"] == 1
    assert report["next_action"] == "continue guard measurement until f418 has 3 measured active windows"


def test_post_rotation_report_uses_decision_time_age_for_submitted_windows() -> None:
    guard = {
        "window_participation": {
            "rows": [
                _row(F418, 1784355300.0, "eligible", 13.7, submits=0),
                _row(F418, 1784355600.0, "inventory_confirmed_unchanged_no_edge", 2.0),
                _row(F418, 1784355900.0, "inventory_confirmed_unchanged_no_edge", 2.5),
            ]
        }
    }
    rotation = {
        "target_candidate_id": "runtime_auto_degrade_f418d3a1a9",
        "selection_pin": {"created_at": "2026-07-18T06:05:23Z"},
    }
    live_execution = {
        "orders": [
            {
                "source_wallet": F418,
                "market_slug": "btc-updown-5m-1784355300",
                "order_id": "0xsubmitted",
                "condition_id": "",
                "outcome": "",
                "submitted_at": "2026-07-18T06:15:11Z",
                "latency_budget": {"intent_built_ts": 1784355310.0},
                "source_intent": {
                    "observed_ts": 1784355307.6,
                    "metadata": {
                        "inventory_v2": {
                            "latest_observed_age_s": 2.4,
                            "latest_observed_ts": 1784355307.6,
                            "live_build_max_observed_age_s": 3.0,
                        }
                    },
                },
            }
        ]
    }

    report = build_report(guard=guard, rotation=rotation, live_execution=live_execution)

    submitted = report["windows"][0]
    assert submitted["submit_eligible"] is True
    assert submitted["matched_live_order_id"] == "0xsubmitted"
    assert submitted["latest_observed_age_s"] == 13.7
    assert submitted["decision_time_observed_age_s"] == 2.4
    assert submitted["observed_age_bucket"] == "<=3s"
    assert report["observed_age_histogram"]["<=3s"] == 3
    assert report["snapshot_observed_age_histogram"][">3s"] == 1
    assert report["decision_time_observed_age_cap_violation_rows"] == 0
    assert report["submitted_decision_time_observed_age_cap_violation_rows"] == 0


def test_post_rotation_report_summarizes_one_row_per_market_window_preferring_submits() -> None:
    up_skip = {**_row(F418, 1784355300.0, "inventory_confirmed_unchanged_no_edge", 10.0), "outcome": "Up"}
    down_submit = {**_row(F418, 1784355300.0, "inventory_residual_gap_below_min_order", 54.0, submits=1), "outcome": "Down"}
    guard = {
        "window_participation": {
            "rows": [
                up_skip,
                down_submit,
                _row(F418, 1784355600.0, "inventory_confirmed_unchanged_no_edge", 2.0),
                _row(F418, 1784355900.0, "inventory_confirmed_unchanged_no_edge", 2.5),
            ]
        }
    }
    rotation = {"selection_pin": {"created_at": "2026-07-18T06:05:23Z"}}
    live_execution = {
        "orders": [
            {
                "source_wallet": F418,
                "market_slug": "btc-updown-5m-1784355300",
                "order_id": "0xdown",
                "outcome": "Down",
                "submitted_at": "2026-07-18T06:15:11Z",
                "source_intent": {"metadata": {"inventory_v2": {"latest_observed_age_s": 2.8}}},
            }
        ]
    }

    report = build_report(guard=guard, rotation=rotation, live_execution=live_execution)

    assert report["measured_windows_found"] == 3
    assert report["windows"][0]["outcome"] == "Down"
    assert report["windows"][0]["matched_live_order_id"] == "0xdown"
    assert report["submit_eligible_rows"] == 1
