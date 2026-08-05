from scripts.report_weekend_copy_shadows import (
    FAK_EXPERIMENT,
    HOUR_EXPERIMENT,
    WINDOW_EXPERIMENT,
    build_packets,
)


def test_weekend_shadow_packets_are_read_only_and_prospective_fak_only() -> None:
    live_state = {
        "orders": [
            {
                "status": "FILLED",
                "submitted_at": "2026-07-19T12:01:00Z",
                "market_slug": "btc-updown-5m-1784462400",
                "limit_price": 0.4,
                "source_intent": {"wallet_usdc_size": 25.0},
            }
        ]
    }
    resolutions = {}
    reject_report = {
        "fak_no_match_analysis": {
            "rows": [
                {
                    "submitted_at": "2026-07-20T11:51:00Z",
                    "market_slug": "btc-updown-5m-1784548200",
                    "outcome": "Up",
                    "submit_to_window_close_s": 100,
                    "book_state_at_submit": {"verdict": "ask_at_or_inside_limit"},
                    "same_window_outcome": {"same_window_eventual_fill": False},
                }
            ]
        }
    }
    registrations = {
        WINDOW_EXPERIMENT: "2026-07-20T11:50:53Z",
        HOUR_EXPERIMENT: "2026-07-20T11:50:53Z",
        FAK_EXPERIMENT: "2026-07-20T11:50:53Z",
    }

    window, hour, fak = build_packets(
        live_state,
        resolutions,
        reject_report,
        registrations,
        generated_at="2026-07-20T11:52:00Z",
    )

    assert window["live_path_mutated"] is False
    assert hour["paper_only"] is True
    assert fak["summary"]["historical_baseline_observations"] == 0
    assert fak["summary"]["prospective_observations"] == 1
    assert fak["summary"]["prospective_requote_eligible"] == 1


def test_fak_shadow_excludes_recovered_and_late_rows() -> None:
    rows = [
        {
            "submitted_at": "2026-07-20T11:51:00Z",
            "submit_to_window_close_s": 9,
            "book_state_at_submit": {"verdict": "ask_at_or_inside_limit"},
            "same_window_outcome": {"same_window_eventual_fill": False},
        },
        {
            "submitted_at": "2026-07-20T11:52:00Z",
            "submit_to_window_close_s": 100,
            "book_state_at_submit": {"verdict": "ask_at_or_inside_limit"},
            "same_window_outcome": {"same_window_eventual_fill": True},
        },
    ]
    _, _, fak = build_packets(
        {"orders": []},
        {},
        {"fak_no_match_analysis": {"rows": rows}},
        {FAK_EXPERIMENT: "2026-07-20T11:50:53Z"},
        generated_at="2026-07-20T11:53:00Z",
    )

    assert fak["summary"]["prospective_observations"] == 2
    assert fak["summary"]["prospective_requote_eligible"] == 0
