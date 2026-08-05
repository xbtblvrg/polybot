from scripts.report_selection_visibility_packet import build_packet


def test_selection_visibility_packet_merges_fee_rows_and_starts_clock() -> None:
    routing_shadow = {
        "generated_at": "2026-07-10T22:31:00Z",
        "live_orders_allowed": False,
        "summary": {"copyintent_parity_status": "PASS", "copyintent_parity_conflicts": 0},
        "rows": [
            {
                "cycle_generated_at": "2026-07-10T22:31:00Z",
                "extra_would_submit_window": True,
                "market_slug": f"btc-updown-5m-{idx}",
                "selected_wallet_at_cycle": "0xselected",
                "selected_wallet_signal_present": False,
                "window_start_s": float(idx),
                "winning_intent_id": f"ci_{idx}",
                "winning_observed_ts": float(idx) + 1.0,
                "winning_source_wallet": "0xwinner",
                "winning_source_wallet_short": "0xw...nner",
                "would_change_selected_wallet": True,
            }
            for idx in range(10)
        ],
        "fee_gated_measurement_rows": [
            {
                "intent_id": "ci_0",
                "side": "NO",
                "outcome": "Up",
                "limit_price": 0.47,
                "shares": 5.0,
                "expected_fee_usd": 0.087182,
                "realized_paper_outcome": {"status": "RESOLVED", "paper_pnl_usd": 2.65},
            }
        ],
    }
    preregistration = {
        "registered_at": "2026-07-10T22:30:00Z",
        "deadline_utc": "2026-07-11T22:30:00Z",
        "notes": "ORDER2 clarification from Fable DIRECTION add3807: instrumentation fail vs informative fail.",
    }

    packet = build_packet(
        routing_shadow=routing_shadow,
        preregistration=preregistration,
        generated_at="2026-07-10T22:32:00Z",
    )

    assert packet["paper_only"] is True
    assert packet["producing_live_mutation"] is False
    assert packet["summary"]["clock_start_condition_met"] is True
    assert packet["summary"]["sampled_signal_emitted_but_not_selected_windows"] == 10
    assert packet["summary"]["would_submit_pnl_fee_field_rows"] == 1
    assert packet["summary"]["would_submit_measurement_join_counts"]["intent_id"] == 1
    assert packet["summary"]["sampled_would_submit_measurement_join_counts"][
        "routing_shadow_fee_gated_measurement_rows:intent_id"
    ] == 1
    assert packet["summary"]["aggregate_measured_would_submit_post_fee_pnl_usd"] == 2.562818
    assert packet["rows"][0]["selector_reason_code"] == "runtime_selected_wallet_no_routeable_signal"
    assert packet["rows"][0]["would_submit_side"] == "NO"
    assert packet["rows"][0]["would_submit_price"] == 0.47
    assert packet["rows"][0]["would_submit_fee_usd"] == 0.087182
    assert packet["rows"][0]["would_submit_measurement_source"] == "routing_shadow_fee_gated_measurement_rows:intent_id"
    assert "add3807" in packet["preregistration"]["notes"]


def test_selection_visibility_packet_joins_fee_rows_by_window_and_wallet_when_intent_id_differs() -> None:
    packet = build_packet(
        routing_shadow={
            "generated_at": "2026-07-10T22:31:00Z",
            "live_orders_allowed": False,
            "summary": {"copyintent_parity_status": "PASS", "copyintent_parity_conflicts": 0},
            "rows": [
                {
                    "cycle_generated_at": "2026-07-10T22:31:00Z",
                    "extra_would_submit_window": True,
                    "market_slug": "btc-updown-5m-1800000000",
                    "selected_wallet_at_cycle": "0xselected",
                    "selected_wallet_signal_present": False,
                    "window_start_s": 1_800_000_000.0,
                    "winning_intent_id": "missing-intent",
                    "winning_observed_ts": 1_800_000_012.0,
                    "winning_source_wallet": "0xwinner",
                    "winning_source_wallet_short": "0xw...nner",
                    "would_change_selected_wallet": True,
                }
            ],
            "fee_gated_measurement_rows": [
                {
                    "intent_id": "different-intent",
                    "source_wallet": "0xwinner",
                    "market_slug": "btc-updown-5m-1800000000",
                    "window_start_s": 1_800_000_000.0,
                    "observed_ts": 1_800_000_010.0,
                    "side": "YES",
                    "outcome": "UP",
                    "limit_price": 0.40,
                    "shares": 10.0,
                    "expected_fee_usd": 0.168,
                    "realized_paper_outcome": {"status": "RESOLVED", "wins": True, "paper_pnl_usd": 6.0},
                }
            ],
        },
        preregistration={"registered_at": "2026-07-10T22:30:00Z"},
        generated_at="2026-07-10T22:32:00Z",
        min_clock_start_samples=1,
    )

    assert packet["summary"]["clock_start_condition_met"] is True
    assert packet["summary"]["would_submit_measurement_join_counts"] == {
        "market_window_wallet_closest_observed_ts": 1
    }
    assert packet["summary"]["sampled_would_submit_measurement_join_counts"] == {
        "routing_shadow_fee_gated_measurement_rows:market_window_wallet_closest_observed_ts": 1
    }
    assert packet["summary"]["would_submit_pnl_fee_field_coverage_pct"] == 100.0
    assert packet["summary"]["aggregate_measured_would_submit_post_fee_pnl_usd"] == 5.832
    assert packet["rows"][0]["would_submit_measurement_source"] == (
        "routing_shadow_fee_gated_measurement_rows:market_window_wallet_closest_observed_ts"
    )


def test_selection_visibility_packet_waits_for_post_registration_refresh() -> None:
    packet = build_packet(
        routing_shadow={
            "generated_at": "2026-07-10T22:00:00Z",
            "summary": {"copyintent_parity_status": "PASS", "copyintent_parity_conflicts": 0},
            "rows": [{"extra_would_submit_window": True, "market_slug": "btc-updown-5m-1"}],
        },
        preregistration={"registered_at": "2026-07-10T22:30:00Z"},
        generated_at="2026-07-10T22:32:00Z",
    )

    assert packet["summary"]["status"] == "AWAITING_POST_REGISTRATION_SAMPLE"
    assert packet["summary"]["excluded_pre_registration_rows"] == 1
    assert packet["rows"] == []


def test_selection_visibility_packet_falls_back_to_window_wallet_fee_join() -> None:
    packet = build_packet(
        routing_shadow={
            "generated_at": "2026-07-10T22:31:00Z",
            "summary": {"copyintent_parity_status": "PASS", "copyintent_parity_conflicts": 0},
            "rows": [
                {
                    "cycle_generated_at": "2026-07-10T22:31:00Z",
                    "extra_would_submit_window": True,
                    "market_slug": "btc-updown-5m-1783722900",
                    "selected_wallet_at_cycle": "0xselected",
                    "window_start_s": 1783722900.0,
                    "winning_intent_id": "ci_not_in_fee_rows",
                    "winning_observed_ts": 1783722883.7,
                    "winning_source_wallet": "0xwinner",
                }
            ],
            "fee_gated_measurement_rows": [
                {
                    "intent_id": "ci_fee_row",
                    "market_slug": "btc-updown-5m-1783722900",
                    "window_start_s": 1783722900.0,
                    "source_wallet": "0xwinner",
                    "observed_ts": 1783722884.0,
                    "side": "YES",
                    "outcome": "Up",
                    "limit_price": 0.48,
                    "shares": 5.0,
                    "expected_fee_usd": 0.087357,
                    "realized_paper_outcome": {"status": "UNRESOLVED"},
                }
            ],
        },
        preregistration={"registered_at": "2026-07-10T22:30:00Z"},
        generated_at="2026-07-10T22:32:00Z",
    )

    assert packet["summary"]["would_submit_pnl_fee_field_rows"] == 1
    row = packet["rows"][0]
    assert row["would_submit_side"] == "YES"
    assert row["would_submit_price"] == 0.48
    assert row["would_submit_measurement_source"].endswith("market_window_wallet_closest_observed_ts")
