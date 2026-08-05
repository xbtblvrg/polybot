from scripts.report_campaign_lat_packet import build_packet


def test_campaign_lat_packet_separates_live_day_from_trailing_coverage() -> None:
    packet = build_packet(
        coverage_gap={
            "generated_at": "2026-07-10T18:50:00Z",
            "summary": {
                "windows_total": 288,
                "submitted_windows": 169,
                "zero_submission_windows": 119,
                "unobserved_zero_submission_windows": 101,
                "dominant_reason_class": "no-eligible-signal",
                "op_volume_target_windows": 144,
            },
        },
        signal_supply={
            "generated_at": "2026-07-10T18:51:00Z",
            "summary": {
                "unobserved_no_signal_windows": 101,
                "sources_traded_but_unobserved_windows": 99,
                "sources_idle_windows": 2,
                "fetch_complete": True,
                "dominant_class": "sources_traded_but_unobserved",
                "root_cause": "participation_rollup_retention_gap_not_source_ingest",
            },
        },
        routing_disambiguation={
            "generated_at": "2026-07-10T18:52:00Z",
            "summary": {
                "candidate_windows": 99,
                "sampled_windows": 20,
                "dominant_class": "signal-emitted-but-not-selected",
                "class_counts": {"signal-emitted-but-not-selected": 20},
                "selected_wallet_at_report_time": "0xe6db20932faf0f9780acf75d95c74c9984407dac",
            },
        },
        state_digest={
            "generated_at": "2026-07-10T18:53:00Z",
            "volume": {
                "windows_filled": 134,
                "windows_submitted": 139,
                "denominator_windows": 288,
                "missed_active_windows": 0,
                "incident_triggered": False,
            },
            "runtime_speed_baseline": {
                "status": "PASS",
                "metrics": {"signal_age_p90_s": 1.0, "signal_to_order_p90_s": 98.0},
            },
        },
        routing_shadow={
            "generated_at": "2026-07-10T18:54:00Z",
            "summary": {
                "copyintent_parity_status": "PASS",
                "copyintent_parity_conflicts": 0,
                "extra_would_submit_post_fee_measurement": {
                    "measured_unique_windows": 22,
                    "post_fee_pnl_usd": -3.774701,
                    "pre_fee_pnl_usd": -1.856385,
                    "wins": 10,
                    "losses": 12,
                    "gate_result": "NO_FLIP",
                },
            },
        },
        stage2_verdict={
            "generated_at": "2026-07-10T18:44:59Z",
            "verdict": "NO_FLIP",
            "evidence": {"measured_unique_windows": 22, "post_fee_pnl_usd": -3.774701},
        },
        generated_at="2026-07-10T18:55:00Z",
    )

    assert packet["summary"]["live_day_status"] == "LIVE_DAY_UNDER_TARGET"
    assert packet["summary"]["live_day_submitted_gap_to_144"] == 5
    assert packet["summary"]["trailing_24h_coverage_status"] == "TRAILING_24H_TARGET_CLEAR"
    assert packet["summary"]["primary_constraint"] == "retention_selection_visibility"
    assert packet["summary"]["stage2_verdict"] == "INTERIM_NO_FLIP_NOT_FREEZE_OF_RECORD"
    assert packet["summary"]["stage2_interim_verdict"] == "NO_FLIP"
    assert packet["summary"]["stage2_freeze_of_record_status"] == "NOT_DUE_INTERIM_ONLY"
    assert packet["live_orders_allowed"] is False
    assert packet["producing_live_mutation"] is False
    assert [row["id"] for row in packet["recommended_next_actions"]][-1] == "NO_ELIGIBILITY_LOOSENING"


def test_campaign_lat_packet_handles_missing_optional_inputs() -> None:
    packet = build_packet(
        coverage_gap={},
        signal_supply={},
        routing_disambiguation={},
        state_digest={},
        routing_shadow={},
        stage2_verdict={},
        generated_at="2026-07-10T18:55:00Z",
    )

    assert packet["status"] == "P1_PACKET_READY"
    assert packet["summary"]["live_day_status"] == "LIVE_DAY_UNDER_TARGET"
    assert packet["summary"]["primary_constraint"] == ""
