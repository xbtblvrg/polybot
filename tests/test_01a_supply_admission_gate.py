from scripts.report_01a_supply_admission_gate import build_report


ACTIVE = "0x" + "a" * 40
CANDIDATE = "0x" + "b" * 40
UNDERPOWERED = "0x" + "c" * 40


def test_gate_covers_enabled_and_queue_and_refuses_underpowered_extrapolation() -> None:
    report = build_report(
        census={
            "generated_at": "2026-08-05T00:00:00Z",
            "ranking": [
                {"wallet": ACTIVE, "span_days": 8.0, "observed_btc5m_window_count": 120, "qualifying_window_count": 0},
                {"wallet": CANDIDATE, "span_days": 10.0, "observed_btc5m_window_count": 150, "qualifying_window_count": 20},
                {"wallet": UNDERPOWERED, "span_days": 6.0, "observed_btc5m_window_count": 99, "qualifying_window_count": 50},
            ]
        },
        active_set={"members": [{"source_wallet": ACTIVE, "enabled": True}]},
        ready_queue={"ranked_members": [{"wallet": CANDIDATE}, {"wallet": UNDERPOWERED}]},
        standings={"generated_at": "2026-08-05T01:00:00Z", "standings": [{"wallet": CANDIDATE, "resolved_orders": 2, "second_half_post_fee_pnl_usd": 1.5}]},
        generated_at="2026-08-05T01:00:00Z",
    )
    rows = {row["wallet"]: row for row in report["rows"]}
    assert set(rows) == {ACTIVE, CANDIDATE, UNDERPOWERED}
    assert rows[CANDIDATE]["qualifying_01a_windows_per_day"] == 2.0
    assert rows[CANDIDATE]["candidate_admission_gate_pass"] is True
    assert rows[UNDERPOWERED]["qualifying_01a_windows_per_day"] == "NO_MEASUREMENT"
    assert rows[UNDERPOWERED]["candidate_admission_gate_pass"] is False
    assert report["eligible_replacement_wallets"] == [CANDIDATE]
    assert report["rotation_authorized"] is False


def test_negative_holdout_cannot_pass_candidate_gate() -> None:
    report = build_report(
        census={"generated_at": "2026-08-05T00:00:00Z", "ranking": [{"wallet": CANDIDATE, "span_days": 7.0, "observed_btc5m_window_count": 100, "qualifying_window_count": 7}]},
        active_set={"members": []},
        ready_queue={"ranked_members": [{"wallet": CANDIDATE}]},
        standings={"generated_at": "2026-08-05T01:00:00Z", "standings": [{"wallet": CANDIDATE, "resolved_orders": 2, "second_half_post_fee_pnl_usd": -0.01}]},
        generated_at="2026-08-05T01:00:00Z",
    )
    row = report["rows"][0]
    assert row["measurement_status"] == "MEASURED"
    assert row["candidate_admission_gate_pass"] is False
    assert "holdout_ev_negative" in row["measurement_deficits"]
    assert report["status"] == "NO_ADMISSIBLE_REPLACEMENT"


def test_resolved_census_wallet_expands_scope_but_stale_inputs_refuse() -> None:
    report = build_report(
        census={"generated_at": "2026-08-03T00:00:00Z", "ranking": [{"wallet": CANDIDATE, "span_days": 7.0, "observed_btc5m_window_count": 100, "qualifying_window_count": 10}]},
        active_set={"members": []},
        ready_queue={"ranked_members": []},
        standings={"generated_at": "2026-08-05T00:00:00Z", "standings": [{"wallet": CANDIDATE, "resolved_orders": 3, "second_half_post_fee_pnl_usd": 1.0}]},
        generated_at="2026-08-05T00:00:00Z",
    )
    assert report["resolved_dual_leg_candidate_count"] == 1
    assert report["dual_leg_cohort_overlap"]["census_intersect_resolved"] == 1
    assert report["rows"][0]["measurement_status"] == "STALE_INPUT_REFUSED"
    assert report["rows"][0]["candidate_admission_gate_pass"] is False


def test_seven_contiguous_partitions_can_mature_using_partition_freshness() -> None:
    partitions = []
    for day_index, day in enumerate(range(6, 13)):
        epochs = list(range(day_index * 300, day_index * 300 + 200))
        partitions.append({
            "day_utc": f"2026-08-{day:02d}",
            "generated_at": "2026-08-13T00:10:00Z",
            "rows": [{"wallet": CANDIDATE, "observed_window_epochs": epochs, "qualifying_01a_window_epochs": epochs[:2]}],
        })
    report = build_report(
        census={"generated_at": "2026-08-03T00:00:00Z", "ranking": []},
        active_set={"members": []},
        ready_queue={"ranked_members": [{"wallet": CANDIDATE}]},
        standings={"generated_at": "2026-08-13T00:00:00Z", "standings": [{"wallet": CANDIDATE, "resolved_orders": 4, "second_half_post_fee_pnl_usd": 1.0}]},
        partitions=partitions,
        accumulator_state={"initialized_at": "2026-08-05T01:00:00Z", "next_byte_offset": 1},
        generated_at="2026-08-13T01:00:00Z",
    )
    row = report["rows"][0]
    assert report["partition_maturity"]["status"] == "READY"
    assert report["input_freshness"]["supply_input_selected"] == "daily_partitions"
    assert row["observed_market_windows"] == 1400
    assert row["measurement_status"] == "MEASURED"
    assert row["candidate_admission_gate_pass"] is True


def test_gap_and_thin_day_move_projected_maturity_from_current_run() -> None:
    partitions = [
        {
            "day_utc": f"2026-08-{day:02d}",
            "generated_at": "2026-08-13T00:10:00Z",
            "distinct_window_epoch_count": 200 if day != 8 else 1,
            "rows": [],
        }
        for day in range(6, 13)
    ]
    report = build_report(
        census={"generated_at": "2026-08-13T00:00:00Z", "ranking": []},
        active_set={"members": []},
        ready_queue={"ranked_members": []},
        standings={"generated_at": "2026-08-13T00:00:00Z", "standings": []},
        partitions=partitions,
        accumulator_state={"initialized_at": "2026-08-05T01:00:00Z", "next_byte_offset": 1},
        generated_at="2026-08-13T01:00:00Z",
    )
    assert report["partition_maturity"]["contiguous_completed_partition_count"] == 4
    assert report["partition_maturity"]["projected_first_measured_cut_date"] == "2026-08-16"
    thin = next(row for row in report["partition_maturity"]["partition_coverage"] if row["day_utc"] == "2026-08-08")
    assert thin["counted_as_complete"] is False
