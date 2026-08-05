import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import scripts.update_state_digest as update_state_digest
from scripts.report_active_set_pin_consumer_sweep import build_report as build_pin_consumer_sweep
from scripts.update_state_digest import build_digest


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _d16_order(ts: str, price: float, *, resolved: bool = True, cost: float = 1.0) -> dict:
    return {
        "final_status": "FILLED",
        "updated_at": ts,
        "alternate_transport_attribution": {
            "resolution_status": "RESOLVED" if resolved else "PENDING"
        },
        "lifecycle": [
            {
                "status": "LIVE_FILLED",
                "ts": ts,
                "payload": {
                    "realized_entry_price": price,
                    "response_filled_size_usd": cost,
                },
            }
        ],
    }


def test_d16_acceptance_waits_for_ruled_sample_and_full_days() -> None:
    result = update_state_digest._d16_entry_band_acceptance(
        [_d16_order("2026-08-04T13:00:00Z", 0.30)],
        activation_at="2026-08-04T12:37:39Z",
        now=datetime(2026, 8, 5, 12, tzinfo=UTC),
    )

    assert result["status"] == "PENDING_EVIDENCE"
    assert result["resolved_fill_sample"]["count"] == 1
    assert result["resolved_fill_sample"]["in_band_rate"] == 1.0
    assert result["out_of_band_cost_usd_total"] == 0.0
    assert result["rollback_to_0_50_allowed"] is False


def test_d16_acceptance_fails_immediately_on_any_out_of_band_fill() -> None:
    result = update_state_digest._d16_entry_band_acceptance(
        [_d16_order("2026-08-04T13:00:00Z", 0.24, resolved=False, cost=1.04)],
        activation_at="2026-08-04T12:37:39Z",
        now=datetime(2026, 8, 4, 14, tzinfo=UTC),
    )

    assert result["status"] == "FAIL_OUT_OF_BAND_FILL"
    assert result["out_of_band_fill_count"] == 1
    assert result["out_of_band_cost_usd_by_day"] == {"2026-08-04": 1.04}


def test_d16_acceptance_routes_low_daily_volume_to_supply_measurement() -> None:
    orders = [
        _d16_order(f"2026-08-05T0{hour}:00:00Z", 0.30)
        for hour in range(2)
    ]
    result = update_state_digest._d16_entry_band_acceptance(
        orders,
        activation_at="2026-08-04T12:37:39Z",
        now=datetime(2026, 8, 6, 1, tzinfo=UTC),
    )

    assert result["status"] == "FAIL_DAILY_SUPPLY"
    assert result["first_two_full_utc_days"][0]["fills_01a"] == 2
    assert "D16-3" in result["next_action"]
    assert result["rollback_to_0_50_allowed"] is False


def test_goal_reachability_governs_on_observed_realized_closed_leg() -> None:
    result = update_state_digest._goal_reachability(
        guard={
            "guard_runtime_filter": {
                "price_band_decision_min_price": 0.25,
                "price_band_decision_max_price": 0.32,
                "per_window_fill_cap": 1,
            }
        },
        guard_caps={"max_order_usd": 8.0},
        measured_band={"post_fee_roi_pct": 26.253203},
        day_actual_pnl_usd=-10.559992,
        roi_evidence={
            "subbands": {
                "00_below_25": {
                    "chronological_holdout": {"post_fee_roi_pct": -100.0},
                    "sample_gate": {"status": "ACCRUING"},
                }
            }
        },
        in_band_fill_rate=5 / 9,
        realized_entry_events=[
            {
                "realized_entry_band": "00_00_25",
                "out_of_band_fill": True,
            }
            for _ in range(4)
        ],
        now=datetime(2026, 8, 4, 12, tzinfo=UTC),
    )

    governing = result["cap_to_goal"]["governing_rung"]
    rung = next(
        row
        for row in result["cap_to_goal"]["rungs"]
        if row["roi_basis"] == "holdout_headline"
        and row["supply_basis"] == "perfect_288"
    )
    assert governing["in_band_fill_rate_h"] == 5 / 9
    assert rung["out_of_band_fill_rate"] == 0.444444444
    assert rung["closed_leg_roi_pct"] == -100.0
    assert rung["observed_closed_leg_subband"] == "00_00_25"
    assert rung["observed_closed_leg_fill_count"] == 4
    assert rung["closed_leg_sample_gate_status"] == "ACCRUING"
    assert rung["hypothetical_h"] == 1.0
    assert rung["hypothetical_h_one_blended_roi_pct"] == 26.253203
    assert result["cap_to_goal"]["governing_verdict"] == "UNREACHABLE_AT_ANY_CAP"


def test_goal_reachability_marks_zero_eligible_fill_clock_undefined() -> None:
    result = update_state_digest._goal_reachability(
        guard={"guard_runtime_filter": {"per_window_fill_cap": 1}},
        guard_caps={"max_order_usd": 1.0},
        measured_band={"post_fee_roi_pct": 10.0, "rows": 40},
        day_actual_pnl_usd=0.0,
        floor_gate_enforced_fill_count=0,
        floor_gate_observation_started_at="2026-08-05T00:05:24Z",
        eligible_intent_count=0,
        now=datetime(2026, 8, 5, 1, tzinfo=UTC),
    )
    gate = result["live_price_gate"]
    clock = gate["first_30_enforced_fill_attribution"]
    assert gate["realized_entry_binding_status"] == "UNDEFINED_ZERO_ELIGIBLE"
    assert clock["projected_maturity_at"] is None
    assert clock["projection_basis"]["observed_fill_rate_per_hour"] is None
    assert result["goal_floor_reachable_today"] is False
    assert (
        result["goal_floor_reachability_basis"]["metric"]
        == "perfect_288_profit_ceiling_usd_per_day"
    )


def test_goal_reachability_projects_fill_clock_from_observed_rate() -> None:
    result = update_state_digest._goal_reachability(
        guard={"guard_runtime_filter": {"per_window_fill_cap": 1}},
        guard_caps={"max_order_usd": 1.0},
        measured_band={"post_fee_roi_pct": 10.0, "rows": 40},
        day_actual_pnl_usd=0.0,
        floor_gate_enforced_fill_count=3,
        floor_gate_observation_started_at="2026-08-05T00:00:00Z",
        eligible_intent_count=1,
        now=datetime(2026, 8, 5, 1, tzinfo=UTC),
    )
    clock = result["live_price_gate"]["first_30_enforced_fill_attribution"]
    assert clock["status"] == "ACCRUING_WITH_ETA"
    assert clock["projection_basis"]["observed_fill_rate_per_hour"] == 3.0
    assert clock["projected_maturity_at"] == "2026-08-05T10:00:00Z"


def test_digest_reports_every_arming_selection_surface() -> None:
    surfaces = update_state_digest._live_selection_surfaces(
        {
            "guard_side_halt_signal": {
                "runtime_member_submittability": {
                    "source_wallet": "0xruntime",
                    "policy_id": "runtime-policy",
                }
            }
        },
        {
            "latest_nonempty_bridge_report": {
                "selected_wallet": "0xbridge",
                "selected_policy_id": "bridge-policy",
            },
            "terminal_ring": [
                {"source_wallet": "0xruntime", "orders_submitted": 1},
                {"source_wallet": "0xbridge", "orders_submitted": 2},
                {"source_wallet": "0xbridge", "orders_submitted": 1},
            ],
        },
    )

    assert surfaces == [
        {
            "surface": "deadman_runtime_member",
            "wallet": "0xruntime",
            "policy_id": "runtime-policy",
            "submitted_orders_last_ring": 1,
            "source_artifact": "data/research/order_flow_deadman_state.json",
        },
        {
            "surface": "alternate_transport_bridge",
            "wallet": "0xbridge",
            "policy_id": "bridge-policy",
            "submitted_orders_last_ring": 3,
            "source_artifact": (
                "data/research/wallet_copy_orderfilled_fast_lane_state.json"
            ),
        },
    ]


def test_deadman_admission_publish_health_summarizes_d9_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "admission.jsonl"
    rows = [
        {
            "kind": "order_flow_deadman_admission_publish",
            "stage": "early",
            "publish_lag_s": 1.0,
            "publish_window_start_s": 100,
            "crossed": False,
            "authorized": True,
        },
        {
            "kind": "order_flow_deadman_admission_publish",
            "stage": "candidate_selection_60s_heartbeat",
            "publish_lag_s": 2.0,
            "publish_window_start_s": 100,
            "crossed": False,
            "authorized": False,
        },
        {
            "kind": "order_flow_deadman_admission_publish",
            "stage": "final",
            "publish_lag_s": 3.0,
            "publish_window_start_s": 200,
            "crossed": True,
            "authorized": False,
            "status": "ADMISSION_STALE_WINDOW",
        },
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))

    health = update_state_digest._deadman_admission_publish_health(path)

    assert health["sample_count"] == 3
    assert health["publish_lag_p95_s"] == 3.0
    assert health["publish_counts_by_window"] == {"100": 2, "200": 1}
    assert health["authorized_crossings"] == 0
    assert health["stale_window_refusals"] == 1
    assert health["latest"]["stage"] == "final"


def test_scorecard_for_digest_refuses_stale_day_when_fresh_build_unavailable(
    tmp_path: Path, monkeypatch
) -> None:
    data = tmp_path / "data" / "research"
    stale = {
        "kind": "wallet_copy_daily_scorecard",
        "day_utc": "2026-07-31",
        "today": {"total": {"pnl_usd": -99.0, "resolved_fills": 7}},
        "canonical_pnl_truth": {"by_day": {"2026-07-31": {"pnl_usd": -99.0}}},
        "day_pnl_basis": {"day_pnl_response_basis": -99.0},
        "since_topup_truth": {"actual_delta_vs_baseline_usd": -18.0},
    }
    _write_json(data / "wallet_copy_daily_scorecard_2026-07-31.json", stale)
    monkeypatch.setattr(update_state_digest, "_current_scorecard", lambda _root: {})
    monkeypatch.setattr(update_state_digest, "ROOT", tmp_path)

    selected = update_state_digest._scorecard_for_digest(tmp_path, data)

    assert selected["day_utc"] == datetime.now(UTC).date().isoformat()
    assert selected["current_day_scorecard_status"] == "UNAVAILABLE_STALE_DAY_DATA_REFUSED"
    assert selected["stale_scorecard_day_utc"] == "2026-07-31"
    assert selected["today"] == {}
    assert selected["canonical_pnl_truth"] == {}
    assert selected["since_topup_truth"] == {"actual_delta_vs_baseline_usd": -18.0}


def test_current_scorecard_timeout_exceeds_measured_direct_runtime_threshold() -> None:
    assert (
        update_state_digest.SCORECARD_CURRENT_BUILD_TIMEOUT_S
        > update_state_digest.SCORECARD_DIRECT_RUNTIME_THRESHOLD_S
    )


def test_goal_reachability_uses_most_restrictive_live_cap_without_mutation() -> None:
    result = update_state_digest._goal_reachability(
        guard={
            "active_set": {
                "members": [
                    {
                        "source_wallet": "0xabc",
                        "policy_id": "measured-policy",
                        "max_order_usd": 0.5,
                    }
                ]
            },
            "guard_runtime_filter": {"per_window_fill_cap": 1},
        },
        guard_caps={"max_order_usd": 8.0},
        measured_band={"resolved_fills": 1410, "roi_pct": 3.0},
        day_actual_pnl_usd=-4.0,
        now=datetime(2026, 8, 2, 12, 0, tzinfo=UTC),
    )

    assert result["status"] == "CAP_BOUND"
    assert result["measurement_only"] is True
    assert result["live_mutation"] is False
    assert result["effective_max_order_usd"] == 0.5
    assert result["per_window_fill_cap"] == 1
    assert result["windows_remaining"] == 144
    assert result["remaining_profit_ceiling_usd"] == 2.16
    assert result["max_achievable_day_usd"] == -1.84


def test_goal_reachability_can_pass_when_measured_ceiling_reaches_floor() -> None:
    result = update_state_digest._goal_reachability(
        guard={
            "active_set": {"members": [{"max_order_usd": 30.0}]},
            "guard_runtime_filter": {"per_window_fill_cap": 1},
        },
        guard_caps={"max_order_usd": 30.0},
        measured_band={"resolved_fills": 200, "roi_pct": 3.0},
        day_actual_pnl_usd=0.0,
        now=datetime(2026, 8, 2, 12, 0, tzinfo=UTC),
    )

    assert result["status"] == "PASS"
    assert result["max_achievable_day_usd"] == 129.6


def test_goal_reachability_reports_01a_supply_bound_from_day_bounded_holdout() -> None:
    result = update_state_digest._goal_reachability(
        guard={
            "active_set": {"members": [{"max_order_usd": 4.0}]},
            "active_set_runtime": {
                "runtime_member_submittability": {"effective_max_order_usd": 4.0}
            },
            "guard_runtime_filter": {"per_window_fill_cap": 1},
        },
        guard_caps={"max_order_usd": 8.0},
        measured_band={
            "rows": 45,
            "post_fee_roi_pct": 16.809873,
            "first_submitted_at": "2026-07-19T05:35:27Z",
            "last_submitted_at": "2026-08-02T14:05:51Z",
        },
        day_actual_pnl_usd=-3.111974,
        now=datetime(2026, 8, 2, 17, 0, tzinfo=UTC),
    )

    assert result["status"] == "SUPPLY_BOUND"
    assert result["measured_band"] == "01a_25_32"
    assert result["effective_max_order_usd"] == 4.0
    assert result["supply"]["calendar_span_days"] == 15
    assert result["supply"]["qualifying_windows_per_day"] == 3.0
    assert result["supply"]["required_qualifying_windows_per_day"] == 148.722123
    assert result["supply"]["observed_supply_profit_ceiling_usd_per_day"] == 2.017185


def test_goal_reachability_prefers_source_side_windows_over_gated_fills() -> None:
    green = "0xgreen"
    source_rows = []
    for window in range(7):
        source_rows.append(
            {
                "event_id": f"base-{window}",
                "source_wallet": green,
                "market_slug": f"btc-updown-5m-{1_800_000_000 + window * 300}",
                "action": "BUY",
                "price": 0.5,
                "source": "polygon_http_getLogs_tail",
                "paper_only": True,
            }
        )
        if window < 6:
            source_rows.append(
                {
                    "event_id": f"01a-{window}",
                    "source_wallet": green,
                    "market_slug": f"btc-updown-5m-{1_800_000_000 + window * 300}",
                    "action": "BUY",
                    "price": 0.3,
                    "source": "polygon_ws",
                    "paper_only": True,
                }
            )

    result = update_state_digest._goal_reachability(
        guard={
            "active_set": {"members": [{"max_order_usd": 1.0}]},
            "guard_runtime_filter": {"per_window_fill_cap": 1},
        },
        guard_caps={"max_order_usd": 8.0},
        measured_band={
            "rows": 47,
            "post_fee_roi_pct": 26.25,
            "first_submitted_at": "2026-07-19T00:00:00Z",
            "last_submitted_at": "2026-08-02T00:00:00Z",
        },
        source_side_supply={
            "generated_at": "2026-08-04T05:57:28Z",
            "prospective_current_market": {
                "identity_clean_events": source_rows,
                "actuator_consumption_gate": {
                    "exact_policy_chronological_holdout_by_wallet": {
                        green: {
                            "exact-green": {
                                "passed": True,
                                "wide_policy_fingerprint": "exact-green",
                            },
                        },
                        "0xred": {"passed": False, "wide_policy_fingerprint": None},
                    }
                },
            },
        },
        day_actual_pnl_usd=-6.199995,
        now=datetime(2026, 8, 4, 6, 0, tzinfo=UTC),
    )

    supply = result["supply"]
    assert supply["authority"] == "qualified_pool_source_side_distinct_market_windows"
    assert supply["qualifying_windows_per_day"] == 246.857143
    assert supply["observed_supply_profit_ceiling_usd_per_day"] == 64.8
    assert supply["source_side"]["push_observed_01a_market_windows"] == 6
    assert supply["gated_fill_counterfactual"]["qualifying_windows_per_day"] == 3.133333
    assert supply["gated_fill_counterfactual"]["profit_ceiling_usd_per_day"] == 0.8225


def _cap_to_goal_result(
    in_band_fill_rate: float | None = 1.0,
    *,
    active_set_registry: dict | None = None,
    realized_live_participation_windows: int = 6,
    resolved_live_fills_at_current_h: int = 0,
    since_topup_actual_usd: float | None = None,
    restart_acceptance: dict | None = None,
) -> dict:
    green = "0xgreen"
    source_rows = []
    for window in range(7):
        market = f"btc-updown-5m-{1_800_000_000 + window * 300}"
        source_rows.append(
            {
                "event_id": f"base-{window}",
                "source_wallet": green,
                "market_slug": market,
                "action": "BUY",
                "price": 0.5,
                "source": "polygon_http_getLogs_tail",
                "paper_only": True,
            }
        )
        if window < 5:
            source_rows.append(
                {
                    "event_id": f"01a-{window}",
                    "source_wallet": green,
                    "market_slug": market,
                    "action": "BUY",
                    "price": 0.3,
                    "source": "polygon_ws",
                    "paper_only": True,
                }
            )
    return update_state_digest._goal_reachability(
        guard={
            "active_set": {
                "members": [{"source_wallet": "0xlive", "max_order_usd": 0.5}]
            },
            "active_set_runtime": {
                "runtime_member_submittability": {"effective_max_order_usd": 1.0}
            },
            "guard_runtime_filter": {"per_window_fill_cap": 1},
        },
        guard_caps={"max_order_usd": 8.0},
        measured_band={"rows": 47, "post_fee_roi_pct": 26.253203},
        roi_evidence={
            "subbands": {
                "00_below_25": {
                    "chronological_holdout": {"post_fee_roi_pct": -81.496943}
                }
            },
            "development": {"post_fee_roi_pct": 14.865843},
            "focus_robustness": {
                "holdout_drop_k_curve": [
                    {"drop_k": 1, "post_fee_roi_pct": 13.596867},
                    {"drop_k": 2, "post_fee_roi_pct": 3.875267},
                ],
                "holdout_leave_one_day_out": {
                    "curve": [
                        {"omitted_day": "2026-07-19", "post_fee_roi_pct": 24.861076},
                        {"omitted_day": "2026-07-22", "post_fee_roi_pct": 11.494343},
                    ]
                },
            },
        },
        in_band_fill_rate=in_band_fill_rate,
        realized_entry_events=(
            [{"realized_entry_band": "00_00_25", "out_of_band_fill": True}]
            if in_band_fill_rate is not None and in_band_fill_rate < 1.0
            else []
        ),
        closed_leg_roi_pct=-81.496943,
        active_set_registry=active_set_registry,
        realized_live_participation_windows=realized_live_participation_windows,
        resolved_live_fills_at_current_h=resolved_live_fills_at_current_h,
        since_topup_actual_usd=since_topup_actual_usd,
        restart_acceptance=restart_acceptance,
        source_side_supply={
            "prospective_current_market": {
                "identity_clean_events": source_rows,
                "actuator_consumption_gate": {
                    "exact_policy_chronological_holdout_by_wallet": {
                        green: {
                            "exact-green": {
                                "passed": True,
                                "wide_policy_fingerprint": "exact-green",
                            }
                        }
                    }
                },
            }
        },
        day_actual_pnl_usd=-7.0,
        now=datetime(2026, 8, 4, 10, 40, tzinfo=UTC),
    )


def test_cap_to_goal_publishes_every_roi_rung_with_source() -> None:
    cap_to_goal = _cap_to_goal_result()["cap_to_goal"]

    assert len(cap_to_goal["rungs"]) == 10
    assert {row["roi_basis"] for row in cap_to_goal["rungs"]} == {
        "holdout_headline",
        "development",
        "holdout_drop_1",
        "holdout_drop_2",
        "lodo_worst_day",
    }
    assert all(row["roi_source"] for row in cap_to_goal["rungs"])
    assert cap_to_goal["fill_cap_sensitivity_exercised"] is False


def test_cap_to_goal_governing_rung_is_most_conservative_positive_roi() -> None:
    governing = _cap_to_goal_result()["cap_to_goal"]["governing_rung"]

    assert governing["roi_basis"] == "holdout_drop_2"
    assert governing["roi_pct"] == 3.875267
    assert governing["supply_basis"] == "perfect_288"


def test_cap_to_goal_observed_supply_rung_carries_its_extrapolation_sample() -> None:
    rungs = _cap_to_goal_result()["cap_to_goal"]["rungs"]
    observed = next(
        row
        for row in rungs
        if row["roi_basis"] == "holdout_drop_2"
        and row["supply_basis"] == "observed_source_side"
    )

    assert observed["windows_per_day"] == "NO_MEASUREMENT_FOR_TRADED_COHORT"
    assert observed["extrapolated_from_observed_market_windows"] == 0


def test_cap_to_goal_marks_required_cap_above_guard_flag_inadmissible() -> None:
    cap_to_goal = _cap_to_goal_result()["cap_to_goal"]
    governing = next(
        row
        for row in cap_to_goal["rungs"]
        if row["roi_basis"] == "holdout_drop_2" and row["supply_basis"] == "perfect_288"
    )

    assert governing["required_cap_usd"] > 8.0
    assert governing["admissible_under_guard_flag"] is False
    assert cap_to_goal["governing_verdict"] == "UNREACHABLE_AT_ADMISSIBLE_CAP"


def test_goal_reachability_effective_cap_bounds_rather_than_replaces_member_caps() -> None:
    result = _cap_to_goal_result()

    assert result["selected_runtime_cap_usd"] == 1.0
    assert result["active_member_policy_caps"][0]["max_order_usd"] == 0.5
    assert result["effective_max_order_usd"] == 0.5


def test_cap_to_goal_publishes_h_as_explicit_roi_multiplicand() -> None:
    rung = _cap_to_goal_result(0.5)["cap_to_goal"]["rungs"][0]

    assert rung["in_band_fill_rate_h"] == 0.5
    assert rung["out_of_band_fill_rate"] == 0.5
    assert rung["in_band_roi_contribution_pct"] == 13.126601
    assert rung["closed_leg_roi_contribution_pct"] == -40.748472
    assert rung["blended_roi_pct"] == -27.62187


def test_cap_to_goal_refuses_required_cap_when_h_is_unmeasured() -> None:
    cap_to_goal = _cap_to_goal_result(None)["cap_to_goal"]

    assert cap_to_goal["governing_verdict"] == "UNREACHABLE_AT_ANY_CAP"
    assert all(row["required_cap_usd"] is None for row in cap_to_goal["rungs"])
    assert all(row["rung_verdict"] == "UNREACHABLE_AT_ANY_CAP" for row in cap_to_goal["rungs"])


def test_cap_to_goal_refuses_required_cap_when_h_is_below_break_even() -> None:
    cap_to_goal = _cap_to_goal_result(0.5)["cap_to_goal"]

    assert cap_to_goal["governing_verdict"] == "UNREACHABLE_AT_ANY_CAP"
    assert all(row["required_cap_usd"] is None for row in cap_to_goal["rungs"])


def test_goal_reachability_uses_worst_reachable_closed_leg_subband() -> None:
    result = update_state_digest._goal_reachability(
        guard={
            "active_set": {"members": [{"max_order_usd": 1.0}]},
            "guard_runtime_filter": {
                "per_window_fill_cap": 1,
                "price_band_decision_min_price": 0.25,
                "price_band_decision_max_price": 0.50,
            },
        },
        guard_caps={"max_order_usd": 8.0},
        measured_band={"rows": 47, "post_fee_roi_pct": 26.253203},
        roi_evidence={
            "subbands": {
                "00_below_25": {"aggregate": {"post_fee_roi_pct": -81.496943}},
                "01a_25_32": {"chronological_holdout": {"post_fee_roi_pct": 26.253203}},
                "01b_32_40": {"chronological_holdout": {"post_fee_roi_pct": -3.984629}},
                "01c_40_50": {"chronological_holdout": {"post_fee_roi_pct": -2.804179}},
            },
            "focus_robustness": {
                "holdout_drop_k_curve": [{"drop_k": 2, "post_fee_roi_pct": 3.875267}]
            },
        },
        in_band_fill_rate=0.625,
        realized_entry_events=[
            {"realized_entry_band": "01b_32_40", "out_of_band_fill": True}
        ],
        closed_leg_roi_pct=-81.496943,
        day_actual_pnl_usd=-7.0,
        now=datetime(2026, 8, 4, 10, 40, tzinfo=UTC),
    )

    gate = result["live_price_gate"]
    assert gate["binds"] == "decision_price_and_gate_probe_best_ask"
    assert gate["gate_probe_best_ask_floor_predicate"] == "below_ruled_entry_floor"
    assert gate["realized_entry_bound"] is False
    assert gate["realized_entry_binding_status"] == "PENDING_POST_ACTIVATION_FILL_EVIDENCE"
    assert gate["closed_leg_reachable_under_enforced_gate"] is True
    assert gate["closed_leg_roi_pct"] == -3.984629
    governing = next(
        row
        for row in result["cap_to_goal"]["rungs"]
        if row["roi_basis"] == "holdout_drop_2" and row["supply_basis"] == "perfect_288"
    )
    assert governing["closed_leg_sample_gate_status"] is None
    assert governing["closed_leg_roi_pct"] == -3.984629
    assert governing["blended_roi_pct"] == 0.927806
    assert governing["required_cap_usd"] == 37.424011
    assert result["cap_to_goal"]["governing_selection_basis"] == "raw_roi_pct_pre_leak"
    assert governing["governing_selection_basis"] == "raw_roi_pct_pre_leak"


def test_goal_reachability_01a_only_gate_has_no_closed_leg() -> None:
    result = update_state_digest._goal_reachability(
        guard={
            "active_set": {"members": [{"max_order_usd": 1.0}]},
            "guard_runtime_filter": {
                "per_window_fill_cap": 1,
                "price_band_decision_min_price": 0.25,
                "price_band_decision_max_price": 0.32,
            },
        },
        guard_caps={"max_order_usd": 8.0},
        measured_band={"rows": 47, "post_fee_roi_pct": 26.253203},
        roi_evidence={
            "subbands": {
                "01a_25_32": {"chronological_holdout": {"post_fee_roi_pct": 26.253203}},
                "01b_32_40": {"chronological_holdout": {"post_fee_roi_pct": -3.984629}},
            },
            "focus_robustness": {
                "holdout_drop_k_curve": [{"drop_k": 2, "post_fee_roi_pct": 3.875267}]
            },
        },
        in_band_fill_rate=0.625,
        day_actual_pnl_usd=-7.0,
        now=datetime(2026, 8, 4, 10, 40, tzinfo=UTC),
    )

    gate = result["live_price_gate"]
    assert gate["closed_leg_reachable_under_enforced_gate"] is None
    assert gate["closed_leg_roi_pct"] is None
    governing = next(
        row
        for row in result["cap_to_goal"]["rungs"]
        if row["roi_basis"] == "holdout_drop_2" and row["supply_basis"] == "perfect_288"
    )
    assert governing["observed_in_band_fill_rate_h"] == 0.625
    assert governing["in_band_fill_rate_h"] == 0.625
    assert governing["blended_roi_pct"] is None
    assert governing["rung_verdict"] == "UNREACHABLE_AT_ANY_CAP"


def test_goal_reachability_measures_supply_on_traded_cohort() -> None:
    live = "0xlive"
    rows = [
        {
            "event_id": f"event-{index}",
            "source_wallet": live,
            "market_slug": f"btc-updown-5m-{1_800_000_000 + index * 300}",
            "action": "BUY",
            "price": 0.30 if index < 2 else 0.40,
            "paper_only": True,
        }
        for index in range(4)
    ]
    result = update_state_digest._goal_reachability(
        guard={
            "active_set": {"members": [{"source_wallet": live, "max_order_usd": 1.0}]},
            "guard_runtime_filter": {"per_window_fill_cap": 1},
        },
        guard_caps={"max_order_usd": 8.0},
        measured_band={"rows": 47, "post_fee_roi_pct": 26.253203},
        source_side_supply={
            "prospective_current_market": {
                "identity_clean_events": rows,
                "actuator_consumption_gate": {
                    "exact_policy_chronological_holdout_by_wallet": {}
                },
            }
        },
        day_actual_pnl_usd=-7.0,
        now=datetime(2026, 8, 4, 10, 40, tzinfo=UTC),
    )

    supply = result["supply"]
    assert supply["denominator_status"] == "TRADED_COHORT_MEASURED"
    assert supply["qualifying_windows_per_day_for_ladder"] == 144.0
    assert supply["source_side"]["traded_cohort_wallets"] == [live]
    assert supply["source_side"]["traded_cohort_observed_market_windows"] == 4


def test_goal_reachability_falls_back_when_supply_cohort_has_zero_live_overlap() -> None:
    result = _cap_to_goal_result(
        active_set_registry={
            "members": [
                {
                    "source_wallet": "0xgreen",
                    "enabled": False,
                    "status": "SUPERSEDED_BY_NORMAL_GATE_UNIQUE_ALLPASS",
                },
                {
                    "source_wallet": "0xlive",
                    "enabled": True,
                    "status": "LIVE_MEMBER",
                },
            ]
        }
    )

    supply = result["supply"]
    assert supply["supply_cohort_live_overlap"] == 0
    assert supply["live_members_with_no_supply_measurement"] == ["0xlive"]
    assert supply["denominator_status"] == "NO_MEASUREMENT_FOR_TRADED_COHORT"
    assert supply["qualifying_windows_per_day"] == "NO_MEASUREMENT_FOR_TRADED_COHORT"
    assert supply["measured_non_traded_cohort_qualifying_windows_per_day"] == 205.714286
    assert supply["qualifying_windows_per_day_for_ladder"] == "NO_MEASUREMENT_FOR_TRADED_COHORT"
    assert supply["authority"] == "NO_MEASUREMENT_FOR_TRADED_COHORT"
    assert supply["source_side"]["holdout_green_wallets"] == [
        {
            "source_wallet": "0xgreen",
            "enabled": False,
            "status": "SUPERSEDED_BY_NORMAL_GATE_UNIQUE_ALLPASS",
        }
    ]
    observed = next(
        row
        for row in result["cap_to_goal"]["rungs"]
        if row["roi_basis"] == "holdout_drop_2"
        and row["supply_basis"] == "observed_source_side"
    )
    assert observed["windows_per_day"] == "NO_MEASUREMENT_FOR_TRADED_COHORT"
    assert observed["required_cap_usd"] is None


def test_cap_provenance_and_four_of_four_step_preregistration_are_published() -> None:
    result = _cap_to_goal_result(
        active_set_registry={
            "members": [
                {
                    "source_wallet": "0xgreen",
                    "enabled": False,
                    "status": "SUPERSEDED",
                    "max_order_usd": 1.0,
                    "admission_wave_id": "2026-07-14T17:04Z-fable-mass-admission-wave",
                },
                {
                    "source_wallet": "0xlive",
                    "enabled": True,
                    "status": "LIVE_MEMBER",
                    "max_order_usd": 1.0,
                    "cap_reason": "measured-positive-holdout",
                },
            ]
        },
        resolved_live_fills_at_current_h=30,
        since_topup_actual_usd=-30.392442,
        restart_acceptance={
            "blocker_taxonomy_published": True,
            "post_restart_in_band_fill_rate_is_one": True,
            "first_hour_submitted_nonincrease": True,
        },
    )

    provenance = result["cap_provenance"]
    assert provenance["cap_basis_counts"] == {
        "evidence_linked": 1,
        "hand_set_admission_wave": 1,
    }
    assert [row["cap_basis"] for row in provenance["members"]] == [
        "hand_set_admission_wave",
        "evidence_linked",
    ]
    prereg = result["cap_step_preregistration"]
    assert prereg["criteria_required"] == 4
    assert prereg["criteria_passed"] == 3
    assert prereg["criteria"]["lodo_worst_is_governing_positive_rung"] is False
    assert prereg["cap_raise_authorized"] is False
    assert prereg["live_mutation"] is False


def test_state_digest_surfaces_lifetime_money_price_subbands(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text("")
    _write_json(
        data / "wallet_copy_daily_scorecard_2026-07-31.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "day_utc": "2026-07-31",
            "generated_at": "2026-07-31T23:59:00Z",
            "canonical_pnl_truth": {
                "by_day": {"2026-07-31": {"pnl_usd": 1.0, "resolved_fills": 2}}
            },
            "lifetime_pnl_truth": {
                "by_price_band": {
                    "01_25_50": {
                        "resolved_fills": 1809,
                        "cost_usd": 3234.549827,
                        "pnl_usd": 96.927622,
                        "roi_pct": 2.996634,
                    }
                },
                "by_price_subband": {
                    "01a_25_32": {
                        "resolved_fills": 159,
                        "cost_usd": 213.323956,
                        "pnl_usd": 25.555369,
                        "roi_pct": 11.979606,
                    },
                    "01b_32_40": {
                        "resolved_fills": 247,
                        "cost_usd": 387.747087,
                        "pnl_usd": -5.120478,
                        "roi_pct": -1.320572,
                    },
                    "01c_40_50": {
                        "resolved_fills": 1403,
                        "cost_usd": 2633.478784,
                        "pnl_usd": 76.492731,
                        "roi_pct": 2.904627,
                    },
                },
            },
        },
    )

    digest, text = build_digest(tmp_path)

    assert digest["pnl"]["lifetime_price_band_01_25_50"]["resolved_fills"] == 1809
    assert digest["pnl"]["lifetime_price_subbands_01_25_50"]["01b_32_40"] == {
        "resolved_fills": 247,
        "cost_usd": 387.747087,
        "payout_usd": None,
        "pnl_usd_realized": -5.120478,
        "roi_pct_realized": -1.320572,
    }
    assert "lifetime_money_subbands={'01a_25_32':" in text


def test_state_digest_surfaces_passive_at_source_holdout(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text("")
    _write_json(
        data / "passive_at_source_holdout_latest.json",
        {
            "generated_at": "2026-08-02T16:20:00Z",
            "verdict": "NEGATIVE_CHRONOLOGICAL_HOLDOUT",
            "measurement_only": True,
            "live_orders_allowed": False,
            "sample_gate": {"status": "PASS", "development_rows": 48, "holdout_rows": 48},
            "aggregate": {"rows": 96, "post_fee_pnl_usd": -29.03},
            "development": {"rows": 48, "post_fee_pnl_usd": 2.05},
            "chronological_holdout": {"rows": 48, "post_fee_pnl_usd": -31.08},
            "next_action": "ask Fable to close passive-at-source live lane; holdout is negative",
        },
    )

    digest, text = build_digest(tmp_path)

    holdout = digest["pnl"]["passive_at_source_holdout"]
    assert holdout["verdict"] == "NEGATIVE_CHRONOLOGICAL_HOLDOUT"
    assert holdout["chronological_holdout"]["post_fee_pnl_usd"] == -31.08
    assert holdout["live_orders_allowed"] is False
    assert "passive_holdout={'generated_at': '2026-08-02T16:20:00Z'" in text


def test_state_digest_surfaces_taker_price_subband_holdout(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text("")
    _write_json(
        data / "taker_price_subband_holdout_latest.json",
        {
            "generated_at": "2026-08-02T16:51:37Z",
            "measurement_only": True,
            "live_orders_allowed": False,
            "focus_subband": "01a_25_32",
            "focus_verdict": "POSITIVE_DAY_BOUNDED_HOLDOUT",
            "size_ruling_request_ready": True,
            "subbands": {
                "01a_25_32": {
                    "verdict": "POSITIVE_DAY_BOUNDED_HOLDOUT",
                    "sample_gate": {
                        "status": "PASS",
                        "development_rows": 45,
                        "holdout_rows": 45,
                    },
                    "chronological_holdout": {
                        "rows": 45,
                        "post_fee_pnl_usd": 7.952746,
                    },
                }
            },
            "next_action": "ask Fable for band-scoped size ruling",
        },
    )

    digest, text = build_digest(tmp_path)

    holdout = digest["pnl"]["taker_price_subband_holdout"]
    assert holdout["focus_verdict"] == "POSITIVE_DAY_BOUNDED_HOLDOUT"
    assert holdout["size_ruling_request_ready"] is True
    assert holdout["subbands"]["01a_25_32"]["chronological_holdout"][
        "post_fee_pnl_usd"
    ] == 7.952746
    assert "taker_subband_holdout={'generated_at': '2026-08-02T16:51:37Z'" in text


def test_state_digest_surfaces_band_pnl_reconciliation(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text("")
    _write_json(
        data / "band_pnl_surface_reconciliation_latest.json",
        {
            "generated_at": "2026-08-02T21:56:00Z",
            "decision": "REALIZED_DEV_HOLDOUT_SIGN_SPLIT_NO_ESTABLISHED_EDGE",
            "payout_usd_semantics": {"classification": "REALIZED_PAYOUT_MINUS_RECEIPT_MAPPED_COST"},
            "band_definition_mismatch": {"exact_0_50_artefact_rows": 172},
            "bands": {"01c_40_50": {"observed_pnl_gap_usd": 94.921184}},
            "whole_book_day_bounded": {
                "chronological_holdout": {
                    "roi_pct_realized": 1.168373,
                }
            },
            "live_mutation": False,
        },
    )

    digest, _ = build_digest(tmp_path)

    report = digest["pnl"]["band_pnl_surface_reconciliation"]
    assert report["decision"] == "REALIZED_DEV_HOLDOUT_SIGN_SPLIT_NO_ESTABLISHED_EDGE"
    assert report["bands"]["01c_40_50"]["observed_pnl_gap_usd"] == 94.921184
    assert report["band_definition_mismatch"]["exact_0_50_artefact_rows"] == 172


def test_state_digest_surfaces_fee_bank_identity(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text("")
    _write_json(
        data / "fee_realization_bank_reconciliation_latest.json",
        {
            "generated_at": "2026-08-02T22:10:00Z",
            "status": "PASS_BANK_IDENTITY_FEE_MODEL_DEMOTED_STALE_BASELINE_RESTATED",
            "bank_identity": {"residual_usd": 0.52995, "status": "PASS_WITHIN_1.00"},
            "population_fork": {"resolved_fill_delta": 781},
            "fee_authority": {"active_fee_rate": 0.0},
            "live_mutation": False,
        },
    )

    digest, _ = build_digest(tmp_path)

    report = digest["pnl"]["fee_realization_bank_reconciliation"]
    assert report["bank_identity"]["residual_usd"] == 0.52995
    assert report["population_fork"]["resolved_fill_delta"] == 781
    assert report["fee_authority"]["active_fee_rate"] == 0.0


def test_state_digest_surfaces_wide_wallet_terminal_breakdown(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text("")
    wallet = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    _write_json(
        data / "82c8_wide_terminal_breakdown_latest.json",
        {
            "wallet": wallet,
            "measurement_cut_at": "2026-07-31T02:42:42Z",
            "attempts": 172,
            "terminal_taxonomy": {
                "REFUSED_ALPHA_PROFILE_FILTER": 170,
                "REFUSED_STALE_RECEIPT_TO_FETCH": 2,
            },
            "metadata_missing_predominant": False,
        },
    )
    _write_json(
        data / "82c8_resolved_tape_gap_closure_latest.json",
        {
            "decision": "GAP_CLOSED_SAMPLE_DIVERSITY_PENDING_REFUSE",
            "resolved_latest_event_age_h": 0.164289,
            "resolved_recent_trades": 50,
            "resolved_recent_unique_windows": 2,
            "resolved_recent_pnl_usd": 2.857188,
        },
    )

    digest, text = build_digest(tmp_path)

    assert digest["wide_wallet_terminal_breakdown"]["attempts"] == 172
    assert "wallet_terminal_breakdown=0x82c8...da77" in text
    assert "metadata_predominant:False" in text
    assert (
        digest["resolved_tape_gap_closure"]["decision"]
        == "GAP_CLOSED_SAMPLE_DIVERSITY_PENDING_REFUSE"
    )
    assert "resolved_tape_gap=GAP_CLOSED_SAMPLE_DIVERSITY_PENDING_REFUSE" in text


def test_state_digest_surfaces_natural_order128_manifest_authorization(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text("")
    manifest = data / "wide_exact_policy_manifest_wide_cut.json"
    _write_json(
        data / "wide_exact_policy_manifest_active.json",
        {"manifest_path": str(manifest.relative_to(tmp_path))},
    )
    _write_json(
        data / "order128_fastest_lawful_path_latest.json",
        {
            "generated_at": "2026-08-01T01:47:05.684109+00:00",
            "score_run_id": "wide_20260801T014705Z",
        },
    )
    _write_json(
        manifest,
        {
            "generated_at": "2026-08-01T01:47:08Z",
            "manifest_id": "widemanifest_cut",
            "score_run_id": "wide_20260801T014705Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {
                "admitted_wallets": 0,
                "promotion_admitted_wallets": 0,
                "order128_sticky_focus_authorized": True,
            },
        },
    )

    digest, text = build_digest(tmp_path)

    binding = digest["order128_manifest_binding"]
    assert binding["order128_sticky_focus_authorized"] is True
    assert binding["paper_only"] is True
    assert binding["admitted_wallets"] == 0
    assert binding["manifest_generated_at"] == "2026-08-01T01:47:08Z"
    assert binding["order128_packet_generated_at"].startswith(
        "2026-08-01T01:47:05"
    )
    assert binding["cuts_agree"] is True
    assert (
        "manifest_auth=True@wide_20260801T014705Z:cuts_agree=True:"
        "paper=True:admitted=0"
    ) in text


def test_state_digest_order128_manifest_binding_names_missing_pointer(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text("")

    digest, text = build_digest(tmp_path)

    binding = digest["order128_manifest_binding"]
    assert binding["status"] == "NO_POINTER"
    assert binding["order128_sticky_focus_authorized"] == "NO_POINTER"
    assert "manifest_auth=NO_POINTER@NO_POINTER:cuts_agree=UNASSERTABLE" in text


def test_state_digest_order128_manifest_binding_names_missing_manifest(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text("")
    _write_json(
        data / "wide_exact_policy_manifest_active.json",
        {"manifest_path": "data/research/does_not_exist.json"},
    )

    digest, text = build_digest(tmp_path)

    binding = digest["order128_manifest_binding"]
    assert binding["status"] == "NO_MANIFEST"
    assert binding["order128_sticky_focus_authorized"] == "NO_MANIFEST"
    assert "manifest_auth=NO_MANIFEST@NO_MANIFEST:cuts_agree=UNASSERTABLE" in text


def test_order128_manifest_binding_does_not_reconstruct_missing_packet_cut() -> None:
    binding = update_state_digest._order128_manifest_binding(
        {"manifest_path": "manifest.json"},
        {
            "score_run_id": "wide_20260801T021818Z",
            "summary": {"order128_sticky_focus_authorized": True},
        },
        {"generated_at": "2026-08-01T02:18:19.161518Z"},
    )

    assert binding["order128_packet_score_run_id"] == "UNASSERTABLE"
    assert binding["cuts_agree"] == "UNASSERTABLE"


def test_order128_manifest_binding_does_not_compare_missing_manifest_cut() -> None:
    binding = update_state_digest._order128_manifest_binding(
        {"manifest_path": "manifest.json"},
        {
            "generated_at": "2026-08-01T02:18:22Z",
            "summary": {"order128_sticky_focus_authorized": True},
        },
        {
            "generated_at": "2026-08-01T02:18:19.161518Z",
            "score_run_id": "wide_20260801T021818Z",
        },
    )

    assert binding["manifest_score_run_id"] == "NO_MANIFEST"
    assert binding["order128_packet_score_run_id"] == "wide_20260801T021818Z"
    assert binding["cuts_agree"] == "UNASSERTABLE"


def test_digest_carries_bounded_park_provenance_rows() -> None:
    rows = update_state_digest._park_provenance_rows(
        {
            "rows": [
                {
                    "wallet": "0x82c8",
                    "wide_policy_fingerprint": "bac25",
                    "park_provenance": {
                        "park_basis": "MEASURED_NEGATIVE",
                        "source_record": "data/research/82c8_park_reconciliation_latest.json",
                        "reason_string": "PARK_SEAT_EVIDENCE_BAR_NOT_CROSSED_AT_DEADLINE",
                    },
                },
                {"wallet": "0xclear", "park_provenance": None},
            ]
        }
    )

    assert rows == [
        {
            "wallet": "0x82c8",
            "wide_policy_fingerprint": "bac25",
            "park_basis": "MEASURED_NEGATIVE",
            "source_record": "data/research/82c8_park_reconciliation_latest.json",
            "reason_string": "PARK_SEAT_EVIDENCE_BAR_NOT_CROSSED_AT_DEADLINE",
        }
    ]


def test_walk_forward_frontier_uses_direct_candidate_count_and_seeded_high_water():
    frontier = update_state_digest._walk_forward_refusal_frontier(
        {
            "candidate_evidence": {"candidate_count": 11},
            "refusal_counts": {
                "f1_walk_forward_admissible": 11,
                "another_check": 7,
            },
        },
        {},
        generated_at="2026-07-30T10:00:00Z",
    )

    assert frontier["candidate_pool"] == 11
    assert frontier["candidate_pool_high_water"] == 20
    assert frontier["candidate_pool_delta_from_high_water"] == -9
    assert frontier["display"] == "11/11 (pool 20->11, delta -9)"


def test_walk_forward_frontier_persists_larger_observed_high_water():
    frontier = update_state_digest._walk_forward_refusal_frontier(
        {
            "candidate_evidence": {"candidate_count": 21},
            "refusal_counts": {"f1_walk_forward_admissible": 18},
        },
        {
            "walk_forward_refusal_frontier": {
                "candidate_pool_high_water": 24,
                "candidate_pool_high_water_first_observed_at": "2026-07-30T10:00:00Z",
            }
        },
        generated_at="2026-07-30T10:05:00Z",
    )

    assert frontier["candidate_pool"] == 21
    assert frontier["candidate_pool_high_water"] == 24
    assert frontier["candidate_pool_high_water_first_observed_at"] == "2026-07-30T10:00:00Z"
    assert frontier["display"] == "18/21 (pool 24->21, delta -3)"


def test_walk_forward_frontier_stamps_new_high_water_at_current_cut():
    frontier = update_state_digest._walk_forward_refusal_frontier(
        {
            "candidate_evidence": {"candidate_count": 30},
            "refusal_counts": {"f1_walk_forward_admissible": 30},
        },
        {"walk_forward_refusal_frontier": {"candidate_pool_high_water": 20}},
        generated_at="2026-07-30T10:10:00Z",
    )

    assert frontier["candidate_pool_high_water"] == 30
    assert frontier["candidate_pool_high_water_first_observed_at"] == "2026-07-30T10:10:00Z"


def test_latest_scorecard_prefers_requested_day_over_newer_historical_write(
    tmp_path: Path,
) -> None:
    _write_json(
        tmp_path / "wallet_copy_daily_scorecard_2026-07-30.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "day_utc": "2026-07-30",
            "day_pnl_response_basis": -1.0,
        },
    )
    _write_json(
        tmp_path / "wallet_copy_daily_scorecard_2026-07-29.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "day_utc": "2026-07-29",
            "day_pnl_response_basis": 99.0,
        },
    )

    selected = update_state_digest._latest_scorecard(
        tmp_path,
        day_utc="2026-07-30",
    )

    assert selected["day_utc"] == "2026-07-30"
    assert selected["day_pnl_response_basis"] == -1.0


def test_latest_live_order_snapshot_preserves_latest_venue_accepted_fill() -> None:
    rejected = {
        "order_id": "lo_rejected",
        "intent_id": "ci_rejected",
        "final_status": "REJECTED",
        "submitted_at": "2026-07-25T22:35:51Z",
        "trade_result": {
            "order_id": "",
            "maker_min_share_funding": {
                "requested_notional_usd": 1.0,
                "funded_notional_usd": 2.4,
                "funded_shares": 5.0,
            },
        },
    }
    filled = {
        "order_id": "0xaccepted",
        "intent_id": "ci_accepted",
        "final_status": "FILLED",
        "submitted_at": "2026-07-25T22:30:51Z",
        "filled_size_usd": 1.297821,
        "trade_result": {
            "order_id": "0xaccepted",
            "maker_min_share_funding": {
                "requested_notional_usd": 1.0,
                "funded_notional_usd": 1.3,
                "funded_shares": 5.0,
            },
            "wallet_copy_maker_cancel": {"matched_shares": 4.991619},
        },
    }

    state = {"orders": [filled, rejected]}
    assert update_state_digest._latest_live_order_snapshot(state)["order_id"] == "lo_rejected"
    accepted = update_state_digest._latest_live_order_snapshot(state, accepted_only=True)
    assert accepted["order_id"] == "0xaccepted"
    assert accepted["status"] == "FILLED"
    assert accepted["funded_notional_usd"] == 1.3
    assert accepted["response_fill_size_shares"] == 4.991619


def test_c539_deferred_open_probe_summary_is_paper_only_and_gate_scoped() -> None:
    summary = update_state_digest._c539_deferred_open_probe_summary(
        {
            "generated_at": "2026-07-29T19:00:00Z",
            "registered_at": "2026-07-29T18:30:00Z",
            "observation_deadline_at": "2026-07-30T18:30:00Z",
            "status": "ACCRUING_PAPER_ONLY",
            "paper_only": True,
            "live_orders_allowed": False,
            "source_wallet": "0xc539",
            "policy_drift": False,
            "frozen_policy": {
                "policy_id": "frozen",
                "policy_fingerprint": "abc123",
            },
            "summary": {
                "c539_buy_rows": 12,
                "not_open_yet_rows": 11,
                "not_open_yet_share": 0.916667,
                "deferred_window_outcomes": 2,
                "survived_open_re_evaluation": 1,
                "resolved": 1,
                "post_fee_pnl_usd": 0.2,
                "first_half_post_fee_pnl_usd": 0.2,
                "second_half_post_fee_pnl_usd": None,
                "open_grace_covered_windows": 1,
                "open_grace_total_windows": 2,
                "open_grace_coverage": 0.5,
                "open_grace_coverage_below_60pct": True,
                "open_grace_coverage_rows": [
                    {
                        "window_start_s": 1785347700,
                        "resident_up": True,
                        "first_evaluation_lag_s": 1.0,
                        "covered": True,
                    }
                ],
                "open_grace_coverage_by_hour": [
                    {
                        "hour": "2026-07-29T19:00:00Z",
                        "covered": 1,
                        "total": 2,
                        "coverage": 0.5,
                    }
                ],
            },
            "predecessor_terminal_records": [
                {
                    "status": "PROBE_UNREACHABLE_BY_CONSTRUCTION",
                    "refusal_decomposition": {"verbatim": "20/2/18"},
                }
            ],
            "preregistration": {
                "rationale": "wf=0.50 is 5x mirror ratio; $1 cap makes evidence comparable"
            },
            "source_rows": [
                {
                    "event_id": "rtds",
                    "condition_id": "condition",
                    "token_id": "token",
                    "window_start_s": 1785347700,
                    "source_price": 0.48,
                    "source_size": 5.0,
                    "source_usdc": 2.4,
                    "observed_ts": 1785347699.9,
                    "not_open_yet": True,
                },
                {
                    "event_id": "polygon",
                    "condition_id": "condition",
                    "token_id": "token",
                    "window_start_s": 1785347700,
                    "source_price": 0.48,
                    "source_size": 5.0,
                    "source_usdc": 2.4,
                    "observed_ts": 1785347708.8,
                    "not_open_yet": False,
                },
            ],
            "admission": {
                "eligible": False,
                "live_authority": False,
                "required_bars": {"resolved_gte_50": False},
            },
        }
    )

    assert summary["paper_only"] is True
    assert summary["live_orders_allowed"] is False
    assert summary["raw_rows"] == 2
    assert summary["distinct_signals"] == 1
    assert summary["cross_feed_duplicate_rows"] == 1
    assert summary["open_grace_coverage"] == 0.5
    assert summary["open_grace_coverage_below_60pct"] is True
    assert summary["open_grace_coverage_rows"][0]["resident_up"] is True
    assert summary["latest_predecessor_terminal"]["refusal_decomposition"]["verbatim"] == "20/2/18"
    assert summary["not_open_yet_share"] == 1.0
    assert summary["policy_fingerprint"] == "abc123"
    assert summary["live_authority"] is False
    assert summary["required_bars"] == {"resolved_gte_50": False}


def test_wide_heartbeat_watch_summary_surfaces_wake_age_and_dual_half_inventory() -> None:
    wake_wallet = "0x3048d65321be3497164cdfc2996f94f98a2e7537"
    requeue_wallet = "0x9d57c42e847173d06841703825d3fe2299e456ea"
    now = datetime(2026, 7, 28, 5, 0, tzinfo=UTC)
    summary = update_state_digest._wide_heartbeat_watch_summary(
        {
            "wallets": {
                wake_wallet: {
                    "source_wallet": wake_wallet,
                    "latest_matching_event_ts": datetime(
                        2026, 7, 27, 1, 0, tzinfo=UTC
                    ).timestamp(),
                },
                requeue_wallet: {
                    "source_wallet": requeue_wallet,
                    "latest_matching_event_ts": datetime(
                        2026, 7, 28, 4, 30, tzinfo=UTC
                    ).timestamp(),
                },
            }
        },
        {
            "cells": [
                {
                    "identity": {
                        "wallet": wake_wallet,
                        "wide_policy_fingerprint": "fp-a",
                    },
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "f1_pass": True,
                        "first_half_post_fee_pnl_usd": 2.0,
                        "second_half_post_fee_pnl_usd": 1.0,
                    },
                },
                {
                    "identity": {
                        "wallet": wake_wallet,
                        "wide_policy_fingerprint": "fp-b",
                    },
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "f1_pass": True,
                        "first_half_post_fee_pnl_usd": 1.0,
                        "second_half_post_fee_pnl_usd": 3.0,
                    },
                },
                {
                    "identity": {
                        "wallet": "0x" + "1" * 40,
                        "wide_policy_fingerprint": "fp-c",
                    },
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "f1_pass": True,
                        "first_half_post_fee_pnl_usd": 4.0,
                        "second_half_post_fee_pnl_usd": 5.0,
                    },
                },
                {
                    "identity": {
                        "wallet": "0x" + "2" * 40,
                        "wide_policy_fingerprint": "fp-red",
                    },
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "f1_pass": True,
                        "first_half_post_fee_pnl_usd": 4.0,
                        "second_half_post_fee_pnl_usd": -1.0,
                    },
                },
            ]
        },
        now=now,
        freeze_resolution_accelerator={
            "direction_id": "2026-07-28T08:11:26Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "direct_climb_priority": [
                {
                    "wallet": "0x" + "5" * 40,
                    "wide_policy_fingerprint": "74534a",
                }
            ],
            "direct_climb_priority_unresolved_window_count": 3,
            "fresh_forward_clock": {
                "started_at": "2026-07-28T08:11:26Z",
                "status": "ACCRUING_PAPER_ONLY",
                "target_resolved": 100,
                "baseline_full_window": {"resolved": 1258},
                "kill_rules": {"full_window": "kill on H2 <= 0"},
                "source_drought_policy": {
                    "cycle_s": 1800,
                    "freeze_after_consecutive_cycles": 3,
                    "consequence": "FREEZE_PRESERVE_DIGITS_NO_STICKY_REFUSE",
                },
            },
        },
        order_flow_deadman={
            "guard_memory": {
            "pid": 27603,
            "rss_gib": 4.08,
            "threshold_rss_gib": None,
            "threshold_rss_source": None,
            "in_process_stage_boundary_rss": {},
            "rss_observation_threshold_grade": None,
            "warn_gib": 5.0,
                "restart_gib": 6.0,
                "trend_gib": 1.46,
                "trend_elapsed_s": 3600.0,
            }
        },
    )

    assert summary["wake_watch"] == {
        "wallet": wake_wallet,
        "latest_own_buy_ts": datetime(
            2026, 7, 27, 1, 0, tzinfo=UTC
        ).timestamp(),
        "latest_own_buy_at": "2026-07-27T01:00:00Z",
        "age_h": 28.0,
        "trigger_age_lte_h": 24.0,
        "triggered": False,
        "source": "wallet_copy_rtds_observation_watermarks.wallets",
    }
    assert summary["liveness_wake_watches"][1] == {
        "wallet": requeue_wallet,
        "latest_own_buy_ts": datetime(
            2026, 7, 28, 4, 30, tzinfo=UTC
        ).timestamp(),
        "latest_own_buy_at": "2026-07-28T04:30:00Z",
        "age_h": 0.5,
        "trigger_age_lte_h": 24.0,
        "triggered": True,
        "source": "wallet_copy_rtds_observation_watermarks.wallets",
        "on_trigger": "mechanically_requeue_and_rescore_candidate_only",
        "live_enablement_from_wake": False,
    }
    assert summary["triggered_wake_wallets"] == [requeue_wallet]
    assert summary["dual_half_positive_inventory"] == {
        "f1_pass_cell_count": 3,
        "distinct_wallet_count": 2,
        "wallets": ["0x" + "1" * 40, wake_wallet],
        "source": "wide_policy_fingerprint_evidence.cells.venue_executable_full_stream_rescore",
    }
    assert summary["fresh_forward_climb"] == {
        "direction_id": "2026-07-28T08:11:26Z",
        "wallet": "0x" + "5" * 40,
        "wide_policy_fingerprint": "74534a",
        "started_at": "2026-07-28T08:11:26Z",
        "status": "ACCRUING_PAPER_ONLY",
        "target_resolved": 100,
        "baseline_full_window": {"resolved": 1258},
        "kill_rules": {"full_window": "kill on H2 <= 0"},
        "source_drought_policy": {
            "cycle_s": 1800,
            "freeze_after_consecutive_cycles": 3,
            "consequence": "FREEZE_PRESERVE_DIGITS_NO_STICKY_REFUSE",
        },
        "unresolved_window_count": 3,
        "paper_only": True,
        "live_orders_allowed": False,
        "source": "freeze_resolution_accelerator_state",
    }
    assert summary["guard_rss_watch"] == {
        "pid": 27603,
        "rss_gib": 4.08,
        "threshold_rss_gib": None,
        "threshold_rss_source": None,
        "in_process_stage_boundary_rss": {},
        "rss_observation_threshold_grade": None,
        "warn_gib": 5.0,
        "restart_gib": 6.0,
        "rss_observation": {},
        "source": "order_flow_deadman_state.guard_memory",
    }


def test_latest_same_window_capture_summary_includes_exact_policy_metrics(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    run_dir = data / "same_window_capture" / "newest"
    wallet = "0x" + "7" * 40
    _write_json(
        run_dir / "same_window_capture_state.json",
        {
            "status": "COMPLETED",
            "gate_status": "PASS",
            "run_id": "newest",
            "completed_at": "2026-07-23T14:11:49Z",
            "selected_wallets": [wallet],
            "paper_only": True,
            "live_orders_allowed": False,
            "gates": {"alpha_overlap_s_gt_0": True},
            "paths": {
                "alpha_report": "data/research/same_window_capture/newest/alpha.json",
                "top10": "data/research/same_window_capture/newest/top10.json",
            },
        },
    )
    _write_json(
        run_dir / "alpha.json",
        {
            "alpha_decay": {
                "status": "PASS",
                "fills_total": 3,
                "fills_with_any_book_coverage": 2,
                "capture_windows": {"overlap_s": 120.0},
                "per_wallet": {
                    wallet: {
                        "fills_with_any_coverage": 2,
                        "horizons": {
                            "1s": {
                                "edge": {"mean": 0.01},
                                "positive_edge_fraction": 0.5,
                                "timely_coverage": 2,
                            }
                        },
                    }
                },
            }
        },
    )
    _write_json(
        run_dir / "top10.json",
        {
            "status": "WATCH",
            "blockers": ["top10_paper_pnl_non_positive"],
            "source": {"rows_scanned": 99},
            "summary": {"buy_events": 10, "copyable_buy_events": 1, "paper_pnl_usd": -1.0},
        },
    )
    _write_json(data / "ranked_queue_clearance_packets_latest.json", {"packets": [{"wallet": wallet}]})

    summary = update_state_digest._latest_same_window_capture_summary(data)

    assert summary["gate_status"] == "PASS"
    assert summary["top10"]["rows_scanned"] == 99
    assert summary["exact_policy"] == [
        {
            "wallet": wallet,
            "selected": True,
            "status": "MEASURED",
            "fills_with_any_coverage": 2,
            "edge_mean_1s": 0.01,
            "positive_edge_fraction_1s": 0.5,
            "timely_coverage_1s": 2,
        }
    ]


def test_wide_digest_includes_generation_terminal_and_direct_latency_proof(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "research"
    _write_json(
        data / "wide_copyable_rate_reachability_latest.json",
        {
            "generated_at": "2026-07-30T16:45:00Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "threshold_pct_unchanged": 70.0,
            "summary": {
                "decision_branch": "RETIRE_WIDE_EXACT_POLICY_AS_NEAR_TERM_MONEY_ROUTE",
                "input_rows": 8140,
                "terminal_rows": 8140,
                "input_equals_terminal": True,
                "policy_addressable_rate_gte_70_count": 0,
            },
            "wallets": [
                {
                    "wallet": "0x424eb20fcd25113e3b98f42522a54580350b263b",
                    "as_built": {"copyable_rate_pct": 40.0},
                    "policy_addressable": {"copyable_rate_pct": 40.0},
                    "denominator_comparison": {"equal": True},
                    "excluded_attempt_taxonomy": {
                        "out_of_selected_slice": 1,
                        "metadata_missing": 2,
                        "slippage_cap": 3,
                        "stale_receipt": 4,
                    },
                }
            ],
        },
    )
    _write_json(
        data / "wide_f3_batch_interval_attribution_latest.json",
        {
            "generated_at": "2026-07-30T17:30:00Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "copyable_rate_threshold_pct_unchanged": 70.0,
            "f3_lag_limit_s_unchanged": 5.0,
            "instrumentation_completeness": {
                "instrumented_f2_pass_rows": 22,
                "minimum_f2_pass_attempts": 1500,
                "window_complete": False,
            },
            "summary": {
                "batch_interval_attributable_share_pct": 85.0,
                "decision_branch": "ACCRUING_PROSPECTIVE_WINDOW",
                "decision_is_binding": False,
            },
            "decision_rule": {
                "evidence_window": "later of 24h and 1500 attempts"
            },
        },
    )
    _write_json(
        data / "wide_frontier_deficit_partition_latest.json",
        {
            "generated_at": "2026-07-30T19:30:00Z",
            "paper_only": True,
            "measurement_only": True,
            "live_orders_allowed": False,
            "source": {
                "source_checksum": "frontier-checksum",
                "frontier_checksum": "set-checksum",
                "frontier_key": "wallet|wide_policy_fingerprint|source_generation",
                "candidate_count": 20,
            },
            "partition": {
                "zero_deficit_rows": 0,
                "exactly_one_deficit_rows": 1,
                "multi_deficit_rows": 19,
            },
            "single_check_flip_ranking": [
                {
                    "check": "f3_not_enabled_or_cooloff_or_fading",
                    "rows_flipped_if_check_alone_cleared": 1,
                }
            ],
            "not_passed_prevalence": {
                "both_resolved_halves_positive": 7,
            },
            "check_outcomes": {
                "both_resolved_halves_positive": {
                    "explicit_false": 6,
                    "not_evaluated": 1,
                    "not_passed": 7,
                    "not_evaluated_rows": [{"wallet": "0xmissing"}],
                }
            },
        },
    )
    _write_json(
        data / "wide_resolved_signal_accrual_latest.json",
        {
            "generated_at": "2026-07-30T20:40:00Z",
            "paper_only": True,
            "measurement_only": True,
            "source": {"frontier_checksum": "set-checksum"},
            "measurement": {"f1_resolved_signal_bar_unchanged": 200},
            "rows": [
                {
                    "wallet": "0xtop",
                    "resolved_signals": 190,
                    "projected_crossing_at": "2026-07-30T23:00:00Z",
                }
            ],
            "summary": {"top3_current_resolved_signals": [190, 121, 87]},
        },
    )
    _write_json(
        data / "wide_fingerprint_durability_latest.json",
        {
            "generated_at": "2026-07-30T21:10:00Z",
            "paper_only": True,
            "measurement_only": True,
            "source": {"frontier_checksum": "durability-set"},
            "quality_bars_unchanged": {"f1_resolved_signal_bar": 200},
            "frontier_wallets": [{"wallet": "0xtop"}],
            "observation_frame": {
                "blank_recoverability": "FULLY_RECOVERABLE"
            },
            "rows": [{"wallet": "0xtop", "ever_reached_200": True}],
            "summary": {
                "fingerprints_reached_200": 3,
                "move_slice_key_count_vs_max_resolved_pearson_r": 0.7,
            },
        },
    )
    manifest = data / "wide_manifest.json"
    _write_json(manifest, {"manifest_id": "wide-manifest", "summary": {"capture_watch_wallets": 31}})
    _write_json(
        data / "wide_prospective_supervisor_state.json",
        {
            "status": "CAPTURE_AND_SCORER_RESIDENT",
            "managed_run_id": "wide-run",
            "manifest": "data/research/wide_manifest.json",
            "direct_event_cycles": 3,
            "event_handoff": "DIRECT_UNIX_DGRAM_PARSED_ORDERFILLED_V1",
            "latest_cycles": [
                {
                    "started_at_s": 123.0,
                    "results": [
                        {"cmd": ["python", script], "ok": True}
                        for script in update_state_digest._WIDE_SUPERVISOR_REQUIRED_SCORE_STEPS
                    ],
                }
            ],
        },
    )
    _write_json(
        data / "wide_exact_policy_paper_state.json",
        {
            "summary": {"attempted_exact_policy_buys": 10},
            "wallets": {
                "0x424eb20fcd25113e3b98f42522a54580350b263b": {
                    "attempted_exact_policy_buys": 6,
                    "copyable_exact_policy_buys": 4,
                    "resolved_orders": 0,
                }
            },
            "terminal_reconciliation": {
                "run_id": "wide-run",
                "input_rows": 10,
                "terminal_rows": 10,
                "input_equals_terminal": True,
            },
            "attempt_terminals": [
                {"run_id": "wide-run", "receipt_to_fetch_ms": float(index * 100)}
                for index in range(1, 11)
            ],
        },
    )
    climb_wallet = "0x424eb20fcd25113e3b98f42522a54580350b263b"
    climb_fingerprint = "d6d45c4a430354c6842c10cc66129fba710891925afd2c4c3f46d1e56de4c06a"
    _write_json(
        data / "frozen_fingerprint_f2_prewarm_backup_rank_shadow_latest.json",
        {
            "primary": {
                "wallet": climb_wallet,
                "wide_policy_fingerprint": climb_fingerprint,
                "fresh_own_source_buy_rows_30m": 7,
                "f2_minimum": 10,
            }
        },
    )
    _write_json(
        data / "freeze_resolution_accelerator_state.json",
        {
            "direct_climb_priority": [
                {
                    "wallet": climb_wallet,
                    "wide_policy_fingerprint": climb_fingerprint,
                }
            ]
        },
    )
    _write_json(
        data / "wide_direct_admissible_frontier_latest.json",
        {
            "nearest_frontier": [
                {
                    "wallet": climb_wallet,
                    "wide_policy_fingerprint": climb_fingerprint,
                    "direct_source": {
                        "attempts": 7,
                        "copyable": 4,
                        "latest_receipt_at": "2026-07-28T00:42:51Z",
                    },
                }
            ]
        },
    )
    _write_json(
        data / "wide_alpha_capture_roster_latest.json",
        {
            "paper_only": True,
            "live_orders_allowed": False,
            "wallets": [
                {
                    "address": climb_wallet,
                    "enabled": True,
                    "tags": [
                        "paper_only",
                        "direct_climb_priority",
                        "manifest_capture_watch",
                    ],
                }
            ],
        },
    )
    backup_wallet = "0x00033f1089ff061813850e5135483bed39ce3b49"
    backup_fingerprint = (
        "fd05d1bbcb24ba2f6e58f6e0337db000f24b766d83da8193d7e02fa441916317"
    )
    _write_json(
        data / "wide_exact_policy_manifest_climb_backup_fd05.json",
        {
            "capture_watch_wallets": [
                {
                    "wallet": backup_wallet,
                    "paper_measurement_only": True,
                    "promotion_authority": False,
                }
            ]
        },
    )
    _write_json(
        data / "wide_exact_policy_paper_state_climb_backup_fd05.json",
        {
            "updated_at": "2026-07-27T10:09:44Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "manifest": {
                "wallet_policy_identities": {
                    backup_wallet: {
                        "wide_policy_fingerprint": backup_fingerprint,
                        "move_slice_keys": ["060-120|>0.75"],
                    }
                }
            },
            "summary": {
                "attempted_exact_policy_buys": 0,
                "copyable_exact_policy_buys": 0,
            },
        },
    )
    _write_json(
        data / "wide_positive_slice_family_state.json",
        {
            "status": "PROSPECTIVE_ACCRUAL",
            "family_checksum": "family",
            "attrition_funnel": {
                "watched_buy": 10,
                "wallet_match": 2,
                "frozen_slice_match": 0,
            },
            "activation": {
                "status": "EVIDENCE_GATE_CLOSED",
                "live_mutation_allowed": False,
                "family_checksum": "family",
                "evidence_checksum": "evidence",
            },
        },
    )
    _write_json(
        data / "wide_order7a_alpha_causal_reanchor_latest.json",
        {"verdict": "MIX_SHIFT_NONSTATIONARY", "control_chart": {"recenter_applied": True}},
    )
    _write_json(
        data / "wide_order7b_metadata_diagnosis_latest.json",
        {"diagnosis": "NON_BTC5M_BALLAST_PLUS_UNRESOLVED_UNKNOWN", "unknown_shrink": {"rows": 12}},
    )
    _write_json(
        data / "wide_order6_gen2_retarget_latest.json",
        {"decision": "RETARGET_RUNNER_UP", "to_fingerprint": "57d944ade6d26e90"},
    )

    digest, text = build_digest(tmp_path)
    wide = digest["wide_candidate_measurement"]

    assert wide["terminal_reconciliation"]["input_equals_terminal"] is True
    assert wide["direct_latency"] == {
        "samples": 10,
        "p95_receipt_to_fetch_ms": 1000.0,
        "max_receipt_to_fetch_ms": 1000.0,
        "gate_lte_5s": True,
    }
    assert wide["copyable_rate_reachability"]["summary"]["decision_branch"] == (
        "RETIRE_WIDE_EXACT_POLICY_AS_NEAR_TERM_MONEY_ROUTE"
    )
    assert wide["copyable_rate_reachability"]["wallets"][0] == {
        "wallet": "0x424eb20fcd25113e3b98f42522a54580350b263b",
        "as_built_rate_pct": 40.0,
        "policy_addressable_rate_pct": 40.0,
        "denominators_equal": True,
        "residual_taxonomy": {
            "out_of_selected_slice": 1,
            "metadata_missing": 2,
            "slippage_cap": 3,
            "stale_receipt": 4,
        },
    }
    assert "copyable_rate_reachability=" in text
    assert digest["wide_order7a_alpha_causal_reanchor"]["verdict"] == "MIX_SHIFT_NONSTATIONARY"
    assert digest["wide_order7b_metadata_diagnosis"]["unknown_shrink"]["rows"] == 12
    assert digest["wide_order6_gen2_retarget"]["decision"] == "RETARGET_RUNNER_UP"
    assert "order7=MIX_SHIFT_NONSTATIONARY/recenter:True" in text
    assert wide["f3_batch_interval_attribution"]["summary"] == {
        "batch_interval_attributable_share_pct": 85.0,
        "decision_branch": "ACCRUING_PROSPECTIVE_WINDOW",
        "decision_is_binding": False,
    }
    assert wide["f3_batch_interval_attribution"]["instrumentation_completeness"][
        "window_complete"
    ] is False
    assert "f3_batch_interval_attribution=" in text
    assert wide["frontier_deficit_partition"]["partition"] == {
        "zero_deficit_rows": 0,
        "exactly_one_deficit_rows": 1,
        "multi_deficit_rows": 19,
    }
    assert wide["frontier_deficit_partition"]["source"]["frontier_checksum"] == (
        "set-checksum"
    )
    assert wide["frontier_deficit_partition"]["check_outcomes"][
        "both_resolved_halves_positive"
    ]["not_evaluated"] == 1
    assert "frontier_deficit_partition=" in text
    assert wide["resolved_signal_accrual"]["summary"][
        "top3_current_resolved_signals"
    ] == [190, 121, 87]
    assert "resolved_signal_accrual=" in text
    assert wide["fingerprint_durability"]["summary"][
        "fingerprints_reached_200"
    ] == 3
    assert "fingerprint_durability=" in text
    assert wide["supervisor"]["direct_event_cycles"] == 3
    assert wide["roster_direct_climb_members"] == [
        {
            "wallet": climb_wallet,
            "enabled": True,
            "tags": [
                "paper_only",
                "direct_climb_priority",
                "manifest_capture_watch",
            ],
        }
    ]
    assert wide["direct_climb_exact"] == {
        "wallet": climb_wallet,
        "wide_policy_fingerprint": climb_fingerprint,
        "attempted_exact_policy_buys": 6,
        "copyable_exact_policy_buys": 4,
        "resolved_orders": 0,
        "fresh_own_source_buy_rows_30m": 7,
        "f2_minimum": 10,
        "direct_source_attempts": 7,
        "direct_source_copyable": 4,
        "latest_direct_source_receipt_at": "2026-07-28T00:42:51Z",
    }
    assert wide["supervisor_pipeline_deployment"]["status"] == "PASS"
    assert wide["supervisor_pipeline_deployment"]["missing_steps"] == []
    assert wide["positive_slice_family"]["attrition_funnel"]["wallet_match"] == 2
    assert wide["positive_slice_family"]["activation"]["status"] == "EVIDENCE_GATE_CLOSED"
    assert wide["climb_backup"] == {
        "status": "ARMED_PAPER_ONLY",
        "armed_at": "2026-07-27T10:09:44Z",
        "wallet": backup_wallet,
        "wide_policy_fingerprint": backup_fingerprint,
        "move_slice_keys": ["060-120|>0.75"],
        "paper_measurement_only": True,
        "promotion_authority": False,
        "live_orders_allowed": False,
        "summary": {
            "attempted_exact_policy_buys": 0,
            "copyable_exact_policy_buys": 0,
        },
    }
    assert "direct_latency={'samples': 10" in text
    assert "direct_climb_exact={'wallet': '0x424eb20f" in text
    assert f"direct_climb_members=[{{'wallet': '{climb_wallet}'" in text
    assert "climb_backup={'status': 'ARMED_PAPER_ONLY'" in text
    assert "positive_slice_family={'generated_at': None" in text


def test_wide_digest_uses_active_sidecar_f2_when_prewarm_identity_is_retired(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "research"
    climb_wallet = "0x0484e64092ba4108c2786b61e6fc052d3bf41b1a"
    climb_fingerprint = "e836ec9de1bcd054a3dcd804f0d7437627d770fb9e3893c6a81dc15afa7eec32"
    _write_json(
        data / "freeze_resolution_accelerator_state.json",
        {
            "direct_climb_priority": [
                {
                    "wallet": climb_wallet,
                    "wide_policy_fingerprint": climb_fingerprint,
                }
            ]
        },
    )
    _write_json(
        data / "frozen_fingerprint_f2_prewarm_backup_rank_shadow_latest.json",
        {
            "primary": {
                "wallet": "0x424eb20fcd25113e3b98f42522a54580350b263b",
                "wide_policy_fingerprint": "retired",
                "fresh_own_source_buy_rows_30m": 40,
                "f2_minimum": 10,
            }
        },
    )
    _write_json(
        data / "copy_freeze_near_bar_allpass_dryrun_sidecar_latest.json",
        {
            "primary": {
                "wallet": climb_wallet,
                "wide_policy_fingerprint": climb_fingerprint,
                "resolved": 56,
            },
            "checks": {
                "fresh_own_source_buy_rows_30m": 0,
                "f2_minimum": 10,
            },
        },
    )

    digest, _ = build_digest(tmp_path)

    assert digest["wide_candidate_measurement"]["direct_climb_exact"] == {
        "wallet": climb_wallet,
        "wide_policy_fingerprint": climb_fingerprint,
        "attempted_exact_policy_buys": None,
        "copyable_exact_policy_buys": None,
        "resolved_orders": None,
        "fresh_own_source_buy_rows_30m": 0,
        "f2_minimum": 10,
        "direct_source_attempts": None,
        "direct_source_copyable": None,
        "latest_direct_source_receipt_at": None,
    }


def test_wide_digest_flags_resident_pre_sidecar_pipeline(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    _write_json(
        data / "wide_prospective_supervisor_state.json",
        {
            "status": "CAPTURE_AND_SCORER_RESIDENT",
            "latest_cycles": [
                {
                    "started_at_s": 123.0,
                    "results": [
                        {
                            "cmd": ["python", script],
                            "ok": True,
                        }
                        for script in update_state_digest._WIDE_SUPERVISOR_REQUIRED_SCORE_STEPS[
                            :3
                        ]
                    ],
                }
            ],
        },
    )

    digest, text = build_digest(tmp_path)
    deployment = digest["wide_candidate_measurement"][
        "supervisor_pipeline_deployment"
    ]

    assert deployment["status"] == "STALE_OR_INCOMPLETE_PIPELINE"
    assert deployment["missing_steps"] == [
        "scripts/build_frozen_fingerprint_f2_prewarm_shadow.py",
        "scripts/build_copy_freeze_near_bar_allpass_dryrun_sidecar.py",
    ]
    assert "supervisor_pipeline={'status': 'STALE_OR_INCOMPLETE_PIPELINE'" in text


def test_model_runtime_evidence_matches_config_and_latest_turn(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text('model = "gpt-5.6-sol"\n')
    with sqlite3.connect(tmp_path / "logs_2.sqlite") as connection:
        connection.execute(
            "CREATE TABLE logs (id INTEGER PRIMARY KEY, ts INTEGER, ts_nanos INTEGER, "
            "feedback_log_body TEXT)"
        )
        connection.execute(
            "INSERT INTO logs (ts, ts_nanos, feedback_log_body) VALUES (?, ?, ?)",
            (1784508600, 1, "turn model=gpt-5.6-sol codex.turn.reasoning_effort=medium"),
        )

    evidence = update_state_digest._model_runtime_evidence(tmp_path)

    assert evidence["status"] == "MATCH"
    assert evidence["configured_model"] == "gpt-5.6-sol"
    assert evidence["runtime_model"] == "gpt-5.6-sol"
    assert evidence["runtime_observed_at"] == "2026-07-20T00:50:00Z"


def test_state_digest_loads_scorecard_json_from_noisy_stdout() -> None:
    text = (
        'log before {"error":"Could not create api key"}\n'
        '{"kind":"wallet_copy_daily_scorecard","generated_at":"2026-07-08T20:00:00Z"}\n'
    )

    loaded = update_state_digest._load_json_from_text(text, {})

    assert loaded["kind"] == "wallet_copy_daily_scorecard"
    assert loaded["generated_at"] == "2026-07-08T20:00:00Z"


def test_heartbeat_ledger_delta_uses_latest_status_cut_and_money_baseline() -> None:
    latest_status = {
        "heading": "## 2026-07-21T01:49Z codex STATUS [LIVE]",
        "body": "- ledger_delta: today=+$0.040001; since-topup actual=+$12.296820.",
    }
    live = {
        "orders": [
            {"submitted_at": "2026-07-21T01:48:59Z", "status": "FILLED"},
            {"submitted_at": "2026-07-21T01:55:00Z", "status": "REJECTED"},
            {
                "submitted_at": "2026-07-21T02:05:00Z",
                "status": "FILLED",
                "trade_result": {"response_filled_size_usd": 1.049998},
            },
        ]
    }

    delta = update_state_digest._heartbeat_ledger_delta(
        live,
        latest_status,
        current_day_pnl_usd=-0.959998,
        current_since_topup_actual_usd=11.221101,
    )

    assert delta["ledger_records"] == 2
    assert delta["status_counts"] == {"REJECTED": 1, "FILLED": 1}
    assert delta["filled_size_usd"] == 1.049998
    assert delta["realized_pnl_delta_usd"] == -0.999999
    assert delta["since_topup_actual_delta_usd"] == -1.075719


def test_heartbeat_ledger_delta_prefers_canonical_day_over_sensor_today_count() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-21T18:15Z codex STATUS [LIVE]",
            "body": "- ledger_delta: canonical day=-1.852020 USD.\n- sensor: peer_active_idle_windows today=97.",
        },
        current_day_pnl_usd=-3.852019,
        current_since_topup_actual_usd=None,
    )

    assert delta["previous_day_pnl_usd"] == -1.85202
    assert delta["realized_pnl_delta_usd"] == -1.999999


def test_heartbeat_ledger_delta_does_not_read_episode_today_as_day_pnl() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-29T20:46:04Z codex STATUS [LIVE]",
            "body": (
                "- episodes [OBSERVE]: episodes_today=`5`; natural_clears=`4`.\n"
                "- money [LIVE]: freshest pre-incident digest day=`+$15.398484`, "
                "since-topup actual=`-$5.640017` `NOT_PRODUCING`."
            ),
        },
        current_day_pnl_usd=15.398484,
        current_since_topup_actual_usd=-6.711396,
    )

    assert delta["previous_day_pnl_usd"] == 15.398484
    assert delta["realized_pnl_delta_usd"] == 0.0


def test_heartbeat_ledger_delta_reads_day_from_canonical_pnl_bullet_only() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-21T22:03Z codex STATUS [LIVE]",
            "body": (
                "- volume: day=46/288.\n"
                "- pnl [LIVE]: day=-$11.307979/46 resolved, ROI=-24.58%.\n"
                "- sensor: peer_active_idle_windows day=97."
            ),
        },
        current_day_pnl_usd=-10.139503,
        current_since_topup_actual_usd=None,
    )

    assert delta["previous_day_pnl_usd"] == -11.307979
    assert delta["realized_pnl_delta_usd"] == 1.168476


def test_heartbeat_ledger_delta_reads_day_from_money_bullet() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-29T16:48:10Z codex STATUS [LIVE]",
            "body": (
                "- money [LIVE]: day=`+$7.039305` / `80` fills; "
                "since-topup actual=`-$11.030823` `NOT_PRODUCING`.\n"
                "- volume [OBSERVE]: windows_traded=`80/288`."
            ),
        },
        current_day_pnl_usd=7.45833,
        current_since_topup_actual_usd=-13.090081,
    )

    assert delta["previous_day_pnl_usd"] == 7.039305
    assert delta["realized_pnl_delta_usd"] == 0.419025


def test_heartbeat_ledger_delta_reads_final_values_from_status_transitions() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-22T12:27Z codex STATUS [LIVE]",
            "body": (
                "- pnl [LIVE]: day +$2.496021 -> +$3.677840, realized delta +$1.181819; "
                "since-topup actual +$2.647366 -> +$6.848200, delta +$4.200834."
            ),
        },
        current_day_pnl_usd=3.903648,
        current_since_topup_actual_usd=3.721333,
    )

    assert delta["previous_day_pnl_usd"] == 3.67784
    assert delta["realized_pnl_delta_usd"] == 0.225808
    assert delta["previous_since_topup_actual_usd"] == 6.8482
    assert delta["since_topup_actual_delta_usd"] == -3.126867


def test_heartbeat_ledger_delta_reads_hungarian_markdown_money_bullet() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-29T02:06:01Z codex STATUS [LIVE]",
            "body": (
                "- daily-PnL gap [LIVE]: nap=`-$0.980357/8 resolved`; "
                "since-topup actual=`-$18.459418`, `NOT_PRODUCING`."
            ),
        },
        current_day_pnl_usd=-2.980355,
        current_since_topup_actual_usd=-20.581886,
    )

    assert delta["previous_day_pnl_usd"] == -0.980357
    assert delta["realized_pnl_delta_usd"] == -1.999998
    assert delta["previous_since_topup_actual_usd"] == -18.459418
    assert delta["since_topup_actual_delta_usd"] == -2.122468


def test_heartbeat_ledger_delta_reads_daily_pnl_from_acceptance_gap_bullet() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-29T15:16:22Z codex STATUS [LIVE/DEFEND/OBSERVE]",
            "body": (
                "- gaps [LIVE]: continuity gap persists at `74/288` traded windows; "
                "daily-PnL gap persists at `+$3.727677`, about `$96.27` below the "
                "`$100` floor; since-topup actual=`-$16.513824` NOT_PRODUCING."
            ),
        },
        current_day_pnl_usd=1.607678,
        current_since_topup_actual_usd=-18.721753,
    )

    assert delta["previous_day_pnl_usd"] == 3.727677
    assert delta["realized_pnl_delta_usd"] == -2.119999
    assert delta["previous_since_topup_actual_usd"] == -16.513824
    assert delta["since_topup_actual_delta_usd"] == -2.207929


def test_heartbeat_ledger_delta_reads_daily_pnl_from_equals_gap_bullet() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-29T15:49:13Z codex STATUS [LIVE/DEFEND/OBSERVE/SELF-DEV]",
            "body": (
                "- gaps [LIVE]: continuity gap=`76/288` traded windows; "
                "daily-PnL gap=`+$1.607678`, `$98.39` below the `$100` floor; "
                "since-topup actual=`-$18.721753` NOT_PRODUCING."
            ),
        },
        current_day_pnl_usd=2.711844,
        current_since_topup_actual_usd=-17.699013,
    )

    assert delta["previous_day_pnl_usd"] == 1.607678
    assert delta["realized_pnl_delta_usd"] == 1.104166
    assert delta["previous_since_topup_actual_usd"] == -18.721753
    assert delta["since_topup_actual_delta_usd"] == 1.02274


def test_heartbeat_ledger_delta_reads_day_from_underscored_gap_bullet() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-29T19:28:17Z codex STATUS [LIVE/DEFEND/OBSERVE]",
            "body": (
                "- daily_pnl_gap [LIVE]: day=`+$13.966857`, `$86.033143` below "
                "`$100`; since-topup actual=`-$7.028554` `NOT_PRODUCING`."
            ),
        },
        current_day_pnl_usd=13.966857,
        current_since_topup_actual_usd=-7.028554,
    )

    assert delta["previous_day_pnl_usd"] == 13.966857
    assert delta["realized_pnl_delta_usd"] == 0.0
    assert delta["previous_since_topup_actual_usd"] == -7.028554
    assert delta["since_topup_actual_delta_usd"] == 0.0


def test_heartbeat_ledger_delta_reads_final_day_from_ledger_delta_transition() -> None:
    delta = update_state_digest._heartbeat_ledger_delta(
        {"orders": []},
        {
            "heading": "## 2026-07-29T21:50:29Z codex STATUS [LIVE]",
            "body": (
                "- ledger_delta [LIVE]: since `21:29:28Z` rows=`3`; "
                "day PnL `+$15.439301→+$14.419302`, delta=`-$1.019999`.\n"
                "- money [LIVE]: since-topup actual `-$5.670580→-$6.779589`."
            ),
        },
        current_day_pnl_usd=17.775186,
        current_since_topup_actual_usd=-3.463595,
    )

    assert delta["previous_day_pnl_usd"] == 14.419302
    assert delta["realized_pnl_delta_usd"] == 3.355884
    assert delta["previous_since_topup_actual_usd"] == -6.779589
    assert delta["since_topup_actual_delta_usd"] == 3.315994


def test_eth5m_tombstone_is_not_reported_as_stale_active_cadence() -> None:
    cadence = update_state_digest._eth5m_scout_cadence(
        {
            "generated_at": "2026-07-20T01:53:26Z",
            "status": "TOMBSTONED_DECISIVE_NEGATIVE",
            "tombstone": {
                "id": "TOMBSTONE-ETH5M-REPL-20260720",
                "bound_at": "2026-07-20T02:00:00Z",
                "reopen_allowed": False,
                "final_post_fee_pnl_usd": -753.752943,
            },
        },
        now=datetime(2026, 7, 21, 18, 40, tzinfo=UTC),
    )

    assert cadence["status"] == "NO_ACTIVE_ITEM_TOMBSTONED"
    assert cadence["time_in_stage_h"] == 0.0
    assert cadence["collector_expected_running"] is False
    assert cadence["breached"] is False


def test_rotation_wallets_accepts_scalar_fields() -> None:
    rotation = {
        "demoted_wallet": "0xF418D3A1A941292F9C8707D62A14980C5BEB95A3",
        "admitted_wallet": "0xDF2C0702FC00BE90BD795234A86E28F1ED39118A",
    }

    assert update_state_digest._rotation_wallets(
        rotation,
        list_key="demoted_wallets",
        scalar_key="demoted_wallet",
    ) == ["0xf418d3a1a941292f9c8707d62a14980c5beb95a3"]
    assert update_state_digest._rotation_wallets(
        rotation,
        list_key="admitted_wallets",
        scalar_key="admitted_wallet",
    ) == ["0xdf2c0702fc00be90bd795234a86e28f1ed39118a"]


def test_active_set_pin_consumer_sweep_marks_packet_snapshots_display_only() -> None:
    report = build_pin_consumer_sweep(
        overlay={
            "selection_pin": {
                "pin_id": "weekend-seat-loss-f418-to-a689",
                "source_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
                "expires_at": "2026-07-18T13:30:43Z",
                "quiet_clock_expiry_derived": True,
                "quiet_clock": {
                    "anchor_iso": "2026-07-18T09:30:43Z",
                    "earliest_fire_iso": "2026-07-18T13:30:43Z",
                },
            }
        },
        rotation_packet={
            "status": "PRESTAGED_NO_LIVE_CHANGE",
            "quiet_clock": {
                "anchor_iso": "2026-07-18T09:10:46Z",
                "earliest_fire_iso": "2026-07-18T13:10:46Z",
            },
        },
        rotation_execution={
            "status": "SELECTED_ROTATION_PIN_WRITTEN",
            "selection_pin": {
                "source_wallet": "0xf418d3a1a941292f9c8707d62a14980c5beb95a3",
                "expires_at": "2026-07-18T07:00:23Z",
            },
        },
    )

    assert report["status"] == "PASS_NO_PACKET_EXPIRY_CONSUMERS"
    assert report["dangerous_consumers"] == []
    assert report["source_of_truth"] == "data/research/wallet_copy_active_set_auto_degrade_state.json.selection_pin"
    assert {row["snapshot_authority"] for row in report["packet_snapshots"]} == {
        "DISPLAY_ONLY_NOT_PIN_EXPIRY_AUTHORITY"
    }


def test_state_digest_surfaces_pin_consumer_sweep(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-18T09:34Z fable DIRECTION [LIVE/DEFEND]\n"
        "- next: pin-consumer sweep above.\n"
    )
    _write_json(
        data / "active_set_pin_consumer_sweep_latest.json",
        {
            "generated_at": "2026-07-18T09:50:00Z",
            "status": "PASS_NO_PACKET_EXPIRY_CONSUMERS",
            "source_of_truth": "data/research/wallet_copy_active_set_auto_degrade_state.json.selection_pin",
            "dangerous_consumers": [],
            "finding": "No checked consumer enforces pin expiry from packet snapshots.",
            "packet_snapshots": [
                {
                    "path": "data/research/active_set_selected_rotation_execution_latest.json",
                    "snapshot_authority": "DISPLAY_ONLY_NOT_PIN_EXPIRY_AUTHORITY",
                }
            ],
        },
    )

    digest, text = build_digest(root)

    assert digest["active_set_pin_consumer_sweep"]["status"] == "PASS_NO_PACKET_EXPIRY_CONSUMERS"
    assert "pin_consumer_sweep=PASS_NO_PACKET_EXPIRY_CONSUMERS/dangerous=0" in text
    assert "/snapshots=display_only" in text


def test_state_digest_surfaces_active_member_orderfilled_hot_source_shadow(
    tmp_path: Path,
) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text("")
    _write_json(
        data / "active_member_orderfilled_hot_source_shadow_state.json",
        {
            "status": "ACCRUING",
            "paper_only": True,
            "live_orders_allowed": False,
            "prospective_wallet_count": 1,
            "prospective_wallets": ["0x805a8bbd411324a1c121dd87fac96c2fb13012c8"],
            "prospective_current_market": {
                "genuine_buy_identities": 0,
                "required_genuine_buy_identities": 10,
                "gate_passed": False,
                "reconciled": True,
            },
            "unique_resolved_source_events": 52,
            "required_unique_resolved_source_events": 100,
            "current_or_next_window_events": 29,
            "token_mapping_missing": 0,
            "identity_market_outcome_parity_violations": 0,
            "live_source_wiring_gate_passed": False,
            "resource_usage": {"max_rss_gib": 0.19, "under_limit": True},
            "incremental_reader": {"accumulator_rows": 855},
            "pid": 55783,
        },
    )

    digest, text = build_digest(root)

    shadow = digest["active_member_orderfilled_hot_source_shadow"]
    assert shadow["unique_resolved_source_events"] == 52
    assert shadow["prospective_wallet_count"] == 1
    assert shadow["prospective_current_market"]["genuine_buy_identities"] == 0
    assert shadow["prospective_current_market"]["reconciled"] is True
    assert shadow["resource_usage"]["under_limit"] is True
    assert "corrected_gate=52/100" in text
    assert "mapping_missing=0 parity=0 gate=False rss_gib=0.19" in text


def test_state_digest_surfaces_orderfilled_fast_lane(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text("")
    _write_json(
        data / "wallet_copy_orderfilled_fast_lane_state.json",
        {
            "status": "PROTECTED_NO_SURVIVOR",
            "pid": 15303,
            "thread_name": "orderfilled_fast_lane_tick",
            "sole_submitter_process": "scripts/run_wallet_copy_live_guard.py",
            "wake_source": "writer_datagram",
            "stat_poll_fallback_s": 0.1,
            "receipt_to_guard_sample_count": 4,
            "receipt_to_guard_p95_s": 0.063856,
            "runtime_generation": "generation-a",
            "source_report": {"status": "PASS", "eligible_rows": 1},
            "bridge_report": {
                "status": "PROTECTED_NO_SURVIVOR",
                "submit_stage_invocations": 0,
            },
        },
    )
    _write_json(
        data / "copy_source_wake_activation_latest.json",
        {
            "activated": True,
            "paper_only_proof": True,
            "live_orders_allowed": False,
            "paper_survivor_identity": "polygon_orderfilled:0xabc|7",
            "paper_survivor": {"orders_submitted": 0},
            "runtime_generation": "proof-generation",
        },
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "pid": 15303,
            "guard_code_identity": {
                "git_head_at_launch": "abc123",
                "script_sha256": "script-sha",
                "live_guard_generation_sha256": "guard-generation",
            },
        },
    )
    (data / "order_flow_deadman_incidents.jsonl").write_text(
        json.dumps(
            {
                "checked_at": "2026-07-24T23:37:02Z",
                "policy_choke": {
                    "actuator": {
                        "status": "RUNG_C_NO_ADMISSIBLE_TARGET",
                        "reason": "dry",
                        "refusal_counts": {"f2": 150},
                        "quality_bars_unchanged": True,
                    }
                },
            }
        )
        + "\n"
    )

    digest, text = build_digest(root)

    fast_lane = digest["orderfilled_fast_lane"]
    assert fast_lane["receipt_to_guard_sample_count"] == 4
    assert fast_lane["source_report"]["eligible_rows"] == 1
    assert "orderfilled_fast_lane: status=PROTECTED_NO_SURVIVOR" in text
    assert "wake=writer_datagram samples=4 p95_s=0.063856" in text
    assert "source=PASS/1 bridge=PROTECTED_NO_SURVIVOR/0" in text
    assert "sole=scripts/run_wallet_copy_live_guard.py" in text
    activation = digest["copy_source_wake_activation"]
    assert activation["paper_survivor_orders_submitted"] == 0
    assert activation["resident_guard"]["git_head_at_launch"] == "abc123"
    assert activation["forced_sweep"]["status"] == "RUNG_C_NO_ADMISSIBLE_TARGET"
    assert "wake_activation: active=True" in text
    assert "proof=polygon_orderfilled:0xabc|7 proof_submits=0" in text
    assert "resident=15303/abc123/script-sha generation=guard-generation" in text
    assert "forced_sweep=RUNG_C_NO_ADMISSIBLE_TARGET refusals={'f2': 150}" in text


def test_state_digest_surfaces_runtime_speed_metric_statuses(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-18T12:36Z fable DIRECTION [SELF-DEV]\n"
        "- next: runtime speed hardening.\n"
    )
    _write_json(
        data / "runtime_speed_baseline_latest.json",
        {
            "status": "STALE_SAMPLE",
            "generated_at": "2026-07-18T13:05:00Z",
            "baseline_created_at": "2026-07-10T18:34:19Z",
            "comparison": {
                "regression_count": 0,
                "regressions": [],
            },
            "metrics": {
                "guard_cycle_total_s": {"value": 10.0},
                "signal_age_p90_s": {"value": 1.0},
                "heartbeat_cadence_latest_s": {"value": 600.0},
                "brainless_run_duration_s": {
                    "value": 632.0,
                    "status": "STALE_SAMPLE",
                    "sample_age_s": 1800.0,
                    "post_stale_lock_reclaim_run_ordinal": 1,
                },
                "ask_fable_latest_wall_s": {
                    "value": 391.0,
                    "status": "INFO",
                    "return_code": 0,
                },
                "state_digest_generation_s": {"value": 4.0},
                "signal_to_order_p90_s": {"value": 39.0},
            },
            "next_action": "restore producer",
        },
    )
    (data / "daily_scorecard_timing_latest.err").write_text("real 22.15\nuser 6.97\nsys 0.63\n")

    digest, text = build_digest(root)

    runtime_speed = digest["runtime_speed_baseline"]
    assert runtime_speed["metric_statuses"]["brainless_run_duration_s"] == "STALE_SAMPLE"
    assert runtime_speed["metric_statuses"]["ask_fable_latest_wall_s"] == "INFO"
    scorecard_runtime = digest["scorecard_runtime_evidence"]
    assert scorecard_runtime["status"] == "PASS"
    assert scorecard_runtime["real_s"] == 22.15
    assert scorecard_runtime["user_s"] == 6.97
    assert scorecard_runtime["sys_s"] == 0.63
    assert "brainless=632.0/STALE_SAMPLE(age=1800.0,post_reclaim_run=1)" in text
    assert "scorecard_direct=22.15/PASS" in text
    assert "ask_fable=391.0/INFO(rc=0)" in text


def test_state_digest_tracks_runtime_speed_persistence_counts(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-18T19:40Z fable DIRECTION [SELF-DEV]\n"
        "- next: count post-restart runtime speed rows.\n"
    )
    _write_json(
        data / "order_flow_deadman_state.json",
        {
            "guard_memory": {
                "pid": 200,
                "restart_gib": 16.0,
                "samples": [
                    {"pid": 100, "rss_gib": 19.3, "checked_at": "2026-07-18T19:38:57Z"},
                    {"pid": 200, "rss_gib": 2.9, "checked_at": "2026-07-18T19:49:17Z"},
                ],
            }
        },
    )
    _write_json(
        data / "state_digest.json",
        {
            "runtime_speed_baseline": {
                "persistence_counts": {
                    "scope_start_at": "2026-07-18T19:38:57Z",
                    "scope_pid": 200,
                    "rows": {
                        "brainless_step:state_digest": {
                            "consecutive_over_threshold": 2,
                            "status": "REGRESSION",
                        },
                        "brainless_step:cli_versions": {
                            "consecutive_over_threshold": 1,
                            "status": "REGRESSION",
                        },
                    },
                }
            }
        },
    )
    _write_json(
        data / "runtime_speed_baseline_latest.json",
        {
            "status": "REGRESSION",
            "generated_at": "2026-07-18T19:51:47Z",
            "baseline_created_at": "2026-07-18T16:42:08Z",
            "comparison": {
                "regression_count": 1,
                "regressions": [
                    {"metric": "brainless_step:state_digest", "status": "REGRESSION"},
                ],
                "rows": [
                    {
                        "metric": "brainless_step:state_digest",
                        "status": "REGRESSION",
                        "ratio": 3.05,
                        "threshold_ratio": 1.2,
                    },
                    {
                        "metric": "brainless_step:cli_versions",
                        "status": "PASS",
                        "ratio": 1.0,
                        "threshold_ratio": 1.2,
                    },
                    {
                        "metric": "brainless_run_duration_s",
                        "status": "PASS",
                        "ratio": 1.05,
                        "threshold_ratio": 1.2,
                    },
                ],
            },
            "metrics": {
                "guard_cycle_total_s": {"value": 8.0},
                "brainless_run_duration_s": {"value": 244.0, "status": "PASS"},
            },
        },
    )

    digest, text = build_digest(root)

    persistence = digest["runtime_speed_baseline"]["persistence_counts"]
    assert persistence["scope"] == "post_restart"
    assert persistence["scope_start_at"] == "2026-07-18T19:38:57Z"
    assert persistence["scope_source"] == "guard_memory_samples"
    assert persistence["scope_pid"] == 200
    assert persistence["rows"]["brainless_step:state_digest"]["consecutive_over_threshold"] == 3
    assert persistence["rows"]["brainless_step:cli_versions"]["consecutive_over_threshold"] == 0
    assert persistence["actionable_candidates"] == ["brainless_step:state_digest"]
    assert "persistence_counts=['brainless_step:state_digest:3']" in text
    assert "actionable=['brainless_step:state_digest']" in text


def test_runtime_speed_persistence_counts_do_not_increment_same_sample() -> None:
    order_flow_deadman = {
        "guard_memory": {
            "pid": 200,
            "restart_gib": 16.0,
            "samples": [
                {"pid": 100, "rss_gib": 19.3, "checked_at": "2026-07-18T19:38:57Z"},
                {"pid": 200, "rss_gib": 2.9, "checked_at": "2026-07-18T19:49:17Z"},
            ],
        }
    }
    runtime_speed = {
        "generated_at": "2026-07-18T19:51:47Z",
        "comparison": {
            "rows": [
                {
                    "metric": "brainless_step:state_digest",
                    "status": "REGRESSION",
                    "ratio": 2.8,
                    "threshold_ratio": 1.2,
                }
            ]
        },
    }
    previous_digest = {
        "runtime_speed_baseline": {
            "persistence_counts": {
                "scope_start_at": "2026-07-18T19:38:57Z",
                "scope_pid": 200,
                "runtime_speed_generated_at": "2026-07-18T19:51:47Z",
                "rows": {
                    "brainless_step:state_digest": {
                        "consecutive_over_threshold": 2,
                        "status": "REGRESSION",
                    }
                },
            }
        }
    }

    counts = update_state_digest._runtime_speed_persistence_counts(
        runtime_speed,
        previous_digest,
        order_flow_deadman,
    )

    assert counts["rows"]["brainless_step:state_digest"]["consecutive_over_threshold"] == 2
    assert counts["actionable_candidates"] == []


def test_runtime_speed_persistence_counts_ignore_external_wait_rows() -> None:
    runtime_speed = {
        "generated_at": "2026-07-18T20:34:00Z",
        "comparison": {
            "rows": [
                {
                    "metric": "brainless_step:wallet_outflow_deadman",
                    "status": "REGRESSION",
                    "current": 61.50189,
                    "baseline": 21.710881,
                    "ratio": 2.832768,
                    "threshold_ratio": 1.2,
                }
            ]
        },
    }
    previous_digest = {
        "runtime_speed_baseline": {
            "persistence_counts": {
                "scope_start_at": "2026-07-18T19:38:58Z",
                "scope_pid": 200,
                "rows": {
                    "brainless_step:wallet_outflow_deadman": {
                        "consecutive_over_threshold": 2,
                        "status": "REGRESSION",
                    }
                },
            }
        }
    }
    order_flow_deadman = {
        "guard_memory": {
            "pid": 200,
            "restart_gib": 16.0,
            "samples": [{"pid": 200, "rss_gib": 2.9, "checked_at": "2026-07-18T20:15:17Z"}],
        }
    }
    wallet_outflow_deadman = {
        "fetch": {
            "transfer_attempts": [
                {
                    "source": "polygon_rpc_eth_getLogs",
                    "status": "HTTP_403",
                    "duration_s": 0.617,
                },
                {
                    "source": "blockscout_account_tokentx",
                    "status": "ERROR",
                    "duration_s": 39.038,
                },
                {
                    "source": "rpc_secondary",
                    "status": "OK",
                    "duration_s": 8.255,
                },
            ]
        }
    }

    counts = update_state_digest._runtime_speed_persistence_counts(
        runtime_speed,
        previous_digest,
        order_flow_deadman,
        wallet_outflow_deadman,
        fallback_scope_start_at="2026-07-18T19:38:58Z",
    )

    row = counts["rows"]["brainless_step:wallet_outflow_deadman"]
    assert row["consecutive_over_threshold"] == 0
    assert row["status"] == "EXPLAINED_EXTERNAL"
    assert row["raw_status"] == "REGRESSION"
    assert row["regression_exclusion"]["external_wait_s"] == 47.91
    assert counts["actionable_candidates"] == []


def test_runtime_speed_persistence_counts_ignore_subsecond_ratio_rows() -> None:
    runtime_speed = {
        "generated_at": "2026-07-18T20:34:00Z",
        "comparison": {
            "rows": [
                {
                    "metric": "brainless_step:live_guard_auto_restart",
                    "status": "REGRESSION",
                    "current": 0.233815,
                    "baseline": 0.149243,
                    "ratio": 1.566673,
                    "threshold_ratio": 1.2,
                }
            ]
        },
    }
    previous_digest = {
        "runtime_speed_baseline": {
            "persistence_counts": {
                "scope_start_at": "2026-07-18T19:38:58Z",
                "scope_pid": 200,
                "rows": {
                    "brainless_step:live_guard_auto_restart": {
                        "consecutive_over_threshold": 2,
                        "status": "REGRESSION",
                    }
                },
            }
        }
    }
    order_flow_deadman = {
        "guard_memory": {
            "pid": 200,
            "restart_gib": 16.0,
            "samples": [{"pid": 200, "rss_gib": 2.9, "checked_at": "2026-07-18T20:15:17Z"}],
        }
    }

    counts = update_state_digest._runtime_speed_persistence_counts(
        runtime_speed,
        previous_digest,
        order_flow_deadman,
        fallback_scope_start_at="2026-07-18T19:38:58Z",
    )

    row = counts["rows"]["brainless_step:live_guard_auto_restart"]
    assert row["consecutive_over_threshold"] == 0
    assert row["status"] == "PASS_ABS_DELTA_LT_1S"
    assert row["raw_status"] == "REGRESSION"
    assert row["regression_exclusion"]["delta_s"] == 0.084572
    assert counts["actionable_candidates"] == []


def test_state_digest_recovers_runtime_speed_scope_from_handoff_after_sample_rolloff(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-18T19:40Z fable DIRECTION [SELF-DEV]\n"
        "- HEADLINE: the RSS-16 tripwire FIRED and the auto-restart EXECUTED at 19:38:58Z.\n"
        "- ACCEPTANCE CLOCK STARTS NOW: the post-restart treatment-panel grading begins at 19:38:58Z.\n"
    )
    _write_json(
        data / "order_flow_deadman_state.json",
        {
            "guard_memory": {
                "pid": 200,
                "restart_gib": 16.0,
                "samples": [
                    {"pid": 200, "rss_gib": 2.9, "checked_at": "2026-07-18T20:15:17Z"},
                    {"pid": 200, "rss_gib": 3.1, "checked_at": "2026-07-18T20:20:17Z"},
                ],
            }
        },
    )
    _write_json(
        data / "state_digest.json",
        {
            "runtime_speed_baseline": {
                "persistence_counts": {
                    "scope_start_at": "2026-07-18T19:38:58Z",
                    "scope_pid": 200,
                    "runtime_speed_generated_at": "2026-07-18T20:07:47Z",
                    "rows": {
                        "brainless_step:state_digest": {
                            "consecutive_over_threshold": 2,
                            "status": "REGRESSION",
                        },
                    },
                }
            }
        },
    )
    _write_json(
        data / "runtime_speed_baseline_latest.json",
        {
            "status": "REGRESSION",
            "generated_at": "2026-07-18T20:23:47Z",
            "baseline_created_at": "2026-07-18T16:42:08Z",
            "comparison": {
                "regression_count": 1,
                "regressions": [
                    {"metric": "brainless_step:state_digest", "status": "REGRESSION"},
                ],
                "rows": [
                    {
                        "metric": "brainless_step:state_digest",
                        "status": "REGRESSION",
                        "ratio": 2.8,
                        "threshold_ratio": 1.2,
                    },
                ],
            },
        },
    )

    digest, text = build_digest(root)

    persistence = digest["runtime_speed_baseline"]["persistence_counts"]
    assert persistence["scope"] == "post_restart"
    assert persistence["scope_start_at"] == "2026-07-18T19:38:58Z"
    assert persistence["scope_source"] == "handoff_direction"
    assert persistence["scope_pid"] == 200
    assert persistence["rows"]["brainless_step:state_digest"]["consecutive_over_threshold"] == 3
    assert persistence["actionable_candidates"] == ["brainless_step:state_digest"]
    assert "persistence_scope=post_restart@2026-07-18T19:38:58Z" in text


def test_state_digest_handoff_restart_scope_ignores_post_restart_order_time() -> None:
    entries = [
        {
            "heading": "## 2026-07-18T20:05Z fable DIRECTION [LIVE/DEFEND]",
            "body": (
                "- ANSWER: order 0x3548 accepted at 20:00:28Z "
                "after the 19:38:58Z restart.\n"
                "- NEXT: report terminal fill later."
            ),
        }
    ]

    assert (
        update_state_digest._latest_post_restart_scope_start_from_handoff(entries)
        == "2026-07-18T19:38:58Z"
    )


def test_runtime_speed_persistence_counts_do_not_carry_across_pid_change() -> None:
    order_flow_deadman = {
        "guard_memory": {
            "pid": 300,
            "restart_gib": 16.0,
            "samples": [
                {"pid": 300, "rss_gib": 2.9, "checked_at": "2026-07-18T20:15:17Z"},
            ],
        }
    }
    runtime_speed = {
        "generated_at": "2026-07-18T20:23:47Z",
        "comparison": {
            "rows": [
                {
                    "metric": "brainless_step:state_digest",
                    "status": "REGRESSION",
                    "ratio": 2.8,
                    "threshold_ratio": 1.2,
                }
            ]
        },
    }
    previous_digest = {
        "runtime_speed_baseline": {
            "persistence_counts": {
                "scope_start_at": "2026-07-18T19:38:58Z",
                "scope_pid": 200,
                "runtime_speed_generated_at": "2026-07-18T20:07:47Z",
                "rows": {
                    "brainless_step:state_digest": {
                        "consecutive_over_threshold": 2,
                        "status": "REGRESSION",
                    }
                },
            }
        }
    }

    counts = update_state_digest._runtime_speed_persistence_counts(
        runtime_speed,
        previous_digest,
        order_flow_deadman,
        fallback_scope_start_at="2026-07-18T19:38:58Z",
    )

    assert counts["scope"] == "post_restart"
    assert counts["scope_pid"] == 300
    assert counts["rows"]["brainless_step:state_digest"]["consecutive_over_threshold"] == 1
    assert counts["actionable_candidates"] == []


def test_state_digest_member_trigger_watch_from_canonical_events() -> None:
    wallet = "0xdf2c0702fc00be90bd795234a86e28f1ed39118a"
    scorecard = {
        "canonical_pnl_truth": {
            "events": [
                {"source_wallet": wallet, "submitted_at": "2026-07-17T00:00:00Z", "resolved": True, "pnl_usd": -2.0},
                {"source_wallet": wallet, "submitted_at": "2026-07-17T00:05:00Z", "resolved": True, "pnl_usd": 1.0},
                {"source_wallet": wallet, "submitted_at": "2026-07-17T00:10:00Z", "resolved": True, "pnl_usd": -3.5},
                {"source_wallet": wallet, "submitted_at": "2026-07-17T00:15:00Z", "resolved": True, "pnl_usd": -1.0},
                {"source_wallet": wallet, "submitted_at": "2026-07-17T00:20:00Z", "resolved": True, "pnl_usd": 0.75},
                {"source_wallet": wallet, "submitted_at": "2026-07-17T00:25:00Z", "resolved": True, "pnl_usd": -0.5},
                {"source_wallet": wallet, "submitted_at": "2026-07-17T00:30:00Z", "resolved": False, "pnl_usd": -9.0},
            ]
        }
    }

    watch = update_state_digest._canonical_member_trigger_watch(scorecard)[wallet]

    assert watch["resolved_fills"] == 6
    assert watch["resolved_only_pnl_usd"] == -5.25
    assert watch["last6_signs"] == "-+--+-"
    assert watch["tail_negative"] == 1
    assert watch["distance_to_first_slice_trigger_usd"] == 0.75
    assert watch["trigger_fired_by_pnl"] is False
    assert watch["trigger_fired_by_tail"] is False


def test_state_digest_surfaces_live_reject_negative_fill_summary(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-17T13:52Z Fable DIRECTION [LIVE/MEASURE/VOLUME]\n"
        "- next: negative-fill / maker-recovery PnL attribution.\n"
    )
    _write_json(
        data / "live_order_reject_attribution_latest.json",
        {
            "generated_at": "2026-07-17T14:00:00Z",
            "summary": {
                "rejects": 83,
                "orders": 142,
                "dominant_class": "price_band_or_policy_cap",
                "forgone_usd_estimate": 103.173621,
                "scheduler_triggered_subset": {"rejects": 0},
            },
            "fak_no_match_analysis": {
                "summary": {
                    "unrecovered_rows": 9,
                    "net_unrecovered_forgone_usd": 12.7625,
                    "race_loss_usd": 1.0,
                }
            },
            "negative_fill_pnl_analysis": {
                "summary": {
                    "negative_fills": 25,
                    "negative_pnl_usd": -50.683645,
                    "maker_recovery_vs_direct_taker": {
                        "maker_recovery_avg_pnl_usd": 1.484211,
                        "direct_taker_avg_pnl_usd": 0.122065,
                    },
                    "aggregate_by_price_band_tranche_type": {
                        "01_25_50|direct_taker_fill": {
                            "resolved_fills": 32,
                            "roi_pct": 4.74513,
                            "fix_candidate_gate": {"passes": False},
                        },
                        "02_50_70|direct_taker_fill": {
                            "resolved_fills": 7,
                            "roi_pct": 19.95334,
                            "fix_candidate_gate": {"passes": False},
                        },
                    },
                }
            },
        },
    )

    digest, text = build_digest(root)

    live_reject = digest["live_order_reject_attribution"]
    assert live_reject["fak_no_match_summary"]["unrecovered_rows"] == 9
    assert live_reject["negative_fill_pnl_summary"]["negative_fills"] == 25
    assert "fak_unrecovered=9/12.7625 race_loss=1.0" in text
    assert "negative_fills=25 negative_pnl=-50.683645" in text
    assert "maker_recovery_avg=1.484211 direct_taker_avg=0.122065" in text
    assert "band_gate_passes=[]" in text
    assert "('01_25_50|direct_taker_fill', 32, 4.74513)" in text


def test_state_digest_surfaces_pinned_tranche_economics(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-16T08:47Z fable DIRECTION [MEASURE/LIVE/DEFEND]\n"
        "- next: pinned-fill economics packet at resolved-pinned n>=10.\n"
    )
    _write_json(
        data / "wallet_copy_pinned_tranche_economics_latest.json",
        {
            "generated_at": "2026-07-16T09:05:00Z",
            "status": "TRIGGER_MET_PACKET_READY",
            "threshold_change_allowed": False,
            "inputs": {"scorecard_generated_at": "2026-07-16T08:57:48Z"},
            "summary": {
                "resolved_pinned_fills": 11,
                "pinned_filled_orders": 12,
                "pinned_status_counts": {"FILLED": 12, "REJECTED": 1},
                "pnl_usd": 10.971504,
                "win_rate_pct": 72.727273,
                "breakeven_win_rate_pct": 47.812345,
                "wilson_95_lower_bound_win_rate_pct": 43.210987,
                "wilson_lower_bound_gt_breakeven": False,
                "worst_bucket": {"price_bucket": "01_25_50", "pnl_usd": 10.971504},
                "probe_trigger_usd": -8.0,
                "distance_to_probe_trigger_usd": 18.971504,
                "sizing_gate_20_30z": {"all_criteria_met": False},
            },
        },
    )

    digest, text = build_digest(root)

    assert digest["pinned_tranche_economics"]["status"] == "TRIGGER_MET_PACKET_READY"
    assert digest["pinned_tranche_economics"]["resolved_pinned_fills"] == 11
    assert digest["pinned_tranche_economics"]["breakeven_win_rate_pct"] == 47.812345
    assert digest["pinned_tranche_economics"]["wilson_95_lower_bound_win_rate_pct"] == 43.210987
    assert digest["pinned_tranche_economics"]["sizing_gate_20_30z"] == {"all_criteria_met": False}
    assert "pinned_tranche_economics: status=TRIGGER_MET_PACKET_READY resolved=11" in text
    assert "breakeven=47.812345 wilson_lb=43.210987" in text


def test_state_digest_surfaces_pinned_tranche_midday_due_check(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-16T09:15Z fable DIRECTION [MEASURE]\n"
        "- next: mid-day pinned economics refresh at n>=35 or 14:30Z.\n"
    )
    _write_json(
        data / "wallet_copy_pinned_tranche_midday_due_check_latest.json",
        {
            "generated_at": "2026-07-16T09:31:00Z",
            "status": "PENDING_TRIGGER",
            "threshold_change_allowed": False,
            "inputs": {"scorecard_generated_at": "2026-07-16T09:30:17Z"},
            "trigger": {"resolved_pinned_fill_floor": 35},
            "summary": {
                "resolved_pinned_fills": 15,
                "pinned_filled_orders": 17,
                "pnl_usd": 10.336292,
                "win_rate_pct": 66.666667,
                "probe_trigger_usd": -8.0,
                "distance_to_probe_trigger_usd": 18.336292,
            },
        },
    )

    digest, text = build_digest(root)

    assert digest["pinned_tranche_midday_due_check"]["status"] == "PENDING_TRIGGER"
    assert digest["pinned_tranche_midday_due_check"]["trigger_n"] == 35
    assert digest["pinned_tranche_midday_due_check"]["resolved_pinned_fills"] == 15
    assert "pinned_tranche_midday_due_check: status=PENDING_TRIGGER trigger_n=35 resolved=15" in text


def test_state_digest_surfaces_maker_fallback_conversion(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-17T06:02Z Fable DIRECTION [LIVE/DEFEND/MEASURE]\n"
        "- NEXT PROFIT ACTION: report maker fallback conversion.\n"
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_2026-07-17.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "day_utc": "2026-07-17",
            "today": {"total": {"orders": 4, "fills": 2, "rejects": 2, "pnl_usd": 1.0}},
            "canonical_pnl_truth": {
                "by_day": {"2026-07-17": {"pnl_usd": 1.0}},
                "events": [
                    {"order_id": "filled-direct", "status": "FILLED"},
                    {"order_id": "filled-maker", "status": "FILLED"},
                    {"order_id": "maker-canceled", "status": "REJECTED"},
                    {"order_id": "raw-rejected", "status": "REJECTED"},
                ],
            },
            "execution_model_kpi": {
                "orders_per_submitted_window": 1.0,
                "orders_per_filled_window": 1.0,
                "fill_rate_pct": 50.0,
                "copy_model_counts": {"drip": 4},
                "drip": {"orders": 4, "fills": 2, "drip_stop_saves": 0},
            },
        },
    )
    (data / "wallet_copy_live_execution_events.jsonl").parent.mkdir(parents=True, exist_ok=True)
    (data / "wallet_copy_live_execution_events.jsonl").write_text(
        "\n".join(
            json.dumps(row)
            for row in [
                {
                    "event": "wallet_copy_live_lifecycle",
                    "ts": "2026-07-17T00:05:00+00:00",
                    "order_id": "filled-direct",
                    "status": "LIVE_FILLED",
                },
                {
                    "event": "wallet_copy_live_lifecycle",
                    "ts": "2026-07-17T00:10:00+00:00",
                    "order_id": "filled-maker",
                    "status": "LIVE_MAKER_FILLED",
                },
                {
                    "event": "wallet_copy_live_lifecycle",
                    "ts": "2026-07-17T00:15:00+00:00",
                    "order_id": "maker-canceled",
                    "status": "LIVE_MAKER_CANCELED",
                },
                {
                    "event": "wallet_copy_live_lifecycle",
                    "ts": "2026-07-17T00:20:00+00:00",
                    "order_id": "raw-rejected",
                    "status": "LIVE_REJECTED",
                },
                {
                    "event": "wallet_copy_live_lifecycle",
                    "ts": "2026-07-16T23:55:00+00:00",
                    "order_id": "old-maker-canceled",
                    "status": "LIVE_MAKER_CANCELED",
                },
            ]
        )
        + "\n"
    )

    digest, text = build_digest(root)
    conversion = digest["execution_model"]["maker_fallback_conversion"]

    assert conversion["day_utc"] == "2026-07-17"
    assert conversion["scorecard_filled_submissions"] == 2
    assert conversion["scorecard_rejected_submissions"] == 2
    assert conversion["maker_fallback_filled_before_cancel"] == 1
    assert conversion["maker_fallback_filled_before_cancel_scorecard_filled"] == 1
    assert conversion["maker_fallback_canceled_window_end_no_fill"] == 1
    assert conversion["maker_fallback_canceled_window_end_no_fill_scorecard_rejected"] == 1
    assert conversion["raw_rejected_or_unfilled"] == 1
    assert "maker_fallback_conversion: day=2026-07-17 canceled_window_end_no_fill=1" in text
    assert "filled_submissions=2 maker_filled_before_cancel=1 raw_rejected_or_unfilled=1" in text


def test_state_digest_surfaces_latest_mechanical_loss_demotion_and_cooloff(
    tmp_path: Path,
) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-26T14:21:34Z fable DIRECTION [LIVE/ROTATE]\n"
        "- next: demote the first canonical negative fill.\n"
    )
    wallet = "0x1015bb260154f51e5f432cb0a3227c1619fcbac8"
    fingerprint = "5d5a524301303bd4badd9410f002a3be6629587b2d29d4cb9f2ce529d4a79325"
    demotion = {
        "status": "APPLIED",
        "target_wallet": wallet,
        "wide_policy_fingerprint": fingerprint,
        "canonical_pnl_usd": -2.3,
        "resolution_winner": "DOWN",
        "generated_at": "2026-07-26T14:39:23Z",
    }
    _write_json(
        data / "wallet_copy_active_set_auto_degrade_state.json",
        {"members": [], "latest_mechanical_temporal_loss_demotion": demotion},
    )
    _write_json(
        data / "order_flow_deadman_state.json",
        {
            "policy_choke_rung_b_cooloffs": {
                f"{wallet}|{fingerprint}": {
                    "expires_at": "2026-07-27T14:39:23Z",
                    "reason": "mechanical_loss_demotion",
                },
            }
        },
    )

    digest, text = build_digest(root)

    assert digest["active_set"]["latest_mechanical_temporal_loss_demotion"] == demotion
    assert "mechanical_demotion=0x1015...bac8/APPLIED/pnl:-2.3/winner:DOWN" in text
    assert "cooloff=2026-07-27T14:39:23Z" in text


def test_state_digest_surfaces_scorecard_same_cut_basis_check(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-16T08:47Z fable DIRECTION [MEASURE]\n"
        "- next: one full-mode scorecard rerun.\n"
    )
    _write_json(
        data / "wallet_copy_scorecard_same_cut_basis_check_latest.json",
        {
            "generated_at": "2026-07-16T09:10:00Z",
            "status": "MATCH",
            "scorecard_generated_at": "2026-07-16T09:09:55Z",
            "json_totals": {"orders": 14, "fills": 12, "resolved_fills": 12, "rejects": 1, "pnl_usd": 8.521504},
            "text_totals": {"orders": 14, "fills": 12, "resolved_fills": 12, "rejects": 1, "pnl_usd": 8.521504},
            "count_match": True,
            "pnl_match": True,
        },
    )
    _write_json(
        data / "post_panic_integrity_audit_latest.json",
        {
            "generated_at": "2026-07-16T09:11:00Z",
            "status": "PASS",
            "checked_count": 9,
            "failure_count": 0,
            "repair_count": 0,
            "output": "data/research/post_panic_integrity_audit_latest.json",
        },
    )
    _write_json(
        data / "copy_event_triggered_cycle_scheduler_paper_lane_latest.json",
        {
            "generated_at": "2026-07-16T09:12:00Z",
            "status": "PAPER_CLOCK_POSITIVE_ACCRUING",
            "clock_start_utc": "2026-07-15T01:45:00Z",
            "clock_end_utc": "2026-07-17T01:45:00Z",
            "summary": {
                "paper_clock_rows_landed": 20612,
                "paper_clock_rows_resolved": 12595,
                "paper_clock_post_fee_would_pnl_usd": 96.84868,
            },
        },
    )
    _write_json(
        data / "copy_event_triggered_cycle_scheduler_verdict_latest.json",
        {
            "generated_at": "2026-07-17T12:40:00Z",
            "summary": {
                "verdict": "PASS_PREAUTHORIZED_LIVE_PROMOTION",
                "gate_pass": True,
                "gate_pnl_usd": 96.84868,
                "rows_landed": 20612,
                "rows_resolved": 12595,
                "resolved_windows": 324,
                "invariants_clean": True,
            },
            "gate": {"summary_matches_accumulator": True},
            "clock": {"clock_complete": True},
            "invariants": {"clean": True},
            "promotion": {"pre_authorized_by_ruling21b": True},
        },
    )

    digest, text = build_digest(root)

    assert digest["scorecard_same_cut_basis_check"]["status"] == "MATCH"
    assert digest["scorecard_same_cut_basis_check"]["count_match"] is True
    assert digest["post_panic_integrity"]["status"] == "PASS"
    assert digest["post_panic_integrity"]["checked_count"] == 9
    assert digest["scheduler_ratchet"]["path"] == "data/research/copy_event_triggered_cycle_scheduler_paper_lane_latest.json"
    assert digest["scheduler_ratchet"]["paper_clock_rows_landed"] == 20612
    assert digest["scheduler_verdict"]["summary"]["verdict"] == "PASS_PREAUTHORIZED_LIVE_PROMOTION"
    assert digest["scheduler_verdict"]["summary"]["gate_pass"] is True
    assert "scorecard_same_cut_basis: status=MATCH" in text
    assert "post_panic=PASS/9/fail0/repair0" in text
    assert "scheduler_rows=20612" in text
    assert "scheduler_verdict=PASS_PREAUTHORIZED_LIVE_PROMOTION gate_pnl=96.84868 gate_pass=True" in text


def test_scorecard_automation_drift_summary_lists_pointer_statuses() -> None:
    summary = update_state_digest._scorecard_automation_drift_summary(
        {
            "automation_drift": [
                {"kind": "local_runner", "name": "codex_heartbeat.sh", "pointer_status": "POINTER"},
                {"kind": "launchd", "name": "bad.plist", "pointer_status": "CONTENT_DEFECT"},
            ]
        }
    )

    assert summary["entries"] == 2
    assert summary["content_defects"] == 1
    assert summary["status_counts"] == {"CONTENT_DEFECT": 1, "POINTER": 1}
    assert summary["items"][0]["name"] == "codex_heartbeat.sh"


def test_state_digest_renders_agy_fallback_smoke(tmp_path: Path) -> None:
    root = tmp_path
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-13T02:04Z fable DIRECTION [SELF-DEV]\n"
        "- next: wire agy smoke evidence.\n"
    )
    _write_json(
        root / "data" / "research" / "agy_fallback_smoke_latest.json",
        {
            "result": "PARTIAL_PASS_WITH_AGY_QUOTA_DEFECT",
            "tested_at": "2026-07-13T02:15:40Z",
            "fallback_provider": "agy_then_codex_gpt",
            "initial_agy_answer_proof": {
                "provider_log_basename": "agy_rc0.log",
            },
            "post_pin_substitute_attempts": [
                {"provider": "agy", "mode": "SUBSTITUTE-FABLE", "rc": 1},
            ],
            "next_action": "ask Fable for ruling",
        },
    )
    _write_json(
        root / "data" / "research" / "agy_quota_state.json",
        {
            "status": "DEGRADED_QUOTA",
            "degraded_until": "2026-07-20T02:00:00Z",
            "observed_at": "2026-07-13T02:15:40Z",
        },
    )

    digest, text = build_digest(root)

    assert digest["agy_fallback_smoke"]["result"] == "PARTIAL_PASS_WITH_AGY_QUOTA_DEFECT"
    assert "agy_smoke: result=PARTIAL_PASS_WITH_AGY_QUOTA_DEFECT" in text
    assert "post_pin=[1]" in text
    assert "agy_quota: status=DEGRADED_QUOTA degraded_until=2026-07-20T02:00:00Z" in text


def test_state_digest_surfaces_a689_postfix_live_one_liner(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-16T00:10Z fable DIRECTION [LIVE/DEFEND/MEASURE]\n"
        "- next: keep a689 post-fix instrumentation as one line.\n"
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "window_participation": {
                "rows": [
                    {
                        "source_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
                        "market_slug": "btc-updown-5m-1784153700",
                        "latest_observed_ts": 1784153700.0,
                        "wallet_eligible_orders": 99,
                        "our_submits": 0,
                        "our_fills": 0,
                        "dominant_skip_reason": "old_row_ignored",
                        "participation_skip_category": "OLD",
                    },
                    {
                        "source_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
                        "market_slug": "btc-updown-5m-1784160300",
                        "window_start_s": 1784160300.0,
                        "latest_observed_ts": 1784160497.0,
                        "wallet_eligible_orders": 4,
                        "our_submits": 0,
                        "our_fills": 0,
                        "dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
                        "participation_skip_category": "FLOOR_BLOCKED_MISS",
                    },
                    {
                        "source_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
                        "market_slug": "btc-updown-5m-1784161800",
                        "window_start_s": 1784161800.0,
                        "latest_observed_ts": 1784162033.0,
                        "wallet_eligible_orders": 5,
                        "our_submits": 0,
                        "our_fills": 0,
                        "dominant_skip_reason": "inventory_late_window_guard",
                        "participation_skip_category": "CORRECT_SKIP",
                    },
                ]
            }
        },
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "orders": [
                {
                    "source_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
                    "updated_at": "2026-07-16T00:10:00Z",
                    "final_status": "FILLED",
                },
                {
                    "source_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
                    "updated_at": "2026-07-15T12:00:00Z",
                    "final_status": "FILLED",
                },
            ]
        },
    )
    _write_json(
        data / "a689_0200_tripwire_latest.json",
        {
            "generated_at": "2026-07-16T02:03:38Z",
            "verdict": "CONFIG_LEAK_DEFECT_POLICY_MAX_1",
            "pre_ruled_action": "HOLD_DRIP_MIN_CHANGE_AND_ASK_FABLE",
            "rows": 30,
            "windows": 25,
            "accepted_order_rows": 0,
            "tripwire_class_counts": {"FLOOR_BUDGET_BIND": 14, "PIPELINE_LATE": 9},
            "dominant_skip_reason_counts": {"drip_min_tranche_exceeds_window_budget": 14},
            "floor_budget_bind": {"policy_max_1_rows": 14},
        },
    )
    _write_json(
        data / "f418_readmission_packet_latest.json",
        {
            "generated_at": "2026-07-16T05:40:00Z",
            "status": "PRE_RULED_ADMIT_F418_ACTIVATION",
            "decision": "ADMIT_UNDER_FABLE_20260716T0528_BRANCH_A",
            "source_wallet": "0xf418d3a1a941292f9c8707d62a14980c5beb95a3",
            "candidate_id": "cohort_alive_admit_f418d3a1",
            "counterfactual_gate_since_non_admissible_boundary": {
                "gate_pass": True,
                "basis": "routing_shadow_member_attribution",
                "routing_shadow_member": {
                    "measurable_resolved_intents": 15,
                    "post_fee_pnl_usd": 4.544406,
                },
            },
            "sizing_if_fable_allows_activation": {
                "max_order_usd": 1.0,
                "max_price": 0.5,
                "min_order_usd": 1.0,
            },
            "next_action": "Apply the pre-ruled branch (a) activation and restart the single guard",
        },
    )

    digest, text = build_digest(root)

    postfix = digest["a689_live_postfix"]
    assert postfix["rows"] == 2
    assert postfix["windows"] == 2
    assert postfix["wallet_eligible_orders"] == 9
    assert postfix["our_submits"] == 0
    assert postfix["accepted_order_rows"] == 1
    assert postfix["category_counts"] == {"CORRECT_SKIP": 1, "FLOOR_BLOCKED_MISS": 1}
    assert digest["a689_0200_tripwire"]["verdict"] == "CONFIG_LEAK_DEFECT_POLICY_MAX_1"
    assert digest["a689_0200_tripwire"]["floor_budget_bind"]["policy_max_1_rows"] == 14
    assert digest["f418_readmission_packet"]["status"] == "PRE_RULED_ADMIT_F418_ACTIVATION"
    assert (
        digest["f418_readmission_packet"]["counterfactual"]["routing_shadow_member"]["measurable_resolved_intents"]
        == 15
    )
    assert "a689_postfix_rows=2/windows=2/accepted=1" in text
    assert "a689_0200_verdict=CONFIG_LEAK_DEFECT_POLICY_MAX_1" in text
    assert "a689_0200_policy_max_1=14" in text
    assert "f418_readmit=PRE_RULED_ADMIT_F418_ACTIVATION" in text
    assert "f418_basis=routing_shadow_member_attribution" in text
    assert "f418_n=15" in text
    assert "f418_post_fee=4.544406" in text


def test_state_digest_surfaces_fresh_flow_probe_counters(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-13T23:40Z fable DIRECTION [PROMOTE]\n"
        "- next: refresh remaining unknown liveness.\n"
    )
    _write_json(
        data / "queue_remote_dataapi_fresh_flow_probe_latest.json",
        {
            "generated_at": "2026-07-13T23:52:53Z",
            "summary": {
                "wallets": 42,
                "pass_admission_threshold": 42,
                "error_wallets": 0,
                "cumulative_wallets": 92,
                "cumulative_pass_admission_threshold": 92,
                "cumulative_error_wallets": 0,
            },
            "rows": [{"wallet": "0x1"}, {"wallet": "0x2"}],
        },
    )

    digest, text = build_digest(tmp_path)

    probe = digest["gates"]["fresh_flow_probe"]
    assert probe["selected_wallets"] == 42
    assert probe["selected_pass_admission_threshold"] == 42
    assert probe["selected_error_wallets"] == 0
    assert probe["cumulative_wallets"] == 92
    assert probe["cumulative_pass_admission_threshold"] == 92
    assert probe["cumulative_error_wallets"] == 0
    assert probe["rows"] == 2
    assert "fresh_flow_probe: generated_at=2026-07-13T23:52:53Z selected=42/42" in text
    assert "cumulative=92/92 cumulative_errors=0 rows=2" in text


def test_recent_live_fills_preserves_corner_inputs() -> None:
    from scripts.update_state_digest import _recent_live_fills

    live = {
        "orders": [
            {"final_status": "REJECTED", "intent_id": "skip"},
            {
                "final_status": "FILLED",
                "intent_id": "ci_recent",
                "market_slug": "btc-updown-5m-123",
                "outcome": "Up",
                "updated_at": "2026-07-22T18:05:49Z",
                "expected_vs_realized_fee": {"response_expected_fee_usd": 0.043091},
                "lifecycle": [
                    {
                        "status": "LIVE_FILLED",
                        "payload": {
                            "details": {"makingAmount": "1.079999", "takingAmount": "2.511626"},
                            "response_fill_price": 0.43,
                        },
                    }
                ],
            },
        ]
    }

    assert _recent_live_fills(live) == [
        {
            "intent_id": "ci_recent",
            "market_slug": "btc-updown-5m-123",
            "outcome": "Up",
            "updated_at": "2026-07-22T18:05:49Z",
            "making_amount": "1.079999",
            "taking_amount": "2.511626",
            "response_fill_price": 0.43,
            "response_expected_fee_usd": 0.043091,
        }
    ]


def test_state_digest_surfaces_market_mining_cadence(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-14T17:31Z fable DIRECTION [DISCOVER]\n"
        "- next: wire continuous mining cadence.\n"
    )
    _write_json(
        data / "wallet_market_mining_cadence_state.json",
        {
            "status": "NULL_CYCLE_SHIPPED_SCOPE_WIDENED",
            "generated_at": "2026-07-14T17:40:00Z",
            "due_steps": ["intake", "replay"],
            "ran_steps": ["intake", "replay"],
            "observed": {
                "intake_active_wallets": 963,
                "cohort_size": 963,
                "shadow_positive": 456,
                "live_ready_picks": 435,
                "packet_count": 25,
                "external_liveness_pass": 92,
                "source_active_policy_pass": 18,
            },
            "null_cycle": {
                "status": "SHIPPED",
                "scope_widened": True,
            },
            "next_scope": {
                "intake_max_pages": 30,
                "replay_wallet_limit": 15,
            },
        },
    )
    _write_json(
        data / "wallet_market_scan_ranked.json",
        {
            "window": {
                "lookback_complete": False,
                "oldest_trade_iso_seen": "2026-07-14T17:28:32Z",
                "newest_trade_iso_seen": "2026-07-14T17:40:00Z",
            },
            "rate_limit_budget": {
                "exhaustion_class": "SHORT_PAGE_TRUNCATION",
                "short_page_truncation": {
                    "class": "SHORT_PAGE_TRUNCATION",
                    "offset": 10500,
                },
            },
        },
    )

    digest, text = build_digest(tmp_path)

    mining = digest["market_mining_cadence"]
    assert mining["status"] == "NULL_CYCLE_SHIPPED_SCOPE_WIDENED"
    assert mining["observed"]["live_ready_picks"] == 435
    assert mining["intake_window"]["lookback_complete"] is False
    assert mining["intake_window"]["oldest_trade_iso_seen"] == "2026-07-14T17:28:32Z"
    assert mining["intake_exhaustion"]["class"] == "SHORT_PAGE_TRUNCATION"
    assert "market_mining_cadence: status=NULL_CYCLE_SHIPPED_SCOPE_WIDENED" in text
    assert "lookback_complete=False" in text
    assert "exhaustion=SHORT_PAGE_TRUNCATION" in text
    assert "live_ready=435 packets=25" in text
    assert "null=SHIPPED widened=True" in text


def test_state_digest_surfaces_leaderboard_pipeline_heartbeat(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-21T06:06Z fable DIRECTION [SELF-DEV]\n- next: bounded pipeline heartbeat.\n"
    )
    _write_json(
        data / "wallet_copy_leaderboard_scan_state.json",
        {"updated_at": "2026-07-21T06:20:37Z", "status": "PASS", "command": {"returncode": 0, "duration_s": 100.65}},
    )
    _write_json(
        data / "wallet_copy_leaderboard_crypto_state.json",
        {
            "pipeline_requested": True,
            "pipeline_roster_wallets": 4,
            "summary": {"unique_wallets": 28023, "copy_all_fetched_wallets_to_registry": True},
            "command_results": [{
                "name": "history_and_paper",
                "returncode": 0,
                "duration_s": 6.34,
                "stdout_json": {
                    "data_api_ingest_status": "PASS",
                    "data_api_skip_count": 0,
                    "data_api_timeout": {"connect_s": 10.0, "read_s": 30.0, "retries": 2},
                },
            }],
        },
    )

    digest, text = build_digest(tmp_path)

    heartbeat = digest["leaderboard_pipeline_heartbeat"]
    assert heartbeat["scan_returncode"] == 0
    assert heartbeat["pipeline_roster_wallets"] == 4
    assert heartbeat["data_api_ingest_status"] == "PASS"
    assert heartbeat["data_api_timeout"] == {"connect_s": 10.0, "read_s": 30.0, "retries": 2}
    assert "leaderboard_pipeline_heartbeat: generated_at=2026-07-21T06:20:37Z scan=PASS/rc=0" in text
    assert "registered=28023/all=True roster=4" in text


def test_state_digest_surfaces_direct_four_way_admission_gate(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-20T12:08Z fable DIRECTION [LEARN/PROMOTE]\n"
        "- next: audit any admission gate crossing.\n"
    )
    _write_json(
        data / "cohort_alive_admission_packets_latest.json",
        {
            "generated_at": "2026-07-20T12:27:34Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {
                "four_way_admission_ready": 1,
                "top_four_way_wallet": "0xee888fa7b96007f7fa270988e92bddb0ae19ed10",
            },
            "top_four_way_candidate": {
                "candidate_id": "market_cohort_alive_ddb0ae19ed10",
                "history_completeness": "complete",
                "resolved_copyable_events": 495,
                "paper_pnl_usd": 62.781846,
                "roi_pct": 6.214365,
            },
        },
    )
    _write_json(
        tmp_path / "configs" / "wallet_copy" / "registry_observation_admissions.json",
        {
            "wallets": [
                "0xee888fa7b96007f7fa270988e92bddb0ae19ed10",
                "0x1111111111111111111111111111111111111111",
            ],
            "runtime_observation_top_n": 2,
        },
    )
    _write_json(
        data / "wallet_copy_watch_tier_poller_state.json",
        {
            "generated_at": "2026-07-20T12:28:00Z",
            "source_wallets": [
                "0xee888fa7b96007f7fa270988e92bddb0ae19ed10",
                "0x2222222222222222222222222222222222222222",
            ],
            "summary": {"fresh_poll_only_by_wallet": {}},
        },
    )

    digest, text = build_digest(tmp_path)

    admission = digest["cohort_admission"]
    assert admission["summary"]["four_way_admission_ready"] == 1
    assert admission["top_four_way_candidate"]["resolved_copyable_events"] == 495
    assert admission["admitted_watch_tier_polled_count"] == 1
    assert admission["watch_tier_source_wallet_cohorts"] == [
        {
            "wallet": "0xee888fa7b96007f7fa270988e92bddb0ae19ed10",
            "cohort": "registry_observation",
        },
        {
            "wallet": "0x2222222222222222222222222222222222222222",
            "cohort": "legacy_watch_tier",
        },
    ]
    assert "cohort_admission: generated_at=2026-07-20T12:27:34Z four_way_ready=1" in text
    assert "candidate=market_cohort_alive_ddb0ae19ed10 history=complete" in text
    assert "admitted_watch_polled=1" in text


def test_state_digest_surfaces_weekday_readmission_digits(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-20T12:41Z fable DIRECTION [PROMOTE_PREP]\n"
        "- next: produce df2c/d918 readmission digits.\n"
    )
    _write_json(
        data / "weekday_readmission_status_latest.json",
        {
            "generated_at": "2026-07-20T12:44:54Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {
                "decision": "NEITHER_QUALIFIES_ON_CURRENT_EVIDENCE",
                "clock_pass": 1,
                "readmission_gate_pass": 0,
                "queued_for_normal_admission": 0,
            },
            "wallets": [
                {
                    "source_wallet": "0xdf2c0702fc00be90bd795234a86e28f1ed39118a",
                    "demotion_clock_elapsed_h": 82.77,
                    "fresh_watch_tier": {"resolved_signals": 0, "resolved_gap": 30, "roi_pct": 0.0},
                    "fresh_external_liveness": {"btc5m_trades_24h": 0},
                    "verdict": "READMISSION_GATE_FAIL_NO_FRESH_SAMPLE_OR_LIVENESS",
                }
            ],
        },
    )

    digest, text = build_digest(tmp_path)

    readmission = digest["weekday_readmission_status"]
    assert readmission["summary"]["readmission_gate_pass"] == 0
    assert readmission["wallets"][0]["demotion_clock_elapsed_h"] == 82.77
    assert "weekday_readmission_status: generated_at=2026-07-20T12:44:54Z" in text
    assert "decision=NEITHER_QUALIFIES_ON_CURRENT_EVIDENCE" in text


def test_state_digest_surfaces_factory_funnel(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-14T17:38Z fable DIRECTION [SELF-DEV]\n"
        "- next: build factory funnel.\n"
    )
    _write_json(
        data / "factory_funnel_latest.json",
        {
            "generated_at": "2026-07-14T17:50:00Z",
            "day_utc": "2026-07-14",
            "counts": {
                "market_population": 1253,
                "mined_actives": 1137,
                "scored": 973,
                "shadow_positive": 460,
                "live_ready": 439,
                "admitted": 10,
                "armed_runtime_loaded": 10,
                "submitted_live_orders_today": 97,
                "filled_live_orders_today": 74,
                "profitable_day": 0,
            },
            "enemy_line": {
                "status": "RED",
                "link_id": "live_ready_to_admitted",
            },
        },
    )

    digest, text = build_digest(tmp_path)

    funnel = digest["factory_funnel"]
    assert funnel["enemy_line"]["link_id"] == "live_ready_to_admitted"
    assert funnel["counts"]["live_ready"] == 439
    assert "factory_funnel: generated_at=2026-07-14T17:50:00Z enemy=RED" in text
    assert "enemy_link=live_ready_to_admitted" in text
    assert "topological_first=live_ready_to_admitted" in text
    assert "live_ready=439 admitted=10 armed=10" in text


def test_state_digest_surfaces_f418_size_clamp_fee_leak_shadow(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-23T15:52Z fable DIRECTION [OBSERVE/LEARN]\n"
        "- next: measure f418 size-clamp fee leak.\n"
    )
    _write_json(
        data / "f418_size_clamp_fee_leak_shadow_latest.json",
        {
            "generated_at": "2026-07-23T16:13:03Z",
            "status": "FAIL_NO_SIGNIFICANT_MICRO_FEE_LEAK_PRIMARY_DRAG",
            "micro_1usd": {"n": 307, "post_fee_ev_per_fill_usd": -0.020526},
            "standing_2_to_2_5usd": {"n": 45, "post_fee_ev_per_fill_usd": 0.534993},
            "comparison": {
                "micro_minus_standing_ev_usd": -0.555519,
                "ci95_low_usd": -1.332564,
                "ci95_high_usd": 0.221526,
                "expected_fee_share_delta_pp": 3.177408,
            },
            "decision": "REPORT_ONLY_NO_LIVE_SIZE_OR_POLICY_CHANGE",
            "live_mutation": False,
        },
    )

    digest, text = build_digest(tmp_path)

    shadow = digest["f418_size_clamp_fee_leak_shadow"]
    assert shadow["micro_1usd"]["n"] == 307
    assert shadow["comparison"]["ci95_high_usd"] == 0.221526
    assert "f418_size_fee_leak=FAIL_NO_SIGNIFICANT_MICRO_FEE_LEAK_PRIMARY_DRAG" in text
    assert "micro=307/-0.020526" in text
    assert "standing=45/0.534993" in text
    assert "fee_delta_pp=3.177408" in text


def test_state_digest_surfaces_member_native_policy_uplift_shadow(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-23T16:57Z fable DIRECTION [OBSERVE/LEARN]\n"
        "- next: run member-native policy uplift.\n"
    )
    _write_json(
        data / "member_native_policy_acceptance_uplift_shadow_latest.json",
        {
            "generated_at": "2026-07-23T17:11:25Z",
            "verdict": "ACCRUE_MEMBER_NATIVE_CLOSED_WINDOWS",
            "sample_started_at": "2026-07-23T17:00:00Z",
            "member_count": 7,
            "frozen_policy_binding_count": 7,
            "incremental": {
                "resolved_windows": 0,
                "positive_windows": 0,
                "post_fee_pnl_usd": 0.0,
            },
            "incumbent_twin": {"resolved_windows": 1, "post_fee_pnl_usd": -1.0},
            "gate": {"minimum_incremental_resolved_windows": 20, "pass": False},
            "single_submitter_preserved": True,
            "live_mutation": False,
        },
    )

    digest, text = build_digest(tmp_path)

    shadow = digest["member_native_policy_acceptance_uplift_shadow"]
    assert shadow["member_count"] == 7
    assert shadow["gate"]["pass"] is False
    assert shadow["persistent_runner"]["fresh"] is False
    assert (
        shadow["persistent_runner"]["deadman_status"]
        == "OPEN_DEFECT_RESTART_OR_REFRESH_RUNNER"
    )
    assert "member_native_uplift=ACCRUE_MEMBER_NATIVE_CLOSED_WINDOWS" in text
    assert "members=7/7" in text
    assert "single_submitter=True" in text


def test_state_digest_verifies_atomic_a689_82c8_shadow_cut(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-23T18:22Z fable DIRECTION [ROTATE/PROMOTE]\n"
        "- next: verify exact shadow cut.\n"
    )
    successor = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    _write_json(
        data / "a689_82c8_ready_shadow_cut_spec_latest.json",
        {
            "generated_at": "2026-07-23T18:22:27Z",
            "execution_status": "EXECUTED",
            "decision": "TERMINATE_A689_EMPTY_AND_BIND_82C8",
            "cut_at": "2026-07-23T18:22:07.136412Z",
        },
    )
    _write_json(
        data / "wallet_copy_ready_shadow_lanes_state.json",
        {
            "a689_82c8_cut": {"status": "EXECUTED_ATOMIC_STATE_REBIND"},
            "lanes": [
                {
                    "wallet": successor,
                    "source_binding": "FABLE_20260723_82C8_READY_SHADOW",
                    "source_binding_status": "WIRED",
                    "standby_evidence_started_at": "2026-07-23T18:22:27Z",
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            ],
        },
    )
    _write_json(
        data / "hot_standby_source_liveness_latest.json",
        {
            "rows": [
                {
                    "wallet": successor,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "user_only_hot_path_supported": True,
                        "last_trade_age_h": 0.001,
                        "last_trade_iso": "2026-07-23T18:22:34Z",
                    },
                }
            ]
        },
    )
    _write_json(
        data / "82c8_terminal_decision_latest.json",
        {
            "generated_at": "2026-07-25T15:45:00Z",
            "status": "NOT_DUE",
            "decision_at": "2026-07-25T18:32:33.543905Z",
            "due": False,
            "required_hours": 48,
            "evidence": {"resolved": 0, "required_resolved": 30},
            "checks": {"resolved_gte_30": False},
            "paper_only": True,
            "live_orders_allowed": False,
        },
    )

    digest, text = build_digest(tmp_path)
    cut = digest["a689_82c8_ready_shadow_cut"]
    assert cut["atomic_verification_pass"] is True
    assert cut["a689_terminal_absent_from_lanes"] is True
    assert cut["successor_bound"] is True
    assert cut["successor_liveness"]["recommended_query_key"] == "user"
    terminal = digest["terminal_82c8_decision"]
    assert terminal["decision"] == "NOT_DUE"
    assert terminal["clock"]["decision_at"] == "2026-07-25T18:32:33.543905Z"
    assert terminal["evidence"]["resolved"] == 0
    assert terminal["failed_gates"] == ["resolved_gte_30"]


def test_state_digest_surfaces_commitment_overdue_summary(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-14T06:48Z fable DIRECTION [SELF-DEV]\n"
        "- next: surface overdue commitments.\n"
        "evidence delivered for another row\n"
    )
    (data / "commitments.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "id": "missing_contract",
                        "due_ts": "2000-01-01T00:00:00Z",
                        "evidence_pattern": "docs/agents/HANDOFF.md:.*not-present",
                        "status": "OPEN",
                    }
                ),
                json.dumps(
                    {
                        "id": "evidence_exists_but_unmarked",
                        "due_ts": "2000-01-02T00:00:00Z",
                        "evidence_pattern": "docs/agents/HANDOFF.md:.*evidence delivered",
                        "status": "OPEN",
                    }
                ),
                json.dumps(
                    {
                        "id": "merged_contract",
                        "due_ts": "2000-01-01T00:00:00Z",
                        "evidence_pattern": "docs/agents/HANDOFF.md:.*not-present",
                        "status": "MERGED",
                    }
                ),
            ]
        )
        + "\n"
    )

    digest, text = build_digest(tmp_path)

    assert digest["commitments_overdue"]["overdue"] == 2
    assert digest["commitments_overdue"]["oldest_id"] == "missing_contract"
    assert digest["commitments_overdue"]["evidence_unmarked"] == 1
    assert digest["commitments_overdue"]["overdue_with_evidence"] == 1
    assert digest["commitments_overdue"]["sample_ids"] == [
        "missing_contract",
        "evidence_exists_but_unmarked",
    ]
    assert "commitments_overdue={count:2,oldest_id:missing_contract}" in text
    assert "overdue_with_evidence=1" in text


def test_state_digest_surfaces_benign_skip_deadman_warning(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-14T09:33Z fable DIRECTION [SELF-DEV]\n"
        "- next: surface benign skip override warning.\n"
    )
    _write_json(
        data / "order_flow_deadman_state.json",
        {
            "status": "MEASURED_SOURCE_QUIET",
            "deadman_class": "HOST_DOWNTIME_RESTART",
            "can_trade": True,
            "can_trade_reason": "guard_top_level_live_no_blockers",
            "effective_deadman_idle_s": 145.0,
            "host_downtime_attribution": {
                "status": "HOST_DOWNTIME_RESTART",
                "host_boot_time": "2026-07-21T17:18:24+00:00",
                "post_boot_recovery_liveness_ts": "2026-07-21T17:28:38+00:00",
            },
            "deadman_warning": "UNSUBMITTABLE_MEMBER_UNDER_BENIGN_SKIP",
            "benign_skip_overrode": ["member_unsubmittable"],
            "guard_side_halt_signal": {
                "active": False,
                "warning": "UNSUBMITTABLE_MEMBER_UNDER_BENIGN_SKIP",
                "benign_skip_overrode": ["member_unsubmittable"],
            },
        },
    )

    digest, text = build_digest(tmp_path)

    deadman = digest["order_flow_deadman"]
    assert deadman["deadman_warning"] == "UNSUBMITTABLE_MEMBER_UNDER_BENIGN_SKIP"
    assert deadman["benign_skip_overrode"] == ["member_unsubmittable"]
    assert deadman["guard_side_halt_warning"] == "UNSUBMITTABLE_MEMBER_UNDER_BENIGN_SKIP"
    assert deadman["guard_side_halt_benign_skip_overrode"] == ["member_unsubmittable"]
    assert deadman["deadman_class"] == "HOST_DOWNTIME_RESTART"
    assert deadman["can_trade_reason"] == "guard_top_level_live_no_blockers"
    assert deadman["effective_deadman_idle_s"] == 145.0
    assert deadman["host_downtime_attribution"]["host_boot_time"] == "2026-07-21T17:18:24+00:00"
    assert "warning=UNSUBMITTABLE_MEMBER_UNDER_BENIGN_SKIP" in text
    assert "class=HOST_DOWNTIME_RESTART" in text
    assert "can_trade_reason=guard_top_level_live_no_blockers" in text
    assert "effective_idle_s=145.0" in text
    assert "benign_skip_overrode=['member_unsubmittable']" in text


def test_state_digest_surfaces_current_guard_memory_not_historical_sample(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-18T18:59Z fable DIRECTION [SELF-DEV/DEFEND]\n"
        "- next: report canonical guard memory rss.\n"
    )
    _write_json(
        data / "order_flow_deadman_state.json",
        {
            "status": "MEASURED_NONSELECTED_DEMAND",
            "can_trade": True,
            "liveness_source": "accepted_order",
            "latest_order_ts": "2026-07-18T18:20:24.899607+00:00",
            "guard_memory": {
                "status": "OK",
                "pid": 41723,
                "rss_gib": 12.276138,
                "rss_kib": 12872464,
                "memory_probe": {"raw_ps_line": "12872464", "verified_pid": 41723},
                "trend_gib": 8.577789,
                "trend_window_s": 162.303627,
                "warn_gib": 28.0,
                "restart_gib": 32.0,
                "auto_restart": {"status": "NOT_APPLICABLE"},
                "samples": [
                    {
                        "checked_at": "2026-07-18T18:51:12.026786+00:00",
                        "pid": 41723,
                        "rss_gib": 3.698349,
                        "rss_kib": 3878000,
                    },
                    {
                        "checked_at": "2026-07-18T18:53:54.330413+00:00",
                        "pid": 41723,
                        "rss_gib": 12.276138,
                        "rss_kib": 12872464,
                    },
                ],
            },
        },
    )

    digest, text = build_digest(tmp_path)

    guard_memory = digest["order_flow_deadman"]["guard_memory"]
    assert guard_memory["rss_gib"] == 12.276138
    assert guard_memory["rss_kib"] == 12872464
    assert guard_memory["raw_ps_line"] == "12872464"
    assert guard_memory["verified_pid"] == 41723
    assert guard_memory["sample_count"] == 2
    assert guard_memory["samples_are_history_not_authority"] is True
    assert (
        "guard_memory=status:OK,rss_gib:12.276138,rss_kib:12872464,"
        "raw_ps_line:12872464,verified_pid:41723,pid:41723"
    ) in text
    assert (
        "source:data/research/order_flow_deadman_state.json."
        "guard_memory.threshold_rss_gib"
    ) in text
    assert "rss_gib:3.698349" not in text


def test_state_digest_labels_deadman_lag_with_own_budget_and_status(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "research"
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text("")
    checked_at = (
        datetime.now(UTC) - update_state_digest.timedelta(seconds=90)
    ).isoformat()
    _write_json(
        data / "order_flow_deadman_state.json",
        {
            "status": "INCIDENT_ORDER_FLOW_DEAD",
            "checked_at": checked_at,
            "can_trade": False,
        },
    )

    digest, text = build_digest(tmp_path)

    deadman = digest["order_flow_deadman"]
    assert deadman["digest_lag_s"] >= 89.0
    assert deadman["digest_lag_budget_s"] == 60.0
    assert deadman["digest_lag_status"] == "STALE"
    assert f"digest_lag_s={deadman['digest_lag_s']}" in text
    assert "digest_lag_budget_s=60.0" in text
    assert "digest_lag_status=STALE" in text


def test_state_digest_surfaces_e6db_loser_autopsy(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-14T09:45Z fable DIRECTION [LIVE/DEFEND]\n"
        "- next: e6db loser autopsy.\n"
    )
    _write_json(
        data / "e6db_loser_autopsy_latest.json",
        {
            "generated_at": "2026-07-14T09:52:00Z",
            "summary": {
                "day_utc": "2026-07-14",
                "classification": "structural_negative_edge",
                "fills": 50,
                "realized_pnl_usd": -13.760096,
                "actual_win_rate_pct": 44.0,
                "required_win_rate_at_payoff_shape_pct": 49.701238,
                "actual_minus_required_win_rate_pp": -5.7012,
                "gap_sigma_pp": 7.070645,
                "gap_in_sigma": -0.80633,
                "avg_win_per_winner_usd": 2.427949,
                "avg_loss_per_loser_abs_usd": 2.399106,
                "expected_fee_usd": 4.282566,
                "expected_fee_share_of_gross_pnl_swing_pct": 3.551348,
                "diagnostic_pre_expected_fee_pnl_usd": -9.47753,
            },
        },
    )

    digest, text = build_digest(tmp_path)

    summary = digest["e6db_loser_autopsy"]["summary"]
    assert summary["classification"] == "structural_negative_edge"
    assert summary["fills"] == 50
    assert digest["e6db_loser_autopsy"]["path"] == "data/research/e6db_loser_autopsy_latest.json"
    assert "e6db_autopsy: day=2026-07-14 classification=structural_negative_edge" in text
    assert "win_rate=44.0 required=49.701238 gap_pp=-5.7012" in text
    assert "sigma_pp=7.070645 gap_sigma=-0.80633" in text


def test_state_digest_surfaces_successor_dossier_on_queue_line(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-14T10:03Z fable DIRECTION [LIVE/DEFEND]\n"
        "- next: pre-stage successor dossier.\n"
    )
    _write_json(
        data / "successor_dossier_latest.json",
        {
            "generated_at": "2026-07-14T10:08:00Z",
            "summary": {
                "status": "PRESTAGED_NO_LIVE_CHANGE",
                "candidate_wallet": "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1",
                "queue_rank": 1,
                "routing_status": "NOT_SEATED_IN_ROUTING_SHADOW_RETAINED",
                "routing_measured_windows": None,
                "routing_post_fee_pnl_usd": None,
                "routing_would_fill_count": None,
                "fee_coverage_status": "NO_MEMBER_FEE_CALIBRATION_ROW",
                "gap_sigma_status": "NOT_COMPUTABLE_MISSING_BREAKEVEN_PAYOFF_SHAPE",
                "gap_in_sigma": None,
                "live_change": False,
            },
            "routing_shadow": {
                "validation_elapsed_hours": 77.470259,
                "overall_would_submit_windows": 528,
            },
            "corrected_probe": {"btc5m_buys": 493},
            "temporal_profile": {"classification": "FADING"},
            "gap_sigma": {"status": "NOT_COMPUTABLE_MISSING_BREAKEVEN_PAYOFF_SHAPE"},
        },
    )

    digest, text = build_digest(tmp_path)

    summary = digest["successor_dossier"]["summary"]
    assert summary["candidate_wallet"] == "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"
    assert digest["successor_dossier"]["path"] == "data/research/successor_dossier_latest.json"
    assert digest["successor_dossier"]["routing_shadow"]["overall_would_submit_windows"] == 528
    assert digest["successor_dossier"]["corrected_probe"]["btc5m_buys"] == 493
    assert digest["successor_dossier"]["temporal_profile"]["classification"] == "FADING"
    assert "queue: " in text
    assert "successor=0xc539...73d1/rank=1/status=PRESTAGED_NO_LIVE_CHANGE" in text
    assert "/routing=NOT_SEATED_IN_ROUTING_SHADOW_RETAINED/mw=None/post=None/would=None" in text
    assert "/fee=NO_MEMBER_FEE_CALIBRATION_ROW/sigma=NOT_COMPUTABLE_MISSING_BREAKEVEN_PAYOFF_SHAPE" in text
    assert "/live_change=False/path=data/research/successor_dossier_latest.json" in text


def test_state_digest_surfaces_active_set_rotation_packet_on_queue_line(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-18T03:24Z fable DIRECTION [LIVE/DEFEND/WEEKEND/ROTATE]\n"
        "- next: pre-stage rotation packet.\n"
    )
    _write_json(
        data / "active_set_rotation_packet_latest.json",
        {
            "generated_at": "2026-07-18T03:52:58Z",
            "status": "PRESTAGED_NO_LIVE_CHANGE",
            "presumptive_target": "0xf418d3a1a941292f9c8707d62a14980c5beb95a3",
            "presumptive_candidate_id": "runtime_auto_degrade_f418d3a1a9",
            "selected_wallet": "0x32de91fa203321fa7735e7854f2b1c844e71ce9d",
            "selected_premerge_new_matching_events": 0,
            "selected_latest_retained_matching_event_iso": "2026-07-18T03:31:28Z",
            "live_path_mutated": False,
            "quiet_clock": {
                "anchor_iso": "2026-07-18T02:38:01Z",
                "earliest_fire_iso": "2026-07-18T06:38:01Z",
                "fires_now": False,
            },
            "ranked_candidates": [
                {
                    "source_wallet": "0xf418d3a1a941292f9c8707d62a14980c5beb95a3",
                    "fresh_matching_events_4h": 824,
                    "fresh_matching_event_rate_per_hour": 206.0,
                    "local_entry_latency_p50_s": 0.585219,
                    "routing_shadow_post_fee_pnl_usd": -57.329311,
                }
            ],
        },
    )

    digest, text = build_digest(tmp_path)

    summary = digest["active_set_rotation_packet"]["summary"]
    assert summary["presumptive_target"] == "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
    assert summary["earliest_fire_iso"] == "2026-07-18T06:38:01Z"
    assert digest["active_set_rotation_packet"]["path"] == "data/research/active_set_rotation_packet_latest.json"
    assert "rotation_packet=PRESTAGED_NO_LIVE_CHANGE/target=0xf418...95a3" in text
    assert "/fresh4h=824/rate_h=206.0/lat_p50=0.585219/post=-57.329311" in text
    assert "/quiet_fire=2026-07-18T06:38:01Z/fires_now=False/live_change=False" in text


def test_weekly_verdict_from_scorecards_uses_closed_week_and_targets(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    for day, pnl, windows in [
        ("2026-07-06", 10.0, 10),
        ("2026-07-07", -2.5, 12),
        ("2026-07-08", 1.0, 8),
        ("2026-07-09", 0.5, 7),
        ("2026-07-10", 5.0, 15),
        ("2026-07-11", 2.0, 9),
    ]:
        _write_json(
            data / f"wallet_copy_daily_scorecard_{day}.json",
            {
                "kind": "wallet_copy_daily_scorecard",
                "day_utc": day,
                "day_pnl_basis": {"day_pnl_response_basis": pnl},
                "today": {"total": {"orders": 1}},
                "since_topup_truth": {"baseline_usd": 100.0},
                "volume_kpi": {"canonical_daily": {"windows_filled": windows}},
            },
        )

    verdict = update_state_digest._weekly_verdict_from_scorecards(
        data,
        today=update_state_digest.datetime(2026, 7, 12, tzinfo=update_state_digest.timezone.utc),
    )

    assert verdict["due"] is True
    assert verdict["week_start_utc"] == "2026-07-06"
    assert verdict["week_end_utc"] == "2026-07-11"
    assert verdict["weekly_pnl_usd"] == 16.0
    assert verdict["target_min_usd"] == 10.0
    assert verdict["verdict"] == "PASS"
    assert verdict["windows_filled_total"] == 61


def test_overlay_runtime_member_count_prefers_guard_runtime_roster() -> None:
    overlay_members = [
        {"source_wallet": "0x1", "enabled": True, "status": "MEMBER_BAR_QUALIFIED"},
        {"source_wallet": "0x2", "enabled": True, "status": "DEMOTED_FABLE"},
        {"source_wallet": "0x3", "enabled": False, "status": "MEMBER_BAR_QUALIFIED"},
        {"source_wallet": "0x4", "enabled": True, "status": "AUTO_DISABLED_100PCT_TOTAL_LOSS_RATE"},
    ]
    guard_runtime_members = [
        {"source_wallet": "0x1"},
        {"source_wallet": "0x5"},
    ]

    assert update_state_digest._overlay_runtime_member_count(overlay_members, guard_runtime_members) == 2
    assert update_state_digest._overlay_runtime_member_count(overlay_members, []) == 1


def test_defense_tripwires_report_distances_and_triggers() -> None:
    quiet = update_state_digest._defense_tripwires(
        {"pnl_usd": -20.75},
        {"actual_delta_vs_baseline_usd": 41.89},
        {"bucket_counts": {"lte_-5": 0}},
    )

    assert quiet["status"] == "OK"
    assert quiet["t1_day_pnl_distance_to_floor_usd"] == 14.25
    assert quiet["t1_since_topup_distance_to_floor_usd"] == 21.89
    assert quiet["t2_lte_minus_5_window_count"] == 0
    assert quiet["floor_breach_defense_posture"] == "CLEAR"
    assert quiet["intraday_probe_degrade_trigger_usd"] == -15.0
    assert quiet["size_defense_action"] == "PROBE_CAPS_REST_OF_UTC_DAY"

    triggered = update_state_digest._defense_tripwires(
        {"pnl_usd": -35.01},
        {"actual_delta_vs_baseline_usd": 19.99},
        {"bucket_counts": {"lte_-5": 1}},
    )

    assert triggered["status"] == "TRIGGERED"
    assert triggered["t1_day_pnl_triggered"] is True
    assert triggered["t1_day_floor_action"] == "PAPER_ONLY_REST_OF_UTC_DAY"
    assert triggered["t1_day_floor_resume_rule"] == "resume live at UTC rollover under standard restore evaluation"
    assert triggered["t1_since_topup_triggered"] is True
    assert triggered["t2_window_tail_triggered"] is True
    assert triggered["floor_breach_defense_posture"] == "ARMED"
    assert triggered["intraday_probe_degrade_trigger_usd"] == -8.0
    assert triggered["size_defense_action"] == "PAPER_ONLY_REST_OF_UTC_DAY"
    assert triggered["next_action"] == (
        "flip guard to paper_only for rest of UTC day; resume live at UTC rollover under standard restore evaluation"
    )

    probe_caps = update_state_digest._defense_tripwires(
        {"pnl_usd": -15.01},
        {"actual_delta_vs_baseline_usd": 19.99},
        {"bucket_counts": {"lte_-5": 1}},
    )

    assert probe_caps["status"] == "TRIGGERED"
    assert probe_caps["t1_day_pnl_triggered"] is False
    assert probe_caps["t1_since_topup_triggered"] is True
    assert probe_caps["t2_window_tail_triggered"] is True
    assert probe_caps["floor_breach_defense_posture"] == "ARMED"
    assert probe_caps["intraday_probe_degrade_trigger_usd"] == -8.0
    assert probe_caps["t1_day_floor_action"] == "NONE"
    assert probe_caps["t1_day_floor_resume_rule"] == ""
    assert probe_caps["size_defense_action"] == "PROBE_CAPS_REST_OF_UTC_DAY"
    assert probe_caps["probe_caps_cap_usd"] == 1.0
    assert probe_caps["next_action"] == "drop lane to probe caps for rest of UTC day"

    floor_grind = update_state_digest._defense_tripwires(
        {"pnl_usd": -9.20},
        {"actual_delta_vs_baseline_usd": 19.99},
        {"bucket_counts": {"lte_-5": 0}},
    )

    assert floor_grind["status"] == "TRIGGERED"
    assert floor_grind["intraday_probe_triggered"] is True
    assert floor_grind["single_fill_probe_triggered"] is False
    assert floor_grind["size_defense_action"] == "PROBE_CAPS_REST_OF_UTC_DAY"
    assert floor_grind["next_action"] == "drop lane to probe caps for rest of UTC day"

    cumulative_floor_only = update_state_digest._defense_tripwires(
        {"pnl_usd": -3.111974},
        {"actual_delta_vs_baseline_usd": -25.532568},
        {"bucket_counts": {"lte_-5": 0}},
    )

    assert cumulative_floor_only["intraday_probe_triggered"] is False
    assert cumulative_floor_only["single_fill_probe_triggered"] is False
    assert cumulative_floor_only["size_defense_action"] == "WATCH_TIGHTENED_PROBE_TRIGGER"


def test_defense_tripwires_scale_loss_triggers_in_stake_units() -> None:
    watching = update_state_digest._defense_tripwires(
        {"pnl_usd": -8.0},
        {"actual_delta_vs_baseline_usd": -25.0},
        {
            "bucket_counts": {"lte_-5": 1},
            "worst_windows": [{"pnl_usd": -8.0}],
        },
        effective_stake_usd=4.0,
    )

    assert watching["effective_stake_usd"] == 4.0
    assert watching["stake_admissible_max_usd"] == 1.75
    assert watching["stake_exceeds_bankroll_admissible"] is True
    assert watching["first_trigger_clamp_stake_usd"] == 2.133333
    assert watching["armed_clear_posture_collapse_stake_usd"] == 4.0
    assert watching["ladder_degenerate"] is True
    assert watching["intraday_probe_degrade_trigger_usd"] == -32.0
    assert watching["standard_intraday_probe_degrade_trigger_usd"] == -32.0
    assert watching["floor_breach_intraday_probe_degrade_trigger_usd"] == -32.0
    assert watching["single_fill_probe_degrade_trigger_usd"] == -20.0
    assert watching["trigger_r_multiple"] == {"intraday": 8.0, "single_fill": 5.0}
    assert watching["intraday_probe_triggered"] is False
    assert watching["single_fill_probe_triggered"] is False
    assert watching["size_defense_action"] == "WATCH_TIGHTENED_PROBE_TRIGGER"

    triggered = update_state_digest._defense_tripwires(
        {"pnl_usd": -32.0},
        {"actual_delta_vs_baseline_usd": -25.0},
        {
            "bucket_counts": {"lte_-5": 1},
            "worst_windows": [{"pnl_usd": -20.0}],
        },
        effective_stake_usd=4.0,
    )

    assert triggered["intraday_probe_triggered"] is True
    assert triggered["single_fill_probe_triggered"] is True
    assert triggered["size_defense_action"] == "PROBE_CAPS_REST_OF_UTC_DAY"

    sub_dollar = update_state_digest._defense_tripwires(
        {"pnl_usd": -4.0},
        {"actual_delta_vs_baseline_usd": -25.0},
        {
            "bucket_counts": {"lte_-5": 0},
            "min_window_pnl_usd": -5.25,
            "worst_windows": [],
        },
        effective_stake_usd=0.5,
    )

    assert sub_dollar["effective_stake_usd"] == 0.5
    assert sub_dollar["ladder_degenerate"] is False
    assert sub_dollar["stake_exceeds_bankroll_admissible"] is False
    assert sub_dollar["min_window_pnl_usd"] == -5.25
    assert sub_dollar["single_fill_probe_triggered"] is True
    assert sub_dollar["trigger_r_multiple"] == {"intraday": 16.0, "single_fill": 10.0}


def test_weekend_day_probe_uses_packet_trigger_at_weekend_open() -> None:
    packet = {
        "generated_at": "2026-07-17T15:55:55Z",
        "current_roster_weekend_posture_plan": {
            "direction_id": "2026-07-17T15:52Z-fable-weekend-prep",
            "weekend_starts_at": "2026-07-18T00:00:00Z",
            "weekend_loss_ladder": {"day_probe_trigger_usd": -8.0},
        },
    }

    pending = update_state_digest._weekend_day_probe(
        packet,
        {"pnl_usd": -9.0},
        generated_at="2026-07-17T23:59:59Z",
    )
    triggered = update_state_digest._weekend_day_probe(
        packet,
        {"pnl_usd": -8.01},
        generated_at="2026-07-18T00:01:00Z",
    )

    assert pending["status"] == "PENDING_WEEKEND_OPEN"
    assert pending["triggered"] is False
    assert triggered["status"] == "TRIGGERED"
    assert triggered["weekend_day_probe_trigger_usd"] == -8.0
    assert triggered["machine_tripwire_controlling"] is False
    assert triggered["size_defense_action"] == "PROBE_CAPS_AND_ASK_FABLE"
    assert triggered["seat_loss_rotation_rider"]["triggered"] is True
    assert triggered["seat_loss_rotation_rider"]["action"] == "ROTATE_F418_TO_A689"
    assert (
        triggered["seat_loss_rotation_rider"]["target_wallet"]
        == update_state_digest.WEEKEND_SEAT_LOSS_RIDER_TARGET_WALLET
    )


def test_weekend_day_probe_closes_at_monday_not_sticky() -> None:
    """weekend_starts_at without end must not keep current_is_weekend true forever."""
    packet = {
        "generated_at": "2026-07-17T15:55:55Z",
        "current_roster_weekend_posture_plan": {
            "direction_id": "2026-07-17T15:52Z-fable-weekend-prep",
            "weekend_starts_at": "2026-07-18T00:00:00Z",
            "weekend_loss_ladder": {"day_probe_trigger_usd": -8.0},
        },
    }
    sunday = update_state_digest._weekend_day_probe(
        packet,
        {"pnl_usd": -3.0},
        generated_at="2026-07-19T12:00:00Z",
    )
    monday = update_state_digest._weekend_day_probe(
        packet,
        {"pnl_usd": -3.0},
        generated_at="2026-07-20T12:00:00Z",
    )
    assert sunday["current_is_weekend"] is True
    assert sunday["status"] == "OK"
    assert sunday["weekend_ends_at"] == "2026-07-20T00:00:00Z"
    assert monday["current_is_weekend"] is False
    assert monday["status"] == "WEEKEND_CLOSED"
    assert monday["triggered"] is False
    assert monday["size_defense_action"] == "NONE"
    assert monday["seat_loss_rotation_rider"]["triggered"] is False


def test_weekend_day_probe_falls_back_from_stale_prior_week_bounds() -> None:
    packet = {
        "generated_at": "2026-07-18T00:00:00Z",
        "current_roster_weekend_posture_plan": {
            "weekend_starts_at": "2026-07-18T00:00:00Z",
            "weekend_ends_at": "2026-07-20T00:00:00Z",
            "weekend_loss_ladder": {"day_probe_trigger_usd": -8.0},
        },
    }
    friday = update_state_digest._weekend_day_probe(
        packet, {"pnl_usd": 0.495299}, generated_at="2026-07-24T23:59:59Z"
    )
    saturday = update_state_digest._weekend_day_probe(
        packet, {"pnl_usd": 0.495299}, generated_at="2026-07-25T00:00:00Z"
    )
    sunday = update_state_digest._weekend_day_probe(
        packet, {"pnl_usd": -8.0}, generated_at="2026-07-26T12:00:00Z"
    )
    monday = update_state_digest._weekend_day_probe(
        packet, {"pnl_usd": -8.0}, generated_at="2026-07-27T00:00:00Z"
    )
    assert friday["status"] == "PENDING_WEEKEND_OPEN"
    assert saturday["status"] == "OK"
    assert saturday["current_is_weekend"] is True
    assert saturday["distance_to_weekend_probe_usd"] == 8.495299
    assert saturday["weekend_starts_at"] == "2026-07-25T00:00:00Z"
    assert saturday["weekend_ends_at"] == "2026-07-27T00:00:00Z"
    assert sunday["status"] == "TRIGGERED"
    assert sunday["size_defense_action"] == "PROBE_CAPS_AND_ASK_FABLE"
    assert monday["status"] == "WEEKEND_CLOSED"
    assert monday["current_is_weekend"] is False


def test_state_digest_surfaces_weekend_rider_probe_cap_check(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-18T07:55Z fable DIRECTION [LIVE/DEFEND/ROTATE]\n"
        "- next: probe-cap controlled reload + fail-loud rider check.\n"
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "pid": 63564,
            "status": "LIVE_GUARD_RUNNING",
            "live_orders_allowed": True,
            "active_set_weekend_seat_loss_rotation": {
                "status": "A689_PIN_ALREADY_ACTIVE",
                "probe_caps_guard_flag_check": {
                    "status": "PASS",
                    "guard_max_order_usd": 1.0,
                    "guard_drip_max_tranche_usd": 1.0,
                    "probe_cap_usd": 1.0,
                    "passed": True,
                },
            },
        },
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "summary": {
                "can_trade": True,
                "live_orders": 1,
                "filled_orders": 1,
                "rejected_orders": 0,
                "submitted_orders": 0,
                "latest_order_ts": "2026-07-18T07:50:03Z",
            }
        },
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_2026-07-18.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "generated_at": "2026-07-18T07:51:14Z",
            "today": {"total": {"orders": 16, "fills": 12, "rejects": 4, "pnl_usd": -10.189995}},
            "canonical_pnl_truth": {"by_day": {"2026-07-18": {"pnl_usd": -10.189995, "resolved_fills": 11}}},
            "since_topup_truth": {
                "actual_delta_vs_baseline_usd": 17.693535,
                "actual_basis_reconciled_delta_vs_baseline_usd": 18.877069,
                "primary_verdict": "PRODUCING",
                "actual_basis_reconciled_verdict": "PRODUCING_RECONCILED_BASIS",
            },
            "volume_kpi": {
                "canonical_daily": {
                    "windows_filled": 12,
                    "windows_submitted": 12,
                    "denominator_windows": 288,
                }
            },
        },
    )

    digest, text = build_digest(tmp_path)

    rider = digest["live"]["active_set_weekend_seat_loss_rotation"]
    assert rider["probe_caps_guard_flag_check"]["status"] == "PASS"
    assert "rider_status=A689_PIN_ALREADY_ACTIVE" in text
    assert "rider_cap_check=PASS guard_caps=1.0/1.0" in text


def test_state_digest_surfaces_guard_caps_from_live_process_without_rider_check(monkeypatch, tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-18T08:18Z fable DIRECTION [LIVE/DEFEND/ROTATE/MEASURE]\n"
        "- next: surface guard caps even when rider cap-check is missing.\n"
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "pid": 63564,
            "status": "LIVE_GUARD_RUNNING",
            "live_orders_allowed": True,
            "active_set_weekend_seat_loss_rotation": {"status": "CLEAR"},
        },
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "summary": {
                "can_trade": True,
                "live_orders": 1,
                "filled_orders": 1,
                "rejected_orders": 0,
                "submitted_orders": 0,
                "latest_order_ts": "2026-07-18T08:10:14Z",
            }
        },
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_2026-07-18.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "generated_at": "2026-07-18T08:11:14Z",
            "today": {"total": {"orders": 1, "fills": 1, "rejects": 0, "pnl_usd": 0.1}},
            "canonical_pnl_truth": {"by_day": {"2026-07-18": {"pnl_usd": 0.1, "resolved_fills": 1}}},
            "since_topup_truth": {"actual_delta_vs_baseline_usd": 1.0},
        },
    )

    real_run = update_state_digest.subprocess.run

    def fake_run(cmd, *args, **kwargs):
        if cmd == ["ps", "-axo", "pid=,ppid=,stat=,etime=,command="]:
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "63564 1 S 01:23 python3 scripts/run_wallet_copy_live_guard.py "
                    "--max-order-usd 1.0 --drip-max-tranche-usd 1.0\n"
                ),
            )
        return real_run(cmd, *args, **kwargs)

    monkeypatch.setattr(update_state_digest.subprocess, "run", fake_run)

    digest, text = build_digest(tmp_path)

    assert digest["live"]["guard_caps"]["max_order_usd"] == 1.0
    assert digest["live"]["guard_caps"]["drip_max_tranche_usd"] == 1.0
    assert "rider_status=CLEAR rider_cap_check=None guard_caps=1.0/1.0" in text


def test_latest_active_auto_degrade_member_uses_last_action_admission() -> None:
    overlay = {
        "last_action": {
            "admitted": "leaderboard_crypto_5960377576",
            "demoted": "leaderboard_crypto_d918959370",
        },
        "latest_admission": {
            "candidate_id": "leaderboard_crypto_d918959370",
            "source_wallet": "0xd9189593701e94574ee140295a2ff6042317813b",
            "enabled": True,
            "status": "FABLE_0333_D918_HALF_SIZE_PROMOTED_DF2C_REPLACEMENT",
        },
        "members": [
            {
                "candidate_id": "leaderboard_crypto_d918959370",
                "source_wallet": "0xd9189593701e94574ee140295a2ff6042317813b",
                "enabled": False,
                "status": "DEMOTED_FABLE_0425_D918_NEGATIVE_ABORT_PACKET",
            },
            {
                "candidate_id": "leaderboard_crypto_5960377576",
                "source_wallet": "0x59603775762a631d4bcd156980c0a174bbc4c2d2",
                "enabled": True,
                "status": "FABLE_0425_5960_HALF_SIZE_PROMOTED_AFTER_D918_ABORT",
                "summary": {"direction_id": "2026-07-10T04:25Z-codex-d918-abort-fire-5960-promote"},
            },
        ],
    }

    member = update_state_digest._latest_active_auto_degrade_member(overlay)

    assert member["candidate_id"] == "leaderboard_crypto_5960377576"
    assert member["source_wallet"] == "0x59603775762a631d4bcd156980c0a174bbc4c2d2"


def test_latest_status_entry_allows_direction_word_in_status_summary() -> None:
    entries = [
        {
            "heading": "## 2026-07-13T14:37Z fable DIRECTION [PROMOTE] - next work",
            "text": "",
        },
        {
            "heading": "## 2026-07-13T14:59Z codex STATUS [LIVE] - 14:37Z DIRECTION executed",
            "text": "",
        },
    ]

    latest = update_state_digest._latest_status_entry(entries)

    assert latest["heading"].startswith("## 2026-07-13T14:59Z codex STATUS")


def test_latest_status_entry_uses_timestamp_not_file_order() -> None:
    entries = [
        {
            "heading": "## 2026-07-19T06:16Z codex STATUS [LIVE]",
            "body": "",
        },
        {
            "heading": "## 2026-07-19T05:47Z codex STATUS [LIVE]",
            "body": "",
        },
    ]

    latest = update_state_digest._latest_status_entry(entries)

    assert latest["heading"].startswith("## 2026-07-19T06:16Z")


def test_latest_handoff_entry_includes_newest_brainless_notify() -> None:
    entries = [
        {
            "heading": "## 2026-07-27T21:45:00Z fable DIRECTION [LIVE]",
            "body": "",
        },
        {
            "heading": "## 2026-07-27T21:59:54Z brainless NOTIFY — ORDER_FLOW_DEADMAN",
            "body": "",
        },
        {
            "heading": "## 2026-07-27T21:37:57Z codex STATUS [LIVE]",
            "body": "",
        },
    ]

    latest = update_state_digest._latest_handoff_entry(entries)

    assert "21:59:54Z brainless NOTIFY" in latest["heading"]


def test_latest_enabled_overflow_proposal_loads_newest_valid_artifact(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    _write_json(
        data / "enabled_overflow_semantics_proposal_20260710T0500Z.json",
        {
            "kind": "wallet_copy_enabled_overflow_semantics_proposal",
            "generated_at": "2026-07-10T05:00:00Z",
            "status": "STALE_DRAFT",
        },
    )
    newest = data / "enabled_overflow_semantics_proposal_20260710T0503Z.json"
    _write_json(
        newest,
        {
            "kind": "wallet_copy_enabled_overflow_semantics_proposal",
            "generated_at": "2026-07-10T05:03:44Z",
            "status": "DRAFT_ONLY_NO_LIVE_MUTATION",
            "recommended_design": {"name": "explicit_queue_position"},
        },
    )
    _write_json(
        data / "enabled_overflow_semantics_proposal_invalid.json",
        {
            "kind": "other_artifact",
            "status": "IGNORE",
        },
    )

    loaded = update_state_digest._latest_enabled_overflow_proposal(data)

    assert loaded["status"] == "DRAFT_ONLY_NO_LIVE_MUTATION"
    assert loaded["recommended_design"]["name"] == "explicit_queue_position"
    assert loaded["_path"] == str(newest)


def test_append_cash_diff_residual_trend_updates_artifact_with_cap(tmp_path: Path) -> None:
    path = tmp_path / "data" / "research" / "wallet_copy_today_fill_cash_diff_latest.json"
    _write_json(
        path,
        {
            "summary": {
                "scorecard_delta_residual_trend": [
                    {"generated_at": f"2026-07-10T20:{idx:02d}:00Z", "cash_diff_residual_usd": idx}
                    for idx in range(51)
                ]
            }
        },
    )

    trend = update_state_digest._append_cash_diff_residual_trend(
        tmp_path,
        {"state_path": "data/research/wallet_copy_today_fill_cash_diff_latest.json", "residual_usd": 15.800909},
        generated_at="2026-07-10T21:30:00Z",
    )

    updated = json.loads(path.read_text())
    assert len(trend) == update_state_digest.RESIDUAL_TREND_LIMIT
    assert trend[0]["cash_diff_residual_usd"] == 2
    assert trend[-1] == {
        "generated_at": "2026-07-10T21:30:00Z",
        "cash_diff_residual_usd": 15.800909,
        "basis": "unknown",
        "writer": "scripts/update_state_digest.py",
    }
    assert updated["summary"]["scorecard_delta_residual_trend"] == trend


def test_append_cash_diff_residual_trend_treats_null_previous_trend_as_empty(tmp_path: Path) -> None:
    path = tmp_path / "data" / "research" / "wallet_copy_today_fill_cash_diff_latest.json"
    _write_json(path, {"summary": {"scorecard_delta_residual_trend": None}})

    trend = update_state_digest._append_cash_diff_residual_trend(
        tmp_path,
        {"state_path": str(path), "residual_usd": -19.693051},
        generated_at="2026-07-10T21:35:00Z",
    )

    assert trend == [
        {
            "generated_at": "2026-07-10T21:35:00Z",
            "cash_diff_residual_usd": -19.693051,
            "basis": "unknown",
            "writer": "scripts/update_state_digest.py",
        }
    ]


def test_append_cash_diff_residual_trend_tags_basis_and_writer(tmp_path: Path) -> None:
    path = tmp_path / "data" / "research" / "wallet_copy_today_fill_cash_diff_latest.json"
    _write_json(path, {"summary": {}})

    trend = update_state_digest._append_cash_diff_residual_trend(
        tmp_path,
        {
            "state_path": "data/research/wallet_copy_today_fill_cash_diff_latest.json",
            "residual_usd": 3.25,
            "basis": "response_filled_size_usd",
            "writer": "unit-test",
        },
        generated_at="2026-07-10T22:00:00Z",
    )

    assert trend == [
        {
            "generated_at": "2026-07-10T22:00:00Z",
            "cash_diff_residual_usd": 3.25,
            "basis": "response_filled_size_usd",
            "writer": "unit-test",
        }
    ]


def test_state_digest_surfaces_selection_visibility_packet(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    _write_json(
        data / "selection_visibility_packet_latest.json",
        {
            "generated_at": "2026-07-10T22:40:00Z",
            "summary": {
                "status": "CLOCK_STARTED_ACCRUING",
                "sampled_signal_emitted_but_not_selected_windows": 10,
                "post_registration_extra_rows": 10,
                "excluded_pre_registration_rows": 3,
                "clock_start_condition_met": True,
                "selector_reason_coverage_pct": 100.0,
                "would_submit_pnl_fee_field_coverage_pct": 100.0,
                "measured_unique_windows": 10,
                "aggregate_measured_would_submit_post_fee_pnl_usd": 8.0,
                "copyintent_parity_status": "PASS",
                "instrumentation_fail_reasons": [],
            },
        },
    )

    digest, text = build_digest(tmp_path)

    summary = digest["selection_visibility_packet"]["summary"]
    assert summary["status"] == "CLOCK_STARTED_ACCRUING"
    assert summary["sampled_signal_emitted_but_not_selected_windows"] == 10
    assert "selection_visibility=status=CLOCK_STARTED_ACCRUING" in text
    assert "clock=True" in text


def test_state_digest_keeps_routing_source_when_pin_summary_is_newer(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    _write_json(
        data / "routing_shadow_validation_latest.json",
        {
            "generated_at": "2026-07-13T00:56:00Z",
            "summary": {
                "status": "ACCRUING_UNDER_PREREGISTERED_CLOCK",
                "validation_elapsed_hours": 43.59,
                "would_submit_windows": 425,
                "extra_would_submit_windows": 181,
                "copyintent_parity_status": "PASS",
                "runtime_selected_wallet": "0xe6db20932faf0f9780acf75d95c74c9984407dac",
                "runtime_selected_wallet_source": "selected_member",
                "selection_changes": 139,
            },
        },
    )
    _write_json(
        data / "routing_shadow_validation_attribution_pin_latest.json",
        {
            "generated_at": "2026-07-13T00:57:00Z",
            "summary": {
                "status": "ACCRUING_UNDER_PREREGISTERED_CLOCK",
                "validation_elapsed_hours": 43.60,
                "would_submit_windows": 425,
                "extra_would_submit_windows": 181,
                "copyintent_parity_status": "PASS",
                "runtime_selected_wallet": "0x59603775762a631d4bcd156980c0a174bbc4c2d2",
                "selection_changes": 140,
            },
        },
    )

    _, text = build_digest(tmp_path)

    assert "routing_shadow=status=ACCRUING_UNDER_PREREGISTERED_CLOCK" in text
    assert "runtime_source=selected_member" in text


def test_state_digest_surfaces_maker_first_book_aware_no_fallback_gate(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    _write_json(
        data / "maker_first_btc5m_book_aware_state.json",
        {
            "prospective_no_fallback_summary": {
                "resolved_paper_fills": 169,
                "resolved_paper_pnl_usd": 81.065291,
                "terminal_maker_fill_rate_pct": 93.274838,
            },
            "prospective_no_fallback_resolved_fill_ledger": {
                "distinct_resolved_fill_ids": 265,
                "current_source_distinct_resolved_fill_ids": 169,
                "prior_distinct_resolved_fill_ids": 265,
                "source_restated_lower_than_prior": True,
            },
            "promotion_gate": {
                "promotion_150_prospective_no_fallback_positive": "PASS_AUTO_PROMOTE",
                "gate_metric": "prospective_no_fallback_resolved_fill_ledger.distinct_resolved_fill_ids",
                "gate_source_file": "data/research/maker_first_btc5m_book_aware_state.json",
                "monotonicity_tripwire": {"status": "PASS"},
            },
        },
    )

    digest, text = build_digest(tmp_path)

    gates = digest["gates"]
    assert gates["e5_book_aware_no_fallback_gate"] == "PASS_AUTO_PROMOTE"
    assert gates["e5_book_aware_append_only_resolved"] == 265
    assert gates["e5_book_aware_current_source_resolved"] == 169
    assert gates["e5_book_aware_summary_pnl_usd"] == 81.065291
    assert gates["e5_book_aware_terminal_fill_rate_pct"] == 93.274838
    assert gates["e5_book_aware_monotonicity"] == "PASS"
    assert "E5_book_aware_no_fallback: gate=PASS_AUTO_PROMOTE append_only=265" in text


def test_canonical_day_score_prefers_response_basis_headline() -> None:
    score = update_state_digest._canonical_day_score(
        {
            "day_pnl_basis": {"day_pnl_response_basis": 24.876193},
            "today": {"total": {"pnl_usd": 63.017415, "resolved_fills": 258}},
        }
    )

    assert score["pnl_usd"] == 24.876193
    assert score["resolved_fills"] == 258


def test_day_pnl_basis_reconciliation_names_text_vs_fill_delta() -> None:
    reconciliation = update_state_digest._day_pnl_basis_reconciliation(
        selected_day_pnl=-20.041367,
        selected_basis="response_filled_size_usd",
        selected_resolved_fills=25,
        scorecard_text_day_pnl={
            "pnl_usd": -22.691367,
            "fills": 25,
            "resolved_fills": 25,
            "mtime": "2026-07-15T05:44:00Z",
        },
        deadman_fill_basis_day_pnl=-20.041367,
    )

    assert reconciliation["status"] == "MISMATCH"
    assert reconciliation["scorecard_text_fills"] == 25
    assert reconciliation["scorecard_text_resolved_fills"] == 25
    assert reconciliation["scorecard_text_mtime"] == "2026-07-15T05:44:00Z"
    assert reconciliation["scorecard_text_vs_deadman_delta_usd"] == -2.65
    assert reconciliation["scorecard_text_vs_deadman_abs_delta_usd"] == 2.65
    assert reconciliation["selected_vs_deadman_delta_usd"] == 0.0
    assert reconciliation["dashboard_money_source"] == "state_digest.pnl.day_pnl_usd"


def test_scorecard_text_day_pnl_captures_counts_and_mtime(tmp_path: Path) -> None:
    scorecard = tmp_path / "brainless_ops_scorecard.out"
    scorecard.write_text(
        "day_utc=2026-07-15 window=2026-07-15T00:00:00Z..2026-07-16T00:00:00Z\n"
        "total orders=33 fills=24 resolved=24 rejects=9 pnl=-1.111652\n"
    )

    parsed = update_state_digest._scorecard_text_day_pnl(scorecard)

    assert parsed is not None
    assert parsed["orders"] == 33
    assert parsed["fills"] == 24
    assert parsed["resolved_fills"] == 24
    assert parsed["rejects"] == 9
    assert parsed["pnl_usd"] == -1.111652
    assert parsed["day_utc"] == "2026-07-15"
    assert str(parsed["mtime"]).endswith("Z")


def test_scorecard_text_truth_supersedes_lower_count_same_day_json(
    tmp_path: Path,
) -> None:
    scorecard = tmp_path / "brainless_ops_scorecard.out"
    scorecard.write_text(
        "day_utc=2026-07-27 window=2026-07-27T00:00:00Z..2026-07-28T00:00:00Z\n"
        "total orders=32 fills=13 resolved=13 rejects=18 pnl=-1.750000\n"
        "since_topup_truth: verdict=NOT_PRODUCING baseline=$335.00 "
        "actual_delta=-6.893561\n"
        "volume_kpi: windows_filled=13/288 filled_pct=4.51 "
        "windows_submitted=29/288\n"
    )
    text_truth = update_state_digest._scorecard_text_day_pnl(scorecard)

    total, volume, since_topup = (
        update_state_digest._prefer_newer_scorecard_text_truth(
            scorecard={"day_utc": "2026-07-27"},
            score_total={
                "orders": 24,
                "fills": 10,
                "resolved_fills": 10,
                "rejects": 14,
                "pnl_usd": -0.95,
            },
            volume={
                "windows_filled": 10,
                "windows_submitted": 24,
                "denominator_windows": 288,
            },
            since_topup={
                "actual_delta_vs_baseline_usd": -3.993561,
                "primary_verdict": "NOT_PRODUCING",
            },
            text_truth=text_truth,
        )
    )

    assert total["pnl_usd"] == -1.75
    assert total["resolved_fills"] == 13
    assert volume["windows_filled"] == 13
    assert volume["windows_submitted"] == 29
    assert since_topup["actual_delta_vs_baseline_usd"] == -6.893561
    assert since_topup["scorecard_text_truth_preferred"] is True


def test_day_pnl_basis_reconciliation_ignores_prior_day_rollover_sources() -> None:
    reconciliation = update_state_digest._day_pnl_basis_reconciliation(
        selected_day_pnl=0.0,
        selected_basis="response_filled_size_usd",
        selected_resolved_fills=0,
        selected_day_utc="2026-07-22",
        scorecard_text_day_pnl={
            "day_utc": "2026-07-21",
            "pnl_usd": -7.353787,
            "fills": 56,
            "resolved_fills": 55,
        },
        deadman_fill_basis_day_pnl=-6.353788,
        deadman_fill_basis_day_utc="2026-07-21",
    )

    assert reconciliation["status"] == "PARTIAL"
    assert reconciliation["selected_day_utc"] == "2026-07-22"
    assert reconciliation["scorecard_text_day_mismatch"] is True
    assert reconciliation["deadman_fill_basis_day_mismatch"] is True
    assert reconciliation["scorecard_text_day_pnl_usd"] is None
    assert reconciliation["deadman_fill_basis_day_pnl_usd"] is None


def test_day_pnl_basis_reconciliation_marks_stale_text_basis() -> None:
    reconciliation = update_state_digest._day_pnl_basis_reconciliation(
        selected_day_pnl=2.223831,
        selected_basis="response_filled_size_usd",
        selected_resolved_fills=25,
        scorecard_text_day_pnl={
            "pnl_usd": -1.111652,
            "fills": 24,
            "resolved_fills": 24,
            "mtime": "2026-07-15T05:44:00Z",
        },
        deadman_fill_basis_day_pnl=None,
    )

    assert reconciliation["status"] == "STALE_TEXT_BASIS"
    assert reconciliation["scorecard_text_fills"] == 24
    assert reconciliation["selected_resolved_fills"] == 25
    assert reconciliation["selected_vs_scorecard_text_delta_usd"] == 3.335483


def test_day_pnl_basis_reconciliation_uses_text_resolved_for_fill_count() -> None:
    reconciliation = update_state_digest._day_pnl_basis_reconciliation(
        selected_day_pnl=12.834251,
        selected_basis="response_filled_size_usd",
        selected_resolved_fills=16,
        scorecard_text_day_pnl={
            "pnl_usd": 10.336292,
            "fills": 17,
            "resolved_fills": 15,
            "mtime": "2026-07-16T09:34:12Z",
        },
        deadman_fill_basis_day_pnl=12.834251,
        deadman_fill_basis_observed_at="2026-07-16T09:35:00Z",
    )

    assert reconciliation["status"] == "STALE_TEXT_BASIS"
    assert reconciliation["scorecard_text_fills"] == 17
    assert reconciliation["scorecard_text_resolved_fills"] == 15
    assert reconciliation["deadman_fill_basis_day_pnl_usd"] == 12.834251


def test_day_pnl_basis_reconciliation_marks_text_stale_when_deadman_fill_basis_is_newer() -> None:
    reconciliation = update_state_digest._day_pnl_basis_reconciliation(
        selected_day_pnl=-9.178957,
        selected_basis="response_filled_size_usd",
        selected_resolved_fills=72,
        scorecard_text_day_pnl={
            "pnl_usd": -9.178957,
            "fills": 72,
            "resolved_fills": 72,
            "mtime": "2026-07-15T12:14:57Z",
        },
        deadman_fill_basis_day_pnl=-7.178957,
        deadman_fill_basis_observed_at="2026-07-15T12:39:21Z",
    )

    assert reconciliation["status"] == "STALE_TEXT_BASIS"
    assert reconciliation["selected_vs_deadman_delta_usd"] == -2.0
    assert reconciliation["scorecard_text_vs_deadman_delta_usd"] == -2.0
    assert reconciliation["scorecard_text_older_than_deadman_fill_basis"] is True


def test_day_pnl_basis_reconciliation_marks_newer_text_than_fill_basis_as_benign_race() -> None:
    reconciliation = update_state_digest._day_pnl_basis_reconciliation(
        selected_day_pnl=-9.178957,
        selected_basis="response_filled_size_usd",
        selected_resolved_fills=72,
        scorecard_text_day_pnl={
            "pnl_usd": -9.178957,
            "fills": 72,
            "resolved_fills": 72,
            "mtime": "2026-07-15T12:40:00Z",
        },
        deadman_fill_basis_day_pnl=-7.178957,
        deadman_fill_basis_observed_at="2026-07-15T12:39:21Z",
    )

    assert reconciliation["status"] == "BENIGN_RACE"
    assert reconciliation["scorecard_text_older_than_deadman_fill_basis"] is False
    assert reconciliation["selected_vs_deadman_delta_usd"] == -2.0


def test_day_pnl_basis_reconciliation_names_newer_selected_hardening_as_benign_race() -> None:
    reconciliation = update_state_digest._day_pnl_basis_reconciliation(
        selected_day_pnl=15.647765,
        selected_basis="response_filled_size_usd",
        selected_resolved_fills=92,
        selected_day_utc="2026-07-22",
        scorecard_text_day_pnl={
            "day_utc": "2026-07-22",
            "pnl_usd": 16.727764,
            "fills": 93,
            "resolved_fills": 91,
            "mtime": "2026-07-22T19:35:53Z",
        },
        deadman_fill_basis_day_pnl=16.727764,
        deadman_fill_basis_observed_at="2026-07-22T19:37:25Z",
        deadman_fill_basis_day_utc="2026-07-22",
    )

    assert reconciliation["status"] == "BENIGN_RACE"
    assert reconciliation["scorecard_text_vs_deadman_abs_delta_usd"] == 0.0
    assert reconciliation["selected_vs_deadman_delta_usd"] == -1.079999


def test_state_digest_summarizes_live_execution_probes(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    _write_json(
        data / "wallet_copy_live_execution_probe_a727.json",
        {
            "status": "LIVE_ARMED_NO_FRESH_INTENTS",
            "candidate_id": "leaderboard_crypto_a727aa7c18",
            "source_wallet": "0xa727aa7c18821d191023561b6e410949215d91b5",
            "orders_submitted": 0,
            "candidate_intent_summary": {
                "fresh_candidate_intents": 0,
                "fresh_candidate_intents_after_toxicity_protection": 0,
                "source_events": 13,
                "live_event_prefilter": {
                    "skip_counts": {"not_btc_5m": 11, "market_closed_now": 2},
                    "min_live_floor_pin_enabled": False,
                    "min_live_floor_pin_direction_id": "2026-07-16T07:40Z-fable-seat-holder-min-live-pin",
                    "latest_source_event_runtime": {
                        "market_slug": "sol-updown-15m-1783627200",
                        "btc_5m_scope_ok": False,
                        "market_closed_now": False,
                        "event_age_s": 443.0,
                        "observed_age_s": 442.5,
                    },
                },
            },
        },
    )

    rows = update_state_digest._live_execution_probe_summaries(data)

    assert rows == [
        {
            "path": str(data / "wallet_copy_live_execution_probe_a727.json"),
            "label": "a727",
            "status": "LIVE_ARMED_NO_FRESH_INTENTS",
            "candidate_id": "leaderboard_crypto_a727aa7c18",
            "source_wallet": "0xa727aa7c18821d191023561b6e410949215d91b5",
            "orders_submitted": 0,
            "fresh_candidate_intents": 0,
            "fresh_candidate_intents_after_toxicity_protection": 0,
            "source_events": 13,
            "skip_counts": {"not_btc_5m": 11, "market_closed_now": 2},
            "min_live_floor_pin_enabled": False,
            "min_live_floor_pin_direction_id": "2026-07-16T07:40Z-fable-seat-holder-min-live-pin",
            "latest_market_slug": "sol-updown-15m-1783627200",
            "latest_btc_5m_scope_ok": False,
            "latest_market_closed_now": False,
            "latest_event_age_s": 443.0,
            "latest_observed_age_s": 442.5,
        }
    ]


def test_state_digest_refreshes_structural_scalp_lane_artifacts(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    _write_json(data / "wallet_copy_live_guard_hot_history_state.json", {"events": []})
    events_seen = {}

    def fake_build_state(root: Path, args):
        assert root == tmp_path
        assert args.history == "data/research/btc5m_structural_scalp_forward_source_events.jsonl"
        assert args.hot_source == "data/research/wallet_copy_live_guard_hot_history_state.json"
        assert args.state == "data/research/btc5m_structural_scalp_paper_lane_state.json"
        return (
            {
                "generated_at": "2026-07-07T19:15:00Z",
                "summary": {"forward_fills": 3, "forward_pnl_usd": 1.25, "current_intents": 2},
                "metrics": {"forward": {"fills": 3, "pnl_usd": 1.25, "span_days": 0.034722}},
                "live_gate": {"status": "FORWARD_GATE_PENDING", "ready_for_live": False},
            },
            [{"paper_fill_id": "fill-a"}],
        )

    def fake_write_events(path: Path, rows: list[dict]) -> None:
        events_seen["path"] = path
        events_seen["rows"] = rows
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")

    refresh = update_state_digest._refresh_btc5m_structural_scalp_lane(
        tmp_path,
        build_state_func=fake_build_state,
        write_events_func=fake_write_events,
    )

    assert refresh["status"] == "REFRESHED"
    assert refresh["forward_fills"] == 3
    assert refresh["forward_pnl_usd"] == 1.25
    assert refresh["forward_span_days"] == 0.034722
    assert refresh["event_rows"] == 1
    state = json.loads((data / "btc5m_structural_scalp_paper_lane_state.json").read_text())
    assert state["summary"]["current_intents"] == 2
    assert events_seen["path"] == data / "btc5m_structural_scalp_paper_lane_events.jsonl"
    assert events_seen["rows"] == [{"paper_fill_id": "fill-a"}]


def test_digest_double_reads_before_publishing_blocked(monkeypatch, tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    guard_path = data / "wallet_copy_live_guard_state.json"
    live_path = data / "wallet_copy_live_execution_state.json"
    reads = {
        str(guard_path): [
            {"pid": 1, "status": "LIVE_GUARD_BLOCKED", "live_orders_allowed": False},
            {"pid": 2, "status": "LIVE_GUARD_RUNNING", "live_orders_allowed": True},
        ],
        str(live_path): [
            {"summary": {"can_trade": False}},
            {"summary": {"can_trade": True}},
        ],
    }

    def fake_load_json(path: Path, default):
        queue = reads[str(path)]
        return queue.pop(0) if queue else default

    monkeypatch.setattr(update_state_digest, "_load_json", fake_load_json)
    monkeypatch.setattr(update_state_digest.time, "sleep", lambda _: None)

    guard, live, consistency = update_state_digest._load_confirmed_guard_live_pair(data)

    assert guard["status"] == "LIVE_GUARD_RUNNING"
    assert live["summary"]["can_trade"] is True
    assert consistency["double_read"] is True
    assert consistency["confirmed_blocked"] is False
    assert consistency["confirmed_non_trading"] is False
    assert consistency["first_guard_status"] == "LIVE_GUARD_BLOCKED"
    assert consistency["second_guard_status"] == "LIVE_GUARD_RUNNING"


def test_digest_double_reads_false_can_trade_even_when_guard_is_running(monkeypatch, tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    guard_path = data / "wallet_copy_live_guard_state.json"
    live_path = data / "wallet_copy_live_execution_state.json"
    reads = {
        str(guard_path): [
            {"pid": 1, "status": "LIVE_GUARD_RUNNING", "live_orders_allowed": True},
            {"pid": 1, "status": "LIVE_GUARD_RUNNING", "live_orders_allowed": True},
        ],
        str(live_path): [
            {"summary": {"can_trade": False}},
            {"summary": {"can_trade": True}},
        ],
    }

    def fake_load_json(path: Path, default):
        queue = reads[str(path)]
        return queue.pop(0) if queue else default

    monkeypatch.setattr(update_state_digest, "_load_json", fake_load_json)
    monkeypatch.setattr(update_state_digest.time, "sleep", lambda _: None)

    guard, live, consistency = update_state_digest._load_confirmed_guard_live_pair(data)

    assert guard["status"] == "LIVE_GUARD_RUNNING"
    assert live["summary"]["can_trade"] is True
    assert consistency["double_read"] is True
    assert consistency["confirmed_non_trading"] is False
    assert consistency["first_can_trade"] is False
    assert consistency["second_can_trade"] is True


def test_guard_process_snapshot_reports_live_guard_stat(monkeypatch, tmp_path: Path) -> None:
    class Result:
        returncode = 0
        stdout = (
            "42507 1 U 14:26 /venv/bin/python scripts/run_wallet_copy_live_guard.py --execute-live\n"
            "123 1 S 00:01 /bin/other\n"
        )

    monkeypatch.setattr(update_state_digest.subprocess, "run", lambda *args, **kwargs: Result())

    snapshot = update_state_digest._guard_process_snapshot(tmp_path)

    assert snapshot["status"] == "PASS"
    assert snapshot["process_count"] == 1
    assert snapshot["rows"][0]["pid"] == 42507
    assert snapshot["rows"][0]["stat"] == "U"


def test_state_digest_backfills_guard_loop_profile_from_fresh_event_log(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    docs = root / "docs" / "agents"
    docs.mkdir(parents=True)
    docs.joinpath("HANDOFF.md").write_text(
        "## 2026-07-16T19:45Z fable DIRECTION [LIVE/DEFEND]\n"
        "- ORDER: digest guard telemetry must stay non-empty post-restart.\n"
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "pid": 19811,
            "status": "LIVE_GUARD_BLOCKED",
            "live_orders_allowed": True,
            "guard_loop_profile": {},
        },
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {"summary": {"can_trade": False, "live_orders": 1, "filled_orders": 1, "rejected_orders": 0}},
    )
    (data / "wallet_copy_live_guard_events.jsonl").write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00Z",
                "pid": 19811,
                "guard_loop_profile": {
                    "status": "MEASURING_GUARD_CYCLE_CADENCE",
                    "cycle_duration_s": 3.399785,
                    "stage_timers": [{"name": "select_active_set_runtime", "duration_s": 0.821374}],
                },
            }
        )
        + "\n"
    )

    digest, text = build_digest(root)

    profile = digest["live"]["guard_loop_profile"]
    assert profile["status"] == "MEASURING_GUARD_CYCLE_CADENCE"
    assert profile["pid"] == 19811
    assert profile["source"] == "data/research/wallet_copy_live_guard_events.jsonl"
    assert profile["total_s_before_state_write"] == 3.399785
    assert profile["freshness_status"] == "CURRENT"
    assert "guard_loop_profile: status=MEASURING_GUARD_CYCLE_CADENCE total_s=3.399785" in text


def test_state_digest_marks_old_guard_cache_evidence_historical(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    docs = root / "docs" / "agents"
    docs.mkdir(parents=True)
    docs.joinpath("HANDOFF.md").write_text(
        "## 2026-07-29T22:05:41Z fable DIRECTION [OBSERVE]\n"
        "- order_1: stale cache timers must not render as current.\n"
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "generated_at": "2999-01-01T00:00:00Z",
            "pid": 19811,
            "status": "LIVE_GUARD_RUNNING",
            "live_orders_allowed": True,
            "guard_loop_profile": {
                "status": "MEASURING_GUARD_CYCLE_CADENCE",
                "cycle_started_at": "2999-01-01T00:00:00Z",
                "cycle_duration_s": 4.0,
                "stage_timers": [{"name": "live_execution", "duration_s": 2.0}],
            },
        },
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {"summary": {"can_trade": True}},
    )
    _write_json(
        data / "guard_memory_json_cache_evidence_latest.json",
        {
            "generated_at": "2026-07-18T16:21:49Z",
            "status": "PASS",
            "cycle_stage_evidence": {"cycle_duration_s": 11.414848},
        },
    )

    digest, _ = build_digest(root)

    cache = digest["live"]["guard_json_cache_evidence"]
    assert cache["freshness_status"] == "STALE"
    assert cache["cycle_stage_evidence_current"] is False
    assert cache["cycle_stage_evidence"] == {}
    assert cache["historical_cycle_stage_evidence"] == {
        "cycle_duration_s": 11.414848
    }


def test_material_working_tree_snapshot_surfaces_code_and_config(monkeypatch, tmp_path: Path) -> None:
    class Result:
        returncode = 0
        stdout = "\n".join(
            [
                " M data/research/runtime.json",
                " M scripts/run_wallet_copy_live_guard.py",
                " M tests/test_wallet_copy_core.py",
                " M configs/wallet_copy/wallets.json",
                " M docs/agents/HANDOFF.md",
            ]
        )

    monkeypatch.setattr(update_state_digest.subprocess, "run", lambda *args, **kwargs: Result())

    snapshot = update_state_digest._material_working_tree_snapshot(tmp_path)

    assert snapshot["status"] == "PASS"
    assert snapshot["count"] == 4
    assert snapshot["paths"] == [
        "scripts/run_wallet_copy_live_guard.py",
        "tests/test_wallet_copy_core.py",
        "configs/wallet_copy/wallets.json",
        "docs/agents/HANDOFF.md",
    ]


def test_latest_status_entry_accepts_daily_plus_status_heading() -> None:
    entries = [
        {
            "heading": "## 2026-07-12T23:57Z codex STATUS [LIVE]",
            "body": "- defect | stale | next=old",
        },
        {
            "heading": "## 2026-07-13T00:12Z codex DAILY+STATUS [LIVE/DEFEND]",
            "body": "- defect | current | next=new",
        },
        {
            "heading": "## 2026-07-13T00:32Z fable DIRECTION [LIVE]",
            "body": "- RULING: accepted",
        },
    ]

    latest = update_state_digest._latest_status_entry(entries)

    assert latest["heading"] == "## 2026-07-13T00:12Z codex DAILY+STATUS [LIVE/DEFEND]"


def test_direction_next_block_captures_fable_direction_status_next_summary() -> None:
    text = "\n".join(
        [
            "- Audit of commit -- PASS.",
            "- DIRECTION status [LIVE/ROTATE/SELF-DEV]: NEXT(1) runtime roster closure; NEXT(2) digest parser fix.",
            "- trailing material.",
        ]
    )

    assert update_state_digest._direction_next_block(text) == [
        "- DIRECTION status [LIVE/ROTATE/SELF-DEV]: NEXT(1) runtime roster closure; NEXT(2) digest parser fix.",
    ]
    assert update_state_digest._material_direction_lines(text) == [
        "- DIRECTION status [LIVE/ROTATE/SELF-DEV]: NEXT(1) runtime roster closure; NEXT(2) digest parser fix.",
    ]


def test_direction_next_block_captures_prefixed_resumption_queue() -> None:
    text = "\n".join(
        [
            "- AUDIT OF THE OUTAGE: PASS.",
            "- RESUMPTION QUEUE (codex, in order, credits now available):",
            "  1. P1c handback + scorecard refresh.",
            "  2. 08:00Z 48H VERDICT PACKET.",
            "- konzisztens: igen.",
        ]
    )

    expected = [
        "- RESUMPTION QUEUE (codex, in order, credits now available):",
        "  1. P1c handback + scorecard refresh.",
        "  2. 08:00Z 48H VERDICT PACKET.",
    ]

    assert update_state_digest._direction_next_block(text) == expected
    assert update_state_digest._material_direction_lines(text) == []


def test_direction_next_block_captures_numbered_markdown_queue_heading() -> None:
    text = "\n".join(
        [
            "### 6. Queue — reordered. Flow outranks the paper band",
            "",
            "1. **ORDER134-E6** — measure, then repair only on a material flip.",
            "2. **ORDER134-E2b** — repair record honesty.",
            "3. **ORDER134-E3/E4** — reinstate with taker costs remeasured.",
            "",
            "Money remains unchanged.",
        ]
    )

    assert update_state_digest._direction_next_block(text) == [
        "### 6. Queue — reordered. Flow outranks the paper band",
        "1. **ORDER134-E6** — measure, then repair only on a material flip.",
        "2. **ORDER134-E2b** — repair record honesty.",
        "3. **ORDER134-E3/E4** — reinstate with taker costs remeasured.",
    ]


def test_direction_next_block_captures_numbered_hungarian_next_actions_heading() -> None:
    text = """\
### 7. Evidence

- complete: measured.

### 8. Pontos következő akciók

1. Freeze the generation.
2. Run exactly one automatic restart.
### 9. Money

unchanged.
"""

    assert update_state_digest._direction_next_block(text) == [
        "### 8. Pontos következő akciók",
        "1. Freeze the generation.",
        "2. Run exactly one automatic restart.",
    ]


def test_direction_next_block_captures_numbered_hungarian_order_heading() -> None:
    text = """\
### 8. A pénz

Nincs változás.

### 9. Sorrend — profit/óra szerint, a következő munkára

1. **R75-javítás** — kompozit bizonyítékkulcs.
2. **R73-javítás** — üres taxonómia megtagadása.
3. **R76-javítás** — szerződésazonos bin-élek.

**R1–R72 fenntartva.**
"""

    assert update_state_digest._direction_next_block(text) == [
        "### 9. Sorrend — profit/óra szerint, a következő munkára",
        "1. **R75-javítás** — kompozit bizonyítékkulcs.",
        "2. **R73-javítás** — üres taxonómia megtagadása.",
        "3. **R76-javítás** — szerződésazonos bin-élek.",
    ]


def test_direction_next_block_captures_comma_queue_heading() -> None:
    text = "\n".join(
        [
            "### 5. Queue, reordered by profit-per-hour",
            "",
            "1. **ORDER134-E6(c)** — taker execution cost, measured.",
            "2. **ORDER134-E6(b)** — order-type repair gated on cost.",
            "",
            "### 6. Money",
        ]
    )

    assert update_state_digest._direction_next_block(text) == [
        "### 5. Queue, reordered by profit-per-hour",
        "1. **ORDER134-E6(c)** — taker execution cost, measured.",
        "2. **ORDER134-E6(b)** — order-type repair gated on cost.",
    ]


def test_direction_next_block_captures_direction_dash_queue_heading() -> None:
    text = "\n".join(
        [
            "### 7. Direction — the queue, ordered by profit-per-hour",
            "",
            "1. **ORDER135-H5** — instrument empty generations.",
            "2. **ORDER135-H6** — rank copyable continuity first.",
            "3. **ORDER135-H7** — repair packet cadence.",
            "",
            "### 8. Money",
        ]
    )

    assert update_state_digest._direction_next_block(text) == [
        "### 7. Direction — the queue, ordered by profit-per-hour",
        "1. **ORDER135-H5** — instrument empty generations.",
        "2. **ORDER135-H6** — rank copyable continuity first.",
        "3. **ORDER135-H7** — repair packet cadence.",
    ]


def test_direction_next_block_captures_descriptive_direction_queue_heading() -> None:
    text = "\n".join(
        [
            "### 4. Direction — H10 opens at the top; the queue is re-ordered around it",
            "",
            "1. **ORDER135-H10** — write journal deltas.",
            "2. **ORDER135-H6** — re-rank after H10.",
            "",
            "### 5. Money",
        ]
    )

    assert update_state_digest._direction_next_block(text) == [
        "### 4. Direction — H10 opens at the top; the queue is re-ordered around it",
        "1. **ORDER135-H10** — write journal deltas.",
        "2. **ORDER135-H6** — re-rank after H10.",
    ]


def test_direction_next_block_captures_answer_order_restated_list() -> None:
    text = "\n".join(
        [
            "- AUDIT 8a1e3ce: ACCEPTED.",
            "- NEW DATUM, no new defect: source restated 155->147 vs append-only 162.",
            "- ANSWER to codex question: packet accepted; nothing further is owed before the FINAL packet. Next action order restated:",
            "  1. Sun 2026-07-12T23:30Z -- FINAL weekend-parity rerun + fresh Q3 stakeout + maker refresh.",
            "  2. Mon 2026-07-13T00:15Z -- Fable confirmation; 23c7-must-not-pass.",
            "- STANDING GUARD unchanged.",
        ]
    )

    expected = [
        "- ANSWER to codex question: packet accepted; nothing further is owed before the FINAL packet. Next action order restated:",
        "  1. Sun 2026-07-12T23:30Z -- FINAL weekend-parity rerun + fresh Q3 stakeout + maker refresh.",
        "  2. Mon 2026-07-13T00:15Z -- Fable confirmation; 23c7-must-not-pass.",
    ]

    assert update_state_digest._direction_next_block(text) == expected
    assert update_state_digest._material_direction_lines(text) == [
        "- NEW DATUM, no new defect: source restated 155->147 vs append-only 162.",
        "- ANSWER to codex question: packet accepted; nothing further is owed before the FINAL packet. Next action order restated:",
        "1. Sun 2026-07-12T23:30Z -- FINAL weekend-parity rerun + fresh Q3 stakeout + maker refresh.",
        "2. Mon 2026-07-13T00:15Z -- Fable confirmation; 23c7-must-not-pass.",
    ]


def test_direction_next_block_captures_answer_sequence_stands_line() -> None:
    text = "\n".join(
        [
            "- AUDIT: heartbeat accepted.",
            "- ANSWER to codex (PROACTIVE STEERING PULSE, hourly): no order change. Sequence stands: Sun 23:30Z FINAL rerun -> Mon 00:00Z activation -> Mon 00:15Z Fable confirmation -> Mon 12:00Z attribution. Until 23:30Z: quiet window holds.",
        ]
    )

    expected = [
        "- ANSWER to codex (PROACTIVE STEERING PULSE, hourly): no order change. Sequence stands: Sun 23:30Z FINAL rerun -> Mon 00:00Z activation -> Mon 00:15Z Fable confirmation -> Mon 12:00Z attribution. Until 23:30Z: quiet window holds.",
    ]

    assert update_state_digest._direction_next_block(text) == expected
    assert update_state_digest._material_direction_lines(text) == expected


def test_direction_next_block_captures_order_bullets_without_next_label() -> None:
    text = "\n".join(
        [
            "- AUDIT of packet: VERIFIED.",
            "- ORDER (skip-fast, codex): on quota-class rc1 persist degraded_until.",
            "  Fallback order otherwise unchanged.",
            "- BUDGET NOTE + NOTIFY: decide subscription upgrade vs weekly quota.",
            "- ORDER (grok update, codex): update grok in post-09:00Z quiet window.",
            "  If smoke fails, pin grok degraded.",
            "- ORDER (agy latest-source policy, codex): set latest_source explicitly.",
            "- LIVE unchanged and outranked by nothing here: deadmen OK, source quiet.",
            "  Milestones stand: >=05:10Z shadow readout, 07-09Z cap proof, Mon 12:00Z attribution.",
            "- konzisztens: igen.",
        ]
    )

    expected = [
        "- ORDER (skip-fast, codex): on quota-class rc1 persist degraded_until.",
        "  Fallback order otherwise unchanged.",
        "- BUDGET NOTE + NOTIFY: decide subscription upgrade vs weekly quota.",
        "- ORDER (grok update, codex): update grok in post-09:00Z quiet window.",
        "  If smoke fails, pin grok degraded.",
        "- ORDER (agy latest-source policy, codex): set latest_source explicitly.",
        "- LIVE unchanged and outranked by nothing here: deadmen OK, source quiet.",
        "  Milestones stand: >=05:10Z shadow readout, 07-09Z cap proof, Mon 12:00Z attribution.",
    ]

    assert update_state_digest._direction_next_block(text) == expected
    assert update_state_digest._material_direction_lines(text) == [
        "- ORDER (skip-fast, codex): on quota-class rc1 persist degraded_until.",
        "- BUDGET NOTE + NOTIFY: decide subscription upgrade vs weekly quota.",
        "- ORDER (grok update, codex): update grok in post-09:00Z quiet window.",
        "- ORDER (agy latest-source policy, codex): set latest_source explicitly.",
        "- LIVE unchanged and outranked by nothing here: deadmen OK, source quiet.",
        "Milestones stand: >=05:10Z shadow readout, 07-09Z cap proof, Mon 12:00Z attribution.",
    ]


def test_direction_next_block_captures_priority_bullet_without_next_label() -> None:
    text = "\n".join(
        [
            "- AUDIT: no new decidables.",
            "- DECISION: standing tripwire response remains active.",
            "- PRIORITY (unchanged): 00:00Z verdict + mandatory managed restart > 23:45Z snapshot > R9 latency check.",
            "  Include guard_loop_profile latency digits in the snapshot.",
            "- konzisztens: igen.",
        ]
    )

    expected = [
        "- PRIORITY (unchanged): 00:00Z verdict + mandatory managed restart > 23:45Z snapshot > R9 latency check.",
        "  Include guard_loop_profile latency digits in the snapshot.",
    ]

    assert update_state_digest._direction_next_block(text) == expected
    assert update_state_digest._material_direction_lines(text) == [
        "- DECISION: standing tripwire response remains active.",
        "- PRIORITY (unchanged): 00:00Z verdict + mandatory managed restart > 23:45Z snapshot > R9 latency check.",
    ]


def test_material_direction_captures_named_structural_fact() -> None:
    text = "\n".join(
        [
            "- pulse audit: six rejects.",
            "- named structural fact, not action [OBSERVE]: with a $1 tranche and 5 minimum shares, price > $0.20 is infeasible.",
            "- ruling 1 — checkpoint unchanged.",
        ]
    )

    assert update_state_digest._material_direction_lines(text) == [
        "- named structural fact, not action [OBSERVE]: with a $1 tranche and 5 minimum shares, price > $0.20 is infeasible.",
        "- ruling 1 — checkpoint unchanged.",
    ]


def test_direction_next_block_captures_plural_priorities_bullet() -> None:
    text = "\n".join(
        [
            "- MONEY: all clear.",
            "- PRIORITIES UNCHANGED: 1) 13:00Z scheduler read; 2) 20:30Z routing read;",
            "  3) 2026-07-19 first-hour cap restore.",
            "- INVARIANTS: singleton guard.",
        ]
    )

    expected = [
        "- PRIORITIES UNCHANGED: 1) 13:00Z scheduler read; 2) 20:30Z routing read;",
        "  3) 2026-07-19 first-hour cap restore.",
    ]

    assert update_state_digest._direction_next_block(text) == expected
    assert update_state_digest._material_direction_lines(text) == [
        "- PRIORITIES UNCHANGED: 1) 13:00Z scheduler read; 2) 20:30Z routing read;"
    ]


def test_state_digest_material_direction_falls_back_to_verbatim_top_level_bullets(tmp_path: Path) -> None:
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-13T03:40Z fable DIRECTION [LIVE/SELF-DEV]",
                "- Audit accepted with unfamiliar phrasing.",
                "- Money-pipe cadence stays exactly as scheduled.",
                "  This continuation is not a top-level bullet.",
                "- Unknown heading form still matters.",
            ]
        )
    )

    digest, text = build_digest(tmp_path)

    assert digest["latest_direction_material_extraction_basis"] == "verbatim_fallback"
    assert digest["latest_direction_material"] == [
        "- Audit accepted with unfamiliar phrasing.",
        "- Money-pipe cadence stays exactly as scheduled.",
        "- Unknown heading form still matters.",
    ]
    assert "## Latest Direction Material Lines (verbatim_fallback)" in text
    assert "- Money-pipe cadence stays exactly as scheduled." in text


def test_state_digest_includes_active_alpha_overlap_capture_rows(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    _write_json(
        data / "alpha_decay_13e0_f418_a689_registry.json",
        {
            "paper_only": True,
            "live_orders_allowed": False,
            "wallets": [{"address": "0x1"}, {"address": "0x2"}, {"address": "0x3"}],
        },
    )
    (data / "polygon_orderfilled_alpha_overlap_13e0_f418_a689_20260722T0025Z.jsonl").write_text(
        '{"row": 1}\n{"row": 2}\n'
    )
    (data / "clob_books_alpha_overlap_13e0_f418_a689_20260722T0025Z.jsonl").write_text(
        '{"row": 1}\n'
    )

    digest, text = build_digest(tmp_path)

    capture = digest["alpha_overlap_capture"]
    assert capture["run_suffix"] == "20260722T0025Z"
    assert capture["polygon_rows"] == 2
    assert capture["clob_rows"] == 1
    assert capture["wallet_count"] == 3
    assert capture["paper_only"] is True
    assert capture["live_orders_allowed"] is False
    assert "alpha_overlap_capture:" in text
    assert "rows=2/1" in text


def test_state_digest_active_alpha_overlap_capture_outranks_stale_terminal_files(
    tmp_path: Path, monkeypatch
) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    _write_json(
        data / "alpha_decay_13e0_f418_a689_registry.json",
        {"paper_only": True, "live_orders_allowed": False, "wallets": []},
    )
    (data / "polygon_orderfilled_alpha_overlap_13e0_f418_a689_20260722T0231Z_recap.jsonl").write_text(
        '{"row": 1}\n'
    )
    (data / "alpha_decay_13e0_f418_a689_overlap_latest.json").write_text("{}")
    (data / "alpha_decay_13e0_f418_a689_overlap_state.json").write_text("{}")
    monkeypatch.setattr(update_state_digest, "_launchctl_job_pid", lambda _label: 90356)

    digest, _text = build_digest(tmp_path)

    capture = digest["alpha_overlap_capture"]
    assert capture["pid"] == 90356
    assert capture["report_exists"] is True
    assert capture["state_exists"] is True
    assert capture["status"] == "RUNNING_CAPTURE"


def test_entry_timestamp_iso_normalizes_direction_heading() -> None:
    entry = {"heading": "## 2026-07-09T07:14Z fable DIRECTION", "body": ""}

    assert update_state_digest._entry_timestamp_iso(entry) == "2026-07-09T07:14:00Z"


def test_state_digest_is_shadow_readable_and_line_bounded(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    market_facts = tmp_path / "docs" / "agents" / "MARKET_FACTS.md"
    market_facts.write_text(
        "\n".join(
            [
                "# Market Facts",
                "- Fees are not a fixed per-order charge.",
                "- Tick sizes vary by market.",
            ]
        )
    )
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-06T20:50Z fable DIRECTION",
                "- next: build digest",
                "  continue mining",
                "- warnings: grade carefully",
                "## 2026-07-06T20:51Z STATUS [LIVE/SELF-DEV]",
                "- defect | attempts (3+) | next: OP-VOLUME below 144/288 | attempts: a,b,c | next: mine",
                "## 2026-07-06T21:05Z fable DIRECTION",
                "- next: keep digest next-list verbatim",
                "  preserve continuation lines",
                "- warnings: none.",
                "## 2026-07-06T21:15Z DIRECTION (Fable, co-operator audit)",
                "- next (codex, in order): accept newer direction heading",
                "  preserve variant prefix",
                "- CORRECTION 2: report full active set, not only current member",
                "- ruling 1: capture lower-case ruling material",
                "- warnings: none.",
                "## 2026-07-06T21:25Z Fable DIRECTION",
                "- RULING 1: queue-form direction wins when newest",
                "  QUEUE for codex:",
                "  1) revive polygon_ws shadow.",
                "  2) tighten dataapi poller.",
                "- FORBIDDEN: losing flow.",
            ]
        )
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "summary": {
                "can_trade": True,
                "live_orders": 10,
                "filled_orders": 4,
                "rejected_orders": 6,
                "submitted_orders": 0,
                "latest_order_ts": "2026-07-06T20:25:00Z",
            },
            "orders": [
                {
                    "submitted_at": "2026-07-06T20:25:00Z",
                    "status": "FILLED",
                    "source_intent": {"metadata": {"expected_fee_gate": {"status": "CAPTURED"}}},
                    "expected_vs_realized_fee": {"expected_fee_usd": 0.1},
                }
            ],
        },
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "pid": 10107,
            "status": "LIVE_GUARD_RUNNING",
            "live_orders_allowed": True,
            "candidate_id": "candidate_a",
            "source_wallet": "0x1234567890abcdef",
            "policy_id": "policy_a",
            "window_participation": {
                "active_windows": 3,
                "missed_active_windows": 0,
                "consecutive_missed_active_windows": 0,
                "incident_triggered": False,
            },
            "active_set_rtds_premerge": {
                "status": "PASS",
                "wallets_refreshed": 8,
                "new_matching_events": 12,
                "retained_matching_rows": 40,
                "max_rtds_catchup_lag_s": 0.75,
                "paper_only": True,
                "live_orders_allowed": False,
            },
            "event_triggered_cycle_scheduler": {
                "status": "TRIGGER_NEXT_GUARD_CYCLE",
                "enabled": True,
                "triggered": True,
                "reason": "fresh_in_window_btc5m_copy_event",
                "sleep_s": 0.0,
                "configured_sleep_s": 2.0,
                "trigger_sleep_s": 0.0,
                "premerge_new_matching_events": 12,
                "premerge_wallets": ["0xfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed"],
                "source_event": {
                    "event_id": "we_scheduler",
                    "source_wallet": "0xfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed",
                    "market_slug": "btc-updown-5m-1783965000",
                    "window_close_at": "2026-07-13T17:55:00Z",
                },
                "last_trigger": {
                    "event_id": "we_scheduler",
                    "trigger_cycle": 44,
                    "trigger_cycle_started_at": "2026-07-06T20:25:00Z",
                },
                "single_submitter_change": False,
                "copyintent_parity_change": False,
                "cap_threshold_eligibility_change": False,
                "scheduler_submits_orders": False,
                "submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains the sole live order submitter",
            },
            "own_impact_monitor": {
                "status": "STALE_INVENTORY_CAUSE_NAMED",
                "named_cause": "active_set_history_freshness_skew",
                "stale_inventory_rows": 2,
                "wallet_eligible_orders": 7,
                "our_submits": 1,
                "our_fills": 1,
                "active_set_rtds_wallets_refreshed": 8,
                "active_set_rtds_new_matching_events": 12,
            },
            "active_set": {
                "members": [
                    {
                        "candidate_id": "round_robin_original",
                        "source_wallet": "0xffffffffffffffffffffffffffffffffffffffff",
                        "policy_id": "policy_round_robin",
                        "is_current_cycle_member": True,
                    },
                    {
                        "candidate_id": "candidate_b",
                        "source_wallet": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "policy_id": "policy_b",
                        "is_current_cycle_member": False,
                    }
                ]
            },
            "active_set_runtime": {
                "qualified_member_count": 1,
                "selected_member": {
                    "candidate_id": "runtime_selected_member",
                    "source_wallet": "0xfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed",
                    "policy_id": "policy_runtime",
                },
                "members": [
                    {
                        "candidate_id": "runtime_selected_member",
                        "source_wallet": "0xfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed",
                        "policy_id": "policy_runtime",
                    }
                ],
                "total_loss_auto_disable": {
                    "enabled": True,
                    "min_resolved_fills": 3,
                    "rule": "auto-disable active-set members with 3+ resolved fills and 100% total-loss rate",
                    "disabled_members": [
                        {
                            "candidate_id": "candidate_b",
                            "source_wallet": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                            "resolved_fills": 3,
                            "total_loss_fills": 3,
                            "pnl_usd": -6.3,
                            "cost_usd": 6.3,
                        }
                    ],
                },
            },
            "candidate": {
                "pass_gate": {
                    "passed": False,
                    "failed_checks": ["status_live_admissible"],
                    "status": "FABLE_TEST_STATUS",
                    "live_protection_gate": {"passed": True},
                }
            },
        },
    )
    (data / "wallet_copy_live_guard_events.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "generated_at": f"2026-07-06T20:{minute}:00Z",
                    "pid": pid,
                    "guard_loop_profile": {
                        "total_s_before_state_write": total,
                        "stage_timers": [{"name": "active_set_rtds_premerge", "duration_s": rtds}],
                    },
                }
            )
            for minute, pid, total, rtds in [
                ("20", 10106, 28.0, 12.0),
                ("21", 10106, 31.0, 13.0),
                ("22", 10106, 32.0, 14.0),
                ("23", 10106, 33.0, 13.5),
                ("24", 10107, 22.0, 4.8),
                ("25", 10107, 21.0, 4.7),
            ]
        )
        + "\n"
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_test.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "today": {
                "total": {"pnl_usd": 1.25, "resolved_fills": 4},
                "per_member": {
                    "0x1234567890abcdef": {"orders": 3, "fills": 2, "rejects": 1, "pnl_usd": 1.25}
                },
            },
            "since_topup_truth": {
                "primary_verdict": "PRODUCING",
                "canonical_pnl_usd": 1.25,
                "actual_delta_vs_baseline_usd": 1.3,
                "actual_basis_reconciled_delta_vs_baseline_usd": 2.05,
                "actual_basis_reconciled_verdict": "PRODUCING_RECONCILED_BASIS",
                "self_feed_reconciliation_overlay": {
                    "overlay_delta_usd": 0.75,
                    "mode": "RECONCILIATION_OVERLAY",
                    "ledger_rewrite": False,
                },
                "reconciliation_status": "PASS",
            },
            "volume_kpi": {
                "canonical_daily": {
                    "windows_filled": 2,
                    "windows_submitted": 3,
                    "denominator_windows": 288,
                }
            },
            "per_window_pnl_histogram": {
                "reporting_only": True,
                "gate_use_allowed": False,
                "resolved_windows": 2,
                "positive_windows": 1,
                "negative_windows": 1,
                "zero_windows": 0,
                "bucket_counts": {"-1_to_0": 1, "1_to_5": 1},
            },
            "execution_model_kpi": {
                "orders_per_submitted_window": 1.5,
                "orders_per_filled_window": 2.0,
                "fill_rate_pct": 66.666667,
                "copy_model_counts": {"drip": 3},
                "drip": {"orders": 3, "fills": 2, "drip_stop_saves": 1, "avg_entry_minus_source_vwap": 0.01},
                "strong_tier": {"orders": 1, "resolved_fills": 1, "resolved_pnl_usd": 0.25},
            },
            "active_set_roster": {
                "members": [
                    {
                        "candidate_id": "candidate_a",
                        "source_wallet": "0x1234567890abcdef",
                        "policy_id": "policy_a",
                        "is_current_cycle_member": True,
                    }
                ]
            },
        },
    )
    _write_json(
        data / "wallet_copy_member_rolling20_latest.json",
        {
            "kind": "wallet_copy_member_rolling20",
            "generated_at": "2026-07-06T20:31:00Z",
            "active_member_count": 2,
            "threshold_usd": -8.0,
            "mechanical_rotation_required": False,
            "rows": [
                {
                    "wallet": "0xffffffffffffffffffffffffffffffffffffffff",
                    "rolling20_n": 20,
                    "rolling20_pnl_usd": 1.25,
                    "rolling20_ready": True,
                    "rolling20_rotation_triggered": False,
                },
                {
                    "wallet": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    "rolling20_n": 17,
                    "rolling20_pnl_usd": -8.5,
                    "rolling20_ready": False,
                    "rolling20_rotation_triggered": False,
                },
            ],
        },
    )
    _write_json(data / "wallet_copy_full_pool_member_queue.json", {"summary": {"ready_for_live": 0, "queue_depth": 1}})
    _write_json(
        data / "coverage_gap_diagnosis_latest.json",
        {
            "generated_at": "2026-07-10T07:07:50Z",
            "window": {
                "start_iso": "2026-07-09T07:05:00Z",
                "end_iso": "2026-07-10T07:05:00Z",
                "windows_total": 288,
            },
            "summary": {
                "windows_total": 288,
                "submitted_windows": 104,
                "zero_submission_windows": 184,
                "observed_zero_submission_windows": 35,
                "unobserved_zero_submission_windows": 149,
                "dominant_reason_class": "no-eligible-signal",
                "reason_class_counts": {
                    "no-eligible-signal": 149,
                    "selector-abstain": 0,
                    "price/eligibility-filter": 12,
                    "guard-reject": 0,
                    "stale-flow-protected": 23,
                },
                "submitted_gap_to_op_volume": 40,
            },
        },
    )
    _write_json(
        data / "coverage_gap_signal_supply_check_latest.json",
        {
            "generated_at": "2026-07-10T07:31:00Z",
            "summary": {
                "unobserved_no_signal_windows": 148,
                "sources_idle_windows": 120,
                "sources_traded_but_unobserved_windows": 28,
                "unknown_fetch_incomplete_windows": 0,
                "dominant_class": "sources_idle",
                "root_cause": "sources_idle_dominates",
                "fetch_complete": True,
            },
        },
    )
    _write_json(
        data / "routing_disambiguation_latest.json",
        {
            "generated_at": "2026-07-10T07:45:00Z",
            "summary": {
                "sampled_windows": 20,
                "dominant_class": "signal-emitted-but-not-selected",
                "selected_wallet_at_report_time": "0x5e4aa0f176014729f5168e821ce614484fbebe6b",
                "target_wallet_active_runtime_member": True,
            },
        },
    )
    _write_json(
        data / "campaign_lat_p1_packet_latest.json",
        {
            "generated_at": "2026-07-10T18:55:00Z",
            "summary": {
                "primary_constraint": "retention_selection_visibility",
                "live_day_submitted_gap_to_144": 5,
                "trailing_24h_submitted_gap_to_144": 0,
                "next_decision": "paper/shadow evidence only; no live eligibility loosening from this packet",
                "stage2_verdict": "INTERIM_NO_FLIP_NOT_FREEZE_OF_RECORD",
            },
            "recommended_next_actions": [{"id": "D1_RETENTION_PACKET"}],
        },
    )
    _write_json(
        data / "routing_shadow_validation_latest.json",
        {
            "generated_at": "2026-07-10T08:00:00Z",
            "summary": {
                "status": "ACCUMULATING",
                "validation_elapsed_hours": 1.25,
                "would_submit_windows": 3,
                "extra_would_submit_windows": 2,
                "copyintent_parity_status": "PASS",
                "runtime_selected_wallet": "0x1234567890abcdef",
                "runtime_selected_wallet_source": "selected_member",
                "shadow_selected_wallet": "0xfedcba0987654321",
                "selection_changes": 1,
                "evaluated_member_count": 4,
                "coverage_accounted_member_count": 6,
                "non_denied_runtime_members": 6,
                "filter_attrition_totals_latest_cycle": {
                    "fresh_candidate_intents": 11,
                    "fresh_candidate_intents_after_expected_fee_gate": 3,
                    "fresh_candidate_intents_after_toxicity_protection": 2,
                    "routeable_signals": 2,
                    "would_submit": 1,
                },
                "fee_gate_calibration_retained": {
                    "fee_gated_intents": 5,
                    "resolved_intents": 2,
                    "measurable_resolved_intents": 2,
                    "unmeasured_resolved_intents": 0,
                    "pre_fee_pnl_usd": 1.75,
                    "expected_fee_usd_sum": 0.2,
                    "post_fee_pnl_usd": 1.55,
                },
            },
        },
    )
    _write_json(
        data / "member_factory_kpi_state.json",
        {
            "kind": "member_factory_kpi",
            "queue_depth": {"ready_for_live": 2, "target": 5, "queue_depth": 7},
            "factory_throughput": {"replay_promotable": 3, "ready_for_live": 2},
        },
    )
    _write_json(
        data / "wallet_copy_full_universe_copyability_latest.json",
        {
            "summary": {
                "registry_wallets": 4,
                "wallets_scored": 4,
                "wallets_with_any_evidence": 2,
                "wallets_with_replay": 2,
                "positive_copy_pnl_wallets": 1,
                "ranked_queue_depth": 1,
            },
            "top_wallets": [
                {
                    "wallet": "0x1234567890abcdef",
                    "copyability_score": 9.5,
                    "admission_status": "READY_QUEUE",
                    "copy_replay": {"paper_pnl_usd": 1.2, "copyable_buy_events": 21},
                    "followability": {"score": 3.4},
                }
            ],
        },
    )
    for name in (
        "wallet_copy_full_universe_copyability_leaderboard_latest.json",
        "wallet_copy_full_universe_copyability_leaderboard_summary_latest.json",
    ):
        _write_json(
            data / name,
            {
                "status": "DEPRECATED_POINTER",
                "canonical_path": "data/research/wallet_copy_full_universe_copyability_latest.json",
                "generated_at": "2026-07-22T17:48:43Z",
            },
        )
    _write_json(
        data / "maker_first_btc5m_paper_state.json",
        {"promotion_gate": {"promotion_50_resolved_positive": "PENDING", "unresolved_paper_fills": 10}},
    )
    _write_json(
        data / "e5_review_split_latest.json",
        {"book_aware_non_fallback_summary": {"resolved_paper_fills": 50, "resolved_paper_pnl_usd": 2.0}},
    )
    _write_json(
        data / "btc5m_late_window_penny_watcher_state.json",
        {"summary": {"observed_windows": 7, "penny_opportunities": 22, "calibration_gate": {"required_observed_windows": 100}}},
    )
    _write_json(
        data / "brainless_ops_latest.json",
        {"status": "OK", "rotation_action": "NONE", "queue_ready_for_live": 0, "queue_depth": 1},
    )
    _write_json(
        data / "own_positions_latest.json",
        {
            "status": "PASS",
            "generated_at": "2026-07-08T09:30:00Z",
            "wallet": "0x1234...abcd",
            "summary": {
                "positions_rows": 3,
                "data_api_redeemable_rows": 1,
                "data_api_redeemable_locked_usd": 24.0,
                "ledger_estimated_redeemable_locked_usd": 22.5,
                "redeemable_locked_usd": 24.0,
                "locked_value_source": "data_api_positions",
            },
            "data_api": {"status": "OK"},
        },
    )
    _write_json(
        data / "own_position_deadman_state.json",
        {
            "status": "WATCH_REDEEMABLE_LOCKED",
            "age_s": 120.0,
            "incident": False,
            "next_action": "continue 10-minute refresh/redeem cycle",
        },
    )
    _write_json(
        data / "own_redeemer_state.json",
        {
            "status": "DRY_RUN_READY",
            "candidate_count": 1,
            "candidate_source": "ledger_estimate_data_api_unavailable",
            "executed": False,
        },
    )
    _write_json(
        data / "wallet_outflow_deadman_state.json",
        {
            "status": "OK",
            "checked_at": "2026-07-08T09:31:00Z",
            "incident": False,
            "wallet": "0x1234567890abcdef1234567890abcdef12345678",
            "last_ok_checked_at": "2026-07-08T09:31:00Z",
            "consecutive_degraded_fetches": 0,
            "window": {"fetch_gap_exceeded": False},
            "summary": {
                "outflow_rows": 5,
                "matched_order_outflows": 5,
                "matched_redemption_outflows": 0,
                "unmatched_outflows": 0,
                "incident_outflows": 0,
                "unmatched_outflow_usd": 0.0,
            },
            "next_action": "continue brainless outflow watch",
        },
    )
    _write_json(
        data / "research_disk_deadman_state.json",
        {
            "status": "OK",
            "generated_at": "2026-07-16T18:45:00Z",
            "incident": False,
            "incident_keys": [],
            "disk": {"free_gib": 277.5, "used_pct": 84.9},
            "memory_swap": {
                "status": "OK",
                "incident": False,
                "swapfiles": {"count": 4, "threshold_count": 20, "above_threshold": False},
                "memory_pressure": {"free_pct": 62, "pressure_pct": 38, "critical": False},
            },
            "inventory_count": 7,
            "large_files": [{"path": "data/research/polygon_orderfilled_ws_shadow_resident.jsonl", "size_bytes": 17179869184}],
            "uninventoried_large_files": [],
            "next_action": "continue 10-minute brainless disk deadman",
        },
    )
    _write_json(
        data / "wallet_copy_guard_event_log_rotation_state.json",
        {
            "status": "REPAIRED",
            "generated_at": "2026-07-18T05:37:05Z",
            "action": "rename_archive_reseed_tail",
            "path": "data/research/wallet_copy_live_guard_events.jsonl",
            "size_before_bytes": 9904767552,
            "size_after_bytes": 65959556,
            "archive_path": "data/research/log_archives/wallet_copy_live_guard_events_20260718T053705Z.jsonl",
            "archive_size_bytes": 9904767552,
            "bytes_removed_from_hot_path": 9838807996,
            "retained_tail_bytes": 65959556,
            "tail_line_aligned": True,
        },
    )
    _write_json(
        data / "alpha_decay_curve_study_latest.json",
        {
            "updated_at": "2026-07-08T09:31:00Z",
            "alpha_decay": {
                "status": "PASS",
                "horizons_s": [1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0],
                "fills_total": 300,
                "fills_with_any_book_coverage": 240,
                "overlapping_fill_book_assets": 12,
                "coverage_by_horizon": {
                    "5s": {"coverage": 240, "edge": {"mean": 0.016, "p50": 0.01}},
                    "10s": {"coverage": 230, "edge": {"mean": 0.012}},
                },
                "next_action": "use alpha-decay report for execution profiles",
            },
        },
    )
    _write_json(
        data / "data_layer_v1_manifest.json",
        {
            "status": "PASS",
            "rows_converted": 5,
            "files_converted": 1,
            "files_considered": 1,
            "bytes_read": 2852,
            "duckdb": {"rows": 5, "duckdb_path": "data/derived/wallet_copy_data_layer_v1/wallet_copy.duckdb"},
            "missing_dependencies": [],
        },
    )
    _write_json(
        data / "wallet_copy_self_feed_duckdb_benchmark_latest.json",
        {
            "status": "PASS",
            "generated_at": "2026-07-07T15:41:49Z",
            "jsonl_summary": {"rows": 270, "tx_groups": 269, "cost_usd": 446.00644},
            "duckdb_summary": {"rows": 270, "tx_groups": 269, "cost_usd": 446.00644},
            "parity": {"rows": True, "tx_groups": True, "cost_usd": True},
            "gap_scan": {
                "status": "PASS",
                "duckdb_summary": {
                    "self_feed_missing_ledger_critical": 73,
                    "self_feed_missing_ledger_cost_usd": 99.00432,
                    "probable_split_fill_groups": 4,
                    "price_rounding_mismatch_tx_groups": 15,
                },
                "parity": {"self_feed_missing_ledger_critical": True},
            },
            "classification_packet": {
                "classification_summary": {
                    "self_feed_missing_ledger_rows": 73,
                    "join_key_defect_probable_split_fill": 4,
                    "true_unrecorded_fill_candidate": 69,
                },
                "full_ledger_retrace": {
                    "class_counts": {
                        "b3_duplicate_full_ledger_match": 29,
                        "b3_join_scope_artifact_size_price_or_time_mismatch": 40,
                    }
                },
                "resolved_pnl_overlay": {
                    "pnl_usd": 31.822431,
                    "reconciled_actual_estimate_usd": 6.912425,
                },
                "recommendation": {"mode": "RECONCILIATION_OVERLAY", "ledger_rewrite": False},
            },
            "benchmarks": [
                {"label": "jsonl_self_feed_scan", "elapsed_ms": 1.306},
                {"label": "duckdb_wallet_copy_events_scan", "elapsed_ms": 38.22875},
                {"label": "duckdb_self_feed_gap_scan", "elapsed_ms": 442.02325},
            ],
            "next_action": "wire self-feed reconciliation to DuckDB summary queries",
        },
    )
    _write_json(
        data / "h2_external_redemption_ingestion_latest.json",
        {
            "status": "PASS",
            "generated_at": "2026-07-08T18:59:00Z",
            "overlay_source_name": "external_data_api_redeem_condition_join",
            "ledger_rewrite": False,
            "summary": {
                "external_redeem_rows": 10,
                "confirmed_external_redeem_rows": 10,
                "total_redeem_usdc": 58.685738,
                "max_abs_delta_usd": 0.0,
            },
            "acceptance": {
                "cash_diff_residual_usd": -7.590385,
                "residual_explained_by_external_redeems_usd": 0.0,
                "residual_unexplained_after_external_redeems_usd": -7.590385,
                "anchor_relabel": "CONFIRMED_EXTERNAL_REDEEM",
            },
        },
    )
    _write_json(
        data / "wallet_copy_residual_cash_diff_audit_latest.json",
        {
            "generated_at": "2026-07-16T08:31:00Z",
            "status": "PASS_TX_HASH_OR_TIMESTAMP_SKEW_NAMED",
            "residual_reconciliation": {
                "canonical_residual_usd": -19.777051,
                "canonical_residual_classification": "unaccounted_one_time_cash_movement",
                "current_residual_single_movement_match_status": (
                    "MULTIPLE_MATCHES_FOUND_REQUIRES_DAY_FILTER_OR_SECONDARY_EVIDENCE"
                ),
                "scorecard_day_residual_direct_tx_matches": [],
                "current_residual_direct_tx_matches": [
                    {"tx": "0xaaa", "signed_amount_usd": 19.636362},
                    {"tx": "0xbbb", "signed_amount_usd": 19.735539},
                ],
                "conclusion": "multiple baseline-window matches; no ledger rewrite",
            },
            "other_counterparty_rows": [],
        },
    )
    _write_json(
        data / "wallet_copy_dr_preflight_latest.json",
        {
            "status": "OFF_MACHINE_REMOTE_MISSING",
            "summary": {"has_push_remote": False, "remote_count": 0, "dirty_paths": 2, "tracked_secret_paths": []},
            "next_action": "add_private_remote_then_run_dr_push",
        },
    )
    _write_json(
        data / "experiment_preregistration_latest.json",
        {
            "status": "PASS",
            "registry_records": 1,
            "valid_records": 1,
            "active_count": 1,
            "latest_experiment_id": "btc5m-pair-sum-forward-20260707",
            "latest_success_criterion": ">=30 forward fills and positive paper PnL",
            "latest_deadline_utc": "2026-07-09T08:00:00Z",
            "missing_required_ids": [],
        },
    )
    _write_json(
        data / "fee_edge_decomposition_latest.json",
        {
            "generated_at": "2026-07-20T16:41:00Z",
            "experiment_id": "fee-edge-decomposition-20260720",
            "verdict": "WINNER",
            "winner_count": 1,
            "winners": [
                {
                    "cohort": "routing_shadow_extra_would",
                    "axis": "entry_price_band",
                    "slice": "[0.4,0.6)",
                    "n_resolved_windows": 159,
                    "post_fee_pnl_usd": 8.395767,
                }
            ],
            "measurement_only": True,
            "live_mutation": False,
        },
    )
    _write_json(
        data / "paper_copy_weekend_window_sign_skew_latest.json",
        {"paper_only": True, "live_path_mutated": False, "summary": {"windows": 74, "pnl_usd": -6.14, "negative_discovery_cells_n10": 0, "gate": "HOLDOUT_PENDING"}},
    )
    _write_json(
        data / "paper_copy_weekend_hour_of_day_skew_latest.json",
        {"paper_only": True, "live_path_mutated": False, "summary": {"candidate_hours_n20": 0, "gate": "HOLDOUT_PENDING"}},
    )
    _write_json(
        data / "paper_copy_fak_nomatch_requote_latest.json",
        {"paper_only": True, "live_path_mutated": False, "summary": {"prospective_observations": 0, "target_observations": 30, "prospective_requote_eligible": 0}},
    )
    _write_json(
        data / "wallet_copy_active_set_auto_degrade_state.json",
        {
            "kind": "wallet_copy_active_set_auto_degrade_state",
            "members": [
                {"source_wallet": "0x1234567890abcdef", "enabled": True},
                {"source_wallet": "0xfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed", "enabled": True},
            ],
            "latest_admission_wave": {
                "direction_id": "2026-07-14T17:04Z-fable-mass-admission-wave",
                "updated_at": "2026-07-14T17:05:00Z",
                "status": "CONFIGURED",
                "picked_count": 2,
                "runtime_cap": 9,
                "unruled_aging_gt24h_count": 0,
                "picked_wallets": [
                    "0x1234567890abcdef",
                    "0xfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed",
                ],
            },
            "latest_rotation": {
                "direction_id": "2026-07-07T15:19:29Z-fable-post-patch-rotation-trigger",
                "updated_at": "2026-07-07T15:27:26Z",
                "demoted_wallets": ["0x1234567890abcdef"],
                "admitted_wallets": ["0xabcdef1234567890"],
                "post_patch_fresh_fill_basis": {"wallet": "0x1234567890abcdef"},
                "reason": "unit rotation reason",
            },
        },
    )
    _write_json(
        data / "btc5m_live_paper_fleet_latest.json",
        {
            "generated_at": "2026-07-07T18:25:00Z",
            "summary": {
                "fleet_size": 200,
                "matrix_rows": 10,
                "matrix_windows": 8,
                "fleet_wallets_with_matrix_rows": 3,
                "ready_queue_wallets": 2,
                "positive_copy_pnl_wallets": 4,
                "top_wallet": "0x1234567890abcdef",
                "top_score": 9.5,
                "top50_matrix_coverage": {"wallets": 50, "with_rows": 3, "none": 47},
                "coverage_defect": True,
                "coverage_defect_next_action": "mark matrix gaps",
            },
        },
    )
    _write_json(
        data / "btc5m_two_sided_prime_study_latest.json",
        {
            "generated_at": "2026-07-07T18:26:00Z",
            "summary": {
                "accepted_events": 12,
                "resolved_windows": 5,
                "paired_markets": 4,
                "pair_sum_candidates": 2,
                "pair_sum_frequency_pct": 50.0,
                "two_sided_wallets": 2,
                "top_mechanism": "structural-pair-sum-arb",
                "top_ev_per_day_usd": 1.5,
            },
            "mechanism_rows": [
                {
                    "mechanism_id": "structural-pair-sum-arb",
                    "status": "HOLDOUT_PASS",
                    "oos_trades": 4,
                    "oos_pnl_usd": 1.5,
                }
            ],
        },
    )
    _write_json(
        data / "btc5m_morning_ranked_table_latest.json",
        {
            "generated_at": "2026-07-07T18:27:00Z",
            "summary": {
                "rows": 9,
                "holdout_passed_rows": 2,
                "matrix_coverage_none_rows": 3,
                "top_rank": {
                    "rank": 1,
                    "mechanism_id": "copy-1to1-taker",
                    "candidate_id": "0x1234567890abcdef",
                    "family": "copy",
                    "status": "HOLDOUT_PASS_READY_QUEUE",
                    "ev_per_day_usd": 4.2,
                    "oos_trades": 21,
                    "proposed_funding_size_usd": 4.0,
                },
            },
        },
    )
    _write_json(
        data / "polygon_ws_shadow_service_state.json",
        {
            "status": "RUNNING",
            "pid": 12345,
            "started_at": "2026-07-07T22:52:54Z",
            "cycles": 0,
            "paper_only": True,
            "live_orders_allowed": False,
            "process_invariant": {"count": 1, "status": "PASS", "rows": []},
            "comparison_jsonl": "data/research/polygon_ws_dataapi_active_set_comparison.jsonl",
        },
    )
    _write_json(
        data / "strategy_map_latest.json",
        {
            "generated_at": "2026-07-07T18:28:00Z",
            "authority": "READING_AID_NOT_AUTHORITY_FULL_CONTEXT_REQUIRED",
            "summary": {
                "rows": 34,
                "active_or_gated_rows": 30,
                "fresh_rows": 26,
                "stale_rows": 8,
                "stale_is_defect": True,
            },
            "stale_defects": [{"id": "signal-consensus"}, {"id": "signal-inventory-e4"}],
        },
    )
    _write_json(
        data / "resource_utilization_latest.json",
        {
            "generated_at": "2026-07-07T18:29:00Z",
            "verdict": "IDLE_CAPACITY",
            "active_or_gated_lane_count": 30,
            "memory_capped_max_lane_count": 42,
            "idle_lane_capacity": 12,
            "utilization_pct": 71.428571,
            "cpu": {"headroom_pct": 80.0},
            "memory_pressure": {"headroom_to_pause_pct": 35.0},
            "defect": {"open": True, "next": "add paper lanes"},
        },
    )

    digest, text = build_digest(tmp_path)

    assert digest["validation_mode"] == "SHADOW_READ_BOTH_FULL_CONTEXT_AND_DIGEST"
    assert digest["line_budget_ok"] is True
    assert digest["fee_event_check"]["expected_fee_gate_present"] is True
    assert digest["execution_model"]["drip_orders"] == 3
    assert digest["market_facts"]["exists"] is True
    assert digest["live"]["can_trade_source"] == "data/research/wallet_copy_live_execution_state.json.summary.can_trade"
    assert digest["live"]["polygon_ws_shadow"]["status"] == "RUNNING"
    assert digest["live"]["polygon_ws_shadow"]["process_invariant"]["count"] == 1
    assert digest["strategy_map"]["stale_ids"] == ["signal-consensus", "signal-inventory-e4"]
    assert digest["resource_utilization"]["verdict"] == "IDLE_CAPACITY"
    assert digest["resource_utilization"]["idle_lane_capacity"] == 12
    assert digest["resource_utilization"]["defect_open"] is True
    assert digest["latest_direction_next_verbatim"] == [
        "  QUEUE for codex:",
        "  1) revive polygon_ws shadow.",
        "  2) tighten dataapi poller.",
    ]
    assert digest["latest_direction_ts"] == "2026-07-06T21:25:00Z"
    assert digest["newest_fable_direction_ts"] == "2026-07-06T21:25:00Z"
    assert digest["direction_freshness_status"] == "FRESH"
    assert digest["latest_direction_material"] == [
        "- RULING 1: queue-form direction wins when newest",
    ]
    assert digest["data_layer_v1"]["status"] == "PASS"
    assert digest["btc5m_live_paper_fleet"]["top50_matrix_coverage"]["none"] == 47
    assert digest["btc5m_two_sided_prime"]["top_mechanism"] == "structural-pair-sum-arb"
    assert digest["btc5m_morning_ranked_table"]["top_status"] == "HOLDOUT_PASS_READY_QUEUE"
    assert digest["btc5m_structural_scalp_paper_lane"]["refresh_status"] == "SKIPPED_SOURCE_MISSING"
    assert digest["self_feed_duckdb_benchmark"]["status"] == "PASS"
    assert digest["self_feed_duckdb_benchmark"]["duckdb_rows"] == 270
    assert digest["self_feed_duckdb_benchmark"]["gap_critical"] == 73
    assert digest["h2_external_redemptions"]["status"] == "PASS"
    assert digest["h2_external_redemptions"]["confirmed_external_redeem_rows"] == 10
    assert digest["h2_external_redemptions"]["residual_explained_by_external_redeems_usd"] == 0.0
    assert digest["h2_external_redemptions"]["residual_unexplained_after_external_redeems_usd"] == -7.590385
    assert digest["residual_cash_diff_audit"]["status"] == "PASS_TX_HASH_OR_TIMESTAMP_SKEW_NAMED"
    assert digest["residual_cash_diff_audit"]["canonical_residual_usd"] == -19.777051
    assert digest["residual_cash_diff_audit"]["scorecard_day_direct_matches"] == 0
    assert digest["residual_cash_diff_audit"]["baseline_window_direct_matches"] == 2
    assert digest["residual_cash_diff_audit"]["other_counterparty_rows"] == 0
    assert digest["dr_preflight"]["status"] == "OFF_MACHINE_REMOTE_MISSING"
    assert digest["experiment_preregistration"]["latest_experiment_id"] == "btc5m-pair-sum-forward-20260707"
    assert digest["fee_edge_decomposition"]["winner_count"] == 1
    assert digest["fee_edge_decomposition"]["live_mutation"] is False
    assert digest["weekend_copy_shadows"]["window_sign_skew"]["windows"] == 74
    assert digest["weekend_copy_shadows"]["paper_only"] is True
    assert digest["weekend_copy_shadows"]["live_path_mutated"] is False
    assert digest["active_set_rtds_premerge"]["wallets_refreshed"] == 8
    assert digest["active_set"]["current_candidate_id"] == "runtime_selected_member"
    assert digest["active_set"]["current_wallet"] == "0xfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed"
    assert digest["active_set"]["current_policy_id"] == "policy_runtime"
    assert digest["active_set"]["member_count"] == 2
    assert digest["active_set"]["runtime"]["member_count"] == 1
    assert digest["active_set"]["runtime"]["overlay_enabled_member_count"] == 2
    assert digest["active_set"]["runtime"]["overlay_disabled_member_count"] == 0
    assert digest["active_set"]["runtime"]["overlay_runtime_member_delta"] == 1
    assert digest["active_set"]["runtime"]["selected_candidate_id"] == "runtime_selected_member"
    assert digest["active_set"]["runtime"]["candidate_pass_gate"] == {
        "passed": False,
        "failed_checks": ["status_live_admissible"],
        "status": "FABLE_TEST_STATUS",
        "live_protection_passed": True,
    }
    assert "active_member: runtime_selected_member 0xfeed...feed policy=policy_runtime" in text
    assert "overlay_enabled=2 overlay_runtime_delta=1" in text
    assert "'failed_checks': ['status_live_admissible']" in text
    assert digest["active_set"]["runtime"]["total_loss_auto_disable"] == {
        "enabled": True,
        "min_resolved_fills": 3,
        "rule": "auto-disable active-set members with 3+ resolved fills and 100% total-loss rate",
        "disabled_count": 1,
        "disabled_members": [
            {
                "candidate_id": "candidate_b",
                "wallet": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "resolved_fills": 3,
                "total_loss_fills": 3,
                "pnl_usd": -6.3,
                "cost_usd": 6.3,
            }
        ],
    }
    assert digest["member_rolling20"]["ready_count"] == 1
    assert digest["member_rolling20"]["active_member_count"] == 2
    assert digest["member_rolling20"]["mechanical_rotation_required"] is False
    assert digest["member_rolling20"]["sample_incomplete_negative_rows"] == [
        {
            "wallet": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "rolling20_n": 17,
            "rolling20_pnl_usd": -8.5,
        }
    ]
    assert digest["own_impact_monitor"]["named_cause"] == "active_set_history_freshness_skew"
    assert digest["active_set"]["live_members_today"] == [
        {
            "wallet": "0x1234567890abcdef",
            "orders": 3,
            "fills": 2,
            "resolved_fills": None,
            "rejects": 1,
            "pnl_usd": 1.25,
            "trigger_watch": {},
        }
    ]
    assert digest["active_set"]["latest_rotation"]["direction_id"] == (
        "2026-07-07T15:19:29Z-fable-post-patch-rotation-trigger"
    )
    assert digest["member_factory"]["kind"] == "member_factory_kpi"
    assert digest["member_factory_kpi"]["queue_depth"]["ready_for_live"] == 2
    assert digest["coverage_gap_diagnosis"]["summary"]["dominant_reason_class"] == "no-eligible-signal"
    assert digest["coverage_gap_diagnosis"]["summary"]["submitted_gap_to_op_volume"] == 40
    assert digest["coverage_gap_signal_supply"]["summary"]["dominant_class"] == "sources_idle"
    assert digest["coverage_gap_signal_supply"]["summary"]["sources_traded_but_unobserved_windows"] == 28
    assert digest["routing_disambiguation"]["summary"]["dominant_class"] == "signal-emitted-but-not-selected"
    assert digest["campaign_lat_p1"]["summary"]["primary_constraint"] == "retention_selection_visibility"
    assert digest["campaign_lat_p1"]["recommended_next_actions"] == [{"id": "D1_RETENTION_PACKET"}]
    assert digest["per_window_pnl_histogram"]["reporting_only"] is True
    assert digest["per_window_pnl_histogram"]["gate_use_allowed"] is False
    assert digest["routing_shadow_validation"]["summary"]["status"] == "ACCUMULATING"
    assert digest["routing_shadow_validation"]["summary"]["extra_would_submit_windows"] == 2
    assert digest["live"]["guard_latency_trigger"]["status"] == "OK"
    assert digest["live"]["guard_latency_trigger"]["scope"] == "current_guard_pid"
    assert digest["live"]["guard_latency_trigger"]["current_pid"] == 10107
    assert digest["live"]["guard_latency_trigger"]["max_consecutive_over_threshold"] == 0
    assert digest["live"]["guard_latency_trigger"]["historical_status"] == "TRIGGERED"
    assert digest["live"]["guard_latency_trigger"]["historical_max_consecutive_over_threshold"] == 3
    assert digest["own_positions"]["redeemable_locked_usd"] == 24.0
    assert digest["own_positions"]["deadman_status"] == "WATCH_REDEEMABLE_LOCKED"
    assert digest["own_redeemer"]["status"] == "DRY_RUN_READY"
    assert digest["own_redeemer"]["candidate_source"] == "ledger_estimate_data_api_unavailable"
    assert digest["wallet_outflow_deadman"]["status"] == "OK"
    assert digest["wallet_outflow_deadman"]["matched_order_outflows"] == 5
    assert digest["wallet_outflow_deadman"]["unmatched_outflows"] == 0
    assert digest["wallet_outflow_deadman"]["consecutive_degraded_fetches"] == 0
    assert digest["wallet_outflow_deadman"]["last_ok_checked_at"] == "2026-07-08T09:31:00Z"
    assert digest["wallet_outflow_deadman"]["fetch_gap_exceeded"] is False
    assert digest["research_disk_deadman"]["status"] == "OK"
    assert digest["research_disk_deadman"]["free_gib"] == 277.5
    assert digest["research_disk_deadman"]["inventory_count"] == 7
    assert digest["research_disk_deadman"]["uninventoried_count"] == 0
    assert digest["research_disk_deadman"]["memory_swap_status"] == "OK"
    assert digest["research_disk_deadman"]["swapfile_count"] == 4
    assert digest["guard_event_log_rotation"]["status"] == "REPAIRED"
    assert digest["guard_event_log_rotation"]["action"] == "rename_archive_reseed_tail"
    assert digest["guard_event_log_rotation"]["size_before_bytes"] == 9904767552
    assert digest["guard_event_log_rotation"]["tail_line_aligned"] is True
    assert digest["alpha_decay_curve"]["five_s"]["verdict"] == "FAST_PATH_BUILD_ORDER_1"
    assert digest["alpha_decay_curve"]["edge_mean_10s"] == 0.012
    assert digest["pnl"]["reconciled_actual_delta_usd"] == 2.05
    assert digest["pnl"]["reconciled_actual_verdict"] == "PRODUCING_RECONCILED_BASIS"
    assert [row["heading"] for row in digest["recent_directions"]] == [
        "## 2026-07-06T21:15Z DIRECTION (Fable, co-operator audit)",
        "## 2026-07-06T21:25Z Fable DIRECTION",
    ]
    assert digest["directions_newer_than_latest_status"][0]["heading"].endswith("fable DIRECTION")
    assert len(digest["directions_newer_than_latest_status"]) == 2
    assert "read this AND full source context" in text
    assert "direction_freshness: FRESH latest_ts=2026-07-06T21:25:00Z" in text
    assert "strategy_map: rows=34 fresh=26 stale=8 active_or_gated=30" in text
    assert (
        "utilization=verdict=IDLE_CAPACITY registry_active=30 "
        "registry_occupancy_pct=None productive=None measured_max=42 idle=12"
    ) in text
    assert "READING_AID_NOT_AUTHORITY_FULL_CONTEXT_REQUIRED" in text
    assert "runtime_selected_member" in text
    assert (
        "admission_wave: status=CONFIGURED direction=2026-07-14T17:04Z-fable-mass-admission-wave "
        "picked=2 admitted=2 runtime_loaded=1 runtime_missing=1 filled=1 runtime_cap=9 aging_gt24h_unruled=0"
    ) in text
    assert "members=2 rolling20_ready=1/2 trigger=False" in text
    assert "total_loss_disabled=1" in text
    assert "candidate_b:0xabcd...abcd:n3:loss3:pnl-6.3" in text
    assert "0xabcd...abcd:n17:pnl-8.5" in text
    assert (
        "live_members_today: 0x1234...cdef orders=3 fills=2 resolved=None rejects=1 "
        "pnl=1.25 signs=None tail=None dist=None"
    ) in text
    assert "active_set_rotation: direction=2026-07-07T15:19:29Z-fable-post-patch-rotation-trigger" in text
    assert "demoted=0x1234...cdef admitted=0xabcd...7890" in text
    assert "QUEUE for codex" in text
    assert "revive polygon_ws shadow" in text
    assert "source=data/research/wallet_copy_live_execution_state.json.summary.can_trade" in text
    assert "polygon_ws_shadow: status=RUNNING pid=12345" in text
    assert "latency_trigger=OK/0 scope=current_guard_pid pid=10107 historical=TRIGGERED/3" in text
    assert "market_facts: exists=True" in text
    assert "own_positions: status=PASS" in text
    assert "own_redeemer: status=DRY_RUN_READY" in text
    assert "wallet_outflow_deadman: status=OK incident=False degraded=0 last_ok=2026-07-08T09:31:00Z" in text
    assert "gap_exceeded=False outflows=5 matched_order=5" in text
    assert "research_disk_deadman: status=OK incident=False free_gib=277.5" in text
    assert "inventory=7 large_files=1 uninventoried=0" in text
    assert "memory_swap=OK swapfiles=4" in text
    assert "guard_log_rotation=status:REPAIRED,action:rename_archive_reseed_tail" in text
    assert "before:9904767552,after:65959556" in text
    assert "alpha_decay_curve: status=PASS" in text
    assert "rule5s=FAST_PATH_BUILD_ORDER_1" in text
    assert "drip_orders=3" in text
    assert "coverage_gap=submitted=104/288 zero=184 dominant=no-eligible-signal gap_to_144=40" in text
    assert "signal_supply=idle=120 traded_unobserved=28 unknown=0 dominant=sources_idle root_cause=sources_idle_dominates" in text
    assert "routing=sampled=20 dominant=signal-emitted-but-not-selected selected=0x5e4a...be6b" in text
    assert (
        "campaign_lat=status=paper/shadow evidence only; no live eligibility loosening from this packet "
        "primary=retention_selection_visibility live_gap=5 trail_gap=0 stage2=INTERIM_NO_FLIP_NOT_FREEZE_OF_RECORD"
    ) in text
    assert (
        "window_pnl=reporting_only:True/gate:False/resolved:2/positive:1/negative:1/zero:0"
        "/buckets:{'-1_to_0': 1, '1_to_5': 1}"
    ) in text
    assert (
        "routing_shadow=status=ACCUMULATING elapsed_h=1.25 would=3 extra=2 parity=PASS "
        "runtime=0x1234...cdef runtime_source=selected_member shadow=0xfedc...4321 "
        "selection_changes=1 coverage=4/6 "
        "accounted=6/6 attrition=fresh:11->fee:3->tox:2->route:2->would:1 "
        "fee_cal=count:5/resolved:2/measured:2/unmeasured:0/pre:1.75/fee:0.2/post:1.55"
    ) in text
    assert "full_universe_copyability: registry=4 scored=4 evidence=2 replay=2 positive_copy=1 ready_queue=1" in text
    assert "legacy_twins=[{'status': 'DEPRECATED_POINTER'" in text
    assert "hot_history_accumulator=status=None events=None" in text
    assert "btc5m_fleet: size=200 matrix_rows=10 matrix_windows=8 wallets_with_rows=3 top50_with_rows=3/50" in text
    assert "btc5m_two_sided_prime: events=12 windows=5 paired=4 pair_sum=2 freq=50.0" in text
    assert "btc5m_morning_table: rows=9 holdout=2 matrix_none=3 top_rank=1 top=copy-1to1-taker" in text
    assert "btc5m_structural_scalp_lane:" in text
    assert "refresh=SKIPPED_SOURCE_MISSING" in text
    assert "data_layer_v1: status=PASS rows=5 files=1/1" in text
    assert "self_feed_duckdb_benchmark: status=PASS rows=270/270 tx_groups=269" in text
    assert "gap=PASS critical=73 gap_cost=99.00432 split=4 price_rounding=15" in text
    assert "overlay_pnl=31.822431 reconciled_actual=6.912425 recommendation=RECONCILIATION_OVERLAY" in text
    assert "h2_external_redemptions: status=PASS rows=10 confirmed=10 redeem_usdc=58.685738" in text
    assert "explained=0.0 unexplained=-7.590385 anchor=CONFIRMED_EXTERNAL_REDEEM" in text
    assert "overlay_source=external_data_api_redeem_condition_join ledger_rewrite=False" in text
    assert "residual_cash_diff_audit: status=PASS_TX_HASH_OR_TIMESTAMP_SKEW_NAMED residual=-19.777051" in text
    assert "scorecard_day_matches=0 baseline_matches=2 other_counterparty=0" in text
    assert "dr_preflight: status=OFF_MACHINE_REMOTE_MISSING remote=False" in text
    assert "experiment_preregistration: status=PASS records=1/1 active=1" in text
    assert "fee_edge_decomposition: verdict=WINNER winners=1" in text
    assert "weekend_shadows=window=74/-6.14/cells_n10=0/HOLDOUT_PENDING" in text
    assert "active_set_rtds_premerge: status=PASS wallets=8 new=12" in text
    assert "event_scheduler=TRIGGER_NEXT_GUARD_CYCLE/True event_sleep=0.0" in text
    assert "event_id=we_scheduler last_event=we_scheduler single_submitter_change=False" in text
    assert "own_impact_monitor: status=STALE_INVENTORY_CAUSE_NAMED cause=active_set_history_freshness_skew" in text
    assert "OP-VOLUME below 144/288" in text
    assert "## 2026-07-06T21:05Z fable DIRECTION" in text
    assert "accept newer direction heading" in text
    assert len(text.splitlines()) <= 100


def test_state_digest_prefers_fresh_current_day_scorecard(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    scripts = tmp_path / "scripts"
    handoff.parent.mkdir(parents=True)
    scripts.mkdir()
    (tmp_path / "docs" / "agents" / "MARKET_FACTS.md").write_text("# Market Facts\n")
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-07T01:32Z DIRECTION (Fable, proactive steering pulse)",
                "- next: current scorecard",
                "## 2026-07-07T01:33Z STATUS [LIVE/SELF-DEV]",
                "- defect | attempts (3+) | next: stale digest | attempts: a,b,c | next: fix",
            ]
        )
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "summary": {
                "can_trade": True,
                "live_orders": 11,
                "filled_orders": 5,
                "rejected_orders": 6,
                "submitted_orders": 0,
                "latest_order_ts": "2026-07-07T02:22:00Z",
            },
            "orders": [],
        },
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "pid": 10106,
            "status": "LIVE_GUARD_RUNNING",
            "live_orders_allowed": True,
            "window_participation": {},
        },
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_2026-07-06.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "day_utc": "2026-07-06",
            "today": {"total": {"pnl_usd": 23.0, "resolved_fills": 84}},
            "since_topup_truth": {
                "primary_verdict": "PRODUCING",
                "canonical_pnl_usd": 2.0,
                "actual_delta_vs_baseline_usd": 1.5,
                "reconciliation_status": "PASS",
            },
            "volume_kpi": {"canonical_daily": {"windows_filled": 67, "windows_submitted": 73, "denominator_windows": 288}},
            "execution_model_kpi": {},
        },
    )
    (scripts / "daily_scorecard.py").write_text(
        "import json\n"
        "print(json.dumps({\n"
        "  'kind': 'wallet_copy_daily_scorecard',\n"
        "  'day_utc': '2026-07-07',\n"
        "  'today': {'total': {'pnl_usd': -19.07944, 'resolved_fills': 12}},\n"
        "  'canonical_pnl_truth': {'by_day': {'2026-07-07': {'pnl_usd': -21.5, 'resolved_fills': 13, 'payout_fill_count': 0, 'in_band_resolved_fill_count': 5, 'in_band_payout_fill_count': 0, 'in_band_winner_binomial_lower_tail_p_value': 0.17063983, 'in_band_holdout_live_status': 'WATCH'}}},\n"
        "  'since_topup_truth': {\n"
        "    'primary_verdict': 'NOT_PRODUCING',\n"
        "    'canonical_pnl_usd': -16.616471,\n"
        "    'actual_delta_vs_baseline_usd': -15.06564,\n"
        "    'reconciliation_status': 'PASS'\n"
        "  },\n"
        "  'volume_kpi': {'canonical_daily': {'windows_filled': 11, 'windows_submitted': 11, 'denominator_windows': 288}},\n"
        "  'defense_regret': {\n"
        "    'status': 'PARTIAL_OPEN_DAY',\n"
        "    'actual_probe_capped_pnl_usd': -21.5,\n"
        "    'raw_standard_cap_post_fee_pnl_usd': 1.25,\n"
        "    'fill_realism_haircut': {'post_fee_pnl_usd': -0.75},\n"
        "    'defense_flipped_sign': False,\n"
        "    'two_consecutive_sign_flips': False,\n"
        "    'framework_audit_auto_pull': False\n"
        "  },\n"
        "  'peer_active_idle_windows': {\n"
        "    'status': 'CLEAR', 'peer_active_idle_windows': 4,\n"
        "    'consecutive_peer_active_idle_windows': 0,\n"
        "    'incident_threshold_windows': 3, 'incident_triggered': False\n"
        "  },\n"
        "  'execution_model_kpi': {}\n"
        "}))\n"
    )

    digest, text = build_digest(tmp_path)

    assert digest["pnl"]["day_pnl_usd"] == -21.5
    assert digest["pnl"]["day_resolved_fills"] == 13
    assert digest["pnl"]["day_payout_fill_count"] == 0
    assert digest["pnl"]["day_in_band_resolved_fill_count"] == 5
    assert digest["pnl"]["day_in_band_payout_fill_count"] == 0
    assert digest["pnl"]["day_in_band_winner_binomial_lower_tail_p_value"] == 0.17063983
    assert digest["pnl"]["since_topup_verdict"] == "NOT_PRODUCING"
    assert digest["pnl"]["since_topup_canonical_pnl_usd"] == -16.616471
    assert digest["since_topup_identity"]["scope"] == "current_steering_metric"
    assert digest["since_topup_identity"]["baseline_usd"] is None
    assert digest["since_topup_identity"]["actual_delta_vs_baseline_usd"] == -15.06564
    assert digest["closed_daily"]["day_utc"] == "2026-07-06"
    assert digest["closed_daily"]["scope"] == "historical_closed_day_not_current_steering_metric"
    assert digest["closed_daily"]["current_steering_metric"] is False
    assert digest["closed_daily"]["pnl_usd"] == 23.0
    assert digest["closed_daily"]["windows_filled"] == 67
    assert digest["defense_regret"]["fill_realism_haircut"]["post_fee_pnl_usd"] == -0.75
    assert "defense_regret=status:PARTIAL_OPEN_DAY,actual:-21.5,raw_standard:1.25,haircut_standard:-0.75" in text
    assert "peer_active_idle=status:CLEAR,total:4,consecutive:0/3,incident:False" in text
    assert "verdict=NOT_PRODUCING" in text
    assert "payout_fills=0 in_band_winners=0/5 in_band_lower_tail_p=0.17063983 in_band_status=WATCH" in text
    assert "closed_daily: scope=historical_closed_day_not_current_steering_metric day=2026-07-06 pnl=23.0" in text


def test_state_digest_volume_uses_rolling_participation_when_cycle_aggregate_resets(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "MARKET_FACTS.md").write_text("# Market Facts\n")
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-09T12:18Z fable DIRECTION [LIVE/LEARN]",
                "- next: inventory trace",
                "## 2026-07-09T12:20Z codex STATUS [LIVE/LEARN]",
                "- defect | [LIVE] volume split | attempts: a,b,c | next=fix digest",
            ]
        )
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {"summary": {"can_trade": True, "live_orders": 1, "filled_orders": 0, "rejected_orders": 0}},
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "status": "LIVE_GUARD_RUNNING",
            "live_orders_allowed": True,
            "window_participation": {
                "active_windows": 0,
                "missed_active_windows": 0,
                "consecutive_missed_active_windows": 0,
                "incident_triggered": False,
                "incident_threshold_windows": 2,
                "window_rollups": [
                    {
                        "market_slug": "btc-updown-5m-1783590000",
                        "wallet_eligible_orders": 2,
                        "our_submits": 1,
                        "our_fills": 0,
                        "missed_active_window": False,
                    },
                    {
                        "market_slug": "btc-updown-5m-1783590300",
                        "wallet_eligible_orders": 3,
                        "our_submits": 0,
                        "our_fills": 0,
                        "missed_active_window": True,
                    },
                    {
                        "market_slug": "btc-updown-5m-1783590600",
                        "wallet_eligible_orders": 1,
                        "our_submits": 0,
                        "our_fills": 0,
                        "missed_active_window": True,
                    },
                ],
            },
        },
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_test.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "today": {"total": {"pnl_usd": 0.0, "resolved_fills": 0}},
            "since_topup_truth": {"primary_verdict": "NOT_PRODUCING"},
            "volume_kpi": {"canonical_daily": {"windows_filled": 0, "windows_submitted": 1, "denominator_windows": 288}},
            "execution_model_kpi": {},
        },
    )

    digest, text = build_digest(tmp_path)

    assert digest["volume"]["active_windows"] == 0
    assert digest["volume"]["missed_active_windows"] == 0
    assert digest["volume"]["consecutive_missed_active_windows"] == 0
    assert digest["volume"]["incident_triggered"] is False
    assert digest["volume"]["participation_basis"] == "window_participation_current_generation"
    assert digest["volume"]["current_generation"]["active_windows"] == 0
    assert digest["volume"]["rolling_288"]["active_windows"] == 3
    assert digest["volume"]["rolling_288"]["consecutive_missed_active_windows"] == 2
    assert "raw_current_active=0 raw_missed=0 raw_consecutive=0" in text
    assert "incident=False" in text


def test_state_digest_recent_participation_uses_newest_first_rollups(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "MARKET_FACTS.md").write_text("# Market Facts\n")
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-09T12:18Z fable DIRECTION [LIVE/LEARN]",
                "- next: hold shadow",
                "## 2026-07-09T12:20Z codex STATUS [LIVE/LEARN]",
                "- actual_live_trading=igen",
            ]
        )
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "summary": {"can_trade": True, "live_orders": 3, "filled_orders": 2, "rejected_orders": 1},
            "orders": [
                {"market_slug": "btc-updown-5m-10", "status": "FILLED"},
                {"market_slug": "btc-updown-5m-10", "status": "REJECTED"},
                {"market_slug": "btc-updown-5m-9", "status": "FILLED"},
            ],
        },
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "status": "LIVE_GUARD_RUNNING",
            "live_orders_allowed": True,
            "window_participation": {
                "active_windows": 10,
                "missed_active_windows": 0,
                "consecutive_missed_active_windows": 0,
                "incident_triggered": False,
                "window_rollups": [
                    {
                        "market_slug": f"btc-updown-5m-{10 - idx}",
                        "wallet_eligible_orders": idx + 1,
                        "our_submits": 1,
                        "our_fills": 1,
                        "window_start_s": float(10 - idx),
                    }
                    for idx in range(10)
                ],
            },
        },
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_test.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "today": {"total": {"pnl_usd": 0.0, "resolved_fills": 0}},
            "since_topup_truth": {"primary_verdict": "NOT_PRODUCING"},
            "volume_kpi": {"canonical_daily": {"windows_filled": 0, "windows_submitted": 1, "denominator_windows": 288}},
            "execution_model_kpi": {},
        },
    )

    digest, text = build_digest(tmp_path)

    recent = digest["volume"]["recent_participation_rows"]
    assert [row["market_slug"] for row in recent] == [f"btc-updown-5m-{idx}" for idx in range(10, 2, -1)]
    assert recent[0]["our_submits"] == 2
    assert recent[0]["our_fills"] == 1
    assert recent[0]["our_rejects"] == 1
    assert recent[0]["rollup_our_submits"] == 1
    assert recent[2]["our_submits"] == 1
    assert recent[2]["our_fills"] == 1
    assert "recent_participation: btc-updown-5m-10" in text
    assert "btc-updown-5m-10 w=1 s=2 f=1 r=1" in text
    assert "btc-updown-5m-3" in text
    assert "btc-updown-5m-2" not in text


def test_authoritative_wide_standby_overrides_missing_scorecard_clock() -> None:
    pipeline = {
        "standby_ready": {
            "seat": {
                "status": "CLOCK_OR_SOURCE_BINDING_MISSING",
                "source_binding_status": None,
            }
        }
    }
    artifact = {
        "execution_status": "EXECUTED",
        "binding": {
            "wallet": "0x82c8",
            "source_binding_status": "WIRED",
            "source_binding_id": "widebind_1",
            "standby_evidence_started_at": "2026-07-29T06:44:50Z",
            "standby_evidence_elapsed_h": 0.25,
            "standby_evidence_minimum_h": 48.0,
            "resolved_paper_fills": 2,
            "promotion_resolved_fill_gate": 30,
            "in_lane_post_fee_pnl_usd": 0.5,
            "next": "accrue",
            "terminal_outcome_on_deadline": {
                "status": "PARK_SEAT_UNFED_CLOCK",
                "terminal": True,
            },
        },
    }

    merged = update_state_digest._merge_authoritative_wide_standby(
        pipeline,
        artifact,
        {
            "lane_present": False,
            "resolved_paper_fills": None,
        },
    )

    assert merged["standby_ready"]["seat"]["source_binding_status"] == "WIRED"
    assert merged["standby_ready"]["seat"]["clock_or_source_binding_missing"] is False
    assert merged["standby_ready"]["seat"]["status"] == "ACCRUING_RED_DIVERGENT"
    assert merged["seat_clock_divergence"] == {
        "binding_resolved": 2,
        "lane_resolved": None,
        "resolved_delta": None,
        "binding_elapsed_h": 0.25,
        "digest_elapsed_h": None,
        "elapsed_delta_h": None,
        "lane_present": False,
        "status": "ACCRUING_RED_DIVERGENT",
    }
    assert pipeline["standby_ready"]["seat"]["source_binding_status"] is None


def test_authoritative_wide_standby_park_committed_is_terminal_truth() -> None:
    pipeline = {
        "standby_ready": {
            "seat": {
                "wall_clock_elapsed_h": 27.72,
                "resolutions_attempted": 0,
                "resolution_attempt_taxonomy": {},
                "attempt_log_retention": {
                    "status": "NO_RETAINED_ATTEMPT_LOG_FOR_WINDOW"
                },
                "attempt_window_start": "2026-07-29T06:44:50Z",
                "attempt_window_end": "2026-07-31T06:45:02.613320Z",
            }
        }
    }
    artifact = {
        "execution_status": "PARK_COMMITTED",
        "binding": {
            "wallet": "0x82c8",
            "source_binding_status": "WIRED",
            "standby_evidence_started_at": "2026-07-29T06:44:50Z",
            "standby_evidence_elapsed_h": 0.0,
            "resolved_paper_fills": 0,
            "promotion_resolved_fill_gate": 30,
            "terminal_executed_at": "2026-07-31T06:45:02.613320Z",
            "terminal_outcome_on_deadline": {
                "status": "PARK_SEAT_UNFED_CLOCK",
                "terminal": True,
                "stop_writer": True,
            },
        },
    }

    merged = update_state_digest._merge_authoritative_wide_standby(
        pipeline,
        artifact,
        {"lane_present": True, "resolved_paper_fills": 0},
    )

    seat = merged["standby_ready"]["seat"]
    assert seat["status"] == "PARK_SEAT_UNFED_CLOCK_COMMITTED"
    assert seat["terminal_executed_at"] == "2026-07-31T06:45:02.613320Z"
    assert seat["next_action"] == (
        "none; terminal park committed at 2026-07-31T06:45:02.613320Z"
    )
    assert seat["projected_resolved_at_48h"] == 0.0
    assert seat["resolutions_attempted"] == 0
    assert seat["attempt_log_retention"]["status"] == "NO_RETAINED_ATTEMPT_LOG_FOR_WINDOW"
    assert seat["attempt_window_end"] == "2026-07-31T06:45:02.613320Z"
    assert seat["admission_forecast"] is False
    assert seat["terminal_outcome_on_deadline"]["status"] == "PARK_SEAT_UNFED_CLOCK"


def test_state_digest_inventory_trace_prefers_starvation_packet(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "MARKET_FACTS.md").write_text("# Market Facts\n")
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-09T12:18Z fable DIRECTION [LIVE/LEARN]",
                "- next: inventory trace",
                "## 2026-07-09T12:25Z codex STATUS [LIVE/LEARN]",
                "- trace pending",
            ]
        )
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {"summary": {"can_trade": True, "live_orders": 0, "filled_orders": 0, "rejected_orders": 0}},
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {"status": "LIVE_GUARD_RUNNING", "live_orders_allowed": True},
    )
    _write_json(
        data / "active_set_starvation_packet_latest.json",
        {
            "generated_at": "2026-07-09T12:27:00Z",
            "summary": {
                "eligible_intents_24h": 7,
                "late_suppressed_intents_24h": 11,
                "pass_to_late_transitions_24h": 0,
                "pass_to_terminal_resnapshot_24h": 2,
                "unique_intents_24h": 13,
            },
            "inventory_skip_lifecycle_trace": {
                "measure_only": True,
                "record_count": 2,
                "recoverable_intent_estimate": 1,
                "skip_reason_counts_24h": {
                    "inventory_best_ask_above_vwap_plus_buffer": 3,
                    "inventory_best_ask_missing": 5,
                },
                "skip_reason_counts_24h_by_wallet": {
                    "0x1111111111111111111111111111111111111111": {
                        "inventory_best_ask_missing": 5,
                    }
                },
                "sample_traces": [{"intent_id": "ci_a"}],
            },
        },
    )
    _write_json(
        data / "inventory_skip_lifecycle_trace_latest.json",
        {
            "generated_at": "2026-07-09T12:20:00Z",
            "live_path_mutated": True,
            "summary": {
                "c4": {"eligible_intents_24h": 999},
                "inventory_skip_total": 99,
                "recoverable_inventory_skip_total": 88,
            },
        },
    )

    digest, text = build_digest(tmp_path)

    trace = digest["inventory_skip_lifecycle"]
    assert trace["source"] == "active_set_starvation_packet"
    assert trace["generated_at"] == "2026-07-09T12:27:00Z"
    assert trace["measure_only"] is True
    assert trace["live_path_mutated"] is False
    assert trace["record_count"] == 2
    assert trace["recoverable_intent_estimate"] == 1
    assert trace["skip_reason_counts_24h"]["inventory_best_ask_missing"] == 5
    assert trace["skip_reason_counts_24h_by_wallet"]["0x1111111111111111111111111111111111111111"] == {
        "inventory_best_ask_missing": 5,
    }
    assert trace["c4"]["eligible_intents_24h"] == 7
    assert trace["c4"]["unique_intents_24h"] == 13
    assert trace["inventory_skip_total"] == 99
    assert "source=active_set_starvation_packet records=2 recoverable_intents=1" in text


def test_state_digest_treats_daily_heading_as_status() -> None:
    entries = [
        {"heading": "## 2026-07-08T00:52Z fable DIRECTION", "body": "- next: act"},
        {"heading": "## 2026-07-08T00:58Z DAILY [LIVE/SELF-DEV]", "body": "- daily close"},
    ]

    latest = update_state_digest._latest_status_entry(entries)

    assert latest["heading"] == "## 2026-07-08T00:58Z DAILY [LIVE/SELF-DEV]"


def test_state_digest_treats_codex_daily_status_heading_as_status() -> None:
    entries = [
        {"heading": "## 2026-07-08T00:52Z codex STATUS [LIVE]", "body": "- old"},
        {"heading": "## 2026-07-08T00:58Z codex DAILY STATUS [LIVE/SELF-DEV]", "body": "- daily close"},
    ]

    latest = update_state_digest._latest_status_entry(entries)

    assert latest["heading"] == "## 2026-07-08T00:58Z codex DAILY STATUS [LIVE/SELF-DEV]"


def test_state_digest_does_not_treat_direction_audit_heading_as_status() -> None:
    entries = [
        {"heading": "## 2026-07-08T07:47Z codex STATUS [LIVE/LEARN/SELF-DEV]", "body": "- defect | attempts (3+) | next: x"},
        {"heading": "## 2026-07-08T07:52Z fable DIRECTION [HEARTBEAT AUDIT — 07:47Z CODEX STATUS]", "body": "- NEXT: hold"},
    ]

    latest = update_state_digest._latest_status_entry(entries)

    assert latest["heading"] == "## 2026-07-08T07:47Z codex STATUS [LIVE/LEARN/SELF-DEV]"


def test_state_digest_latest_direction_uses_heading_timestamp_across_mixed_file_order() -> None:
    entries = [
        {"heading": "## 2026-07-08T12:15Z fable DIRECTION [LIVE]", "body": "- NEXT: newest"},
        {"heading": "## 2026-07-08T12:13Z fable DIRECTION [LIVE]", "body": "- NEXT: older-appended-late"},
    ]

    latest = update_state_digest._latest_fable_direction(entries)
    recent = update_state_digest._recent_entries(
        entries,
        update_state_digest._is_fable_direction,
        limit=2,
    )

    assert latest["heading"] == "## 2026-07-08T12:15Z fable DIRECTION [LIVE]"
    assert [row["heading"] for row in recent] == [
        "## 2026-07-08T12:15Z fable DIRECTION [LIVE]",
        "## 2026-07-08T12:13Z fable DIRECTION [LIVE]",
    ]


def test_state_digest_latest_direction_respects_newest_first_handoff_order() -> None:
    entries = [
        {"heading": "## 2026-07-24T11:05Z fable DIRECTION [LIVE]", "body": "- NEXT: actual newest"},
        {"heading": "## 2026-07-24T10:38Z fable DIRECTION [LIVE]", "body": "- NEXT: prior"},
        {
            "heading": "## 2026-07-24T12:28Z fable DIRECTION [LIVE]",
            "body": "- NEXT: older entry with malformed future timestamp",
        },
    ]

    latest = update_state_digest._latest_fable_direction(entries)

    assert latest["heading"] == "## 2026-07-24T11:05Z fable DIRECTION [LIVE]"


def test_state_digest_latest_direction_detects_append_era_after_rolled_head() -> None:
    entries = [
        {"heading": "## 2026-07-25T03:13Z fable DIRECTION [LIVE]", "body": "- NEXT: rolled head"},
        {"heading": "## 2026-07-25T03:01Z fable DIRECTION [LIVE]", "body": "- NEXT: prior"},
        {"heading": "## 2026-07-25T03:35Z fable DIRECTION [LIVE]", "body": "- NEXT: append one"},
        {"heading": "## 2026-07-25T05:45Z fable DIRECTION [LIVE]", "body": "- NEXT: actual newest"},
    ]

    latest = update_state_digest._latest_fable_direction(entries)

    assert latest["heading"] == "## 2026-07-25T05:45Z fable DIRECTION [LIVE]"


def test_state_digest_latest_direction_detects_status_then_appended_direction() -> None:
    entries = [
        {"heading": "## 2026-07-27T10:29Z fable DIRECTION [LIVE]", "body": "- NEXT: rolled head"},
        {"heading": "## 2026-07-27T10:19Z fable DIRECTION [LIVE]", "body": "- NEXT: prior"},
        {"heading": "## 2026-07-27T10:38Z codex STATUS [LIVE]", "body": "- escalation"},
        {"heading": "## 2026-07-27T10:42Z fable DIRECTION [LIVE]", "body": "- NEXT: appended ruling"},
    ]

    latest = update_state_digest._latest_fable_direction(entries)

    assert latest["heading"] == "## 2026-07-27T10:42Z fable DIRECTION [LIVE]"


def test_state_digest_latest_direction_uses_append_order_after_isolated_clock_skew() -> None:
    entries = [
        {"heading": "## 2026-08-02T17:00Z fable DIRECTION [LIVE]", "body": "- NEXT: one"},
        {"heading": "## 2026-08-02T17:30Z fable DIRECTION [LIVE]", "body": "- NEXT: two"},
        {"heading": "## 2026-08-02T18:00Z fable DIRECTION [LIVE]", "body": "- NEXT: three"},
        {
            "heading": "## 2026-08-02T20:35Z fable DIRECTION [LIVE]",
            "body": "- NEXT: local time stamped with a literal Z",
        },
        {"heading": "## 2026-08-02T18:25Z fable DIRECTION [LIVE]", "body": "- NEXT: corrected UTC"},
        {"heading": "## 2026-08-02T18:53Z fable DIRECTION [LIVE]", "body": "- NEXT: actual newest"},
    ]

    latest = update_state_digest._latest_fable_direction(entries)

    assert latest["heading"] == "## 2026-08-02T18:53Z fable DIRECTION [LIVE]"


def test_state_digest_direction_next_accepts_directions_heading() -> None:
    body = (
        "- DIRECTIONS:\n"
        "  - 1. Maintain current live matrix unchanged.\n"
        "  - 2. Let the shadow lane accumulate.\n"
        "- warnings: none.\n"
    )

    assert update_state_digest._direction_next_block(body) == [
        "- DIRECTIONS:",
        "  - 1. Maintain current live matrix unchanged.",
        "  - 2. Let the shadow lane accumulate.",
    ]


def test_status_day_pnl_reads_compact_ledger_delta_bullet() -> None:
    body = (
        "- ledger_delta [LIVE]: since prior, 17 rows; resolved delta -$5.199997; "
        "day +$9.353642.\n"
        "- participation [LIVE]: day=82 windows."
    )

    assert update_state_digest._status_pnl_day_usd(body) == 9.353642


def test_state_digest_latest_direction_includes_fable_order_addendum() -> None:
    entries = [
        {"heading": "## 2026-07-08T12:15Z fable DIRECTION [LIVE]", "body": "- NEXT: first"},
        {
            "heading": "## 2026-07-08T12:18Z fable ORDER ADDENDUM [LIVE, W6]",
            "body": "- ORDER: rebind Monday auto-return to W1 bench list.",
        },
    ]

    latest = update_state_digest._latest_fable_direction(entries)
    recent = update_state_digest._recent_entries(
        entries,
        update_state_digest._is_fable_direction,
        limit=2,
    )

    assert latest["heading"] == "## 2026-07-08T12:18Z fable ORDER ADDENDUM [LIVE, W6]"
    assert [row["heading"] for row in recent] == [
        "## 2026-07-08T12:15Z fable DIRECTION [LIVE]",
        "## 2026-07-08T12:18Z fable ORDER ADDENDUM [LIVE, W6]",
    ]


def test_state_digest_latest_direction_includes_fable_orders_and_plain_addendum() -> None:
    entries = [
        {"heading": "## 2026-07-22T18:19Z fable DIRECTION [LIVE]", "body": "- next: first"},
        {"heading": "## 2026-07-22T18:40Z audit + fable ORDERS", "body": "- ORDER O3: daemonize feed"},
        {"heading": "## 2026-07-22T18:55Z fable ADDENDUM [priority]", "body": "- PRIORITY: O3 > O1"},
    ]

    latest = update_state_digest._latest_fable_direction(entries)
    recent = update_state_digest._recent_entries(
        entries,
        update_state_digest._is_fable_direction,
        limit=3,
    )

    assert latest["heading"] == "## 2026-07-22T18:55Z fable ADDENDUM [priority]"
    assert [row["heading"] for row in recent] == [entry["heading"] for entry in entries]


def test_state_digest_renders_latest_operator_order(tmp_path: Path) -> None:
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-08T12:15Z fable DIRECTION [LIVE]",
                "- NEXT: keep live lane running.",
                "",
                "## 2026-07-08T12:17Z operator ORDER (verbatim, via Fable)",
                "- GREEN means live lanes profit.",
            ]
        )
    )

    digest, text = build_digest(tmp_path)

    assert digest["latest_direction"] == "## 2026-07-08T12:15Z fable DIRECTION [LIVE]"
    assert (
        digest["latest_operator_order"]
        == "## 2026-07-08T12:17Z operator ORDER (verbatim, via Fable)"
    )
    assert digest["latest_operator_order_ts"] == "2026-07-08T12:17:00Z"
    assert (
        "latest_operator_order: ## 2026-07-08T12:17Z operator ORDER (verbatim, via Fable)"
        in text
    )


def test_state_digest_treats_operator_pin_as_operator_marker(tmp_path: Path) -> None:
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text(
        "## 2026-07-13T18:12Z operator PIN (verbatim intent, via Fable)\n"
        "- zero-AI backup.\n"
    )

    digest, text = build_digest(tmp_path)

    assert digest["latest_operator_order"] == "## 2026-07-13T18:12Z operator PIN (verbatim intent, via Fable)"
    assert digest["latest_operator_order_ts"] == "2026-07-13T18:12:00Z"
    assert "latest_operator_order: ## 2026-07-13T18:12Z operator PIN" in text


def test_state_digest_treats_operator_handover_as_operator_marker(tmp_path: Path) -> None:
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text(
        "## 2026-07-14T17:56Z operator HANDOVER (verbatim intent, via Fable)\n"
        "- autonomous mode.\n"
    )

    digest, text = build_digest(tmp_path)

    assert (
        digest["latest_operator_order"]
        == "## 2026-07-14T17:56Z operator HANDOVER (verbatim intent, via Fable)"
    )
    assert digest["latest_operator_order_ts"] == "2026-07-14T17:56:00Z"
    assert "latest_operator_order: ## 2026-07-14T17:56Z operator HANDOVER" in text


def test_state_digest_treats_operator_preference_as_material_operator_marker(tmp_path: Path) -> None:
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text(
        "## 2026-07-20T18:05Z operator PREFERENCE (via Fable) — OP-SCALE-AXES-PRIORITY\n"
        "- operator intent: lead scaling with parallel seats and\n"
        "  two-sided inventory.\n"
        "- BINDING consequence: multi-seat readiness precedes raw order-size steps.\n"
    )

    digest, text = build_digest(tmp_path)

    assert "operator PREFERENCE" in digest["latest_operator_order"]
    assert digest["latest_operator_order_ts"] == "2026-07-20T18:05:00Z"
    assert digest["latest_operator_order_material"] == [
        "- operator intent: lead scaling with parallel seats and two-sided inventory.",
        "- BINDING consequence: multi-seat readiness precedes raw order-size steps.",
    ]
    assert "multi-seat readiness precedes raw order-size steps" in text


def test_state_digest_treats_operator_correction_as_material_operator_marker(tmp_path: Path) -> None:
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text(
        "## 2026-07-21T19:22Z operator ORDER — OP-RND-288\n"
        "- Every window must be profitable.\n\n"
        "## 2026-07-21T19:28Z operator CORRECTION — OP-RND-288 AMENDED\n"
        "- Daily aggregate profit is the target; per-window losses are accepted variance.\n"
    )

    digest, text = build_digest(tmp_path)

    assert "operator CORRECTION" in digest["latest_operator_order"]
    assert digest["latest_operator_order_ts"] == "2026-07-21T19:28:00Z"
    assert digest["latest_operator_order_material"] == [
        "- Daily aggregate profit is the target; per-window losses are accepted variance."
    ]
    assert "Daily aggregate profit is the target" in text


def test_state_digest_surfaces_selected_member_guard_submit_attribution(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "data/research/selected_member_guard_submit_attribution_latest.json",
        {
            "generated_at": "2026-07-22T07:40:00Z",
            "measurement_started_at": "2026-07-22T06:32:00Z",
            "status": "PASS",
            "defect_classification": "NONE",
            "selected_wallet_count": 5,
            "selected_policy_eligible_unique_intents": 76,
            "submitted_intents": 0,
            "telemetry_defects": 0,
            "wiring_defects": 0,
            "terminal_stage_counts": {"toxicity_protection": 23},
            "live_mutation": False,
        },
    )

    digest, text = build_digest(tmp_path)

    attribution = digest["selected_member_guard_submit_attribution"]
    assert attribution["defect_classification"] == "NONE"
    assert attribution["selected_policy_eligible_unique_intents"] == 76
    assert "selected_member_guard_submit_attribution: status=PASS defect=NONE" in text
    assert "eligible=76 submitted=0 telemetry=0 wiring=0" in text


def test_state_digest_surfaces_e1_reject_count_and_288_map_summary(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / "research"
    data_dir.mkdir(parents=True)
    (data_dir / "e1_framework_audit_inputs_2026-07-21.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-21T19:30:00Z",
                "day_utc": "2026-07-21",
                "defense_regret": {
                    "actual_probe_capped_pnl_usd": -6.179874,
                    "defense_flipped_sign": False,
                },
                "reject_cluster": {
                    "ledger_newest_submitted_at": "2026-07-21T19:25:00Z",
                    "reject_rows": 10,
                    "taxonomy_counts": {"fak_no_match": 5, "policy_cap_maker_fallback": 5},
                    "ruled_ceiling_refused_by_tighter_cap_rejects": 5,
                },
                "full_utc_day_reject_cluster": {
                    "reject_rows": 20,
                    "taxonomy_counts": {
                        "fak_no_match": 2,
                        "policy_cap_maker_fallback": 18,
                    },
                    "ruled_ceiling_refused_by_tighter_cap_rejects": 18,
                },
                "participation_288_map_summary": {"traded_windows": 30},
                "daily_gate_conversion": {
                    "policy_eligible_signals_in": 100,
                    "accounting_gap": 0,
                },
                "highest_rejection_gate_ev": {
                    "gate": "profit_latency_suppression",
                    "paper_counterfactual_roi_pct": -2.5,
                },
                "multi_day_roi_distribution": {
                    "days_with_cost": 17,
                    "distribution": {"aggregate_roi_pct": 0.15, "worst_daily_roi_pct": -27.2},
                },
            }
        )
    )
    (data_dir / "btc5m_288_participation_map_2026-07-21.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-21T19:30:00Z",
                "day_utc": "2026-07-21",
                "summary": {
                    "traded_windows": 30,
                    "elapsed_windows": 231,
                    "active_windows": 1,
                    "future_pending_windows": 56,
                    "daily_resolved_pnl_usd": -4.117129,
                    "win_rate_reporting_only_pct": 37.931034,
                },
                "hour_band_aggregates": [],
            }
        )
    )

    digest, text = build_digest(tmp_path)

    assert digest["e1_framework_audit_inputs"]["reject_cluster"]["reject_rows"] == 10
    assert digest["e1_framework_audit_inputs"]["defense_regret"] == {
        "actual_probe_capped_pnl_usd": -6.179874,
        "defense_flipped_sign": False,
    }
    assert digest["participation_288_map"]["summary"]["traded_windows"] == 30
    assert "ruled_ceiling_refused:5" in text
    assert "full_day_rejects:20" in text
    assert "full_day_taxonomy:{'fak_no_match': 2, 'policy_cap_maker_fallback': 18}" in text
    assert "actual:-6.179874,sign_flip:False" in text
    assert "gate_in:100,gate_gap:0,highest_gate:profit_latency_suppression" in text
    assert "days:17,multi_roi:0.15,worst_roi:-27.2" in text
    assert "map288=traded:30/288" in text


def test_state_digest_refuses_stale_e1_reject_cluster(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / "research"
    data_dir.mkdir(parents=True)
    (data_dir / "e1_framework_audit_inputs_2026-07-30.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-30T00:30:00Z",
                "day_utc": "2026-07-30",
                "reject_cluster": {
                    "ledger_newest_submitted_at": "2026-07-30T00:20:00Z",
                    "reject_rows": 5,
                    "taxonomy_counts": {"policy_cap_maker_fallback": 5},
                    "ruled_ceiling_refused_by_tighter_cap_rejects": 5,
                },
            }
        )
    )
    (data_dir / "wallet_copy_live_execution_state.json").write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "intent_id": "new-ledger-row",
                        "submitted_at": "2026-07-30T00:21:00Z",
                        "final_status": "FILLED",
                    }
                ]
            }
        )
    )
    (data_dir / "order_flow_deadman_state.json").write_text(
        json.dumps({"status": "INCIDENT_POLICY_CHOKE", "can_trade": False})
    )

    digest, text = build_digest(tmp_path)

    e1 = digest["e1_framework_audit_inputs"]
    assert e1["freshness_status"] == "STALE_ARTEFACT_LEDGER_LAG"
    assert e1["ledger_lag_s"] == 60.0
    assert "e1_inputs=STALE_ARTEFACT_LEDGER_LAG" in text
    assert "artifact_ledger_cut:2026-07-30T00:20:00+00:00" in text
    assert "live_ledger_cut:2026-07-30T00:21:00+00:00" in text
    assert "ledger_lag_s:60.0" in text
    assert e1["live_ledger_cut_frozen_by"] == "ORDER_FLOW_DEAD"
    assert "live_ledger_cut_frozen_by:ORDER_FLOW_DEAD" in text
    assert "policy_cap_maker_fallback" not in text
    assert "ruled_ceiling_refused:" not in text


def test_state_digest_surfaces_policy_family_terminal_registry(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / "research"
    data_dir.mkdir(parents=True)
    (data_dir / "wide_policy_family_terminal_registry_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-30T11:20:00Z",
                "paper_only": True,
                "live_orders_allowed": False,
                "entries": [
                    {
                        "policy_family": "fast_family",
                        "wide_policy_fingerprint": "fingerprint",
                        "status": "REGISTERED_ACTIVE",
                        "terminal": False,
                        "stop_writer": False,
                        "refuse_alias_reregistration": True,
                        "effective_at": "2026-07-31T08:33:31Z",
                        "reason_source": [],
                        "precommitted_negative_outcomes": {"full_sample": {}},
                        "precommitted_nonnegative_outcomes": {
                            "insufficient_sample": (
                                "MEASURED_INADMISSIBLE_INSUFFICIENT_SAMPLE"
                            )
                        },
                    }
                ],
            }
        )
    )

    digest, text = build_digest(tmp_path)

    entry = digest["policy_family_terminal_registry"]["entries"][0]
    assert entry["policy_family"] == "fast_family"
    assert entry["wide_policy_fingerprint"] == "fingerprint"
    assert entry["status"] == "REGISTERED_ACTIVE"
    assert entry["terminal"] is False
    assert entry["refuse_alias_reregistration"] is True
    assert entry["precommitted_nonnegative_outcomes"][
        "insufficient_sample"
    ] == "MEASURED_INADMISSIBLE_INSUFFICIENT_SAMPLE"
    assert "family_terminal_registry=" in text
    assert "'status': 'REGISTERED_ACTIVE'" in text


def test_state_digest_surfaces_wallet_951b_forward_only_lane(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        update_state_digest,
        "_utc_now_iso",
        lambda: "2026-07-30T12:38:31Z",
    )
    _write_json(
        tmp_path / "data/research/951b_forward_only_lane_latest.json",
        {
            "generated_at": "2026-07-30T12:37:31Z",
            "registered_at": "2026-07-30T12:35:30Z",
            "observation_deadline_at": "2026-07-31T12:35:30Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "wallet": "0x951bd740ef681d05891ca35440232488271d433e",
            "wide_policy_fingerprint": "2d8af91d",
            "forward_n": 0,
            "forward_post_fee_pnl_usd": 0.0,
            "checks": {"forward_n_gte_200": False},
            "admission_eligible": False,
            "live_authority": False,
        },
    )

    digest, text = build_digest(tmp_path)

    lane = digest["wallet_951b_forward_only_lane"]
    assert lane["wallet"] == "0x951bd740ef681d05891ca35440232488271d433e"
    assert lane["observation_deadline_at"] == "2026-07-31T12:35:30Z"
    assert lane["forward_n"] == 0
    assert lane["paper_only"] is True
    assert lane["live_authority"] is False
    assert digest["forward_lane_digest_lag_s"] == 60.0
    assert digest["forward_lane_digest_lag_status"] == "PASS"
    assert digest["forward_lane_digest_generated_at"] == "2026-07-30T12:38:31Z"
    assert digest["forward_lane_newest_generated_at"] == "2026-07-30T12:37:31Z"
    assert "forward_lane_digest_lag_s=60.0" in text
    assert "digest_generated_at=2026-07-30T12:38:31Z" in text
    assert "wallet_951b_forward_only=registered=2026-07-30T12:35:30Z" in text


def test_state_digest_names_stale_pipeline_slo_generator_artifact(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "data/research/pipeline_slo_and_standby_readiness_latest.json"
    generator = tmp_path / "scripts/report_pipeline_slo.py"
    _write_json(
        artifact,
        {
            "generated_at": datetime.now(UTC).isoformat(),
            "pipeline_slo": {"breach_count": 2, "stages": []},
            "standby_ready": {},
        },
    )
    _write_json(
        tmp_path / "data/research/codex_refresh_cadence_state.json",
        {
            "producer_path": "scripts/codex_refresh_cadence.sh",
            "last_cycle_start_at": "2026-08-03T01:40:00Z",
            "cycle_period_s_observed": 901.0,
            "cycle_period_status": "PASS_WITHIN_2X_DECLARED",
        },
    )
    generator.parent.mkdir(parents=True)
    generator.write_text("# generator\n")
    os.utime(artifact, (10, 10))
    os.utime(generator, (20, 20))

    stale, stale_text = build_digest(tmp_path)

    pipeline = stale["pipeline_slo_and_standby_readiness"]
    assert pipeline["freshness_status"] == "SLO_ARTIFACT_PREDATES_GENERATOR"
    assert "pipeline_slo=freshness:SLO_ARTIFACT_PREDATES_GENERATOR" in stale_text

    os.utime(artifact, (30, 30))
    current, current_text = build_digest(tmp_path)

    assert "freshness_status" not in current["pipeline_slo_and_standby_readiness"]
    assert "pipeline_slo=freshness:None" in current_text

    payload = json.loads(artifact.read_text())
    payload["generated_at"] = (
        datetime.now(UTC) - timedelta(hours=2.1)
    ).isoformat()
    _write_json(artifact, payload)
    aged, aged_text = build_digest(tmp_path)

    pipeline = aged["pipeline_slo_and_standby_readiness"]
    assert pipeline["freshness_status"] == "SLO_ARTIFACT_STALE_AGE"
    assert pipeline["artifact_age_h"] > 2.0
    assert "pipeline_slo=freshness:SLO_ARTIFACT_STALE_AGE,age_h:" in aged_text
    assert "artifact:data/research/pipeline_slo_and_standby_readiness_latest.json" in aged_text
    assert "producer:scripts/report_pipeline_slo.py" in aged_text
    assert "cadence_producer:scripts/codex_refresh_cadence.sh" in aged_text
    assert "cycle_period_s:901.0" in aged_text
    assert pipeline["cycle_period_status"] == "PASS_WITHIN_2X_DECLARED"


def test_state_digest_surfaces_positive_wallet_slice_falsifier(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / "research"
    data_dir.mkdir(parents=True)
    (data_dir / "positive_wallet_slice_selector_falsifier_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-30T01:30:00Z",
                "verdict": "MIXED_NO_PREREGISTERED_THRESHOLD",
                "sign_inversion_wallets": ["0x951b"],
                "rows": [
                    {
                        "wallet": "0x951b",
                        "unsliced": {
                            "post_fee_pnl_usd": 199.914165,
                            "both_halves_positive": True,
                        },
                        "sliced": {"post_fee_pnl_usd": -3.280643},
                    }
                ],
            }
        )
    )

    digest, text = build_digest(tmp_path)

    report = digest["positive_wallet_slice_selector_falsifier"]
    assert report["verdict"] == "MIXED_NO_PREREGISTERED_THRESHOLD"
    assert report["sign_inversion_wallets"] == ["0x951b"]
    assert (
        "positive_wallet_slice_falsifier: generated_at=2026-07-30T01:30:00Z "
        "verdict=MIXED_NO_PREREGISTERED_THRESHOLD"
    ) in text
    assert "('0x951b', 199.914165, True, -3.280643)" in text


def test_state_digest_surfaces_two_arm_concentration_precommit(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / "research"
    data_dir.mkdir(parents=True)
    (data_dir / "two_arm_concentration_decomposition_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-30T04:00:00Z",
                "execution_status": "NOT_DUE",
                "arms": [
                    {
                        "arm": "31c2_unsliced",
                        "base": {
                            "resolved": 427,
                            "distinct_markets": 54,
                            "top_1_market_share_of_total_pnl_pct": 153.954525,
                            "win_rate_pct": 59.484778,
                        },
                        "min_price_0_10": {"post_fee_pnl_usd": 33.753583},
                        "verdict": {"classification": "LONGSHOT_ARTEFACT"},
                    }
                ],
                "seat_82c8_maturity_precommit": {
                    "decision": "PARK_AT_MATURITY",
                    "third_48h_clock_allowed": False,
                },
            }
        )
    )

    digest, text = build_digest(tmp_path)

    report = digest["two_arm_concentration_decomposition"]
    assert report["execution_status"] == "NOT_DUE"
    assert report["seat_82c8_maturity_precommit"]["decision"] == "PARK_AT_MATURITY"
    assert "two_arm_concentration: generated_at=2026-07-30T04:00:00Z" in text
    assert "('31c2_unsliced', 427, 54, 153.954525" in text
    assert "'third_48h_clock_allowed': False" in text


def test_admission_wave_summary_marks_temporal_partial_runtime() -> None:
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    summary = update_state_digest._admission_wave_summary(
        {
            "latest_admission_wave": {
                "direction_id": "wave-1",
                "status": "RUNTIME_LOADED_NO_WAVE_ORDERS_YET",
                "picked_wallets": [wallet_a, wallet_b],
            },
            "members": [
                {"source_wallet": wallet_a, "enabled": True},
                {"source_wallet": wallet_b, "enabled": True},
            ],
        },
        [{"source_wallet": wallet_a}],
        [],
        {"temporal_slice_exclusion": {"excluded_wallets": [wallet_b]}},
    )

    assert summary["status"] == "TEMPORAL_SLICE_PARTIAL_RUNTIME"
    assert summary["configured_status"] == "RUNTIME_LOADED_NO_WAVE_ORDERS_YET"
    assert summary["runtime_loaded_count"] == 1
    assert summary["runtime_missing_count"] == 1
    assert summary["runtime_missing_wallets"] == [wallet_b]
    assert summary["temporal_excluded_wallets"] == [wallet_b]


def test_state_digest_renders_clause_gate_artifacts(tmp_path: Path) -> None:
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text("## 2026-07-14T19:28Z fable DIRECTION [LIVE]\n- next: 20:00Z bundle.\n")
    data = tmp_path / "data" / "research"
    wallet = "0xe6db20932faf0f9780acf75d95c74c9984407dac"
    _write_json(
        data / "wallet_copy_active_set_auto_degrade_state.json",
        {
            "latest_e6db_2000_probe_cap_cut": {
                "status": "APPLIED",
                "applied_at": "2026-07-14T20:02:54Z",
                "trigger_pnl_usd": -13.760096,
                "threshold_pnl_usd": -10.0,
                "live_path_mutation": "config_overlay_only_no_process_restart",
                "members_touched": [
                    {
                        "old_member_max_order_usd": 4.0,
                        "new_max_order_usd": 1.0,
                    }
                ],
            },
            "latest_admission_wave": {
                "direction_id": "wave-1",
                "picked_wallets": [wallet],
            },
            "members": [{"source_wallet": wallet, "enabled": True}],
        },
    )
    _write_json(
        data / "wave_gate_attribution_20260714T2000Z.json",
        {
            "generated_at": "2026-07-14T20:01:00Z",
            "summary": {
                "gate_status": "FAIL_PIPELINE_EVIDENCE",
                "members": 1,
                "members_with_cycles": 1,
                "members_with_submit": 0,
                "total_cycles_landed": 9,
                "total_fresh_intents": 0,
                "total_ledger_orders": 0,
                "classification_counts": {"PIPELINE_NO_INTENTS_FROM_SOURCE_ACTIVITY": 1},
            },
            "members": [
                {
                    "source_wallet": wallet,
                    "candidate_id": "runtime_auto_degrade_e6db20932f",
                    "classification": "PIPELINE_NO_INTENTS_FROM_SOURCE_ACTIVITY",
                    "cycles_landed": 9,
                    "fresh_intents": 0,
                    "ledger": {"orders": 0},
                    "source_activity": {
                        "policy_eligible_windows": 1,
                        "policy_eligible_rows": 2,
                        "source_active_windows": 1,
                    },
                }
            ],
        },
    )
    _write_json(
        data / "admitted_member_gate_3048_20260714T2000Z.json",
        {
            "summary": {"gate_status": "FAIL_PIPELINE_EVIDENCE"},
            "members": [
                {
                    "source_wallet": "0x3048d65321be3497164cdfc2996f94f98a2e7537",
                    "classification": "NO_CYCLES_LANDED",
                    "fresh_intents": 0,
                    "ledger": {"orders": 0},
                    "source_activity": {"policy_eligible_windows": 4},
                }
            ],
        },
    )
    _write_json(
        data / "wave_repair_attribution_addendum_20260714T2030Z.json",
        {
            "generated_at": "2026-07-14T20:30:00Z",
            "status": "SELECTED_MEMBER_ONLY_CADENCE_CONFIRMED_FLAGGED_REPAIR_BUILT",
            "summary": {
                "status": "SELECTED_MEMBER_ONLY_CADENCE_CONFIRMED_FLAGGED_REPAIR_BUILT",
                "guard_evaluates_all_runtime_members_per_cycle_current": False,
                "main_live_execution_selected_members_per_cycle": 1,
                "all_member_flag_added": "--active-set-evaluate-all-runtime-members-per-cycle",
                "all_member_flag_default": False,
                "loaded_next_managed_restart_only": True,
                "zero_cycle_wallets_explained": 2,
                "single_seat_only_reclass_confirmed_for_pipeline_no_intents": False,
                "pipeline_no_intents_corrected_classification_counts": {
                    "LIVE_GUARD_STATUS_GATE_NOT_LIVE_ADMISSIBLE": 2,
                },
            },
        },
    )

    digest, text = build_digest(tmp_path)

    assert digest["active_set"]["e6db_2000_probe_cap_cut"]["status"] == "APPLIED"
    assert digest["active_set"]["wave_gate_attribution"]["gate_status"] == "FAIL_PIPELINE_EVIDENCE"
    assert digest["active_set"]["admitted_member_gate_3048"]["rows"][0]["policy_eligible_windows"] == 4
    assert digest["active_set"]["wave_repair_addendum"]["main_live_execution_selected_members_per_cycle"] == 1
    assert digest["active_set"]["wave_repair_addendum"]["all_member_flag_default"] is False
    assert "e6db_2000_cap_cut: status=APPLIED" in text
    assert "wave_gate_attribution: status=FAIL_PIPELINE_EVIDENCE" in text
    assert "gate_3048: status=FAIL_PIPELINE_EVIDENCE" in text
    assert "wave_repair_addendum: status=SELECTED_MEMBER_ONLY_CADENCE_CONFIRMED_FLAGGED_REPAIR_BUILT" in text
    assert "flag=--active-set-evaluate-all-runtime-members-per-cycle default=False" in text


def test_state_digest_surfaces_ranked_queue_reject_and_post_fee_decision(tmp_path: Path) -> None:
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text(
        "## 2026-07-21T01:17Z fable DIRECTION [PROMOTE/LEARN]\n"
        "- next: deliver f353 reject cells.\n"
    )
    _write_json(
        tmp_path / "data" / "research" / "ranked_queue_clearance_packets_latest.json",
        {
            "generated_at": "2026-07-21T01:23:00Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {"structural_no_ask_park_count": 1},
            "parked_dormant": [
                {
                    "wallet": "0x4444444444444444444444444444444444444444",
                    "status": "PARKED_DORMANT",
                    "reason": "absent_from_ranked_member_queue",
                    "recheck_at": "2026-07-22T01:23:00Z",
                }
            ],
            "parked_reject_ratio": [
                {
                    "wallet": "0x5555555555555555555555555555555555555555",
                    "status": "PARK_REJECT_RATIO",
                    "paper_orders": 200,
                    "attributable_reject_ratio": 0.61,
                    "reject_ratio_park_threshold": 0.60,
                }
            ],
            "packets": [
                {
                    "wallet": "0xf3531b23b504cf0aed4ff21325232b2a2d496685",
                    "replay_fill_backed": {
                        "attributable_reject_ratio": 0.715753,
                        "prospective_reject_taxonomy": {
                            "dominant_attributable_reject_category": "no_ask",
                            "shares_of_attributable_rejects": {"no_ask": 0.861244},
                        },
                    },
                    "exact_policy_post_fee_shadow": {
                        "resolved_orders": 82,
                        "post_fee_pnl_usd": 915.551409,
                        "gate_pass": True,
                    },
                    "clearance": {
                        "paper_disposition": "PARK_STRUCTURAL_NO_ASK_DOMINANT",
                        "named_cause": "no_recoverable_ask_dominates_attributable_rejects",
                        "ready_for_live": False,
                        "hot_standby_ready": False,
                    },
                }
            ],
        },
    )
    _write_json(
        tmp_path / "data" / "research" / "wallet_copy_ready_shadow_lanes_state.json",
        {
            "summary": {"next_gate_wallet": "0xf3531b23b504cf0aed4ff21325232b2a2d496685"},
            "standby_adjudications": [
                {
                    "wallet": "0x13e0d447520ebe7f8eeaf7817211201b2c585204",
                    "status": "NO_PROMOTE_DEAD_SOURCE",
                    "adjudicated_at": "2026-07-24T01:38:33Z",
                    "source_last_trade_iso": "2026-07-21T01:38:00Z",
                    "slot_action": "RELEASED_TO_NEXT_FULL_POOL_QUEUE_MEMBER",
                    "reenrollment_rule": "REQUIRES_FRESH_OBSERVED_BTC5M_TRADE_AFTER_DEAD_SOURCE_RULING",
                }
            ],
            "lanes": [
                {
                    "wallet": "0xf3531b23b504cf0aed4ff21325232b2a2d496685",
                    "canary_path": "CLEAR_TO_HOT_STANDBY_PAPER_CANARY",
                    "paper_policy_id": "exact",
                    "paper_canary_enrolled_at": "2026-07-21T01:38:33Z",
                    "paper_canary_elapsed_h": 0.1,
                    "paper_canary_minimum_h": 24.0,
                    "ready_shadow_full_utc_day": False,
                    "copyintent_parity_capture": {"status": "ARMED_PAPER_ONLY"},
                    "source_liveness": {"status": "PASS", "living_source": True},
                    "live_canary_packet_preconditions": {
                        "hot_standby_ready": True,
                        "ready_shadow_full_utc_day": False,
                    },
                    "readiness_verdict": "READY_SHADOW_24H_CANARY_ACCRUING",
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            ],
        },
    )

    digest, text = build_digest(tmp_path)

    packet = digest["ranked_queue_clearance_packets"]["packets"][0]
    assert packet["attributable_reject_ratio"] == 0.715753
    assert packet["resolved_orders"] == 82
    assert packet["post_fee_pnl_usd"] == 915.551409
    assert packet["paper_disposition"] == "PARK_STRUCTURAL_NO_ASK_DOMINANT"
    assert packet["ready_for_live"] is False
    parked = digest["ranked_queue_clearance_packets"]["parked_dormant"][0]
    assert parked["status"] == "PARKED_DORMANT"
    assert parked["reason"] == "absent_from_ranked_member_queue"
    ratio_park = digest["ranked_queue_clearance_packets"]["parked_reject_ratio"][0]
    assert ratio_park["status"] == "PARK_REJECT_RATIO"
    assert ratio_park["paper_orders"] == 200
    canary = digest["ready_shadow"]["paper_canary"]
    assert canary["paper_canary_elapsed_h"] == 0.1
    assert canary["copyintent_parity_capture"]["status"] == "ARMED_PAPER_ONLY"
    assert canary["source_liveness"]["living_source"] is True
    assert canary["live_orders_allowed"] is False
    adjudication = digest["ready_shadow"]["standby_adjudications"][0]
    assert adjudication["status"] == "NO_PROMOTE_DEAD_SOURCE"
    assert adjudication["slot_action"] == "RELEASED_TO_NEXT_FULL_POOL_QUEUE_MEMBER"
    assert "ranked_queue_clearance: paper_only=True live_allowed=False" in text
    assert "no_recoverable_ask_dominates_attributable_rejects" in text
    assert "PARKED_DORMANT" in text
    assert "PARK_REJECT_RATIO" in text
    assert "ready_shadow_paper_canary: wallet=0xf353...6685" in text
    assert "ready_shadow_dead_source_adjudication: wallet=0x13e0...5204" in text


def test_state_digest_surfaces_deadman_r1_and_trade_lane_attribution(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    (tmp_path / "docs" / "agents").mkdir(parents=True)
    (tmp_path / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-14T20:48Z fable DIRECTION [LIVE/DEFEND]\n"
        "- next: commit R1/R2 attribution.\n"
    )
    _write_json(
        data / "order_flow_deadman_r1_attribution_20260714T2050Z.json",
        {
            "generated_at": "2026-07-14T20:50:49Z",
            "direction_id": "2026-07-14T20:48Z-fable-deadman-reconciliation",
            "status": "RULED_QUIET_GUARD_SIDE_DEFECT_ATTRIBUTED",
            "expires_at": "2026-07-15T00:00:00Z",
            "midnight_restart_mandatory": True,
            "no_restart_before_expiry": True,
            "summary": {
                "deadman_top_status": "OK",
                "deadman_warning": None,
                "gated_quiet_classification": "GUARD_SIDE_HALT",
                "selected_wallet": "0x3048d65321be3497164cdfc2996f94f98a2e7537",
                "selected_status": "FABLE_1554_3048_COMPLETE_HISTORY_1USD_PROBE_CONFIG_RELOAD",
                "selected_failed_checks": ["status_live_admissible"],
                "selected_live_protection_passed": True,
                "fallthrough_admissible_targets": 0,
                "fresh_buy_rows_le_10s": 1,
                "policy_compatible_fresh_buy_rows_le_30s": 1,
                "policy_reject_counts_fresh_buy_le_30s": {"price_outside_policy": 1},
                "latest_live_order_ts": "2026-07-14T17:53:10.496422+00:00",
                "repair_status": "DORMANT_CODE_ONLY_UNTIL_2026-07-15T00:00Z",
                "allowlist_entry": "FABLE_1554_3048_COMPLETE_HISTORY_1USD_PROBE_CONFIG_RELOAD",
            },
        },
    )
    _write_json(
        data / "trade_executor_lane_attribution_20260714T2050Z.json",
        {
            "generated_at": "2026-07-14T20:50:49Z",
            "direction_id": "2026-07-14T20:42Z-fable-r2-trade-log-attribution",
            "status": "PASS_TEST_OR_PAPER_LINES_NO_LIVE_LEDGER_BYPASS",
            "summary": {
                "executing_trade_lines_203202z": 7,
                "size_usd_gt_1_lines": 7,
                "unit_test_signature_lines": 7,
                "live_ledger_orders_at_2032": 0,
                "live_execution_event_rows_at_2032": 1,
                "ledger_bypassing_submissions": 0,
                "only_live_guard_process_seen": True,
                "single_live_guard_pid": 12881,
                "logging_fix": "TradeExecutor Executing trade log now includes lane=<execution_lane>",
            },
        },
    )

    digest, text = build_digest(tmp_path)

    r1 = digest["order_flow_deadman_r1_attribution"]
    assert r1["status"] == "RULED_QUIET_GUARD_SIDE_DEFECT_ATTRIBUTED"
    assert r1["fallthrough_admissible_targets"] == 0
    assert r1["selected_failed_checks"] == ["status_live_admissible"]
    lane = digest["trade_executor_lane_attribution"]
    assert lane["ledger_bypassing_submissions"] == 0
    assert lane["only_live_guard_process_seen"] is True
    assert "deadman_r1_attribution: status=RULED_QUIET_GUARD_SIDE_DEFECT_ATTRIBUTED" in text
    assert "fallthrough=0" in text
    assert "trade_executor_lane_attribution: status=PASS_TEST_OR_PAPER_LINES_NO_LIVE_LEDGER_BYPASS" in text
    assert "lines=7 size_gt_1=7 unit_test=7 ledger_2032=0" in text
    assert "bypass=0 single_guard=True/12881" in text


def test_state_digest_renders_wallet_market_scan_metric(tmp_path: Path) -> None:
    now_ts = update_state_digest.datetime.now(update_state_digest.timezone.utc).timestamp()
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text(
        "## 2026-07-13T17:44Z fable DIRECTION [DISCOVER/LEARN]\n"
        "- next: run market-wide scan.\n"
    )
    _write_json(
        tmp_path / "data" / "research" / "wallet_market_scan_ranked.json",
        {
            "generated_at": "2026-07-13T17:50:00Z",
            "status": "PARTIAL_REPLAY_PENDING",
            "summary": {
                "active_wallets": 123,
                "new_active_wallets": 45,
                "wallets_ranked": 130,
                "trades_scanned": 5000,
                "crypto5m_trades_matched": 450,
                "pages_completed": 10,
                "scanned_alive_profitable": 7,
                "replay_status": "PENDING_REMOTE_HISTORY_REPLAY",
                "top_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "top_rank_score": 99.5,
            },
            "window": {"lookback_complete": False},
            "rate_limit_budget": {"page_cap_exhausted": True, "budget_exhausted": False},
            "route_class_counts": {"DIRECT_PASS": 10},
        },
    )
    _write_json(
        tmp_path / "data" / "research" / "wallet_market_cohort_replay_latest.json",
        {
            "generated_at": "2026-07-13T18:40:00Z",
            "status": "BATCH_REPLAY_COMPLETE",
            "summary": {
                "cohort_size": 25,
                "cohort_shadow_positive": 3,
                "live_ready_picks": 2,
                "top_live_ready_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "top_shadow_positive_wallet": "0xcccccccccccccccccccccccccccccccccccccccc",
            },
            "live_ready_picks": [
                {
                    "wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "paper_pnl_usd": 12.5,
                    "roi_pct": 25.0,
                    "resolved_copyable_events": 20,
                },
                {
                    "wallet": "0xdddddddddddddddddddddddddddddddddddddddd",
                    "paper_pnl_usd": 9.0,
                    "roi_pct": 18.0,
                    "resolved_copyable_events": 16,
                },
            ],
        },
    )
    _write_json(
        tmp_path / "data" / "research" / "queue_remote_dataapi_fresh_flow_probe_latest.json",
        {
            "generated_at": "2026-07-14T12:00:00Z",
            "rows": [
                {
                    "wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "status": "PASS",
                    "latest_btc5m_trade_ts": now_ts - 60.0,
                    "btc5m_trades_24h": 5,
                    "btc5m_buys_24h": 5,
                },
                {
                    "wallet": "0xdddddddddddddddddddddddddddddddddddddddd",
                    "status": "PASS",
                    "latest_btc5m_trade_ts": now_ts - (49.0 * 3600.0),
                    "btc5m_trades_24h": 5,
                    "btc5m_buys_24h": 5,
                },
            ],
        },
    )

    digest, text = build_digest(tmp_path)

    assert digest["wallet_market_scan"]["active_wallets"] == 123
    assert digest["wallet_market_scan"]["scanned_alive_profitable"] == 1
    assert digest["wallet_market_scan"]["raw_cohort_live_ready_picks"] == 2
    assert digest["wallet_market_scan"]["alive_profitable_failed_liveness_reason_counts"] == {
        "external_liveness_age_gte_24h": 1
    }
    assert digest["wallet_market_scan"]["cohort_size"] == 25
    assert digest["wallet_market_scan"]["cohort_shadow_positive"] == 3
    assert digest["wallet_market_scan"]["live_ready_picks"] == 2
    assert "market_scan_active=123" in text
    assert "cohort_size=25" in text
    assert "scanned_alive_profitable=1" in text
    assert "raw_cohort_live_ready=2" in text


def test_state_digest_renders_focused_candidate_p1_packet(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "data" / "research" / "focused_candidate_p1_8a47951a3c_latest.json",
        {
            "generated_at": "2026-07-20T05:30:00Z",
            "wallet": "0x8a47951a3cefcc98dc8b41eb438d1c49249872ef",
            "history_depth": {"status": "COMPLETE_TO_PREREGISTERED_LOOKBACK"},
            "old_vs_new": {
                "old_packet_resolved": 36,
                "new_deep_resolved": 238,
                "old_packet_pnl_usd": 3278.125906,
                "new_deep_pnl_usd": -3237.849726,
                "pnl_delta_usd": -6515.975632,
            },
            "temporal_hour_match": {"status": "FAIL"},
            "concentration": {
                "top1_positive_pnl_share_pct": 25.547535,
                "top3_positive_pnl_share_pct": 64.385009,
                "concentration_discounted": False,
            },
            "decision": {"verdict": "P1_FAIL_NO_ROTATION"},
        },
    )

    digest, text = build_digest(tmp_path)

    assert digest["focused_candidate_p1"]["decision"]["verdict"] == "P1_FAIL_NO_ROTATION"
    assert "focused_candidate_p1:wallet=0x8a47...72ef" in text
    assert "resolved=36->238" in text
    assert "verdict=P1_FAIL_NO_ROTATION" in text


def test_state_digest_newer_directions_use_file_order_after_status() -> None:
    entries = [
        {"heading": "## 2026-07-08T12:20Z fable DIRECTION [LIVE]", "body": "- NEXT: second"},
        {"heading": "## 2026-07-08T12:05Z codex STATUS [LIVE]", "body": "- done"},
        {"heading": "## 2026-07-08T12:10Z fable DIRECTION [LIVE]", "body": "- NEXT: first"},
        {"heading": "## 2026-07-08T12:03Z fable DIRECTION [LIVE]", "body": "- NEXT: before-status"},
    ]
    status = update_state_digest._latest_status_entry(entries)

    newer = update_state_digest._entries_after(
        entries,
        status,
        update_state_digest._is_fable_direction,
    )

    assert [row["heading"] for row in newer] == [
        "## 2026-07-08T12:10Z fable DIRECTION [LIVE]",
        "## 2026-07-08T12:03Z fable DIRECTION [LIVE]",
    ]


def test_material_direction_captures_bold_answer_next_line() -> None:
    body = "\n".join(
        [
            "- **Codex closeout audit: PASS.**",
            "- **Answer to codex question -- NEXT confirmed unchanged in structure, with one figure amendment:** "
            "(1) heartbeat freshness. (2) 18:00Z rerun.",
            "- **Missed windows 3 -> 6 since 14:52Z: WATCH, not incident**",
        ]
    )
    expected = [
        "- **Answer to codex question -- NEXT confirmed unchanged in structure, with one figure amendment:** "
        "(1) heartbeat freshness. (2) 18:00Z rerun.",
    ]

    assert update_state_digest._direction_next_block(body) == expected
    assert update_state_digest._material_direction_lines(body) == expected


def test_material_direction_captures_operator_amendments() -> None:
    body = "\n".join(
        [
            "- OPERATOR ORDER (verbatim intent): SOS interventions are EXECUTED.",
            "- THREE AMENDMENTS to the 19:00Z discipline:",
            "  1. SOS CLAUSE: act immediately.",
            "  2. JUSTIFIED-TOUCH RULE: name enemy or defect.",
            "  3. CHANGE JOURNAL: append-only traceability.",
        ]
    )

    assert update_state_digest._material_direction_lines(body) == [
        "- OPERATOR ORDER (verbatim intent): SOS interventions are EXECUTED.",
        "- THREE AMENDMENTS to the 19:00Z discipline:",
        "1. SOS CLAUSE: act immediately.",
        "2. JUSTIFIED-TOUCH RULE: name enemy or defect.",
        "3. CHANGE JOURNAL: append-only traceability.",
    ]


def test_state_digest_refreshes_stale_current_day_scorecard(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    scripts = tmp_path / "scripts"
    handoff.parent.mkdir(parents=True)
    scripts.mkdir()
    (tmp_path / "docs" / "agents" / "MARKET_FACTS.md").write_text("# Market Facts\n")
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-07T01:32Z DIRECTION (Fable, proactive steering pulse)",
                "- next: current scorecard freshness",
                "## 2026-07-07T01:33Z STATUS [LIVE/SELF-DEV]",
                "- defect | attempts (3+) | next: stale current-day digest | attempts: a,b,c | next: fix",
            ]
        )
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "summary": {
                "can_trade": True,
                "live_orders": 12,
                "filled_orders": 6,
                "rejected_orders": 6,
                "submitted_orders": 0,
                "latest_order_ts": "2026-07-07T02:22:00Z",
            },
            "orders": [],
        },
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "pid": 10106,
            "status": "LIVE_GUARD_RUNNING",
            "live_orders_allowed": True,
            "window_participation": {},
        },
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_2026-07-07.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "generated_at": "2026-07-07T01:00:00Z",
            "day_utc": "2026-07-07",
            "today": {"total": {"pnl_usd": 99.0, "resolved_fills": 1}},
            "canonical_pnl_truth": {"by_day": {"2026-07-07": {"pnl_usd": 99.0, "resolved_fills": 1}}},
            "since_topup_truth": {
                "primary_verdict": "STALE",
                "canonical_pnl_usd": 99.0,
                "actual_delta_vs_baseline_usd": 99.0,
                "reconciliation_status": "STALE",
            },
            "volume_kpi": {"canonical_daily": {"windows_filled": 1, "windows_submitted": 1, "denominator_windows": 288}},
            "execution_model_kpi": {},
        },
    )
    (scripts / "daily_scorecard.py").write_text(
        "import json\n"
        "print(json.dumps({\n"
        "  'kind': 'wallet_copy_daily_scorecard',\n"
        "  'generated_at': '2026-07-07T02:23:00Z',\n"
        "  'day_utc': '2026-07-07',\n"
        "  'today': {'total': {'pnl_usd': -20.25, 'resolved_fills': 14}},\n"
        "  'canonical_pnl_truth': {'by_day': {'2026-07-07': {'pnl_usd': -21.5, 'resolved_fills': 13}}},\n"
        "  'since_topup_truth': {\n"
        "    'primary_verdict': 'NOT_PRODUCING',\n"
        "    'canonical_pnl_usd': -16.616471,\n"
        "    'actual_delta_vs_baseline_usd': -15.06564,\n"
        "    'reconciliation_status': 'PASS'\n"
        "  },\n"
        "  'volume_kpi': {'canonical_daily': {'windows_filled': 11, 'windows_submitted': 11, 'denominator_windows': 288}},\n"
        "  'execution_model_kpi': {}\n"
        "}))\n"
    )

    digest, text = build_digest(tmp_path)

    assert digest["pnl"]["day_pnl_usd"] == -21.5
    assert digest["pnl"]["day_resolved_fills"] == 13
    assert digest["pnl"]["since_topup_verdict"] == "NOT_PRODUCING"
    assert "pnl: day=-21.5 fills=13" in text


def test_state_digest_header_folds_direction_next_and_material(tmp_path: Path) -> None:
    root = tmp_path
    (root / "data" / "research").mkdir(parents=True)
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-17T04:27Z fable DIRECTION [LIVE/DEFEND]\n"
        "- RULING 11: header must carry the operative direction body.\n"
        "- next: keep fresh cohort at floor size.\n"
        "- next: run the 12:00Z abstain count if still 0 resolved.\n"
        "- next: no eligibility loosening.\n"
        "- next: no rotation.\n"
    )

    digest, text = build_digest(root)

    header_line = next(
        line for line in text.splitlines() if line.startswith("latest_direction:")
    )
    assert "| next=- next: keep fresh cohort at floor size." in header_line
    assert "(+1 lines)" in header_line
    assert (
        "| material=- RULING 11: header must carry the operative direction body."
        in header_line
    )
    assert digest["latest_direction_next_verbatim"][0] == "- next: keep fresh cohort at floor size."


def test_state_digest_surfaces_d60c_latency_attribution(tmp_path: Path) -> None:
    root = tmp_path
    data = root / "data" / "research"
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "docs" / "agents" / "HANDOFF.md").write_text(
        "## 2026-07-17T12:10Z Fable DIRECTION [LIVE/MEASURE]\n"
        "- RULING 20c: d60c latency attribution next.\n"
    )
    _write_json(
        data / "d60c_latency_attribution_latest.json",
        {
            "generated_at": "2026-07-17T12:20:00Z",
            "summary": {
                "target_market_closed_count": 4,
                "recovered_market_closed_count": 4,
                "class_counts": {"A": 4},
                "majority_verdict": "MOSTLY_A_DECISION_LATENCY",
            },
            "target": {"decision_ts_iso": "2026-07-17T12:04:06Z"},
            "scan": {"selection_basis": "previous_ruling10_btc5m_window"},
            "events": [
                {
                    "event_id": "we_1",
                    "market_slug": "btc-updown-5m-1784289300",
                    "received_ts_iso": "2026-07-17T11:55:11Z",
                    "window_close_ts_iso": "2026-07-17T12:00:00Z",
                    "decided_ts_iso": "2026-07-17T12:04:06Z",
                    "latency_class": "A",
                    "decision_minus_close_s": 246.0,
                    "decision_minus_received_s": 535.0,
                }
            ],
        },
    )

    digest, text = build_digest(root)

    assert digest["d60c_latency_attribution"]["summary"]["target_market_closed_count"] == 4
    assert digest["d60c_latency_attribution"]["events"][0]["latency_class"] == "A"
    assert "d60c_latency=MOSTLY_A_DECISION_LATENCY target=4 recovered=4 classes={'A': 4}" in text


def test_state_digest_foregrounds_dead_flow_and_cross_exchange_actuator(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    agents = tmp_path / "docs" / "agents"
    agents.mkdir(parents=True)
    agents.joinpath("HANDOFF.md").write_text(
        "## 2026-07-25T03:35:24Z fable DIRECTION [LIVE/ROTATE/SELF-DEV]\n"
        "- next: wire cross-exchange emergency actuator.\n"
    )
    _write_json(
        data / "order_flow_deadman_state.json",
        {
            "status": "INCIDENT_ORDER_FLOW_DEAD",
            "can_trade": True,
            "consecutive_incidents": 27,
            "raw_accepted_order_deadman": {"accepted_order_idle_s": 11547.9},
            "policy_choke": {
                "selected_wallet": "0x32de91fa203321fa7735e7854f2b1c844e71ce9d",
                "selected_fresh_source_rows": 0,
                "selected_eligible_intents": 0,
                "selected_guard_submit_attempts": 0,
                "selected_accepted_orders": 0,
                "source_roster_drought": {
                    "candidate_evidence": {
                        "regime": "weekday",
                        "candidate_count": 9,
                        "eligible_count": 0,
                        "refusal_counts": {
                            "f1_walk_forward_admissible": 8,
                        },
                        "supply_dropouts": [
                            {
                                "wallet": "0x31c290a2772e1e3143bcb6debbdbbf08ac081d13",
                                "previous_supply_source": "full_pool_member_queue",
                                "absent_reason": (
                                    "absent_from_current_reconciled_direct_generation_supply"
                                ),
                            }
                        ],
                    }
                },
            },
        },
    )
    _write_json(
        data / "wallet_copy_active_set_auto_degrade_state.json",
        {
            "alternate_source_rotation": {
                "mechanical_demotion": {
                    "demoted_members": [
                        {"source_wallet": "0x4d8b", "rolling_realized_pnl_usd": -11.8},
                        {"source_wallet": "0xc50d", "rolling_realized_pnl_usd": -11.79},
                    ]
                }
            }
        },
    )
    _write_json(
        data / "btc5m_cross_exchange_probability_edge_live_actuator_latest.json",
        {
            "status": "ARMED_WAITING_QUALIFYING_SIGNAL",
            "terminal_reason": "PROTECTED_SKIP",
            "activation_started_at": "2026-07-25T03:40:00Z",
            "activation_expires_at": "2026-07-25T04:40:00Z",
            "last_order_id": "",
        },
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "orders": [
                {
                    "order_id": "0xcross",
                    "source_wallet": "btc5m_cross_exchange_probability_edge_v1",
                    "status": "FILLED",
                    "final_status": "FILLED",
                    "submitted_at": "2026-07-25T04:46:04Z",
                    "response_filled_size_usd": 0.999999,
                    "alternate_transport_attribution": {
                        "resolution_status": "RESOLVED",
                        "resolved_post_fee_pnl_usd": -0.999999,
                    },
                }
            ]
        },
    )
    _write_json(
        data / "live_method_supply_packet_latest.json",
        {"activation": {"status": "PAPER_ONLY"}},
    )
    _write_json(
        data / "e5_delayed_offset_side_selective_promotion_packet_latest.json",
        {"decision": "PARK_DELAYED_OFFSET_METHOD_PAPER_ONLY"},
    )
    _write_json(
        data / "btc5m_multivenue_residual_matrix_state.json",
        {
            "generated_at": "2026-07-25T07:30:00Z",
            "status": "PAPER_MATRIX_ACTIVE",
            "generation_checksum": "8212395f28a7",
            "synchronized_venue_clocks": {"sample_count": 100},
            "consensus_cell_count": 8,
            "residual_lane_slots": 5,
            "residual_sibling_count": 40,
            "cell_count": 48,
            "paper_only": True,
            "live_orders_allowed": False,
        },
    )
    _write_json(
        data / "btc5m_cross_exchange_promoted_cell_latest.json",
        {"status": "NO_GATE_COMPLETE_CELL", "selected": None},
    )
    _write_json(
        data / "btc5m_complete_set_paired_maker_state.json",
        {
            "generated_at": "2026-07-25T14:14:50Z",
            "status": "PARK_ZERO_INTENT_GENERATION",
            "generation_checksum": "2c92543487b6",
            "complete_liveness_window_starts_s": [1784988300, 1784988600],
            "completed_liveness_windows": 2,
            "positive_edge_intents": 0,
            "blocker_taxonomy": {"Up:complete_set_actual_book_or_post_only_bounds": 2},
            "stop_writer": True,
            "promoted_cell_selector": {
                "status": "NO_GATE_COMPLETE_CELL",
                "selected": None,
            },
            "paper_only": True,
            "live_orders_allowed": False,
        },
    )
    _write_json(
        data / "btc5m_complete_set_split_sell_overround_state.json",
        {
            "generated_at": "2026-07-25T14:27:09Z",
            "status": "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE",
            "generation_checksum": "8fd6d6730866",
            "complete_liveness_window_starts_s": [300, 600, 900, 1200, 1500, 1800],
            "completed_liveness_windows": 2,
            "positive_edge_intents": 21,
            "attribution_funnel": {"resolved_selector_cells": 20},
            "realized_post_cost_pnl_usd": 0.0,
            "gate_checks": {"inventory_conservation": True},
            "stop_writer": False,
            "paper_only": True,
            "live_orders_allowed": False,
        },
    )
    _write_json(
        data / "current_f1_f4_fallout_audit.json",
        {
            "generated_at": "2026-07-25T14:34:26Z",
            "status": "RUNG_C_NO_ADMISSIBLE_TARGET",
            "candidate_count": 44,
            "eligible_count": 0,
            "refusal_counts": {"f1_measured_positive_regime_cell": 44},
            "active_temporal_join_defect_found": False,
        },
    )

    digest, text = build_digest(tmp_path)

    foreground = digest["live_flow_incident_foreground"]
    assert foreground["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    assert foreground["selected_fresh_source_rows"] == 0
    assert foreground["candidate_supply"]["candidate_count"] == 9
    assert foreground["candidate_supply"]["eligible_count"] == 0
    assert foreground["candidate_supply"]["supply_dropouts"][0]["wallet"].endswith(
        "081d13"
    )
    assert len(foreground["loss_exclusions"]) == 2
    assert foreground["cross_exchange_actuator_status"] == "ARMED_WAITING_QUALIFYING_SIGNAL"
    assert foreground["cross_exchange_campaign_orders_submitted"] == 1
    assert foreground["cross_exchange_campaign_orders_accepted"] == 1
    assert foreground["cross_exchange_campaign_orders_filled"] == 1
    assert foreground["cross_exchange_campaign_method_pnl"]["resolved_fills"] == 1
    assert foreground["cross_exchange_campaign_method_pnl"]["rolling_realized_pnl_usd"] == -0.999999
    assert (
        digest["cross_exchange_probability_edge"]["delayed_offset_park"]["decision"]
        == "PARK_DELAYED_OFFSET_METHOD_PAPER_ONLY"
    )
    assert (
        digest["cross_exchange_probability_edge"]["multivenue_residual_matrix"][
            "cell_count"
        ]
        == 48
    )
    assert (
        digest["cross_exchange_probability_edge"]["complete_set_paired_maker"][
            "complete_liveness_window_starts_s"
        ]
        == [1784988300, 1784988600]
    )
    assert "flow_incident_foreground: status=INCIDENT_ORDER_FLOW_DEAD" in text
    assert "funnel=0/0/0/0" in text
    assert "candidate_supply={'regime': 'weekday', 'candidate_count': 9" in text
    assert "rule=RUNNING_PID_NEVER_IMPLIES_FLOW_PASS" in text
    assert "campaign=1/1/1" in text
    assert "multivenue=PAPER_MATRIX_ACTIVE/48/NO_GATE_COMPLETE_CELL/8212395f28a7" in text
    assert "paired_complete_set=PARK_ZERO_INTENT_GENERATION/2/0/2c92543487b6" in text
    assert "split_sell=PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE/6/21/20/8fd6d6730866" in text
    assert "f1_f4_fallout=RUNG_C_NO_ADMISSIBLE_TARGET/44/0/join_defect=False" in text


def test_state_digest_surfaces_order147_seat_feedstock_audit(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    (data / "order147_seat_feedstock_divergence_latest.json").write_text(
        json.dumps(
            {
                "status": "E1_GUARD_READER_ALIVE_DOWNSTREAM_PREDICATE_DEFECT",
                "pre_registered_branch": "E1'''''''",
                "seated_wallet": "0x2d7c9298b64713de86402bd8a41695e31865a945",
                "paper_accumulator": {
                    "seated_wallet_buy_rows_in_generation": 38,
                    "unique_chain_identities": 3,
                    "guard_read_unique_identities": 3,
                },
            }
        )
    )
    digest, text = build_digest(tmp_path)
    assert digest["order147_seat_feedstock"]["pre_registered_branch"] == "E1'''''''"
    assert "order147=E1_GUARD_READER_ALIVE_DOWNSTREAM_PREDICATE_DEFECT/E1'''''''/38/3/3" in text


def test_state_digest_surfaces_order148_and_order149_rotation_evidence(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    (data / "order148_seated_fill_dispositions_latest.json").write_text(
        json.dumps(
            {
                "price_band_hypothesis": {"status": "CONFIRMED"},
                "supply_goal_arithmetic": {
                    "in_band_fills": 0,
                    "perfect_all_wins_daily_profit_upper_bound_usd": 0.0,
                },
            }
        )
    )
    (data / "order149_rotation_qualification_latest.json").write_text(
        json.dumps(
            {
                "status": "E3_OWN_POLICY_REPLAY_INPUTS_NOT_PERSISTED_STOP",
                "pre_registered_branch": "E3⁹",
                "shortlist_wallet_count": 2,
                "exact_blocking_fields": ["book_snapshot", "token_metadata.market_slug"],
            }
        )
    )
    (data / "order149_token_metadata_backfill_latest.json").write_text(
        json.dumps(
            {
                "pre_registered_branch": "E2¹⁰",
                "unique_rows": 242,
                "metadata_resolved": 161,
                "unresolved_token_ids": ["t1", "t2"],
            }
        )
    )
    (data / "order149_gamma_metadata_recovery_latest.json").write_text(
        json.dumps(
            {
                "status": "GAMMA_UNREACHABLE_OR_TOKEN_UNKNOWN_RESIDUAL_PUBLISHED",
                "recovered_count": 0,
                "residual_count": 2,
            }
        )
    )
    (data / "order149_depth_at_size_latest.json").write_text(
        json.dumps({
            "verdict": "MEASURED",
            "snapshot_count": 10,
            "targets": [{"target_usd": 12.138, "within_250bps_of_best_ask_fill_rate": 0.6}],
        }),
        encoding="utf-8",
    )
    (data / "order150_window_supply_attribution_latest.json").write_text(
        json.dumps({
            "elapsed_windows": 80,
            "category_counts": {"BOOK_OBSERVED_NO_GUARD_REASON": 20, "FUTURE_PENDING": 208, "NO_BOOK_OR_GUARD_EVIDENCE": 60},
            "book_observed_unreasoned_windows_with_12p138_fillability": 18,
        }),
        encoding="utf-8",
    )
    (data / "order150_joint_supply_size_projection_latest.json").write_text(
        json.dumps({
            "status": "JOINT_PROJECTION_BELOW_GOAL_FLOOR",
            "projected_effective_fillable_windows_per_288": 64.8,
            "projected_incremental_daily_profit_usd": 22.5,
        }),
        encoding="utf-8",
    )
    digest, text = build_digest(tmp_path)
    assert digest["order148_seated_fill_dispositions"]["price_band_hypothesis"]["status"] == "CONFIRMED"
    assert digest["order149_rotation_qualification"]["pre_registered_branch"] == "E3⁹"
    assert "order148=CONFIRMED/0/0.0" in text
    assert "order149=E3_OWN_POLICY_REPLAY_INPUTS_NOT_PERSISTED_STOP/E3⁹/2/" in text
    assert "order149_meta=E2¹⁰/161/242/residual_tokens=2" in text
    assert "order149_gamma=GAMMA_UNREACHABLE_OR_TOKEN_UNKNOWN_RESIDUAL_PUBLISHED/0/2" in text
    assert "order149_depth=MEASURED/10/[(12.138, 0.6)]" in text
    assert "order150_supply=80/" in text
    assert "recoverable=18" in text
    assert "order150_joint=JOINT_PROJECTION_BELOW_GOAL_FLOOR/64.8/$22.5" in text


def test_state_digest_surfaces_order_flow_episode_aggregate(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    now = datetime.now(UTC).replace(microsecond=0)
    cleared_at = now.isoformat().replace("+00:00", "Z")
    data.mkdir(parents=True)
    (data / "order_flow_deadman_episodes.jsonl").write_text(
        json.dumps(
            {
                "kind": "order_flow_deadman_episode_closeout",
                "episode_id": "episode-1",
                "cleared_at": cleared_at,
                "episode_duration_s": 193.463764,
                "restart_performed": True,
                "fire_mechanical_escalation": "MANAGED_RESTART_BYTE_IDENTICAL",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    _write_json(
        data / "order_flow_deadman_episodes_manifest.json",
        {
            "schema_version": 1,
            "kind": "order_flow_incident_archive_manifest",
            "archives": [],
            "ledger_scope": {
                "first_armed_commit": "09176b85",
                "first_armed_at": cleared_at,
                "prior_episodes_unrecorded": True,
                "prior_incident_rows_at_arming": 12,
                "prior_recorded_clears": 0,
            },
        },
    )

    digest, text = build_digest(tmp_path)

    episodes = digest["flow_episodes"]
    assert episodes["episodes_today"] == 1
    assert episodes["total_dead_s"] == 193.463764
    assert episodes["natural_clears"] == 0
    assert episodes["restarts_performed"] == 1
    assert episodes["unknown_restart_clears"] == 0
    assert episodes["escalations_proposed"] == 1
    assert episodes["ledger_scope"]["first_armed_at"] == cleared_at
    assert (
        "flow_episodes: episodes_today=1(+12_prior_unrecorded) "
        f"armed={cleared_at} total_dead_s=193.463764"
    ) in text
    assert "restarts_performed=1" in text
    assert "prior_unrecorded=True" in text


def test_state_digest_surfaces_live_guard_restart_preflight_and_sweep(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "research"
    handoff = tmp_path / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text(
        "## 2026-07-29T00:00Z fable DIRECTION [LIVE]\n"
        "- next: verify the canonical restart.\n"
    )
    _write_json(
        data / "brainless_live_guard_restart_state.json",
        {
            "latest_decision": {
                "generated_at": "2026-07-29T00:00:04Z",
                "status": "RESTART_EXECUTED",
                "reason": "operator_approved_generation_reload",
                "feed_health_preflight": {
                    "status": "PASS",
                    "passed": True,
                    "checks": {
                        "writer_pid_alive": True,
                        "parse_tail_clean": True,
                    },
                },
                "execution": {
                    "actual_pid": 61002,
                    "quiescent_remnant_sweep": {
                        "status": "REFUSED_GUARD_NOT_QUIESCENT",
                        "deleted": [],
                        "retained": [],
                    },
                },
            }
        },
    )
    _write_json(
        data / "guard_generation_delta_latest.json",
        {
            "generated_at": "2026-07-29T00:00:05Z",
            "status": "PASS_CITABLE",
            "reconstruction_status": "MATCH",
            "citation_allowed": True,
            "loaded_commit": "abc123",
            "historical_loaded_file_count": 9,
            "comparison_file_count": 8,
            "changed_count": 1,
            "rows": [
                {"path": "scripts/run_wallet_copy_live_guard.py", "changed": True},
                {"path": "src/wallet_copy/mission.py", "changed": False},
            ],
            "live_mutation": False,
        },
    )

    digest, text = build_digest(tmp_path)

    restart = digest["live_guard_restart"]
    assert restart["status"] == "RESTART_EXECUTED"
    assert restart["preflight_passed"] is True
    assert restart["actual_pid"] == 61002
    assert restart["sweep_status"] == "REFUSED_GUARD_NOT_QUIESCENT"
    assert restart["latest_executed_restart"]["status"] == "RESTART_EXECUTED"
    assert restart["latest_executed_restart"]["actual_pid"] == 61002
    assert restart["generation_delta"]["reconstruction_status"] == "MATCH"
    assert restart["generation_delta"]["changed_count"] == 1
    assert restart["generation_delta"]["changed_paths"] == [
        "scripts/run_wallet_copy_live_guard.py"
    ]
    assert "live_guard_restart=status=RESTART_EXECUTED" in text
    assert "preflight=PASS/True" in text
    assert "sweep=REFUSED_GUARD_NOT_QUIESCENT" in text
    assert "generation_delta=PASS_CITABLE/MATCH citable=True changed=1" in text


def test_state_digest_surfaces_prospective_flow_episode_aggregate(
    tmp_path: Path, monkeypatch
) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(
        update_state_digest,
        "_utc_now_iso",
        lambda: "2026-07-30T12:00:00Z",
    )
    episode_at = now.isoformat()
    episode = {
        "cleared_at": episode_at,
        "episode_duration_s": 125.5,
        "restart_performed": False,
        "fire_mechanical_escalation": "MANAGED_RESTART_BYTE_IDENTICAL",
    }
    (data / "order_flow_deadman_episodes.jsonl").write_text(
        json.dumps(episode) + "\n",
        encoding="utf-8",
    )
    _write_json(
        data / "order_flow_deadman_episodes_manifest.json",
        {
            "archives": [],
            "ledger_scope": {
                "first_armed_at": episode_at,
                "prior_episodes_unrecorded": True,
                "prior_incident_rows_at_arming": 12,
            },
        },
    )

    summary = update_state_digest._flow_episode_summary(data, now)

    assert summary["episodes_today"] == 1
    assert summary["total_dead_s"] == 125.5
    assert summary["natural_clears"] == 1
    assert summary["restarts_performed"] == 0
    assert summary["escalations_proposed"] == 1
    assert summary["ledger_scope"]["prior_episodes_unrecorded"] is True

    open_fire_at = (now - update_state_digest.timedelta(minutes=10)).isoformat()
    _write_json(
        data / "order_flow_deadman_state.json",
        {
            "status": "INCIDENT_ORDER_FLOW_DEAD",
            "checked_at": open_fire_at,
            "episode_fire_at": open_fire_at,
            "episode_fire_idle_s": 1801.0,
            "mechanical_escalation": "MANAGED_RESTART_BYTE_IDENTICAL",
            "can_trade": True,
        },
    )
    digest, text = build_digest(tmp_path)
    assert digest["flow_episodes"]["episodes_today"] == 2
    assert digest["flow_episodes"]["open_episode_fire_at"] == open_fire_at
    assert digest["flow_episodes"]["open_episode_idle_s"] == 1801.0
    assert digest["flow_episodes"]["open_dead_s"] >= 599.0
    assert (
        "flow_episodes: episodes_today=2(+12_prior_unrecorded) "
        f"armed={episode_at} total_dead_s=125.5"
    ) in text
    assert "open_episode_fire_at=" in text
    assert f"first_armed_at={episode_at}" in text
    assert "STALE_ACROSS_TRANSITION" in text
    assert "status=INCIDENT_ORDER_FLOW_DEAD" in text
    assert "mechanical_escalation=MANAGED_RESTART_BYTE_IDENTICAL" in text
    assert f"deadman_checked_at={open_fire_at}" in text


def test_state_digest_surfaces_current_wide_source_depth_and_park_truth(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    _write_json(
        data / "wide_exact_policy_manifest_order93_widened_latest.json",
        {
            "generated_at": "2026-07-31T10:45:00Z",
            "source_alpha_report": "fresh.json",
            "source_alpha_status": "PASS_CURRENT_SOURCE",
            "source_alpha_age_h": 0.03,
            "admitted_wallets_blocked_by": None,
            "summary": {"admitted_wallets": 0},
        },
    )
    _write_json(
        data / "wide_depth_priority_frontier_latest.json",
        {
            "generated_at": "2026-07-31T11:00:00Z",
            "summary": {"cell_count": 1},
            "cells": [{
                "wallet": "0x3048",
                "wide_policy_fingerprint": "8c39887e",
                "resolved": 208,
                "gap_to_400": 192,
                "observed_resolved_signals_per_day": 0.0,
                "rate_status": "TWO_CUT_RATE_OBSERVED",
                "f1_walk_forward_admissible": False,
            }],
        },
    )
    _write_json(
        data / "82c8_park_reconciliation_latest.json",
        {
            "terminal_outcome": "PARK_COMMITTED",
            "replacement_reason": "PARK_SEAT_EVIDENCE_BAR_NOT_CROSSED_AT_DEADLINE",
            "park_reopened": False,
        },
    )
    _write_json(
        data / "repaired_eligible_slice_cohort_gap_latest.json",
        {
            "generated_at": "2026-07-31T11:22:47Z",
            "cohort_manifest_id": "widemanifest_50d",
            "frontier_checksum": "e906db5c",
            "summary": {
                "eligible_outside_cohort_wallets": 1,
                "binding_rows": 3,
                "repairable_rows": 3,
                "terminal_rows": 0,
            },
            "rows": [
                {
                    "wallet": "0x3139",
                    "binding_exclusion": "not_in_cohort_manifest",
                    "repairable_by_next_wide_manifest": True,
                }
            ],
            "paper_only": True,
            "live_orders_allowed": False,
            "promotion_authority": False,
        },
    )
    _write_json(
        data / "alpha_decay_report_wide_order104_latest.json",
        {
            "eligibility_delta": {
                "status": "MEASURED",
                "eligible_profile_count": {"before": 2, "after": 1},
                "eligible_move_slice_count": {"before": 1, "after": 3},
            }
        },
    )

    digest, text = build_digest(tmp_path)
    wide = digest["wide_candidate_measurement"]
    assert wide["current_alpha_manifest"]["source_alpha_status"] == "PASS_CURRENT_SOURCE"
    assert wide["depth_priority_frontier"]["cells"][0]["resolved"] == 208
    assert wide["park_reconciliation_82c8"]["park_reopened"] is False
    assert wide["repaired_eligible_slice_cohort_gap"]["summary"] == {
        "eligible_outside_cohort_wallets": 1,
        "binding_rows": 3,
        "repairable_rows": 3,
        "terminal_rows": 0,
    }
    assert wide["repaired_eligible_slice_cohort_gap"]["promotion_authority"] is False
    assert wide["order104_alpha_eligibility_delta"]["eligible_profile_count"] == {
        "before": 2,
        "after": 1,
    }
    assert "current_alpha_manifest=" in text
    assert "depth_priority_frontier=" in text
    assert "park_reconciliation_82c8=" in text
    assert "repaired_cohort_gap=" in text
    assert "order104_delta=" in text


def _standdown_guard(*, included_count, member_count=0):
    return {
        "generated_at": "2026-08-01T22:05:00Z",
        "blockers": [
            "no_enabled_active_set_members",
            "runtime_admission_candidate_missing",
            "runtime_admission_source_wallet_missing",
        ],
        "live_orders_allowed": False,
        "paper_only": True,
        "coverage_kpi": {"below_active_set_min": True, "enabled": False},
        "active_set_runtime": {
            "member_count": member_count,
            "enabled": False,
            "qualified_member_count": 0,
            "selection_mode": "no_enabled_active_set_members",
            "temporal_slice_exclusion": {
                "as_of": "2026-08-01T22:05:00Z",
                "active_slices": ["weekend"],
                "excluded_count": 1,
                "included_count": included_count,
                "excluded_members": [
                    {
                        "source_wallet": "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1",
                        "reason": "temporal_slice_weekend_proven_negative",
                        "matched_slice": {"slice": "weekend", "resolved_trades": 755},
                    }
                ],
            },
        },
    }


def test_seat_standdown_requires_the_exclusion_to_explain_the_empty_seat() -> None:
    """A survivor of the slice filter that still fails to seat is not a stand-down.

    After the 18-22Z dead band lifts, an unrelated weekend exclusion must not
    keep labelling the empty seat 'scheduled' and suppressing its diagnostics.
    """
    stood_down = update_state_digest._live_seat_standdown(_standdown_guard(included_count=0))
    assert stood_down["status"] == "SEAT_STOOD_DOWN_TEMPORAL_SLICE"
    assert stood_down["slice_explains_empty_seat"] is True
    assert stood_down["derived_during_standdown"]["suppressed"] is True

    leaked = update_state_digest._live_seat_standdown(_standdown_guard(included_count=1))
    assert leaked["status"] == "SEAT_EMPTY_NOT_SLICE_EXPLAINED"
    assert leaked["slice_explains_empty_seat"] is False
    assert leaked["derived_during_standdown"]["suppressed"] is False
    assert leaked["included_count"] == 1


def test_seat_standdown_names_generic_no_candidate_blockers_as_derived() -> None:
    stood_down = update_state_digest._live_seat_standdown(_standdown_guard(included_count=0))
    assert stood_down["derived_during_standdown"]["blockers.generic_no_candidate"] == [
        "runtime_admission_candidate_missing",
        "runtime_admission_source_wallet_missing",
    ]


def test_seat_standdown_missing_included_count_stays_permissive() -> None:
    guard = _standdown_guard(included_count=0)
    del guard["active_set_runtime"]["temporal_slice_exclusion"]["included_count"]
    assert (
        update_state_digest._live_seat_standdown(guard)["status"]
        == "SEAT_STOOD_DOWN_TEMPORAL_SLICE"
    )
