from __future__ import annotations

from datetime import UTC, datetime, timedelta

from scripts.run_ready_wallet_shadow_lanes import (
    A689_HOT_STANDBY_WALLET,
    _liveness_by_wallet,
    _source_liveness,
    build_state,
)


def test_wide_binding_sidecar_is_ingested_idempotently_without_clock_rewrite() -> None:
    binding = {
        "execution_status": "EXECUTED",
        "authority": "fable",
        "binding": {
            "wallet": "0x82c857cb4d18e919c1b7d3c6865be4debe50da77",
            "source_binding": "FABLE_20260729_82C8_WIDE_RECEIPT_STANDBY",
            "source_binding_id": "widebind_1",
            "source_binding_status": "WIRED",
            "source_binding_authority": "fable",
            "standby_evidence_started_at": "2026-07-29T06:44:50Z",
            "standby_evidence_elapsed_h": 0.0,
            "standby_evidence_minimum_h": 48.0,
            "resolved_paper_fills": 0,
            "promotion_resolved_fill_gate": 30,
            "terminal_outcome_on_deadline": {
                "status": "PARK_SEAT_UNFED_CLOCK",
                "terminal": True,
            },
            "paper_only": True,
            "live_orders_allowed": False,
        },
    }
    common = {
        "queue": {"ranked_members": []},
        "limit": 5,
        "gate": 50,
        "wide_standby_binding": binding,
        "now": datetime(2026, 7, 29, 7, 0, tzinfo=UTC),
    }

    first = build_state(**common)
    second = build_state(**common, previous_state=first)
    first_lane = next(row for row in first["lanes"] if row["wallet"].startswith("0x82c8"))
    second_lane = next(row for row in second["lanes"] if row["wallet"].startswith("0x82c8"))
    adjudications = [
        row
        for row in second["standby_adjudications"]
        if row.get("source_binding_id") == "widebind_1"
    ]

    assert first_lane["standby_evidence_started_at"] == "2026-07-29T06:44:50Z"
    assert second_lane["standby_evidence_started_at"] == first_lane["standby_evidence_started_at"]
    assert second_lane["source_binding_status"] == "WIRED"
    assert second_lane["paper_only"] is True
    assert second_lane["live_orders_allowed"] is False
    assert len(adjudications) == 1


def test_rank1_open_evidence_window_preserves_forward_fill_accounting() -> None:
    wallet = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=30,
        previous_state={
            "lanes": [{
                "wallet": wallet,
                "source_binding": "FABLE_20260729_82C8_WIDE_RECEIPT_STANDBY",
                "standby_evidence_started_at": "2026-07-29T06:44:50Z",
            }]
        },
        wide_exact_state={
            "orders": [{
                "wallet": wallet,
                "recorded_at": "2026-07-29T07:00:00Z",
                "resolved": True,
                "resolution_computed_at": "2026-07-29T07:05:00Z",
                "pre_fee_pnl_usd": 1.0,
                "post_fee_pnl_usd": 0.9,
                "f1_f4_terminal": {"terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL"},
            }]
        },
        now=datetime(2026, 7, 29, 8, 0, tzinfo=UTC),
    )

    lane = next(row for row in state["lanes"] if row["wallet"] == wallet)
    assert lane["paper_orders"] == 1
    assert lane["resolved_paper_fills"] == 1
    assert lane["in_lane_post_fee_pnl_usd"] == 0.9
    assert "evidence_window_closed_at" not in lane
    assert "post_window_resolved_fills" not in lane


def test_rank1_closed_window_separates_late_resolutions_from_evidence_bar() -> None:
    wallet = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    orders = [
        {
            "wallet": wallet,
            "recorded_at": f"2026-07-31T09:{minute:02d}:00Z",
            "resolved": True,
            "resolution_computed_at": f"2026-07-31T09:{minute:02d}:30Z",
            "pre_fee_pnl_usd": 1.0,
            "post_fee_pnl_usd": 0.9,
            "f1_f4_terminal": {"terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL"},
        }
        for minute in range(31)
    ]
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=30,
        previous_state={
            "lanes": [{
                "wallet": wallet,
                "source_binding": "FABLE_20260729_82C8_WIDE_RECEIPT_STANDBY",
                "standby_evidence_started_at": "2026-07-29T06:44:50Z",
                "terminal_executed_at": "2026-07-31T06:45:02Z",
            }]
        },
        wide_exact_state={"orders": orders},
        now=datetime(2026, 7, 31, 10, 0, tzinfo=UTC),
    )

    lane = next(row for row in state["lanes"] if row["wallet"] == wallet)
    assert lane["raw_paper_orders"] == 31
    assert lane["post_window_resolved_fills"] == 31
    assert lane["paper_orders"] == 0
    assert lane["resolved_paper_fills"] == 0
    assert lane["post_fee_evidence_bar_crossed"] is False
    assert lane["readiness_verdict"] == "EVIDENCE_WINDOW_CLOSED_ACCRUAL_NON_QUALIFYING"


def test_951b_f1_sample_lane_replaces_terminal_volume_seat() -> None:
    wallet = "0x951bd740ef681d05891ca35440232488271d433e"
    fingerprint = (
        "dcef9b3028326682bf283dfc028a7225a64bc26b472960e1df1f98ed27d3c078"
    )
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state={
            "lanes": [
                {
                    "wallet": "0x13e0d447520ebe7f8eeaf7817211201b2c585204",
                    "shadow_status": "GATE_CROSSED",
                }
            ]
        },
        wide_fingerprint_evidence={
            "cells": [
                {
                    "identity": {
                        "wallet": wallet,
                        "wide_policy_fingerprint": fingerprint,
                        "policy_id": "wide-paper",
                        "move_slice_keys": ["060-120|0.50-0.75"],
                    },
                    "venue_executable_full_stream_rescore": {
                        "resolved": 94,
                        "post_fee_pnl_usd": -8.0,
                        "roi_pct": -8.5,
                        "first_half_post_fee_pnl_usd": -4.0,
                        "second_half_post_fee_pnl_usd": -4.0,
                        "concentration_admissible": False,
                        "f1_walk_forward_admissible": False,
                    },
                    "resolution_evidence_summary": {
                        "matured_unresolved_window_count": 12
                    },
                }
            ]
        },
        volume_promotion_packet={
            "terminal_decision": "PARK_VOLUME_STANDBY_PAPER_ONLY"
        },
        now=datetime(2026, 7, 31, 9, 0, tzinfo=UTC),
    )

    assert all(
        row["wallet"] != "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
        for row in state["lanes"]
    )
    lane = next(row for row in state["lanes"] if row["wallet"] == wallet)
    assert lane["shadow_status"] == "F1_SAMPLE_MEASUREMENT_ONLY"
    assert lane["wide_policy_fingerprint"] == fingerprint
    assert lane["paper_only"] is True
    assert lane["live_orders_allowed"] is False
    assert lane["ready_for_live"] is False
    assert lane["replaced_measurement_seat_wallet"].startswith("0x13e0")
    projection = lane["projected_resolved_signals_at_200"]
    assert projection["current_resolved_signals"] == 94
    assert projection["resolved_signal_gap"] == 106
    assert projection["projected_after_matured_resolution"] == 106
    assert projection["status"] == "SAMPLE_ACCRUING_ON_NEGATIVE_CELL"
    assert lane["f1_sign_flip_required"] is True
    assert lane["f2_status"]["pass"] is False


def test_951b_second_zero_cut_reports_zero_rate_and_live_f2_legs() -> None:
    wallet = "0x951bd740ef681d05891ca35440232488271d433e"
    fingerprint = (
        "dcef9b3028326682bf283dfc028a7225a64bc26b472960e1df1f98ed27d3c078"
    )
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state={"lanes": [{
            "wallet": wallet,
            "wide_policy_fingerprint": fingerprint,
            "measurement_started_at": "2026-07-31T09:00:00Z",
            "measurement_baseline_resolved_signals": 95,
        }]},
        wide_fingerprint_evidence={"cells": [{
            "identity": {
                "wallet": wallet,
                "wide_policy_fingerprint": fingerprint,
                "policy_id": "wide-paper",
            },
            "venue_executable_full_stream_rescore": {
                "resolved": 95,
                "post_fee_pnl_usd": -7.4,
            },
        }]},
        order_flow_deadman_state={"policy_choke": {
            "source_roster_drought": {"candidate_evidence": {"rows": [{
                "wallet": wallet,
                "wide_policy_fingerprint": fingerprint,
                "fresh_own_source_buy_rows_30m": 10,
                "direct_source": {"policy_depth_pass": 0},
            }]}}
        }},
        now=datetime(2026, 7, 31, 9, 11, tzinfo=UTC),
    )

    lane = next(row for row in state["lanes"] if row["wallet"] == wallet)
    projection = lane["projected_resolved_signals_at_200"]
    assert projection["status"] == "SECOND_CUT_OBSERVED_ZERO_ACCRUAL"
    assert projection["observed_resolved_signals_per_day"] == 0.0
    assert lane["f1_sign_flip_required"] is True
    assert lane["f2_status"]["fresh_own_source_buy_rows_30m"] == 10
    assert lane["f2_status"]["policy_depth_pass"] == 0
    assert lane["f2_status"]["pass"] is False


def test_951b_third_zero_cut_parks_stalled_f1_lane() -> None:
    wallet = "0x951bd740ef681d05891ca35440232488271d433e"
    fingerprint = (
        "dcef9b3028326682bf283dfc028a7225a64bc26b472960e1df1f98ed27d3c078"
    )
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state={"lanes": [{
            "wallet": wallet,
            "wide_policy_fingerprint": fingerprint,
            "measurement_started_at": "2026-07-31T09:00:00Z",
            "measurement_baseline_resolved_signals": 95,
            "f1_accrual_cuts": [
                {"cut_at": "2026-07-31T09:00:00Z", "resolved_signals": 95},
                {"cut_at": "2026-07-31T09:11:00Z", "resolved_signals": 95},
            ],
        }]},
        wide_fingerprint_evidence={"cells": [{
            "identity": {
                "wallet": wallet,
                "wide_policy_fingerprint": fingerprint,
                "policy_id": "wide-paper",
            },
            "venue_executable_full_stream_rescore": {
                "resolved": 95,
                "post_fee_pnl_usd": -7.4,
            },
        }]},
        now=datetime(2026, 7, 31, 9, 22, tzinfo=UTC),
    )

    lane = next(row for row in state["lanes"] if row["wallet"] == wallet)
    projection = lane["projected_resolved_signals_at_200"]
    assert lane["shadow_status"] == "PARKED_ZERO_F1_ACCRUAL"
    assert lane["measurement_terminal"] is True
    assert projection["status"] == "PARKED_ZERO_F1_ACCRUAL"
    assert projection["observed_resolved_signals_per_day"] == 0.0
    assert projection["estimated_days_to_200"] is None
    assert [row["resolved_signals"] for row in lane["f1_accrual_cuts"]] == [95, 95, 95]


def test_951b_negative_cell_parks_at_unchanged_f1_floor() -> None:
    wallet = "0x951bd740ef681d05891ca35440232488271d433e"
    fingerprint = (
        "dcef9b3028326682bf283dfc028a7225a64bc26b472960e1df1f98ed27d3c078"
    )
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        wide_fingerprint_evidence={"cells": [{
            "identity": {
                "wallet": wallet,
                "wide_policy_fingerprint": fingerprint,
                "policy_id": "wide-paper",
            },
            "venue_executable_full_stream_rescore": {
                "resolved": 200,
                "post_fee_pnl_usd": -1.0,
            },
        }]},
        now=datetime(2026, 7, 31, 10, 0, tzinfo=UTC),
    )

    lane = next(row for row in state["lanes"] if row["wallet"] == wallet)
    assert lane["shadow_status"] == "F1_SAMPLE_PARKED_NEGATIVE_AT_FLOOR"
    assert lane["measurement_terminal"] is True
    assert lane["ready_for_live"] is False
    assert lane["projected_resolved_signals_at_200"]["status"] == (
        "PARKED_NEGATIVE_AT_F1_FLOOR"
    )


def test_thin_pass_replay_enters_copyable_buy_watch() -> None:
    state = build_state(
        queue={
            "ranked_members": [
                {
                    "queue_rank": 43,
                    "wallet": "0xad825954d08beba32f74b594821f4251460c3df1",
                    "ready_for_live": False,
                    "replay": {
                        "status": "PASS",
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "paper_pnl_usd": 0.485392,
                        "paper_orders": 11,
                        "resolved_orders": 8,
                        "copyable_buy_events": 8,
                        "candidate_clob_backed_orders": 8,
                    },
                }
            ]
        },
        limit=5,
        gate=50,
    )

    assert state["summary"]["lane_count"] == 1
    assert state["summary"]["copyable_watch"] == 1
    assert state["summary"]["min_copyable_buy_gap"] == 12
    assert state["lanes"][0]["shadow_status"] == "COPYABLE_BUY_WATCH"
    assert state["lanes"][0]["live_orders_allowed"] is False
    assert state["lanes"][0]["next"] == "continue paper shadow until copyable/CLOB-backed BUYs >=20; no threshold cut"


def test_a689_ruling_is_explicitly_bound_to_ready_shadow_and_accrues_fresh_only() -> None:
    now = datetime(2026, 7, 21, 18, 20, tzinfo=UTC)
    common = {
        "queue": {"ranked_members": []},
        "limit": 5,
        "gate": 50,
        "watch_tier_shadow_ev": {
            "wallets": [{"source_wallet": A689_HOT_STANDBY_WALLET, "eligible_signals": 51, "resolved_signals": 41, "pnl_usd": -19.0}]
        },
        "readmission_rulings": {
            "rulings": [{"source_wallet": A689_HOT_STANDBY_WALLET, "ruling": "HOT_STANDBY_PENDING_LIVENESS", "ruling_id": "r1"}]
        },
        "hot_standby_liveness": {
            "rows": [{"wallet": A689_HOT_STANDBY_WALLET, "address_selection": {"recommended_query_key": "user", "last_trade_age_h": 0.1, "user_only_hot_path_supported": True}}]
        },
    }
    first = build_state(**common, now=now)
    lane = next(row for row in first["lanes"] if row["wallet"] == A689_HOT_STANDBY_WALLET)
    assert lane["source_binding_status"] == "WIRED"
    assert lane["resolved_paper_fills"] == 0
    assert lane["standby_evidence_elapsed_h"] == 0.0
    assert lane["hot_standby_ready"] is False

    second_inputs = dict(common)
    second_inputs["watch_tier_shadow_ev"] = {
        "wallets": [{"source_wallet": A689_HOT_STANDBY_WALLET, "eligible_signals": 91, "resolved_signals": 71, "pnl_usd": -10.0}]
    }
    second = build_state(**second_inputs, previous_state=first, now=now + timedelta(hours=48))
    lane = next(row for row in second["lanes"] if row["wallet"] == A689_HOT_STANDBY_WALLET)
    assert lane["resolved_paper_fills"] == 30
    assert lane["in_lane_post_fee_pnl_usd"] == 6.39
    assert lane["standby_evidence_clock_complete"] is True
    assert lane["hot_standby_ready"] is True


def test_source_liveness_age_advances_from_stale_probe_observation() -> None:
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        watch_tier_shadow_ev={
            "wallets": [{"source_wallet": A689_HOT_STANDBY_WALLET, "eligible_signals": 1, "resolved_signals": 0, "pnl_usd": 0.0}]
        },
        readmission_rulings={
            "rulings": [{"source_wallet": A689_HOT_STANDBY_WALLET, "ruling": "HOT_STANDBY_PENDING_LIVENESS", "ruling_id": "r1"}]
        },
        hot_standby_liveness={
            "observed_ts": datetime(2026, 7, 21, 1, 0, tzinfo=UTC).timestamp(),
            "rows": [
                {
                    "wallet": A689_HOT_STANDBY_WALLET,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "user_only_hot_path_supported": True,
                        "last_trade_age_h": 0.5,
                        "last_trade_iso": "2026-07-21T00:30:00Z",
                    },
                }
            ],
        },
        now=datetime(2026, 7, 21, 18, 0, tzinfo=UTC),
    )
    lane = next(row for row in state["lanes"] if row["wallet"] == A689_HOT_STANDBY_WALLET)
    assert lane["source_liveness"]["last_trade_age_h"] == 17.5
    assert lane["source_liveness"]["last_trade_iso"] == "2026-07-21T00:30:00Z"


def test_source_liveness_uses_confirmed_user_route_when_proxy_match_is_fresher() -> None:
    wallet = "0x1313131313131313131313131313131313131313"
    payload = {
        "observed_ts": datetime(2026, 7, 23, 22, 0, tzinfo=UTC).timestamp(),
        "rows": [
            {
                "wallet": wallet,
                "address_selection": {
                    "recommended_query_key": "proxyWallet",
                    "last_trade_age_h": 0.01,
                    "user_last_trade_age_h": 0.02,
                    "user_last_trade_iso": "2026-07-23T21:58:48Z",
                    "user_only_hot_path_supported": True,
                },
            }
        ],
    }

    result = _source_liveness(
        wallet,
        _liveness_by_wallet(payload),
        now=datetime(2026, 7, 23, 22, 1, tzinfo=UTC),
    )

    assert result["status"] == "PASS"
    assert result["living_source"] is True
    assert result["recommended_query_key"] == "user"
    assert result["freshest_query_key"] == "proxyWallet"


def test_ready_queue_lane_arms_paper_canary_clock() -> None:
    wallet = "0x1313131313131313131313131313131313131313"
    state = build_state(
        queue={
            "ranked_members": [
                {
                    "queue_rank": 1,
                    "wallet": wallet,
                    "ready_for_live": True,
                    "bench_liveness": {"status": "READY_AND_ALIVE"},
                    "fresh_flow_rank": {"fresh_flow": True},
                    "replay": {
                        "status": "CLEAR",
                        "policy_id": "exact_policy",
                        "paper_pnl_usd": 5.0,
                        "resolved_orders": 60,
                        "copyable_buy_events": 60,
                        "candidate_clob_backed_orders": 60,
                    },
                }
            ]
        },
        limit=5,
        gate=50,
        hot_standby_liveness={
            "rows": [
                {
                    "wallet": wallet,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "last_trade_age_h": 1.0,
                        "last_trade_iso": "2026-07-21T00:30:00Z",
                        "user_only_hot_path_supported": True,
                    },
                }
            ]
        },
        now=datetime(2026, 7, 23, 22, 1, tzinfo=UTC),
    )

    lane = state["lanes"][0]
    assert lane["canary_path"] == "CLEAR_TO_HOT_STANDBY_PAPER_CANARY"
    assert lane["copyintent_parity_capture_armed"] is True
    assert lane["copyintent_parity_capture"]["status"] == "ARMED_PAPER_ONLY"
    assert lane["copyintent_parity_capture"]["single_submitter_unchanged"] is True
    assert lane["paper_canary_elapsed_h"] == 0.0
    assert lane["paper_canary_minimum_h"] == 24.0
    assert lane["ready_shadow_full_utc_day"] is False
    assert lane["paper_only"] is True
    assert lane["live_orders_allowed"] is False
    assert lane["source_liveness"]["living_source"] is True
    assert lane["live_canary_packet_preconditions"] == {
        "hot_standby_ready": True,
        "fading_clear": False,
        "fresh_external_btc5m_lt_24h": True,
        "fresh_source_active_window": True,
        "ready_shadow_full_utc_day": False,
        "defense_not_in_triggered_rung": False,
    }
    assert lane["readiness_verdict"] == "READY_SHADOW_24H_CANARY_ACCRUING"
    assert lane["next"] == "capture paper CopyIntents for >=1 full UTC day; no live mutation"


def test_enrolled_standby_lane_survives_ranked_queue_rebuild_until_adjudication() -> None:
    wallet = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
    enrolled_at = "2026-07-21T01:38:33.633921Z"
    prior_lane = {
        "wallet": wallet,
        "shadow_status": "GATE_CROSSED",
        "paper_only": True,
        "live_orders_allowed": False,
        "paper_canary_enrolled_at": enrolled_at,
        "paper_canary_minimum_h": 24.0,
        "paper_canary_elapsed_h": 12.0,
        "copyable_buy_events": 284,
        "paper_pnl_usd": 177.809634,
    }
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state={"lanes": [prior_lane]},
        now=datetime(2026, 7, 22, 10, 18, tzinfo=UTC),
    )
    lane = next(row for row in state["lanes"] if row["wallet"] == wallet)
    assert lane["paper_canary_enrolled_at"] == enrolled_at
    assert lane["copyable_buy_events"] == 284
    assert lane["paper_pnl_usd"] == 177.809634
    assert lane["paper_canary_elapsed_h"] > 32.0
    assert lane["sticky_enrolled_standby"] is True
    assert lane["live_orders_allowed"] is False

    adjudicated = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state={"lanes": [{**prior_lane, "standby_clock_adjudicated_at": "2026-07-22T10:00:00Z"}]},
        now=datetime(2026, 7, 22, 10, 18, tzinfo=UTC),
    )
    assert all(row.get("wallet") != wallet for row in adjudicated["lanes"])


def test_mature_stale_volume_standby_releases_slot_to_next_queue_member() -> None:
    stale_wallet = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
    next_wallet = "0x2222222222222222222222222222222222222222"
    enrolled_at = "2026-07-21T01:38:00Z"
    ready_row = lambda rank, wallet: {
        "queue_rank": rank,
        "wallet": wallet,
        "ready_for_live": True,
        "bench_liveness": {"status": "READY_AND_ALIVE"},
        "fresh_flow_rank": {"fresh_flow": True},
        "replay": {
            "status": "PASS",
            "policy_id": "exact",
            "paper_pnl_usd": 10.0,
            "resolved_orders": 60,
            "copyable_buy_events": 60,
            "candidate_clob_backed_orders": 60,
        },
    }
    state = build_state(
        queue={"ranked_members": [ready_row(1, stale_wallet), ready_row(2, next_wallet)]},
        limit=1,
        gate=50,
        previous_state={
            "lanes": [
                {
                    "wallet": stale_wallet,
                    "paper_canary_enrolled_at": enrolled_at,
                    "paper_canary_minimum_h": 24.0,
                }
            ]
        },
        hot_standby_liveness={
            "rows": [
                {
                    "wallet": stale_wallet,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "user_only_hot_path_supported": True,
                        "last_trade_age_h": 72.0,
                        "last_trade_iso": enrolled_at,
                    },
                },
                {
                    "wallet": next_wallet,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "user_only_hot_path_supported": True,
                        "last_trade_age_h": 1.0,
                        "last_trade_iso": "2026-07-24T00:30:00Z",
                    },
                },
            ]
        },
        now=datetime(2026, 7, 24, 1, 38, tzinfo=UTC),
    )

    assert [row["wallet"] for row in state["lanes"]] == [next_wallet]
    ruling = state["standby_adjudications"][0]
    assert ruling["wallet"] == stale_wallet
    assert ruling["status"] == "NO_PROMOTE_DEAD_SOURCE"
    assert ruling["slot_action"] == "RELEASED_TO_NEXT_FULL_POOL_QUEUE_MEMBER"
    assert state["summary"]["dead_source_slots_released"] == 1


def test_dead_source_reenrollment_requires_newer_observed_trade() -> None:
    stale_wallet = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
    next_wallet = "0x2222222222222222222222222222222222222222"
    ruled_trade = "2026-07-21T01:38:00Z"
    ready_rows = [
        {
            "queue_rank": rank,
            "wallet": wallet,
            "ready_for_live": True,
            "replay": {
                "status": "PASS",
                "paper_pnl_usd": 10.0,
                "resolved_orders": 60,
                "copyable_buy_events": 60,
                "candidate_clob_backed_orders": 60,
            },
        }
        for rank, wallet in ((1, stale_wallet), (2, next_wallet))
    ]
    previous = {
        "standby_adjudications": [
            {
                "wallet": stale_wallet,
                "status": "NO_PROMOTE_DEAD_SOURCE",
                "source_last_trade_iso": ruled_trade,
            }
        ]
    }
    unchanged = build_state(
        queue={"ranked_members": ready_rows},
        limit=1,
        gate=50,
        previous_state=previous,
        hot_standby_liveness={
            "rows": [
                {
                    "wallet": stale_wallet,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "user_only_hot_path_supported": True,
                        "last_trade_age_h": 80.0,
                        "last_trade_iso": ruled_trade,
                    },
                }
            ]
        },
        now=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )
    assert unchanged["lanes"][0]["wallet"] == next_wallet
    assert unchanged["summary"]["dead_source_reenrollment_suppressed_wallets"] == [stale_wallet]

    resumed = build_state(
        queue={"ranked_members": ready_rows},
        limit=1,
        gate=50,
        previous_state=previous,
        hot_standby_liveness={
            "rows": [
                {
                    "wallet": stale_wallet,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "user_only_hot_path_supported": True,
                        "last_trade_age_h": 0.1,
                        "last_trade_iso": "2026-07-24T09:54:00Z",
                    },
                }
            ]
        },
        now=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )
    assert resumed["lanes"][0]["wallet"] == stale_wallet
    assert resumed["standby_adjudications"][0]["status"] == "REENROLLMENT_RELEASED_FRESH_TRADE_OBSERVED"


def test_four_way_candidate_enters_fable_gated_ready_shadow_lane() -> None:
    wallet = "0xee888fa7b96007f7fa270988e92bddb0ae19ed10"
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        cohort_admission={
            "paper_only": True,
            "live_orders_allowed": False,
            "top_four_way_candidate": {
                "wallet": wallet,
                "candidate_id": "market_cohort_alive_ddb0ae19ed10",
                "recommendation": "ADMISSION_PACKET_READY",
                "paper_only": True,
                "live_orders_allowed": False,
                "history_completeness": "complete",
                "resolved_copyable_events": 495,
                "paper_pnl_usd": 62.781846,
                "roi_pct": 6.214365,
                "latest_trade_age_h": 11.56,
                "source_active": {
                    "status": "POLICY_ELIGIBLE_PASS",
                    "source_active_tally_status": "PASS",
                    "source_active_windows": 2,
                },
                "external_liveness": {"status": "PASS"},
                "temporal_evidence": {"classification": "FADING"},
            },
        },
    )

    assert state["summary"]["cohort_admission_ready_shadow"] == 1
    lane = state["lanes"][0]
    assert lane["wallet"] == wallet
    assert lane["shadow_status"] == "COHORT_ADMISSION_READY_SHADOW"
    assert lane["paper_only"] is True
    assert lane["live_orders_allowed"] is False
    assert lane["copyintent_parity_capture"]["status"] == "ARMED_PAPER_ONLY"
    assert lane["copyintent_parity_capture"]["single_submitter_unchanged"] is True
    assert lane["live_canary_packet_preconditions"] == {
        "fading_clear": False,
        "fresh_external_btc5m_lt_24h": True,
        "fresh_source_active_window": True,
        "ready_shadow_full_utc_day": False,
        "defense_not_in_triggered_rung": False,
    }
    assert lane["resolved_paper_fills"] == 0
    assert lane["retrospective_resolved_signals"] == 495


def test_watch_tier_readmission_enters_post_fee_pending_shadow_lane() -> None:
    state = build_state(
        queue={
            "ranked_members": [
                {
                    "queue_rank": 1,
                    "wallet": "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0",
                    "ready_for_live": True,
                    "replay": {
                        "status": "PASS",
                        "paper_pnl_usd": 2.0,
                        "resolved_orders": 9,
                        "copyable_buy_events": 15,
                        "candidate_clob_backed_orders": 15,
                    },
                }
            ]
        },
        limit=5,
        gate=50,
        watch_tier_shadow_ev={
            "summary": {
                "wallets_admitted_by_ruling": [
                    "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0",
                    "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473",
                ]
            },
            "wallets": [
                {
                    "source_wallet": "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0",
                    "status": "READMISSION_ADMITTED_MEASUREMENT_ONLY",
                    "readmission_consideration_eligible": True,
                    "roi_pct": 47.760388,
                    "pnl_usd": 72.595789,
                    "resolved_signals": 152,
                    "eligible_signals": 311,
                },
                {
                    "source_wallet": "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473",
                    "status": "READMISSION_RULING_DUE",
                    "readmission_consideration_eligible": True,
                    "roi_pct": 8.19,
                    "pnl_usd": 2.0,
                    "resolved_signals": 31,
                    "eligible_signals": 50,
                },
                {
                    "source_wallet": "0x5e4aa0f176014729f5168e821ce614484fbebe6b",
                    "roi_pct": 59.28,
                    "pnl_usd": 13.6,
                    "resolved_signals": 23,
                    "eligible_signals": 40,
                    "status": "READMISSION_RULING_DUE",
                    "readmission_consideration_eligible": True,
                },
            ]
        },
        watch_tier_readmission_roi_bar_pct=13.7,
    )

    assert state["summary"]["watch_tier_readmitted"] == 1
    assert state["summary"]["lane_count"] == 1
    lane = state["lanes"][0]
    assert lane["wallet"] == "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0"
    assert lane["shadow_status"] == "WATCH_TIER_READMITTED_POST_FEE_PENDING"
    assert lane["paper_only"] is True
    assert lane["live_orders_allowed"] is False
    assert lane["ready_for_live"] is False
    assert lane["resolved_paper_fills"] == 0
    assert lane["in_lane_fresh_resolved_signals"] == 0
    assert lane["retrospective_resolved_signals"] == 152
    assert lane["feed_status"] == "BASELINED_NO_NEW_ORDERS"
    assert lane["admission_status"] == "READMISSION_ADMITTED_MEASUREMENT_ONLY"


def test_watch_tier_readmission_counts_only_post_baseline_feed_delta() -> None:
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state={
            "lanes": [
                {
                    "wallet": "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0",
                    "readmission_started_at": "2026-07-10T20:34:00Z",
                    "feed_baseline_eligible_signals": 311,
                    "feed_baseline_resolved_signals": 152,
                    "feed_baseline_gross_pnl_usd": 72.595789,
                }
            ]
        },
        watch_tier_shadow_ev={
            "summary": {"wallets_due": ["0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0"]},
            "wallets": [
                {
                    "source_wallet": "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0",
                    "status": "READMISSION_RULING_DUE",
                    "readmission_consideration_eligible": True,
                    "roi_pct": 47.0,
                    "pnl_usd": 75.595789,
                    "resolved_signals": 154,
                    "eligible_signals": 316,
                }
            ],
        },
        watch_tier_readmission_roi_bar_pct=13.7,
    )

    lane = state["lanes"][0]
    assert lane["paper_orders"] == 5
    assert lane["in_lane_fresh_resolved_signals"] == 2
    assert lane["in_lane_gross_pnl_usd"] == 3.0
    assert lane["in_lane_post_fee_pnl_usd"] == 2.826
    assert lane["feed_status"] == "FEED_ACCRUING"


def test_watch_tier_bar_crossing_pages_until_hot_standby_ruling_and_liveness_pass() -> None:
    wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
    previous_state = {
        "lanes": [
            {
                "wallet": wallet,
                "shadow_status": "WATCH_TIER_READMITTED_POST_FEE_PENDING",
                "readmission_started_at": "2026-07-10T20:38:15Z",
                "feed_baseline_eligible_signals": 100,
                "feed_baseline_resolved_signals": 40,
                "feed_baseline_gross_pnl_usd": 10.0,
            }
        ]
    }
    watch_tier_shadow_ev = {
        "summary": {"wallets_due": []},
        "wallets": [
            {
                "source_wallet": wallet,
                "status": "READMISSION_ALREADY_LANED",
                "readmission_consideration_eligible": True,
                "roi_pct": 25.0,
                "pnl_usd": 23.0,
                "resolved_signals": 70,
                "eligible_signals": 170,
            }
        ],
    }

    unrulled = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state=previous_state,
        watch_tier_shadow_ev=watch_tier_shadow_ev,
        watch_tier_readmission_roi_bar_pct=13.7,
    )

    lane = unrulled["lanes"][0]
    assert lane["in_lane_fresh_resolved_signals"] == 30
    assert lane["in_lane_post_fee_pnl_usd"] == 10.39
    assert lane["post_fee_evidence_bar_crossed"] is True
    assert lane["page_fable_due"] is True
    assert lane["hot_standby_ready"] is False
    assert unrulled["summary"]["watch_tier_page_fable_due"] == 1

    ruled_live = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state=previous_state,
        watch_tier_shadow_ev=watch_tier_shadow_ev,
        watch_tier_readmission_roi_bar_pct=13.7,
        readmission_rulings={
            "rulings": [
                {
                    "source_wallet": wallet,
                    "ruling": "HOT_STANDBY_PENDING_LIVENESS",
                    "ruling_id": "2026-07-13T15:12Z-fable-hot-standby",
                    "min_roi_pct": 13.7,
                }
            ]
        },
        hot_standby_liveness={
            "rows": [
                {
                    "wallet": wallet,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "user_only_hot_path_supported": True,
                        "last_trade_age_h": 2.5,
                        "last_trade_iso": "2026-07-13T13:00:00Z",
                    },
                }
            ]
        },
    )

    lane = ruled_live["lanes"][0]
    assert lane["page_fable_due"] is False
    assert lane["source_liveness"]["status"] == "PASS"
    assert lane["hot_standby_ready"] is True
    assert lane["succession_eligible"] is True
    assert ruled_live["summary"]["watch_tier_hot_standby_ready"] == 1
    assert ruled_live["summary"]["all_measurement_hot_standby_ready"] == 1
    assert ruled_live["summary"]["sos_top_standby_wallet"] == wallet


def test_watch_tier_readmission_preserves_existing_lane_when_due_clears() -> None:
    wallet = "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0"
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state={
            "lanes": [
                {
                    "wallet": wallet,
                    "shadow_status": "WATCH_TIER_READMITTED_POST_FEE_PENDING",
                    "readmission_started_at": "2026-07-10T20:34:00Z",
                    "feed_baseline_eligible_signals": 311,
                    "feed_baseline_resolved_signals": 152,
                    "feed_baseline_gross_pnl_usd": 72.595789,
                }
            ]
        },
        watch_tier_shadow_ev={
            "summary": {"wallets_due": []},
            "wallets": [
                {
                    "source_wallet": wallet,
                    "status": "READMISSION_ALREADY_LANED",
                    "readmission_consideration_eligible": True,
                    "roi_pct": 47.0,
                    "pnl_usd": 75.595789,
                    "resolved_signals": 154,
                    "eligible_signals": 316,
                }
            ],
        },
        watch_tier_readmission_roi_bar_pct=13.7,
    )

    assert state["summary"]["watch_tier_readmitted"] == 1
    lane = state["lanes"][0]
    assert lane["wallet"] == wallet
    assert lane["shadow_status"] == "WATCH_TIER_READMITTED_POST_FEE_PENDING"
    assert lane["paper_orders"] == 5
    assert lane["in_lane_fresh_resolved_signals"] == 2


def test_breadth_temporal_measurement_lane_overrides_readmission_lane() -> None:
    wallet = "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0"
    state = build_state(
        queue={
            "ranked_members": [
                {
                    "queue_rank": 26,
                    "wallet": wallet,
                    "ready_for_live": False,
                    "resolved_pnl": 2.246536,
                    "breadth_disposition": {
                        "status": "MEASUREMENT_ONLY_TEMPORAL_CLOSURE",
                        "reason": "active weekday UNPROVEN",
                    },
                    "replay": {
                        "status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "paper_pnl_usd": 2.246536,
                        "paper_orders": 87,
                        "resolved_orders": 9,
                        "copyable_buy_events": 9,
                        "candidate_clob_backed_orders": 9,
                    },
                }
            ]
        },
        limit=5,
        gate=50,
        watch_tier_shadow_ev={
            "summary": {"wallets_due": [wallet]},
            "wallets": [
                {
                    "source_wallet": wallet,
                    "status": "READMISSION_RULING_DUE",
                    "readmission_consideration_eligible": True,
                    "roi_pct": 47.0,
                    "pnl_usd": 75.595789,
                    "resolved_signals": 154,
                    "eligible_signals": 316,
                }
            ],
        },
    )

    assert state["summary"]["lane_count"] == 1
    assert state["summary"]["watch_tier_readmitted"] == 0
    assert state["summary"]["breadth_temporal_measurement"] == 1
    lane = state["lanes"][0]
    assert lane["wallet"] == wallet
    assert lane["shadow_status"] == "BREADTH_TEMPORAL_MEASUREMENT_PENDING"
    assert lane["promotion_resolved_fill_gate"] == 10
    assert lane["resolved_paper_fills"] == 9
    assert lane["resolved_fill_gap"] == 1
    assert lane["paper_orders"] == 87
    assert lane["paper_pnl_usd"] == 2.246536
    assert lane["packet_paper_pnl_usd"] == 2.246536
    assert lane["estimated_fee_per_simulated_fill_usd"] == 0.087
    assert lane["in_lane_fee_estimate_usd"] == 0.783
    assert lane["in_lane_post_fee_pnl_usd"] == 1.463536
    assert lane["hot_standby_ready"] is False
    assert lane["live_orders_allowed"] is False


def test_breadth_temporal_hot_standby_requires_positive_post_fee_and_n10() -> None:
    wallet = "0x141d08cb2efe0b57ee1d7d4f524cce12f40f59"
    state = build_state(
        queue={
            "ranked_members": [
                {
                    "queue_rank": 27,
                    "wallet": wallet,
                    "ready_for_live": False,
                    "breadth_disposition": {
                        "status": "MEASUREMENT_ONLY_TEMPORAL_CLOSURE",
                        "paper_pnl_usd": 0.800504,
                        "simulated_copies": 62,
                        "resolved_simulated_copies": 22,
                    },
                    "replay": {
                        "status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "paper_pnl_usd": 0.800504,
                        "paper_orders": 62,
                        "resolved_orders": 22,
                        "copyable_buy_events": 22,
                        "candidate_clob_backed_orders": 22,
                    },
                },
                {
                    "queue_rank": 28,
                    "wallet": "0xpositive0000000000000000000000000000000000",
                    "ready_for_live": False,
                    "breadth_disposition": {
                        "status": "MEASUREMENT_ONLY_TEMPORAL_CLOSURE",
                        "paper_pnl_usd": 2.0,
                        "simulated_copies": 20,
                        "resolved_simulated_copies": 10,
                    },
                    "replay": {
                        "status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "paper_pnl_usd": 2.0,
                        "paper_orders": 20,
                        "resolved_orders": 10,
                        "copyable_buy_events": 10,
                        "candidate_clob_backed_orders": 10,
                    },
                },
            ]
        },
        limit=5,
        gate=50,
    )

    lanes = {row["wallet"]: row for row in state["lanes"]}
    assert lanes[wallet]["in_lane_post_fee_pnl_usd"] == -1.113496
    assert lanes[wallet]["hot_standby_ready"] is False
    assert lanes[wallet]["readiness_verdict"] == "READINESS_DENIED_POST_FEE_NEGATIVE"
    assert lanes[wallet]["succession_eligible"] is False
    assert lanes["0xpositive0000000000000000000000000000000000"]["in_lane_post_fee_pnl_usd"] == 1.13
    assert lanes["0xpositive0000000000000000000000000000000000"]["hot_standby_ready"] is True
    assert state["summary"]["breadth_temporal_hot_standby_ready"] == 1
    assert state["summary"]["sos_top_standby_wallet"] == "0xpositive0000000000000000000000000000000000"


def test_volume_preconditions_refresh_from_current_temporal_liveness_and_defense_inputs() -> None:
    wallet = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
    state = build_state(
        queue={
            "ranked_members": [
                {
                    "queue_rank": 1,
                    "wallet": wallet,
                    "ready_for_live": True,
                    "bench_liveness": {"status": "READY_AND_ALIVE"},
                    "fresh_flow_rank": {"fresh_flow": True},
                    "replay": {
                        "status": "PASS",
                        "policy_id": "exact",
                        "paper_pnl_usd": 177.809634,
                        "resolved_orders": 276,
                        "copyable_buy_events": 279,
                        "candidate_clob_backed_orders": 279,
                    },
                }
            ]
        },
        limit=5,
        gate=50,
        temporal_profitability={
            "generated_at": "2026-07-23T02:00:00Z",
            "wallets": [{"wallet": wallet, "classification": "CONTINUOUS"}],
        },
        scorecard={
            "generated_at": "2026-07-23T02:00:00Z",
            "since_topup_truth": {"actual_delta_vs_baseline_usd": 20.01},
        },
        hot_standby_liveness={
            "rows": [
                {
                    "wallet": wallet,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "last_trade_age_h": 1.0,
                        "last_trade_iso": "2026-07-23T01:00:00Z",
                        "user_only_hot_path_supported": True,
                    },
                }
            ]
        },
        now=datetime(2026, 7, 23, 2, 0, tzinfo=UTC),
    )

    lane = state["lanes"][0]
    assert lane["live_canary_packet_preconditions"]["fading_clear"] is True
    assert lane["live_canary_packet_preconditions"]["fresh_external_btc5m_lt_24h"] is True
    assert lane["live_canary_packet_preconditions"]["defense_not_in_triggered_rung"] is True
    assert lane["live_canary_precondition_evidence"]["fading"]["classification"] == "CONTINUOUS"
    assert lane["live_canary_precondition_evidence"]["defense"]["crossed_floor"] is True


def test_sticky_volume_lane_recomputes_preconditions_instead_of_reusing_snapshot() -> None:
    wallet = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
    state = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state={
            "lanes": [
                {
                    "wallet": wallet,
                    "paper_canary_enrolled_at": "2026-07-23T23:30:00Z",
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "live_canary_packet_preconditions": {
                        "fading_clear": False,
                        "fresh_external_btc5m_lt_24h": False,
                        "defense_not_in_triggered_rung": False,
                    },
                }
            ]
        },
        temporal_profitability={
            "generated_at": "2026-07-24T00:01:00Z",
            "wallets": [{"wallet": wallet, "classification": "CONTINUOUS"}],
        },
        scorecard={"since_topup_truth": {"actual_delta_vs_baseline_usd": 16.0}},
        hot_standby_liveness={
            "rows": [
                {
                    "wallet": wallet,
                    "address_selection": {
                        "recommended_query_key": "user",
                        "last_trade_age_h": 0.5,
                        "last_trade_iso": "2026-07-24T00:00:00Z",
                        "user_only_hot_path_supported": True,
                    },
                }
            ]
        },
        now=datetime(2026, 7, 24, 0, 1, tzinfo=UTC),
    )

    lane = state["lanes"][0]
    assert lane["sticky_enrolled_standby"] is True
    assert lane["live_canary_packet_preconditions"]["fading_clear"] is True
    assert lane["live_canary_packet_preconditions"]["fresh_external_btc5m_lt_24h"] is True
    assert lane["live_canary_packet_preconditions"]["defense_not_in_triggered_rung"] is True
    assert lane["live_canary_precondition_evidence"]["defense"]["utc_release_due"] is True


def test_terminal_82c8_capacity_release_survives_readiness_regeneration() -> None:
    wallet = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    refreshed = build_state(
        queue={
            "ranked_members": [{
                "queue_rank": 1,
                "wallet": wallet,
                "ready_for_live": False,
                "replay": {
                    "status": "PASS",
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "paper_pnl_usd": 10.0,
                    "paper_orders": 40,
                    "resolved_orders": 30,
                    "copyable_buy_events": 30,
                    "candidate_clob_backed_orders": 30,
                },
            }]
        },
        limit=5,
        gate=30,
        previous_state={
            "terminal_82c8_decision": {"status": "PARK_VOLUME_STANDBY_PAPER_ONLY"},
            "lanes": [],
        },
        now=datetime(2026, 7, 25, 19, 0, tzinfo=UTC),
    )
    assert all(row.get("wallet") != wallet for row in refreshed["lanes"])
