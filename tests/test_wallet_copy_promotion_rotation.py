from __future__ import annotations

from scripts.report_fill_quality import build_fill_quality_report
from scripts.select_wallet_copy_promotion_rotation import _merge_clearance_candidates
from src.wallet_copy.promotion_rotation import (
    PromotionRotationConfig,
    build_promotion_rotation_state,
    evaluate_inactivity_rotation,
    evaluate_live_rotation,
    select_promotion_candidates,
)


PRE_WEEKEND_AWARE_NOW_TS = 1_783_260_000.0


def _filled_order(*, order_id: str, price: float, side: str, submitted_at: str) -> dict:
    return {
        "order_id": order_id,
        "final_status": "FILLED",
        "condition_id": "cond-up",
        "side": side,
        "limit_price": price,
        "submitted_at": submitted_at,
        "trade_result": {
            "response_filled_size_usd": 0.5,
            "response_fill_size_shares": 1.0,
        },
    }


def _candidate_evidence(*, depth: float = 5.0, fill_sample: int = 24) -> dict:
    return {
        "ready_for_live": True,
        "max_recent_ask_depth_usd": depth,
        "copyability_profile_gate_enabled": True,
        "copyability_profile_eligible": True,
        "execution_profile": {
            "latency_horizon_s": 2.0,
            "fill_sample": fill_sample,
            "copyable_rate_pct": 80.0,
            "mean_edge": 0.01,
            "median_edge": 0.01,
        },
    }


def test_price_band_decision_window_includes_exact_cap_and_triggers_rotation() -> None:
    ledger = {
        "orders": [
            *[
                _filled_order(
                    order_id=f"loss-{index}",
                    price=0.50,
                    side="NO",
                    submitted_at=f"2026-07-04T00:3{index}:00+00:00",
                )
                for index in range(10)
            ],
            _filled_order(
                order_id="too-expensive",
                price=0.51,
                side="YES",
                submitted_at="2026-07-04T00:35:00+00:00",
            ),
            _filled_order(
                order_id="too-old",
                price=0.50,
                side="YES",
                submitted_at="2026-07-04T00:10:00+00:00",
            ),
        ]
    }

    report = build_fill_quality_report(
        ledger,
        {"cond-up": {"direction": "UP"}},
        price_band_decision_since="2026-07-04T00:25:00Z",
        price_band_decision_max_price=0.50,
        price_band_decision_min_resolved=10,
        price_band_decision_min_fill_rate_pct=40.0,
    )

    window = report["price_band_decision_window"]
    assert window["enabled"] is True
    assert window["status"] == "CORRECTION"
    assert window["orders"] == 10
    assert window["resolved_filled"] == 10
    assert window["realized_pnl_usd"] == -5.0
    assert window["rotation_triggered"] is True
    assert "price_band_realized_pnl_negative" in window["blockers"]


def test_price_band_decision_window_triggers_on_rolling_last_20_loss() -> None:
    ledger = {
        "orders": [
            *[
                _filled_order(
                    order_id=f"old-win-{index}",
                    price=0.50,
                    side="YES",
                    submitted_at=f"2026-07-04T00:{index:02d}:00+00:00",
                )
                for index in range(40)
            ],
            *[
                _filled_order(
                    order_id=f"recent-loss-{index}",
                    price=0.50,
                    side="NO",
                    submitted_at=f"2026-07-04T01:{index:02d}:00+00:00",
                )
                for index in range(20)
            ],
        ]
    }

    report = build_fill_quality_report(
        ledger,
        {"cond-up": {"direction": "UP"}},
        price_band_decision_since="2026-07-04T00:00:00Z",
        price_band_decision_max_price=0.50,
        price_band_decision_min_resolved=10,
        price_band_decision_min_fill_rate_pct=40.0,
    )

    window = report["price_band_decision_window"]
    rolling = window["rolling_rotation_trigger"]
    assert window["realized_pnl_usd"] == 10.0
    assert rolling["sample_ready"] is True
    assert rolling["resolved_filled"] == 20
    assert rolling["realized_pnl_usd"] == -10.0
    assert rolling["rotation_triggered"] is True
    assert window["rotation_triggered"] is True
    assert "rolling_20_resolved_pnl_below_loss_threshold" in window["blockers"]

    state = evaluate_live_rotation(report)
    assert state["status"] == "CORRECTION"
    assert state["action"] == "ROTATE_LIVE_WALLET"
    assert state["rolling_rotation_trigger"]["rotation_triggered"] is True


def test_live_guard_price_band_snapshot_pins_fill_quality_window() -> None:
    from scripts.run_wallet_copy_live_guard import _live_price_band_decision_snapshot

    ledger = {
        "orders": [
            _filled_order(
                order_id=f"win-{index}",
                price=0.45,
                side="YES",
                submitted_at=f"2026-07-04T15:1{index}:00+00:00",
            )
            for index in range(10)
        ]
    }

    snapshot = _live_price_band_decision_snapshot(
        ledger,
        {"cond-up": {"direction": "UP"}},
        since="2026-07-04T15:01:00Z",
        max_price=0.50,
        min_resolved=10,
        min_fill_rate_pct=40.0,
    )

    window = snapshot["price_band_decision_window"]
    assert snapshot["source"] == "scripts/report_fill_quality.py"
    assert snapshot["ledger_orders"] == 10
    assert "--price-band-decision-since" in snapshot["argv_equivalent"]
    assert window["status"] == "PASS"
    assert window["resolved_filled"] == 10
    assert window["rotation_triggered"] is False


def test_live_rotation_waits_for_resolution_threshold() -> None:
    state = evaluate_live_rotation(
        {
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 4,
                "realized_pnl_usd": -4.0,
                "fill_rate_pct": 100.0,
                "rotation_triggered": False,
                "blockers": ["price_band_resolved_sample_below_threshold"],
            }
        }
    )

    assert state["status"] == "WATCH"
    assert state["action"] == "CONTINUE_LIVE_PRICE_BAND_SAMPLE"
    assert state["rotation_triggered"] is False


def test_promotion_candidates_require_positive_paper_and_copyable_policy() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    lane_state = {
        "ranked_wallets": [
            {
                "rank": 1,
                "wallet": wallet,
                "live_executable_paper_eligible": True,
                **_candidate_evidence(),
                "paper_eligible_policy_ids": ["segmented_25pct"],
                "paper_policy_gate": {
                    "best_policy_id": "segmented_25pct",
                    "best_policy_paper_pnl_usd": 0.42,
                    "best_policy_copyable_buy_events": 20,
                    "best_policy_copyable_rate_pct": 66.666667,
                },
            },
            {
                "rank": 2,
                "wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "live_executable_paper_eligible": True,
                "paper_eligible_policy_ids": ["mission_5pct"],
                "paper_policy_gate": {
                    "best_policy_id": "mission_5pct",
                    "best_policy_paper_pnl_usd": -0.01,
                    "best_policy_copyable_buy_events": 21,
                },
            },
        ]
    }

    candidates = select_promotion_candidates(lane_state)

    assert [row["wallet"] for row in candidates] == [wallet]
    assert candidates[0]["best_policy_id"] == "segmented_25pct"
    assert candidates[0]["best_policy_paper_pnl_usd"] == 0.42


def test_promotion_candidates_reject_sample_thin_default_floor() -> None:
    lane_state = {
        "ranked_wallets": [
            {
                "rank": 1,
                "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "live_executable_paper_eligible": True,
                "paper_eligible_policy_ids": ["segmented_25pct"],
                "paper_policy_gate": {
                    "best_policy_id": "segmented_25pct",
                    "best_policy_paper_pnl_usd": 0.75,
                    "best_policy_copyable_buy_events": 8,
                },
            }
        ]
    }

    assert select_promotion_candidates(lane_state) == []


def test_promotion_candidates_preserve_and_require_execution_profile_gate() -> None:
    wallet_pass = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_fail = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    lane_state = {
        "ranked_wallets": [
            {
                "rank": 1,
                "wallet": wallet_fail,
                "live_executable_paper_eligible": True,
                "copyability_profile_gate_enabled": True,
                "copyability_profile_eligible": False,
                "paper_eligible_policy_ids": ["mission_5pct"],
                "paper_policy_gate": {
                    "best_policy_id": "mission_5pct",
                    "best_policy_paper_pnl_usd": 1.0,
                    "best_policy_copyable_buy_events": 21,
                },
            },
            {
                "rank": 2,
                "wallet": wallet_pass,
                "live_executable_paper_eligible": True,
                "copyability_profile_gate_enabled": True,
                "copyability_profile_eligible": True,
                "max_recent_ask_depth_usd": 5.0,
                "execution_profile": {
                    "latency_horizon_s": 2.0,
                    "fill_sample": 22,
                    "copyable_rate_pct": 72.727273,
                    "mean_edge": 0.012,
                    "median_edge": 0.01,
                },
                "paper_eligible_policy_ids": ["mission_5pct"],
                "paper_policy_gate": {
                    "best_policy_id": "mission_5pct",
                    "best_policy_paper_pnl_usd": 0.5,
                    "best_policy_copyable_buy_events": 20,
                },
            },
        ]
    }

    candidates = select_promotion_candidates(lane_state)

    assert [row["wallet"] for row in candidates] == [wallet_pass]
    assert candidates[0]["copyability_profile"]["enabled"] is True
    assert candidates[0]["copyability_profile"]["fill_sample"] == 22
    assert candidates[0]["copyability_profile"]["mean_edge"] == 0.012


def test_promotion_rotation_state_is_fable_ready_when_live_loses_and_candidate_exists() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 10,
                "realized_pnl_usd": -1.25,
                "fill_rate_pct": 45.0,
                "rotation_triggered": True,
                "blockers": ["price_band_realized_pnl_negative"],
            }
        },
        lane_state={
            "ranked_wallets": [
                {
                    "rank": 1,
                    "wallet": wallet,
                    "live_executable_paper_eligible": True,
                    **_candidate_evidence(),
                    "paper_eligible_policy_ids": ["segmented_25pct"],
                    "paper_policy_gate": {
                    "best_policy_id": "segmented_25pct",
                    "best_policy_paper_pnl_usd": 0.75,
                    "best_policy_copyable_buy_events": 24,
                },
                }
            ]
        },
        config=PromotionRotationConfig(min_live_resolved_fills=10),
    )

    assert state["status"] == "PASS"
    assert state["decision"]["action"] == "FABLE_ROTATION_DECISION_READY"
    assert state["decision"]["requires_fable_decision"] is True
    assert state["decision"]["best_candidate_wallet"] == wallet
    assert state["decision"]["rotation_application_allowed"] is True
    assert state["decision"]["rotation_application_interlock"]["active"] is False
    assert state["leak_rules_1_2"]["ready"] is True


def test_promotion_rotation_interlock_stays_active_when_leak_rule_disabled() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 10,
                "realized_pnl_usd": -1.25,
                "fill_rate_pct": 45.0,
                "rotation_triggered": True,
                "blockers": ["price_band_realized_pnl_negative"],
            }
        },
        lane_state={
            "ranked_wallets": [
                {
                    "rank": 1,
                    "wallet": wallet,
                    "live_executable_paper_eligible": True,
                    **_candidate_evidence(),
                    "paper_eligible_policy_ids": ["segmented_25pct"],
                    "paper_policy_gate": {
                        "best_policy_id": "segmented_25pct",
                        "best_policy_paper_pnl_usd": 0.75,
                        "best_policy_copyable_buy_events": 24,
                    },
                }
            ]
        },
        config=PromotionRotationConfig(
            min_live_resolved_fills=10,
            live_our_fill_pnl_outranks_paper_for_retention=False,
        ),
    )

    assert state["decision"]["action"] == "FABLE_ROTATION_DECISION_READY"
    assert state["decision"]["rotation_application_allowed"] is False
    assert state["decision"]["rotation_application_interlock"]["active"] is True
    assert state["decision"]["rotation_application_interlock"]["reason"] == "leak_rules_1_2_absent"


def test_promotion_rotation_lifts_application_interlock_when_leak_rules_are_ready() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 10,
                "realized_pnl_usd": -1.25,
                "fill_rate_pct": 45.0,
                "rotation_triggered": True,
                "blockers": ["price_band_realized_pnl_negative"],
            }
        },
        lane_state={
            "leak_rules_1_2": {
                "live_our_fill_pnl_outranks_paper": True,
                "tripwire_clocks_pause_while_deadman_red": True,
            },
            "ranked_wallets": [
                {
                    "rank": 1,
                    "wallet": wallet,
                    "live_executable_paper_eligible": True,
                    **_candidate_evidence(),
                    "paper_eligible_policy_ids": ["segmented_25pct"],
                    "paper_policy_gate": {
                        "best_policy_id": "segmented_25pct",
                        "best_policy_paper_pnl_usd": 0.75,
                        "best_policy_copyable_buy_events": 24,
                    },
                }
            ],
        },
        config=PromotionRotationConfig(min_live_resolved_fills=10),
    )

    assert state["decision"]["action"] == "FABLE_ROTATION_DECISION_READY"
    assert state["decision"]["rotation_application_allowed"] is True
    assert state["decision"]["rotation_application_interlock"]["active"] is False


def test_inactivity_rotation_triggers_only_for_active_promotable_alternate() -> None:
    live_wallet = "0xd97ae021645712fe5cf73139049383a100cac068"
    alternate_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 14,
                "realized_pnl_usd": 13.84,
                "fill_rate_pct": 80.0,
                "rotation_triggered": False,
                "blockers": [],
            }
        },
        live_execution_state={
            "orders": [
                {
                    "source_wallet": live_wallet,
                    "paper_only": False,
                    "live_orders_allowed": True,
                    "submitted_at": "2026-07-05T06:00:00+00:00",
                    "final_status": "FILLED",
                    "order_id": "live-old",
                }
            ]
        },
        lane_state={
            "ranked_wallets": [
                {
                    "rank": 1,
                    "wallet": alternate_wallet,
                    "live_executable_paper_eligible": True,
                    **_candidate_evidence(),
                    "recent_copy_sized_buy_events": 2,
                    "paper_eligible_policy_ids": ["segmented_25pct"],
                    "paper_policy_gate": {
                        "best_policy_id": "segmented_25pct",
                        "best_policy_paper_pnl_usd": 0.75,
                        "best_policy_copyable_buy_events": 24,
                    },
                }
            ]
        },
        config=PromotionRotationConfig(
            live_inactivity_rotation_threshold_s=3 * 60 * 60,
            min_live_resolved_fills=10,
        ),
        now_ts=PRE_WEEKEND_AWARE_NOW_TS,
    )

    inactivity = state["inactivity_rotation"]
    assert inactivity["rotation_triggered"] is True
    assert inactivity["evaluated_at"]
    assert inactivity["live_source_wallet"] == live_wallet
    assert inactivity["active_promotable_candidate_count"] == 1
    assert inactivity["best_active_candidate_wallet"] == alternate_wallet
    assert inactivity["considered_active_candidate_wallets"] == [alternate_wallet]
    assert state["decision"]["action"] == "FABLE_ROTATION_DECISION_READY"
    assert state["decision"]["price_rotation_triggered"] is False
    assert state["decision"]["inactivity_rotation_triggered"] is True
    assert state["decision"]["best_candidate_wallet"] == alternate_wallet


def test_live_positive_rolling_our_fill_pnl_suppresses_inactivity_rotation() -> None:
    live_wallet = "0xd97ae021645712fe5cf73139049383a100cac068"
    alternate_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 30,
                "realized_pnl_usd": 4.0,
                "fill_rate_pct": 80.0,
                "rotation_triggered": False,
                "rolling_rotation_trigger": {
                    "enabled": True,
                    "sample_ready": True,
                    "resolved_filled": 20,
                    "realized_pnl_usd": 2.5,
                    "rotation_triggered": False,
                    "sample_orders": [{"order_id": f"canonical-{idx}", "pnl_usd": 0.125} for idx in range(5)],
                    "bases": {
                        "canonical": {
                            "enabled": True,
                            "sample_ready": True,
                            "resolved_filled": 20,
                            "realized_pnl_usd": 2.5,
                            "sample_orders": [{"order_id": f"canonical-{idx}", "pnl_usd": 0.125} for idx in range(5)],
                        },
                        "reconciled": {
                            "enabled": True,
                            "sample_ready": True,
                            "resolved_filled": 20,
                            "realized_pnl_usd": 1.75,
                            "sample_orders": [{"order_id": f"reconciled-{idx}", "pnl_usd": 0.0875} for idx in range(5)],
                        },
                    },
                },
            }
        },
        live_execution_state={
            "can_trade": True,
            "live_orders_allowed": True,
            "orders": [
                {
                    "source_wallet": live_wallet,
                    "paper_only": False,
                    "live_orders_allowed": True,
                    "submitted_at": "2026-07-05T06:00:00+00:00",
                    "final_status": "FILLED",
                    "order_id": "live-old",
                }
            ],
        },
        lane_state={
            "ranked_wallets": [
                {
                    "rank": 1,
                    "wallet": alternate_wallet,
                    "live_executable_paper_eligible": True,
                    **_candidate_evidence(),
                    "recent_copy_sized_buy_events": 2,
                    "paper_eligible_policy_ids": ["segmented_25pct"],
                    "paper_policy_gate": {
                        "best_policy_id": "segmented_25pct",
                        "best_policy_paper_pnl_usd": 0.75,
                        "best_policy_copyable_buy_events": 24,
                    },
                }
            ]
        },
        config=PromotionRotationConfig(
            live_inactivity_rotation_threshold_s=3 * 60 * 60,
            min_live_resolved_fills=10,
        ),
        now_ts=PRE_WEEKEND_AWARE_NOW_TS,
    )

    inactivity = state["inactivity_rotation"]
    assert inactivity["rotation_triggered"] is False
    assert inactivity["rotation_suppressed"]["original_rotation_triggered"] is True
    assert "live_positive_rolling_our_fill_pnl_retention_protected" in inactivity["rotation_suppressed"]["reasons"]
    assert state["decision"]["inactivity_rotation_triggered"] is False
    assert state["decision"]["action"] == "PROMOTION_CANDIDATE_READY_LIVE_CAN_STAY"


def test_cumulative_empty_rolling_sample_cannot_suppress_inactivity_rotation() -> None:
    live_wallet = "0xd97ae021645712fe5cf73139049383a100cac068"
    alternate_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 328,
                "realized_pnl_usd": 14.487717,
                "fill_rate_pct": 80.0,
                "rotation_triggered": False,
                "rolling_rotation_trigger": {
                    "enabled": False,
                    "window_size": 20,
                    "sample_ready": True,
                    "resolved_filled": 328,
                    "realized_pnl_usd": 14.487717,
                    "rotation_triggered": False,
                    "sample_orders": [],
                },
            }
        },
        live_execution_state={
            "can_trade": True,
            "live_orders_allowed": True,
            "orders": [
                {
                    "source_wallet": live_wallet,
                    "paper_only": False,
                    "live_orders_allowed": True,
                    "submitted_at": "2026-07-05T06:00:00+00:00",
                    "final_status": "FILLED",
                    "order_id": "live-old",
                }
            ],
        },
        lane_state={
            "ranked_wallets": [
                {
                    "rank": 1,
                    "wallet": alternate_wallet,
                    "live_executable_paper_eligible": True,
                    **_candidate_evidence(),
                    "recent_copy_sized_buy_events": 2,
                    "paper_eligible_policy_ids": ["segmented_25pct"],
                    "paper_policy_gate": {
                        "best_policy_id": "segmented_25pct",
                        "best_policy_paper_pnl_usd": 0.75,
                        "best_policy_copyable_buy_events": 24,
                    },
                }
            ]
        },
        config=PromotionRotationConfig(
            live_inactivity_rotation_threshold_s=3 * 60 * 60,
            min_live_resolved_fills=10,
        ),
        now_ts=PRE_WEEKEND_AWARE_NOW_TS,
    )

    leak_1 = state["leak_rules_1_2"]["leak_1_live_retention"]
    assert leak_1["suppresses_non_pnl_rotation"] is False
    assert "canonical_rolling_disabled_or_missing" in leak_1["blockers"]
    assert "canonical_rolling_sample_exceeds_window_size" in leak_1["blockers"]
    assert "canonical_rolling_sample_orders_missing" in leak_1["blockers"]
    assert state["inactivity_rotation"]["rotation_triggered"] is True
    assert state["decision"]["action"] == "FABLE_ROTATION_DECISION_READY"


def test_deadman_or_unsubmittable_state_pauses_inactivity_tripwire_clock() -> None:
    live_wallet = "0xd97ae021645712fe5cf73139049383a100cac068"
    alternate_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 14,
                "realized_pnl_usd": 13.84,
                "fill_rate_pct": 80.0,
                "rotation_triggered": False,
            }
        },
        live_execution_state={
            "can_trade": False,
            "live_orders_allowed": True,
            "orders": [
                {
                    "source_wallet": live_wallet,
                    "paper_only": False,
                    "live_orders_allowed": True,
                    "submitted_at": "2026-07-05T06:00:00+00:00",
                    "final_status": "FILLED",
                    "order_id": "live-old",
                }
            ],
        },
        order_flow_deadman_state={"status": "INCIDENT_ORDER_FLOW_DEAD", "can_trade": False},
        lane_state={
            "ranked_wallets": [
                {
                    "rank": 1,
                    "wallet": alternate_wallet,
                    "live_executable_paper_eligible": True,
                    **_candidate_evidence(),
                    "recent_copy_sized_buy_events": 2,
                    "paper_eligible_policy_ids": ["segmented_25pct"],
                    "paper_policy_gate": {
                        "best_policy_id": "segmented_25pct",
                        "best_policy_paper_pnl_usd": 0.75,
                        "best_policy_copyable_buy_events": 24,
                    },
                }
            ]
        },
        config=PromotionRotationConfig(
            live_inactivity_rotation_threshold_s=3 * 60 * 60,
            min_live_resolved_fills=10,
        ),
        now_ts=PRE_WEEKEND_AWARE_NOW_TS,
    )

    inactivity = state["inactivity_rotation"]
    assert inactivity["rotation_triggered"] is False
    assert "tripwire_clock_paused_until_submittable_window" in inactivity["rotation_suppressed"]["reasons"]
    assert state["leak_rules_1_2"]["leak_2_tripwire_clock"]["active"] is True
    assert state["leak_rules_1_2"]["leak_2_tripwire_clock"]["pause_reasons"] == [
        "guard_cannot_submit",
        "deadman_red",
        "deadman_cannot_trade",
    ]


def test_inactivity_rotation_waits_without_active_promotable_alternate() -> None:
    state = evaluate_inactivity_rotation(
        live_execution_state={
            "orders": [
                {
                    "source_wallet": "0xd97ae021645712fe5cf73139049383a100cac068",
                    "paper_only": False,
                    "live_orders_allowed": True,
                    "submitted_at": "2026-07-05T09:00:00+00:00",
                }
            ]
        },
        lane_state={
            "ranked_wallets": [
                {
                    "rank": 1,
                    "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "live_executable_paper_eligible": True,
                    **_candidate_evidence(),
                    "recent_copy_sized_buy_events": 0,
                    "paper_eligible_policy_ids": ["segmented_25pct"],
                    "paper_policy_gate": {
                        "best_policy_id": "segmented_25pct",
                        "best_policy_paper_pnl_usd": 0.75,
                        "best_policy_copyable_buy_events": 24,
                    },
                }
            ]
        },
        config=PromotionRotationConfig(live_inactivity_rotation_threshold_s=3 * 60 * 60),
        now_ts=1783260000.0,
    )

    assert state["rotation_triggered"] is False
    assert state["evaluated_at"] == "2026-07-05T14:00:00+00:00"
    assert state["promotable_candidate_count"] == 1
    assert state["active_promotable_candidate_count"] == 0
    assert "no_promotable_candidate_active_in_activity_window" in state["blockers"]


def test_calendar_profile_suppresses_expected_inactive_weekend_clock() -> None:
    live_wallet = "0xd97ae021645712fe5cf73139049383a100cac068"
    alternate_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    zero_weights = {f"{dow}:{hour:02d}": 0.0 for dow in range(7) for hour in range(24)}
    state = evaluate_inactivity_rotation(
        live_execution_state={
            "orders": [
                {
                    "source_wallet": live_wallet,
                    "paper_only": False,
                    "live_orders_allowed": True,
                    "submitted_at": "2026-07-11T07:00:00+00:00",
                }
            ]
        },
        lane_state={
            "ranked_wallets": [
                {
                    "rank": 1,
                    "wallet": alternate_wallet,
                    "live_executable_paper_eligible": True,
                    **_candidate_evidence(),
                    "recent_copy_sized_buy_events": 2,
                    "paper_eligible_policy_ids": ["segmented_25pct"],
                    "paper_policy_gate": {
                        "best_policy_id": "segmented_25pct",
                        "best_policy_paper_pnl_usd": 0.75,
                        "best_policy_copyable_buy_events": 24,
                    },
                }
            ]
        },
        config=PromotionRotationConfig(live_inactivity_rotation_threshold_s=3 * 60 * 60),
        now_ts=1_783_771_200.0,
        dow_profile_state={
            "profiles_by_wallet": {
                live_wallet: {
                    "wallet": live_wallet,
                    "trade_count": 20,
                    "weekend_evidence_status": "HAS_WEEKEND_SAMPLE",
                    "expected_active_dow_hour_weights": zero_weights,
                }
            }
        },
    )

    assert state["rotation_triggered"] is False
    assert state["calendar_clock"]["expected_active_age_s"] == 0.0
    assert state["calendar_clock"]["expected_active_age_below_threshold"] is True
    assert "calendar_expected_active_inactivity_below_threshold" in state["blockers"]


def test_promotion_rotation_blocks_losing_live_when_no_candidate_exists() -> None:
    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 10,
                "realized_pnl_usd": -1.25,
                "fill_rate_pct": 45.0,
                "rotation_triggered": True,
            }
        },
        lane_state={"ranked_wallets": []},
    )

    assert state["status"] == "CORRECTION"
    assert state["decision"]["action"] == "RETAIN_NO_ELIGIBLE_CANDIDATE"
    assert "rotation_triggered_without_promotable_candidate" in state["decision"]["blockers"]


def test_clearance_ready_candidate_counts_as_rotation_candidate() -> None:
    wallet = "0x141d08cb2efe0b57ee1d7d7d4f524cce12f40f59"
    config = PromotionRotationConfig(min_live_resolved_fills=10)
    lane_state = _merge_clearance_candidates(
        {"ranked_wallets": []},
        {
            "kind": "wallet_copy_queue_clearance_gaps",
            "candidates": [
                {
                    "classification": "CLEAR",
                    "queue_rank": 1,
                    "ready_for_live": True,
                    "wallet": wallet,
                    "metrics": {
                        "candidate_clob_backed_orders": 18,
                        "copyable_buy_events": 18,
                        "paper_pnl_usd": 0.800504,
                        "resolved_orders": 18,
                        "unresolved_filled_order_count": 0,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                }
            ],
        },
        config,
    )

    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 10,
                "realized_pnl_usd": -1.25,
                "fill_rate_pct": 45.0,
                "rotation_triggered": True,
            }
        },
        lane_state=lane_state,
        config=config,
    )

    assert lane_state["clearance_candidates_summary"]["promotable_replays"] == 1
    assert state["decision"]["action"] == "FABLE_ROTATION_DECISION_READY"
    assert state["decision"]["best_candidate_wallet"] == wallet
    assert state["paper_promotion"]["best_candidate"]["evidence_source"] == "wallet_copy_queue_clearance_gaps"
    assert state["paper_promotion"]["best_candidate"]["candidate_evidence_bar"]["copyable_buy_events"] == 18


def test_bucket_fallback_negative_pnl_cannot_trigger_rotation() -> None:
    state = evaluate_live_rotation(
        {
            "buckets": {
                "01_25_50": {
                    "orders": 20,
                    "filled": 20,
                    "resolved_filled": 20,
                    "realized_pnl_usd": -8.0,
                    "fill_rate_pct": 100.0,
                }
            }
        }
    )

    assert state["rotation_triggered"] is False
    assert state["action"] != "ROTATE_LIVE_WALLET"
    assert "live_price_band_decision_window_missing" in state["blockers"]
    assert "fallback_negative_pnl_observed_not_actionable" in state["blockers"]


def test_historical_clearance_without_current_queue_readiness_is_not_rotation_destination() -> None:
    wallet = "0x927f7694de44d19a72bce76254e628d1c141d215"
    config = PromotionRotationConfig(min_live_resolved_fills=10)
    lane_state = _merge_clearance_candidates(
        {"ranked_wallets": []},
        {
            "candidates": [
                {
                    "classification": "CLEAR",
                    "queue_rank": 1,
                    "ready_for_live": True,
                    "wallet": wallet,
                    "metrics": {
                        "candidate_clob_backed_orders": 20,
                        "copyable_buy_events": 20,
                        "paper_pnl_usd": 1.0,
                        "resolved_orders": 20,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                }
            ]
        },
        config,
        current_queue_ready_wallets=set(),
    )
    state = build_promotion_rotation_state(
        live_fill_report={
            "price_band_decision_window": {
                "enabled": True,
                "resolved_filled": 10,
                "realized_pnl_usd": -1.0,
                "fill_rate_pct": 100.0,
                "rotation_triggered": True,
            }
        },
        lane_state=lane_state,
        config=config,
    )

    assert lane_state["clearance_candidates_summary"]["promotable_replays"] == 0
    assert state["decision"]["action"] == "RETAIN_NO_ELIGIBLE_CANDIDATE"
    assert state["decision"]["best_candidate_wallet"] is None
