import pytest

from scripts.report_288_participation_map import build_map
from scripts.report_e1_framework_audit_inputs import (
    build_packet,
    counted_reject_cluster,
    daily_gate_conversion,
    highest_rejection_gate_ev,
    maker_fallback_activation_evidence,
    multi_day_roi_distribution,
)


def _scorecard() -> dict:
    return {
        "generated_at": "2026-07-21T00:17:00Z",
        "volume_kpi": {
            "rows": [
                {
                    "window_start_s": 1784592000,
                    "wallet_eligible_orders": 4,
                    "our_submits": 1,
                    "our_fills": 1,
                    "skip_reasons": {},
                },
                {
                    "window_start_s": 1784592300,
                    "wallet_eligible_orders": 3,
                    "our_submits": 0,
                    "our_fills": 0,
                    "skip_reasons": {"window_time_gte_60s": 1},
                },
            ]
        },
        "canonical_pnl_truth": {
            "events": [
                {
                    "market_slug": "btc-updown-5m-1784592000",
                    "status": "FILLED",
                    "resolved": True,
                    "pnl_usd": 1.25,
                    "lane": "lane-a",
                    "source_wallet": "0xabc",
                }
            ]
        },
    }


def _reject(role: str, error: str, submitted_at: str, *, intent_id: str, limit_price: float) -> dict:
    row = {
        "submitted_at": submitted_at,
        "final_status": "REJECTED",
        "intent_id": intent_id,
        "market_slug": "btc-updown-5m-1784660400",
        "execution_role": role,
        "limit_price": limit_price,
        "lifecycle": [{"payload": {"error": error}}],
    }
    if "min-share bump would exceed wallet-copy policy cap" in error.lower():
        row["trade_decision"] = {
            "strategy_reason": "wallet_copy_fak_miss_maker_fallback",
        }
    return row


def test_288_map_has_all_slots_and_measured_participation_semantics() -> None:
    report = build_map(_scorecard(), day="2026-07-21")

    assert report["row_count"] == 288
    assert report["rows"][0]["participation_status"] == "traded"
    assert report["rows"][0]["resolved_pnl_usd"] == 1.25
    assert report["rows"][0]["resolved_pnl_available"] is True
    assert report["rows"][1]["participation_status"] == "skipped_with_measured_reason"
    assert report["rows"][1]["owner"] == "RND_TRACK_B_EXECUTION_LATENCY"
    assert report["rows"][2]["participation_status"] == "uncovered_with_owner"
    assert report["rows"][2]["owner"] == "RND_TRACK_A_MEASUREMENT_COVERAGE"
    assert report["rows"][2]["measurement_status"] == "UNMEASURED"
    assert report["rows"][2]["participation_predicate"] == "producer_ran_and_wrote_no_row"
    assert report["summary"]["elapsed_uncovered_without_classification"] == 0
    assert report["rows"][4]["lifecycle"] == "future_pending"
    assert report["summary"]["win_rate_is_gate"] is False
    assert report["hour_band_aggregates"][0]["resolved_pnl_usd"] == 1.25


def test_reject_cluster_counts_fak_and_ruled_ceiling_refusal_without_estimation() -> None:
    since = "2026-07-21T18:50:00Z"
    ledger = {
        "orders": [
            _reject(
                "taker",
                "no orders found to match with FAK order",
                "2026-07-21T19:00:01Z",
                intent_id="ci-1",
                limit_price=0.49,
            ),
            _reject(
                "maker",
                "CLOB min-share bump would exceed wallet-copy policy cap",
                "2026-07-21T19:00:03Z",
                intent_id="ci-1",
                limit_price=0.49,
            ),
            _reject(
                "maker",
                "CLOB min-share bump would exceed wallet-copy policy cap",
                "2026-07-21T19:01:03Z",
                intent_id="ci-2",
                limit_price=0.60,
            ),
            _reject(
                "maker",
                "CLOB five-share minimum would exceed the Fable-ruled hard ceiling",
                "2026-07-21T19:00:05Z",
                intent_id="ci-3",
                limit_price=0.51,
            ),
            _reject(
                "maker",
                "unclassified exchange refusal",
                "2026-07-21T19:00:07Z",
                intent_id="ci-4",
                limit_price=0.40,
            ),
            _reject("taker", "old", "2026-07-21T18:40:00Z", intent_id="old", limit_price=0.4),
        ]
    }

    report = counted_reject_cluster(ledger, since=since, until="2026-07-21T19:01:00Z")

    assert report["reject_rows"] == 4
    assert report["distinct_intents"] == 3
    assert report["ledger_newest_submitted_at"] == "2026-07-21T19:01:03Z"
    assert report["taxonomy_counts"] == {
        "fak_no_match": 1,
        "policy_cap_maker_fallback": 1,
        "unknown": 1,
        "venue_min_share_hard_ceiling_exceeded": 1,
    }
    assert report["ruled_ceiling_refused_by_tighter_cap_rejects"] == 1
    assert report["rows"][1]["ruled_ceiling_refused_by_tighter_cap"] is True
    assert report["rows"][2]["ruled_ceiling_refused_by_tighter_cap"] is False


def test_packet_publishes_full_utc_day_reject_taxonomy_alongside_window() -> None:
    ledger = {
        "orders": [
            _reject(
                "maker",
                "CLOB min-share bump would exceed wallet-copy policy cap",
                "2026-07-21T01:00:00Z",
                intent_id="early",
                limit_price=0.49,
            ),
            _reject(
                "taker",
                "no orders found to match with FAK order",
                "2026-07-21T19:00:01Z",
                intent_id="window",
                limit_price=0.49,
            ),
            _reject(
                "maker",
                "CLOB min-share bump would exceed wallet-copy policy cap",
                "2026-07-22T01:00:00Z",
                intent_id="next-day",
                limit_price=0.49,
            ),
        ]
    }

    packet = build_packet(
        _scorecard(),
        ledger,
        day="2026-07-21",
        reject_since="2026-07-21T18:50:00Z",
        reject_until="2026-07-22T00:00:00Z",
    )

    assert packet["reject_cluster"]["taxonomy_counts"] == {"fak_no_match": 1}
    assert packet["full_utc_day_reject_cluster"]["taxonomy_counts"] == {
        "fak_no_match": 1,
        "policy_cap_maker_fallback": 1,
    }
    assert (
        packet["full_utc_day_reject_cluster"][
            "ruled_ceiling_refused_by_tighter_cap_rejects"
        ]
        == 1
    )
    assert packet["scorecard_input_generated_at"] == "2026-07-21T00:17:00Z"
    assert packet["e1_vs_scorecard_lag_s"] >= 0
    assert packet["scorecard_input_freshness_status"] == "STALE_BEYOND_ONE_CYCLE"


def test_gate_conversion_is_sequential_and_ev_joins_resolved_paper_rows() -> None:
    funnel = {
        "day_utc": "2026-07-22",
        "source_wallet": "0xabc",
        "policy_eligible_unique_intents": 10,
        "terminal_stage_counts": {
            "not_selected_live_seat": 2,
            "profit_latency_suppression": 5,
            "exchange_rejected": 1,
            "accepted_live_order": 2,
        },
        "rows": [
            {"intent_id": f"ci-{index}", "terminal_stage": "profit_latency_suppression"}
            for index in range(5)
        ],
    }
    conversion = daily_gate_conversion(funnel)
    assert [(row["signals_in"], row["survivors_out"]) for row in conversion["rows"]] == [
        (10, 8),
        (8, 3),
        (3, 2),
    ]
    assert conversion["accounting_gap"] == 0
    routing = {
        "fee_gated_measurement_rows": [
            {
                "intent_id": "ci-0",
                "shares": 2,
                "limit_price": 0.5,
                "realized_paper_outcome": {"resolution_status": "resolved", "paper_pnl_usd": 0.8},
            },
            {
                "intent_id": "ci-0",
                "shares": 2,
                "limit_price": 0.5,
                "realized_paper_outcome": {"resolution_status": "resolved", "paper_pnl_usd": 0.8},
            },
            {
                "intent_id": "ci-1",
                "shares": 2,
                "limit_price": 0.5,
                "realized_paper_outcome": {"resolution_status": "pending"},
            },
        ]
    }
    ev = highest_rejection_gate_ev(conversion, funnel, routing)
    assert ev["gate"] == "profit_latency_suppression"
    assert ev["resolved_counterfactual_intents"] == 1
    assert ev["paper_counterfactual_roi_pct"] == 80.0


def test_multi_day_roi_distribution_uses_cost_weighted_aggregate() -> None:
    scorecards = [
        {"day_utc": "2026-07-05", "today": {"total": {"cost_usd": 100, "pnl_usd": 10, "resolved_fills": 4}}},
        {"day_utc": "2026-07-06", "today": {"total": {"cost_usd": 50, "pnl_usd": -10, "resolved_fills": 2}}},
    ]
    report = multi_day_roi_distribution(scorecards, since_day="2026-07-05")
    assert report["days_with_cost"] == 2
    assert report["distribution"]["aggregate_roi_pct"] == 0.0
    assert report["distribution"]["positive_days"] == 1
    assert report["distribution"]["negative_days"] == 1


def _fallback_attempt(
    *,
    order_id: str,
    submitted_at: str,
    status: str,
    cost: float,
    cap: float = 2.5,
    error: str = "",
) -> dict:
    row = {
        "order_id": order_id,
        "submitted_at": submitted_at,
        "final_status": status,
        "limit_price": cost / 5.0,
        "maker_min_share_effective_cap_usd": cap,
        "maker_min_share_bump_cost_usd": cost,
        "trade_decision": {
            "strategy_reason": "wallet_copy_fak_miss_maker_fallback",
        },
    }
    if error:
        row["error_class"] = "maker_min_share_bump_exceeds_policy_cap"
        row["lifecycle"] = [{"payload": {"error": error}}]
    return row


def test_maker_fallback_activation_packet_proves_submit_refusal_and_pnl() -> None:
    ledger = {
        "orders": [
            _fallback_attempt(
                order_id="bumped-fill",
                submitted_at="2026-07-30T00:10:00Z",
                status="FILLED",
                cost=2.45,
            ),
            _fallback_attempt(
                order_id="above-cap",
                submitted_at="2026-07-30T00:15:00Z",
                status="REJECTED",
                cost=2.55,
                error="CLOB min-share bump would exceed wallet-copy policy cap",
            ),
            {
                **_fallback_attempt(
                    order_id="excluded-reason",
                    submitted_at="2026-07-30T00:20:00Z",
                    status="REJECTED",
                    cost=2.4,
                    error="CLOB min-share bump would exceed wallet-copy policy cap",
                ),
                "trade_decision": {
                    "strategy_reason": "wallet_copy_passive_at_source",
                },
            },
        ]
    }
    scorecard = {
        "since_topup_truth": {"actual_delta_vs_baseline_usd": -8.25},
        "canonical_pnl_truth": {
            "events": [
                {
                    "order_id": "bumped-fill",
                    "resolved": True,
                    "pnl_usd": 0.49,
                }
            ]
        }
    }

    report = maker_fallback_activation_evidence(
        ledger,
        scorecard,
        since="2026-07-30T00:01:07Z",
        until="2026-07-31T00:00:00Z",
    )

    assert report["addressable_attempts"] == 2
    assert report["bumped_submitted"] == 1
    assert report["still_refused"] == 1
    assert report["cap_keys_present_attempts"] == 2
    assert report["reject_taxonomy_counts"] == {
        "fak_no_match": 0,
        "policy_cap_other": 0,
        "policy_cap_maker_fallback": 1,
        "policy_cap_passive_at_source": 0,
        "entry_price_band_closed_negative_holdout": 0,
        "passive_at_source_lane_closed": 0,
        "unknown": 0,
        "venue_min_share_hard_ceiling_exceeded": 0,
    }
    assert report["conservation"]["gap"] == 0
    assert report["resolved_bumped_fills"] == 1
    assert report["effective_notional_usd"] == 2.45
    assert report["post_fee_pnl_usd"] == 0.49
    assert report["post_fee_roi_pct"] == 20.0
    assert report["abort_b"]["probe_triggered"] is True
    assert report["abort_b"]["kill_triggered"] is False
    assert report["abort_c_scope_leaks"] == 0
    assert report["verdict"] == "PASS"


def test_maker_fallback_activation_fails_refusal_inside_emitted_cap() -> None:
    ledger = {
        "orders": [
            _fallback_attempt(
                order_id="inside-cap",
                submitted_at="2026-07-30T00:15:00Z",
                status="REJECTED",
                cost=2.4,
                error="CLOB min-share bump would exceed wallet-copy policy cap",
            )
        ]
    }

    report = maker_fallback_activation_evidence(
        ledger,
        {},
        since="2026-07-30T00:01:07Z",
        until="2026-07-31T00:00:00Z",
    )

    assert report["refused_inside_effective_cap"] == 1
    assert report["verdict"] == "FAIL_REFUSED_INSIDE_EFFECTIVE_CAP"


def test_maker_fallback_activation_zero_supply_is_insufficient() -> None:
    report = maker_fallback_activation_evidence(
        {"orders": []},
        {},
        since="2026-07-30T00:01:07Z",
        until="2026-07-31T00:00:00Z",
    )

    assert report["addressable_attempts"] == 0
    assert report["verdict"] == "INSUFFICIENT_SUPPLY"
    assert set(report["reject_taxonomy_counts"]) == {
        "fak_no_match",
        "policy_cap_other",
        "policy_cap_maker_fallback",
        "policy_cap_passive_at_source",
        "entry_price_band_closed_negative_holdout",
        "passive_at_source_lane_closed",
        "unknown",
        "venue_min_share_hard_ceiling_exceeded",
    }


def test_maker_fallback_activation_excludes_legitimate_passive_bump_from_scope_leak() -> None:
    passive = _fallback_attempt(
        order_id="passive",
        submitted_at="2026-07-30T00:20:00Z",
        status="FILLED",
        cost=0.95,
        cap=1.0,
    )
    passive["trade_decision"]["strategy_reason"] = "wallet_copy_passive_at_source"

    report = maker_fallback_activation_evidence(
        {"orders": [passive]},
        {},
        since="2026-07-30T00:01:07Z",
        until="2026-07-31T00:00:00Z",
    )

    assert report["addressable_attempts"] == 0
    assert report["abort_c_scope_leaks"] == 0


@pytest.mark.parametrize("leaked_cap", [2.5, 8.0])
def test_maker_fallback_activation_surfaces_hard_ceiling_scope_leak(
    leaked_cap: float,
) -> None:
    leaked = _fallback_attempt(
        order_id="wrong-reason",
        submitted_at="2026-07-30T00:20:00Z",
        status="FILLED",
        cost=2.4,
        cap=leaked_cap,
    )
    leaked["trade_decision"]["strategy_reason"] = "wallet_copy_passive_at_source"

    report = maker_fallback_activation_evidence(
        {"orders": [leaked]},
        {},
        since="2026-07-30T00:01:07Z",
        until="2026-07-31T00:00:00Z",
    )

    assert report["addressable_attempts"] == 0
    assert report["abort_c_scope_leaks"] == 1


def test_reject_cluster_splits_passive_policy_cap_from_fallback() -> None:
    fallback = _reject(
        "maker",
        "CLOB min-share bump would exceed wallet-copy policy cap",
        "2026-07-30T00:20:00Z",
        intent_id="fallback",
        limit_price=0.49,
    )
    passive = {
        **_reject(
            "maker",
            "CLOB min-share bump would exceed wallet-copy policy cap",
            "2026-07-30T00:25:00Z",
            intent_id="passive",
            limit_price=0.44,
        ),
        "trade_decision": {"strategy_reason": "wallet_copy_passive_at_source"},
    }

    report = counted_reject_cluster(
        {"orders": [fallback, passive]},
        since="2026-07-30T00:00:00Z",
    )

    assert report["taxonomy_counts"] == {
        "policy_cap_maker_fallback": 1,
        "policy_cap_passive_at_source": 1,
    }
    assert report["ruled_ceiling_refused_by_tighter_cap_rejects"] == 1


def test_reject_cluster_registers_named_passive_lane_closure() -> None:
    closed = _reject(
        "maker",
        "passive-at-source live lane closed",
        "2026-08-02T16:55:00Z",
        intent_id="closed",
        limit_price=0.44,
    )
    closed["trade_result"] = {
        "order_id": "",
        "error_class": "passive_at_source_lane_closed",
    }

    report = counted_reject_cluster(
        {"orders": [closed]},
        since="2026-08-02T00:00:00Z",
    )

    assert report["taxonomy_counts"] == {"passive_at_source_lane_closed": 1}


def test_reject_cluster_registers_named_entry_band_closure() -> None:
    closed = _reject(
        "taker",
        "entry price band closed by negative holdout",
        "2026-08-02T17:15:00Z",
        intent_id="entry-band-closed",
        limit_price=0.44,
    )
    closed["trade_result"] = {
        "order_id": "",
        "error_class": "entry_price_band_closed_negative_holdout",
    }

    report = counted_reject_cluster(
        {"orders": [closed]},
        since="2026-08-02T00:00:00Z",
    )

    assert report["taxonomy_counts"] == {
        "entry_price_band_closed_negative_holdout": 1
    }
