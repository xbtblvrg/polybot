from scripts.build_queue_clearance_gaps import build_manifest
from scripts.replay_discover_live_band_candidates import (
    _annotate_reject_blocking_reason,
    _failure_reasons,
)


def test_replay_rejected_fill_gate_uses_ratio_not_presence() -> None:
    reasons = _failure_reasons(
        intent_count=10,
        summary={"unresolved_ratio": 0.0, "pnl_usd": 1.0},
        fill_summary={
            "filled_orders": 8,
            "candidate_clob_backed_orders": 8,
            "candidate_rejected_fill_count": 2,
        },
        max_unresolved_ratio=0.5,
        max_rejected_fill_ratio=0.30,
    )

    assert "candidate_rejected_fill_events_present" not in reasons
    assert "candidate_rejected_fill_ratio_above_maximum" not in reasons

    rejected = _failure_reasons(
        intent_count=10,
        summary={"unresolved_ratio": 0.0, "pnl_usd": 1.0},
        fill_summary={
            "filled_orders": 6,
            "candidate_clob_backed_orders": 6,
            "candidate_rejected_fill_count": 4,
        },
        max_unresolved_ratio=0.5,
        max_rejected_fill_ratio=0.30,
    )

    assert "candidate_rejected_fill_ratio_above_maximum" in rejected


def test_replay_reject_attribution_labels_missing_book_status_for_clearance() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    annotated_order = _annotate_reject_blocking_reason(
        {
            "final_status": "REJECTED",
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-1783202400",
            "fill_estimate": {
                "source": "no_book_rejected",
                "status": "REJECTED",
                "reject_reason": "missing_clob_book_evidence",
                "reject_stage": "missing_execution_evidence",
                "reject_details": {"book_status": "BOOK_NOT_FOUND_OR_CLOSED"},
            },
        }
    )

    fill = annotated_order["fill_estimate"]
    assert fill["blocking_reason"] == "book_not_found_or_closed"
    assert fill["reject_details"]["blocking_reason"] == "book_not_found_or_closed"

    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "failure_reasons": ["candidate_rejected_fill_ratio_above_maximum"],
                        "paper_orders": 1,
                        "policy_buy_events": 1,
                        "copyable_buy_events": 1,
                        "resolved_orders": 1,
                        "paper_pnl_usd": 1.0,
                        "candidate_clob_backed_orders": 1,
                        "unresolved_ratio": 0.0,
                        "replay_orders": [annotated_order],
                    },
                }
            ]
        },
        resolutions={"0xabc": {"market_slug": "btc-updown-5m-1783202400", "winning_outcome": "YES"}},
        limit=14,
        max_rejected_fill_ratio=0.30,
    )

    row = payload["candidates"][0]
    assert row["reject_reasons"] == {"book_not_found_or_closed": 1}


def _filled_order(index: int = 0) -> dict:
    return {
        "final_status": "FILLED",
        "condition_id": "0xabc",
        "market_slug": "btc-updown-5m-1783202400",
        "token_id": f"tok{index}",
    }


def _rejected_order(reason: str) -> dict:
    return {
        "final_status": "REJECTED",
        "condition_id": "0xabc",
        "market_slug": "btc-updown-5m-1783202400",
        "fill_estimate": {"reject_details": {"blocking_reason": reason}},
    }


def test_queue_clearance_recomputes_reject_ratio_over_attributable_classes() -> None:
    wallet = "0x5151515151515151515151515151515151515151"
    replay_orders = [
        *[_filled_order(index) for index in range(8)],
        *[_rejected_order("price_above_slippage_cap") for _ in range(2)],
        *[_rejected_order("book_not_found_or_closed") for _ in range(13)],
    ]

    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "failure_reasons": ["candidate_rejected_fill_ratio_above_maximum"],
                        "paper_orders": len(replay_orders),
                        "policy_buy_events": 8,
                        "copyable_buy_events": 8,
                        "resolved_orders": 8,
                        "paper_pnl_usd": 1.0,
                        "candidate_clob_backed_orders": 8,
                        "unresolved_ratio": 0.0,
                        "replay_orders": replay_orders,
                    },
                }
            ]
        },
        resolutions={"0xabc": {"market_slug": "btc-updown-5m-1783202400", "winning_outcome": "YES"}},
        limit=14,
        max_rejected_fill_ratio=0.45,
    )

    row = payload["candidates"][0]
    assert row["classification"] == "CLEAR"
    assert row["failed_gates"] == []
    assert row["metrics"]["raw_reject_ratio"] == 0.652174
    assert row["metrics"]["attributable_reject_numerator"] == 2
    assert row["metrics"]["attributable_reject_denominator"] == 10
    assert row["metrics"]["attributable_reject_ratio"] == 0.2
    assert row["metrics"]["attributable_reject_floor_status"] == "PASS"
    assert row["metrics"]["environment_reject_count"] == 13


def test_queue_clearance_charges_unknown_rejects_to_candidate() -> None:
    wallet = "0x6161616161616161616161616161616161616161"
    replay_orders = [
        *[_filled_order(index) for index in range(8)],
        *[_rejected_order("unknown_reject") for _ in range(8)],
    ]

    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "failure_reasons": ["candidate_rejected_fill_ratio_above_maximum"],
                        "paper_orders": len(replay_orders),
                        "policy_buy_events": 8,
                        "copyable_buy_events": 8,
                        "resolved_orders": 8,
                        "paper_pnl_usd": 1.0,
                        "candidate_clob_backed_orders": 8,
                        "unresolved_ratio": 0.0,
                        "replay_orders": replay_orders,
                    },
                }
            ]
        },
        resolutions={"0xabc": {"market_slug": "btc-updown-5m-1783202400", "winning_outcome": "YES"}},
        limit=14,
        max_rejected_fill_ratio=0.45,
    )

    row = payload["candidates"][0]
    assert row["classification"] == "ANALYZE_GATE_CLEARANCE_PENDING"
    assert row["failed_gates"] == ["attributable_reject_ratio_above_maximum"]
    assert row["metrics"]["attributable_reject_numerator"] == 8
    assert row["metrics"]["attributable_reject_denominator"] == 16
    assert row["metrics"]["attributable_reject_ratio"] == 0.5
    assert row["metrics"]["unknown_reject_charges_candidate"] is True


def test_queue_clearance_requires_attributable_sample_floor() -> None:
    wallet = "0x7171717171717171717171717171717171717171"
    replay_orders = [
        *[_filled_order(index) for index in range(5)],
        *[_rejected_order("book_not_found_or_closed") for _ in range(50)],
    ]

    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "failure_reasons": ["candidate_rejected_fill_ratio_above_maximum"],
                        "paper_orders": len(replay_orders),
                        "policy_buy_events": 5,
                        "copyable_buy_events": 5,
                        "resolved_orders": 5,
                        "paper_pnl_usd": 1.0,
                        "candidate_clob_backed_orders": 5,
                        "unresolved_ratio": 0.0,
                        "replay_orders": replay_orders,
                    },
                }
            ]
        },
        resolutions={"0xabc": {"market_slug": "btc-updown-5m-1783202400", "winning_outcome": "YES"}},
        limit=14,
        max_rejected_fill_ratio=0.45,
    )

    row = payload["candidates"][0]
    assert row["classification"] == "ANALYZE_GATE_CLEARANCE_PENDING"
    assert row["failed_gates"] == ["insufficient_attributable_reject_sample"]
    assert row["metrics"]["attributable_reject_denominator"] == 5
    assert row["metrics"]["attributable_reject_floor_status"] == "FAILED_INSUFFICIENT_ATTRIBUTABLE_SAMPLE"


def test_queue_clearance_manifest_itemizes_top_candidate_gates() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "failure_reasons": [
                            "candidate_missing_clob_fill_evidence",
                            "candidate_paper_pnl_not_positive",
                            "candidate_unresolved_ratio_above_maximum",
                        ],
                        "paper_orders": 1,
                        "resolved_orders": 0,
                        "paper_pnl_usd": 0.0,
                        "candidate_clob_backed_orders": 0,
                        "unresolved_ratio": 1.0,
                        "replay_orders": [
                            {
                                "final_status": "FILLED",
                                "condition_id": "0xabc",
                                "market_slug": "btc-updown-5m-1783202400",
                                "token_id": "tok",
                            }
                        ],
                    },
                }
            ]
        },
        resolutions={},
        limit=14,
        max_rejected_fill_ratio=0.30,
    )

    row = payload["candidates"][0]
    assert row["classification"] == "UNMEASURABLE_RESOLUTION_BLIND_SPOT"
    assert row["failed_gates"] == [
        "missing_clob_fill_evidence",
        "paper_pnl_not_positive",
        "resolution_attachment_or_market_lifecycle",
    ]
    assert payload["summary"]["targeted_resolution_market_slugs"] == ["btc-updown-5m-1783202400"]
    assert payload["summary"]["unmeasurable_count"] == 1
    assert row["window_coverage"]["missing_resolution_market_count"] == 1


def test_queue_clearance_manifest_treats_replay_pass_as_clear() -> None:
    wallet = "0x2222222222222222222222222222222222222222"
    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 1},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": True}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "failure_reasons": [],
                        "paper_orders": 2,
                        "resolved_orders": 1,
                        "paper_pnl_usd": 1.0,
                        "candidate_clob_backed_orders": 2,
                        "unresolved_ratio": 0.5,
                        "replay_orders": [
                            {
                                "final_status": "FILLED",
                                "condition_id": "0xabc",
                                "market_slug": "btc-updown-5m-1783202400",
                                "token_id": "tok",
                            }
                        ],
                    },
                }
            ]
        },
        resolutions={"0xabc": {"market_slug": "btc-updown-5m-1783202400", "winning_outcome": "YES"}},
        limit=14,
        max_rejected_fill_ratio=0.30,
    )

    assert payload["candidates"][0]["classification"] == "CLEAR"
    assert payload["candidates"][0]["metrics"]["unresolved_ratio"] == 0.0
    assert payload["candidates"][0]["metrics"]["unresolved_filled_order_count"] == 0
    assert payload["summary"]["targeted_resolution_market_slugs"] == []


def test_queue_clearance_ignores_stale_unresolved_ratio_failure_when_absolute_counts_are_clear() -> None:
    wallet = "0x4444444444444444444444444444444444444444"
    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_STALE_UNRESOLVED_RATIO",
                        "failure_reasons": ["candidate_unresolved_ratio_above_maximum"],
                        "paper_orders": 1,
                        "policy_buy_events": 1,
                        "copyable_buy_events": 1,
                        "resolved_orders": 1,
                        "paper_pnl_usd": 1.0,
                        "candidate_clob_backed_orders": 1,
                        "unresolved_ratio": 1.0,
                        "replay_orders": [
                            {
                                "final_status": "FILLED",
                                "condition_id": "0xabc",
                                "market_slug": "btc-updown-5m-1783202400",
                                "token_id": "tok",
                            }
                        ],
                    },
                }
            ]
        },
        resolutions={"0xabc": {"market_slug": "btc-updown-5m-1783202400", "winning_outcome": "YES"}},
        limit=14,
        max_rejected_fill_ratio=0.30,
    )

    row = payload["candidates"][0]
    assert row["metrics"]["unresolved_ratio"] == 0.0
    assert row["metrics"]["unresolved_filled_order_count"] == 0
    assert "resolution_attachment_or_market_lifecycle" not in row["failed_gates"]
    assert row["failed_gates"] == []
    assert row["classification"] == "CLEAR"


def test_queue_clearance_manifest_marks_lane_coverage_without_copyable_buys() -> None:
    wallet = "0x3333333333333333333333333333333333333333"
    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "failure_reasons": ["candidate_missing_clob_fill_evidence"],
                        "paper_orders": 1,
                        "policy_buy_events": 1,
                        "copyable_buy_events": 0,
                        "resolved_orders": 0,
                        "paper_pnl_usd": 0.0,
                        "candidate_clob_backed_orders": 0,
                        "unresolved_ratio": 0.0,
                        "replay_orders": [
                            {
                                "final_status": "REJECTED",
                                "condition_id": "0xabc",
                                "market_slug": "btc-updown-5m-1783202400",
                                "fill_estimate": {"reject_details": {"blocking_reason": "no_ask_liquidity"}},
                            }
                        ],
                    },
                }
            ]
        },
        resolutions={},
        limit=14,
        max_rejected_fill_ratio=0.30,
    )

    row = payload["candidates"][0]
    assert row["classification"] == "FAIL_LANE_COVERAGE"
    assert row["reject_reasons"] == {"no_ask_liquidity": 1}
    assert payload["summary"]["lane_coverage_fail_count"] == 1


def test_queue_clearance_recomputes_reject_ratio_over_candidate_attributable_reasons() -> None:
    wallet = "0x8888888888888888888888888888888888888888"
    orders = [
        {"final_status": "FILLED", "condition_id": "0xabc", "market_slug": "btc-updown-5m-1783202400"}
        for _ in range(11)
    ]
    orders.extend(
        {
            "final_status": "REJECTED",
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-1783202400",
            "fill_estimate": {"reject_details": {"blocking_reason": "book_not_found_or_closed"}},
        }
        for _ in range(40)
    )
    orders.append(
        {
            "final_status": "REJECTED",
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-1783202400",
            "fill_estimate": {"reject_details": {"blocking_reason": "price_above_slippage_cap"}},
        }
    )

    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_REJECT_RATIO",
                        "failure_reasons": ["candidate_rejected_fill_ratio_above_maximum"],
                        "paper_orders": len(orders),
                        "policy_buy_events": len(orders),
                        "copyable_buy_events": 11,
                        "resolved_orders": 11,
                        "paper_pnl_usd": 1.25,
                        "candidate_clob_backed_orders": 11,
                        "replay_orders": orders,
                    },
                }
            ]
        },
        resolutions={"0xabc": {"market_slug": "btc-updown-5m-1783202400", "winning_outcome": "YES"}},
        limit=14,
        max_rejected_fill_ratio=0.45,
    )

    row = payload["candidates"][0]
    assert row["classification"] == "CLEAR"
    assert row["failed_gates"] == []
    assert row["metrics"]["reject_ratio"] == 0.788462
    assert row["metrics"]["attributable_reject_ratio"] == 0.083333
    assert row["metrics"]["attributable_denominator"] == 12
    assert row["metrics"]["environment_rejects"] == 40
    assert row["metrics"]["attributable_rejects"] == 1
    assert row["metrics"]["attributable_sample_floor_met"] is True


def test_queue_clearance_unknown_reject_charges_candidate() -> None:
    wallet = "0x9999999999999999999999999999999999999999"
    orders = [
        {"final_status": "FILLED", "condition_id": "0xabc", "market_slug": "btc-updown-5m-1783202400"}
        for _ in range(10)
    ]
    orders.extend(
        {
            "final_status": "REJECTED",
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-1783202400",
            "fill_estimate": {"reject_details": {"blocking_reason": "unknown_reject"}},
        }
        for _ in range(10)
    )

    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_REJECT_RATIO",
                        "failure_reasons": ["candidate_rejected_fill_ratio_above_maximum"],
                        "paper_orders": len(orders),
                        "policy_buy_events": len(orders),
                        "copyable_buy_events": 10,
                        "resolved_orders": 10,
                        "paper_pnl_usd": 1.25,
                        "candidate_clob_backed_orders": 10,
                        "replay_orders": orders,
                    },
                }
            ]
        },
        resolutions={"0xabc": {"market_slug": "btc-updown-5m-1783202400", "winning_outcome": "YES"}},
        limit=14,
        max_rejected_fill_ratio=0.45,
    )

    row = payload["candidates"][0]
    assert row["classification"] == "ANALYZE_GATE_CLEARANCE_PENDING"
    assert row["failed_gates"] == ["attributable_reject_ratio_above_maximum"]
    assert row["metrics"]["reject_reason_class_counts"] == {"candidate_attributable": 10}
    assert row["metrics"]["attributable_reject_ratio"] == 0.5


def test_queue_clearance_requires_attributable_sample_floor() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    orders = [
        {"final_status": "FILLED", "condition_id": "0xabc", "market_slug": "btc-updown-5m-1783202400"}
        for _ in range(8)
    ]
    orders.extend(
        {
            "final_status": "REJECTED",
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-1783202400",
            "fill_estimate": {"reject_details": {"blocking_reason": "book_not_found_or_closed"}},
        }
        for _ in range(20)
    )

    payload = build_manifest(
        queue={
            "summary": {"queue_depth": 1, "ready_for_live": 0},
            "ranked_members": [{"queue_rank": 1, "wallet": wallet, "ready_for_live": False}],
        },
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_REJECT_RATIO",
                        "failure_reasons": ["candidate_rejected_fill_ratio_above_maximum"],
                        "paper_orders": len(orders),
                        "policy_buy_events": len(orders),
                        "copyable_buy_events": 8,
                        "resolved_orders": 8,
                        "paper_pnl_usd": 1.25,
                        "candidate_clob_backed_orders": 8,
                        "replay_orders": orders,
                    },
                }
            ]
        },
        resolutions={"0xabc": {"market_slug": "btc-updown-5m-1783202400", "winning_outcome": "YES"}},
        limit=14,
        max_rejected_fill_ratio=0.45,
    )

    row = payload["candidates"][0]
    assert row["classification"] == "ANALYZE_GATE_CLEARANCE_PENDING"
    assert row["failed_gates"] == ["insufficient_attributable_reject_sample"]
    assert row["metrics"]["attributable_reject_ratio"] == 0.0
    assert row["metrics"]["attributable_denominator"] == 8
    assert row["metrics"]["attributable_sample_floor_met"] is False
