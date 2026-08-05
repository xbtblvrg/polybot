from __future__ import annotations

import asyncio
import time
import json
from dataclasses import replace
from types import SimpleNamespace

import scripts.run_wallet_copy_live_execution as live_execution
import scripts.run_wallet_copy_live_guard as guard
from src.wallet_copy.models import CopyIntent
from src.wallet_copy.store import atomic_write_json, load_json


def _intent(*, source_tag: str, lane: str, window_start: int, price: float, size_usd: float = 1.0) -> CopyIntent:
    return CopyIntent(
        intent_id=f"ci_{lane}_{source_tag.lower()}",
        source_wallet=source_tag,
        wallet_name=lane,
        source_event_id=f"event_{lane}",
        condition_id=f"cond_{window_start}",
        market_slug=f"btc-updown-5m-{window_start}",
        outcome="Down",
        side="NO",
        limit_price=price,
        wallet_usdc_size=size_usd,
        copy_size_usd=size_usd,
        shares=round(size_usd / price, 6),
        observed_ts=time.time(),
        strategy_family=lane,
        policy_id=f"{lane}_policy",
        sizing_policy_id=f"fixed_{size_usd}",
        mode="paper",
        action="BUY",
        token_id=f"token_{lane}",
        event_ts=time.time(),
        live_orders_allowed=False,
        metadata={"copy_model": "maker_first_btc5m" if lane.startswith("e5") else "e6_whale_side"},
    )


def _args(tmp_path):
    args = SimpleNamespace(
        shadow_lanes=True,
        shadow_state=str(tmp_path / "shadow_state.json"),
        shadow_event_log=str(tmp_path / "shadow_events.jsonl"),
        shadow_max_intents_per_lane=6,
        shadow_retain_rows=100,
        e5_shadow_lane_state=str(tmp_path / "e5_state.json"),
        e6_shadow_lane_state=str(tmp_path / "e6_lane_state.json"),
        e6_shadow_paper_state=str(tmp_path / "e6_paper_state.json"),
        live_ledger_state=str(tmp_path / "live_ledger.json"),
        profit_state=str(tmp_path / "profit_state.json"),
        history_state=str(tmp_path / "history_state.json"),
        history_window_index=str(tmp_path / "history_window_index.json"),
        live_arm_state=str(tmp_path / "live_arm_state.json"),
        live_ledger_event_log=str(tmp_path / "live_ledger_events.jsonl"),
        operator_approval_id="OP-LIVE-TEST",
        max_event_age_s=30.0,
        live_build_max_observed_age_s=30.0,
        max_intents=6,
        shadow_max_event_age_s=300.0,
        shadow_live_build_max_observed_age_s=300.0,
        price_band_decision_max_price=0.50,
        min_live_order_usd=1.0,
        alpha_decay_report=str(tmp_path / "alpha_decay.json"),
        max_drift_buffer_price=0.05,
        enable_drift_buffer=False,
        enable_maker_fallback=False,
        execute_live=False,
        explicit_live_operator_go=False,
        live_orders_allowed=False,
        inventory_best_ask_timeout_s=0.1,
        e5_live_actuator=True,
        e5_live_intents_state=str(tmp_path / "e5_live_intents.json"),
        e5_live_actuator_state=str(tmp_path / "e5_live_actuator.json"),
        e5_5share_regrade_state=str(tmp_path / "e5_5share_regrade.json"),
        cross_exchange_live_actuator=False,
        cross_exchange_paper_state=str(tmp_path / "cross_exchange_paper.json"),
        cross_exchange_live_actuator_state=str(tmp_path / "cross_exchange_actuator.json"),
        cross_exchange_deadman_state=str(tmp_path / "deadman.json"),
        cross_exchange_live_actuator_ttl_s=3600.0,
        per_window_fill_cap=1,
    )
    atomic_write_json(
        args.e5_5share_regrade_state,
        {
            "kind": "e5_maker_first_5share_regrade",
            "source": {"cohort_sha256": "unit"},
            "contract": {"size_shares": 5.0, "sizing_policy_id": "fixed_shares_5"},
            "summary": {
                "resolved_distinct_executions": 200,
                "resolved_post_fee_pnl_usd": 10.0,
                "resolved_post_fee_roi_pct": 5.0,
                "terminal_maker_fill_rate_pct": 92.0,
                "fallback_violations": 0,
                "copyintent_parity_violations": 0,
                "max_notional_usd": 2.45,
            },
            "gate": {
                "pass": True,
                "decision": "PASS_AUTO_PROMOTE_FIXED_SHARES_5",
                "resolved_executions_required": 150,
                "terminal_maker_fill_rate_required_pct": 90.0,
            },
        },
    )
    return args


def _cross_exchange_signal(window_start: int, *, price: float = 0.45, depth_usd: float = 1.0, edge: float = 0.10):
    return {
        "signal_id": f"xep_unit_{window_start}",
        "condition_id": f"condition_{window_start}",
        "market_slug": f"btc-updown-5m-{window_start}",
        "outcome": "Down",
        "token_id": f"token_{window_start}",
        "executable_price": price,
        "best_ask": price,
        "net_edge_per_share": edge,
        "signal_ts": window_start + 30,
        "observed_ts": window_start + 35,
        "window_start_s": window_start,
        "blockers": [],
        "book": {
            "status": "OK",
            "fillable_usd": depth_usd,
            "best_ask": price,
            "book_hash": "cross-exchange-book-unit",
        },
    }


def _write_cross_exchange_gate_state(args, signal, *, checksum=guard._CROSS_EXCHANGE_MODEL_CHECKSUM):
    intent = guard._cross_exchange_probability_signal_to_intent(signal)
    atomic_write_json(
        args.cross_exchange_paper_state,
        {
            "paper_only": True,
            "orders_submitted": 0,
            "frozen_model": {
                "checksum": checksum,
                "status": "IMMUTABLE_CHECKSUM_VERIFIED",
            },
            "walk_forward": {
                "train": {"positive": True, "post_fee_pnl_usd": 36.0, "resolved_signals": 163},
                "chronological_holdout": {
                    "positive": True,
                    "post_fee_pnl_usd": 16.0,
                    "resolved_signals": 70,
                },
            },
            "prospective_executable_book": {
                "positive": True,
                "post_fee_pnl_usd": 7.0,
                "resolved_signals": 23,
                "distinct_windows": 23,
            },
            "promotion_gate": {
                "pass": False,
                "checks": {"resolved_signals_gte_200": False},
            },
            "current_terminal": {
                "terminal_status": "SIGNAL",
                "model_checksum": checksum,
                "window_start_s": signal["window_start_s"],
                "signal": signal,
                "intent": intent.asdict(),
                "blockers": [],
            },
        },
    )


def _write_cross_exchange_deadman(args):
    atomic_write_json(
        args.cross_exchange_deadman_state,
        {
            "status": "INCIDENT_ORDER_FLOW_DEAD",
            "can_trade": True,
            "raw_accepted_order_deadman": {"accepted_order_idle_s": 4000.0},
            "policy_choke": {
                "selected_wallet": guard._CROSS_EXCHANGE_SILENT_WALLET,
                "selected_fresh_source_rows": 0,
                "rung_a_seat_read": {"target_wallet": None},
            },
            "policy_choke_fire_drill": {
                "rung_c_full_pool_liveness_drill": {
                    "rung_c_candidate_count": 149,
                    "fresh_own_source_positive_rows": 0,
                }
            },
        },
    )


def _e5_live_feed(intent: CopyIntent) -> dict:
    metadata = dict(intent.metadata or {})
    metadata["e5_maker_first_btc5m_v1"] = {
        "book_evidence_mode": "enforced_no_fallback",
        "enforced_no_fallback_book": True,
        "quote_price": intent.limit_price,
        "token_id": intent.token_id,
        "side": intent.side,
        "outcome": intent.outcome,
        "window_start_s": float(intent.market_slug.rsplit("-", 1)[-1]),
        "window_end_s": float(intent.market_slug.rsplit("-", 1)[-1]) + 300.0,
        "top_of_book": {
            "status": "OK",
            "book_hash": "book-hash-unit",
            "route_report": {"status": "PASS", "route_class": "DIRECT_PASS"},
        },
    }
    exact = replace(
        intent,
        metadata=metadata,
        wallet_usdc_size=round(5 * intent.limit_price, 6),
        copy_size_usd=round(5 * intent.limit_price, 6),
        shares=5.0,
        sizing_policy_id="fixed_shares_5",
    )
    return {
        "updated_at": "2026-07-24T02:00:00+00:00",
        "current_intents": [exact.asdict()],
        "promotion_gate": {
            "auto_promote_to_live_guard_on_pass": True,
            "promotion_150_prospective_no_fallback_positive": "PASS_AUTO_PROMOTE",
            "gate_lane": "e5_maker_first_btc5m_v1",
            "gate_source_file": "data/research/maker_first_btc5m_book_aware_state.json",
            "copyintent_parity_violations": 0,
            "monotonicity_tripwire": {"status": "PASS"},
            "prospective_no_fallback_resolved_fills_required": 150,
            "maker_fill_rate_required_pct": 90.0,
        },
        "prospective_no_fallback_summary": {
            "resolved_paper_fills": 200,
            "resolved_paper_pnl_usd": 10.0,
            "terminal_maker_fill_rate_pct": 92.0,
        },
    }


def test_e5_live_route_requires_exact_gate_book_parity_and_freshness(tmp_path):
    window_start = int(time.time() // 300) * 300
    intent = _intent(
        source_tag="E5_MAKER_FIRST",
        lane="e5_maker_first_btc5m_v1",
        window_start=window_start,
        price=0.45,
    )
    now = time.time()
    args = _args(tmp_path)
    intent = replace(
        intent,
        wallet_usdc_size=round(5 * intent.limit_price, 6),
        copy_size_usd=round(5 * intent.limit_price, 6),
        shares=5.0,
        sizing_policy_id="fixed_shares_5",
    )
    feed = _e5_live_feed(intent)
    atomic_write_json(
        args.e5_5share_regrade_state,
        {
            "kind": "e5_maker_first_5share_regrade",
            "source": {"cohort_sha256": "unit"},
            "contract": {"size_shares": 5.0, "sizing_policy_id": "fixed_shares_5"},
            "summary": {
                "resolved_distinct_executions": 200,
                "resolved_post_fee_pnl_usd": 10.0,
                "resolved_post_fee_roi_pct": 5.0,
                "terminal_maker_fill_rate_pct": 92.0,
                "fallback_violations": 0,
                "copyintent_parity_violations": 0,
                "max_notional_usd": 2.45,
            },
            "gate": {
                "pass": True,
                "decision": "PASS_AUTO_PROMOTE_FIXED_SHARES_5",
                "resolved_executions_required": 150,
                "terminal_maker_fill_rate_required_pct": 90.0,
            },
        },
    )

    candidates, selection = guard._e5_live_route_candidates(args, feed, now_ts=now)

    assert [row.intent_id for row in candidates] == [intent.intent_id]
    assert selection["gate_pass"] is True
    assert selection["selected_candidates"] == 1

    cases = {}
    gate_fail = json.loads(json.dumps(feed))
    failed_regrade = load_json(args.e5_5share_regrade_state, default={})
    failed_regrade["gate"]["pass"] = False
    atomic_write_json(args.e5_5share_regrade_state, failed_regrade)
    cases["gate_pass"] = gate_fail
    missing_hash = json.loads(json.dumps(feed))
    missing_hash["current_intents"][0]["metadata"]["e5_maker_first_btc5m_v1"]["top_of_book"]["book_hash"] = ""
    cases["book_hash"] = missing_hash
    fallback = json.loads(json.dumps(feed))
    fallback["current_intents"][0]["metadata"]["e5_maker_first_btc5m_v1"]["top_of_book"]["route_report"][
        "fallback_source"
    ] = "direct_clob_after_primary_failure"
    cases["route_pass"] = fallback
    stale = json.loads(json.dumps(feed))
    stale["current_intents"][0]["event_ts"] = now - 31.0
    stale["current_intents"][0]["observed_ts"] = now - 31.0
    cases["event_fresh"] = stale
    parity_fail = json.loads(json.dumps(feed))
    parity_fail["current_intents"][0]["shares"] = 4.0
    cases["fixed_shares_5"] = parity_fail

    for expected_reason, candidate_feed in cases.items():
        if expected_reason != "gate_pass":
            failed_regrade["gate"]["pass"] = True
            atomic_write_json(args.e5_5share_regrade_state, failed_regrade)
        refused, refusal = guard._e5_live_route_candidates(args, candidate_feed, now_ts=now)
        assert refused == []
        assert refusal["refusal_counts"][expected_reason] == 1


def test_e5_live_actuator_runs_only_inside_guard_and_persists_attribution(monkeypatch, tmp_path):
    now = time.time()
    window_start = int(now // 300) * 300
    intent = _intent(
        source_tag="E5_MAKER_FIRST",
        lane="e5_maker_first_btc5m_v1",
        window_start=window_start,
        price=0.45,
    )
    args = _args(tmp_path)
    args.execute_live = True
    args.explicit_live_operator_go = True
    args.live_orders_allowed = True
    atomic_write_json(args.e5_live_intents_state, _e5_live_feed(intent))
    atomic_write_json(args.live_ledger_state, {"kind": "wallet_copy_live_execution_state", "orders": []})

    monkeypatch.setattr(
        guard,
        "_parity_capsules",
        lambda intents, **kwargs: (
            [{"status": "PASS", "paper_intent_id": intents[0].intent_id, "live_intent_id": "live-unit"}],
            {intents[0].condition_id: [intents[0].token_id]},
            [],
        ),
    )

    async def fake_execute(_args, intents, *, token_map):
        assert token_map == {intents[0].condition_id: [intents[0].token_id]}
        return {
            "results": [
                {
                    "status": "submitted",
                    "post_status": "live",
                    "order_id": "e5-unit-order",
                    "order_type": "GTC",
                    "execution_lane": "e5_maker_first_btc5m_v1",
                    "execution_role": "maker",
                }
            ]
        }

    monkeypatch.setattr(guard, "_execute_e5_live_route_async", fake_execute)

    payload = guard._run_e5_live_actuator(args, generated_at="2026-07-24T02:05:00Z")

    assert payload["status"] == "LIVE_SUBMITTED"
    assert payload["single_submitter"] == "scripts/run_wallet_copy_live_guard.py"
    assert payload["lane"] == "e5_maker_first_btc5m_v1"
    assert payload["order_type"] == "GTC"
    assert payload["post_only_strict"] is True
    assert payload["direct_fallback_allowed"] is False
    assert payload["orders_submitted"] == payload["orders_accepted"] == 1
    assert load_json(args.e5_live_actuator_state, default={})["book_hashes"] == ["book-hash-unit"]


def test_e5_live_actuator_refuses_parity_mismatch_before_execution(monkeypatch, tmp_path):
    now = time.time()
    window_start = int(now // 300) * 300
    intent = _intent(
        source_tag="E5_MAKER_FIRST",
        lane="e5_maker_first_btc5m_v1",
        window_start=window_start,
        price=0.45,
    )
    args = _args(tmp_path)
    args.execute_live = True
    args.explicit_live_operator_go = True
    args.live_orders_allowed = True
    atomic_write_json(args.e5_live_intents_state, _e5_live_feed(intent))
    atomic_write_json(args.live_ledger_state, {"kind": "wallet_copy_live_execution_state", "orders": []})
    monkeypatch.setattr(
        guard,
        "_parity_capsules",
        lambda intents, **kwargs: ([{"status": "FAIL"}], {}, ["unit parity mismatch"]),
    )
    monkeypatch.setattr(
        guard,
        "_execute_e5_live_route_async",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not execute")),
    )

    payload = guard._run_e5_live_actuator(args, generated_at="2026-07-24T02:05:00Z")

    assert payload["status"] == "PARITY_BLOCKED"
    assert payload["orders_submitted"] == 0
    assert payload["parity_blockers"] == ["unit parity mismatch"]


def test_e5_disabled_actuator_force_cancels_before_persisting_demotion(monkeypatch, tmp_path):
    args = _args(tmp_path)
    args.e5_live_actuator = False
    calls = []

    async def fake_cancel(_args, *, force_all_on_demotion=False):
        calls.append(force_all_on_demotion)
        return {"status": "PASS", "due_orders": 2, "canceled_orders": 2}

    monkeypatch.setattr(guard, "_cancel_due_e5_maker_orders_async", fake_cancel)

    payload = guard._run_e5_live_actuator(args, generated_at="2026-07-24T03:42:00Z")

    assert calls == [True]
    assert payload["status"] == "DISABLED"
    assert payload["orders_accepted"] == 0
    assert payload["demotion_force_cancel"]["canceled_orders"] == 2


def test_e5_disabled_actuator_is_noop_after_demotion_cancel(monkeypatch, tmp_path):
    args = _args(tmp_path)
    args.e5_live_actuator = False
    atomic_write_json(
        args.e5_live_actuator_state,
        {
            "status": "DISABLED",
            "demotion_force_cancel": {"status": "PASS", "canceled_orders": 0},
        },
    )
    monkeypatch.setattr(
        guard,
        "_cancel_due_e5_maker_orders_async",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not rescan")),
    )

    payload = guard._run_e5_live_actuator(args, generated_at="2026-08-03T09:35:00Z")

    assert payload["status"] == "DISABLED"
    assert payload["disabled_hot_path_noop"] is True
    assert payload["orders_submitted"] == 0


def test_cross_exchange_live_actuator_submits_only_exact_parity_signal(monkeypatch, tmp_path):
    window_start = 1784950800
    signal = _cross_exchange_signal(window_start)
    args = _args(tmp_path)
    args.cross_exchange_live_actuator = True
    args.execute_live = True
    args.explicit_live_operator_go = True
    args.live_orders_allowed = True
    _write_cross_exchange_gate_state(args, signal)
    _write_cross_exchange_deadman(args)
    atomic_write_json(args.live_ledger_state, {"orders": []})
    monkeypatch.setattr(
        guard,
        "_parity_capsules",
        lambda intents, **kwargs: (
            [
                {
                    "status": "PASS",
                    "paper_intent_id": intents[0].intent_id,
                    "live_intent_id": intents[0].intent_id,
                    "parity_digest": "cip_cross_unit",
                    "blockers": [],
                    "mismatched_fields": [],
                    "decision_wallet_copy_matches_live_intent": True,
                }
            ],
            {intents[0].condition_id: [f"yes_{window_start}", intents[0].token_id]},
            [],
        ),
    )

    async def fake_execute(_args, intents, *, token_map, **identity):
        assert intents[0].mode == "paper"
        assert intents[0].live_orders_allowed is False
        assert intents[0].copy_size_usd == 1.0
        assert token_map[intents[0].condition_id][-1] == intents[0].token_id
        assert identity["source_tag"] == guard._CROSS_EXCHANGE_SOURCE
        assert identity["lane"] == guard._CROSS_EXCHANGE_LANE
        assert identity["execution_mode"] == "taker"
        atomic_write_json(
            _args.live_ledger_state,
            {
                "orders": [
                    {
                        "order_id": "cross-unit-order",
                        "source_wallet": guard._CROSS_EXCHANGE_SOURCE.lower(),
                        "market_slug": intents[0].market_slug,
                        "status": "FILLED",
                        "final_status": "FILLED",
                        "response_filled_size_usd": 1.0,
                        "submitted_at": "2026-07-25T03:40:41Z",
                        "trade_decision": {
                            "strategy_family": guard._CROSS_EXCHANGE_LANE,
                            "wallet_copy": intents[0].asdict(),
                        },
                    }
                ]
            },
        )
        return {
            "results": [
                {
                    "status": "submitted",
                    "post_status": "live",
                    "order_id": "cross-unit-order",
                    "accepted_at": "2026-07-25T03:40:41Z",
                }
            ]
        }

    monkeypatch.setattr(guard, "_execute_cross_exchange_live_route_async", fake_execute)
    payload = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T03:40:40Z",
    )

    assert payload["status"] == "LIVE_SUBMITTED"
    assert payload["orders_accepted"] == 1
    assert payload["orders_submitted"] == 1
    assert payload["orders_filled"] == 1
    assert payload["method_pnl"]["unresolved_fills"] == 1
    assert payload["last_order_id"] == "cross-unit-order"
    assert payload["paper_intent_hash"] == payload["live_intent_hash"]
    assert payload["parity_digest"] == "cip_cross_unit"
    assert payload["activation_non_refreshing"] is True
    assert payload["single_submitter"] == "scripts/run_wallet_copy_live_guard.py"
    assert payload["emergency_probe_does_not_waive_permanent_gate"] is True


def test_promoted_passive_selector_reaches_strict_gtc_actuator_without_synthetic_depth(
    monkeypatch, tmp_path
):
    window_start = 1784950800
    signal = _cross_exchange_signal(window_start, depth_usd=0.0)
    signal["passive_quote_evidence"] = {
        "passive_fill_model_checksum": "passive-model",
        "queue_ahead_shares_at_quote": 2.0,
        "book_sequence": 1784950830000,
    }
    args = _args(tmp_path)
    args.cross_exchange_live_actuator = True
    args.execute_live = True
    args.explicit_live_operator_go = True
    args.live_orders_allowed = True
    args.cross_exchange_promoted_cell_state = str(tmp_path / "selector.json")
    passive_state = tmp_path / "passive-cell.json"
    selected_checks = {
        "paper_only": True,
        "preregistration_checksum_exact": True,
        "model_checksum_exact": True,
        "distinct_generation_checksum": True,
        "model_checksum_not_forbidden": True,
        "resolved_gte_10": True,
        "aggregate_post_fee_positive": True,
        "first_half_positive": True,
        "second_half_positive": True,
        "zero_unresolved_disagreement": True,
        "zero_duplicate_disagreement": True,
        "zero_sign_disagreement": True,
        "zero_parity_disagreement": True,
        "zero_lookahead_violations": True,
        "actual_depth_verified": True,
        "passive_fill_model_verified": True,
        "not_previously_terminal_parked": True,
    }
    selected = {
        "cell_id": "passive-cell",
        "gate_pass": True,
        "activation_id": "promoted-passive-unit",
        "record_checksum": "record",
        "evidence_snapshot_checksum": "evidence",
        "preregistration_checksum": "prereg",
        "model_checksum": "new-model",
        "signal_offset_s": 30,
        "execution_mode": "passive",
        "state_path": str(passive_state),
        "evidence_snapshot": {"checks": selected_checks},
    }
    atomic_write_json(
        args.cross_exchange_promoted_cell_state,
        {"status": "PROMOTED_CELL_READY", "selected": selected, "cells": [selected]},
    )
    atomic_write_json(
        passive_state,
        {
            "paper_only": True,
            "live_orders_allowed": False,
            "frozen_model": {"checksum": "new-model"},
            "current_terminal": {
                "terminal_status": "SIGNAL",
                "model_checksum": "new-model",
                "window_start_s": window_start,
                "signal": signal,
                "blockers": [],
            },
        },
    )
    _write_cross_exchange_deadman(args)
    atomic_write_json(args.live_ledger_state, {"orders": []})
    monkeypatch.setattr(
        guard,
        "_parity_capsules",
        lambda intents, **kwargs: (
            [
                {
                    "status": "PASS",
                    "paper_intent_id": intents[0].intent_id,
                    "live_intent_id": intents[0].intent_id,
                    "parity_digest": "cip_passive_unit",
                    "blockers": [],
                    "mismatched_fields": [],
                    "decision_wallet_copy_matches_live_intent": True,
                }
            ],
            {intents[0].condition_id: ["yes-unit", intents[0].token_id]},
            [],
        ),
    )
    seen = {}

    async def fake_execute(_args, intents, *, token_map, **identity):
        seen["intent"] = intents[0]
        seen["identity"] = identity
        assert intents[0].metadata["precision_requires_passive_source"][
            "execution_path"
        ] == "direct_post_only_gtc_at_source"
        return {
            "results": [
                {
                    "status": "submitted",
                    "post_status": "live",
                    "order_id": "passive-unit-order",
                    "accepted_at": "2026-07-25T03:40:41Z",
                }
            ]
        }

    monkeypatch.setattr(guard, "_execute_cross_exchange_live_route_async", fake_execute)
    payload = guard._run_cross_exchange_live_actuator(
        args, generated_at="2026-07-25T03:40:40Z"
    )

    assert payload["status"] == "LIVE_SUBMITTED"
    assert payload["signal_checks"]["executable_depth"] is True
    assert payload["signal_checks"]["passive_quote_execution_proof"] is True
    assert seen["identity"]["execution_mode"] == "passive"
    assert seen["identity"]["source_tag"] == "BTC5M_PROMOTED_CELL:PASSIVE-CELL"


def test_cross_exchange_execution_snapshot_normalizes_signal_engine_identity(monkeypatch, tmp_path):
    args = _args(tmp_path)
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    intent = guard._cross_exchange_probability_signal_to_intent(
        _cross_exchange_signal(1784950800)
    )

    async def fake_executor(**kwargs):
        return object()

    class FakeAdapter:
        def __init__(self, *, gate, **kwargs):
            assert (
                gate.admission_snapshot.candidate_source_wallet
                == guard._CROSS_EXCHANGE_SOURCE.lower()
            )
            self.gate = gate

        async def execute_async(self, intents, *, clob_token_ids_by_condition):
            self.gate.assert_live_allowed(intents)
            return {"results": []}

    monkeypatch.setattr(guard, "_live_trade_executor", fake_executor)
    monkeypatch.setattr(guard, "CopyExecutionAdapter", FakeAdapter)

    result = asyncio.run(
        guard._execute_cross_exchange_live_route_async(
            args,
            [intent],
            token_map={intent.condition_id: ["yes-unit", intent.token_id]},
        )
    )

    assert result == {"results": []}


def test_promoted_cell_execution_binds_snapshot_checksums_and_routes_taker_vs_passive(monkeypatch, tmp_path):
    from src.wallet_copy.execution import intent_to_trade_executor_decision

    args = _args(tmp_path)
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    base = guard._cross_exchange_probability_signal_to_intent(
        _cross_exchange_signal(1784950800)
    ).asdict()
    source = "BTC5M_PROMOTED_CELL:CELL-A"
    lane = "btc5m_cross_exchange_promoted_cell:cell-a"
    seen = []

    async def fake_executor(**kwargs):
        return object()

    class FakeAdapter:
        def __init__(self, *, gate, **kwargs):
            assert gate.admission_snapshot.candidate_source_wallet == source.lower()

        async def execute_async(self, intents, *, clob_token_ids_by_condition):
            seen.extend(intents)
            return {"results": []}

    monkeypatch.setattr(guard, "_live_trade_executor", fake_executor)
    monkeypatch.setattr(guard, "CopyExecutionAdapter", FakeAdapter)
    for mode in ("taker", "passive"):
        copy_size_usd = float(base["copy_size_usd"])
        payload = {
            **base,
            "source_wallet": source,
            "wallet_name": lane,
            "strategy_family": lane,
            "limit_price": 0.30,
            "shares": round(copy_size_usd / 0.30, 6),
        }
        metadata = dict(payload["metadata"])
        metadata["promoted_cell"] = {
            "record_checksum": "record",
            "evidence_snapshot_checksum": "evidence",
            "execution_mode": mode,
        }
        if mode == "passive":
            metadata["precision_requires_passive_source"] = {
                "status": "promoted_cell_post_only",
                "execution_path": "direct_post_only_gtc_at_source",
                "passive_price": payload["limit_price"],
                "original_source_price": payload["limit_price"],
                "buffered_limit_price": payload["limit_price"],
                "max_copy_price": 0.50,
                "best_ask": payload["limit_price"] + 0.01,
            }
        payload["metadata"] = metadata
        intent = guard.CopyIntent.from_dict(payload)
        tokens = (
            [intent.token_id, "no-unit"]
            if intent.outcome == "Up"
            else ["yes-unit", intent.token_id]
        )
        asyncio.run(
            guard._execute_cross_exchange_live_route_async(
                args,
                [intent],
                token_map={intent.condition_id: tokens},
                source_tag=source,
                lane=lane,
                execution_mode=mode,
                record_checksum="record",
                evidence_snapshot_checksum="evidence",
            )
        )
        decision = intent_to_trade_executor_decision(
            intent,
            clob_token_ids=tokens,
        )
        assert decision["order_type"] == (
            "FAK" if mode == "taker" else "POLICY_TERMINAL"
        )
        assert decision["post_only_strict"] is False
    assert len(seen) == 2


def test_paired_promoted_bundle_maps_two_parity_intents_and_cancels_asymmetric_survivor(monkeypatch, tmp_path):
    args = _args(tmp_path)
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    base = guard._cross_exchange_probability_signal_to_intent(_cross_exchange_signal(1784950800)).asdict()
    pair_id = "pair-unit"
    rows = []
    for outcome, token in (("Up", "up-unit"), ("Down", "down-unit")):
        row = {**base, "outcome": outcome, "side": "YES" if outcome == "Up" else "NO", "token_id": token, "copy_size_usd": 0.5, "wallet_usdc_size": 0.5, "shares": 1.25}
        rows.append(row)
    terminal = {
        "terminal_id": pair_id,
        "paired_intents": rows,
        "signal": {"paired_legs": [{"best_ask": 0.41}, {"best_ask": 0.41}]},
    }
    intents = guard._promoted_paired_bundle_intents(
        terminal,
        cell_id="pair-cell",
        source_tag="BTC5M_PROMOTED_CELL:PAIR-CELL",
        lane="btc5m_cross_exchange_promoted_cell:pair-cell",
        activation_id="pair-activation",
        record_checksum="record",
        evidence_snapshot_checksum="evidence",
    )
    assert {row.outcome for row in intents} == {"Up", "Down"}
    assert sum(row.copy_size_usd for row in intents) == 1.0
    assert {row.metadata["paired_bundle"]["pair_id"] for row in intents} == {pair_id}

    class FakeExecutor:
        def __init__(self): self.cancelled = []
        async def cancel_order(self, order_id): self.cancelled.append(order_id); return True

    executor = FakeExecutor()
    seen = {}

    async def fake_executor(**kwargs): return executor

    class FakeAdapter:
        def __init__(self, *, per_window_fill_cap, **kwargs): assert per_window_fill_cap == 2
        async def execute_async(self, live_intents, *, clob_token_ids_by_condition):
            seen["intents"] = live_intents
            return {"results": [{"status": "submitted", "order_id": "survivor"}, {"status": "error", "order_id": ""}]}

    monkeypatch.setattr(guard, "_live_trade_executor", fake_executor)
    monkeypatch.setattr(guard, "CopyExecutionAdapter", FakeAdapter)
    result = asyncio.run(guard._execute_cross_exchange_live_route_async(
        args,
        intents,
        token_map={intents[0].condition_id: ["up-unit", "down-unit"]},
        source_tag="BTC5M_PROMOTED_CELL:PAIR-CELL",
        lane="btc5m_cross_exchange_promoted_cell:pair-cell",
        execution_mode="paired_passive",
        record_checksum="record",
        evidence_snapshot_checksum="evidence",
    ))
    assert result["paired_bundle_execution"]["asymmetric_rejection"] is True
    assert result["paired_bundle_execution"]["survivor_cancelled"] is True
    assert executor.cancelled == ["survivor"]


def test_split_sell_bundle_confirms_ctf_split_before_two_sell_submissions(monkeypatch, tmp_path):
    from src.wallet_copy.execution import intent_to_trade_executor_decision

    args = _args(tmp_path)
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    condition = "0x" + "11" * 32
    base = guard._cross_exchange_probability_signal_to_intent(_cross_exchange_signal(1784950800)).asdict()
    rows = []
    for outcome, token, price in (("Up", "up-unit", 0.58), ("Down", "down-unit", 0.48)):
        rows.append({
            **base, "condition_id": condition, "outcome": outcome,
            "side": "YES" if outcome == "Up" else "NO", "token_id": token,
            "limit_price": price, "action": "SELL", "copy_size_usd": 0.5,
            "wallet_usdc_size": 0.5, "shares": 1.0,
        })
    terminal = {"terminal_id": "split-pair", "paired_intents": rows, "signal": {"paired_legs": [{}, {}]}}
    intents = guard._promoted_paired_bundle_intents(
        terminal, cell_id="split-cell", source_tag="BTC5M_PROMOTED_CELL:SPLIT-CELL",
        lane="btc5m_cross_exchange_promoted_cell:split-cell", activation_id="a",
        record_checksum="record", evidence_snapshot_checksum="evidence",
        execution_mode="paired_split_sell",
    )
    assert {row.action for row in intents} == {"SELL"}
    assert {row.shares for row in intents} == {1.0}
    assert all(row.metadata["complete_set_split_sell"]["ctf_split_confirmed_before_submit"] for row in intents)
    decision = intent_to_trade_executor_decision(intents[0], clob_token_ids=["up-unit", "down-unit"])
    assert decision["action"] == "sell"
    assert decision["order_type"] == "FAK"
    assert decision["allow_sell_below_min_shares"] is True

    calls = []
    monkeypatch.setattr(guard, "_execute_complete_set_split", lambda cid: calls.append(cid) or {"status": "CONFIRMED"})

    class FakeExecutor:
        pass

    async def fake_executor(**kwargs):
        return FakeExecutor()

    class FakeAdapter:
        def __init__(self, **kwargs):
            pass
        async def execute_async(self, live_intents, *, clob_token_ids_by_condition):
            assert calls == [condition]
            return {"results": [{"status": "submitted", "order_id": "sold"}, {"status": "error", "order_id": ""}]}

    monkeypatch.setattr(guard, "_live_trade_executor", fake_executor)
    monkeypatch.setattr(guard, "CopyExecutionAdapter", FakeAdapter)
    result = asyncio.run(guard._execute_cross_exchange_live_route_async(
        args, intents, token_map={condition: ["up-unit", "down-unit"]},
        source_tag="BTC5M_PROMOTED_CELL:SPLIT-CELL",
        lane="btc5m_cross_exchange_promoted_cell:split-cell",
        execution_mode="paired_split_sell", record_checksum="record",
        evidence_snapshot_checksum="evidence",
    ))
    assert result["complete_set_split"]["status"] == "CONFIRMED"
    assert result["paired_bundle_execution"]["orphan_inventory_action"] == "CARRY_UNSOLD_LEG_TO_CANONICAL_RESOLUTION"


def test_cross_exchange_live_actuator_refuses_checksum_regression(monkeypatch, tmp_path):
    window_start = 1784950800
    args = _args(tmp_path)
    args.cross_exchange_live_actuator = True
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    _write_cross_exchange_gate_state(args, _cross_exchange_signal(window_start), checksum="wrong")
    _write_cross_exchange_deadman(args)
    monkeypatch.setattr(
        guard,
        "_execute_cross_exchange_live_route_async",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not execute")),
    )

    payload = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T03:40:40Z",
    )

    assert payload["status"] == "EVIDENCE_GATE_BLOCKED"
    assert payload["evidence"]["checks"]["frozen_checksum_exact"] is False
    assert payload["orders_submitted"] == 0


def test_cross_exchange_live_actuator_refuses_stale_bounds_depth_and_edge(monkeypatch, tmp_path):
    args = _args(tmp_path)
    args.cross_exchange_live_actuator = True
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    signal = _cross_exchange_signal(1784950500, price=0.53, depth_usd=0.0, edge=-0.01)
    _write_cross_exchange_gate_state(args, signal)
    _write_cross_exchange_deadman(args)
    monkeypatch.setattr(
        guard,
        "_execute_cross_exchange_live_route_async",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not execute")),
    )

    payload = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T03:40:40Z",
    )

    assert payload["status"] == "PROTECTED_SKIP"
    assert payload["signal_checks"]["current_btc5m_window"] is False
    assert payload["signal_checks"]["executable_depth"] is False
    assert payload["signal_checks"]["hard_price_bounds"] is False
    assert payload["signal_checks"]["positive_net_edge"] is False
    assert payload["orders_submitted"] == 0


def test_cross_exchange_live_actuator_dedupes_accepted_window(monkeypatch, tmp_path):
    window_start = 1784950800
    signal = _cross_exchange_signal(window_start)
    args = _args(tmp_path)
    args.cross_exchange_live_actuator = True
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    _write_cross_exchange_gate_state(args, signal)
    _write_cross_exchange_deadman(args)
    atomic_write_json(
        args.live_ledger_state,
        {
            "orders": [
                {
                    "market_slug": signal["market_slug"],
                    "source_wallet": guard._CROSS_EXCHANGE_SOURCE,
                    "final_status": "FILLED",
                }
            ]
        },
    )
    monkeypatch.setattr(
        guard,
        "_execute_cross_exchange_live_route_async",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not execute")),
    )

    payload = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T03:40:40Z",
    )

    assert payload["status"] == "NO_NEW_WINDOWS"
    assert payload["window_dedupe"]["skipped_market_slugs"] == [signal["market_slug"]]
    assert payload["orders_accepted"] == 1
    assert payload["orders_filled"] == 1


def test_cross_exchange_live_actuator_ttl_is_non_refreshing(monkeypatch, tmp_path):
    window_start = 1784950800
    signal = _cross_exchange_signal(window_start)
    args = _args(tmp_path)
    args.cross_exchange_live_actuator = True
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    _write_cross_exchange_gate_state(args, signal)
    _write_cross_exchange_deadman(args)
    state = load_json(args.cross_exchange_paper_state, default={})
    state["current_terminal"]["terminal_status"] = "PROTECTED_SKIP"
    state["current_terminal"]["blockers"] = ["insufficient_executable_depth"]
    atomic_write_json(args.cross_exchange_paper_state, state)

    first = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T03:40:40Z",
    )
    expired = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T04:40:41Z",
    )

    assert first["status"] == "ARMED_WAITING_QUALIFYING_SIGNAL"
    assert expired["status"] == "EXPIRED_ZERO_CONVERSION"
    assert expired["activation_started_at"] == first["activation_started_at"]
    assert expired["activation_expires_at"] == first["activation_expires_at"]


def test_cross_exchange_live_actuator_mechanically_demotes_negative_method_pnl(monkeypatch, tmp_path):
    window_start = 1784950800
    args = _args(tmp_path)
    args.cross_exchange_live_actuator = True
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    _write_cross_exchange_gate_state(args, _cross_exchange_signal(window_start))
    _write_cross_exchange_deadman(args)
    atomic_write_json(
        args.live_ledger_state,
        {
            "orders": [
                {
                    "source_wallet": guard._CROSS_EXCHANGE_SOURCE,
                    "final_status": "FILLED",
                    "pnl_usd": -1.0,
                    "order_id": "cross-loss-unit",
                }
            ]
        },
    )
    monkeypatch.setattr(
        guard,
        "_execute_cross_exchange_live_route_async",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not execute")),
    )

    payload = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T03:40:40Z",
    )

    assert payload["status"] == "DEMOTED_NEGATIVE_ROLLING_PNL"
    assert payload["method_pnl"]["rolling_realized_pnl_usd"] == -1.0
    assert payload["orders_submitted"] == 1
    assert payload["orders_accepted"] == 1
    assert payload["orders_filled"] == 1


def test_cross_exchange_campaign_truth_survives_clear_deadman_and_resolution(tmp_path):
    args = _args(tmp_path)
    args.cross_exchange_live_actuator = True
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    args.resolutions = str(tmp_path / "resolutions.jsonl")
    (tmp_path / "resolutions.jsonl").write_text("", encoding="utf-8")
    atomic_write_json(
        args.cross_exchange_live_actuator_state,
        {
            "activation_id": guard._CROSS_EXCHANGE_ACTIVATION_ID,
            "activation_started_at": "2026-07-25T03:40:40Z",
            "activation_expires_at": "2026-07-25T04:40:40Z",
            "last_order_id": "cross-ledger-order",
            "paper_intent_hash": "same",
            "live_intent_hash": "same",
        },
    )
    atomic_write_json(
        args.cross_exchange_deadman_state,
        {"status": "OK", "can_trade": True, "accepted_order_idle_s": 20.0},
    )
    order = {
        "order_id": "cross-ledger-order",
        "source_wallet": guard._CROSS_EXCHANGE_SOURCE.lower(),
        "market_slug": "btc-updown-5m-1784950800",
        "status": "FILLED",
        "final_status": "FILLED",
        "response_filled_size_usd": 1.0,
        "submitted_at": "2026-07-25T03:40:41Z",
        "trade_decision": {"strategy_family": guard._CROSS_EXCHANGE_LANE},
    }
    atomic_write_json(args.live_ledger_state, {"orders": [order]})

    unresolved = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T03:41:00Z",
    )
    assert unresolved["status"] == "DEADMAN_GATE_BLOCKED"
    assert (
        unresolved["orders_submitted"],
        unresolved["orders_accepted"],
        unresolved["orders_filled"],
    ) == (1, 1, 1)
    assert unresolved["method_pnl"]["resolved_fills"] == 0
    assert unresolved["method_pnl"]["unresolved_fills"] == 1

    order["resolution"] = {"pnl_usd": 0.75}
    atomic_write_json(args.live_ledger_state, {"orders": [order]})
    resolved = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T03:42:00Z",
    )
    assert resolved["orders_submitted"] == 1
    assert resolved["method_pnl"]["resolved_fills"] == 1
    assert resolved["method_pnl"]["unresolved_fills"] == 0
    assert resolved["method_pnl"]["rolling_realized_pnl_usd"] == 0.75


def test_cross_exchange_negative_resolution_demotes_even_after_deadman_clears(tmp_path):
    args = _args(tmp_path)
    args.cross_exchange_live_actuator = True
    args.execute_live = args.explicit_live_operator_go = args.live_orders_allowed = True
    atomic_write_json(
        args.cross_exchange_deadman_state,
        {"status": "OK", "can_trade": True, "accepted_order_idle_s": 20.0},
    )
    atomic_write_json(
        args.live_ledger_state,
        {
            "orders": [
                {
                    "order_id": "cross-negative-clear-deadman",
                    "source_wallet": guard._CROSS_EXCHANGE_SOURCE.lower(),
                    "status": "FILLED",
                    "final_status": "FILLED",
                    "response_filled_size_usd": 1.0,
                    "resolution": {"pnl_usd": -1.0},
                    "submitted_at": "2026-07-25T03:40:41Z",
                    "trade_decision": {"strategy_family": guard._CROSS_EXCHANGE_LANE},
                }
            ]
        },
    )

    payload = guard._run_cross_exchange_live_actuator(
        args,
        generated_at="2026-07-25T03:42:00Z",
    )
    assert payload["status"] == "DEMOTED_NEGATIVE_ROLLING_PNL"
    assert payload["orders_accepted"] == 1
    assert payload["method_pnl"]["rolling_realized_pnl_usd"] == -1.0


def test_e5_in_process_builder_emits_paper_intent_and_never_submits(monkeypatch, tmp_path):
    now = time.time()
    intent = _intent(
        source_tag="E5_MAKER_FIRST",
        lane="e5_maker_first_btc5m_v1",
        window_start=int(now // 300) * 300,
        price=0.45,
    )
    args = _args(tmp_path)
    args.rtds_jsonl = str(tmp_path / "rtds.jsonl")
    args.rtds_scan_limit = 50_000
    args.rtds_tail_bytes = 5_242_880
    monkeypatch.setattr(guard, "_e5_load_recent_events", lambda *args, **kwargs: (["event"], {"accepted": 1}))
    monkeypatch.setattr(
        guard,
        "_e5_build_quote_signals",
        lambda events, **kwargs: (["signal"], {"signals": 1, "enforced_no_fallback": True}),
    )
    monkeypatch.setattr(guard, "_e5_maker_signal_to_intent", lambda signal: intent)

    rows, diagnostics = guard._e5_rtds_live_intents(args)

    assert rows == [intent.asdict()]
    assert rows[0]["mode"] == "paper"
    assert rows[0]["live_orders_allowed"] is False
    assert diagnostics["status"] == "PASS"
    assert diagnostics["intents"] == 1
    assert diagnostics["orders_submitted"] == 0
    assert diagnostics["single_submitter"] == "scripts/run_wallet_copy_live_guard.py"


def test_e5_window_dedupe_prevents_stacked_live_gtc_orders(tmp_path):
    now = time.time()
    intent = _intent(
        source_tag="E5_MAKER_FIRST",
        lane="e5_maker_first_btc5m_v1",
        window_start=int(now // 300) * 300,
        price=0.45,
    )
    ledger_path = tmp_path / "live_ledger.json"
    atomic_write_json(
        ledger_path,
        {
            "orders": [
                {
                    "market_slug": intent.market_slug,
                    "final_status": "SUBMITTED",
                    "trade_decision": {"execution_lane": "e5_maker_first_btc5m_v1"},
                }
            ]
        },
    )

    kept, diagnostics = guard._drop_e5_already_routed_windows(
        [intent],
        live_ledger_state=str(ledger_path),
    )

    assert kept == []
    assert diagnostics["new_window_intents"] == 0
    assert diagnostics["skipped_market_slugs"] == [intent.market_slug]
    assert diagnostics["rule"] == "at_most_one_accepted_e5_gtc_per_btc5m_window"


def test_guard_shadow_route_e5_first_e6_second_zero_live_and_provenance(monkeypatch, tmp_path):
    now = int(time.time())
    window_start = int(now // 300) * 300
    e5_intent = _intent(source_tag="E5_MAKER_FIRST", lane="e5_maker_first_btc5m_v1", window_start=window_start, price=0.45)
    e6_intent = _intent(source_tag="E6_WHALE_SIDE", lane="e6_whale_net_flow_v1", window_start=window_start, price=0.40, size_usd=8.0)
    args = _args(tmp_path)
    atomic_write_json(
        args.e5_shadow_lane_state,
        {
            "current_intents": [e5_intent.asdict()],
            "orders": [
                {
                    "order_id": "po_e5",
                    "intent_id": e5_intent.intent_id,
                    "final_status": "FILLED",
                    "filled_size_usd": 1.0,
                    "source_intent": e5_intent.asdict(),
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "abc123",
                            "best_bid": 0.44,
                            "best_ask": 0.45,
                            "copy_size_usd": 1.0,
                            "fillable_usd": 1.0,
                            "fillable_shares": 2.222222,
                            "fill_ratio": 1.0,
                            "avg_fill_price": 0.45,
                            "instant_fill_status": "PASS",
                            "levels_used": 1,
                            "route_report": {
                                "status": "PASS",
                                "route_class": "DIRECT_PASS",
                                "fallback_source": "direct_clob_after_primary_failure",
                                "primary_error": "503",
                            },
                        }
                    },
                }
            ],
        },
    )
    atomic_write_json(args.e6_shadow_lane_state, {"current_intents": [e6_intent.asdict()]})
    atomic_write_json(
        args.e6_shadow_paper_state,
        {
            "orders": [
                {
                    "order_id": "po_e6",
                    "intent_id": e6_intent.intent_id,
                    "final_status": "FILLED",
                    "filled_size_usd": 8.0,
                    "source_intent": {**e6_intent.asdict(), "fill_estimate": {"source": "source_price_plus_slippage_fallback"}},
                }
            ]
        },
    )
    atomic_write_json(args.live_ledger_state, {"kind": "wallet_copy_live_execution_state", "orders": []})

    def fake_parity(intents, **kwargs):
        if isinstance(kwargs.get("profile_out"), dict):
            kwargs["profile_out"].update({"intents": len(intents)})
        return ([{"status": "PASS", "paper_intent_id": intent.intent_id} for intent in intents], {}, [])

    monkeypatch.setattr(guard, "_parity_capsules", fake_parity)

    payload = guard._run_guard_shadow_lanes(args, generated_at="2026-07-06T06:10:00Z")

    assert payload["route_order"] == ["e5_maker_first_btc5m_v1", "e6_whale_net_flow_v1"]
    assert payload["summary"]["orders_submitted"] == 0
    assert payload["zero_live_assertion"]["status"] == "PASS"
    assert [lane["lane"] for lane in payload["lanes"]] == ["e5_maker_first_btc5m_v1", "e6_whale_net_flow_v1"]
    e5_row = next(row for row in payload["rows"] if row["lane"] == "e5_maker_first_btc5m_v1")
    assert e5_row["would_have_filled"] is True
    assert e5_row["orders_submitted"] == 0
    assert e5_row["live_orders_allowed"] is False
    assert e5_row["pricing_provenance"]["category"] == "direct_fallback"
    assert e5_row["book_aware_fill_test"]["best_ask"] == 0.45
    assert e5_row["book_aware_fill_test"]["fillable_usd"] == 1.0
    assert e5_row["book_aware_fill_test"]["fill_ratio"] == 1.0
    assert e5_row["book_aware_fill_test"]["instant_fill_status"] == "PASS"
    assert e5_row["shadow_max_event_age_s"] == 300.0
    assert e5_row["shadow_live_build_max_observed_age_s"] == 300.0
    e6_row = next(row for row in payload["rows"] if row["lane"] == "e6_whale_net_flow_v1")
    assert e6_row["pricing_provenance"]["category"] == "source_price_plus_slippage_fallback"
    e5_summary = next(lane for lane in payload["lanes"] if lane["lane"] == "e5_maker_first_btc5m_v1")
    assert e5_summary["filters"]["freshness"]["applied_max_event_age_s"] == 300.0
    assert e5_summary["filters"]["freshness"]["live_path_max_event_age_s"] == 30.0
    persisted = load_json(args.shadow_state, default={})
    assert persisted["summary"]["would_have_filled"] == 2


def test_guard_shadow_uses_300s_freshness_without_relaxing_live_path(monkeypatch, tmp_path):
    now = time.time()
    window_start = int(now // 300) * 300
    stale_intent = _intent(
        source_tag="E5_MAKER_FIRST",
        lane="e5_maker_first_btc5m_v1",
        window_start=window_start,
        price=0.45,
    )
    stale_intent = replace(stale_intent, observed_ts=now - 100.0, event_ts=now - 100.0)
    args = _args(tmp_path)
    atomic_write_json(
        args.e5_shadow_lane_state,
        {
            "current_intents": [stale_intent.asdict()],
            "orders": [
                {
                    "order_id": "po_e5_stale_for_live",
                    "intent_id": stale_intent.intent_id,
                    "final_status": "FILLED",
                    "filled_size_usd": 1.0,
                    "source_intent": stale_intent.asdict(),
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "abc123",
                            "best_bid": 0.44,
                            "best_ask": 0.45,
                            "copy_size_usd": 1.0,
                            "fillable_usd": 1.0,
                            "fillable_shares": 2.222222,
                            "fill_ratio": 1.0,
                            "avg_fill_price": 0.45,
                            "instant_fill_status": "PASS",
                            "levels_used": 1,
                            "route_report": {"status": "PASS", "route_class": "DIRECT_PASS"},
                        }
                    },
                }
            ],
        },
    )
    atomic_write_json(args.e6_shadow_lane_state, {"current_intents": []})
    atomic_write_json(args.e6_shadow_paper_state, {"orders": []})
    atomic_write_json(args.live_ledger_state, {"kind": "wallet_copy_live_execution_state", "orders": []})

    assert (
        live_execution._fresh_intents(
            [stale_intent],
            max_event_age_s=30.0,
            max_intents=6,
            live_build_max_observed_age_s=30.0,
        )
        == []
    )

    def fake_parity(intents, **kwargs):
        return ([{"status": "PASS", "paper_intent_id": intent.intent_id} for intent in intents], {}, [])

    monkeypatch.setattr(guard, "_parity_capsules", fake_parity)

    payload = guard._run_guard_shadow_lanes(args, generated_at="2026-07-06T06:30:00Z")

    e5_summary = next(lane for lane in payload["lanes"] if lane["lane"] == "e5_maker_first_btc5m_v1")
    assert e5_summary["shadow_live_build_max_observed_age_s"] == 300.0
    assert e5_summary["fresh_intents"] == 1
    assert e5_summary["orders_submitted"] == 0
    assert e5_summary["filters"]["freshness"]["candidate_intents_observed_age_gt_live_build_max"] == 0
    assert e5_summary["filters"]["freshness"]["filter_reason_counts"] == {"pass": 1}
    assert e5_summary["filters"]["freshness"]["live_path_max_event_age_s"] == 30.0
    assert e5_summary["filters"]["freshness"]["live_path_live_build_max_observed_age_s"] == 30.0
    e5_row = next(row for row in payload["rows"] if row["lane"] == "e5_maker_first_btc5m_v1")
    assert e5_row["shadow_status"] == "SHADOW_READY"
    assert e5_row["shadow_freshness"]["applied_live_build_max_observed_age_s"] == 300.0
    assert e5_row["shadow_freshness"]["filter_reason"] is None
    assert e5_row["shadow_event_age_s_at_cycle"] >= 99.0
    live_command = guard._live_command(args, candidate_id="candidate")
    assert live_command[live_command.index("--max-event-age-s") + 1] == "30.0"
    assert live_command[live_command.index("--live-build-max-observed-age-s") + 1] == "30.0"


def test_guard_shadow_parks_e6_when_floor_trips(monkeypatch, tmp_path):
    now = int(time.time())
    window_start = int(now // 300) * 300
    e5_intent = _intent(source_tag="E5_MAKER_FIRST", lane="e5_maker_first_btc5m_v1", window_start=window_start, price=0.45)
    args = _args(tmp_path)
    atomic_write_json(args.e5_shadow_lane_state, {"current_intents": [e5_intent.asdict()]})
    atomic_write_json(
        args.e6_shadow_lane_state,
        {
            "current_intents": [],
            "promotion_gate": {
                "resolved_paper_fills": 82,
                "resolved_paper_pnl_usd": -32.088449,
                "resolved_paper_roi_pct": -4.891532,
            },
        },
    )
    atomic_write_json(args.e6_shadow_paper_state, {"orders": []})
    atomic_write_json(args.live_ledger_state, {"kind": "wallet_copy_live_execution_state", "orders": []})

    def fake_parity(intents, **kwargs):
        return ([{"status": "PASS", "paper_intent_id": intent.intent_id} for intent in intents], {}, [])

    monkeypatch.setattr(guard, "_parity_capsules", fake_parity)

    payload = guard._run_guard_shadow_lanes(args, generated_at="2026-07-06T08:40:00Z")

    assert payload["route_order"] == ["e5_maker_first_btc5m_v1"]
    assert [lane["lane"] for lane in payload["lanes"]] == ["e5_maker_first_btc5m_v1"]
    assert payload["parked_lanes"] == [
        {
            "flow_stage": "ROTATE/PROMOTE",
            "lane": "e6_whale_net_flow_v1",
            "parked": True,
            "pnl_floor_usd": -30.0,
            "reason": "RULING_AQ_DECIDE_2_E6_FLOOR",
            "resolved_paper_pnl_usd": -32.088449,
            "resolved_paper_roi_pct": -4.891532,
            "roi_floor_pct": -5.0,
            "source_tag": "E6_WHALE_SIDE",
            "state_path": args.e6_shadow_lane_state,
        }
    ]
    assert payload["summary"]["orders_submitted"] == 0
    assert payload["zero_live_assertion"]["status"] == "PASS"


def test_guard_shadow_event_log_skips_no_delta_full_snapshots(monkeypatch, tmp_path):
    now = int(time.time())
    window_start = int(now // 300) * 300
    e5_intent = _intent(source_tag="E5_MAKER_FIRST", lane="e5_maker_first_btc5m_v1", window_start=window_start, price=0.45)
    args = _args(tmp_path)
    atomic_write_json(
        args.e5_shadow_lane_state,
        {
            "current_intents": [e5_intent.asdict()],
            "orders": [
                {
                    "order_id": "po_e5",
                    "intent_id": e5_intent.intent_id,
                    "final_status": "FILLED",
                    "filled_size_usd": 1.0,
                    "source_intent": e5_intent.asdict(),
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "abc123",
                            "best_bid": 0.44,
                            "best_ask": 0.45,
                            "copy_size_usd": 1.0,
                            "fillable_usd": 1.0,
                            "fill_ratio": 1.0,
                            "avg_fill_price": 0.45,
                            "instant_fill_status": "PASS",
                            "levels_used": 1,
                            "route_report": {"status": "PASS", "route_class": "DIRECT_PASS"},
                        }
                    },
                }
            ],
        },
    )
    atomic_write_json(args.e6_shadow_lane_state, {"current_intents": []})
    atomic_write_json(args.e6_shadow_paper_state, {"orders": []})
    atomic_write_json(args.live_ledger_state, {"kind": "wallet_copy_live_execution_state", "orders": []})

    def fake_parity(intents, **kwargs):
        return ([{"status": "PASS", "paper_intent_id": intent.intent_id} for intent in intents], {}, [])

    monkeypatch.setattr(guard, "_parity_capsules", fake_parity)

    first = guard._run_guard_shadow_lanes(args, generated_at="2026-07-06T06:40:00Z")
    first_events = [json.loads(line) for line in open(args.shadow_event_log, encoding="utf-8")]
    assert first["event_emitter"]["snapshot_written"] is True
    assert {event["event"] for event in first_events} >= {
        "wallet_copy_guard_shadow_lanes_snapshot",
        "shadow_intent_first_seen",
        "shadow_book_test",
        "shadow_would_have_filled",
    }

    second = guard._run_guard_shadow_lanes(args, generated_at="2026-07-06T06:40:04Z")
    second_events = [json.loads(line) for line in open(args.shadow_event_log, encoding="utf-8")]
    assert second["event_emitter"]["snapshot_written"] is False
    assert len(second_events) == len(first_events)


def test_guard_shadow_zero_submit_assertion_records_incident():
    payload = {"enabled": True, "summary": {"orders_submitted": 1}}

    result = guard._shadow_zero_submit_assertion(payload)

    assert result["enabled"] is False
    assert result["incident_triggered"] is True
    assert result["status"] == "SHADOW_INCIDENT_NONZERO_SUBMIT"
    assert result["zero_live_assertion"]["orders_submitted"] == 1


def test_cross_exchange_canonical_resolution_patch_covers_win_loss_and_unresolved():
    base = {
        "order_id": "0xmethod",
        "status": "FILLED",
        "final_status": "FILLED",
        "condition_id": "condition",
        "market_slug": "btc-updown-5m-1000",
        "outcome": "Up",
        "response_filled_size_usd": 1.0,
        "response_fill_size_shares": 2.0,
    }
    up_resolution = {
        "condition": {
            "direction": "UP",
            "source": "polymarket_gamma_resolved_outcome",
        }
    }
    win = guard._cross_exchange_resolution_patch(
        base,
        up_resolution,
        generated_at="2026-07-25T05:20:00Z",
    )
    assert win["pnl_usd"] == 1.0
    assert win["resolution"]["win"] is True
    loss = guard._cross_exchange_resolution_patch(
        {**base, "outcome": "Down"},
        up_resolution,
        generated_at="2026-07-25T05:20:00Z",
    )
    assert loss["pnl_usd"] == -1.0
    assert loss["resolution"]["win"] is False
    assert (
        guard._cross_exchange_resolution_patch(
            base,
            {},
            generated_at="2026-07-25T05:20:00Z",
        )
        is None
    )


def test_cross_exchange_campaign_resolution_join_is_idempotent_and_scorecard_signed(
    tmp_path,
):
    ledger = tmp_path / "ledger.json"
    resolutions = tmp_path / "resolutions.jsonl"
    ledger.write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "order_id": "0xmethod",
                        "status": "FILLED",
                        "final_status": "FILLED",
                        "source_wallet": "btc5m_cross_exchange_probability_edge_v1",
                        "condition_id": "condition",
                        "market_slug": "btc-updown-5m-1000",
                        "outcome": "Down",
                        "response_filled_size_usd": 0.999999,
                        "response_fill_size_shares": 2.222221,
                    }
                ]
            }
        )
    )
    resolutions.write_text(
        json.dumps(
            {
                "condition_id": "condition",
                "market_slug": "btc-updown-5m-1000",
                "direction": "UP",
                "source": "polymarket_gamma_resolved_outcome",
            }
        )
        + "\n"
    )
    first = guard._resolve_cross_exchange_campaign_orders(
        str(ledger),
        str(resolutions),
        generated_at="2026-07-25T05:20:00Z",
    )
    first_payload = json.loads(ledger.read_text())
    second = guard._resolve_cross_exchange_campaign_orders(
        str(ledger),
        str(resolutions),
        generated_at="2026-07-25T05:21:00Z",
    )
    second_payload = json.loads(ledger.read_text())

    assert first["changed_orders"] == 1
    assert second["changed_orders"] == 0
    assert first_payload == second_payload
    order = second_payload["orders"][0]
    assert order["pnl_usd"] == -0.999999
    assert order["canonical_method_resolution"]["post_fee_pnl_usd"] == -0.999999
