from argparse import Namespace
import json

from scripts import run_btc5m_multivenue_residual_matrix as matrix


def test_generation_is_distinct_and_frozen_before_outcomes():
    assert not matrix.GENERATION_CHECKSUM.startswith(matrix.FORBIDDEN_CHECKSUM)
    assert matrix.GENERATION_CONFIG["training_cutoff"] == "2026-07-25T07:10:34Z"
    assert matrix.GENERATION_CONFIG["venues"] == ["binance", "coinbase", "kraken"]
    assert matrix.GENERATION_CONFIG["clock_resolution_s"] == 1


def test_matrix_has_eight_consensus_cells_and_five_residual_lane_slots():
    rows = [
        matrix._preregistration(offset, mode, threshold)
        for offset in matrix.OFFSETS
        for mode in ("taker", "passive")
        for threshold in (None, *matrix.RESIDUAL_THRESHOLDS)
    ]
    assert len([row for row in rows if row["residual_threshold"] is None]) == 8
    assert len(matrix.RESIDUAL_THRESHOLDS) == 5
    assert len({row["cell_id"] for row in rows}) == len(rows)
    assert len({row["checksum"] for row in rows}) == len(rows)


def test_two_of_three_consensus_rejects_unsynchronized_rows():
    samples = [
        {
            "second": 300,
            "clock_spread_s": 0.8,
            "prices": {"binance": 100.0, "coinbase": 100.0, "kraken": 100.0},
        },
        {
            "second": 315,
            "clock_spread_s": 0.9,
            "prices": {"binance": 101.0, "coinbase": 100.5, "kraken": 99.8},
        },
    ]
    feature, blockers = matrix._consensus_feature(samples, window_start=300, offset=15)
    assert not blockers
    assert feature["outcome"] == "Up"
    assert feature["directional_votes"] == {"up": 2, "down": 1}
    samples[1]["clock_spread_s"] = 1.1
    feature, blockers = matrix._consensus_feature(samples, window_start=300, offset=15)
    assert feature is None
    assert blockers == ["synchronized_open_or_signal_clock_missing"]


def test_passive_quote_requires_later_actual_book_cross_to_fill():
    orders = [
        {
            "intent_id": "i-1",
            "market_slug": "btc-updown-5m-300",
            "token_id": "token",
            "limit_price": 0.40,
            "status": "OPEN",
            "last_book_sequence": 1,
            "last_same_price_bid_size": 0.5,
            "queue_ahead_shares_at_quote": 0.0,
            "requested_shares": 0.5,
            "generation_checksum": matrix.GENERATION_CHECKSUM,
            "cell_id": "cell",
            "passive_fill_model_checksum": matrix.PASSIVE_FILL_MODEL_CHECKSUM,
        }
    ]
    events = matrix._advance_passive_orders(
        orders,
        now_ts=400,
        book_loader=lambda *_: {
            "status": "OK",
            "best_ask": 0.40,
            "book_hash": "book",
            "book_timestamp": 399,
            "sequence": 2,
            "same_price_bid_size": 0.0,
        },
    )
    assert orders[0]["status"] == "FILLED"
    assert orders[0]["fill_evidence"]["kind"] == "sequence_continuous_queue_depletion"
    assert events[0]["event"] == "passive_quote_filled"


def test_unfilled_passive_quote_never_becomes_selector_evidence():
    orders = [
        {
            "intent_id": "i-2",
            "market_slug": "btc-updown-5m-300",
            "token_id": "token",
            "limit_price": 0.40,
            "status": "OPEN",
            "last_book_sequence": 1,
            "last_same_price_bid_size": 1.0,
            "queue_ahead_shares_at_quote": 1.0,
            "requested_shares": 0.5,
        }
    ]
    events = matrix._advance_passive_orders(
        orders,
        now_ts=400,
        book_loader=lambda *_: {
            "status": "OK",
            "best_ask": 0.40,
            "sequence": 2,
            "same_price_bid_size": 0.8,
        },
    )
    assert orders[0]["status"] == "OPEN"
    assert events == []


def _guard_cell_args() -> Namespace:
    return Namespace(timeout_s=1.0, clob_timeout_s=1.0, clob_base_url="unused", generation_family="resident_matrix")


def _book(token: str, bid: float, ask: float, tick: float = 0.01) -> dict:
    return {"status": "OK", "token_id": token, "best_bid": bid, "best_ask": ask, "tick_size": tick}


def test_two_sided_post_only_evaluates_both_tokens_and_selects_down() -> None:
    quote, blockers = matrix._choose_two_sided_quote(
        p_up=0.20,
        books={"Up": _book("up", 0.40, 0.41), "Down": _book("down", 0.40, 0.41)},
        family="two_sided_post_only_residual",
        threshold=0.01,
    )
    assert not blockers
    assert quote["outcome"] == "Down"
    assert quote["token_id"] == "down"
    assert quote["executable_price"] == 0.40


def test_inside_spread_maker_is_one_tick_strict_and_charges_improvement() -> None:
    post, _ = matrix._choose_two_sided_quote(
        p_up=0.80, books={"Up": _book("up", 0.40, 0.42), "Down": _book("down", 0.40, 0.42)},
        family="two_sided_post_only_residual", threshold=0.01,
    )
    inside, _ = matrix._choose_two_sided_quote(
        p_up=0.80, books={"Up": _book("up", 0.40, 0.42), "Down": _book("down", 0.40, 0.42)},
        family="two_sided_inside_spread_maker", threshold=0.01,
    )
    assert inside["executable_price"] == 0.41
    assert inside["executable_price"] < inside["best_ask"]
    assert inside["quote_improvement"] == 0.01
    assert inside["required_residual"] > post["required_residual"]
    rejected, blockers = matrix._choose_two_sided_quote(
        p_up=0.80, books={"Up": _book("up", 0.40, 0.405), "Down": _book("down", 0.40, 0.405)},
        family="two_sided_inside_spread_maker", threshold=0.01,
    )
    assert rejected is None
    assert any("inside_spread_strict_post_only_failed" in reason for reason in blockers)


def test_two_sided_cost_gate_and_generation_checksums_are_isolated() -> None:
    quote, blockers = matrix._choose_two_sided_quote(
        p_up=0.50, books={"Up": _book("up", 0.48, 0.50), "Down": _book("down", 0.48, 0.50)},
        family="two_sided_inside_spread_maker", threshold=0.01,
    )
    assert quote is None
    assert any("residual_below_frozen_margin" in reason for reason in blockers)
    _, first = matrix._generation_variant_config("a", "two_sided_post_only_residual", "passive")
    _, second = matrix._generation_variant_config("b", "two_sided_inside_spread_maker", "passive")
    assert first != second


def test_paired_complement_requires_positive_worst_case_cost_and_both_legs() -> None:
    paired, blockers = matrix._choose_paired_complement_quote(
        books={"Up": _book("up", 0.44, 0.45), "Down": _book("down", 0.44, 0.45)}
    )
    assert not blockers
    assert [row["outcome"] for row in paired["legs"]] == ["Up", "Down"]
    assert paired["worst_case_post_cost_edge"] > 0
    refused, blockers = matrix._choose_paired_complement_quote(
        books={"Up": _book("up", 0.49, 0.50), "Down": _book("down", 0.49, 0.50)}
    )
    assert refused is None
    assert "paired_worst_case_post_cost_edge_nonpositive" in blockers


def test_complete_set_pair_is_book_only_strict_post_only_and_one_dollar_total() -> None:
    quote, blockers = matrix._choose_complete_set_paired_quote(
        books={"Up": _book("up", 0.44, 0.45), "Down": _book("down", 0.44, 0.45)}
    )
    assert not blockers
    assert quote["worst_case_post_cost_edge"] > 0
    assert all(row["executable_price"] < row["best_ask"] for row in quote["legs"])
    assert matrix.PAIR_LEG_USD * len(quote["legs"]) == matrix.ORDER_USD
    refused, blockers = matrix._choose_complete_set_paired_quote(
        books={"Up": _book("up", 0.49, 0.50), "Down": _book("down", 0.49, 0.50)}
    )
    assert refused is None
    assert "complete_set_pair_post_cost_edge_nonpositive" in blockers


def test_complete_set_generation_checksum_freezes_book_only_orphan_accounting() -> None:
    config, checksum = matrix._generation_variant_config(
        "complete-set-unit", "complete_set_paired_maker", "passive"
    )
    assert config["paired_signal_dependency"] == "none_book_updates_only"
    assert config["orphan_accounting"] == "canonical_resolution_full_realized_pnl"
    assert config["bundle_total_notional_usd"] == 1.0
    assert checksum != matrix.GENERATION_CHECKSUM


def test_split_sell_overround_requires_full_actual_bid_depth_and_cost_positive() -> None:
    books = {
        "Up": {
            "status": "OK", "token_id": "up", "executable_sell_limit_price": 0.58,
            "avg_sell_fill_price": 0.58, "fillable_sell_shares": 1.0,
        },
        "Down": {
            "status": "OK", "token_id": "down", "executable_sell_limit_price": 0.48,
            "avg_sell_fill_price": 0.48, "fillable_sell_shares": 1.0,
        },
    }
    quote, blockers = matrix._choose_complete_set_split_sell_quote(books=books)
    assert not blockers
    assert quote["worst_case_post_cost_edge"] > 0
    assert quote["inventory_conservation"]["disagreement"] == 0
    refused, blockers = matrix._choose_complete_set_split_sell_quote(
        books={**books, "Down": {**books["Down"], "fillable_sell_shares": 0.5}}
    )
    assert refused is None
    assert blockers == ["Down:split_sell_actual_bid_depth_missing"]


def test_split_sell_copyintent_is_one_share_sell_per_half_dollar_leg() -> None:
    intent = matrix._copy_intent(
        {
            "generation_family": "complete_set_split_sell_overround",
            "executable_price": 0.55,
            "signal_id": "s", "condition_id": "0x" + "11" * 32,
            "market_slug": "btc-updown-5m-300", "outcome": "Up", "token_id": "up",
            "observed_ts": 315.0, "signal_ts": 315.0,
        },
        "cell",
        {"checksum": "pre"},
    )
    assert intent.action == "SELL"
    assert intent.shares == 1.0
    assert intent.copy_size_usd == 0.5


def test_split_sell_leg_ids_are_distinct_by_outcome_and_token() -> None:
    base = {
        "generation_family": "complete_set_split_sell_overround",
        "executable_price": 0.55,
        "signal_id": "same-signal",
        "condition_id": "0x" + "11" * 32,
        "market_slug": "btc-updown-5m-300",
        "observed_ts": 315.0,
        "signal_ts": 315.0,
    }
    up = matrix._copy_intent({**base, "outcome": "Up", "token_id": "up"}, "cell", {"checksum": "pre"})
    down = matrix._copy_intent({**base, "outcome": "Down", "token_id": "down"}, "cell", {"checksum": "pre"})
    assert up.intent_id != down.intent_id
    assert {up.side, down.side} == {"YES", "NO"}


def test_split_sell_terminal_reconciliation_is_append_only_idempotent_and_signed(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(matrix, "GENERATION_CHECKSUM", "generation")
    terminals = tmp_path / "terminals.jsonl"
    events = tmp_path / "events.jsonl"
    cell_state = tmp_path / "cell.json"
    matrix_state = tmp_path / "matrix.json"
    selector_state = tmp_path / "selector.json"
    resolutions = tmp_path / "resolutions.jsonl"
    paired = [
        {
            "intent_id": "colliding-id",
            "source_event_id": "signal",
            "condition_id": "condition",
            "market_slug": "btc-updown-5m-300",
            "outcome": outcome,
            "token_id": token,
            "shares": 1.0,
            "limit_price": price,
            "copy_size_usd": 0.5,
            "metadata": {"parity_disagreement": 0},
        }
        for outcome, token, price in (("Up", "up-token", 0.60), ("Down", "down-token", 0.50))
    ]
    terminal = {
        "terminal_id": "bundle",
        "generation_checksum": "generation",
        "cell_id": "cell",
        "terminal_status": "SIGNAL",
        "window_start_s": 300,
        "market_slug": "btc-updown-5m-300",
        "recorded_at": "2026-07-25T00:04:30Z",
        "paired_intents": paired,
    }
    terminals.write_text(json.dumps(terminal) + "\n", encoding="utf-8")
    cell_state.write_text(json.dumps({"live_orders_allowed": False}), encoding="utf-8")
    resolutions.write_text(
        json.dumps({
            "market_slug": "btc-updown-5m-300",
            "direction": "UP",
            "research_only": False,
            "gamma_lifecycle_ended": True,
            "source": "polymarket_gamma_resolved_outcome",
            "computed_at_iso": "2026-07-25T00:05:30Z",
            "condition_id": "condition",
        }) + "\n",
        encoding="utf-8",
    )
    matrix_state.write_text(json.dumps({
        "generation_checksum": "generation",
        "status": "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE",
        "stop_writer": True,
        "complete_liveness_window_starts_s": [300],
        "attribution_funnel": {"resolved_selector_cells": 0},
        "cells": [{
            "cell_id": "cell",
            "execution_mode": "paired_split_sell",
            "generation_checksum": "generation",
            "model_checksum": "generation",
            "paired_bundle_required": True,
            "paper_only": True,
            "actual_depth_verified": True,
            "pair_accounting_disagreement": 0,
            "state_path": str(cell_state),
            "terminals_path": str(terminals),
            "fill_events_path": str(events),
        }],
    }), encoding="utf-8")
    args = Namespace(
        state=str(matrix_state), selector_state=str(selector_state), resolutions=str(resolutions),
        generation_family="complete_set_split_sell_overround",
    )
    terminal_before = terminals.read_bytes()
    first = matrix.reconcile_split_sell_terminal_evidence(args)
    second = matrix.reconcile_split_sell_terminal_evidence(args)
    event = json.loads(events.read_text(encoding="utf-8").strip())
    assert first["appended_bundle_events"] == 1
    assert second["appended_bundle_events"] == 0
    assert second["resolved_selector_cells"] == 1
    assert terminals.read_bytes() == terminal_before
    assert len({row["intent_id"] for row in event["legs"]}) == 2
    assert event["split_sell_accounting"]["realized_post_cost_pnl_usd"] > 0
    assert event["split_sell_accounting"]["inventory_conservation"]["disagreement"] == 0
    repaired = json.loads(matrix_state.read_text(encoding="utf-8"))
    assert repaired["status"] == "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE"
    assert repaired["complete_liveness_window_starts_s"] == [300]
    assert repaired["resolution_reconciliation"]["clock_mutated"] is False


def test_sell_book_snapshot_consumes_descending_actual_bid_depth() -> None:
    class Level:
        def __init__(self, price, size):
            self.price, self.size = price, size

    class Book:
        timestamp = 123
        bids = [Level("0.50", "0.4"), Level("0.48", "0.8")]

    class Clob:
        def get_book(self, _token):
            return Book()

    snapshot = matrix._sell_book_snapshot(clob=Clob(), token_id="up", shares=1.0)
    assert snapshot["status"] == "OK"
    assert snapshot["fillable_sell_shares"] == 1.0
    assert snapshot["executable_sell_limit_price"] == 0.48
    assert snapshot["avg_sell_fill_price"] == 0.488


def test_split_sell_partial_fill_is_charged_and_unsold_winner_carries_to_resolution() -> None:
    accounting = matrix._settle_split_sell_inventory(
        legs=[
            {
                "outcome": "Up",
                "requested_shares": 1.0,
                "filled_shares": 1.0,
                "fill_price": 0.53,
            },
            {
                "outcome": "Down",
                "requested_shares": 1.0,
                "filled_shares": 0.25,
                "fill_price": 0.52,
            },
        ],
        winner="Down",
    )
    assert accounting["unsold_inventory"] == {"Up": 0.0, "Down": 0.75}
    assert accounting["resolution_value_usd"] == 0.75
    assert accounting["inventory_conservation"]["disagreement"] == 0
    assert accounting["resolved"]
    assert not accounting["dual_leg_fill_verified"]


def test_complete_set_signal_does_not_complete_clock_before_full_window() -> None:
    assert not matrix._complete_set_window_complete(terminal_present=True, elapsed_s=269.999)
    assert matrix._complete_set_window_complete(terminal_present=True, elapsed_s=270.0)
    assert not matrix._complete_set_window_complete(terminal_present=False, elapsed_s=300.0)
    assert not matrix._generation_window_clock_complete(
        family="complete_set_paired_maker",
        cells=[{"current_window_terminal_complete": False}],
        complete_clock_offsets=set(matrix.OFFSETS),
    )
    assert matrix._generation_window_clock_complete(
        family="complete_set_paired_maker",
        cells=[{"current_window_terminal_complete": True}],
        complete_clock_offsets=set(),
    )


def test_liveness_counts_only_complete_prospective_raw_clock_windows() -> None:
    prior = {"complete_liveness_window_starts_s": [100, 300]}
    assert matrix._complete_liveness_windows(
        prior, window_start=600, clock_complete=False, complete_after_s=300
    ) == [300]
    assert matrix._complete_liveness_windows(
        prior, window_start=600, clock_complete=True, complete_after_s=300
    ) == [300, 600]


def test_complete_set_generation_never_emits_partial_launch_window_evidence() -> None:
    assert not matrix._generation_evidence_window_eligible(
        family="complete_set_split_sell_overround", window_start=300, liveness_start=600
    )
    assert matrix._generation_evidence_window_eligible(
        family="complete_set_split_sell_overround", window_start=600, liveness_start=600
    )
    assert matrix._generation_evidence_window_eligible(
        family="resident_matrix", window_start=300, liveness_start=600
    )


def test_generation_checksums_isolate_new_execution_mechanisms() -> None:
    families = (
        "paired_complement_fill_then_hedge",
        "paired_complement_dual_ioc",
        "basis_triggered_single_outcome_maker",
    )
    checksums = {
        matrix._generation_variant_config(f"fresh-{family}", family, "passive")[1]
        for family in families
    }
    assert len(checksums) == len(families)


def test_fill_then_hedge_requires_locked_positive_executable_hedge() -> None:
    books = {
        "Up": {**_book("up", 0.42, 0.43), "avg_fill_price": 0.43, "fillable_usd": 1.0},
        "Down": {**_book("down", 0.42, 0.43), "avg_fill_price": 0.43, "fillable_usd": 1.0},
    }
    quote, blockers = matrix._choose_fill_then_hedge_quote(books=books)
    assert not blockers
    assert quote["legs"][0]["execution"] == "PASSIVE_FIRST"
    assert quote["legs"][1]["execution"] == "EXECUTABLE_DEPTH_HEDGE"
    refused, blockers = matrix._choose_fill_then_hedge_quote(
        books={
            "Up": {**books["Up"], "fillable_usd": 0.0},
            "Down": {**books["Down"], "fillable_usd": 0.0},
        }
    )
    assert refused is None
    assert blockers

    completion, failure = matrix._fill_then_hedge_completion(
        {
            "fill_price": 0.42,
            "one_leg_risk_per_share": 0.01,
            "hedge_leg": {"token_id": "down", "outcome": "Down"},
        },
        hedge_book={"status": "OK", "avg_fill_price": 0.43, "fillable_usd": 1.0},
    )
    assert failure is None
    assert completion["locked_post_cost_edge"] > 0
    refused_completion, failure = matrix._fill_then_hedge_completion(
        {
            "fill_price": 0.49,
            "one_leg_risk_per_share": 0.01,
            "hedge_leg": {"token_id": "down", "outcome": "Down"},
        },
        hedge_book={"status": "OK", "avg_fill_price": 0.50, "fillable_usd": 1.0},
    )
    assert refused_completion is None
    assert failure == "fill_then_hedge_locked_value_nonpositive_at_fill"


def test_dual_ioc_prices_chronological_depth_and_one_leg_failure() -> None:
    books = {
        "Up": {**_book("up", 0.44, 0.45), "avg_fill_price": 0.45, "fillable_usd": 1.0},
        "Down": {**_book("down", 0.44, 0.45), "avg_fill_price": 0.45, "fillable_usd": 1.0},
    }
    quote, blockers = matrix._choose_dual_ioc_quote(books=books)
    assert not blockers
    assert [row["chronological_sequence"] for row in quote["legs"]] == [1, 2]
    assert quote["one_leg_failure_cost"] > 0


def test_basis_maker_requires_three_venue_displacement_and_post_cost_edge() -> None:
    feature = {
        "outcome": "Up",
        "consensus_probability": 0.8,
        "venue_returns": {"binance": 0.003, "coinbase": 0.002, "kraken": -0.001},
    }
    quote, blockers = matrix._choose_basis_maker_quote(
        feature=feature,
        books={"Up": _book("up", 0.40, 0.41), "Down": _book("down", 0.40, 0.41)},
        offset=15,
    )
    assert not blockers
    assert quote["basis_displacement"] > quote["basis_trigger"]
    refused, blockers = matrix._choose_basis_maker_quote(
        feature={**feature, "venue_returns": {"binance": 0.001, "coinbase": 0.001, "kraken": 0.001}},
        books={"Up": _book("up", 0.40, 0.41)},
        offset=15,
    )
    assert refused is None
    assert blockers == ["three_venue_basis_displacement_below_trigger"]


def _guard_cell_feature() -> dict:
    return {
        "outcome": "Up",
        "consensus_probability": 0.80,
        "venue_returns": {"binance": 0.01, "coinbase": 0.01, "kraken": -0.001},
        "directional_votes": {"up": 2, "down": 1},
    }


def _patch_guard_cell_sources(monkeypatch):
    class FakeBook:
        timestamp = 1
        bids = []

    class FakeClob:
        def __init__(self, *args, **kwargs):
            pass

        def get_book(self, token_id):
            return FakeBook()

    monkeypatch.setattr(matrix, "CLOBMarketClient", FakeClob)
    monkeypatch.setattr(
        matrix,
        "_market_for_slug",
        lambda slug, timeout_s: {
            "conditionId": "condition",
            "clobTokenIds": ["up-token", "down-token"],
            "outcomes": ["Up", "Down"],
        },
    )
    monkeypatch.setattr(
        matrix,
        "_book_snapshot_with_direct_fallback",
        lambda **kwargs: {
            "status": "OK",
            "avg_fill_price": 0.40,
            "best_ask": 0.40,
            "best_bid": 0.39,
            "fillable_usd": 1.0,
            "book_hash": "book",
        },
    )


def test_cell_state_exposes_guard_consumable_taker_terminal(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_guard_cell_sources(monkeypatch)
    row = matrix._evaluate_cell(
        _guard_cell_args(),
        now_ts=315.0,
        offset=15,
        mode="taker",
        threshold=None,
        feature=_guard_cell_feature(),
        feature_blockers=[],
    )
    state = matrix.load_json(row["state_path"], default={})
    terminal = state["current_terminal"]
    assert terminal["terminal_status"] == "SIGNAL"
    assert terminal["model_checksum"] == matrix.GENERATION_CHECKSUM
    assert terminal["signal"]["window_start_s"] == 300
    assert terminal["signal"]["net_edge_per_share"] > 0
    assert terminal["intent"]["intent_id"]
    assert row["terminal_count"] == 1


def test_cell_state_exposes_guard_consumable_passive_decision(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_guard_cell_sources(monkeypatch)
    row = matrix._evaluate_cell(
        _guard_cell_args(),
        now_ts=315.0,
        offset=15,
        mode="passive",
        threshold=0.01,
        feature=_guard_cell_feature(),
        feature_blockers=[],
    )
    state = matrix.load_json(row["state_path"], default={})
    assert state["current_terminal"]["terminal_status"] == "SIGNAL"
    assert (
        state["current_terminal"]["passive_guard_projection"][
            "passive_fill_model_checksum"
        ]
        == matrix.PASSIVE_FILL_MODEL_CHECKSUM
    )
    assert state["current_decision"]["eligible"] is True
    assert state["current_decision"]["quote_price"] == 0.39
    assert state["current_decision"]["window_start_s"] == 300
    assert state["current_decision"]["generation_checksum"] == matrix.GENERATION_CHECKSUM
    assert row["terminal_count"] == 1
