import json
import time
from argparse import Namespace
from collections import Counter
from datetime import UTC, datetime, timedelta

from scripts import reconcile_wide_exact_policy_paper as subject
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd


WALLET = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
TOKEN_UP = "101"
TOKEN_DOWN = "202"
SLUG = "btc-updown-5m-1000"
CONDITION = "0xcondition"


def test_default_history_uses_live_guard_hot_rtds_state():
    assert subject.DEFAULT_HISTORY == (
        "data/research/wallet_copy_live_guard_hot_history_state.json"
    )


def test_metadata_summary_emits_explicit_integer_yield(tmp_path, monkeypatch):
    cache = tmp_path / "cache.json"
    cache.write_text(json.dumps({"known": {"market_slug": "known"}}))
    monkeypatch.setattr(
        subject,
        "_token_metadata",
        lambda _path: {
            "known": {"market_slug": "ignored"},
            "new": {"market_slug": "btc-updown-5m-1000"},
        },
    )
    summary = {}

    meta = subject._metadata_for_events(
        "history.json", "gamma", [], str(cache), summary
    )

    assert meta["known"]["market_slug"] == "known"
    assert summary == {"new_tokens_merged": 1, "new_tokens_from_hot_history": 1}
    assert isinstance(summary["new_tokens_merged"], int)


def test_direct_event_file_is_consumed_without_argv_json(tmp_path):
    payload = [{"transaction_hash": "0xabc", "blob": "x" * 1_100_000}]
    path = tmp_path / "direct.json"
    path.write_text(json.dumps(payload))

    loaded = subject.direct_event_input(
        Namespace(direct_event_json="", direct_event_file=str(path))
    )

    assert json.loads(loaded) == payload


def test_empty_direct_cycle_records_upstream_fetch_zero(tmp_path, monkeypatch):
    monkeypatch.setattr(subject, "_metadata_for_events", lambda *args: {})
    args = Namespace(
        wallet=[WALLET],
        manifest="",
        cohort_id="cohort",
        run_id="run",
        polygon_jsonl="unused.jsonl",
        direct_event_json="[]",
        scan_limit=10,
        history_state="history",
        gamma_base_url="gamma",
        alpha_report=str(tmp_path / "alpha.json"),
        clob_base_url="clob",
        clob_timeout_s=1,
        resolutions=str(tmp_path / "resolutions.jsonl"),
    )

    state, _events = subject.score_cycle(args, {
        "cohort": {"cohort_id": "cohort", "run_id": "run",
                   "run_ids": ["run"], "started_at_s": 0.0}
    })

    assert state["generation_flow"]["empty_generation"] is True
    assert state["generation_flow"]["empty_stage"] == "FETCH_ZERO"
    assert state["generation_flow"]["stages"]["fetch"] == {
        "input_direct_rows": 0,
        "output_rows": 0,
    }


def _order(order_id="one", token=TOKEN_UP, ts=1010.0, price=0.4, shares=2.0):
    return {
        "order_id": order_id,
        "wallet": WALLET,
        "token_id": token,
        "condition_id": CONDITION,
        "market_slug": SLUG,
        "source_event_ts": ts,
        "fill_price": price,
        "filled_shares": shares,
        "filled_cost_usd": round(price * shares, 6),
        "receipt_to_book_fetch_lag_s": 0.25,
        "resolved": False,
        "expected_fee_usd": None,
        "pre_fee_pnl_usd": None,
        "post_fee_pnl_usd": None,
    }


def _resolution():
    return {
        "market_slug": SLUG,
        "condition_id": CONDITION,
        "direction": "UP",
        "yes_token": TOKEN_UP,
        "no_token": TOKEN_DOWN,
        "source": "test",
        "computed_at_iso": "2026-07-25T00:00:00Z",
    }


def _prior(orders=None):
    return {
        "cohort": {
            "cohort_id": "cohort_test",
            "run_id": "run_test",
            "started_at_s": 1000.0,
        },
        "orders": orders or [],
    }


def test_manifest_policy_consumers_skip_policy_absent_rows():
    manifest = {
        "capture_watch_wallets": [
            {
                "wallet": WALLET,
                "move_slice_keys": [],
                "policy_absent": True,
            }
        ]
    }

    assert subject.manifest_wallet_policy_identities(manifest) == {}
    assert subject._manifest_policy(manifest) == ([], {})


def test_resolution_join_accounts_for_winner_loser_and_expected_fee():
    rows, events = subject.apply_resolutions(
        [_order("win", TOKEN_UP), _order("loss", TOKEN_DOWN)],
        [_resolution()],
    )
    winner, loser = rows
    fee = expected_polymarket_buy_fee_usd(shares=2.0, price=0.4)
    assert winner["won"] is True
    assert winner["payout_usd"] == 2.0
    assert winner["pre_fee_pnl_usd"] == 1.2
    assert winner["post_fee_pnl_usd"] == round(1.2 - fee, 6)
    assert loser["won"] is False
    assert loser["pre_fee_pnl_usd"] == -0.8
    assert loser["post_fee_pnl_usd"] == round(-0.8 - fee, 6)
    assert len(events) == 2


def test_duplicate_fill_replay_appends_one_logical_order():
    state, events = subject.reconcile_state(
        _prior(),
        new_orders=[_order(), _order()],
        resolutions=[],
        attempts={WALLET: 1},
        refusals={WALLET: Counter()},
        wallet_order=[WALLET],
    )
    assert len(state["orders"]) == 1
    assert [row["event"] for row in events] == ["wide_exact_policy_paper_order_filled"]


def test_manifest_rotation_drops_historical_attempt_terminals_from_generation_reconciliation(
    monkeypatch,
    tmp_path,
):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_id": "new-manifest",
                "generated_at": "2026-07-25T06:45:00Z",
                "capture_watch_wallets": [
                    {"wallet": WALLET, "move_slice_keys": ["060-120|0.25-0.50"]}
                ],
                "admitted_wallets": [
                    {"wallet": WALLET, "move_slice_keys": ["060-120|0.25-0.50"]}
                ],
            }
        )
    )
    monkeypatch.setattr(subject, "_metadata_for_events", lambda *_: {})
    args = Namespace(
        wallet=[],
        manifest=str(manifest_path),
        cohort_id="",
        run_id="new-run",
        polygon_jsonl="unused.jsonl",
        direct_event_json="[]",
        scan_limit=10,
        history_state="history",
        gamma_base_url="gamma",
        alpha_report="alpha",
        clob_base_url="clob",
        clob_timeout_s=1,
        resolutions=str(tmp_path / "resolutions.jsonl"),
    )
    prior = {
        "manifest": {"manifest_id": "old-manifest"},
        "cohort": {
            "cohort_id": "old-cohort",
            "run_id": "old-run",
            "run_ids": ["old-run"],
            "started_at_s": 0.0,
        },
        "attempt_terminals": [
            {
                "attempt_id": "historical",
                "cohort_id": "old-cohort",
                "run_id": "old-run",
            }
        ],
        "terminal_source_ids": ["historical"],
        "observed_source_ids": [],
    }

    state, _ = subject.score_cycle(args, prior)

    assert state["attempt_terminals"] == []
    assert state["terminal_reconciliation"] == {
        "run_id": "new-run",
        "cohort_id": state["cohort"]["cohort_id"],
        "input_rows": 0,
        "terminal_rows": 0,
        "input_equals_terminal": True,
        "direct_event_handoff": True,
        "scope": "generation_local_tx_hash_log_index_wallet_buy",
    }


def test_run_rotation_retains_same_cohort_attempt_terminals(monkeypatch, tmp_path):
    monkeypatch.setattr(subject, "_metadata_for_events", lambda *_: {})
    args = Namespace(
        wallet=[WALLET],
        manifest="",
        cohort_id="cohort",
        run_id="new-run",
        polygon_jsonl="unused.jsonl",
        direct_event_json="[]",
        scan_limit=10,
        history_state="history",
        gamma_base_url="gamma",
        alpha_report=str(tmp_path / "alpha.json"),
        clob_base_url="clob",
        clob_timeout_s=1,
        resolutions=str(tmp_path / "resolutions.jsonl"),
    )
    terminal = {
        "attempt_id": "historical",
        "cohort_id": "cohort",
        "run_id": "old-run",
        "wallet": WALLET,
        "f1_f4_terminal": {
            "F2_alpha_profile": "PASS",
            "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
        },
    }
    prior = {
        "cohort": {
            "cohort_id": "cohort",
            "run_id": "old-run",
            "run_ids": ["old-run"],
            "started_at_s": 0.0,
        },
        "attempt_terminals": [terminal],
        "terminal_source_ids": ["historical"],
        "observed_source_ids": ["historical"],
    }

    state, _ = subject.score_cycle(args, prior)

    assert state["cohort"]["run_ids"] == ["old-run", "new-run"]
    assert state["attempt_terminals"] == [terminal]
    assert state["terminal_reconciliation"]["input_rows"] == 1
    assert state["terminal_reconciliation"]["terminal_rows"] == 1
    assert state["terminal_reconciliation"]["input_equals_terminal"] is True


def test_late_resolution_survives_restart_and_changes_one_order_once():
    first, first_events = subject.reconcile_state(
        _prior(),
        new_orders=[_order()],
        resolutions=[],
        attempts={WALLET: 1},
        refusals={WALLET: Counter()},
        wallet_order=[WALLET],
    )
    assert first["orders"][0]["resolved"] is False
    second, second_events = subject.reconcile_state(
        first,
        new_orders=[],
        resolutions=[_resolution()],
        attempts={WALLET: 1},
        refusals={WALLET: Counter()},
        wallet_order=[WALLET],
    )
    assert second["orders"][0]["resolved"] is True
    assert len(second_events) == 1
    third, third_events = subject.reconcile_state(
        second,
        new_orders=[],
        resolutions=[_resolution()],
        attempts={WALLET: 1},
        refusals={WALLET: Counter()},
        wallet_order=[WALLET],
    )
    assert third["orders"] == second["orders"]
    assert third_events == []
    assert len(first_events) == 1


def test_summary_exposes_fee_coverage_halves_lag_and_checkpoint():
    orders = []
    for index in range(10):
        row = _order(str(index), ts=1010.0 + index)
        row.update(
            {
                "resolved": True,
                "expected_fee_usd": 0.01,
                "pre_fee_pnl_usd": 0.11,
                "post_fee_pnl_usd": 0.10,
                "market_slug": f"{SLUG}-{index // 2}",
                "receipt_to_book_fetch_lag_s": 0.1 + index / 10,
            }
        )
        orders.append(row)
    state = subject.build_summary(
        cohort=_prior()["cohort"],
        orders=orders,
        attempts={WALLET: 10},
        refusals={WALLET: Counter({"metadata_missing": 2})},
        wallet_order=[WALLET],
    )
    wallet = state["wallets"][WALLET]
    assert wallet["fee_coverage_pct"] == 100.0
    assert wallet["first_half_post_fee_pnl_usd"] == 0.5
    assert wallet["second_half_post_fee_pnl_usd"] == 0.5
    assert wallet["max_receipt_to_book_fetch_lag_s"] == 1.0
    assert wallet["checkpoint"] == 10
    assert wallet["refusal_counts"] == {"metadata_missing": 2}


def test_positive_alpha_policy_uses_only_positive_70pct_slices():
    alpha = {
        "execution_profiles": {
            "profiles_by_wallet": {
                WALLET: {
                    "eligible": True,
                    "move_slices": [
                        {"move_slice_key": "a", "mean_edge": 0.1, "median_edge": 0.1, "copyable_rate_pct": 70},
                        {"move_slice_key": "b", "mean_edge": -0.1, "median_edge": 0.1, "copyable_rate_pct": 100},
                        {"move_slice_key": "c", "mean_edge": 0.1, "median_edge": 0.1, "copyable_rate_pct": 69},
                    ],
                }
            }
        }
    }
    assert subject._positive_profile_slices(alpha, [WALLET]) == {WALLET: {"a"}}


def test_positive_alpha_policy_does_not_double_veto_slice_with_profile_aggregate():
    alpha = {
        "execution_profiles": {
            "profiles_by_wallet": {
                WALLET: {
                    "eligible": False,
                    "blockers": ["execution_profile_copyable_rate_below_threshold"],
                    "move_slices": [
                        {
                            "move_slice_key": "060-120|<=0.25",
                            "mean_edge": 0.020524,
                            "median_edge": 0.015,
                            "copyable_rate_pct": 71.428571,
                        },
                        {
                            "move_slice_key": "060-120|>0.75",
                            "mean_edge": 0.025635,
                            "median_edge": 0.015,
                            "copyable_rate_pct": 77.272727,
                        },
                        {
                            "move_slice_key": "060-120|0.50-0.75",
                            "mean_edge": 0.004655,
                            "median_edge": -0.005,
                            "copyable_rate_pct": 48.275862,
                        },
                    ],
                }
            }
        }
    }

    selected = subject._positive_profile_slices(alpha, [WALLET])[WALLET]

    assert selected == {"060-120|<=0.25", "060-120|>0.75"}
    assert "060-120|0.50-0.75" not in selected


def test_reconciler_refuses_stale_alpha_report(tmp_path):
    alpha_path = tmp_path / "alpha.json"
    alpha_path.write_text(
        json.dumps(
            {"updated_at": (datetime.now(tz=UTC) - timedelta(hours=43)).isoformat()}
        )
    )

    try:
        subject._load_fresh_alpha_report(str(alpha_path))
    except ValueError as exc:
        assert "STALE_ALPHA_REPORT_REFUSED" in str(exc)
        assert "age_h=" in str(exc)
        assert "max_age_h=24.000000" in str(exc)
    else:
        raise AssertionError("stale alpha report was accepted")


def test_reconciler_refuses_manifest_bound_to_stale_alpha(tmp_path):
    alpha_path = tmp_path / "alpha.json"
    alpha_path.write_text(
        json.dumps(
            {"updated_at": (datetime.now(tz=UTC) - timedelta(hours=43)).isoformat()}
        )
    )

    try:
        subject._require_fresh_manifest_alpha(
            {"source_alpha_report": str(alpha_path)}, manifest_path="manifest.json"
        )
    except ValueError as exc:
        assert "STALE_ALPHA_REPORT_REFUSED" in str(exc)
        assert str(alpha_path) in str(exc)
    else:
        raise AssertionError("manifest with stale alpha binding was accepted")


def test_fetch_provenance_is_independent_of_computed_lag_outcome(monkeypatch):
    monkeypatch.setattr(subject.time, "monotonic", lambda: 99.0)
    prefetch = {
        "fetch_provenance": "capture_prefetched",
        "fetch_cycle_id": "cycle",
        "fetch_started_monotonic_s": 12.5,
    }
    before = subject.terminal_fetch_context(
        prefetch=prefetch,
        raw_row={"recv_monotonic_s": 10.0, "receipt_to_fetch_ms": 2500.0},
        default_cycle_id="fallback",
        token_event_ordinal_in_cycle=3,
        token_cycle_first_recv_monotonic_s=9.0,
    )
    after = subject.terminal_fetch_context(
        prefetch=prefetch,
        raw_row={"recv_monotonic_s": 10.0, "receipt_to_fetch_ms": 9000.0},
        default_cycle_id="fallback",
        token_event_ordinal_in_cycle=3,
        token_cycle_first_recv_monotonic_s=9.0,
    )

    assert before == after
    assert before == {
        "fetch_provenance": "capture_prefetched",
        "fetch_cycle_id": "cycle",
        "fetch_started_monotonic_s": 12.5,
        "fetch_started_monotonic_observed": True,
        "recv_monotonic_s": 10.0,
        "fanout_received_monotonic_s": None,
        "upstream_to_fanout_ms": None,
        "fanout_to_fetch_ms": None,
        "prefetch_queue_wait_ms": None,
        "prefetch_worker_queue_ms": None,
        "prefetch_network_ms": None,
        "prefetch_parse_ms": None,
        "fetch_pairing_check": None,
        "capture_to_decision_ms": 86500.0,
        "token_cycle_first_recv_monotonic_s": 9.0,
        "token_event_ordinal_in_cycle": 3,
    }


def test_fetch_provenance_rejects_unknown_literal():
    import pytest

    with pytest.raises(ValueError, match="unknown fetch provenance"):
        subject.terminal_fetch_context(
            prefetch={"fetch_provenance": "derived_from_lag"},
            raw_row={},
            default_cycle_id="cycle",
            token_event_ordinal_in_cycle=0,
            token_cycle_first_recv_monotonic_s=None,
        )


def test_missing_fetch_stamp_is_explicitly_unobserved(monkeypatch):
    monkeypatch.setattr(subject.time, "monotonic", lambda: 99.0)
    context = subject.terminal_fetch_context(
        prefetch={},
        raw_row={"recv_monotonic_s": 10.0},
        default_cycle_id="cycle",
        token_event_ordinal_in_cycle=0,
        token_cycle_first_recv_monotonic_s=10.0,
    )

    assert context["fetch_started_monotonic_s"] is None
    assert context["fetch_started_monotonic_observed"] is False


def test_fetch_context_separates_upstream_fanout_from_paper_prefetch_delay():
    context = subject.terminal_fetch_context(
        prefetch={
            "fetch_provenance": "capture_prefetched",
            "fetch_started_monotonic_s": 22.1,
            "prefetch_queue_wait_ms": 101.0,
        },
        raw_row={
            "recv_monotonic_s": 10.0,
            "fanout_received_monotonic_s": 22.0,
        },
        default_cycle_id="cycle",
        token_event_ordinal_in_cycle=0,
        token_cycle_first_recv_monotonic_s=10.0,
    )

    assert context["upstream_to_fanout_ms"] == 12000.0
    assert context["fanout_to_fetch_ms"] == 100.0
    assert context["fetch_pairing_check"] == "PASS"


def test_fetch_pairing_failure_is_reporting_only(monkeypatch):
    monkeypatch.setattr(subject.time, "monotonic", lambda: 30.0)
    context = subject.terminal_fetch_context(
        prefetch={
            "fetch_provenance": "capture_prefetched",
            "fetch_started_monotonic_s": 22.1,
            "prefetch_queue_wait_ms": 1.0,
        },
        raw_row={"recv_monotonic_s": 10.0, "fanout_received_monotonic_s": 22.0},
        default_cycle_id="cycle",
        token_event_ordinal_in_cycle=0,
        token_cycle_first_recv_monotonic_s=10.0,
    )

    assert context["fetch_pairing_check"] == "FAIL"
    assert context["capture_to_decision_ms"] == 7900.0


def test_metadata_join_combines_history_and_gamma(monkeypatch):
    monkeypatch.setattr(subject, "_token_metadata", lambda _: {"old": {"market_slug": "old"}})
    monkeypatch.setattr(
        subject,
        "_gamma_token_metadata",
        lambda base, starts: {"new": {"market_slug": str(starts[0])}},
    )
    result = subject._metadata_for_events("history.json", "gamma", [{"block_ts": 1199.0}])
    assert result == {
        "old": {
            "market_slug": "old",
            "token_mapping_source": "rtds_source_event_derived",
        },
        "new": {"market_slug": "900"},
    }


def test_metadata_join_uses_normalized_direct_event_timestamp(monkeypatch):
    monkeypatch.setattr(subject, "_token_metadata", lambda _: {})
    monkeypatch.setattr(
        subject,
        "_normalized_realtime_event",
        lambda _: {"asset": TOKEN_UP, "event_ts": 1510.0},
    )
    monkeypatch.setattr(
        subject,
        "_gamma_token_metadata",
        lambda base, starts: {TOKEN_UP: {"market_slug": str(starts[0])}},
    )

    result = subject._metadata_for_events(
        "history.json",
        "gamma",
        [{"decoded": {"asset": TOKEN_UP}}],
    )

    assert result[TOKEN_UP]["market_slug"] == "1500"


def test_score_cycle_missing_metadata_fails_closed(tmp_path, monkeypatch):
    raw = {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "block_ts": 1010.0,
        "received_at_s": 1011.1,
        "transaction_hash": "0xtx",
        "log_index": 4,
        "maker": WALLET,
        "decoded": {
            "decode_status": "OK",
            "asset": TOKEN_UP,
            "price": 0.4,
            "size": 10,
            "maker": WALLET,
            "maker_side": "BUY",
        },
    }
    polygon = tmp_path / "polygon.jsonl"
    polygon.write_text(json.dumps(raw) + "\n")
    monkeypatch.setattr(subject, "_metadata_for_events", lambda *args: {})
    monkeypatch.setattr(subject.time, "time", lambda: 1011.0)
    args = Namespace(
        wallet=[WALLET],
        cohort_id="cohort",
        run_id="run",
        polygon_jsonl=str(polygon),
        scan_limit=10,
        history_state="history",
        gamma_base_url="gamma",
        alpha_report="alpha",
        clob_base_url="clob",
        clob_timeout_s=1,
        resolutions=str(tmp_path / "resolutions.jsonl"),
        manifest="",
    )
    state, events = subject.score_cycle(
        args,
        {
            "cohort": {
                "cohort_id": "cohort",
                "run_id": "run",
                "run_ids": ["run"],
                "started_at_s": 0.0,
            }
        },
    )
    assert events == []
    assert state["orders"] == []
    assert state["wallets"][WALLET]["refusal_counts"] == {"metadata_missing": 1}
    assert state["wallets"][WALLET]["attempted_exact_policy_buys"] == 0
    assert state["terminal_source_ids"] == []
    assert len(state["observed_source_ids"]) == 1


def test_metadata_retry_rows_recovers_only_requested_immutable_attempt(tmp_path):
    raw = {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "block_ts": 1010.0,
        "received_at_s": 1011.1,
        "transaction_hash": "0xtx",
        "log_index": 4,
        "maker": WALLET,
        "decoded": {
            "decode_status": "OK",
            "asset": TOKEN_UP,
            "price": 0.4,
            "size": 10,
            "maker": WALLET,
            "maker_side": "BUY",
        },
    }
    normalized = subject._normalized_realtime_event(raw)
    attempt_id = subject._order_identity("cohort", "run", WALLET, normalized)
    polygon = tmp_path / "polygon.jsonl"
    polygon.write_text(
        "\n".join(
            (
                json.dumps({**raw, "decoded": {**raw["decoded"], "asset": TOKEN_DOWN}}),
                json.dumps(raw),
            )
        )
        + "\n"
    )

    recovered = subject._metadata_retry_rows(
        str(polygon),
        token_ids={TOKEN_UP},
        desired_attempt_ids={attempt_id},
        cohort_id="cohort",
        run_id="run",
        wallets={WALLET},
    )

    assert recovered == [raw]


def test_authoritative_ledger_replay_dedupes_physical_rows_and_resolution():
    state, fill_events = subject.reconcile_state(
        _prior(),
        new_orders=[_order()],
        resolutions=[],
        attempts={WALLET: 1},
        refusals={WALLET: Counter()},
        wallet_order=[WALLET],
    )
    replayed, event_ids = subject.replay_ledger(fill_events + fill_events)
    assert len(replayed) == 1
    assert len(event_ids) == 1
    resolved, resolution_events = subject.reconcile_state(
        {**state, "orders": replayed},
        new_orders=[],
        resolutions=[_resolution()],
        attempts={WALLET: 1},
        refusals={WALLET: Counter()},
        wallet_order=[WALLET],
    )
    crash_recovered, recovered_ids = subject.replay_ledger(
        fill_events + fill_events + resolution_events + resolution_events
    )
    assert len(crash_recovered) == 1
    assert crash_recovered[0]["resolved"] is True
    assert crash_recovered[0]["post_fee_pnl_usd"] == resolved["orders"][0]["post_fee_pnl_usd"]
    assert len(recovered_ids) == 2


def test_metadata_arriving_next_cycle_produces_exactly_one_fill(tmp_path, monkeypatch):
    raw = {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "block_ts": 1010.0,
        "received_at_s": 1011.1,
        "transaction_hash": "0xtx",
        "log_index": 4,
        "maker": WALLET,
        "decoded": {
            "decode_status": "OK",
            "asset": TOKEN_UP,
            "price": 0.4,
            "size": 10,
            "maker": WALLET,
            "maker_side": "BUY",
        },
    }
    polygon = tmp_path / "polygon.jsonl"
    polygon.write_text(json.dumps(raw) + "\n")
    alpha_path = tmp_path / "alpha.json"
    alpha_path.write_text(
        json.dumps(
            {
                "execution_profiles": {
                    "profiles_by_wallet": {
                        WALLET: {
                            "eligible": True,
                            "move_slices": [
                                {
                                    "move_slice_key": "060-120|0.25-0.50",
                                    "mean_edge": 0.1,
                                    "median_edge": 0.1,
                                    "copyable_rate_pct": 100,
                                }
                            ],
                        }
                    }
                }
            }
        )
    )
    calls = {"count": 0}

    def metadata(*_):
        calls["count"] += 1
        if calls["count"] == 1:
            return {}
        return {
            TOKEN_UP: {
                "market_slug": "btc-updown-5m-900",
                "condition_id": CONDITION,
                "outcome": "Up",
            }
        }

    class FakeClob:
        def __init__(self, **_):
            pass

        def get_book(self, _):
            return {
                "asset_id": TOKEN_UP,
                "timestamp": 1011100,
                "hash": "book",
                "asks": [{"price": "0.4", "size": "10"}],
                "bids": [{"price": "0.39", "size": "10"}],
            }

        @staticmethod
        def summarize_book(*_, **__):
            return {
                "instant_fill_status": "PASS",
                "fill_ratio": 1.0,
                "fillable_shares": 2.5,
                "avg_fill_price": 0.4,
                "fillable_usd": 1.0,
            }

    monkeypatch.setattr(subject, "_metadata_for_events", metadata)
    monkeypatch.setattr(subject, "CLOBMarketClient", FakeClob)
    monkeypatch.setattr(subject.time, "time", lambda: 1011.0)
    args = Namespace(
        wallet=[WALLET],
        manifest="",
        cohort_id="cohort",
        run_id="run",
        polygon_jsonl=str(polygon),
        scan_limit=10,
        history_state="history",
        gamma_base_url="gamma",
        alpha_report=str(alpha_path),
        clob_base_url="clob",
        clob_timeout_s=1,
        resolutions=str(tmp_path / "resolutions.jsonl"),
    )
    first, first_events = subject.score_cycle(args, {})
    second, second_events = subject.score_cycle(args, first)
    third, third_events = subject.score_cycle(args, second)
    assert first_events == []
    assert len(second_events) == 1
    assert len(second["orders"]) == 1
    assert third_events == []
    assert third["orders"] == second["orders"]


def test_direct_event_records_monotonic_latency_route_and_f1_f4_terminal(
    tmp_path, monkeypatch
):
    raw = {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "block_ts": 1010.0,
        "received_at_s": time.time(),
        "recv_monotonic_s": time.monotonic() - 0.05,
        "transaction_hash": "0xdirect",
        "log_index": 8,
        "maker": WALLET,
        "decoded": {
            "decode_status": "OK",
            "asset": TOKEN_UP,
            "price": 0.4,
            "size": 10,
            "maker": WALLET,
            "maker_side": "BUY",
        },
    }
    alpha_path = tmp_path / "alpha.json"
    alpha_path.write_text(
        json.dumps(
            {
                "execution_profiles": {
                    "profiles_by_wallet": {
                        WALLET: {
                            "eligible": True,
                            "move_slices": [
                                {
                                    "move_slice_key": "060-120|0.25-0.50",
                                    "mean_edge": 0.1,
                                    "median_edge": 0.1,
                                    "copyable_rate_pct": 100,
                                }
                            ],
                        }
                    }
                }
            }
        )
    )
    monkeypatch.setattr(
        subject,
        "_metadata_for_events",
        lambda *_: {
            TOKEN_UP: {
                "market_slug": "btc-updown-5m-900",
                "condition_id": CONDITION,
                "outcome": "Up",
            }
        },
    )

    class FakeClob:
        def __init__(self, **_):
            pass

        def get_book(self, _):
            return {
                "asset_id": TOKEN_UP,
                "timestamp": 1011100,
                "hash": "book",
                "__walletCopyClobRouteReport": {"request_fingerprint": "route-123"},
                "asks": [{"price": "0.4", "size": "10"}],
                "bids": [{"price": "0.39", "size": "10"}],
            }

        @staticmethod
        def summarize_book(*_, **__):
            return {
                "instant_fill_status": "PASS",
                "fill_ratio": 1.0,
                "fillable_shares": 2.5,
                "avg_fill_price": 0.4,
                "fillable_usd": 1.0,
            }

    monkeypatch.setattr(subject, "CLOBMarketClient", FakeClob)
    instrumentation_path = tmp_path / "f3-events.jsonl"
    args = Namespace(
        wallet=[WALLET],
        manifest="",
        cohort_id="cohort",
        run_id="run",
        polygon_jsonl="unused.jsonl",
        direct_event_json=json.dumps(raw),
        scan_limit=10,
        history_state="history",
        gamma_base_url="gamma",
        alpha_report=str(alpha_path),
        clob_base_url="clob",
        clob_timeout_s=1,
        resolutions=str(tmp_path / "resolutions.jsonl"),
        f3_instrumentation_jsonl=str(instrumentation_path),
        f3_instrumentation_run_prefix="run",
    )

    state, events = subject.score_cycle(
        args,
        {
            "cohort": {
                "cohort_id": "cohort",
                "run_id": "run",
                "run_ids": ["run"],
                "started_at_s": 0.0,
            }
        },
    )

    terminal = state["attempt_terminals"][0]
    persisted = [
        json.loads(line) for line in instrumentation_path.read_text().splitlines()
    ]
    assert persisted == [terminal]
    assert terminal["receipt_to_fetch_ms"] <= 5000
    assert terminal["book_fetch_ms"] >= 0
    assert terminal["route_fingerprint"] == "route-123"
    assert terminal["fetch_instrumentation_schema_version"] == 4
    assert terminal["fetch_provenance"] == "reconcile_batch_fetched"
    assert terminal["fetch_pairing_check"] is None
    assert terminal["capture_to_decision_ms"] is not None
    assert terminal["f1_f4_terminal"] == {
        "F1_metadata": "PASS",
        "F2_alpha_profile": "PASS",
        "F3_receipt_freshness": "PASS",
        "F4_executable_book": "PASS",
        "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
    }
    assert state["terminal_reconciliation"]["input_equals_terminal"] is True
    assert [row["event"] for row in events] == [
        "wide_exact_policy_paper_order_filled",
        "wide_exact_policy_attempt_terminal",
    ]


def test_two_capture_cycles_same_token_keep_row_own_prefetch(tmp_path, monkeypatch):
    alpha_path = tmp_path / "alpha.json"
    alpha_path.write_text(json.dumps({"execution_profiles": {"profiles_by_wallet": {
        WALLET: {"eligible": True, "move_slices": [{
            "move_slice_key": "060-120|0.25-0.50", "mean_edge": 0.1,
            "median_edge": 0.1, "copyable_rate_pct": 100,
        }]}
    }}}))
    monkeypatch.setattr(subject, "_metadata_for_events", lambda *_: {
        TOKEN_UP: {"market_slug": "btc-updown-5m-900", "condition_id": CONDITION, "outcome": "Up"}
    })

    class FakeClob:
        def __init__(self, **_): pass
        def get_book(self, _): raise AssertionError("row-own prefetch must win")
        @staticmethod
        def summarize_book(*_, **__):
            return {"instant_fill_status": "PASS", "fill_ratio": 1.0,
                    "fillable_shares": 2.5, "avg_fill_price": 0.4,
                    "fillable_usd": 1.0}

    monkeypatch.setattr(subject, "CLOBMarketClient", FakeClob)
    rows = []
    for index, base in enumerate((10.0, 20.0), start=1):
        rows.append({
            "event": "polygon_orderfilled_log", "source": "polygon_ws",
            "block_ts": 1010.0, "received_at_s": time.time(),
            "recv_monotonic_s": base,
            "fanout_received_monotonic_s": base + 0.005,
            "transaction_hash": "0x" + str(index) * 64, "log_index": index,
            "maker": WALLET,
            "decoded": {"decode_status": "OK", "asset": TOKEN_UP,
                        "price": 0.4, "size": 10, "maker": WALLET,
                        "maker_side": "BUY"},
            "_direct_book_prefetch": {
                "book": {"asset_id": TOKEN_UP, "timestamp": 1011100,
                         "hash": f"book-{index}",
                         "asks": [{"price": "0.4", "size": "10"}],
                         "bids": [{"price": "0.39", "size": "10"}]},
                "fetch_started_monotonic_s": base + 0.010,
                "fetch_cycle_id": f"cycle-{index}",
                "fetch_provenance": "capture_prefetched",
                "prefetch_queue_wait_ms": 5.0, "book_fetch_ms": 1.0,
                "error": None,
            },
        })
    args = Namespace(
        wallet=[WALLET], manifest="", cohort_id="cohort", run_id="run",
        polygon_jsonl="unused", direct_event_json=json.dumps(rows), scan_limit=10,
        history_state="history", gamma_base_url="gamma", alpha_report=str(alpha_path),
        clob_base_url="clob", clob_timeout_s=1,
        resolutions=str(tmp_path / "resolutions.jsonl"),
    )

    state, _events = subject.score_cycle(args, {
        "cohort": {"cohort_id": "cohort", "run_id": "run",
                   "run_ids": ["run"], "started_at_s": 0.0}
    })
    terminals = state["attempt_terminals"]
    assert [row["fetch_cycle_id"] for row in terminals] == ["cycle-1", "cycle-2"]
    assert [row["token_event_ordinal_in_cycle"] for row in terminals] == [0, 0]
    assert all(row["fetch_pairing_check"] == "PASS" for row in terminals)
    assert all(row["receipt_to_fetch_ms"] < 5000 for row in terminals)
    assert all((row["f1_f4_terminal"] or {})["terminal"] == "COPYABLE_EXACT_POLICY_PAPER_FILL" for row in terminals)


def test_metadata_cache_unions_history_without_overwriting_cached_asset(tmp_path, monkeypatch):
    cache = tmp_path / "metadata.json"
    cache.write_text(
        json.dumps(
            {
                TOKEN_UP: {
                    "market_slug": "btc-updown-5m-900",
                    "condition_id": CONDITION,
                    "outcome": "Up",
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(subject, "_token_metadata", lambda *_: {
        TOKEN_UP: {"market_slug": "wrong", "outcome": "Down"},
        TOKEN_DOWN: {
            "market_slug": "btc-updown-5m-900",
            "condition_id": CONDITION,
            "outcome": "Down",
        },
    })
    monkeypatch.setattr(
        subject,
        "_gamma_token_metadata",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("known token should not refetch")
        ),
    )

    meta = subject._metadata_for_events(
        "large-history.json",
        "gamma",
        [
            {
                "block_ts": 1010,
                "decoded": {"decode_status": "OK", "asset": TOKEN_UP},
            }
        ],
        str(cache),
    )

    assert meta[TOKEN_UP]["outcome"] == "Up"
    assert meta[TOKEN_DOWN] == {
        "market_slug": "btc-updown-5m-900",
        "condition_id": CONDITION,
        "outcome": "Down",
        "token_mapping_source": "rtds_source_event_derived",
    }
    persisted = json.loads(cache.read_text(encoding="utf-8"))
    assert persisted == meta
