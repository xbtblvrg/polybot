import json

from scripts.run_active_member_orderfilled_hot_source_shadow import (
    attach_forward_signal_books,
    build_forward_book_summary,
    decision_time_book_signal_row,
    raw_forward_book_candidate_rows,
    _early_01a_watch_wallets,
    _otherwise_qualified_wallets,
    _prospective_wallets,
    _read_jsonl_incremental,
    build_shadow,
)


def test_hot_source_shadow_dedupes_and_resolves_active_member_event() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    token = "12345"
    tx = "0xabc"
    polygon = {
        "event": "polygon_orderfilled_log",
        "selected_wallet": wallet,
        "maker": wallet,
        "taker": "0x2222222222222222222222222222222222222222",
        "transaction_hash": tx,
        "log_index": 7,
        "received_at_s": 1_800_000_001.0,
        "block_ts": 1_800_000_000.0,
        "decoded": {"side": "BUY", "asset": token, "price": 0.49, "size": 2.0},
    }
    guard = {
        "active_set": {
            "members": [{"source_wallet": wallet, "enabled": True}],
        }
    }
    history = {
        "events": [
            {
                "source_wallet": wallet,
                "transaction_hash": tx,
                "token_id": token,
                "condition_id": "0xcondition",
                "market_slug": "btc-updown-5m-1800000000",
                "outcome": "Up",
                "observed_ts": 1_800_000_004.0,
            }
        ]
    }

    result = build_shadow(
        polygon_rows=[polygon, dict(polygon)],
        guard=guard,
        history=history,
        now_ts=1_800_000_100.0,
    )

    assert result["paper_only"] is True
    assert result["live_orders_allowed"] is False
    assert result["unique_resolved_source_events"] == 1
    assert result["duplicate_rows"] == 1
    assert result["current_or_next_window_events"] == 1
    assert result["identity_market_outcome_parity_violations"] == 0
    assert result["detection_lead_s"]["p50"] == 3.0
    assert result["live_source_wiring_gate_passed"] is False


def test_hot_source_shadow_uses_transaction_hash_and_log_index_identity() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    base = {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "selected_wallet": wallet,
        "maker": wallet,
        "taker": "0x2222222222222222222222222222222222222222",
        "transaction_hash": "0xsame",
        "received_at_s": 1_800_000_001.0,
        "block_ts": 1_800_000_000.0,
        "decoded": {"side": "BUY", "asset": "12345", "price": 0.49, "size": 2.0},
    }
    guard = {"active_set": {"members": [{"source_wallet": wallet, "enabled": True}]}}
    token_meta = {
        "12345": {
            "condition_id": "0xcondition",
            "market_slug": "btc-updown-5m-1800000000",
            "outcome": "Up",
        }
    }

    result = build_shadow(
        polygon_rows=[
            {**base, "log_index": 7},
            {**base, "log_index": 8},
            {**base, "log_index": 7},
        ],
        guard=guard,
        history={},
        now_ts=1_800_000_100.0,
        token_meta_seed=token_meta,
    )

    assert result["unique_resolved_source_events"] == 2
    assert result["duplicate_rows"] == 1
    assert {row["identity"] for row in result["sample_rows"]} == {"0xsame|7", "0xsame|8"}


def test_incremental_reader_only_returns_appended_rows(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(json.dumps({"n": 1}) + "\n", encoding="utf-8")

    first, offset, reset = _read_jsonl_incremental(
        path,
        byte_offset=0,
        bootstrap_tail_bytes=1024,
    )
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"n": 2}) + "\n")
    second, next_offset, reset_again = _read_jsonl_incremental(
        path,
        byte_offset=offset,
        bootstrap_tail_bytes=1024,
    )

    assert first == [{"n": 1}]
    assert second == [{"n": 2}]
    assert next_offset == path.stat().st_size
    assert reset is False
    assert reset_again is False


def test_incremental_reader_filters_non_active_wallets_while_streaming(tmp_path) -> None:
    active = "0x1111111111111111111111111111111111111111"
    path = tmp_path / "events.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"selected_wallet": "0x" + "2" * 40, "n": 1}),
                json.dumps({"selected_wallet": active, "n": 2}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    rows, _, _ = _read_jsonl_incremental(
        path,
        byte_offset=0,
        bootstrap_tail_bytes=1024,
        active_wallets={active},
    )

    assert rows == [{"selected_wallet": active, "n": 2}]


def _unmapped_row(wallet: str) -> dict[str, object]:
    return {
        "event": "polygon_orderfilled_log",
        "selected_wallet": wallet,
        "maker": wallet,
        "taker": "0x2222222222222222222222222222222222222222",
        "transaction_hash": "0xother",
        "log_index": 9,
        "received_at_s": 1_800_000_001.0,
        "block_ts": 1_800_000_000.0,
        "decoded": {"side": "BUY", "asset": "non-btc-token", "price": 0.2, "size": 1.0},
    }


def test_unmapped_token_in_resolved_window_is_out_of_scope_not_gate_failure() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    result = build_shadow(
        polygon_rows=[_unmapped_row(wallet)],
        guard={"active_set": {"members": [{"source_wallet": wallet, "enabled": True}]}},
        history={},
        now_ts=1_800_000_100.0,
        # The window itself resolved: we hold both of its BTC-5m outcome tokens,
        # so a third token in that window is genuinely another market.
        token_meta_seed={
            "btc-up-token": {
                "market_slug": "btc-updown-5m-1800000000",
                "condition_id": "0xcond",
                "outcome": "Up",
            }
        },
    )

    assert result["token_mapping_missing"] == 0
    assert result["unmapped_out_of_scope_rows"] == 1
    assert result["unmapped_unresolved_window_rows"] == 0
    assert result["identity_market_outcome_parity_violations"] == 0


def test_unmapped_token_in_unresolved_window_is_not_claimed_out_of_scope() -> None:
    # Regression for the defect that pinned the OrderFilled stakeout at
    # unique_resolved_source_events=0 for 4766 iterations: with no resolved
    # window map, an unmapped token's scope is UNKNOWN. Counting it as
    # out-of-lane renders a broken token map as "no BTC-5m supply".
    wallet = "0x1111111111111111111111111111111111111111"
    result = build_shadow(
        polygon_rows=[_unmapped_row(wallet)],
        guard={"active_set": {"members": [{"source_wallet": wallet, "enabled": True}]}},
        history={},
        now_ts=1_800_000_100.0,
    )

    assert result["token_mapping_missing"] == 0
    assert result["unmapped_out_of_scope_rows"] == 0
    assert result["unmapped_unresolved_window_rows"] == 1
    assert result["identity_market_outcome_parity_violations"] == 0


def test_forward_book_capture_is_written_on_new_early_01a_signal_row() -> None:
    row = {
        "identity": "0xabc|7",
        "token_id": "token",
        "market_slug": "btc-updown-5m-1800000000",
        "price": 0.30,
        "event_ts": 1_800_000_020.0,
        "received_at_s": 1_800_000_021.0,
    }
    cache = {}
    stats = attach_forward_signal_books(
        [row],
        new_identities={"0xabc|7"},
        snapshot_cache=cache,
        book_fetcher=lambda _token: {
            "bids": [{"price": "0.29", "size": "3"}],
            "asks": [{"price": "0.31", "size": "2"}],
        },
        now_ts=1_800_000_022.0,
    )
    assert stats["captured"] == 1
    assert row["signal_detection_book"]["capture_mode"] == "forward_at_signal_detection"
    assert row["signal_detection_book"]["best_bid"] == 0.29
    assert row["signal_detection_book"]["best_ask"] == 0.31
    assert cache["0xabc|7"]["spread"] == 0.02


def test_raw_forward_book_candidate_is_ready_before_batch_enrichment() -> None:
    wallet = "0x" + "a" * 40
    rows = raw_forward_book_candidate_rows(
        [
            {
                "transaction_hash": "0xabc",
                "log_index": 7,
                "selected_wallet": wallet,
                "event_ts": 1_800_000_020.0,
                "received_at_s": 1_800_000_020.2,
                "source": "polygon_ws",
                "decoded": {"side": "BUY", "asset": "token", "price": 0.30},
            }
        ],
        token_meta_cache={"token": {"market_slug": "btc-updown-5m-1800000000"}},
        watch_wallets={wallet},
    )

    assert rows == [
        {
            "identity": "0xabc|7",
            "source_wallet": wallet,
            "token_id": "token",
            "market_slug": "btc-updown-5m-1800000000",
            "price": 0.30,
            "event_ts": 1_800_000_020.0,
            "received_at_s": 1_800_000_020.2,
            "source": "polygon_ws",
            "forward_book_roster_rule": "top_8_by_qualifying_window_count_exchange_filtered_wallet_lexicographic_tiebreak",
        }
    ]


def test_forward_book_capture_never_backfills_old_or_non_01a_rows() -> None:
    rows = [
        {
            "identity": "old",
            "token_id": "token",
            "market_slug": "btc-updown-5m-1800000000",
            "price": 0.30,
            "event_ts": 1_800_000_020.0,
            "received_at_s": 1_800_000_021.0,
        },
        {
            "identity": "high",
            "token_id": "token",
            "market_slug": "btc-updown-5m-1800000000",
            "price": 0.40,
            "event_ts": 1_800_000_020.0,
            "received_at_s": 1_800_000_099.0,
        },
    ]
    stats = attach_forward_signal_books(
        rows,
        new_identities={"old", "high"},
        snapshot_cache={},
        book_fetcher=lambda _token: (_ for _ in ()).throw(AssertionError("must not fetch")),
        now_ts=1_800_000_100.0,
        max_detection_age_s=60.0,
    )
    assert stats["eligible"] == 0
    assert all("signal_detection_book" not in row for row in rows)


def test_forward_book_output_schema_and_summary_need_no_archive_join() -> None:
    signal = {
        "identity": "0xabc|7",
        "source_wallet": "0x" + "a" * 40,
        "token_id": "token",
        "market_slug": "btc-updown-5m-1800000000",
        "price": 0.30,
        "event_ts": 1_800_000_020.0,
        "signal_detection_book": {
            "status": "OK",
            "signal_received_at_s": 1_800_000_020.0,
            "capture_started_at_s": 1_800_000_021.0,
            "captured_at_s": 1_800_000_021.4,
            "best_bid": 0.29,
            "best_ask": 0.31,
        },
    }
    output = decision_time_book_signal_row(signal)
    assert output is not None
    assert output["capture_source"] == "decision_time_forward"
    assert output["book_lag_s"] == 1.4
    assert output["book_fetch_rtt_s"] == 0.4
    assert output["book_lag_s"] != output["book_fetch_rtt_s"]
    assert output["executable"] is True
    assert {"signal_detected_at", "trade_ts", "wallet", "asset_id", "slug", "best_bid", "best_ask"} <= output.keys()
    summary = build_forward_book_summary([output], generated_at="now")
    assert summary["status"] == "ACCRUING_FORWARD_BOOK"
    assert summary["signal_count"] == 1
    assert summary["executable_fraction"] == 1.0
    assert summary["best_ask"]["mean"] == 0.31


def test_forward_book_output_fails_closed_when_detection_lag_exceeds_two_seconds() -> None:
    output = decision_time_book_signal_row(
        {
            "identity": "late",
            "token_id": "token",
            "market_slug": "btc-updown-5m-1800000000",
            "price": 0.30,
            "signal_detection_book": {
                "status": "OK",
                "signal_received_at_s": 1_800_000_020.0,
                "capture_started_at_s": 1_800_000_023.0,
                "captured_at_s": 1_800_000_023.1,
                "best_bid": 0.29,
                "best_ask": 0.30,
            },
        }
    )

    assert output is not None
    assert output["book_status"] == "STALE_DETECTION"
    assert output["executable"] is False


def test_early_01a_watch_roster_is_ranked_and_exchange_filtered(tmp_path) -> None:
    path = tmp_path / "candidates.json"
    path.write_text(
        json.dumps(
            {
                "candidates": [
                    {"wallet": "0x" + "b" * 40, "qualifying_window_count": 8},
                    {"wallet": "0x" + "a" * 40, "qualifying_window_count": 8},
                    {"wallet": "0xe111180000d2663c0091e4f400237545b87b996b", "qualifying_window_count": 99},
                    {"wallet": "0x" + "c" * 40, "qualifying_window_count": 7},
                ]
            }
        ),
        encoding="utf-8",
    )
    assert _early_01a_watch_wallets(path, limit=2) == {"0x" + "a" * 40, "0x" + "b" * 40}


def test_prospective_wallet_counts_only_current_market_buy_in_trailing_30m() -> None:
    wallet = "0x805a8bbd411324a1c121dd87fac96c2fb13012c8"
    token = "12345"
    now_ts = 1_800_000_400.0
    base = {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "selected_wallet": wallet,
        "maker": wallet,
        "taker": "0x2222222222222222222222222222222222222222",
        "received_at_s": 1_800_000_100.0,
        "block_ts": 1_800_000_099.0,
        "decoded": {"side": "BUY", "asset": token, "price": 0.49, "size": 2.0},
    }
    token_meta = {
        token: {
            "condition_id": "0xcondition",
            "market_slug": "btc-updown-5m-1800000000",
            "outcome": "Up",
        }
    }

    result = build_shadow(
        polygon_rows=[
            {**base, "transaction_hash": "0xcurrent", "log_index": 1},
            {
                **base,
                "transaction_hash": "0xbackfill",
                "log_index": 2,
                "received_at_s": 1_800_000_301.0,
            },
        ],
        guard={"active_set": {"members": []}},
        history={},
        now_ts=now_ts,
        token_meta_seed=token_meta,
        prospective_wallets={wallet},
        prospective_roster=[
            {
                "wallet": wallet,
                "wide_policy_fingerprint": "abc123",
                "regime_evidence": {
                    "first_half_post_fee_pnl_usd": 4.0,
                    "second_half_post_fee_pnl_usd": 6.0,
                    "resolved_signals": 250,
                    "source": "venue_executable_full_stream_rescore",
                },
            }
        ],
        prior_state={
            "prospective_current_market": {
                "actuator_consumption_gate": {
                    "order143_measurement_started_at_s": 1_800_000_000.0,
                    "order144_code_resident_started_at_s": 1_800_000_000.0,
                },
                "identity_clean_events": [],
            }
        },
    )

    prospective = result["prospective_current_market"]
    assert result["unique_resolved_source_events"] == 0
    assert prospective["genuine_buy_identities"] == 1
    assert prospective["terminal_counts"] == {
        "f2_current_market_buy_30m": 1,
        "closed_window_or_backfill_f2_zero": 1,
    }
    assert prospective["reconciled"] is True
    assert prospective["identity_clean_events"][0]["event_id"] == "0xcurrent|1"
    assert prospective["identity_clean_events"][0]["proxy_to_wallet"]["resolved_source_wallet"] == wallet
    assert prospective["actuator_consumption_gate"]["passed"] is False
    assert prospective["actuator_consumption_gate"]["max_current_market_buy_count"] == 1
    assert prospective["actuator_consumption_gate"]["receipt_to_f2_p50_s"] == 1.0
    assert prospective["actuator_consumption_gate"]["receipt_to_f2_p95_s"] == 1.0
    assert prospective["actuator_consumption_gate"]["receipt_to_f2_samples"] == 1
    assert prospective["actuator_consumption_gate"]["push_block_to_receipt"]["samples"] == 1
    assert prospective["actuator_consumption_gate"]["batch_block_to_receipt"]["samples"] == 0
    assert prospective["actuator_consumption_gate"]["signed_block_to_receipt"][
        "negative_sample_fraction"
    ] == 0.0
    assert prospective["actuator_consumption_gate"]["receipt_to_f2_admission_p95_s"] == 300.0
    assert prospective["actuator_consumption_gate"]["receipt_to_f2_admission_samples"] == 1
    assert prospective["actuator_consumption_gate"]["admission_stamp_clusters"] == {
        "samples": 1,
        "distinct_stamps": 1,
        "mean_cluster_size": 1,
        "max_cluster_size": 1,
        "iteration_period_p50_s": None,
    }
    assert prospective["actuator_consumption_gate"][
        "positive_exact_policy_chronological_holdout"
    ] is True
    assert prospective["actuator_consumption_gate"][
        "exact_policy_chronological_holdout_by_wallet"
    ][wallet]["abc123"]["passed"] is True
    sub_gates = prospective["actuator_consumption_gate"]["sub_gate_measurements"]
    assert sub_gates["receipt_to_f2_p95_s"] == {
        "value": 1.0,
        "threshold_lt": 2.0,
        "status": "measured",
        "green": True,
    }
    assert sub_gates["push_only_receipt_to_f2_p95_s"] == {
        "value": 1.0,
        "status": "diagnostic_only_not_a_gate_conjunct",
    }
    assert sub_gates["exact_policy_chronological_holdout"]["green"] is True
    assert result["live_source_wiring_gate_passed"] is False
    assert result["live_orders_allowed"] is False


def test_exact_policy_holdout_aggregate_is_unknown_when_candidate_lacks_cell() -> None:
    covered = "0x" + "a" * 40
    missing = "0x" + "b" * 40
    result = build_shadow(
        polygon_rows=[],
        guard={"active_set": {"members": []}},
        history={},
        now_ts=1_800_000_100.0,
        prospective_wallets={covered, missing},
        prospective_roster=[{
            "wallet": covered,
            "wide_policy_fingerprint": "fp-covered",
            "regime_evidence": {
                "first_half_post_fee_pnl_usd": 1.0,
                "second_half_post_fee_pnl_usd": 1.0,
            },
        }],
    )

    gate = result["prospective_current_market"]["actuator_consumption_gate"]
    assert gate["positive_exact_policy_chronological_holdout"] == "UNKNOWN"
    assert gate["exact_policy_holdout_candidate_denominator"] == 2
    assert gate["exact_policy_holdout_covered_wallets"] == 1
    assert gate["exact_policy_holdout_missing_wallets"] == [missing]
    assert gate["sub_gate_measurements"]["exact_policy_chronological_holdout"]["green"] is False


def test_prospective_wallet_config_is_separate_and_validated(tmp_path) -> None:
    valid = "0x805a8bbd411324a1c121dd87fac96c2fb13012c8"
    path = tmp_path / "prospective.json"
    path.write_text(
        json.dumps({"wallets": [{"wallet": valid}, {"wallet": "not-an-address"}]}),
        encoding="utf-8",
    )

    assert _prospective_wallets(path) == {valid}


def test_otherwise_qualified_roster_excludes_only_f2() -> None:
    wallet = "0x805a8bbd411324a1c121dd87fac96c2fb13012c8"
    checks = {
        "f1_measured_positive_regime_cell": True,
        "f2_fresh_rows_and_own_policy_copyable": False,
        "f3_not_enabled_or_cooloff_or_fading": True,
        "f4_external_liveness": True,
        "own_evidenced_policy_available": True,
        "active_temporal_not_proven_negative": True,
    }
    state = {
        "policy_choke": {
            "actuator": {
                "candidate_evidence": {
                    "rows": [
                        {
                            "wallet": wallet,
                            "candidate_id": "candidate",
                            "wide_policy_fingerprint": "fp",
                            "regime_evidence": {
                                "first_half_post_fee_pnl_usd": 1.0,
                                "second_half_post_fee_pnl_usd": 2.0,
                            },
                            "checks": checks,
                        }
                    ]
                }
            }
        }
    }

    wallets, roster = _otherwise_qualified_wallets(state)

    assert wallets == {wallet}
    assert roster[0]["excluded_check"] == "f2_fresh_rows_and_own_policy_copyable"
    assert roster[0]["wide_policy_fingerprint"] == "fp"
    assert roster[0]["regime_evidence"]["second_half_post_fee_pnl_usd"] == 2.0


def test_otherwise_qualified_roster_retains_distinct_fingerprints_for_same_wallet() -> None:
    wallet = "0x805a8bbd411324a1c121dd87fac96c2fb13012c8"
    checks = {
        "f1_measured_positive_regime_cell": True,
        "f2_fresh_rows_and_own_policy_copyable": False,
        "f3_not_enabled_or_cooloff_or_fading": True,
    }
    rows = [
        {"wallet": wallet, "wide_policy_fingerprint": fingerprint, "checks": checks}
        for fingerprint in ("fingerprint-a", "fingerprint-b")
    ]

    wallets, roster = _otherwise_qualified_wallets(
        {"policy_choke": {"actuator": {"candidate_evidence": {"rows": rows}}}}
    )

    assert wallets == {wallet}
    assert {row["wide_policy_fingerprint"] for row in roster} == {
        "fingerprint-a",
        "fingerprint-b",
    }


def test_order143_separates_push_batch_and_signed_latency() -> None:
    wallet = "0x805a8bbd411324a1c121dd87fac96c2fb13012c8"
    token = "12345"
    token_meta = {
        token: {
            "condition_id": "0xcondition",
            "market_slug": "btc-updown-5m-1800000000",
            "outcome": "Up",
        }
    }
    base = {
        "event": "polygon_orderfilled_log",
        "selected_wallet": wallet,
        "maker": wallet,
        "taker": "0x2222222222222222222222222222222222222222",
        "decoded": {"side": "BUY", "asset": token, "price": 0.49, "size": 2.0},
    }

    result = build_shadow(
        polygon_rows=[
            {
                **base,
                "source": "polygon_ws",
                "transaction_hash": "0xpush",
                "log_index": 1,
                "received_at_s": 1_800_000_001.0,
                "block_ts": 1_800_000_000.0,
            },
            {
                **base,
                "source": "polygon_http_getLogs_tail",
                "http_capture_kind": "eth_getLogs",
                "transaction_hash": "0xbatch",
                "log_index": 2,
                "received_at_s": 1_800_000_020.0,
                "block_ts": 1_800_000_005.0,
            },
            {
                **base,
                "source": "polygon_ws",
                "transaction_hash": "0xnegative",
                "log_index": 3,
                "received_at_s": 1_800_000_010.0,
                "block_ts": 1_800_000_011.0,
            },
        ],
        guard={"active_set": {"members": []}},
        history={},
        now_ts=1_800_000_100.0,
        token_meta_seed=token_meta,
        prospective_wallets={wallet},
        prospective_roster=[
            {
                "wallet": wallet,
                "wide_policy_fingerprint": "abc123",
                "regime_evidence": {
                    "first_half_post_fee_pnl_usd": 4.0,
                    "second_half_post_fee_pnl_usd": 6.0,
                    "resolved_signals": 250,
                },
            }
        ],
        prior_state={
            "prospective_current_market": {
                "actuator_consumption_gate": {
                    "order143_measurement_started_at_s": 1_800_000_000.0,
                    "order144_code_resident_started_at_s": 1_800_000_000.0,
                },
                "identity_clean_events": [],
            }
        },
    )

    gate = result["prospective_current_market"]["actuator_consumption_gate"]
    events = result["prospective_current_market"]["identity_clean_events"]
    assert gate["passed"] is False
    assert gate["receipt_to_f2_samples"] == 2
    assert gate["receipt_to_f2_p95_s"] == 1.0
    assert gate["push_block_to_receipt"]["samples"] == 2
    assert gate["batch_block_to_receipt"] == {"samples": 1, "p50_s": 15.0, "p95_s": 15.0}
    assert gate["signed_block_to_receipt"]["min_s"] == -1.0
    assert gate["signed_block_to_receipt"]["negative_sample_fraction"] == 1 / 3
    assert gate["receipt_to_f2_admission_samples"] == 3
    assert gate["receipt_to_f2_admission_status"] == "diagnostic_only_not_a_gate_conjunct"
    assert gate["admission_stamp_clusters"] == {
        "samples": 3,
        "distinct_stamps": 1,
        "mean_cluster_size": 3,
        "max_cluster_size": 3,
        "iteration_period_p50_s": None,
    }
    assert gate["sub_gate_measurements"]["receipt_to_f2_admission_p95_s"] == {
        "value": 99.0,
        "status": "diagnostic_only_not_a_gate_conjunct",
    }
    assert gate["sub_gate_measurements"]["receipt_to_f2_p95_s"] == {
        "value": 15.0,
        "threshold_lt": 2.0,
        "status": "measured",
        "green": False,
    }
    assert gate["sub_gate_measurements"]["push_only_receipt_to_f2_p95_s"] == {
        "value": 1.0,
        "status": "diagnostic_only_not_a_gate_conjunct",
    }
    assert events[0]["source"] == "polygon_ws"
    assert events[1]["http_capture_kind"] == "eth_getLogs"
    assert events[1]["transport"] == "batch"


def test_order145_publishes_sampler_recall_candidate_coverage_and_union_latency() -> None:
    wallet = "0x00000000000000000000000000000000000000aa"
    base = {
        "event": "polygon_orderfilled_log",
        "selected_wallet": wallet,
        "maker": wallet,
        "taker": "0x00000000000000000000000000000000000000bb",
        "decoded": {"side": "BUY", "asset": "1", "price": 0.4, "size": 2.0},
        "block_ts": 1_800_000_010.0,
    }
    rows = [
        {**base, "source": "polygon_ws", "transaction_hash": "0xboth", "log_index": 1, "received_at_s": 1_800_000_010.5},
        {**base, "source": "polygon_http_getLogs_tail", "http_capture_kind": "tail", "transaction_hash": "0xboth", "log_index": 1, "received_at_s": 1_800_000_011.0},
        {**base, "source": "polygon_http_getLogs_tail", "http_capture_kind": "tail", "transaction_hash": "0xbatch", "log_index": 2, "received_at_s": 1_800_000_011.5},
    ]
    result = build_shadow(
        polygon_rows=rows,
        guard={"active_set": {"members": []}},
        history={},
        now_ts=1_800_000_100.0,
        token_meta_seed={"1": {"market_slug": "btc-updown-5m-1800000000", "condition_id": "c", "outcome": "Up"}},
        prospective_wallets={wallet},
        prospective_roster=[],
        prior_state={"prospective_current_market": {"actuator_consumption_gate": {"order144_code_resident_started_at_s": 1_800_000_000.0}}},
    )
    gate = result["prospective_current_market"]["actuator_consumption_gate"]
    assert gate["passed"] is False
    assert gate["push_recall_vs_batch"] == {
        "common_event_ts_start_s": 1_800_000_010.0,
        "common_event_ts_end_s": 1_800_000_010.0,
        "common_event_ts_window_s": 0.0,
        "push": 1,
        "batch": 2,
        "both": 1,
        "push_only": 0,
        "batch_only": 1,
        "push_recall_fraction": 0.5,
    }
    assert gate["candidate_push_coverage"]["push_rows_for_candidate"] == 1
    assert gate["candidate_push_coverage"]["row_counts_by_transport"]["batch"] == 2
    assert gate["candidate_push_coverage"]["status"] == "measured"
    assert gate["union_earliest_receipt_block_to_receipt"] == {"samples": 2, "p50_s": 0.5, "p95_s": 1.5}
    assert gate["sub_gate_measurements"]["receipt_to_f2_p95_s"] == {
        "value": 1.5,
        "threshold_lt": 2.0,
        "status": "measured",
        "green": True,
    }
    assert gate["sub_gate_measurements"]["push_only_receipt_to_f2_p95_s"] == {
        "value": 0.5,
        "status": "diagnostic_only_not_a_gate_conjunct",
    }
