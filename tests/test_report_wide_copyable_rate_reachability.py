from scripts.report_wide_copyable_rate_reachability import build_report


WALLET = "0x0000000000000000000000000000000000000001"


def _terminal(name: str, *, token_id: str = "token", event_id: str = "1") -> dict:
    return {
        "wallet": WALLET,
        "transaction_hash": "0xtx",
        "source_event_id": event_id,
        "token_id": token_id,
        "f1_f4_terminal": {"terminal": name},
    }


def _measurement(terminals: list[dict]) -> dict:
    return {
        "manifest": {
            "manifest_id": "manifest",
            "wallet_policy_identities": {
                WALLET: {
                    "move_slice_keys": ["060-120|0.25-0.50"],
                    "wide_policy_fingerprint": "fingerprint",
                }
            },
        },
        "cohort": {"cohort_id": "cohort"},
        "wallets": {
            WALLET: {
                "attempted_exact_policy_buys": 4,
                "copyable_exact_policy_buys": 2,
            }
        },
        "attempt_terminals": terminals,
        "terminal_reconciliation": {
            "input_rows": len(terminals),
            "terminal_rows": len(terminals),
            "input_equals_terminal": True,
        },
    }


def _standings(attempted: int = 4, copyable: int = 2) -> dict:
    return {
        "standings": [
            {
                "wallet": WALLET,
                "attempted_buy_events": attempted,
                "copyable_buy_events": copyable,
            }
        ],
        "summary": {"dual_gate_winners": 0},
        "winner_wallets": [],
    }


def test_reconciles_raw_and_policy_addressable_denominators() -> None:
    terminals = [
        _terminal("COPYABLE_EXACT_POLICY_PAPER_FILL"),
        _terminal("COPYABLE_EXACT_POLICY_PAPER_FILL"),
        _terminal("REFUSED_STALE_RECEIPT_TO_FETCH"),
        _terminal("REFUSED_PRICE_ABOVE_SLIPPAGE_CAP"),
        _terminal("REFUSED_ALPHA_PROFILE_FILTER"),
        _terminal("REFUSED_METADATA_MISSING"),
    ]
    report = build_report(_measurement(terminals), _standings())
    row = report["wallets"][0]

    assert row["raw_input_rows"] == 6
    assert row["as_built"]["attempted_buy_events"] == 4
    assert row["policy_addressable"]["attempted_buy_events"] == 4
    assert row["as_built"]["copyable_rate_pct"] == 50.0
    assert row["policy_addressable"]["copyable_rate_pct"] == 50.0
    assert row["excluded_attempt_taxonomy"] == {
        "out_of_selected_slice": 1,
        "metadata_missing": 1,
        "slippage_cap": 1,
        "no_ask_liquidity": 0,
        "stale_receipt": 1,
    }
    assert all(row["checks"].values())
    assert report["summary"]["decision_branch"] == (
        "RETIRE_WIDE_EXACT_POLICY_AS_NEAR_TERM_MONEY_ROUTE"
    )
    assert report["summary"]["dual_gate_winners_after"] == 0
    assert report["summary"]["winner_wallets_after"] == []
    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False


def test_infrastructure_decomposition_names_missing_fields_and_window_status() -> None:
    terminals = [
        _terminal("REFUSED_METADATA_MISSING", token_id="missing", event_id="1"),
        {
            **_terminal("REFUSED_STALE_RECEIPT_TO_FETCH", token_id="complete", event_id="2"),
            "receipt_to_fetch_ms": 6000.0,
            "book_fetch_ms": 4.0,
        },
    ]
    measurement = _measurement(terminals)
    measurement["wallets"][WALLET].update(
        attempted_exact_policy_buys=1, copyable_exact_policy_buys=0
    )
    source_events = [
        {
            "transaction_hash": "0xtx",
            "log_index": "1",
            "selected_wallet": WALLET,
            "event_ts": 1001,
            "received_at_s": 1002,
        },
        {
            "transaction_hash": "0xtx",
            "log_index": "2",
            "selected_wallet": WALLET,
            "event_ts": 1001,
            "received_at_s": 1002,
        },
    ]
    report = build_report(
        measurement,
        _standings(attempted=1, copyable=0),
        source_events=source_events,
        token_metadata={
            "complete": {
                "market_slug": "btc-updown-5m-900",
                "condition_id": "condition",
                "outcome": "Up",
            }
        },
    )
    split = report["infrastructure_refusal_decomposition"]
    metadata = split["by_refusal_class"]["REFUSED_METADATA_MISSING"]
    stale = split["by_refusal_class"]["REFUSED_STALE_RECEIPT_TO_FETCH"]
    assert metadata["missing_metadata_fields"] == {
        "market_slug": 1,
        "condition_id": 1,
        "outcome": 1,
    }
    assert metadata["metadata_token_status"] == {"TOKEN_ABSENT_FROM_CACHE": 1}
    assert metadata["btc5m_membership"] == {"DEFINITELY_NOT_TIMESTAMP_BTC5M": 1}
    assert stale["btc5m_membership"] == {"CONFIRMED_BTC5M": 1}
    assert stale["market_open_at_observation"] == {"true": 1}
    assert stale["api_latency_s"]["not_applicable_polygon_ws_rows"] == 1
    assert stale["receipt_to_fetch_ms"]["median"] == 6000.0
    assert stale["book_fetch_ms"]["median"] == 4.0


def test_policy_addressable_rate_at_threshold_opens_metric_branch() -> None:
    terminals = [
        *[_terminal("COPYABLE_EXACT_POLICY_PAPER_FILL") for _ in range(7)],
        *[_terminal("REFUSED_PRICE_ABOVE_SLIPPAGE_CAP") for _ in range(3)],
        _terminal("REFUSED_ALPHA_PROFILE_FILTER"),
    ]
    measurement = _measurement(terminals)
    measurement["wallets"][WALLET].update(
        attempted_exact_policy_buys=10,
        copyable_exact_policy_buys=7,
    )
    report = build_report(measurement, _standings(attempted=11, copyable=7))

    assert report["wallets"][0]["policy_addressable"]["copyable_rate_pct"] == 70.0
    assert report["wallets"][0]["as_built"]["copyable_rate_pct"] == 63.636364
    assert report["wallets"][0]["denominator_comparison"]["equal"] is False
    assert report["summary"]["decision_branch"] == "OPEN_DEFECT_P_METRIC_MISDENOMINATED"


def test_paper_prefetch_delay_maps_to_stale_infrastructure_bucket() -> None:
    terminals = [_terminal("REFUSED_PAPER_PREFETCH_DELAY_GT_5S")]
    measurement = _measurement(terminals)
    measurement["wallets"][WALLET].update(
        attempted_exact_policy_buys=1,
        copyable_exact_policy_buys=0,
    )

    report = build_report(measurement, _standings(attempted=1, copyable=0))

    row = report["wallets"][0]
    assert row["excluded_attempt_taxonomy"]["stale_receipt"] == 1
    assert report["infrastructure_refusal_decomposition"]["by_refusal_class"][
        "REFUSED_PAPER_PREFETCH_DELAY_GT_5S"
    ]["rows"] == 1


def test_insufficient_depth_maps_to_slippage_bucket() -> None:
    terminals = [_terminal("REFUSED_INSUFFICIENT_DEPTH_WITHIN_SLIPPAGE_CAP")]
    measurement = _measurement(terminals)
    measurement["wallets"][WALLET].update(
        attempted_exact_policy_buys=1,
        copyable_exact_policy_buys=0,
    )

    report = build_report(measurement, _standings(attempted=1, copyable=0))

    assert report["wallets"][0]["excluded_attempt_taxonomy"]["slippage_cap"] == 1


def test_unknown_terminal_fails_closed() -> None:
    terminals = [
        _terminal("COPYABLE_EXACT_POLICY_PAPER_FILL"),
        _terminal("REFUSED_NEW_UNCLASSIFIED_PATH"),
    ]
    measurement = _measurement(terminals)
    measurement["wallets"][WALLET].update(
        attempted_exact_policy_buys=2,
        copyable_exact_policy_buys=1,
    )

    import pytest

    with pytest.raises(ValueError, match="unknown terminal taxonomy"):
        build_report(measurement, _standings(attempted=2, copyable=1))
