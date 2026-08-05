from scripts.report_order146_f2_own_policy_replay_audit import build_report


def _deadman(wallet: str) -> dict:
    return {"policy_choke": {"actuator": {"candidate_evidence": {"nearest_frontier": [{
        "wallet": wallet,
        "wide_policy_fingerprint": "fp",
        "f2_copyable_policy_id": "base",
        "policy": {"move_slice_keys": ["000-060|0.25-0.50"]},
    }]}}}}


def test_order146_replay_measures_exact_policy_and_book() -> None:
    wallet = "0x" + "1" * 40
    token = "asset"
    envelopes = [{"identity": {"run_id": "run"}, "rows": [{
        "wallet": wallet,
        "attempt_id": "a1",
        "token_id": token,
        "transaction_hash": "0xtx",
        "source_event_id": "7",
        "f1_f4_terminal": {"terminal": "REFUSED_ALPHA_PROFILE_FILTER"},
    }]}]
    source = {
        ("0xtx", 7): {
            "event_ts": 1_700_000_010,
            "received_at_s": 1_700_000_011,
            "decoded": {"asset": token, "price": 0.4},
        }
    }
    books = {token: [(1_700_000_010, {
        "asset_id": token,
        "asks": [{"price": 0.4, "size": 10}],
        "bids": [{"price": 0.39, "size": 10}],
    })]}
    metadata = {token: {
        "condition_id": "c",
        "market_slug": "btc-updown-5m-1700000000",
        "outcome": "Up",
    }}
    report = build_report(
        deadman=_deadman(wallet),
        envelopes=envelopes,
        metadata=metadata,
        source_events=source,
        book_snapshots=books,
        identities=((wallet, "fp"),),
    )
    assert report["status"] == "OWN_POLICY_COPYABLE_FOUND"
    assert report["verdict"] is True
    assert report["candidates"][0]["own_policy_replay_copyables"] == 1
    assert report["candidates"][0]["observed_own_policy_copyables"] == 1
    assert report["candidates"][0]["book_join_age_s_max"] == 1.0
    assert report["candidates"][0]["book_coverage_own_policy_pass"] == 1.0


def test_order146_positive_lower_bound_is_decisive_despite_missing_rows() -> None:
    wallet = "0x" + "1" * 40
    token = "asset"
    envelopes = [{"rows": [
        {"wallet": wallet, "attempt_id": "a1", "token_id": token, "transaction_hash": "0xtx", "source_event_id": "7"},
        {"wallet": wallet, "attempt_id": "a2", "token_id": "missing", "transaction_hash": "0xmissing", "source_event_id": "8"},
    ]}]
    report = build_report(
        deadman=_deadman(wallet),
        envelopes=envelopes,
        metadata={token: {"condition_id": "c", "market_slug": "btc-updown-5m-1700000000", "outcome": "Up"}},
        source_events={("0xtx", 7): {"event_ts": 1_700_000_010, "received_at_s": 1_700_000_011, "decoded": {"asset": token, "price": 0.4}}},
        book_snapshots={token: [(1_700_000_010, {"asks": [{"price": 0.4, "size": 10}], "bids": []})]},
        identities=((wallet, "fp"),),
    )
    assert report["status"] == "OWN_POLICY_COPYABLE_FOUND"
    assert report["verdict"] is True
    assert report["candidates"][0]["replay_status"] == "MEASURED_LOWER_BOUND"
    assert report["candidates"][0]["own_policy_replay_copyables"] == 1
    assert report["candidates"][0]["missing_required_fields"] == {"token_metadata_join": 1}


def test_order146_replay_reports_specific_join_rate_failure() -> None:
    wallet = "0x" + "1" * 40
    envelopes = [{"rows": [{
        "wallet": wallet,
        "attempt_id": "a1",
        "token_id": "asset",
        "transaction_hash": "0xtx",
        "source_event_id": "7",
        "f1_f4_terminal": {"terminal": "REFUSED_METADATA_MISSING"},
    }]}]
    report = build_report(
        deadman=_deadman(wallet),
        envelopes=envelopes,
        identities=((wallet, "fp"),),
    )
    assert report["status"] == "JOIN_RATE_VERDICT_FAIL"
    assert report["verdict"] is None
    assert report["stopped_before_policy_replay"] is False
    assert report["candidates"][0]["missing_required_fields"] == {"token_metadata_join": 1}
