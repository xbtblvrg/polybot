from scripts.report_early_01a_candidate_edge import build_report


WALLET_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
WALLET_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
MAKER = "0xcccccccccccccccccccccccccccccccccccccccc"
EXCHANGE = "0xe111180000d2663c0091e4f400237545b87b996b"


def _row(*, wallet: str, asset: str, start: int, outcome_side: str = "BUY") -> dict:
    return {
        "maker": wallet if outcome_side == "BUY" else MAKER,
        "taker": MAKER if outcome_side == "BUY" else wallet,
        "block_ts": start + 20,
        "decoded": {"side": outcome_side, "maker_side": outcome_side, "asset": asset, "price": 0.26, "size": 1.0},
    }


def test_edge_report_uses_pair_units_resolution_gate_and_quoting_flag() -> None:
    rows = []
    metadata = {}
    resolutions = []
    for index in range(30):
        start = 1_000 + 300 * index
        slug = f"btc-updown-5m-{start}"
        asset = f"asset-{index}"
        metadata[asset] = {
            "market_slug": slug,
            "condition_id": f"c-{index}",
            "outcome": "Up",
            "winning_outcome": "Up" if index < 10 else "Down",
        }
        rows.append(_row(wallet=WALLET_A, asset=asset, start=start))
        # Duplicate fills in a window must not inflate resolved n.
        rows.append(_row(wallet=WALLET_A, asset=asset, start=start))
        if index < 9:
            rows.append(_row(wallet=WALLET_B, asset=asset, start=start, outcome_side="SELL"))
        if index % 2:
            resolutions.append({"market_slug": slug, "direction": "UP" if index < 10 else "DOWN"})

    report = build_report(
        rows,
        token_metadata=metadata,
        resolution_rows=resolutions,
        since_block_ts=0,
        generated_at="now",
    )
    assert report["rung_clearing_qualifying_window_count_threshold"] == 4
    assert report["rung_clearing_candidate_count"] == 2
    assert report["secondary_per_wallet_positive_roi_pass_count"] == 1
    by_wallet = {item["wallet"]: item for item in report["candidates"]}
    assert by_wallet[WALLET_A]["n_resolved"] == 30
    assert by_wallet[WALLET_A]["n_distinct_windows"] == 30
    assert by_wallet[WALLET_A]["realized_01a_win_rate_pct"] == 33.333333
    assert by_wallet[WALLET_A]["grade"] == "PASS_POSITIVE_ROI_ABOVE_OWN_BREAKEVEN"
    assert by_wallet[WALLET_A]["realized_flat_1usd_roi_pct"] > 0
    assert by_wallet[WALLET_A]["realized_01a_win_rate_pct"] > by_wallet[WALLET_A]["own_count_breakeven_win_rate_pct"]
    assert by_wallet[WALLET_A]["windows_observed"] == 30
    assert by_wallet[WALLET_A]["windows_in_band"] == 30
    assert by_wallet[WALLET_A]["in_band_coverage_pct"] == 100.0
    assert by_wallet[WALLET_A]["presence_coverage_pct"] == 100.0
    assert by_wallet[WALLET_A]["suspected_two_sided_quoting"] is True
    assert by_wallet[WALLET_B]["n_resolved"] == 9
    assert by_wallet[WALLET_B]["grade"] == "INSUFFICIENT_N"
    assert report["diagnostics"]["qualifying_pairs_resolved_from_gamma_metadata"] > 0
    assert report["pooled_cohort_including_quoting"]["n_resolved"] == 39
    assert report["pooled_cohort_including_quoting"]["n_distinct_windows"] == 30
    for item in report["candidates"]:
        assert item["in_band_coverage_pct"] <= 100.0
        assert item["presence_coverage_pct"] <= 100.0


def test_buyer_attribution_covers_both_maker_side_branches() -> None:
    rows = [
        _row(wallet=WALLET_A, asset="buy", start=1_000, outcome_side="BUY"),
        _row(wallet=WALLET_B, asset="sell", start=1_000, outcome_side="SELL"),
    ]
    metadata = {
        "buy": {"market_slug": "btc-updown-5m-1000", "outcome": "Up", "winning_outcome": "Up"},
        "sell": {"market_slug": "btc-updown-5m-1000", "outcome": "Down", "winning_outcome": "Down"},
    }
    report = build_report(rows, token_metadata=metadata, resolution_rows=[], since_block_ts=0, generated_at="now")
    assert {item["wallet"] for item in report["candidates"]} == {WALLET_A, WALLET_B}


def test_settlement_contract_attribution_is_excluded() -> None:
    rows = [
        _row(wallet=WALLET_A, asset="buy", start=1_000, outcome_side="BUY"),
        {
            "maker": MAKER,
            "taker": EXCHANGE,
            "block_ts": 1_020,
            "decoded": {"side": "SELL", "maker_side": "SELL", "asset": "sell", "price": 0.26, "size": 1.0},
        },
    ]
    metadata = {
        "buy": {"market_slug": "btc-updown-5m-1000", "outcome": "Up", "winning_outcome": "Up"},
        "sell": {"market_slug": "btc-updown-5m-1000", "outcome": "Down", "winning_outcome": "Down"},
    }
    report = build_report(rows, token_metadata=metadata, resolution_rows=[], since_block_ts=0, generated_at="now")
    assert {item["wallet"] for item in report["candidates"]} == {WALLET_A}
    assert report["diagnostics"]["attributions_to_settlement_contracts"] == 1


def test_pooled_edge_refuses_correlated_window_inflation() -> None:
    rows = []
    metadata = {}
    for wallet in (WALLET_A, WALLET_B, "0xdddddddddddddddddddddddddddddddddddddddd"):
        for index in range(4):
            start = 20_000 + 300 * index
            asset = f"{wallet[-4:]}-{index}"
            metadata[asset] = {
                "market_slug": f"btc-updown-5m-{start}",
                "outcome": "Up",
                "winning_outcome": "Up",
            }
            rows.append(_row(wallet=wallet, asset=asset, start=start))
    report = build_report(rows, token_metadata=metadata, resolution_rows=[], since_block_ts=0, generated_at="now")
    pooled = report["pooled_cohort_including_quoting"]
    assert pooled["n_resolved"] == 12
    assert pooled["n_distinct_windows"] == 4
    assert pooled["resolved_to_distinct_window_ratio"] == 3.0
    assert pooled["grade"] == "OUTCOME_CORRELATED_POOL_NOT_INDEPENDENT"


def test_pooled_edge_refuses_two_wallets_sharing_every_window() -> None:
    rows = []
    metadata = {}
    for wallet in (WALLET_A, WALLET_B):
        for index in range(30):
            start = 30_000 + 300 * index
            asset = f"{wallet[-4:]}-{index}"
            metadata[asset] = {
                "market_slug": f"btc-updown-5m-{start}",
                "outcome": "Up",
                "winning_outcome": "Up",
            }
            rows.append(_row(wallet=wallet, asset=asset, start=start))
    report = build_report(rows, token_metadata=metadata, resolution_rows=[], since_block_ts=0, generated_at="now")
    pooled = report["pooled_cohort_including_quoting"]
    assert pooled["n_resolved"] == 60
    assert pooled["n_distinct_windows"] == 30
    assert pooled["resolved_to_distinct_window_ratio"] == 2.0
    assert pooled["grade"] == "OUTCOME_CORRELATED_POOL_NOT_INDEPENDENT"
    assert report["publication_status"] == "QUARANTINED_OUTCOME_CORRELATED_POOL"


def test_negative_roi_never_passes_even_above_reference_constant() -> None:
    rows = []
    metadata = {}
    resolutions = []
    for index in range(30):
        start = 10_000 + 300 * index
        slug = f"btc-updown-5m-{start}"
        asset = f"high-{index}"
        metadata[asset] = {"market_slug": slug, "outcome": "Up", "winning_outcome": "Up" if index < 8 else "Down"}
        row = _row(wallet=WALLET_A, asset=asset, start=start)
        row["decoded"]["price"] = 0.31
        rows.append(row)
        resolutions.append({"market_slug": slug, "direction": "UP" if index < 8 else "DOWN"})
    report = build_report(rows, token_metadata=metadata, resolution_rows=resolutions, since_block_ts=0, generated_at="now")
    candidate = report["candidates"][0]
    assert candidate["realized_01a_win_rate_pct"] > 23.861261
    assert candidate["realized_flat_1usd_roi_pct"] < 0
    assert candidate["grade"] == "FAIL_NONPOSITIVE_ROI_OR_OWN_BREAKEVEN"
