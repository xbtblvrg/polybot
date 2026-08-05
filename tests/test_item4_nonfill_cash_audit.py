from scripts.report_item4_nonfill_cash_audit import (
    POLYMARKET_CTF,
    ZERO_ADDRESS,
    _address_topic,
    _canonical_checksum,
    _exact_attribution,
    _scorecard_residual_endpoint,
    _residual_candidates,
    _topic_address,
    classify_transfer,
)


WALLET = "0xee888fa7b96007f7fa270988e92bddb0ae19ed10"
EXTERNAL = "0x1111111111111111111111111111111111111111"
EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"


def _row(**overrides):
    base = {
        "tx": "0xabc",
        "direction": "IN",
        "counterparty": EXTERNAL,
        "amount_usd": 1.0,
        "block_ts": 1783260000.0,
    }
    base.update(overrides)
    return base


def test_topic_address_round_trip():
    topic = _address_topic(WALLET)

    assert topic == "0x000000000000000000000000ee888fa7b96007f7fa270988e92bddb0ae19ed10"
    assert _topic_address(topic) == WALLET


def test_classify_ledger_fill_takes_precedence():
    result = classify_transfer(
        _row(tx="0xfill", direction="OUT", counterparty=EXTERNAL, amount_usd=2.4),
        wallet=WALLET,
        start_ts=1783260000.0,
        ledger_txs={"0xfill": {"orders": 1, "cost_usd": 2.4}},
        redeem_txs={},
        activity_redeem_txs={},
    )

    assert result["classification"] == "fill_settlement"
    assert result["signed_amount_usd"] == -2.4


def test_classify_redemption_from_activity():
    result = classify_transfer(
        _row(tx="0xredeem", direction="IN", counterparty=EXTERNAL, amount_usd=5.0),
        wallet=WALLET,
        start_ts=1783260000.0,
        ledger_txs={},
        redeem_txs={},
        activity_redeem_txs={"0xredeem": {"amount_usd": 5.0}},
    )

    assert result["classification"] == "redemption_payout"


def test_classify_large_inbound_near_baseline_as_topup():
    result = classify_transfer(
        _row(amount_usd=335.0, block_ts=1783260300.0),
        wallet=WALLET,
        start_ts=1783260000.0,
        ledger_txs={},
        redeem_txs={},
        activity_redeem_txs={},
    )

    assert result["classification"] == "topup_deposit"


def test_classify_known_counterparty_as_settlement_and_external_as_other():
    settlement = classify_transfer(
        _row(tx="0xknown", direction="OUT", counterparty=EXCHANGE, amount_usd=3.0),
        wallet=WALLET,
        start_ts=1783260000.0,
        ledger_txs={},
        redeem_txs={},
        activity_redeem_txs={},
    )
    redeem_candidate = classify_transfer(
        _row(tx="0xctf", direction="IN", counterparty=POLYMARKET_CTF.lower(), amount_usd=4.0),
        wallet=WALLET,
        start_ts=1783260000.0,
        ledger_txs={},
        redeem_txs={},
        activity_redeem_txs={},
    )
    other = classify_transfer(
        _row(tx="0xother", direction="OUT", counterparty=EXTERNAL, amount_usd=7.0),
        wallet=WALLET,
        start_ts=1783260000.0,
        ledger_txs={},
        redeem_txs={},
        activity_redeem_txs={},
    )

    assert settlement["classification"] == "fill_settlement"
    assert redeem_candidate["classification"] == "redemption_payout"
    assert other["classification"] == "other_counterparty_out"


def test_classify_zero_address_mint_and_disperse_inbound_as_redemption_payout():
    zero_mint = classify_transfer(
        _row(
            tx="0xzero",
            direction="IN",
            counterparty="0x0000000000000000000000000000000000000000",
            amount_usd=7.58,
        ),
        wallet=WALLET,
        start_ts=1783260000.0,
        ledger_txs={},
        redeem_txs={},
        activity_redeem_txs={},
    )
    disperse = classify_transfer(
        _row(
            tx="0xdisperse",
            direction="IN",
            counterparty=EXTERNAL,
            amount_usd=15.58,
            blockscout={"function_name": "disperseToken(address token, address[] recipients, uint256[] values)"},
        ),
        wallet=WALLET,
        start_ts=1783260000.0,
        ledger_txs={},
        redeem_txs={},
        activity_redeem_txs={},
    )

    assert zero_mint["classification"].startswith("redemption_payout")
    assert disperse["classification"].startswith("redemption_payout")


def test_classify_zero_address_inbound_as_unmatched_payout_mint():
    result = classify_transfer(
        _row(tx="0xmint", direction="IN", counterparty=ZERO_ADDRESS, amount_usd=4.897958),
        wallet=WALLET,
        start_ts=1783260000.0,
        ledger_txs={},
        redeem_txs={},
        activity_redeem_txs={},
    )

    assert result["classification"] == "redemption_payout_unmatched_zero_mint"


def test_residual_candidates_use_order6_half_dollar_match_rule():
    rows = [
        _row(tx="0xnear", direction="OUT", amount_usd=19.31, signed_amount_usd=-19.31),
        _row(tx="0xfar", direction="OUT", amount_usd=19.20, signed_amount_usd=-19.20),
    ]

    matches = _residual_candidates(rows, [-19.777051])

    assert [row["tx"] for row in matches] == ["0xnear"]
    assert matches[0]["target_usd"] == -19.777051


def test_current_residual_endpoint_and_checksum_are_deterministic():
    scorecard = {
        "generated_at": "2026-07-19T00:00:00Z",
        "chain_reconciliation": {
            "live_cash_balance_usd": 353.0,
            "expected_cash_identity_usd": 343.0,
            "cash_delta_vs_expected_identity_usd": 10.0,
            "balance_sampling": {"samples": [{"ts": "2026-07-18T23:59:59Z"}]},
        },
    }
    endpoint = _scorecard_residual_endpoint(scorecard, "scorecard.json")
    assert endpoint["balance_sample_at"] == "2026-07-18T23:59:59Z"
    assert endpoint["residual_usd"] == 10.0
    assert _canonical_checksum(endpoint) == _canonical_checksum(dict(reversed(list(endpoint.items()))))


def test_current_residual_endpoint_accepts_canonical_delta_field():
    endpoint = _scorecard_residual_endpoint(
        {
            "generated_at": "2026-07-19T00:00:00Z",
            "chain_reconciliation": {
                "delta_vs_expected_usd": 10.328578,
                "balance_sampling": {
                    "selected_balance_usd": 353.960469,
                    "samples": [{"ts": "2026-07-19T02:58:34Z"}],
                },
            },
        },
        "scorecard.json",
    )
    assert endpoint["residual_usd"] == 10.328578
    assert endpoint["balance_usd"] == 353.960469


def test_exact_attribution_returns_smallest_deterministic_match_or_empty():
    rows = [
        {"tx": "a", "signed_amount_usd": -5.0},
        {"tx": "b", "signed_amount_usd": -3.51765},
        {"tx": "c", "signed_amount_usd": 1.0},
    ]
    assert [row["tx"] for row in _exact_attribution(rows, -8.51765)] == ["a", "b"]
    assert _exact_attribution(rows, 99.0) == []
