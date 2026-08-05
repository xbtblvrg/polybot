import pytest

from scripts.report_orderfilled_early_01a_supply_census import build_census


WALLET_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
WALLET_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
MAKER = "0xcccccccccccccccccccccccccccccccccccccccc"
EXCHANGE = "0xe111180000d2663c0091e4f400237545b87b996b"


def _row(*, maker: str, taker: str, maker_side: str, asset: str, block_ts: int, price: float) -> dict:
    return {
        "maker": maker,
        "taker": taker,
        "block_ts": block_ts,
        "decoded": {"side": maker_side, "maker_side": maker_side, "asset": asset, "price": price, "size": 1.0},
    }


def test_census_uses_buy_counterparty_and_unions_shared_window() -> None:
    rows = [
        _row(maker=WALLET_A, taker=MAKER, maker_side="BUY", asset="up", block_ts=1_020, price=0.26),
        _row(maker=MAKER, taker=WALLET_B, maker_side="SELL", asset="down", block_ts=1_030, price=0.27),
        _row(maker=WALLET_A, taker=MAKER, maker_side="BUY", asset="late", block_ts=1_370, price=0.30),
    ]
    metadata = {
        "up": {"market_slug": "btc-updown-5m-1000"},
        "down": {"market_slug": "btc-updown-5m-1000"},
        "late": {"market_slug": "btc-updown-5m-1300"},
    }
    report = build_census(rows, token_metadata=metadata, since_block_ts=0, generated_at="now")
    assert report["distinct_btc5m_01a_windows_observed"] == 2
    assert report["distinct_btc5m_01a_within_60s_windows_observed"] == 1
    assert report["distinct_wallets_with_at_least_1_qualifying_window"] == 2
    assert report["union_qualifying_window_count"] == 1
    assert report["union_qualifying_01a_windows_per_day"] == 144.0
    assert {item["wallet"] for item in report["ranking"]} == {WALLET_A, WALLET_B}
    assert report["rung_clearing_qualifying_window_count_threshold"] == 1
    assert report["rung_clearing_wallet_count"] == 2
    for item in report["ranking"]:
        assert item["span_days"] == report["union_span_days"]
        assert item["qualifying_01a_windows_per_day"] == pytest.approx(
            item["qualifying_window_count"] / report["union_span_days"], rel=1e-4
        )


def test_census_applies_since_block_timestamp() -> None:
    rows = [_row(maker=WALLET_A, taker=MAKER, maker_side="BUY", asset="up", block_ts=1_020, price=0.26)]
    report = build_census(
        rows,
        token_metadata={"up": {"market_slug": "btc-updown-5m-1000"}},
        since_block_ts=1_021,
        generated_at="now",
    )
    assert report["distinct_wallets_observed"] == 0
    assert report["diagnostics"]["rows_before_since_block_ts"] == 1


def test_census_excludes_settlement_contract_attribution() -> None:
    rows = [
        _row(maker=WALLET_A, taker=MAKER, maker_side="BUY", asset="up", block_ts=1_020, price=0.26),
        _row(maker=MAKER, taker=EXCHANGE, maker_side="SELL", asset="down", block_ts=1_030, price=0.27),
    ]
    metadata = {
        "up": {"market_slug": "btc-updown-5m-1000"},
        "down": {"market_slug": "btc-updown-5m-1000"},
    }
    report = build_census(rows, token_metadata=metadata, since_block_ts=0, generated_at="now")
    assert {item["wallet"] for item in report["ranking"]} == {WALLET_A}
    assert report["diagnostics"]["attributions_to_settlement_contracts"] == 1
