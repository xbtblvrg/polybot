import json

from src.wallet_copy.live_tracker import (
    _prefer_earliest_observed_events,
    _read_rtds_wallet_events,
)
from src.wallet_copy.models import WalletEvent, WalletSpec


WALLET = "0x927f7694de44d19a72bce76254e628d1c141d215"


def _spec() -> WalletSpec:
    return WalletSpec(address=WALLET, name="927f", market_filter="btc_5m")


def _event(*, source: str, observed_ts: float) -> WalletEvent:
    return WalletEvent(
        source_wallet=WALLET,
        wallet_name="927f",
        row_type=source,
        action="BUY",
        condition_id="condition",
        market_slug="btc-updown-5m-1784866800",
        outcome="Up",
        price=0.4,
        size=10.0,
        usdc_size=4.0,
        event_ts=1784866810.0,
        observed_ts=observed_ts,
        source=source,
        token_id="token",
        transaction_hash="tx",
    )


def test_mux_prefers_earliest_observed_cross_route_copy() -> None:
    selected = _prefer_earliest_observed_events(
        [_event(source="polymarket_data_api", observed_ts=20.0), _event(source="rtds_activity", observed_ts=10.0)]
    )

    assert len(selected) == 1
    assert selected[0].source == "rtds_activity"


def test_rtds_cursor_is_independent_monotone_and_restart_idempotent(tmp_path) -> None:
    path = tmp_path / "rtds.jsonl"
    row = {
        "event": "rtds_trade_event",
        "source_wallet": WALLET,
        "received_at_s": 1784866811.0,
        "raw": {
            "proxyWallet": WALLET,
            "conditionId": "condition",
            "slug": "btc-updown-5m-1784866800",
            "eventSlug": "btc-updown-5m-1784866800",
            "asset": "token",
            "outcome": "Up",
            "side": "BUY",
            "price": 0.4,
            "size": 10.0,
            "timestamp": 1784866810,
            "transactionHash": "tx",
        },
    }
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    first_events, first = _read_rtds_wallet_events(str(path), specs=[_spec()], cursor={"byte_offset": 0})
    second_events, second = _read_rtds_wallet_events(
        str(path),
        specs=[_spec()],
        cursor={"byte_offset": first["next_byte_offset"]},
    )

    assert len(first_events[WALLET]) == 1
    assert second_events[WALLET] == []
    assert second["next_byte_offset"] == first["next_byte_offset"]


def test_uncursored_rtds_attach_starts_at_eof_without_synthetic_binding(tmp_path) -> None:
    path = tmp_path / "rtds.jsonl"
    path.write_text("{}\n", encoding="utf-8")

    events, report = _read_rtds_wallet_events(str(path), specs=[_spec()], cursor=None)

    assert not events
    assert report["initialized_at_eof"] is True
    assert report["next_byte_offset"] == path.stat().st_size
