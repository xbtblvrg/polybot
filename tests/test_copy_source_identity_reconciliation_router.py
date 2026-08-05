from __future__ import annotations

from scripts import run_copy_source_identity_reconciliation_router as router


WALLET = "0x1111111111111111111111111111111111111111"
PROXY = "0x2222222222222222222222222222222222222222"
TOKEN = "123"
TX = "0xabc"


def source(*, observed: float = 1_000.0, polygon_woken: bool = True) -> dict:
    return {
        "action": "BUY",
        "source_wallet": WALLET,
        "event_id": f"event-{observed}",
        "transaction_hash": TX,
        "token_id": TOKEN,
        "market_slug": "btc-updown-5m-900",
        "outcome": "UP",
        "observed_ts": observed,
        "source": "polygon_orderfilled_direct" if polygon_woken else "data_api",
    }


def polygon(
    *,
    log_index: int = 7,
    received: float = 1_010.0,
    maker: str = WALLET,
    token: str = TOKEN,
) -> dict:
    return {
        "event": "polygon_orderfilled_log",
        "transaction_hash": TX,
        "log_index": log_index,
        "received_at_s": received,
        "maker": maker,
        "taker": PROXY,
        "decoded": {"asset": token, "maker": maker, "taker": PROXY},
    }


META = {TOKEN: {"market_slug": "btc-updown-5m-900", "outcome": "UP"}}


def reconcile(row: dict, log: dict | None) -> dict:
    index, _ = router.polygon_identity_index([] if log is None else [log])
    return router.reconcile_cohort([row], polygon_by_transaction=index, token_metadata=META)


def test_proxy_mapping_and_exact_reconciliation() -> None:
    report = reconcile(source(), polygon())
    assert report["input_rows"] == report["terminal_rows"] == 1
    assert report["terminal_counts"] == {"matched_enabled_proxy": 1}
    assert report["identity_market_outcome_parity_violations"] == 0
    assert report["duplicate_routes"] == 0
    assert report["proxy_source_aliases"][PROXY] == WALLET
    assert report["ambiguous_proxy_aliases"] == {}


def test_unmapped_proxy_and_missing_sidecar_are_distinct() -> None:
    unmapped = reconcile(source(), polygon(maker=PROXY))
    missing = reconcile(source(), None)
    assert unmapped["terminal_counts"] == {"matched_unmapped_proxy": 1}
    assert missing["terminal_counts"] == {"missing_sidecar": 1}


def test_late_receipt_and_current_not_woken_are_distinct() -> None:
    late = reconcile(source(), polygon(received=1_200.0))
    not_woken = reconcile(source(polygon_woken=False), polygon(received=1_010.0))
    assert late["terminal_counts"] == {"received_after_market_close": 1}
    assert not_woken["terminal_counts"] == {"current/+300_not_woken": 1}


def test_duplicate_polygon_identity_is_deduped_and_source_route_is_counted_once() -> None:
    duplicate = polygon()
    index, replay_deduped = router.polygon_identity_index([duplicate, dict(duplicate)])
    report = router.reconcile_cohort(
        [source(), dict(source(), event_id="event-2")],
        polygon_by_transaction=index,
        token_metadata=META,
    )
    assert replay_deduped == 1
    assert report["input_rows"] == report["terminal_rows"] == 2
    assert report["duplicate_routes"] == 0
    assert report["source_identity_replays_deduped"] == 1
    assert report["representative_rows"][0]["route_eligible"] is True
    assert report["representative_rows"][1]["route_eligible"] is False


def test_token_market_outcome_parity_violation_is_reported() -> None:
    report = reconcile(source(), polygon(token="different"))
    assert report["identity_market_outcome_parity_violations"] == 1


def test_source_cohort_is_stable_deduped_and_latest_limited() -> None:
    rows = [
        dict(source(observed=1_000.0), event_id="a"),
        dict(source(observed=1_001.0), event_id="a"),
        dict(source(observed=1_002.0), event_id="b"),
    ]
    cohort = router.source_cohort(
        rows,
        enabled_wallets={WALLET},
        start_s=900.0,
        end_s=1_100.0,
        limit_latest=1,
    )
    assert [row["event_id"] for row in cohort] == ["b"]


def test_source_cohort_excludes_non_btc_rows() -> None:
    rows = [
        source(observed=1_000.0),
        dict(
            source(observed=1_001.0),
            event_id="bnb",
            market_slug="bnb-updown-5m-900",
        ),
    ]
    cohort = router.source_cohort(
        rows,
        enabled_wallets={WALLET},
        start_s=900.0,
        end_s=1_100.0,
    )
    assert [row["event_id"] for row in cohort] == ["event-1000.0"]
    assert (
        router.excluded_non_btc_count(
            rows,
            enabled_wallets={WALLET},
            start_s=900.0,
            end_s=1_100.0,
        )
        == 1
    )


def test_frozen_roster_retains_later_demoted_wallet_but_current_roster_does_not() -> None:
    wallet = "0x4d8bc628487bbc9931b4d039e6a7529b8ae1a00d"
    row = dict(source(observed=1_000.0), source_wallet=wallet)

    frozen = router.source_cohort(
        [row],
        enabled_wallets=router.FROZEN_ENABLED_WALLETS,
        start_s=900.0,
        end_s=1_100.0,
    )
    current = router.source_cohort(
        [row],
        enabled_wallets={WALLET},
        start_s=900.0,
        end_s=1_100.0,
    )

    assert len(frozen) == 1
    assert current == []
