from scripts.report_wallet_data_api_address_form import (
    build_report,
    choose_authoritative_key,
    freshness_context,
    select_wallets,
    summarize_query_rows,
)


def _trade_row(wallet: str, *, ts: float, tx: str = "0xtx", side: str = "BUY") -> dict:
    return {
        "proxyWallet": wallet,
        "timestamp": ts,
        "marketSlug": f"btc-updown-5m-{int(ts // 300) * 300}",
        "eventSlug": f"btc-updown-5m-{int(ts // 300) * 300}",
        "title": "BTC Up or Down - 5m",
        "side": side,
        "price": 0.33,
        "size": 10,
        "usdcSize": 3.3,
        "transactionHash": tx,
        "asset": "token-yes",
        "outcome": "Up",
    }


def test_address_form_user_wins_when_proxy_wallet_returns_global_rows() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    other = "0x2222222222222222222222222222222222222222"
    user_report = summarize_query_rows(
        wallet=wallet,
        query_key="user",
        rows=[_trade_row(wallet, ts=1_800_000_000.0, tx="0xuser")],
        observed_ts=1_800_000_060.0,
        requested_pages=1,
        limit=10,
    )
    proxy_report = summarize_query_rows(
        wallet=wallet,
        query_key="proxyWallet",
        rows=[_trade_row(other, ts=1_800_000_050.0, tx="0xglobal")],
        observed_ts=1_800_000_060.0,
        requested_pages=1,
        limit=10,
    )

    selection = choose_authoritative_key([user_report, proxy_report])

    assert proxy_report["appears_unfiltered_or_global"] is True
    assert proxy_report["wallet_identity_mismatch_rows"] == 1
    assert selection["recommended_query_key"] == "user"
    assert selection["selection_basis"] == "freshest_matching_btc5m_trade"
    assert selection["proxyWallet_route_status"] == "UNFILTERED_OR_GLOBAL"


def test_address_form_proxy_can_win_when_it_has_matching_fresher_rows() -> None:
    wallet = "0x3333333333333333333333333333333333333333"
    user_report = summarize_query_rows(
        wallet=wallet,
        query_key="user",
        rows=[_trade_row(wallet, ts=1_800_000_000.0, tx="0xuser")],
        observed_ts=1_800_000_120.0,
        requested_pages=1,
        limit=10,
    )
    proxy_report = summarize_query_rows(
        wallet=wallet,
        query_key="proxyWallet",
        rows=[_trade_row(wallet, ts=1_800_000_090.0, tx="0xproxy")],
        observed_ts=1_800_000_120.0,
        requested_pages=1,
        limit=10,
    )

    selection = choose_authoritative_key([user_report, proxy_report])

    assert proxy_report["appears_unfiltered_or_global"] is False
    assert selection["recommended_query_key"] == "proxyWallet"
    assert selection["last_trade_ts"] == 1_800_000_090.0
    assert selection["user_only_hot_path_supported"] is True
    assert selection["user_last_trade_ts"] == 1_800_000_000.0


def test_address_form_selects_active_breadth_and_explicit_wallets() -> None:
    active = "0x4444444444444444444444444444444444444444"
    breadth = "0x5555555555555555555555555555555555555555"
    explicit = "0x6666666666666666666666666666666666666666"

    rows = select_wallets(
        live_guard_state={
            "active_set": {
                "members": [
                    {
                        "source_wallet": active,
                        "status": "ACTIVE",
                        "candidate_id": "active_candidate",
                    }
                ]
            }
        },
        breadth_dispositions={
            "wallets": [
                {
                    "wallet": breadth,
                    "status": "MEASUREMENT_ONLY_TEMPORAL_CLOSURE",
                    "candidate_id": "breadth_candidate",
                }
            ]
        },
        queue={"ranked_members": []},
        include_wallets=[explicit],
    )

    by_wallet = {row["wallet"]: row for row in rows}
    assert set(by_wallet) == {active, breadth, explicit}
    assert by_wallet[active]["sources"] == ["live_guard_active_set"]
    assert by_wallet[breadth]["statuses"] == ["MEASUREMENT_ONLY_TEMPORAL_CLOSURE"]
    assert by_wallet[explicit]["sources"] == ["explicit_include"]


def test_address_form_freshness_context_flags_embedded_age_contradiction() -> None:
    wallet = "0x7777777777777777777777777777777777777777"
    context = freshness_context(
        wallet=wallet,
        queue_row={
            "fresh_flow_rank": {
                "latest_btc5m_trade_ts": 1_800_000_000.0,
                "latest_trade_age_h": 38.6,
                "source_reported_latest_trade_age_h": 0.05,
            }
        },
        address_selection={"last_trade_age_h": 39.0},
    )

    assert context["freshness_age_contradiction_detected"] is True
    assert context["queue_computed_latest_trade_age_h"] == 38.6
    assert context["queue_source_reported_latest_trade_age_h"] == 0.05


def test_address_form_build_report_uses_injected_fetcher() -> None:
    wallet = "0x8888888888888888888888888888888888888888"

    def fake_fetch(fetch_wallet, query_key, **_kwargs):
        if query_key == "user":
            rows = [_trade_row(fetch_wallet, ts=1_800_000_000.0, tx="0xuser")]
        else:
            rows = [_trade_row("0x9999999999999999999999999999999999999999", ts=1_800_000_100.0, tx="0xglobal")]
        return {
            "query_key": query_key,
            "rows": rows,
            "duration_s": 0.01,
            "errors": [],
            "route_report": {"status": "PASS", "route_class": "DIRECT_PASS"},
        }

    report = build_report(
        live_guard_state={"active_set": {"members": [{"source_wallet": wallet, "status": "ACTIVE"}]}},
        breadth_dispositions={},
        queue={},
        include_wallets=[],
        pages=1,
        limit=10,
        timeout_s=1.0,
        retries=0,
        fetch_query_rows=fake_fetch,
    )

    assert report["summary"]["wallets_probed"] == 1
    assert report["summary"]["user_recommended"] == 1
    assert report["summary"]["proxyWallet_unfiltered_or_global"] == 1
    assert report["rows"][0]["address_selection"]["recommended_query_key"] == "user"
