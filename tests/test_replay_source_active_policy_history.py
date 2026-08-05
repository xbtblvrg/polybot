from scripts.replay_source_active_policy_history import (
    _normalize_history_event,
    _select_recurring_cohort_targets,
    _select_targets,
)
from scripts.build_wallet_market_cohort_replay import _fetch_wallet_page


class _Response:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list[dict]:
        return []


class _Client:
    def __init__(self) -> None:
        self.params = None

    def request(self, _method, _url, *, params, **_kwargs):
        self.params = params
        return _Response()


def test_history_fetch_uses_canonical_taker_false_route() -> None:
    client = _Client()

    _fetch_wallet_page(
        client,
        wallet="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        limit=500,
        offset=0,
        timeout_s=10.0,
    )

    assert client.params["takerOnly"] == "false"


def test_select_targets_filters_active_denylisted_and_sorts_policy_then_recency() -> None:
    packet = {
        "packets": [
            {
                "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "source_active": {"policy_eligible_tally_status": "PASS", "policy_eligible_windows": 2},
                "latest_trade_age_h": 1.0,
                "paper_pnl_usd": 10,
            },
            {
                "wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "source_active": {"policy_eligible_tally_status": "PASS", "policy_eligible_windows": 5},
                "latest_trade_age_h": 3.0,
                "paper_pnl_usd": 10,
            },
            {
                "wallet": "0xcccccccccccccccccccccccccccccccccccccccc",
                "source_active": {"policy_eligible_tally_status": "PASS", "policy_eligible_windows": 5},
                "latest_trade_age_h": 0.5,
                "paper_pnl_usd": 10,
            },
            {
                "wallet": "0xdddddddddddddddddddddddddddddddddddddddd",
                "source_active": {"policy_eligible_tally_status": "PASS", "policy_eligible_windows": 9},
                "already_active_runtime_member": True,
            },
            {
                "wallet": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "source_active": {"policy_eligible_tally_status": "PASS", "policy_eligible_windows": 9},
                "denylist_cells": [{"reason": "test"}],
            },
        ]
    }

    targets = _select_targets(packet, limit=3)

    assert [row["wallet"] for row in targets] == [
        "0xcccccccccccccccccccccccccccccccccccccccc",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ]

    prioritized = _select_targets(
        packet,
        limit=3,
        priority_wallets=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
    )
    assert [row["wallet"] for row in prioritized] == [
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xcccccccccccccccccccccccccccccccccccccccc",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]

    skip_replayed = _select_targets(
        packet,
        limit=3,
        already_replayed_wallets={"0xcccccccccccccccccccccccccccccccccccccccc"},
    )
    assert [row["wallet"] for row in skip_replayed] == [
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ]


def test_normalize_history_event_emits_temporal_compatible_btc5m_buy() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    event = _normalize_history_event(
        {
            "proxyWallet": wallet,
            "slug": "btc-updown-5m-1783965000",
            "side": "BUY",
            "outcome": "Up",
            "price": 0.4,
            "size": 10,
            "timestamp": 1783965010,
            "transactionHash": "0xabc",
            "conditionId": "cond-1",
        },
        wallet=wallet,
        wallet_name="candidate",
        cutoff_ts=1783960000,
    )
    assert event is not None
    assert event["source_wallet"] == wallet
    assert event["action"] == "BUY"
    assert event["market_slug"] == "btc-updown-5m-1783965000"
    assert event["usdc_size"] == 4.0

    assert (
        _normalize_history_event(
            {
                "proxyWallet": wallet,
                "slug": "eth-updown-5m-1783965000",
                "side": "BUY",
                "outcome": "Up",
                "price": 0.4,
                "size": 10,
                "timestamp": 1783965010,
            },
            wallet=wallet,
            wallet_name="candidate",
            cutoff_ts=1783960000,
        )
        is None
    )


def test_recurring_cohort_rotates_oldest_replay_wallets_first() -> None:
    wallets = [
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "0xcccccccccccccccccccccccccccccccccccccccc",
    ]
    cohort = {
        "nearest_frontier": [
            {"wallet": wallets[0]},
            {"wallet": wallets[1]},
            {"wallet": wallets[2]},
            {"wallet": wallets[0], "wide_policy_fingerprint": "duplicate"},
        ]
    }
    manifest = {
        "supplemental_history_files": [
            "data/research/source_active_policy_history_aaaaaaaaaa_20260730T190000Z.json",
            "data/research/source_active_policy_history_bbbbbbbbbb_20260729T190000Z.json",
        ]
    }

    selected = _select_recurring_cohort_targets(cohort, manifest, limit=2)

    assert [row["wallet"] for row in selected] == [wallets[2], wallets[1]]
    assert all(
        row["source_active"]["source"] == "recurring_wide_frontier"
        for row in selected
    )


def test_recurring_cohort_honors_explicit_priority_wallet() -> None:
    wallets = [
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "0xcccccccccccccccccccccccccccccccccccccccc",
    ]
    cohort = {"nearest_frontier": [{"wallet": wallet} for wallet in wallets]}

    selected = _select_recurring_cohort_targets(
        cohort,
        {"supplemental_history_files": []},
        limit=1,
        priority_wallets=[wallets[2]],
    )

    assert [row["wallet"] for row in selected] == [wallets[2]]
