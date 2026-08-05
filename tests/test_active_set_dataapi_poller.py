from __future__ import annotations

from argparse import Namespace

import json

from scripts import merge_dataapi_active_set_events as poller
from scripts.run_wallet_copy_live_guard import _active_set_poll_wallets
from src.wallet_copy.models import WalletEvent


def _event(wallet: str, tx: str, *, source: str = "polymarket_data_api_poll") -> WalletEvent:
    return WalletEvent(
        source_wallet=wallet,
        wallet_name="unit",
        row_type="trade",
        action="BUY",
        condition_id="0xcondition",
        market_slug="btc-updown-5m-1783102500",
        outcome="Up",
        price=0.6,
        size=2.0,
        usdc_size=1.2,
        event_ts=1783102531.0,
        observed_ts=1783102532.0,
        source=source,
        token_id="token-up",
        transaction_hash=tx,
        raw={"_walletCopySource": source},
    )


def test_active_set_poll_wallets_uses_members_and_dedupes(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.run_wallet_copy_live_guard._active_live_set_members_contract",
        lambda: [],
    )
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    runtime = {
        "members": [
            {"source_wallet": wallet_a},
            {"source_wallet": wallet_a.upper()},
            {"source_wallet": wallet_b},
            {"source_wallet": "not-a-wallet"},
        ]
    }

    assert _active_set_poll_wallets(runtime, fallback_wallet="0xcccccccccccccccccccccccccccccccccccccccc") == [
        wallet_a,
        wallet_b,
    ]


def test_active_set_poll_wallets_adds_contract_members(monkeypatch) -> None:
    contract_wallet = "0xcccccccccccccccccccccccccccccccccccccccc"
    monkeypatch.setattr(
        "scripts.run_wallet_copy_live_guard._active_live_set_members_contract",
        lambda: [{"source_wallet": contract_wallet}],
    )

    assert _active_set_poll_wallets({"members": []}) == [contract_wallet]


def test_dataapi_poller_merges_only_poll_only_transactions(monkeypatch, tmp_path) -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    history = tmp_path / "history.json"
    history_index = tmp_path / "history_index.json"
    event_log = tmp_path / "wallet_events.jsonl"
    first_seen_log = tmp_path / "dataapi_first_seen.jsonl"
    watermark_state = tmp_path / "watermarks.json"
    state = tmp_path / "poller_state.json"
    rtds_event = _event(wallet, "0xabc", source="rtds_activity")
    history.write_text(
        json.dumps(
            {
                "kind": "wallet_copy_history_state",
                "events": [rtds_event.asdict()],
                "copy_intents": [],
                "wallets": [{"address": wallet, "name": "unit"}],
                "wallet_results": [],
                "paper_only": True,
                "live_orders_allowed": False,
            }
        ),
        encoding="utf-8",
    )

    def fake_fetch(fetch_wallet: str, args: Namespace):
        assert fetch_wallet == wallet
        return (
            wallet,
            [_event(wallet, "0xabc"), _event(wallet, "0xdef")],
            {"status": "PASS", "raw_rows": 2, "normalized_trade_events": 2},
        )

    monkeypatch.setattr(poller, "_fetch_wallet_events", fake_fetch)
    args = Namespace(
        source_wallet=[],
        source_wallets=wallet,
        history_state=str(history),
        history_window_index=str(history_index),
        wallet_event_log=str(event_log),
        dataapi_first_seen_jsonl=str(first_seen_log),
        observation_watermark_state=str(watermark_state),
        state=str(state),
        limit=80,
        pages=1,
        timeout_s=2.0,
        retries=1,
        trade_query_keys="user",
        parallel_sources=True,
        disable_source_base_overrides=True,
        max_workers=2,
        poll_interval_s=12.0,
        force=True,
        max_event_age_s=10_000_000.0,
        history_retain_events=250_000,
    )

    summary = poller.run_poll(args)

    assert summary["status"] == "PASS"
    assert summary["summary"]["events_fetched"] == 2
    assert summary["summary"]["poll_only_signals"] == 1
    assert summary["summary"]["duplicate_counts"]["duplicate_tx_hash"] == 1
    payload = json.loads(history.read_text(encoding="utf-8"))
    txs = {row["transaction_hash"] for row in payload["events"]}
    assert txs == {"0xabc", "0xdef"}
    poll_rows = [row for row in payload["events"] if row["transaction_hash"] == "0xdef"]
    assert poll_rows[0]["source"] == "polymarket_data_api_poll"
    lines = event_log.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["transaction_hash"] == "0xdef"
    first_seen_rows = [json.loads(line) for line in first_seen_log.read_text(encoding="utf-8").splitlines()]
    assert len(first_seen_rows) == 1
    assert first_seen_rows[0]["event"] == "dataapi_first_seen"
    assert first_seen_rows[0]["wallet"] == wallet
    assert first_seen_rows[0]["transactionHash"] == "0xdef"
    assert first_seen_rows[0]["backfill"] is False
    assert first_seen_rows[0]["dataapi_event_age_s"] == 1.0
    assert first_seen_rows[0]["paper_only"] is True
    watermarks = json.loads(watermark_state.read_text(encoding="utf-8"))
    row = watermarks["wallets"][wallet]
    assert row["observation_source"] == "polymarket_data_api_poll"
    assert row["retained_matching_rows"] == 2
    assert row["new_matching_events"] == 1
    assert row["paper_only"] is True
    assert row["live_orders_allowed"] is False
