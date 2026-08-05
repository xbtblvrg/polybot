from __future__ import annotations

import json
from types import SimpleNamespace

from scripts import build_wallet_market_cohort_replay as replay


def _args(tmp_path, **overrides):
    values = {
        "market_scan": "data/research/wallet_market_scan_ranked.json",
        "output": "data/research/wallet_market_cohort_replay_latest.json",
        "state": "data/research/wallet_market_cohort_replay_state.json",
        "resolutions": "data/research/resolutions.jsonl",
        "wallet_limit": 2,
        "history_limit": 2,
        "max_pages_per_wallet": 2,
        "lookback_days": 7.0,
        "timeout_s": 1.0,
        "sleep_s": 0.0,
        "max_wall_runtime_s": 0.0,
        "min_resolved_buys": 1,
        "min_unique_markets": 1,
        "min_paper_pnl_usd": 0.0,
        "reset_state": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_market_cohort_replay_scores_wallet_user_history(monkeypatch, tmp_path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    (research / "wallet_market_scan_ranked.json").write_text(
        json.dumps(
            {
                "ranked_wallets": [
                    {
                        "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "crypto5m_trade_count": 5,
                        "rank_score": 10.0,
                    }
                ]
            }
        )
    )
    (research / "resolutions.jsonl").write_text(
        json.dumps({"market_slug": "btc-updown-5m-1783965000", "direction": "UP"}) + "\n"
    )

    def fake_fetch(_client, *, wallet, limit, offset, timeout_s):
        assert wallet == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        if offset:
            return [], {"route_class": "DIRECT_PASS"}
        return [
            {
                "proxyWallet": wallet,
                "slug": "btc-updown-5m-1783965000",
                "side": "BUY",
                "outcome": "Up",
                "price": 0.4,
                "size": 10,
                "timestamp": 1783965010,
                "transactionHash": "0xabc",
            },
            {
                "proxyWallet": wallet,
                "slug": "eth-updown-5m-1783965000",
                "side": "SELL",
                "outcome": "Down",
                "price": 0.8,
                "size": 3,
                "timestamp": 1783965011,
                "transactionHash": "0xdef",
            },
        ], {"route_class": "DIRECT_PASS"}

    monkeypatch.setattr(replay, "_fetch_wallet_page", fake_fetch)

    report = replay.build_report(root, _args(tmp_path), now_ts=1783965300.0)

    assert report["summary"]["cohort_size"] == 1
    assert report["summary"]["cohort_shadow_positive"] == 1
    assert report["summary"]["live_ready_picks"] == 1
    row = report["wallets"][0]
    assert row["paper_pnl_usd"] == 6.0
    assert row["resolved_copyable_events"] == 1
    assert row["status"] == "LIVE_READY_SHADOW_PICK"


def test_fetch_wallet_page_treats_positive_offset_502_as_pagination_cap() -> None:
    class Response:
        status_code = 502
        wallet_copy_route_report = {"route_class": "SOURCE_ROUTE_BLOCKED"}

        def raise_for_status(self) -> None:  # pragma: no cover - should not be reached
            raise AssertionError("positive-offset pagination cap should not raise")

    class Client:
        calls = 0

        def request(self, *args, **kwargs):
            self.calls += 1
            return Response()

    client = Client()
    rows, report = replay._fetch_wallet_page(
        client,
        wallet="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        limit=500,
        offset=3500,
        timeout_s=1.0,
    )

    assert client.calls == 2
    assert rows == []
    assert report["route_class"] == "DATA_API_USER_PAGINATION_CAP"
    assert report["pagination_cap_reached"] is True
    assert report["pagination_cap_status_code"] == 502
