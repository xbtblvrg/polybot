from __future__ import annotations

import json
from types import SimpleNamespace

import requests

from scripts import build_wallet_market_scan_intake as scan


def _args(tmp_path, **overrides):
    values = {
        "output": str(tmp_path / "wallet_market_scan_intake_latest.json"),
        "ranked_output": str(tmp_path / "wallet_market_scan_ranked.json"),
        "lookback_days": 7.0,
        "limit": 3,
        "max_pages": 3,
        "timeout_s": 1.0,
        "max_wall_runtime_s": 0.0,
        "sleep_s": 0.0,
        "reset": False,
        "include_leaderboard": False,
        "leaderboard_limit": 10,
        "leaderboard_pages": 1,
        "leaderboard_timeout_s": 1.0,
        "leaderboard_retries": 1,
        "market_scope_resolutions": "",
        "market_scope_limit": 0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_merge_trade_keeps_only_crypto_5m_wallets() -> None:
    stats = {}

    assert scan.merge_trade(
        stats,
        {
            "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "slug": "btc-updown-5m-1783964700",
            "side": "BUY",
            "size": 5,
            "price": 0.4,
            "timestamp": 1783964800,
            "transactionHash": "0xabc",
        },
        source="data_api_trades",
    )
    assert not scan.merge_trade(
        stats,
        {
            "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "slug": "btc-updown-5m-1783964700",
            "side": "BUY",
            "size": 5,
            "price": 0.4,
            "timestamp": 1783964800,
            "transactionHash": "0xabc",
        },
        source="data_api_trades",
    )
    assert not scan.merge_trade(
        stats,
        {
            "proxyWallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "slug": "bnb-up-or-down-july-13-2026-1pm-et",
            "side": "BUY",
            "size": 5,
            "price": 0.4,
            "timestamp": 1783964800,
        },
        source="data_api_trades",
    )

    wallet = stats["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    assert wallet["crypto5m_trade_count"] == 1
    assert wallet["btc5m_trade_count"] == 1
    assert wallet["approx_notional_usd"] == 2.0


def test_build_market_scan_batches_pages_and_preserves_replay_pending(monkeypatch, tmp_path) -> None:
    now_ts = 1783965000.0
    previous = {
        "ranked_wallets": [
            {
                "wallet": "0xcccccccccccccccccccccccccccccccccccccccc",
                "source_wallet": "0xcccccccccccccccccccccccccccccccccccccccc",
                "source_channels": ["data_api_trades"],
                "trade_count": 1,
                "crypto5m_trade_count": 1,
                "btc5m_trade_count": 0,
                "buy_count": 1,
                "sell_count": 0,
                "approx_notional_usd": 1.5,
                "first_seen_ts": now_ts - 3600,
                "last_seen_ts": now_ts - 3600,
                "symbols": {"eth": 1},
                "top_slugs": [{"slug": "eth-updown-5m-1783961100", "trade_count": 1}],
                "sample_trades": [],
            }
        ]
    }
    ranked_output = tmp_path / "wallet_market_scan_ranked.json"
    ranked_output.write_text(json.dumps(previous), encoding="utf-8")
    pages = {
        0: [
            {
                "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "slug": "btc-updown-5m-1783964700",
                "side": "BUY",
                "size": 6,
                "price": 0.33,
                "timestamp": now_ts - 60,
            },
            {
                "proxyWallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "slug": "eth-updown-5m-1783964700",
                "side": "SELL",
                "size": 2,
                "price": 0.5,
                "timestamp": now_ts - 30,
            },
            {
                "proxyWallet": "0xdddddddddddddddddddddddddddddddddddddddd",
                "slug": "will-btc-hit-120k",
                "side": "BUY",
                "size": 2,
                "price": 0.5,
                "timestamp": now_ts - 30,
            },
        ],
        3: [
            {
                "proxyWallet": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "slug": "btc-updown-5m-1783350000",
                "side": "BUY",
                "size": 1,
                "price": 0.5,
                "timestamp": now_ts - (8 * 86400),
            }
        ],
    }

    def fake_fetch(_client, *, limit, offset, timeout_s):
        return pages[offset], {"route_class": "DIRECT_PASS", "status": "PASS"}

    monkeypatch.setattr(scan, "_fetch_trade_page", fake_fetch)

    payload = scan.build_market_scan(_args(tmp_path, ranked_output=str(ranked_output)), now_ts=now_ts)

    assert payload["status"] == "PASS_LOOKBACK_COMPLETE"
    assert payload["summary"]["trades_scanned"] == 4
    assert payload["summary"]["crypto5m_trades_matched"] == 2
    assert payload["summary"]["active_wallets"] == 3
    assert payload["summary"]["new_active_wallets"] == 2
    assert payload["summary"]["scanned_alive_profitable"] == 0
    assert payload["summary"]["replay_status"] == "PENDING_REMOTE_HISTORY_REPLAY"
    assert payload["window"]["lookback_complete"] is True
    assert payload["route_class_counts"] == {"DIRECT_PASS": 2}
    assert payload["ranked_wallets"][0]["profitability_status"] == "PENDING_REMOTE_HISTORY_REPLAY"


def test_fetch_trade_page_treats_positive_offset_400_as_pagination_cap() -> None:
    response = requests.Response()
    response.status_code = 400
    response.url = "https://data-api.polymarket.com/trades?limit=500&offset=3500"
    response._content = b'{"error":"offset too high"}'
    response.wallet_copy_route_report = {
        "status": "PASS",
        "route_class": "DIRECT_PASS",
        "route_report_id": "rr_test",
        "attempts": [{"http_status": 400}],
    }

    class FakeClient:
        def request(self, *args, **kwargs):
            return response

    rows, meta = scan._fetch_trade_page(FakeClient(), limit=500, offset=3500, timeout_s=1.0)

    assert rows == []
    assert meta["route_class"] == "DATA_API_PAGINATION_CAP"
    assert meta["pagination_cap_reached"] is True
    assert meta["http_status"] == 400


def test_build_market_scan_labels_short_page_truncation(monkeypatch, tmp_path) -> None:
    now_ts = 1783965000.0

    def fake_fetch(_client, *, limit, offset, timeout_s):
        assert limit == 3
        return (
            [
                {
                    "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "slug": "btc-updown-5m-1783964700",
                    "side": "BUY",
                    "size": 1,
                    "price": 0.5,
                    "timestamp": now_ts - 60,
                }
            ],
            {"route_class": "DIRECT_PASS"},
        )

    monkeypatch.setattr(scan, "_fetch_trade_page", fake_fetch)

    payload = scan.build_market_scan(_args(tmp_path), now_ts=now_ts)

    budget = payload["rate_limit_budget"]
    assert payload["window"]["lookback_complete"] is False
    assert budget["exhaustion_class"] == "SHORT_PAGE_TRUNCATION"
    assert budget["short_page_truncation"] == {
        "class": "SHORT_PAGE_TRUNCATION",
        "page": 0,
        "offset": 0,
        "rows": 1,
        "requested_limit": 3,
        "lookback_complete": False,
    }


def test_build_market_scan_can_scope_requests_to_btc5m_conditions(monkeypatch, tmp_path) -> None:
    now_ts = 1783965000.0
    resolutions = tmp_path / "resolutions.jsonl"
    resolutions.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "asset": "BTC",
                        "window_type": "5m",
                        "window_start_unix_ts": now_ts - 7200,
                        "condition_id": "condition-old",
                    }
                ),
                json.dumps(
                    {
                        "asset": "BTC",
                        "window_type": "5m",
                        "window_start_unix_ts": now_ts - 3600,
                        "condition_id": "condition-new",
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    requested = []

    def fake_fetch(_client, *, limit, offset, timeout_s, market=""):
        requested.append((offset, market))
        return (
            [
                {
                    "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "slug": "btc-updown-5m-1783964700",
                    "side": "BUY",
                    "size": 1,
                    "price": 0.5,
                    "timestamp": now_ts - 3600,
                }
            ],
            {"route_class": "DIRECT_PASS"},
        )

    monkeypatch.setattr(scan, "_fetch_trade_page", fake_fetch)
    args = _args(
        tmp_path,
        market_scope_resolutions=str(resolutions),
        market_scope_limit=2,
    )

    payload = scan.build_market_scan(args, now_ts=now_ts)

    assert requested == [(0, "condition-old"), (0, "condition-new")]
    assert payload["source"]["market_scoped"] is True
    assert payload["source"]["market_scope_condition_ids_completed"] == 2
    assert payload["summary"]["trades_scanned"] == 2
    assert payload["rate_limit_budget"]["page_cap_exhausted"] is True
    assert payload["rate_limit_budget"]["exhaustion_class"] == "PAGE_CAP"
