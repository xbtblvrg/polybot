from __future__ import annotations

import urllib.error

from scripts import report_no_copy_signal_watcher_gap as report


def test_sample_evenly_keeps_first_last_and_requested_size() -> None:
    rows = [{"window_start_s": index} for index in range(10)]

    sampled = report._sample_evenly(rows, 4)

    assert len(sampled) == 4
    assert sampled[0]["window_start_s"] == 0
    assert sampled[-1]["window_start_s"] == 9


def test_classify_sampled_windows_names_watcher_gap_when_active_wallet_traded() -> None:
    sampled = [
        {"window_start_s": 100, "market_slug": "btc-updown-5m-100"},
        {"window_start_s": 400, "market_slug": "btc-updown-5m-400"},
    ]
    trades = {
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": [
            {
                "slug": "btc-updown-5m-100",
                "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "transactionHash": "0xtx",
                "side": "BUY",
                "outcome": "Up",
                "size": 2,
                "price": 0.4,
                "timestamp": 123,
            },
            {
                "slug": "eth-updown-5m-400",
                "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "transactionHash": "0xeth",
                "side": "BUY",
                "outcome": "Up",
                "size": 2,
                "price": 0.4,
                "timestamp": 124,
            },
        ]
    }

    rows = report.classify_sampled_windows(sampled, trades)

    assert rows[0]["classification"] == "WATCHER_GAP"
    assert rows[0]["active_wallet_trade_count"] == 1
    assert rows[1]["classification"] == "COVERAGE_GAP"
    assert rows[1]["active_wallet_trade_count"] == 0


def test_fetch_wallet_trades_records_http_error(monkeypatch) -> None:
    def raise_http_error(*args, **kwargs):
        raise urllib.error.HTTPError(
            url="https://data-api.polymarket.com/trades",
            code=400,
            msg="Bad Request",
            hdrs={},
            fp=None,
        )

    monkeypatch.setattr(report.urllib.request, "urlopen", raise_http_error)

    rows, meta = report._fetch_wallet_trades(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        start_ts=100,
        end_ts=200,
        limit=10,
        max_pages=2,
        timeout_s=1.0,
    )

    assert rows == []
    assert meta["errored"] is True
    assert meta["pages"] == 1
    assert "HTTPError" in meta["error"]


def test_build_report_includes_per_wallet_no_copy_coverage(monkeypatch) -> None:
    scorecard = {
        "window": {"start_ts": 1000, "end_ts": 2000},
        "volume_kpi": {
            "missed_window_attribution": {
                "rows": [
                    {
                        "attribution": "no_copy_signal",
                        "market_slug": "btc-updown-5m-1200",
                        "window_start_s": 1200,
                    },
                    {
                        "attribution": "no_copy_signal",
                        "market_slug": "btc-updown-5m-1500",
                        "window_start_s": 1500,
                    },
                ]
            }
        },
    }
    guard_state = {
        "active_set": {
            "members": [
                {
                    "source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "candidate_id": "candidate_a",
                    "policy_id": "policy_a",
                }
            ]
        }
    }

    def fake_load_json(path, default=None):
        return guard_state if "guard" in str(path) else scorecard

    def fake_fetch_wallet_trades(*args, **kwargs):
        return (
            [
                {"slug": "btc-updown-5m-1200", "timestamp": 1210, "proxyWallet": args[0]},
                {"slug": "btc-updown-5m-1800", "timestamp": 1810, "proxyWallet": args[0]},
            ],
            {"pages": 1, "truncated": False},
        )

    monkeypatch.setattr(report, "load_json", fake_load_json)
    monkeypatch.setattr(report, "load_fresh_scorecard", lambda _path: scorecard)
    monkeypatch.setattr(report, "_fetch_wallet_trades", fake_fetch_wallet_trades)

    args = type(
        "Args",
        (),
        {
            "scorecard": "scorecard.json",
            "guard_state": "guard.json",
            "sample_size": 30,
            "limit": 10,
            "max_pages": 1,
            "timeout_s": 1.0,
        },
    )()

    result = report.build_report(args)

    assert result["summary"]["active_set_no_copy_signal_windows_covered"] == 1
    assert result["summary"]["active_set_no_copy_signal_windows_uncovered"] == 1
    assert result["summary"]["active_set_no_copy_signal_windows_covered_starts"] == [1200]
    assert result["summary"]["active_set_no_copy_signal_windows_uncovered_starts"] == [1500]
    assert result["per_wallet_coverage"] == [
        {
            "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "candidate_id": "candidate_a",
            "policy_id": "policy_a",
            "btc_trade_windows": 2,
            "no_copy_signal_windows_covered": 1,
            "sample_no_copy_signal_windows_covered": [1200],
        }
    ]
