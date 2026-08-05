import json
from pathlib import Path

from scripts import build_exact_wallet_source_history_acquisition as subject


def test_acquisition_requires_contemporaneous_executable_ask(tmp_path: Path):
    clob = tmp_path / "clob.jsonl"
    clob.write_text(
        json.dumps(
            {
                "asset_id": "asset",
                "captured_at_s": 103.0,
                "best_ask": 0.51,
                "asks": [{"price": 0.51, "size": 10.0}],
                "bids": [{"price": 0.50, "size": 10.0}],
                "route_report": {"request_fingerprint": "route"},
            }
        )
        + "\n"
    )
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "clob_jsonl": [str(clob)],
                "alpha_decay": {
                    "sample_rows": [
                        {
                            "wallet": subject.WALLET,
                            "side": "BUY",
                            "move_slice_key": "180-240|0.50-0.75",
                            "seconds_bucket": "180-240",
                            "entry_price_band": "0.50-0.75",
                            "horizons": {
                                "2s": {
                                    "observed_ts": 103.0,
                                    "observation_lag_s": 1.0,
                                }
                            },
                            "block_ts": 100.0,
                            "fill_price": 0.50,
                            "size": 5.0,
                            "tx": "0xtx",
                            "asset_id": "asset",
                            "condition_id": "condition",
                            "market_slug": "btc-updown-5m-0",
                        }
                    ]
                },
            }
        )
    )

    payload = subject.acquire([report])

    assert payload["paper_only"] is True
    assert payload["live_mutation"] is False
    assert payload["summary"]["rows_admitted_exact_fp"] == 1
    order = payload["orders"][0]
    assert order["fill_price"] == 0.51
    assert order["wide_policy_fingerprint"] == subject.FINGERPRINT
    assert order["our_price_evidence"]["source"] == "clob_rest_book_snapshot"


def test_source_price_without_snapshot_is_refused(tmp_path: Path):
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "clob_jsonl": [],
                "alpha_decay": {
                    "sample_rows": [
                        {
                            "wallet": subject.WALLET,
                            "side": "BUY",
                            "move_slice_key": "180-240|0.50-0.75",
                            "horizons": {"2s": {"observed_ts": 103.0}},
                            "block_ts": 100.0,
                            "fill_price": 0.50,
                            "tx": "0xtx",
                            "asset_id": "asset",
                        }
                    ]
                },
            }
        )
    )

    payload = subject.acquire([report])

    assert payload["orders"] == []
    assert payload["summary"]["refused_missing_exact_snapshot"] == 1
