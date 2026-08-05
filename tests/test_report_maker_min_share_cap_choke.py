import datetime as dt

from scripts.report_maker_min_share_cap_choke import build_report


def test_report_proves_local_maker_cap_foreclosure() -> None:
    ledger = {
        "orders": [
            {
                "intent_id": "ci_a",
                "market_slug": "btc-updown-5m-1",
                "updated_at": "2026-08-02T12:00:00Z",
                "alternate_transport_attribution": {"source_wallet": "0xABC"},
                "lifecycle": [
                    {
                        "payload": {
                            "details": {
                                "error_class": "maker_min_share_bump_exceeds_policy_cap",
                                "entry_price": 0.29,
                                "maker_min_share_bump_cost_usd": 1.45,
                                "maker_min_share_effective_cap_usd": 1.0,
                                "order_id": "",
                            }
                        }
                    }
                ],
            },
            {
                "intent_id": "wrong-day",
                "updated_at": "2026-08-01T12:00:00Z",
                "error_class": "maker_min_share_bump_exceeds_policy_cap",
            },
        ]
    }

    report = build_report(ledger, day=dt.date(2026, 8, 2))

    assert report["summary"] == {
        "eligible_maker_intents": 1,
        "maker_foreclosed_intents": 1,
        "empty_venue_order_ids": 1,
        "all_observed_eligible_maker_intents_foreclosed": True,
        "verdict": "TAKER_ONLY_OR_NO_LANE_AT_EFFECTIVE_CAP",
    }
    assert report["rows"][0]["required_minus_cap_usd"] == 0.45
    assert report["rows"][0]["source_wallet"] == "0xabc"
    assert report["live_path_mutated"] is False
