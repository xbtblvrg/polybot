from scripts.report_f1_rescore_registry_reconciliation import build_packet


def test_reconciliation_compares_same_regime_and_keeps_floor() -> None:
    wallet = "0x" + "a" * 40
    packet = build_packet(
        {
            "policy_choke": {
                "source_roster_drought": {
                    "candidate_evidence": {
                        "regime": "weekday",
                        "rows": [
                            {
                                "wallet": wallet,
                                "wide_policy_fingerprint": "fingerprint",
                                "paper_policy_id": "policy",
                                "policy": {
                                    "move_slice_keys": ["a", "b"],
                                },
                                "regime_evidence": {
                                    "resolved_signals": 17,
                                },
                            }
                        ],
                    }
                }
            }
        },
        {
            "wallets": [
                {
                    "wallet": wallet,
                    "slice_labels": {
                        "weekday": {
                            "label": "PROVEN-POSITIVE",
                            "resolved_trades": 1815,
                            "pnl_usd": 1882.95,
                            "roi_pct": 2.599,
                        }
                    },
                }
            ]
        },
        generated_at="2026-07-31T08:00:00+00:00",
    )

    row = packet["rows"][0]
    assert row["move_slice_key_count"] == 2
    assert row["rescore_resolved_signals"] == 17
    assert row["registry_same_regime_resolved_trades"] == 1815
    assert row["rescore_to_registry_ratio_pct"] == 0.936639
    assert row["rescore_clears_f1_200_floor"] is False
    assert packet["f1_floor_unchanged"] == 200
    assert packet["live_mutation"] is False
    assert packet["decision"] == "WIDEN_FINGERPRINT_PAPER_ONLY_KEEP_F1_FLOOR_200"
