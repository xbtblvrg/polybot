from scripts.report_selected_member_guard_submit_attribution import build_selected_report


def test_report_aggregates_every_wallet_selected_since_boundary():
    day = "2026-07-22"
    since = "2026-07-22T06:32:00Z"
    wallets = ("0xaaa", "0xbbb")
    routing_rows = []
    cycles = []
    for index, wallet in enumerate(wallets):
        intent = f"ci_{index}"
        observed = 1_784_702_000.0 + index
        routing_rows.append(
            {
                "source_wallet": wallet,
                "intent_id": intent,
                "observed_ts": observed,
                "dominant_skip_reason": "eligible",
                "market_slug": "btc-updown-5m-1784700900",
            }
        )
        cycles.append(
            {
                "event": "wallet_copy_live_guard_cycle",
                "generated_at": f"2026-07-22T06:4{index}:00Z",
                "source_wallet": wallet,
                "live_execution": {
                    "candidate_intent_summary": {
                        "sample_intents": [{"intent_id": intent}],
                        "toxicity_protection": {
                            "sample_filtered_intents": [{"intent_id": intent}]
                        },
                    }
                },
            }
        )
    report = build_selected_report(
        routing_shadow={"fee_gated_measurement_rows": routing_rows},
        guard_cycles=cycles,
        execution_events=[],
        ledger={"orders": []},
        day=day,
        since_at=since,
        generated_at="2026-07-22T07:00:00Z",
    )
    assert report["status"] == "PASS"
    assert report["selected_wallets"] == ["0xaaa", "0xbbb"]
    assert report["selected_policy_eligible_unique_intents"] == 2
    assert report["terminal_stage_counts"] == {"toxicity_protection": 2}
    assert {row["source_wallet"] for row in report["rows"]} == set(wallets)
