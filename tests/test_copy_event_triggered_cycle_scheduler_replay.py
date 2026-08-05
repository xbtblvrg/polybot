from scripts.report_copy_event_triggered_cycle_scheduler_replay import replay_scheduler


def test_replay_scheduler_recovers_target_window_inside_fresh_horizon():
    wave = {
        "kind": "wave_gate_attribution",
        "summary": {"total_cycles_landed": 636},
        "members": [
            {
                "source_wallet": "0xabc",
                "candidate_id": "candidate_a",
                "classification": "SOURCE_ACTIVE_BUT_NO_FRESH_CYCLE_OVERLAP",
                "source_activity": {
                    "policy_window_details": [
                        {
                            "cycle_fresh_overlap": False,
                            "event_cycle_fresh_overlap": False,
                            "first_policy_event_iso": "2026-07-14T18:58:51Z",
                            "first_policy_received_iso": "2026-07-14T18:58:51.500000Z",
                            "market_slug": "btc-updown-5m-1784055300",
                            "rows": 2,
                        }
                    ]
                },
            },
            {
                "source_wallet": "0xdef",
                "candidate_id": "candidate_b",
                "classification": "PIPELINE_NO_INTENTS_FROM_SOURCE_ACTIVITY",
                "source_activity": {
                    "policy_window_details": [
                        {
                            "cycle_fresh_overlap": False,
                            "first_policy_event_iso": "2026-07-14T18:58:51Z",
                            "first_policy_received_iso": "2026-07-14T18:58:51Z",
                            "market_slug": "btc-updown-5m-1784055300",
                        }
                    ]
                },
            },
        ],
    }
    guard_cycles = [
        {
            "cycle_started_at": __import__("datetime").datetime.fromisoformat(
                "2026-07-14T18:59:05+00:00"
            ),
            "cycle_started_at_iso": "2026-07-14T18:59:05Z",
            "source_wallet": "0xabc",
            "candidate_id": "candidate_a",
            "pid": 1,
            "cycle": 10,
        }
    ]

    payload = replay_scheduler(
        wave,
        guard_cycles,
        generated_at="2026-07-15T01:00:00Z",
        fresh_horizon_s=30.0,
        trigger_delay_s=0.0,
    )

    assert payload["status"] == "PASS_PAPER_SEED_NEXT"
    assert payload["paper_only"] is True
    assert payload["copyintent_parity_change"] is False
    assert payload["single_submitter_change"] is False
    assert payload["summary"]["target_members"] == 1
    assert payload["summary"]["target_policy_windows"] == 1
    assert payload["summary"]["recovered_source_active_but_no_fresh_cycle_overlap_windows"] == 1
    assert payload["rows"][0]["counterfactual_fresh_overlap_recovered"] is True
    assert payload["rows"][0]["actual_matching_cycle_count"] == 1


def test_replay_scheduler_rejects_trigger_after_market_close():
    wave = {
        "members": [
            {
                "source_wallet": "0xabc",
                "candidate_id": "candidate_a",
                "classification": "SOURCE_ACTIVE_BUT_NO_FRESH_CYCLE_OVERLAP",
                "source_activity": {
                    "policy_window_details": [
                        {
                            "cycle_fresh_overlap": False,
                            "first_policy_event_iso": "2026-07-14T19:25:01Z",
                            "first_policy_received_iso": "2026-07-14T19:25:01Z",
                            "market_slug": "btc-updown-5m-1784056800",
                        }
                    ]
                },
            }
        ]
    }

    payload = replay_scheduler(
        wave,
        [],
        generated_at="2026-07-15T01:00:00Z",
        fresh_horizon_s=30.0,
        trigger_delay_s=0.0,
    )

    assert payload["status"] == "FAIL_TOMBSTONE_R8"
    assert payload["summary"]["recovered_source_active_but_no_fresh_cycle_overlap_windows"] == 0
    assert payload["rows"][0]["counterfactual_fresh_overlap_recovered"] is False
