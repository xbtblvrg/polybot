from scripts.report_live_method_supply_packet import SOURCE_WALLET, build_packet


def test_method_supply_packet_fails_closed_until_every_frozen_gate_passes() -> None:
    router = {
        "enabled_wallets": [SOURCE_WALLET],
        "current_cohort": {
            "per_wallet_counts": {},
            "proxy_source_aliases": {},
            "ambiguous_proxy_aliases": {},
            "identity_market_outcome_parity_violations": 0,
        },
    }
    deadman = {
        "policy_choke": {
            "actuator": {
                "candidate_evidence": {
                    "status": "RUNG_C_FULL_POOL_SWEEP",
                    "candidate_count": 149,
                    "eligible_count": 0,
                    "refusal_counts": {"f2": 149},
                },
                "quality_bars_unchanged": True,
            }
        }
    }
    checks = {
        "copyintent_parity": True,
        "distinct_windows_gte_10": True,
        "positive_post_fee_chronological_holdout": True,
        "positive_post_fee_train": True,
        "prospective_executable_book_post_fee_pnl_positive": True,
        "resolved_signals_gte_200": False,
        "single_guard_only": True,
    }
    cross_exchange = {
        "lane_id": "cross-exchange",
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_gate": {"checks": checks},
        "walk_forward": {
            "train": {"positive": True},
            "chronological_holdout": {"positive": True},
        },
        "prospective_executable_book": {"positive": True, "resolved_signals": 23},
    }

    packet = build_packet(
        router=router,
        deadman=deadman,
        cross_exchange=cross_exchange,
    )

    assert packet["source_acquisition"]["status"] == "SOURCE_SILENT_32DE"
    assert packet["unchanged_bar_sweep"]["eligible_count"] == 0
    assert packet["candidate"]["promotion_checks"]["resolved_signals_gte_200"] is False
    assert packet["activation"]["status"] == "ZERO_LIVE_PACKET_EVIDENCE_GATE_CLOSED"
    assert packet["activation"]["live_mutation_allowed"] is False

    checks["resolved_signals_gte_200"] = True
    ready = build_packet(
        router=router,
        deadman=deadman,
        cross_exchange=cross_exchange,
    )
    assert ready["activation"]["status"] == "PROMOTION_PACKET_READY_FOR_FABLE"
    assert ready["activation"]["live_mutation_allowed"] is False
