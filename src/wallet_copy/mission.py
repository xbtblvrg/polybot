"""Canonical mission contract for the wallet-copy subsystem.

This module is deliberately small and imported by audit/repair scripts so the
project objective is machine-readable in every autonomous state, not only in
docs or prompts.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


RULED_01A_ENTRY_PRICE_MIN = 0.25
RULED_01A_GATE_PROBE_PRICE_MIN = 0.26
RULED_01A_ENTRY_PRICE_MAX_EXCLUSIVE = 0.32
BEST_ASK_FLOOR_COVERED_COPY_MODELS = frozenset({"inventory", "drip"})


MISSION_CONTRACT_VERSION = 31

WALLET_COPY_MISSION_CONTRACT: dict[str, Any] = {
    "schema_version": MISSION_CONTRACT_VERSION,
    "primary_goal": "find_profitable_direction_by_copying_external_wallet_orders_on_btc_5m_markets",
    "active_market_scope": "BTC_5M_ONLY",
    "primary_profit_hypothesis": (
        "externally profitable wallets reveal cloneable order-flow direction; "
        "the bot must copy, measure, and profitability-filter those wallet orders until a "
        "profitable paper-proven direction or multi-wallet inventory posture is found"
    ),
    "active_development_objective": (
        "copy profitable external wallet order-flow as efficiently as possible, operate the "
        "approved first-live single-wallet guard after explicit operator approval, keep "
        "background and multi-wallet development paper-only, and keep repairing latency, "
        "slippage, fill, and lifecycle defects without creating a second live authority"
    ),
    "active_strategy_authority": "wallet_order_copying",
    "allowed_decision_sources": [
        "operator_provided_profitable_wallets",
        "polymarket_crypto_leaderboard_wallets",
        "wallet_history_replay",
        "paper_live_tracker_copy_efficiency",
        "cross_wallet_consensus_on_copied_orders",
        "ml_or_ai_rankers_only_when_subordinate_to_wallet_copy_evidence",
    ],
    "required_pipeline": [
        "wallet_registry",
        "history_ingest",
        "normalized_wallet_events",
        "copy_intents",
        "executable_paper_fill_model",
        "paper_order_lifecycle",
        "copyability_gate",
        "copy_efficiency_scorecard",
        "profit_engine_admission",
        "paper_live_tracker_truth",
        "guarded_live_adapter_same_copy_intents",
    ],
    "paper_live_contract": {
        "paper_first": True,
        "paper_and_live_share_copy_intent": True,
        "parity_scope": "selected_policy_eligible_copy_intents",
        "strict_source_order_1_to_1_required": False,
        "profitability_filtered_copy_required": True,
        "live_requires_explicit_operator_gate": True,
        "live_orders_allowed_default": False,
        "fallback_fills_are_live_admission_truth": False,
    },
    "current_runtime_phase_contract": {
        "phase_id": "single_wallet_live_guard_runtime",
        "description": (
            "the first single-wallet copy lane is live-admissible after paper/live admission "
            "returned PASS; the guarded live runner is the only live order authority while "
            "background wallet discovery, backup wallets, and multi-wallet inventory stay paper-only"
        ),
        "live_mode": "profitability_filtered_single_wallet_copy",
        "copy_style": "policy_filtered_not_strict_1_to_1",
        "profitability_first": True,
        "strict_source_order_1_to_1_required": False,
        "selected_intent_parity_required": True,
        "as_of": "2026-07-07T06:08:00Z",
        "primary_live_candidate": {
            "candidate_id": "runtime_auto_degrade_32de91fa20",
            "candidate_type": "SINGLE_WALLET",
            "source_wallet": "0x32de91fa203321fa7735e7854f2b1c844e71ce9d",
            "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
        },
        "active_live_set": {
            "mode": "active_set_single_guard",
            "status": "ACTIVE_PROTECTION_BOUNDED_REFILL",
            "live_set_empty_until_replacement": False,
            "empty_direction_id": "2026-07-05T20:15:00Z-fable-accounting-closed-rotate-loss",
            "empty_reason": "mechanical breach on reconciled books; no replacement promotion gate crossed",
            "refill_direction_id": "2026-07-07T06:08:00Z-fable-joint-admit-rotate",
            "replacement_rule": "keep since-topup-positive live producers, demote zero-order dead slots, and admit 06:00Z shadow-economics winners under standard protection policy",
            "target_member_count_min": 3,
            "target_member_count_max": 8,
            "sizing_decision_id": "2026-07-05T15:52:19Z-fable-expansion-two-members",
            "overnight_defensive_sizing": {
                "enabled": True,
                "direction_id": "2026-07-07T17:15:00Z-fable-overnight-defensive-sizing",
                "budget_multiplier": 0.5,
                "strong_tier_suspended": True,
                "reason": "reconciled_actual_not_producing; keep breadth while buying overnight evidence at half exposure",
                "re_expand_rule": "restore full sizing immediately when a sealed holdout plus gates pass in the morning verdict wave",
            },
            "members": [
                {
                    "candidate_id": "standby_act_a6896d11",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 1.0,
                    "fable_cap_max_order_usd": 1.0,
                    "fable_1413_defensive_cap": {
                        "direction_id": "2026-07-14T11:14Z-fable-coverage-hunger-activation",
                        "fable_cap_max_order_usd": 1.0,
                        "reason": "coverage-hunger probe cap remains $1 while broader defensive sizing is active",
                    },
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": True,
                    "status": "COVERAGE_HUNGER_PROBE_ACTIVE_FABLE_20260714T1134_LIVENESS_VERIFIED",
                    "policy": {
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "min_price": 0.01,
                        "max_price": 0.50,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 1.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-14T11:34Z-fable-global-liveness-invariant",
                        "promotion_basis": "HOT-STANDBY activation per COVERAGE-HUNGER rule at $1 probe cap; refreshed external liveness pass required before runtime selection",
                        "probation_tripwire": "rolling_loss_trigger or first-20-fills negative ROI",
                    },
                },
                {
                    "candidate_id": "standby_act_d9189593",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0xd9189593701e94574ee140295a2ff6042317813b",
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 1.0,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "AUTO_DISABLED_EXTERNAL_LIVENESS_FABLE_20260714T1134",
                    "policy": {
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "min_price": 0.01,
                        "max_price": 0.50,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 1.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-14T11:34Z-fable-global-liveness-invariant",
                        "promotion_basis": "disabled because the static mission row has no refreshed external BTC-5m liveness pass; runtime overlay may admit only a correctly verified wallet row",
                        "next_action": "refresh external Data API liveness and re-enable only after BTC-5m last trade age <24h",
                    },
                },
                {
                    "candidate_id": "cohort_alive_admit_f418d3a1",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0xf418d3a1a941292f9c8707d62a14980c5beb95a3",
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 2.5,
                    "fable_cap_max_order_usd": 2.5,
                    "fable_1232_probe_cap": {
                        "direction_id": "2026-07-14T12:32Z-fable-cohort-alive-admission",
                        "fable_cap_max_order_usd": 2.5,
                        "reason": "Fable 2026-07-16T06:42Z structural-bind branch (b): clear exchange 5-share min across the <=0.50 f418 band",
                    },
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "activate_not_before_utc": "2026-07-14T13:00:00Z",
                    "status": "DEMOTED_FABLE_SUBSTITUTE_ROTATION",
                    "policy": {
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "min_price": 0.01,
                        "max_price": 0.50,
                        "min_seconds_from_open": 0,
                        "max_seconds_from_open": 300,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 2.5,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-14T12:32Z-fable-cohort-alive-admission",
                        "promotion_basis": "admission packet from cohort alive subset: PASS_PROVEN_POSITIVE_ACTIVE_SLICE, paper_pnl_usd=842.0265, denylist_cells=0, already_active_runtime_member=false",
                        "paper_pnl_usd": 842.0265,
                        "hour_match_status": "PASS_PROVEN_POSITIVE_ACTIVE_SLICE",
                        "resolved_copyable_events": 1475,
                        "latest_trade_age_h": 0.727105,
                        "external_liveness_rule": "enabled only while refreshed BTC-5m last trade age is <24h; live guard sweep auto-disables otherwise",
                        "restart_rule": "load only on the single post-13Z managed restart carrying the Fable 12:32 five-payload set",
                        "copyintent_parity": "unchanged; live guard remains sole submitter.",
                    },
                },
                {
                    "candidate_id": "shadow_ev_readmit_c03c7cc1",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0xc03c7cc1478a750a59ce44183ede1b85e7606cd9",
                    "policy_id": "fast_wf_0.10_cap_2_all_prices_minusd_0_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 2.0,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "AUTO_DISABLED_EXTERNAL_LIVENESS_FABLE_20260714T1134",
                    "policy": {
                        "policy_id": "fast_wf_0.10_cap_2_all_prices_minusd_0_all_window",
                        "min_price": 0.01,
                        "max_price": 0.50,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 2.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-14T11:34Z-fable-global-liveness-invariant",
                        "promotion_basis": "prior shadow-EV readmission evidence is not enough without refreshed external BTC-5m liveness; disabled until last trade age <24h is verified",
                        "shadow_n": 45,
                        "shadow_roi_pct": 37.07,
                        "next_action": "refresh external Data API liveness and re-enable only after BTC-5m last trade age <24h",
                    },
                },
                {
                    "candidate_id": "bucket_conc_927f7694d215",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x927f7694de44d19a72bce76254e628d1c141d215",
                    "policy_id": "bucket_conc_0.10_cap_8_all_window_25_50",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.45,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "DEMOTED_FABLE_20260708T1800_TOXICITY_SHADOW",
                    "policy": {
                        "policy_id": "bucket_conc_0.10_cap_8_all_window_25_50",
                        "min_price": 0.25,
                        "max_price": 0.45,
                        "min_seconds_from_open": 0,
                        "max_seconds_from_open": 300,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-07T21:45:00Z-fable-bucket-concentration-readmit",
                        "promotion_basis": "OUR_FILL_evidence_today: 34 live fills +26.0pct ROI (+$15.67) in 01_25_50; toxic cells (00_00_25, 02_50_70) blocked by tightened denylist",
                        "our_fill_roi_pct": 26.0,
                        "our_fill_count": 34,
                        "our_fill_pnl_usd": 15.67,
                        "probation_tripwire": "rolling_loss_trigger hit or cell ROI turns negative over next 20 fills",
                        "demoted_at": "2026-07-08T18:00:00Z",
                        "demotion_direction_id": "2026-07-08T17:45Z-fable-r1-r2-r3-execution",
                        "demotion_reason": "R1 toxicity: 18:00Z ledger-derived day split has 76 resolved fills, -14.906758pct ROI, -22.197465 USD PnL; keep shadow measurement, no registry deletion.",
                        "running_guard_enforcement": "configs/wallet_copy/toxicity_denylist.json blocks all BUY price buckets for this source wallet until next Fable ruling or guarded restart reloads mission.py.",
                    },
                },
                {
                    "candidate_id": "bucket_conc_251c1a2541f",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x251c1a283703beed41590b0875a8dcb8ddd1541f",
                    "policy_id": "bucket_conc_0.10_cap_8_all_window_25_50",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.45,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "PAPER_WATCH_FABLE_20260708T1934_ZERO_FLOW_TOXICITY_SKIP_WITHOUT_PREJUDICE",
                    "policy": {
                        "policy_id": "bucket_conc_0.10_cap_8_all_window_25_50",
                        "min_price": 0.25,
                        "max_price": 0.45,
                        "min_seconds_from_open": 0,
                        "max_seconds_from_open": 300,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-07T21:45:00Z-fable-bucket-concentration-readmit",
                        "promotion_basis": "OUR_FILL_evidence_today: 38 live fills +16.2pct ROI (+$11.40) in 01_25_50; full-universe top score 160; toxic cell 00_00_25 blocked by tightened denylist",
                        "our_fill_roi_pct": 16.2,
                        "our_fill_count": 38,
                        "our_fill_pnl_usd": 11.40,
                        "probation_tripwire": "rolling_loss_trigger hit or cell ROI turns negative over next 20 fills",
                        "parked_at": "2026-07-08T19:34:00Z",
                        "park_direction_id": "2026-07-08T19:34Z-fable-rotation-ruling",
                        "park_reason": "zero_flow_toxicity_skip; n=0 after readmit so park without prejudice and keep paper watch evidence.",
                    },
                },
                {
                    "candidate_id": "rtds_live_band_e2542ab40de0",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0",
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 5.0,
                    "drip_max_tranche_usd": 2.5,
                    "max_price": 0.50,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": True,
                    "status": "FABLE_1134_LIVING_QUEUE_REFILL_ADMITTED",
                    "policy": {
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "min_price": 0.01,
                        "max_price": 0.50,
                        "min_seconds_from_open": 0,
                        "max_seconds_from_open": 300,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 5.0,
                        "drip_max_tranche_usd": 2.5,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-14T11:34Z-fable-global-liveness-invariant",
                        "promotion_basis": "top clearance-ready queue refill with refreshed external BTC-5m liveness: paper_pnl_usd=2.246536, copyable_buy_events=9, candidate_clob_backed_orders=9, latest_trade_age_h<1",
                        "queue_rank": 1,
                        "queue_source": "clearance_ready",
                        "paper_pnl_usd": 2.246536,
                        "copyable_buy_events": 9,
                        "candidate_clob_backed_orders": 9,
                        "attributable_reject_ratio": 0.25,
                        "attributable_reject_numerator": 3,
                        "attributable_reject_denominator": 12,
                        "nominal_max_order_usd_before_defensive_sizing": 5.0,
                        "runtime_max_order_usd_after_defensive_sizing": 2.5,
                        "external_liveness_rule": "enabled only while refreshed BTC-5m last trade age is <24h; live guard sweep auto-disables otherwise",
                        "copyintent_parity": "unchanged; live guard remains sole submitter.",
                    },
                },
                {
                    "candidate_id": "rtds_live_band_4cce12f40f59",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x141d08cb2efe0b57ee1d7d7d4f524cce12f40f59",
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 5.0,
                    "drip_max_tranche_usd": 2.5,
                    "max_price": 0.50,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": True,
                    "status": "FABLE_1134_LIVING_QUEUE_REFILL_ADMITTED",
                    "policy": {
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "min_price": 0.01,
                        "max_price": 0.50,
                        "min_seconds_from_open": 0,
                        "max_seconds_from_open": 300,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 5.0,
                        "drip_max_tranche_usd": 2.5,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-14T11:34Z-fable-global-liveness-invariant",
                        "promotion_basis": "living clearance-ready queue refill after refreshed external BTC-5m liveness pass; prior legacy neutralization superseded by Fable 11:34 refill requirement",
                        "queue_rank": 3,
                        "queue_source": "clearance_ready",
                        "paper_pnl_usd": 0.800504,
                        "copyable_buy_events": 22,
                        "candidate_clob_backed_orders": 22,
                        "attributable_reject_ratio": 0.12,
                        "attributable_reject_numerator": 3,
                        "attributable_reject_denominator": 25,
                        "nominal_max_order_usd_before_defensive_sizing": 5.0,
                        "runtime_max_order_usd_after_defensive_sizing": 2.5,
                        "external_liveness_rule": "enabled only while refreshed BTC-5m last trade age is <24h; live guard sweep auto-disables otherwise",
                        "copyintent_parity": "unchanged; live guard remains sole submitter.",
                        "previous_neutralization_direction_id": "2026-07-11T20:14Z-fable-legacy-set-neutralization",
                        "neutralized_at": "2026-07-11T20:14:00Z",
                        "neutralization_direction_id": "2026-07-11T20:14Z-fable-legacy-set-neutralization",
                        "neutralized_from_status": "FABLE_1934_HALF_SIZE_PIN_CLEARANCE_READY",
                        "neutralization_reason": "legacy mission.py active_live_set rows are no longer authoritative while active_set_runtime overlay carries the ruled live set; stale enabled rows previously fed fallback candidate resurrection.",
                    },
                },
                {
                    "candidate_id": "protection_refill_960bf404c1",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x960bf404c1eca257411203164357a925e33fae8d",
                    "policy_id": "protection_refill_0.10_cap_8_120_180_le_50",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.45,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "DEMOTED_FABLE_20260707_0608_DEAD_SLOT",
                    "policy": {
                        "policy_id": "protection_refill_0.10_cap_8_120_180_le_50",
                        "min_price": 0.25,
                        "max_price": 0.45,
                        "min_seconds_from_open": 120,
                        "max_seconds_from_open": 180,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-05T20:45:00Z-fable-protection-bounded-refill-now",
                        "promotion_basis": "active_set_expansion_shortlist_existing_evidence",
                        "resolved_pnl": 5840.536188,
                        "resolved_pnl_period": "week",
                        "copyable_rate_pct": 92.307692,
                        "band_replay_proxy": "active_set_expansion_shortlist.eligible_profile.best_eligible_move_slice",
                        "member_bar_band": "120-180|0.25-0.50",
                        "band_fill_sample": 33,
                        "band_expected_edge_proxy": 0.00499999,
                        "parallel_confirmation": "fast_track_replacement_960bf404c1",
                        "probation_tripwire": "rolling_loss_trigger hit or zero eligible flow while registry positives trade",
                        "demotion_direction_id": "2026-07-07T06:08:00Z-fable-joint-admit-rotate",
                        "demotion_reason": "zero orders and zero fills since 2026-07-05T12:55Z top-up; dead slot while active set is at max",
                    },
                },
                {
                    "candidate_id": "protection_refill_04a162e06d",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x04a162e06d1e82745a08b95e247bf3a965693527",
                    "policy_id": "protection_refill_0.10_cap_2_000_060_le_50",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 4.0,
                    "max_price": 0.45,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "DEMOTED_FABLE_20260709T1507_ZERO_ACCEPTED_ROTATION",
                    "policy": {
                        "policy_id": "protection_refill_0.10_cap_2_000_060_le_50",
                        "min_price": 0.25,
                        "max_price": 0.45,
                        "min_seconds_from_open": 0,
                        "max_seconds_from_open": 60,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 4.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-05T20:45:00Z-fable-protection-bounded-refill-now",
                        "promotion_basis": "active_set_expansion_shortlist_existing_evidence",
                        "resolved_pnl": 2776.085123,
                        "resolved_pnl_period": "week",
                        "copyable_rate_pct": 72.368421,
                        "band_replay_proxy": "active_set_expansion_shortlist.eligible_profile.best_eligible_move_slice",
                        "member_bar_band": "000-060|0.50-0.50",
                        "band_fill_sample": 49,
                        "band_expected_edge_proxy": 0.014387724,
                        "parallel_confirmation": "fast_track_replacement_04a162e06d",
                        "probation_tripwire": "rolling_loss_trigger hit or zero eligible flow while registry positives trade",
                        "demotion_direction_id": "2026-07-09T15:07Z-fable-soak-failure-checkpoint2",
                        "demotion_reason": "checkpoint (2) rotation evidence: 0 orders, 0 fills, 0 PnL, suppressed-only flow and no accepted orders while live volume target is failing; demote only this wallet, keep 0xad82.",
                    },
                },
                {
                    "candidate_id": "protection_refill_ba8c5fbcc5",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0xba8c5fbcc5f58b0e4ae0c1413e0413f8c803e77d",
                    "policy_id": "protection_refill_0.10_cap_8_240_300_le_50",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.45,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "DEMOTED_FABLE_20260707_0608_DEAD_SLOT",
                    "policy": {
                        "policy_id": "protection_refill_0.10_cap_8_240_300_le_50",
                        "min_price": 0.25,
                        "max_price": 0.45,
                        "min_seconds_from_open": 240,
                        "max_seconds_from_open": 300,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                        "late_window_stop_s": 10.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-05T20:45:00Z-fable-protection-bounded-refill-now",
                        "promotion_basis": "active_set_expansion_shortlist_existing_evidence",
                        "resolved_pnl": 4681.407631,
                        "resolved_pnl_period": "week",
                        "copyable_rate_pct": 73.076923,
                        "band_replay_proxy": "active_set_expansion_shortlist.eligible_profile.best_eligible_move_slice",
                        "member_bar_band": "240-300|0.25-0.50",
                        "band_fill_sample": 21,
                        "band_expected_edge_proxy": 0.029761878,
                        "parallel_confirmation": "added_by_20:45_refill_override",
                        "probation_tripwire": "rolling_loss_trigger hit or zero eligible flow while registry positives trade",
                        "demotion_direction_id": "2026-07-07T06:08:00Z-fable-joint-admit-rotate",
                        "demotion_reason": "zero orders and zero fills since 2026-07-05T12:55Z top-up; dead slot while active set is at max",
                    },
                },
                {
                    "candidate_id": "rtds_live_band_83a100cac068",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0xd97ae021645712fe5cf73139049383a100cac068",
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 4.0,
                    "max_price": 0.50,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "DEMOTED_FABLE_20260709T1200_P4B_ZERO_ELIGIBLE_WATCH",
                    "policy": {
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "min_price": 0.01,
                        "max_price": 0.50,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 4.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-07T16:22:18Z-fable-reject-ratio-gate-045",
                        "promotion_basis": "full_pool_replay_rerun_after_fable_gate_tuning",
                        "paper_pnl_usd": 10.419704,
                        "copyable_buy_events": 39,
                        "candidate_clob_backed_orders": 39,
                        "resolved_orders": 39,
                        "reject_ratio": 0.426471,
                        "max_rejected_fill_ratio": 0.45,
                        "unresolved_ratio": 0.426471,
                        "probation_tripwire": "first fill triggers same-heartbeat reconciled actual-basis rotation evaluation",
                        "demoted_at": "2026-07-09T12:00:00Z",
                        "demotion_direction_id": "2026-07-09T10:57Z-fable-p4b-execution",
                        "demotion_reason": "P4b 12:00Z starvation packet: eligible_intents_24h=0, demotion_class=all_late_zero_eligible; demote to watch tier without replacement because proposed 0x251c refill still requires shadow-EV bar.",
                        "watch_tier": True,
                    },
                },
                {
                    "candidate_id": "active_set_member_8d3a458",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x037c0f46600702e77ccb738721a78d6418d3a458",
                    "policy_id": "member_bar_0.10_cap_8_180_240_le_25",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.25,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "DEMOTED_LOSS_TRIGGER",
                    "policy": {
                        "policy_id": "member_bar_0.10_cap_8_180_240_le_25",
                        "min_price": 0.01,
                        "max_price": 0.25,
                        "min_seconds_from_open": 180,
                        "max_seconds_from_open": 240,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "demotion_direction_id": "2026-07-05T20:15:00Z-fable-accounting-closed-rotate-loss",
                        "demotion_reason": "mechanical breach on reconciled books",
                        "demotion_reconciliation_start": "2026-07-05T12:55:00Z",
                        "demotion_since_reset_pnl_usd": -20.389931,
                        "demotion_loss_trigger_usd": -16.0,
                        "direction_id": "2026-07-05T13:15:00Z-fable-member-bar",
                        "learn_profile_positive": True,
                        "active_24h_buy_events": 34,
                        "active_24h_unique_windows": 1,
                        "band_replay_proxy": "alpha_decay_2s_execution_profile",
                        "member_bar_band": "180-240|<=0.25",
                        "band_fill_sample": 50,
                        "band_copyable_rate_pct": 70.0,
                        "band_expected_edge_proxy": 0.1275,
                    },
                },
                {
                    "candidate_id": "active_set_member_2608ad88",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x3725d52f3c252e8374999cc8617292ea2608ad88",
                    "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.25,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "SUPERSEDED_BY_PROTECTION_REFILL",
                    "policy": {
                        "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                        "min_price": 0.01,
                        "max_price": 0.25,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-05T13:15:00Z-fable-member-bar",
                        "learn_profile_positive": True,
                        "active_24h_buy_events": 9,
                        "active_24h_unique_windows": 2,
                        "band_replay_proxy": "alpha_decay_2s_execution_profile",
                        "member_bar_band": "unknown_seconds|<=0.25",
                        "band_fill_sample": 21,
                        "band_copyable_rate_pct": 76.190476,
                        "band_expected_edge_proxy": 1.095,
                    },
                },
                {
                    "candidate_id": "active_set_member_ba115f4b",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x9020ed1a82a40ca9d4174f085b62619aba115f4b",
                    "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.25,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "SUPERSEDED_BY_PROTECTION_REFILL",
                    "policy": {
                        "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                        "min_price": 0.01,
                        "max_price": 0.25,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-05T14:40:00Z-fable-volume-kpi",
                        "learn_profile_positive": True,
                        "active_24h_buy_events": 93,
                        "active_24h_unique_windows": 4,
                        "band_replay_proxy": "wallet_copy_active_set_member_bar_replay",
                        "member_bar_band": "unknown_seconds|<=0.25",
                        "band_fill_sample": 49,
                        "band_copyable_rate_pct": 85.714286,
                        "band_expected_edge_proxy": 0.004489796,
                    },
                },
                {
                    "candidate_id": "active_set_member_c9caad8a",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x1faa66202ed5b3da4e807be1956c7e46c9caad8a",
                    "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.25,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "SUPERSEDED_BY_PROTECTION_REFILL",
                    "policy": {
                        "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                        "min_price": 0.01,
                        "max_price": 0.25,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-05T14:40:00Z-fable-volume-kpi",
                        "learn_profile_positive": True,
                        "active_24h_buy_events": 54,
                        "active_24h_unique_windows": 4,
                        "band_replay_proxy": "wallet_copy_active_set_member_bar_replay",
                        "member_bar_band": "unknown_seconds|<=0.25",
                        "band_fill_sample": 22,
                        "band_copyable_rate_pct": 100.0,
                        "band_expected_edge_proxy": 0.007727273,
                    },
                },
                {
                    "candidate_id": "active_set_member_315981e0",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x60b84c9f528f0da2217f2e37b73699f6315981e0",
                    "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.25,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "SUPERSEDED_BY_PROTECTION_REFILL",
                    "policy": {
                        "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                        "min_price": 0.01,
                        "max_price": 0.25,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-05T15:52:19Z-fable-expansion-two-members",
                        "learn_profile_positive": True,
                        "resolved_pnl": 17311.994117,
                        "recent_btc_5m_fills": 167,
                        "complementary_window_pct": 100.0,
                        "promotion_basis": "active_set_expansion_shortlist",
                        "probation_tripwire": "0 fills in first 20 active windows or rolling_loss_trigger hit",
                    },
                },
                {
                    "candidate_id": "active_set_member_e09819b8",
                    "candidate_type": "SINGLE_WALLET",
                    "source_wallet": "0x91aab8d0ae5c7dde7d9ea7ca49aeb985e09819b8",
                    "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                    "wallet_fraction": 0.10,
                    "max_order_usd": 8.0,
                    "max_price": 0.25,
                    "rolling_loss_trigger_usd": -16.0,
                    "enabled": False,
                    "status": "SUPERSEDED_BY_PROTECTION_REFILL",
                    "policy": {
                        "policy_id": "member_bar_0.10_cap_8_le_25_all_window",
                        "min_price": 0.01,
                        "max_price": 0.25,
                        "wallet_fraction": 0.10,
                        "max_order_usd": 8.0,
                        "min_order_usd": 1.0,
                    },
                    "summary": {
                        "direction_id": "2026-07-05T15:52:19Z-fable-expansion-two-members",
                        "learn_profile_positive": True,
                        "resolved_pnl": 9959.705809,
                        "recent_btc_5m_fills": 182,
                        "complementary_window_pct": 100.0,
                        "promotion_basis": "active_set_expansion_shortlist",
                        "probation_tripwire": "0 fills in first 20 active windows or rolling_loss_trigger hit",
                    },
                },
            ],
        },
        "profitability_filter_contract": {
            "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
            "copy_all_source_orders_required": False,
            # Mechanical 2026-07-24 rotation closure: use the same policy as
            # the guard-qualified 32de fallback; defense caps and per-member
            # protections remain authoritative at runtime.
            "selected_intents_per_cycle_cap": 10,
            "min_price": 0.0,
            "max_price": 0.50,
            "min_seconds_from_open": 0,
            "max_seconds_from_open": 300,
            "max_event_age_s": 30.0,
            "wallet_fraction": 0.10,
            "max_order_usd": 1.0,
            "min_live_order_usd": 1.0,
            "intent_source": "mission_active_live_set_protection_bounded_refill",
        },
        "live_guard": {
            "script": "scripts/run_wallet_copy_live_guard.py",
            "state": "data/research/wallet_copy_live_guard_state.json",
            "arm_state": "data/research/wallet_copy_live_execution_arm_state.json",
            "ledger_state": "data/research/wallet_copy_live_execution_state.json",
            "expected_status": "LIVE_GUARD_RUNNING",
            "expected_runtime_live_orders_allowed": True,
            "expected_runtime_paper_only": False,
        },
        "proof_state_contract": {
            "profit_engine_state_remains_paper_only": True,
            "profit_engine_live_orders_allowed_remains_false": True,
            "runtime_live_allowed_only_in_guarded_execution_arm": True,
        },
        "heartbeat_mode": "live_gate_monitor",
        "repair_policy": {
            "do_not_run_autonomous_repair_while_live_guard_healthy": True,
            "not_ok_requires_autonomous_fix_attempt": True,
            "correction_needed_requires_find_and_apply_safest_fix": True,
            "actual_live_trading_false_is_not_ok": True,
            "report_only_allowed_for_correction": False,
            "safe_fix_boundaries": [
                "do_not_place_live_orders_from_heartbeat",
                "do_not_start_second_live_runner",
                "do_not_flip_live_orders_allowed",
                "do_not_bypass_profit_strategy_operator_gates",
                "inspect_running_writer_before_starting_state_mutating_repair",
            ],
            "repair_or_backlog_when": [
                "live_guard_stopped_or_blocked",
                "actual_live_trading_false",
                "live_copy_progress_status_correction",
                "repeated_no_fresh_or_no_live_tradeable_intents",
                "proof_intent_token_or_operator_gate_blocker_present",
                "copy_trading_green_or_live_ready_flips_false",
                "new_live_rejection_pattern_requires_code_fix",
            ],
            "repair_action_order": [
                "identify_proximate_blocker_from_guard_arm_ledger_profit_strategy_state",
                "apply_code_config_or_dependency_fix_when_safe",
                "refresh_derived_state_and_run_targeted_tests",
                "if_fix_requires_live_guard_restart_report_single-runner_restart_required_instead_of_starting_duplicate",
                "if_primary_wallet_has_no_fresh_tradeable_flow_prepare_operator_approved_rotation_evidence",
            ],
        },
    },
    "layered_live_progression_contract": {
        "purpose": (
            "run wallet-copy as a layered production path: keep discovering wallets, promote the best "
            "single-wallet copy candidate to guarded live first, keep all other wallets in paper as backup "
            "candidates, and develop multi-wallet copy as a separate paper strategy until it beats the "
            "single-wallet live lane under the same CopyIntent gates"
        ),
        "primary_live_architecture": "single_wallet_copy_promotion_with_background_paper_backup_pool",
        "upgrade_live_architecture": "weighted_multi_wallet_inventory_by_window_with_multi_wallet_filter",
        "layers": [
            {
                "id": "leaderboard_wallet_scanner",
                "mode": "continuous_background",
                "goal": "scan CRYPTO WEEK and MONTH leaderboards, preserve registry wallets, and add every fetched wallet to paper observation",
                "live_authority": False,
            },
            {
                "id": "single_wallet_paper_copy_factory",
                "mode": "paper_only_until_candidate_gate_passes",
                "goal": "copy each candidate wallet in paper through CopyIntent, rank profitability plus current copyability, and keep a backup pool",
                "live_authority": False,
            },
            {
                "id": "single_wallet_live_promotion_gate",
                "mode": "first_live_candidate",
                "goal": "promote exactly one best paper-proven, CLOB-copyable single wallet/policy to guarded live as a profitability-filtered policy slice using the same CopyIntent lifecycle",
                "live_authority": True,
                "requires_explicit_operator_gate": True,
            },
            {
                "id": "background_backup_wallet_rotation",
                "mode": "paper_only_while_primary_live_runs",
                "goal": "continue copying all observed leaderboard/operator wallets in paper and maintain ranked alternates if the live wallet degrades",
                "live_authority": False,
            },
            {
                "id": "multi_wallet_paper_strategy_builder",
                "mode": "parallel_paper_research",
                "goal": "copy many wallets in paper, build weighted multi-wallet inventory, and compare against the live single-wallet baseline",
                "live_authority": False,
            },
            {
                "id": "multi_wallet_upgrade_gate",
                "mode": "upgrade_only_after_superior_paper_and_current_poll_truth",
                "goal": "promote multi-wallet copy to live only after it is more profitable or more reliable than the single-wallet lane and passes live-readiness gates",
                "live_authority": True,
                "requires_explicit_operator_gate": True,
            },
        ],
        "promotion_order": [
            "leaderboard_wallet_scanner",
            "single_wallet_paper_copy_factory",
            "single_wallet_live_promotion_gate",
            "background_backup_wallet_rotation",
            "multi_wallet_paper_strategy_builder",
            "multi_wallet_upgrade_gate",
        ],
        "non_negotiable_guards": [
            "paper runners stay paper_only with live_orders_allowed false until explicit operator gate",
            "first live path is one best single wallet, not the whole leaderboard universe",
            "live single-wallet copy is profitability-filtered and policy-sized, not a strict 1-to-1 mirror of every source order",
            "background and multi-wallet lanes remain paper-only until separately proven",
            "multi-wallet live promotion requires better or more reliable performance than the single-wallet baseline",
            "paper and live must share the same CopyIntent body, token mapping, sizing, and lifecycle ledger for selected policy-eligible intents",
        ],
    },
    "non_deviation_contract": {
        "purpose": "prevent silent drift away from BTC-5m wallet-order copy trading",
        "default_lane": "proof_led_single_wallet_until_candidate_profit_and_current_poll_truth_are_attached",
        "immediate_development_lane": "profitable_wallet_copy_efficiency",
        "target_live_architecture": "weighted_multi_wallet_inventory_by_window_with_multi_wallet_filter",
        "status_vocabulary": ["PASS", "GREEN", "WATCH", "ANALYZE", "CORRECTION", "BUG_SUSPECT"],
        "forbidden_statuses": ["HOLD"],
        "non_green_action_loop": {
            "required": True,
            "continue_until_live_ready": True,
            "success_condition": "copy_trading_green_and_bot_live_ready",
            "passive_wait_allowed": False,
            "report_only_allowed_for_not_ok_or_correction": False,
            "not_ok_requires_find_and_apply_safest_fix": True,
            "repeat_watch_requires_new_evidence": True,
            "allowed_progress_actions": [
                "safe_fix",
                "sharper_measurement",
                "code_level_backlog_with_file_function_and_verification_command",
            ],
        },
        "allowed_deviation_requires": [
            "explicit_operator_instruction_or_source_of_truth_evidence",
            "updated_mission_or_framework_documentation",
            "updated_tests_or_audit_checks",
            "persisted_state_showing_replacement_coverage",
            "no_green_by_removal_guard_pass",
        ],
        "forbidden_green_paths": [
            "disabling_or_deleting_a_failing_source",
            "hiding_fallback_fills_or_rejected_orders",
            "treating_seeded_orders_as_live_truth",
            "treating_tracker_time_replay_as_current_poll_truth",
            "declaring_strict_live_blockers_too_strict_without_paper_only_profit_evidence",
            "promoting_ML_or_AI_rankers_to_live_authority_without_wallet_copy_evidence",
        ],
    },
    "copy_efficiency_development_contract": {
        "lane_id": "profitable_wallet_copy_efficiency",
        "purpose": (
            "select profitable wallets with enough current BUY activity and copy surface, then "
            "improve paper-only CopyIntent efficiency before changing any live-admission threshold"
        ),
        "paper_only": True,
        "live_gate_relaxation_allowed": False,
        "rank_inputs": [
            "source_leaderboard_or_operator_profit",
            "current_poll_or_active_forward_buy_events",
            "clob_or_book_filled_buy_events",
            "copyability_reject_reasons",
            "freshness_le_10s_and_le_30s",
            "paper_profitability_outcomes",
        ],
        "required_repairs_before_live": [
            "current_source_path_latency",
            "slippage_shadow_profitability",
            "paper_profitability_and_validation_sample",
            "copy_intent_lifecycle_rejects",
            "paper_live_parity_ledger",
        ],
    },
    "leaderboard_observation_contract": {
        "source": "polymarket_crypto_leaderboards",
        "periods": ["WEEK", "MONTH"],
        "order_by": "PNL",
        "observation_mode": "maximize_bounded_weekly_monthly_universe",
        "pagination": "fetch_until_empty_or_max_pages",
        "default_max_pages_per_period": 20,
        "copy_all_fetched_wallets_to_registry": True,
        "preserve_previous_leaderboard_wallets": True,
        "preserve_registry_leaderboard_wallets": True,
        "paper_only": True,
        "live_orders_allowed": False,
        "result_change_required_when_not_live_ready": True,
        "change_actions": [
            "expand_or_resume_leaderboard_wallet_coverage",
            "rerank_by_current_copyability_and_paper_profit",
            "pin_best_copyable_wallets_for_current_poll_burnin",
            "rebuild_weighted_multi_wallet_inventory_search",
            "write_code_level_backlog_if_measurement_or_copy_path_blocks_progress",
        ],
    },
    "development_research_contract": {
        "page_id": "multi_wallet_copy_trader_inventory_builder",
        "purpose": (
            "build a paper-only multi-wallet copy trader that watches the widest available crypto leaderboard "
            "wallet universe, copies all observed BTC-5m BUY flow through CopyIntent, and constructs profitable "
            "weighted inventory in every eligible window"
        ),
        "target": "weighted_multi_wallet_inventory_by_window_with_multi_wallet_filter",
        "not_enough_result_policy": "initialize_change_not_idle",
        "required_outputs": [
            "leaderboard_observation_policy",
            "wallet_universe_coverage",
            "copyability_ranked_wallets",
            "single_wallet_live_promotion_candidate",
            "background_backup_wallet_pool",
            "multi_wallet_inventory_candidate",
            "paper_profitability_and_validation_gaps",
            "next_change_action",
        ],
    },
    "development_lane_limit_contract": {
        "purpose": "prevent a non-improving research or repair lane from consuming unlimited heartbeat cycles",
        "default_max_no_improvement_cycles": 3,
        "default_max_same_blocker_cycles": 3,
        "short_diagnostic_max_no_improvement_cycles": 2,
        "tracked_lanes": [
            "profitable_wallet_copy_efficiency",
            "single_wallet_best_copyable",
            "wr_repair_single_wallet",
            "multi_wallet_all_order_exact_copy",
            "weighted_wallet_inventory_by_window",
            "multi_wallet_filter_consensus",
        ],
        "progress_signals": [
            "more_current_poll_source_events",
            "more_clob_or_book_filled_copy_events",
            "lower_reject_or_fallback_count",
            "lower_runtime_event_age_p95",
            "higher_paper_resolved_orders",
            "higher_paper_wr_roi_or_validation_wr",
            "higher_multi_wallet_avg_orders_per_window",
            "higher_candidate_specific_clob_fill_rate",
            "fewer_blockers_or_status_progress",
        ],
        "limit_hit_policy": "reevaluate_rerank_rebuild_or_repair_before_repeating_lane",
        "forbidden_limit_response": [
            "repeat_same_lane_without_new_evidence",
            "remove_or_hide_the_failing_source",
            "narrow_observation_to_make_metrics_green",
            "relax_live_readiness_gate_without_paper_only_proof",
        ],
    },
    "development_program_rethink_contract": {
        "purpose": (
            "force a full research/development rethink when evidence shows the current hypothesis portfolio "
            "is no longer the actual bottleneck"
        ),
        "required_when": [
            "single_wallet_copyability_passes_but_multi_wallet_inventory_cannot_promote_it",
            "large_leaderboard_universe_still_has_copy_execution_edge_loss",
            "paper_inventory_profit_exists_but_current_poll_clob_truth_is_missing",
            "tracker_time_inventory_replay_does_not_reach_current_poll_consensus",
            "all_order_copyintent_lifecycle_has_fallback_or_reject_truth",
            "any_development_lane_hits_logical_limit",
        ],
        "required_outputs": [
            "hypothesis_statuses",
            "strategic_traps",
            "stop_doing_list",
            "next_major_change_action",
            "implementation_backlog_with_file_function_verify",
        ],
        "forbidden_response": [
            "continue_wallet_discovery_as_primary_unlock_when_copy_execution_is_the_bottleneck",
            "continue_single_wallet_proof_when_bridge_to_multi_wallet_inventory_is_the_bottleneck",
            "treat_research_replay_or_fallback_profit_as_live_admissible",
            "repeat_heartbeat_without_a_major_change_action",
        ],
    },
    "operating_workflow": [
        {
            "step": "discover_and_register_wallets",
            "source_of_truth": [
                "configs/wallet_copy/wallets.json",
                "data/research/wallet_copy_leaderboard_crypto_state.json",
            ],
            "required_output": "enabled BTC-5m wallet registry with operator and leaderboard provenance",
        },
        {
            "step": "ingest_history_and_normalize_events",
            "source_of_truth": ["data/research/wallet_copy_history_state.json"],
            "required_output": "normalized wallet events with source wallet, market window, outcome, side, price, size, and fingerprint",
        },
        {
            "step": "generate_copy_intents_and_paper_lifecycle",
            "source_of_truth": [
                "data/research/wallet_copy_paper_state.json",
                "data/research/wallet_copy_*paper_events*.jsonl",
            ],
            "required_output": "paper CopyIntent orders plus BUY/SELL/MERGE/REDEEM lifecycle accounting",
        },
        {
            "step": "measure_live_copyability",
            "source_of_truth": [
                "data/research/wallet_copy_active_hotlane_live_tracking_state.json",
                "data/research/wallet_copy_candidate_forward_live_tracking_state.json",
            ],
            "required_output": "current-poll CLOB-backed required BUY evidence with zero fallback/reject/miss for admissible candidates",
        },
        {
            "step": "rank_profit_candidates",
            "source_of_truth": [
                "data/research/wallet_copy_profit_engine_state.json",
                "data/research/wallet_copy_strategy_direction_state.json",
            ],
            "required_output": "walk-forward candidate ranking where runtime proof is attached to the profit candidate",
        },
        {
            "step": "prove_live_parity",
            "source_of_truth": [
                "src/wallet_copy/execution.py",
                "data/research/wallet_copy_profit_engine_state.json",
            ],
            "required_output": "same CopyIntent body for paper and live, plus token mapping and durable live lifecycle ledger",
        },
    ],
    "live_readiness_gates": {
        "global": {
            "paper_only_during_proof": True,
            "live_orders_allowed_during_proof": False,
            "min_wr_pct": 70.0,
            "min_validation_wr_pct": 70.0,
            "min_roi_pct": 2.0,
            "min_resolved_orders": 100,
            "min_unique_windows": 10,
            "min_avg_orders_per_window": 2.0,
            "fallback_filled_buy_copy_events": 0,
            "rejected_buy_copy_events": 0,
            "missed_buy_copy_events": 0,
            "requires_canonical_or_live_admissible_resolution": True,
        },
        "proof_led_single_wallet": {
            "min_distinct_required_buy_source_events": 10,
            "min_market_windows": 3,
            "max_event_age_p95_s": 10.0,
            "requires_candidate_attached_to_current_profit_ranking": True,
            "is_primary_first_live_path": True,
            "background_wallet_copy_must_continue": True,
        },
        "weighted_multi_wallet_inventory": {
            "min_agreeing_wallets": 2,
            "requires_current_poll_consensus": True,
            "requires_candidate_specific_clob_truth": True,
            "is_upgrade_path_not_first_live_dependency": True,
            "requires_better_or_more_reliable_than_single_wallet_baseline": True,
        },
        "execution_parity": {
            "same_copy_intent_body": True,
            "parity_scope": "selected_policy_eligible_copy_intents",
            "strict_source_order_1_to_1_required": False,
            "token_mapping_guard_required": True,
            "live_lifecycle_ledger_required": True,
            "operator_gate_required": True,
        },
    },
    "green_semantics": {
        "subsystem_pass_is_not_global_green": True,
        "copy_trading_green_requires": [
            "primary_single_wallet_copy_gate_pass",
            "source_route_status_pass",
            "candidate_scoped_current_poll_clob_truth",
            "zero_fallback_filled_buy_copy_events",
            "zero_rejected_buy_copy_events",
            "zero_missed_buy_copy_events",
        ],
        "multi_wallet_upgrade_green_requires": [
            "multi_wallet_all_order_exact_copy_pass",
            "weighted_inventory_current_poll_clob_truth",
            "better_or_more_reliable_than_single_wallet_baseline",
            "zero_fallback_filled_buy_copy_events",
            "zero_rejected_buy_copy_events",
            "zero_missed_buy_copy_events",
        ],
        "bot_green_requires": [
            "copy_trading_green",
            "live_ready_true",
            "profitability_proven_true",
            "live_readiness_gates_pass",
            "paper_live_copy_intent_parity",
            "explicit_operator_gate_before_live_submit",
        ],
        "forbidden_global_green_when": [
            "source_route_reset_or_partial",
            "research_only_resolution_dependence",
            "bounded_or_missing_current_poll_evidence",
            "candidate_profit_and_runtime_copy_truth_are_split",
            "any_live_readiness_blocker_present",
        ],
    },
    "heartbeat_contract": {
        "mode": "live_gate_monitor",
        "primary_inspection": "inspect the pinned single-wallet live guard, paper admission truth, and live execution ledger state",
        "pinned_candidate_id": "runtime_auto_degrade_32de91fa20",
        "pinned_source_wallet": "0x32de91fa203321fa7735e7854f2b1c844e71ce9d",
        "do_not_run_while_live_guard_healthy": "python3 scripts/run_wallet_copy_autonomous_repair.py",
        "must_report": [
            "live_guard_status",
            "live_execution_arm_status",
            "profitability_filtered_copy_mode",
            "strict_source_order_1_to_1_required_false",
            "live_lifecycle_ledger_summary",
            "strategy_direction",
            "pipeline_resume_coverage",
            "wallet_coverage",
            "paper_results",
            "copy_effectiveness",
            "candidate_runtime_proof",
            "live_readiness_blockers",
            "next_progress_action",
        ],
        "dont_notify_when": [
            "live_guard_running_state_unchanged",
            "runtime_live_orders_allowed_true",
            "runtime_paper_only_false",
            "no_new_live_order_action",
        ],
        "notify_when": [
            "live_guard_stopped_or_blocked",
            "new_live_submit_fill_or_reject",
            "proof_intent_token_or_operator_gate_blocker_present",
            "copy_trading_green_or_live_ready_flips_false",
            "live_admission_becomes_pass_and_guarded_rotation_is_ready",
            "multi_wallet_upgrade_material_change",
        ],
        "non_green_requires_one_progress_action": [
            "safe_fix",
            "sharper_measurement",
            "code_level_backlog_with_verification_command",
        ],
    },
    "green_requires": [
        "no_green_by_removal",
        "profitable_candidate_or_explicit_research_analyze",
        "raw_baseline_visible",
        "resolved_sample_not_too_thin",
        "unresolved_ratio_below_cap",
        "clob_backed_copyability_evidence",
        "copy_efficiency_pass",
        "paper_lifecycle_truth",
        "logs_and_jsonl_counters_present",
    ],
    "out_of_scope_as_primary_authority": [
        "SOL_live_or_SOL_only_trading",
        "direct_ML_signal_trading",
        "fair_value_signal_trading",
        "watchdog_replacement_promotion",
        "generic_non_wallet_orderflow_strategies",
    ],
}


def mission_contract() -> dict[str, Any]:
    """Return a defensive copy of the canonical wallet-copy mission."""

    return deepcopy(WALLET_COPY_MISSION_CONTRACT)


def mission_contract_check() -> dict[str, Any]:
    """Return a compact PASS/FAIL check for audit state files."""

    contract = WALLET_COPY_MISSION_CONTRACT
    required = {
        "primary_goal": "find_profitable_direction_by_copying_external_wallet_orders_on_btc_5m_markets",
        "active_market_scope": "BTC_5M_ONLY",
        "active_strategy_authority": "wallet_order_copying",
    }
    violations = [
        f"{key}={contract.get(key)!r} expected {expected!r}"
        for key, expected in required.items()
        if contract.get(key) != expected
    ]
    paper_live = contract.get("paper_live_contract") if isinstance(contract.get("paper_live_contract"), dict) else {}
    if paper_live.get("live_orders_allowed_default") is not False:
        violations.append("live_orders_allowed_default must remain false")
    if paper_live.get("paper_and_live_share_copy_intent") is not True:
        violations.append("paper and live must share CopyIntent")
    if paper_live.get("parity_scope") != "selected_policy_eligible_copy_intents":
        violations.append("paper/live parity must be scoped to selected policy-eligible CopyIntents")
    if paper_live.get("strict_source_order_1_to_1_required") is not False:
        violations.append("live copy must not require strict 1-to-1 mirroring of every source order")
    if paper_live.get("profitability_filtered_copy_required") is not True:
        violations.append("live copy must remain profitability-filtered")
    if "copy_intents" not in (contract.get("required_pipeline") or []):
        violations.append("required pipeline must include copy_intents")
    if "copy_efficiency_pass" not in (contract.get("green_requires") or []):
        violations.append("GREEN requires copy_efficiency_pass")
    runtime_phase = (
        contract.get("current_runtime_phase_contract")
        if isinstance(contract.get("current_runtime_phase_contract"), dict)
        else {}
    )
    if runtime_phase.get("phase_id") not in {
        "single_wallet_live_gate_blocked_pending_paper_admission",
        "single_wallet_live_guard_runtime",
    }:
        violations.append("current runtime phase must be the single-wallet live gate")
    if runtime_phase.get("live_mode") != "profitability_filtered_single_wallet_copy":
        violations.append("current runtime phase must be profitability-filtered single-wallet live copy")
    if runtime_phase.get("copy_style") != "policy_filtered_not_strict_1_to_1":
        violations.append("current runtime phase must be policy-filtered, not strict 1-to-1")
    if runtime_phase.get("profitability_first") is not True:
        violations.append("current runtime phase must prioritize profitability first")
    if runtime_phase.get("strict_source_order_1_to_1_required") is not False:
        violations.append("current runtime phase must not require strict source-order 1-to-1")
    if runtime_phase.get("selected_intent_parity_required") is not True:
        violations.append("selected CopyIntent parity remains required for live")
    primary_live_candidate = (
        runtime_phase.get("primary_live_candidate")
        if isinstance(runtime_phase.get("primary_live_candidate"), dict)
        else {}
    )
    if primary_live_candidate.get("candidate_type") != "SINGLE_WALLET":
        violations.append("current live runtime must stay pinned to a single-wallet candidate")
    if not primary_live_candidate.get("candidate_id") or not primary_live_candidate.get("source_wallet"):
        violations.append("current live runtime must identify the pinned candidate and source wallet")
    live_guard = runtime_phase.get("live_guard") if isinstance(runtime_phase.get("live_guard"), dict) else {}
    if live_guard.get("script") != "scripts/run_wallet_copy_live_guard.py":
        violations.append("current live runtime must be owned by run_wallet_copy_live_guard.py")
    expected_status = live_guard.get("expected_status")
    if expected_status not in {"LIVE_GUARD_BLOCKED", "LIVE_GUARD_RUNNING"}:
        violations.append("current live runtime must expect a guarded live status")
    if expected_status == "LIVE_GUARD_BLOCKED":
        if live_guard.get("expected_runtime_live_orders_allowed") is not False:
            violations.append("blocked live gate must expect runtime live_orders_allowed false")
        if live_guard.get("expected_runtime_paper_only") is not True:
            violations.append("blocked live gate must stay runtime paper_only true")
    if expected_status == "LIVE_GUARD_RUNNING":
        if live_guard.get("expected_runtime_live_orders_allowed") is not True:
            violations.append("running live guard must expect runtime live_orders_allowed true")
        if live_guard.get("expected_runtime_paper_only") is not False:
            violations.append("running live guard must expect runtime paper_only false")
    profitability_filter = (
        runtime_phase.get("profitability_filter_contract")
        if isinstance(runtime_phase.get("profitability_filter_contract"), dict)
        else {}
    )
    if profitability_filter.get("copy_all_source_orders_required") is not False:
        violations.append("live profitability filter must not require copying all source orders")
    if str(profitability_filter.get("policy_id") or "") != str(primary_live_candidate.get("policy_id") or ""):
        violations.append("live profitability filter policy must match the pinned primary candidate")
    proof_state_contract = (
        runtime_phase.get("proof_state_contract")
        if isinstance(runtime_phase.get("proof_state_contract"), dict)
        else {}
    )
    if proof_state_contract.get("profit_engine_live_orders_allowed_remains_false") is not True:
        violations.append("paper proof state must keep live_orders_allowed false after live guard starts")
    layered = (
        contract.get("layered_live_progression_contract")
        if isinstance(contract.get("layered_live_progression_contract"), dict)
        else {}
    )
    if (
        layered.get("primary_live_architecture")
        != "single_wallet_copy_promotion_with_background_paper_backup_pool"
    ):
        violations.append("primary live architecture must promote one best single-wallet copy candidate first")
    if layered.get("upgrade_live_architecture") != "weighted_multi_wallet_inventory_by_window_with_multi_wallet_filter":
        violations.append("multi-wallet architecture must remain the upgrade path")
    layered_ids = {
        str(row.get("id"))
        for row in (layered.get("layers") or [])
        if isinstance(row, dict) and row.get("id")
    }
    required_layers = {
        "leaderboard_wallet_scanner",
        "single_wallet_paper_copy_factory",
        "single_wallet_live_promotion_gate",
        "background_backup_wallet_rotation",
        "multi_wallet_paper_strategy_builder",
        "multi_wallet_upgrade_gate",
    }
    if not required_layers.issubset(layered_ids):
        violations.append("layered live progression must include scanner, single-wallet live, backup, and multi-wallet upgrade layers")
    guards = set(layered.get("non_negotiable_guards") or [])
    if not any("first live path is one best single wallet" in str(guard) for guard in guards):
        violations.append("layered live progression must keep first live path to one best single wallet")
    if not any("profitability-filtered" in str(guard) and "not a strict 1-to-1" in str(guard) for guard in guards):
        violations.append("layered live progression must encode profitability-filtered non-1-to-1 live copy")
    if not any("background and multi-wallet lanes remain paper-only" in str(guard) for guard in guards):
        violations.append("background and multi-wallet lanes must stay paper-only until separately proven")
    development_objective = str(contract.get("active_development_objective") or "")
    if "profitable" not in development_objective or "wallet order-flow" not in development_objective:
        violations.append("active development objective must prioritize profitable wallet order-flow copying")
    non_deviation = (
        contract.get("non_deviation_contract") if isinstance(contract.get("non_deviation_contract"), dict) else {}
    )
    if non_deviation.get("immediate_development_lane") != "profitable_wallet_copy_efficiency":
        violations.append("immediate development lane must prioritize profitable wallet copy efficiency")
    if non_deviation.get("target_live_architecture") != "weighted_multi_wallet_inventory_by_window_with_multi_wallet_filter":
        violations.append("target live architecture must stay wallet-copy weighted multi-wallet inventory")
    if "HOLD" not in (non_deviation.get("forbidden_statuses") or []):
        violations.append("HOLD must remain forbidden as a passive workflow status")
    action_loop = (
        non_deviation.get("non_green_action_loop")
        if isinstance(non_deviation.get("non_green_action_loop"), dict)
        else {}
    )
    if action_loop.get("required") is not True or action_loop.get("passive_wait_allowed") is not False:
        violations.append("non-green status must require active progress, not passive waiting")
    if action_loop.get("continue_until_live_ready") is not True:
        violations.append("non-green status must keep repairing until copy-trading and bot live-ready gates pass")
    leaderboard_observation = (
        contract.get("leaderboard_observation_contract")
        if isinstance(contract.get("leaderboard_observation_contract"), dict)
        else {}
    )
    if set(leaderboard_observation.get("periods") or []) != {"WEEK", "MONTH"}:
        violations.append("leaderboard observation must include WEEK and MONTH CRYPTO wallets")
    if leaderboard_observation.get("copy_all_fetched_wallets_to_registry") is not True:
        violations.append("leaderboard observation must copy all fetched wallets into the registry")
    if leaderboard_observation.get("result_change_required_when_not_live_ready") is not True:
        violations.append("non-live-ready leaderboard workflow must initialize a change action")
    development_research = (
        contract.get("development_research_contract")
        if isinstance(contract.get("development_research_contract"), dict)
        else {}
    )
    if development_research.get("page_id") != "multi_wallet_copy_trader_inventory_builder":
        violations.append("development research contract must target multi-wallet copy trader inventory")
    if development_research.get("not_enough_result_policy") != "initialize_change_not_idle":
        violations.append("development research contract must initialize change when results are insufficient")
    required_research_outputs = set(development_research.get("required_outputs") or [])
    if "single_wallet_live_promotion_candidate" not in required_research_outputs:
        violations.append("development research must report the single-wallet live promotion candidate")
    if "background_backup_wallet_pool" not in required_research_outputs:
        violations.append("development research must report the background backup wallet pool")
    lane_limits = (
        contract.get("development_lane_limit_contract")
        if isinstance(contract.get("development_lane_limit_contract"), dict)
        else {}
    )
    required_limit_lanes = {
        "profitable_wallet_copy_efficiency",
        "single_wallet_best_copyable",
        "wr_repair_single_wallet",
        "multi_wallet_all_order_exact_copy",
        "weighted_wallet_inventory_by_window",
        "multi_wallet_filter_consensus",
    }
    if int(lane_limits.get("default_max_no_improvement_cycles") or 0) <= 0:
        violations.append("development lane limits must cap no-improvement cycles")
    if int(lane_limits.get("default_max_same_blocker_cycles") or 0) <= 0:
        violations.append("development lane limits must cap same-blocker cycles")
    if set(lane_limits.get("tracked_lanes") or []) != required_limit_lanes:
        violations.append("development lane limits must cover every strategy direction lane")
    if lane_limits.get("limit_hit_policy") != "reevaluate_rerank_rebuild_or_repair_before_repeating_lane":
        violations.append("development lane limit hits must force reevaluation or repair before repeat")
    if "repeat_same_lane_without_new_evidence" not in (lane_limits.get("forbidden_limit_response") or []):
        violations.append("development lane limits must forbid repeating the same lane without new evidence")
    program_rethink = (
        contract.get("development_program_rethink_contract")
        if isinstance(contract.get("development_program_rethink_contract"), dict)
        else {}
    )
    required_rethink_outputs = set(program_rethink.get("required_outputs") or [])
    for output in (
        "hypothesis_statuses",
        "strategic_traps",
        "stop_doing_list",
        "next_major_change_action",
        "implementation_backlog_with_file_function_verify",
    ):
        if output not in required_rethink_outputs:
            violations.append(f"development program rethink must output {output}")
    required_when = set(program_rethink.get("required_when") or [])
    if "single_wallet_copyability_passes_but_multi_wallet_inventory_cannot_promote_it" not in required_when:
        violations.append("program rethink must trigger when single-wallet proof cannot promote to inventory")
    if "large_leaderboard_universe_still_has_copy_execution_edge_loss" not in required_when:
        violations.append("program rethink must trigger when wallet discovery is not the primary bottleneck")
    forbidden_rethink = set(program_rethink.get("forbidden_response") or [])
    if "repeat_heartbeat_without_a_major_change_action" not in forbidden_rethink:
        violations.append("program rethink must forbid repeating heartbeat without a major change action")
    gates = contract.get("live_readiness_gates") if isinstance(contract.get("live_readiness_gates"), dict) else {}
    global_gates = gates.get("global") if isinstance(gates.get("global"), dict) else {}
    if float(global_gates.get("min_wr_pct") or 0.0) < 70.0:
        violations.append("live readiness minimum WR must be at least 70pct")
    if float(global_gates.get("min_resolved_orders") or 0.0) < 100.0:
        violations.append("live readiness minimum resolved orders must be at least 100")
    if global_gates.get("fallback_filled_buy_copy_events") != 0:
        violations.append("fallback BUY fills must stay zero for live readiness")
    single_gates = gates.get("proof_led_single_wallet") if isinstance(gates.get("proof_led_single_wallet"), dict) else {}
    if single_gates.get("is_primary_first_live_path") is not True:
        violations.append("proof-led single wallet must remain the primary first live path")
    if single_gates.get("background_wallet_copy_must_continue") is not True:
        violations.append("background wallet copy must continue while promoting the single-wallet lane")
    multi_gates = (
        gates.get("weighted_multi_wallet_inventory")
        if isinstance(gates.get("weighted_multi_wallet_inventory"), dict)
        else {}
    )
    if multi_gates.get("is_upgrade_path_not_first_live_dependency") is not True:
        violations.append("multi-wallet inventory must be upgrade path, not first-live dependency")
    if multi_gates.get("requires_better_or_more_reliable_than_single_wallet_baseline") is not True:
        violations.append("multi-wallet live upgrade must beat or improve reliability over the single-wallet baseline")
    green_semantics = contract.get("green_semantics") if isinstance(contract.get("green_semantics"), dict) else {}
    if green_semantics.get("subsystem_pass_is_not_global_green") is not True:
        violations.append("subsystem PASS must not imply global green")
    copy_green = green_semantics.get("copy_trading_green_requires") or []
    if "primary_single_wallet_copy_gate_pass" not in copy_green:
        violations.append("copy-trading green must require primary single-wallet copy gate pass")
    if "candidate_scoped_current_poll_clob_truth" not in copy_green:
        violations.append("copy-trading green must require candidate-scoped current-poll CLOB truth")
    upgrade_green = green_semantics.get("multi_wallet_upgrade_green_requires") or []
    if "multi_wallet_all_order_exact_copy_pass" not in upgrade_green:
        violations.append("multi-wallet upgrade green must require all-order exact copy")
    if "better_or_more_reliable_than_single_wallet_baseline" not in upgrade_green:
        violations.append("multi-wallet upgrade must beat or improve reliability over the single-wallet baseline")
    bot_green = green_semantics.get("bot_green_requires") or []
    if "live_ready_true" not in bot_green or "profitability_proven_true" not in bot_green:
        violations.append("bot green must require live-ready profitable proof")
    heartbeat = contract.get("heartbeat_contract") if isinstance(contract.get("heartbeat_contract"), dict) else {}
    if heartbeat.get("mode") != "live_gate_monitor":
        violations.append("heartbeat must monitor the single-wallet live gate")
    if heartbeat.get("pinned_candidate_id") != primary_live_candidate.get("candidate_id"):
        violations.append("heartbeat pinned candidate must match current live runtime candidate")
    if heartbeat.get("pinned_source_wallet") != primary_live_candidate.get("source_wallet"):
        violations.append("heartbeat pinned wallet must match current live runtime wallet")
    if "live_guard_status" not in (heartbeat.get("must_report") or []):
        violations.append("heartbeat must report live guard status")
    if "live_guard_stopped_or_blocked" not in (heartbeat.get("notify_when") or []):
        violations.append("heartbeat must notify when the live guard stops or blocks")
    quiet_reasons = set(heartbeat.get("dont_notify_when") or [])
    if not ({"live_gate_blocked_state_unchanged", "live_guard_running_state_unchanged"} & quiet_reasons):
        violations.append("heartbeat must stay quiet on unchanged live gate cycles")

    return {
        "status": "PASS" if not violations else "FAIL",
        "contract_version": MISSION_CONTRACT_VERSION,
        "primary_goal": contract["primary_goal"],
        "active_market_scope": contract["active_market_scope"],
        "active_strategy_authority": contract["active_strategy_authority"],
        "violations": violations,
    }
