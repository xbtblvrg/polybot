import gzip
import json
from argparse import Namespace
from pathlib import Path

from scripts.report_active_set_starvation_packet import build_report


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_jsonl_gz(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_starvation_packet_flags_all_late_zero_eligible_member(tmp_path: Path) -> None:
    abandoned_wallet = "0x4444444444444444444444444444444444444444"
    guard = tmp_path / "guard.json"
    current = tmp_path / "events.jsonl"
    archives = tmp_path / "archives"
    ranking = tmp_path / "ranking.json"
    queue = tmp_path / "queue.json"
    resolutions = tmp_path / "resolutions.jsonl"
    live_ledger = tmp_path / "live_ledger.json"
    _write_json(
        guard,
        {
            "active_set": {"member_count": 2},
            "active_set_runtime": {
                "member_count": 3,
                "qualified_member_count": 3,
                "members": [
                    {
                        "candidate_id": "late",
                        "source_wallet": "0x1111111111111111111111111111111111111111",
                        "policy_id": "p",
                        "enabled": True,
                    },
                    {
                        "candidate_id": "eligible",
                        "source_wallet": "0x2222222222222222222222222222222222222222",
                        "policy_id": "p",
                        "enabled": True,
                    },
                    {
                        "candidate_id": "abandoned",
                        "source_wallet": abandoned_wallet,
                        "policy_id": "p",
                        "enabled": True,
                    },
                ],
            },
            "active_set_dataapi_poller": {
                "fetch_meta": {
                    abandoned_wallet: {
                        "freshest_buy_lag_s_by_source": {"trade:user": 49 * 3600},
                    }
                }
            },
        },
    )
    _write_jsonl_gz(
        archives / "wallet_copy_live_guard_events_20260709T000000+0000.jsonl.gz",
        [
            {
                "generated_at": "2026-07-09T07:00:00+00:00",
                "live_execution": {
                    "profit_latency_suppression": {
                        "sample_filtered_intents": [
                            {
                                "source_wallet": "0x1111111111111111111111111111111111111111",
                                "market_slug": "btc-updown-5m-1783583700",
                                "taxonomy": "window_time_gte_180s",
                                "event_ts": 1783583820.0,
                                "dataapi_first_seen_ts": 1783583835.0,
                            }
                        ],
                        "sample_passed_intents": [
                            {"source_wallet": "0x2222222222222222222222222222222222222222"}
                        ],
                    }
                },
            }
        ],
    )
    _write_jsonl(current, [])
    _write_json(
        ranking,
        {
            "selected": [
                {
                    "wallet": "0x3333333333333333333333333333333333333333",
                    "selection_reason": "fresh_corrected_probe_flow",
                }
            ]
        },
    )
    _write_json(queue, {"ranked_members": []})
    _write_jsonl(resolutions, [])
    _write_json(live_ledger, {"orders": []})
    args = Namespace(
        guard_state=guard,
        current_events=current,
        log_archives=archives,
        watch_ranking=ranking,
        queue=queue,
        resolutions=resolutions,
        live_ledger_state=live_ledger,
        output=tmp_path / "out.json",
        hours=24.0,
        toxicity_shadow_min_n=20,
        rolling_pnl_n=20,
        now="2026-07-09T08:00:00+00:00",
    )

    report = build_report(args)
    by_wallet = {row["source_wallet"]: row for row in report["members"]}

    late = by_wallet["0x1111111111111111111111111111111111111111"]
    assert late["eligible_intents_24h"] == 0
    assert late["late_window_suppression_share"] == 1.0
    assert late["demotion_candidate"] is True
    assert late["demotion_class"] == "all_late_zero_eligible"
    assert late["hours_since_last_source_trade"] == 0.05
    assert late["late_suppression_latency_attribution"]["source_trade_to_detection_lag_s_p50"] == 15.0
    assert late["late_suppression_latency_attribution"]["window_open_to_source_trade_s_p50"] == 120.0
    assert late["zero_eligible_age_hours"] == 1.0
    assert late["zero_eligible_age_basis"] == "lower_bound_no_profit_latency_pass_seen_in_scanned_guard_events"
    assert late["zero_eligible_age_is_lower_bound"] is True
    assert late["rolling_realized_pnl"]["status"] == "NO_LIVE_FILLS"
    assert late["rolling_shadow_pnl"]["status"] == "NO_RESOLVED_SAMPLE"
    assert "source_to_detection_lag_s" not in late
    assert "window_open_to_source_trade_s" not in late
    assert late["proposed_replacement"]["source_wallet"] == "0x3333333333333333333333333333333333333333"
    eligible = by_wallet["0x2222222222222222222222222222222222222222"]
    assert eligible["eligible_intents_24h"] == 1
    assert eligible["zero_eligible_age_hours"] == 0.0
    assert eligible["zero_eligible_age_basis"] == "eligible_intents_present_in_window"
    abandoned = by_wallet[abandoned_wallet]
    assert abandoned["demotion_candidate"] is True
    assert abandoned["demotion_class"] == "abandoned_source"
    assert abandoned["hours_since_last_source_trade"] == 49.0
    assert report["summary"]["demotion_candidates"] == 2
    assert report["summary"]["abandoned_source_candidates"] == 1


def test_starvation_packet_dedups_intents_and_traces_pass_to_late(tmp_path: Path) -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    guard = tmp_path / "guard.json"
    current = tmp_path / "events.jsonl"
    archives = tmp_path / "archives"
    ranking = tmp_path / "ranking.json"
    queue = tmp_path / "queue.json"
    resolutions = tmp_path / "resolutions.jsonl"
    live_ledger = tmp_path / "live_ledger.json"
    _write_json(
        guard,
        {
            "active_set_runtime": {
                "member_count": 1,
                "qualified_member_count": 1,
                "members": [
                    {
                        "candidate_id": "member",
                        "source_wallet": wallet,
                        "policy_id": "p",
                        "enabled": True,
                    }
                ],
            }
        },
    )
    _write_jsonl(
        current,
        [
            {
                "generated_at": "2026-07-09T07:00:00+00:00",
                "active_set_runtime": {
                    "set_generation_id": "active_set_gen_unit",
                    "members": [{"candidate_id": "member", "source_wallet": wallet, "enabled": True}],
                },
                "live_execution": {
                    "status": "LIVE_ARMED_DRY_RUN",
                    "orders_submitted": 0,
                    "fresh_candidate_intents": 1,
                    "new_live_candidate_intents": 0,
                    "drought_funnel": {
                        "fresh_after_toxicity_protection": 0,
                        "reject_taxonomy_counts": {"toxicity_protection": 1},
                    },
                    "profit_latency_suppression": {
                        "sample_passed_intents": [
                            {
                                "intent_id": "ci_flip",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "profit_latency_pass",
                                "event_ts": 1783580409.0,
                                "dataapi_first_seen_ts": 1783580410.2,
                                "signal_age_s": 1.2,
                                "window_time_s": 54.0,
                                "outcome": "Down",
                                "limit_price": 0.5,
                                "copy_size_usd": 2.0,
                            },
                            {
                                "intent_id": "ci_pass",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "profit_latency_pass",
                                "event_ts": 1783580415.0,
                                "dataapi_first_seen_ts": 1783580415.5,
                                "outcome": "Up",
                                "limit_price": 0.5,
                                "copy_size_usd": 2.0,
                            },
                        ]
                    },
                    "toxicity_protection": {
                        "sample_filtered_intents": [
                            {
                                "intent_id": "ci_flip",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "toxicity_protection",
                                "reject_reason": "toxicity_protection",
                            }
                        ]
                    },
                },
            },
            {
                "generated_at": "2026-07-09T07:02:00+00:00",
                "live_execution": {
                    "profit_latency_suppression": {
                        "sample_filtered_intents": [
                            {
                                "intent_id": "ci_flip",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "window_time_gte_180s",
                                "taxonomy_tags": ["window_time_gte_180s"],
                                "event_ts": 1783580409.0,
                                "dataapi_first_seen_ts": 1783580410.2,
                                "signal_age_s": 1.2,
                                "window_time_s": 134.0,
                            },
                            {
                                "intent_id": "ci_late",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580700",
                                "taxonomy": "window_time_gte_180s",
                                "taxonomy_tags": ["window_time_gte_180s"],
                                "event_ts": 1783580710.0,
                                "dataapi_first_seen_ts": 1783580712.0,
                            },
                        ]
                    }
                },
            },
            {
                "generated_at": "2026-07-09T07:03:00+00:00",
                "live_execution": {
                    "profit_latency_suppression": {
                        "sample_filtered_intents": [
                            {
                                "intent_id": "ci_flip",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "window_time_gte_180s",
                                "taxonomy_tags": ["window_time_gte_180s"],
                                "event_ts": 1783580409.0,
                                "dataapi_first_seen_ts": 1783580410.2,
                            },
                            {
                                "intent_id": "ci_late",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580700",
                                "taxonomy": "window_time_gte_180s",
                                "taxonomy_tags": ["window_time_gte_180s"],
                                "event_ts": 1783580710.0,
                                "dataapi_first_seen_ts": 1783580712.0,
                            },
                        ]
                    }
                },
            },
        ],
    )
    _write_json(ranking, {"selected": []})
    _write_json(queue, {"ranked_members": []})
    _write_json(
        live_ledger,
        {
            "orders": [
                {
                    "order_id": "live-1",
                    "source_wallet": wallet,
                    "market_slug": "btc-updown-5m-1783580400",
                    "outcome": "Down",
                    "final_status": "FILLED",
                    "limit_price": 0.5,
                    "filled_size_usd": 2.0,
                    "filled_shares": 4.0,
                    "submitted_at": "2026-07-09T07:05:00+00:00",
                }
            ]
        },
    )
    _write_jsonl(
        resolutions,
        [
            {
                "asset": "BTC",
                "condition_id": "0xabc",
                "direction": "DOWN",
                "expiry_unix_ts": 1783580700,
                "market_slug": "btc-updown-5m-1783580400",
                "window_type": "5m",
            }
        ],
    )
    args = Namespace(
        guard_state=guard,
        current_events=current,
        log_archives=archives,
        watch_ranking=ranking,
        queue=queue,
        resolutions=resolutions,
        live_ledger_state=live_ledger,
        output=tmp_path / "out.json",
        hours=24.0,
        toxicity_shadow_min_n=20,
        rolling_pnl_n=20,
        now="2026-07-09T08:00:00+00:00",
    )

    report = build_report(args)
    member = report["members"][0]

    assert member["eligible_intents_24h"] == 2
    assert member["suppressed_intents_24h"] == 1
    assert member["late_window_suppressed_intents_24h"] == 1
    assert member["unique_intents_24h"] == 3
    assert member["sample_occurrences_24h"] == 7
    assert member["taxonomy_transitions"]["pass_to_late"] == 0
    assert member["taxonomy_transitions"]["pass_to_terminal_resnapshot"] == 1
    assert member["terminal_resnapshot_after_toxicity_occurrences"] == 2
    assert member["rolling_realized_pnl"]["status"] == "PASS"
    assert member["rolling_realized_pnl"]["resolved"] == 1
    assert member["rolling_realized_pnl"]["pnl_usd"] == 2.0
    assert member["rolling_shadow_pnl"]["orders_scored"] == 2
    assert report["summary"]["pass_to_late_transitions_24h"] == 0
    assert report["summary"]["pass_to_terminal_resnapshot_24h"] == 1
    assert report["terminal_resnapshot_lifecycle_trace"]["mechanism_counts"] == {
        "downstream_toxicity_gate_then_resnapshot_late": 1
    }
    trace = report["terminal_resnapshot_lifecycle_trace"]["sample_traces"][0]
    assert trace["intent_id"] == "ci_flip"
    assert trace["mechanism"] == "downstream_toxicity_gate_then_resnapshot_late"
    assert trace["timeline"][-1]["taxonomy"] == "resnapshot_after_terminal"
    assert report["toxicity_denial_shadow_ev"]["path_attribution"]["status"] == "NOT_CONFIRMED_DIAGNOSTIC_DRY_RUN_ONLY"
    c5b = report["toxicity_denial_shadow_ev"]["c5b_real_submit_path_shadow_ev"]
    assert c5b["denied_set"]["unique_intents"] == 0
    assert c5b["decision_status"] == "REAL_SUBMIT_SAMPLE_INSUFFICIENT"
    dry_run_trace = report["toxicity_denial_shadow_ev"]["c5b_dry_run_starvation_trace"]
    assert dry_run_trace["record_count"] == 1
    assert "diagnostic-only path evidence" in dry_run_trace["label_explanation"]
    assert dry_run_trace["by_dominant_reject_class"][0]["sample_trace"]["why_pass_reached_dry_run_not_submit"] in {
        "terminal_toxicity_zeroed_fresh_intent_in_dry_run_cycle",
        "fresh_candidate_not_new_to_live_submit_path",
    }
    denied = report["toxicity_denial_shadow_ev"]["denied_set"]
    assert denied["resolved"] == 1
    assert denied["roi_pct"] == 100.0


def test_starvation_packet_reports_c5b_real_submit_subset(tmp_path: Path) -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    guard = tmp_path / "guard.json"
    current = tmp_path / "events.jsonl"
    archives = tmp_path / "archives"
    ranking = tmp_path / "ranking.json"
    queue = tmp_path / "queue.json"
    resolutions = tmp_path / "resolutions.jsonl"
    live_ledger = tmp_path / "live_ledger.json"
    _write_json(
        guard,
        {
            "active_set_runtime": {
                "member_count": 1,
                "qualified_member_count": 1,
                "members": [
                    {
                        "candidate_id": "member",
                        "source_wallet": wallet,
                        "policy_id": "p",
                        "enabled": True,
                    }
                ],
            }
        },
    )
    _write_jsonl(
        current,
        [
            {
                "generated_at": "2026-07-09T07:00:00+00:00",
                "active_set_runtime": {
                    "set_generation_id": "active_set_gen_unit",
                    "members": [{"candidate_id": "member", "source_wallet": wallet, "enabled": True}],
                },
                "live_execution": {
                    "status": "LIVE_ARMED_DRY_RUN",
                    "orders_submitted": 0,
                    "fresh_candidate_intents": 1,
                    "new_live_candidate_intents": 0,
                    "drought_funnel": {
                        "fresh_after_toxicity_protection": 0,
                        "reject_taxonomy_counts": {"window_time_gte_180s": 3, "toxicity_protection": 1},
                    },
                    "profit_latency_suppression": {
                        "sample_passed_intents": [
                            {
                                "intent_id": "ci_dry",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "profit_latency_pass",
                                "event_ts": 1783580410.0,
                                "dataapi_first_seen_ts": 1783580411.0,
                                "outcome": "Up",
                                "limit_price": 0.5,
                                "copy_size_usd": 2.0,
                            }
                        ]
                    },
                    "toxicity_protection": {
                        "sample_filtered_intents": [
                            {
                                "intent_id": "ci_dry",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "toxicity_protection",
                                "reject_reason": "toxicity_protection",
                            }
                        ]
                    },
                },
            },
            {
                "generated_at": "2026-07-09T07:05:00+00:00",
                "live_execution": {
                    "status": "LIVE_EXECUTION_SUBMITTED",
                    "orders_submitted": 1,
                    "fresh_candidate_intents": 1,
                    "new_live_candidate_intents": 1,
                    "drought_funnel": {
                        "fresh_after_toxicity_protection": 0,
                        "reject_taxonomy_counts": {"toxicity_protection": 1},
                    },
                    "profit_latency_suppression": {
                        "sample_passed_intents": [
                            {
                                "intent_id": "ci_submit",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580700",
                                "taxonomy": "profit_latency_pass",
                                "event_ts": 1783580710.0,
                                "dataapi_first_seen_ts": 1783580711.0,
                                "outcome": "Down",
                                "limit_price": 0.5,
                                "copy_size_usd": 2.0,
                            }
                        ]
                    },
                    "toxicity_protection": {
                        "sample_filtered_intents": [
                            {
                                "intent_id": "ci_submit",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580700",
                                "taxonomy": "toxicity_protection",
                                "reject_reason": "toxicity_protection",
                            }
                        ]
                    },
                },
            },
        ],
    )
    _write_json(ranking, {"selected": []})
    _write_json(queue, {"ranked_members": []})
    _write_json(live_ledger, {"orders": []})
    _write_jsonl(
        resolutions,
        [
            {
                "asset": "BTC",
                "condition_id": "0xabc",
                "direction": "DOWN",
                "expiry_unix_ts": 1783581000,
                "market_slug": "btc-updown-5m-1783580700",
                "window_type": "5m",
            }
        ],
    )
    args = Namespace(
        guard_state=guard,
        current_events=current,
        log_archives=archives,
        watch_ranking=ranking,
        queue=queue,
        resolutions=resolutions,
        live_ledger_state=live_ledger,
        output=tmp_path / "out.json",
        hours=24.0,
        toxicity_shadow_min_n=1,
        rolling_pnl_n=20,
        now="2026-07-09T08:00:00+00:00",
    )

    report = build_report(args)
    shadow = report["toxicity_denial_shadow_ev"]

    assert shadow["path_attribution"]["first_pass_live_execution_status_counts"] == {
        "LIVE_ARMED_DRY_RUN": 1,
        "LIVE_EXECUTION_SUBMITTED": 1,
    }
    c5b = shadow["c5b_real_submit_path_shadow_ev"]
    assert c5b["denied_set"]["unique_intents"] == 1
    assert c5b["denied_set"]["resolved"] == 1
    assert c5b["denied_set"]["roi_pct"] == 100.0
    assert c5b["decision_status"] == "REAL_SUBMIT_POSITIVE_SHADOW_EV_REQUIRES_FABLE_RULING"
    dry_run_trace = shadow["c5b_dry_run_starvation_trace"]
    assert dry_run_trace["record_count"] == 1
    assert "fresh_after_toxicity_protection to 0" in dry_run_trace["label_explanation"]
    assert dry_run_trace["by_dominant_reject_class"][0]["dominant_reject_class"] == "window_time_gte_180s"


def test_starvation_packet_reports_inventory_skip_lifecycle_trace(tmp_path: Path) -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    guard = tmp_path / "guard.json"
    current = tmp_path / "events.jsonl"
    archives = tmp_path / "archives"
    ranking = tmp_path / "ranking.json"
    queue = tmp_path / "queue.json"
    resolutions = tmp_path / "resolutions.jsonl"
    live_ledger = tmp_path / "live_ledger.json"
    _write_json(
        guard,
        {
            "active_set_runtime": {
                "member_count": 1,
                "qualified_member_count": 1,
                "members": [
                    {
                        "candidate_id": "member",
                        "source_wallet": wallet,
                        "policy_id": "p",
                        "enabled": True,
                    }
                ],
            }
        },
    )
    _write_jsonl(
        current,
        [
            {
                "generated_at": "2026-07-09T07:00:00+00:00",
                "live_execution": {
                    "status": "LIVE_ARMED_DRY_RUN",
                    "orders_submitted": 0,
                    "fresh_candidate_intents": 1,
                    "new_live_candidate_intents": 0,
                    "drought_funnel": {
                        "fresh_after_toxicity_protection": 1,
                        "reject_taxonomy_counts": {"window:inventory_best_ask_missing": 2},
                    },
                    "profit_latency_suppression": {
                        "sample_passed_intents": [
                            {
                                "intent_id": "ci_inventory",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "profit_latency_pass",
                                "event_ts": 1783580430.0,
                                "limit_price": 0.5,
                                "copy_size_usd": 2.0,
                                "best_ask": 0.52,
                                "source_inventory_vwap": 0.49,
                                "target_usd_at_vwap": 2.0,
                            }
                        ]
                    },
                },
            }
        ],
    )
    _write_json(ranking, {"selected": []})
    _write_json(queue, {"ranked_members": []})
    _write_jsonl(resolutions, [])
    _write_json(live_ledger, {"orders": []})
    args = Namespace(
        guard_state=guard,
        current_events=current,
        log_archives=archives,
        watch_ranking=ranking,
        queue=queue,
        resolutions=resolutions,
        live_ledger_state=live_ledger,
        output=tmp_path / "out.json",
        hours=24.0,
        toxicity_shadow_min_n=20,
        rolling_pnl_n=20,
        now="2026-07-09T08:00:00+00:00",
    )

    report = build_report(args)
    trace = report["inventory_skip_lifecycle_trace"]

    assert trace["skip_reason_counts_24h"] == {"inventory_best_ask_missing": 2}
    assert trace["skip_reason_counts_by_wallet_24h"][wallet] == {"inventory_best_ask_missing": 2}
    assert trace["recoverable_intent_estimate"] == 1
    assert trace["sample_traces"][0]["dominant_inventory_reject_class"] == "inventory_best_ask_missing"
    assert trace["sample_traces"][0]["first_pass_path_fields"]["best_ask"] == 0.52


def test_starvation_packet_embeds_inventory_skip_lifecycle_trace(tmp_path: Path) -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    guard = tmp_path / "guard.json"
    current = tmp_path / "events.jsonl"
    archives = tmp_path / "archives"
    ranking = tmp_path / "ranking.json"
    queue = tmp_path / "queue.json"
    resolutions = tmp_path / "resolutions.jsonl"
    live_ledger = tmp_path / "live_ledger.json"
    _write_json(
        guard,
        {
            "active_set_runtime": {
                "member_count": 1,
                "qualified_member_count": 1,
                "members": [
                    {
                        "candidate_id": "member",
                        "source_wallet": wallet,
                        "policy_id": "p",
                        "enabled": True,
                    }
                ],
            }
        },
    )
    _write_jsonl(
        current,
        [
            {
                "generated_at": "2026-07-09T07:00:00+00:00",
                "active_set_runtime": {
                    "set_generation_id": "active_set_gen_unit",
                    "members": [{"candidate_id": "member", "source_wallet": wallet, "enabled": True}],
                },
                "live_execution": {
                    "status": "LIVE_ARMED_DRY_RUN",
                    "orders_submitted": 0,
                    "fresh_candidate_intents": 2,
                    "new_live_candidate_intents": 0,
                    "drought_funnel": {
                        "reject_taxonomy_counts": {
                            "window:inventory_best_ask_missing": 2,
                            "window:inventory_best_ask_above_vwap_plus_buffer": 1,
                        }
                    },
                    "profit_latency_suppression": {
                        "sample_passed_intents": [
                            {
                                "intent_id": "ci_inventory",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "profit_latency_pass",
                                "event_ts": 1783580410.0,
                                "dataapi_first_seen_ts": 1783580411.0,
                                "outcome": "Up",
                                "limit_price": 0.5,
                                "copy_size_usd": 2.0,
                            }
                        ]
                    },
                    "toxicity_protection": {
                        "sample_filtered_intents": [
                            {
                                "intent_id": "ci_inventory",
                                "source_wallet": wallet,
                                "market_slug": "btc-updown-5m-1783580400",
                                "taxonomy": "toxicity_protection",
                                "reject_reason": "toxicity_protection",
                            }
                        ]
                    },
                },
            },
        ],
    )
    _write_json(ranking, {"selected": []})
    _write_json(queue, {"ranked_members": []})
    _write_json(live_ledger, {"orders": []})
    _write_jsonl(resolutions, [])
    args = Namespace(
        guard_state=guard,
        current_events=current,
        log_archives=archives,
        watch_ranking=ranking,
        queue=queue,
        resolutions=resolutions,
        live_ledger_state=live_ledger,
        output=tmp_path / "out.json",
        hours=24.0,
        toxicity_shadow_min_n=20,
        rolling_pnl_n=20,
        now="2026-07-09T08:00:00+00:00",
    )

    report = build_report(args)
    trace = report["inventory_skip_lifecycle_trace"]

    assert trace["measure_only"] is True
    assert trace["record_count"] == 1
    assert trace["recoverable_intent_estimate"] == 1
    assert trace["skip_reason_counts_24h"] == {
        "inventory_best_ask_above_vwap_plus_buffer": 1,
        "inventory_best_ask_missing": 2,
    }
    assert trace["skip_reason_counts_24h_by_wallet"][wallet]["inventory_best_ask_missing"] == 2
    assert trace["sample_traces"][0]["inventory_skip_reason"] == "inventory_best_ask_missing"
    assert trace["sample_traces"][0]["first_terminal_gate"] == "inventory_best_ask_missing"
    assert trace["sample_traces"][0]["set_generation_id"] == "active_set_gen_unit"
    assert trace["sample_traces"][0]["current_generation"] is True
    assert trace["sample_traces"][0]["first_pass_path_fields"]["fresh_candidate_intents"] == 2
