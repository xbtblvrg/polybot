from scripts.report_live_order_reject_attribution import build_report


def _reject(reason: str, *, market: str = "btc-updown-5m-1", role: str = "taker") -> dict:
    return {
        "status": "REJECTED",
        "submitted_at": "2026-07-17T13:00:00+00:00",
        "market_slug": market,
        "intent_id": f"ci_{reason}",
        "execution_role": role,
        "expected_fee_gate": {"estimated_response_cost_usd": 1.0},
        "lifecycle": [
            {
                "message": "live order rejected or unfilled",
                "payload": {
                    "error_class": reason,
                    "error": reason,
                    "size_usd": 1.0,
                    "market_order_amount_usd": 1.23,
                },
            }
        ],
    }


def test_reject_attribution_classifies_policy_cap_and_scheduler_subset() -> None:
    live_state = {
        "orders": [
            _reject("market_buy_precision_cap_exceeded"),
            _reject("fak_no_match", market="btc-updown-5m-2"),
            {"status": "FILLED", "submitted_at": "2026-07-17T13:01:00+00:00", "market_slug": "btc-updown-5m-3"},
        ]
    }
    guard_state = {
        "pid": 123,
        "event_triggered_cycle_scheduler": {
            "source_event": {"market_slug": "btc-updown-5m-1"},
            "last_trigger": {"market_slug": "btc-updown-5m-1"},
        },
    }

    packet = build_report(live_state, guard_state, day="2026-07-17", generated_at="2026-07-17T13:20:00Z")

    summary = packet["summary"]
    assert summary["orders"] == 3
    assert summary["fills"] == 1
    assert summary["rejects"] == 2
    assert summary["dominant_class"] == "best_ask_timeout_or_no_match"
    assert summary["classes"]["price_band_or_policy_cap"]["count"] == 1
    assert summary["classes"]["best_ask_timeout_or_no_match"]["count"] == 1
    assert summary["scheduler_triggered_subset"]["rejects"] == 1
    assert summary["scheduler_triggered_subset"]["classes"]["price_band_or_policy_cap"]["count"] == 1
    assert packet["live_path_mutated"] is False


def test_reject_attribution_detects_maker_window_end_no_fill() -> None:
    live_state = {
        "orders": [
            {
                "final_status": "REJECTED",
                "submitted_at": "2026-07-17T13:05:00+00:00",
                "market_slug": "btc-updown-5m-9",
                "lifecycle": [
                    {
                        "message": "maker fallback order canceled at BTC-5m window end without a fill",
                        "payload": {"size_usd": 1.0},
                    }
                ],
            }
        ]
    }

    packet = build_report(live_state, {}, day="2026-07-17", generated_at="2026-07-17T13:20:00Z")

    assert packet["summary"]["reason_counts"]["maker_window_end_no_fill"] == 1
    assert packet["summary"]["classes"]["market_state_or_window_end"]["count"] == 1


def test_fak_no_match_analysis_links_latency_book_maker_and_same_window_fill() -> None:
    fak = _reject("fak_no_match", market="btc-updown-5m-1784252400")
    fak["order_id"] = "0xfak"
    fak["outcome"] = "Up"
    fak["parity_capsule"] = {
        "live_intent": {
            "metadata": {
                "inventory_best_ask_gate": {
                    "status": "PASS",
                    "best_ask": 0.49,
                    "book_route_winner": "direct_clob",
                    "maker_fallback_candidate": True,
                }
            }
        }
    }
    fak["lifecycle"][-1]["payload"]["entry_price"] = 0.49
    fak["lifecycle"][-1]["payload"]["wallet_copy_latency_budget"] = {
        "submit_sent_ts": 1784252347.0,
        "exchange_ack_ts": 1784252348.0,
        "hops": {"submit_sent_to_exchange_ack_s": 1.0},
    }
    maker = {
        "status": "REJECTED",
        "submitted_at": "2026-07-17T01:39:10+00:00",
        "market_slug": "btc-updown-5m-1784252400",
        "intent_id": "ci_maker",
        "order_id": "0xmaker",
        "outcome": "Up",
        "maker_fallback": {"parent_order_id": "0xfak"},
        "lifecycle": [
            {
                "message": "maker fallback order canceled at BTC-5m window end without a fill",
                "payload": {"size_usd": 1.0},
            }
        ],
    }
    filled = {
        "status": "FILLED",
        "submitted_at": "2026-07-17T01:40:00+00:00",
        "market_slug": "btc-updown-5m-1784252400",
        "outcome": "Up",
        "lifecycle": [{"payload": {"filled_size_usd": 1.0}}],
    }
    packet = build_report(
        {"orders": [fak, maker, filled]},
        {},
        day="2026-07-17",
        generated_at="2026-07-17T13:20:00Z",
    )

    analysis = packet["fak_no_match_analysis"]
    assert analysis["count"] == 1
    assert analysis["summary"]["count"] == 1
    assert analysis["summary"]["fak_no_match_rejects"] == 1
    assert analysis["summary"]["maker_fallback_engaged"] == 1
    assert analysis["summary"]["same_window_eventual_fill"] == 1
    assert analysis["summary"]["same_window_eventual_fill_count"] == 1
    assert analysis["summary"]["book_state_verdict_counts"] == {"ask_at_or_inside_limit": 1}
    row = analysis["rows"][0]
    assert row["submit_to_window_close_s"] == 353.0
    assert row["book_state_at_submit"]["displayed_size_available"] is False
    assert row["maker_fallback_link"]["linked_order_ids"] == ["0xmaker"]
    assert row["same_window_outcome"]["forgone_estimate_overstated_by_same_window_fill"] is True


def test_fak_no_match_summary_reports_unrecovered_and_race_loss() -> None:
    recovered = _reject("fak_no_match", market="btc-updown-5m-1784252400")
    recovered["order_id"] = "0xfak_recovered"
    recovered["outcome"] = "Up"
    recovered["parity_capsule"] = {
        "live_intent": {
            "metadata": {
                "inventory_best_ask_gate": {
                    "best_ask": 0.55,
                    "maker_fallback_candidate": True,
                }
            }
        }
    }
    recovered["lifecycle"][-1]["payload"]["entry_price"] = 0.49
    recovered["lifecycle"][-1]["payload"]["wallet_copy_latency_budget"] = {"submit_sent_ts": 1784252347.0}
    unrecovered_race = _reject("fak_no_match", market="btc-updown-5m-1784252700")
    unrecovered_race["order_id"] = "0xfak_race"
    unrecovered_race["outcome"] = "Down"
    unrecovered_race["lifecycle"][-1]["payload"]["size_usd"] = 1.0
    unrecovered_race["lifecycle"][-1]["payload"]["market_order_amount_usd"] = 1.0
    unrecovered_race["lifecycle"][-1]["payload"]["entry_price"] = 0.5
    unrecovered_race["lifecycle"][-1]["payload"]["wallet_copy_latency_budget"] = {"submit_sent_ts": 1784252640.0}
    unrecovered_race["parity_capsule"] = {
        "live_intent": {
            "metadata": {
                "inventory_best_ask_gate": {
                    "best_ask": 0.49,
                    "maker_fallback_candidate": False,
                }
            }
        }
    }
    fill = {
        "status": "FILLED",
        "submitted_at": "2026-07-17T01:40:00+00:00",
        "market_slug": "btc-updown-5m-1784252400",
        "outcome": "Up",
        "lifecycle": [{"payload": {"filled_size_usd": 1.0}}],
    }

    packet = build_report(
        {"orders": [recovered, unrecovered_race, fill]},
        {},
        day="2026-07-17",
        generated_at="2026-07-17T13:20:00Z",
    )

    summary = packet["fak_no_match_analysis"]["summary"]
    assert summary["count"] == 2
    assert summary["fak_no_match_rejects"] == 2
    assert summary["same_window_eventual_fill"] == 1
    assert summary["same_window_eventual_fill_count"] == 1
    assert summary["unrecovered_rows"] == 1
    assert summary["net_unrecovered_forgone_usd"] == 1.0
    assert summary["race_loss_usd"] == 1.0


def _filled(
    order_id: str,
    condition_id: str,
    *,
    side: str = "YES",
    source_wallet: str = "0x1111111111111111111111111111111111111111",
    policy_id: str = "policy_a",
    price: float = 0.6,
    shares: float = 1.0,
    maker_parent_order_id: str = "",
) -> dict:
    row = {
        "status": "FILLED",
        "final_status": "FILLED",
        "submitted_at": "2026-07-17T13:01:00+00:00",
        "market_slug": f"btc-updown-5m-{condition_id}",
        "order_id": order_id,
        "intent_id": f"ci_{order_id}",
        "condition_id": condition_id,
        "side": side,
        "outcome": "Up" if side == "YES" else "Down",
        "source_wallet": source_wallet,
        "limit_price": price,
        "requested_size_usd": price,
        "requested_shares": shares,
        "execution_role": "maker" if maker_parent_order_id else "taker",
        "source_intent": {
            "metadata": {
                "wallet_copy_policy": {"policy_id": policy_id},
            }
        },
        "trade_result": {
            "response_filled_size_usd": price,
            "response_fill_size_shares": shares,
        },
    }
    if maker_parent_order_id:
        row["maker"] = True
        row["maker_fallback"] = {"parent_order_id": maker_parent_order_id}
    return row


def test_negative_fill_pnl_analysis_compares_maker_recovery_to_direct_taker() -> None:
    parent_reject = _reject("fak_no_match", market="btc-updown-5m-loss_maker")
    parent_reject["order_id"] = "0xparent"
    orders = [
        _filled("0xdirect_loss", "loss_direct", price=0.6),
        _filled("0xdirect_win", "win_direct", price=0.6),
        parent_reject,
        _filled("0xmaker_loss", "loss_maker", price=0.6, maker_parent_order_id="0xparent"),
    ]
    resolutions = {
        "loss_direct": {"direction": "DOWN", "source": "test"},
        "win_direct": {"direction": "UP", "source": "test"},
        "loss_maker": {"direction": "DOWN", "source": "test"},
    }

    packet = build_report(
        {"orders": orders},
        {},
        day="2026-07-17",
        generated_at="2026-07-17T13:20:00Z",
        resolutions=resolutions,
    )

    analysis = packet["negative_fill_pnl_analysis"]
    assert analysis["summary"]["counts_basis"] == "resolved_only_canonical_pnl_truth_events_joined_by_order_id"
    assert analysis["summary"]["resolved_fills"] == 3
    assert analysis["summary"]["resolved_fills_joined"] == 3
    assert analysis["summary"]["negative_fills"] == 2
    assert analysis["summary"]["aggregate_by_tranche_type"]["maker_recovery_fill"]["fills"] == 1
    assert analysis["summary"]["aggregate_by_tranche_type"]["direct_taker_fill"]["fills"] == 2
    band_tranche = analysis["summary"]["aggregate_by_price_band_tranche_type"]
    assert band_tranche["02_50_70|direct_taker_fill"]["fills"] == 2
    assert band_tranche["02_50_70|direct_taker_fill"]["fix_candidate_gate"] == {
        "min_resolved_fills": 15,
        "max_roi_pct": -20.0,
        "passes": False,
    }
    comparison = analysis["summary"]["maker_recovery_vs_direct_taker"]
    assert comparison["maker_recovery_avg_pnl_usd"] == -0.6
    assert comparison["direct_taker_avg_pnl_usd"] == -0.1
    rows_by_order = {row["order_id"]: row for row in analysis["rows"]}
    assert rows_by_order["0xmaker_loss"]["parent_reject_reason"] == "fak_no_match"
    assert rows_by_order["0xmaker_loss"]["tranche_type"] == "maker_recovery_fill"


def test_negative_fill_analysis_splits_direct_and_recovery_tranches() -> None:
    fak = _reject("fak_no_match", market="btc-updown-5m-1784252400")
    fak["order_id"] = "0xfak"
    fak["outcome"] = "Up"
    recovery_fill = _filled("0xrecovery", "1784252400", price=1.0, maker_parent_order_id="0xfak")
    recovery_fill["market_slug"] = "btc-updown-5m-1784252400"
    recovery_fill["outcome"] = "Up"
    direct_fill = _filled("0xdirect", "1784252700", price=2.0, side="NO")
    direct_fill["market_slug"] = "btc-updown-5m-1784252700"
    resolutions = {
        "1784252400": {"direction": "DOWN", "source": "test"},
        "1784252700": {"direction": "UP", "source": "test"},
    }

    packet = build_report(
        {"orders": [fak, recovery_fill, direct_fill]},
        {},
        day="2026-07-17",
        generated_at="2026-07-17T13:20:00Z",
        resolutions=resolutions,
    )

    negative = packet["negative_fill_pnl_analysis"]
    assert negative["summary"]["counts_basis"] == "resolved_only_canonical_pnl_truth_events_joined_by_order_id"
    assert negative["summary"]["negative_fills"] == 2
    assert negative["summary"]["negative_pnl_usd"] == -3.0
    assert set(negative["summary"]["aggregate_by_price_band_tranche_type"]) == {
        "04_85_100|direct_taker_fill",
        "04_85_100|maker_recovery_fill",
    }
    by_type = negative["summary"]["aggregate_by_tranche_type"]
    assert by_type["maker_recovery_fill"]["negative_fills"] == 1
    assert by_type["direct_taker_fill"]["negative_fills"] == 1
    assert negative["rows"][0]["same_window_recovery_after_fak_no_match"] is True
