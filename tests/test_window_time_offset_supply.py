from scripts.report_window_time_offset_supply import active_roster_wallets, build_report


WALLET_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
WALLET_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _row(wallet: str, start: int, offset: float, price: float, outcome: str = "Up") -> dict:
    return {
        "event": "wallet_copy_live_profit_latency_suppression_reject",
        "reject_reason": "window_time_gte_60s" if offset >= 60 else "market_buy_precision_infeasible",
        "source_wallet": wallet,
        "market_slug": f"btc-updown-5m-{start}",
        "outcome": outcome,
        "window_time_s": offset,
        "limit_price": price,
    }


def test_active_roster_prefers_enabled_runtime_members() -> None:
    guard = {
        "source_wallet": WALLET_B,
        "active_set_runtime": {
            "members": [
                {"source_wallet": WALLET_A, "enabled": True},
                {"source_wallet": WALLET_B, "enabled": False},
            ]
        },
    }
    assert active_roster_wallets(guard) == [WALLET_A]


def test_report_emits_histogram_rate_and_daily_admission_metric() -> None:
    events = [
        _row(WALLET_A, 1_000, 20, 0.26),
        _row(WALLET_A, 1_000, 30, 0.26),  # duplicate window/outcome; earliest wins
        _row(WALLET_A, 1_300, 50, 0.31),
        _row(WALLET_A, 1_600, 70, 0.30),
        _row(WALLET_A, 1_900, 100, 0.40),
        _row(WALLET_B, 2_200, 181, 0.27),
    ]
    report = build_report(events, wallets=[WALLET_A, WALLET_B], generated_at="2026-08-03T00:00:00Z")
    a = report["per_wallet"][WALLET_A]
    assert a["offset_histogram"] == {
        "lt_45s": 1,
        "45_60s": 1,
        "60_75s": 1,
        "75_90s": 0,
        "90_120s": 1,
        "120_180s": 0,
        "gte_180s": 0,
    }
    assert a["in_band_01a_windows"] == 3
    assert a["in_band_01a_within_60s_windows"] == 2
    assert a["in_band_01a_within_60s_rate"] == 0.666667
    assert a["qualifying_01a_windows_per_day_contributed"] == 144.0
    assert a["qualifying_window_count"] == 2
    assert a["span_days"] == 0.013889
    assert a["span_days_below_1"] is True
    assert a["rate_confidence"] == "LOW_CONFIDENCE_SUB_DAY_SPAN"
    assert report["ranking"][0]["wallet"] == WALLET_A
    assert report["union_qualifying_window_count"] == 2
    assert report["aggregate_qualifying_01a_windows_per_day"] == 115.2
    assert report["status"] == "LOW_CONFIDENCE_SUB_DAY_SPAN"


def test_early_signal_rejected_by_another_gate_still_counts_as_supply() -> None:
    row = _row(WALLET_A, 1_000, 40, 0.27)
    row["event"] = "wallet_copy_live_profit_latency_suppression_reject"
    row["reject_reason"] = "market_buy_precision_infeasible"
    report = build_report([row], wallets=[WALLET_A], generated_at="2026-08-03T00:00:00Z")
    assert report["per_wallet"][WALLET_A]["in_band_01a_within_60s_windows"] == 1
    assert report["per_wallet"][WALLET_A]["qualifying_01a_windows_per_day_contributed"] == 288.0


def test_corpus_shaped_live_order_uses_submitted_at_offset() -> None:
    # Verbatim field subset from a production wallet_copy_live_order JSONL row.
    row = {
        "event": "wallet_copy_live_order",
        "source_wallet": "0x9412cdfc1e3171e1aabb013d0f494986445d0cd0",
        "market_slug": "btc-updown-5m-1783190700",
        "outcome": "Down",
        "limit_price": 0.3,
        "submitted_at": "2026-07-04T18:45:26.146000+00:00",
        "final_status": "REJECTED",
    }
    report = build_report([row], wallets=[row["source_wallet"]], generated_at="2026-08-03T00:00:00Z")
    metrics = report["per_wallet"][row["source_wallet"]]
    assert metrics["offset_basis_counts"] == {"window_time_s": 0, "submitted_at": 1}
    assert metrics["offset_histogram"]["lt_45s"] == 1
    assert metrics["in_band_01a_within_60s_windows"] == 1


def test_unobserved_wallet_is_not_summed_as_zero_supply() -> None:
    report = build_report([_row(WALLET_A, 1_000, 20, 0.26)], wallets=[WALLET_A, WALLET_B], generated_at="now")
    assert report["per_wallet"][WALLET_B]["observation_status"] == "UNOBSERVED"
    assert report["per_wallet"][WALLET_B]["qualifying_01a_windows_per_day_contributed"] == "UNOBSERVED"
    assert report["unobserved_wallets"] == [WALLET_B]
    assert report["aggregate_qualifying_01a_windows_per_day"] == 288.0
    assert report["status"] == "PARTIAL_COVERAGE_LOW_CONFIDENCE_SUB_DAY_SPAN"


def test_pre_window_offsets_keep_real_skew_but_drop_bad_join() -> None:
    kept = _row(WALLET_A, 1_000, -45, 0.26)
    dropped = _row(WALLET_A, 1_300, -301, 0.26)
    report = build_report([kept, dropped], wallets=[WALLET_A], generated_at="now")
    assert report["per_wallet"][WALLET_A]["observed_unique_windows"] == 1


def test_union_counts_same_window_once_across_wallets() -> None:
    events = [
        _row(WALLET_A, 1_000, 20, 0.26),
        _row(WALLET_B, 1_000, 30, 0.27),
    ]
    report = build_report(events, wallets=[WALLET_A, WALLET_B], generated_at="now")
    assert report["per_wallet"][WALLET_A]["qualifying_window_count"] == 1
    assert report["per_wallet"][WALLET_B]["qualifying_window_count"] == 1
    assert report["union_qualifying_window_count"] == 1
    assert report["union_qualifying_01a_windows_per_day"] == 288.0
