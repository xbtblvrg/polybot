from scripts.report_passive_at_source_holdout import build_report


def _order(index: int, *, outcome: str = "Up", price: float = 0.4) -> dict:
    condition = f"condition-{index}"
    return {
        "submitted_at": f"2026-07-{1 + index // 24:02d}T{index % 24:02d}:00:00Z",
        "market_slug": f"btc-updown-5m-{index * 300}",
        "condition_id": condition,
        "outcome": outcome,
        "limit_price": price,
        "source_wallet": "0xabc",
        "trade_decision": {"strategy_reason": "wallet_copy_passive_at_source"},
        "trade_result": {
            "order_id": "",
            "error_class": "maker_min_share_bump_exceeds_policy_cap",
        },
    }


def test_passive_holdout_deduplicates_windows_and_rules_negative_holdout() -> None:
    orders = [_order(index) for index in range(144)]
    orders.append(_order(143))
    resolutions = {
        f"condition-{index}": "UP" if index < 72 else "DOWN"
        for index in range(144)
    }

    report = build_report(
        {"orders": orders},
        resolution_by_condition=resolutions,
        resolution_by_slug={},
    )

    assert report["sample_gate"] == {
        "status": "PASS",
        "minimum_rows_per_chronological_bin": 40,
        "minimum_distinct_days_per_chronological_bin": 3,
        "development_rows": 72,
        "holdout_rows": 72,
        "distinct_days_per_bin": {"development": 3, "holdout": 3},
        "split_integrity": "DAY_BOUNDED",
    }
    assert report["counting_basis"]["candidate_attempts"] == 145
    assert report["aggregate"]["rows"] == 144
    assert report["development"]["post_fee_pnl_usd"] > 0
    assert report["chronological_holdout"]["post_fee_pnl_usd"] < 0
    assert report["verdict"] == "CLOSED_TERMINAL_NEGATIVE_FROZEN_COHORT"
    assert report["lineage_measurement_verdict"] == "NEGATIVE_CHRONOLOGICAL_HOLDOUT"
    assert report["measurement_only"] is True
    assert report["live_orders_allowed"] is False
    assert report["lane_lifecycle"] == {
        "status": "CLOSED_TERMINAL_NEGATIVE_FROZEN_COHORT",
        "terminal_reason": "passive_at_source_lane_closed",
        "closure_basis": "live CopyIntent policy now refuses passive-at-source before CLOB submit",
        "new_rows_expected": False,
        "frozen_cohort_reason": (
            "matcher intentionally includes only historical wallet_copy_passive_at_source + "
            "maker_min_share_bump_exceeds_policy_cap rows; the closed emitter cannot add rows"
        ),
    }
    assert report["next_action"] == "none; emitter closed at a034c8c8, cohort cannot grow"


def test_passive_holdout_accrues_below_minimum_bin_size() -> None:
    report = build_report(
        {"orders": [_order(index) for index in range(20)]},
        resolution_by_condition={f"condition-{index}": "UP" for index in range(20)},
        resolution_by_slug={},
    )

    assert report["sample_gate"]["status"] == "ACCRUING"
    assert report["verdict"] == "CLOSED_TERMINAL_NEGATIVE_FROZEN_COHORT"
    assert report["lineage_measurement_verdict"] == "ACCRUING_DAY_BOUNDED_SAMPLE_GATE"
