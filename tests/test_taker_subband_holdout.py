import pytest

from scripts.report_taker_subband_holdout import (
    _drop_k_curve,
    _leave_one_day_out,
    _pnl_concentration,
    _stats,
    build_report,
)


def _filled(index: int, *, day: int, price: float, outcome: str = "Up") -> dict:
    return {
        "submitted_at": f"2026-07-{day:02d}T{index % 24:02d}:00:00Z",
        "market_slug": f"btc-updown-5m-{day * 100000 + index * 300}",
        "condition_id": f"condition-{day}-{index}-{price}",
        "outcome": outcome,
        "limit_price": price,
        "execution_role": "taker",
        "status": "FILLED",
        "final_status": "FILLED",
        "response_fill_size_shares": 2.0,
        "response_filled_size_usd": 2.0 * price,
        "trade_result": {"execution_role": "taker", "order_id": f"0x{day}{index}"},
    }


def test_taker_subbands_use_day_bounded_positive_focus_holdout() -> None:
    orders = []
    resolutions = {}
    prices = (0.20, 0.30, 0.35, 0.45)
    for day in range(1, 7):
        for band_index, price in enumerate(prices):
            for row_index in range(20):
                index = band_index * 100 + row_index
                row = _filled(index, day=day, price=price)
                orders.append(row)
                resolutions[row["condition_id"]] = "UP" if price == 0.30 else "DOWN"

    report = build_report(
        {"orders": orders},
        resolution_by_condition=resolutions,
        resolution_by_slug={},
    )

    focus = report["subbands"]["01a_25_32"]
    assert focus["sample_gate"]["status"] == "PASS"
    assert focus["sample_gate"]["development_rows"] == 60
    assert focus["sample_gate"]["holdout_rows"] == 60
    assert focus["sample_gate"]["distinct_days_per_bin"] == {
        "development": 3,
        "holdout": 3,
    }
    assert focus["sample_gate"]["split_integrity"] == "DAY_BOUNDED"
    assert focus["chronological_holdout"]["post_fee_pnl_usd"] > 0
    assert focus["chronological_holdout"]["cost_weighted_mean_entry_price"] == 0.30
    assert focus["chronological_holdout"]["entry_price_histogram_0_01"] == {"0.30_0.31": 60}
    assert focus["chronological_holdout"]["win_rate_pct"] == 100.0
    assert focus["chronological_holdout"]["cost_weighted_win_share_pct"] == 100.0
    assert focus["chronological_holdout"]["fee_inclusive_breakeven_cost_weighted_win_share_pct"] == 30.0
    assert focus["chronological_holdout"]["breakeven_gap_pct"] == 70.0
    assert focus["chronological_holdout"]["basis_consistency"] == "PASS"
    assert focus["verdict"] == "POSITIVE_DAY_BOUNDED_HOLDOUT"
    assert report["size_ruling_request_ready"] is False
    assert report["next_action"] == "quantify no-eligible-signal and inventory-no-edge supply foreclosures"
    assert report["focus_holdout_pnl_concentration"]["positive_after_top_n_drop"] is True
    assert report["focus_holdout_pnl_concentration"]["pnl_concentration_top2_pct"] is not None
    robustness = report["focus_robustness"]
    assert [row["drop_k"] for row in robustness["development_drop_k_curve"]] == list(range(6))
    assert [row["drop_k"] for row in robustness["holdout_drop_k_curve"]] == list(range(6))
    assert robustness["holdout_leave_one_day_out"]["distinct_days"] == 3
    assert robustness["holdout_leave_one_day_out"]["all_leave_one_day_out_positive"] is True
    assert robustness["drop_2_both_halves_positive"] is True
    assert robustness["verdict"] == "ROBUST_TO_DAY_AND_TOP2_REMOVAL"
    assert report["subbands"]["01b_32_40"]["verdict"] == "NEGATIVE_DAY_BOUNDED_HOLDOUT"
    assert report["subbands"]["01c_40_50"]["verdict"] == "NEGATIVE_DAY_BOUNDED_HOLDOUT"
    assert report["subbands"]["00_below_25"]["verdict"] == "NEGATIVE_DAY_BOUNDED_HOLDOUT"
    assert report["realised_price_subbands"]["01a_25_32"]["verdict"] == (
        "POSITIVE_DAY_BOUNDED_HOLDOUT"
    )
    assert report["limit_to_realised_slippage"]["median_realised_to_limit_ratio"] == 1.0
    matrix = report["limit_band_realised_outcome"]
    assert set(matrix) == {"00_below_25", "01a_25_32", "01b_32_40", "01c_40_50"}
    for limit_band, realised_cells in matrix.items():
        assert set(realised_cells) == set(matrix)
        for realised_band, cell in realised_cells.items():
            assert cell["limit_price_band"] == limit_band
            assert cell["realised_price_band"] == realised_band
            assert cell["sample_gate"]["status"] in {"PASS", "ACCRUING"}
            for bin_key in ("aggregate", "development", "chronological_holdout"):
                assert "filled_cost_usd" in cell[bin_key]
                assert "post_fee_roi_pct" in cell[bin_key]
                assert "breakeven_gap_pct" in cell[bin_key]
    for basis_key in ("subbands", "realised_price_subbands"):
        for band in report[basis_key].values():
            for bin_key in ("aggregate", "development", "chronological_holdout"):
                stats = band[bin_key]
                assert stats["basis_consistency"] == "PASS"
                gap = stats["breakeven_gap_pct"]
                harmonic_price = stats["winner_cost_weighted_harmonic_entry_price"]
                roi = stats["post_fee_roi_pct"]
                if gap is not None and harmonic_price is not None and roi is not None:
                    assert abs(gap / 100.0 - harmonic_price * roi / 100.0) <= 1e-6


def test_taker_subband_accrues_without_minimum_rows() -> None:
    orders = [_filled(index, day=1, price=0.30) for index in range(10)]
    resolutions = {row["condition_id"]: "UP" for row in orders}

    report = build_report(
        {"orders": orders},
        resolution_by_condition=resolutions,
        resolution_by_slug={},
    )

    assert report["focus_verdict"] == "ACCRUING_DAY_BOUNDED_SAMPLE_GATE"
    assert report["size_ruling_request_ready"] is False


def test_stats_breakeven_identity_uses_actual_filled_shares_on_limit_basis() -> None:
    rows = [
        {
            "submitted_at": "2026-07-01T00:00:00Z",
            "entry_price": 0.30,
            "realised_entry_price": 0.15,
            "filled_shares": 4.0,
            "filled_cost_usd": 0.60,
            "expected_fee_usd": 0.0,
            "post_fee_pnl_usd": 3.40,
            "won": True,
        },
        {
            "submitted_at": "2026-07-01T00:05:00Z",
            "entry_price": 0.30,
            "realised_entry_price": 0.30,
            "filled_shares": 2.0,
            "filled_cost_usd": 0.60,
            "expected_fee_usd": 0.0,
            "post_fee_pnl_usd": -0.60,
            "won": False,
        },
    ]

    stats = _stats(rows, price_field="entry_price")

    assert stats["basis_consistency"] == "PASS"
    assert stats["basis_consistency_abs_error"] == 0.0
    assert abs(
        stats["breakeven_gap_pct"] / 100.0
        - stats["winner_cost_weighted_harmonic_entry_price"]
        * stats["post_fee_roi_pct"]
        / 100.0
    ) <= 1e-6


def test_top_two_drop_exposes_concentrated_positive_pnl() -> None:
    rows = [
        {"market_slug": "winner-a", "submitted_at": "2026-08-01T00:00:00Z", "post_fee_pnl_usd": 5.0},
        {"market_slug": "winner-b", "submitted_at": "2026-08-01T00:05:00Z", "post_fee_pnl_usd": 4.0},
        {"market_slug": "loser", "submitted_at": "2026-08-01T00:10:00Z", "post_fee_pnl_usd": -3.0},
    ]

    result = _pnl_concentration(rows)

    assert result["pnl_concentration_top2_pct"] == 150.0
    assert result["post_fee_pnl_after_top_n_drop_usd"] == -3.0
    assert result["positive_after_top_n_drop"] is False


def test_drop_k_and_day_jackknife_publish_signed_curves() -> None:
    rows = [
        {
            "market_slug": "day-a-winner",
            "submitted_at": "2026-08-01T00:00:00Z",
            "entry_price": 0.30,
            "filled_cost_usd": 1.0,
            "filled_shares": 1.0,
            "expected_fee_usd": 0.0,
            "post_fee_pnl_usd": 4.0,
            "won": True,
        },
        {
            "market_slug": "day-b-loser",
            "submitted_at": "2026-08-02T00:00:00Z",
            "entry_price": 0.30,
            "filled_cost_usd": 1.0,
            "filled_shares": 1.0,
            "expected_fee_usd": 0.0,
            "post_fee_pnl_usd": -1.0,
            "won": False,
        },
    ]

    curve = _drop_k_curve(rows)
    jackknife = _leave_one_day_out(rows)

    assert curve[0]["post_fee_pnl_usd"] == 3.0
    assert curve[1]["post_fee_pnl_usd"] == -1.0
    assert curve[1]["sign"] == "NON_POSITIVE"
    assert jackknife["distinct_days"] == 2
    assert jackknife["all_leave_one_day_out_positive"] is False
    assert [row["sign"] for row in jackknife["curve"]] == ["NON_POSITIVE", "POSITIVE"]


def test_realized_binary_long_roi_cannot_fall_below_minus_100_pct() -> None:
    with pytest.raises(AssertionError, match="below -100%"):
        _stats(
            [
                {
                    "submitted_at": "2026-08-01T00:00:00Z",
                    "entry_price": 0.30,
                    "filled_cost_usd": 1.0,
                    "filled_shares": 1.0,
                    "expected_fee_usd": 0.0,
                    "pnl_usd_realized": -1.01,
                    "post_fee_pnl_usd": -1.01,
                    "won": False,
                }
            ]
        )
