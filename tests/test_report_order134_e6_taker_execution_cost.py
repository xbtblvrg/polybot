import json

import pytest

from scripts.report_order134_e6_taker_execution_cost import (
    _band,
    _snapshot_measurement,
    quoted_taker_cost,
)


def test_one_dollar_taker_cost_matches_fee_and_touch_crossing() -> None:
    cost = quoted_taker_cost(bid=0.73, ask=0.74, fee_rate=0.07)

    assert cost["shares_for_one_usd"] == pytest.approx(1 / 0.74)
    assert cost["fee_usd"] == pytest.approx(0.07 * (1 - 0.74))
    assert cost["half_spread_usd"] == pytest.approx(0.005 / 0.74)
    assert cost["total_cost_usd"] == pytest.approx(
        0.07 * (1 - 0.74) + 0.005 / 0.74
    )


def test_bands_are_non_overlapping_at_boundaries() -> None:
    assert _band(0.50) == "0.50-0.75"
    assert _band(0.749) == "0.50-0.75"
    assert _band(0.75) == "0.75-0.90"
    assert _band(0.90) == "0.90-1.00"
    assert _band(1.00) == "0.90-1.00"
    assert _band(0.49) is None


def test_invalid_book_fails_closed() -> None:
    with pytest.raises(ValueError):
        quoted_taker_cost(bid=0.8, ask=0.7)


def test_snapshot_measurement_uses_binary_market_lower_bound(tmp_path) -> None:
    path = tmp_path / "books.jsonl"
    rows = []
    for index, ask in enumerate((0.6, 0.8, 0.95)):
        for asset in range(14):
            rows.append(
                {
                    "event_type": "best_bid_ask",
                    "asset_id": f"{index}-{asset}",
                    "best_bid": ask - 0.01,
                    "best_ask": ask,
                    "captured_at_iso": "2026-08-01T00:00:00Z",
                }
            )
    path.write_text("\n".join(json.dumps(row) for row in rows))

    result = _snapshot_measurement([path])

    assert result["distinct_assets"] == 42
    assert result["distinct_binary_markets_lower_bound"] == 21
    assert result["market_sample_pass"] is True
    assert all(summary["samples"] == 14 for summary in result["by_band"].values())
