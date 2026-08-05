import pytest

from src.wallet_copy.venue_executability import (
    row_is_venue_executable,
    venue_discard_price_band,
    venue_discard_reason,
    venue_minimum_max_price,
)


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        ({}, "price_field_absent"),
        ({"entry_price": 0.0}, "price_field_absent"),
        ({"entry_price": -0.1}, "price_nonpositive"),
        ({"entry_price": "bad"}, "price_nonpositive"),
        ({"entry_price": 0.20}, None),
        ({"entry_price": 0.50}, None),
        ({"entry_price": 0.5001}, "price_above_venue_minimum_max_price"),
        ({"entry_price": 1.0}, "price_above_venue_minimum_max_price"),
    ],
)
def test_executability_and_reason_are_one_truth(row, reason) -> None:
    assert venue_discard_reason(row, min_order_usd=1.0) == reason
    assert row_is_venue_executable(row, min_order_usd=1.0) is (reason is None)


def test_nominal_one_reduces_to_positive_price_at_or_below_ceiling() -> None:
    ceiling = venue_minimum_max_price(1.0)
    for price in (-0.1, 0.0, 0.01, 0.2, 0.5, 0.5001, 0.9, 1.0):
        assert row_is_venue_executable(
            {"fill_price": price}, min_order_usd=1.0
        ) is (0 < price <= ceiling)


def test_higher_paper_nominal_expands_ceiling_without_constant_change() -> None:
    assert row_is_venue_executable({"fill_price": 0.9}, min_order_usd=2.5) is False
    assert row_is_venue_executable({"fill_price": 0.9}, min_order_usd=5.0) is True
    assert venue_discard_price_band(0.5001) == "0.50-0.60"
    assert venue_discard_price_band(0.95) == "0.90-1.00"


def test_taker_notional_does_not_widen_or_mutate_maker_share_branch() -> None:
    row = {"fill_price": 0.9}

    assert row_is_venue_executable(
        row, min_order_usd=1.0, order_type="maker"
    ) is False
    assert venue_minimum_max_price(1.0, order_type="maker") == 0.5
    assert row_is_venue_executable(
        row, min_order_usd=1.0, order_type="taker"
    ) is True
    assert venue_minimum_max_price(1.0, order_type="taker") == 1.0
    assert row_is_venue_executable(
        row, min_order_usd=1.0, order_type="maker"
    ) is False


def test_unknown_order_type_fails_closed() -> None:
    with pytest.raises(ValueError, match="unsupported venue order type"):
        venue_minimum_max_price(1.0, order_type="unknown")
