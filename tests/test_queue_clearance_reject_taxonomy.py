from collections import Counter

from scripts.build_queue_clearance_gaps import _prospective_reject_taxonomy


def test_prospective_reject_taxonomy_separates_requested_cells() -> None:
    taxonomy = _prospective_reject_taxonomy(
        Counter(
            {
                "no_ask_liquidity": 6,
                "insufficient_depth_within_slippage_cap": 2,
                "book_not_found_or_closed": 10,
                "api_latency_exceeded": 1,
                "price_above_slippage_cap": 1,
            }
        )
    )

    assert taxonomy["counts"] == {
        "no_ask": 6,
        "slippage": 2,
        "env": 10,
        "latency": 1,
        "price": 1,
        "other": 0,
    }
    assert taxonomy["environment_only_dominates_all_rejects"] is False
    assert taxonomy["dominant_all_reject_category"] == "env"
    assert taxonomy["dominant_attributable_reject_category"] == "no_ask"
    assert taxonomy["shares_of_all_rejects"]["env"] == 0.5
    assert taxonomy["shares_of_attributable_rejects"]["no_ask"] == 0.6
