from scripts import run_btc5m_native_l2_depth_weighted_microprice_parity_stale_ask as lane


def _raw_book() -> dict:
    return {
        "timestamp": "1",
        "bids": [
            {"price": "0.40", "size": "10"},
            {"price": "0.39", "size": "20"},
            {"price": "0.38", "size": "30"},
            {"price": "0.37", "size": "1000"},
        ],
        "asks": [
            {"price": "0.42", "size": "30"},
            {"price": "0.43", "size": "20"},
            {"price": "0.44", "size": "10"},
            {"price": "0.45", "size": "1000"},
        ],
    }


def test_generation_is_unique_frozen_and_paper_only():
    assert lane.CHECKSUM == lane.base.canonical_checksum(lane.CONFIG)
    assert lane.CONFIG["features"]["top_n_levels"] == 3
    assert lane.CONFIG["forward_start_s"] == 1_785_011_700
    assert lane.CONFIG["training_cutoff_s"] == 1_785_011_100
    assert lane.CONFIG["paper_only"] is True
    assert lane.CONFIG["live_orders_allowed"] is False


def test_top3_depth_weighted_microprice_excludes_fourth_level():
    summary = lane.summarize_l2(_raw_book(), token_id="t", observed_at_s=1.0)
    assert summary["top_n_levels"] == 3
    assert len(summary["top_n_bids"]) == 3
    assert len(summary["top_n_asks"]) == 3
    assert 0.39 < summary["depth_weighted_microprice"] < 0.43


def test_choose_signal_uses_depth_weighted_complement_fair():
    up = lane.summarize_l2(_raw_book(), token_id="u", observed_at_s=1.0)
    down_raw = _raw_book()
    down_raw["bids"] = [{"price": "0.34", "size": "10"}]
    down_raw["asks"] = [{"price": "0.36", "size": "10"}]
    down = lane.summarize_l2(down_raw, token_id="d", observed_at_s=1.0)
    signal, _ = lane.choose_signal(
        markets={"BTC": {}},
        trades_by_asset={},
        snapshots=[],
        books={"BTC": {"Up": up, "Down": down}},
        now=60.0,
        elapsed=60.0,
    )
    assert signal is not None
    assert signal["fair_basis"] == "complement_top3_depth_weighted_microprice"
    assert signal["top_n_levels"] == 3
