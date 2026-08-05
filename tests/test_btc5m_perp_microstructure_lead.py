from argparse import Namespace

import pytest

from scripts import run_btc5m_perp_microstructure_lead as perp


def _raw(second: int, *, direction: int = 1, skew: float = 0.1):
    received = float(second)
    base = 100_000.0
    move = direction * 20.0 if second else 0.0
    return {
        "second": second,
        "received_ts": received,
        "polymarket": {
            "complete": True,
            "books": {
                "Up": {"status": "OK", "best_bid": 0.40, "best_ask": 0.41},
                "Down": {"status": "OK", "best_bid": 0.40, "best_ask": 0.41},
            },
        },
        "binance_usdm": {
            "exchange_ts": received + skew,
            "mid": base + move,
            "microprice": base + move + direction,
            "signed_aggressive_volume_3s": 9.0 * direction,
            "gross_aggressive_volume_3s": 10.0,
            "signed_aggressive_volume_1s": 3.0 * direction,
            "gross_aggressive_volume_1s": 3.2,
        },
        "bybit_linear": {
            "exchange_ts": received + skew,
            "mid": base + move,
            "microprice": base + move + direction,
            "signed_aggressive_volume_3s": 9.0 * direction,
            "gross_aggressive_volume_3s": 10.0,
            "signed_aggressive_volume_1s": 3.0 * direction,
            "gross_aggressive_volume_1s": 3.2,
        },
    }


def _spot(second: int, price: float = 100_000.0):
    return {
        "second": second,
        "clock_spread_s": 0.2,
        "prices": {"binance": price, "coinbase": price, "kraken": price},
    }


def test_perp_families_have_distinct_frozen_checksums_and_costs():
    rows = [perp._config(family) for family in perp.FAMILIES]
    assert len({checksum for _, checksum in rows}) == 3
    for config, _ in rows:
        assert config["entry_bounds"] == [0.25, 0.50]
        assert config["one_intent_per_window"] is True
        assert config["complete_window_zero_intent_deadline"] == 2
        assert config["positive_intent_max_windows"] == 6
    assert rows[0][0]["shared_raw_clock_role"] == "SOLE_WRITER"
    assert rows[0][0]["aggregate_selector_role"] == "SOLE_WRITER"
    assert all(config["shared_raw_clock_role"] == "READ_ONLY_PEER" for config, _ in rows[1:])
    assert all(config["shared_raw_cache_path"] == perp.SHARED_RAW_CACHE for config, _ in rows)


def test_frozen_shared_cache_and_aggregate_roles_fail_closed_before_io():
    with pytest.raises(RuntimeError, match="shared raw cache role"):
        perp.run_once(Namespace(
            family="perp_spot_basis_impulse_taker",
            raw_cache=perp.SHARED_RAW_CACHE,
            raw_cache_read_only=False,
            aggregate_writer=False,
        ))
    with pytest.raises(RuntimeError, match="aggregate selector role"):
        perp.run_once(Namespace(
            family="dual_perp_aggressor_consensus_taker",
            raw_cache=perp.SHARED_RAW_CACHE,
            raw_cache_read_only=False,
            aggregate_writer=False,
        ))


def test_dual_perp_signal_requires_aggressor_microprice_and_unrepriced_spot():
    opened = _raw(300)
    signaled = _raw(315)
    opened["binance_usdm"]["mid"] = opened["binance_usdm"]["microprice"] = 100_000.0
    opened["bybit_linear"]["mid"] = opened["bybit_linear"]["microprice"] = 100_000.0
    feature, blockers = perp._complete_feature(
        family="dual_perp_aggressor_consensus_taker",
        raw_rows=[opened, signaled],
        spot_rows=[_spot(300), _spot(315, 100_001.0)],
        window_start=300,
    )
    assert not blockers
    assert feature["outcome"] == "Up"
    caught_up, blockers = perp._complete_feature(
        family="dual_perp_aggressor_consensus_taker",
        raw_rows=[opened, signaled],
        spot_rows=[_spot(300), _spot(315, 100_030.0)],
        window_start=300,
    )
    assert caught_up is None
    assert blockers == ["spot_already_caught_up"]


def test_complete_feature_fails_closed_on_perpetual_clock_skew():
    opened, signaled = _raw(300), _raw(315, skew=4.0)
    feature, blockers = perp._complete_feature(
        family="dual_perp_aggressor_consensus_taker",
        raw_rows=[opened, signaled],
        spot_rows=[_spot(300), _spot(315)],
        window_start=300,
    )
    assert feature is None
    assert blockers == ["perpetual_exchange_clock_skew"]


def test_terminalization_retries_incomplete_clock_but_counts_complete_no_signal():
    assert not perp._terminalization_ready(
        elapsed_s=15,
        blockers=["complete_shared_clock_slice_missing"],
    )
    assert perp._terminalization_ready(
        elapsed_s=15,
        blockers=["dual_perp_aggressor_microprice_consensus_missing"],
    )


def test_terminal_id_is_unique_per_generation_window():
    first = perp._terminal_id("generation-a", 300)
    assert first == perp._terminal_id("generation-a", 300)
    assert first != perp._terminal_id("generation-a", 600)
    assert first != perp._terminal_id("generation-b", 300)


def test_perp_signal_maps_to_copyintent_without_parity_drift():
    config, checksum = perp._config("dual_perp_aggressor_consensus_taker")
    prereg = perp._prereg(config, checksum, "state.json")
    signal = {
        "signal_id": "signal-1",
        "condition_id": "condition-1",
        "market_slug": "btc-updown-5m-300",
        "outcome": "Up",
        "token_id": "up-token",
        "executable_price": 0.40,
        "execution_mode": "taker",
        "signal_ts": 315.0,
        "observed_ts": 315.2,
    }
    intent = perp._copy_intent(signal, checksum, prereg)
    assert intent.condition_id == signal["condition_id"]
    assert intent.market_slug == signal["market_slug"]
    assert intent.outcome == signal["outcome"]
    assert intent.token_id == signal["token_id"]
    assert intent.limit_price == signal["executable_price"]
    assert intent.copy_size_usd == 1.0
    assert intent.live_orders_allowed is False
    assert intent.metadata["generation_checksum"] == checksum
    assert intent.metadata["preregistration_checksum"] == prereg["checksum"]


def test_passive_preregistration_freezes_verified_queue_model():
    config, checksum = perp._config("dual_perp_lead_post_only")
    prereg = perp._prereg(config, checksum, "state.json")
    assert prereg["passive_fill_model_checksum"] == perp.PASSIVE_FILL_MODEL_CHECKSUM
    assert prereg["registered_before_outcome_inspection"] is True
