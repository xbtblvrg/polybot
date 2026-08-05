import math
from pathlib import Path

from scripts import run_btc5m_cross_exchange_probability_edge_paper_lane as lane


def _bar(ts: int, open_price: float, close_price: float) -> list:
    return [ts * 1000, str(open_price), str(max(open_price, close_price)), str(min(open_price, close_price)), str(close_price)]


def test_walk_forward_sample_uses_frozen_past_vol_and_30_second_feature() -> None:
    start = 1_800_000_000
    rows = {}
    price = 100.0
    for ts in range(start - lane.VOL_LOOKBACK_S, start + 300):
        next_price = price * math.exp(0.0001 if ts % 2 else -0.00008)
        rows[ts] = _bar(ts, price, next_price)
        price = next_price
    rows[start] = _bar(start, 100.0, 100.01)
    rows[start + lane.SIGNAL_OFFSET_S - 1] = _bar(start + 29, 100.2, 101.0)
    rows[start + 299] = _bar(start + 299, 101.0, 102.0)

    sample = lane._sample_for_window(rows, start, require_label=True)

    assert sample is not None
    assert sample["signal_offset_s"] == 30
    assert sample["time_to_close_s"] == 270
    assert sample["signal_price"] == 101.0
    assert sample["up"] is True
    assert sample["rolling_realized_volatility"] > 0


def test_calibration_is_train_only_laplace_by_frozen_bin() -> None:
    train = [
        {"z_score": -2.0, "up": False},
        {"z_score": -2.0, "up": False},
        {"z_score": 2.0, "up": True},
        {"z_score": 2.0, "up": True},
    ]

    calibration = lane.fit_calibration(train)

    assert calibration[0] == 0.25
    assert calibration[5] == 0.75


def test_probability_signal_intent_is_guard_compatible_and_paper_only() -> None:
    signal = {
        "signal_id": "signal-1",
        "condition_id": "condition-1",
        "market_slug": "btc-updown-5m-1800000000",
        "outcome": "Up",
        "token_id": "token-1",
        "executable_price": 0.4,
        "observed_ts": 1_800_000_031.0,
        "signal_ts": 1_800_000_030.0,
    }

    intent = lane.probability_signal_to_intent(signal)

    assert intent.mode == "paper"
    assert intent.live_orders_allowed is False
    assert intent.copy_size_usd == 1.0
    assert intent.limit_price == 0.4
    assert intent.metadata["single_guard_adapter"] is True
    assert intent.strategy_family == lane.LANE_ID


def test_frozen_model_checksum_and_metrics_ignore_later_bars(tmp_path: Path) -> None:
    model_path = tmp_path / "model.json"
    before = [
        {"window_start_s": lane.TRAINING_CUTOFF_S - 900, "z_score": -2.0, "up": False},
        {"window_start_s": lane.TRAINING_CUTOFF_S - 600, "z_score": 2.0, "up": True},
        {"window_start_s": lane.TRAINING_CUTOFF_S - 300, "z_score": 2.0, "up": True},
    ]
    first = lane._load_or_create_frozen_model(str(model_path), before)
    later = before + [
        {"window_start_s": lane.TRAINING_CUTOFF_S + 300, "z_score": -2.0, "up": True},
        {"window_start_s": lane.TRAINING_CUTOFF_S + 600, "z_score": 2.0, "up": False},
    ]
    second = lane._load_or_create_frozen_model(str(model_path), later)

    assert second["checksum"] == first["checksum"]
    assert second["train_metrics"] == first["train_metrics"]
    assert second["holdout_metrics"] == first["holdout_metrics"]
    assert all(window < lane.TRAINING_CUTOFF_S for window in second["train_window_ids"])
    assert all(window < lane.TRAINING_CUTOFF_S for window in second["holdout_window_ids"])


def test_protected_terminal_is_not_counted_as_prospective_signal(tmp_path: Path) -> None:
    terminal = {
        "terminal_id": "terminal-1",
        "terminal_status": "PROTECTED_SKIP",
        "market_slug": "btc-updown-5m-1800000000",
        "blockers": ["unchanged_hard_entry_bounds"],
        "intent": None,
    }

    summary = lane._prospective_summary([terminal], str(tmp_path / "missing-resolutions.jsonl"))

    assert summary["scheduled_windows"] == 1
    assert summary["terminal_counts"] == {"PROTECTED_SKIP": 1}
    assert summary["resolved_signals"] == 0
    assert summary["distinct_windows"] == 0
