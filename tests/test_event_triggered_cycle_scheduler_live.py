import argparse
import json

from scripts.run_wallet_copy_live_guard import _event_triggered_cycle_scheduler_decision


def _args(history_state, *, enabled=True, execute_live=True, live_orders_allowed=True):
    return argparse.Namespace(
        history_state=str(history_state),
        sleep_s=2.0,
        execute_live=execute_live,
        live_orders_allowed=live_orders_allowed,
        event_triggered_cycle_scheduler=enabled,
        event_triggered_cycle_trigger_sleep_s=0.0,
        event_triggered_cycle_max_signal_age_s=30.0,
        live_build_max_observed_age_s=30.0,
    )


def _write_history(path, events):
    path.write_text(json.dumps({"events": events}), encoding="utf-8")


def _premerge(*, new_matching_events=1, wallet="0xabc"):
    return {
        "status": "PASS",
        "new_matching_events": new_matching_events,
        "selected_wallet": wallet,
        "rows": [
            {
                "source_wallet": wallet,
                "new_matching_events": new_matching_events,
                "latest_observed_ts": 1001.0,
            }
        ],
    }


def _event(**overrides):
    row = {
        "event_id": "we_fresh",
        "source_wallet": "0xabc",
        "action": "BUY",
        "asset": "BTC",
        "duration": "5m",
        "market_slug": "btc-updown-5m-900",
        "observed_ts": 1001.0,
        "event_ts": 1000.5,
        "outcome": "Up",
        "price": 0.49,
    }
    row.update(overrides)
    return row


def test_event_triggered_cycle_scheduler_wakes_same_guard_for_fresh_open_btc5m_event(tmp_path):
    history = tmp_path / "history.json"
    _write_history(history, [_event()])

    decision = _event_triggered_cycle_scheduler_decision(
        _args(history),
        active_set_rtds_premerge=_premerge(),
        previous_state={},
        blockers=[],
        cycle=7,
        cycle_started_wall_ts=1000.0,
        cycle_duration_s=4.0,
        generated_at="1970-01-01T00:16:44Z",
        now_ts=1004.0,
    )

    assert decision["triggered"] is True
    assert decision["status"] == "TRIGGER_NEXT_GUARD_CYCLE"
    assert decision["sleep_s"] == 0.0
    assert decision["source_event"]["event_id"] == "we_fresh"
    assert decision["source_event"]["window_close_ts"] == 1200.0
    assert decision["last_trigger"]["trigger_cycle"] == 7
    assert decision["last_trigger"]["trigger_cycle_latency_vs_window_close_s"] == -196.0
    assert decision["scheduler_submits_orders"] is False
    assert decision["single_submitter_change"] is False
    assert decision["copyintent_parity_change"] is False
    assert decision["cap_threshold_eligibility_change"] is False
    assert "scripts/run_wallet_copy_live_guard.py remains the sole live order submitter" in decision["submitter_invariant"]


def test_event_triggered_cycle_scheduler_keeps_configured_sleep_without_new_events(tmp_path):
    history = tmp_path / "history.json"
    _write_history(history, [_event()])

    decision = _event_triggered_cycle_scheduler_decision(
        _args(history),
        active_set_rtds_premerge=_premerge(new_matching_events=0),
        previous_state={},
        blockers=[],
        cycle=8,
        cycle_started_wall_ts=1000.0,
        cycle_duration_s=4.0,
        generated_at="1970-01-01T00:16:44Z",
        now_ts=1004.0,
    )

    assert decision["triggered"] is False
    assert decision["sleep_s"] == 2.0
    assert decision["reason"] == "no_new_matching_events_from_premerge"


def test_event_triggered_cycle_scheduler_does_not_wake_for_closed_market(tmp_path):
    history = tmp_path / "history.json"
    _write_history(history, [_event(observed_ts=1195.0)])

    decision = _event_triggered_cycle_scheduler_decision(
        _args(history),
        active_set_rtds_premerge=_premerge(),
        previous_state={},
        blockers=[],
        cycle=9,
        cycle_started_wall_ts=1194.0,
        cycle_duration_s=4.0,
        generated_at="1970-01-01T00:20:01Z",
        now_ts=1201.0,
    )

    assert decision["triggered"] is False
    assert decision["sleep_s"] == 2.0
    assert decision["reason"] == "no_open_fresh_btc5m_buy_event_in_hot_history"


def test_event_triggered_cycle_scheduler_carries_last_trigger_when_idle(tmp_path):
    history = tmp_path / "history.json"
    _write_history(history, [])
    previous = {
        "event_triggered_cycle_scheduler": {
            "last_trigger": {
                "event_id": "we_prior",
                "trigger_cycle": 4,
            }
        }
    }

    decision = _event_triggered_cycle_scheduler_decision(
        _args(history),
        active_set_rtds_premerge=_premerge(new_matching_events=0),
        previous_state=previous,
        blockers=[],
        cycle=10,
        cycle_started_wall_ts=1000.0,
        cycle_duration_s=4.0,
        generated_at="1970-01-01T00:16:44Z",
        now_ts=1004.0,
    )

    assert decision["triggered"] is False
    assert decision["last_trigger"]["event_id"] == "we_prior"
    assert decision["last_trigger"]["trigger_cycle"] == 4


def test_event_triggered_cycle_scheduler_requires_live_guard_can_trade(tmp_path):
    history = tmp_path / "history.json"
    _write_history(history, [_event()])

    decision = _event_triggered_cycle_scheduler_decision(
        _args(history, execute_live=False),
        active_set_rtds_premerge=_premerge(),
        previous_state={},
        blockers=[],
        cycle=11,
        cycle_started_wall_ts=1000.0,
        cycle_duration_s=4.0,
        generated_at="1970-01-01T00:16:44Z",
        now_ts=1004.0,
    )

    assert decision["triggered"] is False
    assert decision["reason"] == "live_guard_not_tradeable"
    assert decision["sleep_s"] == 2.0
