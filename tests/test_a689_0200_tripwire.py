import json
from pathlib import Path

import scripts.report_a689_0200_tripwire as tripwire


WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"


def _write_state(tmp_path: Path, rows: list[dict], orders: list[dict] | None = None) -> tuple[Path, Path]:
    guard_state = tmp_path / "guard.json"
    live_state = tmp_path / "live.json"
    guard_state.write_text(json.dumps({"window_participation": {"rows": rows}}), encoding="utf-8")
    live_state.write_text(json.dumps({"orders": orders or []}), encoding="utf-8")
    return guard_state, live_state


def _floor_row(**overrides):
    row = {
        "source_wallet": WALLET,
        "market_slug": "btc-updown-5m-1784166900",
        "outcome": "Up",
        "window_start_s": 1784166900.0,
        "latest_observed_ts": 1784166926.0,
        "source_detection_observed_ts": 1784166926.0,
        "effective_latest_observed_ts": 1784167126.0,
        "dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
        "participation_skip_category": "FLOOR_BLOCKED_MISS",
        "window_budget_usd": 1.0,
        "drip_min_tranche_usd": 2.0,
        "min_order_usd": 1.0,
        "policy_max_order_usd": 1.0,
        "wallet_eligible_orders": 1,
        "our_submits": 0,
        "our_fills": 0,
    }
    row.update(overrides)
    return row


def _build(tmp_path: Path, rows: list[dict], orders: list[dict] | None = None):
    guard_state, live_state = _write_state(tmp_path, rows, orders)
    return tripwire.build_report(
        guard_state_path=guard_state,
        live_state_path=live_state,
        wallet=WALLET,
        policy_id=tripwire.DEFAULT_POLICY_ID,
        postfix_start_iso=tripwire.DEFAULT_POSTFIX_START,
    )


def test_policy_max_one_leak_stops_preruled_drip_min_change(tmp_path: Path):
    report = _build(tmp_path, [_floor_row(), _floor_row(market_slug="btc-updown-5m-1784167200")])

    assert report["verdict"] == "CONFIG_LEAK_DEFECT_POLICY_MAX_1"
    assert report["pre_ruled_action"] == "HOLD_DRIP_MIN_CHANGE_AND_ASK_FABLE"
    assert report["floor_budget_bind"]["leak_check"] == "FAIL_POLICY_MAX_1"
    assert report["floor_budget_bind"]["policy_max_1_rows"] == 2


def test_clean_budget_binds_authorizes_drip_min_one(tmp_path: Path):
    rows = [
        _floor_row(policy_max_order_usd=2.0, market_slug=f"btc-updown-5m-{1784166900 + i * 300}")
        for i in range(3)
    ]
    report = _build(tmp_path, rows)

    assert report["verdict"] == "BUDGET_BINDS_DRIP_MIN_1_AUTHORIZED"
    assert report["pre_ruled_action"] == "LOWER_A689_DRIP_MIN_TO_1_CAP_STAYS_2"
    assert report["floor_budget_bind"]["budget_bind_rows"] == 3
    assert report["floor_budget_bind"]["leak_check"] == "PASS_NO_POLICY_MAX_1_BINDER"


def test_policy_max_one_can_be_expected_for_f418_readmission_probe(tmp_path: Path):
    f418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
    guard_state, live_state = _write_state(tmp_path, [{**_floor_row(source_wallet=f418), "source_wallet": f418}])

    report = tripwire.build_report(
        guard_state_path=guard_state,
        live_state_path=live_state,
        wallet=f418,
        policy_id="fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
        postfix_start_iso=tripwire.DEFAULT_POSTFIX_START,
        policy_leak_max_usd=0.0,
        budget_bind_under_min_action="KEEP_F418_ARMED_UNTIL_5_RESOLVED_LIVE_FILL_TRIPWIRE",
        basis="Fable 2026-07-16T05:35Z f418 post-activation tripwire",
        rule="f418 $1 cap is intended; keep armed until the resolved-live-fill tripwire fires",
    )

    assert report["verdict"] == "BUDGET_BIND_UNDER_MIN_SAMPLE"
    assert report["pre_ruled_action"] == "KEEP_F418_ARMED_UNTIL_5_RESOLVED_LIVE_FILL_TRIPWIRE"
    assert report["floor_budget_bind"]["policy_max_1_rows"] == 0
    assert report["floor_budget_bind"]["leak_check"] == "DISABLED_EXPECTED_POLICY_MAX"


def test_late_rows_split_source_late_from_pipeline_late(tmp_path: Path):
    source_late = {
        "source_wallet": WALLET,
        "market_slug": "btc-updown-5m-1784161800",
        "outcome": "Up",
        "window_start_s": 1784161800.0,
        "latest_observed_ts": 1784162082.0,
        "source_detection_observed_ts": 1784162082.0,
        "effective_latest_observed_ts": 1784162095.0,
        "dominant_skip_reason": "inventory_late_window_guard",
        "participation_skip_category": "CORRECT_SKIP",
        "wallet_eligible_orders": 1,
    }
    pipeline_late = {
        **source_late,
        "market_slug": "btc-updown-5m-1784161200",
        "window_start_s": 1784161200.0,
        "latest_observed_ts": 1784161316.0,
        "source_detection_observed_ts": 1784161316.0,
        "effective_latest_observed_ts": 1784161498.0,
        "first_seen_at": "2026-07-16T00:24:58Z",
    }
    report = _build(tmp_path, [source_late, pipeline_late])

    assert report["verdict"] == "REAL_DEFECT_NOTIFY_NO_SELF_REMEDIATION"
    assert report["tripwire_class_counts"]["SOURCE_LATE"] == 1
    assert report["tripwire_class_counts"]["PIPELINE_LATE"] == 1


def test_timely_first_touch_later_aging_is_not_pipeline_late(tmp_path: Path):
    row = {
        "source_wallet": WALLET,
        "market_slug": "btc-updown-5m-1784161200",
        "outcome": "Up",
        "window_start_s": 1784161200.0,
        "latest_observed_ts": 1784161316.0,
        "source_detection_observed_ts": 1784161316.0,
        "effective_latest_observed_ts": 1784161498.0,
        "first_seen_at": "2026-07-16T00:21:56Z",
        "dominant_skip_reason": "inventory_late_window_guard",
        "participation_skip_category": "CORRECT_SKIP",
        "wallet_eligible_orders": 1,
    }

    report = _build(tmp_path, [row])

    assert report["verdict"] == "SOURCE_BEHAVIOR_NOT_DEFECT"
    assert report["tripwire_class_counts"]["WINDOW_AGED_OUT_AFTER_TIMELY_TOUCH"] == 1
    assert report["pipeline_late"]["rows"] == 0


def test_accepted_a689_order_preempts_tripwire(tmp_path: Path):
    order = {
        "status": "FILLED",
        "updated_at": "2026-07-16T01:59:00Z",
        "wallet_copy_inventory": {"source_wallet": WALLET},
    }
    report = _build(tmp_path, [_floor_row()], [order])

    assert report["verdict"] == "A689_ACCEPTED_ORDER_PREEMPT"
    assert report["pre_ruled_action"] == "NOTIFY_ACCEPTED_ORDER"
    assert report["accepted_order_rows"] == 1
