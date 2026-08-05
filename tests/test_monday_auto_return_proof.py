from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.wallet_copy.ingest import normalize_polymarket_wallet_row
from src.wallet_copy.models import WalletSpec
from src.wallet_copy.promotion_rotation import PromotionRotationConfig, evaluate_inactivity_rotation


E6DB = "0xe6db20932faf0f9780acf75d95c74c9984407dac"
AC05 = "0xac0586732786905d285959613f1813bc89246729"
ALT = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
WALLET_5960 = "0x59603775762a631d4bcd156980c0a174bbc4c2d2"
A95B = "0xa95b27b0626973f7bc7600e65cd88cd7b42d0d14"


@pytest.fixture(autouse=True)
def _default_external_liveness(monkeypatch, tmp_path: Path) -> None:
    import scripts.run_wallet_copy_live_guard as guard

    wallets = [E6DB, AC05, ALT, WALLET_5960, A95B]
    now_ts = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc).timestamp()
    state = tmp_path / "queue_remote_dataapi_fresh_flow_probe_latest.json"
    state.write_text(
        json.dumps(
            {
                "generated_at": "2026-07-14T12:00:00+00:00",
                "rows": [
                    {
                        "wallet": wallet,
                        "status": "PASS",
                        "latest_btc5m_trade_ts": now_ts - 60.0,
                        "btc5m_trades_24h": 10,
                        "btc5m_buys_24h": 10,
                    }
                    for wallet in wallets
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(guard, "ACTIVE_SET_EXTERNAL_LIVENESS_STATE", state)


def _source_ref(obj: object) -> str:
    path = Path(inspect.getsourcefile(obj) or "").name
    line = inspect.getsourcelines(obj)[1]
    return f"{path}:{line}"


def _slice_label(label: str, *, n: int, roi: float) -> dict[str, object]:
    return {
        "label": label,
        "resolved_trades": n,
        "roi_pct": roi,
        "pnl_usd": round(roi, 6),
        "reason": f"n={n}; roi={roi}",
    }


def _write_temporal_registry(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "wallets": [
                    {
                        "wallet": E6DB,
                        "classification": "WEEKDAY-ONLY",
                        "regime_profiles": {
                            "weekday": {
                                "pnl_usd": 946.294491,
                                "resolved_trades": 1792,
                                "latest_event_ts": "2026-07-10T23:49:54Z",
                            },
                            "weekend": {"pnl_usd": -31.78, "resolved_trades": 253},
                        },
                        "slice_labels": {
                            "weekday": _slice_label("PROVEN-POSITIVE", n=1792, roi=12.74),
                            "weekend": _slice_label("PROVEN-NEGATIVE", n=253, roi=-12.56),
                            "dead_band_18_22_utc": _slice_label("PROVEN-NEGATIVE", n=47, roi=-9.0),
                        },
                    },
                    {
                        "wallet": AC05,
                        "classification": "WEEKEND-ONLY",
                        "regime_profiles": {
                            "weekday": {"pnl_usd": -429.64, "resolved_trades": 837},
                            "weekend": {"pnl_usd": 102.03, "resolved_trades": 184},
                        },
                        "slice_labels": {
                            "weekday": _slice_label("PROVEN-NEGATIVE", n=837, roi=-8.02),
                            "weekend": _slice_label("PROVEN-POSITIVE", n=184, roi=11.52),
                            "dead_band_18_22_utc": _slice_label("PROVEN-POSITIVE", n=32, roi=3.5),
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )


def _weekend_benched_e6db() -> dict[str, object]:
    return {
        "candidate_id": "runtime_auto_degrade_e6db20932f",
        "candidate_type": "SINGLE_WALLET",
        "enabled": False,
        "max_order_usd": 2.0,
        "policy_id": "weekday_policy",
        "source_wallet": E6DB,
        "status": "WEEKEND_BENCHED_WEEKDAY_SEAT_PRESERVED",
        "weekend_bench": {
            "auto_return_at": "2026-07-13T00:00:00Z",
            "weekday_seat_preserved": True,
            "classification": "WEEKDAY-ONLY",
        },
    }


def _active_set_contract() -> dict[str, object]:
    return {
        "status": "ACTIVE",
        "mode": "active_set_single_guard",
        "target_member_count_max": 2,
        "members": [
            _weekend_benched_e6db(),
            {
                "candidate_id": "runtime_auto_degrade_ac05",
                "candidate_type": "SINGLE_WALLET",
                "enabled": True,
                "max_order_usd": 2.0,
                "policy_id": "weekend_policy",
                "source_wallet": AC05,
                "status": "ACTIVE_WEEKEND_SPECIALIST",
            },
        ],
    }


def _runtime_args(tmp_path: Path, now_iso: str) -> SimpleNamespace:
    poller_state = tmp_path / "active_set_poller.json"
    ledger_state = tmp_path / "live_ledger.json"
    poller_state.write_text(json.dumps({"fetch_meta": {}}), encoding="utf-8")
    ledger_state.write_text(json.dumps({"orders": []}), encoding="utf-8")
    return SimpleNamespace(
        active_set=True,
        active_set_member_limit=0,
        candidate_id="",
        source_wallet="",
        policy_id="",
        inventory_late_window_stop_s=60.0,
        active_set_dataapi_poller_state=str(poller_state),
        live_ledger_state=str(ledger_state),
        active_set_temporal_now_iso=now_iso,
    )


def _candidate_evidence() -> dict[str, object]:
    return {
        "max_recent_ask_depth_usd": 5.0,
        "copyability_profile_gate_enabled": True,
        "copyability_profile_eligible": True,
        "execution_profile": {
            "latency_horizon_s": 2.0,
            "fill_sample": 24,
            "copyable_rate_pct": 80.0,
            "mean_edge": 0.01,
            "median_edge": 0.01,
        },
        "live_executable_paper_eligible": True,
        "recent_copy_sized_buy_events": 2,
        "paper_eligible_policy_ids": ["segmented_25pct"],
        "paper_policy_gate": {
            "best_policy_id": "segmented_25pct",
            "best_policy_paper_pnl_usd": 0.75,
            "best_policy_copyable_buy_events": 24,
        },
    }


def _reserved_5960_member() -> dict[str, object]:
    policy_id = "deadman_microprobe_0.10_cap_0.5_le_25"
    return {
        "candidate_id": "leaderboard_crypto_5960377576",
        "candidate_type": "SINGLE_WALLET",
        "enabled": True,
        "source_wallet": WALLET_5960,
        "status": "FABLE_2004_5960_READMISSION_MICROPROBE",
        "policy_id": policy_id,
        "wallet_fraction": 0.10,
        "max_order_usd": 0.5,
        "max_price": 0.25,
        "rolling_loss_trigger_usd": -2.0,
        "policy": {
            "policy_id": policy_id,
            "wallet_fraction": 0.10,
            "min_order_usd": 0.5,
            "max_order_usd": 0.5,
            "max_price": 0.25,
        },
        "summary": {
            "direction_id": "2026-07-11T20:04Z-fable-5960-readmission",
            "loss_action": "auto-demote on worse_of <= -2.00 or cumulative resolved copy PnL < 0 after 6 resolved fills",
            "no_fill_deadline": "2026-07-13T12:00:00Z",
        },
        "prior_live_abort": {
            "resolved_fills": 6,
            "rule": "worse_of_abort_lte_usd <= -2.50",
        },
    }


def _a95b_member() -> dict[str, object]:
    return {
        "candidate_id": "runtime_auto_degrade_a95b27b062",
        "candidate_type": "SINGLE_WALLET",
        "enabled": True,
        "source_wallet": A95B,
        "status": "FABLE_1413_A95B_LIVE_DEFEND_CAP1",
        "policy_id": "cap1_a95b",
        "wallet_fraction": 0.10,
        "max_order_usd": 1.0,
        "max_price": 0.25,
        "rolling_loss_trigger_usd": -16.0,
        "policy": {
            "policy_id": "cap1_a95b",
            "wallet_fraction": 0.10,
            "min_order_usd": 1.0,
            "max_order_usd": 1.0,
            "max_price": 0.25,
        },
    }


def _reserved_5960_active_set() -> dict[str, object]:
    return {
        "status": "ACTIVE",
        "mode": "active_set_single_guard",
        "target_member_count_max": 2,
        "members": [_reserved_5960_member(), _a95b_member()],
    }


def _write_5960_temporal_registry(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": "2026-07-11T20:54:26.665639Z",
                "kind": "wallet_temporal_profitability_registry",
                "summary": {"history_skipped": {"events_scanned": 154091}},
                "wallets": [
                    {
                        "wallet": WALLET_5960,
                        "classification": "WEEKDAY-ONLY",
                        "slice_labels": {
                            "weekday": _slice_label("PROVEN-POSITIVE", n=1265, roi=10.0),
                            "weekend": _slice_label("PROVEN-NEGATIVE", n=270, roi=-3.7),
                            "dead_band_18_22_utc": _slice_label("PROVEN-NEGATIVE", n=104, roi=-7.83),
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _write_5960_selection_freeze(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "enabled": True,
                "frozen_wallets": [
                    {
                        "source_wallet": A95B,
                        "direction_id": "2026-07-11T20:04Z-fable-5960-readmission",
                        "reason": "5960 re-admission gets the active probe seat",
                        "expiry_policy": "condition_based_no_clock_expiry",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _profitless_runtime_args(tmp_path: Path, now_iso: str) -> SimpleNamespace:
    args = _runtime_args(tmp_path, now_iso)
    profit_state = tmp_path / "profit_state.json"
    rotation_state = tmp_path / "promotion_rotation_state.json"
    profit_state.write_text(json.dumps({}), encoding="utf-8")
    rotation_state.write_text(json.dumps({}), encoding="utf-8")
    args.profit_state = str(profit_state)
    args.promotion_rotation_state = str(rotation_state)
    args.min_live_order_usd = 1.0
    return args


def _btc5m_event(*, wallet: str, now_ts: float, index: int = 0):
    window_start = int(now_ts // 300.0) * 300
    spec = WalletSpec(name="5960", address=wallet)
    row = {
        "side": "BUY",
        "proxyWallet": wallet,
        "conditionId": f"0x5960-cond-{index}",
        "marketSlug": f"btc-updown-5m-{window_start}",
        "eventSlug": f"btc-updown-5m-{window_start}",
        "title": "Bitcoin Up or Down - 5 minutes",
        "outcome": "Up",
        "price": 0.25,
        "size": 20.0,
        "usdcSize": 5.0,
        "asset": "token-up",
        "transactionHash": f"0x5960-monday-{index}",
        "timestamp": int(now_ts - 2 - index),
    }
    event = normalize_polymarket_wallet_row(row, spec=spec, row_type="trade", observed_ts=now_ts - 1 - index)
    assert event is not None
    return event


def _probe_gate_args() -> SimpleNamespace:
    return SimpleNamespace(
        min_price=0.0,
        max_price=0.25,
        wallet_fraction=0.10,
        max_order_usd=0.5,
        max_freshest_buy_lag_s=7200.0,
        min_clob_copyable_buys_24h=5,
        corrected_joint_gate=True,
        min_temporal_history_events_scanned=153000.0,
        fail_closed_on_active_unproven_temporal_slice=True,
    )


def test_probe_cap_size_defense_clamps_selected_runtime_policy(tmp_path: Path) -> None:
    import scripts.run_wallet_copy_live_guard as guard

    digest = tmp_path / "state_digest.json"
    digest.write_text(
        json.dumps(
            {
                "defense_tripwires": {
                    "size_defense_action": "PROBE_CAPS_REST_OF_UTC_DAY",
                    "probe_caps_cap_usd": 1.0,
                    "probe_caps_weight": 0.10,
                    "t1_day_pnl_usd": -8.91,
                    "t1_since_topup_actual_usd": 17.46,
                    "intraday_probe_triggered": True,
                    "single_fill_probe_triggered": False,
                }
            }
        ),
        encoding="utf-8",
    )
    selected = {
        "candidate_id": "runtime_auto_degrade_e6db20932f",
        "source_wallet": E6DB,
        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
        "wallet_fraction": 0.25,
        "max_order_usd": 4.0,
        "policy": {
            "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
            "wallet_fraction": 0.25,
            "max_order_usd": 4.0,
            "min_order_usd": 2.0,
            "min_live_order_usd": 2.0,
            "drip_max_tranche_usd": 2.5,
        },
    }
    policy = dict(selected["policy"])

    result = guard._apply_probe_cap_size_defense(
        selected=selected,
        selected_runtime_policy=policy,
        args=SimpleNamespace(state_digest_json=str(digest)),
    )

    assert result["status"] == "PROBE_CAPS_REST_OF_UTC_DAY"
    assert result["old_max_order_usd"] == 4.0
    assert policy["wallet_fraction"] == 0.1
    assert policy["max_order_usd"] == 1.0
    assert policy["min_order_usd"] == 1.0
    assert policy["min_live_order_usd"] == 1.0
    assert policy["drip_max_tranche_usd"] == 1.0
    assert policy["maker_min_share_funding_cap_usd"] == 1.0
    assert policy["maker_min_share_original_policy_cap_usd"] == 4.0
    assert policy["maker_min_share_base_request_cap_usd"] == 1.0
    assert selected["max_order_usd"] == 1.0
    assert selected["maker_min_share_funding_cap_usd"] == 1.0
    assert selected["policy"]["maker_min_share_funding_cap_usd"] == 1.0
    assert selected["policy"]["size_defense"]["intraday_probe_triggered"] is True


def test_monday_auto_return_proof_enumerates_weekend_conditioned_gates() -> None:
    import scripts.order_flow_deadman as order_deadman
    import scripts.run_wallet_copy_live_guard as guard
    import src.wallet_copy.promotion_rotation as promotion_rotation

    gate_refs = {
        "weekend_bench_auto_return": _source_ref(guard._weekend_bench_auto_return),
        "temporal_active_slices": _source_ref(guard._temporal_active_slice_names),
        "temporal_slice_exclusion": _source_ref(guard._temporal_slice_exclusion),
        "active_set_runtime_clock_injection": _source_ref(guard._active_set_runtime_args),
        "calendar_inactivity_clock": _source_ref(promotion_rotation._calendar_clock_evidence),
        "measured_skip_recomputed_from_evidence": _source_ref(order_deadman._gated_quiet_classification),
    }

    assert set(gate_refs) == {
        "weekend_bench_auto_return",
        "temporal_active_slices",
        "temporal_slice_exclusion",
        "active_set_runtime_clock_injection",
        "calendar_inactivity_clock",
        "measured_skip_recomputed_from_evidence",
    }
    assert "run_wallet_copy_live_guard.py:" in gate_refs["weekend_bench_auto_return"]
    assert "run_wallet_copy_live_guard.py:" in gate_refs["temporal_active_slices"]
    assert "promotion_rotation.py:" in gate_refs["calendar_inactivity_clock"]
    assert "order_flow_deadman.py:" in gate_refs["measured_skip_recomputed_from_evidence"]


def test_monday_auto_return_lifts_weekend_bench_and_temporal_slices_at_utc_boundary(monkeypatch, tmp_path: Path) -> None:
    import scripts.run_wallet_copy_live_guard as guard

    temporal = tmp_path / "wallet_temporal_profitability_latest.json"
    overlay = tmp_path / "wallet_copy_active_set_auto_degrade_state.json"
    freeze = tmp_path / "selection_priority_freeze.json"
    _write_temporal_registry(temporal)
    overlay.write_text(json.dumps({}), encoding="utf-8")
    freeze.write_text(json.dumps({}), encoding="utf-8")
    monkeypatch.setattr(guard, "WALLET_TEMPORAL_PROFITABILITY_STATE", temporal)
    monkeypatch.setattr(guard, "AUTO_DEGRADE_ACTIVE_SET_STATE", overlay)
    monkeypatch.setattr(guard, "SELECTION_PRIORITY_FREEZE_STATE", freeze)
    monkeypatch.setattr(
        guard,
        "mission_contract",
        lambda: {"current_runtime_phase_contract": {"active_live_set": _active_set_contract()}},
    )
    monkeypatch.setattr(guard, "_active_live_set_is_empty", lambda active_set=None: False)

    sunday = "2026-07-12T12:00:00Z"
    pre_monday = "2026-07-12T23:59:59Z"
    monday = "2026-07-13T00:00:00Z"

    sunday_contract = guard._active_live_set_contract(now=datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc))
    sunday_e6db = next(row for row in sunday_contract["members"] if row["source_wallet"] == E6DB)
    assert sunday_e6db["weekend_bench_auto_return"]["reason"] == "auto_return_not_due"

    sunday_args, sunday_runtime = guard._active_set_runtime_args(_runtime_args(tmp_path, sunday), cycle=1)
    pre_args, pre_runtime = guard._active_set_runtime_args(_runtime_args(tmp_path, pre_monday), cycle=1)
    monday_args, monday_runtime = guard._active_set_runtime_args(_runtime_args(tmp_path, monday), cycle=1)

    assert sunday_args.source_wallet == AC05
    assert [row["source_wallet"] for row in sunday_runtime["members"]] == [AC05]
    assert sunday_runtime["temporal_slice_exclusion"]["active_slices"] == ["weekend"]
    assert E6DB not in [row["source_wallet"] for row in sunday_runtime["members"]]
    assert pre_args.source_wallet == AC05
    assert [row["source_wallet"] for row in pre_runtime["members"]] == [AC05]

    assert monday_args.source_wallet == E6DB
    assert [row["source_wallet"] for row in monday_runtime["members"]] == [E6DB]
    returned = monday_runtime["members"][0]
    assert monday_args.active_set_selected_member_status == "WEEKEND_BENCH_AUTO_RETURNED"
    assert returned["temporal_slice_evaluation"]["reason"] == "no_active_temporal_slice_proven_negative"
    assert monday_runtime["temporal_slice_exclusion"]["active_slices"] == ["weekday"]
    assert monday_runtime["temporal_slice_exclusion"]["excluded_wallets"] == [AC05]

    assert guard._temporal_active_slice_names(datetime(2026, 7, 12, 18, 30, tzinfo=timezone.utc)) == [
        "weekend",
        "dead_band_18_22_utc",
    ]
    assert guard._temporal_active_slice_names(datetime(2026, 7, 13, 0, 0, tzinfo=timezone.utc)) == ["weekday"]


def test_5960_reserved_seat_self_activates_monday_and_builds_probe_intent(monkeypatch, tmp_path: Path) -> None:
    import scripts.probe_deadman_microprobe_gate as probe
    import scripts.run_wallet_copy_live_execution as live_execution
    import scripts.run_wallet_copy_live_guard as guard

    temporal = tmp_path / "wallet_temporal_profitability_latest.json"
    overlay = tmp_path / "wallet_copy_active_set_auto_degrade_state.json"
    freeze = tmp_path / "selection_priority_freeze.json"
    ledger = tmp_path / "live_ledger.json"
    _write_5960_temporal_registry(temporal)
    overlay.write_text(json.dumps({}), encoding="utf-8")
    _write_5960_selection_freeze(freeze)
    ledger.write_text(json.dumps({"orders": []}), encoding="utf-8")
    monkeypatch.setattr(guard, "WALLET_TEMPORAL_PROFITABILITY_STATE", temporal)
    monkeypatch.setattr(guard, "AUTO_DEGRADE_ACTIVE_SET_STATE", overlay)
    monkeypatch.setattr(guard, "SELECTION_PRIORITY_FREEZE_STATE", freeze)
    monkeypatch.setattr(
        guard,
        "mission_contract",
        lambda: {"current_runtime_phase_contract": {"active_live_set": _reserved_5960_active_set()}},
    )
    monkeypatch.setattr(guard, "_active_live_set_is_empty", lambda active_set=None: False)

    saturday_args, saturday_runtime = guard._active_set_runtime_args(
        _profitless_runtime_args(tmp_path, "2026-07-11T20:05:00Z"),
        cycle=1,
    )
    saturday_ruled = guard._ruled_flat_active_set_state(saturday_runtime)
    assert saturday_args.active_set_executable_roster_empty is True
    assert saturday_ruled["active"] is True
    assert saturday_ruled["blockers"] == [
        "temporal_slice_active_unproven_basis",
        "temporal_slice_exclusion_all_measured_members",
        "seat_reserved(direction_id=2026-07-11T20:04Z-fable-5960-readmission, activates_at=2026-07-13T00:00:00Z)",
    ]
    assert saturday_runtime["fresh_runtime_member_selection"]["selection_priority_freeze"]["wallets"] == [A95B]

    monday_args, monday_runtime = guard._active_set_runtime_args(
        _profitless_runtime_args(tmp_path, "2026-07-13T00:05:00Z"),
        cycle=1,
    )
    monday_candidate, monday_source, monday_blockers = guard._load_candidate(monday_args)

    assert monday_runtime["enabled"] is True
    assert guard._ruled_flat_active_set_state(monday_runtime)["active"] is False
    assert monday_runtime["temporal_slice_exclusion"]["active_slices"] == ["weekday"]
    assert monday_args.candidate_id == "leaderboard_crypto_5960377576"
    assert monday_args.source_wallet == WALLET_5960
    assert monday_args.active_set_selected_member_policy["max_order_usd"] == 1.0
    assert monday_args.active_set_selected_member_policy["min_order_usd"] == 1.0
    assert monday_args.min_live_order_usd == 1.0
    assert monday_args.price_band_decision_max_price == 0.25
    assert monday_runtime["selected_member"]["max_price"] == 0.25
    assert monday_runtime["members"][0]["rolling_loss_trigger_usd"] == -2.0
    assert "6 resolved fills" in _reserved_5960_member()["summary"]["loss_action"]
    assert monday_source == WALLET_5960
    assert monday_blockers == []
    assert monday_candidate["guard_pass_gate"]["passed"] is True

    gate_events = [_btc5m_event(wallet=WALLET_5960, now_ts=datetime(2026, 7, 13, 0, 5, tzinfo=timezone.utc).timestamp(), index=i) for i in range(5)]
    monkeypatch.setattr(
        probe,
        "_temporal_slice_exclusion",
        lambda member: {
            "excluded": False,
            "reason": "no_active_temporal_slice_proven_negative",
            "active_slices": ["weekday"],
            "evaluated_slices": [
                {
                    "slice": "weekday",
                    "label": "PROVEN-POSITIVE",
                    "resolved_trades": 1265,
                    "pnl_usd": 523.99,
                    "roi_pct": 10.0,
                    "label_reason": "weekday profitable fixture",
                }
            ],
        },
    )
    gate_row = probe._row_for_wallet(
        WALLET_5960,
        events=gate_events,
        fetch_report={"status": "PASS"},
        clearance_by_wallet={},
        queue_by_wallet={},
        rotation_by_wallet={},
        clob=object(),
        now_s=datetime(2026, 7, 13, 0, 5, tzinfo=timezone.utc).timestamp(),
        args=_probe_gate_args(),
        temporal_registry_basis={"events_scanned": 154091, "generated_at": "2026-07-11T20:54:26.665639Z"},
    )
    assert gate_row["gate"]["pass"] is True
    assert gate_row["corrected_joint_gate"]["temporal_integrity"]["basis"]["events_scanned"] == 154091

    policy = live_execution.CandidatePolicy(
        policy_id=monday_args.active_set_selected_member_policy["policy_id"],
        wallet_fraction=monday_args.active_set_selected_member_policy["wallet_fraction"],
        max_order_usd=monday_args.active_set_selected_member_policy["max_order_usd"],
        min_order_usd=monday_args.active_set_selected_member_policy["min_order_usd"],
        max_price=monday_args.active_set_selected_member_policy["max_price"],
    )
    intents, summary = live_execution._build_inventory_v2_intents(
        [_btc5m_event(wallet=WALLET_5960, now_ts=datetime(2026, 7, 13, 0, 5, tzinfo=timezone.utc).timestamp())],
        policy,
        now_ts=datetime(2026, 7, 13, 0, 5, tzinfo=timezone.utc).timestamp(),
        max_event_age_s=30.0,
        live_build_max_observed_age_s=30.0,
        live_ledger_state=str(ledger),
        late_window_stop_s=60.0,
        max_converge_orders_per_window=6,
    )
    assert summary["retained_intents"] == 1
    assert len(intents) == 1
    assert intents[0].source_wallet == WALLET_5960
    assert intents[0].policy_id == "deadman_microprobe_0.10_cap_0.5_le_25"
    assert intents[0].copy_size_usd == 1.0
    assert intents[0].metadata["wallet_copy_policy"]["max_order_usd"] == 1.0

    first_transition = guard._operator_notify_transition(
        {"blockers": saturday_ruled["blockers"]},
        current_blockers=monday_blockers,
        suppression=saturday_ruled["operator_notify_suppression"],
    )
    repeat_transition = guard._operator_notify_transition(
        {"blockers": monday_blockers},
        current_blockers=monday_blockers,
        suppression={},
    )
    assert first_transition["notify"] is True
    assert first_transition["blocker_set_changed"] is True
    assert repeat_transition["notify"] is False
    assert repeat_transition["blocker_set_changed"] is False


def test_calendar_inactivity_clock_blocks_sunday_but_not_monday_boundary() -> None:
    zero_weekend_one_weekday = {f"{dow}:{hour:02d}": (0.0 if dow >= 5 else 1.0) for dow in range(7) for hour in range(24)}
    live_state = {
        "orders": [
            {
                "source_wallet": E6DB,
                "paper_only": False,
                "live_orders_allowed": True,
                "submitted_at": "2026-07-11T07:00:00+00:00",
            }
        ]
    }
    lane_state = {"ranked_wallets": [{"rank": 1, "wallet": ALT, **_candidate_evidence()}]}
    profile_state = {
        "profiles_by_wallet": {
            E6DB: {
                "wallet": E6DB,
                "trade_count": 20,
                "weekend_evidence_status": "HAS_WEEKEND_SAMPLE",
                "expected_active_dow_hour_weights": zero_weekend_one_weekday,
            }
        }
    }
    config = PromotionRotationConfig(live_inactivity_rotation_threshold_s=3 * 60 * 60)

    sunday = evaluate_inactivity_rotation(
        live_execution_state=live_state,
        lane_state=lane_state,
        config=config,
        now_ts=datetime(2026, 7, 12, 23, 59, 59, tzinfo=timezone.utc).timestamp(),
        dow_profile_state=profile_state,
    )
    monday = evaluate_inactivity_rotation(
        live_execution_state=live_state,
        lane_state=lane_state,
        config=config,
        now_ts=datetime(2026, 7, 13, 3, 0, 0, tzinfo=timezone.utc).timestamp(),
        dow_profile_state=profile_state,
    )

    assert sunday["calendar_clock"]["current_is_weekend"] is True
    assert sunday["calendar_clock"]["expected_active_age_below_threshold"] is True
    assert "calendar_expected_active_inactivity_below_threshold" in sunday["blockers"]
    assert sunday["rotation_triggered"] is False

    assert monday["calendar_clock"]["current_is_weekend"] is False
    assert monday["calendar_clock"]["expected_active_age_below_threshold"] is False
    assert "calendar_expected_active_inactivity_below_threshold" not in monday["blockers"]
    assert monday["rotation_triggered"] is True


def test_monday_return_reporter_builds_pass_artifact(monkeypatch, tmp_path: Path) -> None:
    import scripts.report_monday_return_synthetic_proof as reporter
    import scripts.run_wallet_copy_live_guard as guard

    temporal = tmp_path / "wallet_temporal_profitability_latest.json"
    _write_temporal_registry(temporal)
    monkeypatch.setattr(guard, "WALLET_TEMPORAL_PROFITABILITY_STATE", temporal)

    active_state = {
        "active_set": {
            "qualified_member_count": 2,
            "submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains the sole live order submitter",
            "members": [
                {
                    "candidate_id": "runtime_auto_degrade_e6db20932f",
                    "source_wallet": E6DB,
                    "status": "AUTO_DEGRADE_RUNTIME_ROSTER_PROTECTION_BOUNDED",
                    "is_current_cycle_member": True,
                    "policy_id": "weekday_policy",
                    "max_order_usd": 1.0,
                },
                {
                    "candidate_id": "runtime_auto_degrade_ac05",
                    "source_wallet": AC05,
                    "status": "ACTIVE_WEEKEND_SPECIALIST",
                    "policy_id": "weekend_policy",
                    "max_order_usd": 1.0,
                },
            ],
        }
    }
    address_form = {
        "generated_at": "2026-07-13T09:42:26Z",
        "rows": [
            {
                "freshness_context": {"wallet": E6DB},
                "address_selection": {
                    "recommended_query_key": "user",
                    "freshest_btc5m_trade_age_h": 0.1,
                },
            },
            {
                "freshness_context": {"wallet": AC05},
                "address_selection": {
                    "recommended_query_key": "user",
                    "freshest_btc5m_trade_age_h": 0.2,
                },
            },
        ],
    }

    report = reporter.build_report(
        active_set_state=active_state,
        address_form_map=address_form,
        live_execution_state={"can_trade": True},
        process_rows=["123 scripts/run_wallet_copy_live_guard.py --execute-live"],
    )

    assert report["status"] == "PASS"
    assert report["checks"] == {
        "address_form_user_fresh": True,
        "runtime_can_trade": True,
        "runtime_roster_live_unbenched": True,
        "single_guard_process": True,
        "synthetic_monday_auto_return": True,
        "weekend_return_targets_bound": True,
    }
    assert report["synthetic_monday_auto_return"]["auto_return"]["eligible"] is True


def test_monday_return_reporter_rebinds_to_w1_weekend_targets_without_current_can_trade(
    monkeypatch, tmp_path: Path
) -> None:
    import scripts.report_monday_return_synthetic_proof as reporter
    import scripts.run_wallet_copy_live_guard as guard

    temporal = tmp_path / "wallet_temporal_profitability_latest.json"
    _write_temporal_registry(temporal)
    monkeypatch.setattr(guard, "WALLET_TEMPORAL_PROFITABILITY_STATE", temporal)

    active_state = {
        "active_set": {
            "qualified_member_count": 1,
            "submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains the sole live order submitter",
            "members": [
                {
                    "candidate_id": "runtime_auto_degrade_e6db20932f",
                    "source_wallet": E6DB,
                    "status": "AUTO_DEGRADE_RUNTIME_ROSTER_PROTECTION_BOUNDED",
                    "is_current_cycle_member": False,
                    "policy_id": "weekday_policy",
                    "max_order_usd": 0.5,
                },
            ],
        }
    }
    weekend_packet = {
        "generated_at": "2026-07-17T23:55:00Z",
        "current_roster_weekend_posture_plan": {
            "direction_id": "2026-07-17T15:52Z-fable-weekend-prep",
            "weekend_starts_at": "2026-07-18T00:00:00Z",
            "members": [
                {
                    "candidate_id": "runtime_auto_degrade_e6db20932f",
                    "source_wallet": E6DB,
                    "policy_id": "weekday_policy",
                    "weekend_posture": "TRADE_FLOOR_SIZE",
                    "current_weekday_evidence": {"fills": 12, "pnl_usd": 7.25},
                }
            ],
        },
    }
    address_form = {
        "rows": [
            {
                "freshness_context": {"wallet": E6DB},
                "address_selection": {
                    "recommended_query_key": "user",
                    "freshest_btc5m_trade_age_h": 0.1,
                },
            },
        ],
    }

    report = reporter.build_report(
        active_set_state=active_state,
        address_form_map=address_form,
        live_execution_state={"can_trade": False},
        weekend_parity_packet=weekend_packet,
        require_runtime_can_trade=False,
        process_rows=["123 scripts/run_wallet_copy_live_guard.py --execute-live"],
    )

    assert report["status"] == "PASS"
    assert report["checks"]["runtime_can_trade_not_required"] is True
    assert report["weekend_return_plan"]["target_wallets"] == [E6DB]
    assert report["weekend_return_plan"]["bench_ids"] == ["runtime_auto_degrade_e6db20932f"]
    assert report["weekend_return_plan"]["auto_return_at"] == "2026-07-20T00:00:00Z"
    assert report["synthetic_monday_auto_returns"][0]["auto_return"]["eligible"] is True
