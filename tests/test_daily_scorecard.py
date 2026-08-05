from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import daily_scorecard
from scripts import report_daily_scorecard
from scripts.roll_handoff_archive import roll_handoff
from src.wallet_copy.store import atomic_write_json


def test_standing_price_band_money_is_derived_from_lifetime_truth() -> None:
    first = daily_scorecard._standing_price_band_evidence(
        {
            "total": {
                "cost_usd": 100.0,
                "payout_usd": 103.0,
                "pnl_usd": 3.0,
                "resolved_fills": 10,
            }
        }
    )
    second = daily_scorecard._standing_price_band_evidence(
        {
            "total": {
                "cost_usd": 200.0,
                "payout_usd": 212.0,
                "pnl_usd": 12.0,
                "resolved_fills": 20,
            }
        }
    )

    assert first["pnl_usd_realized"] == 3.0
    assert first["roi_pct_realized"] == 3.0
    assert second["pnl_usd_realized"] == 12.0
    assert second["roi_pct_realized"] == 6.0
    assert first != second


def test_scorecard_output_refreshes_current_pointer_only_for_current_day(tmp_path, monkeypatch) -> None:
    pointer = tmp_path / "current.json"
    monkeypatch.setattr(daily_scorecard, "CURRENT_SCORECARD_POINTER", pointer)
    current_output = tmp_path / "dated.json"
    daily_scorecard._write_scorecard_outputs(
        str(current_output), {"day_utc": "2026-07-22", "marker": "fresh"}, now_day="2026-07-22"
    )
    assert current_output.exists()
    assert daily_scorecard.load_json(pointer, default={})["marker"] == "fresh"

    historical_output = tmp_path / "historical.json"
    daily_scorecard._write_scorecard_outputs(
        str(historical_output), {"day_utc": "2026-07-21", "marker": "old"}, now_day="2026-07-22"
    )
    assert daily_scorecard.load_json(pointer, default={})["marker"] == "fresh"


def test_current_pointer_refreshes_without_an_output_path(tmp_path, monkeypatch) -> None:
    """The heartbeat builds today's scorecard for text display with no --output.

    Gating the current-day pointer on --output pinned state_digest.pnl.day_utc
    to the last day that happened to run with an explicit output path, which
    zeroed the canonical day PnL and blinded every day-scoped tripwire.
    """

    pointer = tmp_path / "current.json"
    monkeypatch.setattr(daily_scorecard, "CURRENT_SCORECARD_POINTER", pointer)

    daily_scorecard._write_scorecard_outputs(
        "", {"day_utc": "2026-08-01", "marker": "heartbeat"}, now_day="2026-08-01"
    )
    assert daily_scorecard.load_json(pointer, default={})["marker"] == "heartbeat"


def test_current_pointer_ignores_a_closed_day_without_an_output_path(tmp_path, monkeypatch) -> None:
    pointer = tmp_path / "current.json"
    monkeypatch.setattr(daily_scorecard, "CURRENT_SCORECARD_POINTER", pointer)
    atomic_write_json(pointer, {"day_utc": "2026-08-01", "marker": "today"})

    daily_scorecard._write_scorecard_outputs(
        "", {"day_utc": "2026-07-31", "marker": "closed"}, now_day="2026-08-01"
    )
    assert daily_scorecard.load_json(pointer, default={})["marker"] == "today"


def _filled(order_id: str, submitted_at: str, pnl_usd: float) -> dict:
    shares = 1.0 + pnl_usd
    return {
        "order_id": order_id,
        "final_status": "FILLED",
        "submitted_at": submitted_at,
        "condition_id": f"condition-{order_id}",
        "side": "YES",
        "requested_size_usd": 1.0,
        "requested_shares": shares,
    }


def _filled_in_window(order_id: str, window_start_s: int, seconds_to_close: int, pnl_usd: float) -> dict:
    order = _filled(
        order_id,
        datetime.fromtimestamp(window_start_s + 300 - seconds_to_close, tz=UTC).isoformat(),
        pnl_usd,
    )
    order["market_slug"] = f"btc-updown-5m-{window_start_s}"
    return order


def test_defense_regret_metric_is_post_fee_haircut_and_pulls_audit_on_second_flip() -> None:
    day = "2026-07-20"
    start = datetime(2026, 7, 20, tzinfo=UTC).timestamp()
    routing = {
        "rows": [
            {
                "market_slug": "btc-updown-5m-1",
                "winning_observed_ts": start + 10,
                "winning_intent_id": "ci_win",
                "limit_price": 0.4,
                "expected_fee_rate": 0.07,
                "realized_paper_outcome": {"status": "RESOLVED", "wins": True},
            },
            {
                "market_slug": "btc-updown-5m-1",
                "winning_observed_ts": start + 20,
                "winning_intent_id": "ci_duplicate",
                "limit_price": 0.9,
                "expected_fee_rate": 0.07,
                "realized_paper_outcome": {"status": "RESOLVED", "wins": False},
            },
        ]
    }
    metric = daily_scorecard._defense_regret_metric(
        day,
        -1.0,
        routing,
        previous_scorecard={"defense_regret": {"defense_flipped_sign": True}},
        now_ts=start + 86400,
    )
    assert metric["candidate_windows"] == 1
    assert metric["resolved_windows"] == 1
    assert metric["raw_standard_cap_post_fee_pnl_usd"] > 0
    assert metric["fill_realism_haircut"]["post_fee_pnl_usd"] > 0
    assert metric["fill_realism_haircut"]["post_fee_pnl_usd"] < metric["raw_standard_cap_post_fee_pnl_usd"]
    assert metric["defense_flipped_sign"] is True
    assert metric["framework_audit_auto_pull"] is True


def test_defense_regret_metric_does_not_blame_defense_when_haircut_stays_red() -> None:
    start = datetime(2026, 7, 21, tzinfo=UTC).timestamp()
    metric = daily_scorecard._defense_regret_metric(
        "2026-07-21",
        -2.0,
        {
            "rows": [
                {
                    "market_slug": "btc-updown-5m-2",
                    "winning_observed_ts": start + 10,
                    "limit_price": 0.4,
                    "expected_fee_rate": 0.07,
                    "realized_paper_outcome": {"status": "RESOLVED", "wins": False},
                }
            ]
        },
        previous_scorecard={"defense_regret": {"defense_flipped_sign": True}},
        now_ts=start + 100,
    )
    assert metric["status"] == "PARTIAL_OPEN_DAY"
    assert metric["counterfactual_green"] is False
    assert metric["framework_audit_auto_pull"] is False


def test_peer_active_idle_windows_fires_on_three_consecutive_elapsed_guard_rejects() -> None:
    metric = daily_scorecard._peer_active_idle_windows(
        {
            "missed_window_attribution": {
                "rows": [
                    {"window_start_s": 100, "attribution": "filled"},
                    {"window_start_s": 400, "attribution": "guard_reject"},
                    {"window_start_s": 700, "attribution": "guard_reject"},
                    {"window_start_s": 1000, "attribution": "guard_reject"},
                    {"window_start_s": 1300, "attribution": "guard_reject"},
                ]
            }
        },
        end_ts=2000,
        now_ts=1400,
    )
    assert metric["peer_active_idle_windows"] == 3
    assert metric["consecutive_peer_active_idle_windows"] == 3
    assert metric["status"] == "RED_INCIDENT"
    assert metric["swap_gate"]["status"] == "FABLE_TARGET_VALIDATION_REQUIRED"
    assert metric["swap_gate"]["swap_authorized"] is False
    assert metric["next_action"].startswith("ASK_FABLE:")
    assert "execute RED->SWAP" not in metric["next_action"]


def test_peer_active_idle_windows_clears_consecutive_count_after_fill() -> None:
    metric = daily_scorecard._peer_active_idle_windows(
        {
            "missed_window_attribution": {
                "rows": [
                    {"window_start_s": 100, "attribution": "guard_reject"},
                    {"window_start_s": 400, "attribution": "filled"},
                ]
            }
        },
        end_ts=1000,
        now_ts=1000,
    )
    assert metric["peer_active_idle_windows"] == 1
    assert metric["consecutive_peer_active_idle_windows"] == 0
    assert metric["incident_triggered"] is False
    assert metric["swap_gate"]["status"] == "NOT_APPLICABLE"


def test_peer_active_idle_windows_reset_ignores_pre_reset_incident_rows() -> None:
    metric = daily_scorecard._peer_active_idle_windows(
        {
            "missed_window_attribution": {
                "rows": [
                    {"window_start_s": 400, "attribution": "guard_reject"},
                    {"window_start_s": 700, "attribution": "guard_reject"},
                    {"window_start_s": 1000, "attribution": "guard_reject"},
                ]
            }
        },
        end_ts=2000,
        now_ts=1600,
        reset_state={
            "reset_at": "1970-01-01T00:20:00Z",
            "reason": "measured_skip_ruling_20260722T0537Z",
            "direction_id": "2026-07-22T06:43Z-fable",
        },
    )
    assert metric["peer_active_idle_windows"] == 3
    assert metric["consecutive_peer_active_idle_windows"] == 0
    assert metric["incident_triggered"] is False
    assert metric["post_reset_windows_evaluated"] == 0
    assert metric["counter_reset"]["reason"] == "measured_skip_ruling_20260722T0537Z"


def test_policy_cap_refusal_is_not_a_venue_submit_or_peer_idle_reject(tmp_path: Path) -> None:
    guard_state = tmp_path / "guard.json"
    atomic_write_json(
        guard_state,
        {
            "window_participation": {
                "window_rollups": [
                    {
                        "market_slug": "btc-updown-5m-600",
                        "window_start_s": 600,
                        "wallet_eligible_orders": 1,
                        "our_submits": 1,
                        "our_fills": 0,
                        "missed_active_window": True,
                    }
                ]
            }
        },
    )
    orders = [
        {
            "market_slug": "btc-updown-5m-600",
            "submitted_at": "1970-01-01T00:10:01Z",
            "status": "REJECTED",
            "final_status": "REJECTED",
            "order_id": "",
            "error_class": "maker_min_share_bump_exceeds_policy_cap",
        }
    ]

    volume = daily_scorecard._volume_kpi(
        str(guard_state), orders=orders, start_ts=0.0, end_ts=86400.0
    )
    row = volume["missed_window_attribution"]["rows"][2]
    peer_idle = daily_scorecard._peer_active_idle_windows(
        volume,
        end_ts=1200,
        now_ts=1200,
    )

    assert volume["windows_submitted"] == 0
    assert row["attribution"] == "policy_cap_refusal"
    assert volume["missed_window_attribution"]["categories"]["policy_cap_refusal"] == 1
    assert peer_idle["status"] == "CLEAR"
    assert peer_idle["peer_active_idle_windows"] == 0


def test_any_empty_venue_order_reject_is_pre_submit_not_guard_reject(tmp_path: Path) -> None:
    guard_state = tmp_path / "guard.json"
    atomic_write_json(
        guard_state,
        {
            "window_participation": {
                "window_rollups": [
                    {
                        "market_slug": "btc-updown-5m-600",
                        "window_start_s": 600,
                        "wallet_eligible_orders": 1,
                        "our_submits": 0,
                        "our_fills": 0,
                        "missed_active_window": True,
                    }
                ]
            }
        },
    )
    order = {
        "market_slug": "btc-updown-5m-600",
        "submitted_at": "1970-01-01T00:10:01Z",
        "status": "REJECTED",
        "final_status": "REJECTED",
        "trade_result": {"order_id": "", "error_class": "future_pre_submit_class"},
    }

    volume = daily_scorecard._volume_kpi(
        str(guard_state), orders=[order], start_ts=0.0, end_ts=86400.0
    )
    row = volume["missed_window_attribution"]["rows"][2]

    assert row["attribution"] == "pre_submit_refusal"
    assert volume["windows_submitted"] == 0
    assert volume["missed_window_attribution"]["categories"]["guard_reject"] == 0


def test_report_daily_scorecard_wrapper_times_out_child(monkeypatch, tmp_path: Path, capsys) -> None:
    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=["daily_scorecard.py"], timeout=1.0, output=b"partial\n", stderr=b"slow\n")

    monkeypatch.setattr(report_daily_scorecard.subprocess, "run", fake_run)
    monkeypatch.setattr(
        report_daily_scorecard,
        "parse_args",
        lambda: SimpleNamespace(
            day="2026-07-07",
            output=str(tmp_path / "scorecard.json"),
            format="json",
            bankroll_usd=335.0,
            reconciliation_start="",
            balance_sample_count=1,
            balance_sample_interval_s=0.0,
            timeout_s=1.0,
            skip_handoff_roll=True,
        ),
    )

    assert report_daily_scorecard.main() == 124
    captured = capsys.readouterr()
    assert "partial" in captured.out
    assert "slow" in captured.err
    assert "daily_scorecard timed out after 1.0s" in captured.err


def test_report_daily_scorecard_wrapper_passes_offline_no_chain(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, list[list[str]]] = {"cmds": []}

    def fake_run(cmd, **kwargs):
        captured["cmds"].append(list(cmd))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(report_daily_scorecard.subprocess, "run", fake_run)
    monkeypatch.setattr(
        report_daily_scorecard,
        "parse_args",
        lambda: SimpleNamespace(
            day="2026-07-16",
            output=str(tmp_path / "scorecard.json"),
            format="json",
            bankroll_usd=335.0,
            reconciliation_start="",
            balance_sample_count=1,
            balance_sample_interval_s=0.0,
            timeout_s=30.0,
            skip_handoff_roll=True,
            offline_no_chain=True,
        ),
    )

    assert report_daily_scorecard.main() == 0
    assert "--offline-no-chain" in captured["cmds"][0]
    assert captured["cmds"][1][1].endswith("report_288_participation_map.py")
    assert "2026-07-16" in captured["cmds"][1]


def test_roll_handoff_archive_moves_entries_older_than_two_full_days(tmp_path: Path) -> None:
    handoff = tmp_path / "HANDOFF.md"
    archive = tmp_path / "HANDOFF_ARCHIVE_2026-07.md"
    handoff.write_text(
        "# Handoff\n\n"
        "## 2026-07-05T23:00:00Z fable DIRECTION\n"
        "- old\n\n"
        "## STATUS 2026-07-05T23:30Z codex LIVE/SELF-DEV\n"
        "- old legacy heading\n\n"
        "## DIRECTION — 2026-07-05T23:45Z legacy fable heading\n"
        "- old legacy direction\n\n"
        "## 2026-07-06T00:00:00Z codex STATUS\n"
        "- keep\n\n"
        "## 2026-07-07T00:00:00Z codex STATUS\n"
        "- keep too\n",
        encoding="utf-8",
    )

    summary = roll_handoff(
        handoff_path=handoff,
        archive_path=archive,
        today=datetime(2026, 7, 7, tzinfo=UTC).date(),
        dry_run=False,
    )

    assert summary["entries_moved"] == 3
    assert "2026-07-05T23:00:00Z fable DIRECTION" in archive.read_text(encoding="utf-8")
    assert "STATUS 2026-07-05T23:30Z codex LIVE/SELF-DEV" in archive.read_text(encoding="utf-8")
    assert "DIRECTION — 2026-07-05T23:45Z legacy fable heading" in archive.read_text(encoding="utf-8")
    remaining = handoff.read_text(encoding="utf-8")
    assert "2026-07-05T23:00:00Z fable DIRECTION" not in remaining
    assert "STATUS 2026-07-05T23:30Z codex LIVE/SELF-DEV" not in remaining
    assert "DIRECTION — 2026-07-05T23:45Z legacy fable heading" not in remaining
    assert "2026-07-06T00:00:00Z codex STATUS" in remaining


def test_balance_feed_monitor_counts_consecutive_unavailable_generations(tmp_path: Path, monkeypatch) -> None:
    state_path = tmp_path / "balance_feed_state.json"
    monkeypatch.setenv("WALLET_COPY_BALANCE_FEED_STATE_PATH", str(state_path))

    first = daily_scorecard._balance_feed_monitor(
        {"balance_status": "UNAVAILABLE", "balance_reason": "clob_balance_fetch_returned_negative"}
    )
    second = daily_scorecard._balance_feed_monitor(
        {"balance_status": "UNAVAILABLE", "balance_reason": "clob_balance_fetch_returned_negative"}
    )
    recovered = daily_scorecard._balance_feed_monitor({"balance_status": "OK", "balance_reason": ""})

    assert first["status"] == "UNAVAILABLE"
    assert first["consecutive_unavailable_generations"] == 1
    assert first["defect"] is False
    assert second["status"] == "DEFECT"
    assert second["consecutive_unavailable_generations"] == 2
    assert second["defect"] is True
    assert recovered["status"] == "PASS"
    assert recovered["consecutive_unavailable_generations"] == 0


def test_proof_gates_report_sample_based_phase_and_lane_progress(tmp_path: Path) -> None:
    live_orders = [
        _filled(f"day1-{idx}", f"2026-07-01T00:{idx % 60:02d}:00+00:00", 0.01)
        for idx in range(100)
    ]
    live_orders.extend(
        _filled(f"day2-{idx}", f"2026-07-02T00:{idx % 60:02d}:00+00:00", 0.01)
        for idx in range(100)
    )
    live_orders.extend(
        _filled(f"day3-{idx}", f"2026-07-03T00:{idx % 60:02d}:00+00:00", 0.01)
        for idx in range(100)
    )
    paper_state = tmp_path / "paper_state.json"
    atomic_write_json(
        paper_state,
        {
            "paper_only": True,
            "orders": [_filled(f"paper-{idx}", f"2026-07-04T00:{idx % 60:02d}:00+00:00", 0.02) for idx in range(50)],
        },
    )

    resolutions = {f"condition-{row['order_id']}": {"direction": "UP"} for row in live_orders}
    resolutions.update(
        {
            f"condition-paper-{idx}": {"direction": "UP"}
            for idx in range(50)
        }
    )

    proof = daily_scorecard._proof_gates(
        live_orders,
        resolutions,
        paper_lane_paths=(("test_lane", paper_state),),
    )

    assert proof["phase2_promotion"]["ready_for_phase2"] is True
    assert proof["phase2_promotion"]["resolved_live_fills"] == 300
    assert proof["phase2_promotion"]["positive_pnl_days"] == 3
    assert proof["sizing_ramp"]["positive_completed_blocks"] == 3
    assert proof["sizing_ramp"]["reported_unlocked_size_usd"] == 20.0
    lane = proof["lane_promotion"]["paper_lanes"][0]
    assert lane["resolved_paper_fills"] == 50
    assert lane["small_live_promotion_ready"] is True


def test_engine_race_gate_reports_e5_e6_promotion_state(tmp_path: Path) -> None:
    e5 = tmp_path / "e5.json"
    e6 = tmp_path / "e6.json"
    atomic_write_json(
        e5,
        {
            "updated_at": "2026-07-05T22:24:22Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "can_trade": False,
            "promotion_gate": {
                "paper_quotes": 16,
                "filled_orders": 9,
                "maker_fill_rate_pct": 56.25,
                "resolved_paper_fills": 0,
                "resolved_paper_fills_required": 50,
                "resolved_paper_pnl_usd": 0.0,
                "copyintent_parity_violations": 0,
            },
        },
    )
    atomic_write_json(
        e6,
        {
            "updated_at": "2026-07-05T22:23:34Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "can_trade": False,
            "promotion_gate": {
                "paper_filled_orders": 14,
                "resolved_paper_fills": 13,
                "resolved_paper_fills_required": 50,
                "resolved_paper_wins": 11,
                "resolved_paper_losses": 2,
                "resolved_paper_pnl_usd": 16.820107,
                "resolved_paper_roi_pct": 16.17318,
            },
        },
    )

    gates = daily_scorecard._engine_race_gates((("E5", e5), ("E6", e6)))

    assert gates["parity_violations"] == 0
    assert gates["gate_ready_lanes"] == []
    assert gates["lanes"][0]["paper_quotes"] == 16
    assert gates["lanes"][0]["maker_fill_rate_pct"] == 56.25
    assert gates["lanes"][1]["resolved_paper_fills"] == 13
    assert gates["lanes"][1]["resolved_paper_wins"] == 11
    assert gates["lanes"][1]["resolved_paper_pnl_usd"] == 16.820107


def test_engine_race_gate_does_not_ready_on_weak_scorecard_signal(tmp_path: Path) -> None:
    state_path = tmp_path / "e5_pending.json"
    atomic_write_json(
        state_path,
        {
            "updated_at": "2026-07-06T20:43:31Z",
            "paper_only": True,
            "live_orders_allowed": False,
            "can_trade": False,
            "promotion_gate": {
                "paper_quotes": 3169,
                "filled_orders": 2903,
                "maker_fill_rate_pct": 91.52,
                "resolved_paper_fills": 1989,
                "resolved_paper_fills_required": 50,
                "resolved_paper_pnl_usd": 57.825151,
                "promotion_50_resolved_positive": "PENDING",
                "no_unresolved_inventory_older_than_one_window": False,
                "copyintent_parity_violations": 0,
            },
        },
    )

    gate = daily_scorecard._engine_race_gate("E5", state_path)

    assert gate["gate_ready"] is False
    assert "lane_promotion_gate_pending" in gate["gate_ready_reasons"]
    assert "unresolved_inventory_not_cleared" in gate["gate_ready_reasons"]


def test_late_window_cohort_buckets_experiment_fills_and_tripwire() -> None:
    loss = _filled_in_window("blocked-by-old-guard-loss", 1783278300, 45, 0.0)
    loss["side"] = "NO"
    loss["requested_size_usd"] = 12.0
    loss["requested_shares"] = 12.0
    orders = [
        _filled_in_window("blocked-by-old-guard-win", 1783278000, 45, 0.5),
        loss,
        _filled_in_window("still-blocked", 1783278600, 20, 0.25),
        _filled_in_window("before-guard", 1783278900, 90, 0.75),
        _filled_in_window("rest", 1783279200, 180, 0.1),
    ]
    for row in orders:
        row["copy_model"] = "inventory"
    e5_fill = _filled_in_window("e5-maker-fill", 1783279500, 45, 0.5)
    e5_fill["copy_model"] = "maker_first_btc5m"
    e5_fill["source_intent"] = {"metadata": {"copy_model": "maker_first_btc5m"}}
    orders.append(e5_fill)
    resolutions = {f"condition-{row['order_id']}": {"direction": "UP"} for row in orders}

    cohort = daily_scorecard._late_window_cohort(
        orders,
        resolutions,
        start_ts=0.0,
        end_ts=9999999999.0,
        experiment_start="2026-07-05T00:00:00Z",
    )

    assert cohort["buckets"]["030_060"]["fills"] == 2
    assert cohort["buckets"]["030_060"]["resolved_fills"] == 2
    assert cohort["buckets"]["030_060"]["pnl_usd"] == -11.5
    assert cohort["scope"] == "inventory_like_copy_models"
    assert all(row["market_slug"] != e5_fill["market_slug"] for row in cohort["rows"])
    assert cohort["buckets"]["000_030"]["fills"] == 1
    assert cohort["buckets"]["060_120"]["fills"] == 1
    assert cohort["experiment_cohort"]["resolved_fills"] == 2
    assert cohort["experiment_cohort"]["pnl_usd"] == -11.5
    assert cohort["experiment"]["tripwire_triggered"] is True


def test_execution_model_kpi_tracks_drip_and_strong_tier() -> None:
    window_start = 1783278000
    filled = _filled_in_window("drip-strong-fill", window_start, 120, 0.0)
    filled.update(
        {
            "copy_model": "drip",
            "requested_size_usd": 2.5,
            "requested_shares": 5.0,
            "source_intent": {
                "metadata": {
                    "copy_model": "drip",
                    "inventory_v2": {"source_inventory_vwap": 0.48},
                    "inventory_v3_drip": {"signal_tier": "strong"},
                }
            },
        }
    )
    rejected = {
        "order_id": "drip-reject",
        "final_status": "REJECTED",
        "submitted_at": datetime.fromtimestamp(window_start + 190, tz=UTC).isoformat(),
        "market_slug": f"btc-updown-5m-{window_start}",
        "condition_id": "condition-drip-reject",
        "side": "YES",
        "requested_size_usd": 2.5,
        "requested_shares": 5.0,
        "copy_model": "drip",
        "source_intent": {
            "metadata": {
                "copy_model": "drip",
                "inventory_v2": {"source_inventory_vwap": 0.48},
                "inventory_v3_drip": {"signal_tier": "baseline"},
            }
        },
    }
    guard_state = {
        "window_participation": {
            "window_rollups": [
                {"dominant_skip_reason_counts": {"drip_residual_gap_below_min_tranche": 2}}
            ]
        }
    }

    kpi = daily_scorecard._execution_model_kpi(
        [filled, rejected],
        {"condition-drip-strong-fill": {"direction": "UP"}},
        guard_state,
        start_ts=0.0,
        end_ts=9999999999.0,
    )

    assert kpi["copy_model_counts"] == {"drip": 2}
    assert kpi["filled_by_copy_model"] == {"drip": 1}
    assert kpi["orders_per_submitted_window"] == 2.0
    assert kpi["fill_rate_pct"] == 50.0
    assert kpi["drip"]["orders"] == 2
    assert kpi["drip"]["fills"] == 1
    assert kpi["drip"]["avg_entry_minus_source_vwap"] == 0.02
    assert kpi["drip"]["drip_stop_saves"] == 2
    assert kpi["strong_tier"]["orders"] == 1
    assert kpi["strong_tier"]["resolved_fills"] == 1
    assert kpi["strong_tier"]["resolved_pnl_usd"] == 2.5


def test_default_resolutions_path_selects_newest_btc_resolution_jsonl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    research = tmp_path / "data" / "research"
    research.mkdir(parents=True)
    old = research / "btc_resolutions_from_gamma_live_ledger_old.jsonl"
    new = research / "btc_resolutions_from_btcusdt_ticks.jsonl"
    old.write_text("{}\n", encoding="utf-8")
    new.write_text("{}\n", encoding="utf-8")
    os.utime(old, (1000, 1000))
    os.utime(new, (2000, 2000))
    monkeypatch.setattr(daily_scorecard, "ROOT", tmp_path)

    assert daily_scorecard._default_resolutions_path() == "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"


def test_load_receipt_costs_maps_tx_and_order_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daily_scorecard, "ROOT", tmp_path)
    research = tmp_path / "data" / "research"
    research.mkdir(parents=True)
    artifact = research / "wallet_copy_cash_flow_replay_tx_match_2026-07-06T1838Z.json"
    atomic_write_json(
        artifact,
        {
            "rows": [
                {
                    "tx": "0xTx",
                    "sample_order": "0xOrder",
                    "pUSD_out": 4.75,
                }
            ],
            "summary": {
                "tx_count": 1,
                "pUSD_out_sum": 4.75,
                "ledger_cost_sum": 4.0,
                "out_minus_cost_sum": 0.75,
            },
        },
    )

    costs, meta = daily_scorecard._load_receipt_costs()

    assert costs["0xtx"] == 4.75
    assert costs["0xorder"] == 4.75
    assert meta["status"] == "LOADED"
    assert meta["cost_basis_source"] == "tx_receipt_pusd_debit"
    assert meta["fallback_cost_basis_source"] == "response_filled_size_usd"


def test_load_actual_trade_costs_maps_single_order_tx_and_order_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(daily_scorecard, "ROOT", tmp_path)
    research = tmp_path / "data" / "research"
    research.mkdir(parents=True)
    artifact = research / "wallet_copy_today_fill_cash_diff_latest.json"
    atomic_write_json(
        artifact,
        {
            "rows": [
                {
                    "join_status": "JOINED_DATA_API",
                    "tx": "0xTx",
                    "order_ids": ["0xOrder"],
                    "actual_cost_usd": 1.96,
                },
                {
                    "join_status": "JOINED_CLOB_ASSOCIATE_TRADE",
                    "tx": "0xMulti",
                    "order_ids": ["0xA", "0xB"],
                    "actual_cost_usd": 3.0,
                },
            ],
            "summary": {"joined_tx_groups": 2, "ledger_fills_missing_tx": 20},
        },
    )

    costs, meta = daily_scorecard._load_actual_trade_costs()

    assert costs["0xtx"]["actual_cost_usd"] == 1.96
    assert costs["0xtx"]["source"] == "actual_trade_record"
    assert costs["0xorder"]["actual_cost_usd"] == 1.96
    assert "0xmulti" not in costs
    assert meta["status"] == "LOADED"
    assert meta["cost_basis_source"] == "actual_trade_record"
    assert meta["fills_missing_tx"] == 20
    assert meta["skipped_multi_order_txs"] == 1


def test_chain_reconciliation_scope_uses_pnl_after_baseline_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    orders = [
        _filled("before-reset", "2026-07-05T12:54:00+00:00", 5.0),
        _filled("after-reset", "2026-07-05T12:56:00+00:00", -0.25),
    ]
    resolutions = {
        "condition-before-reset": {"direction": "UP"},
        "condition-after-reset": {"direction": "UP"},
    }
    captured: dict[str, float] = {}

    def fake_chain_reconciliation(**kwargs: float) -> dict:
        captured.update(kwargs)
        return {"status": "PASS"}

    monkeypatch.setattr(daily_scorecard, "chain_reconciliation", fake_chain_reconciliation)

    reconciliation, truth, bounds = daily_scorecard._chain_reconciliation_scope(
        orders,
        resolutions,
        baseline_usd=335.0,
        reconciliation_start="2026-07-05T12:55:00Z",
        balance_sample_count=3,
        balance_sample_interval_s=60.0,
        balance_unavailable_resample_count=0,
        balance_unavailable_resample_interval_s=0.0,
        balance_mismatch_resample_count=0,
        balance_mismatch_resample_interval_s=0.0,
    )

    assert truth["total"]["resolved_fills"] == 1
    assert captured["canonical_pnl_usd"] == -0.25
    assert captured["baseline_usd"] == 335.0
    assert captured["balance_sample_count"] == 3
    assert captured["balance_sample_interval_s"] == 60.0
    assert captured["unavailable_resample_count"] == 0
    assert captured["unavailable_resample_interval_s"] == 0.0
    assert captured["mismatch_resample_count"] == 0
    assert captured["mismatch_resample_interval_s"] == 0.0
    assert bounds["unresolved_fills"] == 0
    assert reconciliation["canonical_pnl_scope"] == "since_reconciliation_start"


def test_daily_scorecard_defaults_to_bounded_heartbeat_balance_sampling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WALLET_COPY_SCORECARD_BALANCE_SAMPLE_COUNT", raising=False)
    monkeypatch.delenv("WALLET_COPY_SCORECARD_BALANCE_SAMPLE_INTERVAL_S", raising=False)
    monkeypatch.delenv("WALLET_COPY_SCORECARD_BALANCE_UNAVAILABLE_RESAMPLE_COUNT", raising=False)
    monkeypatch.delenv("WALLET_COPY_SCORECARD_BALANCE_UNAVAILABLE_RESAMPLE_INTERVAL_S", raising=False)
    monkeypatch.delenv("WALLET_COPY_SCORECARD_BALANCE_MISMATCH_RESAMPLE_COUNT", raising=False)
    monkeypatch.delenv("WALLET_COPY_SCORECARD_BALANCE_MISMATCH_RESAMPLE_INTERVAL_S", raising=False)
    monkeypatch.setattr(sys, "argv", ["daily_scorecard.py"])

    args = daily_scorecard.parse_args()

    assert args.balance_sample_count == 1
    assert args.balance_sample_interval_s == 0.0
    assert args.balance_unavailable_resample_count == 0
    assert args.balance_unavailable_resample_interval_s == 0.0
    assert args.balance_mismatch_resample_count == 0
    assert args.balance_mismatch_resample_interval_s == 0.0


def test_order_time_scopes_split_day_prior_and_since_topup() -> None:
    day_start = daily_scorecard._parse_any_ts("2026-07-18T00:00:00Z")
    day_end = daily_scorecard._parse_any_ts("2026-07-19T00:00:00Z")
    since_start = daily_scorecard._parse_any_ts("2026-07-05T12:55:00Z")
    before_since = _filled("before-since", "2026-07-05T12:54:00+00:00", 1.0)
    prior_day = _filled("prior-day", "2026-07-17T01:00:00+00:00", 1.0)
    current_day = _filled("current-day", "2026-07-18T01:00:00+00:00", 1.0)
    after_day = _filled("after-day", "2026-07-19T01:00:00+00:00", 1.0)
    missing_ts = {"order_id": "missing-ts"}

    scopes = daily_scorecard._order_time_scopes(
        [before_since, prior_day, current_day, after_day, missing_ts],
        start_ts=day_start,
        end_ts=day_end,
        reconciliation_start_ts=since_start,
    )

    assert [row["order_id"] for row in scopes["day_orders"]] == ["current-day"]
    assert [row["order_id"] for row in scopes["previous_day_orders"]] == ["prior-day"]
    assert [row["order_id"] for row in scopes["since_reconciliation_orders"]] == [
        "prior-day",
        "current-day",
        "after-day",
    ]
    assert scopes["summary"]["ledger_orders"] == 5
    assert scopes["summary"]["missing_ts_orders"] == 1


def test_offline_no_chain_scorecard_labels_basis_and_does_not_increment_balance_streak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "balance_feed_state.json"
    atomic_write_json(state_path, {"consecutive_unavailable_generations": 4})
    monkeypatch.setenv("WALLET_COPY_BALANCE_FEED_STATE_PATH", str(state_path))
    orders = [_filled("offline-fill", "2026-07-05T12:56:00+00:00", 0.5)]
    resolutions = {"condition-offline-fill": {"direction": "UP"}}

    reconciliation, truth, bounds = daily_scorecard._offline_no_chain_reconciliation_scope(
        orders,
        resolutions,
        baseline_usd=335.0,
        reconciliation_start="2026-07-05T12:55:00Z",
    )
    monitor = daily_scorecard._balance_feed_monitor(reconciliation)

    assert truth["total"]["pnl_usd"] == 0.5
    assert bounds["unresolved_fills"] == 0
    assert reconciliation["status"] == "OFFLINE_NO_CHAIN"
    assert reconciliation["chain_reconciliation_available"] is False
    assert reconciliation["basis"] == "local_ledger_fill_data_only"
    assert reconciliation["balance_status"] == "UNAVAILABLE"
    assert reconciliation["balance_reason"] == "offline_no_chain_scorecard_mode"
    assert reconciliation["live_cash_balance_usd"] is None
    assert reconciliation["account_value_usd"] is None
    assert monitor["status"] == "OFFLINE_NO_CHAIN_SKIPPED"
    assert monitor["defect"] is False
    assert monitor["consecutive_unavailable_generations"] == 4


def test_since_topup_truth_uses_balance_as_primary_verdict() -> None:
    truth = {"total": {"pnl_usd": 12.5, "resolved_fills": 7}}
    reconciliation = {
        "baseline_usd": 335.0,
        "canonical_pnl_usd": 12.5,
        "expected_value_usd": 347.5,
        "expected_cash_identity_usd": 347.5,
        "live_cash_balance_usd": 333.0,
        "account_value_usd": 333.0,
        "status": "MISMATCH",
        "balance_status": "OK",
        "reconciliation_start_iso": "2026-07-05T12:55:00Z",
    }

    verdict = daily_scorecard._since_topup_truth(reconciliation, truth)

    assert verdict["primary_verdict"] == "NOT_PRODUCING"
    assert verdict["primary_verdict_basis"] == "chain_anchored_actual"
    assert verdict["canonical_pnl_reporting_only"] is True
    assert verdict["canonical_pnl_scope"] == "since_last_wallet_topup"
    assert verdict["baseline_usd"] == 335.0
    assert verdict["canonical_pnl_usd"] == 12.5
    assert verdict["actual_delta_vs_baseline_usd"] == -2.0
    assert verdict["daily_delta_success_claim_allowed"] is False


def test_pending_redemption_adjustment_counts_absent_credit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daily_scorecard, "_load_dotenv_value", lambda name: "0xwallet")
    monkeypatch.setattr(daily_scorecard, "_fetch_data_api_activity", lambda **kwargs: ([], {"status": "OK"}))

    result = daily_scorecard._pending_redemption_adjustment(
        {
            "events": [
                {
                    "condition_id": "0xcondition",
                    "market_slug": "btc-updown-5m-1783102500",
                    "payout_usd": 5.0,
                }
            ]
        },
        reconciliation_start_ts=1783100000.0,
        scorecard_ts=1783102900.0,
    )

    assert result["pending_redemption_usd"] == 5.0
    assert result["pending_redemption_conditions"][0]["condition_id"] == "0xcondition"


def test_pending_redemption_adjustment_drops_matched_credit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daily_scorecard, "_load_dotenv_value", lambda name: "0xwallet")
    monkeypatch.setattr(
        daily_scorecard,
        "_fetch_data_api_activity",
        lambda **kwargs: (
            [
                {
                    "type": "REDEEM",
                    "conditionId": "0xcondition",
                    "usdcSize": 5.0,
                    "timestamp": 1783102810,
                }
            ],
            {"status": "OK"},
        ),
    )

    result = daily_scorecard._pending_redemption_adjustment(
        {
            "events": [
                {
                    "condition_id": "0xcondition",
                    "market_slug": "btc-updown-5m-1783102500",
                    "payout_usd": 5.0,
                }
            ]
        },
        reconciliation_start_ts=1783100000.0,
        scorecard_ts=1783102900.0,
    )

    assert result["pending_redemption_usd"] == 0.0
    assert result["redemption_lag"]["matched_conditions"] == 1
    assert result["redemption_lag"]["max_observed_lag_s"] == 10.0


def test_in_flight_and_unjoined_adjustments_are_separate() -> None:
    orders = [
        {
            "order_id": "after-sample",
            "status": "FILLED",
            "submitted_at": "2026-07-07T10:00:20Z",
            "trade_result": {"tx_hashes": ["0xafter"]},
        },
        {
            "order_id": "before-sample",
            "status": "FILLED",
            "submitted_at": "2026-07-07T09:59:00Z",
            "trade_result": {"tx_hashes": ["0xbefore"]},
        },
        {
            "order_id": "joined",
            "status": "FILLED",
            "submitted_at": "2026-07-07T09:58:00Z",
            "trade_result": {"tx_hashes": ["0xjoined"]},
        },
        {
            "order_id": "prior-day",
            "status": "FILLED",
            "submitted_at": "2026-07-06T23:59:00Z",
            "trade_result": {"tx_hashes": ["0xprior"]},
        },
    ]
    truth = {
        "events": [
            {"order_id": "after-sample", "cost_usd": 2.0},
            {"order_id": "before-sample", "cost_usd": 3.0},
            {"order_id": "joined", "cost_usd": 4.0},
            {"order_id": "prior-day", "cost_usd": 9.0},
        ]
    }

    result = daily_scorecard._in_flight_fill_adjustment(
        orders,
        truth,
        actual_trade_costs={"0xjoined": {"actual_cost_usd": 4.0}},
        balance_sample_ts=daily_scorecard._parse_any_ts("2026-07-07T10:00:00Z"),
        unjoined_actual_gap_start_ts=daily_scorecard._parse_any_ts("2026-07-07T00:00:00Z"),
    )

    assert result["in_flight_fill_usd"] == 2.0
    assert result["unjoined_actual_gap_usd"] == 3.0
    assert [row["order_id"] for row in result["unjoined_actual_gap_rows"]] == ["before-sample"]


def test_point_in_time_adjusted_delta_moves_status_to_adjusted_basis() -> None:
    reconciliation = {
        "status": "MISMATCH",
        "delta_vs_expected_usd": -5.0,
        "tolerance_usd": 2.0,
        "point_in_time_adjustments": {"basis_unification_usd": 0.0},
    }

    adjusted = daily_scorecard._apply_point_in_time_adjustments(
        reconciliation,
        pending_redemption_usd=5.0,
        in_flight_fill_usd=0.0,
        unjoined_actual_gap_usd=0.0,
    )

    assert adjusted["status"] == "PASS"
    assert adjusted["raw_status_before_point_in_time_adjustment"] == "MISMATCH"
    assert adjusted["point_in_time_adjustments"]["adjusted_delta_vs_expected_usd"] == 0.0
    assert adjusted["point_in_time_adjustments"]["raw_delta_vs_expected_usd"] == -5.0


def test_daily_scorecard_text_prints_since_topup_truth_near_daily_pnl() -> None:
    scorecard = {
        "day_utc": "2026-07-06",
        "window": {"start_iso": "2026-07-06T00:00:00Z", "end_iso": "2026-07-07T00:00:00Z"},
        "today": {
            "total": {"orders": 1, "fills": 1, "resolved_fills": 1, "rejects": 0, "pnl_usd": 1.0},
            "per_member": {},
            "price_band_buckets": {},
        },
        "since_topup_truth": {
            "primary_verdict": "NOT_PRODUCING",
            "baseline_usd": 335.0,
            "baseline_iso": "2026-07-05T12:55:00Z",
            "canonical_pnl_usd": -5.5,
            "canonical_pnl_pct": -1.641791,
            "actual_value_usd": 324.25,
            "actual_delta_vs_baseline_usd": -10.75,
            "actual_value_basis": "account_value",
            "live_cash_balance_usd": 324.25,
            "reconciliation_status": "MISMATCH",
        },
        "d97_prior_day_baseline": {
            "total": {"orders": 0, "fills": 0, "resolved_fills": 0, "pnl_usd": 0.0},
        },
        "baseline_comparison": {"today_set_minus_d97_prior_day_pnl_usd": 1.0},
        "automation_drift": [],
        "volume_kpi": {},
        "active_set_roster": {},
        "guard_skip_histogram": {},
    }

    lines = daily_scorecard._text(scorecard).splitlines()

    assert lines[1].startswith("total orders=1")
    assert lines[2].startswith("since_topup_truth: verdict=NOT_PRODUCING")
    assert "canonical_pnl=-5.500000/-1.642%" in lines[2]


def test_goal_arithmetic_invariant_flags_unreachable_live_caps() -> None:
    scorecard = {
        "today": {"total": {"roi_pct": 28.5}},
        "volume_kpi": {
            "canonical_daily": {
                "denominator_windows": 288,
                "windows_submitted": 80,
                "windows_filled": 64,
            }
        },
    }
    guard = {
        "candidate": {
            "candidate_id": "live_member",
            "source_wallet": "0xabc",
            "policy": {"max_order_usd": 4.0, "drip_max_tranche_usd": 1.0},
        },
        "guard_runtime_filter": {
            "copy_model": "drip",
            "max_order_usd": 8.0,
            "drip_max_tranche_usd": 2.5,
            "per_window_fill_cap": 1,
        },
    }

    invariant = daily_scorecard._goal_arithmetic_invariant(scorecard, guard)

    assert invariant["status"] == "RED_CONFIG_GOAL_CONTRADICTION"
    assert invariant["max_achievable_day_usd"] == 82.08
    assert invariant["observed_participation_capacity_usd"] == 22.8
    assert invariant["inputs"]["effective_fill_cap_usd"] == 1.0


def test_goal_arithmetic_invariant_passes_reachable_config() -> None:
    scorecard = {
        "today": {"total": {"roi_pct": 20.0}},
        "volume_kpi": {
            "canonical_daily": {
                "denominator_windows": 288,
                "windows_submitted": 200,
                "windows_filled": 150,
            }
        },
    }
    guard = {
        "candidate": {"policy": {"max_order_usd": 4.0, "drip_max_tranche_usd": 4.0}},
        "guard_runtime_filter": {"copy_model": "drip", "per_window_fill_cap": 1},
    }

    invariant = daily_scorecard._goal_arithmetic_invariant(scorecard, guard)

    assert invariant["status"] == "PASS_GOAL_ARITHMETIC_REACHABLE"
    assert invariant["max_achievable_day_usd"] == 230.4


def test_daily_scorecard_applies_self_feed_reconciliation_overlay() -> None:
    since = {
        "primary_verdict": "NOT_PRODUCING",
        "actual_delta_vs_baseline_usd": -24.910006,
    }
    overlay = {
        "status": "PASS",
        "mode": "RECONCILIATION_OVERLAY",
        "ledger_rewrite": False,
        "overlay_delta_usd": 0.2,
        "raw_missing_pnl_upper_bound_usd": 31.822431,
        "double_count_excluded_usd": 31.622431,
        "resolved_tx_groups": 73,
        "unresolved_tx_groups": 0,
        "named_sources": {
            "h2_external_redemptions": {
                "status": "PASS",
                "overlay_source_name": "external_data_api_redeem_condition_join",
                "ledger_rewrite": False,
                "external_redeem_rows": 10,
                "confirmed_external_redeem_rows": 10,
                "residual_explained_by_external_redeems_usd": 0.0,
            }
        },
    }

    adjusted = daily_scorecard._apply_self_feed_reconciliation_overlay(since, overlay)

    assert adjusted["actual_delta_vs_baseline_usd"] == -24.910006
    assert adjusted["actual_basis_reconciled_delta_vs_baseline_usd"] == -24.710006
    assert adjusted["actual_basis_reconciled_verdict"] == "NOT_PRODUCING_RECONCILED_BASIS"
    assert adjusted["self_feed_reconciliation_overlay"]["ledger_rewrite"] is False
    assert adjusted["self_feed_reconciliation_overlay"]["ledger_actual_delta_vs_baseline_usd"] == -24.910006
    assert adjusted["self_feed_reconciliation_overlay"]["raw_missing_pnl_upper_bound_usd"] == 31.822431
    assert adjusted["self_feed_reconciliation_overlay"]["double_count_excluded_usd"] == 31.622431
    assert adjusted["self_feed_reconciliation_overlay"]["named_sources"]["h2_external_redemptions"] == {
        "status": "PASS",
        "overlay_source_name": "external_data_api_redeem_condition_join",
        "ledger_rewrite": False,
        "external_redeem_rows": 10,
        "confirmed_external_redeem_rows": 10,
        "residual_explained_by_external_redeems_usd": 0.0,
    }


def test_daily_scorecard_marks_stale_self_feed_fallback_unknown() -> None:
    since = {
        "primary_verdict": "UNKNOWN_BALANCE",
        "actual_delta_vs_baseline_usd": None,
    }
    overlay = {
        "status": "PASS",
        "mode": "RECONCILIATION_OVERLAY",
        "ledger_rewrite": False,
        "generated_at": "2026-07-07T21:58:25Z",
        "generated_date_utc": "2026-07-07",
        "current_date_utc": "2026-07-10",
        "freshness_status": "STALE_FALLBACK",
        "raw_actual_delta_usd": -33.160465,
        "overlay_delta_usd": -20.158263,
    }

    adjusted = daily_scorecard._apply_self_feed_reconciliation_overlay(since, overlay)

    assert adjusted["actual_basis_reconciled_delta_vs_baseline_usd"] is None
    assert adjusted["actual_basis_reconciled_producing"] is None
    assert adjusted["actual_basis_reconciled_verdict"] == "UNKNOWN_BALANCE_STALE_FALLBACK"
    assert adjusted["self_feed_reconciliation_overlay"]["fallback_status"] == "STALE_FALLBACK"
    assert adjusted["self_feed_reconciliation_overlay"]["reconciled_actual_delta_vs_baseline_usd"] is None
    assert adjusted["self_feed_reconciliation_overlay"]["stale_reconciled_actual_delta_vs_baseline_usd"] == -53.318728
    assert adjusted["self_feed_reconciliation_overlay"]["reconciled_verdict"] == "UNKNOWN_BALANCE_STALE_FALLBACK"


def test_daily_scorecard_applies_cash_diff_named_residual() -> None:
    since = {
        "primary_verdict": "NOT_PRODUCING",
        "actual_delta_vs_baseline_usd": -24.910006,
        "reconciliation_status": "MISMATCH",
    }
    residual = {
        "status": "NAMED_RESIDUAL",
        "scorecard_reconciliation_delta_usd": -7.590385,
        "fill_cost_payout_explained_usd": 0.0,
        "residual_usd": -7.590385,
        "residual_classification": "unaccounted_one_time_cash_movement",
        "joined_tx_groups": 147,
        "ledger_tx_groups": 147,
        "unjoined_tx_groups": 0,
        "ledger_fills_missing_tx": 0,
    }

    adjusted = daily_scorecard._apply_cash_diff_reconciliation_residual(since, residual)

    assert adjusted["raw_reconciliation_status"] == "MISMATCH"
    assert adjusted["reconciliation_status"] == "MISMATCH"
    assert adjusted["residual_honesty_status"] == "PASS_NO_UNPROVEN_RECONCILED_LABEL"
    assert adjusted["cash_diff_reconciliation_residual"]["residual_usd"] == -7.590385
    assert adjusted["cash_diff_reconciliation_residual"]["joined_tx_groups"] == 147


def test_daily_scorecard_surfaces_named_residual_in_chain_reconciliation() -> None:
    chain = {"status": "MISMATCH", "delta_vs_expected_usd": -66.246523}
    since = {
        "reconciliation_status": "MISMATCH",
        "cash_diff_reconciliation_residual": {
            "residual_usd": 10.328578,
            "residual_classification": "unaccounted_one_time_cash_movement",
        },
    }

    annotated = daily_scorecard._annotate_chain_reconciliation(chain, since)

    assert annotated["status"] == "MISMATCH"
    assert annotated["adjusted_status"] == "MISMATCH"
    assert annotated["cash_diff_residual_usd"] == 10.328578
    assert annotated["residual_class"] == "unaccounted_one_time_cash_movement"


def test_daily_scorecard_flips_raw_recon_when_retrace_equation_reconciles() -> None:
    since = {
        "primary_verdict": "NOT_PRODUCING",
        "actual_delta_vs_baseline_usd": -33.160465,
        "reconciliation_status": "MISMATCH",
    }
    residual = {
        "status": "NAMED_RESIDUAL",
        "scorecard_reconciliation_delta_usd": -7.590385,
        "fill_cost_payout_explained_usd": 0.0,
        "residual_usd": -7.590385,
        "residual_classification": "unaccounted_one_time_cash_movement",
    }
    retrace = {"status": "RECONCILED", "unexplained_usd": -0.141817}

    adjusted = daily_scorecard._apply_cash_diff_reconciliation_residual(since, residual, retrace)

    assert adjusted["raw_reconciliation_status"] == "RECONCILED"
    assert adjusted["unadjusted_reconciliation_status"] == "MISMATCH"
    assert adjusted["reconciliation_status"] == "RECONCILED_BY_RETRACE_EQUATION"
    assert adjusted["residual_honesty_status"] == "PASS_PROVEN_RETRACE"
    assert adjusted["cash_diff_reconciliation_residual"]["retrace_unexplained_usd"] == -0.141817


def test_daily_scorecard_text_prints_reconciled_actual_overlay() -> None:
    scorecard = {
        "day_utc": "2026-07-06",
        "window": {"start_iso": "2026-07-06T00:00:00Z", "end_iso": "2026-07-07T00:00:00Z"},
        "today": {
            "total": {"orders": 1, "fills": 1, "resolved_fills": 1, "rejects": 0, "pnl_usd": 1.0},
            "per_member": {},
            "price_band_buckets": {},
        },
        "since_topup_truth": {
            "primary_verdict": "NOT_PRODUCING",
            "baseline_usd": 335.0,
            "baseline_iso": "2026-07-05T12:55:00Z",
            "canonical_pnl_usd": -27.159952,
            "canonical_pnl_pct": -8.107448,
            "actual_value_usd": 310.089994,
            "actual_delta_vs_baseline_usd": -24.910006,
            "actual_value_basis": "account_value",
            "live_cash_balance_usd": 310.089994,
            "reconciliation_status": "MISMATCH",
            "self_feed_reconciliation_overlay": {
                "reconciled_verdict": "NOT_PRODUCING_RECONCILED_BASIS",
                "ledger_actual_delta_vs_baseline_usd": -24.910006,
                "overlay_delta_usd": 0.2,
                "double_count_excluded_usd": 31.622431,
                "reconciled_actual_delta_vs_baseline_usd": -24.710006,
                "mode": "RECONCILIATION_OVERLAY",
                "ledger_rewrite": False,
            },
        },
        "d97_prior_day_baseline": {
            "total": {"orders": 0, "fills": 0, "resolved_fills": 0, "pnl_usd": 0.0},
        },
        "baseline_comparison": {"today_set_minus_d97_prior_day_pnl_usd": 1.0},
        "automation_drift": [],
        "volume_kpi": {},
        "active_set_roster": {},
        "guard_skip_histogram": {},
    }

    text = daily_scorecard._text(scorecard)

    assert "self_feed_reconciled_actual: verdict=NOT_PRODUCING_RECONCILED_BASIS" in text
    assert "ledger_actual=-24.910006 overlay_delta=0.2 double_count_excluded=31.622431" in text
    assert "reconciled_actual=-24.710006" in text


def test_daily_scorecard_reports_active_set_skip_histogram_and_lane_gates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(daily_scorecard, "ROOT", tmp_path)
    research = tmp_path / "data" / "research"
    research.mkdir(parents=True)
    atomic_write_json(
        research / "maker_first_btc5m_paper_state.json",
        {
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {"paper_quotes": 10, "filled_orders": 3},
            "promotion_gate": {"resolved_paper_fills": 0, "promotion_50_resolved_positive": "PENDING"},
        },
    )
    atomic_write_json(
        research / "maker_first_btc5m_resolution_state.json",
        {
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {"paper_quotes": 10, "filled_orders": 3},
            "promotion_gate": {"resolved_paper_fills": 0, "promotion_50_resolved_positive": "PENDING"},
        },
    )
    atomic_write_json(
        research / "e6_whale_net_flow_paper_lane_state.json",
        {
            "paper_only": True,
            "live_orders_allowed": False,
            "summary": {"paper_orders": 14, "filled_orders": 14},
            "promotion_gate": {
                "resolved_paper_fills": 13,
                "resolved_paper_pnl_usd": 16.82,
                "promotion_50_resolved_positive": "PENDING",
            },
        },
    )
    guard = {
        "active_set": {
            "flow_stage": "LIVE/PROMOTE/ROTATE",
            "qualified_member_count": 1,
            "members": [
                {
                    "candidate_id": "member-1",
                    "source_wallet": "0xabc",
                    "policy_id": "policy",
                    "max_price": 0.5,
                    "is_current_cycle_member": True,
                }
            ],
        },
        "window_participation": {
            "window_rollups": [
                {
                    "market_slug": "btc-updown-5m-1",
                    "wallet_eligible_orders": 2,
                    "our_submits": 0,
                    "our_fills": 0,
                    "missed_active_window": True,
                    "dominant_skip_reason_counts": {"inventory_late_window_guard": 2},
                }
            ]
        },
    }

    roster = daily_scorecard._active_set_roster(guard)
    histogram = daily_scorecard._guard_skip_histogram(guard)
    snapshots = daily_scorecard._paper_lane_gate_snapshots()

    assert roster["qualified_member_count"] == 1
    assert roster["members"][0]["candidate_id"] == "member-1"
    assert histogram["missed_active_windows"] == 1
    assert histogram["skip_reason_counts"]["inventory_late_window_guard"] == 2
    assert snapshots["e5_maker_first_btc5m"]["summary"]["filled_orders"] == 3
    assert snapshots["e6_whale_net_flow"]["promotion_gate"]["resolved_paper_fills"] == 13


def test_live_book_age_gate_evidence_is_fill_independent() -> None:
    guard = {
        "guard_code_identity": {
            "live_guard_generation_sha256": "resident-generation"
        },
        "live_execution": {
            "candidate_intent_summary": {
                "inventory_best_ask_gate": {
                    "book_cache_ttl_s": 2.0,
                    "max_gate_probe_best_ask_age_at_gate_s": 1.0,
                    "threshold_independent_of_cache_ttl": True,
                    "gate_probe_best_ask_age_at_gate_s": {
                        "count": 2,
                        "min": 0.1,
                        "median": 0.2,
                        "p95": 0.3,
                        "max": 0.3,
                    },
                    "blocker_counts": {"stale_book_at_gate": 1},
                }
            }
        },
    }

    evidence = daily_scorecard._live_book_age_gate_evidence(guard)

    assert evidence["generation_sha256"] == "resident-generation"
    assert evidence["threshold_independent_of_cache_ttl"] is True
    assert evidence["gate_probe_best_ask_age_at_gate_s"]["count"] == 2
    assert evidence["evidence_available_without_fill"] is True


def test_active_set_roster_uses_runtime_truth_not_stale_qualified_rows() -> None:
    guard = {
        "generated_at": "2026-07-25T11:00:00Z",
        "active_set": {
            "qualified_member_count": 3,
            "members": [{"candidate_id": "stale", "source_wallet": "0xabc", "is_current_cycle_member": False}],
        },
    }
    roster = daily_scorecard._active_set_roster(guard)
    assert roster["membership_source"] == "guard_runtime_active_artifact_current_cycle"
    assert roster["generated_at"] == guard["generated_at"]
    assert roster["qualified_member_count"] == 0
    assert roster["current_cycle_member_count"] == 0
    assert roster["members"] == []
    assert roster["historical_qualified_members"][0]["candidate_id"] == "stale"


def test_window_coverage_alias_reports_canonical_daily_fields() -> None:
    volume_kpi = {
        "source": "guard.json",
        "windows_total": 288,
        "windows_traded": 1,
        "windows_submitted": 2,
        "canonical_daily": {
            "definition": "canonical test",
            "denominator_windows": 288,
            "windows_filled": 15,
            "windows_submitted": 16,
        },
    }

    coverage = daily_scorecard._window_coverage(volume_kpi)

    assert coverage["definition"] == "canonical test"
    assert coverage["windows_total"] == 288
    assert coverage["windows_traded"] == 15
    assert coverage["coverage_pct"] == 5.208333
    assert coverage["windows_submitted"] == 16
    assert coverage["submitted_coverage_pct"] == 5.555556


def test_volume_kpi_classifies_non_filled_windows_by_drop_stage(tmp_path: Path) -> None:
    guard_state = tmp_path / "guard.json"
    atomic_write_json(
        guard_state,
        {
            "window_participation": {
                "window_rollups": [
                    {
                        "market_slug": "btc-updown-5m-0",
                        "window_start_s": 0,
                        "wallet_eligible_orders": 3,
                        "our_submits": 2,
                        "our_fills": 1,
                    },
                    {
                        "market_slug": "btc-updown-5m-300",
                        "window_start_s": 300,
                        "wallet_eligible_orders": 2,
                        "our_submits": 1,
                        "our_fills": 0,
                    },
                    {
                        "market_slug": "btc-updown-5m-600",
                        "window_start_s": 600,
                        "wallet_eligible_orders": 2,
                        "our_submits": 0,
                        "our_fills": 0,
                        "missed_active_window": True,
                        "dominant_skip_reason_counts": {"price_above_band": 2},
                    },
                    {
                        "market_slug": "btc-updown-5m-900",
                        "window_start_s": 900,
                        "wallet_eligible_orders": 0,
                        "our_submits": 0,
                        "our_fills": 0,
                    },
                ]
            }
        },
    )

    volume = daily_scorecard._volume_kpi(str(guard_state), orders=[], start_ts=0.0, end_ts=9999999999.0)

    assert volume["missed_window_attribution"]["categories"] == {
        "filled": 1,
        "guard_reject": 1,
        "no_copy_signal": 285,
        "policy_cap_refusal": 0,
        "pre_submit_refusal": 0,
        "submitted_not_filled": 1,
    }
    assert volume["missed_window_attribution"]["denominator_windows"] == 288
    assert len(volume["missed_window_attribution"]["rows"]) == 288
    by_slug = {row["market_slug"]: row["missed_window_attribution"] for row in volume["rows"]}
    assert by_slug["btc-updown-5m-0"] == "filled"
    assert by_slug["btc-updown-5m-300"] == "submitted_not_filled"
    assert by_slug["btc-updown-5m-600"] == "guard_reject"
    assert by_slug["btc-updown-5m-900"] == "no_copy_signal"


def test_score_orders_exposes_by_lane_table() -> None:
    lane_a = _filled("lane-a-win", "2026-07-06T01:00:00+00:00", 2.0)
    lane_a["source_intent"] = {"policy_id": "lane-a"}
    lane_b = _filled("lane-b-loss", "2026-07-06T01:05:00+00:00", -1.0)
    lane_b["source_intent"] = {"policy_id": "lane-b"}
    lane_b["side"] = "NO"
    lane_b["requested_size_usd"] = 1.0
    lane_b["requested_shares"] = 1.0
    resolutions = {
        "condition-lane-a-win": {"direction": "UP"},
        "condition-lane-b-loss": {"direction": "UP"},
    }

    score = daily_scorecard._score_orders(
        [lane_a, lane_b],
        resolutions,
        start_ts=0.0,
        end_ts=9999999999.0,
    )

    assert score["by_lane"]["lane-a"]["pnl_usd"] == 2.0
    assert score["by_lane"]["lane-b"]["pnl_usd"] == -1.0


def test_day_pnl_basis_reports_response_primary_and_actual_secondary() -> None:
    improved = _filled("actual-improved", "2026-07-06T01:00:00+00:00", 1.0)
    fallback = _filled("actual-missing", "2026-07-06T01:05:00+00:00", 0.5)
    resolutions = {
        "condition-actual-improved": {"direction": "UP"},
        "condition-actual-missing": {"direction": "UP"},
    }

    response_truth = daily_scorecard.build_pnl_truth(
        {"orders": [improved, fallback]},
        resolutions,
        start_ts=0.0,
        end_ts=9999999999.0,
        receipt_costs={},
        actual_trade_costs={},
    )
    actual_truth = daily_scorecard.build_pnl_truth(
        {"orders": [improved, fallback]},
        resolutions,
        start_ts=0.0,
        end_ts=9999999999.0,
        receipt_costs={},
        actual_trade_costs={
            "actual-improved": {
                "actual_cost_usd": 0.75,
                "tx": "0ximproved",
            }
        },
    )

    response_score = daily_scorecard._score_from_truth(response_truth)
    actual_score = daily_scorecard._score_from_truth(actual_truth)
    coverage = daily_scorecard._actual_basis_coverage(actual_truth)
    actual_costs = {
        "actual-improved": {
            "actual_cost_usd": 0.75,
            "tx": "0ximproved",
            "join_key": "0ximproved",
            "join_key_source": "unit.actual_trades[].transactionHash",
        }
    }
    audit = daily_scorecard._actual_basis_spot_audit(response_truth, actual_truth, actual_costs)
    decomposition = daily_scorecard._basis_split_decomposition(
        response_truth,
        actual_truth,
        actual_costs,
        basis_split_delta_usd=0.25,
    )

    assert response_score["total"]["pnl_usd"] == 1.5
    assert actual_score["total"]["pnl_usd"] == 1.75
    assert daily_scorecard._basis_split_delta(response_score, actual_score) == 0.25
    assert coverage == {
        "basis": "actual_trade_record",
        "fallback_basis": "response_filled_size_usd",
        "joined": 1,
        "missing": 1,
        "total_resolved_fills": 2,
        "coverage_pct": 50.0,
        "source": "same_generation_actual_basis_day_events",
    }
    assert audit["sampled_joined_tx_groups"] == 1
    assert audit["rows"][0]["classification"] == "price_improvement"
    assert audit["rows"][0]["join_key"] == "0ximproved"
    assert audit["rows"][0]["matched_cost_key"] == "actual-improved"
    assert audit["rows"][0]["matched_cost_key_source"] == "order_id_alias"
    assert audit["rows"][0]["actual_minus_response_usd"] == -0.25
    assert decomposition["assertion"] == "PASS_WITHIN_0.01"
    assert decomposition["sum_joined_improvement_usd"] == 0.25
    assert decomposition["remainder_usd"] == 0.0
    assert decomposition["join_key_schema"]["source_field"] == "wallet_copy_today_fill_cash_diff.rows[].tx"
    assert decomposition["rows"][0]["join_key"] == "0ximproved"


def test_day_pnl_resolution_split_names_realized_and_open_mark_components() -> None:
    resolved = _filled("split-closed", "2026-07-06T01:00:00+00:00", 2.0)
    unresolved = _filled("split-open", "2026-07-06T01:05:00+00:00", 1.0)
    unresolved["market_slug"] = "btc-updown-5m-1783299900"
    truth = daily_scorecard.build_pnl_truth(
        {"orders": [resolved, unresolved]},
        {"condition-split-closed": {"direction": "UP"}},
        start_ts=0.0,
        end_ts=9999999999.0,
        receipt_costs={},
        actual_trade_costs={},
    )

    split = daily_scorecard._day_pnl_resolution_split(truth)

    assert split["status"] == "OPEN_MARK_ACTIVE"
    assert split["total_day_pnl_usd"] == 2.0
    assert split["realized_closed_pnl_usd"] == 2.0
    assert split["open_mark_pnl_usd"] == 0.0
    assert split["realized_closed_fills"] == 1
    assert split["unresolved_open_fills"] == 1
    assert split["open_cost_usd"] == 1.0
    assert split["open_shares"] == 2.0
    assert split["unresolved_position_value_bounds_usd"] == [0.0, 2.0]
    assert split["latest_unresolved_submitted_at"] == "2026-07-06T01:05:00+00:00"
    assert split["unresolved_market_slugs_sample"] == ["btc-updown-5m-1783299900"]

    text = daily_scorecard._text(
        {
            "day_utc": "2026-07-06",
            "window": {"start_iso": "2026-07-06T00:00:00Z", "end_iso": "2026-07-07T00:00:00Z"},
            "today": daily_scorecard._score_from_truth(truth),
            "day_pnl_resolution_split": split,
            "d97_prior_day_baseline": {"total": {"orders": 0, "fills": 0, "resolved_fills": 0, "pnl_usd": 0.0}},
            "baseline_comparison": {"today_set_minus_d97_prior_day_pnl_usd": 2.0},
            "automation_drift": [],
            "volume_kpi": {"canonical_daily": {"windows_filled": 0, "denominator_windows": 288, "windows_submitted": 0}},
            "per_window_pnl_histogram": {},
        }
    )

    assert "day_pnl_resolution_split:" in text
    assert "realized_closed=+2.000000" in text
    assert "unresolved_fills=1" in text


def test_score_orders_reports_per_window_pnl_histogram() -> None:
    win_a_1 = _filled_in_window("win-a-1", 1_800_000_000, 120, 2.0)
    win_a_2 = _filled_in_window("win-a-2", 1_800_000_000, 90, -0.5)
    win_b_1 = _filled_in_window("win-b-1", 1_800_000_300, 120, -0.5)
    win_b_2 = _filled_in_window("win-b-2", 1_800_000_300, 90, -0.6)
    unresolved = _filled_in_window("unresolved", 1_800_000_600, 90, 1.0)
    resolutions = {
        "condition-win-a-1": {"direction": "UP"},
        "condition-win-a-2": {"direction": "UP"},
        "condition-win-b-1": {"direction": "UP"},
        "condition-win-b-2": {"direction": "UP"},
    }

    score = daily_scorecard._score_orders(
        [win_a_1, win_a_2, win_b_1, win_b_2, unresolved],
        resolutions,
        start_ts=0.0,
        end_ts=9999999999.0,
    )

    histogram = score["per_window_pnl_histogram"]
    assert histogram["reporting_only"] is True
    assert histogram["gate_use_allowed"] is False
    assert histogram["submitted_windows"] == 3
    assert histogram["filled_windows"] == 3
    assert histogram["resolved_windows"] == 2
    assert histogram["unresolved_filled_windows"] == 1
    assert histogram["positive_windows"] == 1
    assert histogram["negative_windows"] == 1
    assert histogram["min_window_pnl_usd"] == -1.1
    assert histogram["bucket_counts"]["1_to_5"] == 1
    assert histogram["bucket_counts"]["-5_to_-1"] == 1
    assert histogram["top_windows"][0]["market_slug"] == "btc-updown-5m-1800000000"
    assert histogram["worst_windows"][0]["market_slug"] == "btc-updown-5m-1800000300"


def test_daily_how_line_reports_path_and_ranked_alternatives() -> None:
    scorecard = {
        "today": {"total": {"pnl_usd": 29.0}},
        "volume_kpi": {"canonical_daily": {"windows_filled": 134, "denominator_windows": 288}},
    }

    daily_how = daily_scorecard._daily_how_line(scorecard)

    assert daily_how["report_layer_only"] is True
    assert daily_how["gap_to_min_band_usd"] == 71.0
    assert daily_how["alternatives"][0]["id"] == "campaign_lat_retention_selection"
    assert daily_how["alternatives"][0]["expected_usd_today"] == round((29.0 / 134.0) * 10.0, 6)
    assert len(daily_how["alternatives"]) >= 3
    assert "paper twin" in daily_how["alternatives"][2]["basis"]
    assert "parent-config epochs" in daily_how["alternatives"][2]["basis"]
    assert "need $71.00" in daily_how["path_to_band_today"]


def test_score_orders_uses_actual_trade_costs_when_available() -> None:
    order = _filled("actual-cost-win", "2026-07-06T01:00:00+00:00", 1.0)
    order["trade_result"] = {
        "response_filled_size_usd": 4.0,
        "response_fill_size_shares": 5.0,
        "tx_hashes": ["0xactual"],
    }
    resolutions = {"condition-actual-cost-win": {"direction": "UP"}}

    score = daily_scorecard._score_orders(
        [order],
        resolutions,
        start_ts=0.0,
        end_ts=9999999999.0,
        actual_trade_costs={"0xactual": {"actual_cost_usd": 2.5, "source": "actual_trade_record"}},
    )

    assert score["total"]["pnl_usd"] == 2.5
    assert score["price_band_buckets"]["00_00_25"]["pnl_usd"] == 2.5


def test_expansion_cohort_reports_member_table_and_raise_rule() -> None:
    wallet_a = "0x251c1a283703beed41590b0875a8dcb8ddd1541f"
    wallet_b = "0x4aa59eb561fa247d6a8bbe12a50e2a3497ef1a27"
    ignored_before_start = _filled("before-start", "2026-07-06T15:50:00+00:00", 9.0)
    ignored_before_start["source_wallet"] = wallet_a
    ignored_before_start["source_intent"] = {"source_wallet": wallet_a, "policy_id": "policy-a"}
    rejected = _filled("rejected", "2026-07-06T15:57:52+00:00", 0.0)
    rejected["final_status"] = "REJECTED"
    rejected["source_wallet"] = wallet_a
    rejected["source_intent"] = {"source_wallet": wallet_a, "policy_id": "policy-a"}
    filled = _filled("filled", "2026-07-06T15:57:53+00:00", 3.0)
    filled["source_wallet"] = wallet_a
    filled["source_intent"] = {"source_wallet": wallet_a, "policy_id": "policy-a"}
    guard = {
        "active_set": {
            "members": [
                {
                    "candidate_id": "expansion_rank_dcb8ddd1541f",
                    "source_wallet": wallet_a,
                    "policy_id": "policy-a",
                },
                {
                    "candidate_id": "expansion_rank_2a3497ef1a27",
                    "source_wallet": wallet_b,
                    "policy_id": "policy-b",
                },
            ]
        }
    }
    resolutions = {
        "condition-before-start": {"direction": "UP"},
        "condition-filled": {"direction": "UP"},
    }

    cohort = daily_scorecard._expansion_cohort(
        [ignored_before_start, rejected, filled],
        resolutions,
        guard,
        start_ts=0.0,
        end_ts=9999999999.0,
        cohort_start="2026-07-06T15:51:00Z",
    )

    assert cohort["summary"]["members"] == 2
    assert cohort["summary"]["submit_attempts"] == 2
    assert cohort["summary"]["members_with_resolved_fill"] == 1
    assert cohort["summary"]["resolved_pnl_usd"] == 3.0
    assert cohort["raise_rule"]["raise_to_10_ready_now"] is False
    assert "2026-07-06T16:13:22Z" in cohort["source_direction"]
    assert "2026-07-06T16:48:30Z" in cohort["source_direction"]
    assert "mechanical_eod_branches" in cohort["raise_rule"]
    assert cohort["raise_rule"]["negative_member_blocks_ready_resolved_fills_gte"] == 3
    assert cohort["raise_rule"]["branch_precedence"][0] == "negative_attempted_member_with_resolved_fills_gte_3"
    assert ">=3 resolved fills" in cohort["raise_rule"]["if_not_ready_next"]
    assert "watch noise" in cohort["raise_rule"]["mechanical_eod_branches"]["ready"]
    member_a = cohort["members"][0]
    assert member_a["candidate_id"] == "expansion_rank_dcb8ddd1541f"
    assert member_a["rejects"] == 1
    assert member_a["resolved_fills"] == 1
