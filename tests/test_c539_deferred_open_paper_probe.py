from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts import run_c539_deferred_open_paper_probe as probe


class FakeClient:
    def get_book(self, token_id: str) -> dict:
        return {
            "asset_id": token_id,
            "timestamp": 1785347700001,
            "hash": "book-at-open",
            "asks": [{"price": "0.20", "size": "10"}],
            "bids": [{"price": "0.19", "size": "10"}],
        }


def _write(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        hot_history=str(tmp_path / "history.json"),
        guard=str(tmp_path / "guard.json"),
        resolutions=str(tmp_path / "resolutions.jsonl"),
        state=str(tmp_path / "state.json"),
        clob_base="https://example.invalid",
        timeout_s=0.1,
        open_grace_s=15.0,
    )


def _guard() -> dict:
    return {
        "active_set_runtime": {
            "policy_by_wallet": {
                probe.WALLET: {
                    "policy_id": "deadman_microprobe_0.10_cap_0.5_le_25",
                    "wallet_fraction": 0.1,
                    "max_order_usd": 1.0,
                    "min_order_usd": 1.0,
                    "min_price": 0.0,
                    "max_price": 0.25,
                    "max_seconds_from_open": 300,
                }
            }
        }
    }


def _event(event_id: str, *, observed_ts: float, usdc_size: float = 6.0) -> dict:
    return {
        "event_id": event_id,
        "source_fingerprint": f"fp-{event_id}",
        "source_wallet": probe.WALLET,
        "action": "BUY",
        "market_slug": "btc-updown-5m-1785347700",
        "condition_id": "condition",
        "token_id": "token-up",
        "outcome": "Up",
        "event_ts": observed_ts,
        "observed_ts": observed_ts,
        "price": 0.48,
        "size": usdc_size / 0.48,
        "usdc_size": usdc_size,
        "source": "rtds_activity",
    }


def test_probe_defers_preopen_inventory_and_scores_resolution(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write(Path(args.guard), _guard())
    _write(
        Path(args.hot_history),
        {
            "events": [
                _event("one", observed_ts=1785347690),
                _event("two", observed_ts=1785347695, usdc_size=5.0),
            ]
        },
    )
    Path(args.resolutions).write_text(
        json.dumps({"market_slug": "btc-updown-5m-1785347700", "direction": "UP"}) + "\n",
        encoding="utf-8",
    )

    state = probe.run_once(args, now_s=1785347680, client=FakeClient())
    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False
    state = probe.run_once(args, now_s=1785347701, client=FakeClient())

    assert state["summary"]["c539_buy_rows"] == 2
    assert state["summary"]["not_open_yet_rows"] == 2
    assert state["summary"]["not_open_yet_share"] == 1.0
    assert state["summary"]["survived_open_re_evaluation"] == 1
    assert state["summary"]["resolved"] == 1
    assert state["summary"]["venue_executable_max_price"] == 0.5
    assert state["summary"]["frozen_band_venue_reachable_share_pct"] == 100.0
    row = state["window_evaluations"][0]
    assert row["status"] == "PAPER_FILLED_AT_OPEN"
    assert row["target_copy_usd"] == 1.0
    assert row["avg_fill_price"] == 0.2
    assert row["post_fee_pnl_usd"] > 0
    assert state["copyintents_emitted"] == 0
    assert state["admission"]["live_authority"] is False
    assert state["admission"]["required_bars"]["venue_executable"] is True


def test_summary_excludes_venue_unreachable_paper_fill_from_f1() -> None:
    state = probe._new_state(probe._policy_from_guard(_guard()), 1785347680)
    state["window_evaluations"] = [
        {
            "evaluation_id": "executable",
            "window_start_s": 1785347700,
            "status": "PAPER_FILLED_AT_OPEN",
            "avg_fill_price": 0.49,
            "resolved": True,
            "post_fee_pnl_usd": 0.10,
        },
        {
            "evaluation_id": "paper-only",
            "window_start_s": 1785348000,
            "status": "PAPER_FILLED_AT_OPEN",
            "avg_fill_price": 0.51,
            "resolved": True,
            "post_fee_pnl_usd": -1.0,
        },
    ]

    probe._summarize(state, now_s=1785348010)

    assert state["summary"]["survived_open_re_evaluation"] == 2
    assert state["summary"]["venue_executable_open_fills"] == 1
    assert state["summary"]["venue_unreachable_open_fills"] == 1
    assert state["summary"]["frozen_band_venue_reachable_share_pct"] == 50.0
    assert state["summary"]["resolved"] == 1
    assert state["summary"]["post_fee_pnl_usd"] == 0.1
    assert state["admission"]["required_bars"]["venue_executable"] is True


def test_summary_names_projected_expiry_when_forward_n_cannot_reach_50() -> None:
    registered_s = 1785347680
    state = probe._new_state(probe._policy_from_guard(_guard()), registered_s)
    state["source_rows"] = [{"not_open_yet": True}]
    state["window_evaluations"] = [
        {
            "evaluation_id": "one",
            "window_start_s": registered_s + 300,
            "status": "PAPER_FILLED_AT_OPEN",
            "avg_fill_price": 0.49,
            "resolved": True,
            "post_fee_pnl_usd": 0.10,
        }
    ]

    probe._summarize(state, now_s=registered_s + 12 * 3600)

    projection = state["summary"]["forward_evidence_projection"]
    assert projection["status"] == "PROJECTED_EXPIRY_WITHOUT_EVIDENCE"
    assert projection["forward_n_now"] == 1
    assert projection["projected_forward_n_at_deadline"] == 2.0
    assert "below the rate required" in projection["cause"]


def test_probe_freezes_policy_and_refuses_outside_open_band(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write(Path(args.guard), _guard())
    _write(Path(args.hot_history), {"events": [_event("one", observed_ts=1785347690, usdc_size=4.0)]})
    Path(args.resolutions).write_text("", encoding="utf-8")

    initial = probe.run_once(args, now_s=1785347680, client=FakeClient())
    changed = _guard()
    changed["active_set_runtime"]["policy_by_wallet"][probe.WALLET]["max_price"] = 0.70
    _write(Path(args.guard), changed)
    state = probe.run_once(args, now_s=1785347701, client=FakeClient())

    assert state["policy_drift"] is True
    assert state["frozen_policy"]["policy_fingerprint"] == initial["frozen_policy"]["policy_fingerprint"]
    assert state["window_evaluations"][0]["status"] == "REFUSED_AT_OPEN"
    assert state["window_evaluations"][0]["reason"] == "inventory_below_frozen_min_order"


def test_probe_marks_missed_open_without_backfilled_book_claim(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write(Path(args.guard), _guard())
    _write(Path(args.hot_history), {"events": [_event("one", observed_ts=1785347690, usdc_size=12.0)]})
    Path(args.resolutions).write_text("", encoding="utf-8")

    probe.run_once(args, now_s=1785347680, client=FakeClient())
    state = probe.run_once(args, now_s=1785347800, client=FakeClient())

    assert state["window_evaluations"][0]["status"] == "OPEN_SAMPLE_MISSED"
    assert state["summary"]["survived_open_re_evaluation"] == 0


def test_probe_cross_feed_dedup_retains_earliest_economic_signal(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write(Path(args.guard), _guard())
    early = _event("rtds", observed_ts=1785347699.9, usdc_size=12.0)
    early["condition_id"] = "same-condition"
    early["source"] = "rtds_activity"
    late = _event("polygon", observed_ts=1785347708.8, usdc_size=12.0)
    late["condition_id"] = "same-condition"
    late["source"] = "polygon_orderfilled_ws_premerge"
    _write(Path(args.hot_history), {"events": [late, early]})
    Path(args.resolutions).write_text("", encoding="utf-8")

    probe.run_once(args, now_s=1785347680, client=FakeClient())
    state = probe.run_once(args, now_s=1785347701, client=FakeClient())

    assert state["summary"]["raw_rows"] == 2
    assert state["summary"]["distinct_signals"] == 1
    assert state["summary"]["cross_feed_duplicate_rows"] == 1
    assert state["summary"]["not_open_yet_rows"] == 1
    assert state["summary"]["not_open_yet_share"] == 1.0
    assert len(state["distinct_signals"]) == 1
    signal = state["distinct_signals"][0]
    assert signal["event_id"] == "rtds"
    assert signal["duplicate_source_event_ids"] == ["polygon"]
    assert state["window_evaluations"][0]["source_row_count"] == 1


def test_preregister_successor_preserves_terminal_truth_and_only_changes_ratio() -> None:
    frozen = probe._policy_from_guard(_guard())
    state = probe._new_state(frozen, 1785347000)
    successor = probe.preregister_wf050_successor(state, now_s=1785348000)

    terminal = successor["predecessor_terminal_records"][-1]
    assert terminal["status"] == "PROBE_UNREACHABLE_BY_CONSTRUCTION"
    assert terminal["terminal_cohort_evaluations"] == 40
    assert terminal["refusal_decomposition"]["verbatim"] == "20/2/18"
    assert "never admissible as evidence against c539" in terminal["evidence_interpretation"]
    assert successor["frozen_policy"]["wallet_fraction"] == 0.50
    assert successor["frozen_policy"]["max_order_usd"] == 1.0
    assert successor["frozen_policy"]["min_order_usd"] == 1.0
    assert successor["registered_policy_fingerprint"] == successor["frozen_policy"][
        "policy_fingerprint"
    ]
    assert successor["preregistration"]["changed_policy_fields"] == [
        "policy_fingerprint",
        "wallet_fraction",
    ]


def test_probe_reports_hourly_open_grace_coverage(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write(Path(args.guard), _guard())
    _write(Path(args.hot_history), {"events": [_event("one", observed_ts=1785347690)]})
    Path(args.resolutions).write_text("", encoding="utf-8")

    probe.run_once(args, now_s=1785347680, client=FakeClient())
    state = probe.run_once(args, now_s=1785347800, client=FakeClient())

    assert state["summary"]["open_grace_covered_windows"] == 0
    assert state["summary"]["open_grace_total_windows"] == 1
    assert state["summary"]["open_grace_coverage"] == 0.0
    assert state["summary"]["open_grace_instrumented_coverage"] is None
    assert state["summary"]["open_grace_coverage_below_60pct"] is True
    assert state["summary"]["open_grace_coverage_by_hour"][0]["total"] == 1
    assert state["summary"]["open_grace_coverage_rows"][0] == {
        "window_start_s": 1785347700,
        "resident_up": None,
        "first_evaluation_lag_s": None,
        "missed_detected_after_s": 100.0,
        "covered": False,
    }
    missed = state["window_evaluations"][0]
    assert missed["missed_detected_after_s"] == 100.0


def test_resident_fast_path_evaluates_cached_signal_at_open(tmp_path: Path) -> None:
    args = _args(tmp_path)
    frozen = probe._policy_from_guard(_guard())
    state = probe._new_state(frozen, 1785347680)
    captured = probe._captured_source_row(
        _event("one", observed_ts=1785347690, usdc_size=12.0),
        registered_s=1785347680,
    )
    assert captured is not None
    state["distinct_signals"] = [captured]

    assert probe._next_unevaluated_open_s(state, now_s=1785347699) == 1785347700
    probe._evaluate_pending_groups(
        state,
        args=args,
        now_s=1785347700.25,
        client=FakeClient(),
        mark_missed=False,
    )
    probe._summarize(state, now_s=1785347700.25)

    row = state["window_evaluations"][0]
    assert row["status"] == "PAPER_FILLED_AT_OPEN"
    assert row["resident_up"] is True
    assert row["first_evaluation_lag_s"] == 0.25
    assert state["summary"]["open_grace_coverage"] == 1.0
    assert state["summary"]["open_grace_instrumented_coverage"] == 1.0


def test_preregister_full_band_successor_preserves_price_terminal_proof() -> None:
    predecessor = probe._policy_from_guard(_guard())
    predecessor["wallet_fraction"] = 0.50
    predecessor_without_fp = {
        key: value for key, value in predecessor.items() if key != "policy_fingerprint"
    }
    predecessor["policy_fingerprint"] = probe._checksum(predecessor_without_fp)
    state = probe._new_state(predecessor, 1785347000)
    successor = probe.preregister_full_band_successor(state, now_s=1785348000)

    terminal = successor["predecessor_terminal_records"][-1]
    assert terminal["status"] == "PROBE_UNREACHABLE_BY_CONSTRUCTION_PRICE_BAND"
    assert terminal["refusal_decomposition"]["verbatim"] == "0/1/2"
    assert terminal["source_price_distribution"] == [0.49, 0.49, 0.49, 0.48]
    assert terminal["frozen_max_price"] == 0.25
    assert successor["frozen_policy"]["max_price"] == 1.0
    assert successor["frozen_policy"]["min_price"] == 0.0
    assert successor["frozen_policy"]["min_order_usd"] == 1.0
    assert successor["preregistration"]["changed_policy_fields"] == [
        "max_price",
        "policy_fingerprint",
        "policy_id",
    ]
    assert successor["preregistration"]["allowed_policy_fields"] == [
        "max_price",
        "policy_fingerprint",
        "policy_id",
        "wallet_fraction",
    ]


def test_successor_runtime_difference_is_not_mid_clock_policy_drift(
    tmp_path: Path,
) -> None:
    args = _args(tmp_path)
    _write(Path(args.guard), _guard())
    _write(Path(args.hot_history), {"events": []})
    Path(args.resolutions).write_text("", encoding="utf-8")
    predecessor = probe._new_state(probe._policy_from_guard(_guard()), 1785347000)
    successor = probe.preregister_wf050_successor(predecessor, now_s=1785347680)
    _write(Path(args.state), successor)

    state = probe.run_once(args, now_s=1785347681, client=FakeClient())

    assert state["policy_drift"] is False
    assert state["runtime_policy_diff_expected"] is True


def test_successor_migrates_pre_patch_registered_predecessor_fingerprint(
    tmp_path: Path,
) -> None:
    args = _args(tmp_path)
    _write(Path(args.guard), _guard())
    _write(Path(args.hot_history), {"events": []})
    Path(args.resolutions).write_text("", encoding="utf-8")
    predecessor = probe._new_state(probe._policy_from_guard(_guard()), 1785347000)
    successor = probe.preregister_wf050_successor(predecessor, now_s=1785347680)
    successor["registered_policy_fingerprint"] = (
        successor["preregistration"]["predecessor_policy_fingerprint"]
    )
    _write(Path(args.state), successor)

    state = probe.run_once(args, now_s=1785347681, client=FakeClient())

    assert state["registered_policy_fingerprint"] == state["frozen_policy"][
        "policy_fingerprint"
    ]
    assert state["policy_drift"] is False
