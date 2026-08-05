from __future__ import annotations

import argparse

from scripts import report_no_copy_signal_candidate_coverage as report


def test_candidate_trigger_uses_uncovered_no_copy_windows(monkeypatch, tmp_path) -> None:
    scorecard = {
        "window": {"start_ts": 1000, "end_ts": 2000},
        "volume_kpi": {
            "missed_window_attribution": {
                "rows": [
                    {"window_start_s": 1000, "attribution": "no_copy_signal"},
                    {"window_start_s": 1300, "attribution": "no_copy_signal"},
                    {"window_start_s": 1600, "attribution": "no_copy_signal"},
                ]
            }
        },
    }
    guard_state = {
        "active_set": {
            "members": [
                {"source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                {"source_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
            ]
        }
    }
    window_index = {
        "windows": {
            "1000": {"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": [[1, 1]]},
            "1300": {"0xcccccccccccccccccccccccccccccccccccccccc": [[2, 1]]},
            "1600": {"0xcccccccccccccccccccccccccccccccccccccccc": [[3, 1]]},
        }
    }
    live_execution = {"live_orders": []}

    def fake_load_json(path, default=None):
        name = str(path)
        if name.endswith("scorecard.json"):
            return scorecard
        if name.endswith("guard.json"):
            return guard_state
        if name.endswith("index.json"):
            return window_index
        if name.endswith("live.json"):
            return live_execution
        return default

    monkeypatch.setattr(report, "load_json", fake_load_json)
    monkeypatch.setattr(report, "load_fresh_scorecard", lambda _path: scorecard)

    args = argparse.Namespace(
        candidate_wallet="0xcccccccccccccccccccccccccccccccccccccccc",
        focus_wallet="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        scorecard=tmp_path / "scorecard.json",
        guard_state=tmp_path / "guard.json",
        window_index=tmp_path / "index.json",
        live_execution=tmp_path / "live.json",
        watcher_gap_report=tmp_path / "watcher.json",
        output=tmp_path / "out.json",
        trigger_threshold=2,
        direction_id="test",
    )

    result = report.build_report(args)

    assert result["post_admission_roster"]["watcher_trade_no_copy_signal_windows_uncovered"] == 2
    assert result["candidate"]["uncovered_no_copy_signal_windows_with_wallet_signal"] == 2
    assert result["candidate"]["trigger_met"] is True
    assert result["candidate"]["action"] == "ADMIT_HALF_SIZE_PREAUTHORIZED"


def test_focus_wallet_counts_incremental_coverage_and_live_fills(monkeypatch, tmp_path) -> None:
    scorecard = {
        "window": {"start_ts": 1000, "end_ts": 2000},
        "volume_kpi": {
            "missed_window_attribution": {
                "rows": [
                    {"window_start_s": 1000, "attribution": "no_copy_signal"},
                    {"window_start_s": 1300, "attribution": "no_copy_signal"},
                ]
            }
        },
    }
    guard_state = {
        "active_set": {
            "members": [
                {"source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                {"source_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
            ]
        }
    }
    window_index = {
        "windows": {
            "1000": {"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": [[1, 1]]},
            "1300": {"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": [[2, 1]]},
        }
    }
    live_execution = {
        "live_orders": [
            {
                "source_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "final_status": "FILLED",
            },
            {
                "source_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "final_status": "REJECTED",
            },
        ]
    }

    def fake_load_json(path, default=None):
        name = str(path)
        if name.endswith("scorecard.json"):
            return scorecard
        if name.endswith("guard.json"):
            return guard_state
        if name.endswith("index.json"):
            return window_index
        if name.endswith("live.json"):
            return live_execution
        return default

    monkeypatch.setattr(report, "load_json", fake_load_json)
    monkeypatch.setattr(report, "load_fresh_scorecard", lambda _path: scorecard)

    args = argparse.Namespace(
        candidate_wallet="0xcccccccccccccccccccccccccccccccccccccccc",
        focus_wallet="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        scorecard=tmp_path / "scorecard.json",
        guard_state=tmp_path / "guard.json",
        window_index=tmp_path / "index.json",
        live_execution=tmp_path / "live.json",
        watcher_gap_report=tmp_path / "watcher.json",
        output=tmp_path / "out.json",
        trigger_threshold=10,
        direction_id="test",
    )

    result = report.build_report(args)

    assert result["focus_wallet"]["incremental_no_copy_windows_vs_roster_without_focus"] == 1
    assert result["focus_wallet"]["live_counts"] == {
        "orders": 2,
        "fills": 1,
        "rejects": 1,
        "submitted": 0,
    }


def test_watcher_gap_report_drives_roster_uncovered_set(monkeypatch, tmp_path) -> None:
    scorecard = {
        "window": {"start_ts": 1000, "end_ts": 2000},
        "volume_kpi": {
            "missed_window_attribution": {
                "rows": [
                    {"window_start_s": 1000, "attribution": "no_copy_signal"},
                    {"window_start_s": 1300, "attribution": "no_copy_signal"},
                ]
            }
        },
    }
    guard_state = {"active_set": {"members": [{"source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}}
    window_index = {
        "windows": {
            "1000": {"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": [[1, 1]]},
        }
    }
    watcher_gap_report = {
        "summary": {
            "active_set_btc_trade_windows_total": 2,
            "active_set_no_copy_signal_windows_covered_starts": [1000, 1300],
        }
    }

    def fake_load_json(path, default=None):
        name = str(path)
        if name.endswith("scorecard.json"):
            return scorecard
        if name.endswith("guard.json"):
            return guard_state
        if name.endswith("index.json"):
            return window_index
        if name.endswith("watcher.json"):
            return watcher_gap_report
        if name.endswith("live.json"):
            return {"live_orders": []}
        return default

    monkeypatch.setattr(report, "load_json", fake_load_json)
    monkeypatch.setattr(report, "load_fresh_scorecard", lambda _path: scorecard)

    args = argparse.Namespace(
        candidate_wallet="0xcccccccccccccccccccccccccccccccccccccccc",
        focus_wallet="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        scorecard=tmp_path / "scorecard.json",
        guard_state=tmp_path / "guard.json",
        window_index=tmp_path / "index.json",
        live_execution=tmp_path / "live.json",
        watcher_gap_report=tmp_path / "watcher.json",
        output=tmp_path / "out.json",
        trigger_threshold=1,
        direction_id="test",
    )

    result = report.build_report(args)

    assert result["post_admission_roster"]["watcher_trade_coverage_source"] == "watcher_gap_report"
    assert result["post_admission_roster"]["watcher_trade_windows"] == 2
    assert result["post_admission_roster"]["watcher_trade_no_copy_signal_windows"] == 2
    assert result["post_admission_roster"]["watcher_trade_no_copy_signal_windows_uncovered"] == 0


def test_roster_copyintent_coverage_comes_from_live_execution_orders(monkeypatch, tmp_path) -> None:
    scorecard = {
        "window": {"start_ts": 1000, "end_ts": 2000},
        "volume_kpi": {
            "missed_window_attribution": {
                "rows": [
                    {"window_start_s": 1000, "attribution": "no_copy_signal"},
                    {"window_start_s": 1300, "attribution": "no_copy_signal"},
                ]
            }
        },
    }
    guard_state = {"active_set": {"members": [{"source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}}
    live_execution = {
        "orders": [
            {
                "source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "market_slug": "btc-updown-5m-1000",
                "final_status": "REJECTED",
            },
            {
                "source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "market_slug": "eth-updown-5m-1300",
                "final_status": "FILLED",
            },
        ]
    }

    def fake_load_json(path, default=None):
        name = str(path)
        if name.endswith("scorecard.json"):
            return scorecard
        if name.endswith("guard.json"):
            return guard_state
        if name.endswith("live.json"):
            return live_execution
        if name.endswith("index.json"):
            return {"windows": {}}
        if name.endswith("watcher.json"):
            return {}
        return default

    monkeypatch.setattr(report, "load_json", fake_load_json)
    monkeypatch.setattr(report, "load_fresh_scorecard", lambda _path: scorecard)

    args = argparse.Namespace(
        candidate_wallet="0xcccccccccccccccccccccccccccccccccccccccc",
        focus_wallet="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        scorecard=tmp_path / "scorecard.json",
        guard_state=tmp_path / "guard.json",
        window_index=tmp_path / "index.json",
        live_execution=tmp_path / "live.json",
        watcher_gap_report=tmp_path / "watcher.json",
        output=tmp_path / "out.json",
        trigger_threshold=1,
        direction_id="test",
    )

    result = report.build_report(args)

    assert result["post_admission_roster"]["copyintent_windows"] == 1
    assert result["post_admission_roster"]["copyintent_no_copy_signal_windows"] == 1
    assert result["post_admission_roster"]["copyintent_no_copy_signal_windows_missing"] == 1
