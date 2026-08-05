import argparse
import json
import subprocess
import sys
from pathlib import Path

from scripts import refresh_self_feed_overlay as refresh


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_overlay_refresh_reports_mismatch_without_failing(tmp_path: Path) -> None:
    benchmark = tmp_path / "data/research/wallet_copy_self_feed_duckdb_benchmark_latest.json"
    retrace = tmp_path / "data/research/wallet_copy_self_feed_full_ledger_retrace_latest.json"
    classification = tmp_path / "data/research/wallet_copy_recon_window_cash_ledger_latest.json"
    output = tmp_path / "data/research/self_feed_overlay_refresh_latest.json"
    _write_json(
        benchmark,
        {
            "status": "PASS",
            "generated_at": "2026-07-07T21:58:25Z",
            "classification_packet": {
                "resolved_pnl_overlay": {"overlay_delta_usd": -20.158263},
                "recommendation": {"mode": "RECONCILIATION_OVERLAY", "ledger_rewrite": False},
            },
        },
    )
    _write_json(
        retrace,
        {"reconciliation_equation": {"status": "MISMATCH", "account_value_residual_usd": 14.784506}},
    )
    _write_json(classification, {"summary": {"true_unrecorded_fill_candidate": 68}})

    def fake_run(cmd, cwd):
        script = cmd[-1]
        if script.endswith("benchmark_self_feed_duckdb_scan.py"):
            _write_json(
                benchmark,
                {
                    "status": "MISMATCH",
                    "generated_at": "2026-07-10T20:53:44Z",
                    "classification_packet": {
                        "resolved_pnl_overlay": {
                            "overlay_delta_usd": 4.954682,
                            "reconciled_actual_estimate_usd": 42.001532,
                        },
                        "recommendation": {"mode": "RECONCILIATION_OVERLAY", "ledger_rewrite": False},
                    },
                },
            )
            return subprocess.CompletedProcess(cmd, 1, "mismatch\n", "")
        if script.endswith("retrace_self_feed_full_ledger.py"):
            _write_json(
                retrace,
                {
                    "reconciliation_equation": {
                        "status": "MISMATCH",
                        "account_value_residual_usd": 14.784506,
                        "unexplained_usd": 47.690607,
                    }
                },
            )
        return subprocess.CompletedProcess(cmd, 0, "ok\n", "")

    report = refresh.build_report(
        argparse.Namespace(
            benchmark=str(benchmark),
            retrace=str(retrace),
            classification=str(classification),
            output=str(output),
        ),
        root=tmp_path,
        run_func=fake_run,
    )

    assert report["status"] == "REFRESHED_WITH_MISMATCH"
    assert report["before_overlay"]["overlay_delta_usd"] == -20.158263
    assert report["after_overlay"]["overlay_delta_usd"] == 4.954682
    assert report["after_residual"]["account_value_residual_usd"] == 14.784506
    assert report["commands"][-1]["returncode"] == 1
    assert report["mismatch_is_reportable"] is True
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "REFRESHED_WITH_MISMATCH"


def test_overlay_refresh_main_exits_zero_for_reportable_mismatch(tmp_path: Path, monkeypatch) -> None:
    report = {
        "status": "REFRESHED_WITH_MISMATCH",
        "before_overlay": {"overlay_delta_usd": -20.158263},
        "after_overlay": {"overlay_delta_usd": 4.954682},
        "after_residual": {"account_value_residual_usd": 14.784506},
    }
    monkeypatch.setattr(refresh, "build_report", lambda _args: report)

    assert refresh.main(["--output", str(tmp_path / "out.json")]) == 0
