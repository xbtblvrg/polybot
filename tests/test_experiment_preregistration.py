import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path("scripts/preregister_experiment.py")
ROOT = Path(__file__).resolve().parents[1]


def _run_prereg(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--registry",
            str(tmp_path / "experiment_preregistry.jsonl"),
            "--summary",
            str(tmp_path / "experiment_preregistration_latest.json"),
            *args,
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def test_preregister_experiment_writes_append_only_record_and_audit_summary(tmp_path: Path) -> None:
    result = _run_prereg(
        tmp_path,
        "--experiment-id",
        "btc5m-pair-sum-forward-20260707",
        "--flow-stage",
        "LEARN/PROMOTE",
        "--hypothesis",
        "pair-sum complement dislocations produce positive copy-priced EV",
        "--success-criterion",
        ">=30 forward fills and positive paper PnL before promotion packet",
        "--failure-criterion",
        "forward PnL <= 0 after 30 fills or fill rate below 1/hour",
        "--primary-metric",
        "forward_pnl_usd",
        "--measurement-window",
        "post-registration BTC5M resolved windows",
        "--decision-rule",
        "promote only after Fable direction if success criterion passes",
        "--deadline-utc",
        "2026-07-09T08:00:00Z",
        "--owner",
        "Codex/Fable",
        "--artifact",
        "data/research/btc5m_pair_sum_paper_lane_state.json",
    )

    assert result.returncode == 0, result.stderr + result.stdout
    records = (tmp_path / "experiment_preregistry.jsonl").read_text().strip().splitlines()
    assert len(records) == 1
    record = json.loads(records[0])
    assert record["experiment_id"] == "btc5m-pair-sum-forward-20260707"
    assert record["success_criterion"].startswith(">=30 forward fills")
    summary = json.loads((tmp_path / "experiment_preregistration_latest.json").read_text())
    assert summary["status"] == "PASS"
    assert summary["valid_records"] == 1
    assert summary["latest_experiment_id"] == "btc5m-pair-sum-forward-20260707"

    audit = _run_prereg(
        tmp_path,
        "--audit-only",
        "--require-experiment-id",
        "btc5m-pair-sum-forward-20260707",
    )
    assert audit.returncode == 0, audit.stderr + audit.stdout


def test_preregister_experiment_rejects_incomplete_duplicate_and_missing_required(tmp_path: Path) -> None:
    invalid = _run_prereg(
        tmp_path,
        "--experiment-id",
        "bad",
        "--flow-stage",
        "LEARN",
    )

    assert invalid.returncode == 2
    invalid_summary = json.loads((tmp_path / "experiment_preregistration_latest.json").read_text())
    assert "missing_success_criterion" in invalid_summary["errors"]
    assert "missing_result_artifacts" in invalid_summary["errors"]

    base_args = [
        "--experiment-id",
        "btc5m-valid-20260707",
        "--flow-stage",
        "LEARN",
        "--hypothesis",
        "unit hypothesis",
        "--success-criterion",
        "unit success",
        "--failure-criterion",
        "unit failure",
        "--primary-metric",
        "unit_metric",
        "--measurement-window",
        "unit window",
        "--decision-rule",
        "unit decision",
        "--deadline-utc",
        "2026-07-09T08:00:00Z",
        "--owner",
        "Codex/Fable",
        "--artifact",
        "data/research/unit.json",
    ]
    assert _run_prereg(tmp_path, *base_args).returncode == 0
    duplicate = _run_prereg(tmp_path, *base_args)
    assert duplicate.returncode == 2
    duplicate_summary = json.loads((tmp_path / "experiment_preregistration_latest.json").read_text())
    assert "duplicate_experiment_id_requires_amend" in duplicate_summary["errors"]

    missing = _run_prereg(tmp_path, "--audit-only", "--require-experiment-id", "missing-exp")
    assert missing.returncode == 2
    missing_summary = json.loads((tmp_path / "experiment_preregistration_latest.json").read_text())
    assert missing_summary["status"] == "MISSING_REQUIRED"
    assert missing_summary["missing_required_ids"] == ["missing-exp"]


def test_preregister_experiment_tombstones_append_only_and_removes_active_id(tmp_path: Path) -> None:
    base_args = [
        "--experiment-id",
        "eth5m-replication-scout-paper-20260720",
        "--flow-stage",
        "DISCOVER/OBSERVE",
        "--hypothesis",
        "unit hypothesis",
        "--success-criterion",
        "unit success",
        "--failure-criterion",
        "unit failure",
        "--primary-metric",
        "unit_metric",
        "--measurement-window",
        "unit window",
        "--decision-rule",
        "unit decision",
        "--deadline-utc",
        "2026-07-23T00:25:00Z",
        "--owner",
        "Codex/Fable",
        "--artifact",
        "data/research/eth5m_replication_scout_paper_state.json",
    ]
    assert _run_prereg(tmp_path, *base_args).returncode == 0

    retired = _run_prereg(
        tmp_path,
        "--tombstone-experiment-id",
        "eth5m-replication-scout-paper-20260720",
        "--notes",
        "TOMBSTONED_DECISIVE_NEGATIVE; reopen_allowed=false",
    )

    assert retired.returncode == 0, retired.stderr + retired.stdout
    rows = [json.loads(line) for line in (tmp_path / "experiment_preregistry.jsonl").read_text().splitlines()]
    assert [row["status"] for row in rows] == ["PREREGISTERED", "TOMBSTONED"]
    summary = json.loads((tmp_path / "experiment_preregistration_latest.json").read_text())
    assert summary["active_count"] == 0
    assert "eth5m-replication-scout-paper-20260720" not in summary["active_ids"]


def test_weekend_neg8_probe_is_preregistered_and_visible_in_digest() -> None:
    experiment_id = "weekend-day-probe-neg8-20260721"
    registry = [
        json.loads(line)
        for line in (ROOT / "data/research/experiment_preregistry.jsonl").read_text().splitlines()
        if line.strip()
    ]
    record = next(row for row in reversed(registry) if row.get("experiment_id") == experiment_id)
    summary = json.loads((ROOT / "data/research/experiment_preregistration_latest.json").read_text())
    digest = json.loads((ROOT / "data/research/state_digest.json").read_text())

    assert record["status"] == "PREREGISTERED"
    assert "canonical day PnL <= -8.0 on a weekend UTC day" in record["decision_rule"]
    assert "PROBE_CAPS_REST_OF_UTC_DAY" in record["decision_rule"]
    assert "Arm 2026-07-25T00:00Z" in record["measurement_window"]
    assert "disarm at weekend close" in record["measurement_window"]
    assert experiment_id in summary["active_ids"]
    assert summary["latest_experiment_id"] is not None
    assert digest["experiment_preregistration"]["latest_experiment_id"] is not None

