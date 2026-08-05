from pathlib import Path


def test_heartbeat_refreshes_pipeline_slo_nonfatally() -> None:
    root = Path(__file__).resolve().parents[1]
    serving_script = (root / "scripts" / "codex_heartbeat.sh").read_text()
    refresh_script = (
        root / "scripts" / "codex_refresh_cadence.sh"
    ).read_text()

    assert "python3 scripts/report_pipeline_slo.py" not in serving_script
    assert "python3 scripts/report_pipeline_slo.py" in refresh_script
    assert (
        '|| echo "pipeline SLO reporter failed at '
        "$(date -u +%H:%M:%SZ)\" >>\"$LOG\""
    ) in refresh_script
    assert "cycle_period_s_observed" in refresh_script


def test_refresh_cadence_has_independent_900s_launchd_job() -> None:
    root = Path(__file__).resolve().parents[1]
    plist = (
        root / "launchd" / "com.belavarga.polymarket.codex-refresh-cadence.plist"
    ).read_text()

    assert "scripts/codex_refresh_cadence.sh" in plist
    assert "<key>StartInterval</key>" in plist
    assert "<integer>900</integer>" in plist
