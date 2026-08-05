import json
import os
import subprocess
import sys
from pathlib import Path


def test_brainless_ops_pins_wallet_outflow_to_blockscout() -> None:
    script = (Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh").read_text()

    assert "scripts/wallet_outflow_deadman.py \\" in script
    assert "--transfer-source blockscout" in script
    assert "--timeout-s 30" in script


def test_brainless_ops_wires_continuous_market_mining_cadence() -> None:
    script = (Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh").read_text()

    assert "scripts/run_wallet_market_mining_cadence.py" in script
    assert "scripts/build_factory_funnel.py" in script
    assert "market_mining_cadence" in script
    assert "factory_funnel" in script
    assert "continuous intake/replay/liveness/packet mining cadence" in script
    assert "ordered causal ladder from market population to profitable sustained output" in script


def test_brainless_ops_wires_f418_post_band_causal_shadow() -> None:
    script = (Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh").read_text()

    assert "run_step f418_post_band_gate_residual_loss_causal" in script
    assert "scripts/report_f418_post_band_gate_residual_loss_causal.py" in script


def test_brainless_ops_wires_research_disk_deadman() -> None:
    script = (Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh").read_text()

    assert "scripts/research_disk_deadman.py enforce" in script
    assert "scripts/research_disk_deadman.py audit --write-handoff-on-incident" in script
    assert "research_disk_deadman" in script
    assert "free<150GiB" in script
    assert "absent from rotation inventory" in script


def test_brainless_ops_queues_all_handoff_writes_for_atomic_commit() -> None:
    script = (Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh").read_text()

    assert 'order_flow_deadman.py --handoff "$handoff_pending"' in script
    assert 'codex_starvation_deadman.py --handoff "$handoff_pending"' in script
    assert 'record_cli_versions.py --handoff "$handoff_pending" record' in script
    assert 'report_own_positions.py --handoff "$handoff_pending"' in script
    assert 'append_handoff_and_commit.py' in script
    assert '} >> "$HANDOFF"' not in script


def test_brainless_ops_refreshes_scorecard_immediately_before_basis_reconciliation() -> None:
    script = (Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh").read_text()

    refresh = script.index("run_step scorecard_basis_refresh")
    reconcile = script.index("run_step state_digest", refresh)
    assert refresh < reconcile
    assert "cp data/research/brainless_ops_scorecard_basis_refresh.out" in script[
        refresh:reconcile
    ]
    assert "run_step " not in script[
        refresh + len("run_step scorecard_basis_refresh") : reconcile
    ]


def test_brainless_ops_refreshes_e1_ledger_cut_before_digest() -> None:
    script = (Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh").read_text()

    e1 = script.index("run_step e1_framework_audit_inputs")
    scorecard = script.index("run_step scorecard_basis_refresh", e1)
    digest = script.index("run_step state_digest", scorecard)
    assert e1 < scorecard < digest
    assert "--reject-since \"${e1_refresh_day}T00:00:00Z\"" in script[e1:scorecard]
    bank = script.index("run_step fee_realization_bank_reconciliation", digest)
    bank_digest = script.index("run_step fee_bank_digest_refresh", bank)
    assert digest < bank < bank_digest


def test_brainless_ops_synthetic_outage_rotation(tmp_path: Path) -> None:
    root = tmp_path
    script = Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh"
    env = os.environ.copy()
    env.update(
        {
            "POLYMARKET_AGENT_ROOT": str(root),
            "POLYMARKET_AGENT_PYTHON": sys.executable,
            "BRAINLESS_SYNTHETIC_ROTATION_TEST": "1",
            "BRAINLESS_FORCE_BRAIN_OUTAGE": "1",
            "PATH": f"{tmp_path}:{env.get('PATH', '')}",
        }
    )
    for name in ("claude", "grok", "agy", "codex"):
        stub = tmp_path / name
        stub.write_text("#!/usr/bin/env sh\nexit 1\n")
        stub.chmod(0o755)

    proc = subprocess.run(["bash", str(script)], env=env, text=True, capture_output=True, check=True)
    payload = json.loads(proc.stdout)

    assert payload["brain_outage"] is True
    assert payload["rotation_action"] == "RETAIN_NO_ELIGIBLE_CANDIDATE"
    assert payload["queue_depth"] == 1
    assert payload["queue_ready_for_live"] == 0
    assert payload["live_path_mutated"] is False
    assert payload["failed_steps"] == []
    assert payload["memory_pressure"]["action"] in {
        "ALLOW_BACKGROUND_REFRESH",
        "UNKNOWN_ALLOW_BACKGROUND_REFRESH",
        "PAUSE_LOW_PRIORITY_BACKGROUND_JOBS",
    }
    assert "no_ai_calls" in payload["forbidden_actions_enforced"]
    handoff = root / "docs" / "agents" / "HANDOFF.md"
    assert "brainless STATUS" in handoff.read_text()


def test_brainless_ops_pauses_low_priority_jobs_under_memory_pressure(tmp_path: Path) -> None:
    root = tmp_path
    script = Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh"
    scripts = root / "scripts"
    scripts.mkdir()
    (scripts / "daily_scorecard.py").write_text(
        "print('day_utc=2099-01-01')\nprint('total orders=0 fills=0 resolved=0 rejects=0 pnl=+0.000000')\n"
    )
    (scripts / "update_state_digest.py").write_text(
        "from pathlib import Path\n"
        "Path('data/research').mkdir(parents=True, exist_ok=True)\n"
        "Path('data/research/state_digest.md').write_text('# digest\\n')\n"
        "Path('data/research/state_digest.json').write_text('{}\\n')\n"
        "print('{\"ok\": true}')\n"
    )
    fake_memory_pressure = tmp_path / "memory_pressure"
    fake_memory_pressure.write_text(
        "#!/usr/bin/env sh\n"
        "echo 'The system has 51539607552 (3145728 pages with a page size of 16384).'\n"
        "echo 'System-wide memory free percentage: 20%'\n"
    )
    fake_memory_pressure.chmod(0o755)
    env = os.environ.copy()
    env.update(
        {
            "POLYMARKET_AGENT_ROOT": str(root),
            "POLYMARKET_AGENT_PYTHON": sys.executable,
            "BRAINLESS_MEMORY_PRESSURE_MAX_PCT": "70",
            "PATH": f"{tmp_path}:{env.get('PATH', '')}",
        }
    )

    proc = subprocess.run(["bash", str(script)], env=env, text=True, capture_output=True, check=True)
    payload = json.loads(proc.stdout)

    assert payload["status"] == "DEGRADED"
    assert payload["memory_pressure"]["free_pct"] == 20
    assert payload["memory_pressure"]["pressure_pct"] == 80
    assert payload["memory_pressure"]["action"] == "PAUSE_LOW_PRIORITY_BACKGROUND_JOBS"
    assert (root / "data/research/brainless_ops_scorecard.out").exists()
    assert (root / "data/research/brainless_ops_state_digest.out").exists()
    assert (root / "data/research/brainless_ops_memory_pressure_pause.out").exists()
    assert not (root / "data/research/brainless_ops_member_queue.out").exists()


def test_brainless_ops_escalates_commitment_overdue_from_digest(tmp_path: Path) -> None:
    root = tmp_path
    script = Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh"
    scripts = root / "scripts"
    scripts.mkdir()
    (scripts / "daily_scorecard.py").write_text(
        "print('day_utc=2099-01-01')\nprint('total orders=0 fills=0 resolved=0 rejects=0 pnl=+0.000000')\n"
    )
    (scripts / "update_state_digest.py").write_text(
        "import json\n"
        "from pathlib import Path\n"
        "data = Path('data/research')\n"
        "data.mkdir(parents=True, exist_ok=True)\n"
        "payload = {\n"
        "    'commitments_overdue': {\n"
        "        'path': 'data/research/commitments.jsonl',\n"
        "        'overdue': 1,\n"
        "        'oldest_id': 'synthetic_overdue',\n"
        "        'oldest_due_ts': '2000-01-01T00:00:00Z',\n"
        "        'late': 0,\n"
        "        'due_today': 0,\n"
        "        'active': 1,\n"
        "        'evidence_unmarked': 1,\n"
        "        'overdue_with_evidence': 1,\n"
        "    }\n"
        "}\n"
        "data.joinpath('state_digest.json').write_text(json.dumps(payload))\n"
        "data.joinpath('state_digest.md').write_text('commitments_overdue={count:1,oldest_id:synthetic_overdue}\\n')\n"
    )
    fake_memory_pressure = tmp_path / "memory_pressure"
    fake_memory_pressure.write_text(
        "#!/usr/bin/env sh\n"
        "echo 'The system has 51539607552 (3145728 pages with a page size of 16384).'\n"
        "echo 'System-wide memory free percentage: 20%'\n"
    )
    fake_memory_pressure.chmod(0o755)
    env = os.environ.copy()
    env.update(
        {
            "POLYMARKET_AGENT_ROOT": str(root),
            "POLYMARKET_AGENT_PYTHON": sys.executable,
            "BRAINLESS_MEMORY_PRESSURE_MAX_PCT": "70",
            "PATH": f"{tmp_path}:{env.get('PATH', '')}",
        }
    )

    proc = subprocess.run(["bash", str(script)], env=env, text=True, capture_output=True, check=True)
    payload = json.loads(proc.stdout)

    commitments = payload["commitments_overdue"]
    assert payload["status"] == "DEGRADED"
    assert commitments["status"] == "OVERDUE_NOTIFY"
    assert commitments["overdue"] == 1
    assert commitments["oldest_id"] == "synthetic_overdue"
    assert commitments["overdue_with_evidence"] == 1
    assert commitments["operator_notify"] is True
    handoff = root / "docs" / "agents" / "HANDOFF.md"
    assert "brainless NOTIFY - COMMITMENT_DEADMAN" in handoff.read_text()


def test_brainless_ops_records_failed_run_step_names(tmp_path: Path) -> None:
    root = tmp_path
    script = Path(__file__).resolve().parents[1] / "scripts" / "brainless_ops.sh"
    scripts = root / "scripts"
    scripts.mkdir()
    (scripts / "daily_scorecard.py").write_text("raise SystemExit(7)\n")
    (scripts / "update_state_digest.py").write_text(
        "from pathlib import Path\n"
        "Path('data/research').mkdir(parents=True, exist_ok=True)\n"
        "Path('data/research/state_digest.md').write_text('# digest\\n')\n"
        "Path('data/research/state_digest.json').write_text('{}\\n')\n"
    )
    fake_memory_pressure = tmp_path / "memory_pressure"
    fake_memory_pressure.write_text(
        "#!/usr/bin/env sh\n"
        "echo 'The system has 51539607552 (3145728 pages with a page size of 16384).'\n"
        "echo 'System-wide memory free percentage: 20%'\n"
    )
    fake_memory_pressure.chmod(0o755)
    env = os.environ.copy()
    env.update(
        {
            "POLYMARKET_AGENT_ROOT": str(root),
            "POLYMARKET_AGENT_PYTHON": sys.executable,
            "BRAINLESS_MEMORY_PRESSURE_MAX_PCT": "70",
            "PATH": f"{tmp_path}:{env.get('PATH', '')}",
        }
    )

    proc = subprocess.run(["bash", str(script)], env=env, text=True, capture_output=True, check=True)
    payload = json.loads(proc.stdout)

    assert payload["status"] == "DEGRADED"
    assert "scorecard" in payload["failed_steps"]
    assert payload["step_durations"]["scorecard"] >= 0.0
    assert payload["step_results"]["scorecard"]["rc"] == 7
    assert payload["slowest_step"]["name"] in payload["step_durations"]


def test_record_cli_versions_marks_and_clears_update_smoke(tmp_path: Path) -> None:
    root = tmp_path
    script = Path(__file__).resolve().parents[1] / "scripts" / "record_cli_versions.py"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    def write_tool(name: str, version: str) -> None:
        tool = bin_dir / name
        tool.write_text(f"#!/usr/bin/env sh\nif [ \"$1\" = \"--version\" ]; then echo '{version}'; exit 0; fi\nexit 0\n")
        tool.chmod(0o755)

    write_tool("claude", "claude 1.0.0")
    write_tool("codex", "codex 1.0.0")
    write_tool("agy", "1.1.1")
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    env["CLI_VERSION_SKIP_NETWORK"] = "1"

    subprocess.run([sys.executable, str(script), "record"], cwd=root, env=env, check=True)
    state_path = root / "data/research/cli_versions_state.json"
    state = json.loads(state_path.read_text())
    assert state["status"] == "OK"
    assert state["pending_smoke_test"] is None

    no_tool_env = os.environ.copy()
    no_tool_env["PATH"] = "/usr/bin:/bin"
    no_tool_env["CLI_VERSION_SKIP_NETWORK"] = "1"
    subprocess.run([sys.executable, str(script), "record"], cwd=root, env=no_tool_env, check=True)
    state = json.loads(state_path.read_text())
    assert state["status"] == "OK"
    assert state["versions"]["codex"]["path"] == str(bin_dir / "codex")
    assert state["pending_smoke_test"] is None

    write_tool("codex", "codex 1.1.0")
    subprocess.run([sys.executable, str(script), "record"], cwd=root, env=env, check=True)
    state = json.loads(state_path.read_text())
    assert state["status"] == "VERSION_CHANGE_PENDING_SMOKE"
    assert state["pending_smoke_test"]["changed_tools"] == ["codex"]
    assert "cli_version_change" in (root / "docs/agents/HANDOFF.md").read_text()

    subprocess.run(
        [
            sys.executable,
            str(script),
            "smoke",
            "--provider",
            "claude",
            "--rc",
            "0",
            "--output-sanity",
            "PASS",
        ],
        cwd=root,
        env=env,
        check=True,
    )
    state = json.loads(state_path.read_text())
    assert state["status"] == "OK"
    assert state["pending_smoke_test"] is None
    assert state["last_smoke_test"]["status"] == "PASS"


def test_record_cli_versions_records_latest_stale_flags(tmp_path: Path) -> None:
    root = tmp_path
    script = Path(__file__).resolve().parents[1] / "scripts" / "record_cli_versions.py"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    for name, version in {
        "claude": "claude 1.0.0",
        "codex": "codex-cli 1.1.0",
        "grok": "grok 0.2.0",
        "agy": "1.1.1",
    }.items():
        tool = bin_dir / name
        tool.write_text(f"#!/usr/bin/env sh\nif [ \"$1\" = \"--version\" ]; then echo '{version}'; exit 0; fi\nexit 0\n")
        tool.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "CLI_VERSION_LATEST_CLAUDE": "1.2.0",
            "CLI_VERSION_LATEST_CODEX": "1.1.0",
            "CLI_VERSION_LATEST_GROK": "0.2.0",
        }
    )

    subprocess.run([sys.executable, str(script), "record"], cwd=root, env=env, check=True)
    state = json.loads((root / "data/research/cli_versions_state.json").read_text())

    assert state["status"] == "STALE_UPDATE_DUE"
    assert state["stale_tools"] == ["claude"]
    assert state["freshness"]["claude"]["stale"] is True
    assert state["freshness"]["codex"]["stale"] is False
    assert state["freshness"]["grok"]["stale"] is False
    assert state["freshness"]["agy"]["latest"]["source"] == "manual_env_sweep"
    assert state["freshness"]["agy"]["stale"] is False
    assert state["freshness"]["claude"]["latest"]["source"] == "env"


def test_record_cli_versions_uses_grok_update_check_as_latest_source(tmp_path: Path) -> None:
    root = tmp_path
    script = Path(__file__).resolve().parents[1] / "scripts" / "record_cli_versions.py"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    for name, version in {
        "claude": "claude 1.0.0",
        "codex": "codex-cli 1.0.0",
        "agy": "agy 1.1.1",
    }.items():
        tool = bin_dir / name
        tool.write_text(f"#!/usr/bin/env sh\nif [ \"$1\" = \"--version\" ]; then echo '{version}'; exit 0; fi\nexit 0\n")
        tool.chmod(0o755)

    grok = bin_dir / "grok"
    grok.write_text(
        "#!/usr/bin/env sh\n"
        "if [ \"$1\" = \"--version\" ]; then echo 'grok 0.2.82 (abc123)'; exit 0; fi\n"
        "if [ \"$1\" = \"update\" ] && [ \"$2\" = \"--check\" ] && [ \"$3\" = \"--json\" ]; then\n"
        "  echo '{\"currentVersion\":\"0.2.82\",\"latestVersion\":\"0.2.93\",\"updateAvailable\":true,\"installer\":\"internal\",\"channel\":\"stable\",\"error\":null}'\n"
        "  exit 0\n"
        "fi\n"
        "exit 1\n"
    )
    grok.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "CLI_VERSION_LATEST_CLAUDE": "1.0.0",
            "CLI_VERSION_LATEST_CODEX": "1.0.0",
            "CLI_VERSION_LATEST_AGY": "1.1.1",
        }
    )

    subprocess.run([sys.executable, str(script), "record"], cwd=root, env=env, check=True)
    state = json.loads((root / "data/research/cli_versions_state.json").read_text())

    grok_freshness = state["freshness"]["grok"]
    assert state["status"] == "STALE_UPDATE_DUE"
    assert state["stale_tools"] == ["grok"]
    assert state["unknown_latest_tools"] == []
    assert grok_freshness["stale"] is True
    assert grok_freshness["latest"]["source"] == "grok_update_check"
    assert grok_freshness["latest"]["latest_version"] == "0.2.93"
    assert grok_freshness["latest"]["update_available"] is True


def test_record_cli_versions_ignores_version_string_decoration_changes(tmp_path: Path) -> None:
    root = tmp_path
    script = Path(__file__).resolve().parents[1] / "scripts" / "record_cli_versions.py"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    grok = bin_dir / "grok"
    grok.write_text(
        "#!/usr/bin/env sh\n"
        "if [ \"$1\" = \"--version\" ]; then echo 'grok 0.2.93 (f00f96316d4b)'; exit 0; fi\n"
        "if [ \"$1\" = \"update\" ] && [ \"$2\" = \"--check\" ] && [ \"$3\" = \"--json\" ]; then\n"
        "  echo '{\"currentVersion\":\"0.2.93\",\"latestVersion\":\"0.2.99\",\"updateAvailable\":true,\"installer\":\"internal\",\"channel\":\"stable\",\"error\":null}'\n"
        "  exit 0\n"
        "fi\n"
        "exit 1\n"
    )
    grok.chmod(0o755)
    for name, version in {
        "claude": "claude 1.0.0",
        "codex": "codex-cli 1.0.0",
        "agy": "1.1.1",
    }.items():
        tool = bin_dir / name
        tool.write_text(f"#!/usr/bin/env sh\nif [ \"$1\" = \"--version\" ]; then echo '{version}'; exit 0; fi\nexit 0\n")
        tool.chmod(0o755)

    (root / "data" / "research").mkdir(parents=True)
    (root / "data" / "research" / "cli_versions_state.json").write_text(
        json.dumps(
            {
                "versions": {
                    "grok": {
                        "available": True,
                        "path": str(grok),
                        "version": "grok 0.2.93 (f00f96316d4b) [stable]",
                    }
                }
            }
        )
    )
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "CLI_VERSION_LATEST_CLAUDE": "1.0.0",
            "CLI_VERSION_LATEST_CODEX": "1.0.0",
            "CLI_VERSION_LATEST_AGY": "1.1.1",
        }
    )

    subprocess.run([sys.executable, str(script), "record"], cwd=root, env=env, check=True)
    state = json.loads((root / "data/research/cli_versions_state.json").read_text())

    assert state["changed_tools"] == []
    assert state["pending_smoke_test"] is None


def test_record_cli_versions_reports_grok_update_check_failure(tmp_path: Path) -> None:
    root = tmp_path
    script = Path(__file__).resolve().parents[1] / "scripts" / "record_cli_versions.py"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    for name, version in {
        "claude": "claude 1.0.0",
        "codex": "codex-cli 1.0.0",
    }.items():
        tool = bin_dir / name
        tool.write_text(f"#!/usr/bin/env sh\nif [ \"$1\" = \"--version\" ]; then echo '{version}'; exit 0; fi\nexit 0\n")
        tool.chmod(0o755)

    grok = bin_dir / "grok"
    grok.write_text(
        "#!/usr/bin/env sh\n"
        "if [ \"$1\" = \"--version\" ]; then echo 'grok 0.2.82 (abc123)'; exit 0; fi\n"
        "if [ \"$1\" = \"update\" ] && [ \"$2\" = \"--check\" ] && [ \"$3\" = \"--json\" ]; then\n"
        "  echo '{\"currentVersion\":\"0.2.82\",\"latestVersion\":null,\"updateAvailable\":false,\"error\":\"offline\"}'\n"
        "  exit 0\n"
        "fi\n"
        "exit 1\n"
    )
    grok.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "CLI_VERSION_LATEST_CLAUDE": "1.0.0",
            "CLI_VERSION_LATEST_CODEX": "1.0.0",
            "CLI_VERSION_LATEST_AGY": "1.1.1",
        }
    )

    subprocess.run([sys.executable, str(script), "record"], cwd=root, env=env, check=True)
    state = json.loads((root / "data/research/cli_versions_state.json").read_text())

    grok_freshness = state["freshness"]["grok"]
    assert state["status"] == "OK"
    assert state["stale_tools"] == []
    assert state["unknown_latest_tools"] == ["grok"]
    assert grok_freshness["stale"] is None
    assert grok_freshness["latest"]["source"] == "grok_update_check"
    assert grok_freshness["latest"]["status"] == "LATEST_CHECK_FAILED"
