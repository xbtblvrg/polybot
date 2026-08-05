import datetime as dt
import json
from pathlib import Path

from scripts import brainless_live_guard_restart as restart


def test_generation_files_track_live_submitter_modules():
    assert restart.ROOT / "scripts/run_wallet_copy_live_execution.py" in restart.GENERATION_FILES
    assert restart.ROOT / "src/trade_executor.py" in restart.GENERATION_FILES
    assert restart.ROOT / "src/wallet_copy/execution.py" in restart.GENERATION_FILES
    assert restart.ROOT / "src/wallet_copy/pnl_truth.py" in restart.GENERATION_FILES
    assert (
        restart.ROOT / "launchd/com.belavarga.polymarket.wallet-copy-live-guard.plist"
        in restart.GENERATION_FILES
    )
    assert all(path.exists() for path in restart.GENERATION_FILES)
    assert (
        restart.ROOT / "data/research/wallet_copy_config_generation.json"
        not in restart.GENERATION_FILES
    )


def test_canonical_start_and_launchd_plist_share_32mib_rtds_tail() -> None:
    start = (restart.ROOT / "scripts/start_live_guard.sh").read_text(encoding="utf-8")
    plist = (
        restart.ROOT / "launchd/com.belavarga.polymarket.wallet-copy-live-guard.plist"
    ).read_text(encoding="utf-8")

    assert "--rtds-tail-bytes 33554432" in start
    assert "--rtds-cold-tail-bytes 33554432" in start
    assert plist.count("<string>33554432</string>") == 2
    assert "<string>5242880</string>" not in plist


def test_detector_and_guard_generation_tuples_are_identical():
    """A one-sided tuple edit makes generation_mismatch permanently true."""
    from scripts import run_wallet_copy_live_guard as guard

    detector = [str(path.resolve()) for path in restart.GENERATION_FILES]
    recorded = [str(path.resolve()) for path in guard.LIVE_GUARD_GENERATION_FILES]
    assert detector == recorded


def test_loaded_generation_exposes_disk_symmetric_sha256_alias(monkeypatch, tmp_path):
    generation_file = tmp_path / "live_submitter.py"
    generation_file.write_text("resident-generation", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (generation_file,))
    generation_sha256 = restart.disk_generation()["sha256"]

    decision = restart.build_decision(
        deadman={"status": "OK", "idle_s": 0},
        guard_state={
            "generated_at": "2026-07-13T15:09:30Z",
            "guard_code_identity": {
                "live_guard_generation_sha256": generation_sha256,
            },
        },
        restart_state={},
        now=dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
    )

    assert decision["loaded_generation"]["sha256"] == decision["disk_generation"]["sha256"]
    assert (
        decision["loaded_generation"]["sha256"]
        == decision["loaded_generation"]["generation_sha256"]
    )


def test_live_submitter_module_change_flips_generation_mismatch(monkeypatch, tmp_path):
    live_execution = tmp_path / "scripts" / "run_wallet_copy_live_execution.py"
    pnl_truth = tmp_path / "src" / "wallet_copy" / "pnl_truth.py"
    live_execution.parent.mkdir(parents=True)
    pnl_truth.parent.mkdir(parents=True)
    monkeypatch.setattr(
        restart,
        "GENERATION_FILES",
        (live_execution, pnl_truth),
    )
    now = dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc)

    for changed_path in (live_execution, pnl_truth):
        live_execution.write_text("live-execution-v1", encoding="utf-8")
        pnl_truth.write_text("pnl-truth-v1", encoding="utf-8")
        loaded_generation = restart.disk_generation()["sha256"]
        changed_path.write_text("changed-live-submit-path", encoding="utf-8")

        decision = restart.build_decision(
            deadman={"status": "OK", "idle_s": 0},
            guard_state={
                "generated_at": "2026-07-13T15:09:30Z",
                "guard_code_identity": {
                    "live_guard_generation_sha256": loaded_generation,
                },
            },
            restart_state={},
            now=now,
        )

        assert decision["generation_mismatch"] is True


def test_generation_verdict_fails_closed_when_cached_or_unreadable():
    now = dt.datetime(2026, 8, 4, 12, 0, tzinfo=dt.timezone.utc)
    fresh = {
        "generated_at": "2026-08-04T11:59:59Z",
        "loaded_generation": {"started_at_utc": "2026-08-04T11:59:00Z"},
    }
    assert restart.generation_verdict(fresh, now=now)["stale"] is False

    old = {
        "generated_at": "2026-08-04T11:49:59Z",
        "loaded_generation": {"started_at_utc": "2026-08-04T11:40:00Z"},
    }
    verdict = restart.generation_verdict(old, now=now)
    assert verdict["stale"] is True
    assert verdict["status"] == "GENERATION_VERDICT_STALE"
    assert "verdict_older_than_declared_cadence" in verdict["reasons"]

    predates = {
        "generated_at": "2026-08-04T11:59:00Z",
        "loaded_generation": {"started_at_utc": "2026-08-04T11:59:30Z"},
    }
    assert restart.generation_verdict(predates, now=now)["stale"] is True
    assert restart.generation_verdict({}, now=now)["stale"] is True


def test_red_generation_mismatch_requires_restart(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    start = tmp_path / "start_live_guard.sh"
    mission = tmp_path / "mission.py"
    config = tmp_path / "wallet_copy_config_generation.json"
    for path, text in (
        (script, "guard-v2"),
        (start, "start"),
        (mission, "mission"),
        (config, "{}"),
    ):
        path.write_text(text, encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script, start, mission, config))
    disk_sha = restart.disk_generation()["sha256"]

    decision = restart.build_decision(
        deadman={"status": "INCIDENT_ORDER_FLOW_DEAD", "idle_s": 1900},
        guard_state={
            "generated_at": "2026-07-13T15:00:00Z",
            "guard_code_identity": {"script_sha256": "old"},
        },
        restart_state={
            "mismatch_generation_sha256": disk_sha,
            "mismatch_generation_unchanged_since": "2026-07-13T14:55:00Z",
        },
        now=dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
    )

    assert decision["status"] == "RESTART_REQUIRED"
    assert decision["reason"] == "red_order_flow_generation_mismatch"
    assert decision["restart_allowed"] is True
    assert decision["generation_mismatch"] is True


def test_red_selected_candidate_pending_adoption_requires_managed_restart(
    monkeypatch, tmp_path
):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    generation_sha = restart.disk_generation()["sha256"]

    decision = restart.build_decision(
        deadman={
            "status": "INCIDENT_ORDER_FLOW_DEAD",
            "idle_s": 9_000,
            "mechanical_escalation": "MANAGED_RESTART_SELECTION_PENDING_ADOPTION",
        },
        guard_state={
            "generated_at": "2026-08-02T12:47:30Z",
            "guard_code_identity": {
                "live_guard_generation_sha256": generation_sha,
            },
        },
        restart_state={},
        now=dt.datetime(2026, 8, 2, 12, 48, tzinfo=dt.timezone.utc),
    )

    assert decision["generation_mismatch"] is False
    assert decision["selection_adoption_due"] is True
    assert decision["restart_required"] is True
    assert decision["restart_allowed"] is True
    assert decision["reason"] == "selection_pending_adoption"


def test_measured_quiet_selected_candidate_pending_adoption_requires_managed_restart(
    monkeypatch, tmp_path
):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    generation_sha = restart.disk_generation()["sha256"]

    decision = restart.build_decision(
        deadman={
            "status": "MEASURED_SOURCE_QUIET",
            "idle_s": 9_000,
            "mechanical_escalation": "MANAGED_RESTART_SELECTION_PENDING_ADOPTION",
        },
        guard_state={
            "generated_at": "2026-08-03T01:56:30Z",
            "guard_code_identity": {
                "live_guard_generation_sha256": generation_sha,
            },
        },
        restart_state={},
        now=dt.datetime(2026, 8, 3, 1, 57, tzinfo=dt.timezone.utc),
    )

    assert decision["deadman_restart_authority"] is False
    assert decision["selection_adoption_due"] is True
    assert decision["restart_required"] is True
    assert decision["restart_allowed"] is True
    assert decision["reason"] == "selection_pending_adoption"


def test_no_admissible_target_diagnostic_strips_deadman_restart_authority(
    monkeypatch, tmp_path
):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    generation_sha = restart.disk_generation()["sha256"]

    decision = restart.build_decision(
        deadman={
            "status": "INCIDENT_ORDER_FLOW_DEAD",
            "idle_s": 9_000,
            "mechanical_escalation": "MANAGED_RESTART_SELECTION_PENDING_ADOPTION",
            "episode_fire_wallet_policy_diagnostic": (
                "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
            ),
        },
        guard_state={
            "generated_at": "2026-08-03T00:20:00Z",
            "guard_code_identity": {
                "live_guard_generation_sha256": generation_sha,
            },
        },
        restart_state={},
        now=dt.datetime(2026, 8, 3, 0, 20, tzinfo=dt.timezone.utc),
    )

    assert decision["deadman_restart_authority"] is False
    assert decision["selection_adoption_due"] is False
    assert decision["restart_required"] is False
    assert decision["reason"] == "no_restart_condition"


def test_raw_order_flow_limb_preserves_restart_authority_under_zero_supply_headline(
    monkeypatch, tmp_path
):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v2", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    disk_sha = restart.disk_generation()["sha256"]
    now = dt.datetime(2026, 8, 5, 0, 10, tzinfo=dt.timezone.utc)

    decision = restart.build_decision(
        deadman={
            "status": "INCIDENT_ZERO_SUPPLY_SEAT",
            "accepted_order_idle_s": 9_000,
            "raw_accepted_order_deadman": {
                "firing": True,
                "global_firing": False,
                "deadman_class": "ORDER_FLOW_DEAD",
                "ruled_posture_exemption": False,
            },
        },
        guard_state={
            "generated_at": "2026-08-05T00:09:59Z",
            "guard_code_identity": {"live_guard_generation_sha256": "resident"},
        },
        restart_state={
            "mismatch_generation_sha256": disk_sha,
            "mismatch_generation_unchanged_since": "2026-08-04T23:50:00Z",
        },
        now=now,
    )

    assert decision["deadman_restart_authority"] is True
    assert decision["restart_required"] is True
    assert decision["reason"] == "red_order_flow_generation_mismatch"
    assert decision["deadman"]["red_basis"] == "raw_accepted_order_limb"
    assert decision["deadman"]["raw_limb_class"] == "ORDER_FLOW_DEAD"
    assert decision["deadman"]["raw_limb_firing"] is True


def test_global_raw_order_flow_limb_without_local_firing_does_not_grant_authority():
    red, evidence = restart._deadman_red(
        {
            "status": "INCIDENT_ZERO_SUPPLY_SEAT",
            "accepted_order_idle_s": 9_000,
            "raw_accepted_order_deadman": {
                "firing": False,
                "global_firing": True,
                "deadman_class": "ORDER_FLOW_DEAD",
            },
        },
        min_red_s=1_800,
    )

    assert red is False
    assert evidence["red_basis"] is None
    assert evidence["raw_limb_firing"] is False


def test_raw_order_flow_limb_exemption_does_not_veto_guard_side_halt():
    red, evidence = restart._deadman_red(
        {
            "status": "INCIDENT_GUARD_SIDE_HALT",
            "accepted_order_idle_s": 30,
            "raw_accepted_order_deadman": {
                "firing": True,
                "deadman_class": "ORDER_FLOW_DEAD",
                "ruled_posture_exemption": True,
            },
        },
        min_red_s=1_800,
    )

    assert red is True
    assert evidence["red_basis"] == "guard_side_halt"
    assert evidence["raw_limb_ruled_posture_exemption"] is True


def test_raw_order_flow_limb_exemption_vetoes_only_order_flow_branch():
    red, evidence = restart._deadman_red(
        {
            "status": "INCIDENT_ZERO_SUPPLY_SEAT",
            "accepted_order_idle_s": 9_000,
            "raw_accepted_order_deadman": {
                "firing": True,
                "deadman_class": "ORDER_FLOW_DEAD",
                "ruled_posture_exemption": True,
            },
        },
        min_red_s=1_800,
    )

    assert red is False
    assert evidence["red_basis"] is None
    assert evidence["raw_limb_ruled_posture_exemption"] is True


def test_measured_source_quiet_does_not_veto_guard_side_halt_class():
    red, evidence = restart._deadman_red(
        {
            "status": "MEASURED_SOURCE_QUIET",
            "deadman_class": "GUARD_SIDE_HALT",
            "accepted_order_idle_s": 30,
        },
        min_red_s=1_800,
    )

    assert red is True
    assert evidence["red_basis"] == "guard_side_halt"


def test_measured_source_quiet_vetoes_raw_order_flow_limb_only():
    red, evidence = restart._deadman_red(
        {
            "status": "MEASURED_SOURCE_QUIET",
            "accepted_order_idle_s": 9_000,
            "raw_accepted_order_deadman": {
                "firing": True,
                "deadman_class": "ORDER_FLOW_DEAD",
            },
        },
        min_red_s=1_800,
    )

    assert red is False
    assert evidence["red_basis"] is None


def test_no_admissible_target_strips_selection_adoption_restart_authority(
    monkeypatch, tmp_path
):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    generation_sha = restart.disk_generation()["sha256"]
    decision = restart.build_decision(
        deadman={
            "status": "INCIDENT_ORDER_FLOW_DEAD",
            "idle_s": 9_000,
            "mechanical_escalation": "MANAGED_RESTART_SELECTION_PENDING_ADOPTION",
            "policy_choke": {
                "wallet_policy_diagnostic": "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
            },
        },
        guard_state={
            "generated_at": "2026-08-02T12:47:30Z",
            "guard_code_identity": {"live_guard_generation_sha256": generation_sha},
        },
        restart_state={},
        now=dt.datetime(2026, 8, 2, 12, 48, tzinfo=dt.timezone.utc),
    )
    assert decision["selection_adoption_due"] is False
    assert decision["restart_required"] is False


def test_selection_adoption_wallet_reads_deadman_selected_identity():
    wallet = "0x" + "3" * 40
    assert restart._selection_adoption_wallet(
        {
            "policy_choke": {
                "actuator": {
                    "candidate_evidence": {"selected": {"wallet": wallet}}
                }
            }
        }
    ) == wallet


def test_selection_adoption_refresh_requires_fresh_selected_row(
    monkeypatch, tmp_path
):
    wallet = "0x" + "3" * 40
    output = tmp_path / "liveness.json"
    output.write_text(
        json.dumps(
            {
                "generated_at": "2026-08-02T13:07:34Z",
                "rows": [
                    {
                        "wallet": wallet,
                        "status": "PASS",
                        "selected_this_run": True,
                        "checkpoint_carryover": False,
                        "fetched_at": "2026-08-02T13:07:34Z",
                        "fetched_at_s": 1785676054.0,
                        "latest_btc5m_trade_ts": 4102444800.0,
                        "btc5m_buys_24h": 601,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    class Completed:
        returncode = 0
        stdout = "{}"
        stderr = ""

    commands = []
    monkeypatch.setattr(
        restart.subprocess,
        "run",
        lambda command, **kwargs: commands.append(command) or Completed(),
    )
    result = restart._refresh_selection_adoption_liveness(
        wallet,
        probe_script=Path("scripts/probe_queue_remote_dataapi_fresh_flow.py"),
        output_path=output,
    )

    assert result["passed"] is True
    assert result["external_liveness_gate"]["reason"] == "external_liveness_pass"
    assert commands[0][-9:] == [
        "--include-wallet",
        wallet,
        "--clearance-limit",
        "0",
        "--ranked-limit",
        "0",
        "--cohort-limit",
        "0",
        "--no-default-include-wallet",
    ]


def test_selection_adoption_refresh_rejects_checkpoint_carryover(
    monkeypatch, tmp_path
):
    wallet = "0x" + "3" * 40
    output = tmp_path / "liveness.json"
    output.write_text(
        json.dumps(
            {
                "generated_at": "2026-08-02T13:07:34Z",
                "rows": [
                    {
                        "wallet": wallet,
                        "status": "PASS",
                        "selected_this_run": False,
                        "checkpoint_carryover": True,
                        "fetched_at_s": 1785676054.0,
                        "latest_btc5m_trade_ts": 4102444800.0,
                        "btc5m_buys_24h": 601,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    class Completed:
        returncode = 0
        stdout = "{}"
        stderr = ""

    monkeypatch.setattr(restart.subprocess, "run", lambda *args, **kwargs: Completed())
    result = restart._refresh_selection_adoption_liveness(
        wallet,
        output_path=output,
    )

    assert result["passed"] is False
    assert result["reason"] == "selection_liveness_row_not_refreshed"


def test_measured_source_quiet_does_not_restart_on_generation_mismatch(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v2", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))

    decision = restart.build_decision(
        deadman={"status": "MEASURED_SOURCE_QUIET", "idle_s": 4000},
        guard_state={
            "generated_at": "2026-07-13T15:09:30Z",
            "guard_code_identity": {"script_sha256": "old"},
        },
        restart_state={},
        now=dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
    )

    assert decision["status"] == "WATCH"
    assert decision["deadman"]["ruled_posture_exemption"] is True
    assert decision["restart_required"] is False


def test_restart_snapshot_names_live_authority_and_daily_cap_reset(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v2", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    now = dt.datetime(2026, 7, 31, 5, 22, tzinfo=dt.timezone.utc)

    decision = restart.build_decision(
        deadman={"status": "INCIDENT_ORDER_FLOW_DEAD", "idle_s": 80_000},
        guard_state={
            "generated_at": "2026-07-31T05:21:30Z",
            "guard_code_identity": {"script_sha256": "old"},
        },
        restart_state={
            "last_restart_at": "2026-07-31T05:16:33Z",
            "restart_events": [
                {"at": f"2026-07-31T0{hour}:00:00Z", "reason": "operator_approved_generation_reload"}
                for hour in (1, 3, 5)
            ],
        },
        now=now,
    )

    assert decision["snapshot_of"] == "restart_actuator_decision_at_generated_at"
    assert decision["superseded_by_guard_state"]["path"].endswith(
        "wallet_copy_live_guard_state.json"
    )
    assert decision["next_eligible_restart_at"] == "2026-08-01T00:00:00Z"
    assert "daily_restart_cap" in decision["next_eligible_restart_reasons"]


def test_storm_guard_counts_every_restart_reason() -> None:
    now = dt.datetime(2026, 8, 2, 19, 30, tzinfo=dt.timezone.utc)
    storm = restart._storm_guard(
        {
            "restart_events": [
                {"at": "2026-08-02T00:00:00Z", "reason": "operator_approved_generation_reload"},
                {"at": "2026-08-02T12:00:00Z", "reason": "selection_pending_adoption"},
                {"at": "2026-08-02T15:00:00Z", "reason": "local_skip_generation_mismatch"},
                {"at": "2026-08-02T19:00:00Z", "reason": "generation_mismatch_adoption"},
            ]
        },
        now=now,
        cooldown_s=1800.0,
        max_per_day=3,
    )

    assert storm["restarts_today"] == 4
    assert storm["daily_cap_clear"] is False
    assert storm["excluded_non_storm_reasons"] == []


def test_red_generation_mismatch_waits_for_quiescence(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v2", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))

    decision = restart.build_decision(
        deadman={"status": "INCIDENT_ORDER_FLOW_DEAD", "idle_s": 2000},
        guard_state={
            "generated_at": "2026-07-13T15:09:30Z",
            "guard_code_identity": {"script_sha256": "old"},
        },
        restart_state={},
        now=dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
    )

    assert decision["status"] == "WATCH"
    assert decision["restart_required"] is False
    assert decision["generation_mismatch_quiescence"]["clear"] is False
    assert decision["generation_mismatch_quiescence"]["unchanged_age_s"] == 0.0


def test_local_skip_generation_mismatch_uses_one_daily_restart(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v2", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    now = dt.datetime(2026, 7, 31, 0, 20, tzinfo=dt.timezone.utc)
    disk_sha = restart.disk_generation()["sha256"]
    base_state = {
        "mismatch_generation_sha256": disk_sha,
        "mismatch_generation_unchanged_since": "2026-07-31T00:00:00Z",
        "restart_events": [
            {
                "at": f"2026-07-31T00:0{index}:00Z",
                "reason": "selection_pending_adoption",
            }
            for index in range(3)
        ],
    }
    deadman = {
        "status": "WATCH_LOCAL_SKIP_STARVATION",
        "deadman_class": "POLICY_CHOKE_LOCAL_SKIP",
        "idle_s": 2000,
        "can_trade": True,
    }
    guard = {
        "generated_at": "2026-07-31T00:19:30Z",
        "guard_code_identity": {"script_sha256": "old"},
    }

    first = restart.build_decision(
        deadman=deadman,
        guard_state=guard,
        restart_state=base_state,
        now=now,
    )
    second = restart.build_decision(
        deadman=deadman,
        guard_state=guard,
        restart_state={
            **base_state,
            "restart_events": [
                {"at": "2026-07-31T00:10:00Z", "reason": "local_skip_generation_mismatch"}
            ],
        },
        now=now,
    )
    third = restart.build_decision(
        deadman=deadman,
        guard_state=guard,
        restart_state={
            **base_state,
            "restart_events": [
                {"at": "2026-07-31T00:10:00Z", "reason": "local_skip_generation_mismatch"},
                {"at": "2026-07-31T00:15:00Z", "reason": "generation_mismatch_adoption"},
            ],
        },
        now=now,
    )

    assert first["reason"] == "local_skip_generation_mismatch"
    assert first["restart_required"] is True
    assert first["restart_allowed"] is True
    assert first["storm_guard"]["daily_cap_clear"] is False
    assert first["generation_adoption_budget"]["clear"] is False
    assert first["generation_adoption_budget"]["restarts_today"] == 3
    assert first["local_skip_reload_budget"]["clear"] is True
    assert second["reason"] == "no_restart_condition"
    assert second["restart_allowed"] is False
    assert second["local_skip_reload_budget"]["clear"] is False
    assert second["generation_adoption_budget"]["clear"] is False
    assert third["restart_required"] is False
    assert third["next_eligible_restart_at"] == "2026-08-01T00:00:00Z"
    assert "local_skip_reload_cap" in third["next_eligible_restart_reasons"]
    assert "generation_adoption_cap" in third["next_eligible_restart_reasons"]


def test_red_generation_mismatch_spends_shared_daily_adoption_budget(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v2", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    now = dt.datetime(2026, 8, 2, 18, 40, tzinfo=dt.timezone.utc)
    disk_sha = restart.disk_generation()["sha256"]
    base_state = {
        "mismatch_generation_sha256": disk_sha,
        "mismatch_generation_unchanged_since": "2026-08-02T18:00:00Z",
        "restart_events": [
            {
                "at": f"2026-08-02T0{hour}:00:00Z",
                "reason": "selection_pending_adoption",
            }
            for hour in (1, 3, 5)
        ],
    }
    deadman = {"status": "INCIDENT_ORDER_FLOW_DEAD", "idle_s": 4000}
    guard = {
        "generated_at": "2026-08-02T18:39:30Z",
        "guard_code_identity": {"script_sha256": "old"},
    }

    allowed = restart.build_decision(
        deadman=deadman,
        guard_state=guard,
        restart_state=base_state,
        now=now,
    )
    spent = restart.build_decision(
        deadman=deadman,
        guard_state={**guard, "generated_at": "2026-08-02T18:49:30Z"},
        restart_state={
            **base_state,
            "restart_events": [
                *base_state["restart_events"],
                {
                    "at": "2026-08-02T18:45:00Z",
                    "reason": "red_order_flow_generation_mismatch",
                },
            ],
        },
        now=dt.datetime(2026, 8, 2, 18, 50, tzinfo=dt.timezone.utc),
    )

    assert allowed["reason"] == "red_order_flow_generation_mismatch"
    assert allowed["restart_allowed"] is False
    assert allowed["storm_guard"]["daily_cap_clear"] is False
    assert allowed["generation_adoption_budget"]["clear"] is False
    assert allowed["generation_adoption_budget"]["restarts_today"] == 3
    assert spent["restart_required"] is True
    assert spent["restart_allowed"] is False
    assert spent["generation_adoption_budget"]["clear"] is False
    assert "generation_adoption_cap" in spent["next_eligible_restart_reasons"]


def test_storm_escalation_forbids_further_same_day_restart(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v2", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    now = dt.datetime(2026, 8, 4, 14, 0, tzinfo=dt.timezone.utc)

    decision = restart.build_decision(
        deadman={"status": "OK", "idle_s": 0},
        guard_state={
            "generated_at": "2026-08-04T13:59:30Z",
            "guard_code_identity": {"script_sha256": "old"},
        },
        restart_state={
            "storm_escalation_events": [
                {
                    "at": "2026-08-04T13:30:00Z",
                    "status": "ESCALATE_RESTART_STORM",
                }
            ]
        },
        now=now,
        allow_generation_reload=True,
        generation_mismatch_quiescence_s=0.0,
        max_per_day=99,
        cooldown_s=0.0,
    )

    assert decision["restart_required"] is True
    assert decision["restart_allowed"] is False
    assert decision["status"] == "ESCALATE_RESTART_STORM"
    assert decision["storm_escalated_today"] is True


def test_generation_reload_requires_explicit_allow_flag(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v2", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))

    base = {
        "deadman": {"status": "OK", "idle_s": 0},
        "guard_state": {
            "generated_at": "2026-07-13T15:09:30Z",
            "guard_code_identity": {"script_sha256": "old"},
        },
        "restart_state": {},
        "now": dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
    }

    denied = restart.build_decision(**base)
    allowed = restart.build_decision(
        **base,
        allow_generation_reload=True,
        generation_reload_reason="unit test code reload",
    )

    assert denied["status"] == "WATCH"
    assert denied["restart_required"] is False
    assert allowed["status"] == "RESTART_REQUIRED"
    assert allowed["reason"] == "operator_approved_generation_reload"
    assert allowed["generation_reload"]["reason"] == "unit test code reload"


def test_feed_health_preflight_requires_stable_fresh_canonical_clean_feed(monkeypatch, tmp_path):
    feed = tmp_path / "feed.jsonl"
    feed.write_text('{"event":"rtds_trade_event"}\n', encoding="utf-8")
    circuit = tmp_path / "circuit.json"
    circuit.write_text(
        json.dumps(
            {
                "status": "OK",
                "pid": 123,
                "current_process_started_at_s": 100.0,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(restart.os, "kill", lambda _pid, _signal: None)
    restart.os.utime(feed, (1090.0, 1090.0))

    result = restart.feed_health_preflight(
        circuit_state_path=circuit,
        feed_path=feed,
        canonical_feed_path=feed,
        min_uptime_s=900,
        max_feed_age_s=60,
        now_ts=1100.0,
    )

    assert result["status"] == "PASS"
    assert result["passed"] is True
    assert all(result["checks"].values())
    assert result["feed_age_s"] == 10.0


def test_feed_health_preflight_rejects_dirty_tail(tmp_path):
    feed = tmp_path / "feed.jsonl"
    feed.write_text('{"event":"ok"}\nnot-json\n', encoding="utf-8")
    circuit = tmp_path / "circuit.json"
    circuit.write_text("{}", encoding="utf-8")

    result = restart.feed_health_preflight(
        circuit_state_path=circuit,
        feed_path=feed,
        canonical_feed_path=feed,
        now_ts=1100.0,
    )

    assert result["status"] == "WAIT_FEED_HEALTH"
    assert result["checks"]["parse_tail_clean"] is False


def test_execute_holds_restart_when_feed_health_preflight_fails(monkeypatch, tmp_path):
    state = tmp_path / "restart_state.json"
    deadman = tmp_path / "deadman.json"
    guard = tmp_path / "guard.json"
    generation = tmp_path / "run_wallet_copy_live_guard.py"
    generation.write_text("new", encoding="utf-8")
    deadman.write_text(
        json.dumps({"status": "INCIDENT_ORDER_FLOW_DEAD", "idle_s": 1900}),
        encoding="utf-8",
    )
    guard.write_text(
        json.dumps(
            {
                "generated_at": "2099-01-01T00:00:00Z",
                "guard_code_identity": {"script_sha256": "old"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(restart, "GENERATION_FILES", (generation,))
    state.write_text(
        json.dumps(
            {
                "mismatch_generation_sha256": restart.disk_generation()["sha256"],
                "mismatch_generation_unchanged_since": "2026-07-30T02:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        restart.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("restart must not execute")),
    )
    monkeypatch.setattr(
        restart.subprocess,
        "Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("restart must not execute")),
    )

    restart.main(
        [
            "--deadman",
            str(deadman),
            "--guard-state",
            str(guard),
            "--state",
            str(state),
            "--event-log",
            str(tmp_path / "events.jsonl"),
            "--feed-circuit-state",
            str(tmp_path / "missing-circuit.json"),
            "--feed-path",
            str(tmp_path / "missing-feed.jsonl"),
            "--execute",
        ]
    )

    stored = json.loads(state.read_text(encoding="utf-8"))
    assert stored["latest_decision"]["status"] == "WAIT_FEED_HEALTH"
    assert stored["latest_decision"]["restart_allowed"] is False
    assert stored["latest_decision"]["restart_deferred_reason"] == "feed_health_preflight_failed"


def test_force_restart_reason_requires_restart(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    generation = restart.disk_generation((script,))
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))

    decision = restart.build_decision(
        deadman={"status": "OK", "idle_s": 0},
        guard_state={
            "generated_at": "2026-07-18T15:09:30Z",
            "guard_code_identity": {"live_guard_generation_sha256": generation["sha256"]},
        },
        restart_state={},
        now=dt.datetime(2026, 7, 18, 15, 10, tzinfo=dt.timezone.utc),
        force_restart_reason="guard_memory_rss_threshold",
    )

    assert decision["status"] == "RESTART_REQUIRED"
    assert decision["reason"] == "guard_memory_rss_threshold"
    assert decision["restart_allowed"] is True
    assert decision["forced_restart"]["enabled"] is True
    assert decision["forced_restart"]["reason"] == "guard_memory_rss_threshold"


def test_guard_side_halt_requires_restart_without_generation_mismatch(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    generation = restart.disk_generation((script,))
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))

    decision = restart.build_decision(
        deadman={"status": "INCIDENT_GUARD_SIDE_HALT", "deadman_class": "GUARD_SIDE_HALT", "idle_s": 30},
        guard_state={
            "generated_at": "2026-07-13T15:09:30Z",
            "guard_code_identity": {"live_guard_generation_sha256": generation["sha256"]},
        },
        restart_state={},
        now=dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
    )

    assert decision["status"] == "RESTART_REQUIRED"
    assert decision["reason"] == "guard_side_halt"
    assert decision["restart_allowed"] is True


def test_already_running_full_state_is_restart_input(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))

    decision = restart.build_decision(
        deadman={"status": "OK", "idle_s": 0},
        guard_state={"status": "LIVE_GUARD_ALREADY_RUNNING", "generated_at": "2026-07-13T15:09:59Z"},
        restart_state={"consecutive_guard_liveness_breaches": 1},
        now=dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
    )

    assert decision["status"] == "RESTART_REQUIRED"
    assert decision["reason"] == "guard_unresponsive"
    assert decision["guard_liveness"]["stale_already_running_state"] is True


def test_single_stale_guard_sample_does_not_restart(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))

    decision = restart.build_decision(
        deadman={"status": "OK", "idle_s": 0},
        guard_state={
            "status": "LIVE_GUARD_RUNNING",
            "generated_at": "2026-07-13T15:04:50Z",
        },
        restart_state={},
        now=dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
        max_guard_state_age_s=300.0,
    )

    assert decision["status"] == "WATCH"
    assert decision["restart_required"] is False
    assert decision["guard_liveness"]["raw_breach"] is True
    assert decision["guard_liveness"]["consecutive_breaches"] == 1


def test_dead_guard_pid_restarts_on_first_strike(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))
    monkeypatch.setattr(restart.os, "kill", lambda *_args: (_ for _ in ()).throw(OSError()))

    decision = restart.build_decision(
        deadman={"status": "OK", "idle_s": 0},
        guard_state={
            "status": "LIVE_GUARD_RUNNING",
            "generated_at": "2026-07-13T15:09:50Z",
            "pid": 999999,
        },
        restart_state={},
        now=dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
        max_guard_state_age_s=300.0,
    )

    assert decision["reason"] == "guard_unresponsive"
    assert decision["restart_required"] is True
    assert decision["guard_liveness"]["pid_dead"] is True
    assert decision["guard_liveness"]["consecutive_breaches"] == 0


def test_second_stale_guard_sample_restarts(monkeypatch, tmp_path):
    script = tmp_path / "run_wallet_copy_live_guard.py"
    script.write_text("guard-v1", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (script,))

    decision = restart.build_decision(
        deadman={"status": "OK", "idle_s": 0},
        guard_state={
            "status": "LIVE_GUARD_RUNNING",
            "generated_at": "2026-07-13T15:04:50Z",
        },
        restart_state={"consecutive_guard_liveness_breaches": 1},
        now=dt.datetime(2026, 7, 13, 15, 10, tzinfo=dt.timezone.utc),
        max_guard_state_age_s=300.0,
    )

    assert decision["reason"] == "guard_unresponsive"
    assert decision["restart_required"] is True
    assert decision["guard_liveness"]["consecutive_breaches"] == 2


def test_dry_run_preserves_persisted_latches(monkeypatch, tmp_path):
    state = tmp_path / "restart_state.json"
    deadman = tmp_path / "deadman.json"
    guard = tmp_path / "guard.json"
    generation = tmp_path / "run_wallet_copy_live_guard.py"
    generation.write_text("guard-v1", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (generation,))
    prior = {
        "consecutive_guard_liveness_breaches": 1,
        "mismatch_generation_sha256": "prior-sha",
        "mismatch_generation_unchanged_since": "2026-07-13T15:00:00Z",
        "restart_events": [],
    }
    state.write_text(json.dumps(prior), encoding="utf-8")
    deadman.write_text(json.dumps({"status": "OK", "idle_s": 0}), encoding="utf-8")
    guard.write_text(
        json.dumps(
            {
                "status": "LIVE_GUARD_RUNNING",
                "generated_at": "2099-01-01T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    assert restart.main(
        [
            "--deadman",
            str(deadman),
            "--guard-state",
            str(guard),
            "--state",
            str(state),
            "--event-log",
            str(tmp_path / "events.jsonl"),
        ]
    ) == 0

    stored = json.loads(state.read_text(encoding="utf-8"))
    assert stored["consecutive_guard_liveness_breaches"] == 1
    assert stored["mismatch_generation_sha256"] == "prior-sha"
    assert (
        stored["mismatch_generation_unchanged_since"]
        == "2026-07-13T15:00:00Z"
    )
    assert stored["latest_decision"]["consecutive_guard_liveness_breaches"] == 1
    assert stored["latest_decision"]["computed_liveness_breaches"] == 0
    assert stored["latest_decision"]["dry_run"] is True


def test_restart_state_records_execute_decision(monkeypatch, tmp_path):
    state = tmp_path / "restart_state.json"
    deadman = tmp_path / "deadman.json"
    guard = tmp_path / "guard.json"
    start = tmp_path / "start_live_guard.sh"
    event_log = tmp_path / "events.jsonl"
    live_change_journal = tmp_path / "live_change_journal.jsonl"
    start.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    deadman.write_text(json.dumps({"status": "INCIDENT_ORDER_FLOW_DEAD", "idle_s": 1900}), encoding="utf-8")
    guard.write_text(json.dumps({"generated_at": "2099-01-01T00:00:00Z", "guard_code_identity": {"script_sha256": "old"}}), encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (start,))
    state.write_text(
        json.dumps(
            {
                "mismatch_generation_sha256": restart.disk_generation()["sha256"],
                "mismatch_generation_unchanged_since": "2026-07-30T02:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    class Completed:
        returncode = 0
        stdout = ""
        stderr = ""

    class Proc:
        pid = 12345

    monkeypatch.setattr(restart.subprocess, "run", lambda *args, **kwargs: Completed())
    monkeypatch.setattr(restart.subprocess, "Popen", lambda *args, **kwargs: Proc())
    monkeypatch.setattr(restart.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        restart,
        "sweep_stale_remnants",
        lambda **_kwargs: {"status": "PASS", "deleted": [], "retained": []},
    )

    rc = restart.main(
        [
            "--deadman",
            str(deadman),
            "--guard-state",
            str(guard),
            "--state",
            str(state),
            "--event-log",
            str(event_log),
            "--live-change-journal",
            str(live_change_journal),
            "--start-script",
            str(start),
            "--execute",
            "--no-require-feed-health-preflight",
        ]
    )

    assert rc == 0
    stored = json.loads(state.read_text(encoding="utf-8"))
    assert stored["latest_decision"]["status"] == "RESTART_EXECUTED"
    assert stored["latest_decision"]["execution"]["started_pid"] == 12345
    assert stored["latest_decision"]["execution"]["quiescent_remnant_sweep"]["status"] == "PASS"
    assert stored["restart_events"][0]["started_pid"] == 12345
    assert stored["storm_guard"]["last_restart_at"] == stored["last_restart_at"]
    assert stored["storm_guard"]["restarts_today"] == 1
    assert stored["storm_guard"]["cooldown_clear"] is False
    journal_row = json.loads(live_change_journal.read_text(encoding="utf-8").splitlines()[0])
    assert journal_row["defect_id"] == "red_order_flow_generation_mismatch"
    assert stored["latest_decision"]["pre_restart_journal"]["status"] == "RECORDED_BEFORE_RESTART"


def test_restart_execute_records_actual_lock_holder_pid(monkeypatch, tmp_path):
    state = tmp_path / "restart_state.json"
    deadman = tmp_path / "deadman.json"
    guard = tmp_path / "guard.json"
    start = tmp_path / "start_live_guard.sh"
    event_log = tmp_path / "events.jsonl"
    live_change_journal = tmp_path / "live_change_journal.jsonl"
    lock = tmp_path / "lock.json"
    start.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    deadman.write_text(json.dumps({"status": "OK", "idle_s": 0}), encoding="utf-8")
    guard.write_text(json.dumps({"generated_at": "2099-01-01T00:00:00Z"}), encoding="utf-8")
    lock.write_text(
        json.dumps(
            {
                "pid": 777,
                "started_at": "2026-07-18T15:00:00Z",
                "state": "data/research/wallet_copy_live_guard_state.json",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(restart, "GENERATION_FILES", (start,))

    class Completed:
        returncode = 0
        stdout = ""
        stderr = ""

    class Proc:
        pid = 12345

    monkeypatch.setattr(restart.subprocess, "run", lambda *args, **kwargs: Completed())
    monkeypatch.setattr(restart.subprocess, "Popen", lambda *args, **kwargs: Proc())
    monkeypatch.setattr(restart.time, "sleep", lambda _seconds: None)
    guard_row_calls = iter(
        [
            [],
            [{"pid": 777, "command": "python scripts/run_wallet_copy_live_guard.py"}],
        ]
    )
    monkeypatch.setattr(restart, "_live_guard_process_rows", lambda: next(guard_row_calls))

    rc = restart.main(
        [
            "--deadman",
            str(deadman),
            "--guard-state",
            str(guard),
            "--state",
            str(state),
            "--event-log",
            str(event_log),
            "--live-change-journal",
            str(live_change_journal),
            "--start-script",
            str(start),
            "--lock-file",
            str(lock),
            "--execute",
            "--no-require-feed-health-preflight",
            "--force-restart-reason",
            "guard_memory_rss_threshold",
        ]
    )

    assert rc == 0
    stored = json.loads(state.read_text(encoding="utf-8"))
    execution = stored["latest_decision"]["execution"]
    assert execution["start_process_pid"] == 12345
    assert execution["started_pid"] == 777
    assert execution["actual_pid"] == 777
    assert execution["actual_pid_source"] == "lock_holder"
    assert execution["lock_holder"]["pid"] == 777
    assert stored["restart_events"][0]["started_pid"] == 777


def test_restart_unloads_launchd_before_sweep_and_bootstraps_after(monkeypatch, tmp_path):
    start = tmp_path / "start_live_guard.sh"
    start.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    events = []

    class Completed:
        def __init__(self, returncode=0, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run(args, **_kwargs):
        events.append(tuple(args))
        if args[:2] == ["launchctl", "print"]:
            return Completed(stdout=restart.DEFAULT_LAUNCHD_LABEL)
        return Completed()

    monkeypatch.setattr(restart.subprocess, "run", fake_run)
    monkeypatch.setattr(
        restart.subprocess,
        "Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("launchd-managed restart must not start a second process directly")
        ),
    )
    monkeypatch.setattr(restart, "_live_guard_process_rows", lambda: [])
    monkeypatch.setattr(
        restart,
        "_actual_live_guard_pid",
        lambda _lock: {
            "actual_pid": 24680,
            "actual_pid_source": "lock_holder",
            "lock_holder": {"pid": 24680},
            "pgrep_rows": [],
        },
    )

    def fake_sweep(**_kwargs):
        events.append(("sweep",))
        return {"status": "PASS", "deleted": [], "retained": []}

    monkeypatch.setattr(restart, "sweep_stale_remnants", fake_sweep)

    result = restart._execute_restart(
        start,
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        lock_path=tmp_path / "guard.lock",
    )

    bootout_index = next(i for i, row in enumerate(events) if row[:2] == ("launchctl", "bootout"))
    sweep_index = events.index(("sweep",))
    bootstrap_index = next(i for i, row in enumerate(events) if row[:2] == ("launchctl", "bootstrap"))
    assert bootout_index < sweep_index < bootstrap_index
    assert result["launchd_coordination"]["unload_before_sweep"] is True
    assert result["launchd_coordination"]["reloaded_after_sweep"] is True
    assert result["start_process_pid"] is None
    assert result["started_pid"] == 24680


def test_watch_mode_recovers_restart_history_from_event_log(monkeypatch, tmp_path):
    state = tmp_path / "restart_state.json"
    deadman = tmp_path / "deadman.json"
    guard = tmp_path / "guard.json"
    event_log = tmp_path / "events.jsonl"
    generation_file = tmp_path / "run_wallet_copy_live_guard.py"
    generation_file.write_text("guard", encoding="utf-8")
    monkeypatch.setattr(restart, "GENERATION_FILES", (generation_file,))
    deadman.write_text(json.dumps({"status": "MEASURED_SOURCE_QUIET", "idle_s": 4000}), encoding="utf-8")
    guard.write_text(
        json.dumps(
            {
                "generated_at": "2099-01-01T00:00:00Z",
                "pid": 0,
                "guard_code_identity": {
                    "live_guard_generation_sha256": restart.disk_generation((generation_file,))["sha256"],
                    "script_sha256": "same",
                },
            }
        ),
        encoding="utf-8",
    )
    event_log.write_text(
        json.dumps(
            {
                "status": "RESTART_EXECUTED",
                "generated_at": "2026-07-13T16:06:04Z",
                "reason": "guard_unresponsive",
                "execution": {"started_pid": 777},
                "disk_generation": {"sha256": "abc"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    rc = restart.main(
        [
            "--deadman",
            str(deadman),
            "--guard-state",
            str(guard),
            "--state",
            str(state),
            "--event-log",
            str(event_log),
        ]
    )

    stored = json.loads(state.read_text(encoding="utf-8"))
    assert rc == 0
    assert stored["restart_events"][0]["started_pid"] == 777
    assert stored["latest_decision"]["status"] == "WATCH"
    assert stored["generated_at"] == stored["latest_decision"]["generated_at"]
    assert stored["status"] == stored["latest_decision"]["status"]
    assert stored["storm_guard"] == stored["latest_decision"]["storm_guard"]
    assert stored["last_restart_at"] == "2026-07-13T16:06:04Z"
