from scripts import build_wide_exact_policy_manifest as manifest_builder

build_manifest = manifest_builder.build_manifest
ORDER128_TEST_IDENTITY = (
    "0x3048d65321be3497164cdfc2996f94f98a2e7537",
    "8c39887edd0b5adbcb75bde537372fa9e4b98665c92f0579cf486488514f7a58",
)


def test_production_order128_paper_focus_is_empty_after_deadline() -> None:
    assert manifest_builder.STICKY_PAPER_ACCRUAL_FOCUS == ()


def test_order128_focus_requires_stable_packet_at_consumed_deadman_cut(monkeypatch) -> None:
    wallet, fingerprint = ORDER128_TEST_IDENTITY
    monkeypatch.setattr(
        manifest_builder, "STICKY_PAPER_ACCRUAL_FOCUS", ((wallet, fingerprint),)
    )
    packet = {
        "cut_consistent": True,
        "deadman_checked_at": "cut-2",
        "binding_action": {
            "wallet": wallet,
            "wide_policy_fingerprint": fingerprint,
        },
        "rank_stability": {
            "cuts_agreed": 2,
            "stable_for_accrual": True,
        },
    }
    assert manifest_builder._order128_focus_authorized(
        packet, {"checked_at": "cut-2"}
    ) is True
    assert manifest_builder._order128_focus_authorized(
        packet, {"checked_at": "cut-3"}
    ) is False
import json
import os
import plistlib
import pytest

from scripts.run_wide_prospective_supervisor import (
    _DirectFanoutReceiver,
    _attach_direct_book_prefetch,
    _capture_cmd,
    _drain_fanout,
    _matured_unresolved_slugs,
    _run,
    _run_policy_fingerprint_evidence,
    _refresh_order128_packet,
    _retained_scorer_cycles,
    _require_order128_refresh_success,
    ForwardLaneSpec,
    acquire_supervisor_lock,
    ensure_capture_inventory,
    publish_active_manifest_pointer,
    record_missing_produced_seed,
    resolve_seed_alpha,
    recover_completed_boundary,
    score_once,
    score_forward_lane,
)
import socket


def test_scorer_cycle_history_retains_twelve_starts_for_cadence_median() -> None:
    cycles = [{"started_at_s": float(index)} for index in range(20)]

    retained = _retained_scorer_cycles(cycles)

    assert len(retained) == 12
    assert [row["started_at_s"] for row in retained] == [float(i) for i in range(8, 20)]


def test_run_contains_timeout_and_records_duration(monkeypatch):
    def timeout(*_args, **kwargs):
        raise __import__("subprocess").TimeoutExpired("cmd", kwargs["timeout"])

    monkeypatch.setattr("scripts.run_wide_prospective_supervisor.subprocess.run", timeout)
    result = _run(["python", "slow.py"], timeout_s=0.01)

    assert result["ok"] is False
    assert result["timed_out"] is True
    assert result["returncode"] is None
    assert result["duration_s"] >= 0
    assert result["stderr_tail"] == "TimeoutExpired after 0.01s"


def test_policy_fingerprint_timeout_is_recorded_without_crashing() -> None:
    cmd = ["python", "scripts/build_wide_policy_fingerprint_evidence.py"]

    def runner(command, timeout_s):
        raise __import__("subprocess").TimeoutExpired(command, timeout_s)

    result = _run_policy_fingerprint_evidence(cmd, runner=runner)
    assert result["status"] == "POLICY_FINGERPRINT_EVIDENCE_TIMEOUT"
    assert result["ok"] is False
    assert result["timeout_s"] == 60.0


def test_dedicated_deadman_launchd_job_has_bound_cadence_and_no_handoff() -> None:
    with open(
        "launchd/com.belavarga.polymarket.order-flow-deadman.plist", "rb"
    ) as handle:
        job = plistlib.load(handle)
    argv = job["ProgramArguments"]
    assert job["RunAtLoad"] is True
    assert job["StartInterval"] == 180
    assert argv[:3] == ["/usr/bin/nice", "-n", "5"]
    assert argv[-1].endswith("scripts/order_flow_deadman.py")
    assert "--handoff" not in argv


def test_refresh_order128_packet_invokes_reporter() -> None:
    calls = []

    def runner(cmd, timeout_s):
        calls.append((cmd, timeout_s))
        return {"ok": True, "returncode": 0}

    result = _refresh_order128_packet("wide_cut", runner)
    assert result == {"ok": True, "returncode": 0}
    assert calls[0][0][1:] == [
        "scripts/report_order128_fastest_lawful_path.py",
        "--score-run-id",
        "wide_cut",
    ]
    assert calls[0][1] == 60.0


def _order128_guard_source() -> tuple[int, int, str]:
    """Locate the refresh call, the manifest build, and the text between them."""
    import ast
    import pathlib

    path = pathlib.Path("scripts/run_wide_prospective_supervisor.py")
    lines = path.read_text().splitlines()
    tree = ast.parse("\n".join(lines))
    fn = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "run_supervisor"
    )
    refresh_lines = [
        node.lineno
        for node in ast.walk(fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_refresh_order128_packet"
    ]
    manifest_lines = [
        node.lineno
        for node in ast.walk(fn)
        if isinstance(node, ast.Constant)
        and node.value == "scripts/build_wide_exact_policy_manifest.py"
    ]
    assert len(refresh_lines) == 1, refresh_lines
    assert len(manifest_lines) == 1, manifest_lines
    between = "\n".join(lines[refresh_lines[0] : manifest_lines[0] - 1])
    return refresh_lines[0], manifest_lines[0], between


def test_refresh_order128_packet_is_ordered_before_manifest_build() -> None:
    """The coupling is source-order, not just a call — assert the order itself."""
    refresh_line, manifest_line, _ = _order128_guard_source()
    assert refresh_line < manifest_line


def test_order128_refresh_failure_blocks_manifest_build_fail_closed() -> None:
    """The checked guard must remain between refresh and manifest build."""
    _, _, between = _order128_guard_source()
    assert "_require_order128_refresh_success(order128_result)" in between


@pytest.mark.parametrize(
    "result",
    [
        {"ok": False, "returncode": 1},
        {"ok": True, "returncode": 2},
        {"ok": True},
    ],
)
def test_order128_refresh_refusal_or_failure_raises(result) -> None:
    """Both command failure and repo-convention refusal fail closed."""
    with pytest.raises(RuntimeError):
        _require_order128_refresh_success(result)


def test_order128_refresh_zero_returncode_passes() -> None:
    _require_order128_refresh_success({"ok": True, "returncode": 0})


def test_order128_packet_producer_and_consumer_share_one_default_path() -> None:
    """Exact-cut equality is only achievable if both defaults name one file."""
    import pathlib
    import re

    reporter = pathlib.Path(
        "scripts/report_order128_fastest_lawful_path.py"
    ).read_text()
    consumer = pathlib.Path(
        "scripts/build_wide_exact_policy_manifest.py"
    ).read_text()
    produced = re.search(
        r'"--output",\s*default="([^"]+order128[^"]+)"', reporter
    )
    consumed = re.search(
        r'"--order128-packet",\s*\n?\s*default="([^"]+order128[^"]+)"', consumer
    )
    assert produced is not None and consumed is not None
    assert produced.group(1) == consumed.group(1)


W1 = "0x0000000000000000000000000000000000000001"
W2 = "0x0000000000000000000000000000000000000002"
CLIMB_IDENTITY = (
    "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82",
    "d277e7fcd160ead7a5cf014f7d516d5dbec64aa74a0c32ed52c074d0c3a3d7f4",
)


def test_manifest_sticky_focus_injects_exact_identity_as_paper_only(monkeypatch):
    wallet, fingerprint = ORDER128_TEST_IDENTITY
    monkeypatch.setattr(
        manifest_builder, "STICKY_PAPER_ACCRUAL_FOCUS", ((wallet, fingerprint),)
    )
    manifest = build_manifest(
        alpha=_alpha(),
        queue={"ranked_queue": []},
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence={
            "cells": [
                {
                    "identity": {
                        "wallet": wallet,
                        "move_slice_keys": ["000-060|<=0.25"],
                    },
                    "wide_policy_fingerprint": fingerprint,
                    "venue_executable_full_stream_rescore": {
                        "resolved": 95,
                        "f1_pass": False,
                    },
                }
            ]
        },
    )

    assert manifest["admitted_wallets"] == []
    row = manifest["capture_watch_wallets"][0]
    assert row["wallet"] == wallet
    assert row["wide_policy_fingerprint"] == fingerprint
    assert row["slice_freeze"]["status"] == "FROZEN_SOLE_FOCUS_PAPER_FEEDSTOCK"
    assert row["paper_measurement_only"] is True
    assert row["promotion_authority"] is False


def test_manifest_refuses_sticky_identity_when_residual_zero_vr_projection_fails(monkeypatch):
    wallet, fingerprint = ORDER128_TEST_IDENTITY
    monkeypatch.setattr(
        manifest_builder, "STICKY_PAPER_ACCRUAL_FOCUS", ((wallet, fingerprint),)
    )
    manifest = build_manifest(
        alpha=_alpha(),
        queue={"ranked_queue": []},
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence={
            "cells": [
                {
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "identity": {"wallet": wallet, "move_slice_keys": ["slice"]},
                    "wide_policy_fingerprint": fingerprint,
                    "venue_executable_full_stream_rescore": {
                        "resolved": 1,
                        "venue_executable_resolved": 1,
                        "venue_unreachable_resolved": 1000,
                        "venue_reachable_share_min_pct": 40.0,
                    },
                }
            ]
        },
    )
    assert not any(
        row.get("wallet") == wallet
        and row.get("wide_policy_fingerprint") == fingerprint
        for row in manifest["capture_watch_wallets"]
    )


def test_bac25_resolution_priority_selects_only_matured_unresolved_windows():
    state = {
        "orders": [
            {"market_slug": "btc-updown-5m-1000", "resolved": False},
            {"market_slug": "btc-updown-5m-1300", "resolved": False},
            {"market_slug": "btc-updown-5m-700", "resolved": True},
            {"market_slug": "not-btc", "resolved": False},
        ]
    }

    assert _matured_unresolved_slugs(state, now_s=1500) == [
        "btc-updown-5m-1000"
    ]


def test_forward_lane_threads_identity_and_all_artifact_paths(tmp_path):
    spec = ForwardLaneSpec(
        run_id="951b_forward_only",
        wallet="0x951b",
        fingerprint="fingerprint951b",
        policy_family="family951b",
        manifest=str(tmp_path / "manifest.json"),
        state=str(tmp_path / "state.json"),
        ledger=str(tmp_path / "orders.jsonl"),
        evidence=str(tmp_path / "evidence.json"),
        atomic_output=str(tmp_path / "atomic.json"),
        lane_output=str(tmp_path / "lane.json"),
        source_history=str(tmp_path / "source.json"),
    )
    commands = []

    def runner(command, timeout):
        commands.append(command)
        return {"returncode": 0}

    score_forward_lane(
        spec=spec,
        seed_alpha="alpha.json",
        polygon_jsonl="polygon.jsonl",
        resolution_path="resolutions.json",
        direct_event=[],
        runner=runner,
    )
    evidence_cmd = next(
        command
        for command in commands
        if "scripts/build_wide_policy_fingerprint_evidence.py" in command
    )
    assert evidence_cmd[evidence_cmd.index("--atomic-output") + 1] == str(
        tmp_path / "atomic.json"
    )
    assert "--no-sweep-output" in evidence_cmd

    builders = [
        command
        for command in commands
        if "scripts/build_bac25_forward_only_lane.py" in command
    ]
    assert len(builders) == 2
    for command in builders:
        assert command[command.index("--wallet") + 1] == spec.wallet
        assert command[command.index("--fingerprint") + 1] == spec.fingerprint
        assert command[command.index("--policy-family") + 1] == spec.policy_family
        assert command[command.index("--score-run-id") + 1] == spec.run_id
        assert command[command.index("--lane-kind") + 1] == (
            f"{spec.run_id}_paper_lane"
        )
        assert command[command.index("--manifest") + 1] == spec.manifest
        assert command[command.index("--output") + 1] == spec.lane_output
    reconcile = next(
        command
        for command in commands
        if "scripts/reconcile_wide_exact_policy_paper.py" in command
    )
    assert reconcile[reconcile.index("--run-id") + 1] == spec.run_id
    assert reconcile[reconcile.index("--state") + 1] == spec.state
    assert reconcile[reconcile.index("--ledger") + 1] == spec.ledger


def test_terminal_forward_lane_stop_writer_skips_all_commands(tmp_path):
    spec = ForwardLaneSpec(
        run_id="951b_forward_only",
        wallet="0x951b",
        fingerprint="fingerprint951b",
        policy_family="family951b",
        manifest=str(tmp_path / "manifest.json"),
        state=str(tmp_path / "state.json"),
        ledger=str(tmp_path / "orders.jsonl"),
        evidence=str(tmp_path / "evidence.json"),
        atomic_output=str(tmp_path / "atomic.json"),
        lane_output=str(tmp_path / "lane.json"),
        source_history=str(tmp_path / "source.json"),
    )
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "terminal_outcome_on_deadline": {
                    "status": "PARK_FORWARD_EVIDENCE_REFUSED",
                    "stop_writer": True,
                    "terminal": True,
                }
            }
        )
    )
    commands = []

    def runner(command, timeout):
        commands.append((command, timeout))
        return {"ok": True, "returncode": 0}

    results = score_forward_lane(
        spec=spec,
        seed_alpha="alpha.json",
        polygon_jsonl="polygon.jsonl",
        resolution_path="resolutions.json",
        direct_event=[],
        runner=runner,
    )

    assert commands == []
    assert results == [
        {
            "cmd": ["forward_lane_terminal_stop_writer", "951b_forward_only"],
            "returncode": 0,
            "ok": True,
            "status": "FORWARD_LANE_TERMINAL_STOP_WRITER_SKIPPED",
            "run_id": "951b_forward_only",
            "manifest": str(tmp_path / "manifest.json"),
            "terminal_status": "PARK_FORWARD_EVIDENCE_REFUSED",
            "stop_writer": True,
        }
    ]


def test_supervisor_lock_refuses_second_holder(tmp_path):
    lock_path = tmp_path / "supervisor.lock"
    first = acquire_supervisor_lock(lock_path)
    try:
        try:
            acquire_supervisor_lock(lock_path)
        except RuntimeError as exc:
            assert str(exc) == "WIDE_PROSPECTIVE_SUPERVISOR_LOCK_HELD"
        else:
            raise AssertionError("second supervisor lock unexpectedly acquired")
    finally:
        first.close()


def test_active_manifest_pointer_names_exact_consumed_manifest(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    pointer_path = tmp_path / "active.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_id": "manifest-fresh",
                "source_alpha_report": "alpha.json",
                "source_alpha_status": "PASS_CURRENT_SOURCE",
                "source_alpha_age_h": 0.5,
            }
        )
    )

    pointer = publish_active_manifest_pointer(
        str(manifest_path), pointer_path=pointer_path
    )

    assert pointer["manifest_id"] == "manifest-fresh"
    assert json.loads(pointer_path.read_text())["manifest_path"] == str(manifest_path)


def test_missing_score_alpha_is_loud_and_seed_does_not_advance(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    state = data / "supervisor.json"

    missing = record_missing_produced_seed(
        supervisor_state_path=state,
        supervisor={},
        seed_run="wide_seed",
        score_run="wide_missing",
        completed=[],
    )

    payload = json.loads(state.read_text())
    assert missing is True
    assert payload["status"] == "SEED_ALPHA_NOT_PRODUCED"
    assert payload["seed_run_id"] == "wide_seed"
    assert payload["managed_run_id"] == "wide_missing"
    assert payload["missing_seed_alpha_path"].endswith(
        "alpha_decay_report_wide_missing.json"
    )


def test_adopted_active_manifest_resolves_its_exact_bound_alpha(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    alpha = data / "alpha_order.json"
    manifest = data / "manifest.json"
    pointer = data / "active.json"
    alpha.write_text("{}")
    manifest.write_text(
        json.dumps(
            {
                "score_run_id": "wide_adopted",
                "manifest_id": "manifest-1",
                "source_alpha_report": str(alpha),
            }
        )
    )
    pointer.write_text(
        json.dumps(
            {"manifest_path": str(manifest), "manifest_id": "manifest-1"}
        )
    )

    assert resolve_seed_alpha(
        "wide_adopted", active_manifest_pointer=pointer
    ) == str(alpha)


def test_wide_capture_is_enrolled_before_writer_starts(tmp_path):
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps({"entries": []}))
    capture = "data/research/polygon_orderfilled_ws_capture_alpha_decay_wide_test.jsonl"

    first = ensure_capture_inventory(capture, inventory_path=inventory)
    second = ensure_capture_inventory(capture, inventory_path=inventory)

    payload = json.loads(inventory.read_text())
    assert first["status"] == "ENROLLED"
    assert second["status"] == "ALREADY_ENROLLED"
    assert len(payload["entries"]) == 1
    assert payload["entries"][0] == {
        "path": capture,
        "cap_bytes": 6 * 1024**3,
        "keep_tail_bytes": 2 * 1024**3,
        "max_consumer_tail_bytes": 2 * 1024**3,
        "rotation_action": "copytruncate_line_aligned_tail",
        "readers": [
            "scripts/run_wide_prospective_supervisor.py direct-event reconcile",
            "scripts/capture_clob_book_snapshots.py --polygon-jsonl tail scanner",
            "scripts/report_alpha_decay.py current rolling WIDE capture",
        ],
    }


def _alpha():
    return {
        "status": "PASS_CURRENT_SOURCE",
        "updated_at": "2026-07-25T00:00:00Z",
        "source_freshness": {"pass": True, "history_is_frozen_d97": False},
        "execution_profiles": {
            "profiles_by_wallet": {
                W1: {
                    "eligible": True,
                    "fill_sample": 30,
                    "copyable_rate_pct": 80,
                    "mean_edge": 0.02,
                    "median_edge": 0.01,
                    "move_slices": [
                        {
                            "move_slice_key": "000-060|0.25-0.50",
                            "mean_edge": 0.02,
                            "median_edge": 0.01,
                            "copyable_rate_pct": 80,
                        }
                    ],
                },
                W2: {
                    "eligible": True,
                    "move_slices": [
                        {
                            "move_slice_key": "000-060|0.25-0.50",
                            "mean_edge": 0.02,
                            "median_edge": 0.01,
                            "copyable_rate_pct": 80,
                        }
                    ],
                },
            }
        },
    }


def _policy_evidence(*wallets: str) -> dict:
    move_slice_keys = ["000-060|0.25-0.50"]
    return {
        "cells": [
            {
                "identity": {
                    "wallet": wallet,
                    "move_slice_keys": move_slice_keys,
                },
                "wide_policy_fingerprint": manifest_builder.wide_policy_identity(
                    wallet=wallet,
                    move_slice_keys=move_slice_keys,
                )["wide_policy_fingerprint"],
                "venue_executable_full_stream_rescore": {
                    "resolved": 40,
                    "f1_pass": False,
                },
            }
            for wallet in wallets
        ]
    }


def test_manifest_is_full_queue_intersection_and_excludes_negative_wallet():
    manifest = build_manifest(
        alpha=_alpha(),
        queue={
            "ranked_queue": [
                {"wallet": W1, "queue_rank": 1, "admission_status": "READY_QUEUE"},
                {"wallet": W2, "queue_rank": 2, "admission_status": "READY_QUEUE"},
            ]
        },
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={"rows": [{"wallet": W2, "regime_slice_label": "PROVEN_NEGATIVE"}]},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence=_policy_evidence(W1),
    )
    assert [row["wallet"] for row in manifest["admitted_wallets"]] == [W1]
    assert [row["wallet"] for row in manifest["capture_watch_wallets"]] == [W1]
    assert manifest["ready_queue_count"] == 2
    refused = next(row for row in manifest["refusal_census"] if row["wallet"] == W2)
    assert refused["refusal_reasons"] == ["standing_demotion_or_negative_exclusion"]


def test_manifest_keeps_positive_copy_pnl_roster_wallet_paper_only():
    manifest = build_manifest(
        alpha=_alpha(),
        queue={
            "ranked_queue": [
                {"wallet": W1, "queue_rank": 1, "admission_status": "READY_QUEUE"},
            ]
        },
        roster={
            "excluded_prior_live_demotion_wallets": [],
            "wallets": [{
                "address": W2,
                "tags": ["paper_only", "positive_copy_pnl_depth"],
                "weekday_resolved_trades": 500,
                "copy_pnl_usd": 25.0,
            }],
        },
        degrade={},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence=_policy_evidence(W1, W2),
    )

    assert manifest["ready_queue_count"] == 2
    assert [row["wallet"] for row in manifest["capture_watch_wallets"]] == [W1, W2]
    assert manifest["paper_only"] is True
    assert manifest["live_orders_allowed"] is False
    assert all(row["promotion_authority"] is False for row in manifest["capture_watch_wallets"])


def test_manifest_observes_demoted_positive_depth_without_promotion() -> None:
    manifest = build_manifest(
        alpha=_alpha(),
        queue={"ranked_queue": []},
        roster={
            "excluded_prior_live_demotion_wallets": [W2],
            "wallets": [{
                "address": W2,
                "tags": ["paper_only", "positive_copy_pnl_depth"],
                "weekday_resolved_trades": 500,
                "copy_pnl_usd": 25.0,
            }],
        },
        degrade={"rows": [{"wallet": W2, "status": "DEMOTED"}]},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence=_policy_evidence(W2),
    )

    assert W2 not in {row["wallet"] for row in manifest["admitted_wallets"]}
    assert [row["wallet"] for row in manifest["capture_watch_wallets"]] == [W2]
    row = manifest["capture_watch_wallets"][0]
    assert row["capture_exclusion_overridden"] is True
    assert row["promotion_authority"] is False
    assert row["paper_measurement_only"] is True


def test_manifest_bootstraps_new_positive_depth_policy_fail_closed() -> None:
    manifest = build_manifest(
        alpha=_alpha(),
        queue={"ranked_queue": []},
        roster={
            "excluded_prior_live_demotion_wallets": [],
            "wallets": [{
                "address": W2,
                "tags": ["paper_only", "positive_copy_pnl_depth"],
                "weekday_resolved_trades": 500,
                "copy_pnl_usd": 25.0,
            }],
        },
        degrade={},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence={"cells": []},
    )

    assert manifest["admitted_wallets"] == []
    assert len(manifest["capture_watch_wallets"]) == 1
    row = manifest["capture_watch_wallets"][0]
    assert row["wide_policy_fingerprint"]
    assert row["slice_freeze"]["f1"] == {
        "status": "PENDING_FIRST_CAPTURE",
        "f1_pass": False,
        "promotion_authority": False,
    }
    assert row["promotion_authority"] is False
    assert row["paper_measurement_only"] is True


def test_manifest_refuses_externally_stale_alpha_source_and_names_cause() -> None:
    alpha = _alpha()
    alpha["updated_at"] = "2026-07-29T16:00:00Z"
    alpha["source_freshness"]["freshness_limit_s"] = 86400.0
    manifest = build_manifest(
        alpha=alpha,
        queue={"ranked_queue": [{
            "wallet": W1,
            "queue_rank": 1,
            "admission_status": "READY_QUEUE",
        }]},
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="stale.json",
        score_run_id="stale",
        source_sha256="abc",
        fingerprint_evidence=_policy_evidence(W1),
        generated_at="2026-07-31T10:00:00Z",
    )

    assert manifest["source_alpha_status"] == "STALE_SOURCE_REFUSED_NOT_CURRENT"
    assert manifest["source_alpha_age_h"] == 42.0
    assert manifest["admitted_wallets"] == []
    assert manifest["promotion_admitted_wallets"] == []
    assert manifest["admitted_wallets_blocked_by"] == "stale_alpha_source"
    assert manifest["promotion_admitted_wallets_blocked_by"] == "stale_alpha_source"
    assert manifest["summary"]["blocked_by"] == "stale_alpha_source"
    assert "stale_alpha_source" in manifest["refusal_census"][0]["refusal_reasons"]


def test_manifest_keeps_fresh_alpha_source_current() -> None:
    alpha = _alpha()
    alpha["updated_at"] = "2026-07-31T09:30:00Z"
    alpha["source_freshness"]["freshness_limit_s"] = 86400.0
    manifest = build_manifest(
        alpha=alpha,
        queue={"ranked_queue": [{
            "wallet": W1,
            "queue_rank": 1,
            "admission_status": "READY_QUEUE",
        }]},
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="fresh.json",
        score_run_id="fresh",
        source_sha256="abc",
        fingerprint_evidence=_policy_evidence(W1),
        generated_at="2026-07-31T10:00:00Z",
    )

    assert manifest["source_alpha_status"] == "PASS_CURRENT_SOURCE"
    assert manifest["source_alpha_age_h"] == 0.5
    assert manifest["admitted_wallets_blocked_by"] is None
    assert [row["wallet"] for row in manifest["admitted_wallets"]] == [W1]


def test_manifest_bootstraps_ready_queue_policy_rotation_without_admission() -> None:
    manifest = build_manifest(
        alpha=_alpha(),
        queue={"ranked_queue": [{
            "wallet": W1,
            "queue_rank": 1,
            "admission_status": "READY_QUEUE",
        }]},
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="fresh-rotation.json",
        score_run_id="fresh-rotation",
        source_sha256="abc",
        fingerprint_evidence={"cells": []},
    )

    assert manifest["admitted_wallets"] == []
    row = manifest["capture_watch_wallets"][0]
    assert row["slice_freeze"]["f1"]["status"] == "PENDING_FIRST_CAPTURE"
    refused = manifest["refusal_census"][0]
    assert "pending_first_fingerprint_capture" in refused["refusal_reasons"]


def test_manifest_places_exact_depth_priority_cell_first_without_admission() -> None:
    move_slice_keys = ["060-120|0.25-0.50"]
    fingerprint = manifest_builder.wide_policy_identity(
        wallet=W2,
        move_slice_keys=move_slice_keys,
    )["wide_policy_fingerprint"]
    evidence = {
        "cells": [{
            "wide_policy_fingerprint": fingerprint,
            "identity": {
                "wallet": W2,
                "wide_policy_fingerprint": fingerprint,
                "move_slice_keys": move_slice_keys,
            },
            "venue_executable_full_stream_rescore": {
                "resolved": 208,
                "post_fee_pnl_usd": 35.0,
                "roi_pct": 17.0,
                "first_half_post_fee_pnl_usd": 14.0,
                "second_half_post_fee_pnl_usd": 21.0,
                "concentration_admissible": True,
                "venue_reachable_share_pct": 95.0,
                "f1_pass": True,
            },
        }],
    }
    manifest = build_manifest(
        alpha=_alpha(),
        queue={"ranked_queue": [{
            "wallet": W1,
            "queue_rank": 1,
            "admission_status": "READY_QUEUE",
        }]},
        roster={
            "excluded_prior_live_demotion_wallets": [],
            "wallets": [{
                "address": W2,
                "tags": ["paper_only", "positive_copy_pnl_depth", "depth_priority_frontier"],
                "depth_priority_rank": 1,
                "depth_priority_cell": {
                    "wide_policy_fingerprint": fingerprint,
                    "move_slice_keys": move_slice_keys,
                },
            }],
        },
        degrade={},
        source_alpha_report="final.json",
        score_run_id="depth",
        source_sha256="abc",
        fingerprint_evidence={**_policy_evidence(W1), "cells": [
            *_policy_evidence(W1)["cells"],
            *evidence["cells"],
        ]},
    )

    rows = manifest["capture_watch_wallets"]
    assert rows[0]["wallet"] == W2
    assert rows[0]["wide_policy_fingerprint"] == fingerprint
    assert rows[0]["move_slice_keys"] == move_slice_keys
    assert rows[0]["depth_priority"] is True
    assert rows[0]["slice_freeze"]["status"] == "FROZEN_DEPTH_PRIORITY_PAPER_FEEDSTOCK"
    assert rows[0]["promotion_authority"] is False
    assert W2 not in {row["wallet"] for row in manifest["admitted_wallets"]}


def test_manifest_binds_alpha_identity_and_live_ready_cohort_intersection():
    alpha = _alpha()
    alpha["asset_context_entries"] = 67
    alpha["execution_profiles"]["fills_total"] = 9146
    manifest = build_manifest(
        alpha=alpha,
        queue={
            "ranked_queue": [
                {"wallet": W1, "queue_rank": 1, "admission_status": "READY_QUEUE"},
                {"wallet": W2, "queue_rank": 2, "admission_status": "READY_QUEUE"},
            ]
        },
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence=_policy_evidence(W1, W2),
        cohort={
            "_source_path": "cohort.json",
            "generated_at": "2026-07-25T00:01:00Z",
            "live_ready_picks": [{"wallet": W1, "status": "LIVE_READY_SHADOW_PICK"}],
        },
    )
    assert [row["wallet"] for row in manifest["admitted_wallets"]] == [W1]
    assert [row["wallet"] for row in manifest["capture_watch_wallets"]] == [W1, W2]
    assert manifest["source_identity"] == {
        "source_artifact": "final.json",
        "source_sha256": "abc",
        "source_status": "PASS_CURRENT_SOURCE",
        "source_timestamp": "2026-07-25T00:00:00Z",
        "fill_count": 9146,
        "overlap_asset_count": 67,
        "eligible_profile_count": 2,
        "eligible_profile_wallets": [W1, W2],
        "cohort_artifact": "cohort.json",
        "cohort_generated_at": "2026-07-25T00:01:00Z",
        "cohort_live_ready_pick_count": 1,
        "intersection_required": True,
    }
    refused = next(row for row in manifest["refusal_census"] if row["wallet"] == W2)
    assert "not_in_live_ready_market_cohort_replay" in refused["refusal_reasons"]


def test_manifest_marks_empty_policy_and_serializes_every_real_policy() -> None:
    alpha = _alpha()
    alpha["execution_profiles"]["profiles_by_wallet"][W2]["move_slices"] = []
    manifest = build_manifest(
        alpha=alpha,
        queue={
            "ranked_queue": [
                {"wallet": W1, "queue_rank": 1, "admission_status": "READY_QUEUE"},
                {"wallet": W2, "queue_rank": 2, "admission_status": "READY_QUEUE"},
            ]
        },
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence=_policy_evidence(W1),
    )

    for row in manifest["capture_watch_wallets"]:
        if row["move_slice_keys"]:
            assert row["wide_policy_fingerprint"]
            assert isinstance(row["slice_freeze"]["f1"], dict)
            assert row["policy_absent"] is False
        else:
            assert row["policy_absent"] is True
            assert row["wide_policy_fingerprint"] is None


def test_manifest_freezes_fingerprint_slices_while_source_drought_fires():
    frozen = ["240-300|>0.75", "000-060|0.25-0.50"]
    manifest = build_manifest(
        alpha=_alpha(),
        queue={
            "ranked_queue": [
                {"wallet": W1, "queue_rank": 1, "admission_status": "READY_QUEUE"}
            ]
        },
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence={
            "generated_at": "2026-07-25T00:01:00Z",
            "freeze_overrides": {
                W1: {
                    "wide_policy_fingerprint": "fp-1",
                    "move_slice_keys": frozen,
                    "reason": "source_roster_drought_fingerprint_accrual",
                    "f1": {"resolved": 199},
                }
            },
        },
        source_roster_drought_firing=True,
    )
    row = manifest["capture_watch_wallets"][0]
    assert row["move_slice_keys"] == sorted(frozen)
    assert row["wide_policy_fingerprint"] == "fp-1"
    assert row["slice_freeze"]["status"] == "FROZEN_SOURCE_ROSTER_DROUGHT"
    assert manifest["fingerprint_slice_freeze"]["frozen_wallets"] == [W1]


def test_manifest_frozen_drought_identity_stays_paper_captured_when_degraded():
    frozen = ["000-060|0.25-0.50"]
    manifest = build_manifest(
        alpha=_alpha(),
        queue={
            "ranked_queue": [
                {"wallet": W1, "queue_rank": 1, "admission_status": "READY_QUEUE"}
            ]
        },
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={"rows": [{"wallet": W1, "regime_slice_label": "PROVEN_NEGATIVE"}]},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence={
            "generated_at": "2026-07-25T00:01:00Z",
            "freeze_overrides": {
                W1: {
                    "wide_policy_fingerprint": "fp-1",
                    "move_slice_keys": frozen,
                    "reason": "source_roster_drought_fingerprint_accrual",
                    "f1": {"resolved": 612},
                }
            },
        },
        source_roster_drought_firing=True,
    )

    assert manifest["admitted_wallets"] == []
    row = manifest["capture_watch_wallets"][0]
    assert row["wallet"] == W1
    assert row["wide_policy_fingerprint"] == "fp-1"
    assert row["promotion_authority"] is False
    assert row["slice_freeze"]["capture_exclusion_overridden"] is True
    refused = next(row for row in manifest["refusal_census"] if row["wallet"] == W1)
    assert "standing_demotion_or_negative_exclusion" in refused["refusal_reasons"]


def test_manifest_injects_f1_closed_climb_identity_as_capture_only(monkeypatch):
    monkeypatch.setattr(
        manifest_builder,
        "DIRECTION_DIRECT_CLIMB_PRIORITY",
        (CLIMB_IDENTITY,),
    )

    wallet, fingerprint = CLIMB_IDENTITY
    slices = ["060-120|<=0.25", "060-120|>0.75", "120-180|>0.75"]
    f1 = {
        "resolved": 227,
        "f1_pass": True,
        "post_fee_pnl_usd": 86.56,
        "first_half_post_fee_pnl_usd": 23.75,
        "second_half_post_fee_pnl_usd": 62.81,
    }
    manifest = build_manifest(
        alpha=_alpha(),
        queue={"ranked_queue": []},
        roster={"excluded_prior_live_demotion_wallets": [wallet]},
        degrade={"rows": [{"wallet": wallet, "status": "DEMOTED"}]},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence={
            "cells": [
                {
                    "identity": {
                        "wallet": wallet,
                        "move_slice_keys": slices,
                    },
                    "wide_policy_fingerprint": fingerprint,
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": f1,
                }
            ]
        },
    )

    assert manifest["admitted_wallets"] == []
    assert manifest["promotion_admitted_wallets"] == []
    assert len(manifest["capture_watch_wallets"]) == 1
    row = manifest["capture_watch_wallets"][0]
    assert row["wallet"] == wallet
    assert row["wide_policy_fingerprint"] == fingerprint
    assert row["move_slice_keys"] == sorted(slices)
    assert row["paper_measurement_only"] is True
    assert row["promotion_authority"] is False
    assert row["capture_exclusion_overridden"] is True
    assert row["slice_freeze"]["status"] == (
        "FROZEN_CLIMB_PRIORITY_PAPER_FEEDSTOCK"
    )


def test_manifest_climb_injects_half_negative_cell_as_capture_only(monkeypatch):
    monkeypatch.setattr(
        manifest_builder,
        "DIRECTION_DIRECT_CLIMB_PRIORITY",
        (CLIMB_IDENTITY,),
    )

    wallet, fingerprint = CLIMB_IDENTITY
    manifest = build_manifest(
        alpha=_alpha(),
        queue={"ranked_queue": []},
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence={
            "cells": [
                {
                    "identity": {
                        "wallet": wallet,
                        "move_slice_keys": ["060-120|<=0.25"],
                    },
                    "wide_policy_fingerprint": fingerprint,
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "resolved": 227,
                        "f1_pass": True,
                        "post_fee_pnl_usd": 10,
                        "first_half_post_fee_pnl_usd": -1,
                        "second_half_post_fee_pnl_usd": 11,
                    },
                }
            ]
        },
    )

    assert manifest["admitted_wallets"] == []
    assert manifest["promotion_admitted_wallets"] == []
    row = manifest["capture_watch_wallets"][0]
    assert row["wide_policy_fingerprint"] == fingerprint
    assert row["paper_measurement_only"] is True
    assert row["promotion_authority"] is False
    assert row["slice_freeze"]["reason"].endswith(
        "f1_closed_halves_open_paper_feedstock"
    )


def test_manifest_climb_overrides_existing_queue_capture_with_exact_fingerprint(
    monkeypatch,
):
    monkeypatch.setattr(
        manifest_builder,
        "DIRECTION_DIRECT_CLIMB_PRIORITY",
        (CLIMB_IDENTITY,),
    )
    wallet, fingerprint = CLIMB_IDENTITY
    manifest = build_manifest(
        alpha=_alpha(),
        queue={
            "ranked_queue": [
                {
                    "wallet": wallet,
                    "queue_rank": 1,
                    "admission_status": "READY_QUEUE",
                }
            ]
        },
        roster={"excluded_prior_live_demotion_wallets": []},
        degrade={},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence={
            "cells": [
                {
                    "identity": {
                        "wallet": wallet,
                        "move_slice_keys": ["060-120|<=0.25"],
                    },
                    "wide_policy_fingerprint": fingerprint,
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "resolved": 72,
                        "f1_pass": False,
                        "post_fee_pnl_usd": -25,
                        "first_half_post_fee_pnl_usd": -11,
                        "second_half_post_fee_pnl_usd": -14,
                    },
                }
            ]
        },
    )

    row = manifest["capture_watch_wallets"][0]
    assert row["wallet"] == wallet
    assert row["wide_policy_fingerprint"] == fingerprint
    assert row["move_slice_keys"] == ["060-120|<=0.25"]
    assert row["promotion_authority"] is False
    assert row["paper_measurement_only"] is True
    assert (
        row["slice_freeze"]["status"]
        == "FROZEN_CLIMB_PRIORITY_PAPER_FEEDSTOCK"
    )


def test_manifest_injects_both_halves_positive_f1_open_climb_as_capture_only(
    monkeypatch,
):
    monkeypatch.setattr(
        manifest_builder,
        "DIRECTION_DIRECT_CLIMB_PRIORITY",
        (CLIMB_IDENTITY,),
    )

    wallet, fingerprint = CLIMB_IDENTITY
    manifest = build_manifest(
        alpha=_alpha(),
        queue={"ranked_queue": []},
        roster={"excluded_prior_live_demotion_wallets": [wallet]},
        degrade={"rows": [{"wallet": wallet, "status": "DEMOTED"}]},
        source_alpha_report="final.json",
        score_run_id="next",
        source_sha256="abc",
        fingerprint_evidence={
            "cells": [
                {
                    "identity": {
                        "wallet": wallet,
                        "move_slice_keys": ["060-120|<=0.25"],
                    },
                    "wide_policy_fingerprint": fingerprint,
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "resolved": 50,
                        "f1_pass": False,
                        "post_fee_pnl_usd": 57.81,
                        "first_half_post_fee_pnl_usd": 20.13,
                        "second_half_post_fee_pnl_usd": 37.67,
                    },
                }
            ]
        },
    )

    assert manifest["admitted_wallets"] == []
    assert manifest["promotion_admitted_wallets"] == []
    row = manifest["capture_watch_wallets"][0]
    assert row["wide_policy_fingerprint"] == fingerprint
    assert row["paper_measurement_only"] is True
    assert row["promotion_authority"] is False
    assert row["slice_freeze"]["reason"].endswith("f1_open_paper_feedstock")


def test_two_automatic_score_cycles_preserve_required_order_without_wallet_list(tmp_path):
    calls = []
    standings = tmp_path / "standings.json"

    def runner(cmd, timeout):
        calls.append((cmd, timeout))
        if "scripts/build_wide_candidate_standings.py" in cmd:
            (tmp_path / "standings.json").write_bytes(b'{"standings":true}')
        return {"ok": True, "returncode": 0}

    for refresh in (True, False):
        results = score_once(
            run_id="next",
            seed_alpha="final.json",
            manifest="manifest.json",
            polygon_jsonl="growing.jsonl",
            state_path="state.json",
            ledger_path="ledger.jsonl",
            standings_path=str(standings),
            resolution_path="resolutions.jsonl",
            refresh_resolutions=refresh,
            runner=runner,
        )
        assert all(result["ok"] for result in results)
    scripts = [next(value for value in cmd if value.startswith("scripts/")) for cmd, _ in calls]
    assert scripts == [
        "scripts/refresh_btc_5m_resolutions_from_gamma.py",
            "scripts/reconcile_wide_exact_policy_paper.py",
            "scripts/build_wide_candidate_standings.py",
            "scripts/report_wide_f3_batch_interval_attribution.py",
            "scripts/build_wide_policy_fingerprint_evidence.py",
        "scripts/build_frozen_fingerprint_f2_prewarm_shadow.py",
        "scripts/build_copy_freeze_near_bar_allpass_dryrun_sidecar.py",
            "scripts/reconcile_wide_exact_policy_paper.py",
            "scripts/build_wide_candidate_standings.py",
            "scripts/report_wide_f3_batch_interval_attribution.py",
            "scripts/build_wide_policy_fingerprint_evidence.py",
        "scripts/build_frozen_fingerprint_f2_prewarm_shadow.py",
        "scripts/build_copy_freeze_near_bar_allpass_dryrun_sidecar.py",
    ]
    reconcile_cmd = calls[1][0]
    assert "--manifest" in reconcile_cmd
    assert "--wallet" not in reconcile_cmd
    assert "--f3-instrumentation-jsonl" in reconcile_cmd
    assert "--f3-instrumentation-run-prefix" in reconcile_cmd
    report_cmd = calls[3][0]
    assert "--instrumentation-events" in report_cmd
    assert "--measurement" not in report_cmd
    evidence_commands = [
        cmd
        for cmd, _timeout in calls
        if "scripts/build_wide_policy_fingerprint_evidence.py" in cmd
    ]
    assert all("--atomic-output" in cmd for cmd in evidence_commands)
    assert all(
        cmd[cmd.index("--atomic-output") + 1]
        == "data/research/order134_c_atomic_move_slice_rescore_latest.json"
        for cmd in evidence_commands
    )
    assert all("--sweep-output" in cmd for cmd in evidence_commands)
    assert all(
        cmd[cmd.index("--sweep-output") + 1]
        == "data/research/order134_d_venue_min_order_sweep_latest.json"
        for cmd in evidence_commands
    )
    standings_builds = [
        cmd for cmd, _ in calls if "scripts/build_wide_candidate_standings.py" in cmd
    ]
    assert len(standings_builds) == 2
    snapshot = tmp_path / "wide_candidate_standings_next.json"
    assert snapshot.read_bytes() == standings.read_bytes()


def test_failed_standings_build_does_not_overwrite_snapshot(tmp_path) -> None:
    standings = tmp_path / "standings.json"
    snapshot = tmp_path / "wide_candidate_standings_next.json"
    standings.write_bytes(b"stale-canonical")
    snapshot.write_bytes(b"preserve-snapshot")
    prior_mtime = snapshot.stat().st_mtime_ns

    def runner(cmd, timeout):
        if "scripts/build_wide_candidate_standings.py" in cmd:
            return {"ok": False, "returncode": 1}
        return {"ok": True, "returncode": 0}

    results = score_once(
        run_id="next",
        seed_alpha="final.json",
        manifest="manifest.json",
        polygon_jsonl="growing.jsonl",
        state_path="state.json",
        ledger_path="ledger.jsonl",
        standings_path=str(standings),
        resolution_path="resolutions.jsonl",
        refresh_resolutions=False,
        runner=runner,
    )

    copy_result = next(
        row
        for row in results
        if row.get("status") == "STANDINGS_SNAPSHOT_SKIPPED_STALE_SOURCE"
    )
    assert copy_result["ok"] is False
    assert snapshot.read_bytes() == b"preserve-snapshot"
    assert snapshot.stat().st_mtime_ns == prior_mtime


def test_score_once_records_policy_evidence_timeout_and_continues() -> None:
    def runner(cmd, timeout):
        if "scripts/build_wide_policy_fingerprint_evidence.py" in cmd:
            raise __import__("subprocess").TimeoutExpired(cmd, timeout)
        return {"ok": True, "returncode": 0, "stdout_tail": "{}"}

    results = score_once(
        run_id="next",
        seed_alpha="final.json",
        manifest="manifest.json",
        polygon_jsonl="growing.jsonl",
        state_path="state.json",
        ledger_path="ledger.jsonl",
        standings_path="standings.json",
        resolution_path="resolutions.jsonl",
        refresh_resolutions=False,
        runner=runner,
    )

    timeout = next(
        row
        for row in results
        if row.get("status") == "POLICY_FINGERPRINT_EVIDENCE_TIMEOUT"
    )
    assert timeout["ok"] is False
    assert any(
        row.get("ok") is True and row.get("returncode") == 0 for row in results
    )


def test_direct_fanout_payload_is_forwarded_without_poll_interval():
    calls = []
    payloads = []

    def runner(cmd, timeout):
        calls.append((cmd, timeout))
        if "--direct-event-file" in cmd:
            with open(cmd[cmd.index("--direct-event-file") + 1]) as handle:
                payloads.append(json.load(handle))
        return {"ok": True, "returncode": 0}

    score_once(
        run_id="next",
        seed_alpha="final.json",
        manifest="manifest.json",
        polygon_jsonl="canonical.jsonl",
        state_path="state.json",
        ledger_path="ledger.jsonl",
        standings_path="standings.json",
        resolution_path="resolutions.jsonl",
        refresh_resolutions=False,
        direct_event={"event": "polygon_orderfilled_log", "transaction_hash": "0xdirect"},
        runner=runner,
    )
    reconcile = calls[0][0]
    assert "--direct-event-file" in reconcile
    assert payloads[0]["transaction_hash"] == "0xdirect"
    capture = _capture_cmd(
        "next",
        1800,
        "roster.json",
        fanout_socket="/tmp/wide.sock",
    )
    assert capture[-2:] == ["--orderfilled-fanout-socket", "/tmp/wide.sock"]


def test_all_pass_sidecar_invokes_existing_deadman_in_same_score_cycle():
    calls = []

    def runner(cmd, timeout):
        calls.append((cmd, timeout))
        if "scripts/build_copy_freeze_near_bar_allpass_dryrun_sidecar.py" in cmd:
            return {
                "ok": True,
                "returncode": 0,
                "stdout_tail": '{"all_pass": true, "status": "ALL_PASS_READY"}',
            }
        return {"ok": True, "returncode": 0}

    score_once(
        run_id="next",
        seed_alpha="final.json",
        manifest="manifest.json",
        polygon_jsonl="canonical.jsonl",
        state_path="state.json",
        ledger_path="ledger.jsonl",
        standings_path="standings.json",
        resolution_path="resolutions.jsonl",
        refresh_resolutions=False,
        runner=runner,
    )
    scripts = [
        next(value for value in cmd if value.startswith("scripts/"))
        for cmd, _ in calls
    ]
    assert scripts[-2:] == [
        "scripts/build_copy_freeze_near_bar_allpass_dryrun_sidecar.py",
        "scripts/order_flow_deadman.py",
    ]


def test_empty_direct_batch_disables_cumulative_sidecar_scan():
    calls = []
    payloads = []

    def runner(command, _timeout):
        calls.append(command)
        if "--direct-event-file" in command:
            with open(command[command.index("--direct-event-file") + 1]) as handle:
                payloads.append(json.load(handle))
        return {"ok": True, "returncode": 0}

    score_once(
        run_id="wide_generation",
        seed_alpha="alpha.json",
        manifest="manifest.json",
        polygon_jsonl="cumulative.jsonl",
        state_path="state.json",
        ledger_path="ledger.jsonl",
        standings_path="standings.json",
        resolution_path="resolutions.jsonl",
        refresh_resolutions=False,
        direct_event=[],
        runner=runner,
    )

    reconcile = calls[0]
    assert "--direct-event-file" in reconcile
    assert payloads[0] == []


def test_oversized_direct_batch_never_enters_argv():
    observed = {}
    oversized = [
        {"transaction_hash": f"0x{index:064x}", "blob": "x" * 20_000}
        for index in range(80)
    ]

    def runner(command, _timeout):
        if "--direct-event-file" in command:
            observed["argv_bytes"] = sum(len(value) for value in command)
            with open(command[command.index("--direct-event-file") + 1]) as handle:
                observed["payload"] = json.load(handle)
        return {"ok": True, "returncode": 0}

    score_once(
        run_id="wide_oversized",
        seed_alpha="alpha.json",
        manifest="manifest.json",
        polygon_jsonl="capture.jsonl",
        state_path="state.json",
        ledger_path="ledger.jsonl",
        standings_path="standings.json",
        resolution_path="resolutions.jsonl",
        refresh_resolutions=False,
        direct_event=oversized,
        runner=runner,
    )

    assert observed["argv_bytes"] < 10_000
    assert observed["payload"] == oversized


def test_direct_fanout_drain_batches_and_dedupes_tx_log(tmp_path):
    path = f"/tmp/wide-drain-{os.getpid()}.sock"
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    receiver.bind(path)
    receiver.setblocking(False)
    sender = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    first = {"transaction_hash": "0xabc", "log_index": 1}
    second = {"transaction_hash": "0xdef", "log_index": 2}
    for row in (first, first, second):
        sender.sendto(json.dumps(row).encode(), path)

    rows = _drain_fanout(receiver)

    sender.close()
    receiver.close()
    os.unlink(path)
    assert [
        {key: value for key, value in row.items() if key != "fanout_received_monotonic_s"}
        for row in rows
    ] == [first, second]
    assert all(row["fanout_received_monotonic_s"] > 0 for row in rows)


def test_direct_book_prefetch_is_attached_at_receive_boundary():
    class FakeClob:
        def get_book(self, token_id):
            return {"asset_id": token_id, "asks": [{"price": "0.4", "size": "10"}]}

    rows = _attach_direct_book_prefetch(
        [
            {
                "transaction_hash": "0xabc",
                "log_index": 1,
                "decoded": {"asset": "101"},
            },
            {
                "transaction_hash": "0xdef",
                "log_index": 2,
                "decoded": {"asset": "101"},
            },
        ],
        clob=FakeClob(),
    )

    assert rows[0]["_direct_book_prefetch"]["book"]["asset_id"] == "101"
    assert rows[0]["_direct_book_prefetch"] == rows[1]["_direct_book_prefetch"]
    assert rows[0]["_direct_book_prefetch"]["error"] is None
    assert rows[0]["_direct_book_prefetch"]["fetch_provenance"] == "capture_prefetched"
    assert len(rows[0]["_direct_book_prefetch"]["fetch_cycle_id"]) == 32
    timing = rows[0]["_direct_book_prefetch"]
    assert timing["prefetch_queue_wait_ms"] >= 0
    assert timing["prefetch_worker_queue_ms"] >= 0
    assert timing["prefetch_network_ms"] == 0
    assert timing["prefetch_parse_ms"] >= 0


def test_direct_receiver_prefetches_while_consumer_is_idle():
    class FakeClob:
        def get_book(self, token_id):
            return {"asset_id": token_id}

    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
    left.setblocking(False)
    receiver = _DirectFanoutReceiver(left, clob=FakeClob())
    receiver.start()
    right.send(
        json.dumps(
            {
                "transaction_hash": "0xthread",
                "log_index": 3,
                "decoded": {"asset": "202"},
            }
        ).encode()
    )
    rows = []
    for _ in range(20):
        rows = receiver.drain()
        if rows:
            break
        import time

        time.sleep(0.02)
    receiver.stop()
    left.close()
    right.close()

    assert rows[0]["transaction_hash"] == "0xthread"
    assert rows[0]["_direct_book_prefetch"]["book"]["asset_id"] == "202"


def test_direct_score_path_does_not_prepend_resolution_refresh():
    calls = []

    def runner(cmd, timeout):
        calls.append((cmd, timeout))
        return {"ok": True, "returncode": 0}

    score_once(
        run_id="next",
        seed_alpha="final.json",
        manifest="manifest.json",
        polygon_jsonl="canonical.jsonl",
        state_path="state.json",
        ledger_path="ledger.jsonl",
        standings_path="standings.json",
        resolution_path="resolutions.jsonl",
        refresh_resolutions=False,
        direct_event=[{"event": "polygon_orderfilled_log", "transaction_hash": "0xdirect"}],
        runner=runner,
    )

    assert all("refresh_btc_5m_resolutions_from_gamma.py" not in cmd for cmd, _ in calls)


def test_completed_timebox_boundary_recovers_from_persisted_state(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    run_id = "wide_20260725T005639Z"
    (data / f"alpha_decay_report_{run_id}.json").write_text(
        json.dumps(
            {
                "status": "PASS_CURRENT_SOURCE",
                "execution_profiles": {"status": "PASS"},
            }
        ),
        encoding="utf-8",
    )
    (data / f"alpha_decay_simultaneous_capture_state_{run_id}.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "commands": [
                    {"name": "polygon_fills", "returncode": -15, "duration_s": 1848.0},
                    {"name": "clob_books", "ok": True},
                    {"name": "alpha_decay_report", "ok": True},
                ],
            }
        ),
        encoding="utf-8",
    )

    seed, completed, recovered = recover_completed_boundary(
        supervisor={"managed_run_id": run_id, "completed_runs": []},
        adopt_run_id="wide_20260725T0026Z",
        duration_s=1800.0,
    )

    assert seed == run_id
    assert completed[0]["run_id"] == run_id
    assert recovered["recovery_reason"] == "completed_timebox_polygon_sigterm_misclassified"
