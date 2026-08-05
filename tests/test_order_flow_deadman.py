import json
import os
import subprocess
import sys
import datetime as dt
import fcntl
from pathlib import Path

from scripts import order_flow_deadman as deadman
from scripts import report_f418_acceptance_funnel as f418_funnel
from scripts import run_wallet_copy_live_guard as live_guard
from src.wallet_copy.gate_registry import (
    GUARD_AUTHORED_GATE_CLASSES,
    PRE_SUBMIT_REFUSAL_CLASSES,
)


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/order_flow_deadman.py"
ORDER128_TEST_IDENTITY = (
    "0x3048d65321be3497164cdfc2996f94f98a2e7537",
    "8c39887edd0b5adbcb75bde537372fa9e4b98665c92f0579cf486488514f7a58",
)


def test_deadman_production_sticky_focus_is_empty_after_deadline() -> None:
    assert deadman.STICKY_PAPER_ACCRUAL_FOCUS == ()


def test_every_guard_authored_gate_is_deadman_approved() -> None:
    approved = deadman.APPROVED_GATED_QUIET_EXACT
    assert set(live_guard.LIVE_DROUGHT_FUNNEL_GATE_KEYS).issubset(approved)
    assert set(f418_funnel.GATE_ORDER).issubset(approved)


def test_every_gate_registry_string_is_known_to_deadman() -> None:
    registered = GUARD_AUTHORED_GATE_CLASSES | PRE_SUBMIT_REFUSAL_CLASSES
    known = deadman.APPROVED_GATED_QUIET_EXACT | deadman.LOCAL_REFUSAL_REJECT_CLASSES
    assert registered.issubset(known)
    reason = "entry_price_band_closed_negative_holdout"
    assert reason in PRE_SUBMIT_REFUSAL_CLASSES
    assert reason in deadman.APPROVED_SUPPRESSION_TAGS
    assert reason in deadman.APPROVED_GATED_QUIET_EXACT


def test_operator_launch_authority_survives_candidate_cycle_refusal() -> None:
    guard = {"live_orders_allowed": False, "guard_code_identity": {"pid": 123}}
    command = (
        "python guard.py --execute-live --live-orders-allowed "
        "--explicit-live-operator-go --operator-approval-id OP-LIVE-TEST"
    )
    authority = deadman._operator_live_authority(guard, process_command=command)
    assert authority["active"] is True
    assert authority["operator_approval_id"] == "OP-LIVE-TEST"


def test_money_tripwires_still_veto_operator_authority() -> None:
    authority = {"active": True}
    assert deadman._money_and_tripwires_clear(
        operator_live_authority=authority,
        weekend_rotation={"status": "CLEAR"},
        total_loss_auto_disable={"disabled_members": []},
    ) is True
    assert deadman._money_and_tripwires_clear(
        operator_live_authority=authority,
        weekend_rotation={"status": "TRIGGERED"},
        total_loss_auto_disable={"disabled_members": []},
    ) is False
    assert deadman._money_and_tripwires_clear(
        operator_live_authority=authority,
        weekend_rotation={"status": "CLEAR"},
        total_loss_auto_disable={"disabled_members": ["0xloss"]},
    ) is False
    assert deadman._money_and_tripwires_clear(
        operator_live_authority=authority,
        weekend_rotation={"status": "CLEAR"},
        total_loss_auto_disable={
            "enabled": True,
            "disabled_members": [{"source_wallet": "0xloss"}],
        },
    ) is True


def _direct_packet(updated_at: str) -> dict:
    return {
        "updated_at": updated_at,
        "terminal_reconciliation": {
            "direct_event_handoff": True,
            "input_equals_terminal": True,
            "input_rows": 1,
            "terminal_rows": 1,
        },
        "attempt_terminals": [
            {
                "wallet": "0x" + "1" * 40,
                "attempt_id": "attempt-1",
                "recorded_at": updated_at,
            }
        ],
        "orders": [],
    }


def test_direct_snapshot_reread_uses_fresher_packet_at_consumption_time() -> None:
    now = dt.datetime(2026, 8, 1, 10, 0, tzinfo=dt.timezone.utc)
    original = _direct_packet("2026-08-01T09:59:20+00:00")
    reread = _direct_packet("2026-08-01T09:59:58+00:00")

    packet, _evidence, accepted = deadman._reread_direct_snapshot_packet(
        original,
        load_packet=lambda: reread,
        fingerprint_evidence={},
        lane={},
        manifest={},
        forward_evidence={},
    )
    snapshot = deadman._wide_direct_source_snapshot(packet, now=now)

    assert accepted is True
    assert snapshot["ready"] is True
    assert snapshot["status"] == "PASS"
    assert snapshot["packet_age_s"] == 2.0


def test_direct_snapshot_reread_failure_retains_original_packet() -> None:
    original = _direct_packet("2026-08-01T09:59:20+00:00")

    def unavailable():
        raise OSError("read failed")

    packet, _evidence, accepted = deadman._reread_direct_snapshot_packet(
        original,
        load_packet=unavailable,
        fingerprint_evidence={},
        lane={},
        manifest={},
        forward_evidence={},
    )

    assert accepted is False
    assert packet == original


def test_direct_snapshot_reread_rejects_older_packet() -> None:
    original = _direct_packet("2026-08-01T09:59:20+00:00")
    older = _direct_packet("2026-08-01T09:59:19+00:00")

    packet, _evidence, accepted = deadman._reread_direct_snapshot_packet(
        original,
        load_packet=lambda: older,
        fingerprint_evidence={},
        lane={},
        manifest={},
        forward_evidence={},
    )

    assert accepted is False
    assert packet == original


def test_gap_closing_coverage_sticky_binds_owned_focus_identity(monkeypatch) -> None:
    wallet, fingerprint = ORDER128_TEST_IDENTITY
    monkeypatch.setattr(
        deadman, "STICKY_PAPER_ACCRUAL_FOCUS", ((wallet, fingerprint),)
    )
    coverage = deadman._gap_closing_coverage(
        {
            "nearest_frontier": [{"wallet": "0x" + "1" * 40}],
            "rows": [
                {
                    "wallet": wallet,
                    "wide_policy_fingerprint": fingerprint,
                    "checks": {"own_evidenced_policy_available": True},
                }
            ],
        }
    )

    assert wallet in coverage


def test_brain_policy_choke_caps_candidate_and_direct_source_rows() -> None:
    payload = {
        "status": "INCIDENT_SOURCE_ROSTER_DROUGHT",
        "actuator": {
            "candidate_evidence": {
                "candidate_count": 110,
                "rows": [{"wallet": f"0x{i:040x}"} for i in range(110)],
                "nearest_frontier": [
                    {"wallet": f"0x{i:040x}", "evidence_deficits": ["f2"]}
                    for i in range(10)
                ],
            }
        },
        "source_roster_drought": {
            "direct_source": {"per_wallet": {str(i): {"attempt_ids": list(range(20))} for i in range(110)}},
            "candidate_evidence": {"candidate_count": 110, "rows": [{"wallet": "x"}] * 110},
        },
    }
    compact = deadman._brain_policy_choke(payload)
    encoded = json.dumps(compact)
    assert '"rows"' not in encoded
    assert "per_wallet" not in encoded
    assert len(compact["actuator"]["candidate_evidence"]["nearest_top3"]) == 3
    assert len(encoded) < 10_000


def test_qualified_pool_stakeout_unions_identity_clean_buy_once() -> None:
    wallet = "0x805a8bbd411324a1c121dd87fac96c2fb13012c8"
    event = {
        "event_id": "0xtx|7",
        "source_wallet": wallet,
        "action": "BUY",
        "paper_only": True,
        "market_slug": "btc-updown-5m-1800000000",
        "observed_ts": 1_800_000_010.0,
    }
    union = deadman._union_qualified_pool_stakeout(
        {"events": [dict(event)]},
        {
            "prospective_current_market": {
                "actuator_consumption_gate": {"passed": True},
                "identity_clean_events": [dict(event)],
            }
        },
    )

    assert union["events"] == [event]
    assert union["qualified_pool_orderfilled_union"]["inserted_events"] == 0


def test_qualified_pool_stakeout_rejects_nonpaper_event() -> None:
    union = deadman._union_qualified_pool_stakeout(
        {"events": []},
        {
            "prospective_current_market": {
                "identity_clean_events": [
                    {
                        "event_id": "0xtx|8",
                        "source_wallet": "0x805a8bbd411324a1c121dd87fac96c2fb13012c8",
                        "action": "BUY",
                        "paper_only": False,
                    }
                ]
            }
        },
    )

    assert union["events"] == []


def test_qualified_pool_stakeout_stays_paper_only_before_gate() -> None:
    union = deadman._union_qualified_pool_stakeout(
        {"events": []},
        {
            "prospective_current_market": {
                "actuator_consumption_gate": {"passed": False},
                "identity_clean_events": [
                    {
                        "event_id": "0xtx|9",
                        "source_wallet": "0x805a8bbd411324a1c121dd87fac96c2fb13012c8",
                        "action": "BUY",
                        "paper_only": True,
                    }
                ],
            }
        },
    )

    assert union["events"] == []
    assert union["qualified_pool_orderfilled_union"][
        "actuator_consumption_gate_passed"
    ] is False


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _recovery_inputs(*, eligible: bool = False) -> tuple[list[dict], list[dict]]:
    preregs, states = [], []
    for index in range(2):
        body = {"cell": f"cell-{index}", "registered_at": "2026-07-07T21:00:00+00:00"}
        prereg = {**body, "checksum": deadman._stable_checksum(body)}
        row = {
            "tuple_id": f"tuple-{index}", "wallet": "0x" + str(index + 1) * 40,
            "policy_id": f"policy-{index}", "policy": {"policy_id": f"policy-{index}", "max_order_usd": 1.0},
            "resolved": 200 if eligible else 100, "post_fee_pnl_usd": 5.0 + index,
            "first_half_pnl_usd": 2.0, "second_half_pnl_usd": 3.0, "eligible": eligible,
        }
        states.append({"preregistration_checksum": prereg["checksum"], "observation_deadline_at": "2026-07-07T21:30:00+00:00", "tuple_evidence": [row]})
        preregs.append(prereg)
    return states, preregs


def test_recovery_ttl_refuses_predeadline_and_checksum_mismatch() -> None:
    states, preregs = _recovery_inputs()
    before = deadman._recovery_ttl_candidate(states=states, preregs=preregs, now=dt.datetime.fromisoformat("2026-07-07T21:29:59+00:00"))
    assert before["status"] == "RECOVERY_TTL_ACCRUING"
    states[0]["preregistration_checksum"] = "tampered"
    mismatch = deadman._recovery_ttl_candidate(states=states, preregs=preregs, now=dt.datetime.fromisoformat("2026-07-07T22:00:00+00:00"))
    assert mismatch["status"] == "REFUSED_RECOVERY_CHECKSUM_MISMATCH"


def test_recovery_ttl_never_pools_and_deterministically_ranks_one_pin() -> None:
    states, preregs = _recovery_inputs(eligible=True)
    due = deadman._recovery_ttl_candidate(states=states, preregs=preregs, now=dt.datetime.fromisoformat("2026-07-07T22:00:00+00:00"))
    assert due["eligible_tuple_count"] == 2
    assert due["candidate"]["paper_policy_id"] == "policy-1"
    overlay, report = deadman._execute_policy_choke_rung_b(overlay={"members": []}, candidate=due["candidate"], now=dt.datetime.fromisoformat("2026-07-07T22:00:00+00:00"), supply_rung="RECOVERY")
    assert report["status"] == "RECOVERY_SELECTION_PIN_WRITTEN"
    assert len(overlay["members"]) == 1


def test_recovery_ttl_no_all_pass_is_immutable_and_idempotent() -> None:
    states, preregs = _recovery_inputs(eligible=False)
    due = deadman._recovery_ttl_candidate(states=states, preregs=preregs, now=dt.datetime.fromisoformat("2026-07-07T22:00:00+00:00"))
    assert due["status"] == "TTL_NO_ALL_PASS_METHOD_SWITCH"
    assert due["tuple_count"] == 2
    replay = deadman._recovery_ttl_candidate(states=states, preregs=preregs, now=dt.datetime.fromisoformat("2026-07-07T22:01:00+00:00"), existing_decision=due)
    assert replay["status"] == "RECOVERY_TTL_DECISION_ALREADY_RECORDED"


def _run_deadman(tmp_path: Path, *extra: str) -> dict:
    env = dict(os.environ, POLYMARKET_DEADMAN_NOTIFY="0")
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--root",
            str(tmp_path),
            "--ledger",
            "ledger.json",
            "--guard",
            "guard.json",
            "--event-log",
            "events.jsonl",
            "--state",
            "deadman.json",
            "--handoff",
            "HANDOFF.md",
            "--max-idle-s",
            "1800",
            "--now",
            "2026-07-07T22:00:00+00:00",
            *extra,
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert completed.stdout.strip()
    state = json.loads((tmp_path / "deadman.json").read_text(encoding="utf-8"))
    state["_flow_line"] = json.loads(completed.stdout.strip().splitlines()[-1])
    return state


def test_concurrent_deadman_skips_and_counts_without_shared_writes(
    tmp_path: Path, capsys
) -> None:
    lock_path = tmp_path / "data/research/order_flow_deadman.lock"
    lock_path.parent.mkdir(parents=True)
    with lock_path.open("a+") as held:
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert deadman.main(["--root", str(tmp_path)]) == 0

    event = json.loads(capsys.readouterr().out.strip())
    assert event["status"] == "SKIPPED_LOCK_HELD"
    rows = (tmp_path / deadman.DEFAULT_LOCK_SKIP_LOG).read_text().splitlines()
    assert len(rows) == 1
    assert json.loads(rows[0])["status"] == "SKIPPED_LOCK_HELD"
    assert not (tmp_path / deadman.DEFAULT_STATE).exists()


def _write_hour_bench_state(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "data/research/wallet_copy_active_set_auto_degrade_state.json",
        {
            "members": [
                {
                    "candidate_id": "runtime_auto_degrade_e6db20932f",
                    "source_wallet": "0xe6db20932faf0f9780acf75d95c74c9984407dac",
                    "status": "HOUR_BENCHED_UNTIL_1300Z_SEAT_PRESERVED_FABLE_20260714T0751Z",
                    "hour_band_bench": {
                        "auto_return_at": "2026-07-14T13:00:00+00:00",
                        "classification": "US_HOURS_SEAT_PRESERVED_MORNING_DISPROVEN",
                        "fable_direction_id": "2026-07-14T07:51Z-fable-schedule-by-proven-hours",
                        "morning_min_size_probe_usd": 2.0,
                        "seat_preserved": True,
                        "trigger": "e6db morning-session rolling PnL <= -12 before 12:00Z",
                        "trigger_pnl_usd": -13.760096,
                        "next_action": "auto-return at 13:00Z",
                    },
                }
            ]
        },
    )


def test_can_trade_reason_names_guard_summary_null_fallback(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "guard.json",
        {"execute_live": True, "live_orders_allowed": True, "blockers": []},
    )
    _write_json(tmp_path / "ledger.json", {"summary": {"can_trade": False}})

    state = _run_deadman(tmp_path)

    assert state["can_trade"] is True
    assert state["can_trade_reason"] == "guard_top_level_live_no_blockers"
    assert state["can_trade_evidence"]["guard_summary_present"] is False
    assert state["can_trade_evidence"]["ledger_summary_can_trade"] is False


def test_can_trade_reason_names_ledger_false_without_live_fallback(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": False},
            "execute_live": True,
            "live_orders_allowed": False,
            "blockers": [],
        },
    )
    _write_json(tmp_path / "ledger.json", {"summary": {"can_trade": False}})

    state = _run_deadman(tmp_path)

    assert state["can_trade"] is False
    assert state["can_trade_reason"] == "ledger_summary_false"


def test_can_trade_reason_names_guard_blockers(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "guard.json",
        {
            "execute_live": True,
            "live_orders_allowed": True,
            "blockers": ["operator_approval_missing"],
        },
    )
    _write_json(tmp_path / "ledger.json", {})

    state = _run_deadman(tmp_path)

    assert state["can_trade"] is False
    assert state["can_trade_reason"] == "guard_blockers_present"
    assert state["can_trade_evidence"]["guard_blockers"] == ["operator_approval_missing"]


def _armed_selected_guard(wallet: str, *, status: str = "LIVE_GUARD_RUNNING") -> dict:
    candidate_id = "candidate-current"
    policy_id = "policy-current"
    return {
        "status": status,
        "execute_live": True,
        "live_orders_allowed": True,
        "source_wallet": wallet,
        "candidate_id": candidate_id,
        "policy_id": policy_id,
        "summary": {"can_trade": True},
        "live_execution": {"status": "LIVE_ARMED_NO_FRESH_INTENTS"},
        "runtime_member_submittability": {
            "status": "PASS",
            "source_wallet": wallet,
            "candidate_id": candidate_id,
            "policy_id": policy_id,
            "generated_at": "2026-07-07T21:50:00+00:00",
        },
        "active_set_runtime": {
            "selected_member": {
                "source_wallet": wallet,
                "candidate_id": candidate_id,
                "policy_id": policy_id,
            },
            "members": [
                {
                    "source_wallet": wallet,
                    "candidate_id": candidate_id,
                    "policy_id": policy_id,
                }
            ],
        },
    }


def test_inherited_prior_seat_idle_gets_post_selection_floor(tmp_path: Path) -> None:
    prior_wallet = "0x" + "a" * 40
    current_wallet = "0x" + "b" * 40
    guard = _armed_selected_guard(current_wallet)
    guard["guard_code_identity"] = {
        "live_guard_generation_sha256": deadman.disk_generation()["sha256"]
    }
    _write_json(tmp_path / "guard.json", guard)
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [
                {
                    "status": "FILLED",
                    "submitted_at": "2026-07-07T21:20:00+00:00",
                    "source_wallet": prior_wallet,
                }
            ],
        },
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "WATCH_INHERITED_PRIOR_SEAT_IDLE"
    assert state["raw_accepted_order_deadman"]["global_firing"] is True
    assert state["raw_accepted_order_deadman"]["firing"] is False
    assert state["post_selection_liveness_floor"]["active"] is True
    assert state["mechanical_escalation"] == "SELECTION_ALREADY_ADOPTED"


def test_same_seat_accept_drought_stays_red_after_threshold() -> None:
    wallet = "0x" + "b" * 40
    guard = _armed_selected_guard(wallet)
    selected = deadman._selected_identity_projection(
        guard,
        {"selected_wallet": wallet, "selected_identity": deadman._selected_runtime_identity(guard)},
    )
    floor = deadman._post_selection_liveness_floor(
        guard=guard,
        selected=selected,
        previous={"selected_seat_epoch_at": "2026-07-07T20:00:00+00:00"},
        latest_accepted={
            "timestamp": dt.datetime.fromisoformat("2026-07-07T20:30:00+00:00"),
            "source_wallet": wallet,
        },
        now=dt.datetime.fromisoformat("2026-07-07T22:00:00+00:00"),
        can_trade=True,
        max_idle_s=1800.0,
        generation_mismatch=False,
    )
    assert floor["inherited_prior_seat_idle"] is False
    assert floor["active"] is False


def test_dead_or_generation_mismatched_guard_never_gets_selection_floor() -> None:
    prior_wallet = "0x" + "a" * 40
    current_wallet = "0x" + "b" * 40
    guard = _armed_selected_guard(current_wallet, status="LIVE_GUARD_BLOCKED")
    selected = deadman._selected_identity_projection(
        guard,
        {"selected_wallet": current_wallet, "selected_identity": deadman._selected_runtime_identity(guard)},
    )
    common = {
        "guard": guard,
        "selected": selected,
        "previous": {},
        "latest_accepted": {
            "timestamp": dt.datetime.fromisoformat("2026-07-07T21:20:00+00:00"),
            "source_wallet": prior_wallet,
        },
        "now": dt.datetime.fromisoformat("2026-07-07T22:00:00+00:00"),
        "max_idle_s": 1800.0,
    }
    assert deadman._post_selection_liveness_floor(
        **common, can_trade=True, generation_mismatch=False
    )["active"] is False
    guard["status"] = "LIVE_GUARD_RUNNING"
    assert deadman._post_selection_liveness_floor(
        **common, can_trade=True, generation_mismatch=True
    )["active"] is False


def test_empty_seat_policy_choke_cannot_emit_dead_order_notify() -> None:
    assert (
        deadman._incident_notify_allowed(
            status="INCIDENT_POLICY_CHOKE",
            can_trade=False,
        )
        is False
    )
    assert (
        deadman._incident_notify_allowed(
            status="INCIDENT_POLICY_CHOKE",
            can_trade=True,
        )
        is True
    )
    assert (
        deadman._incident_notify_allowed(
            status="INCIDENT_ORDER_FLOW_DEAD",
            can_trade=False,
        )
        is True
    )


def test_episode_closeout_appends_exactly_once_per_fire_to_clear(tmp_path: Path) -> None:
    first_fire = {
        "status": "INCIDENT_POLICY_CHOKE",
        "checked_at": "2026-07-29T19:52:43.084216+00:00",
        "idle_s": 1902.798095,
        "deadman_class": "POLICY_CHOKE",
        "mechanical_escalation": "MANAGED_RESTART_BYTE_IDENTICAL",
        "policy_choke": {
            "wallet_policy_diagnostic": "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
        },
    }
    first_fire.update(deadman._episode_fire_fields({}, first_fire))
    repeated_fire = {
        **first_fire,
        "checked_at": "2026-07-29T19:54:43.084216+00:00",
        "idle_s": 2022.798095,
        "consecutive_incidents": 2,
    }
    repeated_fire.update(deadman._episode_fire_fields(first_fire, repeated_fire))
    clear = {
        "status": "OK",
        "checked_at": "2026-07-29T19:55:56.547980+00:00",
        "liveness_ts": "2026-07-29T19:55:56.547980+00:00",
        "liveness_source": "accepted_order",
        "idle_s": 91.889084,
    }

    row = deadman._append_episode_closeout(tmp_path, repeated_fire, clear)
    duplicate = deadman._append_episode_closeout(tmp_path, repeated_fire, clear)

    assert row is not None
    assert duplicate is None
    assert row["episode_fire_at"] == first_fire["checked_at"]
    assert row["episode_duration_s"] == 193.463764
    assert row["restart_performed"] is False
    assert row["restart_provenance"] == "CARRIED_EPISODE_FIELD"
    assert row["restart_events"] == []
    assert row["clear_class"] == "RECOVERED"
    assert row["recovered"] is True
    episode_path = tmp_path / "data/research/order_flow_deadman_episodes.jsonl"
    episode_rows = deadman.load_incident_rows(episode_path)
    assert episode_rows == [row]
    manifest = json.loads(
        (tmp_path / "data/research/order_flow_deadman_episodes_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["ledger_scope"]["first_armed_commit"] == "09176b85"
    assert manifest["ledger_scope"]["first_armed_at"] == clear["checked_at"]
    assert manifest["ledger_scope"]["prior_episodes_unrecorded"] is True
    assert manifest["ledger_scope"]["prior_incident_rows_at_arming"] == 12
    assert manifest["ledger_scope"]["prior_recorded_clears"] == 0


def test_episode_closeout_marks_identical_liveness_as_reclassified_not_recovered() -> None:
    fire = {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "checked_at": "2026-07-30T08:00:00Z",
        "episode_fire_at": "2026-07-30T08:00:00Z",
        "episode_fire_liveness_ts": "2026-07-30T07:11:09Z",
        "consecutive_incidents": 1,
    }
    clear = {
        "status": "OK",
        "checked_at": "2026-08-01T00:14:40Z",
        "liveness_ts": "2026-07-30T07:11:09Z",
    }
    row = deadman._episode_closeout_row(fire, clear)
    assert row is not None
    assert row["clear_class"] == "RECLASSIFIED_NOT_RECOVERED"
    assert row["recovered"] is False


def test_utc_day_money_drives_additive_floor_inputs() -> None:
    ledger = {
        "orders": [
            {
                "submitted_at": "2026-08-01T00:01:00Z",
                "response_filled_size_usd": 2.5,
                "alternate_transport_attribution": {
                    "resolution_status": "RESOLVED",
                    "resolved_post_fee_pnl_usd": 0.75,
                },
            },
            {"submitted_at": "2026-07-31T23:59:00Z", "response_filled_size_usd": 9.0},
        ]
    }
    measured = deadman._utc_day_money(
        ledger, deadman.datetime(2026, 8, 1, 1, tzinfo=deadman.timezone.utc)
    )
    assert measured == {
        "utc_day": "2026-08-01",
        "realized_pnl_usd": 0.75,
        "filled_size_usd": 2.5,
    }
    assert deadman._money_anchored_status(
        accepted_order_idle_s=21600.1,
        money={"realized_pnl_usd": 0.0, "filled_size_usd": 0.0},
    ) == "FLOW_DEAD_MONEY_ANCHORED"
    assert deadman._money_anchored_status(
        accepted_order_idle_s=21600.1,
        money=measured,
    ) == "OK"


def test_episode_closeout_reports_guard_memory_restart_inside_episode(tmp_path: Path) -> None:
    fire = {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "checked_at": "2026-07-29T19:52:43+00:00",
        "idle_s": 1902.0,
        "deadman_class": "ORDER_FLOW_DEAD",
        "consecutive_incidents": 1,
    }
    fire.update(deadman._episode_fire_fields({}, fire))
    restart_check = {
        **fire,
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "checked_at": "2026-07-29T19:54:43+00:00",
        "idle_s": 2022.0,
        "consecutive_incidents": 2,
        "guard_memory": {
            "checked_at": "2026-07-29T19:54:43+00:00",
            "rss_gib": 6.2,
            "auto_restart": {"status": "RESTART_EXECUTED"},
        },
    }
    restart_check.update(deadman._episode_fire_fields(fire, restart_check))

    row = deadman._append_episode_closeout(
        tmp_path,
        restart_check,
        {
            "status": "OK",
            "checked_at": "2026-07-29T19:55:56+00:00",
            "liveness_ts": "2026-07-29T19:55:56+00:00",
            "liveness_source": "accepted_order",
            "idle_s": 1.0,
        },
    )

    assert row is not None
    assert row["restart_performed"] is True
    assert row["restart_provenance"] == "CARRIED_EPISODE_FIELD"
    assert row["restart_events"] == [
        {
            "at": "2026-07-29T19:54:43+00:00",
            "source": "guard_memory_auto_restart",
            "status": "RESTART_EXECUTED",
            "guard_memory_auto_restart_status": "RESTART_EXECUTED",
            "rss_gib": 6.2,
        }
    ]


def test_episode_closeout_uses_actuator_ledger_and_loaded_generation(
    tmp_path: Path,
) -> None:
    restart_path = (
        tmp_path / "data/research/brainless_live_guard_restart_events.jsonl"
    )
    restart_path.parent.mkdir(parents=True)
    restart_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-07-29T20:05:00Z",
                "status": "RESTART_EXECUTED",
                "reason": "guard_unresponsive",
                "execution": {"actual_pid": 202},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    prev = {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "checked_at": "2026-07-29T20:00:00Z",
        "episode_fire_at": "2026-07-29T20:00:00Z",
        "episode_fire_status": "INCIDENT_ORDER_FLOW_DEAD",
        "episode_fire_guard_generation_started_at": "2026-07-29T12:00:00Z",
        "episode_restart_performed": None,
        "consecutive_incidents": 2,
    }
    clear = {
        "status": "OK",
        "checked_at": "2026-07-29T20:10:00Z",
        "guard_loaded_generation_started_at": "2026-07-29T20:05:02Z",
    }

    row = deadman._append_episode_closeout(tmp_path, prev, clear)

    assert row is not None
    assert row["restart_performed"] is True
    assert (
        row["restart_provenance"]
        == "BRAINLESS_ACTUATOR_EVENT_LEDGER+GUARD_LOADED_GENERATION"
    )
    assert {event["source"] for event in row["restart_events"]} == {
        "brainless_actuator",
        "guard_loaded_generation",
    }
    assert row["restart_events"][0]["started_pid"] == 202


def test_episode_closeout_proves_dual_source_restart_absence(tmp_path: Path) -> None:
    restart_path = (
        tmp_path / "data/research/brainless_live_guard_restart_events.jsonl"
    )
    restart_path.parent.mkdir(parents=True)
    restart_path.write_text(
        "\n".join(
            json.dumps(
                {
                    "generated_at": at,
                    "status": "RESTART_EXECUTED",
                    "execution": {"actual_pid": index},
                }
            )
            for index, at in enumerate(
                (
                    "2026-07-29T00:00:04.343018Z",
                    "2026-07-29T02:29:38.821581Z",
                    "2026-07-29T12:18:01.621689Z",
                ),
                start=1,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    generation_started_at = "2026-07-29T12:18:04.593231Z"
    prev = {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "checked_at": "2026-07-29T20:30:32.707861Z",
        "episode_fire_at": "2026-07-29T20:30:32.707861Z",
        "episode_fire_status": "INCIDENT_ORDER_FLOW_DEAD",
        "episode_fire_guard_generation_started_at": generation_started_at,
        "episode_restart_performed": False,
        "consecutive_incidents": 7,
    }
    clear = {
        "status": "OK",
        "checked_at": "2026-07-29T21:18:11.812248Z",
        "guard_loaded_generation_started_at": generation_started_at,
    }

    row = deadman._append_episode_closeout(tmp_path, prev, clear)

    assert row is not None
    assert row["restart_performed"] is False
    assert row["restart_provenance"] == "DUAL_SOURCE_ABSENCE_PROVEN"
    assert row["restart_events"] == []
    assert row["restart_sources_checked"] == [
        "brainless_actuator",
        "guard_loaded_generation",
    ]


def test_episode_migration_preserves_previous_fire_and_unknown_restart_provenance() -> None:
    previous_pre_field_fire = {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "checked_at": "2026-07-29T20:00:00+00:00",
        "idle_s": 1801.0,
        "deadman_class": "ORDER_FLOW_DEAD",
        "consecutive_incidents": 1,
    }
    repeated = {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "checked_at": "2026-07-29T20:10:00+00:00",
        "idle_s": 2401.0,
    }

    fields = deadman._episode_fire_fields(previous_pre_field_fire, repeated)

    assert fields["episode_fire_at"] == previous_pre_field_fire["checked_at"]
    assert fields["episode_fire_idle_s"] == previous_pre_field_fire["idle_s"]
    assert fields["episode_restart_performed"] is None

    second_hop = {**repeated, **fields, "consecutive_incidents": 2}
    second_fields = deadman._episode_fire_fields(second_hop, repeated)
    closeout = deadman._episode_closeout_row(
        {**second_hop, **second_fields},
        {
            "status": "OK",
            "checked_at": "2026-07-29T20:20:00+00:00",
            "idle_s": 1.0,
        },
    )

    assert second_fields["episode_restart_performed"] is None
    assert closeout is not None
    assert closeout["restart_performed"] is None
    assert closeout["restart_provenance"] == "UNKNOWN_PRE_FIELD_EPISODE"


def test_episode_closeout_does_not_change_incident_journal_count_or_status(tmp_path: Path) -> None:
    incident_path = tmp_path / "data/research/order_flow_deadman_incidents.jsonl"
    original_incident = {
        "incident_id": "fire-1",
        "status": "INCIDENT_POLICY_CHOKE",
    }
    deadman.append_incident_row(incident_path, original_incident)
    before_rows = deadman.load_incident_rows(incident_path)
    prev = {
        "status": "INCIDENT_POLICY_CHOKE",
        "checked_at": "2026-07-29T19:52:43.084216+00:00",
        "idle_s": 1902.798095,
        "deadman_class": "POLICY_CHOKE",
        "consecutive_incidents": 1,
        "mechanical_escalation": "MANAGED_RESTART_BYTE_IDENTICAL",
        "policy_choke": {
            "wallet_policy_diagnostic": "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
        },
    }
    clear = {
        "status": "OK",
        "checked_at": "2026-07-29T19:55:56.547980+00:00",
        "liveness_ts": "2026-07-29T19:55:56.547980+00:00",
        "liveness_source": "accepted_order",
        "idle_s": 91.889084,
    }

    row = deadman._append_episode_closeout(tmp_path, prev, clear)

    assert row is not None
    assert row["restart_performed"] is None
    assert row["restart_provenance"] == "UNKNOWN_PRE_FIELD_EPISODE"
    after_rows = deadman.load_incident_rows(incident_path)
    assert len(after_rows) == len(before_rows)
    assert after_rows == before_rows
    assert {row["status"] for row in after_rows} == {"INCIDENT_POLICY_CHOKE"}


def test_episode_closeout_leaves_wide_incident_envelopes_identical(tmp_path: Path) -> None:
    incident_path = tmp_path / "data/research/order_flow_deadman_incidents.jsonl"
    incident = {
        "status": "INCIDENT_POLICY_CHOKE",
        "policy_choke": {
            "source_roster_drought": {
                "direct_source": {
                    "identity": {"run_id": "wide_1", "wallets": ["0xabc"]},
                    "input_rows": 1,
                    "input_equals_terminal": True,
                    "updated_at": "2026-07-29T19:52:43+00:00",
                }
            }
        },
    }
    terminal_rows = [
        {
            "run_id": "wide_1",
            "order_id": "order-1",
            "recorded_at": "2026-07-29T19:52:42+00:00",
        }
    ]
    deadman.append_incident_row(incident_path, incident)
    incidents_before = deadman.load_incident_rows(incident_path)
    envelopes_before = deadman.envelopes_from_incidents(incidents_before, terminal_rows)
    assert len(envelopes_before) == 1

    deadman._append_episode_closeout(
        tmp_path,
        {
            **incident,
            "checked_at": "2026-07-29T19:52:43+00:00",
            "idle_s": 1900.0,
            "consecutive_incidents": 1,
        },
        {
            "status": "OK",
            "checked_at": "2026-07-29T19:55:56+00:00",
            "liveness_ts": "2026-07-29T19:55:56+00:00",
            "liveness_source": "accepted_order",
            "idle_s": 1.0,
        },
    )

    incidents_after = deadman.load_incident_rows(incident_path)
    assert deadman.envelopes_from_incidents(incidents_after, terminal_rows) == envelopes_before


def test_policy_choke_497_suppressed_fires_and_produces_rung_a_seat_read() -> None:
    incumbent = "0x" + "a" * 40
    alternate = "0x" + "b" * 40
    rung_a = {
        "target_wallet": alternate,
        "action": "RUNG_A_RESELECT",
        "rows": [
            {"wallet": incumbent, "policy_accepted_intents": 0},
            {"wallet": alternate, "policy_accepted_intents": 5},
        ],
    }

    result = deadman._policy_choke_incident(
        scan={
            "member_signal_age": {
                incumbent: {
                    "suppressed_intents": 497,
                    "eligible_intents": 100,
                    "accepted_orders": 0,
                    "suppression_taxonomy": {"price_outside_policy": 497},
                }
            }
        },
        selected_wallet=incumbent,
        can_trade=True,
        rung_a=rung_a,
    )

    assert result["status"] == "INCIDENT_POLICY_CHOKE"
    assert result["selected_eligible_intents"] == 100
    assert result["selected_accepted_orders"] == 0
    assert result["trigger_scope"] == "selected_member"
    assert result["mechanical_escalation"] == "RUNG_A_RESELECT"
    assert result["rung_a_seat_read"]["target_wallet"] == alternate


def test_policy_choke_classifies_fak_no_match_submits_as_liquidity_drought() -> None:
    incumbent = "0x" + "a" * 40
    routed_wallet = "0x" + "b" * 40
    result = deadman._policy_choke_incident(
        scan={
            "member_signal_age": {
                incumbent: {
                    "fresh_source_rows": 179,
                    "eligible_intents": 32,
                    "accepted_orders": 0,
                }
            }
        },
        selected_wallet=incumbent,
        can_trade=True,
        rung_a={"target_wallet": None, "action": "NO_RUNG_A_TARGET"},
        submit_outcomes={
            "guard_submit_attempts": 2,
            "fak_no_match_outcomes": 2,
            "per_wallet": {
                incumbent: {"guard_submit_attempts": 0, "fak_no_match_outcomes": 0},
                routed_wallet: {"guard_submit_attempts": 2, "fak_no_match_outcomes": 2},
            },
        },
    )

    assert result["status"] == "LIQUIDITY_DROUGHT"
    assert result["raw_policy_choke_trigger"] is True
    assert result["liquidity_drought"] is True
    assert result["firing"] is False
    assert result["mechanical_escalation"] == "NONE"
    assert result["guard_submit_attempts"] == 2
    assert result["fak_no_match_outcomes"] == 2
    assert result["selected_guard_submit_attempts"] == 0
    assert result["selected_fak_no_match_outcomes"] == 0


def test_fresh_selected_demand_after_order_classifies_guard_side_halt(
    tmp_path: Path,
) -> None:
    incumbent = "0x" + "a" * 40
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [
                {
                    "status": "LIVE_SUBMITTED",
                    "updated_at": "2026-07-07T21:00:00+00:00",
                }
            ],
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "active_set_runtime": {
                "selected_member": {"source_wallet": incumbent},
                "members": [{"source_wallet": incumbent}],
            },
            "drought_funnel": {
                "active_set_fresh_signal_rows": 60,
                "reject_taxonomy_counts": {"fak_no_match": 5},
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "generated_at": "2026-07-07T21:59:00+00:00",
                "source_wallet": incumbent,
                "fresh_source_rows": 60,
                "eligible_intents": 12,
                "accepted_orders": 0,
                "guard_submit_attempts": 5,
                "fak_no_match_outcomes": 5,
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_GUARD_SIDE_HALT"
    assert state["deadman_class"] == "GUARD_SIDE_HALT"
    assert state["accepted_order_idle_s"] == 3600.0
    assert state["raw_accepted_order_deadman"]["firing"] is True
    assert state["mechanical_escalation"] == "NO_LAWFUL_ACTUATION"


def test_policy_choke_global_flow_clears_on_accepted_e5_while_wallet_diagnostic_remains() -> None:
    incumbent = "0x" + "a" * 40
    result = deadman._policy_choke_incident(
        scan={
            "member_signal_age": {
                incumbent: {
                    "fresh_source_rows": 60,
                    "eligible_intents": 0,
                    "accepted_orders": 0,
                }
            }
        },
        selected_wallet=incumbent,
        can_trade=True,
        rung_a={"target_wallet": None},
        method_acceptance={
            "accepted_orders": 1,
            "lane_counts": {"e5_maker_first_btc5m_v1": 1},
        },
    )

    assert result["status"] == "ORDER_FLOW_CLEAR_METHOD_ACCEPTED"
    assert result["firing"] is False
    assert result["order_flow_clear_method_accepted"] is True
    assert result["method_accepted_orders"] == 1
    assert result["whole_runtime_accepted_orders"] == 0
    assert result["wallet_policy_diagnostic"] == "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"


def test_effective_lanes_include_e5_only_for_fresh_enabled_actuator() -> None:
    now = dt.datetime(2026, 7, 24, 4, 0, tzinfo=dt.timezone.utc)
    assert deadman._effective_execution_lanes(
        {"generated_at": "2026-07-24T03:59:00Z", "status": "DISABLED"},
        now=now,
    ) == {"live_guard"}
    assert deadman._effective_execution_lanes(
        {"generated_at": "2026-07-24T03:59:00Z", "status": "PASS"},
        now=now,
    ) == {"live_guard", "e5_maker_first_btc5m_v1"}
    assert deadman._effective_execution_lanes(
        {"generated_at": "2026-07-24T03:00:00Z", "status": "PASS"},
        now=now,
    ) == {"live_guard"}


def test_accepted_method_orders_excludes_demoted_e5_but_counts_wallet_lane() -> None:
    since = dt.datetime(2026, 7, 24, 3, 30, tzinfo=dt.timezone.utc)
    until = dt.datetime(2026, 7, 24, 4, 0, tzinfo=dt.timezone.utc)
    ledger = {
        "orders": [
            {
                "order_id": "e5",
                "status": "FILLED",
                "submitted_at": "2026-07-24T03:40:00Z",
                "trade_decision": {"execution_lane": "e5_maker_first_btc5m_v1"},
            },
            {
                "order_id": "wallet",
                "status": "FILLED",
                "submitted_at": "2026-07-24T03:41:00Z",
                "trade_decision": {"execution_lane": "live_guard"},
            },
        ]
    }
    result = deadman._accepted_method_orders(
        ledger,
        since=since,
        until=until,
        effective_lanes={"live_guard"},
    )
    assert result["accepted_orders"] == 1
    assert result["lane_counts"] == {"live_guard": 1}
    assert result["excluded_demoted_rows"] == 1
    assert result["excluded_lane_counts"] == {"e5_maker_first_btc5m_v1": 1}


def test_submit_outcomes_exclude_demoted_lane_attempts() -> None:
    since = dt.datetime(2026, 7, 24, 3, 30, tzinfo=dt.timezone.utc)
    until = dt.datetime(2026, 7, 24, 4, 0, tzinfo=dt.timezone.utc)
    ledger = {
        "orders": [
            {
                "order_id": "e5",
                "source_wallet": "e5_maker_first",
                "submitted_at": "2026-07-24T03:40:00Z",
                "trade_decision": {"execution_lane": "e5_maker_first_btc5m_v1"},
                "lifecycle": [{"status": "LIVE_SUBMITTED"}],
            },
            {
                "order_id": "wallet",
                "source_wallet": "0xabc",
                "submitted_at": "2026-07-24T03:41:00Z",
                "trade_decision": {"execution_lane": "live_guard"},
                "lifecycle": [{"status": "LIVE_SUBMITTED"}],
            },
        ]
    }
    result = deadman._policy_choke_submit_outcomes(
        ledger,
        since=since,
        until=until,
        effective_lanes={"live_guard"},
    )
    assert result["guard_submit_attempts"] == 1
    assert result["excluded_lane_counts"] == {"e5_maker_first_btc5m_v1": 1}


def test_policy_choke_true_zero_accepted_rows_still_fires() -> None:
    incumbent = "0x" + "a" * 40
    result = deadman._policy_choke_incident(
        scan={
            "member_signal_age": {
                incumbent: {
                    "fresh_source_rows": 60,
                    "eligible_intents": 0,
                    "accepted_orders": 0,
                }
            }
        },
        selected_wallet=incumbent,
        can_trade=True,
        rung_a={"target_wallet": None},
        method_acceptance={"accepted_orders": 0, "lane_counts": {}},
    )

    assert result["status"] == "INCIDENT_POLICY_CHOKE"
    assert result["firing"] is True
    assert result["order_flow_clear_method_accepted"] is False


def test_unattributed_selected_supply_refuses_measured_source_quiet() -> None:
    policy_choke = {
        "status": "CLEAR",
        "firing": False,
        "can_trade": True,
        "selected_fresh_source_rows": 7,
        "selected_eligible_intents": 0,
        "selected_accepted_orders": 0,
        "method_accepted_orders": 0,
        "selected_suppression_taxonomy": {},
        "mechanical_escalation": "NONE",
    }

    assert deadman._enforce_unattributed_selected_seat_attrition(policy_choke) is True
    assert policy_choke["status"] == "UNATTRIBUTED_SELECTED_SEAT_ATTRITION"
    assert policy_choke["firing"] is True
    assert policy_choke["mechanical_escalation"] == "NONE_ATTRIBUTION_REQUIRED"


def test_attributed_selected_supply_can_remain_measured_source_quiet() -> None:
    policy_choke = {
        "status": "CLEAR",
        "firing": False,
        "can_trade": True,
        "selected_fresh_source_rows": 7,
        "selected_eligible_intents": 0,
        "selected_accepted_orders": 0,
        "method_accepted_orders": 0,
        "selected_suppression_taxonomy": {"window_time_gte_60s": 7},
        "mechanical_escalation": "NONE",
    }

    assert deadman._enforce_unattributed_selected_seat_attrition(policy_choke) is False
    assert policy_choke["status"] == "CLEAR"
    assert policy_choke["firing"] is False


def test_four_unmatched_source_rows_without_identity_coverage_are_unmeasurable() -> None:
    wallet = "0x" + "a" * 40
    scan = deadman._policy_choke_scan_from_acceptance(
        [{
            "wallet": wallet,
            "raw_own_source_buy_rows": 4,
            "policy_eligible_intents": 0,
            "accepted_live_orders": 0,
            "unmatched_source_row_count": 4,
            "unmatched_source_row_ids": ["we-1", "we-2", "we-3", "we-4"],
        }],
        {"member_signal_age": {}, "source_row_attribution": {}},
        {},
    )
    incident = deadman._policy_choke_incident(
        scan=scan,
        selected_wallet=wallet,
        can_trade=True,
        rung_a={"target_wallet": None},
        method_acceptance={"accepted_orders": 0},
    )

    member = scan["member_signal_age"][wallet]
    assert member["suppression_taxonomy"] == {}
    assert member["attribution_status"] == "UNMEASURABLE_SOURCE_ROW_IDENTITY"
    assert incident["source_wiring_attrition"] is False


def test_attribution_requires_identity_coverage_and_full_sample() -> None:
    wallet = "0x" + "a" * 40
    base = {
        "wallet": wallet,
        "raw_own_source_buy_rows": 4,
        "policy_eligible_intents": 0,
        "accepted_live_orders": 0,
        "source_row_identity_coverage": 1.0,
        "unmatched_source_row_count": 4,
        "unmatched_source_row_ids": ["we-1", "we-2", "we-3", "we-4"],
    }
    now_s = 1_700_000_000.0
    guard_started_at = "2026-08-05T05:00:00Z"
    guard = {"guard_code_identity": {"pid": 1234, "started_at_utc": guard_started_at}}
    def intent_state(source_ids: list[str]) -> dict:
        return {
            "status": "COPY_INTENTS_SIDECAR_UPDATED",
            "generated_at": "2023-11-14T22:13:19Z",
            "writer_pid": 1234,
            "writer_guard_started_at": guard_started_at,
            "copy_intents": [{
                "source_row_event_id": source_id,
                "source_event_id": "lane-alias-must-not-be-read",
                "source_wallet": wallet,
                "observed_ts": now_s - 1,
            } for source_id in source_ids],
        }
    unrelated = deadman._policy_choke_scan_from_acceptance(
        [base], {"member_signal_age": {}, "source_row_attribution": {}},
        intent_state(["we-other"]), now_s=now_s, guard=guard,
    )["member_signal_age"][wallet]
    complete = deadman._policy_choke_scan_from_acceptance(
        [base], {"member_signal_age": {}, "source_row_attribution": {}},
        intent_state(base["unmatched_source_row_ids"]), now_s=now_s, guard=guard,
    )["member_signal_age"][wallet]
    partial = deadman._policy_choke_scan_from_acceptance(
        [{**base, "unmatched_source_row_count": 60}],
        {"member_signal_age": {}, "source_row_attribution": {}},
        intent_state(["we-other"]), now_s=now_s, guard=guard,
    )["member_signal_age"][wallet]
    epsilon = deadman._policy_choke_scan_from_acceptance(
        [{
            **base,
            "raw_own_source_buy_rows": 12,
            "source_row_identity_coverage": 1 / 5000,
            "source_row_identity_denominator": 5000,
            "source_row_identity_rows": 1,
            "unmatched_source_row_count": 12,
        }],
        {"member_signal_age": {}, "source_row_attribution": {}},
        {}, now_s=now_s, guard=guard,
    )
    epsilon_incident = deadman._policy_choke_incident(
        scan=epsilon,
        selected_wallet=wallet,
        can_trade=True,
        rung_a={"target_wallet": None},
        method_acceptance={"accepted_orders": 0},
    )
    no_intent_axis = deadman._policy_choke_scan_from_acceptance(
        [base], {"member_signal_age": {}, "source_row_attribution": {}}, {}, now_s=now_s, guard=guard
    )["member_signal_age"][wallet]
    adopted_empty_axis = deadman._policy_choke_scan_from_acceptance(
        [base],
        {"member_signal_age": {}, "source_row_attribution": {}},
        {
            "status": "NO_BUILT_COPY_INTENTS",
            "generated_at": "2023-11-14T22:13:19Z",
            "writer_pid": 1234,
            "writer_guard_started_at": guard_started_at,
            "copy_intents": [],
        },
        now_s=now_s,
        guard=guard,
    )["member_signal_age"][wallet]

    assert unrelated["attribution_status"] == "SOURCE_ROWS_NEVER_REACHED_INTENT_BUILDER"
    assert unrelated["intent_axis_coverage_residual_sampled"] == 0.0
    assert unrelated["suppression_taxonomy"] == {"NO_INTENT_RECORD_FOR_SOURCE_ROW": 4}
    assert complete["attribution_status"] == "ATTRIBUTED_SOURCE_ROW_ATTRITION"
    assert complete["intent_axis_coverage_residual_sampled"] == 1.0
    assert complete["suppression_taxonomy"] == {"INTENT_BUILT_NEVER_SUBMITTED": 4}
    assert complete["taxonomy_covers_rows"] == 4
    assert partial["attribution_status"] == "PARTIAL_SAMPLE_ATTRIBUTION"
    assert partial["taxonomy_covers_rows"] == 4
    assert partial["taxonomy_total_rows"] == 60
    assert epsilon["member_signal_age"][wallet]["attribution_status"] == "PARTIAL_SAMPLE_ATTRIBUTION"
    assert epsilon_incident["source_wiring_attrition"] is False
    assert no_intent_axis["suppression_taxonomy"] == {"INTENT_CHANNEL_UNPOPULATED": 4}
    assert no_intent_axis["attribution_status"] == "UNMEASURABLE_INTENT_RECORD_AXIS"
    assert no_intent_axis["intent_channel_status"] == "ABSENT"
    assert adopted_empty_axis["suppression_taxonomy"] == {"NO_INTENT_RECORD_FOR_SOURCE_ROW": 4}
    assert adopted_empty_axis["attribution_status"] == "SOURCE_ROWS_NEVER_REACHED_INTENT_BUILDER"
    assert adopted_empty_axis["intent_channel_status"] == "NO_BUILT_COPY_INTENTS"


def test_attribution_zero_unmatched_and_cross_axis_population_fail_safe() -> None:
    wallet = "0x" + "a" * 40
    now_s = 1_700_000_000.0
    guard_started_at = "2026-08-05T05:00:00Z"
    guard = {"guard_code_identity": {"pid": 1234, "started_at_utc": guard_started_at}}
    state = {
        "status": "COPY_INTENTS_SIDECAR_UPDATED",
        "generated_at": "2023-11-14T22:13:19Z",
        "writer_pid": 1234,
        "writer_guard_started_at": guard_started_at,
        "copy_intents": [{
            "source_row_event_id": "we-other",
            "source_wallet": wallet,
            "observed_ts": now_s - 1,
        }],
    }
    zero = deadman._policy_choke_scan_from_acceptance(
        [{"wallet": wallet, "source_row_identity_coverage": 1.0}],
        {"member_signal_age": {}, "source_row_attribution": {}},
        state,
        now_s=now_s,
        guard=guard,
    )["member_signal_age"][wallet]
    mixed = deadman._policy_choke_scan_from_acceptance(
        [{
            "wallet": wallet,
            "source_row_identity_coverage": 1.0,
            "unmatched_source_row_count": 10,
            "unmatched_source_row_ids": [
                "we-guard-1", "we-guard-2", "we-guard-3", "we-guard-4",
                "we-missing-1", "we-missing-2", "we-missing-3",
                "we-missing-4", "we-missing-5", "we-missing-6",
            ],
        }],
        {
            "member_signal_age": {},
            "source_row_attribution": {
                "we-guard-1": "GUARD_FILTER",
                "we-guard-2": "GUARD_FILTER",
                "we-guard-3": "GUARD_FILTER",
                "we-guard-4": "GUARD_FILTER",
            },
        },
        state,
        now_s=now_s,
        guard=guard,
    )["member_signal_age"][wallet]

    assert zero["attribution_status"] == "NO_UNMATCHED_SOURCE_ROWS"
    assert mixed["attribution_status"] == "SOURCE_ROWS_NEVER_REACHED_INTENT_BUILDER"
    assert mixed["guard_attributed_rows"] == 4
    assert mixed["intent_axis_residual_sampled_rows"] == 6
    assert mixed["suppression_taxonomy"] == {
        "GUARD_FILTER": 4,
        "NO_INTENT_RECORD_FOR_SOURCE_ROW": 6,
    }


def test_policy_choke_scan_uses_strict_rolling_30m(tmp_path: Path) -> None:
    event_log = tmp_path / "events.jsonl"
    _write_jsonl(
        event_log,
        [
            {"event": "wallet_copy_live_profit_latency_suppression_reject", "approved_suppression": True, "ts": "2026-07-07T21:20:00+00:00", "source_wallet": "0xabc", "taxonomy_tags": ["window_time_gte_180s"]},
            {"event": "wallet_copy_live_profit_latency_suppression_reject", "approved_suppression": True, "ts": "2026-07-07T21:40:00+00:00", "source_wallet": "0xabc", "taxonomy_tags": ["window_time_gte_180s"]},
        ],
    )

    scan = deadman._scan_liveness_events(
        event_log,
        since=dt.datetime(2026, 7, 7, 21, 30, tzinfo=dt.timezone.utc),
    )

    assert scan["approved_suppression_events"] == 1
    assert scan["member_signal_age"]["0xabc"]["suppressed_intents"] == 1
    assert scan["member_signal_age"]["0xabc"]["suppression_taxonomy"] == {"window_time_gte_180s": 1}


def test_policy_choke_rung_a_writes_only_a_selection_pin() -> None:
    target = "0x" + "b" * 40
    overlay = {"members": [{"source_wallet": target, "candidate_id": "candidate-b"}]}

    updated, report = deadman._execute_policy_choke_rung_a(
        overlay=overlay,
        target_wallet=target,
        now=dt.datetime(2026, 7, 20, 15, 2, tzinfo=dt.timezone.utc),
    )

    assert report["status"] == "RUNG_A_SELECTION_PIN_WRITTEN"
    assert updated["selection_pin"]["source_wallet"] == target
    assert updated["selection_pin"]["candidate_id"] == "candidate-b"
    assert "selection_pin" not in overlay


def _rung_b_fixture(now: dt.datetime, *, fresh_rows: int = 10):
    wallet = "0x" + "b" * 40
    policy = {"policy_id": "evidenced-policy", "max_order_usd": 4.0, "min_order_usd": 1.0, "max_price": 0.7}
    overlay = {"members": [{"source_wallet": "0x" + "c" * 40, "policy_id": "evidenced-policy", "policy": policy, "enabled": True}]}
    ready = {"lanes": [{"wallet": wallet, "paper_policy_id": "evidenced-policy", "retrospective_gross_pnl_usd": 20.0, "retrospective_gross_roi_pct": 4.0, "retrospective_resolved_signals": 250, "fading_clear": True, "external_liveness_status": "PASS", "external_latest_trade_age_h": 1.0}]}
    market_start = int(now.timestamp()) // 300 * 300
    hot = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "observed_ts": now.timestamp() - i,
                "event_id": f"e{i}",
                "market_slug": f"btc-updown-5m-{market_start}",
            }
            for i in range(fresh_rows)
        ]
    }
    return wallet, overlay, ready, hot


def test_policy_choke_rung_b_dry_run_builds_full_admission_payload() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={})

    unchanged, report = deadman._execute_policy_choke_rung_b(overlay=overlay, candidate=evidence["selected"], now=now, dry_run=True)

    assert report["status"] == "RUNG_B_DRY_RUN_PASS"
    assert report["member"]["source_wallet"] == wallet
    assert report["member"]["copy_size_usd"] == 1.0
    assert report["member"]["policy_id"] == "evidenced-policy"
    assert report["selection_pin"]["pin_id"] == deadman.POLICY_CHOKE_RUNG_B_PIN_ID
    assert unchanged == overlay


def test_direct_authority_holds_generic_wide_winner_until_freeze_allpass() -> None:
    generic = {
        "wallet": "0x" + "b" * 40,
        "wide_policy_fingerprint": "generic-fp",
        "eligible": True,
    }
    candidates = {"selected": generic, "rows": [generic], "eligible_count": 1}

    held, eligible = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={
            "status": "NOT_NEAR_BAR",
            "primary": {
                "wallet": "0x" + "a" * 40,
                "wide_policy_fingerprint": "freeze-fp",
            },
            "checks": {"all_pass": False},
            "actuator_contract": {"eligible_to_invoke": False},
        },
    )

    assert eligible is False
    assert held["selected"] is None
    assert held["status"] == "MEASURED_EMPTY_SEAT_HOLD_FREEZE_NOT_ALL_PASS"


def test_direct_authority_requires_observation_for_exact_freeze_allpass_identity() -> None:
    generic = {
        "wallet": "0x" + "b" * 40,
        "wide_policy_fingerprint": "generic-fp",
        "eligible": True,
    }
    freeze = {
        "wallet": "0x" + "a" * 40,
        "wide_policy_fingerprint": "freeze-fp",
        "source_generation": "generation-a",
        "eligible": True,
        "evidence_deficits": [],
        "fresh_own_source_buy_rows_30m": 10,
        "f2_evaluated_copyable": 2,
        "regime_evidence": {"pnl_usd": 83.0, "roi_pct": 12.0, "resolved_signals": 648},
        "checks": {
            "f1_measured_positive_regime_cell": True,
            "f1_venue_reachable_admissible": True,
            "f1_walk_forward_admissible": True,
            "f2_fresh_rows_and_own_policy_copyable": True,
            "f3_not_enabled_or_cooloff_or_fading": True,
            "f4_external_liveness": True,
            "own_evidenced_policy_available": True,
            "active_temporal_not_proven_negative": True,
            "active_temporal_regime_cell_measured": True,
            "not_terminal_park_red_clock_or_measured_loser": True,
            "both_resolved_halves_positive": True,
            "f1_concentration_admissible": True,
        },
    }

    selected, eligible = deadman._enforce_freeze_only_direct_authority(
        candidates={"selected": generic, "rows": [generic, freeze]},
        freeze_allpass_sidecar={
            "status": "ALL_PASS_READY",
            "primary": freeze,
            "checks": {"all_pass": True},
            "actuator_contract": {"eligible_to_invoke": True},
        },
        now=dt.datetime(2026, 8, 4, 4, 0, tzinfo=dt.timezone.utc),
    )

    assert eligible is False
    assert selected["selected"] is None
    assert selected["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING"
    assert selected["normal_gate_unique_allpass_observation"]["wallet"] == freeze["wallet"]
    assert selected["freeze_allpass_observation_fence"]["status"] == (
        "EXACT_IDENTITY_MUST_CLEAR_NORMAL_GATE_OBSERVATION"
    )


def _normal_gate_unique_allpass_candidates() -> dict:
    row = {
        "wallet": "0x00033f1089ff061813850e5135483bed39ce3b49",
        "wide_policy_fingerprint": "5c20" * 16,
        "source_generation": "generation-a",
        "eligible": True,
        "evidence_deficits": [],
        "fresh_own_source_buy_rows_30m": 10,
        "f2_evaluated_copyable": 2,
        "regime_evidence": {"pnl_usd": 83.0, "roi_pct": 12.0, "resolved_signals": 648},
        "checks": {
            "f1_measured_positive_regime_cell": True,
            "f1_venue_reachable_admissible": True,
            "f1_walk_forward_admissible": True,
            "f2_fresh_rows_and_own_policy_copyable": True,
            "f3_not_enabled_or_cooloff_or_fading": True,
            "f4_external_liveness": True,
            "own_evidenced_policy_available": True,
            "active_temporal_not_proven_negative": True,
            "active_temporal_regime_cell_measured": True,
            "not_terminal_park_red_clock_or_measured_loser": True,
            "both_resolved_halves_positive": True,
            "f1_concentration_admissible": True,
        },
    }
    return {
        "selected": row,
        "rows": [row],
        "eligible_count": 1,
        "frontier_key": "wallet|wide_policy_fingerprint|source_generation",
    }


def test_order141_normal_gate_requires_two_heartbeats_five_minutes_apart() -> None:
    first_at = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    first, first_ready = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_unique_allpass_candidates(),
        freeze_allpass_sidecar={},
        now=first_at,
    )
    assert first_ready is False
    assert first["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING"
    assert first["selected"] is None

    second, second_ready = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_unique_allpass_candidates(),
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=5),
    )
    assert second_ready is True
    assert second["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_SELECTED"
    assert second["selection_authority"] == "normal_gate_unique_all_pass"
    assert second["selected"]["wallet"].startswith("0x00033f")


def _normal_gate_f2_gap_candidates() -> dict:
    candidates = _normal_gate_unique_allpass_candidates()
    row = candidates["rows"][0]
    f2_gap = {
        **row,
        "eligible": False,
        "evidence_deficits": ["f2_fresh_rows_and_own_policy_copyable"],
        "f2_evaluated_copyable": 0,
        "fresh_own_source_buy_rows_30m": 35,
        "checks": {
            **row["checks"],
            "f2_fresh_rows_and_own_policy_copyable": False,
        },
    }
    return {**candidates, "selected": None, "rows": [f2_gap], "eligible_count": 0}


def test_normal_gate_single_f2_gap_preserves_clock_then_strict_allpass_selects() -> None:
    first_at = dt.datetime(2026, 8, 2, 20, 27, tzinfo=dt.timezone.utc)
    first, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_unique_allpass_candidates(),
        freeze_allpass_sidecar={},
        now=first_at,
    )
    assert ready is False
    first_observed_at = first["normal_gate_unique_allpass_observation"]["first_observed_at"]

    gap, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_f2_gap_candidates(),
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=3),
        money_and_tripwires_clear=True,
    )
    assert ready is False
    assert gap["selected"] is None
    assert gap["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING_F2_GAP"
    observation = gap["normal_gate_unique_allpass_observation"]
    assert observation["first_observed_at"] == first_observed_at
    assert observation["f2_gap_heartbeats"] == 1
    assert observation["f2_gap_last_at"] == (first_at + dt.timedelta(minutes=3)).isoformat()

    selected, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_unique_allpass_candidates(),
        freeze_allpass_sidecar={},
        previous_candidates=gap,
        now=first_at + dt.timedelta(minutes=5),
    )
    assert ready is True
    assert selected["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_SELECTED"
    assert selected["normal_gate_unique_allpass_observation"]["f2_gap_heartbeats"] == 0


def test_normal_gate_two_consecutive_f2_gaps_null_observation() -> None:
    first_at = dt.datetime(2026, 8, 2, 20, 27, tzinfo=dt.timezone.utc)
    first, _ = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_unique_allpass_candidates(),
        freeze_allpass_sidecar={},
        now=first_at,
    )
    gap, _ = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_f2_gap_candidates(),
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=1),
        money_and_tripwires_clear=True,
    )

    reset, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_f2_gap_candidates(),
        freeze_allpass_sidecar={},
        previous_candidates=gap,
        now=first_at + dt.timedelta(minutes=2),
        money_and_tripwires_clear=True,
    )
    assert ready is False
    assert reset["selected"] is None
    assert reset["status"] == "MEASURED_EMPTY_SEAT_HOLD_FREEZE_NOT_ALL_PASS"
    assert reset["normal_gate_unique_allpass_observation"] is None


def test_normal_gate_non_f2_deficit_nulls_observation() -> None:
    first_at = dt.datetime(2026, 8, 2, 20, 27, tzinfo=dt.timezone.utc)
    first, _ = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_unique_allpass_candidates(),
        freeze_allpass_sidecar={},
        now=first_at,
    )
    candidates = _normal_gate_f2_gap_candidates()
    row = candidates["rows"][0]
    candidates["rows"] = [
        {
            **row,
            "evidence_deficits": ["f4_external_liveness"],
            "checks": {**row["checks"], "f4_external_liveness": False},
        }
    ]

    reset, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=1),
        money_and_tripwires_clear=True,
    )
    assert ready is False
    assert reset["selected"] is None
    assert reset["status"] == "MEASURED_EMPTY_SEAT_HOLD_FREEZE_NOT_ALL_PASS"
    assert reset["normal_gate_unique_allpass_observation"] is None


def test_normal_gate_f2_gap_heartbeat_never_selects() -> None:
    first_at = dt.datetime(2026, 8, 2, 20, 27, tzinfo=dt.timezone.utc)
    first, _ = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_unique_allpass_candidates(),
        freeze_allpass_sidecar={},
        now=first_at,
    )

    gap, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_f2_gap_candidates(),
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=6),
        money_and_tripwires_clear=True,
    )
    assert ready is False
    assert gap["selected"] is None
    assert gap["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING_F2_GAP"


def test_order152_multiple_allpass_candidates_rank_deterministically() -> None:
    candidates = _normal_gate_unique_allpass_candidates()
    winner = {
        **candidates["rows"][0],
        "direct_source": {
            "copyable_continuity_pass": True,
            "attempt_continuity_pass": True,
            "copyable": 20,
        },
    }
    challenger = {
        **candidates["rows"][0],
        "wallet": "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b",
        "wide_policy_fingerprint": "challenger",
        "direct_source": {
            "copyable_continuity_pass": True,
            "attempt_continuity_pass": True,
            "copyable": 8,
        },
        "regime_evidence": {
            **candidates["rows"][0]["regime_evidence"],
            "pnl_usd": 74.0,
            "roi_pct": 7.0,
        },
    }
    outputs = []
    for rows in ([winner, challenger], [challenger, winner]):
        candidates["rows"] = rows
        candidates["eligible_count"] = 2
        ranked, ready = deadman._enforce_freeze_only_direct_authority(
            candidates=candidates,
            freeze_allpass_sidecar={},
            now=dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc),
        )
        assert ready is False
        assert ranked["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING"
        assert ranked["raw_eligible_count"] == 2
        assert ranked["eligible_count"] == 1
        assert ranked["requires_fable_ping"] is True
        assert ranked["deferred_allpass_challengers"] == ["challenger"]
        outputs.append(ranked["normal_gate_unique_allpass_observation"]["wallet"])
    assert outputs == [winner["wallet"], winner["wallet"]]


def test_order151_inflight_observation_defers_allpass_challenger() -> None:
    first_at = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    first, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        now=first_at,
    )
    assert ready is False
    first_observed_at = first["normal_gate_unique_allpass_observation"]["first_observed_at"]

    challenger = {
        **candidates["rows"][0],
        "wallet": "0x" + "d" * 40,
        "wide_policy_fingerprint": "challenger",
        "source_generation": "generation-b",
    }
    candidates["rows"] = [candidates["rows"][0], challenger]
    candidates["eligible_count"] = 2
    confirming, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=1),
    )

    assert ready is False
    assert confirming["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING"
    assert confirming["selected"] is None
    assert confirming["deferred_allpass_challengers"] == ["challenger"]
    observation = confirming["normal_gate_unique_allpass_observation"]
    assert observation["first_observed_at"] == first_observed_at
    assert observation["consecutive_heartbeats"] == 2


def test_order151_ineligible_incumbent_starts_challenger_confirmation() -> None:
    first_at = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    first, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        now=first_at,
    )
    assert ready is False

    incumbent = {**candidates["rows"][0], "eligible": False}
    challenger = {
        **candidates["rows"][0],
        "wallet": "0x" + "d" * 40,
        "wide_policy_fingerprint": "challenger",
        "source_generation": "generation-b",
    }
    candidates["rows"] = [incumbent, challenger]
    candidates["eligible_count"] = 1
    confirming, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=1),
    )

    assert ready is False
    assert confirming["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING"
    assert confirming["requires_fable_ping"] is True
    assert confirming["normal_gate_unique_allpass_observation"]["wallet"] == challenger["wallet"]
    assert confirming["normal_gate_unique_allpass_observation"]["consecutive_heartbeats"] == 1
    assert confirming["inflight_observation_lost_eligibility"] == {
        "wallet": candidates["rows"][0]["wallet"],
        "wide_policy_fingerprint": candidates["rows"][0]["wide_policy_fingerprint"],
    }


def test_order152_lost_incumbent_ranks_multiple_allpass_deterministically() -> None:
    first_at = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    incumbent = {
        **candidates["rows"][0],
        "wallet": "0x" + "b" * 40,
        "wide_policy_fingerprint": "incumbent",
    }
    candidates["rows"] = [incumbent]
    first, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        now=first_at,
    )
    assert ready is False

    winner = {
        **candidates["rows"][0],
        "wallet": "0x00033f1089ff061813850e5135483bed39ce3b49",
        "wide_policy_fingerprint": "5c20c15050",
        "direct_source": {
            "copyable_continuity_pass": True,
            "attempt_continuity_pass": True,
            "copyable": 20,
        },
    }
    challenger = {
        **winner,
        "wallet": "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b",
        "wide_policy_fingerprint": "successor",
        "direct_source": {
            "copyable_continuity_pass": True,
            "attempt_continuity_pass": True,
            "copyable": 7,
        },
    }
    incumbent = {**incumbent, "eligible": False}

    selected_wallets = []
    for eligible_rows in ([winner, challenger], [challenger, winner]):
        ranked, ready = deadman._enforce_freeze_only_direct_authority(
            candidates={**candidates, "rows": [incumbent, *eligible_rows]},
            freeze_allpass_sidecar={},
            previous_candidates=first,
            now=first_at + dt.timedelta(minutes=1),
        )
        assert ready is False
        assert ranked["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING"
        assert ranked["inflight_observation_lost_eligibility"]["wallet"] == incumbent["wallet"]
        assert ranked["deferred_allpass_challengers"] == ["successor"]
        selected_wallets.append(ranked["normal_gate_unique_allpass_observation"]["wallet"])

    assert selected_wallets == [winner["wallet"], winner["wallet"]]


def test_order151_same_wallet_policies_collapse_to_frontier_first() -> None:
    candidates = _normal_gate_unique_allpass_candidates()
    first = candidates["rows"][0]
    alternate = {**first, "wide_policy_fingerprint": "alternate"}
    candidates["rows"] = [first, alternate]
    candidates["eligible_count"] = 2
    collapsed, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        now=dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc),
    )
    assert ready is False
    assert collapsed["status"] == "NORMAL_GATE_UNIQUE_ALLPASS_CONFIRMING"
    assert collapsed["raw_eligible_count"] == 2
    assert collapsed["eligible_count"] == 1
    assert (
        collapsed["normal_gate_unique_allpass_observation"]["wide_policy_fingerprint"]
        == first["wide_policy_fingerprint"]
    )
    selected, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        previous_candidates=collapsed,
        now=dt.datetime(2026, 8, 2, 3, 5, tzinfo=dt.timezone.utc),
    )
    assert ready is True
    assert (
        selected["selected"]["wide_policy_fingerprint"]
        == first["wide_policy_fingerprint"]
    )
    assert selected["selected"]["collapsed_same_wallet_alternates"] == ["alternate"]


def test_freeze_exact_preselects_inside_same_wallet_collapse() -> None:
    candidates = _normal_gate_unique_allpass_candidates()
    exact = candidates["rows"][0]
    sibling = {
        **exact,
        "wide_policy_fingerprint": "outranking-sibling",
        "direct_source": {
            "copyable_continuity_pass": True,
            "attempt_continuity_pass": True,
            "copyable": 99,
        },
    }
    candidates["rows"] = [sibling, exact]
    candidates["eligible_count"] = 2
    freeze = {
        "status": "ALL_PASS_READY",
        "primary": exact,
        "checks": {"all_pass": True},
        "actuator_contract": {"eligible_to_invoke": True},
    }

    result, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar=freeze,
        now=dt.datetime(2026, 8, 4, 5, 0, tzinfo=dt.timezone.utc),
    )

    assert ready is False
    assert result["normal_gate_unique_allpass_observation"][
        "wide_policy_fingerprint"
    ] == exact["wide_policy_fingerprint"]
    preselection = result["freeze_exact_preselection"]
    assert preselection["status"] == "FREEZE_EXACT_PRESELECTED"
    assert preselection["outranked_siblings"][0][
        "wide_policy_fingerprint"
    ] == "outranking-sibling"


def test_freeze_duplicate_key_refusal_carries_one_heartbeat_grace() -> None:
    now = dt.datetime(2026, 8, 4, 5, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    exact = candidates["rows"][0]
    freeze = {
        "status": "ALL_PASS_READY",
        "primary": exact,
        "checks": {"all_pass": True},
        "actuator_contract": {"eligible_to_invoke": True},
    }
    first, _ = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar=freeze,
        now=now,
    )
    duplicates = {**candidates, "rows": [exact, dict(exact)], "eligible_count": 2}

    grace, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=duplicates,
        freeze_allpass_sidecar=freeze,
        previous_candidates=first,
        now=now + dt.timedelta(minutes=1),
    )
    assert ready is True
    assert grace["status"] == "FREEZE_ALLPASS_IDENTITY_LOST_DURING_WALLET_COLLAPSE"
    observation = grace["normal_gate_unique_allpass_observation"]
    assert observation["consecutive_heartbeats"] == 1
    assert observation["f2_gap_heartbeats"] == 1
    assert observation["freeze_refusal_grace"][
        "consecutive_heartbeats_incremented"
    ] is False

    reset, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=duplicates,
        freeze_allpass_sidecar=freeze,
        previous_candidates=grace,
        now=now + dt.timedelta(minutes=2),
    )
    assert ready is True
    assert reset["normal_gate_unique_allpass_observation"] is None


def test_freeze_absence_identity_change_does_not_receive_grace() -> None:
    now = dt.datetime(2026, 8, 4, 5, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    observed, _ = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        now=now,
    )
    freeze_primary = {
        **candidates["rows"][0],
        "wallet": "0x" + "f" * 40,
        "wide_policy_fingerprint": "different-freeze",
    }
    refused, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={
            "status": "ALL_PASS_READY",
            "primary": freeze_primary,
            "checks": {"all_pass": True},
            "actuator_contract": {"eligible_to_invoke": True},
        },
        previous_candidates=observed,
        now=now + dt.timedelta(minutes=1),
    )
    assert ready is True
    assert refused["status"] == "FREEZE_ALLPASS_IDENTITY_ABSENT_FROM_FRONTIER"
    assert refused["normal_gate_unique_allpass_observation"] is None


def test_freeze_exact_persistent_outrank_alerts_after_three_generations() -> None:
    now = dt.datetime(2026, 8, 4, 3, 0, tzinfo=dt.timezone.utc)
    previous = None
    result = None
    for index in range(3):
        candidates = _normal_gate_unique_allpass_candidates()
        exact = {
            **candidates["rows"][0],
            "source_generation": f"generation-{index}",
            "source_identity": {
                "run_id": f"wide_20260804T0{3 + index}0000Z"
            },
        }
        sibling = {
            **exact,
            "wide_policy_fingerprint": "persistent-sibling",
            "direct_source": {
                "copyable_continuity_pass": True,
                "attempt_continuity_pass": True,
                "copyable": 99,
            },
        }
        candidates["rows"] = [sibling, exact]
        candidates["eligible_count"] = 2
        result, _ = deadman._enforce_freeze_only_direct_authority(
            candidates=candidates,
            freeze_allpass_sidecar={
                "status": "ALL_PASS_READY",
                "primary": exact,
                "checks": {"all_pass": True},
                "actuator_contract": {"eligible_to_invoke": True},
            },
            previous_candidates=previous,
            now=now + dt.timedelta(hours=index),
        )
        previous = result

    assert result is not None
    preselection = result["freeze_exact_preselection"]
    assert preselection["status"] == "FREEZE_EXACT_PERSISTENTLY_OUTRANKED"
    assert preselection["consecutive_outranked_generations"] == 3
    assert result["requires_fable_ping"] is True


def test_order151_normal_gate_observation_records_source_generation() -> None:
    now = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    first, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=_normal_gate_unique_allpass_candidates(),
        freeze_allpass_sidecar={},
        now=now,
    )

    assert ready is False
    assert (
        first["normal_gate_unique_allpass_observation"]["source_generation"]
        == "generation-a"
    )


def test_normal_gate_generation_flip_resets_observation_clock() -> None:
    first_at = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    first, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        now=first_at,
    )
    assert ready is False
    candidates["rows"] = [
        {**candidates["rows"][0], "source_generation": "generation-b"}
    ]
    reset, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=5),
    )
    assert ready is False
    observation = reset["normal_gate_unique_allpass_observation"]
    assert observation["source_generation"] == "generation-b"
    assert observation["consecutive_heartbeats"] == 1
    assert observation["first_observed_at"] == (
        first_at + dt.timedelta(minutes=5)
    ).isoformat()


def test_normal_gate_forward_generation_roll_preserves_observation_clock() -> None:
    first_at = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    candidates["rows"][0]["source_identity"] = {
        "run_id": "wide_20260802T030000Z"
    }
    first, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        now=first_at,
    )
    assert ready is False
    candidates["rows"] = [
        {
            **candidates["rows"][0],
            "source_generation": "generation-b",
            "source_identity": {"run_id": "wide_20260802T033500Z"},
        }
    ]
    selected, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=5),
    )
    assert ready is True
    observation = selected["normal_gate_unique_allpass_observation"]
    assert observation["source_generation"] == "generation-b"
    assert observation["source_generation_transition"] == "FORWARD"
    assert observation["consecutive_heartbeats"] == 2
    assert observation["first_observed_at"] == first_at.isoformat()


def test_normal_gate_backward_generation_roll_resets_observation_clock() -> None:
    first_at = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    candidates["rows"][0].update(
        {
            "source_generation": "generation-b",
            "source_identity": {"run_id": "wide_20260802T033500Z"},
        }
    )
    first, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        now=first_at,
    )
    assert ready is False
    candidates["rows"] = [
        {
            **candidates["rows"][0],
            "source_generation": "generation-a",
            "source_identity": {"run_id": "wide_20260802T030000Z"},
        }
    ]
    reset, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        previous_candidates=first,
        now=first_at + dt.timedelta(minutes=5),
    )
    assert ready is False
    observation = reset["normal_gate_unique_allpass_observation"]
    assert observation["source_generation"] == "generation-a"
    assert observation["source_generation_transition"] == "BACKWARD"
    assert observation["consecutive_heartbeats"] == 1
    assert observation["first_observed_at"] == (
        first_at + dt.timedelta(minutes=5)
    ).isoformat()


def test_wide_generation_churn_measures_hashes_by_run_identity() -> None:
    packet = deadman._wide_generation_churn(
        [
            {
                "source_generation": "hash-c",
                "identity": {"run_id": "wide_20260804T044308Z"},
            },
            {
                "source_generation": "hash-a",
                "identity": {"run_id": "wide_20260804T033544Z"},
            },
            {
                "source_generation": "hash-b",
                "identity": {"run_id": "wide_20260804T040718Z"},
            },
        ],
        required_stability_s=1920.0,
    )
    assert packet["intervals_s"] == [1894.0, 2150.0]
    assert packet["min_interval_s"] == 1894.0
    assert packet["below_required_count"] == 1
    assert packet["d3_required"] is True


def _active_direct_overlay_for_row(row: dict) -> dict:
    candidate_id = "policy_choke_rung_direct_" + row["wallet"][-10:]
    return {
        "members": [
            {
                "candidate_id": candidate_id,
                "source_wallet": row["wallet"],
                "enabled": True,
                "policy": {
                    "wide_policy_fingerprint": row["wide_policy_fingerprint"]
                },
            }
        ],
        "selection_pin": {
            "enabled": True,
            "pin_id": deadman.POLICY_CHOKE_RUNG_B_PIN_ID,
            "candidate_id": candidate_id,
            "source_wallet": row["wallet"],
            "created_at": "2026-08-02T02:30:00+00:00",
            "expires_at": "2026-08-02T04:00:00+00:00",
        },
    }


def test_order153_turnover_refused_when_f3_waived_incumbent_outranks() -> None:
    first_at = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    incumbent = {
        **candidates["rows"][0],
        "eligible": False,
        "wallet_cooloff_active": False,
        "fading_clear": True,
        "checks": {
            **candidates["rows"][0]["checks"],
            "f3_not_enabled_or_cooloff_or_fading": False,
        },
        "direct_source": {
            "copyable_continuity_pass": True,
            "attempt_continuity_pass": True,
            "copyable": 22,
        },
    }
    challenger = {
        **candidates["rows"][0],
        "wallet": "0x" + "d" * 40,
        "wide_policy_fingerprint": "challenger",
        "direct_source": {
            "copyable_continuity_pass": True,
            "attempt_continuity_pass": True,
            "copyable": 17,
        },
    }
    candidates["rows"] = [incumbent, challenger]
    overlay = _active_direct_overlay_for_row(incumbent)
    first, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        active_set_overlay=overlay,
        now=first_at,
    )
    assert ready is False
    refused, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        previous_candidates=first,
        active_set_overlay=overlay,
        now=first_at + dt.timedelta(minutes=5),
    )
    assert ready is False
    assert refused["status"] == "NORMAL_GATE_ALLPASS_TURNOVER_REFUSED"
    refusal = refused["allpass_turnover_refusal"]
    assert refusal["status"] == "allpass_turnover_refused_incumbent_outranks"
    assert refusal["incumbent_rank"] < refusal["challenger_rank"]


def test_order153_turnover_allowed_when_challenger_strictly_outranks_incumbent() -> None:
    first_at = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    candidates = _normal_gate_unique_allpass_candidates()
    incumbent = {
        **candidates["rows"][0],
        "eligible": False,
        "wallet_cooloff_active": False,
        "fading_clear": True,
        "checks": {
            **candidates["rows"][0]["checks"],
            "f3_not_enabled_or_cooloff_or_fading": False,
        },
        "direct_source": {
            "copyable_continuity_pass": True,
            "attempt_continuity_pass": True,
            "copyable": 8,
        },
    }
    challenger = {
        **candidates["rows"][0],
        "wallet": "0x" + "d" * 40,
        "wide_policy_fingerprint": "challenger",
        "direct_source": {
            "copyable_continuity_pass": True,
            "attempt_continuity_pass": True,
            "copyable": 17,
        },
    }
    candidates["rows"] = [incumbent, challenger]
    overlay = _active_direct_overlay_for_row(incumbent)
    first, _ = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        active_set_overlay=overlay,
        now=first_at,
    )
    selected, ready = deadman._enforce_freeze_only_direct_authority(
        candidates=candidates,
        freeze_allpass_sidecar={},
        previous_candidates=first,
        active_set_overlay=overlay,
        now=first_at + dt.timedelta(minutes=5),
    )
    assert ready is True
    assert selected["selected"]["wallet"] == challenger["wallet"]


def test_policy_choke_rung_b_rejects_flowless_candidate() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    _wallet, overlay, ready, hot = _rung_b_fixture(now, fresh_rows=9)

    evidence = deadman._select_policy_choke_rung_b_candidate(ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={})

    assert evidence["selected"] is None
    assert evidence["rows"][0]["checks"]["f2_fresh_rows_and_own_policy_copyable"] is False


def test_policy_choke_rung_b_rejects_recently_observed_closed_market_rows() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    _wallet, overlay, ready, hot = _rung_b_fixture(now)
    closed_start = int(now.timestamp()) // 300 * 300 - 300
    for row in hot["events"]:
        row["market_slug"] = f"btc-updown-5m-{closed_start}"

    evidence = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready,
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        temporal_registry={},
        cooloffs={},
    )

    assert evidence["selected"] is None
    assert evidence["rows"][0]["fresh_own_source_buy_rows_30m"] == 0
    assert evidence["rows"][0]["checks"]["f2_fresh_rows_and_own_policy_copyable"] is False


def test_policy_choke_rung_b_rejects_active_temporal_proven_negative() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    temporal = {
        "wallets": [
            {
                "wallet": wallet,
                "classification": "BAND-SPECIALIST",
                "slice_labels": {
                    "weekday": {
                        "label": "PROVEN-NEGATIVE",
                        "resolved_trades": 762,
                        "roi_pct": -4.698712,
                        "pnl_usd": -794.665049,
                    }
                },
            }
        ]
    }

    evidence = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready,
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        cooloffs={},
        temporal_registry=temporal,
    )

    assert evidence["selected"] is None
    assert evidence["rows"][0]["checks"]["active_temporal_not_proven_negative"] is False
    assert evidence["rows"][0]["active_temporal"]["pnl_usd"] == -794.665049


def test_policy_choke_rung_b_keeps_active_temporal_proven_positive_eligible() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    temporal = {
        "criteria": {"min_trades": 5},
        "wallets": [
            {
                "wallet": wallet,
                "slice_labels": {
                    "weekday": {
                        "label": "PROVEN-POSITIVE",
                        "resolved_trades": 667,
                        "roi_pct": 3.498568,
                        "pnl_usd": 455.376303,
                    }
                },
            }
        ]
    }

    evidence = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready,
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        cooloffs={},
        temporal_registry=temporal,
    )

    assert evidence["selected"]["wallet"] == wallet
    assert evidence["selected"]["checks"]["active_temporal_not_proven_negative"] is True
    assert evidence["selected"]["checks"]["active_temporal_regime_cell_measured"] is True


def test_policy_choke_rung_b_rejects_unmeasured_active_regime_cell() -> None:
    now = dt.datetime(2026, 8, 1, 0, 30, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    temporal = {
        "criteria": {"min_trades": 5},
        "wallets": [
            {
                "wallet": wallet,
                "slice_labels": {
                    "weekend": {"label": "UNPROVEN", "resolved_trades": 0}
                },
            }
        ],
    }

    evidence = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready,
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekend",
        cooloffs={},
        temporal_registry=temporal,
        require_measured_temporal=True,
    )

    row = evidence["rows"][0]
    assert evidence["selected"] is None
    assert row["checks"]["active_temporal_not_proven_negative"] is True
    assert row["checks"]["active_temporal_regime_cell_measured"] is False
    assert evidence["gate_digits"]["f5_min_regime_slice_resolved_trades"] == 5


def test_policy_choke_rung_b_rejects_simultaneous_dead_band_proven_negative() -> None:
    now = dt.datetime(2026, 7, 20, 20, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    temporal = {
        "wallets": [
            {
                "wallet": wallet,
                "classification": "BAND-SPECIALIST",
                "slice_labels": {
                    "weekday": {"label": "UNPROVEN", "resolved_trades": 0},
                    "dead_band_18_22_utc": {
                        "label": "PROVEN-NEGATIVE",
                        "resolved_trades": 391,
                        "roi_pct": -3.725034,
                        "pnl_usd": -117.20705,
                    },
                },
            }
        ]
    }

    evidence = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready,
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        cooloffs={},
        temporal_registry=temporal,
    )

    row = evidence["rows"][0]
    assert evidence["selected"] is None
    assert row["checks"]["active_temporal_not_proven_negative"] is False
    assert [item["slice"] for item in row["active_temporal_slices"]] == [
        "weekday",
        "dead_band_18_22_utc",
    ]
    assert row["active_temporal"]["pnl_usd"] == -117.20705


def test_deadman_prefers_venue_temporal_slice_over_all_venue_slice() -> None:
    wallet = "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b"
    active = deadman._temporal_active_slice(
        {
            "wallets": [{
                "wallet": wallet,
                "slice_labels": {"weekday": {
                    "label": "PROVEN-POSITIVE", "resolved_trades": 11965,
                    "roi_pct": 1.497, "pnl_usd": 1529.72,
                }},
                "venue_slice_labels": {"weekday": {
                    "label": "PROVEN-NEGATIVE", "resolved_trades": 3957,
                    "roi_pct": -7.358, "pnl_usd": -1267.87,
                }},
            }]
        },
        wallet=wallet,
        regime="weekday",
    )

    assert active["label"] == "PROVEN-NEGATIVE"
    assert active["resolved_trades"] == 3957
    assert active["pnl_usd"] == -1267.87


def test_policy_choke_pin_fails_closed_on_guard_temporal_rejection() -> None:
    candidate = {
        "eligible": True,
        "wallet": "0x" + "5" * 40,
        "paper_policy_id": "wide_fp_test",
        "policy": {"policy_id": "wide_fp_test", "max_order_usd": 1.0},
        "active_temporal_slices": [
            {"slice": "weekday", "label": "PROVEN-NEGATIVE", "pnl_usd": -1.0}
        ],
    }
    overlay = {"members": []}

    unchanged, report = deadman._execute_policy_choke_rung_b(
        overlay=overlay,
        candidate=candidate,
        now=dt.datetime(2026, 8, 3, tzinfo=dt.timezone.utc),
        supply_rung="DIRECT",
    )

    assert unchanged == overlay
    assert report["status"] == "REFUSED_GUARD_TEMPORAL_CROSSCHECK"
    assert "selection_pin" not in unchanged


def test_deadman_disables_direct_pin_when_active_temporal_slice_turns_negative() -> None:
    now = dt.datetime(2026, 7, 26, 20, 23, tzinfo=dt.timezone.utc)
    wallet = "0x" + "9" * 40
    fingerprint = "f" * 64
    candidate_id = "policy_choke_rung_direct_" + wallet[-10:]
    overlay = {
        "members": [
            {
                "candidate_id": candidate_id,
                "source_wallet": wallet,
                "enabled": True,
                "policy": {"wide_policy_fingerprint": fingerprint},
            }
        ],
        "selection_pin": {
            "enabled": True,
            "pin_id": deadman.POLICY_CHOKE_RUNG_B_PIN_ID,
            "candidate_id": candidate_id,
            "source_wallet": wallet,
        },
    }
    evidence = {
        "rows": [
            {
                "wallet": wallet,
                "wide_policy_fingerprint": fingerprint,
                "active_temporal_slices": [
                    {"slice": "weekend", "label": "UNPROVEN"},
                    {
                        "slice": "dead_band_18_22_utc",
                        "label": "PROVEN-NEGATIVE",
                        "pnl_usd": -117.20705,
                    },
                ],
            }
        ]
    }

    updated, report = deadman._disable_temporally_ineligible_direct_pin(
        overlay=overlay,
        candidate_evidence=evidence,
        now=now,
    )

    assert report["status"] == "DIRECT_PIN_DISABLED_ACTIVE_TEMPORAL_PROVEN_NEGATIVE"
    assert updated["selection_pin"]["enabled"] is False
    assert updated["selection_pin"]["disabled_reason"] == (
        "temporal_slice_dead_band_18_22_utc_proven_negative"
    )
    assert updated["members"][0]["enabled"] is False


def test_policy_choke_rung_b_is_idempotent_while_pin_active() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    _wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={})
    admitted, first = deadman._execute_policy_choke_rung_b(overlay=overlay, candidate=evidence["selected"], now=now)

    unchanged, second = deadman._execute_policy_choke_rung_b(
        overlay=admitted,
        candidate=evidence["selected"],
        now=now + dt.timedelta(minutes=1),
        supply_rung="DIRECT",
    )

    assert first["status"] == "RUNG_B_ADMISSION_PIN_WRITTEN"
    assert first["policy_choke_rung_b_refusal"]["armed_at"] == first["selection_pin"]["created_at"]
    assert first["policy_choke_rung_b_refusal"]["expires_at"] == first["selection_pin"]["expires_at"]
    assert first["policy_choke_rung_b_refusal"]["frontier_eligible_count"] is None
    assert first["policy_choke_rung_b_refusal"]["arm_count_utc_day"] == 1
    assert first["policy_choke_rung_b_refusal"]["refusal_basis"] == (
        "UNMEASURED_NO_FRONTIER_EVIDENCE"
    )
    assert second["status"] == "DIRECT_SOURCE_SELECTION_ALREADY_ACTIVE"
    assert second["policy_choke_rung_b_refusal"]["supply_rung"] == "DIRECT"
    assert second["policy_choke_rung_b_refusal"]["armed_at"] == first["selection_pin"]["created_at"]
    assert second["policy_choke_rung_b_refusal"]["expires_at"] == first["selection_pin"]["expires_at"]
    assert second["policy_choke_rung_b_refusal"]["frontier_eligible_count"] is None
    assert second["policy_choke_rung_b_refusal"]["arm_count_utc_day"] == 1
    assert unchanged == admitted
    assert len(admitted["members"]) == 2


def test_policy_choke_rung_b_refusal_uses_frontier_counts_and_day_scoped_arms() -> None:
    now = dt.datetime(2026, 8, 4, 16, 50, tzinfo=dt.timezone.utc)
    active_pin = {
        "pin_id": deadman.POLICY_CHOKE_RUNG_B_PIN_ID,
        "created_at": "2026-08-04T15:28:18+00:00",
        "last_renewed_at": "2026-08-04T16:31:33+00:00",
        "expires_at": "2026-08-04T17:31:33+00:00",
    }
    overlay = {
        "selection_pin": active_pin,
        "latest_policy_choke_rung_b_admission": {
            "selection_pin": {
                "pin_id": deadman.POLICY_CHOKE_RUNG_B_PIN_ID,
                "created_at": "2026-08-04T15:28:18+00:00",
            }
        },
        "previous_selection_pins": [
            {
                "pin_id": deadman.POLICY_CHOKE_RUNG_B_PIN_ID,
                "created_at": "2026-08-03T23:58:00+00:00",
            },
            {
                "pin_id": deadman.POLICY_CHOKE_RUNG_B_PIN_ID,
                "created_at": "2026-08-04T14:00:00+00:00",
            },
        ],
    }

    record = deadman._policy_choke_rung_b_refusal_record(
        selection_pin=active_pin,
        candidate_evidence={"candidate_count": 153, "eligible_count": 0},
        overlay=overlay,
        now=now,
        supply_rung="DIRECT",
    )

    assert record["armed_at"] == "2026-08-04T16:31:33+00:00"
    assert record["frontier_candidate_count"] == 153
    assert record["frontier_eligible_count"] == 0
    assert record["refusal_basis"] == "frontier eligible_count = 0"
    assert record["refusal_basis_source"] == (
        "policy_choke.actuator.rung_b_candidate_evidence.eligible_count"
    )
    assert record["supply_rung"] == "DIRECT"
    assert record["record_key_is_legacy_alias"] is True
    assert record["record_key_scope"] == "all_policy_choke_supply_rungs"
    assert record["arm_count_utc_day"] == 2
    assert record["arm_count_utc_day_source"] == (
        "same_utc_day_unique_policy_choke_rung_b_pin_timestamps"
    )
    assert record["renew_count_utc_day"] == 1
    assert record["renew_count_utc_day_source"] == (
        "same_utc_day_unique_policy_choke_rung_b_last_renewed_timestamps"
    )
    assert {event["event_kind"] for event in record["arm_events_utc_day"]} == {
        "ARM"
    }
    assert {event["event_kind"] for event in record["renew_events_utc_day"]} == {
        "RENEW"
    }
    assert all(
        event["armed_at"].startswith("2026-08-04")
        for event in record["arm_events_utc_day"]
    )


def test_forced_measured_positive_seat_waives_only_f2_f4_and_carries_kill_line() -> None:
    wallet = "0x" + "a" * 40
    checks = {
        "f1_measured_positive_regime_cell": True,
        "f1_walk_forward_admissible": True,
        "f1_concentration_admissible": True,
        "f1_venue_reachable_admissible": True,
        "both_resolved_halves_positive": True,
        "active_temporal_not_proven_negative": True,
        "active_temporal_regime_cell_measured": True,
        "f2_fresh_rows_and_own_policy_copyable": False,
        "f3_not_enabled_or_cooloff_or_fading": True,
        "f4_external_liveness": False,
        "not_terminal_park_red_clock_or_measured_loser": True,
        "own_evidenced_policy_available": True,
    }
    candidate = deadman._forced_measured_positive_seat_candidate(
        {
            "rows": [
                {
                    "wallet": wallet,
                    "checks": checks,
                    "paper_policy_id": "wide_fp_test",
                    "policy": {"policy_id": "wide_fp_test", "max_order_usd": 8.0},
                    "direct_source": {"attempt_continuity_pass": True},
                    "regime_evidence": {"pnl_usd": 12.0, "roi_pct": 3.0},
                }
            ]
        }
    )
    assert candidate is not None
    assert candidate["waived_liveness_checks"] == [
        "f2_fresh_rows_and_own_policy_copyable",
        "f4_external_liveness",
    ]
    admitted, report = deadman._execute_policy_choke_rung_b(
        overlay={"members": []},
        candidate=candidate,
        now=dt.datetime(2026, 8, 1, 12, 50, tzinfo=dt.timezone.utc),
        supply_rung="DIRECT",
    )
    assert report["status"] == "DIRECT_SOURCE_SELECTION_PIN_WRITTEN"
    assert admitted["members"][0]["max_order_usd"] == 1.0
    assert admitted["members"][0]["kill_line"]["post_fee_pnl_usd_lte"] == -4.0
    assert admitted["selection_pin"]["kill_line"] == admitted["members"][0]["kill_line"]


def test_forced_measured_positive_seat_does_not_waive_economic_bar() -> None:
    checks = {
        key: True
        for key in (
            "f1_measured_positive_regime_cell",
            "f1_walk_forward_admissible",
            "f1_concentration_admissible",
            "f1_venue_reachable_admissible",
            "both_resolved_halves_positive",
            "active_temporal_not_proven_negative",
            "active_temporal_regime_cell_measured",
            "f3_not_enabled_or_cooloff_or_fading",
            "not_terminal_park_red_clock_or_measured_loser",
            "own_evidenced_policy_available",
        )
    }
    checks["both_resolved_halves_positive"] = False
    assert deadman._forced_measured_positive_seat_candidate(
        {
            "rows": [
                {
                    "wallet": "0x" + "b" * 40,
                    "checks": checks,
                    "paper_policy_id": "wide_fp_test",
                    "policy": {"policy_id": "wide_fp_test", "max_order_usd": 1.0},
                }
            ]
        }
    ) is None


def test_active_fingerprint_direct_pin_repairs_five_share_funding_without_extending_ttl() -> None:
    now = dt.datetime(2026, 7, 25, 22, 0, tzinfo=dt.timezone.utc)
    wallet = "0x" + "1" * 40
    candidate = {
        "eligible": True,
        "wallet": wallet,
        "paper_policy_id": "wide_fp_" + "f" * 24,
        "policy": {
            "policy_id": "wide_fp_" + "f" * 24,
            "max_order_usd": 1.0,
        },
    }
    admitted, first = deadman._execute_policy_choke_rung_b(
        overlay={"members": []},
        candidate=candidate,
        now=now,
        supply_rung="DIRECT",
    )
    policy = admitted["members"][0]["policy"]
    assert policy["max_order_usd"] == 1.0
    assert policy["maker_min_share_base_request_cap_usd"] == 1.0
    assert policy["maker_min_share_funding_cap_usd"] == 2.5
    assert policy["maker_min_share_original_policy_cap_usd"] == 4.0

    stripped = dict(admitted)
    stripped["members"] = [
        {
            **admitted["members"][0],
            "policy": {
                "policy_id": candidate["paper_policy_id"],
                "max_order_usd": 1.0,
            },
        }
    ]
    repaired, report = deadman._execute_policy_choke_rung_b(
        overlay=stripped,
        candidate=candidate,
        now=now + dt.timedelta(minutes=5),
        supply_rung="DIRECT",
    )
    assert report["status"] == "DIRECT_SOURCE_SELECTION_POLICY_REPAIRED"
    assert repaired["selection_pin"]["expires_at"] == first["selection_pin"]["expires_at"]
    assert repaired["members"][0]["policy"]["maker_min_share_base_request_cap_usd"] == 1.0
    assert repaired["members"][0]["policy"]["maker_min_share_funding_cap_usd"] == 2.5


def test_order153_direct_pin_renews_from_incumbent_allpass_without_adoption() -> None:
    now = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    row = _normal_gate_unique_allpass_candidates()["rows"][0]
    incumbent = {
        **row,
        "eligible": False,
        "wallet_cooloff_active": False,
        "fading_clear": True,
        "checks": {
            **row["checks"],
            "f3_not_enabled_or_cooloff_or_fading": False,
        },
    }
    overlay = _active_direct_overlay_for_row(incumbent)

    renewed, report = deadman._renew_direct_pin_from_incumbent_allpass(
        overlay=overlay,
        candidate_evidence={"rows": [incumbent]},
        now=now,
        guard={
            "active_set_dataapi_poller": {
                "fetch_meta": {
                    incumbent["wallet"]: {
                        "policy_feedback": {"freshest_policy_compatible_buy_lag_s": 12.0}
                    }
                }
            }
        },
    )

    assert report["status"] == "DIRECT_SOURCE_SELECTION_PIN_RENEWED"
    assert report["admission_replayed"] is False
    assert report["guard_restart_requested"] is False
    assert renewed["selection_pin"]["created_at"] == overlay["selection_pin"]["created_at"]
    assert renewed["selection_pin"]["expires_at"] == (
        now + dt.timedelta(seconds=deadman.POLICY_CHOKE_RUNG_B_TTL_S)
    ).isoformat()
    assert len(renewed["members"]) == 1


def test_order153_direct_pin_does_not_renew_empty_cycles() -> None:
    now = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    row = _normal_gate_unique_allpass_candidates()["rows"][0]
    incumbent = {
        **row,
        "eligible": False,
        "wallet_cooloff_active": False,
        "fading_clear": True,
        "checks": {**row["checks"], "f3_not_enabled_or_cooloff_or_fading": False},
    }
    overlay = _active_direct_overlay_for_row(incumbent)

    unchanged, report = deadman._renew_direct_pin_from_incumbent_allpass(
        overlay=overlay,
        candidate_evidence={"rows": [incumbent]},
        now=now,
        guard={},
        ledger={"orders": []},
    )

    assert unchanged == overlay
    assert report["status"] == "DIRECT_PIN_RENEWAL_SKIPPED_EMPTY"
    assert report["accepted_orders_since_pin"] == 0


def test_order153_direct_pin_does_not_renew_when_non_f3_gate_fails() -> None:
    now = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    row = _normal_gate_unique_allpass_candidates()["rows"][0]
    incumbent = {
        **row,
        "eligible": False,
        "wallet_cooloff_active": False,
        "fading_clear": True,
        "checks": {
            **row["checks"],
            "f3_not_enabled_or_cooloff_or_fading": False,
            "f4_external_liveness": False,
        },
    }
    overlay = _active_direct_overlay_for_row(incumbent)

    unchanged, report = deadman._renew_direct_pin_from_incumbent_allpass(
        overlay=overlay,
        candidate_evidence={"rows": [incumbent]},
        now=now,
    )

    assert report["status"] == "DIRECT_PIN_RENEWAL_EVIDENCE_NOT_ALLPASS"
    assert unchanged == overlay


def test_policy_choke_rung_b_ttl_disables_and_sets_24h_cooloff() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={})
    admitted, _report = deadman._execute_policy_choke_rung_b(overlay=overlay, candidate=evidence["selected"], now=now)

    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(overlay=admitted, ledger={"orders": []}, previous_state={}, now=now + dt.timedelta(seconds=3601))

    assert lifecycle["status"] == "RUNG_C_METHOD_SWITCH_DUE"
    member = next(row for row in reconciled["members"] if row.get("source_wallet") == wallet)
    assert member["enabled"] is False
    assert member["status"] == "AUTO_DISABLED_RUNG_B_TTL"
    assert "selection_pin" not in reconciled
    assert dt.datetime.fromisoformat(cooloffs[wallet]) == now + dt.timedelta(seconds=3601 + 86400)


def test_policy_choke_rung_b_ttl_prefers_enabled_duplicate_member() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready,
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        temporal_registry={},
        cooloffs={},
    )
    admitted, _report = deadman._execute_policy_choke_rung_b(
        overlay=overlay,
        candidate=evidence["selected"],
        now=now,
    )
    enabled = admitted["members"][-1]
    admitted["members"].insert(
        1,
        {**enabled, "enabled": False, "status": "AUTO_DISABLED_RUNG_B_TTL"},
    )

    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=admitted,
        ledger={"orders": []},
        previous_state={},
        now=now + dt.timedelta(seconds=3601),
    )

    assert lifecycle["status"] == "RUNG_C_METHOD_SWITCH_DUE"
    assert lifecycle["accepted_orders_during_ttl"] == 0
    assert wallet in cooloffs
    matching = [
        row for row in reconciled["members"] if row.get("candidate_id") == enabled["candidate_id"]
    ]
    assert all(row["enabled"] is False for row in matching)


def test_policy_choke_reconcile_disables_emergency_duplicate_during_active_cooloff() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet = "0x" + "b" * 40
    overlay = {
        "members": [
            {
                "candidate_id": f"policy_choke_rung_c_{wallet[-12:]}",
                "source_wallet": wallet,
                "enabled": True,
                "status": "POLICY_CHOKE_RUNG_C_EMERGENCY_ADMISSION",
            }
        ]
    }
    previous_state = {
        "policy_choke_rung_b_cooloffs": {
            wallet: (now + dt.timedelta(hours=24)).isoformat()
        }
    }

    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=overlay,
        ledger={"orders": []},
        previous_state=previous_state,
        now=now,
    )

    assert reconciled["members"][0]["enabled"] is False
    assert reconciled["members"][0]["status"] == "AUTO_DISABLED_RUNG_B_TTL"
    assert lifecycle["status"] == "NO_ACTIVE_RUNG_B"
    assert lifecycle["cooloff_emergency_members_disabled"] is True
    assert wallet in cooloffs


def test_policy_choke_reconcile_persists_measured_loss_demotion_cooloff() -> None:
    demoted_at = dt.datetime(2026, 7, 25, 22, 38, 30, tzinfo=dt.timezone.utc)
    wallet = "0x" + "d" * 40
    overlay = {
        "latest_mechanical_temporal_loss_demotion": {
            "status": "APPLIED",
            "target_wallet": wallet,
            "generated_at": demoted_at.isoformat(),
        },
        "members": [
            {
                "candidate_id": f"policy_choke_rung_direct_{wallet[-12:]}",
                "source_wallet": wallet,
                "enabled": True,
            }
        ],
    }

    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=overlay,
        ledger={"orders": []},
        previous_state={},
        now=demoted_at + dt.timedelta(minutes=5),
    )

    assert dt.datetime.fromisoformat(cooloffs[wallet]) == demoted_at + dt.timedelta(hours=24)
    assert reconciled["members"][0]["enabled"] is False
    assert reconciled["members"][0]["status"] == "AUTO_DISABLED_RUNG_B_TTL"
    assert lifecycle["status"] == "NO_ACTIVE_RUNG_B"


def test_policy_choke_reconcile_writes_fingerprint_scoped_demotion_cooloff() -> None:
    demoted_at = dt.datetime(2026, 7, 25, 22, 38, 30, tzinfo=dt.timezone.utc)
    wallet = "0x" + "e" * 40
    fingerprint = "abcd" * 16
    overlay = {
        "latest_mechanical_temporal_loss_demotion": {
            "status": "APPLIED",
            "target_wallet": wallet,
            "generated_at": demoted_at.isoformat(),
            "wide_policy_fingerprint": fingerprint,
            "cooloff_reason": "cell_scoped_first_slice_breach",
        },
        "members": [
            {
                "candidate_id": f"policy_choke_rung_direct_{wallet[-12:]}",
                "source_wallet": wallet,
                "enabled": False,
                "policy": {"wide_policy_fingerprint": fingerprint},
            }
        ],
    }

    _reconciled, _lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=overlay,
        ledger={"orders": []},
        previous_state={},
        now=demoted_at + dt.timedelta(minutes=5),
    )

    key = f"{wallet}|{fingerprint}"
    assert cooloffs[key]["wide_policy_fingerprint"] == fingerprint
    assert cooloffs[key]["reason"] == "cell_scoped_first_slice_breach"
    assert dt.datetime.fromisoformat(cooloffs[key]["expires_at"]) == (
        demoted_at + dt.timedelta(hours=24)
    )
    assert wallet not in cooloffs


def test_policy_choke_rung_b_early_terminal_requires_ready_successor() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={})
    admitted, _report = deadman._execute_policy_choke_rung_b(overlay=overlay, candidate=evidence["selected"], now=now)
    
    # 900s pin age, zero accepts, zero fresh buys
    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=admitted,
        ledger={"orders": []},
        previous_state={},
        now=now + dt.timedelta(seconds=900),
        hot_history={"events": []},
        successor_ready=True,
    )
    
    assert lifecycle["status"] == "RUNG_C_METHOD_SWITCH_DUE"
    assert lifecycle["accepted_orders_during_ttl"] == 0
    assert wallet in cooloffs
    member = next(row for row in reconciled["members"] if row.get("source_wallet") == wallet)
    assert member["enabled"] is False
    assert member["status"] == "AUTO_DISABLED_RUNG_B_TTL"
    assert "selection_pin" not in reconciled
    assert lifecycle["reason"] == "early_ttl_equivalent_zero_accept; failed_15m_accepted_liveness; lifecycle_F2_active_market_buys=0; exact_successor_ready_for_same_heartbeat_repin"
    assert "terminal_at" in lifecycle


def test_policy_choke_rung_b_zero_f2_holds_when_frontier_is_dry() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready,
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        temporal_registry={},
        cooloffs={},
    )
    admitted, _report = deadman._execute_policy_choke_rung_b(
        overlay=overlay,
        candidate=evidence["selected"],
        now=now,
    )

    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=admitted,
        ledger={"orders": []},
        previous_state={},
        now=now + dt.timedelta(seconds=900),
        hot_history={"events": []},
        successor_ready=False,
    )

    assert lifecycle["status"] == "RUNG_B_ACTIVE"
    assert reconciled == admitted
    assert wallet not in cooloffs


def test_active_direct_pin_supersedes_stale_early_terminal_cooloff() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready,
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        temporal_registry={},
        cooloffs={},
    )
    admitted, _report = deadman._execute_policy_choke_rung_b(
        overlay=overlay,
        candidate=evidence["selected"],
        now=now,
        supply_rung="DIRECT",
    )
    previous_state = {
        "policy_choke_rung_b_cooloffs": {
            wallet: (now + dt.timedelta(hours=24)).isoformat()
        }
    }

    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=admitted,
        ledger={"orders": []},
        previous_state=previous_state,
        now=now + dt.timedelta(seconds=900),
        hot_history={"events": []},
    )

    assert lifecycle["status"] == "RUNG_B_ACTIVE"
    assert reconciled["selection_pin"]["enabled"] is True
    assert reconciled["members"][-1]["enabled"] is True
    assert wallet not in cooloffs


def test_policy_choke_rung_b_early_terminal_does_not_fire_if_accepted_gt_0() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={})
    admitted, _report = deadman._execute_policy_choke_rung_b(overlay=overlay, candidate=evidence["selected"], now=now)
    
    # 900s pin age, accepted order exists, zero fresh buys
    ledger = {
        "orders": [
            {
                "source_wallet": wallet,
                "status": "SUBMITTED",
                "submitted_at": (now + dt.timedelta(seconds=10)).isoformat(),
                "order_id": "ord123"
            }
        ]
    }
    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=admitted,
        ledger=ledger,
        previous_state={},
        now=now + dt.timedelta(seconds=900),
        hot_history={"events": []},
    )
    
    assert lifecycle["status"] == "RUNG_B_ACTIVE"
    assert "selection_pin" in reconciled
    member = next(row for row in reconciled["members"] if row.get("source_wallet") == wallet)
    assert member["enabled"] is not False


def test_policy_choke_rung_b_early_terminal_does_not_fire_if_f2_gte_10() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={})
    admitted, _report = deadman._execute_policy_choke_rung_b(overlay=overlay, candidate=evidence["selected"], now=now)
    
    # 900s pin age, zero accepts, >= 10 fresh buys
    market_start = int((now + dt.timedelta(seconds=900)).timestamp()) // 300 * 300
    active_hot = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "observed_ts": (now + dt.timedelta(seconds=900)).timestamp() - i,
                "event_id": f"e_active_{i}",
                "market_slug": f"btc-updown-5m-{market_start}",
            }
            for i in range(10)
        ]
    }
    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=admitted,
        ledger={"orders": []},
        previous_state={},
        now=now + dt.timedelta(seconds=900),
        hot_history=active_hot,
    )
    
    assert lifecycle["status"] == "RUNG_B_ACTIVE"
    assert "selection_pin" in reconciled
    member = next(row for row in reconciled["members"] if row.get("source_wallet") == wallet)
    assert member["enabled"] is not False


def test_policy_choke_rung_b_early_terminal_uses_full_lookback_not_current_window_only() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready,
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        temporal_registry={},
        cooloffs={},
    )
    admitted, _report = deadman._execute_policy_choke_rung_b(
        overlay=overlay,
        candidate=evidence["selected"],
        now=now,
    )
    terminal_check_at = now + dt.timedelta(seconds=900)
    previous_market_start = int(terminal_check_at.timestamp()) // 300 * 300 - 300
    recent_prior_window_flow = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "observed_ts": terminal_check_at.timestamp() - 60,
                "event_id": "recent-prior-window-buy",
                "market_slug": f"btc-updown-5m-{previous_market_start}",
            }
        ]
    }

    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=admitted,
        ledger={"orders": []},
        previous_state={},
        now=terminal_check_at,
        hot_history=recent_prior_window_flow,
    )

    assert lifecycle["status"] == "RUNG_B_ACTIVE"
    assert "selection_pin" in reconciled
    assert cooloffs == {}


def test_policy_choke_rung_b_early_terminal_does_not_fire_before_900s() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_b_candidate(ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={})
    admitted, _report = deadman._execute_policy_choke_rung_b(overlay=overlay, candidate=evidence["selected"], now=now)
    
    # 899s pin age, zero accepts, zero fresh buys
    reconciled, lifecycle, cooloffs = deadman._reconcile_policy_choke_rung_b(
        overlay=admitted,
        ledger={"orders": []},
        previous_state={},
        now=now + dt.timedelta(seconds=899),
        hot_history={"events": []},
    )
    
    assert lifecycle["status"] == "RUNG_B_ACTIVE"
    assert "selection_pin" in reconciled
    member = next(row for row in reconciled["members"] if row.get("source_wallet") == wallet)
    assert member["enabled"] is not False


def test_policy_choke_rung_b_cooloff_rejects_readmission() -> None:
    now = dt.datetime(2026, 7, 20, 15, 32, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)

    evidence = deadman._select_policy_choke_rung_b_candidate(ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={wallet: (now + dt.timedelta(hours=24)).isoformat()})

    assert evidence["selected"] is None
    assert evidence["rows"][0]["checks"]["f3_not_enabled_or_cooloff_or_fading"] is False


def _rung_c_packet(wallet: str) -> dict:
    return {
        "wallet": wallet,
        "candidate_id": "cohort-candidate",
        "paper_policy_id": "evidenced-policy",
        "temporal_evidence": {
            "classification": "CONTINUOUS",
            "matched_slice": {
                "slice": "weekday",
                "pnl_usd": 25.0,
                "roi_pct": 5.0,
                "resolved_trades": 250,
            },
        },
        "external_liveness": {"status": "PASS"},
        "latest_trade_age_h": 1.0,
    }


def _measured_weekday_temporal(wallet: str) -> dict:
    return {
        "criteria": {"min_trades": 5},
        "wallets": [{
            "wallet": wallet,
            "slice_labels": {
                "weekday": {
                    "label": "PROVEN-POSITIVE",
                    "resolved_trades": 250,
                    "roi_pct": 5.0,
                    "pnl_usd": 25.0,
                }
            },
        }],
    }


def test_policy_choke_rung_c_full_pool_packet_admits_and_pins() -> None:
    now = dt.datetime(2026, 7, 20, 16, 0, tzinfo=dt.timezone.utc)
    wallet, overlay, _ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_c_candidate(
        cohort_admission={"packets": [_rung_c_packet(wallet)]},
        full_pool_queue={"ranked_members": []},
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        cooloffs={},
        temporal_registry=_measured_weekday_temporal(wallet),
    )

    admitted, report = deadman._execute_policy_choke_rung_b(
        overlay=overlay,
        candidate=evidence["selected"],
        now=now,
        supply_rung="C",
    )

    assert evidence["status"] == "RUNG_C_FULL_POOL_SWEEP"
    assert evidence["rows"][0]["f1_slice_basis"]["slice"] == "full_stream"
    assert evidence["rows"][0]["f1_slice_basis"]["resolved_signals"] == 250
    assert "f1_slice_basis" not in evidence["rows"][0]["checks"]
    assert "admissible_slices" not in evidence["rows"][0]["checks"]
    assert report["status"] == "RUNG_C_FULL_POOL_SWEEP_ADMISSION_PIN_WRITTEN"
    assert report["member"]["source_wallet"] == wallet
    assert report["member"]["copy_size_usd"] == 1.0
    assert admitted["selection_pin"]["pin_id"] == deadman.POLICY_CHOKE_RUNG_B_PIN_ID


def test_policy_choke_rung_c_does_not_treat_unscoped_probe_count_as_active_market_flow() -> None:
    now = dt.datetime(2026, 7, 20, 16, 0, tzinfo=dt.timezone.utc)
    wallet, overlay, _ready, _hot = _rung_b_fixture(now)
    packet = _rung_c_packet(wallet)
    packet["fresh_own_source_buy_rows_30m"] = 1

    evidence = deadman._select_policy_choke_rung_c_candidate(
        cohort_admission={"packets": [packet]},
        full_pool_queue={"ranked_members": []},
        overlay=overlay,
        hot_history={"events": []},
        now=now,
        regime="weekday",
        cooloffs={},
        temporal_registry=_measured_weekday_temporal(wallet),
    )

    assert evidence["rows"][0]["fresh_own_source_buy_rows_30m"] == 0
    assert evidence["rows"][0]["claimed_fresh_own_source_buy_rows_30m"] == 1
    assert evidence["rows"][0]["checks"]["f2_fresh_rows_and_own_policy_copyable"] is False


def test_policy_choke_rung_c_full_pool_queue_uses_stamped_external_liveness() -> None:
    now = dt.datetime(2026, 7, 20, 16, 0, tzinfo=dt.timezone.utc)
    wallet, overlay, _ready, hot = _rung_b_fixture(now)
    queue_row = {
        "wallet": wallet,
        "candidate_id": "queue-candidate",
        "paper_policy_id": "evidenced-policy",
        "retrospective_gross_pnl_usd": 25.0,
        "retrospective_gross_roi_pct": 5.0,
        "retrospective_resolved_signals": 250,
        "fading_clear": True,
        "external_liveness_status": "PASS",
        "external_latest_trade_age_h": 2.8,
        "external_liveness_probe": {
            "source": "queue_remote_dataapi_fresh_flow_probe",
            "probe_row_status": "PASS",
        },
    }

    evidence = deadman._select_policy_choke_rung_c_candidate(
        cohort_admission={"packets": []},
        full_pool_queue={"ranked_members": [queue_row]},
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        cooloffs={},
        temporal_registry=_measured_weekday_temporal(wallet),
    )

    assert evidence["selected"]["wallet"] == wallet
    assert evidence["selected"]["supply_source"] == "full_pool_member_queue"
    assert evidence["selected"]["checks"]["f4_external_liveness"] is True
    assert evidence["refusal_counts"]["f4_external_liveness"] == 0


def test_policy_choke_rung_c_reads_negative_and_missing_temporal_cells() -> None:
    now = dt.datetime(2026, 7, 20, 16, 0, tzinfo=dt.timezone.utc)
    negative = "0x" + "d" * 40
    missing = "0x" + "e" * 40
    _wallet, overlay, _ready, hot = _rung_b_fixture(now)
    hot["events"].extend(
        {
            "source_wallet": wallet,
            "action": "BUY",
            "observed_ts": now.timestamp() - index,
            "event_id": f"{wallet}-{index}",
        }
        for wallet in (negative, missing)
        for index in range(10)
    )
    evidence = deadman._select_policy_choke_rung_c_candidate(
        cohort_admission={
            "packets": [_rung_c_packet(negative), _rung_c_packet(missing)]
        },
        full_pool_queue={"ranked_members": []},
        overlay=overlay,
        hot_history=hot,
        now=now,
        regime="weekday",
        cooloffs={},
        temporal_registry={
            "criteria": {"min_trades": 5},
            "wallets": [
                {
                    "wallet": negative,
                    "slice_labels": {
                        "weekday": {
                            "label": "PROVEN-NEGATIVE",
                            "resolved_trades": 250,
                            "roi_pct": -5.0,
                            "pnl_usd": -25.0,
                        }
                    },
                }
            ],
        },
    )

    rows = {row["wallet"]: row for row in evidence["rows"]}
    assert rows[negative]["checks"]["active_temporal_not_proven_negative"] is False
    assert rows[negative]["checks"]["active_temporal_regime_cell_measured"] is True
    assert rows[missing]["checks"]["active_temporal_not_proven_negative"] is True
    assert rows[missing]["checks"]["active_temporal_regime_cell_measured"] is False
    assert evidence["refusal_counts"]["active_temporal_not_proven_negative"] == 1
    assert evidence["refusal_counts"]["active_temporal_regime_cell_measured"] == 1


def test_policy_choke_fire_drill_reports_full_pool_queue_liveness(tmp_path: Path) -> None:
    now = dt.datetime(2026, 7, 7, 22, 0, tzinfo=dt.timezone.utc)
    wallet = "0x" + "d" * 40
    cohort_wallet = "0x" + "e" * 40
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": now.isoformat()}]},
    )
    _write_json(tmp_path / "guard.json", {"summary": {"can_trade": True}})
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_json(
        tmp_path / "data/research/wallet_copy_live_guard_hot_history_state.json",
        {
            "events": [
                {
                    "source_wallet": wallet,
                    "action": "BUY",
                    "observed_ts": now.timestamp() - index,
                    "event_id": f"q{index}",
                    "market_slug": f"btc-updown-5m-{int(now.timestamp()) // 300 * 300}",
                }
                for index in range(10)
            ]
            + [
                {
                    "source_wallet": cohort_wallet,
                    "action": "BUY",
                    "observed_ts": now.timestamp() - index,
                    "event_id": f"c{index}",
                    "market_slug": f"btc-updown-5m-{int(now.timestamp()) // 300 * 300}",
                }
                for index in range(3)
            ]
        },
    )
    _write_json(
        tmp_path / "data/research/wallet_copy_full_pool_member_queue.json",
        {
            "ranked_members": [
                {
                    "wallet": wallet,
                    "candidate_id": "queue-candidate",
                    "paper_policy_id": "evidenced-policy",
                    "retrospective_gross_pnl_usd": 25.0,
                    "retrospective_gross_roi_pct": 5.0,
                    "retrospective_resolved_signals": 250,
                    "fading_clear": True,
                    "external_liveness_status": "PASS",
                    "external_latest_trade_age_h": 2.8,
                    "external_liveness_probe": {
                        "source": "queue_remote_dataapi_fresh_flow_probe",
                        "probe_row_status": "PASS",
                    },
                }
            ]
        },
    )
    _write_json(
        tmp_path / "data/research/cohort_alive_admission_packets_latest.json",
        {
            "packets": [
                {
                    "wallet": cohort_wallet,
                    "candidate_id": "registry-fresh-weekday",
                    "paper_policy_id": "evidenced-policy",
                    "temporal_evidence": {
                        "classification": "WEEKDAY-ONLY",
                        "matched_slice": {
                            "slice": "weekday",
                            "pnl_usd": 30.0,
                            "roi_pct": 6.0,
                            "resolved_trades": 300,
                        },
                    },
                    "external_liveness": {"status": "PASS"},
                    "latest_trade_age_h": 1.2,
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            ]
        },
    )
    _write_json(
        tmp_path / "data/research/wallet_temporal_profitability_latest.json",
        {
            "criteria": {"min_trades": 5},
            "wallets": [
                {
                    "wallet": wallet,
                    "slice_labels": {
                        "weekday": {
                            "label": "PROVEN-POSITIVE",
                            "resolved_trades": 250,
                            "pnl_usd": 25.0,
                            "roi_pct": 5.0,
                        }
                    },
                },
                {
                    "wallet": cohort_wallet,
                    "slice_labels": {
                        "weekday": {
                            "label": "PROVEN-POSITIVE",
                            "resolved_trades": 300,
                            "pnl_usd": 30.0,
                            "roi_pct": 6.0,
                        }
                    },
                },
            ],
        },
    )

    _run_deadman(tmp_path)

    fire_drill = json.loads(
        (tmp_path / "data/research/order_flow_deadman_policy_choke_fire_drill_latest.json").read_text(
            encoding="utf-8"
        )
    )
    drill = fire_drill["rung_c_full_pool_liveness_drill"]
    assert fire_drill["gate_verdict"] == "PASS"
    assert fire_drill["verdict"] == "PASS"
    assert drill["status"] == "PASS"
    assert drill["rung_c_candidate_count"] == 2
    assert drill["fresh_own_source_positive_rows"] == 2
    assert drill["f4_external_liveness_true_queue_rows"] == 1
    assert drill["sample_rows"][0]["supply_source"] == "full_pool_member_queue"
    assert drill["sample_rows"][0]["checks"]["f4_external_liveness"] is True
    assert {
        row["supply_source"] for row in drill["fresh_own_source_positive_sample_rows"]
    } == {"cohort_alive_admission_packets", "full_pool_member_queue"}


def test_policy_choke_fire_drill_distinguishes_gate_failure_from_terminal_skip() -> None:
    gate_verdict, verdict = deadman._policy_choke_fire_drill_verdict(
        rung_a_gate_pass=True,
        rung_b_gate_pass=False,
        queue_gate_pass=True,
        rung_c_settlement={"status": "SATISFIED_BY_RECORDED_DECISION"},
    )
    assert gate_verdict == "GATE_BROKEN"
    assert verdict == "GATE_BROKEN"

    gate_verdict, verdict = deadman._policy_choke_fire_drill_verdict(
        rung_a_gate_pass=True,
        rung_b_gate_pass=True,
        queue_gate_pass=True,
        rung_c_settlement={"status": "SATISFIED_BY_RECORDED_DECISION"},
    )
    assert gate_verdict == "PASS"
    assert verdict == "INAPPLICABLE_TERMINAL_SETTLEMENT"


def test_policy_choke_rung_c_all_fail_has_terminal_refusal_counts() -> None:
    now = dt.datetime(2026, 7, 20, 16, 0, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, _hot = _rung_b_fixture(now, fresh_rows=0)
    rung_b = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready, overlay=overlay, hot_history={"events": []}, now=now, regime="weekday", temporal_registry={}, cooloffs={}
    )
    rung_c = deadman._select_policy_choke_rung_c_candidate(
        cohort_admission={"packets": [_rung_c_packet(wallet)]},
        full_pool_queue={"ranked_members": []},
        overlay=overlay,
        hot_history={"events": []},
        now=now,
        regime="weekday",
        cooloffs={},
        temporal_registry=_measured_weekday_temporal(wallet),
    )

    report = deadman._policy_choke_rung_c_terminal(rung_c, rung_b)

    assert rung_c["eligible_count"] == 0
    assert report["status"] == "RUNG_C_NO_ADMISSIBLE_TARGET"
    assert report["terminal_outcome"] is True
    assert report["refusal_counts"]["f2_fresh_rows_and_own_policy_copyable"] == 1


def test_policy_choke_terminal_reconciliation_accounts_closed_and_current_rows() -> None:
    wallet = "0x" + "a" * 40
    now = dt.datetime.fromtimestamp(1_800_000_150, dt.timezone.utc)
    closed_start = 1_799_999_700
    current_start = 1_800_000_000
    future_start = 1_800_000_300
    guard = {
        "active_set_runtime": {
            "members": [{"source_wallet": wallet, "enabled": True}]
        },
        "window_participation": {
            "rows": [
                {
                    "source_wallet": wallet,
                    "market_slug": f"btc-updown-5m-{current_start}",
                    "outcome": "Up",
                    "dominant_skip_reason": "inventory_best_ask_missing",
                }
            ]
        },
    }
    hot_history = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "event_id": "closed",
                "event_ts": closed_start + 250,
                "observed_ts": now.timestamp() - 100,
                "api_latency_s": 100,
                "market_slug": f"btc-updown-5m-{closed_start}",
                "outcome": "Up",
            },
            {
                "source_wallet": wallet,
                "action": "BUY",
                "event_id": "current",
                "event_ts": current_start + 130,
                "observed_ts": now.timestamp() - 10,
                "api_latency_s": 10,
                "market_slug": f"btc-updown-5m-{current_start}",
                "outcome": "Up",
            },
            {
                "source_wallet": wallet,
                "action": "BUY",
                "event_id": "future",
                "event_ts": future_start - 10,
                "observed_ts": now.timestamp() - 5,
                "api_latency_s": 5,
                "market_slug": f"btc-updown-5m-{future_start}",
                "outcome": "Up",
            },
        ]
    }
    report = deadman._policy_choke_terminal_reconciliation(
        guard=guard,
        hot_history=hot_history,
        routing_shadow={},
        ledger={},
        now=now,
    )
    assert report["source_coverage_full"] is True
    assert report["input_equals_terminal_rows"] is True
    assert report["input_fresh_source_rows"] == 3
    assert report["terminal_rows"] == 3
    assert report["terminal_stage_counts"] == {
        "candidate_not_built": 1,
        "freshness_market_closed_before_evaluation": 1,
        "inventory_gate": 1,
    }
    assert report["live_feedstock_rows"] == 2
    assert report["liveness_only_rows"] == 1
    assert report["per_wallet_live_feedstock_rows"] == {wallet: 2}
    assert report["terminal_phase_cross_tab"]["rows"] == [
        {
            "source_wallet": wallet,
            "terminal_stage": "candidate_not_built",
            "source_event_inside_market_window": "no",
            "api_latency_lte_30s": "yes",
            "rows": 1,
        },
        {
            "source_wallet": wallet,
            "terminal_stage": "freshness_market_closed_before_evaluation",
            "source_event_inside_market_window": "yes",
            "api_latency_lte_30s": "no",
            "rows": 1,
        },
        {
            "source_wallet": wallet,
            "terminal_stage": "inventory_gate",
            "source_event_inside_market_window": "yes",
            "api_latency_lte_30s": "yes",
            "rows": 1,
        },
    ]
    assert report["deferred_re_evaluation_at_open"] == {
        "total_rows": 0,
        "per_wallet_rows": {},
        "c539_rows": 0,
        "re_evaluated_rows": 0,
        "re_evaluated_per_wallet_rows": {},
        "cycle_rescuable": False,
        "next_mechanism": "retain pre-open rows and re-evaluate when now >= market_start",
        "rule": (
            "freshness_market_not_open_yet is retained only until market open; "
            "the first cut at now >= market_start re-runs normal accepted, attempted, "
            "participation, routing, and freshness terminal attribution"
        ),
    }


def test_policy_choke_re_evaluates_preopen_row_at_market_open() -> None:
    now = dt.datetime(2026, 7, 30, 2, 12, 30, tzinfo=dt.timezone.utc)
    market_start = int(now.timestamp() // 300) * 300
    wallet = "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"
    market_slug = f"btc-updown-5m-{market_start}"
    guard = {
        "active_set_runtime": {
            "members": [{"source_wallet": wallet, "enabled": True}]
        },
        "window_participation": {
            "rows": [
                {
                    "source_wallet": wallet,
                    "market_slug": market_slug,
                    "outcome": "UP",
                    "dominant_skip_reason": "inventory_target_already_met",
                }
            ]
        },
    }
    hot_history = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "event_id": "preopen-c539",
                "event_ts": market_start - 2,
                "observed_ts": market_start - 1,
                "api_latency_s": 1,
                "market_slug": market_slug,
                "outcome": "Up",
            }
        ]
    }

    report = deadman._policy_choke_terminal_reconciliation(
        guard=guard,
        hot_history=hot_history,
        routing_shadow={},
        ledger={},
        now=now,
    )

    assert report["terminal_stage_counts"] == {"inventory_gate": 1}
    assert report["live_feedstock_rows"] == 1
    assert report["per_wallet_live_feedstock_rows"] == {wallet: 1}
    assert report["deferred_re_evaluation_at_open"]["total_rows"] == 0
    assert report["deferred_re_evaluation_at_open"]["re_evaluated_rows"] == 1
    assert report["deferred_re_evaluation_at_open"][
        "re_evaluated_per_wallet_rows"
    ] == {wallet: 1}


def test_policy_choke_re_evaluated_preopen_without_candidate_is_candidate_not_built() -> None:
    market_start = 1_785_477_900
    now = dt.datetime.fromtimestamp(market_start + 301, dt.timezone.utc)
    wallet = "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"
    market_slug = f"btc-updown-5m-{market_start}"
    guard = {"active_set_runtime": {"members": [{"source_wallet": wallet, "enabled": True}]}}
    hot_history = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "event_id": "preopen-no-candidate",
                "event_ts": market_start - 110,
                "observed_ts": market_start - 105,
                "api_latency_s": 5,
                "market_slug": market_slug,
                "outcome": "Up",
            }
        ]
    }

    report = deadman._policy_choke_terminal_reconciliation(
        guard=guard,
        hot_history=hot_history,
        routing_shadow={},
        ledger={},
        now=now,
    )

    assert report["terminal_stage_counts"] == {"candidate_not_built": 1}
    assert report["live_feedstock_rows"] == 1
    assert report["liveness_only_rows"] == 0
    assert report["deferred_re_evaluation_at_open"]["re_evaluated_rows"] == 1


def test_rung_a_candidate_reconciliation_names_positive_preopen_exclusion() -> None:
    wallet = "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"
    report = deadman._rung_a_candidate_reconciliation(
        rung_a={
            "action": "NO_RUNG_A_TARGET",
            "rows": [
                {
                    "wallet": wallet,
                    "regime_slice_label": "PROVEN-POSITIVE",
                    "policy_eligible_intents": 1,
                }
            ],
        },
        candidates={"candidate_count": 12, "eligible_count": 0, "nearest_frontier": []},
        terminal_reconciliation={
            "per_wallet_live_feedstock_rows": {wallet: 0},
            "terminal_phase_cross_tab": {
                "rows": [
                    {
                        "source_wallet": wallet,
                        "terminal_stage": "freshness_market_not_open_yet",
                        "source_event_inside_market_window": "no",
                        "api_latency_lte_30s": "yes",
                        "rows": 5,
                    }
                ]
            }
        },
    )

    assert (
        report["status"]
        == "UNRECONCILED_NO_RUNG_A_TARGET_POSITIVE_LIVENESS_ONLY_EXCLUSION"
    )
    assert report["positive_liveness_only_excluded_wallets"] == [wallet]
    assert report["rows"][0]["excluded_by_liveness_only_feedstock_gate"] is True
    assert report["next_mechanism"].startswith("re-evaluate pre-open rows")


def test_rung_a_candidate_reconciliation_releases_unrescuable_exclusion() -> None:
    wallet = "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"
    report = deadman._rung_a_candidate_reconciliation(
        rung_a={
            "action": "NO_RUNG_A_TARGET",
            "rows": [
                {
                    "wallet": wallet,
                    "regime_slice_label": "PROVEN-POSITIVE",
                    "policy_eligible_intents": 1,
                }
            ],
        },
        candidates={"candidate_count": 12, "eligible_count": 0, "nearest_frontier": []},
        terminal_reconciliation={
            "per_wallet_live_feedstock_rows": {wallet: 0},
            "terminal_phase_cross_tab": {
                "rows": [
                    {
                        "source_wallet": wallet,
                        "terminal_stage": "freshness_market_closed_before_evaluation",
                        "source_event_inside_market_window": "no",
                        "api_latency_lte_30s": "yes",
                        "rows": 1,
                    },
                    {
                        "source_wallet": wallet,
                        "terminal_stage": "freshness_source_too_stale",
                        "source_event_inside_market_window": "no",
                        "api_latency_lte_30s": "no",
                        "rows": 1,
                    },
                ]
            },
        },
    )

    assert report["status"] == "RECONCILED_WITH_NAMED_UNRESCUABLE_EXCLUSION"
    assert report["rung_a_action"] == "NO_RUNG_A_TARGET"
    assert report["rescuable_excluded_wallets"] == []
    assert report["unrescuable_excluded_wallets"] == [wallet]
    assert (
        report["rows"][0]["classification"]
        == "PROVEN_POSITIVE_EXCLUDED_UNRESCUABLE_AT_CUT"
    )
    assert report["next_mechanism"].startswith("raise source-side row supply")


def test_rung_a_candidate_reconciliation_names_no_policy_incumbent() -> None:
    wallet = "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"
    report = deadman._rung_a_candidate_reconciliation(
        rung_a={
            "action": "NO_RUNG_A_TARGET",
            "rows": [
                {
                    "wallet": wallet,
                    "regime_slice_label": "PROVEN-POSITIVE",
                    "policy_eligible_intents": 0,
                }
            ],
        },
        candidates={"candidate_count": 10, "eligible_count": 0, "nearest_frontier": []},
        terminal_reconciliation={
            "per_wallet_live_feedstock_rows": {wallet: 0},
            "terminal_phase_cross_tab": {
                "rows": [
                    {
                        "source_wallet": wallet,
                        "terminal_stage": "candidate_not_built",
                        "source_event_inside_market_window": "no",
                        "api_latency_lte_30s": "yes",
                        "rows": 7,
                    }
                ]
            },
        },
    )

    assert report["status"] == "RECONCILED_WITH_NO_EVIDENCED_POLICY_FOR_INCUMBENT"
    assert report["no_evidenced_policy_incumbent_wallets"] == [wallet]
    assert report["rows"][0]["classification"] == "NO_EVIDENCED_POLICY_FOR_INCUMBENT"
    assert report["rows"][0]["excluded_by_liveness_only_feedstock_gate"] is False
    assert report["next_mechanism"] == "NO_EVIDENCED_POLICY_FOR_INCUMBENT"


def test_standby_exclusions_fail_closed_for_permanent_status_stop_writer_and_stale_source() -> None:
    now = dt.datetime(2026, 7, 31, 6, 52, tzinfo=dt.timezone.utc)
    status_wallet = "0x1111111111111111111111111111111111111111"
    stop_wallet = "0x2222222222222222222222222222222222222222"
    stale_wallet = "0x3333333333333333333333333333333333333333"
    exclusions = deadman._standby_wallet_exclusions(
        {
            "generated_at": "2026-07-30T23:30:45Z",
            "standby_ready": {
                "status_path": {
                    "wallet": status_wallet,
                    "status": "UNFED_CLOCK_CANNOT_MATURE",
                    "terminal_decision": None,
                },
                "stop_path": {
                    "wallet": stop_wallet,
                    "status": "PASS",
                    "binding": {"terminal_outcome_on_deadline": {"stop_writer": True}},
                },
                "stale_path": {"wallet": stale_wallet, "status": "PASS"},
            },
        },
        now=now,
    )

    permanent = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    assert exclusions[permanent]["permanent_park"] is True
    assert exclusions[status_wallet]["reason"] == "terminal_park"
    assert exclusions[stop_wallet]["stop_writer"] is True
    assert exclusions[stale_wallet]["reason"] == "stale_exclusion_source"
    assert exclusions[stale_wallet]["standby_exclusion_source_age_s"] > 1800
    assert (
        exclusions[stale_wallet]["standby_exclusion_source_status"]
        == "UNRECONCILED_STALE_EXCLUSION_SOURCE"
    )


def test_policy_choke_local_skip_accepts_local_refusal_provenance() -> None:
    report = deadman._classify_policy_choke_local_skip(
        policy_choke={
            "reject_taxonomy": {"maker_min_share_bump_exceeds_policy_cap": 2},
            "fak_no_match_outcomes": 0,
            "whole_runtime_accepted_orders": 0,
        },
        reconciliation={
            "live_feedstock_rows": 305,
            "terminal_stage_counts": {
                "inventory_gate": 183,
                "price_or_band_gate": 16,
                "copyintent_policy:cap": 22,
            },
        },
        can_trade=True,
        guard_status="LIVE_GUARD_RUNNING",
    )

    assert report["qualifies"] is True
    assert report["deadman_class"] == "POLICY_CHOKE_LOCAL_SKIP"
    assert report["local_terminal_share"] == 0.731148
    assert report["sizing_only_rejects"] is True
    assert report["mechanical_escalation"] == "LOCAL_POLICY_SIZING_CHOKE_DUE"


def test_policy_choke_band_holdout_is_measured_skip_without_sizing_escalation() -> None:
    report = deadman._classify_policy_choke_local_skip(
        policy_choke={
            "reject_taxonomy": {"entry_price_band_closed_negative_holdout": 3},
            "fak_no_match_outcomes": 0,
            "whole_runtime_accepted_orders": 0,
        },
        reconciliation={
            "live_feedstock_rows": 10,
            "terminal_stage_counts": {"inventory_gate": 6},
        },
        can_trade=True,
        guard_status="LIVE_GUARD_RUNNING",
    )

    assert report["qualifies"] is True
    assert report["deadman_class"] == "MEASURED_SKIP_GATED_QUIET"
    assert report["sizing_only_rejects"] is False
    assert report["mechanical_escalation"] == "NONE"


def test_policy_choke_empty_reject_set_is_not_local_refusal_evidence() -> None:
    report = deadman._classify_policy_choke_local_skip(
        policy_choke={
            "reject_taxonomy": {},
            "fak_no_match_outcomes": 0,
            "whole_runtime_accepted_orders": 0,
        },
        reconciliation={
            "live_feedstock_rows": 10,
            "terminal_stage_counts": {"inventory_gate": 9},
        },
        can_trade=True,
        guard_status="LIVE_GUARD_RUNNING",
    )

    assert report["qualifies"] is False
    assert report["explicit_local_refusal_evidence"] is False
    assert report["local_only_rejects"] is False


def test_policy_choke_successful_acceptance_is_flow_alive_not_dead() -> None:
    report = deadman._classify_policy_choke_local_skip(
        policy_choke={
            "reject_taxonomy": {"maker_min_share_bump_exceeds_policy_cap": 2},
            "fak_no_match_outcomes": 0,
            "whole_runtime_accepted_orders": 2,
        },
        reconciliation={
            "live_feedstock_rows": 367,
            "terminal_stage_counts": {"inventory_gate": 226},
        },
        can_trade=True,
        guard_status="LIVE_GUARD_RUNNING",
    )

    assert report["qualifies"] is False
    assert report["deadman_class"] == "FLOW_ALIVE_ACCEPTED_ORDERS"
    assert report["accepted_orders"] == 2
    assert report["whole_runtime_accepted_orders"] == 2


def test_policy_choke_method_acceptance_is_alive_without_local_refusal_terms() -> None:
    report = deadman._classify_policy_choke_local_skip(
        policy_choke={
            "reject_taxonomy": {"post_only_crosses_book": 3},
            "fak_no_match_outcomes": 0,
            "whole_runtime_accepted_orders": 0,
            "method_accepted_orders": 2,
            "selected_accepted_orders": 1,
        },
        reconciliation={
            "live_feedstock_rows": 367,
            "terminal_stage_counts": {"inventory_gate": 10},
        },
        can_trade=True,
        guard_status="LIVE_GUARD_RUNNING",
    )

    assert report["deadman_class"] == "FLOW_ALIVE_ACCEPTED_ORDERS"
    assert report["pipe_healthy_terms"] is True
    assert report["local_refusal_terms"] is False
    assert report["accepted_orders"] == 2
    assert report["accepted_order_counters"] == {
        "whole_runtime_accepted_orders": 0,
        "method_accepted_orders": 2,
        "selected_accepted_orders": 1,
        "chosen_accepted_orders": 2,
        "chosen_rule": "max(whole_runtime, method, selected)",
    }


def test_gated_quiet_local_reject_is_approved_but_inflight_is_excluded() -> None:
    taxonomy, summary = deadman._normalized_gated_quiet_taxonomy(
        {
            "live_order_rejected": 2,
            "ready_to_submit": 1,
            "window:ready_to_submit": 1,
        },
        {"orders": []},
        latest_order=None,
    )

    assert taxonomy == {"live_order_rejected": 2}
    assert summary["inflight_ready_to_submit_excluded"] == 2
    assert deadman._unapproved_gated_quiet_reasons(
        taxonomy,
        local_only_rejects=True,
    ) == []
    assert deadman._unapproved_gated_quiet_reasons(taxonomy) == [
        "live_order_rejected"
    ]
    assert deadman._unapproved_gated_quiet_reasons(
        {
            "maker_min_share_bump_exceeds_policy_cap": 2,
            "window:live_order_rejected": 2,
        },
        local_only_rejects=True,
    ) == []


def test_ruled_floor_reasons_are_approved_gated_quiet() -> None:
    taxonomy = {
        "below_ruled_entry_floor": 1,
        "inventory_best_ask_above_limit_passive_lane_sealed": 1,
        "inventory_best_ask_below_ruled_entry_floor": 1,
        "window:below_ruled_entry_floor": 1,
    }

    assert deadman._unapproved_gated_quiet_reasons(taxonomy) == []
    assert deadman._taxonomy_reason_class(
        "below_ruled_entry_floor"
    ) == "approved_gated_quiet_taxonomy"
    assert deadman._taxonomy_reason_class(
        "inventory_best_ask_above_limit_passive_lane_sealed"
    ) == "approved_gated_quiet_taxonomy"
    assert deadman._taxonomy_reason_class(
        "inventory_best_ask_below_ruled_entry_floor"
    ) == "approved_gated_quiet_taxonomy"


def test_stale_book_at_gate_is_never_approved_quiet_even_window_prefixed() -> None:
    """A stale gate probe is missing evidence, not a ruled refusal: it must stay red."""
    taxonomy = {"stale_book_at_gate": 2, "window:stale_book_at_gate": 2}

    assert deadman._unapproved_gated_quiet_reasons(taxonomy) == [
        "stale_book_at_gate",
        "window:stale_book_at_gate",
    ]
    for reason in ("stale_book_at_gate", "window:stale_book_at_gate"):
        assert deadman._taxonomy_reason_class(
            reason
        ) == "gate_evidence_stale_requires_incident_attribution"


def test_gated_quiet_accepted_orders_are_flow_alive() -> None:
    now = dt.datetime(2026, 8, 2, 16, 27, tzinfo=dt.timezone.utc)
    report = deadman._gated_quiet_classification(
        guard={
            "drought_funnel": {
                "reject_taxonomy_counts": {
                    "maker_min_share_bump_exceeds_policy_cap": 2,
                }
            }
        },
        active_set_state={},
        ledger={"orders": []},
        now=now,
        idle_s=434.0,
        max_idle_s=1800.0,
        latest_order=now - dt.timedelta(seconds=434),
        eligible_drought_status="OK",
        latest_suppression=None,
        local_only_rejects=True,
        accepted_orders=2,
        pipe_healthy_terms=True,
    )

    assert report["status"] == "PASS"
    assert report["classification"] == "FLOW_ALIVE_ACCEPTED_ORDERS"
    assert report["accepted_orders"] == 2
    assert report["pipe_healthy_terms"] is True


def test_named_passive_lane_closure_is_local_pre_submit_refusal() -> None:
    row = {
        "final_status": "REJECTED",
        "trade_result": {
            "order_id": "",
            "error_class": "passive_at_source_lane_closed",
        },
    }

    assert deadman._classified_reject_reason(row) == "passive_at_source_lane_closed"
    assert "passive_at_source_lane_closed" in deadman.LOCAL_REFUSAL_REJECT_CLASSES


def test_expired_qualifying_local_skip_names_sizing_choke_not_reload() -> None:
    disposition = deadman._accepted_order_deadman_disposition(
        raw_firing=True,
        local_skip_qualifies=True,
        ruled_posture_exemption=False,
    )

    assert disposition == {
        "status": "INCIDENT_LOCAL_POLICY_SKIP",
        "deadman_class": "POLICY_CHOKE_LOCAL_SKIP",
        "mechanical_escalation": "LOCAL_POLICY_SIZING_CHOKE_DUE",
    }


def test_expired_measured_band_skip_never_names_sizing_choke() -> None:
    disposition = deadman._accepted_order_deadman_disposition(
        raw_firing=True,
        local_skip_qualifies=True,
        ruled_posture_exemption=False,
        local_skip_deadman_class="MEASURED_SKIP_GATED_QUIET",
        local_skip_mechanical_escalation="NONE",
    )

    assert disposition == {
        "status": "INCIDENT_MEASURED_SKIP_GATED_QUIET",
        "deadman_class": "MEASURED_SKIP_GATED_QUIET",
        "mechanical_escalation": "NONE",
    }


def test_gated_quiet_taxonomy_deduplicates_exact_window_alias_pairs() -> None:
    normalized = deadman._deduplicated_window_alias_taxonomy(
        {
            "inventory_confirmed_unchanged_no_edge": 12,
            "window:inventory_confirmed_unchanged_no_edge": 12,
            "inventory_late_window_guard": 7,
            "window:inventory_late_window_guard": 6,
        }
    )

    assert normalized == {
        "inventory_confirmed_unchanged_no_edge": 12,
        "inventory_late_window_guard": 7,
        "window:inventory_late_window_guard": 6,
    }


def test_nonqualifying_deadman_reports_no_lawful_actuation() -> None:
    disposition = deadman._accepted_order_deadman_disposition(
        raw_firing=True,
        local_skip_qualifies=False,
        ruled_posture_exemption=False,
    )

    assert disposition == {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "deadman_class": "ORDER_FLOW_DEAD",
        "mechanical_escalation": "NO_LAWFUL_ACTUATION",
    }


def test_selected_identity_pending_adoption_authorizes_managed_turnover() -> None:
    disposition = deadman._accepted_order_deadman_disposition(
        raw_firing=True,
        local_skip_qualifies=False,
        ruled_posture_exemption=False,
        selection_pending_adoption=True,
    )

    assert disposition == {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "deadman_class": "ORDER_FLOW_DEAD",
        "mechanical_escalation": "MANAGED_RESTART_SELECTION_PENDING_ADOPTION",
    }


def test_no_admissible_target_never_authorizes_restart() -> None:
    disposition = deadman._accepted_order_deadman_disposition(
        raw_firing=True,
        local_skip_qualifies=False,
        ruled_posture_exemption=False,
        selection_pending_adoption=True,
        wallet_policy_diagnostic="WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET",
        measured_empty_seat_no_target=True,
        selected_eligible_intents=0,
        selected_guard_submit_attempts=0,
    )
    assert disposition == {
        "status": "MEASURED_SKIP_CORRECTLY_IDLE",
        "deadman_class": "MEASURED_NO_ADMISSIBLE_TARGET",
        "mechanical_escalation": "NONE",
    }


def test_no_target_headline_cannot_hide_unexplained_eligible_intent() -> None:
    disposition = deadman._accepted_order_deadman_disposition(
        raw_firing=True,
        local_skip_qualifies=False,
        ruled_posture_exemption=False,
        selection_pending_adoption=False,
        wallet_policy_diagnostic="WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET",
        measured_empty_seat_no_target=True,
        selected_eligible_intents=1,
        selected_guard_submit_attempts=0,
    )

    assert disposition == {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "deadman_class": "ORDER_FLOW_DEAD",
        "mechanical_escalation": "NO_LAWFUL_ACTUATION",
    }
    assert deadman._accepted_disposition_outranks_policy_choke(
        disposition,
        {
            "selected_eligible_intents": 1,
            "selected_guard_submit_attempts": 0,
        },
    ) is True


def test_order_flow_disposition_does_not_outrank_policy_choke_without_gap() -> None:
    disposition = {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "deadman_class": "ORDER_FLOW_DEAD",
        "mechanical_escalation": "NO_LAWFUL_ACTUATION",
    }

    assert deadman._accepted_disposition_outranks_policy_choke(
        disposition,
        {
            "selected_eligible_intents": 0,
            "selected_guard_submit_attempts": 0,
        },
    ) is False


def test_paper_routing_shadow_trace_nulls_unexplained_when_not_comparable() -> None:
    wallet = "0x" + "4" * 40
    now = dt.datetime(2026, 8, 4, 16, 12, 34, tzinfo=dt.timezone.utc)
    since = now - dt.timedelta(minutes=30)
    seat_epoch = dt.datetime(2026, 8, 4, 15, 58, 51, tzinfo=dt.timezone.utc)
    routing_shadow = {
        "fee_gated_measurement_rows": [
            {
                "intent_id": "ci_before",
                "source_wallet": wallet,
                "dominant_skip_reason": "eligible",
                "observed_ts": seat_epoch.timestamp() - 60,
                "market_slug": "btc-updown-5m-1",
            },
            {
                "intent_id": "ci_after",
                "source_wallet": wallet,
                "dominant_skip_reason": "eligible",
                "observed_ts": seat_epoch.timestamp() + 60,
                "market_slug": "btc-updown-5m-2",
            },
        ]
    }

    trace = deadman._paper_routing_shadow_intent_trace(
        routing_shadow=routing_shadow,
        ledger={"orders": []},
        selected_wallet=wallet,
        since=since,
        until=now,
        selected_seat_epoch_at=seat_epoch,
        authority_counter=2,
    )

    assert trace["window_alignment"]["status"] == (
        "NOT_COMPARABLE_LOOKBACK_PREDATES_SEAT_EPOCH"
    )
    assert trace["traced_population_lane"] == "paper_only"
    assert trace["traced_population_live_orders_allowed"] is False
    assert trace["authority_counter_source"] == "policy_choke.selected_eligible_intents"
    assert trace["authority_counter"] == 2
    assert trace["population_parity"] == "NOT_COMPARABLE"
    assert trace["unexplained_eligible_intents"] is None
    assert trace["post_seat_unexplained_eligible_intents"] is None
    assert trace["unexplained_status"] == "NOT_COMPARABLE_LOOKBACK_PREDATES_SEAT_EPOCH"
    assert {row["last_stage"] for row in trace["eligible_intent_traces"]} == {
        "POLICY_ELIGIBLE_ROUTING_SHADOW"
    }
    assert {row["drop_predicate"] for row in trace["eligible_intent_traces"]} == {
        "UNEXPLAINED_NO_LEDGER_ROW"
    }


def test_paper_routing_shadow_trace_treats_pre_submit_refusal_as_explained() -> None:
    wallet = "0x" + "5" * 40
    now = dt.datetime(2026, 8, 4, 16, 12, 34, tzinfo=dt.timezone.utc)
    since = now - dt.timedelta(minutes=30)
    routing_shadow = {
        "fee_gated_measurement_rows": [
            {
                "intent_id": "ci_refused",
                "source_wallet": wallet,
                "dominant_skip_reason": "eligible",
                "observed_ts": (now - dt.timedelta(minutes=1)).timestamp(),
                "market_slug": "btc-updown-5m-1",
            },
            {
                "intent_id": "",
                "source_wallet": wallet,
                "dominant_skip_reason": "eligible",
                "observed_ts": (now - dt.timedelta(minutes=1)).timestamp(),
            },
            {
                "intent_id": "ci_missing_ts",
                "source_wallet": wallet,
                "dominant_skip_reason": "eligible",
            },
        ]
    }
    ledger = {
        "orders": [
            {
                "intent_id": "ci_refused",
                "status": "REJECTED",
                "trade_result": {
                    "error_class": "entry_price_band_closed_negative_holdout"
                },
            }
        ]
    }

    trace = deadman._paper_routing_shadow_intent_trace(
        routing_shadow=routing_shadow,
        ledger=ledger,
        selected_wallet=wallet,
        since=since,
        until=now,
        selected_seat_epoch_at=since,
        authority_counter=1,
    )

    assert trace["window_alignment"]["status"] == "COMPARABLE_LOOKBACK_WITHIN_SEAT_EPOCH"
    assert trace["population_parity"] == "NOT_COMPARABLE"
    assert trace["rows_dropped_missing_intent_id"] == 1
    assert trace["rows_dropped_missing_observed_ts"] == 1
    assert trace["unexplained_eligible_intents"] == 0
    row = trace["eligible_intent_traces"][0]
    assert row["last_stage"] == "PRE_SUBMIT_REFUSAL"
    assert row["drop_predicate"] is None
    assert row["explained_predicate"] == "entry_price_band_closed_negative_holdout"


def test_live_intent_to_submit_reconciliation_names_aggregate_authority() -> None:
    reconciliation = deadman._live_intent_to_submit_reconciliation(
        selected_eligible_intents=4,
        selected_guard_submit_attempts=0,
    )

    assert reconciliation == {
        "measurement_only": True,
        "live_mutation": False,
        "status": "NO_PER_INTENT_GUARD_RECORD",
        "eligible": 4,
        "submit_attempts": 0,
        "explained": 0,
        "unexplained": "UNMEASURABLE_NO_PER_INTENT_RECORD",
        "authority": "guard scan member_signal_age",
    }


def test_raw_policy_choke_diagnostic_does_not_silence_hard_backstop() -> None:
    disposition = deadman._accepted_order_deadman_disposition(
        raw_firing=True,
        local_skip_qualifies=False,
        ruled_posture_exemption=False,
        selection_pending_adoption=False,
        wallet_policy_diagnostic="WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET",
        measured_empty_seat_no_target=False,
    )
    assert disposition == {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "deadman_class": "ORDER_FLOW_DEAD",
        "mechanical_escalation": "NO_LAWFUL_ACTUATION",
    }


def test_episode_no_target_diagnostic_carries_until_candidate_exists() -> None:
    previous = {
        "status": "INCIDENT_ORDER_FLOW_DEAD",
        "episode_fire_wallet_policy_diagnostic": (
            "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
        ),
    }
    current = {"wallet_policy_diagnostic": "WALLET_POLICY_FLOW_CLEAR"}

    assert deadman._wallet_policy_disposition_diagnostic(
        policy_choke=current,
        previous=previous,
        selected_candidate=None,
    ) == "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
    assert deadman._wallet_policy_disposition_diagnostic(
        policy_choke=current,
        previous=previous,
        selected_candidate={"wallet": "0x" + "1" * 40},
    ) == "WALLET_POLICY_FLOW_CLEAR"


def test_temporal_active_slice_prefers_committed_venue_executable_registry() -> None:
    registry = json.loads(
        (ROOT / "data/research/wallet_temporal_profitability_latest.json").read_text()
    )
    wallet = "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b"
    committed = next(item for item in registry["wallets"] if item["wallet"] == wallet)
    row = deadman._temporal_active_slice(
        registry,
        wallet=wallet,
        regime="weekday",
    )
    assert row["label"] == committed["venue_slice_labels"]["weekday"]["label"]
    assert row["roi_pct"] == committed["venue_slice_labels"]["weekday"]["roi_pct"]
    assert row["roi_pct"] != committed["slice_labels"]["weekday"]["roi_pct"]


def test_fak_drought_with_byte_identical_live_guard_never_demands_restart() -> None:
    disposition = deadman._accepted_order_deadman_disposition(
        raw_firing=True,
        local_skip_qualifies=False,
        ruled_posture_exemption=False,
        accepted_orders=0,
        local_terminal_share=0.759259,
        fak_no_match_outcomes=2,
        liquidity_drought=True,
        generation_mismatch=False,
        guard_status="LIVE_GUARD_RUNNING",
    )

    assert disposition == {
        "status": "INCIDENT_MEASURED_NO_FLOW",
        "deadman_class": "MEASURED_ORDER_FLOW_DROUGHT",
        "mechanical_escalation": "NONE",
    }


def test_policy_choke_local_skip_rejects_venue_refusal() -> None:
    report = deadman._classify_policy_choke_local_skip(
        policy_choke={
            "reject_taxonomy": {"fak_no_match": 1},
            "fak_no_match_outcomes": 0,
            "whole_runtime_accepted_orders": 0,
        },
        reconciliation={
            "live_feedstock_rows": 100,
            "terminal_stage_counts": {"inventory_gate": 90},
        },
        can_trade=True,
        guard_status="LIVE_GUARD_RUNNING",
    )

    assert report["qualifies"] is False
    assert report["deadman_class"] == "ORDER_FLOW_DEAD"


def test_policy_choke_local_skip_rejects_fak_no_match() -> None:
    report = deadman._classify_policy_choke_local_skip(
        policy_choke={
            "reject_taxonomy": {},
            "fak_no_match_outcomes": 1,
            "whole_runtime_accepted_orders": 0,
        },
        reconciliation={
            "live_feedstock_rows": 100,
            "terminal_stage_counts": {"inventory_gate": 90},
        },
        can_trade=True,
        guard_status="LIVE_GUARD_RUNNING",
    )

    assert report["qualifies"] is False
    assert report["deadman_class"] == "ORDER_FLOW_DEAD"


def test_policy_choke_feedstock_gate_rejects_closed_and_api_stale_rows() -> None:
    now = dt.datetime(2026, 7, 26, 23, 22, tzinfo=dt.timezone.utc)
    wallet = "0x" + "a" * 40
    current_start = int(now.timestamp() // 300) * 300
    guard = {
        "active_set_runtime": {
            "members": [{"source_wallet": wallet, "enabled": True}]
        }
    }
    hot_history = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "event_id": "stale",
                "event_ts": current_start + 5,
                "observed_ts": current_start + 45,
                "api_latency_s": 40,
                "market_slug": f"btc-updown-5m-{current_start}",
                "outcome": "Up",
            },
            {
                "source_wallet": wallet,
                "action": "BUY",
                "event_id": "open",
                "event_ts": current_start + 50,
                "observed_ts": current_start + 55,
                "api_latency_s": 5,
                "market_slug": f"btc-updown-5m-{current_start}",
                "outcome": "Down",
            },
            {
                "source_wallet": wallet,
                "action": "BUY",
                "event_id": "closed",
                "event_ts": current_start - 600,
                "observed_ts": current_start + 10,
                "api_latency_s": 610,
                "market_slug": f"btc-updown-5m-{current_start - 600}",
                "outcome": "Up",
            },
        ]
    }
    reconciliation = deadman._policy_choke_terminal_reconciliation(
        guard=guard,
        hot_history=hot_history,
        routing_shadow={},
        ledger={},
        now=now,
    )
    policy_choke = {
        "selected_wallet": wallet,
        "selected_eligible_intents": 3,
        "whole_runtime_eligible_intents": 3,
    }

    deadman._apply_policy_choke_feedstock_gate(policy_choke, reconciliation)

    assert reconciliation["terminal_stage_counts"] == {
        "candidate_not_built": 1,
        "freshness_market_closed_before_evaluation": 1,
        "freshness_source_too_stale": 1,
    }
    assert reconciliation["live_feedstock_rows"] == 1
    assert reconciliation["liveness_only_rows"] == 2
    assert policy_choke["selected_eligible_intents_pre_feedstock_gate"] == 3
    assert policy_choke["selected_eligible_intents"] == 1
    assert policy_choke["whole_runtime_eligible_intents"] == 1


def test_terminal_reconciliation_is_outer_source_coverage_authority() -> None:
    result: dict = {"source_coverage_full": True}
    gated = {
        "checks": {"source_coverage_full": True},
        "measured_skip_checks": {"source_coverage_full": True},
        "source_coverage": {"source": "legacy", "source_coverage_full": True},
    }
    reconciliation = {
        "input_fresh_source_rows": 3,
        "terminal_rows": 2,
        "source_coverage_full": False,
        "input_equals_terminal_rows": False,
    }

    deadman._sync_terminal_source_coverage(result, gated, reconciliation)

    assert result["source_coverage_full"] is False
    assert gated["checks"]["source_coverage_full"] is False
    assert gated["measured_skip_checks"]["source_coverage_full"] is False
    assert gated["source_coverage"] == {
        "source": "policy_choke.terminal_reconciliation",
        "input_fresh_source_rows": 3,
        "terminal_rows": 2,
        "source_coverage_full": False,
    }


def test_policy_choke_rung_c_is_idempotent_under_active_pin() -> None:
    now = dt.datetime(2026, 7, 20, 16, 0, tzinfo=dt.timezone.utc)
    wallet, overlay, _ready, hot = _rung_b_fixture(now)
    evidence = deadman._select_policy_choke_rung_c_candidate(
        cohort_admission={"packets": [_rung_c_packet(wallet)]}, full_pool_queue={"ranked_members": []},
        overlay=overlay, hot_history=hot, now=now, regime="weekday", cooloffs={},
        temporal_registry=_measured_weekday_temporal(wallet),
    )
    admitted, _first = deadman._execute_policy_choke_rung_b(
        overlay=overlay, candidate=evidence["selected"], now=now, supply_rung="C"
    )

    unchanged, second = deadman._execute_policy_choke_rung_b(
        overlay=admitted, candidate=evidence["selected"], now=now + dt.timedelta(minutes=1), supply_rung="C"
    )

    assert second["status"] == "RUNG_C_FULL_POOL_SWEEP_ALREADY_ACTIVE"
    assert unchanged == admitted
    assert len(admitted["members"]) == 2


def test_policy_choke_cooloff_is_shared_across_supply_pools() -> None:
    now = dt.datetime(2026, 7, 20, 16, 0, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    cooloffs = {wallet: (now + dt.timedelta(hours=24)).isoformat()}

    rung_b = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs=cooloffs
    )
    rung_c = deadman._select_policy_choke_rung_c_candidate(
        cohort_admission={"packets": [_rung_c_packet(wallet)]}, full_pool_queue={"ranked_members": []},
        overlay=overlay, hot_history=hot, now=now, regime="weekday", cooloffs=cooloffs,
        temporal_registry=_measured_weekday_temporal(wallet),
    )

    assert rung_b["selected"] is None
    assert rung_c["selected"] is None
    assert rung_b["rows"][0]["checks"]["f3_not_enabled_or_cooloff_or_fading"] is False
    assert rung_c["rows"][0]["checks"]["f3_not_enabled_or_cooloff_or_fading"] is False


def test_policy_choke_rung_b_and_c_share_gate_digits() -> None:
    now = dt.datetime(2026, 7, 20, 16, 0, tzinfo=dt.timezone.utc)
    wallet, overlay, ready, hot = _rung_b_fixture(now)
    rung_b = deadman._select_policy_choke_rung_b_candidate(
        ready_shadow=ready, overlay=overlay, hot_history=hot, now=now, regime="weekday", temporal_registry={}, cooloffs={}
    )
    rung_c = deadman._select_policy_choke_rung_c_candidate(
        cohort_admission={"packets": [_rung_c_packet(wallet)]}, full_pool_queue={"ranked_members": []},
        overlay=overlay, hot_history=hot, now=now, regime="weekday", cooloffs={},
        temporal_registry=_measured_weekday_temporal(wallet),
    )

    assert rung_b["gate_digits"] == rung_c["gate_digits"]
    assert tuple(rung_b["rows"][0]["checks"]) == tuple(rung_c["rows"][0]["checks"])


def test_deadman_reports_suppression_as_annotation_only(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(tmp_path / "guard.json", {"summary": {"can_trade": True}})
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T21:45:00+00:00",
                "source_wallet": "0xabc",
                "signal_age_s": 125.0,
                "taxonomy_tags": ["signal_age_gte_60s"],
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    assert state["liveness_source"] == "accepted_order"
    assert state["liveness_basis"] == "accepted_order"
    assert state["annotation_liveness_only"] is True
    assert state["annotation_liveness_source"] == "approved_suppression"
    assert state["latest_approved_suppression_ts"] == "2026-07-07T21:45:00+00:00"
    assert state["member_signal_age"]["0xabc"]["suppressed_intents"] == 1
    assert state["member_signal_age"]["0xabc"]["signal_age_p90_s"] == 125.0
    evidence_path = tmp_path / "data/research/order_flow_deadman_incidents.jsonl"
    evidence_rows = [json.loads(line) for line in evidence_path.read_text(encoding="utf-8").splitlines()]
    assert len(evidence_rows) == 1
    assert evidence_rows[0]["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    assert "pipe_verified_source_quiet" in evidence_rows[0]
    assert "gated_quiet_classification" in evidence_rows[0]
    assert any(gate.startswith("pipe_verified_source_quiet:") for gate in evidence_rows[0]["failed_gates"])
    assert "failed_gates=" in (tmp_path / "HANDOFF.md").read_text(encoding="utf-8")


def test_deadman_attributes_pre_boot_liveness_to_host_downtime(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("WALLET_COPY_HOST_BOOT_TIME_UTC", "2026-07-07T21:30:00+00:00")
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(tmp_path / "guard.json", {"summary": {"can_trade": True}})
    _write_jsonl(tmp_path / "events.jsonl", [])

    state = _run_deadman(tmp_path)

    assert state["status"] == "HOST_DOWNTIME_RESTART"
    assert state["deadman_class"] == "HOST_DOWNTIME_RESTART"
    assert state["host_downtime_attribution"]["status"] == "HOST_DOWNTIME_RESTART"
    assert state["host_downtime_attribution"]["downtime_s"] == 1800.0
    assert "ORDER_FLOW_DEADMAN HOST-DOWNTIME" in (tmp_path / "HANDOFF.md").read_text(encoding="utf-8")


def test_deadman_post_boot_suppression_advances_recovery_clock(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("WALLET_COPY_HOST_BOOT_TIME_UTC", "2026-07-07T21:30:00+00:00")
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(tmp_path / "guard.json", {"summary": {"can_trade": True}})
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T21:45:00+00:00",
                "source_wallet": "0xabc",
                "taxonomy_tags": ["window_time_gte_60s"],
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "OK"
    assert state["accepted_order_idle_s"] == 3600.0
    assert state["effective_deadman_idle_s"] == 900.0
    assert state["host_downtime_attribution"]["post_boot_recovery_grace"] is True
    assert state["host_downtime_attribution"]["post_boot_recovery_liveness_ts"] == "2026-07-07T21:45:00+00:00"


def test_deadman_bounds_post_boot_acceptance_path_grace(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("WALLET_COPY_HOST_BOOT_TIME_UTC", "2026-07-07T21:30:00+00:00")
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(tmp_path / "guard.json", {"summary": {"can_trade": True}})
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T23:20:00+00:00",
                "source_wallet": "0xabc",
                "taxonomy_tags": ["window_time_gte_60s"],
            }
        ],
    )

    state = _run_deadman(tmp_path, "--now", "2026-07-07T23:40:00+00:00")

    assert state["status"] == "UNCERTIFIED_ACCEPTANCE_PATH"
    assert state["deadman_class"] == "UNCERTIFIED_ACCEPTANCE_PATH"
    assert state["effective_deadman_idle_s"] == 1200.0
    assert state["host_downtime_attribution"]["post_boot_recovery_grace"] is True
    assert state["host_downtime_attribution"]["post_boot_accepted_order_pending_since"] == "2026-07-07T21:30:00+00:00"
    assert state["host_downtime_attribution"]["post_boot_acceptance_path_uncertified"] is True
    assert state["acceptance_path_certification"]["live_mutation"] is False


def test_deadman_classifies_all_approved_gates_as_measured_skip_quiet(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:01+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "generated_at": "2026-07-07T21:59:30+00:00",
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:59:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "generated_at": "2026-07-07T21:59:30+00:00",
            "drought_funnel": {
                "reject_taxonomy_counts": {
                    "prefilter:market_closed_now": 6,
                    "inventory_target_already_met": 7,
                    "toxicity_protection": 3,
                    "window_time_gte_180s": 4,
                    "late_window_guard": 1,
                    "best_ask_missing": 1,
                    "inventory_best_ask_gate": 1,
                    "profit_latency_suppression": 1,
                }
            },
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 9,
                    "source_coverage_denominator_windows": 9,
                    "source_coverage_rate_pct": 100.0,
                    "adjusted_consecutive_missed_active_windows": 1,
                }
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:59:30+00:00",
                "fetch_meta": {"0xabc": {"api_errors": []}},
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T21:50:00+00:00",
                "source_wallet": "0xabc",
                "signal_age_s": 125.0,
                "taxonomy_tags": ["window_time_gte_180s"],
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "MEASURED_SOURCE_QUIET"
    gated = state["gated_quiet_classification"]
    assert gated["status"] == "PASS"
    assert gated["checks"]["morning_bench_governed_until_expiry"] is True
    assert gated["morning_bench_gate_attribution"]["failing_reasons"] == []
    assert gated["checks"]["reject_taxonomy_approved_only"] is True
    assert gated["unapproved_gate_reasons"] == []
    assert gated["source_coverage"]["source_coverage_windows"] == 9
    assert not (tmp_path / "HANDOFF.md").exists()


def test_selected_runtime_identity_prefers_dated_guard_pair_over_legacy_snapshot() -> None:
    authoritative = "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"
    legacy = "0x2d7c9298b64713de86402bd8a41695e31865a945"
    identity = deadman._selected_runtime_identity(
        {
            "source_wallet": authoritative,
            "candidate_id": "deadman_microprobe_c5391c6dfd",
            "runtime_member_submittability": {
                "source_wallet": authoritative,
                "candidate_id": "deadman_microprobe_c5391c6dfd",
                "generated_at": "2026-08-03T01:06:46+00:00",
            },
            "active_set_runtime": {
                "selected_member": {
                    "source_wallet": legacy,
                    "candidate_id": "market_cohort_alive_95e31865a945",
                }
            },
        }
    )
    assert identity["status"] == "PASS_LEGACY_SNAPSHOT_DIVERGED"
    assert identity["source_wallet"] == authoritative
    assert identity["legacy_snapshot_wallet"] == legacy


def test_selected_runtime_identity_fails_closed_when_authoritative_pair_disagrees() -> None:
    identity = deadman._selected_runtime_identity(
        {
            "source_wallet": "0x" + "1" * 40,
            "candidate_id": "candidate-1",
            "runtime_member_submittability": {
                "source_wallet": "0x" + "2" * 40,
                "candidate_id": "candidate-2",
                "generated_at": "2026-08-03T01:06:46+00:00",
            },
        }
    )
    assert identity["status"] == "REFUSED_AUTHORITATIVE_IDENTITY_DISAGREEMENT"
    assert identity["source_wallet"] is None


def test_selected_identity_projection_exposes_dated_identity_and_runtime_counts() -> None:
    wallet = "0x" + "1" * 40
    identity = {
        "status": "PASS",
        "source_wallet": wallet,
        "candidate_id": "candidate-1",
        "generated_at": "2026-08-03T01:06:46+00:00",
        "source": "runtime_member_submittability_crosschecked_top_level",
    }
    projected = deadman._selected_identity_projection(
        {
            "source_wallet": wallet,
            "candidate_id": "candidate-1",
            "policy_id": "top-policy",
            "active_set_runtime": {
                "member_count": 4,
                "qualified_member_count": 3,
                "members": [
                    {"source_wallet": wallet, "policy_id": "runtime-policy"}
                ],
            },
        },
        {
            "selected_wallet": wallet,
            "selected_identity": identity,
            "active_set_fresh_signal_rows_total": 0,
            "fresh_buy_rows_le_10s_total": 0,
            "active_set_rtds_new_matching_events_total": 0,
        },
    )

    assert projected["selected_wallet"] == wallet
    assert projected["selected_candidate_id"] == "candidate-1"
    assert projected["selected_policy_id"] == "runtime-policy"
    assert projected["selected_identity"] == identity
    assert projected["selected_identity_resolved"] is True
    assert projected["active_member_count"] == 4
    assert projected["qualified_member_count"] == 3


def test_selected_identity_projection_marks_unresolved_observed_demand() -> None:
    projected = deadman._selected_identity_projection(
        {},
        {
            "selected_wallet": None,
            "selected_identity": {
                "status": "REFUSED_AUTHORITATIVE_IDENTITY_DISAGREEMENT",
                "source": "fail_closed",
            },
            "identity_unresolved_attributed_demand_rows": 6,
        },
    )

    assert projected["selected_identity_resolved"] is False
    assert projected["selected_identity_observed_demand_rows"] == 6
    assert projected["selected_identity_unresolved_with_observed_demand"] is True


def test_pipe_quiet_rejects_future_guard_event_and_discloses_skew(tmp_path: Path) -> None:
    guard = {
        "generated_at": "2026-08-03T00:55:00+00:00",
        "active_set_dataapi_poller": {
            "generated_at": "2026-08-03T00:51:00+00:00",
            "fetch_meta": {
                "0xabc": {
                    "api_errors": [],
                    "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                }
            },
        },
        "drought_funnel": {"active_set_fresh_signal_rows": 0},
    }
    payload = deadman._pipe_verified_source_quiet(
        guard,
        tmp_path / "missing.jsonl",
        dt.datetime.fromisoformat("2026-08-03T00:51:23+00:00"),
    )
    assert payload["guard_state_fresh"] is False
    assert payload["guard_state_future_dated"] is True
    assert payload["pipe_event_future_dated"] is True
    assert payload["pipe_event_clock_skew_s"] == 217.0
    assert payload["max_clock_skew_s"] == 300.0


def test_stale_rtds_event_is_rotation_telemetry_not_fresh_demand() -> None:
    selected = "0x" + "1" * 40
    bench = "0x" + "2" * 40
    split = deadman._active_set_fresh_demand_split(
        {
            "generated_at": "2026-08-03T01:32:51+00:00",
            "source_wallet": selected,
            "candidate_id": "candidate-selected",
            "runtime_member_submittability": {
                "source_wallet": selected,
                "candidate_id": "candidate-selected",
                "generated_at": "2026-08-03T01:32:51+00:00",
            },
            "active_set_dataapi_poller": {
                "fetch_meta": {
                    selected: {
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    },
                    bench: {
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                        "freshest_buy_lag_s_by_source": {"trade:user": 51.465238},
                    },
                }
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
            "active_set_rtds_premerge": {
                "new_matching_events": 1,
                "rows": [
                    {
                        "source_wallet": bench,
                        "new_matching_events": 1,
                        "retained_matching_rows": 1,
                        "latest_event_ts": "2026-08-03T01:31:59.534762+00:00",
                    }
                ],
            },
        }
    )

    assert split["classification"] == "MEASURED_NONSELECTED_ROTATION_EVENT"
    assert split["nonselected_rotation_pressure_rows"] == 0
    assert split["nonselected_rotation_event_rows"] == 1
    assert split["demand_evidence_limb"] == []
    assert split["nonselected_samples"][0]["rtds_new_matching_events"] == 0
    assert split["nonselected_samples"][0]["rtds_new_matching_events_raw"] == 1
    assert split["nonselected_samples"][0]["rtds_latest_event_age_s"] == 51.465238


def test_deadman_ratified_gate_taxonomy_names_are_gated_quiet(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:01+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {
                "reject_taxonomy_counts": {
                    "entry_price_band_gate": 2,
                    "inventory_confirmed_unchanged_no_edge": 3,
                    "window_time_gte_180s": 4,
                }
            },
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 9,
                    "source_coverage_denominator_windows": 9,
                    "source_coverage_rate_pct": 100.0,
                    "adjusted_consecutive_missed_active_windows": 0,
                }
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_entry_price_band_gate_counterfactual",
                "approved_suppression": True,
                "ts": "2026-07-07T21:50:00+00:00",
                "taxonomy_tags": ["entry_price_band_gate"],
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    gated = state["gated_quiet_classification"]
    assert gated["checks"]["reject_taxonomy_approved_only"] is True
    assert gated["unapproved_gate_reasons"] == []


def test_deadman_empty_reject_taxonomy_passes_approved_only_gate(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:01+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {"reject_taxonomy_counts": {}},
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 9,
                    "source_coverage_denominator_windows": 9,
                    "source_coverage_rate_pct": 100.0,
                    "adjusted_consecutive_missed_active_windows": 0,
                }
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [{"event": "approved", "approved_suppression": True, "ts": "2026-07-07T21:50:00+00:00", "taxonomy_tags": []}],
    )

    state = _run_deadman(tmp_path)

    gated = state["gated_quiet_classification"]
    assert gated["deduplicated_gate_taxonomy"] == {}
    assert gated["checks"]["reject_taxonomy_approved_only"] is True
    assert gated["unapproved_gate_reasons"] == []


def test_deadman_approves_armed_probe_and_liquidity_reject_taxonomy(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:01+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "active_set_runtime": {
                "size_defense": {
                    "status": "PROBE_CAPS_REST_OF_UTC_DAY",
                    "intraday_probe_latched": True,
                    "source": "state_digest.defense_tripwires",
                }
            },
            "drought_funnel": {
                "reject_taxonomy_counts": {
                    "probe_cap_blocked": 5,
                    "policy_cap_maker_fallback": 5,
                    "fak_no_match": 5,
                }
            },
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 9,
                    "source_coverage_denominator_windows": 9,
                    "source_coverage_rate_pct": 100.0,
                    "adjusted_consecutive_missed_active_windows": 0,
                }
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [{"event": "approved", "approved_suppression": True, "ts": "2026-07-07T21:50:00+00:00", "taxonomy_tags": []}],
    )

    state = _run_deadman(tmp_path)

    gated = state["gated_quiet_classification"]
    assert gated["armed_probe_latch"]["armed"] is True
    assert gated["checks"]["reject_taxonomy_approved_only"] is True
    assert gated["unapproved_gate_reasons"] == []
    approved = gated["taxonomy_attribution"]["approved_reasons"]
    assert approved["probe_cap_blocked"]["classification"] == "approved_armed_probe_defense_taxonomy"
    assert approved["policy_cap_maker_fallback"]["classification"] == "approved_armed_probe_defense_taxonomy"
    assert approved["fak_no_match"]["classification"] == "approved_gated_quiet_taxonomy"


def test_deadman_keeps_policy_cap_fallback_unapproved_without_armed_probe_latch() -> None:
    taxonomy = {"policy_cap_maker_fallback": 1, "fak_no_match": 1}

    assert deadman._unapproved_gated_quiet_reasons(taxonomy) == ["policy_cap_maker_fallback"]


def test_deadman_treats_client_side_price_band_reject_as_gated_quiet(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [
                {"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:01+00:00"},
                {
                    "status": "REJECTED",
                    "final_status": "price_band_skip",
                    "updated_at": "2026-07-07T21:52:00+00:00",
                    "trade_result": {"error_class": "price_band_skip"},
                    "wallet_copy_price_band": {"taxonomy": "price_band_skip"},
                },
            ],
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {
                "reject_taxonomy_counts": {
                    "live_order_rejected": 1,
                    "window_time_gte_180s": 4,
                }
            },
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 9,
                    "source_coverage_denominator_windows": 9,
                    "adjusted_consecutive_missed_active_windows": 0,
                }
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T21:50:00+00:00",
                "taxonomy_tags": ["window_time_gte_180s"],
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    gated = state["gated_quiet_classification"]
    assert gated["raw_gate_taxonomy"]["live_order_rejected"] == 1
    assert gated["deduplicated_gate_taxonomy"]["price_band_skip"] == 1
    assert gated["live_order_reject_price_band_summary"]["price_band_skip_only"] is True
    assert gated["unapproved_gate_reasons"] == []


def test_deadman_keeps_non_price_band_live_reject_as_real_incident(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [
                {"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:01+00:00"},
                {
                    "status": "REJECTED",
                    "final_status": "exchange_reject",
                    "updated_at": "2026-07-07T21:52:00+00:00",
                    "trade_result": {"error_class": "exchange_reject"},
                },
            ],
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {
                "reject_taxonomy_counts": {
                    "live_order_rejected": 1,
                    "window_time_gte_180s": 4,
                }
            },
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 9,
                    "source_coverage_denominator_windows": 9,
                    "adjusted_consecutive_missed_active_windows": 0,
                }
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T21:50:00+00:00",
                "taxonomy_tags": ["window_time_gte_180s"],
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    gated = state["gated_quiet_classification"]
    assert gated["checks"]["reject_taxonomy_approved_only"] is False
    assert gated["unapproved_gate_reasons"] == ["live_order_rejected"]


def test_deadman_rejects_unapproved_gate_taxonomy_as_real_incident(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:01+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {
                "reject_taxonomy_counts": {
                    "window_time_gte_180s": 4,
                    "selector_empty": 1,
                }
            },
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 9,
                    "source_coverage_denominator_windows": 9,
                    "source_coverage_rate_pct": 100.0,
                    "adjusted_consecutive_missed_active_windows": 0,
                }
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T21:50:00+00:00",
                "taxonomy_tags": ["window_time_gte_180s"],
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    gated = state["gated_quiet_classification"]
    assert gated["status"] == "FAIL"
    assert gated["checks"]["reject_taxonomy_approved_only"] is False
    assert gated["unapproved_gate_reasons"] == ["selector_empty"]


def test_deadman_classifies_exchange_no_match_reject_residue(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [
                {
                    "status": "REJECTED",
                    "updated_at": "2026-07-07T21:00:01+00:00",
                    "trade_result": {"error_class": "fak_no_match"},
                },
                {
                    "status": "REJECTED",
                    "updated_at": "2026-07-07T21:00:02+00:00",
                    "trade_result": {
                        "error_class": "maker_min_share_bump_exceeds_policy_cap",
                        "fallback_parent_result": {"error_class": "fak_no_match"},
                    },
                },
            ],
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {
                "reject_taxonomy_counts": {"live_order_rejected": 1, "ready_to_submit": 1}
            },
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 1,
                    "source_coverage_denominator_windows": 1,
                    "adjusted_consecutive_missed_active_windows": 0,
                }
            },
        },
    )

    state = _run_deadman(tmp_path)
    gated = state["gated_quiet_classification"]
    assert gated["checks"]["reject_taxonomy_approved_only"] is True
    assert gated["unapproved_gate_reasons"] == []
    assert gated["deduplicated_gate_taxonomy"]["fak_no_match"] == 1
    assert "submitted" not in gated["deduplicated_gate_taxonomy"]
    assert gated["live_order_reject_price_band_summary"][
        "inflight_ready_to_submit_excluded"
    ] == 1
    assert "ORDER_FLOW_DEADMAN INCIDENT" in (tmp_path / "HANDOFF.md").read_text(encoding="utf-8")


def test_submit_outcomes_names_local_policy_cap_preflight_reject() -> None:
    wallet = "0x" + "a" * 40
    ledger = {
        "orders": [
            {
                "status": "REJECTED",
                "updated_at": "2026-07-07T21:45:01+00:00",
                "source_wallet": wallet,
                "order_id": "local-preflight-policy-cap",
                "lifecycle": [
                    {"status": "LIVE_SUBMITTED", "payload": {}},
                    {
                        "status": "LIVE_REJECTED",
                        "payload": {
                            "error_class": "maker_min_share_bump_exceeds_policy_cap"
                        },
                    },
                ],
                "trade_result": {
                    "error_class": "maker_min_share_bump_exceeds_policy_cap"
                },
            }
        ]
    }

    result = deadman._policy_choke_submit_outcomes(
        ledger,
        since=dt.datetime.fromisoformat("2026-07-07T21:30:00+00:00"),
        until=dt.datetime.fromisoformat("2026-07-07T22:00:00+00:00"),
        effective_lanes={"live_guard"},
    )

    assert result["guard_submit_attempts"] == 1
    assert result["reject_taxonomy"] == {
        "maker_min_share_bump_exceeds_policy_cap": 1
    }
    assert result["per_wallet"][wallet]["reject_taxonomy"] == {
        "maker_min_share_bump_exceeds_policy_cap": 1
    }


def test_submit_outcomes_names_pre_submit_band_holdout_without_submit() -> None:
    wallet = "0x" + "b" * 40
    ledger = {
        "orders": [
            {
                "status": "REJECTED",
                "updated_at": "2026-08-03T07:55:17+00:00",
                "source_wallet": wallet,
                "order_id": "local-band-holdout",
                "trade_decision": {"execution_lane": "copyintent_policy"},
                "lifecycle": [
                    {
                        "status": "LIVE_SKIPPED",
                        "payload": {
                            "error_class": "entry_price_band_closed_negative_holdout"
                        },
                    }
                ],
                "trade_result": {
                    "error_class": "entry_price_band_closed_negative_holdout"
                },
            }
        ]
    }

    result = deadman._policy_choke_submit_outcomes(
        ledger,
        since=dt.datetime.fromisoformat("2026-08-03T07:30:00+00:00"),
        until=dt.datetime.fromisoformat("2026-08-03T08:00:00+00:00"),
        effective_lanes={"live_guard"},
    )

    assert result["guard_submit_attempts"] == 0
    assert result["excluded_lane_counts"] == {}
    assert result["reject_taxonomy"] == {
        "entry_price_band_closed_negative_holdout": 1
    }
    assert result["per_wallet"][wallet]["reject_taxonomy"] == {
        "entry_price_band_closed_negative_holdout": 1
    }


def test_deadman_names_post_only_crosses_book_from_raw_and_lifecycle_payloads(
    tmp_path: Path,
) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [
                {
                    "status": "REJECTED",
                    "updated_at": "2026-07-07T21:59:01+00:00",
                    "source_wallet": "0x" + "a" * 40,
                    "order_id": "lo_post_only_cross",
                    "raw_clob_reject_payload": {
                        "error_class": "post_only_rejected",
                        "error": (
                            "PolyApiException[status_code=400, "
                            "error_message={'error': 'invalid post-only order: "
                            "order crosses book'}]"
                        ),
                    },
                    "lifecycle": [
                        {
                            "status": "LIVE_SUBMITTED",
                            "payload": {},
                        },
                        {
                            "status": "LIVE_REJECTED",
                            "payload": {
                                "error_class": "post_only_rejected",
                                "error": "invalid post-only order: order crosses book",
                            },
                        }
                    ],
                }
            ],
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {
                "reject_taxonomy_counts": {
                    "live_order_rejected": 1,
                    "ready_to_submit": 1,
                }
            },
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 1,
                    "source_coverage_denominator_windows": 1,
                    "adjusted_consecutive_missed_active_windows": 0,
                }
            },
        },
    )

    state = _run_deadman(tmp_path)
    gated = state["gated_quiet_classification"]
    assert gated["live_order_reject_price_band_summary"][
        "classified_reject_counts"
    ] == {"post_only_crosses_book": 1}
    assert gated["deduplicated_gate_taxonomy"]["post_only_crosses_book"] == 1
    assert gated["unapproved_gate_reasons"] == ["post_only_crosses_book"]
    submit_outcomes = state["policy_choke"]["submit_outcomes"]
    assert submit_outcomes["reject_taxonomy"] == {"post_only_crosses_book": 1}
    assert submit_outcomes["per_wallet"]["0x" + "a" * 40]["reject_taxonomy"] == {
        "post_only_crosses_book": 1
    }


def test_deadman_attributes_inventory_residual_taxonomy_as_approved_quiet(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:01+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {
                "reject_taxonomy_counts": {
                    "filled": 1,
                    "filtered_after_inventory_build": 3,
                    "inventory_residual_gap_below_min_order": 1,
                    "inventory_best_ask_missing": 2,
                }
            },
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 9,
                    "source_coverage_denominator_windows": 9,
                    "source_coverage_rate_pct": 100.0,
                    "adjusted_consecutive_missed_active_windows": 1,
                }
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T21:50:00+00:00",
                "taxonomy_tags": ["window_time_gte_180s"],
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    gated = state["gated_quiet_classification"]
    assert gated["checks"]["reject_taxonomy_approved_only"] is True
    assert gated["unapproved_gate_reasons"] == []
    attribution = gated["taxonomy_attribution"]["approved_reasons"]
    assert attribution["inventory_residual_gap_below_min_order"]["classification"] == (
        "legitimate_inventory_residual_below_min_order"
    )
    assert attribution["filtered_after_inventory_build"]["classification"] == "post_inventory_filter_correct_skip"


def test_deadman_quiet_without_selected_demand_is_not_failed_by_wall_clock(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T20:59:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {"reject_taxonomy_counts": {"window_time_gte_180s": 4}},
            "window_participation": {
                "adjusted_participation": {
                    "source_coverage_windows": 9,
                    "source_coverage_denominator_windows": 9,
                    "adjusted_consecutive_missed_active_windows": 0,
                }
            },
        },
    )
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T21:50:00+00:00",
                "taxonomy_tags": ["window_time_gte_180s"],
            }
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    assert state["gated_quiet_classification"]["checks"]["idle_under_hard_backstop_s"] is True


def test_deadman_incidents_without_order_or_approved_suppression(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(tmp_path / "guard.json", {"summary": {"can_trade": True}})
    _write_jsonl(tmp_path / "events.jsonl", [])

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    assert state["latest_approved_suppression_ts"] is None
    assert "ORDER_FLOW_DEADMAN INCIDENT" in (tmp_path / "HANDOFF.md").read_text(encoding="utf-8")


def test_deadman_reports_eligible_drought_from_profit_filter_pass(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(tmp_path / "guard.json", {"summary": {"can_trade": True}})
    _write_jsonl(
        tmp_path / "events.jsonl",
        [
            {
                "event": "wallet_copy_live_guard_cycle",
                "generated_at": "2026-07-07T21:10:00+00:00",
                "live_execution": {
                    "profit_latency_suppression": {
                        "output_intents": 1,
                        "blocked_intents": 0,
                        "sample_passed_intents": [
                            {"source_wallet": "0xabc", "signal_age_s": 15.0}
                        ],
                    }
                },
            },
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "approved_suppression": True,
                "ts": "2026-07-07T21:45:00+00:00",
                "source_wallet": "0xabc",
                "signal_age_s": 150.0,
                "taxonomy_tags": ["signal_age_gte_60s"],
            },
        ],
    )

    state = _run_deadman(tmp_path)

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    assert state["latest_profit_filter_pass_ts"] == "2026-07-07T21:10:00+00:00"
    assert state["eligible_drought_s"] is not None
    assert state["member_signal_age"]["0xabc"]["eligible_intents"] == 1
    assert state["member_signal_age"]["0xabc"]["suppressed_intents"] == 1


def test_deadman_accepts_verified_source_quiet_pipe_liveness(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "MEASURED_SOURCE_QUIET"
    assert state["deadman_class"] == "MEASURED_SOURCE_QUIET"
    assert state["consecutive_incidents"] == 0
    assert state["liveness_source"] == "accepted_order"
    assert state["annotation_liveness_source"] == "pipe_verified_source_quiet"
    assert state["latest_pipe_verified_source_quiet_ts"] == "2026-07-07T21:59:30+00:00"
    assert state["pipe_verified_source_quiet"]["verified"] is True
    assert state["pipe_verified_source_quiet"]["fresh_buy_rows_le_10s"] == 0
    assert state["pipe_verified_source_quiet"]["active_set_fresh_signal_rows"] == 0
    assert state["gated_quiet_classification"]["classification"] == "MEASURED_SOURCE_QUIET"
    assert not (tmp_path / "HANDOFF.md").exists()


def test_deadman_records_fetch_api_error_details_for_source_quiet_verification(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [
                            {
                                "source": "trade:user",
                                "route_report": {
                                    "status": "TRANSPORT_ERROR",
                                    "attempts": [
                                        {"status": "ERROR", "exception": "ReadTimeout", "error": "timeout"}
                                    ],
                                },
                            }
                        ],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    quiet = state["pipe_verified_source_quiet"]
    assert quiet["api_clean"] is False
    assert quiet["fetch_api_error_wallets"] == ["0xabc"]
    assert quiet["fetch_api_error_details"]["0xabc"][0]["exception"] == "ReadTimeout"
    assert quiet["fetch_api_error_details"]["0xabc"][0]["source"] == "trade:user"


def test_deadman_keeps_source_quiet_when_only_policy_taxonomy_is_present(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xe6db20932faf0f9780acf75d95c74c9984407dac": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "drought_funnel": {
                "active_set_fresh_signal_rows": 0,
                "reject_taxonomy_counts": {"policy:price_outside_policy": 4},
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "MEASURED_SOURCE_QUIET"
    assert state["deadman_class"] == "MEASURED_SOURCE_QUIET"
    assert state["guard_side_halt_signal"]["active"] is False
    assert state["guard_side_halt_signal"]["price_outside_policy_rejects"] == 4
    assert state["guard_side_halt_signal"]["fresh_actionable_signal_rows"] == 0
    assert state["gated_quiet_classification"]["source_quiet_checks"]["no_guard_side_halt_signal"] is True
    assert not (tmp_path / "HANDOFF.md").exists()


def test_deadman_treats_unsubmittable_runtime_member_as_guard_side_halt(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "runtime_member_submittability": {
                "status": "INCIDENT_MEMBER_UNSUBMITTABLE",
                "effective_max_order_usd": 0.015625,
                "configured_min_live_order_usd": 1.0,
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {"0x5960": {"api_errors": [], "fresh_buy_rows_le_10s_by_source": {"trade:user": 0}}},
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "INCIDENT_GUARD_SIDE_HALT"
    assert state["guard_side_halt_signal"]["member_unsubmittable"] is True


def test_deadman_classifies_ruled_morning_bench_as_measured_skip(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-14T08:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "runtime_member_submittability": {
                "status": "INCIDENT_MEMBER_UNSUBMITTABLE",
                "effective_max_order_usd": 0.015625,
                "configured_min_live_order_usd": 1.0,
            },
            "active_set_runtime": {
                "selected_member": {
                    "candidate_id": "runtime_auto_degrade_c03c7cc147",
                    "source_wallet": "0xc03c7cc1478a750a59ce44183ede1b85e7606cd9",
                    "policy_id": "fast_wf_0.10_cap_2_all_prices_minusd_0_all_window",
                    "max_order_usd": 0.015625,
                    "size_defense": {"status": "PROBE_CAPS_REST_OF_UTC_DAY"},
                }
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-14T09:29:00+00:00",
                "fetch_meta": {"0x5960": {"api_errors": [], "fresh_buy_rows_le_10s_by_source": {"trade:user": 0}}},
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-14T09:29:30+00:00"}],
    )
    _write_hour_bench_state(tmp_path)

    state = _run_deadman(
        tmp_path,
        "--guard-event-log",
        "guard_events.jsonl",
        "--now",
        "2026-07-14T09:30:00+00:00",
    )

    assert state["status"] == "MEASURED_SKIP_MORNING_BENCH"
    assert state["deadman_class"] == "MEASURED_SKIP_MORNING_BENCH"
    assert state["guard_side_halt_signal"]["member_unsubmittable"] is True
    ruling = state["gated_quiet_classification"]["morning_bench_ruling"]
    assert ruling["active"] is True
    assert ruling["expires_at"] == "2026-07-14T13:00:00+00:00"
    assert ruling["rows"][0]["source_wallet"] == "0xe6db20932faf0f9780acf75d95c74c9984407dac"
    assert ruling["selected_member_probe_cap_status"] == "PROBE_CAPS_REST_OF_UTC_DAY"
    gate = state["gated_quiet_classification"]["morning_bench_gate_attribution"]
    assert gate["passed"] is True
    assert gate["failing_reasons"] == []
    assert not (tmp_path / "HANDOFF.md").exists()


def test_deadman_morning_bench_ruling_expires_to_guard_side_halt(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-14T12:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "runtime_member_submittability": {
                "status": "INCIDENT_MEMBER_UNSUBMITTABLE",
                "effective_max_order_usd": 0.015625,
                "configured_min_live_order_usd": 1.0,
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-14T13:00:30+00:00",
                "fetch_meta": {"0x5960": {"api_errors": [], "fresh_buy_rows_le_10s_by_source": {"trade:user": 0}}},
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-14T13:00:30+00:00"}],
    )
    _write_hour_bench_state(tmp_path)

    state = _run_deadman(
        tmp_path,
        "--guard-event-log",
        "guard_events.jsonl",
        "--now",
        "2026-07-14T13:01:00+00:00",
    )

    assert state["status"] == "INCIDENT_GUARD_SIDE_HALT"
    assert state["deadman_class"] == "GUARD_SIDE_HALT"
    assert state["gated_quiet_classification"]["morning_bench_ruling"]["active"] is False
    gate = state["gated_quiet_classification"]["morning_bench_gate_attribution"]
    assert gate["passed"] is False
    assert gate["failing_reasons"] == ["no_active_future_hour_band_bench_ruling"]


def test_deadman_span_aware_backstop_uses_previous_fresh_actionable_ts(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {
                "active_set_fresh_signal_rows": 1,
                "reject_taxonomy_counts": {"inventory_target_already_met": 1},
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:39:30+00:00",
                "fetch_meta": {"0xabc": {"api_errors": [], "fresh_buy_rows_le_10s_by_source": {"trade:user": 0}}},
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:39:30+00:00"}],
    )

    first = _run_deadman(
        tmp_path,
        "--guard-event-log",
        "guard_events.jsonl",
        "--now",
        "2026-07-07T21:40:00+00:00",
    )

    assert first["guard_side_halt_signal"]["fresh_actionable_signal_rows"] == 1
    assert first["last_fresh_actionable_ts"] == "2026-07-07T21:40:00+00:00"
    assert first["last_fresh_actionable_source"] == "current_pulse"

    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T22:34:30+00:00",
                "fetch_meta": {"0xabc": {"api_errors": [], "fresh_buy_rows_le_10s_by_source": {"trade:user": 0}}},
            },
        },
    )
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T22:34:30+00:00"}],
    )

    second = _run_deadman(
        tmp_path,
        "--guard-event-log",
        "guard_events.jsonl",
        "--now",
        "2026-07-07T22:35:00+00:00",
    )

    assert second["status"] == "INCIDENT_GUARD_SIDE_HALT"
    halt = second["guard_side_halt_signal"]
    assert halt["fresh_actionable_signal_rows"] == 0
    assert halt["backstop_active"] is True
    assert halt["last_fresh_actionable_ts"] == "2026-07-07T21:40:00+00:00"
    assert halt["last_fresh_actionable_source"] == "previous_state"
    assert halt["last_fresh_actionable_newer_than_latest_order"] is True


def test_deadman_inventory_met_skip_is_measured_not_guard_side_halt(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "runtime_member_submittability": {
                "status": "INCIDENT_MEMBER_UNSUBMITTABLE",
                "effective_max_order_usd": 0.5,
                "configured_min_live_order_usd": 1.0,
            },
            "drought_funnel": {
                "active_set_fresh_signal_rows": 0,
                "reject_taxonomy_counts": {"inventory_target_already_met": 71},
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:59:30+00:00",
                "fetch_meta": {"0xabc": {"api_errors": [], "fresh_buy_rows_le_10s_by_source": {"trade:user": 0}}},
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "INCIDENT_GUARD_SIDE_HALT"
    halt = state["guard_side_halt_signal"]
    assert halt["member_unsubmittable"] is True
    assert halt["benign_inventory_met_skip"] is False
    assert halt["benign_skip_overrode"] == []
    assert halt["warning"] is None
    assert halt["active"] is True
    assert state["deadman_warning"] is None
    assert state["benign_skip_overrode"] == []
    assert state["_flow_line"]["deadman_warning"] is None
    assert state["_flow_line"]["benign_skip_overrode"] == []
    assert state["gated_quiet_classification"]["classification"] == "GUARD_SIDE_HALT"


def test_deadman_inventory_met_skip_without_override_has_no_warning(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "runtime_member_submittability": {"status": "PASS"},
            "drought_funnel": {
                "active_set_fresh_signal_rows": 0,
                "reject_taxonomy_counts": {"inventory_target_already_met": 71},
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:59:30+00:00",
                "fetch_meta": {"0xabc": {"api_errors": [], "fresh_buy_rows_le_10s_by_source": {"trade:user": 0}}},
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    halt = state["guard_side_halt_signal"]
    assert state["status"] == "MEASURED_SOURCE_QUIET"
    assert halt["benign_inventory_met_skip"] is True
    assert halt["benign_skip_overrode"] == []
    assert halt["warning"] is None
    assert state["deadman_warning"] is None
    assert state["benign_skip_overrode"] == []
    assert state["_flow_line"]["deadman_warning"] is None
    assert state["_flow_line"]["benign_skip_overrode"] == []


def test_deadman_accepts_fresh_guard_state_when_event_log_tail_is_empty(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "generated_at": "2026-07-07T21:59:20+00:00",
            "summary": {"can_trade": True},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(tmp_path / "guard_events.jsonl", [])

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "MEASURED_SOURCE_QUIET"
    assert state["annotation_liveness_source"] == "pipe_verified_source_quiet"
    assert state["latest_pipe_verified_source_quiet_ts"] == "2026-07-07T21:59:20+00:00"
    assert state["pipe_verified_source_quiet"]["guard_event_log_fresh"] is False
    assert state["pipe_verified_source_quiet"]["guard_state_fresh"] is True
    assert state["gated_quiet_classification"]["classification"] == "MEASURED_SOURCE_QUIET"
    assert not (tmp_path / "HANDOFF.md").exists()


def test_verified_source_quiet_remains_measured_after_four_hours(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T17:30:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "MEASURED_SOURCE_QUIET"
    assert state["gated_quiet_classification"]["classification"] == "MEASURED_SOURCE_QUIET"
    assert state["gated_quiet_classification"]["source_quiet_checks"]["pipe_verified_source_quiet"] is True
    assert (
        state["gated_quiet_classification"]["checks"]["source_quiet_idle_under_legacy_hard_backstop_s"]
        is False
    )
    assert state["gated_quiet_classification"]["source_quiet_hard_backstop_s"] == 3600.0
    assert (
        state["gated_quiet_classification"]["source_quiet_hard_backstop_mode"]
        == "demand_aware_selected_member_only"
    )
    assert not (tmp_path / "HANDOFF.md").exists()


def test_deadman_still_fires_when_source_has_fresh_rows(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T17:30:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 1},
                    }
                },
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 1},
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "INCIDENT_SELECTED_IDENTITY_UNRESOLVED"
    assert state["deadman_class"] == "SELECTED_IDENTITY_UNRESOLVED"
    assert state["pipe_verified_source_quiet"]["verified"] is False
    assert state["pipe_verified_source_quiet"]["source_quiet"] is False
    assert state["pipe_verified_source_quiet"]["fresh_buy_rows_le_10s"] == 1
    assert state["pipe_verified_source_quiet"]["active_set_fresh_signal_rows"] == 1
    assert (tmp_path / "HANDOFF.md").exists()


def test_deadman_treats_effective_min_floor_rows_as_not_actionable(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [
                {
                    "status": "LIVE_SUBMITTED",
                    "updated_at": "2026-07-07T20:20:00+00:00",
                }
            ],
        },
    )
    floor_rows = [
        {
            "dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
            "source_wallet": "0xd9189593701e94574ee140295a2ff6042317813b",
            "market_slug": "btc-updown-5m-1784127000",
            "condition_id": "0xabc",
            "outcome": "Up",
            "window_start_s": 1784127000.0,
            "window_budget_usd": 1.0,
            "gap_usd_at_vwap": 0.949601,
            "process_min_live_order_usd": 2.375011,
            "drip_min_tranche_usd": 2.375011,
            "probe_cap_min_order_floor_blocked": True,
        },
        {
            "dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
            "source_wallet": "0xd9189593701e94574ee140295a2ff6042317813b",
            "market_slug": "btc-updown-5m-1784126100",
            "condition_id": "0xdef",
            "outcome": "Up",
            "window_start_s": 1784126100.0,
            "window_budget_usd": 1.0,
            "gap_usd_at_vwap": 0.48,
            "process_min_live_order_usd": 2.4,
            "drip_min_tranche_usd": 2.4,
            "probe_cap_min_order_floor_blocked": True,
        },
    ]
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "generated_at": "2026-07-07T21:59:30+00:00",
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "drought_funnel": {
                "active_set_fresh_signal_rows": 2,
                "reject_taxonomy_counts": {
                    "drip_min_tranche_exceeds_window_budget": 2,
                },
            },
            "live_execution": {
                "candidate_intent_summary": {
                    "live_event_prefilter": {
                        "sample_filtered_events": floor_rows,
                    }
                }
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["deadman_class"] == "MEASURED_SOURCE_QUIET"
    assert state["pipe_verified_source_quiet"]["verified"] is True
    assert state["pipe_verified_source_quiet"]["source_quiet"] is True
    assert state["pipe_verified_source_quiet"]["active_set_fresh_signal_rows"] == 2
    assert state["pipe_verified_source_quiet"]["active_set_floor_blocked_signal_rows"] == 2
    assert state["pipe_verified_source_quiet"]["active_set_actionable_signal_rows"] == 0
    halt = state["guard_side_halt_signal"]
    assert halt["active"] is False
    assert halt["active_set_floor_blocked_signal_rows"] == 2
    assert halt["active_set_actionable_signal_rows"] == 0
    assert halt["fresh_actionable_signal_rows"] == 0
    assert not (tmp_path / "HANDOFF.md").exists()


def test_deadman_counts_above_floor_fresh_row_as_actionable_not_quiet(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [
                {
                    "status": "LIVE_SUBMITTED",
                    "updated_at": "2026-07-07T20:20:00+00:00",
                }
            ],
        },
    )
    rows = [
        {
            "dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
            "source_wallet": "0xd9189593701e94574ee140295a2ff6042317813b",
            "market_slug": "btc-updown-5m-1784127000",
            "condition_id": "0xabc",
            "outcome": "Up",
            "window_start_s": 1784127000.0,
            "window_budget_usd": 1.0,
            "gap_usd_at_vwap": 0.949601,
            "process_min_live_order_usd": 2.375011,
            "drip_min_tranche_usd": 2.375011,
            "probe_cap_min_order_floor_blocked": True,
        },
        {
            # Budget above the effective floor: not an honest floor skip,
            # must surface as actionable and defeat source-quiet.
            "dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
            "source_wallet": "0xd9189593701e94574ee140295a2ff6042317813b",
            "market_slug": "btc-updown-5m-1784126100",
            "condition_id": "0xdef",
            "outcome": "Up",
            "window_start_s": 1784126100.0,
            "window_budget_usd": 3.0,
            "gap_usd_at_vwap": 3.0,
            "process_min_live_order_usd": 2.4,
            "drip_min_tranche_usd": 2.4,
            "probe_cap_min_order_floor_blocked": True,
        },
    ]
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "generated_at": "2026-07-07T21:59:30+00:00",
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "drought_funnel": {
                "active_set_fresh_signal_rows": 2,
                "reject_taxonomy_counts": {
                    "drip_min_tranche_exceeds_window_budget": 2,
                },
            },
            "live_execution": {
                "candidate_intent_summary": {
                    "live_event_prefilter": {
                        "sample_filtered_events": rows,
                    }
                }
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    quiet = state["pipe_verified_source_quiet"]
    assert quiet["active_set_fresh_signal_rows"] == 2
    assert quiet["active_set_floor_blocked_signal_rows"] == 1
    assert quiet["active_set_actionable_signal_rows"] == 1
    assert quiet["source_quiet"] is False
    assert quiet["verified"] is False
    assert state["deadman_class"] == "GUARD_SIDE_HALT"
    assert state["status"] == "INCIDENT_GUARD_SIDE_HALT"
    halt = state["guard_side_halt_signal"]
    assert halt["active"] is True
    assert halt["active_set_floor_blocked_signal_rows"] == 1
    assert halt["active_set_actionable_signal_rows"] == 1
    assert (tmp_path / "HANDOFF.md").exists()


def test_deadman_backstop_fires_with_fresh_rows_despite_guard_activity(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T20:20:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 1},
                    }
                },
            },
            "drought_funnel": {
                "active_set_fresh_signal_rows": 1,
                "reject_taxonomy_counts": {"policy:price_outside_policy": 3},
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "INCIDENT_SELECTED_IDENTITY_UNRESOLVED"
    assert state["deadman_class"] == "SELECTED_IDENTITY_UNRESOLVED"
    assert state["guard_side_halt_signal"]["fresh_actionable_signal_rows"] == 2
    assert state["guard_side_halt_signal"]["guard_evaluation_absent_or_stale"] is False
    assert state["guard_side_halt_signal"]["backstop_active"] is True
    assert state["guard_side_halt_signal"]["backstop_s"] == 5400.0


def test_deadman_classifies_accounted_floor_deadlock_as_measured_once(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T20:20:00+00:00"}],
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "runtime_member_submittability": {
                "status": "PASS",
                "effective_max_order_usd": 1.0,
                "configured_min_live_order_usd": 1.0,
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 1},
                    }
                },
            },
            "drought_funnel": {
                "active_set_fresh_signal_rows": 0,
                "reject_taxonomy_counts": {
                    "drip_min_tranche_exceeds_window_budget": 12,
                    "inventory_late_window_guard": 3,
                    "inventory_target_already_met": 2,
                    "policy:price_outside_policy": 1,
                },
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "MEASURED_FLOOR_DEADLOCK"
    assert state["deadman_class"] == "FLOOR_DEADLOCK"
    assert state["consecutive_incidents"] == 0
    floor_deadlock = state["gated_quiet_classification"]["floor_deadlock_classification"]
    assert floor_deadlock["active"] is True
    assert floor_deadlock["member_submittable"] is True
    assert floor_deadlock["guard_event_fresh"] is True
    assert floor_deadlock["active_set_actionable_signal_rows"] == 0
    assert floor_deadlock["taxonomy_accounted"] is True
    assert not (tmp_path / "data/research/order_flow_deadman_incidents.jsonl").exists()
    handoff = (tmp_path / "HANDOFF.md").read_text(encoding="utf-8")
    assert handoff.count("ORDER_FLOW_DEADMAN FLOOR-DEADLOCK") == 1

    second = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert second["status"] == "MEASURED_FLOOR_DEADLOCK"
    handoff = (tmp_path / "HANDOFF.md").read_text(encoding="utf-8")
    assert handoff.count("ORDER_FLOW_DEADMAN FLOOR-DEADLOCK") == 1


def test_deadman_floor_deadlock_ignores_stale_participation_taxonomy(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T20:20:00+00:00"}],
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "runtime_member_submittability": {
                "status": "PASS",
                "effective_max_order_usd": 1.0,
                "configured_min_live_order_usd": 1.0,
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:58:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 1},
                    }
                },
            },
            "drought_funnel": {
                "active_set_fresh_signal_rows": 0,
                "reject_taxonomy_counts": {
                    "window:drip_min_tranche_exceeds_window_budget": 12,
                },
            },
            "window_participation": {
                "dominant_skip_reason_counts": {
                    "drip_min_tranche_exceeds_window_budget": 12,
                },
                "rows": [
                    {
                        "source_wallet": "0xabc",
                        "market_slug": "btc-updown-5m-1783332000",
                        "window_start_s": 1783332000,
                        "last_seen_at": "2026-07-06T10:00:00+00:00",
                        "dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
                    },
                    {
                        "source_wallet": "0xabc",
                        "market_slug": "btc-updown-5m-1783461000",
                        "window_start_s": 1783461000,
                        "last_seen_at": "2026-07-07T21:50:00+00:00",
                        "dominant_skip_reason": "window_time_gte_180s",
                    },
                    {
                        "source_wallet": "0xdef",
                        "market_slug": "btc-updown-5m-1783461300",
                        "window_start_s": 1783461300,
                        "last_seen_at": "2026-07-07T21:55:00+00:00",
                        "dominant_skip_reason": "inventory_late_window_guard",
                    },
                ],
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["deadman_class"] != "FLOOR_DEADLOCK"
    floor_deadlock = state["gated_quiet_classification"]["floor_deadlock_classification"]
    assert floor_deadlock["active"] is False
    assert floor_deadlock["floor_taxonomy_present"] is False
    assert "drip_min_tranche_exceeds_window_budget" not in floor_deadlock["taxonomy"]
    assert floor_deadlock["taxonomy"]["window_time_gte_180s"] == 1
    assert floor_deadlock["taxonomy"]["inventory_late_window_guard"] == 1
    assert floor_deadlock["lookback"]["rows_seen"] == 3
    assert floor_deadlock["lookback"]["rows_in_lookback"] == 2


def test_deadman_current_halt_stale_inventory_is_measured_timing_skip(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T20:00:00+00:00"}],
        },
    )
    _write_json(
        tmp_path / "deadman.json",
        {
            "last_fresh_actionable_ts": "2026-07-07T20:30:00+00:00",
            "guard_side_halt_signal": {"fresh_actionable_signal_rows": 1},
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "generated_at": "2026-07-07T21:59:30+00:00",
            "runtime_member_submittability": {"status": "PASS"},
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:59:30+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "window_participation": {
                "rows": [
                    {
                        "source_wallet": "0xabc",
                        "market_slug": "btc-updown-5m-1783461000",
                        "window_start_s": 1783461000,
                        "last_seen_at": "2026-07-07T21:50:00+00:00",
                        "dominant_skip_reason": "inventory_window_state_stale",
                    },
                    {
                        "source_wallet": "0xabc",
                        "market_slug": "btc-updown-5m-1783461300",
                        "window_start_s": 1783461300,
                        "last_seen_at": "2026-07-07T21:55:00+00:00",
                        "dominant_skip_reason": "window_time_gte_180s",
                        "observed_slug_epoch_delta_s": 190.0,
                        "guard_sized_copy_usd": 1.25,
                    },
                    {
                        "source_wallet": "0xdef",
                        "market_slug": "btc-updown-5m-1783461600",
                        "window_start_s": 1783461600,
                        "last_seen_at": "2026-07-07T21:58:00+00:00",
                        "dominant_skip_reason": "window_time_gte_180s",
                        "observed_slug_epoch_delta_s": 205.0,
                        "guard_sized_copy_usd": 2.0,
                    },
                ],
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "MEASURED_TIMING_SKIP"
    assert state["deadman_class"] == "MEASURED_TIMING_SKIP"
    halt = state["guard_side_halt_signal"]
    assert halt["backstop_active"] is True
    assert halt["last_fresh_actionable_source"] == "previous_state"
    timing = state["gated_quiet_classification"]["current_halt_timing_skip_classification"]
    assert timing["active"] is True
    assert timing["unapproved_gate_reasons"] == []
    assert timing["reasons"] == [
        "inventory_window_state_stale",
        "window_time_gte_180s",
    ]
    summary = state["window_time_near_miss_summary"]
    assert summary["reporting_only"] is True
    assert summary["count"] == 1
    assert summary["wallets"] == ["0xabc"]
    assert summary["would_size_usd_sum"] == 1.25
    assert summary["samples"][0]["elapsed_s"] == 190.0


def test_deadman_classifies_nonselected_active_set_demand_as_rotation_pressure(tmp_path: Path) -> None:
    selected = "0x32de91fa203321fa7735e7854f2b1c844e71ce9d"
    bench = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:15:00+00:00"}],
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "generated_at": "2026-07-07T21:59:30+00:00",
            "source_wallet": selected,
            "candidate_id": "runtime_auto_degrade_32de91fa20",
            "active_set_runtime": {
                "selected_member": {
                    "candidate_id": "runtime_auto_degrade_32de91fa20",
                    "source_wallet": selected,
                },
                "members": [
                    {"source_wallet": selected},
                    {"source_wallet": bench},
                ],
            },
            "runtime_member_submittability": {
                "status": "PASS",
                "source_wallet": selected,
                "candidate_id": "runtime_auto_degrade_32de91fa20",
                "generated_at": "2026-07-07T21:59:30+00:00",
                "configured_min_live_order_usd": 1.0,
                "effective_max_order_usd": 1.0,
            },
            "drought_funnel": {
                "active_set_fresh_signal_rows": 1,
                "reject_taxonomy_counts": {},
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:59:30+00:00",
                "summary": {
                    "fresh_poll_only_signals": 1,
                    "fresh_poll_only_by_wallet": {bench: 1},
                },
                "fetch_meta": {
                    selected: {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    },
                    bench: {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 1},
                        "freshest_buy_lag_s_by_source": {"trade:user": 2.4},
                    },
                },
            },
            "active_set_rtds_premerge": {
                "new_matching_events": 1,
                "rows": [
                    {
                        "source_wallet": selected,
                        "new_matching_events": 0,
                        "retained_matching_rows": 0,
                    },
                    {
                        "source_wallet": bench,
                        "new_matching_events": 1,
                        "retained_matching_rows": 7,
                        "latest_event_ts": 1784344770.0,
                        "latest_observed_ts": 1784344770.7,
                    },
                ],
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "MEASURED_NONSELECTED_DEMAND"
    assert state["deadman_class"] == "MEASURED_NONSELECTED_DEMAND"
    halt = state["guard_side_halt_signal"]
    assert halt["active"] is False
    assert halt["selected_fresh_actionable_signal_rows"] == 0
    assert halt["fresh_buy_rows_le_10s"] == 0
    assert halt["fresh_buy_rows_le_10s_total"] == 1
    pressure = halt["nonselected_rotation_pressure"]
    assert pressure["active"] is True
    assert pressure["classification"] == "MEASURED_NONSELECTED_DEMAND"
    assert pressure["wallets"] == [bench]
    assert pressure["samples"][0]["fresh_buy_rows_le_10s"] == 1
    assert state["gated_quiet_classification"]["source_quiet_checks"]["no_guard_side_halt_signal"] is True
    assert not (tmp_path / "HANDOFF.md").exists()


def test_deadman_nonselected_rotation_pressure_is_not_failed_by_wall_clock(tmp_path: Path) -> None:
    selected = "0x32de91fa203321fa7735e7854f2b1c844e71ce9d"
    bench = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T20:00:00+00:00"}],
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "generated_at": "2026-07-07T21:59:30+00:00",
            "source_wallet": selected,
            "candidate_id": "runtime_auto_degrade_32de91fa20",
            "active_set_runtime": {
                "selected_member": {
                    "candidate_id": "runtime_auto_degrade_32de91fa20",
                    "source_wallet": selected,
                },
                "members": [{"source_wallet": selected}, {"source_wallet": bench}],
            },
            "runtime_member_submittability": {
                "status": "PASS",
                "source_wallet": selected,
                "candidate_id": "runtime_auto_degrade_32de91fa20",
                "generated_at": "2026-07-07T21:59:30+00:00",
                "configured_min_live_order_usd": 1.0,
                "effective_max_order_usd": 1.0,
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 1, "reject_taxonomy_counts": {}},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:59:30+00:00",
                "summary": {
                    "fresh_poll_only_signals": 1,
                    "fresh_poll_only_by_wallet": {bench: 1},
                },
                "fetch_meta": {
                    selected: {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    },
                    bench: {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 1},
                        "freshest_buy_lag_s_by_source": {"trade:user": 2.4},
                    },
                },
            },
            "active_set_rtds_premerge": {
                "new_matching_events": 1,
                "rows": [
                    {"source_wallet": selected, "new_matching_events": 0, "retained_matching_rows": 0},
                    {
                        "source_wallet": bench,
                        "new_matching_events": 1,
                        "retained_matching_rows": 7,
                        "latest_event_ts": 1784344770.0,
                        "latest_observed_ts": 1784344770.7,
                    },
                ],
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "MEASURED_NONSELECTED_DEMAND"
    gated = state["gated_quiet_classification"]
    assert gated["classification"] == "MEASURED_NONSELECTED_DEMAND"
    assert gated["checks"]["idle_under_hard_backstop_s"] is True
    assert gated["nonselected_rotation_pressure"]["active"] is True
    assert not (tmp_path / "HANDOFF.md").exists()


def test_deadman_ignores_legacy_previous_nonselected_actionable_when_selected_split_exists(tmp_path: Path) -> None:
    selected = "0x32de91fa203321fa7735e7854f2b1c844e71ce9d"
    _write_json(
        tmp_path / "ledger.json",
        {
            "summary": {"can_trade": True},
            "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T20:00:00+00:00"}],
        },
    )
    _write_json(
        tmp_path / "deadman.json",
        {
            "last_fresh_actionable_ts": "2026-07-07T21:00:00+00:00",
            "guard_side_halt_signal": {"fresh_actionable_signal_rows": 2},
        },
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "generated_at": "2026-07-07T22:39:30+00:00",
            "source_wallet": selected,
            "candidate_id": "runtime_auto_degrade_32de91fa20",
            "active_set_runtime": {
                "selected_member": {
                    "candidate_id": "runtime_auto_degrade_32de91fa20",
                    "source_wallet": selected,
                }
            },
            "runtime_member_submittability": {
                "status": "PASS",
                "source_wallet": selected,
                "candidate_id": "runtime_auto_degrade_32de91fa20",
                "generated_at": "2026-07-07T22:39:30+00:00",
                "configured_min_live_order_usd": 1.0,
                "effective_max_order_usd": 1.0,
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T22:39:30+00:00",
                "fetch_meta": {
                    selected: {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T22:39:30+00:00"}],
    )

    state = _run_deadman(
        tmp_path,
        "--guard-event-log",
        "guard_events.jsonl",
        "--now",
        "2026-07-07T22:40:00+00:00",
    )

    assert state["status"] == "MEASURED_SOURCE_QUIET"
    assert state["deadman_class"] == "MEASURED_SOURCE_QUIET"
    halt = state["guard_side_halt_signal"]
    assert halt["active"] is False
    assert halt["backstop_active"] is False
    assert halt["last_fresh_actionable_ts"] is None
    assert halt["selected_fresh_actionable_signal_rows"] == 0
    assert not (tmp_path / "HANDOFF.md").exists()


def test_deadman_still_fires_when_source_quiet_poller_is_stale(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T21:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:40:00+00:00",
                "fetch_meta": {
                    "0xabc": {
                        "api_errors": [],
                        "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    }
                },
            },
            "drought_funnel": {"active_set_fresh_signal_rows": 0},
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    assert state["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    assert state["liveness_source"] == "accepted_order"
    assert state["pipe_verified_source_quiet"]["verified"] is False
    assert state["pipe_verified_source_quiet"]["poller_fresh"] is False
    assert "ORDER_FLOW_DEADMAN INCIDENT" in (tmp_path / "HANDOFF.md").read_text(encoding="utf-8")


def test_profit_latency_suppression_appends_approved_reject_event(tmp_path: Path) -> None:
    from scripts.run_wallet_copy_live_execution import _append_profit_latency_suppression_events

    event_log = tmp_path / "events.jsonl"
    written = _append_profit_latency_suppression_events(
        str(event_log),
        {
            "filtered_intents_detail": [
                {
                    "intent_id": "ci_1",
                    "source_wallet": "0xabc",
                    "market_slug": "btc-updown-5m-1",
                    "outcome": "Up",
                    "limit_price": 0.27,
                    "copy_size_usd": 1.25,
                    "window_time_s": 205.0,
                    "window_time_suppress_gte_s": 60.0,
                    "signal_age_s": 91.0,
                    "signal_age_suppress_gte_s": 60.0,
                    "event_ts": 1783000000.0,
                    "dataapi_first_seen_ts": 1783000091.0,
                    "api_indexing_lag_s": 91.0,
                    "poll_wait_s": None,
                    "latency_split_status": "DATAAPI_INDEXING_LAG_GTE_60S",
                    "latency_split_finding": "dataapi_indexing_lag_alone_cannot_pass_60s_signal_age_bar",
                    "taxonomy": "window_time_gte_180s",
                    "taxonomy_tags": ["window_time_gte_180s", "signal_age_gte_60s"],
                    "reject_reason": "window_time_gte_180s+signal_age_gte_60s",
                    "shadow_counterfactual_retained": True,
                }
            ]
        },
        decision_ts="2026-07-07T22:30:00+00:00",
    )

    rows = [json.loads(line) for line in event_log.read_text(encoding="utf-8").splitlines()]
    assert written == 1
    assert rows[0]["event"] == "wallet_copy_live_profit_latency_suppression_reject"
    assert rows[0]["approved_suppression"] is True
    assert rows[0]["event_type"] == "FABLE_APPROVED_SUPPRESSION_REJECT"
    assert rows[0]["taxonomy_tags"] == ["window_time_gte_180s", "signal_age_gte_60s"]
    assert rows[0]["window_time_suppress_gte_s"] == 60.0
    assert rows[0]["signal_age_suppress_gte_s"] == 60.0
    assert rows[0]["api_indexing_lag_s"] == 91.0
    assert rows[0]["latency_split_finding"] == "dataapi_indexing_lag_alone_cannot_pass_60s_signal_age_bar"


def test_deadman_accepts_dynamic_window_time_suppression_tags() -> None:
    from scripts.order_flow_deadman import _taxonomy_reason_class, _unapproved_gated_quiet_reasons

    assert _unapproved_gated_quiet_reasons({"window_time_gte_60s": 1}) == []
    assert _taxonomy_reason_class("window_time_gte_60s") == "approved_gated_quiet_taxonomy"


def test_deadman_price_band_reject_is_source_quiet_not_guard_side_halt(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "ledger.json",
        {"summary": {"can_trade": True}, "orders": [{"status": "LIVE_SUBMITTED", "updated_at": "2026-07-07T20:00:00+00:00"}]},
    )
    _write_json(
        tmp_path / "guard.json",
        {
            "summary": {"can_trade": True},
            "runtime_member_submittability": {"status": "PASS"},
            "drought_funnel": {
                "active_set_fresh_signal_rows": 1,
                "reject_taxonomy_counts": {
                    "policy:price_outside_policy": 1,
                    "window_time_gte_180s": 5,
                },
            },
            "live_execution": {
                "candidate_intent_summary": {
                    "live_event_prefilter": {
                        "sample_filtered_events": [
                            {
                                "source_wallet": "0xabc",
                                "market_slug": "btc-5m-test",
                                "outcome": "Up",
                                "window_start_s": 1783800000,
                                "skip_reason": "policy_price_outside_policy",
                                "source_price": 0.62,
                            }
                        ]
                    }
                }
            },
            "active_set_dataapi_poller": {
                "generated_at": "2026-07-07T21:59:30+00:00",
                "fetch_meta": {"0xabc": {"api_errors": [], "fresh_buy_rows_le_10s_by_source": {"trade:user": 1}}},
            },
        },
    )
    _write_jsonl(tmp_path / "events.jsonl", [])
    _write_jsonl(
        tmp_path / "guard_events.jsonl",
        [{"event": "wallet_copy_live_guard_cycle", "generated_at": "2026-07-07T21:59:30+00:00"}],
    )

    state = _run_deadman(tmp_path, "--guard-event-log", "guard_events.jsonl")

    halt = state["guard_side_halt_signal"]
    assert halt["active_set_price_policy_blocked_signal_rows"] == 1
    assert halt["active_set_actionable_signal_rows"] == 0
    assert halt["fresh_buy_rows_effective"] == 0
    assert halt["fresh_actionable_signal_rows"] == 0
    assert halt["backstop_active"] is False
    assert halt["active"] is False
    assert state["status"] != "INCIDENT_GUARD_SIDE_HALT"
    assert state["last_fresh_actionable_ts"] is None


def test_guard_memory_snapshot_warns_on_high_rss(monkeypatch, tmp_path: Path) -> None:
    now = dt.datetime(2026, 7, 18, 15, 0, tzinfo=dt.timezone.utc)
    previous = {
        "guard_memory": {
            "samples": [
                {
                    "checked_at": "2026-07-18T14:30:00+00:00",
                    "pid": 777,
                    "rss_gib": 20.0,
                }
            ]
        }
    }
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(29 * 1024 * 1024),
            "rss_kib": int(29 * 1024 * 1024),
            "phys_footprint_bytes": None,
        },
    )

    def fail_restart(*_args, **_kwargs):
        raise AssertionError("warn threshold must not restart")

    monkeypatch.setattr(deadman, "_run_memory_restart_actuator", fail_restart)

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {"pid": 777},
        previous,
        now,
        warn_gib=16.0,
        restart_gib=32.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    assert snapshot["status"] == "WARN"
    assert snapshot["pid"] == 777
    assert snapshot["rss_gib"] == 29.0
    assert snapshot["memory_probe"]["verified_pid"] == 777
    assert snapshot["memory_probe"]["raw_ps_line"] == str(29 * 1024 * 1024)
    assert snapshot["trend_gib"] == 9.0
    assert snapshot["trend_window_s"] == 1800.0
    assert snapshot["auto_restart"]["status"] == "NOT_APPLICABLE"
    assert snapshot["notify_grade"] is False
    assert snapshot["rule"] == (
        "warn at retained guard RSS peak >=16 GiB; at >=32 GiB run canonical brainless live-guard restart "
        "with 3600s cooldown"
    )


def test_guard_memory_restart_uses_retained_same_pid_peak(
    monkeypatch, tmp_path: Path
) -> None:
    previous = {
        "guard_memory": {
            "samples": [
                {
                    "checked_at": "2026-07-30T21:00:00+00:00",
                    "pid": 777,
                    "rss_gib": 6.25,
                },
                {
                    "checked_at": "2026-07-30T21:03:00+00:00",
                    "pid": 777,
                    "rss_gib": 1.25,
                },
            ]
        }
    }
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(2 * 1024 * 1024),
            "rss_kib": 2 * 1024 * 1024,
            "phys_footprint_bytes": None,
        },
    )
    calls = []
    monkeypatch.setattr(
        deadman,
        "_run_memory_restart_actuator",
        lambda root, script, *, cooldown_s: (
            calls.append((root, script, cooldown_s))
            or {"status": "RESTART_EXECUTED"}
        ),
    )

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {"pid": 777},
        previous,
        dt.datetime.fromisoformat("2026-07-30T21:06:00+00:00"),
        warn_gib=5.0,
        restart_gib=6.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    assert snapshot["rss_gib"] == 2.0
    assert snapshot["rss_observation"]["max_rss_gib"] == 6.25
    assert snapshot["threshold_rss_gib"] == 6.25
    assert snapshot["status"] == "AUTO_RESTART_EXECUTED"
    assert len(calls) == 1


def test_guard_memory_authority_includes_in_process_stage_peak(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(2 * 1024 * 1024),
            "rss_kib": 2 * 1024 * 1024,
            "phys_footprint_bytes": None,
        },
    )
    calls = []
    monkeypatch.setattr(
        deadman,
        "_run_memory_restart_actuator",
        lambda root, script, *, cooldown_s: (
            calls.append((root, script, cooldown_s))
            or {"status": "RESTART_EXECUTED"}
        ),
    )

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {
            "pid": 777,
            "guard_loop_profile": {
                "live_build_max_observed_age_s": 30.0,
                "cycle_duration_series": [
                    {"cycle_started_at": "2026-07-30T21:04:00Z", "cycle_duration_s": 25.0},
                    {"cycle_started_at": "2026-07-30T21:05:00Z", "cycle_duration_s": 38.0},
                ],
                "stage_timers": [
                    {"name": "one", "rss_gib": 2.5, "rss_sample_offset_s": 0.0},
                    {"name": "peak", "rss_gib": 6.25, "rss_sample_offset_s": 0.0},
                ]
            },
        },
        {},
        dt.datetime.fromisoformat("2026-07-30T21:06:00+00:00"),
        warn_gib=5.0,
        restart_gib=6.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    assert snapshot["threshold_rss_gib"] == 6.25
    assert snapshot["rss_observation_threshold_grade"] == "EXCLUDED_INSUFFICIENT_OBSERVATION"
    assert snapshot["cycle_duration_health"]["status"] == "WARN_CYCLE_EXCEEDS_FRESHNESS_BUDGET"
    assert snapshot["cycle_duration_health"]["max_s"] == 38.0
    assert snapshot["in_process_stage_boundary_rss"]["peak_stage"] == "peak"
    assert snapshot["in_process_stage_boundary_rss"]["sample_count"] == 2
    assert snapshot["status"] == "AUTO_RESTART_EXECUTED"
    assert len(calls) == 1
    assert snapshot["samples"][-1]["attribution"]["out_of_band"]["allocation_admissibility"] == (
        "INADMISSIBLE_SUPERSEDED_BY_IN_PROCESS_STAGE_BOUNDARIES"
    )


def test_process_memory_probe_uses_verified_pid_and_preserves_raw_ps_line(monkeypatch) -> None:
    calls = []

    class Completed:
        returncode = 0
        stdout = " 3732496\n"

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return Completed()

    monkeypatch.setattr(deadman.subprocess, "run", fake_run)

    probe = deadman._process_memory_probe(38019)

    assert calls[0][0] == ["ps", "-p", "38019", "-o", "rss="]
    assert probe["verified_pid"] == 38019
    assert probe["raw_ps_line"] == "3732496"
    assert probe["rss_kib"] == 3732496


def test_guard_memory_defaults_warn_before_binding_restart_threshold() -> None:
    assert deadman.GUARD_MEMORY_WARN_GIB == 5.0
    assert deadman.GUARD_MEMORY_RESTART_GIB == 6.0
    assert deadman.GUARD_MEMORY_WARN_GIB < deadman.GUARD_MEMORY_RESTART_GIB


def test_guard_memory_snapshot_records_stage_attribution_on_rss_spike(monkeypatch, tmp_path: Path) -> None:
    now = dt.datetime(2026, 7, 18, 15, 0, 5, tzinfo=dt.timezone.utc)
    previous = {
        "guard_memory": {
            "samples": [
                {"checked_at": "2026-07-18T14:59:00+00:00", "pid": 777, "rss_gib": 1.0},
                {"checked_at": "2026-07-18T14:59:30+00:00", "pid": 777, "rss_gib": 2.0},
                {"checked_at": "2026-07-18T15:00:00+00:00", "pid": 777, "rss_gib": 2.0},
            ]
        }
    }
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(5 * 1024 * 1024),
            "rss_kib": int(5 * 1024 * 1024),
            "phys_footprint_bytes": None,
        },
    )

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {
            "pid": 777,
            "cycle": 123,
            "cycle_outcome": "LIVE_GUARD_RUNNING",
            "guard_loop_profile": {
                "cycle_started_at": "2026-07-18T15:00:00+00:00",
                "cycle_duration_s": 10.0,
                "stage_timers": [
                    {"name": "select_active_set_runtime", "duration_s": 3.0, "elapsed_s": 3.0},
                    {"name": "live_execution", "duration_s": 5.0, "elapsed_s": 8.0},
                    {"name": "window_participation_merge", "duration_s": 2.0, "elapsed_s": 10.0},
                ],
            },
        },
        previous,
        now,
        warn_gib=16.0,
        restart_gib=32.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    sample = snapshot["samples"][-1]
    assert snapshot["status"] == "OK"
    out_of_band = sample["attribution"]["out_of_band"]
    assert out_of_band["cycle"] == 123
    assert out_of_band["active_stage"] == "live_execution"
    assert out_of_band["last_completed_stage"] == "window_participation_merge"
    assert out_of_band["allocation_admissibility"] == (
        "INADMISSIBLE_SUPERSEDED_BY_IN_PROCESS_STAGE_BOUNDARIES"
    )
    assert sample["rss_spike"]["status"] == "RSS_GT_2X_ROLLING_MEDIAN"
    assert sample["rss_spike"]["task_name"] == "live_execution"
    assert sample["rss_spike"]["multiple"] == 2.5


def test_guard_memory_snapshot_warns_on_retained_peak_and_slow_cycle(
    monkeypatch, tmp_path: Path
) -> None:
    now = dt.datetime(2026, 7, 29, 20, 42, 6, tzinfo=dt.timezone.utc)
    previous = {
        "guard_memory": {
            "samples": [
                {
                    "checked_at": "2026-07-29T20:36:34+00:00",
                    "pid": 777,
                    "rss_gib": 5.495,
                    "attribution": {"cycle_duration_s": 72.713191},
                },
                {
                    "checked_at": "2026-07-29T20:39:46+00:00",
                    "pid": 777,
                    "rss_gib": 1.714,
                    "attribution": {"cycle_duration_s": 72.394823},
                },
            ]
        }
    }
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(512 * 1024),
            "rss_kib": 512 * 1024,
            "phys_footprint_bytes": None,
        },
    )

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {
            "pid": 777,
            "guard_loop_profile": {
                "cycle_started_at": "2026-07-29T20:40:16+00:00",
                "cycle_duration_s": 109.33282,
                "live_build_max_observed_age_s": 30.0,
            },
        },
        previous,
        now,
        warn_gib=5.0,
        restart_gib=6.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    assert snapshot["rss_gib"] == 0.5
    assert snapshot["rss_peak_gib"] == 5.495
    assert snapshot["rss_peak_at"] == "2026-07-29T20:36:34+00:00"
    assert snapshot["rss_observation"] == {
        "status": "MEASURED",
        "pid": 777,
        "sample_count": 3,
        "span_s": 332.0,
        "min_rss_gib": 0.5,
        "max_rss_gib": 5.495,
        "amplitude_gib": 4.995,
        "required_sample_count": 3,
        "required_span_s": 120.0,
        "method": "same-PID retained RSS min/max/amplitude; no slope computed",
    }
    assert snapshot["status"] == "WARN"
    assert snapshot["auto_restart"]["status"] == "NOT_APPLICABLE"
    assert snapshot["cycle_duration_health"] == {
        "status": "WARN_CYCLE_EXCEEDS_FRESHNESS_BUDGET",
        "peak_basis": "retained",
        "min_s": 72.394823,
        "max_s": 109.33282,
        "max_at": "2026-07-29T20:42:06+00:00",
        "rolling_max_s": None,
        "rolling_max_at": None,
        "rolling_window_cycles": 0,
        "retained_max_s": 109.33282,
        "retained_max_at": "2026-07-29T20:42:06+00:00",
        "retained_sample_count": 3,
        "latest_s": 109.33282,
        "peak_decaying": False,
        "latest_under_budget": False,
        "latest_to_max_ratio": 1.0,
        "max_to_latest_elapsed_s": 0.0,
        "freshness_budget_s": 30.0,
        "max_to_freshness_ratio": 3.644427,
        "sample_count": 3,
        "rule": (
            "non-blocking measurement warning when the worse of retained "
            "reservoir max and rolling-window max exceeds the live-build "
            "freshness budget"
        ),
    }


def test_guard_memory_cycle_health_is_max_gated_on_all_four_legs(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(1024 * 1024),
            "rss_kib": 1024 * 1024,
            "phys_footprint_bytes": None,
        },
    )
    cases = [
        ([20.0, 25.0], "OK"),
        ([20.0, 35.0], "WARN_CYCLE_EXCEEDS_FRESHNESS_BUDGET"),
        ([38.0, 25.0, 34.0, 24.0], "WARN_CYCLE_EXCEEDS_FRESHNESS_BUDGET"),
        ([38.0, 34.0, 30.0, 24.0], "WARN_STALE_CYCLE_PEAK_DECAYING"),
    ]
    for index, (durations, expected) in enumerate(cases):
        series = [
            {
                "cycle_started_at": f"2026-07-30T21:{minute:02d}:00Z",
                "cycle_duration_s": duration,
            }
            for minute, duration in enumerate(durations)
        ]
        snapshot = deadman._guard_memory_snapshot(
            tmp_path,
            {
                "pid": 777,
                "guard_loop_profile": {
                    "live_build_max_observed_age_s": 30.0,
                    "cycle_duration_series": series,
                },
            },
            {},
            dt.datetime(2026, 7, 30, 22, index, tzinfo=dt.timezone.utc),
            warn_gib=5.0,
            restart_gib=6.0,
            cooldown_s=3600.0,
            restart_script="scripts/brainless_live_guard_restart.py",
        )
    assert snapshot["cycle_duration_health"]["status"] == expected


def test_guard_memory_cycle_health_uses_retained_peak_outside_rolling_window(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(1024 * 1024),
            "rss_kib": 1024 * 1024,
            "phys_footprint_bytes": None,
        },
    )
    previous = {
        "guard_memory": {
            "samples": [
                {
                    "checked_at": "2026-07-30T20:00:00Z",
                    "pid": 777,
                    "rss_gib": 1.0,
                    "attribution": {"out_of_band": {"cycle_duration_s": 50.37683}},
                }
            ]
        }
    }
    rolling_series = [
        {
            "cycle_started_at": f"2026-07-30T21:{minute:02d}:00Z",
            "cycle_duration_s": 20.0 + minute,
        }
        for minute in range(12)
    ]

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {
            "pid": 777,
            "guard_loop_profile": {
                "live_build_max_observed_age_s": 30.0,
                "cycle_duration_series": rolling_series,
            },
        },
        previous,
        dt.datetime(2026, 7, 30, 21, 12, tzinfo=dt.timezone.utc),
        warn_gib=5.0,
        restart_gib=6.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    health = snapshot["cycle_duration_health"]
    assert health["rolling_max_s"] == 31.0
    assert health["rolling_window_cycles"] == 12
    assert health["retained_max_s"] == 50.37683
    assert health["retained_max_at"] == "2026-07-30T20:00:00Z"
    assert health["peak_basis"] == "retained"
    assert health["sample_count"] == health["retained_sample_count"]
    assert health["max_s"] == 50.37683
    assert health["max_at"] == "2026-07-30T20:00:00Z"
    assert health["status"] == "WARN_CYCLE_EXCEEDS_FRESHNESS_BUDGET"


def test_guard_memory_cycle_health_keeps_rolling_peak_when_not_worse_than_retained(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(1024 * 1024),
            "rss_kib": 1024 * 1024,
            "phys_footprint_bytes": None,
        },
    )
    previous = {
        "guard_memory": {
            "samples": [
                {
                    "checked_at": "2026-07-30T20:00:00Z",
                    "pid": 777,
                    "rss_gib": 1.0,
                    "attribution": {"out_of_band": {"cycle_duration_s": 42.0}},
                }
            ]
        }
    }
    rolling_series = [
        {"cycle_started_at": "2026-07-30T21:00:00Z", "cycle_duration_s": 20.0},
        {"cycle_started_at": "2026-07-30T21:01:00Z", "cycle_duration_s": 50.37683},
        {"cycle_started_at": "2026-07-30T21:02:00Z", "cycle_duration_s": 25.0},
    ]

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {
            "pid": 777,
            "guard_loop_profile": {
                "live_build_max_observed_age_s": 30.0,
                "cycle_duration_series": rolling_series,
            },
        },
        previous,
        dt.datetime(2026, 7, 30, 21, 3, tzinfo=dt.timezone.utc),
        warn_gib=5.0,
        restart_gib=6.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    health = snapshot["cycle_duration_health"]
    assert health["rolling_max_s"] == 50.37683
    assert health["retained_max_s"] == 42.0
    assert health["peak_basis"] == "rolling"
    assert health["sample_count"] == health["rolling_window_cycles"]
    assert health["max_s"] == 50.37683
    assert health["max_at"] == "2026-07-30T21:01:00Z"
    assert health["status"] == "WARN_STALE_CYCLE_PEAK_DECAYING"


def test_guard_memory_cycle_health_degrades_to_rolling_when_retained_empty(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(1024 * 1024),
            "rss_kib": 1024 * 1024,
            "phys_footprint_bytes": None,
        },
    )

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {
            "pid": 777,
            "guard_loop_profile": {
                "live_build_max_observed_age_s": 30.0,
                "cycle_duration_series": [
                    {"cycle_started_at": "2026-07-30T21:00:00Z", "cycle_duration_s": 20.0},
                    {"cycle_started_at": "2026-07-30T21:01:00Z", "cycle_duration_s": 35.0},
                ],
            },
        },
        {},
        dt.datetime(2026, 7, 30, 21, 2, tzinfo=dt.timezone.utc),
        warn_gib=5.0,
        restart_gib=6.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    health = snapshot["cycle_duration_health"]
    assert health["rolling_max_s"] == 35.0
    assert health["retained_max_s"] is None
    assert health["retained_sample_count"] == 0
    assert health["peak_basis"] == "rolling"
    assert health["sample_count"] == health["rolling_window_cycles"]
    assert health["max_s"] == 35.0
    assert health["status"] == "WARN_CYCLE_EXCEEDS_FRESHNESS_BUDGET"


def test_guard_memory_fallback_cycle_samples_keep_max_timestamp_aligned(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(1024 * 1024),
            "rss_kib": 1024 * 1024,
            "phys_footprint_bytes": None,
        },
    )
    previous = {
        "guard_memory": {
            "samples": [
                {
                    "checked_at": "2026-07-30T21:00:00Z",
                    "pid": 777,
                    "rss_gib": 1.0,
                    "attribution": {
                        "out_of_band": {"cycle_duration_s": 45.0}
                    },
                },
                {"checked_at": "2026-07-30T21:01:00Z", "pid": 777, "rss_gib": 1.0},
                {
                    "checked_at": "2026-07-30T21:02:00Z",
                    "pid": 777,
                    "rss_gib": 1.0,
                    "attribution": {
                        "out_of_band": {"cycle_duration_s": 20.0}
                    },
                },
            ]
        }
    }
    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {
            "pid": 777,
            "guard_loop_profile": {
                "cycle_duration_s": 25.0,
                "live_build_max_observed_age_s": 30.0,
            },
        },
        previous,
        dt.datetime(2026, 7, 30, 21, 3, tzinfo=dt.timezone.utc),
        warn_gib=5.0,
        restart_gib=6.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    assert snapshot["cycle_duration_health"]["max_s"] == 45.0
    assert snapshot["cycle_duration_health"]["max_at"] == "2026-07-30T21:00:00Z"
    assert snapshot["cycle_duration_health"]["sample_count"] >= 3


def test_guard_memory_out_of_band_nesting_is_idempotent_across_three_cuts(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(1024 * 1024),
            "rss_kib": 1024 * 1024,
            "phys_footprint_bytes": None,
        },
    )
    previous = {}
    for minute in range(3):
        snapshot = deadman._guard_memory_snapshot(
            tmp_path,
            {
                "pid": 777,
                "guard_loop_profile": {
                    "cycle_started_at": f"2026-07-30T21:0{minute}:00Z",
                    "cycle_duration_s": 35.0 - minute,
                    "live_build_max_observed_age_s": 30.0,
                },
            },
            previous,
            dt.datetime(2026, 7, 30, 21, minute, 30, tzinfo=dt.timezone.utc),
            warn_gib=5.0,
            restart_gib=6.0,
            cooldown_s=3600.0,
            restart_script="scripts/brainless_live_guard_restart.py",
        )
        previous = {"guard_memory": snapshot}
        for sample in snapshot["samples"]:
            attribution = sample["attribution"]
            assert set(attribution) == {"out_of_band"}
            assert "out_of_band" not in attribution["out_of_band"]
    assert snapshot["cycle_duration_health"]["sample_count"] == 3
    assert snapshot["cycle_duration_health"]["status"].startswith("WARN")


def test_guard_memory_cycle_peak_is_dated_and_decay_is_named(
    monkeypatch,
    tmp_path: Path,
) -> None:
    rows = [
        ("2026-07-29T20:39:46.773642+00:00", 72.394823),
        ("2026-07-29T20:42:06.565595+00:00", 109.33282),
        ("2026-07-29T20:57:18.387028+00:00", 87.001042),
        ("2026-07-29T21:01:32.208423+00:00", 65.731388),
        ("2026-07-29T21:18:11.812248+00:00", 47.840327),
    ]
    previous = {
        "guard_memory": {
            "samples": [
                {
                    "checked_at": checked_at,
                    "pid": 777,
                    "rss_gib": 1.0,
                    "attribution": {"cycle_duration_s": duration_s},
                }
                for checked_at, duration_s in rows
            ]
        }
    }
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(1024 * 1024),
            "rss_kib": 1024 * 1024,
            "phys_footprint_bytes": None,
        },
    )

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {
            "pid": 777,
            "guard_loop_profile": {
                "cycle_duration_s": 36.935803,
                "live_build_max_observed_age_s": 30.0,
            },
        },
        previous,
        dt.datetime.fromisoformat("2026-07-29T21:30:43.278516+00:00"),
        warn_gib=5.0,
        restart_gib=6.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    health = snapshot["cycle_duration_health"]
    assert health["status"] == "WARN_STALE_CYCLE_PEAK_DECAYING"
    assert health["max_at"] == "2026-07-29T20:42:06.565595+00:00"
    assert health["latest_s"] == 36.935803
    assert health["latest_to_max_ratio"] == 0.33783
    assert health["max_to_latest_elapsed_s"] == 2916.712921


def test_guard_memory_snapshot_runs_preapproved_restart(monkeypatch, tmp_path: Path) -> None:
    now = dt.datetime(2026, 7, 18, 15, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(33 * 1024 * 1024),
            "rss_kib": int(33 * 1024 * 1024),
            "phys_footprint_bytes": None,
        },
    )
    calls = []

    def fake_restart(root, restart_script, *, cooldown_s):
        calls.append((root, restart_script, cooldown_s))
        return {
            "status": "RESTART_EXECUTED",
            "returncode": 0,
            "decision": {"status": "RESTART_EXECUTED", "execution": {"started_pid": 888}},
        }

    monkeypatch.setattr(deadman, "_run_memory_restart_actuator", fake_restart)

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {"guard_code_identity": {"pid": 777}},
        {},
        now,
        warn_gib=16.0,
        restart_gib=16.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    assert snapshot["status"] == "AUTO_RESTART_EXECUTED"
    assert snapshot["auto_restart"]["status"] == "RESTART_EXECUTED"
    assert snapshot["notify_grade"] is False
    assert calls == [(tmp_path, "scripts/brainless_live_guard_restart.py", 3600.0)]


def test_guard_memory_snapshot_notifies_inside_restart_cooldown(monkeypatch, tmp_path: Path) -> None:
    now = dt.datetime(2026, 7, 18, 15, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(
        deadman,
        "_process_memory_probe",
        lambda pid: {
            "verified_pid": pid,
            "returncode": 0,
            "raw_ps_line": str(33 * 1024 * 1024),
            "rss_kib": int(33 * 1024 * 1024),
            "phys_footprint_bytes": None,
        },
    )

    def fake_restart(_root, _restart_script, *, cooldown_s):
        return {
            "status": "WATCH_COOLDOWN",
            "returncode": 0,
            "decision": {
                "status": "WATCH_COOLDOWN",
                "storm_guard": {"cooldown_remaining_s": cooldown_s},
            },
        }

    monkeypatch.setattr(deadman, "_run_memory_restart_actuator", fake_restart)

    snapshot = deadman._guard_memory_snapshot(
        tmp_path,
        {"pid": 777},
        {},
        now,
        warn_gib=16.0,
        restart_gib=16.0,
        cooldown_s=3600.0,
        restart_script="scripts/brainless_live_guard_restart.py",
    )

    assert snapshot["status"] == "AUTO_RESTART_SUPPRESSED_NOTIFY"
    assert snapshot["auto_restart"]["status"] == "WATCH_COOLDOWN"
    assert snapshot["notify_grade"] is True


def _direct_wide_packet(now: dt.datetime, wallet: str) -> dict:
    return {
        "updated_at": (now - dt.timedelta(seconds=2)).isoformat(),
        "policy_id": "p",
        "manifest": {"manifest_id": "manifest"},
        "cohort": {"cohort_id": "cohort", "run_id": "run"},
        "terminal_reconciliation": {
            "direct_event_handoff": True,
            "input_equals_terminal": True,
            "input_rows": 12,
            "terminal_rows": 12,
        },
        "attempt_terminals": [
            {
                "attempt_id": f"attempt-{index}",
                "order_id": f"order-{index}" if index <= 8 else None,
                "wallet": wallet,
                "recorded_at": (now - dt.timedelta(seconds=index)).isoformat(),
                "f1_f4_terminal": {
                    "terminal": (
                        "COPYABLE_EXACT_POLICY_PAPER_FILL"
                        if index <= 8
                        else "REFUSED_ALPHA_PROFILE_FILTER"
                    ),
                    "F4_executable_book": "PASS" if index <= 8 else "NOT_EVALUATED",
                },
            }
            for index in range(1, 13)
        ],
        "orders": [
            {
                "order_id": f"order-{index}",
                "wallet": wallet,
                "recorded_at": (now - dt.timedelta(seconds=index)).isoformat(),
                "f1_f4_terminal": {
                    "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
                    "F4_executable_book": "PASS",
                },
            }
            for index in range(1, 9)
        ],
    }


def test_direct_wide_snapshot_requires_fresh_exact_terminal_coverage() -> None:
    now = dt.datetime(2026, 7, 25, 10, 0, tzinfo=dt.timezone.utc)
    wallet = "0x" + "d" * 40
    packet = _direct_wide_packet(now, wallet)
    fresh = deadman._wide_direct_source_snapshot(packet, now=now)
    assert fresh["ready"] is True
    assert fresh["current_attempted_buy_rows"] == 12
    assert fresh["per_wallet"][wallet]["copyable"] == 8
    assert fresh["per_wallet"][wallet]["terminal_taxonomy"] == {
        "COPYABLE_EXACT_POLICY_PAPER_FILL": 8,
        "REFUSED_ALPHA_PROFILE_FILTER": 4,
    }
    assert fresh["envelope_copyable_orders_in_window"] == 8
    assert fresh["copyable_cross_source_parity"] is True

    missing_witness = deadman._wide_direct_source_snapshot(
        {**packet, "orders": packet["orders"][:-1]},
        now=now,
    )
    assert missing_witness["ready"] is True
    assert missing_witness["current_copyable_rows"] == 8
    assert missing_witness["envelope_copyable_orders_in_window"] == 7
    assert missing_witness["copyable_cross_source_parity"] is False

    stale = deadman._wide_direct_source_snapshot(
        {**packet, "updated_at": (now - dt.timedelta(seconds=31)).isoformat()},
        now=now,
    )
    assert stale["ready"] is False
    incomplete_packet = dict(packet)
    incomplete_packet["terminal_reconciliation"] = {
        **packet["terminal_reconciliation"],
        "input_equals_terminal": False,
        "terminal_rows": 11,
    }
    incomplete = deadman._wide_direct_source_snapshot(incomplete_packet, now=now)
    assert incomplete["ready"] is False


def test_direct_wide_snapshot_uses_journal_supply_when_fresh_packet_delta_empty() -> None:
    now = dt.datetime(2026, 7, 25, 10, 0, tzinfo=dt.timezone.utc)
    wallet = "0x" + "d" * 40
    payload = _direct_wide_packet(now, wallet)
    envelope = deadman.envelope_from_packet(payload)
    empty = {
        **payload,
        "terminal_reconciliation": {
            "input_rows": 0,
            "terminal_rows": 0,
            "input_equals_terminal": True,
            "direct_event_handoff": True,
        },
        "attempt_terminals": [],
        "orders": [],
    }

    generation = deadman._wide_direct_generation_snapshot(empty, now=now)
    supplied = deadman._wide_direct_source_snapshot(
        empty, now=now, journal=[envelope]
    )
    empty_window = deadman._wide_direct_source_snapshot(empty, now=now, journal=[])

    assert generation["ready"] is True
    assert generation["current_attempted_buy_rows"] == 0
    assert supplied["ready"] is True
    assert supplied["current_attempted_buy_rows"] == 12
    assert empty_window["ready"] is False
    assert empty_window["current_attempted_buy_rows"] == 0


def test_direct_supply_continuity_requires_three_of_four_windows() -> None:
    now = dt.datetime(2026, 8, 1, 12, 0, tzinfo=dt.timezone.utc)
    wallet_pass = "0x" + "a" * 40
    wallet_fail = "0x" + "b" * 40
    rows = []
    for index in (0, 1, 3):
        rows.append(
            {
                "wallet": wallet_pass,
                "attempt_id": f"pass-{index}",
                "recorded_at": (now - dt.timedelta(seconds=1800 * index + 5)).isoformat(),
                "f1_f4_terminal": {"terminal": "REFUSED_TEST"},
            }
        )
    copyable_orders = [
        {
            "wallet": wallet_pass,
            "order_id": f"copy-{index}",
            "recorded_at": (
                now - dt.timedelta(seconds=1800 * index + 5)
            ).isoformat(),
        }
        for index in (0, 1, 3)
    ]
    for index in (0, 2):
        rows.append(
            {
                "wallet": wallet_fail,
                "attempt_id": f"fail-{index}",
                "recorded_at": (now - dt.timedelta(seconds=1800 * index + 5)).isoformat(),
                "f1_f4_terminal": {"terminal": "REFUSED_TEST"},
            }
        )
    continuity = deadman._direct_supply_continuity(
        [
            {
                "source_generation": "generation",
                "input_equals_terminal": True,
                "rows": rows,
                "copyable_orders": copyable_orders,
            }
        ],
        now=now,
        window_s=1800.0,
    )

    assert continuity["per_wallet"][wallet_pass]["attempts_by_window"] == [1, 1, 0, 1]
    assert continuity["per_wallet"][wallet_pass]["copyables_by_window"] == [1, 1, 0, 1]
    assert continuity["per_wallet"][wallet_pass]["attempt_windows_present"] == 3
    assert continuity["per_wallet"][wallet_pass]["copyable_windows_present"] == 3
    assert continuity["per_wallet"][wallet_pass]["attempt_pass"] is True
    assert continuity["per_wallet"][wallet_pass]["copyable_pass"] is True
    assert continuity["per_wallet"][wallet_fail]["attempt_windows_present"] == 2
    assert continuity["per_wallet"][wallet_fail]["copyable_windows_present"] == 0
    assert continuity["per_wallet"][wallet_fail]["attempt_pass"] is False
    assert continuity["per_wallet"][wallet_fail]["copyable_pass"] is False
    assert continuity["attempt_passing_wallet_count"] == 1
    assert continuity["copyable_passing_wallet_count"] == 1


def test_empty_direct_generation_names_fetch_zero_and_envelope_preserves_it() -> None:
    packet = {
        "updated_at": "2026-08-01T12:00:00+00:00",
        "terminal_reconciliation": {
            "direct_event_handoff": True,
            "input_equals_terminal": True,
            "input_rows": 0,
            "terminal_rows": 0,
        },
        "attempt_terminals": [],
        "orders": [],
        "generation_flow": {
            "empty_generation": True,
            "empty_stage": "FETCH_ZERO",
            "stages": {"fetch": {"input_direct_rows": 0, "output_rows": 0}},
        },
    }

    envelope = deadman.envelope_from_packet(packet)

    assert envelope is not None
    assert envelope["generation_flow"]["empty_stage"] == "FETCH_ZERO"

    summary = deadman._direct_generation_flow_summary(
        [
            {
                **envelope,
                "captured_at": "2026-08-01T12:00:00+00:00",
            },
            {
                **envelope,
                "captured_at": "2026-08-01T13:00:00+00:00",
                "input_rows": 1,
                "copyable_orders": [{"order_id": "copy"}],
                "generation_flow": {
                    "empty_generation": False,
                    "empty_stage": None,
                },
            },
        ]
    )
    assert summary["instrumented_after"]["empty_generation_rate_pct"] == 50.0
    assert summary["instrumented_after"]["copyables_per_hour"] == 1.0
    assert summary["instrumented_empty_stage_counts"] == {
        "FETCH_ZERO": 1,
        "NONEMPTY": 1,
    }


def test_direct_wide_snapshot_refuses_copyable_taxonomy_parity_divergence() -> None:
    now = dt.datetime(2026, 7, 25, 10, 0, tzinfo=dt.timezone.utc)
    wallet = "0x" + "d" * 40
    packet = _direct_wide_packet(now, wallet)
    packet["attempt_terminals"][1]["order_id"] = "order-1"

    snapshot = deadman._wide_direct_source_snapshot(packet, now=now)

    assert snapshot["current_copyable_rows"] == 7
    assert snapshot["copyable_terminal_rows"] == 8
    assert snapshot["copyable_parity"] is False
    assert snapshot["status"] == "FAIL_COPYABLE_PARITY"
    assert snapshot["ready"] is False


def test_direct_wide_snapshot_names_latest_non_empty_generation_by_receipt() -> None:
    now = dt.datetime(2026, 8, 2, 10, 0, tzinfo=dt.timezone.utc)
    wallet = "0x" + "d" * 40
    older = _direct_wide_packet(now - dt.timedelta(minutes=10), wallet)
    older["policy_id"] = "older-policy"
    newer = _direct_wide_packet(now, wallet)
    newer["policy_id"] = "newer-policy"
    older_envelope = deadman.envelope_from_packet(older)

    snapshot = deadman._wide_direct_source_snapshot(
        newer,
        now=now,
        journal=[older_envelope],
    )

    index = snapshot["latest_non_empty_generation_index"]
    selected = snapshot["generations"][index]
    assert selected["identity"]["policy_id"] == "newer-policy"
    assert snapshot["latest_non_empty_generation_receipt_at"] == selected["latest_receipt_at"]
    assert dt.datetime.fromisoformat(selected["latest_receipt_at"]) > dt.datetime.fromisoformat(
        next(
            row["latest_receipt_at"]
            for row in snapshot["generations"]
            if row["identity"]["policy_id"] == "older-policy"
        )
    )


def test_direct_source_summary_populates_measured_generation_aliases() -> None:
    now = dt.datetime(2026, 8, 2, 10, 0, tzinfo=dt.timezone.utc)
    wallet = "0x" + "d" * 40

    snapshot = deadman._wide_direct_source_snapshot(
        _direct_wide_packet(now, wallet),
        now=now,
    )

    assert snapshot["fresh_rows"] == snapshot["current_attempted_buy_rows"] == 12
    assert snapshot["copyable"] == snapshot["current_copyable_rows"] == 8
    assert snapshot["latest_receipt_at"] == snapshot[
        "latest_non_empty_generation_receipt_at"
    ]
    assert snapshot["direct_ready"] is snapshot["ready"] is True


def test_direct_source_pass_is_forbidden_when_summary_counters_are_null() -> None:
    evidence = deadman._select_policy_choke_candidate_pool(
        pool=[],
        overlay={"members": []},
        hot_history={"events": []},
        now=dt.datetime(2026, 8, 2, 10, 0, tzinfo=dt.timezone.utc),
        regime="weekend",
        temporal_registry={},
        direct_source={
            "ready": True,
            "status": "PASS",
            "fresh_rows": None,
            "copyable": None,
            "latest_receipt_at": None,
        },
    )

    assert evidence["f4_root_defect"]["direct_status"] == "UNMEASURED"
    assert evidence["f4_root_defect"]["direct_ready"] is False


def test_zero_runtime_rows_with_fresh_direct_supply_fires_source_roster_drought() -> None:
    result = deadman._source_roster_drought_incident(
        can_trade=True,
        accepted_order_idle_s=1801.0,
        runtime_fresh_rows=0,
        direct_source={"ready": True},
        candidates={"selected": None},
    )
    assert result["status"] == "INCIDENT_SOURCE_ROSTER_DROUGHT"
    assert result["mechanical_escalation"] == "RUNG_C_METHOD_SWITCH_DUE"


def test_source_drought_method_switch_survives_empty_roster_live_build_state() -> None:
    result = deadman._source_roster_drought_incident(
        can_trade=False,
        live_build_authorized=True,
        accepted_order_idle_s=1801.0,
        runtime_fresh_rows=0,
        direct_source={"ready": True},
        candidates={"selected": None},
    )
    assert result["status"] == "INCIDENT_SOURCE_ROSTER_DROUGHT"
    assert result["mechanical_escalation"] == "RUNG_C_METHOD_SWITCH_DUE"


def test_rung_c_label_reports_recorded_terminal_successors_as_satisfied() -> None:
    label, evidence = deadman._rung_c_escalation_label(
        recovery_ttl={"status": "RECOVERY_TTL_DECISION_ALREADY_RECORDED"},
        successor_states=[
            {
                "kind": "multivenue_passive_residual",
                "status": "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE",
                "stop_writer": True,
            },
            {
                "kind": "multivenue_maker_first_residual",
                "status": "PARK_ZERO_INTENT_GENERATION",
                "stop_writer": True,
            },
        ],
    )

    assert label == "RUNG_C_SATISFIED_BY_RECORDED_DECISION"
    assert evidence["status"] == "SATISFIED_BY_RECORDED_DECISION"
    assert all(row["terminal"] for row in evidence["successors"])


def test_rung_c_label_stays_due_until_every_successor_is_terminal() -> None:
    label, evidence = deadman._rung_c_escalation_label(
        recovery_ttl={"status": "RECOVERY_TTL_DECISION_ALREADY_RECORDED"},
        successor_states=[
            {
                "kind": "multivenue_passive_residual",
                "status": "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE",
                "stop_writer": True,
            },
            {
                "kind": "multivenue_maker_first_residual",
                "status": "OBSERVING",
                "stop_writer": False,
            },
        ],
    )

    assert label == "RUNG_C_METHOD_SWITCH_DUE"
    assert evidence["status"] == "METHOD_SWITCH_NOT_TERMINALLY_SETTLED"


def test_direct_source_candidate_uses_current_supply_without_prior_acceptance_and_excludes_terminal() -> None:
    now = dt.datetime(2026, 7, 25, 10, 0, tzinfo=dt.timezone.utc)
    candidate_wallet = "0x" + "d" * 40
    parked_wallet = "0x" + "e" * 40
    direct = deadman._wide_direct_source_snapshot(
        {
            **_direct_wide_packet(now, candidate_wallet),
            "terminal_reconciliation": {
                **_direct_wide_packet(now, candidate_wallet)["terminal_reconciliation"],
                "input_rows": 24,
                "terminal_rows": 24,
            },
            "attempt_terminals": [
                *_direct_wide_packet(now, candidate_wallet)["attempt_terminals"],
                *[
                    {
                        "attempt_id": f"parked-{index}",
                        "order_id": f"parked-order-{index}" if index <= 9 else None,
                        "wallet": parked_wallet,
                        "recorded_at": (now - dt.timedelta(seconds=index)).isoformat(),
                        "f1_f4_terminal": {
                            "terminal": (
                                "COPYABLE_EXACT_POLICY_PAPER_FILL"
                                if index <= 9
                                else "REFUSED_ALPHA_PROFILE_FILTER"
                            ),
                            "F4_executable_book": (
                                "PASS" if index <= 9 else "NOT_EVALUATED"
                            ),
                        },
                    }
                    for index in range(1, 13)
                ],
            ],
            "orders": [
                *_direct_wide_packet(now, candidate_wallet)["orders"],
                *[
                    {
                        "order_id": f"parked-order-{index}",
                        "wallet": parked_wallet,
                        "recorded_at": (now - dt.timedelta(seconds=index)).isoformat(),
                        "f1_f4_terminal": {
                            "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
                            "F4_executable_book": "PASS",
                        },
                    }
                    for index in range(1, 10)
                ],
            ],
        },
        now=now,
        journal=[
            {
                "captured_at": (now - dt.timedelta(seconds=1805)).isoformat(),
                "source_generation": "continuity-1",
                "identity": {"policy_id": "p"},
                "input_rows": 1,
                "terminal_rows": 1,
                "input_equals_terminal": True,
                "rows": [
                    {
                        "attempt_id": "continuity-1",
                        "wallet": candidate_wallet,
                        "recorded_at": (now - dt.timedelta(seconds=1805)).isoformat(),
                        "f1_f4_terminal": {"terminal": "REFUSED_TEST"},
                    }
                ],
            },
            {
                "captured_at": (now - dt.timedelta(seconds=3605)).isoformat(),
                "source_generation": "continuity-2",
                "identity": {"policy_id": "p"},
                "input_rows": 1,
                "terminal_rows": 1,
                "input_equals_terminal": True,
                "rows": [
                    {
                        "attempt_id": "continuity-2",
                        "wallet": candidate_wallet,
                        "recorded_at": (now - dt.timedelta(seconds=3605)).isoformat(),
                        "f1_f4_terminal": {"terminal": "REFUSED_TEST"},
                    }
                ],
            },
        ],
    )
    pool = [
        {
            "wallet": wallet,
            "paper_policy_id": "p",
            "retrospective_gross_pnl_usd": 25.0,
            "retrospective_gross_roi_pct": 5.0,
            "retrospective_resolved_signals": 250,
            "external_liveness_status": "FAIL",
            "external_latest_trade_age_h": 99.0,
        }
        for wallet in (candidate_wallet, parked_wallet)
    ]
    evidence = deadman._select_policy_choke_candidate_pool(
        pool=pool,
        overlay={
            "members": [
                {
                    "source_wallet": "0x" + "c" * 40,
                    "enabled": True,
                    "policy_id": "p",
                    "policy": {"policy_id": "p", "max_order_usd": 1.0},
                }
            ]
        },
        hot_history={"events": []},
        now=now,
        regime="weekend",
        temporal_registry={"wallets": {}},
        direct_source=direct,
        standby_readiness={
            "standby_ready": {
                "volume": {
                    "wallet": parked_wallet,
                    "status": "PARKED",
                    "terminal_decision": "PARK_VOLUME_STANDBY_PAPER_ONLY",
                }
            }
        },
    )
    assert evidence["selected"]["wallet"] == candidate_wallet
    assert evidence["selected"]["chosen_liveness_authority"] == "direct_polygon_wide"
    assert evidence["selected"]["checks"]["f4_external_liveness"] is True
    assert "accepted_live_orders" not in evidence["selected"]["checks"]
    parked = next(row for row in evidence["rows"] if row["wallet"] == parked_wallet)
    assert parked["checks"]["not_terminal_park_red_clock_or_measured_loser"] is False
    assert parked["park_provenance"] == {
        "park_basis": "UNCLASSIFIED",
        "source_record": (
            "data/research/pipeline_slo_and_standby_readiness_latest.json"
            "#standby_ready.volume"
        ),
        "reason_string": "PARK_VOLUME_STANDBY_PAPER_ONLY",
        "measurement": None,
    }


def test_unfed_clock_park_provenance_is_evidence_absence() -> None:
    provenance = deadman._park_provenance(
        {
            "lane": "seat",
            "status": "PARK_SEAT_UNFED_CLOCK_COMMITTED",
            "reason": "permanent_park",
            "park_basis": {},
        }
    )

    assert provenance == {
        "park_basis": "EVIDENCE_ABSENCE",
        "source_record": (
            "data/research/82c8_wide_standby_binding_latest.json"
            "#binding.terminal_outcome_on_deadline"
        ),
        "reason_string": "UNFED_CLOCK_CANNOT_MATURE",
        "measurement": None,
    }


def test_82c8_real_park_bytes_carry_measured_negative_provenance() -> None:
    provenance = deadman._park_provenance(
        {
            "lane": "seat",
            "status": "EVIDENCED_PERMANENT_PARK",
            "reason": "permanent_park",
            "park_basis": {
                "artifact_path": "data/research/82c8_park_reconciliation_latest.json",
            },
        }
    )

    assert provenance == {
        "park_basis": "MEASURED_NEGATIVE",
        "source_record": "data/research/82c8_park_reconciliation_latest.json",
        "reason_string": "OBSERVED_MARGINAL_POST_FEE_USD_PER_FILL=-0.309917",
        "measurement": -0.309917,
    }


def test_source_roster_drought_fire_drill_covers_all_required_cases() -> None:
    report = deadman._source_roster_drought_fire_drill(
        now=dt.datetime(2026, 7, 25, 10, 0, tzinfo=dt.timezone.utc)
    )
    assert report["verdict"] == "PASS"
    assert set(report["cases"]) == {
        "zero_runtime_positive_wide",
        "stale_wide",
        "incomplete_terminal_coverage",
        "terminal_park_exclusion",
        "no_eligible_target",
        "successful_pin_consumption",
        "generation_rollover",
    }
    assert report["cases"]["generation_rollover"]["status"] == "PASS"


def test_source_drought_uses_matching_fingerprint_f1_and_catalog_template() -> None:
    now = dt.datetime(2026, 7, 25, 21, 40, tzinfo=dt.timezone.utc)
    wallet = "0x" + "1" * 40
    fingerprint = "f" * 64
    identity = {
        "policy_id": "wide-base",
        "wallet": wallet,
        "move_slice_keys": ["000-060|0.25-0.50"],
        "max_order_usd": 1.0,
        "min_order_usd": 1.0,
        "wallet_fraction": 0.1,
        "max_fill_lag_s": 5.0,
        "fee_model_id": "fee-v1",
        "selection_rule_id": "selection-v1",
        "wide_policy_fingerprint": fingerprint,
    }
    evidence = deadman._select_source_drought_candidate(
        ready_shadow={
            "lanes": [
                {
                    "wallet": wallet,
                    "paper_policy_id": "unrelated-old-policy",
                    "fading_clear": True,
                }
            ]
        },
        cohort_admission={},
        full_pool_queue={},
        overlay={"members": []},
        hot_history={"events": []},
        direct_source={
            "ready": True,
            "checksum": "direct",
            "per_wallet": {wallet: {"attempts": 12, "copyable": 8}},
            "per_wallet_generation": {
                "generation": {
                    "wallet": wallet,
                    "policy_id": "wide-base",
                    "attempts": 12,
                    "copyable": 8,
                    "policy_depth_pass": 8,
                    "source_generation": "generation",
                    "generation_identity": {"manifest_id": "manifest-1"},
                }
            },
        },
        fingerprint_evidence={
            "manifest_wallet_fingerprints": {
                f"manifest-1|{wallet}": identity
            },
            "cells": [
                {
                    "wide_policy_fingerprint": fingerprint,
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                            "resolved": 200,
                            "post_fee_pnl_usd": 10.0,
                            "roi_pct": 5.0,
                                "f1_pass": True,
                                "f1_venue_reachable_admissible": True,
                                "f1_walk_forward_admissible": True,
                            "venue_reachable_share_pct": 50.0,
                            "first_half_post_fee_pnl_usd": 6.0,
                            "second_half_post_fee_pnl_usd": 4.0,
                            "concentration_admissible": True,
                            "pnl_excluding_top_1_market": 8.0,
                            "top_1_market_share_pct": 20.0,
                            "win_rate_pct": 60.0,
                        },
                }
            ],
        },
        exact_policy_holdouts={wallet: {fingerprint: {
            "passed": True,
            "wide_policy_fingerprint": fingerprint,
        }}},
        standby_readiness={
            "standby_ready": {
                "volume": {
                    "wallet": wallet,
                    "status": "PARKED",
                    "terminal_decision": "PARK_VOLUME_STANDBY_PAPER_ONLY",
                }
            }
        },
        now=now,
        regime="weekend",
        cooloffs={},
        temporal_registry={
            "criteria": {"min_trades": 5},
            "wallets": [
                {
                    "wallet": wallet,
                    "slice_labels": {
                        "weekend": {
                            "label": "PROVEN-POSITIVE",
                            "resolved_trades": 50,
                        },
                        "dead_band_18_22_utc": {
                            "label": "PROVEN-POSITIVE",
                            "resolved_trades": 50,
                        },
                    },
                }
            ],
        },
    )
    selected = evidence["selected"]
    assert selected["wallet"] == wallet
    assert selected["wide_policy_fingerprint"] == fingerprint
    assert selected["checks"]["f1_measured_positive_regime_cell"] is True
    assert selected["regime_evidence"]["regime_sliced"] is False
    assert selected["regime_evidence"]["regime_basis"] == (
        "full_stream_not_regime_sliced"
    )
    assert evidence["gate_digits"]["f1_regime_basis"] == (
        "full_stream_not_regime_sliced"
    )
    assert selected["checks"]["own_evidenced_policy_available"] is True
    assert selected["policy"]["move_slice_keys"] == ["000-060|0.25-0.50"]
    assert selected["paper_policy_id"].startswith("wide_fp_")
    assert selected["standby_exclusion_superseded"]["scope"] == "volume_lane_only"


def test_direct_manifest_identity_fallback_joins_exact_named_run(tmp_path) -> None:
    wallet = "0x" + "9" * 40
    run_id = "wide_20260802T000823Z"
    manifest_id = "widemanifest_exact"
    manifest_path = (
        tmp_path
        / "data"
        / "research"
        / f"wide_exact_policy_manifest_{run_id}.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_id": manifest_id,
                "capture_watch_wallets": [
                    {
                        "wallet": wallet,
                        "move_slice_keys": ["060-120|0.25-0.50"],
                        "policy_absent": False,
                    }
                ],
            }
        )
    )

    fallback = deadman._direct_manifest_identity_fallback(
        root=tmp_path,
        direct_source={
            "per_wallet_generation": {
                "generation": {
                    "wallet": wallet,
                    "generation_identity": {
                        "run_id": run_id,
                        "manifest_id": manifest_id,
                    },
                }
            }
        },
    )

    identity = fallback[f"{manifest_id}|{wallet}"]
    assert identity["wallet"] == wallet
    assert identity["policy_id"] == "wide_positive_alpha_exact_wf0p1_max1_min1"
    assert identity["manifest_identity_source"] == (
        "exact_direct_generation_manifest_fallback"
    )


def test_direct_manifest_identity_fallback_refuses_manifest_id_mismatch(tmp_path) -> None:
    wallet = "0x" + "8" * 40
    run_id = "wide_20260802T000823Z"
    manifest_path = (
        tmp_path
        / "data"
        / "research"
        / f"wide_exact_policy_manifest_{run_id}.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_id": "different",
                "capture_watch_wallets": [
                    {
                        "wallet": wallet,
                        "move_slice_keys": ["060-120|0.25-0.50"],
                        "policy_absent": False,
                    }
                ],
            }
        )
    )

    assert deadman._direct_manifest_identity_fallback(
        root=tmp_path,
        direct_source={
            "per_wallet_generation": {
                "generation": {
                    "wallet": wallet,
                    "generation_identity": {
                        "run_id": run_id,
                        "manifest_id": "expected",
                    },
                }
            }
        },
    ) == {}


def test_roster_collapse_limb_fires_after_independent_hold_without_restart() -> None:
    now = dt.datetime(2026, 8, 2, 1, 0, tzinfo=dt.timezone.utc)
    result = deadman._roster_collapse_limb(
        guard={"status": "LIVE_GUARD_BLOCKED", "active_set_runtime": {"member_count": 0}},
        previous={"roster_collapse_limb": {"started_at": "2026-08-02T00:29:59+00:00"}},
        now=now,
    )

    assert result["firing"] is True
    assert result["incident_class"] == "INCIDENT_ROSTER_COLLAPSE"
    assert result["restart_authorized"] is False
    assert result["mechanical_escalation"] == "NONE"


def test_roster_collapse_limb_resets_when_member_returns() -> None:
    result = deadman._roster_collapse_limb(
        guard={"status": "LIVE_GUARD_RUNNING", "active_set_runtime": {"member_count": 1}},
        previous={"roster_collapse_limb": {"started_at": "2026-08-02T00:00:00+00:00"}},
        now=dt.datetime(2026, 8, 2, 1, 0, tzinfo=dt.timezone.utc),
    )

    assert result["firing"] is False
    assert result["started_at"] is None


def test_roster_inadmissible_limb_fires_for_live_artifact_shape() -> None:
    result = deadman._roster_collapse_limb(
        guard={
            "status": "LIVE_GUARD_BLOCKED",
            "active_set_runtime": {"member_count": 1},
            "candidate_pass_gate_diagnostics": {
                "selected_passed": False,
                "fallthrough": {"reason": "no_active_set_member_passed"},
            },
        },
        previous={"roster_collapse_limb": {"started_at": "2026-08-02T00:29:59+00:00"}},
        now=dt.datetime(2026, 8, 2, 1, 0, tzinfo=dt.timezone.utc),
    )

    assert result["firing"] is True
    assert result["incident_class"] == "INCIDENT_ROSTER_INADMISSIBLE"
    assert result["restart_authorized"] is False


def test_zero_supply_seat_limb_authorizes_read_only() -> None:
    result = deadman._zero_supply_seat_limb(
        guard={"status": "LIVE_GUARD_BLOCKED"},
        policy_choke={"selected_fresh_source_rows": 0},
        idle_s=1801.0,
        threshold_s=1800.0,
    )

    assert result["firing"] is True
    assert result["incident_class"] == "INCIDENT_ZERO_SUPPLY_SEAT"
    assert result["rung_a_seat_read_authorized"] is True
    assert result["member_enable_authorized"] is False
    assert result["restart_authorized"] is False


def test_post_selection_floor_does_not_downgrade_zero_supply_seat() -> None:
    assert deadman._post_selection_floor_can_downgrade(
        post_selection_floor={"active": True},
        zero_supply_seat={"firing": True},
        roster_collapse={"firing": False},
    ) is False


def test_adopted_seat_supply_clock_refires_drought_after_max_idle() -> None:
    result = deadman._zero_supply_seat_limb(
        guard={"status": "LIVE_GUARD_RUNNING"},
        policy_choke={
            "selected_fresh_source_rows": 0,
            "whole_runtime_fresh_source_rows": 0,
        },
        idle_s=3600.0,
        threshold_s=1800.0,
        post_selection_floor={
            "matching_adopted_runtime": True,
            "selected_seat_epoch_at": "2026-08-04T08:00:00+00:00",
            "post_selection_idle_s": 1801.0,
        },
    )

    assert result["firing"] is True
    assert result["adopted_supply_clock_expired"] is True
    assert result["selected_seat_epoch_at"] == "2026-08-04T08:00:00+00:00"


def test_policy_choke_never_clear_on_zero_runtime_fresh_rows_past_idle_budget() -> None:
    policy_choke = {
        "status": "CLEAR",
        "firing": False,
        "whole_runtime_fresh_source_rows": 0,
    }

    past_budget = deadman._enforce_policy_choke_zero_supply_status(
        policy_choke,
        idle_s=1801.0,
        threshold_s=1800.0,
    )

    assert past_budget is True
    assert policy_choke["status"] == "ZERO_RUNTIME_SUPPLY_PAST_IDLE_BUDGET"
    assert policy_choke["firing"] is False
    assert policy_choke["clear_refusal"]["threshold_s"] == 1800.0


def test_source_drought_includes_strict_non_current_manifest_fingerprint() -> None:
    wallet = "0x" + "7" * 40
    current = "a" * 64
    strict = "b" * 64
    base_identity = {
        "policy_id": "wide-base",
        "wallet": wallet,
        "move_slice_keys": ["000-060|0.25-0.50"],
        "max_order_usd": 1.0,
        "min_order_usd": 1.0,
        "wallet_fraction": 0.1,
        "max_fill_lag_s": 5.0,
    }
    packet = _capture_watch_frontier_fixture(wallet=wallet, fingerprint=current)
    # Re-run with the fixture's live generation but a second, historically joined
    # fingerprint that independently clears the unchanged full/H1/H2 bars.
    packet = deadman._select_source_drought_candidate(
        ready_shadow={},
        cohort_admission={},
        full_pool_queue={},
        overlay={"members": []},
        hot_history={"events": []},
        direct_source={
            "ready": True,
            "checksum": "direct",
            "per_wallet_generation": {"g": {
                "wallet": wallet, "policy_id": "wide-base", "attempts": 12,
                "copyable": 2, "policy_depth_pass": 2, "source_generation": "g",
                "generation_identity": {"manifest_id": "current-manifest"},
            }},
        },
        fingerprint_evidence={
            "manifest_wallet_fingerprints": {
                f"current-manifest|{wallet}": {
                    **base_identity, "wide_policy_fingerprint": current,
                }
            },
            "cells": [
                {
                    "wide_policy_fingerprint": current,
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "resolved": 200, "post_fee_pnl_usd": -1.0, "roi_pct": -0.5,
                        "f1_venue_reachable_admissible": True,
                        "venue_reachable_share_pct": 50.0,
                        "concentration_admissible": False,
                    },
                },
                {
                    "wide_policy_fingerprint": strict,
                    "identity": {**base_identity, "wide_policy_fingerprint": strict},
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "resolved": 400, "post_fee_pnl_usd": 30.0, "roi_pct": 7.5,
                        "f1_pass": True, "f1_venue_reachable_admissible": True,
                        "venue_reachable_share_pct": 50.0,
                        "concentration_admissible": True,
                        "first_half_post_fee_pnl_usd": 10.0,
                        "second_half_post_fee_pnl_usd": 20.0,
                        "first_half": {
                            "f1_pass": True, "concentration_admissible": True,
                            "post_fee_pnl_usd": 10.0,
                        },
                        "second_half": {
                            "f1_pass": True, "concentration_admissible": True,
                            "post_fee_pnl_usd": 20.0,
                        },
                    },
                },
            ],
        },
        standby_readiness={},
        now=dt.datetime(2026, 7, 30, 6, 0, tzinfo=dt.timezone.utc),
        regime="weekday",
        cooloffs={},
        temporal_registry={},
    )
    row = next(row for row in packet["rows"] if row["wide_policy_fingerprint"] == strict)
    assert row["source_identity"]["manifest_id"] == "current-manifest"
    assert row["checks"]["f1_concentration_admissible"] is True
    assert row["checks"]["both_resolved_halves_positive"] is True


def _capture_watch_frontier_fixture(
    *,
    wallet: str = "0x00033f1089ff061813850e5135483bed39ce3b49",
    fingerprint: str = "1f11" * 16,
    attempts: int = 12,
    copyable: int = 2,
    include_manifest_join: bool = True,
    first_half_pnl: float = 6.0,
    second_half_pnl: float = 4.0,
    concentration_admissible: bool = True,
    overlay: dict | None = None,
    cooloffs: dict | None = None,
    standby_readiness: dict | None = None,
    direct_ready: bool = True,
    temporal_registry: dict | None = None,
    total_loss_auto_disable: dict | None = None,
    exact_policy_holdouts: dict | None = None,
    omit_exact_policy_holdouts: bool = False,
) -> dict:
    now = dt.datetime(2026, 7, 27, 4, 16, tzinfo=dt.timezone.utc)
    policy_id = "wide-base"
    identity = {
        "policy_id": policy_id,
        "wallet": wallet,
        "move_slice_keys": ["060-120|<=0.25"],
        "max_order_usd": 1.0,
        "min_order_usd": 1.0,
        "wallet_fraction": 0.1,
        "max_fill_lag_s": 5.0,
        "wide_policy_fingerprint": fingerprint,
    }
    measured_temporal = {
        "criteria": {"min_trades": 5},
        "wallets": [
            {
                "wallet": wallet,
                "slice_labels": {
                    "weekend": {
                        "label": "PROVEN-POSITIVE",
                        "resolved_trades": 250,
                        "pnl_usd": 25.0,
                        "roi_pct": 5.0,
                    }
                },
            }
        ],
    }
    return deadman._select_source_drought_candidate(
        ready_shadow={},
        cohort_admission={},
        full_pool_queue={},
        overlay=overlay or {"members": []},
        hot_history={"events": []},
        direct_source={
            "ready": direct_ready,
            "checksum": "direct",
            "per_wallet": {
                wallet: {"attempts": attempts, "copyable": copyable}
            },
            "per_wallet_generation": {
                "generation": {
                    "wallet": wallet,
                    "policy_id": policy_id,
                    "attempts": attempts,
                    "copyable": copyable,
                    "policy_depth_pass": copyable,
                    "source_generation": "generation",
                    "generation_identity": {"manifest_id": "manifest-1"},
                }
            },
        },
        fingerprint_evidence={
            "manifest_wallet_fingerprints": (
                {f"manifest-1|{wallet}": identity}
                if include_manifest_join
                else {}
            ),
            "cells": [
                {
                    "wide_policy_fingerprint": fingerprint,
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "resolved": 227,
                        "post_fee_pnl_usd": 10.0,
                        "roi_pct": 5.0,
                        "f1_pass": True,
                        "f1_venue_reachable_admissible": True,
                        "f1_walk_forward_admissible": True,
                        "venue_reachable_share_pct": 50.0,
                        "first_half_post_fee_pnl_usd": first_half_pnl,
                        "second_half_post_fee_pnl_usd": second_half_pnl,
                        "concentration_admissible": concentration_admissible,
                        "pnl_excluding_top_1_market": 8.0,
                        "top_1_market_share_pct": 20.0,
                        "win_rate_pct": 60.0,
                    },
                }
            ],
        },
        standby_readiness=standby_readiness or {},
        now=now,
        regime="weekend",
        cooloffs=cooloffs or {},
        temporal_registry=(
            temporal_registry
            if temporal_registry is not None
            else measured_temporal
        ),
        total_loss_auto_disable=total_loss_auto_disable or {},
        exact_policy_holdouts=(
            None
            if omit_exact_policy_holdouts
            else exact_policy_holdouts
            if exact_policy_holdouts is not None
            else {wallet: {fingerprint: {
                "passed": True,
                "wide_policy_fingerprint": fingerprint,
            }}}
        ),
    )


def test_direct_candidate_selector_requires_matching_holdout_when_cohort_supplied() -> None:
    wallet = "0x00033f1089ff061813850e5135483bed39ce3b49"
    fingerprint = "1f11" * 16
    missing = _capture_watch_frontier_fixture(exact_policy_holdouts={})
    matched = _capture_watch_frontier_fixture(exact_policy_holdouts={
        wallet: {fingerprint: {
            "passed": True,
            "wide_policy_fingerprint": fingerprint,
        }}
    })

    assert missing["selected"] is None
    assert missing["rows"][0]["checks"]["exact_policy_chronological_holdout_pass"] is False
    assert matched["selected"]["wide_policy_fingerprint"] == fingerprint


def test_direct_candidate_selector_fails_closed_when_holdout_cohort_absent() -> None:
    packet = _capture_watch_frontier_fixture(omit_exact_policy_holdouts=True)

    assert packet["selected"] is None
    assert packet["rows"][0]["checks"]["exact_policy_chronological_holdout_pass"] is False
    assert packet["rows"][0]["admission_refusal"] == "EXACT_POLICY_HOLDOUT_COHORT_ABSENT"


def test_required_source_drought_check_missing_is_a_named_failure() -> None:
    checks = {name: True for name in deadman.REQUIRED_SOURCE_DROUGHT_CHECKS}
    del checks["f4_external_liveness"]

    assert deadman._source_drought_checks_pass(checks) is False
    assert deadman._source_drought_check_deficits(checks) == ["f4_external_liveness"]


def test_unmodeled_source_drought_check_fails_closed() -> None:
    checks = {name: True for name in deadman.REQUIRED_SOURCE_DROUGHT_CHECKS}
    checks["future_unmodeled_gate"] = False

    assert deadman._source_drought_checks_pass(checks) is False
    assert deadman._source_drought_unmodeled_checks(checks) == ["future_unmodeled_gate"]


def test_total_loss_disabled_wallet_cannot_pass_direct_f3() -> None:
    wallet = "0x4462e46cf0d31466693058893f4913e2411fbfdf"
    packet = _capture_watch_frontier_fixture(
        wallet=wallet,
        overlay={
            "members": [],
            "previous_selection_pins": [
                {
                    "enabled": False,
                    "source_wallet": wallet,
                    "release_reason": "TOTAL_LOSS_DISABLE",
                }
            ],
        },
        total_loss_auto_disable={
            "enabled": True,
            "disabled_members": [{"source_wallet": wallet}],
        },
    )

    row = next(row for row in packet["rows"] if row["wallet"] == wallet)
    assert row["total_loss_direct_fenced"] is True
    assert row["checks"]["f3_not_enabled_or_cooloff_or_fading"] is False
    assert row["eligible"] is False
    assert row["admission_refusal"] == "f3_not_enabled_or_cooloff_or_fading"


def test_capture_watch_generation_synthesizes_exact_frontier_base() -> None:
    evidence = _capture_watch_frontier_fixture()

    assert evidence["candidate_count"] == 1
    assert evidence["eligible_count"] == 1
    assert evidence["unmodeled_check_candidates"] == 0
    assert evidence["unmodeled_check_names"] == []
    row = evidence["selected"]
    assert row["supply_source"] == "direct_capture_watch_generation"
    assert row["wide_policy_fingerprint"] == "1f11" * 16
    assert row["evidence_deficits"] == []
    assert row["fresh_own_source_buy_rows_30m"] == 12
    assert row["claimed_fresh_own_source_buy_rows_30m"] == 0
    assert row["f1_slice_basis"] == {
        "slice": "full_stream",
        "source": "venue_executable_full_stream_rescore",
        "regime_sliced": False,
        "resolved_signals": row["regime_evidence"]["resolved_signals"],
    }
    assert row["admissible_slices"][0]["slice"] == "weekend"
    assert row["admissible_slices"][0]["resolved_trades"] == 250
    assert "f1_slice_basis" not in row["checks"]
    assert "admissible_slices" not in row["checks"]


def test_capture_watch_generation_row_keeps_full_f2_attempt_bar() -> None:
    evidence = _capture_watch_frontier_fixture(attempts=6, copyable=2)

    assert evidence["eligible_count"] == 0
    row = evidence["rows"][0]
    assert row["direct_source"]["copyable"] == 2
    assert row["evidence_deficits"] == [
        "f2_fresh_rows_and_own_policy_copyable"
    ]


def test_candidate_supply_dropouts_name_missing_direct_generation() -> None:
    vanished = "0x31c290a2772e1e3143bcb6debbdbbf08ac081d13"
    current = "0x" + "a" * 40

    dropouts = deadman._candidate_supply_dropouts(
        previous={
            "rows": [
                {
                    "wallet": vanished,
                    "supply_source": "full_pool_member_queue",
                },
                {"wallet": current, "supply_source": "ready_shadow"},
            ]
        },
        current={"rows": [{"wallet": current}]},
        ready_shadow={"lanes": [{"wallet": vanished}]},
        cohort_admission={},
        full_pool_queue={},
        direct_source={"per_wallet_generation": {}},
    )

    assert dropouts == [
        {
            "wallet": vanished,
            "previous_supply_source": "full_pool_member_queue",
            "absent_reason": (
                "absent_from_current_reconciled_direct_generation_supply"
            ),
        }
    ]


def test_candidate_supply_dropouts_records_pinned_wallet_absent_from_frontier() -> None:
    wallet = "0x" + "b" * 40
    fingerprint = "4f" * 32

    dropouts = deadman._candidate_supply_dropouts(
        previous={},
        current={"rows": []},
        ready_shadow={},
        cohort_admission={},
        full_pool_queue={},
        direct_source={
            "per_wallet_generation": {
                "generation": {"wallet": wallet, "attempts": 65, "copyable": 3}
            }
        },
        overlay={
            "selection_pin": {
                "enabled": True,
                "pin_id": "policy-choke-rung-b",
                "candidate_id": "seat",
                "source_wallet": wallet,
            },
            "members": [
                {
                    "candidate_id": "seat",
                    "source_wallet": wallet,
                    "policy": {"wide_policy_fingerprint": fingerprint},
                }
            ],
        },
    )

    assert dropouts == [
        {
            "wallet": wallet,
            "wide_policy_fingerprint": fingerprint,
            "candidate_id": "seat",
            "pin_id": "policy-choke-rung-b",
            "absent_reason": "pinned_wallet_absent_from_frontier",
            "pinned_wallet_on_frontier": False,
            "supply_fingerprints_for_pinned_wallet": [],
            "direct_attempts": 65,
            "direct_copyable": 3,
            "next_action": "escalate_pin_absent_and_retain_live_seat_until_rotation_authority",
        }
    ]


def test_order140_rtds_liveness_incremental_bounded_tail(tmp_path) -> None:
    capture = tmp_path / "capture.jsonl"
    state = tmp_path / "state.json"
    wallet = "0x00033f1089ff061813850e5135483bed39ce3b49"
    rows = [
        {
            "event": "rtds_trade_event",
            "event_id": f"e{index}",
            "event_ts": 1785640000 + index,
            "received_at_s": 1785640000.5 + index,
            "source_wallet": wallet,
            "market_slug": "btc-updown-5m-1785639900",
        }
        for index in range(3)
    ]
    capture.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    now = dt.datetime.fromtimestamp(1785640010, tz=dt.timezone.utc)
    first = deadman._update_rtds_observed_liveness(
        capture, state, now=now, max_bytes=4096
    )
    assert first["bytes_read"] <= 4096
    assert first["per_wallet"][wallet]["observed_age_s"] == 8.0
    with capture.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({**rows[-1], "event_id": "e3", "event_ts": 1785640009}) + "\n")
    second = deadman._update_rtds_observed_liveness(
        capture, state, now=now, max_bytes=4096
    )
    assert second["offset_reset_to_bounded_tail"] is False
    assert second["per_wallet"][wallet]["observed_age_s"] == 1.0


def test_order140_rtds_observed_is_per_wallet_f4_disjunct() -> None:
    wallet = "0x00033f1089ff061813850e5135483bed39ce3b49"
    now = dt.datetime(2026, 8, 2, 3, 0, tzinfo=dt.timezone.utc)
    evidence = deadman._select_policy_choke_candidate_pool(
        pool=[
            {
                "wallet": wallet,
                "paper_policy_id": "p",
                "retrospective_gross_pnl_usd": 25.0,
                "retrospective_gross_roi_pct": 5.0,
                "retrospective_resolved_signals": 250,
                "external_liveness_status": "FAIL",
                "external_latest_trade_age_h": 99.0,
            }
        ],
        overlay={"members": [{"enabled": True, "source_wallet": "0x" + "a" * 40, "policy_id": "p", "policy": {"policy_id": "p"}}]},
        hot_history={"events": []},
        now=now,
        regime="weekend",
        temporal_registry={},
        direct_source={"ready": False, "status": "FAIL", "fresh_rows": None, "copyable": None, "latest_receipt_at": None},
        rtds_liveness={"status": "PASS", "per_wallet": {wallet: {"observed_age_s": 0.777}}},
    )
    row = evidence["rows"][0]
    assert row["checks"]["f4_external_liveness"] is True
    assert row["f4_basis"] == "rtds_observed"
    assert evidence["f4_root_defect"]["status"] == "GLOBAL_DIRECT_READY_NULL_DISABLES_PER_WALLET_F4"


def test_candidate_supply_dropouts_splits_pinned_fingerprint_absent() -> None:
    wallet = "0x" + "c" * 40
    pinned = "aa" * 32
    supplied = "bb" * 32

    dropouts = deadman._candidate_supply_dropouts(
        previous={},
        current={"rows": [{"wallet": wallet, "wide_policy_fingerprint": supplied}]},
        ready_shadow={},
        cohort_admission={},
        full_pool_queue={},
        direct_source={"per_wallet_generation": {}},
        overlay={
            "selection_pin": {"enabled": True, "candidate_id": "seat", "source_wallet": wallet},
            "members": [
                {
                    "candidate_id": "seat",
                    "source_wallet": wallet,
                    "policy": {"wide_policy_fingerprint": pinned},
                }
            ],
        },
    )

    assert dropouts[0]["absent_reason"] == "pinned_fingerprint_absent_from_supply"
    assert dropouts[0]["pinned_wallet_on_frontier"] is True
    assert dropouts[0]["supply_fingerprints_for_pinned_wallet"] == [supplied]


def test_candidate_supply_dropouts_prefers_explicit_pin_fingerprint_over_stale_member() -> None:
    wallet = "0x" + "d" * 40
    active_fingerprint = "40" * 32
    stale_fingerprint = "e0" * 32

    dropouts = deadman._candidate_supply_dropouts(
        previous={},
        current={"rows": []},
        ready_shadow={},
        cohort_admission={},
        full_pool_queue={},
        direct_source={"per_wallet_generation": {}},
        overlay={
            "selection_pin": {
                "enabled": True,
                "candidate_id": "duplicate-seat",
                "source_wallet": wallet,
                "wide_policy_fingerprint": active_fingerprint,
            },
            "members": [
                {
                    "candidate_id": "duplicate-seat",
                    "source_wallet": wallet,
                    "policy": {"wide_policy_fingerprint": stale_fingerprint},
                }
            ],
        },
    )

    assert dropouts[0]["wide_policy_fingerprint"] == active_fingerprint
    assert dropouts[0]["absent_reason"] == "pinned_wallet_absent_from_frontier"


def test_capture_watch_generation_rejects_concentrated_f1_cell() -> None:
    evidence = _capture_watch_frontier_fixture(concentration_admissible=False)
    row = evidence["rows"][0]

    assert evidence["eligible_count"] == 0
    assert row["eligible"] is False
    assert "f1_concentration_admissible" in row["evidence_deficits"]


def test_capture_watch_generation_reports_deficits_during_latest_packet_seam() -> None:
    evidence = _capture_watch_frontier_fixture(
        attempts=2,
        copyable=0,
        direct_ready=False,
    )

    assert evidence["eligible_count"] == 0
    row = evidence["rows"][0]
    assert row["supply_source"] == "direct_capture_watch_generation"
    assert row["direct_source"]["attempts"] == 2
    assert row["direct_source"]["copyable"] == 0
    assert "f2_fresh_rows_and_own_policy_copyable" in row["evidence_deficits"]


def test_capture_watch_generation_fails_f3_for_temporal_fading() -> None:
    wallet = "0x00033f1089ff061813850e5135483bed39ce3b49"
    evidence = _capture_watch_frontier_fixture(
        wallet=wallet,
        temporal_registry={
            "wallets": [
                {
                    "wallet": wallet,
                    "classification": "FADING",
                    "slice_labels": {
                        "weekend": {
                            "label": "PROVEN-POSITIVE",
                            "resolved_trades": 20,
                            "roi_pct": 1.0,
                            "pnl_usd": 2.0,
                        }
                    },
                }
            ]
        },
    )

    assert evidence["eligible_count"] == 0
    row = evidence["rows"][0]
    assert row["fading_clear"] is False
    assert row["checks"]["f3_not_enabled_or_cooloff_or_fading"] is False
    assert row["evidence_deficits"] == [
        "f3_not_enabled_or_cooloff_or_fading"
    ]


def test_capture_watch_demotion_cooloff_bars_wallet_across_fingerprints() -> None:
    wallet = "0x00033f1089ff061813850e5135483bed39ce3b49"
    old_fingerprint = "2163" * 16
    new_fingerprint = "1f11" * 16
    expires = "2026-07-27T19:05:18+00:00"
    demoted_overlay = {
        "members": [
            {
                "candidate_id": "policy_choke_rung_direct_old",
                "source_wallet": wallet,
                "enabled": False,
                "status": "DEMOTED_FABLE_SUBSTITUTE_ROTATION",
                "policy": {"wide_policy_fingerprint": old_fingerprint},
            }
        ]
    }

    mismatch = _capture_watch_frontier_fixture(
        fingerprint=new_fingerprint,
        overlay=demoted_overlay,
        cooloffs={
            f"{wallet}|{old_fingerprint}": {
                "expires_at": expires,
                "wide_policy_fingerprint": old_fingerprint,
                "reason": "mechanical_loss_demotion",
            }
        },
    )
    mismatch_row = mismatch["rows"][0]
    assert mismatch_row["checks"]["f3_not_enabled_or_cooloff_or_fading"] is False
    assert mismatch_row["cooloff_scope_mismatch_ignored"]
    assert mismatch_row["cooloff_scope"]["wallet_wide_frontier_bar"] is True
    assert mismatch_row["cooloff_until"] == expires
    assert "f3_not_enabled_or_cooloff_or_fading" in mismatch_row[
        "evidence_deficits"
    ]

    matching = _capture_watch_frontier_fixture(
        fingerprint=new_fingerprint,
        overlay={
            "members": [
                {
                    **demoted_overlay["members"][0],
                    "policy": {"wide_policy_fingerprint": new_fingerprint},
                }
            ]
        },
        cooloffs={wallet: expires},
    )
    matching_row = matching["rows"][0]
    assert matching_row["checks"]["f3_not_enabled_or_cooloff_or_fading"] is False
    assert matching_row["cooloff_until"] == expires
    assert matching_row["cooloff_scope"]["wallet_wide_frontier_bar"] is True
    assert "f3_not_enabled_or_cooloff_or_fading" in matching_row[
        "evidence_deficits"
    ]


def test_capture_watch_generation_without_exact_manifest_join_is_omitted() -> None:
    evidence = _capture_watch_frontier_fixture(include_manifest_join=False)

    assert evidence["candidate_count"] == 0
    assert evidence["rows"] == []
    assert evidence["selected"] is None


def test_capture_watch_generation_never_eligible_with_negative_half() -> None:
    evidence = _capture_watch_frontier_fixture(second_half_pnl=-1.0)

    assert evidence["eligible_count"] == 0
    assert evidence["selected"] is None
    assert "both_resolved_halves_positive" in evidence["rows"][0][
        "evidence_deficits"
    ]


def test_capture_watch_generation_terminal_park_stays_refused() -> None:
    wallet = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    evidence = _capture_watch_frontier_fixture(
        wallet=wallet,
        standby_readiness={
            "standby_ready": {
                "volume": {
                    "wallet": wallet,
                    "status": "PARKED",
                    "terminal_decision": "PARK_VOLUME_STANDBY_PAPER_ONLY",
                }
            }
        },
    )

    assert evidence["eligible_count"] == 0
    assert evidence["selected"] is None
    assert "not_terminal_park_red_clock_or_measured_loser" in evidence["rows"][
        0
    ]["evidence_deficits"]


def test_fingerprint_direct_does_not_supersede_permanent_wallet_park() -> None:
    wallet = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    exclusion = {
        "lane": "volume",
        "status": "PARKED",
        "terminal_decision": "PARK_VOLUME_STANDBY_PAPER_ONLY",
        "red_clock": False,
    }
    assert deadman._fingerprint_direct_supersedes_volume_park(
        wallet=wallet,
        lane={"wide_policy_fingerprint": "f" * 64},
        evidence={"resolved_signals": 200, "pnl_usd": 1.0, "roi_pct": 1.0},
        exclusion=exclusion,
    ) is False


def test_direct_pin_admission_requires_submitted_order_or_fresh_01a() -> None:
    now = dt.datetime.fromisoformat("2026-08-03T08:20:20+00:00")
    wallet = "0x" + "a" * 40
    candidate = {"wallet": wallet}
    market_start = int(now.timestamp()) // 300 * 300

    refused = deadman._direct_pin_admission_authority(
        candidate=candidate,
        ledger={"orders": []},
        hot_history={
            "events": [
                {
                    "source_wallet": wallet,
                    "action": "BUY",
                    "price": 0.32,
                    "source": "rtds_activity",
                    "event_ts": now.timestamp() - 2,
                    "observed_ts": now.timestamp() - 1,
                    "market_slug": f"btc-updown-5m-{market_start}",
                }
            ]
        },
        now=now,
    )
    assert refused["authorized"] is False

    authorized = deadman._direct_pin_admission_authority(
        candidate=candidate,
        ledger={"orders": []},
        hot_history={
            "events": [
                {
                    "event_id": "01a-buy",
                    "source_wallet": wallet,
                    "action": "BUY",
                    "price": 0.31,
                    "source": "polygon_orderfilled_ws_premerge",
                    "event_ts": now.timestamp() - 10,
                    "observed_ts": now.timestamp() - 4,
                    "market_slug": f"btc-updown-5m-{market_start}",
                }
            ]
        },
        now=now,
    )
    assert authorized["authorized"] is True
    assert authorized["fresh_policy_compatible_01a_buy"]["event_id"] == "01a-buy"


def test_direct_admission_reads_green_candidate_when_pool_gate_is_red() -> None:
    now = dt.datetime.fromisoformat("2026-08-03T08:20:20+00:00")
    wallet = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
    fingerprint = "402856" * 10 + "4028"
    market_start = int(now.timestamp()) // 300 * 300
    candidate = {
        "wallet": wallet,
        "wide_policy_fingerprint": fingerprint,
        "policy": {"wide_policy_fingerprint": fingerprint},
    }
    stakeout = {
        "prospective_current_market": {
            "actuator_consumption_gate": {
                "passed": False,
                "identity_market_outcome_parity_violations": 0,
                "unique_fills": 2506,
                "wallet_current_market_buy_counts": {wallet: 283},
                "exact_policy_chronological_holdout_by_wallet": {
                    wallet: {
                        fingerprint: {
                            "passed": True,
                            "wide_policy_fingerprint": fingerprint,
                            "first_half_post_fee_pnl_usd": 40.4,
                            "second_half_post_fee_pnl_usd": 55.5,
                        },
                    },
                    "0x" + "c" * 40: {
                        "other-fingerprint": {
                            "passed": False,
                            "wide_policy_fingerprint": "other-fingerprint",
                        },
                    },
                },
            },
            "identity_clean_events": [
                {
                    "event_id": "polygon-push-01a",
                    "source_wallet": wallet,
                    "action": "BUY",
                    "price": 0.31,
                    "source": "polygon_ws",
                    "paper_only": True,
                    "observed_ts": now.timestamp() - 2,
                    "market_slug": f"btc-updown-5m-{market_start}",
                },
                {
                    "event_id": "unrelated-wallet",
                    "source_wallet": "0x" + "c" * 40,
                    "action": "BUY",
                    "price": 0.31,
                    "source": "polygon_ws",
                    "paper_only": True,
                    "observed_ts": now.timestamp() - 2,
                    "market_slug": f"btc-updown-5m-{market_start}",
                },
            ],
        }
    }

    authority = deadman._direct_pin_admission_authority(
        candidate=candidate,
        ledger={"orders": []},
        hot_history={"events": []},
        qualified_pool_stakeout=stakeout,
        now=now,
    )

    assert authority["authorized"] is True
    assert authority["fresh_policy_compatible_01a_buy"]["source"] == "polygon_ws"
    assert authority["admission_read_gate"]["passed"] is True
    assert authority["admission_read_gate"][
        "execution_consumption_gate_unchanged"
    ] is True
    assert stakeout["prospective_current_market"]["actuator_consumption_gate"][
        "passed"
    ] is False


def test_direct_admission_uses_candidate_fingerprint_holdout_for_multi_policy_wallet() -> None:
    now = dt.datetime.fromisoformat("2026-08-03T08:20:20+00:00")
    wallet = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
    selected_fingerprint = "selected-fingerprint"
    candidate = {
        "wallet": wallet,
        "wide_policy_fingerprint": selected_fingerprint,
    }
    gate = {
        "passed": False,
        "identity_market_outcome_parity_violations": 0,
        "unique_fills": 100,
        "wallet_current_market_buy_counts": {wallet: 10},
        "exact_policy_chronological_holdout_by_wallet": {
            wallet: {
                "other-fingerprint": {
                    "passed": False,
                    "wide_policy_fingerprint": "other-fingerprint",
                },
                selected_fingerprint: {
                    "passed": True,
                    "wide_policy_fingerprint": selected_fingerprint,
                },
            }
        },
    }

    authority = deadman._direct_pin_admission_authority(
        candidate=candidate,
        ledger={"orders": []},
        hot_history={"events": []},
        qualified_pool_stakeout={
            "prospective_current_market": {
                "actuator_consumption_gate": gate,
                "identity_clean_events": [],
            }
        },
        now=now,
    )

    assert authority["admission_read_gate"]["candidate_holdout"] == {
        "passed": True,
        "wide_policy_fingerprint": selected_fingerprint,
    }
    assert authority["admission_read_gate"][
        "candidate_exact_policy_evidence_status"
    ] == "PASS"
    assert authority["admission_read_gate"]["checks"][
        "candidate_exact_policy_holdout_pass"
    ] is True


def test_direct_admission_distinguishes_missing_from_failed_exact_policy_cell() -> None:
    now = dt.datetime.fromisoformat("2026-08-03T08:20:20+00:00")
    wallet = "0x" + "b" * 40
    candidate = {"wallet": wallet, "wide_policy_fingerprint": "wanted"}

    def status(holdouts: dict) -> str:
        authority = deadman._direct_pin_admission_authority(
            candidate=candidate,
            ledger={"orders": []},
            hot_history={"events": []},
            qualified_pool_stakeout={"prospective_current_market": {
                "actuator_consumption_gate": {
                    "unique_fills": 100,
                    "wallet_current_market_buy_counts": {wallet: 10},
                    "exact_policy_chronological_holdout_by_wallet": holdouts,
                },
                "identity_clean_events": [],
            }},
            now=now,
        )
        return authority["admission_read_gate"]["candidate_exact_policy_evidence_status"]

    assert status({}) == "NO_EXACT_POLICY_EVIDENCE_CELL"
    assert status({wallet: {"wanted": {"passed": False, "wide_policy_fingerprint": "wanted"}}}) == "EVIDENCE_CELL_FAILED"


def test_direct_admission_interval_admits_between_checks_but_not_prior_window() -> None:
    now = dt.datetime.fromisoformat("2026-08-04T06:04:00+00:00")
    wallet = "0x" + "b" * 40
    market_start = int(now.timestamp()) // 300 * 300
    previous = {
        "wallet": wallet,
        "checked_at": (now - dt.timedelta(seconds=240)).isoformat(),
    }

    def authority_for(event_market_start: int) -> dict:
        event_ts = now.timestamp() - 200
        return deadman._direct_pin_admission_authority(
            candidate={"wallet": wallet},
            ledger={"orders": []},
            hot_history={
                "events": [
                    {
                        "event_id": "between-checks-01a",
                        "source_wallet": wallet,
                        "action": "BUY",
                        "price": 0.31,
                        "source": "polygon_ws",
                        "event_ts": event_ts,
                        "received_at_s": event_ts + 0.2,
                        "market_slug": f"btc-updown-5m-{event_market_start}",
                    }
                ]
            },
            previous_admission_authority=previous,
            now=now,
        )

    admitted = authority_for(market_start)
    assert admitted["authorized"] is True
    interval = admitted["admission_interval_read"]
    assert interval["interval_source"] == "window_open"
    assert interval["rows_qualifying"] == 1
    assert interval["refusal_counts"] == {}
    assert interval["winning_row"]["ingest_age_s"] == 0.2

    prior_window = authority_for(market_start - 300)
    assert prior_window["authorized"] is False
    assert prior_window["admission_interval_read"]["rows_qualifying"] == 0


def test_direct_admission_interval_reports_first_refusal_predicate() -> None:
    now = dt.datetime.fromisoformat("2026-08-04T06:04:00+00:00")
    wallet = "0x" + "b" * 40
    market_start = int(now.timestamp()) // 300 * 300
    base = {
        "source_wallet": wallet,
        "action": "BUY",
        "source": "polygon_ws",
        "market_slug": f"btc-updown-5m-{market_start}",
    }
    events = [
        {**base, "price": 0.40, "event_ts": now.timestamp() - 5, "received_at_s": now.timestamp() - 4},
        {**base, "price": 0.30, "event_ts": now.timestamp() - 20, "received_at_s": now.timestamp() + 20},
        {**base, "price": 0.30, "event_ts": now.timestamp() - 100, "received_at_s": now.timestamp() - 99},
        {**base, "price": 0.30, "event_ts": now.timestamp() + 1, "received_at_s": now.timestamp() + 1},
    ]
    authority = deadman._direct_pin_admission_authority(
        candidate={"wallet": wallet},
        ledger={"orders": []},
        hot_history={"events": events},
        previous_admission_authority={
            "wallet": wallet,
            "checked_at": (now - dt.timedelta(seconds=30)).isoformat(),
        },
        now=now,
    )

    assert authority["authorized"] is False
    assert authority["admission_interval_read"]["rows_scanned"] == 4
    assert authority["admission_interval_read"]["refusal_counts"] == {
        "event_after_now": 1,
        "event_before_interval_start": 1,
        "ingest_age_gt_30s": 1,
        "price_out_of_band": 1,
    }


def test_direct_admission_lookback_covers_ingest_lag_but_not_prior_window() -> None:
    now = dt.datetime.fromisoformat("2026-08-04T07:04:00+00:00")
    wallet = "0x" + "d" * 40
    window_start = int(now.timestamp()) // 300 * 300
    previous = now - dt.timedelta(seconds=5)

    def authority(event_ts: float, market_start: int) -> dict:
        return deadman._direct_pin_admission_authority(
            candidate={"wallet": wallet}, ledger={"orders": []},
            hot_history={"events": [{
                "source_wallet": wallet, "action": "BUY", "price": 0.30,
                "source": "polygon_ws", "event_ts": event_ts,
                "received_at_s": previous.timestamp() + 1,
                "market_slug": f"btc-updown-5m-{market_start}",
            }]},
            previous_admission_authority={"wallet": wallet, "checked_at": previous.isoformat()},
            now=now,
        )

    delayed = authority(previous.timestamp() - 10, window_start)
    assert delayed["authorized"] is True
    prior = authority(window_start - 1, window_start - 300)
    assert prior["authorized"] is False


def test_admission_publish_preserves_body_checked_at_before_cycle_final_write(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text('{"checked_at":"old","unrelated":{"kept":true}}')
    checked_at = dt.datetime.fromisoformat("2026-08-04T07:01:00+00:00")
    authority = {
        "checked_at": checked_at.isoformat(),
        "authorized": True,
        "admission_interval_read": {"interval_end": checked_at.isoformat()},
    }

    published = deadman._publish_admission_authority(
        state_path,
        authority=authority,
        checked_at=checked_at,
        published_at=checked_at + dt.timedelta(seconds=1),
    )

    on_disk = json.loads(state_path.read_text())
    assert published["authorized"] is True
    assert on_disk["checked_at"] == "old"
    assert on_disk["admission_checked_at"] == checked_at.isoformat()
    assert on_disk["admission_published_at"] == (checked_at + dt.timedelta(seconds=1)).isoformat()
    assert on_disk["unrelated"] == {"kept": True}
    assert on_disk["policy_choke"]["actuator"]["admission_authority"]["publish_lag_s"] == 1.0
    audit = json.loads((tmp_path / "order_flow_deadman_admission_intervals.jsonl").read_text())
    assert audit["published_at"] == (checked_at + dt.timedelta(seconds=1)).isoformat()
    assert audit["crossed"] is False


def test_admission_final_publish_downgrades_prior_window() -> None:
    interval_end = dt.datetime.fromisoformat("2026-08-04T07:04:59+00:00")
    published_at = dt.datetime.fromisoformat("2026-08-04T07:05:01+00:00")

    published = deadman._admission_authority_for_publish(
        {
            "authorized": True,
            "status": "DIRECT_PIN_ADMISSION_AUTHORIZED",
            "admission_interval_read": {"interval_end": interval_end.isoformat()},
        },
        published_at=published_at,
    )

    assert published["authorized"] is False
    assert published["status"] == "ADMISSION_STALE_WINDOW"
    assert published["stale_window_refusal"]["interval_window_start_s"] != published["stale_window_refusal"]["publish_window_start_s"]


def test_one_cycle_produces_five_publish_audit_rows_with_final_lag(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "deadman.json"
    state_path.write_text('{"checked_at":"prior-cycle"}')
    start = dt.datetime.fromisoformat("2026-08-04T07:01:00+00:00")
    latest = None
    for index, stage in enumerate(
        ("early", "inputs_loaded", "candidate_selected", "actuator_complete")
    ):
        checked_at = start + dt.timedelta(seconds=index * 20)
        latest = deadman._publish_admission_authority(
            state_path,
            authority={
                "checked_at": checked_at.isoformat(),
                "authorized": False,
                "admission_interval_read": {
                    "interval_start": start.isoformat(),
                    "interval_end": checked_at.isoformat(),
                    "interval_source": "window_open",
                },
            },
            checked_at=checked_at,
            published_at=checked_at + dt.timedelta(seconds=1),
            stage=stage,
            cycle_elapsed_s=float(index * 20 + 1),
        )

    assert latest is not None
    final_published_at = start + dt.timedelta(seconds=65)
    final_authority = deadman._admission_authority_for_publish(
        latest,
        published_at=final_published_at,
    )
    final_checked_at = dt.datetime.fromisoformat(final_authority["checked_at"])
    deadman._append_admission_publish_audit(
        state_path,
        published=final_authority,
        checked_at=final_checked_at,
        published_at=final_published_at,
        stage="final",
        cycle_elapsed_s=65.0,
    )

    rows = [
        json.loads(line)
        for line in (tmp_path / "order_flow_deadman_admission_intervals.jsonl")
        .read_text()
        .splitlines()
    ]
    assert len(rows) == 5
    assert rows[-1]["stage"] == "final"
    assert rows[-1]["checked_at"] == final_authority["checked_at"]
    assert rows[-1]["publish_lag_s"] == 5.0


def test_simulated_350s_candidate_stage_yields_three_spread_heartbeats() -> None:
    last = 0.0
    published_at = []
    for current in (15.0, 65.0, 130.0, 195.0, 260.0, 325.0, 350.0):
        if deadman._heartbeat_due(last, current):
            published_at.append(current)
            last = current

    assert len(published_at) >= 3
    assert published_at == [65.0, 130.0, 195.0, 260.0, 325.0]
    assert all(
        later - earlier >= 60.0
        for earlier, later in zip(published_at, published_at[1:])
    )


def test_unseen_counter_excludes_prior_window_arrival() -> None:
    now = dt.datetime.fromisoformat("2026-08-04T07:05:20+00:00")
    previous = now - dt.timedelta(seconds=5)
    wallet = "0x" + "e" * 40
    window_start = int(now.timestamp()) // 300 * 300
    authority = deadman._direct_pin_admission_authority(
        candidate={"wallet": wallet},
        ledger={"orders": []},
        hot_history={
            "events": [
                {
                    "source_wallet": wallet,
                    "action": "BUY",
                    "price": 0.30,
                    "source": "polygon_ws",
                    "event_ts": window_start - 1,
                    "received_at_s": previous.timestamp() + 1,
                    "market_slug": f"btc-updown-5m-{window_start}",
                }
            ]
        },
        previous_admission_authority={
            "wallet": wallet,
            "checked_at": previous.isoformat(),
        },
        now=now,
    )

    refusals = authority["admission_interval_read"]["refusal_counts"]
    assert refusals["event_before_interval_start"] == 1
    assert "event_before_interval_start_unseen" not in refusals


def test_direct_pin_write_refuses_empty_admission_authority() -> None:
    candidate = {
        "wallet": "0x" + "b" * 40,
        "eligible": True,
        "paper_policy_id": "fast_wf_test",
        "policy": {"policy_id": "fast_wf_test", "max_order_usd": 1.0},
    }
    overlay = {"members": []}

    unchanged, report = deadman._execute_policy_choke_rung_b(
        overlay=overlay,
        candidate=candidate,
        now=dt.datetime.fromisoformat("2026-08-03T08:20:20+00:00"),
        supply_rung="DIRECT",
        admission_authority={"authorized": False, "orders_submitted": 0},
    )

    assert unchanged == overlay
    assert report["status"] == "DIRECT_PIN_ADMISSION_REFUSED_EMPTY"


def test_deadman_preserves_active_external_selection_pin() -> None:
    now = dt.datetime.fromisoformat("2026-08-03T08:30:00+00:00")
    pin = {
        "enabled": True,
        "pin_id": "fable-fresh-01a",
        "source_wallet": "0x" + "c" * 40,
        "expires_at": "2026-08-03T09:00:00+00:00",
    }
    overlay = {"members": [], "selection_pin": pin}

    unchanged, report = deadman._execute_policy_choke_rung_b(
        overlay=overlay,
        candidate={
            "wallet": "0x" + "d" * 40,
            "eligible": True,
            "paper_policy_id": "fast_wf_test",
            "policy": {"policy_id": "fast_wf_test", "max_order_usd": 1.0},
        },
        now=now,
        supply_rung="DIRECT",
        admission_authority={"authorized": True, "orders_submitted": 1},
    )

    assert unchanged == overlay
    assert report["status"] == "ACTIVE_SELECTION_PIN_AUTHORITY_PRESERVED"
