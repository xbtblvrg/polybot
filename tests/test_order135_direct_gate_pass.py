import datetime as dt
import json

from scripts import run_wallet_copy_live_guard as guard
from scripts.run_wallet_copy_live_guard import _order135_direct_gate_pass


WALLET = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
FINGERPRINT = "4028560ff42ee6da51e715295b2e78eeacceb04d9c538cd2df4eb5f2b98a5c46"


def _write(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def _evidence():
    rescore = {
        "resolved": 439,
        "post_fee_pnl_usd": 95.473794,
        "roi_pct": 21.748017,
        "first_half_post_fee_pnl_usd": 38.883958,
        "second_half_post_fee_pnl_usd": 56.589836,
    }
    return {
        "generated_at": "2026-08-01T09:12:00Z",
        "cells": [
            {
                "identity": {"wallet": WALLET},
                "wide_policy_fingerprint": FINGERPRINT,
                "evidence_authority": "venue_executable_full_stream_rescore",
                "venue_executable_full_stream_rescore": rescore,
            }
        ],
        "freeze_overrides": {
            WALLET: {
                "wide_policy_fingerprint": FINGERPRINT,
                "reason": "climb_priority_exact_fp_f1_closed_paper_feedstock",
                "f1": rescore,
            }
        },
    }


def _deadman(*, fresh=10, f3=True, f4=True, checked_at="2026-08-01T09:12:00Z"):
    f2 = fresh >= 10
    eligible = f2 and f4
    candidate = {
        "wallet": WALLET,
        "wide_policy_fingerprint": FINGERPRINT,
        "eligible": eligible,
        "paper_policy_id": "wide_fp_4028560f",
        "policy": {
            "policy_id": "wide_fp_4028560f",
            "max_order_usd": 1.0,
            "min_order_usd": 1.0,
        },
        "fresh_own_source_buy_rows_30m": fresh,
        "checks": {
            "active_temporal_not_proven_negative": True,
            "f2_fresh_rows_and_own_policy_copyable": f2,
            "f3_not_enabled_or_cooloff_or_fading": f3,
            "f4_external_liveness": f4,
            "not_terminal_park_red_clock_or_measured_loser": True,
            "own_evidenced_policy_available": True,
        },
    }
    return {
        "checked_at": checked_at,
        "policy_choke_rung_b_cooloffs": {},
        "policy_choke": {
            "actuator": {
                "candidate_evidence": {
                    "gate_digits": {
                        "f2_min_fresh_own_source_buy_rows_30m": 10,
                    },
                    "nearest_frontier": [candidate],
                }
            },
            "source_roster_drought": {
                "direct_source": {"checksum": "packet-checksum"},
            },
        },
    }


def _direct_packet(*, attempts=10, copyable=1, updated_at="2026-08-01T09:12:00Z"):
    attempt_rows = [
        {
            "wallet": WALLET,
            "attempt_id": f"attempt-{index}",
            "order_id": f"order-{index}" if index < copyable else None,
            "recorded_at": updated_at,
            "f1_f4_terminal": (
                {
                    "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
                    "F4_executable_book": "PASS",
                }
                if index < copyable
                else {"terminal": "REFUSED_TEST"}
            ),
        }
        for index in range(attempts)
    ]
    orders = [
        {
            "wallet": WALLET,
            "order_id": f"order-{index}",
            "recorded_at": updated_at,
            "f1_f4_terminal": {
                "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
                "F4_executable_book": "PASS",
            },
        }
        for index in range(copyable)
    ]
    return {
        "updated_at": updated_at,
        "policy_id": "wide_fp_4028560f",
        "terminal_reconciliation": {
            "direct_event_handoff": True,
            "input_equals_terminal": True,
            "input_rows": attempts,
            "terminal_rows": attempts,
        },
        "attempt_terminals": attempt_rows,
        "orders": orders,
    }


def _paths(
    tmp_path,
    deadman,
    *,
    attempts=10,
    copyable=1,
    packet_updated_at="2026-08-01T09:12:00Z",
    cover_journal=True,
):
    evidence = tmp_path / "evidence.json"
    deadman_path = tmp_path / "deadman.json"
    shadow = tmp_path / "shadow.json"
    sidecar = tmp_path / "sidecar.json"
    overlay = tmp_path / "overlay.json"
    log = tmp_path / "passes.jsonl"
    direct_packet = tmp_path / "direct-packet.json"
    journal = tmp_path / "journal.jsonl"
    _write(evidence, _evidence())
    _write(deadman_path, deadman)
    _write(shadow, {"primary": {"wallet": WALLET, "wide_policy_fingerprint": FINGERPRINT}})
    _write(overlay, {"members": []})
    _write(
        direct_packet,
        _direct_packet(
            attempts=attempts,
            copyable=copyable,
            updated_at=packet_updated_at,
        ),
    )
    if cover_journal:
        journal.write_text(
            json.dumps(
                {
                    "captured_at": "2026-08-01T08:40:00Z",
                    "source_generation": "old-generation",
                    "input_rows": 0,
                    "terminal_rows": 0,
                    "input_equals_terminal": True,
                    "rows": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )
    else:
        journal.write_text("", encoding="utf-8")
    guard._ORDER135_DIRECT_PROJECTION_CACHE.clear()
    return evidence, deadman_path, shadow, sidecar, overlay, log, direct_packet, journal


def test_order135_same_pass_invokes_direct_pin_only_on_fresh_all_pass(tmp_path):
    paths = _paths(tmp_path, _deadman())
    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0],
        deadman_path=paths[1],
        freeze_shadow_path=paths[2],
        sidecar_path=paths[3],
        overlay_path=paths[4],
        gate_log_path=str(paths[5]),
        direct_packet_path=paths[6],
        direct_journal_path=paths[7],
    )

    assert result["status"] == "DIRECT_SOURCE_SELECTION_PIN_WRITTEN"
    assert result["all_pass"] is True
    assert result["within_packet_budget"] is True
    assert result["packet_checksum"]
    assert result["packet_checksum"] == result["direct_packet_checksum"]
    assert result["projection_ready"] is True
    assert result["projection_direct_event_handoff"] is True
    assert result["projection_input_equals_terminal"] is True
    assert result["projection_current_attempted_buy_rows"] == 10
    assert result["projection_copyable_parity"] is True
    assert result["projection_pinned_attempts"] == 10
    assert result["projection_pinned_copyable"] == 1
    assert result["invoked"] is True
    overlay = json.loads(paths[4].read_text(encoding="utf-8"))
    assert overlay["selection_pin"]["source_wallet"] == WALLET
    audit = json.loads(paths[5].read_text(encoding="utf-8").splitlines()[-1])
    assert audit["all_pass"] is True
    assert audit["packet_checksum"] == result["direct_packet_checksum"]


def test_order135_same_pass_records_false_gate_without_invocation(tmp_path):
    paths = _paths(tmp_path, _deadman(fresh=3, f3=False, f4=False))

    def forbidden_actuator(**_kwargs):
        raise AssertionError("false gate must not invoke actuator")

    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0],
        deadman_path=paths[1],
        freeze_shadow_path=paths[2],
        sidecar_path=paths[3],
        overlay_path=paths[4],
        gate_log_path=str(paths[5]),
        direct_packet_path=paths[6],
        direct_journal_path=paths[7],
        actuator=forbidden_actuator,
    )

    assert result["status"] == "WAIT_OTHER_GATE"
    assert result["all_pass"] is False
    assert result["invoked"] is False
    assert result["false_checks"] == [
        "f2_fresh_rows_and_own_policy_copyable",
        "f3_not_enabled_or_cooloff_or_fading",
        "f4_external_liveness",
    ]


def test_order135_fresh_projection_supersedes_stale_sidecar_f2_f4(tmp_path):
    paths = _paths(tmp_path, _deadman(fresh=3))
    invoked = []

    def actuator(**kwargs):
        invoked.append(kwargs)
        return kwargs["overlay"], {"status": "DIRECT_SOURCE_SELECTION_PIN_WRITTEN"}

    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0], deadman_path=paths[1],
        freeze_shadow_path=paths[2], sidecar_path=paths[3], overlay_path=paths[4],
        gate_log_path=str(paths[5]), direct_packet_path=paths[6],
        direct_journal_path=paths[7], actuator=actuator,
    )

    assert result["sidecar_all_pass"] is False
    assert result["history_halves_pass"] is True
    assert result["all_pass"] is True
    assert result["direct_f2"]["attempts"] == 10
    assert result["direct_f4_pass"] is True
    assert result["invoked"] is True
    assert len(invoked) == 1


def test_order135_cached_projection_preserves_copyable_parity_refusal(tmp_path):
    paths = _paths(tmp_path, _deadman())
    packet = json.loads(paths[6].read_text(encoding="utf-8"))
    packet["attempt_terminals"][1]["order_id"] = "order-0"
    packet["attempt_terminals"][1]["f1_f4_terminal"] = {
        "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
        "F4_executable_book": "PASS",
    }
    _write(paths[6], packet)
    now = dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc)

    first = guard._order135_direct_projection(
        now=now, packet_path=paths[6], journal_path=paths[7]
    )
    cached = guard._order135_direct_projection(
        now=now + dt.timedelta(seconds=1),
        packet_path=paths[6],
        journal_path=paths[7],
    )

    assert first["copyable_parity"] is False
    assert first["ready"] is False
    assert first["status"] == "FAIL_COPYABLE_PARITY"
    assert cached["projection_cache_hit"] is True
    assert cached["copyable_terminal_rows"] == first["copyable_terminal_rows"]
    assert cached["envelope_copyable_orders_in_window"] == first[
        "envelope_copyable_orders_in_window"
    ]
    assert cached["copyable_parity"] is False
    assert cached["ready"] is False
    assert cached["status"] == "FAIL_COPYABLE_PARITY"


def test_order135_refuses_direct_f2_attempts_below_minimum(tmp_path):
    paths = _paths(tmp_path, _deadman(), attempts=9, copyable=0)
    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0],
        deadman_path=paths[1],
        freeze_shadow_path=paths[2],
        sidecar_path=paths[3],
        overlay_path=paths[4],
        gate_log_path=str(paths[5]),
        direct_packet_path=paths[6],
        direct_journal_path=paths[7],
    )

    assert result["status"] == "REFUSED_DIRECT_F2_ARITHMETIC"
    assert result["direct_f2"] == {
        "attempts": 9,
        "copyable": 0,
        "minimum_attempts": 10,
        "minimum_copyable": 1,
    }
    assert result["invoked"] is False


def test_order135_refuses_packet_age_above_30_seconds(tmp_path):
    paths = _paths(tmp_path, _deadman(), packet_updated_at="2026-08-01T09:12:00Z")
    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 31, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0], deadman_path=paths[1],
        freeze_shadow_path=paths[2], sidecar_path=paths[3], overlay_path=paths[4],
        gate_log_path=str(paths[5]), direct_packet_path=paths[6], direct_journal_path=paths[7],
    )

    assert result["status"] == "REFUSED_STALE_DIRECT_PACKET"
    assert result["packet_age_s"] == 31.0
    assert result["invoked"] is False


def test_order135_refuses_incomplete_journal_window(tmp_path):
    paths = _paths(tmp_path, _deadman(), cover_journal=False)
    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0], deadman_path=paths[1],
        freeze_shadow_path=paths[2], sidecar_path=paths[3], overlay_path=paths[4],
        gate_log_path=str(paths[5]), direct_packet_path=paths[6], direct_journal_path=paths[7],
    )

    assert result["status"] == "REFUSED_DIRECT_PROJECTION_INCOMPLETE"
    assert result["journal_window_covered"] is False
    assert result["invoked"] is False


def test_order135_refuses_deadman_evidence_older_than_900_seconds(tmp_path):
    paths = _paths(tmp_path, _deadman(checked_at="2026-08-01T08:56:59Z"))
    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0], deadman_path=paths[1],
        freeze_shadow_path=paths[2], sidecar_path=paths[3], overlay_path=paths[4],
        gate_log_path=str(paths[5]), direct_packet_path=paths[6], direct_journal_path=paths[7],
    )

    assert result["status"] == "REFUSED_STALE_DEADMAN_EVIDENCE"
    assert result["deadman_evidence_age_s"] == 901.0
    assert result["history_halves"] is None
    assert result["history_halves_pass"] is None
    assert result["invoked"] is False


def test_order135_missing_authority_is_labeled_pin_unresolved(tmp_path):
    deadman = _deadman()
    deadman["policy_choke"]["actuator"]["candidate_evidence"][
        "nearest_frontier"
    ] = []
    paths = _paths(tmp_path, deadman)

    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0], deadman_path=paths[1],
        freeze_shadow_path=paths[2], sidecar_path=paths[3], overlay_path=paths[4],
        gate_log_path=str(paths[5]), direct_packet_path=paths[6],
        direct_journal_path=paths[7],
    )

    assert result["status"] == "WAIT_PIN_UNRESOLVED"
    assert result["invoked"] is False


def test_order135_projection_rss_delta_stays_below_100_mib(tmp_path):
    paths = _paths(tmp_path, _deadman())
    before = guard._current_process_rss_gib()
    _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0], deadman_path=paths[1],
        freeze_shadow_path=paths[2], sidecar_path=paths[3], overlay_path=paths[4],
        gate_log_path=str(paths[5]), direct_packet_path=paths[6], direct_journal_path=paths[7],
    )
    after = guard._current_process_rss_gib()

    assert max(0.0, after - before) < (100.0 / 1024.0)


def test_order135_gate_pass_overrides_stale_deadman_f4_liveness(tmp_path):
    """The 2026-08-01T12:45:35Z seam: gate green, deadman artefact stale.

    The deadman snapshot is written on its own ~2-3min cadence, so its
    ``f4_external_liveness`` re-derives ``packet_age_s <= 30`` from a stale
    ``direct_source`` and marks the candidate ineligible.  The gate has just
    proved the packet fresh this cycle; the pass must survive.
    """
    captured = {}

    def _spy(*, overlay, candidate, now, probe_cap_usd, supply_rung):
        captured["candidate"] = candidate
        captured["probe_cap_usd"] = probe_cap_usd
        captured["supply_rung"] = supply_rung
        return overlay, {"status": "DIRECT_SOURCE_SELECTION_PIN_WRITTEN"}

    paths = _paths(tmp_path, _deadman(f4=False))
    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0], deadman_path=paths[1],
        freeze_shadow_path=paths[2], sidecar_path=paths[3], overlay_path=paths[4],
        gate_log_path=str(paths[5]), direct_packet_path=paths[6],
        direct_journal_path=paths[7],
        actuator=_spy,
    )

    assert result["invoked"] is True
    assert result["status"] == "DIRECT_SOURCE_SELECTION_PIN_WRITTEN"
    # the audit trail names the authority that seated the wallet
    assert result["eligible_basis"] == "order135_direct_gate_pass"
    assert result["authority_eligible"] is False
    assert result["authority_false_checks"] == ["f4_external_liveness"]

    candidate = captured["candidate"]
    assert candidate["eligible"] is True
    assert candidate["eligible_basis"] == "order135_direct_gate_pass"
    assert candidate["wallet"] == WALLET
    assert candidate["policy"]["policy_id"] == "wide_fp_4028560f"
    assert captured["probe_cap_usd"] == 1.0
    assert captured["supply_rung"] == "DIRECT"

    liveness = candidate["direct_liveness"]
    assert liveness["packet_max_age_s"] == guard.ORDER135_PACKET_MAX_AGE_S
    assert liveness["packet_age_s"] <= guard.ORDER135_PACKET_MAX_AGE_S
    assert liveness["direct_f4_pass"] is True
    assert liveness["history_halves_pass"] is True
    assert liveness["direct_f2"]["copyable"] >= 1


def test_order135_refused_gate_never_reaches_actuator(tmp_path):
    """No override without a pass: a stale packet still dies before invoke."""
    calls = []

    def _spy(*, overlay, candidate, now, probe_cap_usd, supply_rung):
        calls.append(candidate)
        return overlay, {"status": "DIRECT_SOURCE_SELECTION_PIN_WRITTEN"}

    paths = _paths(
        tmp_path,
        _deadman(f4=False),
        packet_updated_at="2026-08-01T09:00:00Z",
    )
    result = _order135_direct_gate_pass(
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        fingerprint_evidence_path=paths[0], deadman_path=paths[1],
        freeze_shadow_path=paths[2], sidecar_path=paths[3], overlay_path=paths[4],
        gate_log_path=str(paths[5]), direct_packet_path=paths[6],
        direct_journal_path=paths[7],
        actuator=_spy,
    )

    assert result["invoked"] is False
    assert result["status"] == "REFUSED_STALE_DIRECT_PACKET"
    assert result.get("eligible_basis") is None
    assert calls == []


def test_deadman_rung_b_still_refuses_ineligible_candidate():
    """The override is constructed at the gate call site and nowhere else."""
    from scripts.order_flow_deadman import _execute_policy_choke_rung_b

    candidate = {
        "wallet": WALLET,
        "wide_policy_fingerprint": FINGERPRINT,
        "eligible": False,
        "paper_policy_id": "wide_fp_4028560f",
        "policy": {"policy_id": "wide_fp_4028560f", "max_order_usd": 1.0},
        "checks": {"f4_external_liveness": False},
    }
    overlay, result = _execute_policy_choke_rung_b(
        overlay={"members": []},
        candidate=candidate,
        now=dt.datetime(2026, 8, 1, 9, 12, 5, tzinfo=dt.timezone.utc),
        probe_cap_usd=1.0,
        supply_rung="DIRECT",
    )

    assert result["status"] == "RUNG_C_METHOD_SWITCH_DUE"
    assert result["reason"] == "no_F1_F4_rung_b_candidate"
    assert overlay == {"members": []}
