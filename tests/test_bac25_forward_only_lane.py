import pytest

from scripts import build_bac25_forward_only_lane as subject
from scripts import order_flow_deadman
from src.trade_executor import WALLET_COPY_MIN_SHARE_HARD_CAP_USD
from src.wallet_copy.venue_executability import (
    VENUE_NOMINAL_MIN_ORDER_USD,
    VENUE_REACHABLE_SHARE_MIN_PCT,
)


def _cell(*, resolved: int, reachable: float, first: float, second: float):
    return {
        "wide_policy_fingerprint": subject.FINGERPRINT,
        "identity": {
            "wallet": subject.WALLET,
            "move_slice_keys": ["120-180|0.25-0.50"],
        },
        "evidence_authority": "venue_executable_full_stream_rescore",
        "venue_executable_full_stream_rescore": {
            "evidence_authority": "venue_executable_full_stream_rescore",
            "resolved": resolved,
            "post_fee_pnl_usd": 10.0,
            "roi_pct": 5.0,
            "venue_reachable_share_pct": reachable,
            "half_pnl_excluding_top_1_market": {
                "first_half": first,
                "second_half": second,
            },
        },
    }


def test_forward_lane_registers_at_zero_without_retrospective_credit():
    global_evidence = {"cells": [_cell(resolved=720, reachable=42.6, first=38.7, second=31.8)]}
    manifest = subject.register_manifest(
        global_evidence,
        registered_at="2026-07-30T08:40:00Z",
        terminal_family_registry=[],
    )
    manifest = subject.ensure_manifest_clock(manifest)
    state = subject.build_state(
        manifest=manifest,
        global_evidence=global_evidence,
        forward_evidence={},
        generated_at="2026-07-30T08:40:01Z",
    )

    assert state["retrospective_n"] == 720
    assert state["forward_n"] == 0
    assert state["retrospective_is_admission_input"] is False
    assert state["retrospective_and_forward_may_be_summed"] is False
    assert state["admission_eligible"] is False
    assert manifest["observation_window_s"] == 86_400
    assert manifest["observation_deadline_at"] == "2026-07-31T08:40:00+00:00"
    assert manifest["terminal_outcome_on_deadline"]["status"] == "PARK_FORWARD_EVIDENCE_REFUSED"
    assert manifest["terminal_outcome_on_deadline"]["stop_writer"] is True
    assert manifest["terminal_outcome_on_deadline"]["live_authority"] is False
    family_outcomes = manifest["terminal_outcome_on_deadline"][
        "policy_family_outcomes_at_deadline"
    ]
    assert family_outcomes["insufficient_sample"]["status"] == (
        "MEASURED_NEGATIVE_INSUFFICIENT_SAMPLE"
    )
    assert family_outcomes["full_sample"]["status"] == (
        "MEASURED_NEGATIVE_FULL_SAMPLE"
    )
    assert family_outcomes["full_sample"]["wide_policy_fingerprint"] == (
        subject.FINGERPRINT
    )
    assert family_outcomes["full_sample"]["refuse_alias_reregistration"] is True
    assert state["forward_evidence_projection"]["status"] == (
        "PROJECTED_EXPIRY_WITHOUT_EVIDENCE"
    )
    assert state["forward_evidence_projection"]["projected_forward_n_at_deadline"] == 0.0
    assert manifest["capture_watch_wallets"][0]["wide_policy_fingerprint"] == subject.FINGERPRINT

    drifted_state = subject.build_state(
        manifest=manifest,
        global_evidence={
            "cells": [
                _cell(resolved=721, reachable=42.6, first=39.0, second=32.0)
            ]
        },
        forward_evidence={},
        generated_at="2026-07-30T08:40:02Z",
    )
    assert drifted_state["retrospective_n"] == 720


def test_venue_reachability_floor_is_derived_from_live_nominal_to_hard_cap():
    assert VENUE_REACHABLE_SHARE_MIN_PCT == (
        100.0 * VENUE_NOMINAL_MIN_ORDER_USD / WALLET_COPY_MIN_SHARE_HARD_CAP_USD
    )
    assert VENUE_REACHABLE_SHARE_MIN_PCT == 40.0


def test_manifest_clock_refuses_moving_generated_at_fallback():
    with pytest.raises(ValueError, match="immutable effective_at missing"):
        subject.ensure_manifest_clock(
            {
                "generated_at": "2026-07-30T09:00:00Z",
                "observation_window_s": 1,
            }
        )


def test_observation_window_change_is_refused_after_forward_accrual():
    spec = subject.LaneSpec(
        subject.WALLET,
        subject.FINGERPRINT,
        subject.POLICY_FAMILY,
        subject.DEFAULT_SPEC.score_run_id,
        subject.DEFAULT_SPEC.lane_kind,
        172_800,
    )
    with pytest.raises(ValueError, match="FORWARD_WINDOW_CHANGE_REFUSED_AFTER_ACCRUAL"):
        subject.refuse_observation_window_change_after_accrual(
            {"observation_window_s": 86_400},
            {"forward_n": 1},
            spec,
        )
    with pytest.raises(ValueError, match="current_window_s=missing"):
        subject.refuse_observation_window_change_after_accrual(
            {},
            {"forward_n": 1},
            spec,
        )

    subject.refuse_observation_window_change_after_accrual(
        {"observation_window_s": 86_400},
        {"forward_n": 0},
        spec,
    )


def test_terminal_negative_fingerprint_cannot_be_reregistered_under_alias():
    global_evidence = {
        "cells": [_cell(resolved=720, reachable=42.6, first=38.7, second=31.8)]
    }
    with pytest.raises(ValueError, match="refusing alias re-registration"):
        subject.register_manifest(
            global_evidence,
            registered_at="2026-08-01T00:00:00Z",
            policy_family="renamed_same_policy",
            terminal_family_registry=[
                {
                    "wide_policy_fingerprint": subject.FINGERPRINT,
                    "status": "MEASURED_NEGATIVE_FULL_SAMPLE",
                    "refuse_alias_reregistration": True,
                }
            ],
        )


def test_default_registry_path_refuses_terminal_fingerprint(
    tmp_path, monkeypatch
):
    registry = tmp_path / "registry.json"
    registry.write_text(
        '{"entries":[{"wide_policy_fingerprint":"'
        + subject.FINGERPRINT
        + '","status":"MEASURED_NEGATIVE_INSUFFICIENT_SAMPLE",'
        + '"refuse_alias_reregistration":true}]}'
    )
    monkeypatch.setattr(subject, "DEFAULT_FAMILY_TERMINAL_REGISTRY", str(registry))
    global_evidence = {
        "cells": [_cell(resolved=720, reachable=42.6, first=38.7, second=31.8)]
    }

    with pytest.raises(ValueError, match="refusing alias re-registration"):
        subject.register_manifest(
            global_evidence,
            registered_at="2026-08-01T00:00:00Z",
            policy_family="renamed_same_policy",
        )


def test_active_registry_prevents_alias_without_premature_terminal_verdict():
    state = {
        "generated_at": "2026-07-30T12:00:00Z",
        "observation_deadline_at": "2026-07-31T08:33:31Z",
        "forward_n": 28,
        "forward_post_fee_pnl_usd": 9.14,
    }

    registry = subject.update_family_terminal_registry({}, state)
    entry = registry["entries"][0]

    assert entry["status"] == "REGISTERED_ACTIVE"
    assert entry["terminal"] is False
    assert entry["stop_writer"] is False
    assert entry["refuse_alias_reregistration"] is True
    assert entry["precommitted_negative_outcomes"]["insufficient_sample"][
        "status"
    ] == "MEASURED_NEGATIVE_INSUFFICIENT_SAMPLE"


@pytest.mark.parametrize(
    ("forward_n", "expected"),
    [
        (199, "MEASURED_NEGATIVE_INSUFFICIENT_SAMPLE"),
        (200, "MEASURED_NEGATIVE_FULL_SAMPLE"),
    ],
)
def test_registry_applies_negative_branch_only_at_deadline(forward_n, expected):
    registry = subject.update_family_terminal_registry(
        {},
        {
            "generated_at": "2026-07-31T08:33:31Z",
            "observation_deadline_at": "2026-07-31T08:33:31Z",
            "forward_n": forward_n,
            "forward_post_fee_pnl_usd": -1.0,
        },
    )

    entry = registry["entries"][0]
    assert entry["status"] == expected
    assert entry["terminal"] is True
    assert entry["stop_writer"] is True


@pytest.mark.parametrize(
    ("pnl", "forward_n", "admission_eligible", "expected"),
    [
        (-1.0, 199, False, "MEASURED_NEGATIVE_INSUFFICIENT_SAMPLE"),
        (1.0, 199, False, "MEASURED_INADMISSIBLE_INSUFFICIENT_SAMPLE"),
        (1.0, 200, False, "MEASURED_INADMISSIBLE_CONCENTRATION"),
        (1.0, 200, True, "MEASURED_ADMISSIBLE"),
    ],
)
def test_deadline_registry_never_terminalizes_as_active(
    pnl, forward_n, admission_eligible, expected
):
    state = {
        "generated_at": "2026-07-31T08:33:31Z",
        "observation_deadline_at": "2026-07-31T08:33:31Z",
        "forward_n": forward_n,
        "forward_post_fee_pnl_usd": pnl,
        "admission_eligible": admission_eligible,
        "checks": {
            "forward_n_gte_200": forward_n >= 200,
            "forward_first_half_pnl_excluding_top_1_market_positive": (
                admission_eligible
            ),
            "forward_second_half_pnl_excluding_top_1_market_positive": (
                admission_eligible
            ),
        },
    }

    entry = subject.update_family_terminal_registry({}, state)["entries"][0]

    assert entry["status"] == expected
    assert entry["terminal"] is True
    assert entry["status"] != "REGISTERED_ACTIVE"
    assert entry["live_authority"] is False
    if admission_eligible is False:
        assert entry["reason_source"]
    if expected == "MEASURED_INADMISSIBLE_INSUFFICIENT_SAMPLE":
        assert "forward_n_gte_200" in entry["reason_source"]
    if expected == "MEASURED_INADMISSIBLE_CONCENTRATION":
        assert entry["reason_source"] == [
            "forward_first_half_pnl_excluding_top_1_market_positive",
            "forward_second_half_pnl_excluding_top_1_market_positive",
        ]


@pytest.mark.parametrize(
    ("forward_n", "expected"),
    [
        (199, "MEASURED_NEGATIVE_INSUFFICIENT_SAMPLE"),
        (200, "MEASURED_NEGATIVE_FULL_SAMPLE"),
        (201, "MEASURED_NEGATIVE_FULL_SAMPLE"),
    ],
)
def test_negative_family_outcome_covers_both_sample_branches(
    forward_n, expected
):
    outcome = subject.negative_family_outcome(forward_n, -1.0)
    assert outcome["status"] == expected
    assert outcome["terminal"] is True
    assert outcome["stop_writer"] is True
    assert outcome["refuse_alias_reregistration"] is True


def test_forward_lane_publishes_boundary_proximity_from_marginal_rows():
    global_evidence = {
        "cells": [_cell(resolved=720, reachable=42.6, first=38.7, second=31.8)]
    }
    manifest = subject.register_manifest(
        global_evidence,
        registered_at="2026-07-30T08:40:00Z",
        terminal_family_registry=[],
    )
    forward_cell = _cell(resolved=35, reachable=45.0, first=-1.0, second=-2.0)
    forward_cell["venue_executable_full_stream_rescore"]["post_fee_pnl_usd"] = 1.8
    state = subject.build_state(
        manifest=manifest,
        global_evidence=global_evidence,
        forward_evidence={"cells": [forward_cell]},
        generated_at="2026-07-30T09:40:00Z",
        previous_state={
            "forward_n": 34,
            "forward_post_fee_pnl_usd": 2.8,
        },
        observed_venue_marginal_pnl_per_row=-0.5,
    )

    assert state["marginal_pnl_per_row"] == -1.0
    assert state["headline_margin_rows"] == 1.8
    assert state["boundary_proximity"] is True
    assert state["marginal_basis"] == "unfiltered_journal_batch"
    assert state["headline_margin_rows_basis"] != (
        state["headline_margin_rows_venue_executable_basis"]
    )
    assert state["headline_margin_rows_venue_executable"] == 3.6
    assert state["decision_variable"] == (
        "forward_half_pnl_excluding_top_1_market"
    )
    assert state["headline_sign_reliability"] == "UNRELIABLE_BELOW_REQUIRED_N"
    assert state["top_1_market_contribution_usd"] == 4.8
    registry = subject.update_family_terminal_registry({}, state)["entries"][0]
    assert registry["score_run_id"] == "bac25_forward_only"
    assert registry["headline_margin_rows"] == 1.8
    assert registry["boundary_proximity"] is True
    assert registry["marginal_basis"] == "unfiltered_journal_batch"
    assert registry["headline_margin_rows_venue_executable"] == 3.6


def test_latest_resolution_batch_supplies_marginal_when_sample_is_unchanged(
    tmp_path,
):
    orders = tmp_path / "orders.jsonl"
    orders.write_text(
        "\n".join(
            [
                '{"event":"wide_exact_policy_paper_order_resolved",'
                '"resolution_computed_at":"2026-07-30T11:40:00Z",'
                '"post_fee_pnl_usd":9.0}',
                '{"event":"wide_exact_policy_paper_order_resolved",'
                '"resolution_computed_at":"2026-07-30T11:52:41Z",'
                '"post_fee_pnl_usd":-1.03}',
                '{"event":"wide_exact_policy_paper_order_resolved",'
                '"resolution_computed_at":"2026-07-30T11:52:41Z",'
                '"post_fee_pnl_usd":-1.04}',
            ]
        )
    )

    assert subject.latest_resolution_batch_marginal(orders) == -1.035


def test_parameterized_lane_precommits_zero_intent_terminal():
    spec = subject.LaneSpec(
        "0x951bd740ef681d05891ca35440232488271d433e",
        "2d8af91d223d7cc91753bf4b9ad0a03dcaeeebbb3903becbfc2382ae0d34f212",
        "fast_wf_0.10_cap_4_951b_two_slice_forward",
        "951b_forward_only",
        "951b_forward_only_paper_lane",
    )
    cell = _cell(resolved=85, reachable=100.0, first=-1.0, second=-1.0)
    cell["identity"]["wallet"] = spec.wallet
    cell["wide_policy_fingerprint"] = spec.fingerprint
    manifest = subject.register_manifest(
        {"cells": [cell]},
        registered_at="2026-07-30T12:00:00Z",
        policy_family=spec.policy_family,
        terminal_family_registry=[],
        spec=spec,
    )
    manifest = subject.ensure_manifest_clock(manifest, spec)

    assert manifest["capture_watch_wallets"][0]["wallet"] == spec.wallet
    assert manifest["score_run_id"] == "951b_forward_only"
    assert manifest["score_run_id"] != subject.register_manifest(
        {
            "cells": [
                _cell(resolved=720, reachable=42.6, first=38.7, second=31.8)
            ]
        },
        registered_at="2026-07-30T12:00:00Z",
        terminal_family_registry=[],
    )["score_run_id"]
    zero = manifest["terminal_outcome_on_deadline"][
        "policy_family_outcomes_at_deadline"
    ]["zero_intent_generation"]
    assert zero["status"] == "PARK_ZERO_INTENT_GENERATION"
    assert zero["stop_writer"] is True
    assert zero["deadline_extension_allowed"] is False
    registry = subject.update_family_terminal_registry(
        {},
        {
            "generated_at": "2026-07-31T12:00:00Z",
            "observation_deadline_at": "2026-07-31T12:00:00Z",
            "forward_n": 0,
            "forward_post_fee_pnl_usd": 0,
            "admission_eligible": False,
            "checks": {"forward_n_gte_200": False},
        },
        spec,
    )
    assert registry["entries"][0]["status"] == "PARK_ZERO_INTENT_GENERATION"
    state = subject.build_state(
        manifest=manifest,
        global_evidence={"cells": [cell]},
        forward_evidence={"cells": []},
        generated_at="2026-07-30T12:01:00Z",
        spec=spec,
    )
    assert state["kind"] == "951b_forward_only_paper_lane"
    assert state["score_run_id"] == "951b_forward_only"


def test_forward_lane_requires_n200_both_ex_top1_halves_and_reachability():
    global_evidence = {"cells": [_cell(resolved=720, reachable=42.6, first=38.7, second=31.8)]}
    manifest = subject.register_manifest(
        global_evidence,
        registered_at="2026-07-30T08:40:00Z",
        terminal_family_registry=[],
    )
    forward_evidence = {
        "cells": [_cell(resolved=200, reachable=45.0, first=2.0, second=1.0)]
    }
    state = subject.build_state(
        manifest=manifest,
        global_evidence=global_evidence,
        forward_evidence=forward_evidence,
        generated_at="2026-07-30T08:45:00Z",
    )

    assert state["forward_n"] == 200
    assert state["admission_eligible"] is True
    assert state["forward_evidence_projection"]["status"] == (
        "PROJECTED_TO_REACH_EVIDENCE_GATE"
    )

    forward_evidence["cells"][0]["venue_executable_full_stream_rescore"][
        "half_pnl_excluding_top_1_market"
    ]["second_half"] = 0.0
    refused = subject.build_state(
        manifest=manifest,
        global_evidence=global_evidence,
        forward_evidence=forward_evidence,
        generated_at="2026-07-30T08:45:01Z",
    )
    assert refused["admission_eligible"] is False
    assert refused["forward_evidence_projection"]["status"] == (
        "PROJECTED_TO_REACH_N_WITH_FAILING_SUBSTANTIVE_CHECKS"
    )
    assert refused["forward_evidence_projection"]["admission_forecast"] is False


def test_negative_forward_pnl_outranks_failing_substantive_checks():
    global_evidence = {"cells": [_cell(resolved=720, reachable=42.6, first=38.7, second=31.8)]}
    manifest = subject.register_manifest(
        global_evidence,
        registered_at="2026-07-30T08:40:00Z",
        terminal_family_registry=[],
    )
    forward_cell = _cell(resolved=16, reachable=45.0, first=0.12, second=-7.30)
    forward_cell["venue_executable_full_stream_rescore"]["post_fee_pnl_usd"] = -4.44
    state = subject.build_state(
        manifest=manifest,
        global_evidence=global_evidence,
        forward_evidence={"cells": [forward_cell]},
        generated_at="2026-07-30T09:40:00Z",
    )

    projection = state["forward_evidence_projection"]
    assert projection["projected_forward_n_at_deadline"] > 200
    assert projection["status"] == "PROJECTED_TO_REACH_N_WITH_NEGATIVE_FORWARD_PNL"
    assert projection["admission_forecast"] is False
    assert state["forward_post_fee_pnl_usd"] == -4.44
    assert state["forward_half_pnl_excluding_top_1_market"] == {
        "first_half": 0.12,
        "second_half": -7.30,
    }


def test_expiry_and_negative_pnl_are_both_preserved():
    global_evidence = {"cells": [_cell(resolved=720, reachable=42.6, first=38.7, second=31.8)]}
    manifest = subject.register_manifest(
        global_evidence,
        registered_at="2026-07-30T08:40:00Z",
        terminal_family_registry=[],
    )
    cell = _cell(resolved=1, reachable=45.0, first=-1.0, second=-2.0)
    cell["venue_executable_full_stream_rescore"]["post_fee_pnl_usd"] = -3.0
    state = subject.build_state(
        manifest=manifest,
        global_evidence=global_evidence,
        forward_evidence={"cells": [cell]},
        generated_at="2026-07-30T09:40:00Z",
    )
    projection = state["forward_evidence_projection"]

    assert projection["status"] == "PROJECTED_EXPIRY_WITHOUT_EVIDENCE_AND_NEGATIVE_FORWARD_PNL"
    assert projection["refusal_reasons"] == [
        "PROJECTED_EXPIRY_WITHOUT_EVIDENCE",
        "NEGATIVE_FORWARD_PNL",
        "FAILING_SUBSTANTIVE_CHECKS",
    ]


def test_deadman_exposes_lane_only_after_forward_gate_passes():
    manifest = {"manifest_id": "forward-manifest"}
    identity = {
        "wallet": subject.WALLET,
        "policy_id": "wide-forward",
        "wide_policy_fingerprint": subject.FINGERPRINT,
    }
    forward_evidence = {
        "cells": [
            {
                "wide_policy_fingerprint": subject.FINGERPRINT,
                "identity": identity,
                "evidence_authority": "venue_executable_full_stream_rescore",
                "venue_executable_full_stream_rescore": {
                    "resolved": 200,
                    "f1_walk_forward_admissible": True,
                    "venue_reachable_share_pct": 40.0,
                },
            }
        ]
    }
    lane = {
        "admission_eligible": False,
        "manifest_id": "forward-manifest",
        "wallet": subject.WALLET,
        "wide_policy_fingerprint": subject.FINGERPRINT,
        "forward_n": 200,
        "retrospective_is_admission_input": False,
        "retrospective_and_forward_may_be_summed": False,
    }

    direct, evidence = order_flow_deadman._admit_forward_only_supply(
        direct_source={},
        fingerprint_evidence={"cells": []},
        lane=lane,
        manifest=manifest,
        forward_evidence=forward_evidence,
    )
    assert direct == {}
    assert evidence == {"cells": []}

    direct, evidence = order_flow_deadman._admit_forward_only_supply(
        direct_source={},
        fingerprint_evidence={"cells": []},
        lane={**lane, "admission_eligible": True},
        manifest=manifest,
        forward_evidence=forward_evidence,
    )
    generation = next(iter(direct["per_wallet_generation"].values()))
    assert generation["attempts"] == 200
    assert generation["retrospective_credit"] == 0
    assert evidence["cells"] == forward_evidence["cells"]
