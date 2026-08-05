from scripts.update_state_digest import (
    _annotate_covered_rates,
    _daily_floor_gate_residency,
)


def test_covered_rates_carry_running_binary_enforcement_truth():
    raw = {
        "floor_gate_enforced_fill_rate": 1.0,
        "floor_gate_enforced_fill_rate_pct": 100.0,
    }

    unenforced = _annotate_covered_rates(raw, enforced=False)
    assert unenforced["enforced_by_running_binary"] is False
    assert unenforced["coverage_measurement_status"] == "UNENFORCED_MEASUREMENT"
    assert unenforced["floor_gate_enforced_fill_rate_enforced_by_running_binary"] is False

    enforced = _annotate_covered_rates({"nested": raw}, enforced=True)["nested"]
    assert enforced["enforced_by_running_binary"] is True
    assert (
        enforced["floor_gate_enforced_fill_rate_status"]
        == "PER_ORDER_ENFORCEMENT_MEASUREMENT"
    )


def test_daily_floor_gate_residency_binds_cost_to_loaded_and_disk_generations():
    row = _daily_floor_gate_residency(
        "2026-08-04",
        {
            "fills": 9,
            "floor_gate_enforced_fill_count": 0,
            "ruled_floor_breach_count": 4,
            "ruled_floor_breach_cost_usd": 4.759996,
        },
        loaded_generation_sha256="resident-sha",
        disk_generation_sha256="disk-sha",
        generation_verdict={
            "age_s": 12.0,
            "generation_mismatch_citable": True,
        },
    )

    assert row == {
        "day_utc": "2026-08-04",
        "fills": 9,
        "floor_gate_enforced_fill_count": 0,
        "floor_gate_enforced_fill_rate": 0.0,
        "floor_gate_enforced_fill_rate_pct": 0.0,
        "ruled_floor_breach_count": 4,
        "ruled_floor_breach_cost_usd": 4.759996,
        "running_submitter_floor_gate_generation_sha256": "resident-sha",
        "disk_floor_gate_generation_sha256": "disk-sha",
        "generation_match": False,
        "generation_evidence_status": "MISMATCH",
        "generation_verdict_age_s": 12.0,
        "generation_mismatch_citable": True,
        "measurement_only": True,
        "live_mutation": False,
    }


def test_daily_floor_gate_residency_generation_evidence_is_tri_state():
    metric = {"fills": 1, "floor_gate_enforced_fill_count": 1}

    matching = _daily_floor_gate_residency(
        "2026-08-05",
        metric,
        loaded_generation_sha256="same",
        disk_generation_sha256="same",
        generation_verdict={"generation_mismatch_citable": True, "age_s": 1.0},
    )
    stale = _daily_floor_gate_residency(
        "2026-08-05",
        metric,
        loaded_generation_sha256="resident",
        disk_generation_sha256="disk",
        generation_verdict={"generation_mismatch_citable": False, "age_s": 601.0},
    )
    missing = _daily_floor_gate_residency(
        "2026-08-05",
        metric,
        loaded_generation_sha256=None,
        disk_generation_sha256="disk",
        generation_verdict={"generation_mismatch_citable": True, "age_s": 1.0},
    )

    assert matching["generation_evidence_status"] == "MATCH"
    assert stale["generation_evidence_status"] == "UNKNOWN_STALE_OR_MISSING"
    assert missing["generation_evidence_status"] == "UNKNOWN_STALE_OR_MISSING"
