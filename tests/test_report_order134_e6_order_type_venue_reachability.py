from scripts.report_order134_e6_order_type_venue_reachability import build_report


def _candidate(fingerprint, *, maker=False, other=True):
    return {
        "wallet": "0xwallet",
        "wide_policy_fingerprint": fingerprint,
        "eligible": False,
        "checks": {
            "f1_venue_reachable_admissible": maker,
            "other_gate": other,
        },
    }


def _cell(fingerprint, *, executable, maker_refused, missing=0):
    return {
        "wide_policy_fingerprint": fingerprint,
        "venue_executable_full_stream_rescore": {
            "f1_venue_reachable_admissible": executable >= maker_refused,
            "venue_reachable_share_pct": 100.0 * executable / max(
                1, executable + maker_refused + missing
            ),
            "venue_discard_reason_counts_resolved": {
                "executable": executable,
                "price_above_venue_minimum_max_price": maker_refused,
                "price_field_absent": missing,
            },
        },
    }


def test_measurement_flips_only_maker_ceiling_and_preserves_other_gates():
    deadman = {
        "checked_at": "t",
        "policy_choke": {
            "source_roster_drought": {
                "candidate_evidence": {
                    "frontier_checksum": "f",
                    "rows": [
                        _candidate("a", other=True),
                        _candidate("b", other=False),
                        _candidate("missing", other=True),
                    ],
                }
            }
        },
    }
    evidence = {
        "generated_at": "e",
        "cells": [
            _cell("a", executable=1, maker_refused=9),
            _cell("b", executable=1, maker_refused=9),
        ],
    }

    report = build_report(deadman=deadman, evidence=evidence)

    assert report["summary"] == {
        "candidate_count": 3,
        "exact_fingerprint_evidence_count": 2,
        "missing_exact_fingerprint_evidence_count": 1,
        "venue_reachable_maker_count": 0,
        "venue_reachable_taker_count": 2,
        "maker_to_taker_flip_count": 2,
        "maker_to_taker_flip_share_pct": 66.666667,
        "material_flip_min_count": 1,
        "material_flip": True,
        "eligible_before": 0,
        "eligible_after_taker_reachability_only": 1,
        "eligible_count_delta": 1,
    }
    assert report["rows"][2]["venue_reachable_taker"] is False
    assert report["rows"][2]["exact_fingerprint_evidence"] is False
    assert report["promotion_authority"] is False


def test_non_ceiling_discard_stays_fail_closed_for_taker():
    deadman = {
        "policy_choke": {
            "source_roster_drought": {
                "candidate_evidence": {"rows": [_candidate("a")]}
            }
        }
    }
    evidence = {"cells": [_cell("a", executable=1, maker_refused=0, missing=9)]}

    report = build_report(deadman=deadman, evidence=evidence)

    assert report["rows"][0]["taker_reachable_share_pct"] == 10.0
    assert report["rows"][0]["venue_reachable_taker"] is False
    assert report["summary"]["material_flip"] is False


def test_post_migration_report_uses_separate_maker_summary():
    deadman = {
        "policy_choke": {
            "source_roster_drought": {
                "candidate_evidence": {"rows": [_candidate("a")]}
            }
        }
    }
    maker = _cell("a", executable=1, maker_refused=9)[
        "venue_executable_full_stream_rescore"
    ]
    evidence = {
        "cells": [
            {
                "wide_policy_fingerprint": "a",
                "maker_venue_executable_full_stream_rescore": maker,
                "venue_executable_full_stream_rescore": {
                    **maker,
                    "venue_order_type": "taker",
                    "f1_venue_reachable_admissible": True,
                },
            }
        ]
    }

    report = build_report(deadman=deadman, evidence=evidence)

    assert report["rows"][0]["venue_reachable_maker"] is False
    assert report["rows"][0]["venue_reachable_taker"] is True
