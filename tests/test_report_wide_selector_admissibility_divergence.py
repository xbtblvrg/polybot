import json
from pathlib import Path

from scripts.report_wide_selector_admissibility_divergence import build_report


WALLET = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def _cell(fingerprint: str, *, admissible: bool, nkeys: int) -> dict:
    return {
        "wide_policy_fingerprint": fingerprint,
        "identity": {
            "wallet": WALLET,
            "move_slice_keys": [f"slice-{index}" for index in range(nkeys)],
        },
        "venue_executable_full_stream_rescore": {
            "f1_pass": admissible,
            "f1_walk_forward_admissible": admissible,
            "concentration_admissible": admissible,
            "f1_venue_reachable_admissible": admissible,
            "venue_reachable_share_pct": 50.0 if admissible else 20.0,
            "resolved": 500,
            "first_half": {"resolved": 250},
            "second_half": {"resolved": 250},
        },
    }


def test_reports_chronic_selector_miss(tmp_path: Path) -> None:
    manifests: list[Path] = []
    for index in range(2):
        path = tmp_path / f"manifest-{index}.json"
        path.write_text(
            json.dumps(
                {
                    "generated_at": f"2026-07-3{index}T00:00:00Z",
                    "capture_watch_wallets": [
                        {
                            "wallet": WALLET,
                            "wide_policy_fingerprint": "selected",
                            "move_slice_keys": ["selected-slice"],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        manifests.append(path)
    report = build_report(
        evidence={
            "cells": [
                _cell("admissible", admissible=True, nkeys=2),
                _cell("selected", admissible=False, nkeys=1),
            ]
        },
        evidence_checksum="checksum",
        manifest_paths=manifests,
    )

    assert report["summary"]["simultaneously_admissible_cells"] == 1
    assert report["summary"]["latest_selection_misses_admissible_cell"] == 1
    assert report["summary"][
        "historical_selections_of_currently_admissible_cell"
    ] == 0
    assert report["wallets"][0]["historical_match_rate_pct"] == 0.0
