from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from scripts.build_direction_leaderboard import build_leaderboard


def _write(root: Path, name: str, payload: dict) -> None:
    path = root / "data/research" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_direction_leaderboard_ranks_measured_projection_and_carries_gate_digits(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "wide_policy_fingerprint_evidence_latest.json",
        {
            "cells": [
                {
                    "evidence_authority": "venue_executable_full_stream_rescore",
                    "venue_executable_full_stream_rescore": {
                        "f1_pass": True,
                        "resolved": 100,
                        "post_fee_pnl_usd": 10,
                    }
                }
            ]
        },
    )
    _write(
        tmp_path,
        "band_scoped_admission_latest.json",
        {
            "status": "PASS",
            "members": [
                {
                    "band_scoped_admission": {
                        "admitted_bands": [
                            {
                                "evidence_authority": "venue_executable_full_stream_rescore",
                                "venue_executable_full_stream_rescore": {
                                    "f1_pass": True,
                                    "resolved": 100,
                                    "post_fee_pnl_usd": 5,
                                }
                            }
                        ]
                    }
                }
            ],
        },
    )
    _write(
        tmp_path,
        "btc5m_structural_scalp_paper_lane_state.json",
        {"summary": {"study_ev_per_day_usd": -1, "forward_fills": 300, "forward_pnl_usd": -5}},
    )
    _write(tmp_path, "btc5m_multivenue_ttl_passive_residual_selector.json", {"cells": [], "status": "NO_GATE_COMPLETE_CELL"})
    _write(tmp_path, "wide_direct_admissible_frontier_latest.json", {"eligible_count": 0, "candidate_count": 24})

    result = build_leaderboard(
        tmp_path,
        now=dt.datetime(2026, 7, 28, 20, tzinfo=dt.timezone.utc),
    )

    assert [row["direction_id"] for row in result["rows"][:2]] == [
        "t2_exact_cell_portfolio",
        "t1_band_scoped_seats",
    ]
    assert result["rows"][0]["measured_expected_usd_per_day"] == 10.0
    multivenue = next(row for row in result["rows"] if row["direction_id"] == "multivenue_passive_residual")
    assert multivenue["evidence"]["gate_digits"]["executable_fill_rate_gte_pct"] == 60
    scalp = next(row for row in result["rows"] if row["direction_id"] == "structural_scalp_new_identity")
    assert scalp["evidence"]["unlock"]["new_identity"] is True
