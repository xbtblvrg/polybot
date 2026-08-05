import argparse
import json
from pathlib import Path

from scripts import run_freeze_resolution_accelerator as accelerator
from scripts.refresh_btc_5m_resolutions_from_gamma import _profit_state_slugs

TEST_IDENTITY = (
    "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82",
    "d277e7fcd160ead7a5cf014f7d516d5dbec64aa74a0c32ed52c074d0c3a3d7f4",
)


def test_direction_priority_targets_bf337_after_sample_and_supply_accrual():
    assert accelerator.DIRECTION_ID == "2026-08-01T09:12:00Z"
    assert accelerator.DIRECTION_DIRECT_CLIMB_PRIORITY == (
        (
            "0xbf337426aa856996b8bb79b238345dd1a0276bf7",
            "4028560ff42ee6da51e715295b2e78eeacceb04d9c538cd2df4eb5f2b98a5c46",
        ),
    )
    assert accelerator.DIRECTION_HOLD_NO_ACTIVE_CLIMB is False
    assert (
        accelerator.FRESH_FORWARD_CLOCK["status"]
        == "FROZEN_SOURCE_DROUGHT_3_OF_3"
    )
    assert accelerator.FRESH_FORWARD_CLOCK["sticky_refuse"] is False
    assert accelerator.FRESH_FORWARD_CLOCK["baseline_full_window"]["resolved"] == 0
    assert accelerator.FRESH_FORWARD_CLOCK["predecessor_kill"]["sticky_refuse"] is True
    assert (
        accelerator.FRESH_FORWARD_CLOCK["predecessor_kill"]["post_fee_pnl_usd"] < 0
    )
    drought = accelerator.FRESH_FORWARD_CLOCK["source_drought_policy"]
    assert drought["direction_id"] == "2026-07-28T10:05:09Z"
    assert drought["cycle_s"] == 1800
    assert drought["freeze_after_consecutive_cycles"] == 3
    assert drought["stale_pipeline_counts_as_drought"] is False
    assert drought["consequence"] == "FREEZE_PRESERVE_DIGITS_NO_STICKY_REFUSE"
    assert drought["mechanical_rearm_within_s"] == 86400
    assert drought["live_source_no_fresh_resolution_stall_s"] == 21600
    closing = accelerator.FRESH_FORWARD_CLOCK["closing_fresh_forward"]
    assert closing["attempted_exact_policy_buys"] == 0
    assert closing["input_equals_terminal"] is True
    assert len(closing["drought_boundaries"]) == 3


def test_profit_state_prioritizes_unresolved_exact_policy_orders(tmp_path: Path):
    state = tmp_path / "wide.json"
    state.write_text(
        json.dumps(
            {
                "orders": [
                    {"market_slug": "btc-updown-5m-100", "resolved": False},
                    {"market_slug": "btc-updown-5m-200", "resolved": True},
                    {"market_slug": "eth-updown-5m-300", "resolved": False},
                ]
            }
        )
    )

    assert _profit_state_slugs([str(state)]) == ["btc-updown-5m-100"]


def test_accelerator_is_paper_only_and_targets_wide_state(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(accelerator, "DIRECTION_HOLD_NO_ACTIVE_CLIMB", False)
    paper = tmp_path / "wide.json"
    paper.write_text(
        json.dumps(
            {
                "orders": [
                    {"market_slug": "btc-updown-5m-100", "resolved": False},
                    {"market_slug": "btc-updown-5m-100", "resolved": False},
                ]
            }
        )
    )
    resolutions = tmp_path / "resolutions.jsonl"
    resolutions.write_text('{"market_slug":"btc-updown-5m-0"}\n')
    sidecar = tmp_path / "sidecar.json"
    sidecar.write_text(
        json.dumps(
            {
                "primary": {
                    "wallet": "0x1015",
                    "wide_policy_fingerprint": "48d5",
                    "resolved": 142,
                    "resolved_target": 200,
                    "remaining_resolved": 58,
                }
            }
        )
    )
    output = tmp_path / "state.json"
    args = argparse.Namespace(
        paper_state=str(paper),
        resolutions=str(resolutions),
        output=str(output),
        summary_output=str(tmp_path / "summary.json"),
        sidecar=str(sidecar),
        frontier=str(tmp_path / "frontier.json"),
        fingerprint_evidence=str(tmp_path / "fingerprints.json"),
        interval_s=60.0,
        max_windows=250,
        max_wall_runtime_s=45.0,
        request_timeout_s=5.0,
    )
    calls = []

    def runner(command, **kwargs):
        calls.append((command, kwargs))
        return type("Result", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()

    payload = accelerator.run_once(args, runner=runner)

    command = calls[0][0]
    assert command[command.index("--profit-state") + 1] == str(paper)
    assert payload["status"] == "PASS"
    assert payload["paper_only"] is True
    assert payload["live_orders_allowed"] is False
    assert payload["live_mutation"] is False
    assert payload["priority_unresolved_window_count"] == 1
    assert payload["freeze_primary"]["resolved"] == 142
    assert json.loads(output.read_text())["single_submitter"].endswith(
        "run_wallet_copy_live_guard.py"
    )


def test_accelerator_puts_direct_climb_windows_at_manual_priority_head(
    tmp_path: Path,
):
    wallet, fingerprint = TEST_IDENTITY
    paper = tmp_path / "wide.json"
    paper.write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "market_slug": "btc-updown-5m-100",
                        "resolved": False,
                        "wallet": "0xother",
                        "wide_policy_fingerprint": "other",
                    },
                    {
                        "market_slug": "btc-updown-5m-200",
                        "resolved": False,
                        "wallet": wallet,
                        "wide_policy_fingerprint": fingerprint,
                    },
                ]
            }
        )
    )
    args = argparse.Namespace(
        paper_state=str(paper),
        resolutions=str(tmp_path / "resolutions.jsonl"),
        output=str(tmp_path / "state.json"),
        summary_output=str(tmp_path / "summary.json"),
        sidecar=str(tmp_path / "sidecar.json"),
        frontier=str(tmp_path / "frontier.json"),
        fingerprint_evidence=str(tmp_path / "fingerprints.json"),
        interval_s=0.0,
        max_windows=250,
        max_wall_runtime_s=45.0,
        request_timeout_s=5.0,
    )
    command = accelerator._refresh_command(
        args,
        direct_climb_priority_windows=accelerator._direct_climb_unresolved_windows(
            paper,
            direct_climb_priority=[(wallet, fingerprint)],
        ),
    )

    assert command[-2:] == ["--market-slug", "btc-updown-5m-200"]


def test_direct_climb_priority_keeps_direction_identity_when_frontier_absent(
    tmp_path: Path, monkeypatch,
):
    p1 = TEST_IDENTITY
    monkeypatch.setattr(accelerator, "DIRECTION_DIRECT_CLIMB_PRIORITY", (p1,))
    frontier = tmp_path / "frontier.json"
    frontier.write_text(json.dumps({"nearest_frontier": []}))

    assert accelerator._direct_climb_priority(frontier) == [p1]


def test_direct_climb_priority_keeps_paper_feedstock_when_live_frontier_refuses(
    tmp_path: Path, monkeypatch,
):
    p1 = TEST_IDENTITY
    monkeypatch.setattr(accelerator, "DIRECTION_DIRECT_CLIMB_PRIORITY", (p1,))
    frontier = tmp_path / "frontier.json"
    frontier.write_text(
        json.dumps(
            {
                "nearest_frontier": [
                    {
                        "wallet": p1[0],
                        "wide_policy_fingerprint": p1[1],
                        "evidence_deficits": [
                            "both_resolved_halves_positive",
                            "f1_measured_positive_regime_cell",
                        ],
                        "checks": {
                            "active_temporal_not_proven_negative": True,
                            "f3_not_enabled_or_cooloff_or_fading": True,
                            "f4_external_liveness": True,
                            "not_terminal_park_red_clock_or_measured_loser": False,
                        },
                    },
                ]
            }
        )
    )

    assert accelerator._direct_climb_priority(frontier) == [p1]


def test_history_backfill_uses_exact_f1_only_green_fingerprint_cell(
    tmp_path: Path,
):
    identity = TEST_IDENTITY
    frontier = tmp_path / "frontier.json"
    frontier.write_text(
        json.dumps(
            {
                "nearest_frontier": [
                    {
                        "wallet": identity[0],
                        "wide_policy_fingerprint": identity[1],
                        "evidence_deficits": [
                            "f1_measured_positive_regime_cell"
                        ],
                        "regime_evidence": {
                            "pnl_usd": 13.1,
                            "first_half_post_fee_pnl_usd": 2.7,
                            "second_half_post_fee_pnl_usd": 10.4,
                        },
                    }
                ]
            }
        )
    )
    evidence = tmp_path / "fingerprints.json"
    evidence.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "identity": {"wallet": identity[0]},
                        "wide_policy_fingerprint": identity[1],
                        "evidence_authority": "venue_executable_full_stream_rescore",
                        "venue_executable_full_stream_rescore": {
                            "f1_deficits": ["resolved_gte_200"],
                            "post_fee_pnl_usd": 13.1,
                            "first_half_post_fee_pnl_usd": 2.7,
                            "second_half_post_fee_pnl_usd": 10.4,
                        },
                        "resolution_evidence_summary": {
                            "matured_unresolved_windows": [
                                "btc-updown-5m-300",
                                "eth-updown-5m-400",
                                "btc-updown-5m-300",
                                "btc-updown-5m-200",
                            ]
                        },
                    }
                ]
            }
        )
    )

    windows, selected = accelerator._history_backfill_windows(
        frontier_path=frontier,
        fingerprint_evidence_path=evidence,
        direct_climb_priority=[identity],
    )

    assert selected == identity
    assert windows == ["btc-updown-5m-300", "btc-updown-5m-200"]


def test_accelerator_backfills_empty_paper_and_rebinds_completed_primary(
    tmp_path: Path, monkeypatch,
):
    identity = TEST_IDENTITY
    monkeypatch.setattr(
        accelerator, "DIRECTION_DIRECT_CLIMB_PRIORITY", (identity,)
    )
    paper = tmp_path / "wide.json"
    paper.write_text(json.dumps({"orders": []}))
    frontier = tmp_path / "frontier.json"
    frontier.write_text(
        json.dumps(
            {
                "nearest_frontier": [
                    {
                        "wallet": identity[0],
                        "wide_policy_fingerprint": identity[1],
                        "evidence_deficits": [
                            "f1_measured_positive_regime_cell"
                        ],
                        "regime_evidence": {
                            "resolved_signals": 40,
                            "pnl_usd": 13.1,
                            "first_half_post_fee_pnl_usd": 2.7,
                            "second_half_post_fee_pnl_usd": 10.4,
                        },
                        "checks": {
                            "active_temporal_not_proven_negative": True,
                            "f3_not_enabled_or_cooloff_or_fading": True,
                            "f4_external_liveness": True,
                            "not_terminal_park_red_clock_or_measured_loser": True,
                        },
                    }
                ]
            }
        )
    )
    fingerprints = tmp_path / "fingerprints.json"
    fingerprints.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "identity": {"wallet": identity[0]},
                        "wide_policy_fingerprint": identity[1],
                        "evidence_authority": "venue_executable_full_stream_rescore",
                        "venue_executable_full_stream_rescore": {
                            "f1_deficits": ["resolved_gte_200"],
                            "post_fee_pnl_usd": 13.1,
                            "first_half_post_fee_pnl_usd": 2.7,
                            "second_half_post_fee_pnl_usd": 10.4,
                        },
                        "resolution_evidence_summary": {
                            "matured_unresolved_windows": [
                                "btc-updown-5m-300"
                            ]
                        },
                    }
                ]
            }
        )
    )
    sidecar = tmp_path / "sidecar.json"
    sidecar.write_text(
        json.dumps(
            {
                "primary": {
                    "wallet": "0x424e",
                    "wide_policy_fingerprint": "d6d4",
                    "resolved": 342,
                    "resolved_target": 200,
                    "remaining_resolved": 0,
                }
            }
        )
    )
    args = argparse.Namespace(
        paper_state=str(paper),
        resolutions=str(tmp_path / "resolutions.jsonl"),
        output=str(tmp_path / "state.json"),
        summary_output=str(tmp_path / "summary.json"),
        sidecar=str(sidecar),
        frontier=str(frontier),
        fingerprint_evidence=str(fingerprints),
        interval_s=0.0,
        max_windows=250,
        max_wall_runtime_s=45.0,
        request_timeout_s=5.0,
    )
    calls = []

    def runner(command, **kwargs):
        calls.append(command)
        return type("Result", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()

    payload = accelerator.run_once(args, runner=runner)

    assert calls[0][-2:] == ["--market-slug", "btc-updown-5m-300"]
    assert payload["direct_climb_priority_window_source"] == (
        "fingerprint_full_stream_history_backfill"
    )
    assert payload["freeze_primary"] == {
        "wallet": identity[0],
        "wide_policy_fingerprint": identity[1],
        "resolved": 40,
        "resolved_target": 200,
        "remaining_resolved": 160,
    }


def test_history_backfill_allows_frontier_absence_and_ignores_other_fingerprint(
    tmp_path: Path,
):
    identity = TEST_IDENTITY
    frontier = tmp_path / "frontier.json"
    frontier.write_text(
        json.dumps(
            {
                "nearest_frontier": [
                    {
                        "wallet": identity[0],
                        "wide_policy_fingerprint": "different-negative-fp",
                        "evidence_deficits": [
                            "both_resolved_halves_positive",
                            "f1_measured_positive_regime_cell",
                        ],
                        "regime_evidence": {
                            "pnl_usd": -5.0,
                            "first_half_post_fee_pnl_usd": 1.0,
                            "second_half_post_fee_pnl_usd": -6.0,
                        },
                    }
                ]
            }
        )
    )
    evidence = tmp_path / "fingerprints.json"
    evidence.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "identity": {"wallet": identity[0]},
                        "wide_policy_fingerprint": identity[1],
                        "evidence_authority": "venue_executable_full_stream_rescore",
                        "venue_executable_full_stream_rescore": {
                            "f1_deficits": ["resolved_gte_200"],
                            "post_fee_pnl_usd": 13.1,
                            "first_half_post_fee_pnl_usd": 2.7,
                            "second_half_post_fee_pnl_usd": 10.4,
                        },
                        "resolution_evidence_summary": {
                            "matured_unresolved_windows": [
                                "btc-updown-5m-300"
                            ]
                        },
                    }
                ]
            }
        )
    )

    windows, selected = accelerator._history_backfill_windows(
        frontier_path=frontier,
        fingerprint_evidence_path=evidence,
        direct_climb_priority=[identity],
    )

    assert selected == identity
    assert windows == ["btc-updown-5m-300"]


def test_history_backfill_preserves_climb_identity_without_matured_windows(
    tmp_path: Path,
):
    identity = TEST_IDENTITY
    frontier = tmp_path / "frontier.json"
    frontier.write_text(json.dumps({"nearest_frontier": []}))
    evidence = tmp_path / "fingerprints.json"
    evidence.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "identity": {"wallet": identity[0]},
                        "wide_policy_fingerprint": identity[1],
                        "evidence_authority": "venue_executable_full_stream_rescore",
                        "venue_executable_full_stream_rescore": {
                            "f1_deficits": ["resolved_gte_200"],
                            "post_fee_pnl_usd": 13.1,
                            "first_half_post_fee_pnl_usd": 2.7,
                            "second_half_post_fee_pnl_usd": 10.4,
                        },
                        "resolution_evidence_summary": {
                            "matured_unresolved_windows": []
                        },
                    }
                ]
            }
        )
    )

    assert accelerator._history_backfill_windows(
        frontier_path=frontier,
        fingerprint_evidence_path=evidence,
        direct_climb_priority=[identity],
    ) == ([], identity)


def test_history_backfill_vetoes_exact_identity_negative_frontier(
    tmp_path: Path,
):
    identity = TEST_IDENTITY
    frontier = tmp_path / "frontier.json"
    frontier.write_text(
        json.dumps(
            {
                "nearest_frontier": [
                    {
                        "wallet": identity[0],
                        "wide_policy_fingerprint": identity[1],
                        "evidence_deficits": [
                            "both_resolved_halves_positive",
                            "f1_measured_positive_regime_cell",
                        ],
                        "regime_evidence": {
                            "pnl_usd": -1.0,
                            "first_half_post_fee_pnl_usd": 1.0,
                            "second_half_post_fee_pnl_usd": -2.0,
                        },
                    }
                ]
            }
        )
    )
    evidence = tmp_path / "fingerprints.json"
    evidence.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "identity": {"wallet": identity[0]},
                        "wide_policy_fingerprint": identity[1],
                        "evidence_authority": "venue_executable_full_stream_rescore",
                        "venue_executable_full_stream_rescore": {
                            "f1_deficits": ["resolved_gte_200"],
                            "post_fee_pnl_usd": 13.1,
                            "first_half_post_fee_pnl_usd": 2.7,
                            "second_half_post_fee_pnl_usd": 10.4,
                        },
                        "resolution_evidence_summary": {
                            "matured_unresolved_windows": [
                                "btc-updown-5m-300"
                            ]
                        },
                    }
                ]
            }
        )
    )

    assert accelerator._history_backfill_windows(
        frontier_path=frontier,
        fingerprint_evidence_path=evidence,
        direct_climb_priority=[identity],
    ) == ([], None)


def test_history_backfill_vetoes_exact_identity_negative_offline_rescore(
    tmp_path: Path,
):
    identity = TEST_IDENTITY
    frontier = tmp_path / "frontier.json"
    frontier.write_text(json.dumps({"nearest_frontier": []}))
    evidence = tmp_path / "fingerprints.json"
    evidence.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "identity": {"wallet": identity[0]},
                        "wide_policy_fingerprint": identity[1],
                        "evidence_authority": "venue_executable_full_stream_rescore",
                        "venue_executable_full_stream_rescore": {
                            "f1_deficits": [
                                "resolved_gte_200",
                                "post_fee_pnl_positive",
                            ],
                            "post_fee_pnl_usd": -1.0,
                            "first_half_post_fee_pnl_usd": 1.0,
                            "second_half_post_fee_pnl_usd": -2.0,
                        },
                        "resolution_evidence_summary": {
                            "matured_unresolved_windows": [
                                "btc-updown-5m-300"
                            ]
                        },
                    }
                ]
            }
        )
    )

    assert accelerator._history_backfill_windows(
        frontier_path=frontier,
        fingerprint_evidence_path=evidence,
        direct_climb_priority=[identity],
    ) == ([], None)


def test_offline_resolved_count_is_exact_identity_scoped(tmp_path: Path):
    identity = TEST_IDENTITY
    evidence = tmp_path / "fingerprints.json"
    evidence.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "identity": {"wallet": identity[0]},
                        "wide_policy_fingerprint": "different-fp",
                        "evidence_authority": "venue_executable_full_stream_rescore",
                        "venue_executable_full_stream_rescore": {"resolved": 999},
                    },
                    {
                        "identity": {"wallet": identity[0]},
                        "wide_policy_fingerprint": identity[1],
                        "evidence_authority": "venue_executable_full_stream_rescore",
                        "venue_executable_full_stream_rescore": {"resolved": 40},
                    },
                ]
            }
        )
    )

    assert accelerator._offline_resolved_count(evidence, identity) == 40
