import json
from pathlib import Path

import pytest

from scripts.report_wide_fingerprint_durability import build_report


WALLETS = [
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "0xcccccccccccccccccccccccccccccccccccccccc",
]


def _manifest(
    path: Path,
    *,
    generated_at: str,
    resolved_a: int,
    resolved_b: int,
) -> Path:
    path.write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "capture_watch_wallets": [
                    {
                        "wallet": WALLETS[0],
                        "wide_policy_fingerprint": "fp-a",
                        "move_slice_keys": ["one", "two"],
                        "slice_freeze": {"f1": {"resolved": resolved_a}},
                    },
                    {
                        "wallet": WALLETS[1],
                        "wide_policy_fingerprint": "fp-b",
                        "move_slice_keys": ["one"],
                        "slice_freeze": {"f1": {"resolved": resolved_b}},
                    },
                    {
                        "wallet": WALLETS[2],
                        "wide_policy_fingerprint": "fp-c",
                        "move_slice_keys": ["one"],
                        "slice_freeze": {"f1": {"resolved": 50}},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_durability_evidenced_span_and_time_to_200(tmp_path: Path) -> None:
    manifests = [
        _manifest(
            tmp_path / "m1.json",
            generated_at="2026-07-29T00:00:00Z",
            resolved_a=100,
            resolved_b=20,
        ),
        _manifest(
            tmp_path / "m2.json",
            generated_at="2026-07-30T00:00:00Z",
            resolved_a=220,
            resolved_b=30,
        ),
    ]
    frontier = {
        "frontier_checksum": "frontier",
        "nearest_frontier": [
            {
                "wallet": wallet,
                "regime_evidence": {"resolved_signals": resolved},
            }
            for wallet, resolved in zip(WALLETS, [220, 50, 30], strict=True)
        ],
    }

    report = build_report(
        frontier=frontier,
        manifest_paths=manifests,
        expected_frontier_checksum="frontier",
    )

    row_a = next(row for row in report["rows"] if row["wallet"] == WALLETS[0])
    assert row_a["first_evidenced_at"] == "2026-07-29T00:00:00Z"
    assert row_a["evidenced_span_hours"] == 24.0
    assert row_a["observation_count"] == 2
    assert row_a["PRE_MATURE_AT_FIRST_OBSERVATION"] is False
    assert row_a["observed_crossing_from_below"] is True
    assert row_a["ever_reached_200"] is True
    assert row_a[
        "hours_from_explicit_first_evidence_to_first_200_observation"
    ] == 24.0
    assert row_a["move_slice_key_count"] == 2
    assert report["summary"]["fingerprints_reached_200"] == 1
    assert report["summary"][
        "median_lifetime_hours_reached_200"
    ] == 24.0
    assert report["summary"][
        "median_lifetime_hours_reached_200_n"
    ] == 1
    assert "move_slice_key_count_vs_max_resolved_pearson_r" not in (
        report["summary"]
    )
    assert report["quality_bars_unchanged"][
        "f1_resolved_signal_bar_per_half"
    ] == 200
    assert report["quality_bars_unchanged"][
        "composite_walk_forward_min_total_resolved"
    ] == 400


def test_durability_rejects_frontier_checksum_drift(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="frontier checksum changed"):
        build_report(
            frontier={"frontier_checksum": "new"},
            manifest_paths=[],
            expected_frontier_checksum="old",
        )


def test_durability_rejects_slice_changes_within_fingerprint(
    tmp_path: Path,
) -> None:
    manifests = [
        _manifest(
            tmp_path / "m1.json",
            generated_at="2026-07-29T00:00:00Z",
            resolved_a=100,
            resolved_b=20,
        ),
        _manifest(
            tmp_path / "m2.json",
            generated_at="2026-07-30T00:00:00Z",
            resolved_a=220,
            resolved_b=30,
        ),
    ]
    changed = json.loads(manifests[1].read_text(encoding="utf-8"))
    changed["capture_watch_wallets"][0]["move_slice_keys"] = ["changed"]
    manifests[1].write_text(json.dumps(changed), encoding="utf-8")
    frontier = {
        "frontier_checksum": "frontier",
        "nearest_frontier": [
            {
                "wallet": wallet,
                "regime_evidence": {"resolved_signals": resolved},
            }
            for wallet, resolved in zip(WALLETS, [220, 50, 30], strict=True)
        ],
    }

    with pytest.raises(ValueError, match="move-slice keys changed"):
        build_report(
            frontier=frontier,
            manifest_paths=manifests,
            expected_frontier_checksum="frontier",
        )


def test_durability_recovers_keyed_rows_and_excludes_policy_absent_placeholders(
    tmp_path: Path,
) -> None:
    manifests = [
        _manifest(
            tmp_path / "m1.json",
            generated_at="2026-07-29T00:00:00Z",
            resolved_a=220,
            resolved_b=20,
        ),
        _manifest(
            tmp_path / "m2.json",
            generated_at="2026-07-30T00:00:00Z",
            resolved_a=240,
            resolved_b=30,
        ),
    ]
    for manifest in manifests:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        payload["kind"] = "wide_exact_policy_frozen_manifest"
        for row in payload["capture_watch_wallets"]:
            row.pop("wide_policy_fingerprint")
        payload["capture_watch_wallets"].append(
            {
                "wallet": WALLETS[0],
                "move_slice_keys": [],
                "policy_absent": True,
            }
        )
        manifest.write_text(json.dumps(payload), encoding="utf-8")
    frontier = {
        "frontier_checksum": "frontier",
        "nearest_frontier": [
            {
                "wallet": wallet,
                "regime_evidence": {"resolved_signals": resolved},
            }
            for wallet, resolved in zip(WALLETS, [240, 50, 30], strict=True)
        ],
    }

    report = build_report(
        frontier=frontier,
        manifest_paths=manifests,
        expected_frontier_checksum="frontier",
    )

    assert len(report["rows"]) == 3
    assert all(
        row["birth_observation_source"] == "RECOVERED_FROM_KEYS"
        for row in report["rows"]
    )
    assert report["summary"]["observed_crossings_from_below"] == 0
    assert report["observation_frame"]["blank_recoverability"] == (
        "PARTIAL_RECOVERY_KEYED_ROWS_ONLY_POLICY_ABSENT_EXCLUDED"
    )
    assert report["observation_frame"]["recovered_from_keys"] == 6
    assert report["observation_frame"]["policy_absent_placeholder"] == 2
    assert report["observation_frame"][
        "synthetic_empty_key_identity_count"
    ] == 1
    assert report["observation_frame"]["excluded_rows"] == [
        {
            "wallet": WALLETS[0],
            "classification": "SYNTHETIC_EMPTY_KEY_IDENTITY",
            "observation_count": 2,
        }
    ]
    assert report["summary"]["pre_mature_at_first_observation_count"] == 0
    assert report["summary"]["analysis_rows_excluding_pre_mature"] == 3
