from pathlib import Path

import pytest

from scripts.report_wide_alpha_metric_validity import (
    build_report,
    pearson,
    resolve_active_alpha_report,
    spearman,
    wilson_lower_bound,
)


def test_active_alpha_source_resolves_from_paper_only_manifest(tmp_path: Path):
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json"}'
    )
    (data / "manifest.json").write_text(
        '{"paper_only":true,"live_orders_allowed":false,'
        '"source_alpha_report":"data/research/alpha.json"}'
    )
    (data / "alpha.json").write_text(
        '{"paper_only":true,"live_orders_allowed":false}'
    )

    assert resolve_active_alpha_report(root=tmp_path) == "data/research/alpha.json"


def test_active_alpha_source_refuses_live_enabled_manifest(tmp_path: Path):
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json"}'
    )
    (data / "manifest.json").write_text(
        '{"paper_only":true,"live_orders_allowed":true,'
        '"source_alpha_report":"data/research/alpha.json"}'
    )

    with pytest.raises(RuntimeError, match="ACTIVE_WIDE_MANIFEST_NOT_PAPER_ONLY"):
        resolve_active_alpha_report(root=tmp_path)


def _alpha(rate=50.0):
    return {
        "updated_at": "2026-07-31T00:00:00Z",
        "execution_profiles": {
            "profiles_by_wallet": {
                "0xwallet": {
                    "copyable_rate_pct": rate,
                    "fill_sample": 20,
                    "raw_fill_coverage": 40,
                    "stale_or_missing_book_observations": 20,
                    "mean_edge": 0.1,
                    "median_edge": 0.05,
                    "eligible_move_slice_count": 1,
                    "eligible": False,
                }
            }
        },
    }


def _temporal():
    return {
        "wallets": [
            {
                "wallet": "0xwallet",
                "regime_profiles": {
                    "weekday": {"roi_pct": 2.0, "pnl_usd": 10.0, "resolved_trades": 50}
                },
                "slice_labels": {"weekday": {"label": "PROVEN-POSITIVE"}},
            }
        ]
    }


def test_statistics_are_deterministic_and_tie_aware():
    assert pearson([1, 2, 3], [1, 2, 3]) == 1.0
    assert spearman([1, 1, 2], [1, 1, 3]) == 1.0
    assert wilson_lower_bound(38, 42) == 77.934894


def test_report_emits_both_denominators_and_holds_gate():
    report = build_report(
        alpha=_alpha(),
        alpha_path="alpha.json",
        alpha_sha256="a" * 64,
        temporal=_temporal(),
        temporal_path="temporal.json",
    )
    row = report["rows"][0]
    assert row["copyable_samples"] == 10
    assert row["copyable_rate_pct_censored"] == 50.0
    assert row["copyable_rate_pct_raw"] == 25.0
    assert row["book_censorship_pct"] == 50.0
    assert report["paper_only"] is True
    assert report["gate_mutated"] is False
    assert report["preregistered_falsifier"]["status"] == "AWAITING_SECOND_INDEPENDENT_CUT"


def test_stale_producer_is_distinct_from_honest_evidence_accrual():
    report = build_report(
        alpha=_alpha(),
        alpha_path="alpha.json",
        alpha_sha256="a" * 64,
        temporal=_temporal(),
        temporal_path="temporal.json",
        producer_heartbeat={"status": "BLOCKED_NO_CUT_PRODUCER"},
    )

    falsifier = report["preregistered_falsifier"]
    assert falsifier["status"] == "PRODUCER_NON_PASS_AT_CUT"
    assert falsifier["gate_disposition"] == "HOLD_UNCHANGED_RESTORE_CUT_PRODUCER"
    assert falsifier["gate_change_applied"] is False


def test_two_nonpositive_independent_cuts_precommit_demotion_without_mutation():
    synthetic_cut = {
        "cut_id": "first-qualified",
        "alpha_report": "alpha1.json",
        "weekday_joined_count": 12,
        "total_fill_sample": 1000,
        "correlations": {
            "copyable_rate_pct_censored_vs_weekday_roi_pct": {
                "n": 12,
                "pearson_r": -0.1,
                "spearman_rho": -0.1,
            },
            "copyable_rate_pct_raw_vs_weekday_roi_pct": {
                "n": 12,
                "pearson_r": -0.1,
                "spearman_rho": -0.1,
            },
        },
    }
    profiles = {}
    temporal_wallets = []
    for index in range(12):
        wallet = f"0x{index:040x}"
        profiles[wallet] = {
            "copyable_rate_pct": 40.0 + index,
            "fill_sample": 100,
            "raw_fill_coverage": 125,
            "stale_or_missing_book_observations": 25,
        }
        temporal_wallets.append(
            {
                "wallet": wallet,
                "regime_profiles": {
                    "weekday": {
                        "roi_pct": 12.0 - index,
                        "pnl_usd": 1.0,
                        "resolved_trades": 50,
                    }
                },
            }
        )
    second_alpha = {
        "updated_at": "2026-08-01T00:00:00Z",
        "execution_profiles": {"profiles_by_wallet": profiles},
    }
    second = build_report(
        alpha=second_alpha,
        alpha_path="alpha2.json",
        alpha_sha256="b" * 64,
        temporal={"wallets": temporal_wallets},
        temporal_path="temporal.json",
        prior={"independent_cuts": [synthetic_cut]},
    )
    assert second["preregistered_falsifier"]["status"] == "FALSIFIED_DEMOTE_TO_REPORTED_DIAGNOSTIC"
    assert second["preregistered_falsifier"]["gate_change_applied"] is False


def test_no_fill_sample_growth_cannot_manufacture_second_cut():
    profiles = {}
    temporal_wallets = []
    for index in range(12):
        wallet = f"0x{index:040x}"
        profiles[wallet] = {
            "copyable_rate_pct": 40.0 + index,
            "fill_sample": 100,
            "raw_fill_coverage": 125,
            "stale_or_missing_book_observations": 25,
        }
        temporal_wallets.append(
            {
                "wallet": wallet,
                "regime_profiles": {"weekday": {"roi_pct": 12.0 - index}},
            }
        )
    alpha = {
        "updated_at": "2026-08-01T00:00:00Z",
        "execution_profiles": {"profiles_by_wallet": profiles},
    }
    first = build_report(
        alpha=alpha,
        alpha_path="alpha1.json",
        alpha_sha256="a" * 64,
        temporal={"wallets": temporal_wallets},
        temporal_path="temporal.json",
    )
    alpha["updated_at"] = "2026-08-01T01:00:00Z"
    second = build_report(
        alpha=alpha,
        alpha_path="alpha2.json",
        alpha_sha256="b" * 64,
        temporal={"wallets": temporal_wallets},
        temporal_path="temporal.json",
        prior=first,
    )
    assert second["summary"]["independent_cut_count"] == 2
    assert second["summary"]["qualifying_independent_cut_count"] == 1
    assert second["independent_cuts"][-1]["qualification"][
        "strict_total_fill_sample_growth_pass"
    ] is False
    assert second["preregistered_falsifier"]["status"] == "AWAITING_SECOND_INDEPENDENT_CUT"


def test_growth_qualification_sorts_cuts_by_alpha_timestamp_not_input_order():
    correlations = {
        "copyable_rate_pct_censored_vs_weekday_roi_pct": {"pearson_r": 0.1},
        "copyable_rate_pct_raw_vs_weekday_roi_pct": {"pearson_r": 0.1},
    }
    later = {
        "cut_id": "later",
        "alpha_updated_at": "2026-08-01T01:00:00Z",
        "weekday_joined_count": 12,
        "total_fill_sample": 200,
        "correlations": correlations,
    }
    earlier = {
        "cut_id": "earlier",
        "alpha_updated_at": "2026-08-01T00:00:00Z",
        "weekday_joined_count": 12,
        "total_fill_sample": 100,
        "correlations": correlations,
    }

    report = build_report(
        alpha={
            "updated_at": "2026-08-01T02:00:00Z",
            "execution_profiles": {"profiles_by_wallet": {}},
        },
        alpha_path="alpha3.json",
        alpha_sha256="c" * 64,
        temporal={"wallets": []},
        temporal_path="temporal.json",
        prior={"independent_cuts": [later, earlier]},
    )

    assert [row["cut_id"] for row in report["independent_cuts"][:2]] == [
        "earlier",
        "later",
    ]
    assert report["independent_cuts"][0]["qualification"]["qualifies"] is True
    assert report["independent_cuts"][1]["qualification"]["qualifies"] is True
    assert report["independent_cuts"][1]["qualification"][
        "prior_qualifying_total_fill_sample"
    ] == 100


def test_non_pass_producer_disqualifies_cut_from_independent_evidence():
    profiles = {}
    temporal_wallets = []
    for index in range(12):
        wallet = f"0x{index:040x}"
        profiles[wallet] = {
            "copyable_rate_pct": 40.0 + index,
            "fill_sample": 100,
            "raw_fill_coverage": 125,
            "stale_or_missing_book_observations": 25,
        }
        temporal_wallets.append(
            {
                "wallet": wallet,
                "regime_profiles": {"weekday": {"roi_pct": 12.0 - index}},
            }
        )
    report = build_report(
        alpha={
            "updated_at": "2026-08-01T00:00:00Z",
            "execution_profiles": {"profiles_by_wallet": profiles},
        },
        alpha_path="alpha.json",
        alpha_sha256="c" * 64,
        temporal={"wallets": temporal_wallets},
        temporal_path="temporal.json",
        producer_heartbeat={"status": "PRODUCER_CRASH_RESTART"},
    )

    assert report["summary"]["qualifying_independent_cut_count"] == 0
    assert report["independent_cuts"][0]["qualification"]["qualifies"] is False
    assert report["independent_cuts"][0]["qualification"]["producer_status"] == "PRODUCER_CRASH_RESTART"
    assert report["preregistered_falsifier"]["status"] == "PRODUCER_NON_PASS_AT_CUT"
