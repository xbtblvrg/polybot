import pytest

from scripts.report_wide_frontier_deficit_partition import build_report


CHECKSUM = "checksum"
FRONTIER_CHECKSUM = "frontier-checksum"


def _candidate(wallet: str, deficits: list[str]) -> dict:
    checks = {"a": True, "b": True, "c": True}
    for deficit in deficits:
        checks[deficit] = False
    return {
        "wallet": wallet,
        "paper_policy_id": f"policy-{wallet}",
        "wide_policy_fingerprint": f"fingerprint-{wallet}",
        "eligible": not deficits,
        "checks": checks,
        "evidence_deficits": deficits,
    }


def _frontier() -> dict:
    rows = [
        _candidate("one", ["a"]),
        _candidate("two", ["a"]),
        _candidate("three", ["a", "b"]),
        _candidate("four", []),
    ]
    return {
        "generated_at": "2026-07-30T19:14:41Z",
        "source_checksum": CHECKSUM,
        "frontier_checksum": FRONTIER_CHECKSUM,
        "frontier_key": "wallet|wide_policy_fingerprint|source_generation",
        "candidate_count": len(rows),
        "eligible_count": 1,
        "nearest_frontier": rows,
        "refusal_counts": {"a": 3, "b": 1, "c": 0},
        "quality_bars": {"f1_min_resolved_signals": 200},
    }


def test_partitions_and_ranks_only_single_check_flips() -> None:
    report = build_report(
        _frontier(),
        expected_source_checksum=CHECKSUM,
        expected_frontier_checksum=FRONTIER_CHECKSUM,
    )

    assert report["partition"] == {
        "zero_deficit_rows": 1,
        "exactly_one_deficit_rows": 2,
        "multi_deficit_rows": 1,
        "row_count_reconciles": True,
    }
    assert report["single_check_flip_ranking"] == [
        {"check": "a", "rows_flipped_if_check_alone_cleared": 2}
    ]
    assert report["recommendation"] is None
    assert report["admission_authority"] is False
    assert report["source"]["frontier_checksum"] == FRONTIER_CHECKSUM
    assert report["check_outcomes"]["a"] == {
        "explicit_false": 3,
        "not_evaluated": 0,
        "not_passed": 3,
        "not_evaluated_rows": [],
    }


def test_rejects_source_checksum_drift() -> None:
    with pytest.raises(ValueError, match="source checksum changed"):
        build_report(
            _frontier(),
            expected_source_checksum="different",
            expected_frontier_checksum=FRONTIER_CHECKSUM,
        )


def test_rejects_frontier_checksum_drift() -> None:
    with pytest.raises(ValueError, match="frontier checksum changed"):
        build_report(
            _frontier(),
            expected_source_checksum=CHECKSUM,
            expected_frontier_checksum="different",
        )


def test_rejects_published_deficit_mismatch() -> None:
    frontier = _frontier()
    frontier["nearest_frontier"][0]["evidence_deficits"] = ["b"]

    with pytest.raises(ValueError, match="explicit-false/evidence-deficit"):
        build_report(
            frontier,
            expected_source_checksum=CHECKSUM,
            expected_frontier_checksum=FRONTIER_CHECKSUM,
        )


def test_missing_check_is_named_as_not_evaluated_and_not_passed() -> None:
    frontier = _frontier()
    row = frontier["nearest_frontier"][3]
    del row["checks"]["c"]
    frontier["refusal_counts"]["c"] = 1

    report = build_report(
        frontier,
        expected_source_checksum=CHECKSUM,
        expected_frontier_checksum=FRONTIER_CHECKSUM,
    )

    assert report["rows"][3]["explicit_false_set"] == []
    assert report["rows"][3]["not_evaluated_set"] == ["c"]
    assert report["rows"][3]["not_passed_set"] == ["c"]
    assert report["check_outcomes"]["c"]["not_evaluated_rows"][0]["wallet"] == "four"
