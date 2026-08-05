import argparse
import json
from pathlib import Path

from scripts import benchmark_self_feed_duckdb_scan as bench


def test_jsonl_self_feed_summary_groups_txs_and_cost(tmp_path: Path) -> None:
    log = tmp_path / "self.jsonl"
    log.write_text(
        "\n".join(
            [
                json.dumps({"tx": "0xA", "cost_usd": 1.25, "event_ts": 100.0}),
                json.dumps({"tx": "0xa", "cost_usd": 2.25, "event_ts": 110.0}),
                json.dumps({"tx": "0xB", "cost_usd": 3.5, "event_ts": 90.0}),
                "not-json",
            ]
        )
        + "\n"
    )

    summary = bench._jsonl_self_feed_summary(log)

    assert summary == {
        "rows": 3,
        "tx_groups": 2,
        "cost_usd": 7.0,
        "min_event_ts": 90.0,
        "max_event_ts": 110.0,
    }


def test_self_feed_duckdb_benchmark_reports_parity(tmp_path: Path, monkeypatch) -> None:
    log = tmp_path / "self.jsonl"
    log.write_text(json.dumps({"tx": "0xA", "cost_usd": 1.25, "event_ts": 100.0}) + "\n")
    self_feed_report = tmp_path / "self_feed.json"
    classification = tmp_path / "classification.json"
    full_retrace = tmp_path / "full_retrace.json"
    self_feed_report.write_text(
        json.dumps(
            {
                "summary": {
                    "data_api_trade_rows": 1,
                    "self_feed_tx_groups": 1,
                    "self_feed_missing_ledger_critical": 1,
                    "self_feed_missing_ledger_cost_usd": 1.25,
                    "self_feed_missing_ledger_payout_usd": 2.0,
                    "self_feed_missing_ledger_pnl_usd": 0.75,
                    "self_feed_missing_ledger_resolved_tx_groups": 1,
                    "self_feed_missing_ledger_unresolved_tx_groups": 0,
                    "amount_mismatch_tx_groups": 0,
                    "probable_split_fill_groups": 0,
                    "price_rounding_mismatch_tx_groups": 0,
                },
                "self_feed_missing_ledger_pnl": {
                    "resolved_tx_groups": 1,
                    "unresolved_tx_groups": 0,
                    "cost_usd": 1.25,
                    "payout_usd": 2.0,
                    "pnl_usd": 0.75,
                },
            }
        )
    )
    classification.write_text(
        json.dumps(
            {
                "summary": {
                    "self_feed_missing_ledger_rows": 1,
                    "join_key_defect_probable_split_fill": 0,
                    "true_unrecorded_fill_candidate": 1,
                    "cost_by_class_usd": {"true_unrecorded_fill_candidate": 1.25},
                    "pnl_by_class_usd": {"true_unrecorded_fill_candidate": 0.75},
                }
            }
        )
    )
    full_retrace.write_text(
        json.dumps(
            {
                "summary": {
                    "candidate_total": 1,
                    "class_counts": {"b3_duplicate_full_ledger_match": 1},
                    "cost_by_class_usd": {"b3_duplicate_full_ledger_match": 1.25},
                    "pnl_by_class_usd": {"b3_duplicate_full_ledger_match": 0.75},
                    "b1_confirmed_count": 0,
                    "b2_suspect_count": 0,
                    "immediate_notify_required": False,
                },
                "backfill_gate": {"allowed": False},
                "reconciliation_equation": {"actual_delta_usd": -0.5},
                "rows": [
                    {
                        "tx": "0xA",
                        "classification": "b3_duplicate_full_ledger_match",
                        "cost_usd": 1.25,
                        "pnl_usd": 0.75,
                        "nearest_full_ledger_fill": {"cost_usd": 1.25},
                    }
                ],
            }
        )
    )

    def fake_duckdb_summary(_duckdb_path: Path, _source_file: str) -> dict:
        return {
            "rows": 1,
            "tx_groups": 1,
            "cost_usd": 1.25,
            "min_event_ts": 100.0,
            "max_event_ts": 100.0,
        }

    def fake_gap_summary(
        _duckdb_path: Path,
        _source_file: str,
        _self_feed_report: Path,
        _live_state: Path,
    ) -> dict:
        return {
            "status": "PASS",
            "parity": {"self_feed_missing_ledger_critical": True, "self_feed_missing_ledger_cost_usd": True},
            "expected_summary": {"self_feed_missing_ledger_critical": 1},
            "duckdb_summary": {"self_feed_missing_ledger_critical": 1},
        }

    monkeypatch.setattr(bench, "_duckdb_self_feed_summary", fake_duckdb_summary)
    monkeypatch.setattr(bench, "_duckdb_gap_summary", fake_gap_summary)
    report = bench.build_report(
        argparse.Namespace(
            self_feed_log=str(log),
            duckdb_path=str(tmp_path / "wallet_copy.duckdb"),
            self_feed_report=str(self_feed_report),
            live_state=str(tmp_path / "live.json"),
            classification=str(classification),
            full_retrace=str(full_retrace),
            output=str(tmp_path / "out.json"),
        )
    )

    assert report["status"] == "PASS"
    assert all(report["parity"].values())
    assert all(report["gap_scan"]["parity"].values())
    assert report["jsonl_summary"] == report["duckdb_summary"]
    assert report["classification_packet"]["recommendation"]["mode"] == "RECONCILIATION_OVERLAY"
    overlay = report["classification_packet"]["resolved_pnl_overlay"]
    assert overlay["raw_missing_pnl_upper_bound_usd"] == 0.75
    assert overlay["overlay_delta_usd"] == 0.0
    assert overlay["double_count_excluded_usd"] == 0.75
    assert overlay["reconciled_actual_estimate_usd"] == -0.5
    assert overlay["class_decomposition"]["b3_duplicate_full_ledger_match"]["overlay_delta_usd"] == 0.0


def test_duckdb_gap_summary_respects_ledger_missing_grace(tmp_path: Path, monkeypatch) -> None:
    report = tmp_path / "self_feed.json"
    live = tmp_path / "live.json"
    report.write_text(
        json.dumps(
            {
                "window": {"start_ts": 900.0, "end_ts": 1100.0},
                "summary": {
                    "ledger_filled_tx_groups": 1,
                    "matched_ledger_tx_groups": 0,
                    "self_feed_tx_groups": 0,
                    "data_api_trade_rows": 0,
                    "amount_mismatch_tx_groups": 0,
                    "price_rounding_mismatch_tx_groups": 0,
                    "probable_split_fill_groups": 0,
                    "probable_split_fill_missing_tx_groups": 0,
                    "ledger_missing_self_feed_critical": 0,
                    "ledger_missing_self_feed_within_grace": 1,
                    "self_feed_missing_ledger_critical": 0,
                    "self_feed_missing_ledger_cost_usd": 0.0,
                    "ledger_missing_grace_s": 300.0,
                },
            }
        )
    )
    live.write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "final_status": "FILLED",
                        "submitted_at": "1970-01-01T00:16:40Z",
                        "price": 0.5,
                        "shares": 2.0,
                        "trade_result": {"tx_hashes": ["0xfresh"], "side": "BUY"},
                    }
                ]
            }
        )
    )
    monkeypatch.setattr(bench, "_self_feed_rows_from_duckdb", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(bench.time, "time", lambda: 1010.0)

    summary = bench._duckdb_gap_summary(tmp_path / "wallet_copy.duckdb", "self.jsonl", report, live)

    assert summary["status"] == "PASS"
    assert summary["duckdb_summary"]["ledger_missing_self_feed_critical"] == 0
    assert summary["duckdb_summary"]["ledger_missing_self_feed_within_grace"] == 1


def test_dedup_aware_overlay_uses_only_per_group_residual() -> None:
    overlay = bench._dedup_aware_overlay(
        raw_missing_pnl_usd=2.5,
        classification_summary={"pnl_by_class_usd": {"join_key_defect_probable_split_fill": 0.25}},
        full_retrace={
            "rows": [
                {
                    "tx": "0xdup",
                    "classification": "b3_duplicate_full_ledger_match",
                    "cost_usd": 1.0,
                    "pnl_usd": 1.0,
                    "nearest_full_ledger_fill": {"cost_usd": 1.0},
                },
                {
                    "tx": "0xnear",
                    "classification": "b3_join_scope_artifact_size_price_or_time_mismatch",
                    "cost_usd": 1.0,
                    "pnl_usd": 1.5,
                    "nearest_full_ledger_fill": {"cost_usd": 1.2},
                },
            ]
        },
    )

    assert overlay["overlay_delta_usd"] == 0.2
    assert overlay["double_count_excluded_usd"] == 2.3
    assert overlay["class_decomposition"]["b3_duplicate_full_ledger_match"]["overlay_delta_usd"] == 0.0
    assert overlay["class_decomposition"]["b3_join_scope_artifact_size_price_or_time_mismatch"]["overlay_delta_usd"] == 0.2
    assert overlay["class_decomposition"]["join_key_defect_probable_split_fill"]["overlay_delta_usd"] == 0.0
