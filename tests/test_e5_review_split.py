from __future__ import annotations

import json
import sys
from pathlib import Path

from scripts import report_e5_review_split
from scripts.report_e5_review_split import build_report
from src.wallet_copy.store import atomic_write_json


def test_e5_review_split_counts_only_non_fallback_book_evidence(tmp_path: Path):
    paper_state = tmp_path / "paper.json"
    book_state = tmp_path / "book.json"
    atomic_write_json(
        paper_state,
        {
            "summary": {"resolved_paper_fills": 2, "resolved_paper_pnl_usd": 9.0},
            "orders": [
                {
                    "order_id": "fallback",
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "hash-1",
                            "route_report": {"fallback_source": "direct_clob_after_primary_failure"},
                        }
                    },
                },
                {
                    "order_id": "genuine",
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "hash-2",
                            "route_report": {"route_class": "DIRECT_PASS"},
                        }
                    },
                },
            ],
        },
    )
    atomic_write_json(
        book_state,
        {
            "summary": {"resolved_paper_fills": 2, "resolved_paper_pnl_usd": 9.0},
            "scored_orders": [
                {"order_id": "fallback", "resolved": True, "win": True, "cost_usd": 1.0, "payout_usd": 10.0, "pnl_usd": 9.0},
                {"order_id": "genuine", "resolved": True, "win": False, "cost_usd": 1.0, "payout_usd": 0.0, "pnl_usd": -1.0},
            ],
        },
    )

    report = build_report(paper_state_path=paper_state, book_aware_state_path=book_state)

    assert report["aggregate_summary"]["resolved_paper_pnl_usd"] == 9.0
    assert report["book_aware_full_summary"]["resolved_paper_pnl_usd"] == 9.0
    assert report["fallback_reason_histogram"] == {"direct_clob_after_primary_failure": 1}
    non_fallback = report["book_aware_non_fallback_summary"]
    assert non_fallback["resolved_paper_fills"] == 1
    assert non_fallback["resolved_paper_pnl_usd"] == -1.0
    assert non_fallback["non_fallback_book_evidence_positive"] is False


def test_e5_review_split_counts_fallback_reasons_outside_scored_tail(tmp_path: Path):
    paper_state = tmp_path / "paper.json"
    book_state = tmp_path / "book.json"
    atomic_write_json(
        paper_state,
        {
            "orders": [
                {
                    "order_id": "fallback-not-scored",
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "hash-fallback",
                            "route_report": {"fallback_source": "direct_clob_after_primary_failure"},
                        }
                    },
                },
                {
                    "order_id": "genuine",
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "hash-genuine",
                            "route_report": {"route_class": "DIRECT_PASS"},
                        }
                    },
                },
            ]
        },
    )
    atomic_write_json(
        book_state,
        {
            "scored_orders": [
                {"order_id": "genuine", "resolved": True, "win": True, "cost_usd": 1.0, "payout_usd": 2.0, "pnl_usd": 1.0},
            ]
        },
    )

    report = build_report(paper_state_path=paper_state, book_aware_state_path=book_state)

    assert report["fallback_reason_histogram"] == {"direct_clob_after_primary_failure": 1}
    assert report["book_aware_non_fallback_summary"]["resolved_paper_fills"] == 1


def test_e5_review_split_reuses_existing_snapshot_by_default(tmp_path: Path):
    paper_state = tmp_path / "paper.json"
    book_state = tmp_path / "book.json"
    snapshot_dir = tmp_path / "snapshot"
    atomic_write_json(
        paper_state,
        {
            "summary": {"resolved_paper_fills": 1, "resolved_paper_pnl_usd": 1.0},
            "orders": [
                {
                    "order_id": "first",
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "hash-first",
                            "route_report": {"route_class": "DIRECT_PASS"},
                        }
                    },
                },
            ],
        },
    )
    atomic_write_json(
        book_state,
        {
            "summary": {"resolved_paper_fills": 1, "resolved_paper_pnl_usd": 1.0},
            "scored_orders": [
                {"order_id": "first", "resolved": True, "win": True, "cost_usd": 1.0, "payout_usd": 2.0, "pnl_usd": 1.0},
            ],
        },
    )

    first = build_report(paper_state_path=paper_state, book_aware_state_path=book_state, snapshot_dir=snapshot_dir)
    atomic_write_json(
        paper_state,
        {
            "summary": {"resolved_paper_fills": 1, "resolved_paper_pnl_usd": -1.0},
            "orders": [
                {
                    "order_id": "second",
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "hash-second",
                            "route_report": {"route_class": "DIRECT_PASS"},
                        }
                    },
                },
            ],
        },
    )
    atomic_write_json(
        book_state,
        {
            "summary": {"resolved_paper_fills": 1, "resolved_paper_pnl_usd": -1.0},
            "scored_orders": [
                {"order_id": "second", "resolved": True, "win": False, "cost_usd": 1.0, "payout_usd": 0.0, "pnl_usd": -1.0},
            ],
        },
    )

    second = build_report(paper_state_path=paper_state, book_aware_state_path=book_state, snapshot_dir=snapshot_dir)
    refreshed = build_report(
        paper_state_path=paper_state,
        book_aware_state_path=book_state,
        snapshot_dir=snapshot_dir,
        refresh_snapshot=True,
    )

    assert first["snapshot_paths"]["reused_existing"] is False
    assert second["snapshot_paths"]["reused_existing"] is True
    assert second["book_aware_non_fallback_summary"]["resolved_paper_pnl_usd"] == 1.0
    assert refreshed["snapshot_paths"]["refresh_snapshot"] is True
    assert refreshed["book_aware_non_fallback_summary"]["resolved_paper_pnl_usd"] == -1.0


def test_e5_review_split_main_refreshes_latest_pointer(monkeypatch, tmp_path: Path):
    paper_state = tmp_path / "paper.json"
    book_state = tmp_path / "book.json"
    out = tmp_path / "dated.json"
    latest = tmp_path / "latest.json"
    atomic_write_json(
        paper_state,
        {
            "summary": {"resolved_paper_fills": 1, "resolved_paper_pnl_usd": 1.0},
            "orders": [
                {
                    "order_id": "genuine",
                    "maker_quote": {
                        "top_of_book": {
                            "status": "OK",
                            "book_hash": "hash",
                            "route_report": {"route_class": "DIRECT_PASS"},
                        }
                    },
                },
            ],
        },
    )
    atomic_write_json(
        book_state,
        {
            "summary": {"resolved_paper_fills": 1, "resolved_paper_pnl_usd": 1.0},
            "scored_orders": [
                {"order_id": "genuine", "resolved": True, "win": True, "cost_usd": 1.0, "payout_usd": 2.0, "pnl_usd": 1.0},
            ],
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "report_e5_review_split.py",
            "--paper-state",
            str(paper_state),
            "--book-aware-state",
            str(book_state),
            "--out",
            str(out),
            "--latest-out",
            str(latest),
        ],
    )

    report_e5_review_split.main()

    assert json.loads(out.read_text())["kind"] == "e5_review_split"
    assert json.loads(latest.read_text())["book_aware_non_fallback_summary"]["resolved_paper_pnl_usd"] == 1.0
