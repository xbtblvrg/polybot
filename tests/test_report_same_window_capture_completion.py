from __future__ import annotations

import json
from pathlib import Path

from scripts.report_same_window_capture_completion import build_audit


def _write(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_completion_audit_excludes_rows_after_boundary(tmp_path: Path) -> None:
    _write(
        tmp_path / "polygon_orderfilled.jsonl",
        [
            {"event": "polygon_orderfilled_log", "event_ts": 100.0, "captured_at_s": 101.0, "registry_wallets": ["0xabc"], "decoded": {"asset": "a"}},
            {"event": "polygon_ws_connection_error", "event_ts": 101.0},
            {"event": "polygon_orderfilled_log", "event_ts": 9000.0, "captured_at_s": 9000.0, "decoded": {"asset": "late"}},
            {"event": "polygon_orderfilled_log", "event_ts": 99.0, "captured_at_s": 9001.0, "decoded": {"asset": "backfill"}},
        ],
    )
    _write(
        tmp_path / "clob_books.jsonl",
        [
            {"event_type": "best_bid_ask", "captured_at_s": 100.0, "asset_id": "a"},
            {"event_type": "best_bid_ask", "captured_at_s": 9000.0, "asset_id": "late"},
        ],
    )
    _write(
        tmp_path / "dataapi_wallet_events.jsonl",
        [
            {"event": "wallet_copy_wallet_event", "observed_ts": 100.0, "source_wallet": "0xabc", "token_id": "a"},
            {"event": "wallet_copy_wallet_event", "observed_ts": 9000.0, "source_wallet": "0xdef", "token_id": "late"},
        ],
    )

    report = build_audit(tmp_path, boundary_s=8000.0)

    assert report["sources"]["polygon"]["rows"] == 1
    assert report["sources"]["polygon"]["rows_after_boundary_excluded"] == 2
    assert report["sources"]["clob"]["distinct_assets"] == 1
    assert report["sources"]["dataapi"]["wallets"] == ["0xabc"]
