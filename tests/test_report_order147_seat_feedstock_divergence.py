from scripts.report_order147_seat_feedstock_divergence import (
    build_report,
    guard_source_snapshot,
    three_window_feedstock,
)


def _polygon_row(wallet: str, tx: str, log_index: int, source: str) -> dict:
    return {
        "event": "polygon_orderfilled_log",
        "source": source,
        "maker": wallet,
        "taker": "0x" + "3" * 40,
        "transaction_hash": tx,
        "log_index": log_index,
        "event_ts": 1_800_000_001.0,
        "decoded": {"maker_side": "BUY", "price": 0.4, "size": 2.0},
    }


def test_order147_fires_e1_when_guard_history_retains_accumulator_identity() -> None:
    wallet = "0x" + "2" * 40
    tx = "0xabc"
    guard = {
        "guard_code_identity": {
            "pid": 10,
            "started_at_utc": "2026-08-02T00:00:00Z",
            "live_guard_generation_sha256": "hash",
        },
        "guard_runtime_filter": {"rtds_jsonl": "rtds.jsonl", "rtds_offset_state": "cursor.json"},
        "pipeline": {
            "stdout_json": {
                "source_wallet": wallet,
                "previous_offset": 8,
                "next_offset": 9,
                "rtds_rows": 3,
                "polygon_ws_premerge": {
                    "path": "polygon.jsonl",
                    "profile": {
                        "line_count": 4,
                        "tail_open_seek_read": {"start_offset": 10, "end_offset": 20, "file_size": 20},
                    },
                },
            }
        },
    }
    accumulator = {"rows": [_polygon_row(wallet, tx, 7, "polygon_ws"), _polygon_row(wallet, tx, 7, "polygon_http_getLogs_tail")]}
    history = {
        "events": [{
            "source_wallet": wallet,
            "event_ts": 1_800_000_001.0,
            "transaction_hash": tx,
            "source": "polygon_orderfilled_ws_premerge",
            "raw": {"log_index": 7, "observation_sources": ["polygon_orderfilled_ws_premerge"]},
        }]
    }
    report = build_report(
        guard=guard,
        accumulator=accumulator,
        history=history,
        accumulator_path="accumulator.json",
    )
    assert report["status"] == "E1_GUARD_READER_ALIVE_DOWNSTREAM_PREDICATE_DEFECT"
    assert report["pre_registered_branch"] == "E1'''''''"
    assert report["paper_accumulator"]["seated_wallet_buy_rows_in_generation"] == 2
    assert report["paper_accumulator"]["unique_chain_identities"] == 1
    assert report["paper_accumulator"]["guard_read_unique_identities"] == 1
    assert report["paper_accumulator"]["identities"][0]["disposition"] == "READ_RETAINED_IN_GUARD_HOT_HISTORY"


def test_order147_matches_rtds_merge_by_transaction_hash() -> None:
    wallet = "0x" + "2" * 40
    tx = "0xdef"
    guard = {
        "guard_code_identity": {"started_at_utc": "2026-08-02T00:00:00Z"},
        "pipeline": {"stdout_json": {"source_wallet": wallet}},
    }
    history = {"events": [{
        "source_wallet": wallet,
        "event_ts": 1_800_000_001.0,
        "transaction_hash": tx,
        "source": "rtds_activity",
        "raw": {"observation_sources": ["polygon_orderfilled_ws_premerge", "rtds_activity"]},
    }]}
    report = build_report(
        guard=guard,
        accumulator={"rows": [_polygon_row(wallet, tx, 9, "polygon_ws")]},
        history=history,
        accumulator_path="accumulator.json",
    )
    row = report["paper_accumulator"]["identities"][0]
    assert row["guard_read"] is True
    assert row["match_mode"] == "transaction_hash_after_cross_source_merge"


def test_guard_source_snapshot_uses_guard_fields_without_scanning() -> None:
    guard = {
        "guard_runtime_filter": {"rtds_jsonl": "missing-rtds.jsonl", "rtds_offset_state": "offset.json"},
        "pipeline": {"stdout_json": {
            "previous_offset": 11,
            "next_offset": 22,
            "rtds_rows": 5,
            "polygon_ws_premerge": {"path": "missing-polygon.jsonl", "profile": {
                "line_count": 7,
                "tail_open_seek_read": {"start_offset": 33, "end_offset": 44, "file_size": 55},
            }},
        }},
    }
    snapshot = guard_source_snapshot(guard)
    assert snapshot["rtds"]["cursor_next_offset"] == 22
    assert snapshot["rtds"]["latest_scan_row_count"] == 5
    assert snapshot["polygon_orderfilled_ws_premerge"]["cursor_previous_offset"] == 33
    assert snapshot["polygon_orderfilled_ws_premerge"]["guard_reported_file_size"] == 55


def test_three_window_feedstock_reports_policy_and_clob_minimum_geometry() -> None:
    wallet = "0x" + "2" * 40
    guard = {
        "generated_at": "2027-01-15T08:42:30Z",
        "active_set_runtime": {
            "selected_member": {"source_wallet": wallet, "max_price": 0.5, "max_order_usd": 1.6},
            "members": [{
                "source_wallet": wallet,
                "enabled": True,
                "policy_id": "all_prices",
                "max_price": 0.5,
                "max_order_usd": 1.0,
            }],
        },
    }
    history = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "market_slug": "btc-updown-5m-1800002400",
                "event_ts": 1800002410,
                "price": 0.28,
                "transaction_hash": "0xa",
                "source": "rtds_activity",
            },
            {
                "source_wallet": wallet,
                "action": "BUY",
                "market_slug": "btc-updown-5m-1800002400",
                "event_ts": 1800002411,
                "price": 0.40,
                "transaction_hash": "0xb",
                "source": "polymarket_data_api_poll",
            },
        ]
    }

    report = three_window_feedstock(guard, history)
    row = report["rows"][0]
    assert row["selected"] is True
    assert row["source_events"] == 2
    assert row["price_band_counts"]["[0.25,0.32)"] == 1
    assert row["price_band_counts"]["[0.32,0.50]"] == 1
    assert row["clob_min_notional_lte_current_cap"] == 1
    assert row["clob_min_notional_lte_d2_cap"] == 2
