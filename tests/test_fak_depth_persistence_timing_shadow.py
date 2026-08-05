from scripts.capture_clob_book_snapshots import _snapshot_row_from_book
from scripts.launch_fak_depth_persistence_timing_shadow_service import build_launchd_payload
from scripts.report_fak_depth_persistence_timing_shadow import F418, POLICY, build_report


def test_snapshot_retains_sorted_l2_depth() -> None:
    row, available = _snapshot_row_from_book(
        "token", {"bids": [{"price": "0.3", "size": "2"}],
                  "asks": [{"price": "0.4", "size": "1"}, {"price": "0.35", "size": "3"}]},
        captured_at_s=100,
    )
    assert available
    assert row["asks"] == [{"price": 0.35, "size": 3.0}, {"price": 0.4, "size": 1.0}]


def test_delayed_depth_shadow_is_paper_only_and_scores_fixed_bins() -> None:
    event = {"event": "wallet_copy_live_order", "final_status": "REJECTED", "intent_id": "i1",
             "source_wallet": F418, "policy_id": POLICY, "market_slug": "m", "condition_id": "c",
             "outcome": "up", "lifecycle": [{"status": "LIVE_REJECTED", "ts": 100.0,
             "payload": {"error_class": "fak_no_match", "market_id": "t", "order_size": 2,
             "entry_price": 0.4, "size_usd": 0.8}}]}
    books = [{"event_type": "best_bid_ask", "asset_id": "t", "captured_at_s": 100.0,
              "asks": [{"price": 0.4, "size": 1}]},
             {"event_type": "best_bid_ask", "asset_id": "t", "captured_at_s": 100.25,
              "asks": [{"price": 0.4, "size": 2}]}]
    report = build_report(event_rows=[event], book_rows=books,
                          resolution_rows=[{"condition_id": "c", "winning_outcome": "up"}],
                          generated_at="x", activation_utc="1970-01-01T00:00:00Z", min_resolved_opportunities=1)
    assert report["delay_bins_ms"] == [0, 100, 250, 500, 1000, 1500]
    assert report["metrics"]["immediate_executable"] == 0
    assert report["metrics"]["delayed_first_executable"] == 1
    assert report["paper_only"] is True and report["live_mutation"] is False
    assert report["parity"]["violations"] == 0


def test_launcher_has_no_live_guard_or_execute_live_argument() -> None:
    payload = build_launchd_payload(python="/usr/bin/python3", stdout="/tmp/out", stderr="/tmp/err")
    argv = payload["ProgramArguments"]
    assert "run_fak_depth_persistence_timing_shadow_service.py" in argv[1]
    assert all("live_guard" not in item and "execute-live" not in item for item in argv)
