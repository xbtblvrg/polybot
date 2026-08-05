import json
from argparse import Namespace
from pathlib import Path

from scripts import report_alpha_decay
from src.wallet_copy.alpha_decay import (
    ExecutionProfileConfig,
    build_alpha_decay_report,
    build_execution_profiles,
    clob_book_source_diagnostics,
    clob_market_points,
    polygon_fill_observations,
)
from scripts.capture_clob_book_snapshots import _active_asset_ids, _asset_ids_from_polygon_rows, _row_ts, _snapshot_row_from_book
from scripts.write_btc5m_clob_asset_ids import _market_tokens, _merge_asset_ids, _target_slugs


def test_alpha_decay_joins_fill_to_future_midpoint() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    fills = polygon_fill_observations(
        [
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0xabc",
                "block_ts": 100.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "asset-a", "side": "BUY", "price": 0.5, "size": 10},
            }
        ]
    )
    points = clob_market_points(
        [
            {"event_type": "best_bid_ask", "asset_id": "asset-a", "captured_at_s": 100.5, "best_bid": "0.50", "best_ask": "0.52"},
            {"event_type": "best_bid_ask", "asset_id": "asset-a", "captured_at_s": 102.0, "best_bid": "0.54", "best_ask": "0.56"},
        ]
    )

    report = build_alpha_decay_report(fills, points, horizons_s=(1.0, 2.0), sample_limit=10)

    assert report["status"] == "PASS"
    assert report["fills_total"] == 1
    assert report["coverage_by_horizon"]["1s"]["coverage"] == 1
    assert report["coverage_by_horizon"]["2s"]["edge"]["p50"] == 0.050000000000000044


def test_clob_snapshot_asset_refresh_rereads_asset_ids_file(tmp_path) -> None:
    asset_file = tmp_path / "asset_ids.json"
    asset_file.write_text('{"asset_ids": ["old-token"]}', encoding="utf-8")
    args = type(
        "Args",
        (),
        {
            "asset_id": [],
            "asset_ids_file": str(asset_file),
            "max_assets": 10,
            "polygon_jsonl": "",
            "polygon_scan_limit": 10,
            "polygon_max_age_s": 0.0,
            "polygon_registry_only": True,
            "polygon_source": [],
        },
    )()

    first, first_source = _active_asset_ids(args)
    asset_file.write_text('{"asset_ids": ["new-token"]}', encoding="utf-8")
    second, second_source = _active_asset_ids(args)

    assert first == ["old-token"]
    assert second == ["new-token"]
    assert first_source["explicit_assets"] == 1
    assert second_source["explicit_assets"] == 1


def test_btc5m_asset_sidecar_targets_current_and_next_window_tokens() -> None:
    assert _target_slugs(now_ts=1_700_000_123, windows_ahead=1) == [
        "btc-updown-5m-1699999800",
        "btc-updown-5m-1700000100",
    ]
    assert _market_tokens(
        {
            "markets": [
                {"slug": "btc-updown-5m-1699999800", "clobTokenIds": '["yes-token", "no-token"]'},
                {"slug": "btc-updown-5m-other", "clobTokenIds": '["ignored"]'},
            ]
        },
        "btc-updown-5m-1699999800",
    ) == ["yes-token", "no-token"]


def test_alpha_decay_can_opt_into_polygon_http_backfill_fills() -> None:
    wallet = "0x1212121212121212121212121212121212121212"
    http_row = {
        "event": "polygon_orderfilled_log",
        "source": "polygon_http_getLogs",
        "transaction_hash": "0xaaa",
        "block_ts": 100.0,
        "received_at_s": 130.0,
        "selected_wallet": wallet,
        "registry_wallets": [wallet],
        "decoded": {"asset": "asset-http", "side": "BUY", "price": 0.4, "size": 10},
    }
    ws_row = {
        **http_row,
        "source": "polygon_ws",
        "received_at_s": 101.0,
        "decoded": {"asset": "asset-http", "side": "BUY", "price": 0.41, "size": 10},
    }

    assert polygon_fill_observations([http_row]) == []

    fills = polygon_fill_observations([http_row, ws_row], sources=("polygon_ws", "polygon_http_getLogs"))

    assert len(fills) == 1
    assert fills[0].source == "polygon_ws"
    assert fills[0].price == 0.41


def test_report_alpha_decay_promotes_same_window_pass_for_default_stale_inputs(tmp_path: Path, monkeypatch) -> None:
    report_path = (
        tmp_path
        / "data/research/same_window_capture/20260719T162300Z/run1_final/alpha_decay_report.json"
    )
    report_path.parent.mkdir(parents=True)
    report_path.write_text(
        json.dumps(
            {
                "alpha_decay": {
                    "status": "PASS",
                    "missing_assets_top": [{"asset_id": "next-asset", "fills": 3}],
                },
                "execution_profiles": {
                    "eligible_profile_count": 5,
                    "profiles": [{"wallet": "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473"}],
                },
                "paper_only": True,
                "live_orders_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "alpha_decay_report.json"
    asset_ids = tmp_path / "alpha_decay_target_asset_ids.json"
    args = Namespace(
        polygon_jsonl=report_alpha_decay.DEFAULT_POLYGON_JSONL,
        report=str(output),
        asset_ids_output=str(asset_ids),
    )
    monkeypatch.setattr(report_alpha_decay, "ROOT", tmp_path)

    promoted = report_alpha_decay._promote_same_window_report_if_default_stale(
        args,
        [report_alpha_decay.DEFAULT_CLOB_JSONL],
    )

    assert promoted["promoted_from_same_window_report"].endswith("run1_final/alpha_decay_report.json")
    written = json.loads(output.read_text())
    assert written["alpha_decay"]["status"] == "PASS"
    assert written["execution_profiles"]["eligible_profile_count"] == 5
    assert written["status"] == "STALE_SOURCE_FAIL_CLOSED"
    assert written["promotion_grade"] is False
    assert written["source_freshness"]["pass"] is False
    targets = json.loads(asset_ids.read_text())
    assert targets["asset_ids"] == ["next-asset"]


def test_report_alpha_decay_does_not_promote_same_window_for_explicit_inputs(tmp_path: Path, monkeypatch) -> None:
    report_path = (
        tmp_path
        / "data/research/same_window_capture/20260719T162300Z/run1_final/alpha_decay_report.json"
    )
    report_path.parent.mkdir(parents=True)
    report_path.write_text(
        json.dumps(
            {
                "alpha_decay": {"status": "PASS"},
                "execution_profiles": {"eligible_profile_count": 5},
            }
        ),
        encoding="utf-8",
    )
    args = Namespace(
        polygon_jsonl="data/research/fresh_explicit_polygon.jsonl",
        report=str(tmp_path / "alpha_decay_report.json"),
        asset_ids_output=str(tmp_path / "alpha_decay_target_asset_ids.json"),
    )
    monkeypatch.setattr(report_alpha_decay, "ROOT", tmp_path)

    promoted = report_alpha_decay._promote_same_window_report_if_default_stale(
        args,
        ["data/research/fresh_explicit_clob.jsonl"],
    )

    assert promoted is None


def test_report_alpha_decay_explicit_current_history_never_promotes_stale_artifact(
    tmp_path: Path, monkeypatch
) -> None:
    capture = tmp_path / report_alpha_decay.SAME_WINDOW_CAPTURE_DIR / "capture" / "run1_final"
    capture.mkdir(parents=True)
    (capture / "alpha_decay_report.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-07-21T20:00:00Z",
                "status": "PASS_CURRENT_SOURCE",
                "promotion_grade": True,
                "history_state": "data/research/wallet_copy_history_state.json",
                "alpha_decay": {"status": "PASS"},
                "execution_profiles": {"eligible_profile_count": 1},
            }
        )
    )
    monkeypatch.setattr(report_alpha_decay, "ROOT", tmp_path)
    args = Namespace(
        polygon_jsonl=report_alpha_decay.DEFAULT_POLYGON_JSONL,
        history_state=report_alpha_decay.DEFAULT_HISTORY_STATE,
        history_state_explicit=True,
        report=str(tmp_path / "report.json"),
        asset_ids_output="",
    )

    promoted = report_alpha_decay._promote_same_window_report_if_default_stale(
        args,
        [report_alpha_decay.DEFAULT_CLOB_JSONL],
    )

    assert promoted is None
    assert not Path(args.report).exists()


def test_alpha_decay_sell_edge_flips_direction() -> None:
    wallet = "0x2222222222222222222222222222222222222222"
    fills = polygon_fill_observations(
        [
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0xdef",
                "block_ts": 100.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "asset-b", "side": "SELL", "price": 0.6},
            }
        ]
    )
    points = clob_market_points(
        [{"event_type": "best_bid_ask", "asset_id": "asset-b", "captured_at_s": 101.0, "best_bid": "0.54", "best_ask": "0.56"}]
    )

    report = build_alpha_decay_report(fills, points, horizons_s=(1.0,), sample_limit=10)

    assert report["coverage_by_horizon"]["1s"]["edge"]["p50"] == 0.04999999999999993
    assert report["coverage_by_horizon"]["1s"]["positive_edge_fraction"] == 1.0


def test_alpha_decay_reports_insufficient_book_coverage() -> None:
    wallet = "0x3333333333333333333333333333333333333333"
    fills = polygon_fill_observations(
        [
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0xbeef",
                "block_ts": 100.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "asset-c", "side": "BUY", "price": 0.4},
            }
        ]
    )

    report = build_alpha_decay_report(fills, {}, horizons_s=(1.0,), sample_limit=10)

    assert report["status"] == "INSUFFICIENT_BOOK_COVERAGE"
    assert "alpha_decay_book_source_empty" in report["blockers"]
    assert report["fills_with_any_book_coverage"] == 0
    assert report["missing_assets_top"] == [{"asset_id": "asset-c", "fills": 1}]


def test_alpha_decay_reports_fill_book_asset_overlap_missing() -> None:
    wallet = "0x4444444444444444444444444444444444444444"
    fills = polygon_fill_observations(
        [
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0xcafe",
                "block_ts": 100.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "fill-asset", "side": "BUY", "price": 0.4},
            }
        ]
    )
    points = clob_market_points(
        [{"event_type": "best_bid_ask", "asset_id": "book-asset", "captured_at_s": 101.0, "best_bid": "0.42", "best_ask": "0.44"}]
    )

    report = build_alpha_decay_report(fills, points, horizons_s=(1.0,), sample_limit=10)

    assert report["status"] == "INSUFFICIENT_BOOK_COVERAGE"
    assert report["overlapping_fill_book_assets"] == 0
    assert report["fills_on_book_assets"] == 0
    assert "alpha_decay_fill_book_asset_overlap_missing" in report["blockers"]
    assert report["next_action"] == "capture simultaneous wallet fills and CLOB books for the same active asset ids"


def test_alpha_decay_invalidates_non_overlapping_capture_windows() -> None:
    wallet = "0x4646464646464646464646464646464646464646"
    fills = polygon_fill_observations(
        [
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0x4646",
                "block_ts": 2_000.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "same-asset", "side": "BUY", "price": 0.4},
            }
        ]
    )
    points = clob_market_points(
        [
            {
                "event_type": "best_bid_ask",
                "asset_id": "same-asset",
                "captured_at_s": 101.0,
                "best_bid": "0.42",
                "best_ask": "0.44",
            }
        ]
    )

    report = build_alpha_decay_report(fills, points, horizons_s=(1.0,), sample_limit=10)

    assert report["status"] == "INVALID_CAPTURE_WINDOW_MISMATCH"
    assert report["overlapping_fill_book_assets"] == 1
    assert report["capture_windows"]["status"] == "INVALID_CAPTURE_WINDOW_MISMATCH"
    assert report["capture_windows"]["gap_s"] == 1899.0
    assert "alpha_decay_capture_windows_do_not_overlap" in report["blockers"]
    assert report["next_action"] == "rerun simultaneous wallet-fill and CLOB-book capture in the same wall-clock window"


def test_alpha_decay_reports_seed_only_fill_source_before_window_mismatch() -> None:
    wallet = "0x4747474747474747474747474747474747474747"
    fills = polygon_fill_observations(
        [
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_http_getLogs",
                "transaction_hash": "0xseed",
                "block_ts": 2_000.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "same-asset", "side": "BUY", "price": 0.4},
            }
        ],
        sources=("polygon_http_getLogs",),
    )
    points = clob_market_points(
        [
            {
                "event_type": "best_bid_ask",
                "asset_id": "same-asset",
                "captured_at_s": 101.0,
                "best_bid": "0.42",
                "best_ask": "0.44",
            }
        ]
    )

    report = build_alpha_decay_report(
        fills,
        points,
        horizons_s=(1.0,),
        sample_limit=10,
        fill_source_diagnostics={"ws_flap_cycle_count": 45},
    )

    assert report["status"] == "FILL_SOURCE_SEED_ONLY"
    assert report["tail_or_ws_fill_count"] == 0
    assert report["fill_source_diagnostics"]["ws_flap_cycle_count"] == 45
    assert "alpha_decay_fill_source_seed_only" in report["blockers"]
    assert "alpha_decay_capture_windows_do_not_overlap" not in report["blockers"]


def test_alpha_decay_reports_empty_book_truth_for_fill_assets() -> None:
    wallet = "0x5555555555555555555555555555555555555555"
    fills = polygon_fill_observations(
        [
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0x555",
                "block_ts": 100.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "empty-token", "side": "BUY", "price": 0.4},
            }
        ]
    )
    book_rows = [
        {
            "event_type": "clob_book_snapshot_unavailable",
            "asset_id": "empty-token",
            "empty_book_truth": True,
            "route_report": {"status": "HTTP_404_EMPTY_BOOK", "empty_book_truth": True},
        }
    ]

    report = build_alpha_decay_report(
        fills,
        {},
        horizons_s=(1.0,),
        sample_limit=10,
        book_source_diagnostics=clob_book_source_diagnostics(book_rows, fill_assets={fill.asset_id for fill in fills}),
    )

    assert "alpha_decay_fill_assets_empty_book_truth" in report["blockers"]
    assert report["book_source_diagnostics"]["overlapping_empty_book_truth_fill_assets"] == 1
    assert report["next_action"] == "filter empty-book assets from selection and keep simultaneous capture on liquid active assets"


def test_execution_profiles_require_positive_latency_edge() -> None:
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    report = {
        "status": "PASS",
        "fills_total": 4,
        "fills_with_any_book_coverage": 4,
        "per_wallet": {
            wallet_a: {
                "fills_with_any_coverage": 2,
                "horizons": {
                    "2s": {
                        "coverage": 2,
                        "positive_edge_fraction": 1.0,
                        "edge": {"count": 2, "mean": 0.02, "p50": 0.02},
                        "timely_coverage": 2,
                        "timely_positive_edge_fraction": 1.0,
                        "timely_edge": {"count": 2, "mean": 0.02, "p50": 0.02},
                    }
                },
            },
            wallet_b: {
                "fills_with_any_coverage": 2,
                "horizons": {
                    "2s": {
                        "coverage": 2,
                        "positive_edge_fraction": 0.5,
                        "edge": {"count": 2, "mean": -0.01, "p50": -0.01},
                        "timely_coverage": 2,
                        "timely_positive_edge_fraction": 0.5,
                        "timely_edge": {"count": 2, "mean": -0.01, "p50": -0.01},
                    }
                },
            },
        },
    }

    profiles = build_execution_profiles(
        report,
        config=ExecutionProfileConfig(latency_horizon_s=2.0, min_fills=2, min_positive_edge_fraction=0.7),
    )

    assert profiles["flow_stage"] == "LEARN"
    assert profiles["eligible_profile_count"] == 1
    assert profiles["profiles"][0]["wallet"] == wallet_a
    assert profiles["profiles_by_wallet"][wallet_a]["eligible"] is True
    assert profiles["profiles_by_wallet"][wallet_a]["copyable_rate_pct"] == 100.0
    assert profiles["profiles_by_wallet"][wallet_b]["eligible"] is False
    assert "execution_profile_copyable_rate_below_threshold" in profiles["profiles_by_wallet"][wallet_b]["blockers"]


def test_alpha_decay_builds_btc5m_move_slices_from_history_context() -> None:
    wallet = "0x7777777777777777777777777777777777777777"
    fills = polygon_fill_observations(
        [
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0x777",
                "block_ts": 1005.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "slice-token", "side": "BUY", "price": 0.2, "size": 5},
            }
        ],
        asset_context={
            "slice-token": {
                "market_slug": "btc-updown-5m-1000",
                "condition_id": "0xslice",
            }
        },
    )
    points = clob_market_points(
        [{"event_type": "best_bid_ask", "asset_id": "slice-token", "captured_at_s": 1007.0, "best_bid": "0.24", "best_ask": "0.26"}]
    )

    report = build_alpha_decay_report(fills, points, horizons_s=(2.0,), sample_limit=10)

    sample = report["sample_rows"][0]
    assert sample["market_slug"] == "btc-updown-5m-1000"
    assert sample["seconds_from_open"] == 5.0
    assert sample["move_slice_key"] == "000-060|<=0.25"
    slice_row = report["per_wallet"][wallet]["move_slices"]["2s"]["000-060|<=0.25"]
    assert slice_row["timely_coverage"] == 1
    assert slice_row["timely_positive_edge_fraction"] == 1.0


def test_execution_profiles_emit_eligible_move_slice_when_wallet_profile_is_not_eligible() -> None:
    wallet = "0x8888888888888888888888888888888888888888"
    fills = polygon_fill_observations(
        [
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0xpos1",
                "block_ts": 1010.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "pos-token-a", "side": "BUY", "price": 0.2, "size": 5},
            },
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0xpos2",
                "block_ts": 1020.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "pos-token-b", "side": "BUY", "price": 0.22, "size": 5},
            },
            {
                "event": "polygon_orderfilled_log",
                "source": "polygon_ws",
                "transaction_hash": "0xneg",
                "block_ts": 1250.0,
                "selected_wallet": wallet,
                "registry_wallets": [wallet],
                "decoded": {"asset": "neg-token", "side": "BUY", "price": 0.8, "size": 5},
            },
        ],
        asset_context={
            "pos-token-a": {"market_slug": "btc-updown-5m-1000", "condition_id": "0xpos"},
            "pos-token-b": {"market_slug": "btc-updown-5m-1000", "condition_id": "0xpos"},
            "neg-token": {"market_slug": "btc-updown-5m-1000", "condition_id": "0xneg"},
        },
    )
    points = clob_market_points(
        [
            {"event_type": "best_bid_ask", "asset_id": "pos-token-a", "captured_at_s": 1012.0, "best_bid": "0.24", "best_ask": "0.26"},
            {"event_type": "best_bid_ask", "asset_id": "pos-token-b", "captured_at_s": 1022.0, "best_bid": "0.25", "best_ask": "0.27"},
            {"event_type": "best_bid_ask", "asset_id": "neg-token", "captured_at_s": 1252.0, "best_bid": "0.70", "best_ask": "0.72"},
        ]
    )
    report = build_alpha_decay_report(fills, points, horizons_s=(2.0,), sample_limit=10)

    profiles = build_execution_profiles(
        report,
        config=ExecutionProfileConfig(latency_horizon_s=2.0, min_fills=2, min_positive_edge_fraction=0.7),
    )

    wallet_profile = profiles["profiles_by_wallet"][wallet]
    assert wallet_profile["eligible"] is False
    assert "execution_profile_copyable_rate_below_threshold" in wallet_profile["blockers"]
    assert wallet_profile["eligible_move_slice_count"] == 1
    best_slice = wallet_profile["best_eligible_move_slice"]
    assert best_slice["eligible"] is True
    assert best_slice["move_slice_key"] == "000-060|<=0.25"
    assert best_slice["copyable_rate_pct"] == 100.0


def test_alpha_decay_snapshot_assets_follow_recent_registry_polygon_rows() -> None:
    rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "received_at_s": 100.0,
            "selected_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "decoded": {"decode_status": "OK", "asset": "old-token"},
        },
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "received_at_s": 120.0,
            "selected_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "decoded": {"decode_status": "OK", "asset": "fresh-token"},
        },
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "received_at_s": 121.0,
            "decoded": {"decode_status": "OK", "asset": "sitewide-token"},
        },
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "received_at_s": 122.0,
            "selected_wallet": "0xcccccccccccccccccccccccccccccccccccccccc",
            "decoded": {"decode_status": "FAILED", "asset": "bad-token"},
        },
    ]

    asset_ids = _asset_ids_from_polygon_rows(
        rows,
        max_assets=10,
        now_s=125.0,
        max_age_s=10.0,
        registry_only=True,
    )

    assert asset_ids == ["fresh-token"]


def test_alpha_decay_snapshot_assets_can_use_recent_http_rows_by_block_time() -> None:
    rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_http_getLogs",
            "block_ts": 90.0,
            "received_at_s": 124.0,
            "selected_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "decoded": {"decode_status": "OK", "asset": "stale-http-token"},
        },
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_http_getLogs",
            "block_ts": 121.0,
            "received_at_s": 124.0,
            "selected_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "decoded": {"decode_status": "OK", "asset": "fresh-http-token"},
        },
    ]

    asset_ids = _asset_ids_from_polygon_rows(
        rows,
        max_assets=10,
        now_s=125.0,
        max_age_s=10.0,
        registry_only=True,
        sources=("polygon_ws", "polygon_http_getLogs"),
    )

    assert _row_ts(rows[0]) == 90.0
    assert asset_ids == ["fresh-http-token"]


def test_clob_snapshot_asset_refresh_rereads_asset_id_file(tmp_path) -> None:
    asset_file = tmp_path / "assets.json"
    asset_file.write_text(json.dumps({"asset_ids": ["asset-a"]}), encoding="utf-8")
    args = Namespace(
        asset_id=[],
        asset_ids_file=str(asset_file),
        max_assets=5,
        polygon_jsonl="",
        polygon_scan_limit=10,
        polygon_max_age_s=0.0,
        polygon_registry_only=True,
        polygon_source=[],
    )

    first, first_source = _active_asset_ids(args)
    asset_file.write_text(json.dumps({"asset_ids": ["asset-b"]}), encoding="utf-8")
    second, second_source = _active_asset_ids(args)

    assert first == ["asset-a"]
    assert first_source["explicit_assets"] == 1
    assert second == ["asset-b"]
    assert second_source["explicit_assets"] == 1


def test_btc5m_asset_sidecar_merges_polygon_seed_assets() -> None:
    assert _merge_asset_ids(["gamma-up", "gamma-down"], ["gamma-down", "seed-token"]) == [
        "gamma-up",
        "gamma-down",
        "seed-token",
    ]


def test_alpha_decay_snapshot_row_preserves_empty_book_truth() -> None:
    row, available = _snapshot_row_from_book(
        "token-empty",
        {
            "asset_id": "token-empty",
            "asks": [],
            "bids": [],
            "empty_book_truth": True,
            "__walletCopyClobRouteReport": {"status": "HTTP_404_EMPTY_BOOK", "empty_book_truth": True},
        },
        captured_at_s=100.0,
    )

    assert available is False
    assert row["event_type"] == "clob_book_snapshot_unavailable"
    assert row["empty_book_truth"] is True
    assert row["route_report"]["status"] == "HTTP_404_EMPTY_BOOK"


def test_alpha_decay_snapshot_row_emits_best_bid_ask_point() -> None:
    row, available = _snapshot_row_from_book(
        "token-live",
        {
            "asset_id": "token-live",
            "asks": [{"price": "0.52", "size": "10"}],
            "bids": [{"price": "0.50", "size": "10"}],
            "__walletCopyClobRouteReport": {"status": "PASS"},
        },
        captured_at_s=100.0,
    )

    assert available is True
    assert row["event_type"] == "best_bid_ask"
    assert row["best_bid"] == 0.5
    assert row["best_ask"] == 0.52
    assert row["source"] == "clob_rest_book_snapshot"


def test_execution_profiles_preserve_alpha_decay_blockers() -> None:
    profiles = build_execution_profiles(
        {
            "status": "INSUFFICIENT_BOOK_COVERAGE",
            "fills_total": 10,
            "fills_with_any_book_coverage": 0,
            "blockers": ["alpha_decay_fill_book_asset_overlap_missing"],
            "next_action": "capture simultaneous wallet fills and CLOB books for the same active asset ids",
            "per_wallet": {},
        },
        config=ExecutionProfileConfig(latency_horizon_s=2.0, min_fills=2),
    )

    assert "alpha_decay_fill_book_asset_overlap_missing" in profiles["blockers"]
    assert "alpha_decay_profile_coverage_missing" in profiles["blockers"]
    assert profiles["next_action"] == "capture simultaneous wallet fills and CLOB books for the same active asset ids"
def test_alpha_report_metadata_cache_repairs_missing_history_slug(tmp_path: Path) -> None:
    token_id = "123"
    cache = tmp_path / "tokens.json"
    cache.write_text(
        json.dumps({
            token_id: {
                "market_slug": "btc-updown-5m-1700000100",
                "condition_id": "0xcondition",
            }
        }),
        encoding="utf-8",
    )

    context = report_alpha_decay._asset_context_from_token_metadata(str(cache))

    assert context[token_id] == {
        "market_slug": "btc-updown-5m-1700000100",
        "condition_id": "0xcondition",
    }


def test_alpha_report_unknown_seconds_diagnostic_names_slug_join_seam() -> None:
    diagnostics = report_alpha_decay._move_slice_context_diagnostics([
        {"seconds_bucket": "unknown_seconds", "block_ts": 1.0, "market_slug": ""},
        {"seconds_bucket": "unknown_seconds", "block_ts": None, "market_slug": "other"},
        {"seconds_bucket": "000-060", "block_ts": 1.0, "market_slug": "btc-updown-5m-0"},
    ])

    assert diagnostics["unknown_seconds_rows"] == 2
    assert diagnostics["unknown_seconds_event_ts_absent"] == 1
    assert diagnostics["unknown_seconds_market_slug_absent"] == 1
    assert diagnostics["unknown_seconds_non_btc_slug"] == 1
    assert "token_id" in diagnostics["join_seam"]


def test_alpha_context_history_wins_on_collision() -> None:
    merged = report_alpha_decay._merge_asset_context(
        {"token": {"market_slug": "metadata", "condition_id": "m"}},
        {"token": {"market_slug": "history", "condition_id": "h"}},
    )

    assert merged["token"] == {"market_slug": "history", "condition_id": "h"}


def test_alpha_eligibility_delta_reports_wallet_drop_and_slice_add() -> None:
    prior = {
        "execution_profiles": {
            "profiles_by_wallet": {
                "0xold": {
                    "eligible": True,
                    "move_slices": [{"eligible": False, "move_slice_key": "old"}],
                }
            }
        }
    }
    current = {
        "profiles_by_wallet": {
            "0xnew": {
                "eligible": False,
                "move_slices": [{"eligible": True, "move_slice_key": "new"}],
            }
        }
    }

    delta = report_alpha_decay._eligibility_delta(prior, current)

    assert delta["eligible_profile_count"] == {"before": 1, "after": 0}
    assert delta["eligible_profile_wallet_drops"] == ["0xold"]
    assert delta["eligible_move_slice_count"] == {"before": 0, "after": 1}
    assert delta["eligible_move_slice_wallet_adds"] == ["0xnew"]
