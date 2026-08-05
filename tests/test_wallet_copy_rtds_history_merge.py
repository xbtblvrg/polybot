from __future__ import annotations

from argparse import Namespace

import json
from pathlib import Path
import sys

from scripts.merge_rtds_wallet_events import (
    _batch_line_plan,
    _iter_incremental_lines,
    _iter_recent_jsonl,
    _read_batch_lines,
    _wallet_event_from_rtds,
)
from scripts.run_wallet_copy_live_guard import _pipeline_command
from src.wallet_copy.profit_engine import load_events_from_history


def test_wallet_event_from_rtds_preserves_realtime_latency_and_token():
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    row = {
        "event": "rtds_trade_event",
        "source_wallet": wallet,
        "side": "BUY",
        "asset": "token-yes",
        "condition_id": "0xcondition",
        "market_slug": "btc-updown-5m-1783102500",
        "price": 0.65,
        "size": 2.25,
        "event_ts": 1783102531.0,
        "received_at_s": 1783102532.25,
        "transaction_hash": "0xabc",
        "raw": {"outcome": "Up", "outcomeIndex": 0, "title": "Bitcoin Up or Down"},
    }

    event = _wallet_event_from_rtds(row, source_wallet=wallet, wallet_name="unit")

    assert event is not None
    assert event.source == "rtds_activity"
    assert event.source_wallet == wallet
    assert event.action == "BUY"
    assert event.token_id == "token-yes"
    assert event.market_slug == "btc-updown-5m-1783102500"
    assert event.outcome == "Up"
    assert event.usdc_size == 1.4625
    assert event.api_latency_s == 1.25
    assert event.window_start_s == 1783102500


def test_live_guard_uses_rtds_history_refresh_when_configured():
    args = Namespace(
        rtds_jsonl="data/research/rtds.jsonl",
        rtds_scan_limit=123,
        rtds_max_new_events=7,
        rtds_tail_bytes=4096,
        history_state="data/research/history.json",
        wallet_event_log="data/research/events.jsonl",
    )

    argv = _pipeline_command(args, source_wallet="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

    assert argv[1] == "scripts/merge_rtds_wallet_events.py"
    assert "--rtds-jsonl" in argv
    assert "data/research/rtds.jsonl" in argv
    assert "--source-wallet" in argv
    assert "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" in argv
    assert "--scan-limit" in argv
    assert "123" in argv
    assert "--max-new-events" in argv
    assert "7" in argv
    assert "--tail-bytes" in argv
    assert "4096" in argv
    assert "--cold-tail-bytes" in argv
    assert argv[argv.index("--cold-tail-bytes") + 1] == "4096"
    assert "--offset-state" in argv
    assert argv[argv.index("--offset-state") + 1].endswith(".rtds_offset.json")


def test_iter_recent_jsonl_tail_seek_discards_partial_first_line(tmp_path):
    capture = tmp_path / "rtds.jsonl"
    rows = [
        {"event": "ignore", "idx": 0, "payload": "x" * 80},
        {"event": "rtds_trade_event", "idx": 1},
        {"event": "rtds_trade_event", "idx": 2},
    ]
    capture.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    parsed = _iter_recent_jsonl(str(capture), limit=10, tail_bytes=120)

    assert [row["idx"] for row in parsed] == [1, 2]


def test_incremental_rtds_reader_clamps_stale_matching_offset(tmp_path):
    capture = tmp_path / "rtds.jsonl"
    lines = [json.dumps({"event": "rtds_trade_event", "idx": idx, "payload": "x" * 80}) for idx in range(6)]
    capture.write_text("\n".join(lines) + "\n", encoding="utf-8")
    stat = capture.stat()
    offset_state = tmp_path / "offset.json"
    offset_state.write_text(
        json.dumps({"schema_version": 1, "path": str(capture), "inode": stat.st_ino, "offset": 0}),
        encoding="utf-8",
    )

    parsed_lines, ingest_state = _iter_incremental_lines(
        str(capture),
        offset_state=str(offset_state),
        tail_bytes=len(lines[-1]) + 8,
        cold_tail_bytes=10 * len(lines[-1]),
    )

    assert ingest_state["mode"] == "offset_gap_tail_clamp"
    assert ingest_state["previous_offset"] == 0
    assert ingest_state["premerge_substage_profile"]["tail_open_seek_read"]["start_offset"] > 0
    assert [json.loads(line)["idx"] for line in parsed_lines] == [5]
    assert json.loads(offset_state.read_text(encoding="utf-8"))["offset"] == stat.st_size


def test_multi_wallet_rtds_plan_clamps_stale_matching_offsets(tmp_path):
    capture = tmp_path / "rtds.jsonl"
    lines = [json.dumps({"event": "rtds_trade_event", "idx": idx, "payload": "x" * 80}) for idx in range(6)]
    capture.write_text("\n".join(lines) + "\n", encoding="utf-8")
    stat = capture.stat()
    wallets = [
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]
    offset_states = {}
    for wallet in wallets:
        offset_state = tmp_path / f"offset_{wallet[-4:]}.json"
        offset_state.write_text(
            json.dumps({"schema_version": 1, "path": str(capture), "inode": stat.st_ino, "offset": 0}),
            encoding="utf-8",
        )
        offset_states[wallet] = str(offset_state)

    plans, summary = _batch_line_plan(
        str(capture),
        wallets=wallets,
        offset_states=offset_states,
        tail_bytes=len(lines[-1]) + 8,
        cold_tail_bytes=10 * len(lines[-1]),
    )

    assert summary["min_start"] > 0
    assert {plan["mode"] for plan in plans.values()} == {"offset_gap_tail_clamp"}
    assert {plan["previous_offset"] for plan in plans.values()} == {0}
    assert {plan["fallback_tail_bytes"] for plan in plans.values()} == {len(lines[-1]) + 8}


def test_multi_wallet_batch_reader_hard_caps_bytes(tmp_path):
    capture = tmp_path / "rtds.jsonl"
    lines = [json.dumps({"event": "rtds_trade_event", "idx": idx, "payload": "x" * 80}) for idx in range(8)]
    capture.write_text("\n".join(lines) + "\n", encoding="utf-8")

    rows, profile = _read_batch_lines(str(capture), min_start=0, max_bytes=len(lines[0]))

    assert [json.loads(line)["idx"] for _offset, line in rows] == [0]
    assert profile["max_bytes"] == len(lines[0])
    assert profile["bytes_read"] <= len(lines[0])
    assert profile["end_offset"] < capture.stat().st_size


def test_rtds_merge_appends_only_new_history_events(monkeypatch, tmp_path, capsys):
    from scripts import merge_rtds_wallet_events as merge

    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    capture = tmp_path / "rtds.jsonl"
    history = tmp_path / "history.json"
    history_index = tmp_path / "history_window_index.json"
    event_log = tmp_path / "wallet_events.jsonl"
    offset_state = tmp_path / "offset.json"
    watermark_state = tmp_path / "watermarks.json"
    row = {
        "event": "rtds_trade_event",
        "source_wallet": wallet,
        "side": "BUY",
        "asset": "token-yes",
        "condition_id": "0xcondition",
        "market_slug": "btc-updown-5m-1783102500",
        "price": 0.65,
        "size": 2.25,
        "event_ts": 1783102531.0,
        "received_at_s": 1783102532.25,
        "transaction_hash": "0xabc",
        "raw": {"outcome": "Up", "outcomeIndex": 0, "title": "Bitcoin Up or Down"},
    }
    capture.write_text(json.dumps(row) + "\n", encoding="utf-8")

    argv = [
        "merge_rtds_wallet_events.py",
        "--rtds-jsonl",
        str(capture),
        "--source-wallet",
        wallet,
        "--wallet-name",
        "unit",
        "--history-state",
        str(history),
        "--history-window-index",
        str(history_index),
        "--wallet-event-log",
        str(event_log),
        "--scan-limit",
        "10",
        "--max-new-events",
        "10",
        "--tail-bytes",
        "4096",
        "--offset-state",
        str(offset_state),
        "--watermark-state",
        str(watermark_state),
    ]
    monkeypatch.setattr(merge.time, "time", lambda: 1783102535.25)
    monkeypatch.setattr(sys, "argv", argv)
    assert merge.main() == 0
    first_summary = json.loads(capsys.readouterr().out)
    assert first_summary["ingest_mode"] == "tail_fallback"
    assert first_summary["rtds_catchup_lag_s"] == 3.0
    assert first_summary["history_window_index"]["rebuilt"] is True
    assert first_summary["history_window_index"]["indexed_rows"] == 1
    assert history_index.exists()
    assert offset_state.exists()
    first_lines = event_log.read_text(encoding="utf-8").splitlines()
    assert len(first_lines) == 1

    capture.write_text(capture.read_text(encoding="utf-8") + json.dumps({**row, "transaction_hash": "0xdef"}) + "\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", argv)
    assert merge.main() == 0
    stdout = capsys.readouterr().out
    second_lines = event_log.read_text(encoding="utf-8").splitlines()

    assert len(second_lines) == 2
    summary = json.loads(stdout)
    assert summary["ingest_mode"] == "offset"
    assert summary["history_write_skipped"] is False
    assert summary["retained_matching_rows"] == 1
    assert summary["new_matching_events"] == 1
    assert summary["deduped_matching_events"] == 0
    payload = json.loads(history.read_text(encoding="utf-8"))
    assert payload["rtds_ingest"]["retained_matching_rows"] == 1
    assert payload["rtds_ingest"]["new_matching_events"] == 1
    assert payload["rtds_ingest"]["deduped_matching_events"] == 0
    assert payload["rtds_ingest"]["rtds_catchup_lag_s"] == 3.0
    scoped = load_events_from_history(
        history,
        source_wallet=wallet,
        window_starts={1783102500},
        history_window_index_path=history_index,
    )
    assert len(scoped) == 2
    assert {event.transaction_hash for event in scoped} == {"0xabc", "0xdef"}

    monkeypatch.setattr(sys, "argv", argv)
    assert merge.main() == 0
    idle_summary = json.loads(capsys.readouterr().out)
    assert idle_summary["ingest_mode"] == "offset"
    assert idle_summary["rtds_catchup_lag_s"] == 0.0
    assert idle_summary["history_write_skipped"] is True
    assert idle_summary["history_window_index"]["rebuilt"] is False
    assert idle_summary["rtds_rows"] == 0
    assert idle_summary["observation_watermark"]["source_wallet"] == wallet
    assert idle_summary["observation_watermark"]["latest_checked_ts"] > 0
    watermarks = json.loads(watermark_state.read_text(encoding="utf-8"))
    assert watermarks["wallets"][wallet]["history_write_skipped_safe"] is True
    assert event_log.read_text(encoding="utf-8").splitlines() == second_lines


def test_multi_wallet_rtds_merge_matches_sequential_outputs(monkeypatch, tmp_path):
    from scripts import merge_rtds_wallet_events as merge

    wallets = [
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "0xcccccccccccccccccccccccccccccccccccccccc",
    ]
    capture = tmp_path / "rtds.jsonl"
    base_rows = [
        {
            "event": "rtds_trade_event",
            "source_wallet": wallets[0],
            "side": "BUY",
            "asset": "token-yes-a",
            "condition_id": "0xcondition-a",
            "market_slug": "btc-updown-5m-1783102500",
            "price": 0.65,
            "size": 2.0,
            "event_ts": 1783102531.0,
            "received_at_s": 1783102532.0,
            "transaction_hash": "0xaaa",
            "raw": {"outcome": "Up", "outcomeIndex": 0, "title": "Bitcoin Up or Down"},
        },
        {
            "event": "rtds_trade_event",
            "source_wallet": wallets[1],
            "side": "BUY",
            "asset": "token-yes-b",
            "condition_id": "0xcondition-b",
            "market_slug": "btc-updown-5m-1783102800",
            "price": 0.45,
            "size": 3.0,
            "event_ts": 1783102831.0,
            "received_at_s": 1783102832.0,
            "transaction_hash": "0xbbb",
            "raw": {"outcome": "Down", "outcomeIndex": 1, "title": "Bitcoin Up or Down"},
        },
    ]
    capture.write_text("\n".join(json.dumps(row) for row in base_rows) + "\n", encoding="utf-8")

    def args_for(root, wallet=None):
        kwargs = {
            "rtds_jsonl": str(capture),
            "history_state": str(root / "history.json"),
            "history_window_index": str(root / "history_window_index.json"),
            "wallet_event_log": str(root / "wallet_events.jsonl"),
            "scan_limit": 10,
            "tail_bytes": 4096,
            "cold_tail_bytes": 4096,
            "watermark_state": str(root / "watermarks.json"),
            "max_new_events": 10,
            "history_retain_events": 250_000,
            "history_retain_copy_intents": 250_000,
        }
        if wallet is not None:
            kwargs.update(
                {
                    "source_wallet": wallet,
                    "wallet_name": f"unit_{wallet[-4:]}",
                    "offset_state": str(root / f"offset_{wallet[-4:]}.json"),
                }
            )
        return Namespace(**kwargs)

    monkeypatch.setattr(merge.time, "time", lambda: 1783102840.0)
    sequential_root = tmp_path / "sequential"
    batch_root = tmp_path / "batch"
    sequential_root.mkdir()
    batch_root.mkdir()

    sequential_summaries = {}
    for wallet in wallets:
        sequential_summaries[wallet] = merge.run_merge(args_for(sequential_root, wallet))

    batch_args = args_for(batch_root)
    batch_args.source_wallets = wallets
    batch_args.wallet_names = {wallet: f"unit_{wallet[-4:]}" for wallet in wallets}
    batch_args.offset_states = {wallet: str(batch_root / f"offset_{wallet[-4:]}.json") for wallet in wallets}
    batch_args.aggregate_offset_state = str(batch_root / "aggregate_offset.json")
    batch_summary = merge.run_multi_wallet_merge(batch_args)
    batch_summaries = batch_summary["wallet_summaries"]

    sequential_history = json.loads((sequential_root / "history.json").read_text(encoding="utf-8"))
    batch_history = json.loads((batch_root / "history.json").read_text(encoding="utf-8"))
    assert {
        (row["source_wallet"], row["transaction_hash"], row["event_id"])
        for row in sequential_history["events"]
    } == {
        (row["source_wallet"], row["transaction_hash"], row["event_id"])
        for row in batch_history["events"]
    }
    for wallet in wallets:
        assert batch_summaries[wallet]["new_matching_events"] == sequential_summaries[wallet]["new_matching_events"]
        assert batch_summaries[wallet]["retained_matching_rows"] == sequential_summaries[wallet]["retained_matching_rows"]
        assert batch_summaries[wallet]["latest_event_ts"] == sequential_summaries[wallet]["latest_event_ts"]
        assert batch_summaries[wallet]["latest_observed_ts"] == sequential_summaries[wallet]["latest_observed_ts"]
        assert Path(batch_summaries[wallet]["offset_state"]).exists()
    aggregate_offset = json.loads((batch_root / "aggregate_offset.json").read_text(encoding="utf-8"))
    assert aggregate_offset["mode"] == "multi_wallet_aggregate"
    assert aggregate_offset["offset"] == capture.stat().st_size
    assert sorted(aggregate_offset["wallets"]) == sorted(wallets)
    assert batch_summary["history_write_executed"] is True
    assert batch_summary["premerge_substage_profile"]["history_write"]["stage"] == "history_write"


def test_multi_wallet_rtds_merge_skips_non_dict_raw_rows(monkeypatch, tmp_path):
    from scripts import merge_rtds_wallet_events as merge

    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    capture = tmp_path / "rtds.jsonl"
    capture.write_text(
        "\n".join(
            json.dumps(row)
            for row in [
                {"event": "rtds_trade_event", "raw": "not-a-dict"},
                {
                    "event": "rtds_trade_event",
                    "source_wallet": wallet,
                    "side": "BUY",
                    "asset": "token-yes",
                    "condition_id": "0xcondition",
                    "market_slug": "btc-updown-5m-1783102500",
                    "price": 0.65,
                    "size": 2.0,
                    "event_ts": 1783102531.0,
                    "received_at_s": 1783102532.0,
                    "transaction_hash": "0xaaa",
                    "raw": {"outcome": "Up", "outcomeIndex": 0, "title": "Bitcoin Up or Down"},
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(merge.time, "time", lambda: 1783102840.0)
    args = Namespace(
        rtds_jsonl=str(capture),
        source_wallets=[wallet],
        wallet_names={wallet: "unit"},
        history_state=str(tmp_path / "history.json"),
        history_window_index=str(tmp_path / "history_window_index.json"),
        wallet_event_log=str(tmp_path / "wallet_events.jsonl"),
        scan_limit=10,
        tail_bytes=4096,
        cold_tail_bytes=4096,
        offset_states={wallet: str(tmp_path / "offset.json")},
        watermark_state=str(tmp_path / "watermarks.json"),
        max_new_events=10,
        history_retain_events=250_000,
        history_retain_copy_intents=250_000,
    )

    summary = merge.run_multi_wallet_merge(args)

    assert summary["wallet_summaries"][wallet]["new_matching_events"] == 1
    assert summary["premerge_substage_profile"]["line_json_parse"]["parsed_rows"] == 2


def test_multi_wallet_premerge_seeds_polygon_token_metadata_from_same_batch_rtds(monkeypatch, tmp_path):
    from scripts import merge_rtds_wallet_events as merge

    selected_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet = "0xcccccccccccccccccccccccccccccccccccccccc"
    token = "token-yes"
    capture = tmp_path / "rtds.jsonl"
    polygon = tmp_path / "polygon.jsonl"
    capture.write_text(
        json.dumps(
            {
                "event": "rtds_trade_event",
                "source_wallet": selected_wallet,
                "side": "BUY",
                "asset": token,
                "condition_id": "0xcondition",
                "market_slug": "btc-updown-5m-1783102500",
                "price": 0.65,
                "size": 2.0,
                "event_ts": 1783102531.0,
                "received_at_s": 1783102532.0,
                    "transaction_hash": "0xrtds-selected",
                "raw": {"outcome": "Up", "outcomeIndex": 0, "title": "Bitcoin Up or Down"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    polygon.write_text(
        json.dumps(
            {
                "event": "polygon_orderfilled_log",
                "selected_wallet": wallet,
                "maker": wallet,
                "taker": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "transaction_hash": "0xpolygon",
                "block_ts": 1783102533.0,
                "received_at_s": 1783102534.0,
                "decoded": {
                    "side": "BUY",
                    "asset": token,
                    "condition_id": "0xcondition",
                    "price": 0.66,
                    "size": 1.5,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(merge.time, "time", lambda: 1783102840.0)
    monkeypatch.setattr(merge, "_gamma_token_metadata", lambda *args, **kwargs: {})
    args = Namespace(
        rtds_jsonl=str(capture),
        source_wallets=[selected_wallet, wallet],
        wallet_names={selected_wallet: "selected", wallet: "unit"},
        history_state=str(tmp_path / "history.json"),
        history_window_index=str(tmp_path / "history_window_index.json"),
        wallet_event_log=str(tmp_path / "wallet_events.jsonl"),
        scan_limit=10,
        tail_bytes=4096,
        cold_tail_bytes=4096,
        offset_states={
            selected_wallet: str(tmp_path / "offset_selected.json"),
            wallet: str(tmp_path / "offset_wallet.json"),
        },
        watermark_state=str(tmp_path / "watermarks.json"),
        max_new_events=10,
        history_retain_events=250_000,
        history_retain_copy_intents=250_000,
        polygon_jsonl=str(polygon),
        polygon_tail_bytes=4096,
    )

    summary = merge.run_multi_wallet_merge(args)

    polygon_profile = summary["premerge_substage_profile"]["polygon_ws_premerge_parse"]
    assert polygon_profile["matching_events"] == 1
    assert polygon_profile["diagnostics"].get("token_mapping_missing", 0) == 0
    history = json.loads((tmp_path / "history.json").read_text(encoding="utf-8"))
    assert {row["source"] for row in history["events"]} == {"rtds_activity", "polygon_orderfilled_ws_premerge"}
