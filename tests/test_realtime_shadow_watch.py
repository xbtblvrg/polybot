import json
from argparse import Namespace

from src.wallet_copy.store import load_json
from scripts.run_realtime_shadow_watch import _read_incremental_lines, build_scored_state, run_resident_loop


def _args(tmp_path, *, now_s: float = 1783375990.0) -> Namespace:
    return Namespace(
        watch_state=str(tmp_path / "watch.json"),
        rtds_jsonl=str(tmp_path / "rtds.jsonl"),
        output=str(tmp_path / "state.json"),
        event_log=str(tmp_path / "events.jsonl"),
        offset_state=str(tmp_path / "offset.json"),
        tail_bytes=1024,
        max_events=10,
        book_timeout_s=0.1,
        copy_size_usd=1.0,
        strict_slippage_bps=250.0,
        drift_buffer_price=0.05,
        max_source_age_s=60.0,
        buy_only=True,
        market_slug_prefix="btc-updown-5m-",
        iterations=1,
        sleep_s=0.0,
        max_retained_events_per_wallet=200,
        now_s=now_s,
    )


def test_realtime_shadow_watch_records_missing_book_inputs_without_submit(tmp_path) -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    args = _args(tmp_path)

    state = build_scored_state(
        watch_state={
            "wallets": [{"wallet": wallet}],
            "passive_wallets": [],
        },
        rtds_lines=[
            '{"event":"rtds_trade_event","source_wallet":"0x1111111111111111111111111111111111111111","event_ts":1783375989,"received_at_s":1783375989.5,"side":"BUY","market_slug":"btc-updown-5m-1783376100"}'
        ],
        args=args,
    )

    assert state["summary"]["registered_wallets"] == 1
    assert state["summary"]["new_events"] == 1
    assert state["summary"]["orders_submitted"] == 0
    assert state["events"][0]["status"] == "NEEDS_BOOK_FETCH_INPUTS"
    assert state["events"][0]["book_age_s"] is None


def test_realtime_shadow_watch_skips_stale_source_events(tmp_path) -> None:
    wallet = "0x1111111111111111111111111111111111111111"

    state = build_scored_state(
        watch_state={"wallets": [{"wallet": wallet}], "passive_wallets": []},
        rtds_lines=[
            '{"event":"rtds_trade_event","source_wallet":"0x1111111111111111111111111111111111111111","event_ts":1783375800,"received_at_s":1783375800,"side":"BUY","asset":"123","price":0.5,"market_slug":"btc-updown-5m-1783376100"}'
        ],
        args=_args(tmp_path, now_s=1783375990.0),
    )

    assert state["summary"]["new_events"] == 0
    assert state["diagnostics"]["source_age_gt_cap"] == 1


def test_realtime_shadow_watch_skips_non_buy_events(tmp_path) -> None:
    wallet = "0x1111111111111111111111111111111111111111"

    state = build_scored_state(
        watch_state={"wallets": [{"wallet": wallet}], "passive_wallets": []},
        rtds_lines=[
            '{"event":"rtds_trade_event","source_wallet":"0x1111111111111111111111111111111111111111","event_ts":1783375989,"received_at_s":1783375989.5,"side":"SELL","asset":"123","price":0.5,"market_slug":"btc-updown-5m-1783376100"}'
        ],
        args=_args(tmp_path),
    )

    assert state["summary"]["new_events"] == 0
    assert state["diagnostics"]["non_buy_event"] == 1


def test_realtime_shadow_watch_persists_incremental_offset(tmp_path) -> None:
    rtds = tmp_path / "rtds.jsonl"
    offset = tmp_path / "offset.json"
    rtds.write_text('{"event":"old"}\n', encoding="utf-8")

    first_lines, first_state = _read_incremental_lines(rtds, offset, tail_bytes=1024)
    second_lines, second_state = _read_incremental_lines(rtds, offset, tail_bytes=1024)
    with rtds.open("a", encoding="utf-8") as handle:
        handle.write('{"event":"new"}\n')
    third_lines, third_state = _read_incremental_lines(rtds, offset, tail_bytes=1024)

    assert first_lines == ['{"event":"old"}']
    assert second_lines == []
    assert third_lines == ['{"event":"new"}']
    assert first_state["offset"] == second_state["previous_offset"]
    assert third_state["previous_offset"] == second_state["offset"]
    assert third_state["status"] == "OK"


def test_realtime_shadow_watch_resident_loop_exits_after_iterations(tmp_path) -> None:
    args = _args(tmp_path)
    args.iterations = 2
    args.sleep_s = 0.0
    watch_path = tmp_path / "watch.json"
    rtds_path = tmp_path / "rtds.jsonl"
    watch_path.write_text('{"wallets":[],"passive_wallets":[],"summary":{}}\n', encoding="utf-8")
    rtds_path.write_text('{"event":"rtds_trade_event","source_wallet":"0x2222222222222222222222222222222222222222"}\n', encoding="utf-8")

    assert run_resident_loop(args) == 0

    watch_state = load_json(str(watch_path), default={})
    resident = watch_state["resident_scorer"]
    assert resident["status"] == "EXITED"
    assert resident["iteration"] == 2
    assert resident["orders_submitted"] == 0


def test_realtime_shadow_watch_caps_events_but_keeps_window_counters(tmp_path) -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    args = _args(tmp_path)
    args.max_retained_events_per_wallet = 2
    prior_events = [
        {
            "event_id": f"e{idx}",
            "wallet": wallet,
            "market_slug": f"btc-updown-5m-{1783376100 + idx * 300}",
            "status": "SCORED",
            "within_copy_latency_window": True,
            "book_age_s": 0.1,
            "taker_fillable": True,
        }
        for idx in range(3)
    ]
    (tmp_path / "state.json").write_text(
        (
            '{"summary":{"realtime_taker_market_windows_by_wallet":'
            '{"0x1111111111111111111111111111111111111111":["btc-updown-5m-1783376100"]}},'
            '"events":'
            + json.dumps(prior_events)
            + "}"
        ),
        encoding="utf-8",
    )

    state = build_scored_state(
        watch_state={"wallets": [{"wallet": wallet}], "passive_wallets": []},
        rtds_lines=[],
        args=args,
    )

    assert len(state["events"]) == 2
    assert state["summary"]["evicted_events"] == 1
    assert state["summary"]["scored_events"] == 3
    assert state["summary"]["retained_scored_events"] == 2
    assert state["summary"]["promotion_eligible_realtime_taker_events"] == 3
    assert state["summary"]["retained_promotion_eligible_realtime_taker_events"] == 2
    assert state["summary"]["realtime_taker_distinct_market_windows_by_wallet"][wallet] == 3
    assert state["summary"]["promotion_unit"] == "distinct_market_window"
