import json
from pathlib import Path

from scripts.accumulate_orderfilled_01a_supply_daily import (
    _hold_cursor_on_gamma_failure,
    accumulate,
    _merge_partition,
)


WALLET = "0x" + "a" * 40
OTHER = "0x" + "b" * 40


def _event(wallet: str, epoch: int, price: float) -> dict:
    return {"maker": wallet, "taker": OTHER, "block_ts": epoch + 20, "decoded": {"side": "BUY", "maker_side": "BUY", "asset": "asset", "price": price, "size": 1}}


def test_accumulator_initializes_at_eof_then_reads_only_increment(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    events.write_text(json.dumps(_event(WALLET, 1_000, 0.26)) + "\n", encoding="utf-8")
    daily, state = accumulate(events_path=events, metadata={"asset": {"market_slug": "btc-updown-5m-1000"}}, state={}, max_increment_bytes=10_000)
    assert daily == {}
    assert state["status"] == "INITIALIZED_FROM_EOF"
    with events.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_event(WALLET, 1_300, 0.27)) + "\n")
    daily, state = accumulate(events_path=events, metadata={"asset": {"market_slug": "btc-updown-5m-1300"}}, state=state, max_increment_bytes=10_000)
    day = next(iter(daily.values()))
    assert day[WALLET]["observed"] == {1300}
    assert day[WALLET]["qualifying"] == {1300}
    assert state["capture_gap"] is False


def test_partition_merge_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "2026-08-05.json"
    rows = {WALLET: {"observed": {1000, 1300}, "qualifying": {1000}}}
    _merge_partition(path, day_utc="2026-08-05", rows=rows, generated_at="now")
    _merge_partition(path, day_utc="2026-08-05", rows=rows, generated_at="later")
    payload = json.loads(path.read_text())
    assert payload["rows"][0]["observed_window_epochs"] == [1000, 1300]
    assert payload["rows"][0]["qualifying_01a_window_epochs"] == [1000]
    assert payload["distinct_window_epoch_count"] == 2


def test_gamma_failure_holds_cursor_for_idempotent_retry() -> None:
    state = {"next_byte_offset": 200, "status": "PASS_INCREMENTAL"}
    _hold_cursor_on_gamma_failure(
        state,
        bounds=(None, 100, 200, False, 100),  # type: ignore[arg-type]
        gamma_stats={"request_failures": 1},
    )
    assert state["next_byte_offset"] == 100
    assert state["status"] == "GAMMA_UNRESOLVED_HOLD"
