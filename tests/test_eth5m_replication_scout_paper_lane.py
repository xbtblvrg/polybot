import json
from pathlib import Path

from scripts.run_eth5m_replication_scout_paper_lane import collect_once


def _row(*, event_ts: float, received_at_s: float, slug: str = "eth-updown-5m-1784507100") -> dict:
    return {
        "event": "rtds_trade_event",
        "event_id": f"event-{event_ts}",
        "event_ts": event_ts,
        "received_at_s": received_at_s,
        "market_slug": slug,
        "condition_id": "condition",
        "asset": "token-up",
        "source_wallet": "0xabc",
        "side": "BUY",
        "price": 0.42,
        "raw": {"outcome": "Up"},
    }


def test_collector_admits_fresh_post_prereg_event_and_deduplicates(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    state = tmp_path / "state.json"
    events = tmp_path / "events.jsonl"
    fresh = _row(event_ts=1784507000.0, received_at_s=1784507001.0)
    source.write_text(json.dumps(fresh) + "\n" + json.dumps(fresh) + "\n")

    payload = collect_once(source, state, events, initial_tail_bytes=1024)

    assert payload["status"] == "ACCRUING"
    assert payload["observations"] == 1
    assert payload["distinct_windows"] == 1
    assert payload["copyintent_parity"] == "PASS_EVENT_TO_PAPER_INTENT_FIELDS"
    assert payload["live_order_attempts"] == 0
    assert "paper_intents" not in payload
    assert len(payload["seen_intent_keys"]) == 1
    assert len(payload["seen_intent_keys"][0]) == 40


def test_collector_rejects_stale_preprereg_and_non_eth_events(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    rows = [
        _row(event_ts=1784507000.0, received_at_s=1784507030.0),
        _row(event_ts=1784506900.0, received_at_s=1784506901.0),
        _row(event_ts=1784507000.0, received_at_s=1784507001.0, slug="btc-updown-5m-1784507100"),
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))

    payload = collect_once(source, tmp_path / "state.json", tmp_path / "events.jsonl", initial_tail_bytes=1024)

    assert payload["status"] == "ACCRUING_WAITING_FIRST_ELIGIBLE_EVENT"
    assert payload["observations"] == 0
    assert payload["live_orders_allowed"] is False


def test_collector_resume_keeps_first_complete_line_after_checkpoint(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    state = tmp_path / "state.json"
    events = tmp_path / "events.jsonl"
    first = _row(event_ts=1784507000.0, received_at_s=1784507001.0)
    source.write_text(json.dumps(first) + "\n")
    initial = collect_once(source, state, events, initial_tail_bytes=1024)
    second = _row(event_ts=1784507300.0, received_at_s=1784507301.0, slug="eth-updown-5m-1784507400")
    with source.open("a") as handle:
        handle.write(json.dumps(second) + "\n")

    resumed = collect_once(source, state, events, initial_tail_bytes=1024)

    assert initial["observations"] == 1
    assert resumed["observations"] == 2
    assert resumed["last_scan"]["admitted"] == 1


def test_collector_rejects_missing_or_invalid_price(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    missing = _row(event_ts=1784507000.0, received_at_s=1784507001.0)
    invalid = _row(event_ts=1784507300.0, received_at_s=1784507301.0, slug="eth-updown-5m-1784507400")
    missing["price"] = None
    invalid["price"] = 1.2
    source.write_text(json.dumps(missing) + "\n" + json.dumps(invalid) + "\n")

    payload = collect_once(source, tmp_path / "state.json", tmp_path / "events.jsonl", initial_tail_bytes=1024)

    assert payload["observations"] == 0
    assert payload["last_scan"]["rejected_counts"] == {"missing_price": 1, "invalid_price": 1}


def test_collector_preserves_explicit_zero_offset(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    state = tmp_path / "state.json"
    source.write_text(json.dumps(_row(event_ts=1784507000.0, received_at_s=1784507001.0)) + "\n")
    state.write_text(json.dumps({"source_offset_bytes": 0}))

    payload = collect_once(source, state, tmp_path / "events.jsonl", initial_tail_bytes=1)

    assert payload["observations"] == 1
