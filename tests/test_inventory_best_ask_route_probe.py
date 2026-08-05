import json

from scripts import probe_inventory_best_ask_route as probe


def test_probe_targets_book_skip_rollup_tokens(tmp_path, monkeypatch):
    guard_state = tmp_path / "guard.json"
    events = tmp_path / "events.jsonl"
    output = tmp_path / "probe.json"
    guard_state.write_text(
        json.dumps(
            {
                "window_participation": {
                    "window_rollups": [
                        {
                            "market_slug": "btc-updown-5m-1783950000",
                            "source_wallet": "0xe6db20932faf0f9780acf75d95c74c9984407dac",
                            "outcomes": ["Down"],
                            "dominant_skip_reason_counts": {"inventory_best_ask_book_error": 1},
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    events.write_text(
        json.dumps(
            {
                "market_slug": "btc-updown-5m-1783950000",
                "source_wallet": "0xe6db20932faf0f9780acf75d95c74c9984407dac",
                "outcome": "Down",
                "token_id": "token-down",
                "price": 0.46,
                "event_ts": 1783950005,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        probe,
        "_probe_direct",
        lambda token_id, timeout_s: {"route": "direct", "status": "PASS", "best_ask": 0.47},
    )
    monkeypatch.setattr(
        probe,
        "_probe_client",
        lambda token_id, timeout_s: {"route": "client", "status": "PASS", "best_ask": 0.47},
    )

    payload = probe.build_probe(
        guard_state_path=guard_state,
        wallet_events=events,
        output=output,
        limit=8,
        timeout_s=1.0,
        tail_bytes=0,
    )

    assert payload["status"] == "PASS_ROUTE_HEALTHY"
    assert payload["target_tokens"] == 1
    assert payload["rows"][0]["token_id"] == "token-down"


def test_probe_classifies_closed_404_no_asks_as_stale_target(tmp_path, monkeypatch):
    guard_state = tmp_path / "guard.json"
    events = tmp_path / "events.jsonl"
    output = tmp_path / "probe.json"
    guard_state.write_text(
        json.dumps(
            {
                "window_participation": {
                    "window_rollups": [
                        {
                            "market_slug": "btc-updown-5m-1783950000",
                            "source_wallet": "0xe6db20932faf0f9780acf75d95c74c9984407dac",
                            "outcomes": ["Down"],
                            "dominant_skip_reason_counts": {"inventory_best_ask_missing": 1},
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    events.write_text(
        json.dumps(
            {
                "market_slug": "btc-updown-5m-1783950000",
                "source_wallet": "0xe6db20932faf0f9780acf75d95c74c9984407dac",
                "outcome": "Down",
                "token_id": "token-down",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(probe.time, "time", lambda: 1783950900.0)
    monkeypatch.setattr(
        probe,
        "_probe_direct",
        lambda token_id, timeout_s: {"route": "direct", "status": "ERROR", "error": "HTTPError: 404"},
    )
    monkeypatch.setattr(
        probe,
        "_probe_client",
        lambda token_id, timeout_s: {"route": "client", "status": "NO_ASKS", "best_ask": 0.0},
    )

    payload = probe.build_probe(
        guard_state_path=guard_state,
        wallet_events=events,
        output=output,
        limit=8,
        timeout_s=1.0,
        tail_bytes=0,
    )

    assert payload["status"] == "PASS_STALE_TARGETS_CLASSIFIED"
    assert payload["summary"]["stale_no_book_classified"] == 1
