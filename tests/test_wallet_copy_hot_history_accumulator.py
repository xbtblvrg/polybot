import json
import sqlite3
from pathlib import Path

from scripts.accumulate_wallet_copy_hot_history import (
    accumulate,
    export_supplemental_history,
)


def _write_hot(path: Path, events: list[dict], copy_intents: list[dict] | None = None) -> None:
    path.write_text(
        json.dumps({"events": events, "copy_intents": copy_intents or []}),
        encoding="utf-8",
    )


def test_accumulator_retains_rotated_events_and_deduplicates_stable_ids(tmp_path: Path) -> None:
    hot = tmp_path / "hot.json"
    database = tmp_path / "accumulator.sqlite3"
    state_path = tmp_path / "state.json"
    _write_hot(hot, [{"event_id": "a", "event_ts": 100.0}, {"event_id": "b", "event_ts": 200.0}])
    first = accumulate(hot_history=hot, database=database, state_path=state_path, now_ts=205.0)
    _write_hot(hot, [{"event_id": "b", "event_ts": 200.0}, {"event_id": "c", "event_ts": 300.0}])
    second = accumulate(hot_history=hot, database=database, state_path=state_path, now_ts=305.0)

    with sqlite3.connect(database) as connection:
        ids = [row[0] for row in connection.execute("SELECT event_id FROM events ORDER BY event_ts")]
    assert ids == ["a", "b", "c"]
    assert first["event_count"] == 2
    assert second["events_inserted"] == 1
    assert second["event_count"] == 3
    assert second["span_days"] > 0
    assert second["source_freshness"]["pass"] is True
    assert second["repoint_allowed"] is False


def test_accumulator_fails_closed_without_source_timestamps(tmp_path: Path) -> None:
    hot = tmp_path / "hot.json"
    _write_hot(hot, [{"event_id": "a"}])
    state = accumulate(
        hot_history=hot,
        database=tmp_path / "accumulator.sqlite3",
        state_path=tmp_path / "state.json",
        now_ts=500.0,
    )

    assert state["status"] == "STALE_SOURCE_FAIL_CLOSED"
    assert state["source_freshness"]["pass"] is False


def test_export_is_additive_and_preserves_existing_manifest_files(tmp_path: Path) -> None:
    database = tmp_path / "accumulator.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE events (event_id TEXT PRIMARY KEY, event_ts REAL NOT NULL, payload_json TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO events VALUES (?, ?, ?)",
            (
                "a",
                100.0,
                json.dumps(
                    {
                        "event_id": "a",
                        "event_ts": 100.0,
                        "source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "raw": {"must": "not be exported"},
                    }
                ),
            ),
        )
        connection.commit()
    output = tmp_path / "supplemental.json"
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "supplemental_history_files": ["existing.json"],
                "last_batch_id": "old-batch",
            }
        ),
        encoding="utf-8",
    )

    result = export_supplemental_history(
        database=database,
        output=output,
        manifest_path=manifest,
        generated_at="2026-07-30T20:00:00Z",
    )

    exported = json.loads(output.read_text(encoding="utf-8"))
    updated_manifest = json.loads(manifest.read_text(encoding="utf-8"))
    assert result["event_count"] == 1
    assert result["wallet_count"] == 1
    assert exported["additive_supplemental"] is True
    assert "raw" not in exported["events"][0]
    assert set(updated_manifest["supplemental_history_files"]) == {
        "existing.json",
        str(output),
    }
    assert updated_manifest["last_batch_id"] == "old-batch"


def test_accumulator_exports_projected_copy_intents(tmp_path: Path) -> None:
    hot = tmp_path / "hot.json"
    database = tmp_path / "accumulator.sqlite3"
    output = tmp_path / "supplemental.json"
    manifest = tmp_path / "manifest.json"
    intent_sidecar = tmp_path / "copy_intents.json"
    _write_hot(
        hot,
        [{"event_id": "we-1", "event_ts": 100.0}],
        [],
    )
    intent_sidecar.write_text(json.dumps({"copy_intents": [{
            "intent_id": "ci-1",
            "source_event_id": "lane-alias",
            "source_row_event_id": "we-1",
            "source_wallet": "0x" + "a" * 40,
            "market_slug": "btc-updown-5m-100",
            "observed_ts": 101.0,
            "metadata": {"must": "not be exported"},
        }]}), encoding="utf-8")
    state = accumulate(
        hot_history=hot,
        database=database,
        state_path=tmp_path / "state.json",
        now_ts=105.0,
        copy_intents_state=intent_sidecar,
    )
    exported = export_supplemental_history(
        database=database,
        output=output,
        manifest_path=manifest,
        generated_at="2026-08-05T00:00:00Z",
    )
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert state["copy_intent_count"] == 1
    assert exported["copy_intent_count"] == 1
    assert payload["copy_intents"] == [{
        "intent_id": "ci-1",
        "market_slug": "btc-updown-5m-100",
        "observed_ts": 101.0,
        "source_event_id": "lane-alias",
        "source_row_event_id": "we-1",
        "source_wallet": "0x" + "a" * 40,
    }]
