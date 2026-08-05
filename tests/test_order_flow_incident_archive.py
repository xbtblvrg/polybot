import gzip
import hashlib
import json
import multiprocessing
from pathlib import Path

from scripts.order_flow_incident_archive import (
    append_incident_row,
    load_incident_rows,
    rotate_incident_journal,
)


def _write_rows(path: Path, rows: list[dict]) -> bytes:
    payload = b"".join(
        json.dumps(row, sort_keys=True).encode("utf-8") + b"\n" for row in rows
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return payload


def _append_worker(active: str, manifest: str, identity: str) -> None:
    append_incident_row(
        Path(active),
        {"incident_id": identity, "blob": "race"},
        manifest_path=Path(manifest),
    )


def test_rotation_is_lossless_line_aligned_and_manifest_verified(tmp_path: Path):
    active = tmp_path / "order_flow_deadman_incidents.jsonl"
    manifest = tmp_path / "order_flow_deadman_incidents_manifest.json"
    original_rows = [
        {"incident_id": f"i{index}", "checked_at": f"2026-07-25T00:{index:02d}:00Z", "blob": "x" * 80}
        for index in range(20)
    ]
    original = _write_rows(active, original_rows)

    result = rotate_incident_journal(
        active, manifest_path=manifest, max_tail_bytes=500
    )

    assert result["status"] == "ROTATED"
    assert result["reconstruction_verified"] is True
    metadata = json.loads(manifest.read_text())
    archive = tmp_path / metadata["archives"][0]["path"]
    prefix = gzip.decompress(archive.read_bytes())
    assert prefix + active.read_bytes() == original
    assert hashlib.sha256(prefix).hexdigest() == metadata["archives"][0]["uncompressed_sha256"]
    assert load_incident_rows(active, manifest_path=manifest) == original_rows


def test_rotation_and_append_are_idempotent_without_double_count(tmp_path: Path):
    active = tmp_path / "order_flow_deadman_incidents.jsonl"
    manifest = tmp_path / "order_flow_deadman_incidents_manifest.json"
    rows = [{"incident_id": f"i{index}", "blob": "x" * 60} for index in range(20)]
    _write_rows(active, rows)
    first = rotate_incident_journal(active, manifest_path=manifest, max_tail_bytes=300)
    second = rotate_incident_journal(active, manifest_path=manifest, max_tail_bytes=300)
    append_incident_row(active, {"incident_id": "new", "blob": "y"}, manifest_path=manifest)

    assert first["status"] == "ROTATED"
    assert second["status"] == "NO_ROTATION_NEEDED"
    loaded = load_incident_rows(active, manifest_path=manifest)
    assert [row["incident_id"] for row in loaded] == [
        *(f"i{index}" for index in range(20)),
        "new",
    ]


def test_reader_deduplicates_archive_active_overlap(tmp_path: Path):
    active = tmp_path / "order_flow_deadman_incidents.jsonl"
    manifest = tmp_path / "order_flow_deadman_incidents_manifest.json"
    rows = [{"incident_id": f"i{index}", "blob": "x" * 80} for index in range(10)]
    _write_rows(active, rows)
    rotate_incident_journal(active, manifest_path=manifest, max_tail_bytes=250)
    archived_first = load_incident_rows(active, manifest_path=manifest)[0]
    with active.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(archived_first) + "\n")

    loaded = load_incident_rows(active, manifest_path=manifest)

    assert len(loaded) == len(rows)


def test_concurrent_append_is_lock_serialized(tmp_path: Path):
    active = tmp_path / "order_flow_deadman_incidents.jsonl"
    manifest = tmp_path / "order_flow_deadman_incidents_manifest.json"
    processes = [
        multiprocessing.Process(
            target=_append_worker,
            args=(str(active), str(manifest), f"race-{index}"),
        )
        for index in range(4)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)

    assert all(process.exitcode == 0 for process in processes)
    assert {row["incident_id"] for row in load_incident_rows(active, manifest_path=manifest)} == {
        *(f"race-{index}" for index in range(4))
    }


def test_default_names_are_derived_for_a_second_journal(tmp_path: Path):
    active = tmp_path / "wide_direct_handoff_journal.jsonl"
    rows = [{"incident_id": f"wide-{index}", "blob": "x" * 80} for index in range(20)]
    _write_rows(active, rows)

    result = rotate_incident_journal(active, max_tail_bytes=300)

    assert Path(result["archive"]).name == "wide_direct_handoff_journal_0001.jsonl.gz"
    assert Path(result["manifest"]).name == "wide_direct_handoff_journal_manifest.json"
    assert load_incident_rows(active) == rows
