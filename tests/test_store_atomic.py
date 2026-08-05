import json
import threading
import time
from pathlib import Path

from src.wallet_copy import store as store_module
from src.wallet_copy.store import atomic_write_json, load_json


def test_atomic_write_json_never_exposes_partial_json(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    atomic_write_json(path, {"version": 0, "payload": "seed"})
    errors: list[Exception] = []
    done = threading.Event()

    def writer() -> None:
        for version in range(1, 80):
            atomic_write_json(path, {"version": version, "payload": "x" * 25_000})
            time.sleep(0.0005)
        done.set()

    def reader() -> None:
        while not done.is_set():
            try:
                json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:  # pragma: no cover - assertion payload
                errors.append(exc)
                done.set()
                return

    writer_thread = threading.Thread(target=writer)
    reader_thread = threading.Thread(target=reader)
    writer_thread.start()
    reader_thread.start()
    writer_thread.join()
    done.set()
    reader_thread.join()

    assert errors == []
    assert load_json(path, default={})["version"] == 79


def test_load_json_caches_large_unchanged_files_and_reloads_atomic_replace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "large_state.json"
    monkeypatch.setattr(store_module, "_LOAD_JSON_CACHE_MIN_BYTES", 0)
    store_module._LOAD_JSON_CACHE.clear()

    atomic_write_json(path, {"version": 1, "payload": "x" * 100})
    uncached_first = load_json(path, default={})
    uncached_second = load_json(path, default={})

    assert uncached_second == uncached_first
    assert uncached_second is not uncached_first

    first = load_json(path, default={}, cache_readonly=True)
    second = load_json(path, default={}, cache_readonly=True)

    assert second is first

    atomic_write_json(path, {"version": 2, "payload": "x" * 100})
    changed = load_json(path, default={}, cache_readonly=True)

    assert changed is not first
    assert changed["version"] == 2
    store_module._LOAD_JSON_CACHE.clear()
