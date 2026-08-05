"""Small JSON/JSONL persistence helpers for wallet-copy state."""

from __future__ import annotations

import json
import os
import tempfile
import gzip
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import fcntl

_LOAD_JSON_CACHE_MIN_BYTES = int(os.environ.get("WALLET_COPY_LOAD_JSON_CACHE_MIN_BYTES", str(8 * 1024 * 1024)))
_LOAD_JSON_CACHE_MAX_ENTRIES = int(os.environ.get("WALLET_COPY_LOAD_JSON_CACHE_MAX_ENTRIES", "16"))
_LOAD_JSON_CACHE: dict[str, tuple[int, int, int, Any]] = {}


def load_json(path: str | Path, default: Any = None, *, cache_readonly: bool = False) -> Any:
    target = Path(path)
    if not cache_readonly:
        try:
            if target.suffix == ".gz":
                with gzip.open(target, "rt", encoding="utf-8") as handle:
                    return json.load(handle)
            return json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            _LOAD_JSON_CACHE.pop(str(target), None)
            return default

    cache_key = str(target)
    try:
        stat = target.stat()
    except OSError:
        _LOAD_JSON_CACHE.pop(cache_key, None)
        return default

    cacheable = stat.st_size >= max(0, _LOAD_JSON_CACHE_MIN_BYTES)
    signature = (int(stat.st_ino), int(stat.st_mtime_ns), int(stat.st_size))
    if cacheable:
        cached = _LOAD_JSON_CACHE.get(cache_key)
        if cached is not None and cached[:3] == signature:
            return cached[3]

    try:
        if target.suffix == ".gz":
            with gzip.open(target, "rt", encoding="utf-8") as handle:
                loaded = json.load(handle)
        else:
            loaded = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _LOAD_JSON_CACHE.pop(cache_key, None)
        return default
    if cacheable:
        if cache_key not in _LOAD_JSON_CACHE and len(_LOAD_JSON_CACHE) >= max(1, _LOAD_JSON_CACHE_MAX_ENTRIES):
            _LOAD_JSON_CACHE.pop(next(iter(_LOAD_JSON_CACHE)), None)
        _LOAD_JSON_CACHE[cache_key] = (*signature, loaded)
    return loaded


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent))
    try:
        if target.suffix == ".gz":
            with os.fdopen(fd, "wb") as raw_handle:
                with gzip.GzipFile(fileobj=raw_handle, mode="wb") as gzip_handle:
                    gzip_handle.write(text.encode("utf-8"))
                raw_handle.flush()
                os.fsync(raw_handle.fileno())
        else:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(tmp_name, target)
        _fsync_directory(target.parent)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def atomic_write_json(path: str | Path, payload: Any, *, compact: bool = False) -> None:
    if compact:
        text = json.dumps(payload, sort_keys=False, separators=(",", ":"), default=str)
    else:
        text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    atomic_write_text(path, text + "\n")


@contextmanager
def json_file_lock(path: str | Path) -> Iterator[None]:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target.with_name(f".{target.name}.lock")
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def append_jsonl(path: str | Path, row: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, default=str))
        handle.write("\n")


def append_jsonl_many(path: str | Path, rows: list[dict[str, Any]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, default=str))
            handle.write("\n")
