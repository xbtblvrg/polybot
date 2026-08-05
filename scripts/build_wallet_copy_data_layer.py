#!/usr/bin/env python3
"""Build the wallet-copy analytical Parquet/DuckDB layer from raw JSONL."""

from __future__ import annotations

import argparse
import glob
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = ROOT / "data" / "research" / "data_layer_v1_manifest.json"
DEFAULT_PROGRESS_STATE = ROOT / "data" / "research" / "data_layer_v1_progress.json"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "derived" / "wallet_copy_data_layer_v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.=-]+", "_", value.strip() or "unknown")
    return cleaned[:96] or "unknown"


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000.0
        try:
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            return _parse_ts(float(text))
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def _first_ts(row: dict[str, Any]) -> datetime | None:
    for key in (
        "observed_ts",
        "event_ts",
        "timestamp",
        "ts",
        "created_at",
        "createdAt",
        "updated_at",
        "updatedAt",
        "submitted_at",
        "filled_at",
        "window_start",
        "market_start",
        "generated_at",
    ):
        ts = _parse_ts(row.get(key))
        if ts is not None:
            return ts
    return None


def _series(row: dict[str, Any], source: Path) -> str:
    haystack = " ".join(
        str(row.get(key) or "")
        for key in ("series", "market_series", "market_slug", "slug", "condition_id", "title", "question")
    ).lower()
    slug = str(row.get("market_slug") or row.get("slug") or "").lower()
    match = re.match(r"([a-z0-9]+)-updown-([0-9]+[mhd])", slug)
    if match:
        return f"{match.group(1)}_{match.group(2)}"
    source_name = source.name.lower()
    if "btc" in haystack or "btc" in source_name:
        return "btc"
    if "eth" in haystack or "eth" in source_name:
        return "eth"
    if "sol" in haystack or "sol" in source_name:
        return "sol"
    return "unknown"


def _condition_id(row: dict[str, Any]) -> str | None:
    for key in ("condition_id", "conditionId", "market", "market_id", "marketId"):
        value = row.get(key)
        if value:
            return str(value)
    return None


def _wallet(row: dict[str, Any]) -> str | None:
    for key in ("source_wallet", "wallet", "proxy_wallet", "user", "maker", "taker"):
        value = row.get(key)
        if value:
            return str(value).lower()
    return None


def _relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _remove_prior_outputs(paths: list[str]) -> None:
    for raw in paths:
        path = Path(raw)
        if not path.is_absolute():
            path = ROOT / path
        try:
            path.unlink()
        except FileNotFoundError:
            continue


def _iter_jsonl(path: Path, *, max_rows: int | None = None) -> Iterable[tuple[int, dict[str, Any]]]:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_no, line in enumerate(handle, start=1):
            if max_rows is not None and line_no > max_rows:
                break
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield line_no, payload


def _row(source: Path, line_no: int, payload: dict[str, Any]) -> dict[str, Any]:
    ts = _first_ts(payload)
    day = ts.date().isoformat() if ts is not None else "unknown"
    return {
        "source_file": _relative(source),
        "source_line": line_no,
        "event_ts": ts.isoformat().replace("+00:00", "Z") if ts is not None else None,
        "day": day,
        "series": _series(payload, source),
        "condition_id": _condition_id(payload),
        "wallet": _wallet(payload),
        "raw_json": json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")),
    }


def _dependencies() -> tuple[Any | None, Any | None, Any | None, list[str]]:
    missing: list[str] = []
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except Exception:
        pa = None
        pq = None
        missing.append("pyarrow")
    try:
        import duckdb  # type: ignore
    except Exception:
        duckdb = None
        missing.append("duckdb")
    return pa, pq, duckdb, missing


def _input_files(patterns: list[str], *, max_files: int | None) -> list[Path]:
    seen: set[Path] = set()
    files: list[Path] = []
    for pattern in patterns:
        for raw in sorted(glob.glob(pattern, recursive=True)):
            path = Path(raw)
            if not path.is_absolute():
                path = ROOT / path
            if path in seen or not path.is_file():
                continue
            seen.add(path)
            files.append(path)
            if max_files is not None and len(files) >= max_files:
                return files
    return files


def _write_partition(pa: Any, pq: Any, output_dir: Path, key: tuple[str, str, str], rows: list[dict[str, Any]]) -> str:
    dataset, day, series = key
    partition_dir = output_dir / f"dataset={_safe_part(dataset)}" / f"day={_safe_part(day)}" / f"series={_safe_part(series)}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    output = partition_dir / f"part-{len(list(partition_dir.glob('part-*.parquet'))):06d}.parquet"
    pq.write_table(pa.Table.from_pylist(rows), output)
    return str(output)


def _source_current(source: Path, prior: dict[str, Any]) -> bool:
    return (
        int(prior.get("size") or -1) == source.stat().st_size
        and int(prior.get("mtime_ns") or -1) == source.stat().st_mtime_ns
    )


def build_layer(args: argparse.Namespace) -> dict[str, Any]:
    pa, pq, duckdb, missing = _dependencies()
    input_globs = args.input_glob or ["data/research/*.jsonl"]
    progress_path = Path(args.progress_state)
    if not progress_path.is_absolute():
        progress_path = ROOT / progress_path
    progress = _load_json(progress_path, {})
    progress_files = progress.get("files") if isinstance(progress.get("files"), dict) else {}
    all_files = _input_files(input_globs, max_files=None)
    files: list[Path] = []
    selected_bytes = 0
    max_bytes = int(args.max_bytes or 0)
    for source in all_files:
        prior = progress_files.get(str(source), {}) if isinstance(progress_files.get(str(source)), dict) else {}
        if not args.force and _source_current(source, prior):
            continue
        source_size = source.stat().st_size
        if max_bytes and files and selected_bytes + source_size > max_bytes:
            break
        files.append(source)
        selected_bytes += source_size
        if args.max_files is not None and len(files) >= args.max_files:
            break
    report: dict[str, Any] = {
        "schema_version": 1,
        "kind": "data_layer_v1_manifest",
        "flow_stage": "SELF-DEV/LEARN",
        "generated_at": _utc_now_iso(),
        "status": "MISSING_DEPENDENCY" if missing else "READY",
        "missing_dependencies": missing,
        "input_globs": input_globs,
        "files_considered": len(files),
        "files_converted": 0,
        "rows_converted": 0,
        "bytes_read": 0,
        "output_root": str(Path(args.output_dir)),
        "partitioning": ["dataset", "day", "series"],
        "raw_jsonl_remains_source_of_record": True,
        "duckdb": {"duckdb_path": str(Path(args.duckdb_path)), "rows": None, "view": "wallet_copy_events"},
        "results": [],
        "partitions": {},
        "next_action": "pip install -r requirements.txt then rerun without --manifest-only" if missing else "run without --manifest-only on the full analytical JSONL set",
    }
    if args.manifest_only or missing:
        return report

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    bytes_budget = int(args.max_bytes or 0)
    for source in files:
        if bytes_budget and report["bytes_read"] >= bytes_budget:
            break
        kind = _safe_part(source.stem)
        prior = progress_files.get(str(source), {}) if isinstance(progress_files.get(str(source)), dict) else {}
        _remove_prior_outputs([str(path) for path in prior.get("output_files", []) if path])
        batches: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        file_rows = 0
        file_partitions: list[str] = []
        for line_no, payload in _iter_jsonl(source, max_rows=args.max_rows_per_file):
            normalized = _row(source, line_no, payload)
            key = (kind, normalized["day"], normalized["series"])
            batches[key].append(normalized)
            file_rows += 1
            if len(batches[key]) >= args.batch_size:
                part = _write_partition(pa, pq, output_dir, key, batches[key])
                file_partitions.append(part)
                report["files_converted"] += 1
                report["rows_converted"] += len(batches[key])
                report["partitions"][part] = report["partitions"].get(part, 0) + len(batches[key])
                batches[key] = []
        for key, rows in list(batches.items()):
            if not rows:
                continue
            part = _write_partition(pa, pq, output_dir, key, rows)
            file_partitions.append(part)
            report["files_converted"] += 1
            report["rows_converted"] += len(rows)
            report["partitions"][part] = report["partitions"].get(part, 0) + len(rows)
        report["bytes_read"] += source.stat().st_size
        report["results"].append({"path": str(source), "rows": file_rows, "size": source.stat().st_size, "status": "QUEUED", "output_files": file_partitions})
        progress_files[str(source)] = {
            "path": str(source),
            "size": source.stat().st_size,
            "mtime_ns": source.stat().st_mtime_ns,
            "rows": file_rows,
            "output_files": file_partitions,
            "updated_at": _utc_now_iso(),
        }

    parquet_files = list(output_dir.glob("dataset=*/day=*/series=*/*.parquet"))
    if not parquet_files:
        report["status"] = "READY"
        report["next_action"] = "no parquet files exist yet; run with non-empty JSONL inputs"
        return report

    duckdb_path = Path(args.duckdb_path)
    if not duckdb_path.is_absolute():
        duckdb_path = ROOT / duckdb_path
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path))
    parquet_glob = str((output_dir / "dataset=*" / "day=*" / "series=*" / "*.parquet").resolve()).replace("'", "''")
    con.execute(
        "CREATE OR REPLACE VIEW wallet_copy_events AS "
        f"SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true, union_by_name=true)"
    )
    rows = con.execute("SELECT count(*) FROM wallet_copy_events").fetchone()[0]
    con.close()
    report["status"] = "PASS"
    report["duckdb"] = {
        "duckdb_path": str(duckdb_path),
        "parquet_glob": str((output_dir / "**" / "*.parquet").resolve()),
        "rows": rows,
        "view": "wallet_copy_events",
    }
    report["next_action"] = "migrate the next corpus scan to DuckDB wallet_copy_events and benchmark"
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "data_layer_v1_progress",
                "generated_at": _utc_now_iso(),
                "output_root": str(output_dir),
                "files": progress_files,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-glob", action="append")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--duckdb-path", default=str(DEFAULT_OUTPUT_DIR / "wallet_copy.duckdb"))
    parser.add_argument("--state-output", default=str(DEFAULT_STATE))
    parser.add_argument("--progress-state", default=str(DEFAULT_PROGRESS_STATE))
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--max-files", type=int)
    parser.add_argument("--max-bytes", type=int, default=0)
    parser.add_argument("--max-rows-per-file", type=int)
    parser.add_argument("--manifest-only", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if not args.input_glob:
        args.input_glob = ["data/research/*.jsonl"]
    return args


def main() -> int:
    args = parse_args()
    report = build_layer(args)
    state_output = Path(args.state_output)
    if not state_output.is_absolute():
        state_output = ROOT / state_output
    state_output.parent.mkdir(parents=True, exist_ok=True)
    state_output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"output": str(state_output), "rows_converted": report["rows_converted"], "status": report["status"]}, indent=2, sort_keys=True))
    return 0 if report["status"] in {"PASS", "READY", "MISSING_DEPENDENCY"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
