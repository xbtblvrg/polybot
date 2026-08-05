#!/usr/bin/env python3
"""Summarize read-only realtime detection evidence."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.realtime_feed import normalize_polygon_orderfilled_row, parse_rtds_trade_frame
from src.wallet_copy.store import atomic_write_json, load_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rtds-jsonl", default="data/research/rtds_activity_raw_capture.jsonl")
    parser.add_argument("--polygon-jsonl", default="data/research/polygon_orderfilled_ws_capture.jsonl")
    parser.add_argument("--dataapi-first-seen-jsonl", default="")
    parser.add_argument("--report", default="data/research/detection_latency_report.json")
    return parser.parse_args()


def _iter_jsonl(path: str) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _latency_stats(values: list[float]) -> dict[str, Any]:
    clean = sorted(values)
    if not clean:
        return {"count": 0}
    p90_index = min(len(clean) - 1, int(len(clean) * 0.9))
    p10_index = min(len(clean) - 1, int(len(clean) * 0.1))
    return {
        "count": len(clean),
        "min_s": clean[0],
        "p10_s": clean[p10_index],
        "p50_s": statistics.median(clean),
        "p90_s": clean[p90_index],
        "max_s": clean[-1],
    }


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) == 42:
        return text
    return ""


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _event_ts(row: dict[str, Any]) -> float | None:
    for key in ("event_ts", "timestamp", "block_ts"):
        value = _float_or_none(row.get(key))
        if value is not None and value > 0:
            return value
    return None


def _wallet_candidates(row: dict[str, Any]) -> list[str]:
    wallets = [_norm_wallet(row.get("selected_wallet")), _norm_wallet(row.get("wallet"))]
    wallets.extend(_norm_wallet(wallet) for wallet in (row.get("registry_wallets") or []))
    wallets.extend([_norm_wallet(row.get("maker")), _norm_wallet(row.get("taker"))])
    decoded = row.get("decoded")
    if isinstance(decoded, dict):
        wallets.extend([_norm_wallet(decoded.get("maker")), _norm_wallet(decoded.get("taker"))])
    return [item for item in dict.fromkeys(wallets) if item]


def _matched_polygon_dataapi_rows(
    polygon_rows: list[dict[str, Any]],
    dataapi_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    first_seen: dict[tuple[str, str], dict[str, Any]] = {}
    for row in dataapi_rows:
        if row.get("event") != "dataapi_first_seen":
            continue
        wallet = _norm_wallet(row.get("wallet"))
        tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
        if not wallet or not tx:
            continue
        key = (wallet, tx)
        if key not in first_seen or float(row.get("captured_at_s") or 0.0) < float(first_seen[key].get("captured_at_s") or 0.0):
            first_seen[key] = row

    matched_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    backfill_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in polygon_rows:
        if row.get("event") != "polygon_orderfilled_log" or row.get("source") != "polygon_ws":
            continue
        tx = str(row.get("transaction_hash") or row.get("transactionHash") or "").lower()
        if not tx:
            continue
        for wallet in _wallet_candidates(row):
            dataapi = first_seen.get((wallet, tx))
            if not dataapi:
                continue
            try:
                ws_recv_ts = float(row.get("received_at_s") or 0.0)
                dataapi_first_seen_ts = float(dataapi.get("captured_at_s") or 0.0)
            except (TypeError, ValueError):
                continue
            try:
                signed_lag = float(row.get("ws_receive_lag_signed_s"))
            except (TypeError, ValueError):
                signed_lag = None
            try:
                block_ts = float(row.get("block_ts"))
            except (TypeError, ValueError):
                block_ts = None
            try:
                poller_started_at_s = float(dataapi.get("poller_started_at_s"))
            except (TypeError, ValueError):
                poller_started_at_s = None
            try:
                poll_interval_s = float(dataapi.get("poll_interval_s") or 0.0)
            except (TypeError, ValueError):
                poll_interval_s = 0.0
            is_backfill = bool(dataapi.get("backfill"))
            if (
                str(dataapi.get("endpoint") or "") != "active_set_dataapi_poller"
                and poller_started_at_s is not None
                and block_ts is not None
            ):
                is_backfill = is_backfill or block_ts < poller_started_at_s + max(0.0, poll_interval_s)
            candidate = {
                "wallet": wallet,
                "tx": tx,
                "block_ts": row.get("block_ts"),
                "ws_recv_ts": ws_recv_ts,
                "dataapi_first_seen_ts": dataapi_first_seen_ts,
                "ws_receive_lag_signed_s": signed_lag,
                "ws_lead_s": dataapi_first_seen_ts - ws_recv_ts,
                "polygon_side": (row.get("decoded") or {}).get("side") if isinstance(row.get("decoded"), dict) else "",
                "dataapi_side": dataapi.get("side"),
                "dataapi_endpoint": dataapi.get("endpoint"),
                "backfill": is_backfill,
            }
            target = backfill_by_key if is_backfill else matched_by_key
            match_key = (wallet, tx)
            if match_key not in target or ws_recv_ts < float(target[match_key].get("ws_recv_ts") or 0.0):
                target[match_key] = candidate
    return list(matched_by_key.values()), list(backfill_by_key.values())


def _polygon_key_index(
    polygon_rows: list[dict[str, Any]],
    *,
    source: str | None,
    wallets: set[str] | None = None,
    min_event_ts: float | None = None,
    max_event_ts: float | None = None,
) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for row in polygon_rows:
        if row.get("event") != "polygon_orderfilled_log":
            continue
        if source is not None and row.get("source") != source:
            continue
        event_ts = _event_ts(row)
        if min_event_ts is not None and (event_ts is None or event_ts < min_event_ts):
            continue
        if max_event_ts is not None and (event_ts is None or event_ts > max_event_ts):
            continue
        tx = str(row.get("transaction_hash") or row.get("transactionHash") or "").lower()
        if not tx:
            continue
        for wallet in _wallet_candidates(row):
            if wallets is not None and wallet not in wallets:
                continue
            keys.add((wallet, tx))
    return keys


def _dataapi_first_seen_keys(
    dataapi_rows: list[dict[str, Any]],
    *,
    min_event_ts: float | None = None,
    max_event_ts: float | None = None,
) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for row in dataapi_rows:
        if row.get("event") != "dataapi_first_seen" or row.get("backfill") is True:
            continue
        event_ts = _event_ts(row)
        if min_event_ts is not None and (event_ts is None or event_ts < min_event_ts):
            continue
        if max_event_ts is not None and (event_ts is None or event_ts > max_event_ts):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
        if wallet and tx:
            keys.add((wallet, tx))
    return keys


def _unbounded_rows(
    rows: list[dict[str, Any]],
    *,
    event: str,
    source: str | None = None,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        if row.get("event") != event:
            continue
        if source is not None and row.get("source") != source:
            continue
        if _event_ts(row) is not None:
            continue
        output.append(row)
    return output


def _unbounded_sample(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sample: list[dict[str, Any]] = []
    for row in rows[:20]:
        sample.append(
            {
                "source": row.get("source"),
                "wallets": _wallet_candidates(row)[:5],
                "tx": str(row.get("transaction_hash") or row.get("transactionHash") or "").lower(),
                "captured_at_s": row.get("captured_at_s") or row.get("received_at_s"),
                "block_number": row.get("block_number") or row.get("blockNumber"),
                "block_ts_error": row.get("block_ts_error"),
            }
        )
    return sample


def _event_ts_bounds(rows: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    values = [_event_ts(row) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None, None
    return min(clean), max(clean)


def _overlap_bounds(
    left: tuple[float | None, float | None],
    right: tuple[float | None, float | None],
) -> tuple[float | None, float | None]:
    starts = [value for value in (left[0], right[0]) if value is not None]
    ends = [value for value in (left[1], right[1]) if value is not None]
    if not starts or not ends:
        return None, None
    start = max(starts)
    end = min(ends)
    if start > end:
        return None, None
    return start, end


def _dataapi_wallets(dataapi_rows: list[dict[str, Any]]) -> set[str]:
    return {
        wallet
        for row in dataapi_rows
        if row.get("event") == "dataapi_first_seen"
        for wallet in [_norm_wallet(row.get("wallet"))]
        if wallet
    }


def _match_polygon_dataapi(
    polygon_rows: list[dict[str, Any]],
    dataapi_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    matched, backfill = _matched_polygon_dataapi_rows(polygon_rows, dataapi_rows)
    active_wallets = _dataapi_wallets(dataapi_rows)
    ws_rows = [
        row
        for row in polygon_rows
        if row.get("event") == "polygon_orderfilled_log" and row.get("source") == "polygon_ws"
    ]
    dataapi_nonbackfill_rows = [
        row
        for row in dataapi_rows
        if row.get("event") == "dataapi_first_seen" and row.get("backfill") is not True
    ]
    polygon_unbounded_rows = _unbounded_rows(polygon_rows, event="polygon_orderfilled_log")
    polygon_ws_unbounded_rows = _unbounded_rows(polygon_rows, event="polygon_orderfilled_log", source="polygon_ws")
    dataapi_unbounded_rows = _unbounded_rows(dataapi_nonbackfill_rows, event="dataapi_first_seen")
    ws_bounds = _event_ts_bounds(ws_rows)
    dataapi_bounds = _event_ts_bounds(dataapi_nonbackfill_rows)
    overlap_min_ts, overlap_max_ts = _overlap_bounds(ws_bounds, dataapi_bounds)
    dataapi_keys = _dataapi_first_seen_keys(
        dataapi_rows,
        min_event_ts=overlap_min_ts,
        max_event_ts=overlap_max_ts,
    )
    polygon_ws_keys = _polygon_key_index(
        polygon_rows,
        source="polygon_ws",
        wallets=active_wallets or None,
        min_event_ts=overlap_min_ts,
        max_event_ts=overlap_max_ts,
    )
    polygon_all_keys = _polygon_key_index(
        polygon_rows,
        source=None,
        wallets=active_wallets or None,
        min_event_ts=overlap_min_ts,
        max_event_ts=overlap_max_ts,
    )
    dataapi_missing_ws = sorted(dataapi_keys - polygon_ws_keys)
    dataapi_present_non_ws = [key for key in dataapi_missing_ws if key in polygon_all_keys]
    dataapi_absent_all_polygon = [key for key in dataapi_missing_ws if key not in polygon_all_keys]
    ws_missing_dataapi = sorted(polygon_ws_keys - dataapi_keys)
    leads = [float(row["ws_lead_s"]) for row in matched if row.get("ws_lead_s") is not None]
    abs_lags = [
        abs(float(row["ws_receive_lag_signed_s"]))
        for row in matched
        if row.get("ws_receive_lag_signed_s") is not None
    ]
    return {
        "count": len(matched),
        "total_matched_including_backfill": len(matched) + len(backfill),
        "ws_before_dataapi_count": sum(1 for row in matched if float(row.get("ws_lead_s") or 0.0) >= 0),
        "ws_lead": _latency_stats(leads),
        "abs_ws_receive_lag": _latency_stats(abs_lags),
        "rows": matched[-500:],
        "matched_backfill": {
            "count": len(backfill),
            "rows": backfill[-100:],
        },
        "misses": {
            "active_wallet_count": len(active_wallets),
            "dataapi_backfill_false_keys": len(_dataapi_first_seen_keys(dataapi_rows)),
            "dataapi_backfill_false_keys_in_overlap": len(dataapi_keys),
            "dataapi_not_in_polygon_ws_count": len(dataapi_missing_ws),
            "dataapi_present_non_ws_count": len(dataapi_present_non_ws),
            "dataapi_absent_all_polygon_count": len(dataapi_absent_all_polygon),
            "ws_not_in_dataapi_count": len(ws_missing_dataapi),
            "coverage": {
                "dataapi_min_event_ts": dataapi_bounds[0],
                "dataapi_max_event_ts": dataapi_bounds[1],
                "polygon_ws_min_event_ts": ws_bounds[0],
                "polygon_ws_max_event_ts": ws_bounds[1],
                "overlap_min_event_ts": overlap_min_ts,
                "overlap_max_event_ts": overlap_max_ts,
            },
            "unbounded_rows": {
                "polygon_orderfilled_count": len(polygon_unbounded_rows),
                "polygon_ws_count": len(polygon_ws_unbounded_rows),
                "polygon_non_ws_count": len(polygon_unbounded_rows) - len(polygon_ws_unbounded_rows),
                "dataapi_first_seen_count": len(dataapi_unbounded_rows),
                "polygon_sample": _unbounded_sample(polygon_unbounded_rows),
                "dataapi_sample": _unbounded_sample(dataapi_unbounded_rows),
            },
            "dataapi_not_in_polygon_ws_sample": [
                {"wallet": wallet, "tx": tx} for wallet, tx in dataapi_missing_ws[:20]
            ],
            "dataapi_present_non_ws_sample": [
                {"wallet": wallet, "tx": tx} for wallet, tx in dataapi_present_non_ws[:20]
            ],
            "dataapi_absent_all_polygon_sample": [
                {"wallet": wallet, "tx": tx} for wallet, tx in dataapi_absent_all_polygon[:20]
            ],
            "ws_not_in_dataapi_sample": [
                {"wallet": wallet, "tx": tx} for wallet, tx in ws_missing_dataapi[:20]
            ],
        },
    }


def build_summary(
    rtds_rows: list[dict[str, Any]],
    polygon_rows: list[dict[str, Any]],
    dataapi_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rtds_events = []
    for row in rtds_rows:
        if row.get("event") != "rtds_raw_frame" or not row.get("data_frame_like"):
            continue
        try:
            rtds_events.extend(parse_rtds_trade_frame(str(row.get("raw") or ""), received_at_s=float(row.get("captured_at_s") or 0.0)))
        except Exception:
            continue

    polygon_events = []
    polygon_lags = []
    for row in polygon_rows:
        if row.get("event") != "polygon_orderfilled_log":
            continue
        event = normalize_polygon_orderfilled_row(row)
        if event is not None:
            polygon_events.append(event)
        if row.get("source") == "polygon_ws":
            try:
                polygon_lags.append(float(row.get("ws_receive_lag_signed_s")))
            except (TypeError, ValueError):
                pass

    rtds_lags = [
        event.received_at_s - event.event_ts
        for event in rtds_events
        if event.event_ts is not None and event.received_at_s
    ]
    return {
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "dataapi_first_seen": {
            "rows": sum(1 for row in (dataapi_rows or []) if row.get("event") == "dataapi_first_seen"),
        },
        "rtds": {
            "raw_frames": sum(1 for row in rtds_rows if row.get("event") == "rtds_raw_frame"),
            "normalized_events": len(rtds_events),
            "receive_lag": _latency_stats(rtds_lags),
        },
        "polygon_ws": {
            "raw_orderfilled_rows": sum(1 for row in polygon_rows if row.get("event") == "polygon_orderfilled_log"),
            "normalized_events": len(polygon_events),
            "unique_wallets": len({event.source_wallet for event in polygon_events if event.source_wallet}),
            "unique_txs": len({event.transaction_hash for event in polygon_events if event.transaction_hash}),
            "registry_rows": sum(
                1
                for row in polygon_rows
                if row.get("event") == "polygon_orderfilled_log" and row.get("is_registry_wallet")
            ),
            "receive_lag": _latency_stats(polygon_lags),
            "matched": _match_polygon_dataapi(polygon_rows, dataapi_rows or []),
        },
    }


def main() -> int:
    args = parse_args()
    report = load_json(args.report, default={})
    if not isinstance(report, dict):
        report = {}
    summary = build_summary(
        _iter_jsonl(args.rtds_jsonl),
        _iter_jsonl(args.polygon_jsonl),
        _iter_jsonl(args.dataapi_first_seen_jsonl) if args.dataapi_first_seen_jsonl else [],
    )
    report["summary"] = summary
    report["updated_at"] = utc_now_iso()
    atomic_write_json(args.report, report)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
