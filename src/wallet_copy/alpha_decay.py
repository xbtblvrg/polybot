"""Alpha-decay measurement for realtime wallet-copy detections.

The module is data-side only: it joins wallet-attributed Polygon WSS fills to
public CLOB market observations and estimates whether the copied side still had
edge after fixed latency horizons.
"""

from __future__ import annotations

import bisect
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Any


DEFAULT_HORIZONS_S = (1.0, 2.0, 5.0, 30.0)
DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S = 5.0
DEFAULT_FILL_SOURCES = ("polygon_ws",)
TAIL_OR_WS_FILL_SOURCES = ("polygon_ws", "polygon_http_getLogs_tail")


@dataclass(frozen=True)
class FillObservation:
    wallet: str
    tx: str
    asset_id: str
    side: str
    price: float
    block_ts: float
    size: float | None = None
    source: str = "polygon_ws"
    market_slug: str = ""
    condition_id: str = ""


@dataclass(frozen=True)
class MarketPoint:
    asset_id: str
    ts: float
    mid: float
    best_bid: float | None = None
    best_ask: float | None = None


@dataclass(frozen=True)
class ExecutionProfileConfig:
    latency_horizon_s: float = 2.0
    min_fills: int = 20
    min_positive_edge_fraction: float = 0.70
    min_mean_edge: float = 0.0
    min_median_edge: float = 0.0
    max_observation_lag_s: float = DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _midpoint(best_bid: Any, best_ask: Any) -> tuple[float | None, float | None, float | None]:
    bid = _float(best_bid)
    ask = _float(best_ask)
    if bid is None or ask is None:
        return None, bid, ask
    return (bid + ask) / 2.0, bid, ask


def _row_ts(row: dict[str, Any]) -> float | None:
    timestamp = row.get("timestamp")
    if timestamp is not None:
        try:
            value = float(timestamp)
        except (TypeError, ValueError):
            value = 0.0
        if value > 10_000_000_000:
            return value / 1000.0
        if value > 0:
            return value
    return _float(row.get("captured_at_s"))


def _is_tail_or_ws_fill_source(source: str) -> bool:
    text = str(source or "")
    return text == "polygon_ws" or text.endswith("_tail")


def _btc_5m_window_start_s(market_slug: str) -> float | None:
    slug = str(market_slug or "")
    if not slug.startswith("btc-updown-5m-"):
        return None
    marker = slug.rsplit("-", 1)[-1]
    if not marker.isdigit():
        return None
    return float(marker)


def _seconds_from_open(*, market_slug: str, event_ts: float | None) -> float | None:
    start = _btc_5m_window_start_s(market_slug)
    if start is None or event_ts is None:
        return None
    return max(0.0, min(300.0, float(event_ts) - start))


def _seconds_bucket(seconds_from_open: float | None) -> str:
    if seconds_from_open is None:
        return "unknown_seconds"
    value = max(0.0, min(299.999999, float(seconds_from_open)))
    start = int(value // 60) * 60
    end = start + 60
    return f"{start:03d}-{end:03d}"


def _entry_price_band(price: float | None) -> str:
    if price is None:
        return "unknown_price"
    value = float(price)
    if value <= 0.25:
        return "<=0.25"
    if value <= 0.50:
        return "0.25-0.50"
    if value <= 0.75:
        return "0.50-0.75"
    return ">0.75"


def btc_5m_move_slice_for_values(
    *,
    market_slug: str,
    event_ts: float | None,
    price: float | None,
) -> dict[str, Any]:
    """Classify a BTC-5m move by timing and entry band for LEARN gating."""

    seconds = _seconds_from_open(market_slug=market_slug, event_ts=event_ts)
    seconds_bucket = _seconds_bucket(seconds)
    price_band = _entry_price_band(price)
    key = f"{seconds_bucket}|{price_band}"
    return {
        "market_type": "btc_5m" if _btc_5m_window_start_s(market_slug) is not None else "unknown",
        "seconds_from_open": None if seconds is None else round(seconds, 6),
        "seconds_bucket": seconds_bucket,
        "entry_price_band": price_band,
        "move_slice_key": key,
    }


def polygon_fill_observations(
    rows: list[dict[str, Any]],
    *,
    sources: tuple[str, ...] | list[str] | set[str] | None = None,
    asset_context: dict[str, dict[str, Any]] | None = None,
) -> list[FillObservation]:
    allowed_sources = tuple(str(item) for item in (sources or DEFAULT_FILL_SOURCES) if str(item))
    source_priority = {source: index for index, source in enumerate(allowed_sources)}
    fills_by_key: dict[tuple[str, str, str], FillObservation] = {}
    priorities_by_key: dict[tuple[str, str, str], int] = {}
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        source = str(row.get("source") or "")
        if row.get("event") != "polygon_orderfilled_log" or source not in source_priority:
            continue
        decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
        asset_id = str(decoded.get("asset") or "")
        context = (asset_context or {}).get(asset_id)
        context = context if isinstance(context, dict) else {}
        side = str(decoded.get("side") or "").upper()
        tx = str(row.get("transaction_hash") or row.get("transactionHash") or "").lower()
        price = _float(decoded.get("price"))
        block_ts = _float(row.get("block_ts"))
        if not asset_id or side not in {"BUY", "SELL"} or not tx or price is None or block_ts is None:
            continue
        market_slug = str(
            row.get("market_slug")
            or row.get("marketSlug")
            or decoded.get("market_slug")
            or decoded.get("marketSlug")
            or context.get("market_slug")
            or ""
        )
        condition_id = str(
            row.get("condition_id")
            or row.get("conditionId")
            or decoded.get("condition_id")
            or decoded.get("conditionId")
            or context.get("condition_id")
            or ""
        )
        wallets = [_wallet(row.get("selected_wallet"))]
        wallets.extend(_wallet(wallet) for wallet in (row.get("registry_wallets") or []))
        for wallet in [item for item in dict.fromkeys(wallets) if item]:
            key = (wallet, tx, asset_id)
            priority = int(source_priority[source])
            if key in seen and priority >= priorities_by_key.get(key, priority):
                continue
            seen.add(key)
            priorities_by_key[key] = priority
            fills_by_key[key] = FillObservation(
                wallet=wallet,
                tx=tx,
                asset_id=asset_id,
                side=side,
                price=price,
                block_ts=block_ts,
                size=_float(decoded.get("size")),
                source=source,
                market_slug=market_slug,
                condition_id=condition_id,
            )
    return list(fills_by_key.values())


def clob_market_points(rows: list[dict[str, Any]]) -> dict[str, list[MarketPoint]]:
    points_by_asset: dict[str, list[MarketPoint]] = defaultdict(list)
    for row in rows:
        event_type = row.get("event_type") or row.get("type") or row.get("event")
        ts = _row_ts(row)
        if ts is None:
            continue
        if event_type == "price_change":
            for change in row.get("price_changes") or []:
                if not isinstance(change, dict):
                    continue
                asset_id = str(change.get("asset_id") or "")
                mid, bid, ask = _midpoint(change.get("best_bid"), change.get("best_ask"))
                if asset_id and mid is not None:
                    points_by_asset[asset_id].append(MarketPoint(asset_id=asset_id, ts=ts, mid=mid, best_bid=bid, best_ask=ask))
            continue
        asset_id = str(row.get("asset_id") or "")
        if not asset_id:
            continue
        if event_type == "book":
            bids = [_float(item.get("price")) for item in (row.get("bids") or []) if isinstance(item, dict)]
            asks = [_float(item.get("price")) for item in (row.get("asks") or []) if isinstance(item, dict)]
            bids_clean = [item for item in bids if item is not None]
            asks_clean = [item for item in asks if item is not None]
            if not bids_clean or not asks_clean:
                continue
            bid = max(bids_clean)
            ask = min(asks_clean)
            points_by_asset[asset_id].append(MarketPoint(asset_id=asset_id, ts=ts, mid=(bid + ask) / 2.0, best_bid=bid, best_ask=ask))
            continue
        mid, bid, ask = _midpoint(row.get("best_bid"), row.get("best_ask"))
        if mid is not None:
            points_by_asset[asset_id].append(MarketPoint(asset_id=asset_id, ts=ts, mid=mid, best_bid=bid, best_ask=ask))
    for asset_id, points in list(points_by_asset.items()):
        points_by_asset[asset_id] = sorted(points, key=lambda point: point.ts)
    return dict(points_by_asset)


def clob_book_source_diagnostics(rows: list[dict[str, Any]], *, fill_assets: set[str] | None = None) -> dict[str, Any]:
    """Summarize raw book-capture rows that may not produce midpoints."""

    fill_asset_set = {str(item) for item in (fill_assets or set()) if str(item)}
    event_counts: Counter[str] = Counter()
    point_assets: set[str] = set()
    unavailable_assets: set[str] = set()
    empty_book_truth_assets: set[str] = set()
    error_assets: set[str] = set()
    for row in rows:
        event_type = str(row.get("event_type") or row.get("type") or row.get("event") or "")
        if not event_type:
            continue
        event_counts[event_type] += 1
        asset_id = str(row.get("asset_id") or "")
        if not asset_id:
            continue
        if event_type in {"best_bid_ask", "book", "price_change"}:
            point_assets.add(asset_id)
        elif event_type == "clob_book_snapshot_unavailable":
            unavailable_assets.add(asset_id)
            route_report = row.get("route_report") if isinstance(row.get("route_report"), dict) else {}
            if row.get("empty_book_truth") or route_report.get("empty_book_truth"):
                empty_book_truth_assets.add(asset_id)
        elif event_type == "clob_book_snapshot_error":
            error_assets.add(asset_id)
    return {
        "event_type_counts": dict(sorted(event_counts.items())),
        "point_assets": len(point_assets),
        "unavailable_assets": len(unavailable_assets),
        "empty_book_truth_assets": len(empty_book_truth_assets),
        "error_assets": len(error_assets),
        "point_asset_ids": sorted(point_assets)[:100],
        "unavailable_asset_ids": sorted(unavailable_assets)[:100],
        "empty_book_truth_asset_ids": sorted(empty_book_truth_assets)[:100],
        "error_asset_ids": sorted(error_assets)[:100],
        "overlapping_point_fill_assets": len(point_assets.intersection(fill_asset_set)),
        "overlapping_unavailable_fill_assets": len(unavailable_assets.intersection(fill_asset_set)),
        "overlapping_empty_book_truth_fill_assets": len(empty_book_truth_assets.intersection(fill_asset_set)),
        "overlapping_error_fill_assets": len(error_assets.intersection(fill_asset_set)),
    }


def _stats(values: list[float]) -> dict[str, Any]:
    clean = sorted(values)
    if not clean:
        return {"count": 0}
    return {
        "count": len(clean),
        "min": clean[0],
        "p10": clean[min(len(clean) - 1, int(len(clean) * 0.1))],
        "p50": statistics.median(clean),
        "mean": statistics.fmean(clean),
        "p90": clean[min(len(clean) - 1, int(len(clean) * 0.9))],
        "max": clean[-1],
    }


def _horizon_key(horizon_s: float) -> str:
    return f"{float(horizon_s):g}s"


def _unwrap_alpha_decay_report(report: dict[str, Any]) -> dict[str, Any]:
    alpha_decay = report.get("alpha_decay") if isinstance(report.get("alpha_decay"), dict) else report
    return alpha_decay if isinstance(alpha_decay, dict) else {}


def _capture_window_report(
    fills: list[FillObservation],
    market_points_by_asset: dict[str, list[MarketPoint]],
    *,
    max_horizon_s: float,
) -> dict[str, Any]:
    fill_times = sorted(float(fill.block_ts) for fill in fills if fill.block_ts is not None)
    book_times = sorted(float(point.ts) for points in market_points_by_asset.values() for point in points)
    report: dict[str, Any] = {
        "flow_stage": "LEARN",
        "join_key": "wall_clock_overlap_between_fill_capture_and_book_capture",
        "fill_count": len(fill_times),
        "book_point_count": len(book_times),
        "fill_min_ts": fill_times[0] if fill_times else None,
        "fill_max_ts": fill_times[-1] if fill_times else None,
        "fill_horizon_max_ts": (fill_times[-1] + float(max_horizon_s)) if fill_times else None,
        "max_horizon_s": round(float(max_horizon_s), 6),
        "book_min_ts": book_times[0] if book_times else None,
        "book_max_ts": book_times[-1] if book_times else None,
        "overlap_s": None,
        "gap_s": None,
    }
    if not fill_times or not book_times:
        report["status"] = "INSUFFICIENT_WINDOW_EVIDENCE"
        return report
    overlap_start = max(fill_times[0], book_times[0])
    overlap_end = min(fill_times[-1] + float(max_horizon_s), book_times[-1])
    overlap_s = overlap_end - overlap_start
    if overlap_s < 0:
        report["status"] = "INVALID_CAPTURE_WINDOW_MISMATCH"
        report["overlap_s"] = 0.0
        report["gap_s"] = round(abs(overlap_s), 6)
        return report
    report["status"] = "PASS"
    report["overlap_s"] = round(overlap_s, 6)
    report["gap_s"] = 0.0
    return report


def _horizon_metric_lists(
    rows: list[dict[str, Any]],
    *,
    horizon_key: str,
    max_observation_lag_s: float,
) -> dict[str, list[float]]:
    edges = [
        float(result["edge"])
        for row in rows
        if isinstance((result := row["horizons"].get(horizon_key)), dict) and result.get("edge") is not None
    ]
    observation_lags = [
        float(result["observation_lag_s"])
        for row in rows
        if isinstance((result := row["horizons"].get(horizon_key)), dict)
        and result.get("observation_lag_s") is not None
    ]
    timely_edges = [
        float(result["edge"])
        for row in rows
        if isinstance((result := row["horizons"].get(horizon_key)), dict)
        and result.get("edge") is not None
        and result.get("observation_lag_s") is not None
        and float(result["observation_lag_s"]) <= float(max_observation_lag_s)
    ]
    return {
        "edges": edges,
        "observation_lags": observation_lags,
        "timely_edges": timely_edges,
    }


def _horizon_summary(
    rows: list[dict[str, Any]],
    *,
    horizon_key: str,
    fills_denominator: int,
    max_observation_lag_s: float = DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S,
) -> dict[str, Any]:
    metrics = _horizon_metric_lists(
        rows,
        horizon_key=horizon_key,
        max_observation_lag_s=max_observation_lag_s,
    )
    edges = metrics["edges"]
    timely_edges = metrics["timely_edges"]
    return {
        "coverage": len(edges),
        "coverage_fraction": (len(edges) / fills_denominator) if fills_denominator else 0.0,
        "edge": _stats(edges),
        "observation_lag_s": _stats(metrics["observation_lags"]),
        "positive_edge_fraction": (sum(1 for edge in edges if edge > 0) / len(edges)) if edges else 0.0,
        "timely_max_observation_lag_s": max_observation_lag_s,
        "timely_coverage": len(timely_edges),
        "timely_coverage_fraction": (len(timely_edges) / fills_denominator) if fills_denominator else 0.0,
        "timely_edge": _stats(timely_edges),
        "timely_positive_edge_fraction": (
            sum(1 for edge in timely_edges if edge > 0) / len(timely_edges)
        )
        if timely_edges
        else 0.0,
    }


def _move_slice_summaries(
    rows: list[dict[str, Any]],
    *,
    horizons_s: tuple[float, ...],
) -> dict[str, dict[str, Any]]:
    by_horizon: dict[str, dict[str, Any]] = {}
    slice_keys = sorted(
        {
            str(row.get("move_slice_key") or "")
            for row in rows
            if str(row.get("move_slice_key") or "")
            and str(row.get("move_slice_key") or "") != "unknown_seconds|unknown_price"
        }
    )
    for horizon in horizons_s:
        horizon_key = _horizon_key(horizon)
        horizon_slices: dict[str, Any] = {}
        for slice_key in slice_keys:
            slice_rows = [row for row in rows if str(row.get("move_slice_key") or "") == slice_key]
            if not slice_rows:
                continue
            first = slice_rows[0]
            summary = _horizon_summary(
                slice_rows,
                horizon_key=horizon_key,
                fills_denominator=len(slice_rows),
                max_observation_lag_s=DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S,
            )
            horizon_slices[slice_key] = {
                "flow_stage": "LEARN",
                "horizon_key": horizon_key,
                "seconds_bucket": first.get("seconds_bucket"),
                "entry_price_band": first.get("entry_price_band"),
                "move_slice_key": slice_key,
                "fills_with_any_coverage": len(slice_rows),
                **summary,
            }
        by_horizon[horizon_key] = horizon_slices
    return by_horizon


def _profile_blockers(
    *,
    fill_sample: int,
    raw_coverage: int,
    positive_fraction: float,
    mean_edge: float | None,
    median_edge: float | None,
    config: ExecutionProfileConfig,
    prefix: str,
) -> list[str]:
    blockers: list[str] = []
    if raw_coverage > 0 and fill_sample <= 0 and float(config.max_observation_lag_s) > 0.0:
        blockers.append(f"{prefix}_timely_book_coverage_missing")
    if fill_sample < int(config.min_fills):
        blockers.append(f"{prefix}_fill_sample_below_threshold")
    if positive_fraction < float(config.min_positive_edge_fraction):
        blockers.append(f"{prefix}_copyable_rate_below_threshold")
    if mean_edge is None or mean_edge <= float(config.min_mean_edge):
        blockers.append(f"{prefix}_mean_edge_not_positive")
    if median_edge is None or median_edge <= float(config.min_median_edge):
        blockers.append(f"{prefix}_median_edge_not_positive")
    return blockers


def _slice_execution_profile(
    *,
    wallet: str,
    slice_key: str,
    raw: dict[str, Any],
    config: ExecutionProfileConfig,
    horizon_key: str,
) -> dict[str, Any]:
    if float(config.max_observation_lag_s) > 0.0:
        edge = raw.get("timely_edge") if isinstance(raw.get("timely_edge"), dict) else {}
        fill_sample = int(raw.get("timely_coverage") or 0)
        positive_fraction = _float(raw.get("timely_positive_edge_fraction"))
    else:
        edge = raw.get("edge") if isinstance(raw.get("edge"), dict) else {}
        fill_sample = int(raw.get("coverage") or 0)
        positive_fraction = _float(raw.get("positive_edge_fraction"))
    if positive_fraction is None:
        positive_fraction = 0.0
    raw_coverage = int(raw.get("coverage") or 0)
    mean_edge = _float(edge.get("mean"))
    median_edge = _float(edge.get("p50"))
    blockers = _profile_blockers(
        fill_sample=fill_sample,
        raw_coverage=raw_coverage,
        positive_fraction=positive_fraction,
        mean_edge=mean_edge,
        median_edge=median_edge,
        config=config,
        prefix="execution_move_slice",
    )
    eligible = not blockers
    return {
        "flow_stage": "LEARN",
        "scope": "wallet_move_slice",
        "wallet": wallet,
        "status": "PASS" if eligible else ("ANALYZE" if fill_sample else "MISSING"),
        "eligible": eligible,
        "latency_horizon_s": round(float(config.latency_horizon_s), 6),
        "horizon_key": horizon_key,
        "move_slice_key": slice_key,
        "seconds_bucket": raw.get("seconds_bucket"),
        "entry_price_band": raw.get("entry_price_band"),
        "fill_sample": fill_sample,
        "raw_fill_coverage": raw_coverage,
        "min_fills": int(config.min_fills),
        "copyable_rate_pct": round(float(positive_fraction) * 100.0, 6),
        "min_copyable_rate_pct": round(float(config.min_positive_edge_fraction) * 100.0, 6),
        "mean_edge": round(float(mean_edge), 9) if mean_edge is not None else None,
        "median_edge": round(float(median_edge), 9) if median_edge is not None else None,
        "max_observation_lag_s": round(float(config.max_observation_lag_s), 6),
        "stale_or_missing_book_observations": max(0, raw_coverage - fill_sample),
        "edge_stats": edge,
        "blockers": blockers,
    }


def build_execution_profiles(
    alpha_decay_report: dict[str, Any],
    *,
    config: ExecutionProfileConfig | None = None,
) -> dict[str, Any]:
    """Convert alpha-decay edges into per-wallet copyability profiles.

    Profiles are evidence only. A wallet is profile-eligible only when it has a
    sufficiently large sample whose edge remains positive at the configured
    detection/execution latency.
    """

    cfg = config or ExecutionProfileConfig()
    report = _unwrap_alpha_decay_report(alpha_decay_report)
    horizon_key = _horizon_key(cfg.latency_horizon_s)
    per_wallet = report.get("per_wallet") if isinstance(report.get("per_wallet"), dict) else {}
    profiles: list[dict[str, Any]] = []
    for wallet, raw in sorted(per_wallet.items()):
        address = _wallet(wallet)
        if not address or not isinstance(raw, dict):
            continue
        horizons = raw.get("horizons") if isinstance(raw.get("horizons"), dict) else {}
        horizon = horizons.get(horizon_key) if isinstance(horizons.get(horizon_key), dict) else {}
        if float(cfg.max_observation_lag_s) > 0.0:
            edge = horizon.get("timely_edge") if isinstance(horizon.get("timely_edge"), dict) else {}
            fill_sample = int(horizon.get("timely_coverage") or 0)
            positive_fraction = _float(horizon.get("timely_positive_edge_fraction"))
        else:
            edge = horizon.get("edge") if isinstance(horizon.get("edge"), dict) else {}
            fill_sample = int(horizon.get("coverage") or 0)
            positive_fraction = _float(horizon.get("positive_edge_fraction"))
        if positive_fraction is None:
            positive_fraction = 0.0
        raw_coverage = int(horizon.get("coverage") or 0)
        mean_edge = _float(edge.get("mean"))
        median_edge = _float(edge.get("p50"))
        blockers = _profile_blockers(
            fill_sample=fill_sample,
            raw_coverage=raw_coverage,
            positive_fraction=positive_fraction,
            mean_edge=mean_edge,
            median_edge=median_edge,
            config=cfg,
            prefix="execution_profile",
        )
        eligible = not blockers
        slice_rows = (
            raw.get("move_slices", {})
            if isinstance(raw.get("move_slices"), dict)
            else {}
        ).get(horizon_key, {})
        slice_profiles = [
            _slice_execution_profile(
                wallet=address,
                slice_key=str(slice_key),
                raw=slice_raw,
                config=cfg,
                horizon_key=horizon_key,
            )
            for slice_key, slice_raw in sorted((slice_rows if isinstance(slice_rows, dict) else {}).items())
            if isinstance(slice_raw, dict)
        ]
        slice_profiles = sorted(
            slice_profiles,
            key=lambda row: (
                0 if row.get("eligible") else 1,
                -float(row.get("mean_edge") or -1_000_000_000.0),
                -float(row.get("copyable_rate_pct") or 0.0),
                -int(row.get("fill_sample") or 0),
                str(row.get("move_slice_key") or ""),
            ),
        )
        eligible_slices = [row for row in slice_profiles if row.get("eligible")]
        profiles.append(
            {
                "flow_stage": "LEARN",
                "scope": "wallet",
                "wallet": address,
                "status": "PASS" if eligible else ("ANALYZE" if fill_sample else "MISSING"),
                "eligible": eligible,
                "latency_horizon_s": round(float(cfg.latency_horizon_s), 6),
                "horizon_key": horizon_key,
                "fill_sample": fill_sample,
                "raw_fill_coverage": raw_coverage,
                "min_fills": int(cfg.min_fills),
                "copyable_rate_pct": round(float(positive_fraction) * 100.0, 6),
                "min_copyable_rate_pct": round(float(cfg.min_positive_edge_fraction) * 100.0, 6),
                "mean_edge": round(float(mean_edge), 9) if mean_edge is not None else None,
                "median_edge": round(float(median_edge), 9) if median_edge is not None else None,
                "max_observation_lag_s": round(float(cfg.max_observation_lag_s), 6),
                "stale_or_missing_book_observations": max(0, raw_coverage - fill_sample),
                "edge_stats": edge,
                "eligible_move_slice_count": len(eligible_slices),
                "best_eligible_move_slice": eligible_slices[0] if eligible_slices else {},
                "move_slices": slice_profiles,
                "move_slices_by_key": {str(row["move_slice_key"]): row for row in slice_profiles},
                "blockers": blockers,
            }
        )
    profiles = sorted(
        profiles,
        key=lambda row: (
            0 if row.get("eligible") else 1,
            -float(row.get("mean_edge") or -1_000_000_000.0),
            -float(row.get("copyable_rate_pct") or 0.0),
            -int(row.get("fill_sample") or 0),
            str(row.get("wallet") or ""),
        ),
    )
    eligible_count = sum(1 for row in profiles if row.get("eligible"))
    eligible_move_slice_count = sum(int(row.get("eligible_move_slice_count") or 0) for row in profiles)
    blockers: list[str] = []
    alpha_blockers = report.get("blockers") if isinstance(report.get("blockers"), list) else []
    for blocker in alpha_blockers:
        text = str(blocker or "")
        if text and text not in blockers:
            blockers.append(text)
    if not profiles:
        if "alpha_decay_profile_coverage_missing" not in blockers:
            blockers.append("alpha_decay_profile_coverage_missing")
    if profiles and eligible_count <= 0:
        blockers.append("no_execution_profile_positive_at_latency")
    return {
        "schema_version": 1,
        "kind": "wallet_copy_execution_profiles",
        "flow_stage": "LEARN",
        "status": "PASS" if eligible_count else "ANALYZE",
        "latency_horizon_s": round(float(cfg.latency_horizon_s), 6),
        "horizon_key": horizon_key,
        "config": cfg.asdict(),
        "alpha_decay_status": report.get("status"),
        "fills_total": int(report.get("fills_total") or 0),
        "fills_with_any_book_coverage": int(report.get("fills_with_any_book_coverage") or 0),
        "profile_count": len(profiles),
        "eligible_profile_count": eligible_count,
        "eligible_move_slice_count": eligible_move_slice_count,
        "profiles": profiles,
        "profiles_by_wallet": {str(row["wallet"]): row for row in profiles},
        "blockers": blockers,
        "next_action": (
            "use eligible profiles as promotion candidates"
            if eligible_count
            else str(report.get("next_action") or "capture CLOB book coverage for active fill assets until profiles can be assessed")
        ),
    }


def build_alpha_decay_report(
    fills: list[FillObservation],
    market_points_by_asset: dict[str, list[MarketPoint]],
    *,
    horizons_s: tuple[float, ...] = DEFAULT_HORIZONS_S,
    sample_limit: int = 500,
    book_source_diagnostics: dict[str, Any] | None = None,
    fill_source_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    point_ts_by_asset = {asset: [point.ts for point in points] for asset, points in market_points_by_asset.items()}
    rows: list[dict[str, Any]] = []
    fill_assets = {fill.asset_id for fill in fills}
    fill_source_counts = Counter(fill.source for fill in fills)
    book_assets = set(market_points_by_asset)
    capture_windows = _capture_window_report(
        fills,
        market_points_by_asset,
        max_horizon_s=max(horizons_s) if horizons_s else 0.0,
    )
    overlapping_assets = fill_assets.intersection(book_assets)
    fills_on_book_assets = sum(1 for fill in fills if fill.asset_id in overlapping_assets)
    missing_assets = Counter(fill.asset_id for fill in fills if fill.asset_id not in market_points_by_asset)
    diagnostics = book_source_diagnostics if isinstance(book_source_diagnostics, dict) else {}
    source_diagnostics = fill_source_diagnostics if isinstance(fill_source_diagnostics, dict) else {}
    tail_or_ws_fill_count = sum(count for source, count in fill_source_counts.items() if _is_tail_or_ws_fill_source(source))
    if fills and tail_or_ws_fill_count <= 0:
        return {
            "status": "FILL_SOURCE_SEED_ONLY",
            "blockers": ["alpha_decay_fill_source_seed_only"],
            "next_action": "rerun report with tail/ws fill rows overlapping the CLOB book window; seed-only HTTP getLogs rows are not execution-profile evidence",
            "horizons_s": list(horizons_s),
            "fills_total": len(fills),
            "fill_source_counts": dict(sorted(fill_source_counts.items())),
            "tail_or_ws_fill_count": 0,
            "fills_with_any_book_coverage": 0,
            "unique_fill_assets": len(fill_assets),
            "unique_book_assets": len(market_points_by_asset),
            "overlapping_fill_book_assets": len(overlapping_assets),
            "fills_on_book_assets": fills_on_book_assets,
            "capture_windows": capture_windows,
            "move_slice_counts": {},
            "market_type_counts": {},
            "book_source_diagnostics": diagnostics,
            "fill_source_diagnostics": source_diagnostics,
            "coverage_by_horizon": {},
            "per_wallet": {},
            "missing_assets_top": [{"asset_id": asset, "fills": count} for asset, count in missing_assets.most_common(100)],
            "sample_rows": [],
            "join_design": {
                "fill_source": "configured_polygon_orderfilled_rows",
                "fill_source_note": "polygon_http_getLogs seed rows are LEARN/OBSERVE discovery evidence only; execution-profile promotion evidence requires polygon_ws or tail fill rows.",
                "book_source": "clob_market_ws_best_bid_ask_price_change_book_rows",
                "join_key": "asset_id",
                "time_rule": "first_book_observation_at_or_after_fill_block_ts_plus_horizon",
                "timely_profile_observation_lag_s": DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S,
                "edge_rule": "BUY: future_mid-fill_price; SELL: fill_price-future_mid",
            },
        }
    covered_fills = 0
    for fill in fills:
        points = market_points_by_asset.get(fill.asset_id) or []
        point_ts = point_ts_by_asset.get(fill.asset_id) or []
        if not points:
            continue
        horizon_results: dict[str, Any] = {}
        has_any = False
        for horizon in horizons_s:
            target_ts = fill.block_ts + horizon
            index = bisect.bisect_left(point_ts, target_ts)
            key = f"{horizon:g}s"
            if index >= len(points):
                horizon_results[key] = {"status": "MISSING_AFTER_HORIZON"}
                continue
            point = points[index]
            edge = point.mid - fill.price if fill.side == "BUY" else fill.price - point.mid
            horizon_results[key] = {
                "target_ts": target_ts,
                "observed_ts": point.ts,
                "observation_lag_s": point.ts - target_ts,
                "mid": point.mid,
                "edge": edge,
            }
            has_any = True
        if has_any:
            covered_fills += 1
            move_slice = btc_5m_move_slice_for_values(
                market_slug=fill.market_slug,
                event_ts=fill.block_ts,
                price=fill.price,
            )
            rows.append(
                {
                    "wallet": fill.wallet,
                    "tx": fill.tx,
                    "asset_id": fill.asset_id,
                    "condition_id": fill.condition_id,
                    "market_slug": fill.market_slug,
                    "side": fill.side,
                    "fill_price": fill.price,
                    "block_ts": fill.block_ts,
                    "size": fill.size,
                    "fill_source": fill.source,
                    **move_slice,
                    "horizons": horizon_results,
                }
            )

    per_wallet: dict[str, Any] = {}
    for wallet in sorted({row["wallet"] for row in rows}):
        wallet_rows = [row for row in rows if row["wallet"] == wallet]
        horizon_stats: dict[str, Any] = {}
        for horizon in horizons_s:
            key = f"{horizon:g}s"
            edges = [
                float(result["edge"])
                for row in wallet_rows
                if isinstance((result := row["horizons"].get(key)), dict) and result.get("edge") is not None
            ]
            observation_lags = [
                float(result["observation_lag_s"])
                for row in wallet_rows
                if isinstance((result := row["horizons"].get(key)), dict)
                and result.get("observation_lag_s") is not None
            ]
            timely_edges = [
                float(result["edge"])
                for row in wallet_rows
                if isinstance((result := row["horizons"].get(key)), dict)
                and result.get("edge") is not None
                and result.get("observation_lag_s") is not None
                and float(result["observation_lag_s"]) <= DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S
            ]
            horizon_stats[key] = {
                "coverage": len(edges),
                "edge": _stats(edges),
                "observation_lag_s": _stats(observation_lags),
                "positive_edge_fraction": (sum(1 for edge in edges if edge > 0) / len(edges)) if edges else 0.0,
                "timely_max_observation_lag_s": DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S,
                "timely_coverage": len(timely_edges),
                "timely_edge": _stats(timely_edges),
                "timely_positive_edge_fraction": (
                    sum(1 for edge in timely_edges if edge > 0) / len(timely_edges)
                )
                if timely_edges
                else 0.0,
            }
        per_wallet[wallet] = {
            "fills_with_any_coverage": len(wallet_rows),
            "fill_sources": dict(sorted(Counter(str(row.get("fill_source") or "") for row in wallet_rows).items())),
            "horizons": horizon_stats,
            "move_slices": _move_slice_summaries(wallet_rows, horizons_s=horizons_s),
        }

    coverage_by_horizon: dict[str, Any] = {}
    for horizon in horizons_s:
        key = f"{horizon:g}s"
        edges = [
            float(result["edge"])
            for row in rows
            if isinstance((result := row["horizons"].get(key)), dict) and result.get("edge") is not None
        ]
        observation_lags = [
            float(result["observation_lag_s"])
            for row in rows
            if isinstance((result := row["horizons"].get(key)), dict)
            and result.get("observation_lag_s") is not None
        ]
        timely_edges = [
            float(result["edge"])
            for row in rows
            if isinstance((result := row["horizons"].get(key)), dict)
            and result.get("edge") is not None
            and result.get("observation_lag_s") is not None
            and float(result["observation_lag_s"]) <= DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S
        ]
        coverage_by_horizon[key] = {
            "coverage": len(edges),
            "coverage_fraction": (len(edges) / len(fills)) if fills else 0.0,
            "edge": _stats(edges),
            "observation_lag_s": _stats(observation_lags),
            "positive_edge_fraction": (sum(1 for edge in edges if edge > 0) / len(edges)) if edges else 0.0,
            "timely_max_observation_lag_s": DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S,
            "timely_coverage": len(timely_edges),
            "timely_coverage_fraction": (len(timely_edges) / len(fills)) if fills else 0.0,
            "timely_edge": _stats(timely_edges),
            "timely_positive_edge_fraction": (
                sum(1 for edge in timely_edges if edge > 0) / len(timely_edges)
            )
            if timely_edges
            else 0.0,
        }

    blockers: list[str] = []
    if fills and book_assets and not overlapping_assets:
        blockers.append("alpha_decay_fill_book_asset_overlap_missing")
    if fills and not book_assets:
        blockers.append("alpha_decay_book_source_empty")
    if int(diagnostics.get("overlapping_empty_book_truth_fill_assets") or 0) > 0:
        blockers.append("alpha_decay_fill_assets_empty_book_truth")
    if capture_windows.get("status") == "INVALID_CAPTURE_WINDOW_MISMATCH":
        blockers.append("alpha_decay_capture_windows_do_not_overlap")
    if not covered_fills:
        blockers.append("alpha_decay_profile_coverage_missing")

    next_action = "use alpha-decay report for execution profiles"
    if "alpha_decay_capture_windows_do_not_overlap" in blockers:
        next_action = "rerun simultaneous wallet-fill and CLOB-book capture in the same wall-clock window"
    elif "alpha_decay_fill_assets_empty_book_truth" in blockers:
        next_action = "filter empty-book assets from selection and keep simultaneous capture on liquid active assets"
    elif "alpha_decay_fill_book_asset_overlap_missing" in blockers:
        next_action = "capture simultaneous wallet fills and CLOB books for the same active asset ids"
    elif "alpha_decay_book_source_empty" in blockers:
        next_action = "repair CLOB book capture before rerunning alpha-decay"
    elif "alpha_decay_profile_coverage_missing" in blockers:
        next_action = "capture CLOB book coverage for active fill assets until profiles can be assessed"

    return {
        "status": (
            "INVALID_CAPTURE_WINDOW_MISMATCH"
            if "alpha_decay_capture_windows_do_not_overlap" in blockers
            else "PASS"
            if covered_fills
            else "INSUFFICIENT_BOOK_COVERAGE"
        ),
        "blockers": blockers,
        "next_action": next_action,
        "horizons_s": list(horizons_s),
        "fills_total": len(fills),
        "fill_source_counts": dict(sorted(fill_source_counts.items())),
        "fills_with_any_book_coverage": covered_fills,
        "unique_fill_assets": len(fill_assets),
        "unique_book_assets": len(market_points_by_asset),
        "overlapping_fill_book_assets": len(overlapping_assets),
        "fills_on_book_assets": fills_on_book_assets,
        "capture_windows": capture_windows,
        "move_slice_counts": dict(sorted(Counter(str(row.get("move_slice_key") or "unknown") for row in rows).items())),
        "market_type_counts": dict(sorted(Counter(str(row.get("market_type") or "unknown") for row in rows).items())),
        "book_source_diagnostics": diagnostics,
        "fill_source_diagnostics": source_diagnostics,
        "coverage_by_horizon": coverage_by_horizon,
        "per_wallet": per_wallet,
        "missing_assets_top": [{"asset_id": asset, "fills": count} for asset, count in missing_assets.most_common(100)],
        "sample_rows": rows[-max(0, int(sample_limit)):],
        "join_design": {
            "fill_source": "configured_polygon_orderfilled_rows",
            "fill_source_note": "polygon_http_getLogs rows are LEARN/OBSERVE seed/backfill evidence only; promotion still requires positive execution-profile metrics at the configured latency horizon.",
            "book_source": "clob_market_ws_best_bid_ask_price_change_book_rows",
            "join_key": "asset_id",
            "time_rule": "first_book_observation_at_or_after_fill_block_ts_plus_horizon",
            "timely_profile_observation_lag_s": DEFAULT_PROFILE_MAX_OBSERVATION_LAG_S,
            "edge_rule": "BUY: future_mid-fill_price; SELL: fill_price-future_mid",
        },
    }
