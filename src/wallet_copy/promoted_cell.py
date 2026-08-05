"""Deterministic promotion reducer for isolated cross-exchange paper cells."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .fees import expected_polymarket_buy_fee_usd
from .performance import load_resolutions, score_order
from .store import load_json


def _checksum(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _resolved_rows(cell: dict[str, Any], state: dict[str, Any], resolutions: dict[str, Any]) -> list[dict[str, Any]]:
    mode = str(cell.get("execution_mode") or "")
    if mode == "taker":
        terminals_path = str(cell.get("terminals_path") or "")
        rows: list[dict[str, Any]] = []
        if terminals_path and Path(terminals_path).exists():
            for line in Path(terminals_path).read_text(encoding="utf-8", errors="ignore").splitlines():
                try:
                    terminal = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(terminal, dict) or terminal.get("terminal_status") != "SIGNAL":
                    continue
                intent = terminal.get("intent") if isinstance(terminal.get("intent"), dict) else {}
                scored = score_order(
                    {
                        **intent,
                        "status": "FILLED",
                        "filled_size_usd": intent.get("copy_size_usd"),
                        "filled_shares": intent.get("shares"),
                    },
                    resolutions,
                )
                if scored.get("resolved"):
                    fee = expected_polymarket_buy_fee_usd(
                        shares=_num(intent.get("shares")),
                        price=_num(intent.get("limit_price")),
                    )
                    rows.append(
                        {
                            "market_slug": intent.get("market_slug"),
                            "resolved_at_order": terminal.get("recorded_at"),
                            "pnl_usd": round(_num(scored.get("pnl_usd")) - fee, 6),
                        }
                    )
        return rows

    rows = []
    events_path = str(cell.get("fill_events_path") or "")
    event_rows: list[dict[str, Any]] = []
    if events_path and Path(events_path).exists():
        for line in Path(events_path).read_text(encoding="utf-8", errors="ignore").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                event_rows.append(event)
    if cell.get("paired_bundle_required") is True:
        for bundle in event_rows:
            if (
                bundle.get("event") != "paired_bundle_terminal"
                or str(bundle.get("generation_checksum") or "") != str(cell.get("generation_checksum") or "")
                or str(bundle.get("cell_id") or "") != str(cell.get("cell_id") or "")
            ):
                continue
            legs = [row for row in bundle.get("legs") or [] if isinstance(row, dict)]
            filled = [row for row in legs if row.get("status") == "FILLED"]
            if not filled or len(legs) != 2:
                continue
            split_accounting = (
                bundle.get("split_sell_accounting")
                if isinstance(bundle.get("split_sell_accounting"), dict)
                else {}
            )
            if split_accounting:
                conservation = (
                    split_accounting.get("inventory_conservation")
                    if isinstance(split_accounting.get("inventory_conservation"), dict)
                    else {}
                )
                if (
                    len(filled) == 2
                    and bundle.get("dual_leg_fill_verified") is True
                    and int(conservation.get("disagreement") or 0) == 0
                    and _num(split_accounting.get("split_collateral_usd")) == 1.0
                ):
                    rows.append({
                        "market_slug": str(filled[0].get("market_slug") or ""),
                        "resolved_at_order": min(str(row.get("submitted_at") or "") for row in filled),
                        "pnl_usd": round(_num(split_accounting.get("realized_post_cost_pnl_usd")), 6),
                        "paired_bundle_id": bundle.get("paired_bundle_id"),
                        "dual_leg_filled": True,
                        "orphan_leg_count": 0,
                        "split_inventory_conservation_exact": True,
                    })
                continue
            bundle_pnl = 0.0
            market_slug = str(filled[0].get("market_slug") or "")
            resolved = True
            for leg in filled:
                evidence = leg.get("fill_evidence") if isinstance(leg.get("fill_evidence"), dict) else {}
                if evidence.get("verified") is not True or evidence.get("kind") != "sequence_continuous_queue_depletion":
                    resolved = False
                    break
                price = _num(leg.get("fill_price") or leg.get("limit_price"))
                size_usd = _num(leg.get("filled_size_usd") or leg.get("requested_size_usd"))
                normalized = {**leg, "filled_size_usd": size_usd, "filled_shares": size_usd / price if price > 0 else 0.0, "limit_price": price}
                scored = score_order(normalized, resolutions)
                if not scored.get("resolved"):
                    resolved = False
                    break
                bundle_pnl += _num(scored.get("pnl_usd")) - expected_polymarket_buy_fee_usd(shares=_num(normalized.get("filled_shares")), price=price)
            if resolved:
                rows.append({
                    "market_slug": market_slug,
                    "resolved_at_order": min(str(row.get("submitted_at") or "") for row in filled),
                    "pnl_usd": round(bundle_pnl, 6),
                    "paired_bundle_id": bundle.get("paired_bundle_id"),
                    "dual_leg_filled": len(filled) == 2 and bundle.get("dual_leg_fill_verified") is True,
                    "orphan_leg_count": 2 - len(filled),
                })
        return rows
    for order in event_rows:
        evidence = order.get("fill_evidence") if isinstance(order.get("fill_evidence"), dict) else {}
        if (
            order.get("event") != "passive_quote_filled"
            or order.get("status") != "FILLED"
            or evidence.get("verified") is not True
            or evidence.get("kind") != "sequence_continuous_queue_depletion"
            or str(order.get("generation_checksum") or "")
            != str(cell.get("generation_checksum") or "")
            or str(order.get("cell_id") or "") != str(cell.get("cell_id") or "")
            or str(order.get("passive_fill_model_checksum") or "")
            != str(cell.get("passive_fill_model_checksum") or "")
            or str(evidence.get("generation_checksum") or "")
            != str(cell.get("generation_checksum") or "")
            or str(evidence.get("cell_id") or "") != str(cell.get("cell_id") or "")
            or str(evidence.get("intent_id") or "") != str(order.get("intent_id") or "")
            or str(evidence.get("passive_fill_model_checksum") or "")
            != str(cell.get("passive_fill_model_checksum") or "")
            or _num(evidence.get("cumulative_verified_depletion_shares"))
            + 1e-9
            < _num(evidence.get("required_depletion_shares"))
        ):
            continue
        price = _num(order.get("fill_price") or order.get("limit_price"))
        size_usd = _num(order.get("requested_size_usd"))
        normalized = {
            **order,
            "filled_size_usd": size_usd,
            "filled_shares": size_usd / price if price > 0 else 0.0,
            "limit_price": price,
        }
        scored = score_order(normalized, resolutions)
        if scored.get("resolved"):
            fee = expected_polymarket_buy_fee_usd(
                shares=_num(normalized.get("filled_shares")),
                price=price,
            )
            rows.append(
                {
                    "market_slug": order.get("market_slug"),
                    "resolved_at_order": order.get("submitted_at"),
                    "pnl_usd": round(_num(scored.get("pnl_usd")) - fee, 6),
                }
            )
    return rows


def reduce_promoted_cells(
    matrix: dict[str, Any],
    *,
    resolutions_path: str,
    prior_activation: dict[str, Any] | None = None,
    prior_cells: list[dict[str, Any]] | None = None,
    forbidden_model_checksums: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Rank only cells satisfying the frozen emergency-forward evidence gate."""

    resolutions = load_resolutions(resolutions_path)
    prior_activation = prior_activation if isinstance(prior_activation, dict) else {}
    prior_by_id = {
        str(row.get("cell_id") or ""): row
        for row in (prior_cells or [])
        if isinstance(row, dict) and row.get("cell_id")
    }
    rows: list[dict[str, Any]] = []
    for cell in matrix.get("cells") or []:
        if not isinstance(cell, dict):
            continue
        state_path = str(cell.get("state_path") or "")
        state = load_json(state_path, default={})
        state = state if isinstance(state, dict) else {}
        resolved = _resolved_rows(cell, state, resolutions)
        midpoint = len(resolved) // 2
        first = resolved[:midpoint]
        second = resolved[midpoint:]
        pnl = round(sum(_num(row.get("pnl_usd")) for row in resolved), 6)
        first_pnl = round(sum(_num(row.get("pnl_usd")) for row in first), 6)
        second_pnl = round(sum(_num(row.get("pnl_usd")) for row in second), 6)
        requires_incremental = cell.get("requires_positive_incremental_vs_matched_cross_exchange") is True
        matched_pnl = 0.0
        if requires_incremental:
            matched_path = str(cell.get("matched_cross_exchange_terminals_path") or "")
            matched_rows = _resolved_rows(
                {"execution_mode": "taker", "terminals_path": matched_path}, {}, resolutions
            ) if matched_path else []
            resolved_markets = {str(row.get("market_slug") or "") for row in resolved}
            matched_pnl = round(
                sum(_num(row.get("pnl_usd")) for row in matched_rows if str(row.get("market_slug") or "") in resolved_markets),
                6,
            )
        prereg_path = str(cell.get("preregistration_path") or "")
        prereg = load_json(prereg_path, default={})
        prereg = prereg if isinstance(prereg, dict) else {}
        prereg_body = {key: value for key, value in prereg.items() if key != "checksum"}
        passive_mode = str(cell.get("execution_mode") or "") in {"passive", "paired_passive"}
        checks = {
            "paper_only": cell.get("paper_only") is True and state.get("live_orders_allowed") is False,
            "preregistration_checksum_exact": bool(prereg)
            and str(prereg.get("checksum") or "") == str(cell.get("preregistration_checksum") or "")
            and str(prereg.get("checksum") or "") == _checksum(prereg_body),
            "model_checksum_exact": bool(cell.get("model_checksum"))
            and str(cell.get("model_checksum")) == str(prereg.get("model_checksum") or ""),
            "distinct_generation_checksum": bool(cell.get("generation_checksum"))
            and str(cell.get("generation_checksum"))
            == str(prereg.get("generation_checksum") or ""),
            "model_checksum_not_forbidden": not any(
                str(cell.get("model_checksum") or "").startswith(checksum)
                for checksum in forbidden_model_checksums
            ),
            "resolved_gte_10": len(resolved) >= 10,
            "aggregate_post_fee_positive": pnl > 0.0,
            "first_half_positive": bool(first) and first_pnl > 0.0,
            "second_half_positive": bool(second) and second_pnl > 0.0,
            "zero_unresolved_disagreement": int(cell.get("unresolved_disagreement") or 0) == 0,
            "zero_duplicate_disagreement": int(cell.get("duplicate_disagreement") or 0) == 0,
            "zero_sign_disagreement": int(cell.get("sign_disagreement") or 0) == 0,
            "zero_parity_disagreement": int(cell.get("parity_disagreement") or 0) == 0,
            "zero_lookahead_violations": int(cell.get("lookahead_violations") or 0) == 0,
            "zero_clock_disagreement": int(cell.get("clock_disagreement") or 0) == 0,
            "actual_depth_verified": cell.get("actual_depth_verified") is True,
            "positive_incremental_vs_matched_cross_exchange": (
                not requires_incremental or (bool(resolved) and pnl > matched_pnl)
            ),
            "passive_fill_model_verified": (
                not passive_mode
                or (
                    cell.get("passive_fill_model_verified") is True
                    and bool(cell.get("passive_fill_model_checksum"))
                    and str(cell.get("passive_fill_model_checksum") or "")
                    == str(prereg.get("passive_fill_model_checksum") or "")
                )
            ),
        }
        if cell.get("paired_bundle_required") is True:
            checks.update({
                "paired_bundle_accounting_exact": int(cell.get("pair_accounting_disagreement") or 0) == 0,
                "genuine_two_leg_cycle_present": any(row.get("dual_leg_filled") is True for row in resolved),
                "orphan_losses_included": all("orphan_leg_count" in row for row in resolved),
            })
        prior_terminal_park = (
            str((prior_by_id.get(str(cell.get("cell_id") or "")) or {}).get("status") or "")
            == "TERMINAL_PARK_NEGATIVE"
        )
        checks["not_previously_terminal_parked"] = not prior_terminal_park
        gate_pass = all(checks.values())
        evidence_snapshot = {
            "resolved_fills": len(resolved),
            "post_fee_pnl_usd": pnl,
            "first_half": {"resolved_fills": len(first), "post_fee_pnl_usd": first_pnl},
            "second_half": {"resolved_fills": len(second), "post_fee_pnl_usd": second_pnl},
            "rows_sha256": _checksum({"rows": resolved}),
            "matched_cross_exchange_post_fee_pnl_usd": matched_pnl,
            "incremental_post_fee_pnl_usd": round(pnl - matched_pnl, 6),
            "checks": checks,
        }
        record_body = {
            "schema_version": 1,
            "cell_id": str(cell.get("cell_id") or ""),
            "preregistration_checksum": str(cell.get("preregistration_checksum") or ""),
            "model_checksum": str(cell.get("model_checksum") or ""),
            "signal_offset_s": int(cell.get("signal_offset_s") or 0),
            "execution_mode": str(cell.get("execution_mode") or ""),
            "state_path": state_path,
            "evidence_snapshot": evidence_snapshot,
        }
        record = {
            **record_body,
            "evidence_snapshot_checksum": _checksum(evidence_snapshot),
            "record_checksum": _checksum(record_body),
            "gate_pass": gate_pass,
            "status": (
                "ELIGIBLE"
                if gate_pass
                else (
                    "TERMINAL_PARK_NEGATIVE"
                    if prior_terminal_park or (resolved and pnl < 0)
                    else "ACCRUING"
                )
            ),
        }
        rows.append(record)

    eligible = [row for row in rows if row["gate_pass"]]
    eligible.sort(
        key=lambda row: (
            -_num(row["evidence_snapshot"]["post_fee_pnl_usd"]),
            -int(row["evidence_snapshot"]["resolved_fills"]),
            int(row["signal_offset_s"]),
            str(row["cell_id"]),
        )
    )
    selected = dict(eligible[0]) if eligible else None
    if selected:
        activation_seed = {
            "cell_id": selected["cell_id"],
            "record_checksum": selected["record_checksum"],
            "evidence_snapshot_checksum": selected["evidence_snapshot_checksum"],
        }
        selected["activation_id"] = f"promoted-cell-{_checksum(activation_seed)[:20]}"
        if (
            prior_activation.get("cell_id") == selected["cell_id"]
            and prior_activation.get("record_checksum") == selected["record_checksum"]
            and prior_activation.get("activation_id")
        ):
            selected["activation_id"] = prior_activation["activation_id"]
    return {
        "schema_version": 1,
        "kind": "btc5m_cross_exchange_promoted_cell_selector",
        "flow_stage": "LIVE/ROTATE/PROMOTE/LEARN/SELF-DEV",
        "status": "PROMOTED_CELL_READY" if selected else "NO_GATE_COMPLETE_CELL",
        "permanent_promotion_resolved_required": 200,
        "emergency_forward_resolved_required": 10,
        "selection_order": "post_fee_pnl_desc,fill_count_desc,offset_asc,cell_id_asc",
        "cells": rows,
        "selected": selected,
    }
