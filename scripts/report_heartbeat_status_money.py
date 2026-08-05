#!/usr/bin/env python3
"""Render the heartbeat money line from the machine-written state digest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    return float(value)


def build_money_line(packet: dict[str, Any]) -> str:
    pnl = packet.get("pnl") if isinstance(packet.get("pnl"), dict) else {}
    day = _number(pnl.get("day_pnl_usd"), "pnl.day_pnl_usd")
    fills = int(_number(pnl.get("day_resolved_fills"), "pnl.day_resolved_fills"))
    actual = _number(
        pnl.get("since_topup_actual_delta_usd"),
        "pnl.since_topup_actual_delta_usd",
    )
    return f"day {day:.6f}, fills {fills}, since_topup actual {actual:.6f}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--packet",
        default="data/research/state_digest.json",
    )
    args = parser.parse_args()
    packet = json.loads(Path(args.packet).read_text(encoding="utf-8"))
    print(build_money_line(packet))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
