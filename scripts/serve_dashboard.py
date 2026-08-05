#!/usr/bin/env python3
"""Read-only realtime dashboard szerver (operator, 2026-07-14).

Zero-AI, zero-write: csak olvassa a data/research artifactokat és a
HANDOFF-ot, és egy helyi weboldalon mutatja. Indítás:
    python3 scripts/serve_dashboard.py   ->  http://localhost:8777
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PORT = 8777


def _read_json(rel: str, default=None):
    try:
        return json.loads((ROOT / rel).read_text())
    except Exception:
        return default if default is not None else {}


def _digest_lines() -> dict:
    out = {}
    try:
        for line in (ROOT / "data/research/state_digest.md").read_text().splitlines():
            if ":" in line and not line.startswith("#"):
                k, _, v = line.partition(":")
                k = k.strip()
                if k and " " not in k:
                    out[k] = v.strip()[:600]
    except Exception:
        pass
    return out


def _handoff_tail(n: int = 30) -> list:
    try:
        heads = [l for l in (ROOT / "docs/agents/HANDOFF.md").read_text().splitlines()
                 if l.startswith("## ")]
        return heads[-n:][::-1]
    except Exception:
        return []


def _jsonl_tail(rel: str, n: int = 20) -> list:
    try:
        lines = (ROOT / rel).read_text().splitlines()
        rows = []
        for l in lines[-n:]:
            try:
                rows.append(json.loads(l))
            except Exception:
                continue
        return rows[::-1]
    except Exception:
        return []


def _guard_summary() -> dict:
    g = _read_json("data/research/wallet_copy_live_guard_state.json")
    gi = g.get("guard_code_identity") or {}
    pf = g.get("profitability_filter") or {}
    members = {}

    def walk(d):
        if isinstance(d, dict):
            if "source_wallet" in d and "enabled" in d and "policy_id" in d:
                members[str(d["source_wallet"])[:10]] = {
                    "enabled": bool(d.get("enabled")),
                    "status": str(d.get("status", ""))[:36],
                }
            for v in d.values():
                walk(v)
        elif isinstance(d, list):
            for v in d:
                walk(v)

    walk(g)
    return {
        "cycle": g.get("cycle"),
        "outcome": g.get("cycle_outcome"),
        "pid": gi.get("pid"),
        "started": str(gi.get("started_at_utc", ""))[:19],
        "filter": {"max_order": pf.get("max_order_usd"), "max_price": pf.get("max_price")},
        "selected": str((g.get("candidate") or {}).get("source_wallet", ""))[:10],
        "members": members,
    }


def _api_all() -> dict:
    digest = _digest_lines()
    cohort = _read_json("data/research/wallet_market_cohort_replay_latest.json").get("summary", {})
    return {
        "digest": digest,
        "deadman": _read_json("data/research/order_flow_deadman_state.json"),
        "codex_watch": _read_json("data/research/codex_starvation_deadman_state.json"),
        "guard": _guard_summary(),
        "commitments": _jsonl_tail("data/research/commitments.jsonl", 25),
        "journal": _jsonl_tail("data/research/live_change_journal.jsonl", 10),
        "handoff": _handoff_tail(30),
        "cohort": {k: cohort.get(k) for k in (
            "cohort_size", "cohort_shadow_positive", "live_ready_picks",
            "status_counts", "top_live_ready_wallet") if k in cohort},
        "utilization": _read_json("data/research/resource_utilization_latest.json").get("summary")
        or _read_json("data/research/resource_utilization_latest.json"),
        "funnel": _read_json("data/research/factory_funnel_latest.json"),
        "brainless": _read_json("data/research/brainless_ops_latest.json"),
    }


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):  # csendes szerver
        pass

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/all"):
            self._send(200, json.dumps(_api_all(), default=str).encode(), "application/json")
        elif self.path in ("/", "/index.html"):
            try:
                body = (ROOT / "dashboard/dashboard.html").read_bytes()
                self._send(200, body, "text/html; charset=utf-8")
            except Exception:
                self._send(500, b"dashboard.html missing", "text/plain")
        else:
            self._send(404, b"not found", "text/plain")


if __name__ == "__main__":
    print(f"Dashboard: http://localhost:{PORT}  (read-only, local)")
    HTTPServer(("127.0.0.1", PORT), H).serve_forever()
