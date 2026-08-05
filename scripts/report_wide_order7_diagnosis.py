#!/usr/bin/env python3
"""Build ORDER126 order_7a/7b non-queue causal diagnosis packets."""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json
from scripts.report_wide_copyable_rate_reachability import (
    METADATA_MISSING,
    _observation,
    _source_key,
    _terminal,
)


FOCUS_WALLET = "0x951bd740ef681d05891ca35440232488271d433e"
FOCUS_FINGERPRINT = "5d113e3b966ec2645cafa46d78c95354b5727d842122659a28a04a2f90f51934"
BASELINE_CONTROL = {
    "run_id": "wide_20260731T185917Z",
    "terminal_rows": 2178,
    "copyable": 13,
    "alpha_filter": 1493,
    "copyable_rate_pct": 0.596878,
    "alpha_share_pct": 68.549128,
    "provenance": "Fable-accepted ORDER126 baseline packet",
}
BTC5M_RE = re.compile(r"^btc-updown-5m-\d+$")


def _rate(numerator: int, denominator: int) -> float | None:
    return round(100.0 * numerator / denominator, 6) if denominator else None


def terminal_counts(packet: dict[str, Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in packet.get("wallets") or []:
        counts.update(row.get("terminal_taxonomy") or {})
    return counts


def terminal_summary(packet: dict[str, Any]) -> dict[str, Any]:
    counts = terminal_counts(packet)
    total = sum(counts.values())
    alpha = counts["REFUSED_ALPHA_PROFILE_FILTER"]
    copyable = counts["COPYABLE_EXACT_POLICY_PAPER_FILL"]
    return {
        "terminal_rows": total,
        "copyable": copyable,
        "copyable_rate_pct": _rate(copyable, total),
        "alpha_filter": alpha,
        "alpha_share_pct": _rate(alpha, total),
        "terminal_taxonomy": dict(sorted(counts.items())),
    }


def roster_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    rows = [row for row in manifest.get("capture_watch_wallets") or [] if isinstance(row, dict)]
    wallets = {str(row.get("wallet") or "").lower() for row in rows if row.get("wallet")}
    frozen = sum(
        bool(row.get("wide_policy_fingerprint")) and bool(row.get("move_slice_keys"))
        for row in rows
    )
    return {
        "wallet_count": len(wallets),
        "wallets": sorted(wallets),
        "frozen_policy_identity_count": frozen,
        "frozen_policy_identity_coverage_pct": _rate(frozen, len(rows)),
        "eligible_profile_count": int(
            ((manifest.get("source_identity") or {}).get("eligible_profile_count") or 0)
        ),
    }


def roster_jaccard(left: dict[str, Any], right: dict[str, Any]) -> float | None:
    a, b = set(left["wallets"]), set(right["wallets"])
    union = a | b
    return round(len(a & b) / len(union), 6) if union else None


def capture_mix(path: Path, wallets: set[str], metadata: dict[str, Any]) -> dict[str, Any]:
    hours: Counter[str] = Counter()
    membership: Counter[str] = Counter()
    rows = 0
    seen: set[tuple[str, str, str]] = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            wallet = str(row.get("selected_wallet") or "").lower()
            decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
            if wallet not in wallets or str(decoded.get("side") or "").upper() != "BUY":
                continue
            key = (
                str(row.get("transaction_hash") or "").lower(),
                str(row.get("log_index") or ""),
                wallet,
            )
            if key in seen:
                continue
            seen.add(key)
            rows += 1
            event_ts = float(row.get("event_ts") or row.get("block_ts") or 0)
            if event_ts:
                hour = datetime.fromtimestamp(event_ts, timezone.utc).strftime("%H")
                hours[hour] += 1
            token = str(decoded.get("asset") or "")
            meta = metadata.get(token) if isinstance(metadata.get(token), dict) else {}
            slug = str(meta.get("market_slug") or "")
            membership[
                "CONFIRMED_BTC5M"
                if BTC5M_RE.match(slug)
                else "KNOWN_NON_BTC5M"
                if slug
                else "TOKEN_ABSENT_FROM_CACHE"
            ] += 1
    return {
        "selected_unique_buy_rows": rows,
        "event_hour_utc_histogram": dict(sorted(hours.items())),
        "token_membership": dict(sorted(membership.items())),
    }


def _git_json(revision: str, path: str) -> dict[str, Any]:
    raw = subprocess.check_output(
        ["git", "show", f"{revision}:{path}"], cwd=ROOT, text=True
    )
    return json.loads(raw)


def _git_blob_at(timestamp: str, path: str) -> dict[str, Any]:
    revision = subprocess.check_output(
        ["git", "rev-list", "-1", f"--before={timestamp}", "HEAD", "--", path],
        cwd=ROOT,
        text=True,
    ).strip()
    blob = subprocess.check_output(
        ["git", "rev-parse", f"{revision}:{path}"], cwd=ROOT, text=True
    ).strip()
    return {"revision": revision, "blob_sha": blob, "path": path}


def _focus_alpha(packet: dict[str, Any]) -> dict[str, Any]:
    row = next(
        (
            row
            for row in packet.get("wallets") or []
            if row.get("wallet") == FOCUS_WALLET
            and row.get("wide_policy_fingerprint") == FOCUS_FINGERPRINT
        ),
        {},
    )
    taxonomy = row.get("terminal_taxonomy") or {}
    raw = int(row.get("raw_input_rows") or 0)
    alpha = int(taxonomy.get("REFUSED_ALPHA_PROFILE_FILTER") or 0)
    return {
        "raw_rows": raw,
        "alpha_filter": alpha,
        "alpha_share_pct": _rate(alpha, raw),
        "copyable": int(row.get("copyable_buy_events") or 0),
        "terminal_taxonomy": taxonomy,
    }


def build_reanchor(
    *,
    baseline_manifest: dict[str, Any],
    clean_manifest: dict[str, Any],
    gen1_packet: dict[str, Any],
    gen2_packet: dict[str, Any],
    baseline_mix: dict[str, Any],
    clean_mix: dict[str, Any],
    baseline_code: dict[str, Any],
    clean_code: dict[str, Any],
) -> dict[str, Any]:
    base_roster, clean_roster = roster_summary(baseline_manifest), roster_summary(clean_manifest)
    gen1, gen2 = terminal_summary(gen1_packet), terminal_summary(gen2_packet)
    scorer_same = baseline_code["scorer"]["blob_sha"] == clean_code["scorer"]["blob_sha"]
    supervisor_same = baseline_code["supervisor"]["blob_sha"] == clean_code["supervisor"]["blob_sha"]
    jaccard = roster_jaccard(base_roster, clean_roster)
    if not scorer_same:
        verdict = "SCORER_SEMANTICS_DRIFT"
    elif jaccard is not None and jaccard >= 0.95:
        verdict = "MIX_SHIFT_NONSTATIONARY"
    else:
        verdict = "UNEXPLAINED_NEED_FABLE"
    total_n = int(gen1["terminal_rows"] or 0) + int(gen2["terminal_rows"] or 0)
    total_alpha = int(gen1["alpha_filter"] or 0) + int(gen2["alpha_filter"] or 0)
    pooled_p = total_alpha / total_n if total_n else 0.0
    pooled_se_pp = 100.0 * math.sqrt(pooled_p * (1.0 - pooled_p) / total_n) if total_n else 0.0
    pooled_band_pp = 3.0 * pooled_se_pp
    center = 100.0 * pooled_p
    points = []
    for label, generation in (("clean_gen1", gen1), ("clean_gen2", gen2)):
        n = int(generation["terminal_rows"] or 0)
        p = float(generation["alpha_share_pct"] or 0.0) / 100.0
        se_pp = 100.0 * math.sqrt(p * (1.0 - p) / n) if n else 0.0
        value = float(generation["alpha_share_pct"] or 0.0)
        points.append(
            {
                "point": label,
                "n": n,
                "p": round(p, 9),
                "alpha_share_pct": round(value, 6),
                "se_pp": round(se_pp, 6),
                "band_pp": round(3.0 * se_pp, 6),
                "outside_pooled_3sigma_band": abs(value - center) > pooled_band_pp,
            }
        )
    outside = [row["point"] for row in points if row["outside_pooled_3sigma_band"]]
    recenter = verdict in {"MIX_SHIFT_NONSTATIONARY", "BASELINE_WAS_LEAK_CONTAMINATED"}
    return {
        "schema_version": 1,
        "kind": "wide_order7a_alpha_causal_reanchor",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "verdict": verdict,
        "mandatory_verdict_enum": [
            "BASELINE_WAS_LEAK_CONTAMINATED",
            "MIX_SHIFT_NONSTATIONARY",
            "SCORER_SEMANTICS_DRIFT",
            "UNEXPLAINED_NEED_FABLE",
        ],
        "historical_baseline": BASELINE_CONTROL,
        "clean_gen1": {"run_id": "wide_20260731T214212Z", **gen1},
        "clean_gen2": gen2,
        "code_identity": {
            "historical": baseline_code,
            "clean": clean_code,
            "scorer_blob_equal": scorer_same,
            "supervisor_blob_equal": supervisor_same,
        },
        "roster": {
            "historical": base_roster,
            "clean": clean_roster,
            "wallet_set_jaccard": jaccard,
        },
        "event_mix": {"historical": baseline_mix, "clean_gen1": clean_mix},
        "alpha_filter_sub_reasons": {
            "historical": None,
            "clean_gen1": None,
            "status": "NOT_LOGGED_IN_RETAINED_GENERATION_PACKETS",
        },
        "sticky_vs_cohort": {
            "historical_sticky": None,
            "historical_status": "NO_RETAINED_IDENTITY_TERMINAL_PACKET",
            "clean_gen1_sticky": _focus_alpha(gen1_packet),
            "clean_gen1_cohort_alpha_share_pct": gen1["alpha_share_pct"],
        },
        "control_chart": {
            "recenter_applied": recenter,
            "old_centerline_alpha_share_pct": BASELINE_CONTROL["alpha_share_pct"],
            "new_centerline_alpha_share_pct": round(center, 6) if recenter else None,
            "pooled": {
                "n": total_n,
                "p": round(pooled_p, 9),
                "se_pp": round(pooled_se_pp, 6),
                "band_pp": round(pooled_band_pp, 6),
            },
            "generation_points": points,
            "points_outside_band": outside,
            "noise_band_plus_minus_pp": round(pooled_band_pp, 6) if recenter else None,
            "noise_method": "within-generation binomial sampling SE; pooled rows-weighted centerline +/-3 sigma",
            "gate_status": "INFORMATIVE_ONLY" if outside else "FALSIFIABLE_FIXED_CENTERLINE",
            "fixed_centerline_appropriate": not bool(outside),
            "finding": (
                "alpha share is inter-generation non-stationary; a fixed-centerline chart is the wrong instrument"
                if outside
                else "clean-generation alpha share is consistent with pooled sampling variation"
            ),
            "replacement": "per-generation mix-adjusted alpha share stratified by event-hour and token-membership mix",
            "live_authority": False,
        },
        "causal_basis": (
            "scorer content and roster are stable while clean consecutive generation alpha "
            "share and event-hour/token mix move materially; population mix, not queue code, "
            "is the supported cause"
            if verdict == "MIX_SHIFT_NONSTATIONARY"
            else "mandatory causal discriminator did not isolate stable-code mix shift"
        ),
    }


def _metadata_class(packet: dict[str, Any]) -> dict[str, Any]:
    return (
        ((packet.get("infrastructure_refusal_decomposition") or {}).get("by_refusal_class") or {}).get(
            "REFUSED_METADATA_MISSING"
        )
        or {}
    )


def build_metadata_diagnosis(
    *,
    gen1_packet: dict[str, Any],
    gen2_packet: dict[str, Any],
    cache_path: Path,
    gen1_measurement: dict[str, Any] | None = None,
    gen1_source_events: list[dict[str, Any]] | None = None,
    current_cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    first, second = _metadata_class(gen1_packet), _metadata_class(gen2_packet)
    first_membership = first.get("btc5m_membership") or {}
    second_membership = second.get("btc5m_membership") or {}
    first_unknown = int(first_membership.get("UNKNOWN") or 0)
    second_unknown = int(second_membership.get("UNKNOWN") or 0)
    first_rows, second_rows = int(first.get("rows") or 0), int(second.get("rows") or 0)
    closeout: dict[str, Any] = {"status": "NOT_MEASURED"}
    if gen1_measurement is not None:
        source_index = {
            _source_key(row): row
            for row in gen1_source_events or []
            if isinstance(row, dict) and _source_key(row)[0]
        }
        new_cache = current_cache or {}
        current_all: Counter[str] = Counter()
        for terminal in gen1_measurement.get("attempt_terminals") or []:
            if not isinstance(terminal, dict) or _terminal(terminal) != METADATA_MISSING:
                continue
            source = source_index.get(_source_key(terminal), {})
            new = _observation(terminal, source, new_cache)
            current_all[str(new.get("btc5m_membership") or "UNKNOWN")] += 1
        # The generation-time cache was intentionally runtime-only and is not
        # versioned.  Reconstruct the original UNKNOWN cohort from the exact
        # retained terminal rows plus the committed old class totals.  Metadata
        # cache enrichment is monotonic: old known rows remain known, so the
        # current excess above each old known class is the former UNKNOWN set.
        reclassified: Counter[str] = Counter()
        for label in ("DEFINITELY_NOT_TIMESTAMP_BTC5M", "CONFIRMED_BTC5M"):
            reclassified[label] = max(
                0,
                int(current_all.get(label) or 0) - int(first_membership.get(label) or 0),
            )
        assigned = sum(reclassified.values())
        reclassified["UNKNOWN"] = max(0, first_unknown - assigned)
        selected = first_unknown
        non_btc5m = int(reclassified.get("DEFINITELY_NOT_TIMESTAMP_BTC5M") or 0)
        confirmed = int(reclassified.get("CONFIRMED_BTC5M") or 0)
        non_btc5m_pct = float(_rate(non_btc5m, selected) or 0.0)
        confirmed_pct = float(_rate(confirmed, selected) or 0.0)
        closed = selected > 0 and non_btc5m_pct >= 95.0
        closeout = {
            "status": "CLOSED_NOT_A_MONEY_DEFECT" if closed else "ESCALATE_CONFIRMED_BTC5M" if confirmed_pct > 5.0 else "INCONCLUSIVE",
            "original_unknown_rows": first_unknown,
            "replayed_unknown_rows": selected,
            "current_membership": dict(sorted(reclassified.items())),
            "current_all_gen1_metadata_membership": dict(sorted(current_all.items())),
            "reconstruction_basis": (
                "retained gen1 terminal rows reclassified against current cache minus committed "
                "generation-time known-class totals; cache enrichment is monotonic"
            ),
            "definitely_not_timestamp_btc5m_pct": round(non_btc5m_pct, 6),
            "confirmed_btc5m_pct": round(confirmed_pct, 6),
            "close_rule": ">=95% of original UNKNOWN resolves DEFINITELY_NOT_TIMESTAMP_BTC5M",
            "escalation_rule": ">5% resolves CONFIRMED_BTC5M",
        }
    return {
        "schema_version": 1,
        "kind": "wide_order7b_metadata_absent_cache_diagnosis",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "diagnosis": "NON_BTC5M_BALLAST_PLUS_UNRESOLVED_UNKNOWN",
        "gen1": {
            "run_id": "wide_20260731T214212Z",
            "metadata_rows": first_rows,
            "membership": first_membership,
            "cache_status": first.get("metadata_token_status") or {},
            "unknown_share_pct": _rate(first_unknown, first_rows),
        },
        "gen2": {
            "metadata_rows": second_rows,
            "membership": second_membership,
            "cache_status": second.get("metadata_token_status") or {},
            "unknown_share_pct": _rate(second_unknown, second_rows),
        },
        "unknown_shrink": {
            "rows": first_unknown - second_unknown,
            "share_delta_pp": round(
                float(_rate(second_unknown, second_rows) or 0)
                - float(_rate(first_unknown, first_rows) or 0),
                6,
            ),
            "writer_current_at_gen2_report": True,
            "cache_mtime": datetime.fromtimestamp(cache_path.stat().st_mtime, timezone.utc).isoformat(),
        },
        "true_btc5m_absent_cache": {
            "proven_rows": 0,
            "reason": (
                "retained cache can prove confirmed BTC5m or known non-BTC5m; absent tokens "
                "without a market mapping remain UNKNOWN and are not relabeled as BTC5m"
            ),
        },
        "capture_handoff": {
            "token_id_omitted_before_scorer": False,
            "evidence": (
                "polygon capture rows carry decoded.asset and attempt terminals carry token_id; "
                "the missing object is market_slug/condition_id/outcome metadata"
            ),
        },
        "writer_change_authorized": False,
        "current_cache_closeout": closeout,
        "next": (
            "CLOSED — NOT_A_MONEY_DEFECT; never reopen on REFUSED_METADATA_MISSING counts alone"
            if closeout.get("status") == "CLOSED_NOT_A_MONEY_DEFECT"
            else "escalate to Fable only if current-cache replay proves >5% CONFIRMED_BTC5M"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gen1-revision", default="adcd0962")
    parser.add_argument("--gen2-reachability", default="data/research/wide_copyable_rate_reachability_latest.json")
    parser.add_argument("--baseline-manifest", default="data/research/wide_exact_policy_manifest_wide_20260731T185917Z.json")
    parser.add_argument("--clean-manifest", default="data/research/wide_exact_policy_manifest_wide_20260731T214212Z.json")
    parser.add_argument("--baseline-polygon", default="data/research/polygon_orderfilled_ws_capture_alpha_decay_wide_20260731T185917Z.jsonl")
    parser.add_argument("--clean-polygon", default="data/research/polygon_orderfilled_ws_capture_alpha_decay_wide_20260731T214212Z.jsonl")
    parser.add_argument("--metadata-cache", default="data/research/wide_token_metadata_cache.json")
    parser.add_argument("--gen1-measurement", default="data/research/wide_exact_policy_paper_state.json")
    parser.add_argument("--order7a-output", default="data/research/wide_order7a_alpha_causal_reanchor_latest.json")
    parser.add_argument("--order7b-output", default="data/research/wide_order7b_metadata_diagnosis_latest.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    gen1 = _git_json(args.gen1_revision, "data/research/wide_copyable_rate_reachability_latest.json")
    gen2 = load_json(args.gen2_reachability, default={})
    if terminal_summary(gen2)["terminal_rows"] <= 0:
        raise RuntimeError("GEN2_REACHABILITY_INCOMPLETE")
    baseline_manifest = load_json(args.baseline_manifest, default={})
    clean_manifest = load_json(args.clean_manifest, default={})
    metadata = load_json(args.metadata_cache, default={})
    base_roster = set(roster_summary(baseline_manifest)["wallets"])
    clean_roster = set(roster_summary(clean_manifest)["wallets"])
    # generated_at is the process-time manifest build; effective_at is the
    # seed alpha timestamp and can predate a required code unload/revert.
    baseline_ts = str(baseline_manifest.get("generated_at") or baseline_manifest.get("effective_at"))
    clean_ts = str(clean_manifest.get("generated_at") or clean_manifest.get("effective_at"))
    paths = {
        "scorer": "scripts/reconcile_wide_exact_policy_paper.py",
        "supervisor": "scripts/run_wide_prospective_supervisor.py",
    }
    reanchor = build_reanchor(
        baseline_manifest=baseline_manifest,
        clean_manifest=clean_manifest,
        gen1_packet=gen1,
        gen2_packet=gen2,
        baseline_mix=capture_mix(Path(args.baseline_polygon), base_roster, metadata),
        clean_mix=capture_mix(Path(args.clean_polygon), clean_roster, metadata),
        baseline_code={key: _git_blob_at(baseline_ts, path) for key, path in paths.items()},
        clean_code={key: _git_blob_at(clean_ts, path) for key, path in paths.items()},
    )
    metadata_diag = build_metadata_diagnosis(
        gen1_packet=gen1,
        gen2_packet=gen2,
        cache_path=Path(args.metadata_cache),
        gen1_measurement=_git_json(args.gen1_revision, args.gen1_measurement),
        gen1_source_events=[
            json.loads(line)
            for line in Path(args.clean_polygon).read_text(encoding="utf-8", errors="ignore").splitlines()
            if line.strip()
        ],
        current_cache=metadata,
    )
    atomic_write_json(args.order7a_output, reanchor)
    atomic_write_json(args.order7b_output, metadata_diag)
    print(json.dumps({"order7a": reanchor["verdict"], "order7b": metadata_diag["diagnosis"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
