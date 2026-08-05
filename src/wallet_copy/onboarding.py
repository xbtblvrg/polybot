"""Operator-provided wallet onboarding workflow helpers."""

from __future__ import annotations

import re
from urllib.parse import quote, unquote, urlparse
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import requests

from src.wallet_copy.models import WalletSpec, stable_id, utc_now_iso
from src.wallet_copy.registry import DEFAULT_REGISTRY_PATH, upsert_wallet


ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")
HANDLE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


@dataclass(frozen=True)
class OperatorWalletCandidate:
    address: str
    name: str
    notes: str = ""
    tags: tuple[str, ...] = ("operator_provided", "btc_5m", "candidate")

    def normalized_address(self) -> str:
        return self.address.lower()

    def asdict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["address"] = self.normalized_address()
        return payload


@dataclass(frozen=True)
class WalletOnboardingConfig:
    registry_path: str = DEFAULT_REGISTRY_PATH
    scoped_registry_path: str = "data/research/operator_wallet_scoped_registry.json"
    history_state: str = "data/research/wallet_copy_history_state.json"
    wallet_event_log: str = "data/research/wallet_copy_events.jsonl"
    paper_state: str = "data/research/wallet_copy_paper_state.json"
    paper_event_log: str = "data/research/wallet_copy_paper_events.jsonl"
    research_state: str = "data/research/wallet_copy_research_state.json"
    inventory_paper_state: str = "data/research/wallet_copy_inventory_paper_state.json"
    inventory_paper_event_log: str = "data/research/wallet_copy_inventory_paper_events.jsonl"
    ml_dataset: str = "data/research/wallet_copy_ml_dataset.jsonl"
    profit_state: str = "data/research/wallet_copy_profit_engine_state.json"
    live_tracker_state: str = "data/research/wallet_copy_live_tracking_state.json"
    live_tracker_event_log: str = "data/research/wallet_copy_live_tracking_events.jsonl"
    live_tracker_paper_state: str = "data/research/wallet_copy_live_tracker_paper_state.json"
    live_tracker_paper_event_log: str = "data/research/wallet_copy_live_tracker_paper_events.jsonl"
    market_ws_jsonl: str = "data/research/clob_market_ws_events.jsonl"
    resolutions: str = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
    pages: int = 1
    limit: int = 500
    wallet_fraction: float = 0.05
    max_order_usd: float = 2.0
    slippage_bps: float = 250.0
    max_unresolved_ratio: float = 0.5
    data_api_timeout_s: float = 2.0
    live_tracker_iterations: int = 1
    live_tracker_poll_interval_s: float = 1.0
    live_tracker_max_runtime_s: float = 0.0
    run_live_tracker: bool = True

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OnboardingCommand:
    name: str
    argv: tuple[str, ...]
    purpose: str

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def default_wallet_name(address: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "", address.lower())
    return f"operator_wallet_{cleaned[:10]}"


def _candidate_name_and_source(value: str) -> tuple[str, str]:
    text = str(value or "").strip()
    name = ""
    address_source = text
    if "=" in text and not text.startswith("http"):
        name, address_source = text.split("=", 1)
        name = re.sub(r"[^a-zA-Z0-9_-]+", "_", name.strip()).strip("_")
    return name, address_source.strip()


def extract_polymarket_profile_handle(value: str) -> str | None:
    """Return a Polymarket @handle from a handle/profile URL, if present."""

    _, source = _candidate_name_and_source(value)
    if ADDRESS_RE.search(source):
        return None
    text = source.strip()
    if not text:
        return None
    parsed = urlparse(text if "://" in text else "")
    if parsed.netloc:
        host = parsed.netloc.lower()
        if not (host == "polymarket.com" or host.endswith(".polymarket.com")):
            return None
        parts = [unquote(part) for part in parsed.path.split("/") if part]
        for part in parts:
            if part.startswith("@") and HANDLE_RE.match(part[1:]):
                return part[1:].lower()
        return None
    if text.startswith("@") and HANDLE_RE.match(text[1:]):
        return text[1:].lower()
    if HANDLE_RE.match(text) and "/" not in text:
        return text.lower()
    return None


def resolve_polymarket_profile_address(
    value: str,
    *,
    base_url: str = "https://polymarket.com",
    timeout_s: float = 8.0,
) -> dict[str, Any]:
    """Resolve a Polymarket profile handle URL to its page-level wallet address."""

    handle = extract_polymarket_profile_handle(value)
    if not handle:
        return {
            "status": "SKIP",
            "reason": "not_a_polymarket_profile_handle",
            "input": str(value or ""),
        }

    base = str(base_url or "https://polymarket.com").rstrip("/")
    html_url = f"{base}/@{quote(handle)}"
    response = requests.get(html_url, timeout=timeout_s)
    response.raise_for_status()
    build_match = re.search(r'"buildId"\s*:\s*"([^"]+)"', response.text)
    if not build_match:
        return {
            "status": "FAIL",
            "reason": "missing_next_build_id",
            "handle": handle,
            "profile_url": html_url,
        }

    build_id = build_match.group(1)
    encoded_slug = "%40" + quote(handle)
    data_urls = [
        f"{base}/_next/data/{build_id}/profile/{encoded_slug}.json?locale=en&slug={encoded_slug}",
        f"{base}/_next/data/{build_id}/en/profile/{encoded_slug}.json?slug={encoded_slug}&locale=en",
    ]
    failures: list[dict[str, Any]] = []
    for data_url in data_urls:
        data_response = requests.get(data_url, timeout=timeout_s)
        try:
            data_response.raise_for_status()
            payload = data_response.json()
        except (requests.RequestException, ValueError) as exc:
            failures.append({"url": data_url, "error": str(exc)})
            continue

        page_props = payload.get("pageProps") if isinstance(payload, dict) else None
        if not isinstance(page_props, dict):
            failures.append({"url": data_url, "error": "missing_page_props"})
            continue
        address = str(page_props.get("proxyAddress") or page_props.get("primaryAddress") or "").lower()
        if not ADDRESS_RE.fullmatch(address):
            failures.append({"url": data_url, "error": "missing_proxy_or_primary_address"})
            continue
        return {
            "status": "PASS",
            "handle": handle,
            "address": address,
            "username": str(page_props.get("username") or handle),
            "profile_slug": str(page_props.get("profileSlug") or f"@{handle}"),
            "profile_url": html_url,
            "next_data_url": data_url,
            "build_id": build_id,
        }

    html_address_match = re.search(r'"proxyAddress"\s*:\s*"(0x[a-fA-F0-9]{40})"', response.text)
    if html_address_match:
        return {
            "status": "PASS",
            "handle": handle,
            "address": html_address_match.group(1).lower(),
            "username": handle,
            "profile_slug": f"@{handle}",
            "profile_url": html_url,
            "next_data_url": None,
            "build_id": build_id,
            "fallback": "html_proxy_address",
        }
    return {
        "status": "FAIL",
        "reason": "profile_address_not_resolved",
        "handle": handle,
        "profile_url": html_url,
        "build_id": build_id,
        "failures": failures,
    }


def operator_wallet_artifact_stem(
    candidates: list[OperatorWalletCandidate],
    *,
    base_dir: str | Path = "data/research",
) -> str:
    """Return the isolated artifact prefix for one operator onboarding run."""

    if not candidates:
        suffix = "empty"
    elif len(candidates) == 1:
        suffix = candidates[0].normalized_address()[2:10]
    else:
        suffix = stable_id("batch", [candidate.normalized_address() for candidate in candidates])
    return str(Path(base_dir) / f"operator_wallet_{suffix}")


def default_operator_wallet_artifact_paths(
    candidates: list[OperatorWalletCandidate],
    *,
    base_dir: str | Path = "data/research",
) -> dict[str, str]:
    """Return scoped output paths that do not overwrite global wallet-copy truth."""

    stem = operator_wallet_artifact_stem(candidates, base_dir=base_dir)
    return {
        "scoped_registry_path": f"{stem}_registry.json",
        "history_state": f"{stem}_history_state.json",
        "wallet_event_log": f"{stem}_events.jsonl",
        "paper_state": f"{stem}_paper_state.json",
        "paper_event_log": f"{stem}_paper_orders.jsonl",
        "research_state": f"{stem}_research_state.json",
        "inventory_paper_state": f"{stem}_inventory_paper_state.json",
        "inventory_paper_event_log": f"{stem}_inventory_paper_events.jsonl",
        "ml_dataset": f"{stem}_ml_dataset.jsonl",
        "profit_state": f"{stem}_profit_engine_state.json",
        "live_tracker_state": f"{stem}_live_tracking_state.json",
        "live_tracker_event_log": f"{stem}_live_events.jsonl",
        "live_tracker_paper_state": f"{stem}_live_paper_state.json",
        "live_tracker_paper_event_log": f"{stem}_live_paper_orders.jsonl",
    }


def parse_wallet_candidate(value: str, *, notes: str = "", tags: tuple[str, ...] = ()) -> OperatorWalletCandidate:
    """Parse `address`, `name=address`, or a pasted Polymarket profile URL."""

    text = str(value or "").strip()
    if not text:
        raise ValueError("wallet candidate is empty")
    name, address_source = _candidate_name_and_source(text)
    match = ADDRESS_RE.search(address_source)
    if not match:
        raise ValueError(f"no 0x EVM address found in wallet candidate: {value!r}")
    address = match.group(0).lower()
    merged_tags = tuple(dict.fromkeys(("operator_provided", "btc_5m", "candidate", *tags)))
    return OperatorWalletCandidate(
        address=address,
        name=name or default_wallet_name(address),
        notes=notes,
        tags=merged_tags,
    )


def parse_or_resolve_wallet_candidate(
    value: str,
    *,
    notes: str = "",
    tags: tuple[str, ...] = (),
    profile_resolve_timeout_s: float = 8.0,
) -> OperatorWalletCandidate:
    """Parse direct 0x input, or resolve Polymarket @handle/profile URLs first."""

    try:
        return parse_wallet_candidate(value, notes=notes, tags=tags)
    except ValueError as direct_error:
        resolution = resolve_polymarket_profile_address(value, timeout_s=profile_resolve_timeout_s)
        if resolution.get("status") != "PASS":
            reason = resolution.get("reason") or resolution.get("status") or "unknown_resolution_failure"
            raise ValueError(f"{direct_error}; profile handle resolution failed: {reason}") from direct_error

    explicit_name, _ = _candidate_name_and_source(value)
    handle = str(resolution.get("handle") or resolution.get("username") or "").strip()
    fallback_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", handle).strip("_").lower()
    address = str(resolution["address"]).lower()
    merged_tags = tuple(dict.fromkeys(("operator_provided", "btc_5m", "candidate", "polymarket_profile_handle", *tags)))
    resolved_notes = notes
    source_note = f"Resolved from Polymarket profile {resolution.get('profile_slug') or handle}: {resolution.get('profile_url')}"
    if source_note not in resolved_notes:
        resolved_notes = f"{resolved_notes}\n{source_note}".strip()
    return OperatorWalletCandidate(
        address=address,
        name=explicit_name or fallback_name or default_wallet_name(address),
        notes=resolved_notes,
        tags=merged_tags,
    )


def register_operator_wallets(
    candidates: list[OperatorWalletCandidate],
    *,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for candidate in candidates:
        spec = WalletSpec(
            name=candidate.name,
            address=candidate.normalized_address(),
            enabled=True,
            market_filter="btc_5m",
            asset_allowlist=("BTC",),
            tags=candidate.tags,
            notes=candidate.notes
            or "Operator-provided wallet: analyze with full/raw baseline, ML dataset, copy-efficiency, and paper/live-admission gates.",
        )
        payload = upsert_wallet(spec, path=registry_path)
    return payload


def scoped_operator_wallet_registry_payload(candidates: list[OperatorWalletCandidate]) -> dict[str, Any]:
    """Return a registry payload containing only the current operator wallets."""

    return {
        "schema_version": 1,
        "kind": "operator_wallet_scoped_registry",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "wallets": [
            WalletSpec(
                name=candidate.name,
                address=candidate.normalized_address(),
                enabled=True,
                market_filter="btc_5m",
                asset_allowlist=("BTC",),
                tags=candidate.tags,
                notes=candidate.notes
                or "Scoped operator wallet onboarding registry; prevents broad registry contamination.",
            ).asdict()
            for candidate in candidates
        ],
    }


def _history_and_paper_source_args(candidates: list[OperatorWalletCandidate], config: WalletOnboardingConfig) -> tuple[str, ...]:
    if len(candidates) == 1:
        candidate = candidates[0]
        return (
            "--wallet",
            candidate.normalized_address(),
            "--wallet-name",
            candidate.name,
        )
    return ("--wallets-config", config.scoped_registry_path)


def _wallet_address_args(candidates: list[OperatorWalletCandidate]) -> tuple[str, ...]:
    args: list[str] = []
    for candidate in candidates:
        args.extend(("--wallet-address", candidate.normalized_address()))
    return tuple(args)


def build_onboarding_commands(
    config: WalletOnboardingConfig,
    candidates: list[OperatorWalletCandidate] | None = None,
) -> list[OnboardingCommand]:
    python = "python3"
    live_today_sprint_operator_approval_id = "OP-LIVE-20260703-BELA"
    scoped_candidates = candidates or []
    profit_admission_argv = (
        python,
        "scripts/run_wallet_copy_profit_engine.py",
        "--history-state",
        config.history_state,
        "--resolutions",
        config.resolutions,
        "--output",
        config.profit_state,
        "--live-tracker-state",
        config.live_tracker_state,
        "--max-unresolved-ratio",
        str(float(config.max_unresolved_ratio)),
        "--slippage-bps",
        str(float(config.slippage_bps)),
        "--live-today-sprint-operator-approval-id",
        live_today_sprint_operator_approval_id,
    )
    commands = [
        OnboardingCommand(
            name="history_and_paper",
            purpose="ingest the operator wallet BTC-5m history and replay exact-copy intents to paper",
            argv=(
                python,
                "scripts/run_wallet_copy_pipeline.py",
                *_history_and_paper_source_args(scoped_candidates, config),
                "--limit",
                str(int(config.limit)),
                "--pages",
                str(int(config.pages)),
                "--history-state",
                config.history_state,
                "--wallet-event-log",
                config.wallet_event_log,
                "--paper-state",
                config.paper_state,
                "--paper-event-log",
                config.paper_event_log,
                "--reset-paper-state",
                "--wallet-fraction",
                str(float(config.wallet_fraction)),
                "--max-order-usd",
                str(float(config.max_order_usd)),
                "--policy-id",
                "operator_wallet_exact_copy_all_buys",
            ),
        ),
        OnboardingCommand(
            name="cross_wallet_research",
            purpose="build AI/research features, wallet summaries, consensus, inventory plans, and resolution-backed paper scores",
            argv=(
                python,
                "scripts/analyze_wallet_copy_research.py",
                "--history-state",
                config.history_state,
                "--paper-state",
                config.paper_state,
                "--inventory-paper-state",
                config.inventory_paper_state,
                "--inventory-paper-event-log",
                config.inventory_paper_event_log,
                "--resolutions",
                config.resolutions,
                "--max-unresolved-ratio",
                str(float(config.max_unresolved_ratio)),
                "--output",
                config.research_state,
            ),
        ),
        OnboardingCommand(
            name="ml_dataset",
            purpose="export feature rows and paper labels for ML/reverse-engineering",
            argv=(
                python,
                "scripts/export_wallet_copy_dataset.py",
                "--history-state",
                config.history_state,
                "--paper-state",
                config.paper_state,
                "--resolutions",
                config.resolutions,
                "--output",
                config.ml_dataset,
                "--include-unresolved",
            ),
        ),
        OnboardingCommand(
            name="profit_admission",
            purpose="search single-wallet, consensus, and inventory policies with raw-baseline and copy-efficiency gates",
            argv=profit_admission_argv,
        ),
    ]
    if config.run_live_tracker:
        commands.append(
            OnboardingCommand(
                name="paper_live_tracker",
                purpose="poll registered wallets into paper live-tracking with CLOB-backed copy-efficiency evidence; no live orders",
                argv=(
                    python,
                    "scripts/run_wallet_live_tracker.py",
                    "--registry",
                    config.registry_path,
                    "--state",
                    config.live_tracker_state,
                    "--event-log",
                    config.live_tracker_event_log,
                    "--paper-state",
                    config.live_tracker_paper_state,
                    "--paper-event-log",
                    config.live_tracker_paper_event_log,
                    "--profit-policy-state",
                    config.profit_state,
                    "--seed-before-poll",
                    "--seed-history-state",
                    config.history_state,
                    "--limit",
                    "50",
                    "--pages",
                    "1",
                    "--data-api-timeout-s",
                    str(float(config.data_api_timeout_s)),
                    "--iterations",
                    str(int(config.live_tracker_iterations)),
                    "--poll-interval-s",
                    str(float(config.live_tracker_poll_interval_s)),
                    "--max-runtime-s",
                    str(float(config.live_tracker_max_runtime_s)),
                    "--market-ws-jsonl",
                    config.market_ws_jsonl,
                    "--enable-clob-books",
                    "--admission-mode",
                    "--strict-mirror-coverage",
                    "--no-use-profit-search-scope",
                    *_wallet_address_args(scoped_candidates),
                ),
            )
        )
        commands.append(
            OnboardingCommand(
                name="profit_admission_after_tracker",
                purpose="refresh wallet-copy admission after paper live-tracking writes CLOB/copy-efficiency truth",
                argv=profit_admission_argv,
            )
        )
    return commands


def build_onboarding_plan(
    candidates: list[OperatorWalletCandidate],
    *,
    config: WalletOnboardingConfig | None = None,
) -> dict[str, Any]:
    cfg = config or WalletOnboardingConfig()
    return {
        "schema_version": 1,
        "kind": "operator_wallet_onboarding_plan",
        "generated_at": utc_now_iso(),
        "plan_id": stable_id(
            "wop",
            {
                "wallets": [candidate.normalized_address() for candidate in candidates],
                "config": cfg.asdict(),
            },
        ),
        "paper_only": True,
        "live_orders_allowed": False,
        "candidate_wallets": [candidate.asdict() for candidate in candidates],
        "config": cfg.asdict(),
        "scoped_registry": {
            "path": cfg.scoped_registry_path,
            "wallets": [candidate.asdict() for candidate in candidates],
            "purpose": "scope multi-wallet operator onboarding to the current provided wallets, not the full registry",
        },
        "commands": [command.asdict() for command in build_onboarding_commands(cfg, candidates)],
        "admission_contract": {
            "scope": "BTC 5-minute wallet-copy only",
            "research": [
                "full/raw wallet-copy baseline",
                "filtered candidate policies",
                "multi-wallet consensus and inventory",
                "ML dataset export",
                "copy-efficiency and missed-copy taxonomy",
            ],
            "paper_gate": "history and live-tracker paper states must remain live_orders_allowed=false",
            "live_gate": "only candidate-policy-scoped copy_efficiency PASS with CLOB-backed fills can become live-admission evidence",
        },
    }
