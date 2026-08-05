"""Canonical wallet-copy subsystem.

The wallet-copy stack is the new primary path:
wallet history -> normalized events -> deterministic copy intents -> paper
order lifecycle -> optional guarded live execution using the same intents.
"""

from src.wallet_copy.fill_model import FillModelConfig, estimate_executable_fill
from src.wallet_copy.mission import mission_contract, mission_contract_check
from src.wallet_copy.models import CopyIntent, PaperOrder, WalletEvent, WalletSpec

__all__ = [
    "CopyIntent",
    "FillModelConfig",
    "PaperOrder",
    "WalletEvent",
    "WalletSpec",
    "estimate_executable_fill",
    "mission_contract",
    "mission_contract_check",
]
