
from __future__ import annotations
from typing import Set
from .text import normalize_market

def should_process_market(market: str, allowed_markets: Set[str] | None) -> bool:
    """Return True when market passes an allowlist (None means allow all)."""
    if allowed_markets is None:
        return True
    return normalize_market(market) in allowed_markets
