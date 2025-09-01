
from __future__ import annotations

import threading
from typing import Dict, Tuple

# Shared state for EV/Arbitrage derivation per fixture/market
state_lock = threading.Lock()

# key: (sport, fixture_id, market_norm, is_live) -> { outcome -> {"best_price": float, "book": str, "prices": [float]} }
market_state: Dict[Tuple[str, str, str, bool], dict] = {}

# Cache latest EV per quote to annotate grouped odds even when current batch yields no EV list
ev_cache: Dict[Tuple[str, str, str, str], float] = {}

# fixture_id -> metadata for enrichment
fixture_meta: Dict[str, dict] = {}
fixture_meta_fetched: set[str] = set()


def reset_all_state() -> None:
    """Utility for tests: clear all shared, in-memory state."""
    with state_lock:
        market_state.clear()
        ev_cache.clear()
        fixture_meta.clear()
        fixture_meta_fetched.clear()
