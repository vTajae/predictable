
"""
calc_modular
============

A cleaned, modular refactor of the original `calculations.py` that provided
EV/arbitrage calculations and a streaming SSE worker.

Public API (stable):
- compute_arbitrage, compute_ev_pct (from evcalc)
- process_odds_batch (from evcalc)
- sse_worker (from sse)

Stateful singletons (module-level in state.py):
- state_lock
- market_state
- ev_cache
- fixture_meta
- fixture_meta_fetched
"""
from .evcalc import compute_arbitrage, compute_ev_pct, process_odds_batch
from .sse import sse_worker

__all__ = [
    "compute_arbitrage",
    "compute_ev_pct",
    "process_odds_batch",
    "sse_worker",
]
