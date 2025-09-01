
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Set


def _env_bool(name: str, default: bool = False) -> bool:
    return (os.getenv(name, str(int(default))) or "").strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name, default)
    return default if v is None else str(v)


def env_allowed_markets() -> Optional[Set[str]]:
    """
    Resolve upstream allowed_markets strictly from env (ARB_MARKETS).

    Returns a set of lower-cased market names or None if unrestricted.
    """
    am_raw = (_env_str("ARB_MARKETS", "all") or "").strip().lower()
    if am_raw in ("", "all", "*"):
        return None
    return {m.strip().lower() for m in am_raw.split(",") if m.strip()}


@dataclass(frozen=True)
class Settings:
    # Websocket defaults
    default_odds_format: str = _env_str("ODDS_FORMAT", "decimal")
    default_ev_threshold: float = _env_float("EV_THRESHOLD_PERCENT", 3.0)
    default_arb_threshold: float = _env_float("ARB_THRESHOLD_PERCENT", _env_float("ABB_THRESHOLD_PERCENT", 3.0))

    # Debugging / behavior
    ws_debug: bool = _env_bool("WS_DEBUG", False)
    ingest_filters_enabled: bool = _env_bool("INGEST_FILTERS", False)
    include_fixture_updates: bool = _env_bool("INCLUDE_FIXTURE_UPDATES", True)

    # Worker / batching
    max_workers: Optional[int] = None if _env_int("MAX_WORKERS", 8) < 0 else _env_int("MAX_WORKERS", 8)
    sportsbook_chunk_size: int = _env_int("SPORTSBOOK_CHUNK_SIZE", 10)
    league_chunk_size: int = _env_int("LEAGUE_CHUNK_SIZE", 5)

    # Ports / run
    port: int = _env_int("PORT", 8000)

    # Allowed markets (env-only; avoids over-filtering at ingestion layer)
    allowed_markets: Optional[Set[str]] = env_allowed_markets()
