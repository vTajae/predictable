
"""
optic_modular
=============

A cleaned, modular refactor of the original `getOpticOdds.py` that handled
OpticOdds catalog queries (sports, leagues, sportsbooks) and started SSE workers.

Public API (stable re-exports):
- subscribe_all_sports (from subscribe)
- get_all_active_sportsbooks, get_all_sports, get_all_sports_verbose,
  get_leagues_for_sport, get_leagues_verbose (from catalogue)
- API constants (from config): API_KEY, API_BASE, STREAM_BASE, SPORTSBOOKS_URL, SPORTS_URL, LEAGUES_URL
"""
from .config import API_KEY, API_BASE, STREAM_BASE, SPORTSBOOKS_URL, SPORTS_URL, LEAGUES_URL
from .catalogue import (
    get_all_active_sportsbooks,
    get_all_sports,
    get_all_sports_verbose,
    get_leagues_for_sport,
    get_leagues_verbose,
)
from .subscribe import subscribe_all_sports

__all__ = [
    "API_KEY", "API_BASE", "STREAM_BASE", "SPORTSBOOKS_URL", "SPORTS_URL", "LEAGUES_URL",
    "get_all_active_sportsbooks",
    "get_all_sports", "get_all_sports_verbose",
    "get_leagues_for_sport", "get_leagues_verbose",
    "subscribe_all_sports",
]
