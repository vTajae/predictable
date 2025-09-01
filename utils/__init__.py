
"""
utils_modular
=============

A cleaned, modular refactor of the original `utils.py`.

Public API (re-exports):
- normalize_market (text)
- parse_decimal_odds (odds)
- to_epoch_seconds (time)
- chunk_list (chunk)
- should_process_market (filters)
"""
from .text import normalize_market
from .odds import parse_decimal_odds
from .timeutil import to_epoch_seconds
from .chunk import chunk_list
from .filters import should_process_market

__all__ = [
    "normalize_market", "parse_decimal_odds", "to_epoch_seconds",
    "chunk_list", "should_process_market",
]
