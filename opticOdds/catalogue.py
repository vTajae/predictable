
from __future__ import annotations

from typing import Tuple, List, Dict
from .config import SPORTS_URL, LEAGUES_URL, SPORTSBOOKS_URL, TRACE_ENABLED, logger
from .http import get_json
from .utils import dedupe_preserve_order

def get_all_sports() -> list[str]:
    data = get_json(SPORTS_URL, timeout=30) or {}
    arr = data.get("data") or []
    return [s.get("id") for s in arr if isinstance(s, dict) and s.get("id")]

def get_all_sports_verbose() -> tuple[list[str], dict[str, str]]:
    data = get_json(SPORTS_URL, timeout=30) or {}
    arr = data.get("data") or []
    mapping: Dict[str, str] = {}
    ids: List[str] = []
    for s in arr:
        if not isinstance(s, dict):
            continue
        sid = s.get("id")
        name = s.get("name") or s.get("title") or sid
        if sid:
            ids.append(sid)
            mapping[sid] = name
    return ids, mapping

def get_all_active_sportsbooks() -> list[str]:
    """Fetch sportsbooks and return display names for streaming.
    Do not filter by is_active—include all variants so the stream is broad.
    Fallbacks: use 'title' or 'id' if 'name' missing. Deduplicate while preserving order.
    """
    data = get_json(SPORTSBOOKS_URL, timeout=30) or {}
    arr = data.get("data") or []
    if not isinstance(arr, list):
        arr = []
    names_raw: list[str] = []
    for sb in arr:
        if not isinstance(sb, dict):
            continue
        n = sb.get("name") or sb.get("title") or sb.get("display_name") or sb.get("id")
        if isinstance(n, str) and n.strip():
            names_raw.append(n.strip())
    unique_names = dedupe_preserve_order(names_raw)
    if not unique_names:
      pass
    return unique_names

def get_leagues_for_sport(sport: str) -> list[str]:
    data = get_json(LEAGUES_URL, params={"sport": sport}, timeout=60) or {}
    arr = data.get("data") or []
    return [l.get("id") for l in arr if isinstance(l, dict) and l.get("id")]

def get_leagues_verbose(sport: str) -> tuple[list[str], dict[str, str]]:
    data = get_json(LEAGUES_URL, params={"sport": sport}, timeout=60) or {}
    arr = data.get("data") or []
    mapping: Dict[str, str] = {}
    ids: List[str] = []
    for l in arr:
        if not isinstance(l, dict):
            continue
        lid = l.get("id")
        name = l.get("name") or l.get("title") or lid
        if lid:
            ids.append(lid)
            mapping[lid] = name
    return ids, mapping
