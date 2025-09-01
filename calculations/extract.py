
from __future__ import annotations

from typing import Any, Optional, Tuple
from .normalize import is_generic_label
import re


def extract_deep_link(it: dict) -> str:
    """Search nested objects for a deep link URL; return empty string when not found."""
    try:
        def _search(obj, depth=0):
            if depth > 12:
                return None
            if isinstance(obj, dict):
                # nested deep_link dicts
                for k in ("deep_link",):
                    dl = obj.get(k)
                    if isinstance(dl, dict):
                        for p in ("desktop", "Desktop"):
                            u = dl.get(p)
                            if isinstance(u, str) and u:
                                return u
                for subk in ("raw", "raw_data", "data", "attributes", "payload"):
                    sub = obj.get(subk)
                    if sub is not None:
                        r = _search(sub, depth + 1)
                        if r:
                            return r
                for v in obj.values():
                    r = _search(v, depth + 1)
                    if r:
                        return r
            elif isinstance(obj, (list, tuple, set)):
                for item in obj:
                    r = _search(item, depth + 1)
                    if r:
                        return r
            return None
        return _search(it) or ""
    except Exception:
        return ""


def parse_decimal_odds(item: dict) -> Optional[float]:
    """Parse decimal odds from an item that may contain decimal or American odds."""
    price_obj = item.get("price") if isinstance(item.get("price"), dict) else None

    def american_to_decimal(a: float) -> Optional[float]:
        if a >= 100:
            return 1.0 + (a / 100.0)
        if a <= -100:
            return 1.0 + (100.0 / abs(a))
        return None

    # explicit decimal
    for src in (item, price_obj) if price_obj else (item,):
        if not isinstance(src, dict):
            continue
        for k in ("decimal", "odds_decimal", "price_decimal", "decimal_price"):
            v = src.get(k)
            try:
                f = float(v)
                if f >= 1.01:
                    return f
            except Exception:
                pass
    # american
    for src in (item, price_obj) if price_obj else (item,):
        if not isinstance(src, dict):
            continue
        for k in ("american", "odds_american"):
            v = src.get(k)
            try:
                f = float(v)
                dec = american_to_decimal(f)
                if dec:
                    return dec
            except Exception:
                pass
    # generic odds or price
    for k in ("odds", "price"):
        v = item.get(k)
        try:
            f = float(v)
            if f >= 1.01:
                return f
        except Exception:
            pass
    return None


def pick_first(item: dict, keys: list[str]):
    for k in keys:
        if isinstance(item, dict) and k in item and item.get(k) not in (None, ""):
            return item.get(k)
    return None


def _norm_name(v):
    try:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            if s == "" or s.lower() in ("none", "null", "n/a", "na"):
                return None
            return s
        s = str(v).strip()
        if s == "" or s.lower() in ("none", "null", "n/a", "na"):
            return None
        return s
    except Exception:
        return None


def extract_home_away(item: dict) -> tuple[Optional[str], Optional[str]]:
    """Try to extract home/away participant names from various shapes of feed objects."""
    def from_obj(obj: dict) -> tuple[Optional[str], Optional[str]]:
        if not isinstance(obj, dict):
            return (None, None)
        home: str | None = None
        away: str | None = None

        home = pick_first(obj, ["home_team_display"]) or home
        away = pick_first(obj, ["away_team_display"]) or away

        if not home or not away:
            for key in ("participants", "participant", "competitors", "teams", "sides"):
                coll = obj.get(key)
                if isinstance(coll, list) and len(coll) >= 2:
                    def name_of(x):
                        if not isinstance(x, dict):
                            return None
                        return pick_first(x, ["name","team","team_name","full_name","short_name","displayName","home_team","away_team","homeTeam","awayTeam"])
                    n0 = name_of(coll[0])
                    n1 = name_of(coll[1])
                    home = home or n0
                    away = away or n1
                    break

        try:
            sport_val = obj.get("sport") or obj.get("sport_name") or ""
            sport_val = str(sport_val).strip().lower()
        except Exception:
            sport_val = ""
        if (not home or not away) and sport_val in ("tennis", "table_tennis", "table-tennis", "volleyball"):
            def _extract_players_from_obj(o: dict) -> tuple[Optional[str], Optional[str]]:
                try:
                    for k in ("participants","participant","competitors","teams","sides"):
                        v = o.get(k)
                        if isinstance(v, list) and len(v) >= 2:
                            def name_of(x):
                                if not isinstance(x, dict):
                                    return None
                                return x.get("name") or x.get("full_name") or x.get("short_name") or x.get("displayName") or x.get("player") or x.get("team")
                            n0 = name_of(v[0]); n1 = name_of(v[1])
                            if n0 and n1:
                                return (n0, n1)
                except Exception:
                    pass
                return (None, None)
            ph, pa = _extract_players_from_obj(obj)
            home = home or ph
            away = away or pa

        if is_generic_label(home):
            home = None
        if is_generic_label(away):
            away = None
        return (_norm_name(home), _norm_name(away))

    h, a = from_obj(item)
    if h or a:
        return (h, a)
    for k in ("fixture", "event", "match", "game"):
        sub = item.get(k)
        if isinstance(sub, dict):
            h, a = from_obj(sub)
            if h or a:
                return (h, a)
    return (None, None)


def to_epoch_seconds(v) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            if v > 1_000_000_000_000:
                return int(v // 1000)
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            if s.isdigit():
                return int(s)
            from datetime import datetime
            s2 = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
            return int(dt.timestamp())
    except Exception:
        return None
    return None


def extract_start_time(item: dict) -> Optional[int]:
    v = pick_first(item, ["start_time","commence_time","start_date","kickoff","event_date","game_time","fixture_start","start_at","timestamp"])
    if v is None:
        fx = item.get("fixture") or item.get("event") or item.get("match")
        if isinstance(fx, dict):
            v = pick_first(fx, ["start_time","commence_time","start_date","kickoff","start_at","timestamp"])
    return to_epoch_seconds(v)


def extract_league_name(item: dict) -> Optional[str]:
    lg = item.get("league")
    if isinstance(lg, str):
        return lg
    if isinstance(lg, dict):
        return lg.get("name") or lg.get("title") or lg.get("id")
    return None
