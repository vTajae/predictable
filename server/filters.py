
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Set, Any


def norm_token(x: str) -> str:
    try:
        return (x or "").strip().lower()
    except Exception:
        return ""


def norm_clean(x: str) -> str:
    try:
        return re.sub(r"[^a-z0-9]", "", (x or "").lower())
    except Exception:
        return ""


def canon_market_text(x: str) -> str:
    """
    Canonicalize market strings for fuzzy matching.
    - Normalize ordinals / periods (q1, q2, h1, h2)
    - Remove ignorable tokens and non-alphanumerics
    """
    try:
        s = (x or "").lower().strip()
        s = re.sub(r"\b(first|1st)\s+quarter\b", " q1 ", s)
        s = re.sub(r"\b(second|2nd)\s+quarter\b", " q2 ", s)
        s = re.sub(r"\b(third|3rd)\s+quarter\b", " q3 ", s)
        s = re.sub(r"\b(fourth|4th)\s+quarter\b", " q4 ", s)
        s = re.sub(r"\b(first|1st)\s+half\b", " h1 ", s)
        s = re.sub(r"\b(second|2nd)\s+half\b", " h2 ", s)
        # short tokens
        s = re.sub(r"\b1h\b", " h1 ", s)
        s = re.sub(r"\b2h\b", " h2 ", s)
        s = re.sub(r"\bq1\b", " q1 ", s)
        s = re.sub(r"\bq2\b", " q2 ", s)
        s = re.sub(r"\bq3\b", " q3 ", s)
        s = re.sub(r"\bq4\b", " q4 ", s)
        # remove ignorable tokens
        for t in ("quarter", "half", "points", "point", "pts"):
            s = s.replace(t, " ")
        s = s.replace("team total points", " team total ")
        s = s.replace("team points", " team total ")
        s = re.sub(r"[^a-z0-9]+", "", s)
        return s
    except Exception:
        return ""


def norm_market(x: str) -> str:
    return canon_market_text(x)


def norm_league_alias(x: str) -> str:
    v = norm_clean(x)
    aliases = {
        "ncaaf": "ncaafootball",
        "ncaafb": "ncaafootball",
        "ncaam": "ncaabasketball",
        "ncaab": "ncaabasketball",
        "ncaaw": "ncaawbasketball",
    }
    if v in aliases:
        return aliases[v]
    return v.replace("collegefootball", "ncaafootball")


def normalize_filter_values(value: Any) -> Set[str]:
    """
    Accept str (comma-separated), list/tuple/set, or scalar and normalize to a lower-cased set.
    """
    vals: Set[str] = set()
    try:
        if value is None:
            return set()
        if isinstance(value, str):
            cand = [p.strip() for p in value.split(",")]
        elif isinstance(value, (list, tuple, set)):
            cand = []
            for it in value:
                try:
                    cand.append(str(it).strip())
                except Exception:
                    continue
        else:
            cand = [str(value).strip()]
        for it in cand:
            if not it:
                continue
            vals.add(it.lower())
    except Exception:
        return set()
    return vals


@dataclass(frozen=True)
class FilterSets:
    sport: Set[str]
    market_raw: Set[str]
    market_norm: Set[str]
    sportsbook_raw: Set[str]
    sportsbook_clean: Set[str]
    league_raw: Set[str]
    league_clean: Set[str]

    @staticmethod
    def from_prefs(filters: Dict[str, Any]) -> "FilterSets":
        sport = normalize_filter_values(filters.get("sport"))
        market_raw = normalize_filter_values(filters.get("market"))
        sportsbook_raw = normalize_filter_values(filters.get("sportsbook") or filters.get("sportbook"))
        league_raw = normalize_filter_values(filters.get("league"))
        return FilterSets(
            sport=sport,
            market_raw=market_raw,
            market_norm={norm_market(v) for v in market_raw},
            sportsbook_raw=sportsbook_raw,
            sportsbook_clean={norm_clean(v) for v in (sportsbook_raw or set())},
            league_raw=league_raw,
            league_clean={norm_league_alias(v) for v in league_raw},
        )


def ev_matches(e: Dict[str, Any], fs: FilterSets) -> bool:
    try:
        if not isinstance(e, dict):
            return False
        if fs.sport:
            v = norm_token(str(e.get("sport") or ""))
            if v not in fs.sport:
                return False
        if fs.market_raw:
            v_raw = str(e.get("market") or "")
            v_norm = norm_market(v_raw)
            if not (v_norm in fs.market_norm or any(fn and fn in v_norm for fn in fs.market_norm)):
                return False
        if fs.sportsbook_raw:
            v = norm_clean(str(e.get("sportsbook") or ""))
            if not (v in fs.sportsbook_clean or any(fv and fv in v for fv in fs.sportsbook_clean)):
                return False
        if fs.league_raw:
            v_clean = norm_league_alias(str(e.get("league") or ""))
            if not any(lv and (lv in v_clean or v_clean in lv) for lv in fs.league_clean):
                return False
        return True
    except Exception:
        return False


def arb_matches(a: Dict[str, Any], fs: FilterSets) -> bool:
    try:
        if not isinstance(a, dict):
            return False
        if fs.sport:
            v = norm_token(str(a.get("sport") or ""))
            if v not in fs.sport:
                return False
        if fs.market_raw:
            v_raw = str(a.get("market_name") or "")
            v_norm = norm_market(v_raw)
            if not (v_norm in fs.market_norm or any(fn and fn in v_norm for fn in fs.market_norm)):
                return False
        if fs.sportsbook_raw:
            # Accept if ANY outcome's sportsbook matches filter
            arr = a.get("outcomes") or []
            ok = False
            for o in arr:
                try:
                    if not isinstance(o, dict):
                        continue
                    sbn = norm_clean(str(o.get("sports_book_name") or ""))
                    if (sbn in fs.sportsbook_clean) or any(fv and fv in sbn for fv in fs.sportsbook_clean):
                        ok = True
                        break
                except Exception:
                    continue
            if not ok:
                return False
        # league not present on arbitrage objects currently
        return True
    except Exception:
        return False
