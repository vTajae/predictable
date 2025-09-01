
from __future__ import annotations

import re
from typing import Any, Iterable


GENERIC_OUTCOME_TOKENS = {"over", "under", "yes", "no", "odd", "even"}


def is_generic_label(val: str) -> bool:
    """Return True when a string is a generic non-team outcome (Over/Under/etc.)."""
    if not isinstance(val, str):
        return False
    s = val.strip().lower()
    if not s:
        return False
    if s in GENERIC_OUTCOME_TOKENS:
        return True
    # "Over 11.5" / "Under 35"
    if re.match(r"^(over|under)\s+[+\-]?\d+(?:\.\d+)?$", s, flags=re.I):
        return True
    # Defensive: token plus non-letters only
    if re.match(r"^(over|under|yes|no|odd|even)[^a-zA-Z]*$", s, flags=re.I):
        return True
    return False


def normalize_market(m: str) -> str:
    return (m or "").strip().lower()


def is_nonexclusive_market(market_norm: str) -> bool:
    """Return True for markets where outcomes are not mutually exclusive."""
    s = (market_norm or "").lower()
    if (
        "scorer" in s
        or "to score" in s
        or "touchdown" in s
        or "goalscorer" in s
        or "home run" in s
    ) and not ("first" in s or "1st" in s):
        return True
    if "anytime" in s and any(t in s for t in ("td", "touchdown", "goal", "home run", "scorer")):
        return True
    return False


def compose_market(it: dict) -> str:
    """Compose a market string that includes period/segment when available."""
    try:
        base = str(it.get("market") or it.get("market_name") or "").strip()
        seg_candidates = []
        for k in ("period", "bet_period", "segment", "scope", "type", "marketType", "market_type"):
            v = it.get(k)
            if isinstance(v, (str, int, float)):
                s = str(v).strip()
                if s:
                    seg_candidates.append(s)
        seg = next((s for s in seg_candidates if s), "")
        if seg:
            low = base.lower()
            if seg.lower() not in low:
                return f"{seg} {base}".strip()
        return base
    except Exception:
        return str(it.get("market") or "").strip()


def clean_outcome_team_name(name: str) -> str:
    """Attempt to derive the team/player name from an outcome label."""
    try:
        s = (name or "").strip()
        if not s:
            return ""
        s = re.sub(r"\s+(?:over|under)\s+[+\-]?\d+(?:\.\d+)?$", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s+moneyline$", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s+\([^)]*\)$", "", s)
        return s.strip()
    except Exception:
        return (name or "").strip()


def compact_token(s: str) -> str:
    """Compact string for contains comparisons: lower-case, remove spaces, separators, slashes."""
    return (s or "").strip().lower().replace(" ", "").replace("_", "").replace("-", "").replace("/", "")


def soft_tokens(s: str) -> tuple[str, ...]:
    """Lower-cased tokens, keeping word boundaries (used for 'all words present' checks)."""
    return tuple(t for t in (s or "").strip().lower().replace("-", " ").replace("_", " ").split() if t)


def norm_clean_alnum(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def canon_market_text(x: str) -> str:
    """Canonicalize market strings for fuzzy matching: q1/q2/h1/h2, remove ignorable tokens."""
    try:
        s = (x or "").lower().strip()
        s = re.sub(r"\b(first|1st)\s+quarter\b", " q1 ", s)
        s = re.sub(r"\b(second|2nd)\s+quarter\b", " q2 ", s)
        s = re.sub(r"\b(third|3rd)\s+quarter\b", " q3 ", s)
        s = re.sub(r"\b(fourth|4th)\s+quarter\b", " q4 ", s)
        s = re.sub(r"\b(first|1st)\s+half\b", " h1 ", s)
        s = re.sub(r"\b(second|2nd)\s+half\b", " h2 ", s)
        s = re.sub(r"\b1h\b", " h1 ", s)
        s = re.sub(r"\b2h\b", " h2 ", s)
        s = re.sub(r"\bq1\b", " q1 ", s)
        s = re.sub(r"\bq2\b", " q2 ", s)
        s = re.sub(r"\bq3\b", " q3 ", s)
        s = re.sub(r"\bq4\b", " q4 ", s)
        for t in ("quarter", "half", "points", "point", "pts"):
            s = s.replace(t, " ")
        s = s.replace("team total points", " team total ")
        s = s.replace("team points", " team total ")
        s = re.sub(r"[^a-z0-9]+", "", s)
        return s
    except Exception:
        return ""
