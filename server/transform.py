
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple, Optional

from .filters import canon_market_text, norm_league_alias, norm_token, FilterSets


def extract_deep_link(entry: Dict[str, Any]) -> str:
    """
    Recursively search a payload/object for a deep link URL in common places.
    """
    try:
        def _search(obj, depth=0):
            if depth > 12:
                return None
            if isinstance(obj, dict):
                # direct url fields first
                for k in ("deep_link"):
                    v = obj.get(k)
                    if isinstance(v, str) and v:
                        return v
                # nested deep_link dicts
                for key in ("deep_link"):
                    dl = obj.get(key)
                    if isinstance(dl, dict):
                        for kk in ("desktop","Desktop"):
                            val = dl.get(kk)
                            if isinstance(val, str) and val:
                                return val
                # common nests
                for key in ("raw", "raw_data", "data", "attributes", "payload"):
                    sub = obj.get(key)
                    if sub is not None:
                        r = _search(sub, depth + 1)
                        if r:
                            return r
                for val in obj.values():
                    r = _search(val, depth + 1)
                    if r:
                        return r
            elif isinstance(obj, (list, tuple, set)):
                for item in obj:
                    r = _search(item, depth + 1)
                    if r:
                        return r
            return None
        return _search(entry) or ""
    except Exception:
        return ""


def _not_generic_team(s: str) -> bool:
    try:
        if not isinstance(s, str):
            return False
        t = s.strip().lower()
        if not t:
            return False
        if t in ("over", "under", "odd", "even", "yes", "no"):
            return False
        # reject strings that start with 'over'/'under'
        if re.match(r"^(over|under)\b", t):
            return False
        # require at least one ascii letter to consider it a valid team
        if not re.search(r"[a-z]", t):
            return False
        return True
    except Exception:
        return False


def _infer_h2h_names_from_odds(odds_list: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    def _base(n: str) -> str:
        s = (n or "").strip()
        if not s:
            return ""
        s = re.sub(r"\s+(?:over|under)\s+[+-]?\d+(?:\.\d+)?$", "", s, flags=re.I)
        s = re.sub(r"\s*[+\-]\d+(?:\.\d+)?$", "", s)
        s = re.sub(r"\s+moneyline$", "", s, flags=re.I)
        s = re.sub(r"\s+\([^)]*\)$", "", s)
        return s.strip()

    def _prefers_h2h(mkt: str) -> bool:
        m = (mkt or "").lower()
        return any(k in m for k in ("moneyline", "match winner", "matchwinner", "ml", "winner"))

    if not isinstance(odds_list, list):
        return (None, None)
    first, rest = [], []
    for o in odds_list:
        if not isinstance(o, dict):
            continue
        raw = o.get("name")
        if not raw:
            continue
        low = str(raw).strip().lower()
        if low in {"over", "under", "odd", "even", "yes", "no"} or re.match(r"^(over|under)\b", low):
            continue
        base = _base(raw)
        if not base:
            continue
        (first if _prefers_h2h(o.get("market")) else rest).append(base)

    names = first + rest
    seen, uniq = set(), []
    for nm in names:
        key = nm.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(nm)
        if len(uniq) == 2:
            break
    return (uniq[0], uniq[1]) if len(uniq) >= 2 else (None, None)


def _sport_display(s: str) -> str:
    return (s or "").replace("_", " ").title() if s else ""


def group_ev_list(ev_list: List[Dict[str, Any]], fx_participants: Dict[str, tuple], include_backfill: bool = True) -> Dict[str, Any]:
    """
    Transform a list of EV entries into the grouped-by-sportsbook structure that the client expects.
    """
    grouped: Dict[str, Any] = {}
    for e in ev_list:
        book = (e.get("sportsbook") or "").strip() or "Unknown"
        book_lower = book.lower()
        fxid = e.get("fixture_id") or ""
        sport_disp = _sport_display(e.get("sport") or "")
        league_disp = e.get("league") or ""
        if isinstance(league_disp, str):
            league_disp = league_disp.upper() if len(league_disp) <= 6 else league_disp
        m_norm = e.get("market") or ""
        home_val = e.get("home_team") or ""
        away_val = e.get("away_team") or ""
        game_obj = {
            "home_team": (home_val if _not_generic_team(home_val) else ""),
            "away_team": (away_val if _not_generic_team(away_val) else ""),
            "id": fxid,
            "start_date": e.get("start_date"),
            "sport": sport_disp,
            "league": league_disp,
        }
        deep_link = extract_deep_link(e)
        odds_obj = {
            "id": f"{fxid}:{book_lower}:{m_norm}:{(e.get('name') or '').lower().replace(' ','_')}",
            "market": (m_norm or "").strip().lower(),
            "sports_book_name": book_lower,
            "deep_link": deep_link,
            "ev_value": e.get("ev_value"),
            "name": e.get("name"),
            "price": e.get("price"),
            "has_been_posted": False,
            "is_live": e.get("is_live"),
        }
        entry = grouped.setdefault(book, {"data": []})
        found = None
        for g in entry["data"]:
            if g.get("id") == fxid:
                found = g
                break
        if not found:
            found = dict(game_obj)
            found["odds"] = []
            entry["data"].append(found)
        else:
            if not (found.get("home_team") or "") and (game_obj.get("home_team") or ""):
                found["home_team"] = game_obj.get("home_team")
            if not (found.get("away_team") or "") and (game_obj.get("away_team") or ""):
                found["away_team"] = game_obj.get("away_team")
        found["odds"].append(odds_obj)

        # Backfill Tennis (and other H2H) participants from odds when missing
        if include_backfill:
            # sanitize generic team names
            if not _not_generic_team(found.get("home_team") or ""):
                found["home_team"] = ""
            if not _not_generic_team(found.get("away_team") or ""):
                found["away_team"] = ""
            fxid_str = str(found.get("id") or "")
            pair = fx_participants.get(fxid_str)
            if pair and not (found["home_team"] or found["away_team"]):
                found["home_team"], found["away_team"] = pair
            if not (found["home_team"] or found["away_team"]):
                sp = (found.get("sport") or "").strip().lower()
                if sp in ("tennis", "table tennis", "table-tennis", "mma", "boxing"):
                    p1, p2 = _infer_h2h_names_from_odds(found.get("odds") or [])
                    if p1 and p2:
                        found["home_team"], found["away_team"] = (p1, p2)
                        if fxid_str:
                            fx_participants[fxid_str] = (p1, p2)
    return {"payload": grouped}


def filter_grouped_raw_odds(obj: Dict[str, Any], fs: FilterSets) -> Dict[str, Any]:
    """
    Apply websocket-level filters to an incoming grouped-odds payload:
    { book: { data: [ { sport, league, odds: [ { market or market_name } ] } ] } }
    Only keep items that match, and ensure deep_link is present.
    """
    def league_match(lg: str) -> bool:
        if not fs.league_raw:
            return True
        l = norm_league_alias(lg)
        return any(fl and (fl in l or l in fl) for fl in fs.league_clean)

    def sport_match(sp: str) -> bool:
        if not fs.sport:
            return True
        return norm_token(sp) in fs.sport

    def market_match(m: str) -> bool:
        if not fs.market_raw:
            return True
        mclean = canon_market_text(m)
        for fm in fs.market_raw:
            fmc = canon_market_text(fm)
            if fmc and (fmc in mclean):
                return True
        return False

    grouped = {}
    for book, data in obj.items():
        # sportsbook filtering: compare normalized names with contains semantics
        if fs.sportsbook_raw:
            s_clean = re.sub(r"[^a-z0-9]", "", book.lower())
            if not any(fv and fv in s_clean for fv in fs.sportsbook_clean):
                continue

        block = data if isinstance(data, dict) else {}
        games = block.get("data", [])
        out_games = []
        for g in games:
            if not isinstance(g, dict):
                continue
            if not sport_match(g.get("sport")):
                continue
            if not league_match(g.get("league")):
                continue
            odds = g.get("odds") if isinstance(g.get("odds"), list) else []
            out_odds = []
            for o in odds:
                if not isinstance(o, dict):
                    continue
                if not market_match(o.get("market") or o.get("market_name") or ""):
                    continue
                o2 = dict(o)
                if "deep_link" not in o2 or not o2.get("deep_link"):
                    o2["deep_link"] = extract_deep_link(o2)
                out_odds.append(o2)
            if out_odds:
                gg = dict(g)
                gg["odds"] = out_odds
                out_games.append(gg)
        if out_games:
            grouped[book] = {"data": out_games}
    return {"payload": grouped} if grouped else {}
