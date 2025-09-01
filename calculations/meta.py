
from __future__ import annotations

from typing import Iterable, Optional
import requests
from .state import state_lock, fixture_meta, fixture_meta_fetched
from .extract import extract_home_away, extract_start_time, extract_league_name


def ensure_fixture_meta(sport: str, fixture_id: str, leagues: Optional[list[str]] = None) -> None:
    """Seed fixture_meta[fixture_id] by querying fixtures/active once if needed."""
    try:
        if not fixture_id or fixture_id in fixture_meta_fetched:
            return
        from marketData.getOpticOdds import API_BASE, API_KEY

        base = f"{API_BASE}/fixtures/active?key={API_KEY}&sport={requests.utils.quote(str(sport), safe='')}"
        urls = [
            base + f"&id={requests.utils.quote(str(fixture_id), safe='')}",
            base + f"&fixture_id={requests.utils.quote(str(fixture_id), safe='')}",
        ]
        if leagues:
            rep = "".join(
                f"&league={requests.utils.quote(str(l), safe='')}"
                for l in leagues
                if not isinstance(l, (list, dict, set, tuple))
            )
            urls = [u + rep for u in urls]
        for url in urls:
            try:
                rs = requests.get(url, timeout=15)
                if rs.status_code != 200:
                    continue
                data = rs.json() or {}
                arr = data.get("data") or []
                if isinstance(arr, dict):
                    arr = [arr]
                if not arr:
                    continue
                with state_lock:
                    for it in arr:
                        cands = [it.get("id"), it.get("fixture_id"), it.get("event_id"), it.get("match_id")]
                        for fid in cands:
                            if not fid:
                                continue
                            hm, aw = extract_home_away(it)
                            st = extract_start_time(it)
                            lg = extract_league_name(it)
                            meta = fixture_meta.setdefault(str(fid), {})
                            if hm:
                                meta["home_team"] = hm
                            if aw:
                                meta["away_team"] = aw
                            if st:
                                meta["start_date"] = st
                            if lg:
                                meta["league"] = lg
                        fixture_meta_fetched.add(str(fixture_id))
                break
            except Exception:
                continue
    except Exception:
        pass
