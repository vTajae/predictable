
from __future__ import annotations

import os, json, threading
from typing import Callable, Optional

from .config import API_KEY, STREAM_BASE, API_BASE, TRACE_ENABLED, logger
from .catalogue import (
    get_all_active_sportsbooks,
    get_all_sports,
    get_all_sports_verbose,
    get_leagues_for_sport,
    get_leagues_verbose,
)
from .utils import chunk_list

def _chunks_for(sport_id: str) -> tuple[int, int]:
    """Return (sportsbook_chunk_size, league_chunk_size) tuned per sport."""
    sbs = int(os.getenv("SPORTSBOOK_CHUNK_SIZE", "10"))
    lgs = int(os.getenv("LEAGUE_CHUNK_SIZE", "5"))
    if (sport_id or "").lower() == "soccer":
        sbs = int(os.getenv("SPORTSBOOK_CHUNK_SIZE_SOCCER", "6"))
        lgs = int(os.getenv("LEAGUE_CHUNK_SIZE_SOCCER", "3"))
    return sbs, lgs

def subscribe_all_sports(
    sportsbook_chunk_size: int = 20,
    league_chunk_size: int = 10,
    max_workers: int | None = 30,
    allowed_markets: set[str] | None = None,
    arb_threshold_pct: float = 3.0,
    ev_threshold_pct: float = 3.0,
    include_fixture_updates: bool = False,
    max_batches: int | None = None,
    on_payload: Optional[Callable[[dict], None]] = None,
    odds_format: str | dict = "decimal",
    stop_event: threading.Event | None = None,
    sport_allow: set[str] | None = None,
    sportsbook_allow: set[str] | None = None,
    league_allow: set[str] | None = None,
    on_scope: Optional[Callable[[dict], None]] = None,
    # Dependency injection for testability / custom runners
    _sse_worker: Optional[Callable[..., None]] = None,
):
    """Start SSE streams that cover ALL sports and ALL sportsbooks.
    Chunking prevents overly long URLs. Optionally cap worker count.
    """
    if _sse_worker is None:
        # Default to the calculations module's worker to preserve behavior
        try:
            from calculations import sse_worker as _sse_worker  # type: ignore
        except Exception:
            from calculations import sse_worker as _sse_worker  # type: ignore

    # Fetch scope: sportsbooks + sports (+ mapping for names)
    sportsbooks = get_all_active_sportsbooks()
    sports, sport_map = get_all_sports_verbose()

    if not sportsbooks and on_payload:
        try:
            on_payload({"error": {"where": "subscribe_all_sports", "message": "no_sportsbooks", "api_key_present": bool(API_KEY), "sports_count": len(sports or [])}})
        except Exception:
            pass
    if not sports:
        # Fallback mapping if API unavailable
        sport_map = {sid: sid for sid in sports}

    # Restrict sports via env allowlist and/or explicit sport_allow
    allow_env = os.getenv("SPORTS_ALLOWLIST") or os.getenv("SPORTS")
    want_env = {s.strip().lower() for s in allow_env.split(",")} if allow_env else set()
    want = set(x.lower() for x in (sport_allow or set())) | want_env
    if want:
        original_sports = list(sports)
        sports = [
            s for s in sports
            if (s.lower() in want)
            or (sport_map.get(s, "").strip().lower() in want)
            or any(w in (sport_map.get(s, "") or "").strip().lower() for w in want)
        ] or original_sports

    # Restrict sportsbooks by explicit sportsbook_allow (case-insensitive, contains match supported)
    if sportsbook_allow:
        allow_clean = {(sb or "").strip().lower() for sb in sportsbook_allow}
        def matches_allowed(name: str) -> bool:
            nl = (name or "").strip().lower()
            return any(a and a in nl for a in allow_clean)
        filtered = [sb for sb in sportsbooks if matches_allowed(sb)]
        if filtered:
            sportsbooks = filtered
        else:
            # Nothing matched: announce scope and exit
            if on_scope is not None:
                try:
                    on_scope({
                        "control":"stream_scope",
                        "sports": list(sports),
                        "sportsbooks": [],
                        "note": "no_sportsbooks_matched",
                        "filters": {
                            "sport_allow": sorted(list(sport_allow or [])),
                            "sportsbook_allow": sorted(list(sportsbook_allow or [])),
                            "league_allow": sorted(list(league_allow or [])),
                            "allowed_markets": sorted(list(allowed_markets or [])) if allowed_markets else []
                        }
                    })
                except Exception:
                    pass
            return

    # Build scope summary (+ collect league names for visibility)
    league_names = set()
    for s in sports:
        try:
            _ids, _map = get_leagues_verbose(s)
            league_names.update(_map.values())
        except Exception:
            pass
    scope = {
        "sports": list(sports),
        "sportsbooks": list(sportsbooks),
        "filters": {
            "sport_allow": sorted(list(sport_allow or [])),
            "sportsbook_allow": sorted(list(sportsbook_allow or [])),
            "league_allow": sorted(list(league_allow or [])),
            "allowed_markets": (sorted(list(allowed_markets or [])) if allowed_markets else []),
        },
        "leagues": sorted(league_names),
    }
    if on_scope is not None:
        try:
            on_scope({"control": "stream_scope", **scope})
        except Exception:
            pass

    # Create threads per sport (each thread will chunk leagues/books internally)
    stop_event = stop_event or threading.Event()
    threads: list[threading.Thread] = []
    try:
        batches_ctrl = {"remaining": max_batches} if isinstance(max_batches, int) and max_batches > 0 else None

        # Prepare tasks with per-sport leagues (obey league_allow tokens if provided)
        def _clean_token(x: str) -> str:
            import re
            try:
                return re.sub(r"[^a-z0-9]", "", (x or "").lower())
            except Exception:
                return ""

        per_sport: list[tuple[str, list[str]]] = []
        for s in sports:
            l_ids, l_map = get_leagues_verbose(s)
            leagues = list(l_ids)
            if league_allow:
                allow_raw = {(lv or "").strip().lower() for lv in league_allow}
                allow_clean = {_clean_token(lv) for lv in allow_raw}
                leagues_filtered: list[str] = []
                for lid in l_ids:
                    nm = (l_map.get(lid, "") or "").strip().lower()
                    nm_clean = _clean_token(nm)
                    lid_clean = _clean_token(lid)
                    if (lid.strip().lower() in allow_raw) or (nm in allow_raw):
                        leagues_filtered.append(lid)
                        continue
                    if any(a and (a in nm_clean or a in lid_clean or nm_clean in a) for a in allow_clean):
                        leagues_filtered.append(lid)
                if leagues_filtered:
                    leagues = leagues_filtered
                if not leagues and league_allow:
                    leagues = [lv for lv in league_allow if isinstance(lv, str)]
            if leagues:
                per_sport.append((s, leagues))

        # Cap by max_workers
        if isinstance(max_workers, int) and max_workers > 0 and len(per_sport) > max_workers:
            per_sport = per_sport[:max_workers]

        # Start threads
        for idx, (s, leagues) in enumerate(per_sport, start=1):
            sb_chunk_size, lg_chunk_size = _chunks_for(s)
            t = threading.Thread(
                target=_sse_worker,
                args=(
                    s, leagues, sportsbooks, idx, stop_event,
                    include_fixture_updates, allowed_markets, arb_threshold_pct, ev_threshold_pct,
                    batches_ctrl, on_payload, odds_format, on_scope, lg_chunk_size, sb_chunk_size
                ),
                daemon=True,
            )
            t.start()
            threads.append(t)

        # Idle until stop
        while not stop_event.is_set():
            stop_event.wait(1.0)

    except KeyboardInterrupt:
        stop_event.set()
        try:
            for t in threads:
                t.join(timeout=5)
        except KeyboardInterrupt:
            pass
    # Quiet exit
