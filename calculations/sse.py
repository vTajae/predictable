
from __future__ import annotations

import json
import os
import time
import traceback
from typing import Callable, Optional

import requests
import sseclient
from ssl import SSLError
from requests import RequestException
from requests.exceptions import ChunkedEncodingError

from .state import state_lock, fixture_meta
from .extract import extract_home_away, extract_start_time, extract_league_name
from .normalize import compact_token, soft_tokens, canon_market_text
from .evcalc import process_odds_batch


def _build_allowed_markets_pred(allowed_markets: Optional[set[str]]):
    """Return a predicate m_ok(it) that tests an odds item against allowed markets; None -> allow all."""
    if allowed_markets is None:
        return None

    # Normalize allowed tokens
    am_list = [str(x) for x in allowed_markets if str(x).strip()]
    am_norm = {compact_token(x) for x in am_list}
    am_tokens = [soft_tokens(x) for x in am_list]

    def m_ok(itm: dict) -> bool:
        try:
            if not isinstance(itm, dict):
                return False
            fields = []
            for k in ("market","market_name","marketType","type","market_type","period","bet_period","segment","scope"):
                v = itm.get(k)
                if isinstance(v, (str, int, float)):
                    fields.append(str(v))
            f_clean = [compact_token(v) for v in fields if v]
            f_soft = [(" " + v.strip().lower() + " ") for v in fields if v]

            for mn in f_clean:
                for a in am_norm:
                    if a and (a in mn):
                        return True
            # all words present check (soft)
            for words in am_tokens:
                if words and all(any((" " + w + " ") in fs for fs in f_soft) for w in words):
                    return True
        except Exception:
            return False
        return False

    return m_ok


def sse_worker(
    sport: str,
    leagues: list[str],
    sportsbooks: list[str],
    worker_id: int,
    stop_event,
    include_fixture_updates: bool,
    allowed_markets: set[str] | None,
    arb_threshold_pct: float,
    ev_threshold_pct: float,
    batches_ctrl: dict | None = None,
    on_payload: Optional[Callable[[dict], None]] = None,
    odds_format: object | str = "decimal",
    on_scope: Optional[Callable[[dict], None]] = None,
    league_chunk_size: int = 5,
    sportsbook_chunk_size: int = 10,
):
    """Stream raw odds from OpticOdds and forward structured JSON lines.

    This worker batches odds entries, updates in-memory state, and emits:
      - grouped raw odds payloads (by sportsbook)
      - EV lists
      - arbitrage opportunities
    """
    from marketData.getOpticOdds import API_KEY, STREAM_BASE, API_BASE

    debug = os.getenv("WS_DEBUG", "0").strip().lower() in ("1","true","yes","on") or os.getenv("SSE_VERBOSE", "0").strip().lower() in ("1","true","yes")
    lg_size = league_chunk_size
    sb_size = sportsbook_chunk_size

    try:
        from marketData.utils import chunk_list
        lg_subchunks: list[list[str]] = chunk_list(list(leagues or []), lg_size)
        sb_subchunks: list[list[str]] = chunk_list(list(sportsbooks or []), sb_size)
    except Exception:
        lg_subchunks = [list(leagues)] if leagues else []
        sb_subchunks = [list(sportsbooks)] if sportsbooks else []

    last_entry_id = None
    lg_idx = 0
    sb_idx = 0
    backoff = 2
    max_backoff = 30

    # Seed fixture metadata using fixtures/active
    try:
        base_url = f"{API_BASE}/fixtures/active?key={API_KEY}&sport={requests.utils.quote(str(sport), safe='')}"
        seed_url = base_url
        for lg in leagues or []:
            if isinstance(lg, (list, dict, set, tuple)):
                continue
            try:
                seed_url += f"&league={requests.utils.quote(str(lg), safe='')}"
            except Exception:
                seed_url += f"&league={str(lg)}"
        try:
            rs = requests.get(seed_url, timeout=30)
            if rs.status_code == 200:
                payload = rs.json() or {}
                arr = payload.get("data") or []
                if isinstance(arr, dict):
                    arr = [arr]
                if arr:
                    with state_lock:
                        for it in arr:
                            fid = (it.get("id") or it.get("fixture_id") or it.get("event_id") or it.get("match_id"))
                            if not fid:
                                continue
                            hm, aw = extract_home_away(it)
                            st = extract_start_time(it)
                            lg = extract_league_name(it)
                            meta = fixture_meta.setdefault(str(fid), {})
                            if hm: meta["home_team"] = hm
                            if aw: meta["away_team"] = aw
                            if st: meta["start_date"] = st
                            if lg: meta["league"] = lg
        except Exception:
            pass
    except Exception:
        pass

    observed_markets: set[str] = set()
    observed_leagues: set[str] = set()
    observed_books: set[str] = set()
    last_scope_counts = (0, 0, 0)

    m_ok = _build_allowed_markets_pred(allowed_markets)

    while not stop_event.is_set():
        try:
            lg_chunk = lg_subchunks[lg_idx % len(lg_subchunks)] if lg_subchunks else []
            sb_chunk = sb_subchunks[sb_idx % len(sb_subchunks)] if sb_subchunks else []

            url = f"{STREAM_BASE}/{sport}?key={API_KEY}"
            for lg in lg_chunk:
                if isinstance(lg, (list, dict, set, tuple)):
                    continue
                try:
                    url += f"&league={requests.utils.quote(str(lg), safe='')}"
                except Exception:
                    url += f"&league={str(lg)}"
            for sb in sb_chunk:
                try:
                    url += f"&sportsbook={requests.utils.quote(str(sb), safe='')}"
                except Exception:
                    url += f"&sportsbook={str(sb)}"
            if include_fixture_updates:
                url += "&include_fixture_updates=true"
            url += "&include_deep_link=true"
            fmt = (str(odds_format.get("val", "decimal")) if isinstance(odds_format, dict) else str(odds_format or "decimal"))
            url += f"&odds_format={fmt}"

            if debug:
                try:
                    print(json.dumps({"debug":{"where":"sse/request","sport":sport,"lg_chunk":lg_chunk[:10]+(["..."] if len(lg_chunk)>10 else []),"sb_chunk":sb_chunk}}))
                except Exception:
                    pass

            headers = {"Last-Event-ID": str(last_entry_id)} if last_entry_id else {}
            try:
                r = requests.get(url, stream=True, timeout=(5, 45), headers=headers)
            except RequestException as e:
                try:
                    print(json.dumps({"error":{"where":"sse/request","message":str(e),"url":url}}))
                except Exception:
                    pass
                time.sleep(min(backoff, max_backoff))
                backoff = min(backoff * 2, max_backoff)
                if lg_subchunks: lg_idx += 1
                if sb_subchunks: sb_idx += 1
                continue
            if r.status_code != 200:
                try:
                    print(json.dumps({"error":{"where":"sse/status","status":r.status_code,"message":r.text[:200]}}))
                except Exception:
                    pass
                if r.status_code in (400, 414):
                    def split_chunks(chunks: list[list[str]]) -> list[list[str]]:
                        new_chunks: list[list[str]] = []
                        for ch in chunks:
                            if len(ch) > 1:
                                mid = len(ch) // 2
                                new_chunks.append(ch[:mid]); new_chunks.append(ch[mid:])
                            else:
                                new_chunks.append(ch)
                        return [c for c in new_chunks if c]
                    if lg_subchunks and any(len(c) > 1 for c in lg_subchunks):
                        lg_subchunks = split_chunks(lg_subchunks)
                    if sb_subchunks and any(len(c) > 1 for c in sb_subchunks):
                        sb_subchunks = split_chunks(sb_subchunks)
                    lg_idx += 1; sb_idx += 1
                time.sleep(min(backoff, max_backoff))
                backoff = min(backoff * 2, max_backoff)
                continue

            client = sseclient.SSEClient(r)
            backoff = 2

            for event in client.events():
                if stop_event.is_set():
                    break

                if event.event in ("odds", "locked-odds"):
                    try:
                        data = json.loads(event.data)
                    except Exception:
                        continue
                    last_entry_id = data.get("entry_id")
                    arr = data.get("data") or []

                    # Update observed scope
                    try:
                        changed = False
                        if isinstance(arr, list) and arr:
                            for it in arr[:200]:
                                if not isinstance(it, dict):
                                    continue
                                mk = str((it.get("market_name") or it.get("market") or "")).strip()
                                if mk and mk not in observed_markets:
                                    observed_markets.add(mk); changed = True
                                lg = extract_league_name(it)
                                if lg:
                                    s = str(lg).strip()
                                    if s and s not in observed_leagues:
                                        observed_leagues.add(s); changed = True
                                sb = str(it.get("sportsbook") or "").strip()
                                if sb and sb not in observed_books:
                                    observed_books.add(sb); changed = True
                        if changed and on_scope is not None:
                            cts = (len(observed_markets), len(observed_leagues), len(observed_books))
                            if cts != last_scope_counts:
                                last_scope_counts = cts
                                try:
                                    on_scope({"control":"observed_scope","sport":sport,"markets":sorted(list(observed_markets))[:50],"leagues":sorted(list(observed_leagues))[:50],"sportsbooks":sorted(list(observed_books))[:50]})
                                except Exception:
                                    pass
                    except Exception:
                        pass

                    # Optional allowed market filtering
                    if m_ok is not None and isinstance(arr, list):
                        before_mkt = len(arr)
                        arr = [it for it in arr if m_ok(it)]
                        if debug:
                            try:
                                print(json.dumps({"debug":{"where":"sse/market_filter","sport":sport,"before":before_mkt,"after":len(arr),"allowed_markets":sorted(list(allowed_markets or []))}}))
                            except Exception:
                                pass

                    if arr:
                        # Update fixture meta opportunistically
                        with state_lock:
                            for it in arr:
                                if not isinstance(it, dict):
                                    continue
                                fid = (it.get("fixture_id") or it.get("event_id") or it.get("fixture") or it.get("match_id") or it.get("id"))
                                if isinstance(fid, dict):
                                    fid = fid.get("id") or fid.get("fixture_id")
                                if not fid:
                                    continue
                                hm, aw = extract_home_away(it)
                                st = extract_start_time(it)
                                lg = extract_league_name(it)
                                meta = fixture_meta.setdefault(str(fid), {})
                                if hm: meta["home_team"] = hm
                                if aw: meta["away_team"] = aw
                                if st: meta["start_date"] = st
                                if lg: meta["league"] = lg

                        # Compute EV & arbitrage, and emit grouped payloads
                        evs, arbs = process_odds_batch(sport, arr)

                        if debug:
                            try:
                                sample = [{"market":str(it.get("market") or ""), "sportsbook":str(it.get("sportsbook") or "")} for it in arr[:2] if isinstance(it, dict)]
                                dbg = {"debug":{"where":"sse/processed","sport":sport,"items":len(arr),"ev":len(evs or []),"arb":len(arbs or []),"sample":sample}}
                                print(json.dumps(dbg))
                            except Exception:
                                pass

                        # Build grouped raw-odds payload (so "all" mode clients can render immediately)
                        try:
                            grouped: dict[str, dict] = {}
                            # infer teams per fixture from outcome labels if missing
                            inferred_by_fx: dict[str, tuple[str,str]] = {}
                            try:
                                candidates: dict[str, list[str]] = {}
                                for it in arr:
                                    if not isinstance(it, dict):
                                        continue
                                    fxid0 = (it.get("fixture_id") or it.get("event_id") or it.get("fixture") or it.get("match_id") or it.get("id"))
                                    if isinstance(fxid0, dict):
                                        fxid0 = fxid0.get("id") or fxid0.get("fixture_id")
                                    if not fxid0:
                                        continue
                                    nm0 = str(it.get("name") or it.get("outcome") or "").strip()
                                    base = nm0
                                    # lightweight cleanup to remove O/U suffixes, moneyline, parentheses
                                    import re as _re
                                    base = _re.sub(r"\s+(?:over|under)\s+[+\-]?\d+(?:\.\d+)?$", "", base, flags=_re.I)
                                    base = _re.sub(r"\s+moneyline$", "", base, flags=_re.I)
                                    base = _re.sub(r"\s+\([^)]*\)$", "", base).strip()
                                    if base and base.lower() not in ("draw","tie","over","under"):
                                        candidates.setdefault(str(fxid0), []).append(base)
                                for fid, names in candidates.items():
                                    uniq = []; seen=set()
                                    for nm in names:
                                        low = nm.lower()
                                        if low in ("draw","tie","over","under"):
                                            continue
                                        if low in seen: continue
                                        uniq.append(nm); seen.add(low)
                                        if len(uniq) >= 2: break
                                    if len(uniq) >= 2:
                                        inferred_by_fx[str(fid)] = (uniq[0], uniq[1])
                            except Exception:
                                inferred_by_fx = {}

                            # EV lookup to annotate odds where available
                            ev_map: dict[tuple[str,str,str,str], float] = {}
                            try:
                                for evi in evs or []:
                                    try:
                                        fid = str(evi.get("fixture_id") or "")
                                        sb = str(evi.get("sportsbook") or "").strip().lower()
                                        mk = str(evi.get("market") or "").strip().lower()
                                        nm = str(evi.get("name") or "").strip().lower()
                                        if fid and sb and mk and nm:
                                            ev_map[(fid, sb, mk, nm)] = float(evi.get("ev_value") or 0.0)
                                    except Exception:
                                        continue
                            except Exception:
                                pass

                            for it in arr:
                                if not isinstance(it, dict):
                                    continue
                                book = (it.get("sportsbook") or "").strip() or "Unknown"
                                fxid = (it.get("fixture_id") or it.get("event_id") or it.get("fixture") or it.get("match_id") or it.get("id") or "")
                                if isinstance(fxid, dict):
                                    fxid = fxid.get("id") or fxid.get("fixture_id") or ""
                                market = str(it.get("market") or it.get("market_name") or "").strip()
                                home, away = extract_home_away(it)
                                if not home or not away:
                                    with state_lock:
                                        meta_fx = fixture_meta.get(str(fxid), {}) or {}
                                    home = home or (meta_fx.get("home_team") or "")
                                    away = away or (meta_fx.get("away_team") or "")
                                if (not home or not away) and str(fxid) in inferred_by_fx:
                                    ih, ia = inferred_by_fx[str(fxid)]
                                    home = home or ih; away = away or ia

                                start_date = extract_start_time(it)
                                league_name = extract_league_name(it) or ""
                                odds_price = None
                                try:
                                    v = it.get("price")
                                    odds_price = float(v) if isinstance(v,(int,float,str)) and str(v) not in ("","None") else None
                                except Exception:
                                    odds_price = None
                                name = str(it.get("name") or it.get("outcome") or "")
                                deep_link_val = ""
                                try:
                                    dl = it.get("deep_link")
                                    if isinstance(dl, dict):
                                        deep_link_val = dl.get("desktop") or dl.get("Desktop") or ""
                                except Exception:
                                    deep_link_val = ""

                                odds_obj = {
                                    "id": f"{fxid}:{book.lower()}:{(market or '').lower()}:{name.lower().replace(' ', '_')}",
                                    "market": (market or '').strip().lower(),
                                    "sports_book_name": book.lower(),
                                    "deep_link": deep_link_val,
                                    "ev_value": ev_map.get((str(fxid), book.lower(), (market or '').strip().lower(), name.strip().lower())),
                                    "name": name if name else None,
                                    "price": odds_price,
                                    "has_been_posted": False,
                                    "is_live": bool(it.get("is_live")),
                                }
                                entry = grouped.setdefault(book, {"data": []})
                                found = None
                                for g in entry["data"]:
                                    if g.get("id") == fxid:
                                        found = g
                                        break
                                if not found:
                                    entry["data"].append({
                                        "home_team": home or "",
                                        "away_team": away or "",
                                        "id": str(fxid),
                                        "start_date": start_date,
                                        "sport": str(sport).replace("_", " ").title(),
                                        "league": league_name,
                                        "odds": [odds_obj],
                                    })
                                else:
                                    if not (found.get("home_team") or "") and (home or ""):
                                        found["home_team"] = home
                                    if not (found.get("away_team") or "") and (away or ""):
                                        found["away_team"] = away
                                    if not found.get("start_date") and start_date:
                                        found["start_date"] = start_date
                                    if not (found.get("league") or "") and (league_name or ""):
                                        found["league"] = league_name
                                    found["odds"].append(odds_obj)

                            if grouped:
                                if on_payload is not None:
                                    on_payload({"payload": grouped})
                                else:
                                    print(json.dumps({"payload": grouped}, ensure_ascii=False))
                        except Exception:
                            pass

                        try:
                            if on_payload is not None:
                                if evs:
                                    on_payload({"payload": {"ev": evs}})
                                for arb in arbs:
                                    on_payload({"payload": {"arbitrage": arb}})
                            else:
                                if evs:
                                    print(json.dumps({"payload": {"ev": evs}}, ensure_ascii=False))
                                for arb in arbs:
                                    print(json.dumps({"payload": {"arbitrage": arb}}, ensure_ascii=False))
                        except Exception:
                            pass

                        if batches_ctrl is not None and isinstance(batches_ctrl.get("remaining"), int):
                            batches_ctrl["remaining"] -= 1
                            if batches_ctrl["remaining"] <= 0:
                                stop_event.set()

                elif event.event == "fixture-status":
                    try:
                        data = json.loads(event.data)
                        arr = data.get("data") or []
                    except Exception:
                        arr = []
                    if arr:
                        with state_lock:
                            for it in arr:
                                if not isinstance(it, dict):
                                    continue
                                fid = (it.get("fixture_id") or it.get("event_id") or it.get("fixture") or it.get("match_id") or it.get("id"))
                                if isinstance(fid, dict):
                                    fid = fid.get("id") or fid.get("fixture_id")
                                if not fid:
                                    continue
                                hm, aw = extract_home_away(it)
                                st = extract_start_time(it)
                                lg = extract_league_name(it)
                                meta = fixture_meta.setdefault(str(fid), {})
                                if hm: meta["home_team"] = hm
                                if aw: meta["away_team"] = aw
                                if st: meta["start_date"] = st
                                if lg: meta["league"] = lg
                else:
                    pass

        except ChunkedEncodingError as e:
            try:
                print(json.dumps({"error":{"where":"sse/chunked","message":str(e)}}))
            except Exception:
                pass
            time.sleep(min(backoff, max_backoff)); backoff = min(backoff * 2, max_backoff)
        except (SSLError, RequestException) as e:
            try:
                print(json.dumps({"error":{"where":"sse/request-loop","message":str(e)}}))
            except Exception:
                pass
            if lg_subchunks: lg_idx += 1
            if sb_subchunks: sb_idx += 1
            time.sleep(min(backoff, max_backoff)); backoff = min(backoff * 2, max_backoff)
        except Exception as e:
            try:
                print(json.dumps({"error":{"where":"sse/loop","message":str(e),"trace":traceback.format_exc()}}))
            except Exception:
                pass
            time.sleep(min(backoff, max_backoff)); backoff = min(backoff * 2, max_backoff)
