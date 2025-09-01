
from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple

from .state import state_lock, market_state, ev_cache, fixture_meta
from .normalize import normalize_market, is_nonexclusive_market, compose_market, clean_outcome_team_name
from .extract import parse_decimal_odds, extract_home_away, extract_league_name, extract_start_time, extract_deep_link
from .meta import ensure_fixture_meta


def compute_arbitrage(best_odds_by_outcome: Dict[str, tuple[float, str]]) -> Optional[tuple[float, float]]:
    """Given best odds per outcome, return (total_implied, arb_percent) if < 100%."""
    if len(best_odds_by_outcome) < 2:
        return None
    total_implied = 0.0
    for o, _ in best_odds_by_outcome.values():
        try:
            if o and o >= 1.01:
                total_implied += 1.0 / float(o)
        except Exception:
            continue
    if 0.0 < total_implied < 1.0:
        return total_implied, (1.0 - total_implied) * 100.0
    return None


def compute_ev_pct(fair_prob: float, offered_odds: float) -> float:
    """Return EV% given a fair probability and offered decimal odds."""
    try:
        fp = max(0.0, min(1.0, float(fair_prob)))
        od = max(1.0, float(offered_odds))
        return (fp * od - 1.0) * 100.0
    except Exception:
        return 0.0


def process_odds_batch(sport: str, items: List[dict]) -> tuple[List[dict], List[dict]]:
    """Update market_state with incoming items and return (ev_items, arbitrages).

    - EV entries include deep links and fixture metadata (backfilled when missing).
    - Arbitrage entries are market-level opportunities across outcomes.
    """
    affected_keys: set[tuple[str, str, str, bool]] = set()
    # First pass: update state
    with state_lock:
        for it in items:
            market = compose_market(it)
            outcome = (it.get("name") or "").strip()
            sb = it.get("sportsbook")
            if not market or not outcome or not sb:
                continue
            odds = parse_decimal_odds(it)
            if not odds:
                continue
            fixture_id = (
                it.get("fixture_id") or it.get("event_id") or it.get("fixture") or it.get("match_id") or it.get("id")
            )
            if isinstance(fixture_id, dict):
                fixture_id = fixture_id.get("id") or fixture_id.get("fixture_id")
            if not fixture_id:
                continue
            market_norm = normalize_market(market)
            is_live = bool(it.get("is_live"))
            key = (sport, str(fixture_id), market_norm, is_live)
            d = market_state.setdefault(key, {})
            rec = d.setdefault(outcome, {"best_price": 0.0, "book": None, "prices": []})
            rec["prices"].append(float(odds))
            if odds > rec["best_price"]:
                rec["best_price"] = float(odds)
                rec["book"] = sb
            affected_keys.add(key)

    ev_items: List[dict] = []
    arbitrages: List[dict] = []

    for key in affected_keys:
        with state_lock:
            outcome_map = market_state.get(key) or {}
            best = {
                out: (vals.get("best_price"), vals.get("book"))
                for out, vals in outcome_map.items()
                if vals.get("best_price", 0) >= 1.01
            }
            base_probs = {}
            for out, (price, _book) in best.items():
                try:
                    if price and float(price) >= 1.01:
                        base_probs[out] = 1.0 / float(price)
                except Exception:
                    continue

        def infer_teams_from_outcomes() -> tuple[Optional[str], Optional[str]]:
            try:
                outs = list(outcome_map.keys())
                if not outs:
                    return (None, None)
                skip = {"draw", "tie", "over", "under"}
                cand_raw = [o for o in outs if isinstance(o, str) and o.strip()]
                cand = []
                for o in cand_raw:
                    oclean = clean_outcome_team_name(o)
                    if not oclean:
                        continue
                    if oclean.strip().lower() in skip:
                        continue
                    cand.append(oclean)
                uniq: list[str] = []
                seen = set()
                for name in cand:
                    n = name.strip()
                    if n.lower() in seen:
                        continue
                    uniq.append(n)
                    seen.add(n.lower())
                    if len(uniq) >= 2:
                        break
                if len(uniq) == 2:
                    return (uniq[0], uniq[1])
            except Exception:
                pass
            return (None, None)

        fair_probs: dict[str, float] = {}
        if base_probs:
            team_groups: dict[str, list[str]] = {}
            for out in base_probs.keys():
                try:
                    team = clean_outcome_team_name(out).lower()
                except Exception:
                    team = str(out).strip().lower()
                if not team:
                    team = str(out).strip().lower()
                team_groups.setdefault(team, []).append(out)
            for team, outs in team_groups.items():
                if len(outs) < 2:
                    continue
                total = sum(base_probs.get(o, 0.0) for o in outs)
                if 0.6 <= total <= 2.0:
                    for o in outs:
                        bp = base_probs.get(o)
                        if bp:
                            fair_probs[o] = bp / total
            if not fair_probs and len(base_probs) >= 2:
                total = sum(base_probs.values())
                if 0.6 <= total <= 2.0 and not is_nonexclusive_market(key[2]):
                    fair_probs = {out: (bp / total) for out, bp in base_probs.items()}

        # Arbitrage
        arb = compute_arbitrage(best)
        if arb:
            total_impl_prob, arb_pct = arb
            sport_name, fixture_id, market_norm, is_live = key
            ordered = sorted(best.items(), key=lambda kv: kv[1][0], reverse=True)
            if len(ordered) >= 2:
                arbitrages.append(
                    {
                        "sport": sport_name,
                        "fixture_id": str(fixture_id),
                        "market_name": market_norm,
                        "is_live": bool(is_live),
                        "outcomes": [
                            {"name": name, "sports_book_name": (book or ""), "price": float(price or 0.0)}
                            for (name, (price, book)) in ordered
                        ],
                        "total_implied_percent": round(float(total_impl_prob * 100.0), 3),
                        "arbitrage_percent": round(float(arb_pct), 3),
                    }
                )

        if fair_probs:
            sport_name, fixture_id, market_norm, is_live = key
            # Precompute participant name candidates per sportsbook for this fixture (H2H preference)
            try:
                participants_by_sb: dict[str, tuple[str, str] | None] = {}
                agg_names: list[str] = []
                for cand in items:
                    try:
                        fid = (cand.get("fixture_id") or cand.get("event_id") or cand.get("fixture") or cand.get("match_id") or cand.get("id"))
                        if isinstance(fid, dict):
                            fid = fid.get("id") or fid.get("fixture_id")
                        if str(fid) != str(fixture_id):
                            continue
                        if normalize_market(cand.get("market")) != market_norm:
                            continue
                        sb = str(cand.get("sportsbook") or "").strip().lower() or ""
                        name = cand.get("name") or cand.get("outcome") or None
                        if not name:
                            continue
                        low = str(name).strip().lower()
                        if low in {"over","under","odd","even","yes","no"} or re.match(r"^(over|under)\b", low):
                            continue
                        mk = str(cand.get("market") or "").lower()
                        if any(k in mk for k in ("moneyline","match winner","matchwinner","ml","winner")):
                            lst = participants_by_sb.setdefault(sb, [])
                            lst.insert(0, name)
                        else:
                            lst = participants_by_sb.setdefault(sb, [])
                            lst.append(name)
                        agg_names.append(name)
                    except Exception:
                        continue
                for sb_k, lst in list(participants_by_sb.items()):
                    try:
                        seen = set(); uniq = []
                        for n in lst:
                            if n not in seen:
                                uniq.append(n); seen.add(n)
                            if len(uniq) >= 2:
                                break
                        participants_by_sb[sb_k] = (uniq[0], uniq[1]) if len(uniq) >= 2 else None
                    except Exception:
                        participants_by_sb[sb_k] = None
                agg_pair = None
                try:
                    seen = set(); uniq = []
                    for n in agg_names:
                        if n not in seen:
                            uniq.append(n); seen.add(n)
                        if len(uniq) >= 2:
                            break
                    if len(uniq) >= 2:
                        agg_pair = (uniq[0], uniq[1])
                except Exception:
                    agg_pair = None
            except Exception:
                participants_by_sb = {}; agg_pair = None

            for it in items:
                try:
                    if normalize_market(it.get("market")) != market_norm:
                        continue
                    fid = (it.get("fixture_id") or it.get("event_id") or it.get("fixture") or it.get("match_id") or it.get("id"))
                    if isinstance(fid, dict):
                        fid = fid.get("id") or fid.get("fixture_id")
                    if str(fid) != str(fixture_id):
                        continue
                    with state_lock:
                        missing_meta = not fixture_meta.get(str(fixture_id))
                    if missing_meta:
                        ensure_fixture_meta(sport_name, str(fixture_id))
                    out = (it.get("name") or "").strip()
                    if out not in fair_probs:
                        continue
                    od = parse_decimal_odds(it)
                    if not od:
                        continue
                    evv = compute_ev_pct(fair_probs[out], od)
                    with state_lock:
                        meta = fixture_meta.get(str(fixture_id), {}) or {}
                    home, away = extract_home_away(it)
                    # per-sportsbook or aggregate backfill
                    try:
                        if not (meta.get("home_team") or home) or not (meta.get("away_team") or away):
                            sb = str(it.get("sportsbook") or "").strip().lower() or ""
                            p = None
                            if sb and participants_by_sb.get(sb):
                                p = participants_by_sb.get(sb)
                            elif agg_pair:
                                p = agg_pair
                            if p and (not (meta.get("home_team") or home) and not (meta.get("away_team") or away)):
                                try:
                                    home = home or p[0]; away = away or p[1]
                                except Exception:
                                    pass
                    except Exception:
                        pass
                    if not (meta.get("home_team") or home) or not (meta.get("away_team") or away):
                        ih, ia = infer_teams_from_outcomes()
                        if ih and not (meta.get("home_team") or home):
                            home = ih
                        if ia and not (meta.get("away_team") or away):
                            away = ia
                        if ih and ia:
                            with state_lock:
                                fm = fixture_meta.setdefault(str(fixture_id), {})
                                fm.setdefault("home_team", ih)
                                fm.setdefault("away_team", ia)
                    link = extract_deep_link(it)
                    ev_items.append(
                        {
                            "sport": sport_name,
                            "fixture_id": str(fixture_id),
                            "market": market_norm,
                            "market_base": str(it.get("market") or it.get("market_name") or "").strip(),
                            "market_type": str(it.get("type") or it.get("marketType") or it.get("market_type") or "").strip(),
                            "league": meta.get("league") or extract_league_name(it) or "",
                            "home_team": meta.get("home_team") or home or "",
                            "away_team": meta.get("away_team") or away or "",
                            "start_date": meta.get("start_date") or extract_start_time(it),
                            "name": out,
                            "price": float(od),
                            "sportsbook": it.get("sportsbook") or "",
                            "is_live": bool(is_live),
                            "ev_value": round(float(evv), 3),
                            "deep_link": link,
                        }
                    )
                    # Update EV cache
                    try:
                        with state_lock:
                            fid_key = str(fixture_id)
                            sb_key = str(it.get("sportsbook") or "").strip().lower()
                            mk_key = str(market_norm or "").strip().lower()
                            nm_key = str(out or "").strip().lower()
                            if fid_key and sb_key and mk_key and nm_key:
                                ev_cache[(fid_key, sb_key, mk_key, nm_key)] = float(round(float(evv), 3))
                    except Exception:
                        pass
                except Exception:
                    continue
    return ev_items, arbitrages

import re  # needed for a small regex in process_odds_batch
