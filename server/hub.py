
from __future__ import annotations

import asyncio
import json
import traceback
from typing import Set, Dict, Any

from fastapi import WebSocket

from .config import Settings
from .filters import FilterSets, normalize_filter_values, ev_matches, arb_matches
from .transform import group_ev_list, filter_grouped_raw_odds


class Hub:
    """
    Connection hub that tracks per-connection preferences and broadcasts payloads
    to matching recipients.
    """
    def __init__(self, settings: Settings):
        self.settings = settings
        self.connections: Set[WebSocket] = set()
        self.prefs: Dict[WebSocket, Dict[str, Any]] = {}
        self.lock = asyncio.Lock()
        # Fixture participants cache: fx_id -> (home, away)
        self.fx_participants: Dict[str, tuple] = {}

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self.lock:
            self.connections.add(ws)
            self.prefs[ws] = {
                "prod_type": "all",
                "ev_threshold": 0.0,  # 0 = no threshold filter unless client provides one
                "arb_threshold": 0.0,
                "odds_format": self.settings.default_odds_format,
                "odds_holder": {"val": self.settings.default_odds_format},  # mutable holder for background thread
                "filters": {
                    "sport": set(),
                    "market": set(),
                    "sportsbook": set(),
                    "league": set(),
                },
                "quiet_controls": True,
                "debug_scope": False,
                "include_filters_in_payload": False,
            }

    async def disconnect(self, ws: WebSocket):
        async with self.lock:
            self.connections.discard(ws)
            self.prefs.pop(ws, None)

    async def set_prod_type(self, ws: WebSocket, prod_type: str):
        async with self.lock:
            if ws in self.prefs:
                self.prefs[ws]["prod_type"] = prod_type

    async def set_odds_format(self, ws: WebSocket, odds_format: str):
        async with self.lock:
            if ws in self.prefs:
                self.prefs[ws]["odds_format"] = odds_format
                holder = self.prefs[ws].get("odds_holder")
                if isinstance(holder, dict):
                    holder["val"] = odds_format

    async def set_ev_threshold(self, ws: WebSocket, value: float):
        try:
            v = float(value)
        except Exception:
            return
        async with self.lock:
            if ws in self.prefs:
                self.prefs[ws]["ev_threshold"] = v

    async def set_arb_threshold(self, ws: WebSocket, value: float):
        try:
            v = float(value)
        except Exception:
            return
        async with self.lock:
            if ws in self.prefs:
                self.prefs[ws]["arb_threshold"] = v

    async def update_filters(self, ws: WebSocket, updates: Dict[str, Any], *, reset: bool = False):
        """
        Update per-connection filters, normalizing values and supporting aliases.
        """
        keys = ("sport", "market", "sportsbook", "league", "sportbook")
        async with self.lock:
            if ws not in self.prefs:
                return
            if reset:
                self.prefs[ws]["filters"] = {k: set() for k in ("sport", "market", "sportsbook", "league")}
            f = self.prefs[ws].setdefault("filters", {k: set() for k in ("sport", "market", "sportsbook", "league")})
            for k in keys:
                if k in updates:
                    vals = normalize_filter_values(updates.get(k))
                    if k == "sportbook":
                        f["sportsbook"] = set(vals)
                    else:
                        f[k] = set(vals)
            ack_filters = {
                "sport": sorted(list(f.get("sport") or [])),
                "market": sorted(list(f.get("market") or [])),
                "sportsbook": sorted(list(f.get("sportsbook") or [])),
                "league": sorted(list(f.get("league") or [])),
            }
        try:
            async with self.lock:
                quiet = bool(self.prefs.get(ws, {}).get("quiet_controls", False))
            if not quiet:
                await ws.send_text(json.dumps({"control": "filters_updated", "filters": ack_filters}))
        except Exception:
            pass

    async def broadcast(self, payload: Dict[str, Any]):
        """
        Broadcast a payload to all connected websockets, respecting per-connection
        filters and product types.
        """
        is_arb = False
        is_ev_payload = False
        inner = None
        if isinstance(payload, dict) and ("payload" in payload):
            inner = payload.get("payload")
            if isinstance(inner, dict) and "arbitrage" in inner:
                is_arb = True
            elif isinstance(inner, dict) and "ev" in inner and isinstance(inner["ev"], list):
                is_ev_payload = True

        if self.settings.ws_debug:
            try:
                pkeys = list(inner.keys()) if isinstance(inner, dict) else []
                print(json.dumps({"debug": {"where": "broadcast/receive", "is_arb": bool(is_arb), "is_ev": bool(is_ev_payload), "payload_keys": pkeys}}))
            except Exception:
                pass

        async with self.lock:
            targets = list(self.connections)

        dead: list = []
        for ws in targets:
            try:
                async with self.lock:
                    pref = dict(self.prefs.get(ws, {}))  # shallow copy
                ptype = pref.get("prod_type", "all")
                filters = pref.get("filters") or {}
                include_filters = bool(pref.get("include_filters_in_payload", False))
                fs = FilterSets.from_prefs(filters)

                # Helper to attach filters echo if requested
                def filters_echo() -> Dict[str, Any]:
                    try:
                        return {
                            "sport": sorted(list(fs.sport)),
                            "market": sorted(list(fs.market_raw)),
                            "sportsbook": sorted(list(fs.sportsbook_raw)),
                            "league": sorted(list(fs.league_raw)),
                        }
                    except Exception:
                        return {}

                if ptype == "all":
                    # If payload is EV or Arb, apply filters; otherwise passthrough (but attempt grouped filtering)
                    if is_ev_payload:
                        ev_list = (payload.get("payload") or {}).get("ev") or []
                        if isinstance(ev_list, list):
                            ev_list = [e for e in ev_list if ev_matches(e, fs)]
                            if ev_list:
                                out_msg = group_ev_list(ev_list, self.fx_participants)
                                if include_filters:
                                    out_msg["filters"] = filters_echo()
                                await ws.send_text(json.dumps(out_msg, ensure_ascii=False))
                    elif is_arb:
                        arb_obj = (payload.get("payload") or {}).get("arbitrage") or {}
                        if isinstance(arb_obj, dict) and arb_matches(arb_obj, fs):
                            out_msg = dict(payload)
                            if include_filters:
                                out_msg = {"filters": filters_echo(), **out_msg}
                            await ws.send_text(json.dumps(out_msg, ensure_ascii=False))
                    else:
                        obj = payload.get("payload")
                        if isinstance(obj, dict):
                            filtered = filter_grouped_raw_odds(obj, fs)
                            if filtered:
                                if include_filters:
                                    filtered["filters"] = filters_echo()
                                await ws.send_text(json.dumps(filtered, ensure_ascii=False))
                elif ptype == "arbitrage":
                    if is_arb:
                        arb_th = float(pref.get("arb_threshold", self.settings.default_arb_threshold))
                        arb_obj = (payload.get("payload") or {}).get("arbitrage") or {}
                        try:
                            arb_pct = float(arb_obj.get("arbitrage_percent", 0.0))
                        except Exception:
                            arb_pct = 0.0
                        matched = False
                        try:
                            matched = arb_matches(arb_obj, fs)
                        except Exception:
                            matched = False
                        if self.settings.ws_debug:
                            try:
                                print("arb debug: pct=%s threshold=%s matched=%s" % (arb_pct, arb_th, matched))
                            except Exception:
                                pass
                        if (arb_pct >= arb_th) and matched:
                            await ws.send_text(json.dumps({"filters": filters_echo(), **payload}, ensure_ascii=False))
                elif ptype == "ev":
                    if is_ev_payload:
                        ev_th = float(pref.get("ev_threshold", self.settings.default_ev_threshold))
                        ev_list = (payload.get("payload") or {}).get("ev") or []
                        if isinstance(ev_list, list):
                            ev_list = [e for e in ev_list if ev_matches(e, fs)]
                            if ev_th > 0:
                                kept = []
                                for e in ev_list:
                                    try:
                                        evv = float((e or {}).get("ev_value"))
                                    except Exception:
                                        evv = -1e12
                                    if evv >= ev_th:
                                        kept.append(e)
                            else:
                                kept = list(ev_list)
                            if kept:
                                out_msg = group_ev_list(kept, self.fx_participants)
                                if include_filters:
                                    out_msg["filters"] = filters_echo()
                                await ws.send_text(json.dumps(out_msg, ensure_ascii=False))
                else:
                    # Unknown prod_type: passthrough
                    await ws.send_text(json.dumps(payload, ensure_ascii=False))
            except Exception:
                if self.settings.ws_debug:
                    try:
                        print(json.dumps({"error": {"where": "broadcast", "trace": traceback.format_exc()}}))
                    except Exception:
                        pass
                dead.append(ws)

        if dead:
            async with self.lock:
                for ws in dead:
                    self.connections.discard(ws)
                    self.prefs.pop(ws, None)
