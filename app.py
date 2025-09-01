
from __future__ import annotations

import asyncio
import json
import os
import threading
from typing import Any, Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
import uvicorn

from opticOdds.catalogue import get_all_active_sportsbooks

from server.config import Settings
from server.hub import Hub


settings = Settings()
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
hub = Hub(settings)


@app.get("/health")
async def health():
    return PlainTextResponse("ok")


@app.websocket("/stream")
async def stream(ws: WebSocket):
    await hub.connect(ws)
    try:
        loop = asyncio.get_running_loop()
        stream_thread = None
        stream_stop_evt = None

        def on_payload(payload: Dict[str, Any]):
            asyncio.run_coroutine_threadsafe(hub.broadcast(payload), loop)

        def on_scope(scope_payload: Dict[str, Any]):
            # Forward scope summary to this connection only if debug_scope enabled
            try:
                async def _send_if_needed():
                    async with hub.lock:
                        dbg = bool(hub.prefs.get(ws, {}).get("debug_scope", False))
                        quiet = bool(hub.prefs.get(ws, {}).get("quiet_controls", False))
                    if dbg and not quiet:
                        await ws.send_text(json.dumps(scope_payload))
                asyncio.run_coroutine_threadsafe(_send_if_needed(), loop)
            except Exception:
                pass

        # Helper to start the streaming worker lazily, after client sends first control message
        async def start_streaming_if_needed():
            nonlocal stream_thread, stream_stop_evt
            if stream_thread is not None:
                try:
                    if hasattr(stream_thread, "is_alive") and not stream_thread.is_alive():
                        stream_thread = None
                    else:
                        return
                except Exception:
                    return

            # Snapshot stream filter sets from current prefs (per-connection)
            async with hub.lock:
                prefs = hub.prefs.get(ws, {})
                odds_holder = prefs.get("odds_holder", {"val": settings.default_odds_format})
                filters = prefs.get("filters", {})

                def to_set(v):
                    if isinstance(v, (list, set, tuple)):
                        return {str(x).strip().lower() for x in v}
                    if isinstance(v, str):
                        return {s.strip().lower() for s in v.split(",") if s.strip()}
                    return set()

                sport_allow = to_set(filters.get("sport"))
                sportsbook_allow = to_set(filters.get("sportsbook"))
                league_allow = to_set(filters.get("league"))
                market_allow = to_set(filters.get("market"))

            stream_stop_evt = threading.Event()
            stream_thread = threading.Thread(
                target=get_all_active_sportsbooks,
                kwargs=dict(
                    sportsbook_chunk_size=settings.sportsbook_chunk_size,
                    league_chunk_size=settings.league_chunk_size,
                    max_workers=settings.max_workers,
                    allowed_markets=settings.allowed_markets,  # keep env-only
                    include_fixture_updates=settings.include_fixture_updates,
                    max_batches=None,
                    on_payload=on_payload,
                    on_scope=on_scope,
                    odds_format=odds_holder,
                    stop_event=stream_stop_evt,
                    # only apply ingest filters when explicitly enabled
                    sport_allow=sport_allow if (settings.ingest_filters_enabled and sport_allow) else None,
                    sportsbook_allow=sportsbook_allow if (settings.ingest_filters_enabled and sportsbook_allow) else None,
                    league_allow=league_allow if (settings.ingest_filters_enabled and league_allow) else None,
                ),
                daemon=True,
            )
            stream_thread.start()
            nonlocal current_stream_filters
            current_stream_filters = {
                "sport": sorted(list(sport_allow)),
                "sportsbook": sorted(list(sportsbook_allow)),
                "league": sorted(list(league_allow)),
                "market": sorted(list(market_allow)),
            }

        # Keep the websocket open; accept control messages to set prod_type, filters; start/restart stream as needed
        current_stream_filters = {"sport": [], "sportsbook": [], "league": [], "market": []}
        while True:
            try:
                msg = await ws.receive_text()
                try:
                    data = json.loads(msg)
                except Exception:
                    data = None
                if isinstance(data, dict):
                    v = str(data.get("prod_type", "")).strip().lower()
                    if v in ("ev", "arbitrage", "all"):
                        await hub.set_prod_type(ws, v)
                    # Optional odds format update
                    if "odds_format" in data:
                        ofmt = str(data.get("odds_format") or "").strip().lower()
                        if ofmt in ("decimal", "american"):
                            await hub.set_odds_format(ws, ofmt)
                    if "ev_threshold" in data:
                        await hub.set_ev_threshold(ws, data.get("ev_threshold"))
                    if "arb_threshold" in data:
                        await hub.set_arb_threshold(ws, data.get("arb_threshold"))
                    # Top-level filter keys
                    filter_updates = {}
                    for fk in ("sport", "market", "sportsbook", "league", "sportbook"):
                        if fk in data:
                            filter_updates[fk] = data.get(fk)
                    # Combined filters object
                    fobj = data.get("filters")
                    filters_reset = False
                    if isinstance(fobj, dict):
                        if not fobj:
                            filters_reset = True
                        for fk in ("sport", "market", "sportsbook", "league", "sportbook"):
                            if fk in fobj:
                                filter_updates[fk] = fobj.get(fk)
                        if str(fobj.get("replace") or fobj.get("clear") or fobj.get("reset") or "").strip().lower() in ("1", "true", "yes", "on"):
                            filters_reset = True
                    if str(data.get("filters_replace") or data.get("filters_clear") or data.get("clear_filters") or "").strip().lower() in ("1", "true", "yes", "on"):
                        filters_reset = True

                    did_restart = False
                    # Optional toggles: quiet controls and debug scope acks
                    if "quiet" in data:
                        try:
                            async with hub.lock:
                                hub.prefs[ws]["quiet_controls"] = bool(data.get("quiet"))
                        except Exception:
                            pass
                    if "ack" in data:
                        try:
                            async with hub.lock:
                                hub.prefs[ws]["quiet_controls"] = not bool(data.get("ack"))
                        except Exception:
                            pass
                    if "debug_scope" in data:
                        try:
                            async with hub.lock:
                                hub.prefs[ws]["debug_scope"] = bool(data.get("debug_scope"))
                        except Exception:
                            pass

                    if filter_updates:
                        await hub.update_filters(ws, filter_updates, reset=filters_reset)
                        # Decide whether to restart streaming with updated upstream filters
                        async with hub.lock:
                            prefs2 = hub.prefs.get(ws, {})
                            fl = prefs2.get("filters", {})

                            def to_set(v):
                                if isinstance(v, (list, set, tuple)):
                                    return {str(x).strip().lower() for x in v}
                                if isinstance(v, str):
                                    return {s.strip().lower() for s in v.split(",") if s.strip()}
                                return set()

                            snap = {
                                "sport": sorted(list(to_set(fl.get("sport")))),
                                "sportsbook": sorted(list(to_set(fl.get("sportsbook")))),
                                "league": sorted(list(to_set(fl.get("league")))),
                                "market": sorted(list(to_set(fl.get("market")))),
                            }
                        if snap != current_stream_filters:
                            try:
                                if stream_stop_evt is not None:
                                    stream_stop_evt.set()
                                if stream_thread is not None:
                                    stream_thread.join(timeout=5)
                            except Exception:
                                pass
                            stream_thread = None
                            await start_streaming_if_needed()
                            did_restart = True
                            try:
                                async with hub.lock:
                                    quiet = bool(hub.prefs.get(ws, {}).get("quiet_controls", False))
                                if not quiet:
                                    await ws.send_text(json.dumps({"control": "stream_restarted", "filters": snap}))
                            except Exception:
                                pass

                    if stream_thread is None and not did_restart:
                        await start_streaming_if_needed()
            except WebSocketDisconnect:
                break
            except Exception:
                break
    finally:
        try:
            if "stream_stop_evt" in locals() and stream_stop_evt is not None:
                stream_stop_evt.set()
        except Exception:
            pass
        await hub.disconnect(ws)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=settings.port, reload=False)
