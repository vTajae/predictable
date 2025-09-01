
from __future__ import annotations

import os
import logging
from dotenv import load_dotenv

# Load API key from .env (if present)
load_dotenv()
API_KEY = os.getenv("OPTICODDS_API_KEY")

# Optic Odds API endpoints
API_BASE = "https://api.opticodds.com/api/v3"
STREAM_BASE = f"{API_BASE}/stream/odds"
SPORTSBOOKS_URL = f"{API_BASE}/sportsbooks"
SPORTS_URL = f"{API_BASE}/sports"
LEAGUES_URL = f"{API_BASE}/leagues"

# Tracing / logging setup
TRACE_ENABLED = os.getenv("TRACE", "0").strip().lower() in ("1", "true", "yes", "on")
if TRACE_ENABLED:
    logging.basicConfig(
        filename=os.getenv("TRACE_FILE", "trace.log"),
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
    )
logger = logging.getLogger("opticodds")
