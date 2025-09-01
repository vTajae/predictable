
from __future__ import annotations

from typing import Dict, Any
import requests
from requests.exceptions import RequestException
from .config import API_KEY, TRACE_ENABLED, logger

def get_json(url: str, params: Dict[str, Any] | None = None, *, timeout: int = 30) -> dict | None:
    """GET a JSON resource with basic error handling. Returns parsed JSON or None."""
    params = dict(params or {})
    if API_KEY and "key" not in params:
        params["key"] = API_KEY
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if TRACE_ENABLED:
            logger.debug("GET %s status=%s", url, getattr(r, "status_code", None))
        r.raise_for_status()
        return r.json() or {}
    except RequestException as e:
        if TRACE_ENABLED:
            logger.exception("GET failed for %s", url)
        return None
