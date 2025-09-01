
from __future__ import annotations
from typing import Optional, Any

def to_epoch_seconds(v: Any) -> Optional[int]:
    """Best-effort conversion of common timestamp shapes to epoch seconds."""
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            if v > 1_000_000_000_000:
                return int(v // 1000)
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            if s.isdigit():
                return int(s)
            from datetime import datetime
            s2 = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
            return int(dt.timestamp())
    except Exception:
        return None
    return None
