
from __future__ import annotations
from typing import Optional, Dict, Any

def parse_decimal_odds(item: Dict[str, Any]) -> Optional[float]:
    """Parse offered odds into decimal odds.

    Priority:
      1) explicit decimal fields (top-level or nested under price)
      2) american odds (top-level or nested under price) converted to decimal
      3) generic numeric odds/price (assume american if |x|>=100, else decimal)
    """
    if not isinstance(item, dict):
        return None
    price_obj = item.get("price") if isinstance(item.get("price"), dict) else None

    # 1) decimal odds
    for src in (item, price_obj) if price_obj else (item,):
        if not isinstance(src, dict):
            continue
        for k in ("decimal","odds_decimal","price_decimal","decimal_price"):
            v = src.get(k)
            try:
                f = float(v)
                if f >= 1.01:
                    return f
            except Exception:
                pass

    # american -> decimal
    def american_to_decimal(a: float) -> Optional[float]:
        if a >= 100: return 1.0 + (a / 100.0)
        if a <= -100: return 1.0 + (100.0 / abs(a))
        return None

    for src in (item, price_obj) if price_obj else (item,):
        if not isinstance(src, dict):
            continue
        for k in ("american","odds_american"):
            v = src.get(k)
            try:
                f = float(v)
                dec = american_to_decimal(f)
                if dec:
                    return dec
            except Exception:
                pass

    # 3) generic numeric fields: detect american by magnitude
    for k in ("odds","price"):
        v = item.get(k)
        try:
            f = float(v)
            dec = american_to_decimal(f)
            if dec:
                return dec
            if f >= 1.01:
                return f
        except Exception:
            pass
    return None
