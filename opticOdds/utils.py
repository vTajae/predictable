
from __future__ import annotations

from typing import Iterable, List, TypeVar

T = TypeVar("T")

def dedupe_preserve_order(items: Iterable[T]) -> list[T]:
    seen = set()
    out: list[T] = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

def chunk_list(items: list[T], size: int | None) -> list[list[T]]:
    """Return a list of equally sized chunks (last may be smaller).
    If size is None or <= 0, return a single chunk with all items.
    """
    if items is None:
        return []
    if size is None:
        size = len(items) or 1
    try:
        n = int(size)
    except Exception:
        n = 1
    if n <= 0:
        return [list(items)] if items else []
    return [items[i:i+n] for i in range(0, len(items), n)]
