
from __future__ import annotations
from typing import List, TypeVar

T = TypeVar("T")

def chunk_list(items: list[T], size: int | None) -> list[list[T]]:
    """Return grouped chunks of `items`. If size <=0 or None, return one chunk with all items."""
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
