from __future__ import annotations

import time
from typing import Any, Dict, Tuple

_CACHE: Dict[str, Tuple[float, Any]] = {}


def get_cached(key: str, ttl_seconds: int) -> Any | None:
    entry = _CACHE.get(key)
    if not entry:
        return None
    ts, value = entry
    if (time.time() - ts) > ttl_seconds:
        _CACHE.pop(key, None)
        return None
    return value


def set_cached(key: str, value: Any) -> None:
    _CACHE[key] = (time.time(), value)
