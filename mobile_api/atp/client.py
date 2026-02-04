from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests

from .cache import get_cached, set_cached

ATP_BASE = "https://api.balldontlie.io/atp/v1"


class AtpApiError(RuntimeError):
    pass


RATE_LIMITS = {
    "ALL_STAR": {
        "page_delay": 1.2,
        "batch_delay": 1.5,
        "retry_delay": 10.0,
    },
    "GOAT": {
        "page_delay": 0.25,
        "batch_delay": 0.3,
        "retry_delay": 4.0,
    },
}


def _rate_profile() -> str:
    raw = os.getenv("BDL_ATP_TIER") or os.getenv("BALLDONTLIE_TIER") or "ALL_STAR"
    return raw.upper().replace("-", "_")


def get_rate_limits() -> Dict[str, float]:
    return RATE_LIMITS.get(_rate_profile(), RATE_LIMITS["ALL_STAR"])


def _get_api_key() -> str:
    key = (
        os.getenv("BDL_ATP_API_KEY")
        or os.getenv("BALLDONTLIE_API_KEY")
        or os.getenv("BDL_API_KEY")
    )
    if not key:
        raise AtpApiError("Missing BDL_ATP_API_KEY (or BALLDONTLIE_API_KEY/BDL_API_KEY).")
    return key


def _headers() -> Dict[str, str]:
    return {
        "Authorization": _get_api_key(),
        "Accept": "application/json",
    }


def fetch_one_page(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    cache_ttl: int = 300,
    timeout: int = 20,
) -> Dict[str, Any]:
    params = params or {}
    cache_key = f"one:{path}:{sorted(params.items())}"
    cached = get_cached(cache_key, cache_ttl)
    if cached is not None:
        return cached

    resp = requests.get(
        f"{ATP_BASE}{path}",
        headers=_headers(),
        params=params,
        timeout=timeout,
    )
    if not resp.ok:
        raise AtpApiError(f"ATP API error {resp.status_code}: {resp.text}")

    payload = resp.json()
    set_cached(cache_key, payload)
    return payload


def fetch_paginated(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    per_page: int = 100,
    max_pages: Optional[int] = None,
    cache_ttl: int = 900,
    timeout: int = 20,
) -> List[Dict[str, Any]]:
    params = params or {}
    per_page = min(max(per_page, 1), 100)
    rate = get_rate_limits()
    cache_key = f"list:{path}:{sorted(params.items())}:{per_page}:{max_pages}"
    cached = get_cached(cache_key, cache_ttl)
    if cached is not None:
        return cached

    results: List[Dict[str, Any]] = []
    cursor: Optional[int] = params.get("cursor")
    page_count = 0

    while True:
        page_params = {**params, "per_page": per_page}
        if cursor is not None:
            page_params["cursor"] = cursor

        resp = requests.get(
            f"{ATP_BASE}{path}",
            headers=_headers(),
            params=page_params,
            timeout=timeout,
        )

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after else rate["retry_delay"]
            time.sleep(sleep_for)
            continue

        if not resp.ok:
            raise AtpApiError(f"ATP API error {resp.status_code}: {resp.text}")

        payload = resp.json()
        results.extend(payload.get("data", []) or [])

        cursor = (payload.get("meta") or {}).get("next_cursor")
        page_count += 1

        if cursor is None:
            break
        if max_pages is not None and page_count >= max_pages:
            break

        time.sleep(rate["page_delay"])

    set_cached(cache_key, results)
    return results
