from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests

from .cache import get_cached, set_cached


BASE_URL = "https://api.balldontlie.io/pga/v1"
DEFAULT_TIMEOUT = 20


class PgaApiError(RuntimeError):
    pass


def _get_api_key() -> str:
    key = (
        os.getenv("BDL_PGA_API_KEY")
        or os.getenv("BDL_API_KEY")
        or os.getenv("BALDONTLIE_KEY")
        or os.getenv("BALLDONTLIE_API_KEY")
    )
    if not key:
        raise PgaApiError(
            "Missing PGA API key. Set BDL_PGA_API_KEY (preferred) or "
            "BDL_API_KEY/BALDONTLIE_KEY/BALLDONTLIE_API_KEY."
        )
    return key


def _headers() -> Dict[str, str]:
    return {"Authorization": _get_api_key()}


def fetch_paginated(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    per_page: int = 100,
    max_pages: int = 50,
    cache_ttl: int = 900,
) -> List[Dict[str, Any]]:
    params = params or {}
    cache_key = f"{path}:{sorted(params.items())}:{per_page}:{max_pages}"
    cached = get_cached(cache_key, cache_ttl)
    if cached is not None:
        return cached

    results: List[Dict[str, Any]] = []
    cursor: Optional[int] = params.get("cursor")

    for _ in range(max_pages):
        page_params = {**params, "per_page": per_page}
        if cursor is not None:
            page_params["cursor"] = cursor

        resp = requests.get(
            f"{BASE_URL}{path}",
            headers=_headers(),
            params=page_params,
            timeout=DEFAULT_TIMEOUT,
        )
        if not resp.ok:
            raise PgaApiError(f"PGA API error {resp.status_code}: {resp.text}")

        payload = resp.json()
        results.extend(payload.get("data", []))

        meta = payload.get("meta", {}) or {}
        cursor = meta.get("next_cursor")
        if cursor is None:
            break

    set_cached(cache_key, results)
    return results


def fetch_one_page(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    cache_ttl: int = 300,
) -> Dict[str, Any]:
    params = params or {}
    cache_key = f"one:{path}:{sorted(params.items())}"
    cached = get_cached(cache_key, cache_ttl)
    if cached is not None:
        return cached

    resp = requests.get(
        f"{BASE_URL}{path}",
        headers=_headers(),
        params=params,
        timeout=DEFAULT_TIMEOUT,
    )
    if not resp.ok:
        raise PgaApiError(f"PGA API error {resp.status_code}: {resp.text}")

    payload = resp.json()
    set_cached(cache_key, payload)
    return payload
