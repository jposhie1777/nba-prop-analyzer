from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests

BASE_URL = os.getenv("BDL_EPL_BASE_URL", "https://api.balldontlie.io/epl/v2")
API_KEY = os.getenv("BALLDONTLIE_API_KEY", "")
TIMEOUT = int(os.getenv("BDL_EPL_TIMEOUT_SECONDS", "30"))


class EplApiError(RuntimeError):
    pass


def _headers() -> Dict[str, str]:
    if not API_KEY:
        raise EplApiError("BALLDONTLIE_API_KEY is not configured")
    return {"Authorization": API_KEY}


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    response = requests.get(url, headers=_headers(), params=params or {}, timeout=TIMEOUT)
    if response.status_code >= 400:
        raise EplApiError(f"{response.status_code} {response.text}")
    return response.json()


def fetch_paginated(path: str, params: Optional[Dict[str, Any]] = None, per_page: int = 100) -> List[Dict[str, Any]]:
    query = dict(params or {})
    query.setdefault("per_page", per_page)
    rows: List[Dict[str, Any]] = []
    cursor: Optional[int] = None

    while True:
        page_params = dict(query)
        if cursor is not None:
            page_params["cursor"] = cursor

        payload = _get(path, page_params)
        data = payload.get("data") or []
        rows.extend(data)

        meta = payload.get("meta") or {}
        cursor = meta.get("next_cursor")
        if cursor is None or not data:
            break

    return rows


def fetch_single(path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    payload = _get(path, params)
    return payload.get("data") or []
