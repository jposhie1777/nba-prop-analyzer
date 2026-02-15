from __future__ import annotations

import os
import time
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional

import requests

BASE_URL = os.getenv("BDL_EPL_BASE_URL", "https://api.balldontlie.io/epl/v2")
API_KEY = os.getenv("BDL_EPL_API_KEY") or os.getenv("BALLDONTLIE_API_KEY", "")
TIMEOUT = int(os.getenv("BDL_EPL_TIMEOUT_SECONDS", "30"))
RETRY_ATTEMPTS = int(os.getenv("BDL_EPL_RETRY_ATTEMPTS", "6"))
RETRY_BASE_SECONDS = float(os.getenv("BDL_EPL_RETRY_BASE_SECONDS", "1.0"))
RETRY_CAP_SECONDS = float(os.getenv("BDL_EPL_RETRY_CAP_SECONDS", "30.0"))


class EplApiError(RuntimeError):
    pass


def _headers() -> Dict[str, str]:
    if not API_KEY:
        raise EplApiError("BALLDONTLIE_API_KEY is not configured")
    return {"Authorization": API_KEY}


def _retry_delay_seconds(response: requests.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), 0.0)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                now = time.time()
                return max(retry_at.timestamp() - now, 0.0)
            except Exception:
                pass

    delay = RETRY_BASE_SECONDS * (2 ** max(attempt - 1, 0))
    return min(delay, RETRY_CAP_SECONDS)


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        response = requests.get(url, headers=_headers(), params=params or {}, timeout=TIMEOUT)
        if response.status_code < 400:
            return response.json()

        if response.status_code == 429 and attempt < RETRY_ATTEMPTS:
            time.sleep(_retry_delay_seconds(response, attempt))
            continue

        raise EplApiError(f"{response.status_code} {response.text}")

    raise EplApiError("429 Too many requests, retry budget exhausted")


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
