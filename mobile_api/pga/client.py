from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests

from .bq import (
    fetch_course_holes as bq_fetch_course_holes,
    fetch_courses as bq_fetch_courses,
    fetch_courses_page as bq_fetch_courses_page,
    fetch_players as bq_fetch_players,
    fetch_players_page as bq_fetch_players_page,
    fetch_tournament_course_stats as bq_fetch_tournament_course_stats,
    fetch_tournament_results as bq_fetch_tournament_results,
    fetch_tournaments as bq_fetch_tournaments,
    fetch_tournaments_page as bq_fetch_tournaments_page,
)
from .cache import get_cached, set_cached


BASE_URL = "https://api.balldontlie.io/pga/v1"
DEFAULT_TIMEOUT = 20


class PgaApiError(RuntimeError):
    pass


def _use_bq() -> bool:
    source = os.getenv("PGA_DATA_SOURCE")
    if source:
        return source.strip().lower() == "bq"
    return os.getenv("PGA_USE_BQ", "true").strip().lower() == "true"


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


def _resolve_source(source: Optional[str]) -> str:
    if source:
        return source.strip().lower()
    return "bq" if _use_bq() else "api"


def _fetch_paginated_bq(path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    if path == "/players":
        return bq_fetch_players(params)
    if path == "/tournaments":
        return bq_fetch_tournaments(params)
    if path == "/courses":
        return bq_fetch_courses(params)
    if path == "/tournament_results":
        return bq_fetch_tournament_results(params)
    if path == "/tournament_course_stats":
        return bq_fetch_tournament_course_stats(params)
    if path == "/course_holes":
        return bq_fetch_course_holes(params)
    raise PgaApiError(f"Unsupported PGA BigQuery path: {path}")


def _fetch_one_page_bq(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if path == "/players":
        return bq_fetch_players_page(params)
    if path == "/tournaments":
        return bq_fetch_tournaments_page(params)
    if path == "/courses":
        return bq_fetch_courses_page(params)
    raise PgaApiError(f"Unsupported PGA BigQuery path: {path}")


def fetch_paginated(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    per_page: int = 100,
    max_pages: int = 50,
    cache_ttl: int = 900,
    source: Optional[str] = None,
) -> List[Dict[str, Any]]:
    params = params or {}
    resolved_source = _resolve_source(source)
    cache_key = f"{path}:{sorted(params.items())}:{per_page}:{max_pages}:{resolved_source}"
    cached = get_cached(cache_key, cache_ttl)
    if cached is not None:
        return cached

    if resolved_source == "bq":
        try:
            results = _fetch_paginated_bq(path, params)
        except Exception as exc:
            raise PgaApiError(f"PGA BigQuery error for {path}: {exc}") from exc
        set_cached(cache_key, results)
        return results

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
    source: Optional[str] = None,
) -> Dict[str, Any]:
    params = params or {}
    resolved_source = _resolve_source(source)
    cache_key = f"one:{path}:{sorted(params.items())}:{resolved_source}"
    cached = get_cached(cache_key, cache_ttl)
    if cached is not None:
        return cached

    if resolved_source == "bq":
        try:
            payload = _fetch_one_page_bq(path, params)
        except Exception as exc:
            raise PgaApiError(f"PGA BigQuery error for {path}: {exc}") from exc
        set_cached(cache_key, payload)
        return payload

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
