from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    from .premierleague_scraper import fetch_match_details, fetch_match_events, fetch_match_stats
except ImportError:
    from mobile_api.ingest.epl.premierleague_scraper import fetch_match_details, fetch_match_events, fetch_match_stats


class EplApiError(RuntimeError):
    pass


def fetch_paginated(path: str, params: Optional[Dict[str, Any]] = None, per_page: int = 100) -> List[Dict[str, Any]]:
    _ = (params, per_page)
    raise EplApiError(
        "fetch_paginated is deprecated for EPL ingestion. "
        "Use premierleague_scraper schedule/details/events/stats helpers instead."
    )


def fetch_single(path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    _ = params
    if path == "match_details":
        return [fetch_match_details(str(params.get("match_id")))] if params and params.get("match_id") else []
    if path == "match_events":
        return [fetch_match_events(str(params.get("match_id")))] if params and params.get("match_id") else []
    if path == "match_stats":
        return fetch_match_stats(str(params.get("match_id"))) if params and params.get("match_id") else []
    raise EplApiError(f"Unsupported path: {path}")
