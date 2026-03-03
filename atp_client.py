from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)


class ATPClient:
    def __init__(
        self,
        base_url: str = "https://www.atptour.com",
        timeout: int = 20,
        user_agent: str = "nba-prop-analyzer-atp-fetcher/1.2",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

        retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"],
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def fetch_calendar(self, cache_hints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._get_json("/en/-/tournaments/calendar/tour", cache_hints=cache_hints)

    def fetch_tournament_overview(self, tournament_id: str, cache_hints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._get_json(f"/en/-/tournaments/profile/{tournament_id}/overview", cache_hints=cache_hints)

    def fetch_tournament_top_seeds(self, tournament_id: str, event_year: int, cache_hints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._get_json(f"/en/-/tournaments/{tournament_id}/{event_year}/topseeds", cache_hints=cache_hints)

    def fetch_head_to_head(self, left_player_id: str, right_player_id: str, cache_hints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._get_json(
            f"/en/-/tour/Head2HeadSearch/GetHead2HeadData/{left_player_id}/{right_player_id}",
            cache_hints=cache_hints,
        )

    def fetch_match_schedule_html(self, tournament_slug: str, tournament_id: str, day: Optional[int] = None, cache_hints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        path = f"/en/scores/current/{tournament_slug}/{tournament_id}/daily-schedule"
        if day is not None:
            path = f"{path}?day={day}"
        return self._get_text(path, cache_hints=cache_hints)

    def fetch_match_results_html(self, tournament_slug: str, tournament_id: str, cache_hints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._get_text(f"/en/scores/current/{tournament_slug}/{tournament_id}/results", cache_hints=cache_hints)

    def _base_get(self, path: str, cache_hints: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers: Dict[str, str] = {}
        if cache_hints:
            if cache_hints.get("etag"):
                headers["If-None-Match"] = cache_hints["etag"]
            if cache_hints.get("last_modified"):
                headers["If-Modified-Since"] = cache_hints["last_modified"]

        response = self._session.get(url, timeout=self.timeout, headers=headers)
        return {"url": url, "response": response, "cache_hints": cache_hints or {}}

    def _get_json(self, path: str, cache_hints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        result = self._base_get(path, cache_hints)
        url = result["url"]
        response = result["response"]
        hints = result["cache_hints"]

        if response.status_code == 304:
            cached = hints.get("cached_payload")
            if cached is None:
                raise RuntimeError(f"Received 304 for {url} without cached payload available.")
            return {
                "url": url,
                "status_code": response.status_code,
                "fetched_json": cached,
                "etag": response.headers.get("ETag") or hints.get("etag"),
                "last_modified": response.headers.get("Last-Modified") or hints.get("last_modified"),
                "content_type": response.headers.get("Content-Type", "application/json"),
                "is_not_modified": True,
            }

        response.raise_for_status()
        payload = response.json()
        return {
            "url": url,
            "status_code": response.status_code,
            "fetched_json": payload,
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "content_type": response.headers.get("Content-Type", ""),
            "is_not_modified": False,
        }

    def _get_text(self, path: str, cache_hints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        result = self._base_get(path, cache_hints)
        url = result["url"]
        response = result["response"]
        hints = result["cache_hints"]

        if response.status_code == 304:
            cached = hints.get("cached_payload")
            if cached is None:
                raise RuntimeError(f"Received 304 for {url} without cached payload available.")
            return {
                "url": url,
                "status_code": response.status_code,
                "fetched_text": cached,
                "etag": response.headers.get("ETag") or hints.get("etag"),
                "last_modified": response.headers.get("Last-Modified") or hints.get("last_modified"),
                "content_type": response.headers.get("Content-Type", "text/html"),
                "is_not_modified": True,
            }

        response.raise_for_status()
        return {
            "url": url,
            "status_code": response.status_code,
            "fetched_text": response.text,
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "content_type": response.headers.get("Content-Type", ""),
            "is_not_modified": False,
        }


def load_cache_hints(cache_file: Path) -> Dict[str, Dict[str, Any]]:
    if not cache_file.exists():
        return {}
    try:
        return json.loads(cache_file.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache_hints(cache_file: Path, cache_hints: Dict[str, Dict[str, Any]]) -> None:
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(cache_hints, indent=2, sort_keys=True))
