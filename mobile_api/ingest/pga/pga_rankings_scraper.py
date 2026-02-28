"""
PGA Tour GraphQL scraper — priority rankings (FedEx Cup standings, etc.).

Queries priorityRankings(tourCode, year) for ranked player lists across
all ranking categories (FedEx Cup, Korn Ferry Tour standings, etc.).

Field names confirmed via live schema introspection of PriorityPlayer and PriorityCategory.
Run: python3 -c "from introspect_pga_schema import introspect_type; introspect_type('PriorityCategory')"
to re-discover if this query ever breaks.

Usage (standalone CLI):
    python pga_rankings_scraper.py --tour R --year 2025
    python pga_rankings_scraper.py --tour R --year 2025 --raw   # dump raw JSON

Usage (as a module):
    from ingest.pga.pga_rankings_scraper import fetch_priority_rankings, rankings_to_records
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

GRAPHQL_ENDPOINT = "https://orchestrator.pgatour.com/graphql"
DEFAULT_API_KEY = os.getenv("PGA_TOUR_GQL_API_KEY", "da2-gsrx5bibzbb4njvhl7t37wqyl4")
DEFAULT_TIMEOUT = 30


def _graphql_headers(api_key: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "x-pgat-platform": "web",
        "Referer": "https://www.pgatour.com/",
        "Origin": "https://www.pgatour.com",
    }


# ---------------------------------------------------------------------------
# GraphQL query
# ---------------------------------------------------------------------------

# Field names confirmed via live schema introspection of PriorityCategory and PriorityPlayer.
# Run: python3 -c "from introspect_pga_schema import introspect_type; introspect_type('PriorityPlayer')"
# to re-discover if this query ever breaks.
PRIORITY_RANKINGS_QUERY = """
query PriorityRankings($tourCode: TourCode!, $year: Int!) {
  priorityRankings(tourCode: $tourCode, year: $year) {
    tourCode
    year
    displayYear
    throughText
    yearPills {
      year
      displaySeason
    }
    categories {
      displayName
      detail
      players {
        playerId
        displayName
      }
    }
  }
}
""".strip()


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class RankedPlayer:
    """One player's position within a single priority ranking category."""

    player_id: str
    display_name: str
    category_name: str
    rank: int                    # 1-based position within the category
    tour_code: str
    year: int
    through_text: Optional[str]  # e.g. "Through The Genesis Invitational"


@dataclass
class PriorityRankingsResult:
    tour_code: str
    year: int
    display_year: Optional[str]
    through_text: Optional[str]
    players: List[RankedPlayer] = field(default_factory=list)


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------


def _post_graphql(
    query: str,
    variables: Dict[str, Any],
    *,
    api_key: str = DEFAULT_API_KEY,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    resp = requests.post(
        GRAPHQL_ENDPOINT,
        headers=_graphql_headers(api_key),
        json={"query": query, "variables": variables},
        timeout=timeout,
    )
    resp.raise_for_status()
    result = resp.json()
    errors = result.get("errors")
    if errors:
        raise RuntimeError(f"GraphQL errors: {json.dumps(errors, indent=2)}")
    return result.get("data", {})


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _parse_priority_rankings(data: Dict[str, Any]) -> PriorityRankingsResult:
    rankings = data.get("priorityRankings") or {}
    tour_code = str(rankings.get("tourCode") or "")
    year = _safe_int(rankings.get("year")) or 0
    display_year = rankings.get("displayYear")
    through_text = rankings.get("throughText")

    players: List[RankedPlayer] = []
    for cat in rankings.get("categories") or []:
        cat_name = str(cat.get("displayName") or "")
        for idx, p in enumerate(cat.get("players") or [], start=1):
            players.append(
                RankedPlayer(
                    player_id=str(p.get("playerId") or ""),
                    display_name=str(p.get("displayName") or ""),
                    category_name=cat_name,
                    rank=idx,
                    tour_code=tour_code,
                    year=year,
                    through_text=through_text,
                )
            )

    return PriorityRankingsResult(
        tour_code=tour_code,
        year=year,
        display_year=display_year,
        through_text=through_text,
        players=players,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_priority_rankings(
    tour_code: str = "R",
    year: int = 2025,
    *,
    api_key: str = DEFAULT_API_KEY,
    retries: int = 3,
) -> PriorityRankingsResult:
    """
    Fetch priority rankings (FedEx Cup, Korn Ferry Tour, etc.) for a season.

    Args:
        tour_code: PGA Tour code, e.g. ``"R"`` (PGA Tour) or ``"S"`` (Korn Ferry).
        year:      Season year, e.g. ``2025``.
        api_key:   API key (uses env var or built-in default).
        retries:   Number of retry attempts on transient HTTP failures.

    Returns:
        :class:`PriorityRankingsResult` with ranked players across all categories.
    """
    backoff = 2
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            data = _post_graphql(
                PRIORITY_RANKINGS_QUERY,
                {"tourCode": tour_code, "year": year},
                api_key=api_key,
            )
            return _parse_priority_rankings(data)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status in (429, 500, 502, 503, 504):
                last_exc = exc
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
    raise RuntimeError(f"Exceeded {retries} retries. Last error: {last_exc}")


def fetch_priority_rankings_raw(
    tour_code: str = "R",
    year: int = 2025,
    *,
    api_key: str = DEFAULT_API_KEY,
) -> Dict[str, Any]:
    """Return the raw GraphQL response dict — useful for debugging field names."""
    return _post_graphql(
        PRIORITY_RANKINGS_QUERY, {"tourCode": tour_code, "year": year}, api_key=api_key
    )


def rankings_to_records(
    result: PriorityRankingsResult,
    *,
    run_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten PriorityRankingsResult into a list of BigQuery-ready row dicts.

    Produces one row per (player, category).
    """
    from datetime import datetime

    ts = run_ts or datetime.utcnow().isoformat()
    return [
        {
            "run_ts": ts,
            "ingested_at": ts,
            "tour_code": p.tour_code,
            "year": p.year,
            "display_year": result.display_year,
            "through_text": p.through_text,
            "category_name": p.category_name,
            "rank": p.rank,
            "player_id": p.player_id,
            "display_name": p.display_name,
        }
        for p in result.players
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PGA Tour priority rankings from the official GraphQL API."
    )
    parser.add_argument("--tour", default="R", metavar="TOUR_CODE",
                        help="Tour code, e.g. R (default) or S")
    parser.add_argument("--year", type=int, default=2025, metavar="YEAR",
                        help="Season year, e.g. 2025")
    parser.add_argument("--raw", action="store_true",
                        help="Dump raw JSON response (useful for debugging field names)")
    parser.add_argument("--json", action="store_true", dest="as_json",
                        help="Output flat JSON records")
    args = parser.parse_args()

    print(f"Fetching priority rankings: tour={args.tour} year={args.year}", file=sys.stderr)

    if args.raw:
        data = fetch_priority_rankings_raw(args.tour, args.year)
        print(json.dumps(data, indent=2, default=str))
        return

    result = fetch_priority_rankings(args.tour, args.year)
    if not result.players:
        print("No rankings returned. Check tour code and year.", file=sys.stderr)
        sys.exit(1)

    if args.as_json:
        print(json.dumps(rankings_to_records(result), indent=2, default=str))
        return

    # Group by category for display
    by_cat: Dict[str, List[RankedPlayer]] = {}
    for p in result.players:
        by_cat.setdefault(p.category_name, []).append(p)

    print(f"\nThrough: {result.through_text or 'N/A'} ({result.display_year})")
    for cat_name, rows in by_cat.items():
        print(f"\n── {cat_name} ──")
        print(f"  {'Rank':>4}  {'Player':30}")
        print("  " + "-" * 40)
        for r in rows[:20]:
            print(f"  {r.rank:>4}  {r.display_name}")
        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more players")


if __name__ == "__main__":
    _cli()
