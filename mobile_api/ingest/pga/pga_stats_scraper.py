"""
PGA Tour GraphQL scraper — player stats overview.

Queries statOverview(tourCode, year) for all per-stat player leaderboards.
Each stat returns a ranked list of players with their stat value, rank, country, etc.

Field names confirmed via live schema introspection of LeaderStat and OverviewStat.
Run: python3 -c "from introspect_pga_schema import introspect_type; introspect_type('LeaderStat')"
to re-discover if this query ever breaks.

Usage (standalone CLI):
    python pga_stats_scraper.py --tour R --year 2025
    python pga_stats_scraper.py --tour R --year 2025 --raw   # dump raw JSON

Usage (as a module):
    from ingest.pga.pga_stats_scraper import fetch_stat_overview, stat_players_to_records
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

# Field names confirmed via live schema introspection of LeaderStat and OverviewStat.
# Run: python3 -c "from introspect_pga_schema import introspect_type; introspect_type('LeaderStat')"
# to re-discover if this query ever breaks.
STAT_OVERVIEW_QUERY = """
query StatOverview($tourCode: TourCode!, $year: Int!) {
  statOverview(tourCode: $tourCode, year: $year) {
    tourCode
    year
    categories {
      category
      displayName
      subCategories {
        displayName
      }
    }
    stats {
      statId
      statName
      tourAvg
      players {
        statId
        playerId
        statTitle
        statValue
        playerName
        rank
        country
        countryFlag
      }
    }
  }
}
""".strip()


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class StatPlayerRow:
    """One player's position on a single stat leaderboard."""

    stat_id: str
    stat_name: str
    player_id: str
    player_name: str
    stat_title: str
    stat_value: str
    rank: int
    country: Optional[str]
    country_flag: Optional[str]
    tour_avg: Optional[str]       # tour average for this stat
    tour_code: str
    year: int


@dataclass
class StatCategory:
    category: str
    display_name: str
    sub_categories: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class StatOverviewResult:
    tour_code: str
    year: int
    categories: List[StatCategory] = field(default_factory=list)
    players: List[StatPlayerRow] = field(default_factory=list)


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


def _parse_stat_overview(data: Dict[str, Any]) -> StatOverviewResult:
    overview = data.get("statOverview") or {}
    tour_code = str(overview.get("tourCode") or "")
    year = _safe_int(overview.get("year")) or 0

    categories: List[StatCategory] = []
    for cat in overview.get("categories") or []:
        sub_cats = [
            {"display_name": s.get("displayName")}
            for s in (cat.get("subCategories") or [])
        ]
        categories.append(
            StatCategory(
                category=str(cat.get("category") or ""),
                display_name=str(cat.get("displayName") or ""),
                sub_categories=sub_cats,
            )
        )

    players: List[StatPlayerRow] = []
    for stat in overview.get("stats") or []:
        stat_id = str(stat.get("statId") or "")
        stat_name = str(stat.get("statName") or "")
        tour_avg = stat.get("tourAvg")
        for p in stat.get("players") or []:
            players.append(
                StatPlayerRow(
                    stat_id=stat_id,
                    stat_name=stat_name,
                    player_id=str(p.get("playerId") or ""),
                    player_name=str(p.get("playerName") or ""),
                    stat_title=str(p.get("statTitle") or ""),
                    stat_value=str(p.get("statValue") or ""),
                    rank=_safe_int(p.get("rank")) or 0,
                    country=p.get("country"),
                    country_flag=p.get("countryFlag"),
                    tour_avg=tour_avg,
                    tour_code=tour_code,
                    year=year,
                )
            )

    return StatOverviewResult(
        tour_code=tour_code,
        year=year,
        categories=categories,
        players=players,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_stat_overview(
    tour_code: str = "R",
    year: int = 2025,
    *,
    api_key: str = DEFAULT_API_KEY,
    retries: int = 3,
) -> StatOverviewResult:
    """
    Fetch all stat leaderboards for a tour and season year.

    Args:
        tour_code: PGA Tour code, e.g. ``"R"`` (PGA Tour) or ``"S"`` (Korn Ferry).
        year:      Season year, e.g. ``2025``.
        api_key:   API key (uses env var or built-in default).
        retries:   Number of retry attempts on transient HTTP failures.

    Returns:
        :class:`StatOverviewResult` with all stat-player rows.
    """
    backoff = 2
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            data = _post_graphql(
                STAT_OVERVIEW_QUERY,
                {"tourCode": tour_code, "year": year},
                api_key=api_key,
            )
            return _parse_stat_overview(data)
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


def fetch_stat_overview_raw(
    tour_code: str = "R",
    year: int = 2025,
    *,
    api_key: str = DEFAULT_API_KEY,
) -> Dict[str, Any]:
    """Return the raw GraphQL response dict — useful for debugging field names."""
    return _post_graphql(
        STAT_OVERVIEW_QUERY, {"tourCode": tour_code, "year": year}, api_key=api_key
    )


def stat_players_to_records(
    result: StatOverviewResult,
    *,
    run_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten StatOverviewResult into a list of BigQuery-ready row dicts.

    Produces one row per (player, stat).
    """
    from datetime import datetime

    ts = run_ts or datetime.utcnow().isoformat()
    return [
        {
            "run_ts": ts,
            "ingested_at": ts,
            "tour_code": p.tour_code,
            "year": p.year,
            "stat_id": p.stat_id,
            "stat_name": p.stat_name,
            "tour_avg": p.tour_avg,
            "player_id": p.player_id,
            "player_name": p.player_name,
            "stat_title": p.stat_title,
            "stat_value": p.stat_value,
            "rank": p.rank,
            "country": p.country,
            "country_flag": p.country_flag,
        }
        for p in result.players
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PGA Tour stat overview from the official GraphQL API."
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

    print(f"Fetching stat overview: tour={args.tour} year={args.year}", file=sys.stderr)

    if args.raw:
        data = fetch_stat_overview_raw(args.tour, args.year)
        print(json.dumps(data, indent=2, default=str))
        return

    result = fetch_stat_overview(args.tour, args.year)
    if not result.players:
        print("No stat records returned. Check tour code and year.", file=sys.stderr)
        sys.exit(1)

    if args.as_json:
        print(json.dumps(stat_players_to_records(result), indent=2, default=str))
        return

    # Group by stat for display
    by_stat: Dict[str, List[StatPlayerRow]] = {}
    for p in result.players:
        by_stat.setdefault(p.stat_id, []).append(p)

    for stat_id, rows in list(by_stat.items())[:5]:
        stat_name = rows[0].stat_name if rows else stat_id
        print(f"\n── {stat_name} ({stat_id}) ──")
        print(f"  {'Rank':>4}  {'Player':30}  {'Value':>10}  Country")
        print("  " + "-" * 60)
        for r in rows[:10]:
            print(f"  {r.rank:>4}  {r.player_name:30}  {r.stat_value:>10}  {r.country or ''}")
        if len(rows) > 10:
            print(f"  ... and {len(rows) - 10} more players")

    remaining = len(by_stat) - 5
    if remaining > 0:
        print(f"\n  ... and {remaining} more stats")


if __name__ == "__main__":
    _cli()
