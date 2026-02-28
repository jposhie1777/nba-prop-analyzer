"""
PGA Tour GraphQL scraper — tournament schedule.

Queries schedule(tourCode: $tourCode, year: $year) for the full season
schedule including tournament IDs, dates, and status.

Usage (standalone CLI):
    python pga_schedule.py --tour-code R --year 2026
    python pga_schedule.py --tour-code R --raw    # dump raw JSON to inspect fields

Usage (as a module):
    from ingest.pga.pga_schedule import fetch_schedule, schedule_to_records
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import os

import requests

GRAPHQL_ENDPOINT = "https://orchestrator.pgatour.com/graphql"
DEFAULT_API_KEY = os.getenv("PGA_TOUR_GQL_API_KEY", "da2-gsrx5bibzbb4njvhl7t37wqyl4")
DEFAULT_TIMEOUT = 20


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

# The schedule query buckets events into completed / current / upcoming.
# Each bucket contains a list of tournament objects.
SCHEDULE_QUERY = """
query Schedule($tourCode: String!, $year: String) {
  schedule(tourCode: $tourCode, year: $year) {
    completed {
      tournaments {
        id
        tournamentName
        startDate
        endDate
        city
        state
        country
        status
        purse
        champion
        inSeasonTournament
      }
    }
    current {
      tournaments {
        id
        tournamentName
        startDate
        endDate
        city
        state
        country
        status
        purse
        inSeasonTournament
      }
    }
    upcoming {
      tournaments {
        id
        tournamentName
        startDate
        endDate
        city
        state
        country
        status
        purse
        inSeasonTournament
      }
    }
  }
}
"""

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ScheduleTournament:
    tournament_id: str
    name: str
    bucket: str               # "completed" | "current" | "upcoming"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    status: Optional[str] = None
    purse: Optional[str] = None
    champion: Optional[str] = None
    in_season_tournament: Optional[bool] = None


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


def _parse_tournament(raw: Dict[str, Any], bucket: str) -> ScheduleTournament:
    return ScheduleTournament(
        tournament_id=str(raw.get("id") or ""),
        name=raw.get("tournamentName") or raw.get("name") or "",
        bucket=bucket,
        start_date=raw.get("startDate"),
        end_date=raw.get("endDate"),
        city=raw.get("city"),
        state=raw.get("state"),
        country=raw.get("country"),
        status=raw.get("status"),
        purse=raw.get("purse"),
        champion=raw.get("champion"),
        in_season_tournament=raw.get("inSeasonTournament"),
    )


def _parse_schedule(data: Dict[str, Any]) -> List[ScheduleTournament]:
    schedule = data.get("schedule") or {}
    tournaments: List[ScheduleTournament] = []
    for bucket in ("completed", "current", "upcoming"):
        section = schedule.get(bucket) or {}
        for raw in section.get("tournaments") or []:
            if isinstance(raw, dict):
                tournaments.append(_parse_tournament(raw, bucket))
    return tournaments


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_schedule(
    tour_code: str = "R",
    year: Optional[str] = None,
    *,
    api_key: str = DEFAULT_API_KEY,
    retries: int = 3,
) -> List[ScheduleTournament]:
    """
    Fetch the full season schedule for a PGA Tour.

    Args:
        tour_code: Tour code — ``"R"`` for PGA Tour, ``"S"`` for Korn Ferry,
                   ``"H"`` for Champions Tour.
        year:      Season year as a string, e.g. ``"2026"``.
                   Defaults to the API's current season.
        api_key:   API key.
        retries:   Retry attempts on transient HTTP failures.

    Returns:
        List of :class:`ScheduleTournament` ordered completed → current → upcoming.
    """
    variables: Dict[str, Any] = {"tourCode": tour_code}
    if year:
        variables["year"] = year

    backoff = 2
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            data = _post_graphql(SCHEDULE_QUERY, variables, api_key=api_key)
            return _parse_schedule(data)
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


def fetch_schedule_raw(
    tour_code: str = "R",
    year: Optional[str] = None,
    *,
    api_key: str = DEFAULT_API_KEY,
) -> Dict[str, Any]:
    """Return the raw GraphQL response — useful for debugging field names."""
    variables: Dict[str, Any] = {"tourCode": tour_code}
    if year:
        variables["year"] = year
    return _post_graphql(SCHEDULE_QUERY, variables, api_key=api_key)


def get_active_tournament_ids(
    tour_code: str = "R",
    year: Optional[str] = None,
    *,
    api_key: str = DEFAULT_API_KEY,
) -> List[str]:
    """
    Convenience function — return tournament IDs for events currently in progress.

    Useful as a first step before calling :func:`fetch_leaderboard` or
    :func:`fetch_scorecard`.
    """
    tournaments = fetch_schedule(tour_code, year, api_key=api_key)
    return [t.tournament_id for t in tournaments if t.bucket == "current" and t.tournament_id]


def schedule_to_records(
    tournaments: List[ScheduleTournament],
    *,
    run_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Flatten schedule into BigQuery-ready row dicts."""
    from datetime import datetime

    ts = run_ts or datetime.utcnow().isoformat()
    return [
        {
            "run_ts": ts,
            "ingested_at": ts,
            "tournament_id": t.tournament_id,
            "name": t.name,
            "bucket": t.bucket,
            "start_date": t.start_date,
            "end_date": t.end_date,
            "city": t.city,
            "state": t.state,
            "country": t.country,
            "status": t.status,
            "purse": t.purse,
            "champion": t.champion,
            "in_season_tournament": t.in_season_tournament,
        }
        for t in tournaments
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PGA Tour season schedule from the official GraphQL API."
    )
    parser.add_argument(
        "--tour-code",
        default="R",
        metavar="TOUR_CODE",
        help="Tour code: R=PGA Tour, S=Korn Ferry, H=Champions (default: R)",
    )
    parser.add_argument(
        "--year",
        metavar="YEAR",
        help="Season year, e.g. 2026 (default: current season)",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Dump raw JSON response (useful for debugging field names)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Output flat JSON records",
    )
    args = parser.parse_args()

    label = args.year or "current"
    print(f"Fetching schedule: tourCode={args.tour_code} year={label}", file=sys.stderr)

    if args.raw:
        data = fetch_schedule_raw(args.tour_code, args.year)
        print(json.dumps(data, indent=2, default=str))
        return

    tournaments = fetch_schedule(args.tour_code, args.year)
    if not tournaments:
        print("No tournaments returned. Check tour code and year.", file=sys.stderr)
        sys.exit(1)

    if args.as_json:
        print(json.dumps(schedule_to_records(tournaments), indent=2, default=str))
        return

    print(f"\n{'ID':15}  {'Name':40}  {'Start':10}  {'Bucket':10}  {'Status'}")
    print("-" * 90)
    for t in tournaments:
        name = (t.name or "")[:40]
        start = (t.start_date or "")[:10]
        print(
            f"{t.tournament_id:15}  {name:40}  {start:10}  "
            f"{t.bucket:10}  {t.status or ''}"
        )


if __name__ == "__main__":
    _cli()
