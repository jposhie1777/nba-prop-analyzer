"""
PGA Tour GraphQL scraper for round pairings / tee times.

Uses the official PGA Tour GraphQL orchestrator (the same API that powers
pgatour.com). No account or paid subscription is required.

Endpoint : https://orchestrator.pgatour.com/graphql
Query    : teeTimes(id: $tournamentId)

Schema path:
  TeeTimes
    .rounds[]          TeeTimeRound   (roundInt, roundStatus, …)
      .groups[]        Group          (groupNumber, teeTime, startTee, …)
        .players[]     Player         (id, displayName, firstName, …)

Usage (standalone CLI):
    python pga_tour_graphql.py --tournament R2026010 --round 1

Usage (as a module):
    from ingest.pga.pga_tour_graphql import fetch_pairings, pairings_to_records
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GRAPHQL_ENDPOINT = "https://orchestrator.pgatour.com/graphql"

# Public API key embedded in the PGA Tour web application.
# Not a secret – present in every unauthenticated browser request to pgatour.com.
DEFAULT_API_KEY = os.getenv("PGA_TOUR_GQL_API_KEY", "da2-gsrx5bibzbb4njvhl7t37wqyl4")

DEFAULT_TIMEOUT = 20

# ---------------------------------------------------------------------------
# GraphQL query  (verified against live schema via introspection)
# ---------------------------------------------------------------------------

TEE_TIMES_QUERY = """
query TeeTimes($tournamentId: ID!) {
  teeTimes(id: $tournamentId) {
    id
    timezone
    defaultRound
    rounds {
      roundInt
      roundStatusDisplay
      roundDisplay
      roundStatus
      groups {
        groupNumber
        teeTime
        startTee
        backNine
        courseId
        courseName
        players {
          id
          displayName
          firstName
          lastName
          country
          amateur
          tourBound
          headshot
          playerBio {
            rankWorld
            rankCountry
          }
        }
      }
    }
  }
}
"""

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class PlayerInfo:
    player_id: str
    display_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    country: Optional[str] = None
    world_rank: Optional[int] = None
    amateur: bool = False


@dataclass
class Pairing:
    tournament_id: str
    round_number: int
    round_status: Optional[str]
    group_number: int
    tee_time: Optional[str]
    start_hole: int
    back_nine: bool
    course_id: Optional[str]
    course_name: Optional[str]
    players: List[PlayerInfo] = field(default_factory=list)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _graphql_headers(api_key: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "x-pgat-platform": "web",
        "Referer": "https://www.pgatour.com/",
        "Origin": "https://www.pgatour.com",
    }


def _post_graphql(
    query: str,
    variables: Dict[str, Any],
    *,
    api_key: str = DEFAULT_API_KEY,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    payload = {"query": query, "variables": variables}
    resp = requests.post(
        GRAPHQL_ENDPOINT,
        headers=_graphql_headers(api_key),
        json=payload,
        timeout=timeout,
    )
    resp.raise_for_status()
    result = resp.json()
    errors = result.get("errors")
    if errors:
        raise RuntimeError(f"GraphQL errors: {json.dumps(errors, indent=2)}")
    return result.get("data", {})


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_player(raw: Dict[str, Any]) -> PlayerInfo:
    bio = raw.get("playerBio") or {}
    name = raw.get("displayName") or ""
    if not name:
        first = raw.get("firstName") or ""
        last = raw.get("lastName") or ""
        name = f"{first} {last}".strip()
    return PlayerInfo(
        player_id=str(raw.get("id", "")),
        display_name=name,
        first_name=raw.get("firstName"),
        last_name=raw.get("lastName"),
        country=raw.get("country"),
        world_rank=_safe_int(bio.get("rankWorld")),
        amateur=bool(raw.get("amateur", False)),
    )


def _parse_tee_times(
    data: Dict[str, Any],
    tournament_id: str,
    round_number: int,
) -> List[Pairing]:
    tee_times = data.get("teeTimes") or {}
    rounds = tee_times.get("rounds") or []

    result: List[Pairing] = []
    for rnd in rounds:
        rnd_int = _safe_int(rnd.get("roundInt")) or 0
        if round_number != 0 and rnd_int != round_number:
            continue
        rnd_status = rnd.get("roundStatusDisplay")
        for grp in (rnd.get("groups") or []):
            players = [_parse_player(p) for p in (grp.get("players") or [])]
            result.append(
                Pairing(
                    tournament_id=tournament_id,
                    round_number=rnd_int,
                    round_status=rnd_status,
                    group_number=_safe_int(grp.get("groupNumber")) or 0,
                    tee_time=grp.get("teeTime"),
                    start_hole=_safe_int(grp.get("startTee")) or 1,
                    back_nine=bool(grp.get("backNine", False)),
                    course_id=grp.get("courseId"),
                    course_name=grp.get("courseName"),
                    players=players,
                )
            )
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_pairings(
    tournament_id: str,
    round_number: int | str,
    *,
    api_key: str = DEFAULT_API_KEY,
    retries: int = 3,
) -> List[Pairing]:
    """
    Fetch round pairings / tee times from the PGA Tour GraphQL API.

    Args:
        tournament_id: PGA Tour tournament ID, e.g. ``"R2026010"``.
        round_number:  Round to return (1–4).  Pass ``0`` for all rounds.
        api_key:       API key (uses env var ``PGA_TOUR_GQL_API_KEY`` or built-in default).
        retries:       Number of retry attempts on transient HTTP failures.

    Returns:
        List of :class:`Pairing` objects sorted by (round_number, group_number).
    """
    rnd = int(round_number)
    variables: Dict[str, Any] = {"tournamentId": tournament_id}

    backoff = 2
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            data = _post_graphql(TEE_TIMES_QUERY, variables, api_key=api_key)
            pairings = _parse_tee_times(data, tournament_id, rnd)
            return sorted(pairings, key=lambda p: (p.round_number, p.group_number))
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


def pairings_to_records(
    pairings: List[Pairing],
    *,
    run_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten pairings into a list of dicts suitable for BigQuery insertion.
    Each row represents one player in one group.
    """
    from datetime import datetime

    ts = run_ts or datetime.utcnow().isoformat()
    rows: List[Dict[str, Any]] = []
    for pairing in pairings:
        for player in pairing.players:
            rows.append(
                {
                    "run_ts": ts,
                    "ingested_at": ts,
                    "tournament_id": pairing.tournament_id,
                    "round_number": pairing.round_number,
                    "round_status": pairing.round_status,
                    "group_number": pairing.group_number,
                    "tee_time": pairing.tee_time,
                    "start_hole": pairing.start_hole,
                    "back_nine": pairing.back_nine,
                    "course_id": pairing.course_id,
                    "course_name": pairing.course_name,
                    "player_id": player.player_id,
                    "player_display_name": player.display_name,
                    "player_first_name": player.first_name,
                    "player_last_name": player.last_name,
                    "country": player.country,
                    "world_rank": player.world_rank,
                    "amateur": player.amateur,
                }
            )
    return rows


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PGA Tour round pairings from the official GraphQL API."
    )
    parser.add_argument("--tournament", required=True, metavar="TOURNAMENT_ID",
                        help="PGA Tour tournament ID, e.g. R2026010")
    parser.add_argument("--round", required=True, metavar="ROUND",
                        help="Round number 1-4 (or 0 for all rounds)")
    parser.add_argument("--json", action="store_true", dest="as_json",
                        help="Output flat JSON records instead of a table")
    args = parser.parse_args()

    print(f"Fetching: tournament={args.tournament} round={args.round}")
    pairings = fetch_pairings(args.tournament, args.round)

    if not pairings:
        print("No pairings returned. Check the tournament ID and round number.")
        sys.exit(1)

    if args.as_json:
        print(json.dumps(pairings_to_records(pairings), indent=2, default=str))
        return

    print(f"\nFound {len(pairings)} groups  |  round {pairings[0].round_number}"
          f"  |  {pairings[0].course_name or 'N/A'}\n")
    print(f"{'Grp':>4}  {'Tee time (UTC)':>19}  {'Hole':>4}  Players")
    print("-" * 80)
    for p in pairings:
        names = ", ".join(pl.display_name for pl in p.players)
        tee = p.tee_time or "TBD"
        print(f"{p.group_number:>4}  {tee:>19}  {p.start_hole:>4}  {names}")


if __name__ == "__main__":
    _cli()
