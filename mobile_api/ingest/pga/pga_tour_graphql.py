"""
PGA Tour GraphQL scraper for round pairings / tee times.

PGA Tour exposes a public GraphQL orchestrator used by pgatour.com itself.
Endpoint : https://orchestrator.pgatour.com/graphql
Auth     : x-api-key header (public, embedded in the PGA Tour web app)

No account or paid subscription is required – this is the same API that
powers https://www.pgatour.com/leaderboard and the tee-times pages.

Usage (standalone):
    python pga_tour_graphql.py --tournament R2025016 --round 1

Usage (as a module):
    from ingest.pga.pga_tour_graphql import fetch_pairings, Pairing
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
# This is NOT a secret – it is present in every unauthenticated request from
# pgatour.com and is safe to commit.
DEFAULT_API_KEY = os.getenv("PGA_TOUR_GQL_API_KEY", "da2-gsrx5bibzbb4njvhl7t37wqyl4")

DEFAULT_TIMEOUT = 20

# ---------------------------------------------------------------------------
# GraphQL query
# ---------------------------------------------------------------------------

# The `pairingsByRound` query returns all groups for a given round.
# Each group contains: players (with player info), tee time, start hole,
# and the round/course it belongs to.
PAIRINGS_QUERY = """
query PairingsByRound($tournamentId: ID!, $roundId: ID!, $cut: CutLineEnum) {
  pairingsByRound(tournamentId: $tournamentId, roundId: $roundId, cut: $cut) {
    pairings {
      groupNumber
      teeTime
      startHole
      players {
        id
        displayName
        firstName
        lastName
        country
        countryFlag
        amateur
        tourBound
        headshot
        playerBio {
          rankWorld
          rankCountry
        }
      }
    }
    roundComplete
    roundStatus
    roundNumber
    tournamentId
    courseId
    courseName
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
    group_number: int
    tee_time: Optional[str]
    start_hole: int
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
        # Mimic the Referer that pgatour.com sends so the request is accepted.
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
    return PlayerInfo(
        player_id=str(raw.get("id", "")),
        display_name=raw.get("displayName") or f"{raw.get('firstName', '')} {raw.get('lastName', '')}".strip(),
        first_name=raw.get("firstName"),
        last_name=raw.get("lastName"),
        country=raw.get("country"),
        world_rank=_safe_int(bio.get("rankWorld")),
        amateur=bool(raw.get("amateur", False)),
    )


def _parse_pairings_response(
    data: Dict[str, Any],
    tournament_id: str,
) -> List[Pairing]:
    pairings_by_round = data.get("pairingsByRound") or {}
    raw_pairings = pairings_by_round.get("pairings") or []
    round_number = _safe_int(pairings_by_round.get("roundNumber")) or 0
    course_name = pairings_by_round.get("courseName")

    result: List[Pairing] = []
    for raw in raw_pairings:
        players = [_parse_player(p) for p in (raw.get("players") or [])]
        result.append(
            Pairing(
                tournament_id=tournament_id,
                round_number=round_number,
                group_number=_safe_int(raw.get("groupNumber")) or 0,
                tee_time=raw.get("teeTime"),
                start_hole=_safe_int(raw.get("startHole")) or 1,
                course_name=course_name,
                players=players,
            )
        )
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_pairings(
    tournament_id: str,
    round_id: str,
    *,
    cut: Optional[str] = None,
    api_key: str = DEFAULT_API_KEY,
    retries: int = 3,
) -> List[Pairing]:
    """
    Fetch round pairings/tee times from the PGA Tour GraphQL API.

    Args:
        tournament_id: PGA Tour tournament ID, e.g. ``"R2025016"``.
        round_id:      Round number as a string, e.g. ``"1"``, ``"2"``, etc.
        cut:           Optional cut filter.  Accepted values: ``"ALL"``,
                       ``"MADE"``, ``"MISSED"`` (default ``None`` → all players).
        api_key:       PGA Tour GraphQL API key (uses env var or built-in default).
        retries:       Number of retry attempts on transient failures.

    Returns:
        List of :class:`Pairing` objects sorted by group number.
    """
    variables: Dict[str, Any] = {
        "tournamentId": tournament_id,
        "roundId": round_id,
    }
    if cut:
        variables["cut"] = cut

    backoff = 2
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            data = _post_graphql(PAIRINGS_QUERY, variables, api_key=api_key)
            pairings = _parse_pairings_response(data, tournament_id)
            return sorted(pairings, key=lambda p: p.group_number)
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
                    "group_number": pairing.group_number,
                    "tee_time": pairing.tee_time,
                    "start_hole": pairing.start_hole,
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
# CLI entry point (for quick local testing)
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PGA Tour round pairings from the official GraphQL API."
    )
    parser.add_argument(
        "--tournament",
        required=True,
        metavar="TOURNAMENT_ID",
        help="PGA Tour tournament ID, e.g. R2025016",
    )
    parser.add_argument(
        "--round",
        required=True,
        metavar="ROUND",
        help="Round number (1-4)",
    )
    parser.add_argument(
        "--cut",
        choices=["ALL", "MADE", "MISSED"],
        default=None,
        help="Filter by cut status (default: all players)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Output raw JSON records instead of the default table",
    )
    args = parser.parse_args()

    print(f"Fetching pairings: tournament={args.tournament} round={args.round} cut={args.cut}")
    pairings = fetch_pairings(args.tournament, args.round, cut=args.cut)

    if not pairings:
        print("No pairings returned. Check the tournament ID and round number.")
        sys.exit(1)

    if args.as_json:
        records = pairings_to_records(pairings)
        print(json.dumps(records, indent=2, default=str))
        return

    # Pretty-print as a table
    print(f"\nFound {len(pairings)} groups\n")
    print(f"{'Grp':>4}  {'Tee':>5}  {'Hole':>4}  {'Players'}")
    print("-" * 80)
    for p in pairings:
        player_names = ", ".join(pl.display_name for pl in p.players)
        tee = p.tee_time or "TBD"
        # Trim ISO tee time to HH:MM for readability
        if "T" in tee:
            tee = tee.split("T")[1][:5]
        print(f"{p.group_number:>4}  {tee:>5}  {p.start_hole:>4}  {player_names}")


if __name__ == "__main__":
    _cli()
