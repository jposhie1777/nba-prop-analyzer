"""
PGA Tour GraphQL scraper — tournament leaderboard.

Queries leaderboardV3(id: $tournamentId) for current standings, positions
and per-round scoring summaries.

Usage (standalone CLI):
    python pga_leaderboard.py --tournament R2026010
    python pga_leaderboard.py --tournament R2026010 --raw   # dump raw JSON

Usage (as a module):
    from ingest.pga.pga_leaderboard import fetch_leaderboard, leaderboard_to_records
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
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

# Field names confirmed via live schema introspection of LeaderboardRowV3.
# Run: python3 -c "from introspect_pga_schema import introspect_type; introspect_type('LeaderboardRowV3')"
# to re-discover if this query ever breaks.
LEADERBOARD_QUERY = """
query Leaderboard($id: ID!) {
  leaderboardV3(id: $id) {
    id
    players {
      ... on PlayerRowV3 {
        id
        leaderboardSortOrder
        player {
          id
          firstName
          lastName
          displayName
          country
          amateur
          status
        }
        scoringData {
          currentRound
          playerState
          backNine
          totalStrokes
          total
          totalSort
          thru
          score
          scoreSort
          position
          rounds
          roundStatus
          movementDirection
          movementAmount
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
class RoundSummary:
    round_number: int
    score: Optional[str] = None          # score to par string e.g. "-4", "E", "+2"


@dataclass
class LeaderboardPlayer:
    player_id: str
    display_name: str
    first_name: str = ""
    last_name: str = ""
    country: str = ""
    amateur: bool = False
    position: Optional[str] = None
    sort_order: Optional[int] = None     # leaderboardSortOrder
    total: Optional[str] = None          # score to par, e.g. "-10" or "E"
    total_sort: Optional[int] = None     # numeric sort key for total
    total_strokes: Optional[str] = None  # raw stroke count string
    thru: Optional[str] = None           # "F", "18", "9", etc.
    score: Optional[str] = None          # current-round score to par
    current_round: Optional[int] = None
    player_state: Optional[str] = None   # e.g. "active", "cut", "wd"
    round_status: Optional[str] = None
    back_nine: bool = False
    movement_direction: Optional[str] = None
    movement_amount: Optional[str] = None
    rounds: List[RoundSummary] = field(default_factory=list)


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


def _parse_player(raw: Dict[str, Any]) -> Optional["LeaderboardPlayer"]:
    # Skip InformationRow items (cut-line banners etc.) — they have no 'player' key
    player_info = raw.get("player")
    if not player_info:
        return None

    scoring = raw.get("scoringData") or {}
    rounds_raw = scoring.get("rounds") or []

    return LeaderboardPlayer(
        player_id=str(player_info.get("id") or raw.get("id", "")),
        display_name=player_info.get("displayName") or "",
        first_name=player_info.get("firstName") or "",
        last_name=player_info.get("lastName") or "",
        country=player_info.get("country") or "",
        amateur=bool(player_info.get("amateur", False)),
        position=scoring.get("position"),
        sort_order=_safe_int(raw.get("leaderboardSortOrder")),
        total=scoring.get("total"),
        total_sort=_safe_int(scoring.get("totalSort")),
        total_strokes=scoring.get("totalStrokes"),
        thru=scoring.get("thru"),
        score=scoring.get("score"),
        current_round=_safe_int(scoring.get("currentRound")),
        player_state=str(scoring.get("playerState") or ""),
        round_status=scoring.get("roundStatus"),
        back_nine=bool(scoring.get("backNine", False)),
        movement_direction=str(scoring.get("movementDirection") or ""),
        movement_amount=scoring.get("movementAmount"),
        rounds=[
            RoundSummary(round_number=i + 1, score=s if isinstance(s, str) else str(s))
            for i, s in enumerate(rounds_raw)
            if s is not None
        ],
    )


def _parse_leaderboard(data: Dict[str, Any]) -> List[LeaderboardPlayer]:
    lb = data.get("leaderboardV3") or {}
    results = []
    for p in lb.get("players") or []:
        parsed = _parse_player(p)
        if parsed is not None:
            results.append(parsed)
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_leaderboard(
    tournament_id: str,
    *,
    api_key: str = DEFAULT_API_KEY,
    retries: int = 3,
) -> List[LeaderboardPlayer]:
    """
    Fetch leaderboard standings for a tournament.

    Args:
        tournament_id: PGA Tour tournament ID, e.g. ``"R2026010"``.
        api_key:       API key (uses env var or built-in default).
        retries:       Number of retry attempts on transient HTTP failures.

    Returns:
        List of :class:`LeaderboardPlayer` objects in position order.
    """
    backoff = 2
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            data = _post_graphql(
                LEADERBOARD_QUERY, {"id": tournament_id}, api_key=api_key
            )
            return _parse_leaderboard(data)
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


def fetch_leaderboard_raw(
    tournament_id: str,
    *,
    api_key: str = DEFAULT_API_KEY,
) -> Dict[str, Any]:
    """Return the raw GraphQL response dict — useful for debugging field names."""
    return _post_graphql(LEADERBOARD_QUERY, {"id": tournament_id}, api_key=api_key)


def leaderboard_to_records(
    tournament_id: str,
    players: List[LeaderboardPlayer],
    *,
    run_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten leaderboard players into BigQuery-ready row dicts.

    Produces one row per player when there are no round breakdowns, or one row
    per (player, round) when round data is present.
    """
    from datetime import datetime

    ts = run_ts or datetime.utcnow().isoformat()
    rows: List[Dict[str, Any]] = []
    for p in players:
        base = {
            "run_ts": ts,
            "ingested_at": ts,
            "tournament_id": tournament_id,
            "player_id": p.player_id,
            "player_display_name": p.display_name,
            "first_name": p.first_name,
            "last_name": p.last_name,
            "country": p.country,
            "amateur": p.amateur,
            "position": p.position,
            "sort_order": p.sort_order,
            "total": p.total,
            "total_sort": p.total_sort,
            "total_strokes": p.total_strokes,
            "thru": p.thru,
            "score": p.score,
            "current_round": p.current_round,
            "player_state": p.player_state,
            "round_status": p.round_status,
            "back_nine": p.back_nine,
            "movement_direction": p.movement_direction,
            "movement_amount": p.movement_amount,
        }
        if p.rounds:
            for rnd in p.rounds:
                row = dict(base)
                row["round_number"] = rnd.round_number
                row["round_score"] = rnd.score
                rows.append(row)
        else:
            rows.append(base)
    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PGA Tour leaderboard from the official GraphQL API."
    )
    parser.add_argument(
        "--tournament",
        required=True,
        metavar="TOURNAMENT_ID",
        help="PGA Tour tournament ID, e.g. R2026010",
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

    print(f"Fetching leaderboard: tournament={args.tournament}", file=sys.stderr)

    if args.raw:
        data = fetch_leaderboard_raw(args.tournament)
        print(json.dumps(data, indent=2, default=str))
        return

    players = fetch_leaderboard(args.tournament)
    if not players:
        print("No players returned. Check the tournament ID.", file=sys.stderr)
        sys.exit(1)

    if args.as_json:
        print(
            json.dumps(
                leaderboard_to_records(args.tournament, players), indent=2, default=str
            )
        )
        return

    print(f"\n{'Pos':>5}  {'Player':30}  {'Total':>6}  {'Strokes':>7}  {'Thru':>5}  {'State'}")
    print("-" * 75)
    for p in players[:30]:
        pos = p.position or "?"
        total = p.total or "?"
        strokes = p.total_strokes or "?"
        thru = p.thru or "?"
        state = p.player_state or ""
        print(f"{pos:>5}  {p.display_name:30}  {total:>6}  {strokes:>7}  {thru:>5}  {state}")
    if len(players) > 30:
        print(f"  ... and {len(players) - 30} more players")


if __name__ == "__main__":
    _cli()
