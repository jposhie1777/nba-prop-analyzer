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

import requests

from .pga_tour_graphql import (
    GRAPHQL_ENDPOINT,
    DEFAULT_API_KEY,
    DEFAULT_TIMEOUT,
    _graphql_headers,
)

# ---------------------------------------------------------------------------
# GraphQL query
# ---------------------------------------------------------------------------

LEADERBOARD_QUERY = """
query Leaderboard($id: ID!) {
  leaderboardV3(id: $id) {
    id
    players {
      id
      isWithdrawn
      displayName
      position
      startPosition
      total
      totalStrokes
      scoringData {
        total
        totalStrokes
        rounds
        projected
        movementDirection
        movementAmount
      }
      rounds {
        birdies
        bogeys
        eagles
        pars
        doubleOrWorse
        roundScore
        parRelativeScore
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
    birdies: Optional[int] = None
    bogeys: Optional[int] = None
    eagles: Optional[int] = None
    pars: Optional[int] = None
    double_or_worse: Optional[int] = None
    round_score: Optional[int] = None
    par_relative_score: Optional[int] = None


@dataclass
class LeaderboardPlayer:
    player_id: str
    display_name: str
    position: Optional[str] = None
    start_position: Optional[str] = None
    total: Optional[str] = None          # score to par, e.g. "-10" or "E"
    total_strokes: Optional[int] = None
    is_withdrawn: bool = False
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


def _parse_round(raw: Dict[str, Any]) -> RoundSummary:
    return RoundSummary(
        birdies=_safe_int(raw.get("birdies")),
        bogeys=_safe_int(raw.get("bogeys")),
        eagles=_safe_int(raw.get("eagles")),
        pars=_safe_int(raw.get("pars")),
        double_or_worse=_safe_int(raw.get("doubleOrWorse")),
        round_score=_safe_int(raw.get("roundScore")),
        par_relative_score=_safe_int(raw.get("parRelativeScore")),
    )


def _parse_player(raw: Dict[str, Any]) -> LeaderboardPlayer:
    scoring = raw.get("scoringData") or {}
    rounds_raw = raw.get("rounds") or []
    return LeaderboardPlayer(
        player_id=str(raw.get("id", "")),
        display_name=raw.get("displayName") or "",
        position=raw.get("position"),
        start_position=raw.get("startPosition"),
        total=raw.get("total") or scoring.get("total"),
        total_strokes=_safe_int(raw.get("totalStrokes") or scoring.get("totalStrokes")),
        is_withdrawn=bool(raw.get("isWithdrawn", False)),
        rounds=[_parse_round(r) for r in rounds_raw if isinstance(r, dict)],
    )


def _parse_leaderboard(data: Dict[str, Any]) -> List[LeaderboardPlayer]:
    lb = data.get("leaderboardV3") or {}
    return [_parse_player(p) for p in (lb.get("players") or [])]


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
            "position": p.position,
            "start_position": p.start_position,
            "total": p.total,
            "total_strokes": p.total_strokes,
            "is_withdrawn": p.is_withdrawn,
        }
        if p.rounds:
            for i, rnd in enumerate(p.rounds, start=1):
                row = dict(base)
                row.update(
                    {
                        "round_number": i,
                        "birdies": rnd.birdies,
                        "bogeys": rnd.bogeys,
                        "eagles": rnd.eagles,
                        "pars": rnd.pars,
                        "double_or_worse": rnd.double_or_worse,
                        "round_score": rnd.round_score,
                        "round_par_relative_score": rnd.par_relative_score,
                    }
                )
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

    print(f"\n{'Pos':>5}  {'Player':30}  {'Total':>6}  {'Strokes':>7}  {'Status'}")
    print("-" * 65)
    for p in players[:30]:
        pos = p.position or "?"
        total = p.total or "?"
        strokes = str(p.total_strokes) if p.total_strokes is not None else "?"
        status = "WD" if p.is_withdrawn else ""
        print(f"{pos:>5}  {p.display_name:30}  {total:>6}  {strokes:>7}  {status}")
    if len(players) > 30:
        print(f"  ... and {len(players) - 30} more players")


if __name__ == "__main__":
    _cli()
