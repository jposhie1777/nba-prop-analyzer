"""
PGA Tour GraphQL scraper — player scorecards (hole-by-hole).

Queries scorecardV3(tournamentId: $id, playerId: $playerId) for per-hole
data for a specific player in a tournament.

Usage (standalone CLI):
    python pga_scorecards.py --tournament R2026010 --player 46046
    python pga_scorecards.py --tournament R2026010 --player 46046 --raw

Usage (as a module):
    from ingest.pga.pga_scorecards import fetch_scorecard, scorecard_to_records
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


class GraphQLQueryError(RuntimeError):
    """GraphQL response-level error with access to the structured errors list."""

    def __init__(self, errors: List[Dict[str, Any]]):
        self.errors = errors
        super().__init__(f"GraphQL errors: {json.dumps(errors, indent=2)}")

    def has_field_undefined(self, *, type_name: str, field_name: str) -> bool:
        target = f"Field '{field_name}' in type '{type_name}' is undefined"
        for err in self.errors:
            msg = str((err or {}).get("message") or "")
            if "FieldUndefined" in msg and target in msg:
                return True
        return False


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

SCORECARD_QUERY = """
query Scorecard($id: ID!, $playerId: ID!) {
  scorecardV3(tournamentId: $id, playerId: $playerId) {
    player {
      id
      displayName
    }
    rounds {
      roundNumber
      parRelativeScore
      strokes
      holes {
        holeNumber
        par
        score
        birdie
        eagle
        bogey
        doubleOrWorse
        putts
        drivingDistance
        holeInOne
      }
    }
  }
}
"""

# Backward-compatible fallback for older schema variants.
SCORECARD_QUERY_LEGACY = SCORECARD_QUERY.replace("    rounds {", "    courseRounds {")

# scorecardStats gives aggregate stats per player per round (no hole detail)
SCORECARD_STATS_QUERY = """
query ScorecardStats($id: ID!, $playerId: ID!) {
  scorecardStats(id: $id, playerId: $playerId) {
    player {
      id
      displayName
    }
    courseRounds {
      roundNumber
      parRelativeScore
      strokes
      birdies
      bogeys
      eagles
      pars
      doubleOrWorse
      greensInRegulation
      fairwaysHit
      putts
      drivingDistance
      drivingAccuracy
    }
  }
}
"""

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class HoleScore:
    hole_number: int
    par: Optional[int] = None
    score: Optional[int] = None          # strokes taken on this hole
    birdie: Optional[bool] = None
    eagle: Optional[bool] = None
    bogey: Optional[bool] = None
    double_or_worse: Optional[bool] = None
    putts: Optional[int] = None
    driving_distance: Optional[int] = None
    hole_in_one: Optional[bool] = None


@dataclass
class ScorecardRound:
    round_number: int
    par_relative_score: Optional[int] = None
    strokes: Optional[int] = None
    # Aggregate stats (from scorecardStats query)
    birdies: Optional[int] = None
    bogeys: Optional[int] = None
    eagles: Optional[int] = None
    pars: Optional[int] = None
    double_or_worse: Optional[int] = None
    greens_in_regulation: Optional[int] = None
    fairways_hit: Optional[int] = None
    putts: Optional[int] = None
    driving_distance: Optional[int] = None
    driving_accuracy: Optional[float] = None
    # Hole-by-hole detail (from scorecardV3 query)
    holes: List[HoleScore] = field(default_factory=list)


@dataclass
class PlayerScorecard:
    player_id: str
    display_name: str
    tournament_id: str
    rounds: List[ScorecardRound] = field(default_factory=list)


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
        raise GraphQLQueryError(errors)
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


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _parse_hole(raw: Dict[str, Any]) -> HoleScore:
    return HoleScore(
        hole_number=_safe_int(raw.get("holeNumber")) or 0,
        par=_safe_int(raw.get("par")),
        score=_safe_int(raw.get("score")),
        birdie=raw.get("birdie"),
        eagle=raw.get("eagle"),
        bogey=raw.get("bogey"),
        double_or_worse=raw.get("doubleOrWorse"),
        putts=_safe_int(raw.get("putts")),
        driving_distance=_safe_int(raw.get("drivingDistance")),
        hole_in_one=raw.get("holeInOne"),
    )


def _parse_round(raw: Dict[str, Any]) -> ScorecardRound:
    return ScorecardRound(
        round_number=_safe_int(raw.get("roundNumber")) or 0,
        par_relative_score=_safe_int(raw.get("parRelativeScore")),
        strokes=_safe_int(raw.get("strokes")),
        birdies=_safe_int(raw.get("birdies")),
        bogeys=_safe_int(raw.get("bogeys")),
        eagles=_safe_int(raw.get("eagles")),
        pars=_safe_int(raw.get("pars")),
        double_or_worse=_safe_int(raw.get("doubleOrWorse")),
        greens_in_regulation=_safe_int(raw.get("greensInRegulation")),
        fairways_hit=_safe_int(raw.get("fairwaysHit")),
        putts=_safe_int(raw.get("putts")),
        driving_distance=_safe_int(raw.get("drivingDistance")),
        driving_accuracy=_safe_float(raw.get("drivingAccuracy")),
        holes=[_parse_hole(h) for h in (raw.get("holes") or []) if isinstance(h, dict)],
    )


def _parse_scorecard(
    data: Dict[str, Any],
    tournament_id: str,
    key: str = "scorecardV3",
) -> Optional[PlayerScorecard]:
    raw = data.get(key)
    if not raw:
        return None
    # Support both schema shapes for round lists (`rounds` vs `courseRounds`).
    # Player identity can also be nested (`player { id, displayName }`) or root-level.
    player_obj = raw.get("player") or {}
    player_id = str(player_obj.get("id") or raw.get("playerId") or "")
    display_name = player_obj.get("displayName") or raw.get("displayName") or ""
    rounds_data = raw.get("courseRounds") or raw.get("rounds") or []
    return PlayerScorecard(
        player_id=player_id,
        display_name=display_name,
        tournament_id=tournament_id,
        rounds=[_parse_round(r) for r in rounds_data if isinstance(r, dict)],
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_scorecard(
    tournament_id: str,
    player_id: str,
    *,
    api_key: str = DEFAULT_API_KEY,
    retries: int = 3,
) -> Optional[PlayerScorecard]:
    """
    Fetch hole-by-hole scorecard for a player in a tournament.

    Args:
        tournament_id: PGA Tour tournament ID, e.g. ``"R2026010"``.
        player_id:     PGA Tour player ID, e.g. ``"46046"``.
        api_key:       API key.
        retries:       Retry attempts on transient HTTP failures.

    Returns:
        :class:`PlayerScorecard` with hole-by-hole detail, or ``None`` if not found.
    """
    variables: Dict[str, Any] = {"id": tournament_id, "playerId": player_id}
    backoff = 2
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            try:
                data = _post_graphql(SCORECARD_QUERY, variables, api_key=api_key)
            except GraphQLQueryError as exc:
                # PGA schema has changed between `courseRounds` and `rounds` over time.
                # Retry the legacy query when the current field isn't available.
                if not exc.has_field_undefined(type_name="ScorecardV3", field_name="rounds"):
                    raise

                try:
                    data = _post_graphql(SCORECARD_QUERY_LEGACY, variables, api_key=api_key)
                except GraphQLQueryError as legacy_exc:
                    # If both variants are undefined, surface the *primary* (current)
                    # schema error so logs reflect the main query attempted.
                    if legacy_exc.has_field_undefined(type_name="ScorecardV3", field_name="courseRounds"):
                        raise exc
                    raise
            return _parse_scorecard(data, tournament_id, key="scorecardV3")
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


def merge_scorecards(
    detail: Optional[PlayerScorecard],
    stats: Optional[PlayerScorecard],
) -> Optional[PlayerScorecard]:
    """Merge scorecard detail (holes) with scorecardStats round aggregates."""
    if detail is None:
        return stats
    if stats is None:
        return detail

    detail_by_round = {r.round_number: r for r in detail.rounds}
    for s in stats.rounds:
        rnd = detail_by_round.get(s.round_number)
        if rnd is None:
            detail.rounds.append(s)
            continue
        rnd.birdies = s.birdies
        rnd.bogeys = s.bogeys
        rnd.eagles = s.eagles
        rnd.pars = s.pars
        rnd.double_or_worse = s.double_or_worse
        rnd.greens_in_regulation = s.greens_in_regulation
        rnd.fairways_hit = s.fairways_hit
        rnd.putts = s.putts
        rnd.driving_distance = s.driving_distance
        rnd.driving_accuracy = s.driving_accuracy

    detail.rounds.sort(key=lambda r: r.round_number)
    return detail


def fetch_scorecard_stats(
    tournament_id: str,
    player_id: str,
    *,
    api_key: str = DEFAULT_API_KEY,
    retries: int = 3,
) -> Optional[PlayerScorecard]:
    """
    Fetch per-round aggregate stats (birdies, putts, GIR, etc.) without hole detail.

    Uses ``scorecardStats`` which is lighter than ``scorecardV3``.
    """
    variables: Dict[str, Any] = {"id": tournament_id, "playerId": player_id}
    backoff = 2
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            data = _post_graphql(SCORECARD_STATS_QUERY, variables, api_key=api_key)
            return _parse_scorecard(data, tournament_id, key="scorecardStats")
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


def fetch_scorecard_raw(
    tournament_id: str,
    player_id: str,
    *,
    stats_only: bool = False,
    api_key: str = DEFAULT_API_KEY,
) -> Dict[str, Any]:
    """Return the raw GraphQL response — useful for debugging field names.

    Args:
        stats_only: If ``True``, use ``scorecardStats`` (round aggregates only);
                    otherwise use ``scorecardV3`` (hole-by-hole detail).
    """
    variables: Dict[str, Any] = {"id": tournament_id, "playerId": player_id}
    query = SCORECARD_STATS_QUERY if stats_only else SCORECARD_QUERY
    return _post_graphql(query, variables, api_key=api_key)


def scorecard_to_records(
    scorecard: PlayerScorecard,
    *,
    run_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten scorecard into BigQuery-ready row dicts.

    If hole-by-hole data is present, produces one row per hole per round.
    Otherwise produces one row per round with aggregate stats only.
    """
    from datetime import datetime

    ts = run_ts or datetime.utcnow().isoformat()
    rows: List[Dict[str, Any]] = []

    for rnd in scorecard.rounds:
        round_base = {
            "run_ts": ts,
            "ingested_at": ts,
            "tournament_id": scorecard.tournament_id,
            "player_id": scorecard.player_id,
            "player_display_name": scorecard.display_name,
            "round_number": rnd.round_number,
            "round_par_relative_score": rnd.par_relative_score,
            "round_strokes": rnd.strokes,
            "birdies": rnd.birdies,
            "bogeys": rnd.bogeys,
            "eagles": rnd.eagles,
            "pars": rnd.pars,
            "double_or_worse": rnd.double_or_worse,
            "greens_in_regulation": rnd.greens_in_regulation,
            "fairways_hit": rnd.fairways_hit,
            "putts": rnd.putts,
            "driving_distance": rnd.driving_distance,
            "driving_accuracy": rnd.driving_accuracy,
        }
        if rnd.holes:
            for hole in rnd.holes:
                row = dict(round_base)
                row.update(
                    {
                        "hole_number": hole.hole_number,
                        "par": hole.par,
                        "score": hole.score,
                        "birdie": hole.birdie,
                        "eagle": hole.eagle,
                        "bogey": hole.bogey,
                        "double_or_worse_hole": hole.double_or_worse,
                        "hole_putts": hole.putts,
                        "hole_driving_distance": hole.driving_distance,
                        "hole_in_one": hole.hole_in_one,
                    }
                )
                rows.append(row)
        else:
            rows.append(round_base)

    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PGA Tour player scorecard from the official GraphQL API."
    )
    parser.add_argument(
        "--tournament",
        required=True,
        metavar="TOURNAMENT_ID",
        help="PGA Tour tournament ID, e.g. R2026010",
    )
    parser.add_argument(
        "--player",
        required=True,
        metavar="PLAYER_ID",
        help="PGA Tour player ID, e.g. 46046",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Use scorecardStats (round aggregates) instead of scorecardV3 (hole-by-hole)",
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
        help="Output flat JSON records (one row per hole, or per round with --stats-only)",
    )
    args = parser.parse_args()

    print(
        f"Fetching scorecard: tournament={args.tournament} player={args.player}",
        file=sys.stderr,
    )

    if args.raw:
        data = fetch_scorecard_raw(
            args.tournament, args.player, stats_only=args.stats_only
        )
        print(json.dumps(data, indent=2, default=str))
        return

    scorecard = (
        fetch_scorecard_stats(args.tournament, args.player)
        if args.stats_only
        else fetch_scorecard(args.tournament, args.player)
    )

    if not scorecard:
        print(
            "No scorecard returned. Check tournament and player IDs.", file=sys.stderr
        )
        sys.exit(1)

    if args.as_json:
        print(json.dumps(scorecard_to_records(scorecard), indent=2, default=str))
        return

    print(f"\n{scorecard.display_name} — {scorecard.tournament_id}\n")
    for rnd in scorecard.rounds:
        rel = (
            f"{rnd.par_relative_score:+d}"
            if rnd.par_relative_score is not None
            else "?"
        )
        print(f"  Round {rnd.round_number}  ({rnd.strokes or '?'} strokes, {rel})")

        if rnd.holes:
            print(f"    {'Hole':>4}  {'Par':>4}  {'Score':>6}  {'Putts':>5}  Result")
            for h in rnd.holes:
                marker = ""
                if h.hole_in_one:
                    marker = "HIO"
                elif h.eagle:
                    marker = "Eagle"
                elif h.birdie:
                    marker = "Birdie"
                elif h.bogey:
                    marker = "Bogey"
                elif h.double_or_worse:
                    marker = "Dbl+"
                putts = str(h.putts) if h.putts is not None else "-"
                par = str(h.par) if h.par is not None else "?"
                score = str(h.score) if h.score is not None else "?"
                print(
                    f"    {h.hole_number:>4}  {par:>4}  {score:>6}  {putts:>5}  {marker}"
                )
        else:
            stats = []
            if rnd.birdies is not None:
                stats.append(f"B:{rnd.birdies}")
            if rnd.bogeys is not None:
                stats.append(f"Bo:{rnd.bogeys}")
            if rnd.putts is not None:
                stats.append(f"Putts:{rnd.putts}")
            if rnd.greens_in_regulation is not None:
                stats.append(f"GIR:{rnd.greens_in_regulation}")
            if stats:
                print(f"    {' | '.join(stats)}")
        print()


if __name__ == "__main__":
    _cli()
