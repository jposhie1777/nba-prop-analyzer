# routes/ladders.py
from fastapi import APIRouter, Query
from google.cloud import bigquery
from typing import Dict, Any, List
from collections import defaultdict

from bq import get_bq_client

router = APIRouter(prefix="/ladders", tags=["ladders"])

LADDERS_QUERY = """
WITH props AS (
  SELECT
    p.game_id,
    p.player_id,
    p.market,
    p.market_type,
    p.line,
    p.book,
    p.over_odds,
    p.under_odds,
    p.milestone_odds,
    p.snapshot_ts
  FROM `nba_live.v_live_player_prop_odds_latest` p
),
games AS (
  SELECT
    game_id,
    home_team_abbr,
    away_team_abbr
  FROM `nba_live.live_games`
),
players AS (
  SELECT
    player_id,
    player_name
  FROM `nba_goat_data.player_lookup`
)
SELECT
  p.game_id,
  p.player_id,
  pl.player_name,
  g.home_team_abbr,
  g.away_team_abbr,
  p.market,
  p.market_type,
  p.line,
  p.book,
  p.over_odds,
  p.under_odds,
  p.milestone_odds,
  p.snapshot_ts
FROM props p
JOIN games g ON p.game_id = g.game_id
LEFT JOIN players pl ON p.player_id = pl.player_id
ORDER BY p.player_id, p.market, p.line, p.book
"""


def calculate_ladder_score(odds: int) -> float:
    """
    Calculate a simple ladder score based on odds.
    Better odds (more negative or less positive) = higher score.
    """
    if odds is None:
        return 0.0
    # Convert American odds to implied probability edge
    if odds < 0:
        implied = abs(odds) / (abs(odds) + 100)
    else:
        implied = 100 / (odds + 100)
    # Score: higher implied prob = better value
    return round((implied - 0.5) * 10, 1)


def determine_tier(vendor_count: int, score_spread: float) -> str:
    """Determine ladder tier based on vendor coverage and score spread."""
    if vendor_count >= 3 and score_spread >= 2.0:
        return "A"
    elif vendor_count >= 2 and score_spread >= 1.0:
        return "B"
    else:
        return "C"


@router.get("")
def get_ladders(
    limit: int = Query(50, ge=10, le=200),
    min_vendors: int = Query(1, ge=1, le=5),
    market: str | None = Query(None, description="Filter by market (pts, ast, reb, etc.)"),
) -> Dict[str, Any]:
    """
    Returns prop ladders - comparison of lines across vendors for each player+market.

    A ladder shows all available lines from different sportsbooks for the same prop,
    allowing users to find the best value.
    """
    client = get_bq_client()

    job = client.query(LADDERS_QUERY)
    rows = list(job.result())

    # Group by player_id + market to build ladders
    ladder_groups: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "game_id": None,
        "player_id": None,
        "player_name": None,
        "player_team_abbr": None,
        "opponent_team_abbr": None,
        "market": None,
        "vendors": defaultdict(list),
        "all_lines": [],
        "snapshot_ts": None,
    })

    for row in rows:
        key = f"{row.player_id}-{row.market}"
        group = ladder_groups[key]

        # Set metadata from first row
        if group["game_id"] is None:
            group["game_id"] = row.game_id
            group["player_id"] = row.player_id
            group["player_name"] = row.player_name or f"Player {row.player_id}"
            group["player_team_abbr"] = row.home_team_abbr or "UNK"
            group["opponent_team_abbr"] = row.away_team_abbr or "UNK"
            group["market"] = row.market
            group["snapshot_ts"] = row.snapshot_ts

        # Determine which odds to use (over_odds for over/under, milestone_odds for yes/no)
        odds = row.over_odds or row.milestone_odds
        if odds is None:
            continue

        score = calculate_ladder_score(odds)

        rung = {
            "line": float(row.line) if row.line else 0,
            "odds": odds,
            "ladder_score": score,
        }

        group["vendors"][row.book].append(rung)
        group["all_lines"].append(row.line)

    # Build final ladder list
    ladders: List[Dict[str, Any]] = []

    for key, group in ladder_groups.items():
        vendor_count = len(group["vendors"])

        # Apply min_vendors filter
        if vendor_count < min_vendors:
            continue

        # Apply market filter
        if market and group["market"] != market:
            continue

        # Calculate aggregate stats
        all_scores = []
        for vendor, rungs in group["vendors"].items():
            all_scores.extend([r["ladder_score"] for r in rungs])

        if not all_scores:
            continue

        max_score = max(all_scores)
        min_score = min(all_scores)
        score_spread = max_score - min_score

        # Determine anchor line (most common line)
        if group["all_lines"]:
            anchor_line = max(set(group["all_lines"]), key=group["all_lines"].count)
        else:
            anchor_line = 0

        tier = determine_tier(vendor_count, score_spread)

        # Build vendor ladder blocks
        ladder_by_vendor = []
        for vendor, rungs in group["vendors"].items():
            # Sort rungs by line
            sorted_rungs = sorted(rungs, key=lambda x: x["line"])
            ladder_by_vendor.append({
                "vendor": vendor,
                "rungs": sorted_rungs,
            })

        # Sort vendors alphabetically
        ladder_by_vendor.sort(key=lambda x: x["vendor"])

        ladders.append({
            "game_id": group["game_id"],
            "player_id": group["player_id"],
            "player_name": group["player_name"],
            "player_team_abbr": group["player_team_abbr"],
            "opponent_team_abbr": group["opponent_team_abbr"],
            "market": group["market"],
            "ladder_tier": tier,
            "anchor_line": float(anchor_line) if anchor_line else 0,
            "ladder_score": max_score,
            "ladder_by_vendor": ladder_by_vendor,
        })

    # Sort by ladder_score descending
    ladders.sort(key=lambda x: x["ladder_score"], reverse=True)

    # Apply limit
    ladders = ladders[:limit]

    return {
        "count": len(ladders),
        "ladders": ladders,
    }
