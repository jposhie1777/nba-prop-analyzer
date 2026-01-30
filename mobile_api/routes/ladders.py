# routes/ladders.py
from fastapi import APIRouter, Query
from typing import Dict, Any, List, Literal
from collections import defaultdict

from bq import get_bq_client

router = APIRouter(prefix="/ladders", tags=["ladders"])

# Minimum odds threshold (filter out extreme favorites like -5000)
MIN_ODDS_THRESHOLD = -800

# Pre-live: Uses player_prop_odds_master (pre-game props)
PRE_LIVE_QUERY = """
WITH props AS (
  SELECT
    p.game_id,
    p.player_id,
    p.market_key AS market,
    p.market_type,
    p.line_value AS line,
    p.vendor AS book,
    p.odds_over AS over_odds,
    p.odds_under AS under_odds,
    p.milestone_odds,
    p.snapshot_ts
  FROM `nba_live.player_prop_odds_master` p
  WHERE LOWER(p.vendor) IN ('draftkings', 'fanduel')
    AND p.market_window = 'FULL'
),
games AS (
  SELECT
    game_id,
    home_team_abbr,
    away_team_abbr,
    state,
    home_score,
    away_score
  FROM `nba_live.live_games`
  WHERE state = 'UPCOMING'
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
  g.home_score,
  g.away_score,
  'UPCOMING' AS game_state,
  p.market,
  p.market_type,
  p.line,
  p.book,
  p.over_odds,
  p.under_odds,
  p.milestone_odds,
  p.snapshot_ts,
  NULL AS current_stat
FROM props p
JOIN games g ON p.game_id = g.game_id
LEFT JOIN players pl ON p.player_id = pl.player_id
ORDER BY p.player_id, p.market, p.line, p.book
"""

# Live: Uses v_live_player_prop_odds_latest with player stats and game scores
LIVE_QUERY = """
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
  WHERE LOWER(p.book) IN ('draftkings', 'fanduel')
),
games AS (
  SELECT
    game_id,
    home_team_abbr,
    away_team_abbr,
    state,
    home_score,
    away_score,
    period,
    clock
  FROM `nba_live.live_games`
  WHERE state = 'LIVE'
),
players AS (
  SELECT
    player_id,
    player_name
  FROM `nba_goat_data.player_lookup`
),
player_stats AS (
  SELECT
    game_id,
    player_id,
    pts,
    reb,
    ast,
    fg3_made,
    ROW_NUMBER() OVER (PARTITION BY game_id, player_id ORDER BY ingested_at DESC) AS rn
  FROM `nba_live.live_player_stats`
)
SELECT
  p.game_id,
  p.player_id,
  pl.player_name,
  g.home_team_abbr,
  g.away_team_abbr,
  g.home_score,
  g.away_score,
  g.period,
  g.clock,
  'LIVE' AS game_state,
  p.market,
  p.market_type,
  p.line,
  p.book,
  p.over_odds,
  p.under_odds,
  p.milestone_odds,
  p.snapshot_ts,
  CASE
    WHEN UPPER(p.market) IN ('PTS', 'POINTS', 'PLAYER_POINTS') THEN ps.pts
    WHEN UPPER(p.market) IN ('REB', 'REBOUNDS', 'PLAYER_REBOUNDS', 'TRB') THEN ps.reb
    WHEN UPPER(p.market) IN ('AST', 'ASSISTS', 'PLAYER_ASSISTS') THEN ps.ast
    WHEN UPPER(p.market) IN ('3PM', '3PTS', 'THREES', 'FG3M', 'THREE_POINTERS', 'PLAYER_THREES', 'MADE_THREES') THEN ps.fg3_made
    ELSE NULL
  END AS current_stat
FROM props p
JOIN games g ON p.game_id = g.game_id
LEFT JOIN players pl ON p.player_id = pl.player_id
LEFT JOIN player_stats ps ON p.game_id = ps.game_id AND p.player_id = ps.player_id AND ps.rn = 1
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
    if vendor_count >= 2 and score_spread >= 1.0:
        return "A"
    elif vendor_count >= 1 and score_spread >= 0.5:
        return "B"
    else:
        return "C"


def process_rows_to_ladders(
    rows: List,
    min_vendors: int,
    market_filter: str | None,
    limit: int,
    min_odds: int = MIN_ODDS_THRESHOLD,
) -> List[Dict[str, Any]]:
    """Process query rows into ladder format."""

    # Group by player_id + market to build ladders
    ladder_groups: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "game_id": None,
        "player_id": None,
        "player_name": None,
        "player_team_abbr": None,
        "opponent_team_abbr": None,
        "home_score": None,
        "away_score": None,
        "period": None,
        "clock": None,
        "game_state": None,
        "market": None,
        "current_stat": None,
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
            group["home_score"] = row.home_score
            group["away_score"] = row.away_score
            group["period"] = getattr(row, "period", None)
            group["clock"] = getattr(row, "clock", None)
            group["game_state"] = row.game_state or "UPCOMING"
            group["market"] = row.market
            group["current_stat"] = getattr(row, "current_stat", None)
            group["snapshot_ts"] = row.snapshot_ts

        # Determine which odds to use (over_odds for over/under, milestone_odds for yes/no)
        odds = row.over_odds or row.milestone_odds
        if odds is None:
            continue

        # Convert to int for comparison
        odds = int(odds)

        # Filter out extreme odds (e.g., -5000)
        # Skip if odds are worse than min_odds threshold
        if odds < min_odds:
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
        if market_filter and group["market"] != market_filter:
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

            # Dedupe: prefer whole numbers over half-lines (e.g., keep 4, drop 3.5 and 4.5)
            # Build set of whole number lines (e.g., 2.0, 3.0, 4.0)
            whole_lines = set()
            for r in sorted_rungs:
                line = round(r["line"], 1)
                if line == int(line):
                    whole_lines.add(int(line))

            deduped_rungs = []
            for r in sorted_rungs:
                line = round(r["line"], 1)
                # Check if this is a half-line (x.5)
                if line != int(line):
                    # It's a half-line - check if adjacent whole numbers exist
                    floor_val = int(line)  # e.g., 3.5 -> 3
                    ceil_val = floor_val + 1  # e.g., 3.5 -> 4
                    if floor_val in whole_lines or ceil_val in whole_lines:
                        continue  # Skip this half-line
                deduped_rungs.append(r)

            ladder_by_vendor.append({
                "vendor": vendor,
                "rungs": deduped_rungs,
            })

        # Sort vendors alphabetically
        ladder_by_vendor.sort(key=lambda x: x["vendor"])

        ladder_data = {
            "game_id": group["game_id"],
            "player_id": group["player_id"],
            "player_name": group["player_name"],
            "player_team_abbr": group["player_team_abbr"],
            "opponent_team_abbr": group["opponent_team_abbr"],
            "game_state": group["game_state"],
            "market": group["market"],
            "ladder_tier": tier,
            "anchor_line": float(anchor_line) if anchor_line else 0,
            "ladder_score": max_score,
            "ladder_by_vendor": ladder_by_vendor,
        }

        # Add live-specific fields
        if group["game_state"] == "LIVE":
            ladder_data["current_stat"] = group["current_stat"]
            ladder_data["game_score"] = {
                "home": group["home_score"],
                "away": group["away_score"],
            }
            ladder_data["game_clock"] = {
                "period": group["period"],
                "clock": group["clock"],
            }

        ladders.append(ladder_data)

    # Sort by ladder_score descending
    ladders.sort(key=lambda x: x["ladder_score"], reverse=True)

    # Apply limit
    return ladders[:limit]


@router.get("")
def get_ladders(
    mode: Literal["pre-live", "live"] = Query("pre-live", description="pre-live for upcoming games, live for in-progress"),
    limit: int = Query(50, ge=10, le=200),
    min_vendors: int = Query(1, ge=1, le=5),
    market: str | None = Query(None, description="Filter by market (pts, ast, reb, etc.)"),
    min_odds: int = Query(MIN_ODDS_THRESHOLD, description="Minimum odds threshold (e.g., -800 filters out -5000)"),
) -> Dict[str, Any]:
    """
    Returns prop ladders - comparison of lines across vendors for each player+market.

    - pre-live: Shows props for upcoming games (from player_prop_odds_master)
    - live: Shows props for in-progress games (from v_live_player_prop_odds_latest)
      - Includes current_stat (player's current stat for the market)
      - Includes game_score (current home/away scores)
    """
    client = get_bq_client()

    # Select query based on mode
    query = PRE_LIVE_QUERY if mode == "pre-live" else LIVE_QUERY

    job = client.query(query)
    rows = list(job.result())

    ladders = process_rows_to_ladders(rows, min_vendors, market, limit, min_odds)

    return {
        "mode": mode,
        "count": len(ladders),
        "ladders": ladders,
    }
