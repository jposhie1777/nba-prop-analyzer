# live_odds_routes.py
from fastapi import APIRouter, Query
from google.cloud import bigquery
from typing import Dict, Any, List
import os

# ======================================================
# Router
# ======================================================

router = APIRouter(
    prefix="/live/odds",
    tags=["live-odds"],
)

PROJECT_ID = os.getenv("GCP_PROJECT", "graphite-flare-477419-h7")
bq = bigquery.Client(project=PROJECT_ID)


# ======================================================
# Live PLAYER PROPS query
# ======================================================

PLAYER_PROPS_QUERY = """
SELECT
  game_id,
  player_id,
  market,
  market_type,
  line,
  book,
  over_odds,
  under_odds,
  milestone_odds,
  snapshot_ts
FROM `graphite-flare-477419-h7.nba_live.v_live_player_prop_odds_latest`
WHERE game_id = @game_id
ORDER BY market, market_type, player_id, line, book
"""

@router.get("/player-props")
def get_live_player_props(
    game_id: int = Query(..., description="BallDontLie game_id"),
) -> Dict[str, Any]:
    """
    Returns LIVE player prop odds (PTS / AST / REB / 3PM)
    for DraftKings + FanDuel only.

    Source: live_player_prop_odds_latest
    """

    job = bq.query(
        PLAYER_PROPS_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "game_id",
                    "INT64",
                    game_id,
                )
            ]
        ),
    )

    rows = list(job.result())

    props: List[Dict[str, Any]] = []
    last_updated = None

    for r in rows:
        last_updated = r.snapshot_ts
        props.append(
            {
                "player_id": r.player_id,
                "market": r.market,
                "market_type": r.market_type,
                "line": r.line,
                "book": r.book,
                "over_odds": r.over_odds,
                "under_odds": r.under_odds,
                "milestone_odds": r.milestone_odds,
            }
        )



    return {
        "game_id": game_id,
        "updated_at": (
            last_updated.isoformat()
            if last_updated else None
        ),
        "count": len(props),
        "props": props,
    }

# ======================================================
# Live GAME ODDS query
# ======================================================

GAME_ODDS_QUERY = """
SELECT
  game_id,
  book,

  spread_home,
  spread_away,
  spread_home_odds,
  spread_away_odds,

  total,
  over_odds,
  under_odds,

  moneyline_home_odds,
  moneyline_away_odds,

  snapshot_ts
FROM `graphite-flare-477419-h7.nba_live.live_game_odds_flat`
WHERE game_id = @game_id
ORDER BY book
"""

@router.get("/games")
def get_live_game_odds(
    game_id: int = Query(..., description="BallDontLie game_id"),
) -> Dict[str, Any]:
    """
    Returns LIVE game betting odds
    (spread + total) for DraftKings + FanDuel.

    Source: live_game_odds_flat
    """

    job = bq.query(
        GAME_ODDS_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "game_id",
                    "INT64",
                    game_id,
                )
            ]
        ),
    )

    rows = list(job.result())

    odds: List[Dict[str, Any]] = []
    last_updated = None

    for r in rows:
        last_updated = r.snapshot_ts
        odds.append(
            {
                "book": r.book,
        
                "spread_home": r.spread_home,
                "spread_away": r.spread_away,
                "spread_home_odds": r.spread_home_odds,
                "spread_away_odds": r.spread_away_odds,
        
                "total": r.total,
                "over": r.over_odds,
                "under": r.under_odds,
        
                "moneyline_home_odds": r.moneyline_home_odds,
                "moneyline_away_odds": r.moneyline_away_odds,
            }
        )

    return {
        "game_id": game_id,
        "updated_at": (
            last_updated.isoformat()
            if last_updated else None
        ),
        "count": len(odds),
        "odds": odds,
    }