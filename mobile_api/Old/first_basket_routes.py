# first_basket_routes.py
from fastapi import APIRouter
from google.cloud import bigquery
from typing import List, Dict, Any
import os

# ======================================================
# Router
# ======================================================

router = APIRouter(
    prefix="/first-basket",
    tags=["first-basket"],
)

# ======================================================
# BigQuery
# ======================================================

PROJECT_ID = (
    os.environ.get("GCP_PROJECT")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
)

bq = bigquery.Client(project=PROJECT_ID)

# ======================================================
# Query
# ======================================================

FIRST_BASKET_QUERY = """
SELECT
  *
FROM `nba_goat_data.first_basket_projection_enriched_v1`
ORDER BY
  game_date,
  game_id,
  rank_within_game
"""

# ======================================================
# Endpoint
# ======================================================

@router.get("")
def get_first_basket_projections() -> List[Dict[str, Any]]:
    """
    Returns first basket projections (player-level).

    One row per player per game.
    Grouping is handled client-side.
    """

    rows = bq.query(FIRST_BASKET_QUERY).result()

    results: List[Dict[str, Any]] = []

    for r in rows:
        results.append(
            {
                "game_id": r.game_id,
                "game_date": str(r.game_date),

                "team_id": r.team_id,
                "team_abbr": r.team_abbr,

                "player_id": r.player_id,
                "player": r.player,

                "starter_pct": r.starter_pct,
                "rotation_tier": r.rotation_tier,
                "first_shot_share": r.first_shot_share,
                "team_first_score_rate": r.team_first_score_rate,

                "pts_per_min": r.pts_per_min,
                "fga_per_min": r.fga_per_min,
                "usage_l10": r.usage_l10,

                "player_first_basket_count": r.player_first_basket_count,
                "player_team_first_basket_count": r.player_team_first_basket_count,
                "team_first_basket_count": r.team_first_basket_count,

                "raw_projection_score": r.raw_projection_score,
                "first_basket_probability": r.first_basket_probability,

                "rank_within_team": r.rank_within_team,
                "rank_within_game": r.rank_within_game,

                "team_tip_win_pct": r.team_tip_win_pct,

                "projected_at": (
                    r.projected_at.isoformat()
                    if r.projected_at
                    else None
                ),
                "model_version": r.model_version,
            }
        )

    return results