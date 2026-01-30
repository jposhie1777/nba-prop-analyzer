from fastapi import APIRouter
from typing import Dict, Any
from google.cloud import bigquery

from bq import get_bq_client  # 👈 USE YOUR EXISTING HELPER

router = APIRouter(prefix="/players", tags=["players"])

DATASET = "nba_goat_data"
TABLE = "v_player_season_mega"
ROSTERS_VIEW = "v_team_rosters_with_player_id"


@router.get("/season-mega")
def get_player_season_mega(limit: int = 500) -> Dict[str, Any]:
    """
    Raw read from v_player_season_mega.
    Frontend handles deduping for now.
    """

    client = get_bq_client()

    query = f"""
    SELECT *
    FROM `{DATASET}.{TABLE}`
    LIMIT @limit
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("limit", "INT64", limit)
            ]
        ),
    )

    rows = [dict(r) for r in job]

    return {
        "rows": rows,
        "count": len(rows),
        "source": f"{DATASET}.{TABLE}",
    }


@router.get("/positions")
def get_player_positions() -> Dict[str, Any]:
    """
    Player position lookup keyed by player_id.
    """

    client = get_bq_client()

    query = f"""
    SELECT
      player_id,
      position,
      team_abbr
    FROM `{DATASET}.{ROSTERS_VIEW}`
    WHERE player_id IS NOT NULL
      AND position IS NOT NULL
      AND team_abbr IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY player_id
      ORDER BY depth ASC, role ASC
    ) = 1
    """

    job = client.query(query)

    rows = [dict(r) for r in job]

    return {
        "rows": rows,
        "count": len(rows),
        "source": f"{DATASET}.{ROSTERS_VIEW}",
    }
