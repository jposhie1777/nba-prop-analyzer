# /routes/teams.py
from fastapi import APIRouter
from bq import get_bq_client

router = APIRouter(prefix="/teams", tags=["Teams"])

@router.get("/season-stats")
def get_team_season_stats():
    client = get_bq_client()
    query = """
    SELECT *
    FROM nba_goat_data.v_team_season_general_base
    WHERE season = 2025
      AND season_type = 'regular'
    """
    return [dict(r) for r in client.query(query).result()]