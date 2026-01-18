# /routes/teams.py
from fastapi import APIRouter
from bq import get_bq_client

router = APIRouter(prefix="/teams", tags=["Teams"])


# ======================================================
# DATA — team season stats
# ======================================================
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


# ======================================================
# SCHEMA — column metadata (auto-table support)
# ======================================================
@router.get("/season-stats/schema")
def get_team_season_schema():
    client = get_bq_client()

    table = client.get_table(
        "nba_goat_data.v_team_season_general_base"
    )

    return [
        {
            "name": field.name,
            "type": field.field_type,
        }
        for field in table.schema
    ]