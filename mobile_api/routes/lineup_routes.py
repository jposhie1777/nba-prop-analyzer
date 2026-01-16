from fastapi import APIRouter
from google.cloud import bigquery
import os

router = APIRouter(
    prefix="/lineups",
    tags=["lineups"],
)

PROJECT_ID = os.getenv("GCP_PROJECT")
bq = bigquery.Client(project=PROJECT_ID)

@router.get("/tonight")
def get_tonight_lineups():
    query = """
    SELECT *
    FROM `nba_goat_data.v_games_tonight_lineups`
    ORDER BY game_time
    """
    rows = bq.query(query).result()
    return [dict(r) for r in rows]