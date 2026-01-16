from fastapi import APIRouter
from bq import get_bq_client

router = APIRouter(
    prefix="/lineups",
    tags=["lineups"],
)

bq = get_bq_client()

@router.get("/tonight")
def get_tonight_lineups():
    query = """
    SELECT *
    FROM `graphite-flare-477419-h7.nba_goat_data.v_games_tonight_lineups`
    ORDER BY game_time
    """
    rows = bq.query(query).result()
    return [dict(r) for r in rows]