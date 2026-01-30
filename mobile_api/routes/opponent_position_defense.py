# /routes/opponent_position_defense.py
from fastapi import APIRouter
from bq import get_bq_client

router = APIRouter(
    prefix="/opponent-position-defense",
    tags=["Opponent Position Defense"],
)


@router.get("")
def get_opponent_position_defense():
    client = get_bq_client()

    query = """
    SELECT
      *
    FROM nba_goat_data.opponent_position_defense
    ORDER BY opponent_team_abbr, player_position
    """

    return [dict(r) for r in client.query(query).result()]
