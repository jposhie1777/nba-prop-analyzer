# mobile_api/routes/first_basket.py

from fastapi import APIRouter
from collections import defaultdict
from bq import get_bq_client

router = APIRouter(prefix="/first-basket", tags=["First Basket"])

@router.get("/matchups")
def get_first_basket_matchups():
    client = get_bq_client()

    query = """
    SELECT *
    FROM nba_goat_data.v_first_basket_matchup_pairs
    ORDER BY game_id, rank_within_team
    """

    rows = client.query(query).result()

    games = {}

    for r in rows:
        game_id = r.game_id

        if game_id not in games:
            games[game_id] = {
                "gameId": game_id,
                "homeTeam": None,
                "awayTeam": None,
                "rows": []
            }

        game = games[game_id]

        # infer teams once
        if game["homeTeam"] is None:
            game["homeTeam"] = r.home_player is not None and r.home_player or None
        if game["awayTeam"] is None:
            game["awayTeam"] = r.away_player is not None and r.away_player or None

        # hide rows where both sides are empty
        if r.home_player is None and r.away_player is None:
            continue

        game["rows"].append({
            "rank": r.rank_within_team,
            "home": None if r.home_player is None else {
                "player": r.home_player,
                "firstBasketPct": r.home_first_basket_pct,
                "firstShotShare": r.home_first_shot_share,
                "playerFirstBasketCount": r.home_player_first_basket_count,
                "playerTeamFirstBasketCount": r.home_player_team_first_basket_count,
            },
            "away": None if r.away_player is None else {
                "player": r.away_player,
                "firstBasketPct": r.away_first_basket_pct,
                "firstShotShare": r.away_first_shot_share,
                "playerFirstBasketCount": r.away_player_first_basket_count,
                "playerTeamFirstBasketCount": r.away_player_team_first_basket_count,
            }
        })

    return list(games.values())
