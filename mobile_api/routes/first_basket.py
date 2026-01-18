from fastapi import APIRouter
from bq import get_bq_client

router = APIRouter(prefix="/first-basket", tags=["First Basket"])

@router.get("/matchups")
def get_first_basket_matchups():
    client = get_bq_client()

    query = """
    SELECT *
    FROM nba_goat_data.v_first_basket_matchups
    ORDER BY game_id, rank_within_team
    """

    rows = client.query(query).result()
    games = {}

    for r in rows:
        gid = r.game_id

        if gid not in games:
            games[gid] = {
                "gameId": gid,
                "homeTeam": r.home_team_abbr,
                "awayTeam": r.away_team_abbr,
                "homeTipWinPct": round(r.home_tip_win_pct),
                "awayTipWinPct": round(r.away_tip_win_pct),
                "rows": [],
            }

        # hide rows where both sides are empty
        if r.player is None:
            continue

        side_obj = {
            "player": r.player,
            "firstBasketPct": r.first_basket_probability,
            "firstShotShare": r.first_shot_share,
            "playerFirstBasketCount": r.player_first_basket_count,
            "playerTeamFirstBasketCount": r.player_team_first_basket_count,
        }

        # determine side by team_abbr
        if r.team_abbr == games[gid]["homeTeam"]:
            home = side_obj
            away = None
        else:
            home = None
            away = side_obj

        games[gid]["rows"].append({
            "rank": r.rank_within_team,
            "home": home,
            "away": away,
        })

    return list(games.values())
