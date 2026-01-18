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
        gid = r.game_id

        if gid not in games:
            games[gid] = {
                "gameId": gid,
                # ✅ FLAT TEAM FIELDS (what frontend expects)
                "homeTeam": r.home_team_abbr,
                "awayTeam": r.away_team_abbr,
                "homeTipWinPct": 0,
                "awayTipWinPct": 0,
                "rows": [],
                # internal counters (not returned)
                "_homeWins": 0,
                "_awayWins": 0,
            }

        game = games[gid]

        # Skip fully empty rows
        if r.home_player is None and r.away_player is None:
            continue

        home_side = None
        away_side = None

        if r.home_player is not None:
            home_side = {
                "player": r.home_player,
                "firstBasketPct": r.home_first_basket_pct,
                "firstShotShare": r.home_first_shot_share,
                "playerFirstBasketCount": r.home_player_first_basket_count,
                "playerTeamFirstBasketCount": r.home_player_team_first_basket_count,
            }

        if r.away_player is not None:
            away_side = {
                "player": r.away_player,
                "firstBasketPct": r.away_first_basket_pct,
                "firstShotShare": r.away_first_shot_share,
                "playerFirstBasketCount": r.away_player_first_basket_count,
                "playerTeamFirstBasketCount": r.away_player_team_first_basket_count,
            }

        # ✅ Tip winner logic (simple, deterministic)
        if home_side and away_side:
            if r.home_first_basket_pct > r.away_first_basket_pct:
                game["_homeWins"] += 1
            elif r.away_first_basket_pct > r.home_first_basket_pct:
                game["_awayWins"] += 1

        game["rows"].append({
            "rank": r.rank_within_team,
            "home": home_side,
            "away": away_side,
        })

    # ✅ Finalize tip win %
    results = []
    for game in games.values():
        total = game["_homeWins"] + game["_awayWins"] or 1

        game["homeTipWinPct"] = round((game["_homeWins"] / total) * 100)
        game["awayTipWinPct"] = round((game["_awayWins"] / total) * 100)

        # remove internal fields
        del game["_homeWins"]
        del game["_awayWins"]

        results.append(game)

    return results
