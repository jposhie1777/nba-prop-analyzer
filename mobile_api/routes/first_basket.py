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

    # =========================
    # 1️⃣ BUILD rowsByRank
    # =========================
    for r in rows:
        gid = r.game_id

        if gid not in games:
            games[gid] = {
                "gameId": gid,
                "homeTeam": r.home_team_abbr,
                "awayTeam": r.away_team_abbr,

                # 🔹 RAW probabilities (0–1)
                "homeTipWinPct": r.home_tip_win_pct,
                "awayTipWinPct": r.away_tip_win_pct,

                "rowsByRank": {},
            }

        if r.player is None:
            continue

        side_obj = {
            "player": r.player,

            # 🔹 RAW probabilities (0–1)
            "firstBasketPct": r.first_basket_probability,
            "firstShotShare": r.first_shot_share,

            # 🔹 counts
            "playerFirstBasketCount": r.player_first_basket_count,
            "playerTeamFirstBasketCount": r.team_first_basket_count,
        }

        rank = r.rank_within_team

        row = games[gid]["rowsByRank"].setdefault(
            rank,
            {"rank": rank, "home": None, "away": None}
        )

        if r.team_abbr == games[gid]["homeTeam"]:
            row["home"] = side_obj
        else:
            row["away"] = side_obj

    # =========================
    # 2️⃣ FINALIZE rows
    # =========================
    for g in games.values():
        g["rows"] = sorted(
            g["rowsByRank"].values(),
            key=lambda r: r["rank"]
        )
        del g["rowsByRank"]

    return list(games.values())
