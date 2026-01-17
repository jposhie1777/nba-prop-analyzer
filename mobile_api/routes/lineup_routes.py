# mobile_api/routes/lineup_routes.py
from fastapi import APIRouter
from bq import get_bq_client

router = APIRouter(
    prefix="/lineups",
    tags=["lineups"],
)

bq = get_bq_client()

# ------------------------------------------------------
# 1️⃣ RAW DATA CHECK (no views, no filters)
# ------------------------------------------------------
@router.get("/debug/raw")
def debug_raw():
    rows = bq.query("""
        SELECT
          game_id,
          game_date,
          state,
          home_team_abbr,
          away_team_abbr
        FROM `graphite-flare-477419-h7.nba_live.live_games`
        ORDER BY game_id
        LIMIT 20
    """).result()
    return [dict(r) for r in rows]


# ------------------------------------------------------
# 2️⃣ DATE / TIMEZONE CHECK
# ------------------------------------------------------
@router.get("/debug/date")
def debug_date():
    rows = bq.query("""
        SELECT
          CURRENT_DATE() AS current_date,
          CURRENT_TIMESTAMP() AS current_ts
    """).result()
    return [dict(r) for r in rows]


# ------------------------------------------------------
# 3️⃣ ORIGINAL ENDPOINT (unchanged, for comparison)
# ------------------------------------------------------
@router.get("/tonight")
def get_tonight_lineups():
    rows = bq.query("""
        SELECT *
        FROM `graphite-flare-477419-h7.nba_goat_data.v_games_tonight_lineups`
        ORDER BY game_time
    """).result()
    return [dict(r) for r in rows]