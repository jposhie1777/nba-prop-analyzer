from fastapi import APIRouter
from datetime import datetime
from google.cloud import bigquery

from bq import get_bq_client
from services.wowy import get_wowy_for_injured_players

router = APIRouter(prefix="/injuries/wowy/cache", tags=["WOWY Cache"])

@router.post("/refresh")
def refresh_wowy_cache(season: int | None = None):
    client = get_bq_client()
    now = datetime.utcnow()

    results = get_wowy_for_injured_players(
        season=season,
        only_today_games=False,
    )

    rows = []

    for r in results:
        ip = r["injured_player"]
        ti = r["team_impact"]

        for tm in r["teammates"]:
            for stat in ["pts", "reb", "ast", "fg3m"]:
                rows.append({
                    "season": season,
                    "injured_player_id": ip["player_id"],
                    "injured_player_name": ip["player_name"],
                    "team_abbr": ip["team"],
                    "injury_status": ip["status"],

                    "stat": stat,

                    "teammate_player_id": tm["player_id"],
                    "teammate_name": tm["teammate_name"],

                    "games_with": tm["games_with"],
                    "games_without": tm["games_without"],

                    "stat_with": tm[f"{stat}_with"],
                    "stat_without": tm[f"{stat}_without"],
                    "stat_diff": tm[f"{stat}_diff"],

                    "team_ppg_diff": ti["team_ppg_diff"],
                    "updated_at": now,
                })

    table_id = "nba_live.wowy_injured_cache"

    # 🔁 Replace season partition
    client.query(f"""
      DELETE FROM `{table_id}`
      WHERE season = {season}
    """).result()

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(errors)

    return {
        "status": "ok",
        "rows_written": len(rows),
        "season": season,
    }
