from fastapi import APIRouter
from google.cloud import bigquery
from typing import Dict, Any, List
import os

router = APIRouter(prefix="/live", tags=["live"])

PROJECT_ID = os.getenv("GCP_PROJECT", "graphite-flare-477419-h7")

bq = bigquery.Client(project=PROJECT_ID)

LIVE_GAMES_QUERY = """
WITH latest_snapshot AS (
  SELECT
    snapshot_ts,
    payload_json
  FROM `graphite-flare-477419-h7.nba_live.live_games_raw_snapshots`
  WHERE ARRAY_LENGTH(JSON_QUERY_ARRAY(payload_json, '$.games')) > 0
  QUALIFY ROW_NUMBER() OVER (ORDER BY snapshot_ts DESC) = 1
),

live_games AS (
  SELECT
    CAST(JSON_VALUE(g, '$.game_id') AS INT64) AS game_id,
    JSON_VALUE(g, '$.home_team') AS home_team,
    JSON_VALUE(g, '$.away_team') AS away_team,
    CAST(JSON_VALUE(g, '$.home_score') AS INT64) AS home_score,
    CAST(JSON_VALUE(g, '$.away_score') AS INT64) AS away_score,
    JSON_VALUE(g, '$.period') AS period,
    JSON_VALUE(g, '$.clock') AS clock
  FROM latest_snapshot,
  UNNEST(JSON_QUERY_ARRAY(payload_json, '$.games')) AS g
)

SELECT
  lg.game_id,
  lg.home_team,
  lg.away_team,
  lg.home_score,
  lg.away_score,
  lg.period,
  lg.clock,

  g.start_time_utc,
  DATETIME(g.start_time_utc, "America/New_York") AS start_time_et,

  CASE
    WHEN lg.period IS NOT NULL THEN "LIVE"
    WHEN g.start_time_utc IS NOT NULL
         AND CURRENT_TIMESTAMP() >= g.start_time_utc THEN "LIVE"
    ELSE "UPCOMING"
  END AS state
FROM live_games lg
LEFT JOIN `graphite-flare-477419-h7.nba_goat_data.games` g
  ON g.game_id = lg.game_id
ORDER BY start_time_utc
"""
@router.get("/games")
def get_live_games() -> Dict[str, Any]:
    job = bq.query(LIVE_GAMES_QUERY)
    rows = list(job.result())

    if not rows:
        return {"count": 0, "games": []}

    for r in rows:
        games.append({
            "game_id": r.game_id,
            "home_team": r.home_team,
            "away_team": r.away_team,
            "home_score": r.home_score,
            "away_score": r.away_score,
            "period": r.period,
            "clock": r.clock,
            "start_time_et": (
                r.start_time_et.isoformat()
                if r.start_time_et else None
            ),
            "state": r.state,
        })

    return {
        "count": len(games),
        "games": games,
    }
