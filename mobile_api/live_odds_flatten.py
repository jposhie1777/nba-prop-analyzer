# live_odds_flatten.py

from google.cloud import bigquery
from datetime import datetime, timezone
import os

PROJECT_ID = os.getenv("GCP_PROJECT", "graphite-flare-477419-h7")

client = bigquery.Client(project=PROJECT_ID)

# ======================================================
# Player prop flatten
# ======================================================

PLAYER_PROP_FLATTEN_SQL = """
INSERT INTO `graphite-flare-477419-h7.nba_live.live_player_prop_odds_flat`
SELECT
  TIMESTAMP(snapshot_ts) AS snapshot_ts,
  game_id,

  CAST(JSON_VALUE(m, '$.player_id') AS INT64) AS player_id,

  CASE JSON_VALUE(m, '$.market')
    WHEN 'points' THEN 'PTS'
    WHEN 'assists' THEN 'AST'
    WHEN 'rebounds' THEN 'REB'
    WHEN 'three_pointers_made' THEN '3PM'
  END AS market,

  CAST(JSON_VALUE(m, '$.line') AS FLOAT64) AS line,
  JSON_VALUE(m, '$.book') AS book,

  CAST(JSON_VALUE(m, '$.odds.over') AS INT64) AS over_odds,
  CAST(JSON_VALUE(m, '$.odds.under') AS INT64) AS under_odds
FROM `graphite-flare-477419-h7.nba_live.live_player_prop_odds_raw`,
UNNEST(JSON_QUERY_ARRAY(payload, '$.markets')) AS m
WHERE JSON_VALUE(m, '$.market') IS NOT NULL
"""

# ======================================================
# Game odds flatten
# ======================================================

GAME_ODDS_FLATTEN_SQL = """
INSERT INTO `graphite-flare-477419-h7.nba_live.live_game_odds_flat`
SELECT
  TIMESTAMP(snapshot_ts) AS snapshot_ts,
  game_id,
  JSON_VALUE(payload, '$.book') AS book,

  CAST(JSON_VALUE(payload, '$.spread') AS FLOAT64) AS spread,
  CAST(JSON_VALUE(payload, '$.spread_odds') AS INT64) AS spread_odds,

  CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
  CAST(JSON_VALUE(payload, '$.over_odds') AS INT64) AS over_odds,
  CAST(JSON_VALUE(payload, '$.under_odds') AS INT64) AS under_odds
FROM `graphite-flare-477419-h7.nba_live.live_game_odds_raw`
"""

# ======================================================
# Runner
# ======================================================

def run_live_odds_flatten():
    """
    Flatten all live odds RAW tables into flat tables.
    Safe to run every 30s.
    """

    print("🔧 Flattening live player prop odds")
    client.query(PLAYER_PROP_FLATTEN_SQL).result()

    print("🔧 Flattening live game odds")
    client.query(GAME_ODDS_FLATTEN_SQL).result()

    print("✅ Live odds flatten complete")