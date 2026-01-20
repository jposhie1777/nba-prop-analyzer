# live_odds_flatten.py

from google.cloud import bigquery
import os

PROJECT_ID = os.getenv("GCP_PROJECT", "graphite-flare-477419-h7")

client = bigquery.Client(project=PROJECT_ID)

# ======================================================
# 🔁 IDPOTENT FLATTEN: LIVE PLAYER PROP ODDS
# ======================================================
#
# One row per:
#   game_id × player_id × market × line × book
#
# Odds are UPDATED in place when they change
# ======================================================
PLAYER_PROP_FLATTEN_SQL = """
MERGE `graphite-flare-477419-h7.nba_live.live_player_prop_odds_flat` T
USING (
  SELECT
    game_id,
    player_id,
    market,
    market_type,
    line,
    book,

    ARRAY_AGG(
      STRUCT(
        snapshot_ts AS snapshot_ts,
        over_odds AS over_odds,
        under_odds AS under_odds,
        milestone_odds AS milestone_odds
      )
      ORDER BY snapshot_ts DESC
      LIMIT 1
    )[OFFSET(0)].*
  FROM (
    SELECT
      TIMESTAMP(snapshot_ts) AS snapshot_ts,
      game_id,
    
      CAST(JSON_VALUE(m, '$.player_id') AS INT64) AS player_id,
    
      CASE LOWER(JSON_VALUE(m, '$.market'))
        WHEN 'points' THEN 'pts'
        WHEN 'assists' THEN 'ast'
        WHEN 'rebounds' THEN 'reb'
        WHEN 'three_pointers_made' THEN '3pm'
        ELSE JSON_VALUE(m, '$.market')
      END AS market,
    
      CASE
        WHEN JSON_VALUE(m, '$.market_type') IS NOT NULL
          THEN JSON_VALUE(m, '$.market_type')
        WHEN JSON_VALUE(m, '$.odds.yes') IS NOT NULL
          THEN 'milestone'
        WHEN JSON_VALUE(m, '$.odds.over') IS NOT NULL
          OR JSON_VALUE(m, '$.odds.under') IS NOT NULL
          THEN 'over_under'
        ELSE 'unknown'
      END AS market_type,
    
      CAST(JSON_VALUE(m, '$.line') AS FLOAT64) AS line,
      JSON_VALUE(m, '$.book') AS book,
    
      SAFE_CAST(JSON_VALUE(m, '$.odds.over') AS INT64) AS over_odds,
      SAFE_CAST(JSON_VALUE(m, '$.odds.under') AS INT64) AS under_odds,
      SAFE_CAST(JSON_VALUE(m, '$.odds.yes') AS INT64) AS milestone_odds
    
    FROM `graphite-flare-477419-h7.nba_live.live_player_prop_odds_raw`,
    UNNEST(JSON_QUERY_ARRAY(payload, '$.items')) AS m
    
    WHERE JSON_VALUE(m, '$.market') IN (
      'points',
      'assists',
      'rebounds',
      'three_pointers_made'
    )
  GROUP BY 1,2,3,4,5,6
) S
ON
  T.game_id     = S.game_id
  AND T.player_id  = S.player_id
  AND T.market  = S.market
  AND T.market_type = S.market_type
  AND T.line    = S.line
  AND T.book    = S.book

WHEN MATCHED THEN
  UPDATE SET
    snapshot_ts    = S.snapshot_ts,
    over_odds      = S.over_odds,
    under_odds     = S.under_odds,
    milestone_odds = S.milestone_odds

WHEN NOT MATCHED THEN
  INSERT (
    snapshot_ts,
    game_id,
    player_id,
    market,
    market_type,
    line,
    book,
    over_odds,
    under_odds,
    milestone_odds
  )
  VALUES (
    S.snapshot_ts,
    S.game_id,
    S.player_id,
    S.market,
    S.market_type,
    S.line,
    S.book,
    S.over_odds,
    S.under_odds,
    S.milestone_odds
  );
"""
# ======================================================
# 🔁 IDPOTENT FLATTEN: LIVE GAME ODDS
# ======================================================
#
# One row per:
#   game_id × book
#
# ======================================================

GAME_ODDS_FLATTEN_SQL = """
MERGE `graphite-flare-477419-h7.nba_live.live_game_odds_flat` T
USING (
  SELECT *
  FROM (
    SELECT
      TIMESTAMP(snapshot_ts) AS snapshot_ts,
      game_id,
      JSON_VALUE(payload, '$.book') AS book,

      CAST(JSON_VALUE(payload, '$.spread_home') AS FLOAT64) AS spread_home,
      CAST(JSON_VALUE(payload, '$.spread_away') AS FLOAT64) AS spread_away,

      CAST(JSON_VALUE(payload, '$.spread_home_odds') AS INT64) AS spread_home_odds,
      CAST(JSON_VALUE(payload, '$.spread_away_odds') AS INT64) AS spread_away_odds,

      CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
      CAST(JSON_VALUE(payload, '$.total_over_odds') AS INT64) AS over_odds,
      CAST(JSON_VALUE(payload, '$.total_under_odds') AS INT64) AS under_odds,

      CAST(JSON_VALUE(payload, '$.moneyline_home_odds') AS INT64) AS moneyline_home_odds,
      CAST(JSON_VALUE(payload, '$.moneyline_away_odds') AS INT64) AS moneyline_away_odds,

      ROW_NUMBER() OVER (
        PARTITION BY game_id, JSON_VALUE(payload, '$.book')
        ORDER BY TIMESTAMP(snapshot_ts) DESC
      ) AS rn
    FROM `graphite-flare-477419-h7.nba_live.live_game_odds_raw`
  )
  WHERE rn = 1
) S
ON
  T.game_id = S.game_id
  AND T.book = S.book

WHEN MATCHED THEN
  UPDATE SET
    snapshot_ts            = S.snapshot_ts,
    spread_home            = S.spread_home,
    spread_away            = S.spread_away,
    spread_home_odds       = S.spread_home_odds,
    spread_away_odds       = S.spread_away_odds,
    total                  = S.total,
    over_odds              = S.over_odds,
    under_odds             = S.under_odds,
    moneyline_home_odds    = S.moneyline_home_odds,
    moneyline_away_odds    = S.moneyline_away_odds

WHEN NOT MATCHED THEN
  INSERT (
    snapshot_ts,
    game_id,
    book,
    spread_home,
    spread_away,
    spread_home_odds,
    spread_away_odds,
    total,
    over_odds,
    under_odds,
    moneyline_home_odds,
    moneyline_away_odds
  )
  VALUES (
    S.snapshot_ts,
    S.game_id,
    S.book,
    S.spread_home,
    S.spread_away,
    S.spread_home_odds,
    S.spread_away_odds,
    S.total,
    S.over_odds,
    S.under_odds,
    S.moneyline_home_odds,
    S.moneyline_away_odds
  );
"""

# ======================================================
# Runner
# ======================================================

def run_live_odds_flatten():
    """
    Idempotently flatten all live odds RAW tables
    into stateful FLAT tables.

    Safe to run every 30s.
    """

    print("🔁 Flattening LIVE PLAYER PROP odds (idempotent)")
    client.query(PLAYER_PROP_FLATTEN_SQL).result()

    print("🔁 Flattening LIVE GAME odds (idempotent)")
    client.query(GAME_ODDS_FLATTEN_SQL).result()

    print("✅ Live odds flatten complete")
    
if __name__ == "__main__":
    run_live_odds_flatten()