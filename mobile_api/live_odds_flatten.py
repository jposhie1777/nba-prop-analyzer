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
    TIMESTAMP(snapshot_ts) AS snapshot_ts,
    game_id,

    CAST(JSON_VALUE(m, '$.player_id') AS INT64) AS player_id,

    -- ✅ keep raw market string (recommended)
    JSON_VALUE(m, '$.market') AS market,

    CAST(JSON_VALUE(m, '$.line') AS FLOAT64) AS line,
    JSON_VALUE(m, '$.book') AS book,

    CAST(JSON_VALUE(m, '$.odds.over') AS INT64)  AS over_odds,
    CAST(JSON_VALUE(m, '$.odds.under') AS INT64) AS under_odds
  FROM `graphite-flare-477419-h7.nba_live.live_player_prop_odds_raw`,
  UNNEST(
    JSON_QUERY_ARRAY(PARSE_JSON(payload), '$.markets')
  ) AS m
) S
ON
  T.game_id    = S.game_id
  AND T.player_id = S.player_id
  AND T.market = S.market
  AND T.line   = S.line
  AND T.book   = S.book

WHEN MATCHED THEN
  UPDATE SET
    snapshot_ts = S.snapshot_ts,
    over_odds   = S.over_odds,
    under_odds  = S.under_odds

WHEN NOT MATCHED THEN
  INSERT (
    snapshot_ts,
    game_id,
    player_id,
    market,
    line,
    book,
    over_odds,
    under_odds
  )
  VALUES (
    S.snapshot_ts,
    S.game_id,
    S.player_id,
    S.market,
    S.line,
    S.book,
    S.over_odds,
    S.under_odds
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
  SELECT
    TIMESTAMP(snapshot_ts) AS snapshot_ts,
    game_id,
  
    JSON_VALUE(PARSE_JSON(payload), '$.book') AS book,
  
    CAST(JSON_VALUE(PARSE_JSON(payload), '$.spread') AS FLOAT64) AS spread,
    CAST(JSON_VALUE(PARSE_JSON(payload), '$.spread_odds') AS INT64) AS spread_odds,
  
    CAST(JSON_VALUE(PARSE_JSON(payload), '$.total') AS FLOAT64) AS total,
    CAST(JSON_VALUE(PARSE_JSON(payload), '$.over_odds') AS INT64) AS over_odds,
    CAST(JSON_VALUE(PARSE_JSON(payload), '$.under_odds') AS INT64) AS under_odds
  FROM `graphite-flare-477419-h7.nba_live.live_game_odds_raw`
) S
ON
  T.game_id = S.game_id
  AND T.book = S.book

WHEN MATCHED THEN
  UPDATE SET
    snapshot_ts = S.snapshot_ts,
    spread      = S.spread,
    spread_odds = S.spread_odds,
    total       = S.total,
    over_odds   = S.over_odds,
    under_odds  = S.under_odds

WHEN NOT MATCHED THEN
  INSERT (
    snapshot_ts,
    game_id,
    book,
    spread,
    spread_odds,
    total,
    over_odds,
    under_odds
  )
  VALUES (
    S.snapshot_ts,
    S.game_id,
    S.book,
    S.spread,
    S.spread_odds,
    S.total,
    S.over_odds,
    S.under_odds
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