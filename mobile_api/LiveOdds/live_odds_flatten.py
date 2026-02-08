# live_odds_flatten.py
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from google.cloud import bigquery

# =============================================================================
# CONFIG
# =============================================================================
PROJECT_ID = os.getenv("GCP_PROJECT", "graphite-flare-477419-h7")
DATASET = os.getenv("PULSE_DATASET", "nba_live")

# State table (Option B)
STATE_TABLE = os.getenv(
    "INGEST_STATE_TABLE",
    f"{PROJECT_ID}.{DATASET}.ingest_state",
)

# RAW tables
RAW_PLAYER_PROP = f"{PROJECT_ID}.{DATASET}.live_player_prop_odds_raw"
RAW_GAME_ODDS = f"{PROJECT_ID}.{DATASET}.live_game_odds_raw"

# STAGE tables (append-only, partitioned by snapshot_ts ideally)
STAGE_PLAYER_PROP = f"{PROJECT_ID}.{DATASET}.live_player_prop_odds_stage"
STAGE_GAME_ODDS = f"{PROJECT_ID}.{DATASET}.live_game_odds_stage"

# LATEST tables (one row per key)
LATEST_PLAYER_PROP = f"{PROJECT_ID}.{DATASET}.live_player_prop_odds_latest"
LATEST_GAME_ODDS = f"{PROJECT_ID}.{DATASET}.live_game_odds_latest"

# FLAT tables (consumer tables for app; one row per key)
FLAT_PLAYER_PROP = f"{PROJECT_ID}.{DATASET}.live_player_prop_odds_flat"
FLAT_GAME_ODDS = f"{PROJECT_ID}.{DATASET}.live_game_odds_flat"

# Runtime safety
LEASE_SECONDS = int(os.getenv("INGEST_LEASE_SECONDS", "180"))  # 3 min lock
MAX_BYTES_PER_QUERY = int(os.getenv("MAX_BYTES_PER_QUERY", str(2_000_000_000)))  # 2GB
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
ENABLE_LIVE_ODDS_FLATTEN = os.getenv("ENABLE_LIVE_ODDS_FLATTEN", "true").lower() == "true"

_client: bigquery.Client | None = None


def _get_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT_ID)
    return _client


# =============================================================================
# SQL: STATE (lock + watermark)
# =============================================================================

ENSURE_STATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{STATE_TABLE}` (
  state_key STRING NOT NULL,
  last_processed_ts TIMESTAMP,
  lock_owner STRING,
  lock_until TIMESTAMP,
  updated_at TIMESTAMP
)
"""

ACQUIRE_LOCK_SQL = f"""
MERGE `{STATE_TABLE}` T
USING (
  SELECT
    @state_key AS state_key,
    @lock_owner AS lock_owner,
    TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @lease_seconds SECOND) AS lock_until
) S
ON T.state_key = S.state_key
WHEN MATCHED AND (T.lock_until IS NULL OR T.lock_until < CURRENT_TIMESTAMP()) THEN
  UPDATE SET
    lock_owner = S.lock_owner,
    lock_until = S.lock_until,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (state_key, last_processed_ts, lock_owner, lock_until, updated_at)
  VALUES (S.state_key, TIMESTAMP("1970-01-01"), S.lock_owner, S.lock_until, CURRENT_TIMESTAMP())
"""

GET_STATE_SQL = f"""
SELECT
  state_key,
  last_processed_ts,
  lock_owner,
  lock_until
FROM `{STATE_TABLE}`
WHERE state_key = @state_key
"""

RELEASE_LOCK_AND_ADVANCE_SQL = f"""
UPDATE `{STATE_TABLE}`
SET
  last_processed_ts = @new_last_processed_ts,
  lock_owner = NULL,
  lock_until = NULL,
  updated_at = CURRENT_TIMESTAMP()
WHERE state_key = @state_key AND lock_owner = @lock_owner
"""

RELEASE_LOCK_NO_ADVANCE_SQL = f"""
UPDATE `{STATE_TABLE}`
SET
  lock_owner = NULL,
  lock_until = NULL,
  updated_at = CURRENT_TIMESTAMP()
WHERE state_key = @state_key AND lock_owner = @lock_owner
"""


# =============================================================================
# SQL: PLAYER PROP PIPELINE (RAW → STAGE → LATEST → FLAT)
# =============================================================================

# Stage inserts ONLY new raw rows in (watermark, window_max]
# Adds a deterministic row_id to dedupe (game_id + player_id + market + type + line + book + snapshot_ts)
PLAYER_PROP_STAGE_INSERT_SQL = f"""
DECLARE window_start TIMESTAMP DEFAULT @window_start;
DECLARE window_end   TIMESTAMP DEFAULT @window_end;

INSERT INTO `{STAGE_PLAYER_PROP}` (
  snapshot_ts,
  game_id,
  player_id,
  market,
  market_type,
  line,
  book,
  over_odds,
  under_odds,
  milestone_odds,
  row_id,
  ingested_at
)
WITH new_raw AS (
  SELECT
    snapshot_ts,
    game_id,
    payload
  FROM `{RAW_PLAYER_PROP}`
  WHERE snapshot_ts > window_start
    AND snapshot_ts <= window_end
),
parsed AS (
  SELECT
    r.snapshot_ts,
    r.game_id,

    CAST(JSON_VALUE(i, '$.player_id') AS INT64) AS player_id,

    CASE LOWER(JSON_VALUE(i, '$.prop_type'))
      WHEN 'points' THEN 'pts'
      WHEN 'assists' THEN 'ast'
      WHEN 'rebounds' THEN 'reb'
      WHEN 'three_pointers_made' THEN '3pm'
      WHEN 'threes' THEN '3pm'
      ELSE NULL
    END AS market,

    CASE
      WHEN JSON_VALUE(i, '$.market.type') IS NOT NULL
        THEN JSON_VALUE(i, '$.market.type')
      WHEN JSON_VALUE(i, '$.market.odds') IS NOT NULL
        THEN 'milestone'
      WHEN JSON_VALUE(i, '$.market.over_odds') IS NOT NULL
        OR JSON_VALUE(i, '$.market.under_odds') IS NOT NULL
        THEN 'over_under'
      ELSE 'unknown'
    END AS market_type,

    CAST(JSON_VALUE(i, '$.line_value') AS FLOAT64) AS line,
    JSON_VALUE(i, '$.normalized_book') AS book,

    SAFE_CAST(JSON_VALUE(i, '$.market.over_odds') AS INT64) AS over_odds,
    SAFE_CAST(JSON_VALUE(i, '$.market.under_odds') AS INT64) AS under_odds,
    SAFE_CAST(JSON_VALUE(i, '$.market.odds') AS INT64) AS milestone_odds

  FROM new_raw r,
  UNNEST(JSON_QUERY_ARRAY(r.payload, '$.items')) AS i
  WHERE JSON_VALUE(i, '$.prop_type') IN (
    'points','assists','rebounds','three_pointers_made','threes'
  )
),
filtered AS (
  SELECT *
  FROM parsed
  WHERE market IS NOT NULL
    AND player_id IS NOT NULL
    AND game_id IS NOT NULL
    AND book IS NOT NULL
    AND line IS NOT NULL
),
dedup AS (
  SELECT
    *,
    TO_HEX(SHA256(CONCAT(
      CAST(game_id AS STRING), '|',
      CAST(player_id AS STRING), '|',
      market, '|',
      market_type, '|',
      CAST(line AS STRING), '|',
      book, '|',
      CAST(snapshot_ts AS STRING)
    ))) AS row_id
  FROM filtered
)
SELECT
  snapshot_ts,
  game_id,
  player_id,
  market,
  market_type,
  line,
  book,
  over_odds,
  under_odds,
  milestone_odds,
  row_id,
  CURRENT_TIMESTAMP() AS ingested_at
FROM dedup
QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY snapshot_ts DESC) = 1
"""

# LATEST is updated ONLY for keys appearing in the current window
PLAYER_PROP_LATEST_MERGE_SQL = f"""
DECLARE window_start TIMESTAMP DEFAULT @window_start;
DECLARE window_end   TIMESTAMP DEFAULT @window_end;

MERGE `{LATEST_PLAYER_PROP}` T
USING (
  WITH stage_window AS (
    SELECT *
    FROM `{STAGE_PLAYER_PROP}`
    WHERE snapshot_ts > window_start
      AND snapshot_ts <= window_end
  ),
  latest_per_key AS (
    SELECT
      game_id,
      player_id,
      market,
      market_type,
      line,
      book,
      ARRAY_AGG(
        STRUCT(
          snapshot_ts,
          over_odds,
          under_odds,
          milestone_odds
        )
        ORDER BY snapshot_ts DESC
        LIMIT 1
      )[OFFSET(0)] AS latest
    FROM stage_window
    GROUP BY
      game_id, player_id, market, market_type, line, book
  )
  SELECT
    game_id,
    player_id,
    market,
    market_type,
    line,
    book,
    latest.snapshot_ts AS snapshot_ts,
    latest.over_odds AS over_odds,
    latest.under_odds AS under_odds,
    latest.milestone_odds AS milestone_odds
  FROM latest_per_key
) S
ON
  T.game_id     = S.game_id
  AND T.player_id  = S.player_id
  AND T.market  = S.market
  AND T.market_type = S.market_type
  AND T.line    = S.line
  AND T.book    = S.book
WHEN MATCHED AND S.snapshot_ts > T.snapshot_ts THEN
  UPDATE SET
    snapshot_ts    = S.snapshot_ts,
    over_odds      = S.over_odds,
    under_odds     = S.under_odds,
    milestone_odds = S.milestone_odds,
    updated_at     = CURRENT_TIMESTAMP()
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
    milestone_odds,
    updated_at
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
    S.milestone_odds,
    CURRENT_TIMESTAMP()
  )
"""

# FLAT mirrors LATEST (but kept separate so the app table never “breaks” if you evolve LATEST later)
PLAYER_PROP_FLAT_MERGE_SQL = f"""
DECLARE window_start TIMESTAMP DEFAULT @window_start;
DECLARE window_end   TIMESTAMP DEFAULT @window_end;

MERGE `{FLAT_PLAYER_PROP}` T
USING (
  -- Only keys touched in this window
  WITH stage_window AS (
    SELECT DISTINCT
      game_id, player_id, market, market_type, line, book
    FROM `{STAGE_PLAYER_PROP}`
    WHERE snapshot_ts > window_start
      AND snapshot_ts <= window_end
  )
  SELECT
    L.*
  FROM `{LATEST_PLAYER_PROP}` L
  JOIN stage_window W
  USING (game_id, player_id, market, market_type, line, book)
) S
ON
  T.game_id     = S.game_id
  AND T.player_id  = S.player_id
  AND T.market  = S.market
  AND T.market_type = S.market_type
  AND T.line    = S.line
  AND T.book    = S.book
WHEN MATCHED AND S.snapshot_ts > T.snapshot_ts THEN
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
  )
"""


# =============================================================================
# SQL: GAME ODDS PIPELINE (RAW → STAGE → LATEST → FLAT)
# =============================================================================

GAME_ODDS_STAGE_INSERT_SQL = f"""
DECLARE window_start TIMESTAMP DEFAULT @window_start;
DECLARE window_end   TIMESTAMP DEFAULT @window_end;

INSERT INTO `{STAGE_GAME_ODDS}` (
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
  moneyline_away_odds,
  row_id,
  ingested_at
)
WITH new_raw AS (
  SELECT
    snapshot_ts,
    game_id,
    payload
  FROM `{RAW_GAME_ODDS}`
  WHERE snapshot_ts > window_start
    AND snapshot_ts <= window_end
),
parsed AS (
  SELECT
    snapshot_ts,
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
    CAST(JSON_VALUE(payload, '$.moneyline_away_odds') AS INT64) AS moneyline_away_odds
  FROM new_raw
),
filtered AS (
  SELECT *
  FROM parsed
  WHERE game_id IS NOT NULL AND book IS NOT NULL AND snapshot_ts IS NOT NULL
),
dedup AS (
  SELECT
    *,
    TO_HEX(SHA256(CONCAT(
      CAST(game_id AS STRING), '|',
      book, '|',
      CAST(snapshot_ts AS STRING)
    ))) AS row_id
  FROM filtered
)
SELECT
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
  moneyline_away_odds,
  row_id,
  CURRENT_TIMESTAMP() AS ingested_at
FROM dedup
QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY snapshot_ts DESC) = 1
"""

GAME_ODDS_LATEST_MERGE_SQL = f"""
DECLARE window_start TIMESTAMP DEFAULT @window_start;
DECLARE window_end   TIMESTAMP DEFAULT @window_end;

MERGE `{LATEST_GAME_ODDS}` T
USING (
  WITH stage_window AS (
    SELECT *
    FROM `{STAGE_GAME_ODDS}`
    WHERE snapshot_ts > window_start
      AND snapshot_ts <= window_end
  ),
  latest_per_key AS (
    SELECT
      game_id,
      book,
      ARRAY_AGG(
        STRUCT(
          snapshot_ts,
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
        ORDER BY snapshot_ts DESC
        LIMIT 1
      )[OFFSET(0)] AS latest
    FROM stage_window
    GROUP BY game_id, book
  )
  SELECT
    game_id,
    book,
    latest.snapshot_ts AS snapshot_ts,
    latest.spread_home AS spread_home,
    latest.spread_away AS spread_away,
    latest.spread_home_odds AS spread_home_odds,
    latest.spread_away_odds AS spread_away_odds,
    latest.total AS total,
    latest.over_odds AS over_odds,
    latest.under_odds AS under_odds,
    latest.moneyline_home_odds AS moneyline_home_odds,
    latest.moneyline_away_odds AS moneyline_away_odds
  FROM latest_per_key
) S
ON T.game_id = S.game_id AND T.book = S.book
WHEN MATCHED AND S.snapshot_ts > T.snapshot_ts THEN
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
    moneyline_away_odds    = S.moneyline_away_odds,
    updated_at             = CURRENT_TIMESTAMP()
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
    moneyline_away_odds,
    updated_at
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
    S.moneyline_away_odds,
    CURRENT_TIMESTAMP()
  )
"""

GAME_ODDS_FLAT_MERGE_SQL = f"""
DECLARE window_start TIMESTAMP DEFAULT @window_start;
DECLARE window_end   TIMESTAMP DEFAULT @window_end;

MERGE `{FLAT_GAME_ODDS}` T
USING (
  WITH stage_window AS (
    SELECT DISTINCT game_id, book
    FROM `{STAGE_GAME_ODDS}`
    WHERE snapshot_ts > window_start
      AND snapshot_ts <= window_end
  )
  SELECT L.*
  FROM `{LATEST_GAME_ODDS}` L
  JOIN stage_window W USING (game_id, book)
) S
ON T.game_id = S.game_id AND T.book = S.book
WHEN MATCHED AND S.snapshot_ts > T.snapshot_ts THEN
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
  )
"""


# =============================================================================
# HELPERS
# =============================================================================

def _run_query(sql: str, params: Optional[Dict[str, Any]] = None) -> bigquery.job.QueryJob:
    client = _get_client()
    job_config = bigquery.QueryJobConfig()

    qparams = []
    if params:
        for k, v in params.items():
            if isinstance(v, int):
                qparams.append(bigquery.ScalarQueryParameter(k, "INT64", v))
            elif isinstance(v, float):
                qparams.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
            elif isinstance(v, datetime):
                qparams.append(bigquery.ScalarQueryParameter(k, "TIMESTAMP", v))
            else:
                qparams.append(bigquery.ScalarQueryParameter(k, "STRING", v))
    job_config.query_parameters = qparams

    # Dry run / safety
    job_config.dry_run = DRY_RUN
    job_config.use_query_cache = False
    if MAX_BYTES_PER_QUERY > 0:
        job_config.maximum_bytes_billed = MAX_BYTES_PER_QUERY

    job = client.query(sql, job_config=job_config)

    if job.total_bytes_processed is not None and job.total_bytes_processed > MAX_BYTES_PER_QUERY:
        raise RuntimeError(
            f"Refusing to run query; would process {job.total_bytes_processed:,} bytes "
            f"(limit {MAX_BYTES_PER_QUERY:,})."
        )

    if DRY_RUN:
        print(f"🧪 DRY_RUN: would process {job.total_bytes_processed:,} bytes")
        return job

    job.result()
    return job


def _ensure_state_table() -> None:
    _run_query(ENSURE_STATE_TABLE_SQL)


def _acquire_lock(state_key: str, lock_owner: str) -> bool:
    _run_query(
        ACQUIRE_LOCK_SQL,
        {
            "state_key": state_key,
            "lock_owner": lock_owner,
            "lease_seconds": LEASE_SECONDS,
        },
    )
    # Verify we own it
    rows = list(
        _run_query(GET_STATE_SQL, {"state_key": state_key}).result()
        if not DRY_RUN
        else []
    )
    if DRY_RUN:
        return True
    if not rows:
        return False
    r = rows[0]
    return r["lock_owner"] == lock_owner and (r["lock_until"] is not None)


def _get_last_processed_ts(state_key: str) -> datetime:
    rows = list(
        _run_query(GET_STATE_SQL, {"state_key": state_key}).result()
        if not DRY_RUN
        else []
    )
    if DRY_RUN:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if not rows or rows[0]["last_processed_ts"] is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    ts = rows[0]["last_processed_ts"]
    # BigQuery returns naive datetime in UTC sometimes; normalize:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _get_raw_max_snapshot_ts(raw_table: str) -> Optional[datetime]:
    sql = f"SELECT MAX(snapshot_ts) AS max_ts FROM `{raw_table}`"
    rows = list(_run_query(sql).result() if not DRY_RUN else [])
    if DRY_RUN:
        return datetime.now(timezone.utc)
    max_ts = rows[0]["max_ts"] if rows else None
    if max_ts is None:
        return None
    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    return max_ts


def _release_lock(state_key: str, lock_owner: str, new_last_processed_ts: Optional[datetime]) -> None:
    if new_last_processed_ts is None:
        _run_query(RELEASE_LOCK_NO_ADVANCE_SQL, {"state_key": state_key, "lock_owner": lock_owner})
    else:
        _run_query(
            RELEASE_LOCK_AND_ADVANCE_SQL,
            {
                "state_key": state_key,
                "lock_owner": lock_owner,
                "new_last_processed_ts": new_last_processed_ts,
            },
        )


# =============================================================================
# PIPELINE RUNNERS
# =============================================================================

def _run_player_prop_pipeline(lock_owner: str) -> None:
    state_key = "live_player_prop_odds"

    print(f"\n=== 🏀 PLAYER PROP PIPELINE ({state_key}) ===")
    if not _acquire_lock(state_key, lock_owner):
        print("⏭️  Lock busy; skipping this run.")
        return

    try:
        last_ts = _get_last_processed_ts(state_key)
        raw_max = _get_raw_max_snapshot_ts(RAW_PLAYER_PROP)

        print("⏱️ PLAYER_PROP WATERMARK", {
            "state_key": state_key,
            "last_processed_ts": str(last_ts),
            "raw_max_ts": str(raw_max),
        })
        
        if raw_max is None or raw_max <= last_ts:
            print(f"✅ No new RAW rows. last_processed={last_ts} raw_max={raw_max}")
            _release_lock(state_key, lock_owner, None)
            return

        window_start = last_ts
        window_end = raw_max

        print(f"🔎 Window: ({window_start} , {window_end}]")

        print("➡️  RAW → STAGE (player props)")
        _run_query(
            PLAYER_PROP_STAGE_INSERT_SQL,
            {"window_start": window_start, "window_end": window_end},
        )

        print("➡️  STAGE → LATEST (player props)")
        _run_query(
            PLAYER_PROP_LATEST_MERGE_SQL,
            {"window_start": window_start, "window_end": window_end},
        )

        print("➡️  LATEST → FLAT (player props)")
        _run_query(
            PLAYER_PROP_FLAT_MERGE_SQL,
            {"window_start": window_start, "window_end": window_end},
        )

        print("✅ Player props complete; advancing watermark")
        print("🔓 ADVANCING PLAYER_PROP WATERMARK TO", str(window_end))
        _release_lock(state_key, lock_owner, window_end)

    except Exception as e:
        print(f"❌ Player props pipeline failed: {e}")
        # Always release lock so you don't deadlock future runs
        try:
            _release_lock(state_key, lock_owner, None)
        except Exception as e2:
            print(f"⚠️  Failed to release lock after error: {e2}")
        raise


def _run_game_odds_pipeline(lock_owner: str) -> None:
    state_key = "live_game_odds"

    print(f"\n=== 🎲 GAME ODDS PIPELINE ({state_key}) ===")
    if not _acquire_lock(state_key, lock_owner):
        print("⏭️  Lock busy; skipping this run.")
        return

    try:
        last_ts = _get_last_processed_ts(state_key)
        raw_max = _get_raw_max_snapshot_ts(RAW_GAME_ODDS)

        if raw_max is None or raw_max <= last_ts:
            print(f"✅ No new RAW rows. last_processed={last_ts} raw_max={raw_max}")
            _release_lock(state_key, lock_owner, None)
            return

        window_start = last_ts
        window_end = raw_max

        print(f"🔎 Window: ({window_start} , {window_end}]")

        print("➡️  RAW → STAGE (game odds)")
        _run_query(
            GAME_ODDS_STAGE_INSERT_SQL,
            {"window_start": window_start, "window_end": window_end},
        )

        print("➡️  STAGE → LATEST (game odds)")
        _run_query(
            GAME_ODDS_LATEST_MERGE_SQL,
            {"window_start": window_start, "window_end": window_end},
        )

        print("➡️  LATEST → FLAT (game odds)")
        _run_query(
            GAME_ODDS_FLAT_MERGE_SQL,
            {"window_start": window_start, "window_end": window_end},
        )

        print("✅ Game odds complete; advancing watermark")
        _release_lock(state_key, lock_owner, window_end)

    except Exception as e:
        print(f"❌ Game odds pipeline failed: {e}")
        try:
            _release_lock(state_key, lock_owner, None)
        except Exception as e2:
            print(f"⚠️  Failed to release lock after error: {e2}")
        raise


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def run_live_odds_orchestrator() -> None:
    """
    Safe orchestration runner:
      - uses ingest_state for watermark + lease locks
      - processes only new RAW rows since watermark
      - runs RAW → STAGE → LATEST → FLAT
      - safe to run every 2–5 minutes (or faster)

    Set DRY_RUN=true to estimate bytes.
    """
    if not ENABLE_LIVE_ODDS_FLATTEN:
        print("🚫 Live Odds Orchestrator disabled (ENABLE_LIVE_ODDS_FLATTEN=false)")
        return

    print("🚦 Live Odds Orchestrator starting")
    print(f"Project={PROJECT_ID} Dataset={DATASET} DryRun={DRY_RUN}")

    _ensure_state_table()

    lock_owner = f"{os.getenv('HOSTNAME', 'runner')}-{uuid.uuid4().hex[:8]}"

    _run_player_prop_pipeline(lock_owner)
    _run_game_odds_pipeline(lock_owner)

    print("\n🏁 Live Odds Orchestrator finished")


if __name__ == "__main__":
    run_live_odds_orchestrator()
