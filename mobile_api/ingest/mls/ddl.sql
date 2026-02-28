-- BigQuery DDL for MLS ingestion tables
-- Dataset: mls_data

-- Optional dataset creation (run once)
CREATE SCHEMA IF NOT EXISTS `mls_data`
OPTIONS(location = "US");

CREATE TABLE IF NOT EXISTS `mls_data.teams` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS teams payload snapshots from BallDontLie MLS API'
);

CREATE TABLE IF NOT EXISTS `mls_data.players` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS players payload snapshots from BallDontLie MLS API'
);

CREATE TABLE IF NOT EXISTS `mls_data.rosters` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS team roster payload snapshots from BallDontLie MLS API'
);

CREATE TABLE IF NOT EXISTS `mls_data.standings` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS standings payload snapshots from BallDontLie MLS API'
);

CREATE TABLE IF NOT EXISTS `mls_data.matches` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS matches payload snapshots from BallDontLie MLS API'
);

CREATE TABLE IF NOT EXISTS `mls_data.match_events` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS match events payload snapshots from BallDontLie MLS API'
);

CREATE TABLE IF NOT EXISTS `mls_data.match_lineups` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS match lineups payload snapshots from BallDontLie MLS API'
);

-- ============================================================
-- mlssoccer.com scraper tables (stats-api.mlssoccer.com)
-- ============================================================

CREATE TABLE IF NOT EXISTS `mls_data.mlssoccer_schedule` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS match schedule snapshots from stats-api.mlssoccer.com'
);

CREATE TABLE IF NOT EXISTS `mls_data.mlssoccer_team_stats` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS per-club season stats from stats-api.mlssoccer.com'
);

CREATE TABLE IF NOT EXISTS `mls_data.mlssoccer_player_stats` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS per-player season stats from stats-api.mlssoccer.com'
);

CREATE TABLE IF NOT EXISTS `mls_data.mlssoccer_team_game_stats` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS per-club per-match stats from stats-api.mlssoccer.com (entity_id = match_id_club_id)'
);

CREATE TABLE IF NOT EXISTS `mls_data.mlssoccer_player_game_stats` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'MLS per-player per-match stats from stats-api.mlssoccer.com (entity_id = match_id_player_id)'
);

-- ============================================================
-- RAW layer (Option A) — replaces mlssoccer_* as write target
-- Adds payload_hash (dedup/change detection) and ingest_run_id
-- (traceability). Old mlssoccer_* tables left intact (Step 0).
-- entity_id stable natural keys:
--   raw_schedule_json     → match_id
--   raw_team_season_json  → team_id
--   raw_player_season_json→ player_id
--   raw_team_match_json   → match_id_team_id
--   raw_player_match_json → match_id_player_id
-- ============================================================

CREATE TABLE IF NOT EXISTS `mls_data.raw_schedule_json` (
  ingested_at   TIMESTAMP NOT NULL,
  season        INT64 NOT NULL,
  entity_id     STRING,
  payload       STRING,
  payload_hash  STRING,
  ingest_run_id STRING,
  source        STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'RAW MLS match schedule — entity_id = match_id'
);

CREATE TABLE IF NOT EXISTS `mls_data.raw_team_season_json` (
  ingested_at   TIMESTAMP NOT NULL,
  season        INT64 NOT NULL,
  entity_id     STRING,
  payload       STRING,
  payload_hash  STRING,
  ingest_run_id STRING,
  source        STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'RAW MLS per-club season stats — entity_id = team_id'
);

CREATE TABLE IF NOT EXISTS `mls_data.raw_player_season_json` (
  ingested_at   TIMESTAMP NOT NULL,
  season        INT64 NOT NULL,
  entity_id     STRING,
  payload       STRING,
  payload_hash  STRING,
  ingest_run_id STRING,
  source        STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'RAW MLS per-player season stats — entity_id = player_id'
);

CREATE TABLE IF NOT EXISTS `mls_data.raw_team_match_json` (
  ingested_at   TIMESTAMP NOT NULL,
  season        INT64 NOT NULL,
  entity_id     STRING,
  payload       STRING,
  payload_hash  STRING,
  ingest_run_id STRING,
  source        STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'RAW MLS per-club per-match stats — entity_id = match_id_team_id'
);

CREATE TABLE IF NOT EXISTS `mls_data.raw_player_match_json` (
  ingested_at   TIMESTAMP NOT NULL,
  season        INT64 NOT NULL,
  entity_id     STRING,
  payload       STRING,
  payload_hash  STRING,
  ingest_run_id STRING,
  source        STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'RAW MLS per-player per-match stats — entity_id = match_id_player_id'
);

-- ============================================================
-- STAGING layer — typed + deduped (latest row wins per grain)
-- SQL guardrail: match_date <= CURRENT_DATE() prevents future
-- boxscores from leaking into facts even if Python writes them.
-- ============================================================

CREATE OR REPLACE VIEW `mls_data.stg_matches` AS
WITH ranked AS (
  SELECT
    JSON_VALUE(payload, '$.match_id')                         AS match_id,
    CAST(JSON_VALUE(payload, '$.season') AS INT64)            AS season,
    DATE(JSON_VALUE(payload, '$.match_date'))                  AS match_date,
    JSON_VALUE(payload, '$.home_team_id')                     AS home_team_id,
    JSON_VALUE(payload, '$.away_team_id')                     AS away_team_id,
    JSON_VALUE(payload, '$.match_status')                     AS match_status,
    CAST(JSON_VALUE(payload, '$.home_team_goals') AS INT64)   AS home_goals,
    CAST(JSON_VALUE(payload, '$.away_team_goals') AS INT64)   AS away_goals,
    ingested_at,
    payload,
    payload_hash,
    ingest_run_id,
    ROW_NUMBER() OVER (
      PARTITION BY JSON_VALUE(payload, '$.match_id')
      ORDER BY ingested_at DESC
    ) AS rn
  FROM `mls_data.raw_schedule_json`
  WHERE
    DATE(JSON_VALUE(payload, '$.match_date')) <= CURRENT_DATE()
    AND LOWER(JSON_VALUE(payload, '$.match_status')) IN (
        'finalwhistle','final','ft','completed','finished'
    )
)
SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;

CREATE OR REPLACE VIEW `mls_data.stg_team_season` AS
WITH ranked AS (
  SELECT
    CAST(season AS INT64)                                      AS season,
    JSON_VALUE(payload, '$.team_id')                          AS team_id,
    JSON_VALUE(payload, '$.club_id')                          AS club_id,
    JSON_VALUE(payload, '$.team_name')                        AS team_name,
    CAST(JSON_VALUE(payload, '$.appearances') AS INT64)       AS appearances,
    CAST(JSON_VALUE(payload, '$.goals') AS INT64)             AS goals,
    CAST(JSON_VALUE(payload, '$.assists') AS INT64)           AS assists,
    ingested_at,
    payload,
    payload_hash,
    ingest_run_id,
    ROW_NUMBER() OVER (
      PARTITION BY season, COALESCE(JSON_VALUE(payload, '$.team_id'), JSON_VALUE(payload, '$.club_id'))
      ORDER BY ingested_at DESC
    ) AS rn
  FROM `mls_data.raw_team_season_json`
)
SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;

CREATE OR REPLACE VIEW `mls_data.stg_player_season` AS
WITH ranked AS (
  SELECT
    CAST(season AS INT64)                                      AS season,
    JSON_VALUE(payload, '$.player_id')                        AS player_id,
    JSON_VALUE(payload, '$.player_name')                      AS player_name,
    JSON_VALUE(payload, '$.club')                             AS club,
    JSON_VALUE(payload, '$.position')                         AS position,
    CAST(JSON_VALUE(payload, '$.appearances') AS INT64)       AS appearances,
    CAST(JSON_VALUE(payload, '$.goals') AS INT64)             AS goals,
    CAST(JSON_VALUE(payload, '$.assists') AS INT64)           AS assists,
    CAST(JSON_VALUE(payload, '$.minutes_played') AS INT64)    AS minutes_played,
    CAST(JSON_VALUE(payload, '$.yellow_cards') AS INT64)      AS yellow_cards,
    CAST(JSON_VALUE(payload, '$.red_cards') AS INT64)         AS red_cards,
    ingested_at,
    payload,
    payload_hash,
    ingest_run_id,
    ROW_NUMBER() OVER (
      PARTITION BY season, JSON_VALUE(payload, '$.player_id')
      ORDER BY ingested_at DESC
    ) AS rn
  FROM `mls_data.raw_player_season_json`
)
SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;

CREATE OR REPLACE VIEW `mls_data.stg_team_match` AS
WITH ranked AS (
  SELECT
    JSON_VALUE(payload, '$.match_id')                         AS match_id,
    COALESCE(
      JSON_VALUE(payload, '$.team_id'),
      JSON_VALUE(payload, '$.club_id')
    )                                                         AS team_id,
    CAST(season AS INT64)                                     AS season,
    DATE(JSON_VALUE(payload, '$.match_date'))                 AS match_date,
    ingested_at,
    payload,
    payload_hash,
    ingest_run_id,
    ROW_NUMBER() OVER (
      PARTITION BY entity_id   -- match_id_team_id
      ORDER BY ingested_at DESC
    ) AS rn
  FROM `mls_data.raw_team_match_json`
  WHERE
    DATE(JSON_VALUE(payload, '$.match_date')) <= CURRENT_DATE()  -- SQL guardrail
)
SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;

CREATE OR REPLACE VIEW `mls_data.stg_player_match` AS
WITH ranked AS (
  SELECT
    JSON_VALUE(payload, '$.match_id')                         AS match_id,
    COALESCE(
      JSON_VALUE(payload, '$.player_id'),
      JSON_VALUE(payload, '$.playerId')
    )                                                         AS player_id,
    CAST(season AS INT64)                                     AS season,
    DATE(JSON_VALUE(payload, '$.match_date'))                 AS match_date,
    CAST(JSON_VALUE(payload, '$.minutes_played') AS INT64)    AS minutes_played,
    CAST(JSON_VALUE(payload, '$.goals') AS INT64)             AS goals,
    CAST(JSON_VALUE(payload, '$.assists') AS INT64)           AS assists,
    CAST(JSON_VALUE(payload, '$.shots') AS INT64)             AS shots,
    CAST(JSON_VALUE(payload, '$.shots_on_target') AS INT64)   AS shots_on_target,
    CAST(JSON_VALUE(payload, '$.yellow_cards') AS INT64)      AS yellow_cards,
    CAST(JSON_VALUE(payload, '$.red_cards') AS INT64)         AS red_cards,
    CAST(JSON_VALUE(payload, '$.passes') AS INT64)            AS passes,
    CAST(JSON_VALUE(payload, '$.key_passes') AS INT64)        AS key_passes,
    CAST(JSON_VALUE(payload, '$.tackles') AS INT64)           AS tackles,
    CAST(JSON_VALUE(payload, '$.xg') AS FLOAT64)              AS xg,
    CAST(JSON_VALUE(payload, '$.xa') AS FLOAT64)              AS xa,
    ingested_at,
    payload,
    payload_hash,
    ingest_run_id,
    ROW_NUMBER() OVER (
      PARTITION BY entity_id   -- match_id_player_id
      ORDER BY ingested_at DESC
    ) AS rn
  FROM `mls_data.raw_player_match_json`
  WHERE
    DATE(JSON_VALUE(payload, '$.match_date')) <= CURRENT_DATE()  -- SQL guardrail
)
SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;

-- ============================================================
-- FACT layer — partitioned by match_date (not ingested_at)
-- These are views over staging for now; promote to tables via
-- the stored procedures in stored_procedures.sql when volume
-- warrants materialization.
-- ============================================================

CREATE OR REPLACE VIEW `mls_data.f_match` AS
SELECT
  match_id,
  season,
  match_date,
  home_team_id,
  away_team_id,
  match_status,
  home_goals,
  away_goals,
  CASE WHEN home_goals > away_goals THEN 'home'
       WHEN away_goals > home_goals THEN 'away'
       ELSE 'draw' END                           AS result,
  (home_goals + away_goals)                      AS total_goals,
  ingested_at,
  payload_hash,
  ingest_run_id
FROM `mls_data.stg_matches`;

CREATE OR REPLACE VIEW `mls_data.f_team_match` AS
SELECT
  stm.match_id,
  stm.team_id,
  stm.season,
  stm.match_date,
  fm.home_team_id,
  fm.away_team_id,
  CASE WHEN stm.team_id = fm.home_team_id THEN 'home' ELSE 'away' END AS side,
  CASE WHEN stm.team_id = fm.home_team_id THEN fm.away_team_id
       ELSE fm.home_team_id END                                         AS opponent_id,
  fm.result,
  stm.payload,
  stm.payload_hash,
  stm.ingest_run_id
FROM `mls_data.stg_team_match` stm
LEFT JOIN `mls_data.f_match` fm USING (match_id);

CREATE OR REPLACE VIEW `mls_data.f_player_match` AS
SELECT
  spm.match_id,
  spm.player_id,
  spm.season,
  spm.match_date,
  fm.home_team_id,
  fm.away_team_id,
  fm.result,
  spm.minutes_played,
  spm.goals,
  spm.assists,
  spm.shots,
  spm.shots_on_target,
  spm.yellow_cards,
  spm.red_cards,
  spm.passes,
  spm.key_passes,
  spm.tackles,
  spm.xg,
  spm.xa,
  spm.payload,
  spm.payload_hash,
  spm.ingest_run_id
FROM `mls_data.stg_player_match` spm
LEFT JOIN `mls_data.f_match` fm USING (match_id);

CREATE OR REPLACE VIEW `mls_data.f_team_season` AS
SELECT
  season,
  COALESCE(team_id, club_id)  AS team_id,
  team_name,
  appearances,
  goals,
  assists,
  ingested_at,
  payload_hash,
  ingest_run_id
FROM `mls_data.stg_team_season`;

CREATE OR REPLACE VIEW `mls_data.f_player_season` AS
SELECT
  season,
  player_id,
  player_name,
  club,
  position,
  appearances,
  goals,
  assists,
  minutes_played,
  yellow_cards,
  red_cards,
  ingested_at,
  payload_hash,
  ingest_run_id
FROM `mls_data.stg_player_season`;
