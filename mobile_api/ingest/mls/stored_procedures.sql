-- =============================================================================
-- MLS BigQuery Stored Procedures
-- Implements Steps 5-8 of the MLS pipeline architecture.
--
-- Execution order (called by run_mls_pipeline):
--   1. build_stg_*      — dedupe raw rows → staging tables
--   2. build_f_*        — staging → fact tables (partitioned by match_date)
--   3. build_feat_*     — facts → feature tables (rolling form windows)
--
-- Usage:
--   CALL mls_data.run_mls_pipeline(2025);
--   CALL mls_data.run_mls_pipeline(EXTRACT(YEAR FROM CURRENT_DATE()));
-- =============================================================================

-- ============================================================
-- STAGING BUILD PROCEDURES
-- "Latest row wins" dedup per natural key.
-- SQL guardrail: match_date <= CURRENT_DATE() on match tables.
-- ============================================================

CREATE OR REPLACE PROCEDURE `mls_data.build_stg_matches`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.stg_matches`
  PARTITION BY match_date
  CLUSTER BY season, home_team_id, away_team_id
  AS
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
      season = season_arg
      AND DATE(JSON_VALUE(payload, '$.match_date')) <= CURRENT_DATE()
      AND LOWER(JSON_VALUE(payload, '$.match_status')) IN (
          'finalwhistle','final','ft','completed','finished'
      )
  )
  SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;
END;


CREATE OR REPLACE PROCEDURE `mls_data.build_stg_team_season`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.stg_team_season`
  PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2020, 2030, 1))
  CLUSTER BY season, team_id
  AS
  WITH ranked AS (
    SELECT
      CAST(season AS INT64)                                      AS season,
      COALESCE(
        JSON_VALUE(payload, '$.team_id'),
        JSON_VALUE(payload, '$.club_id')
      )                                                          AS team_id,
      JSON_VALUE(payload, '$.team_name')                        AS team_name,
      CAST(JSON_VALUE(payload, '$.appearances') AS INT64)       AS appearances,
      CAST(JSON_VALUE(payload, '$.goals') AS INT64)             AS goals,
      CAST(JSON_VALUE(payload, '$.assists') AS INT64)           AS assists,
      CAST(JSON_VALUE(payload, '$.shots') AS INT64)             AS shots,
      CAST(JSON_VALUE(payload, '$.yellow_cards') AS INT64)      AS yellow_cards,
      CAST(JSON_VALUE(payload, '$.red_cards') AS INT64)         AS red_cards,
      ingested_at,
      payload,
      payload_hash,
      ingest_run_id,
      ROW_NUMBER() OVER (
        PARTITION BY
          season,
          COALESCE(JSON_VALUE(payload, '$.team_id'), JSON_VALUE(payload, '$.club_id'))
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `mls_data.raw_team_season_json`
    WHERE season = season_arg
  )
  SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;
END;


CREATE OR REPLACE PROCEDURE `mls_data.build_stg_player_season`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.stg_player_season`
  PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2020, 2030, 1))
  CLUSTER BY season, player_id
  AS
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
      CAST(JSON_VALUE(payload, '$.shots') AS INT64)             AS shots,
      CAST(JSON_VALUE(payload, '$.shots_on_target') AS INT64)   AS shots_on_target,
      CAST(JSON_VALUE(payload, '$.xg') AS FLOAT64)              AS xg,
      CAST(JSON_VALUE(payload, '$.xa') AS FLOAT64)              AS xa,
      ingested_at,
      payload,
      payload_hash,
      ingest_run_id,
      ROW_NUMBER() OVER (
        PARTITION BY season, JSON_VALUE(payload, '$.player_id')
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `mls_data.raw_player_season_json`
    WHERE season = season_arg
  )
  SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;
END;


CREATE OR REPLACE PROCEDURE `mls_data.build_stg_team_match`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.stg_team_match`
  PARTITION BY match_date
  CLUSTER BY season, team_id
  AS
  WITH ranked AS (
    SELECT
      JSON_VALUE(payload, '$.match_id')                         AS match_id,
      COALESCE(
        JSON_VALUE(payload, '$.team_id'),
        JSON_VALUE(payload, '$.club_id')
      )                                                         AS team_id,
      CAST(season AS INT64)                                     AS season,
      DATE(JSON_VALUE(payload, '$.match_date'))                 AS match_date,
      CAST(JSON_VALUE(payload, '$.goals') AS INT64)             AS goals,
      CAST(JSON_VALUE(payload, '$.shots') AS INT64)             AS shots,
      CAST(JSON_VALUE(payload, '$.shots_on_target') AS INT64)   AS shots_on_target,
      CAST(JSON_VALUE(payload, '$.possession') AS FLOAT64)      AS possession,
      CAST(JSON_VALUE(payload, '$.passes') AS INT64)            AS passes,
      CAST(JSON_VALUE(payload, '$.corners') AS INT64)           AS corners,
      CAST(JSON_VALUE(payload, '$.fouls') AS INT64)             AS fouls,
      CAST(JSON_VALUE(payload, '$.yellow_cards') AS INT64)      AS yellow_cards,
      CAST(JSON_VALUE(payload, '$.red_cards') AS INT64)         AS red_cards,
      ingested_at,
      entity_id,
      payload,
      payload_hash,
      ingest_run_id,
      ROW_NUMBER() OVER (
        PARTITION BY entity_id   -- match_id_team_id
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `mls_data.raw_team_match_json`
    WHERE
      season = season_arg
      AND DATE(JSON_VALUE(payload, '$.match_date')) <= CURRENT_DATE()  -- SQL guardrail
  )
  SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;
END;


CREATE OR REPLACE PROCEDURE `mls_data.build_stg_player_match`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.stg_player_match`
  PARTITION BY match_date
  CLUSTER BY season, player_id
  AS
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
      entity_id,
      payload,
      payload_hash,
      ingest_run_id,
      ROW_NUMBER() OVER (
        PARTITION BY entity_id   -- match_id_player_id
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `mls_data.raw_player_match_json`
    WHERE
      season = season_arg
      AND DATE(JSON_VALUE(payload, '$.match_date')) <= CURRENT_DATE()  -- SQL guardrail
  )
  SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;
END;


-- ============================================================
-- FACT BUILD PROCEDURES
-- Clean selects from staging with derived fields.
-- Partitioned by match_date so betting windows are cheap.
-- ============================================================

CREATE OR REPLACE PROCEDURE `mls_data.build_f_match`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.f_match`
  PARTITION BY match_date
  CLUSTER BY season
  AS
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
         ELSE 'draw' END                         AS result,
    (home_goals + away_goals)                    AS total_goals,
    (home_goals IS NOT NULL
     AND away_goals IS NOT NULL
     AND home_goals > 0
     AND away_goals > 0)                         AS btts,
    ingested_at,
    payload_hash,
    ingest_run_id
  FROM `mls_data.stg_matches`
  WHERE season = season_arg;
END;


CREATE OR REPLACE PROCEDURE `mls_data.build_f_team_match`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.f_team_match`
  PARTITION BY match_date
  CLUSTER BY team_id, season
  AS
  SELECT
    stm.match_id,
    stm.team_id,
    stm.season,
    stm.match_date,
    fm.home_team_id,
    fm.away_team_id,
    CASE WHEN stm.team_id = fm.home_team_id THEN 'home' ELSE 'away' END  AS side,
    CASE WHEN stm.team_id = fm.home_team_id THEN fm.away_team_id
         ELSE fm.home_team_id END                                          AS opponent_id,
    fm.result,
    CASE
      WHEN stm.team_id = fm.home_team_id AND fm.result = 'home' THEN 'win'
      WHEN stm.team_id = fm.away_team_id AND fm.result = 'away' THEN 'win'
      WHEN fm.result = 'draw' THEN 'draw'
      ELSE 'loss'
    END                                                                    AS team_result,
    stm.goals,
    stm.shots,
    stm.shots_on_target,
    stm.possession,
    stm.passes,
    stm.corners,
    stm.fouls,
    stm.yellow_cards,
    stm.red_cards,
    stm.payload_hash,
    stm.ingest_run_id
  FROM `mls_data.stg_team_match` stm
  LEFT JOIN `mls_data.f_match` fm USING (match_id)
  WHERE stm.season = season_arg;
END;


CREATE OR REPLACE PROCEDURE `mls_data.build_f_player_match`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.f_player_match`
  PARTITION BY match_date
  CLUSTER BY player_id, season
  AS
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
    spm.payload_hash,
    spm.ingest_run_id
  FROM `mls_data.stg_player_match` spm
  LEFT JOIN `mls_data.f_match` fm USING (match_id)
  WHERE spm.season = season_arg;
END;


CREATE OR REPLACE PROCEDURE `mls_data.build_f_team_season`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.f_team_season`
  PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2020, 2030, 1))
  CLUSTER BY season, team_id
  AS
  SELECT
    season,
    COALESCE(team_id, club_id) AS team_id,
    team_name,
    appearances,
    goals,
    assists,
    shots,
    yellow_cards,
    red_cards,
    ingested_at,
    payload_hash,
    ingest_run_id
  FROM `mls_data.stg_team_season`
  WHERE season = season_arg;
END;


CREATE OR REPLACE PROCEDURE `mls_data.build_f_player_season`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.f_player_season`
  PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2020, 2030, 1))
  CLUSTER BY season, player_id
  AS
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
    shots,
    shots_on_target,
    xg,
    xa,
    ingested_at,
    payload_hash,
    ingest_run_id
  FROM `mls_data.stg_player_season`
  WHERE season = season_arg;
END;


-- ============================================================
-- FEATURE BUILD PROCEDURES (Step 7)
-- Rolling 5 / 10 game windows for betting model features.
-- ============================================================

CREATE OR REPLACE PROCEDURE `mls_data.build_feat_player_form`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.feat_player_form`
  PARTITION BY match_date
  CLUSTER BY player_id, season
  AS
  SELECT
    player_id,
    season,
    match_id,
    match_date,

    -- Rolling 5
    SUM(minutes_played) OVER w5    AS minutes_r5,
    SUM(goals)          OVER w5    AS goals_r5,
    SUM(assists)        OVER w5    AS assists_r5,
    SUM(shots)          OVER w5    AS shots_r5,
    SUM(shots_on_target)OVER w5    AS sot_r5,
    SUM(xg)             OVER w5    AS xg_r5,
    SUM(xa)             OVER w5    AS xa_r5,

    -- Rolling 10
    SUM(minutes_played) OVER w10   AS minutes_r10,
    SUM(goals)          OVER w10   AS goals_r10,
    SUM(assists)        OVER w10   AS assists_r10,
    SUM(shots)          OVER w10   AS shots_r10,
    SUM(shots_on_target)OVER w10   AS sot_r10,
    SUM(xg)             OVER w10   AS xg_r10,
    SUM(xa)             OVER w10   AS xa_r10,

    -- Rest days (gap from previous match)
    DATE_DIFF(
      match_date,
      LAG(match_date) OVER (PARTITION BY player_id ORDER BY match_date),
      DAY
    )                              AS rest_days,

    minutes_played,
    goals,
    assists,
    shots,
    shots_on_target,
    yellow_cards,
    red_cards,
    xg,
    xa

  FROM `mls_data.f_player_match`
  WHERE season = season_arg

  WINDOW
    w5  AS (PARTITION BY player_id ORDER BY match_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
    w10 AS (PARTITION BY player_id ORDER BY match_date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW);
END;


CREATE OR REPLACE PROCEDURE `mls_data.build_feat_team_form`(season_arg INT64)
BEGIN
  CREATE OR REPLACE TABLE `mls_data.feat_team_form`
  PARTITION BY match_date
  CLUSTER BY team_id, season
  AS
  SELECT
    team_id,
    season,
    match_id,
    match_date,
    side,
    opponent_id,
    team_result,

    -- Rolling 5
    SUM(goals)            OVER w5  AS goals_scored_r5,
    SUM(shots)            OVER w5  AS shots_r5,
    SUM(shots_on_target)  OVER w5  AS sot_r5,
    SUM(possession)       OVER w5  AS possession_r5,
    COUNTIF(team_result='win')  OVER w5  AS wins_r5,
    COUNTIF(team_result='draw') OVER w5  AS draws_r5,
    COUNTIF(team_result='loss') OVER w5  AS losses_r5,

    -- Rolling 10
    SUM(goals)            OVER w10 AS goals_scored_r10,
    SUM(shots)            OVER w10 AS shots_r10,
    SUM(shots_on_target)  OVER w10 AS sot_r10,
    SUM(possession)       OVER w10 AS possession_r10,
    COUNTIF(team_result='win')  OVER w10 AS wins_r10,
    COUNTIF(team_result='draw') OVER w10 AS draws_r10,
    COUNTIF(team_result='loss') OVER w10 AS losses_r10,

    -- Rest days
    DATE_DIFF(
      match_date,
      LAG(match_date) OVER (PARTITION BY team_id ORDER BY match_date),
      DAY
    )                              AS rest_days,

    goals,
    shots,
    shots_on_target,
    possession,
    yellow_cards,
    red_cards

  FROM `mls_data.f_team_match`
  WHERE season = season_arg

  WINDOW
    w5  AS (PARTITION BY team_id ORDER BY match_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
    w10 AS (PARTITION BY team_id ORDER BY match_date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW);
END;


-- ============================================================
-- MASTER PIPELINE PROCEDURE (Step 8)
-- One call rebuilds an entire season end-to-end.
-- Scheduler runs:
--   CALL mls_data.run_mls_pipeline(EXTRACT(YEAR FROM CURRENT_DATE()));
-- ============================================================

CREATE OR REPLACE PROCEDURE `mls_data.run_mls_pipeline`(season_arg INT64)
BEGIN
  -- Staging (dedupe raw → typed)
  CALL mls_data.build_stg_matches(season_arg);
  CALL mls_data.build_stg_team_season(season_arg);
  CALL mls_data.build_stg_player_season(season_arg);
  CALL mls_data.build_stg_team_match(season_arg);
  CALL mls_data.build_stg_player_match(season_arg);

  -- Facts (staging → partitioned by match_date)
  CALL mls_data.build_f_match(season_arg);
  CALL mls_data.build_f_team_match(season_arg);
  CALL mls_data.build_f_player_match(season_arg);
  CALL mls_data.build_f_team_season(season_arg);
  CALL mls_data.build_f_player_season(season_arg);

  -- Features (rolling windows for betting models)
  CALL mls_data.build_feat_player_form(season_arg);
  CALL mls_data.build_feat_team_form(season_arg);
END;
