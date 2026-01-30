-- BigQuery DDL for Season Averages tables
-- Stores NBA Season Averages from Balldontlie API
-- https://docs.balldontlie.io/#season-averages

-- ===========================================================
-- PLAYER SEASON AVERAGES
-- One row per player per season/season_type/category/type
-- ===========================================================

CREATE TABLE IF NOT EXISTS `nba_live.player_season_averages` (
    -- Metadata
    run_ts TIMESTAMP NOT NULL,                   -- Batch run timestamp
    ingested_at TIMESTAMP NOT NULL,              -- Row insert timestamp

    -- Season identifiers
    season INT64 NOT NULL,                       -- e.g., 2024 for 2024-2025 season
    season_type STRING NOT NULL,                 -- regular, playoffs, ist, playin

    -- Category/Type (determines which stats are in payload)
    category STRING NOT NULL,                    -- general, clutch, defense, shooting, playtype, tracking, hustle, shotdashboard
    stat_type STRING,                            -- base, advanced, etc. (null for hustle)

    -- Player info (denormalized for easy querying)
    player_id INT64 NOT NULL,
    player_first_name STRING,
    player_last_name STRING,
    player_position STRING,
    player_height STRING,
    player_weight STRING,
    player_jersey_number STRING,
    player_college STRING,
    player_country STRING,
    player_draft_year INT64,
    player_draft_round INT64,
    player_draft_number INT64,

    -- Full API response payload as JSON (stats vary by category/type)
    stats JSON,                                  -- The stats object with all metrics
)
PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY player_id, category, stat_type
OPTIONS (
    description = 'NBA Player Season Averages from Balldontlie API. Partitioned by season.',
    labels = [("source", "balldontlie"), ("data_type", "season_averages")]
);


-- ===========================================================
-- TEAM SEASON AVERAGES
-- One row per team per season/season_type/category/type
-- ===========================================================

CREATE TABLE IF NOT EXISTS `nba_live.team_season_averages` (
    -- Metadata
    run_ts TIMESTAMP NOT NULL,                   -- Batch run timestamp
    ingested_at TIMESTAMP NOT NULL,              -- Row insert timestamp

    -- Season identifiers
    season INT64 NOT NULL,                       -- e.g., 2024 for 2024-2025 season
    season_type STRING NOT NULL,                 -- regular, playoffs, ist, playin

    -- Category/Type (determines which stats are in payload)
    category STRING NOT NULL,                    -- general, clutch, shooting, playtype, tracking, hustle, shotdashboard
    stat_type STRING,                            -- base, advanced, etc. (null for hustle)

    -- Team info (denormalized for easy querying)
    team_id INT64 NOT NULL,
    team_conference STRING,
    team_division STRING,
    team_city STRING,
    team_name STRING,
    team_full_name STRING,
    team_abbreviation STRING,

    -- Full API response payload as JSON (stats vary by category/type)
    stats JSON,                                  -- The stats object with all metrics
)
PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY team_id, category, stat_type
OPTIONS (
    description = 'NBA Team Season Averages from Balldontlie API. Partitioned by season.',
    labels = [("source", "balldontlie"), ("data_type", "season_averages")]
);


-- ===========================================================
-- USEFUL VIEWS
-- ===========================================================

-- Latest player season averages (general/base only - most common use case)
CREATE OR REPLACE VIEW `nba_live.player_season_averages_latest` AS
SELECT
    psa.*,
    JSON_VALUE(stats, '$.pts') AS pts,
    JSON_VALUE(stats, '$.reb') AS reb,
    JSON_VALUE(stats, '$.ast') AS ast,
    JSON_VALUE(stats, '$.stl') AS stl,
    JSON_VALUE(stats, '$.blk') AS blk,
    JSON_VALUE(stats, '$.min') AS min,
    JSON_VALUE(stats, '$.gp') AS gp,
    JSON_VALUE(stats, '$.fg_pct') AS fg_pct,
    JSON_VALUE(stats, '$.fg3_pct') AS fg3_pct,
    JSON_VALUE(stats, '$.ft_pct') AS ft_pct
FROM `nba_live.player_season_averages` psa
WHERE category = 'general'
  AND stat_type = 'base'
  AND run_ts = (
      SELECT MAX(run_ts)
      FROM `nba_live.player_season_averages`
      WHERE season = psa.season
        AND season_type = psa.season_type
        AND category = 'general'
        AND stat_type = 'base'
  );

-- Latest team season averages (general/base only)
CREATE OR REPLACE VIEW `nba_live.team_season_averages_latest` AS
SELECT
    tsa.*,
    JSON_VALUE(stats, '$.pts') AS pts,
    JSON_VALUE(stats, '$.reb') AS reb,
    JSON_VALUE(stats, '$.ast') AS ast,
    JSON_VALUE(stats, '$.stl') AS stl,
    JSON_VALUE(stats, '$.blk') AS blk,
    JSON_VALUE(stats, '$.w') AS w,
    JSON_VALUE(stats, '$.l') AS l,
    JSON_VALUE(stats, '$.gp') AS gp,
    JSON_VALUE(stats, '$.fg_pct') AS fg_pct,
    JSON_VALUE(stats, '$.fg3_pct') AS fg3_pct,
    JSON_VALUE(stats, '$.ft_pct') AS ft_pct
FROM `nba_live.team_season_averages` tsa
WHERE category = 'general'
  AND stat_type = 'base'
  AND run_ts = (
      SELECT MAX(run_ts)
      FROM `nba_live.team_season_averages`
      WHERE season = tsa.season
        AND season_type = tsa.season_type
        AND category = 'general'
        AND stat_type = 'base'
  );
