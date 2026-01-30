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
    -- Core stats
    CAST(JSON_VALUE(stats, '$.gp') AS INT64) AS gp,
    CAST(JSON_VALUE(stats, '$.w') AS INT64) AS w,
    CAST(JSON_VALUE(stats, '$.l') AS INT64) AS l,
    CAST(JSON_VALUE(stats, '$.min') AS FLOAT64) AS min,
    CAST(JSON_VALUE(stats, '$.pts') AS FLOAT64) AS pts,
    CAST(JSON_VALUE(stats, '$.reb') AS FLOAT64) AS reb,
    CAST(JSON_VALUE(stats, '$.ast') AS FLOAT64) AS ast,
    CAST(JSON_VALUE(stats, '$.stl') AS FLOAT64) AS stl,
    CAST(JSON_VALUE(stats, '$.blk') AS FLOAT64) AS blk,
    CAST(JSON_VALUE(stats, '$.tov') AS FLOAT64) AS tov,
    CAST(JSON_VALUE(stats, '$.pf') AS FLOAT64) AS pf,
    CAST(JSON_VALUE(stats, '$.pfd') AS FLOAT64) AS pfd,
    CAST(JSON_VALUE(stats, '$.age') AS FLOAT64) AS age,
    -- Rebounds breakdown
    CAST(JSON_VALUE(stats, '$.oreb') AS FLOAT64) AS oreb,
    CAST(JSON_VALUE(stats, '$.dreb') AS FLOAT64) AS dreb,
    -- Field goals
    CAST(JSON_VALUE(stats, '$.fga') AS FLOAT64) AS fga,
    CAST(JSON_VALUE(stats, '$.fgm') AS FLOAT64) AS fgm,
    CAST(JSON_VALUE(stats, '$.fg_pct') AS FLOAT64) AS fg_pct,
    -- 3-pointers
    CAST(JSON_VALUE(stats, '$.fg3a') AS FLOAT64) AS fg3a,
    CAST(JSON_VALUE(stats, '$.fg3m') AS FLOAT64) AS fg3m,
    CAST(JSON_VALUE(stats, '$.fg3_pct') AS FLOAT64) AS fg3_pct,
    -- Free throws
    CAST(JSON_VALUE(stats, '$.fta') AS FLOAT64) AS fta,
    CAST(JSON_VALUE(stats, '$.ftm') AS FLOAT64) AS ftm,
    CAST(JSON_VALUE(stats, '$.ft_pct') AS FLOAT64) AS ft_pct,
    -- Other
    CAST(JSON_VALUE(stats, '$.blka') AS FLOAT64) AS blka,
    CAST(JSON_VALUE(stats, '$.dd2') AS INT64) AS dd2,
    CAST(JSON_VALUE(stats, '$.td3') AS INT64) AS td3,
    CAST(JSON_VALUE(stats, '$.w_pct') AS FLOAT64) AS w_pct,
    CAST(JSON_VALUE(stats, '$.plus_minus') AS FLOAT64) AS plus_minus,
    CAST(JSON_VALUE(stats, '$.nba_fantasy_pts') AS FLOAT64) AS nba_fantasy_pts,
    CAST(JSON_VALUE(stats, '$.team_count') AS INT64) AS team_count,
    -- Ranks
    CAST(JSON_VALUE(stats, '$.gp_rank') AS INT64) AS gp_rank,
    CAST(JSON_VALUE(stats, '$.w_rank') AS INT64) AS w_rank,
    CAST(JSON_VALUE(stats, '$.l_rank') AS INT64) AS l_rank,
    CAST(JSON_VALUE(stats, '$.min_rank') AS INT64) AS min_rank,
    CAST(JSON_VALUE(stats, '$.pts_rank') AS INT64) AS pts_rank,
    CAST(JSON_VALUE(stats, '$.reb_rank') AS INT64) AS reb_rank,
    CAST(JSON_VALUE(stats, '$.ast_rank') AS INT64) AS ast_rank,
    CAST(JSON_VALUE(stats, '$.stl_rank') AS INT64) AS stl_rank,
    CAST(JSON_VALUE(stats, '$.blk_rank') AS INT64) AS blk_rank,
    CAST(JSON_VALUE(stats, '$.tov_rank') AS INT64) AS tov_rank,
    CAST(JSON_VALUE(stats, '$.pf_rank') AS INT64) AS pf_rank,
    CAST(JSON_VALUE(stats, '$.pfd_rank') AS INT64) AS pfd_rank,
    CAST(JSON_VALUE(stats, '$.oreb_rank') AS INT64) AS oreb_rank,
    CAST(JSON_VALUE(stats, '$.dreb_rank') AS INT64) AS dreb_rank,
    CAST(JSON_VALUE(stats, '$.fga_rank') AS INT64) AS fga_rank,
    CAST(JSON_VALUE(stats, '$.fgm_rank') AS INT64) AS fgm_rank,
    CAST(JSON_VALUE(stats, '$.fg_pct_rank') AS INT64) AS fg_pct_rank,
    CAST(JSON_VALUE(stats, '$.fg3a_rank') AS INT64) AS fg3a_rank,
    CAST(JSON_VALUE(stats, '$.fg3m_rank') AS INT64) AS fg3m_rank,
    CAST(JSON_VALUE(stats, '$.fg3_pct_rank') AS INT64) AS fg3_pct_rank,
    CAST(JSON_VALUE(stats, '$.fta_rank') AS INT64) AS fta_rank,
    CAST(JSON_VALUE(stats, '$.ftm_rank') AS INT64) AS ftm_rank,
    CAST(JSON_VALUE(stats, '$.ft_pct_rank') AS INT64) AS ft_pct_rank,
    CAST(JSON_VALUE(stats, '$.blka_rank') AS INT64) AS blka_rank,
    CAST(JSON_VALUE(stats, '$.dd2_rank') AS INT64) AS dd2_rank,
    CAST(JSON_VALUE(stats, '$.td3_rank') AS INT64) AS td3_rank,
    CAST(JSON_VALUE(stats, '$.w_pct_rank') AS INT64) AS w_pct_rank,
    CAST(JSON_VALUE(stats, '$.plus_minus_rank') AS INT64) AS plus_minus_rank,
    CAST(JSON_VALUE(stats, '$.nba_fantasy_pts_rank') AS INT64) AS nba_fantasy_pts_rank
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
    -- Core stats
    CAST(JSON_VALUE(stats, '$.gp') AS INT64) AS gp,
    CAST(JSON_VALUE(stats, '$.w') AS INT64) AS w,
    CAST(JSON_VALUE(stats, '$.l') AS INT64) AS l,
    CAST(JSON_VALUE(stats, '$.min') AS FLOAT64) AS min,
    CAST(JSON_VALUE(stats, '$.pts') AS FLOAT64) AS pts,
    CAST(JSON_VALUE(stats, '$.reb') AS FLOAT64) AS reb,
    CAST(JSON_VALUE(stats, '$.ast') AS FLOAT64) AS ast,
    CAST(JSON_VALUE(stats, '$.stl') AS FLOAT64) AS stl,
    CAST(JSON_VALUE(stats, '$.blk') AS FLOAT64) AS blk,
    CAST(JSON_VALUE(stats, '$.tov') AS FLOAT64) AS tov,
    CAST(JSON_VALUE(stats, '$.pf') AS FLOAT64) AS pf,
    CAST(JSON_VALUE(stats, '$.pfd') AS FLOAT64) AS pfd,
    -- Rebounds breakdown
    CAST(JSON_VALUE(stats, '$.oreb') AS FLOAT64) AS oreb,
    CAST(JSON_VALUE(stats, '$.dreb') AS FLOAT64) AS dreb,
    -- Field goals
    CAST(JSON_VALUE(stats, '$.fga') AS FLOAT64) AS fga,
    CAST(JSON_VALUE(stats, '$.fgm') AS FLOAT64) AS fgm,
    CAST(JSON_VALUE(stats, '$.fg_pct') AS FLOAT64) AS fg_pct,
    -- 3-pointers
    CAST(JSON_VALUE(stats, '$.fg3a') AS FLOAT64) AS fg3a,
    CAST(JSON_VALUE(stats, '$.fg3m') AS FLOAT64) AS fg3m,
    CAST(JSON_VALUE(stats, '$.fg3_pct') AS FLOAT64) AS fg3_pct,
    -- Free throws
    CAST(JSON_VALUE(stats, '$.fta') AS FLOAT64) AS fta,
    CAST(JSON_VALUE(stats, '$.ftm') AS FLOAT64) AS ftm,
    CAST(JSON_VALUE(stats, '$.ft_pct') AS FLOAT64) AS ft_pct,
    -- Other
    CAST(JSON_VALUE(stats, '$.blka') AS FLOAT64) AS blka,
    CAST(JSON_VALUE(stats, '$.w_pct') AS FLOAT64) AS w_pct,
    CAST(JSON_VALUE(stats, '$.plus_minus') AS FLOAT64) AS plus_minus,
    -- Ranks
    CAST(JSON_VALUE(stats, '$.gp_rank') AS INT64) AS gp_rank,
    CAST(JSON_VALUE(stats, '$.w_rank') AS INT64) AS w_rank,
    CAST(JSON_VALUE(stats, '$.l_rank') AS INT64) AS l_rank,
    CAST(JSON_VALUE(stats, '$.min_rank') AS INT64) AS min_rank,
    CAST(JSON_VALUE(stats, '$.pts_rank') AS INT64) AS pts_rank,
    CAST(JSON_VALUE(stats, '$.reb_rank') AS INT64) AS reb_rank,
    CAST(JSON_VALUE(stats, '$.ast_rank') AS INT64) AS ast_rank,
    CAST(JSON_VALUE(stats, '$.stl_rank') AS INT64) AS stl_rank,
    CAST(JSON_VALUE(stats, '$.blk_rank') AS INT64) AS blk_rank,
    CAST(JSON_VALUE(stats, '$.tov_rank') AS INT64) AS tov_rank,
    CAST(JSON_VALUE(stats, '$.pf_rank') AS INT64) AS pf_rank,
    CAST(JSON_VALUE(stats, '$.pfd_rank') AS INT64) AS pfd_rank,
    CAST(JSON_VALUE(stats, '$.oreb_rank') AS INT64) AS oreb_rank,
    CAST(JSON_VALUE(stats, '$.dreb_rank') AS INT64) AS dreb_rank,
    CAST(JSON_VALUE(stats, '$.fga_rank') AS INT64) AS fga_rank,
    CAST(JSON_VALUE(stats, '$.fgm_rank') AS INT64) AS fgm_rank,
    CAST(JSON_VALUE(stats, '$.fg_pct_rank') AS INT64) AS fg_pct_rank,
    CAST(JSON_VALUE(stats, '$.fg3a_rank') AS INT64) AS fg3a_rank,
    CAST(JSON_VALUE(stats, '$.fg3m_rank') AS INT64) AS fg3m_rank,
    CAST(JSON_VALUE(stats, '$.fg3_pct_rank') AS INT64) AS fg3_pct_rank,
    CAST(JSON_VALUE(stats, '$.fta_rank') AS INT64) AS fta_rank,
    CAST(JSON_VALUE(stats, '$.ftm_rank') AS INT64) AS ftm_rank,
    CAST(JSON_VALUE(stats, '$.ft_pct_rank') AS INT64) AS ft_pct_rank,
    CAST(JSON_VALUE(stats, '$.blka_rank') AS INT64) AS blka_rank,
    CAST(JSON_VALUE(stats, '$.w_pct_rank') AS INT64) AS w_pct_rank,
    CAST(JSON_VALUE(stats, '$.plus_minus_rank') AS INT64) AS plus_minus_rank
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
