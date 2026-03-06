-- Flattened current-season website player stats (one row per player/stat).
-- Replace `YOUR_PROJECT_ID` / dataset if needed.
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.pga_data.website_player_stats` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  tour_code STRING,
  season_year INT64,
  player_id STRING NOT NULL,
  player_name STRING,
  country STRING,
  country_flag STRING,
  stat_id STRING,
  stat_name STRING,
  stat_title STRING,
  stat_value STRING,
  rank INT64,
  tour_avg STRING
)
PARTITION BY RANGE_BUCKET(season_year, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY tour_code, season_year, player_id;
