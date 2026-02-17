-- BigQuery DDL for ATP data tables
-- Dataset: atp_data

-- Create dataset (run once)
-- CREATE SCHEMA IF NOT EXISTS `atp_data`;

CREATE TABLE IF NOT EXISTS `atp_data.players` (
  run_ts TIMESTAMP,
  ingested_at TIMESTAMP,
  player_id INT64,
  first_name STRING,
  last_name STRING,
  full_name STRING,
  country STRING,
  country_code STRING,
  birth_place STRING,
  age INT64,
  height_cm INT64,
  weight_kg INT64,
  plays STRING,
  turned_pro INT64,
  raw_json STRING
)
CLUSTER BY player_id
OPTIONS (
  description = 'ATP players (BallDontLie ATP API)'
);

CREATE TABLE IF NOT EXISTS `atp_data.tournaments` (
  run_ts TIMESTAMP,
  ingested_at TIMESTAMP,
  tournament_id INT64,
  name STRING,
  location STRING,
  surface STRING,
  category STRING,
  season INT64,
  start_date STRING,
  end_date STRING,
  prize_money INT64,
  prize_currency STRING,
  draw_size INT64,
  raw_json STRING
)
PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(1990, 2040, 1))
CLUSTER BY tournament_id, season
OPTIONS (
  description = 'ATP tournaments (BallDontLie ATP API)'
);

CREATE TABLE IF NOT EXISTS `atp_data.matches` (
  run_ts TIMESTAMP,
  ingested_at TIMESTAMP,
  match_id INT64,
  season INT64,
  round STRING,
  score STRING,
  duration STRING,
  number_of_sets INT64,
  match_status STRING,
  is_live BOOL,
  scheduled_time TIMESTAMP,
  not_before_text STRING,
  tournament_id INT64,
  tournament_name STRING,
  tournament_location STRING,
  surface STRING,
  category STRING,
  tournament_season INT64,
  tournament_start_date STRING,
  tournament_end_date STRING,
  player1_id INT64,
  player1_name STRING,
  player2_id INT64,
  player2_name STRING,
  winner_id INT64,
  winner_name STRING,
  raw_json STRING
)
PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(1990, 2040, 1))
CLUSTER BY tournament_id, player1_id, player2_id
OPTIONS (
  description = 'ATP matches (BallDontLie ATP API — ALL-STAR tier)'
);

CREATE TABLE IF NOT EXISTS `atp_data.rankings` (
  run_ts TIMESTAMP,
  ingested_at TIMESTAMP,
  ranking_id INT64,
  ranking_date STRING,
  rank INT64,
  points INT64,
  movement INT64,
  player_id INT64,
  player_name STRING,
  country STRING,
  country_code STRING,
  raw_json STRING
)
CLUSTER BY player_id, ranking_date
OPTIONS (
  description = 'ATP rankings (BallDontLie ATP API)'
);

CREATE TABLE IF NOT EXISTS `atp_data.player_lookup` (
  player_id INT64,
  player_name STRING,
  espn_player_id INT64,
  espn_display_name STRING,
  player_image_url STRING,
  last_verified TIMESTAMP,
  source STRING
)
CLUSTER BY player_id
OPTIONS (
  description = 'ATP player ESPN headshot lookup (one row per player)'
);

CREATE TABLE IF NOT EXISTS `atp_data.atp_race` (
  run_ts TIMESTAMP,
  ingested_at TIMESTAMP,
  race_id INT64,
  ranking_date STRING,
  rank INT64,
  points INT64,
  movement INT64,
  is_qualified BOOL,
  player_id INT64,
  player_name STRING,
  country STRING,
  country_code STRING,
  raw_json STRING
)
CLUSTER BY player_id, ranking_date
OPTIONS (
  description = 'ATP Race to Turin standings (BallDontLie ATP API — ALL-STAR tier)'
);

CREATE TABLE IF NOT EXISTS `atp_data.atp_betting_analytics` (
  player_id INT64 NOT NULL,
  player_name STRING,
  surface_key STRING NOT NULL,
  world_rank INT64,
  ranking_points INT64,
  total_matches INT64,
  total_wins INT64,
  raw_win_rate FLOAT64,
  adj_win_rate FLOAT64,
  l10_matches INT64,
  l10_wins INT64,
  l10_win_rate FLOAT64,
  l10_adj_win_rate FLOAT64,
  l15_matches INT64,
  l15_wins INT64,
  l15_win_rate FLOAT64,
  l15_adj_win_rate FLOAT64,
  l20_matches INT64,
  l20_wins INT64,
  l20_win_rate FLOAT64,
  l40_matches INT64,
  l40_wins INT64,
  l40_win_rate FLOAT64,
  l40_adj_win_rate FLOAT64,
  l10_surface_matches INT64,
  l10_surface_wins INT64,
  l10_surface_win_rate FLOAT64,
  l10_surface_adj_win_rate FLOAT64,
  l20_surface_matches INT64,
  l20_surface_wins INT64,
  l20_surface_win_rate FLOAT64,
  l20_surface_adj_win_rate FLOAT64,
  matches_vs_top50 INT64,
  wins_vs_top50 INT64,
  win_rate_vs_top50 FLOAT64,
  adj_win_rate_vs_top50 FLOAT64,
  straight_sets_wins INT64,
  straight_sets_rate FLOAT64,
  tiebreak_matches INT64,
  tiebreak_rate FLOAT64,
  avg_sets_per_match FLOAT64,
  retirement_matches INT64,
  retirement_rate FLOAT64,
  tournaments_played INT64,
  titles INT64,
  finals_reached INT64,
  semis_reached INT64,
  quarters_reached INT64,
  grand_slam_matches INT64,
  grand_slam_wins INT64,
  grand_slam_win_rate FLOAT64,
  masters_matches INT64,
  masters_wins INT64,
  masters_win_rate FLOAT64,
  current_win_streak INT64,
  current_loss_streak INT64,
  betting_form_score FLOAT64,
  sample_confidence FLOAT64,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(updated_at)
CLUSTER BY player_id, surface_key
OPTIONS (
  description = 'ATP betting analytics — precomputed by scheduled SQL (atp_betting_analytics.sql)'
);
