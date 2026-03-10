CREATE SCHEMA IF NOT EXISTS `kbo_data`
OPTIONS(location = "US");

CREATE TABLE IF NOT EXISTS `kbo_data.games` (
  ingested_at TIMESTAMP NOT NULL,
  ingest_run_id STRING,
  season INT64 NOT NULL,
  game_date DATE NOT NULL,
  game_type STRING,
  game_time STRING,
  away_team STRING,
  home_team STRING,
  away_runs INT64,
  home_runs INT64,
  outcome STRING,
  status STRING,
  location STRING,
  notes STRING,
  game_key STRING
)
PARTITION BY game_date
CLUSTER BY season, away_team, home_team
OPTIONS(description='KBO game-level history from DailySchedule.aspx');

CREATE TABLE IF NOT EXISTS `kbo_data.team_summary` (
  ingested_at TIMESTAMP NOT NULL,
  ingest_run_id STRING,
  season INT64 NOT NULL,
  team STRING NOT NULL,
  games_played INT64,
  wins INT64,
  losses INT64,
  ties INT64,
  runs_scored INT64,
  runs_allowed INT64,
  avg_runs_scored FLOAT64,
  avg_runs_allowed FLOAT64
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, team
OPTIONS(description='KBO team season summary derived from games table');
