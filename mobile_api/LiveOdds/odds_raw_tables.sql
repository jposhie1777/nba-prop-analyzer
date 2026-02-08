-- BigQuery table schema for The Odds API NBA alternate player points (over/under)
-- Dataset: odds_raw
-- Table: nba_alt_player_points

CREATE TABLE IF NOT EXISTS `graphite-flare-477419-h7.odds_raw.nba_alt_player_points` (
  snapshot_ts TIMESTAMP NOT NULL,
  request_date DATE NOT NULL,
  event_id STRING NOT NULL,
  sport_key STRING,
  sport_title STRING,
  commence_time TIMESTAMP,
  home_team STRING,
  away_team STRING,
  regions STRING,
  markets STRING,
  bookmaker_count INT64,
  payload STRING NOT NULL
)
PARTITION BY DATE(snapshot_ts)
CLUSTER BY event_id
OPTIONS (
  description = 'Raw The Odds API alternate player points (over/under) for NBA games'
);

-- ==========================================================
-- BigQuery table schema for The Odds API NBA alternate player props
-- Dataset: odds_raw
-- Table: nba_alt_player_props
-- Includes alternate points, rebounds, assists, threes, and combo markets.
-- ==========================================================

CREATE TABLE IF NOT EXISTS `graphite-flare-477419-h7.odds_raw.nba_alt_player_props` (
  snapshot_ts TIMESTAMP NOT NULL,
  request_date DATE NOT NULL,
  event_id STRING NOT NULL,
  sport_key STRING,
  sport_title STRING,
  commence_time TIMESTAMP,
  home_team STRING,
  away_team STRING,
  regions STRING,
  markets STRING,
  bookmaker_count INT64,
  payload STRING NOT NULL
)
PARTITION BY DATE(snapshot_ts)
CLUSTER BY event_id
OPTIONS (
  description = 'Raw The Odds API alternate player props for NBA games'
);
