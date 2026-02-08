-- BigQuery table schema for The Odds API NBA alternate totals (over/under)
-- Dataset: odds_raw
-- Table: nba_alt_points

CREATE TABLE IF NOT EXISTS `graphite-flare-477419-h7.odds_raw.nba_alt_points` (
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
  description = 'Raw The Odds API alternate totals (over/under) odds for NBA games'
);
