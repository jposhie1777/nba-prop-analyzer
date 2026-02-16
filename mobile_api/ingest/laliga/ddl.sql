-- BigQuery DDL for LALIGA ingestion tables
-- Dataset: laliga_data

-- Optional dataset creation (run once)
CREATE SCHEMA IF NOT EXISTS `laliga_data`
OPTIONS(location = "US");

CREATE TABLE IF NOT EXISTS `laliga_data.teams` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'LALIGA teams payload snapshots from BallDontLie LaLiga API'
);

CREATE TABLE IF NOT EXISTS `laliga_data.players` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'LALIGA players payload snapshots from BallDontLie LaLiga API'
);

CREATE TABLE IF NOT EXISTS `laliga_data.rosters` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'LALIGA team roster payload snapshots from BallDontLie LaLiga API'
);

CREATE TABLE IF NOT EXISTS `laliga_data.standings` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'LALIGA standings payload snapshots from BallDontLie LaLiga API'
);

CREATE TABLE IF NOT EXISTS `laliga_data.matches` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'LALIGA matches payload snapshots from BallDontLie LaLiga API'
);

CREATE TABLE IF NOT EXISTS `laliga_data.match_events` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'LALIGA match events payload snapshots from BallDontLie LaLiga API'
);

CREATE TABLE IF NOT EXISTS `laliga_data.match_lineups` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'LALIGA match lineups payload snapshots from BallDontLie LaLiga API'
);
