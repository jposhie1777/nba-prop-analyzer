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
