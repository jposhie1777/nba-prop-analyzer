-- BigQuery DDL for EPL ingestion tables
-- Dataset: epl_data

-- Optional dataset creation (run once)
CREATE SCHEMA IF NOT EXISTS `epl_data`
OPTIONS(location = "US");

CREATE TABLE IF NOT EXISTS `epl_data.teams` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'EPL teams payload snapshots from BallDontLie v2'
);

CREATE TABLE IF NOT EXISTS `epl_data.players` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'EPL players payload snapshots from BallDontLie v2'
);

CREATE TABLE IF NOT EXISTS `epl_data.rosters` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'EPL team roster payload snapshots from BallDontLie v2'
);

CREATE TABLE IF NOT EXISTS `epl_data.standings` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'EPL standings payload snapshots from BallDontLie v2'
);

CREATE TABLE IF NOT EXISTS `epl_data.matches` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'EPL matches payload snapshots from BallDontLie v2'
);

CREATE TABLE IF NOT EXISTS `epl_data.match_events` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'EPL match events payload snapshots from BallDontLie v2'
);

CREATE TABLE IF NOT EXISTS `epl_data.match_lineups` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (
  description = 'EPL match lineups payload snapshots from BallDontLie v2'
);
