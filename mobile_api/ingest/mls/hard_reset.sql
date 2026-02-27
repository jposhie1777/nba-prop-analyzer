-- Hard reset for MLS raw ingest tables.
-- WARNING: This drops existing MLS tables and all historical data in them.
-- Run with: bq query --use_legacy_sql=false < mobile_api/ingest/mls/hard_reset.sql

CREATE SCHEMA IF NOT EXISTS `mls_data`
OPTIONS(location = "US");

DROP TABLE IF EXISTS `mls_data.teams`;
DROP TABLE IF EXISTS `mls_data.players`;
DROP TABLE IF EXISTS `mls_data.rosters`;
DROP TABLE IF EXISTS `mls_data.standings`;
DROP TABLE IF EXISTS `mls_data.matches`;
DROP TABLE IF EXISTS `mls_data.match_events`;
DROP TABLE IF EXISTS `mls_data.match_lineups`;
DROP TABLE IF EXISTS `mls_data.mlssoccer_schedule`;
DROP TABLE IF EXISTS `mls_data.mlssoccer_team_stats`;
DROP TABLE IF EXISTS `mls_data.mlssoccer_player_stats`;

CREATE TABLE `mls_data.teams` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS teams payload snapshots from BallDontLie MLS API');

CREATE TABLE `mls_data.players` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS players payload snapshots from BallDontLie MLS API');

CREATE TABLE `mls_data.rosters` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS team roster payload snapshots from BallDontLie MLS API');

CREATE TABLE `mls_data.standings` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS standings payload snapshots from BallDontLie MLS API');

CREATE TABLE `mls_data.matches` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS matches payload snapshots from BallDontLie MLS API');

CREATE TABLE `mls_data.match_events` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS match events payload snapshots from BallDontLie MLS API');

CREATE TABLE `mls_data.match_lineups` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS match lineups payload snapshots from BallDontLie MLS API');

-- ============================================================
-- mlssoccer.com scraper tables (stats-api.mlssoccer.com)
-- ============================================================

CREATE TABLE `mls_data.mlssoccer_schedule` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS match schedule snapshots from stats-api.mlssoccer.com');

CREATE TABLE `mls_data.mlssoccer_team_stats` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS per-club season stats from stats-api.mlssoccer.com');

CREATE TABLE `mls_data.mlssoccer_player_stats` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'MLS per-player season stats from stats-api.mlssoccer.com');
