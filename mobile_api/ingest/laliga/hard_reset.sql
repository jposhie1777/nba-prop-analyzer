-- Hard reset for LALIGA raw ingest tables.
-- WARNING: This drops existing LALIGA tables and all historical data in them.
-- Run with: bq query --use_legacy_sql=false < mobile_api/ingest/laliga/hard_reset.sql

CREATE SCHEMA IF NOT EXISTS `laliga_data`
OPTIONS(location = "US");

DROP TABLE IF EXISTS `laliga_data.teams`;
DROP TABLE IF EXISTS `laliga_data.players`;
DROP TABLE IF EXISTS `laliga_data.rosters`;
DROP TABLE IF EXISTS `laliga_data.standings`;
DROP TABLE IF EXISTS `laliga_data.matches`;
DROP TABLE IF EXISTS `laliga_data.match_events`;
DROP TABLE IF EXISTS `laliga_data.match_lineups`;

CREATE TABLE `laliga_data.teams` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'LALIGA teams payload snapshots from BallDontLie LaLiga API');

CREATE TABLE `laliga_data.players` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'LALIGA players payload snapshots from BallDontLie LaLiga API');

CREATE TABLE `laliga_data.rosters` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'LALIGA team roster payload snapshots from BallDontLie LaLiga API');

CREATE TABLE `laliga_data.standings` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'LALIGA standings payload snapshots from BallDontLie LaLiga API');

CREATE TABLE `laliga_data.matches` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'LALIGA matches payload snapshots from BallDontLie LaLiga API');

CREATE TABLE `laliga_data.match_events` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'LALIGA match events payload snapshots from BallDontLie LaLiga API');

CREATE TABLE `laliga_data.match_lineups` (
  ingested_at TIMESTAMP NOT NULL,
  season INT64 NOT NULL,
  entity_id STRING,
  payload STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY season, entity_id
OPTIONS (description = 'LALIGA match lineups payload snapshots from BallDontLie LaLiga API');
