-- BigQuery table for precomputed ATP matchup compare payloads.
-- Override table name via ATP_COMPARE_CACHE_TABLE env var in API runtime if needed.

CREATE TABLE IF NOT EXISTS `atp_data.atp_matchup_compare_cache` (
  cache_key STRING NOT NULL,
  player_ids STRING NOT NULL,
  season INT64,
  seasons_back INT64,
  start_season INT64,
  end_season INT64,
  surface STRING,
  tournament_id INT64,
  match_id STRING,
  payload_json STRING NOT NULL,
  computed_at TIMESTAMP NOT NULL,
  expires_at TIMESTAMP
)
PARTITION BY DATE(computed_at)
CLUSTER BY cache_key, tournament_id;

-- Optional cleanup (run daily)
-- DELETE FROM `atp_data.atp_matchup_compare_cache`
-- WHERE expires_at IS NOT NULL AND expires_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);
