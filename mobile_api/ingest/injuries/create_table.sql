-- BigQuery table for player injuries
-- Run this in BigQuery console to create the table

CREATE TABLE IF NOT EXISTS `nba_live.player_injuries` (
    -- Metadata
    injury_id INT64,
    run_ts TIMESTAMP,
    ingested_at TIMESTAMP,

    -- Player info
    player_id INT64 NOT NULL,
    player_first_name STRING,
    player_last_name STRING,
    player_name STRING NOT NULL,

    -- Team info
    team_id INT64 NOT NULL,
    team_abbreviation STRING,
    team_name STRING,

    -- Injury details
    status STRING,           -- "Out", "Questionable", "Doubtful", "Day-To-Day", "Probable"
    injury_type STRING,      -- "Ankle", "Knee", "Rest", "Illness", etc.
    report_date DATE,        -- When the injury was reported
    return_date DATE         -- Expected return date (if available)
)
OPTIONS (
    description = 'Current NBA player injuries from BallDontLie API. Refreshed periodically.'
);

-- Create index on commonly queried columns
-- Note: BigQuery doesn't support traditional indexes, but clustering can help
-- If you want to optimize queries, you can recreate the table with clustering:
--
-- CREATE OR REPLACE TABLE `nba_live.player_injuries`
-- CLUSTER BY team_id, status
-- AS SELECT * FROM `nba_live.player_injuries`;
