-- BigQuery table schemas for pre-game odds ingestion
-- Run these in BigQuery console to create the required tables

-- ======================================================
-- 1. RAW TABLE: All hourly snapshots (for historical analysis)
-- ======================================================
CREATE TABLE IF NOT EXISTS `graphite-flare-477419-h7.nba_live.pregame_game_odds_raw` (
  snapshot_ts TIMESTAMP NOT NULL,
  game_id INT64 NOT NULL,
  book STRING NOT NULL,
  payload STRING NOT NULL  -- JSON blob with all odds data
)
PARTITION BY DATE(snapshot_ts)
CLUSTER BY game_id, book
OPTIONS (
  description = 'Raw hourly pre-game odds snapshots from BallDontLie API'
);


-- ======================================================
-- 2. FLAT TABLE: Latest pre-game odds per game/book
-- ======================================================
CREATE TABLE IF NOT EXISTS `graphite-flare-477419-h7.nba_live.pregame_game_odds_flat` (
  snapshot_ts TIMESTAMP NOT NULL,
  game_id INT64 NOT NULL,
  book STRING NOT NULL,

  -- Spread
  spread_home FLOAT64,
  spread_away FLOAT64,
  spread_home_odds INT64,
  spread_away_odds INT64,

  -- Total
  total FLOAT64,
  over_odds INT64,
  under_odds INT64,

  -- Moneyline
  moneyline_home_odds INT64,
  moneyline_away_odds INT64
)
CLUSTER BY game_id, book
OPTIONS (
  description = 'Latest pre-game odds per game/book - updated via MERGE'
);


-- ======================================================
-- 3. CLOSING LINES TABLE: Final pre-game odds for ATS tracking
-- ======================================================
CREATE TABLE IF NOT EXISTS `graphite-flare-477419-h7.nba_live.closing_lines` (
  game_id INT64 NOT NULL,
  game_date DATE NOT NULL,
  book STRING NOT NULL,
  captured_at TIMESTAMP NOT NULL,

  -- Spread (the closing spread for ATS tracking)
  spread_home FLOAT64,
  spread_away FLOAT64,
  spread_home_odds INT64,
  spread_away_odds INT64,

  -- Total (closing over/under)
  total FLOAT64,
  over_odds INT64,
  under_odds INT64,

  -- Moneyline (closing moneyline)
  moneyline_home_odds INT64,
  moneyline_away_odds INT64
)
PARTITION BY game_date
CLUSTER BY game_id, book
OPTIONS (
  description = 'Closing lines captured at game tipoff - for ATS tracking and analysis'
);


-- ======================================================
-- USEFUL QUERIES
-- ======================================================

-- Get all closing lines for a specific date
-- SELECT * FROM `graphite-flare-477419-h7.nba_live.closing_lines`
-- WHERE game_date = '2025-01-30'
-- ORDER BY game_id, book;

-- Get pre-game odds movement for a specific game (from raw snapshots)
-- SELECT
--   snapshot_ts,
--   JSON_VALUE(payload, '$.spread_home') as spread_home,
--   JSON_VALUE(payload, '$.total') as total,
--   JSON_VALUE(payload, '$.moneyline_home_odds') as ml_home
-- FROM `graphite-flare-477419-h7.nba_live.pregame_game_odds_raw`
-- WHERE game_id = 12345
-- ORDER BY snapshot_ts;

-- Compare closing line to game result (join with game results table)
-- SELECT
--   c.game_id,
--   c.game_date,
--   c.book,
--   c.spread_home as closing_spread,
--   c.total as closing_total,
--   g.home_score,
--   g.away_score,
--   g.home_score - g.away_score as actual_margin,
--   CASE
--     WHEN g.home_score + c.spread_home > g.away_score THEN 'HOME_COVER'
--     WHEN g.home_score + c.spread_home < g.away_score THEN 'AWAY_COVER'
--     ELSE 'PUSH'
--   END as ats_result,
--   CASE
--     WHEN g.home_score + g.away_score > c.total THEN 'OVER'
--     WHEN g.home_score + g.away_score < c.total THEN 'UNDER'
--     ELSE 'PUSH'
--   END as total_result
-- FROM `graphite-flare-477419-h7.nba_live.closing_lines` c
-- JOIN `graphite-flare-477419-h7.nba_live.game_results` g
--   ON c.game_id = g.game_id
-- WHERE c.game_date = '2025-01-30';
