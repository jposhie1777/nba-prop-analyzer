-- Precompute ATP player metrics so compare endpoints avoid per-request raw match scans.
-- Suggested schedule: every 1-6 hours.

CREATE TABLE IF NOT EXISTS `atp_data.atp_player_compare_metrics` (
  player_id INT64 NOT NULL,
  player_name STRING,
  surface_key STRING NOT NULL,
  matches_total INT64,
  wins_total INT64,
  overall_win_rate FLOAT64,
  recent_matches INT64,
  recent_wins INT64,
  recent_win_rate FLOAT64,
  straight_sets_win_rate FLOAT64,
  tiebreak_rate FLOAT64,
  form_score FLOAT64,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(updated_at)
CLUSTER BY player_id, surface_key;

CREATE OR REPLACE TABLE `atp_data.atp_player_compare_metrics`
PARTITION BY DATE(updated_at)
CLUSTER BY player_id, surface_key AS
WITH base AS (
  SELECT
    match_id,
    scheduled_time,
    LOWER(COALESCE(surface, 'unknown')) AS surface_key,
    player1_id AS player_id,
    player1_name AS player_name,
    player2_id AS opponent_id,
    IF(winner_id = player1_id, 1, 0) AS win,
    score
  FROM `atp_data.matches`
  WHERE player1_id IS NOT NULL AND player2_id IS NOT NULL AND winner_id IS NOT NULL

  UNION ALL

  SELECT
    match_id,
    scheduled_time,
    LOWER(COALESCE(surface, 'unknown')) AS surface_key,
    player2_id AS player_id,
    player2_name AS player_name,
    player1_id AS opponent_id,
    IF(winner_id = player2_id, 1, 0) AS win,
    score
  FROM `atp_data.matches`
  WHERE player1_id IS NOT NULL AND player2_id IS NOT NULL AND winner_id IS NOT NULL
),
with_flags AS (
  SELECT
    *,
    IF(REGEXP_CONTAINS(score, r'(^|\s)6-[0-4](\s|$)|(^|\s)7-[0-5](\s|$)'), 1, 0) AS has_clean_set,
    IF(REGEXP_CONTAINS(score, r'7-6|6-7'), 1, 0) AS has_tiebreak
  FROM base
),
surface_rollup AS (
  SELECT
    player_id,
    ANY_VALUE(player_name) AS player_name,
    surface_key,
    COUNT(*) AS matches_total,
    SUM(win) AS wins_total,
    SAFE_DIVIDE(SUM(win), COUNT(*)) AS overall_win_rate,
    SUM(has_clean_set * win) AS straight_sets_wins,
    SUM(has_tiebreak) AS tiebreak_matches
  FROM with_flags
  GROUP BY player_id, surface_key
),
recent_surface AS (
  SELECT
    player_id,
    surface_key,
    COUNT(*) AS recent_matches,
    SUM(win) AS recent_wins
  FROM (
    SELECT
      player_id,
      surface_key,
      win,
      ROW_NUMBER() OVER (PARTITION BY player_id, surface_key ORDER BY scheduled_time DESC, match_id DESC) AS rn
    FROM with_flags
  )
  WHERE rn <= 20
  GROUP BY player_id, surface_key
),
all_rollup AS (
  SELECT
    player_id,
    ANY_VALUE(player_name) AS player_name,
    'all' AS surface_key,
    COUNT(*) AS matches_total,
    SUM(win) AS wins_total,
    SAFE_DIVIDE(SUM(win), COUNT(*)) AS overall_win_rate,
    SUM(has_clean_set * win) AS straight_sets_wins,
    SUM(has_tiebreak) AS tiebreak_matches
  FROM with_flags
  GROUP BY player_id
),
recent_all AS (
  SELECT
    player_id,
    'all' AS surface_key,
    COUNT(*) AS recent_matches,
    SUM(win) AS recent_wins
  FROM (
    SELECT
      player_id,
      win,
      ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY scheduled_time DESC, match_id DESC) AS rn
    FROM with_flags
  )
  WHERE rn <= 20
  GROUP BY player_id
),
combined AS (
  SELECT * FROM surface_rollup
  UNION ALL
  SELECT * FROM all_rollup
),
combined_recent AS (
  SELECT * FROM recent_surface
  UNION ALL
  SELECT * FROM recent_all
)
SELECT
  c.player_id,
  c.player_name,
  c.surface_key,
  c.matches_total,
  c.wins_total,
  c.overall_win_rate,
  COALESCE(r.recent_matches, 0) AS recent_matches,
  COALESCE(r.recent_wins, 0) AS recent_wins,
  SAFE_DIVIDE(COALESCE(r.recent_wins, 0), NULLIF(COALESCE(r.recent_matches, 0), 0)) AS recent_win_rate,
  SAFE_DIVIDE(c.straight_sets_wins, NULLIF(c.wins_total, 0)) AS straight_sets_win_rate,
  SAFE_DIVIDE(c.tiebreak_matches, NULLIF(c.matches_total, 0)) AS tiebreak_rate,
  (
    COALESCE(SAFE_DIVIDE(COALESCE(r.recent_wins, 0), NULLIF(COALESCE(r.recent_matches, 0), 0)), 0) * 0.65
    + COALESCE(SAFE_DIVIDE(c.straight_sets_wins, NULLIF(c.wins_total, 0)), 0) * 0.20
    + (1 - COALESCE(SAFE_DIVIDE(c.tiebreak_matches, NULLIF(c.matches_total, 0)), 0)) * 0.15
  ) AS form_score,
  CURRENT_TIMESTAMP() AS updated_at
FROM combined c
LEFT JOIN combined_recent r
  ON c.player_id = r.player_id
 AND c.surface_key = r.surface_key;
