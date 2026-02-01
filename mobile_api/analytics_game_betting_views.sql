-- Game betting analytics views for moneyline, spread, and totals
-- NOTE: If your odds/lines columns have different names, update them below.

CREATE OR REPLACE VIEW `nba_goa_data.v_game_betting_base` AS
SELECT
  game_id,
  season,
  game_date,
  start_time_est,
  status,
  is_final,
  home_team_abbr,
  away_team_abbr,
  home_score_final,
  away_score_final,
  -- Odds/lines (update column names if needed)
  home_moneyline,
  away_moneyline,
  spread_home,
  spread_away,
  total_line,
  -- Derived scoring
  CASE
    WHEN home_score_final IS NULL OR away_score_final IS NULL THEN NULL
    ELSE home_score_final - away_score_final
  END AS home_margin,
  CASE
    WHEN home_score_final IS NULL OR away_score_final IS NULL THEN NULL
    ELSE home_score_final + away_score_final
  END AS total_points,
  -- Moneyline result
  CASE
    WHEN home_score_final IS NULL OR away_score_final IS NULL THEN NULL
    WHEN home_score_final > away_score_final THEN 'HOME'
    WHEN home_score_final < away_score_final THEN 'AWAY'
    ELSE 'PUSH'
  END AS moneyline_result,
  -- Spread result (home spread applied to home score)
  CASE
    WHEN spread_home IS NULL
      OR home_score_final IS NULL
      OR away_score_final IS NULL THEN NULL
    WHEN home_score_final + spread_home > away_score_final THEN 'HOME'
    WHEN home_score_final + spread_home < away_score_final THEN 'AWAY'
    ELSE 'PUSH'
  END AS spread_result,
  -- Total result
  CASE
    WHEN total_line IS NULL
      OR home_score_final IS NULL
      OR away_score_final IS NULL THEN NULL
    WHEN home_score_final + away_score_final > total_line THEN 'OVER'
    WHEN home_score_final + away_score_final < total_line THEN 'UNDER'
    ELSE 'PUSH'
  END AS total_result
FROM `nba_goa_data.games`;

CREATE OR REPLACE VIEW `nba_goa_data.v_game_betting_team_form` AS
WITH base AS (
  SELECT *
  FROM `nba_goa_data.v_game_betting_base`
  WHERE is_final = TRUE
),
team_games AS (
  SELECT
    game_id,
    game_date,
    home_team_abbr AS team_abbr,
    away_team_abbr AS opponent_abbr,
    'HOME' AS side,
    home_score_final AS team_score,
    away_score_final AS opponent_score,
    moneyline_result,
    spread_result,
    total_result
  FROM base
  UNION ALL
  SELECT
    game_id,
    game_date,
    away_team_abbr AS team_abbr,
    home_team_abbr AS opponent_abbr,
    'AWAY' AS side,
    away_score_final AS team_score,
    home_score_final AS opponent_score,
    moneyline_result,
    spread_result,
    total_result
  FROM base
),
team_rows AS (
  SELECT
    game_id,
    game_date,
    team_abbr,
    opponent_abbr,
    side,
    team_score,
    opponent_score,
    CASE
      WHEN moneyline_result = side THEN 1
      WHEN moneyline_result = 'PUSH' THEN 0.5
      ELSE 0
    END AS ml_win,
    CASE
      WHEN spread_result = side THEN 1
      WHEN spread_result = 'PUSH' THEN 0.5
      ELSE 0
    END AS ats_win,
    CASE
      WHEN total_result = 'OVER' THEN 1
      WHEN total_result = 'PUSH' THEN 0.5
      ELSE 0
    END AS over_hit
  FROM team_games
)
SELECT
  game_id,
  game_date,
  team_abbr,
  opponent_abbr,
  side,
  team_score,
  opponent_score,
  team_score - opponent_score AS team_margin,
  COUNT(1) OVER (
    PARTITION BY team_abbr
    ORDER BY game_date
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS games_l10,
  AVG(ml_win) OVER (
    PARTITION BY team_abbr
    ORDER BY game_date
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS win_pct_l10,
  AVG(ats_win) OVER (
    PARTITION BY team_abbr
    ORDER BY game_date
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS ats_pct_l10,
  AVG(over_hit) OVER (
    PARTITION BY team_abbr
    ORDER BY game_date
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS over_pct_l10,
  AVG(team_score - opponent_score) OVER (
    PARTITION BY team_abbr
    ORDER BY game_date
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS avg_margin_l10
FROM team_rows;

CREATE OR REPLACE VIEW `nba_goa_data.v_game_betting_board` AS
SELECT
  base.*,
  home_form.games_l10 AS home_games_l10,
  home_form.win_pct_l10 AS home_win_pct_l10,
  home_form.ats_pct_l10 AS home_ats_pct_l10,
  home_form.over_pct_l10 AS home_over_pct_l10,
  home_form.avg_margin_l10 AS home_avg_margin_l10,
  away_form.games_l10 AS away_games_l10,
  away_form.win_pct_l10 AS away_win_pct_l10,
  away_form.ats_pct_l10 AS away_ats_pct_l10,
  away_form.over_pct_l10 AS away_over_pct_l10,
  away_form.avg_margin_l10 AS away_avg_margin_l10
FROM `nba_goa_data.v_game_betting_base` AS base
LEFT JOIN `nba_goa_data.v_game_betting_team_form` AS home_form
  ON base.game_id = home_form.game_id
  AND base.home_team_abbr = home_form.team_abbr
LEFT JOIN `nba_goa_data.v_game_betting_team_form` AS away_form
  ON base.game_id = away_form.game_id
  AND base.away_team_abbr = away_form.team_abbr;
