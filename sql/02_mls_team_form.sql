-- ============================================================
-- 02_mls_team_form.sql
--
-- Builds per-team rolling form metrics from fact_team_match.
-- This replaces team_master_metrics which is currently empty.
--
-- Source: mls_data.fact_team_match
-- Output: soccer_data.mls_team_form
-- ============================================================

CREATE OR REPLACE TABLE `graphite-flare-477419-h7.soccer_data.mls_team_form` AS

WITH base AS (
  -- One row per team per game, current season only
  SELECT
    team_id,
    team_name,
    team_short_name,
    team_three_letter_code,
    match_id,
    match_date,
    season,
    team_role,                                        -- 'home' or 'away'
    CAST(goals AS FLOAT64)                            AS goals_for,
    CAST(goals_conceded AS FLOAT64)                   AS goals_against,
    CAST(corners AS FLOAT64)                          AS corners,
    CAST(yellow_cards AS FLOAT64)                     AS yellow_cards,
    CAST(red_cards AS FLOAT64)                        AS red_cards,
    CAST(yellow_red_cards AS FLOAT64)                 AS yellow_red_cards,
    CAST(shots_total AS FLOAT64)                      AS shots_total,
    CAST(shots_on_target AS FLOAT64)                  AS shots_on_target,
    CAST(xg AS FLOAT64)                               AS xg,
    CAST(fouls AS FLOAT64)                            AS fouls,
    CASE WHEN goals > goals_conceded THEN 1 ELSE 0 END AS is_win,
    CASE WHEN goals = goals_conceded THEN 1 ELSE 0 END AS is_draw,
    CASE WHEN goals < goals_conceded THEN 1 ELSE 0 END AS is_loss,
    CASE WHEN goals > 0 THEN 1 ELSE 0 END             AS did_score,
    CASE WHEN goals_conceded = 0 THEN 1 ELSE 0 END    AS clean_sheet,
    CASE WHEN goals > 0 AND goals_conceded > 0
         THEN 1 ELSE 0 END                            AS btts,
    CASE WHEN (goals + goals_conceded) > 2
         THEN 1 ELSE 0 END                            AS over_25,
    CASE WHEN (goals + goals_conceded) > 3
         THEN 1 ELSE 0 END                            AS over_35,
    -- Total cards (yellow + red + yellow-red)
    CAST(yellow_cards AS FLOAT64)
      + CAST(red_cards AS FLOAT64)
      + CAST(yellow_red_cards AS FLOAT64)             AS total_cards
  FROM `graphite-flare-477419-h7.mls_data.fact_team_match`
  WHERE scope = 'total'                               -- match totals only, not halftime splits
),

ranked AS (
  -- Rank each team's games most recent first
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY team_id
      ORDER BY match_date DESC
    ) AS game_rank
  FROM base
)

SELECT
  team_id,
  team_name,
  team_short_name,
  team_three_letter_code,
  MAX(season)                                         AS current_season,
  COUNT(*)                                            AS season_games,
  MAX(match_date)                                     AS last_match_date,

  -- ── Season averages ───────────────────────────────────
  ROUND(AVG(goals_for), 3)                            AS season_goals_pg,
  ROUND(AVG(goals_against), 3)                        AS season_goals_allowed_pg,
  ROUND(AVG(xg), 3)                                   AS season_xg_pg,
  ROUND(AVG(corners), 3)                              AS season_corners_pg,
  ROUND(AVG(total_cards), 3)                          AS season_cards_pg,
  ROUND(AVG(is_win), 3)                               AS season_win_rate,
  ROUND(AVG(is_draw), 3)                              AS season_draw_rate,
  ROUND(AVG(btts), 3)                                 AS season_btts_rate,
  ROUND(AVG(over_25), 3)                              AS season_over_25_rate,

  -- ── L3 form ───────────────────────────────────────────
  ROUND(AVG(IF(game_rank <= 3, goals_for, NULL)), 3)          AS l3_goals_pg,
  ROUND(AVG(IF(game_rank <= 3, goals_against, NULL)), 3)      AS l3_goals_allowed_pg,
  ROUND(AVG(IF(game_rank <= 3, xg, NULL)), 3)                 AS l3_xg_pg,
  ROUND(AVG(IF(game_rank <= 3, corners, NULL)), 3)            AS l3_corners_pg,
  ROUND(AVG(IF(game_rank <= 3, total_cards, NULL)), 3)        AS l3_cards_pg,
  ROUND(AVG(IF(game_rank <= 3, is_win, NULL)), 3)             AS l3_win_rate,
  ROUND(AVG(IF(game_rank <= 3, is_draw, NULL)), 3)            AS l3_draw_rate,
  ROUND(AVG(IF(game_rank <= 3, is_loss, NULL)), 3)            AS l3_loss_rate,
  ROUND(AVG(IF(game_rank <= 3, did_score, NULL)), 3)          AS l3_score_rate,
  ROUND(AVG(IF(game_rank <= 3, clean_sheet, NULL)), 3)        AS l3_clean_sheet_rate,
  ROUND(AVG(IF(game_rank <= 3, btts, NULL)), 3)               AS l3_btts_rate,
  ROUND(AVG(IF(game_rank <= 3, over_25, NULL)), 3)            AS l3_over_25_rate,
  ROUND(AVG(IF(game_rank <= 3, over_35, NULL)), 3)            AS l3_over_35_rate,
  ROUND(AVG(IF(game_rank <= 3, shots_total, NULL)), 3)        AS l3_shots_pg,
  ROUND(AVG(IF(game_rank <= 3, shots_on_target, NULL)), 3)    AS l3_sot_pg,

  -- ── L5 form ───────────────────────────────────────────
  ROUND(AVG(IF(game_rank <= 5, goals_for, NULL)), 3)          AS l5_goals_pg,
  ROUND(AVG(IF(game_rank <= 5, goals_against, NULL)), 3)      AS l5_goals_allowed_pg,
  ROUND(AVG(IF(game_rank <= 5, xg, NULL)), 3)                 AS l5_xg_pg,
  ROUND(AVG(IF(game_rank <= 5, corners, NULL)), 3)            AS l5_corners_pg,
  ROUND(AVG(IF(game_rank <= 5, total_cards, NULL)), 3)        AS l5_cards_pg,
  ROUND(AVG(IF(game_rank <= 5, is_win, NULL)), 3)             AS l5_win_rate,
  ROUND(AVG(IF(game_rank <= 5, is_draw, NULL)), 3)            AS l5_draw_rate,
  ROUND(AVG(IF(game_rank <= 5, is_loss, NULL)), 3)            AS l5_loss_rate,
  ROUND(AVG(IF(game_rank <= 5, did_score, NULL)), 3)          AS l5_score_rate,
  ROUND(AVG(IF(game_rank <= 5, clean_sheet, NULL)), 3)        AS l5_clean_sheet_rate,
  ROUND(AVG(IF(game_rank <= 5, btts, NULL)), 3)               AS l5_btts_rate,
  ROUND(AVG(IF(game_rank <= 5, over_25, NULL)), 3)            AS l5_over_25_rate,
  ROUND(AVG(IF(game_rank <= 5, over_35, NULL)), 3)            AS l5_over_35_rate,
  ROUND(AVG(IF(game_rank <= 5, shots_total, NULL)), 3)        AS l5_shots_pg,
  ROUND(AVG(IF(game_rank <= 5, shots_on_target, NULL)), 3)    AS l5_sot_pg,

  -- ── L10 form ──────────────────────────────────────────
  ROUND(AVG(IF(game_rank <= 10, goals_for, NULL)), 3)         AS l10_goals_pg,
  ROUND(AVG(IF(game_rank <= 10, goals_against, NULL)), 3)     AS l10_goals_allowed_pg,
  ROUND(AVG(IF(game_rank <= 10, xg, NULL)), 3)                AS l10_xg_pg,
  ROUND(AVG(IF(game_rank <= 10, corners, NULL)), 3)           AS l10_corners_pg,
  ROUND(AVG(IF(game_rank <= 10, total_cards, NULL)), 3)       AS l10_cards_pg,
  ROUND(AVG(IF(game_rank <= 10, is_win, NULL)), 3)            AS l10_win_rate,
  ROUND(AVG(IF(game_rank <= 10, btts, NULL)), 3)              AS l10_btts_rate,
  ROUND(AVG(IF(game_rank <= 10, over_25, NULL)), 3)           AS l10_over_25_rate,

  CURRENT_TIMESTAMP()                                         AS updated_at

FROM ranked
GROUP BY team_id, team_name, team_short_name, team_three_letter_code
