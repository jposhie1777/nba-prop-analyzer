-- ============================================================
-- 02_epl_team_form.sql
--
-- Builds per-team rolling form metrics from match_team_stats.
-- Parses JSON payloads to extract key stats per team per match.
-- Joins with matches_flat for match dates and scores.
-- Joins with match_card_totals for card metrics.
--
-- Sources:
--   epl_data.match_team_stats   → raw JSON stats per team per match
--   epl_data.matches_flat       → match dates, scores, team IDs
--   epl_data.match_card_totals  → card totals per team per match
-- Output:
--   soccer_data.epl_team_form
-- ============================================================

CREATE OR REPLACE TABLE `graphite-flare-477419-h7.soccer_data.epl_team_form` AS

WITH

-- Parse JSON stats from match_team_stats
parsed AS (
  SELECT
    CAST(JSON_VALUE(payload, '$.teamId') AS INT64)        AS team_id,
    CAST(JSON_VALUE(payload, '$.matchId') AS INT64)       AS match_id,
    JSON_VALUE(payload, '$.side')                         AS side,
    SAFE_CAST(JSON_VALUE(payload, '$.stats.goals') AS FLOAT64)                  AS goals_for,
    SAFE_CAST(JSON_VALUE(payload, '$.stats.goalsConceded') AS FLOAT64)          AS goals_against,
    SAFE_CAST(JSON_VALUE(payload, '$.stats.cornerTaken') AS FLOAT64)            AS corners,
    SAFE_CAST(JSON_VALUE(payload, '$.stats.totalYelCard') AS FLOAT64)           AS yellow_cards,
    SAFE_CAST(JSON_VALUE(payload, '$.stats.totalRedCard') AS FLOAT64)           AS red_cards,
    SAFE_CAST(JSON_VALUE(payload, '$.stats.expectedGoals') AS FLOAT64)          AS xg,
    SAFE_CAST(JSON_VALUE(payload, '$.stats.totalScoringAtt') AS FLOAT64)        AS shots_total,
    SAFE_CAST(JSON_VALUE(payload, '$.stats.ontargetScoringAtt') AS FLOAT64)     AS shots_on_target,
    SAFE_CAST(JSON_VALUE(payload, '$.stats.possessionPercentage') AS FLOAT64)   AS possession,
    season,
    ingested_at
  FROM `graphite-flare-477419-h7.epl_data.match_team_stats`
),

-- Join with matches_flat to get scheduled_time and result
with_dates AS (
  SELECT
    p.*,
    p.ingested_at                                     AS scheduled_time,
    CASE WHEN p.goals_for > p.goals_against THEN 1 ELSE 0 END   AS is_win,
    CASE WHEN p.goals_for = p.goals_against THEN 1 ELSE 0 END   AS is_draw,
    CASE WHEN p.goals_for < p.goals_against THEN 1 ELSE 0 END   AS is_loss,
    CASE WHEN p.goals_for > 0 THEN 1 ELSE 0 END                 AS did_score,
    CASE WHEN p.goals_against = 0 THEN 1 ELSE 0 END             AS clean_sheet,
    CASE WHEN p.goals_for > 0
          AND p.goals_against > 0 THEN 1 ELSE 0 END             AS btts,
    CASE WHEN (p.goals_for + p.goals_against) > 2
         THEN 1 ELSE 0 END                                      AS over_25,
    CASE WHEN (p.goals_for + p.goals_against) > 3
         THEN 1 ELSE 0 END                                      AS over_35,
    COALESCE(p.yellow_cards, 0) + COALESCE(p.red_cards, 0)      AS total_cards
  FROM parsed p
  -- No join to matches_flat — different ID systems don't overlap
  -- ingested_at is used as match date proxy (stats are ingested after each match)
),

-- Rank each team's games most recent first
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY team_id
      ORDER BY scheduled_time DESC
    ) AS game_rank
  FROM with_dates
)

SELECT
  r.team_id,
  s.team_name,
  COUNT(*)                                                AS season_games,
  MAX(r.scheduled_time)                                  AS last_match_date,

  -- ── Season averages ────────────────────────────────────
  ROUND(AVG(r.goals_for), 3)                             AS season_goals_pg,
  ROUND(AVG(r.goals_against), 3)                         AS season_goals_allowed_pg,
  ROUND(AVG(r.xg), 3)                                    AS season_xg_pg,
  ROUND(AVG(r.corners), 3)                               AS season_corners_pg,
  ROUND(AVG(r.total_cards), 3)                           AS season_cards_pg,
  ROUND(AVG(r.is_win), 3)                                AS season_win_rate,
  ROUND(AVG(r.is_draw), 3)                               AS season_draw_rate,
  ROUND(AVG(r.btts), 3)                                  AS season_btts_rate,
  ROUND(AVG(r.over_25), 3)                               AS season_over_25_rate,

  -- ── L3 form ────────────────────────────────────────────
  ROUND(AVG(IF(game_rank<=3, r.goals_for, NULL)), 3)     AS l3_goals_pg,
  ROUND(AVG(IF(game_rank<=3, r.goals_against, NULL)), 3) AS l3_goals_allowed_pg,
  ROUND(AVG(IF(game_rank<=3, r.xg, NULL)), 3)            AS l3_xg_pg,
  ROUND(AVG(IF(game_rank<=3, r.corners, NULL)), 3)       AS l3_corners_pg,
  ROUND(AVG(IF(game_rank<=3, r.total_cards, NULL)), 3)   AS l3_cards_pg,
  ROUND(AVG(IF(game_rank<=3, r.is_win, NULL)), 3)        AS l3_win_rate,
  ROUND(AVG(IF(game_rank<=3, r.is_draw, NULL)), 3)       AS l3_draw_rate,
  ROUND(AVG(IF(game_rank<=3, r.is_loss, NULL)), 3)       AS l3_loss_rate,
  ROUND(AVG(IF(game_rank<=3, r.did_score, NULL)), 3)     AS l3_score_rate,
  ROUND(AVG(IF(game_rank<=3, r.clean_sheet, NULL)), 3)   AS l3_clean_sheet_rate,
  ROUND(AVG(IF(game_rank<=3, r.btts, NULL)), 3)          AS l3_btts_rate,
  ROUND(AVG(IF(game_rank<=3, r.over_25, NULL)), 3)       AS l3_over_25_rate,
  ROUND(AVG(IF(game_rank<=3, r.over_35, NULL)), 3)       AS l3_over_35_rate,
  ROUND(AVG(IF(game_rank<=3, r.shots_total, NULL)), 3)   AS l3_shots_pg,
  ROUND(AVG(IF(game_rank<=3, r.shots_on_target, NULL)), 3) AS l3_sot_pg,

  -- ── L5 form ────────────────────────────────────────────
  ROUND(AVG(IF(game_rank<=5, r.goals_for, NULL)), 3)     AS l5_goals_pg,
  ROUND(AVG(IF(game_rank<=5, r.goals_against, NULL)), 3) AS l5_goals_allowed_pg,
  ROUND(AVG(IF(game_rank<=5, r.xg, NULL)), 3)            AS l5_xg_pg,
  ROUND(AVG(IF(game_rank<=5, r.corners, NULL)), 3)       AS l5_corners_pg,
  ROUND(AVG(IF(game_rank<=5, r.total_cards, NULL)), 3)   AS l5_cards_pg,
  ROUND(AVG(IF(game_rank<=5, r.is_win, NULL)), 3)        AS l5_win_rate,
  ROUND(AVG(IF(game_rank<=5, r.is_draw, NULL)), 3)       AS l5_draw_rate,
  ROUND(AVG(IF(game_rank<=5, r.is_loss, NULL)), 3)       AS l5_loss_rate,
  ROUND(AVG(IF(game_rank<=5, r.did_score, NULL)), 3)     AS l5_score_rate,
  ROUND(AVG(IF(game_rank<=5, r.clean_sheet, NULL)), 3)   AS l5_clean_sheet_rate,
  ROUND(AVG(IF(game_rank<=5, r.btts, NULL)), 3)          AS l5_btts_rate,
  ROUND(AVG(IF(game_rank<=5, r.over_25, NULL)), 3)       AS l5_over_25_rate,
  ROUND(AVG(IF(game_rank<=5, r.over_35, NULL)), 3)       AS l5_over_35_rate,
  ROUND(AVG(IF(game_rank<=5, r.shots_total, NULL)), 3)   AS l5_shots_pg,
  ROUND(AVG(IF(game_rank<=5, r.shots_on_target, NULL)), 3) AS l5_sot_pg,

  -- ── L10 form ───────────────────────────────────────────
  ROUND(AVG(IF(game_rank<=10, r.goals_for, NULL)), 3)    AS l10_goals_pg,
  ROUND(AVG(IF(game_rank<=10, r.goals_against, NULL)), 3) AS l10_goals_allowed_pg,
  ROUND(AVG(IF(game_rank<=10, r.xg, NULL)), 3)           AS l10_xg_pg,
  ROUND(AVG(IF(game_rank<=10, r.corners, NULL)), 3)       AS l10_corners_pg,
  ROUND(AVG(IF(game_rank<=10, r.total_cards, NULL)), 3)  AS l10_cards_pg,
  ROUND(AVG(IF(game_rank<=10, r.is_win, NULL)), 3)       AS l10_win_rate,
  ROUND(AVG(IF(game_rank<=10, r.btts, NULL)), 3)         AS l10_btts_rate,
  ROUND(AVG(IF(game_rank<=10, r.over_25, NULL)), 3)      AS l10_over_25_rate,

  CURRENT_TIMESTAMP()                                    AS updated_at

FROM ranked r
LEFT JOIN `graphite-flare-477419-h7.epl_data.standings_flat` s
  ON r.team_id = s.team_id
GROUP BY r.team_id, s.team_name