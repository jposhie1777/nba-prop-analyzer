-- ============================================================
-- 03_epl_betting_analytics.sql
--
-- Builds the final EPL betting analytics table.
-- Joins FanDuel markets + deep links with team form metrics.
--
-- Sources:
--   soccer_data.raw_fanduel_soccer_markets → odds + deep links
--   soccer_data.epl_team_name_map          → FD name → team_id (step 1)
--   soccer_data.epl_team_form              → form metrics (step 2)
--   soccer_data.odds_lines                 → multi-book consensus
-- Output:
--   soccer_data.epl_betting_analytics
-- ============================================================

CREATE OR REPLACE TABLE `graphite-flare-477419-h7.soccer_data.epl_betting_analytics`
PARTITION BY DATE(event_start_ts)
CLUSTER BY market_type, home_team, away_team
AS

WITH

-- ── Step 1: Deduplicate FanDuel EPL markets ───────────────────────────────
fd_raw AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY event_id, market_id, selection_id
        ORDER BY scraped_at DESC
      ) AS rn
    FROM `graphite-flare-477419-h7.soccer_data.raw_fanduel_soccer_markets`
    WHERE league = 'EPL'
      AND market_type != 'futures'
      AND market_status = 'OPEN'
      AND runner_status = 'ACTIVE'
  )
  WHERE rn = 1
),

-- ── Step 2: Add timestamps + implied probability ──────────────────────────
fd_priced AS (
  SELECT
    *,
    TIMESTAMP(event_start)                            AS event_start_ts,
    CASE
      WHEN SAFE_CAST(odds_american AS FLOAT64) > 0
        THEN 100.0 / (SAFE_CAST(odds_american AS FLOAT64) + 100)
      WHEN SAFE_CAST(odds_american AS FLOAT64) < 0
        THEN ABS(SAFE_CAST(odds_american AS FLOAT64)) / (ABS(SAFE_CAST(odds_american AS FLOAT64)) + 100)
    END                                               AS implied_probability
  FROM fd_raw
),

-- ── Step 3: No-vig calculation for binary markets ─────────────────────────
paired AS (
  SELECT
    a.event_id, a.market_id, a.selection_id,
    b.implied_probability                             AS opposite_implied_probability,
    SAFE_DIVIDE(
      a.implied_probability,
      a.implied_probability + b.implied_probability
    )                                                 AS no_vig_probability,
    (a.implied_probability + b.implied_probability) - 1 AS market_hold
  FROM fd_priced a
  LEFT JOIN fd_priced b
    ON a.event_id = b.event_id
    AND a.market_id = b.market_id
    AND a.selection_id != b.selection_id
  WHERE (
    SELECT COUNT(DISTINCT selection_id)
    FROM fd_raw x
    WHERE x.event_id = a.event_id AND x.market_id = a.market_id
  ) = 2
),

-- ── Step 4: Price rank within each market ─────────────────────────────────
price_ranked AS (
  SELECT event_id, market_id, selection_id,
    ROW_NUMBER() OVER (
      PARTITION BY event_id, market_id
      ORDER BY odds_decimal DESC
    ) AS price_rank_in_market
  FROM fd_priced
),

-- ── Step 5: Multi-book consensus from Oddspedia odds_lines ────────────────
book_consensus AS (
  SELECT
    game, start_time_et, market, outcome,
    CAST(line AS STRING)                              AS line_str,
    COUNT(DISTINCT bookmaker)                         AS books_reporting,
    AVG(price)                                        AS consensus_avg_price,
    MIN(price)                                        AS consensus_min_price,
    MAX(price)                                        AS consensus_max_price,
    AVG(CASE
      WHEN price > 0 THEN 100.0 / (price + 100)
      WHEN price < 0 THEN ABS(price) / (ABS(price) + 100.0)
    END)                                              AS consensus_implied_prob
  FROM `graphite-flare-477419-h7.soccer_data.odds_lines`
  WHERE league = 'EPL'
  GROUP BY game, start_time_et, market, outcome, line_str
)

-- ── Final SELECT ──────────────────────────────────────────────────────────
SELECT

  -- Identity
  f.scraped_at,
  f.event_start_ts,
  DATE(f.event_start_ts)                              AS event_date,
  'EPL'                                               AS league,
  f.event_id                                          AS fd_event_id,
  CONCAT(f.home_team, ' vs ', f.away_team)            AS game,
  f.home_team,
  f.away_team,

  -- Market
  f.market_id                                         AS fd_market_id,
  f.market_name,
  f.market_type,
  f.market_type_raw,
  f.selection_id                                      AS fd_selection_id,
  f.selection_name,
  f.handicap,
  f.turn_in_play,
  f.inplay,

  -- FanDuel odds
  f.odds_decimal,
  SAFE_CAST(f.odds_american AS INT64)                 AS odds_american,
  ROUND(f.implied_probability, 6)                     AS implied_probability,
  ROUND(p.opposite_implied_probability, 6)            AS opposite_implied_probability,
  ROUND(p.no_vig_probability, 6)                      AS no_vig_probability,
  ROUND(p.market_hold, 6)                             AS market_hold,
  pr.price_rank_in_market,
  pr.price_rank_in_market = 1                         AS is_best_price,

  -- Deep links
  f.deep_link                                         AS fd_deep_link,
  CONCAT(
    'fanduelsportsbook://launch?deepLink=addToBetslip',
    '%3FmarketId%5B%5D=', f.market_id,
    '&selectionId%5B%5D=', f.selection_id
  )                                                   AS fd_parlay_deep_link,

  -- Multi-book consensus
  c.books_reporting,
  c.consensus_avg_price,
  c.consensus_min_price,
  c.consensus_max_price,
  ROUND(c.consensus_implied_prob, 6)                  AS consensus_implied_prob,
  ROUND(
    COALESCE(p.no_vig_probability, f.implied_probability)
    - COALESCE(c.consensus_implied_prob, f.implied_probability),
  6)                                                  AS probability_vs_consensus,

  -- Home team identity
  hmap.team_id                                        AS home_team_id,
  hmap.bq_team_name                                   AS home_team_bq_name,

  -- Home team form
  h.season_games                                      AS home_season_games,
  h.season_goals_pg                                   AS home_season_goals_pg,
  h.season_goals_allowed_pg                           AS home_season_goals_allowed_pg,
  h.season_win_rate                                   AS home_season_win_rate,
  h.season_btts_rate                                  AS home_season_btts_rate,
  h.season_over_25_rate                               AS home_season_over_25_rate,
  h.l3_goals_pg                                       AS home_l3_goals_pg,
  h.l3_goals_allowed_pg                               AS home_l3_goals_allowed_pg,
  h.l3_xg_pg                                          AS home_l3_xg_pg,
  h.l3_corners_pg                                     AS home_l3_corners_pg,
  h.l3_cards_pg                                       AS home_l3_cards_pg,
  h.l3_win_rate                                       AS home_l3_win_rate,
  h.l3_draw_rate                                      AS home_l3_draw_rate,
  h.l3_loss_rate                                      AS home_l3_loss_rate,
  h.l3_score_rate                                     AS home_l3_score_rate,
  h.l3_clean_sheet_rate                               AS home_l3_clean_sheet_rate,
  h.l3_btts_rate                                      AS home_l3_btts_rate,
  h.l3_over_25_rate                                   AS home_l3_over_25_rate,
  h.l3_over_35_rate                                   AS home_l3_over_35_rate,
  h.l5_goals_pg                                       AS home_l5_goals_pg,
  h.l5_goals_allowed_pg                               AS home_l5_goals_allowed_pg,
  h.l5_xg_pg                                          AS home_l5_xg_pg,
  h.l5_corners_pg                                     AS home_l5_corners_pg,
  h.l5_cards_pg                                       AS home_l5_cards_pg,
  h.l5_win_rate                                       AS home_l5_win_rate,
  h.l5_draw_rate                                      AS home_l5_draw_rate,
  h.l5_loss_rate                                      AS home_l5_loss_rate,
  h.l5_score_rate                                     AS home_l5_score_rate,
  h.l5_clean_sheet_rate                               AS home_l5_clean_sheet_rate,
  h.l5_btts_rate                                      AS home_l5_btts_rate,
  h.l5_over_25_rate                                   AS home_l5_over_25_rate,
  h.l5_over_35_rate                                   AS home_l5_over_35_rate,
  h.l5_shots_pg                                       AS home_l5_shots_pg,
  h.l5_sot_pg                                         AS home_l5_sot_pg,
  h.l10_goals_pg                                      AS home_l10_goals_pg,
  h.l10_goals_allowed_pg                              AS home_l10_goals_allowed_pg,
  h.l10_win_rate                                      AS home_l10_win_rate,
  h.l10_btts_rate                                     AS home_l10_btts_rate,

  -- Away team identity
  amap.team_id                                        AS away_team_id,
  amap.bq_team_name                                   AS away_team_bq_name,

  -- Away team form
  a.season_games                                      AS away_season_games,
  a.season_goals_pg                                   AS away_season_goals_pg,
  a.season_goals_allowed_pg                           AS away_season_goals_allowed_pg,
  a.season_win_rate                                   AS away_season_win_rate,
  a.season_btts_rate                                  AS away_season_btts_rate,
  a.season_over_25_rate                               AS away_season_over_25_rate,
  a.l3_goals_pg                                       AS away_l3_goals_pg,
  a.l3_goals_allowed_pg                               AS away_l3_goals_allowed_pg,
  a.l3_xg_pg                                          AS away_l3_xg_pg,
  a.l3_corners_pg                                     AS away_l3_corners_pg,
  a.l3_cards_pg                                       AS away_l3_cards_pg,
  a.l3_win_rate                                       AS away_l3_win_rate,
  a.l3_draw_rate                                      AS away_l3_draw_rate,
  a.l3_loss_rate                                      AS away_l3_loss_rate,
  a.l3_score_rate                                     AS away_l3_score_rate,
  a.l3_clean_sheet_rate                               AS away_l3_clean_sheet_rate,
  a.l3_btts_rate                                      AS away_l3_btts_rate,
  a.l3_over_25_rate                                   AS away_l3_over_25_rate,
  a.l3_over_35_rate                                   AS away_l3_over_35_rate,
  a.l5_goals_pg                                       AS away_l5_goals_pg,
  a.l5_goals_allowed_pg                               AS away_l5_goals_allowed_pg,
  a.l5_xg_pg                                          AS away_l5_xg_pg,
  a.l5_corners_pg                                     AS away_l5_corners_pg,
  a.l5_cards_pg                                       AS away_l5_cards_pg,
  a.l5_win_rate                                       AS away_l5_win_rate,
  a.l5_draw_rate                                      AS away_l5_draw_rate,
  a.l5_loss_rate                                      AS away_l5_loss_rate,
  a.l5_score_rate                                     AS away_l5_score_rate,
  a.l5_clean_sheet_rate                               AS away_l5_clean_sheet_rate,
  a.l5_btts_rate                                      AS away_l5_btts_rate,
  a.l5_over_25_rate                                   AS away_l5_over_25_rate,
  a.l5_over_35_rate                                   AS away_l5_over_35_rate,
  a.l5_shots_pg                                       AS away_l5_shots_pg,
  a.l5_sot_pg                                         AS away_l5_sot_pg,
  a.l10_goals_pg                                      AS away_l10_goals_pg,
  a.l10_goals_allowed_pg                              AS away_l10_goals_allowed_pg,
  a.l10_win_rate                                      AS away_l10_win_rate,
  a.l10_btts_rate                                     AS away_l10_btts_rate,

  -- ── Model signals ──────────────────────────────────────────────────────
  ROUND((
    COALESCE(h.l5_goals_pg, 0) + COALESCE(a.l5_goals_allowed_pg, 0) +
    COALESCE(a.l5_goals_pg, 0) + COALESCE(h.l5_goals_allowed_pg, 0)
  ) / 2.0, 3)                                         AS model_expected_total_goals,

  ROUND((COALESCE(h.l5_xg_pg, 0) + COALESCE(a.l5_xg_pg, 0)) / 2.0, 3)
                                                       AS model_xg_total,

  ROUND(COALESCE(h.l5_score_rate, 0) * COALESCE(a.l5_score_rate, 0), 3)
                                                       AS model_btts_probability,

  ROUND(COALESCE(h.l5_clean_sheet_rate, 0) * (1 - COALESCE(a.l5_score_rate, 0)), 3)
                                                       AS model_home_clean_sheet_prob,

  ROUND(COALESCE(h.l5_corners_pg, 0) + COALESCE(a.l5_corners_pg, 0), 2)
                                                       AS model_expected_corners,

  ROUND(COALESCE(h.l5_cards_pg, 0) + COALESCE(a.l5_cards_pg, 0), 2)
                                                       AS model_expected_cards,

  ROUND(COALESCE(h.l5_win_rate, 0) - COALESCE(a.l5_win_rate, 0), 3)
                                                       AS model_home_win_form_edge,

  -- Totals line edge
  CASE
    WHEN f.market_type IN ('totals', 'halftime_totals', 'team_totals')
      AND f.handicap IS NOT NULL
    THEN ROUND(CASE
      WHEN LOWER(f.selection_name) LIKE '%over%'
        THEN (
          COALESCE(h.l5_goals_pg,0) + COALESCE(a.l5_goals_allowed_pg,0) +
          COALESCE(a.l5_goals_pg,0) + COALESCE(h.l5_goals_allowed_pg,0)
        ) / 2.0 - f.handicap
      WHEN LOWER(f.selection_name) LIKE '%under%'
        THEN f.handicap - (
          COALESCE(h.l5_goals_pg,0) + COALESCE(a.l5_goals_allowed_pg,0) +
          COALESCE(a.l5_goals_pg,0) + COALESCE(h.l5_goals_allowed_pg,0)
        ) / 2.0
      ELSE NULL END, 3)
    ELSE NULL
  END                                                  AS model_total_line_edge,

  -- Edge tier
  CASE
    WHEN f.market_type IN ('totals', 'halftime_totals', 'team_totals')
      AND f.handicap IS NOT NULL
    THEN CASE
      WHEN ABS((
        COALESCE(h.l5_goals_pg,0) + COALESCE(a.l5_goals_allowed_pg,0) +
        COALESCE(a.l5_goals_pg,0) + COALESCE(h.l5_goals_allowed_pg,0)
      ) / 2.0 - f.handicap) >= 0.75 THEN 'Strong'
      WHEN ABS((
        COALESCE(h.l5_goals_pg,0) + COALESCE(a.l5_goals_allowed_pg,0) +
        COALESCE(a.l5_goals_pg,0) + COALESCE(h.l5_goals_allowed_pg,0)
      ) / 2.0 - f.handicap) >= 0.35 THEN 'Medium'
      ELSE 'Lean'
    END
    ELSE NULL
  END                                                  AS model_edge_tier,

  CURRENT_TIMESTAMP()                                  AS analytics_updated_at

FROM fd_priced f

-- Team name → team_id mapping
LEFT JOIN `graphite-flare-477419-h7.soccer_data.epl_team_name_map` hmap
  ON f.home_team = hmap.fd_name
LEFT JOIN `graphite-flare-477419-h7.soccer_data.epl_team_name_map` amap
  ON f.away_team = amap.fd_name

-- Form metrics
LEFT JOIN `graphite-flare-477419-h7.soccer_data.epl_team_form` h
  ON hmap.team_id = h.team_id
LEFT JOIN `graphite-flare-477419-h7.soccer_data.epl_team_form` a
  ON amap.team_id = a.team_id

-- No-vig + hold
LEFT JOIN paired p
  ON f.event_id = p.event_id
  AND f.market_id = p.market_id
  AND f.selection_id = p.selection_id

-- Price rank
LEFT JOIN price_ranked pr
  ON f.event_id = pr.event_id
  AND f.market_id = pr.market_id
  AND f.selection_id = pr.selection_id

-- Multi-book consensus
LEFT JOIN book_consensus c
  ON CONCAT(f.home_team, ' vs ', f.away_team) = c.game
  AND f.event_start_ts = c.start_time_et
  AND f.market_type = c.market
  AND CAST(f.handicap AS STRING) = c.line_str