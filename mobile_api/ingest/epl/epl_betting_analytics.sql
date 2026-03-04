-- Builds a daily EPL betting analytics table from soccer odds and current team form.
-- Scope: EPL only, limited to FanDuel + DraftKings.

CREATE SCHEMA IF NOT EXISTS `soccer_data` OPTIONS(location = "US");

CREATE OR REPLACE PROCEDURE `soccer_data.run_epl_betting_analytics_daily`()
BEGIN
  CREATE OR REPLACE TABLE `soccer_data.epl_betting_analytics`
  PARTITION BY DATE(start_time_et)
  CLUSTER BY bookmaker, market, home_team, away_team
  AS
  WITH team_name_map AS (
    SELECT
      team_id,
      team_name,
      LOWER(REGEXP_REPLACE(team_name, r'[^a-z0-9]+', '')) AS team_name_key
    FROM (
      SELECT
        team_id,
        team_name,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY ingested_at DESC) AS rn
      FROM `epl_data.standings_flat`
      WHERE team_name IS NOT NULL
    )
    WHERE rn = 1
  ),
  team_form AS (
    SELECT
      tf.*,
      m.team_name,
      m.team_name_key
    FROM `epl_data.team_form_current` tf
    LEFT JOIN team_name_map m
      ON SAFE_CAST(tf.team_id AS INT64) = m.team_id
  ),
  odds_deduped AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        o.*,
        ROW_NUMBER() OVER (
          PARTITION BY league, game, start_time_et, bookmaker, market, outcome, CAST(line AS STRING)
          ORDER BY ingested_at DESC
        ) AS rn
      FROM `soccer_data.odds_lines` o
      WHERE league = 'EPL'
        AND bookmaker IN ('FanDuel', 'DraftKings')
    )
    WHERE rn = 1
  ),
  odds_enriched AS (
    SELECT
      o.*,
      LOWER(TRIM(o.market)) AS market_key,
      LOWER(TRIM(o.outcome)) AS outcome_key,
      CASE
        WHEN o.price IS NULL THEN NULL
        WHEN o.price > 0 THEN ROUND(1 + (o.price / 100.0), 6)
        WHEN o.price < 0 THEN ROUND(1 + (100.0 / ABS(o.price)), 6)
        ELSE NULL
      END AS decimal_odds,
      CASE
        WHEN o.price IS NULL THEN NULL
        WHEN o.price > 0 THEN ROUND(100.0 / (o.price + 100.0), 8)
        WHEN o.price < 0 THEN ROUND(ABS(o.price) / (ABS(o.price) + 100.0), 8)
        ELSE NULL
      END AS implied_probability,
      CASE
        WHEN LOWER(TRIM(o.outcome)) = 'over' THEN 'UNDER'
        WHEN LOWER(TRIM(o.outcome)) = 'under' THEN 'OVER'
        WHEN LOWER(TRIM(o.outcome)) = 'yes' THEN 'NO'
        WHEN LOWER(TRIM(o.outcome)) = 'no' THEN 'YES'
        ELSE NULL
      END AS opposite_outcome_key
    FROM odds_deduped o
  ),
  paired_market AS (
    SELECT
      a.league,
      a.game,
      a.start_time_et,
      a.bookmaker,
      a.market,
      a.outcome,
      a.line,
      b.implied_probability AS opposite_implied_probability,
      CASE
        WHEN a.implied_probability IS NOT NULL
         AND b.implied_probability IS NOT NULL
         AND (a.implied_probability + b.implied_probability) > 0
        THEN ROUND(
          a.implied_probability / (a.implied_probability + b.implied_probability),
          8
        )
        ELSE NULL
      END AS no_vig_probability,
      CASE
        WHEN a.implied_probability IS NOT NULL
         AND b.implied_probability IS NOT NULL
        THEN ROUND((a.implied_probability + b.implied_probability) - 1, 8)
        ELSE NULL
      END AS market_hold
    FROM odds_enriched a
    LEFT JOIN odds_enriched b
      ON a.league = b.league
     AND a.game = b.game
     AND a.start_time_et = b.start_time_et
     AND a.bookmaker = b.bookmaker
     AND a.market = b.market
     AND (a.line IS NULL AND b.line IS NULL OR a.line = b.line)
     AND a.opposite_outcome_key IS NOT NULL
     AND UPPER(b.outcome_key) = a.opposite_outcome_key
  ),
  market_consensus AS (
    SELECT
      league,
      game,
      start_time_et,
      market,
      outcome,
      line,
      COUNT(*) AS books_reporting,
      ROUND(AVG(price), 3) AS avg_price,
      MIN(price) AS min_price,
      MAX(price) AS max_price,
      ROUND(AVG(implied_probability), 8) AS consensus_implied_probability,
      ROUND(AVG(COALESCE(pm.no_vig_probability, oe.implied_probability)), 8) AS consensus_fair_probability
    FROM odds_enriched oe
    LEFT JOIN paired_market pm
      USING (league, game, start_time_et, bookmaker, market, outcome, line)
    GROUP BY league, game, start_time_et, market, outcome, line
  )
  SELECT
    oe.ingested_at,
    oe.league,
    oe.game,
    oe.start_time_et,
    oe.bookmaker,
    oe.market,
    oe.outcome,
    oe.line,
    oe.price,
    oe.home_team,
    oe.away_team,

    -- Home team form metrics
    h.team_id AS home_team_id,
    h.side AS home_side,
    h.l3_goals_pg AS home_l3_goals_pg,
    h.l3_goals_allowed_pg AS home_l3_goals_allowed_pg,
    h.l5_goals_pg AS home_l5_goals_pg,
    h.l5_goals_allowed_pg AS home_l5_goals_allowed_pg,
    h.l7_goals_pg AS home_l7_goals_pg,
    h.l7_goals_allowed_pg AS home_l7_goals_allowed_pg,
    h.l3_win_rate AS home_l3_win_rate,
    h.l5_win_rate AS home_l5_win_rate,
    h.l7_win_rate AS home_l7_win_rate,

    -- Away team form metrics
    a.team_id AS away_team_id,
    a.side AS away_side,
    a.l3_goals_pg AS away_l3_goals_pg,
    a.l3_goals_allowed_pg AS away_l3_goals_allowed_pg,
    a.l5_goals_pg AS away_l5_goals_pg,
    a.l5_goals_allowed_pg AS away_l5_goals_allowed_pg,
    a.l7_goals_pg AS away_l7_goals_pg,
    a.l7_goals_allowed_pg AS away_l7_goals_allowed_pg,
    a.l3_win_rate AS away_l3_win_rate,
    a.l5_win_rate AS away_l5_win_rate,
    a.l7_win_rate AS away_l7_win_rate,

    -- Derived pricing
    ROUND(oe.decimal_odds, 4) AS decimal_odds,
    ROUND(oe.implied_probability, 6) AS implied_probability,
    ROUND(pm.opposite_implied_probability, 6) AS opposite_implied_probability,
    ROUND(pm.no_vig_probability, 6) AS no_vig_probability,
    ROUND(pm.market_hold, 6) AS market_hold,

    -- Consensus analytics (FD vs DK for now)
    mc.books_reporting,
    mc.avg_price AS market_avg_price,
    mc.min_price AS market_min_price,
    mc.max_price AS market_max_price,
    ROUND(mc.consensus_implied_probability, 6) AS market_consensus_implied_probability,
    ROUND(mc.consensus_fair_probability, 6) AS market_consensus_fair_probability,
    ROUND(COALESCE(pm.no_vig_probability, oe.implied_probability) - mc.consensus_fair_probability, 6)
      AS probability_vs_market,
    CASE
      WHEN oe.price = mc.max_price THEN TRUE
      ELSE FALSE
    END AS is_best_price,
    ROW_NUMBER() OVER (
      PARTITION BY oe.league, oe.game, oe.start_time_et, oe.market, oe.outcome, CAST(oe.line AS STRING)
      ORDER BY oe.price DESC, oe.bookmaker
    ) AS price_rank,

    -- Derived form model (simple, explainable baseline)
    ROUND(
      (
        COALESCE(h.l5_goals_pg, 0)
        + COALESCE(a.l5_goals_allowed_pg, 0)
        + COALESCE(a.l5_goals_pg, 0)
        + COALESCE(h.l5_goals_allowed_pg, 0)
      ) / 2.0,
      4
    ) AS model_expected_total_goals,
    ROUND(COALESCE(a.l5_win_rate, 0) - COALESCE(h.l5_win_rate, 0), 4) AS model_away_win_form_edge,
    ROUND(COALESCE(h.l5_win_rate, 0) - COALESCE(a.l5_win_rate, 0), 4) AS model_home_win_form_edge,

    CASE
      WHEN LOWER(oe.market) IN ('alternate_totals', 'alt_totals', 'total_goals', 'totals', 'over_under')
       AND oe.line IS NOT NULL
      THEN ROUND(
        CASE
          WHEN LOWER(oe.outcome) = 'over' THEN (
            (
              COALESCE(h.l5_goals_pg, 0)
              + COALESCE(a.l5_goals_allowed_pg, 0)
              + COALESCE(a.l5_goals_pg, 0)
              + COALESCE(h.l5_goals_allowed_pg, 0)
            ) / 2.0
          ) - oe.line
          WHEN LOWER(oe.outcome) = 'under' THEN oe.line - (
            (
              COALESCE(h.l5_goals_pg, 0)
              + COALESCE(a.l5_goals_allowed_pg, 0)
              + COALESCE(a.l5_goals_pg, 0)
              + COALESCE(h.l5_goals_allowed_pg, 0)
            ) / 2.0
          )
          ELSE NULL
        END,
        4
      )
      ELSE NULL
    END AS model_total_line_edge,

    CASE
      WHEN LOWER(oe.market) IN ('alternate_totals', 'alt_totals', 'total_goals', 'totals', 'over_under')
       AND oe.line IS NOT NULL
      THEN CASE
        WHEN ABS(
          CASE
            WHEN LOWER(oe.outcome) = 'over' THEN (
              (
                COALESCE(h.l5_goals_pg, 0)
                + COALESCE(a.l5_goals_allowed_pg, 0)
                + COALESCE(a.l5_goals_pg, 0)
                + COALESCE(h.l5_goals_allowed_pg, 0)
              ) / 2.0
            ) - oe.line
            WHEN LOWER(oe.outcome) = 'under' THEN oe.line - (
              (
                COALESCE(h.l5_goals_pg, 0)
                + COALESCE(a.l5_goals_allowed_pg, 0)
                + COALESCE(a.l5_goals_pg, 0)
                + COALESCE(h.l5_goals_allowed_pg, 0)
              ) / 2.0
            )
            ELSE 0
          END
        ) >= 0.75 THEN 'Strong'
        WHEN ABS(
          CASE
            WHEN LOWER(oe.outcome) = 'over' THEN (
              (
                COALESCE(h.l5_goals_pg, 0)
                + COALESCE(a.l5_goals_allowed_pg, 0)
                + COALESCE(a.l5_goals_pg, 0)
                + COALESCE(h.l5_goals_allowed_pg, 0)
              ) / 2.0
            ) - oe.line
            WHEN LOWER(oe.outcome) = 'under' THEN oe.line - (
              (
                COALESCE(h.l5_goals_pg, 0)
                + COALESCE(a.l5_goals_allowed_pg, 0)
                + COALESCE(a.l5_goals_pg, 0)
                + COALESCE(h.l5_goals_allowed_pg, 0)
              ) / 2.0
            )
            ELSE 0
          END
        ) >= 0.35 THEN 'Medium'
        ELSE 'Lean'
      END
      ELSE NULL
    END AS model_edge_tier,

    CURRENT_TIMESTAMP() AS analytics_updated_at
  FROM odds_enriched oe
  LEFT JOIN paired_market pm
    USING (league, game, start_time_et, bookmaker, market, outcome, line)
  LEFT JOIN market_consensus mc
    USING (league, game, start_time_et, market, outcome, line)
  LEFT JOIN team_form h
    ON LOWER(REGEXP_REPLACE(oe.home_team, r'[^a-z0-9]+', '')) = h.team_name_key
   AND LOWER(h.side) = 'home'
  LEFT JOIN team_form a
    ON LOWER(REGEXP_REPLACE(oe.away_team, r'[^a-z0-9]+', '')) = a.team_name_key
   AND LOWER(a.side) = 'away';
END;

-- Run after odds load:
-- CALL `soccer_data.run_epl_betting_analytics_daily`();
