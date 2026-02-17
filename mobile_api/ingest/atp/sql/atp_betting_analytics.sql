-- =============================================================================
-- ATP Betting Analytics — Scheduled Materialized Table
-- =============================================================================
-- Replaces on-the-fly Python computations with a precomputed BigQuery table.
-- Suggested schedule: every 4–6 hours (or after each ingestion run).
--
-- Key design decisions:
--   1. Bayesian shrinkage: raw win rates are regressed toward a league-average
--      prior (50 %) using an effective-sample-size parameter.  This prevents
--      a player with 3 wins / 3 matches from showing a 100 % win rate that
--      outranks Djokovic.  The formula:
--        adjusted_rate = (wins + prior_wins) / (matches + prior_n)
--      where prior_n = 10 and prior_wins = prior_n * 0.5.
--
--   2. Multiple rolling windows (L10, L15, L20, L40) across overall and
--      surface-specific match histories.
--
--   3. Quality-of-opposition: win rate vs opponents ranked inside the top 50.
--
--   4. Tournament depth: titles, finals, semis reached in recent history.
--
--   5. One row per player per surface_key ("all", "clay", "hard", "grass", …).
-- =============================================================================

CREATE OR REPLACE TABLE `atp_data.atp_betting_analytics`
PARTITION BY DATE(updated_at)
CLUSTER BY player_id, surface_key
AS

-- ─── Bayesian prior constants ───────────────────────────────────────────────
-- prior_n: pseudo-observations added to every player.  Higher = more
-- shrinkage toward 50 %.  10 is a moderate choice — roughly saying "we
-- believe a player with zero history is a coin-flip until proven otherwise".
WITH params AS (
  SELECT
    10   AS prior_n,
    0.50 AS prior_rate
),

-- ─── Deduplicate matches ────────────────────────────────────────────────────
-- The matches table can contain duplicate rows for the same match_id from
-- different ingestion runs.  Keep only the latest row per match_id.
deduped_matches AS (
  SELECT * EXCEPT(_rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY run_ts DESC) AS _rn
    FROM `atp_data.matches`
    WHERE match_status IN ('F', 'finished', 'completed', 'Finished')
      AND winner_id IS NOT NULL
      AND player1_id IS NOT NULL
      AND player2_id IS NOT NULL
  )
  WHERE _rn = 1
),

-- ─── Flatten: one row per player per match ──────────────────────────────────
base AS (
  SELECT
    m.match_id,
    m.scheduled_time,
    m.season,
    LOWER(COALESCE(m.surface, 'unknown'))           AS surface_key,
    COALESCE(m.category, 'Unknown')                 AS category,
    m.round,
    m.tournament_id,
    m.tournament_name,
    m.score,
    m.player1_id                                    AS player_id,
    m.player1_name                                  AS player_name,
    m.player2_id                                    AS opponent_id,
    IF(m.winner_id = m.player1_id, 1, 0)            AS win,
    m.winner_id
  FROM deduped_matches m

  UNION ALL

  SELECT
    m.match_id,
    m.scheduled_time,
    m.season,
    LOWER(COALESCE(m.surface, 'unknown'))           AS surface_key,
    COALESCE(m.category, 'Unknown')                 AS category,
    m.round,
    m.tournament_id,
    m.tournament_name,
    m.score,
    m.player2_id                                    AS player_id,
    m.player2_name                                  AS player_name,
    m.player1_id                                    AS opponent_id,
    IF(m.winner_id = m.player2_id, 1, 0)            AS win,
    m.winner_id
  FROM deduped_matches m
),

-- ─── Score-level flags ──────────────────────────────────────────────────────
with_flags AS (
  SELECT
    b.*,
    -- Straight-sets win detection (best-of-3: 2-0, best-of-5: 3-0)
    CASE
      WHEN b.win = 1
        AND REGEXP_CONTAINS(b.score, r'^(\d+-\d+)\s+(\d+-\d+)$')             THEN 1  -- exactly 2 sets
      WHEN b.win = 1
        AND REGEXP_CONTAINS(b.score, r'^(\d+-\d+)\s+(\d+-\d+)\s+(\d+-\d+)$')
        AND NOT REGEXP_CONTAINS(b.score, r'(\d+-\d+)\s+(\d+-\d+)\s+(\d+-\d+)\s+')
        AND LOWER(COALESCE(b.category, '')) LIKE '%grand slam%'               THEN 1  -- 3 sets in a slam = straight
      ELSE 0
    END AS straight_sets_win,
    IF(REGEXP_CONTAINS(b.score, r'7-6|6-7'), 1, 0)                           AS has_tiebreak,
    -- Retirement / walkover flag
    IF(REGEXP_CONTAINS(UPPER(COALESCE(b.score, '')), r'RET|W/O|WO|DEF|ABD|ABN'), 1, 0) AS is_retirement,
    -- Number of sets played (count score segments like "6-4")
    ARRAY_LENGTH(REGEXP_EXTRACT_ALL(b.score, r'\d+-\d+'))                     AS sets_played,
    -- Row number for rolling windows (overall, most recent first)
    ROW_NUMBER() OVER (
      PARTITION BY b.player_id
      ORDER BY b.scheduled_time DESC, b.match_id DESC
    ) AS rn_overall,
    -- Row number for surface-specific rolling windows
    ROW_NUMBER() OVER (
      PARTITION BY b.player_id, b.surface_key
      ORDER BY b.scheduled_time DESC, b.match_id DESC
    ) AS rn_surface
  FROM base b
),

-- ─── Latest ranking per player ──────────────────────────────────────────────
latest_ranking AS (
  SELECT player_id, rank AS world_rank, points AS ranking_points
  FROM (
    SELECT
      player_id, rank, points,
      ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY ranking_date DESC) AS _rn
    FROM `atp_data.rankings`
  )
  WHERE _rn = 1
),

-- ─── Opponent ranking at match time (latest available) ──────────────────────
opponent_rank AS (
  SELECT DISTINCT
    f.match_id,
    f.player_id,
    lr.world_rank AS opponent_rank
  FROM with_flags f
  JOIN latest_ranking lr ON lr.player_id = f.opponent_id
),

-- ─── Rolling window aggregates (overall) ────────────────────────────────────
rolling_overall AS (
  SELECT
    player_id,
    -- L10
    COUNTIF(rn_overall <= 10)                                          AS l10_matches,
    SUM(IF(rn_overall <= 10, win, 0))                                  AS l10_wins,
    -- L15
    COUNTIF(rn_overall <= 15)                                          AS l15_matches,
    SUM(IF(rn_overall <= 15, win, 0))                                  AS l15_wins,
    -- L20
    COUNTIF(rn_overall <= 20)                                          AS l20_matches,
    SUM(IF(rn_overall <= 20, win, 0))                                  AS l20_wins,
    -- L40
    COUNTIF(rn_overall <= 40)                                          AS l40_matches,
    SUM(IF(rn_overall <= 40, win, 0))                                  AS l40_wins
  FROM with_flags
  WHERE rn_overall <= 40
  GROUP BY player_id
),

-- ─── Current streak calculation ─────────────────────────────────────────────
-- Uses a running sum to detect the first result change from rn_overall=1.
streak_base AS (
  SELECT
    player_id,
    win,
    rn_overall,
    -- running count of losses from most recent backward; stays 0 while winning
    SUM(1 - win) OVER (PARTITION BY player_id ORDER BY rn_overall ROWS UNBOUNDED PRECEDING) AS cum_losses,
    -- running count of wins from most recent backward; stays 0 while losing
    SUM(win) OVER (PARTITION BY player_id ORDER BY rn_overall ROWS UNBOUNDED PRECEDING) AS cum_wins
  FROM with_flags
  WHERE rn_overall <= 40
),

streaks AS (
  SELECT
    player_id,
    -- Consecutive wins from the start = rows where cum_losses is still 0
    COUNTIF(cum_losses = 0)  AS current_win_streak,
    -- Consecutive losses from the start = rows where cum_wins is still 0
    COUNTIF(cum_wins = 0)    AS current_loss_streak
  FROM streak_base
  GROUP BY player_id
),

-- ─── Rolling window aggregates (surface-specific) ───────────────────────────
rolling_surface AS (
  SELECT
    player_id,
    surface_key,
    -- L10 surface
    COUNTIF(rn_surface <= 10)                                          AS l10_surface_matches,
    SUM(IF(rn_surface <= 10, win, 0))                                  AS l10_surface_wins,
    -- L20 surface
    COUNTIF(rn_surface <= 20)                                          AS l20_surface_matches,
    SUM(IF(rn_surface <= 20, win, 0))                                  AS l20_surface_wins
  FROM with_flags
  WHERE rn_surface <= 20
  GROUP BY player_id, surface_key
),

-- ─── Lifetime / all-history aggregates per player+surface ───────────────────
lifetime_surface AS (
  SELECT
    player_id,
    ANY_VALUE(player_name)                                             AS player_name,
    surface_key,
    COUNT(*)                                                           AS total_matches,
    SUM(win)                                                           AS total_wins,
    SUM(straight_sets_win)                                             AS straight_sets_wins,
    SUM(has_tiebreak)                                                  AS tiebreak_matches,
    SUM(is_retirement)                                                 AS retirement_matches,
    AVG(IF(sets_played > 0, sets_played, NULL))                        AS avg_sets_per_match,
    -- vs top-50 opponents
    SUM(IF(opp.opponent_rank IS NOT NULL AND opp.opponent_rank <= 50, 1, 0))         AS matches_vs_top50,
    SUM(IF(opp.opponent_rank IS NOT NULL AND opp.opponent_rank <= 50, win, 0))       AS wins_vs_top50
  FROM with_flags f
  LEFT JOIN opponent_rank opp
    ON f.match_id = opp.match_id AND f.player_id = opp.player_id
  GROUP BY player_id, surface_key
),

-- ─── Lifetime aggregates (all surfaces) ─────────────────────────────────────
lifetime_all AS (
  SELECT
    player_id,
    ANY_VALUE(player_name)                                             AS player_name,
    'all'                                                              AS surface_key,
    COUNT(*)                                                           AS total_matches,
    SUM(win)                                                           AS total_wins,
    SUM(straight_sets_win)                                             AS straight_sets_wins,
    SUM(has_tiebreak)                                                  AS tiebreak_matches,
    SUM(is_retirement)                                                 AS retirement_matches,
    AVG(IF(sets_played > 0, sets_played, NULL))                        AS avg_sets_per_match,
    SUM(IF(opp.opponent_rank IS NOT NULL AND opp.opponent_rank <= 50, 1, 0))         AS matches_vs_top50,
    SUM(IF(opp.opponent_rank IS NOT NULL AND opp.opponent_rank <= 50, win, 0))       AS wins_vs_top50
  FROM with_flags f
  LEFT JOIN opponent_rank opp
    ON f.match_id = opp.match_id AND f.player_id = opp.player_id
  GROUP BY player_id
),

-- ─── Tournament depth (all surfaces) ────────────────────────────────────────
tournament_depth AS (
  SELECT
    player_id,
    'all' AS surface_key,
    COUNT(DISTINCT tournament_id)                                      AS tournaments_played,
    COUNTIF(LOWER(round) IN ('f', 'final', 'finals') AND win = 1)     AS titles,
    COUNTIF(LOWER(round) IN ('f', 'final', 'finals'))                  AS finals_reached,
    COUNTIF(LOWER(round) IN ('sf', 'semifinal', 'semifinals',
                             'semi final', 'semi finals', 'semis'))    AS semis_reached,
    COUNTIF(LOWER(round) IN ('qf', 'quarterfinal', 'quarterfinals',
                             'quarter final', 'quarter finals',
                             'quarters'))                              AS quarters_reached
  FROM with_flags
  GROUP BY player_id
),

tournament_depth_surface AS (
  SELECT
    player_id,
    surface_key,
    COUNT(DISTINCT tournament_id)                                      AS tournaments_played,
    COUNTIF(LOWER(round) IN ('f', 'final', 'finals') AND win = 1)     AS titles,
    COUNTIF(LOWER(round) IN ('f', 'final', 'finals'))                  AS finals_reached,
    COUNTIF(LOWER(round) IN ('sf', 'semifinal', 'semifinals',
                             'semi final', 'semi finals', 'semis'))    AS semis_reached,
    COUNTIF(LOWER(round) IN ('qf', 'quarterfinal', 'quarterfinals',
                             'quarter final', 'quarter finals',
                             'quarters'))                              AS quarters_reached
  FROM with_flags
  GROUP BY player_id, surface_key
),

-- ─── Category-level splits (Grand Slam vs Masters vs 250/500) ───────────────
category_stats AS (
  SELECT
    player_id,
    CASE
      WHEN LOWER(category) LIKE '%grand slam%' THEN 'grand_slam'
      WHEN LOWER(category) LIKE '%masters%' OR LOWER(category) LIKE '%1000%' THEN 'masters'
      ELSE 'other'
    END AS category_bucket,
    COUNT(*) AS matches,
    SUM(win) AS wins
  FROM with_flags
  GROUP BY player_id, category_bucket
),

category_pivot AS (
  SELECT
    player_id,
    MAX(IF(category_bucket = 'grand_slam', matches, 0)) AS gs_matches,
    MAX(IF(category_bucket = 'grand_slam', wins, 0))    AS gs_wins,
    MAX(IF(category_bucket = 'masters', matches, 0))    AS masters_matches,
    MAX(IF(category_bucket = 'masters', wins, 0))        AS masters_wins
  FROM category_stats
  GROUP BY player_id
),

-- ─── Combine surface + all ──────────────────────────────────────────────────
combined_lifetime AS (
  SELECT * FROM lifetime_surface
  UNION ALL
  SELECT * FROM lifetime_all
),

combined_rolling_surface AS (
  -- Surface-specific rows already exist; create an "all" rollup for L10/L20 surface windows
  SELECT * FROM rolling_surface
),

combined_tournament_depth AS (
  SELECT * FROM tournament_depth
  UNION ALL
  SELECT * FROM tournament_depth_surface
),

-- ─── Assemble final output ──────────────────────────────────────────────────
assembled AS (
  SELECT
    lt.player_id,
    lt.player_name,
    lt.surface_key,

    -- Ranking
    lr.world_rank,
    lr.ranking_points,

    -- Lifetime raw rates
    lt.total_matches,
    lt.total_wins,
    SAFE_DIVIDE(lt.total_wins, lt.total_matches)                       AS raw_win_rate,

    -- Bayesian-adjusted lifetime win rate
    SAFE_DIVIDE(
      lt.total_wins + p.prior_n * p.prior_rate,
      lt.total_matches + p.prior_n
    )                                                                  AS adj_win_rate,

    -- ── Rolling windows (overall) ───────────────────────────────────
    ro.l10_matches,
    ro.l10_wins,
    SAFE_DIVIDE(ro.l10_wins, ro.l10_matches)                           AS l10_win_rate,
    SAFE_DIVIDE(
      ro.l10_wins + p.prior_n * p.prior_rate,
      ro.l10_matches + p.prior_n
    )                                                                  AS l10_adj_win_rate,

    ro.l15_matches,
    ro.l15_wins,
    SAFE_DIVIDE(ro.l15_wins, ro.l15_matches)                           AS l15_win_rate,
    SAFE_DIVIDE(
      ro.l15_wins + p.prior_n * p.prior_rate,
      ro.l15_matches + p.prior_n
    )                                                                  AS l15_adj_win_rate,

    ro.l20_matches,
    ro.l20_wins,
    SAFE_DIVIDE(ro.l20_wins, ro.l20_matches)                           AS l20_win_rate,

    ro.l40_matches,
    ro.l40_wins,
    SAFE_DIVIDE(ro.l40_wins, ro.l40_matches)                           AS l40_win_rate,
    SAFE_DIVIDE(
      ro.l40_wins + p.prior_n * p.prior_rate,
      ro.l40_matches + p.prior_n
    )                                                                  AS l40_adj_win_rate,

    -- ── Rolling windows (surface-specific) ──────────────────────────
    rs.l10_surface_matches,
    rs.l10_surface_wins,
    SAFE_DIVIDE(rs.l10_surface_wins, rs.l10_surface_matches)           AS l10_surface_win_rate,
    SAFE_DIVIDE(
      rs.l10_surface_wins + p.prior_n * p.prior_rate,
      rs.l10_surface_matches + p.prior_n
    )                                                                  AS l10_surface_adj_win_rate,

    rs.l20_surface_matches,
    rs.l20_surface_wins,
    SAFE_DIVIDE(rs.l20_surface_wins, rs.l20_surface_matches)           AS l20_surface_win_rate,
    SAFE_DIVIDE(
      rs.l20_surface_wins + p.prior_n * p.prior_rate,
      rs.l20_surface_matches + p.prior_n
    )                                                                  AS l20_surface_adj_win_rate,

    -- ── Quality of opposition ───────────────────────────────────────
    lt.matches_vs_top50,
    lt.wins_vs_top50,
    SAFE_DIVIDE(lt.wins_vs_top50, lt.matches_vs_top50)                 AS win_rate_vs_top50,
    SAFE_DIVIDE(
      lt.wins_vs_top50 + p.prior_n * p.prior_rate,
      lt.matches_vs_top50 + p.prior_n
    )                                                                  AS adj_win_rate_vs_top50,

    -- ── Dominance indicators ────────────────────────────────────────
    lt.straight_sets_wins,
    SAFE_DIVIDE(lt.straight_sets_wins, GREATEST(lt.total_wins, 1))     AS straight_sets_rate,
    lt.tiebreak_matches,
    SAFE_DIVIDE(lt.tiebreak_matches, lt.total_matches)                 AS tiebreak_rate,
    lt.avg_sets_per_match,

    -- ── Injury / retirement risk ────────────────────────────────────
    lt.retirement_matches,
    SAFE_DIVIDE(lt.retirement_matches, lt.total_matches)               AS retirement_rate,

    -- ── Tournament depth ────────────────────────────────────────────
    COALESCE(td.tournaments_played, 0)                                 AS tournaments_played,
    COALESCE(td.titles, 0)                                             AS titles,
    COALESCE(td.finals_reached, 0)                                     AS finals_reached,
    COALESCE(td.semis_reached, 0)                                      AS semis_reached,
    COALESCE(td.quarters_reached, 0)                                   AS quarters_reached,

    -- ── Category splits (only on "all" surface row) ─────────────────
    COALESCE(cp.gs_matches, 0)                                         AS grand_slam_matches,
    COALESCE(cp.gs_wins, 0)                                            AS grand_slam_wins,
    SAFE_DIVIDE(cp.gs_wins, cp.gs_matches)                             AS grand_slam_win_rate,
    COALESCE(cp.masters_matches, 0)                                    AS masters_matches,
    COALESCE(cp.masters_wins, 0)                                       AS masters_wins,
    SAFE_DIVIDE(cp.masters_wins, cp.masters_matches)                   AS masters_win_rate,

    -- ── Streaks ─────────────────────────────────────────────────────
    COALESCE(sk.current_win_streak, 0)                                 AS current_win_streak,
    COALESCE(sk.current_loss_streak, 0)                                AS current_loss_streak,

    -- ── Composite form score (Bayesian-adjusted) ────────────────────
    -- Weighted blend of adjusted rates:
    --   40 % recent form (L15 adj)
    --   25 % surface form (L20 surface adj, falls back to L15 adj)
    --   15 % medium-term (L40 adj)
    --   10 % quality of opposition (adj vs top50)
    --   10 % dominance (straight sets rate - tiebreak rate, scaled)
    (
      0.40 * COALESCE(
        SAFE_DIVIDE(ro.l15_wins + p.prior_n * p.prior_rate, ro.l15_matches + p.prior_n),
        0.5
      )
      + 0.25 * COALESCE(
        SAFE_DIVIDE(rs.l20_surface_wins + p.prior_n * p.prior_rate, rs.l20_surface_matches + p.prior_n),
        SAFE_DIVIDE(ro.l15_wins + p.prior_n * p.prior_rate, ro.l15_matches + p.prior_n),
        0.5
      )
      + 0.15 * COALESCE(
        SAFE_DIVIDE(ro.l40_wins + p.prior_n * p.prior_rate, ro.l40_matches + p.prior_n),
        0.5
      )
      + 0.10 * COALESCE(
        SAFE_DIVIDE(lt.wins_vs_top50 + p.prior_n * p.prior_rate, lt.matches_vs_top50 + p.prior_n),
        0.5
      )
      + 0.10 * (
        0.5
        + 0.5 * (
          COALESCE(SAFE_DIVIDE(lt.straight_sets_wins, GREATEST(lt.total_wins, 1)), 0)
          - COALESCE(SAFE_DIVIDE(lt.tiebreak_matches, lt.total_matches), 0)
        )
      )
    )                                                                  AS betting_form_score,

    -- ── Sample size confidence (0–1 scale) ──────────────────────────
    -- Sigmoid-like ramp: confidence = match_count / (match_count + k)
    -- At 20 matches confidence ≈ 0.67, at 50 ≈ 0.83, at 100 ≈ 0.91
    SAFE_DIVIDE(lt.total_matches, lt.total_matches + 10.0)             AS sample_confidence,

    CURRENT_TIMESTAMP()                                                AS updated_at

  FROM combined_lifetime lt
  CROSS JOIN params p
  LEFT JOIN latest_ranking lr
    ON lr.player_id = lt.player_id
  LEFT JOIN rolling_overall ro
    ON ro.player_id = lt.player_id
    AND lt.surface_key = 'all'          -- rolling_overall only joins to "all" surface rows
  LEFT JOIN combined_rolling_surface rs
    ON rs.player_id = lt.player_id
    AND rs.surface_key = lt.surface_key
  LEFT JOIN combined_tournament_depth td
    ON td.player_id = lt.player_id
    AND td.surface_key = lt.surface_key
  LEFT JOIN category_pivot cp
    ON cp.player_id = lt.player_id
    AND lt.surface_key = 'all'          -- category splits only on "all" row
  LEFT JOIN streaks sk
    ON sk.player_id = lt.player_id
    AND lt.surface_key = 'all'          -- streaks are overall only
);
