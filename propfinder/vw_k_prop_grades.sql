-- vw_k_prop_grades
-- Grades today's pitcher strikeout projections against sportsbook lines.
-- Joins vw_strikeout_signal (proj_ks, k_signal_score) with raw_k_props (standard lines).
-- Separate table/view from vw_strikeout_signal for clean separation of concerns.

CREATE OR REPLACE VIEW `graphite-flare-477419-h7.propfinder.vw_k_prop_grades` AS

WITH run AS (
  SELECT MAX(run_date) AS dt
  FROM `graphite-flare-477419-h7.propfinder.raw_pitcher_matchup`
),

-- Deduplicate K props (latest ingest per pitcher/game)
k_props AS (
  SELECT * EXCEPT(_rn)
  FROM (
    SELECT
      kp.*,
      ROW_NUMBER() OVER (
        PARTITION BY kp.pitcher_id, kp.game_pk
        ORDER BY kp.ingested_at DESC
      ) AS _rn
    FROM `graphite-flare-477419-h7.propfinder.raw_k_props` kp
    CROSS JOIN run
    WHERE kp.run_date = run.dt
  )
  WHERE _rn = 1
),

graded AS (
  SELECT
    sig.run_date,
    sig.game_pk,
    sig.pitcher_id,
    sig.pitcher_name,
    sig.pitcher_hand,
    kp.team_code,
    kp.opp_team_code,
    sig.home_team_name,
    sig.away_team_name,
    sig.ballpark_name,
    sig.over_under AS game_total,

    -- Sportsbook line
    kp.line                AS k_line,
    kp.best_price          AS k_best_price,
    kp.best_book           AS k_best_book,
    kp.pf_rating,
    kp.hit_rate_l10,
    kp.hit_rate_season,
    kp.hit_rate_vs_team,
    kp.avg_l10,
    kp.avg_home_away,
    kp.avg_vs_opponent,
    kp.streak,
    kp.deep_link_desktop,
    kp.deep_link_ios,

    -- Our projection
    sig.proj_ks,
    sig.proj_ip,
    sig.proj_outs,
    sig.k_signal_score,

    -- Pitcher underlying stats
    sig.strikeouts_per_9,
    sig.k_pct,
    sig.strike_pct,
    sig.strikeout_walk_ratio,
    sig.arsenal_whiff_rate,
    sig.team_k_adj,

    -- ── Edge calculation ──
    -- Positive edge = our projection exceeds the line (lean over)
    -- Negative edge = our projection is below the line (lean under)
    ROUND(sig.proj_ks - kp.line, 1) AS edge,

    -- Edge as a percentage of the line
    ROUND(SAFE_DIVIDE(sig.proj_ks - kp.line, kp.line) * 100, 1) AS edge_pct,

    -- ── Grade (A+ to F) ──
    CASE
      WHEN sig.proj_ks - kp.line >= 2.0  THEN 'A+'
      WHEN sig.proj_ks - kp.line >= 1.5  THEN 'A'
      WHEN sig.proj_ks - kp.line >= 1.0  THEN 'B+'
      WHEN sig.proj_ks - kp.line >= 0.5  THEN 'B'
      WHEN sig.proj_ks - kp.line >= 0.0  THEN 'C+'
      WHEN sig.proj_ks - kp.line >= -0.5 THEN 'C'
      WHEN sig.proj_ks - kp.line >= -1.0 THEN 'C-'
      WHEN sig.proj_ks - kp.line >= -1.5 THEN 'D'
      ELSE 'F'
    END AS over_grade,

    -- Lean direction
    CASE
      WHEN sig.proj_ks - kp.line >= 0.5  THEN 'OVER'
      WHEN sig.proj_ks - kp.line <= -0.5 THEN 'UNDER'
      ELSE 'PASS'
    END AS lean,

    -- Confidence tier combining edge + signal score + propfinder rating
    CASE
      WHEN ABS(sig.proj_ks - kp.line) >= 1.0
        AND sig.k_signal_score >= 50
        THEN 'HIGH'
      WHEN ABS(sig.proj_ks - kp.line) >= 0.5
        AND sig.k_signal_score >= 35
        THEN 'MEDIUM'
      ELSE 'LOW'
    END AS confidence

  FROM `graphite-flare-477419-h7.propfinder.vw_strikeout_signal` sig

  INNER JOIN k_props kp
    ON kp.pitcher_id = sig.pitcher_id
    AND kp.game_pk = sig.game_pk
)

SELECT
  *,
  RANK() OVER (ORDER BY edge DESC) AS over_rank,
  RANK() OVER (ORDER BY edge ASC)  AS under_rank
FROM graded
ORDER BY ABS(edge) DESC;
