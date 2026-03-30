-- vw_strikeout_signal
-- Joins raw_pitcher_matchup + raw_team_strikeout_rankings + raw_pitch_log
-- into a composite strikeout prop signal for today's pitchers.
--
-- Run date is parameterized via the CTE; defaults to latest available date.

CREATE OR REPLACE VIEW `graphite-flare-477419-h7.propfinder.vw_strikeout_signal` AS

WITH run AS (
  SELECT MAX(run_date) AS dt
  FROM `graphite-flare-477419-h7.propfinder.raw_pitcher_matchup`
),

-- 1. Deduplicate pitcher matchup rows (multiple ingest runs per day)
pitcher_deduped AS (
  SELECT
    pm.*,
    ROW_NUMBER() OVER (
      PARTITION BY pm.pitcher_id, pm.game_pk, pm.split
      ORDER BY pm.ingested_at DESC
    ) AS _rn
  FROM `graphite-flare-477419-h7.propfinder.raw_pitcher_matchup` pm
  CROSS JOIN run
  WHERE pm.run_date = run.dt
),

-- 2. Pitcher season-level K profile
pitcher_season AS (
  SELECT
    run_date,
    game_pk,
    pitcher_id,
    pitcher_name,
    pitcher_hand,
    opp_team_id,
    ip,
    strikeouts,
    strikeouts_per_9,
    strikeout_walk_ratio,
    k_pct,
    strike_pct,
    batters_faced,
    whip,
    woba
  FROM pitcher_deduped
  WHERE split = 'Season' AND _rn = 1
),

-- 3. Pitcher handedness-split K profile (vsLHB / vsRHB)
pitcher_vs_hand AS (
  SELECT
    pitcher_id,
    game_pk,
    split                  AS hand_split,
    strikeouts_per_9       AS hand_k_per_9,
    k_pct                  AS hand_k_pct,
    strikeout_walk_ratio   AS hand_k_bb,
    batters_faced          AS hand_bf
  FROM pitcher_deduped
  WHERE split IN ('vsLHB', 'vsRHB') AND _rn = 1
),

-- 4. Deduplicate team K rankings
team_k_deduped AS (
  SELECT
    tk.*,
    ROW_NUMBER() OVER (
      PARTITION BY tk.team_id, tk.split, tk.category
      ORDER BY tk.ingested_at DESC
    ) AS _rn
  FROM `graphite-flare-477419-h7.propfinder.raw_team_strikeout_rankings` tk
  CROSS JOIN run
  WHERE tk.run_date = run.dt
    AND tk.category = 'strikeouts'
),

team_k_season AS (
  SELECT team_id, team_name,
    rank  AS team_k_rank_season,
    value AS team_k_total_season
  FROM team_k_deduped
  WHERE split = 'Season' AND _rn = 1
),

team_k_vs_hand AS (
  SELECT team_id,
    split AS team_hand_split,
    rank  AS team_k_rank_vs_hand,
    value AS team_k_total_vs_hand
  FROM team_k_deduped
  WHERE split IN ('vs LHP', 'vs RHP') AND _rn = 1
),

team_k_recent AS (
  SELECT team_id,
    rank  AS team_k_rank_l15,
    value AS team_k_total_l15
  FROM team_k_deduped
  WHERE split = 'L15 Days' AND _rn = 1
),

-- 5. Deduplicate pitch log + arsenal whiff/K aggregation (usage-weighted)
pitch_log_deduped AS (
  SELECT
    pl.*,
    ROW_NUMBER() OVER (
      PARTITION BY pl.pitcher_id, pl.game_pk, pl.batter_hand, pl.pitch_code
      ORDER BY pl.ingested_at DESC
    ) AS _rn
  FROM `graphite-flare-477419-h7.propfinder.raw_pitch_log` pl
  CROSS JOIN run
  WHERE pl.run_date = run.dt
),

arsenal AS (
  SELECT
    pitcher_id,
    game_pk,
    SAFE_DIVIDE(
      SUM(whiff * count),
      SUM(count)
    ) AS arsenal_whiff_rate,
    SAFE_DIVIDE(
      SUM(k_percent * count),
      SUM(count)
    ) AS arsenal_k_pct,
    MAX(whiff) AS max_pitch_whiff,
    COUNT(DISTINCT pitch_code) AS pitch_type_count,
    SUM(count) AS total_pitches
  FROM pitch_log_deduped
  WHERE _rn = 1
  GROUP BY pitcher_id, game_pk
),

-- 6. League-average team K total (for adjustment factor)
league_avg_k AS (
  SELECT AVG(value) AS avg_k
  FROM team_k_deduped
  WHERE split = 'Season' AND _rn = 1
),

-- 7. Game context (deduplicated)
game_ctx AS (
  SELECT
    gw.game_pk,
    gw.home_team_id,
    gw.away_team_id,
    gw.home_team_name,
    gw.away_team_name,
    gw.ballpark_name,
    gw.over_under
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY ingested_at DESC) AS _rn
    FROM `graphite-flare-477419-h7.propfinder.raw_game_weather`
    WHERE run_date = (SELECT dt FROM run)
  ) gw
  WHERE gw._rn = 1
),

-- 8. Assemble + score
assembled AS (
  SELECT
    ps.run_date,
    ps.game_pk,
    ps.pitcher_id,
    ps.pitcher_name,
    ps.pitcher_hand,
    ps.opp_team_id,
    gc.home_team_name,
    gc.away_team_name,
    gc.ballpark_name,
    gc.over_under,

    -- Pitcher season K stats
    ps.ip,
    ps.strikeouts,
    ps.strikeouts_per_9,
    ps.k_pct,
    ps.strike_pct,
    ps.strikeout_walk_ratio,
    ps.batters_faced,
    ps.whip,
    ps.woba,

    -- Pitcher vs-hand K stats (matched to opposing lineup handedness)
    pvh.hand_split,
    pvh.hand_k_per_9,
    pvh.hand_k_pct,
    pvh.hand_k_bb,

    -- Opposing team K vulnerability
    tks.team_k_rank_season,
    tks.team_k_total_season,
    tkh.team_k_rank_vs_hand,
    tkh.team_k_total_vs_hand,
    tkr.team_k_rank_l15,
    tkr.team_k_total_l15,

    -- Arsenal metrics
    a.arsenal_whiff_rate,
    a.arsenal_k_pct     AS arsenal_k_pct,
    a.max_pitch_whiff,
    a.pitch_type_count,
    a.total_pitches,

    -- ── Rough projections ──
    -- Estimated starts = season IP / 5.5 (league-avg IP/start)
    -- Avg IP/start and BF/start from pitcher's own season pace
    ROUND(SAFE_DIVIDE(ps.ip, GREATEST(ROUND(ps.ip / 5.5, 0), 1)), 1)
      AS avg_ip_per_start,

    ROUND(SAFE_DIVIDE(ps.batters_faced, GREATEST(ROUND(ps.ip / 5.5, 0), 1)), 1)
      AS avg_bf_per_start,

    -- Team K adjustment: opp team's season Ks / league average
    -- >1.0 = team strikes out more than average (good for pitcher)
    ROUND(SAFE_DIVIDE(
      CAST(tks.team_k_total_season AS FLOAT64),
      lak.avg_k
    ), 3) AS team_k_adj,

    -- Projected IP this start (pitcher's avg pace, no adjustment)
    ROUND(SAFE_DIVIDE(ps.ip, GREATEST(ROUND(ps.ip / 5.5, 0), 1)), 1)
      AS proj_ip,

    -- Projected outs = projected IP * 3
    ROUND(SAFE_DIVIDE(ps.ip, GREATEST(ROUND(ps.ip / 5.5, 0), 1)) * 3, 0)
      AS proj_outs,

    -- Projected Ks = (K% / 100) * avg_bf_per_start * team_k_adjustment
    ROUND(
      (COALESCE(ps.k_pct, 0) / 100.0)
      * SAFE_DIVIDE(ps.batters_faced, GREATEST(ROUND(ps.ip / 5.5, 0), 1))
      * COALESCE(SAFE_DIVIDE(CAST(tks.team_k_total_season AS FLOAT64), lak.avg_k), 1.0)
    , 1) AS proj_ks,

    -- ── Composite strikeout signal (0-100 scale) ──
    -- 40% pitcher K rate | 30% team K vulnerability | 30% arsenal whiff
    ROUND(
      -- Pitcher K/9 component: scale 6-12 K/9 to 0-100
      0.40 * LEAST(100, GREATEST(0,
        (COALESCE(ps.strikeouts_per_9, 0) - 6.0) / 6.0 * 100
      ))
      -- Team K vulnerability: rank 1=most Ks (good for pitcher), 30=fewest
      -- invert so rank 1 → 100, rank 30 → 0
      + 0.30 * LEAST(100, GREATEST(0,
        (31.0 - COALESCE(tks.team_k_rank_season, 15)) / 30.0 * 100
      ))
      -- Arsenal whiff: scale 15-40% whiff to 0-100
      + 0.30 * LEAST(100, GREATEST(0,
        (COALESCE(a.arsenal_whiff_rate, 0) - 0.15) / 0.25 * 100
      ))
    , 1) AS k_signal_score

  FROM pitcher_season ps

  CROSS JOIN league_avg_k lak

  -- Handedness-matched pitcher split:
  -- LHP pitcher → how they do vsRHB (majority of lineup), RHP → vsLHB
  LEFT JOIN pitcher_vs_hand pvh
    ON pvh.pitcher_id = ps.pitcher_id
    AND pvh.game_pk = ps.game_pk
    AND pvh.hand_split = CASE ps.pitcher_hand
      WHEN 'LHP' THEN 'vsRHB'
      WHEN 'RHP' THEN 'vsLHB'
      ELSE 'vsRHB'
    END

  -- Opposing team season K ranking
  LEFT JOIN team_k_season tks
    ON tks.team_id = ps.opp_team_id

  -- Opposing team handedness-matched K ranking
  LEFT JOIN team_k_vs_hand tkh
    ON tkh.team_id = ps.opp_team_id
    AND tkh.team_hand_split = CASE ps.pitcher_hand
      WHEN 'LHP' THEN 'vs LHP'
      WHEN 'RHP' THEN 'vs RHP'
      ELSE 'vs RHP'
    END

  -- Opposing team recent (L15 days) K ranking
  LEFT JOIN team_k_recent tkr
    ON tkr.team_id = ps.opp_team_id

  -- Pitch arsenal aggregation
  LEFT JOIN arsenal a
    ON a.pitcher_id = ps.pitcher_id
    AND a.game_pk = ps.game_pk

  -- Game context
  LEFT JOIN game_ctx gc
    ON gc.game_pk = ps.game_pk
)

SELECT
  *,
  RANK() OVER (ORDER BY k_signal_score DESC) AS k_signal_rank
FROM assembled
ORDER BY k_signal_score DESC;
