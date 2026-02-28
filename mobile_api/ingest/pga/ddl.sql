-- BigQuery DDL for PGA data tables
-- Dataset: pga_data

-- ─── Latest pairings snapshot ────────────────────────────────────────────────
-- One row per player × round × group, always reflecting the most-recent ingest.
-- Use this view as the base for all pairing analytics.
CREATE OR REPLACE VIEW `pga_data.v_pairings_latest` AS
SELECT * EXCEPT (row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY tournament_id, round_number, group_number, player_id
      ORDER BY run_ts DESC
    ) AS row_num
  FROM `pga_data.tournament_round_pairings`
)
WHERE row_num = 1;
-- ─────────────────────────────────────────────────────────────────────────────

-- ─── Per-player form / placement stats (last 3 seasons) ──────────────────────
-- Mirrors Python finish_value() / is_cut() logic.
-- Requires: tournament_results, tournaments
-- Used by: v_pairings_analytics, pga/analytics/pairings endpoint
CREATE OR REPLACE VIEW `pga_data.v_player_stats` AS
WITH
seasons AS (
  SELECT s AS season
  FROM UNNEST(
    GENERATE_ARRAY(EXTRACT(YEAR FROM CURRENT_DATE()) - 2,
                   EXTRACT(YEAR FROM CURRENT_DATE()))
  ) s
),
latest_results AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY tournament_id, player_id, season ORDER BY run_ts DESC
      ) AS rn
    FROM `pga_data.tournament_results`
  )
  WHERE rn = 1
),
latest_tournaments AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY tournament_id, season ORDER BY run_ts DESC
      ) AS rn
    FROM `pga_data.tournaments`
  )
  WHERE rn = 1
),
results_in_scope AS (
  SELECT
    r.player_id,
    r.player_display_name,
    COALESCE(t.start_date, r.tournament_start_date)                       AS start_date,
    r.position,
    r.position_numeric,
    CASE
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(r.position, '')),
                           r'^(MC|CUT|WD|DQ|MDF|DMQ)') THEN 80.0
      ELSE COALESCE(CAST(r.position_numeric AS FLOAT64), 80.0)
    END AS finish_value,
    CASE
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(r.position, '')),
                           r'^(MC|CUT|WD|DQ|MDF|DMQ)') THEN 1 ELSE 0
    END AS is_cut
  FROM latest_results r
  INNER JOIN seasons           s ON r.season          = s.season
  LEFT  JOIN latest_tournaments t ON t.tournament_id  = r.tournament_id
                                  AND t.season        = r.season
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY start_date DESC) AS rn
  FROM results_in_scope
),
form AS (
  SELECT
    player_id,
    ANY_VALUE(player_display_name)                       AS player_display_name,
    COUNT(*)                                             AS form_starts,
    ROUND(AVG(finish_value),                         2) AS avg_finish,
    ROUND(AVG(IF(position_numeric <= 10, 1.0, 0.0)), 3) AS top10_rate,
    ROUND(AVG(IF(position_numeric <= 20, 1.0, 0.0)), 3) AS top20_rate,
    ROUND(AVG(CAST(is_cut AS FLOAT64)),              3) AS cut_rate
  FROM ranked WHERE rn <= 10
  GROUP BY player_id HAVING COUNT(*) >= 2
),
placement AS (
  SELECT
    player_id,
    COUNT(*)                                             AS placement_starts,
    ROUND(AVG(IF(position_numeric <= 5,  1.0, 0.0)), 3) AS top5_prob,
    ROUND(AVG(IF(position_numeric <= 10, 1.0, 0.0)), 3) AS top10_prob,
    ROUND(AVG(IF(position_numeric <= 20, 1.0, 0.0)), 3) AS top20_prob
  FROM ranked WHERE rn <= 20
  GROUP BY player_id HAVING COUNT(*) >= 3
)
SELECT
  f.player_id,
  f.player_display_name,
  LOWER(TRIM(f.player_display_name))                   AS player_display_name_norm,
  f.form_starts,
  f.avg_finish,
  f.top10_rate,
  f.top20_rate,
  f.cut_rate,
  ROUND(
    (f.top10_rate * 0.5) + (f.top20_rate * 0.3)
    + ((1 - f.cut_rate) * 0.2) - (f.avg_finish / 100.0),
    4
  )                AS form_score,
  p.placement_starts,
  p.top5_prob,
  p.top10_prob,
  p.top20_prob
FROM form f
LEFT JOIN placement p USING (player_id);

-- ─── Pairings + analytics (single-query API source) ──────────────────────────
-- LEFT JOINs v_pairings_latest with v_player_stats.
-- pairings.player_id = PGA Tour GraphQL ID (STRING)
-- tournament_results.player_id = BallDontLie ID (INT64)  ← different namespaces
-- So we join on normalised display name instead.
-- Queried by: pga/analytics/pairings endpoint (fetch_pairings_analytics)
CREATE OR REPLACE VIEW `pga_data.v_pairings_analytics` AS
SELECT
  p.tournament_id,
  p.round_number,
  p.round_status,
  p.group_number,
  p.tee_time,
  p.start_hole,
  p.back_nine,
  p.course_id,
  p.course_name,
  p.player_id,
  p.player_display_name,
  p.player_first_name,
  p.player_last_name,
  p.country,
  p.world_rank,
  p.amateur,
  p.run_ts,
  s.player_id                      AS bdl_player_id,
  s.form_score,
  s.form_starts,
  s.avg_finish,
  s.top10_rate,
  s.top20_rate,
  s.cut_rate,
  s.placement_starts,
  s.top5_prob,
  s.top10_prob,
  s.top20_prob
FROM `pga_data.v_pairings_latest`  p
LEFT JOIN `pga_data.v_player_stats` s
       ON LOWER(TRIM(p.player_display_name)) = s.player_display_name_norm;

-- ─────────────────────────────────────────────────────────────────────────────



-- Optional: create dataset (run once)
-- CREATE SCHEMA IF NOT EXISTS `pga_data`;

CREATE TABLE IF NOT EXISTS `pga_data.players` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  player_id INT64 NOT NULL,
  first_name STRING,
  last_name STRING,
  display_name STRING,
  country STRING,
  country_code STRING,
  height STRING,
  weight STRING,
  birth_date STRING,
  birthplace_city STRING,
  birthplace_state STRING,
  birthplace_country STRING,
  turned_pro STRING,
  school STRING,
  residence_city STRING,
  residence_state STRING,
  residence_country STRING,
  owgr INT64,
  active BOOL
)
CLUSTER BY player_id
OPTIONS (
  description = 'PGA players (BallDontLie PGA API)'
);

CREATE TABLE IF NOT EXISTS `pga_data.courses` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  course_id INT64 NOT NULL,
  name STRING,
  city STRING,
  state STRING,
  country STRING,
  par INT64,
  yardage STRING,
  established STRING,
  architect STRING,
  fairway_grass STRING,
  rough_grass STRING,
  green_grass STRING
)
CLUSTER BY course_id
OPTIONS (
  description = 'PGA courses (BallDontLie PGA API)'
);

CREATE TABLE IF NOT EXISTS `pga_data.tournaments` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  tournament_id INT64 NOT NULL,
  season INT64,
  name STRING,
  start_date TIMESTAMP,
  end_date STRING,
  city STRING,
  state STRING,
  country STRING,
  course_name STRING,
  purse STRING,
  status STRING,
  champion_id INT64,
  champion_display_name STRING,
  champion_country STRING,
  courses STRING
)
PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY tournament_id, season
OPTIONS (
  description = 'PGA tournaments (BallDontLie PGA API); courses stored as JSON string'
);

CREATE TABLE IF NOT EXISTS `pga_data.tournament_results` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  season INT64,
  tournament_id INT64 NOT NULL,
  tournament_name STRING,
  tournament_start_date TIMESTAMP,
  player_id INT64 NOT NULL,
  player_display_name STRING,
  position STRING,
  position_numeric INT64,
  total_score INT64,
  par_relative_score INT64
)
PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY tournament_id, player_id
OPTIONS (
  description = 'PGA tournament results (BallDontLie PGA API)'
);

CREATE TABLE IF NOT EXISTS `pga_data.tournament_round_scores` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  season INT64,
  tournament_id INT64 NOT NULL,
  tournament_name STRING,
  tournament_start_date TIMESTAMP,
  round_number INT64,
  round_date DATE,
  player_id INT64 NOT NULL,
  player_display_name STRING,
  round_score INT64,
  par_relative_score INT64,
  total_score INT64
)
PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY tournament_id, player_id, round_number
OPTIONS (
  description = 'PGA tournament round scores (BallDontLie PGA API)'
);

CREATE TABLE IF NOT EXISTS `pga_data.tournament_course_stats` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  season INT64,
  tournament_id INT64 NOT NULL,
  tournament_name STRING,
  course_id INT64 NOT NULL,
  course_name STRING,
  hole_number INT64,
  round_number INT64,
  scoring_average FLOAT64,
  scoring_diff FLOAT64,
  difficulty_rank INT64,
  eagles INT64,
  birdies INT64,
  pars INT64,
  bogeys INT64,
  double_bogeys INT64
)
PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY tournament_id, course_id
OPTIONS (
  description = 'PGA tournament hole stats (BallDontLie PGA API)'
);

CREATE TABLE IF NOT EXISTS `pga_data.course_holes` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  course_id INT64 NOT NULL,
  course_name STRING,
  hole_number INT64,
  par INT64,
  yardage INT64
)
CLUSTER BY course_id
OPTIONS (
  description = 'PGA course holes (BallDontLie PGA API)'
);

CREATE TABLE IF NOT EXISTS `pga_data.player_stats` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  tour_code STRING,
  year INT64,
  stat_id STRING,
  stat_name STRING,
  tour_avg STRING,
  player_id STRING,
  player_name STRING,
  stat_title STRING,
  stat_value STRING,
  rank INT64,
  country STRING,
  country_flag STRING
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY tour_code, stat_id, player_id
OPTIONS (
  description = 'PGA Tour per-stat player rankings (statOverview GraphQL)'
);

CREATE TABLE IF NOT EXISTS `pga_data.priority_rankings` (
  run_ts TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  tour_code STRING,
  year INT64,
  display_year STRING,
  through_text STRING,
  category_name STRING,
  rank INT64,
  player_id STRING,
  display_name STRING
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY tour_code, year, category_name, player_id
OPTIONS (
  description = 'PGA Tour priority rankings / FedEx Cup standings (priorityRankings GraphQL)'
);
