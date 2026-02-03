-- BigQuery DDL for PGA data tables
-- Dataset: pga_data

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
  courses JSON
)
PARTITION BY RANGE_BUCKET(season, GENERATE_ARRAY(2015, 2035, 1))
CLUSTER BY tournament_id, season
OPTIONS (
  description = 'PGA tournaments (BallDontLie PGA API)'
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
