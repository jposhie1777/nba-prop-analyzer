CREATE TABLE IF NOT EXISTS `atp_data.website_raw_responses` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  endpoint_key STRING NOT NULL,
  source_file STRING,
  url STRING,
  content_type STRING,
  request_header STRING,
  payload_json STRING,
  payload_text STRING
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY endpoint_key;

CREATE TABLE IF NOT EXISTS `atp_data.website_tournament_months` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  display_month STRING,
  is_expanded BOOL,
  no_events INT64
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY display_month;

CREATE TABLE IF NOT EXISTS `atp_data.website_tournaments` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  month_display_date STRING,
  tournament_id STRING,
  name STRING,
  location STRING,
  formatted_date STRING,
  type STRING,
  event_type STRING,
  event_type_detail INT64,
  surface STRING,
  indoor_outdoor STRING,
  sgl_draw_size INT64,
  dbl_draw_size INT64,
  total_financial_commitment STRING,
  prize_money_details STRING,
  scores_url STRING,
  draws_url STRING,
  schedule_url STRING,
  tournament_site_url STRING,
  overview_url STRING,
  tickets_url STRING,
  ticket_hotline STRING,
  phone_number STRING,
  email STRING,
  pdf_schedule_url STRING,
  pdf_mds_url STRING,
  pdf_mdd_url STRING,
  pdf_qs_url STRING,
  is_live BOOL,
  is_past_event BOOL
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY tournament_id, type;

CREATE TABLE IF NOT EXISTS `atp_data.website_daily_schedule` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  url STRING,
  payload_html STRING
)
PARTITION BY DATE(snapshot_ts_utc);

CREATE TABLE IF NOT EXISTS `atp_data.website_upcoming_matches` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  tournament_slug STRING,
  tournament_id STRING,
  day STRING,
  court_name STRING,
  schedule_type STRING,
  start_label STRING,
  player_1_name STRING,
  player_1_profile_url STRING,
  player_1_seed STRING,
  player_2_name STRING,
  player_2_profile_url STRING,
  player_2_seed STRING,
  status_text STRING
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY tournament_id, tournament_slug;

CREATE TABLE IF NOT EXISTS `atp_data.website_draws` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  url STRING,
  payload_html STRING
)
PARTITION BY DATE(snapshot_ts_utc);

CREATE TABLE IF NOT EXISTS `atp_data.website_tournament_bracket` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  tournament_slug STRING,
  tournament_id STRING,
  bracket_html STRING
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY tournament_id, tournament_slug;

CREATE TABLE IF NOT EXISTS `atp_data.website_head_to_head` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  url STRING,
  payload_json STRING
)
PARTITION BY DATE(snapshot_ts_utc);

CREATE TABLE IF NOT EXISTS `atp_data.website_head_to_head_matches` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  left_player_id STRING,
  right_player_id STRING,
  event_id STRING,
  event_year INT64,
  tournament_name STRING,
  surface STRING,
  in_outdoor_display STRING,
  match_id STRING,
  winner_player_id STRING,
  is_doubles BOOL,
  is_qualifier BOOL,
  round_short_name STRING,
  round_long_name STRING,
  match_time STRING,
  is_match_live BOOL,
  player_set_scores STRING,
  opponent_set_scores STRING,
  scoreline_display STRING
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY left_player_id, right_player_id;

CREATE TABLE IF NOT EXISTS `atp_data.website_match_results` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  url STRING,
  payload_html STRING
)
PARTITION BY DATE(snapshot_ts_utc);

CREATE TABLE IF NOT EXISTS `atp_data.website_match_results_rows` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  tournament_slug STRING,
  tournament_id STRING,
  day_label STRING,
  match_date DATE,
  round_and_court STRING,
  match_duration STRING,
  player_1_name STRING,
  player_1_profile_url STRING,
  player_1_is_winner BOOL,
  player_1_scores STRING,
  player_2_name STRING,
  player_2_profile_url STRING,
  player_2_is_winner BOOL,
  player_2_scores STRING,
  h2h_url STRING,
  stats_url STRING,
  umpire STRING,
  match_notes STRING
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY tournament_id, tournament_slug;

CREATE TABLE IF NOT EXISTS `atp_data.website_player_stats` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  court_type STRING,
  url STRING,
  payload_json STRING
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY court_type;

CREATE TABLE IF NOT EXISTS `atp_data.website_player_stats_records` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  court_type STRING,
  stat_name STRING,
  stat_value STRING
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY court_type, stat_name;

CREATE TABLE IF NOT EXISTS `atp_data.website_who_is_playing` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  url STRING,
  payload_json STRING
)
PARTITION BY DATE(snapshot_ts_utc);

CREATE TABLE IF NOT EXISTS `atp_data.website_who_is_playing_players` (
  snapshot_ts_utc TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  profile_url STRING,
  country_flag_url STRING
)
PARTITION BY DATE(snapshot_ts_utc)
CLUSTER BY last_name, first_name;
