-- BigQuery DDL for game_advanced_stats table
-- Stores NBA Game Advanced Stats V2 from Balldontlie API
-- https://nba.balldontlie.io/#attributes-v2

CREATE TABLE IF NOT EXISTS `nba_live.game_advanced_stats` (
    -- Metadata
    id INT64 NOT NULL,                           -- Unique stat record ID from API
    run_ts TIMESTAMP NOT NULL,                   -- Batch run timestamp
    ingested_at TIMESTAMP NOT NULL,              -- Row insert timestamp

    -- Period info (0 = full game, 1-4 = quarters, 5+ = OT)
    period INT64,

    -- Player info
    player_id INT64 NOT NULL,
    player_first_name STRING,
    player_last_name STRING,
    player_position STRING,
    player_height STRING,
    player_weight STRING,
    player_jersey_number STRING,
    player_college STRING,
    player_country STRING,
    player_draft_year INT64,
    player_draft_round INT64,
    player_draft_number INT64,

    -- Team info
    team_id INT64,
    team_conference STRING,
    team_division STRING,
    team_city STRING,
    team_name STRING,
    team_full_name STRING,
    team_abbreviation STRING,

    -- Game info
    game_id INT64 NOT NULL,
    game_date DATE NOT NULL,
    game_season INT64,
    game_status STRING,
    game_period INT64,
    game_time STRING,
    game_postseason BOOL,
    game_postponed BOOL,
    home_team_score INT64,
    visitor_team_score INT64,
    home_team_id INT64,
    visitor_team_id INT64,

    -- Extended game info (V2)
    game_datetime TIMESTAMP,
    home_q1 INT64,
    home_q2 INT64,
    home_q3 INT64,
    home_q4 INT64,
    home_ot1 INT64,
    home_ot2 INT64,
    home_ot3 INT64,
    home_timeouts_remaining INT64,
    home_in_bonus BOOL,
    visitor_q1 INT64,
    visitor_q2 INT64,
    visitor_q3 INT64,
    visitor_q4 INT64,
    visitor_ot1 INT64,
    visitor_ot2 INT64,
    visitor_ot3 INT64,
    visitor_timeouts_remaining INT64,
    visitor_in_bonus BOOL,
    ist_stage STRING,

    -- Core Advanced Stats
    pie FLOAT64,                                 -- Player Impact Estimate
    pace FLOAT64,                                -- Possessions per 48 minutes
    pace_per_40 FLOAT64,
    possessions FLOAT64,
    assist_percentage FLOAT64,
    assist_ratio FLOAT64,
    assist_to_turnover FLOAT64,
    defensive_rating FLOAT64,
    offensive_rating FLOAT64,
    net_rating FLOAT64,
    estimated_defensive_rating FLOAT64,
    estimated_offensive_rating FLOAT64,
    estimated_net_rating FLOAT64,
    estimated_pace FLOAT64,
    estimated_usage_percentage FLOAT64,
    defensive_rebound_percentage FLOAT64,
    offensive_rebound_percentage FLOAT64,
    rebound_percentage FLOAT64,
    effective_field_goal_percentage FLOAT64,
    true_shooting_percentage FLOAT64,
    turnover_ratio FLOAT64,
    usage_percentage FLOAT64,

    -- Miscellaneous Stats
    blocks_against INT64,
    fouls_drawn INT64,
    points_fast_break INT64,
    points_off_turnovers INT64,
    points_paint INT64,
    points_second_chance INT64,
    opp_points_fast_break INT64,
    opp_points_off_turnovers INT64,
    opp_points_paint INT64,
    opp_points_second_chance INT64,

    -- Scoring Stats
    pct_assisted_2pt FLOAT64,
    pct_assisted_3pt FLOAT64,
    pct_assisted_fgm FLOAT64,
    pct_fga_2pt FLOAT64,
    pct_fga_3pt FLOAT64,
    pct_pts_2pt FLOAT64,
    pct_pts_3pt FLOAT64,
    pct_pts_fast_break FLOAT64,
    pct_pts_free_throw FLOAT64,
    pct_pts_midrange_2pt FLOAT64,
    pct_pts_off_turnovers FLOAT64,
    pct_pts_paint FLOAT64,
    pct_unassisted_2pt FLOAT64,
    pct_unassisted_3pt FLOAT64,
    pct_unassisted_fgm FLOAT64,

    -- Four Factors Stats
    four_factors_efg_pct FLOAT64,
    free_throw_attempt_rate FLOAT64,
    four_factors_oreb_pct FLOAT64,
    team_turnover_pct FLOAT64,
    opp_efg_pct FLOAT64,
    opp_free_throw_attempt_rate FLOAT64,
    opp_oreb_pct FLOAT64,
    opp_turnover_pct FLOAT64,

    -- Hustle Stats
    box_outs INT64,
    box_out_player_rebounds INT64,
    box_out_player_team_rebounds INT64,
    defensive_box_outs INT64,
    offensive_box_outs INT64,
    charges_drawn INT64,
    contested_shots INT64,
    contested_shots_2pt INT64,
    contested_shots_3pt INT64,
    deflections INT64,
    loose_balls_recovered_def INT64,
    loose_balls_recovered_off INT64,
    loose_balls_recovered_total INT64,
    screen_assists INT64,
    screen_assist_points INT64,

    -- Defensive Stats
    matchup_minutes STRING,
    matchup_fg_pct FLOAT64,
    matchup_fga INT64,
    matchup_fgm INT64,
    matchup_3pt_pct FLOAT64,
    matchup_3pa INT64,
    matchup_3pm INT64,
    matchup_assists INT64,
    matchup_turnovers INT64,
    partial_possessions FLOAT64,
    matchup_player_points INT64,
    switches_on INT64,

    -- Tracking Stats
    speed FLOAT64,
    distance FLOAT64,
    touches INT64,
    passes INT64,
    secondary_assists INT64,
    free_throw_assists INT64,
    contested_fga INT64,
    contested_fgm INT64,
    contested_fg_pct FLOAT64,
    uncontested_fga INT64,
    uncontested_fgm INT64,
    uncontested_fg_pct FLOAT64,
    defended_at_rim_fga INT64,
    defended_at_rim_fgm INT64,
    defended_at_rim_fg_pct FLOAT64,

    -- Usage Stats (additional)
    rebound_chances_def INT64,
    rebound_chances_off INT64,
    rebound_chances_total INT64,
    pct_blocks FLOAT64,
    pct_blocks_allowed FLOAT64,
    pct_fga FLOAT64,
    pct_fgm FLOAT64,
    pct_fta FLOAT64,
    pct_ftm FLOAT64,
    pct_personal_fouls FLOAT64,
    pct_personal_fouls_drawn FLOAT64,
    pct_points FLOAT64,
    pct_rebounds_def FLOAT64,
    pct_rebounds_off FLOAT64,
    pct_rebounds_total FLOAT64,
    pct_steals FLOAT64,
    pct_3pa FLOAT64,
    pct_3pm FLOAT64,
    pct_turnovers FLOAT64,
)
PARTITION BY game_date
CLUSTER BY player_id, game_id, team_id
OPTIONS (
    description = 'NBA Game Advanced Stats V2 from Balldontlie API. Partitioned by game_date for efficient querying.',
    labels = [("source", "balldontlie"), ("version", "v2")]
);
