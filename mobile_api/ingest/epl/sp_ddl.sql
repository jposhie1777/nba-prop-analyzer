CALL `graphite-flare-477419-h7.soccer_data.run_soccer_daily`();


CREATE OR REPLACE PROCEDURE `graphite-flare-477419-h7.soccer_data.run_soccer_daily`()
BEGIN

  --------------------------------------------------------------------------------
  -- ================================ EPL =======================================
  --------------------------------------------------------------------------------

  -- 1) EPL matches_flat
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.epl_data.matches_flat`
  PARTITION BY DATE(ingested_at)
  CLUSTER BY match_id
  AS
  SELECT
    ingested_at,
    season AS season_partition,
    entity_id,

    SAFE_CAST(JSON_VALUE(payload, '$.id') AS INT64)           AS match_id,
    SAFE_CAST(JSON_VALUE(payload, '$.season') AS INT64)       AS season,
    SAFE_CAST(JSON_VALUE(payload, '$.home_team_id') AS INT64) AS home_team_id,
    SAFE_CAST(JSON_VALUE(payload, '$.away_team_id') AS INT64) AS away_team_id,

    TIMESTAMP(JSON_VALUE(payload, '$.date'))                  AS scheduled_time,

    JSON_VALUE(payload, '$.name')                             AS match_name,
    JSON_VALUE(payload, '$.short_name')                       AS short_name,
    JSON_VALUE(payload, '$.status')                           AS status,
    JSON_VALUE(payload, '$.status_detail')                    AS status_detail,

    SAFE_CAST(JSON_VALUE(payload, '$.home_score') AS INT64)   AS home_score,
    SAFE_CAST(JSON_VALUE(payload, '$.away_score') AS INT64)   AS away_score,

    JSON_VALUE(payload, '$.venue_name')                       AS venue_name,
    JSON_VALUE(payload, '$.venue_city')                       AS venue_city,
    SAFE_CAST(JSON_VALUE(payload, '$.attendance') AS INT64)   AS attendance
  FROM `graphite-flare-477419-h7.epl_data.matches`;

  -- 2) EPL match_events_flat (betting-grade)
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.epl_data.match_events_flat`
  PARTITION BY DATE(ingested_at)
  CLUSTER BY match_id, team_id, event_type
  AS
  SELECT
    ingested_at,
    season AS season_partition,
    entity_id,

    SAFE_CAST(JSON_VALUE(payload, '$.id') AS INT64)           AS event_id,
    SAFE_CAST(JSON_VALUE(payload, '$.match_id') AS INT64)     AS match_id,
    SAFE_CAST(JSON_VALUE(payload, '$.team_id') AS INT64)      AS team_id,
    JSON_VALUE(payload, '$.event_type')                       AS event_type,
    SAFE_CAST(JSON_VALUE(payload, '$.event_time') AS INT64)   AS event_minute,
    SAFE_CAST(JSON_VALUE(payload, '$.period') AS INT64)       AS period,

    SAFE_CAST(JSON_VALUE(payload, '$.player.id') AS INT64)    AS player_id,
    JSON_VALUE(payload, '$.player.display_name')              AS player_name,
    SAFE_CAST(JSON_VALUE(payload, '$.player.age') AS INT64)   AS player_age,
    JSON_VALUE(payload, '$.player.citizenship')               AS player_citizenship,

    SAFE_CAST(JSON_VALUE(payload, '$.secondary_player.id') AS INT64) AS assist_player_id,
    JSON_VALUE(payload, '$.secondary_player.display_name')            AS assist_player_name,

    JSON_VALUE(payload, '$.goal_type')                        AS goal_type,
    SAFE_CAST(JSON_VALUE(payload, '$.is_own_goal') AS BOOL)   AS is_own_goal,

    CASE WHEN JSON_VALUE(payload, '$.event_type') = 'goal' THEN TRUE ELSE FALSE END AS is_goal,
    CASE WHEN JSON_VALUE(payload, '$.event_type') IN ('yellow_card','red_card') THEN TRUE ELSE FALSE END AS is_card,
    CASE WHEN JSON_VALUE(payload, '$.event_type') = 'yellow_card' THEN TRUE ELSE FALSE END AS is_yellow_card,
    CASE WHEN JSON_VALUE(payload, '$.event_type') = 'red_card' THEN TRUE ELSE FALSE END AS is_red_card,
    CASE WHEN JSON_VALUE(payload, '$.event_type') = 'substitution' THEN TRUE ELSE FALSE END AS is_substitution
  FROM `graphite-flare-477419-h7.epl_data.match_events`;

  -- 3) EPL standings_flat
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.epl_data.standings_flat`
  PARTITION BY DATE(ingested_at)
  CLUSTER BY season, team_id
  AS
  SELECT
    ingested_at,
    season AS season_partition,
    entity_id,

    SAFE_CAST(JSON_VALUE(payload, '$.team.id') AS INT64)      AS team_id,
    JSON_VALUE(payload, '$.team.name')                        AS team_name,
    JSON_VALUE(payload, '$.team.short_name')                  AS team_short_name,
    JSON_VALUE(payload, '$.team.abbreviation')                AS team_abbreviation,
    JSON_VALUE(payload, '$.team.location')                    AS team_location,

    SAFE_CAST(JSON_VALUE(payload, '$.season') AS INT64)       AS season,
    JSON_VALUE(payload, '$.group_name')                       AS league_name,
    JSON_VALUE(payload, '$.note')                             AS standing_note,

    SAFE_CAST(JSON_VALUE(payload, '$.rank') AS INT64)         AS rank,
    SAFE_CAST(JSON_VALUE(payload, '$.rank_change') AS INT64)  AS rank_change,

    SAFE_CAST(JSON_VALUE(payload, '$.games_played') AS INT64) AS games_played,
    SAFE_CAST(JSON_VALUE(payload, '$.wins') AS INT64)         AS wins,
    SAFE_CAST(JSON_VALUE(payload, '$.losses') AS INT64)       AS losses,
    SAFE_CAST(JSON_VALUE(payload, '$.draws') AS INT64)        AS draws,
    SAFE_CAST(JSON_VALUE(payload, '$.points') AS INT64)       AS points,
    SAFE_CAST(JSON_VALUE(payload, '$.goals_for') AS INT64)    AS goals_for,
    SAFE_CAST(JSON_VALUE(payload, '$.goals_against') AS INT64) AS goals_against,
    SAFE_CAST(JSON_VALUE(payload, '$.goal_differential') AS INT64) AS goal_differential,

    ROUND(SAFE_CAST(JSON_VALUE(payload, '$.points_per_game') AS FLOAT64), 2) AS points_per_game
  FROM `graphite-flare-477419-h7.epl_data.standings`;

  -- 4) EPL team_matches (completed only)
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.epl_data.team_matches`
  PARTITION BY DATE(scheduled_time)
  CLUSTER BY team_id
  AS
  SELECT
    match_id, scheduled_time, season, status,
    home_team_id AS team_id,
    away_team_id AS opponent_id,
    home_score AS goals_scored,
    away_score AS goals_allowed,
    TRUE AS is_home
  FROM `graphite-flare-477419-h7.epl_data.matches_flat`
  WHERE status = 'STATUS_FULL_TIME'

  UNION ALL

  SELECT
    match_id, scheduled_time, season, status,
    away_team_id AS team_id,
    home_team_id AS opponent_id,
    away_score AS goals_scored,
    home_score AS goals_allowed,
    FALSE AS is_home
  FROM `graphite-flare-477419-h7.epl_data.matches_flat`
  WHERE status = 'STATUS_FULL_TIME';

  -- 5) EPL team_betting_metrics (rounded to 1 decimal)
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.epl_data.team_betting_metrics`
  CLUSTER BY team_id
  AS
  WITH ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY scheduled_time DESC) AS game_rank
    FROM `graphite-flare-477419-h7.epl_data.team_matches`
  )
  SELECT
    team_id,
    COUNT(*) AS season_games,

    ROUND(AVG(goals_scored), 1) AS season_avg_goals_scored,
    ROUND(AVG(goals_allowed), 1) AS season_avg_goals_allowed,
    ROUND(AVG(CASE WHEN goals_scored > 0 THEN 1 ELSE 0 END), 1) AS season_score_rate,
    ROUND(AVG(CASE WHEN goals_allowed > 0 THEN 1 ELSE 0 END), 1) AS season_allow_rate,

    ROUND(AVG(IF(game_rank <= 10, goals_scored, NULL)), 1) AS last10_avg_scored,
    ROUND(AVG(IF(game_rank <= 10, goals_allowed, NULL)), 1) AS last10_avg_allowed,

    ROUND(AVG(IF(game_rank <= 5, goals_scored, NULL)), 1) AS last5_avg_scored,
    ROUND(AVG(IF(game_rank <= 5, goals_allowed, NULL)), 1) AS last5_avg_allowed,

    ROUND(AVG(IF(game_rank <= 3, goals_scored, NULL)), 1) AS last3_avg_scored,
    ROUND(AVG(IF(game_rank <= 3, goals_allowed, NULL)), 1) AS last3_avg_allowed
  FROM ranked
  GROUP BY team_id;

  -- 6) EPL match_card_totals
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.epl_data.match_card_totals`
  CLUSTER BY match_id, team_id
  AS
  WITH cards AS (
    SELECT
      match_id,
      team_id,
      COUNTIF(is_card) AS team_cards
    FROM `graphite-flare-477419-h7.epl_data.match_events_flat`
    WHERE is_card = TRUE
    GROUP BY match_id, team_id
  ),
  expanded AS (
    SELECT
      tm.match_id,
      tm.team_id,
      tm.opponent_id,
      tm.scheduled_time,
      COALESCE(c.team_cards, 0) AS team_cards
    FROM `graphite-flare-477419-h7.epl_data.team_matches` tm
    LEFT JOIN cards c
      ON tm.match_id = c.match_id
     AND tm.team_id = c.team_id
  )
  SELECT
    e.match_id,
    e.team_id,
    e.opponent_id,
    e.scheduled_time,
    e.team_cards,
    COALESCE(o.team_cards, 0) AS opponent_cards,
    e.team_cards + COALESCE(o.team_cards, 0) AS total_cards
  FROM expanded e
  LEFT JOIN expanded o
    ON e.match_id = o.match_id
   AND e.opponent_id = o.team_id;

  -- 7) EPL team_card_metrics (rounded to 1 decimal)
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.epl_data.team_card_metrics`
  CLUSTER BY team_id
  AS
  WITH ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY scheduled_time DESC) AS game_rank
    FROM `graphite-flare-477419-h7.epl_data.match_card_totals`
  )
  SELECT
    team_id,
    COUNT(*) AS season_games,

    ROUND(AVG(team_cards), 1)     AS season_team_cards_pg,
    ROUND(AVG(opponent_cards), 1) AS season_opponent_cards_pg,
    ROUND(AVG(total_cards), 1)    AS season_total_cards_pg,

    ROUND(AVG(IF(game_rank <= 10, team_cards, NULL)), 1)     AS l10_team_cards_pg,
    ROUND(AVG(IF(game_rank <= 10, opponent_cards, NULL)), 1) AS l10_opponent_cards_pg,
    ROUND(AVG(IF(game_rank <= 10, total_cards, NULL)), 1)    AS l10_total_cards_pg,

    ROUND(AVG(IF(game_rank <= 5, team_cards, NULL)), 1)      AS l5_team_cards_pg,
    ROUND(AVG(IF(game_rank <= 5, opponent_cards, NULL)), 1)  AS l5_opponent_cards_pg,
    ROUND(AVG(IF(game_rank <= 5, total_cards, NULL)), 1)     AS l5_total_cards_pg,

    ROUND(AVG(IF(game_rank <= 3, team_cards, NULL)), 1)      AS l3_team_cards_pg,
    ROUND(AVG(IF(game_rank <= 3, opponent_cards, NULL)), 1)  AS l3_opponent_cards_pg,
    ROUND(AVG(IF(game_rank <= 3, total_cards, NULL)), 1)     AS l3_total_cards_pg
  FROM ranked
  GROUP BY team_id;

  -- 8) EPL team_master_metrics (goals + cards + latest standings)
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.epl_data.team_master_metrics`
  CLUSTER BY team_id
  AS
  WITH latest_standings AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY ingested_at DESC) AS rn
      FROM `graphite-flare-477419-h7.epl_data.standings_flat`
    )
    WHERE rn = 1
  )
  SELECT
    g.team_id,

    s.team_name,
    s.team_short_name,
    s.rank,
    s.points,
    s.goal_differential,
    s.points_per_game,
    s.standing_note,

    g.season_games,
    g.season_avg_goals_scored,
    g.season_avg_goals_allowed,
    g.season_score_rate,
    g.season_allow_rate,
    g.last10_avg_scored,
    g.last10_avg_allowed,
    g.last5_avg_scored,
    g.last5_avg_allowed,
    g.last3_avg_scored,
    g.last3_avg_allowed,

    c.season_team_cards_pg,
    c.season_opponent_cards_pg,
    c.season_total_cards_pg,
    c.l10_team_cards_pg,
    c.l10_opponent_cards_pg,
    c.l10_total_cards_pg,
    c.l5_team_cards_pg,
    c.l5_opponent_cards_pg,
    c.l5_total_cards_pg,
    c.l3_team_cards_pg,
    c.l3_opponent_cards_pg,
    c.l3_total_cards_pg
  FROM `graphite-flare-477419-h7.epl_data.team_betting_metrics` g
  LEFT JOIN `graphite-flare-477419-h7.epl_data.team_card_metrics` c
    ON g.team_id = c.team_id
  LEFT JOIN latest_standings s
    ON g.team_id = s.team_id;

  --------------------------------------------------------------------------------
  -- ============================== LA LIGA =====================================
  --------------------------------------------------------------------------------

  -- 1) La Liga matches_flat
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.laliga_data.matches_flat`
  PARTITION BY DATE(ingested_at)
  CLUSTER BY match_id
  AS
  SELECT
    ingested_at,
    season AS season_partition,
    entity_id,

    SAFE_CAST(JSON_VALUE(payload, '$.id') AS INT64)           AS match_id,
    SAFE_CAST(JSON_VALUE(payload, '$.season') AS INT64)       AS season,
    SAFE_CAST(JSON_VALUE(payload, '$.home_team_id') AS INT64) AS home_team_id,
    SAFE_CAST(JSON_VALUE(payload, '$.away_team_id') AS INT64) AS away_team_id,

    TIMESTAMP(JSON_VALUE(payload, '$.date'))                  AS scheduled_time,

    JSON_VALUE(payload, '$.name')                             AS match_name,
    JSON_VALUE(payload, '$.short_name')                       AS short_name,
    JSON_VALUE(payload, '$.status')                           AS status,
    JSON_VALUE(payload, '$.status_detail')                    AS status_detail,

    SAFE_CAST(JSON_VALUE(payload, '$.home_score') AS INT64)   AS home_score,
    SAFE_CAST(JSON_VALUE(payload, '$.away_score') AS INT64)   AS away_score,

    JSON_VALUE(payload, '$.venue_name')                       AS venue_name,
    JSON_VALUE(payload, '$.venue_city')                       AS venue_city,
    SAFE_CAST(JSON_VALUE(payload, '$.attendance') AS INT64)   AS attendance
  FROM `graphite-flare-477419-h7.laliga_data.matches`;

  -- 2) La Liga match_events_flat
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.laliga_data.match_events_flat`
  PARTITION BY DATE(ingested_at)
  CLUSTER BY match_id, team_id, event_type
  AS
  SELECT
    ingested_at,
    season AS season_partition,
    entity_id,

    SAFE_CAST(JSON_VALUE(payload, '$.id') AS INT64)           AS event_id,
    SAFE_CAST(JSON_VALUE(payload, '$.match_id') AS INT64)     AS match_id,
    SAFE_CAST(JSON_VALUE(payload, '$.team_id') AS INT64)      AS team_id,
    JSON_VALUE(payload, '$.event_type')                       AS event_type,
    SAFE_CAST(JSON_VALUE(payload, '$.event_time') AS INT64)   AS event_minute,
    SAFE_CAST(JSON_VALUE(payload, '$.period') AS INT64)       AS period,

    SAFE_CAST(JSON_VALUE(payload, '$.player.id') AS INT64)    AS player_id,
    JSON_VALUE(payload, '$.player.display_name')              AS player_name,
    SAFE_CAST(JSON_VALUE(payload, '$.player.age') AS INT64)   AS player_age,
    JSON_VALUE(payload, '$.player.citizenship')               AS player_citizenship,

    SAFE_CAST(JSON_VALUE(payload, '$.secondary_player.id') AS INT64) AS assist_player_id,
    JSON_VALUE(payload, '$.secondary_player.display_name')            AS assist_player_name,

    JSON_VALUE(payload, '$.goal_type')                        AS goal_type,
    SAFE_CAST(JSON_VALUE(payload, '$.is_own_goal') AS BOOL)   AS is_own_goal,

    CASE WHEN JSON_VALUE(payload, '$.event_type') = 'goal' THEN TRUE ELSE FALSE END AS is_goal,
    CASE WHEN JSON_VALUE(payload, '$.event_type') IN ('yellow_card','red_card') THEN TRUE ELSE FALSE END AS is_card,
    CASE WHEN JSON_VALUE(payload, '$.event_type') = 'yellow_card' THEN TRUE ELSE FALSE END AS is_yellow_card,
    CASE WHEN JSON_VALUE(payload, '$.event_type') = 'red_card' THEN TRUE ELSE FALSE END AS is_red_card,
    CASE WHEN JSON_VALUE(payload, '$.event_type') = 'substitution' THEN TRUE ELSE FALSE END AS is_substitution
  FROM `graphite-flare-477419-h7.laliga_data.match_events`;

  -- 3) La Liga standings_flat
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.laliga_data.standings_flat`
  PARTITION BY DATE(ingested_at)
  CLUSTER BY season, team_id
  AS
  SELECT
    ingested_at,
    season AS season_partition,
    entity_id,

    SAFE_CAST(JSON_VALUE(payload, '$.team.id') AS INT64)      AS team_id,
    JSON_VALUE(payload, '$.team.name')                        AS team_name,
    JSON_VALUE(payload, '$.team.short_name')                  AS team_short_name,
    JSON_VALUE(payload, '$.team.abbreviation')                AS team_abbreviation,
    JSON_VALUE(payload, '$.team.location')                    AS team_location,

    SAFE_CAST(JSON_VALUE(payload, '$.season') AS INT64)       AS season,
    JSON_VALUE(payload, '$.group_name')                       AS league_name,
    JSON_VALUE(payload, '$.note')                             AS standing_note,

    SAFE_CAST(JSON_VALUE(payload, '$.rank') AS INT64)         AS rank,
    SAFE_CAST(JSON_VALUE(payload, '$.rank_change') AS INT64)  AS rank_change,

    SAFE_CAST(JSON_VALUE(payload, '$.games_played') AS INT64) AS games_played,
    SAFE_CAST(JSON_VALUE(payload, '$.wins') AS INT64)         AS wins,
    SAFE_CAST(JSON_VALUE(payload, '$.losses') AS INT64)       AS losses,
    SAFE_CAST(JSON_VALUE(payload, '$.draws') AS INT64)        AS draws,
    SAFE_CAST(JSON_VALUE(payload, '$.points') AS INT64)       AS points,
    SAFE_CAST(JSON_VALUE(payload, '$.goals_for') AS INT64)    AS goals_for,
    SAFE_CAST(JSON_VALUE(payload, '$.goals_against') AS INT64) AS goals_against,
    SAFE_CAST(JSON_VALUE(payload, '$.goal_differential') AS INT64) AS goal_differential,

    ROUND(SAFE_CAST(JSON_VALUE(payload, '$.points_per_game') AS FLOAT64), 2) AS points_per_game
  FROM `graphite-flare-477419-h7.laliga_data.standings`;

  -- 4) La Liga team_matches
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.laliga_data.team_matches`
  PARTITION BY DATE(scheduled_time)
  CLUSTER BY team_id
  AS
  SELECT
    match_id, scheduled_time, season, status,
    home_team_id AS team_id,
    away_team_id AS opponent_id,
    home_score AS goals_scored,
    away_score AS goals_allowed,
    TRUE AS is_home
  FROM `graphite-flare-477419-h7.laliga_data.matches_flat`
  WHERE status = 'STATUS_FULL_TIME'

  UNION ALL

  SELECT
    match_id, scheduled_time, season, status,
    away_team_id AS team_id,
    home_team_id AS opponent_id,
    away_score AS goals_scored,
    home_score AS goals_allowed,
    FALSE AS is_home
  FROM `graphite-flare-477419-h7.laliga_data.matches_flat`
  WHERE status = 'STATUS_FULL_TIME';

  -- 5) La Liga team_betting_metrics
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.laliga_data.team_betting_metrics`
  CLUSTER BY team_id
  AS
  WITH ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY scheduled_time DESC) AS game_rank
    FROM `graphite-flare-477419-h7.laliga_data.team_matches`
  )
  SELECT
    team_id,
    COUNT(*) AS season_games,

    ROUND(AVG(goals_scored), 1) AS season_avg_goals_scored,
    ROUND(AVG(goals_allowed), 1) AS season_avg_goals_allowed,
    ROUND(AVG(CASE WHEN goals_scored > 0 THEN 1 ELSE 0 END), 1) AS season_score_rate,
    ROUND(AVG(CASE WHEN goals_allowed > 0 THEN 1 ELSE 0 END), 1) AS season_allow_rate,

    ROUND(AVG(IF(game_rank <= 10, goals_scored, NULL)), 1) AS last10_avg_scored,
    ROUND(AVG(IF(game_rank <= 10, goals_allowed, NULL)), 1) AS last10_avg_allowed,

    ROUND(AVG(IF(game_rank <= 5, goals_scored, NULL)), 1) AS last5_avg_scored,
    ROUND(AVG(IF(game_rank <= 5, goals_allowed, NULL)), 1) AS last5_avg_allowed,

    ROUND(AVG(IF(game_rank <= 3, goals_scored, NULL)), 1) AS last3_avg_scored,
    ROUND(AVG(IF(game_rank <= 3, goals_allowed, NULL)), 1) AS last3_avg_allowed
  FROM ranked
  GROUP BY team_id;

  -- 6) La Liga match_card_totals
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.laliga_data.match_card_totals`
  CLUSTER BY match_id, team_id
  AS
  WITH cards AS (
    SELECT
      match_id,
      team_id,
      COUNTIF(is_card) AS team_cards
    FROM `graphite-flare-477419-h7.laliga_data.match_events_flat`
    WHERE is_card = TRUE
    GROUP BY match_id, team_id
  ),
  expanded AS (
    SELECT
      tm.match_id,
      tm.team_id,
      tm.opponent_id,
      tm.scheduled_time,
      COALESCE(c.team_cards, 0) AS team_cards
    FROM `graphite-flare-477419-h7.laliga_data.team_matches` tm
    LEFT JOIN cards c
      ON tm.match_id = c.match_id
     AND tm.team_id = c.team_id
  )
  SELECT
    e.match_id,
    e.team_id,
    e.opponent_id,
    e.scheduled_time,
    e.team_cards,
    COALESCE(o.team_cards, 0) AS opponent_cards,
    e.team_cards + COALESCE(o.team_cards, 0) AS total_cards
  FROM expanded e
  LEFT JOIN expanded o
    ON e.match_id = o.match_id
   AND e.opponent_id = o.team_id;

  -- 7) La Liga team_card_metrics
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.laliga_data.team_card_metrics`
  CLUSTER BY team_id
  AS
  WITH ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY scheduled_time DESC) AS game_rank
    FROM `graphite-flare-477419-h7.laliga_data.match_card_totals`
  )
  SELECT
    team_id,
    COUNT(*) AS season_games,

    ROUND(AVG(team_cards), 1)     AS season_team_cards_pg,
    ROUND(AVG(opponent_cards), 1) AS season_opponent_cards_pg,
    ROUND(AVG(total_cards), 1)    AS season_total_cards_pg,

    ROUND(AVG(IF(game_rank <= 10, team_cards, NULL)), 1)     AS l10_team_cards_pg,
    ROUND(AVG(IF(game_rank <= 10, opponent_cards, NULL)), 1) AS l10_opponent_cards_pg,
    ROUND(AVG(IF(game_rank <= 10, total_cards, NULL)), 1)    AS l10_total_cards_pg,

    ROUND(AVG(IF(game_rank <= 5, team_cards, NULL)), 1)      AS l5_team_cards_pg,
    ROUND(AVG(IF(game_rank <= 5, opponent_cards, NULL)), 1)  AS l5_opponent_cards_pg,
    ROUND(AVG(IF(game_rank <= 5, total_cards, NULL)), 1)     AS l5_total_cards_pg,

    ROUND(AVG(IF(game_rank <= 3, team_cards, NULL)), 1)      AS l3_team_cards_pg,
    ROUND(AVG(IF(game_rank <= 3, opponent_cards, NULL)), 1)  AS l3_opponent_cards_pg,
    ROUND(AVG(IF(game_rank <= 3, total_cards, NULL)), 1)     AS l3_total_cards_pg
  FROM ranked
  GROUP BY team_id;

  -- 8) La Liga team_master_metrics (goals + cards + latest standings)
  CREATE OR REPLACE TABLE `graphite-flare-477419-h7.laliga_data.team_master_metrics`
  CLUSTER BY team_id
  AS
  WITH latest_standings AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY ingested_at DESC) AS rn
      FROM `graphite-flare-477419-h7.laliga_data.standings_flat`
    )
    WHERE rn = 1
  )
  SELECT
    g.team_id,

    s.team_name,
    s.team_short_name,
    s.rank,
    s.points,
    s.goal_differential,
    s.points_per_game,
    s.standing_note,

    g.season_games,
    g.season_avg_goals_scored,
    g.season_avg_goals_allowed,
    g.season_score_rate,
    g.season_allow_rate,
    g.last10_avg_scored,
    g.last10_avg_allowed,
    g.last5_avg_scored,
    g.last5_avg_allowed,
    g.last3_avg_scored,
    g.last3_avg_allowed,

    c.season_team_cards_pg,
    c.season_opponent_cards_pg,
    c.season_total_cards_pg,
    c.l10_team_cards_pg,
    c.l10_opponent_cards_pg,
    c.l10_total_cards_pg,
    c.l5_team_cards_pg,
    c.l5_opponent_cards_pg,
    c.l5_total_cards_pg,
    c.l3_team_cards_pg,
    c.l3_opponent_cards_pg,
    c.l3_total_cards_pg
  FROM `graphite-flare-477419-h7.laliga_data.team_betting_metrics` g
  LEFT JOIN `graphite-flare-477419-h7.laliga_data.team_card_metrics` c
    ON g.team_id = c.team_id
  LEFT JOIN latest_standings s
    ON g.team_id = s.team_id;

END;