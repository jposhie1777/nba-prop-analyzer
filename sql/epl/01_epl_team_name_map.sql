-- ============================================================
-- 01_epl_team_name_map.sql
--
-- Builds a FanDuel → BQ team_id mapping table for EPL.
-- FanDuel uses shortened names; BQ uses full official names.
--
-- Source: epl_data.standings_flat
-- Output: soccer_data.epl_team_name_map
-- ============================================================

CREATE OR REPLACE TABLE `graphite-flare-477419-h7.soccer_data.epl_team_name_map` AS

WITH bq_teams AS (
  SELECT
    team_id,
    team_name,
    LOWER(REGEXP_REPLACE(LOWER(team_name), r'[^a-z0-9]+', '')) AS team_key
  FROM `graphite-flare-477419-h7.epl_data.standings_flat`
),

-- fd_name = exactly how FanDuel spells it in raw_fanduel_soccer_markets
fd_name_map AS (
  SELECT fd_name, bq_key FROM UNNEST([
    STRUCT('Arsenal'         AS fd_name, 'arsenal'                    AS bq_key),
    STRUCT('Aston Villa'     AS fd_name, 'astonvilla'                 AS bq_key),
    STRUCT('Bournemouth'     AS fd_name, 'afcbournemouth'             AS bq_key),
    STRUCT('Brentford'       AS fd_name, 'brentford'                  AS bq_key),
    STRUCT('Brighton'        AS fd_name, 'brightonhovealbion'         AS bq_key),
    STRUCT('Burnley'         AS fd_name, 'burnley'                    AS bq_key),
    STRUCT('Chelsea'         AS fd_name, 'chelsea'                    AS bq_key),
    STRUCT('Crystal Palace'  AS fd_name, 'crystalpalace'              AS bq_key),
    STRUCT('Everton'         AS fd_name, 'everton'                    AS bq_key),
    STRUCT('Fulham'          AS fd_name, 'fulham'                     AS bq_key),
    STRUCT('Leeds'           AS fd_name, 'leedsunited'                AS bq_key),
    STRUCT('Liverpool'       AS fd_name, 'liverpool'                  AS bq_key),
    STRUCT('Man City'        AS fd_name, 'manchestercity'             AS bq_key),
    STRUCT('Man Utd'         AS fd_name, 'manchesterunited'           AS bq_key),
    STRUCT('Newcastle'       AS fd_name, 'newcastleunited'            AS bq_key),
    STRUCT('Nottm Forest'    AS fd_name, 'nottinghamforest'           AS bq_key),
    STRUCT('Sunderland'      AS fd_name, 'sunderland'                 AS bq_key),
    STRUCT('Tottenham'       AS fd_name, 'tottenhamhotspur'           AS bq_key),
    STRUCT('West Ham'        AS fd_name, 'westhamunited'              AS bq_key),
    STRUCT('Wolves'          AS fd_name, 'wolverhamptonwanderers'     AS bq_key)
  ])
)

SELECT
  m.fd_name,
  b.team_id,
  b.team_name                                         AS bq_team_name,
  CURRENT_TIMESTAMP()                                 AS updated_at
FROM fd_name_map m
LEFT JOIN bq_teams b ON m.bq_key = b.team_key