-- ============================================================
-- 01_mls_team_name_map.sql
--
-- Builds a FanDuel → BQ team_id mapping table.
-- This is the single source of truth for joining FanDuel
-- team names to BQ fact tables.
--
-- Source: mls_data.fact_team_match (team_id, team_name, team_short_name)
-- Output: soccer_data.mls_team_name_map
-- ============================================================

CREATE OR REPLACE TABLE `graphite-flare-477419-h7.soccer_data.mls_team_name_map` AS

WITH bq_teams AS (
  -- Get latest distinct team identity from fact_team_match
  SELECT
    team_id,
    team_name,
    team_short_name,
    team_three_letter_code,
    LOWER(REGEXP_REPLACE(LOWER(team_name), r'[^a-z0-9]+', ''))       AS full_key,
    LOWER(REGEXP_REPLACE(LOWER(team_short_name), r'[^a-z0-9]+', '')) AS short_key
  FROM `graphite-flare-477419-h7.mls_data.fact_team_match`
  GROUP BY team_id, team_name, team_short_name, team_three_letter_code
),

-- FanDuel uses shorter/different names — build explicit mapping
-- fd_name = exactly how FanDuel spells it in raw_fanduel_soccer_markets
fd_name_map AS (
  SELECT fd_name, bq_key FROM UNNEST([
    STRUCT('Atlanta Utd'           AS fd_name, 'atlantaunited'              AS bq_key),
    STRUCT('Austin FC'             AS fd_name, 'austinfc'                   AS bq_key),
    STRUCT('CF Montreal'           AS fd_name, 'cfmontral'                  AS bq_key),
    STRUCT('Charlotte FC'          AS fd_name, 'charlottefc'                AS bq_key),
    STRUCT('Chicago Fire'          AS fd_name, 'chicagofirefc'              AS bq_key),
    STRUCT('Colorado'              AS fd_name, 'coloradorapids'             AS bq_key),
    STRUCT('Columbus'              AS fd_name, 'columbuscrew'               AS bq_key),
    STRUCT('DC Utd'                AS fd_name, 'dcunited'                   AS bq_key),
    STRUCT('FC Dallas'             AS fd_name, 'fcdallas'                   AS bq_key),
    STRUCT('Houston Dynamo'        AS fd_name, 'houstondynamofc'            AS bq_key),
    STRUCT('Inter Miami CF'        AS fd_name, 'intermiamicf'               AS bq_key),
    STRUCT('Kansas City'           AS fd_name, 'sportingkansascity'         AS bq_key),
    STRUCT('LA Galaxy'             AS fd_name, 'lagalaxy'                   AS bq_key),
    STRUCT('Los Angeles FC'        AS fd_name, 'losangelesfootballclub'     AS bq_key),
    STRUCT('Minnesota Utd'         AS fd_name, 'minnesotaunitedfc'          AS bq_key),
    STRUCT('New England'           AS fd_name, 'newenglandrevolution'       AS bq_key),
    STRUCT('New York City'         AS fd_name, 'newyorkcityfc'              AS bq_key),
    STRUCT('New York Red Bulls'    AS fd_name, 'newyorkredbulls'            AS bq_key),
    STRUCT('Orlando City'          AS fd_name, 'orlandocity'                AS bq_key),
    STRUCT('Philadelphia Union'    AS fd_name, 'philadelphiaunion'          AS bq_key),
    STRUCT('Portland Timbers'      AS fd_name, 'portlandtimbers'            AS bq_key),
    STRUCT('Real Salt Lake'        AS fd_name, 'realsaltlake'               AS bq_key),
    STRUCT('San Diego FC'          AS fd_name, 'sandiegofc'                 AS bq_key),
    STRUCT('San Jose Earthquakes'  AS fd_name, 'sanjoseearthquakes'         AS bq_key),
    STRUCT('Seattle Sounders'      AS fd_name, 'seattlesoundersfc'          AS bq_key),
    STRUCT('Toronto FC'            AS fd_name, 'torontofc'                  AS bq_key),
    STRUCT('Vancouver Whitecaps'   AS fd_name, 'vancouverwhitecapsfc'       AS bq_key),
    STRUCT('Nashville SC'          AS fd_name, 'nashvillesc'                AS bq_key),
    STRUCT('St. Louis City'        AS fd_name, 'stlouiscitysc'              AS bq_key)
  ])
)

SELECT
  m.fd_name,
  b.team_id,
  b.team_name                                         AS bq_team_name,
  b.team_short_name,
  b.team_three_letter_code,
  CURRENT_TIMESTAMP()                                 AS updated_at
FROM fd_name_map m
LEFT JOIN bq_teams b ON m.bq_key = b.full_key
