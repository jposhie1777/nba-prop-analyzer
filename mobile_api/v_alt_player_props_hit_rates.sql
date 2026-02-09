-- =============================================================================
-- View: v_alt_player_props_hit_rates
-- Description: Enhances the alt player props view with L5/L10/L20 hit rates
--              and averages per player/market/line by joining with
--              historical_player_trends.
--
-- Hit rate = % of games where stat >= line (over hit rate).
--            For under bets, under_hit_rate = 1 - hit_rate.
-- =============================================================================

CREATE OR REPLACE VIEW `graphite-flare-477419-h7.nba_goat_data.v_alt_player_props_hit_rates` AS

WITH latest_request AS (
    SELECT
        MAX(request_date) AS request_date
    FROM `graphite-flare-477419-h7.odds_raw.nba_alt_player_props`
),

latest_snapshot_per_event AS (
    SELECT
        event_id,
        MAX(snapshot_ts) AS snapshot_ts
    FROM `graphite-flare-477419-h7.odds_raw.nba_alt_player_props`
    WHERE request_date = (SELECT request_date FROM latest_request)
    GROUP BY event_id
),

base AS (
    SELECT
        props.snapshot_ts,
        props.request_date,
        props.event_id,
        props.sport_key,
        props.sport_title,
        props.commence_time,
        props.home_team,
        props.away_team,
        props.payload
    FROM `graphite-flare-477419-h7.odds_raw.nba_alt_player_props` AS props
    JOIN latest_snapshot_per_event AS latest
        ON props.event_id = latest.event_id
        AND props.snapshot_ts = latest.snapshot_ts
    WHERE props.request_date = (SELECT request_date FROM latest_request)
),

bookmakers AS (
    SELECT
        b.*,
        bm
    FROM base b,
    UNNEST(JSON_QUERY_ARRAY(b.payload, '$.bookmakers')) AS bm
),

markets AS (
    SELECT
        snapshot_ts,
        request_date,
        event_id,
        sport_key,
        sport_title,
        commence_time,
        home_team,
        away_team,
        JSON_VALUE(bm, '$.key') AS bookmaker_key,
        JSON_VALUE(bm, '$.title') AS bookmaker_title,
        m AS market
    FROM bookmakers,
    UNNEST(JSON_QUERY_ARRAY(bm, '$.markets')) AS m
),

outcomes AS (
    SELECT
        snapshot_ts,
        request_date,
        event_id,
        sport_key,
        sport_title,
        commence_time,
        home_team,
        away_team,
        bookmaker_key,
        bookmaker_title,
        JSON_VALUE(market, '$.key') AS market_key,
        TIMESTAMP(JSON_VALUE(market, '$.last_update')) AS market_last_update,
        o AS outcome
    FROM markets,
    UNNEST(JSON_QUERY_ARRAY(market, '$.outcomes')) AS o
),

-- Flatten outcomes into typed columns with market_key -> stat_key mapping
props AS (
    SELECT
        snapshot_ts,
        request_date,
        event_id,
        sport_key,
        sport_title,
        commence_time,
        home_team,
        away_team,
        bookmaker_key,
        bookmaker_title,
        market_key,
        market_last_update,
        JSON_VALUE(outcome, '$.name') AS outcome_name,
        JSON_VALUE(outcome, '$.description') AS player_name,
        SAFE_CAST(JSON_VALUE(outcome, '$.price') AS INT64) AS price,
        SAFE_CAST(JSON_VALUE(outcome, '$.point') AS FLOAT64) AS line,
        -- Map market_key to the stat abbreviation used in historical_player_trends
        CASE
            WHEN market_key IN ('player_points', 'player_points_alternate') THEN 'pts'
            WHEN market_key IN ('player_rebounds', 'player_rebounds_alternate') THEN 'reb'
            WHEN market_key IN ('player_assists', 'player_assists_alternate') THEN 'ast'
            WHEN market_key IN ('player_threes', 'player_threes_alternate') THEN 'fg3m'
            WHEN market_key IN ('player_steals', 'player_steals_alternate') THEN 'stl'
            WHEN market_key IN ('player_blocks', 'player_blocks_alternate') THEN 'blk'
            WHEN market_key IN ('player_points_rebounds_assists', 'player_points_rebounds_assists_alternate') THEN 'pra'
            WHEN market_key IN ('player_points_rebounds', 'player_points_rebounds_alternate') THEN 'pr'
            WHEN market_key IN ('player_points_assists', 'player_points_assists_alternate') THEN 'pa'
            WHEN market_key IN ('player_rebounds_assists', 'player_rebounds_assists_alternate') THEN 'ra'
            WHEN market_key IN ('player_double_double') THEN 'dd'
            WHEN market_key IN ('player_triple_double') THEN 'td'
        END AS stat_key
    FROM outcomes
),

-- Unpivot historical_player_trends into one row per player per stat
-- with L5/L10/L20 arrays as columns
player_trends AS (
    SELECT player, 'pts' AS stat_key,
        pts_last5_list AS l5_list, pts_last10_list AS l10_list, pts_last20_list AS l20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'reb',
        reb_last5_list, reb_last10_list, reb_last20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'ast',
        ast_last5_list, ast_last10_list, ast_last20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'fg3m',
        fg3m_last5_list, fg3m_last10_list, fg3m_last20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'stl',
        stl_last5_list, stl_last10_list, stl_last20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'blk',
        blk_last5_list, blk_last10_list, blk_last20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'pra',
        pra_last5_list, pra_last10_list, pra_last20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'pr',
        pr_last5_list, pr_last10_list, pr_last20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'pa',
        pa_last5_list, pa_last10_list, pa_last20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'ra',
        ra_last5_list, ra_last10_list, ra_last20_list
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    -- dd/td are INTEGER arrays; cast to FLOAT64 for consistent types
    SELECT player, 'dd',
        ARRAY(SELECT CAST(v AS FLOAT64) FROM UNNEST(dd_last5_list) AS v),
        ARRAY(SELECT CAST(v AS FLOAT64) FROM UNNEST(dd_last10_list) AS v),
        ARRAY(SELECT CAST(v AS FLOAT64) FROM UNNEST(dd_last20_list) AS v)
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
    UNION ALL
    SELECT player, 'td',
        ARRAY(SELECT CAST(v AS FLOAT64) FROM UNNEST(td_last5_list) AS v),
        ARRAY(SELECT CAST(v AS FLOAT64) FROM UNNEST(td_last10_list) AS v),
        ARRAY(SELECT CAST(v AS FLOAT64) FROM UNNEST(td_last20_list) AS v)
    FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
)

SELECT
    p.snapshot_ts,
    p.request_date,
    p.event_id,
    p.sport_key,
    p.sport_title,
    p.commence_time,
    p.home_team,
    p.away_team,
    p.bookmaker_key,
    p.bookmaker_title,
    p.market_key,
    p.stat_key,
    p.market_last_update,
    p.outcome_name,
    p.player_name,
    p.price,
    p.line,

    -- L5 metrics
    ROUND((SELECT AVG(val) FROM UNNEST(t.l5_list) AS val), 2) AS avg_l5,
    ROUND(SAFE_DIVIDE(
        (SELECT COUNTIF(val >= p.line) FROM UNNEST(t.l5_list) AS val),
        ARRAY_LENGTH(t.l5_list)
    ), 3) AS hit_rate_l5,

    -- L10 metrics
    ROUND((SELECT AVG(val) FROM UNNEST(t.l10_list) AS val), 2) AS avg_l10,
    ROUND(SAFE_DIVIDE(
        (SELECT COUNTIF(val >= p.line) FROM UNNEST(t.l10_list) AS val),
        ARRAY_LENGTH(t.l10_list)
    ), 3) AS hit_rate_l10,

    -- L20 metrics
    ROUND((SELECT AVG(val) FROM UNNEST(t.l20_list) AS val), 2) AS avg_l20,
    ROUND(SAFE_DIVIDE(
        (SELECT COUNTIF(val >= p.line) FROM UNNEST(t.l20_list) AS val),
        ARRAY_LENGTH(t.l20_list)
    ), 3) AS hit_rate_l20

FROM props p
LEFT JOIN player_trends t
    ON LOWER(TRIM(p.player_name)) = LOWER(TRIM(t.player))
    AND p.stat_key = t.stat_key
WHERE p.stat_key IS NOT NULL
