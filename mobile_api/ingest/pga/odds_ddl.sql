-- BigQuery DDL for PGA website odds
-- Dataset: pga_data
-- Table: website_odds
--
-- Single unified table for all 5 odds markets:
--   2032  To Win         (GraphQL / gzip-compressed payload)
--   2033  Finish         (REST)
--   2036  Group Props    (REST)
--   2039  Matchup Props  (REST)
--   2085  3 Ball         (REST)
--
-- Truncated on every load so only current odds are present.
-- One row per player per market entry (group members each get their own row).
-- Rows belonging to the same matchup / 3-ball group share the same
-- (market_id, sub_market_name, group_index) triple.

CREATE TABLE IF NOT EXISTS `pga_data.website_odds` (
    -- ── Ingestion metadata ────────────────────────────────────────────────────
    ingested_at         TIMESTAMP   NOT NULL,   -- UTC timestamp of this load run
    tournament_id       STRING      NOT NULL,   -- PGA Tour tournament ID, e.g. "R2026009"

    -- ── Market metadata ───────────────────────────────────────────────────────
    market_id           INT64       NOT NULL,   -- Numeric market ID (2032, 2033, 2036, 2039, 2085)
    market_name         STRING,                 -- Raw market name, e.g. "Outright Winner"
    market_display_name STRING,                 -- Display label, e.g. "To Win"
    market_type         STRING,                 -- e.g. "FINISH", "MATCHUP_PROPS", "GROUP_PROPS"
    betting_provider    STRING,                 -- e.g. "fanduel"

    -- ── Sub-market / grouping ─────────────────────────────────────────────────
    sub_market_name     STRING,                 -- e.g. "Top 10 Finish", "Best Score - Round 3"
    group_type          STRING,                 -- "SINGLE" or "GROUP"
    group_index         INT64,                  -- Position within the sub-market oddsData list;
                                                -- players sharing a group_index are in the same matchup/3ball group

    -- ── Player ────────────────────────────────────────────────────────────────
    player_id           STRING      NOT NULL,   -- PGA Tour player ID string
    display_name        STRING,                 -- Full name (NULL for To Win market)
    short_name          STRING,                 -- Abbreviated name (NULL for To Win market)

    -- ── Odds ─────────────────────────────────────────────────────────────────
    odds_value          STRING,                 -- American odds string, e.g. "+480", "-135"
    odds_direction      STRING,                 -- "UP", "DOWN", or "CONSTANT" (line movement)
    odds_sort           FLOAT64,                -- Numeric sort key (populated for To Win market)
    option_id           STRING,                 -- Sportsbook option ID
    entity_id           STRING                  -- Entity ID (usually matches player_id)
)
CLUSTER BY tournament_id, market_id, player_id
OPTIONS (
    description = 'PGA Tour website odds — all 5 markets, truncated on each daily load. '
                  'One row per player per market entry. '
                  'Matchup/3-ball group members share the same (market_id, sub_market_name, group_index).'
);
