# EPL BigQuery Daily Flatten + Analysis Pipeline

This plan runs **once per day** in BigQuery:

1. **5:00 AM America/New_York**: flatten only records not flattened yet.
2. **5:30 AM America/New_York**: build an analysis table from flattened data.

> Assumed source tables:
>
> - `project.epl_raw.match_data`
> - `project.epl_raw.match_events`

If your schema uses different names, replace them in the SQL below.

---

## 1) Flatten query (5:00 AM ET)

Create flattened tables once:

```sql
CREATE TABLE IF NOT EXISTS `epl_silver.matches_flat`
(
  match_id STRING,
  competition STRING,
  season STRING,
  match_date DATE,
  home_team_id STRING,
  home_team_name STRING,
  away_team_id STRING,
  away_team_name STRING,
  home_score INT64,
  away_score INT64,
  status STRING,
  loaded_at TIMESTAMP,
  source_snapshot_ts TIMESTAMP
)
PARTITION BY match_date
CLUSTER BY competition, season, home_team_id, away_team_id;

CREATE TABLE IF NOT EXISTS `epl_silver.match_events_flat`
(
  match_id STRING,
  event_id STRING,
  event_minute INT64,
  team_id STRING,
  team_name STRING,
  player_id STRING,
  player_name STRING,
  event_type STRING,
  card_type STRING,
  loaded_at TIMESTAMP,
  source_snapshot_ts TIMESTAMP
)
PARTITION BY DATE(loaded_at)
CLUSTER BY match_id, event_type, team_id;
```

Use this **scheduled query** at 5:00 AM ET to process only records that have not yet been flattened:

```sql
MERGE `epl_silver.matches_flat` T
USING (
  SELECT
    CAST(md.match_id AS STRING) AS match_id,
    CAST(md.competition AS STRING) AS competition,
    CAST(md.season AS STRING) AS season,
    DATE(md.match_datetime) AS match_date,
    CAST(md.home_team.id AS STRING) AS home_team_id,
    md.home_team.name AS home_team_name,
    CAST(md.away_team.id AS STRING) AS away_team_id,
    md.away_team.name AS away_team_name,
    SAFE_CAST(md.score.home AS INT64) AS home_score,
    SAFE_CAST(md.score.away AS INT64) AS away_score,
    CAST(md.status AS STRING) AS status,
    CURRENT_TIMESTAMP() AS loaded_at,
    md.snapshot_ts AS source_snapshot_ts
  FROM `epl_raw.match_data` md
) S
ON T.match_id = S.match_id
WHEN NOT MATCHED THEN
  INSERT ROW;

MERGE `epl_silver.match_events_flat` T
USING (
  SELECT
    CAST(me.match_id AS STRING) AS match_id,
    CAST(me.event_id AS STRING) AS event_id,
    SAFE_CAST(me.minute AS INT64) AS event_minute,
    CAST(me.team.id AS STRING) AS team_id,
    me.team.name AS team_name,
    CAST(me.player.id AS STRING) AS player_id,
    me.player.name AS player_name,
    LOWER(CAST(me.type AS STRING)) AS event_type,
    LOWER(CAST(me.card AS STRING)) AS card_type,
    CURRENT_TIMESTAMP() AS loaded_at,
    me.snapshot_ts AS source_snapshot_ts
  FROM `epl_raw.match_events` me
) S
ON T.match_id = S.match_id AND T.event_id = S.event_id
WHEN NOT MATCHED THEN
  INSERT ROW;
```

---

## 2) Analysis query (5:30 AM ET)

This builds one daily table containing:

- team goals scored average
- team goals allowed average
- opponent goals scored/allowed averages
- both teams to score (BTTS) tendency
- average cards for team
- average cards received by opponent
- explanation fields for why BTTS looks likely/unlikely

```sql
CREATE OR REPLACE TABLE `project.epl_gold.team_daily_analysis` AS
WITH base_matches AS (
  SELECT
    match_id,
    match_date,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_score,
    away_score,
    home_score > 0 AS home_scored,
    away_score > 0 AS away_scored,
    (home_score > 0 AND away_score > 0) AS btts_hit
  FROM `project.epl_silver.matches_flat`
  WHERE status IN ('finished', 'complete', 'ft')
),
team_match_rows AS (
  SELECT
    match_id,
    match_date,
    home_team_id AS team_id,
    home_team_name AS team_name,
    away_team_id AS opp_team_id,
    away_team_name AS opp_team_name,
    home_score AS goals_scored,
    away_score AS goals_allowed,
    btts_hit
  FROM base_matches
  UNION ALL
  SELECT
    match_id,
    match_date,
    away_team_id AS team_id,
    away_team_name AS team_name,
    home_team_id AS opp_team_id,
    home_team_name AS opp_team_name,
    away_score AS goals_scored,
    home_score AS goals_allowed,
    btts_hit
  FROM base_matches
),
cards_by_team_match AS (
  SELECT
    match_id,
    team_id,
    COUNTIF(event_type = 'card') AS cards_total,
    COUNTIF(event_type = 'card' AND card_type = 'yellow') AS yellow_cards,
    COUNTIF(event_type = 'card' AND card_type = 'red') AS red_cards
  FROM `project.epl_silver.match_events_flat`
  GROUP BY 1,2
),
team_enriched AS (
  SELECT
    t.*,
    COALESCE(c.cards_total, 0) AS cards_received
  FROM team_match_rows t
  LEFT JOIN cards_by_team_match c
    ON c.match_id = t.match_id
   AND c.team_id = t.team_id
),
team_stats AS (
  SELECT
    team_id,
    ANY_VALUE(team_name) AS team_name,
    COUNT(*) AS matches_sample,
    AVG(goals_scored) AS avg_goals_scored,
    AVG(goals_allowed) AS avg_goals_allowed,
    AVG(CAST(btts_hit AS INT64)) AS btts_rate,
    AVG(cards_received) AS avg_cards_received
  FROM team_enriched
  GROUP BY 1
),
opp_cards_stats AS (
  SELECT
    team_id,
    AVG(opp_cards) AS avg_cards_for_opponent
  FROM (
    SELECT
      t.match_id,
      t.team_id,
      COALESCE(c2.cards_total, 0) AS opp_cards
    FROM team_match_rows t
    LEFT JOIN cards_by_team_match c2
      ON c2.match_id = t.match_id
     AND c2.team_id = t.opp_team_id
  )
  GROUP BY 1
)
SELECT
  CURRENT_DATE('America/New_York') AS as_of_date,
  ts.team_id,
  ts.team_name,
  ts.matches_sample,
  ROUND(ts.avg_goals_scored, 3) AS avg_goals_scored,
  ROUND(ts.avg_goals_allowed, 3) AS avg_goals_allowed,
  ROUND(ts.btts_rate, 3) AS btts_rate,
  ROUND(ts.avg_cards_received, 3) AS avg_cards_received,
  ROUND(oc.avg_cards_for_opponent, 3) AS avg_cards_for_opponent,

  -- simple directional BTTS score
  ROUND(
    0.45 * ts.avg_goals_scored
    + 0.35 * ts.avg_goals_allowed
    + 0.20 * ts.btts_rate,
    3
  ) AS btts_likelihood_score,

  CASE
    WHEN (0.45 * ts.avg_goals_scored + 0.35 * ts.avg_goals_allowed + 0.20 * ts.btts_rate) >= 1.20 THEN 'high'
    WHEN (0.45 * ts.avg_goals_scored + 0.35 * ts.avg_goals_allowed + 0.20 * ts.btts_rate) >= 0.90 THEN 'medium'
    ELSE 'low'
  END AS btts_likelihood_bucket,

  FORMAT(
    'Scoring %.2f, allowing %.2f, BTTS rate %.2f over %d matches.',
    ts.avg_goals_scored,
    ts.avg_goals_allowed,
    ts.btts_rate,
    ts.matches_sample
  ) AS btts_why
FROM team_stats ts
LEFT JOIN opp_cards_stats oc
  ON oc.team_id = ts.team_id;
```

---

## 3) Scheduling in BigQuery UI

In BigQuery:

1. Go to **Scheduled queries** → **Create scheduled query**.
2. Schedule #1 (Flatten):
   - Query text: flatten SQL above
   - Frequency: Daily
   - Time: 5:00 AM
   - Time zone: `America/New_York`
3. Schedule #2 (Analyze):
   - Query text: analysis SQL above
   - Frequency: Daily
   - Time: 5:30 AM (or 5:15 AM if flatten is fast)
   - Time zone: `America/New_York`

Use destination settings per query defaults (MERGE doesn't need destination, `CREATE OR REPLACE TABLE` writes directly).

---

## 4) Practical notes

- Keep raw tables append-only; never update/delete raw snapshots.
- Use `MERGE ... WHEN NOT MATCHED` to make flatten idempotent.
- If source rows can change after first load, switch to upsert logic (`WHEN MATCHED THEN UPDATE`).
- If analysis needs recent form only, add `WHERE match_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 180 DAY)` in `base_matches`.

