"""
Create (or recreate) the two BigQuery views used by the pairings analytics endpoint.

Run once from the mobile_api directory:

    python -m pga.create_views

Views created in the dataset pointed to by PGA_DATASET (default: pga_data):

  v_player_stats        – per-player form / placement stats (last 3 seasons)
  v_pairings_analytics  – v_pairings_latest LEFT JOIN v_player_stats
"""

from __future__ import annotations

import os
import sys

from bq import get_bq_client

DATASET = os.getenv("PGA_DATASET", "pga_data")
PLAYERS_TABLE = os.getenv("PGA_PLAYERS_TABLE", "players_active")
PAIRINGS_VIEW = os.getenv("PGA_PAIRINGS_VIEW", "v_pairings_latest")


def _tbl(project: str, table: str) -> str:
    return f"`{project}.{DATASET}.{table}`"


def _sql_v_player_stats(project: str) -> str:
    results = _tbl(project, "tournament_results")
    tournaments = _tbl(project, "tournaments")
    return f"""
CREATE OR REPLACE VIEW `{project}.{DATASET}.v_player_stats` AS
WITH
-- ── 1. Rolling 3-season window ───────────────────────────────────────────────
seasons AS (
  SELECT s AS season
  FROM UNNEST(
    GENERATE_ARRAY(EXTRACT(YEAR FROM CURRENT_DATE()) - 2,
                   EXTRACT(YEAR FROM CURRENT_DATE()))
  ) s
),
-- ── 2. Deduplicate tournament_results ───────────────────────────────────────
latest_results AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY tournament_id, player_id, season
        ORDER BY run_ts DESC
      ) AS rn
    FROM {results}
  )
  WHERE rn = 1
),
-- ── 3. Deduplicate tournaments (for start_date) ──────────────────────────────
latest_tournaments AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY tournament_id, season
        ORDER BY run_ts DESC
      ) AS rn
    FROM {tournaments}
  )
  WHERE rn = 1
),
-- ── 4. Scope to the 3 seasons; annotate each result ─────────────────────────
results_in_scope AS (
  SELECT
    r.player_id,
    COALESCE(t.start_date, r.tournament_start_date) AS start_date,
    r.position,
    r.position_numeric,
    -- mirror Python finish_value() / is_cut()
    CASE
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(r.position, '')),
                           r'^(MC|CUT|WD|DQ|MDF|DMQ)')
        THEN 80.0
      ELSE COALESCE(CAST(r.position_numeric AS FLOAT64), 80.0)
    END AS finish_value,
    CASE
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(r.position, '')),
                           r'^(MC|CUT|WD|DQ|MDF|DMQ)')
        THEN 1 ELSE 0
    END AS is_cut
  FROM latest_results r
  INNER JOIN seasons      s ON r.season          = s.season
  LEFT  JOIN latest_tournaments t
          ON t.tournament_id = r.tournament_id
         AND t.season        = r.season
),
-- ── 5. Rank each player's results newest-first ──────────────────────────────
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY start_date DESC) AS rn
  FROM results_in_scope
),
-- ── 6. Form metrics: last 10 starts, min 2 ──────────────────────────────────
form AS (
  SELECT
    player_id,
    COUNT(*)                                                            AS form_starts,
    ROUND(AVG(finish_value),                                        2)  AS avg_finish,
    ROUND(AVG(IF(position_numeric <= 10, 1.0, 0.0)),               3)  AS top10_rate,
    ROUND(AVG(IF(position_numeric <= 20, 1.0, 0.0)),               3)  AS top20_rate,
    ROUND(AVG(CAST(is_cut AS FLOAT64)),                            3)  AS cut_rate
  FROM ranked
  WHERE rn <= 10
  GROUP BY player_id
  HAVING COUNT(*) >= 2
),
-- ── 7. Placement probabilities: last 20 starts, min 3 ───────────────────────
placement AS (
  SELECT
    player_id,
    COUNT(*)                                                            AS placement_starts,
    ROUND(AVG(IF(position_numeric <= 5,  1.0, 0.0)),               3)  AS top5_prob,
    ROUND(AVG(IF(position_numeric <= 10, 1.0, 0.0)),               3)  AS top10_prob,
    ROUND(AVG(IF(position_numeric <= 20, 1.0, 0.0)),               3)  AS top20_prob
  FROM ranked
  WHERE rn <= 20
  GROUP BY player_id
  HAVING COUNT(*) >= 3
)
-- ── 8. Final output ──────────────────────────────────────────────────────────
SELECT
  f.player_id,
  f.form_starts,
  f.avg_finish,
  f.top10_rate,
  f.top20_rate,
  f.cut_rate,
  -- mirror Python: form_score = (top10*0.5) + (top20*0.3) + ((1-cut)*0.2) - (avg/100)
  ROUND(
    (f.top10_rate * 0.5)
    + (f.top20_rate * 0.3)
    + ((1 - f.cut_rate) * 0.2)
    - (f.avg_finish / 100.0),
    4
  )                           AS form_score,
  p.placement_starts,
  p.top5_prob,
  p.top10_prob,
  p.top20_prob
FROM form f
LEFT JOIN placement p USING (player_id)
"""


def _sql_v_pairings_analytics(project: str) -> str:
    return f"""
CREATE OR REPLACE VIEW `{project}.{DATASET}.v_pairings_analytics` AS
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
  -- Cast the STRING player_id from the pairings view to INT64 so we can
  -- join with the INT64 player_id used across tournament_results / players.
  SAFE_CAST(p.player_id AS INT64)  AS player_id_int,
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
FROM `{project}.{DATASET}.{PAIRINGS_VIEW}` p
LEFT JOIN `{project}.{DATASET}.v_player_stats`  s
       ON SAFE_CAST(p.player_id AS INT64) = s.player_id
"""


def create_views() -> None:
    client = get_bq_client()
    project = client.project

    views = [
        ("v_player_stats",       _sql_v_player_stats(project)),
        ("v_pairings_analytics", _sql_v_pairings_analytics(project)),
    ]

    for name, sql in views:
        print(f"Creating {DATASET}.{name} …", end=" ", flush=True)
        client.query(sql).result()
        print("done")

    print("All views created successfully.")


if __name__ == "__main__":
    try:
        create_views()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
