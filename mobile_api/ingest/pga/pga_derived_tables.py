"""
Rebuild PGA derived/analytics tables from source data in BigQuery.

These tables are materialised query results that power the Vercel API endpoints.
They are rebuilt daily after the primary ingest completes.

Source tables:
  - website_player_scorecard  (round-level tournament results per player)
  - website_player_profile_stats  (per-stat rankings from player profile pages)
  - player_stats  (per-stat rankings from statOverview + statDetails)

Derived tables rebuilt:
  - website_player_baseline      (player avg round score)
  - website_round_scores         (unpivoted per-round scores)
  - website_course_history       (player course history)
  - website_course_fit           (player course fit with delta from baseline)
  - website_player_skill_stats   (SG + driving + putting + scoring stats)
  - website_player_recent_form   (season + L3/L5 form metrics)
  - website_player_betting_profile (combined form + skill stats)

Usage:
    python -m mobile_api.ingest.pga.pga_derived_tables [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Dict

from google.cloud import bigquery

DATASET = os.getenv("PGA_DATASET", "pga_data")


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project)


def _run_sql(client: bigquery.Client, label: str, sql: str, *, dry_run: bool = False) -> int:
    """Execute a CREATE OR REPLACE TABLE statement; return rows affected."""
    if dry_run:
        print(f"[derived] DRY RUN — {label}")
        return 0
    print(f"[derived] Rebuilding {label}…")
    try:
        job = client.query(sql)
        job.result()
    except Exception as exc:
        if "different partitioning spec" in str(exc) or "different clustering" in str(exc):
            # Table has incompatible spec — drop and recreate
            print(f"[derived]   Dropping {label} due to spec mismatch…")
            # Extract table name from the CREATE OR REPLACE TABLE `...` statement
            import re
            match = re.search(r'CREATE OR REPLACE TABLE `([^`]+)`', sql)
            if match:
                client.query(f"DROP TABLE IF EXISTS `{match.group(1)}`").result()
                job = client.query(sql)
                job.result()
            else:
                raise
        else:
            raise
    # Get row count from destination table
    dest = job.destination
    if dest:
        table = client.get_table(dest)
        rows = table.num_rows or 0
    else:
        rows = 0
    print(f"[derived]   ✓ {label}: {rows} rows")
    return rows


def rebuild_all(*, dry_run: bool = False) -> Dict[str, int]:
    client = _bq_client()
    project = client.project
    ds = f"{project}.{DATASET}"
    results: Dict[str, int] = {}

    # 1. Player baseline (avg round score across all seasons)
    results["website_player_baseline"] = _run_sql(
        client,
        "website_player_baseline",
        f"""
        CREATE OR REPLACE TABLE `{ds}.website_player_baseline` AS
        WITH deduped AS (
          SELECT * EXCEPT(_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY tournament_id, player_id ORDER BY run_ts DESC
            ) AS _rn
            FROM `{ds}.website_player_scorecard`
          ) WHERE _rn = 1
        ),
        rounds AS (
          SELECT player_id, player_display_name, round_score
          FROM deduped
          UNPIVOT (round_score FOR round_label IN (r1 AS 'R1', r2 AS 'R2', r3 AS 'R3', r4 AS 'R4'))
        )
        SELECT
          player_id,
          ANY_VALUE(player_display_name) AS player_display_name,
          COUNT(*) AS rounds_played,
          ROUND(AVG(round_score), 2) AS avg_round_score
        FROM rounds
        GROUP BY player_id
        """,
        dry_run=dry_run,
    )

    # 2. Round scores (unpivoted from scorecard — long format)
    results["website_round_scores"] = _run_sql(
        client,
        "website_round_scores",
        f"""
        CREATE OR REPLACE TABLE `{ds}.website_round_scores` AS
        WITH deduped AS (
          SELECT * EXCEPT(_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY tournament_id, player_id ORDER BY run_ts DESC
            ) AS _rn
            FROM `{ds}.website_player_scorecard`
          ) WHERE _rn = 1
        )
        SELECT
          player_id,
          player_display_name,
          tournament_id,
          tournament_name,
          course_name,
          season,
          round_num,
          strokes
        FROM deduped
        UNPIVOT (strokes FOR round_num IN (r1 AS 1, r2 AS 2, r3 AS 3, r4 AS 4))
        """,
        dry_run=dry_run,
    )

    # 3. Course history (per player per course)
    results["website_course_history"] = _run_sql(
        client,
        "website_course_history",
        f"""
        CREATE OR REPLACE TABLE `{ds}.website_course_history` AS
        WITH deduped AS (
          SELECT * EXCEPT(_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY tournament_id, player_id ORDER BY run_ts DESC
            ) AS _rn
            FROM `{ds}.website_player_scorecard`
          ) WHERE _rn = 1
        ),
        rounds AS (
          SELECT player_id, player_display_name, course_name, round_score
          FROM deduped
          UNPIVOT (round_score FOR round_label IN (r1 AS 'R1', r2 AS 'R2', r3 AS 'R3', r4 AS 'R4'))
          WHERE course_name IS NOT NULL
        )
        SELECT
          player_id,
          ANY_VALUE(player_display_name) AS player_display_name,
          course_name,
          REGEXP_REPLACE(LOWER(course_name), r'[^a-z]', '') AS course_key,
          COUNT(*) AS rounds_played,
          ROUND(AVG(round_score), 2) AS avg_course_score
        FROM rounds
        GROUP BY player_id, course_name
        """,
        dry_run=dry_run,
    )

    # 4. Course fit (course avg vs player baseline = delta)
    results["website_course_fit"] = _run_sql(
        client,
        "website_course_fit",
        f"""
        CREATE OR REPLACE TABLE `{ds}.website_course_fit` AS
        SELECT
          ch.player_id,
          ch.player_display_name,
          ch.course_name,
          ch.course_key,
          ch.rounds_played,
          ch.avg_course_score,
          b.avg_round_score,
          ROUND(ch.avg_course_score - b.avg_round_score, 2) AS course_delta
        FROM `{ds}.website_course_history` ch
        JOIN `{ds}.website_player_baseline` b USING (player_id)
        """,
        dry_run=dry_run,
    )

    # 5. Player skill stats (from player_stats + profile_stats)
    results["website_player_skill_stats"] = _run_sql(
        client,
        "website_player_skill_stats",
        f"""
        CREATE OR REPLACE TABLE `{ds}.website_player_skill_stats` AS
        WITH current_year AS (
          SELECT EXTRACT(YEAR FROM CURRENT_DATE()) AS yr
        ),
        stats_deduped AS (
          SELECT * EXCEPT(_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY player_id, stat_id ORDER BY run_ts DESC
            ) AS _rn
            FROM `{ds}.player_stats`
            WHERE year = (SELECT yr FROM current_year)
          ) WHERE _rn = 1
        ),
        profile_deduped AS (
          SELECT * EXCEPT(_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY player_id, stat_id ORDER BY run_ts DESC
            ) AS _rn
            FROM `{ds}.website_player_profile_stats`
            WHERE season = (SELECT yr FROM current_year)
          ) WHERE _rn = 1
        ),
        combined AS (
          SELECT player_id, ANY_VALUE(player_name) AS player_name, stat_id,
                 ANY_VALUE(stat_value) AS stat_value
          FROM stats_deduped
          GROUP BY player_id, stat_id
          UNION ALL
          SELECT player_id, ANY_VALUE(player_name) AS player_name, stat_id,
                 ANY_VALUE(stat_value) AS stat_value
          FROM profile_deduped
          WHERE stat_id NOT IN (SELECT DISTINCT stat_id FROM stats_deduped)
          GROUP BY player_id, stat_id
        ),
        pivoted AS (
          SELECT
            player_id,
            ANY_VALUE(player_name) AS player_name,
            MAX(IF(stat_id = '02675', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS sg_total,
            MAX(IF(stat_id = '02567', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS sg_off_tee,
            MAX(IF(stat_id = '02568', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS sg_approach,
            MAX(IF(stat_id = '02564', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS sg_putting,
            MAX(IF(stat_id = '02569', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS sg_around_green,
            MAX(IF(stat_id = '02674', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS sg_tee_to_green,
            MAX(IF(stat_id = '101', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS driving_distance,
            MAX(IF(stat_id = '102', SAFE_CAST(REGEXP_REPLACE(stat_value, r'[^0-9.]', '') AS FLOAT64), NULL)) AS driving_accuracy,
            MAX(IF(stat_id = '103', SAFE_CAST(REGEXP_REPLACE(stat_value, r'[^0-9.]', '') AS FLOAT64), NULL)) AS gir_pct,
            MAX(IF(stat_id = '130', SAFE_CAST(REGEXP_REPLACE(stat_value, r'[^0-9.]', '') AS FLOAT64), NULL)) AS scrambling_pct,
            MAX(IF(stat_id = '104', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS putting_avg,
            MAX(IF(stat_id = '119', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS putts_per_round,
            MAX(IF(stat_id IN ('120', '108'), SAFE_CAST(stat_value AS FLOAT64), NULL)) AS scoring_avg,
            MAX(IF(stat_id = '156', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS birdie_avg,
            MAX(IF(stat_id = '142', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS par3_scoring_avg,
            MAX(IF(stat_id = '143', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS par4_scoring_avg,
            MAX(IF(stat_id = '144', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS par5_scoring_avg,
            MAX(IF(stat_id = '160', SAFE_CAST(REGEXP_REPLACE(stat_value, r'[^0-9.]', '') AS FLOAT64), NULL)) AS bounce_back_pct,
            MAX(IF(stat_id = '117', SAFE_CAST(stat_value AS FLOAT64), NULL)) AS putts_per_gir,
            MAX(IF(stat_id = '331', SAFE_CAST(REGEXP_REPLACE(stat_value, r'[^0-9.]', '') AS FLOAT64), NULL)) AS proximity_to_hole
          FROM combined
          GROUP BY player_id
        )
        SELECT * FROM pivoted
        """,
        dry_run=dry_run,
    )

    # 6. Player recent form (season + L3 + L5 metrics)
    results["website_player_recent_form"] = _run_sql(
        client,
        "website_player_recent_form",
        f"""
        CREATE OR REPLACE TABLE `{ds}.website_player_recent_form` AS
        WITH current_year AS (
          SELECT EXTRACT(YEAR FROM CURRENT_DATE()) AS yr
        ),
        deduped AS (
          SELECT * EXCEPT(_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY tournament_id, player_id ORDER BY run_ts DESC
            ) AS _rn
            FROM `{ds}.website_player_scorecard`
          ) WHERE _rn = 1
        ),
        season_data AS (
          SELECT * FROM deduped WHERE season = (SELECT yr FROM current_year)
        ),
        ranked AS (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY player_id ORDER BY tournament_date DESC
          ) AS event_rank
          FROM season_data
        ),
        finish_value AS (
          SELECT *,
            CASE
              WHEN UPPER(position) IN ('CUT', 'MC', 'WD', 'DQ', 'MDF') THEN 80.0
              ELSE SAFE_CAST(REGEXP_REPLACE(position, r'[^0-9]', '') AS FLOAT64)
            END AS finish_num,
            CASE
              WHEN UPPER(position) IN ('CUT', 'MC', 'WD', 'DQ', 'MDF') THEN 0 ELSE 1
            END AS made_cut
          FROM ranked
        ),
        season_agg AS (
          SELECT
            (SELECT yr FROM current_year) AS season,
            player_id,
            ANY_VALUE(player_display_name) AS player_display_name,
            ROUND(AVG(r1), 2) AS season_r1_avg,
            ROUND(AVG(r2), 2) AS season_r2_avg,
            ROUND(AVG(r3), 2) AS season_r3_avg,
            ROUND(AVG(r4), 2) AS season_r4_avg,
            ROUND(AVG(total_strokes), 2) AS season_total_score_avg,
            ROUND(AVG(finish_num), 2) AS season_finish_avg,
            ROUND(AVG(SAFE_CAST(REGEXP_REPLACE(to_par, r'[^0-9.E+-]', '') AS FLOAT64)), 2) AS season_to_par_avg,
            ROUND(STDDEV(total_strokes), 2) AS score_stddev,
            SUM(made_cut) AS season_cuts_made,
            COUNT(*) AS tournaments_played,
            MAX(SAFE.PARSE_DATE('%m.%d.%Y', tournament_date)) AS last_event_date,
            COUNTIF(r1 IS NOT NULL) AS season_r1_rounds_count,
            COUNTIF(r2 IS NOT NULL) AS season_r2_rounds_count,
            COUNTIF(r3 IS NOT NULL) AS season_r3_rounds_count,
            COUNTIF(r4 IS NOT NULL) AS season_r4_rounds_count,
            COUNTIF(total_strokes IS NOT NULL) AS season_total_score_count,
            COUNTIF(finish_num IS NOT NULL) AS season_finish_count,
            COUNTIF(to_par IS NOT NULL AND to_par != '-') AS season_to_par_count
          FROM finish_value
          GROUP BY player_id
        ),
        l3_agg AS (
          SELECT
            player_id,
            ROUND(AVG(r1), 2) AS l3_r1_avg,
            ROUND(AVG(r2), 2) AS l3_r2_avg,
            ROUND(AVG(r3), 2) AS l3_r3_avg,
            ROUND(AVG(r4), 2) AS l3_r4_avg,
            ROUND(AVG(total_strokes), 2) AS l3_total_score_avg,
            ROUND(AVG(finish_num), 2) AS l3_finish_avg,
            ROUND(AVG(SAFE_CAST(REGEXP_REPLACE(to_par, r'[^0-9.E+-]', '') AS FLOAT64)), 2) AS l3_to_par_avg,
            SUM(made_cut) AS l3_cuts_made,
            COUNT(*) AS l3_tournaments_considered,
            COUNTIF(total_strokes IS NOT NULL) AS l3_total_score_count,
            COUNTIF(finish_num IS NOT NULL) AS l3_finish_count
          FROM finish_value WHERE event_rank <= 3
          GROUP BY player_id
        ),
        l5_agg AS (
          SELECT
            player_id,
            ROUND(AVG(r1), 2) AS l5_r1_avg,
            ROUND(AVG(r2), 2) AS l5_r2_avg,
            ROUND(AVG(r3), 2) AS l5_r3_avg,
            ROUND(AVG(r4), 2) AS l5_r4_avg,
            ROUND(AVG(total_strokes), 2) AS l5_total_score_avg,
            ROUND(AVG(finish_num), 2) AS l5_finish_avg,
            ROUND(AVG(SAFE_CAST(REGEXP_REPLACE(to_par, r'[^0-9.E+-]', '') AS FLOAT64)), 2) AS l5_to_par_avg,
            SUM(made_cut) AS l5_cuts_made,
            COUNT(*) AS l5_tournaments_considered,
            COUNTIF(total_strokes IS NOT NULL) AS l5_total_score_count,
            COUNTIF(finish_num IS NOT NULL) AS l5_finish_count,
            SAFE_DIVIDE(SUM(made_cut), COUNT(*)) AS cut_rate_l5,
            AVG(IF(finish_num <= 10, 1.0, 0.0)) AS top10_rate_l5,
            -- Weighted score: most recent events weighted more
            SUM(total_strokes * (6 - event_rank)) / NULLIF(SUM(IF(total_strokes IS NOT NULL, 6 - event_rank, 0)), 0) AS weighted_l5_score
          FROM finish_value WHERE event_rank <= 5
          GROUP BY player_id
        ),
        trend AS (
          SELECT player_id,
            ROUND(
              AVG(IF(event_rank <= 3, total_strokes, NULL)) -
              AVG(IF(event_rank > 3 AND event_rank <= 6, total_strokes, NULL)),
            2) AS form_trend_3
          FROM finish_value WHERE event_rank <= 6
          GROUP BY player_id
        )
        SELECT
          s.*,
          l3.l3_r1_avg, l3.l3_r2_avg, l3.l3_r3_avg, l3.l3_r4_avg,
          l3.l3_total_score_avg, l3.l3_finish_avg, l3.l3_to_par_avg,
          l3.l3_cuts_made, l3.l3_tournaments_considered,
          l3.l3_total_score_count, l3.l3_finish_count,
          l5.l5_r1_avg, l5.l5_r2_avg, l5.l5_r3_avg, l5.l5_r4_avg,
          l5.l5_total_score_avg, l5.l5_finish_avg, l5.l5_to_par_avg,
          l5.l5_cuts_made, l5.l5_tournaments_considered,
          l5.l5_total_score_count, l5.l5_finish_count,
          ROUND(l5.cut_rate_l5, 3) AS cut_rate_l5,
          ROUND(l5.top10_rate_l5, 3) AS top10_rate_l5,
          ROUND(l5.weighted_l5_score, 2) AS weighted_l5_score,
          t.form_trend_3,
          DATE_DIFF(CURRENT_DATE(), s.last_event_date, DAY) AS days_since_last_event
        FROM season_agg s
        LEFT JOIN l3_agg l3 USING (player_id)
        LEFT JOIN l5_agg l5 USING (player_id)
        LEFT JOIN trend t USING (player_id)
        """,
        dry_run=dry_run,
    )

    # 7. Player betting profile (form + skill combined)
    results["website_player_betting_profile"] = _run_sql(
        client,
        "website_player_betting_profile",
        f"""
        CREATE OR REPLACE TABLE `{ds}.website_player_betting_profile` AS
        SELECT
          f.player_id,
          f.player_display_name,
          f.season,
          f.season_r1_avg, f.season_r2_avg, f.season_r3_avg, f.season_r4_avg,
          f.season_total_score_avg, f.season_finish_avg, f.season_to_par_avg,
          f.season_cuts_made, f.tournaments_played,
          f.l3_r1_avg, f.l3_r2_avg, f.l3_r3_avg, f.l3_r4_avg,
          f.l3_total_score_avg, f.l3_finish_avg, f.l3_to_par_avg, f.l3_cuts_made,
          f.l5_r1_avg, f.l5_r2_avg, f.l5_r3_avg, f.l5_r4_avg,
          f.l5_total_score_avg, f.l5_finish_avg, f.l5_to_par_avg, f.l5_cuts_made,
          f.cut_rate_l5, f.top10_rate_l5, f.weighted_l5_score,
          f.form_trend_3, f.score_stddev, f.days_since_last_event,
          s.sg_total, s.sg_off_tee, s.sg_approach, s.sg_putting,
          s.driving_distance, s.driving_accuracy, s.gir_pct, s.scrambling_pct,
          s.putting_avg, s.putts_per_round, s.scoring_avg, s.birdie_avg
        FROM `{ds}.website_player_recent_form` f
        LEFT JOIN `{ds}.website_player_skill_stats` s USING (player_id)
        """,
        dry_run=dry_run,
    )

    # 8. Flatten player stats for current year
    results["website_player_stats_flat"] = _run_sql(
        client,
        "website_player_stats_flat",
        f"""
        CREATE OR REPLACE TABLE `{ds}.website_player_stats_flat` AS
        WITH current_year AS (
          SELECT EXTRACT(YEAR FROM CURRENT_DATE()) AS yr
        ),
        deduped AS (
          SELECT * EXCEPT(_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY player_id, stat_id ORDER BY run_ts DESC
            ) AS _rn
            FROM `{ds}.player_stats`
            WHERE year = (SELECT yr FROM current_year)
          ) WHERE _rn = 1
        )
        SELECT
          stat_id, stat_name, player_id, player_name,
          stat_value, rank, tour_avg
        FROM deduped
        """,
        dry_run=dry_run,
    )

    print(f"\n[derived] All tables rebuilt: {results}")
    return results


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Rebuild PGA derived analytics tables.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    rebuild_all(dry_run=args.dry_run)


if __name__ == "__main__":
    _cli()
