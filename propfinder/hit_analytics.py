# propfinder/hit_analytics.py

"""
hit_analytics.py — Batter hits analytics: back-testing, self-learning weight
calibration, results tracking, and factor correlation analysis.

The self-learning loop:
  1. After games complete, fetch actual hit totals from MLB Stats API
  2. Update hit_picks_daily with actual_hits and hit flag
  3. Compute per-factor correlation with correct predictions
  4. Adjust weights: factors that correlate with hits get boosted,
     factors that correlate with misses get dampened
  5. Write updated weights to hit_model_weights table
  6. Next run of hit_model.py loads these learned weights
"""

import json
import logging
import os
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from google.cloud import bigquery

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "graphite-flare-477419-h7")
DATASET = "propfinder"
ET = ZoneInfo("America/New_York")
TODAY = datetime.now(ET).date()
NOW = datetime.now(ET)
MLB_API = "https://statsapi.mlb.com/api/v1"

client = bigquery.Client(project=PROJECT)
TABLE = f"`{PROJECT}.{DATASET}"

# Baseline weights — same as hit_model.py
BASELINE_WEIGHTS = {
    "batting_avg":       12.0,
    "contact_rate":      10.0,
    "l15_hit_rate":       8.0,
    "hard_hit_pct":       5.0,
    "babip_proxy":        5.0,
    "p_whip":            10.0,
    "p_k_rate_inv":       8.0,
    "p_woba_allowed":     7.0,
    "bvp_history":        7.0,
    "platoon_edge":       5.0,
    "arsenal_contact":    3.0,
    "pf_rating":          5.0,
    "hit_rate_l10":       4.0,
    "hit_rate_season":    2.0,
    "hit_rate_vs_team":   2.0,
    "vegas_total":        3.0,
    "avg_l10_vs_line":    3.0,
    "streak":             2.0,
}

LEARNING_RATE = 0.15
MIN_SAMPLE_SIZE = 20
WEIGHT_FLOOR = 1.0
WEIGHT_CEILING = 20.0


# ── 1. Results Backfill ──────────────────────────────────────────────────

def fetch_actual_hits(game_date):
    """
    Fetch actual batter hit totals for all games on a given date.
    Returns dict: (game_pk, batter_id) -> actual_hits.
    """
    date_str = game_date.isoformat()
    url = f"{MLB_API}/schedule?sportId=1&date={date_str}&hydrate=boxscore"
    req = Request(url, headers={"User-Agent": "PulseSports/1.0"})
    try:
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        log.warning("Failed to fetch MLB schedule for %s: %s", date_str, exc)
        return {}

    results = {}
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            if game.get("status", {}).get("abstractGameCode") != "F":
                continue
            game_pk = game.get("gamePk")
            if not game_pk:
                continue
            boxscore = game.get("boxscore") or {}
            teams = boxscore.get("teams", {})
            for side in ("away", "home"):
                players = teams.get(side, {}).get("players", {})
                for pid_str, player_data in players.items():
                    pid = int(pid_str.replace("ID", ""))
                    stats = player_data.get("stats", {}).get("batting", {})
                    if stats:
                        hits = int(stats.get("hits", 0))
                        results[(game_pk, pid)] = hits
    return results


def backfill_results(days=3):
    """For each of the past N days, fetch actual hits and update hit_picks_daily."""
    for d in range(1, days + 1):
        game_date = TODAY - timedelta(days=d)
        log.info("Backfilling hit results for %s", game_date)

        actuals = fetch_actual_hits(game_date)
        if not actuals:
            log.info("No completed games found for %s", game_date)
            continue

        updated = 0
        for (game_pk, batter_id), actual_hits in actuals.items():
            update_sql = f"""
            UPDATE {TABLE}.hit_picks_daily`
            SET actual_hits = @actual_hits,
                hit = (@actual_hits >= 1)
            WHERE run_date = @run_date
              AND game_pk = @game_pk
              AND batter_id = @batter_id
              AND actual_hits IS NULL
            """
            params = [
                bigquery.ScalarQueryParameter("actual_hits", "INT64", actual_hits),
                bigquery.ScalarQueryParameter("run_date", "DATE", game_date.isoformat()),
                bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
                bigquery.ScalarQueryParameter("batter_id", "INT64", batter_id),
            ]
            try:
                job = client.query(
                    update_sql,
                    job_config=bigquery.QueryJobConfig(query_parameters=params),
                )
                job.result()
                if job.num_dml_affected_rows and job.num_dml_affected_rows > 0:
                    updated += 1
            except Exception as exc:
                log.debug("Update failed for game %s batter %s: %s", game_pk, batter_id, exc)

        log.info("Updated %s hit results for %s", updated, game_date)


# ── 2. Grade Hit Rates ───────────────────────────────────────────────────

def grade_hit_rates(days=30):
    """How often does each grade actually produce 1+ hit?"""
    sql = f"""
    SELECT
      grade,
      COUNT(*) AS total_picks,
      SUM(CASE WHEN hit THEN 1 ELSE 0 END) AS hits,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hit_rate_pct,
      ROUND(AVG(score), 1) AS avg_pulse,
      ROUND(AVG(best_price), 0) AS avg_odds
    FROM {TABLE}.hit_picks_daily`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
      AND actual_hits IS NOT NULL
    GROUP BY grade
    ORDER BY hit_rate_pct DESC
    """
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


# ── 3. Self-Learning Weight Calibration ──────────────────────────────────

def calibrate_weights(days=30):
    """
    The hit learning engine. Adjusts weights based on factor correlations
    with actual 1+ hit outcomes.
    """
    log.info("Starting hit weight calibration with %s-day lookback", days)

    # Load current weights
    try:
        rows = list(client.query(f"""
            SELECT factor, weight
            FROM {TABLE}.hit_model_weights`
            WHERE run_date = (SELECT MAX(run_date) FROM {TABLE}.hit_model_weights`)
        """).result())
        current_weights = {r.factor: r.weight for r in rows} if rows else dict(BASELINE_WEIGHTS)
    except Exception:
        current_weights = dict(BASELINE_WEIGHTS)

    # Factor correlations with actual outcomes
    sql = f"""
    WITH picks AS (
      SELECT *
      FROM {TABLE}.hit_picks_daily`
      WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
        AND actual_hits IS NOT NULL
        AND hit IS NOT NULL
    )
    SELECT 'batting_avg' AS factor,
      SAFE_DIVIDE(SUM(CASE WHEN batting_avg_vs_hand >= 0.300 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN batting_avg_vs_hand >= 0.300 THEN 1 ELSE 0 END), 0)) AS high_hr,
      CORR(batting_avg_vs_hand, CAST(hit AS INT64)) AS corr,
      COUNT(*) AS n
    FROM picks

    UNION ALL
    SELECT 'contact_rate',
      SAFE_DIVIDE(SUM(CASE WHEN contact_rate >= 82 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN contact_rate >= 82 THEN 1 ELSE 0 END), 0)),
      CORR(contact_rate, CAST(hit AS INT64)), COUNT(*)
    FROM picks

    UNION ALL
    SELECT 'l15_hit_rate',
      SAFE_DIVIDE(SUM(CASE WHEN l15_hit_rate >= 0.350 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN l15_hit_rate >= 0.350 THEN 1 ELSE 0 END), 0)),
      CORR(l15_hit_rate, CAST(hit AS INT64)), COUNT(*)
    FROM picks

    UNION ALL
    SELECT 'hard_hit_pct',
      SAFE_DIVIDE(SUM(CASE WHEN hard_hit_pct >= 45 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN hard_hit_pct >= 45 THEN 1 ELSE 0 END), 0)),
      CORR(hard_hit_pct, CAST(hit AS INT64)), COUNT(*)
    FROM picks

    UNION ALL
    SELECT 'p_whip',
      SAFE_DIVIDE(SUM(CASE WHEN p_whip >= 1.30 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_whip >= 1.30 THEN 1 ELSE 0 END), 0)),
      CORR(p_whip, CAST(hit AS INT64)), COUNT(*)
    FROM picks

    UNION ALL
    SELECT 'p_k_rate_inv',
      SAFE_DIVIDE(SUM(CASE WHEN p_k_rate <= 20 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_k_rate <= 20 THEN 1 ELSE 0 END), 0)),
      CORR(-p_k_rate, CAST(hit AS INT64)), COUNT(*)
    FROM picks

    UNION ALL
    SELECT 'p_woba_allowed',
      SAFE_DIVIDE(SUM(CASE WHEN p_woba_allowed >= 0.320 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_woba_allowed >= 0.320 THEN 1 ELSE 0 END), 0)),
      CORR(p_woba_allowed, CAST(hit AS INT64)), COUNT(*)
    FROM picks

    UNION ALL
    SELECT 'pf_rating',
      SAFE_DIVIDE(SUM(CASE WHEN pf_rating >= 75 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN pf_rating >= 75 THEN 1 ELSE 0 END), 0)),
      CORR(pf_rating, CAST(hit AS INT64)), COUNT(*)
    FROM picks

    UNION ALL
    SELECT 'bvp_history', NULL, NULL, COUNT(*) FROM picks
    UNION ALL
    SELECT 'platoon_edge', NULL, NULL, COUNT(*) FROM picks
    UNION ALL
    SELECT 'arsenal_contact', NULL, NULL, COUNT(*) FROM picks
    UNION ALL
    SELECT 'babip_proxy', NULL, NULL, COUNT(*) FROM picks
    UNION ALL
    SELECT 'hit_rate_l10', NULL, NULL, COUNT(*) FROM picks
    UNION ALL
    SELECT 'hit_rate_season', NULL, NULL, COUNT(*) FROM picks
    UNION ALL
    SELECT 'hit_rate_vs_team', NULL, NULL, COUNT(*) FROM picks
    UNION ALL
    SELECT 'vegas_total', NULL, NULL, COUNT(*) FROM picks
    UNION ALL
    SELECT 'avg_l10_vs_line', NULL, NULL, COUNT(*) FROM picks
    UNION ALL
    SELECT 'streak', NULL, NULL, COUNT(*) FROM picks
    """
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]

    try:
        rows = list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    except Exception as exc:
        log.error("Hit factor analysis query failed: %s", exc)
        return

    sample_size = rows[0].n if rows else 0
    if sample_size < MIN_SAMPLE_SIZE:
        log.info("Only %s hit results — need %s before calibrating", sample_size, MIN_SAMPLE_SIZE)
        return

    new_weights = dict(current_weights)
    weight_rows = []

    for row in rows:
        factor = row.factor
        corr = row.corr
        high_hr = row.high_hr
        n = row.n

        if factor not in new_weights:
            continue

        baseline = BASELINE_WEIGHTS.get(factor, new_weights[factor])

        if corr is None:
            weight_rows.append({
                "run_date": TODAY.isoformat(),
                "factor": factor,
                "weight": round(new_weights[factor], 3),
                "baseline": round(baseline, 3),
                "sample_size": n,
                "correlation": None,
                "hit_rate_pct": round(high_hr * 100, 1) if high_hr else None,
                "updated_at": NOW.isoformat(),
            })
            continue

        adjustment = corr * LEARNING_RATE * new_weights[factor]
        old_weight = new_weights[factor]
        new_weight = max(WEIGHT_FLOOR, min(WEIGHT_CEILING, old_weight + adjustment))
        new_weights[factor] = round(new_weight, 3)

        log.info(
            "  %s: corr=%.3f high=%.1f%% weight %.1f -> %.1f (%+.2f)",
            factor, corr, (high_hr or 0) * 100, old_weight, new_weight, adjustment,
        )

        weight_rows.append({
            "run_date": TODAY.isoformat(),
            "factor": factor,
            "weight": round(new_weight, 3),
            "baseline": round(baseline, 3),
            "sample_size": n,
            "correlation": round(corr, 4),
            "hit_rate_pct": round(high_hr * 100, 1) if high_hr else None,
            "updated_at": NOW.isoformat(),
        })

    # Persist factors without correlation data
    for factor, weight in new_weights.items():
        if not any(r["factor"] == factor for r in weight_rows):
            weight_rows.append({
                "run_date": TODAY.isoformat(),
                "factor": factor,
                "weight": round(weight, 3),
                "baseline": round(BASELINE_WEIGHTS.get(factor, weight), 3),
                "sample_size": sample_size,
                "correlation": None,
                "hit_rate_pct": None,
                "updated_at": NOW.isoformat(),
            })

    if weight_rows:
        errors = client.insert_rows_json(
            f"{PROJECT}.{DATASET}.hit_model_weights", weight_rows
        )
        if errors:
            log.error("BQ insert errors for hit_model_weights: %s", errors[:3])
        else:
            log.info("Wrote %s calibrated hit weights", len(weight_rows))


# ── 4. Score Calibration ─────────────────────────────────────────────────

def score_calibration(days=30):
    """Are Hit-Pulse scores well-calibrated?"""
    sql = f"""
    SELECT
      CASE
        WHEN score >= 80 THEN '80+ FIRE'
        WHEN score >= 65 THEN '65-79 STRONG'
        WHEN score >= 50 THEN '50-64 LEAN'
        ELSE 'Under 50 SKIP'
      END AS bucket,
      COUNT(*) AS total,
      SUM(CASE WHEN hit THEN 1 ELSE 0 END) AS hits,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hit_rate_pct,
      ROUND(AVG(best_price), 0) AS avg_odds,
      ROUND(
        SAFE_DIVIDE(
          SUM(CASE
            WHEN hit AND best_price > 0 THEN best_price / 100.0 * 10
            WHEN hit AND best_price < 0 THEN 100.0 / ABS(best_price) * 10
            ELSE -10
          END),
          COUNT(*) * 10
        ) * 100, 1
      ) AS roi_pct
    FROM {TABLE}.hit_picks_daily`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
      AND actual_hits IS NOT NULL
    GROUP BY bucket
    ORDER BY bucket DESC
    """
    params = [bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat())]
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


# ── 5. Batter Reliability ────────────────────────────────────────────────

def batter_reliability(days=60):
    """Which batters consistently get 1+ hit when we pick them?"""
    sql = f"""
    SELECT
      batter_name,
      COUNT(*) AS picks,
      SUM(CASE WHEN hit THEN 1 ELSE 0 END) AS hits,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hit_rate_pct,
      ROUND(AVG(score), 1) AS avg_pulse,
      ROUND(AVG(actual_hits), 1) AS avg_actual_hits
    FROM {TABLE}.hit_picks_daily`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
      AND actual_hits IS NOT NULL
      AND grade IN ('FIRE', 'STRONG')
    GROUP BY batter_name
    HAVING COUNT(*) >= 3
    ORDER BY hit_rate_pct DESC
    """
    params = [bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat())]
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


# ── Report ────────────────────────────────────────────────────────────────

def print_report(days=30):
    print(f"\n{'='*70}")
    print(f"HIT ANALYTICS REPORT — Last {days} days (as of {TODAY})")
    print(f"{'='*70}")

    print("\n  GRADE HIT RATES")
    print(f"{'Grade':<10} {'Picks':>6} {'Hits':>5} {'Rate':>7} {'Avg Pulse':>10} {'Avg Odds':>9}")
    for r in grade_hit_rates(days):
        print(
            f"{r.grade:<10} {r.total_picks:>6} {r.hits:>5} "
            f"{r.hit_rate_pct:>6.1f}% {r.avg_pulse:>10.1f} {r.avg_odds:>+9.0f}"
        )

    print("\n  SCORE CALIBRATION")
    print(f"{'Bucket':<18} {'Picks':>6} {'Hits':>5} {'Rate':>7} {'ROI':>7}")
    for r in score_calibration(days):
        roi = f"{r.roi_pct:>+6.1f}%" if r.roi_pct is not None else "    N/A"
        print(f"{r.bucket:<18} {r.total:>6} {r.hits:>5} {r.hit_rate_pct:>6.1f}% {roi}")

    print("\n  TOP RELIABLE BATTERS (FIRE/STRONG)")
    for r in batter_reliability(days):
        if r.picks < 3:
            continue
        emoji = "+" if r.hit_rate_pct >= 75 else "~" if r.hit_rate_pct >= 60 else "-"
        print(
            f"  {emoji} {r.batter_name:<22} {r.picks:>3} picks  "
            f"{r.hit_rate_pct:>5.1f}% hit  avg hits:{r.avg_actual_hits:.1f}"
        )


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    log.info("Hit Analytics starting...")

    log.info("Step 1: Backfilling hit results (last 3 days)...")
    backfill_results(days=3)

    log.info("Step 2: Calibrating hit weights...")
    calibrate_weights(days=30)

    log.info("Step 3: Generating report...")
    print_report(30)


if __name__ == "__main__":
    main()
