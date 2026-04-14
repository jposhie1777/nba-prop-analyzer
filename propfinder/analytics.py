"""
analytics.py — HR pick analytics: back-testing, EV modeling, correlations,
pitcher exploitability, stacking, and CLV tracking.

Run standalone to generate a daily analytics report, or import individual functions.
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

# ── HR Model Baseline Weights ────────────────────────────────────────────
# These map to the scoring factors in model.py compute_pulse_score().
# Each factor's weight controls how many raw points it contributes.
BASELINE_WEIGHTS = {
    "p_hr9_vs_hand":   20.0,   # Pitcher HR/9 vs handedness
    "p_hr_fb_pct":     18.0,   # Pitcher HR/FB%
    "p_fb_pct":         8.0,   # Pitcher fly-ball rate
    "p_barrel_pct":     7.0,   # Pitcher barrel% allowed
    "p_hard_hit_pct":   5.0,   # Pitcher hard-hit% allowed
    "p_iso_allowed":    4.0,   # Pitcher ISO allowed
    "platoon":          3.0,   # Platoon edge (same-hand)
    "b_iso":           15.0,   # Batter ISO
    "b_slg":           15.0,   # Batter SLG
    "b_ev":            15.0,   # Batter L15 exit velocity
    "b_barrel":        15.0,   # Batter L15 barrel%
    "hot_form":         4.0,   # Hot-form bonus (elite EV + barrel + HH%)
}

LEARNING_RATE = 0.15          # How aggressively weights shift per cycle
MIN_SAMPLE_SIZE = 20          # Need this many results before adjusting
WEIGHT_FLOOR = 1.0            # Never let a weight drop below this
WEIGHT_CEILING = 25.0         # Never let a weight exceed this


# ── 0a. Results Backfill: Fetch actual HRs from MLB API ──────────────────

def fetch_actual_home_runs(game_date):
    """
    Fetch actual batter HR totals for all games on a given date.
    Returns dict: (game_pk, batter_id) → actual_hr_count.
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
                        hr = int(stats.get("homeRuns", 0))
                        results[(game_pk, pid)] = hr
    return results


def backfill_hr_results(days=3):
    """
    For each of the past N days, fetch actual HR totals and update hr_picks_daily.
    """
    for d in range(1, days + 1):
        game_date = TODAY - timedelta(days=d)
        log.info("Backfilling HR results for %s", game_date)

        actuals = fetch_actual_home_runs(game_date)
        if not actuals:
            log.info("No completed games found for %s", game_date)
            continue

        updated = 0
        for (game_pk, batter_id), actual_hr in actuals.items():
            update_sql = f"""
            UPDATE {TABLE}.hr_picks_daily`
            SET actual_hr = @actual_hr,
                hit = (@actual_hr >= 1)
            WHERE run_date = @run_date
              AND game_pk = @game_pk
              AND batter_id = @batter_id
              AND actual_hr IS NULL
            """
            params = [
                bigquery.ScalarQueryParameter("actual_hr", "INT64", actual_hr),
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

        log.info("Updated %s pick results for %s", updated, game_date)


# ── 0b. Self-Learning Weight Calibration ─────────────────────────────────

def calibrate_hr_weights(days=30):
    """
    The HR learning engine. Adjusts factor weights based on correlation
    with actual HR outcomes.

    Strategy (mirrors k_analytics.py):
    - Factors with positive correlation to HR hits → boost weight
    - Factors with negative/zero correlation → dampen weight
    - Apply learning rate to avoid wild swings
    - Enforce floor/ceiling to keep all factors in play
    """
    log.info("Starting HR weight calibration with %s-day lookback", days)

    # Load current weights (most recent from hr_model_weights)
    try:
        rows = list(client.query(f"""
            SELECT factor, weight
            FROM {TABLE}.hr_model_weights`
            WHERE run_date = (SELECT MAX(run_date) FROM {TABLE}.hr_model_weights`)
        """).result())
        current_weights = {r.factor: r.weight for r in rows} if rows else dict(BASELINE_WEIGHTS)
    except Exception:
        current_weights = dict(BASELINE_WEIGHTS)

    # Compute per-factor correlation with actual HR outcomes
    sql = f"""
    WITH picks AS (
      SELECT *
      FROM {TABLE}.hr_picks_daily`
      WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
        AND actual_hr IS NOT NULL
        AND hit IS NOT NULL
    ),
    overall AS (
      SELECT
        SAFE_DIVIDE(SUM(CASE WHEN hit THEN 1 ELSE 0 END), COUNT(*)) AS base_hit_rate,
        COUNT(*) AS total
      FROM picks
    )
    -- Pitcher HR/9 vs hand
    SELECT 'p_hr9_vs_hand' AS factor,
      SAFE_DIVIDE(SUM(CASE WHEN p_hr9_vs_hand >= 1.8 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_hr9_vs_hand >= 1.8 THEN 1 ELSE 0 END), 0)) AS high_hr,
      CORR(p_hr9_vs_hand, CAST(hit AS INT64)) AS corr,
      COUNT(*) AS n
    FROM picks, overall

    UNION ALL
    SELECT 'p_hr_fb_pct',
      SAFE_DIVIDE(SUM(CASE WHEN p_hr_fb_pct >= 15 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_hr_fb_pct >= 15 THEN 1 ELSE 0 END), 0)),
      CORR(p_hr_fb_pct, CAST(hit AS INT64)), COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'p_fb_pct',
      SAFE_DIVIDE(SUM(CASE WHEN p_fb_pct >= 40 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_fb_pct >= 40 THEN 1 ELSE 0 END), 0)),
      CORR(p_fb_pct, CAST(hit AS INT64)), COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'p_barrel_pct',
      SAFE_DIVIDE(SUM(CASE WHEN p_barrel_pct >= 10 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_barrel_pct >= 10 THEN 1 ELSE 0 END), 0)),
      CORR(p_barrel_pct, CAST(hit AS INT64)), COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'p_hard_hit_pct',
      SAFE_DIVIDE(SUM(CASE WHEN p_hard_hit_pct >= 40 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_hard_hit_pct >= 40 THEN 1 ELSE 0 END), 0)),
      CORR(p_hard_hit_pct, CAST(hit AS INT64)), COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'b_iso',
      SAFE_DIVIDE(SUM(CASE WHEN iso >= 0.300 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN iso >= 0.300 THEN 1 ELSE 0 END), 0)),
      CORR(iso, CAST(hit AS INT64)), COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'b_slg',
      SAFE_DIVIDE(SUM(CASE WHEN slg >= 0.500 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN slg >= 0.500 THEN 1 ELSE 0 END), 0)),
      CORR(slg, CAST(hit AS INT64)), COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'b_ev',
      SAFE_DIVIDE(SUM(CASE WHEN l15_ev >= 92 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN l15_ev >= 92 THEN 1 ELSE 0 END), 0)),
      CORR(l15_ev, CAST(hit AS INT64)), COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'b_barrel',
      SAFE_DIVIDE(SUM(CASE WHEN l15_barrel_pct >= 20 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN l15_barrel_pct >= 20 THEN 1 ELSE 0 END), 0)),
      CORR(l15_barrel_pct, CAST(hit AS INT64)), COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'p_iso_allowed', NULL, NULL, COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'platoon', NULL, NULL, COUNT(*)
    FROM picks, overall

    UNION ALL
    SELECT 'hot_form', NULL, NULL, COUNT(*)
    FROM picks, overall
    """
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]

    try:
        rows = list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    except Exception as exc:
        log.error("HR factor analysis query failed: %s", exc)
        return

    sample_size = rows[0].n if rows else 0
    if sample_size < MIN_SAMPLE_SIZE:
        log.info("Only %s HR results — need %s before calibrating weights", sample_size, MIN_SAMPLE_SIZE)
        return

    # Apply learning adjustments
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
            # No correlation data — keep current weight
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

        # Adjustment: positive correlation → boost, negative → dampen
        adjustment = corr * LEARNING_RATE * new_weights[factor]
        old_weight = new_weights[factor]
        new_weight = max(WEIGHT_FLOOR, min(WEIGHT_CEILING, old_weight + adjustment))
        new_weights[factor] = round(new_weight, 3)

        log.info(
            "  %s: corr=%.3f high_hr=%.1f%% weight %.1f → %.1f (%+.2f)",
            factor, corr, (high_hr or 0) * 100, old_weight, new_weight, adjustment,
        )

        weight_rows.append({
            "run_date": TODAY.isoformat(),
            "factor": factor,
            "weight": round(new_weight, 3),
            "baseline": round(baseline, 3),
            "sample_size": n,
            "correlation": round(corr, 4) if corr is not None else None,
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
            f"{PROJECT}.{DATASET}.hr_model_weights", weight_rows
        )
        if errors:
            log.error("BQ insert errors for hr_model_weights: %s", errors[:3])
        else:
            log.info("Wrote %s calibrated HR weights to hr_model_weights", len(weight_rows))


# ── 1. Results Tracking ────────────────────────────────────────────────────
# Join picks with actual game results from MLB Stats API

def setup_results_view():
    """Create a view over HR picks with actual outcomes (backfilled columns)."""
    sql = f"""
    CREATE OR REPLACE VIEW {TABLE}.vw_hr_pick_results` AS
    SELECT
      run_date,
      game_pk,
      batter_id,
      batter_name,
      bat_side,
      pitcher_id,
      pitcher_name,
      pitcher_hand,
      score AS pulse_score,
      grade,
      why,
      flags,
      iso,
      slg,
      l15_ev,
      l15_barrel_pct,
      season_ev,
      season_barrel_pct,
      l15_hard_hit_pct,
      hr_fb_pct,
      p_hr9_vs_hand,
      p_hr_fb_pct,
      p_barrel_pct,
      p_fb_pct,
      p_hard_hit_pct,
      hr_odds_best_price,
      hr_odds_best_book,
      weather_indicator,
      game_temp,
      wind_speed,
      ballpark_name,
      COALESCE(actual_hr, 0) AS actual_hr,
      COALESCE(hit, FALSE) AS hr_hit,
    FROM {TABLE}.hr_picks_daily`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY run_date, batter_id, pitcher_id
      ORDER BY score DESC
    ) = 1
    """
    client.query(sql).result()
    log.info("Created vw_hr_pick_results view")


# ── 2. Back-Test: Grade Hit Rates ──────────────────────────────────────────

def backtest_grade_hit_rates(days=30):
    """How often does each grade actually produce a HR?"""
    sql = f"""
    SELECT
      grade,
      COUNT(*) AS total_picks,
      SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END) AS hits,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hit_rate_pct,
      ROUND(AVG(pulse_score), 1) AS avg_pulse,
      ROUND(AVG(hr_odds_best_price), 0) AS avg_odds
    FROM {TABLE}.vw_hr_pick_results`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
    GROUP BY grade
    ORDER BY hit_rate_pct DESC
    """
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
    rows = list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    return rows


# ── 3. Flag Correlation Analysis ───────────────────────────────────────────

def setup_flag_analysis_view():
    """Create a view that explodes flags and correlates with HR outcomes."""
    sql = f"""
    CREATE OR REPLACE VIEW {TABLE}.vw_hr_flag_correlation` AS
    SELECT
      flag,
      COUNT(*) AS total,
      SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END) AS hits,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hit_rate_pct,
      ROUND(AVG(pulse_score), 1) AS avg_pulse,
      ROUND(AVG(hr_odds_best_price), 0) AS avg_odds
    FROM {TABLE}.vw_hr_pick_results`,
    UNNEST(SPLIT(flags, ',')) AS flag
    WHERE flag != '' AND flag IS NOT NULL
    GROUP BY flag
    HAVING COUNT(*) >= 5
    ORDER BY hit_rate_pct DESC
    """
    client.query(sql).result()
    log.info("Created vw_hr_flag_correlation view")


# ── 4. Expected Value (EV) Modeling ────────────────────────────────────────

def setup_ev_view():
    """Create a view computing expected value per pick."""
    sql = f"""
    CREATE OR REPLACE VIEW {TABLE}.vw_hr_expected_value` AS
    SELECT
      *,
      -- Convert American odds to implied probability
      CASE
        WHEN hr_odds_best_price > 0 THEN ROUND(100.0 / (hr_odds_best_price + 100) * 100, 1)
        WHEN hr_odds_best_price < 0 THEN ROUND(ABS(hr_odds_best_price) / (ABS(hr_odds_best_price) + 100) * 100, 1)
        ELSE NULL
      END AS implied_prob_pct,
      -- Model probability from historical hit rate of similar Pulse scores
      -- (will be populated by the hit rate analysis)
      ROUND(pulse_score / 100.0 * 15, 1) AS model_prob_pct,  -- rough initial estimate
      -- EV = (model_prob * payout) - (1 - model_prob) * stake
      -- Simplified: EV% = model_prob * (odds/100 + 1) - 1  for positive odds
      CASE
        WHEN hr_odds_best_price > 0 THEN
          ROUND((pulse_score / 100.0 * 0.15) * (hr_odds_best_price / 100.0 + 1) - 1, 3)
        WHEN hr_odds_best_price < 0 THEN
          ROUND((pulse_score / 100.0 * 0.15) * (100.0 / ABS(hr_odds_best_price) + 1) - 1, 3)
        ELSE NULL
      END AS ev_estimate
    FROM {TABLE}.vw_hr_pick_results`
    """
    client.query(sql).result()
    log.info("Created vw_hr_expected_value view")


def top_ev_picks(days=7):
    """Find the highest +EV picks from recent data."""
    sql = f"""
    SELECT
      run_date, batter_name, pitcher_name, grade, pulse_score,
      hr_odds_best_price, implied_prob_pct, model_prob_pct,
      ev_estimate, hr_hit
    FROM {TABLE}.vw_hr_expected_value`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
      AND ev_estimate > 0
    ORDER BY ev_estimate DESC
    LIMIT 20
    """
    params = [bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat())]
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


# ── 5. Pitcher Exploitability Index ────────────────────────────────────────

def setup_pitcher_exploitability_view():
    """Composite score for how exploitable each pitcher is."""
    sql = f"""
    CREATE OR REPLACE VIEW {TABLE}.vw_pitcher_exploitability` AS
    SELECT
      pitcher_id,
      pitcher_name,
      pitcher_hand,
      COUNT(DISTINCT run_date) AS games_tracked,
      COUNT(*) AS times_targeted,
      SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END) AS hrs_allowed_to_picks,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS pick_hr_rate_pct,
      ROUND(AVG(p_hr9_vs_hand), 2) AS avg_hr9,
      ROUND(AVG(p_hr_fb_pct), 1) AS avg_hr_fb_pct,
      ROUND(AVG(p_barrel_pct), 1) AS avg_barrel_pct,
      ROUND(AVG(p_fb_pct), 1) AS avg_fb_pct,
      ROUND(AVG(p_hard_hit_pct), 1) AS avg_hard_hit_pct,
      -- Composite exploitability index (0-100)
      ROUND(
        (COALESCE(AVG(p_hr9_vs_hand), 0) / 3.0 * 25) +  -- HR/9 component (max ~3.0)
        (COALESCE(AVG(p_hr_fb_pct), 0) / 25.0 * 25) +    -- HR/FB component (max ~25%)
        (COALESCE(AVG(p_barrel_pct), 0) / 15.0 * 25) +    -- Barrel component (max ~15%)
        (COALESCE(AVG(p_fb_pct), 0) / 50.0 * 25)          -- FB% component (max ~50%)
      , 1) AS exploitability_index
    FROM {TABLE}.vw_hr_pick_results`
    GROUP BY pitcher_id, pitcher_name, pitcher_hand
    HAVING COUNT(*) >= 3
    ORDER BY exploitability_index DESC
    """
    client.query(sql).result()
    log.info("Created vw_pitcher_exploitability view")


# ── 6. Stacking Analysis ──────────────────────────────────────────────────

def setup_stacking_view():
    """Identify games where multiple picks from same team hit HRs."""
    sql = f"""
    CREATE OR REPLACE VIEW {TABLE}.vw_hr_stacking` AS
    SELECT
      run_date,
      game_pk,
      ballpark_name,
      weather_indicator,
      game_temp,
      pitcher_name,
      pitcher_hand,
      COUNT(*) AS picks_in_game,
      SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END) AS hrs_in_game,
      ROUND(AVG(pulse_score), 1) AS avg_pulse,
      ROUND(AVG(hr_odds_best_price), 0) AS avg_odds,
      ARRAY_AGG(
        STRUCT(batter_name, pulse_score, grade, hr_hit, hr_odds_best_price)
        ORDER BY pulse_score DESC
      ) AS batters
    FROM {TABLE}.vw_hr_pick_results`
    GROUP BY run_date, game_pk, ballpark_name, weather_indicator, game_temp,
             pitcher_name, pitcher_hand
    HAVING COUNT(*) >= 2
    ORDER BY hrs_in_game DESC, picks_in_game DESC
    """
    client.query(sql).result()
    log.info("Created vw_hr_stacking view")


# ── 7. Weather Correlation ─────────────────────────────────────────────────

def weather_correlation(days=60):
    """How does weather affect HR rates?"""
    sql = f"""
    SELECT
      weather_indicator,
      COUNT(*) AS total_picks,
      SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END) AS hrs,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hr_rate_pct,
      ROUND(AVG(game_temp), 0) AS avg_temp,
      ROUND(AVG(wind_speed), 1) AS avg_wind
    FROM {TABLE}.vw_hr_pick_results`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
      AND weather_indicator IS NOT NULL
    GROUP BY weather_indicator
    ORDER BY hr_rate_pct DESC
    """
    params = [bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat())]
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


# ── 8. Ballpark Factor Analysis ────────────────────────────────────────────

def setup_ballpark_view():
    """HR rates by ballpark."""
    sql = f"""
    CREATE OR REPLACE VIEW {TABLE}.vw_hr_ballpark_factors` AS
    SELECT
      ballpark_name,
      COUNT(*) AS total_picks,
      SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END) AS hrs,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hr_rate_pct,
      ROUND(AVG(pulse_score), 1) AS avg_pulse,
      ROUND(AVG(game_temp), 0) AS avg_temp
    FROM {TABLE}.vw_hr_pick_results`
    WHERE ballpark_name IS NOT NULL
    GROUP BY ballpark_name
    HAVING COUNT(*) >= 5
    ORDER BY hr_rate_pct DESC
    """
    client.query(sql).result()
    log.info("Created vw_hr_ballpark_factors view")


# ── 9. CLV Tracking (Closing Line Value) ───────────────────────────────────

def setup_clv_view():
    """Track if our picks beat the closing line.
    Requires odds snapshots — for now, compare best price at pick time
    vs the implied probability from results."""
    sql = f"""
    CREATE OR REPLACE VIEW {TABLE}.vw_hr_clv_tracking` AS
    SELECT
      run_date,
      batter_name,
      pitcher_name,
      grade,
      pulse_score,
      hr_odds_best_price AS opening_odds,
      hr_odds_best_book,
      -- Implied probability from the odds
      CASE
        WHEN hr_odds_best_price > 0 THEN ROUND(100.0 / (hr_odds_best_price + 100) * 100, 1)
        WHEN hr_odds_best_price < 0 THEN ROUND(ABS(hr_odds_best_price) / (ABS(hr_odds_best_price) + 100) * 100, 1)
        ELSE NULL
      END AS implied_prob_pct,
      hr_hit,
      actual_hr
    FROM {TABLE}.vw_hr_pick_results`
    ORDER BY run_date DESC, pulse_score DESC
    """
    client.query(sql).result()
    log.info("Created vw_hr_clv_tracking view")


# ── 10. Pulse Score Calibration ────────────────────────────────────────────

def pulse_calibration(days=60):
    """Are Pulse scores well-calibrated? Group by score bucket and check hit rates."""
    sql = f"""
    SELECT
      CASE
        WHEN pulse_score >= 90 THEN '90-100'
        WHEN pulse_score >= 80 THEN '80-89'
        WHEN pulse_score >= 70 THEN '70-79'
        WHEN pulse_score >= 60 THEN '60-69'
        WHEN pulse_score >= 50 THEN '50-59'
        ELSE 'Under 50'
      END AS score_bucket,
      COUNT(*) AS total,
      SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END) AS hits,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hr_hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hit_rate_pct,
      ROUND(AVG(hr_odds_best_price), 0) AS avg_odds,
      -- ROI if flat betting $10 on every pick
      ROUND(
        SAFE_DIVIDE(
          SUM(CASE
            WHEN hr_hit AND hr_odds_best_price > 0 THEN hr_odds_best_price / 100.0 * 10
            WHEN hr_hit AND hr_odds_best_price < 0 THEN 100.0 / ABS(hr_odds_best_price) * 10
            ELSE -10
          END),
          COUNT(*) * 10
        ) * 100, 1
      ) AS roi_pct
    FROM {TABLE}.vw_hr_pick_results`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
    GROUP BY score_bucket
    ORDER BY score_bucket DESC
    """
    params = [bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat())]
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


# ── Setup All Views ────────────────────────────────────────────────────────

def setup_all():
    """Create all analytics views."""
    setup_results_view()
    setup_flag_analysis_view()
    setup_ev_view()
    setup_pitcher_exploitability_view()
    setup_stacking_view()
    setup_ballpark_view()
    setup_clv_view()
    log.info("All analytics views created")


# ── Report ─────────────────────────────────────────────────────────────────

def print_report(days=30):
    """Print a summary analytics report."""
    print(f"\n{'='*70}")
    print(f"HR ANALYTICS REPORT — Last {days} days (as of {TODAY})")
    print(f"{'='*70}")

    print("\n📊 GRADE HIT RATES")
    print(f"{'Grade':<12} {'Picks':>6} {'Hits':>5} {'Rate':>7} {'Avg Pulse':>10} {'Avg Odds':>9}")
    for r in backtest_grade_hit_rates(days):
        print(f"{r.grade:<12} {r.total_picks:>6} {r.hits:>5} {r.hit_rate_pct:>6.1f}% {r.avg_pulse:>10.1f} {r.avg_odds:>+9.0f}")

    print("\n🌤️ WEATHER CORRELATION")
    print(f"{'Weather':<10} {'Picks':>6} {'HRs':>5} {'Rate':>7} {'Avg Temp':>9}")
    for r in weather_correlation(days):
        print(f"{r.weather_indicator:<10} {r.total_picks:>6} {r.hrs:>5} {r.hr_rate_pct:>6.1f}% {r.avg_temp:>8.0f}°")

    print("\n🎯 PULSE SCORE CALIBRATION")
    print(f"{'Bucket':<12} {'Picks':>6} {'Hits':>5} {'Rate':>7} {'Avg Odds':>9} {'ROI':>7}")
    for r in pulse_calibration(days):
        roi = f"{r.roi_pct:>+6.1f}%" if r.roi_pct is not None else "    N/A"
        print(f"{r.score_bucket:<12} {r.total:>6} {r.hits:>5} {r.hit_rate_pct:>6.1f}% {r.avg_odds:>+9.0f} {roi}")

    print("\n📈 TOP +EV PICKS (last 7 days)")
    for r in top_ev_picks(7):
        hit = "✅" if r.hr_hit else "❌"
        print(f"  {hit} {r.batter_name:<20} vs {r.pitcher_name:<18} {r.grade:<10} "
              f"Pulse:{r.pulse_score:.0f} Odds:{r.hr_odds_best_price:+.0f} EV:{r.ev_estimate:+.3f}")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    log.info("Step 1: Backfilling HR results (last 3 days)...")
    backfill_hr_results(days=3)

    log.info("Step 2: Calibrating HR weights...")
    calibrate_hr_weights(days=30)

    log.info("Step 3: Setting up analytics views...")
    setup_all()

    log.info("Step 4: Generating report...")
    print_report(30)


if __name__ == "__main__":
    main()
