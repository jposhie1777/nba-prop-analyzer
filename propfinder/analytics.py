"""
analytics.py — HR pick analytics: back-testing, EV modeling, correlations,
pitcher exploitability, stacking, and CLV tracking.

Run standalone to generate a daily analytics report, or import individual functions.
"""

import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from google.cloud import bigquery

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "graphite-flare-477419-h7")
DATASET = "propfinder"
ET = ZoneInfo("America/New_York")
TODAY = datetime.now(ET).date()

client = bigquery.Client(project=PROJECT)

TABLE = f"`{PROJECT}.{DATASET}"


# ── 1. Results Tracking ────────────────────────────────────────────────────
# Join picks with actual game results from MLB Stats API

def setup_results_view():
    """Create a view that joins HR picks with actual outcomes."""
    sql = f"""
    CREATE OR REPLACE VIEW {TABLE}.vw_hr_pick_results` AS
    SELECT
      p.run_date,
      p.game_pk,
      p.batter_id,
      p.batter_name,
      p.bat_side,
      p.pitcher_id,
      p.pitcher_name,
      p.pitcher_hand,
      p.score AS pulse_score,
      p.grade,
      p.why,
      p.flags,
      p.iso,
      p.slg,
      p.l15_ev,
      p.l15_barrel_pct,
      p.season_ev,
      p.season_barrel_pct,
      p.l15_hard_hit_pct,
      p.hr_fb_pct,
      p.p_hr9_vs_hand,
      p.p_hr_fb_pct,
      p.p_barrel_pct,
      p.p_fb_pct,
      p.p_hard_hit_pct,
      p.hr_odds_best_price,
      p.hr_odds_best_book,
      p.weather_indicator,
      p.game_temp,
      p.wind_speed,
      p.ballpark_name,
      -- Actual result: did the batter hit a HR in this game?
      COALESCE(r.home_runs, 0) AS actual_hr,
      CASE WHEN COALESCE(r.home_runs, 0) >= 1 THEN TRUE ELSE FALSE END AS hr_hit,
      r.hits AS actual_hits,
      r.at_bats AS actual_ab,
    FROM {TABLE}.hr_picks_daily` p
    LEFT JOIN {TABLE}.raw_pitcher_vs_batting_order` r
      ON p.run_date = r.run_date
      AND p.game_pk = r.game_pk
      AND p.batter_id = r.batter_id
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY p.run_date, p.batter_id, p.pitcher_id
      ORDER BY p.score DESC
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
    log.info("Setting up analytics views...")
    setup_all()
    log.info("Generating report...")
    print_report(30)


if __name__ == "__main__":
    main()
