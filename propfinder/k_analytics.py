"""
k_analytics.py — K prop analytics: back-testing, self-learning weight calibration,
results tracking, and factor correlation analysis.

The self-learning loop:
  1. After games complete, fetch actual K totals from MLB Stats API
  2. Join actual results with predictions in k_picks_daily
  3. Compute per-factor correlation with correct predictions
  4. Adjust weights: factors that correlate with hits get boosted,
     factors that correlate with misses get dampened
  5. Write updated weights to k_model_weights table
  6. Next run of k_model.py loads these learned weights

Run standalone for a report, or import functions individually.
"""

import logging
import os
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import json

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

# Baseline weights — same as k_model.py
BASELINE_WEIGHTS = {
    "k_per_9": 12.0, "k_pct": 10.0, "strike_pct": 5.0,
    "arsenal_whiff": 10.0, "high_whiff_pitches": 8.0,
    "opp_k_rank": 10.0,
    "pf_rating": 8.0, "hit_rate_l10": 7.0, "hit_rate_season": 5.0,
    "hit_rate_vs_team": 5.0,
    "avg_l10_vs_line": 10.0, "avg_vs_opp_vs_line": 5.0,
    "vegas_total": 3.0, "streak": 2.0,
}

LEARNING_RATE = 0.15          # How aggressively weights shift per cycle
MIN_SAMPLE_SIZE = 20          # Need this many results before adjusting
WEIGHT_FLOOR = 1.0            # Never let a weight drop below this
WEIGHT_CEILING = 20.0         # Never let a weight exceed this

K_LEAGUE_TABLE = f"{PROJECT}.{DATASET}.k_league_outcomes"


def _safe_float(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


# ── 0a. League-Wide K Outcome Collection ────────────────────────────────

def fetch_k_league_game_data(game_date):
    """
    Fetch ALL starting pitchers + their actual K totals from every completed game.
    Uses per-game live feed for full boxscore + bio data.
    Returns list of dicts with pitcher info, team codes, actual_k, and IP.
    """
    import time as _time
    date_str = game_date.isoformat()

    sched_url = f"{MLB_API}/schedule?sportId=1&date={date_str}"
    req = Request(sched_url, headers={"User-Agent": "PulseSports/1.0"})
    try:
        with urlopen(req, timeout=15) as resp:
            sched = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        log.warning("Failed to fetch MLB schedule for %s: %s", date_str, exc)
        return []

    game_pks = []
    for date_entry in sched.get("dates", []):
        for game in date_entry.get("games", []):
            if game.get("status", {}).get("abstractGameCode") == "F":
                gpk = game.get("gamePk")
                if gpk:
                    game_pks.append(gpk)

    if not game_pks:
        log.info("No completed games for %s", date_str)
        return []

    log.info("Fetching K data for %s completed games on %s", len(game_pks), date_str)

    rows = []
    for gpk in game_pks:
        feed_url = f"https://statsapi.mlb.com/api/v1.1/game/{gpk}/feed/live"
        req = Request(feed_url, headers={"User-Agent": "PulseSports/1.0"})
        try:
            with urlopen(req, timeout=15) as resp:
                feed = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            log.debug("Failed to fetch live feed for game %s: %s", gpk, exc)
            continue

        gd_players = feed.get("gameData", {}).get("players", {})
        bio = {}
        for pid_str, pdata in gd_players.items():
            pid = int(pid_str.replace("ID", ""))
            bio[pid] = {
                "name": pdata.get("fullName", "Unknown"),
                "pitch_hand": pdata.get("pitchHand", {}).get("code", "R"),
            }

        boxscore = feed.get("liveData", {}).get("boxscore", {})
        teams_box = boxscore.get("teams", {})

        # Get team abbreviations from gameData
        gd_teams = feed.get("gameData", {}).get("teams", {})
        team_codes = {
            "away": gd_teams.get("away", {}).get("abbreviation", ""),
            "home": gd_teams.get("home", {}).get("abbreviation", ""),
        }
        opp_codes = {"away": team_codes["home"], "home": team_codes["away"]}

        for side in ("away", "home"):
            pitcher_ids = teams_box.get(side, {}).get("pitchers", [])
            if not pitcher_ids:
                continue

            # Starting pitcher = first in pitchers list
            sp_id = pitcher_ids[0]
            sp_bio = bio.get(sp_id, {})
            sp_pid_str = f"ID{sp_id}"
            sp_data = teams_box.get(side, {}).get("players", {}).get(sp_pid_str, {})
            pitching = sp_data.get("stats", {}).get("pitching", {})

            actual_k = int(pitching.get("strikeOuts", 0))
            ip_str = pitching.get("inningsPitched", "0")
            try:
                ip = float(ip_str)
            except (ValueError, TypeError):
                ip = 0.0

            rows.append({
                "game_pk": gpk,
                "pitcher_id": sp_id,
                "pitcher_name": sp_bio.get("name", "Unknown"),
                "pitcher_hand": sp_bio.get("pitch_hand", "R"),
                "team_code": team_codes[side],
                "opp_team_code": opp_codes[side],
                "actual_k": actual_k,
                "ip": ip,
            })

        _time.sleep(0.3)

    log.info("Fetched %s starter K appearances for %s", len(rows), date_str)
    return rows


def _load_pregame_k_pitcher_stats(game_date):
    """Load pitcher K-specific stats from raw_pitcher_matchup.
    Returns dict: pitcher_id → {k_per_9, k_pct, strike_pct, whip}."""
    date_str = game_date.isoformat()
    sql = f"""
    SELECT pitcher_id, split, ip, whip,
        SAFE_DIVIDE(
            CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(t), '$.strikeouts') AS INT64),
            NULLIF(ip, 0)
        ) * 9 AS k_per_9_calc
    FROM `{PROJECT}.{DATASET}.raw_pitcher_matchup` t
    WHERE run_date = '{date_str}'
      AND split = 'Season'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pitcher_id ORDER BY ingested_at DESC
    ) = 1
    """
    # Simpler approach: just get the Season row and compute from known columns
    sql = f"""
    SELECT pitcher_id, ip, whip
    FROM `{PROJECT}.{DATASET}.raw_pitcher_matchup`
    WHERE run_date = '{date_str}'
      AND split = 'Season'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pitcher_id ORDER BY ingested_at DESC
    ) = 1
    """
    try:
        rows = list(client.query(sql).result())
        return {int(r.pitcher_id): {
            "whip": round(_safe_float(r.whip), 3),
            "ip": round(_safe_float(r.ip), 1),
        } for r in rows}
    except Exception as exc:
        log.warning("Could not load pregame K pitcher stats for %s: %s", date_str, exc)
        return {}


def _load_pregame_pitch_arsenal(game_date):
    """Load pitch arsenal whiff data from raw_pitch_log.
    Returns dict: pitcher_id → {arsenal_whiff_avg, num_high_whiff_pitches}."""
    date_str = game_date.isoformat()
    sql = f"""
    WITH deduped AS (
        SELECT pitcher_id, pitch_name, whiff, k_percent, percentage,
            ROW_NUMBER() OVER (
                PARTITION BY pitcher_id, pitch_name
                ORDER BY percentage DESC
            ) AS _rn
        FROM `{PROJECT}.{DATASET}.raw_pitch_log`
        WHERE run_date = '{date_str}' AND season = {game_date.year}
    )
    SELECT
        pitcher_id,
        ROUND(AVG(whiff), 1) AS arsenal_whiff_avg,
        COUNTIF(whiff >= 25) AS num_high_whiff_pitches
    FROM deduped
    WHERE _rn = 1 AND percentage >= 5
    GROUP BY pitcher_id
    """
    try:
        rows = list(client.query(sql).result())
        return {int(r.pitcher_id): {
            "arsenal_whiff_avg": round(_safe_float(r.arsenal_whiff_avg), 1),
            "num_high_whiff_pitches": int(r.num_high_whiff_pitches or 0),
        } for r in rows}
    except Exception as exc:
        log.warning("Could not load pregame pitch arsenal for %s: %s", date_str, exc)
        return {}


def _load_pregame_team_k_rankings(game_date):
    """Load team K vulnerability rankings.
    Returns dict: team_code → rank (int)."""
    date_str = game_date.isoformat()
    sql = f"""
    SELECT team_code, rank
    FROM `{PROJECT}.{DATASET}.raw_team_strikeout_rankings`
    WHERE run_date = '{date_str}'
      AND category = 'strikeouts'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY team_code ORDER BY ingested_at DESC) = 1
    """
    try:
        rows = list(client.query(sql).result())
        return {r.team_code: int(r.rank) for r in rows}
    except Exception as exc:
        log.warning("Could not load team K rankings for %s: %s", date_str, exc)
        return {}


def _load_our_k_picks(game_date):
    """Load our K model picks for a date.
    Returns dict: (game_pk, pitcher_id) → {score, grade, side, line}."""
    date_str = game_date.isoformat()
    sql = f"""
    SELECT game_pk, pitcher_id, score, grade, side, line
    FROM `{PROJECT}.{DATASET}.k_picks_daily`
    WHERE run_date = '{date_str}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY game_pk, pitcher_id ORDER BY score DESC
    ) = 1
    """
    try:
        rows = list(client.query(sql).result())
        return {(int(r.game_pk), int(r.pitcher_id)): {
            "score": float(r.score or 0),
            "grade": r.grade,
            "side": r.side,
            "line": float(r.line or 0),
        } for r in rows}
    except Exception as exc:
        log.warning("Could not load K picks for %s: %s", date_str, exc)
        return {}


def collect_k_league_outcomes(days=3):
    """
    League-wide K learning feeder.
    For each of the past N days, fetch ALL starting pitchers from ALL games,
    enrich with pre-game K stats, and store in k_league_outcomes.
    """
    for d in range(1, days + 1):
        game_date = TODAY - timedelta(days=d)
        log.info("Collecting league-wide K outcomes for %s", game_date)

        # Check if already collected
        try:
            check = list(client.query(f"""
                SELECT COUNT(*) AS n FROM `{K_LEAGUE_TABLE}`
                WHERE game_date = '{game_date.isoformat()}'
            """).result())
            if check and check[0].n > 0:
                log.info("Already collected %s K outcomes for %s — skipping",
                         check[0].n, game_date)
                continue
        except Exception:
            pass

        # 1. Fetch all starters + actual Ks
        game_rows = fetch_k_league_game_data(game_date)
        if not game_rows:
            continue

        # 2. Load pre-game stats
        pitcher_stats = _load_pregame_k_pitcher_stats(game_date)
        arsenal_stats = _load_pregame_pitch_arsenal(game_date)
        team_k_ranks = _load_pregame_team_k_rankings(game_date)
        our_picks = _load_our_k_picks(game_date)

        log.info("K pre-game data: %s pitchers, %s arsenals, %s team ranks, %s picks",
                 len(pitcher_stats), len(arsenal_stats), len(team_k_ranks), len(our_picks))

        # 3. Enrich and build output
        output_rows = []
        for row in game_rows:
            pid = row["pitcher_id"]
            opp = row["opp_team_code"]

            p_stats = pitcher_stats.get(pid, {})
            a_stats = arsenal_stats.get(pid, {})
            opp_k_rank = team_k_ranks.get(opp)

            pick = our_picks.get((row["game_pk"], pid))
            pick_line = pick["line"] if pick else None

            output_rows.append({
                "game_date": game_date.isoformat(),
                "game_pk": row["game_pk"],
                "pitcher_id": pid,
                "pitcher_name": row["pitcher_name"],
                "pitcher_hand": row["pitcher_hand"],
                "team_code": row["team_code"],
                "opp_team_code": opp,
                "k_per_9": None,  # not in raw_pitcher_matchup directly
                "k_pct": None,
                "strike_pct": None,
                "whip": p_stats.get("whip"),
                "arsenal_whiff_avg": a_stats.get("arsenal_whiff_avg"),
                "num_high_whiff_pitches": a_stats.get("num_high_whiff_pitches"),
                "opp_team_k_rank": opp_k_rank,
                "ip": row["ip"],
                "actual_k": row["actual_k"],
                "line": pick_line,
                "hit_over": (row["actual_k"] > pick_line) if pick_line else None,
                "hit_under": (row["actual_k"] < pick_line) if pick_line else None,
                "was_picked": pick is not None,
                "pulse_score": pick["score"] if pick else None,
                "grade": pick["grade"] if pick else None,
                "pick_side": pick["side"] if pick else None,
                "collected_at": NOW.isoformat(),
            })

        # 4. Insert
        if output_rows:
            errors = client.insert_rows_json(K_LEAGUE_TABLE, output_rows)
            if errors:
                log.error("BQ insert errors for k_league_outcomes: %s", errors[:3])
            else:
                picked = sum(1 for r in output_rows if r["was_picked"])
                log.info("Wrote %s K league outcomes for %s — %s were our picks",
                         len(output_rows), game_date, picked)


# ── 1. Results Backfill: Fetch actual Ks from MLB API ─────────────────────

def fetch_actual_strikeouts(game_date):
    """
    Fetch actual pitcher strikeout totals for all games on a given date.
    Returns dict: pitcher_id → actual_k_count.
    """
    date_str = game_date.isoformat()
    url = f"{MLB_API}/schedule?sportId=1&date={date_str}&hydrate=probablePitcher,boxscore"
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
            boxscore = game.get("boxscore") or {}
            teams = boxscore.get("teams", {})
            for side in ("away", "home"):
                pitchers = teams.get(side, {}).get("pitchers", [])
                players = teams.get(side, {}).get("players", {})
                for pid_str, player_data in players.items():
                    pid = int(pid_str.replace("ID", ""))
                    stats = player_data.get("stats", {}).get("pitching", {})
                    if stats:
                        results[pid] = int(stats.get("strikeOuts", 0))
    return results


def backfill_results(days=3):
    """
    For each of the past N days, fetch actual K totals and update k_picks_daily.
    """
    for d in range(1, days + 1):
        game_date = TODAY - timedelta(days=d)
        log.info("Backfilling K results for %s", game_date)

        actuals = fetch_actual_strikeouts(game_date)
        if not actuals:
            log.info("No completed games found for %s", game_date)
            continue

        # Update k_picks_daily with actual results
        for pitcher_id, actual_k in actuals.items():
            # Check if pick exists, then update
            update_sql = f"""
            UPDATE {TABLE}.k_picks_daily`
            SET actual_k = @actual_k,
                hit = CASE
                    WHEN side = 'OVER' AND @actual_k > line THEN TRUE
                    WHEN side = 'UNDER' AND @actual_k < line THEN TRUE
                    ELSE FALSE
                END
            WHERE run_date = @run_date
              AND pitcher_id = @pitcher_id
              AND actual_k IS NULL
            """
            params = [
                bigquery.ScalarQueryParameter("actual_k", "INT64", actual_k),
                bigquery.ScalarQueryParameter("run_date", "DATE", game_date.isoformat()),
                bigquery.ScalarQueryParameter("pitcher_id", "INT64", pitcher_id),
            ]
            try:
                client.query(
                    update_sql,
                    job_config=bigquery.QueryJobConfig(query_parameters=params),
                ).result()
            except Exception as exc:
                log.debug("Update failed for pitcher %s on %s: %s", pitcher_id, game_date, exc)

        log.info("Updated %s pitcher results for %s", len(actuals), game_date)


# ── 2. Back-Test: Grade Hit Rates ────────────────────────────────────────

def grade_hit_rates(days=30):
    """How often does each grade/side combo actually hit?"""
    sql = f"""
    SELECT
      grade,
      side,
      COUNT(*) AS total_picks,
      SUM(CASE WHEN hit THEN 1 ELSE 0 END) AS hits,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hit_rate_pct,
      ROUND(AVG(score), 1) AS avg_pulse,
      ROUND(AVG(best_price), 0) AS avg_odds,
      ROUND(AVG(actual_k), 1) AS avg_actual_k,
      ROUND(AVG(line), 1) AS avg_line
    FROM {TABLE}.k_picks_daily`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
      AND actual_k IS NOT NULL
    GROUP BY grade, side
    ORDER BY hit_rate_pct DESC
    """
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


# ── 3. Factor Correlation Analysis ───────────────────────────────────────

def factor_correlations(days=30):
    """
    Compute how each scoring factor correlates with actual hits.
    Returns a dict: factor_name → {correlation, hit_rate_high, hit_rate_low, sample_size}.
    """
    sql = f"""
    WITH picks AS (
      SELECT *
      FROM {TABLE}.k_picks_daily`
      WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
        AND actual_k IS NOT NULL
        AND hit IS NOT NULL
    )
    SELECT
      -- K/9 factor
      CORR(k_per_9, CAST(hit AS INT64)) AS corr_k9,
      CORR(k_pct, CAST(hit AS INT64)) AS corr_k_pct,
      CORR(arsenal_whiff_avg, CAST(hit AS INT64)) AS corr_whiff,
      CORR(num_high_whiff_pitches, CAST(hit AS INT64)) AS corr_high_whiff,
      CORR(pf_rating, CAST(hit AS INT64)) AS corr_pf_rating,
      CORR(avg_l10, CAST(hit AS INT64)) AS corr_avg_l10,
      CORR(strike_pct, CAST(hit AS INT64)) AS corr_strike_pct,
      CORR(game_total, CAST(hit AS INT64)) AS corr_vegas,
      COUNT(*) AS sample_size,
      -- Hit rates for high vs low K/9
      ROUND(SAFE_DIVIDE(
        SUM(CASE WHEN k_per_9 >= 9.0 AND hit THEN 1 ELSE 0 END),
        SUM(CASE WHEN k_per_9 >= 9.0 THEN 1 ELSE 0 END)
      ) * 100, 1) AS hit_rate_high_k9,
      ROUND(SAFE_DIVIDE(
        SUM(CASE WHEN k_per_9 < 7.5 AND hit THEN 1 ELSE 0 END),
        SUM(CASE WHEN k_per_9 < 7.5 THEN 1 ELSE 0 END)
      ) * 100, 1) AS hit_rate_low_k9,
      -- PF rating split
      ROUND(SAFE_DIVIDE(
        SUM(CASE WHEN pf_rating >= 3.5 AND hit THEN 1 ELSE 0 END),
        SUM(CASE WHEN pf_rating >= 3.5 THEN 1 ELSE 0 END)
      ) * 100, 1) AS hit_rate_high_pf,
      ROUND(SAFE_DIVIDE(
        SUM(CASE WHEN pf_rating < 2.5 AND hit THEN 1 ELSE 0 END),
        SUM(CASE WHEN pf_rating < 2.5 THEN 1 ELSE 0 END)
      ) * 100, 1) AS hit_rate_low_pf
    FROM picks
    WHERE side = 'OVER'
    """
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
    rows = list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    return rows[0] if rows else {}


# ── 4. Self-Learning Weight Calibration ──────────────────────────────────

def calibrate_weights(days=30):
    """
    The K learning engine. Adjusts weights based on factor correlations
    with actual K outcomes from LEAGUE-WIDE data (all starters, not just picks).

    Uses k_league_outcomes as primary data source for more samples.
    Falls back to k_picks_daily if league outcomes table has < MIN_SAMPLE_SIZE rows.
    """
    log.info("Starting K weight calibration with %s-day lookback", days)

    # Get current weights
    try:
        rows = list(client.query(f"""
            SELECT factor, weight
            FROM {TABLE}.k_model_weights`
            WHERE run_date = (SELECT MAX(run_date) FROM {TABLE}.k_model_weights`)
        """).result())
        current_weights = {r.factor: r.weight for r in rows} if rows else dict(BASELINE_WEIGHTS)
    except Exception:
        current_weights = dict(BASELINE_WEIGHTS)

    # Check league-wide data availability
    try:
        league_count_rows = list(client.query(f"""
            SELECT COUNT(*) AS n FROM `{K_LEAGUE_TABLE}`
            WHERE game_date >= DATE_SUB(@today, INTERVAL @days DAY)
              AND actual_k IS NOT NULL
        """, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
            bigquery.ScalarQueryParameter("days", "INT64", days),
        ])).result())
        league_count = league_count_rows[0].n if league_count_rows else 0
    except Exception:
        league_count = 0

    use_league = league_count >= MIN_SAMPLE_SIZE
    if use_league:
        log.info("Using league-wide K outcomes for calibration (%s rows)", league_count)
        # League table uses hit_over for OVER correlation
        # We correlate factors with actual_k directly (higher K = factor working)
        source_cte = f"""
        SELECT
          arsenal_whiff_avg, num_high_whiff_pitches, whip, opp_team_k_rank,
          actual_k, ip,
          -- For league data we correlate factors directly with actual K count
          actual_k AS outcome_val
        FROM `{K_LEAGUE_TABLE}`
        WHERE game_date >= DATE_SUB(@today, INTERVAL @days DAY)
          AND actual_k IS NOT NULL
        """
    else:
        log.info("League K data insufficient (%s rows) — using picks-only", league_count)
        source_cte = f"""
        SELECT
          k_per_9, k_pct, arsenal_whiff_avg, num_high_whiff_pitches,
          strike_pct, pf_rating, avg_l10, line, side, hit,
          actual_k, game_total,
          CAST(hit AS INT64) AS outcome_val
        FROM {TABLE}.k_picks_daily`
        WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
          AND actual_k IS NOT NULL
          AND hit IS NOT NULL
        """

    if use_league:
        sql = f"""
        WITH outcomes AS ({source_cte})
        SELECT 'arsenal_whiff' AS factor,
          SAFE_DIVIDE(SUM(CASE WHEN arsenal_whiff_avg >= 25 AND actual_k >= 6 THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN arsenal_whiff_avg >= 25 THEN 1 ELSE 0 END), 0)) AS high_hr,
          CORR(arsenal_whiff_avg, outcome_val) AS corr,
          COUNT(*) AS n
        FROM outcomes

        UNION ALL
        SELECT 'high_whiff_pitches',
          SAFE_DIVIDE(SUM(CASE WHEN num_high_whiff_pitches >= 2 AND actual_k >= 6 THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN num_high_whiff_pitches >= 2 THEN 1 ELSE 0 END), 0)),
          CORR(num_high_whiff_pitches, outcome_val), COUNT(*)
        FROM outcomes

        UNION ALL
        SELECT 'opp_k_rank',
          SAFE_DIVIDE(SUM(CASE WHEN opp_team_k_rank >= 20 AND actual_k >= 6 THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN opp_team_k_rank >= 20 THEN 1 ELSE 0 END), 0)),
          CORR(opp_team_k_rank, outcome_val), COUNT(*)
        FROM outcomes

        UNION ALL
        SELECT 'k_per_9', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'k_pct', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'strike_pct', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'pf_rating', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'hit_rate_l10', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'hit_rate_season', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'hit_rate_vs_team', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'avg_l10_vs_line', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'avg_vs_opp_vs_line', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'vegas_total', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'streak', NULL, NULL, COUNT(*) FROM outcomes
        """
    else:
        sql = f"""
        WITH outcomes AS ({source_cte}),
        overall AS (
          SELECT
            SAFE_DIVIDE(SUM(CASE WHEN hit THEN 1 ELSE 0 END), COUNT(*)) AS base_hit_rate,
            COUNT(*) AS total
          FROM outcomes
        )
        SELECT
          'k_per_9' AS factor,
          SAFE_DIVIDE(SUM(CASE WHEN k_per_9 >= 9.0 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN k_per_9 >= 9.0 THEN 1 ELSE 0 END), 0)) AS high_hr,
          CORR(k_per_9, CAST(hit AS INT64)) AS corr,
          COUNT(*) AS n
        FROM outcomes, overall

        UNION ALL
        SELECT 'k_pct',
          SAFE_DIVIDE(SUM(CASE WHEN k_pct >= 24 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN k_pct >= 24 THEN 1 ELSE 0 END), 0)),
          CORR(k_pct, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes, overall

        UNION ALL
        SELECT 'arsenal_whiff',
          SAFE_DIVIDE(SUM(CASE WHEN arsenal_whiff_avg >= 25 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN arsenal_whiff_avg >= 25 THEN 1 ELSE 0 END), 0)),
          CORR(arsenal_whiff_avg, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes, overall

        UNION ALL
        SELECT 'high_whiff_pitches',
          SAFE_DIVIDE(SUM(CASE WHEN num_high_whiff_pitches >= 2 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN num_high_whiff_pitches >= 2 THEN 1 ELSE 0 END), 0)),
          CORR(num_high_whiff_pitches, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes, overall

        UNION ALL
        SELECT 'pf_rating',
          SAFE_DIVIDE(SUM(CASE WHEN pf_rating >= 3.5 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN pf_rating >= 3.5 THEN 1 ELSE 0 END), 0)),
          CORR(pf_rating, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes, overall

        UNION ALL
        SELECT 'hit_rate_l10',
          NULL, NULL, COUNT(*)
        FROM outcomes, overall

        UNION ALL
        SELECT 'avg_l10_vs_line',
          SAFE_DIVIDE(SUM(CASE WHEN (avg_l10 - line) >= 0.5 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN (avg_l10 - line) >= 0.5 THEN 1 ELSE 0 END), 0)),
          CORR(avg_l10 - line, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes, overall
        WHERE side = 'OVER'

        UNION ALL
        SELECT 'strike_pct',
          SAFE_DIVIDE(SUM(CASE WHEN strike_pct >= 65 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN strike_pct >= 65 THEN 1 ELSE 0 END), 0)),
          CORR(strike_pct, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes, overall
        """
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]

    try:
        rows = list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    except Exception as exc:
        log.error("Factor analysis query failed: %s", exc)
        return

    sample_size = rows[0].n if rows else 0
    if sample_size < MIN_SAMPLE_SIZE:
        log.info("Only %s results — need %s before calibrating weights", sample_size, MIN_SAMPLE_SIZE)
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
        if corr is None:
            # No correlation data — keep current weight
            weight_rows.append({
                "run_date": TODAY.isoformat(),
                "factor": factor,
                "weight": round(new_weights[factor], 3),
                "sample_size": n,
                "correlation": None,
                "hit_rate_pct": round(high_hr * 100, 1) if high_hr else None,
                "updated_at": NOW.isoformat(),
            })
            continue

        # Adjustment: positive correlation → boost, negative → dampen
        # Scale by correlation magnitude
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
            "sample_size": n,
            "correlation": round(corr, 4) if corr is not None else None,
            "hit_rate_pct": round(high_hr * 100, 1) if high_hr else None,
            "updated_at": NOW.isoformat(),
        })

    # Also persist factors without correlation data (streaks, etc.)
    for factor, weight in new_weights.items():
        if not any(r["factor"] == factor for r in weight_rows):
            weight_rows.append({
                "run_date": TODAY.isoformat(),
                "factor": factor,
                "weight": round(weight, 3),
                "sample_size": sample_size,
                "correlation": None,
                "hit_rate_pct": None,
                "updated_at": NOW.isoformat(),
            })

    if weight_rows:
        errors = client.insert_rows_json(
            f"{PROJECT}.{DATASET}.k_model_weights", weight_rows
        )
        if errors:
            log.error("BQ insert errors for k_model_weights: %s", errors[:3])
        else:
            source = "league-wide" if use_league else "picks-only"
            log.info("Wrote %s calibrated K weights to k_model_weights (source: %s)",
                     len(weight_rows), source)


# ── 5. Score Calibration ─────────────────────────────────────────────────

def score_calibration(days=30):
    """Are K-Pulse scores well-calibrated? Group by bucket and check hit rates."""
    sql = f"""
    SELECT
      CASE
        WHEN score >= 80 THEN '80+ FIRE'
        WHEN score >= 65 THEN '65-79 STRONG'
        WHEN score >= 50 THEN '50-64 LEAN'
        ELSE 'Under 50 SKIP'
      END AS bucket,
      side,
      COUNT(*) AS total,
      SUM(CASE WHEN hit THEN 1 ELSE 0 END) AS hits,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hit_rate_pct,
      ROUND(AVG(best_price), 0) AS avg_odds,
      ROUND(AVG(actual_k - line), 2) AS avg_k_diff,
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
    FROM {TABLE}.k_picks_daily`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
      AND actual_k IS NOT NULL
    GROUP BY bucket, side
    ORDER BY bucket DESC, side
    """
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


# ── 6. Pitcher K Reliability Index ───────────────────────────────────────

def pitcher_reliability(days=60):
    """Which pitchers consistently hit their K prop lines?"""
    sql = f"""
    SELECT
      pitcher_name,
      side,
      COUNT(*) AS picks,
      SUM(CASE WHEN hit THEN 1 ELSE 0 END) AS hits,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN hit THEN 1 ELSE 0 END), COUNT(*)) * 100, 1) AS hit_rate_pct,
      ROUND(AVG(score), 1) AS avg_pulse,
      ROUND(AVG(actual_k), 1) AS avg_actual_k,
      ROUND(AVG(line), 1) AS avg_line,
      ROUND(AVG(actual_k - line), 2) AS avg_k_diff
    FROM {TABLE}.k_picks_daily`
    WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
      AND actual_k IS NOT NULL
    GROUP BY pitcher_name, side
    HAVING COUNT(*) >= 3
    ORDER BY hit_rate_pct DESC
    """
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


# ── Report ────────────────────────────────────────────────────────────────

def print_report(days=30):
    print(f"\n{'='*70}")
    print(f"K ANALYTICS REPORT \u2014 Last {days} days (as of {TODAY})")
    print(f"{'='*70}")

    print("\n\u26a1 GRADE HIT RATES")
    print(f"{'Grade':<10} {'Side':<7} {'Picks':>6} {'Hits':>5} {'Rate':>7} {'Avg K-Pulse':>12} {'Avg Odds':>9}")
    for r in grade_hit_rates(days):
        print(
            f"{r.grade:<10} {r.side:<7} {r.total_picks:>6} {r.hits:>5} "
            f"{r.hit_rate_pct:>6.1f}% {r.avg_pulse:>12.1f} {r.avg_odds:>+9.0f}"
        )

    print("\n\U0001f3af SCORE CALIBRATION")
    print(f"{'Bucket':<18} {'Side':<7} {'Picks':>6} {'Hits':>5} {'Rate':>7} {'K Diff':>8} {'ROI':>7}")
    for r in score_calibration(days):
        roi = f"{r.roi_pct:>+6.1f}%" if r.roi_pct is not None else "    N/A"
        diff = f"{r.avg_k_diff:>+7.2f}" if r.avg_k_diff is not None else "    N/A"
        print(f"{r.bucket:<18} {r.side:<7} {r.total:>6} {r.hits:>5} {r.hit_rate_pct:>6.1f}% {diff} {roi}")

    print("\n\U0001f4ca FACTOR CORRELATIONS (OVERS)")
    corr = factor_correlations(days)
    if corr:
        for key in ("corr_k9", "corr_k_pct", "corr_whiff", "corr_high_whiff",
                     "corr_pf_rating", "corr_avg_l10", "corr_strike_pct", "corr_vegas"):
            val = getattr(corr, key, None) if hasattr(corr, key) else corr.get(key)
            label = key.replace("corr_", "").upper()
            if val is not None:
                bar = "\u2588" * int(abs(val) * 20)
                sign = "+" if val > 0 else "-"
                print(f"  {label:<20} {sign}{abs(val):.3f} {bar}")

    print("\n\U0001f3c6 TOP RELIABLE PITCHERS (OVERS)")
    for r in pitcher_reliability(days):
        if r.side != "OVER" or r.picks < 3:
            continue
        hit_emoji = "\u2705" if r.hit_rate_pct >= 60 else "\u26a0\ufe0f" if r.hit_rate_pct >= 45 else "\u274c"
        print(
            f"  {hit_emoji} {r.pitcher_name:<22} {r.picks:>3} picks  "
            f"{r.hit_rate_pct:>5.1f}% hit  avg K:{r.avg_actual_k:.1f}  "
            f"line:{r.avg_line:.1f}  diff:{r.avg_k_diff:+.1f}"
        )


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    log.info("K Analytics starting...")

    log.info("Step 1: Backfilling K results (last 3 days)...")
    backfill_results(days=3)

    log.info("Step 2: Collecting league-wide K outcomes (last 3 days)...")
    collect_k_league_outcomes(days=3)

    log.info("Step 3: Calibrating weights...")
    calibrate_weights(days=30)

    log.info("Step 4: Generating report...")
    print_report(30)


if __name__ == "__main__":
    main()
