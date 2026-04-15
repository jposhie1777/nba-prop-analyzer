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

HIT_LEAGUE_TABLE = f"{PROJECT}.{DATASET}.hit_league_outcomes"


def _safe_float(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


# ── 0a. League-Wide Hit Outcome Collection ──────────────────────────────

def fetch_hit_league_game_data(game_date):
    """
    Fetch ALL batters + starting pitchers from every completed game on a date.
    Returns list of dicts with batter/pitcher info and actual_hits.
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

    log.info("Fetching hit data for %s completed games on %s", len(game_pks), date_str)

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
                "bat_side": pdata.get("batSide", {}).get("code", "R"),
                "pitch_hand": pdata.get("pitchHand", {}).get("code", "R"),
            }

        boxscore = feed.get("liveData", {}).get("boxscore", {})
        teams_box = boxscore.get("teams", {})

        # Starting pitcher for each side
        starters = {}
        for side in ("away", "home"):
            pitcher_ids = teams_box.get(side, {}).get("pitchers", [])
            if pitcher_ids:
                sp_id = pitcher_ids[0]
                sp_bio = bio.get(sp_id, {})
                starters[side] = {
                    "id": sp_id,
                    "name": sp_bio.get("name", "Unknown"),
                    "hand": sp_bio.get("pitch_hand", "R"),
                }

        if not starters.get("away") or not starters.get("home"):
            log.debug("Skipping game %s — missing starter info", gpk)
            continue

        opp_starter = {"away": starters["home"], "home": starters["away"]}

        for side in ("away", "home"):
            sp = opp_starter[side]
            players = teams_box.get(side, {}).get("players", {})
            for pid_str, player_data in players.items():
                pid = int(pid_str.replace("ID", ""))
                batting = player_data.get("stats", {}).get("batting", {})
                ab = int(batting.get("atBats", 0))
                if ab == 0:
                    continue

                p_bio = bio.get(pid, {})
                rows.append({
                    "game_pk": gpk,
                    "batter_id": pid,
                    "batter_name": p_bio.get("name", "Unknown"),
                    "bat_side": p_bio.get("bat_side", "R"),
                    "pitcher_id": sp["id"],
                    "pitcher_name": sp["name"],
                    "pitcher_hand": sp["hand"],
                    "actual_hits": int(batting.get("hits", 0)),
                })

        _time.sleep(0.3)

    log.info("Fetched %s batter appearances for %s (%s with 1+ hit)",
             len(rows), date_str,
             sum(1 for r in rows if r["actual_hits"] >= 1))
    return rows


def _load_pregame_hit_batter_stats(game_date):
    """Load batter contact stats from raw_hit_data snapshot.
    Returns dict: batter_id → {hard_hit_pct, line_drive_pct}."""
    date_str = game_date.isoformat()
    sql = f"""
    WITH deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY batter_id, event_date, pitch_type,
                    CAST(launch_speed AS STRING), CAST(launch_angle AS STRING)
                ORDER BY ingested_at DESC
            ) AS _rn
        FROM `{PROJECT}.{DATASET}.raw_hit_data`
        WHERE run_date = '{date_str}'
    ),
    ranked AS (
        SELECT batter_id, launch_speed, launch_angle,
            ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY event_date DESC) AS ev_rank
        FROM deduped
        WHERE _rn = 1
    )
    SELECT
        batter_id,
        SAFE_DIVIDE(
            COUNTIF(ev_rank <= 30 AND launch_speed >= 95),
            COUNTIF(ev_rank <= 30)
        ) * 100 AS hard_hit_pct,
        SAFE_DIVIDE(
            COUNTIF(ev_rank <= 30 AND launch_angle BETWEEN 10 AND 25),
            COUNTIF(ev_rank <= 30)
        ) * 100 AS line_drive_pct
    FROM ranked
    GROUP BY batter_id
    """
    try:
        rows = list(client.query(sql).result())
        return {int(r.batter_id): {
            "hard_hit_pct": round(_safe_float(r.hard_hit_pct), 1),
            "line_drive_pct": round(_safe_float(r.line_drive_pct), 1),
        } for r in rows}
    except Exception as exc:
        log.warning("Could not load pregame hit batter stats for %s: %s", date_str, exc)
        return {}


def _load_pregame_hit_splits(game_date):
    """Load batter AVG/contact from raw_splits.
    Returns dict: (batter_id, split_code) → {batting_avg, contact_rate, l15_hit_rate}."""
    date_str = game_date.isoformat()
    sql = f"""
    SELECT batter_id, split_code, at_bats, hits, strikeouts
    FROM `{PROJECT}.{DATASET}.raw_splits`
    WHERE run_date = '{date_str}'
      AND split_code IN ('vl', 'vr')
    """
    try:
        rows = list(client.query(sql).result())
        result = {}
        for r in rows:
            ab = int(r.at_bats or 0)
            h = int(r.hits or 0)
            k = int(r.strikeouts or 0)
            avg = h / ab if ab > 0 else 0.0
            contact = ((ab - k) / ab * 100) if ab > 0 else 0.0
            result[(int(r.batter_id), r.split_code)] = {
                "batting_avg": round(avg, 3),
                "contact_rate": round(contact, 1),
            }
        return result
    except Exception as exc:
        log.warning("Could not load pregame hit splits for %s: %s", date_str, exc)
        return {}


def _load_pregame_hit_pitcher_stats(game_date):
    """Load pitcher vulnerability stats from raw_pitcher_matchup.
    Returns dict: (pitcher_id, split) → {whip, k_rate, woba, hard_hit_pct, hits_per_9}."""
    date_str = game_date.isoformat()
    sql = f"""
    SELECT pitcher_id, split, whip, woba, hard_hit_pct, ip, home_runs
    FROM `{PROJECT}.{DATASET}.raw_pitcher_matchup`
    WHERE run_date = '{date_str}'
      AND split IN ('vsLHB', 'vsRHB', 'Season')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pitcher_id, split
        ORDER BY ingested_at DESC
    ) = 1
    """
    try:
        rows = list(client.query(sql).result())
        result = {}
        for r in rows:
            p_whip = _safe_float(r.whip)
            p_woba = _safe_float(r.woba)
            p_hh = _safe_float(r.hard_hit_pct)
            if p_hh > 0 and p_hh < 1:
                p_hh *= 100
            ip = _safe_float(r.ip)

            result[(int(r.pitcher_id), r.split)] = {
                "p_whip": round(p_whip, 3),
                "p_woba_allowed": round(p_woba, 3),
                "p_hard_hit_allowed": round(p_hh, 1),
                "p_hits_per_9": round((p_whip * 9 * 0.7), 1) if p_whip > 0 else None,
            }
        return result
    except Exception as exc:
        log.warning("Could not load pregame hit pitcher stats for %s: %s", date_str, exc)
        return {}


def _load_our_hit_picks(game_date):
    """Load our hit model picks for a date.
    Returns dict: (game_pk, batter_id) → {score, grade}."""
    date_str = game_date.isoformat()
    sql = f"""
    SELECT game_pk, batter_id, score, grade
    FROM `{PROJECT}.{DATASET}.hit_picks_daily`
    WHERE run_date = '{date_str}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY game_pk, batter_id ORDER BY score DESC
    ) = 1
    """
    try:
        rows = list(client.query(sql).result())
        return {(int(r.game_pk), int(r.batter_id)): {
            "score": float(r.score or 0),
            "grade": r.grade,
        } for r in rows}
    except Exception as exc:
        log.warning("Could not load hit picks for %s: %s", date_str, exc)
        return {}


def collect_hit_league_outcomes(days=3):
    """
    League-wide hit learning feeder.
    For each of the past N days, fetch ALL batters from ALL games,
    enrich with pre-game contact/pitcher stats, and store in hit_league_outcomes.
    """
    for d in range(1, days + 1):
        game_date = TODAY - timedelta(days=d)
        log.info("Collecting league-wide hit outcomes for %s", game_date)

        # Check if already collected
        try:
            check = list(client.query(f"""
                SELECT COUNT(*) AS n FROM `{HIT_LEAGUE_TABLE}`
                WHERE game_date = '{game_date.isoformat()}'
            """).result())
            if check and check[0].n > 0:
                log.info("Already collected %s hit outcomes for %s — skipping",
                         check[0].n, game_date)
                continue
        except Exception:
            pass

        # 1. Fetch all batters + hits from MLB boxscores
        game_rows = fetch_hit_league_game_data(game_date)
        if not game_rows:
            continue

        # 2. Load pre-game stats
        batter_stats = _load_pregame_hit_batter_stats(game_date)
        splits_map = _load_pregame_hit_splits(game_date)
        pitcher_stats = _load_pregame_hit_pitcher_stats(game_date)
        our_picks = _load_our_hit_picks(game_date)

        log.info("Hit pre-game data: %s batters, %s splits, %s pitchers, %s picks",
                 len(batter_stats), len(splits_map), len(pitcher_stats), len(our_picks))

        # 3. Enrich and build output
        output_rows = []
        for row in game_rows:
            bid = row["batter_id"]
            pid = row["pitcher_id"]
            bat_side = row["bat_side"]
            pitcher_hand = row["pitcher_hand"]

            b_stats = batter_stats.get(bid, {})

            # Splits vs pitcher hand
            split_key = "vl" if pitcher_hand == "L" else "vr"
            b_splits = splits_map.get((bid, split_key), {})

            # Pitcher stats vs batter hand
            p_split_key = "vsLHB" if bat_side == "L" else "vsRHB"
            p_stats = pitcher_stats.get((pid, p_split_key), {})
            if not p_stats:
                p_stats = pitcher_stats.get((pid, "Season"), {})

            pick = our_picks.get((row["game_pk"], bid))

            output_rows.append({
                "game_date": game_date.isoformat(),
                "game_pk": row["game_pk"],
                "batter_id": bid,
                "batter_name": row["batter_name"],
                "bat_side": bat_side,
                "pitcher_id": pid,
                "pitcher_name": row["pitcher_name"],
                "pitcher_hand": pitcher_hand,
                "batting_avg_vs_hand": b_splits.get("batting_avg"),
                "contact_rate": b_splits.get("contact_rate"),
                "l15_hit_rate": None,  # would need more complex calc
                "hard_hit_pct": b_stats.get("hard_hit_pct"),
                "line_drive_pct": b_stats.get("line_drive_pct"),
                "p_whip": p_stats.get("p_whip"),
                "p_k_rate": None,  # not directly in raw_pitcher_matchup
                "p_woba_allowed": p_stats.get("p_woba_allowed"),
                "p_hard_hit_allowed": p_stats.get("p_hard_hit_allowed"),
                "p_hits_per_9": p_stats.get("p_hits_per_9"),
                "actual_hits": row["actual_hits"],
                "hit": row["actual_hits"] >= 1,
                "was_picked": pick is not None,
                "pulse_score": pick["score"] if pick else None,
                "grade": pick["grade"] if pick else None,
                "collected_at": NOW.isoformat(),
            })

        # 4. Insert
        if output_rows:
            errors = client.insert_rows_json(HIT_LEAGUE_TABLE, output_rows)
            if errors:
                log.error("BQ insert errors for hit_league_outcomes: %s", errors[:3])
            else:
                hit_count = sum(1 for r in output_rows if r["hit"])
                picked = sum(1 for r in output_rows if r["was_picked"])
                log.info(
                    "Wrote %s hit league outcomes for %s — %s with 1+ hit, %s were our picks",
                    len(output_rows), game_date, hit_count, picked,
                )


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
    with actual 1+ hit outcomes from LEAGUE-WIDE data (all batters, not just picks).

    Uses hit_league_outcomes as primary data source for more samples.
    Falls back to hit_picks_daily if league table has < MIN_SAMPLE_SIZE rows.
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

    # Check league-wide data availability
    try:
        league_count_rows = list(client.query(f"""
            SELECT COUNT(*) AS n FROM `{HIT_LEAGUE_TABLE}`
            WHERE game_date >= DATE_SUB(@today, INTERVAL @days DAY)
              AND actual_hits IS NOT NULL AND hit IS NOT NULL
        """, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
            bigquery.ScalarQueryParameter("days", "INT64", days),
        ])).result())
        league_count = league_count_rows[0].n if league_count_rows else 0
    except Exception:
        league_count = 0

    use_league = league_count >= MIN_SAMPLE_SIZE

    if use_league:
        log.info("Using league-wide hit outcomes for calibration (%s rows)", league_count)
        sql = f"""
        WITH outcomes AS (
          SELECT * FROM `{HIT_LEAGUE_TABLE}`
          WHERE game_date >= DATE_SUB(@today, INTERVAL @days DAY)
            AND actual_hits IS NOT NULL AND hit IS NOT NULL
        )
        SELECT 'batting_avg' AS factor,
          SAFE_DIVIDE(SUM(CASE WHEN batting_avg_vs_hand >= 0.300 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN batting_avg_vs_hand >= 0.300 THEN 1 ELSE 0 END), 0)) AS high_hr,
          CORR(batting_avg_vs_hand, CAST(hit AS INT64)) AS corr,
          COUNT(*) AS n
        FROM outcomes

        UNION ALL
        SELECT 'contact_rate',
          SAFE_DIVIDE(SUM(CASE WHEN contact_rate >= 82 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN contact_rate >= 82 THEN 1 ELSE 0 END), 0)),
          CORR(contact_rate, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes

        UNION ALL
        SELECT 'hard_hit_pct',
          SAFE_DIVIDE(SUM(CASE WHEN hard_hit_pct >= 45 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN hard_hit_pct >= 45 THEN 1 ELSE 0 END), 0)),
          CORR(hard_hit_pct, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes

        UNION ALL
        SELECT 'p_whip',
          SAFE_DIVIDE(SUM(CASE WHEN p_whip >= 1.30 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN p_whip >= 1.30 THEN 1 ELSE 0 END), 0)),
          CORR(p_whip, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes

        UNION ALL
        SELECT 'p_woba_allowed',
          SAFE_DIVIDE(SUM(CASE WHEN p_woba_allowed >= 0.320 AND hit THEN 1 ELSE 0 END),
                      NULLIF(SUM(CASE WHEN p_woba_allowed >= 0.320 THEN 1 ELSE 0 END), 0)),
          CORR(p_woba_allowed, CAST(hit AS INT64)), COUNT(*)
        FROM outcomes

        UNION ALL
        SELECT 'p_k_rate_inv', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'l15_hit_rate', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'pf_rating', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'bvp_history', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'platoon_edge', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'arsenal_contact', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'babip_proxy', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'hit_rate_l10', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'hit_rate_season', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'hit_rate_vs_team', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'vegas_total', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'avg_l10_vs_line', NULL, NULL, COUNT(*) FROM outcomes
        UNION ALL
        SELECT 'streak', NULL, NULL, COUNT(*) FROM outcomes
        """
    else:
        log.info("League hit data insufficient (%s rows) — using picks-only", league_count)
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
            source = "league-wide" if use_league else "picks-only"
            log.info("Wrote %s calibrated hit weights (source: %s)",
                     len(weight_rows), source)


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
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
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
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
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

    log.info("Step 2: Collecting league-wide hit outcomes (last 3 days)...")
    collect_hit_league_outcomes(days=3)

    log.info("Step 3: Calibrating hit weights...")
    calibrate_weights(days=30)

    log.info("Step 4: Generating report...")
    print_report(30)


if __name__ == "__main__":
    main()
