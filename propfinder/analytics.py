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


# ── 0a-2. League-Wide Outcome Collection ────────────────────────────────
# Fetch ALL batters from completed games — not just our picks — so the
# learning engine sees every HR that happened league-wide.

LEAGUE_TABLE = f"{PROJECT}.{DATASET}.hr_league_outcomes"


def _safe_float(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def fetch_league_game_data(game_date):
    """
    Fetch ALL batters + starting pitchers from every completed game on a date.
    Uses per-game live feed endpoint for full boxscore + player bio data.
    Returns list of dicts: {game_pk, batter_id, batter_name, bat_side,
                            pitcher_id, pitcher_name, pitcher_hand, actual_hr}
    """
    import time as _time
    date_str = game_date.isoformat()

    # Step 1: Get list of completed game PKs from schedule
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

    log.info("Fetching boxscores for %s completed games on %s", len(game_pks), date_str)

    # Step 2: Fetch each game's live feed for full boxscore + player bios
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

        # Player bio data (batSide, pitchHand) lives in gameData.players
        gd_players = feed.get("gameData", {}).get("players", {})
        bio = {}  # player_id → {bat_side, pitch_hand, name}
        for pid_str, pdata in gd_players.items():
            pid = int(pid_str.replace("ID", ""))
            bio[pid] = {
                "name": pdata.get("fullName", "Unknown"),
                "bat_side": pdata.get("batSide", {}).get("code", "R"),
                "pitch_hand": pdata.get("pitchHand", {}).get("code", "R"),
            }

        boxscore = feed.get("liveData", {}).get("boxscore", {})
        teams_box = boxscore.get("teams", {})

        # Determine starting pitcher for each side (first in pitchers list)
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

        # Each batter faces the OPPOSING team's starter
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
                    "actual_hr": int(batting.get("homeRuns", 0)),
                })

        _time.sleep(0.3)  # rate-limit MLB API calls

    log.info("Fetched %s batter appearances for %s (%s with HRs)",
             len(rows), date_str,
             sum(1 for r in rows if r["actual_hr"] > 0))
    return rows


def _load_pregame_batter_stats(game_date):
    """Load batter L15 stats from raw_hit_data snapshot on game_date.
    Returns dict: batter_id → {l15_ev, l15_barrel_pct, l15_hard_hit_pct}."""
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
        SELECT batter_id, launch_speed, is_barrel,
            ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY event_date DESC) AS ev_rank
        FROM deduped
        WHERE _rn = 1
    )
    SELECT
        batter_id,
        AVG(IF(ev_rank <= 15, launch_speed, NULL)) AS l15_ev,
        SAFE_DIVIDE(
            COUNTIF(ev_rank <= 15 AND is_barrel),
            COUNTIF(ev_rank <= 15)
        ) * 100 AS l15_barrel_pct,
        SAFE_DIVIDE(
            COUNTIF(ev_rank <= 15 AND launch_speed >= 95),
            COUNTIF(ev_rank <= 15)
        ) * 100 AS l15_hard_hit_pct
    FROM ranked
    GROUP BY batter_id
    """
    try:
        rows = list(client.query(sql).result())
        return {int(r.batter_id): {
            "l15_ev": round(float(r.l15_ev or 0), 1),
            "l15_barrel_pct": round(float(r.l15_barrel_pct or 0), 1),
            "l15_hard_hit_pct": round(float(r.l15_hard_hit_pct or 0), 1),
        } for r in rows}
    except Exception as exc:
        log.warning("Could not load pregame batter stats for %s: %s", date_str, exc)
        return {}


def _load_pregame_splits(game_date):
    """Load batter ISO/SLG splits from raw_splits snapshot on game_date.
    Returns dict: (batter_id, split) → {iso, slg}."""
    date_str = game_date.isoformat()
    sql = f"""
    SELECT batter_id, split_code, at_bats, doubles, triples, home_runs, slg
    FROM `{PROJECT}.{DATASET}.raw_splits`
    WHERE run_date = '{date_str}'
      AND split_code IN ('vl', 'vr')
    """
    try:
        rows = list(client.query(sql).result())
        result = {}
        for r in rows:
            ab = int(r.at_bats or 0)
            d = int(r.doubles or 0)
            t = int(r.triples or 0)
            hr = int(r.home_runs or 0)
            iso = (d + 2 * t + 3 * hr) / ab if ab > 0 else 0.0
            result[(int(r.batter_id), r.split_code)] = {
                "iso": round(iso, 3),
                "slg": round(float(r.slg or 0), 3),
            }
        return result
    except Exception as exc:
        log.warning("Could not load pregame splits for %s: %s", date_str, exc)
        return {}


def _load_pregame_pitcher_stats(game_date):
    """Load pitcher stats from raw_pitcher_matchup snapshot on game_date.
    Returns dict: (pitcher_id, split) → {p_hr9, p_hr_fb_pct, p_fb_pct, p_barrel_pct, p_hard_hit_pct}."""
    date_str = game_date.isoformat()
    sql = f"""
    SELECT pitcher_id, split,
        hr_per_9, hr_fb_pct, fb_pct, barrel_pct, hard_hit_pct, ip, home_runs
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
            p_hr9 = _safe_float(r.hr_per_9)
            p_barrel = _safe_float(r.barrel_pct)
            if p_barrel > 0 and p_barrel < 1:
                p_barrel *= 100
            p_fb = _safe_float(r.fb_pct)
            if p_fb > 0 and p_fb < 1:
                p_fb *= 100
            p_hh = _safe_float(r.hard_hit_pct)
            if p_hh > 0 and p_hh < 1:
                p_hh *= 100

            stored_hrfb = _safe_float(r.hr_fb_pct)
            if stored_hrfb > 0:
                p_hrfb = stored_hrfb
            else:
                ip = _safe_float(r.ip)
                hr_n = int(r.home_runs or 0)
                est_fb = ip * (p_fb / 100) * 1.2 if ip > 0 and p_fb > 0 else 0
                p_hrfb = min((hr_n / est_fb) * 100, 60.0) if est_fb > 0 else 0.0

            result[(int(r.pitcher_id), r.split)] = {
                "p_hr9": round(p_hr9, 2),
                "p_hr_fb_pct": round(p_hrfb, 1),
                "p_fb_pct": round(p_fb, 1),
                "p_barrel_pct": round(p_barrel, 1),
                "p_hard_hit_pct": round(p_hh, 1),
            }
        return result
    except Exception as exc:
        log.warning("Could not load pregame pitcher stats for %s: %s", date_str, exc)
        return {}


def _load_our_picks(game_date):
    """Load our model's picks for a date so we can mark was_picked + pulse_score.
    Returns dict: (game_pk, batter_id) → {score, grade}."""
    date_str = game_date.isoformat()
    sql = f"""
    SELECT game_pk, batter_id, score, grade
    FROM `{PROJECT}.{DATASET}.hr_picks_daily`
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
        log.warning("Could not load picks for %s: %s", date_str, exc)
        return {}


def collect_league_outcomes(days=3):
    """
    The league-wide learning engine feeder.
    For each of the past N days, fetch ALL batters from ALL completed games,
    enrich with pre-game stats from our raw tables, and store in hr_league_outcomes.
    This gives the weight calibration 10x the data compared to just our picks.
    """
    for d in range(1, days + 1):
        game_date = TODAY - timedelta(days=d)
        log.info("Collecting league-wide outcomes for %s", game_date)

        # Check if we already collected this date
        try:
            check = list(client.query(f"""
                SELECT COUNT(*) AS n FROM `{LEAGUE_TABLE}`
                WHERE game_date = '{game_date.isoformat()}'
            """).result())
            if check and check[0].n > 0:
                log.info("Already collected %s outcomes for %s — skipping",
                         check[0].n, game_date)
                continue
        except Exception:
            pass  # table might not exist yet

        # 1. Fetch all batters + HRs from MLB boxscores
        game_rows = fetch_league_game_data(game_date)
        if not game_rows:
            log.info("No completed games for %s", game_date)
            continue

        # 2. Load pre-game stat snapshots
        batter_stats = _load_pregame_batter_stats(game_date)
        splits_map = _load_pregame_splits(game_date)
        pitcher_stats = _load_pregame_pitcher_stats(game_date)
        our_picks = _load_our_picks(game_date)

        log.info("Pre-game data: %s batters, %s splits, %s pitchers, %s picks",
                 len(batter_stats), len(splits_map), len(pitcher_stats), len(our_picks))

        # 3. Enrich and build output rows
        output_rows = []
        for row in game_rows:
            bid = row["batter_id"]
            pid = row["pitcher_id"]
            bat_side = row["bat_side"]
            pitcher_hand = row["pitcher_hand"]

            # Batter L15 stats
            b_stats = batter_stats.get(bid, {})

            # Batter ISO/SLG vs this pitcher's hand
            split_key = "vl" if pitcher_hand == "L" else "vr"
            b_splits = splits_map.get((bid, split_key), {})

            # Pitcher stats vs this batter's hand
            p_split_key = "vsLHB" if bat_side == "L" else "vsRHB"
            p_stats = pitcher_stats.get((pid, p_split_key), {})
            if not p_stats:
                p_stats = pitcher_stats.get((pid, "Season"), {})

            # Our pick info
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
                "iso": b_splits.get("iso"),
                "slg": b_splits.get("slg"),
                "l15_ev": b_stats.get("l15_ev"),
                "l15_barrel_pct": b_stats.get("l15_barrel_pct"),
                "l15_hard_hit_pct": b_stats.get("l15_hard_hit_pct"),
                "p_hr9_vs_hand": p_stats.get("p_hr9"),
                "p_hr_fb_pct": p_stats.get("p_hr_fb_pct"),
                "p_fb_pct": p_stats.get("p_fb_pct"),
                "p_barrel_pct": p_stats.get("p_barrel_pct"),
                "p_hard_hit_pct": p_stats.get("p_hard_hit_pct"),
                "actual_hr": row["actual_hr"],
                "hit": row["actual_hr"] >= 1,
                "was_picked": pick is not None,
                "pulse_score": pick["score"] if pick else None,
                "grade": pick["grade"] if pick else None,
                "collected_at": NOW.isoformat(),
            })

        # 4. Insert into BigQuery
        if output_rows:
            errors = client.insert_rows_json(LEAGUE_TABLE, output_rows)
            if errors:
                log.error("BQ insert errors for hr_league_outcomes: %s", errors[:3])
            else:
                hr_count = sum(1 for r in output_rows if r["hit"])
                picked_count = sum(1 for r in output_rows if r["was_picked"])
                log.info(
                    "Wrote %s league outcomes for %s — %s HRs, %s were our picks",
                    len(output_rows), game_date, hr_count, picked_count,
                )


# ── 0b. Self-Learning Weight Calibration ─────────────────────────────────

def calibrate_hr_weights(days=30):
    """
    The HR learning engine. Adjusts factor weights based on correlation
    with actual HR outcomes from LEAGUE-WIDE data (all batters, not just picks).

    Uses hr_league_outcomes as the primary data source for 10x more samples.
    Falls back to hr_picks_daily if league outcomes table is empty.

    Strategy:
    - Factors with positive correlation to HR hits → boost weight
    - Factors with negative/zero correlation → dampen weight
    - Apply learning rate to avoid wild swings
    - Enforce floor/ceiling to keep all factors in play
    """
    log.info("Starting HR weight calibration with %s-day lookback (league-wide)", days)

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

    # Try league-wide data first, fall back to picks-only
    league_count = 0
    try:
        check = list(client.query(f"""
            SELECT COUNT(*) AS n FROM `{LEAGUE_TABLE}`
            WHERE game_date >= DATE_SUB(@today, INTERVAL @days DAY)
        """, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
            bigquery.ScalarQueryParameter("days", "INT64", days),
        ])).result())
        league_count = check[0].n if check else 0
    except Exception:
        pass

    use_league = league_count >= MIN_SAMPLE_SIZE
    if use_league:
        source_name = "league-wide"
        log.info("Using league-wide outcomes for calibration (%s rows)", league_count)
    else:
        source_name = "picks-only (league data not yet available)"
        log.info("League data insufficient (%s rows) — falling back to picks-only", league_count)

    # Compute per-factor correlation with actual HR outcomes
    # hr_league_outcomes uses game_date; hr_picks_daily uses run_date
    if use_league:
        source_cte = f"""
        SELECT * FROM `{LEAGUE_TABLE}`
        WHERE game_date >= DATE_SUB(@today, INTERVAL @days DAY)
          AND actual_hr IS NOT NULL AND hit IS NOT NULL
        """
    else:
        source_cte = f"""
        SELECT * FROM {TABLE}.hr_picks_daily`
        WHERE run_date >= DATE_SUB(@today, INTERVAL @days DAY)
          AND actual_hr IS NOT NULL AND hit IS NOT NULL
        """

    sql = f"""
    WITH outcomes AS (
      {source_cte}
    ),
    overall AS (
      SELECT
        SAFE_DIVIDE(SUM(CASE WHEN hit THEN 1 ELSE 0 END), COUNT(*)) AS base_hit_rate,
        COUNT(*) AS total
      FROM outcomes
    )
    -- Pitcher HR/9 vs hand
    SELECT 'p_hr9_vs_hand' AS factor,
      SAFE_DIVIDE(SUM(CASE WHEN p_hr9_vs_hand >= 1.8 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_hr9_vs_hand >= 1.8 THEN 1 ELSE 0 END), 0)) AS high_hr,
      CORR(p_hr9_vs_hand, CAST(hit AS INT64)) AS corr,
      COUNT(*) AS n
    FROM outcomes, overall

    UNION ALL
    SELECT 'p_hr_fb_pct',
      SAFE_DIVIDE(SUM(CASE WHEN p_hr_fb_pct >= 15 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_hr_fb_pct >= 15 THEN 1 ELSE 0 END), 0)),
      CORR(p_hr_fb_pct, CAST(hit AS INT64)), COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'p_fb_pct',
      SAFE_DIVIDE(SUM(CASE WHEN p_fb_pct >= 40 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_fb_pct >= 40 THEN 1 ELSE 0 END), 0)),
      CORR(p_fb_pct, CAST(hit AS INT64)), COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'p_barrel_pct',
      SAFE_DIVIDE(SUM(CASE WHEN p_barrel_pct >= 10 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_barrel_pct >= 10 THEN 1 ELSE 0 END), 0)),
      CORR(p_barrel_pct, CAST(hit AS INT64)), COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'p_hard_hit_pct',
      SAFE_DIVIDE(SUM(CASE WHEN p_hard_hit_pct >= 40 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN p_hard_hit_pct >= 40 THEN 1 ELSE 0 END), 0)),
      CORR(p_hard_hit_pct, CAST(hit AS INT64)), COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'b_iso',
      SAFE_DIVIDE(SUM(CASE WHEN iso >= 0.300 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN iso >= 0.300 THEN 1 ELSE 0 END), 0)),
      CORR(iso, CAST(hit AS INT64)), COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'b_slg',
      SAFE_DIVIDE(SUM(CASE WHEN slg >= 0.500 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN slg >= 0.500 THEN 1 ELSE 0 END), 0)),
      CORR(slg, CAST(hit AS INT64)), COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'b_ev',
      SAFE_DIVIDE(SUM(CASE WHEN l15_ev >= 92 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN l15_ev >= 92 THEN 1 ELSE 0 END), 0)),
      CORR(l15_ev, CAST(hit AS INT64)), COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'b_barrel',
      SAFE_DIVIDE(SUM(CASE WHEN l15_barrel_pct >= 20 AND hit THEN 1 ELSE 0 END),
                  NULLIF(SUM(CASE WHEN l15_barrel_pct >= 20 THEN 1 ELSE 0 END), 0)),
      CORR(l15_barrel_pct, CAST(hit AS INT64)), COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'p_iso_allowed', NULL, NULL, COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'platoon', NULL, NULL, COUNT(*)
    FROM outcomes, overall

    UNION ALL
    SELECT 'hot_form', NULL, NULL, COUNT(*)
    FROM outcomes, overall
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
            log.info("Wrote %s calibrated HR weights to hr_model_weights (source: %s)",
                     len(weight_rows), source_name)


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
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
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
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
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
    params = [
        bigquery.ScalarQueryParameter("today", "DATE", TODAY.isoformat()),
        bigquery.ScalarQueryParameter("days", "INT64", days),
    ]
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

    log.info("Step 2: Collecting league-wide outcomes (last 3 days)...")
    collect_league_outcomes(days=3)

    log.info("Step 3: Calibrating HR weights (league-wide)...")
    calibrate_hr_weights(days=30)

    log.info("Step 4: Setting up analytics views...")
    setup_all()

    log.info("Step 5: Generating report...")
    print_report(30)


if __name__ == "__main__":
    main()
