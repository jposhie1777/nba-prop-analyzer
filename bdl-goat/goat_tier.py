# ======================================================
# GOAT NBA UNIFIED INGESTION SERVICE
# ======================================================

import os
import time
import json
import requests
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# ======================================================
# CONFIG
# ======================================================
PROJECT_ID = os.getenv("PROJECT_ID", "graphite-flare-477419-h7")
DATASET = os.getenv("GOAT_DATASET", "nba_goat_data")

TABLE_STATE = "ingest_state"
TABLE_ACTIVE_PLAYERS = "active_players"
TABLE_GAME_STATS_FULL = "player_game_stats_full"
TABLE_GAME_STATS_PERIOD = "player_game_stats_period"
TABLE_LINEUPS = "game_lineups"
TABLE_PLAYER_PROPS = "player_prop_odds"
TABLE_PLAYER_PROPS_STAGING = "player_prop_odds_staging"
TABLE_GAME_STATS_ADVANCED = "player_game_stats_advanced"
TABLE_GAME_STATS_ADVANCED_STAGING = "player_game_stats_advanced_staging"

BALDONTLIE_STATS_BASE = "https://api.balldontlie.io/v1"
BALDONTLIE_NBA_BASE = "https://api.balldontlie.io/v1"
BALDONTLIE_ODDS_BASE = "https://api.balldontlie.io/v2"

API_KEY = os.getenv("BALDONTLIE_KEY", "")
if not API_KEY:
    print("⚠️ BALDONTLIE_KEY missing")

HEADERS = {"Authorization": API_KEY}

RATE_PROFILE = os.getenv("BALLDONTLIE_TIER", "GOAT").upper()
RATE_LIMITS = {
    "ALL_STAR": {"batch": 5, "delay": 1.2, "retry": 10},
    "GOAT": {"batch": 20, "delay": 0.3, "retry": 3},
}
RATE = RATE_LIMITS.get(RATE_PROFILE, RATE_LIMITS["ALL_STAR"])

THROTTLES = {
    "active_players": 3600,
    "stats_full": 600,
    "stats_period": 900,
    "lineups": 120,
    "props": 120,
}
THROTTLES["box_scores"] = 600

TABLE_GAMES = "games"

THROTTLES["games"] = 120

# ======================================================
# APP
# ======================================================
app = Flask(__name__)
bq = bigquery.Client(project=PROJECT_ID)

# ======================================================
# HELPERS
# ======================================================
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def sleep_s(sec: float):
    time.sleep(max(sec, 0))

def table(name: str) -> str:
    return f"{PROJECT_ID}.{DATASET}.{name}"

def http_get(base: str, path: str, params=None):
    url = f"{base}{path}"

    headers = {}
    if "api.balldontlie.io" in base:
        headers["Authorization"] = API_KEY

    r = requests.get(
        url,
        headers=headers,
        params=params or {},
        timeout=25,
    )

    # Rate limit
    if r.status_code == 429:
        sleep_s(RATE["retry"])
        return http_get(base, path, params)

    # Hard failure
    if not r.ok:
        raise RuntimeError(
            f"HTTP {r.status_code} from {r.url}\n{r.text[:500]}"
        )

    # 🔑 CRITICAL FIX: Check content type
    content_type = r.headers.get("Content-Type", "")
    if "application/json" not in content_type.lower():
        raise RuntimeError(
            f"NON-JSON RESPONSE from {r.url}\n"
            f"Content-Type: {content_type}\n"
            f"Body:\n{r.text[:500]}"
        )

    return r.json()

from datetime import timedelta
import pytz

def yesterday_ny():
    ny = pytz.timezone("America/New_York")
    return (datetime.now(ny).date() - timedelta(days=1)).isoformat()


def merge_stats_advanced():
    bq.query(
        f"""
        MERGE `{table(TABLE_GAME_STATS_ADVANCED)}` t
        USING `{table(TABLE_GAME_STATS_ADVANCED_STAGING)}` s
        ON
          t.game_id = s.game_id
          AND t.player_id = s.player_id
        WHEN NOT MATCHED THEN
          INSERT ROW
        """
    ).result()

def merge_team_box_scores():
    bq.query(
        f"""
        MERGE `{table(TABLE_TEAM_BOX)}` t
        USING `{table(TABLE_TEAM_BOX_STAGING)}` s
        ON
          t.game_id = s.game_id
          AND t.team_id = s.team_id
        WHEN NOT MATCHED THEN
          INSERT ROW
        """
    ).result()


def paginate(base: str, path: str, params: Dict[str, Any]):
    out, cursor = [], None
    while True:
        p = dict(params)
        if cursor:
            p["cursor"] = cursor
        data = http_get(base, path, p)
        out.extend(data.get("data", []))
        cursor = (data.get("meta") or {}).get("next_cursor")
        if not cursor:
            break
        sleep_s(RATE["delay"])
    return out

from datetime import timedelta

def daterange(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

def bq_overwrite(name: str, rows: list):
    if rows:
        bq.load_table_from_json(
            rows,
            table(name),
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                source_format="NEWLINE_DELIMITED_JSON",
            ),
        ).result()


def swap_tables(final_table: str, staging_table: str):
    bq.query(
        f"""
        CREATE OR REPLACE TABLE `{table(final_table)}` AS
        SELECT * FROM `{table(staging_table)}`
        """
    ).result()

    bq.query(
        f"TRUNCATE TABLE `{table(staging_table)}`"
    ).result()




# ======================================================
# STATE / THROTTLE
# ======================================================
def ensure_state():
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table(TABLE_STATE)}`
    (
      job_name STRING NOT NULL,
      last_run_ts TIMESTAMP,
      meta STRING
    )
    """
    bq.query(sql).result()

def throttle(job_name: str):
    ensure_state()

    q = f"""
    SELECT last_run_ts
    FROM `{table(TABLE_STATE)}`
    WHERE job_name = @job_name
    LIMIT 1
    """

    rows = list(
        bq.query(
            q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_name", "STRING", job_name)
                ]
            ),
        ).result()
    )

    if not rows or rows[0]["last_run_ts"] is None:
        return True

    age = (datetime.now(timezone.utc) - rows[0]["last_run_ts"].replace(tzinfo=timezone.utc)).total_seconds()
    return age >= THROTTLES[job_name]

def mark_run(job_name: str, meta: dict):
    ensure_state()

    bq.query(
        f"""
        MERGE `{table(TABLE_STATE)}` t
        USING (
          SELECT
            @job_name AS job_name,
            CURRENT_TIMESTAMP() AS last_run_ts,
            @meta AS meta
        ) s
        ON t.job_name = s.job_name
        WHEN MATCHED THEN
          UPDATE SET last_run_ts = s.last_run_ts, meta = s.meta
        WHEN NOT MATCHED THEN
          INSERT (job_name, last_run_ts, meta)
          VALUES (s.job_name, s.last_run_ts, s.meta)
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_name", "STRING", job_name),
                bigquery.ScalarQueryParameter("meta", "STRING", json.dumps(meta)),
            ]
        ),
    ).result()

def minutes_to_seconds(min_str: Optional[str]) -> Optional[int]:
    try:
        m, s = min_str.split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return None


def bq_append(name: str, rows: list):
    if rows:
        bq.load_table_from_json(
            rows,
            table(name),
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                source_format="NEWLINE_DELIMITED_JSON",
            ),
        ).result()

BALDONTLIE_GAMES_BASE = "https://api.balldontlie.io/v1"

def truncate(table_name: str):
    bq.query(f"TRUNCATE TABLE `{table(table_name)}`").result()


def fetch_games_for_date(game_date: str):
    return http_get(
        BALDONTLIE_GAMES_BASE,
        "/games",
        params={"dates[]": game_date},
    ).get("data", [])

def ensure_backfill_log():
    bq.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{table("backfill_log")}` (
            run_id STRING,
            log_ts TIMESTAMP,
            level STRING,
            scope STRING,
            message STRING,
            meta STRING
        )
        """
    ).result()

import uuid

def log_event(
    run_id: str,
    level: str,
    scope: str,
    message: str,
    meta: Optional[dict] = None,
):
    ensure_backfill_log()

    row = {
        "run_id": run_id,
        "log_ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "scope": scope,
        "message": message,
        "meta": json.dumps(meta or {}),
    }

    # stdout (real-time visibility)
    print(f"[{level}] [{scope}] {message}")

    # BigQuery
    bq_append("backfill_log", [row])

# ======================================================
# ACTIVE PLAYERS
# ======================================================
def ingest_active_players(season: int):
    if not throttle("active_players"):
        return {"status": "throttled"}
    players = paginate(BALDONTLIE_NBA_BASE, "/players/active", {"season": season})
    rows = [{
        "season": season,
        "player_id": p["id"],
        "name": f'{p["first_name"]} {p["last_name"]}',
        "team_id": p.get("team_id"),
        "position": p.get("position"),
        "updated_at": now_iso(),
    } for p in players]
    bq_append(TABLE_ACTIVE_PLAYERS, rows)
    mark_run("active_players", {"rows": len(rows)})
    return {"rows": len(rows)}

# ======================================================
# GAME STATS
# ======================================================
def ingest_stats(
    start: str,
    end: str,
    period: Optional[int],
    *,
    bypass_throttle: bool = False,
):
    job = "stats_period" if period else "stats_full"

    # 🔒 Throttle unless explicitly bypassed (quarters wrapper)
    if not bypass_throttle and not throttle(job):
        return {"status": "throttled"}

    games = paginate(
        BALDONTLIE_NBA_BASE,
        "/games",
        {"start_date": start, "end_date": end},
    )

    rows = []

    for g in games:
        gid = g["id"]

        stats = paginate(
            BALDONTLIE_STATS_BASE,
            "/stats",
            {
                "game_ids[]": gid,
                **({"period": period} if period else {}),
            },
        )

        for s in stats:
            minutes = s.get("min")

            # --------------------------------------------------
            # 🚫 Skip players who did not play
            # --------------------------------------------------
            if not minutes or minutes in ("0:00", "00:00"):
                continue

            seconds_played = minutes_to_seconds(minutes)

            row = {
                # --------------------------------------------------
                # CORE
                # --------------------------------------------------
                "game_id": gid,
                "game_date": g["date"][:10],
                "season": g["season"],

                "player_id": s["player"]["id"],
                "player": f'{s["player"]["first_name"]} {s["player"]["last_name"]}',

                "team_id": s["team"]["id"],
                "team": s["team"]["abbreviation"],

                # --------------------------------------------------
                # PLAYING TIME
                # --------------------------------------------------
                "minutes": minutes,
                "seconds_played": seconds_played,

                # --------------------------------------------------
                # BASIC STATS
                # --------------------------------------------------
                "pts": s.get("pts"),
                "reb": s.get("reb"),
                "ast": s.get("ast"),
                "stl": s.get("stl"),
                "blk": s.get("blk"),

                # --------------------------------------------------
                # POSSESSION / FOULS
                # --------------------------------------------------
                "turnover": s.get("turnover"),
                "pf": s.get("pf"),

                # --------------------------------------------------
                # SHOOTING
                # --------------------------------------------------
                "fgm": s.get("fgm"),
                "fga": s.get("fga"),
                "fg3m": s.get("fg3m"),
                "fg3a": s.get("fg3a"),
                "ftm": s.get("ftm"),
                "fta": s.get("fta"),

                # --------------------------------------------------
                # MISC
                # --------------------------------------------------
                "plus_minus": s.get("plus_minus"),

                # --------------------------------------------------
                # METADATA
                # --------------------------------------------------
                "data_quality": "official",
            }

            # --------------------------------------------------
            # PERIOD NORMALIZATION
            # --------------------------------------------------
            if period:
                row["period"] = f"Q{period}"
                row["period_num"] = period
                row["ingested_at"] = now_iso()

            rows.append(row)

        sleep_s(RATE["delay"])



    bq_append(
        TABLE_GAME_STATS_PERIOD if period else TABLE_GAME_STATS_FULL,
        rows,
    )

    mark_run(job, {"games": len(games), "rows": len(rows)})

    return {"rows": len(rows)}

def ingest_stats_advanced(start: str, end: str, *, bypass_throttle=False):
    job = "stats_advanced"

    if not bypass_throttle and not throttle(job):
        return {"status": "throttled"}

    games = paginate(
        BALDONTLIE_NBA_BASE,
        "/games",
        {"start_date": start, "end_date": end},
    )

    rows = []

    for g in games:
        # Advanced stats only exist for completed games
        if g.get("status") != "Final":
            continue

        stats = paginate(
            BALDONTLIE_STATS_BASE,
            "/stats/advanced",
            {"game_ids[]": g["id"]},
        )

        for s in stats:
            rows.append({
                "game_id": g["id"],
                "game_date": g["date"][:10],
                "season": g["season"],

                "player_id": s["player"]["id"],
                "team_id": s["team"]["id"],

                "pie": s.get("pie"),
                "pace": s.get("pace"),
                "assist_percentage": s.get("assist_percentage"),
                "assist_ratio": s.get("assist_ratio"),
                "assist_to_turnover": s.get("assist_to_turnover"),
                "defensive_rating": s.get("defensive_rating"),
                "defensive_rebound_percentage": s.get("defensive_rebound_percentage"),
                "effective_field_goal_percentage": s.get("effective_field_goal_percentage"),
                "net_rating": s.get("net_rating"),
                "offensive_rating": s.get("offensive_rating"),
                "offensive_rebound_percentage": s.get("offensive_rebound_percentage"),
                "rebound_percentage": s.get("rebound_percentage"),
                "true_shooting_percentage": s.get("true_shooting_percentage"),
                "turnover_ratio": s.get("turnover_ratio"),
                "usage_percentage": s.get("usage_percentage"),

                "data_quality": "official",
                "ingested_at": now_iso(),
            })

        sleep_s(RATE["delay"])

    if rows:
        bq_append(TABLE_GAME_STATS_ADVANCED_STAGING, rows)
        merge_stats_advanced()
        truncate(TABLE_GAME_STATS_ADVANCED_STAGING)

    mark_run(job, {
        "games_checked": len(games),
        "rows_attempted": len(rows),
    })

    return {"rows_attempted": len(rows)}



# ======================================================
# LINEUPS
# ======================================================
def ingest_lineups(start: str, end: str):
    if not throttle("lineups"):
        return {"status": "throttled"}
    games = paginate(BALDONTLIE_NBA_BASE, "/games", {"start_date": start, "end_date": end})
    rows = []
    for g in games:
        for lu in paginate(
            BALDONTLIE_NBA_BASE,
            "/lineups",
            {"game_ids[]": g["id"]},  # ✅ correct param
        ):
            rows.append({
                "game_id": g["id"],
                "team_id": lu["team"]["id"] if lu.get("team") else None,
                "players": lu.get("players") or lu.get("player_ids") or [],
                "minutes": lu.get("minutes"),
                "ingested_at": now_iso(),
            })
    bq_append(TABLE_LINEUPS, rows)
    mark_run("lineups", {"rows": len(rows)})
    return {"rows": len(rows)}

# ======================================================
# PLAYER PROPS (V2 – CORRECT)
# ======================================================
def ingest_player_props(game_date: str):
    """
    Pull LIVE player props for all NBA games on a given date.
    Snapshot-based ingestion. No vendor filtering.
    """

    if not throttle("props"):
        return {"status": "throttled"}

    # --------------------------------------------------
    # 1️⃣ Get scheduled NBA games for the date
    # --------------------------------------------------
    games_resp = http_get(
        "https://api.balldontlie.io/v1",   # ✅ FIXED BASE
        "/games",
        {"dates[]": game_date},
    )

    games = games_resp.get("data", [])
    if not games:
        print(f"⚠️ No NBA games found for {game_date}")
        return {"status": "no_games"}

    rows = []
    games_with_props = 0

    # --------------------------------------------------
    # 2️⃣ Pull props per game_id (REQUIRED)
    # --------------------------------------------------
    for g in games:
        game_id = g["id"]

        try:
            props_resp = http_get(
                "https://api.balldontlie.io/v2/odds",  # ✅ FIXED BASE
                "/player_props",                      # ✅ FIXED PATH
                {"game_id": game_id},
            )
        except Exception as e:
            print(f"❌ Failed props pull for game {game_id}: {e}")
            continue

        props = props_resp.get("data", [])
        if not props:
            print(f"ℹ️ No props available for game {game_id}")
            continue

        games_with_props += 1

        # --------------------------------------------------
        # 3️⃣ Normalize props
        # --------------------------------------------------
        for p in props:
            market = p.get("market") or {}

            rows.append({
                "prop_id": p["id"],
                "game_id": p["game_id"],
                "player_id": p["player_id"],
                "vendor": p["vendor"],
                "prop_type": p["prop_type"],
                "line_value": p["line_value"],
                "market_type": market.get("type"),

                # over / under
                "odds_over": market.get("over_odds"),
                "odds_under": market.get("under_odds"),

                # milestone
                "milestone_odds": market.get("odds"),

                # timestamps
                "updated_at": p["updated_at"],
                "snapshot_ts": now_iso(),
                "ingested_at": now_iso(),
            })

        sleep_s(RATE["delay"])

    # --------------------------------------------------
    # 4️⃣ Write snapshot rows
    # --------------------------------------------------
    if rows:
        bq_overwrite(TABLE_PLAYER_PROPS_STAGING, rows)
        swap_tables(TABLE_PLAYER_PROPS, TABLE_PLAYER_PROPS_STAGING)


    mark_run("props", {
        "date": game_date,
        "games_checked": len(games),
        "games_with_props": games_with_props,
        "rows": len(rows),
    })

    return {
        "date": game_date,
        "games_checked": len(games),
        "games_with_props": games_with_props,
        "rows_inserted": len(rows),
    }

from zoneinfo import ZoneInfo

def ingest_games(game_date: Optional[str] = None):
    """
    Snapshot ingest of NBA games (game-level only).
    Safe to overwrite.
    """

    if not throttle("games"):
        return {"status": "throttled"}

    params = {}
    if game_date:
        params["dates[]"] = game_date

    games = paginate(
        BALDONTLIE_GAMES_BASE,
        "/games",
        params,
    )

    rows = []

    for g in games:
        home_scores = g.get("home_team_scores") or []
        away_scores = g.get("visitor_team_scores") or []

        # quarters
        def q(scores, i):
            return scores[i] if len(scores) > i else None

        # overtime
        home_ot = home_scores[4:] if len(home_scores) > 4 else []
        away_ot = away_scores[4:] if len(away_scores) > 4 else []

        start_est = (
            datetime.fromisoformat(g["date"].replace("Z", "+00:00"))
            .astimezone(ZoneInfo("America/New_York"))
        )

        rows.append({
            "game_id": g["id"],
            "season": g["season"],
            "game_date": g["date"][:10],
            "start_time_est": start_est.isoformat(),

            "status": g["status"],
            "is_final": g["status"] == "Final",
            "has_overtime": len(home_ot) > 0,
            "num_overtimes": len(home_ot),

            "home_team_id": g["home_team"]["id"],
            "home_team_abbr": g["home_team"]["abbreviation"],

            "away_team_id": g["visitor_team"]["id"],
            "away_team_abbr": g["visitor_team"]["abbreviation"],

            "home_score_q1": q(home_scores, 0),
            "home_score_q2": q(home_scores, 1),
            "home_score_q3": q(home_scores, 2),
            "home_score_q4": q(home_scores, 3),
            "home_score_ot": home_ot,
            "home_score_final": g.get("home_team_score"),

            "away_score_q1": q(away_scores, 0),
            "away_score_q2": q(away_scores, 1),
            "away_score_q3": q(away_scores, 2),
            "away_score_q4": q(away_scores, 3),
            "away_score_ot": away_ot,
            "away_score_final": g.get("visitor_team_score"),

            "last_updated": now_iso(),
            "ingested_at": now_iso(),
        })

    if rows:
        bq_overwrite(TABLE_GAMES, rows)

    mark_run("games", {
        "games": len(rows),
        "date": game_date,
    })

    return {
        "games": len(rows),
        "date": game_date,
    }
    
# ======================================================
# BACKFILL
# ======================================================
from datetime import timedelta

def backfill_season(
    start: str,
    end: str,
    *,
    include_full=True,
    include_quarters=True,
    include_advanced=True,
):
    run_id = f"season_backfill_{uuid.uuid4().hex[:8]}"

    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()

    totals = {
        "days": 0,
        "full_rows": 0,
        "quarter_rows": 0,
        "advanced_rows": 0,
    }

    log_event(
        run_id,
        "INFO",
        "INIT",
        f"Starting season backfill {start} → {end}",
        totals,
    )

    for d in daterange(start_d, end_d):
        day = d.isoformat()
        totals["days"] += 1

        log_event(run_id, "INFO", "DAY_START", f"Processing {day}")

        # ----------------------------------
        # FULL GAME STATS
        # ----------------------------------
        if include_full:
            try:
                r = ingest_stats(
                    start=day,
                    end=day,
                    period=None,
                    bypass_throttle=True,
                )
                rows = r.get("rows", 0)
                totals["full_rows"] += rows

                log_event(
                    run_id,
                    "SUCCESS",
                    "STATS_FULL",
                    f"{day} → {rows} rows",
                )
            except Exception as e:
                log_event(
                    run_id,
                    "ERROR",
                    "STATS_FULL",
                    f"{day} failed",
                    {"error": str(e)},
                )

        # ----------------------------------
        # QUARTERS
        # ----------------------------------
        if include_quarters:
            for q in (1, 2, 3, 4):
                try:
                    r = ingest_stats(
                        start=day,
                        end=day,
                        period=q,
                        bypass_throttle=True,
                    )
                    rows = r.get("rows", 0)
                    totals["quarter_rows"] += rows

                    log_event(
                        run_id,
                        "SUCCESS",
                        f"STATS_Q{q}",
                        f"{day} Q{q} → {rows} rows",
                    )
                except Exception as e:
                    log_event(
                        run_id,
                        "ERROR",
                        f"STATS_Q{q}",
                        f"{day} Q{q} failed",
                        {"error": str(e)},
                    )

        # ----------------------------------
        # ADVANCED
        # ----------------------------------
        if include_advanced:
            try:
                r = ingest_stats_advanced(
                    start=day,
                    end=day,
                    bypass_throttle=True,
                )
                rows = r.get("rows_attempted", 0)
                totals["advanced_rows"] += rows

                log_event(
                    run_id,
                    "SUCCESS",
                    "STATS_ADVANCED",
                    f"{day} → {rows} rows",
                )
            except Exception as e:
                log_event(
                    run_id,
                    "ERROR",
                    "STATS_ADVANCED",
                    f"{day} failed",
                    {"error": str(e)},
                )

        # 🛡️ Hard safety buffer
        sleep_s(1.0)

    log_event(
        run_id,
        "SUCCESS",
        "COMPLETE",
        "Season backfill completed",
        totals,
    )

    return {
        "run_id": run_id,
        **totals,
    }


# ======================================================
# ROUTES
# ======================================================
@app.route("/")
def health():
    return "🏀 GOAT NBA ingestion alive"

@app.route("/goat/ingest/player-props")
def route_props():
    date_q = request.args.get("date") or date.today().isoformat()
    return jsonify(ingest_player_props(date_q))

@app.route("/goat/ingest/active-players")
def route_active_players():
    season = request.args.get("season", type=int)

    if not season:
        return jsonify({
            "error": "Missing required query param: season"
        }), 400

    result = ingest_active_players(season)
    return jsonify(result)

@app.route("/goat/ingest/lineups")
def route_lineups():
    start = request.args.get("start")
    end = request.args.get("end")

    if not start or not end:
        return jsonify({
            "error": "Missing required query params: start, end (YYYY-MM-DD)"
        }), 400

    return jsonify(ingest_lineups(start, end))

@app.route("/goat/ingest/stats/advanced")
def route_stats_advanced():
    start = request.args.get("start") or yesterday_ny()
    end = request.args.get("end") or start  
    bypass = request.args.get("bypass", "false").lower() == "true"

    return jsonify(
        ingest_stats_advanced(start, end, bypass_throttle=bypass)
    )


# ======================================================
# GAME STATS ROUTES
# ======================================================

@app.route("/goat/ingest/stats/full")
def route_stats_full():
    start = request.args.get("start") or yesterday_ny()
    end = request.args.get("end") or start

    return jsonify(ingest_stats(start, end, period=None))




@app.route("/goat/ingest/stats/period")
def route_stats_period():
    start = request.args.get("start")
    end = request.args.get("end")
    period = request.args.get("period", type=int)

    if not start or not end or not period:
        return jsonify({
            "error": "Missing required query params: start, end, period"
        }), 400

    if period not in (1, 2, 3, 4):
        return jsonify({
            "error": "period must be 1, 2, 3, or 4"
        }), 400

    return jsonify(ingest_stats(start, end, period=period))

@app.route("/goat/ingest/stats/quarters")
def route_stats_all_quarters():
    start = request.args.get("start") or yesterday_ny()
    end = request.args.get("end") or start


    total_rows = 0

    for q in (1, 2, 3, 4):
        result = ingest_stats(start, end, period=q, bypass_throttle=True)
        total_rows += result.get("rows", 0)

    return {
        "quarters": [1, 2, 3, 4],
        "rows_inserted": total_rows,
    }

@app.route("/goat/ingest/backfill/season")
def route_backfill_season():
    start = request.args.get("start")
    end = request.args.get("end")

    if not start or not end:
        return {"error": "Missing start/end"}, 400

    result = backfill_season(start, end)
    return jsonify(result)

def run_season_backfill_cli():
    start = os.getenv("BACKFILL_START")
    end = os.getenv("BACKFILL_END")

    if not start or not end:
        raise RuntimeError("BACKFILL_START and BACKFILL_END required")

    result = backfill_season(start, end)
    print("✅ BACKFILL COMPLETE")
    print(json.dumps(result, indent=2))

@app.route("/goat/ingest/games")
def route_ingest_games():
    game_date = request.args.get("date")
    return jsonify(ingest_games(game_date))

if __name__ == "__main__":
    if os.getenv("RUN_BACKFILL") == "true":
        run_season_backfill_cli()
    else:
        app.run(host="0.0.0.0", port=8080, debug=True)

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)