import os
import time
import json
import requests
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify
from dotenv import load_dotenv

from google.cloud import bigquery

load_dotenv()

# ======================================================
# CONFIG
# ======================================================
PROJECT_ID = os.getenv("PROJECT_ID", "graphite-flare-477419-h7")
GOAT_DATASET = os.getenv("GOAT_DATASET", "nba_goat_data")

TABLE_STATE = os.getenv("TABLE_STATE", "ingest_state")

TABLE_ACTIVE_PLAYERS = os.getenv("TABLE_ACTIVE_PLAYERS", "active_players")
TABLE_GAME_STATS_FULL = os.getenv("TABLE_GAME_STATS_FULL", "player_game_stats_full")
TABLE_GAME_STATS_PERIOD = os.getenv("TABLE_GAME_STATS_PERIOD", "player_game_stats_period")
TABLE_LINEUPS = os.getenv("TABLE_LINEUPS", "game_lineups")
TABLE_PLAYER_PROPS = os.getenv("TABLE_PLAYER_PROPS", "player_prop_odds")

BALDONTLIE_STATS_BASE = os.getenv(
    "BALDONTLIE_STATS_BASE",
    "https://api.balldontlie.io/v1"
)

BALDONTLIE_NBA_BASE = os.getenv(
    "BALDONTLIE_NBA_BASE",
    "https://nba.balldontlie.io/v1"
)
API_KEY = os.getenv("BALDONTLIE_KEY", "")

if not API_KEY:
    print("⚠️ BALDONTLIE_KEY is missing. Set it in env / Cloud Run env vars.")

HEADERS = {"Authorization": f"Bearer {API_KEY}"}

RATE_PROFILE = os.getenv("BALLDONTLIE_TIER", "ALL_STAR").upper()

RATE_LIMITS = {
    "ALL_STAR": {"batch_size": 5, "page_delay": 1.2, "batch_delay": 1.5, "retry_429": 10},
    "GOAT": {"batch_size": 20, "page_delay": 0.25, "batch_delay": 0.30, "retry_429": 3},
}

RATE = RATE_LIMITS.get(RATE_PROFILE, RATE_LIMITS["ALL_STAR"])

# Throttle defaults (so scheduler triggers don’t overlap)
THROTTLE_SECONDS_FULL = int(os.getenv("THROTTLE_SECONDS_FULL", "600"))     # 10 min
THROTTLE_SECONDS_PERIOD = int(os.getenv("THROTTLE_SECONDS_PERIOD", "900")) # 15 min
THROTTLE_SECONDS_LINEUPS = int(os.getenv("THROTTLE_SECONDS_LINEUPS", "120")) # 2 min
THROTTLE_SECONDS_PROPS = int(os.getenv("THROTTLE_SECONDS_PROPS", "120"))     # 2 min
THROTTLE_SECONDS_PLAYERS = int(os.getenv("THROTTLE_SECONDS_PLAYERS", "3600"))# 1 hr

# ======================================================
# APP
# ======================================================
app = Flask(__name__)
bq = bigquery.Client(project=PROJECT_ID)

# ======================================================
# UTIL
# ======================================================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def sleep_s(seconds: float) -> None:
    time.sleep(max(0.0, seconds))

def bq_table(table_name: str) -> str:
    return f"{PROJECT_ID}.{GOAT_DATASET}.{table_name}"

def http_get(path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 25) -> Dict[str, Any]:
    url = f"{BALDONTLIE_BASE}{path}"
    while True:
        r = requests.get(url, headers=HEADERS, params=params or {}, timeout=timeout)
        if r.status_code == 429:
            sleep_s(RATE["retry_429"])
            continue
        r.raise_for_status()
        return r.json()

def paginate(path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Cursor-based pagination (balldontlie style).
    """
    out: List[Dict[str, Any]] = []
    cursor = None

    while True:
        p = dict(params)
        if cursor:
            p["cursor"] = cursor

        data = http_get(path, params=p)
        out.extend(data.get("data", []))

        cursor = (data.get("meta") or {}).get("next_cursor")
        if not cursor:
            break

        sleep_s(RATE["page_delay"])

    return out

# ======================================================
# STATE THROTTLE (BigQuery)
# ======================================================
def ensure_state_table() -> None:
    """
    Create state table if it doesn't exist.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{bq_table(TABLE_STATE)}`
    (
      job_name STRING NOT NULL,
      last_run_ts TIMESTAMP,
      meta STRING
    )
    """
    bq.query(sql).result()

def get_last_run(job_name: str) -> Optional[datetime]:
    ensure_state_table()
    sql = f"""
    SELECT last_run_ts
    FROM `{bq_table(TABLE_STATE)}`
    WHERE job_name = @job_name
    LIMIT 1
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("job_name", "STRING", job_name)]
        ),
    )
    rows = list(job.result())
    if not rows:
        return None
    return rows[0]["last_run_ts"]

def set_last_run(job_name: str, meta: Optional[Dict[str, Any]] = None) -> None:
    ensure_state_table()
    meta_str = json.dumps(meta or {}, separators=(",", ":"))
    sql = f"""
    MERGE `{bq_table(TABLE_STATE)}` t
    USING (SELECT @job_name AS job_name, CURRENT_TIMESTAMP() AS last_run_ts, @meta AS meta) s
    ON t.job_name = s.job_name
    WHEN MATCHED THEN
      UPDATE SET last_run_ts = s.last_run_ts, meta = s.meta
    WHEN NOT MATCHED THEN
      INSERT (job_name, last_run_ts, meta) VALUES (s.job_name, s.last_run_ts, s.meta)
    """
    bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_name", "STRING", job_name),
                bigquery.ScalarQueryParameter("meta", "STRING", meta_str),
            ]
        ),
    ).result()

def throttle_or_ok(job_name: str, throttle_seconds: int) -> Tuple[bool, Optional[int]]:
    last = get_last_run(job_name)
    if not last:
        return True, None
    age = int((datetime.now(timezone.utc) - last.replace(tzinfo=timezone.utc)).total_seconds())
    if age < throttle_seconds:
        return False, (throttle_seconds - age)
    return True, None

# ======================================================
# GAME LOOKUP
# ======================================================
def fetch_games_range(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    # games endpoint supports start_date/end_date (you were using this already)
    return paginate("/games", {"per_page": 100, "start_date": start_date, "end_date": end_date})

# ======================================================
# ACTIVE PLAYERS
# ======================================================
def fetch_active_players(season: int) -> List[Dict[str, Any]]:
    # docs show /players/active (NBA site docs)
    return paginate("/players/active", {"per_page": 100, "season": season})

def map_active_player_row(p: Dict[str, Any], season: int) -> Dict[str, Any]:
    return {
        "season": season,
        "player_id": p.get("id"),
        "first_name": p.get("first_name"),
        "last_name": p.get("last_name"),
        "player_name": f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
        "position": p.get("position"),
        "height": p.get("height"),
        "weight": p.get("weight"),
        "jersey_number": p.get("jersey_number"),
        "college": p.get("college"),
        "country": p.get("country"),
        "draft_year": p.get("draft_year"),
        "draft_round": p.get("draft_round"),
        "draft_number": p.get("draft_number"),
        "team_id": p.get("team_id"),
        "updated_at_utc": now_utc_iso(),
    }

# ======================================================
# FULL GAME STATS (stats endpoint)
# ======================================================
def fetch_full_game_stats(game_id: int) -> List[Dict[str, Any]]:
    # You confirmed /stats works (and returns 200) with Bearer
    return paginate("/stats", {"per_page": 100, "game_ids[]": game_id})

def map_stat_row(s: Dict[str, Any], stat_period: str) -> Optional[Dict[str, Any]]:
    if not s or not s.get("player") or not s.get("game") or not s.get("team"):
        return None

    player = s.get("player", {})
    team = s.get("team", {})
    game = s.get("game", {})

    # keep minutes as INT when possible
    minutes = None
    try:
        if s.get("min") is not None and str(s.get("min")).strip() != "":
            minutes = int(str(s.get("min")))
    except Exception:
        minutes = None

    return {
        "stat_id": s.get("id"),
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        "season": game.get("season"),
        "stat_period": stat_period,  # FULL or Q1-Q4

        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name','')} {player.get('last_name','')}".strip(),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "team_name": team.get("full_name"),

        "minutes": minutes,

        "pts": s.get("pts"),
        "reb": s.get("reb"),
        "ast": s.get("ast"),
        "stl": s.get("stl"),
        "blk": s.get("blk"),
        "turnover": s.get("turnover"),
        "pf": s.get("pf"),

        "fgm": s.get("fgm"),
        "fga": s.get("fga"),
        "fg3m": s.get("fg3m"),
        "fg3a": s.get("fg3a"),
        "ftm": s.get("ftm"),
        "fta": s.get("fta"),

        "plus_minus": s.get("plus_minus"),

        "data_quality": "RAW",
        "ingested_at_utc": now_utc_iso(),
    }

# ======================================================
# PERIOD STATS (game_player_stats endpoint)
# ======================================================
def fetch_period_stats(game_id: int, period: int) -> List[Dict[str, Any]]:
    # docs show /game_player_stats?game_id=...&period=1..4
    # (some variants accept game_ids[]; we’ll use game_id to match docs)
    return paginate("/game_player_stats", {"per_page": 100, "game_id": game_id, "period": period})

# ======================================================
# LINEUPS
# ======================================================
def fetch_game_lineups(game_id: int) -> List[Dict[str, Any]]:
    # docs show /lineups?game_id=... (lineups available starting 2025 season)
    return paginate("/lineups", {"per_page": 100, "game_id": game_id})

def map_lineup_row(lu: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not lu:
        return None

    # Keep raw nested payload too (future-proof)
    return {
        "lineup_id": lu.get("id"),
        "game_id": lu.get("game_id"),
        "team_id": lu.get("team_id"),
        "team_abbr": (lu.get("team") or {}).get("abbreviation"),
        "lineup_type": lu.get("lineup_type"),
        "period": lu.get("period"),
        "minutes": lu.get("minutes"),
        "seconds": lu.get("seconds"),
        "players": lu.get("players"),  # array of player objects / ids (docs)
        "stats": lu.get("stats"),      # object (docs)
        "raw_json": json.dumps(lu, separators=(",", ":")),
        "ingested_at_utc": now_utc_iso(),
    }

# ======================================================
# PLAYER PROP ODDS
# ======================================================
def fetch_all_player_props(vendor: str | None = None):
    params = {"per_page": 100}
    if vendor:
        params["vendor"] = vendor

    rows = []
    cursor = None

    while True:
        if cursor:
            params["cursor"] = cursor

        r = requests.get(
            f"{BALDONTLIE_NBA_BASE}/player_props",
            headers=HEADERS,
            params=params,
            timeout=25,
        )

        if r.status_code == 429:
            time.sleep(RATE["retry_429"])
            continue

        r.raise_for_status()
        payload = r.json()

        rows.extend(payload.get("data", []))
        cursor = payload.get("meta", {}).get("next_cursor")

        if not cursor:
            break

        time.sleep(RATE["page_delay"])

    return rows

def map_player_prop_row(p: dict):
    return {
        "prop_id": p.get("id"),
        "league": p.get("league"),
        "vendor": p.get("vendor"),

        "game_id": p.get("game_id"),
        "player_id": p.get("player_id"),

        "market": p.get("market"),
        "prop_type": p.get("prop_type"),
        "line": p.get("line_value"),

        "odds_over": p.get("odds_over"),
        "odds_under": p.get("odds_under"),

        "outcome": p.get("outcome"),
        "updated_at": p.get("updated_at"),

        # Always keep raw JSON for safety
        "raw_json": json.dumps(p, separators=(",", ":")),
        "ingested_at_utc": datetime.utcnow().isoformat(),
    }

# ======================================================
# BIGQUERY WRITE (JSON LOAD)
# ======================================================
def bq_append_json(table_name: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    table_id = bq_table(table_name)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=False,  # fail loud if schema mismatch
    )

    # load_table_from_json is safer than insert_rows_json for volume
    load_job = bq.load_table_from_json(rows, table_id, job_config=job_config)
    load_job.result()

# ======================================================
# ROUTES
# ======================================================
@app.route("/")
def health():
    return f"🏀 GOAT unified ingestion (Python) alive | tier={RATE_PROFILE}"

@app.route("/goat/ingest/active-players")
def ingest_active_players():
    ok, wait = throttle_or_ok("active_players", THROTTLE_SECONDS_PLAYERS)
    if not ok:
        return jsonify({"status": "throttled", "wait_seconds": wait}), 200

    season = request.args.get("season")
    if not season:
        return jsonify({"error": "Missing season (e.g. 2025)"}), 400

    season_i = int(season)
    players = fetch_active_players(season_i)
    rows = [map_active_player_row(p, season_i) for p in players]

    bq_append_json(TABLE_ACTIVE_PLAYERS, rows)
    set_last_run("active_players", {"season": season_i, "rows": len(rows)})

    return jsonify({"status": "ok", "season": season_i, "rows_inserted": len(rows)})

@app.route("/goat/ingest/player-stats-full")
def ingest_player_stats_full():
    ok, wait = throttle_or_ok("player_stats_full", THROTTLE_SECONDS_FULL)
    if not ok:
        return jsonify({"status": "throttled", "wait_seconds": wait}), 200

    start = request.args.get("start")
    end = request.args.get("end")
    if not start or not end:
        return jsonify({"error": "Missing start or end (YYYY-MM-DD)"}), 400

    games = fetch_games_range(start, end)

    rows: List[Dict[str, Any]] = []
    for i in range(0, len(games), RATE["batch_size"]):
        batch = games[i:i + RATE["batch_size"]]
        for g in batch:
            gid = g.get("id")
            if not gid:
                continue
            stats = fetch_full_game_stats(int(gid))
            for s in stats:
                row = map_stat_row(s, "FULL")
                if row:
                    rows.append(row)

        sleep_s(RATE["batch_delay"])

    bq_append_json(TABLE_GAME_STATS_FULL, rows)
    set_last_run("player_stats_full", {"start": start, "end": end, "games": len(games), "rows": len(rows)})

    return jsonify({"status": "ok", "games": len(games), "rows_inserted": len(rows)})

@app.route("/goat/ingest/player-stats-periods")
def ingest_player_stats_periods():
    ok, wait = throttle_or_ok("player_stats_periods", THROTTLE_SECONDS_PERIOD)
    if not ok:
        return jsonify({"status": "throttled", "wait_seconds": wait}), 200

    start = request.args.get("start")
    end = request.args.get("end")
    if not start or not end:
        return jsonify({"error": "Missing start or end (YYYY-MM-DD)"}), 400

    games = fetch_games_range(start, end)

    rows: List[Dict[str, Any]] = []
    for i in range(0, len(games), RATE["batch_size"]):
        batch = games[i:i + RATE["batch_size"]]
        for g in batch:
            gid = g.get("id")
            if not gid:
                continue
            gid_int = int(gid)

            for period in (1, 2, 3, 4):
                stats = fetch_period_stats(gid_int, period)
                for s in stats:
                    row = map_stat_row(s, f"Q{period}")
                    if row:
                        rows.append(row)

                sleep_s(RATE["page_delay"])

        sleep_s(RATE["batch_delay"])

    bq_append_json(TABLE_GAME_STATS_PERIOD, rows)
    set_last_run("player_stats_periods", {"start": start, "end": end, "games": len(games), "rows": len(rows)})

    return jsonify({"status": "ok", "games": len(games), "rows_inserted": len(rows)})

@app.route("/goat/ingest/lineups")
def ingest_lineups():
    ok, wait = throttle_or_ok("lineups", THROTTLE_SECONDS_LINEUPS)
    if not ok:
        return jsonify({"status": "throttled", "wait_seconds": wait}), 200

    start = request.args.get("start")
    end = request.args.get("end")
    if not start or not end:
        return jsonify({"error": "Missing start or end (YYYY-MM-DD)"}), 400

    games = fetch_games_range(start, end)

    rows: List[Dict[str, Any]] = []
    for i in range(0, len(games), RATE["batch_size"]):
        batch = games[i:i + RATE["batch_size"]]
        for g in batch:
            gid = g.get("id")
            if not gid:
                continue
            lineups = fetch_game_lineups(int(gid))
            for lu in lineups:
                row = map_lineup_row(lu)
                if row:
                    rows.append(row)

        sleep_s(RATE["batch_delay"])

    bq_append_json(TABLE_LINEUPS, rows)
    set_last_run("lineups", {"start": start, "end": end, "games": len(games), "rows": len(rows)})

    return jsonify({"status": "ok", "games": len(games), "rows_inserted": len(rows)})

@app.route("/goat/ingest/player-props")
def ingest_player_props():
    ok, wait = throttle_or_ok("player_props", THROTTLE_SECONDS_PROPS)
    if not ok:
        return jsonify({"status": "throttled", "wait_seconds": wait})

    vendor = request.args.get("vendor")  # optional

    props = fetch_all_player_props(vendor=vendor)
    rows = [map_player_prop_row(p) for p in props]

    if rows:
        bq_append_json(TABLE_PLAYER_PROPS, rows)

    set_last_run(
        "player_props",
        {
            "vendor": vendor,
            "rows": len(rows),
        },
    )

    return jsonify({
        "status": "ok",
        "rows_inserted": len(rows),
        "vendor": vendor,
    })

@app.route("/goat/ingest/all")
def ingest_all():
    """
    Convenience route: runs all pieces in order.
    """
    start = request.args.get("start")
    end = request.args.get("end")
    season = request.args.get("season")  # for active players
    vendor = request.args.get("vendor")

    if not start or not end or not season:
        return jsonify({"error": "Missing required params: start, end, season"}), 400

    # Run each “sub-job” (throttles will apply)
    r1 = json.loads(ingest_active_players().get_data(as_text=True))
    r2 = json.loads(ingest_player_stats_full().get_data(as_text=True))
    r3 = json.loads(ingest_player_stats_periods().get_data(as_text=True))
    r4 = json.loads(ingest_lineups().get_data(as_text=True))
    r5 = json.loads(ingest_player_props().get_data(as_text=True))

    return jsonify({
        "status": "ok",
        "active_players": r1,
        "player_stats_full": r2,
        "player_stats_periods": r3,
        "lineups": r4,
        "player_props": r5,
    })

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)