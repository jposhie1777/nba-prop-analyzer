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

BALDONTLIE_STATS_BASE = "https://api.balldontlie.io/v1"
BALDONTLIE_NBA_BASE = "https://nba.balldontlie.io/v1"
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
    while True:
        r = requests.get(url, headers=HEADERS, params=params or {}, timeout=25)
        if r.status_code == 401:
            r = requests.get(url, headers={"Authorization": f"Bearer {API_KEY}"}, params=params or {}, timeout=25)
        if r.status_code == 429:
            sleep_s(RATE["retry"])
            continue
        r.raise_for_status()
        return r.json()

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

# ======================================================
# STATE / THROTTLE
# ======================================================
def ensure_state():
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table(TABLE_STATE)}`
    (job STRING, last_run TIMESTAMP, meta STRING)
    """
    bq.query(sql).result()

def throttle(job: str):
    ensure_state()
    q = f"SELECT last_run FROM `{table(TABLE_STATE)}` WHERE job=@j"
    rows = list(bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("j", "STRING", job)]
    )))
    if not rows:
        return True
    age = (datetime.now(timezone.utc) - rows[0]["last_run"]).total_seconds()
    return age >= THROTTLES[job]

def mark_run(job: str, meta: dict):
    ensure_state()
    bq.query(
        f"""
        MERGE `{table(TABLE_STATE)}` t
        USING (SELECT @j job, CURRENT_TIMESTAMP() ts, @m meta) s
        ON t.job=s.job
        WHEN MATCHED THEN UPDATE SET last_run=s.ts, meta=s.meta
        WHEN NOT MATCHED THEN INSERT (job,last_run,meta) VALUES (s.job,s.ts,s.meta)
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("j", "STRING", job),
                bigquery.ScalarQueryParameter("m", "STRING", json.dumps(meta)),
            ]
        ),
    ).result()

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
def ingest_stats(start: str, end: str, period: Optional[int]):
    job = "stats_period" if period else "stats_full"
    if not throttle(job):
        return {"status": "throttled"}

    games = paginate(BALDONTLIE_NBA_BASE, "/games", {"start_date": start, "end_date": end})
    rows = []

    for g in games:
        gid = g["id"]
        if period:
            stats = paginate(BALDONTLIE_NBA_BASE, "/game_player_stats", {"game_id": gid, "period": period})
        else:
            stats = paginate(BALDONTLIE_STATS_BASE, "/stats", {"game_ids[]": gid})

        for s in stats:
            rows.append({
                "game_id": gid,
                "player_id": s["player"]["id"],
                "team_id": s["team"]["id"],
                "pts": s.get("pts"),
                "reb": s.get("reb"),
                "ast": s.get("ast"),
                "period": f"Q{period}" if period else "FULL",
                "ingested_at": now_iso(),
            })

        sleep_s(RATE["delay"])

    bq_append(TABLE_GAME_STATS_PERIOD if period else TABLE_GAME_STATS_FULL, rows)
    mark_run(job, {"games": len(games), "rows": len(rows)})
    return {"rows": len(rows)}

# ======================================================
# LINEUPS
# ======================================================
def ingest_lineups(start: str, end: str):
    if not throttle("lineups"):
        return {"status": "throttled"}
    games = paginate(BALDONTLIE_NBA_BASE, "/games", {"start_date": start, "end_date": end})
    rows = []
    for g in games:
        for lu in paginate(BALDONTLIE_NBA_BASE, "/lineups", {"game_id": g["id"]}):
            rows.append({
                "game_id": g["id"],
                "team_id": lu["team_id"],
                "players": lu["players"],
                "minutes": lu.get("minutes"),
                "ingested_at": now_iso(),
            })
    bq_append(TABLE_LINEUPS, rows)
    mark_run("lineups", {"rows": len(rows)})
    return {"rows": len(rows)}

# ======================================================
# PLAYER PROPS (V2 – CORRECT)
# ======================================================
def ingest_player_props(game_date: str, vendors: Optional[List[str]]):
    if not throttle("props"):
        return {"status": "throttled"}

    games = http_get(BALDONTLIE_ODDS_BASE, "/games", {"dates[]": game_date}).get("data", [])
    rows = []

    for g in games:
        params = {"game_id": g["id"]}
        if vendors:
            params["vendors[]"] = vendors

        props = http_get(BALDONTLIE_ODDS_BASE, "/odds/player_props", params).get("data", [])
        for p in props:
            m = p.get("market") or {}
            rows.append({
                "prop_id": p["id"],
                "game_id": p["game_id"],
                "player_id": p["player_id"],
                "vendor": p["vendor"],
                "prop_type": p["prop_type"],
                "line": p["line_value"],
                "market_type": m.get("type"),
                "odds_over": m.get("over_odds"),
                "odds_under": m.get("under_odds"),
                "milestone_odds": m.get("odds"),
                "updated_at": p["updated_at"],
                "ingested_at": now_iso(),
            })

        sleep_s(RATE["delay"])

    bq_append(TABLE_PLAYER_PROPS, rows)
    mark_run("props", {"date": game_date, "rows": len(rows)})
    return {"rows": len(rows)}

# ======================================================
# ROUTES
# ======================================================
@app.route("/")
def health():
    return "🏀 GOAT NBA ingestion alive"

@app.route("/goat/ingest/player-props")
def route_props():
    date_q = request.args.get("date") or date.today().isoformat()
    vendors = request.args.get("vendors")
    vendor_list = vendors.split(",") if vendors else None
    return jsonify(ingest_player_props(date_q, vendor_list))

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)