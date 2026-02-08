# ======================================================
# GOAT NBA UNIFIED INGESTION SERVICE (IDEMPOTENT)
# ======================================================

import os
import time
import json
import requests
from datetime import datetime, timezone, date
from typing import Any, Dict, Optional

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

TABLE_GAME_STATS_FULL_STAGING = "player_game_stats_full_staging"
TABLE_GAME_STATS_PERIOD_STAGING = "player_game_stats_period_staging"

TABLE_LINEUPS = "game_lineups"
TABLE_PLAYER_PROPS = "player_prop_odds"

BALDONTLIE_STATS_BASE = "https://api.balldontlie.io/v1"
BALDONTLIE_NBA_BASE = "https://api.balldontlie.io/v1"
BALDONTLIE_ODDS_BASE = "https://api.balldontlie.io/v2"

API_KEY = (
    os.getenv("BALDONTLIE_KEY")
    or os.getenv("BALLDONTLIE_API_KEY")
    or ""
)
HEADERS = {"Authorization": API_KEY}

RATE_PROFILE = os.getenv("BALLDONTLIE_TIER", "GOAT").upper()
RATE_LIMITS = {
    "ALL_STAR": {"delay": 1.2, "retry": 10},
    "GOAT": {"delay": 0.3, "retry": 3},
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
    r = requests.get(
        f"{base}{path}",
        headers={"Authorization": API_KEY} if "balldontlie" in base else {},
        params=params or {},
        timeout=25,
    )

    if r.status_code == 429:
        sleep_s(RATE["retry"])
        return http_get(base, path, params)

    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")

    if "application/json" not in r.headers.get("Content-Type", "").lower():
        raise RuntimeError(f"NON-JSON RESPONSE: {r.text[:500]}")

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

def bq_append(table_name: str, rows: list):
    if not rows:
        return
    bq.load_table_from_json(
        rows,
        table(table_name),
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format="NEWLINE_DELIMITED_JSON",
        ),
    ).result()

def truncate(table_name: str):
    bq.query(f"TRUNCATE TABLE `{table(table_name)}`").result()

# ======================================================
# STATE / THROTTLE
# ======================================================
def ensure_state():
    bq.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{table(TABLE_STATE)}`
        (
          job_name STRING NOT NULL,
          last_run_ts TIMESTAMP,
          meta STRING
        )
        """
    ).result()

def throttle(job: str) -> bool:
    ensure_state()

    rows = list(
        bq.query(
            f"""
            SELECT last_run_ts
            FROM `{table(TABLE_STATE)}`
            WHERE job_name = @job
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job", "STRING", job)
                ]
            ),
        ).result()
    )

    if not rows or rows[0]["last_run_ts"] is None:
        return True

    age = (
        datetime.now(timezone.utc)
        - rows[0]["last_run_ts"].replace(tzinfo=timezone.utc)
    ).total_seconds()

    return age >= THROTTLES[job]

def mark_run(job: str, meta: dict):
    ensure_state()
    bq.query(
        f"""
        MERGE `{table(TABLE_STATE)}` t
        USING (
          SELECT
            @job AS job_name,
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
                bigquery.ScalarQueryParameter("job", "STRING", job),
                bigquery.ScalarQueryParameter("meta", "STRING", json.dumps(meta)),
            ]
        ),
    ).result()

# ======================================================
# MERGE HELPERS (IDEMPOTENT)
# ======================================================
def merge_stats_full():
    bq.query(
        f"""
        MERGE `{table(TABLE_GAME_STATS_FULL)}` t
        USING `{table(TABLE_GAME_STATS_FULL_STAGING)}` s
        ON t.game_id = s.game_id
           AND t.player_id = s.player_id
        WHEN NOT MATCHED THEN
          INSERT ROW
        """
    ).result()

def merge_stats_period():
    bq.query(
        f"""
        MERGE `{table(TABLE_GAME_STATS_PERIOD)}` t
        USING `{table(TABLE_GAME_STATS_PERIOD_STAGING)}` s
        ON t.game_id = s.game_id
           AND t.player_id = s.player_id
           AND t.period_num = s.period_num
        WHEN NOT MATCHED THEN
          INSERT ROW
        """
    ).result()

# ======================================================
# GAME STATS INGESTION
# ======================================================
def minutes_to_seconds(min_str: Optional[str]) -> Optional[int]:
    try:
        m, s = min_str.split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return None

def ingest_stats(start: str, end: str, period: Optional[int], *, bypass_throttle=False):
    job = "stats_period" if period else "stats_full"
    if not bypass_throttle and not throttle(job):
        return {"status": "throttled"}

    games = paginate(
        BALDONTLIE_NBA_BASE,
        "/games",
        {"start_date": start, "end_date": end},
    )

    rows = []

    for g in games:
        stats = paginate(
            BALDONTLIE_STATS_BASE,
            "/stats",
            {"game_ids[]": g["id"], **({"period": period} if period else {})},
        )

        for s in stats:
            if not s.get("min") or s["min"] in ("0:00", "00:00"):
                continue

            row = {
                "game_id": g["id"],
                "game_date": g["date"][:10],
                "season": g["season"],
                "player_id": s["player"]["id"],
                "player": f'{s["player"]["first_name"]} {s["player"]["last_name"]}',
                "team_id": s["team"]["id"],
                "team": s["team"]["abbreviation"],
                "minutes": s["min"],
                "seconds_played": minutes_to_seconds(s["min"]),
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
                "data_quality": "official",
            }

            if period:
                row["period"] = f"Q{period}"
                row["period_num"] = period
                row["ingested_at"] = now_iso()

            rows.append(row)

        sleep_s(RATE["delay"])

    staging = (
        TABLE_GAME_STATS_PERIOD_STAGING
        if period
        else TABLE_GAME_STATS_FULL_STAGING
    )

    bq_append(staging, rows)

    if period:
        merge_stats_period()
        truncate(TABLE_GAME_STATS_PERIOD_STAGING)
    else:
        merge_stats_full()
        truncate(TABLE_GAME_STATS_FULL_STAGING)

    mark_run(job, {"games": len(games), "rows": len(rows)})
    return {"rows_inserted": len(rows)}

# ======================================================
# ROUTES
# ======================================================
@app.route("/")
def health():
    return "🏀 GOAT NBA ingestion alive (idempotent)"

@app.route("/goat/ingest/stats/full")
def route_stats_full():
    start = request.args.get("start")
    end = request.args.get("end")
    if not start or not end:
        return {"error": "Missing start/end"}, 400
    return jsonify(ingest_stats(start, end, period=None))

@app.route("/goat/ingest/stats/period")
def route_stats_period():
    start = request.args.get("start")
    end = request.args.get("end")
    period = request.args.get("period", type=int)
    if not start or not end or period not in (1, 2, 3, 4):
        return {"error": "Invalid params"}, 400
    return jsonify(ingest_stats(start, end, period=period))

@app.route("/goat/ingest/stats/quarters")
def route_stats_quarters():
    start = request.args.get("start")
    end = request.args.get("end")
    if not start or not end:
        return {"error": "Missing start/end"}, 400

    total = 0
    for q in (1, 2, 3, 4):
        total += ingest_stats(start, end, q, bypass_throttle=True).get("rows_inserted", 0)

    return {"quarters": [1, 2, 3, 4], "rows_attempted": total}

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
