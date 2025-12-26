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

def fetch_games_for_date(game_date: str):
    return http_get(
        BALDONTLIE_GAMES_BASE,
        "/games",
        params={"dates[]": game_date},
    ).get("data", [])


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

        stats = paginate(
            BALDONTLIE_STATS_BASE,
            "/stats",
            {
                "game_ids[]": gid,
                **({"period": period} if period else {}),
            },
        )


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
        bq_append(TABLE_PLAYER_PROPS, rows)

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

# ======================================================
# GAME STATS ROUTES
# ======================================================

@app.route("/goat/ingest/stats/full")
def route_stats_full():
    start = request.args.get("start")
    end = request.args.get("end")

    if not start or not end:
        return jsonify({
            "error": "Missing required query params: start, end (YYYY-MM-DD)"
        }), 400

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



# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)