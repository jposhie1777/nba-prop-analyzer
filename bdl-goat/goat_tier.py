import os
import time
import requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.auth import default as google_auth_default
from dotenv import load_dotenv
load_dotenv()

# ======================================================
# CONFIG
# ======================================================
PROJECT_ID = "graphite-flare-477419-h7"
GOAT_DATASET = "nba_goat_data"
TABLE_PLAYER_STATS = "player_game_stats"

BALDONTLIE_BASE = "https://api.balldontlie.io/v1"
API_KEY = os.getenv("BALDONTLIE_KEY")

RATE_PROFILE = os.getenv("BALLDONTLIE_TIER", "ALL_STAR")

RATE_LIMITS = {
    "ALL_STAR": {
        "batch_size": 5,
        "page_delay": 1.2,
        "batch_delay": 1.5,
    },
    "GOAT": {
        "batch_size": 20,
        "page_delay": 0.25,
        "batch_delay": 0.3,
    },
}

RATE = RATE_LIMITS.get(RATE_PROFILE, RATE_LIMITS["ALL_STAR"])

# ======================================================
# APP
# ======================================================
app = Flask(__name__)
bq = bigquery.Client(project=PROJECT_ID)

HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

# ======================================================
# HELPERS
# ======================================================
def delay(seconds: float):
    time.sleep(seconds)

def fetch_games_range(start_date: str, end_date: str):
    games = []
    cursor = None

    while True:
        params = {
            "per_page": 100,
            "start_date": start_date,
            "end_date": end_date,
        }
        if cursor:
            params["cursor"] = cursor

        r = requests.get(
            f"{BALDONTLIE_BASE}/games",
            headers=HEADERS,
            params=params,
            timeout=20,
        )

        if r.status_code == 429:
            delay(10)
            continue

        r.raise_for_status()
        data = r.json()

        games.extend(data.get("data", []))
        cursor = data.get("meta", {}).get("next_cursor")

        if not cursor:
            break

        delay(RATE["page_delay"])

    return games


def fetch_goat_player_game_stats(game_id: int, period: int | None = None):
    rows = []
    cursor = None

    while True:
        params = {
            "game_ids[]": game_id,
            "per_page": 100,
        }
        if period:
            params["period"] = period
        if cursor:
            params["cursor"] = cursor

        r = requests.get(
            f"{BALDONTLIE_BASE}/game_player_stats",
            headers=HEADERS,
            params=params,
            timeout=20,
        )

        if r.status_code == 429:
            delay(10)
            continue

        if not r.ok:
            print("FETCH ERROR:", r.text)
            break

        payload = r.json()
        rows.extend(payload.get("data", []))
        cursor = payload.get("meta", {}).get("next_cursor")

        if not cursor:
            break

        delay(RATE["page_delay"])

    return rows


def map_stat_to_row(s, stat_period: str):
    if not s or not s.get("player") or not s.get("game"):
        return None

    player = s["player"]
    team = s["team"]
    game = s["game"]

    try:
        minutes = int(s["min"]) if s.get("min") else None
    except ValueError:
        minutes = None

    return {
        "game_id": game["id"],
        "game_date": game["date"],
        "season": game["season"],
        "stat_period": stat_period,

        "player_id": player["id"],
        "player": f"{player['first_name']} {player['last_name']}",
        "team_id": team["id"],
        "team": team["full_name"],

        "minutes": minutes,

        "pts": s.get("pts", 0),
        "reb": s.get("reb", 0),
        "ast": s.get("ast", 0),
        "stl": s.get("stl", 0),
        "blk": s.get("blk", 0),
        "turnover": s.get("turnover", 0),
        "pf": s.get("pf", 0),

        "fgm": s.get("fgm", 0),
        "fga": s.get("fga", 0),
        "fg3m": s.get("fg3m", 0),
        "fg3a": s.get("fg3a", 0),
        "ftm": s.get("ftm", 0),
        "fta": s.get("fta", 0),

        "plus_minus": s.get("plus_minus"),
        "data_quality": "RAW",
    }



# ======================================================
# ROUTES
# ======================================================
@app.route("/")
def health():
    return "🏀 NBA GOAT Player Stats Service (Python) is alive"


@app.route("/goat/player-stats-backfill")
def backfill_player_stats():
    start = request.args.get("start")
    end = request.args.get("end")

    if not start or not end:
        return jsonify({"error": "Missing start or end (YYYY-MM-DD)"}), 400

    games = fetch_games_range(start, end)
    rows = []

    for i in range(0, len(games), RATE["batch_size"]):
        batch = games[i:i + RATE["batch_size"]]

        for g in batch:
            game_id = g["id"]
        
            # -----------------------
            # FULL GAME STATS
            # -----------------------
            full_stats = fetch_goat_player_game_stats(game_id)
            for s in full_stats:
                row = map_stat_to_row(s, stat_period="FULL")
                if row:
                    rows.append(row)
        
            # -----------------------
            # Q1–Q4 STATS
            # -----------------------
            for period in (1, 2, 3, 4):
                q_stats = fetch_goat_player_game_stats(game_id, period=period)
                for s in q_stats:
                    row = map_stat_to_row(s, stat_period=f"Q{period}")
                    if row:
                        rows.append(row)
        
            delay(RATE["page_delay"])


    if rows:
        table_id = f"{PROJECT_ID}.{GOAT_DATASET}.{TABLE_PLAYER_STATS}"
        errors = bq.insert_rows_json(table_id, rows)
        if errors:
            return jsonify({"error": errors}), 500

    return jsonify({
        "status": "ok",
        "games": len(games),
        "rows_inserted": len(rows),
    })


@app.route("/goat/player-stats-refresh")
def refresh_player_stats():
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)

    return backfill_player_stats.__wrapped__(
        start=yesterday.isoformat(),
        end=today.isoformat(),
    )


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)