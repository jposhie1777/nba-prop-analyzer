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
    "Authorization": API_KEY
}

# ======================================================
# HELPERS
# ======================================================
def delay(seconds: float):
    time.sleep(seconds)


def normalize_stat_period(p):
    if not p:
        return "FULL"

    p = str(p).lower()
    if p in ("1", "q1", "quarter_1"):
        return "Q1"
    if p in ("2", "q2", "quarter_2"):
        return "Q2"
    if p in ("first_half", "h1"):
        return "H1"
    if p in ("second_half", "h2"):
        return "H2"
    if p in ("full", "full_game"):
        return "FULL"

    return "FULL"


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


def fetch_goat_player_game_stats(game_id: int):
    params = {
        "game_ids[]": game_id,
        "per_page": 100,
    }

    r = requests.get(
        f"{BALDONTLIE_BASE}/player_game_stats",
        headers=HEADERS,
        params=params,
        timeout=20,
    )

    if r.status_code == 429:
        delay(10)
        return []

    if not r.ok:
        return []

    return r.json().get("data", [])


def map_stat_to_row(s, stat_period="FULL"):
    if not s or not s.get("player") or not s.get("game"):
        return None

    # Accept ANY stat row that exists
    # Do NOT require points, minutes, or scoring

    player = s.get("player", {})
    team = s.get("team", {})
    game = s.get("game", {})

    # Minutes: keep as INT when possible, else NULL
    try:
        minutes = int(s.get("min")) if s.get("min") is not None else None
    except ValueError:
        minutes = None

    return {
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        "season": game.get("season"),
        "stat_period": stat_period,   # FULL / Q1 / H1

        "player_id": player.get("id"),
        "player": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "team_id": team.get("id"),
        "team": team.get("full_name"),

        "minutes": minutes,

        # Core stats (NULL-safe)
        "pts": s.get("pts", 0),
        "reb": s.get("reb", 0),
        "ast": s.get("ast", 0),
        "stl": s.get("stl", 0),
        "blk": s.get("blk", 0),
        "turnover": s.get("turnover", 0),
        "pf": s.get("pf", 0),

        # Shooting
        "fgm": s.get("fgm", 0),
        "fga": s.get("fga", 0),
        "fg3m": s.get("fg3m", 0),
        "fg3a": s.get("fg3a", 0),
        "ftm": s.get("ftm", 0),
        "fta": s.get("fta", 0),

        # Meta
        "plus_minus": s.get("plus_minus"),
        "data_quality": "RAW"
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
            stats = fetch_goat_player_game_stats(g["id"])
            for s in stats:
                row = map_stat_to_row(s, stat_period="FULL")
                print("ROW BUILT:", row is not None)
                if row:
                    rows.append(row)


        delay(RATE["batch_delay"])

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