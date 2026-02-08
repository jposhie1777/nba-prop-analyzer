import os
import time
import requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.auth import default as google_auth_default

# ======================================================
# CONFIG
# ======================================================
PROJECT_ID = "graphite-flare-477419-h7"
GOAT_DATASET = "nba_goat_data"
TABLE_PLAYER_STATS = "player_game_stats"

BALDONTLIE_BASE = "https://api.balldontlie.io/v1"
API_KEY = (
    os.getenv("BALDONTLIE_KEY")
    or os.getenv("BALLDONTLIE_API_KEY")
    or ""
)
if not API_KEY:
    print("⚠️ BallDontLie API key missing (set BALDONTLIE_KEY or BALLDONTLIE_API_KEY)")

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


def map_goat_stat_row(s: dict):
    if not s.get("game") or not s.get("player") or not s.get("team"):
        return None

    if not s.get("minutes") or s.get("minutes") == 0:
        return None

    return {
        "game_id": s["game"]["id"],
        "player_id": s["player"]["id"],
        "team_id": s["team"]["id"],
        "stat_period": normalize_stat_period(s.get("period")),
        "minutes": s.get("minutes"),
        "points": s.get("points", 0),
        "rebounds": s.get("rebounds", 0),
        "assists": s.get("assists", 0),
        "steals": s.get("steals", 0),
        "blocks": s.get("blocks", 0),
        "turnovers": s.get("turnovers", 0),
        "fouls": s.get("fouls", 0),
        "fg_made": s.get("field_goals_made", 0),
        "fg_attempts": s.get("field_goals_attempted", 0),
        "fg3_made": s.get("three_point_field_goals_made", 0),
        "fg3_attempts": s.get("three_point_field_goals_attempted", 0),
        "ft_made": s.get("free_throws_made", 0),
        "ft_attempts": s.get("free_throws_attempted", 0),
        "fetched_at": datetime.utcnow(),
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
                row = map_goat_stat_row(s)
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