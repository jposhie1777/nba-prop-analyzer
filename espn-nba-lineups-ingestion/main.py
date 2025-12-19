import os
from datetime import datetime, timezone
import requests
from flask import Flask, request, jsonify
from google.cloud import bigquery

# =========================
# CONFIG
# =========================
BQ_DATASET = "nba_prop_analyzer"
BQ_TABLE = "nba_lineups"
SOURCE = "ESPN"

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}"

app = Flask(__name__)

# =========================
# HELPERS
# =========================
def utc_now():
    return datetime.now(timezone.utc)

def fetch_json(url: str) -> dict:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def extract_rows(summary: dict, fetched_at: datetime) -> list[dict]:
    rows = []

    header = summary.get("header", {})
    event_id = str(header.get("id", ""))

    competitions = header.get("competitions") or []
    comp = competitions[0] if competitions else {}
    date_str = comp.get("date")

    game_time = None
    game_date = None
    if isinstance(date_str, str):
        try:
            game_time = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            game_date = game_time.date()
        except Exception:
            pass

    teams = summary.get("boxscore", {}).get("teams", []) or []

    for t in teams:
        team_info = t.get("team", {})
        team_id = str(team_info.get("id", ""))
        team_name = team_info.get("displayName", "")

        for p in t.get("players", []) or []:
            athlete = p.get("athlete", {})
            status_obj = athlete.get("status") or {}

            rows.append({
                "event_id": event_id,
                "game_date": str(game_date) if game_date else None,
                "game_time_utc": game_time.isoformat() if game_time else None,
                "team_id": team_id,
                "team": team_name,
                "player_id": str(athlete.get("id", "")),
                "player": athlete.get("displayName", ""),
                "position": (athlete.get("position") or {}).get("abbreviation", ""),
                "is_starter": bool(p.get("starter", False)),
                "status": status_obj.get("type", "") if isinstance(status_obj, dict) else "",
                "source": SOURCE,
                "fetched_at": fetched_at.isoformat(),
            })

    return rows

def write_to_bq(rows: list[dict]) -> int:
    if not rows:
        return 0

    client = bigquery.Client()
    table_id = f"{client.project}.{BQ_DATASET}.{BQ_TABLE}"

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(errors)

    return len(rows)

# =========================
# MAIN INGEST
# =========================
def run_ingest(limit_events: int | None = None) -> dict:
    fetched_at = utc_now()
    scoreboard = fetch_json(SCOREBOARD_URL)

    events = scoreboard.get("events", []) or []
    if limit_events:
        events = events[:limit_events]

    all_rows = []

    for e in events:
        event_id = e.get("id")
        if not event_id:
            continue
        summary = fetch_json(SUMMARY_URL.format(event_id=event_id))
        all_rows.extend(extract_rows(summary, fetched_at))

    inserted = write_to_bq(all_rows)

    return {
        "events": len(events),
        "rows_inserted": inserted,
        "fetched_at": fetched_at.isoformat()
    }

# =========================
# ROUTES
# =========================
@app.route("/health")
def health():
    return "ok", 200

@app.route("/run")
def run():
    limit = request.args.get("limit_events")
    limit_int = int(limit) if limit and limit.isdigit() else None
    return jsonify(run_ingest(limit_int)), 200