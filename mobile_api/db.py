#db.py
from google.cloud import bigquery
from time import time
from typing import Dict, List, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import requests

# --------------------------------------------------
# BigQuery client (LAZY INIT)
# --------------------------------------------------
from google.cloud import bigquery
import os

def get_bq_client() -> bigquery.Client:
    """
    Unified BigQuery client initializer.

    Works in:
    - Cloud Run (auto project)
    - Local dev / Codespaces (env-based)
    """

    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")

    if project:
        return bigquery.Client(project=project)

    # Cloud Run / gcloud auth fallback
    return bigquery.Client()


# --------------------------------------------------
# Simple in-memory cache
# --------------------------------------------------
_CACHE: Dict[str, Tuple[float, List[Dict]]] = {}
_TTL_SECONDS = 60

# --------------------------------------------------
# Core fetch function
# --------------------------------------------------
SUPPORTED_BOOKS = ("fanduel", "draftkings")

def fetch_mobile_props(
    *,
    game_date: str,
    limit: int = 200,
    offset: int = 0,
) -> List[Dict]:

    cache_key = f"{game_date}:{limit}:{offset}"
    now = time()

    if cache_key in _CACHE:
        cached_at, data = _CACHE[cache_key]
        if now - cached_at < _TTL_SECONDS:
            return data

    query = """
    SELECT
      p.*,
      l.player_image_url
    FROM `nba_goat_data.props_mobile_v1` p
    LEFT JOIN `nba_goat_data.player_lookup` l
      ON l.player_name = p.player
    WHERE p.gameDate = @game_date
      AND LOWER(p.book) IN UNNEST(@books)
    ORDER BY
      p.player,
      p.market,
      p.line
    LIMIT @limit
    OFFSET @offset
    """

    client = get_bq_client()
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "DATE", game_date),
                bigquery.ArrayQueryParameter(
                    "books", "STRING", list(SUPPORTED_BOOKS)
                ),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
                bigquery.ScalarQueryParameter("offset", "INT64", offset),
            ]
        ),
    )

    rows = [dict(row) for row in job.result()]
    _CACHE[cache_key] = (now, rows)
    return rows


# ======================================================
# 🔴 ADDITIVE: LIVE GAME INGESTION (NEW)
# ======================================================

BDL_BASE = "https://api.balldontlie.io/v1"

def ingest_live_games_snapshot() -> None:
    """
    Poll BallDontLie /games endpoint and snapshot games
    into nba_live.live_games.

    WRITE-ONLY.
    Safe for Cloud Run background execution.
    """

    # --------------------------------------------------
    # Imports (local-only to avoid import-time failures)
    # --------------------------------------------------
    import os
    import requests
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    # --------------------------------------------------
    # Config
    # --------------------------------------------------
    BALLDONTLIE_API_KEY = os.environ.get("BALLDONTLIE_API_KEY")
    if not BALLDONTLIE_API_KEY:
        raise RuntimeError("BALLDONTLIE_API_KEY is missing")

    BDL_BASE = "https://api.balldontlie.io/v1"
    NY_TZ = ZoneInfo("America/New_York")
    UTC_TZ = ZoneInfo("UTC")

    UTC_DAY_CUTOFF_HOUR = 6  # ✅ allow early-morning UTC games to count as NY "today"

    client = get_bq_client()

    headers = {
        "Authorization": f"Bearer {BALLDONTLIE_API_KEY}",
        "Accept": "application/json",
    }

    # --------------------------------------------------
    # Fetch LIVE BOX SCORES (authoritative clock)
    # --------------------------------------------------
    box_resp = requests.get(
        f"{BDL_BASE}/box_scores/live",
        headers=headers,
        timeout=15,
    )
    box_resp.raise_for_status()

    box_games = box_resp.json().get("data", [])

    box_by_game_id = {
        bg.get("id"): bg
        for bg in box_games
        if bg.get("id") is not None
    }

    # --------------------------------------------------
    # Date window (NY authoritative)
    # --------------------------------------------------
    today_ny = datetime.now(NY_TZ).date()
    tomorrow_ny = today_ny + timedelta(days=1)

    poll_ts = datetime.now(UTC_TZ).isoformat()

    # --------------------------------------------------
    # Fetch games
    # --------------------------------------------------
    resp = requests.get(
        f"{BDL_BASE}/games",
        params={
            "dates[]": [
                today_ny.isoformat(),
                tomorrow_ny.isoformat(),
            ]
        },
        headers=headers,
        timeout=15,
    )

    resp.raise_for_status()
    games = resp.json().get("data", [])

    if not games:
        print("ℹ️ ingest_live_games_snapshot: no games returned")
        return

    # --------------------------------------------------
    # Normalize rows
    # --------------------------------------------------
    rows = []

    for g in games:
        # ---------------------------
        # ✅ Determine authoritative NY game date
        # ---------------------------
        if g.get("date"):
            # BallDontLie date is already the correct game day
            game_date_ny = datetime.fromisoformat(g["date"]).date()
        elif g.get("start_time"):
            start_utc = datetime.fromisoformat(
                g["start_time"].replace("Z", "+00:00")
            )
            game_date_ny = start_utc.astimezone(NY_TZ).date()
        else:
            continue


        # ❌ Skip tomorrow’s games
        if game_date_ny != today_ny:
            continue

        home_score = g.get("home_team_score")
        away_score = g.get("visitor_team_score")

        # ---------------------------
        # Box score lookup (authoritative)
        # ---------------------------
        box = box_by_game_id.get(g["id"], {})

        raw_period = box.get("period")
        raw_time = box.get("time")

        # ---------------------------
        # Game state
        # ---------------------------
        if raw_time == "Final":
            state = "FINAL"
        elif isinstance(raw_period, int) and raw_period >= 1:
            state = "LIVE"
        else:
            state = "UPCOMING"


        # ---------------------------
        # Period + Clock
        # ---------------------------
        period = (
            f"Q{raw_period}"
            if isinstance(raw_period, int)
            else None
        )

        clock = raw_time if isinstance(raw_time, str) else None

        rows.append(
            {
                "game_id": g["id"],
                "game_date": game_date_ny.isoformat(),  # ✅ FIXED

                "state": state,

                "home_team_abbr": g["home_team"]["abbreviation"],
                "away_team_abbr": g["visitor_team"]["abbreviation"],

                "home_score_q1": g.get("home_q1"),
                "home_score_q2": g.get("home_q2"),
                "home_score_q3": g.get("home_q3"),
                "home_score_q4": g.get("home_q4"),

                "away_score_q1": g.get("visitor_q1"),
                "away_score_q2": g.get("visitor_q2"),
                "away_score_q3": g.get("visitor_q3"),
                "away_score_q4": g.get("visitor_q4"),

                "home_score": home_score,
                "away_score": away_score,

                "period": period,
                "clock": clock,

                "poll_ts": poll_ts,
                "ingested_at": poll_ts,
            }
        )

    # --------------------------------------------------
    # Write snapshot (authoritative)
    # --------------------------------------------------
    print(f"📥 ingest_live_games_snapshot: writing {len(rows)} games")

    client.query(
        "TRUNCATE TABLE `nba_live.live_games`"
    ).result()

    errors = client.insert_rows_json(
        "nba_live.live_games",
        rows,
    )

    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    print("✅ ingest_live_games_snapshot: snapshot updated")
