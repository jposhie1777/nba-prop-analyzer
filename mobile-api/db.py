# db.py
from google.cloud import bigquery
from time import time
from typing import List, Dict
from datetime import datetime, timezone
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple

# --------------------------------------------------
# BigQuery client (Cloud Run auto-auth)
# --------------------------------------------------
client = bigquery.Client()

# --------------------------------------------------
# Simple in-memory cache
# --------------------------------------------------
_CACHE: Dict[str, Tuple[float, List[Dict]]] = {}
_TTL_SECONDS = 60

# --------------------------------------------------
# Core fetch function (UNCHANGED)
# --------------------------------------------------
def fetch_mobile_props(
    *,
    game_date: str,
    min_hit_rate: float = 0.60,
    limit: int = 200,
    offset: int = 0,
) -> List[Dict]:
    """
    Fetch mobile-ready props from BigQuery.
    """
    cache_key = f"{game_date}:{min_hit_rate}:{limit}:{offset}"
    now = time()

    if cache_key in _CACHE:
        cached_at, data = _CACHE[cache_key]
        if now - cached_at < _TTL_SECONDS:
            return data

    query = """
    SELECT *
    FROM `nba_goat_data.props_mobile_v1`
    WHERE gameDate = @game_date
    AND hit_rate_l10 >= @min_hit_rate
    ORDER BY hit_rate_l10 DESC, edge_pct DESC
    LIMIT @limit
    OFFSET @offset
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "DATE", game_date),
                bigquery.ScalarQueryParameter("min_hit_rate", "FLOAT64", min_hit_rate),
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

def ingest_live_games_snapshot():
    """
    Poll BallDontLie games endpoint and snapshot games
    into nba_live.live_games.

    WRITE-ONLY.
    Called by Cloud Run background loop.
    """

    import os
    import requests
    from datetime import datetime
    from zoneinfo import ZoneInfo

    # --------------------------------------------------
    # Config
    # --------------------------------------------------
    BALLDONTLIE_API_KEY = os.environ.get("BALLDONTLIE_API_KEY")
    if not BALLDONTLIE_API_KEY:
        raise RuntimeError("BALLDONTLIE_API_KEY is missing")

    headers = {
        "Authorization": f"Bearer {BALLDONTLIE_API_KEY}",
    }

    NY_TZ = ZoneInfo("America/New_York")
    today = datetime.now(NY_TZ).date().isoformat()
    now_dt = datetime.now(tz=ZoneInfo("UTC"))
    now = now_dt.isoformat()

    # --------------------------------------------------
    # Fetch games
    # --------------------------------------------------
    from datetime import timedelta

    today_ny = datetime.now(NY_TZ).date()
    tomorrow_ny = today_ny + timedelta(days=1)

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
        print("ℹ️ No games returned from BallDontLie")
        return

    rows: List[Dict] = []
    # --------------------------------------------------
    # Normalize rows
    # --------------------------------------------------
    for g in games:
        status_raw = (g.get("status") or "").lower()

        # ---------------------------
        # Game state
        # ---------------------------
        home_score = g.get("home_team_score")
        away_score = g.get("visitor_team_score")

        if "final" in status_raw:
            state = "FINAL"
        elif home_score is not None and away_score is not None:
            state = "LIVE"
        else:
            state = "UPCOMING"


        # ---------------------------
        # Period
        # ---------------------------
        period = None
        if "q1" in status_raw:
            period = "Q1"
        elif "q2" in status_raw:
            period = "Q2"
        elif "q3" in status_raw:
            period = "Q3"
        elif "q4" in status_raw:
            period = "Q4"
        elif "ot" in status_raw:
            period = "OT"

        # ---------------------------
        # Clock (best-effort)
        # ---------------------------
        clock = None
        for token in status_raw.split():
            if ":" in token and len(token) <= 5:
                clock = token
                break

        rows.append(
            {
                "game_id": g["id"],
                "game_date": today,

                "state": state,

                "home_team_abbr": g["home_team"]["abbreviation"],
                "away_team_abbr": g["visitor_team"]["abbreviation"],

                # Quarter scores (nullable-safe)
                "home_score_q1": g.get("home_q1"),
                "home_score_q2": g.get("home_q2"),
                "home_score_q3": g.get("home_q3"),
                "home_score_q4": g.get("home_q4"),

                "away_score_q1": g.get("visitor_q1"),
                "away_score_q2": g.get("visitor_q2"),
                "away_score_q3": g.get("visitor_q3"),
                "away_score_q4": g.get("visitor_q4"),

                "home_score": g.get("home_team_score"),
                "away_score": g.get("visitor_team_score"),

                "period": period,
                "clock": clock,
                "wall_clock": now,

                # Ingestion metadata
                "poll_ts": now,
                "ingested_at": now,
            }
        )

    # --------------------------------------------------
    # Write snapshot
    # --------------------------------------------------
    if rows:
        print(f"📥 Ingesting {len(rows)} games for {today}")

        # Snapshot semantics (authoritative table)
        client.query("TRUNCATE TABLE `nba_live.live_games`").result()

        errors = client.insert_rows_json("nba_live.live_games", rows)

        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        print("✅ Live games snapshot updated successfully")