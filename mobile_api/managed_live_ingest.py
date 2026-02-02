"""
Managed Live Ingest - Controllable ingestion loops for NBA live data

This module provides ingestion loops that can be started/stopped by the orchestrator.
Each ingest loop runs every 60 seconds when active and stops when signaled.

Loops included:
1. Live Games Snapshot (BallDontLie -> nba_live.live_games)
2. Box Scores Snapshot (BallDontLie -> nba_live.box_scores_raw)
3. Player Stats Flatten (box scores -> nba_live.live_player_stats)
4. Live odds ingestion/flattening

Read-side helpers (no background loops):
- Live scores snapshot (BigQuery -> payload)
- Player box snapshot (BigQuery -> payload)
- Player stats snapshot (BigQuery -> payload)
"""

import asyncio
import json
import os
import requests
import traceback
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from google.cloud import bigquery

# ==================================================
# ODDS INGESTION IMPORTS
# ==================================================
from LiveOdds.live_game_odds_ingest import ingest_live_game_odds
from LiveOdds.live_player_prop_odds_ingest import ingest_live_player_prop_odds
from LiveOdds.live_odds_flatten import run_live_odds_orchestrator


# ======================================================
# Configuration
# ======================================================

NBA_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")

# Loop intervals (in seconds)
INGEST_INTERVAL_SEC = 60     # How often to run each ingest loop
BQ_TIMEOUT_SEC = 10          # BigQuery query timeout

# BallDontLie API
BDL_BASE = "https://api.balldontlie.io/v1"

# BigQuery tables
LIVE_GAMES_TABLE = "graphite-flare-477419-h7.nba_live.live_games"
BOX_SCORES_RAW_TABLE = "graphite-flare-477419-h7.nba_live.box_scores_raw"
LIVE_PLAYER_STATS_TABLE = "graphite-flare-477419-h7.nba_live.live_player_stats"
PLAYER_BOX_RAW_TABLE = "graphite-flare-477419-h7.nba_live.player_box_raw"

# Read-side cache TTLs (seconds)
LIVE_SCORES_CACHE_TTL_SEC = int(os.getenv("LIVE_SCORES_CACHE_TTL_SEC", "5"))
PLAYER_BOX_CACHE_TTL_SEC = int(os.getenv("PLAYER_BOX_CACHE_TTL_SEC", "5"))
PLAYER_STATS_CACHE_TTL_SEC = int(os.getenv("PLAYER_STATS_CACHE_TTL_SEC", "5"))

# Optional kill switch for player-stats writes from snapshots
ENABLE_PLAYER_STATS_INGEST = (
    os.getenv("ENABLE_PLAYER_STATS_INGEST", "false").lower() == "true"
)


# ======================================================
# Global Control State
# ======================================================

@dataclass
class IngestControl:
    """Control state for all ingest loops"""
    is_running: bool = False
    started_at: Optional[datetime] = None
    stopped_at: Optional[datetime] = None

    # Loop stats
    live_games_count: int = 0
    box_scores_count: int = 0
    live_stream_count: int = 0
    player_box_count: int = 0
    player_stats_count: int = 0

    # Error tracking
    consecutive_errors: int = 0


CONTROL = IngestControl()


# ======================================================
# BigQuery Client
# ======================================================

def get_bq_client() -> bigquery.Client:
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project:
        return bigquery.Client(project=project)
    return bigquery.Client()


# ======================================================
# BallDontLie API
# ======================================================

def get_bdl_headers() -> Dict[str, str]:
    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise RuntimeError("BALLDONTLIE_API_KEY is missing")
    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }


# ======================================================
# Helper: Get NBA "today" (handles midnight crossover)
# ======================================================

def nba_today() -> date:
    """
    Get the current NBA game date.
    Before 4 AM ET, we consider it still "yesterday" for games.
    """
    now = datetime.now(NBA_TZ)
    if now.hour < 4:
        return (now - timedelta(days=1)).date()
    return now.date()


# ======================================================
# LOOP 1: Live Games Snapshot
# ======================================================

def ingest_live_games_snapshot() -> int:
    """
    Fetch live games from BallDontLie and write to BigQuery.

    Returns:
        Number of games written
    """
    headers = get_bdl_headers()
    client = get_bq_client()

    today = nba_today()
    tomorrow = today + timedelta(days=1)

    # Fetch games for today and tomorrow (handles midnight crossover)
    resp = requests.get(
        f"{BDL_BASE}/games",
        params={"dates[]": [today.isoformat(), tomorrow.isoformat()]},
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()
    games = resp.json().get("data", [])

    # Fetch live box scores for accurate clock/period
    box_resp = requests.get(
        f"{BDL_BASE}/box_scores/live",
        headers=headers,
        timeout=15,
    )
    box_resp.raise_for_status()
    box_games = box_resp.json().get("data", [])
    box_by_id = {bg["id"]: bg for bg in box_games if bg.get("id")}

    now_iso = datetime.now(UTC_TZ).isoformat()
    rows = []

    for g in games:
        game_id = g["id"]

        # Determine game date (in EST)
        if g.get("date"):
            game_date = datetime.fromisoformat(g["date"]).date()
        elif g.get("datetime"):
            start_utc = datetime.fromisoformat(g["datetime"].replace("Z", "+00:00"))
            game_date = start_utc.astimezone(NBA_TZ).date()
        else:
            continue

        # Only include today's games (or games that started today and go past midnight)
        if game_date != today:
            # Check if this is a late game from today still running
            box = box_by_id.get(game_id, {})
            if box.get("time") != "Final" and box.get("period"):
                # Game still live, include it
                pass
            else:
                continue

        # Get state from box scores
        box = box_by_id.get(game_id, {})
        raw_time = box.get("time")
        raw_period = box.get("period")

        if raw_time == "Final":
            state = "FINAL"
        elif isinstance(raw_period, int) and raw_period >= 1:
            state = "LIVE"
        else:
            state = "UPCOMING"

        period = f"Q{raw_period}" if isinstance(raw_period, int) else None
        clock = raw_time if isinstance(raw_time, str) and raw_time != "Final" else None

        rows.append({
            "game_id": game_id,
            "game_date": game_date.isoformat(),
            "state": state,
            "home_team_abbr": g["home_team"]["abbreviation"],
            "away_team_abbr": g["visitor_team"]["abbreviation"],
            "home_score": g.get("home_team_score") or 0,
            "away_score": g.get("visitor_team_score") or 0,
            "home_score_q1": g.get("home_q1"),
            "home_score_q2": g.get("home_q2"),
            "home_score_q3": g.get("home_q3"),
            "home_score_q4": g.get("home_q4"),
            "away_score_q1": g.get("visitor_q1"),
            "away_score_q2": g.get("visitor_q2"),
            "away_score_q3": g.get("visitor_q3"),
            "away_score_q4": g.get("visitor_q4"),
            "period": period,
            "clock": clock,
            "poll_ts": now_iso,
            "ingested_at": now_iso,
        })

    if rows:
        # Truncate and insert
        client.query(f"TRUNCATE TABLE `{LIVE_GAMES_TABLE}`").result()

        errors = client.insert_rows_json(LIVE_GAMES_TABLE, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

    return len(rows)


# ======================================================
# LOOP 2: Box Scores Snapshot
# ======================================================

def ingest_box_scores_snapshot() -> int:
    """
    Fetch box scores from BallDontLie and write to BigQuery.

    Returns:
        Number of games with box scores
    """
    headers = get_bdl_headers()
    client = get_bq_client()

    # Fetch live box scores
    resp = requests.get(
        f"{BDL_BASE}/box_scores/live",
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()

    data = resp.json().get("data", [])

    if not data:
        return 0

    today = nba_today()
    now_iso = datetime.now(UTC_TZ).isoformat()

    row = {
        "snapshot_ts": now_iso,
        "game_date": today.isoformat(),
        "payload": json.dumps({"data": data}),
    }

    errors = client.insert_rows_json(BOX_SCORES_RAW_TABLE, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    return len(data)


# ======================================================
# LOOP 3: Flatten Player Stats (Write to live_player_stats)
# ======================================================

def flatten_player_stats_from_box_scores(
    box_data: List[Dict],
    game_date: Optional[date] = None,
    skip_if_disabled: bool = False,
) -> int:
    """
    Flatten player stats from box score data and write to BigQuery.

    Args:
        box_data: List of box score game dicts
        game_date: Optional override for game date
        skip_if_disabled: Respect ENABLE_PLAYER_STATS_INGEST flag

    Returns:
        Number of player rows written
    """
    if skip_if_disabled and not ENABLE_PLAYER_STATS_INGEST:
        return 0

    client = get_bq_client()
    if game_date is None:
        game_date = nba_today()
    now_iso = datetime.now(UTC_TZ).isoformat()

    rows = []

    for game in box_data:
        game_id = game.get("id")
        status = (game.get("status") or "").lower()
        period = game.get("period")
        clock = game.get("clock") or game.get("time")

        # Determine game state
        if status in ("scheduled", "pre"):
            game_state = "PRE"
        elif status in ("in_progress", "live"):
            game_state = "LIVE"
        elif status in ("final", "finished"):
            game_state = "FINAL"
        elif clock == "Final":
            game_state = "FINAL"
        elif isinstance(period, int) and period >= 1:
            game_state = "LIVE"
        else:
            game_state = "PRE"

        for team_key, opp_key in [("home_team", "visitor_team"), ("visitor_team", "home_team")]:
            team = game.get(team_key) or {}
            opponent = game.get(opp_key) or {}

            team_abbr = team.get("abbreviation")
            opponent_abbr = opponent.get("abbreviation")

            for p in team.get("players", []):
                player_meta = p.get("player", {})

                rows.append({
                    "game_id": game_id,
                    "game_date": game_date.isoformat(),
                    "game_state": game_state,
                    "player_id": player_meta.get("id"),
                    "player_name": f"{player_meta.get('first_name', '')} {player_meta.get('last_name', '')}".strip(),
                    "team_abbr": team_abbr,
                    "opponent_abbr": opponent_abbr,
                    "minutes": p.get("min"),
                    "pts": p.get("pts"),
                    "reb": p.get("reb"),
                    "ast": p.get("ast"),
                    "stl": p.get("stl"),
                    "blk": p.get("blk"),
                    "tov": p.get("turnover"),
                    "fg_made": p.get("fgm"),
                    "fg_att": p.get("fga"),
                    "fg3_made": p.get("fg3m"),
                    "fg3_att": p.get("fg3a"),
                    "ft_made": p.get("ftm"),
                    "ft_att": p.get("fta"),
                    "plus_minus": p.get("plus_minus"),
                    "period": period,
                    "clock": clock if clock != "Final" else None,
                    "ingested_at": now_iso,
                })

    if rows:
        errors = client.insert_rows_json(LIVE_PLAYER_STATS_TABLE, rows)
        if errors:
            raise RuntimeError(f"Player stats insert errors: {errors}")

    return len(rows)


# ======================================================
# Combined Ingest Cycle
# ======================================================

def run_full_ingest_cycle() -> Dict[str, int]:
    """
    Run one complete ingest cycle (all data sources).

    Returns:
        Dict with counts for each data type
    """
    results = {
        "live_games": 0,
        "box_scores": 0,
        "player_stats": 0,
        "game_odds": 0,
        "player_prop_odds": 0,
        "odds_flatten": 0,
    }

    # 1. Live games
    try:
        results["live_games"] = ingest_live_games_snapshot()
        print(f"[INGEST] Live games: {results['live_games']} games written")
    except Exception as e:
        print(f"[INGEST] ERROR in live_games: {e}")

    # 2. Box scores
    try:
        results["box_scores"] = ingest_box_scores_snapshot()
        print(f"[INGEST] Box scores: {results['box_scores']} games written")
    except Exception as e:
        print(f"[INGEST] ERROR in box_scores: {e}")

    # 3. Player stats (from box scores)
    try:
        # Fetch fresh box scores for flattening
        headers = get_bdl_headers()
        resp = requests.get(f"{BDL_BASE}/box_scores/live", headers=headers, timeout=15)
        resp.raise_for_status()
        box_data = resp.json().get("data", [])

        results["player_stats"] = flatten_player_stats_from_box_scores(box_data)
        print(f"[INGEST] Player stats: {results['player_stats']} rows written")
    except Exception as e:
        print(f"[INGEST] ERROR in player_stats: {e}")

    # 4. Live game odds
    try:
        ingest_live_game_odds()
        results["game_odds"] = 1
        print("[INGEST] Live game odds: snapshot written")
    except Exception as e:
        print(f"[INGEST] ERROR in game_odds: {e}")

    # 5. Live player prop odds
    try:
        ingest_live_player_prop_odds()
        results["player_prop_odds"] = 1
        print("[INGEST] Live player prop odds: snapshot written")
    except Exception as e:
        print(f"[INGEST] ERROR in player_prop_odds: {e}")

    # 6. Flatten odds (idempotent)
    try:
        run_live_odds_orchestrator()
        results["odds_flatten"] = 1
        print("[INGEST] Live odds flatten: complete")
    except Exception as e:
        print(f"[INGEST] ERROR in odds_flatten: {e}")

    return results


# ======================================================
# Async Ingest Loop
# ======================================================

async def managed_ingest_loop():
    """
    Main ingest loop that runs every 60 seconds when enabled.
    Checks CONTROL.is_running to determine if it should run.
    """
    global CONTROL

    print("\n[INGEST] Managed ingest loop initialized")
    print(f"[INGEST] Interval: {INGEST_INTERVAL_SEC} seconds")
    print("[INGEST] Waiting for orchestrator to start ingestion...\n")

    while True:
        try:
            # Check if we should be running
            if not CONTROL.is_running:
                await asyncio.sleep(5)  # Check every 5 seconds when idle
                continue

            # Run ingest cycle
            start_time = datetime.now(NBA_TZ)

            print(f"\n[INGEST] ========== INGEST CYCLE @ {start_time.strftime('%I:%M:%S %p ET')} ==========")

            results = await asyncio.to_thread(run_full_ingest_cycle)

            CONTROL.live_games_count += 1
            CONTROL.box_scores_count += 1
            CONTROL.player_stats_count += 1
            CONTROL.consecutive_errors = 0

            elapsed = (datetime.now(NBA_TZ) - start_time).total_seconds()
            print(f"[INGEST] Cycle complete in {elapsed:.1f}s")
            print(f"[INGEST] Total cycles: {CONTROL.live_games_count}")

            # Sleep for remaining interval
            sleep_time = max(0, INGEST_INTERVAL_SEC - elapsed)
            await asyncio.sleep(sleep_time)

        except Exception as e:
            CONTROL.consecutive_errors += 1
            print(f"[INGEST] ERROR in ingest loop: {e}")
            print(traceback.format_exc())

            # Exponential backoff on errors
            backoff = min(60, 2 ** CONTROL.consecutive_errors)
            await asyncio.sleep(backoff)


# ======================================================
# Control Functions (called by orchestrator)
# ======================================================

def start_managed_ingest() -> None:
    """Start the managed ingest loops"""
    global CONTROL

    if CONTROL.is_running:
        print("[INGEST] Already running")
        return

    CONTROL.is_running = True
    CONTROL.started_at = datetime.now(NBA_TZ)
    CONTROL.stopped_at = None

    print("\n" + "="*60)
    print("[INGEST] !! INGESTION STARTED !!")
    print(f"[INGEST] Start time: {CONTROL.started_at.strftime('%I:%M:%S %p ET')}")
    print("[INGEST] Running every 60 seconds")
    print("="*60 + "\n")


def stop_managed_ingest() -> None:
    """Stop the managed ingest loops"""
    global CONTROL

    if not CONTROL.is_running:
        print("[INGEST] Already stopped")
        return

    CONTROL.is_running = False
    CONTROL.stopped_at = datetime.now(NBA_TZ)

    duration = (CONTROL.stopped_at - CONTROL.started_at).total_seconds() / 60 if CONTROL.started_at else 0

    print("\n" + "="*60)
    print("[INGEST] !! INGESTION STOPPED !!")
    print(f"[INGEST] Stop time: {CONTROL.stopped_at.strftime('%I:%M:%S %p ET')}")
    print(f"[INGEST] Duration: {duration:.1f} minutes")
    print(f"[INGEST] Total cycles: {CONTROL.live_games_count}")
    print("="*60 + "\n")


def get_ingest_status() -> Dict[str, Any]:
    """Get current ingest status for debugging"""
    return {
        "is_running": CONTROL.is_running,
        "started_at": CONTROL.started_at.isoformat() if CONTROL.started_at else None,
        "stopped_at": CONTROL.stopped_at.isoformat() if CONTROL.stopped_at else None,
        "live_games_cycles": CONTROL.live_games_count,
        "box_scores_cycles": CONTROL.box_scores_count,
        "player_stats_cycles": CONTROL.player_stats_count,
        "consecutive_errors": CONTROL.consecutive_errors,
        "interval_seconds": INGEST_INTERVAL_SEC,
    }


# ======================================================
# Read-Side Cache States (on-demand)
# ======================================================

@dataclass
class LiveStreamState:
    """Cached state for live scores snapshots"""
    payload: Dict[str, Any] = None
    last_updated: Optional[datetime] = None
    last_attempt: Optional[datetime] = None
    consecutive_errors: int = 0
    last_error: Optional[str] = None

    def __post_init__(self):
        if self.payload is None:
            self.payload = {"games": [], "meta": {"status": "WAITING"}}


@dataclass
class PlayerBoxState:
    """Cached state for player box snapshots"""
    payload: Dict[str, Any] = None
    last_updated: Optional[datetime] = None
    last_attempt: Optional[datetime] = None
    consecutive_errors: int = 0
    last_error: Optional[str] = None
    game_date: Optional[date] = None

    def __post_init__(self):
        if self.payload is None:
            self.payload = {"games": [], "meta": {"status": "WAITING"}}


@dataclass
class PlayerStatsState:
    """Cached state for player stats snapshots"""
    payload: Dict[str, Any] = None
    last_updated: Optional[datetime] = None
    last_attempt: Optional[datetime] = None
    consecutive_errors: int = 0
    last_error: Optional[str] = None
    game_date: Optional[date] = None

    def __post_init__(self):
        if self.payload is None:
            self.payload = {"players": [], "meta": {"status": "WAITING"}}


# Global cache states
LIVE_STREAM_STATE = LiveStreamState()
PLAYER_BOX_STATE = PlayerBoxState()
PLAYER_STATS_STATE = PlayerStatsState()


# ======================================================
# Read-Side Queries
# ======================================================

LIVE_SCORES_QUERY = """
WITH ranked AS (
  SELECT
    game_id,
    home_team_abbr,
    away_team_abbr,
    home_score,
    away_score,
    home_score_q1,
    home_score_q2,
    home_score_q3,
    home_score_q4,
    away_score_q1,
    away_score_q2,
    away_score_q3,
    away_score_q4,
    period,
    clock,
    state,
    ingested_at,
    ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY ingested_at DESC) AS rn
  FROM `graphite-flare-477419-h7.nba_live.live_games`
  WHERE state = 'LIVE'
)
SELECT * FROM ranked WHERE rn = 1
ORDER BY ingested_at DESC
"""

LIVE_GAMES_LIST_QUERY = """
WITH ranked AS (
  SELECT
    game_id,
    home_team_abbr,
    away_team_abbr,
    home_score,
    away_score,
    period,
    clock,
    state,
    ingested_at,
    ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY ingested_at DESC) AS rn
  FROM `graphite-flare-477419-h7.nba_live.live_games`
)
SELECT
  r.game_id,
  r.home_team_abbr,
  r.away_team_abbr,
  r.home_score,
  r.away_score,
  r.period,
  r.clock,
  r.state,
  g.start_time_utc,
  DATETIME(g.start_time_utc, "America/New_York") AS start_time_et
FROM ranked r
LEFT JOIN `graphite-flare-477419-h7.nba_goat_data.games` g
  ON g.game_id = r.game_id
WHERE r.rn = 1
ORDER BY start_time_utc
"""

BOX_SCORES_QUERY = """
SELECT
  snapshot_ts,
  payload
FROM `graphite-flare-477419-h7.nba_live.box_scores_raw`
WHERE game_date = @game_date
ORDER BY snapshot_ts DESC
LIMIT 1
"""

PLAYER_STATS_QUERY = """
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY game_id, player_id
      ORDER BY ingested_at DESC
    ) AS rn
  FROM `graphite-flare-477419-h7.nba_live.live_player_stats`
  WHERE game_date = @game_date
)
SELECT *
FROM ranked
WHERE rn = 1
ORDER BY ingested_at DESC
"""


# ======================================================
# Read-Side Helpers (no background loops)
# ======================================================

def _is_cache_fresh(last_updated: Optional[datetime], ttl_sec: int) -> bool:
    if not last_updated:
        return False
    return (datetime.now(UTC_TZ) - last_updated).total_seconds() < ttl_sec


def _build_degraded_payload(state: Any, now: datetime) -> Dict[str, Any]:
    payload = dict(state.payload or {})
    meta = dict(payload.get("meta", {}))
    meta.update(
        {
            "status": "DEGRADED",
            "server_updated_at": now.isoformat(),
            "consecutive_failures": state.consecutive_errors,
            "seconds_since_last_good": (
                int((now - state.last_updated).total_seconds())
                if state.last_updated
                else None
            ),
            "last_error": state.last_error,
        }
    )
    payload["meta"] = meta
    return payload


def get_betting_day() -> date:
    """
    Returns the 'betting day' date.
    NBA games can run past midnight, so if it's before 3 AM ET,
    we use yesterday's date to catch late-finishing games.
    """
    now_et = datetime.now(NBA_TZ)
    if now_et.hour < 3:
        return (now_et - timedelta(days=1)).date()
    return now_et.date()


def fetch_live_scores_snapshot() -> Dict[str, Any]:
    client = get_bq_client()
    rows = list(client.query(LIVE_SCORES_QUERY).result())

    games: List[Dict[str, Any]] = []
    max_updated_at: Optional[datetime] = None

    for r in rows:
        updated_at = r.ingested_at
        if updated_at and updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        if updated_at and (max_updated_at is None or updated_at > max_updated_at):
            max_updated_at = updated_at

        games.append(
            {
                "game_id": r.game_id,
                "home_team": r.home_team_abbr,
                "away_team": r.away_team_abbr,
                "home_score": r.home_score,
                "away_score": r.away_score,
                "home_q": [r.home_score_q1, r.home_score_q2, r.home_score_q3, r.home_score_q4],
                "away_q": [r.away_score_q1, r.away_score_q2, r.away_score_q3, r.away_score_q4],
                "period": r.period,
                "clock": r.clock,
            }
        )

    return {
        "games": games,
        "source_updated_at": max_updated_at.isoformat() if max_updated_at else None,
    }


def get_live_scores_payload(force_refresh: bool = False) -> Dict[str, Any]:
    if (
        not force_refresh
        and _is_cache_fresh(LIVE_STREAM_STATE.last_updated, LIVE_SCORES_CACHE_TTL_SEC)
    ):
        return LIVE_STREAM_STATE.payload

    now = datetime.now(UTC_TZ)
    LIVE_STREAM_STATE.last_attempt = now

    try:
        snapshot = fetch_live_scores_snapshot()
        LIVE_STREAM_STATE.payload = {
            "games": snapshot.get("games", []),
            "meta": {
                "status": "OK",
                "server_updated_at": now.isoformat(),
                "source_updated_at": snapshot.get("source_updated_at"),
                "game_count": len(snapshot.get("games", [])),
            },
        }
        LIVE_STREAM_STATE.last_updated = now
        LIVE_STREAM_STATE.consecutive_errors = 0
        LIVE_STREAM_STATE.last_error = None
    except Exception as exc:
        LIVE_STREAM_STATE.consecutive_errors += 1
        LIVE_STREAM_STATE.last_error = str(exc)
        LIVE_STREAM_STATE.payload = _build_degraded_payload(LIVE_STREAM_STATE, now)

    return LIVE_STREAM_STATE.payload


def fetch_live_games_list() -> List[Dict[str, Any]]:
    client = get_bq_client()
    rows = list(client.query(LIVE_GAMES_LIST_QUERY).result())

    games: List[Dict[str, Any]] = []
    for r in rows:
        games.append(
            {
                "game_id": r.game_id,
                "home": r.home_team_abbr,
                "away": r.away_team_abbr,
                "home_score": r.home_score,
                "away_score": r.away_score,
                "period": r.period,
                "clock": r.clock,
                "start_time_et": r.start_time_et.isoformat() if r.start_time_et else None,
                "state": r.state,
            }
        )

    return games


def get_live_games_payload() -> Dict[str, Any]:
    games = fetch_live_games_list()
    return {"count": len(games), "games": games}


def fetch_player_box_snapshot(game_date: Optional[date] = None) -> Dict[str, Any]:
    if game_date is None:
        game_date = nba_today()

    client = get_bq_client()
    job = client.query(
        BOX_SCORES_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "DATE", game_date)
            ]
        ),
    )
    rows = list(job.result())

    if not rows:
        return {
            "games": [],
            "source_updated_at": None,
            "game_date": game_date.isoformat(),
        }

    row = rows[0]
    raw = row.payload
    payload = json.loads(raw) if isinstance(raw, str) else raw

    return {
        "games": payload.get("data", []),
        "source_updated_at": row.snapshot_ts.isoformat() if row.snapshot_ts else None,
        "game_date": game_date.isoformat(),
    }


def get_player_box_payload(
    game_date: Optional[date] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    if game_date is None:
        game_date = nba_today()

    if (
        not force_refresh
        and PLAYER_BOX_STATE.game_date == game_date
        and _is_cache_fresh(PLAYER_BOX_STATE.last_updated, PLAYER_BOX_CACHE_TTL_SEC)
    ):
        return PLAYER_BOX_STATE.payload

    now = datetime.now(UTC_TZ)
    PLAYER_BOX_STATE.last_attempt = now

    try:
        snapshot = fetch_player_box_snapshot(game_date)
        PLAYER_BOX_STATE.payload = {
            "games": snapshot.get("games", []),
            "meta": {
                "status": "OK",
                "server_updated_at": now.isoformat(),
                "source_updated_at": snapshot.get("source_updated_at"),
                "game_date": game_date.isoformat(),
                "game_count": len(snapshot.get("games", [])),
            },
        }
        PLAYER_BOX_STATE.last_updated = now
        PLAYER_BOX_STATE.game_date = game_date
        PLAYER_BOX_STATE.consecutive_errors = 0
        PLAYER_BOX_STATE.last_error = None
    except Exception as exc:
        PLAYER_BOX_STATE.consecutive_errors += 1
        PLAYER_BOX_STATE.last_error = str(exc)
        PLAYER_BOX_STATE.payload = _build_degraded_payload(PLAYER_BOX_STATE, now)

    return PLAYER_BOX_STATE.payload


def snapshot_player_box(
    game_date: Optional[date] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if game_date is None:
        game_date = nba_today()

    snapshot = fetch_player_box_snapshot(game_date)

    if not dry_run:
        client = get_bq_client()
        table_id = PLAYER_BOX_RAW_TABLE

        row = {
            "snapshot_ts": datetime.now(timezone.utc).isoformat(),
            "game_date": game_date.isoformat(),
            "payload": json.dumps(snapshot),
        }

        errors = client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        flatten_player_stats_from_box_scores(
            snapshot.get("games", []),
            game_date=game_date,
            skip_if_disabled=True,
        )

    return {
        "status": "OK",
        "game_date": str(game_date),
        "games": len(snapshot.get("games", [])),
        "dry_run": dry_run,
    }


def fetch_player_stats_snapshot(game_date: Optional[date] = None) -> Dict[str, Any]:
    if game_date is None:
        game_date = get_betting_day()

    client = get_bq_client()
    job = client.query(
        PLAYER_STATS_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "DATE", game_date)
            ]
        ),
    )
    rows = list(job.result())

    players: List[Dict[str, Any]] = []
    max_ingested_at: Optional[datetime] = None

    for r in rows:
        updated_at = r.ingested_at
        if updated_at and updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        if updated_at and (max_ingested_at is None or updated_at > max_ingested_at):
            max_ingested_at = updated_at

        players.append(
            {
                "game_id": r.game_id,
                "player_id": r.player_id,
                "name": r.player_name or "-",
                "team": r.team_abbr,
                "opponent": r.opponent_abbr,
                "minutes": float(r.minutes) if r.minutes is not None else None,
                "pts": r.pts or 0,
                "reb": r.reb or 0,
                "ast": r.ast or 0,
                "stl": r.stl or 0,
                "blk": r.blk or 0,
                "tov": r.tov or 0,
                "fg": [r.fg_made or 0, r.fg_att or 0],
                "fg3": [r.fg3_made or 0, r.fg3_att or 0],
                "ft": [r.ft_made or 0, r.ft_att or 0],
                "plus_minus": r.plus_minus or 0,
                "period": (
                    int(r.period)
                    if r.period and str(r.period).isdigit()
                    else None
                ),
                "clock": r.clock,
            }
        )

    return {
        "players": players,
        "source_updated_at": max_ingested_at.isoformat() if max_ingested_at else None,
        "game_date": game_date.isoformat(),
    }


def get_player_stats_payload(
    game_date: Optional[date] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    if game_date is None:
        game_date = get_betting_day()

    if (
        not force_refresh
        and PLAYER_STATS_STATE.game_date == game_date
        and _is_cache_fresh(PLAYER_STATS_STATE.last_updated, PLAYER_STATS_CACHE_TTL_SEC)
    ):
        return PLAYER_STATS_STATE.payload

    now = datetime.now(UTC_TZ)
    PLAYER_STATS_STATE.last_attempt = now

    try:
        snapshot = fetch_player_stats_snapshot(game_date)
        PLAYER_STATS_STATE.payload = {
            "players": snapshot.get("players", []),
            "meta": {
                "status": "OK",
                "server_updated_at": now.isoformat(),
                "source_updated_at": snapshot.get("source_updated_at"),
                "game_date": game_date.isoformat(),
                "player_count": len(snapshot.get("players", [])),
            },
        }
        PLAYER_STATS_STATE.last_updated = now
        PLAYER_STATS_STATE.game_date = game_date
        PLAYER_STATS_STATE.consecutive_errors = 0
        PLAYER_STATS_STATE.last_error = None
    except Exception as exc:
        PLAYER_STATS_STATE.consecutive_errors += 1
        PLAYER_STATS_STATE.last_error = str(exc)
        PLAYER_STATS_STATE.payload = _build_degraded_payload(PLAYER_STATS_STATE, now)

    return PLAYER_STATS_STATE.payload
