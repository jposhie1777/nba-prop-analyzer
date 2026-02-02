"""
Stater Game Orchestrator - Intelligent NBA game session manager

This module:
1. Fetches today's games at 5-6 AM EST from BallDontLie API
2. Writes schedule to nba_live.live_games
3. Triggers ingestion when games go LIVE (or pre-game lead time)
4. Runs ingestion on the managed ingest interval during live games
5. Stops when ALL games are FINAL
6. Handles midnight crossover (games don't cut off at day change)

Console debug lines included for Cloud Run monitoring.
"""

import asyncio
import os
import requests
import traceback
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, date
from typing import Any, Dict, List, Optional, Set, Callable
from zoneinfo import ZoneInfo

from google.cloud import bigquery


# ======================================================
# Configuration
# ======================================================

NBA_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")

# When to fetch daily schedule (5:30 AM ET)
SCHEDULE_FETCH_HOUR = 5
SCHEDULE_FETCH_MINUTE = 30

# How many minutes before first game to start ingestion
def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value >= 0 else default
    except ValueError:
        return default


PRE_GAME_LEAD_MINUTES = _read_int_env("LIVE_INGEST_PRE_GAME_MINUTES", 0)

# How many minutes after last game goes FINAL to keep polling
POST_FINAL_GRACE_MINUTES = 10

# BallDontLie API
BDL_BASE = "https://api.balldontlie.io/v1"

# BigQuery tables
LIVE_GAMES_TABLE = "graphite-flare-477419-h7.nba_live.live_games"


# ======================================================
# Data Classes
# ======================================================

@dataclass
class ScheduledGame:
    """A single scheduled game"""
    game_id: int
    game_date: str              # YYYY-MM-DD in EST
    start_time_utc: datetime    # Actual start time in UTC
    start_time_est: datetime    # Converted to EST
    home_team: str
    away_team: str

    # Runtime state
    state: str = "UPCOMING"     # UPCOMING, LIVE, FINAL
    last_checked: Optional[datetime] = None

    def __str__(self) -> str:
        return f"{self.away_team}@{self.home_team} {self.start_time_est.strftime('%I:%M %p ET')} [{self.state}]"


@dataclass
class GameSession:
    """
    Represents a single game session (may span midnight).
    A session starts when we fetch the schedule and ends when all games are FINAL.
    """
    session_date: str           # The EST date when session started
    games: Dict[int, ScheduledGame] = field(default_factory=dict)

    # Session state
    schedule_fetched_at: Optional[datetime] = None
    ingestion_started: bool = False
    ingestion_started_at: Optional[datetime] = None
    session_ended: bool = False
    session_ended_at: Optional[datetime] = None

    def first_game_time(self) -> Optional[datetime]:
        """Get the earliest game start time"""
        if not self.games:
            return None
        return min(g.start_time_est for g in self.games.values())

    def last_game_time(self) -> Optional[datetime]:
        """Get the latest game start time"""
        if not self.games:
            return None
        return max(g.start_time_est for g in self.games.values())

    def all_games_final(self) -> bool:
        """Check if all games are FINAL"""
        if not self.games:
            return True
        return all(g.state == "FINAL" for g in self.games.values())

    def any_games_live(self) -> bool:
        """Check if any games are currently LIVE"""
        return any(g.state == "LIVE" for g in self.games.values())

    def games_started_or_live(self) -> int:
        """Count games that have started"""
        return sum(1 for g in self.games.values() if g.state in ("LIVE", "FINAL"))

    def summary(self) -> str:
        states = {"UPCOMING": 0, "LIVE": 0, "FINAL": 0}
        for g in self.games.values():
            states[g.state] = states.get(g.state, 0) + 1
        return f"Games: {len(self.games)} | UPCOMING:{states['UPCOMING']} LIVE:{states['LIVE']} FINAL:{states['FINAL']}"


# ======================================================
# Global State
# ======================================================

# Current active session
CURRENT_SESSION: Optional[GameSession] = None

# Ingestion control flag
INGESTION_ACTIVE = False

# Callbacks for starting/stopping ingestion
_start_ingestion_callback: Optional[Callable] = None
_stop_ingestion_callback: Optional[Callable] = None


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


def fetch_games_from_api(game_date: str) -> List[Dict[str, Any]]:
    """
    Fetch games for a specific date from BallDontLie API.

    Args:
        game_date: Date in YYYY-MM-DD format

    Returns:
        List of game dictionaries from API
    """
    headers = get_bdl_headers()

    resp = requests.get(
        f"{BDL_BASE}/games",
        params={"dates[]": [game_date]},
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()

    return resp.json().get("data", [])


def fetch_live_box_scores() -> Dict[int, Dict[str, Any]]:
    """
    Fetch current live box scores to get accurate game state.

    Returns:
        Dict mapping game_id to box score data
    """
    headers = get_bdl_headers()

    resp = requests.get(
        f"{BDL_BASE}/box_scores/live",
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()

    games = resp.json().get("data", [])
    return {g["id"]: g for g in games if g.get("id")}


# ======================================================
# Schedule Fetching & Writing
# ======================================================

def fetch_and_write_daily_schedule(game_date: str) -> GameSession:
    """
    Fetch today's games from BallDontLie and write to BigQuery.

    Args:
        game_date: Date in YYYY-MM-DD format (EST)

    Returns:
        GameSession with all scheduled games
    """
    print(f"\n{'='*60}")
    print(f"[ORCHESTRATOR] FETCHING SCHEDULE FOR {game_date}")
    print(f"{'='*60}")

    # Fetch from API
    raw_games = fetch_games_from_api(game_date)

    if not raw_games:
        print(f"[ORCHESTRATOR] NO GAMES FOUND FOR {game_date}")
        session = GameSession(session_date=game_date)
        session.schedule_fetched_at = datetime.now(NBA_TZ)
        return session

    print(f"[ORCHESTRATOR] FOUND {len(raw_games)} GAMES")

    # Also fetch live scores to get current state
    try:
        live_scores = fetch_live_box_scores()
    except Exception as e:
        print(f"[ORCHESTRATOR] Warning: Could not fetch live scores: {e}")
        live_scores = {}

    # Build session
    session = GameSession(session_date=game_date)
    session.schedule_fetched_at = datetime.now(NBA_TZ)

    # Prepare BQ rows
    bq_rows = []
    now_iso = datetime.now(UTC_TZ).isoformat()

    for g in raw_games:
        game_id = g["id"]

        # Parse start time
        start_time_str = g.get("datetime") or g.get("start_time")
        if start_time_str:
            start_time_utc = datetime.fromisoformat(
                start_time_str.replace("Z", "+00:00")
            ).astimezone(UTC_TZ)
        else:
            # Fallback: use date at noon
            start_time_utc = datetime.fromisoformat(f"{game_date}T12:00:00-05:00").astimezone(UTC_TZ)

        start_time_est = start_time_utc.astimezone(NBA_TZ)

        home_team = g["home_team"]["abbreviation"]
        away_team = g["visitor_team"]["abbreviation"]

        # Determine state from live scores
        live_data = live_scores.get(game_id, {})
        raw_time = live_data.get("time")
        raw_period = live_data.get("period")

        if raw_time == "Final":
            state = "FINAL"
        elif isinstance(raw_period, int) and raw_period >= 1:
            state = "LIVE"
        else:
            state = "UPCOMING"

        # Period & clock
        period = f"Q{raw_period}" if isinstance(raw_period, int) else None
        clock = raw_time if isinstance(raw_time, str) and raw_time != "Final" else None

        scheduled_game = ScheduledGame(
            game_id=game_id,
            game_date=game_date,
            start_time_utc=start_time_utc,
            start_time_est=start_time_est,
            home_team=home_team,
            away_team=away_team,
            state=state,
        )

        session.games[game_id] = scheduled_game

        print(f"[ORCHESTRATOR]   -> {scheduled_game}")

        # Build BQ row
        bq_rows.append({
            "game_id": game_id,
            "game_date": game_date,
            "state": state,
            "home_team_abbr": home_team,
            "away_team_abbr": away_team,
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

    # Write to BigQuery
    if bq_rows:
        print(f"\n[ORCHESTRATOR] WRITING {len(bq_rows)} GAMES TO BIGQUERY")
        client = get_bq_client()

        # Truncate and insert
        client.query(f"TRUNCATE TABLE `{LIVE_GAMES_TABLE}`").result()

        errors = client.insert_rows_json(LIVE_GAMES_TABLE, bq_rows)
        if errors:
            print(f"[ORCHESTRATOR] ERROR: BigQuery insert failed: {errors}")
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        print("[ORCHESTRATOR] ========================================")
        print("[ORCHESTRATOR] GAMES WRITTEN TO BIGQUERY SUCCESSFULLY")
        print("[ORCHESTRATOR] ========================================")

    return session


def update_game_states(session: GameSession) -> None:
    """
    Poll current game states from API and update session.
    """
    if not session.games:
        return

    try:
        live_scores = fetch_live_box_scores()
    except Exception as e:
        print(f"[ORCHESTRATOR] Warning: Could not fetch live scores: {e}")
        return

    # Also fetch full game data for accurate state
    try:
        raw_games = fetch_games_from_api(session.session_date)
        games_by_id = {g["id"]: g for g in raw_games}
    except Exception as e:
        print(f"[ORCHESTRATOR] Warning: Could not fetch games: {e}")
        games_by_id = {}

    now = datetime.now(NBA_TZ)
    changes = []

    for game_id, game in session.games.items():
        old_state = game.state

        live_data = live_scores.get(game_id, {})
        raw_time = live_data.get("time")
        raw_period = live_data.get("period")

        if raw_time == "Final":
            game.state = "FINAL"
        elif isinstance(raw_period, int) and raw_period >= 1:
            game.state = "LIVE"
        elif game_id in games_by_id:
            # Check if game has started based on scores
            g = games_by_id[game_id]
            if (g.get("home_team_score") or 0) > 0 or (g.get("visitor_team_score") or 0) > 0:
                game.state = "LIVE"

        game.last_checked = now

        if game.state != old_state:
            changes.append(f"{game.away_team}@{game.home_team}: {old_state} -> {game.state}")

    if changes:
        print(f"\n[ORCHESTRATOR] STATE CHANGES:")
        for change in changes:
            print(f"[ORCHESTRATOR]   -> {change}")


# ======================================================
# Ingestion Control
# ======================================================

def register_ingestion_callbacks(
    start_callback: Callable,
    stop_callback: Callable,
) -> None:
    """
    Register callbacks to start/stop ingestion loops.
    Called from test_main_scheduled.py
    """
    global _start_ingestion_callback, _stop_ingestion_callback
    _start_ingestion_callback = start_callback
    _stop_ingestion_callback = stop_callback
    print("[ORCHESTRATOR] Ingestion callbacks registered")


def start_ingestion() -> None:
    """Signal ingestion loops to start"""
    global INGESTION_ACTIVE

    if INGESTION_ACTIVE:
        return

    INGESTION_ACTIVE = True

    print("\n" + "="*60)
    print("[ORCHESTRATOR] !! FIRING UP INGESTION - GAMES SOON !!")
    print("="*60 + "\n")

    if _start_ingestion_callback:
        _start_ingestion_callback()


def stop_ingestion() -> None:
    """Signal ingestion loops to stop"""
    global INGESTION_ACTIVE

    if not INGESTION_ACTIVE:
        return

    INGESTION_ACTIVE = False

    print("\n" + "="*60)
    print("[ORCHESTRATOR] !! STOPPING INGESTION - ALL GAMES FINAL !!")
    print("="*60 + "\n")

    if _stop_ingestion_callback:
        _stop_ingestion_callback()


def is_ingestion_active() -> bool:
    """Check if ingestion should be running"""
    return INGESTION_ACTIVE


# ======================================================
# Main Orchestrator Loop
# ======================================================

async def orchestrator_loop():
    """
    Main orchestrator loop that manages game sessions.

    Flow:
    1. Wait until 5:30 AM EST
    2. Fetch schedule and write to BQ
    3. Wait until 15 min before first game
    4. Start ingestion
    5. Monitor game states every 30 seconds
    6. Stop ingestion when all games FINAL + grace period
    7. Sleep until next day's 5:30 AM
    """
    global CURRENT_SESSION

    print("\n" + "="*60)
    print("[ORCHESTRATOR] SMART GAME ORCHESTRATOR STARTING")
    print(f"[ORCHESTRATOR] Schedule fetch time: {SCHEDULE_FETCH_HOUR}:{SCHEDULE_FETCH_MINUTE:02d} AM EST")
    if PRE_GAME_LEAD_MINUTES > 0:
        print(f"[ORCHESTRATOR] Pre-game lead time: {PRE_GAME_LEAD_MINUTES} minutes")
    else:
        print("[ORCHESTRATOR] Start mode: LIVE-only (no pre-game lead)")
    print("="*60 + "\n")

    while True:
        try:
            now = datetime.now(NBA_TZ)
            today_str = now.date().isoformat()

            # ==========================================
            # STATE 1: No session - wait for schedule fetch time
            # ==========================================
            if CURRENT_SESSION is None or CURRENT_SESSION.session_date != today_str:

                schedule_time = now.replace(
                    hour=SCHEDULE_FETCH_HOUR,
                    minute=SCHEDULE_FETCH_MINUTE,
                    second=0,
                    microsecond=0,
                )

                # If it's before schedule time, wait
                if now < schedule_time:
                    wait_seconds = (schedule_time - now).total_seconds()
                    print(f"[ORCHESTRATOR] Waiting for schedule fetch time ({schedule_time.strftime('%I:%M %p ET')})")
                    print(f"[ORCHESTRATOR] Sleeping {wait_seconds/60:.1f} minutes...")
                    await asyncio.sleep(min(wait_seconds, 300))  # Check every 5 min max
                    continue

                # Time to fetch schedule
                try:
                    CURRENT_SESSION = await asyncio.to_thread(
                        fetch_and_write_daily_schedule,
                        today_str
                    )

                    if not CURRENT_SESSION.games:
                        print(f"[ORCHESTRATOR] No games today. Sleeping until tomorrow.")
                        # Sleep until tomorrow's fetch time
                        tomorrow = now.date() + timedelta(days=1)
                        next_fetch = datetime.combine(
                            tomorrow,
                            time(SCHEDULE_FETCH_HOUR, SCHEDULE_FETCH_MINUTE),
                            tzinfo=NBA_TZ,
                        )
                        await asyncio.sleep((next_fetch - now).total_seconds())
                        continue

                except Exception as e:
                    print(f"[ORCHESTRATOR] ERROR fetching schedule: {e}")
                    print(traceback.format_exc())
                    await asyncio.sleep(300)  # Retry in 5 min
                    continue

            # ==========================================
            # STATE 2: Have session - manage ingestion
            # ==========================================
            session = CURRENT_SESSION
            now = datetime.now(NBA_TZ)

            # Check if session is complete (all games final + grace)
            if session.session_ended:
                # Sleep until next day
                tomorrow = now.date() + timedelta(days=1)
                next_fetch = datetime.combine(
                    tomorrow,
                    time(SCHEDULE_FETCH_HOUR, SCHEDULE_FETCH_MINUTE),
                    tzinfo=NBA_TZ,
                )
                sleep_seconds = (next_fetch - now).total_seconds()
                print(f"[ORCHESTRATOR] Session complete. Next fetch: {next_fetch.strftime('%Y-%m-%d %I:%M %p ET')}")
                await asyncio.sleep(min(sleep_seconds, 3600))
                continue

            # ==========================================
            # STATE 2a: Check if time to start ingestion
            # ==========================================
            if not session.ingestion_started:
                first_game = session.first_game_time()
                start_ingestion_at = (
                    first_game - timedelta(minutes=PRE_GAME_LEAD_MINUTES)
                    if first_game
                    else None
                )

                # Refresh game states so we can start when games actually go LIVE
                await asyncio.to_thread(update_game_states, session)

                if session.any_games_live():
                    session.ingestion_started = True
                    session.ingestion_started_at = now
                    start_ingestion()
                elif PRE_GAME_LEAD_MINUTES > 0 and start_ingestion_at and now >= start_ingestion_at:
                    session.ingestion_started = True
                    session.ingestion_started_at = now
                    start_ingestion()
                else:
                    if first_game:
                        if PRE_GAME_LEAD_MINUTES > 0 and start_ingestion_at:
                            wait_seconds = (start_ingestion_at - now).total_seconds()
                            print(f"[ORCHESTRATOR] First game at {first_game.strftime('%I:%M %p ET')}")
                            print(f"[ORCHESTRATOR] Starting ingestion at {start_ingestion_at.strftime('%I:%M %p ET')} ({wait_seconds/60:.1f} min)")
                        else:
                            wait_seconds = 60
                            print(f"[ORCHESTRATOR] First game at {first_game.strftime('%I:%M %p ET')}")
                            print("[ORCHESTRATOR] Waiting for games to go LIVE...")
                    else:
                        wait_seconds = 60
                        print("[ORCHESTRATOR] Waiting for games to go LIVE...")

                    await asyncio.sleep(min(wait_seconds, 60))
                    continue

            # ==========================================
            # STATE 2b: Ingestion active - monitor games
            # ==========================================
            if session.ingestion_started and not session.session_ended:
                # Update game states
                await asyncio.to_thread(update_game_states, session)

                print(f"[ORCHESTRATOR] {session.summary()}")

                # Check if all games are final
                if session.all_games_final():
                    # Grace period before stopping
                    if not hasattr(session, '_final_detected_at'):
                        session._final_detected_at = now
                        print(f"[ORCHESTRATOR] All games FINAL. Grace period: {POST_FINAL_GRACE_MINUTES} min")

                    grace_end = session._final_detected_at + timedelta(minutes=POST_FINAL_GRACE_MINUTES)

                    if now >= grace_end:
                        session.session_ended = True
                        session.session_ended_at = now
                        stop_ingestion()
                        continue

                # Sleep between checks (60 seconds during active games)
                await asyncio.sleep(60)
                continue

            # Fallback sleep
            await asyncio.sleep(60)

        except Exception as e:
            print(f"[ORCHESTRATOR] ERROR in main loop: {e}")
            print(traceback.format_exc())
            await asyncio.sleep(60)


# ======================================================
# Public API
# ======================================================

def get_session_info() -> Dict[str, Any]:
    """Get current session info for debugging"""
    if not CURRENT_SESSION:
        return {
            "session_active": False,
            "ingestion_active": INGESTION_ACTIVE,
        }

    session = CURRENT_SESSION
    return {
        "session_active": True,
        "session_date": session.session_date,
        "schedule_fetched_at": session.schedule_fetched_at.isoformat() if session.schedule_fetched_at else None,
        "ingestion_active": INGESTION_ACTIVE,
        "ingestion_started": session.ingestion_started,
        "ingestion_started_at": session.ingestion_started_at.isoformat() if session.ingestion_started_at else None,
        "session_ended": session.session_ended,
        "total_games": len(session.games),
        "games_summary": session.summary(),
        "first_game": session.first_game_time().isoformat() if session.first_game_time() else None,
        "last_game": session.last_game_time().isoformat() if session.last_game_time() else None,
        "games": [
            {
                "game_id": g.game_id,
                "matchup": f"{g.away_team}@{g.home_team}",
                "start_time": g.start_time_est.isoformat(),
                "state": g.state,
            }
            for g in session.games.values()
        ]
    }
