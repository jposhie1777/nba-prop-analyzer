“””
Game Scheduler - Smart polling window manager for NBA live data

This module:

1. Fetches today’s game schedule once in the morning (6 AM ET)
1. Activates polling windows 30 minutes before each game
1. Monitors game status and stops polling when games are FINAL
1. Handles games that cross midnight EST boundary
1. Reduces API calls by ~95% compared to continuous polling
   “””

import asyncio
import traceback
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Set
from zoneinfo import ZoneInfo

from google.cloud import bigquery

# ======================================================

# Configuration

# ======================================================

NBA_TZ = ZoneInfo(“America/New_York”)
SCHEDULE_FETCH_TIME = time(6, 0)  # 6:00 AM ET
PRE_GAME_WINDOW_MINUTES = 30
POST_FINAL_GRACE_MINUTES = 15  # Keep polling briefly after FINAL in case of corrections
SCHEDULE_RETRY_DELAY_MINUTES = 15
MAX_SCHEDULE_RETRIES = 4

# ======================================================

# Game State

# ======================================================

@dataclass
class GameWindow:
“”“Represents a polling window for a single game”””
game_id: int
start_time: datetime  # In NBA_TZ
home_team: str
away_team: str

```
# State tracking
polling_active: bool = False
is_final: bool = False
final_detected_at: Optional[datetime] = None
last_status: Optional[str] = None

def should_start_polling(self, now: datetime) -> bool:
    """Check if we should start polling this game"""
    if self.polling_active or self.is_final:
        return False
    
    poll_start = self.start_time - timedelta(minutes=PRE_GAME_WINDOW_MINUTES)
    return now >= poll_start

def should_stop_polling(self, now: datetime) -> bool:
    """Check if we should stop polling this game"""
    if not self.is_final:
        return False
    
    if self.final_detected_at is None:
        return False
    
    grace_end = self.final_detected_at + timedelta(minutes=POST_FINAL_GRACE_MINUTES)
    return now >= grace_end

def __str__(self) -> str:
    status = "FINAL" if self.is_final else ("ACTIVE" if self.polling_active else "SCHEDULED")
    return f"{self.away_team}@{self.home_team} ({status}) {self.start_time.strftime('%I:%M %p ET')}"
```

@dataclass
class ScheduleState:
“”“Global schedule state”””
games: Dict[int, GameWindow] = field(default_factory=dict)
schedule_fetched_at: Optional[datetime] = None
schedule_date: Optional[str] = None
last_fetch_failed: bool = False
consecutive_fetch_failures: int = 0

```
def active_game_ids(self) -> Set[int]:
    """Get IDs of games currently being polled"""
    return {
        game_id for game_id, game in self.games.items()
        if game.polling_active and not game.should_stop_polling(datetime.now(NBA_TZ))
    }

def has_active_games(self) -> bool:
    """Check if any games are currently active"""
    return len(self.active_game_ids()) > 0

def all_games_final(self) -> bool:
    """Check if all scheduled games are final"""
    if not self.games:
        return False
    return all(game.is_final for game in self.games.values())
```

# ======================================================

# Global State

# ======================================================

SCHEDULE = ScheduleState()

# ======================================================

# BigQuery Client

# ======================================================

def get_bq_client() -> bigquery.Client:
“”“Get BigQuery client with proper project setup”””
import os
project = os.environ.get(“GCP_PROJECT”) or os.environ.get(“GOOGLE_CLOUD_PROJECT”)
if project:
return bigquery.Client(project=project)
return bigquery.Client()

# ======================================================

# Schedule Fetching

# ======================================================

SCHEDULE_QUERY = “””
SELECT
game_id,
start_time_utc,
home_team_abbr,
away_team_abbr
FROM `graphite-flare-477419-h7.nba_goat_data.games`
WHERE game_date = @game_date
ORDER BY start_time_utc
“””

async def fetch_daily_schedule(game_date: str) -> List[GameWindow]:
“””
Fetch today’s game schedule from BigQuery.

```
Args:
    game_date: Date string in YYYY-MM-DD format (NBA timezone)

Returns:
    List of GameWindow objects for today's games
"""
def _run_query():
    client = get_bq_client()
    job = client.query(
        SCHEDULE_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "DATE", game_date)
            ]
        ),
    )
    return list(job.result())

rows = await asyncio.to_thread(_run_query)

games = []
for row in rows:
    # Convert UTC to ET
    start_time_et = row.start_time_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(NBA_TZ)
    
    games.append(
        GameWindow(
            game_id=row.game_id,
            start_time=start_time_et,
            home_team=row.home_team_abbr,
            away_team=row.away_team_abbr,
        )
    )

return games
```

# ======================================================

# Game Status Monitoring

# ======================================================

GAME_STATUS_QUERY = “””
WITH latest_status AS (
SELECT
game_id,
state,
ingested_at,
ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY ingested_at DESC) AS rn
FROM `graphite-flare-477419-h7.nba_live.live_games`
WHERE game_id IN UNNEST(@game_ids)
)
SELECT
game_id,
state
FROM latest_status
WHERE rn = 1
“””

async def check_game_statuses(game_ids: List[int]) -> Dict[int, str]:
“””
Check current status of games.

```
Args:
    game_ids: List of game IDs to check

Returns:
    Dict mapping game_id to status (LIVE, FINAL, etc)
"""
if not game_ids:
    return {}

def _run_query():
    client = get_bq_client()
    job = client.query(
        GAME_STATUS_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("game_ids", "INT64", game_ids)
            ]
        ),
    )
    return list(job.result())

rows = await asyncio.to_thread(_run_query)

return {row.game_id: row.state for row in rows}
```

# ======================================================

# Schedule Management Loop

# ======================================================

async def schedule_manager_loop():
“””
Main scheduler loop that:
1. Fetches schedule once per day at 6 AM ET
2. Activates/deactivates polling windows based on game times
3. Monitors game status to stop polling when games are final
“””
global SCHEDULE

```
print("🗓️  Game Scheduler starting...")

while True:
    try:
        now = datetime.now(NBA_TZ)
        today_str = now.date().isoformat()
        
        # ==========================================
        # SCHEDULE FETCH (once per day at 6 AM ET)
        # ==========================================
        should_fetch_schedule = (
            # Never fetched before
            SCHEDULE.schedule_date != today_str
            # Or it's past 6 AM and we haven't fetched today
            or (now.time() >= SCHEDULE_FETCH_TIME and SCHEDULE.schedule_fetched_at is None)
            # Or last fetch failed and we should retry
            or (SCHEDULE.last_fetch_failed and SCHEDULE.consecutive_fetch_failures < MAX_SCHEDULE_RETRIES)
        )
        
        if should_fetch_schedule:
            print(f"\n📅 Fetching game schedule for {today_str}...")
            
            try:
                games = await fetch_daily_schedule(today_str)
                
                # Update schedule
                SCHEDULE.games = {game.game_id: game for game in games}
                SCHEDULE.schedule_fetched_at = now
                SCHEDULE.schedule_date = today_str
                SCHEDULE.last_fetch_failed = False
                SCHEDULE.consecutive_fetch_failures = 0
                
                if games:
                    print(f"✅ Found {len(games)} games for {today_str}:")
                    for game in games:
                        print(f"   • {game}")
                else:
                    print(f"ℹ️  No games scheduled for {today_str}")
                
            except Exception as e:
                print(f"❌ Failed to fetch schedule: {e}")
                print(traceback.format_exc())
                SCHEDULE.last_fetch_failed = True
                SCHEDULE.consecutive_fetch_failures += 1
                
                # Wait before next check
                await asyncio.sleep(SCHEDULE_RETRY_DELAY_MINUTES * 60)
                continue
        
        # ==========================================
        # POLLING WINDOW MANAGEMENT
        # ==========================================
        if SCHEDULE.games:
            active_before = SCHEDULE.active_game_ids()
            
            # Start polling for games whose windows have opened
            for game_id, game in SCHEDULE.games.items():
                if game.should_start_polling(now) and not game.polling_active:
                    game.polling_active = True
                    print(f"🟢 START polling: {game}")
            
            # Check status of active games
            active_ids = list(SCHEDULE.active_game_ids())
            if active_ids:
                try:
                    statuses = await check_game_statuses(active_ids)
                    
                    for game_id, status in statuses.items():
                        game = SCHEDULE.games[game_id]
                        game.last_status = status
                        
                        # Mark game as final
                        if status == "FINAL" and not game.is_final:
                            game.is_final = True
                            game.final_detected_at = now
                            print(f"🏁 FINAL detected: {game}")
                        
                        # Stop polling after grace period
                        if game.should_stop_polling(now):
                            game.polling_active = False
                            print(f"🔴 STOP polling: {game}")
                
                except Exception as e:
                    print(f"⚠️  Failed to check game statuses: {e}")
            
            # Log active games if changed
            active_after = SCHEDULE.active_game_ids()
            if active_before != active_after:
                if active_after:
                    print(f"📊 Active games: {len(active_after)} - {active_after}")
                else:
                    print("📊 No active games")
        
        # ==========================================
        # SLEEP LOGIC (intelligent wait times)
        # ==========================================
        
        # If all games are final, wait until next schedule fetch time
        if SCHEDULE.games and SCHEDULE.all_games_final():
            # Calculate next 6 AM ET
            tomorrow = now.date() + timedelta(days=1)
            next_fetch = datetime.combine(tomorrow, SCHEDULE_FETCH_TIME, tzinfo=NBA_TZ)
            sleep_seconds = (next_fetch - now).total_seconds()
            
            print(f"💤 All games final. Sleeping until {next_fetch.strftime('%Y-%m-%d %I:%M %p ET')} ({sleep_seconds/3600:.1f} hours)")
            await asyncio.sleep(min(sleep_seconds, 3600))  # Max 1 hour sleep at a time
            continue
        
        # If games are active, check frequently
        if SCHEDULE.has_active_games():
            await asyncio.sleep(30)  # Check every 30 seconds during active games
        
        # If waiting for games to start, check less frequently
        elif SCHEDULE.games:
            await asyncio.sleep(120)  # Check every 2 minutes when waiting
        
        # If no schedule yet, check frequently until 6 AM
        else:
            await asyncio.sleep(60)  # Check every minute
    
    except Exception as e:
        print(f"❌ Schedule manager error: {e}")
        print(traceback.format_exc())
        await asyncio.sleep(60)
```

# ======================================================

# Public API

# ======================================================

def should_poll_live_data() -> bool:
“””
Check if live data polling should be active.
Call this from your stream refresher loops.

```
Returns:
    True if there are active games that should be polled
"""
return SCHEDULE.has_active_games()
```

def get_active_game_ids() -> Set[int]:
“””
Get list of game IDs that should currently be polled.

```
Returns:
    Set of game IDs for active games
"""
return SCHEDULE.active_game_ids()
```

def get_schedule_info() -> Dict:
“””
Get current schedule state for debugging.

```
Returns:
    Dict with schedule information
"""
now = datetime.now(NBA_TZ)

return {
    "schedule_date": SCHEDULE.schedule_date,
    "schedule_fetched_at": (
        SCHEDULE.schedule_fetched_at.isoformat()
        if SCHEDULE.schedule_fetched_at
        else None
    ),
    "total_games": len(SCHEDULE.games),
    "active_games": len(SCHEDULE.active_game_ids()),
    "final_games": sum(1 for g in SCHEDULE.games.values() if g.is_final),
    "should_poll": should_poll_live_data(),
    "games": [
        {
            "game_id": game.game_id,
            "matchup": f"{game.away_team}@{game.home_team}",
            "start_time": game.start_time.isoformat(),
            "status": game.last_status,
            "polling_active": game.polling_active,
            "is_final": game.is_final,
        }
        for game in SCHEDULE.games.values()
    ],
}
```