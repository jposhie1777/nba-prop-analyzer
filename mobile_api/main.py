# main.py
import os
import asyncio
import json
import time
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse

from db import fetch_mobile_props, ingest_live_games_snapshot

# ==================================================
# SMART SCHEDULING IMPORTS
# ==================================================
from stater_game_orchestrator import (
    orchestrator_loop,
    register_ingestion_callbacks,
    get_session_info,
    fetch_and_write_daily_schedule,
    start_ingestion,
    stop_ingestion,
    is_ingestion_active,
)
from managed_live_ingest import (
    managed_ingest_loop,
    start_managed_ingest,
    stop_managed_ingest,
    get_ingest_status,
    nba_today,
    get_live_scores_payload,
    get_live_games_payload,
    get_player_box_payload,
    snapshot_player_box,
    get_player_stats_payload,
    LIVE_STREAM_STATE,
)
from historical_player_trends import router as historical_player_trends_router
from LiveGames.box_scores_snapshot import (
    router as box_scores_router,
    run_box_scores_snapshot,
)
from LiveOdds.live_game_odds_ingest import ingest_live_game_odds
from LiveOdds.live_player_prop_odds_ingest import ingest_live_player_prop_odds
from LiveOdds.live_odds_routes import router as live_odds_router
from LiveOdds.live_odds_flatten import run_live_odds_orchestrator
from LiveOdds.pregame_game_odds_ingest import (
    run_full_pregame_cycle,
    capture_closing_lines,
)
from dev_bq_routes import router as dev_bq_routes_router
from averages.season_averages_routes import router as season_averages_router
from routes.lineup_routes import router as lineups_router
from routes.first_basket import router as first_basket_router
from routes.teams import router as teams_router
from routes.prop_analytics import router as prop_analytics_router
from routes.players_routes import router as players_router
from routes.ingest import router as ingest_router
from routes.props import router as props_router
from routes.live_props_dev import router as live_props_dev_router
from routes.live_props import router as live_props_router
from routes.bad_line_alerts import router as bad_line_alerts_router
from routes.push import router as push_router
from routes.alerts_bad_lines import router as alerts_router
from routes.alerts_hedge import router as hedge_alerts_router
from routes.bad_lines import router as bad_lines_router
from routes.ladders import router as ladders_router
from routes.season_averages import router as season_averages_data_router
from routes.opponent_position_defense import (
    router as opponent_position_defense_router,
)
from routes.game_betting_analytics import (
    router as game_betting_analytics_router,
)
from routes.three_q_100 import router as three_q_100_router
from routes.pga_analytics import router as pga_analytics_router
from routes.atp_analytics import (
    router as atp_analytics_router,
    build_tournament_bracket_payload,
)
from routes.correlations import router as correlations_router
from routes.game_environment import router as game_environment_router

# ==================================================
# Game Advanced Stats V2 imports
# ==================================================
from ingest.game_advanced_stats.routes import router as game_advanced_stats_router
from ingest.game_advanced_stats.ingest import ingest_yesterday as ingest_game_advanced_stats_yesterday

# ==================================================
# Season Averages imports
# ==================================================
from ingest.season_averages.routes import router as season_averages_ingest_router
from ingest.season_averages.ingest import ingest_current_season as ingest_season_averages_current
from ingest.atp.routes import router as atp_ingest_router
from ingest.pga.routes import router as pga_ingest_router

# ==================================================
# Injuries and WOWY imports
# ==================================================
from ingest.injuries.routes import router as injuries_router
from ingest.injuries.ingest import ingest_injuries

from debug.debug_code import register as register_debug_code

# ==================================================
# App (CREATE ONCE)
# ==================================================
app = FastAPI(
    title="Pulse Mobile API",
    version="1.0.0",
)
register_debug_code(app)


# ==================================================
# Timezone (AUTHORITATIVE)
# ==================================================
NY_TZ = ZoneInfo("America/New_York")

def _read_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        return default

# ==================================================
# CORS (REQUIRED for Expo Web)
# ==================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten later
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================================================
# Routers
# ==================================================
app.include_router(box_scores_router)
app.include_router(historical_player_trends_router)
app.include_router(live_odds_router)
app.include_router(dev_bq_routes_router)
app.include_router(season_averages_router)
app.include_router(lineups_router)
app.include_router(first_basket_router)
app.include_router(teams_router)
app.include_router(prop_analytics_router)
app.include_router(players_router)
app.include_router(ingest_router)
app.include_router(props_router)
app.include_router(live_props_dev_router)
app.include_router(live_props_router)
app.include_router(bad_line_alerts_router)
app.include_router(push_router)
app.include_router(alerts_router)
app.include_router(hedge_alerts_router)
app.include_router(bad_lines_router)
app.include_router(ladders_router)
app.include_router(game_advanced_stats_router)
app.include_router(season_averages_ingest_router)
app.include_router(season_averages_data_router)
app.include_router(opponent_position_defense_router)
app.include_router(injuries_router)
app.include_router(game_betting_analytics_router)
app.include_router(three_q_100_router)
app.include_router(pga_analytics_router)
app.include_router(atp_analytics_router)
app.include_router(atp_ingest_router)
app.include_router(pga_ingest_router)
app.include_router(correlations_router)
app.include_router(game_environment_router)

# ==================================================
# Startup hook (SMART SCHEDULED BACKGROUND TASKS)
# ==================================================
@app.on_event("startup")
async def startup():
    use_scheduler = os.environ.get("USE_SMART_SCHEDULER") == "true"
    enable_ingest = os.environ.get("ENABLE_LIVE_INGEST") == "true"
    enable_live_games_snapshot = (
        os.environ.get("ENABLE_LIVE_GAMES_SNAPSHOT") == "true"
    )
    live_games_interval_sec = _read_int_env(
        "LIVE_GAMES_SNAPSHOT_INTERVAL_SEC",
        300,
    )

    print("\n" + "="*60)
    print("[STARTUP] PULSE MOBILE API")
    print(f"[STARTUP] Time: {datetime.now(NY_TZ).strftime('%Y-%m-%d %I:%M:%S %p ET')}")
    print(f"[STARTUP] USE_SMART_SCHEDULER: {use_scheduler}")
    print(f"[STARTUP] ENABLE_LIVE_INGEST: {enable_ingest}")
    print(f"[STARTUP] ENABLE_LIVE_GAMES_SNAPSHOT: {enable_live_games_snapshot}")
    if enable_live_games_snapshot:
        print(
            "[STARTUP] LIVE_GAMES_SNAPSHOT_INTERVAL_SEC:"
            f" {live_games_interval_sec}"
        )
    print("="*60 + "\n")

    # =====================================================
    # SMART SCHEDULER MODE (new)
    # =====================================================
    if use_scheduler:
        print("[STARTUP] Using SMART SCHEDULER mode")
        print("[STARTUP] Ingestion will start when games go LIVE (or pre-game lead)")
        print("[STARTUP] Ingestion will stop when all games FINAL\n")

        # Register callbacks so orchestrator can control ingest
        register_ingestion_callbacks(
            start_callback=start_managed_ingest,
            stop_callback=stop_managed_ingest,
        )

        # Start orchestrator (manages schedule + triggers ingest)
        asyncio.create_task(orchestrator_loop())
        print("[STARTUP] -> Orchestrator loop started")

        # Start managed ingest loop (waits for orchestrator signal)
        asyncio.create_task(managed_ingest_loop())
        print("[STARTUP] -> Managed ingest loop started")

        # Read-side snapshots are fetched on demand via endpoints.

        # -----------------------------
        # DAILY: Game Advanced Stats V2 ingest (runs at 6:00 AM ET)
        # -----------------------------
        async def game_advanced_stats_daily_loop():
            """
            Daily loop that ingests game advanced stats for yesterday's games.
            Runs at 6:00 AM ET, after all games are final and data is available.
            """
            from datetime import time as dt_time, timedelta

            INGEST_HOUR = 6
            INGEST_MINUTE = 0

            print("[GAME_ADV_STATS] Daily ingest loop started")
            print(f"[GAME_ADV_STATS] Scheduled to run at {INGEST_HOUR}:{INGEST_MINUTE:02d} AM ET")

            while True:
                try:
                    now = datetime.now(NY_TZ)

                    # Calculate next run time
                    next_run = now.replace(
                        hour=INGEST_HOUR,
                        minute=INGEST_MINUTE,
                        second=0,
                        microsecond=0,
                    )

                    # If we've passed today's run time, schedule for tomorrow
                    if now >= next_run:
                        next_run = next_run + timedelta(days=1)

                    wait_seconds = (next_run - now).total_seconds()

                    print(f"[GAME_ADV_STATS] Next run: {next_run.strftime('%Y-%m-%d %I:%M %p ET')} ({wait_seconds/3600:.1f} hours)")

                    # Wait until next run time
                    await asyncio.sleep(wait_seconds)

                    # Run the ingest
                    print(f"\n[GAME_ADV_STATS] ========== DAILY INGEST @ {datetime.now(NY_TZ).strftime('%I:%M %p ET')} ==========")

                    result = await asyncio.to_thread(ingest_game_advanced_stats_yesterday)

                    print(f"[GAME_ADV_STATS] Result: {result}")
                    print(f"[GAME_ADV_STATS] Daily ingest complete\n")

                except Exception as e:
                    print(f"[GAME_ADV_STATS] ERROR in daily loop: {e}")
                    # Wait 1 hour before retrying on error
                    await asyncio.sleep(3600)

        asyncio.create_task(game_advanced_stats_daily_loop())
        print("[STARTUP] -> Game Advanced Stats daily ingest loop started")

        # -----------------------------
        # DAILY: Season Averages ingest (runs at 6:15 AM ET)
        # -----------------------------
        async def season_averages_daily_loop():
            """
            Daily loop that ingests season averages for the current season.
            Runs at 6:15 AM ET, after game advanced stats complete.
            """
            from datetime import time as dt_time, timedelta

            INGEST_HOUR = 6
            INGEST_MINUTE = 15

            print("[SEASON_AVG] Daily ingest loop started")
            print(f"[SEASON_AVG] Scheduled to run at {INGEST_HOUR}:{INGEST_MINUTE:02d} AM ET")

            while True:
                try:
                    now = datetime.now(NY_TZ)

                    # Calculate next run time
                    next_run = now.replace(
                        hour=INGEST_HOUR,
                        minute=INGEST_MINUTE,
                        second=0,
                        microsecond=0,
                    )

                    # If we've passed today's run time, schedule for tomorrow
                    if now >= next_run:
                        next_run = next_run + timedelta(days=1)

                    wait_seconds = (next_run - now).total_seconds()

                    print(f"[SEASON_AVG] Next run: {next_run.strftime('%Y-%m-%d %I:%M %p ET')} ({wait_seconds/3600:.1f} hours)")

                    # Wait until next run time
                    await asyncio.sleep(wait_seconds)

                    # Run the ingest
                    print(f"\n[SEASON_AVG] ========== DAILY INGEST @ {datetime.now(NY_TZ).strftime('%I:%M %p ET')} ==========")

                    result = await asyncio.to_thread(ingest_season_averages_current)

                    print(f"[SEASON_AVG] Result: {result}")
                    print(f"[SEASON_AVG] Daily ingest complete\n")

                except Exception as e:
                    print(f"[SEASON_AVG] ERROR in daily loop: {e}")
                    # Wait 1 hour before retrying on error
                    await asyncio.sleep(3600)

        asyncio.create_task(season_averages_daily_loop())
        print("[STARTUP] -> Season Averages daily ingest loop started")

        # -----------------------------
        # DAILY: ATP tournament bracket warm-up (runs at 6:00 AM ET)
        # -----------------------------
        async def atp_bracket_daily_refresh_loop():
            """
            Daily loop that refreshes the ATP tournament bracket cache.
            Runs at 6:00 AM ET to keep the standard endpoint warm.
            """
            REFRESH_HOUR = 6
            REFRESH_MINUTE = 0

            print("[ATP_BRACKET] Daily refresh loop started")
            print(
                "[ATP_BRACKET] Scheduled to run at"
                f" {REFRESH_HOUR}:{REFRESH_MINUTE:02d} AM ET"
            )

            while True:
                try:
                    now = datetime.now(NY_TZ)
                    next_run = now.replace(
                        hour=REFRESH_HOUR,
                        minute=REFRESH_MINUTE,
                        second=0,
                        microsecond=0,
                    )
                    if now >= next_run:
                        next_run = next_run + timedelta(days=1)

                    wait_seconds = (next_run - now).total_seconds()
                    print(
                        "[ATP_BRACKET] Next run:"
                        f" {next_run.strftime('%Y-%m-%d %I:%M %p ET')}"
                        f" ({wait_seconds/3600:.1f} hours)"
                    )
                    await asyncio.sleep(wait_seconds)

                    print(
                        "\n[ATP_BRACKET] ======== DAILY REFRESH @"
                        f" {datetime.now(NY_TZ).strftime('%I:%M %p ET')} ========"
                    )

                    payload = await asyncio.to_thread(build_tournament_bracket_payload)
                    tournament_name = (payload.get("tournament") or {}).get("name")
                    match_count = payload.get("match_count")
                    print(
                        "[ATP_BRACKET] Refreshed bracket cache"
                        f" for {tournament_name} ({match_count} matches)"
                    )
                    print("[ATP_BRACKET] Daily refresh complete\n")

                except Exception as e:
                    print(f"[ATP_BRACKET] ERROR in daily loop: {e}")
                    await asyncio.sleep(3600)

        asyncio.create_task(atp_bracket_daily_refresh_loop())
        print("[STARTUP] -> ATP bracket daily refresh loop started")

        # -----------------------------
        # HOURLY: Pre-game Game Odds ingest (runs every hour until games start)
        # -----------------------------
        async def pregame_odds_hourly_loop():
            """
            Hourly loop that ingests pre-game odds for upcoming games.
            Runs every hour from 8 AM ET until games start.
            Also captures closing lines when games transition to LIVE.
            """
            from datetime import time as dt_time, timedelta

            # Start ingesting at 8 AM ET, stop at 11 PM ET
            START_HOUR = 8
            END_HOUR = 23
            INTERVAL_MINUTES = 60  # Run every hour

            print("[PREGAME_ODDS] Hourly ingest loop started")
            print(f"[PREGAME_ODDS] Active hours: {START_HOUR}:00 AM - {END_HOUR}:00 PM ET")
            print(f"[PREGAME_ODDS] Interval: {INTERVAL_MINUTES} minutes")

            while True:
                try:
                    now = datetime.now(NY_TZ)
                    current_hour = now.hour

                    # Only run during active hours
                    if START_HOUR <= current_hour < END_HOUR:
                        print(f"\n[PREGAME_ODDS] ========== HOURLY INGEST @ {now.strftime('%I:%M %p ET')} ==========")

                        result = await asyncio.to_thread(run_full_pregame_cycle)

                        print(f"[PREGAME_ODDS] Result: {result}")
                        print(f"[PREGAME_ODDS] Hourly ingest complete\n")

                        # Sleep for the interval
                        await asyncio.sleep(INTERVAL_MINUTES * 60)

                    else:
                        # Outside active hours - calculate wait until next active period
                        if current_hour < START_HOUR:
                            # Wait until START_HOUR today
                            next_run = now.replace(hour=START_HOUR, minute=0, second=0, microsecond=0)
                        else:
                            # Wait until START_HOUR tomorrow
                            next_run = (now + timedelta(days=1)).replace(
                                hour=START_HOUR, minute=0, second=0, microsecond=0
                            )

                        wait_seconds = (next_run - now).total_seconds()
                        print(f"[PREGAME_ODDS] Outside active hours. Next run: {next_run.strftime('%Y-%m-%d %I:%M %p ET')}")
                        await asyncio.sleep(min(wait_seconds, 3600))  # Check at least every hour

                except Exception as e:
                    print(f"[PREGAME_ODDS] ERROR in hourly loop: {e}")
                    # Wait 15 minutes before retrying on error
                    await asyncio.sleep(900)

        asyncio.create_task(pregame_odds_hourly_loop())
        print("[STARTUP] -> Pre-game Odds hourly ingest loop started")

        # -----------------------------
        # Closing Line Capture (runs with managed ingest to catch game transitions)
        # -----------------------------
        async def closing_line_capture_loop():
            """
            Runs every 2 minutes during active ingestion to capture closing lines
            when games transition from UPCOMING to LIVE.
            """
            print("[CLOSING_LINES] Capture loop started")

            while True:
                try:
                    # Only run when ingestion is active (games are happening)
                    if is_ingestion_active():
                        result = await asyncio.to_thread(capture_closing_lines)
                        if result.get("closing_lines_captured", 0) > 0:
                            print(f"[CLOSING_LINES] Captured {result['closing_lines_captured']} closing lines")

                    await asyncio.sleep(120)  # Check every 2 minutes

                except Exception as e:
                    print(f"[CLOSING_LINES] ERROR: {e}")
                    await asyncio.sleep(120)

        asyncio.create_task(closing_line_capture_loop())
        print("[STARTUP] -> Closing Line capture loop started")

        return

    # =====================================================
    # LEGACY MODE (original 24/7 loops)
    # =====================================================
    if not enable_ingest:
        if enable_live_games_snapshot:
            print("[STARTUP] Live ingest DISABLED - live games snapshot only")

            async def live_games_snapshot_loop():
                while True:
                    try:
                        await asyncio.to_thread(ingest_live_games_snapshot)
                    except Exception as e:
                        print("[INGEST] Live games snapshot failed:", e)

                    await asyncio.sleep(live_games_interval_sec)

            asyncio.create_task(live_games_snapshot_loop())
            print("[STARTUP] -> Live games snapshot loop started")
        else:
            print("[STARTUP] Live ingest DISABLED - no background loops started")
        return

    print("[STARTUP] Using LEGACY 24/7 ingest mode")

    # Read-side snapshots are fetched on demand via endpoints.

    # -----------------------------
    # WRITE-SIDE: games snapshot
    # -----------------------------
    async def live_ingest_loop():
        while True:
            try:
                await asyncio.to_thread(ingest_live_games_snapshot)
            except Exception as e:
                print("[INGEST] Live games ingest failed:", e)

            await asyncio.sleep(60)

    asyncio.create_task(live_ingest_loop())

    # -----------------------------
    # WRITE-SIDE: box scores snapshot
    # -----------------------------
    async def live_boxscore_snapshot_loop():
        await asyncio.sleep(5)

        while True:
            try:
                await asyncio.to_thread(
                    run_box_scores_snapshot,
                    dry_run=False,
                )
                print("[INGEST] Live boxscore snapshot written")

            except Exception as e:
                print("[INGEST] Live boxscore snapshot failed:", e)

            await asyncio.sleep(60)


    asyncio.create_task(live_boxscore_snapshot_loop())

    # -----------------------------
    # WRITE-SIDE: live game odds
    # -----------------------------
    async def live_game_odds_loop():
        await asyncio.sleep(10)

        while True:
            try:
                await asyncio.to_thread(ingest_live_game_odds)
                print("[INGEST] Live game odds snapshot written")
            except Exception as e:
                print("[INGEST] Live game odds ingest failed:", e)

            await asyncio.sleep(60)

    asyncio.create_task(live_game_odds_loop())


    # -----------------------------
    # WRITE-SIDE: live player props
    # -----------------------------
    async def live_player_prop_odds_loop():
        await asyncio.sleep(12)

        while True:
            try:
                await asyncio.to_thread(ingest_live_player_prop_odds)
                print("[INGEST] Live player prop odds snapshot written")
            except Exception as e:
                print("[INGEST] Live player prop odds ingest failed:", e)

            await asyncio.sleep(60)

    asyncio.create_task(live_player_prop_odds_loop())

    # -----------------------------
    # WRITE-SIDE: live odds FLATTEN (IDEMPOTENT)
    # -----------------------------
    async def live_odds_flatten_loop():
        # Let first RAW snapshots land
        await asyncio.sleep(20)

        while True:
            try:
                await asyncio.to_thread(run_live_odds_orchestrator)
                print("[INGEST] Live odds flatten complete")
            except Exception as e:
                print("[INGEST] Live odds flatten failed:", e)

            await asyncio.sleep(60)

    asyncio.create_task(live_odds_flatten_loop())

# ==================================================
# Health check
# ==================================================
@app.get("/health")
def health():
    return {"status": "ok"}

# ==================================================
# Props endpoint (mobile default)
# ==================================================
@app.get("/props")
def get_props(
    game_date: str | None = None,
    min_hit_rate: float = Query(0.60, ge=0.0, le=1.0),
    limit: int = Query(200, ge=50, le=500),
    offset: int = Query(0, ge=0),
):
    if game_date is None:
        game_date = datetime.now(NY_TZ).date().isoformat()

    props = fetch_mobile_props(
        game_date=game_date,
        min_hit_rate=min_hit_rate,
        limit=limit,
        offset=offset,
    )

    return {
        "date": game_date,
        "count": len(props),
        "props": props,
    }


# ==================================================
# Live Endpoints (on-demand snapshots)
# ==================================================
@app.get("/live/scores")
def get_live_scores(force_refresh: bool = False):
    """Get live scores (cached with TTL)"""
    return get_live_scores_payload(force_refresh=force_refresh)


@app.get("/live/scores/stream")
async def live_scores_stream():
    async def gen():
        last_sent: str | None = None
        last_keepalive = 0.0

        while True:
            now = time.time()
            if now - last_keepalive >= 15:
                yield ":keepalive\n\n"
                last_keepalive = now

            payload = await asyncio.to_thread(get_live_scores_payload)
            data_str = json.dumps(payload, separators=(",", ":"))
            if data_str != last_sent:
                yield f"event: snapshot\ndata: {data_str}\n\n"
                last_sent = data_str

            await asyncio.sleep(1)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/live/scores/debug")
def live_scores_debug():
    now = datetime.now(NY_TZ)
    last_updated = LIVE_STREAM_STATE.last_updated
    last_attempt = LIVE_STREAM_STATE.last_attempt
    return {
        "last_good_seconds_ago": (
            int((now - last_updated).total_seconds()) if last_updated else None
        ),
        "last_attempt_seconds_ago": (
            int((now - last_attempt).total_seconds()) if last_attempt else None
        ),
        "consecutive_failures": LIVE_STREAM_STATE.consecutive_errors,
        "meta": LIVE_STREAM_STATE.payload.get("meta"),
        "game_count": len(LIVE_STREAM_STATE.payload.get("games", [])),
    }


@app.get("/live/games")
def get_live_games():
    """Get live/upcoming games list"""
    return get_live_games_payload()


@app.get("/live/player-box")
def get_live_player_box(
    game_date: date | None = None,
    force_refresh: bool = False,
):
    """Get player box scores"""
    return get_player_box_payload(
        game_date=game_date,
        force_refresh=force_refresh,
    )


@app.get("/live/player-box/snapshot")
def snapshot_live_player_box(
    game_date: date | None = None,
    dry_run: bool = False,
):
    """Persist a player box snapshot (optional dry run)"""
    return snapshot_player_box(game_date=game_date, dry_run=dry_run)


@app.get("/live/player-stats")
def get_live_player_stats(
    game_date: date | None = None,
    force_refresh: bool = False,
):
    """Get player stats snapshot"""
    return get_player_stats_payload(
        game_date=game_date,
        force_refresh=force_refresh,
    )


@app.get("/live/player-stats/stream")
async def live_player_stats_stream(game_date: date | None = None):
    async def gen():
        last_sent: str | None = None
        last_keepalive = 0.0

        while True:
            now = time.time()
            if now - last_keepalive >= 15:
                yield ":keepalive\n\n"
                last_keepalive = now

            payload = await asyncio.to_thread(
                get_player_stats_payload,
                game_date=game_date,
            )
            data_str = json.dumps(payload, separators=(",", ":"))
            if data_str != last_sent:
                yield f"event: snapshot\ndata: {data_str}\n\n"
                last_sent = data_str

            await asyncio.sleep(1)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ==================================================
# Debug endpoints (SMART SCHEDULER)
# ==================================================
@app.get("/debug/orchestrator")
def debug_orchestrator():
    """Get orchestrator/scheduler state"""
    return get_session_info()


@app.get("/debug/ingest")
def debug_ingest():
    """Get ingest loop state"""
    return get_ingest_status()


@app.post("/debug/force-start")
def force_start_ingest():
    """Force start ingestion (for testing)"""
    start_ingestion()
    start_managed_ingest()
    return {"status": "started", "message": "FIRING UP INGESTION - FORCED"}


@app.post("/debug/force-stop")
def force_stop_ingest():
    """Force stop ingestion (for testing)"""
    stop_ingestion()
    stop_managed_ingest()
    return {"status": "stopped", "message": "STOPPING INGESTION - FORCED"}


@app.post("/debug/force-fetch")
async def force_fetch_schedule():
    """Force fetch today's schedule (for testing)"""
    today = nba_today().isoformat()
    try:
        session = await asyncio.to_thread(fetch_and_write_daily_schedule, today)
        return {
            "status": "ok",
            "date": today,
            "games": len(session.games),
            "games_list": [str(g) for g in session.games.values()],
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/debug/full")
def debug_full():
    """Get complete debug state"""
    return {
        "time": datetime.now(NY_TZ).isoformat(),
        "nba_today": nba_today().isoformat(),
        "orchestrator": get_session_info(),
        "ingest": get_ingest_status(),
    }
