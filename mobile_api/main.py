# main.py
import os
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

from live_games import router as live_games_router
from db import fetch_mobile_props, ingest_live_games_snapshot
from live_stream import router as live_stream_router, refresher_loop

# ==================================================
# SMART SCHEDULING IMPORTS
# ==================================================
from smart_game_orchestrator import (
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
)
from historical_player_trends import router as historical_player_trends_router
from LiveGames.box_scores_snapshot import (
    router as box_scores_router,
    run_box_scores_snapshot,
)
from LiveOdds.live_game_odds_ingest import ingest_live_game_odds
from LiveOdds.live_player_prop_odds_ingest import ingest_live_player_prop_odds
from LiveOdds.live_odds_routes import router as live_odds_router
from LiveOdds.live_odds_flatten import run_live_odds_flatten
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
from routes.bad_lines import router as bad_lines_router
from routes.ladders import router as ladders_router

# ==================================================
# 🔴 ADDITION: player box stream imports
# ==================================================
from player_box_stream import (
    router as player_box_router,
    player_box_refresher,
)

# ==================================================
# 🔴 ADDITION: player stats stream imports
# ==================================================
from player_stats_stream import (
    router as player_stats_router,
    player_stats_refresher,
)

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
app.include_router(live_stream_router)
app.include_router(box_scores_router)
app.include_router(live_games_router)
app.include_router(historical_player_trends_router)
app.include_router(live_odds_router)
app.include_router(dev_bq_routes_router)
app.include_router(season_averages_router)
app.include_router(lineups_router)
app.include_router(first_basket_router)
app.include_router(teams_router)
app.include_router(player_box_router)
app.include_router(player_stats_router)
app.include_router(prop_analytics_router)
app.include_router(players_router)
app.include_router(ingest_router)
app.include_router(props_router)
app.include_router(live_props_dev_router)
app.include_router(live_props_router)
app.include_router(bad_line_alerts_router)
app.include_router(push_router)
app.include_router(alerts_router)
app.include_router(bad_lines_router)
app.include_router(ladders_router)


# ==================================================
# Startup hook (SMART SCHEDULED BACKGROUND TASKS)
# ==================================================
@app.on_event("startup")
async def startup():
    use_scheduler = os.environ.get("USE_SMART_SCHEDULER") == "true"
    enable_ingest = os.environ.get("ENABLE_LIVE_INGEST") == "true"

    print("\n" + "="*60)
    print("[STARTUP] PULSE MOBILE API")
    print(f"[STARTUP] Time: {datetime.now(NY_TZ).strftime('%Y-%m-%d %I:%M:%S %p ET')}")
    print(f"[STARTUP] USE_SMART_SCHEDULER: {use_scheduler}")
    print(f"[STARTUP] ENABLE_LIVE_INGEST: {enable_ingest}")
    print("="*60 + "\n")

    # =====================================================
    # SMART SCHEDULER MODE (new)
    # =====================================================
    if use_scheduler:
        print("[STARTUP] Using SMART SCHEDULER mode")
        print("[STARTUP] Ingestion will start 15 min before games")
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

        # READ-SIDE refreshers still run (but check is_ingestion_active)
        asyncio.create_task(refresher_loop())
        asyncio.create_task(player_box_refresher())
        asyncio.create_task(player_stats_refresher())
        print("[STARTUP] -> Cache refreshers started")

        return

    # =====================================================
    # LEGACY MODE (original 24/7 loops)
    # =====================================================
    if not enable_ingest:
        print("[STARTUP] Live ingest DISABLED - no background loops started")
        return

    print("[STARTUP] Using LEGACY 24/7 ingest mode")

    # -----------------------------
    # READ-SIDE (BQ → memory)
    # -----------------------------
    asyncio.create_task(refresher_loop())
    asyncio.create_task(player_box_refresher())
    asyncio.create_task(player_stats_refresher())

    # -----------------------------
    # WRITE-SIDE: games snapshot
    # -----------------------------
    async def live_ingest_loop():
        while True:
            try:
                await asyncio.to_thread(ingest_live_games_snapshot)
            except Exception as e:
                print("[INGEST] Live games ingest failed:", e)

            await asyncio.sleep(15)

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

            await asyncio.sleep(30)


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

            await asyncio.sleep(30)

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

            await asyncio.sleep(30)

    asyncio.create_task(live_player_prop_odds_loop())

    # -----------------------------
    # WRITE-SIDE: live odds FLATTEN (IDEMPOTENT)
    # -----------------------------
    async def live_odds_flatten_loop():
        # Let first RAW snapshots land
        await asyncio.sleep(20)

        while True:
            try:
                await asyncio.to_thread(run_live_odds_flatten)
                print("[INGEST] Live odds flatten complete")
            except Exception as e:
                print("[INGEST] Live odds flatten failed:", e)

            await asyncio.sleep(30)

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
