"""
Test Main Scheduled - FastAPI app with intelligent game scheduling

This is a TEST version of main.py that uses:
- smart_game_orchestrator.py for game scheduling
- managed_live_ingest.py for controllable ingestion

Run with:
    uvicorn test_main_scheduled:app --host 0.0.0.0 --port 8080

Environment variables:
    BALLDONTLIE_API_KEY - Required for API access
    GCP_PROJECT - Optional, auto-detected in Cloud Run

Debug endpoints:
    GET /debug/orchestrator - View scheduler state
    GET /debug/ingest - View ingestion state
    POST /debug/force-start - Force start ingestion (testing)
    POST /debug/force-stop - Force stop ingestion (testing)
    POST /debug/force-fetch - Force fetch schedule (testing)

Console output includes clear debug lines for Cloud Run monitoring.
"""

import os
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# ==================================================
# Orchestrator & Managed Ingest
# ==================================================
from smart_game_orchestrator import (
    orchestrator_loop,
    register_ingestion_callbacks,
    get_session_info,
    fetch_and_write_daily_schedule,
    start_ingestion,
    stop_ingestion,
)

from managed_live_ingest import (
    managed_ingest_loop,
    start_managed_ingest,
    stop_managed_ingest,
    get_ingest_status,
    live_stream_refresher,
    player_stats_refresher,
    LIVE_STREAM_STATE,
    PLAYER_STATS_STATE,
    nba_today,
)

# ==================================================
# Existing routers (import same as main.py)
# ==================================================
from live_games import router as live_games_router
from db import fetch_mobile_props
from historical_player_trends import router as historical_player_trends_router
from LiveOdds.live_odds_routes import router as live_odds_router
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


# ==================================================
# App
# ==================================================
app = FastAPI(
    title="Pulse Mobile API (Scheduled)",
    version="2.0.0",
    description="NBA Prop Analyzer with intelligent game scheduling",
)

# ==================================================
# Timezone
# ==================================================
NBA_TZ = ZoneInfo("America/New_York")

# ==================================================
# CORS
# ==================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================================================
# Include existing routers
# ==================================================
app.include_router(live_games_router)
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
app.include_router(bad_lines_router)


# ==================================================
# Startup
# ==================================================
@app.on_event("startup")
async def startup():
    print("\n" + "="*60)
    print(" PULSE MOBILE API - SCHEDULED VERSION")
    print("="*60)
    print(f" Start time: {datetime.now(NBA_TZ).strftime('%Y-%m-%d %I:%M:%S %p ET')}")
    print(f" NBA Today: {nba_today()}")
    print("="*60 + "\n")

    # Register callbacks so orchestrator can control ingest
    register_ingestion_callbacks(
        start_callback=start_managed_ingest,
        stop_callback=stop_managed_ingest,
    )

    # Start background tasks
    print("[STARTUP] Starting background tasks...")

    # 1. Orchestrator (manages game schedule + triggers ingest)
    asyncio.create_task(orchestrator_loop())
    print("[STARTUP] -> Orchestrator loop started")

    # 2. Managed ingest loop (waits for orchestrator signal)
    asyncio.create_task(managed_ingest_loop())
    print("[STARTUP] -> Managed ingest loop started")

    # 3. Cache refreshers (read from BQ -> memory for SSE)
    asyncio.create_task(live_stream_refresher())
    print("[STARTUP] -> Live stream refresher started")

    asyncio.create_task(player_stats_refresher())
    print("[STARTUP] -> Player stats refresher started")

    print("\n[STARTUP] All background tasks running")
    print("[STARTUP] Waiting for orchestrator to fetch schedule...\n")


# ==================================================
# Health Check
# ==================================================
@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": "2.0.0-scheduled",
        "time": datetime.now(NBA_TZ).isoformat(),
    }


# ==================================================
# Props Endpoint (same as main.py)
# ==================================================
@app.get("/props")
def get_props(
    game_date: str | None = None,
    min_hit_rate: float = Query(0.60, ge=0.0, le=1.0),
    limit: int = Query(200, ge=50, le=500),
    offset: int = Query(0, ge=0),
):
    if game_date is None:
        game_date = datetime.now(NBA_TZ).date().isoformat()

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
# Live Scores Endpoints (from cache)
# ==================================================
@app.get("/live/scores")
def get_live_scores():
    """Get cached live scores"""
    return LIVE_STREAM_STATE.payload


@app.get("/live/player-stats")
def get_live_player_stats():
    """Get cached player stats"""
    return PLAYER_STATS_STATE.payload


# ==================================================
# Debug Endpoints
# ==================================================
@app.get("/debug/orchestrator")
def debug_orchestrator():
    """Get orchestrator/scheduler state"""
    return get_session_info()


@app.get("/debug/ingest")
def debug_ingest():
    """Get ingest loop state"""
    return get_ingest_status()


@app.get("/debug/cache")
def debug_cache():
    """Get cache state"""
    return {
        "live_stream": {
            "game_count": len(LIVE_STREAM_STATE.payload.get("games", [])),
            "last_updated": LIVE_STREAM_STATE.last_updated.isoformat() if LIVE_STREAM_STATE.last_updated else None,
            "status": LIVE_STREAM_STATE.payload.get("meta", {}).get("status"),
        },
        "player_stats": {
            "player_count": len(PLAYER_STATS_STATE.payload.get("players", [])),
            "last_updated": PLAYER_STATS_STATE.last_updated.isoformat() if PLAYER_STATS_STATE.last_updated else None,
            "status": PLAYER_STATS_STATE.payload.get("meta", {}).get("status"),
        },
    }


@app.post("/debug/force-start")
def force_start_ingest():
    """Force start ingestion (for testing)"""
    start_ingestion()
    start_managed_ingest()
    return {"status": "started", "message": "Ingestion force started"}


@app.post("/debug/force-stop")
def force_stop_ingest():
    """Force stop ingestion (for testing)"""
    stop_ingestion()
    stop_managed_ingest()
    return {"status": "stopped", "message": "Ingestion force stopped"}


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
        return {
            "status": "error",
            "error": str(e),
        }


@app.get("/debug/full")
def debug_full():
    """Get complete debug state"""
    return {
        "time": datetime.now(NBA_TZ).isoformat(),
        "nba_today": nba_today().isoformat(),
        "orchestrator": get_session_info(),
        "ingest": get_ingest_status(),
        "cache": {
            "live_stream": {
                "game_count": len(LIVE_STREAM_STATE.payload.get("games", [])),
                "last_updated": LIVE_STREAM_STATE.last_updated.isoformat() if LIVE_STREAM_STATE.last_updated else None,
            },
            "player_stats": {
                "player_count": len(PLAYER_STATS_STATE.payload.get("players", [])),
                "last_updated": PLAYER_STATS_STATE.last_updated.isoformat() if PLAYER_STATS_STATE.last_updated else None,
            },
        },
    }


# ==================================================
# Main
# ==================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
