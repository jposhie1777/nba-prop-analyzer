# main.py
import os
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from live_games import router as live_games_router
from db import fetch_mobile_props, ingest_live_games_snapshot
from live_stream import router as live_stream_router, refresher_loop
from historical_player_trends import router as historical_player_trends_router
from box_scores_snapshot import (
    router as box_scores_router,
    run_box_scores_snapshot,
)
from live_game_odds_ingest import ingest_live_game_odds
from live_player_prop_odds_ingest import ingest_live_player_prop_odds
from live_odds_routes import router as live_odds_router
from live_odds_flatten import run_live_odds_flatten
from dev_bq_routes import router as dev_bq_routes_router
from first_basket_routes import router as first_basket_router
from ingest_season_averages import main as ingest_season_averages

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

from debug_code import register as register_debug_code

# ==================================================
# App (CREATE ONCE)
# ==================================================
app = FastAPI(
    title="Pulse Mobile API",
    version="1.0.0",
)
register_debug_code(app)

# ==================================================
# CORS (REQUIRED for Expo Web)
# ==================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://pulse-web-indol-seven.vercel.app",
        "http://localhost:3000",
        "http://localhost:8081",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    allow_credentials=True,
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
app.include_router(first_basket_router)

# ==================================================
# 🔴 ADDITION: player box + player stats routers
# ==================================================
app.include_router(player_box_router)
app.include_router(player_stats_router)

# ==================================================
# Startup hook (CONTROLLED BACKGROUND TASKS)
# ==================================================
@app.on_event("startup")
async def startup():
    enable_ingest = os.environ.get("ENABLE_LIVE_INGEST") == "true"

    if not enable_ingest:
        print("🛑 Live ingest DISABLED — no background loops started")
        return

    print("🟢 Live ingest ENABLED — starting background loops")

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
                print("❌ Live games ingest failed:", e)

            await asyncio.sleep(15)

    asyncio.create_task(live_ingest_loop())

    # -----------------------------
    # 🔴 WRITE-SIDE: box scores snapshot
    # -----------------------------
    async def live_boxscore_snapshot_loop():
        await asyncio.sleep(5)

        while True:
            try:
                await asyncio.to_thread(
                    run_box_scores_snapshot,
                    dry_run=False,
                )
                print("📸 Live boxscore snapshot written")

            except Exception as e:
                print("❌ Live boxscore snapshot failed:", e)

            await asyncio.sleep(30)


    asyncio.create_task(live_boxscore_snapshot_loop())
    
    # -----------------------------
    # 🔴 WRITE-SIDE: live game odds
    # -----------------------------
    async def live_game_odds_loop():
        await asyncio.sleep(10)
    
        while True:
            try:
                await asyncio.to_thread(ingest_live_game_odds)
                print("📈 Live game odds snapshot written")
            except Exception as e:
                print("❌ Live game odds ingest failed:", e)
    
            await asyncio.sleep(30)
    
    asyncio.create_task(live_game_odds_loop())
    
    
    # -----------------------------
    # 🔴 WRITE-SIDE: live player props
    # -----------------------------
    async def live_player_prop_odds_loop():
        await asyncio.sleep(12)
    
        while True:
            try:
                await asyncio.to_thread(ingest_live_player_prop_odds)
                print("🎯 Live player prop odds snapshot written")
            except Exception as e:
                print("❌ Live player prop odds ingest failed:", e)
    
            await asyncio.sleep(30)
    
    asyncio.create_task(live_player_prop_odds_loop())

    # -----------------------------
    # 🔴 WRITE-SIDE: live odds FLATTEN (IDEMPOTENT)
    # -----------------------------
    async def live_odds_flatten_loop():
        # Let first RAW snapshots land
        await asyncio.sleep(20)
    
        while True:
            try:
                await asyncio.to_thread(run_live_odds_flatten)
                print("🧮 Live odds flatten complete")
            except Exception as e:
                print("❌ Live odds flatten failed:", e)
    
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

@app.post("/admin/ingest/season-averages")
def run_season_averages_ingestion(
    season: int = Query(2024),
    season_type: str = Query("regular")
):
    ingest_season_averages(season=season, season_type=season_type)

    return {
        "status": "started",
        "season": season,
        "season_type": season_type,
        "started_at": datetime.utcnow().isoformat()
    }