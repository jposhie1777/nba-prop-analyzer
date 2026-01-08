# main.py
import os
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from db import fetch_mobile_props, ingest_live_games_snapshot
from live_stream import router as live_stream_router, refresher_loop
from box_scores_snapshot import router as box_scores_router

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

    # ----------------------------------------------
    # Read-side refresher (SSE cache)
    # ----------------------------------------------
    asyncio.create_task(refresher_loop())

    # ----------------------------------------------
    # 🔴 ADDITION: player box read-side refresher
    # ----------------------------------------------
    asyncio.create_task(player_box_refresher())

    # ----------------------------------------------
    # 🔴 ADDITION: player stats read-side refresher
    # ----------------------------------------------
    asyncio.create_task(player_stats_refresher())

    # ----------------------------------------------
    # Write-side live ingest loop (BallDontLie → BQ)
    # ----------------------------------------------
    async def live_ingest_loop():
        while True:
            try:
                await asyncio.to_thread(ingest_live_games_snapshot)
            except Exception as e:
                print("❌ Live ingest failed:", e)

            await asyncio.sleep(15)

    asyncio.create_task(live_ingest_loop())

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