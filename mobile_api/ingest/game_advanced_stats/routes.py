# mobile_api/ingest/game_advanced_stats/routes.py
"""
API Routes for Game Advanced Stats V2 Ingestion

Endpoints for:
- Fetching yesterday's advanced stats (daily scheduled job)
- Fetching stats for a specific date
- Backfilling the entire 2025-2026 season
"""

import asyncio
from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from .ingest import (
    ingest_yesterday,
    ingest_date,
    ingest_date_range,
    backfill_season,
)


# ======================================================
# Router
# ======================================================

router = APIRouter(
    prefix="/ingest/game-advanced-stats",
    tags=["ingest", "advanced-stats"],
)

NBA_TZ = ZoneInfo("America/New_York")


# ======================================================
# Request/Response Models
# ======================================================

class DateIngestRequest(BaseModel):
    date: str  # YYYY-MM-DD format
    period: int = 0  # 0 = full game


class DateRangeIngestRequest(BaseModel):
    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format
    period: int = 0


class BackfillRequest(BaseModel):
    season: int = 2025  # e.g., 2025 for 2025-2026 season
    period: int = 0


class IngestResponse(BaseModel):
    status: str
    message: str
    data: Optional[dict] = None


# ======================================================
# Routes
# ======================================================

@router.post("/yesterday", response_model=IngestResponse)
async def ingest_yesterday_route(period: int = 0):
    """
    Ingest game advanced stats for yesterday's games.

    This is the primary endpoint for daily scheduled ingestion.
    Runs after all games from yesterday are final (typically 5-6 AM ET).

    Args:
        period: Period filter (0 = full game, 1-4 = quarters, 5+ = OT)

    Returns:
        Ingest summary with game and stat counts
    """
    print("[ROUTE] /ingest/game-advanced-stats/yesterday called")

    try:
        result = await asyncio.to_thread(ingest_yesterday, period=period)

        return IngestResponse(
            status="ok",
            message=f"Ingested {result['stats']} stats from {result['games']} games for {result['date']}",
            data=result,
        )

    except Exception as e:
        print(f"[ROUTE] Error in yesterday ingest: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest yesterday's stats: {str(e)}",
        )


@router.post("/date", response_model=IngestResponse)
async def ingest_date_route(request: DateIngestRequest):
    """
    Ingest game advanced stats for a specific date.

    Args:
        request.date: Target date in YYYY-MM-DD format
        request.period: Period filter (0 = full game)

    Returns:
        Ingest summary with game and stat counts
    """
    print(f"[ROUTE] /ingest/game-advanced-stats/date called for {request.date}")

    try:
        target_date = date.fromisoformat(request.date)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: {request.date}. Use YYYY-MM-DD.",
        )

    try:
        result = await asyncio.to_thread(
            ingest_date,
            target_date=target_date,
            period=request.period,
        )

        return IngestResponse(
            status="ok",
            message=f"Ingested {result['stats']} stats from {result['games']} games for {result['date']}",
            data=result,
        )

    except Exception as e:
        print(f"[ROUTE] Error in date ingest: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest stats for {request.date}: {str(e)}",
        )


@router.post("/range", response_model=IngestResponse)
async def ingest_range_route(request: DateRangeIngestRequest):
    """
    Ingest game advanced stats for a date range.

    WARNING: This can be a long-running operation for large date ranges.
    Consider using the backfill endpoint for full season backfills.

    Args:
        request.start_date: Start date (inclusive) in YYYY-MM-DD format
        request.end_date: End date (inclusive) in YYYY-MM-DD format
        request.period: Period filter (0 = full game)

    Returns:
        Ingest summary with total counts
    """
    print(f"[ROUTE] /ingest/game-advanced-stats/range called for {request.start_date} to {request.end_date}")

    try:
        start = date.fromisoformat(request.start_date)
        end = date.fromisoformat(request.end_date)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format. Use YYYY-MM-DD. Error: {e}",
        )

    if start > end:
        raise HTTPException(
            status_code=400,
            detail="start_date must be before or equal to end_date",
        )

    # Warn about large ranges
    days = (end - start).days + 1
    if days > 90:
        print(f"[ROUTE] WARNING: Large date range requested ({days} days)")

    try:
        result = await asyncio.to_thread(
            ingest_date_range,
            start_date=start,
            end_date=end,
            period=request.period,
        )

        return IngestResponse(
            status="ok",
            message=f"Ingested {result['total_stats']} stats from {result['total_games']} games across {result['dates_with_data']} days",
            data=result,
        )

    except Exception as e:
        print(f"[ROUTE] Error in range ingest: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest stats for date range: {str(e)}",
        )


@router.post("/backfill", response_model=IngestResponse)
async def backfill_season_route(
    request: BackfillRequest,
    background_tasks: BackgroundTasks,
):
    """
    Backfill all game advanced stats for a full season.

    This is a potentially long-running operation that fetches all stats
    for the entire season in one batch. Recommended for initial setup
    or recovery scenarios.

    For the 2025-2026 season, use season=2025.

    Args:
        request.season: Season year (e.g., 2025 for 2025-2026)
        request.period: Period filter (0 = full game)

    Returns:
        Ingest summary with total counts
    """
    print(f"[ROUTE] /ingest/game-advanced-stats/backfill called for season {request.season}")

    try:
        result = await asyncio.to_thread(
            backfill_season,
            season=request.season,
            period=request.period,
        )

        return IngestResponse(
            status="ok",
            message=f"Backfilled {result['stats']} stats from {result['games']} games for season {result['season']}",
            data=result,
        )

    except Exception as e:
        print(f"[ROUTE] Error in backfill: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to backfill season {request.season}: {str(e)}",
        )


@router.get("/status")
async def get_status():
    """
    Get the status of the game advanced stats ingestion system.

    Returns:
        Current configuration and last run info
    """
    now_et = datetime.now(NBA_TZ)

    return {
        "status": "ok",
        "current_time_et": now_et.isoformat(),
        "endpoints": {
            "yesterday": "POST /ingest/game-advanced-stats/yesterday",
            "date": "POST /ingest/game-advanced-stats/date",
            "range": "POST /ingest/game-advanced-stats/range",
            "backfill": "POST /ingest/game-advanced-stats/backfill",
        },
        "notes": {
            "v2_availability": "V2 advanced stats available from 2015 season onwards",
            "data_availability": "Stats only available after game completion",
            "period_0": "period=0 returns full game aggregates",
        },
    }
