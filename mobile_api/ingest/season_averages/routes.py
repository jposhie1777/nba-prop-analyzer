# mobile_api/ingest/season_averages/routes.py
"""
API Routes for Season Averages Ingestion

Endpoints for:
- Ingesting current season averages (daily scheduled job)
- Ingesting specific season averages
- Ingesting player-only or team-only averages
"""

import asyncio
from datetime import datetime
from typing import Optional, List
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .ingest import (
    ingest_current_season,
    ingest_all_season_averages,
    ingest_player_season_averages,
    ingest_team_season_averages,
    get_current_season,
    PLAYER_CATEGORY_TYPES,
    TEAM_CATEGORY_TYPES,
)


# ======================================================
# Router
# ======================================================

router = APIRouter(
    prefix="/ingest/season-averages",
    tags=["ingest", "season-averages"],
)

NBA_TZ = ZoneInfo("America/New_York")


# ======================================================
# Request/Response Models
# ======================================================

class SeasonIngestRequest(BaseModel):
    season: int
    season_type: str = "regular"  # regular, playoffs, ist, playin


class PlayerIngestRequest(BaseModel):
    season: int
    season_type: str = "regular"
    categories: Optional[List[str]] = None  # None = all categories


class TeamIngestRequest(BaseModel):
    season: int
    season_type: str = "regular"
    categories: Optional[List[str]] = None  # None = all categories


class IngestResponse(BaseModel):
    status: str
    message: str
    data: Optional[dict] = None


# ======================================================
# Routes
# ======================================================

@router.post("/current", response_model=IngestResponse)
async def ingest_current_route(season_type: str = "regular"):
    """
    Ingest season averages for the current NBA season.

    This is the primary endpoint for daily scheduled ingestion.
    Fetches both player and team season averages for all categories.

    Args:
        season_type: regular, playoffs, ist, playin (default: regular)

    Returns:
        Ingest summary with record counts
    """
    print(f"[ROUTE] /ingest/season-averages/current called (type={season_type})")

    try:
        result = await asyncio.to_thread(
            ingest_current_season,
            season_type=season_type,
        )

        player_count = result.get("player", {}).get("total_records", 0)
        team_count = result.get("team", {}).get("total_records", 0)

        return IngestResponse(
            status="ok",
            message=f"Ingested {player_count} player records and {team_count} team records for season {result['season']}",
            data=result,
        )

    except Exception as e:
        print(f"[ROUTE] Error in current season ingest: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest current season averages: {str(e)}",
        )


@router.post("/season", response_model=IngestResponse)
async def ingest_season_route(request: SeasonIngestRequest):
    """
    Ingest season averages for a specific season.

    Fetches both player and team season averages for all categories.

    Args:
        request.season: Season year (e.g., 2024 for 2024-2025)
        request.season_type: regular, playoffs, ist, playin

    Returns:
        Ingest summary with record counts
    """
    print(f"[ROUTE] /ingest/season-averages/season called for {request.season} ({request.season_type})")

    try:
        result = await asyncio.to_thread(
            ingest_all_season_averages,
            season=request.season,
            season_type=request.season_type,
        )

        player_count = result.get("player", {}).get("total_records", 0)
        team_count = result.get("team", {}).get("total_records", 0)

        return IngestResponse(
            status="ok",
            message=f"Ingested {player_count} player records and {team_count} team records for season {request.season}",
            data=result,
        )

    except Exception as e:
        print(f"[ROUTE] Error in season ingest: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest season {request.season} averages: {str(e)}",
        )


@router.post("/player", response_model=IngestResponse)
async def ingest_player_route(request: PlayerIngestRequest):
    """
    Ingest player season averages only.

    Args:
        request.season: Season year (e.g., 2024 for 2024-2025)
        request.season_type: regular, playoffs, ist, playin
        request.categories: Optional list of categories (default: all)

    Returns:
        Ingest summary with record counts
    """
    print(f"[ROUTE] /ingest/season-averages/player called for {request.season}")

    try:
        result = await asyncio.to_thread(
            ingest_player_season_averages,
            season=request.season,
            season_type=request.season_type,
            categories=request.categories,
        )

        return IngestResponse(
            status="ok",
            message=f"Ingested {result['total_records']} player records for season {request.season}",
            data=result,
        )

    except Exception as e:
        print(f"[ROUTE] Error in player ingest: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest player averages: {str(e)}",
        )


@router.post("/team", response_model=IngestResponse)
async def ingest_team_route(request: TeamIngestRequest):
    """
    Ingest team season averages only.

    Args:
        request.season: Season year (e.g., 2024 for 2024-2025)
        request.season_type: regular, playoffs, ist, playin
        request.categories: Optional list of categories (default: all)

    Returns:
        Ingest summary with record counts
    """
    print(f"[ROUTE] /ingest/season-averages/team called for {request.season}")

    try:
        result = await asyncio.to_thread(
            ingest_team_season_averages,
            season=request.season,
            season_type=request.season_type,
            categories=request.categories,
        )

        return IngestResponse(
            status="ok",
            message=f"Ingested {result['total_records']} team records for season {request.season}",
            data=result,
        )

    except Exception as e:
        print(f"[ROUTE] Error in team ingest: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest team averages: {str(e)}",
        )


@router.get("/status")
async def get_status():
    """
    Get the status of the season averages ingestion system.

    Returns:
        Current configuration, available categories, and endpoint info
    """
    now_et = datetime.now(NBA_TZ)
    current_season = get_current_season()

    return {
        "status": "ok",
        "current_time_et": now_et.isoformat(),
        "current_season": current_season,
        "season_display": f"{current_season}-{current_season + 1}",
        "endpoints": {
            "current": "POST /ingest/season-averages/current",
            "season": "POST /ingest/season-averages/season",
            "player": "POST /ingest/season-averages/player",
            "team": "POST /ingest/season-averages/team",
        },
        "player_categories": {
            cat: types for cat, types in PLAYER_CATEGORY_TYPES.items()
        },
        "team_categories": {
            cat: types for cat, types in TEAM_CATEGORY_TYPES.items()
        },
        "season_types": ["regular", "playoffs", "ist", "playin"],
        "notes": {
            "daily_schedule": "Runs at 6:00 AM ET after game advanced stats ingest",
            "data_freshness": "Season averages update after each game",
        },
    }
