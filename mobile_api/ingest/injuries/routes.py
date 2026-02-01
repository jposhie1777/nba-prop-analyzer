"""
Injuries and WOWY API Routes.

Provides endpoints for:
1. /injuries - List current injuries (for Injuries tab)
2. /injuries/wowy - WOWY analysis (for WOWY tab)
3. /injuries/ingest - Trigger injury data refresh
"""
# mobile/api/ingest/injuries/routes.py
import asyncio
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from google.cloud import bigquery

from .ingest import ingest_injuries, get_current_injuries
from .wowy import (
    analyze_wowy_for_player,
    get_wowy_for_injured_players,
    get_top_beneficiaries,
    get_current_season,
)

# ==================================================
# Router
# ==================================================
router = APIRouter(
    prefix="/injuries",
    tags=["injuries", "wowy"],
)


# ==================================================
# Request/Response Models
# ==================================================
class IngestResponse(BaseModel):
    status: str
    message: str
    data: Optional[dict] = None


class InjuryRecord(BaseModel):
    injury_id: Optional[int]
    player_id: int
    player_name: str
    team_id: int
    team_abbreviation: str
    team_name: Optional[str]
    status: str
    injury_type: Optional[str]
    report_date: Optional[str]
    return_date: Optional[str]


# ==================================================
# INJURIES TAB ENDPOINTS
# ==================================================

@router.get("")
async def get_injuries(
    team_id: Optional[int] = Query(None, description="Filter by team ID"),
    team: Optional[str] = Query(None, description="Filter by team abbreviation (e.g., 'LAL')"),
    status: Optional[str] = Query(None, description="Filter by status (Out, Questionable, Doubtful, Day-To-Day)"),
):
    """
    Get current player injuries.

    Use this endpoint for the Injuries tab in your app.

    Returns all currently injured players with their status and injury type.
    """
    from bq import get_bq_client

    client = get_bq_client()

    where_clauses = ["TRUE"]

    if team_id:
        where_clauses.append(f"team_id = {team_id}")

    if team:
        safe_team = team.upper().replace("'", "''")
        where_clauses.append(f"team_abbreviation = '{safe_team}'")

    if status:
        safe_status = status.replace("'", "''")
        where_clauses.append(f"status = '{safe_status}'")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        injury_id,
        player_id,
        player_name,
        player_first_name,
        player_last_name,
        team_id,
        team_abbreviation,
        team_name,
        status,
        injury_type,
        report_date,
        return_date,
        ingested_at
    FROM `nba_live.player_injuries`
    WHERE {where_sql}
    ORDER BY
        CASE status
            WHEN 'Out' THEN 1
            WHEN 'Doubtful' THEN 2
            WHEN 'Questionable' THEN 3
            WHEN 'Day-To-Day' THEN 4
            WHEN 'Probable' THEN 5
            ELSE 6
        END,
        team_abbreviation,
        player_last_name
    """

    try:
        rows = [dict(r) for r in client.query(query).result()]

        # Group by team for easier display
        by_team = {}
        for row in rows:
            team_abbr = row["team_abbreviation"]
            if team_abbr not in by_team:
                by_team[team_abbr] = {
                    "team": team_abbr,
                    "team_name": row["team_name"],
                    "team_id": row["team_id"],
                    "injuries": [],
                }
            by_team[team_abbr]["injuries"].append(row)

        return {
            "count": len(rows),
            "injuries": rows,
            "by_team": list(by_team.values()),
            "status_summary": {
                "out": sum(1 for r in rows if r["status"] == "Out"),
                "doubtful": sum(1 for r in rows if r["status"] == "Doubtful"),
                "questionable": sum(1 for r in rows if r["status"] == "Questionable"),
                "day_to_day": sum(1 for r in rows if r["status"] == "Day-To-Day"),
                "probable": sum(1 for r in rows if r["status"] == "Probable"),
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch injuries: {str(e)}")


@router.get("/team/{team_abbr}")
async def get_team_injuries(team_abbr: str):
    """
    Get injuries for a specific team by abbreviation.

    Example: /injuries/team/LAL
    """
    return await get_injuries(team=team_abbr)


# ==================================================
# WOWY TAB ENDPOINTS
# ==================================================

@router.get("/wowy/player/{player_id}")
async def get_player_wowy(
    player_id: int,
    season: Optional[int] = Query(None, description="Season year (default: current)"),
    min_games_with: int = Query(5, ge=1, description="Minimum games with player"),
    min_games_without: int = Query(3, ge=1, description="Minimum games without player"),
):
    """
    Get WOWY (With Or Without You) analysis for a specific player.

    Shows how each teammate's stats change when this player is OUT.

    Example: /injuries/wowy/player/237 (Anthony Davis)

    Returns:
    - target_player: Info about the player being analyzed
    - team_impact: Team-level PPG with/without player
    - teammates: Each teammate's stats with/without the player
    """
    try:
        result = await asyncio.to_thread(
            analyze_wowy_for_player,
            player_id,
            season=season,
            min_games_with=min_games_with,
            min_games_without=min_games_without,
        )

        if result.get("status") == "error":
            raise HTTPException(status_code=404, detail=result.get("error"))

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"WOWY analysis failed: {str(e)}")


@router.get("/wowy/injured")
async def get_injured_players_wowy(
    team_id: Optional[int] = Query(None, description="Filter by team ID"),
    team: Optional[str] = Query(None, description="Filter by team abbreviation"),
    status: Optional[str] = Query(None, description="Filter by injury status"),
    season: Optional[int] = Query(None, description="Season year (default: current)"),
    today_only: bool = Query(True, description="Only include players in today's games"),
):
    """
    Get WOWY analysis for all currently injured players.

    This is the main endpoint for the WOWY tab - shows how each
    injured player's absence affects their teammates.

    Returns list of injured players with teammate impact analysis.
    """
    from bq import get_bq_client

    try:
        # Build status filter
        status_filter = None
        if status:
            status_filter = [status]

        # Resolve team abbreviation to ID if provided
        resolved_team_id = team_id
        if team and not team_id:
            client = get_bq_client()
            team_query = f"""
            SELECT DISTINCT team_id
            FROM `nba_live.player_injuries`
            WHERE team_abbreviation = '{team.upper()}'
            LIMIT 1
            """
            team_rows = list(client.query(team_query).result())
            if team_rows:
                resolved_team_id = team_rows[0]["team_id"]

        result = await asyncio.to_thread(
            get_wowy_for_injured_players,
            team_id=resolved_team_id,
            status_filter=status_filter,
            season=season,
            only_today_games=today_only,
        )

        return {
            "count": len(result),
            "season": season or get_current_season(),
            "injured_players": result,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"WOWY analysis failed: {str(e)}")


@router.get("/wowy/beneficiaries/{player_id}")
async def get_wowy_beneficiaries(
    player_id: int,
    stat: str = Query("pts", description="Stat to rank by (pts, reb, ast, fg3m, min)"),
    limit: int = Query(5, ge=1, le=15, description="Number of top beneficiaries"),
    season: Optional[int] = Query(None, description="Season year"),
):
    """
    Get teammates who benefit most when a player is out.

    Example: /injuries/wowy/beneficiaries/237?stat=pts&limit=5

    Great for identifying prop bet opportunities when a star is out.
    """
    try:
        result = await asyncio.to_thread(
            get_top_beneficiaries,
            player_id,
            stat=stat,
            limit=limit,
            season=season,
        )

        return {
            "player_id": player_id,
            "stat": stat,
            "beneficiaries": result,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get beneficiaries: {str(e)}")


# ==================================================
# INGEST ENDPOINTS
# ==================================================

@router.post("/ingest")
async def trigger_injury_ingest():
    """
    Trigger a refresh of injury data from BallDontLie API.

    This fetches the latest injuries and updates the database,
    THEN refreshes the WOWY cache.
    """
    try:
        result = await asyncio.to_thread(ingest_injuries)

        if result.get("status") != "ok":
            raise HTTPException(status_code=500, detail=result.get("error"))

        # 🔁 Trigger WOWY cache refresh AFTER injuries ingest
        from services.http import post_internal
        season = get_current_season()
        post_internal(f"/injuries/wowy/cache/refresh?season={season}")

        return IngestResponse(
            status="ok",
            message=f"Ingested {result.get('injuries_inserted', 0)} injuries",
            data=result,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingest failed: {str(e)}")


@router.get("/status")
async def get_injuries_status():
    """
    Get status and available endpoints for injuries API.
    """
    return {
        "status": "ok",
        "endpoints": {
            "list_injuries": "GET /injuries",
            "team_injuries": "GET /injuries/team/{team_abbr}",
            "player_wowy": "GET /injuries/wowy/player/{player_id}",
            "injured_wowy": "GET /injuries/wowy/injured",
            "beneficiaries": "GET /injuries/wowy/beneficiaries/{player_id}",
            "ingest": "POST /injuries/ingest",
        },
        "description": {
            "injuries_tab": "Use GET /injuries for the Injuries tab",
            "wowy_tab": "Use GET /injuries/wowy/injured for the WOWY tab",
        },
    }


# ==================================================
# SCHEMA ENDPOINTS (for auto-table support)
# ==================================================

@router.get("/schema")
async def get_injuries_schema():
    """Get column schema for injuries table (auto-table support)."""
    return [
        {"name": "player_name", "type": "STRING"},
        {"name": "team_abbreviation", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "injury_type", "type": "STRING"},
        {"name": "report_date", "type": "DATE"},
        {"name": "return_date", "type": "DATE"},
    ]


@router.get("/wowy/schema")
async def get_wowy_schema():
    """Get column schema for WOWY data (auto-table support)."""
    return [
        {"name": "teammate_name", "type": "STRING"},
        {"name": "games_with", "type": "INTEGER"},
        {"name": "games_without", "type": "INTEGER"},
        {"name": "pts_with", "type": "FLOAT"},
        {"name": "pts_without", "type": "FLOAT"},
        {"name": "pts_diff", "type": "FLOAT"},
        {"name": "reb_with", "type": "FLOAT"},
        {"name": "reb_without", "type": "FLOAT"},
        {"name": "reb_diff", "type": "FLOAT"},
        {"name": "ast_with", "type": "FLOAT"},
        {"name": "ast_without", "type": "FLOAT"},
        {"name": "ast_diff", "type": "FLOAT"},
    ]

@router.get("/wowy/injured/cached")
async def get_cached_wowy(
    season: int = Query(...),
    stat: str = Query("pts"),
):
    from bq import get_bq_client

    client = get_bq_client()

    query = """
    SELECT
      injured_player,
      team_impact,
      teammates
    FROM `nba_live.wowy_injured_cache`
    WHERE season = @season
      AND stat = @stat
    ORDER BY team_impact.team_ppg_diff ASC
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("season", "INT64", season),
                bigquery.ScalarQueryParameter("stat", "STRING", stat),
            ]
        ),
    )

    rows = [dict(r) for r in job.result()]

    return {
        "count": len(rows),
        "season": season,
        "injured_players": rows,
    }


@router.post("/wowy/cache/refresh")
async def refresh_wowy_cache(
    season: int = Query(...),
):
    from services.wowy_cache import refresh_wowy_cache_for_season

    result = await asyncio.to_thread(
        refresh_wowy_cache_for_season,
        season,
    )

    return {
        "status": "ok",
        "season": season,
        "rows_written": result,
    }
