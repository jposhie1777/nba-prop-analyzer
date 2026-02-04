from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .ingest import (
    ingest_atp_race,
    ingest_historical,
    ingest_matches,
    ingest_players,
    ingest_rankings,
    ingest_tournaments,
)


router = APIRouter(
    prefix="/ingest/atp",
    tags=["ingest-atp"],
)


class AtpHistoricalIngestRequest(BaseModel):
    start_season: Optional[int] = None
    end_season: Optional[int] = None
    include_players: bool = True
    include_tournaments: bool = True
    include_matches: bool = True
    include_rankings: bool = True
    include_atp_race: bool = True
    create_tables: bool = True


class AtpSeasonIngestRequest(BaseModel):
    season: int
    include_tournaments: bool = True
    include_matches: bool = True
    create_tables: bool = True


@router.post("/historical")
def run_historical(req: AtpHistoricalIngestRequest):
    try:
        return ingest_historical(
            start_season=req.start_season,
            end_season=req.end_season,
            include_players=req.include_players,
            include_tournaments=req.include_tournaments,
            include_matches=req.include_matches,
            include_rankings=req.include_rankings,
            include_atp_race=req.include_atp_race,
            create_tables=req.create_tables,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/season")
def run_season(req: AtpSeasonIngestRequest):
    try:
        summary = {"season": req.season}
        tournaments_payload = None
        if req.include_tournaments:
            tournaments_payload = ingest_tournaments(
                season=req.season,
                create_tables=req.create_tables,
            )
            summary["tournaments"] = {
                "records": tournaments_payload.get("records"),
                "inserted": tournaments_payload.get("inserted"),
            }
        if req.include_matches:
            summary["matches"] = ingest_matches(
                season=req.season,
                tournament_ids=(
                    tournaments_payload.get("tournament_ids") if tournaments_payload else None
                ),
                create_tables=req.create_tables,
            )
        return summary
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/players")
def run_players():
    try:
        return ingest_players()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/rankings")
def run_rankings(ranking_date: Optional[str] = None):
    try:
        return ingest_rankings(ranking_date=ranking_date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/race")
def run_race(ranking_date: Optional[str] = None):
    try:
        return ingest_atp_race(ranking_date=ranking_date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
