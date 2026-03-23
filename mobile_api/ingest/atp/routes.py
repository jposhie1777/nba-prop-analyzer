from __future__ import annotations

from datetime import datetime
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
    ingest_upcoming_scheduled_matches,
)
from .sackmann_ingest import (
    ingest_sackmann_backfill,
    ingest_sackmann_daily,
    ingest_sackmann_years,
    rebuild_sackmann_features,
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


class AtpUpcomingScheduledIngestRequest(BaseModel):
    season: Optional[int] = None
    cutoff_time: Optional[str] = None
    cutoff_day_offset: int = 0
    tournament_ids: Optional[list[int]] = None
    round_name: Optional[str] = None
    include_completed: bool = False
    per_page: int = 100
    max_pages: Optional[int] = None
    create_tables: bool = True


class AtpSackmannBackfillRequest(BaseModel):
    start_year: int = 1968
    end_year: int = datetime.utcnow().year
    years: Optional[list[int]] = None
    include_challenger: bool = True
    include_futures: bool = False
    truncate_raw: bool = True
    rebuild_features: bool = True


class AtpSackmannDailyRequest(BaseModel):
    include_challenger: bool = True
    include_futures: bool = False
    years_back: int = 2
    rebuild_features: bool = True


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


@router.post("/upcoming-scheduled")
def run_upcoming_scheduled(req: AtpUpcomingScheduledIngestRequest):
    try:
        return ingest_upcoming_scheduled_matches(
            season=req.season or datetime.utcnow().year,
            cutoff_time=req.cutoff_time,
            cutoff_day_offset=req.cutoff_day_offset,
            tournament_ids=req.tournament_ids,
            round_name=req.round_name,
            include_completed=req.include_completed,
            per_page=req.per_page,
            max_pages=req.max_pages,
            create_tables=req.create_tables,
        )
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


@router.post("/sackmann/backfill")
def run_sackmann_backfill(req: AtpSackmannBackfillRequest):
    try:
        if req.years:
            return ingest_sackmann_years(
                years=req.years,
                include_challenger=req.include_challenger,
                include_futures=req.include_futures,
                truncate_raw=req.truncate_raw,
                rebuild_features=req.rebuild_features,
            )
        return ingest_sackmann_backfill(
            start_year=req.start_year,
            end_year=req.end_year,
            include_challenger=req.include_challenger,
            include_futures=req.include_futures,
            truncate_raw=req.truncate_raw,
            rebuild_features=req.rebuild_features,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sackmann/daily")
def run_sackmann_daily(req: AtpSackmannDailyRequest):
    try:
        return ingest_sackmann_daily(
            include_challenger=req.include_challenger,
            include_futures=req.include_futures,
            years_back=req.years_back,
            rebuild_features=req.rebuild_features,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sackmann/rebuild-features")
def run_sackmann_rebuild_features():
    try:
        rebuild_sackmann_features()
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
