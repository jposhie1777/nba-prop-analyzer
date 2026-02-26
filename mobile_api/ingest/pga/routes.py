from __future__ import annotations

from datetime import date as DateType
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .round_scores import ingest_round_scores
from .pga_pairings_ingest import ingest_pairings


router = APIRouter(
    prefix="/ingest/pga",
    tags=["ingest-pga"],
)


class PgaRoundScoresRequest(BaseModel):
    date: Optional[DateType] = None
    season: Optional[int] = None
    tournament_id: Optional[int] = None
    round_number: Optional[int] = None
    create_tables: bool = True
    dry_run: bool = False


@router.post("/round-scores")
def run_round_scores(req: PgaRoundScoresRequest):
    try:
        return ingest_round_scores(
            target_date=req.date.isoformat() if req.date else None,
            season=req.season,
            tournament_id=req.tournament_id,
            round_number=req.round_number,
            create_tables=req.create_tables,
            dry_run=req.dry_run,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


class PgaPairingsRequest(BaseModel):
    tournament_id: str
    round_number: int
    cut: Optional[str] = None  # "ALL" | "MADE" | "MISSED"
    create_tables: bool = True
    dry_run: bool = False


@router.post("/pairings")
def run_pairings(req: PgaPairingsRequest):
    """
    Fetch round pairings/tee times from the PGA Tour GraphQL API and insert
    into BigQuery.

    - ``tournament_id``: PGA Tour tournament ID, e.g. ``"R2025016"``
    - ``round_number``: 1–4
    - ``cut``: optional filter – ``"ALL"``, ``"MADE"``, or ``"MISSED"``
    - ``dry_run``: fetch and return data without writing to BigQuery
    """
    try:
        return ingest_pairings(
            tournament_id=req.tournament_id,
            round_number=req.round_number,
            cut=req.cut,
            create_tables=req.create_tables,
            dry_run=req.dry_run,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
