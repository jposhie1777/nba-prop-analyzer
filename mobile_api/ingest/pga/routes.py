from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .round_scores import ingest_round_scores


router = APIRouter(
    prefix="/ingest/pga",
    tags=["ingest-pga"],
)


class PgaRoundScoresRequest(BaseModel):
    date: Optional[str] = None
    season: Optional[int] = None
    tournament_id: Optional[int] = None
    round_number: Optional[int] = None
    create_tables: bool = True
    dry_run: bool = False


@router.post("/round-scores")
def run_round_scores(req: PgaRoundScoresRequest):
    try:
        return ingest_round_scores(
            target_date=req.date,
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
