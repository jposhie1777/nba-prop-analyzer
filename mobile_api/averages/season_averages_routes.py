from fastapi import APIRouter
from averages.ingest_player_season_averages import ingest_player_season_averages

router = APIRouter(
    prefix="/admin/ingest/season-averages",
    tags=["admin"],
)

@router.post("")
def ingest_season_averages():
    return ingest_player_season_averages()