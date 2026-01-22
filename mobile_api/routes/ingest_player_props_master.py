from fastapi import APIRouter
from ingest_player_props_master import ingest_player_props_master
from zoneinfo import ZoneInfo
from datetime import datetime

router = APIRouter(
    prefix="/ingest",
    tags=["ingest"],
)

def today_ny() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()

@router.post("/player-props-master")
def run_master_player_props_ingest():
    """
    Triggers the MASTER player props ingest.
    """
    return ingest_player_props_master(today_ny())