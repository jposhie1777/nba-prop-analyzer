from fastapi import APIRouter, HTTPException
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
    Triggers the MASTER player props ingest for today (NY time).

    Safe behavior:
    - Upstream instability returns a controlled response
    - No partial writes
    - Scheduler-friendly
    """
    date = today_ny()

    try:
        result = ingest_player_props_master(date)
    except Exception as e:
        # Only truly unexpected failures should bubble up
        raise HTTPException(
            status_code=500,
            detail=f"Master ingest failed unexpectedly: {str(e)}",
        )

    # Explicitly surface the benign no-games case
    if result.get("status") == "no_games":
        return {
            "status": "no_games",
            "date": date,
            "message": "No games available or upstream unavailable — safe skip",
        }

    return result