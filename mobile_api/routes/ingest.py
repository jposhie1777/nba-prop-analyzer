# mobile_api/routes/ingest.py
from fastapi import APIRouter, HTTPException
from datetime import datetime
from zoneinfo import ZoneInfo

from ingest_player_props_master import ingest_player_props_master

router = APIRouter(
    prefix="/ingest",
    tags=["ingest"],
)

def today_ny() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()

@router.post("/player-props-master")
def run_master_ingest(
    date: str | None = None,
    dataset: str | None = None,
    staging_table: str | None = None,
    final_table: str | None = None,
    write_mode: str | None = None,
):
    try:
        # Inject overrides ONLY for this request
        if dataset:
            os.environ["PROP_DATASET"] = dataset
        if staging_table:
            os.environ["PROP_TABLE_STAGING"] = staging_table
        if final_table:
            os.environ["PROP_TABLE_FINAL"] = final_table
        if write_mode:
            os.environ["WRITE_MODE"] = write_mode

        game_date = date or today_ny()
        return ingest_player_props_master(game_date)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Master ingest failed: {str(e)}",
        )
