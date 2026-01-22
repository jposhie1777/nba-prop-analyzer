# mobile_api/routes/ingest.py
import os
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ingest_player_props_master import ingest_player_props_master


# ======================================================
# Router
# ======================================================
router = APIRouter(
    prefix="/ingest",
    tags=["ingest"],
)


# ======================================================
# Helpers
# ======================================================
def today_ny() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


# ======================================================
# Request Model (THIS FIXES THE BUG)
# ======================================================
class MasterIngestRequest(BaseModel):
    date: Optional[str] = None
    dataset: Optional[str] = None
    staging_table: Optional[str] = None
    final_table: Optional[str] = None
    write_mode: Optional[str] = None


# ======================================================
# Route
# ======================================================
@router.post("/player-props-master")
def run_master_ingest(req: MasterIngestRequest):
    # 🚨 PROOF THIS ROUTE WAS HIT
    print("🚨🚨🚨 SCHEDULER HIT /ingest/player-props-master 🚨🚨🚨")
    print("DEBUG REQUEST BODY:", req.dict())

    try:
        # Inject overrides ONLY for this request
        if req.dataset:
            os.environ["PROP_DATASET"] = req.dataset
        if req.staging_table:
            os.environ["PROP_TABLE_STAGING"] = req.staging_table
        if req.final_table:
            os.environ["PROP_TABLE_FINAL"] = req.final_table
        if req.write_mode:
            os.environ["WRITE_MODE"] = req.write_mode

        game_date = req.date or today_ny()
        return ingest_player_props_master(game_date)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Master ingest failed: {str(e)}",
        )
