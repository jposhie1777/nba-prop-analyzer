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
# Request Model
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
    # 🚨 ENTRY CONFIRMATION
    print("🚨🚨🚨 ENTERED /ingest/player-props-master 🚨🚨🚨", flush=True)
    print("📦 REQUEST BODY:", req.dict(), flush=True)

    try:
        # --------------------------------------------------
        # Apply request-scoped overrides
        # --------------------------------------------------
        if req.dataset:
            os.environ["PROP_DATASET"] = req.dataset
        if req.staging_table:
            os.environ["PROP_TABLE_STAGING"] = req.staging_table
        if req.final_table:
            os.environ["PROP_TABLE_FINAL"] = req.final_table
        if req.write_mode:
            os.environ["WRITE_MODE"] = req.write_mode

        # --------------------------------------------------
        # Resolve game date
        # --------------------------------------------------
        game_date = req.date or today_ny()

        print("🧠 ABOUT TO RUN MASTER INGEST", {
            "game_date": game_date,
            "WRITE_MODE": os.getenv("WRITE_MODE"),
            "DATASET": os.getenv("PROP_DATASET"),
            "STAGING_TABLE": os.getenv("PROP_TABLE_STAGING"),
            "FINAL_TABLE": os.getenv("PROP_TABLE_FINAL"),
        }, flush=True)

        # --------------------------------------------------
        # RUN INGEST (LONG-RUNNING)
        # --------------------------------------------------
        result = ingest_player_props_master(game_date)

        print("✅ MASTER INGEST COMPLETED", result, flush=True)
        return result

    except Exception as e:
        print("❌ MASTER INGEST FAILED", str(e), flush=True)
        raise HTTPException(
            status_code=500,
            detail=f"Master ingest failed: {str(e)}",
        )
