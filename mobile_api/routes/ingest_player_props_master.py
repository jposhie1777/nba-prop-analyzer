from fastapi import APIRouter
from ingest_player_props_master import ingest_player_props_master

router = APIRouter(
    prefix="/ingest",
    tags=["ingest"],
)

@router.post("/player-props-master")
def run_master_player_props_ingest():
    """
    Triggers the MASTER player props ingest.
    WRITE_MODE controls behavior:
      - DRY_RUN
      - STAGING_ONLY
      - SWAP
    """
    result = ingest_player_props_master("")
    return result