from fastapi import APIRouter, Query
from google.cloud import bigquery
from typing import List, Optional
from bq import get_bq_client



router = APIRouter(
    prefix="/props/player-props",
    tags=["player-props-master"],
)

bq = get_bq_client()

# --------------------------------------
# CONSTANTS / DEFAULTS
# --------------------------------------
PROJECT_ID = "graphite-flare-477419-h7"
DATASET = "nba_live"
LATEST_VIEW = "v_player_prop_odds_master_latest"

DEFAULT_MARKET_WINDOW = "FULL"
DEFAULT_MARKET_KEYS = ["pts", "reb", "ast", "3pm"]

ALLOWED_WINDOWS = {
    "FULL",
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "H1",
    "H2",
    "FIRST3MIN",
}

# --------------------------------------
# MASTER PLAYER PROPS (LATEST SNAPSHOT)
# --------------------------------------
@router.get("/master")
def get_master_player_props(
    market_window: str = Query(
        DEFAULT_MARKET_WINDOW,
        description="Market window (FULL, Q1, H1, FIRST3MIN, etc.)",
    ),
    market_keys: str = Query(
        ",".join(DEFAULT_MARKET_KEYS),
        description="Comma-separated market keys (pts,reb,ast,3pm,...)",
    ),
    game_id: Optional[int] = Query(
        None,
        description="Optional game_id filter",
    ),
    player_id: Optional[int] = Query(
        None,
        description="Optional player_id filter",
    ),
):
    """
    Canonical, window-aware, MASTER player props endpoint.

    Guarantees:
    - Reads ONLY from the latest master snapshot
    - Never mixes snapshots
    - Window-aware (FULL / Q1 / H1 / FIRST3MIN / etc.)
    - Safe defaults if no params are provided
    """

    mw = market_window.upper().strip()
    if mw not in ALLOWED_WINDOWS:
        return {
            "error": f"Invalid market_window '{market_window}'",
            "allowed": sorted(ALLOWED_WINDOWS),
        }

    keys: List[str] = [k.strip().lower() for k in market_keys.split(",") if k.strip()]
    if not keys:
        keys = DEFAULT_MARKET_KEYS

    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{LATEST_VIEW}`
    WHERE market_window = @market_window
      AND market_key IN UNNEST(@market_keys)
    """

    params = [
        bigquery.ScalarQueryParameter("market_window", "STRING", mw),
        bigquery.ArrayQueryParameter("market_keys", "STRING", keys),
    ]

    if game_id is not None:
        sql += " AND game_id = @game_id"
        params.append(
            bigquery.ScalarQueryParameter("game_id", "INT64", game_id)
        )

    if player_id is not None:
        sql += " AND player_id = @player_id"
        params.append(
            bigquery.ScalarQueryParameter("player_id", "INT64", player_id)
        )

    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=params
        ),
    )

    rows = [dict(r) for r in job.result()]

    return {
        "source": LATEST_VIEW,
        "market_window": mw,
        "market_keys": keys,
        "row_count": len(rows),
        "rows": rows,
    }