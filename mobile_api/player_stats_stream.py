# player_stats_stream.py

import asyncio
import json
import random
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter
from google.cloud import bigquery
from starlette.responses import StreamingResponse
import os

# ======================================================
# Router
# ======================================================

router = APIRouter(
    prefix="/live/player-stats",
    tags=["player-stats"],
)

# ======================================================
# Config
# ======================================================

REFRESH_INTERVAL_SEC = 20
BQ_TIMEOUT_SEC = 8
KEEPALIVE_SEC = 15
MAX_BACKOFF_SEC = 120

# ======================================================
# BigQuery
# ======================================================

def get_bq_client() -> bigquery.Client:
    """
    Unified BigQuery client initializer.

    Works in:
    - Cloud Run (auto project)
    - Local dev / Codespaces (env-based)
    """
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project:
        return bigquery.Client(project=project)
    return bigquery.Client()


PLAYER_STATS_QUERY = """
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY game_id, player_id
      ORDER BY ingested_at DESC
    ) AS rn
  FROM `graphite-flare-477419-h7.nba_live.live_player_stats`
  WHERE game_date >= DATE_SUB(@game_date, INTERVAL 1 DAY)  -- 🔧 FIX (midnight safety)
)
SELECT *
FROM ranked
WHERE rn = 1
ORDER BY ingested_at DESC
"""

# ======================================================
# State
# ======================================================

@dataclass
class PlayerStatsState:
    payload: Dict[str, Any]
    last_good_ts: float
    last_attempt_ts: float
    consecutive_failures: int
    source_updated_at: Optional[str] = None


STATE = PlayerStatsState(
    payload={
        "players": [],
        "meta": {"status": "BOOTING"},
    },
    last_good_ts=0.0,
    last_attempt_ts=0.0,
    consecutive_failures=0,
)

# ======================================================
# Helpers
# ======================================================

def _now() -> float:
    return time.time()


def _compute_backoff(failures: int) -> float:
    base = min(MAX_BACKOFF_SEC, 2 ** min(failures, 6))
    return min(MAX_BACKOFF_SEC, base + random.uniform(0, 1))


# ======================================================
# BigQuery fetch (TODAY – NBA TIME)
# ======================================================

async def fetch_player_stats_snapshot() -> Dict[str, Any]:
    nba_today = datetime.now(ZoneInfo("America/New_York")).date()

    def _run():
        client = get_bq_client()
        job = client.query(
            PLAYER_STATS_QUERY,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "game_date",
                        "DATE",
                        nba_today,
                    )
                ]
            ),
        )
        return list(job.result())

    rows = await asyncio.to_thread(_run)

    # 🔎 DEBUG 2 — inspect raw BigQuery row
    print(f"[DEBUG] BQ rows fetched: {len(rows)}")
    
    if rows:
        r = rows[0]
        print("[DEBUG] SAMPLE BQ ROW:", {
            "game_id": r.game_id,
            "player_id": r.player_id,
            "player_name": r.player_name,
            "team_abbr": r.team_abbr,
            "minutes": r.minutes,
            "period": r.period,
            "fg_made": r.fg_made,
            "fg_att": r.fg_att,
            "ingested_at": str(r.ingested_at),
        })
    
    players: List[Dict[str, Any]] = []
    max_ingested_at: Optional[datetime] = None
    
    for r in rows:
        updated_at = r.ingested_at.replace(tzinfo=timezone.utc)
    
        if not max_ingested_at or updated_at > max_ingested_at:
            max_ingested_at = updated_at
    
        players.append(
            {
                "game_id": r.game_id,
                "player_id": r.player_id,
                "name": r.player_name or "—",
                "team": r.team_abbr,
                "opponent": r.opponent_abbr,
                "minutes": float(r.minutes) if r.minutes is not None else None,
                "pts": r.pts or 0,
                "reb": r.reb or 0,
                "ast": r.ast or 0,
                "stl": r.stl or 0,
                "blk": r.blk or 0,
                "tov": r.tov or 0,
                "fg": [r.fg_made or 0, r.fg_att or 0],
                "fg3": [r.fg3_made or 0, r.fg3_att or 0],
                "ft": [r.ft_made or 0, r.ft_att or 0],
                "plus_minus": r.plus_minus or 0,
                "period": (
                    int(r.period)
                    if r.period and str(r.period).isdigit()
                    else None
                ),
                "clock": r.clock,
            }
        )
    
    # 🔎 DEBUG — final payload size
    print(f"[DEBUG] Normalized players sent: {len(players)}")
    
    return {
        "players": players,
        "source_updated_at": (
            max_ingested_at.isoformat() if max_ingested_at else None
        ),
    }


# ======================================================
# Background refresher loop
# ======================================================

async def player_stats_refresher():
    global STATE

    while True:
        start = _now()
        STATE.last_attempt_ts = start

        try:
            snapshot = await fetch_player_stats_snapshot()

            STATE.payload = {
                "players": snapshot["players"],
                "meta": {
                    "status": "OK",
                    "server_updated_at": datetime.utcnow().isoformat() + "Z",
                    "source_updated_at": snapshot["source_updated_at"],
                },
            }

            STATE.last_good_ts = _now()
            STATE.consecutive_failures = 0

            elapsed = _now() - start
            await asyncio.sleep(max(0, REFRESH_INTERVAL_SEC - elapsed))
            continue

        except Exception:
            STATE.consecutive_failures += 1

            print("❌ Player stats refresher error")
            print(traceback.format_exc())

            degraded = dict(STATE.payload)
            meta = dict(degraded.get("meta", {}))
            meta.update(
                {
                    "status": "DEGRADED",
                    "consecutive_failures": STATE.consecutive_failures,
                    "seconds_since_last_good": (
                        int(_now() - STATE.last_good_ts)
                        if STATE.last_good_ts
                        else None
                    ),
                }
            )

            degraded["meta"] = meta
            STATE.payload = degraded

            await asyncio.sleep(
                _compute_backoff(STATE.consecutive_failures)
            )


# ======================================================
# SSE Endpoint
# ======================================================

@router.get("/stream")
async def player_stats_stream():
    async def gen():
        last_sent = None
        last_keepalive = 0.0

        while True:
            now = _now()

            if now - last_keepalive >= KEEPALIVE_SEC:
                yield ":keepalive\n\n"
                last_keepalive = now

            data_str = json.dumps(
                STATE.payload,
                separators=(",", ":"),
            )

            if data_str != last_sent:
                yield f"event: snapshot\ndata: {data_str}\n\n"
                last_sent = data_str

            await asyncio.sleep(1)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ======================================================
# Polling + Debug
# ======================================================

@router.get("")
def get_player_stats():
    return STATE.payload


@router.get("/debug")
def debug_player_stats():
    players = STATE.payload.get("players", [])

    return {
        "status": STATE.payload.get("meta", {}).get("status"),
        "player_count": len(players),
        "sample_player": players[0] if players else None,
        "last_good_seconds_ago": (
            int(_now() - STATE.last_good_ts)
            if STATE.last_good_ts
            else None
        ),
        "consecutive_failures": STATE.consecutive_failures,
    }