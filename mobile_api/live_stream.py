# live_stream.py
import asyncio
import json
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from fastapi import APIRouter
from starlette.responses import StreamingResponse
from google.cloud import bigquery
from typing import Dict, List, Optional


# ======================================================
# Router
# ======================================================
router = APIRouter(prefix="/live", tags=["live"])


# ======================================================
# Configuration
# ======================================================
REFRESH_INTERVAL_SEC = 20
BQ_TIMEOUT_SEC = 8                  # must be < REFRESH_INTERVAL_SEC
KEEPALIVE_SEC = 15                  # keep SSE alive through proxies
MAX_BACKOFF_SEC = 120

ENABLE_RAW_SNAPSHOTS = True
RAW_SNAPSHOT_TABLE = (
    "graphite-flare-477419-h7.nba_live.live_games_raw_snapshots"
)

# ======================================================
# BigQuery
# ======================================================
from google.cloud import bigquery
import os

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

    # Cloud Run / gcloud auth fallback
    return bigquery.Client()



LIVE_GAMES_QUERY = """
WITH ranked AS (
  SELECT
    game_id,

    home_team_abbr,
    away_team_abbr,

    home_score,
    away_score,

    home_score_q1,
    home_score_q2,
    home_score_q3,
    home_score_q4,

    away_score_q1,
    away_score_q2,
    away_score_q3,
    away_score_q4,

    period,
    clock,
    state,
    ingested_at,

    ROW_NUMBER() OVER (
      PARTITION BY game_id
      ORDER BY ingested_at DESC
    ) AS rn
  FROM `graphite-flare-477419-h7.nba_live.live_games`
  WHERE state = 'LIVE'
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
class LiveState:
    payload: Dict[str, Any]
    last_good_ts: float
    last_attempt_ts: float
    consecutive_failures: int
    source_updated_at: Optional[str] = None


STATE = LiveState(
    payload={"games": [], "meta": {"status": "BOOTING"}},
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
    jitter = random.uniform(0, 1)
    return min(MAX_BACKOFF_SEC, base + jitter)


# ======================================================
# BigQuery fetch (ASYNC SAFE)
# ======================================================
async def fetch_live_snapshot_from_bigquery() -> Dict[str, Any]:
    """
    Fetch latest LIVE games snapshot from BigQuery.

    Returns:
    {
      "games": [...],
      "source_updated_at": ISO timestamp or None
    }
    """

    def _run_query():
        client = get_bq_client()
        return list(client.query(LIVE_GAMES_QUERY).result())


    rows = await asyncio.to_thread(_run_query)

    games: List[Dict[str, Any]] = []
    max_updated_at: Optional[datetime] = None

    for r in rows:
        updated_at = r.ingested_at.replace(tzinfo=timezone.utc)

        if max_updated_at is None or updated_at > max_updated_at:
            max_updated_at = updated_at

        games.append(
            {
                "game_id": r.game_id,
                "home_team": r.home_team_abbr,
                "away_team": r.away_team_abbr,

                "home_score": r.home_score,
                "away_score": r.away_score,

                # ✅ QUARTER SCORES (ONLY ADDITION)
                "home_q": [
                    r.home_score_q1,
                    r.home_score_q2,
                    r.home_score_q3,
                    r.home_score_q4,
                ],
                "away_q": [
                    r.away_score_q1,
                    r.away_score_q2,
                    r.away_score_q3,
                    r.away_score_q4,
                ],

                "period": r.period,
                "clock": r.clock,
            }
        )

    return {
        "games": games,
        "source_updated_at": (
            max_updated_at.isoformat() if max_updated_at else None
        ),
    }

# ======================================================
# Raw JSON snapshot writer (fire-and-forget)
# ======================================================
def write_raw_snapshot(snapshot: dict):
    """
    Writes raw JSON snapshot for debugging.
    NEVER raises. NEVER blocks the event loop.
    """
    try:
        client = get_bq_client()

        row = {
            "snapshot_ts": datetime.now(timezone.utc).isoformat(),
            "source": "ball_dont_lie",
            "payload_json": json.dumps(snapshot),
        }

        errors = client.insert_rows_json(
            RAW_SNAPSHOT_TABLE,
            [row],
        )

        if errors:
            print("⚠️ Raw snapshot insert errors:", errors)

    except Exception as e:
        print("⚠️ Raw snapshot insert failed:", str(e))


# ======================================================
# Background refresher loop (ONE per container)
# ======================================================
async def refresher_loop():
    global STATE

    while True:
        start = _now()
        STATE.last_attempt_ts = start

        try:
            snapshot = await asyncio.wait_for(
                fetch_live_snapshot_from_bigquery(),
                timeout=BQ_TIMEOUT_SEC,
            )


            # fire-and-forget raw snapshot logging
            if ENABLE_RAW_SNAPSHOTS:
                asyncio.create_task(
                    asyncio.to_thread(write_raw_snapshot, snapshot)
                )

            payload = {
                "games": snapshot.get("games", []),
                "meta": {
                    "status": "OK",
                    "server_updated_at": time.strftime(
                        "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                    ),
                    "source_updated_at": snapshot.get("source_updated_at"),
                },
            }

            STATE.payload = payload
            STATE.last_good_ts = _now()
            STATE.source_updated_at = snapshot.get("source_updated_at")
            STATE.consecutive_failures = 0

            elapsed = _now() - start
            await asyncio.sleep(max(0, REFRESH_INTERVAL_SEC - elapsed))
            continue

        except TimeoutError:
            print("⚠️ Live refresher timed out")
            STATE.consecutive_failures += 1

        except Exception:
            import traceback
            print("❌ Live refresher error")
            print(traceback.format_exc())
            STATE.consecutive_failures += 1

        # degraded mode (never wipe games)
        degraded = dict(STATE.payload)
        degraded_meta = dict(degraded.get("meta", {}))
        degraded_meta.update(
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
        degraded["meta"] = degraded_meta
        STATE.payload = degraded

        await asyncio.sleep(_compute_backoff(STATE.consecutive_failures))

# ======================================================
# SSE Endpoint
# ======================================================
@router.get("/scores/stream")
async def live_scores_stream():
    async def gen():
        last_sent: Optional[str] = None
        last_keepalive = 0.0

        while True:
            now = _now()

            # keepalive ping
            if now - last_keepalive >= KEEPALIVE_SEC:
                yield ":keepalive\n\n"
                last_keepalive = now

            data_str = json.dumps(STATE.payload, separators=(",", ":"))
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
# Debug Endpoint
# ======================================================
@router.get("/scores/debug")
def debug_live_state():
    return {
        "last_good_seconds_ago": (
            int(_now() - STATE.last_good_ts) if STATE.last_good_ts else None
        ),
        "last_attempt_seconds_ago": int(_now() - STATE.last_attempt_ts),
        "consecutive_failures": STATE.consecutive_failures,
        "meta": STATE.payload.get("meta"),
        "game_count": len(STATE.payload.get("games", [])),
    }

# ======================================================
# Polling Endpoint (JSON)
# ======================================================
@router.get("/scores")
def get_live_scores():
    """
    Lightweight polling endpoint.
    Returns the latest cached live games snapshot.
    """
    return STATE.payload