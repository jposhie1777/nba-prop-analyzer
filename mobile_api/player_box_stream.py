# player_box_stream.py

import asyncio
import json
import os
import random
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter
from google.cloud import bigquery
from starlette.responses import StreamingResponse

# ======================================================
# NBA / EST Timezone (AUTHORITATIVE "TODAY")
# ======================================================

NBA_TZ = ZoneInfo("America/New_York")


def nba_today() -> date:
    return datetime.now(NBA_TZ).date()


# ======================================================
# Ingest Control (WRITE-SIDE ONLY)
# ======================================================

ENABLE_PLAYER_STATS_INGEST = (
    os.getenv("ENABLE_PLAYER_STATS_INGEST", "false").lower() == "true"
)

# ======================================================
# Router
# ======================================================

router = APIRouter(
    prefix="/live/player-box",
    tags=["player-box"],
)

# ======================================================
# Config
# ======================================================

REFRESH_INTERVAL_SEC = 30
BQ_TIMEOUT_SEC = 12
KEEPALIVE_SEC = 20
MAX_BACKOFF_SEC = 180

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



BOX_SCORES_QUERY = """
SELECT
  snapshot_ts,
  payload
FROM `graphite-flare-477419-h7.nba_live.box_scores_raw`
WHERE game_date = @game_date
ORDER BY snapshot_ts DESC
LIMIT 1
"""

# ======================================================
# State
# ======================================================

@dataclass
class PlayerBoxState:
    payload: Dict[str, Any]
    last_good_ts: float
    last_attempt_ts: float
    consecutive_failures: int


STATE = PlayerBoxState(
    payload={
        "games": [],
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
# BigQuery Fetch
# ======================================================

async def fetch_latest_box_scores(
    game_date: Optional[date] = None,
) -> Dict[str, Any]:
    if game_date is None:
        game_date = nba_today()

    def _run():
        client = get_bq_client()
        job = client.query(
            BOX_SCORES_QUERY,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "game_date",
                        "DATE",
                        game_date,
                    )
                ]
            ),
        )
        rows = list(job.result())
        return rows[0] if rows else None

    row = await asyncio.to_thread(_run)

    if not row:
        return {
            "games": [],
            "source_updated_at": None,
        }

    raw = row.payload
    payload = json.loads(raw) if isinstance(raw, str) else raw

    return {
        "games": payload.get("data", []),
        "source_updated_at": row.snapshot_ts.isoformat(),
    }


# ======================================================
# 🔴 WRITE SIDE: Flatten + Write Player Stats
# ======================================================

def flatten_and_write_player_stats(
    snapshot: dict,
    game_date: date,
):
    # 🔒 HARD KILL SWITCH
    if not ENABLE_PLAYER_STATS_INGEST:
        return

    client = get_bq_client()
    errors = client.insert_rows_json(table_id, rows)
    table_id = "graphite-flare-477419-h7.nba_live.live_player_stats"

    now = datetime.now(timezone.utc).isoformat()
    rows: List[Dict[str, Any]] = []

    for game in snapshot.get("games", []):
        game_id = game.get("id")
        game_date_final = game_date.isoformat()
        status = game.get("status")

        if status in ("scheduled", "pre"):
            game_state = "PRE"
        elif status in ("in_progress", "live"):
            game_state = "LIVE"
        elif status in ("final", "finished"):
            game_state = "FINAL"
        else:
            game_state = "UNKNOWN"

        period = game.get("period")
        clock = game.get("clock")

        for side, team_key, opp_key in [
            ("HOME", "home_team", "visitor_team"),
            ("AWAY", "visitor_team", "home_team"),
        ]:
            team = game.get(team_key) or {}
            opponent_team = game.get(opp_key) or {}

            team_abbr = team.get("abbreviation")
            opponent_abbr = opponent_team.get("abbreviation")

            for p in team.get("players", []):
                player_meta = p.get("player", {})

                rows.append(
                    {
                        "game_id": game_id,
                        "game_date": game_date_final,
                        "game_state": game_state,
                        "player_id": player_meta.get("id"),
                        "player_name": (
                            f"{player_meta.get('first_name', '')} "
                            f"{player_meta.get('last_name', '')}"
                        ).strip(),
                        "team_abbr": team_abbr,
                        "opponent_abbr": opponent_abbr,
                        "minutes": p.get("min"),
                        "pts": p.get("pts"),
                        "reb": p.get("reb"),
                        "ast": p.get("ast"),
                        "stl": p.get("stl"),
                        "blk": p.get("blk"),
                        "tov": p.get("turnover"),
                        "fg_made": p.get("fgm"),
                        "fg_att": p.get("fga"),
                        "fg3_made": p.get("fg3m"),
                        "fg3_att": p.get("fg3a"),
                        "ft_made": p.get("ftm"),
                        "ft_att": p.get("fta"),
                        "plus_minus": p.get("plus_minus"),
                        "period": period,
                        "clock": clock,
                        "ingested_at": now,
                    }
                )

    if rows:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            raise RuntimeError(f"Player stat insert errors: {errors}")


# ======================================================
# Background Refresher Loop (READ SIDE ONLY)
# ======================================================

async def player_box_refresher():
    global STATE

    while True:
        start = _now()
        STATE.last_attempt_ts = start

        try:
            snapshot = await fetch_latest_box_scores()

            # Do NOT wipe last good payload on empty snapshot
            if not snapshot["games"]:
                await asyncio.sleep(REFRESH_INTERVAL_SEC)
                continue

            STATE.payload = {
                "games": snapshot["games"],
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

            print("❌ Player box refresher error")
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
async def stream_player_boxes():
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
# Polling Endpoint (JSON)
# ======================================================

@router.get("")
def get_player_boxes():
    return STATE.payload


# ======================================================
# Debug Endpoint
# ======================================================

@router.get("/debug")
def debug_player_boxes():
    return {
        "last_good_seconds_ago": (
            int(_now() - STATE.last_good_ts)
            if STATE.last_good_ts
            else None
        ),
        "last_attempt_seconds_ago": int(
            _now() - STATE.last_attempt_ts
        ),
        "consecutive_failures": STATE.consecutive_failures,
        "game_count": len(STATE.payload.get("games", [])),
    }


# ======================================================
# Snapshot Endpoint (WRITE SIDE TRIGGER)
# ======================================================

@router.get("/snapshot")
async def snapshot_player_box(
    game_date: Optional[date] = None,
    dry_run: bool = False,
):
    if not game_date:
        game_date = nba_today()

    snapshot = await fetch_latest_box_scores()

    if not dry_run:
        client = bigquery.Client()
        table_id = "graphite-flare-477419-h7.nba_live.player_box_raw"

        row = {
            "snapshot_ts": datetime.now(timezone.utc).isoformat(),
            "game_date": game_date.isoformat(),
            "payload": json.dumps(snapshot),
        }

        errors = client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        # 🔴 ADDITION: flattened player stats write
        flatten_and_write_player_stats(snapshot, game_date)

    return {
        "status": "OK",
        "game_date": str(game_date),
        "games": len(snapshot.get("games", [])),
        "dry_run": dry_run,
    }
