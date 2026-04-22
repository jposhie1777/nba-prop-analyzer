"""Warm the Neon cache for MLB endpoints.

Called at the end of .github/workflows/propfinder.yml after the BigQuery
pipeline has finished writing today's picks. Fetches the live endpoint
with ?_refresh=1 to bypass Cloud Run's in-process cache, then upserts the
JSON payload into Neon's mlb_api_cache table.

Warms:
  - /mlb/matchups/cheat-sheet
  - /mlb/matchups/upcoming (limit=30) — also used to enumerate game_pks
  - /mlb/matchups/{game_pk} for every upcoming game (season=2026)
  - /mlb/matchups/{game_pk}/pitching-props for every upcoming game
  - /mlb/matchups/nrfi?state=nj

Env vars:
  NEON_DATABASE_URL  - Postgres connection string (required)
  CLOUD_RUN_BASE_URL - Cloud Run service base (optional, has default)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
import requests

CLOUD_RUN_BASE_URL = os.environ.get(
    "CLOUD_RUN_BASE_URL",
    "https://mobile-api-763243624328.us-central1.run.app",
)

UPSERT_SQL = """
INSERT INTO mlb_api_cache (endpoint, cache_date, params_hash, payload, refreshed_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (endpoint, cache_date, params_hash)
DO UPDATE SET payload = EXCLUDED.payload, refreshed_at = NOW();
"""


def today_et() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


def fetch(endpoint: str, extra_qs: str = "") -> Any:
    qs = f"{extra_qs}&_refresh=1" if extra_qs else "_refresh=1"
    url = f"{CLOUD_RUN_BASE_URL}{endpoint}?{qs}"
    print(f"GET  {url}")
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    return r.json()


def upsert(conn, endpoint: str, params_hash: str, payload: Any) -> None:
    cache_date = today_et()
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_SQL,
            (endpoint, cache_date, params_hash, json.dumps(payload)),
        )
    conn.commit()
    print(f"  -> upserted {endpoint} (cache_date={cache_date}, params_hash={params_hash})")


def try_warm(
    conn,
    endpoint: str,
    params_hash: str,
    extra_qs: str = "",
) -> tuple[bool, Any]:
    try:
        payload = fetch(endpoint, extra_qs)
        upsert(conn, endpoint, params_hash, payload)
        return True, payload
    except Exception as exc:
        print(f"  !! failed {endpoint}: {exc}", file=sys.stderr)
        return False, None


def main() -> int:
    dsn = os.environ.get("NEON_DATABASE_URL")
    if not dsn:
        print("ERROR: NEON_DATABASE_URL not set", file=sys.stderr)
        return 1

    conn = psycopg2.connect(dsn)
    try:
        warmed = 0
        failures = 0

        def tally(ok: bool) -> None:
            nonlocal warmed, failures
            if ok:
                warmed += 1
            else:
                failures += 1

        # 1. Cheat sheet
        ok, _ = try_warm(conn, "/mlb/matchups/cheat-sheet", "none")
        tally(ok)

        # 2. Upcoming — also enumerates today's game_pks for per-game warming
        ok, upcoming = try_warm(
            conn, "/mlb/matchups/upcoming", "none", extra_qs="limit=30"
        )
        tally(ok)

        # 3. Per-game matchup detail + pitching props
        game_pks: list[int] = []
        if isinstance(upcoming, list):
            for game in upcoming:
                if not isinstance(game, dict):
                    continue
                pk = game.get("game_pk")
                if isinstance(pk, int):
                    game_pks.append(pk)

        for pk in game_pks:
            ok, _ = try_warm(
                conn,
                f"/mlb/matchups/{pk}",
                "season:2026",
                extra_qs="season=2026",
            )
            tally(ok)
            ok, _ = try_warm(
                conn,
                f"/mlb/matchups/{pk}/pitching-props",
                "none",
            )
            tally(ok)

        # 4. NRFI
        ok, _ = try_warm(
            conn, "/mlb/matchups/nrfi", "state:nj", extra_qs="state=nj"
        )
        tally(ok)

        total = warmed + failures
        if failures:
            print(f"{failures}/{total} endpoints failed", file=sys.stderr)
            return 1
        print(f"warmed {warmed} endpoint(s)")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
