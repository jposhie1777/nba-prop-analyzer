"""Warm the Neon cache for MLB endpoints.

Called at the end of .github/workflows/propfinder.yml after the BigQuery
pipeline has finished writing today's picks. Fetches the live endpoint
with ?_refresh=1 to bypass Cloud Run's in-process cache, then upserts the
JSON payload into Neon's mlb_api_cache table.

Env vars:
  NEON_DATABASE_URL  - Postgres connection string (required)
  CLOUD_RUN_BASE_URL - Cloud Run service base (optional, has default)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
import requests

CLOUD_RUN_BASE_URL = os.environ.get(
    "CLOUD_RUN_BASE_URL",
    "https://mobile-api-763243624328.us-central1.run.app",
)

ENDPOINTS: list[tuple[str, str]] = [
    # (endpoint_path, params_hash)
    ("/mlb/matchups/cheat-sheet", "none"),
]

UPSERT_SQL = """
INSERT INTO mlb_api_cache (endpoint, cache_date, params_hash, payload, refreshed_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (endpoint, cache_date, params_hash)
DO UPDATE SET payload = EXCLUDED.payload, refreshed_at = NOW();
"""


def today_et() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


def fetch(endpoint: str) -> dict:
    url = f"{CLOUD_RUN_BASE_URL}{endpoint}?_refresh=1"
    print(f"GET  {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def upsert(conn, endpoint: str, params_hash: str, payload: dict) -> None:
    cache_date = today_et()
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_SQL,
            (endpoint, cache_date, params_hash, json.dumps(payload)),
        )
    conn.commit()
    print(f"  -> upserted {endpoint} (cache_date={cache_date}, params_hash={params_hash})")


def main() -> int:
    dsn = os.environ.get("NEON_DATABASE_URL")
    if not dsn:
        print("ERROR: NEON_DATABASE_URL not set", file=sys.stderr)
        return 1

    conn = psycopg2.connect(dsn)
    try:
        failures = 0
        for endpoint, params_hash in ENDPOINTS:
            try:
                payload = fetch(endpoint)
                upsert(conn, endpoint, params_hash, payload)
            except Exception as exc:
                failures += 1
                print(f"  !! failed {endpoint}: {exc}", file=sys.stderr)
        if failures:
            print(f"{failures}/{len(ENDPOINTS)} endpoints failed", file=sys.stderr)
            return 1
        print(f"warmed {len(ENDPOINTS)} endpoint(s)")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
