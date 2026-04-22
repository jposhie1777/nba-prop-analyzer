"""Warm the Neon cache for NBA endpoints.

Called at the end of .github/workflows/nba_propfinder_daily.yml after the
BigQuery ingest writes today's props/games. Fetches the live Cloud Run
endpoint with ?_refresh=1 (bypassing in-process cache) and upserts the JSON
payload into Neon's nba_api_cache table.

Warms:
  - /nba/research  (full unfiltered payload — UI filters client-side)

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

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS nba_api_cache (
    endpoint     TEXT        NOT NULL,
    cache_date   DATE        NOT NULL,
    params_hash  TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (endpoint, cache_date, params_hash)
);
"""

UPSERT_SQL = """
INSERT INTO nba_api_cache (endpoint, cache_date, params_hash, payload, refreshed_at)
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
    r = requests.get(url, timeout=120)
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


def try_warm(conn, endpoint: str, params_hash: str, extra_qs: str = "") -> bool:
    try:
        payload = fetch(endpoint, extra_qs)
        upsert(conn, endpoint, params_hash, payload)
        return True
    except Exception as exc:
        print(f"  !! failed {endpoint}: {exc}", file=sys.stderr)
        return False


def main() -> int:
    dsn = os.environ.get("NEON_DATABASE_URL")
    if not dsn:
        print("ERROR: NEON_DATABASE_URL not set", file=sys.stderr)
        return 1

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()

        warmed = 0
        failures = 0

        for endpoint, params_hash, extra_qs in [
            ("/nba/research", "none", ""),
        ]:
            if try_warm(conn, endpoint, params_hash, extra_qs):
                warmed += 1
            else:
                failures += 1

        total = warmed + failures
        if failures:
            print(f"{failures}/{total} NBA endpoints failed", file=sys.stderr)
            return 1
        print(f"warmed {warmed} NBA endpoint(s)")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
