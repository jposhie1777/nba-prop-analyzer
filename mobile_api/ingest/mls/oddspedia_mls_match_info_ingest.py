# mobile_api/ingest/mls/oddspedia_mls_match_info_ingest.py
"""
Ingests match info and match keys from Oddspedia getMatchInfo API into BigQuery.

Tables:
  oddspedia.mls_match_weather  — matchup + weather + team form
  oddspedia.mls_match_keys     — matchup + ranked statistical statements

Usage:
    python -m mobile_api.ingest.mls.oddspedia_mls_match_info_ingest
    python -m mobile_api.ingest.mls.oddspedia_mls_match_info_ingest --dry-run
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from oddspedia_client import OddspediaClient  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_URL = "https://www.oddspedia.com/us/soccer/usa/mls"
ODDSPEDIA_URL = os.getenv("ODDSPEDIA_MLS_URL", DEFAULT_URL)
DATASET = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")
WEATHER_TABLE = "mls_match_weather"
KEYS_TABLE = "mls_match_keys"

# ── Schemas ───────────────────────────────────────────────────────────────────

WEATHER_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("weather_icon", "STRING"),
    bigquery.SchemaField("weather_temp_c", "FLOAT64"),
    bigquery.SchemaField("home_form", "STRING"),
    bigquery.SchemaField("away_form", "STRING"),
]

KEYS_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("rank", "INT64"),
    bigquery.SchemaField("statement", "STRING"),
    bigquery.SchemaField("teams_json", "JSON"),
]

# ── BigQuery helpers ──────────────────────────────────────────────────────────


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _full_table_id(client: bigquery.Client, table: str) -> str:
    return f"{client.project}.{DATASET}.{table}"


def _ensure_dataset(client: bigquery.Client) -> None:
    dataset_ref = f"{client.project}.{DATASET}"
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = DATASET_LOCATION
        client.create_dataset(ds)
        print(f"[match_info] Created dataset {DATASET}")
    except Conflict:
        pass


def _ensure_table(
    client: bigquery.Client,
    table: str,
    schema: List[bigquery.SchemaField],
) -> None:
    table_id = _full_table_id(client, table)
    try:
        client.get_table(table_id)
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=schema))
        print(f"[match_info] Created table {DATASET}.{table}")
    except Conflict:
        pass


def _truncate_and_insert(
    client: bigquery.Client,
    table: str,
    rows: List[Dict[str, Any]],
    schema: List[bigquery.SchemaField],
    chunk_size: int = 500,
) -> int:
    if not rows:
        return 0
    table_id = _full_table_id(client, table)
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    written = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i: i + chunk_size]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
        written += len(chunk)
        time.sleep(0.05)
    return written


# ── Row builders ──────────────────────────────────────────────────────────────


def _build_weather_row(
    match_id: int,
    data: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> Dict[str, Any]:
    weather = data.get("weather_conditions") or {}
    return {
        "ingested_at": ingested_at,
        "scraped_date": scraped_date,
        "match_id": match_id,
        "home_team": data.get("ht"),
        "away_team": data.get("at"),
        "date_utc": (data.get("starttime") or "").split("+")[0].strip() or None,
        "weather_icon": weather.get("icon"),
        "weather_temp_c": weather.get("temperature"),
        "home_form": data.get("ht_form"),
        "away_form": data.get("at_form"),
    }


def _build_key_rows(
    match_id: int,
    data: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows = []
    home_team = data.get("ht")
    away_team = data.get("at")
    date_utc = (data.get("starttime") or "").split("+")[0].strip() or None

    for rank, key in enumerate(data.get("match_keys") or [], start=1):
        if not isinstance(key, dict):
            continue
        rows.append({
            "ingested_at": ingested_at,
            "scraped_date": scraped_date,
            "match_id": match_id,
            "home_team": home_team,
            "away_team": away_team,
            "date_utc": date_utc,
            "rank": rank,
            "statement": key.get("statement"),
            "teams_json": json.dumps(key.get("teams") or []),
        })
    return rows


# ── Main ingest ───────────────────────────────────────────────────────────────


def ingest_match_info(
    url: Optional[str] = None,
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    target_url = url or ODDSPEDIA_URL
    now = datetime.now(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    print("=" * 60)
    print("[match_info] Starting Oddspedia MLS match info ingest")
    print(f"[match_info] URL         : {target_url}")
    print(f"[match_info] Ingested at : {ingested_at}")
    if dry_run:
        print("[match_info] Mode        : DRY RUN")
    print("=" * 60)

    # Scrape match list to get today's match IDs + team names
    scraper = OddspediaClient()
    matches = scraper.scrape(target_url)
    today_matches = [
        m for m in matches
        if (m.get("date_utc") or "").startswith(scraped_date)
    ]
    print(f"[match_info] {len(today_matches)} matches today")

    weather_rows: List[Dict[str, Any]] = []
    key_rows: List[Dict[str, Any]] = []

    for match in today_matches:
        mid = match.get("match_id")
        if not mid:
            continue

        # Re-use the page session from the scraper isn't available here,
        # so we make a direct HTTP fetch using requests (session cookies not
        # needed — getMatchInfo doesn't require Cloudflare auth)
        import urllib.request
        info_url = (
            f"https://oddspedia.com/api/v1/getMatchInfo"
            f"?matchId={mid}&language=us&geoCode=US"
        )
        try:
            req = urllib.request.Request(
                info_url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    ),
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
            data = body.get("data") or {}
            print(f"[match_info] match={mid} fetched: {data.get('ht')} vs {data.get('at')}")
            weather_rows.append(_build_weather_row(mid, data, ingested_at, scraped_date))
            key_rows.extend(_build_key_rows(mid, data, ingested_at, scraped_date))
        except Exception as exc:
            print(f"[match_info] match={mid} fetch error: {exc}")

    print(f"[match_info] Weather rows : {len(weather_rows)}")
    print(f"[match_info] Key rows     : {len(key_rows)}")

    if dry_run:
        print("\n--- WEATHER SAMPLE ---")
        print(json.dumps(weather_rows[:2], indent=2, default=str))
        print("\n--- KEYS SAMPLE ---")
        print(json.dumps(key_rows[:5], indent=2, default=str))
        return {
            "weather_rows": len(weather_rows),
            "key_rows": len(key_rows),
            "dry_run": True,
        }

    bq = _bq_client()
    _ensure_dataset(bq)
    _ensure_table(bq, WEATHER_TABLE, WEATHER_SCHEMA)
    _ensure_table(bq, KEYS_TABLE, KEYS_SCHEMA)

    w_written = _truncate_and_insert(bq, WEATHER_TABLE, weather_rows, WEATHER_SCHEMA)
    k_written = _truncate_and_insert(bq, KEYS_TABLE, key_rows, KEYS_SCHEMA)

    print(f"[match_info] Written — weather: {w_written}, keys: {k_written}")
    print("=" * 60)

    return {
        "weather_rows_written": w_written,
        "key_rows_written": k_written,
        "errors": [],
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = ingest_match_info(url=args.url, dry_run=args.dry_run)
    print(json.dumps(result, indent=2, default=str))