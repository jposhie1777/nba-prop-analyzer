# mobile_api/ingest/mls/oddspedia_mls_match_info_ingest.py
"""
Ingests match info and match keys from Oddspedia getMatchInfo API into BigQuery.

Tables:
  oddspedia.mls_match_weather  — matchup + weather + team form
  oddspedia.mls_match_keys     — matchup + ranked statistical statements
  oddspedia.mls_betting_stats  — matchup + goals/btts/corners/cards betting stats

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
BETTING_STATS_TABLE = "mls_betting_stats"

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

BETTING_STATS_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("sub_tab", "STRING"),
    bigquery.SchemaField("label", "STRING"),
    bigquery.SchemaField("value", "STRING"),
    bigquery.SchemaField("home", "FLOAT64"),
    bigquery.SchemaField("away", "FLOAT64"),
    bigquery.SchemaField("total_matches_home", "INT64"),
    bigquery.SchemaField("total_matches_away", "INT64"),
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


def _build_betting_stats_rows(
    match_id: int,
    match_info: Dict[str, Any],
    betting_stats: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows = []
    home_team = match_info.get("ht")
    away_team = match_info.get("at")
    date_utc = (match_info.get("starttime") or "").split("+")[0].strip() or None

    for category in (betting_stats.get("data") or []):
        category_label = category.get("label")

        for sub_tab in (category.get("data") or []):
            sub_tab_label = sub_tab.get("label")
            total_matches = sub_tab.get("total_matches") or {}

            for stat in (sub_tab.get("data") or []):
                if not isinstance(stat, dict):
                    continue
                # Skip scoring_minutes — it's a nested structure not a simple home/away
                if stat.get("label") == "scoring_minutes":
                    continue
                rows.append({
                    "ingested_at": ingested_at,
                    "scraped_date": scraped_date,
                    "match_id": match_id,
                    "home_team": home_team,
                    "away_team": away_team,
                    "date_utc": date_utc,
                    "category": category_label,
                    "sub_tab": sub_tab_label,
                    "label": stat.get("label"),
                    "value": str(stat["value"]) if stat.get("value") is not None else None,
                    "home": float(stat["home"]) if stat.get("home") is not None else None,
                    "away": float(stat["away"]) if stat.get("away") is not None else None,
                    "total_matches_home": total_matches.get("home"),
                    "total_matches_away": total_matches.get("away"),
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

    scraper = OddspediaClient()
    matches = scraper.scrape(target_url)
    today_matches = [
        m for m in matches
        if (m.get("date_utc") or "").startswith(scraped_date)
    ]
    print(f"[match_info] {len(today_matches)} matches today")

    weather_rows: List[Dict[str, Any]] = []
    key_rows: List[Dict[str, Any]] = []
    betting_stats_rows: List[Dict[str, Any]] = []

    for match in today_matches:
        mid = match.get("match_id")
        if not mid:
            continue
        data = match.get("match_info") or {}
        if not data:
            print(f"[match_info] match={mid} no match_info on record — skipping")
            continue
        print(f"[match_info] match={mid} processing: {data.get('ht')} vs {data.get('at')}")
        weather_rows.append(_build_weather_row(mid, data, ingested_at, scraped_date))
        key_rows.extend(_build_key_rows(mid, data, ingested_at, scraped_date))

        betting_stats = match.get("betting_stats") or {}
        if betting_stats:
            rows = _build_betting_stats_rows(mid, data, betting_stats, ingested_at, scraped_date)
            betting_stats_rows.extend(rows)
            print(f"[match_info] match={mid} betting stats rows: {len(rows)}")
        else:
            print(f"[match_info] match={mid} no betting stats on record")

    print(f"[match_info] Weather rows      : {len(weather_rows)}")
    print(f"[match_info] Key rows          : {len(key_rows)}")
    print(f"[match_info] Betting stat rows : {len(betting_stats_rows)}")

    if dry_run:
        print("\n--- WEATHER SAMPLE ---")
        print(json.dumps(weather_rows[:2], indent=2, default=str))
        print("\n--- KEYS SAMPLE ---")
        print(json.dumps(key_rows[:5], indent=2, default=str))
        print("\n--- BETTING STATS SAMPLE ---")
        print(json.dumps(betting_stats_rows[:5], indent=2, default=str))
        return {
            "weather_rows": len(weather_rows),
            "key_rows": len(key_rows),
            "betting_stats_rows": len(betting_stats_rows),
            "dry_run": True,
        }

    bq = _bq_client()
    _ensure_dataset(bq)
    _ensure_table(bq, WEATHER_TABLE, WEATHER_SCHEMA)
    _ensure_table(bq, KEYS_TABLE, KEYS_SCHEMA)
    _ensure_table(bq, BETTING_STATS_TABLE, BETTING_STATS_SCHEMA)

    w_written = _truncate_and_insert(bq, WEATHER_TABLE, weather_rows, WEATHER_SCHEMA)
    k_written = _truncate_and_insert(bq, KEYS_TABLE, key_rows, KEYS_SCHEMA)
    b_written = _truncate_and_insert(bq, BETTING_STATS_TABLE, betting_stats_rows, BETTING_STATS_SCHEMA)

    print(f"[match_info] Written — weather: {w_written}, keys: {k_written}, betting stats: {b_written}")
    print("=" * 60)

    return {
        "weather_rows_written": w_written,
        "key_rows_written": k_written,
        "betting_stats_rows_written": b_written,
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