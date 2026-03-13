"""Oddspedia ATP odds → BigQuery ingest.

Scrapes the Oddspedia tennis odds page, extracts the embedded Nuxt SSR state,
and loads all matches (moneyline + spread) into BigQuery as
``oddspedia.atp_odds``.

The table is TRUNCATED before each load so only the latest snapshot is kept.
Dataset and table are created automatically on first run.

Environment variables
---------------------
ODDSPEDIA_URL
    Full URL to scrape.  Default: https://www.oddspedia.com/us/tennis/odds
ODDSPEDIA_DATASET
    BigQuery dataset name.  Default: oddspedia
ODDSPEDIA_DATASET_LOCATION
    BigQuery dataset region.  Default: US
ODDSPEDIA_TABLE
    BigQuery table name.  Default: atp_odds
GCP_PROJECT / GOOGLE_CLOUD_PROJECT
    GCP project (used by the BigQuery client).

Usage
-----
    # Run directly:
    python -m mobile_api.ingest.atp.oddspedia_odds_ingest

    # With a URL override:
    ODDSPEDIA_URL=https://www.oddspedia.com/us/tennis/odds \
        python -m mobile_api.ingest.atp.oddspedia_odds_ingest
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

# ── Path setup ────────────────────────────────────────────────────────────────
# oddspedia_client.py lives at the repo root; add it to sys.path if needed.
_repo_root = Path(__file__).resolve().parents[3]  # mobile_api/ingest/atp/ → root
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from oddspedia_client import OddspediaClient  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_URL = "https://www.oddspedia.com/us/tennis/odds"

ODDSPEDIA_URL      = os.getenv("ODDSPEDIA_URL", DEFAULT_URL)
DATASET            = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION   = os.getenv("ODDSPEDIA_DATASET_LOCATION", "US")
TABLE              = os.getenv("ODDSPEDIA_TABLE", "atp_odds")

# ── BigQuery schema ───────────────────────────────────────────────────────────

SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at",        "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date",        "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("match_id",            "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("sport",               "STRING"),
    bigquery.SchemaField("date_utc",            "TIMESTAMP"),
    bigquery.SchemaField("home_team",           "STRING"),
    bigquery.SchemaField("away_team",           "STRING"),
    bigquery.SchemaField("home_team_id",        "INT64"),
    bigquery.SchemaField("away_team_id",        "INT64"),
    bigquery.SchemaField("inplay",              "BOOL"),
    bigquery.SchemaField("league_id",           "INT64"),
    bigquery.SchemaField("market",              "STRING"),
    bigquery.SchemaField("bookie",              "STRING"),
    bigquery.SchemaField("bookie_slug",         "STRING"),
    bigquery.SchemaField("home_odds_decimal",   "FLOAT64"),
    bigquery.SchemaField("away_odds_decimal",   "FLOAT64"),
    bigquery.SchemaField("home_odds_american",  "INT64"),
    bigquery.SchemaField("away_odds_american",  "INT64"),
    bigquery.SchemaField("home_handicap",       "STRING"),
    bigquery.SchemaField("away_handicap",       "STRING"),
    bigquery.SchemaField("handicap_label",      "STRING"),
    bigquery.SchemaField("status",              "INT64"),
    bigquery.SchemaField("winning_side",        "STRING"),
    bigquery.SchemaField("bet_link",            "STRING"),
]

# ── BigQuery helpers ──────────────────────────────────────────────────────────

def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _full_table_id(client: bigquery.Client) -> str:
    return f"{client.project}.{DATASET}.{TABLE}"


def _ensure_dataset(client: bigquery.Client) -> None:
    dataset_ref = f"{client.project}.{DATASET}"
    try:
        client.get_dataset(dataset_ref)
        print(f"[atp_odds] Dataset {DATASET} already exists")
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = DATASET_LOCATION
        client.create_dataset(ds)
        print(f"[atp_odds] Created dataset {DATASET} (location={DATASET_LOCATION})")
    except Conflict:
        pass


def _ensure_table(client: bigquery.Client) -> None:
    tid = _full_table_id(client)
    try:
        client.get_table(tid)
        print(f"[atp_odds] Table {DATASET}.{TABLE} already exists")
    except NotFound:
        client.create_table(bigquery.Table(tid, schema=SCHEMA))
        print(f"[atp_odds] Created table {DATASET}.{TABLE}")
    except Conflict:
        pass


def _add_missing_columns(client: bigquery.Client) -> None:
    """Patch the live table with any columns present in SCHEMA but not yet in BQ."""
    tid = _full_table_id(client)
    table = client.get_table(tid)
    existing = {f.name for f in table.schema}
    new_fields = [f for f in SCHEMA if f.name not in existing]
    if not new_fields:
        return
    table.schema = list(table.schema) + new_fields
    client.update_table(table, ["schema"])
    print(f"[atp_odds] Added {len(new_fields)} new column(s): {[f.name for f in new_fields]}")


def _truncate_table(client: bigquery.Client) -> None:
    tid = _full_table_id(client)
    client.query(f"TRUNCATE TABLE `{tid}`").result()
    print(f"[atp_odds] Truncated {DATASET}.{TABLE}")


def _insert_rows(
    client: bigquery.Client,
    rows: List[Dict[str, Any]],
    *,
    chunk_size: int = 500,
) -> int:
    if not rows:
        return 0
    tid = _full_table_id(client)
    written = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        errors = client.insert_rows_json(tid, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
        written += len(chunk)
        time.sleep(0.05)
    return written

# ── Row builder ───────────────────────────────────────────────────────────────

def _to_bq_rows(
    matches: List[Dict[str, Any]],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    """Flatten each match × market pair into one BigQuery row."""
    rows: List[Dict[str, Any]] = []
    for match in matches:
        match_id = match.get("match_id")
        base = {
            "ingested_at":   ingested_at,
            "scraped_date":  scraped_date,
            "match_id":      match_id,
            "sport":         match.get("sport"),
            "date_utc":      match.get("date_utc"),
            "home_team":     match.get("home_team"),
            "away_team":     match.get("away_team"),
            "home_team_id":  match.get("home_team_id"),
            "away_team_id":  match.get("away_team_id"),
            "inplay":        match.get("inplay", False),
            "league_id":     match.get("league_id"),
        }
        markets = match.get("markets", {})
        if not markets:
            # Preserve matches that have no odds yet (show nulls for market cols)
            rows.append({**base, "market": None})
            continue
        for market_name, mkt in markets.items():
            rows.append({
                **base,
                "market":             market_name,
                "bookie":             mkt.get("bookie"),
                "bookie_slug":        mkt.get("bookie_slug"),
                "home_odds_decimal":  mkt.get("home_odds_decimal"),
                "away_odds_decimal":  mkt.get("away_odds_decimal"),
                "home_odds_american": mkt.get("home_odds_american"),
                "away_odds_american": mkt.get("away_odds_american"),
                "home_handicap":      mkt.get("home_handicap"),
                "away_handicap":      mkt.get("away_handicap"),
                "handicap_label":     mkt.get("handicap_label"),
                "status":             mkt.get("status"),
                "winning_side":       mkt.get("winning_side"),
                "bet_link":           mkt.get("bet_link"),
            })
    return rows

# ── Main ingest ───────────────────────────────────────────────────────────────

def ingest_atp_odds(
    url: Optional[str] = None,
    *,
    dry_run: bool = False,
    today_only: bool = False,
    has_total: bool = False,
) -> Dict[str, Any]:
    """Scrape Oddspedia and load ATP odds into BigQuery (oddspedia.atp_odds).

    The table is truncated before writing so only the latest snapshot remains.

    Parameters
    ----------
    url:
        Oddspedia page URL.  Defaults to ODDSPEDIA_URL env var or the tennis
        odds page (https://www.oddspedia.com/us/tennis/odds).
    dry_run:
        When True, skip BigQuery entirely and just print scraped matches as JSON.
    today_only:
        When True, only include matches scheduled for today (UTC).
    has_total:
        When True, only include matches that have a ``total`` market.

    Returns
    -------
    Summary dict with match count, row count, and any errors.
    """
    target_url = url or ODDSPEDIA_URL
    now = datetime.now(timezone.utc)
    ingested_at  = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    print("=" * 60)
    print("[atp_odds] Starting Oddspedia ATP odds ingest")
    print(f"[atp_odds] URL          : {target_url}")
    print(f"[atp_odds] Ingested at  : {ingested_at}")
    if dry_run:
        print("[atp_odds] Mode         : DRY RUN (no BigQuery write)")
    else:
        print(f"[atp_odds] Destination  : {DATASET}.{TABLE}")
    print("=" * 60)

    # ── Scrape ────────────────────────────────────────────────────────────────
    client_scraper = OddspediaClient()
    print(f"[atp_odds] Fetching {target_url} …")
    matches = client_scraper.scrape(target_url)
    print(f"[atp_odds] Scraped {len(matches)} matches")

    # ── Optional filters ──────────────────────────────────────────────────────
    if today_only:
        matches = [m for m in matches if (m.get("date_utc") or "").startswith(scraped_date)]
        print(f"[atp_odds] After --today filter : {len(matches)} matches")
    if has_total:
        matches = [m for m in matches if "total" in m.get("markets", {})]
        print(f"[atp_odds] After --has-total filter : {len(matches)} matches")

    # ── Dry-run: print and exit ───────────────────────────────────────────────
    if dry_run:
        print(json.dumps(matches, indent=2))
        return {
            "url":           target_url,
            "ingested_at":   ingested_at,
            "matches_found": len(matches),
            "rows_written":  0,
            "dry_run":       True,
            "errors":        [],
        }

    # ── BigQuery setup ────────────────────────────────────────────────────────
    bq = _bq_client()
    _ensure_dataset(bq)
    _ensure_table(bq)
    _add_missing_columns(bq)

    # ── Load ──────────────────────────────────────────────────────────────────
    rows = _to_bq_rows(matches, ingested_at, scraped_date)
    print(f"[atp_odds] Truncating table …")
    _truncate_table(bq)

    print(f"[atp_odds] Inserting {len(rows)} rows …")
    written = _insert_rows(bq, rows)

    summary: Dict[str, Any] = {
        "url":           target_url,
        "ingested_at":   ingested_at,
        "matches_found": len(matches),
        "rows_written":  written,
        "errors":        [],
    }

    print(f"[atp_odds] Done — {written} rows written ({len(matches)} matches)")
    print("=" * 60)
    return summary


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Oddspedia tennis odds and load into BigQuery (oddspedia.atp_odds)."
    )
    parser.add_argument(
        "--url",
        default=None,
        help=(
            "Oddspedia page URL to scrape. "
            f"Defaults to ODDSPEDIA_URL env var or {DEFAULT_URL}."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print scraped matches as JSON without writing to BigQuery.",
    )
    parser.add_argument(
        "--today",
        action="store_true",
        help="Only include matches scheduled for today (UTC).",
    )
    parser.add_argument(
        "--has-total",
        action="store_true",
        help="Only include matches that have a 'total' market.",
    )
    args = parser.parse_args()

    result = ingest_atp_odds(
        url=args.url,
        dry_run=args.dry_run,
        today_only=args.today,
        has_total=args.has_total,
    )
    print(json.dumps(result, indent=2, default=str))
