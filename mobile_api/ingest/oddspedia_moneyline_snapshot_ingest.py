"""Moneyline snapshot ingest — reads from mls_odds / epl_odds and appends to
oddspedia.moneyline_snapshots.

This script is a post-processor: it runs AFTER the primary odds ingest has
already loaded today's flat rows into mls_odds / epl_odds.  It filters to
1x2 (moneyline) markets, deduplicates to one row per match × outcome × bookie,
maps outcome_side labels (o1→home, o2→draw, o3→away), and appends a clean
snapshot row to the append-only partitioned history table.

Usage
-----
    python -m mobile_api.ingest.oddspedia_moneyline_snapshot_ingest --sport mls
    python -m mobile_api.ingest.oddspedia_moneyline_snapshot_ingest --sport epl
    python -m mobile_api.ingest.oddspedia_moneyline_snapshot_ingest --sport mls --dry-run

Environment variables
---------------------
GCP_PROJECT / GOOGLE_CLOUD_PROJECT
    GCP project id used by the BigQuery client.

ODDSPEDIA_DATASET
    BigQuery dataset name.  Default: oddspedia

ODDSPEDIA_BQ_LOCATION
    BigQuery dataset region.  Default: US
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

# ── Configuration ─────────────────────────────────────────────────────────────

DATASET          = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")
SNAPSHOT_TABLE   = "moneyline_snapshots"

_SOURCE_TABLE: Dict[str, str] = {
    "mls": "mls_odds",
    "epl": "epl_odds",
}

# ── BigQuery schema ───────────────────────────────────────────────────────────

SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at",    "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("sport",          "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("match_id",       "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("home_team",      "STRING"),
    bigquery.SchemaField("away_team",      "STRING"),
    bigquery.SchemaField("match_date_utc", "TIMESTAMP"),
    bigquery.SchemaField("outcome",        "STRING"),
    bigquery.SchemaField("bookie",         "STRING"),
    bigquery.SchemaField("bookie_slug",    "STRING"),
    bigquery.SchemaField("odds_decimal",   "FLOAT64"),
    bigquery.SchemaField("odds_american",  "INT64"),
    bigquery.SchemaField("odds_direction", "INT64"),
    bigquery.SchemaField("scraped_date",   "DATE",      mode="REQUIRED"),
]

# ── Outcome mapping ───────────────────────────────────────────────────────────

_SIDE_MAP: Dict[str, str] = {
    "o1": "home",
    "o2": "draw",
    "o3": "away",
}


def _map_outcome(outcome_side: Optional[str], outcome_name: Optional[str]) -> Optional[str]:
    """Map outcome_side (o1/o2/o3) to home/draw/away; fall back to outcome_name."""
    if outcome_side:
        key = outcome_side.strip().lower()
        if key in _SIDE_MAP:
            return _SIDE_MAP[key]
        if key in ("home", "draw", "away"):
            return key
    if outcome_name:
        name = outcome_name.strip().lower()
        if name in ("home", "draw", "away"):
            return name
    return outcome_side  # fallback: pass through as-is


# ── BigQuery helpers ──────────────────────────────────────────────────────────


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _snapshot_table_id(client: bigquery.Client) -> str:
    return f"{client.project}.{DATASET}.{SNAPSHOT_TABLE}"


def _ensure_dataset(client: bigquery.Client) -> None:
    dataset_ref = f"{client.project}.{DATASET}"
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = DATASET_LOCATION
        client.create_dataset(ds)
        print(f"[moneyline_snapshot] Created dataset {DATASET} (location={DATASET_LOCATION})")
    except Conflict:
        pass


def _ensure_table(client: bigquery.Client) -> None:
    table_id = _snapshot_table_id(client)
    try:
        client.get_table(table_id)
        print(f"[moneyline_snapshot] Table {DATASET}.{SNAPSHOT_TABLE} already exists")
    except NotFound:
        table = bigquery.Table(table_id, schema=SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="scraped_date",
        )
        client.create_table(table)
        print(f"[moneyline_snapshot] Created partitioned table {DATASET}.{SNAPSHOT_TABLE}")
    except Conflict:
        pass


def _already_ingested(client: bigquery.Client, ingested_at: str, sport: str) -> bool:
    """Return True if rows for this exact ingested_at + sport already exist (idempotency guard)."""
    table_id = _snapshot_table_id(client)
    try:
        client.get_table(table_id)
    except NotFound:
        return False

    query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{table_id}`
        WHERE sport = @sport
          AND ingested_at = TIMESTAMP(@ingested_at)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("sport",       "STRING", sport),
            bigquery.ScalarQueryParameter("ingested_at", "STRING", ingested_at),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return bool(rows and rows[0].cnt > 0)


def _query_source_rows(
    client: bigquery.Client,
    sport: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    """Query today's 1x2 moneyline rows from the source odds table."""
    source_table = _SOURCE_TABLE[sport]
    table_id = f"{client.project}.{DATASET}.{source_table}"

    query = f"""
        SELECT
            match_id,
            home_team,
            away_team,
            date_utc         AS match_date_utc,
            outcome_side,
            outcome_name,
            bookie,
            bookie_slug,
            odds_decimal,
            odds_american,
            odds_direction,
            scraped_date
        FROM `{table_id}`
        WHERE market = '1x2'
          AND scraped_date = @scraped_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("scraped_date", "DATE", scraped_date),
        ]
    )
    result = client.query(query, job_config=job_config).result()
    return [dict(row) for row in result]


def _deduplicate(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep one row per (match_id, outcome_side, bookie), preferring highest odds_decimal."""
    best: Dict[tuple, Dict[str, Any]] = {}
    for row in rows:
        key = (row.get("match_id"), row.get("outcome_side"), row.get("bookie"))
        existing = best.get(key)
        odds = row.get("odds_decimal") or 0.0
        if existing is None or odds > (existing.get("odds_decimal") or 0.0):
            best[key] = row
    return list(best.values())


def _to_snapshot_rows(
    source_rows: List[Dict[str, Any]],
    ingested_at: str,
    sport: str,
) -> List[Dict[str, Any]]:
    out = []
    for r in source_rows:
        outcome = _map_outcome(r.get("outcome_side"), r.get("outcome_name"))

        # BQ returns DATE/TIMESTAMP as Python date/datetime objects — normalise to str
        match_date = r.get("match_date_utc")
        if hasattr(match_date, "isoformat"):
            match_date = match_date.isoformat()

        scraped = r.get("scraped_date")
        if hasattr(scraped, "isoformat"):
            scraped = scraped.isoformat()

        out.append({
            "ingested_at":    ingested_at,
            "sport":          sport,
            "match_id":       r["match_id"],
            "home_team":      r.get("home_team"),
            "away_team":      r.get("away_team"),
            "match_date_utc": match_date,
            "outcome":        outcome,
            "bookie":         r.get("bookie"),
            "bookie_slug":    r.get("bookie_slug"),
            "odds_decimal":   float(r["odds_decimal"])   if r.get("odds_decimal")   is not None else None,
            "odds_american":  int(r["odds_american"])    if r.get("odds_american")  is not None else None,
            "odds_direction": int(r["odds_direction"])   if r.get("odds_direction") is not None else None,
            "scraped_date":   scraped,
        })
    return out


def _append_rows(client: bigquery.Client, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    table_id = _snapshot_table_id(client)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")
        tmp_path = f.name

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=SCHEMA,
    )

    with open(tmp_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)

    job.result()
    os.unlink(tmp_path)
    return len(rows)


# ── Main entry point ──────────────────────────────────────────────────────────


def run(sport: str, *, dry_run: bool = False) -> Dict[str, Any]:
    now          = datetime.now(timezone.utc)
    ingested_at  = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    print("=" * 60)
    print(f"[moneyline_snapshot] sport={sport}  scraped_date={scraped_date}")
    if dry_run:
        print("[moneyline_snapshot] Mode: DRY RUN (no BigQuery write)")
    else:
        print(f"[moneyline_snapshot] Destination: {DATASET}.{SNAPSHOT_TABLE}")
    print("=" * 60)

    client = _bq_client()

    # Idempotency guard — skip if this exact run's data is already in the table
    if not dry_run and _already_ingested(client, ingested_at, sport):
        print(
            f"[moneyline_snapshot] Rows for ingested_at={ingested_at} sport={sport} "
            "already exist — skipping."
        )
        return {"sport": sport, "ingested_at": ingested_at, "rows_written": 0, "skipped": True}

    # Query source odds table
    print(f"[moneyline_snapshot] Querying {DATASET}.{_SOURCE_TABLE[sport]} …")
    source_rows = _query_source_rows(client, sport, scraped_date)
    print(f"[moneyline_snapshot] Found {len(source_rows)} raw 1x2 rows")

    # Deduplicate
    deduped = _deduplicate(source_rows)
    print(f"[moneyline_snapshot] After dedup: {len(deduped)} rows")

    # Build snapshot rows
    snapshot_rows = _to_snapshot_rows(deduped, ingested_at, sport)

    if dry_run:
        print(json.dumps(snapshot_rows[:25], indent=2, default=str))
        return {
            "sport":         sport,
            "ingested_at":   ingested_at,
            "source_rows":   len(source_rows),
            "rows_prepared": len(snapshot_rows),
            "rows_written":  0,
            "dry_run":       True,
        }

    # Ensure table exists then append
    _ensure_dataset(client)
    _ensure_table(client)

    print(f"[moneyline_snapshot] Appending {len(snapshot_rows)} rows …")
    written = _append_rows(client, snapshot_rows)
    print(f"[moneyline_snapshot] Done — {written} rows appended to {DATASET}.{SNAPSHOT_TABLE}")
    print("=" * 60)

    return {
        "sport":       sport,
        "ingested_at": ingested_at,
        "source_rows": len(source_rows),
        "rows_written": written,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Append moneyline (1x2) snapshot rows to "
            f"{DATASET}.{SNAPSHOT_TABLE}."
        )
    )
    parser.add_argument(
        "--sport",
        required=True,
        choices=["mls", "epl"],
        help="Sport to snapshot: 'mls' or 'epl'",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print prepared rows without writing to BigQuery.",
    )
    args = parser.parse_args()
    result = run(args.sport, dry_run=args.dry_run)
    print(json.dumps(result, indent=2, default=str))
