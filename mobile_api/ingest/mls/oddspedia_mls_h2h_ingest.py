# DEPRECATED - replaced by epl_betting_analytics / mls_betting_analytics
# mobile_api/ingest/mls/oddspedia_mls_h2h_ingest.py
"""
Ingests head-to-head data from Oddspedia match insights pages into BigQuery.

Tables:
  oddspedia.mls_h2h          — summary (wins/draws/losses/played)
  oddspedia.mls_h2h_matches  — individual past match results

Usage:
    python -m mobile_api.ingest.mls.oddspedia_mls_h2h_ingest
    python -m mobile_api.ingest.mls.oddspedia_mls_h2h_ingest --dry-run
    python -m mobile_api.ingest.mls.oddspedia_mls_h2h_ingest --scrape-only
    python -m mobile_api.ingest.mls.oddspedia_mls_h2h_ingest --load-only
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

DATASET = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")
H2H_SUMMARY_TABLE = "mls_h2h"
H2H_MATCHES_TABLE = "mls_h2h_matches"

# ── Schemas ───────────────────────────────────────────────────────────────────

H2H_SUMMARY_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("ht_wins", "INT64"),
    bigquery.SchemaField("at_wins", "INT64"),
    bigquery.SchemaField("draws", "INT64"),
    bigquery.SchemaField("played_matches", "INT64"),
    bigquery.SchemaField("period_years", "INT64"),
]

H2H_MATCHES_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("h2h_match_id", "INT64"),
    bigquery.SchemaField("h2h_starttime", "TIMESTAMP"),
    bigquery.SchemaField("h2h_ht", "STRING"),
    bigquery.SchemaField("h2h_ht_id", "INT64"),
    bigquery.SchemaField("h2h_at", "STRING"),
    bigquery.SchemaField("h2h_at_id", "INT64"),
    bigquery.SchemaField("h2h_hscore", "INT64"),
    bigquery.SchemaField("h2h_ascore", "INT64"),
    bigquery.SchemaField("h2h_winner", "INT64"),
    bigquery.SchemaField("h2h_league_name", "STRING"),
    bigquery.SchemaField("h2h_league_slug", "STRING"),
    bigquery.SchemaField("h2h_is_archived", "BOOL"),
    bigquery.SchemaField("h2h_periods_json", "JSON"),
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
        print(f"[h2h] Created dataset {DATASET}")
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
        print(f"[h2h] Created table {DATASET}.{table}")
    except Conflict:
        pass


def _truncate_and_insert(
    client: bigquery.Client,
    table: str,
    rows: List[Dict[str, Any]],
    schema: List[bigquery.SchemaField],
) -> int:
    if not rows:
        return 0

    import tempfile

    table_id = _full_table_id(client, table)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")
        tmp_path = f.name

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    with open(tmp_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)

    job.result()
    os.unlink(tmp_path)
    return len(rows)


# ── Row builders ──────────────────────────────────────────────────────────────


def _build_summary_row(
    match_id: int,
    home_team: Optional[str],
    away_team: Optional[str],
    date_utc: Optional[str],
    h2h: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> Dict[str, Any]:
    return {
        "ingested_at": ingested_at,
        "scraped_date": scraped_date,
        "match_id": match_id,
        "home_team": home_team,
        "away_team": away_team,
        "date_utc": date_utc,
        "ht_wins": h2h.get("ht_wins"),
        "at_wins": h2h.get("at_wins"),
        "draws": h2h.get("draws"),
        "played_matches": h2h.get("played_matches"),
        "period_years": h2h.get("period"),
    }


def _build_match_rows(
    match_id: int,
    home_team: Optional[str],
    away_team: Optional[str],
    date_utc: Optional[str],
    h2h: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows = []
    for m in (h2h.get("matches") or []):
        if not isinstance(m, dict):
            continue
        starttime = (m.get("starttime") or "").split("+")[0].strip() or None
        rows.append({
            "ingested_at": ingested_at,
            "scraped_date": scraped_date,
            "match_id": match_id,
            "home_team": home_team,
            "away_team": away_team,
            "date_utc": date_utc,
            "h2h_match_id": m.get("id"),
            "h2h_starttime": starttime,
            "h2h_ht": m.get("ht"),
            "h2h_ht_id": m.get("ht_id"),
            "h2h_at": m.get("at"),
            "h2h_at_id": m.get("at_id"),
            "h2h_hscore": m.get("hscore"),
            "h2h_ascore": m.get("ascore"),
            "h2h_winner": m.get("winner"),
            "h2h_league_name": m.get("league_name"),
            "h2h_league_slug": m.get("league_slug"),
            "h2h_is_archived": m.get("is_match_archived", False),
            "h2h_periods_json": json.dumps(m.get("periods") or []),
        })
    return rows


# ── Fetch today's matches from BQ ─────────────────────────────────────────────


def _get_todays_match_ids(
    client: bigquery.Client,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    table_id = _full_table_id(client, "mls_match_weather")
    query = f"""
        SELECT DISTINCT
            match_id,
            home_team,
            away_team,
            CAST(date_utc AS STRING) AS date_utc
        FROM `{table_id}`
        WHERE scraped_date = '{scraped_date}'
    """
    try:
        rows = list(client.query(query).result())
        return [dict(row) for row in rows]
    except Exception as exc:
        print(f"[h2h] Failed to query mls_match_weather: {exc}")
        return []


# ── Main ingest ───────────────────────────────────────────────────────────────


def ingest_h2h(
    *, dry_run: bool = False, scrape_only: bool = False, load_only: bool = False
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    print("=" * 60)
    print("[h2h] Starting Oddspedia MLS H2H ingest")
    print(f"[h2h] Ingested at : {ingested_at}")
    if dry_run:
        print("[h2h] Mode        : DRY RUN")
    elif scrape_only:
        print("[h2h] Mode        : SCRAPE ONLY (save to file)")
    elif load_only:
        print("[h2h] Mode        : LOAD ONLY (read from file)")
    print("=" * 60)

    # ── Load-only path ────────────────────────────────────────────────────────
    if load_only:
        summary_rows: List[Dict[str, Any]] = []
        match_rows: List[Dict[str, Any]] = []
        for path, target in [
            ("/tmp/mls_scrape_h2h_summary.ndjson", summary_rows),
            ("/tmp/mls_scrape_h2h_matches.ndjson", match_rows),
        ]:
            try:
                with open(path) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            target.append(json.loads(line))
                print(f"[h2h] Loaded {len(target)} rows from {path}")
            except FileNotFoundError:
                print(f"[h2h] No file at {path} — skipping")

        bq = _bq_client()
        _ensure_dataset(bq)
        _ensure_table(bq, H2H_SUMMARY_TABLE, H2H_SUMMARY_SCHEMA)
        _ensure_table(bq, H2H_MATCHES_TABLE, H2H_MATCHES_SCHEMA)
        s_written = _truncate_and_insert(bq, H2H_SUMMARY_TABLE, summary_rows, H2H_SUMMARY_SCHEMA)
        m_written = _truncate_and_insert(bq, H2H_MATCHES_TABLE, match_rows, H2H_MATCHES_SCHEMA)
        print(f"[h2h] Written — summary: {s_written}, matches: {m_written}")
        print("=" * 60)
        return {"summary_rows_written": s_written, "match_rows_written": m_written, "errors": []}

    # ── Scrape path ───────────────────────────────────────────────────────────
    today_matches: List[Dict[str, Any]] = []
    try:
        with open("/tmp/mls_scrape_match_weather.ndjson") as f:
            for line in f:
                line = line.strip()
                if line:
                    today_matches.append(json.loads(line))
        print(f"[h2h] Loaded {len(today_matches)} matches from weather scrape file")
    except FileNotFoundError:
        # Fallback to BQ if file not present (e.g. running standalone)
        bq = _bq_client()
        today_matches = _get_todays_match_ids(bq, scraped_date)

    if not today_matches:
        print("[h2h] No matches found — exiting")
        return {"summary_rows_written": 0, "match_rows_written": 0}

    print(f"[h2h] Found {len(today_matches)} matches for {scraped_date}")
    match_ids = [int(m["match_id"]) for m in today_matches]
    match_meta = {int(m["match_id"]): m for m in today_matches}

    client = OddspediaClient()
    h2h_results = client.fetch_h2h(match_ids)
    print(f"[h2h] Got H2H data for {len(h2h_results)}/{len(match_ids)} matches")

    summary_rows_out: List[Dict[str, Any]] = []
    match_rows_out: List[Dict[str, Any]] = []

    for match_id, h2h in h2h_results.items():
        meta = match_meta.get(match_id, {})
        home_team = meta.get("home_team")
        away_team = meta.get("away_team")
        date_utc = (meta.get("date_utc") or "").split("+")[0].strip() or None
        summary_rows_out.append(
            _build_summary_row(match_id, home_team, away_team, date_utc, h2h, ingested_at, scraped_date)
        )
        match_rows_out.extend(
            _build_match_rows(match_id, home_team, away_team, date_utc, h2h, ingested_at, scraped_date)
        )

    print(f"[h2h] Summary rows : {len(summary_rows_out)}")
    print(f"[h2h] Match rows   : {len(match_rows_out)}")

    if dry_run:
        print("\n--- SUMMARY SAMPLE ---")
        print(json.dumps(summary_rows_out[:2], indent=2, default=str))
        print("\n--- MATCH ROWS SAMPLE ---")
        print(json.dumps(match_rows_out[:3], indent=2, default=str))
        return {"summary_rows": len(summary_rows_out), "match_rows": len(match_rows_out), "dry_run": True}

    if scrape_only:
        for path, row_list in [
            ("/tmp/mls_scrape_h2h_summary.ndjson", summary_rows_out),
            ("/tmp/mls_scrape_h2h_matches.ndjson", match_rows_out),
        ]:
            with open(path, "w") as f:
                for row in row_list:
                    f.write(json.dumps(row, default=str) + "\n")
            print(f"[h2h] Saved {len(row_list)} rows to {path}")
        print("=" * 60)
        return {"summary_rows": len(summary_rows_out), "match_rows": len(match_rows_out), "errors": []}

    # ── Full run: write directly to BQ ────────────────────────────────────────
    bq = _bq_client()
    _ensure_dataset(bq)
    _ensure_table(bq, H2H_SUMMARY_TABLE, H2H_SUMMARY_SCHEMA)
    _ensure_table(bq, H2H_MATCHES_TABLE, H2H_MATCHES_SCHEMA)
    s_written = _truncate_and_insert(bq, H2H_SUMMARY_TABLE, summary_rows_out, H2H_SUMMARY_SCHEMA)
    m_written = _truncate_and_insert(bq, H2H_MATCHES_TABLE, match_rows_out, H2H_MATCHES_SCHEMA)
    print(f"[h2h] Written — summary: {s_written}, matches: {m_written}")
    print("=" * 60)
    return {"summary_rows_written": s_written, "match_rows_written": m_written, "errors": []}


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--scrape-only", action="store_true")
    parser.add_argument("--load-only", action="store_true")
    args = parser.parse_args()

    result = ingest_h2h(
        dry_run=args.dry_run,
        scrape_only=args.scrape_only,
        load_only=args.load_only,
    )
    print(json.dumps(result, indent=2, default=str))