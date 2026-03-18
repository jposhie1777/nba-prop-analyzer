#mobile_api/ingest/atp/oddspedia_atp_odds_ingest.py
"""Oddspedia ATP odds → BigQuery ingest (live scrape, single flat table).

Modeled after EPL/MLS oddspedia ingests:
- ATP-specific scrape params
- optional today filter
- optional scrape-only/insert-only split
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from oddspedia_client import OddspediaClient  # noqa: E402
from mobile_api.ingest.atp.oddspedia_odds_ingest import SCHEMA, _to_bq_rows  # noqa: E402

DEFAULT_URL = "https://www.oddspedia.com/us/tennis"
ODDSPEDIA_URL = os.getenv("ODDSPEDIA_ATP_URL", DEFAULT_URL)
DATASET = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")
TABLE = os.getenv("ODDSPEDIA_ATP_TABLE", "atp_odds")
ROWS_TMP_PATH = "/tmp/atp_rows.json"


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _full_table_id(client: bigquery.Client) -> str:
    return f"{client.project}.{DATASET}.{TABLE}"


def _ensure_dataset(client: bigquery.Client) -> None:
    dataset_ref = f"{client.project}.{DATASET}"
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = DATASET_LOCATION
        client.create_dataset(ds)


def _ensure_table(client: bigquery.Client) -> None:
    table_id = _full_table_id(client)
    try:
        client.get_table(table_id)
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=SCHEMA))


def _truncate_and_insert(client: bigquery.Client, rows: List[Dict[str, Any]]) -> int:
    table_id = _full_table_id(client)
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    if not rows:
        return 0
    for i in range(0, len(rows), 1000):
        errors = client.insert_rows_json(table_id, rows[i:i + 1000])
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        time.sleep(0.2)
    return len(rows)


def ingest_atp_odds(
    url: Optional[str] = None,
    *,
    dry_run: bool = False,
    today_only: bool = True,
    scrape_only: bool = False,
    insert_only: bool = False,
) -> Dict[str, Any]:
    target_url = url or ODDSPEDIA_URL
    now = datetime.now(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    if insert_only:
        rows = json.loads(Path(ROWS_TMP_PATH).read_text())
    else:
        scraper = OddspediaClient()
        matches = scraper.scrape(
            target_url,
            league_category="usa",
            league_slug="atp-miami",
            season_id=134091,
            sport="tennis",
        )
        if today_only and any(m.get("date_utc") for m in matches):
            matches = [m for m in matches if (m.get("date_utc") or "").startswith(scraped_date)]
        rows = _to_bq_rows(matches, ingested_at, scraped_date)
        if dry_run or scrape_only:
            Path(ROWS_TMP_PATH).write_text(json.dumps(rows, default=str))

    if dry_run or scrape_only:
        print(json.dumps(rows[:20], indent=2, default=str))
        return {"rows_prepared": len(rows), "rows_written": 0, "dry_run": dry_run, "scrape_only": scrape_only}

    bq = _bq_client()
    _ensure_dataset(bq)
    _ensure_table(bq)
    written = _truncate_and_insert(bq, rows)
    return {"rows_prepared": len(rows), "rows_written": written, "dry_run": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--all-dates", action="store_true")
    parser.add_argument("--scrape-only", action="store_true")
    parser.add_argument("--insert-only", action="store_true")
    args = parser.parse_args()
    result = ingest_atp_odds(
        args.url,
        dry_run=args.dry_run,
        today_only=not args.all_dates,
        scrape_only=args.scrape_only,
        insert_only=args.insert_only,
    )
    print(json.dumps(result, indent=2, default=str))
