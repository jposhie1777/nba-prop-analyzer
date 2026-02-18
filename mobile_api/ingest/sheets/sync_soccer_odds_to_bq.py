"""Sync soccer odds tabs from a Google Sheet into one BigQuery table.

Expected tabs (defaults):
- EPL Odds
- La Liga Odds
- MLS Odds

All tabs are expected to share the same header schema:
Game, Start Time (ET), Home, Away, Bookmaker, Market, Outcome, Line, Price
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import gspread
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

REPO_ROOT = Path(__file__).resolve().parents[3]

SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

SPREADSHEET_ID = os.getenv("SOCCER_ODDS_SHEETS_SPREADSHEET_ID", os.getenv("SHEETS_SPREADSHEET_ID", ""))
WORKSHEETS = [
    t.strip()
    for t in os.getenv("SOCCER_ODDS_WORKSHEETS", "EPL Odds,La Liga Odds,MLS Odds").split(",")
    if t.strip()
]
BQ_LOCATION = os.getenv("SOCCER_ODDS_BQ_LOCATION", "US")
BQ_DATASET = os.getenv("SOCCER_ODDS_BQ_DATASET", "soccer_data")
BQ_TABLE = os.getenv("SOCCER_ODDS_BQ_TABLE", f"{BQ_DATASET}.odds_lines")
TRUNCATE_BEFORE_LOAD = os.getenv("SOCCER_ODDS_TRUNCATE_BEFORE_LOAD", "true").lower() == "true"

HEADER_ALIASES = {
    "game": "game",
    "starttimeet": "start_time_et",
    "home": "home_team",
    "away": "away_team",
    "bookmaker": "bookmaker",
    "market": "market",
    "outcome": "outcome",
    "line": "line",
    "price": "price",
}

SCHEMA = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("league", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_sheet", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("game", "STRING"),
    bigquery.SchemaField("start_time_et", "TIMESTAMP"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("bookmaker", "STRING"),
    bigquery.SchemaField("market", "STRING"),
    bigquery.SchemaField("outcome", "STRING"),
    bigquery.SchemaField("line", "FLOAT64"),
    bigquery.SchemaField("price", "INT64"),
    bigquery.SchemaField("raw_json", "STRING"),
]


def _normalize_header(value: Any) -> str:
    return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _league_from_tab(tab_name: str) -> str:
    name = tab_name.strip().lower()
    if "epl" in name or "premier" in name:
        return "EPL"
    if "la liga" in name or "laliga" in name:
        return "LaLiga"
    if "mls" in name:
        return "MLS"
    return tab_name.strip()


def _parse_timestamp_et(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.isoformat(sep=" ")
        except ValueError:
            continue
    return None


def _to_float(value: Any) -> Optional[float]:
    raw = str(value or "").strip()
    if raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _to_int(value: Any) -> Optional[int]:
    raw = str(value or "").strip()
    if raw == "":
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _resolve_columns(header: List[Any]) -> Dict[str, int]:
    mapping: Dict[str, int] = {}
    for idx, name in enumerate(header):
        normalized = _normalize_header(name)
        canonical = HEADER_ALIASES.get(normalized)
        if canonical:
            mapping[canonical] = idx
    required = {"game", "start_time_et", "home_team", "away_team", "bookmaker", "market", "outcome", "line", "price"}
    missing = sorted(required - set(mapping))
    if missing:
        raise ValueError(f"Missing required columns in sheet header: {', '.join(missing)}")
    return mapping


def _extract_rows(tab_name: str, rows: List[List[Any]], ingested_at: str) -> List[Dict[str, Any]]:
    if not rows:
        return []
    col_map = _resolve_columns(rows[0])
    league = _league_from_tab(tab_name)

    extracted: List[Dict[str, Any]] = []
    for row in rows[1:]:
        if not any(str(v).strip() for v in row):
            continue

        def get(key: str) -> Any:
            idx = col_map[key]
            return row[idx] if idx < len(row) else None

        record = {
            "ingested_at": ingested_at,
            "league": league,
            "source_sheet": tab_name,
            "game": str(get("game") or "").strip() or None,
            "start_time_et": _parse_timestamp_et(get("start_time_et")),
            "home_team": str(get("home_team") or "").strip() or None,
            "away_team": str(get("away_team") or "").strip() or None,
            "bookmaker": str(get("bookmaker") or "").strip() or None,
            "market": str(get("market") or "").strip() or None,
            "outcome": str(get("outcome") or "").strip() or None,
            "line": _to_float(get("line")),
            "price": _to_int(get("price")),
        }
        record["raw_json"] = json.dumps(record, ensure_ascii=False)
        extracted.append(record)
    return extracted


def _get_credentials() -> service_account.Credentials:
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        return service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)

    raise RuntimeError(
        "Missing credentials. Set GOOGLE_APPLICATION_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS."
    )


def _ensure_table(client: bigquery.Client, table_fqn: str) -> None:
    project = client.project
    dataset_id, table_id = table_fqn.split(".", 1)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = BQ_LOCATION
        try:
            client.create_dataset(dataset)
        except Conflict:
            pass

    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        client.create_table(table)


def _load_rows(client: bigquery.Client, table_fqn: str, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    if TRUNCATE_BEFORE_LOAD:
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job_config = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition=write_disposition)
    job = client.load_table_from_json(rows, table_fqn, job_config=job_config)
    job.result()
    return len(rows)


def sync_soccer_odds_to_bq() -> Dict[str, Any]:
    if not SPREADSHEET_ID:
        raise RuntimeError("SOCCER_ODDS_SHEETS_SPREADSHEET_ID (or SHEETS_SPREADSHEET_ID) is required")

    creds = _get_credentials()
    gs = gspread.authorize(creds)
    spreadsheet = gs.open_by_key(SPREADSHEET_ID)

    ingested_at = datetime.utcnow().isoformat(sep=" ")
    all_rows: List[Dict[str, Any]] = []
    sheet_counts: Dict[str, int] = {}

    for tab in WORKSHEETS:
        ws = spreadsheet.worksheet(tab)
        values = ws.get_all_values()
        parsed = _extract_rows(tab, values, ingested_at)
        sheet_counts[tab] = len(parsed)
        all_rows.extend(parsed)

    bq_client = bigquery.Client(project=os.getenv("GCP_PROJECT"), credentials=creds)
    _ensure_table(bq_client, BQ_TABLE)
    inserted = _load_rows(bq_client, BQ_TABLE, all_rows)

    return {
        "spreadsheet_id": SPREADSHEET_ID,
        "table": BQ_TABLE,
        "worksheets": WORKSHEETS,
        "rows_inserted": inserted,
        "rows_by_sheet": sheet_counts,
        "truncate_before_load": TRUNCATE_BEFORE_LOAD,
    }


if __name__ == "__main__":
    print(json.dumps(sync_soccer_odds_to_bq(), indent=2))
