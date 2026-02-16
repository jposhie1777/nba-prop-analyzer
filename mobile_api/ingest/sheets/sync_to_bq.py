"""Sync today's matches from a Google Sheet into BigQuery.

Reads all rows from the configured Google Sheet, filters for matches
scheduled today (based on the Scheduled Time column), and inserts them
into the BigQuery table `atp_data.sheet_daily_matches`.

Usage (GitHub Action, Cloud Run job, or local):
    python mobile_api/ingest/sheets/sync_to_bq.py

Environment variables:
    GOOGLE_APPLICATION_CREDENTIALS_JSON  Service-account JSON (string)
    GOOGLE_APPLICATION_CREDENTIALS       Path to service-account JSON file
    GCP_PROJECT                          Google Cloud project ID
    SHEETS_SPREADSHEET_ID                Google Sheet ID (default provided)
    SHEETS_WORKSHEET_NAME                Worksheet tab name (default: ATP Matches)
    SHEETS_WORKSHEET_INDEX               0-based worksheet tab index fallback (default: 2)
    SHEETS_BQ_DATASET                    BigQuery dataset (default: atp_data)
    SHEETS_BQ_TABLE                      BigQuery table  (default: atp_data.sheet_daily_matches)
    SHEETS_MATCH_DATE                    Override date filter (YYYY-MM-DD); defaults to today UTC
    SHEETS_TRUNCATE_BEFORE_LOAD          Truncate the BQ table before loading (default: true)
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

EST = ZoneInfo("America/New_York")
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow running as: python mobile_api/ingest/sheets/sync_to_bq.py
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

import gspread
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

# ======================================================
# Config
# ======================================================

SPREADSHEET_ID = os.getenv(
    "SHEETS_SPREADSHEET_ID",
    "1p_rmmiUgU18afioJJ3jCHh9XeX7V4gyHd_E0M3A8M3g",
)
WORKSHEET_NAME = os.getenv("SHEETS_WORKSHEET_NAME", "ATP Matches")
WORKSHEET_INDEX = int(os.getenv("SHEETS_WORKSHEET_INDEX", "2"))

DEFAULT_DATASET = os.getenv("SHEETS_BQ_DATASET", "atp_data")
DEFAULT_TABLE = os.getenv("SHEETS_BQ_TABLE", f"{DEFAULT_DATASET}.sheet_daily_matches")
BQ_LOCATION = os.getenv("SHEETS_BQ_LOCATION", "US")

SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# Column positions (0-indexed) in the Google Sheet.
# These match the header row provided by the user.
COL = {
    "match_id": 0,
    "tournament_id": 1,
    "tournament_name": 2,
    "tournament_location": 3,
    "surface": 4,
    "category": 5,
    "tournament_season": 6,
    "tournament_start_date": 7,
    "tournament_end_date": 8,
    "prize_money": 9,
    "prize_currency": 10,
    "draw_size": 11,
    "season": 12,
    "round": 13,
    "player1_id": 14,
    "player1_first_name": 15,
    "player1_last_name": 16,
    "player1_full_name": 17,
    "player1_country": 18,
    "player1_country_code": 19,
    "player1_birth_place": 20,
    "player1_age": 21,
    "player1_height_cm": 22,
    "player1_weight_kg": 23,
    "player1_plays": 24,
    "player1_turned_pro": 25,
    "player2_id": 26,
    "player2_first_name": 27,
    "player2_last_name": 28,
    "player2_full_name": 29,
    "player2_country": 30,
    "player2_country_code": 31,
    "player2_birth_place": 32,
    "player2_age": 33,
    "player2_height_cm": 34,
    "player2_weight_kg": 35,
    "player2_plays": 36,
    "player2_turned_pro": 37,
    "winner_id": 38,
    "winner_first_name": 39,
    "winner_last_name": 40,
    "winner_full_name": 41,
    "winner_country": 42,
    "winner_country_code": 43,
    "winner_birth_place": 44,
    "winner_age": 45,
    "winner_height_cm": 46,
    "winner_weight_kg": 47,
    "winner_plays": 48,
    "winner_turned_pro": 49,
    "score": 50,
    "duration": 51,
    "number_of_sets": 52,
    "match_status": 53,
    "is_live": 54,
    "scheduled_time": 55,
    "not_before_text": 56,
    "winner_label": 57,
}

# Alternate sheet layout used by ATP Matches where `Winner` is a label
# before score/duration and winner profile columns appear after
# `Not Before Text`.
COL_ALT_WINNER_LABEL = {
    **COL,
    "winner_label": 38,
    "score": 39,
    "duration": 40,
    "number_of_sets": 41,
    "match_status": 42,
    "is_live": 43,
    "scheduled_time": 44,
    "not_before_text": 45,
    "winner_id": 46,
    "winner_first_name": 47,
    "winner_last_name": 48,
    "winner_full_name": 49,
    "winner_country": 50,
    "winner_country_code": 51,
    "winner_birth_place": 52,
    "winner_age": 53,
    "winner_height_cm": 54,
    "winner_weight_kg": 55,
    "winner_plays": 56,
    "winner_turned_pro": 57,
}


def _normalized_header(value: Any) -> str:
    return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def resolve_column_map(header_row: List[Any]) -> Dict[str, int]:
    """Resolve column indexes from header layout.

    Supports both known ATP sheet variants.
    """
    col_map = dict(COL)
    normalized = [_normalized_header(h) for h in header_row]

    # Variant detection: older layout has `Winner` text column at index 38.
    if len(normalized) > 38 and normalized[38] == "winner":
        col_map = dict(COL_ALT_WINNER_LABEL)

    # Header-name override for scheduled/not-before when available.
    for idx, name in enumerate(normalized):
        if name == "scheduledtime":
            col_map["scheduled_time"] = idx
        elif name == "notbeforetext":
            col_map["not_before_text"] = idx

    return col_map

# ======================================================
# BigQuery schema
# ======================================================

SCHEMA = [
    bigquery.SchemaField("sync_ts", "TIMESTAMP"),
    bigquery.SchemaField("match_date", "DATE"),
    bigquery.SchemaField("match_id", "INT64"),
    bigquery.SchemaField("tournament_id", "INT64"),
    bigquery.SchemaField("tournament_name", "STRING"),
    bigquery.SchemaField("tournament_location", "STRING"),
    bigquery.SchemaField("surface", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("tournament_season", "INT64"),
    bigquery.SchemaField("tournament_start_date", "STRING"),
    bigquery.SchemaField("tournament_end_date", "STRING"),
    bigquery.SchemaField("prize_money", "INT64"),
    bigquery.SchemaField("prize_currency", "STRING"),
    bigquery.SchemaField("draw_size", "INT64"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("round", "STRING"),
    bigquery.SchemaField("player1_id", "INT64"),
    bigquery.SchemaField("player1_first_name", "STRING"),
    bigquery.SchemaField("player1_last_name", "STRING"),
    bigquery.SchemaField("player1_full_name", "STRING"),
    bigquery.SchemaField("player1_country", "STRING"),
    bigquery.SchemaField("player1_country_code", "STRING"),
    bigquery.SchemaField("player1_birth_place", "STRING"),
    bigquery.SchemaField("player1_age", "INT64"),
    bigquery.SchemaField("player1_height_cm", "INT64"),
    bigquery.SchemaField("player1_weight_kg", "INT64"),
    bigquery.SchemaField("player1_plays", "STRING"),
    bigquery.SchemaField("player1_turned_pro", "INT64"),
    bigquery.SchemaField("player2_id", "INT64"),
    bigquery.SchemaField("player2_first_name", "STRING"),
    bigquery.SchemaField("player2_last_name", "STRING"),
    bigquery.SchemaField("player2_full_name", "STRING"),
    bigquery.SchemaField("player2_country", "STRING"),
    bigquery.SchemaField("player2_country_code", "STRING"),
    bigquery.SchemaField("player2_birth_place", "STRING"),
    bigquery.SchemaField("player2_age", "INT64"),
    bigquery.SchemaField("player2_height_cm", "INT64"),
    bigquery.SchemaField("player2_weight_kg", "INT64"),
    bigquery.SchemaField("player2_plays", "STRING"),
    bigquery.SchemaField("player2_turned_pro", "INT64"),
    bigquery.SchemaField("winner_id", "INT64"),
    bigquery.SchemaField("winner_first_name", "STRING"),
    bigquery.SchemaField("winner_last_name", "STRING"),
    bigquery.SchemaField("winner_full_name", "STRING"),
    bigquery.SchemaField("winner_country", "STRING"),
    bigquery.SchemaField("winner_country_code", "STRING"),
    bigquery.SchemaField("winner_birth_place", "STRING"),
    bigquery.SchemaField("winner_age", "INT64"),
    bigquery.SchemaField("winner_height_cm", "INT64"),
    bigquery.SchemaField("winner_weight_kg", "INT64"),
    bigquery.SchemaField("winner_plays", "STRING"),
    bigquery.SchemaField("winner_turned_pro", "INT64"),
    bigquery.SchemaField("score", "STRING"),
    bigquery.SchemaField("duration", "STRING"),
    bigquery.SchemaField("number_of_sets", "INT64"),
    bigquery.SchemaField("match_status", "STRING"),
    bigquery.SchemaField("is_live", "BOOL"),
    bigquery.SchemaField("scheduled_time", "TIMESTAMP"),
    bigquery.SchemaField("not_before_text", "STRING"),
    bigquery.SchemaField("raw_json", "STRING"),
]


# ======================================================
# Auth helpers
# ======================================================


def _get_credentials() -> service_account.Credentials:
    """Build scoped credentials from env vars."""
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        return service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)

    raise RuntimeError(
        "Set GOOGLE_APPLICATION_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS"
    )


def get_bq_client(credentials: service_account.Credentials) -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if project:
        return bigquery.Client(project=project, credentials=credentials)
    return bigquery.Client(credentials=credentials)


def get_sheets_client(credentials: service_account.Credentials) -> gspread.Client:
    return gspread.authorize(credentials)


# ======================================================
# BigQuery helpers
# ======================================================


def resolve_table_id(table: str, project: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{project}.{table}"
    return f"{project}.{DEFAULT_DATASET}.{table}"


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        ds = bigquery.Dataset(dataset_id)
        ds.location = BQ_LOCATION
        client.create_dataset(ds)
    except Conflict:
        pass


def ensure_table(
    client: bigquery.Client, table_id: str, schema: List[bigquery.SchemaField]
) -> None:
    try:
        existing = client.get_table(table_id)
        existing_fields = {f.name for f in existing.schema}
        missing = [f for f in schema if f.name not in existing_fields]
        if missing:
            existing.schema = list(existing.schema) + missing
            client.update_table(existing, ["schema"])
    except NotFound:
        tbl = bigquery.Table(table_id, schema=schema)
        client.create_table(tbl)
    except Conflict:
        pass


# ======================================================
# Sheet reading & transformation
# ======================================================


def _safe_int(val: Any) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_bool(val: Any) -> Optional[bool]:
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return val
    return str(val).upper() == "TRUE"


def _parse_scheduled_time(val: Any) -> Optional[str]:
    """Parse a scheduled-time value; return ISO format or None.

    Google Sheets may return timestamps in multiple textual formats depending
    on sheet formatting (ISO strings, US-style date/time, hidden leading
    apostrophes for text cells, etc.).
    """
    dt = _parse_sheet_datetime(val)
    return dt.isoformat() if dt else None


def _parse_sheet_datetime(val: Any) -> Optional[datetime]:
    """Best-effort parse for sheet datetime values."""
    if val is None:
        return None

    if isinstance(val, datetime):
        dt = val
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    text = str(val).strip()
    if not text:
        return None

    # Google Sheets sometimes stores plain-text timestamps with a leading
    # apostrophe; strip it before parsing.
    text = text.lstrip("'")

    # Most common case: ISO timestamp with `Z` suffix.
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M %p",
    ):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None


def _cell(row: List[Any], idx: int) -> Any:
    """Safely get a cell value from a row by index."""
    if idx < len(row):
        return row[idx]
    return None


def transform_row(
    row: List[Any],
    sync_ts: str,
    match_date: str,
    col_map: Dict[str, int],
) -> Dict[str, Any]:
    """Transform a single sheet row into a BigQuery-ready dict."""
    return {
        "sync_ts": sync_ts,
        "match_date": match_date,
        "match_id": _safe_int(_cell(row, col_map["match_id"])),
        "tournament_id": _safe_int(_cell(row, col_map["tournament_id"])),
        "tournament_name": _cell(row, col_map["tournament_name"]) or None,
        "tournament_location": _cell(row, col_map["tournament_location"]) or None,
        "surface": _cell(row, col_map["surface"]) or None,
        "category": _cell(row, col_map["category"]) or None,
        "tournament_season": _safe_int(_cell(row, col_map["tournament_season"])),
        "tournament_start_date": _cell(row, col_map["tournament_start_date"]) or None,
        "tournament_end_date": _cell(row, col_map["tournament_end_date"]) or None,
        "prize_money": _safe_int(_cell(row, col_map["prize_money"])),
        "prize_currency": _cell(row, col_map["prize_currency"]) or None,
        "draw_size": _safe_int(_cell(row, col_map["draw_size"])),
        "season": _safe_int(_cell(row, col_map["season"])),
        "round": _cell(row, col_map["round"]) or None,
        "player1_id": _safe_int(_cell(row, col_map["player1_id"])),
        "player1_first_name": _cell(row, col_map["player1_first_name"]) or None,
        "player1_last_name": _cell(row, col_map["player1_last_name"]) or None,
        "player1_full_name": _cell(row, col_map["player1_full_name"]) or None,
        "player1_country": _cell(row, col_map["player1_country"]) or None,
        "player1_country_code": _cell(row, col_map["player1_country_code"]) or None,
        "player1_birth_place": _cell(row, col_map["player1_birth_place"]) or None,
        "player1_age": _safe_int(_cell(row, col_map["player1_age"])),
        "player1_height_cm": _safe_int(_cell(row, col_map["player1_height_cm"])),
        "player1_weight_kg": _safe_int(_cell(row, col_map["player1_weight_kg"])),
        "player1_plays": _cell(row, col_map["player1_plays"]) or None,
        "player1_turned_pro": _safe_int(_cell(row, col_map["player1_turned_pro"])),
        "player2_id": _safe_int(_cell(row, col_map["player2_id"])),
        "player2_first_name": _cell(row, col_map["player2_first_name"]) or None,
        "player2_last_name": _cell(row, col_map["player2_last_name"]) or None,
        "player2_full_name": _cell(row, col_map["player2_full_name"]) or None,
        "player2_country": _cell(row, col_map["player2_country"]) or None,
        "player2_country_code": _cell(row, col_map["player2_country_code"]) or None,
        "player2_birth_place": _cell(row, col_map["player2_birth_place"]) or None,
        "player2_age": _safe_int(_cell(row, col_map["player2_age"])),
        "player2_height_cm": _safe_int(_cell(row, col_map["player2_height_cm"])),
        "player2_weight_kg": _safe_int(_cell(row, col_map["player2_weight_kg"])),
        "player2_plays": _cell(row, col_map["player2_plays"]) or None,
        "player2_turned_pro": _safe_int(_cell(row, col_map["player2_turned_pro"])),
        "winner_id": _safe_int(_cell(row, col_map["winner_id"])),
        "winner_first_name": _cell(row, col_map["winner_first_name"]) or None,
        "winner_last_name": _cell(row, col_map["winner_last_name"]) or None,
        "winner_full_name": _cell(row, col_map["winner_full_name"]) or None,
        "winner_country": _cell(row, col_map["winner_country"]) or None,
        "winner_country_code": _cell(row, col_map["winner_country_code"]) or None,
        "winner_birth_place": _cell(row, col_map["winner_birth_place"]) or None,
        "winner_age": _safe_int(_cell(row, col_map["winner_age"])),
        "winner_height_cm": _safe_int(_cell(row, col_map["winner_height_cm"])),
        "winner_weight_kg": _safe_int(_cell(row, col_map["winner_weight_kg"])),
        "winner_plays": _cell(row, col_map["winner_plays"]) or None,
        "winner_turned_pro": _safe_int(_cell(row, col_map["winner_turned_pro"])),
        "score": _cell(row, col_map["score"]) or None,
        "duration": _cell(row, col_map["duration"]) or None,
        "number_of_sets": _safe_int(_cell(row, col_map["number_of_sets"])),
        "match_status": _cell(row, col_map["match_status"]) or None,
        "is_live": _safe_bool(_cell(row, col_map["is_live"])),
        "scheduled_time": _parse_scheduled_time(_cell(row, col_map["scheduled_time"])),
        "not_before_text": _cell(row, col_map["not_before_text"]) or None,
        "raw_json": json.dumps(
            {str(k): _cell(row, v) for k, v in col_map.items()},
            separators=(",", ":"),
            ensure_ascii=True,
        ),
    }


def is_match_on_date(row: List[Any], target_date: str, col_map: Dict[str, int]) -> bool:
    """Check if a row's Scheduled Time falls on the target date (YYYY-MM-DD).

    Compares against the UTC date from the timestamp, since the sheet groups
    a day's slate by UTC date.  Matches with placeholder times (T00:00:00Z
    for "Followed By" / TBD starts) belong to that UTC day's slate even
    though they'd roll back a calendar day in EST.
    """
    raw = _cell(row, col_map["scheduled_time"])
    if not raw:
        return False
    dt_utc = _parse_sheet_datetime(raw)
    if dt_utc is None:
        return False
    return dt_utc.strftime("%Y-%m-%d") == target_date


# ======================================================
# Main sync logic
# ======================================================


def sync_sheet_to_bq(
    *,
    match_date: Optional[str] = None,
    table: str = DEFAULT_TABLE,
    truncate: bool = True,
) -> Dict[str, Any]:
    """Read the Google Sheet, filter for today's matches, and write to BigQuery."""
    credentials = _get_credentials()
    bq_client = get_bq_client(credentials)
    gs_client = get_sheets_client(credentials)

    # Resolve target date
    if match_date is None:
        match_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    table_id = resolve_table_id(table, bq_client.project)
    dataset_id = ".".join(table_id.split(".")[:2])

    # Ensure BQ dataset & table exist
    ensure_dataset(bq_client, dataset_id)
    ensure_table(bq_client, table_id, SCHEMA)

    # Read all rows from the sheet
    print(f"  Opening spreadsheet {SPREADSHEET_ID} ...")
    spreadsheet = gs_client.open_by_key(SPREADSHEET_ID)
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        print(f"  Using worksheet: '{WORKSHEET_NAME}'")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.get_worksheet(WORKSHEET_INDEX)
        print(f"  Worksheet '{WORKSHEET_NAME}' not found, using index {WORKSHEET_INDEX}")
    all_rows = worksheet.get_all_values()

    if not all_rows:
        print("  Sheet is empty.")
        return {"table": table_id, "match_date": match_date, "total_rows": 0, "filtered": 0, "inserted": 0}

    # Resolve column layout from header, then skip header row
    header_row = all_rows[0]
    col_map = resolve_column_map(header_row)
    data_rows = all_rows[1:]
    print(f"  Total data rows in sheet: {len(data_rows)}")

    # Filter for target date
    today_rows = [r for r in data_rows if is_match_on_date(r, match_date, col_map)]
    print(f"  Matches on {match_date}: {len(today_rows)}")

    if not today_rows:
        print("  No matches for today. Skipping load and preserving existing table data.")
        return {"table": table_id, "match_date": match_date, "total_rows": len(data_rows), "filtered": 0, "inserted": 0}

    # Optionally truncate only when we actually have rows to load.
    # This prevents accidental emptying of the table on date/input mismatches.
    if truncate:
        print(f"  Truncating {table_id} ...")
        try:
            bq_client.query(f"TRUNCATE TABLE `{table_id}`").result()
        except Exception as exc:
            print(f"  (truncate skipped: {exc})")

    # Transform and load
    sync_ts = datetime.now(timezone.utc).isoformat()
    bq_rows = [transform_row(row, sync_ts, match_date, col_map) for row in today_rows]

    # Use a load job (not streaming insert) so that TRUNCATE works immediately
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = bq_client.load_table_from_json(bq_rows, table_id, job_config=job_config)
    load_job.result()  # wait for completion
    inserted = load_job.output_rows

    print(f"  Inserted {inserted} rows into {table_id}")
    return {
        "table": table_id,
        "match_date": match_date,
        "total_rows": len(data_rows),
        "filtered": len(today_rows),
        "inserted": inserted,
    }


def main() -> None:
    match_date = (os.getenv("SHEETS_MATCH_DATE") or "").strip() or None
    table = os.getenv("SHEETS_BQ_TABLE", DEFAULT_TABLE)
    truncate = os.getenv("SHEETS_TRUNCATE_BEFORE_LOAD", "true").lower() == "true"

    print("=" * 60)
    print("Google Sheets -> BigQuery Sync")
    print(f"  Spreadsheet  : {SPREADSHEET_ID}")
    print(f"  Match date   : {match_date or 'today (UTC)'}")
    print(f"  Target table : {table}")
    print(f"  Truncate     : {truncate}")
    print("=" * 60)

    result = sync_sheet_to_bq(match_date=match_date, table=table, truncate=truncate)

    print("\n" + "=" * 60)
    print("Sync complete.")
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
