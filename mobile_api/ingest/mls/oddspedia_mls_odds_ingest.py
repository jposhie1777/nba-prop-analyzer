#mobile_api/ingest/mls/oddspedia_mls_odds_ingest.py
"""Oddspedia MLS odds → BigQuery ingest (live scrape, single flat table).

Scrapes the Oddspedia MLS odds page via OddspediaClient and loads all today's
matches into BigQuery as `oddspedia.mls_odds`.

This is a NEW workflow that runs independently of the existing
oddspedia_mls_bq_workflow.py (which reads saved capture files).
Both can be run and cross-referenced.

Environment variables
---------------------
ODDSPEDIA_MLS_URL
    Full URL to scrape.
    Default: https://www.oddspedia.com/us/soccer/mls

ODDSPEDIA_DATASET
    BigQuery dataset name.
    Default: oddspedia

ODDSPEDIA_BQ_LOCATION
    BigQuery dataset region.
    Default: US

ODDSPEDIA_MLS_TABLE
    BigQuery table name.
    Default: mls_odds

GCP_PROJECT / GOOGLE_CLOUD_PROJECT
    GCP project id used by the BigQuery client.

Usage
-----
    python -m mobile_api.ingest.mls.oddspedia_mls_odds_ingest

    python -m mobile_api.ingest.mls.oddspedia_mls_odds_ingest --dry-run

    python -m mobile_api.ingest.mls.oddspedia_mls_odds_ingest --today
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

# ── Path setup ────────────────────────────────────────────────────────────────
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from oddspedia_client import OddspediaClient  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_URL = "https://www.oddspedia.com/us/soccer/usa/mls"


ODDSPEDIA_URL = os.getenv("ODDSPEDIA_MLS_URL", DEFAULT_URL)
DATASET = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")
TABLE = os.getenv("ODDSPEDIA_MLS_TABLE", "mls_odds")

# ── BigQuery schema ───────────────────────────────────────────────────────────

SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),

    bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("sport", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("home_team_id", "INT64"),
    bigquery.SchemaField("away_team_id", "INT64"),
    bigquery.SchemaField("inplay", "BOOL"),
    bigquery.SchemaField("league_id", "INT64"),

    bigquery.SchemaField("market_group_id", "INT64"),
    bigquery.SchemaField("market_group_name", "STRING"),
    bigquery.SchemaField("market", "STRING"),

    bigquery.SchemaField("period_id", "INT64"),
    bigquery.SchemaField("period_name", "STRING"),

    bigquery.SchemaField("bookie_id", "INT64"),
    bigquery.SchemaField("bookie", "STRING"),
    bigquery.SchemaField("bookie_slug", "STRING"),

    bigquery.SchemaField("outcome_key", "STRING"),
    bigquery.SchemaField("outcome_name", "STRING"),
    bigquery.SchemaField("outcome_side", "STRING"),
    bigquery.SchemaField("outcome_order", "INT64"),

    bigquery.SchemaField("odds_decimal", "FLOAT64"),
    bigquery.SchemaField("odds_american", "INT64"),
    bigquery.SchemaField("odds_status", "INT64"),
    bigquery.SchemaField("odds_direction", "INT64"),

    bigquery.SchemaField("line_value", "STRING"),
    bigquery.SchemaField("home_handicap", "STRING"),
    bigquery.SchemaField("away_handicap", "STRING"),
    bigquery.SchemaField("handicap_label", "STRING"),

    bigquery.SchemaField("winning_side", "STRING"),
    bigquery.SchemaField("bet_link", "STRING"),

    # Convenience columns for 2-way markets
    bigquery.SchemaField("home_odds_decimal", "FLOAT64"),
    bigquery.SchemaField("away_odds_decimal", "FLOAT64"),
    bigquery.SchemaField("home_odds_american", "INT64"),
    bigquery.SchemaField("away_odds_american", "INT64"),

    bigquery.SchemaField("market_json", "JSON"),
    bigquery.SchemaField("outcome_json", "JSON"),
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
        print(f"[mls_odds] Dataset {DATASET} already exists")
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = DATASET_LOCATION
        client.create_dataset(ds)
        print(f"[mls_odds] Created dataset {DATASET} (location={DATASET_LOCATION})")
    except Conflict:
        pass


def _ensure_table(client: bigquery.Client) -> None:
    table_id = _full_table_id(client)
    try:
        client.get_table(table_id)
        print(f"[mls_odds] Table {DATASET}.{TABLE} already exists")
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=SCHEMA))
        print(f"[mls_odds] Created table {DATASET}.{TABLE}")
    except Conflict:
        pass


def _add_missing_columns(client: bigquery.Client) -> None:
    table_id = _full_table_id(client)
    table = client.get_table(table_id)
    existing = {field.name for field in table.schema}
    new_fields = [field for field in SCHEMA if field.name not in existing]
    if not new_fields:
        return
    table.schema = list(table.schema) + new_fields
    client.update_table(table, ["schema"])
    print(f"[mls_odds] Added {len(new_fields)} new column(s): {[f.name for f in new_fields]}")


def _truncate_table(client: bigquery.Client) -> None:
    table_id = _full_table_id(client)
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    print(f"[mls_odds] Truncated {DATASET}.{TABLE}")


def _insert_rows(
    client: bigquery.Client,
    rows: List[Dict[str, Any]],
    *,
    chunk_size: int = 500,
) -> int:
    if not rows:
        return 0
    table_id = _full_table_id(client)
    written = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
        written += len(chunk)
        time.sleep(0.05)
    return written


# ── Normalization helpers ─────────────────────────────────────────────────────


def _safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)


def _infer_outcome_side(
    outcome_key: Optional[str],
    outcome_name: Optional[str],
    home_team: Optional[str],
    away_team: Optional[str],
) -> Optional[str]:
    key = (outcome_key or "").strip().lower()
    name = (outcome_name or "").strip().lower()
    home = (home_team or "").strip().lower()
    away = (away_team or "").strip().lower()

    if key in {"o1", "1", "home"}:
        return "home"
    if key in {"o2", "2", "away"}:
        return "away"
    # o3 = draw in soccer 1x2
    if key in {"o3", "3", "draw", "x"}:
        return "draw"

    if name in {"home", "1"}:
        return "home"
    if name in {"away", "2"}:
        return "away"
    if name in {"draw", "x", "tie"}:
        return "draw"

    if home and name == home:
        return "home"
    if away and name == away:
        return "away"

    return None


def _match_base_row(
    match: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> Dict[str, Any]:
    return {
        "ingested_at": ingested_at,
        "scraped_date": scraped_date,
        "match_id": _safe_int(match.get("match_id")),
        "sport": match.get("sport"),
        "date_utc": match.get("date_utc"),
        "home_team": match.get("home_team"),
        "away_team": match.get("away_team"),
        "home_team_id": _safe_int(match.get("home_team_id")),
        "away_team_id": _safe_int(match.get("away_team_id")),
        "inplay": bool(match.get("inplay", False)),
        "league_id": _safe_int(match.get("league_id")),
    }


def _normalize_rich_market_rows(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Handle the richer per-match market_rows format from OddspediaClient."""
    market_rows = match.get("market_rows", []) or []
    normalized: List[Dict[str, Any]] = []
    if not isinstance(market_rows, list):
        return normalized

    for row in market_rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "market_group_id": _safe_int(row.get("market_group_id")),
                "market_group_name": row.get("market_group_name"),
                "market": row.get("market"),
                "period_id": _safe_int(row.get("period_id")),
                "period_name": row.get("period_name"),
                "bookie_id": _safe_int(row.get("bookie_id")),
                "bookie": row.get("bookie"),
                "bookie_slug": row.get("bookie_slug"),
                "outcome_key": row.get("outcome_key"),
                "outcome_name": row.get("outcome_name"),
                "outcome_side": row.get("outcome_side") or _infer_outcome_side(
                    row.get("outcome_key"),
                    row.get("outcome_name"),
                    match.get("home_team"),
                    match.get("away_team"),
                ),
                "outcome_order": _safe_int(row.get("outcome_order")),
                "odds_decimal": _safe_float(row.get("odds_decimal")),
                "odds_american": _safe_int(row.get("odds_american")),
                "odds_status": _safe_int(row.get("odds_status")),
                "odds_direction": _safe_int(row.get("odds_direction")),
                "line_value": row.get("line_value"),
                "home_handicap": row.get("home_handicap"),
                "away_handicap": row.get("away_handicap"),
                "handicap_label": row.get("handicap_label"),
                "winning_side": row.get("winning_side"),
                "bet_link": row.get("bet_link"),
                "home_odds_decimal": _safe_float(row.get("home_odds_decimal")),
                "away_odds_decimal": _safe_float(row.get("away_odds_decimal")),
                "home_odds_american": _safe_int(row.get("home_odds_american")),
                "away_odds_american": _safe_int(row.get("away_odds_american")),
                "market_json": row.get("market_json"),
                "outcome_json": row.get("outcome_json"),
            }
        )
    return normalized


def _normalize_legacy_markets(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Handle the legacy listing-page markets dict format."""
    normalized: List[Dict[str, Any]] = []
    markets = match.get("markets", {}) or {}
    if not isinstance(markets, dict):
        return normalized

    for market_name, market_payload in markets.items():
        if not isinstance(market_payload, dict):
            continue
        normalized.append(
            {
                "market_group_id": _safe_int(market_payload.get("market_group_id")),
                "market_group_name": market_payload.get("market_group_name"),
                "market": market_payload.get("market") or market_name,
                "period_id": _safe_int(market_payload.get("period_id")),
                "period_name": market_payload.get("period_name"),
                "bookie_id": _safe_int(market_payload.get("bookie_id")),
                "bookie": market_payload.get("bookie"),
                "bookie_slug": market_payload.get("bookie_slug"),
                "outcome_key": None,
                "outcome_name": None,
                "outcome_side": None,
                "outcome_order": None,
                "odds_decimal": None,
                "odds_american": None,
                "odds_status": _safe_int(
                    market_payload.get("odds_status") or market_payload.get("status")
                ),
                "odds_direction": _safe_int(market_payload.get("odds_direction")),
                "line_value": market_payload.get("line_value"),
                "home_handicap": market_payload.get("home_handicap"),
                "away_handicap": market_payload.get("away_handicap"),
                "handicap_label": market_payload.get("handicap_label"),
                "winning_side": market_payload.get("winning_side"),
                "bet_link": market_payload.get("bet_link"),
                "home_odds_decimal": _safe_float(market_payload.get("home_odds_decimal")),
                "away_odds_decimal": _safe_float(market_payload.get("away_odds_decimal")),
                "home_odds_american": _safe_int(market_payload.get("home_odds_american")),
                "away_odds_american": _safe_int(market_payload.get("away_odds_american")),
                "market_json": market_payload,
                "outcome_json": None,
            }
        )
    return normalized


def _normalized_market_rows(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Collect rows from all available sources (rich first, legacy as fallback/supplement)."""
    rich_rows = _normalize_rich_market_rows(match)
    legacy_rows = _normalize_legacy_markets(match)

    if rich_rows:
        covered = {r.get("market") for r in rich_rows if r.get("market")}
        extra = [r for r in legacy_rows if r.get("market") not in covered]
        return rich_rows + extra

    return legacy_rows


def _to_bq_rows(
    matches: List[Dict[str, Any]],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for match in matches:
        base = _match_base_row(match, ingested_at, scraped_date)
        market_rows = _normalized_market_rows(match)

        if not market_rows:
            rows.append(
                {
                    **base,
                    "market_group_id": None,
                    "market_group_name": None,
                    "market": None,
                    "period_id": None,
                    "period_name": None,
                    "bookie_id": None,
                    "bookie": None,
                    "bookie_slug": None,
                    "outcome_key": None,
                    "outcome_name": None,
                    "outcome_side": None,
                    "outcome_order": None,
                    "odds_decimal": None,
                    "odds_american": None,
                    "odds_status": None,
                    "odds_direction": None,
                    "line_value": None,
                    "home_handicap": None,
                    "away_handicap": None,
                    "handicap_label": None,
                    "winning_side": None,
                    "bet_link": None,
                    "home_odds_decimal": None,
                    "away_odds_decimal": None,
                    "home_odds_american": None,
                    "away_odds_american": None,
                    "market_json": None,
                    "outcome_json": None,
                }
            )
            continue

        for market_row in market_rows:
            rows.append(
                {
                    **base,
                    "market_group_id": market_row.get("market_group_id"),
                    "market_group_name": market_row.get("market_group_name"),
                    "market": market_row.get("market"),
                    "period_id": market_row.get("period_id"),
                    "period_name": market_row.get("period_name"),
                    "bookie_id": market_row.get("bookie_id"),
                    "bookie": market_row.get("bookie"),
                    "bookie_slug": market_row.get("bookie_slug"),
                    "outcome_key": market_row.get("outcome_key"),
                    "outcome_name": market_row.get("outcome_name"),
                    "outcome_side": market_row.get("outcome_side"),
                    "outcome_order": market_row.get("outcome_order"),
                    "odds_decimal": market_row.get("odds_decimal"),
                    "odds_american": market_row.get("odds_american"),
                    "odds_status": market_row.get("odds_status"),
                    "odds_direction": market_row.get("odds_direction"),
                    "line_value": market_row.get("line_value"),
                    "home_handicap": market_row.get("home_handicap"),
                    "away_handicap": market_row.get("away_handicap"),
                    "handicap_label": market_row.get("handicap_label"),
                    "winning_side": market_row.get("winning_side"),
                    "bet_link": market_row.get("bet_link"),
                    "home_odds_decimal": market_row.get("home_odds_decimal"),
                    "away_odds_decimal": market_row.get("away_odds_decimal"),
                    "home_odds_american": market_row.get("home_odds_american"),
                    "away_odds_american": market_row.get("away_odds_american"),
                    "market_json": json.dumps(_jsonable(market_row.get("market_json"))) if market_row.get("market_json") else None,
                    "outcome_json": json.dumps(_jsonable(market_row.get("outcome_json"))) if market_row.get("outcome_json") else None,
                }
            )

    # Drop rows missing required match_id
    return [row for row in rows if row.get("match_id") is not None]


# ── Main ingest ───────────────────────────────────────────────────────────────


def ingest_mls_odds(
    url: Optional[str] = None,
    *,
    dry_run: bool = False,
    today_only: bool = True,
) -> Dict[str, Any]:
    """
    Scrape Oddspedia MLS odds and load into BigQuery (oddspedia.mls_odds).

    Supports both the rich per-match market_rows format and the legacy
    listing-page markets dict returned by OddspediaClient.
    """
    target_url = url or ODDSPEDIA_URL
    now = datetime.now(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    print("=" * 60)
    print("[mls_odds] Starting Oddspedia MLS odds ingest")
    print(f"[mls_odds] URL         : {target_url}")
    print(f"[mls_odds] Ingested at : {ingested_at}")
    if dry_run:
        print("[mls_odds] Mode        : DRY RUN (no BigQuery write)")
    else:
        print(f"[mls_odds] Destination : {DATASET}.{TABLE}")
    print("=" * 60)

    # ── Scrape ────────────────────────────────────────────────────────────────
    client_scraper = OddspediaClient()
    print(f"[mls_odds] Fetching {target_url} …")
    matches = client_scraper.scrape(target_url)

    if matches:
        print(f"DEBUG first match keys: {list(matches[0].keys())}")

    print(f"[mls_odds] Scraped {len(matches)} matches")

    # ── Filter to today ───────────────────────────────────────────────────────
    if today_only and any(m.get("date_utc") for m in matches):
        matches = [
            m for m in matches
            if (m.get("date_utc") or "").startswith(scraped_date)
        ]
        print(f"[mls_odds] After today filter : {len(matches)} matches")
    else:
        print(f"[mls_odds] Today filter skipped (no date_utc available) : {len(matches)} matches")

    # ── Prepare rows ──────────────────────────────────────────────────────────
    rows = _to_bq_rows(matches, ingested_at, scraped_date)
    print(f"[mls_odds] Prepared {len(rows)} rows from {len(matches)} matches")

    # ── Dry-run ───────────────────────────────────────────────────────────────
    if dry_run:
        print(json.dumps(rows[:25], indent=2, default=str))
        return {
            "url": target_url,
            "ingested_at": ingested_at,
            "matches_found": len(matches),
            "rows_prepared": len(rows),
            "rows_written": 0,
            "dry_run": True,
            "errors": [],
        }

    # ── BigQuery setup ────────────────────────────────────────────────────────
    bq = _bq_client()
    _ensure_dataset(bq)
    _ensure_table(bq)
    _add_missing_columns(bq)

    # ── Load ──────────────────────────────────────────────────────────────────
    print("[mls_odds] Truncating table …")
    _truncate_table(bq)

    print(f"[mls_odds] Inserting {len(rows)} rows …")
    written = _insert_rows(bq, rows)

    summary: Dict[str, Any] = {
        "url": target_url,
        "ingested_at": ingested_at,
        "matches_found": len(matches),
        "rows_prepared": len(rows),
        "rows_written": written,
        "errors": [],
    }

    print(f"[mls_odds] Done — {written} rows written ({len(matches)} matches)")
    print("=" * 60)
    return summary


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Oddspedia MLS odds and load into BigQuery (oddspedia.mls_odds)."
    )
    parser.add_argument(
        "--url",
        default=None,
        help=(
            "Oddspedia page URL to scrape. "
            f"Defaults to ODDSPEDIA_MLS_URL env var or {DEFAULT_URL}."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print prepared rows as JSON without writing to BigQuery.",
    )
    parser.add_argument(
        "--today",
        action="store_true",
        default=True,
        help="Only include matches scheduled for today (UTC). Default: true.",
    )
    parser.add_argument(
        "--all-dates",
        action="store_true",
        help="Include all matches on the page, not just today.",
    )

    args = parser.parse_args()
    today_only = not args.all_dates

    result = ingest_mls_odds(
        url=args.url,
        dry_run=args.dry_run,
        today_only=today_only,
    )
    print(json.dumps(result, indent=2, default=str))
