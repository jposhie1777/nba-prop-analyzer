"""Oddspedia ATP odds → BigQuery ingest.

Scrapes the Oddspedia tennis odds page via OddspediaClient and loads the latest
snapshot into BigQuery as `oddspedia.atp_odds`.

This file is the ingest / load layer only:
    scrape -> normalize -> flatten -> truncate -> insert

It supports both:
1) legacy match["markets"] dict output
2) richer flattened market rows returned by an upgraded oddspedia_client.py

Environment variables
---------------------
ODDSPEDIA_URL
    Full URL to scrape.
    Default: https://www.oddspedia.com/us/tennis/odds

ODDSPEDIA_DATASET
    BigQuery dataset name.
    Default: oddspedia

ODDSPEDIA_DATASET_LOCATION
    BigQuery dataset region.
    Default: US

ODDSPEDIA_TABLE
    BigQuery table name.
    Default: atp_odds

GCP_PROJECT / GOOGLE_CLOUD_PROJECT
    GCP project id used by the BigQuery client.

Usage
-----
    python -m mobile_api.ingest.atp.oddspedia_odds_ingest

    python -m mobile_api.ingest.atp.oddspedia_odds_ingest --dry-run

    python -m mobile_api.ingest.atp.oddspedia_odds_ingest --today

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
from typing import Any, Dict, Iterable, List, Optional

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

# ── Path setup ────────────────────────────────────────────────────────────────
# oddspedia_client.py lives at repo root; ensure import works when run as module.
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from oddspedia_client import OddspediaClient  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_URL = "https://www.oddspedia.com/us/tennis/odds"

ODDSPEDIA_URL = os.getenv("ODDSPEDIA_URL", DEFAULT_URL)
DATASET = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_DATASET_LOCATION", "US")
TABLE = os.getenv("ODDSPEDIA_TABLE", "atp_odds")

# ── BigQuery schema ───────────────────────────────────────────────────────────
# Keep this flexible enough for:
# - final / set periods
# - market groups
# - two-way and multi-way outcomes
# - legacy moneyline/spread-style rows

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

    # Legacy / convenience columns for easier querying of 2-way markets
    bigquery.SchemaField("home_odds_decimal", "FLOAT64"),
    bigquery.SchemaField("away_odds_decimal", "FLOAT64"),
    bigquery.SchemaField("home_odds_american", "INT64"),
    bigquery.SchemaField("away_odds_american", "INT64"),

    # Raw payloads for debugging / future parsing
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
        print(f"[atp_odds] Dataset {DATASET} already exists")
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = DATASET_LOCATION
        client.create_dataset(ds)
        print(f"[atp_odds] Created dataset {DATASET} (location={DATASET_LOCATION})")
    except Conflict:
        pass


def _ensure_table(client: bigquery.Client) -> None:
    table_id = _full_table_id(client)
    try:
        client.get_table(table_id)
        print(f"[atp_odds] Table {DATASET}.{TABLE} already exists")
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=SCHEMA))
        print(f"[atp_odds] Created table {DATASET}.{TABLE}")
    except Conflict:
        pass


def _add_missing_columns(client: bigquery.Client) -> None:
    """Patch live table with any schema fields missing in BigQuery."""
    table_id = _full_table_id(client)
    table = client.get_table(table_id)
    existing = {field.name for field in table.schema}
    new_fields = [field for field in SCHEMA if field.name not in existing]

    if not new_fields:
        return

    table.schema = list(table.schema) + new_fields
    client.update_table(table, ["schema"])
    print(
        f"[atp_odds] Added {len(new_fields)} new column(s): "
        f"{[field.name for field in new_fields]}"
    )


def _truncate_table(client: bigquery.Client) -> None:
    table_id = _full_table_id(client)
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    print(f"[atp_odds] Truncated {DATASET}.{TABLE}")


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
    """
    Try to map an outcome to home / away for common 2-way tennis markets.
    """
    key = (outcome_key or "").strip().lower()
    name = (outcome_name or "").strip().lower()
    home = (home_team or "").strip().lower()
    away = (away_team or "").strip().lower()

    if key in {"o1", "1", "home"}:
        return "home"
    if key in {"o2", "2", "away"}:
        return "away"

    if name in {"home", "player 1"}:
        return "home"
    if name in {"away", "player 2"}:
        return "away"

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


def _normalize_legacy_markets(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Support the current / older client output format:

    match["markets"] = {
        "moneyline": {
            "bookie": ...,
            "bookie_slug": ...,
            "home_odds_decimal": ...,
            "away_odds_decimal": ...,
            ...
        },
        ...
    }
    """
    normalized: List[Dict[str, Any]] = []
    markets = match.get("markets", {}) or {}

    if not isinstance(markets, dict):
        return normalized

    for market_name, market_payload in markets.items():
        if not isinstance(market_payload, dict):
            continue

        normalized.append(
            {
                "market_group_id": None,
                "market_group_name": None,
                "market": market_name,
                "period_id": None,
                "period_name": None,
                "bookie_id": None,
                "bookie": market_payload.get("bookie"),
                "bookie_slug": market_payload.get("bookie_slug"),
                "outcome_key": None,
                "outcome_name": None,
                "outcome_side": None,
                "outcome_order": None,
                "odds_decimal": None,
                "odds_american": None,
                "odds_status": market_payload.get("status"),
                "odds_direction": None,
                "line_value": None,
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


def _normalize_rich_market_rows(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Preferred upgraded format returned by oddspedia_client.py:

    match["market_rows"] = [
        {
            "market_group_id": 2,
            "market_group_name": "Moneyline",
            "market": "moneyline",
            "period_id": 201,
            "period_name": "Final",
            "bookie_id": 126,
            "bookie": "bet365",
            "bookie_slug": "bet365",
            "outcome_key": "o1",
            "outcome_name": "Home",
            "outcome_side": "home",
            "outcome_order": 1,
            "odds_decimal": 1.363,
            "odds_american": -275,
            "odds_status": 3,
            "odds_direction": -1,
            "line_value": None,
            "home_handicap": None,
            "away_handicap": None,
            "handicap_label": None,
            "winning_side": None,
            "bet_link": "...",
            "market_json": {...},
            "outcome_json": {...}
        }
    ]
    """
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
                "outcome_side": row.get("outcome_side"),
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


def _normalize_outcomes_from_group_payload(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Secondary supported upgraded format if oddspedia_client.py returns raw grouped API payloads:

    match["market_groups"] = [
        {
            "market_group_id": 2,
            "market_group_name": "Moneyline",
            "market": "moneyline",
            "periods": [{"id": 201, "name": "Final"}, ...],
            "data": {
                "market_name": "Moneyline",
                "market_group_id": 2,
                "outcome_names": ["Home", "Away"],
                "odds": {
                    "201": {
                        "winning_odd": null,
                        "odds": {
                            "o1": {...},
                            "o2": {...}
                        }
                    }
                }
            }
        }
    ]
    """
    groups = match.get("market_groups", []) or []
    normalized: List[Dict[str, Any]] = []

    if not isinstance(groups, list):
        return normalized

    for group in groups:
        if not isinstance(group, dict):
            continue

        market_group_id = _safe_int(
            group.get("market_group_id")
            or group.get("group_id")
            or (group.get("data") or {}).get("market_group_id")
        )
        market_group_name = (
            group.get("market_group_name")
            or group.get("market_name")
            or (group.get("data") or {}).get("market_name")
        )
        market_name = group.get("market") or market_group_name
        periods = group.get("periods", []) or (group.get("data") or {}).get("periods", [])
        periods_by_id = {
            str(period.get("id")): period.get("name")
            for period in periods
            if isinstance(period, dict) and period.get("id") is not None
        }

        payload = group.get("data", {}) if isinstance(group.get("data"), dict) else group
        outcome_names = payload.get("outcome_names", []) or []
        odds_by_period = payload.get("odds", {}) or {}

        for period_key, period_payload in odds_by_period.items():
            if not isinstance(period_payload, dict):
                continue

            period_id = _safe_int(period_key)
            period_name = periods_by_id.get(str(period_key))
            odds_obj = period_payload.get("odds", {}) or {}

            for idx, (outcome_key, outcome_payload) in enumerate(odds_obj.items(), start=1):
                if not isinstance(outcome_payload, dict):
                    continue

                outcome_name = None
                if outcome_key.startswith("o"):
                    key_num = _safe_int(outcome_key[1:])
                    if key_num and 1 <= key_num <= len(outcome_names):
                        outcome_name = outcome_names[key_num - 1]

                outcome_side = _infer_outcome_side(
                    outcome_key=outcome_key,
                    outcome_name=outcome_name,
                    home_team=match.get("home_team"),
                    away_team=match.get("away_team"),
                )

                odds_decimal = _safe_float(outcome_payload.get("odds_value"))
                odds_direction = _safe_int(outcome_payload.get("odds_direction"))

                normalized.append(
                    {
                        "market_group_id": market_group_id,
                        "market_group_name": market_group_name,
                        "market": market_name,
                        "period_id": period_id,
                        "period_name": period_name,
                        "bookie_id": _safe_int(outcome_payload.get("bid")),
                        "bookie": outcome_payload.get("bookie_name"),
                        "bookie_slug": outcome_payload.get("bookie_slug"),
                        "outcome_key": outcome_key,
                        "outcome_name": outcome_name,
                        "outcome_side": outcome_side,
                        "outcome_order": idx,
                        "odds_decimal": odds_decimal,
                        "odds_american": None,
                        "odds_status": _safe_int(outcome_payload.get("odds_status")),
                        "odds_direction": odds_direction,
                        "line_value": outcome_payload.get("line_value"),
                        "home_handicap": None,
                        "away_handicap": None,
                        "handicap_label": None,
                        "winning_side": None,
                        "bet_link": outcome_payload.get("odds_link"),
                        "home_odds_decimal": odds_decimal if outcome_side == "home" else None,
                        "away_odds_decimal": odds_decimal if outcome_side == "away" else None,
                        "home_odds_american": None,
                        "away_odds_american": None,
                        "market_json": payload,
                        "outcome_json": outcome_payload,
                    }
                )

    return normalized


def _normalized_market_rows(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Try richer structures first, fall back to legacy format.
    """
    rows = _normalize_rich_market_rows(match)
    if rows:
        return rows

    rows = _normalize_outcomes_from_group_payload(match)
    if rows:
        return rows

    return _normalize_legacy_markets(match)


def _to_bq_rows(
    matches: List[Dict[str, Any]],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    """
    Flatten matches into BigQuery-ready rows.
    """
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
                    "market_json": _jsonable(market_row.get("market_json")),
                    "outcome_json": _jsonable(market_row.get("outcome_json")),
                }
            )

    # Drop rows missing required match_id just in case the scraper returns junk.
    cleaned_rows = [row for row in rows if row.get("match_id") is not None]
    return cleaned_rows


# ── Main ingest ───────────────────────────────────────────────────────────────


def ingest_atp_odds(
    url: Optional[str] = None,
    *,
    dry_run: bool = False,
    today_only: bool = False,
    has_total: bool = False,
) -> Dict[str, Any]:
    """
    Scrape Oddspedia ATP odds and load into BigQuery (oddspedia.atp_odds).

    This file assumes OddspediaClient.scrape(url) returns a list of matches.
    It supports either legacy markets or richer upgraded market rows.
    """
    target_url = url or ODDSPEDIA_URL
    now = datetime.now(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
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
        matches = [
            match for match in matches
            if (match.get("date_utc") or "").startswith(scraped_date)
        ]
        print(f"[atp_odds] After --today filter     : {len(matches)} matches")

    if has_total:
        filtered_matches: List[Dict[str, Any]] = []
        for match in matches:
            market_rows = _normalized_market_rows(match)
            has_total_market = any(
                "total" in str((row.get("market") or "")).lower()
                or "total" in str((row.get("market_group_name") or "")).lower()
                for row in market_rows
            )
            if has_total_market:
                filtered_matches.append(match)

        matches = filtered_matches
        print(f"[atp_odds] After --has-total filter : {len(matches)} matches")

    # ── Prepare rows ──────────────────────────────────────────────────────────
    rows = _to_bq_rows(matches, ingested_at, scraped_date)
    print(f"[atp_odds] Prepared {len(rows)} rows from {len(matches)} matches")

    # ── Dry-run ───────────────────────────────────────────────────────────────
    if dry_run:
        print(json.dumps(rows[:25], indent=2, default=str))
        return {
            "url": target_url,
            "ingested_at": ingested_at,
            "matches_found": len(matches),
            "rows_written": 0,
            "rows_prepared": len(rows),
            "dry_run": True,
            "errors": [],
        }

    # ── BigQuery setup ────────────────────────────────────────────────────────
    bq = _bq_client()
    _ensure_dataset(bq)
    _ensure_table(bq)
    _add_missing_columns(bq)

    # ── Load ──────────────────────────────────────────────────────────────────
    print("[atp_odds] Truncating table …")
    _truncate_table(bq)

    print(f"[atp_odds] Inserting {len(rows)} rows …")
    written = _insert_rows(bq, rows)

    summary: Dict[str, Any] = {
        "url": target_url,
        "ingested_at": ingested_at,
        "matches_found": len(matches),
        "rows_prepared": len(rows),
        "rows_written": written,
        "errors": [],
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
        help="Print prepared rows as JSON without writing to BigQuery.",
    )
    parser.add_argument(
        "--today",
        action="store_true",
        help="Only include matches scheduled for today (UTC).",
    )
    parser.add_argument(
        "--has-total",
        action="store_true",
        help="Only include matches that have a total market.",
    )

    args = parser.parse_args()

    result = ingest_atp_odds(
        url=args.url,
        dry_run=args.dry_run,
        today_only=args.today,
        has_total=args.has_total,
    )
    print(json.dumps(result, indent=2, default=str))