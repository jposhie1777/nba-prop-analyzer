"""Oddspedia EPL odds → BigQuery ingest (live scrape, single flat table).

Scrapes the Oddspedia Premier League odds page via OddspediaClient and loads
all this gameweek's matches into BigQuery as `oddspedia.epl_odds`.

Mirrors oddspedia_mls_odds_ingest.py with EPL-specific identifiers:
  - league: premier-league  (leagueId=627, seasonId=130281)
  - category: england
  - matchKey-based stats endpoints (not matchId)
  - getOutrights returns 404 — handled gracefully

Environment variables
---------------------
ODDSPEDIA_EPL_URL
    Full URL to scrape.
    Default: https://www.oddspedia.com/us/soccer/england/premier-league

ODDSPEDIA_DATASET
    BigQuery dataset name.  Default: oddspedia

ODDSPEDIA_BQ_LOCATION
    BigQuery dataset region.  Default: US

ODDSPEDIA_EPL_TABLE
    BigQuery table name.  Default: epl_odds

GCP_PROJECT / GOOGLE_CLOUD_PROJECT
    GCP project id used by the BigQuery client.

Usage
-----
    python -m mobile_api.ingest.epl.oddspedia_epl_odds_ingest
    python -m mobile_api.ingest.epl.oddspedia_epl_odds_ingest --dry-run
    python -m mobile_api.ingest.epl.oddspedia_epl_odds_ingest --scrape-only
    python -m mobile_api.ingest.epl.oddspedia_epl_odds_ingest --insert-only
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
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from oddspedia_client import OddspediaClient  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_URL = "https://www.oddspedia.com/us/soccer/england/premier-league"

ODDSPEDIA_URL    = os.getenv("ODDSPEDIA_EPL_URL", DEFAULT_URL)
DATASET          = os.getenv("ODDSPEDIA_DATASET", "oddspedia")
DATASET_LOCATION = os.getenv("ODDSPEDIA_BQ_LOCATION", "US")
TABLE            = os.getenv("ODDSPEDIA_EPL_TABLE", "epl_odds")

EPL_LEAGUE_ID   = 627
EPL_SEASON_ID   = 130281
EPL_CATEGORY    = "england"
EPL_LEAGUE_SLUG = "premier-league"

ROWS_TMP_PATH = "/tmp/epl_rows.json"

# ── BigQuery schema ───────────────────────────────────────────────────────────

SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at",       "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scraped_date",       "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("match_id",           "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("match_key",          "INT64"),
    bigquery.SchemaField("sport",              "STRING"),
    bigquery.SchemaField("date_utc",           "TIMESTAMP"),
    bigquery.SchemaField("home_team",          "STRING"),
    bigquery.SchemaField("away_team",          "STRING"),
    bigquery.SchemaField("home_team_id",       "INT64"),
    bigquery.SchemaField("away_team_id",       "INT64"),
    bigquery.SchemaField("inplay",             "BOOL"),
    bigquery.SchemaField("league_id",          "INT64"),
    bigquery.SchemaField("round_name",         "STRING"),
    bigquery.SchemaField("market_group_id",    "INT64"),
    bigquery.SchemaField("market_group_name",  "STRING"),
    bigquery.SchemaField("market",             "STRING"),
    bigquery.SchemaField("period_id",          "INT64"),
    bigquery.SchemaField("period_name",        "STRING"),
    bigquery.SchemaField("bookie_id",          "INT64"),
    bigquery.SchemaField("bookie",             "STRING"),
    bigquery.SchemaField("bookie_slug",        "STRING"),
    bigquery.SchemaField("outcome_key",        "STRING"),
    bigquery.SchemaField("outcome_name",       "STRING"),
    bigquery.SchemaField("outcome_side",       "STRING"),
    bigquery.SchemaField("outcome_order",      "INT64"),
    bigquery.SchemaField("odds_decimal",       "FLOAT64"),
    bigquery.SchemaField("odds_american",      "INT64"),
    bigquery.SchemaField("odds_status",        "INT64"),
    bigquery.SchemaField("odds_direction",     "INT64"),
    bigquery.SchemaField("line_value",         "STRING"),
    bigquery.SchemaField("home_handicap",      "STRING"),
    bigquery.SchemaField("away_handicap",      "STRING"),
    bigquery.SchemaField("handicap_label",     "STRING"),
    bigquery.SchemaField("winning_side",       "STRING"),
    bigquery.SchemaField("bet_link",           "STRING"),
    bigquery.SchemaField("home_odds_decimal",  "FLOAT64"),
    bigquery.SchemaField("away_odds_decimal",  "FLOAT64"),
    bigquery.SchemaField("home_odds_american", "INT64"),
    bigquery.SchemaField("away_odds_american", "INT64"),
    bigquery.SchemaField("market_json",        "JSON"),
    bigquery.SchemaField("outcome_json",       "JSON"),
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
        print(f"[epl_odds] Dataset {DATASET} already exists")
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = DATASET_LOCATION
        client.create_dataset(ds)
        print(f"[epl_odds] Created dataset {DATASET} (location={DATASET_LOCATION})")
    except Conflict:
        pass


def _ensure_table(client: bigquery.Client) -> None:
    table_id = _full_table_id(client)
    try:
        client.get_table(table_id)
        print(f"[epl_odds] Table {DATASET}.{TABLE} already exists")
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=SCHEMA))
        print(f"[epl_odds] Created table {DATASET}.{TABLE}")
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
    print(f"[epl_odds] Added {len(new_fields)} new column(s): {[f.name for f in new_fields]}")


def _truncate_table(client: bigquery.Client) -> None:
    table_id = _full_table_id(client)
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    print(f"[epl_odds] Truncated {DATASET}.{TABLE}")


def _insert_rows(
    client: bigquery.Client,
    rows: List[Dict[str, Any]],
    *,
    chunk_size: int = 500,
) -> int:
    if not rows:
        return 0
    import tempfile, pathlib, json as _json
    table_id = _full_table_id(client)

    # Write rows as newline-delimited JSON and load via load_table_from_file
    # This avoids the streaming insert HTTPS endpoint that hits SSL errors
    tmp = pathlib.Path(tempfile.mktemp(suffix=".ndjson"))
    try:
        with tmp.open("w") as f:
            for row in rows:
                f.write(_json.dumps(row, default=str) + "\n")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        with tmp.open("rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()  # wait for completion

        if job.errors:
            raise RuntimeError(f"Load job errors: {job.errors[:3]}")

        print(f"[epl_odds] Load job complete: {job.output_rows} rows loaded")
        return job.output_rows
    finally:
        tmp.unlink(missing_ok=True)



# ── Normalisation helpers ─────────────────────────────────────────────────────


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
    key  = (outcome_key  or "").strip().lower()
    name = (outcome_name or "").strip().lower()
    home = (home_team    or "").strip().lower()
    away = (away_team    or "").strip().lower()

    if key in {"o1", "1", "home"}:      return "home"
    if key in {"o2", "2", "away"}:      return "away"
    if key in {"o3", "3", "draw", "x"}: return "draw"
    if name in {"home", "1"}:           return "home"
    if name in {"away", "2"}:           return "away"
    if name in {"draw", "x", "tie"}:    return "draw"
    if home and name == home:           return "home"
    if away and name == away:           return "away"
    return None


def _match_base_row(
    match: Dict[str, Any],
    ingested_at: str,
    scraped_date: str,
) -> Dict[str, Any]:
    return {
        "ingested_at":  ingested_at,
        "scraped_date": scraped_date,
        "match_id":     _safe_int(match.get("match_id")),
        "match_key":    _safe_int(match.get("match_key")),
        "sport":        match.get("sport"),
        "date_utc":     match.get("date_utc"),
        "home_team":    match.get("home_team"),
        "away_team":    match.get("away_team"),
        "home_team_id": _safe_int(match.get("home_team_id")),
        "away_team_id": _safe_int(match.get("away_team_id")),
        "inplay":       bool(match.get("inplay", False)),
        "league_id":    _safe_int(match.get("league_id")),
        "round_name":   match.get("round_name"),
    }


def _normalize_rich_market_rows(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    market_rows = match.get("market_rows") or []
    normalized: List[Dict[str, Any]] = []
    if not isinstance(market_rows, list):
        return normalized
    for row in market_rows:
        if not isinstance(row, dict):
            continue
        normalized.append({
            "market_group_id":    _safe_int(row.get("market_group_id")),
            "market_group_name":  row.get("market_group_name"),
            "market":             row.get("market"),
            "period_id":          _safe_int(row.get("period_id")),
            "period_name":        row.get("period_name"),
            "bookie_id":          _safe_int(row.get("bookie_id")),
            "bookie":             row.get("bookie"),
            "bookie_slug":        row.get("bookie_slug"),
            "outcome_key":        row.get("outcome_key"),
            "outcome_name":       row.get("outcome_name"),
            "outcome_side":       row.get("outcome_side") or _infer_outcome_side(
                                      row.get("outcome_key"), row.get("outcome_name"),
                                      match.get("home_team"), match.get("away_team")),
            "outcome_order":      _safe_int(row.get("outcome_order")),
            "odds_decimal":       _safe_float(row.get("odds_decimal")),
            "odds_american":      _safe_int(row.get("odds_american")),
            "odds_status":        _safe_int(row.get("odds_status")),
            "odds_direction":     _safe_int(row.get("odds_direction")),
            "line_value":         row.get("line_value"),
            "home_handicap":      row.get("home_handicap"),
            "away_handicap":      row.get("away_handicap"),
            "handicap_label":     row.get("handicap_label"),
            "winning_side":       row.get("winning_side"),
            "bet_link":           row.get("bet_link"),
            "home_odds_decimal":  _safe_float(row.get("home_odds_decimal")),
            "away_odds_decimal":  _safe_float(row.get("away_odds_decimal")),
            "home_odds_american": _safe_int(row.get("home_odds_american")),
            "away_odds_american": _safe_int(row.get("away_odds_american")),
            "market_json":        row.get("market_json"),
            "outcome_json":       row.get("outcome_json"),
        })
    return normalized


def _normalize_legacy_markets(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    markets = match.get("markets") or {}
    if not isinstance(markets, dict):
        return normalized
    for market_name, mp in markets.items():
        if not isinstance(mp, dict):
            continue
        normalized.append({
            "market_group_id":    _safe_int(mp.get("market_group_id")),
            "market_group_name":  mp.get("market_group_name"),
            "market":             mp.get("market") or market_name,
            "period_id":          _safe_int(mp.get("period_id")),
            "period_name":        mp.get("period_name"),
            "bookie_id":          _safe_int(mp.get("bookie_id")),
            "bookie":             mp.get("bookie"),
            "bookie_slug":        mp.get("bookie_slug"),
            "outcome_key":        None,
            "outcome_name":       None,
            "outcome_side":       None,
            "outcome_order":      None,
            "odds_decimal":       None,
            "odds_american":      None,
            "odds_status":        _safe_int(mp.get("odds_status") or mp.get("status")),
            "odds_direction":     _safe_int(mp.get("odds_direction")),
            "line_value":         mp.get("line_value"),
            "home_handicap":      mp.get("home_handicap"),
            "away_handicap":      mp.get("away_handicap"),
            "handicap_label":     mp.get("handicap_label"),
            "winning_side":       mp.get("winning_side"),
            "bet_link":           mp.get("bet_link"),
            "home_odds_decimal":  _safe_float(mp.get("home_odds_decimal")),
            "away_odds_decimal":  _safe_float(mp.get("away_odds_decimal")),
            "home_odds_american": _safe_int(mp.get("home_odds_american")),
            "away_odds_american": _safe_int(mp.get("away_odds_american")),
            "market_json":        mp,
            "outcome_json":       None,
        })
    return normalized


def _normalized_market_rows(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    rich   = _normalize_rich_market_rows(match)
    legacy = _normalize_legacy_markets(match)
    if rich:
        covered = {r.get("market") for r in rich if r.get("market")}
        return rich + [r for r in legacy if r.get("market") not in covered]
    return legacy


def _to_bq_rows(
    matches: List[Dict[str, Any]],
    ingested_at: str,
    scraped_date: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    _null_market: Dict[str, Any] = {
        "market_group_id": None, "market_group_name": None, "market": None,
        "period_id": None, "period_name": None,
        "bookie_id": None, "bookie": None, "bookie_slug": None,
        "outcome_key": None, "outcome_name": None, "outcome_side": None,
        "outcome_order": None, "odds_decimal": None, "odds_american": None,
        "odds_status": None, "odds_direction": None,
        "line_value": None, "home_handicap": None, "away_handicap": None,
        "handicap_label": None, "winning_side": None, "bet_link": None,
        "home_odds_decimal": None, "away_odds_decimal": None,
        "home_odds_american": None, "away_odds_american": None,
        "market_json": None, "outcome_json": None,
    }

    for match in matches:
        if match.get("matchstatus") == 4 or match.get("special_status") == "Postponed":
            print(f"[epl_odds] Skipping postponed match {match.get('match_id')}")
            continue

        base        = _match_base_row(match, ingested_at, scraped_date)
        market_rows = _normalized_market_rows(match)

        if not market_rows:
            rows.append({**base, **_null_market})
            continue

        for mr in market_rows:
            rows.append({
                **base,
                "market_group_id":    mr.get("market_group_id"),
                "market_group_name":  mr.get("market_group_name"),
                "market":             mr.get("market"),
                "period_id":          mr.get("period_id"),
                "period_name":        mr.get("period_name"),
                "bookie_id":          mr.get("bookie_id"),
                "bookie":             mr.get("bookie"),
                "bookie_slug":        mr.get("bookie_slug"),
                "outcome_key":        mr.get("outcome_key"),
                "outcome_name":       mr.get("outcome_name"),
                "outcome_side":       mr.get("outcome_side"),
                "outcome_order":      mr.get("outcome_order"),
                "odds_decimal":       mr.get("odds_decimal"),
                "odds_american":      mr.get("odds_american"),
                "odds_status":        mr.get("odds_status"),
                "odds_direction":     mr.get("odds_direction"),
                "line_value":         mr.get("line_value"),
                "home_handicap":      mr.get("home_handicap"),
                "away_handicap":      mr.get("away_handicap"),
                "handicap_label":     mr.get("handicap_label"),
                "winning_side":       mr.get("winning_side"),
                "bet_link":           mr.get("bet_link"),
                "home_odds_decimal":  mr.get("home_odds_decimal"),
                "away_odds_decimal":  mr.get("away_odds_decimal"),
                "home_odds_american": mr.get("home_odds_american"),
                "away_odds_american": mr.get("away_odds_american"),
                "market_json":  json.dumps(_jsonable(mr.get("market_json")))  if mr.get("market_json")  else None,
                "outcome_json": json.dumps(_jsonable(mr.get("outcome_json"))) if mr.get("outcome_json") else None,
            })

    return [r for r in rows if r.get("match_id") is not None]


# ── Main ingest ───────────────────────────────────────────────────────────────


def ingest_epl_odds(
    url: Optional[str] = None,
    *,
    dry_run: bool = False,
    today_only: bool = False,
    scrape_only: bool = False,
) -> Dict[str, Any]:
    target_url   = url or ODDSPEDIA_URL
    now          = datetime.now(timezone.utc)
    ingested_at  = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    scraped_date = now.strftime("%Y-%m-%d")

    print("=" * 60)
    print("[epl_odds] Starting Oddspedia EPL odds ingest")
    print(f"[epl_odds] URL         : {target_url}")
    print(f"[epl_odds] Ingested at : {ingested_at}")
    if dry_run:
        print("[epl_odds] Mode        : DRY RUN (no BigQuery write)")
    elif scrape_only:
        print("[epl_odds] Mode        : SCRAPE ONLY (rows saved to file)")
    else:
        print(f"[epl_odds] Destination : {DATASET}.{TABLE}")
    print("=" * 60)

    client_scraper = OddspediaClient()
    print(f"[epl_odds] Fetching {target_url} …")
    matches = client_scraper.scrape(
        target_url,
        league_category="england",
        league_slug="premier-league",
        season_id=130281,
        sport="soccer",
    )

    import tempfile, pathlib
    tmp = pathlib.Path(tempfile.mktemp(suffix=".json"))
    tmp.write_text(json.dumps(matches, default=str))
    print(f"[epl_odds] Browser closed. Saved {len(matches)} matches to {tmp}")
    matches = json.loads(tmp.read_text())
    tmp.unlink()

    if matches:
        print(f"[epl_odds] DEBUG first match keys: {list(matches[0].keys())}")
    print(f"[epl_odds] Scraped {len(matches)} matches")

    if today_only and any(m.get("date_utc") for m in matches):
        matches = [m for m in matches if (m.get("date_utc") or "").startswith(scraped_date)]
        print(f"[epl_odds] After today filter : {len(matches)} matches")
    else:
        print(f"[epl_odds] Gameweek mode — keeping all {len(matches)} matches")

    rows = _to_bq_rows(matches, ingested_at, scraped_date)
    print(f"[epl_odds] Prepared {len(rows)} rows from {len(matches)} matches")

    if dry_run or scrape_only:
        pathlib.Path(ROWS_TMP_PATH).write_text(json.dumps(rows, default=str))
        print(f"[epl_odds] Saved {len(rows)} rows to {ROWS_TMP_PATH}")
        if dry_run:
            print(json.dumps(rows[:25], indent=2, default=str))
        return {
            "url": target_url,
            "ingested_at": ingested_at,
            "matches_found": len(matches),
            "rows_prepared": len(rows),
            "rows_written": 0,
            "scrape_only": scrape_only,
            "dry_run": dry_run,
            "errors": [],
        }

    return {
        "url": target_url,
        "ingested_at": ingested_at,
        "matches_found": len(matches),
        "rows_prepared": len(rows),
        "rows_written": 0,
        "errors": [],
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Oddspedia EPL odds and load into BigQuery (oddspedia.epl_odds)."
    )
    parser.add_argument("--url", default=None,
        help=f"Oddspedia page URL. Defaults to ODDSPEDIA_EPL_URL or {DEFAULT_URL}.")
    parser.add_argument("--dry-run", action="store_true",
        help="Print prepared rows as JSON without writing to BigQuery.")
    parser.add_argument("--today", action="store_true", default=False,
        help="Restrict to today's UTC date only (default: full gameweek).")
    parser.add_argument("--all-dates", action="store_true",
        help="Alias for default gameweek behaviour (kept for parity with MLS script).")
    parser.add_argument("--scrape-only", action="store_true",
        help=f"Scrape and save rows to {ROWS_TMP_PATH}, skip BQ insert.")
    parser.add_argument("--insert-only", action="store_true",
        help=f"Read rows from {ROWS_TMP_PATH} and insert into BigQuery.")

    args = parser.parse_args()

    if args.insert_only:
        rows = json.loads(Path(ROWS_TMP_PATH).read_text())
        print(f"[epl_odds] Loaded {len(rows)} rows from {ROWS_TMP_PATH}")
        bq = _bq_client()
        _ensure_dataset(bq)
        _ensure_table(bq)
        _add_missing_columns(bq)
        _truncate_table(bq)
        written = _insert_rows(bq, rows)
        print(f"[epl_odds] Done — {written} rows written")
        print(json.dumps({"rows_written": written}, indent=2))
    else:
        today_only = args.today and not args.all_dates
        result = ingest_epl_odds(
            url=args.url,
            dry_run=args.dry_run,
            today_only=today_only,
            scrape_only=args.scrape_only,
        )
        print(json.dumps(result, indent=2, default=str))
