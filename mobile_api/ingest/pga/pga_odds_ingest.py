"""PGA Tour website odds ingest.

Loads all 5 odds markets from the PGA Tour website into pga_data.website_odds.
The table is TRUNCATED before each load so only current odds are stored.

Markets fetched
---------------
2032  To Win         GraphQL (orchestrator.pgatour.com) — gzip-compressed payload
2033  Finish         REST    (data-api.pgatour.com)
2036  Group Props    REST
2039  Matchup Props  REST
2085  3 Ball         REST

Usage
-----
    # Auto-detect active tournament from the PGA schedule:
    python -m mobile_api.ingest.pga.pga_odds_ingest

    # Target a specific tournament:
    python -m mobile_api.ingest.pga.pga_odds_ingest --tournament-id R2026009

Environment variables
---------------------
PGA_DATASET             BigQuery dataset name   (default: pga_data)
PGA_DATASET_LOCATION    BigQuery location       (default: US)
PGA_ODDS_TABLE          Table name              (default: website_odds)
PGA_TOURNAMENT_ID       Tournament ID override  (overridden by --tournament-id flag)
PGA_GRAPHQL_API_KEY     PGA GraphQL API key     (default: public frontend key)
GCP_PROJECT / GOOGLE_CLOUD_PROJECT
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

# ── Configuration ─────────────────────────────────────────────────────────────

DATASET = os.getenv("PGA_DATASET", "pga_data")
DATASET_LOCATION = os.getenv("PGA_DATASET_LOCATION", "US")
ODDS_TABLE = os.getenv("PGA_ODDS_TABLE", "website_odds")

REST_BASE_URL = "https://data-api.pgatour.com/odds/tournament"
GRAPHQL_URL = "https://orchestrator.pgatour.com/graphql"

# Public key used by the PGA Tour frontend — safe to include as default.
PGA_GRAPHQL_API_KEY = os.getenv("PGA_GRAPHQL_API_KEY", "da2-gsrx5bibzbb4njvhl7t37wqyl4")

# REST market IDs (Finish, Group Props, Matchup Props, 3 Ball)
REST_MARKET_IDS = [2033, 2036, 2039, 2085]
TO_WIN_MARKET_ID = 2032

# ── BigQuery schema ───────────────────────────────────────────────────────────

SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("ingested_at",         "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tournament_id",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("market_id",            "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("market_name",          "STRING"),
    bigquery.SchemaField("market_display_name",  "STRING"),
    bigquery.SchemaField("market_type",          "STRING"),
    bigquery.SchemaField("betting_provider",     "STRING"),
    bigquery.SchemaField("sub_market_name",      "STRING"),
    bigquery.SchemaField("group_type",           "STRING"),
    bigquery.SchemaField("group_index",          "INT64"),
    bigquery.SchemaField("player_id",            "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("display_name",         "STRING"),
    bigquery.SchemaField("short_name",           "STRING"),
    bigquery.SchemaField("odds_value",           "STRING"),
    bigquery.SchemaField("odds_direction",       "STRING"),
    bigquery.SchemaField("odds_sort",            "FLOAT64"),
    bigquery.SchemaField("option_id",            "STRING"),
    bigquery.SchemaField("entity_id",            "STRING"),
]

# ── BigQuery helpers ──────────────────────────────────────────────────────────

def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _table_id(client: bigquery.Client, table: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{client.project}.{table}"
    return f"{client.project}.{DATASET}.{table}"


def _ensure_dataset(client: bigquery.Client) -> None:
    dataset_ref = f"{client.project}.{DATASET}"
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = DATASET_LOCATION
        client.create_dataset(ds)
    except Conflict:
        pass


def _ensure_table(client: bigquery.Client) -> None:
    tid = _table_id(client, ODDS_TABLE)
    try:
        client.get_table(tid)
    except NotFound:
        client.create_table(bigquery.Table(tid, schema=SCHEMA))
    except Conflict:
        pass


def _truncate_table(client: bigquery.Client) -> None:
    tid = _table_id(client, ODDS_TABLE)
    client.query(f"TRUNCATE TABLE `{tid}`").result()


def _insert_rows(client: bigquery.Client, rows: List[Dict[str, Any]], *, chunk_size: int = 500) -> int:
    if not rows:
        return 0
    tid = _table_id(client, ODDS_TABLE)
    written = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        errors = client.insert_rows_json(tid, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
        written += len(chunk)
        time.sleep(0.05)
    return written

# ── Fetchers ──────────────────────────────────────────────────────────────────

def _rest_headers() -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.pgatour.com/",
        "Accept": "application/json",
    }


def fetch_rest_market(tournament_id: str, market_id: int) -> Optional[Dict[str, Any]]:
    """Fetch one REST odds market. Returns the parsed JSON or None on failure."""
    url = f"{REST_BASE_URL}/{tournament_id}/{market_id}"
    try:
        resp = requests.get(url, headers=_rest_headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[odds] WARN REST market {market_id} failed: {exc}")
        return None


def fetch_to_win(tournament_id: str) -> Optional[Dict[str, Any]]:
    """Fetch To Win odds via GraphQL and decode the gzip-compressed payload.

    Returns the decoded inner JSON (players list) or None on failure.
    """
    query = (
        "query oddsToWinCompressed($tournamentId: ID!) "
        "{ oddsToWinCompressed(oddsToWinId: $tournamentId) { id payload } }"
    )
    payload = {
        "operationName": "oddsToWinCompressed",
        "query": query,
        "variables": {"tournamentId": tournament_id},
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/graphql-response+json, application/json",
        "Origin": "https://www.pgatour.com",
        "Referer": "https://www.pgatour.com/",
        "x-api-key": PGA_GRAPHQL_API_KEY,
        "x-pgat-platform": "web",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
    }
    try:
        resp = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        compressed_b64 = data["data"]["oddsToWinCompressed"]["payload"]
        decoded = gzip.decompress(base64.b64decode(compressed_b64))
        return json.loads(decoded)
    except Exception as exc:
        print(f"[odds] WARN To Win (GraphQL) failed: {exc}")
        return None

# ── Flatteners ────────────────────────────────────────────────────────────────

def _flatten_rest_market(
    response: Dict[str, Any],
    tournament_id: str,
    ingested_at: str,
) -> List[Dict[str, Any]]:
    """Flatten one REST market response into a list of table rows.

    One row is produced per player per odds entry.
    Players within the same matchup/3-ball group share the same group_index.
    """
    rows: List[Dict[str, Any]] = []

    market_id           = response.get("marketId")
    market_name         = response.get("market")
    market_display_name = response.get("marketDisplayName")
    market_type         = response.get("marketType")
    betting_provider    = response.get("bettingProvider")

    for sub_market in response.get("subMarkets", []):
        sub_market_name = sub_market.get("subMarketName")

        for odds_data_group in sub_market.get("oddsDataGroup", []):
            for group_index, odds_item in enumerate(odds_data_group.get("oddsData", [])):
                group_type = odds_item.get("type")

                for group_entry in odds_item.get("group", []):
                    odds_value    = group_entry.get("oddsValue")
                    odds_direction = group_entry.get("oddDirection")
                    option_id     = group_entry.get("optionId")
                    entity_id     = group_entry.get("entityId")

                    for player in group_entry.get("players", []):
                        rows.append({
                            "ingested_at":          ingested_at,
                            "tournament_id":        tournament_id,
                            "market_id":            market_id,
                            "market_name":          market_name,
                            "market_display_name":  market_display_name,
                            "market_type":          market_type,
                            "betting_provider":     betting_provider,
                            "sub_market_name":      sub_market_name,
                            "group_type":           group_type,
                            "group_index":          group_index,
                            "player_id":            player.get("playerId"),
                            "display_name":         player.get("displayName"),
                            "short_name":           player.get("shortName"),
                            "odds_value":           odds_value,
                            "odds_direction":       odds_direction,
                            "odds_sort":            None,
                            "option_id":            option_id,
                            "entity_id":            entity_id,
                        })
    return rows


def _flatten_to_win(
    decoded: Dict[str, Any],
    tournament_id: str,
    ingested_at: str,
) -> List[Dict[str, Any]]:
    """Flatten the decoded To Win payload into a list of table rows.

    Note: the compressed payload does not include display_name / short_name.
    Join against pga_data.website_active_players on player_id to get names.
    """
    rows: List[Dict[str, Any]] = []

    if not decoded.get("oddsEnabled", True):
        print("[odds] INFO To Win: oddsEnabled=False, skipping")
        return rows

    for player in decoded.get("players", []):
        rows.append({
            "ingested_at":          ingested_at,
            "tournament_id":        tournament_id,
            "market_id":            TO_WIN_MARKET_ID,
            "market_name":          "Outright Winner",
            "market_display_name":  "To Win",
            "market_type":          "OUTRIGHT_WINNER",
            "betting_provider":     "fanduel",
            "sub_market_name":      None,
            "group_type":           "SINGLE",
            "group_index":          0,
            "player_id":            player.get("playerId"),
            "display_name":         None,   # not in compressed payload
            "short_name":           None,   # not in compressed payload
            "odds_value":           player.get("odds"),
            "odds_direction":       player.get("oddsDirection"),
            "odds_sort":            player.get("oddsSort"),
            "option_id":            player.get("optionId"),
            "entity_id":            player.get("playerId"),
        })
    return rows

# ── Tournament ID resolution ──────────────────────────────────────────────────

def _auto_detect_tournament_id(lookahead_days: int = 7) -> str:
    """Detect the best tournament ID to load odds for.

    Priority order:
      1. In-progress  — "upcoming" bucket with start_date <= today
      2. Starting soon — "upcoming" bucket with start_date within the next
                         ``lookahead_days`` days (catches Mon/Tue before a
                         Thursday-start event whose odds are already posted)
      3. Most recently completed — fallback when nothing is active or imminent

    ``lookahead_days=7`` comfortably covers Mon–Wed before any Thursday start.
    """
    from .pga_schedule import fetch_schedule, schedule_to_records

    current_year = datetime.now(timezone.utc).year
    print(f"[odds] Auto-detecting tournament ID for {current_year} season …")
    tournaments = fetch_schedule(tour_code="R", year=str(current_year))
    schedule_rows = schedule_to_records(tournaments)

    today = datetime.now(timezone.utc).date()
    lookahead_cutoff = today + __import__("datetime").timedelta(days=lookahead_days)

    in_progress: list[tuple] = []
    starting_soon: list[tuple] = []
    completed: list[tuple] = []

    for row in schedule_rows:
        tid = str(row.get("tournament_id") or "").strip()
        if not tid:
            continue
        raw_date = row.get("start_date")
        if not raw_date:
            continue
        try:
            from datetime import date as _date
            start = _date.fromisoformat(str(raw_date)[:10])
        except (ValueError, TypeError):
            continue

        bucket = str(row.get("bucket") or "").strip().lower()
        if bucket == "upcoming":
            if start <= today:
                in_progress.append((start, tid))
            elif start <= lookahead_cutoff:
                starting_soon.append((start, tid))
        elif bucket == "completed":
            completed.append((start, tid))

    if in_progress:
        chosen = max(in_progress, key=lambda x: x[0])[1]
        print(f"[odds] Detected in-progress tournament: {chosen}")
        return chosen

    if starting_soon:
        # Pick the soonest upcoming event so odds are for the right week
        chosen = min(starting_soon, key=lambda x: x[0])[1]
        print(f"[odds] No active tournament — detected upcoming tournament starting soon: {chosen}")
        return chosen

    if completed:
        chosen = max(completed, key=lambda x: x[0])[1]
        print(f"[odds] No active or upcoming tournament — falling back to most recently completed: {chosen}")
        return chosen

    raise RuntimeError(
        "Could not auto-detect a tournament. "
        "Pass --tournament-id explicitly or set PGA_TOURNAMENT_ID."
    )


def _resolve_tournament_id(override: Optional[str] = None) -> str:
    if override:
        return override
    from_env = os.getenv("PGA_TOURNAMENT_ID", "").strip()
    if from_env:
        return from_env
    return _auto_detect_tournament_id()

# ── Main ingest ───────────────────────────────────────────────────────────────

def ingest_pga_odds(tournament_id: Optional[str] = None) -> Dict[str, Any]:
    """Fetch all 5 PGA odds markets and load them into pga_data.website_odds.

    The table is truncated before writing so only current odds remain.

    Parameters
    ----------
    tournament_id:
        PGA Tour tournament ID (e.g. "R2026009"). If None, auto-detected
        from the schedule or read from the PGA_TOURNAMENT_ID env var.

    Returns
    -------
    Summary dict with row counts and any error messages.
    """
    tid = _resolve_tournament_id(tournament_id)
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    client = _bq_client()
    _ensure_dataset(client)
    _ensure_table(client)

    print(f"[odds] Truncating {DATASET}.{ODDS_TABLE} …")
    _truncate_table(client)

    summary: Dict[str, Any] = {
        "tournament_id": tid,
        "ingested_at": ingested_at,
        "rows_written": 0,
        "markets": {},
        "errors": [],
    }

    # ── To Win (GraphQL / compressed) ────────────────────────────────────────
    print(f"[odds] Fetching To Win (market {TO_WIN_MARKET_ID}) …")
    to_win_data = fetch_to_win(tid)
    if to_win_data is not None:
        rows = _flatten_to_win(to_win_data, tid, ingested_at)
        written = _insert_rows(client, rows)
        summary["markets"][TO_WIN_MARKET_ID] = {"rows": written}
        summary["rows_written"] += written
        print(f"[odds]   To Win: {written} rows written")
    else:
        msg = f"market {TO_WIN_MARKET_ID} (To Win) fetch failed"
        summary["errors"].append(msg)
        summary["markets"][TO_WIN_MARKET_ID] = {"rows": 0, "error": msg}

    # ── REST markets ──────────────────────────────────────────────────────────
    for market_id in REST_MARKET_IDS:
        print(f"[odds] Fetching REST market {market_id} …")
        response = fetch_rest_market(tid, market_id)
        if response is not None:
            rows = _flatten_rest_market(response, tid, ingested_at)
            written = _insert_rows(client, rows)
            market_name = response.get("marketDisplayName", str(market_id))
            summary["markets"][market_id] = {"name": market_name, "rows": written}
            summary["rows_written"] += written
            print(f"[odds]   {market_name} (market {market_id}): {written} rows written")
        else:
            msg = f"market {market_id} fetch failed"
            summary["errors"].append(msg)
            summary["markets"][market_id] = {"rows": 0, "error": msg}

    print(f"[odds] Done. Total rows written: {summary['rows_written']}")
    if summary["errors"]:
        print(f"[odds] Errors: {summary['errors']}")
    return summary


# ── CLI entry point ───────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest PGA Tour website odds into BigQuery (pga_data.website_odds)."
    )
    parser.add_argument(
        "--tournament-id",
        default=None,
        help=(
            "PGA Tour tournament ID to fetch odds for (e.g. R2026009). "
            "Defaults to auto-detection from the live schedule."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    result = ingest_pga_odds(tournament_id=args.tournament_id)
    print(json.dumps(result, indent=2, default=str))
