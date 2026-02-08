"""
Small test script to pull NBA alternate totals (over/under) from The Odds API
and store raw payloads in BigQuery.

Usage:
  THE_ODDS_API_KEY=... python -m LiveOdds.theoddsapi_alt_points_test
  python -m LiveOdds.theoddsapi_alt_points_test --api-key ... --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

import requests

from LiveOdds.live_odds_common import get_bq_client

THEODDSAPI_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "basketball_nba"
DEFAULT_MARKETS = "alternate_totals"
DEFAULT_REGIONS = "us"
DEFAULT_ODDS_FORMAT = "american"
DEFAULT_DATE_FORMAT = "iso"
TIMEOUT_SEC = 30
NY_TZ = ZoneInfo("America/New_York")


def _resolve_api_key(arg_key: str | None) -> str:
    key = arg_key or os.getenv("THE_ODDS_API_KEY") or os.getenv("ODDS_API_KEY")
    if not key:
        raise RuntimeError("Missing THE_ODDS_API_KEY (or pass --api-key).")
    return key


def _resolve_table_id(project: str | None, dataset: str, table: str, table_id: str | None) -> str:
    if table_id:
        return table_id
    if project:
        return f"{project}.{dataset}.{table}"
    return f"{dataset}.{table}"


def _ny_date_window(date_str: str | None) -> tuple[str, str, str]:
    if date_str:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        target_date = datetime.now(NY_TZ).date()

    start_ny = datetime.combine(target_date, time(0, 0, 0), tzinfo=NY_TZ)
    end_ny = datetime.combine(target_date, time(23, 59, 59), tzinfo=NY_TZ)

    start_utc = start_ny.astimezone(timezone.utc).isoformat()
    end_utc = end_ny.astimezone(timezone.utc).isoformat()
    return target_date.isoformat(), start_utc, end_utc


def _fetch_alt_totals(
    api_key: str,
    markets: str,
    regions: str,
    commence_from: str,
    commence_to: str,
    bookmakers: str | None,
) -> list[dict]:
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": DEFAULT_ODDS_FORMAT,
        "dateFormat": DEFAULT_DATE_FORMAT,
        "commenceTimeFrom": commence_from,
        "commenceTimeTo": commence_to,
    }
    if bookmakers:
        params["bookmakers"] = bookmakers

    url = f"{THEODDSAPI_BASE}/sports/{SPORT_KEY}/odds/"
    resp = requests.get(url, params=params, timeout=TIMEOUT_SEC)
    if resp.status_code >= 400:
        raise RuntimeError(f"The Odds API error {resp.status_code}: {resp.text}")
    resp.raise_for_status()

    remaining = resp.headers.get("x-requests-remaining")
    used = resp.headers.get("x-requests-used")
    if remaining or used:
        print(f"[THEODDSAPI] Requests used={used} remaining={remaining}")

    payload = resp.json()
    if not isinstance(payload, list):
        raise RuntimeError("Unexpected The Odds API response shape.")
    return payload


def _build_rows(
    events: list[dict],
    snapshot_ts: str,
    request_date: str,
    markets: str,
    regions: str,
) -> list[dict]:
    rows = []
    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue
        rows.append(
            {
                "snapshot_ts": snapshot_ts,
                "request_date": request_date,
                "event_id": event_id,
                "sport_key": event.get("sport_key"),
                "sport_title": event.get("sport_title"),
                "commence_time": event.get("commence_time"),
                "home_team": event.get("home_team"),
                "away_team": event.get("away_team"),
                "regions": regions,
                "markets": markets,
                "bookmaker_count": len(event.get("bookmakers", []) or []),
                "payload": json.dumps(event),
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch NBA alternate totals from The Odds API into BigQuery."
    )
    parser.add_argument("--api-key", help="The Odds API key (or set THE_ODDS_API_KEY).")
    parser.add_argument(
        "--date",
        help="Target date in YYYY-MM-DD (default: today in America/New_York).",
    )
    parser.add_argument("--markets", default=DEFAULT_MARKETS)
    parser.add_argument("--regions", default=DEFAULT_REGIONS)
    parser.add_argument("--bookmakers", help="Comma-separated bookmakers (optional).")
    parser.add_argument("--dataset", default="odds_raw")
    parser.add_argument("--table", default="nba_alt_points")
    parser.add_argument("--table-id", help="Full BigQuery table id override.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = _resolve_api_key(args.api_key)
    request_date, commence_from, commence_to = _ny_date_window(args.date)

    print(
        "[THEODDSAPI] Fetching NBA alternate totals",
        {
            "date": request_date,
            "markets": args.markets,
            "regions": args.regions,
            "commence_from": commence_from,
            "commence_to": commence_to,
        },
    )

    events = _fetch_alt_totals(
        api_key=api_key,
        markets=args.markets,
        regions=args.regions,
        commence_from=commence_from,
        commence_to=commence_to,
        bookmakers=args.bookmakers,
    )

    snapshot_ts = datetime.now(timezone.utc).isoformat()
    rows = _build_rows(
        events=events,
        snapshot_ts=snapshot_ts,
        request_date=request_date,
        markets=args.markets,
        regions=args.regions,
    )

    print(f"[THEODDSAPI] Events fetched: {len(events)} rows prepared: {len(rows)}")

    if args.dry_run:
        sample = rows[0] if rows else None
        print("[THEODDSAPI] DRY RUN; sample row:", sample)
        return

    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    table_id = _resolve_table_id(project, args.dataset, args.table, args.table_id)
    print(f"[THEODDSAPI] Inserting into {table_id}")

    if rows:
        client = get_bq_client()
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

    print("[THEODDSAPI] Insert complete.")


if __name__ == "__main__":
    main()
