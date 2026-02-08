"""
Small test script to pull NBA alternate player points (over/under) from
The Odds API and store raw payloads in BigQuery.

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
DEFAULT_MARKETS = "player_points_alternate"
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

    # The Odds API expects Zulu format without offset, e.g. 2020-11-24T16:05:00Z
    start_utc = start_ny.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_ny.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return target_date.isoformat(), start_utc, end_utc


def _fetch_events(
    api_key: str,
    commence_from: str,
    commence_to: str,
) -> list[dict]:
    params = {
        "apiKey": api_key,
        "dateFormat": DEFAULT_DATE_FORMAT,
        "commenceTimeFrom": commence_from,
        "commenceTimeTo": commence_to,
    }
    url = f"{THEODDSAPI_BASE}/sports/{SPORT_KEY}/events"
    resp = requests.get(url, params=params, timeout=TIMEOUT_SEC)
    if resp.status_code >= 400:
        raise RuntimeError(f"The Odds API error {resp.status_code}: {resp.text}")
    resp.raise_for_status()

    payload = resp.json()
    if not isinstance(payload, list):
        raise RuntimeError("Unexpected The Odds API events response shape.")
    return payload


def _fetch_event_player_points(
    api_key: str,
    event_id: str,
    markets: str,
    regions: str,
    bookmakers: str | None,
) -> dict:
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": DEFAULT_ODDS_FORMAT,
        "dateFormat": DEFAULT_DATE_FORMAT,
    }
    if bookmakers:
        params["bookmakers"] = bookmakers

    url = f"{THEODDSAPI_BASE}/sports/{SPORT_KEY}/events/{event_id}/odds"
    resp = requests.get(url, params=params, timeout=TIMEOUT_SEC)
    if resp.status_code >= 400:
        raise RuntimeError(f"The Odds API error {resp.status_code}: {resp.text}")
    resp.raise_for_status()

    remaining = resp.headers.get("x-requests-remaining")
    used = resp.headers.get("x-requests-used")
    if remaining or used:
        print(f"[THEODDSAPI] Requests used={used} remaining={remaining}")

    payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected The Odds API event odds response shape.")
    return payload


def _build_rows(
    event_payloads: list[dict],
    snapshot_ts: str,
    request_date: str,
    markets: str,
    regions: str,
) -> list[dict]:
    rows = []
    for event in event_payloads:
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


def run_alt_player_points_ingest(
    api_key: str | None = None,
    date: str | None = None,
    markets: str = DEFAULT_MARKETS,
    regions: str = DEFAULT_REGIONS,
    bookmakers: str | None = None,
    event_id: str | None = None,
    max_events: int | None = None,
    dataset: str = "odds_raw",
    table: str = "nba_alt_player_points",
    table_id: str | None = None,
    dry_run: bool = False,
    include_sample: bool = False,
) -> dict:
    resolved_key = _resolve_api_key(api_key)
    request_date, commence_from, commence_to = _ny_date_window(date)

    print(
        "[THEODDSAPI] Fetching NBA alternate player points",
        {
            "date": request_date,
            "markets": markets,
            "regions": regions,
            "commence_from": commence_from,
            "commence_to": commence_to,
        },
    )

    event_payloads: list[dict] = []
    events_found: int | None = None
    if event_id:
        event_payloads.append(
            _fetch_event_player_points(
                api_key=resolved_key,
                event_id=event_id,
                markets=markets,
                regions=regions,
                bookmakers=bookmakers,
            )
        )
        events_found = 1
    else:
        events = _fetch_events(
            api_key=resolved_key,
            commence_from=commence_from,
            commence_to=commence_to,
        )
        if max_events is not None:
            events = events[: max(max_events, 0)]

        events_found = len(events)
        print(f"[THEODDSAPI] Events found: {events_found}")
        for idx, event in enumerate(events, start=1):
            eid = event.get("id")
            if not eid:
                continue
            print(f"[THEODDSAPI] Fetching event {idx}/{len(events)}: {eid}")
            event_payloads.append(
                _fetch_event_player_points(
                    api_key=resolved_key,
                    event_id=eid,
                    markets=markets,
                    regions=regions,
                    bookmakers=bookmakers,
                )
            )

    snapshot_ts = datetime.now(timezone.utc).isoformat()
    rows = _build_rows(
        event_payloads=event_payloads,
        snapshot_ts=snapshot_ts,
        request_date=request_date,
        markets=markets,
        regions=regions,
    )

    print(f"[THEODDSAPI] Event payloads: {len(event_payloads)} rows prepared: {len(rows)}")

    result = {
        "status": "OK",
        "request_date": request_date,
        "markets": markets,
        "regions": regions,
        "events_found": events_found,
        "event_payloads": len(event_payloads),
        "rows_prepared": len(rows),
        "dry_run": dry_run,
    }

    sample = rows[0] if rows else None
    if dry_run:
        if include_sample:
            result["sample_row"] = sample
        return result

    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    resolved_table_id = _resolve_table_id(project, dataset, table, table_id)
    print(f"[THEODDSAPI] Inserting into {resolved_table_id}")

    if rows:
        client = get_bq_client()
        errors = client.insert_rows_json(resolved_table_id, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

    result["table_id"] = resolved_table_id
    result["rows_inserted"] = len(rows)
    print("[THEODDSAPI] Insert complete.")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch NBA alternate player points from The Odds API into BigQuery."
    )
    parser.add_argument("--api-key", help="The Odds API key (or set THE_ODDS_API_KEY).")
    parser.add_argument(
        "--date",
        help="Target date in YYYY-MM-DD (default: today in America/New_York).",
    )
    parser.add_argument("--markets", default=DEFAULT_MARKETS)
    parser.add_argument("--regions", default=DEFAULT_REGIONS)
    parser.add_argument("--bookmakers", help="Comma-separated bookmakers (optional).")
    parser.add_argument("--event-id", help="Fetch odds for a single event id only.")
    parser.add_argument(
        "--max-events",
        type=int,
        help="Limit number of events fetched (helps conserve requests).",
    )
    parser.add_argument("--dataset", default="odds_raw")
    parser.add_argument("--table", default="nba_alt_player_points")
    parser.add_argument("--table-id", help="Full BigQuery table id override.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = run_alt_player_points_ingest(
        api_key=args.api_key,
        date=args.date,
        markets=args.markets,
        regions=args.regions,
        bookmakers=args.bookmakers,
        event_id=args.event_id,
        max_events=args.max_events,
        dataset=args.dataset,
        table=args.table,
        table_id=args.table_id,
        dry_run=args.dry_run,
        include_sample=args.dry_run,
    )

    if args.dry_run and result.get("sample_row") is not None:
        print("[THEODDSAPI] DRY RUN; sample row:", result["sample_row"])


if __name__ == "__main__":
    main()
