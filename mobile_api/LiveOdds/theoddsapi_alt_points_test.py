"""
Ingest NBA alternate player props from The Odds API.

Pulls alternate markets:
- Alternate Points
- Alternate Rebounds
- Alternate Assists
- Alternate Threes
- Alternate Points + Rebounds + Assists
- Alternate Points + Rebounds
- Alternate Points + Assists
- Alternate Rebounds + Assists

Intended to run on a schedule (6:00 AM and 3:45 PM ET).
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

import requests

from LiveOdds.live_odds_common import get_bq_client

THEODDSAPI_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "basketball_nba"
DEFAULT_MARKET_LIST = [
    "player_points_alternate",
    "player_rebounds_alternate",
    "player_assists_alternate",
    "player_threes_alternate",
    "player_points_rebounds_assists_alternate",
    "player_points_rebounds_alternate",
    "player_points_assists_alternate",
    "player_rebounds_assists_alternate",
]
DEFAULT_MARKETS = ",".join(DEFAULT_MARKET_LIST)
DEFAULT_REGIONS = "us"
DEFAULT_ODDS_FORMAT = "american"
DEFAULT_DATE_FORMAT = "iso"
TIMEOUT_SEC = 30
NY_TZ = ZoneInfo("America/New_York")

_KEY_SPLIT_RE = re.compile(r"[,\s;|]+")


def _parse_key_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    cleaned = raw.strip()
    if not cleaned:
        return []
    if cleaned.startswith("["):
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            keys = [str(item).strip() for item in parsed if str(item).strip()]
            return keys

    cleaned = cleaned.replace("\n", " ").replace("\r", " ")
    parts = []
    for part in _KEY_SPLIT_RE.split(cleaned):
        token = part.strip().strip('"').strip("'").strip("[]")
        if token:
            parts.append(token)
    return parts


def _resolve_api_keys(api_key: str | None, api_keys: str | None) -> list[str]:
    keys: list[str] = []
    if api_keys:
        keys = _parse_key_list(api_keys)
    if not keys and api_key:
        return [api_key]
    if not keys:
        env_keys = os.getenv("THE_ODDS_API_KEYS") or os.getenv("ODDS_API_KEYS")
        keys = _parse_key_list(env_keys)
    if not keys:
        key = os.getenv("THE_ODDS_API_KEY") or os.getenv("ODDS_API_KEY")
        if key:
            return [key]
    if not keys:
        raise RuntimeError("Missing THE_ODDS_API_KEYS/THE_ODDS_API_KEY (or pass --api-key).")
    return keys


def _mask_key(key: str) -> str:
    if len(key) <= 8:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def _is_invalid_key(resp: requests.Response) -> bool:
    if resp.status_code != 401:
        return False
    try:
        payload = resp.json()
    except ValueError:
        return False
    return payload.get("error_code") == "INVALID_KEY"


def _request_json_with_keys(
    api_keys: list[str],
    url: str,
    params: dict,
) -> tuple[dict | list, requests.structures.CaseInsensitiveDict, str]:
    if not api_keys:
        raise RuntimeError("No The Odds API keys available.")

    last_error = None
    attempts = len(api_keys)
    for _ in range(attempts):
        if not api_keys:
            break
        key = api_keys.pop(0)
        attempt_params = dict(params)
        attempt_params["apiKey"] = key
        resp = requests.get(url, params=attempt_params, timeout=TIMEOUT_SEC)
        if resp.status_code >= 400:
            if _is_invalid_key(resp):
                print(f"[THEODDSAPI] Invalid API key skipped: {_mask_key(key)}")
                last_error = f"{resp.status_code}: {resp.text}"
                continue
            api_keys.append(key)
            raise RuntimeError(f"The Odds API error {resp.status_code}: {resp.text}")

        api_keys.append(key)
        return resp.json(), resp.headers, key

    raise RuntimeError(f"All The Odds API keys failed. Last error: {last_error}")


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


def _normalize_csv(raw: str | None) -> str | None:
    if not raw:
        return None
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    return ",".join(parts) if parts else None


def _normalize_markets(markets: str | None) -> str:
    normalized = _normalize_csv(markets)
    return normalized or DEFAULT_MARKETS


def _fetch_events(
    api_keys: list[str],
    commence_from: str,
    commence_to: str,
) -> list[dict]:
    params = {
        "dateFormat": DEFAULT_DATE_FORMAT,
        "commenceTimeFrom": commence_from,
        "commenceTimeTo": commence_to,
    }
    url = f"{THEODDSAPI_BASE}/sports/{SPORT_KEY}/events"
    payload, _, _ = _request_json_with_keys(api_keys, url, params)
    if not isinstance(payload, list):
        raise RuntimeError("Unexpected The Odds API events response shape.")
    return payload


def _fetch_event_player_props(
    api_keys: list[str],
    event_id: str,
    markets: str,
    regions: str,
    bookmakers: str | None,
) -> dict:
    params = {
        "regions": regions,
        "markets": markets,
        "oddsFormat": DEFAULT_ODDS_FORMAT,
        "dateFormat": DEFAULT_DATE_FORMAT,
    }
    if bookmakers:
        params["bookmakers"] = bookmakers

    url = f"{THEODDSAPI_BASE}/sports/{SPORT_KEY}/events/{event_id}/odds"
    payload, headers, _ = _request_json_with_keys(api_keys, url, params)

    remaining = headers.get("x-requests-remaining")
    used = headers.get("x-requests-used")
    if remaining or used:
        print(f"[THEODDSAPI] Requests used={used} remaining={remaining}")

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


def run_alt_player_props_ingest(
    api_key: str | None = None,
    api_keys: str | None = None,
    date: str | None = None,
    markets: str = DEFAULT_MARKETS,
    regions: str = DEFAULT_REGIONS,
    bookmakers: str | None = None,
    event_id: str | None = None,
    max_events: int | None = None,
    dataset: str = "odds_raw",
    table: str = "nba_alt_player_props",
    table_id: str | None = None,
    dry_run: bool = False,
    include_sample: bool = False,
) -> dict:
    resolved_keys = _resolve_api_keys(api_key, api_keys)
    markets = _normalize_markets(markets)
    bookmakers = _normalize_csv(bookmakers)
    request_date, commence_from, commence_to = _ny_date_window(date)

    print(
        "[THEODDSAPI] Fetching NBA alternate player props",
        {
            "date": request_date,
            "markets": markets,
            "regions": regions,
            "commence_from": commence_from,
            "commence_to": commence_to,
            "api_keys": len(resolved_keys),
        },
    )

    event_payloads: list[dict] = []
    events_found: int | None = None
    if event_id:
        event_payloads.append(
            _fetch_event_player_props(
                api_keys=resolved_keys,
                event_id=event_id,
                markets=markets,
                regions=regions,
                bookmakers=bookmakers,
            )
        )
        events_found = 1
    else:
        events = _fetch_events(
            api_keys=resolved_keys,
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
                _fetch_event_player_props(
                    api_keys=resolved_keys,
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
        description="Fetch NBA alternate player props from The Odds API into BigQuery."
    )
    parser.add_argument("--api-key", help="The Odds API key (or set THE_ODDS_API_KEY).")
    parser.add_argument(
        "--api-keys",
        help=(
            "Multiple The Odds API keys (comma/pipe/semicolon separated) "
            "or set THE_ODDS_API_KEYS."
        ),
    )
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
    parser.add_argument("--table", default="nba_alt_player_props")
    parser.add_argument("--table-id", help="Full BigQuery table id override.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = run_alt_player_props_ingest(
        api_key=args.api_key,
        api_keys=args.api_keys,
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