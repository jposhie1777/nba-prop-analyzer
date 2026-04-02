"""
Ingest PGA Tour tournament weather data → BigQuery.

Fetches hourly and daily weather forecasts for the active tournament
from the PGA Tour GraphQL API.

Usage:
    python -m mobile_api.ingest.pga.pga_weather_ingest [--tournament-id R2026014] [--dry-run]
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import time
from typing import Any, Dict, List, Optional

import requests
from google.cloud import bigquery

DATASET = os.getenv("PGA_DATASET", "pga_data")
TABLE = os.getenv("PGA_WEATHER_TABLE", "tournament_weather")

GRAPHQL_ENDPOINT = "https://orchestrator.pgatour.com/graphql"
API_KEY = os.getenv("PGA_TOUR_GQL_API_KEY", "da2-gsrx5bibzbb4njvhl7t37wqyl4")

WEATHER_QUERY = """
query Weather($tournamentId: ID!) {
  weather(id: $tournamentId) {
    title
    hourly {
      title
      condition
      windDirection
      windSpeedKPH
      windSpeedMPH
      humidity
      precipitation
      temperature {
        __typename
        ... on StandardWeatherTemp {
          tempC
          tempF
        }
        ... on RangeWeatherTemp {
          minTempC
          minTempF
          maxTempC
          maxTempF
        }
      }
    }
    daily {
      title
      condition
      windDirection
      windSpeedKPH
      windSpeedMPH
      humidity
      precipitation
      temperature {
        __typename
        ... on RangeWeatherTemp {
          minTempC
          minTempF
          maxTempC
          maxTempF
        }
      }
    }
  }
}
""".strip()

_SCHEMA = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("forecast_type", "STRING"),  # hourly / daily
    bigquery.SchemaField("time_label", "STRING"),
    bigquery.SchemaField("condition", "STRING"),
    bigquery.SchemaField("wind_direction", "STRING"),
    bigquery.SchemaField("wind_speed_mph", "FLOAT64"),
    bigquery.SchemaField("wind_speed_kph", "FLOAT64"),
    bigquery.SchemaField("humidity_pct", "FLOAT64"),
    bigquery.SchemaField("precipitation_pct", "FLOAT64"),
    bigquery.SchemaField("temp_f", "FLOAT64"),
    bigquery.SchemaField("temp_c", "FLOAT64"),
    bigquery.SchemaField("min_temp_f", "FLOAT64"),
    bigquery.SchemaField("max_temp_f", "FLOAT64"),
]


def _headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "x-api-key": API_KEY,
        "x-pgat-platform": "web",
        "Referer": "https://www.pgatour.com/",
        "Origin": "https://www.pgatour.com",
    }


def _parse_num(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = re.sub(r"[^0-9.\-]", "", str(val))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def fetch_weather(tournament_id: str) -> Dict[str, Any]:
    resp = requests.post(
        GRAPHQL_ENDPOINT,
        headers=_headers(),
        json={"query": WEATHER_QUERY, "variables": {"tournamentId": tournament_id}},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json() or {}
    data = result.get("data") or {}
    return data.get("weather") or {}


def weather_to_rows(
    tournament_id: str,
    weather: Dict[str, Any],
    run_ts: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for entry in weather.get("hourly") or []:
        temp = entry.get("temperature") or {}
        rows.append({
            "run_ts": run_ts,
            "ingested_at": run_ts,
            "tournament_id": tournament_id,
            "forecast_type": "hourly",
            "time_label": entry.get("title"),
            "condition": entry.get("condition"),
            "wind_direction": entry.get("windDirection"),
            "wind_speed_mph": _parse_num(entry.get("windSpeedMPH")),
            "wind_speed_kph": _parse_num(entry.get("windSpeedKPH")),
            "humidity_pct": _parse_num(entry.get("humidity")),
            "precipitation_pct": _parse_num(entry.get("precipitation")),
            "temp_f": _parse_num(temp.get("tempF")),
            "temp_c": _parse_num(temp.get("tempC")),
            "min_temp_f": _parse_num(temp.get("minTempF")),
            "max_temp_f": _parse_num(temp.get("maxTempF")),
        })

    for entry in weather.get("daily") or []:
        temp = entry.get("temperature") or {}
        rows.append({
            "run_ts": run_ts,
            "ingested_at": run_ts,
            "tournament_id": tournament_id,
            "forecast_type": "daily",
            "time_label": entry.get("title"),
            "condition": entry.get("condition"),
            "wind_direction": entry.get("windDirection"),
            "wind_speed_mph": _parse_num(entry.get("windSpeedMPH")),
            "wind_speed_kph": _parse_num(entry.get("windSpeedKPH")),
            "humidity_pct": _parse_num(entry.get("humidity")),
            "precipitation_pct": _parse_num(entry.get("precipitation")),
            "temp_f": _parse_num(temp.get("tempF")),
            "temp_c": _parse_num(temp.get("tempC")),
            "min_temp_f": _parse_num(temp.get("minTempF")),
            "max_temp_f": _parse_num(temp.get("maxTempF")),
        })

    return rows


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project)


def _ensure_table(client: bigquery.Client) -> str:
    table_id = f"{client.project}.{DATASET}.{TABLE}"
    bq_table = bigquery.Table(table_id, schema=_SCHEMA)
    bq_table.description = "PGA Tour tournament weather forecasts (hourly + daily)"
    client.create_table(bq_table, exists_ok=True)
    return table_id


def ingest_weather(
    tournament_id: str,
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    ts = datetime.datetime.utcnow().isoformat()
    print(f"[weather] Fetching weather for {tournament_id}…")
    weather = fetch_weather(tournament_id)

    if not weather:
        print("[weather] No weather data returned.")
        return {"rows_fetched": 0, "rows_inserted": 0}

    rows = weather_to_rows(tournament_id, weather, ts)
    print(f"[weather] Got {len(rows)} weather rows ({len(weather.get('hourly', []))} hourly, {len(weather.get('daily', []))} daily)")

    if dry_run or not rows:
        return {"rows_fetched": len(rows), "rows_inserted": 0}

    client = _bq_client()
    table_id = _ensure_table(client)
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    print(f"[weather] Inserted {len(rows)} rows into {table_id}")
    return {"rows_fetched": len(rows), "rows_inserted": len(rows)}


def _get_active_tournament_id() -> Optional[str]:
    """Auto-detect the active tournament from the schedule."""
    from .pga_schedule import fetch_schedule
    current_year = datetime.datetime.utcnow().year
    tournaments = fetch_schedule(tour_code="R", year=str(current_year))
    today = datetime.datetime.utcnow().date()

    upcoming_started = []
    completed = []

    for t in tournaments:
        tid = getattr(t, "id", None) or getattr(t, "tournament_id", None) or ""
        start = getattr(t, "start_date", None) or ""
        bucket = (getattr(t, "bucket", None) or "").lower()
        if not tid or not start:
            continue
        try:
            start_date = datetime.date.fromisoformat(str(start)[:10])
        except ValueError:
            continue

        if bucket == "upcoming" and start_date <= today + datetime.timedelta(days=3):
            upcoming_started.append((start_date, tid))
        elif bucket == "completed":
            completed.append((start_date, tid))

    if upcoming_started:
        return max(upcoming_started, key=lambda x: x[0])[1]
    if completed:
        return max(completed, key=lambda x: x[0])[1]
    return None


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Ingest PGA weather data.")
    parser.add_argument("--tournament-id", metavar="ID", help="e.g. R2026014")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    tournament_id = args.tournament_id
    if not tournament_id:
        tournament_id = _get_active_tournament_id()
        if not tournament_id:
            print("[weather] No active tournament found.")
            return

    result = ingest_weather(tournament_id, dry_run=args.dry_run)
    print(result)


if __name__ == "__main__":
    _cli()
