from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Sequence

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

try:
    from .client import fetch_paginated, fetch_single
except ImportError:
    from mobile_api.ingest.laliga.client import fetch_paginated, fetch_single

DATASET = os.getenv("LALIGA_DATASET", "laliga_data")
LOCATION = os.getenv("LALIGA_BQ_LOCATION", "US")

TABLE_TEAMS = os.getenv("LALIGA_TEAMS_TABLE", f"{DATASET}.teams")
TABLE_PLAYERS = os.getenv("LALIGA_PLAYERS_TABLE", f"{DATASET}.players")
TABLE_ROSTERS = os.getenv("LALIGA_ROSTERS_TABLE", f"{DATASET}.rosters")
TABLE_STANDINGS = os.getenv("LALIGA_STANDINGS_TABLE", f"{DATASET}.standings")
TABLE_MATCHES = os.getenv("LALIGA_MATCHES_TABLE", f"{DATASET}.matches")
TABLE_MATCH_EVENTS = os.getenv("LALIGA_MATCH_EVENTS_TABLE", f"{DATASET}.match_events")
TABLE_MATCH_LINEUPS = os.getenv("LALIGA_MATCH_LINEUPS_TABLE", f"{DATASET}.match_lineups")


def _get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _table_id(client: bigquery.Client, table: str) -> str:
    if table.count(".") == 2:
        return table
    return f"{client.project}.{table}"


def _ensure_dataset(client: bigquery.Client, table: str) -> None:
    table_id = _table_id(client, table)
    parts = table_id.split(".")
    dataset_id = ".".join(parts[:2])
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = LOCATION
        client.create_dataset(dataset)


def _ensure_table(client: bigquery.Client, table: str) -> str:
    table_id = _table_id(client, table)
    try:
        client.get_table(table_id)
    except NotFound:
        schema = [
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("season", "INT64"),
            bigquery.SchemaField("entity_id", "STRING"),
            bigquery.SchemaField("payload", "STRING"),
        ]
        client.create_table(bigquery.Table(table_id, schema=schema))
    return table_id


def _write_rows(client: bigquery.Client, table: str, season: int, rows: Sequence[Dict[str, Any]], entity_field: str = "id") -> int:
    if not rows:
        return 0
    _ensure_dataset(client, table)
    table_id = _ensure_table(client, table)

    now = datetime.now(timezone.utc).isoformat()
    payload_rows = [
        {
            "ingested_at": now,
            "season": season,
            "entity_id": str(r.get(entity_field) or ""),
            "payload": json.dumps(r, separators=(",", ":"), default=str),
        }
        for r in rows
    ]
    errors = client.insert_rows_json(table_id, payload_rows)
    if errors:
        raise RuntimeError(f"Failed writing to {table_id}: {errors[:3]}")
    return len(payload_rows)


def _season_window(current_season: int | None = None) -> List[int]:
    if current_season is None:
        current_season = datetime.now(timezone.utc).year
    return [current_season - 1, current_season]


def _iter_chunk(values: Sequence[int], size: int = 50) -> Iterable[Sequence[int]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def run_full_ingestion(current_season: int | None = None) -> Dict[str, Any]:
    seasons = _season_window(current_season)
    client = _get_bq_client()
    results: Dict[str, Any] = {"seasons": seasons}

    teams_written = players_written = rosters_written = standings_written = 0
    matches_written = events_written = lineups_written = 0

    team_ids: set[int] = set()
    match_ids: set[int] = set()

    players = fetch_paginated("players")

    for season in seasons:
        teams = fetch_single("teams", {"season": season})
        teams_written += _write_rows(client, TABLE_TEAMS, season, teams)
        team_ids.update(int(t["id"]) for t in teams if t.get("id") is not None)

        standings = fetch_single("standings", {"season": season})
        standings_written += _write_rows(client, TABLE_STANDINGS, season, standings, entity_field="rank")

        matches = fetch_paginated("matches", {"season": season})
        matches_written += _write_rows(client, TABLE_MATCHES, season, matches)
        match_ids.update(int(m["id"]) for m in matches if m.get("id") is not None)

        players_written += _write_rows(client, TABLE_PLAYERS, season, players)

    for season in seasons:
        for team_id in sorted(team_ids):
            roster = fetch_single("rosters", {"team_id": team_id, "season": season})
            rosters_written += _write_rows(client, TABLE_ROSTERS, season, roster, entity_field="player")

    for season in seasons:
        for chunk in _iter_chunk(sorted(match_ids), size=20):
            events = fetch_paginated("match_events", {"match_ids[]": list(chunk)})
            events_written += _write_rows(client, TABLE_MATCH_EVENTS, season, events)
            lineups = fetch_paginated("match_lineups", {"match_ids[]": list(chunk)})
            lineups_written += _write_rows(client, TABLE_MATCH_LINEUPS, season, lineups, entity_field="player")

    results.update(
        {
            "teams": teams_written,
            "players": players_written,
            "rosters": rosters_written,
            "standings": standings_written,
            "matches": matches_written,
            "match_events": events_written,
            "match_lineups": lineups_written,
        }
    )
    return results


def ingest_yesterday_refresh(current_season: int | None = None) -> Dict[str, Any]:
    client = _get_bq_client()
    seasons = _season_window(current_season)
    season = seasons[-1]
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()

    teams = fetch_single("teams", {"season": season})
    standings = fetch_single("standings", {"season": season})
    matches = fetch_paginated("matches", {"dates[]": [yesterday], "season": season})

    match_ids = [int(m["id"]) for m in matches if m.get("id") is not None]
    events: List[Dict[str, Any]] = []
    lineups: List[Dict[str, Any]] = []
    for chunk in _iter_chunk(match_ids, size=20):
        events.extend(fetch_paginated("match_events", {"match_ids[]": list(chunk)}))
        lineups.extend(fetch_paginated("match_lineups", {"match_ids[]": list(chunk)}))

    return {
        "date": yesterday,
        "season": season,
        "teams": _write_rows(client, TABLE_TEAMS, season, teams),
        "standings": _write_rows(client, TABLE_STANDINGS, season, standings, entity_field="rank"),
        "matches": _write_rows(client, TABLE_MATCHES, season, matches),
        "match_events": _write_rows(client, TABLE_MATCH_EVENTS, season, events),
        "match_lineups": _write_rows(client, TABLE_MATCH_LINEUPS, season, lineups, entity_field="player"),
    }
