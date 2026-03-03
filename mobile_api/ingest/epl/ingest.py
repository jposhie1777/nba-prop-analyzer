from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Sequence

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

try:
    from .premierleague_scraper import (
        fetch_match_details,
        fetch_match_events,
        fetch_match_stats,
        fetch_player_stats,
        fetch_schedule,
        fetch_standings,
        fetch_team_stats,
    )
except ImportError:
    from mobile_api.ingest.epl.premierleague_scraper import (
        fetch_match_details,
        fetch_match_events,
        fetch_match_stats,
        fetch_player_stats,
        fetch_schedule,
        fetch_standings,
        fetch_team_stats,
    )

DATASET = os.getenv("EPL_DATASET", "epl_data")
LOCATION = os.getenv("EPL_BQ_LOCATION", "US")

TABLE_TEAMS = os.getenv("EPL_TEAMS_TABLE", f"{DATASET}.teams")
TABLE_PLAYERS = os.getenv("EPL_PLAYERS_TABLE", f"{DATASET}.players")
TABLE_STANDINGS = os.getenv("EPL_STANDINGS_TABLE", f"{DATASET}.standings")
TABLE_MATCHES = os.getenv("EPL_MATCHES_TABLE", f"{DATASET}.matches")
TABLE_MATCH_DETAILS = os.getenv("EPL_MATCH_DETAILS_TABLE", f"{DATASET}.match_details")
TABLE_MATCH_EVENTS = os.getenv("EPL_MATCH_EVENTS_TABLE", f"{DATASET}.match_events")
TABLE_MATCH_TEAM_STATS = os.getenv("EPL_MATCH_TEAM_STATS_TABLE", f"{DATASET}.match_team_stats")


def _get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _table_id(client: bigquery.Client, table: str) -> str:
    return table if table.count(".") == 2 else f"{client.project}.{table}"


def _ensure_dataset(client: bigquery.Client, table: str) -> None:
    dataset_id = ".".join(_table_id(client, table).split(".")[:2])
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


def _write_rows(client: bigquery.Client, table: str, season: int, rows: Sequence[Dict[str, Any]], entity_field: str) -> int:
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


def _season_window(current_season: int | None = None) -> list[int]:
    year = current_season or datetime.now(timezone.utc).year
    return [year - 1, year]


def _build_team_rows(schedule_rows: Sequence[Dict[str, Any]]) -> list[Dict[str, Any]]:
    teams: Dict[str, Dict[str, Any]] = {}
    for m in schedule_rows:
        home = m.get("homeTeam") or {}
        away = m.get("awayTeam") or {}
        for t in (home, away):
            team_id = str(t.get("id") or "")
            if team_id:
                teams[team_id] = t
    return list(teams.values())


def run_full_ingestion(current_season: int | None = None) -> Dict[str, Any]:
    client = _get_bq_client()
    seasons = _season_window(current_season)

    teams_written = players_written = standings_written = 0
    matches_written = details_written = events_written = team_stats_written = 0

    for season in seasons:
        schedule_rows = fetch_schedule(season)
        match_ids = [str(m.get("matchId") or "") for m in schedule_rows if m.get("matchId")]

        teams = fetch_team_stats(season) or _build_team_rows(schedule_rows)
        players = fetch_player_stats(season)

        matches_written += _write_rows(client, TABLE_MATCHES, season, schedule_rows, entity_field="matchId")
        teams_written += _write_rows(client, TABLE_TEAMS, season, teams, entity_field="id")
        players_written += _write_rows(client, TABLE_PLAYERS, season, players, entity_field="id")
        standings = fetch_standings(season)
        standings_written += _write_rows(client, TABLE_STANDINGS, season, standings, entity_field="id")

        details_rows = []
        event_rows = []
        stat_rows = []
        for match_id in match_ids:
            detail = fetch_match_details(match_id)
            if detail:
                detail["_entity_id"] = match_id
                details_rows.append(detail)

            events = fetch_match_events(match_id)
            if events:
                events["matchId"] = match_id
                events["_entity_id"] = match_id
                event_rows.append(events)

            for side in fetch_match_stats(match_id):
                side_name = str(side.get("side") or "unknown").lower()
                side["matchId"] = match_id
                side["_entity_id"] = f"{match_id}_{side_name}"
                stat_rows.append(side)

        details_written += _write_rows(client, TABLE_MATCH_DETAILS, season, details_rows, entity_field="_entity_id")
        events_written += _write_rows(client, TABLE_MATCH_EVENTS, season, event_rows, entity_field="_entity_id")
        team_stats_written += _write_rows(client, TABLE_MATCH_TEAM_STATS, season, stat_rows, entity_field="_entity_id")

    return {
        "seasons": seasons,
        "teams": teams_written,
        "players": players_written,
        "standings": standings_written,
        "matches": matches_written,
        "match_details": details_written,
        "match_events": events_written,
        "match_team_stats": team_stats_written,
    }


def ingest_yesterday_refresh(current_season: int | None = None) -> Dict[str, Any]:
    client = _get_bq_client()
    season = _season_window(current_season)[-1]
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()

    schedule = [m for m in fetch_schedule(season) if str(m.get("kickoff") or "").startswith(yesterday)]
    match_ids = [str(m.get("matchId") or "") for m in schedule if m.get("matchId")]

    details_rows = []
    event_rows = []
    stat_rows = []
    for match_id in match_ids:
        detail = fetch_match_details(match_id)
        if detail:
            detail["_entity_id"] = match_id
            details_rows.append(detail)

        events = fetch_match_events(match_id)
        if events:
            events["matchId"] = match_id
            events["_entity_id"] = match_id
            event_rows.append(events)

        for side in fetch_match_stats(match_id):
            side_name = str(side.get("side") or "unknown").lower()
            side["matchId"] = match_id
            side["_entity_id"] = f"{match_id}_{side_name}"
            stat_rows.append(side)

    return {
        "date": yesterday,
        "season": season,
        "matches": _write_rows(client, TABLE_MATCHES, season, schedule, entity_field="matchId"),
        "match_details": _write_rows(client, TABLE_MATCH_DETAILS, season, details_rows, entity_field="_entity_id"),
        "match_events": _write_rows(client, TABLE_MATCH_EVENTS, season, event_rows, entity_field="_entity_id"),
        "match_team_stats": _write_rows(client, TABLE_MATCH_TEAM_STATS, season, stat_rows, entity_field="_entity_id"),
    }
