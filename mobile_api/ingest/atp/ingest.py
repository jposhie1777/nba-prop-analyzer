from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

from ingest.common.batch import chunked
from .client import AtpApiError, fetch_paginated, get_rate_limits

# ======================================================
# Config
# ======================================================

DEFAULT_DATASET = os.getenv("ATP_DATASET", "atp_data")
DEFAULT_LOCATION = os.getenv("ATP_BQ_LOCATION", "US")

ATP_PLAYERS_TABLE = os.getenv("ATP_PLAYERS_TABLE", f"{DEFAULT_DATASET}.players")
ATP_TOURNAMENTS_TABLE = os.getenv("ATP_TOURNAMENTS_TABLE", f"{DEFAULT_DATASET}.tournaments")
ATP_MATCHES_TABLE = os.getenv("ATP_MATCHES_TABLE", f"{DEFAULT_DATASET}.matches")
ATP_RANKINGS_TABLE = os.getenv("ATP_RANKINGS_TABLE", f"{DEFAULT_DATASET}.rankings")
ATP_RACE_TABLE = os.getenv("ATP_RACE_TABLE", f"{DEFAULT_DATASET}.atp_race")

DEFAULT_START_SEASON = int(os.getenv("ATP_HISTORICAL_START_SEASON", "1990"))
DEFAULT_END_SEASON = int(os.getenv("ATP_HISTORICAL_END_SEASON", str(datetime.utcnow().year)))

# ======================================================
# BigQuery helpers
# ======================================================


def get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if project:
        return bigquery.Client(project=project)
    return bigquery.Client()


def resolve_table_id(table: str, project: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{project}.{table}"
    return f"{project}.{DEFAULT_DATASET}.{table}"


def dataset_id_from_table_id(table_id: str) -> str:
    parts = table_id.split(".")
    if len(parts) < 2:
        raise ValueError(f"Invalid table id: {table_id}")
    return ".".join(parts[:2])


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    try:
        client.get_dataset(dataset_id)
        return
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = DEFAULT_LOCATION
        client.create_dataset(dataset)
    except Conflict:
        return


def ensure_table(
    client: bigquery.Client,
    table_id: str,
    schema: List[bigquery.SchemaField],
) -> None:
    try:
        client.get_table(table_id)
        return
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
    except Conflict:
        return


def insert_rows_to_bq(
    rows: List[Dict[str, Any]],
    table: str,
    *,
    client: Optional[bigquery.Client] = None,
    batch_size: int = 500,
) -> int:
    if not rows:
        return 0

    client = client or get_bq_client()
    table_id = resolve_table_id(table, client.project)

    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            raise RuntimeError(f"BigQuery insert failed: {errors[:3]}")
        total += len(batch)

    return total


# ======================================================
# Schemas
# ======================================================


SCHEMA_PLAYERS = [
    bigquery.SchemaField("run_ts", "TIMESTAMP"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("full_name", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("birth_place", "STRING"),
    bigquery.SchemaField("age", "INT64"),
    bigquery.SchemaField("height_cm", "INT64"),
    bigquery.SchemaField("weight_kg", "INT64"),
    bigquery.SchemaField("plays", "STRING"),
    bigquery.SchemaField("turned_pro", "INT64"),
    bigquery.SchemaField("raw_json", "STRING"),
]

SCHEMA_TOURNAMENTS = [
    bigquery.SchemaField("run_ts", "TIMESTAMP"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("tournament_id", "INT64"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("surface", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("start_date", "STRING"),
    bigquery.SchemaField("end_date", "STRING"),
    bigquery.SchemaField("prize_money", "INT64"),
    bigquery.SchemaField("prize_currency", "STRING"),
    bigquery.SchemaField("draw_size", "INT64"),
    bigquery.SchemaField("raw_json", "STRING"),
]

SCHEMA_MATCHES = [
    bigquery.SchemaField("run_ts", "TIMESTAMP"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("match_id", "INT64"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("round", "STRING"),
    bigquery.SchemaField("score", "STRING"),
    bigquery.SchemaField("duration", "STRING"),
    bigquery.SchemaField("number_of_sets", "INT64"),
    bigquery.SchemaField("match_status", "STRING"),
    bigquery.SchemaField("is_live", "BOOL"),
    bigquery.SchemaField("tournament_id", "INT64"),
    bigquery.SchemaField("tournament_name", "STRING"),
    bigquery.SchemaField("tournament_location", "STRING"),
    bigquery.SchemaField("surface", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("tournament_season", "INT64"),
    bigquery.SchemaField("tournament_start_date", "STRING"),
    bigquery.SchemaField("tournament_end_date", "STRING"),
    bigquery.SchemaField("player1_id", "INT64"),
    bigquery.SchemaField("player1_name", "STRING"),
    bigquery.SchemaField("player2_id", "INT64"),
    bigquery.SchemaField("player2_name", "STRING"),
    bigquery.SchemaField("winner_id", "INT64"),
    bigquery.SchemaField("winner_name", "STRING"),
    bigquery.SchemaField("raw_json", "STRING"),
]

SCHEMA_RANKINGS = [
    bigquery.SchemaField("run_ts", "TIMESTAMP"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("ranking_id", "INT64"),
    bigquery.SchemaField("ranking_date", "STRING"),
    bigquery.SchemaField("rank", "INT64"),
    bigquery.SchemaField("points", "INT64"),
    bigquery.SchemaField("movement", "INT64"),
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("raw_json", "STRING"),
]

SCHEMA_RACE = [
    bigquery.SchemaField("run_ts", "TIMESTAMP"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("race_id", "INT64"),
    bigquery.SchemaField("ranking_date", "STRING"),
    bigquery.SchemaField("rank", "INT64"),
    bigquery.SchemaField("points", "INT64"),
    bigquery.SchemaField("movement", "INT64"),
    bigquery.SchemaField("is_qualified", "BOOL"),
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("raw_json", "STRING"),
]


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _json_dump(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


# ======================================================
# Transformers
# ======================================================


def transform_player(record: Dict[str, Any], run_ts: str) -> Dict[str, Any]:
    return {
        "run_ts": run_ts,
        "ingested_at": _now_iso(),
        "player_id": record.get("id"),
        "first_name": record.get("first_name"),
        "last_name": record.get("last_name"),
        "full_name": record.get("full_name"),
        "country": record.get("country"),
        "country_code": record.get("country_code"),
        "birth_place": record.get("birth_place"),
        "age": record.get("age"),
        "height_cm": record.get("height_cm"),
        "weight_kg": record.get("weight_kg"),
        "plays": record.get("plays"),
        "turned_pro": record.get("turned_pro"),
        "raw_json": _json_dump(record),
    }


def transform_tournament(record: Dict[str, Any], run_ts: str) -> Dict[str, Any]:
    return {
        "run_ts": run_ts,
        "ingested_at": _now_iso(),
        "tournament_id": record.get("id"),
        "name": record.get("name"),
        "location": record.get("location"),
        "surface": record.get("surface"),
        "category": record.get("category"),
        "season": record.get("season"),
        "start_date": record.get("start_date"),
        "end_date": record.get("end_date"),
        "prize_money": record.get("prize_money"),
        "prize_currency": record.get("prize_currency"),
        "draw_size": record.get("draw_size"),
        "raw_json": _json_dump(record),
    }


def transform_match(record: Dict[str, Any], run_ts: str, season: Optional[int]) -> Dict[str, Any]:
    tournament = record.get("tournament") or {}
    player1 = record.get("player1") or {}
    player2 = record.get("player2") or {}
    winner = record.get("winner") or {}
    return {
        "run_ts": run_ts,
        "ingested_at": _now_iso(),
        "match_id": record.get("id"),
        "season": record.get("season") or season,
        "round": record.get("round"),
        "score": record.get("score"),
        "duration": record.get("duration"),
        "number_of_sets": record.get("number_of_sets"),
        "match_status": record.get("match_status"),
        "is_live": record.get("is_live"),
        "tournament_id": tournament.get("id"),
        "tournament_name": tournament.get("name"),
        "tournament_location": tournament.get("location"),
        "surface": tournament.get("surface"),
        "category": tournament.get("category"),
        "tournament_season": tournament.get("season"),
        "tournament_start_date": tournament.get("start_date"),
        "tournament_end_date": tournament.get("end_date"),
        "player1_id": player1.get("id"),
        "player1_name": player1.get("full_name"),
        "player2_id": player2.get("id"),
        "player2_name": player2.get("full_name"),
        "winner_id": winner.get("id"),
        "winner_name": winner.get("full_name"),
        "raw_json": _json_dump(record),
    }


def transform_ranking(record: Dict[str, Any], run_ts: str) -> Dict[str, Any]:
    player = record.get("player") or {}
    return {
        "run_ts": run_ts,
        "ingested_at": _now_iso(),
        "ranking_id": record.get("id"),
        "ranking_date": record.get("ranking_date"),
        "rank": record.get("rank"),
        "points": record.get("points"),
        "movement": record.get("movement"),
        "player_id": player.get("id"),
        "player_name": player.get("full_name"),
        "country": player.get("country"),
        "country_code": player.get("country_code"),
        "raw_json": _json_dump(record),
    }


def transform_race(record: Dict[str, Any], run_ts: str) -> Dict[str, Any]:
    player = record.get("player") or {}
    return {
        "run_ts": run_ts,
        "ingested_at": _now_iso(),
        "race_id": record.get("id"),
        "ranking_date": record.get("ranking_date"),
        "rank": record.get("rank"),
        "points": record.get("points"),
        "movement": record.get("movement"),
        "is_qualified": record.get("is_qualified"),
        "player_id": player.get("id"),
        "player_name": player.get("full_name"),
        "country": player.get("country"),
        "country_code": player.get("country_code"),
        "raw_json": _json_dump(record),
    }


# ======================================================
# Ingest functions
# ======================================================


def ingest_players(*, table: str = ATP_PLAYERS_TABLE, create_tables: bool = True) -> Dict[str, Any]:
    client = get_bq_client()
    table_id = resolve_table_id(table, client.project)
    if create_tables:
        ensure_dataset(client, dataset_id_from_table_id(table_id))
        ensure_table(client, table_id, SCHEMA_PLAYERS)

    run_ts = _now_iso()
    records = fetch_paginated("/players")
    rows = [transform_player(record, run_ts) for record in records]
    inserted = insert_rows_to_bq(rows, table, client=client)
    return {"table": table, "records": len(records), "inserted": inserted}


def ingest_tournaments(
    *,
    season: Optional[int] = None,
    table: str = ATP_TOURNAMENTS_TABLE,
    create_tables: bool = True,
) -> Dict[str, Any]:
    client = get_bq_client()
    table_id = resolve_table_id(table, client.project)
    if create_tables:
        ensure_dataset(client, dataset_id_from_table_id(table_id))
        ensure_table(client, table_id, SCHEMA_TOURNAMENTS)

    run_ts = _now_iso()
    params: Dict[str, Any] = {}
    if season is not None:
        params["season"] = season
    records = fetch_paginated("/tournaments", params=params)
    rows = [transform_tournament(record, run_ts) for record in records]
    inserted = insert_rows_to_bq(rows, table, client=client)
    tournament_ids = [record.get("id") for record in records if record.get("id")]
    return {
        "table": table,
        "season": season,
        "records": len(records),
        "inserted": inserted,
        "tournament_ids": tournament_ids,
    }


def _fetch_matches_by_tournament(
    *,
    season: Optional[int],
    tournament_ids: List[int],
) -> List[Dict[str, Any]]:
    rate = get_rate_limits()
    records: List[Dict[str, Any]] = []
    for batch in chunked(tournament_ids, size=25):
        params: Dict[str, Any] = {"tournament_ids[]": batch}
        if season is not None:
            params["season"] = season
        batch_records = fetch_paginated("/matches", params=params)
        records.extend(batch_records)
        time.sleep(rate["batch_delay"])
    return records


def ingest_matches(
    *,
    season: Optional[int] = None,
    tournament_ids: Optional[List[int]] = None,
    table: str = ATP_MATCHES_TABLE,
    create_tables: bool = True,
) -> Dict[str, Any]:
    client = get_bq_client()
    table_id = resolve_table_id(table, client.project)
    if create_tables:
        ensure_dataset(client, dataset_id_from_table_id(table_id))
        ensure_table(client, table_id, SCHEMA_MATCHES)

    run_ts = _now_iso()
    params: Dict[str, Any] = {}
    if season is not None:
        params["season"] = season

    try:
        records = fetch_paginated("/matches", params=params)
    except AtpApiError:
        if tournament_ids:
            records = _fetch_matches_by_tournament(
                season=season,
                tournament_ids=tournament_ids,
            )
        else:
            raise

    rows = [transform_match(record, run_ts, season) for record in records]
    inserted = insert_rows_to_bq(rows, table, client=client)
    return {
        "table": table,
        "season": season,
        "records": len(records),
        "inserted": inserted,
    }


def ingest_rankings(
    *,
    ranking_date: Optional[str] = None,
    table: str = ATP_RANKINGS_TABLE,
    create_tables: bool = True,
) -> Dict[str, Any]:
    client = get_bq_client()
    table_id = resolve_table_id(table, client.project)
    if create_tables:
        ensure_dataset(client, dataset_id_from_table_id(table_id))
        ensure_table(client, table_id, SCHEMA_RANKINGS)

    run_ts = _now_iso()
    params: Dict[str, Any] = {}
    if ranking_date:
        params["ranking_date"] = ranking_date
    records = fetch_paginated("/rankings", params=params)
    rows = [transform_ranking(record, run_ts) for record in records]
    inserted = insert_rows_to_bq(rows, table, client=client)
    return {"table": table, "records": len(records), "inserted": inserted}


def ingest_atp_race(
    *,
    ranking_date: Optional[str] = None,
    table: str = ATP_RACE_TABLE,
    create_tables: bool = True,
) -> Dict[str, Any]:
    client = get_bq_client()
    table_id = resolve_table_id(table, client.project)
    if create_tables:
        ensure_dataset(client, dataset_id_from_table_id(table_id))
        ensure_table(client, table_id, SCHEMA_RACE)

    run_ts = _now_iso()
    params: Dict[str, Any] = {}
    if ranking_date:
        params["ranking_date"] = ranking_date
    records = fetch_paginated("/atp_race", params=params)
    rows = [transform_race(record, run_ts) for record in records]
    inserted = insert_rows_to_bq(rows, table, client=client)
    return {"table": table, "records": len(records), "inserted": inserted}


def ingest_historical(
    *,
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    include_players: bool = True,
    include_tournaments: bool = True,
    include_matches: bool = True,
    include_rankings: bool = True,
    include_atp_race: bool = True,
    create_tables: bool = True,
) -> Dict[str, Any]:
    rate = get_rate_limits()
    start = start_season or DEFAULT_START_SEASON
    end = end_season or DEFAULT_END_SEASON
    if start > end:
        raise ValueError("start_season must be <= end_season")

    summary: Dict[str, Any] = {
        "start_season": start,
        "end_season": end,
        "seasons": {},
    }

    if include_players:
        summary["players"] = ingest_players(create_tables=create_tables)

    for season in range(start, end + 1):
        season_summary: Dict[str, Any] = {}
        tournaments_payload: Optional[Dict[str, Any]] = None

        if include_tournaments:
            tournaments_payload = ingest_tournaments(
                season=season,
                create_tables=create_tables,
            )
            season_summary["tournaments"] = {
                "records": tournaments_payload.get("records"),
                "inserted": tournaments_payload.get("inserted"),
            }

        if include_matches:
            tournament_ids = (
                tournaments_payload.get("tournament_ids") if tournaments_payload else None
            )
            season_summary["matches"] = ingest_matches(
                season=season,
                tournament_ids=tournament_ids,
                create_tables=create_tables,
            )

        summary["seasons"][season] = season_summary
        time.sleep(rate["batch_delay"])

    if include_rankings:
        summary["rankings"] = ingest_rankings(create_tables=create_tables)

    if include_atp_race:
        summary["atp_race"] = ingest_atp_race(create_tables=create_tables)

    return summary


# ======================================================
# CLI
# ======================================================


def _print_usage() -> None:
    print("Usage:")
    print("  python ingest.py historical <start_season> <end_season>")
    print("  python ingest.py players")
    print("  python ingest.py tournaments <season>")
    print("  python ingest.py matches <season>")
    print("  python ingest.py rankings [ranking_date]")
    print("  python ingest.py race [ranking_date]")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        _print_usage()
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "historical":
        start = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_START_SEASON
        end = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_END_SEASON
        result = ingest_historical(start_season=start, end_season=end)
        print(json.dumps(result, indent=2))
    elif cmd == "players":
        result = ingest_players()
        print(json.dumps(result, indent=2))
    elif cmd == "tournaments" and len(sys.argv) > 2:
        result = ingest_tournaments(season=int(sys.argv[2]))
        print(json.dumps(result, indent=2))
    elif cmd == "matches" and len(sys.argv) > 2:
        result = ingest_matches(season=int(sys.argv[2]))
        print(json.dumps(result, indent=2))
    elif cmd == "rankings":
        ranking_date = sys.argv[2] if len(sys.argv) > 2 else None
        result = ingest_rankings(ranking_date=ranking_date)
        print(json.dumps(result, indent=2))
    elif cmd == "race":
        ranking_date = sys.argv[2] if len(sys.argv) > 2 else None
        result = ingest_atp_race(ranking_date=ranking_date)
        print(json.dumps(result, indent=2))
    else:
        _print_usage()
        sys.exit(1)
