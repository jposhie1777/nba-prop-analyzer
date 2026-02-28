from __future__ import annotations

import os

from google.cloud import bigquery


EXPECTED_SCHEMA = [
    ("ingested_at", "TIMESTAMP"),
    ("season", "INT64"),
    ("entity_id", "STRING"),
    ("payload", "STRING"),
]

# Raw-layer tables have three extra columns.
RAW_EXPECTED_SCHEMA = [
    ("ingested_at", "TIMESTAMP"),
    ("season", "INT64"),
    ("entity_id", "STRING"),
    ("payload", "STRING"),
    ("payload_hash", "STRING"),
    ("ingest_run_id", "STRING"),
    ("source", "STRING"),
]

# BigQuery normalises standard-SQL type names to their legacy aliases when
# returning schema via the REST API (e.g. INT64 → INTEGER, FLOAT64 → FLOAT).
# Normalise both sides before comparing so a mismatch doesn't trigger an
# accidental CREATE OR REPLACE TABLE (which would wipe all historical data).
_BQ_TYPE_NORMALIZE = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
}


def _get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _dataset() -> str:
    return os.getenv("MLS_DATASET", "mls_data")


def _location() -> str:
    return os.getenv("MLS_BQ_LOCATION", "US")


def _table_ddl(dataset: str, table_name: str, description: str, replace: bool = False, raw: bool = False) -> str:
    create_verb = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    extra_cols = ""
    if raw:
        extra_cols = """
      payload_hash  STRING,
      ingest_run_id STRING,
      source        STRING,"""
    return f"""
    {create_verb} `{dataset}.{table_name}` (
      ingested_at TIMESTAMP NOT NULL,
      season INT64 NOT NULL,
      entity_id STRING,
      payload STRING,{extra_cols}
    )
    PARTITION BY DATE(ingested_at)
    CLUSTER BY season, entity_id
    OPTIONS (description = '{description}')
    """


def _schema_matches(table: bigquery.Table, raw: bool = False) -> bool:
    actual = [
        (field.name, _BQ_TYPE_NORMALIZE.get(field.field_type, field.field_type))
        for field in table.schema
    ]
    expected_schema = RAW_EXPECTED_SCHEMA if raw else EXPECTED_SCHEMA
    expected = [
        (name, _BQ_TYPE_NORMALIZE.get(typ, typ))
        for name, typ in expected_schema
    ]
    return actual == expected


def _ensure_table(client: bigquery.Client, dataset: str, table_name: str, description: str, raw: bool = False) -> None:
    table_id = f"{client.project}.{dataset}.{table_name}"
    try:
        table = client.get_table(table_id)
    except Exception:
        client.query(_table_ddl(dataset, table_name, description, replace=False, raw=raw)).result()
        return

    if not _schema_matches(table, raw=raw):
        client.query(_table_ddl(dataset, table_name, description, replace=True, raw=raw)).result()


def main() -> None:
    client = _get_bq_client()
    dataset = _dataset()
    location = _location()

    client.query(f'CREATE SCHEMA IF NOT EXISTS `{dataset}` OPTIONS(location = "{location}")').result()

    # Legacy tables (BallDontLie + original mlssoccer_* — never touched per Step 0)
    tables = [
        ("teams", "MLS teams payload snapshots from BallDontLie API"),
        ("players", "MLS players payload snapshots from BallDontLie API"),
        ("rosters", "MLS team roster payload snapshots from BallDontLie API"),
        ("standings", "MLS standings payload snapshots from BallDontLie API"),
        ("matches", "MLS matches payload snapshots from BallDontLie API"),
        ("match_events", "MLS match events payload snapshots from BallDontLie API"),
        ("match_lineups", "MLS match lineups payload snapshots from BallDontLie API"),
        # mlssoccer.com legacy tables — kept intact, writes switched to raw_* below
        ("mlssoccer_schedule", "MLS match schedule snapshots from stats-api.mlssoccer.com"),
        ("mlssoccer_team_stats", "MLS per-club season stats from stats-api.mlssoccer.com"),
        ("mlssoccer_player_stats", "MLS per-player season stats from stats-api.mlssoccer.com"),
        ("mlssoccer_team_game_stats", "MLS per-club per-match stats from stats-api.mlssoccer.com (entity_id = match_id_club_id)"),
        ("mlssoccer_player_game_stats", "MLS per-player per-match stats from stats-api.mlssoccer.com (entity_id = match_id_player_id)"),
    ]

    for table_name, description in tables:
        _ensure_table(client, dataset, table_name, description)

    # New RAW layer tables (with payload_hash + ingest_run_id + source)
    raw_tables = [
        ("raw_schedule_json",      "RAW MLS match schedule — entity_id = match_id"),
        ("raw_team_season_json",   "RAW MLS per-club season stats — entity_id = team_id"),
        ("raw_player_season_json", "RAW MLS per-player season stats — entity_id = player_id"),
        ("raw_team_match_json",    "RAW MLS per-club per-match stats — entity_id = match_id_team_id"),
        ("raw_player_match_json",  "RAW MLS per-player per-match stats — entity_id = match_id_player_id"),
    ]

    for table_name, description in raw_tables:
        _ensure_table(client, dataset, table_name, description, raw=True)


if __name__ == "__main__":
    main()
