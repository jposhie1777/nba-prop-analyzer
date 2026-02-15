from __future__ import annotations

import os

from google.cloud import bigquery


EXPECTED_SCHEMA = [
    ("ingested_at", "TIMESTAMP"),
    ("season", "INT64"),
    ("entity_id", "STRING"),
    ("payload", "STRING"),
]


def _get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _dataset() -> str:
    return os.getenv("EPL_DATASET", "epl_data")


def _location() -> str:
    return os.getenv("EPL_BQ_LOCATION", "US")


def _table_ddl(dataset: str, table_name: str, description: str, replace: bool = False) -> str:
    create_verb = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    return f"""
    {create_verb} `{dataset}.{table_name}` (
      ingested_at TIMESTAMP NOT NULL,
      season INT64 NOT NULL,
      entity_id STRING,
      payload STRING
    )
    PARTITION BY DATE(ingested_at)
    CLUSTER BY season, entity_id
    OPTIONS (description = '{description}')
    """


def _schema_matches(table: bigquery.Table) -> bool:
    actual = [(field.name, field.field_type) for field in table.schema]
    return actual == EXPECTED_SCHEMA


def _ensure_table(client: bigquery.Client, dataset: str, table_name: str, description: str) -> None:
    table_id = f"{client.project}.{dataset}.{table_name}"
    try:
        table = client.get_table(table_id)
    except Exception:
        client.query(_table_ddl(dataset, table_name, description, replace=False)).result()
        return

    if not _schema_matches(table):
        client.query(_table_ddl(dataset, table_name, description, replace=True)).result()


def main() -> None:
    client = _get_bq_client()
    dataset = _dataset()
    location = _location()

    client.query(f'CREATE SCHEMA IF NOT EXISTS `{dataset}` OPTIONS(location = "{location}")').result()

    tables = [
        ("teams", "EPL teams payload snapshots from BallDontLie v2"),
        ("players", "EPL players payload snapshots from BallDontLie v2"),
        ("rosters", "EPL team roster payload snapshots from BallDontLie v2"),
        ("standings", "EPL standings payload snapshots from BallDontLie v2"),
        ("matches", "EPL matches payload snapshots from BallDontLie v2"),
        ("match_events", "EPL match events payload snapshots from BallDontLie v2"),
        ("match_lineups", "EPL match lineups payload snapshots from BallDontLie v2"),
    ]

    for table_name, description in tables:
        _ensure_table(client, dataset, table_name, description)


if __name__ == "__main__":
    main()
