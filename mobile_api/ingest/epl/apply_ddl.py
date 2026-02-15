from __future__ import annotations

import os

from google.cloud import bigquery


def _get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _dataset() -> str:
    return os.getenv("EPL_DATASET", "epl_data")


def _location() -> str:
    return os.getenv("EPL_BQ_LOCATION", "US")


def _table_ddl(dataset: str, table_name: str, description: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{dataset}.{table_name}` (
      ingested_at TIMESTAMP NOT NULL,
      season INT64 NOT NULL,
      entity_id STRING,
      payload JSON
    )
    PARTITION BY DATE(ingested_at)
    CLUSTER BY season, entity_id
    OPTIONS (description = '{description}')
    """


def main() -> None:
    client = _get_bq_client()
    dataset = _dataset()
    location = _location()

    statements = [
        f'CREATE SCHEMA IF NOT EXISTS `{dataset}` OPTIONS(location = "{location}")',
        _table_ddl(dataset, "teams", "EPL teams payload snapshots from BallDontLie v2"),
        _table_ddl(dataset, "players", "EPL players payload snapshots from BallDontLie v2"),
        _table_ddl(dataset, "rosters", "EPL team roster payload snapshots from BallDontLie v2"),
        _table_ddl(dataset, "standings", "EPL standings payload snapshots from BallDontLie v2"),
        _table_ddl(dataset, "matches", "EPL matches payload snapshots from BallDontLie v2"),
        _table_ddl(dataset, "match_events", "EPL match events payload snapshots from BallDontLie v2"),
        _table_ddl(dataset, "match_lineups", "EPL match lineups payload snapshots from BallDontLie v2"),
    ]

    for statement in statements:
        client.query(statement).result()


if __name__ == "__main__":
    main()
