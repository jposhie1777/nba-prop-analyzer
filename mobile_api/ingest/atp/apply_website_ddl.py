from __future__ import annotations

import os
from pathlib import Path

from google.cloud import bigquery


def _client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _dataset() -> str:
    return os.getenv("ATP_DATASET", "atp_data")


def _location() -> str:
    return os.getenv("ATP_BQ_LOCATION", "US")


def main() -> None:
    client = _client()
    dataset = _dataset()
    location = _location()
    client.query(f'CREATE SCHEMA IF NOT EXISTS `{dataset}` OPTIONS(location = "{location}")').result()

    ddl_path = Path(__file__).with_name("website_ddl.sql")
    sql = ddl_path.read_text()
    sql = sql.replace("`atp_data.", f"`{dataset}.")

    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        client.query(stmt).result()


if __name__ == "__main__":
    main()
