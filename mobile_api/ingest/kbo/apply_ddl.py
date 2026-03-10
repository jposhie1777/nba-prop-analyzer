from __future__ import annotations

import os

from google.cloud import bigquery


def _get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _dataset() -> str:
    return os.getenv("KBO_DATASET", "kbo_data")


def _location() -> str:
    return os.getenv("KBO_BQ_LOCATION", "US")


def main() -> None:
    client = _get_bq_client()
    dataset = _dataset()
    location = _location()

    client.query(f'CREATE SCHEMA IF NOT EXISTS `{dataset}` OPTIONS(location = "{location}")').result()

    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{dataset}.games` (
          ingested_at TIMESTAMP NOT NULL,
          ingest_run_id STRING,
          season INT64 NOT NULL,
          game_date DATE NOT NULL,
          game_type STRING,
          game_time STRING,
          away_team STRING,
          home_team STRING,
          away_runs INT64,
          home_runs INT64,
          outcome STRING,
          status STRING,
          location STRING,
          notes STRING,
          game_key STRING
        )
        PARTITION BY game_date
        CLUSTER BY season, away_team, home_team
        OPTIONS(description='KBO game-level history from DailySchedule.aspx')
        """
    ).result()

    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{dataset}.team_summary` (
          ingested_at TIMESTAMP NOT NULL,
          ingest_run_id STRING,
          season INT64 NOT NULL,
          team STRING NOT NULL,
          games_played INT64,
          wins INT64,
          losses INT64,
          ties INT64,
          runs_scored INT64,
          runs_allowed INT64,
          avg_runs_scored FLOAT64,
          avg_runs_allowed FLOAT64
        )
        PARTITION BY DATE(ingested_at)
        CLUSTER BY season, team
        OPTIONS(description='KBO team season summary derived from games table')
        """
    ).result()


if __name__ == "__main__":
    main()
