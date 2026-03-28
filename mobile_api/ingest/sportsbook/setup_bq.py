"""
Create BigQuery tables for raw FanDuel and DraftKings soccer market data.

Usage:
    python -m mobile_api.ingest.sportsbook.setup_bq [--project PROJECT]

The tables live in the existing 'oddspedia' dataset alongside EPL/MLS odds.
"""

from __future__ import annotations

import argparse
import os

from google.cloud import bigquery

DATASET = "oddspedia"
LOCATION = "US"

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

FANDUEL_SCHEMA = [
    bigquery.SchemaField("scraped_at",     "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("league",         "STRING",    mode="REQUIRED"),  # EPL | MLS
    bigquery.SchemaField("event_id",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("home_team",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("away_team",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("event_start",    "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("market_id",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("market_name",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("selection_id",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("selection_name", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("outcome_side",   "STRING",    mode="NULLABLE"),  # home|away|draw
    bigquery.SchemaField("handicap",       "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("odds_decimal",   "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("odds_american",  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("deep_link",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("raw_response",   "STRING",    mode="NULLABLE"),  # full JSON
]

DRAFTKINGS_SCHEMA = [
    bigquery.SchemaField("scraped_at",      "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("league",          "STRING",    mode="REQUIRED"),  # EPL | MLS
    bigquery.SchemaField("event_id",        "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("home_team",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("away_team",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("event_start",     "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("offer_id",        "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("category_id",     "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("subcategory_id",  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("outcome_id",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("outcome_label",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("outcome_line",    "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("odds_american",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("odds_decimal",    "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("deep_link",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("raw_response",    "STRING",    mode="NULLABLE"),  # full JSON
]

TABLES = {
    "raw_fanduel_soccer_markets":   FANDUEL_SCHEMA,
    "raw_draftkings_soccer_markets": DRAFTKINGS_SCHEMA,
}


def ensure_tables(project: str) -> None:
    client = bigquery.Client(project=project)
    dataset_ref = client.dataset(DATASET)

    for table_name, schema in TABLES.items():
        table_ref = dataset_ref.table(table_name)
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="scraped_at",
        )
        try:
            client.create_table(table)
            print(f"Created table {DATASET}.{table_name}")
        except Exception as exc:
            if "Already Exists" in str(exc):
                print(f"Table {DATASET}.{table_name} already exists — skipping")
            else:
                raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Create sportsbook BQ tables")
    parser.add_argument(
        "--project",
        default=os.environ.get("GCP_PROJECT", "graphite-flare-477419-h7"),
    )
    args = parser.parse_args()
    ensure_tables(args.project)
    print("Done.")


if __name__ == "__main__":
    main()
