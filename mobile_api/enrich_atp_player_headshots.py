# enrich_atp_player_headshots.py
# ESPN-based headshot enrichment for ATP tennis players.
# Modelled on ingest_espn_player_headshots.py (NBA version).

import os
import time
import requests
from datetime import datetime, timezone

from google.cloud import bigquery
from bq import get_bq_client

# ======================================================
# CONFIG
# ======================================================

ESPN_SEARCH_URL = "https://site.web.api.espn.com/apis/common/v3/search"

HEADSHOT_TEMPLATE = (
    "https://a.espncdn.com/i/headshots/tennis/players/full/{id}.png"
)

REQUEST_DELAY_SEC = 0.5  # be polite to ESPN


# ======================================================
# PROJECT / CLIENT HELPERS
# ======================================================

def get_project_id() -> str:
    project = (
        os.environ.get("GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    if not project:
        raise RuntimeError("GCP_PROJECT / GOOGLE_CLOUD_PROJECT not set")
    return project


def get_bq():
    return get_bq_client()


def get_player_lookup_table() -> str:
    return f"{get_project_id()}.atp_data.player_lookup"


# ======================================================
# BIGQUERY READ (ATP PLAYERS — DEDUPED)
# ======================================================

def fetch_atp_players():
    """
    Pull the latest row per player_id from atp_data.players.
    """
    bq = get_bq()
    project = get_project_id()

    query = f"""
    SELECT
      player_id,
      ANY_VALUE(full_name) AS player_name
    FROM `{project}.atp_data.players`
    GROUP BY player_id
    """

    rows = bq.query(query).result()
    return [
        {
            "player_id": r.player_id,
            "player_name": r.player_name,
        }
        for r in rows
    ]


# ======================================================
# ESPN LOOKUP
# ======================================================

def fetch_espn_player(player_name: str):
    """
    Name-based search against the ESPN common search API.
    Filters to tennis players only.

    ESPN v3 search returns: { "results": [ { "items": [ ... ] } ] }
    Each item has id, displayName, type, sport, league, etc.
    """
    resp = requests.get(
        ESPN_SEARCH_URL,
        params={"query": player_name, "limit": 10},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()

    # ESPN nests items inside results[0].items
    items = []
    for result_group in data.get("results", []):
        items.extend(result_group.get("items", []))
    # Fallback: also check top-level items (legacy format)
    items.extend(data.get("items", []))

    for item in items:
        if (
            item.get("type") == "player"
            and item.get("sport") == "tennis"
        ):
            espn_id = int(item["id"])
            return {
                "espn_player_id": espn_id,
                "espn_display_name": item.get("displayName"),
                "player_image_url": HEADSHOT_TEMPLATE.format(id=espn_id),
                "last_verified": datetime.now(timezone.utc),
                "source": "espn_search_v3",
            }

    return None


# ======================================================
# BIGQUERY UPSERT
# ======================================================

PLAYER_LOOKUP_SCHEMA = [
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("espn_player_id", "INT64"),
    bigquery.SchemaField("espn_display_name", "STRING"),
    bigquery.SchemaField("player_image_url", "STRING"),
    bigquery.SchemaField("last_verified", "TIMESTAMP"),
    bigquery.SchemaField("source", "STRING"),
]


def ensure_player_lookup_table():
    """Create atp_data.player_lookup if it doesn't exist."""
    bq = get_bq()
    table_id = get_player_lookup_table()

    try:
        bq.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=PLAYER_LOOKUP_SCHEMA)
        bq.create_table(table)
        print(f"Created table {table_id}")


def upsert_player(player_id: int, player_name: str, espn_row: dict):
    """
    Upsert into atp_data.player_lookup using canonical player_id.
    """
    bq = get_bq()
    table_id = get_player_lookup_table()

    query = f"""
    MERGE `{table_id}` t
    USING (
      SELECT @player_id AS player_id
    ) s
    ON t.player_id = s.player_id
    WHEN MATCHED THEN
      UPDATE SET
        player_name        = @player_name,
        espn_player_id     = @espn_player_id,
        espn_display_name  = @espn_display_name,
        player_image_url   = @player_image_url,
        last_verified      = @last_verified,
        source             = @source
    WHEN NOT MATCHED THEN
      INSERT (
        player_id,
        player_name,
        espn_player_id,
        espn_display_name,
        player_image_url,
        last_verified,
        source
      )
      VALUES (
        @player_id,
        @player_name,
        @espn_player_id,
        @espn_display_name,
        @player_image_url,
        @last_verified,
        @source
      )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            bigquery.ScalarQueryParameter("player_name", "STRING", player_name),
            bigquery.ScalarQueryParameter("espn_player_id", "INT64", espn_row["espn_player_id"]),
            bigquery.ScalarQueryParameter("espn_display_name", "STRING", espn_row["espn_display_name"]),
            bigquery.ScalarQueryParameter("player_image_url", "STRING", espn_row["player_image_url"]),
            bigquery.ScalarQueryParameter("last_verified", "TIMESTAMP", espn_row["last_verified"]),
            bigquery.ScalarQueryParameter("source", "STRING", espn_row["source"]),
        ]
    )

    bq.query(query, job_config=job_config).result()


# ======================================================
# MAIN LOOP
# ======================================================

def main():
    ensure_player_lookup_table()

    players = fetch_atp_players()
    print(f"Loaded {len(players)} ATP players")

    seen = set()
    matched = 0
    missed = 0

    for p in players:
        player_id = p["player_id"]

        if player_id in seen:
            continue
        seen.add(player_id)

        name = p["player_name"]
        print(f"ESPN lookup: {name} (player_id={player_id})")

        espn_row = fetch_espn_player(name)
        if not espn_row:
            print(f"  No ESPN tennis match for {name}")
            missed += 1
            time.sleep(REQUEST_DELAY_SEC)
            continue

        upsert_player(player_id, name, espn_row)
        print(f"  {name} -> ESPN ID {espn_row['espn_player_id']}")
        matched += 1

        time.sleep(REQUEST_DELAY_SEC)

    print(f"Done. matched={matched}, missed={missed}, total={len(seen)}")


# ======================================================
# CLOUD RUN SAFE ENTRYPOINT
# ======================================================

def run_atp_headshot_ingest():
    """Safe entrypoint for Cloud Run background threads."""
    main()


if __name__ == "__main__":
    main()
