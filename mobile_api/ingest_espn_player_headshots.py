# ingest_espn_player_headshots.py

import os
import time
import requests
from datetime import datetime, timezone

from google.cloud import bigquery
from bq import get_bq_client

# ======================================================
# CONFIG (LAZY + CLOUD-RUN SAFE)
# ======================================================

def get_project_id() -> str:
    project = (
        os.environ.get("GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    if not project:
        raise RuntimeError("❌ GCP_PROJECT / GOOGLE_CLOUD_PROJECT not set")
    return project


def get_table_id() -> str:
    return f"{get_project_id()}.nba_goat_data.player_lookup"


def get_bq():
    return get_bq_client()


ESPN_SEARCH_URL = "https://site.web.api.espn.com/apis/common/v3/search"

HEADSHOT_TEMPLATE = (
    "https://a.espncdn.com/i/headshots/nba/players/full/{id}.png"
)

REQUEST_DELAY_SEC = 0.5  # be polite to ESPN

# ======================================================
# BIGQUERY READ
# ======================================================

def fetch_players_from_bq():
    bq = get_bq()
    project = get_project_id()

    query = f"""
    SELECT DISTINCT player
    FROM `{project}.nba_goat_data.props_mobile_v1`
    WHERE player IS NOT NULL
    ORDER BY player
    """

    rows = bq.query(query).result()
    return [r.player for r in rows]

# ======================================================
# ESPN LOOKUP
# ======================================================

def fetch_espn_player(player_name: str):
    resp = requests.get(
        ESPN_SEARCH_URL,
        params={"query": player_name, "type": "player"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()

    for item in data.get("items", []):
        if (
            item.get("type") == "player"
            and item.get("league") == "nba"
            and item.get("sport") == "basketball"
        ):
            return {
                "player_name": player_name,
                "espn_player_id": int(item["id"]),
                "espn_display_name": item.get("displayName"),
                "league": item.get("league"),
                "player_image_url": HEADSHOT_TEMPLATE.format(id=item["id"]),
                "last_verified": datetime.now(timezone.utc).isoformat(),
                "source": "espn_search_v3",
            }

    return None

# ======================================================
# BIGQUERY UPSERT
# ======================================================

def upsert_player(row: dict):
    bq = get_bq()
    table_id = get_table_id()

    query = f"""
    MERGE `{table_id}` t
    USING (SELECT @player_name AS player_name) s
    ON t.player_name = s.player_name
    WHEN MATCHED THEN
      UPDATE SET
        espn_player_id = @espn_player_id,
        player_image_url = @player_image_url,
        espn_display_name = @espn_display_name,
        league = @league,
        last_verified = @last_verified,
        source = @source
    WHEN NOT MATCHED THEN
      INSERT (
        player_name,
        espn_player_id,
        player_image_url,
        espn_display_name,
        league,
        last_verified,
        source
      )
      VALUES (
        @player_name,
        @espn_player_id,
        @player_image_url,
        @espn_display_name,
        @league,
        @last_verified,
        @source
      )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player_name", "STRING", row["player_name"]),
            bigquery.ScalarQueryParameter("espn_player_id", "INT64", row["espn_player_id"]),
            bigquery.ScalarQueryParameter("player_image_url", "STRING", row["player_image_url"]),
            bigquery.ScalarQueryParameter("espn_display_name", "STRING", row["espn_display_name"]),
            bigquery.ScalarQueryParameter("league", "STRING", row["league"]),
            bigquery.ScalarQueryParameter("last_verified", "TIMESTAMP", row["last_verified"]),
            bigquery.ScalarQueryParameter("source", "STRING", row["source"]),
        ]
    )

    bq.query(query, job_config=job_config).result()

# ======================================================
# MAIN LOOP
# ======================================================

def main():
    players = fetch_players_from_bq()

    for name in players:
        print(f"🔍 Fetching ESPN ID for {name}")

        row = fetch_espn_player(name)
        if not row:
            print(f"⚠️ No NBA match found for {name}")
            continue

        upsert_player(row)
        print(f"✅ {name} → ESPN ID {row['espn_player_id']}")

        time.sleep(REQUEST_DELAY_SEC)

# ======================================================
# CLOUD RUN SAFE ENTRYPOINT
# ======================================================

def run_headshot_ingest():
    """
    Safe entrypoint for Cloud Run background threads
    """
    main()


if __name__ == "__main__":
    main()