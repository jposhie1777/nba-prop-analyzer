# ingest_espn_player_headshots.py

import os
import time
import requests
from datetime import datetime, timezone

from google.cloud import bigquery
from bq import get_bq_client

# ======================================================
# CONFIG (CLOUD RUN SAFE)
# ======================================================

ESPN_SEARCH_URL = "https://site.web.api.espn.com/apis/common/v3/search"

HEADSHOT_TEMPLATE = (
    "https://a.espncdn.com/i/headshots/nba/players/full/{id}.png"
)

REQUEST_DELAY_SEC = 0.5  # be polite to ESPN
TARGET_SEASON = 2025


# ======================================================
# PROJECT / CLIENT HELPERS
# ======================================================

def get_project_id() -> str:
    project = (
        os.environ.get("GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    if not project:
        raise RuntimeError("❌ GCP_PROJECT / GOOGLE_CLOUD_PROJECT not set")
    return project


def get_bq():
    return get_bq_client()


def get_player_lookup_table() -> str:
    return f"{get_project_id()}.nba_goat_data.player_lookup"


# ======================================================
# BIGQUERY READ (ACTIVE PLAYERS)
# ======================================================

def fetch_active_players():
    """
    Canonical source of players.
    This guarantees full coverage (no props dependency).
    """
    bq = get_bq()
    project = get_project_id()

    query = f"""
    SELECT
      player_id,
      name AS player_name
    FROM `{project}.nba_goat_data.active_players`
    WHERE season = @season
    """

    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "season", "INT64", TARGET_SEASON
                )
            ]
        ),
    )

    rows = job.result()
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
    ESPN does not expose a full NBA roster API.
    Name-based search is required.
    """
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
            espn_id = int(item["id"])
            return {
                "espn_player_id": espn_id,
                "espn_display_name": item.get("displayName"),
                "league": item.get("league"),
                "player_image_url": HEADSHOT_TEMPLATE.format(id=espn_id),
                "last_verified": datetime.now(timezone.utc),
                "source": "espn_search_v3",
            }

    return None


# ======================================================
# BIGQUERY UPSERT
# ======================================================

def upsert_player(player_id: int, player_name: str, espn_row: dict):
    """
    Upsert into player_lookup using canonical player_id.
    """
    bq = get_bq()
    table_id = get_player_lookup_table()

    query = f"""
    MERGE `{table_id}` t
    USING (
      SELECT
        @player_id AS player_id
    ) s
    ON t.player_id = s.player_id
    WHEN MATCHED THEN
      UPDATE SET
        player_name        = @player_name,
        espn_player_id     = @espn_player_id,
        espn_display_name  = @espn_display_name,
        player_image_url   = @player_image_url,
        league             = @league,
        last_verified      = @last_verified,
        source             = @source
    WHEN NOT MATCHED THEN
      INSERT (
        player_id,
        player_name,
        espn_player_id,
        espn_display_name,
        player_image_url,
        league,
        last_verified,
        source
      )
      VALUES (
        @player_id,
        @player_name,
        @espn_player_id,
        @espn_display_name,
        @player_image_url,
        @league,
        @last_verified,
        @source
      )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            bigquery.ScalarQueryParameter("player_name", "STRING", player_name),
            bigquery.ScalarQueryParameter(
                "espn_player_id", "INT64", espn_row["espn_player_id"]
            ),
            bigquery.ScalarQueryParameter(
                "espn_display_name", "STRING", espn_row["espn_display_name"]
            ),
            bigquery.ScalarQueryParameter(
                "player_image_url", "STRING", espn_row["player_image_url"]
            ),
            bigquery.ScalarQueryParameter(
                "league", "STRING", espn_row["league"]
            ),
            bigquery.ScalarQueryParameter(
                "last_verified", "TIMESTAMP", espn_row["last_verified"]
            ),
            bigquery.ScalarQueryParameter(
                "source", "STRING", espn_row["source"]
            ),
        ]
    )

    bq.query(query, job_config=job_config).result()


# ======================================================
# MAIN LOOP
# ======================================================

def main():
    players = fetch_active_players()
    print(f"📦 Loaded {len(players)} active players")

    for p in players:
        player_id = p["player_id"]
        name = p["player_name"]

        print(f"🔍 ESPN lookup: {name} (player_id={player_id})")

        espn_row = fetch_espn_player(name)
        if not espn_row:
            print(f"⚠️ No ESPN NBA match for {name}")
            continue

        upsert_player(player_id, name, espn_row)
        print(f"✅ {name} → ESPN ID {espn_row['espn_player_id']}")

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