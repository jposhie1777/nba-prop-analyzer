import os
import time

import requests
from google.cloud import bigquery

# ==============================
# CONFIG
# ==============================
PROJECT_ID = os.getenv("PGA_PROJECT_ID", "graphite-flare-477419-h7")
DATASET = os.getenv("PGA_DATASET", "pga_data")
TABLE = os.getenv("PGA_TABLE", "active_players")

ESPN_SEARCH_URL = "https://site.web.api.espn.com/apis/common/v3/search"

client = bigquery.Client(project=PROJECT_ID)

def fetch_players_missing_headshots():
    query = f"""
    SELECT
      player_id,
      first_name,
      last_name,
      display_name
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE espn_headshot_url IS NULL
       OR espn_headshot_url = ""
    """
    return client.query(query).result()

# ==============================
# ESPN SEARCH
# ==============================
def find_espn_athlete(full_name: str):
    params = {
        "query": full_name,
        "limit": 5,
        "type": "athlete",
        "sport": "golf"
    }

    r = requests.get(ESPN_SEARCH_URL, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    for item in data.get("results", []):
        for entry in item.get("contents", []):
            athlete = entry.get("athlete")
            if athlete and athlete.get("displayName"):
                return athlete["id"], athlete["displayName"]

    return None, None


def main():
    updates = []
    players = fetch_players_missing_headshots()

    for row in players:
        name = row.display_name or f"{row.first_name} {row.last_name}"

        try:
            espn_id, espn_name = find_espn_athlete(name)
            if not espn_id:
                print(f"❌ No ESPN match for {name}")
                continue

            headshot_url = (
                f"https://a.espncdn.com/i/headshots/golf/players/full/{espn_id}.png"
            )

            updates.append(
                {
                    "player_id": row.player_id,
                    "espn_player_id": int(espn_id),
                    "espn_headshot_url": headshot_url,
                }
            )

            print(f"✅ {name} → {headshot_url}")
            time.sleep(0.2)  # be polite

        except Exception as e:
            print(f"⚠️ Error for {name}: {e}")

    if not updates:
        print("ℹ️ No missing headshots found.")
        return

    temp_table = f"{PROJECT_ID}.{DATASET}.tmp_player_headshots"

    job = client.load_table_from_json(
        updates,
        temp_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("player_id", "INTEGER"),
                bigquery.SchemaField("espn_player_id", "INTEGER"),
                bigquery.SchemaField("espn_headshot_url", "STRING"),
            ],
        ),
    )
    job.result()

    merge_sql = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{TABLE}` t
    USING `{temp_table}` s
    ON t.player_id = s.player_id
    WHEN MATCHED THEN
      UPDATE SET
        espn_player_id = s.espn_player_id,
        espn_headshot_url = s.espn_headshot_url
    """

    client.query(merge_sql).result()
    print("🎯 BigQuery updated successfully")


if __name__ == "__main__":
    main()
