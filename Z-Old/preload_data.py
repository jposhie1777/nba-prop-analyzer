from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd, os, json

print("🔄 Preloading BigQuery data...")
PROJECT_ID = os.getenv("PROJECT_ID")
GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT")

creds_dict = json.loads(GCP_SERVICE_ACCOUNT)
creds = service_account.Credentials.from_service_account_info(creds_dict)
bq = bigquery.Client(project=PROJECT_ID, credentials=creds)

CACHE_DIR = "/data"
os.makedirs(CACHE_DIR, exist_ok=True)
PLAYER_STATS_CACHE = f"{CACHE_DIR}/player_stats.parquet"

query = f"""
SELECT player AS player_name, team, DATE(game_date) AS game_date, CAST(pts AS FLOAT64) AS pts
FROM `{PROJECT_ID}.nba_data_2024_2025.player_stats`
LIMIT 5000
"""
df = bq.query(query).to_dataframe()
df.to_parquet(PLAYER_STATS_CACHE)
print("✅ Player stats preloaded and cached!")
