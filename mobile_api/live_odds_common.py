import os
import requests
from google.cloud import bigquery

BDL_V2 = "https://api.balldontlie.io/v2"
TIMEOUT_SEC = 15

def get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()

def require_api_key() -> str:
    key = os.getenv("BALLDONTLIE_API_KEY")
    if not key:
        raise RuntimeError("BALLDONTLIE_API_KEY not set")
    return key

def fetch_live_game_ids() -> list[int]:
    """
    Authoritative LIVE gate.
    """
    client = get_bq_client()

    rows = client.query(
        """
        SELECT DISTINCT game_id
        FROM `graphite-flare-477419-h7.nba_live.live_games`
        WHERE state = 'LIVE'
        """
    ).result()

    return [r.game_id for r in rows]