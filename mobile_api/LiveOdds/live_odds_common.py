# live_odds_common.py
import os
import requests
from google.cloud import bigquery

# ==================================================
# Live Odds Constants (SINGLE SOURCE OF TRUTH)
# ==================================================

BDL_V2 = "https://api.balldontlie.io/v2"
TIMEOUT_SEC = 30

# --------------------------------------------------
# Allowed sportsbooks (LIVE ONLY)
# --------------------------------------------------
LIVE_ODDS_BOOKS = {
    "draftkings",
    "fanduel",
}

# --------------------------------------------------
# Allowed LIVE player prop markets
# --------------------------------------------------
LIVE_PLAYER_PROP_MARKETS = {
    "points",
    "assists",
    "rebounds",
    "three_pointers_made",
}

# Optional display aliases (for flattening / UI later)
LIVE_PLAYER_PROP_ALIASES = {
    "points": "PTS",
    "assists": "AST",
    "rebounds": "REB",
    "three_pointers_made": "3PM",
}

def get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()

def normalize_book(book: str | None) -> str | None:
    if not book:
        return None
    return book.strip().lower().replace("-", "").replace("_", "")
    
def require_api_key() -> str:
    key = os.getenv("BALLDONTLIE_API_KEY")
    if not key:
        raise RuntimeError("BALLDONTLIE_API_KEY not set")
    return key

def fetch_live_game_ids() -> list[int]:
    client = get_bq_client()

    rows = list(
        client.query(
            """
            SELECT DISTINCT
              CAST(JSON_VALUE(g, '$.id') AS INT64) AS game_id
            FROM `graphite-flare-477419-h7.nba_live.box_scores_raw`,
                 UNNEST(JSON_QUERY_ARRAY(payload, '$.data')) AS g
            WHERE
              JSON_VALUE(g, '$.time') IS NOT NULL
              AND JSON_VALUE(g, '$.time') != 'Final'
              AND JSON_VALUE(g, '$.period') IS NOT NULL
            """
        ).result()
    )

    game_ids = [r.game_id for r in rows if r.game_id is not None]

    print("🟢 LIVE GAME IDS (box_scores_raw):", game_ids)

    return game_ids