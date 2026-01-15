# ingest/season/league/hustle
import json
from mobile_api.ingest.common.bq import get_bq_client
from mobile_api.ingest.common.http import get
from mobile_api.ingest.common.players import get_active_player_ids
from mobile_api.ingest.common.logging import now_ts

TABLE = "nba_goat_data.league_season_hustle"
URL = "https://api.balldontlie.io/nba/v1/season_averages/hustle"

def run(season: int, season_type: str):
    print("⚠️ Hustle season averages not supported by BDL — skipping")
    return 0