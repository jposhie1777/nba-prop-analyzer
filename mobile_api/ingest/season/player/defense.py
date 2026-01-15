from mobile_api.ingest.season.player.general import run
from mobile_api.ingest.common.bq import get_bq_client

TABLE = "nba_goat_data.player_season_defense"

TYPES = [
    "overall",
    "2_pointers",
    "3_pointers",
    "greater_than_15ft",
    "less_than_10ft",
    "less_than_6ft",
]

def run(season: int, season_type: str):
    ingest_category(
        table=TABLE,
        category="defense",
        types=TYPES,
        season=season,
        season_type=season_type,
    )