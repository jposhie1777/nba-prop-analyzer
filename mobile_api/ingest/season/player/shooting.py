from mobile_api.ingest.season.player.general import run
from mobile_api.ingest.common.bq import get_bq_client

TABLE = "nba_goat_data.player_season_shooting"

TYPES = [
    "overall",
    "by_zone",
    "5ft_range",
]

def run(season: int, season_type: str):
    ingest_category(
        table=TABLE,
        category="shooting",
        types=TYPES,
        season=season,
        season_type=season_type,
    )