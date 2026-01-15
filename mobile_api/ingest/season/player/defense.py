from ingest.season.common import ingest_category

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