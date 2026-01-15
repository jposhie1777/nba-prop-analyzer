from ingest.season.common import ingest_category

TABLE = "nba_goat_data.player_season_clutch"

TYPES = [
    "base",
    "advanced",
    "usage",
    "scoring",
    "misc",
]

def run(season: int, season_type: str):
    ingest_category(
        table=TABLE,
        category="clutch",
        types=TYPES,
        season=season,
        season_type=season_type,
    )