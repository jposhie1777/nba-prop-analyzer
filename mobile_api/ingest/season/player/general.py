from ingest.season.common import ingest_category

TABLE = "nba_goat_data.player_season_general"

TYPES = [
    "base",
    "advanced",
    "usage",
    "scoring",
    "defense",
    "misc",
]

def run(season: int, season_type: str):
    ingest_category(
        table=TABLE,
        category="general",
        types=TYPES,
        season=season,
        season_type=season_type,
    )