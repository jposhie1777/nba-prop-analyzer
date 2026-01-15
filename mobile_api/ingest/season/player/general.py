# ingest/season/player/general.py
from mobile_api.ingest.season.common import ingest_category
from mobile_api.ingest.common.bq import get_bq_client

TABLE = "nba_goat_data.player_season_general"

TYPES = [
    "base",
    "advanced",
    "usage",
    "scoring",
    "defense",
    "misc",
]

def run(season, season_type, player_ids, run_ts):
    ingest_category(
        table=TABLE,
        category="general",
        types=TYPES,
        season=season,
        season_type=season_type,
        player_ids=player_ids,
        run_ts=run_ts,
    )
