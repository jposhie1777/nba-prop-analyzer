# ingest/season/team/run_all.py

from mobile_api.ingest.season.team.common import ingest_team_category
from mobile_api.ingest.season.team.types import TEAM_CATEGORIES

TABLE = "nba_goat_data.team_season_averages"

def run_all(season: int, season_type: str = "regular"):
    print(f"🏀 ingesting TEAM season={season} type={season_type}")

    for category, types in TEAM_CATEGORIES.items():
        if types is None:
            ingest_team_category(
                table=TABLE,
                category=category,
                season=season,
                season_type=season_type,
            )
        else:
            for t in types:
                ingest_team_category(
                    table=TABLE,
                    category=category,
                    stat_type=t,
                    season=season,
                    season_type=season_type,
                )

    print("✅ team season ingestion complete")
