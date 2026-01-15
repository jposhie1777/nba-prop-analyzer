# ingest/season/run_all.py
from mobile_api.ingest.season.player.general import run as run_general
from mobile_api.ingest.season.player.clutch import run as run_clutch
from mobile_api.ingest.season.player.shooting import run as run_shooting
from mobile_api.ingest.season.player.defense import run as run_defense

def run_all(season: int, season_type: str = "regular"):
    print(f"🏀 ingesting season={season} type={season_type}")

    run_general(season, season_type)
    run_clutch(season, season_type)
    run_shooting(season, season_type)
    run_defense(season, season_type)

    print("✅ season ingestion complete")