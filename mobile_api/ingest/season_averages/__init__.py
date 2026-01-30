# mobile_api/ingest/season_averages/__init__.py
"""
Season Averages Ingestion Module

Public API:
    - ingest_current_season(): Ingest current season (daily job)
    - ingest_all_season_averages(): Ingest both player and team for a season
    - ingest_player_season_averages(): Ingest player averages only
    - ingest_team_season_averages(): Ingest team averages only
    - get_current_season(): Get current NBA season year
"""

from .ingest import (
    ingest_current_season,
    ingest_all_season_averages,
    ingest_player_season_averages,
    ingest_team_season_averages,
    get_current_season,
    PLAYER_CATEGORY_TYPES,
    TEAM_CATEGORY_TYPES,
)

from .routes import router

__all__ = [
    "ingest_current_season",
    "ingest_all_season_averages",
    "ingest_player_season_averages",
    "ingest_team_season_averages",
    "get_current_season",
    "PLAYER_CATEGORY_TYPES",
    "TEAM_CATEGORY_TYPES",
    "router",
]
