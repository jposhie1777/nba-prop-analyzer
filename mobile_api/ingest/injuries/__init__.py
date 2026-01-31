"""
Injuries ingestion and WOWY analysis module.

Provides:
- Injury data ingestion from BallDontLie API
- WOWY (With Or Without You) teammate impact analysis
- API routes for injuries and WOWY tabs
"""

from .ingest import ingest_injuries, get_current_injuries
from .wowy import analyze_wowy_for_player, get_wowy_for_injured_players, get_top_beneficiaries
from .routes import router

__all__ = [
    "ingest_injuries",
    "get_current_injuries",
    "analyze_wowy_for_player",
    "get_wowy_for_injured_players",
    "get_top_beneficiaries",
    "router",
]
