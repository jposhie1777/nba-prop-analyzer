# mobile_api/ingest/game_advanced_stats/__init__.py
"""
Game Advanced Stats V2 Ingest Module

Fetches comprehensive advanced stats from Balldontlie API v2 endpoint.
Includes hustle stats, tracking data, and per-period breakdowns.

Usage:
    from mobile_api.ingest.game_advanced_stats import (
        ingest_yesterday,
        ingest_date,
        backfill_season,
    )
"""

from .ingest import (
    ingest_yesterday,
    ingest_date,
    ingest_date_range,
    backfill_season,
    fetch_advanced_stats_for_date,
)

__all__ = [
    "ingest_yesterday",
    "ingest_date",
    "ingest_date_range",
    "backfill_season",
    "fetch_advanced_stats_for_date",
]
