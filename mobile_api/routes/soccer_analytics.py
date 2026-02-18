from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client
from ingest.sheets.sync_soccer_odds_to_bq import sync_soccer_odds_to_bq

router = APIRouter(tags=["Soccer"])

SOCCER_ODDS_TABLE = os.getenv("SOCCER_ODDS_BQ_TABLE", "soccer_data.odds_lines")


@router.post("/ingest/soccer/odds-from-sheets")
def ingest_soccer_odds_from_sheets() -> Dict[str, Any]:
    return sync_soccer_odds_to_bq()


@router.get("/soccer/odds-board")
def soccer_odds_board(
    league: Optional[str] = Query(default=None, description="EPL, LaLiga, or MLS"),
    market: Optional[str] = Query(default=None),
    bookmaker: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    filters: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit)
    ]

    if league:
        filters.append("league = @league")
        params.append(bigquery.ScalarQueryParameter("league", "STRING", league))
    if market:
        filters.append("market = @market")
        params.append(bigquery.ScalarQueryParameter("market", "STRING", market))
    if bookmaker:
        filters.append("bookmaker = @bookmaker")
        params.append(bigquery.ScalarQueryParameter("bookmaker", "STRING", bookmaker))

    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""

    sql = f"""
    SELECT
      league,
      game,
      start_time_et,
      home_team,
      away_team,
      bookmaker,
      market,
      outcome,
      line,
      price,
      ingested_at
    FROM `{SOCCER_ODDS_TABLE}`
    {where_sql}
    ORDER BY start_time_et ASC, league, game, bookmaker, market, outcome
    LIMIT @limit
    """

    client = get_bq_client()
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return [dict(row) for row in job.result()]
