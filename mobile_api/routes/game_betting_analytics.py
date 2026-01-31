from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from google.cloud import bigquery
from pydantic import BaseModel

from bq import get_bq_client

router = APIRouter(prefix="/analytics", tags=["Analytics"])


class GameBettingAnalyticsRow(BaseModel):
    game_id: int
    game_date: date
    start_time_est: Optional[datetime] = None
    status: Optional[str] = None
    is_final: Optional[bool] = None
    home_team_abbr: str
    away_team_abbr: str
    home_score_final: Optional[int] = None
    away_score_final: Optional[int] = None
    home_moneyline: Optional[int] = None
    away_moneyline: Optional[int] = None
    spread_home: Optional[float] = None
    spread_away: Optional[float] = None
    total_line: Optional[float] = None
    home_win_pct_l10: Optional[float] = None
    away_win_pct_l10: Optional[float] = None
    home_ats_pct_l10: Optional[float] = None
    away_ats_pct_l10: Optional[float] = None
    home_over_pct_l10: Optional[float] = None
    away_over_pct_l10: Optional[float] = None
    home_avg_margin_l10: Optional[float] = None
    away_avg_margin_l10: Optional[float] = None


class GameBettingAnalyticsResponse(BaseModel):
    game_date: Optional[date]
    count: int
    games: List[Dict[str, Any]]


def implied_probability(odds: Optional[int]) -> Optional[float]:
    if odds is None:
        return None
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    serialized: Dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (datetime, date)):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value
    return serialized


def enrich_best_bet(row: Dict[str, Any]) -> Dict[str, Any]:
    home_win = row.get("home_win_pct_l10")
    away_win = row.get("away_win_pct_l10")
    home_ats = row.get("home_ats_pct_l10")
    away_ats = row.get("away_ats_pct_l10")
    home_over = row.get("home_over_pct_l10")
    away_over = row.get("away_over_pct_l10")

    home_ml = row.get("home_moneyline")
    away_ml = row.get("away_moneyline")
    spread_home = row.get("spread_home")
    total_line = row.get("total_line")

    home_ml_edge = None
    away_ml_edge = None
    if home_win is not None:
        home_ml_edge = home_win - (implied_probability(home_ml) or 0)
    if away_win is not None:
        away_ml_edge = away_win - (implied_probability(away_ml) or 0)

    spread_edge = None
    if home_ats is not None and away_ats is not None:
        spread_edge = home_ats - away_ats

    total_over_edge = None
    if home_over is not None and away_over is not None:
        total_over_edge = ((home_over + away_over) / 2) - 0.5

    candidates: List[Dict[str, Any]] = []
    if home_ml_edge is not None and away_ml_edge is not None:
        if abs(home_ml_edge) >= abs(away_ml_edge):
            candidates.append(
                {
                    "market": "MONEYLINE",
                    "side": "HOME",
                    "edge": home_ml_edge,
                    "detail": "Home win% vs implied odds",
                }
            )
        else:
            candidates.append(
                {
                    "market": "MONEYLINE",
                    "side": "AWAY",
                    "edge": away_ml_edge,
                    "detail": "Away win% vs implied odds",
                }
            )

    if spread_edge is not None and spread_home is not None:
        candidates.append(
            {
                "market": "SPREAD",
                "side": "HOME" if spread_edge >= 0 else "AWAY",
                "edge": spread_edge,
                "detail": "ATS L10 edge",
            }
        )

    if total_over_edge is not None and total_line is not None:
        candidates.append(
            {
                "market": "TOTAL",
                "side": "OVER" if total_over_edge >= 0 else "UNDER",
                "edge": total_over_edge,
                "detail": "O/U L10 trend",
            }
        )

    best = None
    if candidates:
        best = max(candidates, key=lambda item: abs(item["edge"]))

    row["best_bet_market"] = best["market"] if best else None
    row["best_bet_side"] = best["side"] if best else None
    row["best_bet_edge"] = best["edge"] if best else None
    row["best_bet_reason"] = best["detail"] if best else None
    return row


@router.get("/game-betting", response_model=GameBettingAnalyticsResponse)
def get_game_betting_analytics(
    game_date: Optional[date] = Query(None),
    include_final: bool = Query(False),
    limit: int = Query(30, ge=1, le=250),
):
    client = get_bq_client()

    query = """
    SELECT
      game_id,
      game_date,
      start_time_est,
      status,
      is_final,
      home_team_abbr,
      away_team_abbr,
      home_score_final,
      away_score_final,
      home_moneyline,
      away_moneyline,
      spread_home,
      spread_away,
      total_line,
      home_win_pct_l10,
      away_win_pct_l10,
      home_ats_pct_l10,
      away_ats_pct_l10,
      home_over_pct_l10,
      away_over_pct_l10,
      home_avg_margin_l10,
      away_avg_margin_l10
    FROM `nba_goa_data.v_game_betting_board`
    WHERE (@game_date IS NULL OR game_date = @game_date)
      AND (@include_final OR is_final IS NULL OR is_final = FALSE)
    ORDER BY game_date, start_time_est
    LIMIT @limit
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "DATE", game_date),
                bigquery.ScalarQueryParameter(
                    "include_final", "BOOL", include_final
                ),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ]
        ),
    )

    rows = [dict(row) for row in job.result()]
    enriched = [serialize_row(enrich_best_bet(row)) for row in rows]

    return {
        "game_date": game_date,
        "count": len(enriched),
        "games": enriched,
    }
