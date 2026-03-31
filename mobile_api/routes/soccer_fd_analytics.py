from __future__ import annotations

import os
from collections import defaultdict
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(tags=["Soccer FD Analytics"])

EPL_BETTING_ANALYTICS_TABLE = os.getenv(
    "EPL_BETTING_ANALYTICS_TABLE",
    "graphite-flare-477419-h7.soccer_data.epl_betting_analytics",
)
MLS_BETTING_ANALYTICS_TABLE = os.getenv(
    "MLS_BETTING_ANALYTICS_TABLE",
    "graphite-flare-477419-h7.soccer_data.mls_betting_analytics",
)

_LEAGUE_TABLE_MAP: Dict[str, str] = {
    "EPL": EPL_BETTING_ANALYTICS_TABLE,
    "MLS": MLS_BETTING_ANALYTICS_TABLE,
}


def _query(
    sql: str,
    params: List[bigquery.ScalarQueryParameter],
    client: Optional[bigquery.Client] = None,
) -> List[Dict[str, Any]]:
    if client is None:
        client = get_bq_client()
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    )
    return [dict(row) for row in job.result()]


_LOGO_BASE = "https://raw.githubusercontent.com/luukhopman/football-logos/master/logos"
_LEAGUE_LOGO_PATH: Dict[str, str] = {
    "EPL": "England%20-%20Premier%20League",
    "MLS": "USA%20-%20MLS",
}


def _logo_url(league: str, team_name: str) -> Optional[str]:
    if not team_name:
        return None
    path = _LEAGUE_LOGO_PATH.get(league.upper())
    if not path:
        return None
    return f"{_LOGO_BASE}/{path}/{quote(team_name)}.png"


@router.get("/soccer/games")
def soccer_games() -> List[Dict[str, Any]]:
    """
    Returns upcoming EPL + MLS games for the next 8 days, ordered by kickoff time.
    Each row is one game (deduplicated via GROUP BY ALL from the analytics table).
    """
    sql = f"""
    SELECT
      event_date,
      event_start_ts,
      league,
      fd_event_id,
      game,
      home_team,
      away_team,
      model_expected_total_goals,
      model_xg_total,
      model_btts_probability,
      model_expected_corners,
      model_expected_cards,
      model_home_win_form_edge,
      analytics_updated_at
    FROM `{EPL_BETTING_ANALYTICS_TABLE}`
    WHERE event_date >= CURRENT_DATE()
      AND event_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 8 DAY)
    GROUP BY ALL

    UNION ALL

    SELECT
      event_date,
      event_start_ts,
      league,
      fd_event_id,
      game,
      home_team,
      away_team,
      model_expected_total_goals,
      model_xg_total,
      model_btts_probability,
      model_expected_corners,
      model_expected_cards,
      model_home_win_form_edge,
      analytics_updated_at
    FROM `{MLS_BETTING_ANALYTICS_TABLE}`
    WHERE event_date >= CURRENT_DATE()
      AND event_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 8 DAY)
    GROUP BY ALL

    ORDER BY event_start_ts ASC
    """
    rows = _query(sql, [])
    result = []
    for r in rows:
        row = _serialise_row(r)
        row["home_logo"] = _logo_url(row.get("league", ""), row.get("home_team", ""))
        row["away_logo"] = _logo_url(row.get("league", ""), row.get("away_team", ""))
        result.append(row)
    return result


@router.get("/soccer/analytics")
def soccer_analytics(
    league: str = Query(..., description="EPL or MLS"),
    event_id: str = Query(..., description="fd_event_id of the game"),
) -> Dict[str, Any]:
    """
    Returns full market analytics for a single game grouped by market.
    """
    league_upper = league.upper()
    table = _LEAGUE_TABLE_MAP.get(league_upper)
    if not table:
        raise HTTPException(status_code=400, detail=f"Unknown league: {league}. Must be EPL or MLS.")

    sql = f"""
    SELECT
      fd_event_id,
      game,
      home_team,
      away_team,
      league,
      event_start_ts,
      fd_market_id,
      market_name,
      market_type,
      fd_selection_id,
      selection_name,
      handicap,
      odds_american,
      odds_decimal,
      implied_probability,
      no_vig_probability,
      market_hold,
      is_best_price,
      fd_deep_link,
      fd_parlay_deep_link,
      model_expected_total_goals,
      model_xg_total,
      model_btts_probability,
      model_expected_corners,
      model_expected_cards,
      model_home_win_form_edge,
      model_total_line_edge,
      model_edge_tier,
      home_l5_goals_pg,
      away_l5_goals_pg,
      home_l5_goals_allowed_pg,
      away_l5_goals_allowed_pg,
      home_l5_win_rate,
      away_l5_win_rate,
      home_l5_draw_rate,
      away_l5_draw_rate,
      home_l5_btts_rate,
      away_l5_btts_rate,
      home_l5_corners_pg,
      away_l5_corners_pg,
      home_l5_cards_pg,
      away_l5_cards_pg,
      home_season_goals_pg,
      away_season_goals_pg,
      home_season_win_rate,
      away_season_win_rate
    FROM `{table}`
    WHERE fd_event_id = @event_id
    ORDER BY market_type, market_name, odds_american DESC
    """

    params = [bigquery.ScalarQueryParameter("event_id", "STRING", event_id)]
    rows = _query(sql, params)

    if not rows:
        raise HTTPException(status_code=404, detail=f"No analytics found for event_id={event_id} league={league}")

    rows = [_serialise_row(r) for r in rows]
    first = rows[0]

    # Build model + form blocks from first row (same for all rows in a game)
    model = {
        "expected_total_goals": first.get("model_expected_total_goals"),
        "xg_total": first.get("model_xg_total"),
        "btts_probability": first.get("model_btts_probability"),
        "expected_corners": first.get("model_expected_corners"),
        "expected_cards": first.get("model_expected_cards"),
        "home_win_form_edge": first.get("model_home_win_form_edge"),
    }

    form = {
        "home": {
            "l5_goals_pg": first.get("home_l5_goals_pg"),
            "l5_goals_allowed_pg": first.get("home_l5_goals_allowed_pg"),
            "l5_win_rate": first.get("home_l5_win_rate"),
            "l5_draw_rate": first.get("home_l5_draw_rate"),
            "l5_btts_rate": first.get("home_l5_btts_rate"),
            "l5_corners_pg": first.get("home_l5_corners_pg"),
            "l5_cards_pg": first.get("home_l5_cards_pg"),
            "season_goals_pg": first.get("home_season_goals_pg"),
            "season_win_rate": first.get("home_season_win_rate"),
        },
        "away": {
            "l5_goals_pg": first.get("away_l5_goals_pg"),
            "l5_goals_allowed_pg": first.get("away_l5_goals_allowed_pg"),
            "l5_win_rate": first.get("away_l5_win_rate"),
            "l5_draw_rate": first.get("away_l5_draw_rate"),
            "l5_btts_rate": first.get("away_l5_btts_rate"),
            "l5_corners_pg": first.get("away_l5_corners_pg"),
            "l5_cards_pg": first.get("away_l5_cards_pg"),
            "season_goals_pg": first.get("away_season_goals_pg"),
            "season_win_rate": first.get("away_season_win_rate"),
        },
    }

    # Group selections by market
    markets_map: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        market_id = row.get("fd_market_id") or ""
        market_name = row.get("market_name") or ""
        market_type = row.get("market_type") or ""
        key = market_id or market_name

        if key not in markets_map:
            markets_map[key] = {
                "market_name": market_name,
                "market_type": market_type,
                "fd_market_id": market_id,
                "selections": [],
            }

        markets_map[key]["selections"].append(
            {
                "fd_selection_id": row.get("fd_selection_id"),
                "selection_name": row.get("selection_name"),
                "handicap": row.get("handicap"),
                "odds_american": row.get("odds_american"),
                "odds_decimal": row.get("odds_decimal"),
                "implied_probability": row.get("implied_probability"),
                "no_vig_probability": row.get("no_vig_probability"),
                "market_hold": row.get("market_hold"),
                "is_best_price": row.get("is_best_price"),
                "fd_deep_link": row.get("fd_deep_link"),
                "fd_parlay_deep_link": row.get("fd_parlay_deep_link"),
                "model_total_line_edge": row.get("model_total_line_edge"),
                "model_edge_tier": row.get("model_edge_tier"),
            }
        )

    return {
        "fd_event_id": first.get("fd_event_id"),
        "game": first.get("game"),
        "league": first.get("league"),
        "home_team": first.get("home_team"),
        "away_team": first.get("away_team"),
        "event_start_ts": first.get("event_start_ts"),
        "model": model,
        "form": form,
        "markets": list(markets_map.values()),
    }


def _serialise_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert BigQuery non-JSON-native types (DATE, TIMESTAMP, Decimal) to strings/floats."""
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out
