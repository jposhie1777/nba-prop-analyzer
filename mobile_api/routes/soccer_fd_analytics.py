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


def _logo_url(league: str, team_name: str) -> Optional[str]:
    """Return an ESPN CDN logo URL for a soccer team. Falls back to None."""
    if not team_name:
        return None
    # ESPN team logo IDs — mapped from FanDuel team names
    key = team_name.lower().strip()
    espn_id = _TEAM_ESPN_ID.get(key)
    if espn_id:
        return f"https://a.espncdn.com/i/teamlogos/soccer/500/{espn_id}.png"
    return None


# FanDuel team name (lowercased) → ESPN team logo ID
_TEAM_ESPN_ID: Dict[str, str] = {
    # MLS
    "atlanta utd": "18418", "atlanta united": "18418",
    "austin fc": "20906", "austin": "20906",
    "cf montreal": "9720", "cf montréal": "9720",
    "charlotte fc": "21300", "charlotte": "21300",
    "chicago fire": "182", "chicago": "182",
    "colorado": "184", "colorado rapids": "184",
    "columbus": "183", "columbus crew": "183",
    "d.c. united": "193", "dc united": "193",
    "fc cincinnati": "18267", "cincinnati": "18267",
    "fc dallas": "185", "dallas": "185",
    "houston dynamo": "6077", "houston": "6077",
    "inter miami cf": "20232", "inter miami": "20232", "miami": "20232",
    "la galaxy": "187",
    "lafc": "18966", "los angeles fc": "18966",
    "minnesota utd": "17362", "minnesota united": "17362", "minnesota": "17362",
    "nashville sc": "18986", "nashville": "18986",
    "new england": "189", "new england revolution": "189",
    "new york city fc": "17606", "nycfc": "17606",
    "new york red bulls": "190", "ny red bulls": "190",
    "orlando city": "12011", "orlando": "12011",
    "philadelphia": "10739", "philadelphia union": "10739",
    "portland timbers": "9723", "portland": "9723",
    "real salt lake": "4771", "salt lake": "4771",
    "san diego fc": "22529", "san diego": "22529",
    "san jose": "191", "san jose earthquakes": "191",
    "seattle sounders": "9726", "seattle": "9726",
    "kansas city": "186", "sporting kc": "186", "sporting kansas city": "186",
    "st louis city sc": "21812", "st. louis city sc": "21812", "st louis": "21812",
    "toronto fc": "7318", "toronto": "7318",
    "vancouver whitecaps": "9727", "vancouver": "9727",
    # EPL
    "arsenal": "359", "arsenal fc": "359",
    "aston villa": "362",
    "bournemouth": "349", "afc bournemouth": "349",
    "brentford": "337", "brentford fc": "337",
    "brighton": "331", "brighton & hove albion": "331",
    "chelsea": "363", "chelsea fc": "363",
    "crystal palace": "384",
    "everton": "368", "everton fc": "368",
    "fulham": "370", "fulham fc": "370",
    "ipswich": "373", "ipswich town": "373",
    "leicester": "375", "leicester city": "375",
    "liverpool": "364", "liverpool fc": "364",
    "man city": "382", "manchester city": "382",
    "man utd": "360", "manchester utd": "360", "manchester united": "360",
    "newcastle": "361", "newcastle utd": "361", "newcastle united": "361",
    "nottingham forest": "393", "nott'm forest": "393",
    "southampton": "376",
    "tottenham": "367", "tottenham hotspur": "367", "spurs": "367",
    "west ham": "371", "west ham utd": "371", "west ham united": "371",
    "wolves": "380", "wolverhampton": "380", "wolverhampton wanderers": "380",
}


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
