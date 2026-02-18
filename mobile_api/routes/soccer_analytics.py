from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client
from ingest.sheets.sync_soccer_odds_to_bq import sync_soccer_odds_to_bq

router = APIRouter(tags=["Soccer"])

SOCCER_ODDS_TABLE = os.getenv("SOCCER_ODDS_BQ_TABLE", "soccer_data.odds_lines")
SOCCER_BETTING_ANALYSIS_TABLE = os.getenv(
    "SOCCER_BETTING_ANALYSIS_BQ_TABLE", "soccer_data.betting_analysis"
)
ET_TZ = ZoneInfo("America/New_York")
OUTRIGHT_WINNER_MARKETS: Set[str] = {
    "h2h",
    "moneyline",
    "match_winner",
    "winner",
    "outright_winner",
}
# Aliases observed across odds feeds for alternate totals-style soccer markets.
ALT_TOTALS_MARKETS: Set[str] = {
    "alternate_totals",
    "alt_totals",
    "total_goals",
    "totals",
    "over_under",
}

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


def _american_to_decimal(odds: Optional[float]) -> Optional[float]:
    if odds is None or odds == 0:
        return None
    if odds > 0:
        return 1 + (odds / 100)
    return 1 + (100 / abs(odds))


def _american_to_implied_prob(odds: Optional[float]) -> Optional[float]:
    dec = _american_to_decimal(odds)
    if not dec or dec <= 0:
        return None
    return 1 / dec


def _normalize_market(market: Any) -> str:
    return str(market or "").strip().lower().replace("-", "_").replace(" ", "_")


def _market_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        row.get("league"),
        row.get("game"),
        row.get("start_time_et"),
        _normalize_market(row.get("market")),
        row.get("outcome"),
        row.get("line"),
    )


def _make_suggestion(row: Dict[str, Any]) -> Dict[str, Any]:
    market = _normalize_market(row.get("market"))
    outcome = str(row.get("outcome") or "")
    price = row.get("best_price")

    home_attack = float(row.get("home_season_gf") or 0)
    home_defense = float(row.get("home_season_ga") or 0)
    away_attack = float(row.get("away_season_gf") or 0)
    away_defense = float(row.get("away_season_ga") or 0)
    home_recent_attack = float(row.get("home_l10_gf") or 0)
    home_recent_defense = float(row.get("home_l10_ga") or 0)
    away_recent_attack = float(row.get("away_l10_gf") or 0)
    away_recent_defense = float(row.get("away_l10_ga") or 0)
    combined_gf = float(row.get("combined_season_gf") or 0)

    home_strength = (home_attack - home_defense) + 0.35 * (
        home_recent_attack - home_recent_defense
    )
    away_strength = (away_attack - away_defense) + 0.35 * (
        away_recent_attack - away_recent_defense
    )
    strength_gap = away_strength - home_strength

    implied_prob = _american_to_implied_prob(price) or 0.5
    confidence = implied_prob
    rationale = ""

    if market == "btts":
        if outcome.lower() == "yes":
            confidence = max(implied_prob, min(0.82, 0.42 + combined_gf * 0.12))
            rationale = (
                f"Combined GF profile ({combined_gf:.2f}) points to both attacks finding the net."
            )
        elif outcome.lower() == "no":
            confidence = max(implied_prob, min(0.82, 0.42 + max(0.0, 2.8 - combined_gf) * 0.14))
            rationale = (
                f"Lower scoring context ({combined_gf:.2f} combined GF) supports BTTS No."
            )
    elif market == "draw_no_bet":
        wants_away = outcome == row.get("away_team")
        favored_gap = strength_gap if wants_away else -strength_gap
        confidence = max(implied_prob, min(0.86, 0.5 + favored_gap * 0.2))
        rationale = (
            "Draw protection with team-strength edge "
            f"({favored_gap:+.2f} model gap)."
        )
    elif market == "double_chance":
        supports_away = row.get("away_team") and str(row.get("away_team")) in outcome
        supports_home = row.get("home_team") and str(row.get("home_team")) in outcome
        favored_gap = strength_gap if supports_away and not supports_home else -strength_gap
        confidence = max(implied_prob, min(0.9, 0.58 + abs(favored_gap) * 0.12))
        rationale = (
            "Double chance lowers variance while leaning into the stronger side profile."
        )
    elif market in OUTRIGHT_WINNER_MARKETS:
        supports_away = row.get("away_team") and str(row.get("away_team")) in outcome
        supports_home = row.get("home_team") and str(row.get("home_team")) in outcome
        if supports_away and not supports_home:
            favored_gap = strength_gap
        elif supports_home and not supports_away:
            favored_gap = -strength_gap
        else:
            favored_gap = 0.0
        confidence = max(implied_prob, min(0.86, 0.48 + favored_gap * 0.22))
        rationale = (
            "Outright winner priced from team-strength profile and recent form signal "
            f"({favored_gap:+.2f} model gap)."
        )
    elif market in ALT_TOTALS_MARKETS:
        is_over = outcome.lower().startswith("over")
        baseline_total = max(0.5, combined_gf)
        total_line = float(row.get("line") or baseline_total)
        line_gap = baseline_total - total_line
        direction_gap = line_gap if is_over else -line_gap
        confidence = max(implied_prob, min(0.88, 0.5 + direction_gap * 0.14))
        rationale = (
            "Alternate totals compares model scoring baseline to posted line "
            f"(baseline {baseline_total:.2f} vs line {total_line:.2f})."
        )
    else:
        rationale = "Market is priced as a lean from today's team form and season profile."

    confidence = max(0.01, min(0.99, confidence))
    edge = confidence - implied_prob
    return {
        **row,
        "market": market,
        "implied_prob": round(implied_prob, 4),
        "model_confidence": round(confidence, 4),
        "edge": round(edge, 4),
        "rationale": rationale,
        "recommended": edge >= 0.035,
    }


def _fetch_betting_analysis_rows(
    client: bigquery.Client,
    where_sql: str,
    params: List[bigquery.ScalarQueryParameter],
) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT
      league,
      game,
      start_time_et,
      home_team,
      away_team,
      market,
      outcome,
      line,
      ARRAY_AGG(bookmaker ORDER BY price DESC LIMIT 1)[OFFSET(0)] AS best_bookmaker,
      MAX(price) AS best_price,
      ANY_VALUE(home_season_gf) AS home_season_gf,
      ANY_VALUE(home_season_ga) AS home_season_ga,
      ANY_VALUE(home_l10_gf) AS home_l10_gf,
      ANY_VALUE(home_l10_ga) AS home_l10_ga,
      ANY_VALUE(away_season_gf) AS away_season_gf,
      ANY_VALUE(away_season_ga) AS away_season_ga,
      ANY_VALUE(away_l10_gf) AS away_l10_gf,
      ANY_VALUE(away_l10_ga) AS away_l10_ga,
      ANY_VALUE(combined_season_gf) AS combined_season_gf,
      ANY_VALUE(combined_season_cards) AS combined_season_cards
    FROM `{SOCCER_BETTING_ANALYSIS_TABLE}`
    WHERE {where_sql}
    GROUP BY league, game, start_time_et, home_team, away_team, market, outcome, line
    ORDER BY start_time_et ASC, league, game, market, outcome
    LIMIT @limit
    """
    return [
        dict(row)
        for row in client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=params),
        ).result()
    ]


def _fetch_supplemental_rows(
    client: bigquery.Client,
    where_sql: str,
    params: List[bigquery.ScalarQueryParameter],
) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT
      league,
      game,
      start_time_et,
      home_team,
      away_team,
      market,
      outcome,
      line,
      ARRAY_AGG(bookmaker ORDER BY price DESC LIMIT 1)[OFFSET(0)] AS best_bookmaker,
      MAX(price) AS best_price
    FROM `{SOCCER_ODDS_TABLE}`
    WHERE {where_sql}
      AND LOWER(REPLACE(REPLACE(market, '-', '_'), ' ', '_')) IN (
        'h2h',
        'moneyline',
        'match_winner',
        'winner',
        'outright_winner',
        'alternate_totals',
        'alt_totals',
        'total_goals',
        'totals',
        'over_under'
      )
    GROUP BY league, game, start_time_et, home_team, away_team, market, outcome, line
    ORDER BY start_time_et ASC, league, game, market, outcome
    LIMIT @limit
    """
    return [
        dict(row)
        for row in client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=params),
        ).result()
    ]


@router.get("/soccer/todays-betting-analysis")
def soccer_todays_betting_analysis(
    league: Optional[str] = Query(default=None, description="EPL, LaLiga, or MLS"),
    limit: int = Query(default=200, ge=1, le=1000),
    min_edge: float = Query(default=0.035, ge=-1.0, le=1.0),
) -> Dict[str, Any]:
    client = get_bq_client()
    today_et = datetime.now(tz=ET_TZ).date().isoformat()

    filters: List[str] = ["DATE(start_time_et, 'America/New_York') = @today_et"]
    params: List[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("today_et", "DATE", today_et),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]

    if league:
        filters.append("league = @league")
        params.append(bigquery.ScalarQueryParameter("league", "STRING", league))

    where_sql = " AND ".join(filters)

    analysis_rows = _fetch_betting_analysis_rows(client, where_sql, params)
    stats_by_game = {
        (row.get("league"), row.get("game"), row.get("start_time_et")): {
            "home_season_gf": row.get("home_season_gf"),
            "home_season_ga": row.get("home_season_ga"),
            "home_l10_gf": row.get("home_l10_gf"),
            "home_l10_ga": row.get("home_l10_ga"),
            "away_season_gf": row.get("away_season_gf"),
            "away_season_ga": row.get("away_season_ga"),
            "away_l10_gf": row.get("away_l10_gf"),
            "away_l10_ga": row.get("away_l10_ga"),
            "combined_season_gf": row.get("combined_season_gf"),
            "combined_season_cards": row.get("combined_season_cards"),
        }
        for row in analysis_rows
    }

    existing_keys = {_market_key(row) for row in analysis_rows}
    supplemental_rows = _fetch_supplemental_rows(client, where_sql, params)
    merged_rows = list(analysis_rows)
    for supplemental in supplemental_rows:
        key = _market_key(supplemental)
        if key in existing_keys:
            continue
        game_stats = stats_by_game.get(
            (supplemental.get("league"), supplemental.get("game"), supplemental.get("start_time_et")),
            {},
        )
        merged_rows.append({**supplemental, **game_stats})

    rows = [_make_suggestion(row) for row in merged_rows]

    suggestions = [row for row in rows if row.get("edge", 0) >= min_edge]
    suggestions.sort(key=lambda row: row.get("edge", 0), reverse=True)

    return {
        "date_et": today_et,
        "slate_size": len({f"{r.get('league')}|{r.get('game')}" for r in rows}),
        "markets_count": len(rows),
        "suggestions": suggestions,
        "all_markets": rows,
    }
