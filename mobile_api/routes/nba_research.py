# mobile_api/routes/nba_research.py
"""
NBA Research page data — mirrors PropFinder's Research view.

Returns the full set of props for today's slate (both Over/Under, base + alt
lines) along with the games list and the unique teams/positions/categories so
the client can filter locally.

Endpoint is intentionally cache-friendly: one unfiltered payload covers every
filter combination the UI exposes.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(tags=["nba-research"])

DATASET = "nba"
ET = ZoneInfo("America/New_York")

# Categories the Research page exposes — label + value pairs the UI renders
# as the Categories filter chips.
RESEARCH_CATEGORIES = [
    {"value": "points", "label": "Points"},
    {"value": "rebounds", "label": "Rebounds"},
    {"value": "assists", "label": "Assists"},
    {"value": "threePointsMade", "label": "Three Pointers"},
    {"value": "steals", "label": "Steals"},
    {"value": "blocks", "label": "Blocks"},
    {"value": "stealsBlocks", "label": "Steals + Blocks"},
    {"value": "pointsRebounds", "label": "Points + Rebounds"},
    {"value": "pointsAssists", "label": "Points + Assists"},
    {"value": "reboundAssists", "label": "Rebounds + Assists"},
    {"value": "pointsReboundsAssists", "label": "Points + Rebounds + Assists"},
    {"value": "turnovers", "label": "Turnovers"},
    {"value": "freeThrowsMade", "label": "Free Throws Made"},
    {"value": "pointsInPaint", "label": "Points in Paint"},
    {"value": "fantasyPoints", "label": "Fantasy Points"},
    {"value": "doubleDouble", "label": "Double Double"},
    {"value": "tripleDouble", "label": "Triple Double"},
    {"value": "q1Points", "label": "Q1 Points"},
    {"value": "q1Rebounds", "label": "Q1 Rebounds"},
    {"value": "q1Assists", "label": "Q1 Assists"},
    {"value": "q1ThreePointsMade", "label": "Q1 Three Pointers"},
    {"value": "q1PointsRebounds", "label": "Q1 Points + Rebounds"},
    {"value": "q1PointsAssists", "label": "Q1 Points + Assists"},
    {"value": "q1PointsReboundsAssists", "label": "Q1 Points + Rebounds + Assists"},
]


def _parse_hit_rate(val: Any) -> Optional[float]:
    """PropFinder stores hit rates like '9/10' or '90%'. Return pct 0..1."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except ValueError:
            return None
    if "/" in s:
        a, _, b = s.partition("/")
        try:
            bn = float(b)
            if bn == 0:
                return None
            return float(a) / bn
        except ValueError:
            return None
    try:
        f = float(s)
        # If value looks like 0..1 keep; if 0..100 scale down.
        return f / 100.0 if f > 1.0 else f
    except ValueError:
        return None


def _hit_rate_raw(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _to_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


@router.get("/nba/research")
def get_nba_research(
    _refresh: bool = Query(False),  # bypass in-process cache when warm job calls it
) -> Dict[str, Any]:
    client = get_bq_client()

    # ── Pull today's props (both over/under, base + alt) ──────────────
    props_sql = f"""
    SELECT
      p.prop_id, p.player_id, p.player_name, p.position,
      p.team_code, p.opp_team_code, p.game_id, p.injury_status, p.is_home,
      p.category, p.line, p.over_under, p.is_alternate,
      p.pf_rating, p.matchup_rank, p.matchup_value, p.matchup_label,
      p.hit_rate_season, p.hit_rate_vs_team,
      p.hit_rate_l5, p.hit_rate_l10, p.hit_rate_l20,
      p.streak, p.avg_l10, p.avg_home_away, p.avg_vs_opponent,
      p.dk_price, p.dk_deep_link, p.dk_event_id, p.dk_outcome_code,
      p.fd_price, p.fd_deep_link, p.fd_market_id, p.fd_selection_id,
      p.best_book, p.best_price,
      p.ingested_at
    FROM `{DATASET}.raw_nba_props` p
    WHERE p.run_date = (SELECT MAX(run_date) FROM `{DATASET}.raw_nba_props`)
    """
    props_job = client.query(props_sql)
    prop_rows = list(props_job.result())

    # ── Pull today's games for the Games dropdown ─────────────────────
    games_sql = f"""
    SELECT
      game_id, game_date,
      away_team_code, home_team_code,
      home_ml, away_ml,
      home_spread_line, home_spread_odds,
      away_spread_line, away_spread_odds,
      total_line
    FROM `{DATASET}.raw_nba_games`
    WHERE run_date = (SELECT MAX(run_date) FROM `{DATASET}.raw_nba_games`)
    ORDER BY game_date ASC
    """
    games_job = client.query(games_sql)

    games: List[Dict[str, Any]] = []
    seen_game_ids = set()
    for g in games_job.result():
        gid = g["game_id"]
        if gid in seen_game_ids:
            continue
        seen_game_ids.add(gid)
        game_dt = g.get("game_date")
        date_label = ""
        time_label = ""
        sort_key = ""
        if game_dt:
            et_dt = game_dt.astimezone(ET)
            date_label = et_dt.strftime("%-m/%d")
            time_label = et_dt.strftime("%-I:%M %p")
            sort_key = et_dt.isoformat()

        # Favorite label (e.g. "DET -8")
        home_spread = g.get("home_spread_line")
        away_spread = g.get("away_spread_line")
        fav_label = ""
        try:
            hs = float(home_spread) if home_spread not in (None, "") else None
            as_ = float(away_spread) if away_spread not in (None, "") else None
            if hs is not None and as_ is not None:
                if hs <= as_:
                    fav_label = f"{g['home_team_code']} {hs:+g}"
                else:
                    fav_label = f"{g['away_team_code']} {as_:+g}"
        except (TypeError, ValueError):
            fav_label = ""

        games.append({
            "game_id": gid,
            "away_team_code": g["away_team_code"],
            "home_team_code": g["home_team_code"],
            "date_label": date_label,
            "time_label": time_label,
            "sort_key": sort_key,
            "home_spread_line": home_spread,
            "away_spread_line": away_spread,
            "favorite_label": fav_label,
            "total_line": _to_float(g.get("total_line")),
        })

    # Map game_id -> formatted game label + team/away codes for quick lookup
    game_index = {g["game_id"]: g for g in games}

    # ── Shape props ────────────────────────────────────────────────────
    props: List[Dict[str, Any]] = []
    teams: set = set()
    positions: set = set()
    latest_ingest: Optional[datetime] = None

    for p in prop_rows:
        cat = p["category"]
        if not cat:
            continue
        team = p.get("team_code")
        pos = p.get("position")
        if team:
            teams.add(team)
        if pos:
            positions.add(pos)
        if p.get("ingested_at") and (
            latest_ingest is None or p["ingested_at"] > latest_ingest
        ):
            latest_ingest = p["ingested_at"]

        game_id = p.get("game_id")
        game_meta = game_index.get(game_id, {}) if game_id else {}

        props.append({
            "prop_id": p.get("prop_id"),
            "player_id": p.get("player_id"),
            "player_name": p.get("player_name"),
            "position": pos,
            "team_code": team,
            "opp_team_code": p.get("opp_team_code"),
            "game_id": game_id,
            "is_home": p.get("is_home"),
            "injury_status": p.get("injury_status"),
            "category": cat,
            "line": _to_float(p.get("line")),
            "over_under": p.get("over_under"),
            "is_alternate": bool(p.get("is_alternate")),
            "pf_rating": _to_float(p.get("pf_rating")),
            "streak": _to_int(p.get("streak")),
            "matchup_rank": _to_int(p.get("matchup_rank")),
            "matchup_value": _to_float(p.get("matchup_value")),
            "matchup_label": p.get("matchup_label"),
            "hit_rate_season": _parse_hit_rate(p.get("hit_rate_season")),
            "hit_rate_season_raw": _hit_rate_raw(p.get("hit_rate_season")),
            "hit_rate_vs_team": _parse_hit_rate(p.get("hit_rate_vs_team")),
            "hit_rate_vs_team_raw": _hit_rate_raw(p.get("hit_rate_vs_team")),
            "hit_rate_l5": _parse_hit_rate(p.get("hit_rate_l5")),
            "hit_rate_l5_raw": _hit_rate_raw(p.get("hit_rate_l5")),
            "hit_rate_l10": _parse_hit_rate(p.get("hit_rate_l10")),
            "hit_rate_l10_raw": _hit_rate_raw(p.get("hit_rate_l10")),
            "hit_rate_l20": _parse_hit_rate(p.get("hit_rate_l20")),
            "hit_rate_l20_raw": _hit_rate_raw(p.get("hit_rate_l20")),
            "avg_l10": _to_float(p.get("avg_l10")),
            "avg_home_away": _to_float(p.get("avg_home_away")),
            "avg_vs_opponent": _to_float(p.get("avg_vs_opponent")),
            "dk_price": _to_int(p.get("dk_price")),
            "dk_deep_link": p.get("dk_deep_link"),
            "dk_event_id": p.get("dk_event_id"),
            "dk_outcome_code": p.get("dk_outcome_code"),
            "fd_price": _to_int(p.get("fd_price")),
            "fd_deep_link": p.get("fd_deep_link"),
            "fd_market_id": p.get("fd_market_id"),
            "fd_selection_id": p.get("fd_selection_id"),
            "best_book": p.get("best_book"),
            "best_price": _to_int(p.get("best_price")),
            "game_date_label": game_meta.get("date_label", ""),
            "game_time_label": game_meta.get("time_label", ""),
        })

    refreshed_at = (
        latest_ingest.astimezone(timezone.utc).isoformat()
        if latest_ingest
        else datetime.now(timezone.utc).isoformat()
    )

    return {
        "refreshed_at": refreshed_at,
        "categories": RESEARCH_CATEGORIES,
        "teams": sorted(teams),
        "positions": sorted(positions),
        "games": games,
        "props": props,
    }
