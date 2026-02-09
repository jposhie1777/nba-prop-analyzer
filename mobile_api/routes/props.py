# mobile_api/routes/props.py

from fastapi import APIRouter, Query
from typing import Optional
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(
    prefix="/props",
    tags=["props"],
)

DATASET = "nba_goat_data"
VIEW = "v_alt_player_props_hit_rates"

# ── Market key normalization ────────────────────────────────────────
# Maps raw Odds API market_key values to the short keys the frontend expects.
MARKET_KEY_MAP: dict[str, str] = {
    "player_points": "pts",
    "player_points_alternate": "pts",
    "player_rebounds": "reb",
    "player_rebounds_alternate": "reb",
    "player_assists": "ast",
    "player_assists_alternate": "ast",
    "player_threes": "3pm",
    "player_threes_alternate": "3pm",
    "player_steals": "stl",
    "player_steals_alternate": "stl",
    "player_blocks": "blk",
    "player_blocks_alternate": "blk",
    "player_turnovers": "tov",
    "player_turnovers_alternate": "tov",
    "player_points_rebounds_assists": "pra",
    "player_points_rebounds_assists_alternate": "pra",
    "player_points_rebounds": "pr",
    "player_points_rebounds_alternate": "pr",
    "player_points_assists": "pa",
    "player_points_assists_alternate": "pa",
    "player_rebounds_assists": "ra",
    "player_rebounds_assists_alternate": "ra",
    "player_double_double": "dd",
    "player_triple_double": "td",
    "player_first_basket": "first_basket",
}


def _normalize_market_key(raw: str | None) -> str | None:
    if not raw:
        return raw
    return MARKET_KEY_MAP.get(raw.strip().lower(), raw)


# ── Team full-name → abbreviation ──────────────────────────────────
TEAM_NAME_TO_ABBR: dict[str, str] = {
    "atlanta hawks": "ATL",
    "boston celtics": "BOS",
    "brooklyn nets": "BKN",
    "charlotte hornets": "CHA",
    "chicago bulls": "CHI",
    "cleveland cavaliers": "CLE",
    "dallas mavericks": "DAL",
    "denver nuggets": "DEN",
    "detroit pistons": "DET",
    "golden state warriors": "GSW",
    "houston rockets": "HOU",
    "indiana pacers": "IND",
    "la clippers": "LAC",
    "los angeles clippers": "LAC",
    "la lakers": "LAL",
    "los angeles lakers": "LAL",
    "memphis grizzlies": "MEM",
    "miami heat": "MIA",
    "milwaukee bucks": "MIL",
    "minnesota timberwolves": "MIN",
    "new orleans pelicans": "NOP",
    "new york knicks": "NYK",
    "oklahoma city thunder": "OKC",
    "orlando magic": "ORL",
    "philadelphia 76ers": "PHI",
    "phoenix suns": "PHX",
    "portland trail blazers": "POR",
    "sacramento kings": "SAC",
    "san antonio spurs": "SAS",
    "toronto raptors": "TOR",
    "utah jazz": "UTA",
    "washington wizards": "WAS",
}


def _team_abbr(name: str | None) -> str | None:
    if not name:
        return None
    return TEAM_NAME_TO_ABBR.get(name.strip().lower())


# ── Row post-processing ────────────────────────────────────────────
def _enrich_row(row: dict) -> dict:
    """Normalize fields so the frontend receives a consistent schema."""
    # Market key
    row["market_key"] = _normalize_market_key(row.get("market_key"))

    # Team abbreviations (derive from full names when missing)
    if not row.get("home_team_abbr"):
        row["home_team_abbr"] = _team_abbr(row.get("home_team"))
    if not row.get("away_team_abbr"):
        row["away_team_abbr"] = _team_abbr(row.get("away_team"))

    # Odds side: rename outcome_name → odds_side if missing
    if not row.get("odds_side") and row.get("outcome_name"):
        row["odds_side"] = row["outcome_name"].strip().lower()

    # Player ID: use espn_player_id from lookup when player_id is missing
    if not row.get("player_id") and row.get("espn_player_id"):
        row["player_id"] = row["espn_player_id"]

    return row


# ── Query helpers ──────────────────────────────────────────────────
def build_where(
    game_date: Optional[str],
    market: Optional[str],
):
    clauses = []

    if game_date:
        clauses.append("p.request_date = @game_date")

    if market:
        clauses.append("p.market_key = @market")

    if not clauses:
        return ""

    return "WHERE " + " AND ".join(clauses)


@router.get("")
def read_props(
    game_date: Optional[str] = None,
    market: Optional[str] = None,
    window: Optional[str] = None,
    limit: int = Query(500, ge=100, le=2000),
    offset: int = 0,
):
    client = get_bq_client()

    where_sql = build_where(
        game_date,
        market,
    )

    sql = f"""
    SELECT
      p.*,
      l.player_image_url,
      l.espn_player_id
    FROM `{DATASET}.{VIEW}` p
    LEFT JOIN `nba_goat_data.player_lookup` l
      ON l.player_name = p.player_name
    {where_sql}
    ORDER BY p.commence_time ASC, p.event_id, p.player_name, p.market_key, p.line
    LIMIT @limit
    OFFSET @offset
    """

    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
    ]

    if game_date:
        params.append(
            bigquery.ScalarQueryParameter("game_date", "STRING", game_date)
        )
    if market:
        params.append(
            bigquery.ScalarQueryParameter("market", "STRING", market)
        )

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    )

    rows = [_enrich_row(dict(r)) for r in job.result()]

    return {
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "props": rows,
    }