# mobile_api/ingest/game_advanced_stats/ingest.py
"""
Game Advanced Stats V2 Ingestion Module

Fetches comprehensive advanced stats from Balldontlie API v2 endpoint.
Includes hustle stats, tracking data, and per-period breakdowns.

V2 advanced stats are only available starting from the 2015 season.
Advanced stats are only available upon game completion.

API Endpoint: GET https://api.balldontlie.io/nba/v2/stats/advanced
"""

import os
import time
import requests
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from google.cloud import bigquery


# ======================================================
# Configuration
# ======================================================

BDL_BASE_V2 = "https://api.balldontlie.io/nba/v2"
NBA_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")

# BigQuery table
GAME_ADVANCED_STATS_TABLE = "nba_live.game_advanced_stats"

# Rate limiting
REQUEST_DELAY_SEC = 0.3  # Polite throttling between paginated requests
BATCH_SIZE = 100  # API default per_page


# ======================================================
# BigQuery Client
# ======================================================

def get_bq_client() -> bigquery.Client:
    """Get BigQuery client with project from environment."""
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project:
        return bigquery.Client(project=project)
    return bigquery.Client()


# ======================================================
# BallDontLie API
# ======================================================

def get_bdl_headers() -> Dict[str, str]:
    """Get BallDontLie API headers with auth."""
    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise RuntimeError("BALLDONTLIE_API_KEY is missing")
    return {
        "Authorization": api_key,
        "Accept": "application/json",
    }


def fetch_advanced_stats_page(
    *,
    season: int,
    cursor: Optional[int] = None,
    per_page: int = BATCH_SIZE,
    game_ids: Optional[List[int]] = None,
    player_ids: Optional[List[int]] = None,
    period: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Fetch a single page of advanced stats from V2 API.

    Args:
        season: Season year (e.g., 2025 for 2025-2026 season)
        cursor: Pagination cursor from previous response
        per_page: Number of results per page (max 100)
        game_ids: Optional list of game IDs to filter
        player_ids: Optional list of player IDs to filter
        period: Optional period filter (0 = full game)

    Returns:
        API response dict with 'data' and 'meta'
    """
    headers = get_bdl_headers()

    params: Dict[str, Any] = {
        "seasons[]": [season],
        "per_page": per_page,
    }

    if cursor is not None:
        params["cursor"] = cursor

    if game_ids:
        params["game_ids[]"] = game_ids

    if player_ids:
        params["player_ids[]"] = player_ids

    if period is not None:
        params["period"] = period

    resp = requests.get(
        f"{BDL_BASE_V2}/stats/advanced",
        params=params,
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()

    return resp.json()


def fetch_all_advanced_stats(
    *,
    season: int,
    game_ids: Optional[List[int]] = None,
    player_ids: Optional[List[int]] = None,
    period: int = 0,  # Default to full game stats
    max_pages: int = 1000,  # Safety limit
) -> List[Dict[str, Any]]:
    """
    Fetch all advanced stats with pagination.

    Args:
        season: Season year
        game_ids: Optional game ID filter
        player_ids: Optional player ID filter
        period: Period filter (0 = full game)
        max_pages: Maximum pages to fetch (safety limit)

    Returns:
        List of all stat records
    """
    all_data: List[Dict[str, Any]] = []
    cursor: Optional[int] = None
    page = 0

    print(f"[GAME_ADV_STATS] Fetching advanced stats for season {season}, period={period}")

    while page < max_pages:
        page += 1

        response = fetch_advanced_stats_page(
            season=season,
            cursor=cursor,
            game_ids=game_ids,
            player_ids=player_ids,
            period=period,
        )

        data = response.get("data", [])
        meta = response.get("meta", {})

        if not data:
            print(f"[GAME_ADV_STATS] No more data at page {page}")
            break

        all_data.extend(data)

        next_cursor = meta.get("next_cursor")

        if next_cursor is None:
            print(f"[GAME_ADV_STATS] Reached end of pagination at page {page}")
            break

        cursor = next_cursor

        if page % 10 == 0:
            print(f"[GAME_ADV_STATS] Fetched page {page}, total records: {len(all_data)}")

        time.sleep(REQUEST_DELAY_SEC)

    print(f"[GAME_ADV_STATS] Total records fetched: {len(all_data)}")
    return all_data


# ======================================================
# Game ID Fetching (for date-based filtering)
# ======================================================

def fetch_game_ids_for_date(target_date: date) -> List[int]:
    """
    Fetch game IDs for a specific date.

    Args:
        target_date: Date to fetch games for

    Returns:
        List of game IDs
    """
    headers = get_bdl_headers()

    resp = requests.get(
        "https://api.balldontlie.io/v1/games",
        params={"dates[]": [target_date.isoformat()]},
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()

    games = resp.json().get("data", [])

    # Only include completed games (status = "Final")
    game_ids = [
        g["id"]
        for g in games
        if g.get("status") == "Final" or g.get("time") == "Final"
    ]

    print(f"[GAME_ADV_STATS] Found {len(game_ids)} completed games for {target_date}")
    return game_ids


def fetch_game_ids_for_season(season: int) -> List[int]:
    """
    Fetch all game IDs for a season by iterating through dates.

    Args:
        season: Season year (e.g., 2025 for 2025-2026 season)

    Returns:
        List of all game IDs for the season
    """
    headers = get_bdl_headers()

    all_game_ids: List[int] = []
    cursor: Optional[int] = None

    print(f"[GAME_ADV_STATS] Fetching all game IDs for season {season}")

    while True:
        params: Dict[str, Any] = {
            "seasons[]": [season],
            "per_page": 100,
        }

        if cursor:
            params["cursor"] = cursor

        resp = requests.get(
            "https://api.balldontlie.io/v1/games",
            params=params,
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()

        result = resp.json()
        games = result.get("data", [])

        # Only include completed games
        for g in games:
            if g.get("status") == "Final" or g.get("time") == "Final":
                all_game_ids.append(g["id"])

        meta = result.get("meta", {})
        cursor = meta.get("next_cursor")

        if not cursor:
            break

        time.sleep(REQUEST_DELAY_SEC)

    print(f"[GAME_ADV_STATS] Found {len(all_game_ids)} completed games for season {season}")
    return all_game_ids


# ======================================================
# Row Transformation
# ======================================================

def transform_stat_to_row(stat: Dict[str, Any], run_ts: str) -> Dict[str, Any]:
    """
    Transform a single API stat record to a BigQuery row.

    Args:
        stat: Raw stat record from API
        run_ts: Batch run timestamp

    Returns:
        Flattened dict ready for BigQuery insertion
    """
    player = stat.get("player") or {}
    team = stat.get("team") or {}
    game = stat.get("game") or {}

    # Parse game datetime if present
    game_datetime = None
    if game.get("datetime"):
        try:
            game_datetime = datetime.fromisoformat(
                game["datetime"].replace("Z", "+00:00")
            ).isoformat()
        except (ValueError, AttributeError):
            pass

    # Parse game date
    game_date = None
    if game.get("date"):
        try:
            game_date = datetime.fromisoformat(game["date"]).date().isoformat()
        except (ValueError, AttributeError):
            pass

    return {
        # Metadata
        "id": stat.get("id"),
        "run_ts": run_ts,
        "ingested_at": datetime.now(UTC_TZ).isoformat(),
        "period": stat.get("period"),

        # Player info
        "player_id": player.get("id"),
        "player_first_name": player.get("first_name"),
        "player_last_name": player.get("last_name"),
        "player_position": player.get("position"),
        "player_height": player.get("height"),
        "player_weight": player.get("weight"),
        "player_jersey_number": player.get("jersey_number"),
        "player_college": player.get("college"),
        "player_country": player.get("country"),
        "player_draft_year": player.get("draft_year"),
        "player_draft_round": player.get("draft_round"),
        "player_draft_number": player.get("draft_number"),

        # Team info
        "team_id": team.get("id"),
        "team_conference": team.get("conference"),
        "team_division": team.get("division"),
        "team_city": team.get("city"),
        "team_name": team.get("name"),
        "team_full_name": team.get("full_name"),
        "team_abbreviation": team.get("abbreviation"),

        # Game info
        "game_id": game.get("id"),
        "game_date": game_date,
        "game_season": game.get("season"),
        "game_status": game.get("status"),
        "game_period": game.get("period"),
        "game_time": game.get("time"),
        "game_postseason": game.get("postseason"),
        "game_postponed": game.get("postponed"),
        "home_team_score": game.get("home_team_score"),
        "visitor_team_score": game.get("visitor_team_score"),
        "home_team_id": game.get("home_team_id"),
        "visitor_team_id": game.get("visitor_team_id"),

        # Extended game info (V2)
        "game_datetime": game_datetime,
        "home_q1": game.get("home_q1"),
        "home_q2": game.get("home_q2"),
        "home_q3": game.get("home_q3"),
        "home_q4": game.get("home_q4"),
        "home_ot1": game.get("home_ot1"),
        "home_ot2": game.get("home_ot2"),
        "home_ot3": game.get("home_ot3"),
        "home_timeouts_remaining": game.get("home_timeouts_remaining"),
        "home_in_bonus": game.get("home_in_bonus"),
        "visitor_q1": game.get("visitor_q1"),
        "visitor_q2": game.get("visitor_q2"),
        "visitor_q3": game.get("visitor_q3"),
        "visitor_q4": game.get("visitor_q4"),
        "visitor_ot1": game.get("visitor_ot1"),
        "visitor_ot2": game.get("visitor_ot2"),
        "visitor_ot3": game.get("visitor_ot3"),
        "visitor_timeouts_remaining": game.get("visitor_timeouts_remaining"),
        "visitor_in_bonus": game.get("visitor_in_bonus"),
        "ist_stage": game.get("ist_stage"),

        # Core Advanced Stats
        "pie": stat.get("pie"),
        "pace": stat.get("pace"),
        "pace_per_40": stat.get("pace_per_40"),
        "possessions": stat.get("possessions"),
        "assist_percentage": stat.get("assist_percentage"),
        "assist_ratio": stat.get("assist_ratio"),
        "assist_to_turnover": stat.get("assist_to_turnover"),
        "defensive_rating": stat.get("defensive_rating"),
        "offensive_rating": stat.get("offensive_rating"),
        "net_rating": stat.get("net_rating"),
        "estimated_defensive_rating": stat.get("estimated_defensive_rating"),
        "estimated_offensive_rating": stat.get("estimated_offensive_rating"),
        "estimated_net_rating": stat.get("estimated_net_rating"),
        "estimated_pace": stat.get("estimated_pace"),
        "estimated_usage_percentage": stat.get("estimated_usage_percentage"),
        "defensive_rebound_percentage": stat.get("defensive_rebound_percentage"),
        "offensive_rebound_percentage": stat.get("offensive_rebound_percentage"),
        "rebound_percentage": stat.get("rebound_percentage"),
        "effective_field_goal_percentage": stat.get("effective_field_goal_percentage"),
        "true_shooting_percentage": stat.get("true_shooting_percentage"),
        "turnover_ratio": stat.get("turnover_ratio"),
        "usage_percentage": stat.get("usage_percentage"),

        # Miscellaneous Stats
        "blocks_against": stat.get("blocks_against"),
        "fouls_drawn": stat.get("fouls_drawn"),
        "points_fast_break": stat.get("points_fast_break"),
        "points_off_turnovers": stat.get("points_off_turnovers"),
        "points_paint": stat.get("points_paint"),
        "points_second_chance": stat.get("points_second_chance"),
        "opp_points_fast_break": stat.get("opp_points_fast_break"),
        "opp_points_off_turnovers": stat.get("opp_points_off_turnovers"),
        "opp_points_paint": stat.get("opp_points_paint"),
        "opp_points_second_chance": stat.get("opp_points_second_chance"),

        # Scoring Stats
        "pct_assisted_2pt": stat.get("pct_assisted_2pt"),
        "pct_assisted_3pt": stat.get("pct_assisted_3pt"),
        "pct_assisted_fgm": stat.get("pct_assisted_fgm"),
        "pct_fga_2pt": stat.get("pct_fga_2pt"),
        "pct_fga_3pt": stat.get("pct_fga_3pt"),
        "pct_pts_2pt": stat.get("pct_pts_2pt"),
        "pct_pts_3pt": stat.get("pct_pts_3pt"),
        "pct_pts_fast_break": stat.get("pct_pts_fast_break"),
        "pct_pts_free_throw": stat.get("pct_pts_free_throw"),
        "pct_pts_midrange_2pt": stat.get("pct_pts_midrange_2pt"),
        "pct_pts_off_turnovers": stat.get("pct_pts_off_turnovers"),
        "pct_pts_paint": stat.get("pct_pts_paint"),
        "pct_unassisted_2pt": stat.get("pct_unassisted_2pt"),
        "pct_unassisted_3pt": stat.get("pct_unassisted_3pt"),
        "pct_unassisted_fgm": stat.get("pct_unassisted_fgm"),

        # Four Factors Stats
        "four_factors_efg_pct": stat.get("four_factors_efg_pct"),
        "free_throw_attempt_rate": stat.get("free_throw_attempt_rate"),
        "four_factors_oreb_pct": stat.get("four_factors_oreb_pct"),
        "team_turnover_pct": stat.get("team_turnover_pct"),
        "opp_efg_pct": stat.get("opp_efg_pct"),
        "opp_free_throw_attempt_rate": stat.get("opp_free_throw_attempt_rate"),
        "opp_oreb_pct": stat.get("opp_oreb_pct"),
        "opp_turnover_pct": stat.get("opp_turnover_pct"),

        # Hustle Stats
        "box_outs": stat.get("box_outs"),
        "box_out_player_rebounds": stat.get("box_out_player_rebounds"),
        "box_out_player_team_rebounds": stat.get("box_out_player_team_rebounds"),
        "defensive_box_outs": stat.get("defensive_box_outs"),
        "offensive_box_outs": stat.get("offensive_box_outs"),
        "charges_drawn": stat.get("charges_drawn"),
        "contested_shots": stat.get("contested_shots"),
        "contested_shots_2pt": stat.get("contested_shots_2pt"),
        "contested_shots_3pt": stat.get("contested_shots_3pt"),
        "deflections": stat.get("deflections"),
        "loose_balls_recovered_def": stat.get("loose_balls_recovered_def"),
        "loose_balls_recovered_off": stat.get("loose_balls_recovered_off"),
        "loose_balls_recovered_total": stat.get("loose_balls_recovered_total"),
        "screen_assists": stat.get("screen_assists"),
        "screen_assist_points": stat.get("screen_assist_points"),

        # Defensive Stats
        "matchup_minutes": stat.get("matchup_minutes"),
        "matchup_fg_pct": stat.get("matchup_fg_pct"),
        "matchup_fga": stat.get("matchup_fga"),
        "matchup_fgm": stat.get("matchup_fgm"),
        "matchup_3pt_pct": stat.get("matchup_3pt_pct"),
        "matchup_3pa": stat.get("matchup_3pa"),
        "matchup_3pm": stat.get("matchup_3pm"),
        "matchup_assists": stat.get("matchup_assists"),
        "matchup_turnovers": stat.get("matchup_turnovers"),
        "partial_possessions": stat.get("partial_possessions"),
        "matchup_player_points": stat.get("matchup_player_points"),
        "switches_on": stat.get("switches_on"),

        # Tracking Stats
        "speed": stat.get("speed"),
        "distance": stat.get("distance"),
        "touches": stat.get("touches"),
        "passes": stat.get("passes"),
        "secondary_assists": stat.get("secondary_assists"),
        "free_throw_assists": stat.get("free_throw_assists"),
        "contested_fga": stat.get("contested_fga"),
        "contested_fgm": stat.get("contested_fgm"),
        "contested_fg_pct": stat.get("contested_fg_pct"),
        "uncontested_fga": stat.get("uncontested_fga"),
        "uncontested_fgm": stat.get("uncontested_fgm"),
        "uncontested_fg_pct": stat.get("uncontested_fg_pct"),
        "defended_at_rim_fga": stat.get("defended_at_rim_fga"),
        "defended_at_rim_fgm": stat.get("defended_at_rim_fgm"),
        "defended_at_rim_fg_pct": stat.get("defended_at_rim_fg_pct"),

        # Usage Stats (additional)
        "rebound_chances_def": stat.get("rebound_chances_def"),
        "rebound_chances_off": stat.get("rebound_chances_off"),
        "rebound_chances_total": stat.get("rebound_chances_total"),
        "pct_blocks": stat.get("pct_blocks"),
        "pct_blocks_allowed": stat.get("pct_blocks_allowed"),
        "pct_fga": stat.get("pct_fga"),
        "pct_fgm": stat.get("pct_fgm"),
        "pct_fta": stat.get("pct_fta"),
        "pct_ftm": stat.get("pct_ftm"),
        "pct_personal_fouls": stat.get("pct_personal_fouls"),
        "pct_personal_fouls_drawn": stat.get("pct_personal_fouls_drawn"),
        "pct_points": stat.get("pct_points"),
        "pct_rebounds_def": stat.get("pct_rebounds_def"),
        "pct_rebounds_off": stat.get("pct_rebounds_off"),
        "pct_rebounds_total": stat.get("pct_rebounds_total"),
        "pct_steals": stat.get("pct_steals"),
        "pct_3pa": stat.get("pct_3pa"),
        "pct_3pm": stat.get("pct_3pm"),
        "pct_turnovers": stat.get("pct_turnovers"),
    }


# ======================================================
# BigQuery Insert
# ======================================================

def insert_rows_to_bq(rows: List[Dict[str, Any]], table: str = GAME_ADVANCED_STATS_TABLE) -> int:
    """
    Insert rows into BigQuery table.

    Args:
        rows: List of row dicts
        table: Target table name

    Returns:
        Number of rows inserted
    """
    if not rows:
        print("[GAME_ADV_STATS] No rows to insert")
        return 0

    client = get_bq_client()

    # Insert in batches of 500
    batch_size = 500
    total_inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]

        errors = client.insert_rows_json(table, batch)

        if errors:
            print(f"[GAME_ADV_STATS] BigQuery insert errors: {errors[:5]}")
            raise RuntimeError(f"BigQuery insert failed: {errors[:3]}")

        total_inserted += len(batch)

        if i > 0 and i % 2000 == 0:
            print(f"[GAME_ADV_STATS] Inserted {total_inserted}/{len(rows)} rows")

    print(f"[GAME_ADV_STATS] Successfully inserted {total_inserted} rows into {table}")
    return total_inserted


# ======================================================
# Public API Functions
# ======================================================

def fetch_advanced_stats_for_date(
    target_date: date,
    season: Optional[int] = None,
    period: int = 0,
) -> List[Dict[str, Any]]:
    """
    Fetch advanced stats for a specific date.

    Args:
        target_date: Date to fetch stats for
        season: Season year (auto-detected if not provided)
        period: Period filter (0 = full game)

    Returns:
        List of stat records
    """
    # Auto-detect season from date
    # NBA season starts in October, so Oct-Dec = next year's season
    if season is None:
        if target_date.month >= 10:
            season = target_date.year
        else:
            season = target_date.year - 1

    # First, get the game IDs for this date
    game_ids = fetch_game_ids_for_date(target_date)

    if not game_ids:
        print(f"[GAME_ADV_STATS] No completed games found for {target_date}")
        return []

    # Fetch advanced stats for these games
    all_stats: List[Dict[str, Any]] = []
    cursor: Optional[int] = None

    print(f"[GAME_ADV_STATS] Fetching advanced stats for {len(game_ids)} games on {target_date}")

    # Fetch stats for each game (API doesn't support game_ids[] filter well with pagination)
    # So we fetch by season and filter locally, or fetch all and filter
    # Better approach: fetch all for the season with cursor and filter by game_id
    while True:
        response = fetch_advanced_stats_page(
            season=season,
            cursor=cursor,
            period=period,
        )

        data = response.get("data", [])
        meta = response.get("meta", {})

        # Filter by game IDs
        for stat in data:
            game = stat.get("game", {})
            if game.get("id") in game_ids:
                all_stats.append(stat)

        next_cursor = meta.get("next_cursor")

        if not next_cursor:
            break

        cursor = next_cursor
        time.sleep(REQUEST_DELAY_SEC)

    print(f"[GAME_ADV_STATS] Found {len(all_stats)} stat records for {target_date}")
    return all_stats


def ingest_date(
    target_date: date,
    season: Optional[int] = None,
    period: int = 0,
    table: str = GAME_ADVANCED_STATS_TABLE,
) -> Dict[str, Any]:
    """
    Ingest advanced stats for a specific date into BigQuery.

    Args:
        target_date: Date to ingest
        season: Season year (auto-detected if not provided)
        period: Period filter (0 = full game)
        table: Target BigQuery table

    Returns:
        Summary dict with counts
    """
    print(f"\n{'='*60}")
    print(f"[GAME_ADV_STATS] INGESTING DATE: {target_date}")
    print(f"{'='*60}")

    run_ts = datetime.now(UTC_TZ).isoformat()

    # Fetch stats
    stats = fetch_advanced_stats_for_date(target_date, season=season, period=period)

    if not stats:
        return {
            "date": target_date.isoformat(),
            "games": 0,
            "stats": 0,
            "status": "no_data",
        }

    # Transform to rows
    rows = [transform_stat_to_row(s, run_ts) for s in stats]

    # Get unique game count
    unique_games = len(set(r["game_id"] for r in rows if r.get("game_id")))

    # Insert to BigQuery
    inserted = insert_rows_to_bq(rows, table)

    return {
        "date": target_date.isoformat(),
        "games": unique_games,
        "stats": inserted,
        "status": "ok",
    }


def ingest_yesterday(
    period: int = 0,
    table: str = GAME_ADVANCED_STATS_TABLE,
) -> Dict[str, Any]:
    """
    Ingest advanced stats for yesterday's games.

    Args:
        period: Period filter (0 = full game)
        table: Target BigQuery table

    Returns:
        Summary dict with counts
    """
    # Get yesterday in ET timezone
    now_et = datetime.now(NBA_TZ)
    yesterday = (now_et - timedelta(days=1)).date()

    print(f"[GAME_ADV_STATS] Yesterday (ET): {yesterday}")

    return ingest_date(yesterday, period=period, table=table)


def ingest_date_range(
    start_date: date,
    end_date: date,
    period: int = 0,
    table: str = GAME_ADVANCED_STATS_TABLE,
) -> Dict[str, Any]:
    """
    Ingest advanced stats for a date range.

    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        period: Period filter (0 = full game)
        table: Target BigQuery table

    Returns:
        Summary dict with total counts
    """
    print(f"\n{'='*60}")
    print(f"[GAME_ADV_STATS] INGESTING DATE RANGE: {start_date} to {end_date}")
    print(f"{'='*60}")

    total_games = 0
    total_stats = 0
    dates_processed = 0
    dates_with_data = 0

    current = start_date
    while current <= end_date:
        result = ingest_date(current, period=period, table=table)

        dates_processed += 1

        if result["status"] == "ok":
            total_games += result["games"]
            total_stats += result["stats"]
            dates_with_data += 1

        current += timedelta(days=1)

        # Small delay between dates
        time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"[GAME_ADV_STATS] DATE RANGE COMPLETE")
    print(f"[GAME_ADV_STATS] Dates processed: {dates_processed}")
    print(f"[GAME_ADV_STATS] Dates with data: {dates_with_data}")
    print(f"[GAME_ADV_STATS] Total games: {total_games}")
    print(f"[GAME_ADV_STATS] Total stats: {total_stats}")
    print(f"{'='*60}\n")

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "dates_processed": dates_processed,
        "dates_with_data": dates_with_data,
        "total_games": total_games,
        "total_stats": total_stats,
        "status": "ok",
    }


def backfill_season(
    season: int = 2025,
    period: int = 0,
    table: str = GAME_ADVANCED_STATS_TABLE,
) -> Dict[str, Any]:
    """
    Backfill all advanced stats for a full season.

    For 2025-2026 season, this fetches all data from season start (Oct 2025)
    through the current date.

    Args:
        season: Season year (e.g., 2025 for 2025-2026 season)
        period: Period filter (0 = full game)
        table: Target BigQuery table

    Returns:
        Summary dict with counts
    """
    print(f"\n{'='*60}")
    print(f"[GAME_ADV_STATS] BACKFILLING SEASON {season}-{season+1}")
    print(f"{'='*60}")

    run_ts = datetime.now(UTC_TZ).isoformat()

    # Fetch all stats for the season directly (more efficient than date-by-date)
    all_stats = fetch_all_advanced_stats(
        season=season,
        period=period,
        max_pages=5000,  # Allow more pages for full season
    )

    if not all_stats:
        return {
            "season": f"{season}-{season+1}",
            "games": 0,
            "stats": 0,
            "status": "no_data",
        }

    # Transform to rows
    rows = [transform_stat_to_row(s, run_ts) for s in all_stats]

    # Get unique game count
    unique_games = len(set(r["game_id"] for r in rows if r.get("game_id")))

    # Get date range
    dates = [r["game_date"] for r in rows if r.get("game_date")]
    min_date = min(dates) if dates else None
    max_date = max(dates) if dates else None

    # Insert to BigQuery
    inserted = insert_rows_to_bq(rows, table)

    print(f"\n{'='*60}")
    print(f"[GAME_ADV_STATS] SEASON BACKFILL COMPLETE")
    print(f"[GAME_ADV_STATS] Season: {season}-{season+1}")
    print(f"[GAME_ADV_STATS] Date range: {min_date} to {max_date}")
    print(f"[GAME_ADV_STATS] Total games: {unique_games}")
    print(f"[GAME_ADV_STATS] Total stats: {inserted}")
    print(f"{'='*60}\n")

    return {
        "season": f"{season}-{season+1}",
        "min_date": min_date,
        "max_date": max_date,
        "games": unique_games,
        "stats": inserted,
        "status": "ok",
    }


# ======================================================
# CLI Entry Point
# ======================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python ingest.py yesterday")
        print("  python ingest.py date YYYY-MM-DD")
        print("  python ingest.py range YYYY-MM-DD YYYY-MM-DD")
        print("  python ingest.py backfill [season]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "yesterday":
        result = ingest_yesterday()
        print(f"Result: {result}")

    elif command == "date" and len(sys.argv) >= 3:
        target = date.fromisoformat(sys.argv[2])
        result = ingest_date(target)
        print(f"Result: {result}")

    elif command == "range" and len(sys.argv) >= 4:
        start = date.fromisoformat(sys.argv[2])
        end = date.fromisoformat(sys.argv[3])
        result = ingest_date_range(start, end)
        print(f"Result: {result}")

    elif command == "backfill":
        season = int(sys.argv[2]) if len(sys.argv) >= 3 else 2025
        result = backfill_season(season)
        print(f"Result: {result}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
