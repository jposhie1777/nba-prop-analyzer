"""
With Or Without You (WOWY) Analysis Module.

Analyzes how teammates' stats change when a specific player is out.
Uses historical game data from nba_goat_data.player_game_stats_full.
"""

from typing import Any, Optional
from bq import get_bq_client

# ==================================================
# Constants
# ==================================================
PLAYER_GAME_STATS_TABLE = "nba_goat_data.player_game_stats_full"
INJURIES_TABLE = "nba_live.player_injuries"


def get_current_season() -> int:
    """Get current NBA season year (e.g., 2025 for 2024-25 season)."""
    from datetime import datetime
    now = datetime.now()
    # NBA season starts in October, so if we're in Oct-Dec, it's the next year's season
    if now.month >= 10:
        return now.year + 1
    return now.year


# ==================================================
# Core WOWY Analysis
# ==================================================
def analyze_wowy_for_player(
    player_id: int,
    *,
    season: Optional[int] = None,
    min_games_with: int = 5,
    min_games_without: int = 3,
) -> dict[str, Any]:
    """
    Analyze teammate performance with/without a specific player.

    For each teammate on the same team, calculates:
    - Average stats when the player WAS playing (minutes > 0)
    - Average stats when the player was OUT (minutes = 0 or not in game)
    - Difference (impact of player being out)

    Args:
        player_id: The player whose absence we're analyzing
        season: Season to analyze (default: current season)
        min_games_with: Minimum games with player to include teammate
        min_games_without: Minimum games without player to include teammate

    Returns:
        Dict with player info and teammate WOWY stats
    """
    client = get_bq_client()

    if season is None:
        season = get_current_season()

    print(f"[WOWY] Analyzing player_id={player_id} for season={season}")

    # First, get the target player's info and team
    player_info_query = f"""
    SELECT DISTINCT
        player_id,
        player,
        team_id,
        team
    FROM `{PLAYER_GAME_STATS_TABLE}`
    WHERE player_id = {player_id}
      AND season = {season}
    LIMIT 1
    """

    player_info_rows = list(client.query(player_info_query).result())
    if not player_info_rows:
        return {
            "status": "error",
            "error": f"Player {player_id} not found in season {season}",
        }

    player_info = dict(player_info_rows[0])
    target_player_name = player_info["player"]
    target_team_id = player_info["team_id"]
    target_team = player_info["team"]

    print(f"[WOWY] Target player: {target_player_name} ({target_team})")

    # Main WOWY query: Compare teammate stats with/without the target player
    wowy_query = f"""
    WITH target_player_games AS (
        -- Games where the target player played (minutes > 0)
        SELECT DISTINCT game_id
        FROM `{PLAYER_GAME_STATS_TABLE}`
        WHERE player_id = {player_id}
          AND season = {season}
          AND minutes > 0
    ),

    target_player_missed AS (
        -- Games where target player's team played but they didn't
        -- (either minutes = 0 or not in the box score at all)
        SELECT DISTINCT g.game_id
        FROM `{PLAYER_GAME_STATS_TABLE}` g
        WHERE g.team_id = {target_team_id}
          AND g.season = {season}
          AND g.game_id NOT IN (SELECT game_id FROM target_player_games)
    ),

    teammate_stats_with AS (
        -- Teammate stats when target player WAS playing
        SELECT
            t.player_id,
            t.player,
            COUNT(DISTINCT t.game_id) AS games_with,
            AVG(t.pts) AS pts_with,
            AVG(t.reb) AS reb_with,
            AVG(t.ast) AS ast_with,
            AVG(t.stl) AS stl_with,
            AVG(t.blk) AS blk_with,
            AVG(t.turnover) AS tov_with,
            AVG(t.fg3m) AS fg3m_with,
            AVG(t.fg3a) AS fg3a_with,
            AVG(t.fgm) AS fgm_with,
            AVG(t.fga) AS fga_with,
            AVG(t.ftm) AS ftm_with,
            AVG(t.fta) AS fta_with,
            AVG(t.minutes) AS min_with,
            AVG(t.plus_minus) AS plus_minus_with
        FROM `{PLAYER_GAME_STATS_TABLE}` t
        JOIN target_player_games tpg ON t.game_id = tpg.game_id
        WHERE t.team_id = {target_team_id}
          AND t.season = {season}
          AND t.player_id != {player_id}
          AND t.minutes > 0
        GROUP BY t.player_id, t.player
    ),

    teammate_stats_without AS (
        -- Teammate stats when target player was OUT
        SELECT
            t.player_id,
            t.player,
            COUNT(DISTINCT t.game_id) AS games_without,
            AVG(t.pts) AS pts_without,
            AVG(t.reb) AS reb_without,
            AVG(t.ast) AS ast_without,
            AVG(t.stl) AS stl_without,
            AVG(t.blk) AS blk_without,
            AVG(t.turnover) AS tov_without,
            AVG(t.fg3m) AS fg3m_without,
            AVG(t.fg3a) AS fg3a_without,
            AVG(t.fgm) AS fgm_without,
            AVG(t.fga) AS fga_without,
            AVG(t.ftm) AS ftm_without,
            AVG(t.fta) AS fta_without,
            AVG(t.minutes) AS min_without,
            AVG(t.plus_minus) AS plus_minus_without
        FROM `{PLAYER_GAME_STATS_TABLE}` t
        JOIN target_player_missed tpm ON t.game_id = tpm.game_id
        WHERE t.team_id = {target_team_id}
          AND t.season = {season}
          AND t.player_id != {player_id}
          AND t.minutes > 0
        GROUP BY t.player_id, t.player
    )

    SELECT
        w.player_id,
        w.player AS teammate_name,
        w.games_with,
        COALESCE(wo.games_without, 0) AS games_without,

        -- Stats WITH target player
        ROUND(w.pts_with, 1) AS pts_with,
        ROUND(w.reb_with, 1) AS reb_with,
        ROUND(w.ast_with, 1) AS ast_with,
        ROUND(w.min_with, 1) AS min_with,
        ROUND(w.fg3m_with, 1) AS fg3m_with,
        ROUND(w.plus_minus_with, 1) AS plus_minus_with,

        -- Stats WITHOUT target player
        ROUND(wo.pts_without, 1) AS pts_without,
        ROUND(wo.reb_without, 1) AS reb_without,
        ROUND(wo.ast_without, 1) AS ast_without,
        ROUND(wo.min_without, 1) AS min_without,
        ROUND(wo.fg3m_without, 1) AS fg3m_without,
        ROUND(wo.plus_minus_without, 1) AS plus_minus_without,

        -- Differences (positive = better without target player)
        ROUND(wo.pts_without - w.pts_with, 1) AS pts_diff,
        ROUND(wo.reb_without - w.reb_with, 1) AS reb_diff,
        ROUND(wo.ast_without - w.ast_with, 1) AS ast_diff,
        ROUND(wo.min_without - w.min_with, 1) AS min_diff,
        ROUND(wo.fg3m_without - w.fg3m_with, 1) AS fg3m_diff,
        ROUND(wo.plus_minus_without - w.plus_minus_with, 1) AS plus_minus_diff

    FROM teammate_stats_with w
    LEFT JOIN teammate_stats_without wo ON w.player_id = wo.player_id

    WHERE w.games_with >= {min_games_with}
      AND COALESCE(wo.games_without, 0) >= {min_games_without}

    ORDER BY wo.pts_without - w.pts_with DESC
    """

    print(f"[WOWY] Running WOWY analysis query...")
    teammate_rows = [dict(r) for r in client.query(wowy_query).result()]

    # Also get team-level stats (wins, total points)
    team_query = f"""
    WITH target_player_games AS (
        SELECT DISTINCT game_id
        FROM `{PLAYER_GAME_STATS_TABLE}`
        WHERE player_id = {player_id}
          AND season = {season}
          AND minutes > 0
    ),

    target_player_missed AS (
        SELECT DISTINCT g.game_id
        FROM `{PLAYER_GAME_STATS_TABLE}` g
        WHERE g.team_id = {target_team_id}
          AND g.season = {season}
          AND g.game_id NOT IN (SELECT game_id FROM target_player_games)
    ),

    team_stats_with AS (
        SELECT
            COUNT(DISTINCT t.game_id) AS games_with,
            SUM(t.pts) AS total_pts_with
        FROM `{PLAYER_GAME_STATS_TABLE}` t
        JOIN target_player_games tpg ON t.game_id = tpg.game_id
        WHERE t.team_id = {target_team_id}
          AND t.season = {season}
    ),

    team_stats_without AS (
        SELECT
            COUNT(DISTINCT t.game_id) AS games_without,
            SUM(t.pts) AS total_pts_without
        FROM `{PLAYER_GAME_STATS_TABLE}` t
        JOIN target_player_missed tpm ON t.game_id = tpm.game_id
        WHERE t.team_id = {target_team_id}
          AND t.season = {season}
    )

    SELECT
        w.games_with AS team_games_with,
        wo.games_without AS team_games_without,
        ROUND(w.total_pts_with / NULLIF(w.games_with, 0), 1) AS team_ppg_with,
        ROUND(wo.total_pts_without / NULLIF(wo.games_without, 0), 1) AS team_ppg_without
    FROM team_stats_with w, team_stats_without wo
    """

    team_rows = list(client.query(team_query).result())
    team_stats = dict(team_rows[0]) if team_rows else {}

    print(f"[WOWY] Found {len(teammate_rows)} teammates with sufficient games")

    return {
        "status": "ok",
        "target_player": {
            "player_id": player_id,
            "player_name": target_player_name,
            "team_id": target_team_id,
            "team": target_team,
        },
        "season": season,
        "team_impact": {
            "games_with": team_stats.get("team_games_with", 0),
            "games_without": team_stats.get("team_games_without", 0),
            "team_ppg_with": team_stats.get("team_ppg_with"),
            "team_ppg_without": team_stats.get("team_ppg_without"),
            "team_ppg_diff": round(
                (team_stats.get("team_ppg_without") or 0) - (team_stats.get("team_ppg_with") or 0), 1
            ) if team_stats.get("team_ppg_with") and team_stats.get("team_ppg_without") else None,
        },
        "teammates": teammate_rows,
        "teammate_count": len(teammate_rows),
    }


def get_wowy_for_injured_players(
    *,
    team_id: Optional[int] = None,
    status_filter: Optional[list[str]] = None,
    season: Optional[int] = None,
) -> list[dict[str, Any]]:
    """
    Get WOWY analysis for all currently injured players.

    Args:
        team_id: Optional team filter
        status_filter: Optional status filter (e.g., ["Out", "Doubtful"])
        season: Season to analyze

    Returns:
        List of WOWY analyses for each injured player
    """
    client = get_bq_client()

    if season is None:
        season = get_current_season()

    # Get currently injured players
    where_clauses = ["TRUE"]

    if team_id:
        where_clauses.append(f"team_id = {team_id}")

    if status_filter:
        status_list = ", ".join(f"'{s}'" for s in status_filter)
        where_clauses.append(f"status IN ({status_list})")

    where_sql = " AND ".join(where_clauses)

    injuries_query = f"""
    SELECT DISTINCT
        player_id,
        player_name,
        team_id,
        team_abbreviation,
        status,
        injury_type
    FROM `{INJURIES_TABLE}`
    WHERE {where_sql}
    ORDER BY team_abbreviation, player_name
    """

    injured_players = [dict(r) for r in client.query(injuries_query).result()]

    print(f"[WOWY] Found {len(injured_players)} injured players")

    results = []
    for player in injured_players:
        print(f"[WOWY] Analyzing: {player['player_name']} ({player['team_abbreviation']})")

        wowy = analyze_wowy_for_player(
            player["player_id"],
            season=season,
            min_games_with=3,  # Lower threshold for injured player analysis
            min_games_without=1,
        )

        if wowy.get("status") == "ok":
            results.append({
                "injured_player": {
                    "player_id": player["player_id"],
                    "player_name": player["player_name"],
                    "team": player["team_abbreviation"],
                    "status": player["status"],
                    "injury_type": player["injury_type"],
                },
                "team_impact": wowy.get("team_impact"),
                "teammates": wowy.get("teammates", []),
                "teammate_count": wowy.get("teammate_count", 0),
            })

    return results


def get_top_beneficiaries(
    player_id: int,
    *,
    stat: str = "pts",
    limit: int = 5,
    season: Optional[int] = None,
) -> list[dict[str, Any]]:
    """
    Get teammates who benefit most when a player is out.

    Args:
        player_id: The player whose absence we're analyzing
        stat: Stat to sort by ("pts", "reb", "ast", "fg3m", "min")
        limit: Number of top beneficiaries to return
        season: Season to analyze

    Returns:
        List of top beneficiaries with their stat increases
    """
    wowy = analyze_wowy_for_player(player_id, season=season)

    if wowy.get("status") != "ok":
        return []

    teammates = wowy.get("teammates", [])

    # Sort by the diff column for the requested stat
    diff_col = f"{stat}_diff"
    sorted_teammates = sorted(
        teammates,
        key=lambda x: x.get(diff_col) or 0,
        reverse=True,
    )

    return sorted_teammates[:limit]
