# mobile_api/routes/hit_rate_matrix.py
"""
Hit Rate Matrix — shows how often each player clears various statistical
thresholds over their last N games, matching the PropFinder Hit Rate Matrix.
"""

from fastapi import APIRouter, Query
from typing import Optional
from google.cloud import bigquery
from collections import defaultdict

from bq import get_bq_client

router = APIRouter(tags=["hit-rate-matrix"])

DATASET = "nba"

# ── Category → game-log column mapping ────────────────────────────
CATEGORY_COL_MAP = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "threePointsMade": "three_points_made",
    "steals": "steals",
    "blocks": "blocks",
    "pointsReboundsAssists": "points_rebounds_assists",
    "pointsRebounds": "points_rebounds",
    "pointsAssists": "points_assists",
    "reboundAssists": "rebound_assists",
}

# ── Category → PropFinder raw_nba_props category name ─────────────
CATEGORY_PROP_MAP = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "threePointsMade": "threePointsMade",
    "steals": "steals",
    "blocks": "blocks",
    "pointsReboundsAssists": "pointsReboundsAssists",
    "pointsRebounds": "pointsRebounds",
    "pointsAssists": "pointsAssists",
    "reboundAssists": "reboundAssists",
}

# ── Thresholds per category ───────────────────────────────────────
CATEGORY_THRESHOLDS = {
    "points": [10, 15, 20, 25, 30, 35, 40, 45],
    "rebounds": [2, 4, 6, 8, 10, 12, 14, 16],
    "assists": [2, 4, 6, 8, 10, 12, 14],
    "threePointsMade": [1, 2, 3, 4, 5, 6, 7, 8],
    "steals": [1, 2, 3, 4, 5, 6],
    "blocks": [1, 2, 3, 4, 5, 6],
    "pointsReboundsAssists": [15, 20, 25, 30, 35, 40, 45, 50],
    "pointsRebounds": [10, 15, 20, 25, 30, 35, 40, 45],
    "pointsAssists": [10, 15, 20, 25, 30, 35, 40, 45],
    "reboundAssists": [4, 6, 8, 10, 12, 14, 16, 18],
}

# ── Game count → number of recent games ───────────────────────────
GAME_COUNT_MAP = {
    "L5": 5,
    "L10": 10,
    "L15": 15,
}


@router.get("/hit-rate-matrix")
def get_hit_rate_matrix(
    category: str = Query("points"),
    position: str = Query("all"),
    game_count: str = Query("L5"),
    game_ids: Optional[str] = None,  # comma-separated game IDs to filter
):
    client = get_bq_client()
    stat_col = CATEGORY_COL_MAP.get(category)
    prop_cat = CATEGORY_PROP_MAP.get(category)
    n_games = GAME_COUNT_MAP.get(game_count, 5)
    thresholds = CATEGORY_THRESHOLDS.get(category, [10, 15, 20, 25, 30, 35, 40, 45])

    if not stat_col or not prop_cat:
        return {"error": f"Unknown category: {category}", "players": [], "thresholds": []}

    # ── 1) Fetch today's props with odds & deep links ─────────────
    props_where = ["p.category = @category", "p.over_under = 'over'"]
    if position and position.lower() != "all":
        props_where.append("UPPER(p.position) = UPPER(@position)")
    if game_ids:
        game_id_list = [g.strip() for g in game_ids.split(",") if g.strip()]
        if game_id_list:
            placeholders = ", ".join([f"'{gid}'" for gid in game_id_list])
            props_where.append(f"p.game_id IN ({placeholders})")

    props_sql = f"""
    SELECT
      p.player_id,
      p.player_name,
      p.position,
      p.team_code,
      p.opp_team_code,
      p.line,
      p.matchup_rank,
      p.matchup_label,
      p.hit_rate_l5,
      p.hit_rate_l10,
      p.hit_rate_l20,
      p.avg_l10,
      p.dk_price,
      p.dk_deep_link,
      p.dk_event_id,
      p.dk_outcome_code,
      p.fd_price,
      p.fd_deep_link,
      p.fd_market_id,
      p.fd_selection_id,
      p.best_book,
      p.best_price,
      p.game_id
    FROM `{DATASET}.raw_nba_props` p
    WHERE p.run_date = (SELECT MAX(run_date) FROM `{DATASET}.raw_nba_props`)
      AND {" AND ".join(props_where)}
    ORDER BY p.player_name
    """

    props_params = [
        bigquery.ScalarQueryParameter("category", "STRING", prop_cat),
    ]
    if position and position.lower() != "all":
        props_params.append(
            bigquery.ScalarQueryParameter("position", "STRING", position.upper())
        )

    props_job = client.query(
        props_sql,
        job_config=bigquery.QueryJobConfig(query_parameters=props_params),
    )
    props_rows = [dict(r) for r in props_job.result()]

    if not props_rows:
        return {"players": [], "thresholds": thresholds, "games": []}

    # Collect unique player IDs
    player_ids = list({r["player_id"] for r in props_rows if r.get("player_id")})

    # ── 2) Fetch recent game logs for these players ───────────────
    # Get the latest run_date for game logs
    logs_sql = f"""
    WITH ranked AS (
      SELECT
        player_id,
        player_name,
        game_date,
        {stat_col} AS stat_value,
        ROW_NUMBER() OVER (
          PARTITION BY player_id
          ORDER BY game_date DESC
        ) AS rn
      FROM `{DATASET}.raw_nba_game_logs`
      WHERE run_date = (SELECT MAX(run_date) FROM `{DATASET}.raw_nba_game_logs`)
        AND player_id IN UNNEST(@player_ids)
        AND {stat_col} IS NOT NULL
    )
    SELECT player_id, player_name, game_date, stat_value, rn
    FROM ranked
    WHERE rn <= @n_games
    ORDER BY player_id, rn
    """

    logs_params = [
        bigquery.ArrayQueryParameter("player_ids", "STRING", player_ids),
        bigquery.ScalarQueryParameter("n_games", "INT64", n_games),
    ]

    logs_job = client.query(
        logs_sql,
        job_config=bigquery.QueryJobConfig(query_parameters=logs_params),
    )

    # Group game logs by player_id
    player_games = defaultdict(list)
    for row in logs_job.result():
        player_games[row["player_id"]].append({
            "game_date": str(row["game_date"]) if row["game_date"] else None,
            "stat_value": row["stat_value"],
        })

    # ── 3) Fetch unique games for the games dropdown ──────────────
    games_sql = f"""
    SELECT DISTINCT
      game_id,
      home_team_code,
      away_team_code
    FROM (
      SELECT
        game_id,
        team_code AS home_team_code,
        opp_team_code AS away_team_code
      FROM `{DATASET}.raw_nba_props`
      WHERE run_date = (SELECT MAX(run_date) FROM `{DATASET}.raw_nba_props`)
        AND is_home = TRUE
      UNION DISTINCT
      SELECT
        game_id,
        opp_team_code AS home_team_code,
        team_code AS away_team_code
      FROM `{DATASET}.raw_nba_props`
      WHERE run_date = (SELECT MAX(run_date) FROM `{DATASET}.raw_nba_props`)
        AND is_home = FALSE
    )
    ORDER BY game_id
    """
    games_job = client.query(games_sql)
    games = []
    seen_game_ids = set()
    for r in games_job.result():
        gid = r["game_id"]
        if gid not in seen_game_ids:
            seen_game_ids.add(gid)
            games.append({
                "game_id": gid,
                "label": f"{r['away_team_code']} @ {r['home_team_code']}",
            })

    # ── 4) Build the matrix ───────────────────────────────────────
    players = []
    for prop in props_rows:
        pid = prop["player_id"]
        game_stats = player_games.get(pid, [])
        total = len(game_stats)
        stat_values = [g["stat_value"] for g in game_stats]

        # Compute hit rate at each threshold
        cells = {}
        for t in thresholds:
            hit = sum(1 for v in stat_values if v is not None and v >= t)
            cells[str(t)] = {"hit": hit, "total": total}

        # Season average from game logs
        szn_avg = round(sum(v for v in stat_values if v is not None) / total, 1) if total > 0 else None

        players.append({
            "player_id": prop["player_id"],
            "player_name": prop["player_name"],
            "position": prop["position"],
            "team_code": prop["team_code"],
            "opp_team_code": prop["opp_team_code"],
            "line": prop["line"],
            "matchup_rank": prop["matchup_rank"],
            "matchup_label": prop["matchup_label"],
            "avg_l10": prop["avg_l10"],
            "dk_price": prop["dk_price"],
            "dk_deep_link": prop["dk_deep_link"],
            "dk_event_id": prop["dk_event_id"],
            "dk_outcome_code": prop["dk_outcome_code"],
            "fd_price": prop["fd_price"],
            "fd_deep_link": prop["fd_deep_link"],
            "fd_market_id": prop["fd_market_id"],
            "fd_selection_id": prop["fd_selection_id"],
            "best_book": prop["best_book"],
            "best_price": prop["best_price"],
            "game_id": prop["game_id"],
            "szn_avg": szn_avg,
            "cells": cells,
            "game_values": stat_values,
        })

    return {
        "thresholds": thresholds,
        "players": players,
        "games": games,
        "category": category,
        "game_count": game_count,
    }
