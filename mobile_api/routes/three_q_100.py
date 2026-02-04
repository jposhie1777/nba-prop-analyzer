from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from google.api_core.exceptions import NotFound

from bq import get_bq_client
from managed_live_ingest import nba_today
from three_q_100 import fetch_three_q_100_rows, query_three_q_100_rows

router = APIRouter(prefix="/analytics", tags=["Analytics"])


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: _serialize_value(value) for key, value in row.items()}


def _group_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    games: Dict[int, Dict[str, Any]] = {}

    for row in rows:
        game_id = row["game_id"]
        if game_id not in games:
            games[game_id] = {
                "game_id": game_id,
                "game_date": row.get("game_date"),
                "start_time_est": row.get("start_time_est"),
                "home_team_abbr": row.get("home_team_abbr"),
                "away_team_abbr": row.get("away_team_abbr"),
                "teams": [],
            }

        games[game_id]["teams"].append(
            {
                "team_abbr": row.get("team_abbr"),
                "opponent_abbr": row.get("opponent_abbr"),
                "side": row.get("side"),
                "games_played": row.get("games_played"),
                "games_defended": row.get("games_defended"),
                "avg_3q_points": row.get("avg_3q_points"),
                "avg_3q_allowed": row.get("avg_3q_allowed"),
                "hit_100_rate": row.get("hit_100_rate"),
                "allow_100_rate": row.get("allow_100_rate"),
                "predicted_hit_rate": row.get("predicted_hit_rate"),
                "predicted_3q_points": row.get("predicted_3q_points"),
            }
        )

    for game in games.values():
        game["teams"].sort(
            key=lambda team: 0 if team.get("side") == "AWAY" else 1
        )

    return sorted(
        games.values(),
        key=lambda game: game.get("start_time_est") or "",
    )


@router.get("/three-q-100")
def get_three_q_100(
    game_date: Optional[date] = Query(None),
    refresh: bool = Query(False),
):
    query_date = game_date or nba_today()
    client = get_bq_client()

    if refresh:
        rows = query_three_q_100_rows(client, query_date)
    else:
        try:
            rows = fetch_three_q_100_rows(client, query_date)
        except NotFound:
            rows = query_three_q_100_rows(client, query_date)

    serialized = [_serialize_row(row) for row in rows]
    games = _group_rows(serialized)
    generated_at = serialized[0].get("generated_at") if serialized else None

    return {
        "game_date": query_date.isoformat(),
        "generated_at": generated_at,
        "count": len(serialized),
        "games": games,
    }
