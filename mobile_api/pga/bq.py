from __future__ import annotations

import json
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence

from google.cloud import bigquery

from bq import get_bq_client

DATASET = os.getenv("PGA_DATASET", "pga_data")
PLAYERS_TABLE = os.getenv("PGA_PLAYERS_TABLE", "players_active")
ROUND_SCORES_TABLE = os.getenv("PGA_ROUND_SCORES_TABLE", "tournament_round_scores")
PAIRINGS_VIEW = os.getenv("PGA_PAIRINGS_VIEW", "v_pairings_latest")


def _table(client: bigquery.Client, table: str) -> str:
    return f"`{client.project}.{DATASET}.{table}`"


def _run_query(
    client: bigquery.Client,
    query: str,
    params: Sequence[bigquery.QueryParameter],
) -> List[Dict[str, Any]]:
    job_config = bigquery.QueryJobConfig(query_parameters=list(params))
    rows = client.query(query, job_config=job_config).result()
    return [dict(row) for row in rows]


def _normalize_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def _normalize_int_list(value: Any) -> Optional[List[int]]:
    if value is None:
        return None
    if isinstance(value, list):
        return [int(v) for v in value if v is not None]
    return [int(value)]


def _iso(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _parse_courses(value: Any) -> List[Dict[str, Any]]:
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        payload = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return payload if isinstance(payload, list) else []


def _player_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    first_name = row.get("first_name") or row.get("player_first_name")
    last_name = row.get("last_name") or row.get("player_last_name")
    display_name = (
        row.get("display_name")
        or row.get("player_display_name")
        or " ".join(filter(None, [first_name, last_name])).strip()
    )
    return {
        "id": row.get("player_id") or row.get("id"),
        "first_name": first_name,
        "last_name": last_name,
        "display_name": display_name,
        "country": row.get("country") or row.get("player_country"),
        "country_code": row.get("country_code") or row.get("player_country_code"),
        "owgr": row.get("owgr") or row.get("player_owgr"),
        "active": row.get("active") if row.get("active") is not None else row.get("player_active"),
    }


def _course_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": row.get("course_id") or row.get("id"),
        "name": row.get("name") or row.get("course_name"),
        "city": row.get("city") or row.get("course_city"),
        "state": row.get("state") or row.get("course_state"),
        "country": row.get("country") or row.get("course_country"),
        "par": row.get("par") or row.get("course_par"),
        "yardage": row.get("yardage") or row.get("course_yardage"),
        "architect": row.get("architect") or row.get("course_architect"),
        "fairway_grass": row.get("fairway_grass") or row.get("course_fairway_grass"),
        "green_grass": row.get("green_grass") or row.get("course_green_grass"),
    }


def _tournament_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    courses = _parse_courses(row.get("courses") or row.get("tournament_courses"))
    return {
        "id": row.get("tournament_id") or row.get("id"),
        "season": row.get("season"),
        "name": row.get("name") or row.get("tournament_name"),
        "start_date": _iso(row.get("start_date") or row.get("tournament_start_date")),
        "end_date": _iso(row.get("end_date") or row.get("tournament_end_date")),
        "city": row.get("city") or row.get("tournament_city"),
        "state": row.get("state") or row.get("tournament_state"),
        "country": row.get("country") or row.get("tournament_country"),
        "course_name": row.get("course_name") or row.get("tournament_course_name"),
        "status": row.get("status") or row.get("tournament_status"),
        "courses": courses,
    }


def _query_players(
    client: bigquery.Client,
    *,
    search: Optional[str],
    active: Optional[bool],
    player_ids: Optional[List[int]],
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Dict[str, Any]]:
    table = _table(client, PLAYERS_TABLE)
    conditions: List[str] = []
    params: List[bigquery.QueryParameter] = []

    if search:
        conditions.append(
            "(LOWER(display_name) LIKE @search OR LOWER(first_name) LIKE @search OR "
            "LOWER(last_name) LIKE @search)"
        )
        params.append(
            bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%")
        )

    if active is not None:
        conditions.append("active = @active")
        params.append(bigquery.ScalarQueryParameter("active", "BOOL", active))

    if player_ids:
        conditions.append("player_id IN UNNEST(@player_ids)")
        params.append(bigquery.ArrayQueryParameter("player_ids", "INT64", player_ids))

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT @limit OFFSET @offset"
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
        params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset or 0))

    query = f"""
    WITH latest AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY run_ts DESC) AS row_num
        FROM {table}
      )
      WHERE row_num = 1
    )
    SELECT
      player_id,
      first_name,
      last_name,
      display_name,
      country,
      country_code,
      owgr,
      active
    FROM latest
    {where_clause}
    ORDER BY display_name
    {limit_clause}
    """
    return _run_query(client, query, params)


def _query_courses(
    client: bigquery.Client,
    *,
    search: Optional[str],
    course_ids: Optional[List[int]],
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Dict[str, Any]]:
    table = _table(client, "courses")
    conditions: List[str] = []
    params: List[bigquery.QueryParameter] = []

    if search:
        conditions.append("LOWER(name) LIKE @search")
        params.append(
            bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%")
        )

    if course_ids:
        conditions.append("course_id IN UNNEST(@course_ids)")
        params.append(bigquery.ArrayQueryParameter("course_ids", "INT64", course_ids))

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT @limit OFFSET @offset"
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
        params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset or 0))

    query = f"""
    WITH latest AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY run_ts DESC) AS row_num
        FROM {table}
      )
      WHERE row_num = 1
    )
    SELECT
      course_id,
      name,
      city,
      state,
      country,
      par,
      yardage,
      architect,
      fairway_grass,
      green_grass
    FROM latest
    {where_clause}
    ORDER BY name
    {limit_clause}
    """
    return _run_query(client, query, params)


def _query_tournaments(
    client: bigquery.Client,
    *,
    season: Optional[int],
    status: Optional[str],
    tournament_ids: Optional[List[int]],
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Dict[str, Any]]:
    table = _table(client, "tournaments")
    conditions: List[str] = []
    params: List[bigquery.QueryParameter] = []

    if season is not None:
        conditions.append("season = @season")
        params.append(bigquery.ScalarQueryParameter("season", "INT64", season))

    if status:
        conditions.append("status = @status")
        params.append(bigquery.ScalarQueryParameter("status", "STRING", status))

    if tournament_ids:
        conditions.append("tournament_id IN UNNEST(@tournament_ids)")
        params.append(
            bigquery.ArrayQueryParameter("tournament_ids", "INT64", tournament_ids)
        )

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT @limit OFFSET @offset"
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
        params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset or 0))

    query = f"""
    WITH latest AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY tournament_id, season ORDER BY run_ts DESC) AS row_num
        FROM {table}
      )
      WHERE row_num = 1
    )
    SELECT
      tournament_id,
      season,
      name,
      start_date,
      end_date,
      city,
      state,
      country,
      course_name,
      status,
      courses
    FROM latest
    {where_clause}
    ORDER BY start_date DESC, name
    {limit_clause}
    """
    return _run_query(client, query, params)


def fetch_players_page(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    params = params or {}
    client = get_bq_client()

    search = params.get("search")
    active = _normalize_bool(params.get("active"))
    per_page = int(params.get("per_page") or 50)
    cursor = params.get("cursor")
    offset = int(cursor or 0)

    rows = _query_players(
        client,
        search=search,
        active=active,
        player_ids=None,
        limit=per_page + 1,
        offset=offset,
    )
    data = [_player_payload(row) for row in rows[:per_page]]
    next_cursor = offset + per_page if len(rows) > per_page else None
    return {"data": data, "meta": {"next_cursor": next_cursor}}


def fetch_courses_page(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    params = params or {}
    client = get_bq_client()

    search = params.get("search")
    per_page = int(params.get("per_page") or 50)
    cursor = params.get("cursor")
    offset = int(cursor or 0)

    rows = _query_courses(
        client,
        search=search,
        course_ids=None,
        limit=per_page + 1,
        offset=offset,
    )
    data = [_course_payload(row) for row in rows[:per_page]]
    next_cursor = offset + per_page if len(rows) > per_page else None
    return {"data": data, "meta": {"next_cursor": next_cursor}}


def fetch_tournaments_page(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    params = params or {}
    client = get_bq_client()

    season = params.get("season")
    status = params.get("status")
    tournament_ids = _normalize_int_list(params.get("tournament_ids"))
    per_page = int(params.get("per_page") or 50)
    cursor = params.get("cursor")
    offset = int(cursor or 0)

    rows = _query_tournaments(
        client,
        season=int(season) if season is not None else None,
        status=status,
        tournament_ids=tournament_ids,
        limit=per_page + 1,
        offset=offset,
    )
    data = [_tournament_payload(row) for row in rows[:per_page]]
    next_cursor = offset + per_page if len(rows) > per_page else None
    return {"data": data, "meta": {"next_cursor": next_cursor}}


def fetch_players(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    params = params or {}
    client = get_bq_client()
    search = params.get("search")
    active = _normalize_bool(params.get("active"))
    player_ids = _normalize_int_list(params.get("player_ids"))
    rows = _query_players(
        client,
        search=search,
        active=active,
        player_ids=player_ids,
    )
    return [_player_payload(row) for row in rows]


def fetch_courses(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    params = params or {}
    client = get_bq_client()
    search = params.get("search")
    course_ids = _normalize_int_list(params.get("course_ids"))
    rows = _query_courses(client, search=search, course_ids=course_ids)
    return [_course_payload(row) for row in rows]


def fetch_tournaments(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    params = params or {}
    client = get_bq_client()
    season = params.get("season")
    status = params.get("status")
    tournament_ids = _normalize_int_list(params.get("tournament_ids"))
    rows = _query_tournaments(
        client,
        season=int(season) if season is not None else None,
        status=status,
        tournament_ids=tournament_ids,
    )
    return [_tournament_payload(row) for row in rows]


def fetch_tournament_results(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    params = params or {}
    client = get_bq_client()
    project = client.project

    season = params.get("season")
    tournament_ids = _normalize_int_list(params.get("tournament_ids"))
    player_ids = _normalize_int_list(params.get("player_ids"))

    results_table = f"`{project}.{DATASET}.tournament_results`"
    players_table = f"`{project}.{DATASET}.{PLAYERS_TABLE}`"
    tournaments_table = f"`{project}.{DATASET}.tournaments`"

    conditions: List[str] = []
    query_params: List[bigquery.QueryParameter] = []

    if season is not None:
        conditions.append("r.season = @season")
        query_params.append(bigquery.ScalarQueryParameter("season", "INT64", int(season)))

    if tournament_ids:
        conditions.append("r.tournament_id IN UNNEST(@tournament_ids)")
        query_params.append(
            bigquery.ArrayQueryParameter("tournament_ids", "INT64", tournament_ids)
        )

    if player_ids:
        conditions.append("r.player_id IN UNNEST(@player_ids)")
        query_params.append(
            bigquery.ArrayQueryParameter("player_ids", "INT64", player_ids)
        )

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
    WITH latest_players AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY run_ts DESC) AS row_num
        FROM {players_table}
      )
      WHERE row_num = 1
    ),
    latest_tournaments AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY tournament_id, season ORDER BY run_ts DESC) AS row_num
        FROM {tournaments_table}
      )
      WHERE row_num = 1
    ),
    latest_results AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY tournament_id, player_id, season ORDER BY run_ts DESC) AS row_num
        FROM {results_table}
      )
      WHERE row_num = 1
    )
    SELECT
      r.season,
      r.tournament_id,
      COALESCE(t.name, r.tournament_name) AS tournament_name,
      COALESCE(t.start_date, r.tournament_start_date) AS tournament_start_date,
      t.end_date AS tournament_end_date,
      t.city AS tournament_city,
      t.state AS tournament_state,
      t.country AS tournament_country,
      t.course_name AS tournament_course_name,
      t.status AS tournament_status,
      t.courses AS tournament_courses,
      r.player_id,
      COALESCE(p.display_name, r.player_display_name) AS player_display_name,
      p.first_name AS player_first_name,
      p.last_name AS player_last_name,
      p.country AS player_country,
      p.country_code AS player_country_code,
      p.owgr AS player_owgr,
      p.active AS player_active,
      r.position,
      r.position_numeric,
      r.total_score,
      r.par_relative_score
    FROM latest_results r
    LEFT JOIN latest_tournaments t
      ON t.tournament_id = r.tournament_id AND t.season = r.season
    LEFT JOIN latest_players p
      ON p.player_id = r.player_id
    {where_clause}
    """

    rows = _run_query(client, query, query_params)
    payload: List[Dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "player": _player_payload(row),
                "tournament": _tournament_payload(row),
                "position": row.get("position"),
                "position_numeric": row.get("position_numeric"),
                "total_score": row.get("total_score"),
                "par_relative_score": row.get("par_relative_score"),
            }
        )
    return payload


def fetch_tournament_round_scores(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    params = params or {}
    client = get_bq_client()
    project = client.project

    season = params.get("season")
    tournament_ids = _normalize_int_list(params.get("tournament_ids"))
    player_ids = _normalize_int_list(params.get("player_ids"))
    round_numbers = _normalize_int_list(params.get("round_numbers"))

    table = f"`{project}.{DATASET}.{ROUND_SCORES_TABLE}`"

    conditions: List[str] = []
    query_params: List[bigquery.QueryParameter] = []

    if season is not None:
        conditions.append("season = @season")
        query_params.append(bigquery.ScalarQueryParameter("season", "INT64", int(season)))

    if tournament_ids:
        conditions.append("tournament_id IN UNNEST(@tournament_ids)")
        query_params.append(
            bigquery.ArrayQueryParameter("tournament_ids", "INT64", tournament_ids)
        )

    if player_ids:
        conditions.append("player_id IN UNNEST(@player_ids)")
        query_params.append(
            bigquery.ArrayQueryParameter("player_ids", "INT64", player_ids)
        )

    if round_numbers:
        conditions.append("round_number IN UNNEST(@round_numbers)")
        query_params.append(
            bigquery.ArrayQueryParameter("round_numbers", "INT64", round_numbers)
        )

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
    WITH latest AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY tournament_id, player_id, round_number
            ORDER BY run_ts DESC
          ) AS row_num
        FROM {table}
      )
      WHERE row_num = 1
    )
    SELECT
      season,
      tournament_id,
      tournament_name,
      tournament_start_date,
      round_number,
      round_date,
      player_id,
      player_display_name,
      round_score,
      par_relative_score,
      total_score
    FROM latest
    {where_clause}
    ORDER BY round_number
    """

    rows = _run_query(client, query, query_params)
    payload: List[Dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "season": row.get("season"),
                "tournament_id": row.get("tournament_id"),
                "tournament_name": row.get("tournament_name"),
                "tournament_start_date": _iso(row.get("tournament_start_date")),
                "round_number": row.get("round_number"),
                "round_date": _iso(row.get("round_date")),
                "player_id": row.get("player_id"),
                "player_display_name": row.get("player_display_name"),
                "round_score": row.get("round_score"),
                "par_relative_score": row.get("par_relative_score"),
                "total_score": row.get("total_score"),
            }
        )
    return payload


def fetch_tournament_course_stats(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    params = params or {}
    client = get_bq_client()
    project = client.project

    season = params.get("season")
    tournament_ids = _normalize_int_list(params.get("tournament_ids"))
    course_ids = _normalize_int_list(params.get("course_ids"))
    hole_number = params.get("hole_number")
    round_number = params.get("round_number")

    stats_table = f"`{project}.{DATASET}.tournament_course_stats`"
    courses_table = f"`{project}.{DATASET}.courses`"
    tournaments_table = f"`{project}.{DATASET}.tournaments`"

    conditions: List[str] = []
    query_params: List[bigquery.QueryParameter] = []

    if season is not None:
        conditions.append("s.season = @season")
        query_params.append(bigquery.ScalarQueryParameter("season", "INT64", int(season)))

    if tournament_ids:
        conditions.append("s.tournament_id IN UNNEST(@tournament_ids)")
        query_params.append(
            bigquery.ArrayQueryParameter("tournament_ids", "INT64", tournament_ids)
        )

    if course_ids:
        conditions.append("s.course_id IN UNNEST(@course_ids)")
        query_params.append(
            bigquery.ArrayQueryParameter("course_ids", "INT64", course_ids)
        )

    if hole_number is not None:
        conditions.append("s.hole_number = @hole_number")
        query_params.append(
            bigquery.ScalarQueryParameter("hole_number", "INT64", int(hole_number))
        )

    if round_number is not None:
        conditions.append("s.round_number = @round_number")
        query_params.append(
            bigquery.ScalarQueryParameter("round_number", "INT64", int(round_number))
        )

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
    WITH latest_courses AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY run_ts DESC) AS row_num
        FROM {courses_table}
      )
      WHERE row_num = 1
    ),
    latest_tournaments AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY tournament_id, season ORDER BY run_ts DESC) AS row_num
        FROM {tournaments_table}
      )
      WHERE row_num = 1
    ),
    latest_stats AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY tournament_id, course_id, season, hole_number, round_number
            ORDER BY run_ts DESC
          ) AS row_num
        FROM {stats_table}
      )
      WHERE row_num = 1
    )
    SELECT
      s.season,
      s.tournament_id,
      t.name AS tournament_name,
      t.start_date AS tournament_start_date,
      t.end_date AS tournament_end_date,
      t.city AS tournament_city,
      t.state AS tournament_state,
      t.country AS tournament_country,
      t.course_name AS tournament_course_name,
      t.status AS tournament_status,
      t.courses AS tournament_courses,
      s.course_id,
      c.name AS course_name,
      c.city AS course_city,
      c.state AS course_state,
      c.country AS course_country,
      c.par AS course_par,
      c.yardage AS course_yardage,
      c.architect AS course_architect,
      c.fairway_grass AS course_fairway_grass,
      c.green_grass AS course_green_grass,
      s.hole_number,
      s.round_number,
      s.scoring_average,
      s.scoring_diff,
      s.difficulty_rank,
      s.eagles,
      s.birdies,
      s.pars,
      s.bogeys,
      s.double_bogeys
    FROM latest_stats s
    LEFT JOIN latest_tournaments t
      ON t.tournament_id = s.tournament_id AND t.season = s.season
    LEFT JOIN latest_courses c
      ON c.course_id = s.course_id
    {where_clause}
    """

    rows = _run_query(client, query, query_params)
    payload: List[Dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "tournament": _tournament_payload(row),
                "course": _course_payload(row),
                "hole_number": row.get("hole_number"),
                "round_number": row.get("round_number"),
                "scoring_average": row.get("scoring_average"),
                "scoring_diff": row.get("scoring_diff"),
                "difficulty_rank": row.get("difficulty_rank"),
                "eagles": row.get("eagles"),
                "birdies": row.get("birdies"),
                "pars": row.get("pars"),
                "bogeys": row.get("bogeys"),
                "double_bogeys": row.get("double_bogeys"),
            }
        )
    return payload


def fetch_round_pairings(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Return the most-recent pairing snapshot for one or more rounds.

    Supported params:
        tournament_id  – PGA Tour tournament ID, e.g. ``"R2026010"``
        round_numbers  – int or list[int]; omit to return all rounds
        group_number   – int; filter to a single group
        player_ids     – list[str]; filter to specific players
    """
    params = params or {}
    client = get_bq_client()
    project = client.project

    view = f"`{project}.{DATASET}.{PAIRINGS_VIEW}`"
    conditions: List[str] = []
    query_params: List[bigquery.QueryParameter] = []

    tournament_id = params.get("tournament_id")
    if tournament_id:
        conditions.append("tournament_id = @tournament_id")
        query_params.append(
            bigquery.ScalarQueryParameter("tournament_id", "STRING", tournament_id)
        )

    round_numbers = _normalize_int_list(params.get("round_numbers"))
    if round_numbers:
        conditions.append("round_number IN UNNEST(@round_numbers)")
        query_params.append(
            bigquery.ArrayQueryParameter("round_numbers", "INT64", round_numbers)
        )

    group_number = params.get("group_number")
    if group_number is not None:
        conditions.append("group_number = @group_number")
        query_params.append(
            bigquery.ScalarQueryParameter("group_number", "INT64", int(group_number))
        )

    player_ids = params.get("player_ids")
    if player_ids:
        conditions.append("player_id IN UNNEST(@player_ids)")
        query_params.append(
            bigquery.ArrayQueryParameter("player_ids", "STRING", list(player_ids))
        )

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
    SELECT
      tournament_id,
      round_number,
      round_status,
      group_number,
      tee_time,
      start_hole,
      back_nine,
      course_id,
      course_name,
      player_id,
      player_display_name,
      player_first_name,
      player_last_name,
      country,
      world_rank,
      amateur,
      run_ts
    FROM {view}
    {where_clause}
    ORDER BY round_number, group_number, player_display_name
    """

    rows = _run_query(client, query, query_params)
    return [
        {
            "tournament_id": row.get("tournament_id"),
            "round_number": row.get("round_number"),
            "round_status": row.get("round_status"),
            "group_number": row.get("group_number"),
            "tee_time": row.get("tee_time"),
            "start_hole": row.get("start_hole"),
            "back_nine": row.get("back_nine"),
            "course_id": row.get("course_id"),
            "course_name": row.get("course_name"),
            "player_id": row.get("player_id"),
            "player_display_name": row.get("player_display_name"),
            "player_first_name": row.get("player_first_name"),
            "player_last_name": row.get("player_last_name"),
            "country": row.get("country"),
            "world_rank": row.get("world_rank"),
            "amateur": row.get("amateur"),
            "snapshot_ts": _iso(row.get("run_ts")),
        }
        for row in rows
    ]


def fetch_pairings_analytics(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Query v_pairings_analytics — the pre-joined view that combines
    v_pairings_latest with per-player form / placement stats computed
    from the last 3 seasons of tournament_results.

    One BigQuery round-trip returns everything the pairings endpoint needs.

    Supported params:
        tournament_id  – STRING, e.g. ``"R2026010"``
        round_numbers  – int or list[int]
    """
    params = params or {}
    client = get_bq_client()
    project = client.project

    view = f"`{project}.{DATASET}.v_pairings_analytics`"
    conditions: List[str] = []
    query_params: List[bigquery.QueryParameter] = []

    tournament_id = params.get("tournament_id")
    if tournament_id:
        conditions.append("tournament_id = @tournament_id")
        query_params.append(
            bigquery.ScalarQueryParameter("tournament_id", "STRING", tournament_id)
        )

    round_numbers = _normalize_int_list(params.get("round_numbers"))
    if round_numbers:
        conditions.append("round_number IN UNNEST(@round_numbers)")
        query_params.append(
            bigquery.ArrayQueryParameter("round_numbers", "INT64", round_numbers)
        )

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
    SELECT
      tournament_id,
      round_number,
      round_status,
      group_number,
      tee_time,
      start_hole,
      back_nine,
      course_id,
      course_name,
      player_id,
      player_id_int,
      player_display_name,
      player_first_name,
      player_last_name,
      country,
      world_rank,
      amateur,
      run_ts,
      form_score,
      form_starts,
      avg_finish,
      top10_rate,
      top20_rate,
      cut_rate,
      placement_starts,
      top5_prob,
      top10_prob,
      top20_prob
    FROM {view}
    {where_clause}
    ORDER BY round_number, group_number, player_display_name
    """

    rows = _run_query(client, query, query_params)
    return [
        {
            "tournament_id":        row.get("tournament_id"),
            "round_number":         row.get("round_number"),
            "round_status":         row.get("round_status"),
            "group_number":         row.get("group_number"),
            "tee_time":             row.get("tee_time"),
            "start_hole":           row.get("start_hole"),
            "back_nine":            row.get("back_nine"),
            "course_id":            row.get("course_id"),
            "course_name":          row.get("course_name"),
            "player_id":            row.get("player_id"),
            "player_id_int":        row.get("player_id_int"),
            "player_display_name":  row.get("player_display_name"),
            "player_first_name":    row.get("player_first_name"),
            "player_last_name":     row.get("player_last_name"),
            "country":              row.get("country"),
            "world_rank":           row.get("world_rank"),
            "amateur":              row.get("amateur"),
            "snapshot_ts":          _iso(row.get("run_ts")),
            # analytics (None when player has < min_events history)
            "form_score":           row.get("form_score"),
            "form_starts":          row.get("form_starts"),
            "avg_finish":           row.get("avg_finish"),
            "top10_rate":           row.get("top10_rate"),
            "top20_rate":           row.get("top20_rate"),
            "cut_rate":             row.get("cut_rate"),
            "placement_starts":     row.get("placement_starts"),
            "top5_prob":            row.get("top5_prob"),
            "top10_prob":           row.get("top10_prob"),
            "top20_prob":           row.get("top20_prob"),
        }
        for row in rows
    ]


def fetch_course_holes(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    params = params or {}
    client = get_bq_client()
    project = client.project

    course_ids = _normalize_int_list(params.get("course_ids"))

    holes_table = f"`{project}.{DATASET}.course_holes`"
    courses_table = f"`{project}.{DATASET}.courses`"

    conditions: List[str] = []
    query_params: List[bigquery.QueryParameter] = []

    if course_ids:
        conditions.append("h.course_id IN UNNEST(@course_ids)")
        query_params.append(bigquery.ArrayQueryParameter("course_ids", "INT64", course_ids))

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
    WITH latest_courses AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY run_ts DESC) AS row_num
        FROM {courses_table}
      )
      WHERE row_num = 1
    ),
    latest_holes AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY course_id, hole_number ORDER BY run_ts DESC) AS row_num
        FROM {holes_table}
      )
      WHERE row_num = 1
    )
    SELECT
      h.course_id,
      c.name AS course_name,
      c.city AS course_city,
      c.state AS course_state,
      c.country AS course_country,
      c.par AS course_par,
      c.yardage AS course_yardage,
      c.architect AS course_architect,
      c.fairway_grass AS course_fairway_grass,
      c.green_grass AS course_green_grass,
      h.hole_number,
      h.par,
      h.yardage
    FROM latest_holes h
    LEFT JOIN latest_courses c
      ON c.course_id = h.course_id
    {where_clause}
    ORDER BY h.hole_number
    """

    rows = _run_query(client, query, query_params)
    payload: List[Dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "course": _course_payload(row),
                "hole_number": row.get("hole_number"),
                "par": row.get("par"),
                "yardage": row.get("yardage"),
            }
        )
    return payload
