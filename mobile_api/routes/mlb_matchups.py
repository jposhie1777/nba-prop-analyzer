from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(tags=["MLB Matchups"])

NY_TZ = ZoneInfo("America/New_York")
MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"

PROPFINDER_DATASET = os.getenv("PROPFINDER_DATASET", "propfinder")
HR_PICKS_TABLE = os.getenv("PROPFINDER_HR_PICKS_TABLE", f"{PROPFINDER_DATASET}.hr_picks_daily")
PITCHER_MATCHUP_TABLE = os.getenv(
    "PROPFINDER_PITCHER_MATCHUP_TABLE",
    f"{PROPFINDER_DATASET}.raw_pitcher_matchup",
)


def _today_et_iso() -> str:
    return datetime.now(NY_TZ).date().isoformat()


def _qualified_table(client: bigquery.Client, table_ref: str) -> str:
    parts = table_ref.split(".")
    if len(parts) == 2:
        return f"`{client.project}.{parts[0]}.{parts[1]}`"
    if len(parts) == 3:
        return f"`{table_ref}`"
    raise ValueError(f"Unsupported BigQuery table reference: {table_ref}")


def _query(
    client: bigquery.Client,
    sql: str,
    params: List[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter],
) -> List[Dict[str, Any]]:
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    )
    return [dict(row) for row in job.result()]


def _safe_query(
    client: bigquery.Client,
    sql: str,
    params: List[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter],
) -> List[Dict[str, Any]]:
    try:
        return _query(client, sql, params)
    except Exception:
        return []


def _fetch_schedule_raw(params: Dict[str, Any]) -> Dict[str, Any]:
    query = urlencode(params)
    url = f"{MLB_SCHEDULE_URL}?{query}"
    with urlopen(url, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def _parse_schedule_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for date_block in payload.get("dates", []):
        for game in date_block.get("games", []):
            teams = game.get("teams", {})
            home = teams.get("home", {}) or {}
            away = teams.get("away", {}) or {}
            home_team = (home.get("team") or {}).get("name")
            away_team = (away.get("team") or {}).get("name")
            out.append(
                {
                    "game_pk": game.get("gamePk"),
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_team_id": (home.get("team") or {}).get("id"),
                    "away_team_id": (away.get("team") or {}).get("id"),
                    "start_time_utc": game.get("gameDate"),
                    "venue_name": (game.get("venue") or {}).get("name"),
                    "home_pitcher_name": (home.get("probablePitcher") or {}).get("fullName"),
                    "away_pitcher_name": (away.get("probablePitcher") or {}).get("fullName"),
                }
            )
    out.sort(key=lambda row: row.get("start_time_utc") or "")
    return out


def _fetch_schedule_for_today() -> List[Dict[str, Any]]:
    try:
        payload = _fetch_schedule_raw(
            {
                "sportId": 1,
                "date": _today_et_iso(),
                "hydrate": "probablePitcher,team,venue",
            }
        )
    except Exception:
        return []
    return _parse_schedule_rows(payload)


def _fetch_schedule_for_game(game_pk: int) -> Optional[Dict[str, Any]]:
    try:
        payload = _fetch_schedule_raw(
            {
                "sportId": 1,
                "gamePk": game_pk,
                "hydrate": "probablePitcher,team,venue",
            }
        )
    except Exception:
        return None
    rows = _parse_schedule_rows(payload)
    return rows[0] if rows else None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_flags(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if not isinstance(value, str):
        return [str(value)]
    text = value.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return [str(parsed)]
    except Exception:
        return [text]


@router.get("/mlb/matchups/upcoming")
def mlb_matchups_upcoming(
    limit: int = Query(default=20, ge=1, le=100),
):
    schedule_rows = _fetch_schedule_for_today()
    if not schedule_rows:
        return []

    client = get_bq_client()
    today = _today_et_iso()
    hr_table = _qualified_table(client, HR_PICKS_TABLE)

    summary_rows = _safe_query(
        client,
        f"""
        SELECT
          CAST(game_pk AS INT64) AS game_pk,
          COUNT(*) AS picks_count,
          MAX(score) AS top_score,
          ARRAY_AGG(grade ORDER BY score DESC LIMIT 1)[SAFE_OFFSET(0)] AS top_grade
        FROM {hr_table}
        WHERE run_date = @run_date
        GROUP BY game_pk
        """,
        [bigquery.ScalarQueryParameter("run_date", "DATE", today)],
    )
    summary_map = {int(row["game_pk"]): row for row in summary_rows if row.get("game_pk") is not None}

    rows: List[Dict[str, Any]] = []
    for game in schedule_rows[:limit]:
        game_pk = _safe_int(game.get("game_pk"))
        summary = summary_map.get(game_pk) if game_pk is not None else None
        rows.append(
            {
                "game_pk": game_pk,
                "home_team": game.get("home_team"),
                "away_team": game.get("away_team"),
                "start_time_utc": game.get("start_time_utc"),
                "venue_name": game.get("venue_name"),
                "home_pitcher_name": game.get("home_pitcher_name"),
                "away_pitcher_name": game.get("away_pitcher_name"),
                "has_model_data": bool(summary),
                "picks_count": _safe_int(summary.get("picks_count")) if summary else 0,
                "top_score": _safe_float(summary.get("top_score")) if summary else None,
                "top_grade": summary.get("top_grade") if summary else None,
            }
        )
    return rows


@router.get("/mlb/matchups/{game_pk}")
def mlb_matchup_detail(game_pk: int):
    client = get_bq_client()
    today = _today_et_iso()
    hr_table = _qualified_table(client, HR_PICKS_TABLE)
    pitcher_table = _qualified_table(client, PITCHER_MATCHUP_TABLE)

    picks = _safe_query(
        client,
        f"""
        SELECT
          CAST(game_pk AS INT64) AS game_pk,
          batter_id,
          batter_name,
          bat_side,
          pitcher_id,
          pitcher_name,
          pitcher_hand,
          score,
          grade,
          why,
          flags,
          iso,
          slg,
          l15_ev,
          l15_barrel_pct,
          season_ev,
          season_barrel_pct,
          l15_hard_hit_pct,
          hr_fb_pct,
          p_hr9_vs_hand,
          p_hr_fb_pct,
          p_barrel_pct,
          p_fb_pct,
          p_hard_hit_pct,
          p_iso_allowed
        FROM {hr_table}
        WHERE run_date = @run_date
          AND CAST(game_pk AS INT64) = @game_pk
        ORDER BY score DESC, batter_name ASC
        """,
        [
            bigquery.ScalarQueryParameter("run_date", "DATE", today),
            bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
        ],
    )

    pitcher_splits = _safe_query(
        client,
        f"""
        SELECT
          CAST(game_pk AS INT64) AS game_pk,
          pitcher_id,
          pitcher_name,
          pitcher_hand,
          opp_team_id,
          split,
          ip,
          home_runs,
          hr_per_9,
          barrel_pct,
          hard_hit_pct,
          fb_pct,
          hr_fb_pct,
          whip,
          woba
        FROM {pitcher_table}
        WHERE run_date = @run_date
          AND CAST(game_pk AS INT64) = @game_pk
          AND split IN ('Season', 'vsLHB', 'vsRHB')
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY pitcher_id, split
          ORDER BY ingested_at DESC NULLS LAST
        ) = 1
        ORDER BY pitcher_name ASC, split ASC
        """,
        [
            bigquery.ScalarQueryParameter("run_date", "DATE", today),
            bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
        ],
    )

    grade_counts = {"IDEAL": 0, "FAVORABLE": 0, "AVERAGE": 0, "AVOID": 0}
    pitcher_groups: Dict[int, Dict[str, Any]] = {}

    for split_row in pitcher_splits:
        pitcher_id = _safe_int(split_row.get("pitcher_id"))
        if pitcher_id is None:
            continue
        group = pitcher_groups.setdefault(
            pitcher_id,
            {
                "pitcher_id": pitcher_id,
                "pitcher_name": split_row.get("pitcher_name"),
                "pitcher_hand": split_row.get("pitcher_hand"),
                "opp_team_id": _safe_int(split_row.get("opp_team_id")),
                "splits": {},
                "batters": [],
            },
        )
        split_name = split_row.get("split") or "Season"
        group["splits"][split_name] = {
            "ip": _safe_float(split_row.get("ip")),
            "home_runs": _safe_int(split_row.get("home_runs")),
            "hr_per_9": _safe_float(split_row.get("hr_per_9")),
            "barrel_pct": _safe_float(split_row.get("barrel_pct")),
            "hard_hit_pct": _safe_float(split_row.get("hard_hit_pct")),
            "fb_pct": _safe_float(split_row.get("fb_pct")),
            "hr_fb_pct": _safe_float(split_row.get("hr_fb_pct")),
            "whip": _safe_float(split_row.get("whip")),
            "woba": _safe_float(split_row.get("woba")),
        }

    for pick in picks:
        pitcher_id = _safe_int(pick.get("pitcher_id"))
        if pitcher_id is None:
            continue
        group = pitcher_groups.setdefault(
            pitcher_id,
            {
                "pitcher_id": pitcher_id,
                "pitcher_name": pick.get("pitcher_name"),
                "pitcher_hand": pick.get("pitcher_hand"),
                "opp_team_id": None,
                "splits": {},
                "batters": [],
            },
        )
        grade = (pick.get("grade") or "").upper()
        if grade in grade_counts:
            grade_counts[grade] += 1
        group["batters"].append(
            {
                "batter_id": _safe_int(pick.get("batter_id")),
                "batter_name": pick.get("batter_name"),
                "bat_side": pick.get("bat_side"),
                "score": _safe_float(pick.get("score")),
                "grade": pick.get("grade"),
                "why": pick.get("why"),
                "flags": _parse_flags(pick.get("flags")),
                "iso": _safe_float(pick.get("iso")),
                "slg": _safe_float(pick.get("slg")),
                "l15_ev": _safe_float(pick.get("l15_ev")),
                "l15_barrel_pct": _safe_float(pick.get("l15_barrel_pct")),
                "season_ev": _safe_float(pick.get("season_ev")),
                "season_barrel_pct": _safe_float(pick.get("season_barrel_pct")),
                "l15_hard_hit_pct": _safe_float(pick.get("l15_hard_hit_pct")),
                "hr_fb_pct": _safe_float(pick.get("hr_fb_pct")),
                "p_hr9_vs_hand": _safe_float(pick.get("p_hr9_vs_hand")),
                "p_hr_fb_pct": _safe_float(pick.get("p_hr_fb_pct")),
                "p_barrel_pct": _safe_float(pick.get("p_barrel_pct")),
                "p_fb_pct": _safe_float(pick.get("p_fb_pct")),
                "p_hard_hit_pct": _safe_float(pick.get("p_hard_hit_pct")),
                "p_iso_allowed": _safe_float(pick.get("p_iso_allowed")),
            }
        )

    schedule = _fetch_schedule_for_game(game_pk)
    home_team = schedule.get("home_team") if schedule else None
    away_team = schedule.get("away_team") if schedule else None
    home_team_id = _safe_int(schedule.get("home_team_id")) if schedule else None
    away_team_id = _safe_int(schedule.get("away_team_id")) if schedule else None

    pitchers_out: List[Dict[str, Any]] = []
    for pitcher in pitcher_groups.values():
        offense_team = None
        opp_team_id = pitcher.get("opp_team_id")
        if opp_team_id is not None:
            if home_team_id is not None and opp_team_id == home_team_id:
                offense_team = home_team
            elif away_team_id is not None and opp_team_id == away_team_id:
                offense_team = away_team

        pitcher["offense_team"] = offense_team
        pitcher["batters"] = sorted(
            pitcher.get("batters", []),
            key=lambda row: row.get("score") or 0,
            reverse=True,
        )
        pitchers_out.append(pitcher)

    pitchers_out.sort(
        key=lambda row: (row["batters"][0].get("score") or 0) if row.get("batters") else 0,
        reverse=True,
    )

    return {
        "game_pk": game_pk,
        "run_date": today,
        "game": {
            "home_team": home_team,
            "away_team": away_team,
            "start_time_utc": schedule.get("start_time_utc") if schedule else None,
            "venue_name": schedule.get("venue_name") if schedule else None,
            "home_pitcher_name": schedule.get("home_pitcher_name") if schedule else None,
            "away_pitcher_name": schedule.get("away_pitcher_name") if schedule else None,
        },
        "grade_counts": grade_counts,
        "pitchers": pitchers_out,
    }
