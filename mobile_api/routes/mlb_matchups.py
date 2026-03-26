from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen
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
GAME_WEATHER_TABLE = os.getenv(
    "PROPFINDER_GAME_WEATHER_TABLE",
    f"{PROPFINDER_DATASET}.raw_game_weather",
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


def _exception_message(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _fetch_schedule_raw(params: Dict[str, Any]) -> Dict[str, Any]:
    query = urlencode(params)
    url = f"{MLB_SCHEDULE_URL}?{query}"
    request = Request(
        url,
        headers={
            "User-Agent": "PulseSports/1.0",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=15) as response:
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
    today = datetime.now(NY_TZ).date()
    target_dates = [today, today + timedelta(days=1)]
    combined: List[Dict[str, Any]] = []
    seen_game_pks: set[int] = set()

    for target_date in target_dates:
        date_iso = target_date.isoformat()
        try:
            payload = _fetch_schedule_raw(
                {
                    "sportId": 1,
                    "date": date_iso,
                    "hydrate": "probablePitcher,team,venue",
                }
            )
        except Exception:
            continue

        rows = _parse_schedule_rows(payload)
        for row in rows:
            game_pk = _safe_int(row.get("game_pk"))
            if game_pk is not None and game_pk in seen_game_pks:
                continue
            if game_pk is not None:
                seen_game_pks.add(game_pk)
            combined.append(row)

        # If we found games for today, no need to query farther ahead.
        if rows and target_date == today:
            break

    combined.sort(key=lambda row: row.get("start_time_utc") or "")
    return combined


def _fetch_schedule_for_date_iso(date_iso: str) -> List[Dict[str, Any]]:
    try:
        payload = _fetch_schedule_raw(
            {
                "sportId": 1,
                "date": date_iso,
                "hydrate": "probablePitcher,team,venue",
            }
        )
    except Exception:
        return []
    return _parse_schedule_rows(payload)


def _fetch_schedule_for_date_iso_debug(date_iso: str) -> Dict[str, Any]:
    try:
        payload = _fetch_schedule_raw(
            {
                "sportId": 1,
                "date": date_iso,
                "hydrate": "probablePitcher,team,venue",
            }
        )
        rows = _parse_schedule_rows(payload)
        return {
            "date": date_iso,
            "ok": True,
            "rows_count": len(rows),
            "total_games": (payload.get("dates") or [{}])[0].get("totalGames", 0),
            "sample_game_pks": [row.get("game_pk") for row in rows[:5]],
        }
    except Exception as exc:
        return {
            "date": date_iso,
            "ok": False,
            "error": _exception_message(exc),
            "rows_count": 0,
            "total_games": 0,
            "sample_game_pks": [],
        }


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


def _normalized_name(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _pitcher_group_key(pitcher_id: Optional[int], pitcher_name: Any) -> Optional[str]:
    if pitcher_id is not None:
        return f"id:{pitcher_id}"
    name = _normalized_name(pitcher_name)
    if name:
        return f"name:{name}"
    return None


def _fetch_game_weather_map(
    client: bigquery.Client,
    weather_table_qualified: str,
    run_date: str,
    game_pks: Optional[List[int]] = None,
) -> Dict[int, Dict[str, Any]]:
    """Query raw_game_weather and return dict keyed by game_pk."""
    try:
        if game_pks:
            rows = _safe_query(
                client,
                f"""
                SELECT
                  CAST(game_pk AS INT64) AS game_pk,
                  weather_indicator,
                  game_temp,
                  wind_speed,
                  wind_dir,
                  wind_gust,
                  precip_prob,
                  conditions,
                  ballpark_name,
                  roof_type,
                  home_moneyline,
                  away_moneyline,
                  over_under,
                  weather_note
                FROM {weather_table_qualified}
                WHERE run_date = @run_date
                  AND CAST(game_pk AS INT64) IN UNNEST(@game_pks)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY ingested_at DESC) = 1
                """,
                [
                    bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                    bigquery.ArrayQueryParameter("game_pks", "INT64", game_pks),
                ],
            )
        else:
            rows = _safe_query(
                client,
                f"""
                SELECT
                  CAST(game_pk AS INT64) AS game_pk,
                  weather_indicator,
                  game_temp,
                  wind_speed,
                  wind_dir,
                  wind_gust,
                  precip_prob,
                  conditions,
                  ballpark_name,
                  roof_type,
                  home_moneyline,
                  away_moneyline,
                  over_under,
                  weather_note
                FROM {weather_table_qualified}
                WHERE run_date = @run_date
                QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY ingested_at DESC) = 1
                """,
                [bigquery.ScalarQueryParameter("run_date", "DATE", run_date)],
            )
        return {int(row["game_pk"]): row for row in rows if row.get("game_pk") is not None}
    except Exception:
        return {}


def _resolve_latest_run_date_for_game(
    client: bigquery.Client,
    table_ref_qualified: str,
    game_pk: int,
    preferred_date: str,
) -> str:
    row = _safe_query(
        client,
        f"""
        SELECT CAST(MAX(run_date) AS STRING) AS run_date
        FROM {table_ref_qualified}
        WHERE CAST(game_pk AS INT64) = @game_pk
        """,
        [bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk)],
    )
    latest = row[0].get("run_date") if row else None
    if isinstance(latest, str) and latest:
        return latest
    return _resolve_latest_run_date(client, table_ref_qualified, preferred_date)


def _resolve_latest_run_date(
    client: bigquery.Client,
    table_ref_qualified: str,
    preferred_date: str,
) -> str:
    row = _safe_query(
        client,
        f"""
        SELECT CAST(MAX(run_date) AS STRING) AS run_date
        FROM {table_ref_qualified}
        """,
        [],
    )
    latest = row[0].get("run_date") if row else None
    if isinstance(latest, str) and latest:
        return latest
    return preferred_date


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
    weather_table = _qualified_table(client, GAME_WEATHER_TABLE)
    run_date = _resolve_latest_run_date(client, hr_table, today)

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
        [bigquery.ScalarQueryParameter("run_date", "DATE", run_date)],
    )
    summary_map = {int(row["game_pk"]): row for row in summary_rows if row.get("game_pk") is not None}

    schedule_game_pks = [_safe_int(g.get("game_pk")) for g in schedule_rows if g.get("game_pk") is not None]
    weather_map = _fetch_game_weather_map(client, weather_table, run_date, schedule_game_pks)

    rows: List[Dict[str, Any]] = []
    for game in schedule_rows[:limit]:
        game_pk = _safe_int(game.get("game_pk"))
        summary = summary_map.get(game_pk) if game_pk is not None else None
        gw = weather_map.get(game_pk) if game_pk is not None else None
        rows.append(
            {
                "game_pk": game_pk,
                "home_team": game.get("home_team"),
                "away_team": game.get("away_team"),
                "start_time_utc": game.get("start_time_utc"),
                "venue_name": gw.get("ballpark_name") if gw else game.get("venue_name"),
                "home_pitcher_name": game.get("home_pitcher_name"),
                "away_pitcher_name": game.get("away_pitcher_name"),
                "has_model_data": bool(summary),
                "picks_count": _safe_int(summary.get("picks_count")) if summary else 0,
                "top_score": _safe_float(summary.get("top_score")) if summary else None,
                "top_grade": summary.get("top_grade") if summary else None,
                # Weather fields
                "weather_indicator": gw.get("weather_indicator") if gw else None,
                "game_temp": _safe_float(gw.get("game_temp")) if gw else None,
                "wind_speed": _safe_float(gw.get("wind_speed")) if gw else None,
                "wind_dir": _safe_int(gw.get("wind_dir")) if gw else None,
                "precip_prob": _safe_float(gw.get("precip_prob")) if gw else None,
                "conditions": gw.get("conditions") if gw else None,
                "ballpark_name": gw.get("ballpark_name") if gw else None,
                "roof_type": gw.get("roof_type") if gw else None,
                # Odds fields
                "home_moneyline": _safe_int(gw.get("home_moneyline")) if gw else None,
                "away_moneyline": _safe_int(gw.get("away_moneyline")) if gw else None,
                "over_under": _safe_float(gw.get("over_under")) if gw else None,
                "weather_note": gw.get("weather_note") if gw else None,
            }
        )
    return rows


@router.get("/mlb/matchups/upcoming/debug")
def mlb_matchups_upcoming_debug():
    now_et = datetime.now(NY_TZ)
    today = now_et.date()
    tomorrow = today + timedelta(days=1)
    today_iso = today.isoformat()
    tomorrow_iso = tomorrow.isoformat()

    today_debug = _fetch_schedule_for_date_iso_debug(today_iso)
    tomorrow_debug = _fetch_schedule_for_date_iso_debug(tomorrow_iso)

    schedule_rows = _fetch_schedule_for_today()
    game_pks = [row.get("game_pk") for row in schedule_rows]

    bq_status: Dict[str, Any]
    try:
        client = get_bq_client()
        hr_table = _qualified_table(client, HR_PICKS_TABLE)
        bq_rows = _safe_query(
            client,
            f"""
            SELECT COUNT(*) AS row_count
            FROM {hr_table}
            WHERE run_date = @run_date
            """,
            [bigquery.ScalarQueryParameter("run_date", "DATE", _today_et_iso())],
        )
        row_count = int(bq_rows[0]["row_count"]) if bq_rows else 0
        bq_status = {"ok": True, "today_row_count": row_count}
    except Exception as exc:
        bq_status = {"ok": False, "error": _exception_message(exc), "today_row_count": 0}

    return {
        "now_et": now_et.isoformat(),
        "today_et": today_iso,
        "tomorrow_et": tomorrow_iso,
        "schedule_today": today_debug,
        "schedule_tomorrow": tomorrow_debug,
        "combined_schedule_rows": len(schedule_rows),
        "combined_game_pks": game_pks[:20],
        "bq_status": bq_status,
        "upcoming_endpoint_return_count": len(mlb_matchups_upcoming(limit=20)),
    }


@router.get("/mlb/matchups/{game_pk}")
def mlb_matchup_detail(game_pk: int):
    client = get_bq_client()
    today = _today_et_iso()
    hr_table = _qualified_table(client, HR_PICKS_TABLE)
    pitcher_table = _qualified_table(client, PITCHER_MATCHUP_TABLE)
    weather_table = _qualified_table(client, GAME_WEATHER_TABLE)
    schedule = _fetch_schedule_for_game(game_pk)
    home_team = schedule.get("home_team") if schedule else None
    away_team = schedule.get("away_team") if schedule else None
    home_team_id = _safe_int(schedule.get("home_team_id")) if schedule else None
    away_team_id = _safe_int(schedule.get("away_team_id")) if schedule else None
    run_date = _resolve_latest_run_date_for_game(client, hr_table, game_pk, today)
    weather_map = _fetch_game_weather_map(client, weather_table, run_date, [game_pk])
    gw = weather_map.get(game_pk)

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
            bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
            bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
        ],
    )

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
          p_iso_allowed,
          IF(home_moneyline IS NOT NULL, home_moneyline, NULL) AS home_moneyline,
          IF(away_moneyline IS NOT NULL, away_moneyline, NULL) AS away_moneyline,
          over_under
        FROM {hr_table}
        WHERE run_date = @run_date
          AND CAST(game_pk AS INT64) = @game_pk
        ORDER BY score DESC, batter_name ASC
        """,
        [
            bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
            bigquery.ScalarQueryParameter("game_pk", "INT64", game_pk),
        ],
    )

    if not picks:
        pitcher_ids = sorted({_safe_int(row.get("pitcher_id")) for row in pitcher_splits if _safe_int(row.get("pitcher_id")) is not None})
        pitcher_names = sorted({_normalized_name(row.get("pitcher_name")) for row in pitcher_splits if _normalized_name(row.get("pitcher_name"))})
        team_names = sorted({_normalized_name(name) for name in [home_team, away_team] if _normalized_name(name)})

        if pitcher_ids or pitcher_names:
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
                  p_iso_allowed,
                  IF(home_moneyline IS NOT NULL, home_moneyline, NULL) AS home_moneyline,
                  IF(away_moneyline IS NOT NULL, away_moneyline, NULL) AS away_moneyline,
                  over_under
                FROM {hr_table}
                WHERE run_date = @run_date
                  AND (
                    (ARRAY_LENGTH(@pitcher_ids) > 0 AND CAST(pitcher_id AS INT64) IN UNNEST(@pitcher_ids))
                    OR (ARRAY_LENGTH(@pitcher_names) > 0 AND LOWER(CAST(pitcher_name AS STRING)) IN UNNEST(@pitcher_names))
                  )
                  AND (
                    ARRAY_LENGTH(@team_names) = 0
                    OR LOWER(CAST(home_team AS STRING)) IN UNNEST(@team_names)
                    OR LOWER(CAST(away_team AS STRING)) IN UNNEST(@team_names)
                  )
                ORDER BY score DESC, batter_name ASC
                """,
                [
                    bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                    bigquery.ArrayQueryParameter("pitcher_ids", "INT64", pitcher_ids),
                    bigquery.ArrayQueryParameter("pitcher_names", "STRING", pitcher_names),
                    bigquery.ArrayQueryParameter("team_names", "STRING", team_names),
                ],
            )

    grade_counts = {"IDEAL": 0, "FAVORABLE": 0, "AVERAGE": 0, "AVOID": 0}
    pitcher_groups: Dict[str, Dict[str, Any]] = {}

    for split_row in pitcher_splits:
        pitcher_id = _safe_int(split_row.get("pitcher_id"))
        group_key = _pitcher_group_key(pitcher_id, split_row.get("pitcher_name"))
        if group_key is None:
            continue
        group = pitcher_groups.setdefault(
            group_key,
            {
                "pitcher_id": pitcher_id,
                "pitcher_name": split_row.get("pitcher_name"),
                "pitcher_hand": split_row.get("pitcher_hand"),
                "opp_team_id": _safe_int(split_row.get("opp_team_id")),
                "splits": {},
                "batters": [],
            },
        )
        if group.get("pitcher_id") is None and pitcher_id is not None:
            group["pitcher_id"] = pitcher_id
        if not group.get("pitcher_name") and split_row.get("pitcher_name"):
            group["pitcher_name"] = split_row.get("pitcher_name")
        if not group.get("pitcher_hand") and split_row.get("pitcher_hand"):
            group["pitcher_hand"] = split_row.get("pitcher_hand")
        if group.get("opp_team_id") is None and _safe_int(split_row.get("opp_team_id")) is not None:
            group["opp_team_id"] = _safe_int(split_row.get("opp_team_id"))
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
        group_key = _pitcher_group_key(pitcher_id, pick.get("pitcher_name"))
        if group_key is None:
            continue
        group = pitcher_groups.setdefault(
            group_key,
            {
                "pitcher_id": pitcher_id,
                "pitcher_name": pick.get("pitcher_name"),
                "pitcher_hand": pick.get("pitcher_hand"),
                "opp_team_id": None,
                "splits": {},
                "batters": [],
            },
        )
        if group.get("pitcher_id") is None and pitcher_id is not None:
            group["pitcher_id"] = pitcher_id
        if not group.get("pitcher_name") and pick.get("pitcher_name"):
            group["pitcher_name"] = pick.get("pitcher_name")
        if not group.get("pitcher_hand") and pick.get("pitcher_hand"):
            group["pitcher_hand"] = pick.get("pitcher_hand")
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
                "home_moneyline": _safe_int(pick.get("home_moneyline")),
                "away_moneyline": _safe_int(pick.get("away_moneyline")),
                "over_under": _safe_float(pick.get("over_under")),
            }
        )

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
        "run_date": run_date,
        "game": {
            "home_team": home_team,
            "away_team": away_team,
            "start_time_utc": schedule.get("start_time_utc") if schedule else None,
            "venue_name": gw.get("ballpark_name") if gw else (schedule.get("venue_name") if schedule else None),
            "home_pitcher_name": schedule.get("home_pitcher_name") if schedule else None,
            "away_pitcher_name": schedule.get("away_pitcher_name") if schedule else None,
            # Weather
            "weather_indicator": gw.get("weather_indicator") if gw else None,
            "game_temp": _safe_float(gw.get("game_temp")) if gw else None,
            "wind_speed": _safe_float(gw.get("wind_speed")) if gw else None,
            "wind_dir": _safe_int(gw.get("wind_dir")) if gw else None,
            "wind_gust": _safe_float(gw.get("wind_gust")) if gw else None,
            "precip_prob": _safe_float(gw.get("precip_prob")) if gw else None,
            "conditions": gw.get("conditions") if gw else None,
            "ballpark_name": gw.get("ballpark_name") if gw else None,
            "roof_type": gw.get("roof_type") if gw else None,
            "weather_note": gw.get("weather_note") if gw else None,
            # Odds
            "home_moneyline": _safe_int(gw.get("home_moneyline")) if gw else None,
            "away_moneyline": _safe_int(gw.get("away_moneyline")) if gw else None,
            "over_under": _safe_float(gw.get("over_under")) if gw else None,
        },
        "grade_counts": grade_counts,
        "pitchers": pitchers_out,
    }
