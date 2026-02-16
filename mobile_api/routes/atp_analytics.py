from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import os
from threading import Lock
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from google.cloud import bigquery

from bq import get_bq_client

from atp.analytics import (
    _round_rank,
    build_compare,
    build_head_to_head,
    build_player_form,
    build_region_splits,
    build_set_distribution,
    build_surface_splits,
    build_tournament_performance,
    normalize_round_name,
)
from atp.client import AtpApiError, fetch_one_page, fetch_paginated


router = APIRouter(prefix="/atp", tags=["ATP"])


_CACHE_LOCK = Lock()
_ATP_COMPARE_CACHE: Dict[str, tuple[datetime, Dict[str, Any]]] = {}
_ATP_BRACKET_CACHE: Dict[str, tuple[datetime, Dict[str, Any]]] = {}

_ATP_COMPARE_DEFAULTS = {
    "season": None,
    "seasons_back": 2,
    "start_season": None,
    "end_season": None,
    "surface": None,
    "last_n": 25,
    "surface_last_n": 45,
    "recent_last_n": 10,
    "recent_surface_last_n": 20,
    "max_pages": 5,
}
_ATP_COMPARE_CACHE_TABLE = os.getenv("ATP_COMPARE_CACHE_TABLE", "atp_data.atp_matchup_compare_cache")


_ATP_PLAYER_METRICS_TABLE = os.getenv("ATP_PLAYER_METRICS_TABLE", "atp_data.atp_player_compare_metrics")
_ATP_SHEET_DAILY_MATCHES_TABLE = os.getenv("ATP_SHEET_DAILY_MATCHES_TABLE", "atp_data.sheet_daily_matches")
_ATP_PLAYER_LOOKUP_TABLE = os.getenv("ATP_PLAYER_LOOKUP_TABLE", "atp_data.player_lookup")


def _read_player_metrics_from_bq(player_ids: List[int], surface: Optional[str]) -> Dict[int, Dict[str, Any]]:
    if not player_ids:
        return {}
    try:
        client = get_bq_client()
        surface_key = (surface or "").strip().lower()
        sql = f"""
        SELECT
          player_id,
          player_name,
          surface_key,
          overall_win_rate,
          recent_win_rate,
          straight_sets_win_rate,
          tiebreak_rate,
          form_score,
          updated_at
        FROM `{_ATP_PLAYER_METRICS_TABLE}`
        WHERE player_id IN UNNEST(@player_ids)
          AND surface_key IN UNNEST(@surface_keys)
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY player_id
          ORDER BY IF(surface_key = @preferred_surface, 0, 1), updated_at DESC
        ) = 1
        """
        preferred = surface_key or "all"
        surface_keys = ["all"] if not surface_key else [surface_key, "all"]
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("player_ids", "INT64", player_ids),
                    bigquery.ArrayQueryParameter("surface_keys", "STRING", surface_keys),
                    bigquery.ScalarQueryParameter("preferred_surface", "STRING", preferred),
                ]
            ),
        )
        out: Dict[int, Dict[str, Any]] = {}
        for row in job.result():
            out[int(row["player_id"])] = dict(row)
        return out
    except Exception:
        return {}


def _read_h2h_from_bq(player_id: int, opponent_id: int, surface: Optional[str]) -> Dict[str, Any]:
    try:
        client = get_bq_client()
        sql = """
        SELECT
          COUNT(1) AS starts,
          SUM(CASE WHEN winner_id = @player_id THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN winner_id = @opponent_id THEN 1 ELSE 0 END) AS losses
        FROM `atp_data.matches`
        WHERE (
          (player1_id = @player_id AND player2_id = @opponent_id)
          OR (player1_id = @opponent_id AND player2_id = @player_id)
        )
          AND (@surface IS NULL OR LOWER(surface) = @surface)
        """
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
                    bigquery.ScalarQueryParameter("opponent_id", "INT64", opponent_id),
                    bigquery.ScalarQueryParameter("surface", "STRING", (surface or "").lower() or None),
                ]
            ),
        )
        row = next(iter(job.result()), None)
        starts = int(row["starts"] or 0) if row else 0
        wins = int(row["wins"] or 0) if row else 0
        losses = int(row["losses"] or 0) if row else 0
        return {
            "player_id": player_id,
            "opponent_id": opponent_id,
            "starts": starts,
            "wins": wins,
            "losses": losses,
            "win_rate": (wins / starts) if starts else 0.0,
            "by_surface": [],
            "matches": [],
        }
    except Exception:
        return {
            "player_id": player_id,
            "opponent_id": opponent_id,
            "starts": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "by_surface": [],
            "matches": [],
        }


def _build_compare_from_precomputed(
    *,
    player_ids: List[int],
    surface: Optional[str],
    rankings: Dict[int, Any],
) -> Optional[Dict[str, Any]]:
    metrics_rows = _read_player_metrics_from_bq(player_ids, surface)
    if len(metrics_rows) < len(player_ids):
        return None

    weights = {
        "form": 0.50,
        "surface": 0.20,
        "ranking": 0.10,
        "head_to_head": 0.20,
    }

    players_payload: List[Dict[str, Any]] = []
    for pid in player_ids:
        row = metrics_rows.get(pid) or {}
        players_payload.append(
            {
                "player_id": pid,
                "rank": rankings.get(pid),
                "score": 0.0,
                "metrics": {
                    "form_score": float(row.get("form_score") or 0.0),
                    "recent_win_rate": float(row.get("recent_win_rate") or 0.0),
                    "surface_win_rate": float(row.get("overall_win_rate") or 0.0),
                    "ranking": rankings.get(pid),
                    "recent_surface_win_rate": float(row.get("recent_win_rate") or 0.0),
                    "straight_sets_win_rate": float(row.get("straight_sets_win_rate") or 0.0),
                    "tiebreak_rate": float(row.get("tiebreak_rate") or 0.0),
                },
            }
        )

    def _normalize(values: Dict[int, Optional[float]], reverse: bool = False) -> Dict[int, float]:
        valid = [float(v) for v in values.values() if v is not None]
        if not valid:
            return {k: 0.5 for k in values}
        lo, hi = min(valid), max(valid)
        if hi == lo:
            return {k: 0.5 for k in values}
        out: Dict[int, float] = {}
        for k, v in values.items():
            if v is None:
                out[k] = 0.5
            else:
                n = (float(v) - lo) / (hi - lo)
                out[k] = 1 - n if reverse else n
        return out

    form_norm = _normalize({p["player_id"]: p["metrics"]["form_score"] for p in players_payload})
    surface_norm = _normalize({p["player_id"]: p["metrics"]["surface_win_rate"] for p in players_payload})
    ranking_norm = _normalize({p["player_id"]: p["metrics"]["ranking"] for p in players_payload}, reverse=True)

    h2h = {}
    h2h_norm = {pid: 0.5 for pid in player_ids}
    if len(player_ids) >= 2:
        p1, p2 = player_ids[0], player_ids[1]
        h2h = _read_h2h_from_bq(p1, p2, surface)
        if h2h.get("starts"):
            r = float(h2h.get("win_rate") or 0.0)
            h2h_norm[p1] = r
            h2h_norm[p2] = 1 - r

    for row in players_payload:
        pid = row["player_id"]
        score = (
            weights["form"] * form_norm.get(pid, 0.5)
            + weights["surface"] * surface_norm.get(pid, 0.5)
            + weights["ranking"] * ranking_norm.get(pid, 0.5)
            + weights["head_to_head"] * h2h_norm.get(pid, 0.5)
        )
        row["score"] = round(score, 4)

    players_payload.sort(key=lambda x: x["score"], reverse=True)
    recommendation = None
    if len(players_payload) >= 2:
        edge = round(players_payload[0]["score"] - players_payload[1]["score"], 4)
        if edge > 0.01:
            recommendation = {
                "player_id": players_payload[0]["player_id"],
                "label": "Lean" if edge < 0.08 else "Pick",
                "edge": edge,
                "reasons": ["Precomputed form edge", "Precomputed surface edge"],
            }

    return {
        "player_ids": player_ids,
        "surface": surface,
        "weights": weights,
        "players": players_payload,
        "head_to_head": h2h,
        "recommendation": recommendation,
    }


def _compare_cache_key(
    *,
    player_ids: List[int],
    season: Optional[int],
    seasons_back: Optional[int],
    start_season: Optional[int],
    end_season: Optional[int],
    surface: Optional[str],
    last_n: int,
    surface_last_n: int,
    recent_last_n: int,
    recent_surface_last_n: int,
    max_pages: Optional[int],
) -> str:
    return "|".join(
        [
            ",".join(str(pid) for pid in sorted(player_ids)),
            str(season or ""),
            str(seasons_back if seasons_back is not None else ""),
            str(start_season or ""),
            str(end_season or ""),
            (surface or "").strip().lower(),
            str(last_n),
            str(surface_last_n),
            str(recent_last_n),
            str(recent_surface_last_n),
            str(max_pages or ""),
        ]
    )


def _match_analysis_key(match: Dict[str, Any]) -> str:
    return str(match.get("id") if match.get("id") is not None else f"{match.get('player1')}::{match.get('player2')}")


def _ensure_compare_cache_table(client: bigquery.Client) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{_ATP_COMPARE_CACHE_TABLE}` (
      cache_key STRING NOT NULL,
      player_ids STRING NOT NULL,
      season INT64,
      seasons_back INT64,
      start_season INT64,
      end_season INT64,
      surface STRING,
      tournament_id INT64,
      match_id STRING,
      payload_json STRING NOT NULL,
      computed_at TIMESTAMP NOT NULL,
      expires_at TIMESTAMP
    )
    PARTITION BY DATE(computed_at)
    CLUSTER BY cache_key, tournament_id
    """
    client.query(ddl).result()


def _read_compare_from_bq(cache_key: str) -> Optional[Dict[str, Any]]:
    try:
        client = get_bq_client()
        _ensure_compare_cache_table(client)
        sql = f"""
        SELECT payload_json
        FROM `{_ATP_COMPARE_CACHE_TABLE}`
        WHERE cache_key = @cache_key
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
        ORDER BY computed_at DESC
        LIMIT 1
        """
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("cache_key", "STRING", cache_key)]
            ),
        )
        rows = list(job.result())
        if not rows:
            return None
        return json.loads(rows[0]["payload_json"])
    except Exception:
        return None


def _read_compare_batch_from_bq(cache_keys: List[str]) -> Dict[str, Dict[str, Any]]:
    if not cache_keys:
        return {}
    try:
        client = get_bq_client()
        _ensure_compare_cache_table(client)
        sql = f"""
        SELECT cache_key, payload_json
        FROM `{_ATP_COMPARE_CACHE_TABLE}`
        WHERE cache_key IN UNNEST(@cache_keys)
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cache_key ORDER BY computed_at DESC) = 1
        """
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("cache_keys", "STRING", cache_keys)]
            ),
        )
        out: Dict[str, Dict[str, Any]] = {}
        for row in job.result():
            out[row["cache_key"]] = json.loads(row["payload_json"])
        return out
    except Exception:
        return {}


def _write_compare_to_bq(
    *,
    cache_key: str,
    payload: Dict[str, Any],
    player_ids: List[int],
    season: Optional[int],
    seasons_back: Optional[int],
    start_season: Optional[int],
    end_season: Optional[int],
    surface: Optional[str],
    tournament_id: Optional[int],
    match_id: Optional[str],
    ttl_seconds: int = 3600,
) -> None:
    try:
        client = get_bq_client()
        _ensure_compare_cache_table(client)
        now = datetime.now(timezone.utc)
        row = {
            "cache_key": cache_key,
            "player_ids": ",".join(str(pid) for pid in sorted(player_ids)),
            "season": season,
            "seasons_back": seasons_back,
            "start_season": start_season,
            "end_season": end_season,
            "surface": (surface or "").lower() or None,
            "tournament_id": tournament_id,
            "match_id": match_id,
            "payload_json": json.dumps(payload),
            "computed_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=ttl_seconds)).isoformat(),
        }
        errors = client.insert_rows_json(_ATP_COMPARE_CACHE_TABLE, [row])
        if errors:
            print(f"[ATP_CACHE] insert_rows_json errors: {errors}")
    except Exception as exc:
        print(f"[ATP_CACHE] Failed to write compare cache: {exc}")


def _build_compare_payload(
    *,
    player_ids: List[int],
    season: Optional[int],
    seasons_back: Optional[int],
    start_season: Optional[int],
    end_season: Optional[int],
    surface: Optional[str],
    last_n: int,
    surface_last_n: int,
    recent_last_n: int,
    recent_surface_last_n: int,
    max_pages: Optional[int],
) -> Dict[str, Any]:
    seasons = _resolve_seasons(
        season=season,
        seasons_back=seasons_back,
        start_season=start_season,
        end_season=end_season,
    )
    matches = _fetch_matches_for_seasons(seasons, max_pages=max_pages)

    rankings_payload = fetch_paginated("/rankings", params={"per_page": 100}, cache_ttl=900, max_pages=3)
    rankings_map = {}
    for row in rankings_payload:
        player = row.get("player") or {}
        pid = player.get("id")
        if pid:
            rankings_map[pid] = row.get("rank")

    return build_compare(
        matches,
        player_ids=player_ids,
        surface=surface,
        last_n=last_n,
        surface_last_n=surface_last_n,
        recent_last_n=recent_last_n,
        recent_surface_last_n=recent_surface_last_n,
        rankings=rankings_map,
    )


def _cache_get(
    cache: Dict[str, tuple[datetime, Dict[str, Any]]],
    key: str,
) -> Optional[Dict[str, Any]]:
    with _CACHE_LOCK:
        cached = cache.get(key)
        if not cached:
            return None
        expires_at, payload = cached
        if datetime.now(timezone.utc) >= expires_at:
            cache.pop(key, None)
            return None
        return payload


def _cache_set(
    cache: Dict[str, tuple[datetime, Dict[str, Any]]],
    key: str,
    payload: Dict[str, Any],
    *,
    ttl_seconds: int,
) -> None:
    with _CACHE_LOCK:
        cache[key] = (
            datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
            payload,
        )


def _current_season() -> int:
    return datetime.utcnow().year


def _handle_error(err: Exception) -> None:
    if isinstance(err, AtpApiError):
        raise HTTPException(status_code=502, detail=str(err))
    raise HTTPException(status_code=500, detail=str(err))


def _resolve_seasons(
    *,
    season: Optional[int] = None,
    seasons_back: Optional[int] = None,
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
) -> List[int]:
    if start_season is not None or end_season is not None:
        start = start_season if start_season is not None else (end_season or _current_season())
        end = end_season if end_season is not None else start
        if start > end:
            start, end = end, start
        return list(range(start, end + 1))

    if season is not None:
        return [season]

    if seasons_back is not None:
        current = _current_season()
        return [current - offset for offset in range(seasons_back + 1)]

    return [_current_season()]


def _fetch_matches_for_seasons(
    seasons: List[int],
    *,
    max_pages: Optional[int] = None,
) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for season in seasons:
        matches.extend(
            fetch_paginated(
                "/matches",
                params={"season": season},
                cache_ttl=900,
                max_pages=max_pages,
            )
        )
    return matches


def _parse_date(value: Optional[str], fallback: date) -> date:
    if not value:
        return fallback
    return datetime.strptime(value, "%Y-%m-%d").date()


def _chunked(items: List[int], size: int = 25) -> List[List[int]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _date_overlaps(
    *,
    start: date,
    end: date,
    target_start: date,
    target_end: date,
) -> bool:
    return start <= target_end and end >= target_start


def _parse_match_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_scheduled_after(match: Dict[str, Any], cutoff: datetime) -> bool:
    scheduled = _parse_match_time(match.get("scheduled_time"))
    if scheduled is None:
        return False
    return scheduled >= cutoff


def _player_label(player: Any) -> str:
    if not player:
        return "TBD"
    if isinstance(player, str):
        return player
    if isinstance(player, dict):
        name = player.get("name") or player.get("full_name")
        if name:
            return name
        first = player.get("first_name")
        last = player.get("last_name")
        if first or last:
            return " ".join(part for part in [first, last] if part)
    return "TBD"


def _player_id(player: Any) -> Optional[int]:
    if isinstance(player, dict):
        pid = player.get("id")
        return int(pid) if pid is not None else None
    return None


def _format_match(match: Dict[str, Any]) -> Dict[str, Any]:
    round_raw = match.get("round") or match.get("round_name") or "Round"
    round_name = normalize_round_name(str(round_raw))
    scheduled_raw = (
        match.get("scheduled_time")
        or match.get("start_time")
        or match.get("start_time_utc")
        or match.get("scheduled_at")
        or match.get("date")
        or match.get("start_date")
    )
    scheduled_at = _parse_match_time(scheduled_raw)
    match_date = match.get("match_date")
    p1_raw = match.get("player1") or match.get("player_1")
    p2_raw = match.get("player2") or match.get("player_2")
    return {
        "id": match.get("id"),
        "round": round_name,
        "round_order": match.get("round_order") if match.get("round_order") is not None else _round_rank(round_name),
        "status": match.get("match_status"),
        "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
        "match_date": str(match_date) if match_date is not None else None,
        "not_before_text": match.get("not_before_text") or match.get("not_before"),
        "player1": _player_label(p1_raw),
        "player2": _player_label(p2_raw),
        "player1_id": _player_id(p1_raw),
        "player2_id": _player_id(p2_raw),
        "player1_headshot_url": (p1_raw or {}).get("player_image_url") if isinstance(p1_raw, dict) else None,
        "player2_headshot_url": (p2_raw or {}).get("player_image_url") if isinstance(p2_raw, dict) else None,
        "winner": _player_label(match.get("winner")),
        "score": match.get("score"),
    }


def _fetch_atp_player_headshots(player_ids: List[int]) -> Dict[int, str]:
    if not player_ids:
        return {}

    client = get_bq_client()
    sql = f"""
    WITH latest AS (
      SELECT * EXCEPT (row_num)
      FROM (
        SELECT
          player_id,
          player_image_url,
          ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY last_verified DESC
          ) AS row_num
        FROM `{_ATP_PLAYER_LOOKUP_TABLE}`
        WHERE player_id IN UNNEST(@player_ids)
      )
      WHERE row_num = 1
    )
    SELECT player_id, player_image_url
    FROM latest
    WHERE player_image_url IS NOT NULL
    """

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("player_ids", "INT64", sorted(set(player_ids))),
            ]
        ),
    )

    return {
        int(row.get("player_id")): row.get("player_image_url")
        for row in job.result()
        if row.get("player_id") is not None and row.get("player_image_url")
    }


def _attach_headshots(matches: List[Dict[str, Any]], headshots: Dict[int, str]) -> List[Dict[str, Any]]:
    if not matches or not headshots:
        return matches

    enriched: List[Dict[str, Any]] = []
    for match in matches:
        player1_id = match.get("player1_id")
        player2_id = match.get("player2_id")
        enriched.append(
            {
                **match,
                "player1_headshot_url": match.get("player1_headshot_url") or headshots.get(player1_id),
                "player2_headshot_url": match.get("player2_headshot_url") or headshots.get(player2_id),
            }
        )
    return enriched


def _match_identity(match: Dict[str, Any]) -> tuple:
    """Return a stable identity key for deduplicating bracket matches."""
    match_id = match.get("id")
    if match_id is not None:
        return ("id", str(match_id))

    p1 = match.get("player1_id")
    p2 = match.get("player2_id")
    if p1 is not None and p2 is not None:
        return ("players", tuple(sorted((int(p1), int(p2)))))

    p1_name = str(match.get("player1") or "").strip().lower()
    p2_name = str(match.get("player2") or "").strip().lower()
    if p1_name and p2_name:
        return ("names", tuple(sorted((p1_name, p2_name))))

    return ("unknown", id(match))


def _merge_missing_sheet_matches(
    *,
    formatted_matches: List[Dict[str, Any]],
    sheet_matches: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge sheet-backed matches into ATP API matches when API is missing pairings."""
    existing = {_match_identity(match) for match in formatted_matches}
    merged = list(formatted_matches)

    for match in sheet_matches:
        identity = _match_identity(match)
        if identity in existing:
            continue
        merged.append(match)
        existing.add(identity)

    return merged


def _read_today_sheet_matches(
    *,
    tournament_id: Optional[int],
    upcoming_limit: int,
) -> List[Dict[str, Any]]:
    client = get_bq_client()
    sql = f"""
    SELECT
      match_id,
      round,
      match_status,
      match_date,
      scheduled_time,
      not_before_text,
      player1_id,
      player1_full_name,
      player2_id,
      player2_full_name,
      winner_id,
      winner_full_name,
      score
    FROM `{_ATP_SHEET_DAILY_MATCHES_TABLE}`
    WHERE (
      -- The Google sheet's "today" slate is keyed by UTC date, including
      -- placeholder 00:00:00Z rows used for "Followed By" matches.
      DATE(scheduled_time) = CURRENT_DATE('UTC')
      OR match_date = CURRENT_DATE('UTC')
    )
      AND (@tournament_id IS NULL OR tournament_id = @tournament_id)
    ORDER BY scheduled_time ASC
    LIMIT @limit
    """

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("tournament_id", "INT64", tournament_id),
                bigquery.ScalarQueryParameter("limit", "INT64", upcoming_limit),
            ]
        ),
    )

    out: List[Dict[str, Any]] = []
    for row in job.result():
        scheduled = row.get("scheduled_time")
        out.append(
            {
                "id": row.get("match_id"),
                "round": normalize_round_name(str(row.get("round") or "Round")),
                "round_order": _round_rank(str(row.get("round") or "Round")),
                "status": row.get("match_status"),
                "scheduled_at": scheduled.isoformat() if scheduled else None,
                "match_date": row.get("match_date").isoformat() if row.get("match_date") else None,
                "not_before_text": row.get("not_before_text"),
                "player1": row.get("player1_full_name") or "TBD",
                "player2": row.get("player2_full_name") or "TBD",
                "player1_id": int(row.get("player1_id")) if row.get("player1_id") is not None else None,
                "player2_id": int(row.get("player2_id")) if row.get("player2_id") is not None else None,
                "winner": row.get("winner_full_name") or "TBD",
                "score": row.get("score"),
            }
        )

    return out


def _select_tournament(
    tournaments: List[Dict[str, Any]],
    *,
    tournament_id: Optional[int],
    tournament_name: Optional[str],
) -> Optional[Dict[str, Any]]:
    if tournament_id is not None:
        for tournament in tournaments:
            if tournament.get("id") == tournament_id:
                return tournament
        return None

    filtered = tournaments
    if tournament_name:
        name_lower = tournament_name.lower()
        filtered = [
            tournament
            for tournament in tournaments
            if name_lower in (tournament.get("name") or "").lower()
            or name_lower in (tournament.get("city") or "").lower()
            or name_lower in (tournament.get("location") or "").lower()
        ]
        if not filtered:
            filtered = tournaments

    def parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None

    today = date.today()
    active = []
    upcoming = []
    past = []

    for tournament in filtered:
        start = parse_date(tournament.get("start_date"))
        end = parse_date(tournament.get("end_date"))
        if not start or not end:
            continue
        if start <= today <= end:
            active.append((start, end, tournament))
        elif start >= today:
            upcoming.append((start, end, tournament))
        else:
            past.append((start, end, tournament))

    if active:
        active.sort(key=lambda item: (item[0], item[1]))
        return active[0][2]
    if upcoming:
        upcoming.sort(key=lambda item: (item[0], item[1]))
        return upcoming[0][2]
    if past:
        past.sort(key=lambda item: (item[1], item[0]), reverse=True)
        return past[0][2]
    return None


@router.get("/players")
def get_atp_players(
    search: Optional[str] = None,
    per_page: int = Query(50, ge=1, le=100),
    cursor: Optional[int] = None,
):
    try:
        params: Dict[str, Any] = {"per_page": per_page}
        if search:
            params["search"] = search
        if cursor is not None:
            params["cursor"] = cursor
        payload = fetch_one_page("/players", params=params, cache_ttl=300)
        data = payload.get("data", [])
        return {"data": data, "count": len(data), "meta": payload.get("meta", {})}
    except Exception as err:
        _handle_error(err)


@router.get("/tournaments")
def get_atp_tournaments(
    season: Optional[int] = None,
    category: Optional[str] = None,
    surface: Optional[str] = None,
    per_page: int = Query(50, ge=1, le=100),
    cursor: Optional[int] = None,
):
    try:
        params: Dict[str, Any] = {"per_page": per_page}
        if season is not None:
            params["season"] = season
        if category:
            params["category"] = category
        if surface:
            params["surface"] = surface
        if cursor is not None:
            params["cursor"] = cursor
        payload = fetch_one_page("/tournaments", params=params, cache_ttl=300)
        data = payload.get("data", [])
        return {"data": data, "count": len(data), "meta": payload.get("meta", {})}
    except Exception as err:
        _handle_error(err)


@router.get("/rankings")
def get_atp_rankings(
    ranking_date: Optional[str] = None,
    per_page: int = Query(50, ge=1, le=100),
    cursor: Optional[int] = None,
):
    try:
        params: Dict[str, Any] = {"per_page": per_page}
        if ranking_date:
            params["ranking_date"] = ranking_date
        if cursor is not None:
            params["cursor"] = cursor
        payload = fetch_one_page("/rankings", params=params, cache_ttl=300)
        data = payload.get("data", [])
        return {"data": data, "count": len(data), "meta": payload.get("meta", {})}
    except Exception as err:
        _handle_error(err)


@router.get("/matches/upcoming")
def get_atp_upcoming_matches(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_completed: bool = Query(False),
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    try:
        today = date.today()
        default_end = today + timedelta(days=1)
        window_start = _parse_date(start_date, today)
        window_end = _parse_date(end_date, default_end)
        if window_start > window_end:
            window_start, window_end = window_end, window_start

        seasons = sorted({window_start.year, window_end.year})
        tournaments: List[Dict[str, Any]] = []
        for season in seasons:
            payload = fetch_one_page(
                "/tournaments",
                params={"season": season, "per_page": 100},
                cache_ttl=300,
            )
            tournaments.extend(payload.get("data", []) or [])

        eligible_tournaments: List[Dict[str, Any]] = []
        for tournament in tournaments:
            start_raw = tournament.get("start_date")
            end_raw = tournament.get("end_date")
            if not start_raw or not end_raw:
                continue
            try:
                start = datetime.strptime(start_raw, "%Y-%m-%d").date()
                end = datetime.strptime(end_raw, "%Y-%m-%d").date()
            except ValueError:
                continue
            if _date_overlaps(
                start=start,
                end=end,
                target_start=window_start,
                target_end=window_end,
            ):
                eligible_tournaments.append(tournament)

        tournament_ids = [tournament.get("id") for tournament in eligible_tournaments if tournament.get("id")]
        matches: List[Dict[str, Any]] = []
        for batch in _chunked(tournament_ids, size=25):
            batch_matches = fetch_paginated(
                "/matches",
                params={"tournament_ids[]": batch},
                cache_ttl=300,
                max_pages=max_pages,
            )
            matches.extend(batch_matches)

        if not include_completed:
            matches = [match for match in matches if match.get("match_status") != "F"]

        return {
            "window": {"start_date": window_start.isoformat(), "end_date": window_end.isoformat()},
            "tournaments": {
                "count": len(eligible_tournaments),
                "ids": tournament_ids,
            },
            "matches": matches,
            "count": len(matches),
        }
    except Exception as err:
        _handle_error(err)


@router.get("/matches/upcoming-scheduled")
def get_atp_upcoming_matches_scheduled(
    season: Optional[int] = None,
    tournament_ids: Optional[List[int]] = Query(None, alias="tournament_ids[]"),
    round_name: Optional[str] = Query(None, alias="round"),
    from_time: Optional[str] = Query(None, alias="from"),
    include_completed: bool = Query(False),
    per_page: int = Query(100, ge=1, le=100),
    max_pages: Optional[int] = Query(10, ge=1, le=500),
):
    try:
        cutoff = _parse_match_time(from_time) or datetime.now(timezone.utc)
        params: Dict[str, Any] = {"season": season or _current_season()}
        if tournament_ids:
            params["tournament_ids[]"] = tournament_ids
        if round_name:
            params["round"] = round_name

        matches = fetch_paginated(
            "/matches",
            params=params,
            per_page=per_page,
            max_pages=max_pages,
            cache_ttl=300,
        )

        upcoming = [match for match in matches if _is_scheduled_after(match, cutoff)]
        if not include_completed:
            upcoming = [match for match in upcoming if match.get("match_status") != "F"]

        upcoming.sort(
            key=lambda match: _parse_match_time(match.get("scheduled_time")) or datetime.min.replace(tzinfo=timezone.utc)
        )

        return {
            "cutoff": cutoff.isoformat(),
            "count": len(upcoming),
            "matches": upcoming,
        }
    except Exception as err:
        _handle_error(err)


@router.get("/active-tournaments")
def get_atp_active_tournaments(
    season: Optional[int] = None,
):
    """Return all tournaments that are currently active (today falls within
    their start/end dates) or upcoming within the next 7 days."""
    try:
        selected_season = season or _current_season()
        payload = fetch_one_page(
            "/tournaments",
            params={"season": selected_season, "per_page": 100},
            cache_ttl=300,
        )
        tournaments = payload.get("data", []) or []

        today = date.today()
        window_end = today + timedelta(days=7)
        active: List[Dict[str, Any]] = []

        for tournament in tournaments:
            start_raw = tournament.get("start_date")
            end_raw = tournament.get("end_date")
            if not start_raw or not end_raw:
                continue
            try:
                start = datetime.strptime(start_raw, "%Y-%m-%d").date()
                end = datetime.strptime(end_raw, "%Y-%m-%d").date()
            except ValueError:
                continue
            # Include if currently running or starting within the next 7 days
            if start <= today <= end or (today < start <= window_end):
                active.append({
                    "id": tournament.get("id"),
                    "name": tournament.get("name"),
                    "surface": tournament.get("surface"),
                    "start_date": tournament.get("start_date"),
                    "end_date": tournament.get("end_date"),
                    "category": tournament.get("category"),
                    "city": tournament.get("city"),
                    "country": tournament.get("country"),
                })

        # Sort: currently running first, then by start date
        active.sort(
            key=lambda t: (
                0 if _parse_date(t.get("start_date"), today) <= today else 1,
                _parse_date(t.get("start_date"), today),
            )
        )

        return {"tournaments": active, "count": len(active)}
    except Exception as err:
        _handle_error(err)


@router.get("/tournament-bracket")
def get_atp_tournament_bracket(
    tournament_id: Optional[int] = None,
    tournament_name: Optional[str] = None,
    season: Optional[int] = None,
    upcoming_limit: int = Query(50, ge=1, le=50),
    max_pages: Optional[int] = Query(20, ge=1, le=500),
    include_match_analyses: bool = True,
    recompute_missing_analyses: bool = False,
):
    try:
        return build_tournament_bracket_payload(
            tournament_id=tournament_id,
            tournament_name=tournament_name,
            season=season,
            upcoming_limit=upcoming_limit,
            max_pages=max_pages,
            include_match_analyses=include_match_analyses,
            recompute_missing_analyses=recompute_missing_analyses,
        )
    except HTTPException:
        raise
    except Exception as err:
        _handle_error(err)


def build_tournament_bracket_payload(
    *,
    tournament_id: Optional[int] = None,
    tournament_name: Optional[str] = None,
    season: Optional[int] = None,
    upcoming_limit: int = 50,
    max_pages: Optional[int] = 20,
    include_match_analyses: bool = True,
    recompute_missing_analyses: bool = False,
) -> Dict[str, Any]:
    cache_key = "|".join(
        [
            str(tournament_id or ""),
            (tournament_name or "").strip().lower(),
            str(season or ""),
            str(upcoming_limit),
            str(max_pages or ""),
            "1" if include_match_analyses else "0",
        ]
    )
    if not recompute_missing_analyses:
        cached_payload = _cache_get(_ATP_BRACKET_CACHE, cache_key)
        if cached_payload is not None:
            return cached_payload

    selected_season = season or _current_season()
    tournaments_payload = fetch_one_page(
        "/tournaments",
        params={"season": selected_season, "per_page": 100},
        cache_ttl=300,
    )
    tournaments = tournaments_payload.get("data", []) or []
    tournament = _select_tournament(
        tournaments,
        tournament_id=tournament_id,
        tournament_name=tournament_name,
    )
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found.")

    matches = fetch_paginated(
        "/matches",
        params={
            "tournament_ids[]": [tournament.get("id")],
            "season": selected_season,
        },
        cache_ttl=300,
        max_pages=max_pages,
    )

    formatted_matches = [_format_match(match) for match in matches]
    player_ids: List[int] = []
    for match in formatted_matches:
        if match.get("player1_id") is not None:
            player_ids.append(int(match["player1_id"]))
        if match.get("player2_id") is not None:
            player_ids.append(int(match["player2_id"]))

    upcoming_matches = _read_today_sheet_matches(
        tournament_id=tournament.get("id"),
        upcoming_limit=upcoming_limit,
    )
    for match in upcoming_matches:
        if match.get("player1_id") is not None:
            player_ids.append(int(match["player1_id"]))
        if match.get("player2_id") is not None:
            player_ids.append(int(match["player2_id"]))

    formatted_matches = _merge_missing_sheet_matches(
        formatted_matches=formatted_matches,
        sheet_matches=upcoming_matches,
    )

    headshots = _fetch_atp_player_headshots(player_ids)
    formatted_matches = _attach_headshots(formatted_matches, headshots)
    upcoming_matches = _attach_headshots(upcoming_matches, headshots)

    rounds: Dict[str, Dict[str, Any]] = {}
    for match in formatted_matches:
        round_name = str(match.get("round") or "Round")
        if round_name not in rounds:
            rounds[round_name] = {
                "name": round_name,
                "order": match.get("round_order"),
                "matches": [],
            }
        rounds[round_name]["matches"].append(match)

    round_list = list(rounds.values())
    round_list.sort(
        key=lambda item: (
            item.get("order") is None,
            item.get("order") if item.get("order") is not None else 999,
            item.get("name"),
        )
    )

    match_analyses: Dict[str, Dict[str, Any]] = {}
    if include_match_analyses and upcoming_matches:
        match_to_cache_key: Dict[str, str] = {}
        for match in upcoming_matches:
            p1 = match.get("player1_id")
            p2 = match.get("player2_id")
            if p1 is None or p2 is None or p1 == p2:
                continue
            key = _compare_cache_key(
                player_ids=[int(p1), int(p2)],
                season=_ATP_COMPARE_DEFAULTS["season"],
                seasons_back=_ATP_COMPARE_DEFAULTS["seasons_back"],
                start_season=_ATP_COMPARE_DEFAULTS["start_season"],
                end_season=_ATP_COMPARE_DEFAULTS["end_season"],
                surface=(tournament.get("surface") or "").lower() or None,
                last_n=_ATP_COMPARE_DEFAULTS["last_n"],
                surface_last_n=_ATP_COMPARE_DEFAULTS["surface_last_n"],
                recent_last_n=_ATP_COMPARE_DEFAULTS["recent_last_n"],
                recent_surface_last_n=_ATP_COMPARE_DEFAULTS["recent_surface_last_n"],
                max_pages=_ATP_COMPARE_DEFAULTS["max_pages"],
            )
            match_to_cache_key[_match_analysis_key(match)] = key

        cached = _read_compare_batch_from_bq(list(set(match_to_cache_key.values())))
        for mkey, ckey in match_to_cache_key.items():
            payload_row = cached.get(ckey)
            if payload_row is not None:
                match_analyses[mkey] = payload_row

        if recompute_missing_analyses:
            rankings_payload = fetch_paginated("/rankings", params={"per_page": 100}, cache_ttl=900, max_pages=3)
            rankings_map: Dict[int, Any] = {}
            for row in rankings_payload:
                player = row.get("player") or {}
                pid = player.get("id")
                if pid:
                    rankings_map[int(pid)] = row.get("rank")

            for match in upcoming_matches:
                mkey = _match_analysis_key(match)
                if mkey in match_analyses:
                    continue
                p1 = match.get("player1_id")
                p2 = match.get("player2_id")
                if p1 is None or p2 is None or p1 == p2:
                    continue
                cache_key = match_to_cache_key.get(mkey)
                if not cache_key:
                    continue
                try:
                    compare_payload = _build_compare_from_precomputed(
                        player_ids=[int(p1), int(p2)],
                        surface=(tournament.get("surface") or "").lower() or None,
                        rankings=rankings_map,
                    )
                    if compare_payload is None:
                        compare_payload = _build_compare_payload(
                            player_ids=[int(p1), int(p2)],
                            season=_ATP_COMPARE_DEFAULTS["season"],
                            seasons_back=_ATP_COMPARE_DEFAULTS["seasons_back"],
                            start_season=_ATP_COMPARE_DEFAULTS["start_season"],
                            end_season=_ATP_COMPARE_DEFAULTS["end_season"],
                            surface=(tournament.get("surface") or "").lower() or None,
                            last_n=_ATP_COMPARE_DEFAULTS["last_n"],
                            surface_last_n=_ATP_COMPARE_DEFAULTS["surface_last_n"],
                            recent_last_n=_ATP_COMPARE_DEFAULTS["recent_last_n"],
                            recent_surface_last_n=_ATP_COMPARE_DEFAULTS["recent_surface_last_n"],
                            max_pages=_ATP_COMPARE_DEFAULTS["max_pages"],
                        )
                    match_analyses[mkey] = compare_payload
                    _write_compare_to_bq(
                        cache_key=cache_key,
                        payload=compare_payload,
                        player_ids=[int(p1), int(p2)],
                        season=_ATP_COMPARE_DEFAULTS["season"],
                        seasons_back=_ATP_COMPARE_DEFAULTS["seasons_back"],
                        start_season=_ATP_COMPARE_DEFAULTS["start_season"],
                        end_season=_ATP_COMPARE_DEFAULTS["end_season"],
                        surface=(tournament.get("surface") or "").lower() or None,
                        tournament_id=tournament.get("id"),
                        match_id=str(match.get("id")) if match.get("id") is not None else None,
                        ttl_seconds=12 * 3600,
                    )
                except Exception:
                    continue

    payload = {
        "tournament": {
            "id": tournament.get("id"),
            "name": tournament.get("name"),
            "surface": tournament.get("surface"),
            "start_date": tournament.get("start_date"),
            "end_date": tournament.get("end_date"),
            "category": tournament.get("category"),
            "city": tournament.get("city"),
            "country": tournament.get("country"),
        },
        "bracket": {
            "rounds": round_list,
        },
        "upcoming_matches": upcoming_matches,
        "match_analyses": match_analyses,
        "match_count": len(formatted_matches),
    }

    # Cache aggressively because this payload is shared across users and only changes
    # when ATP source data updates.
    _cache_set(_ATP_BRACKET_CACHE, cache_key, payload, ttl_seconds=300)
    return payload




@router.post("/tournament-bracket/recompute")
def recompute_atp_tournament_bracket(
    tournament_id: Optional[int] = None,
    tournament_name: Optional[str] = None,
    season: Optional[int] = None,
    upcoming_limit: int = Query(50, ge=1, le=100),
    max_pages: Optional[int] = Query(20, ge=1, le=500),
):
    """
    Force recompute matchup analyses for the tournament bracket and persist to BigQuery cache table.
    """
    try:
        payload = build_tournament_bracket_payload(
            tournament_id=tournament_id,
            tournament_name=tournament_name,
            season=season,
            upcoming_limit=upcoming_limit,
            max_pages=max_pages,
            include_match_analyses=True,
            recompute_missing_analyses=True,
        )
        return {
            "status": "ok",
            "tournament": payload.get("tournament"),
            "analysis_count": len(payload.get("match_analyses") or {}),
            "match_count": payload.get("match_count", 0),
        }
    except HTTPException:
        raise
    except Exception as err:
        _handle_error(err)

@router.get("/analytics/player-form")
def atp_player_form(
    season: Optional[int] = None,
    seasons_back: Optional[int] = Query(None, ge=0, le=10),
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    surface: Optional[str] = None,
    last_n: int = Query(12, ge=3, le=60),
    min_matches: int = Query(5, ge=1, le=30),
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    try:
        seasons = _resolve_seasons(
            season=season,
            seasons_back=seasons_back,
            start_season=start_season,
            end_season=end_season,
        )
        matches = _fetch_matches_for_seasons(seasons, max_pages=max_pages)
        rows = build_player_form(
            matches,
            last_n=last_n,
            min_matches=min_matches,
            surface=surface,
        )
        return {"seasons": seasons, "count": len(rows), "rows": rows}
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/surface-splits")
def atp_surface_splits(
    player_id: int,
    season: Optional[int] = None,
    seasons_back: Optional[int] = Query(2, ge=0, le=10),
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    min_matches: int = Query(5, ge=1, le=30),
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    try:
        seasons = _resolve_seasons(
            season=season,
            seasons_back=seasons_back,
            start_season=start_season,
            end_season=end_season,
        )
        matches = _fetch_matches_for_seasons(seasons, max_pages=max_pages)
        rows = build_surface_splits(matches, player_id=player_id, min_matches=min_matches)
        return {"player_id": player_id, "seasons": seasons, "rows": rows}
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/head-to-head")
def atp_head_to_head(
    player_id: int,
    opponent_id: int,
    season: Optional[int] = None,
    seasons_back: Optional[int] = Query(3, ge=0, le=15),
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    try:
        seasons = _resolve_seasons(
            season=season,
            seasons_back=seasons_back,
            start_season=start_season,
            end_season=end_season,
        )
        matches = _fetch_matches_for_seasons(seasons, max_pages=max_pages)
        payload = build_head_to_head(
            matches,
            player_id=player_id,
            opponent_id=opponent_id,
        )
        payload["seasons"] = seasons
        return payload
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/tournament-performance")
def atp_tournament_performance(
    season: Optional[int] = None,
    seasons_back: Optional[int] = Query(1, ge=0, le=10),
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    surface: Optional[str] = None,
    min_matches: int = Query(5, ge=1, le=30),
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    try:
        seasons = _resolve_seasons(
            season=season,
            seasons_back=seasons_back,
            start_season=start_season,
            end_season=end_season,
        )
        matches = _fetch_matches_for_seasons(seasons, max_pages=max_pages)
        rows = build_tournament_performance(
            matches,
            min_matches=min_matches,
            surface=surface,
        )
        return {"seasons": seasons, "count": len(rows), "rows": rows}
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/region-splits")
def atp_region_splits(
    player_id: int,
    season: Optional[int] = None,
    seasons_back: Optional[int] = Query(2, ge=0, le=10),
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    try:
        seasons = _resolve_seasons(
            season=season,
            seasons_back=seasons_back,
            start_season=start_season,
            end_season=end_season,
        )
        matches = _fetch_matches_for_seasons(seasons, max_pages=max_pages)
        payload = build_region_splits(matches, player_id=player_id)
        payload["seasons"] = seasons
        return payload
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/set-distribution")
def atp_set_distribution(
    player_id: int,
    season: Optional[int] = None,
    seasons_back: Optional[int] = Query(2, ge=0, le=10),
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    surface: Optional[str] = None,
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    try:
        seasons = _resolve_seasons(
            season=season,
            seasons_back=seasons_back,
            start_season=start_season,
            end_season=end_season,
        )
        matches = _fetch_matches_for_seasons(seasons, max_pages=max_pages)
        payload = build_set_distribution(
            matches,
            player_id=player_id,
            surface=surface,
        )
        payload["seasons"] = seasons
        return payload
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/compare")
def atp_compare(
    player_ids: List[int] = Query(...),
    season: Optional[int] = None,
    seasons_back: Optional[int] = Query(2, ge=0, le=10),
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    surface: Optional[str] = None,
    last_n: int = Query(25, ge=5, le=80),
    surface_last_n: int = Query(45, ge=10, le=150),
    recent_last_n: int = Query(10, ge=3, le=30),
    recent_surface_last_n: int = Query(20, ge=5, le=80),
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    try:
        if len(player_ids) < 2 or len(player_ids) > 3:
            raise HTTPException(status_code=400, detail="player_ids must include 2 or 3 IDs")

        compare_cache_key = _compare_cache_key(
            player_ids=player_ids,
            season=season,
            seasons_back=seasons_back,
            start_season=start_season,
            end_season=end_season,
            surface=surface,
            last_n=last_n,
            surface_last_n=surface_last_n,
            recent_last_n=recent_last_n,
            recent_surface_last_n=recent_surface_last_n,
            max_pages=max_pages,
        )

        cached_payload = _cache_get(_ATP_COMPARE_CACHE, compare_cache_key)
        if cached_payload is not None:
            return cached_payload

        bq_cached = _read_compare_from_bq(compare_cache_key)
        if bq_cached is not None:
            _cache_set(_ATP_COMPARE_CACHE, compare_cache_key, bq_cached, ttl_seconds=1200)
            return bq_cached

        rankings_payload = fetch_paginated("/rankings", params={"per_page": 100}, cache_ttl=900, max_pages=3)
        rankings_map: Dict[int, Any] = {}
        for row in rankings_payload:
            player = row.get("player") or {}
            pid = player.get("id")
            if pid:
                rankings_map[int(pid)] = row.get("rank")

        precomputed_payload = _build_compare_from_precomputed(
            player_ids=player_ids,
            surface=surface,
            rankings=rankings_map,
        )
        if precomputed_payload is not None:
            _cache_set(_ATP_COMPARE_CACHE, compare_cache_key, precomputed_payload, ttl_seconds=1200)
            _write_compare_to_bq(
                cache_key=compare_cache_key,
                payload=precomputed_payload,
                player_ids=player_ids,
                season=season,
                seasons_back=seasons_back,
                start_season=start_season,
                end_season=end_season,
                surface=surface,
                tournament_id=None,
                match_id=None,
                ttl_seconds=12 * 3600,
            )
            return precomputed_payload

        payload = _build_compare_payload(
            player_ids=player_ids,
            season=season,
            seasons_back=seasons_back,
            start_season=start_season,
            end_season=end_season,
            surface=surface,
            last_n=last_n,
            surface_last_n=surface_last_n,
            recent_last_n=recent_last_n,
            recent_surface_last_n=recent_surface_last_n,
            max_pages=max_pages,
        )

        _cache_set(_ATP_COMPARE_CACHE, compare_cache_key, payload, ttl_seconds=1200)
        _write_compare_to_bq(
            cache_key=compare_cache_key,
            payload=payload,
            player_ids=player_ids,
            season=season,
            seasons_back=seasons_back,
            start_season=start_season,
            end_season=end_season,
            surface=surface,
            tournament_id=None,
            match_id=None,
            ttl_seconds=12 * 3600,
        )
        return payload
    except Exception as err:
        _handle_error(err)
