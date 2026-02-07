from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from atp.analytics import (
    build_compare,
    build_head_to_head,
    build_player_form,
    build_region_splits,
    build_set_distribution,
    build_surface_splits,
    build_tournament_performance,
)
from atp.client import AtpApiError, fetch_one_page, fetch_paginated


router = APIRouter(prefix="/atp", tags=["ATP"])


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
    last_n: int = Query(12, ge=3, le=60),
    max_pages: Optional[int] = Query(5, ge=1, le=500),
):
    try:
        if len(player_ids) < 2 or len(player_ids) > 3:
            raise HTTPException(status_code=400, detail="player_ids must include 2 or 3 IDs")
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
            rankings=rankings_map,
        )
    except Exception as err:
        _handle_error(err)
