from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from pga.analytics import (
    build_compare,
    build_course_comps,
    build_course_fit,
    build_course_profile,
    build_cut_rates,
    build_matchup,
    build_placement_probabilities,
    build_player_form,
    build_region_splits,
    build_simulated_finishes,
    build_tournament_difficulty,
)
from pga.client import PgaApiError, fetch_one_page, fetch_paginated


router = APIRouter(prefix="/pga", tags=["PGA"])


def _current_season() -> int:
    return datetime.utcnow().year


def _handle_error(err: Exception) -> None:
    if isinstance(err, PgaApiError):
        raise HTTPException(status_code=502, detail=str(err))
    raise HTTPException(status_code=500, detail=str(err))


def _fetch_results_for_seasons(seasons: List[int]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for season in seasons:
        results.extend(
            fetch_paginated(
                "/tournament_results",
                params={"season": season},
                cache_ttl=900,
            )
        )
    return results


def _fetch_tournaments_for_seasons(seasons: List[int]) -> List[Dict[str, Any]]:
    tournaments: List[Dict[str, Any]] = []
    for season in seasons:
        tournaments.extend(
            fetch_paginated(
                "/tournaments",
                params={"season": season},
                cache_ttl=900,
            )
        )
    return tournaments


@router.get("/players")
def get_pga_players(
    search: Optional[str] = None,
    active: Optional[bool] = None,
    per_page: int = Query(50, ge=1, le=100),
    cursor: Optional[int] = None,
):
    try:
        params: Dict[str, Any] = {"per_page": per_page}
        if search:
            params["search"] = search
        if active is not None:
            params["active"] = active
        if cursor is not None:
            params["cursor"] = cursor
        payload = fetch_one_page("/players", params=params, cache_ttl=300)
        data = payload.get("data", [])
        return {"data": data, "count": len(data), "meta": payload.get("meta", {})}
    except Exception as err:
        _handle_error(err)


@router.get("/tournaments")
def get_pga_tournaments(
    season: Optional[int] = None,
    status: Optional[str] = None,
    per_page: int = Query(50, ge=1, le=100),
    cursor: Optional[int] = None,
):
    try:
        params: Dict[str, Any] = {"per_page": per_page}
        if season is not None:
            params["season"] = season
        if status:
            params["status"] = status
        if cursor is not None:
            params["cursor"] = cursor
        payload = fetch_one_page("/tournaments", params=params, cache_ttl=300)
        data = payload.get("data", [])
        return {"data": data, "count": len(data), "meta": payload.get("meta", {})}
    except Exception as err:
        _handle_error(err)


@router.get("/courses")
def get_pga_courses(
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
        payload = fetch_one_page("/courses", params=params, cache_ttl=300)
        data = payload.get("data", [])
        return {"data": data, "count": len(data), "meta": payload.get("meta", {})}
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/player-form")
def pga_player_form(
    season: Optional[int] = None,
    last_n: int = Query(10, ge=3, le=50),
    min_events: int = Query(3, ge=1, le=20),
):
    try:
        season = season or _current_season()
        results = _fetch_results_for_seasons([season])
        return {
            "season": season,
            "count": len(results),
            "rows": build_player_form(results, last_n=last_n, min_events=min_events),
        }
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/placement-probabilities")
def pga_placement_probabilities(
    season: Optional[int] = None,
    last_n: int = Query(20, ge=5, le=60),
    min_events: int = Query(5, ge=1, le=20),
):
    try:
        season = season or _current_season()
        results = _fetch_results_for_seasons([season])
        return {
            "season": season,
            "count": len(results),
            "rows": build_placement_probabilities(results, last_n=last_n, min_events=min_events),
        }
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/cut-rates")
def pga_cut_rates(
    season: Optional[int] = None,
    last_n: int = Query(20, ge=5, le=60),
    min_events: int = Query(5, ge=1, le=20),
):
    try:
        season = season or _current_season()
        results = _fetch_results_for_seasons([season])
        return {
            "season": season,
            "count": len(results),
            "rows": build_cut_rates(results, last_n=last_n, min_events=min_events),
        }
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/tournament-difficulty")
def pga_tournament_difficulty(
    season: Optional[int] = None,
    tournament_id: Optional[int] = None,
    include_rounds: bool = False,
):
    try:
        season = season or _current_season()
        params: Dict[str, Any] = {"season": season}
        if tournament_id:
            params["tournament_ids"] = [tournament_id]
        stats = fetch_paginated("/tournament_course_stats", params=params, cache_ttl=900)
        rows = build_tournament_difficulty(stats, include_rounds=include_rounds)
        return {
            "season": season,
            "count": len(rows),
            "rows": rows,
        }
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/course-profile")
def pga_course_profile(
    course_id: Optional[int] = None,
    tournament_id: Optional[int] = None,
):
    try:
        if course_id is None and tournament_id is None:
            raise HTTPException(status_code=400, detail="course_id or tournament_id required")

        if course_id is None and tournament_id is not None:
            tournament_payload = fetch_one_page(
                "/tournaments", params={"tournament_ids": [tournament_id]}, cache_ttl=300
            )
            tournaments = tournament_payload.get("data", [])
            if not tournaments:
                raise HTTPException(status_code=404, detail="Tournament not found")
            courses = tournaments[0].get("courses") or []
            if not courses:
                raise HTTPException(status_code=404, detail="No courses for tournament")
            course_id = (courses[0].get("course") or {}).get("id")

        holes_payload = fetch_one_page(
            "/course_holes", params={"course_ids": [course_id]}, cache_ttl=900
        )
        holes = holes_payload.get("data", [])
        return build_course_profile(holes)
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/course-comps")
def pga_course_comps(
    course_id: int,
    limit: int = Query(8, ge=3, le=20),
):
    try:
        courses = fetch_paginated("/courses", params={"per_page": 100}, cache_ttl=900)
        return build_course_comps(courses, course_id, limit=limit)
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/course-fit")
def pga_course_fit(
    course_id: int,
    seasons_back: int = Query(2, ge=0, le=5),
    last_n: int = Query(20, ge=5, le=60),
    min_events: int = Query(2, ge=1, le=10),
):
    try:
        current = _current_season()
        seasons = [current - offset for offset in range(seasons_back + 1)]
        results = _fetch_results_for_seasons(seasons)
        tournaments = _fetch_tournaments_for_seasons(seasons)
        courses = fetch_paginated("/courses", params={"per_page": 100}, cache_ttl=900)
        return build_course_fit(
            results,
            tournaments,
            courses,
            target_course_id=course_id,
            last_n=last_n,
            min_events=min_events,
        )
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/matchup")
def pga_matchup(
    player_id: int,
    opponent_id: int,
    season: Optional[int] = None,
):
    try:
        season = season or _current_season()
        results = _fetch_results_for_seasons([season])
        return build_matchup(results, player_id=player_id, opponent_id=opponent_id)
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/region-splits")
def pga_region_splits(
    player_id: int,
    season: Optional[int] = None,
):
    try:
        season = season or _current_season()
        results = _fetch_results_for_seasons([season])
        return build_region_splits(results, player_id=player_id)
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/simulated-finishes")
def pga_simulated_finishes(
    player_id: int,
    season: Optional[int] = None,
    last_n: int = Query(20, ge=5, le=60),
    simulations: int = Query(2000, ge=500, le=10000),
):
    try:
        season = season or _current_season()
        results = _fetch_results_for_seasons([season])
        return build_simulated_finishes(
            results, player_id=player_id, last_n=last_n, simulations=simulations
        )
    except Exception as err:
        _handle_error(err)


@router.get("/analytics/compare")
def pga_compare(
    player_ids: List[int] = Query(...),
    season: Optional[int] = None,
    seasons_back: int = Query(2, ge=0, le=5),
    course_id: Optional[int] = None,
    tournament_id: Optional[int] = None,
    last_n_form: int = Query(10, ge=3, le=50),
    last_n_placement: int = Query(20, ge=5, le=60),
):
    try:
        if len(player_ids) < 2 or len(player_ids) > 3:
            raise HTTPException(status_code=400, detail="player_ids must include 2 or 3 IDs")

        season = season or _current_season()
        seasons = [season - offset for offset in range(seasons_back + 1)]

        results: List[Dict[str, Any]] = []
        for year in seasons:
            results.extend(
                fetch_paginated(
                    "/tournament_results",
                    params={"season": year, "player_ids": player_ids},
                    cache_ttl=900,
                )
            )

        players = fetch_paginated(
            "/players",
            params={"player_ids": player_ids, "per_page": 100},
            cache_ttl=900,
        )

        tournaments: List[Dict[str, Any]] = []
        courses: List[Dict[str, Any]] = []
        if course_id:
            tournaments = _fetch_tournaments_for_seasons(seasons)
            courses = fetch_paginated(
                "/courses",
                params={"per_page": 100},
                cache_ttl=900,
            )

        return build_compare(
            results,
            player_ids=player_ids,
            players=players,
            tournaments=tournaments,
            courses=courses,
            course_id=course_id,
            tournament_id=tournament_id,
            last_n_form=last_n_form,
            last_n_placement=last_n_placement,
        )
    except Exception as err:
        _handle_error(err)
