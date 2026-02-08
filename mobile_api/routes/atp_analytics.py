from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from atp.analytics import (
    _round_rank,
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


def _parse_match_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


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
    round_name = match.get("round") or match.get("round_name") or "Round"
    scheduled_raw = (
        match.get("start_time")
        or match.get("start_time_utc")
        or match.get("scheduled_at")
        or match.get("date")
        or match.get("start_date")
    )
    scheduled_at = _parse_match_time(scheduled_raw)
    p1_raw = match.get("player1") or match.get("player_1")
    p2_raw = match.get("player2") or match.get("player_2")
    return {
        "id": match.get("id"),
        "round": round_name,
        "round_order": match.get("round_order") if match.get("round_order") is not None else _round_rank(round_name),
        "status": match.get("match_status"),
        "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
        "player1": _player_label(p1_raw),
        "player2": _player_label(p2_raw),
        "player1_id": _player_id(p1_raw),
        "player2_id": _player_id(p2_raw),
        "winner": _player_label(match.get("winner")),
        "score": match.get("score"),
    }


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


@router.get("/tournament-bracket")
def get_atp_tournament_bracket(
    tournament_id: Optional[int] = None,
    tournament_name: Optional[str] = None,
    season: Optional[int] = None,
    upcoming_limit: int = Query(6, ge=1, le=50),
    max_pages: Optional[int] = Query(20, ge=1, le=500),
):
    try:
        return build_tournament_bracket_payload(
            tournament_id=tournament_id,
            tournament_name=tournament_name,
            season=season,
            upcoming_limit=upcoming_limit,
            max_pages=max_pages,
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
    upcoming_limit: int = 6,
    max_pages: Optional[int] = 20,
) -> Dict[str, Any]:
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

    def match_sort_key(match: Dict[str, Any]) -> tuple:
        scheduled = _parse_match_time(match.get("scheduled_at"))
        return (
            scheduled is None,
            scheduled or datetime.max,
        )

    upcoming_matches = [
        match
        for match in formatted_matches
        if match.get("status") != "F"
    ]
    upcoming_matches.sort(key=match_sort_key)
    upcoming_matches = upcoming_matches[:upcoming_limit]

    return {
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
        "match_count": len(formatted_matches),
    }


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
