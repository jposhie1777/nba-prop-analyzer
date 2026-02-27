from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from zoneinfo import ZoneInfo

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
    build_simulated_leaderboard,
    build_tournament_difficulty,
)
from pga.bq import fetch_pairings_analytics, fetch_round_pairings, fetch_tournament_round_scores
from pga.client import PgaApiError, fetch_one_page, fetch_paginated
from pga.utils import parse_iso_datetime


router = APIRouter(prefix="/pga", tags=["PGA"])
NY_TZ = ZoneInfo("America/New_York")
ACTIVE_TOURNAMENT_STATUSES = {"in_progress", "active", "ongoing", "live"}


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


def _tournament_date_bounds(tournament: Dict[str, Any]) -> tuple[Optional[date], Optional[date]]:
    start = parse_iso_datetime(tournament.get("start_date"))
    end = parse_iso_datetime(tournament.get("end_date"))
    return (start.date() if start else None, end.date() if end else None)


def _select_round_scores_tournament(
    tournaments: List[Dict[str, Any]],
    *,
    reference_date: date,
) -> Optional[Dict[str, Any]]:
    def sort_key(item: Dict[str, Any]) -> datetime:
        return parse_iso_datetime(item.get("start_date")) or datetime.min

    active = [
        t
        for t in tournaments
        if (t.get("status") or "").strip().lower() in ACTIVE_TOURNAMENT_STATUSES
    ]
    if active:
        return sorted(active, key=sort_key, reverse=True)[0]

    in_window: List[Dict[str, Any]] = []
    recent: List[Dict[str, Any]] = []
    cutoff = reference_date - timedelta(days=1)

    for tournament in tournaments:
        start, end = _tournament_date_bounds(tournament)
        if start and end and start <= reference_date <= end:
            in_window.append(tournament)
        if end and end >= cutoff:
            recent.append(tournament)

    if in_window:
        return sorted(in_window, key=sort_key, reverse=True)[0]
    if recent:
        return sorted(recent, key=sort_key, reverse=True)[0]
    return None




def _fetch_pga_player_headshots(player_ids: List[int]) -> Dict[int, str]:
    if not player_ids:
        return {}

    from google.cloud import bigquery

    from bq import get_bq_client

    client = get_bq_client()
    table = f"`{client.project}.pga_data.player_lookup`"
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
        FROM {table}
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

def _resolve_round_scores_tournament(
    *,
    tournament_id: Optional[int],
    season: int,
) -> Optional[Dict[str, Any]]:
    if tournament_id:
        payload = fetch_one_page(
            "/tournaments",
            params={"tournament_ids": [tournament_id]},
            cache_ttl=300,
        )
        tournaments = payload.get("data", [])
        return tournaments[0] if tournaments else None

    tournaments = fetch_paginated(
        "/tournaments",
        params={"season": season},
        cache_ttl=300,
    )
    reference_date = datetime.now(NY_TZ).date()
    return _select_round_scores_tournament(tournaments, reference_date=reference_date)


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
        seasons_to_try = [season, season - 1, season - 2]
        selected_season = season
        selected_results: List[Dict[str, Any]] = []
        selected_rows: List[Dict[str, Any]] = []

        for year in seasons_to_try:
            results = _fetch_results_for_seasons([year])
            rows = build_placement_probabilities(results, last_n=last_n, min_events=min_events)
            if rows:
                selected_season = year
                selected_results = results
                selected_rows = rows
                break
            if not selected_results:
                selected_season = year
                selected_results = results

        return {
            "season": selected_season,
            "requested_season": season,
            "count": len(selected_results),
            "rows": selected_rows,
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
    min_events: int = Query(5, ge=1, le=20),
    simulations: int = Query(2000, ge=500, le=10000),
):
    try:
        season = season or _current_season()
        results = _fetch_results_for_seasons([season])
        return build_simulated_finishes(
            results,
            player_id=player_id,
            last_n=last_n,
            min_events=min_events,
            simulations=simulations,
        )
    except Exception as err:
        _handle_error(err)




@router.get("/analytics/simulated-leaderboard")
def pga_simulated_leaderboard(
    season: Optional[int] = None,
    last_n: int = Query(20, ge=5, le=60),
    min_events: int = Query(5, ge=1, le=20),
    simulations: int = Query(2000, ge=500, le=10000),
):
    try:
        season = season or _current_season()
        results = _fetch_results_for_seasons([season])
        return build_simulated_leaderboard(
            results,
            last_n=last_n,
            min_events=min_events,
            simulations=simulations,
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
        try:
            headshots = _fetch_pga_player_headshots(player_ids)
        except Exception:
            headshots = {}
        if headshots:
            players = [
                {
                    **player,
                    "player_image_url": headshots.get(player.get("id")),
                }
                for player in players
            ]

        tournaments: List[Dict[str, Any]] = []
        courses: List[Dict[str, Any]] = []
        if course_id:
            tournaments = _fetch_tournaments_for_seasons(seasons)
            courses = fetch_paginated(
                "/courses",
                params={"per_page": 100},
                cache_ttl=900,
            )

        round_scores: List[Dict[str, Any]] = []
        round_scores_tournament = _resolve_round_scores_tournament(
            tournament_id=tournament_id,
            season=season,
        )
        if round_scores_tournament and round_scores_tournament.get("id"):
            round_scores = fetch_tournament_round_scores(
                {
                    "season": round_scores_tournament.get("season") or season,
                    "tournament_ids": [round_scores_tournament.get("id")],
                    "player_ids": player_ids,
                }
            )

        response = build_compare(
            results,
            player_ids=player_ids,
            players=players,
            tournaments=tournaments,
            courses=courses,
            round_scores=round_scores,
            course_id=course_id,
            tournament_id=tournament_id,
            last_n_form=last_n_form,
            last_n_placement=last_n_placement,
        )
        response["round_scores_tournament"] = round_scores_tournament
        return response
    except Exception as err:
        _handle_error(err)


def _normalize_group(
    values: Dict[int, Optional[float]], *, invert: bool = False
) -> Dict[int, Optional[float]]:
    """Min-max normalise a dict of {player_id: value} within one pairing group."""
    filtered = {k: v for k, v in values.items() if v is not None}
    if not filtered:
        return {k: None for k in values}
    lo, hi = min(filtered.values()), max(filtered.values())
    if lo == hi:
        return {k: (0.5 if v is not None else None) for k, v in values.items()}
    out: Dict[int, Optional[float]] = {}
    for k, v in values.items():
        if v is None:
            out[k] = None
        else:
            score = (v - lo) / (hi - lo)
            out[k] = round(1 - score if invert else score, 4)
    return out


@router.get("/analytics/pairings")
def pga_pairings(
    tournament_id: Optional[str] = None,
    round_number: Optional[int] = None,
):
    """
    Return all pairing groups with pre-computed analytics from v_pairings_analytics.

    A single BigQuery query fetches pairings + player form/placement stats
    (computed in SQL over the last 3 seasons).  Per-group composite scoring
    and best-bet recommendation are computed in Python from the already-fetched
    metrics — no extra round-trips, no heavy in-memory processing.

    Run  `python -m pga.create_views`  once to create the required views.
    """
    try:
        params: Dict[str, Any] = {}
        if tournament_id:
            params["tournament_id"] = tournament_id
        if round_number:
            params["round_numbers"] = [round_number]

        rows = fetch_pairings_analytics(params)
        if not rows:
            return {"tournament_id": tournament_id, "round_number": round_number, "groups": []}

        # ── Group rows by (round_number, group_number) ────────────────────────
        from collections import defaultdict
        groups: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[(row["round_number"], row["group_number"])].append(row)

        # ── Fetch headshots once for all players (best-effort) ────────────────
        all_ids_int = [r["player_id_int"] for r in rows if r.get("player_id_int")]
        try:
            headshots = _fetch_pga_player_headshots(list(set(all_ids_int)))
        except Exception:
            headshots = {}

        # ── Build output ──────────────────────────────────────────────────────
        output_groups: List[Dict[str, Any]] = []
        for (rnd, grp_num), members in sorted(groups.items()):
            sample = members[0]

            players_info = [
                {
                    "player_id":          m["player_id"],
                    "player_id_int":      m["player_id_int"],
                    "player_display_name": m["player_display_name"],
                    "player_first_name":  m["player_first_name"],
                    "player_last_name":   m["player_last_name"],
                    "country":            m["country"],
                    "world_rank":         m["world_rank"],
                    "amateur":            m["amateur"],
                    "player_image_url":   headshots.get(m["player_id_int"]),
                }
                for m in members
            ]

            # ── Per-group analytics ───────────────────────────────────────────
            analytics = None
            pids = [m["player_id_int"] for m in members if m.get("player_id_int")]
            if len(pids) >= 2:
                form_raw   = {m["player_id_int"]: m.get("form_score")  for m in members if m.get("player_id_int")}
                top10_raw  = {m["player_id_int"]: m.get("top10_prob")  for m in members if m.get("player_id_int")}

                form_norm  = _normalize_group(form_raw)
                top10_norm = _normalize_group(top10_raw)

                # Weights: form 55 %, top-10 prob 45 %
                # (H2H and course-fit are not available in the pre-computed view)
                FORM_W, TOP10_W = 0.55, 0.45

                players_out: List[Dict[str, Any]] = []
                for m in members:
                    pid = m.get("player_id_int")
                    if pid is None:
                        continue
                    parts, wts = [], []
                    if form_norm.get(pid) is not None:
                        parts.append(FORM_W * form_norm[pid])
                        wts.append(FORM_W)
                    if top10_norm.get(pid) is not None:
                        parts.append(TOP10_W * top10_norm[pid])
                        wts.append(TOP10_W)
                    score = round(sum(parts) / sum(wts), 4) if wts else 0.0

                    players_out.append({
                        "player_id": pid,
                        "player": {
                            "id":           pid,
                            "display_name": m.get("player_display_name") or str(pid),
                            "first_name":   m.get("player_first_name"),
                            "last_name":    m.get("player_last_name"),
                        },
                        "rank":  0,
                        "score": score,
                        "metrics": {
                            "form_score":             m.get("form_score"),
                            "form_starts":            m.get("form_starts"),
                            "avg_finish":             m.get("avg_finish"),
                            "top10_rate":             m.get("top10_rate"),
                            "top10_prob":             m.get("top10_prob"),
                            "top20_prob":             m.get("top20_prob"),
                            "top5_prob":              m.get("top5_prob"),
                            "cut_rate":               m.get("cut_rate"),
                            "head_to_head_win_rate":  None,
                            "head_to_head_starts":    0,
                            "course_fit_score":       None,
                        },
                    })

                players_out.sort(key=lambda r: r["score"], reverse=True)
                for idx, row in enumerate(players_out, 1):
                    row["rank"] = idx

                recommendation = None
                if players_out:
                    top    = players_out[0]
                    runner = players_out[1] if len(players_out) > 1 else None
                    edge   = round(
                        (top["score"] - runner["score"]) if runner else top["score"], 4
                    )
                    reasons: List[str] = []
                    if top["metrics"].get("form_score") is not None and (
                        all(
                            (m.get("form_score") or float("-inf")) <= (top["metrics"]["form_score"] or float("-inf"))
                            for m in members if m.get("player_id_int") != top["player_id"]
                        )
                    ):
                        reasons.append("Best recent form")
                    if top["metrics"].get("top10_prob") is not None and (
                        all(
                            (m.get("top10_prob") or float("-inf")) <= (top["metrics"]["top10_prob"] or float("-inf"))
                            for m in members if m.get("player_id_int") != top["player_id"]
                        )
                    ):
                        reasons.append("Highest top-10 rate")
                    if top["metrics"].get("avg_finish") is not None and (
                        all(
                            (m.get("avg_finish") or float("inf")) >= (top["metrics"]["avg_finish"] or float("inf"))
                            for m in members if m.get("player_id_int") != top["player_id"]
                        )
                    ):
                        reasons.append("Best avg finish")

                    recommendation = {
                        "player_id": top["player_id"],
                        "label":     "Top composite score",
                        "edge":      edge,
                        "reasons":   reasons or ["Highest overall score"],
                    }

                analytics = {
                    "players":        players_out,
                    "recommendation": recommendation,
                }

            output_groups.append({
                "group_number": grp_num,
                "round_number": rnd,
                "round_status": sample.get("round_status"),
                "tee_time":     str(sample["tee_time"]) if sample.get("tee_time") else None,
                "start_hole":   sample.get("start_hole"),
                "back_nine":    sample.get("back_nine"),
                "course_name":  sample.get("course_name"),
                "players":      players_info,
                "analytics":    analytics,
            })

        first = rows[0]
        return {
            "tournament_id": first.get("tournament_id"),
            "round_number":  round_number,
            "snapshot_ts":   first.get("snapshot_ts"),
            "groups":        output_groups,
        }
    except Exception as err:
        _handle_error(err)
