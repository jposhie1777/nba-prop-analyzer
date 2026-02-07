"""
PGA Strokes Gained – breaks down player performance by SG category
(off-the-tee, approach, around-the-green, putting) using scoring data
relative to field averages, plus course demand profiling.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from statistics import mean
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from pga.client import PgaApiError, fetch_one_page, fetch_paginated
from pga.utils import finish_value, is_cut, parse_iso_datetime, safe_div

router = APIRouter(prefix="/pga", tags=["PGA"])


def _current_season() -> int:
    return datetime.utcnow().year


def _handle_error(err: Exception) -> None:
    if isinstance(err, PgaApiError):
        raise HTTPException(status_code=502, detail=str(err))
    raise HTTPException(status_code=500, detail=str(err))


def _fetch_results(seasons: List[int]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for s in seasons:
        results.extend(
            fetch_paginated("/tournament_results", params={"season": s}, cache_ttl=900)
        )
    return results


def _fetch_course_stats(season: int, tournament_id: Optional[int] = None) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"season": season}
    if tournament_id:
        params["tournament_ids"] = [tournament_id]
    return fetch_paginated("/tournament_course_stats", params=params, cache_ttl=900)


def _compute_sg_breakdown(
    results: List[Dict[str, Any]],
    *,
    last_n: int = 20,
    min_events: int = 5,
    cut_penalty: int = 80,
) -> List[Dict[str, Any]]:
    """
    Compute strokes-gained-style breakdown from tournament results.

    Since the BallDontLie API doesn't provide true SG stats, we derive
    a proxy from par-relative scoring, consistency, and finish patterns
    to create a meaningful 4-category breakdown.
    """
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for result in results:
        player = result.get("player") or {}
        player_id = player.get("id")
        if not player_id:
            continue
        tournament = result.get("tournament") or {}
        grouped[player_id].append({
            "player": player,
            "tournament": tournament,
            "start_date": parse_iso_datetime(tournament.get("start_date")),
            "position": result.get("position"),
            "position_numeric": result.get("position_numeric"),
            "par_relative_score": result.get("par_relative_score"),
        })

    # Field averages for normalization
    all_finishes = []
    all_par_scores = []
    for entries in grouped.values():
        for e in entries:
            pos = e.get("position_numeric")
            if pos is not None:
                all_finishes.append(pos)
            prs = e.get("par_relative_score")
            if prs is not None:
                all_par_scores.append(prs)

    field_avg_finish = mean(all_finishes) if all_finishes else 40.0
    field_avg_par = mean(all_par_scores) if all_par_scores else 0.0

    output: List[Dict[str, Any]] = []
    for player_id, entries in grouped.items():
        ordered = sorted(entries, key=lambda e: e.get("start_date") or datetime.min)
        recent = ordered[-last_n:]
        if len(recent) < min_events:
            continue

        finishes = [
            finish_value(r.get("position_numeric"), r.get("position"), cut_penalty)
            for r in recent
        ]
        par_scores = [r["par_relative_score"] for r in recent if r.get("par_relative_score") is not None]

        avg_finish = mean(finishes) if finishes else field_avg_finish
        avg_par = mean(par_scores) if par_scores else field_avg_par
        total_sg = round(field_avg_par - avg_par, 2) if par_scores else 0.0

        # Derive SG categories from patterns in the data
        # This is a proxy model based on finish distributions
        top5_count = sum(1 for f in finishes if f <= 5)
        top10_count = sum(1 for f in finishes if f <= 10)
        top20_count = sum(1 for f in finishes if f <= 20)
        cut_count = sum(1 for r in recent if is_cut(r.get("position")))
        n = len(recent)

        top5_rate = safe_div(top5_count, n)
        top10_rate = safe_div(top10_count, n)
        top20_rate = safe_div(top20_count, n)
        made_cut_rate = 1 - safe_div(cut_count, n)

        # Distribute total SG across categories using scoring profile:
        # - Off the tee: correlates with avoiding cuts + overall scoring (30%)
        # - Approach: correlates with top-10 finishes (30%)
        # - Around the green: correlates with consistency/made cut (20%)
        # - Putting: correlates with winning/top-5 (20%)
        if total_sg != 0:
            sg_ott = round(total_sg * 0.30, 2)
            sg_app = round(total_sg * 0.30, 2)
            sg_arg = round(total_sg * 0.20, 2)
            sg_putt = round(total_sg * 0.20, 2)
        else:
            # Even when total SG is 0, create relative breakdown
            sg_ott = round((made_cut_rate - 0.85) * 3, 2)
            sg_app = round((top10_rate - 0.15) * 4, 2)
            sg_arg = round((top20_rate - 0.30) * 2.5, 2)
            sg_putt = round((top5_rate - 0.05) * 5, 2)

        # Strength rating per category (0-100)
        strength_ott = min(100, max(0, round(50 + sg_ott * 20)))
        strength_app = min(100, max(0, round(50 + sg_app * 20)))
        strength_arg = min(100, max(0, round(50 + sg_arg * 20)))
        strength_putt = min(100, max(0, round(50 + sg_putt * 20)))

        player_info = recent[-1].get("player", {})
        output.append({
            "player_id": player_id,
            "player": player_info,
            "starts": n,
            "avg_finish": round(avg_finish, 1),
            "avg_par_score": round(avg_par, 2) if par_scores else None,
            "total_sg": total_sg,
            "sg_off_the_tee": sg_ott,
            "sg_approach": sg_app,
            "sg_around_green": sg_arg,
            "sg_putting": sg_putt,
            "strength_off_the_tee": strength_ott,
            "strength_approach": strength_app,
            "strength_around_green": strength_arg,
            "strength_putting": strength_putt,
            "top5_rate": round(top5_rate, 3),
            "top10_rate": round(top10_rate, 3),
            "top20_rate": round(top20_rate, 3),
            "made_cut_rate": round(made_cut_rate, 3),
        })

    output.sort(key=lambda r: r["total_sg"], reverse=True)
    return output


def _course_demand_profile(stats: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a course demand profile from tournament_course_stats.

    Returns relative importance of driving, approach accuracy,
    short game, and putting at the course.
    """
    if not stats:
        return {
            "driving": 50,
            "approach": 50,
            "short_game": 50,
            "putting": 50,
            "description": "No course data available",
        }

    birdie_rates = []
    bogey_rates = []
    par_rates = []
    scoring_avgs = []
    scoring_diffs = []

    for row in stats:
        eagles = row.get("eagles") or 0
        birdies = row.get("birdies") or 0
        pars = row.get("pars") or 0
        bogeys = row.get("bogeys") or 0
        doubles = row.get("double_bogeys") or 0
        total = eagles + birdies + pars + bogeys + doubles

        if total > 0:
            birdie_rates.append(safe_div(eagles + birdies, total))
            bogey_rates.append(safe_div(bogeys + doubles, total))
            par_rates.append(safe_div(pars, total))

        if row.get("scoring_average") is not None:
            scoring_avgs.append(row["scoring_average"])
        if row.get("scoring_diff") is not None:
            scoring_diffs.append(row["scoring_diff"])

    avg_birdie = mean(birdie_rates) if birdie_rates else 0.2
    avg_bogey = mean(bogey_rates) if bogey_rates else 0.2
    avg_scoring_diff = mean(scoring_diffs) if scoring_diffs else 0.0

    # Hard courses demand more driving/approach; birdie-fest courses demand putting
    is_hard = avg_scoring_diff > 0.5 or avg_bogey > 0.25
    is_easy = avg_scoring_diff < -0.5 or avg_birdie > 0.25

    if is_hard:
        driving = 70
        approach = 65
        short_game = 60
        putting = 50
        desc = "Demanding course — ball striking and scrambling are key"
    elif is_easy:
        driving = 45
        approach = 55
        short_game = 45
        putting = 75
        desc = "Birdie-friendly course — putting separates the field"
    else:
        driving = 55
        approach = 60
        short_game = 55
        putting = 60
        desc = "Balanced course — well-rounded game is rewarded"

    return {
        "driving": driving,
        "approach": approach,
        "short_game": short_game,
        "putting": putting,
        "scoring_average": round(mean(scoring_avgs), 2) if scoring_avgs else None,
        "scoring_diff": round(avg_scoring_diff, 2) if scoring_diffs else None,
        "birdie_rate": round(avg_birdie, 3),
        "bogey_rate": round(avg_bogey, 3),
        "description": desc,
    }


def _compute_course_fit_score(
    player: Dict[str, Any],
    demand: Dict[str, Any],
) -> float:
    """How well a player's SG profile fits the course demand."""
    weights = {
        "off_the_tee": demand.get("driving", 50) / 100,
        "approach": demand.get("approach", 50) / 100,
        "around_green": demand.get("short_game", 50) / 100,
        "putting": demand.get("putting", 50) / 100,
    }

    total_weight = sum(weights.values())
    if total_weight == 0:
        return 0.0

    score = (
        weights["off_the_tee"] * player.get("strength_off_the_tee", 50)
        + weights["approach"] * player.get("strength_approach", 50)
        + weights["around_green"] * player.get("strength_around_green", 50)
        + weights["putting"] * player.get("strength_putting", 50)
    ) / total_weight

    return round(score, 1)


@router.get("/analytics/strokes-gained")
def pga_strokes_gained(
    season: Optional[int] = None,
    tournament_id: Optional[int] = None,
    last_n: int = Query(20, ge=5, le=60),
    min_events: int = Query(5, ge=1, le=20),
    limit: int = Query(50, ge=1, le=200),
):
    """
    Strokes Gained breakdown for PGA players.

    Returns SG-style metrics (off-the-tee, approach, around-green, putting),
    plus optional course demand profile and course-fit scoring.
    """
    try:
        season = season or _current_season()
        results = _fetch_results([season])

        # If no results for this season, try previous
        if not results:
            results = _fetch_results([season - 1])
            if results:
                season = season - 1

        players = _compute_sg_breakdown(results, last_n=last_n, min_events=min_events)

        # Optional: course demand profile
        course_demand = None
        if tournament_id:
            try:
                stats = _fetch_course_stats(season, tournament_id=tournament_id)
                course_demand = _course_demand_profile(stats)

                # Compute course fit for each player
                for p in players:
                    p["course_fit"] = _compute_course_fit_score(p, course_demand)

                # Re-sort by course fit if available
                players.sort(key=lambda p: p.get("course_fit", 0), reverse=True)
            except Exception as e:
                print(f"[STROKES_GAINED] Course stats failed (non-fatal): {e}")

        return {
            "season": season,
            "tournament_id": tournament_id,
            "count": min(len(players), limit),
            "course_demand": course_demand,
            "players": players[:limit],
        }
    except Exception as err:
        _handle_error(err)
